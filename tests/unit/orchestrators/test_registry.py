"""Tests for dag_healer.orchestrators.registry, .compat and the package (section 3.8 of the design)."""

from __future__ import annotations

import importlib
import importlib.metadata
import importlib.util
import json
import logging
import os
import subprocess
import sys
import textwrap
import uuid
from collections.abc import Callable
from dataclasses import replace
from pathlib import Path
from types import SimpleNamespace
from typing import Any
from unittest import mock

import pytest

import dag_healer.orchestrators as orchestrators
from dag_healer.config import HealerConfig, config_from_mapping
from dag_healer.core.ports import OrchestratorAdapter
from dag_healer.orchestrators import compat, fake, registry
from dag_healer.orchestrators.fake import FakeAdapter, FakeWorld
from dag_healer.orchestrators.registry import (
    AdapterLoadError,
    AdapterOptionsError,
    OrchestratorSettings,
    Registration,
)

MakeConfig = Callable[..., HealerConfig]  # the make_config fixture of conftest.py

REPO_ROOT = Path(__file__).resolve().parents[3]

FACTORIES_MODULE = '''\
"""Third-party style adapter factories (registry tests)."""
from dag_healer.orchestrators.fake import FakeAdapter, FakeWorld

CREATED = []


class AcmeFactory:
    type = "acme"
    display_name = "Acme"
    options_model = None
    secret_options = ()

    def create(self, cfg, *, client=None):
        CREATED.append((cfg, client))
        return FakeAdapter(cfg, world=FakeWorld(instance="acme"), instance="acme")


class NeedsArguments(AcmeFactory):
    def __init__(self, value):
        self.value = value


class ReturnsNothing(AcmeFactory):
    def create(self, cfg, *, client=None):
        return object()


FACTORY = AcmeFactory()
NOT_A_FACTORY = object()
'''


@pytest.fixture
def factories(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Any:
    """A fresh importable module holding the factories above (unique name per test)."""
    name = f"acme_factories_{uuid.uuid4().hex[:12]}"
    (tmp_path / f"{name}.py").write_text(FACTORIES_MODULE, encoding="utf-8")
    monkeypatch.syspath_prepend(str(tmp_path))
    module = importlib.import_module(name)
    yield module
    sys.modules.pop(name, None)


def _write_module(tmp_path: Path, monkeypatch: pytest.MonkeyPatch, source: str) -> str:
    name = f"acme_module_{uuid.uuid4().hex[:12]}"
    (tmp_path / f"{name}.py").write_text(textwrap.dedent(source), encoding="utf-8")
    monkeypatch.syspath_prepend(str(tmp_path))
    return name


def _registered(monkeypatch: pytest.MonkeyPatch, *items: Registration) -> None:
    monkeypatch.setattr(registry, "_entry_point_registrations", lambda: list(items))
    registry.reset_adapters()


# =========================================================================== registrations


def test_pyproject_registers_the_adapters_and_the_extras() -> None:
    tomllib = pytest.importorskip("tomllib")
    project = tomllib.loads((REPO_ROOT / "pyproject.toml").read_text(encoding="utf-8"))["project"]
    group = project["entry-points"][registry.ENTRY_POINT_GROUP]
    assert group == {
        "airflow": "dag_healer.airflow.adapter:FACTORY",
        "stepfunctions": "dag_healer.stepfunctions:FACTORY",
        "fake": "dag_healer.orchestrators.fake:FACTORY",
    }
    assert group == registry.BUILTIN_TYPES
    assert project["entry-points"]["airflow.plugins"] == {
        "dag_healer": "dag_healer.airflow.plugin:HealerPlugin"
    }
    extras = project["optional-dependencies"]
    assert extras["airflow"] == ["apache-airflow>=3.1"]
    assert extras["aws"] == ["boto3>=1.34"]
    assert extras["serve"] == ["uvicorn>=0.29", "PyJWT[crypto]>=2.8"]
    assert extras["dev"] == [
        "pytest>=8",
        "aiosmtpd",
        "ruff",
        "boto3>=1.34",
        "uvicorn>=0.29",
        "PyJWT[crypto]>=2.8",
    ]
    assert registry.BUILTIN_EXTRAS == {"stepfunctions": "aws"}


def test_builtin_types_are_known_without_package_metadata(monkeypatch: pytest.MonkeyPatch) -> None:
    _registered(monkeypatch)
    items = registry.registrations()
    assert [(item.type, item.distribution, item.builtin) for item in items] == [
        ("airflow", "dag-healer", True),
        ("fake", "dag-healer", True),
        ("stepfunctions", "dag-healer", True),
    ]
    assert registry.available_types() == registry.BUILTIN_TYPES


def test_installed_metadata_takes_precedence_over_the_builtin_table(monkeypatch: pytest.MonkeyPatch) -> None:
    _registered(monkeypatch, Registration("fake", "dag_healer.orchestrators.fake:FACTORY", "dag_healer"))
    items = {item.type: item for item in registry.registrations()}
    assert items["fake"].builtin is False and items["fake"].distribution == "dag_healer"
    assert items["airflow"].builtin is True
    assert not registry.conflicts()


def test_entry_points_are_read_with_their_distribution(monkeypatch: pytest.MonkeyPatch) -> None:
    def entry_points(*, group: str) -> list[Any]:
        assert group == "dag_healer.orchestrators"
        dist = SimpleNamespace(name="acme-adapters")
        nameless = SimpleNamespace(name=None, metadata={"Name": "beta-adapters"})
        return [
            SimpleNamespace(name="acme", value=" acme.adapter:FACTORY ", dist=dist),
            SimpleNamespace(name="beta", value="beta:FACTORY", dist=nameless),
            SimpleNamespace(name="gamma", value="gamma:FACTORY", dist=None),
        ]

    monkeypatch.setattr(importlib.metadata, "entry_points", entry_points)
    items = {item.type: item for item in registry.registrations()}
    assert (items["acme"].target, items["acme"].distribution) == ("acme.adapter:FACTORY", "acme-adapters")
    assert items["beta"].distribution == "beta-adapters"
    assert items["gamma"].distribution == "unknown distribution"
    assert {"airflow", "fake", "stepfunctions"} <= set(items)


def test_unreadable_metadata_leaves_the_builtin_types(
    monkeypatch: pytest.MonkeyPatch, caplog: pytest.LogCaptureFixture
) -> None:
    def broken(**_kwargs: Any) -> list[Any]:
        raise RuntimeError("bad METADATA")

    monkeypatch.setattr(importlib.metadata, "entry_points", broken)
    with caplog.at_level(logging.WARNING, logger="dag_healer.orchestrators.registry"):
        assert set(registry.available_types()) == {"airflow", "fake", "stepfunctions"}
    assert "bad METADATA" in caplog.text


def test_a_name_registered_twice_is_an_error_naming_both(monkeypatch: pytest.MonkeyPatch) -> None:
    _registered(monkeypatch, Registration("airflow", "acme.airflow:FACTORY", "acme-airflow"))
    with pytest.raises(AdapterLoadError) as raised:
        registry.available_types()
    message = str(raised.value)
    assert "acme-airflow (acme.airflow:FACTORY)" in message
    assert "dag-healer (dag_healer.airflow.adapter:FACTORY)" in message
    with pytest.raises(AdapterLoadError, match="acme-airflow"):
        registry.load_factory("airflow")
    assert list(registry.conflicts()) == ["airflow"]
    assert registry.load_factory("fake") is fake.FACTORY  # other types still load


def test_a_third_party_type_is_listed_and_loaded(monkeypatch: pytest.MonkeyPatch, factories: Any) -> None:
    _registered(monkeypatch, Registration("acme", f"{factories.__name__}:FACTORY", "acme-adapters"))
    assert registry.available_types()["acme"] == f"{factories.__name__}:FACTORY"
    assert registry.load_factory("acme") is factories.FACTORY


# =========================================================================== load_factory


def test_load_factory_resolves_names_and_explicit_targets(factories: Any) -> None:
    assert registry.load_factory("fake") is fake.FACTORY
    assert registry.load_factory(" fake ") is fake.FACTORY
    assert registry.load_factory("dag_healer.orchestrators.fake:FACTORY") is fake.FACTORY
    assert registry.load_factory(f"{factories.__name__}:FACTORY") is factories.FACTORY
    made = registry.load_factory(f"{factories.__name__}:AcmeFactory")  # a class is instantiated
    assert isinstance(made, factories.AcmeFactory) and made is not factories.FACTORY
    assert registry.load_factory(f"{factories.__name__}:AcmeFactory") is made  # cached


def test_load_factory_caches_until_reset(monkeypatch: pytest.MonkeyPatch, factories: Any) -> None:
    target = f"{factories.__name__}:AcmeFactory"
    first = registry.load_factory(target)
    assert registry.load_factory(target) is first
    registry.reset_adapters()
    assert registry.load_factory(target) is not first


@pytest.mark.parametrize(
    ("type_", "message"),
    [
        ("", "orchestrator.type is empty"),
        ("dagster", "unknown orchestrator type 'dagster' (registered types: airflow, fake, stepfunctions)"),
        ("not a target:", "invalid orchestrator type"),
        ("pkg.mod:", "invalid orchestrator type"),
        ("dag_healer.no_such_adapter:FACTORY", "module 'dag_healer.no_such_adapter' is not installed"),
        ("no_such_package_xyz.adapter:FACTORY", "module 'no_such_package_xyz.adapter' is not installed"),
        ("dag_healer.orchestrators.fake:NO_SUCH_FACTORY", "has no attribute 'NO_SUCH_FACTORY'"),
        ("dag_healer.orchestrators.fake:FakeWorld", "is not an adapter factory (missing: type, display_name"),
    ],
)
def test_load_factory_errors(monkeypatch: pytest.MonkeyPatch, type_: str, message: str) -> None:
    _registered(monkeypatch)
    with pytest.raises(AdapterLoadError) as raised:
        registry.load_factory(type_)
    assert message in str(raised.value)


def test_load_factory_reports_objects_that_are_not_factories(factories: Any) -> None:
    with pytest.raises(AdapterLoadError, match="is not an adapter factory"):
        registry.load_factory(f"{factories.__name__}:NOT_A_FACTORY")
    with pytest.raises(AdapterLoadError, match="cannot be instantiated without arguments"):
        registry.load_factory(f"{factories.__name__}:NeedsArguments")


def test_load_factory_reports_import_failures(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    unmapped = _write_module(
        tmp_path, monkeypatch, "import dag_healer_test_missing_dependency\nFACTORY = 1\n"
    )
    with pytest.raises(AdapterLoadError) as raised:
        registry.load_factory(f"{unmapped}:FACTORY")
    assert "needs the Python module 'dag_healer_test_missing_dependency'" in str(raised.value)
    assert "install the distribution that provides" in str(raised.value)

    monkeypatch.setitem(registry.EXTRA_MODULES, "aws", ("dag_healer_test_aws_sdk",))
    mapped = _write_module(tmp_path, monkeypatch, "import dag_healer_test_aws_sdk.client\nFACTORY = 1\n")
    with pytest.raises(AdapterLoadError, match=r"pip install 'dag-healer\[aws\]'"):
        registry.load_factory(f"{mapped}:FACTORY")

    exploding = _write_module(tmp_path, monkeypatch, "raise RuntimeError('boom at import')\n")
    with pytest.raises(AdapterLoadError, match="importing .* failed: RuntimeError: boom at import"):
        registry.load_factory(f"{exploding}:FACTORY")


def test_a_builtin_type_checks_its_extra_first(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setitem(registry.BUILTIN_EXTRAS, "fake", "aws")
    monkeypatch.setitem(registry.EXTRA_MODULES, "aws", ("dag_healer_test_no_boto",))
    _registered(monkeypatch)
    with pytest.raises(AdapterLoadError) as raised:
        registry.load_factory("fake")
    assert str(raised.value) == (
        "orchestrator type 'fake' needs the 'aws' extra (module 'dag_healer_test_no_boto' is not installed): "
        "pip install 'dag-healer[aws]'"
    )


def test_a_builtin_type_without_its_module(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setitem(registry.BUILTIN_TYPES, "ghost", "dag_healer.no_such_adapter:FACTORY")
    _registered(monkeypatch)
    with pytest.raises(AdapterLoadError) as raised:
        registry.load_factory("ghost")
    assert str(raised.value) == (
        "orchestrator type 'ghost': module 'dag_healer.no_such_adapter' is not installed "
        "(this dag-healer version does not include it)"
    )


def test_the_stepfunctions_entry_without_its_module_is_reported() -> None:
    if importlib.util.find_spec("dag_healer.stepfunctions") is not None:
        pytest.skip("dag_healer.stepfunctions exists (step 8 landed): covered by its own tests")
    with pytest.raises(AdapterLoadError, match="'dag_healer.stepfunctions' is not installed"):
        registry.load_factory("stepfunctions")


def test_a_factory_reporting_another_type_is_logged(
    monkeypatch: pytest.MonkeyPatch, caplog: pytest.LogCaptureFixture
) -> None:
    _registered(monkeypatch, Registration("alias", "dag_healer.orchestrators.fake:FACTORY", "aliases"))
    with caplog.at_level(logging.WARNING, logger="dag_healer.orchestrators.registry"):
        assert registry.load_factory("alias") is fake.FACTORY
    assert "registered as orchestrator type 'alias' reports type 'fake'" in caplog.text


# =========================================================================== settings and options


def test_orchestrator_settings_defaults_and_sections(make_config: MakeConfig) -> None:
    assert registry.orchestrator_settings(HealerConfig()) == OrchestratorSettings("airflow", "airflow", {})
    assert registry.orchestrator_settings(SimpleNamespace(orchestrator=None)) == OrchestratorSettings()
    configured = registry.orchestrator_settings(
        make_config({"type": "fake", "options": {"base_url": "http://x"}})
    )
    assert configured == OrchestratorSettings("fake", "fake", {"base_url": "http://x"})
    assert (
        registry.orchestrator_settings(make_config({"type": "fake", "instance": "fake-eu"})).instance
        == "fake-eu"
    )
    as_mapping = registry.orchestrator_settings(
        SimpleNamespace(orchestrator={"type": " fake ", "instance": ""})
    )
    assert as_mapping == OrchestratorSettings("fake", "fake", {})
    section = SimpleNamespace(type="x.y:FACTORY", instance=None, instance_id="computed", options=None)
    assert registry.orchestrator_settings(SimpleNamespace(orchestrator=section)).instance == "computed"
    with pytest.raises(AdapterOptionsError, match="must be a mapping"):
        registry.orchestrator_settings(SimpleNamespace(orchestrator={"type": "fake", "options": ["world"]}))


def test_parse_options_validates_against_the_options_model(factories: Any) -> None:
    options = registry.parse_options(fake.FACTORY, {"world": "w.yaml", "base_url": "https://ff.test/"})
    assert (options.world, options.base_url) == ("w.yaml", "https://ff.test")
    assert registry.parse_options(fake.FACTORY, None) == fake.FakeOptions()
    with pytest.raises(AdapterOptionsError) as raised:
        registry.parse_options(fake.FACTORY, {"wrold": "typo.yaml", "base_url": "ftp://x"})
    message = str(raised.value)
    assert message.startswith("Invalid orchestrator.options for orchestrator type 'fake':")
    assert "  - orchestrator.options.wrold:" in message and "  - orchestrator.options.base_url:" in message
    assert registry.parse_options(factories.FACTORY, {}) is None
    with pytest.raises(AdapterOptionsError, match=r"takes no orchestrator.options \(got: a, b\)"):
        registry.parse_options(factories.FACTORY, {"b": 1, "a": 2})
    assert isinstance(raised.value, ValueError) and isinstance(raised.value, AdapterLoadError)
    assert registry.secret_option_paths(fake.FACTORY) == ("orchestrator.options.token",)
    assert registry.is_target("pkg.mod:factory") and not registry.is_target("fake")


# =========================================================================== get_adapter


def test_get_adapter_builds_and_caches_one_adapter_per_configuration(
    make_config: MakeConfig, tmp_path: Path
) -> None:
    cfg = make_config({"type": "fake"})
    adapter = registry.get_adapter(cfg)
    assert isinstance(adapter, FakeAdapter) and isinstance(adapter, OrchestratorAdapter)
    assert registry.get_adapter(cfg) is adapter
    assert registry.get_adapter(make_config({"type": "fake"})) is adapter  # same content, same adapter
    other = registry.get_adapter(make_config({"type": "fake"}, repo=tmp_path / "elsewhere"))
    assert other is not adapter
    assert registry.get_adapter(make_config({"type": "fake", "instance": "fake-2"})).instance == "fake-2"


def test_get_adapter_follows_the_configuration_file(make_config: MakeConfig, tmp_path: Path) -> None:
    healer_yaml = tmp_path / "healer.yaml"
    healer_yaml.write_text("version: 1\n", encoding="utf-8")
    cfg = make_config({"type": "fake"}, source_path=healer_yaml)
    first = registry.get_adapter(cfg)
    assert registry.get_adapter(cfg) is first
    stat = healer_yaml.stat()
    os.utime(healer_yaml, ns=(stat.st_atime_ns, stat.st_mtime_ns + 5_000_000_000))
    assert registry.get_adapter(cfg) is not first


def test_get_adapter_with_a_client_builds_a_new_uncached_adapter(make_config: MakeConfig) -> None:
    cfg = make_config({"type": "fake"})
    world = FakeWorld()
    first = registry.get_adapter(cfg, client=world)
    second = registry.get_adapter(cfg, client=world)
    assert first is not second and first.world is world  # type: ignore[attr-defined]
    assert registry.get_adapter(cfg) is not first


def test_get_adapter_checks_the_factory_and_the_type(make_config: MakeConfig, factories: Any) -> None:
    with pytest.raises(AdapterLoadError, match="returned object, which is not an OrchestratorAdapter"):
        registry.get_adapter(make_config({"type": f"{factories.__name__}:ReturnsNothing"}))
    with pytest.raises(AdapterLoadError, match="unknown orchestrator type 'nope'"):
        registry.get_adapter(make_config({"type": "nope"}))
    adapter = registry.get_adapter(make_config({"type": f"{factories.__name__}:FACTORY"}), client="transport")
    assert adapter.instance == "acme" and factories.CREATED[-1][1] == "transport"


def test_reset_closes_the_cached_adapters_and_the_cache_is_bounded(
    make_config: MakeConfig, tmp_path: Path
) -> None:
    adapters = [
        registry.get_adapter(make_config({"type": "fake"}, repo=tmp_path / f"r{i}")) for i in range(20)
    ]
    assert len(registry._adapters) == 16
    registry.reset_adapters()
    assert not registry._adapters
    assert all(adapter.closed for adapter in adapters[-16:])  # type: ignore[attr-defined]
    assert not any(adapter.closed for adapter in adapters[:4])  # evicted, never closed: may still be in use


# =========================================================================== configuration checks


@pytest.mark.parametrize(
    "modes", [["rest", "listener"], ["rest"], ["listener"], ["callback"], ["rest", "listener", "callback"]]
)
@pytest.mark.parametrize("trigger", ["final_failure", "every_failure"])
def test_config_problems_accept_every_phase5_airflow_configuration(modes: list[str], trigger: str) -> None:
    from dag_healer.airflow.adapter import AirflowAdapter

    cfg = config_from_mapping({"monitor": {"modes": modes, "trigger": trigger}})
    assert registry.config_problems(cfg, AirflowAdapter(cfg)) == []


def test_config_problems_name_the_mode_or_trigger_and_the_orchestrator() -> None:
    from dag_healer.airflow.adapter import AirflowAdapter

    queue = config_from_mapping({"monitor": {"modes": ["rest", "queue"]}})
    assert registry.config_problems(queue, AirflowAdapter(queue)) == [
        "monitor.modes includes queue, but Apache Airflow has no hint queue to drain"
    ]
    portless = FakeAdapter()
    portless.feed = portless.push = None
    detection = config_from_mapping({"monitor": {"modes": ["rest", "poll", "listener", "push"]}})
    assert registry.config_problems(detection, portless) == [
        "monitor.modes includes rest, poll, but Fakeflow offers no failure feed to poll",
        "monitor.modes includes listener, push, but Fakeflow accepts no pushed failure events",
    ]
    hidden = FakeAdapter()
    hidden.capabilities = replace(fake.FAKE_CAPABILITIES, attempts_visible=False)
    every = config_from_mapping({"monitor": {"modes": ["poll", "queue"], "trigger": "every_failure"}})
    assert registry.config_problems(every, hidden) == [
        "monitor.trigger is every_failure, but Fakeflow does not show automatic retries as failed "
        "attempts: use final_failure"
    ]
    assert registry.config_problems(every, FakeAdapter()) == []
    # duck typed: no monitor section at all, or one without wants()
    assert registry.config_problems(SimpleNamespace(), hidden) == []
    plain = SimpleNamespace(monitor=SimpleNamespace(modes=["listener", "queue"], trigger="final_failure"))
    assert registry.config_problems(plain, AirflowAdapter(HealerConfig())) == [
        "monitor.modes includes queue, but Apache Airflow has no hint queue to drain"
    ]


def test_the_mode_kinds_are_the_ones_of_the_configuration() -> None:
    """The registry keeps its own copy of ``config.MODE_KINDS`` (the config module needs pydantic)."""
    from dag_healer import config

    assert registry._MODE_KINDS == dict(config.MODE_KINDS)


# =========================================================================== compat


def test_as_adapter_passes_adapters_and_none_through(make_config: MakeConfig) -> None:
    cfg = make_config()
    adapter = FakeAdapter(cfg)
    assert compat.as_adapter(cfg, None) is None
    assert compat.as_adapter(cfg, adapter) is adapter
    assert compat.is_adapter(adapter) and not compat.is_legacy_client(adapter)


def test_as_adapter_wraps_a_legacy_client(
    make_config: MakeConfig, factories: Any, monkeypatch: pytest.MonkeyPatch
) -> None:
    class PhaseFiveClient:
        def clear_task_instances(self, *args: Any, **kwargs: Any) -> dict[str, Any]:
            return {}

    monkeypatch.setattr(compat, "LEGACY_CLIENT_FACTORY", f"{factories.__name__}:FACTORY")
    cfg, client = make_config(), PhaseFiveClient()
    adapter = compat.as_adapter(cfg, client)
    assert isinstance(adapter, FakeAdapter) and factories.CREATED[-1] == (cfg, client)


def test_as_adapter_reports_a_missing_airflow_adapter(
    make_config: MakeConfig, monkeypatch: pytest.MonkeyPatch
) -> None:
    class Client:
        def get_task_log(self, *args: Any, **kwargs: Any) -> str:
            return ""

    assert compat.LEGACY_CLIENT_FACTORY == "dag_healer.airflow.adapter:FACTORY"
    monkeypatch.setattr(compat, "LEGACY_CLIENT_FACTORY", "dag_healer.no_such_adapter:FACTORY")
    with pytest.raises(AdapterLoadError, match="dag_healer.no_such_adapter"):
        compat.as_adapter(make_config(), Client())


def test_as_adapter_rejects_other_objects(make_config: MakeConfig) -> None:
    with pytest.raises(TypeError, match="got str"):
        compat.as_adapter(make_config(), "http://airflow:8080")


def test_mocks_and_partial_doubles_count_as_legacy_clients_not_adapters() -> None:
    mocked = mock.MagicMock()  # every attribute exists through __getattr__, none statically
    assert not compat.is_adapter(mocked) and compat.is_legacy_client(mocked)
    assert compat.is_legacy_client(SimpleNamespace(list_failed_task_instances=lambda **_: []))
    assert not compat.is_legacy_client(SimpleNamespace(get_task_log="not callable"))
    assert not compat.is_legacy_client(object())


def test_adapter_members_match_the_protocol() -> None:
    annotated = set(OrchestratorAdapter.__annotations__)
    methods = {
        name
        for name, value in vars(OrchestratorAdapter).items()
        if callable(value) and not name.startswith("_")
    }
    assert set(compat.ADAPTER_MEMBERS) == annotated | methods
    assert len(compat.ADAPTER_MEMBERS) == len(set(compat.ADAPTER_MEMBERS))


# =========================================================================== the package


def test_package_exports_resolve_lazily() -> None:
    assert orchestrators.get_adapter is registry.get_adapter
    assert orchestrators.as_adapter is compat.as_adapter
    assert orchestrators.ENTRY_POINT_GROUP == "dag_healer.orchestrators"
    assert set(orchestrators.__all__) <= set(dir(orchestrators))
    for name in orchestrators.__all__:
        assert getattr(orchestrators, name) is not None
    with pytest.raises(AttributeError, match="no attribute 'nope'"):
        orchestrators.nope  # noqa: B018


def test_package_registry_and_compat_import_light() -> None:
    code = (
        "import json, sys; before = set(sys.modules); import dag_healer.orchestrators; "
        "package = sorted(set(sys.modules) - before); "
        "import dag_healer.orchestrators.registry, dag_healer.orchestrators.compat; "
        "print(json.dumps([package, sorted(set(sys.modules) - before)]))"
    )
    result = subprocess.run([sys.executable, "-c", code], capture_output=True, text=True, check=True)
    package, loaded = json.loads(result.stdout)
    own = [name for name in package if name.split(".")[0] not in sys.stdlib_module_names]
    assert own == ["dag_healer", "dag_healer._version", "dag_healer.orchestrators"]
    third_party = [
        name for name in loaded if name.split(".")[0] not in (*sys.stdlib_module_names, "dag_healer")
    ]
    assert third_party == []
    assert not [name for name in loaded if name.startswith(("dag_healer.airflow", "dag_healer.config"))]
