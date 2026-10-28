"""CLI commands other than init: check, doctor, contracts, incidents and worker."""

from __future__ import annotations

import json
import logging
import shutil
import sys
from collections.abc import Callable, Iterator, Mapping
from pathlib import Path
from typing import TYPE_CHECKING, Any

import httpx
import pytest
import yaml

from dag_healer.cli import doctor_cmd
from dag_healer.cli._output import mask_url, render_table
from dag_healer.cli.main import main
from dag_healer.contracts.io import contract_path, load_contract, save_contract
from dag_healer.repo.sandbox import PROCESS_BACKEND_WARNING, ContainerSandbox, SandboxProbe

if TYPE_CHECKING:
    from dag_healer.config import HealerConfig

EXAMPLE_REPO = Path(__file__).resolve().parents[2] / "example_existing_repo"
SECRET = "sk-ant-do-not-print-me-123"
#: A sandbox image of the doctor tests (no engine is ever asked about it).
SANDBOX_IMAGE = "registry.example.com/etl-tests:1.4"


@pytest.fixture
def isolated(healer_env: Path, monkeypatch: pytest.MonkeyPatch) -> Path:
    """Isolated env; also undoes the HEALER_CONFIG export done by main(-c ...)."""
    monkeypatch.setenv("HEALER_CONFIG", "")
    monkeypatch.delenv("HEALER_CONFIG")
    for name in (
        "ANTHROPIC_API_KEY",
        "HEALER_LLM_PROVIDER",
        "HEALER_LLM_MODEL",
        "HEALER_SMTP_HOST",
        "HEALER_AIRFLOW_USERNAME",
        "HEALER_AIRFLOW_PASSWORD",
        "HEALER_SANDBOX_IMAGE",
        "GITHUB_TOKEN",
        "GITHUB_REPO",
    ):
        monkeypatch.delenv(name, raising=False)
    yield healer_env
    from dag_healer.store import dispose_engines

    dispose_engines()


@pytest.fixture
def repo(tmp_path: Path, isolated: Path, capsys: pytest.CaptureFixture[str]) -> Path:
    """An initialised copy of example_existing_repo."""
    target = tmp_path / "repo"
    shutil.copytree(EXAMPLE_REPO, target, ignore=shutil.ignore_patterns("__pycache__", "processed"))
    assert main(["init", "--repo", str(target)]) == 0
    capsys.readouterr()
    return target


def _cfg_args(repo: Path) -> list[str]:
    return ["-c", str(repo / "healer.yaml")]


def _broken_sqlite_url(tmp_path: Path) -> str:
    blocker = tmp_path / "not-a-dir"
    blocker.write_text("x", encoding="utf-8")
    return f"sqlite:///{(blocker / 'healer.db').as_posix()}"


# --------------------------------------------------------------------------- helpers


def test_render_table_aligns_columns() -> None:
    table = render_table(("A", "LONG_HEADER"), [("x", None), ("yyyy", True)])
    lines = table.splitlines()
    assert lines[0] == "A     LONG_HEADER"
    assert lines[2] == "x     -"
    assert lines[3] == "yyyy  yes"


@pytest.mark.parametrize(
    ("url", "expected"),
    [
        (
            "postgresql+psycopg2://airflow:s3cret@postgres/healer",
            "postgresql+psycopg2://airflow:***@postgres/healer",
        ),
        ("sqlite:///C:/x/healer.db", "sqlite:///C:/x/healer.db"),
        ("postgresql://user@host/db", "postgresql://user@host/db"),
        (None, None),
    ],
)
def test_mask_url(url: str | None, expected: str | None) -> None:
    assert mask_url(url) == expected


# --------------------------------------------------------------------------- check


def test_check_prints_settings_with_secrets_masked(
    tmp_path: Path, isolated: Path, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    config_file = tmp_path / "healer.yaml"
    config_file.write_text(
        "version: 1\n"
        "github: {repo: acme/etl, token: ghp_literal_token_value}\n"
        "airflow: {username: admin, password: '${HEALER_AIRFLOW_PASSWORD}'}\n"
        "store: {url: 'postgresql://healer:pw-in-url@db/healer'}\n",
        encoding="utf-8",
    )
    monkeypatch.setenv("HEALER_AIRFLOW_PASSWORD", SECRET)
    assert main(["check", "--config", str(config_file)]) == 0
    captured = capsys.readouterr()
    assert "ghp_literal_token_value" not in captured.out
    assert SECRET not in captured.out
    assert "pw-in-url" not in captured.out
    assert "token: '***'" in captured.out
    assert "postgresql://healer:***@db/healer" in captured.out
    assert "Configuration is valid." in captured.out
    assert "github.token" in captured.err and "store.url" in captured.err
    assert "airflow.password" not in captured.err  # it is an ${ENV} reference
    assert "sandbox" not in captured.out + captured.err  # the default sandbox is not shown


def test_check_shows_a_custom_sandbox_and_warns_about_the_process_one(
    tmp_path: Path, isolated: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    config_file = tmp_path / "healer.yaml"
    config_file.write_text("version: 1\nrepo: {sandbox: {backend: process, memory: 1G}}\n", encoding="utf-8")
    assert main(["check", "--config", str(config_file)]) == 0
    captured = capsys.readouterr()
    shown = yaml.safe_load(captured.out.split("\nConfiguration is valid.")[0])
    assert shown["repo"]["sandbox"] == {
        "backend": "process",
        "engine": "docker",
        "image": None,
        "python": "python",
        "user": None,
        "memory": "1g",
        "cpus": 2.0,
        "pids": 512,
        "work_size": "1g",
        "tmp_size": "512m",
    }
    assert captured.err == f"warning: {PROCESS_BACKEND_WARNING}\n"


def test_check_reports_invalid_configuration(
    tmp_path: Path, isolated: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    config_file = tmp_path / "healer.yaml"
    config_file.write_text("limits: {max_fix_attempts: 0}\n", encoding="utf-8")
    assert main(["-c", str(config_file), "check"]) == 1
    assert "limits.max_fix_attempts" in capsys.readouterr().err
    assert main(["check", "-c", str(tmp_path / "missing.yaml")]) == 1
    assert "not found" in capsys.readouterr().err


# --------------------------------------------------------------------------- doctor


class _Response:
    def __init__(self, status_code: int, body: Any = None) -> None:
        self.status_code = status_code
        self._body = body

    def json(self) -> Any:
        if self._body is None:
            raise ValueError("no body")
        return self._body


def _healthy(url: str) -> _Response:
    assert url.endswith("/api/v2/monitor/health")
    return _Response(200, {"metadatabase": {"status": "healthy"}, "scheduler": {"status": "healthy"}})


@pytest.fixture
def healthy_network(monkeypatch: pytest.MonkeyPatch) -> list[tuple[str, int]]:
    """Fake Airflow API and SMTP server; returns the SMTP connections attempted."""
    connections: list[tuple[str, int]] = []

    class _Connection:
        def __enter__(self) -> _Connection:
            return self

        def __exit__(self, *exc: object) -> None:
            return None

    def fake_connect(address: tuple[str, int], timeout: float) -> _Connection:
        assert timeout == doctor_cmd.NETWORK_TIMEOUT_SECONDS
        connections.append(address)
        return _Connection()

    monkeypatch.setattr(doctor_cmd, "_http_get", _healthy)
    monkeypatch.setattr(doctor_cmd, "_http_post", lambda url, payload: _Response(201, {"access_token": "t"}))
    monkeypatch.setattr(doctor_cmd.socket, "create_connection", fake_connect)
    return connections


def _doctor_lines(out: str) -> dict[str, str]:
    """Map check name -> label from lines like ``[ OK ] ui bundle   detail``."""
    lines: dict[str, str] = {}
    for line in out.splitlines():
        if line[:1] == "[" and line[5:6] == "]":
            name = line[7:].split("  ", 1)[0].strip()
            lines[name] = line[:6]
    return lines


@pytest.fixture
def working_sandbox(monkeypatch: pytest.MonkeyPatch) -> SandboxProbe:
    """A container sandbox whose engine, image and smoke run are fine (no engine is asked)."""
    probe = SandboxProbe(
        True,
        f"docker 29.8.2, image {SANDBOX_IMAGE}: uid 10001 (sandbox), no network (interfaces: lo), "
        "pytest 8.4.2 on Python 3.12.12",
    )
    monkeypatch.setattr(ContainerSandbox, "unavailable_reason", lambda self: None)
    monkeypatch.setattr(ContainerSandbox, "smoke_test", lambda self: probe)
    return probe


def test_doctor_all_good(
    repo: Path,
    healthy_network: list[tuple[str, int]],
    working_sandbox: SandboxProbe,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    monkeypatch.setenv("ANTHROPIC_API_KEY", SECRET)
    monkeypatch.setenv("HEALER_AIRFLOW_USERNAME", "admin")
    monkeypatch.setenv("HEALER_AIRFLOW_PASSWORD", "admin-password")
    monkeypatch.setenv("HEALER_SMTP_HOST", "mailpit")
    monkeypatch.setenv("HEALER_SMTP_PORT", "1025")
    monkeypatch.setenv("HEALER_INGEST_TOKEN", "ingest-token-value")
    monkeypatch.setenv("HEALER_SANDBOX_IMAGE", SANDBOX_IMAGE)  # the template's repo.sandbox.image
    assert main([*_cfg_args(repo), "doctor"]) == 0
    out = capsys.readouterr().out
    assert SECRET not in out and "admin-password" not in out and "ingest-token-value" not in out
    status = _doctor_lines(out)
    for name in (
        "config",
        "repository",
        "store",
        "ui bundle",
        "llm",
        "sandbox",
        "orchestrator",
        "airflow api",
        "airflow auth",
        "smtp",
    ):
        assert status[name] == "[ OK ]", (name, out)
    assert _line(out, "sandbox").endswith(f"  {working_sandbox.detail}")
    assert status["contracts"] == "[WARN]"  # drafts only: nothing enforced yet
    assert healthy_network == [("mailpit", 1025)]
    assert "0 error(s)" in out


def test_doctor_reports_errors_with_exit_code_1(
    repo: Path, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    def unreachable(url: str) -> Any:
        raise ConnectionError("connection refused")

    def no_smtp(address: tuple[str, int], timeout: float) -> Any:
        raise OSError("connection refused")

    monkeypatch.setattr(doctor_cmd, "_http_get", unreachable)
    monkeypatch.setattr(doctor_cmd.socket, "create_connection", no_smtp)
    monkeypatch.setenv("HEALER_SMTP_HOST", "smtp.invalid")
    assert main([*_cfg_args(repo), "doctor"]) == 1
    status = _doctor_lines(capsys.readouterr().out)
    assert status["airflow api"] == "[FAIL]"
    assert status["llm"] == "[FAIL]"  # ANTHROPIC_API_KEY missing
    assert status["smtp"] == "[FAIL]"
    assert status["airflow auth"] == "[WARN]"  # no credentials configured


def test_doctor_warns_about_literal_secrets_and_missing_bundle(
    tmp_path: Path,
    isolated: Path,
    healthy_network: list[tuple[str, int]],
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    (tmp_path / "dags").mkdir()
    config_file = tmp_path / "healer.yaml"
    config_file.write_text(
        "llm: {provider: fake}\nairflow: {username: admin, password: literal-pw}\n", encoding="utf-8"
    )
    monkeypatch.setattr(doctor_cmd, "bundle_path", lambda: tmp_path / "missing.umd.cjs")
    assert main(["doctor", "--config", str(config_file)]) == 1
    out = capsys.readouterr().out
    assert "literal-pw" not in out
    status = _doctor_lines(out)
    assert status["secrets"] == "[WARN]"
    assert "airflow.password" in out
    assert status["ui bundle"] == "[FAIL]"
    assert status["llm"] == "[ OK ]"  # fake provider needs no key


def test_doctor_with_invalid_config(
    tmp_path: Path, isolated: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    config_file = tmp_path / "healer.yaml"
    config_file.write_text("nope: true\n", encoding="utf-8")
    assert main(["doctor", "-c", str(config_file)]) == 1
    status = _doctor_lines(capsys.readouterr().out)
    assert status["config"] == "[FAIL]"
    assert "ui bundle" in status


def test_doctor_store_error(
    repo: Path,
    tmp_path: Path,
    healthy_network: list[tuple[str, int]],
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    monkeypatch.setenv("HEALER_DATABASE_URL", _broken_sqlite_url(tmp_path))
    assert main([*_cfg_args(repo), "doctor"]) == 1
    assert _doctor_lines(capsys.readouterr().out)["store"] == "[FAIL]"


AIRFLOW_CHECKS = {"airflow api", "airflow auth", "airflow plugin"}
#: Adapters of the doctor tests, importable as ``doctor_adapters:<FACTORY>`` (see ``custom_adapters``).
DOCTOR_ADAPTERS = """
from dataclasses import replace

from dag_healer.core.ports import DoctorCheck
from dag_healer.orchestrators.fake import FAKE_CAPABILITIES, FakeAdapter

CLOSED = []


class BlindAdapter(FakeAdapter):
    display_name = "Blindflow"
    capabilities = replace(FAKE_CAPABILITIES, attempts_visible=False)


class BrokenChecksAdapter(FakeAdapter):
    display_name = "Brokenflow"

    def doctor_checks(self):
        def boom():
            raise RuntimeError("the check exploded")

        return [DoctorCheck("boom", boom), DoctorCheck("odd", lambda: 42)]

    def close(self):
        CLOSED.append(self.instance)


class Factory:
    options_model = None
    secret_options = ()

    def __init__(self, adapter_class):
        self.adapter_class = adapter_class
        self.type = adapter_class.__name__
        self.display_name = adapter_class.display_name

    def create(self, cfg, *, client=None):
        return self.adapter_class(cfg, instance=cfg.orchestrator.instance_id)


BLIND = Factory(BlindAdapter)
BROKEN = Factory(BrokenChecksAdapter)
"""


@pytest.fixture
def custom_adapters(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Iterator[Any]:
    """The ``doctor_adapters`` module above, importable for this test."""
    import importlib

    from dag_healer.orchestrators import registry

    folder = tmp_path / "adapters"
    folder.mkdir()
    (folder / "doctor_adapters.py").write_text(DOCTOR_ADAPTERS, encoding="utf-8")
    monkeypatch.syspath_prepend(str(folder))
    monkeypatch.delitem(sys.modules, "doctor_adapters", raising=False)
    yield importlib.import_module("doctor_adapters")
    registry.reset_adapters()


def _doctor(
    tmp_path: Path, config: str, capsys: pytest.CaptureFixture[str], *, sandbox: str = "{backend: process}"
) -> tuple[int, str]:
    """``dag-healer doctor`` on a fake-LLM configuration (by default with the process sandbox:
    the unit tests have no container engine)."""
    (tmp_path / "dags").mkdir(exist_ok=True)
    config_file = tmp_path / "healer.yaml"
    config_file.write_text(
        f"llm: {{provider: fake}}\nrepo: {{sandbox: {sandbox}}}\n" + config, encoding="utf-8"
    )
    code = main(["doctor", "-c", str(config_file)])
    return code, capsys.readouterr().out


def _line(out: str, name: str) -> str:
    return next(line for line in out.splitlines() if line[7:].startswith(f"{name} "))


def test_doctor_composes_the_generic_and_the_adapter_checks(
    repo: Path, healthy_network: list[tuple[str, int]], capsys: pytest.CaptureFixture[str]
) -> None:
    from dag_healer.airflow.adapter import AirflowAdapter
    from dag_healer.config import load_config

    main([*_cfg_args(repo), "doctor"])
    out = capsys.readouterr().out
    adapter = AirflowAdapter(load_config(repo / "healer.yaml"))
    adapter_checks = [check.name for check in adapter.doctor_checks()]
    assert set(adapter_checks) == AIRFLOW_CHECKS
    assert list(_doctor_lines(out)) == [
        *("config", "repository", "contracts", "store", "ui bundle", "llm", "sandbox", "orchestrator"),
        *adapter_checks,
        *("smtp", "secrets", "ingest", "github"),
    ]
    assert _line(out, "orchestrator").endswith("  Apache Airflow (type airflow, instance airflow)")


def test_doctor_runs_the_checks_of_another_orchestrator(
    tmp_path: Path, isolated: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    code, out = _doctor(tmp_path, "orchestrator: {type: fake}\n", capsys)
    status = _doctor_lines(out)
    assert code == 0, out
    assert _line(out, "orchestrator").endswith("  Fakeflow (type fake, instance fake)")
    assert status["fakeflow"] == "[ OK ]"
    assert not AIRFLOW_CHECKS & set(status)


def test_doctor_reports_an_orchestrator_that_cannot_load(
    tmp_path: Path, isolated: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    config = "orchestrator: {type: 'no_such_pkg.adapter:FACTORY', instance: elsewhere}\n"
    code, out = _doctor(tmp_path, config, capsys)
    status = _doctor_lines(out)
    assert code == 1
    assert status["orchestrator"] == "[FAIL]"
    assert "module 'no_such_pkg.adapter' is not installed" in _line(out, "orchestrator")
    assert not AIRFLOW_CHECKS & set(status)
    assert status["github"] == "[ OK ]"  # the checks after it still ran


def test_doctor_rejects_a_mode_the_orchestrator_cannot_serve(
    tmp_path: Path, isolated: Path, healthy_network: list[tuple[str, int]], capsys: pytest.CaptureFixture[str]
) -> None:
    code, out = _doctor(tmp_path, "monitor: {modes: [rest, queue]}\n", capsys)
    status = _doctor_lines(out)
    assert code == 1
    assert status["orchestrator"] == "[FAIL]"
    assert "monitor.modes includes queue, but Apache Airflow" in _line(out, "orchestrator")
    assert status["airflow api"] == "[ OK ]"  # the adapter's own checks still run


def test_doctor_rejects_every_failure_without_visible_attempts(
    tmp_path: Path, isolated: Path, custom_adapters: Any, capsys: pytest.CaptureFixture[str]
) -> None:
    config = "orchestrator: {type: 'doctor_adapters:BLIND', instance: blind}\n"
    code, out = _doctor(tmp_path, config + "monitor: {trigger: every_failure}\n", capsys)
    assert code == 1
    line = _line(out, "orchestrator")
    assert line.startswith("[FAIL]") and "every_failure" in line and "Blindflow" in line
    assert _doctor_lines(out)["fakeflow"] == "[ OK ]"


def test_doctor_survives_broken_adapter_checks(
    tmp_path: Path, isolated: Path, custom_adapters: Any, capsys: pytest.CaptureFixture[str]
) -> None:
    config = "orchestrator: {type: 'doctor_adapters:BROKEN', instance: broken}\n"
    code, out = _doctor(tmp_path, config, capsys)
    status = _doctor_lines(out)
    assert code == 1
    assert status["orchestrator"] == "[ OK ]"
    assert _line(out, "boom").endswith("  RuntimeError: the check exploded")
    assert _line(out, "odd").endswith("  the check returned int, not a CheckResult")
    assert status["boom"] == status["odd"] == "[FAIL]"
    assert status["github"] == "[ OK ]"
    assert custom_adapters.CLOSED == ["broken"]


FAKEFLOW = "orchestrator: {type: fake}\n"


def test_doctor_warns_that_the_process_sandbox_isolates_nothing(
    tmp_path: Path, isolated: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    code, out = _doctor(tmp_path, FAKEFLOW, capsys)
    assert code == 0, out
    line = _line(out, "sandbox")
    assert line.startswith("[WARN]") and line.endswith(f"  {PROCESS_BACKEND_WARNING}")


def test_doctor_fails_a_container_sandbox_without_an_image(
    tmp_path: Path, isolated: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    code, out = _doctor(tmp_path, FAKEFLOW, capsys, sandbox="{backend: container}")
    assert code == 1
    line = _line(out, "sandbox")
    assert line.startswith("[FAIL]") and "repo.sandbox.image is not set" in line
    assert "HEALER_SANDBOX_IMAGE" in line  # the fix
    assert _doctor_lines(out)["github"] == "[ OK ]"  # the checks after it still ran


def test_doctor_fails_a_container_sandbox_whose_engine_is_missing(
    tmp_path: Path, isolated: Path, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    from dag_healer.repo import sandbox

    monkeypatch.setattr(sandbox, "engine_command", lambda engine: [str(tmp_path / "no-such-engine")])
    code, out = _doctor(tmp_path, FAKEFLOW, capsys, sandbox=f"{{image: '{SANDBOX_IMAGE}'}}")
    assert code == 1
    line = _line(out, "sandbox")
    assert line.startswith("[FAIL]") and "was not found" in line


def test_doctor_runs_the_smoke_test_of_a_usable_container_sandbox(
    tmp_path: Path, isolated: Path, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    detail = f"the sandbox of image {SANDBOX_IMAGE} has a network (interfaces: lo, eth0)"
    smoke_runs: list[ContainerSandbox] = []

    def leaky(self: ContainerSandbox) -> SandboxProbe:
        smoke_runs.append(self)
        return SandboxProbe(False, detail)

    monkeypatch.setattr(ContainerSandbox, "unavailable_reason", lambda self: None)
    monkeypatch.setattr(ContainerSandbox, "smoke_test", leaky)
    code, out = _doctor(tmp_path, FAKEFLOW, capsys, sandbox=f"{{image: '{SANDBOX_IMAGE}', engine: podman}}")
    assert code == 1
    line = _line(out, "sandbox")
    assert line.startswith("[FAIL]") and line.endswith(f"  {detail}")
    assert [(run.engine, run.image) for run in smoke_runs] == [("podman", SANDBOX_IMAGE)]


@pytest.mark.parametrize(
    ("modes", "auth", "ingest"),
    [
        (["rest", "listener"], doctor_cmd.WARN, doctor_cmd.WARN),
        (["poll"], doctor_cmd.WARN, doctor_cmd.SKIP),
        (["listener", "callback"], doctor_cmd.SKIP, doctor_cmd.WARN),
        (["push"], doctor_cmd.SKIP, doctor_cmd.WARN),
    ],
)
def test_airflow_auth_and_ingest_follow_the_detection_modes(
    isolated: Path, modes: list[str], auth: str, ingest: str
) -> None:
    from dag_healer.config import config_from_mapping

    cfg = config_from_mapping({"monitor": {"modes": modes}})
    auth_result, ingest_result = doctor_cmd.check_airflow_auth(cfg, True), doctor_cmd.check_ingest(cfg)
    assert (auth_result.status, ingest_result.status) == (auth, ingest)
    if auth == doctor_cmd.SKIP:
        assert auth_result.detail == "REST polling disabled (monitor.modes)"
    if ingest == doctor_cmd.SKIP:
        assert ingest_result.detail == "listener/callback modes disabled"


# --------------------------------------------------------------------------- doctor: the bedrock provider

#: Fictional static AWS credentials (nothing is ever sent anywhere with them).
AWS_KEY_ID = "AKIDEXAMPLE"
AWS_SECRET_KEY = "wJalrXUtnFEMI/K7MDENG+bPxRfiCYEXAMPLEKEY"
AWS_SESSION_TOKEN = "example-session-token"
BEDROCK_API_KEY = "bedrock-api-key-EXAMPLE"
BEDROCK_MODEL = "eu.anthropic.claude-opus-5-5"
#: The end of the bedrock llm line, by how the provider would authenticate.
ENV_CREDENTIALS_RESOLVE = "AWS credentials resolve (env; values hidden)"
API_KEY_SET = "AWS_BEARER_TOKEN_BEDROCK is set (value hidden)"


@pytest.fixture
def aws_env(isolated: Path, isolated_aws: Path) -> Path:
    """No AWS setting of the host (``isolated_aws`` of tests/conftest.py: no variable, no AWS file,
    no instance metadata service), in the isolated environment of these tests.

    Returns the folder of the test's ``aws-config`` and ``aws-credentials`` files (absent until a
    test writes them).
    """
    return isolated_aws


@pytest.fixture
def aws_sdk(aws_env: Path) -> Path:
    """``aws_env`` where botocore can resolve credentials (the ``aws`` extra is installed)."""
    pytest.importorskip("botocore")
    pytest.importorskip("boto3")
    return aws_env


def _write_aws_file(path: Path, text: str) -> None:
    path.write_text(text, encoding="utf-8", newline="\n")


def _env_credentials(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("AWS_ACCESS_KEY_ID", AWS_KEY_ID)
    monkeypatch.setenv("AWS_SECRET_ACCESS_KEY", AWS_SECRET_KEY)
    monkeypatch.setenv("AWS_SESSION_TOKEN", AWS_SESSION_TOKEN)


def _bedrock(**llm: Any) -> HealerConfig:
    """A configuration of the bedrock provider (``llm.model`` a Bedrock inference profile ID)."""
    from dag_healer.config import config_from_mapping

    return config_from_mapping({"llm": {"provider": "bedrock", "model": BEDROCK_MODEL, **llm}})


def _without_aws_secrets(text: str) -> bool:
    return not [
        value for value in (AWS_KEY_ID, AWS_SECRET_KEY, AWS_SESSION_TOKEN, BEDROCK_API_KEY) if value in text
    ]


def _no_probe(*args: Any, **kwargs: Any) -> Any:
    raise AssertionError("no AWS credential lookup expected")


def test_doctor_bedrock_line_with_credentials_from_the_environment(
    aws_sdk: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    _env_credentials(monkeypatch)
    monkeypatch.setenv("AWS_REGION", "eu-west-1")
    result = doctor_cmd.check_llm(_bedrock())
    assert (result.name, result.status) == ("llm", doctor_cmd.OK)
    assert result.detail == (
        f"provider bedrock (model eu.anthropic.claude-opus-5-5, region eu-west-1): {ENV_CREDENTIALS_RESOLVE}"
    )
    assert _without_aws_secrets(result.detail)


@pytest.mark.parametrize(
    ("llm", "env", "region"),
    [
        pytest.param(
            {"aws_region": "eu-central-1"},
            {"AWS_REGION": "us-east-1", "AWS_DEFAULT_REGION": "us-west-2"},
            "eu-central-1",
            id="llm",
        ),
        pytest.param(
            {}, {"AWS_REGION": "us-east-1", "AWS_DEFAULT_REGION": "us-west-2"}, "us-east-1", id="aws-region"
        ),
        pytest.param(
            {}, {"AWS_REGION": " ", "AWS_DEFAULT_REGION": "us-west-2"}, "us-west-2", id="default-region"
        ),
    ],
)
def test_doctor_bedrock_region_order(
    aws_sdk: Path, monkeypatch: pytest.MonkeyPatch, llm: dict[str, str], env: dict[str, str], region: str
) -> None:
    _env_credentials(monkeypatch)
    for name, value in env.items():
        monkeypatch.setenv(name, value)
    result = doctor_cmd.check_llm(_bedrock(**llm))
    assert result.status == doctor_cmd.OK
    assert result.detail.startswith(f"provider bedrock (model {BEDROCK_MODEL}, region {region}): ")


@pytest.mark.parametrize(
    "where", [pytest.param("llm", id="llm-profile"), pytest.param("env", id="aws-profile")]
)
def test_doctor_bedrock_takes_the_region_and_credentials_of_the_profile(
    aws_sdk: Path, monkeypatch: pytest.MonkeyPatch, where: str
) -> None:
    _write_aws_file(aws_sdk / "aws-config", "[profile healer-dev]\nregion = eu-west-3\n")
    _write_aws_file(
        aws_sdk / "aws-credentials",
        f"[healer-dev]\naws_access_key_id = {AWS_KEY_ID}\naws_secret_access_key = {AWS_SECRET_KEY}\n",
    )
    if where == "env":
        monkeypatch.setenv("AWS_PROFILE", "healer-dev")
        cfg = _bedrock()
    else:
        cfg = _bedrock(aws_profile="healer-dev")
    result = doctor_cmd.check_llm(cfg)
    assert (result.status, result.detail) == (
        doctor_cmd.OK,
        f"provider bedrock (model {BEDROCK_MODEL}, region eu-west-3): "
        "AWS credentials resolve (shared-credentials-file; values hidden)",
    )
    assert _without_aws_secrets(result.detail)


def test_doctor_bedrock_without_a_region(aws_sdk: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    from dag_healer.agent import bedrock

    _env_credentials(monkeypatch)
    result = doctor_cmd.check_llm(_bedrock())
    assert (result.status, result.detail) == (
        doctor_cmd.ERROR,
        f"provider bedrock (model {BEDROCK_MODEL}, region unset): {bedrock.NO_REGION}",
    )


def test_doctor_bedrock_without_credentials(aws_sdk: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """No variable, no AWS file and no instance metadata service (disabled): nothing resolves."""
    from dag_healer.agent import bedrock

    monkeypatch.setenv("AWS_REGION", "eu-west-1")
    result = doctor_cmd.check_llm(_bedrock())
    assert (result.status, result.detail) == (
        doctor_cmd.ERROR,
        f"provider bedrock (model {BEDROCK_MODEL}, region eu-west-1): {bedrock.NO_CREDENTIALS}",
    )


@pytest.mark.parametrize(
    "model", [pytest.param(None, id="default"), pytest.param("claude-sonnet-5-5", id="sonnet")]
)
def test_doctor_bedrock_refuses_a_claude_api_model_id(
    aws_sdk: Path, monkeypatch: pytest.MonkeyPatch, model: str | None
) -> None:
    from dag_healer.agent import bedrock
    from dag_healer.config import config_from_mapping

    _env_credentials(monkeypatch)
    monkeypatch.setenv("AWS_REGION", "eu-west-1")
    llm = {"provider": "bedrock"} if model is None else {"provider": "bedrock", "model": model}
    shown = model or "claude-opus-5-5"
    result = doctor_cmd.check_llm(config_from_mapping({"llm": llm}))
    assert (result.status, result.detail) == (
        doctor_cmd.ERROR,
        f"provider bedrock (model {shown}, region eu-west-1): {bedrock.model_alias_problem(shown)}",
    )
    assert "docs/bedrock.md" in result.detail


def test_doctor_bedrock_names_every_problem_in_order(aws_sdk: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    from dag_healer.agent import bedrock
    from dag_healer.config import config_from_mapping

    monkeypatch.setenv("ANTHROPIC_BEDROCK_BASE_URL", "")
    result = doctor_cmd.check_llm(config_from_mapping({"llm": {"provider": "bedrock"}}))
    problems = [
        bedrock.model_alias_problem("claude-opus-5-5"),
        bedrock.BASE_URL_EMPTY,
        bedrock.NO_REGION,
        bedrock.NO_CREDENTIALS,
    ]
    assert (result.status, result.detail) == (
        doctor_cmd.ERROR,
        "provider bedrock (model claude-opus-5-5, region unset): " + "; ".join(str(p) for p in problems),
    )


def test_doctor_bedrock_with_an_api_key(aws_sdk: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("AWS_BEARER_TOKEN_BEDROCK", BEDROCK_API_KEY)
    monkeypatch.setenv("AWS_REGION", "eu-west-1")
    monkeypatch.setattr(doctor_cmd, "_aws_credentials", _no_probe)  # the key is never resolved
    result = doctor_cmd.check_llm(_bedrock())
    assert (result.status, result.detail) == (
        doctor_cmd.OK,
        f"provider bedrock (model {BEDROCK_MODEL}, region eu-west-1): {API_KEY_SET}",
    )
    assert _without_aws_secrets(result.detail)


def test_doctor_bedrock_with_an_api_key_reads_the_region_of_the_profile(
    aws_sdk: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    _write_aws_file(aws_sdk / "aws-config", "[default]\nregion = eu-north-1\n")
    monkeypatch.setenv("AWS_BEARER_TOKEN_BEDROCK", BEDROCK_API_KEY)
    result = doctor_cmd.check_llm(_bedrock())
    assert (result.status, result.detail) == (
        doctor_cmd.OK,
        f"provider bedrock (model {BEDROCK_MODEL}, region eu-north-1): {API_KEY_SET}",
    )


def test_doctor_bedrock_treats_a_blank_api_key_as_unset(
    aws_sdk: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setenv("AWS_BEARER_TOKEN_BEDROCK", " ")
    _env_credentials(monkeypatch)
    monkeypatch.setenv("AWS_REGION", "eu-west-1")
    result = doctor_cmd.check_llm(_bedrock())
    assert result.status == doctor_cmd.OK
    assert result.detail.endswith(f": {ENV_CREDENTIALS_RESOLVE}")


@pytest.mark.parametrize(
    ("api_key", "region", "problem"),
    [
        pytest.param(BEDROCK_API_KEY, "eu-west-1", "PROFILE_AND_API_KEY", id="api-key"),
        pytest.param("", None, "EMPTY_API_KEY_WITH_PROFILE", id="empty-api-key"),
    ],
)
def test_doctor_bedrock_refuses_a_profile_next_to_an_api_key(
    aws_sdk: Path, monkeypatch: pytest.MonkeyPatch, api_key: str, region: str | None, problem: str
) -> None:
    from dag_healer.agent import bedrock

    _write_aws_file(aws_sdk / "aws-config", "[profile healer-dev]\nregion = eu-west-3\n")
    _write_aws_file(
        aws_sdk / "aws-credentials",
        f"[healer-dev]\naws_access_key_id = {AWS_KEY_ID}\naws_secret_access_key = {AWS_SECRET_KEY}\n",
    )
    monkeypatch.setenv("AWS_BEARER_TOKEN_BEDROCK", api_key)
    result = doctor_cmd.check_llm(_bedrock(aws_profile="healer-dev", aws_region=region))
    assert result.status == doctor_cmd.ERROR
    assert result.detail.endswith(f": {getattr(bedrock, problem)}")
    assert _without_aws_secrets(result.detail)


def test_doctor_bedrock_names_an_unknown_profile(aws_sdk: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("AWS_REGION", "eu-west-1")
    result = doctor_cmd.check_llm(_bedrock(aws_profile="no-such-profile"))
    assert result.status == doctor_cmd.ERROR
    assert result.detail.startswith(
        f"provider bedrock (model {BEDROCK_MODEL}, region eu-west-1): the AWS credentials for the bedrock "
        "provider cannot be resolved: ProfileNotFound: "
    )
    assert "no-such-profile" in result.detail


@pytest.mark.parametrize(
    ("base_url", "endpoint_url", "shown"),
    [
        pytest.param(
            None, "https://vpce-0123-abcd.bedrock-runtime.eu-west-1.vpce.amazonaws.com/", True, id="vpce"
        ),
        pytest.param("", "https://bedrock-gateway.internal:8443", True, id="empty-variable"),
        pytest.param("https://bedrock-gateway.internal:8443", None, False, id="variable"),
    ],
)
def test_doctor_bedrock_endpoint_overrides(
    aws_sdk: Path,
    monkeypatch: pytest.MonkeyPatch,
    base_url: str | None,
    endpoint_url: str | None,
    shown: bool,
) -> None:
    _env_credentials(monkeypatch)
    if base_url is not None:
        monkeypatch.setenv("ANTHROPIC_BEDROCK_BASE_URL", base_url)
    result = doctor_cmd.check_llm(_bedrock(aws_region="eu-west-1", endpoint_url=endpoint_url))
    assert result.status == doctor_cmd.OK, result.detail
    endpoint = f", endpoint {(endpoint_url or '').rstrip('/')}" if shown else ""
    assert result.detail == (
        f"provider bedrock (model {BEDROCK_MODEL}, region eu-west-1{endpoint}): {ENV_CREDENTIALS_RESOLVE}"
    )


def test_doctor_bedrock_names_the_aws_extra(aws_env: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    from dag_healer.agent import bedrock
    from dag_healer.config import config_from_mapping

    monkeypatch.setattr(bedrock, "missing_aws_sdk", lambda: True)
    monkeypatch.setattr(doctor_cmd, "_aws_credentials", _no_probe)
    monkeypatch.setenv("AWS_REGION", "eu-west-1")
    result = doctor_cmd.check_llm(config_from_mapping({"llm": {"provider": "bedrock"}}))
    assert (result.status, result.detail) == (
        doctor_cmd.ERROR,
        f"provider bedrock (model claude-opus-5-5, region eu-west-1): {bedrock.AWS_SDK_MISSING}",
    )
    assert result.detail.endswith('pip install "dag-healer[aws]"')


def test_doctor_checks_the_provider_the_worker_runs(aws_sdk: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    from dag_healer.config import config_from_mapping

    cfg = config_from_mapping({"llm": {"provider": "anthropic", "model": BEDROCK_MODEL}})
    monkeypatch.setenv("ANTHROPIC_API_KEY", SECRET)
    _env_credentials(monkeypatch)
    monkeypatch.setenv("AWS_REGION", "eu-west-1")
    assert (
        doctor_cmd.check_llm(cfg).detail == f"ANTHROPIC_API_KEY is set (value hidden); model {BEDROCK_MODEL}"
    )
    monkeypatch.setenv("HEALER_LLM_PROVIDER", " Bedrock ")
    result = doctor_cmd.check_llm(cfg)
    assert result.status == doctor_cmd.OK
    assert result.detail.startswith(f"provider bedrock (model {BEDROCK_MODEL}, region eu-west-1): ")
    monkeypatch.setenv("HEALER_LLM_PROVIDER", "fake")
    assert doctor_cmd.check_llm(cfg).detail == "provider 'fake' (offline); no API key needed"


@pytest.mark.parametrize(
    "cfg", [pytest.param(None, id="no-config"), pytest.param("anthropic", id="anthropic")]
)
def test_doctor_reports_an_unknown_provider_name(
    isolated: Path, monkeypatch: pytest.MonkeyPatch, cfg: str | None
) -> None:
    from dag_healer.config import config_from_mapping

    monkeypatch.setenv("HEALER_LLM_PROVIDER", "openai")
    config = None if cfg is None else config_from_mapping({"llm": {"provider": cfg}})
    result = doctor_cmd.check_llm(config)
    assert (result.status, result.detail) == (
        doctor_cmd.ERROR,
        "unknown LLM provider 'openai': expected anthropic, bedrock or fake "
        "(llm.provider or HEALER_LLM_PROVIDER)",
    )


def test_the_credential_probe_asks_the_instance_metadata_service_once_with_a_short_timeout(
    aws_sdk: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    import botocore.session

    seen: dict[str, Any] = {}
    get_credentials = botocore.session.Session.get_credentials

    def spy(self: Any) -> Any:
        seen["timeout"] = self.get_config_variable("metadata_service_timeout")
        seen["attempts"] = self.get_config_variable("metadata_service_num_attempts")
        return get_credentials(self)

    monkeypatch.setattr(botocore.session.Session, "get_credentials", spy)
    monkeypatch.setenv("AWS_METADATA_SERVICE_TIMEOUT", "30")
    monkeypatch.setenv("AWS_METADATA_SERVICE_NUM_ATTEMPTS", "5")
    assert doctor_cmd._aws_credentials(None) == (None, None, None)
    assert seen == {"timeout": doctor_cmd.METADATA_TIMEOUT_SECONDS, "attempts": 1}
    assert doctor_cmd.METADATA_TIMEOUT_SECONDS <= 2


def test_aws_errors_never_show_a_key() -> None:
    botocore_exceptions = pytest.importorskip("botocore.exceptions")
    exc = botocore_exceptions.CredentialRetrievalError(
        provider="custom-process",
        error_msg=f"denied AKIAEXAMPLEKEYID0001 {AWS_SECRET_KEY} for ops@example.com;" + " retry later" * 40,
    )
    text = doctor_cmd._aws_error_text(exc)
    assert text.startswith(
        "CredentialRetrievalError: Error when retrieving credentials from custom-process: denied *** *** for "
    )
    assert "AKIAEXAMPLEKEYID0001" not in text and AWS_SECRET_KEY not in text and "ops@example.com" not in text
    assert text.endswith("...") and len(text) == len("CredentialRetrievalError: ") + 300  # cut
    profile = "healer-worker-production-eu-west-1-account"  # long, but not random: shown
    missing = doctor_cmd._aws_error_text(botocore_exceptions.ProfileNotFound(profile=profile))
    assert missing == f"ProfileNotFound: The config profile ({profile}) could not be found"


@pytest.mark.parametrize(
    ("variable", "region"),
    [
        pytest.param("AWS_REGION", "collector.example.net/", id="host"),
        pytest.param("AWS_REGION", "Europe West", id="words"),
        pytest.param("AWS_DEFAULT_REGION", "eu-west-1.collector.example.net#", id="default-region"),
    ],
)
def test_doctor_bedrock_refuses_a_region_variable_that_is_no_region(
    aws_sdk: Path, monkeypatch: pytest.MonkeyPatch, variable: str, region: str
) -> None:
    """The region names the host of the signed requests: an error, and the value is never shown."""
    from dag_healer.agent import bedrock

    _env_credentials(monkeypatch)
    monkeypatch.setenv(variable, region)
    result = doctor_cmd.check_llm(_bedrock())
    assert (result.status, result.detail) == (
        doctor_cmd.ERROR,
        f"provider bedrock (model {BEDROCK_MODEL}, region invalid): "
        f"{bedrock.REGION_INVALID.format(source=variable)}",
    )
    assert region not in result.detail


def test_doctor_bedrock_refuses_a_profile_region_that_is_no_region(
    aws_sdk: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    from dag_healer.agent import bedrock

    _env_credentials(monkeypatch)
    _write_aws_file(aws_sdk / "aws-config", "[default]\nregion = eu-west-1.collector.example.net#\n")
    result = doctor_cmd.check_llm(_bedrock())
    assert (result.status, result.detail) == (
        doctor_cmd.ERROR,
        f"provider bedrock (model {BEDROCK_MODEL}, region invalid): "
        f"{bedrock.REGION_INVALID.format(source=bedrock.PROFILE_REGION)}",
    )


def test_doctor_bedrock_refuses_a_blank_region_variable_like_the_provider(
    aws_sdk: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """``AWS_REGION=' '`` next to a profile region: the SDK would take the blank value, so doctor
    does not report the profile's region as the one in use."""
    from dag_healer.agent import bedrock

    _env_credentials(monkeypatch)
    _write_aws_file(aws_sdk / "aws-config", "[default]\nregion = eu-west-1\n")
    monkeypatch.setenv("AWS_REGION", " ")
    result = doctor_cmd.check_llm(_bedrock())
    assert (result.status, result.detail) == (
        doctor_cmd.ERROR,
        f"provider bedrock (model {BEDROCK_MODEL}, region unset): "
        f"{bedrock.REGION_BLANK.format(name='AWS_REGION')}",
    )


@pytest.mark.parametrize(
    "base_url",
    [
        "http://collector.example.net",
        "https://collector.example.net\\.bedrock-runtime.eu-west-1.amazonaws.com",
        "https://bedrock.example.com/x?y#z",
    ],
    ids=["http", "backslash", "query-fragment"],
)
def test_doctor_bedrock_refuses_an_invalid_sdk_base_url(
    aws_sdk: Path, monkeypatch: pytest.MonkeyPatch, base_url: str
) -> None:
    from dag_healer.agent import bedrock

    _env_credentials(monkeypatch)
    monkeypatch.setenv("AWS_REGION", "eu-west-1")
    monkeypatch.setenv("ANTHROPIC_BEDROCK_BASE_URL", base_url)
    result = doctor_cmd.check_llm(_bedrock())
    assert (result.status, result.detail) == (
        doctor_cmd.ERROR,
        f"provider bedrock (model {BEDROCK_MODEL}, region eu-west-1): {bedrock.BASE_URL_INVALID}",
    )
    assert "collector" not in result.detail


def test_doctor_bedrock_agrees_with_the_provider(aws_sdk: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """Both read the same rules (``settings_problems``): what doctor refuses, the provider refuses."""
    from dag_healer.agent import bedrock

    _env_credentials(monkeypatch)
    for name, value in (
        ("AWS_REGION", "collector.example.net/"),
        ("ANTHROPIC_BEDROCK_BASE_URL", "http://x.example"),
        ("AWS_SESSION_TOKEN", f"{AWS_SESSION_TOKEN}\nsecond-line-of-the-value"),
    ):
        monkeypatch.setenv(name, value)
        result = doctor_cmd.check_llm(_bedrock())
        with pytest.raises(bedrock.LLMUnavailable) as info:
            bedrock.BedrockProvider(_bedrock()).classify(system="S", prompt="P")
        assert result.status == doctor_cmd.ERROR
        assert str(info.value) in result.detail.split(": ", 1)[1].split("; ")  # one of doctor's problems
        assert "second-line" not in result.detail and _without_aws_secrets(result.detail)
        monkeypatch.delenv(name)


def test_doctor_bedrock_refuses_expired_keys_of_the_environment(
    aws_sdk: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """What stays in a shell once keys exported with ``aws configure export-credentials --format
    env`` expire: doctor refuses them as the provider does, before any request and without their
    values; keys that have not expired yet still resolve."""
    import httpx2

    from dag_healer.agent import bedrock

    def no_request(request: httpx2.Request) -> httpx2.Response:
        raise AssertionError("no request expected")

    _env_credentials(monkeypatch)
    monkeypatch.setenv("AWS_REGION", "eu-west-1")
    monkeypatch.setenv("AWS_CREDENTIAL_EXPIRATION", "2099-01-01T00:00:00Z")
    assert doctor_cmd.check_llm(_bedrock()).detail.endswith(ENV_CREDENTIALS_RESOLVE)
    monkeypatch.setenv("AWS_CREDENTIAL_EXPIRATION", "2020-01-01T00:00:00Z")
    result = doctor_cmd.check_llm(_bedrock())
    expired = "RuntimeError: Credentials were refreshed, but the refreshed credentials are still expired."
    assert (result.status, result.detail) == (
        doctor_cmd.ERROR,
        f"provider bedrock (model {BEDROCK_MODEL}, region eu-west-1): "
        f"the AWS credentials for the bedrock provider cannot be resolved: {expired}",
    )
    assert _without_aws_secrets(result.detail)
    client = httpx2.Client(transport=httpx2.MockTransport(no_request))
    with pytest.raises(bedrock.LLMUnavailable) as info:
        bedrock.BedrockProvider(_bedrock(), http_client=client).classify(system="S", prompt="P")
    assert str(info.value) == (
        "the AWS credentials for the bedrock provider cannot be resolved during the incident "
        f"classification: {expired}"
    )


@pytest.mark.parametrize(
    ("form", "value"),
    [
        pytest.param("url-encoded secret", "quote:" + AWS_SECRET_KEY, id="url-secret"),
        pytest.param(
            "url-encoded token",
            "quote:IQoJb3JpZ2luX2VjEXAMPLE/role+token=Q0123456789abcdefXYZ",
            id="url-token",
        ),
        pytest.param("json-escaped secret", "json:" + AWS_SECRET_KEY, id="json-secret"),
        pytest.param("api key fragment", "ABSKQmVkcm9ja0FQSUtleS1leGFt", id="api-key-fragment"),
        pytest.param(
            "session token field", 'field:"SessionToken": "example-lower-case-token-value"', id="field"
        ),
    ],
)
def test_aws_errors_never_show_a_credential_in_any_form(form: str, value: str) -> None:
    """What a credential process may print: decoded before it is looked at, then hidden."""
    from urllib.parse import quote

    botocore_exceptions = pytest.importorskip("botocore.exceptions")
    kind, _, raw = (
        value.partition(":") if value.startswith(("quote:", "json:", "field:")) else ("", "", value)
    )
    shown = {"quote": quote(raw, safe=""), "json": raw.replace("/", "\\/"), "field": raw, "": value}[kind]
    exc = botocore_exceptions.CredentialRetrievalError(
        provider="custom-process", error_msg=f"output {shown} end"
    )
    text = doctor_cmd._aws_error_text(exc)
    assert text.startswith(
        "CredentialRetrievalError: Error when retrieving credentials from custom-process: output "
    )
    secret = raw.split('": "')[-1].rstrip('"') if kind == "field" else (raw or value)
    assert secret not in text and quote(secret, safe="") not in text, form
    assert not [secret[i : i + 16] for i in range(len(secret) - 15) if secret[i : i + 16] in text], form
    assert "***" in text


def test_doctor_shows_the_bedrock_line(
    tmp_path: Path, aws_sdk: Path, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    (tmp_path / "dags").mkdir()
    config_file = tmp_path / "healer.yaml"
    config_file.write_text(
        f"llm: {{provider: bedrock, model: {BEDROCK_MODEL}, aws_region: eu-west-1, fallbacks: false}}\n"
        "repo: {sandbox: {backend: process}}\n" + FAKEFLOW,
        encoding="utf-8",
    )
    _env_credentials(monkeypatch)
    code = main(["doctor", "-c", str(config_file)])
    out = capsys.readouterr().out
    assert code == 0, out
    line = _line(out, "llm")
    assert line.startswith("[ OK ]") and line.endswith(
        f"  provider bedrock (model {BEDROCK_MODEL}, region eu-west-1): {ENV_CREDENTIALS_RESOLVE}"
    )
    assert _without_aws_secrets(out)


@pytest.mark.parametrize(
    ("llm", "warned"),
    [
        pytest.param("provider: bedrock", False, id="default"),
        pytest.param("provider: bedrock, fallbacks: true", True, id="true"),
        pytest.param("provider: bedrock, fallbacks: false", False, id="false"),
        pytest.param("provider: bedrock, fallbacks: '${HEALER_NO_SUCH_SETTING}'", False, id="env-unset"),
        pytest.param("provider: anthropic, fallbacks: true", False, id="anthropic"),
    ],
)
def test_check_warns_about_fallbacks_written_for_bedrock(
    tmp_path: Path, isolated: Path, capsys: pytest.CaptureFixture[str], llm: str, warned: bool
) -> None:
    from dag_healer.agent import bedrock

    config_file = tmp_path / "healer.yaml"
    config_file.write_text(f"version: 1\nllm: {{{llm}, model: {BEDROCK_MODEL}}}\n", encoding="utf-8")
    assert main(["check", "--config", str(config_file)]) == 0
    captured = capsys.readouterr()
    assert captured.err == (f"warning: {bedrock.FALLBACKS_IGNORED}\n" if warned else "")


def test_check_shows_the_bedrock_settings_only_when_set(
    tmp_path: Path, isolated: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    config_file = tmp_path / "healer.yaml"
    llm = f"provider: bedrock, model: {BEDROCK_MODEL}, fallbacks: false"
    config_file.write_text(f"version: 1\nllm: {{{llm}}}\n", encoding="utf-8")
    assert main(["check", "--config", str(config_file)]) == 0
    shown = yaml.safe_load(capsys.readouterr().out.split("\nConfiguration is valid.")[0])
    assert shown["llm"] == {
        "provider": "bedrock",
        "model": BEDROCK_MODEL,
        "effort": "high",
        "max_tokens": 16000,
        "fallbacks": False,
    }
    endpoint = "https://vpce-0123-abcd.bedrock-runtime.eu-west-1.vpce.amazonaws.com"
    settings = f"aws_region: eu-west-1, aws_profile: healer-dev, endpoint_url: '{endpoint}'"
    config_file.write_text(f"version: 1\nllm: {{{llm}, {settings}}}\n", encoding="utf-8")
    assert main(["check", "--config", str(config_file)]) == 0
    captured = capsys.readouterr()
    shown = yaml.safe_load(captured.out.split("\nConfiguration is valid.")[0])
    assert {name: shown["llm"][name] for name in ("aws_region", "aws_profile", "endpoint_url")} == {
        "aws_region": "eu-west-1",
        "aws_profile": "healer-dev",
        "endpoint_url": endpoint,
    }
    assert captured.err == ""


@pytest.mark.parametrize(
    ("verbosity", "anthropic_log", "dropped"),
    [
        pytest.param(2, None, True, id="vv"),
        pytest.param(2, "debug", False, id="vv-sdk-debug"),
        pytest.param(1, None, False, id="v"),
    ],
)
def test_debug_logs_never_quote_the_requests_of_the_sdk(
    monkeypatch: pytest.MonkeyPatch,
    caplog: pytest.LogCaptureFixture,
    verbosity: int,
    anthropic_log: str | None,
    dropped: bool,
) -> None:
    """``-vv`` puts the root logger at DEBUG: the anthropic SDK's ``Request options`` record would
    quote every request body (the prompts), on both providers. Only ``ANTHROPIC_LOG=debug`` keeps it."""
    from dag_healer.cli import main as main_module

    target = logging.getLogger(main_module.SDK_REQUEST_LOGGER)
    monkeypatch.setattr(target, "filters", [])  # put back at teardown
    monkeypatch.setattr(main_module._SDK_FILTER, "keep_requests", main_module._SDK_FILTER.keep_requests)
    if anthropic_log is None:
        monkeypatch.delenv("ANTHROPIC_LOG", raising=False)
    else:
        monkeypatch.setenv("ANTHROPIC_LOG", anthropic_log)
    main_module._configure_logging(verbosity)
    caplog.set_level(logging.DEBUG)
    target.debug("Request options: %s", {"json_data": {"messages": [{"content": "PROMPT"}]}})
    target.debug("Sending HTTP Request: %s %s", "POST", "https://bedrock-runtime.eu-west-1.amazonaws.com")
    messages = [
        record.getMessage() for record in caplog.records if record.name == main_module.SDK_REQUEST_LOGGER
    ]
    assert any("PROMPT" in text for text in messages) is not dropped
    assert "Sending HTTP Request: POST https://bedrock-runtime.eu-west-1.amazonaws.com" in messages


#: A value that an answer's header echoes; it is in no variable, so only the headers rule hides it.
HEADER_ECHO = "example/echo+value/held-in-no-variable"
#: How a transport's trace prints the headers of an answer, and the same trace once filtered.
RAW_ANSWER_HEADERS = (
    "receive_response_headers.complete return_value=(b'HTTP/1.1', 403, b'Forbidden', "
    f"[(b'Content-Type', b'application/json'), (b'x-amzn-ErrorType', b'AccessDenied:{HEADER_ECHO}')])"
)
FILTERED_ANSWER_HEADERS = (
    "receive_response_headers.complete return_value=(b'HTTP/1.1', 403, b'Forbidden', [headers not shown])"
)
#: How httpcore2's trace of a failed request quotes the header value that h11 refused.
H11_REFUSAL = (
    "send_request_headers.failed exception=LocalProtocolError(LocalProtocolError("
    f"\"Illegal header value b'{HEADER_ECHO}\\\\n'\"))"
)
#: How httpcore2's traces of a failed answer quote the bytes that h11 could not read.
H11_STATUS_LINE = (
    "receive_response_headers.failed exception=RemoteProtocolError(RemoteProtocolError("
    f"\"illegal status line: bytearray(b'HTTP/1.1 4x3 {HEADER_ECHO}')\"))"
)
H11_CHUNK_HEADER = (
    "receive_response_body.failed exception=RemoteProtocolError(RemoteProtocolError("
    f"\"illegal chunk header: bytearray(b'{HEADER_ECHO}\\\\r\\\\n')\"))"
)
H11_CHUNK_FOOTER = (
    "receive_response_body.failed exception=RemoteProtocolError(RemoteProtocolError("
    f"\"malformed chunk footer: bytearray(b'{HEADER_ECHO[:2]}') (expected b'\\\\r\\\\n')\"))"
)
#: A reason phrase that echoes a value, in the status line of an answer.
ECHOED_REASON = f"Forbidden {HEADER_ECHO}"


@pytest.mark.parametrize(
    ("name", "message", "args", "shown"),
    [
        pytest.param(
            "anthropic._base_client",
            'HTTP Response: %s %s "%i %s" %s',
            (
                "POST",
                "https://h.example/model/m/invoke",
                403,
                "Forbidden",
                httpx.Headers({"x-echo": HEADER_ECHO}),
            ),
            'HTTP Response: POST https://h.example/model/m/invoke "403 Forbidden" [headers not shown]',
            id="sdk-answer",
        ),
        pytest.param(
            "anthropic._base_client",
            "request_id: %s",
            (HEADER_ECHO,),
            "request_id: [headers not shown]",
            id="sdk-request-id",
        ),
        pytest.param(
            "anthropic._base_client", "request_id: %s", (None,), "request_id: None", id="sdk-no-request-id"
        ),
        pytest.param(
            "httpcore2.http11", RAW_ANSWER_HEADERS, (), FILTERED_ANSWER_HEADERS, id="httpcore2-answer"
        ),
        pytest.param(
            "httpcore.http11", RAW_ANSWER_HEADERS, (), FILTERED_ANSWER_HEADERS, id="httpcore-answer"
        ),
        pytest.param("httpcore2.http11", H11_REFUSAL, (), None, id="h11-refusal"),
        pytest.param("httpcore2.http11", H11_STATUS_LINE, (), None, id="h11-status-line"),
        pytest.param("httpcore2.http11", H11_CHUNK_HEADER, (), None, id="h11-chunk-header"),
        pytest.param("httpcore.http11", H11_CHUNK_FOOTER, (), None, id="h11-chunk-footer"),
        pytest.param(
            "anthropic._base_client",
            'HTTP Response: %s %s "%i %s" %s',
            ("POST", "https://h.example/model/m/invoke", 403, ECHOED_REASON, {"x-echo": "e"}),
            'HTTP Response: POST https://h.example/model/m/invoke "403 Forbidden" [headers not shown]',
            id="sdk-reason-phrase",
        ),
        pytest.param(
            "httpx2",
            'HTTP Request: %s %s "%s %d %s"',
            ("POST", "https://h.example/model/m/invoke", "HTTP/1.1", 403, ECHOED_REASON),
            'HTTP Request: POST https://h.example/model/m/invoke "HTTP/1.1 403 Forbidden"',
            id="httpx2-reason-phrase",
        ),
        pytest.param(
            "httpx",
            'HTTP Request: %s %s "%s %d %s"',
            ("POST", "https://h.example/v1/messages", "HTTP/1.1", 499, ECHOED_REASON),
            'HTTP Request: POST https://h.example/v1/messages "HTTP/1.1 499 "',
            id="httpx-unknown-status",
        ),
        pytest.param(
            "httpcore2.http11",
            RAW_ANSWER_HEADERS.replace("b'Forbidden'", f"b'{ECHOED_REASON}'"),
            (),
            FILTERED_ANSWER_HEADERS,
            id="httpcore2-reason-phrase",
        ),
        pytest.param(
            "httpcore2.http11",
            RAW_ANSWER_HEADERS.replace("b'Forbidden'", f'b"it\'s {ECHOED_REASON}"'),
            (),
            FILTERED_ANSWER_HEADERS,
            id="httpcore2-double-quoted-reason-phrase",
        ),
        pytest.param(
            "anthropic.lib.tools._beta_runner",
            "Turn ended with stop_reason %r, exiting from tool runner loop.",
            (HEADER_ECHO,),
            None,
            id="sdk-runner-stop-reason",
        ),
        pytest.param(
            "anthropic._base_client",
            "Request headers: %s",
            (f"x-amz-security-token: {HEADER_ECHO}",),
            None,
            id="credential-header",
        ),
        pytest.param(
            "anthropic._base_client",
            "Sending HTTP Request: %s %s",
            ("POST", "https://h.example/model/m/invoke"),
            "Sending HTTP Request: POST https://h.example/model/m/invoke",
            id="sdk-request",
        ),
        pytest.param(
            "httpcore2.http11",
            "send_request_headers.started request=<Request [b'POST']>",
            (),
            "send_request_headers.started request=<Request [b'POST']>",
            id="httpcore2-request",
        ),
        pytest.param(
            "dag_healer.worker.pipeline",
            "Authorization: %s",
            (HEADER_ECHO,),
            f"Authorization: {HEADER_ECHO}",
            id="another-logger",
        ),
    ],
)
def test_the_sdk_log_filter_hides_the_headers_of_an_answer_and_drops_credentials(
    name: str, message: str, args: tuple[Any, ...], shown: str | None
) -> None:
    """``shown``: what a handler prints of the record once filtered (``None``: dropped). A header
    or the reason phrase of an answer can echo a credential that no variable holds (an
    ``x-amzn-ErrorType``, a gateway's header), and h11 quotes whole the bytes it refuses (a header
    value, a status line, a chunk header or footer). The SDK's tool runner logs the stop reason
    as the answer gave it."""
    from dag_healer.cli.main import _SdkRequestFilter

    record = logging.LogRecord(name, logging.DEBUG, __file__, 1, message, args, None)
    kept = _SdkRequestFilter().filter(record)
    assert (record.getMessage() if kept else None) == shown


def test_the_sdk_log_filter_drops_a_traceback_that_quotes_a_credential(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The SDK's ``Encountered Exception`` record carries the traceback of a failed request."""
    from dag_healer.cli.main import _SdkRequestFilter

    key = "sk-ant-api03-EXAMPLE-key-held-in-the-environment-0123456789"
    monkeypatch.setenv("ANTHROPIC_API_KEY", key)

    def failure(error: Exception) -> logging.LogRecord:
        try:
            raise error
        except Exception:
            info = sys.exc_info()
        return logging.LogRecord(
            "anthropic._base_client", logging.DEBUG, __file__, 1, "Encountered Exception", (), info
        )

    keep = _SdkRequestFilter()
    assert keep.filter(failure(ConnectionError("connection refused"))) is True
    assert keep.filter(failure(ValueError(f"Illegal header value b'{HEADER_ECHO}\\n'"))) is False
    # anthropic 1.x logs httpx's HTTPStatusError with its traceback, whose message quotes the status line
    answered = httpx.Response(
        403,
        request=httpx.Request("POST", "https://h.example/v1/messages"),
        extensions={"reason_phrase": ECHOED_REASON.encode("ascii")},
    )
    with pytest.raises(httpx.HTTPStatusError) as status_error:
        answered.raise_for_status()
    assert HEADER_ECHO in str(status_error.value)
    assert keep.filter(failure(status_error.value)) is False
    assert keep.filter(failure(ValueError(f"echo of {key}"))) is False
    assert keep.filter(failure(ValueError(f"echo of {key[12:40]}"))) is False  # a fragment of 16 or more


@pytest.mark.parametrize("verbosity", [0, 1, 2])
def test_the_sdk_log_filter_is_on_at_every_verbosity(
    monkeypatch: pytest.MonkeyPatch, caplog: pytest.LogCaptureFixture, verbosity: int
) -> None:
    """``ANTHROPIC_LOG=debug`` turns the SDK's DEBUG records on whatever ``-v`` says: the headers
    of an answer never show, while the ``Request options`` that it asks for are kept."""
    from dag_healer.cli import main as main_module

    for name in main_module.FILTERED_LOGGERS:
        monkeypatch.setattr(logging.getLogger(name), "filters", [])  # put back at teardown
    monkeypatch.setattr(main_module._SDK_FILTER, "keep_requests", main_module._SDK_FILTER.keep_requests)
    monkeypatch.setenv("ANTHROPIC_LOG", "debug")
    main_module._configure_logging(verbosity)
    caplog.set_level(logging.DEBUG)
    sdk = logging.getLogger(main_module.SDK_REQUEST_LOGGER)
    sdk.debug(
        'HTTP Response: %s %s "%i %s" %s', "POST", "https://h.example/x", 403, "Forbidden", {"x-echo": "e"}
    )
    sdk.debug("Request options: %s", {"json_data": {"messages": [{"content": "PROMPT"}]}})
    logging.getLogger("httpcore2.http11").debug(RAW_ANSWER_HEADERS)
    logging.getLogger("httpx2").info(
        'HTTP Request: %s %s "%s %d %s"', "POST", "u", "HTTP/1.1", 403, ECHOED_REASON
    )
    names = (main_module.SDK_REQUEST_LOGGER, "httpcore2.http11", "httpx2")
    assert [record.getMessage() for record in caplog.records if record.name in names] == [
        'HTTP Response: POST https://h.example/x "403 Forbidden" [headers not shown]',
        "Request options: {'json_data': {'messages': [{'content': 'PROMPT'}]}}",
        FILTERED_ANSWER_HEADERS,
        'HTTP Request: POST u "HTTP/1.1 403 Forbidden"',
    ]


# --------------------------------------------------------------------------- doctor: the GitHub setup

GITHUB_TOKEN_VALUE = "github_pat_DOCTOR0canary0123456789"
GITHUB_CONFIG = "github: {repo: acme-health/etl, token: '${GITHUB_TOKEN}'}\n"
GITHUB_API = "/repos/acme-health/etl"
GITHUB_RULES = f"{GITHUB_API}/rules/branches/main?per_page=100"
#: The API answers of a repository whose ruleset does everything dag-healer recommends.
WELL_SET_GITHUB: dict[str, Any] = {
    GITHUB_API: (200, {"full_name": "acme-health/etl", "permissions": {"admin": False, "push": True}}),
    GITHUB_RULES: (
        200,
        [
            {"type": "deletion", "ruleset_id": 42},
            {"type": "non_fast_forward", "ruleset_id": 42},
            {"type": "pull_request", "ruleset_id": 42, "parameters": {"required_approving_review_count": 1}},
            {
                "type": "required_status_checks",
                "ruleset_id": 42,
                "parameters": {"required_status_checks": [{"context": "unit-tests"}]},
            },
        ],
    ),
    f"{GITHUB_API}/rulesets/42": (
        200,
        {"id": 42, "name": "Protect main", "current_user_can_bypass": "never"},
    ),
}


@pytest.fixture
def github_api(monkeypatch: pytest.MonkeyPatch) -> Callable[[Mapping[str, Any]], list[httpx.Request]]:
    """Point doctor's GitHub checks at a fake API answering by raw path; returns its requests."""

    def install(routes: Mapping[str, Any]) -> list[httpx.Request]:
        requests: list[httpx.Request] = []

        def handler(request: httpx.Request) -> httpx.Response:
            requests.append(request)
            route = routes.get(request.url.raw_path.decode("ascii"), (404, {"message": "Not Found"}))
            if isinstance(route, Exception):
                raise route
            headers = route[2] if len(route) > 2 else None
            return httpx.Response(route[0], json=route[1], headers=headers)

        monkeypatch.setattr(doctor_cmd, "_github_transport", lambda: httpx.MockTransport(handler))
        return requests

    return install


def _github_lines(out: str) -> list[tuple[str, str, str]]:
    """``(label, name, detail)`` of every ``github...`` line (several findings repeat a name)."""
    lines = []
    for line in out.splitlines():
        if line[:1] == "[" and line[5:6] == "]" and line[7:].startswith("github"):
            name, detail = line[7:].split("  ", 1)
            lines.append((line[:6], name.strip(), detail.strip()))
    return lines


def test_doctor_checks_the_github_setup(
    tmp_path: Path,
    isolated: Path,
    github_api: Callable[[Mapping[str, Any]], list[httpx.Request]],
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    monkeypatch.setenv("GITHUB_TOKEN", GITHUB_TOKEN_VALUE)
    requests = github_api(WELL_SET_GITHUB)
    code, out = _doctor(tmp_path, FAKEFLOW + GITHUB_CONFIG, capsys)
    assert code == 0, out
    assert _github_lines(out) == [
        ("[ OK ]", "github", "pull requests on acme-health/etl (token hidden)"),
        ("[ OK ]", "github token", "a fine-grained personal access token"),
        ("[ OK ]", "github repository", "the token's account is not an admin of acme-health/etl"),
        (
            "[ OK ]",
            "github base branch",
            "main requires a pull request with an approving review and blocks force pushes and deletion, "
            "and requires status checks",
        ),
        ("[ OK ]", "github workflows", "no workflow files in .github/workflows"),
    ]
    assert GITHUB_TOKEN_VALUE not in out
    assert {(request.method, request.url.host) for request in requests} == {("GET", "api.github.com")}
    assert all(request.headers["Authorization"] == f"Bearer {GITHUB_TOKEN_VALUE}" for request in requests)


def test_doctor_fails_an_unprotected_base_branch_and_a_workflow_that_runs_the_head(
    tmp_path: Path,
    isolated: Path,
    github_api: Callable[[Mapping[str, Any]], list[httpx.Request]],
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    classic = "ghp_DOCTOR0canary0123456789"
    monkeypatch.setenv("GITHUB_TOKEN", classic)
    workflows = tmp_path / ".github" / "workflows"
    workflows.mkdir(parents=True)
    (workflows / "label.yml").write_text(
        "on: pull_request_target\njobs:\n  label:\n    steps:\n      - uses: actions/checkout@v4\n"
        "        with: {ref: '${{ github.event.pull_request.head.sha }}'}\n",
        encoding="utf-8",
        newline="\n",
    )
    github_api(
        {
            # GitHub lists the scopes of a classic token in every answer
            GITHUB_API: (*WELL_SET_GITHUB[GITHUB_API], {"X-OAuth-Scopes": "repo"}),
            GITHUB_RULES: (200, []),
            f"{GITHUB_API}/branches/main": (200, {"name": "main", "protected": False}),
        }
    )
    code, out = _doctor(tmp_path, FAKEFLOW + GITHUB_CONFIG, capsys)
    assert code == 1
    lines = _github_lines(out)
    assert [(label, name) for label, name, _ in lines] == [
        ("[ OK ]", "github"),
        ("[WARN]", "github token"),
        ("[ OK ]", "github repository"),
        ("[FAIL]", "github base branch"),
        ("[FAIL]", "github base branch"),
        ("[FAIL]", "github base branch"),
        ("[WARN]", "github base branch"),
        ("[FAIL]", "github workflows"),
    ]
    assert lines[1][2].startswith("The GitHub token is a classic personal access token: its scope covers")
    assert lines[3][2].startswith("The base branch main does not require a pull request with at least one")
    assert lines[7][2].startswith("The workflow .github/workflows/label.yml runs on pull_request_target")
    assert classic not in out and "ghp_" not in out


def test_doctor_reports_what_it_could_not_check_on_github_as_warnings(
    tmp_path: Path,
    isolated: Path,
    github_api: Callable[[Mapping[str, Any]], list[httpx.Request]],
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    monkeypatch.setenv("GITHUB_TOKEN", GITHUB_TOKEN_VALUE)
    requests = github_api({GITHUB_API: httpx.ConnectError("no route to host")})
    code, out = _doctor(tmp_path, FAKEFLOW + GITHUB_CONFIG, capsys)
    assert code == 0, out  # warnings only
    lines = _github_lines(out)
    assert [(label, name) for label, name, _ in lines] == [
        ("[ OK ]", "github"),
        ("[ OK ]", "github token"),
        ("[WARN]", "github repository"),
        ("[WARN]", "github base branch"),
        ("[ OK ]", "github workflows"),
    ]
    assert "the GitHub API could not be reached (ConnectError)" in lines[2][2]
    assert len(requests) == 1  # nothing more is sent after a transport error


def test_doctor_calls_github_only_with_a_token_and_a_repository(
    tmp_path: Path, isolated: Path, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    def no_network() -> Any:
        raise AssertionError("no GitHub request expected")

    monkeypatch.setattr(doctor_cmd, "_github_transport", no_network)
    code, out = _doctor(tmp_path, FAKEFLOW + "github: {repo: acme-health/etl}\n", capsys)
    assert code == 0, out
    assert _github_lines(out) == [
        ("[WARN]", "github", "github.repo is acme-health/etl but no token is set; using PR.md")
    ]


def test_doctor_finds_the_github_repository_from_the_origin_remote(
    tmp_path: Path,
    isolated: Path,
    github_api: Callable[[Mapping[str, Any]], list[httpx.Request]],
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    from dag_healer.repo.git import run_git

    run_git(["init", "-q"], tmp_path)
    run_git(["remote", "add", "origin", "https://github.com/acme-health/etl.git"], tmp_path)
    monkeypatch.setenv("GITHUB_TOKEN", GITHUB_TOKEN_VALUE)
    requests = github_api(WELL_SET_GITHUB)
    code, out = _doctor(tmp_path, FAKEFLOW + "github: {token: '${GITHUB_TOKEN}'}\n", capsys)
    assert code == 0, out
    assert _github_lines(out)[0] == (
        "[ OK ]",
        "github",
        "pull requests on acme-health/etl (from the origin remote; token hidden)",
    )
    assert requests[0].url.raw_path == b"/repos/acme-health/etl"


def test_doctor_survives_a_broken_github_setup_check(
    tmp_path: Path, isolated: Path, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    def broken(cfg: Any) -> Any:
        raise RuntimeError(f"cannot use {cfg.github.token}")

    monkeypatch.setenv("GITHUB_TOKEN", GITHUB_TOKEN_VALUE)
    monkeypatch.setattr(doctor_cmd, "github_setup_checks", broken)
    code, out = _doctor(tmp_path, FAKEFLOW + GITHUB_CONFIG, capsys)
    assert code == 0, out  # one warning: what could not be checked never fails doctor
    assert _github_lines(out)[-1] == (
        "[WARN]",
        "github setup",
        "The GitHub setup could not be checked (RuntimeError). Check the network settings and github.repo, "
        "then check the token, the base branch's rules and the workflows by hand.",
    )
    assert GITHUB_TOKEN_VALUE not in out and "cannot use" not in out  # the type only, never the message


# --------------------------------------------------------------------------- contracts


def test_contracts_list_without_contracts(
    tmp_path: Path, isolated: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    config_file = tmp_path / "healer.yaml"
    config_file.write_text("version: 1\n", encoding="utf-8")
    assert main(["contracts", "list", "-c", str(config_file)]) == 0
    assert "No contracts" in capsys.readouterr().out


def test_contracts_without_action_prints_help(isolated: Path, capsys: pytest.CaptureFixture[str]) -> None:
    assert main(["contracts"]) == 2
    assert "approve" in capsys.readouterr().out


def test_approve_unknown_or_invalid_dag(repo: Path, capsys: pytest.CaptureFixture[str]) -> None:
    assert main([*_cfg_args(repo), "contracts", "approve", "nope", "--by", "alice"]) == 1
    assert "no contract for DAG 'nope'" in capsys.readouterr().err
    assert main([*_cfg_args(repo), "contracts", "approve", "../etc", "--by", "alice"]) == 1
    assert "invalid dag_id" in capsys.readouterr().err
    assert main([*_cfg_args(repo), "contracts", "approve", "sales_daily_etl", "--by", "  "]) == 1


def test_approve_survives_an_unreachable_store(
    repo: Path, tmp_path: Path, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    monkeypatch.setenv("HEALER_DATABASE_URL", _broken_sqlite_url(tmp_path))
    assert main([*_cfg_args(repo), "contracts", "approve", "customers_sync", "--by", "carol"]) == 0
    captured = capsys.readouterr()
    assert "could not record the approval in the store" in captured.err
    contract = load_contract(contract_path(repo / "healer" / "contracts", "customers_sync"))
    assert contract.is_approved and contract.approved_by == "carol"


def test_stale_contract_is_listed_and_can_be_reapproved(
    repo: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    path = contract_path(repo / "healer" / "contracts", "sales_daily_etl")
    assert main([*_cfg_args(repo), "contracts", "approve", "sales_daily_etl", "--by", "alice"]) == 0
    approved = load_contract(path)
    save_contract(approved.model_copy(update={"notes": "edited after approval"}), path)
    capsys.readouterr()

    assert main([*_cfg_args(repo), "contracts", "list"]) == 0
    out = capsys.readouterr().out
    sales_line = next(line for line in out.splitlines() if line.startswith("sales_daily_etl"))
    assert sales_line.split()[-1] == "yes"
    assert "Stale contracts" in out

    assert main([*_cfg_args(repo), "contracts", "approve", "sales_daily_etl", "--by", "bob"]) == 0
    reapproved = load_contract(path)
    assert reapproved.approved_by == "bob"
    assert reapproved.source_hash != approved.source_hash


# --------------------------------------------------------------------------- incidents


def _seed_incidents(repo: Path) -> None:
    from dag_healer.config import load_config
    from dag_healer.store import create_incident, session_scope, set_status

    cfg = load_config(repo / "healer.yaml")
    with session_scope(cfg.database_url) as session:
        first, _ = create_incident(
            session, dag_id="sales_daily_etl", run_id="manual__1", task_id="transform_sales", try_number=1,
            source="rest", error_summary="KeyError: 'amount'",
        )  # fmt: skip
        create_incident(
            session, dag_id="customers_sync", run_id="manual__2", task_id="transform", try_number=1,
            source="listener", error_summary="ValueError",
        )  # fmt: skip
        set_status(session, first, "reported", "reported as DATA")


def test_incidents_list_filters_and_json(repo: Path, capsys: pytest.CaptureFixture[str]) -> None:
    assert main([*_cfg_args(repo), "incidents", "list"]) == 0
    assert "No incidents found." in capsys.readouterr().out

    _seed_incidents(repo)
    assert main([*_cfg_args(repo), "incidents", "list"]) == 0
    out = capsys.readouterr().out
    assert "sales_daily_etl" in out and "customers_sync" in out and "STATUS" in out

    assert main([*_cfg_args(repo), "incidents", "list", "--status", "reported"]) == 0
    out = capsys.readouterr().out
    assert "sales_daily_etl" in out and "customers_sync" not in out

    assert main([*_cfg_args(repo), "incidents", "list", "--dag", "customers_sync", "--json"]) == 0
    data = json.loads(capsys.readouterr().out)
    assert [item["dag_id"] for item in data] == ["customers_sync"]
    assert data[0]["status"] == "new"


def test_incidents_list_warns_on_unknown_status(repo: Path, capsys: pytest.CaptureFixture[str]) -> None:
    assert main([*_cfg_args(repo), "incidents", "list", "--status", "bogus,new"]) == 0
    assert "unknown status(es): bogus" in capsys.readouterr().err


def test_incidents_list_store_unreachable(
    repo: Path, tmp_path: Path, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    monkeypatch.setenv("HEALER_DATABASE_URL", _broken_sqlite_url(tmp_path))
    assert main([*_cfg_args(repo), "incidents", "list"]) == 1
    assert "cannot read incidents" in capsys.readouterr().err


# --------------------------------------------------------------------------- worker / registry


def test_worker_rejects_an_invalid_config(
    isolated: Path, tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    bad = tmp_path / "healer.yaml"
    bad.write_text("version: 2\n", encoding="utf-8")
    assert main(["-c", str(bad), "worker", "--once"]) == 1
    assert "unsupported config version" in capsys.readouterr().err


def test_worker_that_cannot_start_exits_1(
    isolated: Path, tmp_path: Path, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    from dag_healer.worker import loop

    reason = "monitor.modes includes queue, but Apache Airflow has no hint queue to drain"

    def refuse(cfg: Any, **kwargs: Any) -> Any:
        raise loop.WorkerStartError(reason)

    monkeypatch.setattr(loop, "run_worker", refuse)
    config_file = tmp_path / "healer.yaml"
    config_file.write_text("version: 1\n", encoding="utf-8")
    level = logging.getLogger("dag_healer").level
    assert main(["-c", str(config_file), "worker", "--once", "--provider", "fake"]) == 1
    captured = capsys.readouterr()
    assert f"error: {reason}" in captured.err
    assert "dag-healer worker:" not in captured.out  # no --once summary
    assert logging.getLogger("dag_healer").level == level


def test_all_commands_registered(capsys: pytest.CaptureFixture[str]) -> None:
    from dag_healer.cli.main import COMMANDS, build_parser

    build_parser()
    assert {"init", "check", "contracts", "incidents", "doctor", "worker", "version"} <= set(COMMANDS)


def test_cli_help_does_not_import_heavy_modules() -> None:
    import subprocess
    import sys

    code = (
        "import sys; from dag_healer.cli.main import build_parser; build_parser(); "
        "heavy = [m for m in ('pandas', 'sqlalchemy', 'fastapi', 'anthropic', 'httpx') if m in sys.modules]; "
        "print(','.join(heavy))"
    )
    result = subprocess.run([sys.executable, "-c", code], capture_output=True, text=True, check=True)
    assert result.stdout.strip() == ""
