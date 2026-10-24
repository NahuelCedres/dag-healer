"""The adapter's wiring (``stepfunctions/adapter.py``) and its factory (``stepfunctions/__init__.py``).

Capabilities per options, the ports and what they share, construction without I/O, the sandbox
environment, ``health`` against stubbed STS and Step Functions clients (never raising),
``close``, ``FACTORY.create`` reading the ``orchestrator:`` section, and ``as_clients``.
"""

from __future__ import annotations

import json
import os
import subprocess
import sys
from collections.abc import Iterator
from datetime import timedelta
from pathlib import Path
from typing import Any

import pytest

from dag_healer import config as config_module
from dag_healer.config import HealerConfig, config_from_mapping
from dag_healer.core.ports import HealthReport, OrchestratorAdapter
from dag_healer.core.vocab import Capabilities, RemediationKind, Vocabulary
from dag_healer.orchestrators import registry
from dag_healer.orchestrators.registry import AdapterOptionsError
from dag_healer.stepfunctions import DISPLAY_NAME, FACTORY, FACTORY_TARGET, SFN_TYPE, StepFunctionsFactory
from dag_healer.stepfunctions.adapter import (
    PUSH_CHANNEL,
    SANDBOX_ENV,
    SFN_VOCABULARY,
    SOURCES,
    StepFunctionsAdapter,
    as_clients,
    capabilities_for,
)
from dag_healer.stepfunctions.aws import AwsClients
from dag_healer.stepfunctions.feed import SfnFeed
from dag_healer.stepfunctions.inspector import SfnInspector
from dag_healer.stepfunctions.intake import SfnIntake
from dag_healer.stepfunctions.links import SfnLinks
from dag_healer.stepfunctions.locator import SfnCodeLocator
from dag_healer.stepfunctions.options import StepFunctionsOptions
from dag_healer.stepfunctions.queue import SfnQueue
from dag_healer.stepfunctions.remediation import SfnRemediator
from dag_healer.stepfunctions.taxonomy import SFN_TAXONOMY

from .conftest import ACCOUNT, INSTANCE, NOW, PIPELINES, REGION, SALES_SM, sfn_options

QUEUE_URL = f"https://sqs.{REGION}.amazonaws.com/{ACCOUNT}/healer-sfn-events"
RESUME = RemediationKind.RESUME_RUN
RERUN = RemediationKind.RERUN_PIPELINE


@pytest.fixture(autouse=True)
def _restore_shared_state(monkeypatch: pytest.MonkeyPatch) -> Iterator[None]:
    """Loading a configuration extends ``config.SECRET_FIELDS`` and may cache an adapter factory."""
    monkeypatch.setattr(config_module, "SECRET_FIELDS", config_module.SECRET_FIELDS)
    yield
    registry.reset_adapters()


def _recording() -> tuple[Any, list[str]]:
    built: list[str] = []

    def factory(service: str) -> Any:
        built.append(service)
        raise AssertionError(f"a {service} client was built")

    return factory, built


def _stubbed_adapter(stubbed: Any, options: StepFunctionsOptions | None = None) -> StepFunctionsAdapter:
    return StepFunctionsAdapter(
        None, options=options or sfn_options(), instance=INSTANCE, clients=lambda service: stubbed(service)[0]
    )


def _identity() -> dict[str, Any]:
    return {
        "UserId": "AROAEXAMPLEID:dag-healer",
        "Account": ACCOUNT,
        "Arn": f"arn:aws:sts::{ACCOUNT}:assumed-role/healer-read/dag-healer",
    }


def _machine(arn: str) -> dict[str, Any]:
    return {
        "stateMachineArn": arn,
        "name": arn.rsplit(":", 1)[-1],
        "status": "ACTIVE",
        "definition": '{"StartAt": "A", "States": {"A": {"Type": "Succeed"}}}',
        "roleArn": f"arn:aws:iam::{ACCOUNT}:role/sfn-etl-role",
        "type": "STANDARD",
        "creationDate": NOW - timedelta(days=30),
    }


def _config(tmp_path: Path, orchestrator: dict[str, Any] | None = None) -> HealerConfig:
    data: dict[str, Any] = {"repo": {"path": str(tmp_path)}}
    if orchestrator is not None:
        data["orchestrator"] = orchestrator
    return config_from_mapping(data, base_dir=tmp_path)


# =========================================================================== capabilities


@pytest.mark.parametrize(
    ("overrides", "remediations"),
    [
        ({}, (RESUME,)),
        ({"rerun": True}, (RESUME, RERUN)),
        ({"retry_strategy": "disabled"}, ()),
        ({"retry_strategy": "disabled", "rerun": True}, (RERUN,)),
    ],
)
def test_capabilities_follow_the_options(overrides: dict[str, Any], remediations: tuple[Any, ...]) -> None:
    capabilities = capabilities_for(sfn_options(**overrides))
    assert capabilities == Capabilities(
        sources=("poll", "eventbridge", "sqs", "cli"),
        push_channels=("eventbridge",),
        attempts_visible=False,
        remediations=remediations,
        fix_delivery="ci_deploy",
        ui_hosts=("standalone",),
        native_authz=False,
    )
    assert SOURCES == capabilities.sources and PUSH_CHANNEL == "eventbridge"


@pytest.mark.parametrize(
    ("overrides", "has_remediator"),
    [({}, True), ({"rerun": True}, True), ({"retry_strategy": "disabled"}, False)],
)
def test_the_remediator_exists_only_with_a_remediation(
    overrides: dict[str, Any], has_remediator: bool
) -> None:
    adapter = StepFunctionsAdapter(None, options=sfn_options(**overrides), clients=_recording()[0])
    assert (adapter.remediator is not None) is has_remediator
    assert bool(adapter.capabilities.remediations) is has_remediator


# =========================================================================== wiring


def test_the_ports_share_one_runtime() -> None:
    factory, built = _recording()
    options = sfn_options(queue_url=QUEUE_URL, rerun=True)
    adapter = StepFunctionsAdapter(None, options=options, instance=INSTANCE, clients=factory)
    assert isinstance(adapter, OrchestratorAdapter)
    assert (adapter.type, adapter.display_name, adapter.instance) == (SFN_TYPE, DISPLAY_NAME, INSTANCE)
    assert adapter.vocabulary is SFN_VOCABULARY and adapter.taxonomy is SFN_TAXONOMY
    runtime = adapter.runtime
    assert runtime.options is options and runtime.instance == INSTANCE and runtime.cfg is None
    assert adapter.clients is runtime.clients and adapter.clients.injected
    assert isinstance(adapter.feed, SfnFeed) and adapter.feed.rt is runtime
    assert isinstance(adapter.push, SfnIntake) and adapter.push.rt is runtime
    assert adapter.feed.localizer is adapter.push.localizer, "one localisation memory for both"
    assert isinstance(adapter.queue, SfnQueue) and adapter.queue.queue_url == QUEUE_URL
    assert isinstance(adapter.inspector, SfnInspector) and adapter.inspector.locator is adapter.code
    assert isinstance(adapter.code, SfnCodeLocator) and adapter.code.options is options
    assert isinstance(adapter.remediator, SfnRemediator) and adapter.remediator.rt is runtime
    assert isinstance(adapter.links, SfnLinks) and adapter.links is not adapter
    assert adapter.push.channels == frozenset(adapter.capabilities.push_channels)
    assert [entry.pipeline_id for entry in runtime.pipelines.entries()] == list(PIPELINES)
    assert repr(adapter) == f"StepFunctionsAdapter(instance={INSTANCE!r}, region={REGION!r})"
    adapter.sandbox_env()
    adapter.doctor_checks()
    assert built == [], "construction and the static attributes do no I/O"


def test_defaults_without_options() -> None:
    adapter = StepFunctionsAdapter(None)
    assert adapter.instance == SFN_TYPE and adapter.options == StepFunctionsOptions()
    assert adapter.queue is None and adapter.remediator is not None
    assert adapter.clients.injected is False
    assert adapter.runtime.now().tzinfo is not None


@pytest.mark.parametrize(
    ("overrides", "expected"),
    [
        ({}, f"https://states.{REGION}.amazonaws.com"),
        ({"region": "cn-north-1"}, "https://states.cn-north-1.amazonaws.com.cn"),
        ({"region": None}, "the default AWS region"),
        ({"endpoint_url": "http://localhost:4566"}, "http://localhost:4566"),
        (
            {
                "endpoint_url": "http://localhost:4566",
                "endpoint_urls": {"stepfunctions": "https://vpce-1.states.eu-west-1.vpce.amazonaws.com"},
            },
            "https://vpce-1.states.eu-west-1.vpce.amazonaws.com",
        ),
    ],
)
def test_base_url_names_the_step_functions_endpoint(overrides: dict[str, Any], expected: str) -> None:
    adapter = StepFunctionsAdapter(None, options=sfn_options(**overrides), clients=lambda service: None)
    assert adapter.base_url == expected


def test_the_vocabulary() -> None:
    assert SFN_VOCABULARY == Vocabulary(
        product="AWS Step Functions",
        pipeline="state machine",
        pipelines="state machines",
        run="execution",
        step="state",
        steps="states",
        attempt="redrive attempt",
        map_index="item index",
        retry_action="Redrive execution",
    )


def test_sandbox_env_removes_the_cloud_credentials() -> None:
    env = StepFunctionsAdapter(None).sandbox_env()
    assert env == SANDBOX_ENV and env is not SANDBOX_ENV
    assert env["AWS_EC2_METADATA_DISABLED"] == "true"
    for name in ("AWS_ACCESS_KEY_ID", "AWS_SECRET_ACCESS_KEY", "AWS_SESSION_TOKEN", "AWS_PROFILE"):
        assert env[name] is None
    assert all(value is None for name, value in env.items() if name != "AWS_EC2_METADATA_DISABLED")
    env["AWS_PROFILE"] = "changed"
    assert SANDBOX_ENV["AWS_PROFILE"] is None


# =========================================================================== health


def test_health_ok(stubbed: Any) -> None:
    _sts, sts = stubbed("sts")
    sts.add_response("get_caller_identity", _identity(), {})
    _sfn, sfn = stubbed("stepfunctions")
    sfn.add_response("describe_state_machine", _machine(SALES_SM), {"stateMachineArn": SALES_SM})
    report = _stubbed_adapter(stubbed).health()
    assert report == HealthReport(
        ok=True,
        detail=f"AWS account {ACCOUNT}; Step Functions answers for sales_daily_etl",
        checks={"credentials": "ok", "states": "ok"},
    )


def test_health_without_pipelines_checks_the_credentials(stubbed: Any) -> None:
    _sts, sts = stubbed("sts")
    sts.add_response("get_caller_identity", _identity(), {})
    report = _stubbed_adapter(stubbed, sfn_options(pipelines={}, discover=True)).health()
    assert report == HealthReport(ok=True, detail=f"AWS account {ACCOUNT}", checks={"credentials": "ok"})


def test_health_with_unusable_credentials(stubbed: Any) -> None:
    _sts, sts = stubbed("sts")
    sts.add_client_error(
        "get_caller_identity",
        service_error_code="InvalidClientTokenId",
        service_message="The security token included in the request is invalid.",
        http_status_code=403,
    )
    report = _stubbed_adapter(stubbed).health()
    assert report.ok is False and report.checks == {"credentials": "error"}
    assert report.detail == (
        "AWS is not reachable: GetCallerIdentity failed: InvalidClientTokenId: "
        "The security token included in the request is invalid."
    )


def test_health_with_a_missing_state_machine(stubbed: Any) -> None:
    _sts, sts = stubbed("sts")
    sts.add_response("get_caller_identity", _identity(), {})
    _sfn, sfn = stubbed("stepfunctions")
    sfn.add_client_error(
        "describe_state_machine",
        service_error_code="StateMachineDoesNotExist",
        service_message="State Machine Does Not Exist",
        expected_params={"stateMachineArn": SALES_SM},
    )
    report = _stubbed_adapter(stubbed).health()
    assert report.ok is False and report.checks == {"credentials": "ok", "states": "error"}
    assert report.detail.startswith(f"AWS account {ACCOUNT}; Step Functions: DescribeStateMachine failed: ")


def test_health_with_a_name_that_does_not_resolve(stubbed: Any) -> None:
    _sts, sts = stubbed("sts")
    sts.add_response("get_caller_identity", _identity(), {})
    _sfn, sfn = stubbed("stepfunctions")
    sfn.add_response("list_state_machines", {"stateMachines": []}, {"maxResults": 1000})
    options = sfn_options(pipelines={"ghost": {"state_machine": "ghost"}})
    report = _stubbed_adapter(stubbed, options).health()
    assert report.ok is False and report.checks["states"] == "error"
    assert report.detail.endswith("state machine of pipeline ghost not found")


def test_health_never_raises() -> None:
    def factory(service: str) -> Any:
        raise RuntimeError(f"no {service} today")

    report = StepFunctionsAdapter(None, options=sfn_options(), clients=factory).health()
    assert report.ok is False and report.checks == {"credentials": "error"}
    assert "RuntimeError" in report.detail


def test_close_is_idempotent() -> None:
    adapter = StepFunctionsAdapter(None, options=sfn_options(), clients=_recording()[0])
    adapter.close()
    adapter.close()
    assert adapter.clients.closed is True


# =========================================================================== the factory


def test_the_factory_is_registered() -> None:
    assert isinstance(FACTORY, StepFunctionsFactory)
    assert (FACTORY.type, FACTORY.display_name) == (SFN_TYPE, DISPLAY_NAME)
    assert registry.load_factory(SFN_TYPE) is FACTORY
    assert FACTORY_TARGET == "dag_healer.stepfunctions:FACTORY"


def test_create_reads_the_orchestrator_section(tmp_path: Path) -> None:
    cfg = _config(
        tmp_path,
        {
            "type": "stepfunctions",
            "instance": "sfn-test",
            "options": {"region": REGION, "pipelines": PIPELINES},
        },
    )
    factory, built = _recording()
    adapter = FACTORY.create(cfg, client=factory, clock=lambda: NOW)
    assert adapter.instance == "sfn-test" and adapter.options == sfn_options(template="template.yaml")
    assert adapter.runtime.cfg is cfg and adapter.runtime.now() == NOW
    assert adapter.code.options is adapter.options
    assert built == []


def test_create_accepts_the_explicit_factory_target(tmp_path: Path) -> None:
    cfg = _config(tmp_path, {"type": FACTORY_TARGET, "instance": "sfn-target", "options": {"rerun": True}})
    adapter = FACTORY.create(cfg, client=_recording()[0])
    assert adapter.instance == "sfn-target" and adapter.options.rerun is True


def test_create_refuses_invalid_options(tmp_path: Path) -> None:
    cfg = _config(tmp_path, {"type": "stepfunctions", "options": {"region": "nowhere", "unknown": 1}})
    with pytest.raises(AdapterOptionsError) as raised:
        FACTORY.create(cfg)
    text = str(raised.value)
    assert "orchestrator.options.region" in text and "orchestrator.options.unknown" in text


def test_create_for_a_configuration_of_another_type(tmp_path: Path) -> None:
    adapter = FACTORY.create(_config(tmp_path), client=_recording()[0])
    assert adapter.instance == SFN_TYPE and adapter.options == StepFunctionsOptions()


# =========================================================================== as_clients


class WorldLike:
    """An object with a ``client_factory`` method (the shape of ``testing.OfflineAws``)."""

    def __init__(self) -> None:
        self.asked: list[str] = []

    def client_factory(self, service: str) -> str:
        self.asked.append(service)
        return f"client of {service}"


def test_as_clients() -> None:
    options = sfn_options()
    built = as_clients(options, None)
    assert isinstance(built, AwsClients) and built.options is options and not built.injected
    given = AwsClients(options, client_factory=lambda service: service)
    assert as_clients(options, given) is given
    world = WorldLike()
    from_world = as_clients(options, world)
    assert (
        from_world.injected
        and from_world.sfn == "client of stepfunctions"
        and world.asked == ["stepfunctions"]
    )
    from_callable = as_clients(options, lambda service: f"made {service}")
    assert from_callable.sts == "made sts"


@pytest.mark.parametrize("client", ["not a client", 42, object()])
def test_as_clients_refuses_anything_else(client: Any) -> None:
    with pytest.raises(TypeError, match="client_factory"):
        as_clients(sfn_options(), client)


def test_importing_the_package_is_light() -> None:
    code = "\n".join(
        [
            "import json, sys",
            "import dag_healer.stepfunctions as package",
            "package.FACTORY.type, package.FACTORY.secret_options",
            "names = ('boto3', 'botocore', 'dag_healer.stepfunctions.adapter',",
            "         'dag_healer.stepfunctions.options')",
            "print(json.dumps(sorted(name for name in names if name in sys.modules)))",
        ]
    )
    env = dict(os.environ)
    env["PYTHONPATH"] = os.pathsep.join(entry for entry in sys.path if entry)
    result = subprocess.run(
        [sys.executable, "-c", code], capture_output=True, text=True, env=env, timeout=180, check=False
    )
    assert result.returncode == 0, result.stderr
    assert json.loads(result.stdout.strip().splitlines()[-1]) == []
