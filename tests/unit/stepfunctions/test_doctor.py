"""``dag-healer doctor`` checks of the Step Functions adapter (``stepfunctions/doctor.py``).

Every check against ``Stubber`` clients (exact parameters) or a configuration: the AWS
credentials, the state machines (missing, EXPRESS, deleting, resolved by name, discovered), the
code mapping (through a stand-in locator: the real one is tested with the fixture repository),
the monitor modes and the event queue. Building the list does no I/O, a check never raises and
the checks that need AWS are skipped once the credentials check failed.
"""

from __future__ import annotations

import json
from collections.abc import Iterator
from datetime import timedelta
from pathlib import Path
from typing import Any

import pytest

from dag_healer import config as config_module
from dag_healer.cli.doctor_cmd import ERROR, OK, SKIP, WARN, CheckResult
from dag_healer.config import HealerConfig, config_from_mapping, load_config
from dag_healer.core.ports import DoctorCheck
from dag_healer.core.specs import DagInfo, TaskInfo
from dag_healer.orchestrators import registry
from dag_healer.stepfunctions import FACTORY, doctor
from dag_healer.stepfunctions.adapter import StepFunctionsAdapter
from dag_healer.stepfunctions.doctor import (
    CODE_MAPPING_CHECK,
    CREDENTIALS_CHECK,
    EVENT_QUEUE_CHECK,
    MONITOR_MODES_CHECK,
    QUEUE_ATTRIBUTES,
    STATE_MACHINES_CHECK,
    check_code_mapping,
    check_credentials,
    check_event_queue,
    check_monitor_modes,
    check_state_machines,
    doctor_checks,
)

from .conftest import ACCOUNT, CUSTOMERS_SM, INVENTORY_SM, NOW, REGION, SALES_SM, SFN_REPO, sfn_options

QUEUE_URL = f"https://sqs.{REGION}.amazonaws.com/{ACCOUNT}/healer-sfn-events"
DLQ_ARN = f"arn:aws:sqs:{REGION}:{ACCOUNT}:healer-sfn-events-dlq"
PRINCIPAL = f"arn:aws:sts::{ACCOUNT}:assumed-role/healer-read/dag-healer"
NAMES = {
    SALES_SM: "SalesDailyEtlStateMachine-AbC123",
    CUSTOMERS_SM: "customers-sync",
    INVENTORY_SM: "InventoryReportStateMachine-XyZ789",
}


@pytest.fixture(autouse=True)
def _restore_shared_state(monkeypatch: pytest.MonkeyPatch) -> Iterator[None]:
    """Loading a configuration extends ``config.SECRET_FIELDS`` and may cache an adapter factory."""
    monkeypatch.setattr(config_module, "SECRET_FIELDS", config_module.SECRET_FIELDS)
    monkeypatch.delenv(doctor.INGEST_TOKEN_ENV, raising=False)
    yield
    registry.reset_adapters()


def _config(tmp_path: Path, **sections: Any) -> HealerConfig:
    return config_from_mapping({"repo": {"path": str(tmp_path)}, **sections}, base_dir=tmp_path)


def _machine(arn: str, *, kind: str = "STANDARD", status: str = "ACTIVE") -> dict[str, Any]:
    """A ``DescribeStateMachine`` answer (the definition is irrelevant here)."""
    return {
        "stateMachineArn": arn,
        "name": NAMES.get(arn, arn.rsplit(":", 1)[-1]),
        "status": status,
        "definition": '{"StartAt": "A", "States": {"A": {"Type": "Succeed"}}}',
        "roleArn": f"arn:aws:iam::{ACCOUNT}:role/sfn-etl-role",
        "type": kind,
        "creationDate": NOW - timedelta(days=30),
    }


def _listed(name: str, kind: str = "STANDARD") -> dict[str, Any]:
    return {
        "stateMachineArn": f"arn:aws:states:{REGION}:{ACCOUNT}:stateMachine:{name}",
        "name": name,
        "type": kind,
        "creationDate": NOW - timedelta(days=30),
    }


def _identity(account: str = ACCOUNT) -> dict[str, Any]:
    return {
        "UserId": "AROAEXAMPLEID:dag-healer",
        "Account": account,
        "Arn": PRINCIPAL.replace(ACCOUNT, account),
    }


class StandInLocator:
    """The two locator methods the code mapping check uses."""

    def __init__(
        self,
        specs: list[DagInfo] | None = None,
        unmapped: dict[str, list[str]] | None = None,
        error: Exception | None = None,
    ) -> None:
        self.specs = specs or []
        self.unmapped = unmapped or {}
        self.error = error
        self.roots: list[Path] = []

    def scan(self, repo_root: Path) -> list[DagInfo]:
        self.roots.append(repo_root)
        if self.error is not None:
            raise self.error
        return self.specs

    def unmapped_states(self, repo_root: Path) -> dict[str, list[str]]:
        return self.unmapped


def _spec(pipeline_id: str, *states: str, mapped: bool = True) -> DagInfo:
    tasks = [
        TaskInfo(state, "lambda", "transform", f"etl.{state.lower()}", "src/etl/x.py" if mapped else None, 3)
        for state in states
    ]
    return DagInfo(pipeline_id, "statemachine/x.asl.json", tasks=tasks)


# =========================================================================== the list


def test_building_the_checks_does_no_io(make_runtime: Any) -> None:
    built: list[str] = []

    def factory(service: str) -> Any:
        built.append(service)
        raise AssertionError("no client may be built")

    checks = doctor_checks(make_runtime(factory=factory), StandInLocator())
    assert [check.name for check in checks] == [
        CREDENTIALS_CHECK,
        STATE_MACHINES_CHECK,
        CODE_MAPPING_CHECK,
        MONITOR_MODES_CHECK,
    ]
    assert all(isinstance(check, DoctorCheck) and callable(check.run) for check in checks)
    with_queue = doctor_checks(make_runtime(sfn_options(queue_url=QUEUE_URL), factory=factory))
    assert [check.name for check in with_queue][-1] == EVENT_QUEUE_CHECK
    assert built == []


def test_the_adapter_contributes_the_checks(tmp_path: Path) -> None:
    adapter = StepFunctionsAdapter(_config(tmp_path), options=sfn_options(), clients=lambda service: None)
    assert [check.name for check in adapter.doctor_checks()] == [
        CREDENTIALS_CHECK,
        STATE_MACHINES_CHECK,
        CODE_MAPPING_CHECK,
        MONITOR_MODES_CHECK,
    ]


def test_a_full_run_in_order(make_runtime: Any, stubbed: Any, tmp_path: Path) -> None:
    cfg = _config(tmp_path, monitor={"modes": ["poll", "queue"], "trigger": "final_failure"})
    runtime = make_runtime(sfn_options(queue_url=QUEUE_URL), cfg=cfg)
    _sts, sts = stubbed("sts")
    sts.add_response("get_caller_identity", _identity(), {})
    _sfn, sfn = stubbed("stepfunctions")
    for arn in (SALES_SM, CUSTOMERS_SM, INVENTORY_SM):
        sfn.add_response("describe_state_machine", _machine(arn), {"stateMachineArn": arn})
    _sqs, sqs = stubbed("sqs")
    sqs.add_response(
        "get_queue_attributes",
        {
            "Attributes": {
                "ApproximateNumberOfMessages": "1",
                "ApproximateNumberOfMessagesNotVisible": "0",
                "RedrivePolicy": json.dumps({"deadLetterTargetArn": DLQ_ARN, "maxReceiveCount": 5}),
            }
        },
        {"QueueUrl": QUEUE_URL, "AttributeNames": list(QUEUE_ATTRIBUTES)},
    )
    locator = StandInLocator(
        specs=[
            _spec("sales_daily_etl", "ExtractSales", "TransformSales", "LoadSales"),
            _spec("customers_sync", "TransformCustomers"),
            _spec("inventory_report", "BuildReport"),
        ]
    )
    results = [check.run() for check in doctor_checks(runtime, locator)]
    assert all(isinstance(result, CheckResult) for result in results)
    assert [(result.name, result.status) for result in results] == [
        (CREDENTIALS_CHECK, OK),
        (STATE_MACHINES_CHECK, OK),
        (CODE_MAPPING_CHECK, OK),
        (MONITOR_MODES_CHECK, OK),
        (EVENT_QUEUE_CHECK, OK),
    ]
    assert results[1].detail == (
        "3 state machine(s): sales_daily_etl (STANDARD), customers_sync (STANDARD), "
        "inventory_report (STANDARD)"
    )
    assert results[2].detail == "3 pipeline(s), 5 Lambda state(s) mapped to repository code"
    assert locator.roots == [tmp_path.resolve()]
    assert (
        results[4].detail
        == "healer-sfn-events: 1 message(s) waiting, 0 in flight; dead-letter queue healer-sfn-events-dlq"
    )


# =========================================================================== aws credentials


def test_credentials(make_runtime: Any, stubbed: Any) -> None:
    _client, stubber = stubbed("sts")
    stubber.add_response("get_caller_identity", _identity(), {})
    result = check_credentials(make_runtime())
    assert (result.name, result.status) == (CREDENTIALS_CHECK, OK)
    assert result.detail == f"account {ACCOUNT} as {PRINCIPAL}, region {REGION}"


def test_credentials_of_another_account_are_a_warning(make_runtime: Any, stubbed: Any) -> None:
    _client, stubber = stubbed("sts")
    stubber.add_response("get_caller_identity", _identity("210987654321"), {})
    result = check_credentials(make_runtime())
    assert result.status == WARN
    assert f"the configured state machines belong to account(s) {ACCOUNT}" in result.detail


@pytest.mark.parametrize(
    ("code", "status_code", "expected"),
    [
        ("InvalidClientTokenId", 403, ERROR),
        ("ExpiredToken", 400, ERROR),
        ("Throttling", 400, WARN),
        ("InternalFailure", 500, WARN),
    ],
)
def test_unusable_credentials(
    make_runtime: Any, stubbed: Any, code: str, status_code: int, expected: str
) -> None:
    _client, stubber = stubbed("sts")
    stubber.add_client_error(
        "get_caller_identity",
        service_error_code=code,
        service_message="refused",
        http_status_code=status_code,
    )
    result = check_credentials(make_runtime())
    assert result.status == expected
    assert result.detail.startswith("AWS credentials are not usable: GetCallerIdentity failed: ")
    assert code in result.detail


def test_a_missing_region_is_a_configuration_error(make_runtime: Any) -> None:
    """Not a credentials problem: the detail says what to set (the client cannot be built)."""
    from botocore.exceptions import NoRegionError

    def no_region(service: str) -> Any:
        raise NoRegionError()

    result = check_credentials(make_runtime(factory=no_region))
    assert result.status == ERROR
    assert result.detail.startswith("AWS cannot be used: creating the sts client failed: NoRegionError")
    assert result.detail.endswith("(set orchestrator.options.region or AWS_DEFAULT_REGION)")


def test_aws_checks_are_skipped_after_a_failed_credentials_check(
    make_runtime: Any, stubbed: Any, tmp_path: Path
) -> None:
    cfg = _config(tmp_path, monitor={"modes": ["poll", "queue"]})
    runtime = make_runtime(sfn_options(queue_url=QUEUE_URL), cfg=cfg)
    _client, stubber = stubbed("sts")
    stubber.add_client_error(
        "get_caller_identity", service_error_code="InvalidClientTokenId", http_status_code=403
    )
    stubbed("stepfunctions")
    stubbed("sqs")
    results = {check.name: check.run() for check in doctor_checks(runtime, StandInLocator())}
    assert results[CREDENTIALS_CHECK].status == ERROR
    assert results[STATE_MACHINES_CHECK].status == SKIP
    assert results[EVENT_QUEUE_CHECK].status == SKIP
    assert results[MONITOR_MODES_CHECK].status == OK


def test_a_check_that_raises_is_an_error(make_runtime: Any, monkeypatch: pytest.MonkeyPatch) -> None:
    def broken(_rt: Any) -> Any:
        raise RuntimeError("boom")

    monkeypatch.setattr(doctor, "check_monitor_modes", broken)
    checks = {check.name: check for check in doctor_checks(make_runtime())}
    result = checks[MONITOR_MODES_CHECK].run()
    assert (result.name, result.status, result.detail) == (MONITOR_MODES_CHECK, ERROR, "RuntimeError: boom")


# =========================================================================== state machines


def test_express_missing_and_deleting_machines_are_errors(make_runtime: Any, stubbed: Any) -> None:
    _client, stubber = stubbed("stepfunctions")
    stubber.add_response(
        "describe_state_machine", _machine(SALES_SM, kind="EXPRESS"), {"stateMachineArn": SALES_SM}
    )
    stubber.add_client_error(
        "describe_state_machine",
        service_error_code="StateMachineDoesNotExist",
        service_message="State Machine Does Not Exist",
        expected_params={"stateMachineArn": CUSTOMERS_SM},
    )
    stubber.add_response(
        "describe_state_machine", _machine(INVENTORY_SM, status="DELETING"), {"stateMachineArn": INVENTORY_SM}
    )
    result = check_state_machines(make_runtime())
    assert result.status == ERROR
    assert result.detail.split("; ") == [
        "sales_daily_etl: SalesDailyEtlStateMachine-AbC123 is an EXPRESS state machine (not supported: "
        "no execution history, redrive or status events)",
        f"customers_sync: state machine {CUSTOMERS_SM} not found",
        "inventory_report: InventoryReportStateMachine-XyZ789 is DELETING",
    ]


def test_a_throttled_state_machine_check_is_a_warning(make_runtime: Any, stubbed: Any) -> None:
    options = sfn_options(pipelines={"customers_sync": {"state_machine": CUSTOMERS_SM}})
    _client, stubber = stubbed("stepfunctions")
    stubber.add_client_error(
        "describe_state_machine",
        service_error_code="ThrottlingException",
        service_message="Rate exceeded",
        expected_params={"stateMachineArn": CUSTOMERS_SM},
    )
    result = check_state_machines(make_runtime(options))
    assert result.status == WARN and "ThrottlingException" in result.detail


def test_a_denied_state_machine_check_is_an_error(make_runtime: Any, stubbed: Any) -> None:
    options = sfn_options(pipelines={"customers_sync": {"state_machine": CUSTOMERS_SM}})
    _client, stubber = stubbed("stepfunctions")
    stubber.add_client_error(
        "describe_state_machine",
        service_error_code="AccessDeniedException",
        service_message="not authorized to perform: states:DescribeStateMachine",
        expected_params={"stateMachineArn": CUSTOMERS_SM},
    )
    result = check_state_machines(make_runtime(options))
    assert result.status == ERROR
    assert result.detail.startswith("customers_sync: DescribeStateMachine failed: AccessDeniedException")


def test_machines_configured_by_name_are_resolved(make_runtime: Any, stubbed: Any) -> None:
    options = sfn_options(
        pipelines={
            "customers_sync": {"state_machine": "customers-sync"},
            "ghost": {"state_machine": "ghost-machine"},
        }
    )
    _client, stubber = stubbed("stepfunctions")
    stubber.add_response(
        "list_state_machines", {"stateMachines": [_listed("customers-sync")]}, {"maxResults": 1000}
    )
    stubber.add_response("describe_state_machine", _machine(CUSTOMERS_SM), {"stateMachineArn": CUSTOMERS_SM})
    stubber.add_response(
        "list_state_machines", {"stateMachines": [_listed("customers-sync")]}, {"maxResults": 1000}
    )
    result = check_state_machines(make_runtime(options))
    assert result.status == ERROR
    assert result.detail == "ghost: state machine ghost-machine not found"


def test_discovered_machines_are_counted(make_runtime: Any, stubbed: Any) -> None:
    options = sfn_options(pipelines={}, discover=True)
    _client, stubber = stubbed("stepfunctions")
    stubber.add_response(
        "list_state_machines",
        {"stateMachines": [_listed("a"), _listed("fast", "EXPRESS")], "nextToken": "page-2"},
        {"maxResults": 1000},
    )
    stubber.add_response(
        "list_state_machines", {"stateMachines": [_listed("b")]}, {"maxResults": 1000, "nextToken": "page-2"}
    )
    result = check_state_machines(make_runtime(options))
    assert (result.status, result.detail) == (
        OK,
        "discover: 2 STANDARD state machine(s) watched, 1 EXPRESS skipped (not supported)",
    )


@pytest.mark.parametrize(
    "name",
    [
        "EtlStateMachine-a1B2c3",
        "SalesDailyEtlStateMachine-AbC123XyZ",
        "SalesDailyEtlStateMachine-AbC123",
        "InventoryReportStateMachine-XyZ789",
        "OrdersStateMachine-1A2B3C4D5E6F",
        "Generated-ZXCVBN123456",
        "sfn-etl-EtlStateMachine-k9Lm2nPq4rSt",
    ],
)
def test_generated_looking_names(name: str) -> None:
    assert doctor.looks_generated(name) is True


@pytest.mark.parametrize(
    "name",
    [
        "customers-sync",
        "sales_daily",
        "sales-daily-etl",
        "orders-pipeline",
        "nightly-20261002",
        "sales-report2",
        "OrdersStateMachine",
        "orders-ABCDEFGHIJKLMN1",
        "EtlStateMachine-a1B2c",
        "my_machine-AbC123",
        "-AbC123",
        "",
    ],
)
def test_names_people_choose_do_not_look_generated(name: str) -> None:
    assert doctor.looks_generated(name) is False


def test_a_discovered_machine_with_a_generated_name_is_a_warning(make_runtime: Any, stubbed: Any) -> None:
    options = sfn_options(pipelines={"customers_sync": {"state_machine": CUSTOMERS_SM}}, discover=True)
    _client, stubber = stubbed("stepfunctions")
    stubber.add_response("describe_state_machine", _machine(CUSTOMERS_SM), {"stateMachineArn": CUSTOMERS_SM})
    stubber.add_response(
        "list_state_machines",
        {
            "stateMachines": [
                _listed("customers-sync"),
                _listed("SalesDailyEtlStateMachine-AbC123"),
                _listed("nightly-report"),
                _listed("ClickstreamStateMachine-Qw3Er5", "EXPRESS"),
            ]
        },
        {"maxResults": 1000},
    )
    result = check_state_machines(make_runtime(options))
    assert result.status == WARN
    assert result.detail == (
        "discover: state machine SalesDailyEtlStateMachine-AbC123 has a name CloudFormation/SAM "
        "generated; its pipeline id is that name, which a redeploy may change (detaching its contracts "
        "and incident history): add an orchestrator.options.pipelines entry for it, with its logical_id; "
        "1 state machine(s): customers_sync (STANDARD); "
        "discover: 3 STANDARD state machine(s) watched, 1 EXPRESS skipped (not supported)"
    )


def test_a_configured_machine_with_a_generated_name_is_fine(make_runtime: Any, stubbed: Any) -> None:
    options = sfn_options(pipelines={"sales_daily_etl": {"state_machine": SALES_SM}}, discover=True)
    _client, stubber = stubbed("stepfunctions")
    stubber.add_response("describe_state_machine", _machine(SALES_SM), {"stateMachineArn": SALES_SM})
    stubber.add_response(
        "list_state_machines",
        {"stateMachines": [_listed("SalesDailyEtlStateMachine-AbC123"), _listed("customers-sync")]},
        {"maxResults": 1000},
    )
    result = check_state_machines(make_runtime(options))
    assert (result.status, result.detail) == (
        OK,
        "1 state machine(s): sales_daily_etl (STANDARD); discover: 2 STANDARD state machine(s) watched",
    )


def test_several_generated_names_are_named_up_to_a_limit(make_runtime: Any, stubbed: Any) -> None:
    names = [f"Etl{index}StateMachine-AbC12{index}" for index in range(7)]
    _client, stubber = stubbed("stepfunctions")
    stubber.add_response(
        "list_state_machines", {"stateMachines": [_listed(name) for name in names]}, {"maxResults": 1000}
    )
    result = check_state_machines(make_runtime(sfn_options(pipelines={}, discover=True)))
    assert result.status == WARN
    assert result.detail.startswith(
        f"discover: state machines {', '.join(names[:5])} and 2 more have names CloudFormation/SAM generated"
    )
    assert result.detail.endswith("; discover: 7 STANDARD state machine(s) watched")


def test_a_discovered_name_taken_by_another_pipeline_is_a_warning(make_runtime: Any, stubbed: Any) -> None:
    options = sfn_options(pipelines={"customers-sync": {"state_machine": SALES_SM}}, discover=True)
    _client, stubber = stubbed("stepfunctions")
    stubber.add_response("describe_state_machine", _machine(SALES_SM), {"stateMachineArn": SALES_SM})
    stubber.add_response(
        "list_state_machines",
        {"stateMachines": [_listed("customers-sync"), _listed("SalesDailyEtlStateMachine-AbC123")]},
        {"maxResults": 1000},
    )
    result = check_state_machines(make_runtime(options))
    assert (result.status, result.detail) == (
        WARN,
        "discover: state machine customers-sync is not watched: its name is the pipeline id of another "
        "state machine (add an orchestrator.options.pipelines entry for it); "
        "1 state machine(s): customers-sync (STANDARD); discover: 1 STANDARD state machine(s) watched",
    )


def test_discovery_warnings_follow_a_failed_configured_machine(make_runtime: Any, stubbed: Any) -> None:
    options = sfn_options(pipelines={"customers_sync": {"state_machine": CUSTOMERS_SM}}, discover=True)
    _client, stubber = stubbed("stepfunctions")
    stubber.add_client_error(
        "describe_state_machine",
        service_error_code="StateMachineDoesNotExist",
        http_status_code=400,
        expected_params={"stateMachineArn": CUSTOMERS_SM},
    )
    stubber.add_response(
        "list_state_machines",
        {"stateMachines": [_listed("EtlStateMachine-a1B2c3")]},
        {"maxResults": 1000},
    )
    result = check_state_machines(make_runtime(options))
    assert result.status == ERROR
    assert result.detail.startswith(f"customers_sync: state machine {CUSTOMERS_SM} not found; ")
    assert "state machine EtlStateMachine-a1B2c3 has a name CloudFormation/SAM generated" in result.detail


def test_nothing_watched_is_a_warning(make_runtime: Any) -> None:
    result = check_state_machines(make_runtime(sfn_options(pipelines={})))
    assert result.status == WARN and "nothing is watched" in result.detail


# =========================================================================== code mapping


def test_unmapped_states_and_missing_pipelines_are_warnings(make_runtime: Any, tmp_path: Path) -> None:
    locator = StandInLocator(
        specs=[
            _spec("sales_daily_etl", "TransformSales"),
            _spec("inventory_report", "BuildReport", mapped=False),
        ],
        unmapped={"inventory_report": ["BuildReport"], "sales_daily_etl": [], "customers_sync": ["X"]},
    )
    result = check_code_mapping(make_runtime(cfg=_config(tmp_path)), locator)  # type: ignore[arg-type]
    assert result.status == WARN
    assert result.detail.split("; ") == [
        "2 pipeline(s), 1 Lambda state(s) mapped to repository code",
        "not found in the repository: customers_sync",
        "inventory_report: no code found for BuildReport",
        "add orchestrator.options.code_map entries or fix the template",
    ]


def test_an_unreadable_template_is_an_error(make_runtime: Any, tmp_path: Path) -> None:
    locator = StandInLocator(error=ValueError("template.yaml: invalid YAML"))
    result = check_code_mapping(make_runtime(cfg=_config(tmp_path)), locator)  # type: ignore[arg-type]
    assert (result.status, result.detail) == (
        ERROR,
        "the repository cannot be mapped: template.yaml: invalid YAML",
    )


def test_an_invalid_template_of_the_real_locator_is_an_error(tmp_path: Path) -> None:
    """The locator does not raise for an invalid template (every pipeline would just be missing):
    the check asks it what is wrong and reports the file and the line."""
    import shutil

    repo = tmp_path / "repo"
    shutil.copytree(SFN_REPO, repo)
    (repo / "template.yaml").write_text("Resources: [unclosed\n", encoding="utf-8")
    cfg = load_config(repo / "healer.yaml")
    adapter = FACTORY.create(cfg, client=lambda service: None)
    result = check_code_mapping(adapter.runtime, adapter.code)
    assert result.status == ERROR
    assert result.detail.startswith("the SAM template cannot be read: template.yaml: invalid template: ")
    assert "(line 2)" in result.detail and str(tmp_path) not in result.detail


def test_code_mapping_of_the_fixture_repository() -> None:
    """The real locator on ``tests/unit/fixtures/repos/sfn_etl``: every Lambda state is mapped."""
    cfg = load_config(SFN_REPO / "healer.yaml")
    adapter = FACTORY.create(cfg, client=lambda service: None)
    result = check_code_mapping(adapter.runtime, adapter.code)
    assert result.status == OK, result.detail
    assert result.detail.startswith("3 pipeline(s), ")
    assert check_monitor_modes(adapter.runtime).status == OK


def test_code_mapping_needs_a_locator_and_a_configuration(make_runtime: Any, tmp_path: Path) -> None:
    assert check_code_mapping(make_runtime(cfg=_config(tmp_path)), None).status == SKIP
    assert check_code_mapping(make_runtime(), StandInLocator()).status == SKIP  # type: ignore[arg-type]


# =========================================================================== monitor modes


MODES_CASES: list[tuple[str, dict[str, Any], dict[str, Any], str, list[str]]] = [
    (
        "poll and queue",
        {"modes": ["poll", "queue"]},
        {"queue_url": QUEUE_URL},
        OK,
        ["modes poll, queue; trigger final_failure"],
    ),
    ("poll only", {"modes": ["poll"]}, {}, OK, ["modes poll; trigger final_failure"]),
    (
        "Airflow modes",
        {"modes": ["rest", "listener"]},
        {},
        ERROR,
        [
            "monitor.modes listener cannot serve AWS Step Functions: use poll, push or queue "
            "(EventBridge events need push)",
            "monitor.modes rest is the Airflow name of poll: it polls the state machines, but write poll",
        ],
    ),
    (
        "rest polls",  # the core polls through the feed for rest as for poll
        {"modes": ["rest"]},
        {},
        WARN,
        ["monitor.modes rest is the Airflow name of poll: it polls the state machines, but write poll"],
    ),
    (
        "no mode at all",
        {"modes": []},
        {},
        ERROR,
        [
            "monitor.modes is empty: no failure is ever detected (use poll, push or queue)",
            "monitor.modes has no poll: failures whose EventBridge event is lost are never seen "
            "(delivery is best effort)",
        ],
    ),
    (
        "queue without a queue_url",
        {"modes": ["poll", "queue"]},
        {},
        ERROR,
        ["monitor.modes has queue but orchestrator.options.queue_url is not set"],
    ),
    (
        "every_failure",
        {"modes": ["poll"], "trigger": "every_failure"},
        {},
        ERROR,
        [
            "monitor.trigger every_failure is not supported: Step Functions retries are not visible "
            "as failed attempts (use final_failure)"
        ],
    ),
    (
        "a queue_url nobody reads",
        {"modes": ["poll"]},
        {"queue_url": QUEUE_URL},
        WARN,
        ["orchestrator.options.queue_url is set but monitor.modes has no queue: it is never read"],
    ),
    (
        "no poll",
        {"modes": ["queue"]},
        {"queue_url": QUEUE_URL},
        WARN,
        [
            "monitor.modes has no poll: failures whose EventBridge event is lost are never seen (delivery is "
            "best effort)"
        ],
    ),
    (
        "push without a token",
        {"modes": ["poll", "push"]},
        {},
        WARN,
        [
            "monitor.modes has push but no ingest token is set (HEALER_INGEST_TOKEN): "
            "POST /events/eventbridge refuses every event"
        ],
    ),
]


@pytest.mark.parametrize(
    ("monitor", "overrides", "status", "details"),
    [case[1:] for case in MODES_CASES],
    ids=[case[0] for case in MODES_CASES],
)
def test_monitor_modes(
    make_runtime: Any,
    tmp_path: Path,
    monitor: dict[str, Any],
    overrides: dict[str, Any],
    status: str,
    details: list[str],
) -> None:
    runtime = make_runtime(sfn_options(**overrides), cfg=_config(tmp_path, monitor=monitor))
    result = check_monitor_modes(runtime)
    assert (result.name, result.status) == (MONITOR_MODES_CHECK, status)
    assert result.detail.split("; ") == details if status != OK else result.detail == details[0]


def test_push_with_an_ingest_token(
    make_runtime: Any, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    cfg = _config(tmp_path, monitor={"modes": ["poll", "push"]})
    monkeypatch.setenv(doctor.INGEST_TOKEN_ENV, "set-in-the-environment")
    result = check_monitor_modes(make_runtime(cfg=cfg))
    assert result.status == OK and "set-in-the-environment" not in result.detail


def test_monitor_modes_without_a_configuration(make_runtime: Any) -> None:
    assert check_monitor_modes(make_runtime()).status == SKIP


# =========================================================================== event queue


def test_a_queue_without_a_dead_letter_queue_is_a_warning(make_runtime: Any, stubbed: Any) -> None:
    _client, stubber = stubbed("sqs")
    stubber.add_response(
        "get_queue_attributes",
        {"Attributes": {"ApproximateNumberOfMessages": "0", "ApproximateNumberOfMessagesNotVisible": "2"}},
        {"QueueUrl": QUEUE_URL, "AttributeNames": list(QUEUE_ATTRIBUTES)},
    )
    result = check_event_queue(make_runtime(sfn_options(queue_url=QUEUE_URL)))
    assert result.status == WARN
    assert result.detail == (
        "healer-sfn-events: 0 message(s) waiting, 2 in flight; no dead-letter queue (RedrivePolicy) for "
        "messages the worker cannot process"
    )


@pytest.mark.parametrize(
    ("code", "expected"),
    [
        ("AWS.SimpleQueueService.NonExistentQueue", ERROR),
        ("QueueDoesNotExist", ERROR),
        ("AccessDenied", ERROR),
        ("RequestThrottled", WARN),
    ],
)
def test_queue_errors(make_runtime: Any, stubbed: Any, code: str, expected: str) -> None:
    _client, stubber = stubbed("sqs")
    stubber.add_client_error(
        "get_queue_attributes",
        service_error_code=code,
        service_message="refused",
        expected_params={"QueueUrl": QUEUE_URL, "AttributeNames": list(QUEUE_ATTRIBUTES)},
    )
    result = check_event_queue(make_runtime(sfn_options(queue_url=QUEUE_URL)))
    assert result.status == expected and result.detail.startswith("healer-sfn-events: ")


def test_odd_queue_attributes(make_runtime: Any, stubbed: Any) -> None:
    _client, stubber = stubbed("sqs")
    stubber.add_response(
        "get_queue_attributes",
        {"Attributes": {"ApproximateNumberOfMessages": "many", "RedrivePolicy": "{not json"}},
        {"QueueUrl": QUEUE_URL, "AttributeNames": list(QUEUE_ATTRIBUTES)},
    )
    result = check_event_queue(make_runtime(sfn_options(queue_url=QUEUE_URL)))
    assert result.status == WARN and result.detail.startswith(
        "healer-sfn-events: ? message(s) waiting, ? in flight"
    )


def test_no_queue_is_skipped(make_runtime: Any) -> None:
    assert check_event_queue(make_runtime()).status == SKIP
