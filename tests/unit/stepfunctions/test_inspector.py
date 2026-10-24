"""Tests of dag_healer.stepfunctions.inspector: describe, logs and pipeline_source (design 9.6).

Step Functions answers come from ``tests/unit/fixtures/stepfunctions/api/evidence`` (through
``api_response``: timestamps become datetimes, as botocore returns them); every call goes through
a ``botocore.stub.Stubber`` with its exact request parameters, in call order.
"""

from __future__ import annotations

import json
import logging
from collections.abc import Callable
from datetime import datetime
from pathlib import Path
from types import SimpleNamespace
from typing import Any

import pytest

from dag_healer.agent.context import extract_traceback
from dag_healer.core.errors import NotFound, OrchestratorError
from dag_healer.core.evidence import AttemptDetail, LogBundle, PipelineSource
from dag_healer.core.failures import AttemptState
from dag_healer.core.ids import RUN_LEVEL_STEP, AttemptRef
from dag_healer.orchestrators.conformance import assert_native_fits
from dag_healer.stepfunctions import arns, history
from dag_healer.stepfunctions.inspector import ORIGIN_CURRENT, ORIGIN_RAN, SfnInspector, _recorded_evidence
from dag_healer.stepfunctions.locator import SfnCodeLocator
from dag_healer.stepfunctions.logs import FILTER_PAGE_LIMIT, LOOKUP_DISABLED_NOTE
from dag_healer.stepfunctions.runtime import epoch_ms, to_utc

from .conftest import (
    CUSTOMERS_SM,
    INSTANCE,
    INVENTORY_SM,
    SALES_SM,
    SFN_REPO,
    api_response,
    load_json,
    sfn_options,
)

SALES_EXECUTION = arns.execution_arn(SALES_SM, "sales-2026-10-02")
CUSTOMERS_EXECUTION = arns.execution_arn(CUSTOMERS_SM, "customers-2026-10-02")
INVENTORY_EXECUTION = arns.execution_arn(INVENTORY_SM, "inventory-2026-10-02")
SALES_FUNCTION = "arn:aws:lambda:eu-west-1:123456789012:function:sfn-etl-SalesFunction-1A2B3C4D5E6F"
CUSTOMERS_FUNCTION = "arn:aws:lambda:eu-west-1:123456789012:function:sfn-etl-CustomersFunction-7G8H9I0J1K2L"
INVENTORY_FUNCTION = "arn:aws:lambda:eu-west-1:123456789012:function:sfn-etl-InventoryFunction-3M4N5O6P7Q8R"
SALES_RID = "6f1c2d3e-4a5b-4c6d-8e7f-9a0b1c2d3e4f"
CUSTOMERS_REDRIVE_RID = "8d2f4e6a-1b3c-4d5e-8f7a-6b5c4d3e2f1a"
SALES_STREAM = "2026/10/02/[$LATEST]0a1b2c3d4e5f60718293a4b5c6d7e8f9"
PII = "alice.pii@example.com"

SALES_REF = AttemptRef(INSTANCE, "sales_daily_etl", "sales-2026-10-02", "TransformSales")
CUSTOMERS_REF = AttemptRef(INSTANCE, "customers_sync", "customers-2026-10-02", "TransformCustomers")
INVENTORY_REF = AttemptRef(INSTANCE, "inventory_report", "inventory-2026-10-02", "BuildReport", 0)


def at(text: str) -> datetime:
    moment = to_utc(text)
    assert moment is not None
    return moment


def ms(text: str) -> int:
    value = epoch_ms(text)
    assert value is not None
    return value


def fixture(name: str, edit: Callable[[dict[str, Any]], None] | None = None) -> dict[str, Any]:
    """An ``api/evidence`` answer (``edit`` changes the decoded JSON first)."""
    data = load_json(f"api/evidence/{name}.json")
    if edit is not None:
        edit(data)
    return api_response(data)


def by_id(data: dict[str, Any]) -> dict[int, dict[str, Any]]:
    return {event["id"]: event for event in data["events"]}


def expect_describe(stubbed: Any, arn: str, answer: dict[str, Any] | str) -> None:
    _client, stubber = stubbed("stepfunctions")
    stubber.add_response(
        "describe_execution", fixture(answer) if isinstance(answer, str) else answer, {"executionArn": arn}
    )


def expect_history(stubbed: Any, arn: str, answer: dict[str, Any] | str) -> None:
    _client, stubber = stubbed("stepfunctions")
    stubber.add_response(
        "get_execution_history",
        fixture(answer) if isinstance(answer, str) else answer,
        {"executionArn": arn, "reverseOrder": True, "maxResults": 1000, "includeExecutionData": False},
    )


def expect_sfn_error(
    stubbed: Any, operation: str, code: str, params: dict[str, Any], *, status: int = 400
) -> None:
    _client, stubber = stubbed("stepfunctions")
    stubber.add_client_error(
        operation,
        service_error_code=code,
        service_message="AWS says no",
        http_status_code=status,
        expected_params=params,
    )


def expect_definition(stubbed: Any, arn: str) -> None:
    _client, stubber = stubbed("stepfunctions")
    stubber.add_response(
        "describe_state_machine_for_execution",
        fixture("describe_state_machine_for_execution_customers"),
        {"executionArn": arn},
    )


def no_client(service: str) -> Any:
    raise AssertionError(f"no {service} client may be built")


def frames_of(text: str) -> list[tuple[str, int, str | None]]:
    _type, _message, frames, _text = extract_traceback(text)
    return [(frame["file"], frame["line"], frame["function"]) for frame in frames]


# =========================================================================== describe


def test_describe_a_lambda_failure_from_the_history(stubbed: Any, make_runtime: Any) -> None:
    expect_describe(stubbed, SALES_EXECUTION, "describe_execution_sales")
    expect_history(stubbed, SALES_EXECUTION, "history_sales")

    detail = SfnInspector(make_runtime()).describe(SALES_REF, {})

    assert isinstance(detail, AttemptDetail) and detail.ref == SALES_REF
    assert (detail.state, detail.is_final) == (AttemptState.FAILED, True)
    assert (detail.error_name, detail.error_message) == ("KeyError", "'amount'")
    assert extract_traceback(detail.traceback_text)[:2] == ("KeyError", "'amount'")
    assert frames_of(detail.traceback_text) == [
        ("/var/task/app.py", 14, "handler"),
        ("/opt/python/etl/sales.py", 31, "transform_sales"),
        ("/opt/python/pandas/core/frame.py", 4102, "__getitem__"),
        ("/opt/python/pandas/core/indexes/base.py", 3812, "get_loc"),
    ]
    cause = fixture("describe_execution_sales")["cause"]
    assert detail.raw_error == f"KeyError: {cause}"
    assert (detail.executor, detail.resource, detail.max_attempts) == ("lambda", SALES_FUNCTION, None)
    assert (detail.started_at, detail.ended_at) == (
        at("2026-10-02T06:00:20.100Z"),
        at("2026-10-02T06:00:41.400Z"),
    )
    assert detail.native == {
        "execution_arn": SALES_EXECUTION,
        "state_machine_arn": SALES_SM,
        "start_date": "2026-10-02T06:00:00.000+00:00",
        "stop_date": "2026-10-02T06:00:41.512+00:00",
        "status": "FAILED",
        "redrive_count": 0,
        "error": "KeyError",
        "failed_event_id": 10,
        "entered_event_id": 7,
        "event_type": "TaskFailed",
        "resource": "invoke",
        "resource_type": "lambda",
        "function_name": SALES_FUNCTION,
        "request_id": SALES_RID,
        "retry_attempts": 0,
    }
    assert_native_fits(detail.native)


def test_describe_never_keeps_or_logs_execution_data(stubbed: Any, make_runtime: Any, caplog: Any) -> None:
    caplog.set_level(logging.DEBUG)
    execution = fixture("describe_execution_customers")
    assert PII in execution["input"]
    assert PII in json.dumps(load_json("api/evidence/history_customers.json"))
    expect_describe(stubbed, CUSTOMERS_EXECUTION, execution)
    expect_history(stubbed, CUSTOMERS_EXECUTION, "history_customers")

    detail = SfnInspector(make_runtime()).describe(CUSTOMERS_REF, {})

    assert PII not in json.dumps(detail.native) and PII not in detail.raw_error
    assert not [record for record in caplog.records if PII in record.getMessage()]
    assert detail.error_message == "time data '2026-10-02T14:03:00Z' does not match format '%Y-%m-%d'"
    assert ("/opt/python/etl/customers.py", 12, "parse_signup_date") in frames_of(detail.traceback_text)
    assert detail.native["function_name"] == CUSTOMERS_FUNCTION and "request_id" not in detail.native
    assert_native_fits(detail.native)


def test_describe_an_inline_map_iteration_with_legacy_lambda_events(stubbed: Any, make_runtime: Any) -> None:
    expect_describe(stubbed, INVENTORY_EXECUTION, "describe_execution_inventory")
    expect_history(stubbed, INVENTORY_EXECUTION, "history_inventory")

    detail = SfnInspector(make_runtime()).describe(INVENTORY_REF, {})

    assert (detail.executor, detail.resource) == ("lambda", INVENTORY_FUNCTION)
    assert (detail.started_at, detail.ended_at) == (
        at("2026-10-02T06:00:00.180Z"),
        at("2026-10-02T06:00:41.300Z"),
    )
    assert detail.native["step_path"] == "PerWarehouse[0]/BuildReport"
    assert detail.native["event_type"] == "LambdaFunctionFailed"
    assert detail.native["resource"] == INVENTORY_FUNCTION
    assert frames_of(detail.traceback_text)[-1] == ("/opt/python/etl/inventory.py", 22, "build_report")


def test_the_recorded_failure_event_is_the_evidence(stubbed: Any, make_runtime: Any) -> None:
    """``native.failed_event_id`` names the first (retried) failure: that event is described."""
    expect_describe(stubbed, SALES_EXECUTION, "describe_execution_sales")
    expect_history(stubbed, SALES_EXECUTION, "history_sales_retried")
    native = {"failed_event_id": 10, "function_name": SALES_FUNCTION, "retry_attempts": 0, "localized": True}

    detail = SfnInspector(make_runtime()).describe(SALES_REF, native)

    assert detail.error_name == "Lambda.ServiceException"
    assert detail.error_message == "Lambda is unable to set up VPC access for the Lambda function."
    assert extract_traceback(detail.traceback_text)[0] == "Lambda.ServiceException"
    assert (detail.started_at, detail.ended_at) == (
        at("2026-10-02T06:00:20.100Z"),
        at("2026-10-02T06:00:20.900Z"),
    )
    assert {key: detail.native.get(key) for key in ("failed_event_id", "entered_event_id", "event_type")} == {
        "failed_event_id": 10,
        "entered_event_id": 7,
        "event_type": "TaskFailed",
    }
    assert (detail.native["retry_attempts"], detail.native["localized"]) == (0, True)
    assert detail.native["function_name"] == SALES_FUNCTION and detail.executor == "lambda"


@pytest.mark.parametrize("recorded", [999, 9, 13])
def test_a_recorded_event_that_is_not_another_failure_is_relocalized(
    stubbed: Any, make_runtime: Any, recorded: int
) -> None:
    expect_describe(stubbed, SALES_EXECUTION, "describe_execution_sales")
    expect_history(stubbed, SALES_EXECUTION, "history_sales_retried")

    detail = SfnInspector(make_runtime()).describe(SALES_REF, {"failed_event_id": recorded})

    assert detail.error_name == "KeyError"
    assert (detail.native["failed_event_id"], detail.native["retry_attempts"]) == (13, 1)
    assert detail.started_at == at("2026-10-02T06:00:22.950Z")


def test_a_run_level_ref_uses_the_terminal_event(stubbed: Any, make_runtime: Any) -> None:
    expect_describe(stubbed, SALES_EXECUTION, "describe_execution_sales_timed_out")
    expect_history(stubbed, SALES_EXECUTION, "history_sales_timed_out")
    ref = AttemptRef(INSTANCE, "sales_daily_etl", "sales-2026-10-02", RUN_LEVEL_STEP)

    detail = SfnInspector(make_runtime()).describe(ref, {})

    assert (detail.state, detail.is_final) == (AttemptState.FAILED, True)
    assert (detail.error_name, detail.error_message, detail.traceback_text) == ("States.Timeout", None, "")
    assert (detail.raw_error, detail.executor, detail.resource) == ("States.Timeout", None, None)
    assert (detail.started_at, detail.ended_at) == (
        at("2026-10-02T06:00:00.000Z"),
        at("2026-10-02T06:05:00.000Z"),
    )
    assert detail.native["event_type"] == "ExecutionTimedOut" and detail.native["status"] == "TIMED_OUT"


def test_a_run_level_ref_of_a_failed_step_names_the_terminal_error(stubbed: Any, make_runtime: Any) -> None:
    """A RUN_LEVEL_STEP incident (an unreadable history at detection) still gets the traceback."""
    expect_describe(stubbed, SALES_EXECUTION, "describe_execution_sales")
    expect_history(stubbed, SALES_EXECUTION, "history_sales")
    ref = AttemptRef(INSTANCE, "sales_daily_etl", "sales-2026-10-02", RUN_LEVEL_STEP)

    detail = SfnInspector(make_runtime()).describe(ref, {"localized": False})

    assert detail.native["event_type"] == "ExecutionFailed" and detail.native["localized"] is False
    assert extract_traceback(detail.traceback_text)[:2] == ("KeyError", "'amount'")
    assert detail.ended_at == at("2026-10-02T06:00:41.512Z")


def _drop_causes(data: dict[str, Any]) -> None:
    for event in data["events"]:
        for key, value in event.items():
            if key.endswith("EventDetails") and isinstance(value, dict):
                value.pop("cause", None)


def test_a_cause_missing_from_the_history_comes_from_describe_execution(
    stubbed: Any, make_runtime: Any
) -> None:
    expect_describe(stubbed, SALES_EXECUTION, "describe_execution_sales")
    expect_history(stubbed, SALES_EXECUTION, fixture("history_sales", _drop_causes))

    detail = SfnInspector(make_runtime()).describe(SALES_REF, {})

    assert extract_traceback(detail.traceback_text)[:2] == ("KeyError", "'amount'")
    assert detail.native["request_id"] == SALES_RID


def test_an_earlier_attempt_never_borrows_the_current_cause(stubbed: Any, make_runtime: Any) -> None:
    expect_describe(stubbed, CUSTOMERS_EXECUTION, "describe_execution_customers_redriven")
    expect_history(stubbed, CUSTOMERS_EXECUTION, fixture("history_customers_redriven", _drop_causes))

    detail = SfnInspector(make_runtime()).describe(CUSTOMERS_REF, {})

    assert (detail.error_name, detail.traceback_text, detail.error_message) == ("ValueError", "", None)


def test_a_cause_of_another_error_is_not_borrowed(stubbed: Any, make_runtime: Any) -> None:
    def other_error(data: dict[str, Any]) -> None:
        _drop_causes(data)
        failed = by_id(data)[10]["taskFailedEventDetails"]
        failed["error"] = "States.TaskFailed"

    expect_describe(stubbed, SALES_EXECUTION, "describe_execution_sales")
    expect_history(stubbed, SALES_EXECUTION, fixture("history_sales", other_error))

    detail = SfnInspector(make_runtime()).describe(SALES_REF, {})

    assert (detail.error_name, detail.traceback_text, detail.raw_error) == (
        "States.TaskFailed",
        "",
        "States.TaskFailed",
    )


def _redacted_parameters(data: dict[str, Any]) -> None:
    by_id(data)[8]["taskScheduledEventDetails"]["parameters"] = ""


def test_the_definition_that_ran_names_the_function(stubbed: Any, make_runtime: Any) -> None:
    expect_describe(stubbed, CUSTOMERS_EXECUTION, "describe_execution_customers")
    expect_history(stubbed, CUSTOMERS_EXECUTION, fixture("history_customers", _redacted_parameters))
    expect_definition(stubbed, CUSTOMERS_EXECUTION)

    detail = SfnInspector(make_runtime()).describe(CUSTOMERS_REF, {})

    assert (detail.executor, detail.resource) == ("lambda", CUSTOMERS_FUNCTION)
    assert detail.native["function_name"] == CUSTOMERS_FUNCTION


def test_a_function_in_native_spares_the_definition(stubbed: Any, make_runtime: Any) -> None:
    expect_describe(stubbed, CUSTOMERS_EXECUTION, "describe_execution_customers")
    expect_history(stubbed, CUSTOMERS_EXECUTION, fixture("history_customers", _redacted_parameters))

    detail = SfnInspector(make_runtime()).describe(
        CUSTOMERS_REF, {"function_name": "sfn-etl-CustomersFunction"}
    )

    assert detail.native["function_name"] == "sfn-etl-CustomersFunction" == detail.resource


class LogGroupLocator:
    """The locator method ``describe`` uses for ``native.log_group`` (the template's group)."""

    def __init__(self, group: str | None = None, error: Exception | None = None) -> None:
        self.group = group
        self.error = error
        self.calls: list[tuple[Path, str, str]] = []

    def log_group(self, repo_root: Path, pipeline_id: str, step_id: str) -> str | None:
        self.calls.append((repo_root, pipeline_id, step_id))
        if self.error is not None:
            raise self.error
        return self.group


def test_the_template_log_group_of_the_function_is_recorded(stubbed: Any, make_runtime: Any) -> None:
    expect_describe(stubbed, INVENTORY_EXECUTION, "describe_execution_inventory")
    expect_history(stubbed, INVENTORY_EXECUTION, "history_inventory")
    locator = SfnCodeLocator(None, sfn_options())

    rt = make_runtime(cfg=SimpleNamespace(repo_root=SFN_REPO))
    detail = SfnInspector(rt, locator=locator).describe(INVENTORY_REF, {})

    assert detail.native["log_group"] == "/sfn-etl/inventory"
    assert detail.native["function_name"] == INVENTORY_FUNCTION
    assert_native_fits(detail.native)


def test_a_default_log_group_is_not_recorded(stubbed: Any, make_runtime: Any) -> None:
    expect_describe(stubbed, SALES_EXECUTION, "describe_execution_sales")
    expect_history(stubbed, SALES_EXECUTION, "history_sales")
    locator = LogGroupLocator(None)

    detail = SfnInspector(make_runtime(cfg=SimpleNamespace(repo_root=SFN_REPO)), locator=locator).describe(
        SALES_REF, {}
    )

    assert "log_group" not in detail.native
    assert locator.calls == [(SFN_REPO, "sales_daily_etl", "TransformSales")]


@pytest.mark.parametrize(
    ("native", "locator", "expected", "lookups"),
    [
        ({"log_group": "/acme/sales"}, LogGroupLocator("/from/template"), "/acme/sales", 0),
        ({}, LogGroupLocator(error=OSError("unreadable")), None, 1),
        ({}, None, None, 0),
    ],
)
def test_a_recorded_log_group_wins_and_a_broken_repository_costs_nothing(
    stubbed: Any,
    make_runtime: Any,
    native: dict[str, Any],
    locator: LogGroupLocator | None,
    expected: str | None,
    lookups: int,
) -> None:
    expect_describe(stubbed, SALES_EXECUTION, "describe_execution_sales")
    expect_history(stubbed, SALES_EXECUTION, "history_sales")
    rt = make_runtime(cfg=SimpleNamespace(repo_root=SFN_REPO))

    detail = SfnInspector(rt, locator=locator).describe(SALES_REF, native)  # type: ignore[arg-type]

    assert detail.native.get("log_group") == expected
    assert len(locator.calls if locator is not None else []) == lookups


@pytest.mark.parametrize(
    ("definition", "resource"),
    [
        (
            {"Comment": "states only", "StartAt": "X", "States": {"X": {"Type": "Pass", "End": True}}},
            "invoke",
        ),
        (
            {
                "StartAt": "TransformCustomers",
                "States": {
                    "TransformCustomers": {
                        "Type": "Task",
                        "Resource": "arn:aws:states:::lambda:invoke",
                        "Parameters": {"FunctionName.$": "$.function", "Payload.$": "$"},
                        "End": True,
                    }
                },
            },
            "invoke",
        ),
    ],
)
def test_a_definition_without_a_literal_function(
    stubbed: Any, make_runtime: Any, definition: dict[str, Any], resource: str
) -> None:
    answer = fixture("describe_state_machine_for_execution_customers")
    answer["definition"] = json.dumps(definition)
    expect_describe(stubbed, CUSTOMERS_EXECUTION, "describe_execution_customers")
    expect_history(stubbed, CUSTOMERS_EXECUTION, fixture("history_customers", _redacted_parameters))
    _client, stubber = stubbed("stepfunctions")
    stubber.add_response(
        "describe_state_machine_for_execution", answer, {"executionArn": CUSTOMERS_EXECUTION}
    )

    detail = SfnInspector(make_runtime()).describe(CUSTOMERS_REF, {})

    assert (detail.executor, detail.resource) == ("lambda", resource)
    assert "function_name" not in detail.native


@pytest.mark.parametrize(
    ("code", "transient"), [("AccessDeniedException", False), ("ThrottlingException", True)]
)
def test_a_failing_definition_lookup(stubbed: Any, make_runtime: Any, code: str, transient: bool) -> None:
    expect_describe(stubbed, CUSTOMERS_EXECUTION, "describe_execution_customers")
    expect_history(stubbed, CUSTOMERS_EXECUTION, fixture("history_customers", _redacted_parameters))
    expect_sfn_error(
        stubbed, "describe_state_machine_for_execution", code, {"executionArn": CUSTOMERS_EXECUTION}
    )
    inspector = SfnInspector(make_runtime())

    if transient:
        with pytest.raises(OrchestratorError) as raised:
            inspector.describe(CUSTOMERS_REF, {})
        assert raised.value.transient
    else:
        detail = inspector.describe(CUSTOMERS_REF, {})
        assert (detail.executor, detail.resource) == ("lambda", "invoke")


# =========================================================================== attempts and states


def test_an_unknown_execution_is_not_found(stubbed: Any, make_runtime: Any) -> None:
    missing = AttemptRef(INSTANCE, "sales_daily_etl", "no-such-run", "TransformSales")
    arn = arns.execution_arn(SALES_SM, "no-such-run")
    expect_sfn_error(stubbed, "describe_execution", "ExecutionDoesNotExist", {"executionArn": arn})

    with pytest.raises(NotFound):
        SfnInspector(make_runtime()).describe(missing, {})


def test_an_attempt_the_execution_never_reached_is_not_found(stubbed: Any, make_runtime: Any) -> None:
    expect_describe(stubbed, CUSTOMERS_EXECUTION, "describe_execution_customers_redriven")

    with pytest.raises(NotFound, match="has 2 attempt"):
        SfnInspector(make_runtime()).describe(
            AttemptRef(INSTANCE, "customers_sync", "customers-2026-10-02", "X", -1, 3), {}
        )


def test_an_unknown_pipeline_is_not_found_without_io(make_runtime: Any) -> None:
    ref = AttemptRef(INSTANCE, "not_watched", "run-1", "Step")

    with pytest.raises(NotFound):
        SfnInspector(make_runtime(factory=no_client)).describe(ref, {})


def test_a_malformed_state_machine_in_native_is_not_found(make_runtime: Any) -> None:
    with pytest.raises(NotFound):
        SfnInspector(make_runtime(factory=no_client)).describe(SALES_REF, {"state_machine_arn": "not-an-arn"})


def test_native_names_the_execution(stubbed: Any, make_runtime: Any) -> None:
    expect_describe(stubbed, SALES_EXECUTION, "describe_execution_sales")
    expect_history(stubbed, SALES_EXECUTION, "history_sales")
    ref = AttemptRef(INSTANCE, "renamed_pipeline", "sales-2026-10-02", "TransformSales")

    detail = SfnInspector(make_runtime()).describe(ref, {"execution_arn": SALES_EXECUTION, "input": PII})

    assert detail.ref == ref and "input" not in detail.native


@pytest.mark.parametrize(("offset", "found"), [(0, True), (1, False)])
def test_a_reused_execution_name(stubbed: Any, make_runtime: Any, offset: int, found: bool) -> None:
    start = ms("2026-10-02T06:00:00.000Z") + offset
    ref = AttemptRef(INSTANCE, "customers_sync", f"customers-2026-10-02@{start}", "TransformCustomers")
    expect_describe(stubbed, CUSTOMERS_EXECUTION, "describe_execution_customers")
    inspector = SfnInspector(make_runtime())

    if found:
        expect_history(stubbed, CUSTOMERS_EXECUTION, "history_customers")
        assert inspector.describe(ref, {}).ref == ref
    else:
        with pytest.raises(NotFound, match="started again under the same name"):
            inspector.describe(ref, {})


def test_an_execution_name_with_an_at_sign_and_digits_is_described(stubbed: Any, make_runtime: Any) -> None:
    """``customers-2026-10-02@1790920800001`` is the execution's own name (its start is ...800000):
    no reuse marker, so the execution is the run."""
    name = "customers-2026-10-02@1790920800001"
    arn = arns.execution_arn(CUSTOMERS_SM, name)

    def renamed(data: dict[str, Any]) -> None:
        data.update(executionArn=arn, name=name)

    expect_describe(stubbed, arn, fixture("describe_execution_customers", renamed))
    expect_history(stubbed, arn, "history_customers")
    ref = AttemptRef(INSTANCE, "customers_sync", name, "TransformCustomers")

    detail = SfnInspector(make_runtime()).describe(ref, {"execution_arn": arn})

    assert detail.ref == ref and detail.native["execution_arn"] == arn
    assert (detail.state, detail.error_name) == (AttemptState.FAILED, "ValueError")


def test_an_earlier_attempt_of_a_redriven_execution(stubbed: Any, make_runtime: Any) -> None:
    expect_describe(stubbed, CUSTOMERS_EXECUTION, "describe_execution_customers_redriven")
    expect_history(stubbed, CUSTOMERS_EXECUTION, "history_customers_redriven")

    detail = SfnInspector(make_runtime()).describe(CUSTOMERS_REF, {})

    assert (detail.state, detail.is_final) == (AttemptState.FAILED, True)
    assert detail.started_at == at("2026-10-02T06:00:15.100Z")
    assert (detail.native["failed_event_id"], detail.native["redrive_count"]) == (10, 0)
    assert not {"status", "stop_date", "request_id"} & set(detail.native)


def test_the_current_attempt_of_a_redriven_execution(stubbed: Any, make_runtime: Any) -> None:
    expect_describe(stubbed, CUSTOMERS_EXECUTION, "describe_execution_customers_redriven")
    expect_history(stubbed, CUSTOMERS_EXECUTION, "history_customers_redriven")
    ref = AttemptRef(INSTANCE, "customers_sync", "customers-2026-10-02", "TransformCustomers", -1, 2)

    detail = SfnInspector(make_runtime()).describe(ref, {})

    assert (detail.state, detail.is_final) == (AttemptState.FAILED, True)
    assert (detail.started_at, detail.ended_at) == (
        at("2026-10-02T06:30:00.100Z"),
        at("2026-10-02T06:30:18.700Z"),
    )
    assert detail.native["request_id"] == CUSTOMERS_REDRIVE_RID
    assert (detail.native["redrive_count"], detail.native["status"], detail.native["failed_event_id"]) == (
        1,
        "FAILED",
        16,
    )


def test_a_running_redrive(stubbed: Any, make_runtime: Any) -> None:
    expect_describe(stubbed, CUSTOMERS_EXECUTION, "describe_execution_customers_redrive_running")
    expect_history(stubbed, CUSTOMERS_EXECUTION, "history_customers_redrive_running")
    ref = AttemptRef(INSTANCE, "customers_sync", "customers-2026-10-02", "TransformCustomers", -1, 2)

    detail = SfnInspector(make_runtime()).describe(ref, {})

    assert (detail.state, detail.is_final) == (AttemptState.RUNNING, False)
    assert (detail.error_name, detail.traceback_text, detail.raw_error) == (None, "", "")
    assert (detail.started_at, detail.ended_at) == (at("2026-10-02T06:30:00.000Z"), None)
    assert detail.native["status"] == "RUNNING"


def test_the_failed_attempt_before_a_running_redrive(stubbed: Any, make_runtime: Any) -> None:
    expect_describe(stubbed, CUSTOMERS_EXECUTION, "describe_execution_customers_redrive_running")
    expect_history(stubbed, CUSTOMERS_EXECUTION, "history_customers_redrive_running")

    detail = SfnInspector(make_runtime()).describe(CUSTOMERS_REF, {})

    assert (detail.state, detail.is_final, detail.error_name) == (AttemptState.FAILED, True, "ValueError")


@pytest.mark.parametrize(
    ("status", "state", "final"),
    [
        ("ABORTED", AttemptState.FAILED, True),
        ("SUCCEEDED", AttemptState.SUCCEEDED, False),
        ("PENDING_REDRIVE", AttemptState.RUNNING, False),
        ("SOMETHING_NEW", AttemptState.UNKNOWN, False),
    ],
)
def test_the_state_follows_the_execution_status(
    stubbed: Any, make_runtime: Any, status: str, state: AttemptState, final: bool
) -> None:
    answer = fixture("describe_execution_sales")
    answer["status"] = status
    expect_describe(stubbed, SALES_EXECUTION, answer)
    expect_history(stubbed, SALES_EXECUTION, "history_sales")

    detail = SfnInspector(make_runtime()).describe(SALES_REF, {})

    assert (detail.state, detail.is_final) == (state, final)


def test_an_unreadable_history_falls_back_to_the_execution(stubbed: Any, make_runtime: Any) -> None:
    expect_describe(stubbed, SALES_EXECUTION, "describe_execution_sales")
    expect_sfn_error(
        stubbed,
        "get_execution_history",
        "AccessDeniedException",
        {
            "executionArn": SALES_EXECUTION,
            "reverseOrder": True,
            "maxResults": 1000,
            "includeExecutionData": False,
        },
    )

    detail = SfnInspector(make_runtime()).describe(SALES_REF, {})

    assert extract_traceback(detail.traceback_text)[:2] == ("KeyError", "'amount'")
    assert (detail.started_at, detail.ended_at) == (
        at("2026-10-02T06:00:00.000Z"),
        at("2026-10-02T06:00:41.512Z"),
    )
    assert "failed_event_id" not in detail.native and detail.native["request_id"] == SALES_RID


def test_an_unreadable_history_of_an_earlier_attempt_gives_no_error(stubbed: Any, make_runtime: Any) -> None:
    expect_describe(stubbed, CUSTOMERS_EXECUTION, "describe_execution_customers_redriven")
    expect_sfn_error(
        stubbed,
        "get_execution_history",
        "KmsAccessDeniedException",
        {
            "executionArn": CUSTOMERS_EXECUTION,
            "reverseOrder": True,
            "maxResults": 1000,
            "includeExecutionData": False,
        },
    )

    detail = SfnInspector(make_runtime()).describe(CUSTOMERS_REF, {})

    assert (detail.state, detail.error_name, detail.traceback_text) == (AttemptState.FAILED, None, "")


@pytest.mark.parametrize(
    ("operation", "code", "status"),
    [("describe_execution", "ThrottlingException", 400), ("get_execution_history", "InternalFailure", 500)],
)
def test_transient_errors_propagate(
    stubbed: Any, make_runtime: Any, operation: str, code: str, status: int
) -> None:
    history_params = {
        "executionArn": SALES_EXECUTION,
        "reverseOrder": True,
        "maxResults": 1000,
        "includeExecutionData": False,
    }
    if operation == "get_execution_history":
        expect_describe(stubbed, SALES_EXECUTION, "describe_execution_sales")
        expect_sfn_error(stubbed, operation, code, history_params, status=status)
    else:
        expect_sfn_error(stubbed, operation, code, {"executionArn": SALES_EXECUTION}, status=status)

    with pytest.raises(OrchestratorError) as raised:
        SfnInspector(make_runtime()).describe(SALES_REF, {})
    assert raised.value.transient


def test_an_execution_deleted_between_two_calls_is_not_found(stubbed: Any, make_runtime: Any) -> None:
    expect_describe(stubbed, SALES_EXECUTION, "describe_execution_sales")
    expect_sfn_error(
        stubbed,
        "get_execution_history",
        "ExecutionDoesNotExist",
        {
            "executionArn": SALES_EXECUTION,
            "reverseOrder": True,
            "maxResults": 1000,
            "includeExecutionData": False,
        },
    )

    with pytest.raises(NotFound):
        SfnInspector(make_runtime()).describe(SALES_REF, {})


# =========================================================================== error rendering


def _glue_task(data: dict[str, Any]) -> None:
    for event in data["events"]:
        for key, value in event.items():
            if key.startswith("task") and key.endswith("EventDetails") and event["id"] >= 8:
                value.update(resourceType="glue", resource="startJobRun.sync")
    glue_cause = json.dumps(
        {"JobName": "sales-compaction", "JobRunState": "FAILED", "ErrorMessage": "Out of memory"}
    )
    by_id(data)[10]["taskFailedEventDetails"].update(error="States.TaskFailed", cause=glue_cause)
    by_id(data)[11]["executionFailedEventDetails"].update(error="States.TaskFailed", cause=glue_cause)


def test_a_failure_that_is_not_a_lambda_payload(stubbed: Any, make_runtime: Any) -> None:
    expect_describe(stubbed, SALES_EXECUTION, "describe_execution_sales")
    expect_history(stubbed, SALES_EXECUTION, fixture("history_sales", _glue_task))
    inspector = SfnInspector(make_runtime())

    detail = inspector.describe(SALES_REF, {})

    cause = '{"JobName": "sales-compaction", "JobRunState": "FAILED", "ErrorMessage": "Out of memory"}'
    assert (detail.error_name, detail.error_message, detail.traceback_text) == (
        "States.TaskFailed",
        cause,
        "",
    )
    assert detail.raw_error == f"States.TaskFailed: {cause}"
    assert (detail.executor, detail.resource) == ("glue", "startJobRun.sync")
    bundle = inspector.logs(SALES_REF, detail)
    assert (bundle.text, bundle.confidence) == ("", "none")
    assert bundle.notes == ("State TransformSales ran a glue task: only Lambda logs are correlated.",)


def test_a_cause_cut_before_its_frames_leaves_the_traceback_to_the_log(
    stubbed: Any, make_runtime: Any
) -> None:
    cut = '{"errorMessage": "time data ' + "x" * 200

    def truncated(data: dict[str, Any]) -> None:
        by_id(data)[10]["taskFailedEventDetails"]["cause"] = cut
        by_id(data)[11]["executionFailedEventDetails"]["cause"] = cut

    expect_describe(stubbed, SALES_EXECUTION, "describe_execution_sales")
    expect_history(stubbed, SALES_EXECUTION, fixture("history_sales", truncated))

    detail = SfnInspector(make_runtime()).describe(SALES_REF, {})

    assert detail.traceback_text == "" and detail.error_name == "KeyError"
    assert detail.error_message == "time data " + "x" * 200 + " [truncated]"
    assert detail.raw_error == f"KeyError: {cut}"


def test_the_given_native_is_kept_and_cleaned(stubbed: Any, make_runtime: Any) -> None:
    expect_describe(stubbed, SALES_EXECUTION, "describe_execution_sales")
    expect_history(stubbed, SALES_EXECUTION, "history_sales")
    native = {
        "localized": True,
        "log_group": "/aws/lambda/x",
        "failed_children": [f"child-{n}" for n in range(9)],
        "input": {"email": PII},
        "unknown_key": "dropped",
        "request_id": "from-detection",
    }

    detail = SfnInspector(make_runtime()).describe(SALES_REF, native)

    assert detail.native["localized"] is True and detail.native["log_group"] == "/aws/lambda/x"
    assert detail.native["failed_children"] == [f"child-{n}" for n in range(5)]
    assert detail.native["request_id"] == SALES_RID  # the cause names the request
    assert not {"input", "unknown_key"} & set(detail.native)
    assert_native_fits(detail.native)


def test_an_oversized_native_falls_back_to_the_fields(stubbed: Any, make_runtime: Any) -> None:
    expect_describe(stubbed, SALES_EXECUTION, "describe_execution_sales")
    expect_history(stubbed, SALES_EXECUTION, "history_sales")
    native = {"failed_children": [{"name": "x" * 500, "error": "y" * 500} for _ in range(5)]}

    detail = SfnInspector(make_runtime()).describe(SALES_REF, native)

    assert "failed_children" not in detail.native and detail.native["failed_event_id"] == 10
    assert_native_fits(detail.native)


# =========================================================================== logs


def expect_sales_logs(stubbed: Any) -> None:
    _client, stubber = stubbed("lambda")
    stubber.add_response(
        "get_function", load_json("api/evidence/get_function_sales.json"), {"FunctionName": SALES_FUNCTION}
    )
    _client, stubber = stubbed("logs")
    group = "/aws/lambda/sfn-etl-SalesFunction-1A2B3C4D5E6F"
    stubber.add_response(
        "filter_log_events",
        load_json("logs/sales_request_id_matches.json"),
        {
            "logGroupName": group,
            "startTime": ms("2026-10-02T06:00:19.100Z"),
            "endTime": ms("2026-10-02T06:00:46.400Z"),
            "limit": FILTER_PAGE_LIMIT,
            "filterPattern": f'"{SALES_RID}"',
        },
    )
    stubber.add_response(
        "filter_log_events",
        load_json("logs/sales_invocation.json"),
        {
            "logGroupName": group,
            "startTime": ms("2026-10-02T06:00:20.150Z"),
            "endTime": ms("2026-10-02T06:00:41.350Z"),
            "limit": FILTER_PAGE_LIMIT,
            "logStreamNames": [SALES_STREAM],
        },
    )


def test_logs_without_a_detail_describe_the_attempt_first(stubbed: Any, make_runtime: Any) -> None:
    expect_describe(stubbed, SALES_EXECUTION, "describe_execution_sales")
    expect_history(stubbed, SALES_EXECUTION, "history_sales")
    expect_sales_logs(stubbed)

    bundle = SfnInspector(make_runtime()).logs(SALES_REF, None)

    assert (bundle.source, bundle.confidence) == (
        "cloudwatch:/aws/lambda/sfn-etl-SalesFunction-1A2B3C4D5E6F",
        "exact",
    )
    assert extract_traceback(bundle.text)[:2] == ("KeyError", "'amount'")


def test_logs_of_a_detail_do_no_describe(stubbed: Any, make_runtime: Any) -> None:
    expect_sales_logs(stubbed)
    detail = AttemptDetail(
        ref=SALES_REF,
        state=AttemptState.FAILED,
        is_final=True,
        error_name="KeyError",
        error_message="'amount'",
        executor="lambda",
        resource="invoke",
        started_at=at("2026-10-02T06:00:20.100Z"),
        ended_at=at("2026-10-02T06:00:41.400Z"),
        native={"function_name": SALES_FUNCTION, "request_id": SALES_RID},
    )

    bundle = SfnInspector(make_runtime()).logs(SALES_REF, detail, max_chars=50)

    assert bundle.truncated and len(bundle.text) <= 50 and bundle.confidence == "exact"


def test_logs_of_a_legacy_lambda_task_use_its_function_arn(stubbed: Any, make_runtime: Any) -> None:
    _client, stubber = stubbed("lambda")
    stubber.add_response(
        "get_function",
        load_json("api/evidence/get_function_inventory.json"),
        {"FunctionName": INVENTORY_FUNCTION},
    )
    _client, stubber = stubbed("logs")
    stubber.add_response(
        "filter_log_events",
        load_json("logs/inventory_window.json"),
        {
            "logGroupName": "/sfn-etl/inventory",
            "startTime": ms("2026-10-02T05:59:59.180Z"),
            "endTime": ms("2026-10-02T06:00:46.300Z"),
            "limit": FILTER_PAGE_LIMIT,
        },
    )
    detail = AttemptDetail(
        ref=INVENTORY_REF,
        state=AttemptState.FAILED,
        is_final=True,
        error_name="KeyError",
        error_message="'qty'",
        executor="lambda",
        resource=INVENTORY_FUNCTION,
        started_at=at("2026-10-02T06:00:00.180Z"),
        ended_at=at("2026-10-02T06:00:41.300Z"),
    )

    bundle = SfnInspector(make_runtime()).logs(INVENTORY_REF, detail)

    assert bundle.confidence == "heuristic" and extract_traceback(bundle.text)[0] == "KeyError"


def test_logs_fall_back_to_the_recorded_log_group_when_get_function_is_refused(
    stubbed: Any, make_runtime: Any
) -> None:
    _client, stubber = stubbed("lambda")
    stubber.add_client_error(
        "get_function",
        service_error_code="AccessDeniedException",
        http_status_code=403,
        expected_params={"FunctionName": INVENTORY_FUNCTION},
    )
    _client, stubber = stubbed("logs")
    stubber.add_response(
        "filter_log_events",
        load_json("logs/inventory_window.json"),
        {
            "logGroupName": "/sfn-etl/inventory",
            "startTime": ms("2026-10-02T05:59:59.180Z"),
            "endTime": ms("2026-10-02T06:00:46.300Z"),
            "limit": FILTER_PAGE_LIMIT,
        },
    )
    detail = AttemptDetail(
        ref=INVENTORY_REF,
        state=AttemptState.FAILED,
        is_final=True,
        error_name="KeyError",
        error_message="'qty'",
        executor="lambda",
        resource=INVENTORY_FUNCTION,
        started_at=at("2026-10-02T06:00:00.180Z"),
        ended_at=at("2026-10-02T06:00:41.300Z"),
        native={"function_name": INVENTORY_FUNCTION, "log_group": "/sfn-etl/inventory"},
    )

    bundle = SfnInspector(make_runtime()).logs(INVENTORY_REF, detail)

    assert bundle.source == "cloudwatch:/sfn-etl/inventory"
    assert extract_traceback(bundle.text)[0] == "KeyError"


def test_logs_of_a_missing_attempt_are_an_empty_bundle(stubbed: Any, make_runtime: Any) -> None:
    missing = AttemptRef(INSTANCE, "sales_daily_etl", "conformance-missing-run", "TransformSales", -1, 42)
    arn = arns.execution_arn(SALES_SM, "conformance-missing-run")
    expect_sfn_error(stubbed, "describe_execution", "ExecutionDoesNotExist", {"executionArn": arn})

    bundle = SfnInspector(make_runtime()).logs(missing, None)

    assert bundle == LogBundle(
        text="",
        source="none",
        confidence="none",
        notes=(
            "Step Functions knows no run conformance-missing-run of sales_daily_etl (attempt 42): "
            "there is no log to correlate.",
        ),
    )


def test_logs_of_an_unknown_pipeline_need_no_io(make_runtime: Any) -> None:
    bundle = SfnInspector(make_runtime(factory=no_client)).logs(AttemptRef(INSTANCE, "nope", "r", "S"), None)
    assert (bundle.text, bundle.confidence) == ("", "none") and bundle.notes


def test_logs_turn_a_refused_describe_into_a_note(stubbed: Any, make_runtime: Any) -> None:
    expect_sfn_error(
        stubbed, "describe_execution", "AccessDeniedException", {"executionArn": SALES_EXECUTION}
    )

    bundle = SfnInspector(make_runtime()).logs(SALES_REF, None)

    assert bundle.notes == (
        "Describing run sales-2026-10-02 of sales_daily_etl (attempt 1) was refused: "
        "there is no log to correlate.",
    )


def test_logs_raise_transient_errors(stubbed: Any, make_runtime: Any) -> None:
    expect_sfn_error(stubbed, "describe_execution", "ThrottlingException", {"executionArn": SALES_EXECUTION})

    with pytest.raises(OrchestratorError) as raised:
        SfnInspector(make_runtime()).logs(SALES_REF, None)
    assert raised.value.transient


def test_logs_disabled_do_no_io(make_runtime: Any) -> None:
    inspector = SfnInspector(make_runtime(sfn_options(log_lookup=False), factory=no_client))

    for detail in (None, _detail()):
        assert inspector.logs(SALES_REF, detail) == LogBundle(
            "", "none", "none", notes=(LOOKUP_DISABLED_NOTE,)
        )


def _detail(**changes: Any) -> AttemptDetail:
    values: dict[str, Any] = {
        "ref": SALES_REF,
        "state": AttemptState.FAILED,
        "is_final": True,
        "error_name": "KeyError",
        "error_message": "'amount'",
        "executor": "lambda",
        "resource": "invoke",
        "started_at": at("2026-10-02T06:00:20.100Z"),
        "ended_at": at("2026-10-02T06:00:41.400Z"),
        "native": {},
    }
    values.update(changes)
    return AttemptDetail(**values)


@pytest.mark.parametrize(
    ("ref", "detail", "note"),
    [
        (
            AttemptRef(INSTANCE, "sales_daily_etl", "sales-2026-10-02", RUN_LEVEL_STEP),
            _detail(),
            "The execution failed outside any state: there is no Lambda log to correlate.",
        ),
        (
            SALES_REF,
            _detail(executor="ecs"),
            "State TransformSales ran a ecs task: only Lambda logs are correlated.",
        ),
        (
            SALES_REF,
            _detail(resource="invoke"),
            "The Lambda function of state TransformSales is unknown: there is no log to correlate.",
        ),
        (
            SALES_REF,
            _detail(executor=None, resource="arn:aws:states:eu-west-1:123456789012:activity:approve"),
            "The Lambda function of state TransformSales is unknown: there is no log to correlate.",
        ),
    ],
)
def test_logs_that_cannot_be_correlated(
    make_runtime: Any, ref: AttemptRef, detail: AttemptDetail, note: str
) -> None:
    bundle = SfnInspector(make_runtime(factory=no_client)).logs(ref, detail)

    assert bundle == LogBundle(text="", source="none", confidence="none", notes=(note,))


# =========================================================================== pipeline source


def test_the_definition_that_ran(stubbed: Any, make_runtime: Any) -> None:
    expect_definition(stubbed, CUSTOMERS_EXECUTION)
    expect_definition(stubbed, CUSTOMERS_EXECUTION)
    inspector = SfnInspector(make_runtime())

    source = inspector.pipeline_source("customers_sync", ref=CUSTOMERS_REF)
    with_text = inspector.pipeline_source("customers_sync", ref=CUSTOMERS_REF, include_text=True)

    assert source == PipelineSource(
        path=CUSTOMERS_SM, relative_path=None, text=None, language="asl-json", origin=ORIGIN_RAN
    )
    assert with_text is not None and json.loads(with_text.text or "")["StartAt"] == "ExtractCustomers"


def test_the_definition_that_ran_from_native_alone(stubbed: Any, make_runtime: Any) -> None:
    expect_definition(stubbed, CUSTOMERS_EXECUTION)

    source = SfnInspector(make_runtime()).pipeline_source(
        "customers_sync", native={"execution_arn": CUSTOMERS_EXECUTION}
    )

    assert source is not None and source.origin == ORIGIN_RAN


def test_the_current_definition(stubbed: Any, make_runtime: Any) -> None:
    _client, stubber = stubbed("stepfunctions")
    stubber.add_response(
        "describe_state_machine",
        fixture("describe_state_machine_customers"),
        {"stateMachineArn": CUSTOMERS_SM},
    )

    source = SfnInspector(make_runtime()).pipeline_source("customers_sync", include_text=True)

    assert source is not None and (source.path, source.origin, source.language) == (
        CUSTOMERS_SM,
        ORIGIN_CURRENT,
        "asl-json",
    )
    assert source.text == fixture("describe_state_machine_customers")["definition"]


def test_a_gone_execution_falls_back_to_the_current_definition(stubbed: Any, make_runtime: Any) -> None:
    expect_sfn_error(
        stubbed,
        "describe_state_machine_for_execution",
        "ExecutionDoesNotExist",
        {"executionArn": CUSTOMERS_EXECUTION},
    )
    _client, stubber = stubbed("stepfunctions")
    stubber.add_response(
        "describe_state_machine",
        fixture("describe_state_machine_customers"),
        {"stateMachineArn": CUSTOMERS_SM},
    )

    source = SfnInspector(make_runtime()).pipeline_source("customers_sync", ref=CUSTOMERS_REF)

    assert source is not None and source.origin == ORIGIN_CURRENT


def test_a_gone_state_machine_is_not_found(stubbed: Any, make_runtime: Any) -> None:
    expect_sfn_error(
        stubbed, "describe_state_machine", "StateMachineDoesNotExist", {"stateMachineArn": CUSTOMERS_SM}
    )

    with pytest.raises(NotFound):
        SfnInspector(make_runtime()).pipeline_source("customers_sync")


def test_an_unknown_pipeline_has_no_source(make_runtime: Any) -> None:
    assert SfnInspector(make_runtime(factory=no_client)).pipeline_source("not_watched") is None


# =========================================================================== the three example cases


@pytest.mark.parametrize(
    ("ref", "execution", "describe", "history", "exception", "repo_frame"),
    [
        (
            SALES_REF,
            SALES_EXECUTION,
            "describe_execution_sales",
            "history_sales",
            "KeyError",
            "/opt/python/etl/sales.py",
        ),
        (
            CUSTOMERS_REF,
            CUSTOMERS_EXECUTION,
            "describe_execution_customers",
            "history_customers",
            "ValueError",
            "/opt/python/etl/customers.py",
        ),
        (
            INVENTORY_REF,
            INVENTORY_EXECUTION,
            "describe_execution_inventory",
            "history_inventory",
            "KeyError",
            "/opt/python/etl/inventory.py",
        ),
    ],
    ids=["data", "code", "ambiguous"],
)
def test_the_evidence_of_the_example_cases(
    stubbed: Any,
    make_runtime: Any,
    ref: AttemptRef,
    execution: str,
    describe: str,
    history: str,
    exception: str,
    repo_frame: str,
) -> None:
    expect_describe(stubbed, execution, describe)
    expect_history(stubbed, execution, history)

    detail = SfnInspector(make_runtime()).describe(ref, {})

    exception_type, _message, frames, _text = extract_traceback(detail.traceback_text)
    assert exception_type == exception
    files = [frame["file"] for frame in frames]
    assert files[0] == "/var/task/app.py" and repo_frame in files
    assert detail.native["function_name"] == detail.resource and detail.executor == "lambda"
    assert_native_fits(detail.native)


# =========================================================================== recorded failure events


def _event(event_id: int, previous: int, kind: str, second: int, **details: Any) -> dict[str, Any]:
    return {
        "timestamp": at(f"2026-10-02T06:00:{second:02d}.000Z"),
        "type": kind,
        "id": event_id,
        "previousEventId": previous,
        **details,
    }


def _task(resource_type: str = "lambda", **extra: Any) -> dict[str, Any]:
    return {"resourceType": resource_type, "resource": "invoke", **extra}


def _caught_then_failed_history() -> dict[str, Any]:
    """TransformSales fails, a Catch sends it to the Fail state SalesFailed, which ends the run."""
    cause = fixture("describe_execution_sales")["cause"]
    events = [
        _event(
            1, 0, "ExecutionStarted", 0, executionStartedEventDetails={"roleArn": "arn:aws:iam::1:role/r"}
        ),
        _event(2, 1, "TaskStateEntered", 1, stateEnteredEventDetails={"name": "TransformSales"}),
        _event(
            3,
            2,
            "TaskScheduled",
            1,
            taskScheduledEventDetails=_task(
                region="eu-west-1", parameters=json.dumps({"FunctionName": "fn"})
            ),
        ),
        _event(4, 3, "TaskStarted", 2, taskStartedEventDetails=_task()),
        _event(5, 4, "TaskFailed", 9, taskFailedEventDetails=_task(error="KeyError", cause=cause)),
        _event(6, 5, "TaskStateExited", 9, stateExitedEventDetails={"name": "TransformSales"}),
        _event(7, 6, "FailStateEntered", 10, stateEnteredEventDetails={"name": "SalesFailed"}),
        _event(
            8,
            7,
            "ExecutionFailed",
            11,
            executionFailedEventDetails={"error": "Sales.Failed", "cause": "The sales transform failed."},
        ),
    ]
    return {"events": list(reversed(events))}


def test_the_recorded_task_failure_before_a_fail_state(stubbed: Any, make_runtime: Any) -> None:
    expect_describe(stubbed, SALES_EXECUTION, "describe_execution_sales")
    expect_history(stubbed, SALES_EXECUTION, _caught_then_failed_history())
    inspector = SfnInspector(make_runtime())

    detail = inspector.describe(SALES_REF, {"failed_event_id": 5, "function_name": SALES_FUNCTION})

    assert extract_traceback(detail.traceback_text)[:2] == ("KeyError", "'amount'")
    assert (detail.started_at, detail.ended_at) == (
        at("2026-10-02T06:00:02.000Z"),
        at("2026-10-02T06:00:09.000Z"),
    )
    assert (detail.native["entered_event_id"], detail.native["failed_event_id"]) == (2, 5)


def test_without_a_recorded_event_the_fail_state_is_described(stubbed: Any, make_runtime: Any) -> None:
    expect_describe(stubbed, SALES_EXECUTION, "describe_execution_sales")
    expect_history(stubbed, SALES_EXECUTION, _caught_then_failed_history())
    ref = AttemptRef(INSTANCE, "sales_daily_etl", "sales-2026-10-02", "SalesFailed")

    detail = SfnInspector(make_runtime()).describe(ref, {})

    assert (detail.error_name, detail.error_message) == ("Sales.Failed", "The sales transform failed.")
    assert (detail.traceback_text, detail.executor, detail.native["event_type"]) == (
        "",
        None,
        "FailStateEntered",
    )


def test_recorded_fail_state_and_evaluation_events() -> None:
    localized = history.Localized(step_id="Other", terminal_error="States.Runtime", terminal_cause="bad path")
    segment = [
        _event(1, 0, "ExecutionStarted", 0),
        _event(2, 1, "PassStateEntered", 1, stateEnteredEventDetails={"name": "Shape"}),
        _event(
            3,
            2,
            "EvaluationFailed",
            2,
            evaluationFailedEventDetails={
                "state": "Shape",
                "error": "States.Runtime",
                "location": "Parameters",
            },
        ),
        _event(4, 3, "FailStateEntered", 3, stateEnteredEventDetails={"name": "Stop"}),
        _event(5, 5, "TaskFailed", 4, taskFailedEventDetails=_task(error="E")),  # links to itself
        _event(6, 9, "TaskFailed", 5, taskFailedEventDetails=_task(error="E2")),  # links forward
        _event(7, 0, "TaskStarted", 6, taskStartedEventDetails=_task()),
    ]

    evaluation = _recorded_evidence(segment, 3, localized)
    fail_state = _recorded_evidence(segment, 4, localized)
    self_link = _recorded_evidence(segment, 5, localized)
    forward_link = _recorded_evidence(segment, 6, localized)

    assert evaluation is not None and (evaluation.step_id, evaluation.error, evaluation.cause) == (
        "Shape",
        "States.Runtime",
        "bad path",
    )
    assert evaluation.entered_event_id == 2 and evaluation.started_at is None
    assert fail_state is not None and (fail_state.step_id, fail_state.entered_event_id) == ("Stop", 4)
    assert (fail_state.error, fail_state.cause) == ("States.Runtime", "bad path")
    assert self_link is not None and (self_link.error, self_link.entered_event_id, self_link.started_at) == (
        "E",
        None,
        None,
    )
    assert forward_link is not None and forward_link.entered_event_id is None
    assert _recorded_evidence(segment, 7, localized) is None  # not a step failure
    assert _recorded_evidence(segment, 42, localized) is None  # not in the segment
