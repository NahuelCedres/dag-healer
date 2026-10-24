"""Redrive and restart (``stepfunctions/remediation.py``, section 9.9 of the design).

``available`` is table-tested without AWS; ``remediate`` runs against botocore ``Stubber``
clients with exact request parameters (``RedriveExecution``, ``DescribeExecution``,
``StartExecution``) and the answers of ``tests/unit/fixtures/stepfunctions/api/remediation``.
The customers execution's input carries an e-mail address that must never reach a result,
``native`` or a log record.
"""

from __future__ import annotations

import logging
import re
from dataclasses import replace
from datetime import datetime, timedelta, timezone
from typing import Any

import pytest

from dag_healer.core.errors import NotFound, NotSupported, OrchestratorError, PermissionDenied
from dag_healer.core.ids import RUN_LEVEL_STEP, UNRESOLVED_STEP, AttemptRef
from dag_healer.core.ports import RemediationResult, Remediator
from dag_healer.core.vocab import RemediationKind
from dag_healer.stepfunctions import remediation
from dag_healer.stepfunctions.remediation import (
    CI_DEPLOY_WARNING,
    CLIENT_TOKEN_MAX,
    REDRIVE_SCOPE_WARNING,
    RERUN_WARNING,
    SfnRemediator,
    client_token,
    rerun_name,
)
from dag_healer.stepfunctions.runtime import native_ref

from .conftest import ACCOUNT, CUSTOMERS_SM, INSTANCE, NOW, REGION, api_response, load_json, sfn_options

RESUME = RemediationKind.RESUME_RUN
RERUN = RemediationKind.RERUN_PIPELINE
RETRY = RemediationKind.RETRY_STEP

EXECUTION_NAME = "customers-2026-10-02"
EXECUTION = f"arn:aws:states:{REGION}:{ACCOUNT}:execution:customers-sync:{EXECUTION_NAME}"
REF = AttemptRef(INSTANCE, "customers_sync", EXECUTION_NAME, "TransformCustomers", -1, 1)
STOPPED = datetime(2026, 10, 2, 6, 0, 41, 512000, tzinfo=timezone.utc)
NATIVE = native_ref(
    execution_arn=EXECUTION,
    state_machine_arn=CUSTOMERS_SM,
    status="FAILED",
    stop_date=STOPPED,
    redrive_count=0,
    failed_event_id=17,
)
KEY = "healer-cmd-0001-customers"
PII = "alice.pii@example.com"
ERRORS = load_json("api/remediation/errors.json")
FAILED_DESCRIPTION = api_response("api/remediation/describe_execution_failed.json")
METADATA_ONLY = api_response("api/remediation/describe_execution_metadata_only.json")
STARTED = load_json("api/remediation/start_execution.json")
RERUN_NAME = "customers-2026-10-02-healer-588bd3c9f929"


def _remediator(make_runtime: Any, *, now: datetime = NOW, **overrides: Any) -> SfnRemediator:
    return SfnRemediator(make_runtime(sfn_options(**overrides), now=now))


def _offline(make_runtime: Any, **overrides: Any) -> tuple[SfnRemediator, list[str]]:
    """A remediator whose clients record every attempt to build one (none may be built)."""
    built: list[str] = []

    def factory(service: str) -> Any:
        built.append(service)
        raise AssertionError(f"available() built a {service} client")

    return SfnRemediator(make_runtime(sfn_options(**overrides), factory=factory)), built


def _client_error(stubber: Any, method: str, group: str, code: str, expected: dict[str, Any]) -> None:
    answer = ERRORS[group][code]
    stubber.add_client_error(
        method,
        service_error_code=code,
        service_message=answer["message"],
        http_status_code=answer["status"],
        expected_params=expected,
    )


def _redrive_params(key: str = KEY, arn: str = EXECUTION) -> dict[str, Any]:
    return {"executionArn": arn, "clientToken": key[:CLIENT_TOKEN_MAX]}


def _expect_described(stubber: Any, described: dict[str, Any] | None = None, arn: str = EXECUTION) -> None:
    """The ``DescribeExecution`` a redrive starts with (the failed attempt 1 by default)."""
    stubber.add_response("describe_execution", described or FAILED_DESCRIPTION, {"executionArn": arn})


def _expect_redrive(stubber: Any, key: str = KEY, arn: str = EXECUTION) -> None:
    """``DescribeExecution`` (attempt 1, FAILED), then an accepted ``RedriveExecution``."""
    _expect_described(stubber, arn=arn)
    stubber.add_response(
        "redrive_execution", api_response("api/remediation/redrive_execution.json"), _redrive_params(key, arn)
    )


def _start_params(name: str = RERUN_NAME) -> dict[str, Any]:
    return {"stateMachineArn": CUSTOMERS_SM, "name": name, "input": FAILED_DESCRIPTION["input"]}


def _texts(result: RemediationResult) -> str:
    return " ".join([result.detail, *result.warnings, repr(dict(result.native))])


# =========================================================================== available


def _days_ago(days: float, *, ms: int = 0) -> str:
    return (NOW - timedelta(days=days, milliseconds=ms)).isoformat()


AVAILABLE_CASES: list[tuple[str, dict[str, Any], dict[str, Any] | None, AttemptRef, tuple[Any, ...]]] = [
    ("defaults: redrive only", {}, NATIVE, REF, (RESUME,)),
    ("rerun on: both, redrive first", {"rerun": True}, NATIVE, REF, (RESUME, RERUN)),
    ("redrive disabled, rerun on", {"retry_strategy": "disabled", "rerun": True}, NATIVE, REF, (RERUN,)),
    ("both disabled", {"retry_strategy": "disabled"}, NATIVE, REF, ()),
    ("TIMED_OUT is redrivable", {}, {**NATIVE, "status": "TIMED_OUT"}, REF, (RESUME,)),
    ("ABORTED is redrivable", {}, {**NATIVE, "status": "ABORTED"}, REF, (RESUME,)),
    ("a lower-case status", {}, {**NATIVE, "status": "failed"}, REF, (RESUME,)),
    ("SUCCEEDED is not", {"rerun": True}, {**NATIVE, "status": "SUCCEEDED"}, REF, (RERUN,)),
    ("RUNNING is not", {}, {**NATIVE, "status": "RUNNING"}, REF, ()),
    ("PENDING_REDRIVE is not", {}, {**NATIVE, "status": "PENDING_REDRIVE"}, REF, ()),
    ("13 days ago", {}, {**NATIVE, "stop_date": _days_ago(13)}, REF, (RESUME,)),
    ("exactly 14 days ago", {}, {**NATIVE, "stop_date": _days_ago(14)}, REF, (RESUME,)),
    ("14 days and 1 ms ago", {}, {**NATIVE, "stop_date": _days_ago(14, ms=1)}, REF, ()),
    ("20 days ago, rerun on", {"rerun": True}, {**NATIVE, "stop_date": _days_ago(20)}, REF, (RERUN,)),
    ("a stop date ahead of the clock", {}, {**NATIVE, "stop_date": _days_ago(-1)}, REF, (RESUME,)),
    ("a stop date in epoch ms", {}, {**NATIVE, "stop_date": 1790920841512}, REF, (RESUME,)),
    ("a stop date as a datetime", {}, {**NATIVE, "stop_date": STOPPED}, REF, (RESUME,)),
    ("no stop date: AWS decides", {}, {k: v for k, v in NATIVE.items() if k != "stop_date"}, REF, (RESUME,)),
    ("an unreadable stop date", {}, {**NATIVE, "stop_date": "yesterday"}, REF, (RESUME,)),
    ("no status: AWS decides", {}, {k: v for k, v in NATIVE.items() if k != "status"}, REF, (RESUME,)),
    ("an empty native", {"rerun": True}, {}, REF, (RESUME, RERUN)),
    ("native is not a mapping", {}, None, REF, (RESUME,)),
    (
        "an Express execution",
        {"rerun": True},
        {**NATIVE, "execution_arn": f"arn:aws:states:{REGION}:{ACCOUNT}:express:customers-sync:x:1a2b"},
        REF,
        (),
    ),
    ("not an execution ARN", {"rerun": True}, {**NATIVE, "execution_arn": CUSTOMERS_SM}, REF, ()),
    ("garbage instead of an ARN", {"rerun": True}, {**NATIVE, "execution_arn": "not-an-arn"}, REF, ()),
    (
        "a Distributed Map child: redrive only",
        {"rerun": True},
        {**NATIVE, "execution_arn": f"arn:aws:states:{REGION}:{ACCOUNT}:execution:customers-sync/Items:abc"},
        REF,
        (RESUME,),
    ),
    ("an unresolved step", {"rerun": True}, NATIVE, replace(REF, step_id=UNRESOLVED_STEP), ()),
    ("a run-level failure", {"rerun": True}, NATIVE, replace(REF, step_id=RUN_LEVEL_STEP), (RESUME, RERUN)),
]


@pytest.mark.parametrize(
    ("overrides", "native", "ref", "expected"),
    [case[1:] for case in AVAILABLE_CASES],
    ids=[case[0] for case in AVAILABLE_CASES],
)
def test_available(
    make_runtime: Any,
    overrides: dict[str, Any],
    native: dict[str, Any] | None,
    ref: AttemptRef,
    expected: tuple[Any, ...],
) -> None:
    remediator, built = _offline(make_runtime, **overrides)
    assert remediator.available(ref, native) == expected  # type: ignore[arg-type]
    assert built == [], "available() must not do I/O"


def test_the_port_satisfies_the_protocol(make_runtime: Any) -> None:
    assert isinstance(_remediator(make_runtime), Remediator)


# =========================================================================== RESUME_RUN


def test_redrive_sends_the_idempotency_key_as_client_token(make_runtime: Any, stubbed: Any) -> None:
    _client, stubber = stubbed("stepfunctions")
    _expect_redrive(stubber)
    remediator = _remediator(make_runtime)
    result = remediator.remediate(REF, NATIVE, RESUME, idempotency_key=KEY, actor="ops-oncall")
    assert result.kind is RESUME and result.accepted is True
    assert result.new_ref == REF.next_attempt() and result.new_ref.attempt == 2
    assert result.warnings == (CI_DEPLOY_WARNING, REDRIVE_SCOPE_WARNING)
    assert dict(result.native) == {"execution_arn": EXECUTION, "redrive_count": 1}
    assert "ops-oncall" in result.detail and EXECUTION_NAME in result.detail
    assert "attempt 2" in result.detail


def test_the_same_key_returns_the_first_result_without_calling_aws(make_runtime: Any, stubbed: Any) -> None:
    _client, stubber = stubbed("stepfunctions")
    _expect_redrive(stubber)
    remediator = _remediator(make_runtime)
    first = remediator.remediate(REF, NATIVE, RESUME, idempotency_key=KEY, actor="ops-oncall")
    again = remediator.remediate(REF, NATIVE, RESUME, idempotency_key=KEY, actor="someone-else")
    assert again is first  # a second RedriveExecution would find no queued answer
    stubber.assert_no_pending_responses()


@pytest.mark.parametrize(
    ("key", "token"),
    [
        ("k" * 100, "k" * 64),
        ("healer-" + "0123456789abcdef" * 4, ("healer-" + "0123456789abcdef" * 4)[:64]),
        ("with space\tand tab", "with-space-and-tab"),
        ("café-résumé", "caf--r-sum-"),
        ("!~", "!~"),
    ],
)
def test_client_token(key: str, token: str) -> None:
    assert client_token(key) == token
    assert len(client_token(key)) <= CLIENT_TOKEN_MAX
    assert re.fullmatch(r"[!-~]+", client_token(key))


def test_a_long_key_is_cut_to_64_characters(make_runtime: Any, stubbed: Any) -> None:
    key = "healer-" + "a" * 80
    _client, stubber = stubbed("stepfunctions")
    _expect_redrive(stubber, key)
    result = _remediator(make_runtime).remediate(REF, NATIVE, RESUME, idempotency_key=key, actor="ops")
    assert result.accepted is True


@pytest.mark.parametrize("code", ["ExecutionNotRedrivable", "ValidationException", "ExecutionLimitExceeded"])
def test_a_refused_redrive_is_not_accepted(
    make_runtime: Any, stubbed: Any, code: str, caplog: pytest.LogCaptureFixture
) -> None:
    caplog.set_level(logging.DEBUG)
    _client, stubber = stubbed("stepfunctions")
    _expect_described(stubber)
    _client_error(stubber, "redrive_execution", "redrive", code, _redrive_params())
    remediator = _remediator(make_runtime)
    result = remediator.remediate(REF, NATIVE, RESUME, idempotency_key=KEY, actor="ops-oncall")
    assert result.kind is RESUME and result.accepted is False and result.new_ref is None
    assert code in result.detail and ERRORS["redrive"][code]["message"] in result.detail
    assert result.warnings == (CI_DEPLOY_WARNING, REDRIVE_SCOPE_WARNING)
    assert dict(result.native) == {"execution_arn": EXECUTION}
    assert remediator.remediate(REF, NATIVE, RESUME, idempotency_key=KEY, actor="x") is result
    logged = " ".join(record.getMessage() for record in caplog.records)
    assert ERRORS["redrive"][code]["message"] not in logged, "AWS messages are never logged"


@pytest.mark.parametrize(
    ("code", "error_type", "transient"),
    [
        ("ExecutionDoesNotExist", NotFound, False),
        ("AccessDeniedException", PermissionDenied, False),
        ("ThrottlingException", OrchestratorError, True),
        ("InternalFailure", OrchestratorError, True),
    ],
)
def test_other_redrive_errors_raise_core_errors_and_are_not_remembered(
    make_runtime: Any, stubbed: Any, code: str, error_type: type[OrchestratorError], transient: bool
) -> None:
    _client, stubber = stubbed("stepfunctions")
    _expect_described(stubber)
    _client_error(stubber, "redrive_execution", "redrive", code, _redrive_params())
    _expect_redrive(stubber)
    remediator = _remediator(make_runtime)
    with pytest.raises(error_type) as raised:
        remediator.remediate(REF, NATIVE, RESUME, idempotency_key=KEY, actor="ops-oncall")
    assert raised.value.transient is transient
    assert code in raised.value.detail
    retried = remediator.remediate(REF, NATIVE, RESUME, idempotency_key=KEY, actor="ops-oncall")
    assert retried.accepted is True, "a failed call is tried again for the same key"


STALE_CASES: list[tuple[str, AttemptRef, dict[str, Any], str]] = [
    (
        "attempt 2 failed since: redrive it from its own incident",
        REF,
        {**FAILED_DESCRIPTION, "redriveCount": 1},
        "is at attempt 2 now, not attempt 1: redrive it from the incident of attempt 2",
    ),
    (
        "a redrive is running already",
        REF,
        {**FAILED_DESCRIPTION, "redriveCount": 1, "status": "RUNNING"},
        "was redriven already: attempt 2 is RUNNING",
    ),
    (
        "an attempt the execution never reached",
        replace(REF, attempt=3),
        FAILED_DESCRIPTION,
        "has 1 attempt(s), not attempt 3",
    ),
    (
        "an earlier execution of a reused name",
        replace(REF, run_id=f"{EXECUTION_NAME}@1782920800000"),
        FAILED_DESCRIPTION,  # started at ...800000 of 2026-10-02: a later execution of the name
        "was started again under the same name",
    ),
]


@pytest.mark.parametrize(
    ("ref", "described", "detail"),
    [case[1:] for case in STALE_CASES],
    ids=[case[0] for case in STALE_CASES],
)
def test_a_redrive_of_an_attempt_that_is_not_current_is_not_sent(
    make_runtime: Any,
    stubbed: Any,
    caplog: pytest.LogCaptureFixture,
    ref: AttemptRef,
    described: dict[str, Any],
    detail: str,
) -> None:
    """``native`` is the execution at detection time: a redrive acts on its current attempt, so an
    older incident must not redrive (and miscount) a newer attempt. No RedriveExecution is queued:
    the Stubber fails the test if one is sent."""
    caplog.set_level(logging.DEBUG)
    _client, stubber = stubbed("stepfunctions")
    _expect_described(stubber, described)
    remediator = _remediator(make_runtime)
    result = remediator.remediate(ref, NATIVE, RESUME, idempotency_key=KEY, actor="ops-oncall")
    assert (result.kind, result.accepted, result.new_ref) == (RESUME, False, None)
    assert detail in result.detail and EXECUTION_NAME in result.detail
    assert result.warnings == (CI_DEPLOY_WARNING, REDRIVE_SCOPE_WARNING)
    assert dict(result.native) == {"execution_arn": EXECUTION}
    assert remediator.remediate(ref, NATIVE, RESUME, idempotency_key=KEY, actor="x") is result
    logged = " ".join(record.getMessage() for record in caplog.records)
    assert "was not sent" in logged and PII not in logged


def test_a_redrive_without_a_redrive_count_lets_aws_decide(make_runtime: Any, stubbed: Any) -> None:
    _client, stubber = stubbed("stepfunctions")
    _expect_described(
        stubber, {key: value for key, value in FAILED_DESCRIPTION.items() if key != "redriveCount"}
    )
    stubber.add_response(
        "redrive_execution", api_response("api/remediation/redrive_execution.json"), _redrive_params()
    )
    result = _remediator(make_runtime).remediate(REF, NATIVE, RESUME, idempotency_key=KEY, actor="ops")
    assert result.accepted is True and result.new_ref == REF.next_attempt()


def test_a_redrive_of_a_missing_execution_is_not_found(make_runtime: Any, stubbed: Any) -> None:
    _client, stubber = stubbed("stepfunctions")
    stubber.add_client_error(
        "describe_execution",
        service_error_code="ExecutionDoesNotExist",
        http_status_code=400,
        expected_params={"executionArn": EXECUTION},
    )
    with pytest.raises(NotFound):
        _remediator(make_runtime).remediate(REF, NATIVE, RESUME, idempotency_key=KEY, actor="ops")


@pytest.mark.parametrize(
    ("run_id", "native"),
    [
        (EXECUTION_NAME, {}),
        (f"{EXECUTION_NAME}@1790920800000", {}),
        (EXECUTION_NAME, {"state_machine_arn": CUSTOMERS_SM, "status": "FAILED"}),
    ],
    ids=["from the pipeline", "a reused name drops its suffix", "from native.state_machine_arn"],
)
def test_the_execution_arn_is_built_when_native_does_not_name_it(
    make_runtime: Any, stubbed: Any, run_id: str, native: dict[str, Any]
) -> None:
    _client, stubber = stubbed("stepfunctions")
    _expect_redrive(stubber)
    ref = replace(REF, run_id=run_id)
    result = _remediator(make_runtime).remediate(ref, native, RESUME, idempotency_key=KEY, actor="ops")
    assert result.accepted is True and result.new_ref == ref.next_attempt()


def test_a_pipeline_configured_by_name_is_resolved_first(make_runtime: Any, stubbed: Any) -> None:
    options = sfn_options(pipelines={"customers_sync": {"state_machine": "customers-sync"}})
    _client, stubber = stubbed("stepfunctions")
    stubber.add_response(
        "list_state_machines",
        {
            "stateMachines": [
                {
                    "stateMachineArn": CUSTOMERS_SM,
                    "name": "customers-sync",
                    "type": "STANDARD",
                    "creationDate": NOW - timedelta(days=30),
                }
            ]
        },
        {"maxResults": 1000},
    )
    _expect_redrive(stubber)
    remediator = SfnRemediator(make_runtime(options))
    assert remediator.remediate(REF, {}, RESUME, idempotency_key=KEY, actor="ops").accepted is True


def test_an_unknown_execution_is_not_found_without_calling_aws(make_runtime: Any) -> None:
    remediator, built = _offline(make_runtime)
    ref = replace(REF, pipeline_id="not_configured")
    with pytest.raises(NotFound):
        remediator.remediate(ref, {}, RESUME, idempotency_key=KEY, actor="ops")
    assert built == []


# =========================================================================== NotSupported


@pytest.mark.parametrize(
    ("overrides", "native", "kind"),
    [
        ({}, NATIVE, RETRY),
        ({"rerun": True}, NATIVE, RETRY),
        ({}, NATIVE, RERUN),
        ({"retry_strategy": "disabled", "rerun": True}, NATIVE, RESUME),
        ({}, {**NATIVE, "stop_date": _days_ago(15)}, RESUME),
        ({}, {**NATIVE, "status": "SUCCEEDED"}, RESUME),
    ],
    ids=[
        "retry step",
        "retry step with rerun",
        "rerun off",
        "redrive disabled",
        "window closed",
        "succeeded",
    ],
)
def test_a_kind_that_is_not_available_is_not_supported(
    make_runtime: Any, overrides: dict[str, Any], native: dict[str, Any], kind: RemediationKind
) -> None:
    remediator, built = _offline(make_runtime, **overrides)
    with pytest.raises(NotSupported):
        remediator.remediate(REF, native, kind, idempotency_key=KEY, actor="ops")
    assert built == []


def test_a_remembered_result_survives_the_window_but_a_new_key_does_not(
    make_runtime: Any, stubbed: Any
) -> None:
    moments = [NOW]
    _client, stubber = stubbed("stepfunctions")
    _expect_redrive(stubber)
    runtime = make_runtime(sfn_options())
    runtime.clock = lambda: moments[0]
    remediator = SfnRemediator(runtime)
    first = remediator.remediate(REF, NATIVE, RESUME, idempotency_key=KEY, actor="ops")
    moments[0] = NOW + timedelta(days=30)
    assert remediator.remediate(REF, NATIVE, RESUME, idempotency_key=KEY, actor="ops") is first
    with pytest.raises(NotSupported):
        remediator.remediate(REF, NATIVE, RESUME, idempotency_key="healer-cmd-0002", actor="ops")


def test_a_key_remembered_for_another_kind_does_not_bypass_available(make_runtime: Any, stubbed: Any) -> None:
    _client, stubber = stubbed("stepfunctions")
    _expect_redrive(stubber)
    remediator = _remediator(make_runtime)
    remediator.remediate(REF, NATIVE, RESUME, idempotency_key=KEY, actor="ops")
    with pytest.raises(NotSupported):
        remediator.remediate(REF, NATIVE, RETRY, idempotency_key=KEY, actor="ops")


def test_an_empty_key_is_refused_before_any_call(make_runtime: Any) -> None:
    remediator, built = _offline(make_runtime)
    with pytest.raises(ValueError, match="idempotency_key"):
        remediator.remediate(REF, NATIVE, RESUME, idempotency_key="  ", actor="ops")
    assert built == []


def test_only_the_latest_keys_are_remembered(
    make_runtime: Any, stubbed: Any, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setattr(remediation, "MAX_REMEMBERED_REMEDIATIONS", 2)
    _client, stubber = stubbed("stepfunctions")
    for key in ("key-a", "key-b", "key-c", "key-a"):
        _expect_redrive(stubber, key)
    remediator = _remediator(make_runtime)
    for key in ("key-a", "key-b", "key-c"):
        remediator.remediate(REF, NATIVE, RESUME, idempotency_key=key, actor="ops")
    remediator.remediate(REF, NATIVE, RESUME, idempotency_key="key-c", actor="ops")  # remembered
    remediator.remediate(REF, NATIVE, RESUME, idempotency_key="key-a", actor="ops")  # forgotten: AWS again
    stubber.assert_no_pending_responses()


# =========================================================================== RERUN_PIPELINE


def test_rerun_name() -> None:
    assert rerun_name(EXECUTION_NAME, KEY) == RERUN_NAME
    assert STARTED["idempotency_key"] == KEY and STARTED["response"]["executionArn"].endswith(
        f":{RERUN_NAME}"
    )
    long_name = "n" * 80
    name = rerun_name(long_name, KEY)
    assert name.startswith("n" * 52 + "-healer-") and len(name) == 72 <= 80
    assert re.fullmatch(r"n{52}-healer-[0-9a-f]{12}", name)
    assert rerun_name(EXECUTION_NAME, "another-key") != RERUN_NAME


def test_rerun_starts_a_new_execution_with_the_original_input(
    make_runtime: Any, stubbed: Any, caplog: pytest.LogCaptureFixture
) -> None:
    caplog.set_level(logging.DEBUG)
    _client, stubber = stubbed("stepfunctions")
    stubber.add_response("describe_execution", FAILED_DESCRIPTION, {"executionArn": EXECUTION})
    stubber.add_response("start_execution", api_response(STARTED["response"]), _start_params())
    remediator = _remediator(make_runtime, rerun=True)
    result = remediator.remediate(REF, NATIVE, RERUN, idempotency_key=KEY, actor="ops-oncall")
    assert result.kind is RERUN and result.accepted is True
    assert result.new_ref == replace(REF, run_id=RERUN_NAME, attempt=1)
    assert result.warnings == (RERUN_WARNING,)
    assert dict(result.native) == {
        "execution_arn": STARTED["response"]["executionArn"],
        "state_machine_arn": CUSTOMERS_SM,
        "start_date": "2026-10-02T07:00:00.500+00:00",
    }
    assert "ops-oncall" in result.detail and RERUN_NAME in result.detail and EXECUTION_NAME in result.detail
    assert PII not in _texts(result)
    logged = " ".join(record.getMessage() for record in caplog.records)
    assert PII not in logged and FAILED_DESCRIPTION["input"] not in logged
    again = remediator.remediate(REF, NATIVE, RERUN, idempotency_key=KEY, actor="someone-else")
    assert again is result


def test_rerun_after_a_restart_of_the_process_gives_the_same_result(make_runtime: Any, stubbed: Any) -> None:
    _client, stubber = stubbed("stepfunctions")
    stubber.add_response("describe_execution", FAILED_DESCRIPTION, {"executionArn": EXECUTION})
    stubber.add_response("start_execution", api_response(STARTED["response"]), _start_params())
    stubber.add_response("describe_execution", FAILED_DESCRIPTION, {"executionArn": EXECUTION})
    _client_error(stubber, "start_execution", "start", "ExecutionAlreadyExists", _start_params())
    first = _remediator(make_runtime, rerun=True).remediate(
        REF, NATIVE, RERUN, idempotency_key=KEY, actor="a"
    )
    second = _remediator(make_runtime, rerun=True).remediate(
        REF, NATIVE, RERUN, idempotency_key=KEY, actor="b"
    )
    assert (second.kind, second.accepted, second.new_ref) == (first.kind, first.accepted, first.new_ref)
    assert second.native["execution_arn"] == first.native["execution_arn"]
    assert "already started" in second.detail


@pytest.mark.parametrize("code", ["InvalidExecutionInput", "StateMachineDeleting"])
def test_a_refused_restart_is_not_accepted(make_runtime: Any, stubbed: Any, code: str) -> None:
    _client, stubber = stubbed("stepfunctions")
    stubber.add_response("describe_execution", FAILED_DESCRIPTION, {"executionArn": EXECUTION})
    _client_error(stubber, "start_execution", "start", code, _start_params())
    result = _remediator(make_runtime, rerun=True).remediate(
        REF, NATIVE, RERUN, idempotency_key=KEY, actor="ops"
    )
    assert result.accepted is False and result.new_ref is None and code in result.detail
    assert PII not in _texts(result)


def test_a_restart_on_a_missing_state_machine_is_not_found(make_runtime: Any, stubbed: Any) -> None:
    _client, stubber = stubbed("stepfunctions")
    stubber.add_response("describe_execution", FAILED_DESCRIPTION, {"executionArn": EXECUTION})
    _client_error(stubber, "start_execution", "start", "StateMachineDoesNotExist", _start_params())
    with pytest.raises(NotFound):
        _remediator(make_runtime, rerun=True).remediate(REF, NATIVE, RERUN, idempotency_key=KEY, actor="ops")


def test_a_restart_without_a_readable_input_is_not_accepted(make_runtime: Any, stubbed: Any) -> None:
    _client, stubber = stubbed("stepfunctions")
    stubber.add_client_error(
        "describe_execution",
        service_error_code="KmsAccessDeniedException",
        service_message="The caller is not authorized to use the KMS key.",
        expected_params={"executionArn": EXECUTION},
    )
    stubber.add_response(
        "describe_execution", METADATA_ONLY, {"executionArn": EXECUTION, "includedData": "METADATA_ONLY"}
    )
    result = _remediator(make_runtime, rerun=True).remediate(
        REF, NATIVE, RERUN, idempotency_key=KEY, actor="ops"
    )
    assert result.kind is RERUN and result.accepted is False and result.new_ref is None
    assert "not readable" in result.detail
    assert dict(result.native) == {"execution_arn": EXECUTION, "state_machine_arn": CUSTOMERS_SM}


def test_a_restart_runs_the_latest_revision_of_the_machine(make_runtime: Any, stubbed: Any) -> None:
    described = {**FAILED_DESCRIPTION, "stateMachineArn": f"{CUSTOMERS_SM}:7"}
    _client, stubber = stubbed("stepfunctions")
    stubber.add_response("describe_execution", described, {"executionArn": EXECUTION})
    stubber.add_response("start_execution", api_response(STARTED["response"]), _start_params())
    result = _remediator(make_runtime, rerun=True).remediate(
        REF, NATIVE, RERUN, idempotency_key=KEY, actor="ops"
    )
    assert result.accepted is True and result.native["state_machine_arn"] == CUSTOMERS_SM


def test_a_transient_describe_error_propagates(make_runtime: Any, stubbed: Any) -> None:
    _client, stubber = stubbed("stepfunctions")
    stubber.add_client_error(
        "describe_execution",
        service_error_code="ThrottlingException",
        service_message="Rate exceeded",
        expected_params={"executionArn": EXECUTION},
    )
    with pytest.raises(OrchestratorError) as raised:
        _remediator(make_runtime, rerun=True).remediate(REF, NATIVE, RERUN, idempotency_key=KEY, actor="ops")
    assert raised.value.transient is True


def test_results_fit_the_store(make_runtime: Any, stubbed: Any) -> None:
    from dag_healer.orchestrators.conformance import assert_native_fits, assert_ref_fits

    _client, stubber = stubbed("stepfunctions")
    _expect_redrive(stubber, "k2")
    stubber.add_response("describe_execution", FAILED_DESCRIPTION, {"executionArn": EXECUTION})
    stubber.add_response("start_execution", api_response(STARTED["response"]), _start_params())
    remediator = _remediator(make_runtime, rerun=True)
    keys = {RESUME: "k2", RERUN: KEY}
    for kind in remediator.available(REF, NATIVE):
        result = remediator.remediate(REF, NATIVE, kind, idempotency_key=keys[kind], actor="ops")
        assert_native_fits(result.native, "result.native")
        assert result.new_ref is not None
        assert_ref_fits(result.new_ref, INSTANCE)


def test_the_actor_and_aws_messages_are_never_logged(
    make_runtime: Any, stubbed: Any, caplog: pytest.LogCaptureFixture
) -> None:
    caplog.set_level(logging.DEBUG)
    actor = "maria.ops@example.com"
    _client, stubber = stubbed("stepfunctions")
    _expect_redrive(stubber)
    _expect_described(stubber)
    _client_error(stubber, "redrive_execution", "redrive", "ExecutionNotRedrivable", _redrive_params("key-2"))
    remediator = _remediator(make_runtime)
    accepted = remediator.remediate(REF, NATIVE, RESUME, idempotency_key=KEY, actor=actor)
    refused = remediator.remediate(REF, NATIVE, RESUME, idempotency_key="key-2", actor=actor)
    assert actor in accepted.detail, "the detail is RAW for the core, which masks it"
    assert refused.accepted is False
    logged = " ".join(record.getMessage() for record in caplog.records)
    assert EXECUTION_NAME in logged
    assert actor not in logged and ERRORS["redrive"]["ExecutionNotRedrivable"]["message"] not in logged
