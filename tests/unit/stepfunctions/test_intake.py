"""Tests of :mod:`dag_healer.stepfunctions.intake`: normalize and the confirmation verdicts.

``confirm`` is driven through ``botocore.stub.Stubber`` with the exact request parameters:
``DescribeExecution`` first, then ``GetExecutionHistory`` (newest first, without execution data).
"""

from __future__ import annotations

import copy
import json
import logging
from collections.abc import Callable
from dataclasses import replace
from typing import Any

import pytest

from dag_healer.core.failures import AttemptState, FailureHint
from dag_healer.core.ids import RUN_LEVEL_STEP, UNRESOLVED_STEP, AttemptRef
from dag_healer.orchestrators.conformance import assert_native_fits, assert_ref_fits, no_network
from dag_healer.stepfunctions import causes
from dag_healer.stepfunctions.intake import MAX_UNREADABLE_CHECKS, SfnIntake
from dag_healer.stepfunctions.runtime import epoch_ms

from .conftest import (
    ACCOUNT,
    CUSTOMERS_SM,
    INSTANCE,
    INVENTORY_SM,
    REGION,
    SALES_SM,
    api_response,
    load_json,
    sfn_options,
)

PII = "alice.pii@example.com"
CUSTOMERS_RUN = "customers-2026-10-02"


def execution_arn(machine: str, name: str) -> str:
    return f"{machine.replace(':stateMachine:', ':execution:')}:{name}"


CUSTOMERS_EXECUTION = execution_arn(CUSTOMERS_SM, CUSTOMERS_RUN)


def history_events(name: str) -> list[dict[str, Any]]:
    return load_json(f"history/{name}")["events"]


def described(**changes: Any) -> dict[str, Any]:
    """The DescribeExecution answer of the customers failure, with ``changes`` (``None`` drops a key)."""
    answer = load_json("api/detection/describe_execution_customers.json")
    answer.update(changes)
    return api_response({key: value for key, value in answer.items() if value is not None})


class Aws:
    """Queues answers on the Step Functions Stubber with their exact request parameters."""

    def __init__(self, stubber: Any) -> None:
        self.stubber = stubber

    def describe(self, answer: dict[str, Any], *, arn: str = CUSTOMERS_EXECUTION) -> None:
        self.stubber.add_response("describe_execution", answer, {"executionArn": arn})

    def describe_error(self, code: str, *, http: int = 400, arn: str = CUSTOMERS_EXECUTION) -> None:
        self.stubber.add_client_error(
            "describe_execution",
            service_error_code=code,
            http_status_code=http,
            expected_params={"executionArn": arn},
        )

    @staticmethod
    def _history_params(arn: str) -> dict[str, Any]:
        return {"executionArn": arn, "reverseOrder": True, "maxResults": 1000, "includeExecutionData": False}

    def history(self, events: list[dict[str, Any]], *, arn: str = CUSTOMERS_EXECUTION) -> None:
        answer = api_response({"events": list(reversed(events))})
        self.stubber.add_response("get_execution_history", answer, self._history_params(arn))

    def history_error(self, code: str, *, http: int = 400, arn: str = CUSTOMERS_EXECUTION) -> None:
        self.stubber.add_client_error(
            "get_execution_history",
            service_error_code=code,
            http_status_code=http,
            expected_params=self._history_params(arn),
        )


@pytest.fixture
def aws(stubbed: Callable[[str], tuple[Any, Any]]) -> Aws:
    return Aws(stubbed("stepfunctions")[1])


@pytest.fixture
def intake_of(make_runtime: Callable[..., Any]) -> Callable[..., SfnIntake]:
    def _make(**overrides: Any) -> SfnIntake:
        return SfnIntake(make_runtime(sfn_options(**overrides)))

    return _make


@pytest.fixture
def intake(intake_of: Callable[..., SfnIntake]) -> SfnIntake:
    return intake_of()


def hint_of(intake: SfnIntake, kind: str = "failed", *, sqs: bool = False) -> FailureHint:
    payload = load_json(f"events/{'sqs_' if sqs else ''}{kind}.json")
    (hint,) = intake.normalize("eventbridge", payload, {})
    return hint


# =========================================================================== normalize


def test_the_channel() -> None:
    assert SfnIntake.channels == frozenset({"eventbridge"})


def test_normalize_envelopes_and_sqs_messages(intake: SfnIntake) -> None:
    direct, queued = hint_of(intake), hint_of(intake, sqs=True)
    assert direct.ref == AttemptRef(INSTANCE, "customers_sync", CUSTOMERS_RUN, UNRESOLVED_STEP, -1, 1)
    assert (direct.channel, direct.source, queued.source) == ("eventbridge", "eventbridge", "sqs")
    assert queued.ref == direct.ref


def test_normalize_is_pure(intake: SfnIntake) -> None:
    with no_network():
        assert intake.normalize("eventbridge", load_json("events/succeeded.json"), {"x-api-key": "k"}) == []
        hint_of(intake)


@pytest.mark.parametrize("channel", ["task-failed", "sqs", ""])
def test_normalize_refuses_other_channels(intake: SfnIntake, channel: str) -> None:
    with pytest.raises(ValueError, match="push channel"):
        intake.normalize(channel, load_json("events/failed.json"), {})


@pytest.mark.parametrize("payload", [{}, {"Body": "{}"}, {"source": "aws.states"}])
def test_normalize_rejects_malformed_payloads(intake: SfnIntake, payload: dict[str, Any]) -> None:
    with pytest.raises(ValueError):
        intake.normalize("eventbridge", payload, {})


def test_normalize_uses_the_configured_statuses(intake_of: Callable[..., SfnIntake]) -> None:
    aborted = load_json("events/aborted.json")
    assert intake_of().normalize("eventbridge", aborted, {}) == []
    assert len(intake_of(statuses=["FAILED", "ABORTED"]).normalize("eventbridge", aborted, {})) == 1


# =========================================================================== confirmed


def test_confirm_localises_the_hint(aws: Aws, intake: SfnIntake) -> None:
    aws.describe(described())
    aws.history(history_events("customers_valueerror.json"))
    hint = hint_of(intake)
    confirmation = intake.confirm(hint, trigger="final_failure")
    assert confirmation.verdict == "confirmed"
    assert confirmation.reason == "FAILED; localised to TransformCustomers (TaskFailed)"
    (failure,) = confirmation.failures
    assert failure.ref == hint.ref.with_step("TransformCustomers")
    assert (failure.state, failure.is_final, failure.source) == (AttemptState.FAILED, True, "eventbridge")
    cause = described()["cause"]
    assert (failure.error_name, failure.error_summary) == (
        "ValueError",
        causes.summarize("ValueError", cause),
    )
    assert failure.failed_at == described()["stopDate"]
    assert failure.native["execution_arn"] == CUSTOMERS_EXECUTION
    assert (failure.native["failed_event_id"], failure.native["entered_event_id"]) == (10, 7)
    assert failure.native["localized"] is True
    assert_ref_fits(failure.ref, INSTANCE)
    assert_native_fits(failure.native, "failure.native")
    assert PII not in json.dumps(dict(failure.native)) + confirmation.reason


def test_trigger_is_ignored_and_the_localisation_remembered(aws: Aws, intake: SfnIntake) -> None:
    aws.describe(described())
    aws.history(history_events("customers_valueerror.json"))
    aws.describe(described())  # the second check describes again, but reads no history
    hint = hint_of(intake)
    first = intake.confirm(hint, trigger="final_failure")
    second = intake.confirm(hint, trigger="every_failure")
    assert first == second


def test_an_sqs_hint_keeps_its_source(aws: Aws, intake: SfnIntake) -> None:
    aws.describe(described())
    aws.history(history_events("customers_valueerror.json"))
    (failure,) = intake.confirm(hint_of(intake, sqs=True), trigger="final_failure").failures
    assert failure.source == "sqs"


def test_an_unknown_hint_source_becomes_eventbridge(aws: Aws, intake: SfnIntake) -> None:
    aws.describe(described())
    aws.history(history_events("customers_valueerror.json"))
    hint = replace(hint_of(intake), source="listener")
    (failure,) = intake.confirm(hint, trigger="final_failure").failures
    assert failure.source == "eventbridge"


def test_a_hint_without_native_names_the_execution_of_its_pipeline(aws: Aws, intake: SfnIntake) -> None:
    aws.describe(described())
    aws.history(history_events("customers_valueerror.json"))
    hint = replace(hint_of(intake), native={})
    assert intake.confirm(hint, trigger="final_failure").verdict == "confirmed"


def test_a_pipeline_configured_by_name_is_resolved(aws: Aws, intake_of: Callable[..., SfnIntake]) -> None:
    intake = intake_of(pipelines={"customers_sync": {"state_machine": "customers-sync"}})
    aws.stubber.add_response(
        "list_state_machines",
        api_response(load_json("api/detection/list_state_machines.json")),
        {"maxResults": 1000},
    )
    aws.describe(described())
    aws.history(history_events("customers_valueerror.json"))
    hint = replace(hint_of(intake), native={})
    assert intake.confirm(hint, trigger="final_failure").verdict == "confirmed"


def test_an_inline_map_failure_is_localised_with_its_iteration(aws: Aws, intake: SfnIntake) -> None:
    events = history_events("inventory_legacy_lambda.json")
    arn = execution_arn(INVENTORY_SM, "inventory-2026-10-02")
    aws.describe(
        described(
            executionArn=arn, stateMachineArn=INVENTORY_SM, name="inventory-2026-10-02", error="KeyError"
        ),
        arn=arn,
    )
    aws.history(events, arn=arn)
    hint = FailureHint(
        ref=AttemptRef(INSTANCE, "inventory_report", "inventory-2026-10-02", UNRESOLVED_STEP, -1, 1),
        channel="eventbridge",
        source="sqs",
        native={"execution_arn": arn},
    )
    confirmation = intake.confirm(hint, trigger="final_failure")
    assert confirmation.reason == "FAILED; localised to PerWarehouse[0]/BuildReport (LambdaFunctionFailed)"
    (failure,) = confirmation.failures
    assert (failure.ref.step_id, failure.ref.map_index) == ("BuildReport", 0)


def test_a_redriven_execution_confirms_the_next_attempt(aws: Aws, intake: SfnIntake) -> None:
    events = history_events("redriven_failed_again.json")
    arn = execution_arn(SALES_SM, "sales-2026-10-02")
    aws.describe(
        described(
            executionArn=arn,
            stateMachineArn=SALES_SM,
            name="sales-2026-10-02",
            error="KeyError",
            redriveCount=1,
            redriveDate=events[11]["timestamp"],
        ),
        arn=arn,
    )
    aws.history(events, arn=arn)
    hint = FailureHint(
        ref=AttemptRef(INSTANCE, "sales_daily_etl", "sales-2026-10-02", UNRESOLVED_STEP, -1, 2),
        channel="eventbridge",
        source="eventbridge",
        native={"execution_arn": arn},
    )
    (failure,) = intake.confirm(hint, trigger="final_failure").failures
    assert failure.ref == AttemptRef(INSTANCE, "sales_daily_etl", "sales-2026-10-02", "TransformSales", -1, 2)
    assert failure.native["redrive_count"] == 1


def test_a_timed_out_execution_is_confirmed_run_level(aws: Aws, intake: SfnIntake) -> None:
    events = history_events("execution_timed_out.json")
    aws.describe(described(status="TIMED_OUT", error="States.Timeout", cause=None))
    aws.history(events)
    confirmation = intake.confirm(hint_of(intake), trigger="final_failure")
    assert confirmation.reason == "TIMED_OUT; no failing state in the history: a run-level failure"
    (failure,) = confirmation.failures
    assert (failure.ref.step_id, failure.error_name) == (RUN_LEVEL_STEP, "States.Timeout")


def test_an_execution_name_ending_in_an_at_sign_and_digits_is_its_own_run_id(
    aws: Aws, intake: SfnIntake
) -> None:
    """``@`` and digits are valid in a name: a suffix that is not the start date is no reuse marker."""
    name = f"{CUSTOMERS_RUN}@1790920800001"  # the execution started at ...800000
    arn = execution_arn(CUSTOMERS_SM, name)
    aws.describe(described(executionArn=arn, name=name), arn=arn)
    aws.history(history_events("customers_valueerror.json"), arn=arn)
    hint = hint_of(intake)
    hint = replace(hint, ref=replace(hint.ref, run_id=name), native={**hint.native, "execution_arn": arn})
    confirmation = intake.confirm(hint, trigger="final_failure")
    assert confirmation.verdict == "confirmed", confirmation.reason
    assert confirmation.failures[0].ref.run_id == name


def test_a_shared_localizer_serves_the_feed_too(aws: Aws, make_runtime: Callable[..., Any]) -> None:
    """The adapter gives the feed and the intake one localizer: what a confirmation localised, the
    next poll returns without reading the history again."""
    from dag_healer.stepfunctions.feed import HistoryLocalizer, SfnFeed

    runtime = make_runtime(sfn_options(pipelines={"customers_sync": {"state_machine": CUSTOMERS_SM}}))
    localizer = HistoryLocalizer(runtime)
    intake, feed = SfnIntake(runtime, localizer=localizer), SfnFeed(runtime, localizer=localizer)
    aws.describe(described())
    aws.history(history_events("customers_valueerror.json"))
    (confirmed,) = intake.confirm(hint_of(intake), trigger="final_failure").failures
    listed = load_json("api/detection/list_executions_customers.json")
    for status, answer in (("FAILED", listed), ("TIMED_OUT", {"executions": []})):
        params = {"stateMachineArn": CUSTOMERS_SM, "statusFilter": status, "maxResults": 100}
        aws.stubber.add_response("list_executions", api_response(answer), params)
    (polled,) = feed.poll(cursor=None, is_monitored=lambda _pipeline: True).failures  # no history read
    assert polled.ref == confirmed.ref and polled.source == "poll"


@pytest.mark.parametrize(("matches", "verdict"), [(True, "confirmed"), (False, "discarded")])
def test_a_run_id_with_a_start_suffix_must_match_the_start_date(
    aws: Aws, intake: SfnIntake, matches: bool, verdict: str
) -> None:
    start_ms = epoch_ms(described()["startDate"])
    assert start_ms is not None
    run_id = f"{CUSTOMERS_RUN}@{start_ms if matches else start_ms - 86_400_000}"
    aws.describe(described())
    if matches:
        aws.history(history_events("customers_valueerror.json"))
    hint = hint_of(intake)
    hint = replace(hint, ref=replace(hint.ref, run_id=run_id))
    confirmation = intake.confirm(hint, trigger="final_failure")
    assert confirmation.verdict == verdict
    if matches:
        assert confirmation.failures[0].ref.run_id == run_id
    else:
        assert "earlier execution" in confirmation.reason


# =========================================================================== discarded


@pytest.mark.parametrize("code", ["ExecutionDoesNotExist", "InvalidArn"])
def test_forged_or_deleted_executions_are_discarded(aws: Aws, intake: SfnIntake, code: str) -> None:
    aws.describe_error(code)
    confirmation = intake.confirm(hint_of(intake), trigger="final_failure")
    assert (confirmation.verdict, confirmation.failures) == ("discarded", ())
    assert "forged or deleted" in confirmation.reason


def test_a_hint_of_another_instance_is_discarded_without_io(intake: SfnIntake) -> None:
    hint = hint_of(intake)
    hint = replace(hint, ref=replace(hint.ref, orchestrator="sfn-other"))
    with no_network():
        confirmation = intake.confirm(hint, trigger="final_failure")
    assert confirmation.verdict == "discarded"
    assert "sfn-other" in confirmation.reason


@pytest.mark.parametrize(
    ("native", "reason"),
    [
        ({"execution_arn": "customers-2026-10-02"}, "does not name a Step Functions execution"),
        ({"execution_arn": CUSTOMERS_SM}, "does not name a Step Functions execution"),
        (
            {"execution_arn": f"arn:aws:states:{REGION}:{ACCOUNT}:express:customers-sync:run-1:7f3e"},
            "Express",
        ),
    ],
)
def test_hints_naming_no_usable_execution_are_discarded_without_io(
    intake: SfnIntake, native: dict[str, Any], reason: str
) -> None:
    hint = replace(hint_of(intake), native=native)
    confirmation = intake.confirm(hint, trigger="final_failure")
    assert confirmation.verdict == "discarded"
    assert reason in confirmation.reason


def test_a_hint_of_an_unknown_pipeline_is_discarded(intake: SfnIntake) -> None:
    hint = FailureHint(
        ref=AttemptRef(INSTANCE, "payroll", "run-1", UNRESOLVED_STEP, -1, 1),
        channel="eventbridge",
        source="sqs",
    )
    confirmation = intake.confirm(hint, trigger="final_failure")
    assert confirmation.verdict == "discarded"
    assert "payroll" in confirmation.reason


@pytest.mark.parametrize("status", ["SUCCEEDED", "ABORTED"])
def test_an_execution_that_is_not_failed_is_discarded(aws: Aws, intake: SfnIntake, status: str) -> None:
    aws.describe(described(status=status))
    confirmation = intake.confirm(hint_of(intake), trigger="final_failure")
    assert confirmation.verdict == "discarded"
    assert confirmation.reason == f"the execution is {status}, not one of FAILED, TIMED_OUT"
    assert confirmation.state is None


@pytest.mark.parametrize("status", ["RUNNING", "PENDING_REDRIVE"])
def test_an_attempt_that_has_not_ended_is_checked_again(aws: Aws, intake: SfnIntake, status: str) -> None:
    aws.describe(described(status=status))
    confirmation = intake.confirm(hint_of(intake), trigger="final_failure")
    assert confirmation.verdict == "retry_later"
    assert confirmation.state is AttemptState.RUNNING
    assert confirmation.reason == f"attempt 1 has not ended yet (the execution is {status})"
    assert confirmation.failures == ()


def test_a_newer_attempt_that_is_running_makes_the_hint_stale(aws: Aws, intake: SfnIntake) -> None:
    aws.describe(described(status="RUNNING", redriveCount=1))
    confirmation = intake.confirm(hint_of(intake), trigger="final_failure")
    assert confirmation.verdict == "discarded"
    assert confirmation.reason == "stale: attempt 1 was redriven, attempt 2 exists"


def test_aborted_executions_are_confirmed_when_watched(aws: Aws, intake_of: Callable[..., SfnIntake]) -> None:
    intake = intake_of(statuses=["FAILED", "ABORTED"])
    aws.describe(described(status="ABORTED", error="ManualStop", cause="Stopped"))
    aws.history(history_events("execution_aborted.json"))
    confirmation = intake.confirm(hint_of(intake), trigger="final_failure")
    assert confirmation.verdict == "confirmed"
    assert confirmation.failures[0].ref.step_id == RUN_LEVEL_STEP


def test_a_stale_attempt_is_discarded(aws: Aws, intake: SfnIntake) -> None:
    aws.describe(described(redriveCount=1))
    confirmation = intake.confirm(hint_of(intake), trigger="final_failure")
    assert confirmation.verdict == "discarded"
    assert confirmation.reason == "stale: attempt 1 was redriven, attempt 2 exists"


def test_an_attempt_the_execution_never_reached_is_discarded(aws: Aws, intake: SfnIntake) -> None:
    aws.describe(described())
    hint = hint_of(intake)
    hint = replace(hint, ref=replace(hint.ref, attempt=3))
    confirmation = intake.confirm(hint, trigger="final_failure")
    assert confirmation.verdict == "discarded"
    assert confirmation.reason == "the execution has no attempt 3 (redrive count 0)"


def test_an_execution_of_another_machine_is_discarded(aws: Aws, intake: SfnIntake) -> None:
    sales_execution = execution_arn(SALES_SM, CUSTOMERS_RUN)
    aws.describe(described(executionArn=sales_execution, stateMachineArn=SALES_SM), arn=sales_execution)
    hint = replace(hint_of(intake), native={"execution_arn": sales_execution})
    confirmation = intake.confirm(hint, trigger="final_failure")
    assert confirmation.verdict == "discarded"
    assert "another state machine" in confirmation.reason


def test_a_discovered_name_taken_by_a_configured_pipeline_is_not_confused(
    aws: Aws, intake_of: Callable[..., SfnIntake]
) -> None:
    """The pipeline id ``customers-sync`` is configured for the sales machine; with ``discover``
    the customers machine (named ``customers-sync``) does not take that id: its events give no
    hint, and a hint that names the pipeline anyway (an older pending event) is discarded."""
    intake = intake_of(pipelines={"customers-sync": {"state_machine": SALES_SM}}, discover=True)
    assert intake.normalize("eventbridge", load_json("events/failed.json"), {}) == []
    original = hint_of(intake_of())
    hint = replace(original, ref=replace(original.ref, pipeline_id="customers-sync"))
    aws.describe(described())
    confirmation = intake.confirm(hint, trigger="final_failure")
    assert confirmation.verdict == "discarded"
    assert "another state machine" in confirmation.reason


def test_an_execution_of_another_name_is_discarded(aws: Aws, intake: SfnIntake) -> None:
    other = execution_arn(CUSTOMERS_SM, "customers-2026-10-01")
    aws.describe(described(executionArn=other, name="customers-2026-10-01"), arn=other)
    hint = replace(hint_of(intake), native={"execution_arn": other})
    confirmation = intake.confirm(hint, trigger="final_failure")
    assert confirmation.verdict == "discarded"
    assert confirmation.reason == f"the execution is not run {CUSTOMERS_RUN}"


# =========================================================================== retry later


@pytest.mark.parametrize(
    ("code", "http", "reason"),
    [
        ("ThrottlingException", 400, "DescribeExecution failed (transient OrchestratorError)"),
        ("InternalFailure", 500, "DescribeExecution failed (transient OrchestratorError)"),
        ("AccessDeniedException", 400, "DescribeExecution failed (PermissionDenied)"),
        ("KmsThrottlingException", 400, "DescribeExecution failed (transient OrchestratorError)"),
    ],
)
def test_describe_problems_are_retried_later(
    aws: Aws, intake: SfnIntake, code: str, http: int, reason: str
) -> None:
    aws.describe_error(code, http=http)
    confirmation = intake.confirm(hint_of(intake), trigger="final_failure")
    assert (confirmation.verdict, confirmation.reason, confirmation.failures) == ("retry_later", reason, ())


def test_a_describe_answer_without_status_is_retried_later(
    aws: Aws, intake: SfnIntake, monkeypatch: pytest.MonkeyPatch
) -> None:
    aws.describe(described())
    client = intake.rt.clients.sfn
    original = client.describe_execution

    def without_status(**params: Any) -> dict[str, Any]:
        response = dict(original(**params))
        response.pop("status")
        return response

    monkeypatch.setattr(client, "describe_execution", without_status)  # botocore would not build it
    confirmation = intake.confirm(hint_of(intake), trigger="final_failure")
    assert confirmation.verdict == "retry_later"
    assert "without execution ARN or status" in confirmation.reason


def test_a_transient_history_error_is_retried_later_without_counting(aws: Aws, intake: SfnIntake) -> None:
    aws.describe(described())
    aws.history_error("ThrottlingException")
    hint = hint_of(intake)
    first = intake.confirm(hint, trigger="final_failure")
    assert (first.verdict, first.reason) == (
        "retry_later",
        "reading the execution history failed (transient OrchestratorError)",
    )
    aws.describe(described())
    aws.history_error("KmsAccessDeniedException")
    second = intake.confirm(hint, trigger="final_failure")
    assert (
        second.reason
        == f"the execution history is not usable yet (PermissionDenied; check 1 of {MAX_UNREADABLE_CHECKS})"
    )


def test_an_unreadable_history_is_confirmed_run_level_after_three_checks(
    aws: Aws, intake: SfnIntake, caplog: pytest.LogCaptureFixture
) -> None:
    caplog.set_level(logging.DEBUG)
    hint = hint_of(intake)
    for check in range(1, MAX_UNREADABLE_CHECKS + 1):
        aws.describe(described())
        aws.history_error("KmsAccessDeniedException")
        confirmation = intake.confirm(hint, trigger="final_failure")
        assert confirmation.verdict == "retry_later"
        assert f"check {check} of {MAX_UNREADABLE_CHECKS}" in confirmation.reason
    aws.describe(described())
    aws.history_error("KmsAccessDeniedException")
    confirmation = intake.confirm(hint, trigger="final_failure")
    assert confirmation.verdict == "confirmed"
    assert confirmation.reason == (
        "FAILED; confirmed as a run-level failure. "
        "The execution history could not be used in 4 checks (PermissionDenied)."
    )
    (failure,) = confirmation.failures
    assert failure.ref == hint.ref.with_step(RUN_LEVEL_STEP)
    assert failure.native["localized"] is False
    assert failure.error_name == "ValueError"
    assert failure.error_summary == causes.summarize("ValueError", described()["cause"])
    assert PII not in json.dumps(dict(failure.native))
    assert not [record for record in caplog.records if PII in record.getMessage()]

    aws.describe(described())  # remembered: no further history read
    assert intake.confirm(hint, trigger="final_failure") == confirmation


def test_a_history_without_terminal_event_is_retried_later(aws: Aws, intake: SfnIntake) -> None:
    aws.describe(described())
    aws.history(history_events("customers_valueerror.json")[:-1])
    confirmation = intake.confirm(hint_of(intake), trigger="final_failure")
    assert confirmation.verdict == "retry_later"
    assert "no terminal event in the history yet; check 1 of 3" in confirmation.reason


def test_a_kms_denied_describe_falls_back_to_metadata(aws: Aws, intake: SfnIntake) -> None:
    aws.describe_error("KmsAccessDeniedException")
    aws.stubber.add_response(
        "describe_execution",
        described(input=None),
        {"executionArn": CUSTOMERS_EXECUTION, "includedData": "METADATA_ONLY"},
    )
    aws.history(history_events("customers_valueerror.json"))
    assert intake.confirm(hint_of(intake), trigger="final_failure").verdict == "confirmed"


def test_reasons_and_logs_never_carry_payloads(
    aws: Aws, intake: SfnIntake, caplog: pytest.LogCaptureFixture
) -> None:
    caplog.set_level(logging.DEBUG)
    hint = hint_of(intake)
    reasons = []
    for answer in (described(status="RUNNING"), described(redriveCount=2), described()):
        aws.describe(copy.deepcopy(answer))
        if answer["status"] == "FAILED" and answer["redriveCount"] == 0:
            aws.history(history_events("customers_valueerror.json"))
        reasons.append(intake.confirm(hint, trigger="final_failure").reason)
    assert all(PII not in reason for reason in reasons)
    assert not [record for record in caplog.records if PII in record.getMessage()]
