"""Tests of :mod:`dag_healer.stepfunctions.events`: EventBridge envelopes and SQS messages -> hints.

The fixtures in ``fixtures/stepfunctions/events`` are EventBridge "Step Functions Execution Status
Change" events (FAILED, TIMED_OUT, ABORTED, SUCCEEDED), each plain and wrapped in the SQS message
the queue port hands over (``{"MessageId", "Body"}``).
"""

from __future__ import annotations

import copy
import json
from datetime import datetime, timezone
from typing import Any

import pytest

from dag_healer.core.failures import FailureHint
from dag_healer.core.ids import UNRESOLVED_STEP, AttemptRef
from dag_healer.orchestrators.conformance import assert_native_fits, assert_ref_fits, no_network
from dag_healer.stepfunctions import events
from dag_healer.stepfunctions.events import (
    MAX_BODY_CHARS,
    MAX_ERROR_TEXT_CHARS,
    ExecutionStatusChange,
    error_text,
    normalize,
    parse_status_change,
    unwrap,
)
from dag_healer.stepfunctions.runtime import PipelineIndex

from .conftest import ACCOUNT, CUSTOMERS_SM, INSTANCE, INVENTORY_SM, REGION, SALES_SM, load_json, sfn_options

KINDS = ("failed", "timed_out", "aborted", "succeeded")
PII = "alice.pii@example.com"
CUSTOMERS_EXECUTION = f"arn:aws:states:{REGION}:{ACCOUNT}:execution:customers-sync:customers-2026-10-02"
DEFAULT_STATUSES = ("FAILED", "TIMED_OUT")


def envelope(kind: str) -> dict[str, Any]:
    return load_json(f"events/{kind}.json")


def sqs_message(kind: str) -> dict[str, Any]:
    return load_json(f"events/sqs_{kind}.json")


def index(*, discover: bool = False) -> PipelineIndex:
    return PipelineIndex.from_options(sfn_options(discover=discover))


def hints(
    payload: Any, *, statuses: tuple[str, ...] = DEFAULT_STATUSES, discover: bool = False
) -> list[FailureHint]:
    return normalize(payload, instance=INSTANCE, pipelines=index(discover=discover), statuses=statuses)


def detail_of(payload: dict[str, Any]) -> dict[str, Any]:
    return payload["detail"]


# =========================================================================== fixtures


@pytest.mark.parametrize("kind", KINDS)
def test_sqs_fixture_wraps_the_plain_envelope(kind: str) -> None:
    message = sqs_message(kind)
    assert set(message) == {"MessageId", "Body"}
    assert json.loads(message["Body"]) == envelope(kind)


@pytest.mark.parametrize("kind", KINDS)
def test_envelope_fixture_is_a_status_change(kind: str) -> None:
    data = envelope(kind)
    assert data["source"] == "aws.states"
    assert data["detail-type"] == "Step Functions Execution Status Change"
    assert data["detail"]["status"] == kind.upper()
    assert data["resources"] == [data["detail"]["executionArn"]]


# =========================================================================== unwrap


def test_unwrap_an_envelope() -> None:
    data = envelope("failed")
    assert unwrap(data) == (data, "eventbridge")


@pytest.mark.parametrize("key", ["Body", "body"])
def test_unwrap_an_sqs_message(key: str) -> None:
    message = sqs_message("failed")
    payload = {"MessageId": message["MessageId"], key: message["Body"]}
    assert unwrap(payload) == (envelope("failed"), "sqs")


def test_unwrap_bytes_and_parsed_bodies() -> None:
    body = sqs_message("failed")["Body"]
    assert unwrap({"Body": body.encode("utf-8")}) == (envelope("failed"), "sqs")
    assert unwrap({"Body": envelope("failed")}) == (envelope("failed"), "sqs")


def test_an_envelope_with_a_body_key_stays_an_envelope() -> None:
    data = {**envelope("failed"), "Body": "ignored"}
    assert unwrap(data) == (data, "eventbridge")


@pytest.mark.parametrize(
    ("payload", "message"),
    [
        ("not a mapping", "JSON object"),
        (["a", "list"], "JSON object"),
        ({"Body": "{not json"}, "not JSON"),
        ({"Body": "[1, 2]"}, "not a JSON object"),
        ({"Body": "null"}, "not a JSON object"),
        ({"Body": 42}, "JSON text"),
        ({"Body": None}, "JSON text"),
        ({"Body": b"\xff\xfe"}, "UTF-8"),
        ({"Body": " " * (MAX_BODY_CHARS + 1)}, "longer than"),
        ({"Body": "[" * 100_000 + "]" * 100_000}, "not JSON"),
    ],
)
def test_unwrap_rejects_malformed_payloads(payload: Any, message: str) -> None:
    with pytest.raises(ValueError, match=message):
        unwrap(payload)


# =========================================================================== parse_status_change


def test_parse_keeps_the_whitelisted_fields() -> None:
    change = parse_status_change(envelope("failed"))
    detail = detail_of(envelope("failed"))
    assert change == ExecutionStatusChange(
        execution_arn=CUSTOMERS_EXECUTION,
        state_machine_arn=CUSTOMERS_SM,
        name="customers-2026-10-02",
        status="FAILED",
        redrive_count=0,
        start_date=datetime(2026, 10, 2, 6, 0, tzinfo=timezone.utc),
        stop_date=datetime(2026, 10, 2, 6, 0, 3, 700000, tzinfo=timezone.utc),
        error="ValueError",
        cause=detail["cause"],
        state_machine_version_arn=None,
        state_machine_alias_arn=None,
        event_id="315c1398-40ff-a850-213b-158f73e60175",
        event_time=datetime(2026, 10, 2, 6, 0, 4, tzinfo=timezone.utc),
    )
    assert change.attempt == 1
    assert PII not in repr(change)


@pytest.mark.parametrize(
    ("mutate", "message"),
    [
        (lambda e: e.update(source="aws.lambda"), "source"),
        (lambda e: e.pop("source"), "source"),
        (lambda e: e.update({"detail-type": "Step Functions State Machine Status Change"}), "detail-type"),
        (lambda e: e.pop("detail"), "detail"),
        (lambda e: e.update(detail="{}"), "detail"),
        (lambda e: e["detail"].pop("executionArn"), "executionArn"),
        (lambda e: e["detail"].update(executionArn=""), "executionArn"),
        (lambda e: e["detail"].update(executionArn=12), "executionArn"),
        (lambda e: e["detail"].update(executionArn="customers-2026-10-02"), "executionArn"),
        (lambda e: e["detail"].update(executionArn=CUSTOMERS_SM), "executionArn"),
        (lambda e: e["detail"].pop("stateMachineArn"), "stateMachineArn"),
        (lambda e: e["detail"].update(stateMachineArn=CUSTOMERS_EXECUTION), "stateMachineArn"),
        (lambda e: e["detail"].update(stateMachineArn=SALES_SM), "does not belong"),
        (
            lambda e: e["detail"].update(stateMachineArn=CUSTOMERS_SM.replace(REGION, "us-east-1")),
            "does not belong",
        ),
        (lambda e: e["detail"].update(name="customers-2026-10-01"), "name"),
        (lambda e: e["detail"].update(name=""), "name"),
        (lambda e: e["detail"].update(name=["customers-2026-10-02"]), "name"),
        (lambda e: e["detail"].pop("status"), "status"),
        (lambda e: e["detail"].update(status=3), "status"),
        (lambda e: e["detail"].update(redriveCount=-1), "redriveCount"),
        (lambda e: e["detail"].update(redriveCount="1"), "redriveCount"),
        (lambda e: e["detail"].update(redriveCount=True), "redriveCount"),
        (lambda e: e["detail"].update(redriveCount=1.5), "redriveCount"),
    ],
)
def test_parse_rejects_malformed_envelopes(mutate: Any, message: str) -> None:
    data = copy.deepcopy(envelope("failed"))
    mutate(data)
    with pytest.raises(ValueError, match=message):
        parse_status_change(data)


def test_parse_rejects_a_non_mapping() -> None:
    with pytest.raises(ValueError, match="JSON object"):
        parse_status_change("event")  # type: ignore[arg-type]


def test_parse_derives_a_missing_name_and_defaults_the_redrive_count() -> None:
    data = copy.deepcopy(envelope("failed"))
    del data["detail"]["name"], data["detail"]["redriveCount"]
    change = parse_status_change(data)
    assert (change.name, change.redrive_count) == ("customers-2026-10-02", 0)
    data["detail"]["redriveCount"] = None
    assert parse_status_change(data).redrive_count == 0
    data["detail"]["redriveCount"] = 2.0
    assert parse_status_change(data).redrive_count == 2


def test_parse_ignores_ill_typed_informational_fields() -> None:
    data = copy.deepcopy(envelope("failed"))
    data["detail"].update(
        startDate={"ms": 1},
        stopDate=True,
        error=404,
        cause=["x"],
        stateMachineVersionArn=7,
        stateMachineAliasArn="",
    )
    data.update(id=None, time="not a time")
    change = parse_status_change(data)
    assert (change.start_date, change.stop_date, change.error, change.cause) == (None, None, None, None)
    assert (change.state_machine_version_arn, change.state_machine_alias_arn) == (None, None)
    assert (change.event_id, change.event_time) == (None, None)


def test_parse_accepts_iso_dates_and_version_arns() -> None:
    data = copy.deepcopy(envelope("failed"))
    version = f"{CUSTOMERS_SM}:3"
    data["detail"].update(startDate="2026-10-02T06:00:00Z", stateMachineVersionArn=version)
    change = parse_status_change(data)
    assert change.start_date == datetime(2026, 10, 2, 6, 0, tzinfo=timezone.utc)
    assert change.state_machine_version_arn == version


# =========================================================================== error_text


@pytest.mark.parametrize(
    ("error", "cause", "text"),
    [
        ("KeyError", "'amount'", "KeyError: 'amount'"),
        ("States.Timeout", None, "States.Timeout"),
        (None, "An error occurred.", "An error occurred."),
        ("  ", "cause", "cause"),
        (None, None, ""),
        ("", "", ""),
    ],
)
def test_error_text(error: str | None, cause: str | None, text: str) -> None:
    assert error_text(error, cause) == text


def test_error_text_is_capped() -> None:
    text = error_text("E", "x" * (2 * MAX_ERROR_TEXT_CHARS))
    assert len(text) == MAX_ERROR_TEXT_CHARS
    assert text.startswith("E: x")


# =========================================================================== normalize


def test_failed_envelope_gives_one_unresolved_hint() -> None:
    data = envelope("failed")
    detail = detail_of(data)
    (hint,) = hints(data)
    assert hint.ref == AttemptRef(INSTANCE, "customers_sync", "customers-2026-10-02", UNRESOLVED_STEP, -1, 1)
    assert (hint.channel, hint.source, hint.received_at) == ("eventbridge", "eventbridge", None)
    assert hint.error_text == f"ValueError: {detail['cause']}"
    assert hint.native == {
        "execution_arn": CUSTOMERS_EXECUTION,
        "state_machine_arn": CUSTOMERS_SM,
        "start_date": "2026-10-02T06:00:00.000+00:00",
        "stop_date": "2026-10-02T06:00:03.700+00:00",
        "status": "FAILED",
        "redrive_count": 0,
        "error": "ValueError",
    }


def test_sqs_message_gives_an_sqs_hint() -> None:
    (from_queue,) = hints(sqs_message("failed"))
    (direct,) = hints(envelope("failed"))
    assert from_queue.source == "sqs"
    assert (from_queue.ref, from_queue.native, from_queue.error_text) == (
        direct.ref,
        direct.native,
        direct.error_text,
    )


@pytest.mark.parametrize("wrap", [envelope, sqs_message])
def test_timed_out_execution_is_watched_by_default(wrap: Any) -> None:
    (hint,) = hints(wrap("timed_out"))
    assert hint.ref.pipeline_id == "inventory_report"
    assert hint.ref.run_id == "inventory-2026-10-01"
    assert hint.error_text == "States.Timeout"
    assert hint.native["status"] == "TIMED_OUT"
    assert hint.native["state_machine_arn"] == INVENTORY_SM


@pytest.mark.parametrize("wrap", [envelope, sqs_message])
def test_aborted_executions_are_opt_in(wrap: Any) -> None:
    assert hints(wrap("aborted")) == []
    (hint,) = hints(wrap("aborted"), statuses=("FAILED", "TIMED_OUT", "ABORTED"))
    assert hint.ref.pipeline_id == "sales_daily_etl"
    assert hint.error_text == "ManualStop: Stopped by the on-call engineer: wrong input date"


@pytest.mark.parametrize("wrap", [envelope, sqs_message])
def test_succeeded_and_running_executions_give_no_hint(wrap: Any) -> None:
    assert hints(wrap("succeeded")) == []
    running = copy.deepcopy(envelope("failed"))
    running["detail"].update(status="RUNNING", stopDate=None, error=None, cause=None)
    assert hints(running) == []
    assert hints(running, statuses=("FAILED",)) == []


def test_unknown_status_texts_are_not_watched() -> None:
    data = copy.deepcopy(envelope("failed"))
    data["detail"]["status"] = "PENDING_REDRIVE"
    assert hints(data) == []


def test_redrive_count_gives_the_attempt() -> None:
    data = copy.deepcopy(envelope("failed"))
    data["detail"]["redriveCount"] = 2
    (hint,) = hints(data)
    assert hint.ref.attempt == 3
    assert hint.native["redrive_count"] == 2


def test_unwatched_machine_gives_no_hint() -> None:
    data = copy.deepcopy(envelope("failed"))
    other = f"arn:aws:states:{REGION}:{ACCOUNT}:stateMachine:payroll"
    data["detail"].update(
        stateMachineArn=other, executionArn=f"{other.replace('stateMachine', 'execution')}:run-1"
    )
    data["detail"]["name"] = "run-1"
    assert hints(data) == []
    (hint,) = hints(data, discover=True)
    assert hint.ref.pipeline_id == "payroll"


def test_a_machine_named_like_another_machines_pipeline_gives_no_hint() -> None:
    """With ``discover``, the customers machine (named ``customers-sync``) does not take the id of
    the pipeline ``customers-sync`` configured for the sales machine."""
    pipelines = PipelineIndex.from_options(
        sfn_options(discover=True, pipelines={"customers-sync": {"state_machine": SALES_SM}})
    )
    assert (
        normalize(envelope("failed"), instance=INSTANCE, pipelines=pipelines, statuses=DEFAULT_STATUSES) == []
    )


def test_distributed_map_children_give_no_hint() -> None:
    data = copy.deepcopy(envelope("failed"))
    data["detail"].update(
        stateMachineArn=f"{CUSTOMERS_SM}/ProcessFiles",
        executionArn=f"arn:aws:states:{REGION}:{ACCOUNT}:execution:customers-sync/ProcessFiles:child-0001",
        name="child-0001",
    )
    assert hints(data) == []
    assert hints(data, discover=True) == []


def test_express_executions_give_no_hint() -> None:
    data = copy.deepcopy(envelope("failed"))
    data["detail"]["executionArn"] = (
        f"arn:aws:states:{REGION}:{ACCOUNT}:express:customers-sync:customers-1:4f2a"
    )
    data["detail"]["name"] = "customers-1"
    assert hints(data) == []


def test_version_arn_reaches_native() -> None:
    data = copy.deepcopy(envelope("failed"))
    data["detail"]["stateMachineVersionArn"] = f"{CUSTOMERS_SM}:7"
    (hint,) = hints(data)
    assert hint.native["definition_version_arn"] == f"{CUSTOMERS_SM}:7"


def test_a_custom_channel_is_kept() -> None:
    (hint,) = normalize(
        envelope("failed"), instance=INSTANCE, pipelines=index(), statuses=DEFAULT_STATUSES, channel="custom"
    )
    assert hint.channel == "custom"


@pytest.mark.parametrize("wrap", [envelope, sqs_message])
@pytest.mark.parametrize("kind", KINDS)
def test_payloads_never_reach_a_hint(kind: str, wrap: Any) -> None:
    statuses = ("FAILED", "TIMED_OUT", "ABORTED", "SUCCEEDED")
    for hint in hints(wrap(kind), statuses=statuses):
        assert_ref_fits(hint.ref, INSTANCE, hint=True)
        assert_native_fits(hint.native, "hint.native")
        text = json.dumps(dict(hint.native)) + hint.error_text
        assert PII not in text
        detail = detail_of(envelope(kind))
        for payload in (detail.get("input"), detail.get("output")):
            if payload and payload != "{}":
                assert payload not in text


def test_normalize_is_pure_and_deterministic() -> None:
    payloads = [envelope(kind) for kind in KINDS] + [sqs_message(kind) for kind in KINDS]
    with no_network():
        first = [hints(copy.deepcopy(p), statuses=("FAILED", "TIMED_OUT", "ABORTED")) for p in payloads]
        again = [hints(copy.deepcopy(p), statuses=("FAILED", "TIMED_OUT", "ABORTED")) for p in payloads]
    assert first == again
    assert [len(result) for result in first] == [1, 1, 1, 0, 1, 1, 1, 0]


def test_normalize_does_not_change_its_payload() -> None:
    payload = sqs_message("failed")
    before = copy.deepcopy(payload)
    hints(payload)
    assert payload == before


@pytest.mark.parametrize("payload", [{}, {"MessageId": "1"}, {"Body": ""}, {"detail": {}}])
def test_normalize_rejects_empty_and_malformed_payloads(payload: dict[str, Any]) -> None:
    with pytest.raises(ValueError):
        hints(payload)


def test_module_exports() -> None:
    assert set(events.__all__) >= {"normalize", "unwrap", "parse_status_change", "error_text", "hint_native"}
