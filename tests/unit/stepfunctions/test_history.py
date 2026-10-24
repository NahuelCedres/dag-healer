"""Tests of :mod:`dag_healer.stepfunctions.history`: section 9.5 localisation on AWS-shaped histories.

Every file of ``fixtures/stepfunctions/history`` is a complete execution history as
``GetExecutionHistory`` returns it (``{"description", "events", "expected", "variants"?}``); the
table test localises it and compares the ``expected`` fields (and each ``variants`` entry, with
its ``redrive_count``). The other tests cover segments, odd inputs and robustness.
"""

from __future__ import annotations

import copy
import dataclasses
import json
import random
from collections.abc import Iterator, Mapping
from datetime import datetime, timedelta, timezone
from typing import Any

import pytest

from dag_healer.core.ids import NO_MAP_INDEX, RUN_LEVEL_STEP
from dag_healer.stepfunctions import history
from dag_healer.stepfunctions.history import Localized, event_details, localize, segment
from dag_healer.stepfunctions.runtime import NATIVE_KEYS, native_ref, to_utc

from .conftest import FIXTURES, api_response, load_json, make_client

HISTORY_DIR = FIXTURES / "history"
HISTORY_FILES = sorted(path.name for path in HISTORY_DIR.glob("*.json"))
PII = "alice.pii@example.com"
_TIMESTAMP_FIELDS = ("started_at", "failed_at")


def _cases() -> Iterator[Any]:
    for name in HISTORY_FILES:
        data = load_json(f"history/{name}")
        stem = name.removesuffix(".json")
        yield pytest.param(name, None, data["expected"], id=stem)
        for variant in data.get("variants", ()):
            count = variant["redrive_count"]
            yield pytest.param(name, count, variant["expected"], id=f"{stem}-redrive_count={count}")


def _events(name: str) -> list[dict[str, Any]]:
    return load_json(f"history/{name}")["events"]


def _assert_fields(localized: Localized, expected: Mapping[str, Any]) -> None:
    actual = dataclasses.asdict(localized)
    for key, wanted in expected.items():
        got = actual[key]
        if key in _TIMESTAMP_FIELDS:
            wanted = to_utc(wanted) if wanted is not None else None
        if key == "notes":
            got = list(got)
        assert got == wanted, f"{key}: expected {wanted!r}, got {got!r}"


# =========================================================================== the table


def test_the_table_covers_the_required_cases() -> None:
    assert len(HISTORY_FILES) >= 15
    for name in (
        "sales_keyerror_after_retries.json",
        "customers_valueerror.json",
        "map_iteration_3.json",
        "parallel_branch.json",
        "caught_then_uncaught.json",
        "map_run_failed.json",
        "execution_timed_out.json",
        "evaluation_failed.json",
        "evaluation_failed_in_map.json",
        "jsonpath_runtime_error.json",
        "redriven_failed_again.json",
        "inventory_legacy_lambda.json",
        "state_entered_previous_zero.json",
        "fail_state_after_catch.json",
    ):
        assert name in HISTORY_FILES


@pytest.mark.parametrize(("name", "redrive_count", "expected"), list(_cases()))
def test_localize_fixture(name: str, redrive_count: int | None, expected: dict[str, Any]) -> None:
    localized = localize(_events(name), redrive_count=redrive_count)
    _assert_fields(localized, expected)


@pytest.mark.parametrize("name", HISTORY_FILES)
def test_fixture_is_a_complete_aws_history(name: str) -> None:
    """Ids run from 1 with ExecutionStarted first, links point backwards, and the events validate
    against botocore's GetExecutionHistory output shape (as Stubber validates responses)."""
    from botocore.validate import validate_parameters

    data = load_json(f"history/{name}")
    assert data["description"].strip() and data["expected"]
    events = data["events"]
    assert [event["id"] for event in events] == list(range(1, len(events) + 1))
    assert events[0]["type"] == "ExecutionStarted"
    for event in events:
        assert 0 <= event["previousEventId"] < event["id"]
        assert event["timestamp"].endswith("Z") and "." in event["timestamp"]
        assert len([key for key in event if key.endswith("EventDetails")]) <= 1
    model = make_client("stepfunctions").meta.service_model
    shape = model.operation_model("GetExecutionHistory").output_shape
    validate_parameters(api_response({"events": events}), shape)


@pytest.mark.parametrize("name", HISTORY_FILES)
def test_event_order_does_not_matter(name: str) -> None:
    events = _events(name)
    shuffled = copy.deepcopy(events)
    random.Random(name).shuffle(shuffled)
    assert localize(shuffled) == localize(events)
    assert localize(list(reversed(events))) == localize(events)


@pytest.mark.parametrize("name", HISTORY_FILES)
def test_botocore_datetimes_and_iso_text_localise_alike(name: str) -> None:
    events = _events(name)
    assert localize(api_response({"events": events})["events"]) == localize(events)


@pytest.mark.parametrize("name", HISTORY_FILES)
def test_native_fields_fit_native_ref(name: str) -> None:
    localized = localize(_events(name))
    fields = localized.native_fields()
    assert set(fields) <= set(NATIVE_KEYS)
    assert "cause" not in fields
    native = native_ref(**fields)
    assert json.loads(json.dumps(native)) == native
    assert native["localized"] is True


def test_payloads_never_reach_the_localisation() -> None:
    events = _events("customers_valueerror.json")
    assert PII in json.dumps(events)  # inputs, outputs and the Lambda parameters carry it
    localized = localize(events)
    assert PII not in repr(localized)
    assert PII not in json.dumps(localized.native_fields())


# =========================================================================== segments


def _redriven() -> list[dict[str, Any]]:
    return _events("redriven_failed_again.json")


def test_segment_of_the_latest_redrive() -> None:
    events = _redriven()
    latest = segment(events)
    assert latest[0]["type"] == "ExecutionRedriven"
    assert [event["id"] for event in latest] == list(range(12, 21))
    assert segment(events, 1) == latest


def test_segment_zero_ends_before_the_first_redrive() -> None:
    first = segment(_redriven(), 0)
    assert first[0]["type"] == "ExecutionStarted"
    assert first[-1]["type"] == "ExecutionFailed"
    assert [event["id"] for event in first] == list(range(1, 12))


def test_segment_missing_from_the_events_is_empty() -> None:
    assert segment(_redriven(), 2) == []


def test_events_without_markers_are_one_segment() -> None:
    tail = [event for event in _redriven() if event["id"] >= 16]
    assert [event["id"] for event in segment(tail)] == [16, 17, 18, 19, 20]
    assert [event["id"] for event in segment(tail, 0)] == [16, 17, 18, 19, 20]


def test_events_before_the_first_marker_belong_to_the_earlier_segment() -> None:
    """A history read from its end may start in the middle of a segment."""
    tail = [event for event in _redriven() if event["id"] >= 8]
    assert [event["id"] for event in segment(tail, 0)] == [8, 9, 10, 11]
    assert segment(tail)[0]["type"] == "ExecutionRedriven"


def test_redriven_event_without_a_count_follows_the_previous_segment() -> None:
    events = _redriven()
    for event in events:
        if event["type"] == "ExecutionRedriven":
            del event["executionRedrivenEventDetails"]
    assert [event["id"] for event in segment(events, 1)] == list(range(12, 21))
    assert localize(events).redrive_count == 1


def test_segment_sorts_and_drops_unusable_events() -> None:
    events = _events("caught_then_uncaught.json")
    noisy: list[Any] = [*reversed(events), "not an event", {"type": "TaskFailed"}, {"id": 3}]
    assert segment(noisy) == events


@pytest.mark.parametrize("value", [-1, True, "1", 1.5])
def test_invalid_redrive_count_is_refused(value: Any) -> None:
    with pytest.raises(ValueError, match="redrive_count"):
        segment(_redriven(), value)
    with pytest.raises(ValueError, match="redrive_count"):
        localize(_redriven(), redrive_count=value)


# =========================================================================== event details


def test_event_details_by_type() -> None:
    failed = {"type": "TaskFailed", "taskFailedEventDetails": {"error": "E"}}
    entered = {"type": "MapStateEntered", "stateEnteredEventDetails": {"name": "M"}}
    exited = {"type": "PassStateExited", "stateExitedEventDetails": {"name": "P"}}
    iteration = {"type": "MapIterationFailed", "mapIterationFailedEventDetails": {"index": 2}}
    assert event_details(failed) == {"error": "E"}
    assert event_details(entered) == {"name": "M"}
    assert event_details(exited) == {"name": "P"}
    assert event_details(iteration) == {"index": 2}


def test_event_details_fallbacks() -> None:
    assert event_details({"type": "TaskFailed", "lambdaFunctionFailedEventDetails": {"error": "E"}}) == {
        "error": "E"
    }
    assert event_details({"type": "TaskFailed", "taskFailedEventDetails": "nope"}) == {}
    assert event_details({"type": "MapStateFailed"}) == {}
    assert event_details({"type": 7}) == {}
    assert event_details("not an event") == {}  # type: ignore[arg-type]


# =========================================================================== odd inputs


def test_empty_history() -> None:
    for events in ([], None):
        localized = localize(events)  # type: ignore[arg-type]
        assert localized.step_id == RUN_LEVEL_STEP
        assert localized.terminal is False
        assert localized.notes == ("The history holds no event of the latest redrive segment.",)


def test_running_execution_has_no_terminal_event() -> None:
    events = _events("sales_keyerror_after_retries.json")[:10]  # up to the first TaskFailed
    localized = localize(events)
    assert localized.terminal is False
    assert (localized.step_id, localized.failed_event_id, localized.retry_attempts) == (
        "TransformSales",
        10,
        0,
    )
    assert localized.notes == ("No terminal event: the execution is still running or the history was cut.",)


def test_succeeded_execution() -> None:
    events = _events("sales_keyerror_after_retries.json")[:6]
    last = events[-1]
    events.append(
        {
            "id": 7,
            "previousEventId": 6,
            "timestamp": last["timestamp"],
            "type": "ExecutionSucceeded",
            "executionSucceededEventDetails": {},
        }
    )
    localized = localize(events)
    assert (localized.step_id, localized.terminal) == (RUN_LEVEL_STEP, False)
    assert localized.notes == ("The execution succeeded in this redrive segment.",)


def test_step_failure_without_its_entry_is_run_level_with_missing_entry() -> None:
    """A redrive that resumed a state without entering it again: segment 1 alone has no entry."""
    events = _events("redriven_without_reentry.json")
    localized = localize(segment(events))
    assert localized.missing_entry is True
    assert localized.step_id == RUN_LEVEL_STEP
    assert (localized.event_type, localized.failed_event_id, localized.error) == (
        "TaskFailed",
        15,
        "KeyError",
    )
    assert localized.redrive_count == 1
    assert localized.notes == ("The state entry of failure event 15 is not in the history.",)
    assert localize(events).missing_entry is False


def test_ids_and_types_are_parsed_defensively() -> None:
    events = copy.deepcopy(_events("caught_then_uncaught.json"))
    for event in events:
        event["id"] = str(event["id"])
        event["previousEventId"] = float(event["previousEventId"])
    events.append({"id": True, "type": "TaskFailed"})
    events.append({"id": 99, "type": "  "})
    events.append({"id": 0, "type": "ExecutionFailed"})
    assert localize(events) == localize(_events("caught_then_uncaught.json"))


def test_the_first_copy_of_a_repeated_event_wins() -> None:
    events = _events("caught_then_uncaught.json")
    impostor = {**events[-2], "taskFailedEventDetails": {"resourceType": "lambda", "resource": "invoke"}}
    assert localize([*events, impostor]) == localize(events)


def test_forward_links_are_ignored() -> None:
    events = copy.deepcopy(_events("sales_keyerror_after_retries.json"))
    events[15]["previousEventId"] = 99  # the final TaskFailed points forward: no link
    localized = localize(events)
    assert (localized.step_id, localized.failed_event_id, localized.retry_attempts) == (
        "TransformSales",
        16,
        2,
    )


def test_failure_without_error_takes_the_terminal_error() -> None:
    events = copy.deepcopy(_events("caught_then_uncaught.json"))
    details = events[-2]["taskFailedEventDetails"]
    del details["error"], details["cause"]
    localized = localize(events)
    assert localized.error == "ClientError"
    assert localized.cause == events[-1]["executionFailedEventDetails"]["cause"]


def test_failure_without_cause_takes_the_cause_of_the_same_terminal_error() -> None:
    events = copy.deepcopy(_events("caught_then_uncaught.json"))
    del events[-2]["taskFailedEventDetails"]["cause"]
    assert localize(events).cause == events[-1]["executionFailedEventDetails"]["cause"]
    events[-1]["executionFailedEventDetails"]["error"] = "Other"
    assert localize(events).cause is None


QUALIFIED_FUNCTION = "arn:aws:lambda:eu-west-1:123456789012:function:fn-c:$LATEST"


@pytest.mark.parametrize(
    ("parameters", "function_name"),
    [
        ('{"FunctionName": "fn-a", "Payload": {"x": 1}}', "fn-a"),
        ({"FunctionName": "fn-b"}, "fn-b"),
        (json.dumps({"FunctionName": QUALIFIED_FUNCTION}), QUALIFIED_FUNCTION),
        ("{not json", None),
        ('["FunctionName"]', None),
        ('{"FunctionName": 7}', None),
        ('{"Payload": {}}', None),
    ],
)
def test_function_name_from_task_parameters(parameters: Any, function_name: str | None) -> None:
    events = copy.deepcopy(_events("caught_then_uncaught.json"))
    events[7]["taskScheduledEventDetails"]["parameters"] = parameters  # QuarantineInput's TaskScheduled
    assert localize(events).function_name == function_name


def test_deeply_nested_parameters_do_not_break_the_localisation() -> None:
    events = copy.deepcopy(_events("caught_then_uncaught.json"))
    events[7]["taskScheduledEventDetails"]["parameters"] = "[" * 200_000 + "]" * 200_000
    localized = localize(events)
    assert (localized.step_id, localized.function_name) == ("QuarantineInput", None)


def test_huge_parameters_are_not_parsed() -> None:
    events = copy.deepcopy(_events("caught_then_uncaught.json"))
    events[7]["taskScheduledEventDetails"]["parameters"] = json.dumps(
        {"FunctionName": "fn", "Payload": "x" * (history._MAX_PARAMETERS_CHARS + 1)}
    )
    assert localize(events).function_name is None


def test_task_scheduled_of_another_service() -> None:
    events = copy.deepcopy(_events("caught_then_uncaught.json"))
    for event in events[7:10]:
        details = next(value for key, value in event.items() if key.endswith("EventDetails"))
        details.update(resourceType="glue", resource="startJobRun.sync")
    localized = localize(events)
    assert (localized.resource_type, localized.resource, localized.function_name) == (
        "glue",
        "startJobRun.sync",
        None,
    )


def test_evaluation_failed_found_by_name_without_links() -> None:
    events = copy.deepcopy(_events("evaluation_failed.json"))
    evaluation = next(event for event in events if event["type"] == "EvaluationFailed")
    evaluation["previousEventId"] = 0
    localized = localize(events)
    assert (localized.step_id, localized.entered_event_id) == ("SummarizeTotals", evaluation["id"] - 1)
    assert "without previousEventId links" in localized.notes[0]


def test_evaluation_failed_without_a_state_name_uses_its_entry() -> None:
    events = copy.deepcopy(_events("evaluation_failed.json"))
    evaluation = next(event for event in events if event["type"] == "EvaluationFailed")
    del evaluation["evaluationFailedEventDetails"]["state"]
    assert localize(events).step_id == "SummarizeTotals"


def test_task_failure_with_no_entry_anywhere_is_run_level() -> None:
    events = [
        {"id": 1, "previousEventId": 0, "timestamp": "2026-10-02T06:00:00.000Z", "type": "ExecutionStarted"},
        {
            "id": 2,
            "previousEventId": 0,
            "timestamp": "2026-10-02T06:00:01.000Z",
            "type": "TaskFailed",
            "taskFailedEventDetails": {"resourceType": "lambda", "resource": "invoke", "error": "E"},
        },
        {
            "id": 3,
            "previousEventId": 2,
            "timestamp": "2026-10-02T06:00:01.100Z",
            "type": "ExecutionFailed",
            "executionFailedEventDetails": {"error": "E"},
        },
    ]
    localized = localize(events)
    assert (localized.step_id, localized.missing_entry, localized.failed_event_id) == (
        RUN_LEVEL_STEP,
        True,
        2,
    )
    assert localized.terminal is True


def test_execution_failed_without_any_open_state_is_run_level() -> None:
    events = _events("sales_keyerror_after_retries.json")[:6]
    events.append(
        {
            "id": 7,
            "previousEventId": 6,
            "timestamp": "2026-10-02T06:00:03.000Z",
            "type": "ExecutionFailed",
            "executionFailedEventDetails": {"error": "States.DataLimitExceeded"},
        }
    )
    localized = localize(events)
    assert (localized.step_id, localized.error, localized.event_type) == (
        RUN_LEVEL_STEP,
        "States.DataLimitExceeded",
        "ExecutionFailed",
    )


def test_epoch_timestamps_are_seconds_or_milliseconds() -> None:
    events = copy.deepcopy(_events("caught_then_uncaught.json"))
    expected = localize(events).failed_at
    assert expected is not None
    seconds = copy.deepcopy(events)
    millis = copy.deepcopy(events)
    for plain, in_seconds, in_millis in zip(events, seconds, millis, strict=True):
        moment = to_utc(plain["timestamp"])
        assert moment is not None
        in_seconds["timestamp"] = moment.timestamp()
        in_millis["timestamp"] = int(moment.timestamp() * 1000)
    assert abs(localize(seconds).failed_at - expected) < timedelta(milliseconds=1)  # type: ignore[operator]
    assert localize(millis).failed_at == expected


def test_unparseable_timestamps_give_none() -> None:
    events = copy.deepcopy(_events("caught_then_uncaught.json"))
    for event in events:
        event["timestamp"] = "yesterday"
    localized = localize(events)
    assert (localized.started_at, localized.failed_at) == (None, None)
    assert localized.step_id == "QuarantineInput"


def test_run_level_property_and_defaults() -> None:
    assert Localized(step_id=RUN_LEVEL_STEP).run_level is True
    assert Localized(step_id="TransformSales").run_level is False
    assert Localized(step_id="X").map_index == NO_MAP_INDEX
    assert Localized(step_id="X").from_history is True


# =========================================================================== robustness


_TYPES = [
    "ExecutionStarted",
    "ExecutionFailed",
    "ExecutionTimedOut",
    "ExecutionRedriven",
    "TaskStateEntered",
    "TaskStateExited",
    "TaskScheduled",
    "TaskStarted",
    "TaskFailed",
    "LambdaFunctionFailed",
    "MapStateEntered",
    "MapStateExited",
    "MapIterationStarted",
    "MapIterationFailed",
    "MapStateFailed",
    "ParallelStateEntered",
    "ParallelStateStarted",
    "ParallelStateFailed",
    "MapRunStarted",
    "MapRunFailed",
    "EvaluationFailed",
    "FailStateEntered",
]


def _random_history(rng: random.Random, size: int) -> list[dict[str, Any]]:
    start = datetime(2026, 10, 2, 6, tzinfo=timezone.utc)
    events: list[dict[str, Any]] = []
    for event_id in range(1, size + 1):
        kind = rng.choice(_TYPES)
        details: Any = rng.choice(
            [
                {"name": rng.choice(["A", "B", "M"]), "index": rng.randint(-1, 3)},
                {"error": rng.choice(["E", None, 3]), "cause": rng.choice(["c", None])},
                {"redriveCount": rng.choice([1, 2, "x", None])},
                {"state": rng.choice(["A", None])},
                "garbage",
            ]
        )
        events.append(
            {
                "id": event_id,
                "previousEventId": rng.randint(0, event_id + 2),
                "timestamp": rng.choice([(start + timedelta(seconds=event_id)).isoformat(), None, 12, "x"]),
                "type": kind,
                history._detail_keys(kind)[0]: details,
            }
        )
    return events


@pytest.mark.parametrize("seed", range(40))
def test_localize_never_raises_on_random_histories(seed: int) -> None:
    rng = random.Random(seed)
    events = _random_history(rng, rng.randint(1, 80))
    for redrive_count in (None, 0, 1):
        localized = localize(events, redrive_count=redrive_count)
        assert isinstance(localized, Localized)
        assert localized.map_index >= NO_MAP_INDEX
        assert localized.retry_attempts >= 0
        native_ref(**localized.native_fields())


def test_large_inline_map_history() -> None:
    """A Map of 2,000 iterations (about 12,000 events, below history_max_events) stays linear."""
    events: list[dict[str, Any]] = [
        {"id": 1, "previousEventId": 0, "timestamp": "2026-10-02T06:00:00.000Z", "type": "ExecutionStarted"},
        {
            "id": 2,
            "previousEventId": 0,
            "timestamp": "2026-10-02T06:00:00.010Z",
            "type": "MapStateEntered",
            "stateEnteredEventDetails": {"name": "PerItem"},
        },
        {"id": 3, "previousEventId": 2, "timestamp": "2026-10-02T06:00:00.020Z", "type": "MapStateStarted"},
    ]

    def add(kind: str, previous: int, **details: Any) -> int:
        event: dict[str, Any] = {
            "id": len(events) + 1,
            "previousEventId": previous,
            "timestamp": "2026-10-02T06:00:01.000Z",
            "type": kind,
        }
        if details:
            event[history._detail_keys(kind)[0]] = details
        events.append(event)
        return event["id"]

    task = {"resourceType": "lambda", "resource": "invoke"}
    for index in range(2000):
        started = add("MapIterationStarted", 3, name="PerItem", index=index)
        entry = add("TaskStateEntered", started, name="Work")
        scheduled = add("TaskScheduled", entry, region="eu-west-1", parameters="{}", **task)
        running = add("TaskStarted", scheduled, **task)
        if index == 1500:
            failed = add("TaskFailed", running, error="KeyError", **task)
            iteration = add("MapIterationFailed", failed, name="PerItem", index=index)
            break
        succeeded = add("TaskSucceeded", running, **task)
        exited = add("TaskStateExited", succeeded, name="Work")
        add("MapIterationSucceeded", exited, name="PerItem", index=index)
    map_failed = add("MapStateFailed", iteration)
    add("ExecutionFailed", map_failed, error="KeyError")
    localized = localize(events)
    assert (localized.step_id, localized.map_index, localized.step_path) == (
        "Work",
        1500,
        "PerItem[1500]/Work",
    )
    assert localized.failed_event_id == failed
