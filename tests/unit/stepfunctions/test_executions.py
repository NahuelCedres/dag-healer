"""Step Functions API calls shared by the ports (``stepfunctions/executions.py``).

``read_history``'s stop rules (the latest redrive segment, the segment of ``redrive_count``,
``ExecutionStarted``, ``history_max_events``, the page cap) and paging with exact request
parameters; ``describe_execution``'s KMS fallback; the thin wrappers' parameters.
"""

from __future__ import annotations

from collections.abc import Mapping
from datetime import timedelta
from typing import Any

import pytest

from dag_healer.core.errors import NotFound, OrchestratorError, PermissionDenied
from dag_healer.stepfunctions import executions
from dag_healer.stepfunctions.executions import (
    HISTORY_PAGE_SIZE,
    HistorySegment,
    describe_execution,
    describe_state_machine,
    describe_state_machine_for_execution,
    list_executions,
    read_history,
)

from .conftest import ACCOUNT, CUSTOMERS_SM, NOW, REGION, sfn_options

EXECUTION = f"arn:aws:states:{REGION}:{ACCOUNT}:execution:customers-sync:customers-2026-10-02"
STARTED = NOW - timedelta(hours=1)
HISTORY_PARAMS = {
    "executionArn": EXECUTION,
    "reverseOrder": True,
    "maxResults": HISTORY_PAGE_SIZE,
    "includeExecutionData": False,
}


def _event(event_id: int, kind: str, previous: int = 0, **details: Any) -> dict[str, Any]:
    return {
        "id": event_id,
        "previousEventId": previous,
        "type": kind,
        "timestamp": STARTED + timedelta(seconds=event_id),
        **details,
    }


def _redriven(event_id: int, count: int) -> dict[str, Any]:
    return _event(
        event_id, "ExecutionRedriven", event_id - 1, executionRedrivenEventDetails={"redriveCount": count}
    )


#: Started, failed, redriven twice (each redrive failed again): 12 events, oldest first.
HISTORY = [
    _event(1, "ExecutionStarted"),
    _event(2, "TaskStateEntered", 1),
    _event(3, "TaskFailed", 2),
    _event(4, "ExecutionFailed", 3),
    _redriven(5, 1),
    _event(6, "TaskStateEntered", 5),
    _event(7, "TaskFailed", 6),
    _event(8, "ExecutionFailed", 7),
    _redriven(9, 2),
    _event(10, "TaskStateEntered", 9),
    _event(11, "TaskFailed", 10),
    _event(12, "ExecutionFailed", 11),
]
NEWEST_FIRST = list(reversed(HISTORY))


def _ids(segment: HistorySegment) -> list[int]:
    return [int(event["id"]) for event in segment.events]


# =========================================================================== read_history


@pytest.mark.parametrize(
    ("redrive_count", "first_id"),
    [(None, 9), (2, 9), (5, 9), (1, 5), (0, 1)],
    ids=["latest segment", "segment 2", "a segment that does not exist yet", "segment 1", "segment 0"],
)
def test_read_history_stops_at_the_start_of_the_wanted_segment(
    make_runtime: Any, stubbed: Any, redrive_count: int | None, first_id: int
) -> None:
    _client, stubber = stubbed("stepfunctions")
    stubber.add_response("get_execution_history", {"events": NEWEST_FIRST}, HISTORY_PARAMS)
    segment = read_history(make_runtime(), EXECUTION, redrive_count=redrive_count)
    assert _ids(segment) == list(range(first_id, 13)), "oldest first, newer segments included"
    assert (segment.complete, segment.pages) == (True, 1)


def test_read_history_pages_with_next_token(make_runtime: Any, stubbed: Any) -> None:
    _client, stubber = stubbed("stepfunctions")
    stubber.add_response(
        "get_execution_history", {"events": NEWEST_FIRST[:6], "nextToken": "page-2"}, HISTORY_PARAMS
    )
    stubber.add_response(
        "get_execution_history", {"events": NEWEST_FIRST[6:]}, {**HISTORY_PARAMS, "nextToken": "page-2"}
    )
    segment = read_history(make_runtime(), EXECUTION, redrive_count=0)
    assert _ids(segment) == list(range(1, 13))
    assert (segment.complete, segment.pages) == (True, 2)


def test_read_history_does_not_read_past_the_segment(make_runtime: Any, stubbed: Any) -> None:
    _client, stubber = stubbed("stepfunctions")
    stubber.add_response(
        "get_execution_history", {"events": NEWEST_FIRST[:6], "nextToken": "page-2"}, HISTORY_PARAMS
    )
    segment = read_history(make_runtime(), EXECUTION)
    assert _ids(segment) == [9, 10, 11, 12] and segment.pages == 1


def test_a_history_without_a_start_event_read_to_its_end_is_complete(make_runtime: Any, stubbed: Any) -> None:
    _client, stubber = stubbed("stepfunctions")
    stubber.add_response("get_execution_history", {"events": NEWEST_FIRST[:3]}, HISTORY_PARAMS)
    segment = read_history(make_runtime(), EXECUTION)
    assert _ids(segment) == [10, 11, 12] and segment.complete is True


def _entered(count: int, first: int = 1000) -> list[dict[str, Any]]:
    return [_event(first - index, "PassStateEntered", first - index - 1) for index in range(count)]


def test_history_max_events_cuts_the_history(make_runtime: Any, stubbed: Any) -> None:
    _client, stubber = stubbed("stepfunctions")
    stubber.add_response(
        "get_execution_history", {"events": _entered(150), "nextToken": "more"}, HISTORY_PARAMS
    )
    segment = read_history(make_runtime(sfn_options(history_max_events=100)), EXECUTION)
    assert len(segment.events) == 100 and segment.complete is False and segment.pages == 1
    assert _ids(segment) == list(range(901, 1001)), "the newest events are kept"


def test_history_max_events_across_pages(make_runtime: Any, stubbed: Any) -> None:
    _client, stubber = stubbed("stepfunctions")
    stubber.add_response("get_execution_history", {"events": _entered(60), "nextToken": "p2"}, HISTORY_PARAMS)
    stubber.add_response(
        "get_execution_history",
        {"events": _entered(60, first=940), "nextToken": "p3"},
        {**HISTORY_PARAMS, "nextToken": "p2"},
    )
    segment = read_history(make_runtime(sfn_options(history_max_events=100)), EXECUTION)
    assert len(segment.events) == 100 and (segment.complete, segment.pages) == (False, 2)


class PagingClient:
    """A Step Functions client whose history pages are given as raw answers (no shape checks)."""

    def __init__(self, *pages: Mapping[str, Any], repeat_last: bool = False) -> None:
        self.pages = list(pages)
        self.repeat_last = repeat_last
        self.calls: list[dict[str, Any]] = []

    def get_execution_history(self, **params: Any) -> Mapping[str, Any]:
        self.calls.append(params)
        if self.repeat_last and len(self.pages) == 1:
            return self.pages[0]
        return self.pages.pop(0)


def test_a_history_that_keeps_handing_out_tokens_stops(
    make_runtime: Any, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setattr(executions, "MAX_HISTORY_PAGES", 3)
    client = PagingClient({"events": [], "nextToken": "again"}, repeat_last=True)
    segment = read_history(make_runtime(factory=lambda service: client), EXECUTION)
    assert (segment.events, segment.complete, segment.pages) == ((), False, 3)
    assert len(client.calls) == 3


@pytest.mark.parametrize(
    ("page", "ids"),
    [
        ({"events": [None, "x", 3, {"id": 1, "type": "ExecutionStarted"}]}, [1]),
        ({"events": "not a list"}, []),
        ({"events": None, "nextToken": ""}, []),
        ({}, []),
    ],
)
def test_odd_history_answers(make_runtime: Any, page: Mapping[str, Any], ids: list[int]) -> None:
    client = PagingClient(page)
    segment = read_history(make_runtime(factory=lambda service: client), EXECUTION)
    assert _ids(segment) == ids and segment.complete is True and segment.pages == 1


def test_history_errors_are_core_errors(make_runtime: Any, stubbed: Any) -> None:
    _client, stubber = stubbed("stepfunctions")
    stubber.add_client_error("get_execution_history", service_error_code="ExecutionDoesNotExist")
    stubber.add_client_error("get_execution_history", service_error_code="ThrottlingException")
    runtime = make_runtime()
    with pytest.raises(NotFound):
        read_history(runtime, EXECUTION)
    with pytest.raises(OrchestratorError) as raised:
        read_history(runtime, EXECUTION)
    assert raised.value.transient is True


# =========================================================================== describe_execution


DESCRIBED = {
    "executionArn": EXECUTION,
    "stateMachineArn": CUSTOMERS_SM,
    "name": "customers-2026-10-02",
    "status": "FAILED",
    "startDate": STARTED,
    "stopDate": STARTED + timedelta(seconds=41),
    "redriveCount": 0,
}


def test_describe_execution(make_runtime: Any, stubbed: Any) -> None:
    _client, stubber = stubbed("stepfunctions")
    stubber.add_response("describe_execution", DESCRIBED, {"executionArn": EXECUTION})
    assert describe_execution(make_runtime(), EXECUTION) == DESCRIBED


@pytest.mark.parametrize("code", ["KmsAccessDeniedException", "KmsInvalidStateException"])
def test_a_kms_denial_is_retried_with_metadata_only(make_runtime: Any, stubbed: Any, code: str) -> None:
    _client, stubber = stubbed("stepfunctions")
    stubber.add_client_error(
        "describe_execution", service_error_code=code, expected_params={"executionArn": EXECUTION}
    )
    stubber.add_response(
        "describe_execution", DESCRIBED, {"executionArn": EXECUTION, "includedData": "METADATA_ONLY"}
    )
    assert describe_execution(make_runtime(), EXECUTION)["status"] == "FAILED"


@pytest.mark.parametrize(
    ("code", "kind", "transient"),
    [
        ("AccessDeniedException", PermissionDenied, False),
        ("KmsThrottlingException", OrchestratorError, True),
        ("ExecutionDoesNotExist", NotFound, False),
    ],
)
def test_other_describe_errors_are_not_retried(
    make_runtime: Any, stubbed: Any, code: str, kind: type[OrchestratorError], transient: bool
) -> None:
    _client, stubber = stubbed("stepfunctions")
    stubber.add_client_error(
        "describe_execution", service_error_code=code, expected_params={"executionArn": EXECUTION}
    )
    with pytest.raises(kind) as raised:
        describe_execution(make_runtime(), EXECUTION)
    assert raised.value.transient is transient


def test_a_kms_denial_of_the_retry_propagates(make_runtime: Any, stubbed: Any) -> None:
    _client, stubber = stubbed("stepfunctions")
    for params in ({"executionArn": EXECUTION}, {"executionArn": EXECUTION, "includedData": "METADATA_ONLY"}):
        stubber.add_client_error(
            "describe_execution", service_error_code="KmsAccessDeniedException", expected_params=params
        )
    with pytest.raises(PermissionDenied):
        describe_execution(make_runtime(), EXECUTION)


# =========================================================================== thin wrappers


def test_list_executions(make_runtime: Any, stubbed: Any) -> None:
    item = {
        key: DESCRIBED[key]
        for key in ("executionArn", "stateMachineArn", "name", "status", "startDate", "stopDate")
    }
    _client, stubber = stubbed("stepfunctions")
    stubber.add_response(
        "list_executions",
        {"executions": [item], "nextToken": "t"},
        {"stateMachineArn": CUSTOMERS_SM, "statusFilter": "FAILED", "maxResults": 100},
    )
    stubber.add_response(
        "list_executions",
        {"executions": []},
        {"stateMachineArn": CUSTOMERS_SM, "statusFilter": "TIMED_OUT", "maxResults": 10, "nextToken": "t"},
    )
    runtime = make_runtime()
    assert list_executions(runtime, CUSTOMERS_SM, "FAILED")["nextToken"] == "t"
    assert list_executions(runtime, CUSTOMERS_SM, "TIMED_OUT", next_token="t", max_results=10) == {
        "executions": []
    }


def test_describe_state_machines(make_runtime: Any, stubbed: Any) -> None:
    definition = '{"StartAt": "A", "States": {"A": {"Type": "Succeed"}}}'
    common = {"name": "customers-sync", "definition": definition, "roleArn": f"arn:aws:iam::{ACCOUNT}:role/r"}
    _client, stubber = stubbed("stepfunctions")
    stubber.add_response(
        "describe_state_machine_for_execution",
        {"stateMachineArn": CUSTOMERS_SM, "updateDate": STARTED, **common},
        {"executionArn": EXECUTION},
    )
    stubber.add_response(
        "describe_state_machine",
        {"stateMachineArn": CUSTOMERS_SM, "type": "STANDARD", "creationDate": STARTED, **common},
        {"stateMachineArn": CUSTOMERS_SM},
    )
    runtime = make_runtime()
    assert describe_state_machine_for_execution(runtime, EXECUTION)["definition"] == definition
    assert describe_state_machine(runtime, CUSTOMERS_SM)["type"] == "STANDARD"
