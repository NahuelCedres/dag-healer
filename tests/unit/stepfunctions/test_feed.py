"""Tests of :mod:`dag_healer.stepfunctions.feed`: the poll, its cursor, caps, discovery and caches.

Every AWS call is answered by ``botocore.stub.Stubber`` with the exact request parameters, in
order (the feed lists FAILED then TIMED_OUT executions of each machine, then reads the histories
of that machine). Histories come from ``fixtures/stepfunctions/history`` (newest first, as
``reverseOrder=True`` returns them); listings from ``fixtures/stepfunctions/api/detection``. The
bursts of failures (more than one poll can read) run against an
:class:`~dag_healer.stepfunctions.testing.OfflineAws` world, which answers any call order.
"""

from __future__ import annotations

import json
import logging
from collections.abc import Callable
from datetime import datetime, timedelta
from typing import Any

import pytest

from dag_healer.core.errors import NotFound, OrchestratorError, PermissionDenied
from dag_healer.core.failures import AttemptState, FailurePage
from dag_healer.core.ids import RUN_LEVEL_STEP, AttemptRef
from dag_healer.orchestrators.conformance import assert_native_fits, assert_ref_fits
from dag_healer.stepfunctions import causes
from dag_healer.stepfunctions.feed import (
    MAX_UNREADABLE_POLLS,
    NOT_BEFORE_KEY,
    POLL_OVERLAP_MS,
    WATERMARK_KEY,
    ExecutionInfo,
    HistoryLocalizer,
    SfnFeed,
    cursor_time,
    execution_failure,
    read_cursor,
)
from dag_healer.stepfunctions.history import Localized
from dag_healer.stepfunctions.runtime import epoch_ms, iso, to_utc
from dag_healer.stepfunctions.testing import OfflineAws

from .conftest import (
    ACCOUNT,
    CUSTOMERS_SM,
    INSTANCE,
    INVENTORY_SM,
    NOW,
    REGION,
    SALES_SM,
    api_response,
    load_json,
    sfn_options,
)

SALES_RUN = "sales-2026-10-02"
#: The watermark of a poll that starts at NOW, and where a first poll's window starts.
WATERMARK = iso(NOW)
NOW_MS = epoch_ms(NOW) or 0
LOOKBACK_START_MS = NOW_MS - 24 * 3600 * 1000
CUSTOMERS_RUN = "customers-2026-10-02"
INVENTORY_RUN = "inventory-2026-10-02"
PII = "alice.pii@example.com"
HISTORIES = {
    SALES_SM: "sales_keyerror_after_retries.json",
    CUSTOMERS_SM: "customers_valueerror.json",
    INVENTORY_SM: "inventory_legacy_lambda.json",
}
LISTINGS = {
    SALES_SM: "list_executions_sales.json",
    CUSTOMERS_SM: "list_executions_customers.json",
    INVENTORY_SM: "list_executions_inventory.json",
}


def watch_all(_pipeline_id: str) -> bool:
    return True


def execution_arn(machine: str, name: str) -> str:
    return f"{machine.replace(':stateMachine:', ':execution:')}:{name}"


def history_events(name: str) -> list[dict[str, Any]]:
    return load_json(f"history/{name}")["events"]


def listing(machine: str) -> dict[str, Any]:
    return load_json(f"api/detection/{LISTINGS[machine]}")


def stop_ms(machine: str) -> int:
    value = epoch_ms(listing(machine)["executions"][0]["stopDate"])
    assert value is not None
    return value


class Aws:
    """Queues Step Functions answers on the shared Stubber with their exact request parameters."""

    def __init__(self, stubber: Any) -> None:
        self.stubber = stubber

    def list(self, machine: str, status: str, answer: dict[str, Any], *, token: str | None = None) -> None:
        params: dict[str, Any] = {"stateMachineArn": machine, "statusFilter": status, "maxResults": 100}
        if token:
            params["nextToken"] = token
        self.stubber.add_response("list_executions", api_response(answer), params)

    def no_executions(self, machine: str, status: str) -> None:
        self.list(machine, status, {"executions": []})

    def history(self, execution: str, events: list[dict[str, Any]]) -> None:
        params = {
            "executionArn": execution,
            "reverseOrder": True,
            "maxResults": 1000,
            "includeExecutionData": False,
        }
        self.stubber.add_response(
            "get_execution_history", api_response({"events": list(reversed(events))}), params
        )

    def history_error(self, execution: str, code: str, status: int = 400) -> None:
        params = {
            "executionArn": execution,
            "reverseOrder": True,
            "maxResults": 1000,
            "includeExecutionData": False,
        }
        self.stubber.add_client_error(
            "get_execution_history", service_error_code=code, http_status_code=status, expected_params=params
        )

    def list_error(self, machine: str, status: str, code: str, http: int = 400) -> None:
        params = {"stateMachineArn": machine, "statusFilter": status, "maxResults": 100}
        self.stubber.add_client_error(
            "list_executions", service_error_code=code, http_status_code=http, expected_params=params
        )

    def machine_day2(self, machine: str, *, read: bool = True) -> None:
        """The day-2 failure of ``machine`` (FAILED), no TIMED_OUT execution, and its history."""
        self.list(machine, "FAILED", listing(machine))
        self.no_executions(machine, "TIMED_OUT")
        if read:
            name = listing(machine)["executions"][0]["name"]
            self.history(execution_arn(machine, name), history_events(HISTORIES[machine]))


@pytest.fixture
def aws(stubbed: Callable[[str], tuple[Any, Any]]) -> Aws:
    return Aws(stubbed("stepfunctions")[1])


@pytest.fixture
def feed_of(make_runtime: Callable[..., Any]) -> Callable[..., SfnFeed]:
    def _make(**overrides: Any) -> SfnFeed:
        return SfnFeed(make_runtime(sfn_options(**overrides)))

    return _make


def by_pipeline(page: FailurePage) -> dict[str, Any]:
    return {failure.ref.pipeline_id: failure for failure in page.failures}


def positions(cursor: Any) -> dict[str, Any]:
    """The machine positions of a cursor (without the core's watermark and floor)."""
    return {key: value for key, value in cursor.items() if key not in (WATERMARK_KEY, NOT_BEFORE_KEY)}


# =========================================================================== the first poll


def test_first_poll_localises_the_three_day2_failures(aws: Aws, feed_of: Callable[..., SfnFeed]) -> None:
    for machine in (SALES_SM, CUSTOMERS_SM, INVENTORY_SM):
        aws.machine_day2(machine)
    page = feed_of().poll(cursor=None, is_monitored=watch_all)

    assert page.complete is True
    assert page.cursor == {
        SALES_SM: stop_ms(SALES_SM),
        CUSTOMERS_SM: stop_ms(CUSTOMERS_SM),
        INVENTORY_SM: stop_ms(INVENTORY_SM),
        WATERMARK_KEY: WATERMARK,
    }
    failures = by_pipeline(page)
    assert [failure.ref for failure in page.failures] == [
        AttemptRef(INSTANCE, "sales_daily_etl", SALES_RUN, "TransformSales", -1, 1),
        AttemptRef(INSTANCE, "customers_sync", CUSTOMERS_RUN, "TransformCustomers", -1, 1),
        AttemptRef(INSTANCE, "inventory_report", INVENTORY_RUN, "BuildReport", 0, 1),
    ]
    for failure in page.failures:
        assert (failure.state, failure.is_final, failure.source) == (AttemptState.FAILED, True, "poll")
        assert_ref_fits(failure.ref, INSTANCE)
        assert_native_fits(failure.native, "failure.native")
        assert failure.failed_at == to_utc(
            listing(failure.native["state_machine_arn"])["executions"][0]["stopDate"]
        )
    assert json.loads(json.dumps(page.cursor)) == page.cursor

    sales = failures["sales_daily_etl"]
    sales_cause = history_events(HISTORIES[SALES_SM])[-1]["executionFailedEventDetails"]["cause"]
    assert sales.error_name == "KeyError"
    assert sales.error_summary == causes.summarize("KeyError", sales_cause)
    assert sales.native == {
        "execution_arn": execution_arn(SALES_SM, SALES_RUN),
        "state_machine_arn": SALES_SM,
        "start_date": "2026-10-02T06:00:00.000+00:00",
        "stop_date": "2026-10-02T06:00:13.590+00:00",
        "status": "FAILED",
        "redrive_count": 0,
        "error": "KeyError",
        "failed_event_id": 16,
        "entered_event_id": 7,
        "event_type": "TaskFailed",
        "resource": "invoke",
        "resource_type": "lambda",
        "function_name": f"arn:aws:lambda:{REGION}:{ACCOUNT}:function:sfn-etl-SalesFunction-1A2B3C4D5E6F",
        "retry_attempts": 2,
        "localized": True,
    }
    inventory = failures["inventory_report"]
    assert inventory.native["step_path"] == "PerWarehouse[0]/BuildReport"
    assert inventory.native["event_type"] == "LambdaFunctionFailed"
    assert failures["customers_sync"].error_name == "ValueError"


def test_poll_never_carries_payloads(
    aws: Aws, feed_of: Callable[..., SfnFeed], caplog: pytest.LogCaptureFixture
) -> None:
    caplog.set_level(logging.DEBUG)
    aws.machine_day2(CUSTOMERS_SM)
    page = feed_of(pipelines={"customers_sync": {"state_machine": CUSTOMERS_SM}}).poll(
        cursor=None, is_monitored=watch_all
    )
    (failure,) = page.failures
    assert PII not in json.dumps(dict(failure.native))
    assert PII not in failure.error_summary
    assert not [record for record in caplog.records if PII in record.getMessage()]


def test_unmonitored_pipelines_are_not_listed(aws: Aws, feed_of: Callable[..., SfnFeed]) -> None:
    aws.machine_day2(SALES_SM)
    aws.machine_day2(INVENTORY_SM)
    page = feed_of().poll(cursor=None, is_monitored=lambda pipeline_id: pipeline_id != "customers_sync")
    assert [failure.ref.pipeline_id for failure in page.failures] == ["sales_daily_etl", "inventory_report"]
    assert set(page.cursor) == {SALES_SM, INVENTORY_SM, WATERMARK_KEY}


def test_nothing_to_report(aws: Aws, feed_of: Callable[..., SfnFeed]) -> None:
    for machine in (SALES_SM, CUSTOMERS_SM, INVENTORY_SM):
        aws.no_executions(machine, "FAILED")
        aws.no_executions(machine, "TIMED_OUT")
    page = feed_of().poll(cursor=None, is_monitored=watch_all)
    assert page == FailurePage(failures=(), cursor={WATERMARK_KEY: WATERMARK}, complete=True)


# =========================================================================== the window and the cursor


def test_the_window_starts_lookback_hours_ago(aws: Aws, feed_of: Callable[..., SfnFeed]) -> None:
    """Paging stops at the first execution older than the window; it is not reported."""
    two_days = load_json("api/detection/list_executions_sales_two_days.json")
    aws.list(SALES_SM, "FAILED", {**two_days, "nextToken": "page-2"})
    aws.no_executions(SALES_SM, "TIMED_OUT")
    rerun = execution_arn(SALES_SM, "sales-2026-10-01-rerun")
    aws.history(rerun, history_events("sales_keyerror_after_retries.json"))  # oldest in the window first
    aws.history(execution_arn(SALES_SM, SALES_RUN), history_events("sales_keyerror_after_retries.json"))
    feed = feed_of(pipelines={"sales_daily_etl": {"state_machine": SALES_SM}})
    page = feed.poll(cursor=None, is_monitored=watch_all)
    assert [failure.ref.run_id for failure in page.failures] == ["sales-2026-10-01-rerun", SALES_RUN]
    assert positions(page.cursor) == {SALES_SM: stop_ms(SALES_SM)}


def test_lookback_hours_shorten_the_first_window(aws: Aws, feed_of: Callable[..., SfnFeed]) -> None:
    aws.list(SALES_SM, "FAILED", load_json("api/detection/list_executions_sales_two_days.json"))
    aws.no_executions(SALES_SM, "TIMED_OUT")
    aws.history(execution_arn(SALES_SM, SALES_RUN), history_events("sales_keyerror_after_retries.json"))
    feed = feed_of(pipelines={"sales_daily_etl": {"state_machine": SALES_SM}}, lookback_hours=2)
    page = feed.poll(cursor=None, is_monitored=watch_all)
    assert [failure.ref.run_id for failure in page.failures] == [SALES_RUN]


def test_paging_follows_next_tokens_while_the_window_lasts(aws: Aws, feed_of: Callable[..., SfnFeed]) -> None:
    first, second = (listing(SALES_SM)["executions"][0] for _ in range(2))
    second = {**second, "name": "sales-2026-10-02-early", "stopDate": "2026-10-02T05:10:00.000Z"}
    second["executionArn"] = execution_arn(SALES_SM, second["name"])
    second["startDate"] = "2026-10-02T05:09:00.000Z"
    aws.list(SALES_SM, "FAILED", {"executions": [first], "nextToken": "token-1"})
    aws.list(SALES_SM, "FAILED", {"executions": [second], "nextToken": "token-2"}, token="token-1")
    aws.list(SALES_SM, "FAILED", {"executions": []}, token="token-2")
    aws.no_executions(SALES_SM, "TIMED_OUT")
    aws.history(second["executionArn"], history_events("sales_keyerror_after_retries.json"))
    aws.history(first["executionArn"], history_events("sales_keyerror_after_retries.json"))
    feed = feed_of(pipelines={"sales_daily_etl": {"state_machine": SALES_SM}})
    page = feed.poll(cursor=None, is_monitored=watch_all)
    assert [failure.ref.run_id for failure in page.failures] == ["sales-2026-10-02-early", SALES_RUN]


def test_overlapping_polls_reuse_the_localisations(aws: Aws, feed_of: Callable[..., SfnFeed]) -> None:
    """The first page's cursor coming back means the core stored its failures: the overlap lists
    them again but they are not returned. A cursor the feed did not write (here: none) gets them
    again, from memory."""
    feed = feed_of()
    for machine in (SALES_SM, CUSTOMERS_SM, INVENTORY_SM):
        aws.machine_day2(machine)
    first = feed.poll(cursor=None, is_monitored=watch_all)
    for machine in (SALES_SM, CUSTOMERS_SM, INVENTORY_SM):
        aws.machine_day2(machine, read=False)  # listed again: no history read (Stubber would fail)
    again = feed.poll(cursor=first.cursor, is_monitored=watch_all)
    assert again.failures == ()
    assert again.cursor == first.cursor and again.complete is True
    for machine in LISTINGS:
        info = ExecutionInfo.from_api(api_response(listing(machine)["executions"][0]))
        assert info is not None and feed.committed(info)

    for machine in (SALES_SM, CUSTOMERS_SM, INVENTORY_SM):
        aws.machine_day2(machine, read=False)
    fresh = feed.poll(cursor=None, is_monitored=watch_all)
    assert fresh.failures == first.failures


def test_the_cursor_window_overlaps_by_five_minutes(aws: Aws, feed_of: Callable[..., SfnFeed]) -> None:
    """With a cursor the window starts 5 minutes before it: a later failure is read, an earlier
    one (before the overlap) ends the paging and is not reported."""
    mark = stop_ms(SALES_SM)
    late = dict(listing(SALES_SM)["executions"][0])
    late.update(
        name="sales-2026-10-02-late",
        stopDate="2026-10-02T06:40:00.000Z",
        startDate="2026-10-02T06:39:00.000Z",
    )
    late["executionArn"] = execution_arn(SALES_SM, late["name"])
    inside = dict(listing(SALES_SM)["executions"][0])  # stopped 3 minutes before the mark: in the overlap
    inside.update(name="sales-2026-10-02-inside", stopDate="2026-10-02T05:57:13.590Z")
    inside["executionArn"] = execution_arn(SALES_SM, inside["name"])
    before = dict(inside)
    before.update(name="sales-2026-10-02-before", stopDate="2026-10-02T05:50:00.000Z")
    before["executionArn"] = execution_arn(SALES_SM, before["name"])
    aws.list(
        SALES_SM,
        "FAILED",
        {"executions": [late, listing(SALES_SM)["executions"][0], inside, before], "nextToken": "more"},
    )
    aws.no_executions(SALES_SM, "TIMED_OUT")
    for item in (inside, listing(SALES_SM)["executions"][0], late):
        aws.history(item["executionArn"], history_events("sales_keyerror_after_retries.json"))
    feed = feed_of(pipelines={"sales_daily_etl": {"state_machine": SALES_SM}})
    page = feed.poll(cursor={SALES_SM: mark}, is_monitored=watch_all)
    assert [failure.ref.run_id for failure in page.failures] == [
        "sales-2026-10-02-inside",
        SALES_RUN,
        "sales-2026-10-02-late",
    ]
    assert page.cursor == {SALES_SM: epoch_ms("2026-10-02T06:40:00.000Z"), WATERMARK_KEY: WATERMARK}
    assert POLL_OVERLAP_MS == 5 * 60 * 1000


def test_the_cursor_never_moves_back(aws: Aws, feed_of: Callable[..., SfnFeed]) -> None:
    ahead = stop_ms(SALES_SM) + 60_000
    aws.machine_day2(SALES_SM)
    page = feed_of(pipelines={"sales_daily_etl": {"state_machine": SALES_SM}}).poll(
        cursor={SALES_SM: ahead}, is_monitored=watch_all
    )
    assert positions(page.cursor) == {SALES_SM: ahead}
    assert [failure.ref.run_id for failure in page.failures] == [SALES_RUN]  # inside the overlap


def test_cursor_entries_of_other_machines_are_kept(aws: Aws, feed_of: Callable[..., SfnFeed]) -> None:
    gone = f"arn:aws:states:{REGION}:{ACCOUNT}:stateMachine:retired"
    aws.machine_day2(SALES_SM)
    page = feed_of(pipelines={"sales_daily_etl": {"state_machine": SALES_SM}}).poll(
        cursor={gone: 1234}, is_monitored=watch_all
    )
    assert positions(page.cursor) == {gone: 1234, SALES_SM: stop_ms(SALES_SM)}


@pytest.mark.parametrize(
    ("cursor", "entries"),
    [
        (None, {}),
        ("not a mapping", {}),
        ({SALES_SM: 5}, {SALES_SM: 5}),
        ({SALES_SM: "17"}, {SALES_SM: 17}),
        ({SALES_SM: 17.0}, {SALES_SM: 17}),
        ({SALES_SM: True}, {}),
        ({SALES_SM: -1}, {}),
        ({SALES_SM: "soon"}, {}),
        ({"sales_daily_etl": 5}, {}),
        ({7: 5}, {}),
        ({SALES_SM: 5, WATERMARK_KEY: WATERMARK, NOT_BEFORE_KEY: None}, {SALES_SM: 5}),
    ],
)
def test_read_cursor(cursor: Any, entries: dict[str, int]) -> None:
    assert read_cursor(cursor) == entries


def test_invalid_cursor_entries_fall_back_to_the_lookback(aws: Aws, feed_of: Callable[..., SfnFeed]) -> None:
    aws.machine_day2(SALES_SM)
    page = feed_of(pipelines={"sales_daily_etl": {"state_machine": SALES_SM}}).poll(
        cursor={SALES_SM: "garbage"}, is_monitored=watch_all
    )
    assert [failure.ref.run_id for failure in page.failures] == [SALES_RUN]


# =========================================================================== the core's cursor keys

#: What ``dag-healer store reset`` writes: the poll restarts at that moment (here: NOW).
RESET_CURSOR = {WATERMARK_KEY: NOW.isoformat(), NOT_BEFORE_KEY: NOW.isoformat()}


def _sales_feed(feed_of: Callable[..., SfnFeed], **overrides: Any) -> SfnFeed:
    return feed_of(pipelines={"sales_daily_etl": {"state_machine": SALES_SM}}, **overrides)


def test_a_reset_cursor_lists_nothing_from_before_the_reset(
    aws: Aws, feed_of: Callable[..., SfnFeed]
) -> None:
    """The day-2 failures stopped before the reset: they are listed by AWS but never returned, and
    the floor is kept for the next polls."""
    for machine in (SALES_SM, CUSTOMERS_SM, INVENTORY_SM):
        aws.machine_day2(machine, read=False)
    page = feed_of().poll(cursor=dict(RESET_CURSOR), is_monitored=watch_all)
    assert page.failures == () and page.complete is True
    assert page.cursor == {WATERMARK_KEY: WATERMARK, NOT_BEFORE_KEY: RESET_CURSOR[NOT_BEFORE_KEY]}


def test_the_floor_survives_polls_that_list_nothing(aws: Aws, feed_of: Callable[..., SfnFeed]) -> None:
    floor = "2026-10-01T22:00:00Z"  # before the day-2 failure: that one is listed
    feed = _sales_feed(feed_of)
    aws.machine_day2(SALES_SM)
    first = feed.poll(cursor={NOT_BEFORE_KEY: floor}, is_monitored=watch_all)
    assert [failure.ref.run_id for failure in first.failures] == [SALES_RUN]
    assert first.cursor == {SALES_SM: stop_ms(SALES_SM), WATERMARK_KEY: WATERMARK, NOT_BEFORE_KEY: floor}
    quiet = feed.poll(cursor={NOT_BEFORE_KEY: floor}, is_monitored=lambda _pipeline: False)
    assert quiet.cursor == {WATERMARK_KEY: WATERMARK, NOT_BEFORE_KEY: floor}  # nothing listed at all
    aws.no_executions(SALES_SM, "FAILED")
    aws.no_executions(SALES_SM, "TIMED_OUT")
    empty = feed.poll(cursor=quiet.cursor, is_monitored=watch_all)
    assert empty.failures == ()
    assert empty.cursor == {WATERMARK_KEY: WATERMARK, NOT_BEFORE_KEY: floor}


def test_the_floor_wins_over_an_older_machine_entry(aws: Aws, feed_of: Callable[..., SfnFeed]) -> None:
    mark = stop_ms(SALES_SM) - 60_000  # the failure is inside the entry's overlap ...
    floor = iso(stop_ms(SALES_SM) + 1)  # ... but stopped before the floor
    aws.machine_day2(SALES_SM, read=False)
    page = _sales_feed(feed_of).poll(cursor={SALES_SM: mark, NOT_BEFORE_KEY: floor}, is_monitored=watch_all)
    assert page.failures == ()
    assert page.cursor == {SALES_SM: mark, WATERMARK_KEY: WATERMARK, NOT_BEFORE_KEY: floor}


@pytest.mark.parametrize(("minutes", "listed"), [(4, True), (6, False)])
def test_a_machine_without_an_entry_starts_five_minutes_before_the_watermark(
    aws: Aws, feed_of: Callable[..., SfnFeed], minutes: int, listed: bool
) -> None:
    watermark = iso(stop_ms(SALES_SM) + minutes * 60_000)
    aws.machine_day2(SALES_SM, read=listed)
    page = _sales_feed(feed_of).poll(cursor={WATERMARK_KEY: watermark}, is_monitored=watch_all)
    assert [failure.ref.run_id for failure in page.failures] == ([SALES_RUN] if listed else [])
    assert page.cursor[WATERMARK_KEY] == WATERMARK  # the start of this poll


def test_now_drives_the_window_and_the_watermark(aws: Aws, make_runtime: Callable[..., Any]) -> None:
    later = NOW + timedelta(days=2)  # the day-2 failure is outside a 24 h lookback from here
    feed = SfnFeed(make_runtime(sfn_options(pipelines={"sales_daily_etl": {"state_machine": SALES_SM}})))
    aws.machine_day2(SALES_SM, read=False)
    page = feed.poll(cursor=None, is_monitored=watch_all, now=later)
    assert page.failures == () and page.cursor == {WATERMARK_KEY: iso(later)}

    shifted = SfnFeed(
        make_runtime(sfn_options(pipelines={"sales_daily_etl": {"state_machine": SALES_SM}}), now=later)
    )
    aws.machine_day2(SALES_SM)
    (failure,) = shifted.poll(cursor=None, is_monitored=watch_all, now=NOW).failures  # now wins
    assert failure.ref.run_id == SALES_RUN


def test_an_invalid_watermark_or_floor_is_ignored_and_kept(
    aws: Aws, feed_of: Callable[..., SfnFeed], caplog: pytest.LogCaptureFixture
) -> None:
    caplog.set_level(logging.DEBUG, logger="dag_healer.stepfunctions.feed")
    feed = _sales_feed(feed_of)
    cursor: dict[str, Any] = {WATERMARK_KEY: "soon", NOT_BEFORE_KEY: 12}
    for poll in range(2):
        aws.machine_day2(SALES_SM, read=poll == 0)  # then remembered: the cursor is not the feed's
        page = feed.poll(cursor=cursor, is_monitored=watch_all)  # the 24 h lookback
        assert [failure.ref.run_id for failure in page.failures] == [SALES_RUN]
        assert page.cursor[NOT_BEFORE_KEY] == 12 and page.cursor[WATERMARK_KEY] == WATERMARK
    invalid = [(r.levelno, r.getMessage()) for r in caplog.records if "invalid" in r.getMessage()]
    warning, debug = logging.WARNING, logging.DEBUG
    assert [level for level, _message in invalid] == [warning, warning, debug, debug]  # once per key
    assert all("soon" not in message for _level, message in invalid)
    assert cursor_time({NOT_BEFORE_KEY: "2026-10-02T07:00:00Z"}, NOT_BEFORE_KEY) == NOW
    assert cursor_time({NOT_BEFORE_KEY: 12}, NOT_BEFORE_KEY) is None and cursor_time(None, "x") is None


# =========================================================================== stored pages


def _late_listed() -> dict[str, Any]:
    """A sales failure that stopped a minute before the day-2 one but is listed only later
    (``ListExecutions`` is eventually consistent): it falls inside the overlap."""
    item = dict(listing(SALES_SM)["executions"][0])
    item.update(
        name="sales-2026-10-02-late-listed",
        startDate="2026-10-02T05:59:00.000Z",
        stopDate="2026-10-02T05:59:13.590Z",
    )
    item["executionArn"] = execution_arn(SALES_SM, item["name"])
    return item


def test_a_page_the_core_did_not_store_is_returned_again(aws: Aws, feed_of: Callable[..., SfnFeed]) -> None:
    """Only the cursor coming back tells the feed a page was stored: a newcomer of the overlap
    whose page the core lost is returned again (from memory) until a page with it comes back."""
    feed = _sales_feed(feed_of)
    day2 = listing(SALES_SM)["executions"][0]
    late = _late_listed()
    moments = [NOW + timedelta(minutes=minute) for minute in range(4)]

    aws.list(SALES_SM, "FAILED", {"executions": [day2]})
    aws.no_executions(SALES_SM, "TIMED_OUT")
    aws.history(day2["executionArn"], history_events(HISTORIES[SALES_SM]))
    first = feed.poll(cursor=None, is_monitored=watch_all, now=moments[0])
    assert [failure.ref.run_id for failure in first.failures] == [SALES_RUN]

    def listed_with_the_newcomer() -> None:
        aws.list(SALES_SM, "FAILED", {"executions": [day2, late]})
        aws.no_executions(SALES_SM, "TIMED_OUT")

    listed_with_the_newcomer()
    aws.history(late["executionArn"], history_events(HISTORIES[SALES_SM]))
    second = feed.poll(cursor=first.cursor, is_monitored=watch_all, now=moments[1])  # first was stored
    assert [failure.ref.run_id for failure in second.failures] == [late["name"]]
    assert positions(second.cursor) == positions(first.cursor)  # the newcomer is older than the mark

    listed_with_the_newcomer()  # the core lost the second page: it passes the first cursor again
    third = feed.poll(cursor=first.cursor, is_monitored=watch_all, now=moments[2])
    assert [failure.ref.run_id for failure in third.failures] == [late["name"]]  # no history read

    listed_with_the_newcomer()
    fourth = feed.poll(cursor=third.cursor, is_monitored=watch_all, now=moments[3])
    assert fourth.failures == ()


def test_a_reused_execution_name_is_localised_again(aws: Aws, feed_of: Callable[..., SfnFeed]) -> None:
    """Same ARN and redrive count, another start date: another execution (its history is read)."""
    feed = _sales_feed(feed_of)
    aws.machine_day2(SALES_SM)
    first = feed.poll(cursor=None, is_monitored=watch_all)
    reused = {**listing(SALES_SM)["executions"][0], "startDate": "2026-10-02T06:00:00.001Z"}
    aws.list(SALES_SM, "FAILED", {"executions": [reused]})
    aws.no_executions(SALES_SM, "TIMED_OUT")
    aws.history(reused["executionArn"], history_events(HISTORIES[SALES_SM]))  # read again
    (failure,) = feed.poll(cursor=first.cursor, is_monitored=watch_all).failures
    assert failure.native["start_date"] == "2026-10-02T06:00:00.001+00:00"


# =========================================================================== bursts (offline world)

BURST_MACHINE = "burst-etl"
QUIET_MACHINE = "quiet-etl"
#: A one-state definition (the executions' minimal histories fail outside any state: run level).
ONE_STATE = {"StartAt": "Work", "States": {"Work": {"Type": "Pass", "End": True}}}


def _world_with_burst(count: int, *, every_ms: int = 300, start: datetime | None = None) -> OfflineAws:
    """``count`` FAILED executions of :data:`BURST_MACHINE` stopping ``every_ms`` apart."""
    world = OfflineAws(now=NOW)
    world.add_state_machine(BURST_MACHINE, ONE_STATE)
    world.add_state_machine(QUIET_MACHINE, ONE_STATE)
    first = start or NOW - timedelta(minutes=30)
    for number in range(count):
        stop = first + timedelta(milliseconds=every_ms * number)
        world.add_execution(
            BURST_MACHINE,
            f"burst-{number:04d}",
            status="FAILED",
            start_date=stop - timedelta(seconds=5),
            stop_date=stop,
            error="States.TaskFailed",
            cause="boom",
        )
    return world


def _burst_feed(make_runtime: Callable[..., Any], world: OfflineAws, *machines: str) -> SfnFeed:
    pipelines = {name: {"state_machine": world.state_machine_arn(name)} for name in machines}
    options = sfn_options(pipelines=pipelines, statuses=["FAILED"], template=None)
    return SfnFeed(make_runtime(options, factory=world.client_factory))


def _drain(feed: SfnFeed, cursor: Any, *, polls: int) -> tuple[list[FailurePage], Any]:
    """Poll ``polls`` times, passing each page's cursor back (the core stored every page)."""
    pages = []
    for _poll in range(polls):
        page = feed.poll(cursor=cursor, is_monitored=watch_all)
        pages.append(page)
        cursor = page.cursor
    return pages, cursor


def test_a_burst_inside_the_overlap_never_holds_the_cursor(make_runtime: Callable[..., Any]) -> None:
    """260 failures within 78 s: 50 histories per poll until every one was returned exactly once;
    the failures stored already neither come back nor count against the caps."""
    world = _world_with_burst(260)
    feed = _burst_feed(make_runtime, world, BURST_MACHINE)
    pages, cursor = _drain(feed, None, polls=7)
    assert [len(page.failures) for page in pages] == [50, 50, 50, 50, 50, 10, 0]
    assert [page.complete for page in pages] == [False] * 5 + [True, True]
    returned = [failure.ref.run_id for page in pages for failure in page.failures]
    assert sorted(returned) == [f"burst-{number:04d}" for number in range(260)]
    assert len(world.calls_to("stepfunctions", "GetExecutionHistory")) == 260  # each history read once
    newest = epoch_ms(NOW - timedelta(minutes=30) + timedelta(milliseconds=300 * 259))
    assert positions(cursor) == {world.state_machine_arn(BURST_MACHINE): newest}


def test_a_remembered_burst_is_free_even_when_no_page_is_stored(make_runtime: Callable[..., Any]) -> None:
    """A core that never stores a page (it passes no cursor) still sees the later failures: the
    remembered ones come back without a history read and outside the caps."""
    world = _world_with_burst(120)
    feed = _burst_feed(make_runtime, world, BURST_MACHINE)
    sizes = [len(feed.poll(cursor=None, is_monitored=watch_all, limit=50).failures) for _poll in range(4)]
    assert sizes == [50, 100, 120, 120]
    assert len(world.calls_to("stepfunctions", "GetExecutionHistory")) == 120


def test_a_burst_on_one_machine_does_not_starve_another(make_runtime: Callable[..., Any]) -> None:
    """The first machine had 200 failures half an hour ago; the second fails later: it is
    returned by the next poll, whether the burst was drained already or not."""
    world = _world_with_burst(200)
    feed = _burst_feed(make_runtime, world, BURST_MACHINE, QUIET_MACHINE)
    pages, cursor = _drain(feed, None, polls=1)
    assert pages[0].complete is False  # the burst used the whole history budget
    late = world.add_execution(
        QUIET_MACHINE,
        "quiet-0001",
        status="FAILED",
        start_date=NOW - timedelta(minutes=2),
        stop_date=NOW - timedelta(minutes=1),
        error="States.TaskFailed",
        cause="boom",
    )
    following = feed.poll(cursor=cursor, is_monitored=watch_all)  # the quiet machine goes first now
    assert following.failures[0].ref.run_id == "quiet-0001"
    assert following.failures[0].native["execution_arn"] == late
    pages, cursor = _drain(feed, following.cursor, polls=4)
    assert pages[-1].complete is True and pages[-1].failures == ()

    world.add_execution(
        QUIET_MACHINE,
        "quiet-0002",
        status="FAILED",
        start_date=NOW - timedelta(seconds=40),
        stop_date=NOW - timedelta(seconds=30),
        error="States.TaskFailed",
        cause="boom",
    )
    (failure,) = feed.poll(cursor=cursor, is_monitored=watch_all, limit=1).failures  # the burst is stored
    assert failure.ref.run_id == "quiet-0002"


# =========================================================================== caps


def test_the_history_cap_leaves_the_rest_for_the_next_poll(aws: Aws, feed_of: Callable[..., SfnFeed]) -> None:
    """One history per poll: each poll serves one machine and the next one starts with the machine
    after it (the others wait); a machine the poll did not reach keeps the window it would have
    listed, and a failure a committed page returned is not returned again."""
    feed = feed_of(max_histories_per_poll=1)
    waiting = LOOKBACK_START_MS + POLL_OVERLAP_MS  # the first window, kept for the next poll
    aws.machine_day2(SALES_SM)
    first = feed.poll(cursor=None, is_monitored=watch_all)
    assert first.complete is False
    assert [failure.ref.pipeline_id for failure in first.failures] == ["sales_daily_etl"]
    assert first.cursor == {
        SALES_SM: stop_ms(SALES_SM),
        CUSTOMERS_SM: waiting,
        INVENTORY_SM: waiting,
        WATERMARK_KEY: WATERMARK,
    }

    aws.machine_day2(CUSTOMERS_SM)  # customers first: sales was served last time
    second = feed.poll(cursor=first.cursor, is_monitored=watch_all)
    assert second.complete is False
    assert [failure.ref.pipeline_id for failure in second.failures] == ["customers_sync"]
    assert positions(second.cursor) == {
        SALES_SM: stop_ms(SALES_SM),
        CUSTOMERS_SM: stop_ms(CUSTOMERS_SM),
        INVENTORY_SM: waiting,
    }

    aws.machine_day2(INVENTORY_SM)
    third = feed.poll(cursor=second.cursor, is_monitored=watch_all)
    assert third.complete is False  # sales and customers were not reached
    assert [failure.ref.pipeline_id for failure in third.failures] == ["inventory_report"]

    for machine in (SALES_SM, CUSTOMERS_SM, INVENTORY_SM):  # sales first again; no history read
        aws.machine_day2(machine, read=False)
    fourth = feed.poll(cursor=third.cursor, is_monitored=watch_all)
    assert (fourth.failures, fourth.complete) == ((), True)
    assert positions(fourth.cursor) == {machine: stop_ms(machine) for machine in LISTINGS}


def test_the_cap_inside_one_machine_holds_its_cursor(aws: Aws, feed_of: Callable[..., SfnFeed]) -> None:
    two_days = load_json("api/detection/list_executions_sales_two_days.json")
    rerun = two_days["executions"][1]
    feed = feed_of(pipelines={"sales_daily_etl": {"state_machine": SALES_SM}}, max_histories_per_poll=1)
    aws.list(SALES_SM, "FAILED", two_days)
    aws.no_executions(SALES_SM, "TIMED_OUT")
    aws.history(rerun["executionArn"], history_events("sales_keyerror_after_retries.json"))
    first = feed.poll(cursor=None, is_monitored=watch_all)
    assert (first.complete, [f.ref.run_id for f in first.failures]) == (False, ["sales-2026-10-01-rerun"])
    assert first.cursor == {SALES_SM: epoch_ms(rerun["stopDate"]), WATERMARK_KEY: WATERMARK}

    aws.list(SALES_SM, "FAILED", two_days)
    aws.no_executions(SALES_SM, "TIMED_OUT")
    aws.history(execution_arn(SALES_SM, SALES_RUN), history_events("sales_keyerror_after_retries.json"))
    second = feed.poll(cursor=first.cursor, is_monitored=watch_all)
    assert second.complete is True
    assert [failure.ref.run_id for failure in second.failures] == [SALES_RUN]  # the rerun is stored
    assert second.cursor == {SALES_SM: stop_ms(SALES_SM), WATERMARK_KEY: WATERMARK}


def test_the_limit_caps_the_page(aws: Aws, feed_of: Callable[..., SfnFeed]) -> None:
    aws.machine_day2(SALES_SM)
    page = feed_of().poll(cursor=None, is_monitored=watch_all, limit=1)
    assert page.complete is False
    assert [failure.ref.pipeline_id for failure in page.failures] == ["sales_daily_etl"]
    waiting = LOOKBACK_START_MS + POLL_OVERLAP_MS
    assert positions(page.cursor) == {
        SALES_SM: stop_ms(SALES_SM),
        CUSTOMERS_SM: waiting,
        INVENTORY_SM: waiting,
    }


# =========================================================================== discovery and name resolution


def test_discovery_watches_the_standard_machines(aws: Aws, feed_of: Callable[..., SfnFeed]) -> None:
    aws.stubber.add_response(
        "list_state_machines",
        api_response(load_json("api/detection/list_state_machines.json")),
        {"maxResults": 1000},
    )
    aws.machine_day2(CUSTOMERS_SM)
    aws.no_executions(INVENTORY_SM, "FAILED")
    aws.no_executions(INVENTORY_SM, "TIMED_OUT")
    aws.no_executions(SALES_SM, "FAILED")
    aws.no_executions(SALES_SM, "TIMED_OUT")
    feed = feed_of(pipelines={}, discover=True)
    page = feed.poll(cursor=None, is_monitored=watch_all)
    (failure,) = page.failures
    assert failure.ref.pipeline_id == "customers-sync"
    assert [entry.pipeline_id for entry in feed.rt.pipelines.entries()] == [
        "customers-sync",
        "InventoryReportStateMachine-XyZ789",
        "SalesDailyEtlStateMachine-AbC123",
    ]  # the EXPRESS machine is not watched


def test_discovery_keeps_configured_pipeline_ids(aws: Aws, feed_of: Callable[..., SfnFeed]) -> None:
    answer = load_json("api/detection/list_state_machines.json")
    aws.stubber.add_response(
        "list_state_machines", api_response({**answer, "nextToken": "next"}), {"maxResults": 1000}
    )
    aws.stubber.add_response(
        "list_state_machines", {"stateMachines": []}, {"maxResults": 1000, "nextToken": "next"}
    )
    aws.machine_day2(SALES_SM)
    for machine in (CUSTOMERS_SM, INVENTORY_SM):
        aws.no_executions(machine, "FAILED")
        aws.no_executions(machine, "TIMED_OUT")
    feed = feed_of(pipelines={"sales_daily_etl": {"state_machine": SALES_SM}}, discover=True)
    page = feed.poll(cursor=None, is_monitored=watch_all)
    assert [failure.ref.pipeline_id for failure in page.failures] == ["sales_daily_etl"]


def test_discovery_skips_a_machine_named_like_another_pipeline(
    aws: Aws, feed_of: Callable[..., SfnFeed], caplog: pytest.LogCaptureFixture
) -> None:
    """``customers-sync`` is the configured id of the sales machine: the customers machine (named
    ``customers-sync``) is not watched, so it never takes that pipeline's incidents."""
    caplog.set_level(logging.DEBUG, logger="dag_healer.stepfunctions.feed")
    aws.stubber.add_response(
        "list_state_machines",
        api_response(load_json("api/detection/list_state_machines.json")),
        {"maxResults": 1000},
    )
    aws.machine_day2(SALES_SM)
    aws.no_executions(INVENTORY_SM, "FAILED")
    aws.no_executions(INVENTORY_SM, "TIMED_OUT")
    feed = feed_of(pipelines={"customers-sync": {"state_machine": SALES_SM}}, discover=True)
    page = feed.poll(cursor=None, is_monitored=watch_all)
    assert [(failure.ref.pipeline_id, failure.ref.run_id) for failure in page.failures] == [
        ("customers-sync", SALES_RUN)
    ]
    assert [entry.pipeline_id for entry in feed.rt.pipelines.entries()] == [
        "customers-sync",
        "InventoryReportStateMachine-XyZ789",
    ]
    assert [record.getMessage() for record in caplog.records if record.levelno == logging.WARNING] == [
        f"dag-healer stepfunctions {INSTANCE}: state machine customers-sync is not watched: its name is "
        "the pipeline id of another state machine"
    ]


def test_a_failed_discovery_still_polls_the_configured_machines(
    aws: Aws, feed_of: Callable[..., SfnFeed]
) -> None:
    aws.stubber.add_client_error(
        "list_state_machines", service_error_code="AccessDeniedException", http_status_code=400
    )
    aws.machine_day2(SALES_SM)
    page = feed_of(pipelines={"sales_daily_etl": {"state_machine": SALES_SM}}, discover=True).poll(
        cursor=None, is_monitored=watch_all
    )
    assert [failure.ref.pipeline_id for failure in page.failures] == ["sales_daily_etl"]


def test_a_throttled_discovery_propagates(aws: Aws, feed_of: Callable[..., SfnFeed]) -> None:
    aws.stubber.add_client_error(
        "list_state_machines", service_error_code="ThrottlingException", http_status_code=400
    )
    with pytest.raises(OrchestratorError) as raised:
        feed_of(discover=True).poll(cursor=None, is_monitored=watch_all)
    assert raised.value.transient is True


def test_a_machine_configured_by_name_is_resolved_once(aws: Aws, feed_of: Callable[..., SfnFeed]) -> None:
    feed = feed_of(pipelines={"customers_sync": {"state_machine": "customers-sync"}})
    aws.stubber.add_response(
        "list_state_machines",
        api_response(load_json("api/detection/list_state_machines.json")),
        {"maxResults": 1000},
    )
    aws.machine_day2(CUSTOMERS_SM)
    first = feed.poll(cursor=None, is_monitored=watch_all)
    assert [failure.ref.pipeline_id for failure in first.failures] == ["customers_sync"]
    aws.machine_day2(CUSTOMERS_SM, read=False)  # no second ListStateMachines
    feed.poll(cursor=first.cursor, is_monitored=watch_all)


def test_an_unknown_machine_name_is_skipped(aws: Aws, feed_of: Callable[..., SfnFeed]) -> None:
    aws.stubber.add_response("list_state_machines", {"stateMachines": []}, {"maxResults": 1000})
    aws.machine_day2(SALES_SM)
    feed = feed_of(
        pipelines={
            "renamed": {"state_machine": "renamed-machine"},
            "sales_daily_etl": {"state_machine": SALES_SM},
        }
    )
    page = feed.poll(cursor=None, is_monitored=watch_all)
    assert [failure.ref.pipeline_id for failure in page.failures] == ["sales_daily_etl"]


# =========================================================================== errors


def test_transient_listing_errors_propagate(aws: Aws, feed_of: Callable[..., SfnFeed]) -> None:
    aws.list_error(SALES_SM, "FAILED", "ThrottlingException")
    with pytest.raises(OrchestratorError) as raised:
        feed_of().poll(cursor=None, is_monitored=watch_all)
    assert raised.value.transient is True


def test_server_errors_propagate_as_transient(aws: Aws, feed_of: Callable[..., SfnFeed]) -> None:
    aws.list_error(SALES_SM, "FAILED", "InternalFailure", http=500)
    with pytest.raises(OrchestratorError) as raised:
        feed_of().poll(cursor=None, is_monitored=watch_all)
    assert raised.value.transient is True


def test_transient_history_errors_propagate(aws: Aws, feed_of: Callable[..., SfnFeed]) -> None:
    aws.list(SALES_SM, "FAILED", listing(SALES_SM))
    aws.no_executions(SALES_SM, "TIMED_OUT")
    aws.history_error(execution_arn(SALES_SM, SALES_RUN), "ThrottlingException")
    with pytest.raises(OrchestratorError) as raised:
        feed_of().poll(cursor=None, is_monitored=watch_all)
    assert raised.value.transient is True


@pytest.mark.parametrize(
    "code", ["StateMachineDoesNotExist", "StateMachineTypeNotSupported", "AccessDeniedException"]
)
def test_a_machine_that_cannot_be_listed_is_skipped(
    code: str, aws: Aws, feed_of: Callable[..., SfnFeed], caplog: pytest.LogCaptureFixture
) -> None:
    aws.list_error(SALES_SM, "FAILED", code)
    aws.machine_day2(CUSTOMERS_SM)
    aws.machine_day2(INVENTORY_SM)
    page = feed_of().poll(cursor=None, is_monitored=watch_all)
    assert [failure.ref.pipeline_id for failure in page.failures] == ["customers_sync", "inventory_report"]
    assert page.cursor[SALES_SM] == LOOKBACK_START_MS + POLL_OVERLAP_MS  # its window waits for the fix
    assert any(
        "sales_daily_etl" in record.getMessage() and code in record.getMessage() for record in caplog.records
    )


def test_a_machine_that_cannot_be_listed_is_warned_about_once(
    aws: Aws, feed_of: Callable[..., SfnFeed], caplog: pytest.LogCaptureFixture
) -> None:
    caplog.set_level(logging.DEBUG, logger="dag_healer.stepfunctions.feed")
    feed = feed_of(
        pipelines={
            "sales_daily_etl": {"state_machine": SALES_SM},
            "customers_sync": {"state_machine": CUSTOMERS_SM},
        }
    )
    for _poll in range(2):
        aws.list_error(SALES_SM, "FAILED", "StateMachineDoesNotExist")
        aws.no_executions(CUSTOMERS_SM, "FAILED")
        aws.no_executions(CUSTOMERS_SM, "TIMED_OUT")
        feed.poll(cursor=None, is_monitored=watch_all)
    levels = [record.levelno for record in caplog.records if "cannot list" in record.getMessage()]
    assert levels == [logging.WARNING, logging.DEBUG]


def test_when_no_machine_can_be_listed_the_error_propagates(
    aws: Aws, feed_of: Callable[..., SfnFeed]
) -> None:
    aws.list_error(SALES_SM, "FAILED", "StateMachineDoesNotExist")
    with pytest.raises(NotFound):
        feed_of(pipelines={"sales_daily_etl": {"state_machine": SALES_SM}}).poll(
            cursor=None, is_monitored=watch_all
        )


def test_permission_denied_for_every_machine_propagates(aws: Aws, feed_of: Callable[..., SfnFeed]) -> None:
    for machine in (SALES_SM, CUSTOMERS_SM, INVENTORY_SM):
        aws.list_error(machine, "FAILED", "AccessDeniedException")
    with pytest.raises(PermissionDenied):
        feed_of().poll(cursor=None, is_monitored=watch_all)


# =========================================================================== localisation details


def test_timed_out_execution_is_run_level(aws: Aws, feed_of: Callable[..., SfnFeed]) -> None:
    item = dict(listing(SALES_SM)["executions"][0])
    events = history_events("execution_timed_out.json")
    item.update(name="sales-2026-10-02-slow", status="TIMED_OUT", stopDate=events[-1]["timestamp"])
    item["executionArn"] = execution_arn(SALES_SM, item["name"])
    aws.no_executions(SALES_SM, "FAILED")
    aws.list(SALES_SM, "TIMED_OUT", {"executions": [item]})
    aws.history(item["executionArn"], events)
    page = feed_of(pipelines={"sales_daily_etl": {"state_machine": SALES_SM}}).poll(
        cursor=None, is_monitored=watch_all
    )
    (failure,) = page.failures
    assert failure.ref == AttemptRef(
        INSTANCE, "sales_daily_etl", "sales-2026-10-02-slow", RUN_LEVEL_STEP, -1, 1
    )
    assert (failure.error_name, failure.native["event_type"], failure.native["status"]) == (
        "States.Timeout",
        "ExecutionTimedOut",
        "TIMED_OUT",
    )
    assert failure.error_summary == causes.summarize("States.Timeout", None)


def test_aborted_executions_are_polled_when_configured(aws: Aws, feed_of: Callable[..., SfnFeed]) -> None:
    item = dict(listing(SALES_SM)["executions"][0])
    events = history_events("execution_aborted.json")
    item.update(name="sales-2026-10-02-manual", status="ABORTED", stopDate=events[-1]["timestamp"])
    item["executionArn"] = execution_arn(SALES_SM, item["name"])
    aws.no_executions(SALES_SM, "FAILED")
    aws.list(SALES_SM, "ABORTED", {"executions": [item]})
    aws.history(item["executionArn"], events)
    feed = feed_of(pipelines={"sales_daily_etl": {"state_machine": SALES_SM}}, statuses=["FAILED", "ABORTED"])
    (failure,) = feed.poll(cursor=None, is_monitored=watch_all).failures
    assert (failure.ref.step_id, failure.error_name) == (RUN_LEVEL_STEP, "ManualStop")


def test_a_redriven_execution_is_the_next_attempt(aws: Aws, feed_of: Callable[..., SfnFeed]) -> None:
    events = history_events("redriven_failed_again.json")
    item = dict(listing(SALES_SM)["executions"][0])
    item.update(redriveCount=1, stopDate=events[-1]["timestamp"], redriveDate=events[11]["timestamp"])
    aws.list(SALES_SM, "FAILED", {"executions": [item]})
    aws.no_executions(SALES_SM, "TIMED_OUT")
    aws.history(item["executionArn"], events)  # read back to ExecutionRedriven (redrive 1)
    page = feed_of(pipelines={"sales_daily_etl": {"state_machine": SALES_SM}}).poll(
        cursor=None, is_monitored=watch_all
    )
    (failure,) = page.failures
    assert failure.ref == AttemptRef(INSTANCE, "sales_daily_etl", SALES_RUN, "TransformSales", -1, 2)
    assert (
        failure.native["redrive_count"],
        failure.native["failed_event_id"],
        failure.native["retry_attempts"],
    ) == (
        1,
        19,
        1,
    )


def test_a_state_resumed_without_entry_is_read_with_the_earlier_segment(
    aws: Aws, feed_of: Callable[..., SfnFeed]
) -> None:
    events = history_events("redriven_without_reentry.json")
    item = dict(listing(SALES_SM)["executions"][0])
    item.update(redriveCount=1, stopDate=events[-1]["timestamp"])
    aws.list(SALES_SM, "FAILED", {"executions": [item]})
    aws.no_executions(SALES_SM, "TIMED_OUT")
    aws.history(item["executionArn"], events)  # redrive 1 only: the entry is missing
    aws.history(item["executionArn"], events)  # redrive 0 too: back to ExecutionStarted
    page = feed_of(pipelines={"sales_daily_etl": {"state_machine": SALES_SM}}).poll(
        cursor=None, is_monitored=watch_all
    )
    (failure,) = page.failures
    assert (failure.ref.step_id, failure.ref.attempt, failure.native["entered_event_id"]) == (
        "TransformSales",
        2,
        7,
    )


# =========================================================================== unusable histories


def _kms_denied_polls(aws: Aws, feed: SfnFeed, polls: int, cursor: Any = None) -> FailurePage:
    page = FailurePage(failures=(), cursor=cursor or {})
    for _poll in range(polls):
        aws.list(SALES_SM, "FAILED", listing(SALES_SM))
        aws.no_executions(SALES_SM, "TIMED_OUT")
        aws.history_error(execution_arn(SALES_SM, SALES_RUN), "KmsAccessDeniedException")
        page = feed.poll(cursor=page.cursor or cursor, is_monitored=watch_all)
    return page


def test_an_unreadable_history_is_checked_again_then_reported_run_level(
    aws: Aws, feed_of: Callable[..., SfnFeed]
) -> None:
    feed = feed_of(pipelines={"sales_daily_etl": {"state_machine": SALES_SM}})
    page = _kms_denied_polls(aws, feed, MAX_UNREADABLE_POLLS)
    assert page.failures == ()
    assert page.complete is True  # nothing was cut: the history is checked again on the next poll
    assert positions(page.cursor) == {SALES_SM: stop_ms(SALES_SM) - 1}  # held before the pending execution

    described = api_response(load_json("api/detection/describe_execution_customers.json"))
    described.update(
        executionArn=execution_arn(SALES_SM, SALES_RUN),
        stateMachineArn=SALES_SM,
        name=SALES_RUN,
        error="KeyError",
    )
    described["cause"] = history_events(HISTORIES[SALES_SM])[-1]["executionFailedEventDetails"]["cause"]
    aws.list(SALES_SM, "FAILED", listing(SALES_SM))
    aws.no_executions(SALES_SM, "TIMED_OUT")
    aws.history_error(execution_arn(SALES_SM, SALES_RUN), "KmsAccessDeniedException")
    aws.stubber.add_response(
        "describe_execution", described, {"executionArn": execution_arn(SALES_SM, SALES_RUN)}
    )
    final = feed.poll(cursor=page.cursor, is_monitored=watch_all)
    (failure,) = final.failures
    assert failure.ref == AttemptRef(INSTANCE, "sales_daily_etl", SALES_RUN, RUN_LEVEL_STEP, -1, 1)
    assert failure.native["localized"] is False
    assert failure.native["event_type"] == "ExecutionFailed"
    assert failure.error_name == "KeyError"
    assert failure.error_summary == causes.summarize("KeyError", described["cause"])
    assert PII not in json.dumps(dict(failure.native))
    assert positions(final.cursor) == {SALES_SM: stop_ms(SALES_SM)}

    aws.machine_day2(SALES_SM, read=False)  # neither the history nor DescribeExecution again
    assert feed.poll(cursor=final.cursor, is_monitored=watch_all).failures == ()  # stored
    aws.machine_day2(SALES_SM, read=False)
    assert feed.poll(cursor=None, is_monitored=watch_all).failures == final.failures  # remembered


def test_the_run_level_fallback_survives_a_failing_describe(
    aws: Aws, feed_of: Callable[..., SfnFeed]
) -> None:
    feed = feed_of(pipelines={"sales_daily_etl": {"state_machine": SALES_SM}})
    page = _kms_denied_polls(aws, feed, MAX_UNREADABLE_POLLS)
    aws.list(SALES_SM, "FAILED", listing(SALES_SM))
    aws.no_executions(SALES_SM, "TIMED_OUT")
    aws.history_error(execution_arn(SALES_SM, SALES_RUN), "KmsAccessDeniedException")
    aws.stubber.add_client_error(
        "describe_execution",
        service_error_code="ExecutionDoesNotExist",
        http_status_code=400,
        expected_params={"executionArn": execution_arn(SALES_SM, SALES_RUN)},
    )
    (failure,) = feed.poll(cursor=page.cursor, is_monitored=watch_all).failures
    assert (failure.ref.step_id, failure.error_name, failure.error_summary) == (RUN_LEVEL_STEP, None, "")


def test_a_history_without_terminal_event_is_not_usable_yet(
    aws: Aws, feed_of: Callable[..., SfnFeed]
) -> None:
    """ListExecutions says FAILED, the history (eventually consistent) does not show it yet."""
    feed = feed_of(pipelines={"sales_daily_etl": {"state_machine": SALES_SM}})
    aws.list(SALES_SM, "FAILED", listing(SALES_SM))
    aws.no_executions(SALES_SM, "TIMED_OUT")
    aws.history(execution_arn(SALES_SM, SALES_RUN), history_events(HISTORIES[SALES_SM])[:-1])
    page = feed.poll(cursor={SALES_SM: stop_ms(SALES_SM) - 1}, is_monitored=watch_all)
    assert page.failures == ()
    assert positions(page.cursor) == {SALES_SM: stop_ms(SALES_SM) - 1}
    aws.machine_day2(SALES_SM)
    (failure,) = feed.poll(cursor=page.cursor, is_monitored=watch_all).failures
    assert failure.ref.step_id == "TransformSales"


def test_a_pending_execution_does_not_block_newer_ones(aws: Aws, feed_of: Callable[..., SfnFeed]) -> None:
    two_days = load_json("api/detection/list_executions_sales_two_days.json")
    rerun = two_days["executions"][1]
    feed = feed_of(pipelines={"sales_daily_etl": {"state_machine": SALES_SM}})
    aws.list(SALES_SM, "FAILED", two_days)
    aws.no_executions(SALES_SM, "TIMED_OUT")
    aws.history_error(rerun["executionArn"], "KmsAccessDeniedException")
    aws.history(execution_arn(SALES_SM, SALES_RUN), history_events(HISTORIES[SALES_SM]))
    page = feed.poll(cursor=None, is_monitored=watch_all)
    assert [failure.ref.run_id for failure in page.failures] == [SALES_RUN]
    assert positions(page.cursor) == {
        SALES_SM: epoch_ms(rerun["stopDate"]) - 1
    }  # held before the pending one


# =========================================================================== building blocks


def test_execution_info_parses_api_items_defensively() -> None:
    item = api_response(listing(SALES_SM)["executions"][0])
    info = ExecutionInfo.from_api(item)
    assert info is not None
    assert (info.name, info.status, info.redrive_count, info.attempt) == (SALES_RUN, "FAILED", 0, 1)
    assert info.key == (execution_arn(SALES_SM, SALES_RUN), 0, epoch_ms(item["startDate"]))
    assert info.start_ms == epoch_ms("2026-10-02T06:00:00.000Z")
    assert info.stop_ms == stop_ms(SALES_SM)
    for broken in (
        {**item, "executionArn": None},
        {**item, "executionArn": "not-an-arn"},
        {**item, "stateMachineArn": "sales"},
        {**item, "status": ""},
        "item",
    ):
        assert ExecutionInfo.from_api(broken) is None
    odd = ExecutionInfo.from_api({**item, "name": None, "redriveCount": True, "stopDate": None})
    assert odd is not None
    assert (odd.name, odd.redrive_count, odd.stop_ms) == (SALES_RUN, 0, None)


def test_executions_without_stop_date_or_of_another_status_are_ignored(
    aws: Aws, feed_of: Callable[..., SfnFeed]
) -> None:
    item = listing(SALES_SM)["executions"][0]
    running = {**item, "name": "sales-running", "status": "RUNNING", "stopDate": None}
    running["executionArn"] = execution_arn(SALES_SM, "sales-running")
    other = {**item, "name": "sales-other", "status": "SUCCEEDED"}
    other["executionArn"] = execution_arn(SALES_SM, "sales-other")
    del running["stopDate"]
    aws.list(SALES_SM, "FAILED", {"executions": [running, other, item]})
    aws.no_executions(SALES_SM, "TIMED_OUT")
    aws.history(execution_arn(SALES_SM, SALES_RUN), history_events(HISTORIES[SALES_SM]))
    page = feed_of(pipelines={"sales_daily_etl": {"state_machine": SALES_SM}}).poll(
        cursor=None, is_monitored=watch_all
    )
    assert [failure.ref.run_id for failure in page.failures] == [SALES_RUN]


def test_execution_failure_falls_back_to_the_execution_error(make_runtime: Callable[..., Any]) -> None:
    info = ExecutionInfo(
        execution_arn(SALES_SM, SALES_RUN),
        SALES_SM,
        SALES_RUN,
        "FAILED",
        stop_date=NOW,
        redrive_count=2,
        error="States.Runtime",
        cause="An error occurred while executing the state 'LoadSales'",
    )
    failure = execution_failure(
        make_runtime(),
        pipeline_id="sales_daily_etl",
        run_id=SALES_RUN,
        info=info,
        localized=Localized(step_id=RUN_LEVEL_STEP, map_index=4),
        source="poll",
    )
    assert failure.ref == AttemptRef(INSTANCE, "sales_daily_etl", SALES_RUN, RUN_LEVEL_STEP, -1, 3)
    assert failure.error_name == "States.Runtime"
    assert failure.error_summary == causes.summarize("States.Runtime", info.cause)
    assert failure.failed_at == NOW
    assert failure.native["error"] == "States.Runtime"
    assert "cause" not in failure.native


def test_execution_failure_keeps_native_within_4_kb(make_runtime: Callable[..., Any]) -> None:
    long = "x" * 600
    info = ExecutionInfo(
        execution_arn(SALES_SM, SALES_RUN), SALES_SM, SALES_RUN, "FAILED", version_arn=f"{SALES_SM}:{long}"
    )
    localized = Localized(
        step_id="TransformSales",
        error=long,
        event_type="Task" + long,
        failed_event_id=16,
        resource=long,
        resource_type=long,
        function_name=long,
        step_path=long,
        map_run_arn=long,
    )
    failure = execution_failure(
        make_runtime(),
        pipeline_id="sales_daily_etl",
        run_id=SALES_RUN,
        info=info,
        localized=localized,
        source="poll",
    )
    assert_native_fits(failure.native, "failure.native")
    assert failure.native["failed_event_id"] == 16
    assert failure.native["event_type"].startswith("Task")
    assert "step_path" not in failure.native  # cut to the identifiers


def test_the_localizer_memory_is_bounded(make_runtime: Callable[..., Any]) -> None:
    localizer = HistoryLocalizer(make_runtime(), cache_size=2)
    infos = [
        ExecutionInfo(execution_arn(SALES_SM, f"run-{count}"), SALES_SM, f"run-{count}", "FAILED")
        for count in range(3)
    ]
    for info in infos:
        localizer._remember(info.key, Localized(step_id=info.name))
    assert localizer.remembered(infos[0]) is None
    assert localizer.remembered(infos[2]) == Localized(step_id="run-2")
    assert localizer.unusable_checks(infos[0]) == 0


def test_the_poll_uses_the_runtime_clock(aws: Aws, make_runtime: Callable[..., Any]) -> None:
    later = NOW + timedelta(days=2)  # the day-2 failure is now outside the 24 h lookback
    feed = SfnFeed(
        make_runtime(sfn_options(pipelines={"sales_daily_etl": {"state_machine": SALES_SM}}), now=later)
    )
    aws.list(SALES_SM, "FAILED", listing(SALES_SM))
    aws.no_executions(SALES_SM, "TIMED_OUT")
    page = feed.poll(cursor=None, is_monitored=watch_all)
    assert page.failures == ()
    assert isinstance(feed.rt.now(), datetime)
