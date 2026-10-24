"""Tests of dag_healer.stepfunctions.logs: Lambda log correlation and rendering (design 9.7).

CloudWatch Logs answers come from ``tests/unit/fixtures/stepfunctions/logs`` (epoch-ms
timestamps, read with ``load_json``); ``GetFunction`` answers from ``api/evidence``. Every AWS
call goes through a ``botocore.stub.Stubber`` with its exact request parameters.
"""

from __future__ import annotations

import copy
import json
import logging
from datetime import datetime, timedelta
from typing import Any

import pytest

from dag_healer.agent.context import extract_traceback
from dag_healer.core.errors import OrchestratorError
from dag_healer.core.evidence import LogBundle
from dag_healer.stepfunctions import logs
from dag_healer.stepfunctions.logs import (
    FILTER_PAGE_LIMIT,
    LATE_LOGS_NOTE,
    LOOKUP_DISABLED_NOTE,
    MAX_INVOCATION_MS,
    MAX_LOG_LINES,
    default_log_group,
    empty_bundle,
    find_lambda_logs,
    render_log_events,
)
from dag_healer.stepfunctions.runtime import epoch_ms, to_utc

from .conftest import NOW, load_json, sfn_options

SALES_FN = "sfn-etl-SalesFunction-1A2B3C4D5E6F"
CUSTOMERS_FN = "sfn-etl-CustomersFunction-7G8H9I0J1K2L"
INVENTORY_FN = "sfn-etl-InventoryFunction-3M4N5O6P7Q8R"
SALES_ARN = f"arn:aws:lambda:eu-west-1:123456789012:function:{SALES_FN}"
SALES_GROUP = f"/aws/lambda/{SALES_FN}"
CUSTOMERS_GROUP = f"/aws/lambda/{CUSTOMERS_FN}"
INVENTORY_GROUP = "/sfn-etl/inventory"
SALES_RID = "6f1c2d3e-4a5b-4c6d-8e7f-9a0b1c2d3e4f"
CUSTOMERS_RID = "3b9e7a51-0c2d-4e6f-9a8b-7c6d5e4f3a2b"
INVENTORY_RID = "c4a8e2f6-9b1d-4f3e-a5c7-2e4f6a8b0c1d"
SALES_STREAM = "2026/10/02/[$LATEST]0a1b2c3d4e5f60718293a4b5c6d7e8f9"
CUSTOMERS_STREAM = "2026/10/02/[$LATEST]c0ffee00c0ffee00c0ffee00c0ffee00"
INVENTORY_STREAM = "2026/10/02/[$LATEST]1f2e3d4c5b6a79881f2e3d4c5b6a7988"
PII = "alice.pii@example.com"


def at(text: str) -> datetime:
    moment = to_utc(text)
    assert moment is not None
    return moment


def ms(text: str) -> int:
    value = epoch_ms(text)
    assert value is not None
    return value


def filter_params(group: str, start: str, end: str, **extra: Any) -> dict[str, Any]:
    return {
        "logGroupName": group,
        "startTime": ms(start),
        "endTime": ms(end),
        "limit": FILTER_PAGE_LIMIT,
        **extra,
    }


def expect_get_function(stubbed: Any, function: str, answer: str) -> None:
    _client, stubber = stubbed("lambda")
    stubber.add_response("get_function", load_json(f"api/evidence/{answer}.json"), {"FunctionName": function})


def expect_filter(stubbed: Any, answer: dict[str, Any] | str, params: dict[str, Any]) -> None:
    _client, stubber = stubbed("logs")
    stubber.add_response(
        "filter_log_events", load_json(f"logs/{answer}.json") if isinstance(answer, str) else answer, params
    )


def expect_filter_error(stubbed: Any, code: str, params: dict[str, Any], *, status: int = 400) -> None:
    _client, stubber = stubbed("logs")
    stubber.add_client_error(
        "filter_log_events",
        service_error_code=code,
        service_message="message text that names arn:aws:logs:eu-west-1:123456789012:log-group:x",
        http_status_code=status,
        expected_params=params,
    )


def event(timestamp: str, message: str, stream: str = "s1", number: int = 0) -> dict[str, Any]:
    return {"logStreamName": stream, "timestamp": ms(timestamp), "message": message, "eventId": f"e{number}"}


# =========================================================================== default_log_group


@pytest.mark.parametrize(
    "function",
    [
        SALES_FN,
        SALES_ARN,
        f"{SALES_ARN}:$LATEST",
        f"{SALES_ARN}:prod",
        f"123456789012:function:{SALES_FN}",
        f"{SALES_FN}:7",
        f"  {SALES_FN}  ",
    ],
)
def test_default_log_group(function: str) -> None:
    assert default_log_group(function) == SALES_GROUP


@pytest.mark.parametrize(
    "function", ["", "   ", "arn:aws:states:eu-west-1:123456789012:activity:a", None, 42]
)
def test_default_log_group_refuses_what_is_not_a_function(function: Any) -> None:
    with pytest.raises(ValueError):
        default_log_group(function)


# =========================================================================== rendering


def test_render_text_events_in_chronological_order() -> None:
    events = [
        event("2026-10-02T06:00:02.000Z", "second\n", number=2),
        event("2026-10-02T06:00:01.000Z", "first\n", number=1),
        event("2026-10-02T06:00:02.000Z", "third, same millisecond\r\n", number=3),
        event("2026-10-02T06:00:03.000Z", "\n", number=4),
    ]

    assert render_log_events(events) == ["first", "second", "third, same millisecond"]


def test_the_runtime_error_event_is_closed_like_cpython() -> None:
    answer = load_json("logs/sales_invocation.json")

    lines = render_log_events(answer["events"])
    exception_type, message, frames, _text = extract_traceback("\n".join(lines))

    assert lines[3:5] == ["[ERROR] KeyError: 'amount'", "Traceback (most recent call last):"]
    assert lines[13:16] == ["KeyError: 'amount'", "", f"END RequestId: {SALES_RID}"]
    assert (exception_type, message) == ("KeyError", "'amount'")
    assert [frame["file"] for frame in frames] == [
        "/var/task/app.py",
        "/opt/python/etl/sales.py",
        "/opt/python/pandas/core/frame.py",
        "/opt/python/pandas/core/indexes/base.py",
    ]


def test_no_break_space_indentation_becomes_plain() -> None:
    message = "[ERROR] ValueError: bad\r\u00a0\u00a0Traceback (most recent call last):\n"
    lines = render_log_events(
        [
            event(
                "2026-10-02T06:00:01.000Z",
                "[ERROR] ValueError: bad\rTraceback (most recent call last):\r"
                '\u00a0\u00a0File "/var/task/app.py", line 3, in handler\r\u00a0\u00a0\u00a0\u00a0run()\n',
            ),
            event("2026-10-02T06:00:02.000Z", message),
        ]
    )

    assert lines[:5] == [
        "[ERROR] ValueError: bad",
        "Traceback (most recent call last):",
        '  File "/var/task/app.py", line 3, in handler',
        "    run()",
        "ValueError: bad",
    ]
    assert lines[7] == "  Traceback (most recent call last):"


@pytest.mark.parametrize(
    "message",
    [
        # a traceback that already ends with its exception line (logging.exception in user code)
        "[ERROR]\t2026-10-02T06:00:01.000Z\trid\tFailed\nTraceback (most recent call last):\n"
        "  File \"/var/task/app.py\", line 3, in handler\nKeyError: 'a'\n",
        # the runtime shape, but the event already carries a line after the frames
        "[ERROR] KeyError: 'a'\rTraceback (most recent call last):\r"
        "  File \"/var/task/app.py\", line 3, in handler\rKeyError: 'a'\n",
        # an [ERROR] line without a traceback
        "[ERROR] KeyError: 'a'\n",
    ],
)
def test_complete_error_events_are_left_alone(message: str) -> None:
    lines = render_log_events([event("2026-10-02T06:00:01.000Z", message)])
    assert lines == message.rstrip("\n").replace("\r", "\n").split("\n")


def test_a_runtime_error_with_a_multi_line_message_and_no_frames() -> None:
    message = (
        "[ERROR] Runtime.ImportModuleError: Unable to import module 'app'\r"
        "second line\rTraceback (most recent call last):\n"
    )

    lines = render_log_events([event("2026-10-02T06:00:01.000Z", message)])

    assert lines[-4:] == [
        "Traceback (most recent call last):",
        "Runtime.ImportModuleError: Unable to import module 'app'",
        "second line",
        "",
    ]
    exception_type, text, _frames, _block = extract_traceback("\n".join(lines))
    assert (exception_type, text) == (
        "Runtime.ImportModuleError",
        "Unable to import module 'app'\nsecond line",
    )


def test_render_json_format_records() -> None:
    answer = load_json("logs/inventory_window.json")
    ours = [item for item in answer["events"] if item["logStreamName"] == INVENTORY_STREAM]

    lines = render_log_events(ours)

    assert lines[0] == f"START RequestId: {INVENTORY_RID} Version: $LATEST"
    assert (
        lines[1]
        == f"[INFO]\t2026-10-02T06:00:00.400Z\t{INVENTORY_RID}\tBuilding the report of warehouse north"
    )
    assert lines[2] == f"[ERROR]\t2026-10-02T06:00:41.250Z\t{INVENTORY_RID}\tKeyError: 'qty'"
    assert lines[-1] == (
        f"REPORT RequestId: {INVENTORY_RID}\tDuration: 41080.25 ms\tBilled Duration: 41081 ms\t"
        "Memory Size: 512 MB\tMax Memory Used: 160 MB\tStatus: error"
    )
    exception_type, message, frames, _text = extract_traceback("\n".join(lines))
    assert (exception_type, message) == ("KeyError", "'qty'")
    assert [frame["file"] for frame in frames] == ["/var/task/app.py", "/opt/python/etl/inventory.py"]


def _record(payload: dict[str, Any], timestamp: str = "2026-10-02T06:00:01.000Z") -> dict[str, Any]:
    return event(timestamp, json.dumps(payload) + "\n")


def test_json_records_of_every_kind() -> None:
    lines = render_log_events(
        [
            _record({"type": "platform.start", "time": "t", "record": {"requestId": "r1"}}),
            _record({"type": "platform.runtimeDone", "record": {"requestId": "r1", "status": "timeout"}}),
            _record({"type": "platform.end", "record": {"requestId": "r1", "status": "success"}}),
            _record(
                {"type": "platform.report", "record": {"requestId": "r1", "metrics": {"durationMs": True}}}
            ),
            _record({"type": "platform.initStart", "record": {"runtimeVersion": "python:3.11"}}),
            _record({"type": "platform.extension", "record": None}),
            _record({"level": "info", "message": {"order": 7, "state": "done"}, "requestId": "r1"}),
            _record({"levelname": "WARNING", "message": "line one\nline two"}),
            _record({"level": "ERROR", "message": {"errorType": "KeyError", "errorMessage": "'x'"}}),
            _record({"order": 7, "total": 10}),
        ]
    )

    assert lines == [
        "START RequestId: r1",
        "END RequestId: r1 Status: timeout",
        "END RequestId: r1",
        "REPORT RequestId: r1",
        'platform.initStart {"runtimeVersion": "python:3.11"}',
        "platform.extension",
        '[INFO]\t2026-10-02T06:00:01.000Z\tr1\t{"order": 7, "state": "done"}',
        "[WARNING]\t2026-10-02T06:00:01.000Z\tline one",
        "line two",
        "[ERROR]\t2026-10-02T06:00:01.000Z\tKeyError: 'x'",
        "Traceback (most recent call last):",
        "KeyError: 'x'",
        "",
        '{"order": 7, "total": 10}',
    ]


def test_a_node_json_error_keeps_its_stack_lines_as_text() -> None:
    record = {
        "timestamp": "2026-10-02T06:00:01.000Z",
        "level": "ERROR",
        "requestId": "r2",
        "message": "Invoke Error",
        "errorType": "TypeError",
        "errorMessage": "x is undefined",
        "stackTrace": ["TypeError: x is undefined", "    at handler (file:///var/task/index.mjs:3:9)"],
    }

    lines = render_log_events([_record(record)])

    assert lines == [
        "[ERROR]\t2026-10-02T06:00:01.000Z\tr2\tInvoke Error",
        "TypeError: x is undefined",
        "    at handler (file:///var/task/index.mjs:3:9)",
    ]
    _type, _message, frames, _text = extract_traceback("\n".join(lines))
    assert frames == []


@pytest.mark.parametrize(
    ("raw", "expected"),
    [
        ({"timestamp": 1, "message": None}, []),
        ({"timestamp": "2026-10-02T06:00:01Z", "message": 42}, ["42"]),
        ({"timestamp": True, "message": "bool timestamp"}, ["bool timestamp"]),
        ({"message": "no timestamp"}, ["no timestamp"]),
        ({"timestamp": 1.5, "message": "{not json"}, ["{not json"]),
        ({"timestamp": 2, "message": "[1, 2]"}, ["[1, 2]"]),
    ],
)
def test_odd_events_are_tolerated(raw: dict[str, Any], expected: list[str]) -> None:
    assert render_log_events([raw, "not a mapping", None]) == expected  # type: ignore[list-item]


def test_a_record_without_timestamp_takes_the_event_time() -> None:
    lines = render_log_events([_record({"level": "INFO", "message": "hello"}, "2026-10-02T06:00:01.250Z")])
    assert lines == ["[INFO]\t2026-10-02T06:00:01.250Z\thello"]


# =========================================================================== exact correlation


def test_exact_by_request_id_reads_the_whole_invocation(stubbed: Any, make_runtime: Any, caplog: Any) -> None:
    caplog.set_level(logging.DEBUG)
    expect_get_function(stubbed, SALES_ARN, "get_function_sales")
    expect_filter(
        stubbed,
        "sales_request_id_matches",
        filter_params(
            SALES_GROUP,
            "2026-10-02T06:00:19.100Z",
            "2026-10-02T06:00:46.400Z",
            filterPattern=f'"{SALES_RID}"',
        ),
    )
    expect_filter(
        stubbed,
        "sales_invocation",
        filter_params(
            SALES_GROUP, "2026-10-02T06:00:20.150Z", "2026-10-02T06:00:41.350Z", logStreamNames=[SALES_STREAM]
        ),
    )

    bundle = find_lambda_logs(
        make_runtime(),
        function=SALES_ARN,
        started_at=at("2026-10-02T06:00:20.100Z"),
        failed_at=at("2026-10-02T06:00:41.400Z"),
        request_id=SALES_RID,
    )

    assert (bundle.source, bundle.confidence, bundle.truncated) == (
        f"cloudwatch:{SALES_GROUP}",
        "exact",
        False,
    )
    assert bundle.notes == (f"Correlated by the Lambda request id {SALES_RID} (log stream {SALES_STREAM}).",)
    lines = bundle.text.split("\n")
    assert lines[0] == f"START RequestId: {SALES_RID} Version: $LATEST"
    assert "columns: date,store,sku,amt,unit_price" in lines  # a print line without the request id
    assert lines[-1].startswith(f"REPORT RequestId: {SALES_RID}\tDuration: 21200.12 ms")
    assert extract_traceback(bundle.text)[:2] == ("KeyError", "'amount'")
    logged = " ".join(record.getMessage() for record in caplog.records)
    assert "dag-healer stepfunctions: log excerpt of" in logged  # ids and counts are logged...
    assert not [text for text in ("KeyError", "acme-landing", "unit_price", "Traceback") if text in logged]


def test_exact_with_json_logs_and_the_configured_log_group(stubbed: Any, make_runtime: Any) -> None:
    window = load_json("logs/inventory_window.json")
    ours = [item for item in window["events"] if item["logStreamName"] == INVENTORY_STREAM]
    function = f"arn:aws:lambda:eu-west-1:123456789012:function:{INVENTORY_FN}"
    expect_get_function(stubbed, function, "get_function_inventory")
    expect_filter(
        stubbed,
        {"events": ours},
        filter_params(
            INVENTORY_GROUP,
            "2026-10-02T05:59:59.180Z",
            "2026-10-02T06:00:46.300Z",
            filterPattern=f'"{INVENTORY_RID}"',
        ),
    )
    expect_filter(
        stubbed,
        {"events": ours},
        filter_params(
            INVENTORY_GROUP,
            "2026-10-02T06:00:00.200Z",
            "2026-10-02T06:00:41.280Z",
            logStreamNames=[INVENTORY_STREAM],
        ),
    )

    bundle = find_lambda_logs(
        make_runtime(),
        function=function,
        started_at=at("2026-10-02T06:00:00.180Z"),
        failed_at=at("2026-10-02T06:00:41.300Z"),
        request_id=INVENTORY_RID,
    )

    assert (bundle.source, bundle.confidence) == (f"cloudwatch:{INVENTORY_GROUP}", "exact")
    assert extract_traceback(bundle.text)[:2] == ("KeyError", "'qty'")
    assert "warehouse south" not in bundle.text


def test_the_only_invocation_of_the_window_is_exact(stubbed: Any, make_runtime: Any) -> None:
    expect_get_function(stubbed, CUSTOMERS_FN, "get_function_customers")
    expect_filter(
        stubbed,
        "customers_window_single_invocation",
        filter_params(CUSTOMERS_GROUP, "2026-10-02T06:00:14.100Z", "2026-10-02T06:00:38.200Z"),
    )

    bundle = find_lambda_logs(
        make_runtime(),
        function=CUSTOMERS_FN,
        started_at=at("2026-10-02T06:00:15.100Z"),
        failed_at=at("2026-10-02T06:00:33.200Z"),
    )

    assert (bundle.source, bundle.confidence) == (f"cloudwatch:{CUSTOMERS_GROUP}", "exact")
    assert bundle.notes == (
        "Correlated by the only Lambda invocation that started during the failed attempt "
        f"(request id {CUSTOMERS_RID}, log stream {CUSTOMERS_STREAM}).",
    )
    assert extract_traceback(bundle.text)[0] == "ValueError"


def test_a_request_id_without_events_falls_back_to_the_window(stubbed: Any, make_runtime: Any) -> None:
    window = ("2026-10-02T06:00:14.100Z", "2026-10-02T06:00:38.200Z")
    expect_get_function(stubbed, CUSTOMERS_FN, "get_function_customers")
    expect_filter(
        stubbed, {"events": []}, filter_params(CUSTOMERS_GROUP, *window, filterPattern=f'"{SALES_RID}"')
    )
    expect_filter(stubbed, "customers_window_single_invocation", filter_params(CUSTOMERS_GROUP, *window))

    bundle = find_lambda_logs(
        make_runtime(),
        function=CUSTOMERS_FN,
        started_at=at("2026-10-02T06:00:15.100Z"),
        failed_at=at("2026-10-02T06:00:33.200Z"),
        request_id=SALES_RID,
    )

    assert bundle.confidence == "exact" and bundle.text
    assert bundle.notes[0] == (
        f"No log event of {CUSTOMERS_GROUP} carries the request id {SALES_RID}: the log was searched by time."
    )


@pytest.mark.parametrize("request_id", ['bad"id', "has space", "", "   ", "-leading-dash", "x" * 200, 12])
def test_unusable_request_ids_are_not_put_in_a_filter_pattern(
    stubbed: Any, make_runtime: Any, request_id: Any
) -> None:
    expect_get_function(stubbed, CUSTOMERS_FN, "get_function_customers")
    expect_filter(
        stubbed,
        "customers_window_single_invocation",
        filter_params(CUSTOMERS_GROUP, "2026-10-02T06:00:14.100Z", "2026-10-02T06:00:38.200Z"),
    )

    bundle = find_lambda_logs(
        make_runtime(),
        function=CUSTOMERS_FN,
        started_at=at("2026-10-02T06:00:15.100Z"),
        failed_at=at("2026-10-02T06:00:33.200Z"),
        request_id=request_id,
    )

    assert bundle.confidence == "exact"


def test_matches_without_a_start_line_use_the_window_end(stubbed: Any, make_runtime: Any) -> None:
    rid_events = [
        event("2026-10-02T06:00:30.000Z", f"[INFO]\t2026-10-02T06:00:30.000Z\t{SALES_RID}\tworking\n")
    ]
    expect_get_function(stubbed, SALES_FN, "get_function_sales")
    expect_filter(
        stubbed,
        {"events": rid_events},
        filter_params(
            SALES_GROUP,
            "2026-10-02T06:00:19.100Z",
            "2026-10-02T06:00:46.400Z",
            filterPattern=f'"{SALES_RID}"',
        ),
    )
    expect_filter(
        stubbed,
        {"events": []},
        filter_params(
            SALES_GROUP, "2026-10-02T06:00:30.000Z", "2026-10-02T06:00:46.400Z", logStreamNames=["s1"]
        ),
    )

    bundle = find_lambda_logs(
        make_runtime(),
        function=SALES_FN,
        started_at=at("2026-10-02T06:00:20.100Z"),
        failed_at=at("2026-10-02T06:00:41.400Z"),
        request_id=SALES_RID,
    )

    assert bundle.confidence == "exact"
    assert bundle.text == f"[INFO]\t2026-10-02T06:00:30.000Z\t{SALES_RID}\tworking"


# =========================================================================== heuristic correlation


def test_the_error_line_closest_to_the_failure_picks_the_stream(
    stubbed: Any, make_runtime: Any, caplog: Any
) -> None:
    caplog.set_level(logging.DEBUG)
    expect_get_function(stubbed, CUSTOMERS_FN, "get_function_customers")
    expect_filter(
        stubbed,
        "customers_window_two_invocations",
        filter_params(CUSTOMERS_GROUP, "2026-10-02T06:00:14.100Z", "2026-10-02T06:00:38.200Z"),
    )

    bundle = find_lambda_logs(
        make_runtime(),
        function=CUSTOMERS_FN,
        started_at=at("2026-10-02T06:00:15.100Z"),
        failed_at=at("2026-10-02T06:00:33.200Z"),
    )

    assert (bundle.source, bundle.confidence) == (f"cloudwatch:{CUSTOMERS_GROUP}", "heuristic")
    assert bundle.notes == (
        "No request id ties the log to the failed attempt: the log stream "
        f"{CUSTOMERS_STREAM} has the error line closest to the failure (heuristic).",
    )
    assert bundle.text.split("\n")[0] == f"START RequestId: {CUSTOMERS_RID} Version: $LATEST"
    assert "Backfill" not in bundle.text
    assert PII in bundle.text  # RAW: the core masks it
    assert not [record for record in caplog.records if PII in record.getMessage()]


def test_json_logs_of_two_map_iterations(stubbed: Any, make_runtime: Any) -> None:
    function = f"arn:aws:lambda:eu-west-1:123456789012:function:{INVENTORY_FN}"
    expect_get_function(stubbed, function, "get_function_inventory")
    expect_filter(
        stubbed,
        "inventory_window",
        filter_params(INVENTORY_GROUP, "2026-10-02T05:59:59.180Z", "2026-10-02T06:00:46.300Z"),
    )

    bundle = find_lambda_logs(
        make_runtime(),
        function=function,
        started_at=at("2026-10-02T06:00:00.180Z"),
        failed_at=at("2026-10-02T06:00:41.300Z"),
    )

    assert bundle.confidence == "heuristic"
    assert "warehouse north" in bundle.text and "warehouse south" not in bundle.text
    assert extract_traceback(bundle.text)[:2] == ("KeyError", "'qty'")


def test_no_error_line_ties_nothing(stubbed: Any, make_runtime: Any) -> None:
    expect_get_function(stubbed, CUSTOMERS_FN, "get_function_customers")
    expect_filter(
        stubbed,
        "customers_window_no_error_line",
        filter_params(CUSTOMERS_GROUP, "2026-10-02T06:00:14.100Z", "2026-10-02T06:00:38.200Z"),
    )

    bundle = find_lambda_logs(
        make_runtime(),
        function=CUSTOMERS_FN,
        started_at=at("2026-10-02T06:00:15.100Z"),
        failed_at=at("2026-10-02T06:00:33.200Z"),
    )

    assert (bundle.text, bundle.source, bundle.confidence) == ("", "none", "none")
    assert bundle.notes == (
        f"The CloudWatch log events of {CUSTOMERS_GROUP} around the failure have no error line "
        "(invocations started in the window: 2): none was tied to the failed attempt.",
    )


def test_ties_prefer_the_later_error_line(stubbed: Any, make_runtime: Any) -> None:
    answer = {
        "events": [
            event("2026-10-02T06:00:09.000Z", "[ERROR] early\n", "a", 1),
            event("2026-10-02T06:00:11.000Z", "[ERROR] late\n", "b", 2),
            event("2026-10-02T06:00:01.000Z", "START RequestId: r-a Version: $LATEST\n", "a", 3),
            event("2026-10-02T06:00:02.000Z", "START RequestId: r-b Version: $LATEST\n", "b", 4),
        ]
    }
    expect_get_function(stubbed, SALES_FN, "get_function_sales")
    expect_filter(
        stubbed, answer, filter_params(SALES_GROUP, "2026-10-02T05:59:59.000Z", "2026-10-02T06:00:15.000Z")
    )

    bundle = find_lambda_logs(
        make_runtime(),
        function=SALES_FN,
        started_at=at("2026-10-02T06:00:00.000Z"),
        failed_at=at("2026-10-02T06:00:10.000Z"),
    )

    assert bundle.text.split("\n") == ["START RequestId: r-b Version: $LATEST", "[ERROR] late"]


def test_an_error_line_after_the_failure_loses_to_an_earlier_one(stubbed: Any, make_runtime: Any) -> None:
    answer = {
        "events": [
            event("2026-10-02T06:00:01.000Z", "START RequestId: r-ours Version: $LATEST\n", "ours", 1),
            event("2026-10-02T06:00:07.000Z", "[ERROR] KeyError: 'amount'\n", "ours", 2),
            event("2026-10-02T06:00:08.000Z", "START RequestId: r-other Version: $LATEST\n", "other", 3),
            event("2026-10-02T06:00:11.500Z", "[ERROR] other invocation\n", "other", 4),
        ]
    }
    expect_get_function(stubbed, SALES_FN, "get_function_sales")
    expect_filter(
        stubbed, answer, filter_params(SALES_GROUP, "2026-10-02T05:59:59.000Z", "2026-10-02T06:00:15.000Z")
    )

    bundle = find_lambda_logs(
        make_runtime(),
        function=SALES_FN,
        started_at=at("2026-10-02T06:00:00.000Z"),
        failed_at=at("2026-10-02T06:00:10.000Z"),
    )

    assert bundle.text.split("\n") == [
        "START RequestId: r-ours Version: $LATEST",
        "[ERROR] KeyError: 'amount'",
    ]


# =========================================================================== nothing found, errors


@pytest.mark.parametrize(("now", "late"), [(NOW, False), (at("2026-10-02T06:05:00Z"), True)])
def test_no_events_is_never_an_error(stubbed: Any, make_runtime: Any, now: datetime, late: bool) -> None:
    expect_get_function(stubbed, CUSTOMERS_FN, "get_function_customers")
    expect_filter(
        stubbed,
        {"events": []},
        filter_params(CUSTOMERS_GROUP, "2026-10-02T06:00:14.100Z", "2026-10-02T06:00:38.200Z"),
    )

    bundle = find_lambda_logs(
        make_runtime(now=now),
        function=CUSTOMERS_FN,
        started_at=at("2026-10-02T06:00:15.100Z"),
        failed_at=at("2026-10-02T06:00:33.200Z"),
    )

    assert (bundle.text, bundle.source, bundle.confidence, bundle.truncated) == ("", "none", "none", False)
    first = (
        f"No CloudWatch log event of {CUSTOMERS_GROUP} was found between "
        "2026-10-02T06:00:14.100+00:00 and 2026-10-02T06:00:38.200+00:00."
    )
    assert bundle.notes == ((first, LATE_LOGS_NOTE) if late else (first,))


def test_a_failed_get_function_uses_the_default_log_group(stubbed: Any, make_runtime: Any) -> None:
    _client, stubber = stubbed("lambda")
    stubber.add_client_error(
        "get_function",
        "AccessDeniedException",
        "not allowed",
        403,
        expected_params={"FunctionName": SALES_ARN},
    )
    expect_filter(
        stubbed,
        "sales_request_id_matches",
        filter_params(
            SALES_GROUP,
            "2026-10-02T06:00:19.100Z",
            "2026-10-02T06:00:46.400Z",
            filterPattern=f'"{SALES_RID}"',
        ),
    )
    expect_filter(
        stubbed,
        "sales_invocation",
        filter_params(
            SALES_GROUP, "2026-10-02T06:00:20.150Z", "2026-10-02T06:00:41.350Z", logStreamNames=[SALES_STREAM]
        ),
    )

    bundle = find_lambda_logs(
        make_runtime(),
        function=SALES_ARN,
        started_at=at("2026-10-02T06:00:20.100Z"),
        failed_at=at("2026-10-02T06:00:41.400Z"),
        request_id=SALES_RID,
    )

    assert bundle.source == f"cloudwatch:{SALES_GROUP}" and bundle.confidence == "exact"


@pytest.mark.parametrize(
    "configuration",
    [
        {"Configuration": {"FunctionName": SALES_FN}},
        {
            "Configuration": {
                "FunctionName": SALES_FN,
                "LoggingConfig": {"LogFormat": "Text", "LogGroup": "  "},
            }
        },
        {},
    ],
)
def test_get_function_answers_without_a_log_group(
    stubbed: Any, make_runtime: Any, configuration: Any
) -> None:
    _client, stubber = stubbed("lambda")
    stubber.add_response("get_function", configuration, {"FunctionName": SALES_FN})
    expect_filter(
        stubbed,
        {"events": []},
        filter_params(SALES_GROUP, "2026-10-02T06:00:19.100Z", "2026-10-02T06:00:46.400Z"),
    )

    bundle = find_lambda_logs(
        make_runtime(),
        function=SALES_FN,
        started_at=at("2026-10-02T06:00:20.100Z"),
        failed_at=at("2026-10-02T06:00:41.400Z"),
    )

    assert SALES_GROUP in bundle.notes[0]


@pytest.mark.parametrize(
    ("code", "status", "note", "late"),
    [
        (
            "ResourceNotFoundException",
            400,
            f"The CloudWatch log group {CUSTOMERS_GROUP} does not exist",
            True,
        ),
        (
            "AccessDeniedException",
            400,
            f"Reading the CloudWatch log group {CUSTOMERS_GROUP} was refused",
            False,
        ),
        (
            "InvalidParameterException",
            400,
            f"Reading the CloudWatch log group {CUSTOMERS_GROUP} failed",
            False,
        ),
    ],
)
def test_non_transient_errors_give_an_empty_bundle(
    stubbed: Any, make_runtime: Any, caplog: Any, code: str, status: int, note: str, late: bool
) -> None:
    caplog.set_level(logging.DEBUG)
    expect_get_function(stubbed, CUSTOMERS_FN, "get_function_customers")
    expect_filter_error(
        stubbed,
        code,
        filter_params(CUSTOMERS_GROUP, "2026-10-02T06:00:14.100Z", "2026-10-02T06:00:38.200Z"),
        status=status,
    )

    bundle = find_lambda_logs(
        make_runtime(now=at("2026-10-02T06:01:00Z")),
        function=CUSTOMERS_FN,
        started_at=at("2026-10-02T06:00:15.100Z"),
        failed_at=at("2026-10-02T06:00:33.200Z"),
    )

    assert (bundle.text, bundle.source, bundle.confidence) == ("", "none", "none")
    assert bundle.notes[0].startswith(note) and f"({code})" in bundle.notes[0]
    assert (LATE_LOGS_NOTE in bundle.notes) is late
    assert "message text" not in " ".join(bundle.notes)  # the AWS message never reaches a note
    assert not [record for record in caplog.records if "message text" in record.getMessage()]


@pytest.mark.parametrize(
    ("code", "status"), [("ThrottlingException", 400), ("ServiceUnavailableException", 503)]
)
def test_transient_errors_propagate(stubbed: Any, make_runtime: Any, code: str, status: int) -> None:
    expect_get_function(stubbed, CUSTOMERS_FN, "get_function_customers")
    expect_filter_error(
        stubbed,
        code,
        filter_params(CUSTOMERS_GROUP, "2026-10-02T06:00:14.100Z", "2026-10-02T06:00:38.200Z"),
        status=status,
    )

    with pytest.raises(OrchestratorError) as raised:
        find_lambda_logs(
            make_runtime(),
            function=CUSTOMERS_FN,
            started_at=at("2026-10-02T06:00:15.100Z"),
            failed_at=at("2026-10-02T06:00:33.200Z"),
        )
    assert raised.value.transient


def _no_client(service: str) -> Any:
    raise AssertionError(f"no {service} client may be built")


def test_lookup_disabled_does_no_io(make_runtime: Any) -> None:
    rt = make_runtime(sfn_options(log_lookup=False), factory=_no_client)

    bundle = find_lambda_logs(rt, function=SALES_FN, started_at=at("2026-10-02T06:00:20Z"), failed_at=NOW)

    assert bundle == LogBundle(text="", source="none", confidence="none", notes=(LOOKUP_DISABLED_NOTE,))


@pytest.mark.parametrize("function", ["", "arn:aws:states:eu-west-1:123456789012:activity:approve", None])
def test_not_a_lambda_function(make_runtime: Any, function: Any) -> None:
    bundle = find_lambda_logs(
        make_runtime(factory=_no_client), function=function, started_at=None, failed_at=NOW
    )

    assert (bundle.text, bundle.confidence) == ("", "none")
    assert bundle.notes and "is not a Lambda function" in bundle.notes[0]


def test_no_timestamps_no_window(make_runtime: Any) -> None:
    bundle = find_lambda_logs(
        make_runtime(factory=_no_client), function=SALES_FN, started_at=None, failed_at=None
    )

    assert bundle.notes == ("The failed attempt has no timestamps: its CloudWatch log window is unknown.",)


def test_empty_bundle_drops_empty_notes() -> None:
    assert empty_bundle("a", "", "b") == LogBundle(
        text="", source="none", confidence="none", notes=("a", "b")
    )


# =========================================================================== windows


@pytest.mark.parametrize(
    ("started", "failed", "start", "end"),
    [
        (None, "2026-10-02T06:30:00.000Z", "2026-10-02T06:14:59.000Z", "2026-10-02T06:30:05.000Z"),
        ("2026-10-02T06:00:00.000Z", None, "2026-10-02T05:59:59.000Z", "2026-10-02T06:15:05.000Z"),
        (
            "2026-10-02T04:00:00.000Z",
            "2026-10-02T06:30:00.000Z",
            "2026-10-02T06:14:59.000Z",
            "2026-10-02T06:30:05.000Z",
        ),
        (
            "2026-10-02T06:40:00.000Z",
            "2026-10-02T06:30:00.000Z",
            "2026-10-02T06:29:59.000Z",
            "2026-10-02T06:30:05.000Z",
        ),
    ],
)
def test_the_window_of_partial_or_odd_timestamps(
    stubbed: Any, make_runtime: Any, started: str | None, failed: str | None, start: str, end: str
) -> None:
    expect_get_function(stubbed, SALES_FN, "get_function_sales")
    expect_filter(stubbed, {"events": []}, filter_params(SALES_GROUP, start, end))

    find_lambda_logs(
        make_runtime(),
        function=SALES_FN,
        started_at=at(started) if started else None,
        failed_at=at(failed) if failed else None,
    )


def test_the_window_constants() -> None:
    assert (logs.WINDOW_BEFORE_MS, logs.WINDOW_AFTER_MS, MAX_INVOCATION_MS) == (1000, 5000, 900_000)
    assert logs.LATE_LOGS_WINDOW == timedelta(minutes=15)


# =========================================================================== paging and caps


def test_pages_are_followed_through_empty_pages(stubbed: Any, make_runtime: Any) -> None:
    params = filter_params(CUSTOMERS_GROUP, "2026-10-02T06:00:14.100Z", "2026-10-02T06:00:38.200Z")
    answer = load_json("logs/customers_window_single_invocation.json")["events"]
    expect_get_function(stubbed, CUSTOMERS_FN, "get_function_customers")
    expect_filter(stubbed, {"events": [], "nextToken": "page-2"}, params)
    expect_filter(stubbed, {"events": answer[:2], "nextToken": "page-3"}, {**params, "nextToken": "page-2"})
    expect_filter(stubbed, {"events": answer[2:]}, {**params, "nextToken": "page-3"})

    bundle = find_lambda_logs(
        make_runtime(),
        function=CUSTOMERS_FN,
        started_at=at("2026-10-02T06:00:15.100Z"),
        failed_at=at("2026-10-02T06:00:33.200Z"),
    )

    assert bundle.confidence == "exact" and bundle.text.count("RequestId") == 3


def test_a_capped_search_rereads_the_chosen_stream(
    stubbed: Any, make_runtime: Any, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setattr(logs, "MAX_FILTER_PAGES", 2)
    params = filter_params(CUSTOMERS_GROUP, "2026-10-02T06:00:14.100Z", "2026-10-02T06:00:38.200Z")
    answer = load_json("logs/customers_window_two_invocations.json")["events"]
    ours = [item for item in answer if item["logStreamName"] == CUSTOMERS_STREAM]
    other = [item for item in answer if item["logStreamName"] != CUSTOMERS_STREAM]
    error = [item for item in ours if "[ERROR]" in item["message"]]
    expect_get_function(stubbed, CUSTOMERS_FN, "get_function_customers")
    expect_filter(stubbed, {"events": [ours[0], *other[:2]], "nextToken": "page-2"}, params)
    expect_filter(stubbed, {"events": error, "nextToken": "page-3"}, {**params, "nextToken": "page-2"})
    expect_filter(stubbed, {"events": ours}, {**params, "logStreamNames": [CUSTOMERS_STREAM]})

    bundle = find_lambda_logs(
        make_runtime(),
        function=CUSTOMERS_FN,
        started_at=at("2026-10-02T06:00:15.100Z"),
        failed_at=at("2026-10-02T06:00:33.200Z"),
    )

    assert bundle.confidence == "heuristic"
    lines = bundle.text.split("\n")
    assert lines[0] == f"START RequestId: {CUSTOMERS_RID} Version: $LATEST"
    assert lines[-1].startswith(f"REPORT RequestId: {CUSTOMERS_RID}")
    assert PII in bundle.text  # a line only the second read returned


def _long_invocation(lines: int) -> dict[str, Any]:
    events = [event("2026-10-02T06:00:01.000Z", "START RequestId: r-long Version: $LATEST\n", number=0)]
    events += [event("2026-10-02T06:00:02.000Z", f"line {n:03d}\n", number=n + 1) for n in range(lines)]
    events += [
        event("2026-10-02T06:00:03.000Z", "REPORT RequestId: r-long\tDuration: 1 ms\n", number=lines + 1)
    ]
    return {"events": events}


def test_the_excerpt_keeps_the_last_200_lines(stubbed: Any, make_runtime: Any) -> None:
    expect_get_function(stubbed, SALES_FN, "get_function_sales")
    expect_filter(
        stubbed,
        _long_invocation(300),
        filter_params(SALES_GROUP, "2026-10-02T05:59:59.000Z", "2026-10-02T06:00:15.000Z"),
    )

    bundle = find_lambda_logs(
        make_runtime(),
        function=SALES_FN,
        started_at=at("2026-10-02T06:00:00.000Z"),
        failed_at=at("2026-10-02T06:00:10.000Z"),
    )

    lines = bundle.text.split("\n")
    assert len(lines) == MAX_LOG_LINES and bundle.truncated
    assert lines[0] == "line 101" and lines[-1].startswith("REPORT RequestId: r-long")
    assert bundle.notes[-1] == f"The log excerpt keeps the last {MAX_LOG_LINES} lines."


def test_max_chars_keeps_the_end_from_a_line_boundary(stubbed: Any, make_runtime: Any) -> None:
    expect_get_function(stubbed, SALES_FN, "get_function_sales")
    expect_filter(
        stubbed,
        _long_invocation(5),
        filter_params(SALES_GROUP, "2026-10-02T05:59:59.000Z", "2026-10-02T06:00:15.000Z"),
    )

    bundle = find_lambda_logs(
        make_runtime(),
        function=SALES_FN,
        started_at=at("2026-10-02T06:00:00.000Z"),
        failed_at=at("2026-10-02T06:00:10.000Z"),
        max_chars=60,
    )

    assert bundle.text == "line 003\nline 004\nREPORT RequestId: r-long\tDuration: 1 ms"
    assert bundle.truncated and bundle.notes[-1] == "The log excerpt was cut to its last 60 characters."


def test_max_chars_inside_one_long_line_keeps_the_cut(stubbed: Any, make_runtime: Any) -> None:
    answer = {"events": [event("2026-10-02T06:00:05.000Z", "[ERROR] " + "x" * 100 + "\n")]}
    expect_get_function(stubbed, SALES_FN, "get_function_sales")
    expect_filter(
        stubbed, answer, filter_params(SALES_GROUP, "2026-10-02T05:59:59.000Z", "2026-10-02T06:00:15.000Z")
    )

    bundle = find_lambda_logs(
        make_runtime(),
        function=SALES_FN,
        started_at=at("2026-10-02T06:00:00.000Z"),
        failed_at=at("2026-10-02T06:00:10.000Z"),
        max_chars=10,
    )

    assert bundle.text == "x" * 10 and bundle.confidence == "heuristic"


def test_blank_events_give_an_empty_bundle(stubbed: Any, make_runtime: Any) -> None:
    answer = {"events": [event("2026-10-02T06:00:05.000Z", "START RequestId: r1 Version: $LATEST\n")]}
    answer["events"][0]["message"] = "   \n"
    answer["events"].append(copy.deepcopy(answer["events"][0]))
    expect_get_function(stubbed, SALES_FN, "get_function_sales")
    expect_filter(
        stubbed, answer, filter_params(SALES_GROUP, "2026-10-02T05:59:59.000Z", "2026-10-02T06:00:15.000Z")
    )

    bundle = find_lambda_logs(
        make_runtime(),
        function=SALES_FN,
        started_at=at("2026-10-02T06:00:00.000Z"),
        failed_at=at("2026-10-02T06:00:10.000Z"),
    )

    assert (bundle.text, bundle.confidence) == ("", "none") and bundle.notes
