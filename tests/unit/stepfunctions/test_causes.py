"""Tests of dag_healer.stepfunctions.causes: Lambda error payloads -> CPython tracebacks.

The table tests read ``tests/unit/fixtures/stepfunctions/causes/*.json``: each file holds a
history event's ``error`` and ``cause`` and what parsing, rendering and summarising must give.
"""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import pytest

from dag_healer.agent.context import extract_traceback
from dag_healer.core.tracebacks import TRACEBACK_HEADER
from dag_healer.stepfunctions.causes import (
    MAX_SUMMARY_CHARS,
    SHAPE_LISTS,
    SHAPE_NO_TRACE,
    SHAPE_PREFORMATTED,
    TRUNCATED_MARKER,
    CauseFrame,
    CauseInfo,
    exception_name,
    parse_cause,
    parse_payload,
    render_traceback,
    stack_frames,
    summarize,
)

from .conftest import FIXTURES, load_json

CASES = sorted((FIXTURES / "causes").glob("*.json"))
PARSED_CASES = [path for path in CASES if load_json(f"causes/{path.name}")["expected"].get("parsed", True)]
#: The 32 KB limit of a history event's ``cause``.
CAUSE_LIMIT = 32768


def _case(path: Path) -> dict[str, Any]:
    return load_json(f"causes/{path.name}")


def _frames(info: CauseInfo) -> list[list[Any]]:
    return [[frame.file, frame.line, frame.function, frame.code] for frame in info.frames]


# =========================================================================== fixture tables


def test_the_fixture_table_covers_every_shape() -> None:
    shapes = {_case(path)["expected"].get("shape") for path in CASES}
    assert {SHAPE_LISTS, SHAPE_PREFORMATTED, SHAPE_NO_TRACE, None} <= shapes
    assert any(_case(path)["expected"].get("truncated") for path in CASES)
    assert any(_case(path)["expected"].get("parsed") is False for path in CASES)


@pytest.mark.parametrize("path", CASES, ids=lambda path: path.stem)
def test_parse_cause(path: Path) -> None:
    case = _case(path)
    expected = case["expected"]
    info = parse_cause(case["cause"], error=case["error"])

    if expected.get("parsed") is False:
        assert info is None
        return
    assert info is not None
    assert (info.error_type, info.error_message, info.request_id, info.shape, info.truncated) == (
        expected["error_type"],
        expected["error_message"],
        expected["request_id"],
        expected["shape"],
        expected["truncated"],
    )
    assert _frames(info) == expected["frames"]


@pytest.mark.parametrize("path", PARSED_CASES, ids=lambda path: path.stem)
def test_rendered_traceback_round_trips_through_extract_traceback(path: Path) -> None:
    case = _case(path)
    expected = case["expected"]
    info = parse_cause(case["cause"], error=case["error"])
    assert info is not None

    text = info.traceback_text
    exception_type, message, frames, traceback_text = extract_traceback(text)

    assert text == render_traceback(info) and text.startswith(TRACEBACK_HEADER)
    assert [exception_type, message] == expected["exception"]
    assert [(frame["file"], frame["line"], frame["function"]) for frame in frames] == [
        (file, line if line is not None else 0, function)
        for file, line, function, _code in expected["frames"]
    ]
    assert traceback_text == text


@pytest.mark.parametrize("path", CASES, ids=lambda path: path.stem)
def test_summarize(path: Path) -> None:
    case = _case(path)
    summary = summarize(case["error"], case["cause"])

    assert summary == case["expected"]["summary"]
    assert "\n" not in summary and len(summary) <= MAX_SUMMARY_CHARS


def test_lambda_frames_keep_their_runtime_paths() -> None:
    info = parse_cause(_case(FIXTURES / "causes" / "python_preformatted_valueerror.json")["cause"])
    assert info is not None

    _type, _message, frames, _text = extract_traceback(info.traceback_text)

    files = [frame["file"] for frame in frames]
    assert files[0] == "/var/task/app.py"
    assert "/opt/python/etl/customers.py" in files
    assert ("/opt/python/etl/customers.py", 12, "parse_signup_date") in {
        (frame["file"], frame["line"], frame["function"]) for frame in frames
    }


# =========================================================================== not a Lambda payload


@pytest.mark.parametrize(
    "cause",
    [
        None,
        "",
        "   ",
        "States.Timeout",
        "[1, 2]",
        "42",
        '"just a sentence"',
        "{",
        '{"Error": "x", "Cause": "y"}',
        "Returned payload: not json",
    ],
)
def test_parse_cause_returns_none_for_other_causes(cause: str | None) -> None:
    assert parse_cause(cause, error="KeyError") is None


@pytest.mark.parametrize("cause", [123, 4.5, {"errorType": "KeyError"}, ["x"], b"{}"])
def test_parse_cause_ignores_non_text(cause: Any) -> None:
    assert parse_cause(cause) is None  # type: ignore[arg-type]


def test_payload_followed_by_text_is_parsed() -> None:
    info = parse_cause('{"errorType": "KeyError", "errorMessage": "\'a\'"} (see the function log)')
    assert info is not None and (info.error_type, info.error_message) == ("KeyError", "'a'")


# =========================================================================== odd payloads


def test_error_type_falls_back_to_the_event_error_then_to_nothing() -> None:
    cause = json.dumps({"errorMessage": "boom"})

    assert parse_cause(cause, error="  Lambda.Unknown ").error_type == "Lambda.Unknown"  # type: ignore[union-attr]
    info = parse_cause(cause)
    assert info is not None and info.error_type == ""
    assert render_traceback(info).splitlines() == [TRACEBACK_HEADER, "Exception: boom"]


@pytest.mark.parametrize(
    ("message", "expected"),
    [
        (None, ""),
        ("", ""),
        ({"b": 1, "a": "x"}, '{"a": "x", "b": 1}'),
        (["x", 2], '["x", 2]'),
        (7, "7"),
    ],
)
def test_error_messages_of_any_type_become_text(message: Any, expected: str) -> None:
    info = parse_cause(json.dumps({"errorType": "CustomError", "errorMessage": message}))
    assert info is not None and info.error_message == expected


def test_malformed_stack_elements_are_skipped_and_lines_checked() -> None:
    stack = [
        None,
        42,
        [],
        [123, 4],
        ["   "],
        ["/var/task/a.py"],
        ["/var/task/b.py", "7", "f"],
        ["/var/task/c.py", True, "g", "x = 1"],
        ["/var/task/d.py", -1, "  ", "   "],
        ("/var/task/e.py", 5, "h", "y()"),
        "",
        {"path": "/var/task/f.go", "line": 3},
    ]

    frames = stack_frames(stack)

    assert frames == (
        CauseFrame("/var/task/a.py"),
        CauseFrame("/var/task/b.py", 7, "f"),
        CauseFrame("/var/task/c.py", None, "g", "x = 1"),
        CauseFrame("/var/task/d.py", None, None, None),
        CauseFrame("/var/task/e.py", 5, "h", "y()"),
    )


@pytest.mark.parametrize("stack", [None, '  File "/var/task/app.py", line 3, in handler', {"a": 1}, 5])
def test_a_stack_that_is_not_a_list_gives_no_frames(stack: Any) -> None:
    info = parse_payload({"errorType": "KeyError", "errorMessage": "'a'", "stackTrace": stack})
    assert info is not None and info.frames == () and info.shape == SHAPE_NO_TRACE


def test_preformatted_strings_with_several_frames_and_markers() -> None:
    element = (
        '  File "/var/task/app.py", line 3, in handler\n'
        "    return run(event)\n"
        "           ^^^^^^^^^^\n"
        '  File "/var/task/app.py", line 9, in run\n'
        "    return run(event)\n"
        "  [Previous line repeated 996 more times]\n"
        '\u00a0\u00a0File "/opt/python/etl/x.py", line 2, in <module>\r\n'
    )

    frames = stack_frames([element])

    assert frames == (
        CauseFrame("/var/task/app.py", 3, "handler", "return run(event)"),
        CauseFrame("/var/task/app.py", 9, "run", "return run(event)"),
        CauseFrame("/opt/python/etl/x.py", 2, "<module>", None),
    )


def test_mixed_shapes_count_as_frame_lists() -> None:
    info = parse_payload(
        {
            "errorType": "KeyError",
            "errorMessage": "'a'",
            "stackTrace": [
                ["/var/task/app.py", 1, "handler", "x"],
                '  File "/opt/python/m.py", line 2, in f\n',
            ],
        }
    )
    assert info is not None and info.shape == SHAPE_LISTS and len(info.frames) == 2


def test_parse_payload_reads_decoded_records() -> None:
    record = {
        "timestamp": "2026-10-02T06:00:41.250Z",
        "level": "ERROR",
        "errorType": "KeyError",
        "errorMessage": "'qty'",
        "requestId": "c4a8e2f6-9b1d-4f3e-a5c7-2e4f6a8b0c1d",
        "stackTrace": ['  File "/var/task/app.py", line 11, in handler\n    return x\n'],
    }

    info = parse_payload(record)

    assert info == CauseInfo(
        error_type="KeyError",
        error_message="'qty'",
        frames=(CauseFrame("/var/task/app.py", 11, "handler", "return x"),),
        request_id="c4a8e2f6-9b1d-4f3e-a5c7-2e4f6a8b0c1d",
        shape=SHAPE_PREFORMATTED,
    )
    assert parse_payload({"level": "INFO", "message": "hello"}) is None


def test_a_request_id_must_be_text() -> None:
    info = parse_cause(json.dumps({"errorType": "E", "errorMessage": "m", "requestId": 12345}))
    assert info is not None and info.request_id is None


def test_the_timeout_request_id_is_only_read_from_timeout_messages() -> None:
    rid = "6f1c2d3e-4a5b-4c6d-8e7f-9a0b1c2d3e4f"
    plain = parse_cause(json.dumps({"errorType": "ValueError", "errorMessage": f"bad order {rid}"}))
    timeout = parse_cause(
        json.dumps({"errorMessage": f"2026-10-02T06:00:44.512Z {rid} Task timed out after 3.00 seconds"})
    )

    assert plain is not None and plain.request_id is None
    assert timeout is not None and timeout.request_id == rid


# =========================================================================== truncated payloads


def _recursion_cause() -> str:
    """A RecursionError whose payload is far larger than the 32 KB cause limit."""
    frame = ["/var/task/app.py", 9, "walk", "return walk(node.parent)"]
    payload = {
        "errorMessage": "maximum recursion depth exceeded",
        "errorType": "RecursionError",
        "requestId": "3b9e7a51-0c2d-4e6f-9a8b-7c6d5e4f3a2b",
        "stackTrace": [["/var/task/app.py", 3, "handler", "return walk(event)"]] + [frame] * 990,
    }
    return json.dumps(payload)


def test_a_cause_cut_at_the_32_kb_limit_keeps_every_complete_frame() -> None:
    full = _recursion_cause()
    cause = full[:CAUSE_LIMIT]
    assert len(full) > CAUSE_LIMIT

    info = parse_cause(cause, error="RecursionError")

    assert info is not None and info.truncated
    assert (info.error_type, info.error_message, info.request_id) == (
        "RecursionError",
        "maximum recursion depth exceeded",
        "3b9e7a51-0c2d-4e6f-9a8b-7c6d5e4f3a2b",
    )
    complete = json.loads(full)["stackTrace"]
    assert 1 < len(info.frames) < len(complete)
    assert _frames(info) == complete[: len(info.frames)]
    exception_type, _message, frames, _text = extract_traceback(info.traceback_text)
    assert exception_type == "RecursionError" and len(frames) == len(info.frames)


@pytest.mark.parametrize(
    ("tail", "expected"),
    [
        ("caf\\u00", "caf"),
        ("caf\\u00e9 au lait", "caf\u00e9 au lait"),
        ("a\\\\", "a\\"),
        ("a\\\\\\", "a\\"),
        ("line\\nnext\\", "line\nnext"),
        ("emoji \\ud83d", "emoji "),
        ("bad \\x escape", "bad \\x escape"),
    ],
)
def test_a_message_cut_inside_an_escape_is_decoded_up_to_the_cut(tail: str, expected: str) -> None:
    info = parse_cause('{"errorMessage": "' + tail, error="ValueError")

    assert info is not None and info.truncated
    assert info.error_message == expected + TRUNCATED_MARKER
    info.error_message.encode("utf-8")  # no lone surrogate survives


def test_a_cut_error_type_is_replaced_by_the_event_error() -> None:
    cut = '{"errorMessage": "\'a\'", "errorType": "KeyEr'

    with_error = parse_cause(cut, error="KeyError")
    without_error = parse_cause(cut)

    assert with_error is not None and with_error.error_type == "KeyError"
    assert without_error is not None and without_error.error_type == ""
    assert with_error.error_message == "'a'"


def test_a_cut_request_id_is_dropped() -> None:
    info = parse_cause('{"errorMessage": "m", "errorType": "E", "requestId": "6f1c2d3e-4a5b')
    assert info is not None and info.request_id is None and info.truncated


def test_a_cut_cause_without_lambda_members_is_not_a_payload() -> None:
    assert (
        parse_cause('{"JobName": "compaction", "ErrorMessage": "Out of mem', error="States.TaskFailed")
        is None
    )


# =========================================================================== rendering


def test_frames_without_line_or_function_still_parse() -> None:
    info = CauseInfo(
        error_type="KeyError",
        error_message="'a'",
        frames=(
            CauseFrame("/var/task/app.py", None, None, None),
            CauseFrame("/opt/python/etl/x.py", 4, "f", "  y  "),
        ),
    )

    text = render_traceback(info)

    assert text.splitlines() == [
        TRACEBACK_HEADER,
        '  File "/var/task/app.py", line 0',
        '  File "/opt/python/etl/x.py", line 4, in f',
        "    y",
        "KeyError: 'a'",
    ]
    _type, _message, frames, _text = extract_traceback(text)
    assert [frame["file"] for frame in frames] == ["/var/task/app.py", "/opt/python/etl/x.py"]


def test_a_multi_line_message_is_rendered_as_is() -> None:
    info = CauseInfo(error_type="ValueError", error_message="first line\nsecond line")

    exception_type, message, _frames, _text = extract_traceback(render_traceback(info))

    assert (exception_type, message) == ("ValueError", "first line\nsecond line")


def test_an_empty_message_prints_the_type_alone() -> None:
    assert render_traceback(CauseInfo(error_type="StopIteration", error_message="")).splitlines() == [
        TRACEBACK_HEADER,
        "StopIteration",
    ]


@pytest.mark.parametrize(
    ("error_type", "expected"),
    [
        ("KeyError", "KeyError"),
        ("Runtime.ImportModuleError", "Runtime.ImportModuleError"),
        ("  Sandbox.Timedout ", "Sandbox.Timedout"),
        ("Function<NoMethodError>", "Function_NoMethodError"),
        ("java.lang.RuntimeException", "java.lang.RuntimeException"),
        ("3rdPartyError", "_3rdPartyError"),
        ("my error", "my_error"),
        ("<>", "Exception"),
        ("", "Exception"),
        (None, "Exception"),
    ],
)
def test_exception_name(error_type: str | None, expected: str) -> None:
    name = exception_name(error_type)

    assert name == expected
    parsed, _message, _frames, _text = extract_traceback(f"{TRACEBACK_HEADER}\n{name}: message")
    assert parsed == expected


# =========================================================================== summaries


def test_summarize_collapses_whitespace_and_caps_the_length() -> None:
    long_cause = "line one\n\n   line\ttwo " + "x" * (MAX_SUMMARY_CHARS * 2)

    summary = summarize("States.Runtime", long_cause)

    assert summary.startswith("States.Runtime: line one line two xxx")
    assert len(summary) == MAX_SUMMARY_CHARS and summary.endswith("...")


@pytest.mark.parametrize(
    ("error", "cause", "expected"),
    [
        (None, None, ""),
        ("", "", ""),
        ("States.Timeout", None, "States.Timeout"),
        (None, "The execution was stopped.", "The execution was stopped."),
        ("  States.Runtime  ", "  bad path  ", "States.Runtime: bad path"),
        (None, json.dumps({"errorMessage": "only a message"}), "only a message"),
        ("KeyError", json.dumps({"errorType": "KeyError", "errorMessage": ""}), "KeyError"),
    ],
)
def test_summarize_with_partial_information(error: str | None, cause: str | None, expected: str) -> None:
    assert summarize(error, cause) == expected
