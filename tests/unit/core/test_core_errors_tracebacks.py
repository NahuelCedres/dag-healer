"""Tests for dag_healer.core.errors and dag_healer.core.tracebacks."""

from __future__ import annotations

import pickle

import pytest

from dag_healer.core.errors import NotFound, NotSupported, OrchestratorError, PermissionDenied
from dag_healer.core.tracebacks import (
    CAUSE_SEPARATOR,
    CONTEXT_SEPARATOR,
    TRACEBACK_HEADER,
    render_python_traceback,
)

# =========================================================================== errors


def test_error_hierarchy_and_attributes() -> None:
    for cls in (NotSupported, NotFound, PermissionDenied):
        assert issubclass(cls, OrchestratorError)
    assert issubclass(OrchestratorError, RuntimeError)

    error = OrchestratorError("Service Unavailable", transient=True, status_code=503)
    assert (error.detail, error.transient, error.status_code) == ("Service Unavailable", True, 503)
    assert str(error) == "Service Unavailable"

    plain = NotFound()
    assert (plain.detail, plain.transient, plain.status_code) == ("", False, None)
    assert str(plain) == "NotFound"
    assert str(PermissionDenied(status_code=403)) == "PermissionDenied (status 403)"


@pytest.mark.parametrize("cls", [OrchestratorError, NotSupported, NotFound, PermissionDenied])
def test_errors_pickle_with_their_attributes(cls: type[OrchestratorError]) -> None:
    error = cls("throttled", transient=True, status_code=429)
    restored = pickle.loads(pickle.dumps(error))
    assert type(restored) is cls
    assert (restored.detail, restored.transient, restored.status_code, str(restored)) == (
        "throttled",
        True,
        429,
        "throttled",
    )


# =========================================================================== tracebacks


def test_constants_moved_and_are_reexported_by_the_airflow_client() -> None:
    from dag_healer.airflow import client

    assert TRACEBACK_HEADER == "Traceback (most recent call last):"
    assert CAUSE_SEPARATOR == "The above exception was the direct cause of the following exception:"
    assert CONTEXT_SEPARATOR == "During handling of the above exception, another exception occurred:"
    assert client.TRACEBACK_HEADER is TRACEBACK_HEADER
    assert client.CAUSE_SEPARATOR is CAUSE_SEPARATOR
    assert client.CONTEXT_SEPARATOR is CONTEXT_SEPARATOR


LAMBDA_FRAMES = [
    ("/var/task/app.py", 12, "handler", "return transform(event)"),
    ("/var/task/src/etl/customers.py", 19, "parse_signup_date", None),
    ("/var/lang/lib/python3.12/_strptime.py", 558, "_strptime", "   raise ValueError(msg)   "),
]


def test_render_python_traceback_format() -> None:
    text = render_python_traceback(LAMBDA_FRAMES, "ValueError", "unconverted data remains: T08:30:00Z")

    assert text.splitlines() == [
        "Traceback (most recent call last):",
        '  File "/var/task/app.py", line 12, in handler',
        "    return transform(event)",
        '  File "/var/task/src/etl/customers.py", line 19, in parse_signup_date',
        '  File "/var/lang/lib/python3.12/_strptime.py", line 558, in _strptime',
        "    raise ValueError(msg)",
        "ValueError: unconverted data remains: T08:30:00Z",
    ]
    assert not text.endswith("\n")


def test_render_python_traceback_round_trips_through_extract_traceback() -> None:
    from dag_healer.agent.context import extract_traceback

    text = render_python_traceback(LAMBDA_FRAMES, "ValueError", "bad value\nsecond line")
    log = (
        "[2026-10-18T06:00:06.300000Z] INFO - START RequestId: 0f1e\n"
        f"{text}\n"
        "[2026-10-18T06:00:06.500000Z] INFO - END RequestId: 0f1e"
    )
    exc_type, message, frames, traceback_text = extract_traceback(log)

    assert (exc_type, message) == ("ValueError", "bad value\nsecond line")
    assert [(f["file"], f["line"], f["function"]) for f in frames] == [
        (f[0], f[1], f[2]) for f in LAMBDA_FRAMES
    ]
    assert traceback_text == text


@pytest.mark.parametrize(
    ("exc_type", "message", "last_line"),
    [
        ("Sandbox.Timedout", None, "Sandbox.Timedout"),
        ("Runtime.ImportModuleError", "", "Runtime.ImportModuleError"),
        ("KeyError", "'amount'", "KeyError: 'amount'"),
        ("", "boom", "Exception: boom"),
    ],
)
def test_render_without_frames_still_parses(exc_type: str, message: str | None, last_line: str) -> None:
    from dag_healer.agent.context import extract_traceback

    text = render_python_traceback([], exc_type, message)

    assert text.splitlines() == [TRACEBACK_HEADER, last_line]
    parsed_type, parsed_message, frames, _text = extract_traceback(text)
    assert parsed_type == (exc_type or "Exception") and frames == []
    assert parsed_message == (message or None)


def test_render_tolerates_partial_frames() -> None:
    text = render_python_traceback([("", 0, "", ""), ("f.py", 3, "", None)], "E", "m")  # type: ignore[list-item]
    assert text.splitlines()[1:3] == ['  File "<unknown>", line 0', '  File "f.py", line 3']


def test_render_matches_the_airflow_structured_log_renderer() -> None:
    from dag_healer.airflow.client import render_exception_chain

    stack = {
        "exc_type": "ValueError",
        "exc_value": "unconverted data remains: T08:30:00Z",
        "frames": [{"filename": f[0], "lineno": f[1], "name": f[2], "line": f[3]} for f in LAMBDA_FRAMES],
    }
    expected = render_python_traceback(LAMBDA_FRAMES, "ValueError", "unconverted data remains: T08:30:00Z")

    assert "\n".join(render_exception_chain([stack])) == expected
