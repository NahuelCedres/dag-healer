"""Lambda error causes -> Python tracebacks (pure, fixture-driven).

A failed Lambda Task's ``cause`` is the function's error payload as JSON text::

    {"errorMessage": "'amount'", "errorType": "KeyError", "requestId": "...",
     "stackTrace": [["/var/task/app.py", 12, "handler", "return transform(event)"], ...]}

``stackTrace`` comes in two shapes [U per runtime]: lists of ``[file, line, function, code]``
(documented for Python) or preformatted strings (``'  File "/var/task/app.py", line 12, in
handler\\n    return transform(event)\\n'``). ``requestId`` may be missing [U]. The cause is cut
at 32 KB, so the JSON may be truncated: :func:`parse_cause` is tolerant and returns ``None``
for anything that is not a Lambda error payload. :func:`render_traceback` produces CPython
traceback text (``core.tracebacks.render_python_traceback``) that
``agent.context.extract_traceback`` parses unchanged.

The details:

* **Truncated payloads.** A cause that starts like a JSON object but does not parse is read
  member by member: complete ``errorType``/``requestId`` strings, the ``errorMessage`` (cut
  where the cause was cut, then marked ``[truncated]``) and every complete ``stackTrace``
  element before the cut. :attr:`CauseInfo.truncated` tells the caller that frames may be
  missing (the log may then hold the whole traceback).
* **Wrapped payloads.** ``Lambda.Unknown`` causes read ``"The cause could not be determined
  because Lambda did not return an error type. Returned payload: {...}"`` [U]: the payload
  after ``Returned payload:`` is parsed. A payload encoded twice (a JSON string holding the
  JSON object) is unwrapped once.
* **No error type.** A Lambda timeout payload (``{"errorMessage": "<time> <request id> Task
  timed out after 3.00 seconds"}``) has no ``errorType``: the history event's ``error``
  (``Sandbox.Timedout``, ``Lambda.Unknown`` ...) names the exception, and the request id is
  read from the runtime's timeout message [U].
* **Other runtimes.** Node.js (``trace``/``stackTrace`` lines ``"    at fn (file:line:col)"``),
  Java, Ruby or .NET stacks are not Python frames: they are dropped (``frames=()``, shape
  ``lambda-no-trace``), so a non-Python Lambda never produces repository frames (design 9.6).
  The exception line is still rendered, with the type reduced to what a Python exception
  line may hold (``Function<NoMethodError>`` -> ``Function_NoMethodError``).
* **Frames.** A frame without a line number is rendered as ``line 0`` (the parser needs a
  number to keep the frame); Python 3.11+ caret lines (``~~^^^``) are not source lines.

Nothing here does I/O or logs: causes are RAW data (they may hold data values).
"""

from __future__ import annotations

import json
import re
from collections.abc import Mapping
from dataclasses import dataclass, field
from typing import Any

from dag_healer.core.tracebacks import render_python_traceback

#: Longest one-line summary :func:`summarize` returns.
MAX_SUMMARY_CHARS = 2000
#: Appended to an ``errorMessage`` that the 32 KB cut of the cause truncated.
TRUNCATED_MARKER = " [truncated]"

SHAPE_LISTS = "lambda"
SHAPE_PREFORMATTED = "lambda-preformatted"
SHAPE_NO_TRACE = "lambda-no-trace"

_PAYLOAD_MARKER = "Returned payload:"
_STRING_MEMBER = re.compile(r'"(?P<key>errorMessage|errorType|requestId)"\s*:\s*"')
_STACK_MEMBER = re.compile(r'"(?P<key>stackTrace|trace)"\s*:\s*\[')
_PYTHON_FRAME = re.compile(
    r'^[ \t]*File "(?P<file>[^"\r\n]+)", line (?P<line>\d+)(?:, in (?P<function>.+?))?[ \t]*$'
)
_MARKER_LINE = re.compile(r"^[ \t]*[~^]+[ \t~^]*$")
_LINE_BREAK = re.compile(r"\r\n|\r|\n")
_EXCEPTION_NAME = re.compile(r"^[A-Za-z_][\w.]*$")
_NOT_NAME_CHARS = re.compile(r"[^\w.]+")
_CUT_ESCAPE = re.compile(r"(\\+)(u[0-9a-fA-F]{0,3})?$")
_SURROGATE = re.compile("[\ud800-\udfff]")
_UUID = r"[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}"
_TIMEOUT_REQUEST_ID = re.compile(rf"(?:RequestId:\s*(?P<a>{_UUID})|(?P<b>{_UUID})\s).*Task timed out")
_WHITESPACE = re.compile(r"\s+")


@dataclass(frozen=True)
class CauseFrame:
    """One stack frame of a Lambda error."""

    file: str
    line: int | None = None
    function: str | None = None
    code: str | None = None


@dataclass(frozen=True)
class CauseInfo:
    """A parsed Lambda error payload (RAW text)."""

    error_type: str  # errorType ("KeyError"); "" when the payload has none
    error_message: str  # errorMessage
    frames: tuple[CauseFrame, ...] = field(default_factory=tuple)  # outermost first
    request_id: str | None = None
    shape: str = "lambda"  # "lambda" (frame lists) | "lambda-preformatted" | "lambda-no-trace"
    truncated: bool = False  # the cause was cut (32 KB): frames after the cut are missing

    @property
    def traceback_text(self) -> str:
        """The CPython traceback text of this error (see :func:`render_traceback`)."""
        return render_traceback(self)


# =========================================================================== parsing


def parse_cause(cause: str | None, *, error: str | None = None) -> CauseInfo | None:
    """Parse a Lambda error payload; ``None`` when ``cause`` is not one. ``error`` (the history
    event's error name) is used when the payload has no ``errorType``."""
    text = _payload_text(cause)
    if text is None:
        return None
    payload, parsed = _decode(text)
    if parsed and isinstance(payload, str):  # a payload encoded twice: unwrap it once
        text = payload.strip()
        if not text.startswith("{"):
            return None
        payload, parsed = _decode(text)
    if not parsed:
        return _partial_payload(text, error)
    if not isinstance(payload, Mapping):
        return None
    return parse_payload(payload, error=error)


def parse_payload(payload: Mapping[str, Any], *, error: str | None = None) -> CauseInfo | None:
    """A Lambda error payload already decoded (a cause, or a JSON-format log record); ``None``
    when ``payload`` has neither ``errorType`` nor ``errorMessage``."""
    if "errorType" not in payload and "errorMessage" not in payload:
        return None
    message = _message_text(payload.get("errorMessage"))
    frames, shape = _stack(_stack_value(payload))
    return CauseInfo(
        error_type=_error_type(payload.get("errorType"), error),
        error_message=message,
        frames=frames,
        request_id=_request_id(payload.get("requestId"), message),
        shape=shape,
    )


def stack_frames(stack: Any) -> tuple[CauseFrame, ...]:
    """The Python frames of a Lambda ``stackTrace`` (outermost first): ``[file, line, function,
    code]`` lists and preformatted ``File "...", line N, in fn`` strings. Elements of other
    runtimes (Node.js ``at ...`` lines, Java frames) and malformed elements are skipped."""
    return _stack(stack)[0]


def _payload_text(cause: Any) -> str | None:
    """The JSON text of the payload a cause holds (``None`` when it holds none)."""
    if not isinstance(cause, str):
        return None
    text = cause.strip()
    if text.startswith(("{", '"')):
        return text
    _head, marker, tail = text.partition(_PAYLOAD_MARKER)
    tail = tail.strip()
    return tail if marker and tail.startswith("{") else None


def _decode(text: str) -> tuple[Any, bool]:
    """``(value, True)`` for text that starts with a JSON value (trailing text is ignored)."""
    try:
        value, _end = json.JSONDecoder(strict=False).raw_decode(text)
    except ValueError:
        return None, False
    return value, True


def _stack_value(payload: Mapping[str, Any]) -> Any:
    """``stackTrace`` (Python, Java ...), else ``trace`` (Node.js)."""
    value = payload.get("stackTrace")
    return value if value is not None else payload.get("trace")


def _stack(stack: Any) -> tuple[tuple[CauseFrame, ...], str]:
    """``(Python frames, shape)`` of a ``stackTrace`` value."""
    if not isinstance(stack, (list, tuple)):
        return (), SHAPE_NO_TRACE
    frames: list[CauseFrame] = []
    from_lists = False
    for item in stack:
        if isinstance(item, (list, tuple)):
            frame = _list_frame(item)
            if frame is not None:
                frames.append(frame)
                from_lists = True
        elif isinstance(item, str):
            frames.extend(_string_frames(item))
    if not frames:
        return (), SHAPE_NO_TRACE
    return tuple(frames), SHAPE_LISTS if from_lists else SHAPE_PREFORMATTED


def _list_frame(item: list[Any] | tuple[Any, ...]) -> CauseFrame | None:
    """``[file, line, function, code]`` (shorter lists are accepted; the file is required)."""
    if not item or not isinstance(item[0], str) or not item[0].strip():
        return None
    return CauseFrame(
        file=item[0].strip(),
        line=_line_number(item[1]) if len(item) > 1 else None,
        function=_optional_text(item[2]) if len(item) > 2 else None,
        code=_optional_text(item[3]) if len(item) > 3 else None,
    )


def _string_frames(text: str) -> list[CauseFrame]:
    """The frames of one preformatted element (``traceback.format_list`` output)."""
    frames: list[CauseFrame] = []
    current: dict[str, Any] | None = None
    for raw in _LINE_BREAK.split(text.replace("\u00a0", " ")):
        match = _PYTHON_FRAME.match(raw)
        if match:
            if current is not None:
                frames.append(CauseFrame(**current))
            function = (match.group("function") or "").strip() or None
            current = {"file": match.group("file"), "line": int(match.group("line")), "function": function}
            continue
        if current is not None and "code" not in current and raw.strip() and not _MARKER_LINE.match(raw):
            current["code"] = raw.strip()
    if current is not None:
        frames.append(CauseFrame(**current))
    return frames


def _line_number(value: Any) -> int | None:
    if isinstance(value, bool):
        return None
    if isinstance(value, int):
        return value if value >= 0 else None
    if isinstance(value, str) and value.strip().isdigit():
        return int(value.strip())
    return None


def _optional_text(value: Any) -> str | None:
    if isinstance(value, str) and value.strip():
        return value
    return None


def _message_text(value: Any) -> str:
    """``errorMessage`` as text (a non-string message, e.g. an object, as compact JSON)."""
    if value is None:
        return ""
    if isinstance(value, str):
        return _SURROGATE.sub("", value)
    try:
        return json.dumps(value, ensure_ascii=False, sort_keys=True, default=str)
    except (TypeError, ValueError):
        return str(value)


def _error_type(value: Any, error: str | None) -> str:
    """``errorType``, else the history event's error name, else ``""``."""
    if isinstance(value, str) and value.strip():
        return value.strip()
    if isinstance(error, str) and error.strip():
        return error.strip()
    return ""


def _request_id(value: Any, message: str) -> str | None:
    """``requestId``, else the id in the runtime's ``Task timed out`` message [U]."""
    if isinstance(value, str) and value.strip():
        return value.strip()
    match = _TIMEOUT_REQUEST_ID.search(message) if "Task timed out" in message else None
    if match:
        return match.group("a") or match.group("b")
    return None


# =========================================================================== truncated JSON


def _partial_payload(text: str, error: str | None) -> CauseInfo | None:
    """The members of a JSON object cut before its end (see the module docstring)."""
    if not text.startswith("{"):
        return None
    strings: dict[str, tuple[str, bool]] = {}
    for match in _STRING_MEMBER.finditer(text):
        key = match.group("key")
        if key not in strings:
            strings[key] = _read_string(text, match.end())
    if "errorType" not in strings and "errorMessage" not in strings:
        return None
    message, complete = strings.get("errorMessage", ("", True))
    if not complete:
        message += TRUNCATED_MARKER
    error_type, type_complete = strings.get("errorType", ("", True))
    request_id, id_complete = strings.get("requestId", ("", True))
    stack_match = _STACK_MEMBER.search(text)
    frames, shape = _stack(_read_array(text, stack_match.end()) if stack_match else None)
    return CauseInfo(
        error_type=_error_type(error_type if type_complete else None, error),  # a cut type is noise
        error_message=message,
        frames=frames,
        request_id=_request_id(request_id if id_complete else None, message),
        shape=shape,
        truncated=True,
    )


def _read_string(text: str, start: int) -> tuple[str, bool]:
    """The JSON string whose body starts at ``start``: ``(value, complete)``. A string cut by
    the end of ``text`` is decoded up to the cut (an escape sequence cut in half is dropped)."""
    try:
        value, _end = json.JSONDecoder(strict=False).raw_decode(text, start - 1)
    except ValueError:
        pass
    else:
        if isinstance(value, str):
            return _SURROGATE.sub("", value), True
    body = text[start:]
    match = _CUT_ESCAPE.search(body)
    if match and len(match.group(1)) % 2 == 1:  # the last backslash opens an escape
        body = body[: match.start() + len(match.group(1)) - 1]
    try:
        value = json.loads(f'"{body}"', strict=False)
    except ValueError:
        value = body  # an invalid escape inside: keep the raw text
    return _SURROGATE.sub("", value), False


def _read_array(text: str, start: int) -> list[Any]:
    """The complete elements of a JSON array whose first element starts at ``start``."""
    decoder = json.JSONDecoder(strict=False)
    items: list[Any] = []
    position = start
    while True:
        position = _skip_space(text, position)
        if position >= len(text) or text[position] == "]":
            return items
        try:
            item, position = decoder.raw_decode(text, position)
        except ValueError:
            return items  # the cut happened inside this element
        items.append(item)
        position = _skip_space(text, position)
        if position < len(text) and text[position] == ",":
            position += 1
            continue
        return items


def _skip_space(text: str, position: int) -> int:
    while position < len(text) and text[position] in " \t\r\n":
        position += 1
    return position


# =========================================================================== rendering


def exception_name(error_type: str | None) -> str:
    """``error_type`` as the type of a CPython exception line (``Exception`` when empty).

    Characters a Python exception line cannot hold are replaced by ``_`` so that
    ``agent.context.extract_traceback`` still finds the line.
    """
    name = (error_type or "").strip()
    if not name:
        return "Exception"
    if _EXCEPTION_NAME.match(name):
        return name
    cleaned = _NOT_NAME_CHARS.sub("_", name).strip("_.")
    if not cleaned:
        return "Exception"
    return cleaned if _EXCEPTION_NAME.match(cleaned) else f"_{cleaned}"


def render_traceback(info: CauseInfo) -> str:
    """CPython traceback text (header, frames outermost first, ``<type>: <message>``)."""
    frames = [
        (frame.file, frame.line if frame.line is not None else 0, frame.function or "", frame.code)
        for frame in info.frames
    ]
    return render_python_traceback(frames, exception_name(info.error_type), info.error_message)


def summarize(error: str | None, cause: str | None) -> str:
    """One-line RAW summary of a step failure: ``"<errorType>: <errorMessage>"`` for a Lambda
    payload, else ``"<error>: <cause>"`` (or whichever is present)."""
    info = parse_cause(cause, error=error)
    if info is not None:
        text = _join(info.error_type, info.error_message)
    else:
        text = _join(error if isinstance(error, str) else "", cause if isinstance(cause, str) else "")
    text = _WHITESPACE.sub(" ", text).strip()
    if len(text) > MAX_SUMMARY_CHARS:
        text = text[: MAX_SUMMARY_CHARS - 3].rstrip() + "..."
    return text


def _join(head: str, tail: str) -> str:
    head, tail = head.strip(), tail.strip()
    if head and tail:
        return f"{head}: {tail}"
    return head or tail


__all__ = [
    "MAX_SUMMARY_CHARS",
    "SHAPE_LISTS",
    "SHAPE_NO_TRACE",
    "SHAPE_PREFORMATTED",
    "TRUNCATED_MARKER",
    "CauseFrame",
    "CauseInfo",
    "exception_name",
    "parse_cause",
    "parse_payload",
    "render_traceback",
    "stack_frames",
    "summarize",
]
