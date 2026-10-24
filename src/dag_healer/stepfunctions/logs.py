"""Lambda log correlation (section 9.7 of the design, v1: Lambda only).

1. Log group: ``GetFunction(...).Configuration.LoggingConfig.LogGroup``, else
   ``/aws/lambda/<function>`` (a denied or failed ``GetFunction`` falls back to the group the
   caller knows from the template, else to the default).
2. Window: ``[started_at - 1 s, failed_at + 5 s]`` of the failed attempt.
3. A known request id (the cause JSON, ``native.request_id``, or a ``START RequestId:`` line)
   gives ``FilterLogEvents(filterPattern='"<request id>"')`` and confidence ``exact``.
   Otherwise the stream with an ``[ERROR]`` / ``Traceback`` / ``Task timed out`` line closest to
   the failure is taken, confidence ``heuristic``.
4. JSON-format Lambda logs (``timestamp``, ``level``, ``message``, ``requestId``, ``errorType``,
   ``errorMessage``, ``stackTrace``) are rendered to text lines (an error record becomes a
   CPython traceback). At most 200 lines (and ``max_chars``) are kept, the end first.
5. Lambda logs can lag 5 to 10 minutes: an empty answer is never an error, it gives
   ``LogBundle(text="", source="none", confidence="none", notes=("... logs may not be delivered
   yet ...",))``. ``options.log_lookup: false`` gives an empty bundle with a note too.

Log lines are RAW (the core masks them) and never logged by the adapter.

How the steps above are carried out:

* **Window.** A missing bound is derived from the other one with the longest Lambda
  invocation (15 minutes, :data:`MAX_INVOCATION_MS`), and the window never starts more than
  15 minutes before the failure (an attempt's start may be the execution's).
* **Exact.** The request-id search finds the invocation's log stream and its ``START`` ...
  ``REPORT`` span; that span of the stream is then read whole (``logStreamNames``), because the
  Python runtime's unhandled-error event (``[ERROR] KeyError: ... Traceback ...``) and plain
  ``print`` output carry no request id in the Text format [U]. The excerpt runs from ``START``
  to ``REPORT``. When no event carries the request id (yet), the window search below runs.
* **Window search.** Every event of the window is read once. Exactly one invocation started in
  the window (one ``START RequestId:`` line): its request id is the attempt's, confidence
  ``exact``. Otherwise the error line closest to ``failed_at`` picks the stream and the
  invocation around it, confidence ``heuristic``; lines written before the failure (within
  :data:`CLOCK_SLACK_MS`) win over later ones, because a function logs its error before Step
  Functions records the failure. Without any error line nothing is tied to the attempt (an
  empty bundle with a note).
* **Paging.** At most :data:`MAX_FILTER_PAGES` pages and :data:`MAX_SCANNED_EVENTS` events per
  search; empty pages that still carry a ``nextToken`` are followed.
* **Errors.** A missing log group, a refused or an invalid call give an empty bundle whose note
  names the log group and the AWS error code (never its message); transient errors propagate.
* **Notes.** :data:`LATE_LOGS_NOTE` is added to an empty bundle when the failure happened less
  than :data:`LATE_LOGS_WINDOW` ago (or when its time is unknown).
* **Text rendering.** ``\\r`` line breaks (the runtime's multi-line error events) become lines
  and no-break-space indentation becomes spaces, so tracebacks parse. JSON records render as
  ``[LEVEL]\\t<timestamp>\\t<requestId>\\t<message>``, platform records (``platform.start``,
  ``platform.runtimeDone``, ``platform.report``) as the Text format's ``START``/``END``/
  ``REPORT`` lines.
"""

from __future__ import annotations

import json
import logging
import re
from collections.abc import Iterable, Mapping, Sequence
from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import Any, Literal

from dag_healer.core.errors import NotFound, OrchestratorError, PermissionDenied
from dag_healer.core.evidence import LogBundle
from dag_healer.core.tracebacks import TRACEBACK_HEADER
from dag_healer.stepfunctions import arns
from dag_healer.stepfunctions.aws import call, error_code
from dag_healer.stepfunctions.causes import CauseInfo, parse_payload, render_traceback
from dag_healer.stepfunctions.runtime import SfnRuntime, epoch_ms, iso, to_utc

logger = logging.getLogger(__name__)

MAX_LOG_LINES = 200
WINDOW_BEFORE_MS = 1000
WINDOW_AFTER_MS = 5000
LATE_LOGS_NOTE = "Lambda logs can take 5 to 10 minutes to reach CloudWatch: logs may not be delivered yet."
#: The longest Lambda invocation (15 minutes): the failed invocation started within it.
MAX_INVOCATION_MS = 15 * 60 * 1000
#: An empty answer this soon after the failure may be late logs (:data:`LATE_LOGS_NOTE`).
LATE_LOGS_WINDOW = timedelta(minutes=15)
#: Clock slack between Lambda log timestamps and Step Functions events: an error line more than
#: this after the failure belongs to another invocation more likely than to the failed one.
CLOCK_SLACK_MS = 1000
FILTER_PAGE_LIMIT = 1000
MAX_FILTER_PAGES = 10
MAX_SCANNED_EVENTS = 5000
SOURCE_PREFIX = "cloudwatch:"
LOOKUP_DISABLED_NOTE = "CloudWatch log lookup is disabled (options.log_lookup: false)."

Confidence = Literal["exact", "heuristic"]
CONFIDENCE_EXACT: Confidence = "exact"
CONFIDENCE_HEURISTIC: Confidence = "heuristic"

_START = re.compile(r"^START RequestId: (?P<id>\S+)")
_END = re.compile(r"^END RequestId: (?P<id>\S+)")
_REPORT = re.compile(r"^REPORT RequestId: (?P<id>\S+)")
_ERROR_TEXT = re.compile(
    r"\[(?:ERROR|CRITICAL|FATAL)\]|Traceback \(most recent call last\):|Task timed out|\tERROR\t"
)
_ERROR_LEVELS = frozenset({"ERROR", "CRITICAL", "FATAL"})
_LEVEL_KEYS = ("level", "log_level", "levelname")
_REQUEST_ID = re.compile(r"^[0-9A-Za-z][0-9A-Za-z-]{0,127}$")
_RUNTIME_ERROR = re.compile(r"^\[ERROR\] (?P<type>[A-Za-z_][\w.]*)(?::[ \t]?(?P<message>.*))?$")
_LINE_BREAK = re.compile(r"\r\n|\r|\n")
_PLATFORM_MARKERS = {
    "platform.start": "start",
    "platform.runtimeDone": "end",
    "platform.end": "end",
    "platform.report": "report",
}
_REPORT_METRICS = (
    ("durationMs", "Duration", "ms"),
    ("billedDurationMs", "Billed Duration", "ms"),
    ("memorySizeMB", "Memory Size", "MB"),
    ("maxMemoryUsedMB", "Max Memory Used", "MB"),
    ("initDurationMs", "Init Duration", "ms"),
)
_FAILED_REPORT_STATUSES = frozenset({"timeout", "error", "failure"})


@dataclass(frozen=True)
class _Event:
    """One CloudWatch log event, parsed once."""

    order: int  # position in the answer (keeps equal timestamps in delivery order)
    timestamp: int  # epoch ms
    stream: str
    message: str
    record: Mapping[str, Any] | None  # a Lambda JSON-format record, else None
    marker: str | None  # "start" | "end" | "report": an invocation boundary
    request_id: str | None  # the request id a boundary line or a JSON record names
    error: bool  # [ERROR] / Traceback / Task timed out line, or a JSON record of level ERROR


@dataclass(frozen=True)
class _Window:
    """The searched time range (epoch ms, inclusive) and the failure time inside it."""

    start: int
    end: int
    failed: int


@dataclass(frozen=True)
class _Scan:
    """The events of one paged ``FilterLogEvents`` search, chronological."""

    events: tuple[_Event, ...]
    complete: bool  # False: the page or event cap stopped the search


@dataclass(frozen=True)
class _Found:
    """The events tied to the failed attempt (possibly none) and why."""

    events: tuple[_Event, ...]
    confidence: Confidence
    notes: tuple[str, ...]


# =========================================================================== public helpers


def default_log_group(function_name: str) -> str:
    """``/aws/lambda/<function name>`` (an ARN or qualified name is reduced to the name)."""
    name = _function_name(function_name)
    if name is None:
        raise ValueError(f"not a Lambda function name: {function_name!r}")
    return f"/aws/lambda/{name}"


def render_log_events(events: Sequence[Mapping[str, Any]]) -> list[str]:
    """``FilterLogEvents`` events (text or JSON format) as text lines, chronological."""
    parsed = [_parse_event(raw, order) for order, raw in enumerate(events) if isinstance(raw, Mapping)]
    return _render(sorted(parsed, key=_chronological))


def find_lambda_logs(
    rt: SfnRuntime,
    *,
    function: str,
    started_at: datetime | None,
    failed_at: datetime | None,
    request_id: str | None = None,
    max_chars: int = 2_000_000,
    log_group: str | None = None,
) -> LogBundle:
    """The CloudWatch log excerpt of one failed Lambda attempt (never raises for missing logs;
    transient AWS errors may propagate as transient :mod:`dag_healer.core.errors`).

    ``log_group`` is the group the template names for the function (``LoggingConfig.LogGroup``):
    ``GetFunction`` still decides, and the hint is used when it cannot tell.
    """
    if not rt.options.log_lookup:
        return empty_bundle(LOOKUP_DISABLED_NOTE)
    name = _function_name(function)
    if name is None:
        return empty_bundle(
            f"{function or 'The step'} is not a Lambda function: there is no log to correlate."
        )
    window = _window(started_at, failed_at)
    if window is None:
        return empty_bundle("The failed attempt has no timestamps: its CloudWatch log window is unknown.")
    group = _log_group(rt, function.strip(), name, known=_usable_log_group(log_group))
    try:
        found = _search(rt, group, window, _usable_request_id(request_id))
    except OrchestratorError as exc:
        if exc.transient:
            raise
        logger.debug("dag-healer stepfunctions: reading log group %s failed: %s", group, type(exc).__name__)
        return empty_bundle(
            _error_note(group, exc), *_late_note(rt, failed_at, missing=isinstance(exc, NotFound))
        )
    bundle = _bundle(group, found, max_chars)
    logger.debug(
        "dag-healer stepfunctions: log excerpt of %s: %d events, confidence %s",
        group,
        len(found.events),
        bundle.confidence,
    )
    if bundle.text:
        return bundle
    return empty_bundle(*bundle.notes, *_late_note(rt, failed_at, missing=True))


def empty_bundle(*notes: str) -> LogBundle:
    """``LogBundle(text="", source="none", confidence="none", notes=notes)``."""
    return LogBundle(text="", source="none", confidence="none", notes=tuple(note for note in notes if note))


# =========================================================================== the search


def _function_name(function: Any) -> str | None:
    """The function name of a Lambda name or ARN (``None`` for anything else)."""
    if not isinstance(function, str):
        return None
    try:
        name = arns.lambda_function_name(function)
    except ValueError:
        return None
    return name or None


def _window(started_at: datetime | None, failed_at: datetime | None) -> _Window | None:
    """The searched range of an attempt (see the module docstring); ``None`` without times."""
    start = epoch_ms(started_at)
    failed = epoch_ms(failed_at)
    if failed is None:
        if start is None:
            return None
        failed = start + MAX_INVOCATION_MS
    earliest = failed - MAX_INVOCATION_MS
    start = earliest if start is None else min(max(start, earliest), failed)
    return _Window(start=start - WINDOW_BEFORE_MS, end=failed + WINDOW_AFTER_MS, failed=failed)


def _usable_log_group(value: Any) -> str | None:
    """A log group name (``None`` for blanks or text with whitespace)."""
    if not isinstance(value, str) or not value.strip() or any(c.isspace() for c in value.strip()):
        return None
    return value.strip()


def _log_group(rt: SfnRuntime, function: str, name: str, *, known: str | None = None) -> str:
    """``LoggingConfig.LogGroup`` of ``GetFunction``, else ``known``, else ``/aws/lambda/<name>``
    (``known`` is used when ``GetFunction`` fails; its answer always wins)."""
    default = f"/aws/lambda/{name}"
    try:
        response = call(rt.clients.lambda_, "get_function", what="GetFunction", FunctionName=function)
    except OrchestratorError as exc:
        fallback = known or default
        logger.debug(
            "dag-healer stepfunctions: GetFunction of %s failed (%s): using %s",
            name,
            type(exc).__name__,
            fallback,
        )
        return fallback
    configuration = response.get("Configuration")
    logging_config = configuration.get("LoggingConfig") if isinstance(configuration, Mapping) else None
    group = logging_config.get("LogGroup") if isinstance(logging_config, Mapping) else None
    return group.strip() if isinstance(group, str) and group.strip() else default


def _usable_request_id(request_id: str | None) -> str | None:
    """A request id safe to quote in a filter pattern (UUID-like), else ``None``."""
    if isinstance(request_id, str) and _REQUEST_ID.match(request_id.strip()):
        return request_id.strip()
    return None


def _search(rt: SfnRuntime, group: str, window: _Window, request_id: str | None) -> _Found:
    """The events tied to the attempt: by request id first, else by the window search."""
    notes: tuple[str, ...] = ()
    if request_id is not None:
        found = _by_request_id(rt, group, window, request_id)
        if found is not None:
            return found
        notes = (
            f"No log event of {group} carries the request id {request_id}: the log was searched by time.",
        )
    found = _by_window(rt, group, window)
    return _Found(found.events, found.confidence, notes + found.notes)


def _by_request_id(rt: SfnRuntime, group: str, window: _Window, request_id: str) -> _Found | None:
    """The invocation of ``request_id`` (``None`` when no event of the window carries it)."""
    matches = _scan(rt, group, window, pattern=f'"{request_id}"').events
    if not matches:
        return None
    start = next((e for e in matches if e.marker == "start" and e.request_id == request_id), None)
    anchor = start or matches[0]
    stream = anchor.stream
    end = next(
        (e for e in matches if e.marker == "report" and e.request_id == request_id and e.stream == stream),
        None,
    )
    events: tuple[_Event, ...] = ()
    if stream:
        span = _Window(start=anchor.timestamp, end=end.timestamp if end else window.end, failed=window.failed)
        events = tuple(e for e in _scan(rt, group, span, streams=(stream,)).events if e.stream == stream)
    if not events:
        events = tuple(e for e in matches if e.stream == stream)
    excerpt = _invocation(events, _anchor_index(events, request_id))
    where = f" (log stream {stream})" if stream else ""
    return _Found(excerpt, CONFIDENCE_EXACT, (f"Correlated by the Lambda request id {request_id}{where}.",))


def _by_window(rt: SfnRuntime, group: str, window: _Window) -> _Found:
    """The window search (see the module docstring)."""
    scan = _scan(rt, group, window)
    events = scan.events
    if not events:
        between = f"between {_iso(window.start)} and {_iso(window.end)}"
        return _Found((), CONFIDENCE_HEURISTIC, (f"No CloudWatch log event of {group} was found {between}.",))
    starts = {e.request_id for e in events if e.marker == "start" and e.request_id}
    if scan.complete and len(starts) == 1:
        request_id = next(iter(starts))
        start = next(e for e in events if e.marker == "start" and e.request_id == request_id)
        stream_events = tuple(e for e in events if e.stream == start.stream)
        note = (
            "Correlated by the only Lambda invocation that started during the failed attempt "
            f"(request id {request_id}, log stream {start.stream})."
        )
        return _Found(_invocation(stream_events, _index_of(stream_events, start)), CONFIDENCE_EXACT, (note,))
    errors = [e for e in events if e.error]
    if not errors:
        return _Found(
            (),
            CONFIDENCE_HEURISTIC,
            (
                f"The CloudWatch log events of {group} around the failure have no error line "
                f"(invocations started in the window: {len(starts)}): none was tied to the failed attempt.",
            ),
        )
    best = min(errors, key=lambda e: _closeness(e, window.failed))
    stream_events = tuple(e for e in events if e.stream == best.stream)
    if not scan.complete and best.stream:  # the capped search may have missed lines of the stream
        reread = tuple(
            e for e in _scan(rt, group, window, streams=(best.stream,)).events if e.stream == best.stream
        )
        same = next((e for e in reread if (e.timestamp, e.message) == (best.timestamp, best.message)), None)
        if same is not None:
            best, stream_events = same, reread
    note = (
        "No request id ties the log to the failed attempt: the log stream "
        f"{best.stream or '(unnamed)'} has the error line closest to the failure (heuristic)."
    )
    return _Found(_invocation(stream_events, _index_of(stream_events, best)), CONFIDENCE_HEURISTIC, (note,))


def _scan(
    rt: SfnRuntime,
    group: str,
    window: _Window,
    *,
    pattern: str | None = None,
    streams: Iterable[str] = (),
) -> _Scan:
    """One paged ``FilterLogEvents`` search (see :data:`MAX_FILTER_PAGES`)."""
    params: dict[str, Any] = {
        "logGroupName": group,
        "startTime": window.start,
        "endTime": window.end,
        "limit": FILTER_PAGE_LIMIT,
    }
    if pattern:
        params["filterPattern"] = pattern
    names = list(streams)
    if names:
        params["logStreamNames"] = names
    events: list[_Event] = []
    token: str | None = None
    complete = False
    for _page in range(MAX_FILTER_PAGES):
        request = {**params, "nextToken": token} if token else params
        response = call(rt.clients.logs, "filter_log_events", what="FilterLogEvents", **request)
        answer = response.get("events")
        for raw in answer if isinstance(answer, list) else ():
            if isinstance(raw, Mapping):
                events.append(_parse_event(raw, len(events)))
        next_token = response.get("nextToken")
        token = next_token if isinstance(next_token, str) and next_token else None
        if token is None:
            complete = True
            break
        if len(events) >= MAX_SCANNED_EVENTS:
            break
    return _Scan(tuple(sorted(events, key=_chronological)), complete)


def _chronological(event: _Event) -> tuple[int, int]:
    return (event.timestamp, event.order)


def _closeness(event: _Event, failed: int) -> tuple[bool, int, int, int]:
    """Sort key of error lines: not after the failure (with slack) first, then the nearest, then
    the latest."""
    late = event.timestamp > failed + CLOCK_SLACK_MS
    return (late, abs(event.timestamp - failed), -event.timestamp, -event.order)


def _index_of(events: Sequence[_Event], wanted: _Event) -> int:
    return next((index for index, event in enumerate(events) if event is wanted), 0)


def _anchor_index(events: Sequence[_Event], request_id: str) -> int:
    """The ``START`` line of ``request_id``, else the first event naming it, else 0."""
    for index, event in enumerate(events):
        if event.marker == "start" and event.request_id == request_id:
            return index
    for index, event in enumerate(events):
        if event.request_id == request_id or request_id in event.message:
            return index
    return 0


def _invocation(events: Sequence[_Event], anchor: int) -> tuple[_Event, ...]:
    """The invocation around ``events[anchor]`` in one stream: from its ``START`` (or the line
    after the previous invocation's end) to its ``REPORT`` (or the line before the next
    ``START``). Lambda runs one invocation at a time per log stream."""
    if not events:
        return ()
    anchor = min(max(anchor, 0), len(events) - 1)
    first = 0
    for index in range(anchor, -1, -1):
        marker = events[index].marker
        if marker == "start":
            first = index
            break
        if marker in ("end", "report") and index < anchor:
            first = index + 1
            break
    last = len(events) - 1
    for index in range(anchor, len(events)):
        marker = events[index].marker
        if marker == "report":
            last = index
            break
        if marker == "start" and index > anchor:
            last = index - 1
            break
    return tuple(events[first : last + 1])


def _bundle(group: str, found: _Found, max_chars: int) -> LogBundle:
    """Render the found events, keep their end (200 lines, ``max_chars``) and add the notes."""
    lines = _render(found.events)
    notes = list(found.notes)
    truncated = False
    if len(lines) > MAX_LOG_LINES:
        lines = lines[-MAX_LOG_LINES:]
        truncated = True
        notes.append(f"The log excerpt keeps the last {MAX_LOG_LINES} lines.")
    text = "\n".join(lines)
    if 0 < max_chars < len(text):
        text = _tail(text, max_chars)
        truncated = True
        notes.append(f"The log excerpt was cut to its last {max_chars} characters.")
    if not text.strip():
        return empty_bundle(*found.notes)
    return LogBundle(
        text=text,
        source=f"{SOURCE_PREFIX}{group}",
        confidence=found.confidence,
        truncated=truncated,
        notes=tuple(notes),
    )


def _tail(text: str, max_chars: int) -> str:
    """The last ``max_chars`` characters, starting at a line boundary when one is inside."""
    cut = text[-max_chars:]
    if text[-max_chars - 1] != "\n":
        newline = cut.find("\n")
        if 0 <= newline < len(cut) - 1:
            cut = cut[newline + 1 :]
    return cut


def _error_note(group: str, exc: OrchestratorError) -> str:
    """The note of a non-transient ``FilterLogEvents`` error (the AWS code, never its message)."""
    label = error_code(exc.__cause__) if exc.__cause__ is not None else None
    label = label or type(exc).__name__
    if isinstance(exc, NotFound):
        return f"The CloudWatch log group {group} does not exist ({label})."
    if isinstance(exc, PermissionDenied):
        return (
            f"Reading the CloudWatch log group {group} was refused ({label}): logs:FilterLogEvents is needed."
        )
    return f"Reading the CloudWatch log group {group} failed ({label})."


def _late_note(rt: SfnRuntime, failed_at: datetime | None, *, missing: bool) -> tuple[str, ...]:
    """``(LATE_LOGS_NOTE,)`` when missing logs may still be on their way."""
    if not missing:
        return ()
    moment = to_utc(failed_at)
    if moment is None or rt.now() - moment <= LATE_LOGS_WINDOW:
        return (LATE_LOGS_NOTE,)
    return ()


def _iso(ms: int) -> str:
    return iso(ms) or str(ms)


# =========================================================================== parsing events


def _parse_event(raw: Mapping[str, Any], order: int) -> _Event:
    """One ``FilterLogEvents`` event (odd fields are tolerated: missing text is ``""``)."""
    message = raw.get("message")
    text = message if isinstance(message, str) else ("" if message is None else str(message))
    stream = raw.get("logStreamName")
    record = _json_record(text)
    if record is not None:
        marker, request_id = _record_marker(record)
        error = _record_is_error(record)
    else:
        marker, request_id = _text_marker(text)
        error = bool(_ERROR_TEXT.search(text))
    return _Event(
        order=order,
        timestamp=_as_ms(raw.get("timestamp")),
        stream=stream if isinstance(stream, str) else "",
        message=text,
        record=record,
        marker=marker,
        request_id=request_id,
        error=error,
    )


def _as_ms(value: Any) -> int:
    """Epoch milliseconds of a CloudWatch ``timestamp`` (0 when unusable)."""
    if isinstance(value, bool):
        return 0
    if isinstance(value, int):
        return value
    if isinstance(value, float) and value == value:
        return int(value)
    return epoch_ms(value) or 0


def _json_record(text: str) -> Mapping[str, Any] | None:
    """The Lambda JSON-format record a message holds (application or platform), else ``None``."""
    stripped = text.strip()
    if not stripped.startswith("{"):
        return None
    try:
        record = json.loads(stripped)
    except ValueError:
        return None
    if not isinstance(record, dict):
        return None
    kind = record.get("type")
    if isinstance(kind, str) and kind.startswith("platform."):
        return record
    if any(key in record for key in (*_LEVEL_KEYS, "errorType", "errorMessage")):
        return record
    return None  # some other JSON the function printed: kept as text


def _record_marker(record: Mapping[str, Any]) -> tuple[str | None, str | None]:
    kind = record.get("type")
    body = record.get("record")
    if isinstance(kind, str) and kind in _PLATFORM_MARKERS:
        request_id = body.get("requestId") if isinstance(body, Mapping) else None
        return _PLATFORM_MARKERS[kind], request_id if isinstance(request_id, str) and request_id else None
    request_id = record.get("requestId")
    return None, request_id if isinstance(request_id, str) and request_id else None


def _text_marker(text: str) -> tuple[str | None, str | None]:
    for marker, pattern in (("start", _START), ("end", _END), ("report", _REPORT)):
        match = pattern.match(text)
        if match:
            return marker, match.group("id")
    return None, None


def _record_level(record: Mapping[str, Any]) -> str:
    for key in _LEVEL_KEYS:
        value = record.get(key)
        if isinstance(value, str) and value.strip():
            return value.strip().upper()
    return ""


def _record_error(record: Mapping[str, Any]) -> tuple[CauseInfo | None, Any, bool]:
    """``(error, raw stack, nested)`` of a record: its top-level error fields, else those of a
    ``message`` object (``nested=True``); ``(None, None, False)`` without error fields."""
    message = record.get("message")
    for source, nested in ((record, False), (message, True)):
        if isinstance(source, Mapping):
            info = parse_payload(source)
            if info is not None:
                stack = source.get("stackTrace")
                return info, stack if stack is not None else source.get("trace"), nested
    return None, None, False


def _record_is_error(record: Mapping[str, Any]) -> bool:
    kind = record.get("type")
    if isinstance(kind, str) and kind.startswith("platform."):
        body = record.get("record")
        status = body.get("status") if isinstance(body, Mapping) else None
        return isinstance(status, str) and status.lower() in _FAILED_REPORT_STATUSES
    return _record_level(record) in _ERROR_LEVELS or _record_error(record)[0] is not None


# =========================================================================== rendering


def _render(events: Iterable[_Event]) -> list[str]:
    lines: list[str] = []
    for event in events:
        if event.record is not None:
            lines.extend(_record_lines(event.record, event.timestamp))
        else:
            lines.extend(_text_lines(event.message))
    return lines


def _text_lines(message: str) -> list[str]:
    """A text event as lines (``\\r`` breaks split, no-break-space indentation made plain)."""
    text = message.rstrip("\r\n")
    if not text.strip():
        return []
    return _closed_runtime_error([_plain_indent(line).rstrip() for line in _LINE_BREAK.split(text)])


def _closed_runtime_error(lines: list[str]) -> list[str]:
    """Close the Python runtime's unhandled-error event like CPython does.

    The runtime prints ``[ERROR] <Type>: <message>``, then ``Traceback (most recent call
    last):`` and the frames, without the closing ``<Type>: <message>`` line: it is appended
    (with a blank line, which ends the exception message for the parser) so the traceback
    parses. An event that already ends with a line of its own is left alone.
    """
    match = _RUNTIME_ERROR.match(lines[0]) if lines else None
    if match is None or TRACEBACK_HEADER not in lines:
        return lines
    header = lines.index(TRACEBACK_HEADER)
    if any(line.strip() and not line[:1].isspace() for line in lines[header + 1 :]):
        return lines
    message = "\n".join([match.group("message") or "", *lines[1:header]]).strip()
    closing = f"{match.group('type')}: {message}" if message else match.group("type")
    return [*lines, *closing.split("\n"), ""]


def _plain_indent(line: str) -> str:
    stripped = line.lstrip(" \t\u00a0")
    return line[: len(line) - len(stripped)].replace("\u00a0", " ") + stripped


def _record_lines(record: Mapping[str, Any], timestamp: int) -> list[str]:
    """A JSON-format record as text lines (see the module docstring)."""
    kind = record.get("type")
    if isinstance(kind, str) and kind.startswith("platform."):
        return [_platform_line(kind, record.get("record"))]
    info, stack, nested = _record_error(record)
    message = record.get("message")
    if isinstance(message, str):
        text = message
    elif message is None or nested:
        text = ""
    else:
        text = _compact(message)
    if not text.strip() and info is not None:
        text = f"{info.error_type}: {info.error_message}" if info.error_message else info.error_type
    when = _first_text(record.get("timestamp"), record.get("time")) or _iso_z(timestamp)
    request_id = _first_text(record.get("requestId"))
    level = _record_level(record)
    body = _text_lines(text) or [""]
    head = "\t".join(part for part in (f"[{level}]" if level else "", when, request_id, body[0]) if part)
    lines = [head, *body[1:]]
    if info is not None:
        lines.extend(_error_lines(info, stack))
    return lines


def _error_lines(info: CauseInfo, stack: Any) -> list[str]:
    """A Python error as a CPython traceback; another runtime's stack lines as they are."""
    foreign = isinstance(stack, list) and any(isinstance(item, str) and item.strip() for item in stack)
    if info.frames or not foreign:
        return [*render_traceback(info).splitlines(), ""]  # the blank line ends the exception message
    lines: list[str] = []
    for item in stack:
        if isinstance(item, str):
            lines.extend(_plain_indent(line).rstrip() for line in _LINE_BREAK.split(item.rstrip("\r\n")))
    return lines


def _platform_line(kind: str, body: Any) -> str:
    """A platform record as the Text format's ``START``/``END``/``REPORT`` line."""
    record = body if isinstance(body, Mapping) else {}
    request_id = _first_text(record.get("requestId"))
    status = _first_text(record.get("status"))
    marker = _PLATFORM_MARKERS.get(kind)
    if marker == "start":
        version = _first_text(record.get("version"))
        return f"START RequestId: {request_id}" + (f" Version: {version}" if version else "")
    if marker == "end":
        suffix = f" Status: {status}" if status and status != "success" else ""
        return f"END RequestId: {request_id}{suffix}"
    if marker == "report":
        parts = [f"REPORT RequestId: {request_id}"]
        metrics = record.get("metrics")
        if isinstance(metrics, Mapping):
            for key, label, unit in _REPORT_METRICS:
                value = metrics.get(key)
                if isinstance(value, (int, float)) and not isinstance(value, bool):
                    parts.append(f"{label}: {value} {unit}")
        if status and status != "success":
            parts.append(f"Status: {status}")
        return "\t".join(parts)
    return f"{kind} {_compact(record)}" if record else kind


def _first_text(*values: Any) -> str:
    for value in values:
        if isinstance(value, str) and value.strip():
            return value.strip()
    return ""


def _compact(value: Any) -> str:
    try:
        return json.dumps(value, ensure_ascii=False, sort_keys=True, default=str)
    except (TypeError, ValueError):
        return str(value)


def _iso_z(ms: int) -> str:
    """``2026-10-02T06:00:41.512Z`` (the Lambda log timestamp format); ``""`` for 0."""
    if not ms:
        return ""
    text = iso(ms)
    return text.replace("+00:00", "Z") if text else ""


__all__ = [
    "CLOCK_SLACK_MS",
    "CONFIDENCE_EXACT",
    "CONFIDENCE_HEURISTIC",
    "FILTER_PAGE_LIMIT",
    "LATE_LOGS_NOTE",
    "LATE_LOGS_WINDOW",
    "LOOKUP_DISABLED_NOTE",
    "MAX_FILTER_PAGES",
    "MAX_INVOCATION_MS",
    "MAX_LOG_LINES",
    "MAX_SCANNED_EVENTS",
    "SOURCE_PREFIX",
    "WINDOW_AFTER_MS",
    "WINDOW_BEFORE_MS",
    "default_log_group",
    "empty_bundle",
    "find_lambda_logs",
    "render_log_events",
]
