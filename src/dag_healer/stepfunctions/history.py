"""Localise the failing state of an execution from its history (pure, table-tested).

:func:`localize` implements section 9.5 of the design on the events of one execution (any
order; they are sorted by ``id`` and de-duplicated). Only one redrive segment counts: the
latest one, or the one of ``redrive_count`` (:func:`segment`). The rules:

1. The terminal event is the last ``ExecutionFailed``, ``ExecutionTimedOut`` or
   ``ExecutionAborted`` of the segment.
2. The terminal step failure is the last event of a :data:`STEP_FAILURE_TYPES` type that is
   neither followed by another ``*Scheduled`` event of the same state entry (a ``Retry``) nor
   by a ``*StateExited`` event of that state, or of a Map/Parallel state around it (a
   ``Catch``), within the segment. When the terminal event's ``previousEventId`` chain reaches a
   step failure through the events that carry a failure up (:data:`PROPAGATION_TYPES`), that
   failure wins: it is what ended the execution, even if a concurrent branch failed later.
3. Its enclosing state: follow ``previousEventId`` to the nearest ``*StateEntered``
   (``stateEnteredEventDetails.name``); entry events may point to 0, so fall back to the nearest
   preceding ``*StateEntered`` of a fitting type (a Task state for ``Task*``/``LambdaFunction*``/
   ``Activity*`` events, a Map state for ``MapRunFailed``) without a matching ``*StateExited``.
4. Inline Map: a ``MapIterationFailed{index, name}`` on the path gives ``map_index = index``;
   ``step_id`` is the inner failing state and ``step_path = "<Map>[<index>]/<state>"``. The Map
   and Parallel states around a state are found by walking ``previousEventId`` up from its entry
   (``MapIterationStarted`` names the iteration; a sibling state is skipped from its exit to its
   entry); a Parallel state appears as ``"<Parallel>/<state>"`` and nested states join their
   parts, outermost first. When the walk ends early (entries pointing to 0), the
   ``MapIterationFailed`` events between the failure and the terminal event name the iteration.
5. Distributed Map: ``MapRunFailed`` gives ``step_id = <Map state>``, ``map_index = -1`` and
   ``map_run_arn`` from ``MapRunStarted``.
6. ``EvaluationFailed`` gives ``step_id = evaluationFailedEventDetails.state`` (a definition
   error, ``event_type = "EvaluationFailed"``).
7. A terminal event without any step failure (an execution timeout or abort) gives
   :data:`~dag_healer.core.ids.RUN_LEVEL_STEP`. An ``ExecutionFailed`` without any step failure
   event (a JSONPath ``States.Runtime`` error, ``States.NoChoiceMatched`` ...) is localised to
   the state still open when the execution failed (``event_type = "ExecutionFailed"``), and is
   run-level when no state was open.
8. ``retry_attempts`` = the ``*Scheduled`` events of that state entry in the segment minus 1
   (``MapRunStarted`` events for a Distributed Map); ``resource``, ``resource_type`` and
   ``function_name`` come from the ``*Scheduled``/``*Failed`` details (the ``FunctionName`` of an
   optimised Lambda Task's ``parameters`` is read in memory; nothing else of them is kept).

A ``FailStateEntered`` failure has no error of its own: its error and cause are the terminal
event's (the Fail state's ``Error``/``Cause``). A run-level result names the terminal event
(``failed_event_id``, ``event_type``). A step failure whose state entry is not among the events
(a redrive that resumed a state without entering it again [U]) is run-level with
``missing_entry=True``, so a caller can retry with the events of the earlier segment too.

Optimised ``lambda:invoke`` Tasks emit ``Task*`` events (``resourceType: lambda``,
``resource: invoke``); legacy Lambda-ARN Tasks emit ``LambdaFunction*`` events (the
``resource`` is the function ARN). Both are handled. Timestamps may be datetimes (botocore),
ISO text (fixtures) or epoch numbers. Nothing here does I/O, and a :class:`Localized` never
holds execution input or output.
"""

from __future__ import annotations

import bisect
import json
from collections.abc import Iterable, Mapping, Sequence
from dataclasses import dataclass, field
from datetime import datetime
from typing import Any

from dag_healer.core.ids import NO_MAP_INDEX, RUN_LEVEL_STEP
from dag_healer.stepfunctions.runtime import to_utc

TERMINAL_EVENT_TYPES: tuple[str, ...] = ("ExecutionFailed", "ExecutionTimedOut", "ExecutionAborted")
STEP_FAILURE_TYPES: tuple[str, ...] = (
    "TaskFailed",
    "TaskTimedOut",
    "TaskStartFailed",
    "TaskSubmitFailed",
    "LambdaFunctionFailed",
    "LambdaFunctionTimedOut",
    "LambdaFunctionStartFailed",
    "LambdaFunctionScheduleFailed",
    "ActivityFailed",
    "ActivityTimedOut",
    "ActivityScheduleFailed",
    "MapRunFailed",
    "EvaluationFailed",
    "FailStateEntered",
)
SCHEDULED_TYPES: tuple[str, ...] = ("TaskScheduled", "LambdaFunctionScheduled", "ActivityScheduled")
STARTED_TYPES: tuple[str, ...] = ("TaskStarted", "LambdaFunctionStarted", "ActivityStarted")
#: Events that carry an inner failure up to its Map/Parallel state and on to the execution.
PROPAGATION_TYPES: tuple[str, ...] = ("MapIterationFailed", "MapStateFailed", "ParallelStateFailed")
EXECUTION_STARTED = "ExecutionStarted"
EXECUTION_REDRIVEN = "ExecutionRedriven"

_ENTERED = "StateEntered"
_EXITED = "StateExited"
_EXECUTION_FAILED = "ExecutionFailed"
_EXECUTION_SUCCEEDED = "ExecutionSucceeded"
_FAIL_STATE_ENTERED = "FailStateEntered"
_EVALUATION_FAILED = "EvaluationFailed"
_MAP_RUN_FAILED = "MapRunFailed"
_MAP_RUN_STARTED = frozenset({"MapRunStarted"})
_MAP_ITERATION_STARTED = "MapIterationStarted"
_MAP_ITERATION_FAILED = "MapIterationFailed"
_TASK_ENTRIES = frozenset({"TaskStateEntered"})
_MAP_ENTRIES = frozenset({"MapStateEntered"})
_CONTAINERS = {"MapStateEntered": "map", "ParallelStateEntered": "parallel"}
_FAILURE_TYPES = frozenset(STEP_FAILURE_TYPES)
_SCHEDULED = frozenset(SCHEDULED_TYPES)
_STARTED = frozenset(STARTED_TYPES)
_TERMINAL = frozenset(TERMINAL_EVENT_TYPES)
_PROPAGATION = frozenset(PROPAGATION_TYPES)
#: A walk from an event towards its state entry stops here: past these it would be in another
#: state (an iteration or branch boundary, the end of a Map/Parallel state whose link points into
#: its children, or the execution itself).
_BOUNDARY_TYPES = frozenset(
    {
        _MAP_ITERATION_STARTED,
        "MapIterationSucceeded",
        _MAP_ITERATION_FAILED,
        "MapIterationAborted",
        "MapStateSucceeded",
        "MapStateFailed",
        "MapStateAborted",
        "ParallelStateStarted",
        "ParallelStateSucceeded",
        "ParallelStateFailed",
        "ParallelStateAborted",
        EXECUTION_STARTED,
        EXECUTION_REDRIVEN,
        _EXECUTION_SUCCEEDED,
        *TERMINAL_EVENT_TYPES,
    }
)
#: Epoch timestamps below this value are seconds (the HistoryEvent wire format), larger ones are
#: milliseconds (1e11 ms is in 1973, 1e11 s thousands of years ahead).
_EPOCH_SECONDS_LIMIT = 10**11
_MAX_PARAMETERS_CHARS = 1024 * 1024


@dataclass(frozen=True)
class Localized:
    """Where and how one execution (redrive segment) failed. Text fields are RAW."""

    step_id: str  # innermost failing state, or RUN_LEVEL_STEP
    map_index: int = NO_MAP_INDEX  # inline Map iteration index, else -1
    redrive_count: int = 0  # the segment that was localised
    error: str | None = None  # error name of the step failure (else of the terminal event)
    cause: str | None = None  # cause of the step failure (else of the terminal event); may be Lambda JSON
    event_type: str | None = None  # type of the step failure event ("TaskFailed" ...), else the terminal type
    failed_event_id: int | None = None
    entered_event_id: int | None = None
    terminal_event_type: str | None = None  # ExecutionFailed / ExecutionTimedOut / ExecutionAborted
    terminal_error: str | None = None
    terminal_cause: str | None = None
    retry_attempts: int = 0
    resource: str | None = None  # "invoke" (optimised) or the function ARN (legacy)
    resource_type: str | None = None  # "lambda", "ecs", "glue" ...
    function_name: str | None = None  # Lambda function name/ARN when the events name it
    step_path: str | None = None  # "<Map>[<index>]/<state>" (or "<Parallel>/<state>")
    map_run_arn: str | None = None
    started_at: datetime | None = None  # *Started of the failing attempt
    failed_at: datetime | None = None  # the step failure event (else the terminal event)
    terminal: bool = True  # False: the segment has no terminal event (still running, or cut)
    notes: tuple[str, ...] = field(default_factory=tuple)
    #: True: a step failure was found but not its ``*StateEntered`` (the entry precedes the
    #: events given); the result is then run-level.
    missing_entry: bool = False
    #: False: not read from a history (the RUN_LEVEL_STEP fallback for an unreadable history).
    from_history: bool = True

    @property
    def run_level(self) -> bool:
        """True when no state is named (:data:`~dag_healer.core.ids.RUN_LEVEL_STEP`)."""
        return self.step_id == RUN_LEVEL_STEP

    def native_fields(self) -> dict[str, Any]:
        """The ``native_ref`` fields of this localisation: ids and names, never the cause."""
        return {
            "error": self.error,
            "failed_event_id": self.failed_event_id,
            "entered_event_id": self.entered_event_id,
            "event_type": self.event_type,
            "resource": self.resource,
            "resource_type": self.resource_type,
            "function_name": self.function_name,
            "retry_attempts": self.retry_attempts,
            "step_path": self.step_path,
            "map_run_arn": self.map_run_arn,
            "localized": self.from_history,
        }


# =========================================================================== event parsing


def _detail_keys(kind: Any) -> tuple[str, ...]:
    """The ``*EventDetails`` key an event of type ``kind`` carries (botocore's ``HistoryEvent``)."""
    if not isinstance(kind, str) or not kind:
        return ()
    if kind.endswith(_ENTERED):
        return ("stateEnteredEventDetails",)
    if kind.endswith(_EXITED):
        return ("stateExitedEventDetails",)
    return (kind[0].lower() + kind[1:] + "EventDetails",)


def event_details(event: Mapping[str, Any]) -> Mapping[str, Any]:
    """The single ``*EventDetails`` mapping of a history event (``{}`` when it has none)."""
    if not isinstance(event, Mapping):
        return {}
    for key in _detail_keys(event.get("type")):
        value = event.get(key)
        if isinstance(value, Mapping):
            return value
    for key, value in event.items():
        if isinstance(key, str) and key.endswith("EventDetails") and isinstance(value, Mapping):
            return value
    return {}


def _event_id(value: Any) -> int | None:
    """An event id (an int >= 1; integral floats and digit strings accepted, bools not)."""
    if isinstance(value, bool):
        return None
    if isinstance(value, float) and value.is_integer():
        value = int(value)
    elif isinstance(value, str) and value.strip().isdigit():
        value = int(value.strip())
    return value if isinstance(value, int) and value >= 1 else None


def _count(value: Any, *, minimum: int = 0) -> int | None:
    """A true int >= ``minimum`` (bools excluded), else ``None``."""
    if isinstance(value, bool) or not isinstance(value, int):
        return None
    return value if value >= minimum else None


def _text(value: Any) -> str | None:
    """Non-blank text, unchanged (RAW); ``None`` for anything else."""
    return value if isinstance(value, str) and value.strip() else None


def _event_time(value: Any) -> datetime | None:
    """An aware UTC datetime of an event timestamp (datetime, ISO text, epoch seconds or ms)."""
    if isinstance(value, (int, float)) and not isinstance(value, bool) and abs(value) < _EPOCH_SECONDS_LIMIT:
        return to_utc(value * 1000)
    return to_utc(value)


@dataclass(frozen=True, eq=False)
class _Event:
    """One parsed history event (``previous`` is 0 when it links to nothing usable)."""

    id: int
    previous: int
    type: str
    details: Mapping[str, Any]
    raw: Mapping[str, Any]

    @property
    def is_entry(self) -> bool:
        return self.type.endswith(_ENTERED)

    @property
    def is_exit(self) -> bool:
        return self.type.endswith(_EXITED)

    @property
    def state_name(self) -> str | None:
        return _text(self.details.get("name"))

    @property
    def timestamp(self) -> datetime | None:
        return _event_time(self.raw.get("timestamp"))

    def text(self, key: str) -> str | None:
        return _text(self.details.get(key))


def _parse(events: Iterable[Any] | None) -> list[_Event]:
    """The usable events sorted by id; the first copy of a repeated id wins.

    An event needs an id >= 1 and a type. A ``previousEventId`` that is missing, not an integer
    or not smaller than the event's own id links to nothing, so every walk back ends.
    """
    parsed: dict[int, _Event] = {}
    for raw in events or ():
        if not isinstance(raw, Mapping):
            continue
        event_id = _event_id(raw.get("id"))
        kind = raw.get("type")
        if event_id is None or event_id in parsed or not isinstance(kind, str) or not kind.strip():
            continue
        previous = _event_id(raw.get("previousEventId")) or 0
        parsed[event_id] = _Event(
            id=event_id,
            previous=previous if previous < event_id else 0,
            type=kind.strip(),
            details=event_details(raw),
            raw=raw,
        )
    return [parsed[event_id] for event_id in sorted(parsed)]


# =========================================================================== segments


def _segment_numbers(events: Sequence[_Event]) -> list[int | None]:
    """The redrive segment of every event (``None`` everywhere when the events hold no marker).

    ``ExecutionStarted`` opens segment 0 and ``ExecutionRedriven`` the segment of its
    ``redriveCount`` (one more than the previous segment when the count is missing). Events
    before the first marker belong to the segment before it (the history was read from its end).
    """
    numbers: list[int | None] = []
    current: int | None = None
    for event in events:
        if event.type == EXECUTION_STARTED:
            current = 0
        elif event.type == EXECUTION_REDRIVEN:
            count = _count(event.details.get("redriveCount"), minimum=1)
            current = count if count is not None else (current + 1 if current is not None else 1)
        numbers.append(current)
    first = next((position for position, number in enumerate(numbers) if number is not None), None)
    if first is not None:
        opened = numbers[first] or 0
        earlier = max(opened - 1, 0) if events[first].type == EXECUTION_REDRIVEN else 0
        for position in range(first):
            numbers[position] = earlier
    return numbers


def _wanted(redrive_count: Any) -> int | None:
    """``redrive_count`` checked: ``None`` (the latest segment) or an int >= 0."""
    if redrive_count is None:
        return None
    count = _count(redrive_count)
    if count is None:
        raise ValueError(f"redrive_count must be None or an integer >= 0, not {redrive_count!r}")
    return count


def _select(events: Sequence[_Event], redrive_count: int | None) -> tuple[int | None, list[_Event]]:
    """``(segment number, its events)``; events without any marker are one segment."""
    numbers = _segment_numbers(events)
    if all(number is None for number in numbers):
        return redrive_count, list(events)
    wanted = numbers[-1] if redrive_count is None else redrive_count
    return wanted, [event for event, number in zip(events, numbers, strict=True) if number == wanted]


def segment(events: Sequence[Mapping[str, Any]], redrive_count: int | None = None) -> list[Mapping[str, Any]]:
    """The events of one redrive segment, sorted by id (``None``: the latest segment).

    Segment 0 starts at ``ExecutionStarted``, segment ``n`` at the ``ExecutionRedriven`` event of
    redrive ``n``. Events without any such marker are taken as the segment asked for; a segment
    that is not in the events gives ``[]``. Unusable events (no id or no type) are dropped.
    """
    _number, chosen = _select(_parse(events), _wanted(redrive_count))
    return [event.raw for event in chosen]


# =========================================================================== the event graph


@dataclass(frozen=True)
class _Container:
    """A Map or Parallel state around a failing state (``index``: the Map iteration, if known)."""

    name: str
    kind: str  # "map" | "parallel"
    entry: _Event | None = None
    index: int | None = None

    def label(self) -> str:
        if self.kind == "map" and self.index is not None:
            return f"{self.name}[{self.index}]"
        return self.name


def _iteration(event: _Event) -> tuple[str, int] | None:
    """``(Map name, index)`` of a ``MapIteration*`` event."""
    name, index = event.state_name, _count(event.details.get("index"))
    return (name, index) if name is not None and index is not None else None


class _History:
    """The parsed events of one execution and their ``previousEventId`` links."""

    def __init__(self, events: list[_Event]) -> None:
        self.events = events
        self.ids = [event.id for event in events]
        self.by_id = {event.id: event for event in events}
        self._chain: dict[int, _Event | None] = {}
        self._owners: dict[tuple[int, frozenset[str]], _Event | None] = {}
        self.exit_of: dict[int, _Event] = {}  # entry id -> its *StateExited
        self.entry_of_exit: dict[int, _Event] = {}  # exit id -> the entry it closes
        self._pair_exits()

    def get(self, event_id: int) -> _Event | None:
        return self.by_id.get(event_id) if event_id else None

    def between(self, after: int, before: int) -> list[_Event]:
        """The events with ``after < id < before``, in order."""
        return self.events[bisect.bisect_right(self.ids, after) : bisect.bisect_left(self.ids, before)]

    # ------------------------------------------------------------------ entries and exits
    def chain_entry(self, event: _Event) -> _Event | None:
        """The ``*StateEntered`` that ``previousEventId`` links lead to (``event`` for an entry).

        The walk stops (``None``) at an exit or a boundary event: past those it is in another
        state. Results are remembered for every event passed.
        """
        if event.is_entry:
            return event
        visited: list[int] = []
        current = self.get(event.previous)
        result: _Event | None = None
        while current is not None:
            if current.id in self._chain:
                result = self._chain[current.id]
                break
            if current.is_entry:
                result = current
                break
            if current.is_exit or current.type in _BOUNDARY_TYPES:
                break
            visited.append(current.id)
            current = self.get(current.previous)
        for event_id in visited:
            self._chain[event_id] = result
        return result

    def _pair_exits(self) -> None:
        """Pair every ``*StateExited`` with the entry it closes (by its links, else by name)."""
        waiting: dict[str, dict[int, _Event]] = {}
        for event in self.events:
            name = event.state_name
            if name is None:
                continue
            if event.is_entry:
                waiting.setdefault(name, {})[event.id] = event
                continue
            candidates = waiting.get(name) if event.is_exit else None
            if not candidates:
                continue
            linked = self.chain_entry(event)
            entry = linked if linked is not None and linked.id in candidates else None
            if entry is None:
                entry = next(reversed(candidates.values()))  # the most recent open entry of that name
            del candidates[entry.id]
            self.exit_of[entry.id] = event
            self.entry_of_exit[event.id] = entry

    def is_open_at(self, entry: _Event, event_id: int) -> bool:
        """True when ``entry``'s state had not exited before event ``event_id``."""
        exit_event = self.exit_of.get(entry.id)
        return exit_event is None or exit_event.id > event_id

    def exited_between(self, entry: _Event, after: int, until: int) -> bool:
        """True when ``entry``'s state exited after event ``after`` (and not after ``until``)."""
        exit_event = self.exit_of.get(entry.id)
        return exit_event is not None and after < exit_event.id <= until

    def open_entry_before(
        self, event_id: int, kinds: frozenset[str] | None, *, since: int = 0, name: str | None = None
    ) -> _Event | None:
        """The nearest ``*StateEntered`` before ``event_id`` (of ``kinds`` and ``name``) still open then."""
        for event in reversed(self.between(since - 1, event_id)):
            if not event.is_entry or event.type == _FAIL_STATE_ENTERED:
                continue
            if (kinds is not None and event.type not in kinds) or (
                name is not None and event.state_name != name
            ):
                continue
            if self.is_open_at(event, event_id):
                return event
        return None

    def entry_for(self, event: _Event, kinds: frozenset[str] | None) -> tuple[_Event | None, bool]:
        """``(entry, linked)``: the state entry of ``event`` by its links, else the nearest open one."""
        linked = self.chain_entry(event)
        if linked is not None and (kinds is None or linked.type in kinds):
            return linked, True
        return self.open_entry_before(event.id, kinds), False

    def owner(self, event: _Event, kinds: frozenset[str]) -> _Event | None:
        """The state entry (of ``kinds``) that ``event`` belongs to (remembered)."""
        key = (event.id, kinds)
        if key not in self._owners:
            self._owners[key] = self.entry_for(event, kinds)[0]
        return self._owners[key]

    def owned(
        self, entry: _Event, types: frozenset[str], after: int, before: int, kinds: frozenset[str]
    ) -> list[_Event]:
        """The events of ``types`` with ``after < id < before`` that belong to ``entry``."""
        start = max(after, entry.id)
        return [
            event
            for event in self.between(start, before)
            if event.type in types and self.owner(event, kinds) is entry
        ]

    def links_back_to(self, event: _Event, target: int) -> bool:
        """True when ``event``'s ``previousEventId`` chain passes event ``target`` within a state."""
        current = self.get(event.previous)
        while current is not None and current.id >= target:
            if current.id == target:
                return True
            if current.is_entry or current.is_exit or current.type in _BOUNDARY_TYPES:
                return False
            current = self.get(current.previous)
        return False

    # ------------------------------------------------------------------ structure around a state
    def containers(self, entry: _Event, at: int) -> list[_Container]:
        """The Map/Parallel states around ``entry`` still open at event ``at``, outermost first.

        Walks ``previousEventId`` up from the entry: ``MapIterationStarted`` gives the iteration
        of the Map entered next, a sibling state is skipped from its exit to its entry, and the
        walk stops at 0 (entries may point to 0) or at the execution's own events.
        """
        found: list[_Container] = []
        index: int | None = None
        current = entry
        while True:
            previous = self.get(current.previous)
            if previous is None or previous.type in (EXECUTION_STARTED, EXECUTION_REDRIVEN):
                break
            if previous.type == _MAP_ITERATION_STARTED:
                index = _count(previous.details.get("index"))
            elif previous.type in _CONTAINERS:
                kind = _CONTAINERS[previous.type]
                if previous.state_name is not None and self.is_open_at(previous, at):
                    found.append(
                        _Container(previous.state_name, kind, previous, index if kind == "map" else None)
                    )
                index = None
            elif previous.is_exit:
                sibling = self.entry_of_exit.get(previous.id)
                if sibling is None:
                    break
                previous, index = sibling, None
            current = previous
        found.reverse()
        return found

    def cause_of(self, terminal: _Event) -> tuple[_Event | None, list[_Event]]:
        """The step failure the terminal event links to through :data:`PROPAGATION_TYPES` events,
        and the propagation events passed (outermost first)."""
        passed: list[_Event] = []
        current = self.get(terminal.previous)
        while current is not None:
            if current.type in _FAILURE_TYPES:
                return current, passed
            if current.type not in _PROPAGATION:
                break
            passed.append(current)
            current = self.get(current.previous)
        return None, passed

    def iterations_after(self, failure: _Event, until: int) -> list[tuple[str, int]]:
        """``(Map, index)`` of the first ``MapIterationFailed`` of each Map after ``failure``
        (up to event ``until``), innermost first."""
        seen: set[str] = set()
        found: list[tuple[str, int]] = []
        for event in self.between(failure.id, until + 1):
            iteration = _iteration(event) if event.type == _MAP_ITERATION_FAILED else None
            if iteration is not None and iteration[0] not in seen:
                seen.add(iteration[0])
                found.append(iteration)
        return found


def _entry_kinds(failure_type: str) -> frozenset[str] | None:
    """The entry types a step failure of ``failure_type`` can belong to (``None``: any)."""
    if failure_type == _MAP_RUN_FAILED:
        return _MAP_ENTRIES
    if failure_type in (_FAIL_STATE_ENTERED, _EVALUATION_FAILED):
        return None
    return _TASK_ENTRIES


# =========================================================================== localisation


@dataclass(frozen=True)
class _Step:
    """The chosen step failure while the result is assembled."""

    failure: _Event  # the step failure event (the terminal event for rule 7)
    entry: _Event | None
    linked: bool  # the entry was found through previousEventId links
    containers: tuple[_Container, ...]
    name: str | None  # the state name (step_id)


class _Localizer:
    """One :func:`localize` call: the segment, its terminal event and the chosen step failure."""

    def __init__(self, events: list[_Event], redrive_count: int | None) -> None:
        self.history = _History(events)
        self.requested = redrive_count
        number, self.segment = _select(events, redrive_count)
        self.redrive_count = number if number is not None else 0
        self.segment_ids = {event.id for event in self.segment}
        self.since = self.segment[0].id if self.segment else 0
        self.until = self.segment[-1].id if self.segment else 0
        self.terminal = next((event for event in reversed(self.segment) if event.type in _TERMINAL), None)
        self.notes: list[str] = []
        self._scheduled: dict[int, list[_Event]] | None = None

    # ------------------------------------------------------------------ the state of a failure
    def step_of(self, failure: _Event, passed: Sequence[_Event] = ()) -> _Step:
        """The state, entry and surrounding Map/Parallel states of a step failure event."""
        history = self.history
        if failure.type == _FAIL_STATE_ENTERED:
            entry, linked, name = failure, True, failure.state_name
        elif failure.type == _EVALUATION_FAILED:
            name = failure.text("state")
            entry = history.chain_entry(failure)
            linked = entry is not None and (name is None or entry.state_name == name)
            if not linked:
                entry = history.open_entry_before(failure.id, None, name=name) if name else None
            name = name or (entry.state_name if entry is not None else None)
        else:
            entry, linked = history.entry_for(failure, _entry_kinds(failure.type))
            name = entry.state_name if entry is not None else None
        containers = history.containers(entry, failure.id) if entry is not None else []
        return _Step(failure, entry, linked, tuple(self._with_iterations(containers, failure, passed)), name)

    def _with_iterations(
        self, containers: list[_Container], failure: _Event, passed: Sequence[_Event]
    ) -> list[_Container]:
        """Complete the Map iterations of ``containers`` from ``MapIterationFailed`` events: those
        the terminal event's chain passed, else those between the failure and the terminal event."""
        hints = [hint for event in reversed(passed) for hint in [_iteration(event)] if hint is not None]
        on_chain = bool(hints)
        if not hints and failure is not self.terminal:
            until = self.terminal.id if self.terminal is not None else self.until
            hints = self.history.iterations_after(failure, until)
        if not hints:
            return containers
        by_name: dict[str, int] = {}
        for name, index in hints:  # innermost first; a Map name is unique in a definition
            by_name.setdefault(name, index)
        if containers:
            return [
                _Container(c.name, c.kind, c.entry, by_name.get(c.name))
                if c.kind == "map" and c.index is None
                else c
                for c in containers
            ]
        completed: list[_Container] = []
        for name, index in reversed(hints):  # outermost first
            entry = self.history.open_entry_before(failure.id, _MAP_ENTRIES, name=name)
            if entry is not None or on_chain:  # a Map running when the state failed, or on the chain
                completed.append(_Container(name, "map", entry, index))
        return completed

    def handled(self, step: _Step) -> bool:
        """True when the failure was retried or caught within the segment (it did not end it)."""
        history, failure, entry = self.history, step.failure, step.entry
        if failure.type == _FAIL_STATE_ENTERED:
            caught = False
        elif entry is not None:
            if any(failure.id < event.id <= self.until for event in self.scheduled(entry)):
                return True  # a Retry scheduled the state again
            caught = history.exited_between(entry, failure.id, self.until)
        else:
            caught = any(
                (event.type in _SCHEDULED or event.is_exit) and history.links_back_to(event, failure.id)
                for event in history.between(failure.id, self.until + 1)
            )
        return caught or any(
            container.entry is not None and history.exited_between(container.entry, failure.id, self.until)
            for container in step.containers
        )

    def scheduled(self, entry: _Event) -> list[_Event]:
        """The ``*Scheduled`` events of ``entry`` in this segment (grouped once for every entry)."""
        if self._scheduled is None:
            grouped: dict[int, list[_Event]] = {}
            for event in self.segment:
                owner = self.history.owner(event, _TASK_ENTRIES) if event.type in _SCHEDULED else None
                if owner is not None:
                    grouped.setdefault(owner.id, []).append(event)
            self._scheduled = grouped
        return self._scheduled.get(entry.id, [])

    # ------------------------------------------------------------------ choosing the failure
    def choose(self) -> _Step | None:
        """The terminal step failure (rules 2-6), or ``None``."""
        terminal = self.terminal
        if terminal is not None:
            cause, passed = self.history.cause_of(terminal)
            if cause is not None and cause.id in self.segment_ids:
                return self.step_of(cause, passed)
        limit = terminal.id if terminal is not None else self.until + 1
        for event in reversed(self.segment):
            if event.type in _FAILURE_TYPES and event.id < limit:
                step = self.step_of(event)
                if not self.handled(step):
                    return step
        return None

    def open_state_at_terminal(self) -> _Step | None:
        """Rule 7: the state still open when an ``ExecutionFailed`` without a step failure ended it."""
        terminal, history = self.terminal, self.history
        if terminal is None or terminal.type != _EXECUTION_FAILED:
            return None
        entry = history.chain_entry(terminal)
        linked = entry is not None and history.is_open_at(entry, terminal.id)
        if not linked:
            entry = history.open_entry_before(terminal.id, None, since=self.since)
        if entry is None or entry.id not in self.segment_ids:
            return None
        containers = tuple(history.containers(entry, terminal.id))
        return _Step(terminal, entry, linked, containers, entry.state_name)

    # ------------------------------------------------------------------ assembling the result
    def attempt_fields(self, step: _Step) -> dict[str, Any]:
        """``retry_attempts``, ``started_at``, ``resource``, ``resource_type``, ``function_name``
        and ``map_run_arn`` of the failing attempt (rules 5 and 8)."""
        failure, entry, history = step.failure, step.entry, self.history
        fields: dict[str, Any] = {"retry_attempts": 0}
        if entry is None or failure.type in (_FAIL_STATE_ENTERED, _EVALUATION_FAILED):
            return fields
        if failure.type == _MAP_RUN_FAILED:
            runs = history.owned(entry, _MAP_RUN_STARTED, self.since - 1, failure.id, _MAP_ENTRIES)
            if runs:
                fields.update(
                    retry_attempts=len(runs) - 1,
                    map_run_arn=runs[-1].text("mapRunArn"),
                    started_at=runs[-1].timestamp,
                )
            return fields
        scheduled = [event for event in self.scheduled(entry) if event.id < failure.id]
        fields["retry_attempts"] = max(len(scheduled) - 1, 0)
        started = history.owned(entry, _STARTED, self.since - 1, failure.id, _TASK_ENTRIES)
        if started:
            fields["started_at"] = started[-1].timestamp
        fields.update(_resource_fields(scheduled[-1] if scheduled else None, failure))
        return fields

    def error_and_cause(self, failure: _Event) -> tuple[str | None, str | None]:
        """The failure's error and cause, else the terminal event's (always for Fail states)."""
        terminal = self.terminal
        terminal_error = terminal.text("error") if terminal is not None else None
        terminal_cause = terminal.text("cause") if terminal is not None else None
        if failure.type in (_FAIL_STATE_ENTERED, *TERMINAL_EVENT_TYPES):
            return terminal_error, terminal_cause
        error, cause = failure.text("error"), failure.text("cause")
        if error is None and cause is None:
            return terminal_error, terminal_cause
        if cause is None and error is not None and error == terminal_error:
            cause = terminal_cause
        return error, cause

    def result(self) -> Localized:
        if not self.segment:
            wanted = "the latest redrive segment" if self.requested is None else f"redrive {self.requested}"
            return self._run_level(None, terminal=False, note=f"The history holds no event of {wanted}.")
        if self.terminal is None:
            if self.segment[-1].type == _EXECUTION_SUCCEEDED:
                self.notes.append("The execution succeeded in this redrive segment.")
            else:
                self.notes.append("No terminal event: the execution is still running or the history was cut.")
        step = self.choose()
        if step is None:
            step = self.open_state_at_terminal()
            if step is not None:
                self.notes.append(
                    "No step failure event: localised to the state open when the execution failed."
                )
        if step is None:
            note = "No step failure in this redrive segment: a run-level failure." if self.terminal else None
            return self._run_level(None, terminal=self.terminal is not None, note=note)
        if step.name is None:
            note = f"The state entry of failure event {step.failure.id} is not in the history."
            return self._run_level(
                step.failure, terminal=self.terminal is not None, note=note, missing_entry=True
            )
        if not step.linked:
            self.notes.append(
                "The failing state was found without previousEventId links (nearest open state)."
            )
        return self._localized(step)

    def _localized(self, step: _Step) -> Localized:
        failure, terminal = step.failure, self.terminal
        error, cause = self.error_and_cause(failure)
        fields = self.attempt_fields(step)
        maps = [container.index for container in step.containers if container.kind == "map"]
        map_index = maps[-1] if maps and maps[-1] is not None else NO_MAP_INDEX
        path = "/".join([c.label() for c in step.containers] + [step.name or ""]) if step.containers else None
        return Localized(
            step_id=step.name or RUN_LEVEL_STEP,
            map_index=map_index,
            redrive_count=self.redrive_count,
            error=error,
            cause=cause,
            event_type=failure.type,
            failed_event_id=failure.id,
            entered_event_id=step.entry.id if step.entry is not None else None,
            terminal_event_type=terminal.type if terminal is not None else None,
            terminal_error=terminal.text("error") if terminal is not None else None,
            terminal_cause=terminal.text("cause") if terminal is not None else None,
            retry_attempts=fields["retry_attempts"],
            resource=fields.get("resource"),
            resource_type=fields.get("resource_type"),
            function_name=fields.get("function_name"),
            step_path=path,
            map_run_arn=fields.get("map_run_arn"),
            started_at=fields.get("started_at"),
            failed_at=failure.timestamp or (terminal.timestamp if terminal is not None else None),
            terminal=terminal is not None,
            notes=tuple(self.notes),
        )

    def _run_level(
        self, failure: _Event | None, *, terminal: bool, note: str | None, missing_entry: bool = False
    ) -> Localized:
        """A result without a state: it names ``failure`` when given, else the terminal event."""
        event = self.terminal
        named = failure if failure is not None else event
        error, cause = self.error_and_cause(named) if named is not None else (None, None)
        return Localized(
            step_id=RUN_LEVEL_STEP,
            redrive_count=self.redrive_count,
            error=error,
            cause=cause,
            event_type=named.type if named is not None else None,
            failed_event_id=named.id if named is not None else None,
            terminal_event_type=event.type if event is not None else None,
            terminal_error=event.text("error") if event is not None else None,
            terminal_cause=event.text("cause") if event is not None else None,
            failed_at=named.timestamp if named is not None else None,
            terminal=terminal,
            notes=tuple(self.notes) + ((note,) if note else ()),
            missing_entry=missing_entry,
        )


def _resource_fields(scheduled: _Event | None, failure: _Event) -> dict[str, Any]:
    """``resource``/``resource_type``/``function_name`` of an attempt from its events."""
    resource = resource_type = function_name = None
    if scheduled is not None:
        if scheduled.type == "TaskScheduled":
            resource_type, resource = scheduled.text("resourceType"), scheduled.text("resource")
            if resource_type == "lambda":
                function_name = _function_from_parameters(scheduled.details.get("parameters"))
        elif scheduled.type == "LambdaFunctionScheduled":
            resource = scheduled.text("resource")
            resource_type, function_name = "lambda", resource
        elif scheduled.type == "ActivityScheduled":
            resource, resource_type = scheduled.text("resource"), "activity"
    if failure.type.startswith("Task"):
        resource = resource or failure.text("resource")
        resource_type = resource_type or failure.text("resourceType")
    elif failure.type.startswith("LambdaFunction"):
        resource_type = resource_type or "lambda"
    elif failure.type.startswith("Activity"):
        resource_type = resource_type or "activity"
    return {"resource": resource, "resource_type": resource_type, "function_name": function_name}


def _function_from_parameters(parameters: Any) -> str | None:
    """``FunctionName`` of an optimised Lambda Task's parameters (JSON text); nothing else is kept."""
    if isinstance(parameters, str):
        if len(parameters) > _MAX_PARAMETERS_CHARS:
            return None
        try:
            parameters = json.loads(parameters)
        except (ValueError, RecursionError):  # not JSON, or nested too deeply to parse
            return None
    if not isinstance(parameters, Mapping):
        return None
    return _text(parameters.get("FunctionName"))


def localize(events: Sequence[Mapping[str, Any]], *, redrive_count: int | None = None) -> Localized:
    """Localise the terminal failure of one redrive segment (see the module docstring).

    Never raises for an odd history: without a terminal event the result has
    ``terminal=False``; without a usable step failure it is ``RUN_LEVEL_STEP`` with a note.
    ``redrive_count`` must be ``None`` (the latest segment) or an integer >= 0
    (:class:`ValueError` otherwise).
    """
    return _Localizer(_parse(events), _wanted(redrive_count)).result()


__all__ = [
    "EXECUTION_REDRIVEN",
    "EXECUTION_STARTED",
    "PROPAGATION_TYPES",
    "SCHEDULED_TYPES",
    "STARTED_TYPES",
    "STEP_FAILURE_TYPES",
    "TERMINAL_EVENT_TYPES",
    "Localized",
    "event_details",
    "localize",
    "segment",
]
