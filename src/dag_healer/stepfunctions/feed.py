"""Pull detection: the :class:`~dag_healer.core.ports.FailureFeed` of Step Functions (section 9.4).

For each monitored STANDARD state machine (configured, plus discovered ones with
``discover: true``) and each status of ``options.statuses``: ``ListExecutions(stateMachineArn,
statusFilter=status, maxResults=100)``, paging while ``stopDate >= <window start>``
(``ListExecutions`` is eventually consistent; finished executions come newest first, ordered by
``stopDate``). Each new ``(executionArn, redriveCount, startDate)`` is localised
(``executions.read_history`` + ``history.localize``) into one terminal
:class:`~dag_healer.core.failures.Failure` (source ``poll``, ``is_final=True``, state ``FAILED``);
localisations are remembered in memory, so the overlap costs no extra history reads. At most
``options.max_histories_per_poll`` histories are read per poll (the 20/s refill) and at most
``limit`` failures are localised per poll; when a cap is hit the page is ``complete=False`` and the
machine's cursor does not move past the executions not read yet.

The cursor (JSON-safe; AWS ``nextToken`` values are never persisted):

* ``{state_machine_arn: <ms>}``: the ``stopDate`` (epoch milliseconds) of the newest execution of
  the machine's handled prefix; the machine's next window starts 5 minutes before it;
* ``"watermark"`` (ISO-8601, owned by the core): when the poll that wrote the cursor started.
  A machine without an entry of its own starts 5 minutes before it (``options.lookback_hours``
  ago without a watermark);
* ``"not_before"`` (ISO-8601, owned by the core: the floor ``dag-healer store reset`` writes): no
  failure whose execution stopped before it is listed, and every new cursor carries it unchanged,
  also after a poll that listed nothing.

``now`` is the moment the poll starts (aware UTC; the runtime clock when ``None``): it is the
new watermark and drives the lookback. AWS errors propagate as :mod:`dag_healer.core.errors`
exceptions (the core keeps its cursor).

Details:

* executions are processed oldest first; the cursor of a machine becomes the ``stopDate`` of the
  newest execution of the longest fully handled prefix, so it never passes an execution left for
  a later poll (cap hit, or a history not usable yet). A machine the poll does not reach (the
  budget ran out first) or cannot list keeps its window for the next poll;
* remembered failures in the overlap cost no history read and do not count against the caps
  (``limit`` counts the failures localised by this poll), so a burst of failures inside the
  overlap can never hold the cursor back. A failure a committed page returned is not returned
  again: a page counts as committed when the core passes its cursor back (the core stores the
  cursor only after it stored every failure of the page). A cursor the feed did not write itself
  (a reset, another process) makes it forget what it knew, so the overlap is returned again
  (the store key dedupes it);
* once a cap cut a poll short, the next poll starts with the machine after the one being served
  when the budget ran out (then wraps around), so one busy machine cannot starve the others;
* a history that cannot be used (KMS denied, no terminal event yet ...) is checked again on the
  next polls; once it stayed unusable for more than :data:`MAX_UNREADABLE_POLLS` checks the
  execution is reported run-level (:data:`~dag_healer.core.ids.RUN_LEVEL_STEP`,
  ``native.localized = False``, its error from ``DescribeExecution``). The count is shared with
  the push intake when the adapter gives both ports one :class:`HistoryLocalizer`;
* transient errors (throttling, 5xx, network) propagate. A machine that cannot be listed for
  another reason (deleted, Express, not allowed) is skipped with a warning (once per machine and
  process) while the others are polled; when no watched machine can be listed, the first such
  error propagates;
* a step failure whose state entry precedes the segment read (a redrive that resumed a state
  without entering it again [U]) is localised again with the previous segment's events too;
* localisations are keyed by ``(executionArn, redriveCount, startDate)``: an execution name reused
  after its 90-day retention is a new execution with the same ARN.

:class:`ExecutionInfo`, :class:`HistoryLocalizer` and :func:`execution_failure` are shared with the
push intake (:mod:`dag_healer.stepfunctions.intake`).
"""

from __future__ import annotations

import logging
import threading
from collections import OrderedDict
from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass, field
from datetime import datetime
from typing import Any

from dag_healer.core.errors import NotFound, OrchestratorError
from dag_healer.core.failures import AttemptState, Failure, FailurePage
from dag_healer.core.ids import NO_MAP_INDEX, RUN_LEVEL_STEP, AttemptRef
from dag_healer.stepfunctions import arns, causes, executions, history
from dag_healer.stepfunctions.aws import call, error_code
from dag_healer.stepfunctions.history import Localized
from dag_healer.stepfunctions.runtime import SfnRuntime, epoch_ms, iso, native_ref, to_utc

logger = logging.getLogger(__name__)

POLL_SOURCE = "poll"
POLL_OVERLAP_MS = 5 * 60 * 1000
LIST_PAGE_SIZE = 100
DEFAULT_POLL_LIMIT = 200
#: ``ListExecutions`` pages read per machine and status in one poll (a window holding more failed
#: executions of one status than this is cut to its newest ones).
MAX_LIST_PAGES = 50
DISCOVERY_PAGE_SIZE = 1000
MAX_DISCOVERY_PAGES = 100
#: Localisations remembered per localizer, by ``(executionArn, redriveCount, startDate ms)``.
LOCALIZATION_CACHE_SIZE = 4096
#: Executions a feed remembers as stored by the core (returned by a committed page).
COMMITTED_CACHE_SIZE = 4096
#: Checks of an unusable history before the execution is reported run-level.
MAX_UNREADABLE_POLLS = 3
#: The cursor keys the core owns (ISO-8601 text): when the last successful poll started, and the
#: floor ``dag-healer store reset`` writes.
WATERMARK_KEY = "watermark"
NOT_BEFORE_KEY = "not_before"

_HOUR_MS = 3600 * 1000
_TERMINAL_EVENT_OF_STATUS = {
    "FAILED": "ExecutionFailed",
    "TIMED_OUT": "ExecutionTimedOut",
    "ABORTED": "ExecutionAborted",
}
_CORE_KEYS = frozenset({WATERMARK_KEY, NOT_BEFORE_KEY})

ExecutionKey = tuple[str, int, int | None]
"""``(executionArn, redriveCount, startDate in epoch ms)``: one attempt of one execution."""


# =========================================================================== executions


def _text(value: Any) -> str | None:
    return value if isinstance(value, str) and value.strip() else None


def _redrive_count(value: Any) -> int:
    return value if isinstance(value, int) and not isinstance(value, bool) and value >= 0 else 0


@dataclass(frozen=True)
class ExecutionInfo:
    """What a ``ListExecutions`` item or a ``DescribeExecution`` answer says about one execution.

    ``error``/``cause`` (RAW) come from ``DescribeExecution`` only; input and output are never
    kept.
    """

    execution_arn: str
    state_machine_arn: str
    name: str
    status: str
    start_date: datetime | None = None
    stop_date: datetime | None = None
    redrive_count: int = 0
    version_arn: str | None = None
    error: str | None = None
    cause: str | None = None

    @property
    def stop_ms(self) -> int | None:
        """``stopDate`` in epoch milliseconds (``None`` while running)."""
        return epoch_ms(self.stop_date)

    @property
    def start_ms(self) -> int | None:
        """``startDate`` in epoch milliseconds (``None`` when unknown)."""
        return epoch_ms(self.start_date)

    @property
    def attempt(self) -> int:
        """The incident attempt (``redriveCount + 1``)."""
        return self.redrive_count + 1

    @property
    def key(self) -> ExecutionKey:
        """``(executionArn, redriveCount, startDate ms)``: what a localisation is remembered by
        (the start date tells an execution from a later one that reused its name)."""
        return (self.execution_arn, self.redrive_count, self.start_ms)

    @classmethod
    def from_api(cls, item: Any) -> ExecutionInfo | None:
        """Parse an API item; ``None`` without a usable execution ARN, state machine ARN or status."""
        if not isinstance(item, Mapping):
            return None
        execution_arn, machine_arn, status = (
            _text(item.get("executionArn")),
            _text(item.get("stateMachineArn")),
            _text(item.get("status")),
        )
        if execution_arn is None or machine_arn is None or status is None:
            return None
        try:
            parsed = arns.parse_execution_arn(execution_arn)
            arns.parse_state_machine_arn(machine_arn)
        except ValueError:
            return None
        return cls(
            execution_arn=execution_arn,
            state_machine_arn=machine_arn,
            name=_text(item.get("name")) or parsed.name,
            status=status,
            start_date=to_utc(item.get("startDate")),
            stop_date=to_utc(item.get("stopDate")),
            redrive_count=_redrive_count(item.get("redriveCount")),
            version_arn=_text(item.get("stateMachineVersionArn")),
            error=_text(item.get("error")),
            cause=_text(item.get("cause")),
        )


def _failure_native(info: ExecutionInfo, localized: Localized) -> dict[str, Any]:
    """The ``native_ref`` of a failure: the execution's facts plus the localisation's ids."""
    base = {
        "execution_arn": info.execution_arn,
        "state_machine_arn": info.state_machine_arn,
        "start_date": info.start_date,
        "stop_date": info.stop_date,
        "status": info.status,
        "redrive_count": info.redrive_count,
        "definition_version_arn": info.version_arn,
    }
    fields = localized.native_fields()
    if fields.get("error") is None:
        fields["error"] = info.error
    try:
        return native_ref(base, **fields)
    except ValueError:  # over 4 KB (very long names): keep the identifiers only
        logger.debug("dag-healer stepfunctions: native_ref of %s cut to its identifiers", info.name)
        ids = ("failed_event_id", "entered_event_id", "event_type", "retry_attempts", "localized")
        return native_ref(base, **{key: fields[key] for key in ids})


def execution_failure(
    rt: SfnRuntime,
    *,
    pipeline_id: str,
    run_id: str,
    info: ExecutionInfo,
    localized: Localized,
    source: str,
) -> Failure:
    """The terminal :class:`~dag_healer.core.failures.Failure` of a localised execution.

    ``error_name`` is the step error (else the execution's), ``error_summary`` is
    :func:`~dag_healer.stepfunctions.causes.summarize` of the step error and cause (RAW),
    ``failed_at`` the execution's ``stopDate``.
    """
    step_id = localized.step_id or RUN_LEVEL_STEP
    map_index = localized.map_index if step_id != RUN_LEVEL_STEP else NO_MAP_INDEX
    error = localized.error or info.error
    cause = localized.cause if (localized.error or localized.cause) else info.cause
    return Failure(
        ref=AttemptRef(rt.instance, pipeline_id, run_id, step_id, map_index, info.attempt),
        state=AttemptState.FAILED,
        is_final=True,
        source=source,
        error_name=error,
        error_summary=causes.summarize(error, cause),
        failed_at=info.stop_date or localized.failed_at,
        native=_failure_native(info, localized),
    )


# =========================================================================== localisation


@dataclass(frozen=True)
class Localization:
    """The outcome of :meth:`HistoryLocalizer.localize` for one ``(execution, redrive)``."""

    localized: Localized | None  # None: the history is not usable yet (check again later)
    reads: int = 0  # read_history calls made
    checks: int = 0  # unusable checks of this (execution, redrive) so far
    problem: str = ""  # why the history was not usable (an error class or a short reason)
    remembered: bool = False  # answered from memory, no I/O


class HistoryLocalizer:
    """Reads and localises execution histories and remembers the results (no I/O when known).

    A history that cannot be used (a non-transient read error such as a KMS denial, or a segment
    without a terminal event) is counted per :attr:`ExecutionInfo.key`; once the count exceeds
    ``max_unusable`` the execution is localised run-level without its history
    (``from_history=False``). Transient errors propagate and are not counted. One localizer may
    serve the feed and the push intake of an adapter: both then agree on every localisation and
    share the count.
    """

    def __init__(
        self,
        rt: SfnRuntime,
        *,
        max_unusable: int = MAX_UNREADABLE_POLLS,
        cache_size: int = LOCALIZATION_CACHE_SIZE,
    ) -> None:
        self.rt = rt
        self.max_unusable = max_unusable
        self.cache_size = max(1, cache_size)
        self._lock = threading.Lock()
        self._known: OrderedDict[ExecutionKey, Localized] = OrderedDict()
        self._unusable: OrderedDict[ExecutionKey, int] = OrderedDict()

    def remembered(self, info: ExecutionInfo) -> Localized | None:
        """The remembered localisation of ``info`` (``None`` when it was not localised yet)."""
        with self._lock:
            localized = self._known.get(info.key)
            if localized is not None:
                self._known.move_to_end(info.key)
            return localized

    def unusable_checks(self, info: ExecutionInfo) -> int:
        """How many times the history of ``info`` could not be used so far."""
        with self._lock:
            return self._unusable.get(info.key, 0)

    def localize(self, info: ExecutionInfo) -> Localization:
        """Localise ``info``'s redrive segment (I/O unless remembered); see the class docstring."""
        known = self.remembered(info)
        if known is not None:
            return Localization(known, remembered=True)
        reads = 0
        try:
            localized, reads = self._read(info)
        except OrchestratorError as exc:
            if exc.transient:
                raise
            localized, reads = None, max(reads, 1)
            problem = type(exc).__name__
        else:
            problem = "" if localized.terminal else "no terminal event in the history yet"
        if localized is not None and localized.terminal:
            self._remember(info.key, localized)
            return Localization(localized, reads=reads)
        checks = self._count(info.key)
        logger.debug(
            "dag-healer stepfunctions %s: history of %s (redrive %d) not usable: %s (check %d)",
            self.rt.instance,
            info.name,
            info.redrive_count,
            problem,
            checks,
        )
        if checks <= self.max_unusable:
            return Localization(None, reads=reads, checks=checks, problem=problem)
        fallback = self._fallback(info, problem, checks)
        self._remember(info.key, fallback)
        return Localization(fallback, reads=reads, checks=checks, problem=problem)

    def _read(self, info: ExecutionInfo) -> tuple[Localized, int]:
        segment = executions.read_history(self.rt, info.execution_arn, redrive_count=info.redrive_count)
        localized = history.localize(segment.events, redrive_count=info.redrive_count)
        if localized.missing_entry and info.redrive_count > 0 and segment.complete:
            earlier = executions.read_history(
                self.rt, info.execution_arn, redrive_count=info.redrive_count - 1
            )
            return history.localize(earlier.events, redrive_count=info.redrive_count), 2
        return localized, 1

    def _fallback(self, info: ExecutionInfo, problem: str, checks: int) -> Localized:
        """The run-level localisation of an execution whose history stayed unusable."""
        error, cause = info.error, info.cause
        if error is None and cause is None:
            error, cause = self._described_error(info)
        note = f"The execution history could not be used in {checks} checks ({problem or 'unknown'})."
        return Localized(
            step_id=RUN_LEVEL_STEP,
            redrive_count=info.redrive_count,
            error=error,
            cause=cause,
            event_type=_TERMINAL_EVENT_OF_STATUS.get(info.status),
            terminal_event_type=_TERMINAL_EVENT_OF_STATUS.get(info.status),
            terminal_error=error,
            terminal_cause=cause,
            failed_at=info.stop_date,
            notes=(note,),
            from_history=False,
        )

    def _described_error(self, info: ExecutionInfo) -> tuple[str | None, str | None]:
        """The execution's error and cause from ``DescribeExecution`` (best effort)."""
        try:
            described = executions.describe_execution(self.rt, info.execution_arn)
        except OrchestratorError as exc:
            logger.debug(
                "dag-healer stepfunctions %s: no error of %s: %s",
                self.rt.instance,
                info.name,
                type(exc).__name__,
            )
            return None, None
        return _text(described.get("error")), _text(described.get("cause"))

    def _remember(self, key: ExecutionKey, localized: Localized) -> None:
        with self._lock:
            self._known[key] = localized
            self._known.move_to_end(key)
            self._unusable.pop(key, None)
            while len(self._known) > self.cache_size:
                self._known.popitem(last=False)

    def _count(self, key: ExecutionKey) -> int:
        with self._lock:
            count = self._unusable.get(key, 0) + 1
            self._unusable[key] = count
            self._unusable.move_to_end(key)
            while len(self._unusable) > self.cache_size:
                self._unusable.popitem(last=False)
            return count


# =========================================================================== the cursor


def _cursor_ms(value: Any) -> int | None:
    """A cursor value (epoch milliseconds) as an int >= 0, else ``None``."""
    if isinstance(value, bool):
        return None
    if isinstance(value, float) and value.is_integer():
        value = int(value)
    elif isinstance(value, str) and value.strip().isdigit():
        value = int(value.strip())
    return value if isinstance(value, int) and value >= 0 else None


def read_cursor(cursor: Mapping[str, Any] | None) -> dict[str, int]:
    """The machine positions of a poll cursor (``{state_machine_arn: stop_ms}``).

    Only state machine ARN keys with a valid value count; the core's ``watermark`` and
    ``not_before`` are read by :func:`cursor_time`, anything else is dropped.
    """
    if not isinstance(cursor, Mapping):
        return {}
    entries: dict[str, int] = {}
    ignored = 0
    for key, value in cursor.items():
        if key in _CORE_KEYS:
            continue
        stop_ms = _cursor_ms(value)
        if isinstance(key, str) and arns.is_arn(key) and stop_ms is not None:
            entries[key] = stop_ms
        else:
            ignored += 1
    if ignored:
        logger.debug("dag-healer stepfunctions: ignored %d invalid poll cursor entries", ignored)
    return entries


def cursor_time(cursor: Mapping[str, Any] | None, key: str) -> datetime | None:
    """``cursor[key]`` (ISO-8601 text, ``...Z`` accepted) as an aware UTC datetime; ``None`` when
    it is missing, empty or not a timestamp."""
    value = cursor.get(key) if isinstance(cursor, Mapping) else None
    if not isinstance(value, (str, datetime)) or value == "":
        return None
    return to_utc(value)


def _invalid_times(cursor: Mapping[str, Any] | None) -> list[str]:
    """The core keys of ``cursor`` that hold something other than a timestamp (or nothing)."""
    if not isinstance(cursor, Mapping):
        return []
    return [
        key
        for key in (WATERMARK_KEY, NOT_BEFORE_KEY)
        if cursor.get(key) not in (None, "") and cursor_time(cursor, key) is None
    ]


@dataclass(frozen=True)
class _Window:
    """Where each machine's window starts in one poll (epoch milliseconds)."""

    positions: Mapping[str, int]
    default_ms: int  # a machine without an entry of its own
    floor_ms: int | None  # never earlier than this (``not_before``)

    @classmethod
    def of(cls, cursor: Mapping[str, Any] | None, *, now_ms: int, lookback_hours: int) -> _Window:
        """The window of a poll that starts at ``now_ms`` (see the module docstring)."""
        floor_ms = epoch_ms(cursor_time(cursor, NOT_BEFORE_KEY))
        watermark_ms = epoch_ms(cursor_time(cursor, WATERMARK_KEY))
        if watermark_ms is not None:
            start = watermark_ms - POLL_OVERLAP_MS
        else:
            start = now_ms - lookback_hours * _HOUR_MS
        default_ms = start if floor_ms is None else max(start, floor_ms)
        return cls(read_cursor(cursor), default_ms, floor_ms)

    def start_of(self, machine_arn: str) -> int:
        """The first ``stopDate`` the machine's window lists."""
        position = self.positions.get(machine_arn)
        start = position - POLL_OVERLAP_MS if position is not None else self.default_ms
        return start if self.floor_ms is None else max(start, self.floor_ms)


def _new_cursor(
    cursor: Mapping[str, Any] | None, positions: Mapping[str, int], moment: datetime
) -> dict[str, Any]:
    """The positions, the poll's start as the watermark and the cursor's floor, unchanged."""
    out: dict[str, Any] = dict(positions)
    out[WATERMARK_KEY] = iso(moment)
    if isinstance(cursor, Mapping) and NOT_BEFORE_KEY in cursor:
        out[NOT_BEFORE_KEY] = cursor[NOT_BEFORE_KEY]
    return out


# =========================================================================== the feed


@dataclass
class _Budget:
    """What one poll may still do."""

    histories: int
    failures: int
    complete: bool = True

    @property
    def spent(self) -> bool:
        return self.histories <= 0 or self.failures <= 0


@dataclass
class _Processed:
    """The result of one machine's window."""

    failures: list[Failure] = field(default_factory=list)
    keys: list[ExecutionKey] = field(default_factory=list)  # of the failures returned
    handled_upto: int | None = None  # stopDate (ms) of the newest execution of the handled prefix
    first_pending: int | None = None  # stopDate (ms) of the first execution left for a later poll

    def handled(self, info: ExecutionInfo) -> None:
        """Note an execution this poll is done with (returned now or by a committed page)."""
        if self.first_pending is None:
            self.handled_upto = info.stop_ms

    def pending(self, info: ExecutionInfo) -> None:
        """Note an execution left for a later poll (the cursor stops before the first one)."""
        if self.first_pending is None:
            self.first_pending = info.stop_ms


@dataclass(frozen=True)
class _Sent:
    """The cursor of a page and the executions it returned (they are stored once it comes back)."""

    cursor: dict[str, Any]
    keys: tuple[ExecutionKey, ...]


def _next_mark(previous: int | None, processed: _Processed) -> int | None:
    """A machine's new cursor value: it never moves back nor past an execution left pending."""
    if processed.handled_upto is not None:
        mark = processed.handled_upto
    elif processed.first_pending is not None and previous is None:
        mark = processed.first_pending - 1
    else:
        return previous
    return mark if previous is None else max(previous, mark)


def _same_cursor(given: Mapping[str, Any] | None, sent: Mapping[str, Any] | None) -> bool:
    return isinstance(given, Mapping) and sent is not None and dict(given) == dict(sent)


def _skip_reason(exc: OrchestratorError) -> str:
    code = error_code(exc.__cause__) if exc.__cause__ is not None else None
    return f"{type(exc).__name__}{f' ({code})' if code else ''}"


class SfnFeed:
    """``FailureFeed`` port (one per adapter); ``localizer`` may be shared with the push intake."""

    def __init__(self, rt: SfnRuntime, *, localizer: HistoryLocalizer | None = None) -> None:
        self.rt = rt
        self.localizer = localizer if localizer is not None else HistoryLocalizer(rt)
        self._warned: set[str] = set()
        self._lock = threading.Lock()
        self._sent: _Sent | None = None  # the last page's cursor and returned executions
        self._acknowledged: dict[str, Any] | None = None  # the last cursor the core passed back
        self._committed: OrderedDict[ExecutionKey, None] = OrderedDict()
        self._first: str | None = None  # the machine the next poll starts with (after a cut)

    def __repr__(self) -> str:
        return f"SfnFeed(instance={self.rt.instance!r})"

    def _warn_once(self, key: str, message: str, *args: Any) -> None:
        """A warning the first time ``key`` comes up, a debug line afterwards (polls repeat)."""
        level = logging.DEBUG if key in self._warned else logging.WARNING
        self._warned.add(key)
        logger.log(level, message, *args)

    def poll(
        self,
        *,
        cursor: Mapping[str, Any] | None,
        is_monitored: Callable[[str], bool],
        limit: int = DEFAULT_POLL_LIMIT,
        now: datetime | None = None,
    ) -> FailurePage:
        """Terminal failures of the watched machines since ``cursor`` (see the module docstring)."""
        rt = self.rt
        moment = to_utc(now) or rt.now()
        with self._lock:
            self._learn_commit(cursor)
            for key in _invalid_times(cursor):
                self._warn_once(
                    f"cursor:{key}",
                    "dag-healer stepfunctions %s: ignoring the invalid %s of the poll cursor",
                    rt.instance,
                    key,
                )
            window = _Window.of(
                cursor, now_ms=epoch_ms(moment) or 0, lookback_hours=rt.options.lookback_hours
            )
            positions: dict[str, int] = dict(window.positions)
            budget = _Budget(histories=rt.options.max_histories_per_poll, failures=max(1, int(limit)))
            failures: list[Failure] = []
            keys: list[ExecutionKey] = []
            watched = self._watched(is_monitored)
            errors = [target for _pipeline_id, target in watched if isinstance(target, OrchestratorError)]
            machines = self._in_turn([(pid, target) for pid, target in watched if isinstance(target, str)])
            stopped_at: int | None = None
            for index, (pipeline_id, arn) in enumerate(machines):
                since_ms = window.start_of(arn)
                if budget.spent:  # not reached: the machine keeps its window for the next poll
                    budget.complete = False
                    positions.setdefault(arn, since_ms + POLL_OVERLAP_MS)
                    continue
                try:
                    listed = self._list_window(arn, since_ms)
                except OrchestratorError as exc:
                    if exc.transient:
                        raise
                    errors.append(exc)
                    self._warn_once(
                        f"list:{arn}",
                        "dag-healer stepfunctions %s: cannot list the executions of pipeline %s: %s",
                        rt.instance,
                        pipeline_id,
                        _skip_reason(exc),
                    )
                    positions.setdefault(arn, since_ms + POLL_OVERLAP_MS)
                    continue
                processed = self._process(pipeline_id, listed, budget)
                failures.extend(processed.failures)
                keys.extend(processed.keys)
                mark = _next_mark(window.positions.get(arn), processed)
                if mark is not None:
                    positions[arn] = mark
                if stopped_at is None and (budget.spent or not budget.complete):
                    stopped_at = index
            if watched and len(errors) == len(watched):
                raise errors[0]
            self._first = (
                machines[(stopped_at + 1) % len(machines)][1]
                if not budget.complete and stopped_at is not None
                else None
            )
            new_cursor = _new_cursor(cursor, positions, moment)
            self._sent = _Sent(dict(new_cursor), tuple(keys))
        logger.debug(
            "dag-healer stepfunctions %s: poll of %d machine(s) found %d failure(s) (complete %s)",
            rt.instance,
            len(watched) - len(errors),
            len(failures),
            budget.complete,
        )
        return FailurePage(failures=tuple(failures), cursor=new_cursor, complete=budget.complete)

    # ------------------------------------------------------------------ committed pages
    def _learn_commit(self, cursor: Mapping[str, Any] | None) -> None:
        """What the given cursor says about the pages the core stored (see the module docstring)."""
        sent, self._sent = self._sent, None
        if sent is not None and _same_cursor(cursor, sent.cursor):
            for key in sent.keys:
                self._committed[key] = None
                self._committed.move_to_end(key)
            while len(self._committed) > COMMITTED_CACHE_SIZE:
                self._committed.popitem(last=False)
            self._acknowledged = sent.cursor
        elif not _same_cursor(cursor, self._acknowledged):
            self._committed.clear()  # a cursor of somebody else (a reset ...): trust nothing
            self._acknowledged = None

    def committed(self, info: ExecutionInfo) -> bool:
        """True when a committed page returned ``info`` (the core stored its incident)."""
        with self._lock:
            return info.key in self._committed

    # ------------------------------------------------------------------ machines
    def _in_turn(self, machines: Sequence[tuple[str, str]]) -> list[tuple[str, str]]:
        """The machines in configuration order, starting after the last cut when there was one."""
        ordered = list(machines)
        start = next((index for index, (_pid, arn) in enumerate(ordered) if arn == self._first), 0)
        return ordered[start:] + ordered[:start]

    def _watched(self, is_monitored: Callable[[str], bool]) -> list[tuple[str, str | OrchestratorError]]:
        """``(pipeline_id, unqualified ARN or the error resolving it)`` of every monitored pipeline."""
        rt = self.rt
        if rt.options.discover:
            self._discover()
        watched: list[tuple[str, str | OrchestratorError]] = []
        seen: set[str] = set()
        for entry in rt.pipelines.entries():
            if not is_monitored(entry.pipeline_id):
                continue
            try:
                arn = rt.state_machine_arn(entry.pipeline_id)
            except OrchestratorError as exc:
                if exc.transient:
                    raise
                self._warn_once(
                    f"resolve:{entry.pipeline_id}",
                    "dag-healer stepfunctions %s: cannot resolve the state machine of pipeline %s: %s",
                    rt.instance,
                    entry.pipeline_id,
                    _skip_reason(exc),
                )
                watched.append((entry.pipeline_id, exc))
                continue
            if arn is None:
                self._warn_once(
                    f"resolve:{entry.pipeline_id}",
                    "dag-healer stepfunctions %s: no state machine named %s for pipeline %s",
                    rt.instance,
                    entry.name,
                    entry.pipeline_id,
                )
                watched.append(
                    (entry.pipeline_id, NotFound(f"no state machine for pipeline {entry.pipeline_id}"))
                )
                continue
            # a second pipeline of the same machine, or a discovered name taken by a configured id
            if arn in seen or (entry.discovered and rt.pipelines.pipeline_id_for(arn) != entry.pipeline_id):
                continue
            seen.add(arn)
            watched.append((entry.pipeline_id, arn))
        return watched

    def _discover(self) -> None:
        """Add every STANDARD state machine of the account and region (``ListStateMachines``)."""
        token: str | None = None
        added = 0
        try:
            for _page in range(MAX_DISCOVERY_PAGES):
                params: dict[str, Any] = {"maxResults": DISCOVERY_PAGE_SIZE}
                if token:
                    params["nextToken"] = token
                response = call(
                    self.rt.clients.sfn, "list_state_machines", what="ListStateMachines", **params
                )
                items = response.get("stateMachines")
                for item in items if isinstance(items, list) else ():
                    if not isinstance(item, Mapping) or item.get("type") != "STANDARD":
                        continue  # EXPRESS machines have no history API (section 9.12)
                    arn = item.get("stateMachineArn")
                    if not _machine_arn(arn):
                        continue
                    try:
                        self.rt.pipelines.add_discovered(arn)
                    except ValueError:  # its name is the pipeline id of another machine
                        self._warn_once(
                            f"discover:{arn}",
                            "dag-healer stepfunctions %s: state machine %s is not watched: its name is "
                            "the pipeline id of another state machine",
                            self.rt.instance,
                            arns.state_machine_name(arn),
                        )
                        continue
                    added += 1
                token = _text(response.get("nextToken"))
                if token is None:
                    break
        except OrchestratorError as exc:
            if exc.transient:
                raise
            self._warn_once(
                "discover",
                "dag-healer stepfunctions %s: state machine discovery failed: %s",
                self.rt.instance,
                _skip_reason(exc),
            )
        logger.debug(
            "dag-healer stepfunctions %s: discovered %d STANDARD machine(s)", self.rt.instance, added
        )

    # ------------------------------------------------------------------ one machine
    def _list_window(self, machine_arn: str, since_ms: int) -> list[ExecutionInfo]:
        """The executions of ``options.statuses`` that stopped at or after ``since_ms``, oldest first."""
        found: dict[str, ExecutionInfo] = {}
        for status in self.rt.options.statuses:
            token: str | None = None
            for _page in range(MAX_LIST_PAGES):
                response = executions.list_executions(
                    self.rt, machine_arn, status, next_token=token, max_results=LIST_PAGE_SIZE
                )
                oldest = self._collect(response.get("executions"), status, since_ms, found)
                token = _text(response.get("nextToken"))
                if token is None or (oldest is not None and oldest < since_ms):
                    break
            else:
                logger.warning(
                    "dag-healer stepfunctions %s: stopped listing %s executions after %d pages",
                    self.rt.instance,
                    status,
                    MAX_LIST_PAGES,
                )
        return sorted(found.values(), key=lambda info: (info.stop_ms or 0, info.execution_arn))

    @staticmethod
    def _collect(items: Any, status: str, since_ms: int, found: dict[str, ExecutionInfo]) -> int | None:
        """Add the page's executions of the window to ``found``; returns the oldest stop seen."""
        oldest: int | None = None
        for item in items if isinstance(items, list) else ():
            info = ExecutionInfo.from_api(item)
            stop_ms = info.stop_ms if info is not None else None
            if info is None or stop_ms is None:
                continue
            oldest = stop_ms if oldest is None else min(oldest, stop_ms)
            if stop_ms < since_ms or info.status != status:
                continue
            known = found.get(info.execution_arn)
            if known is None or (info.redrive_count, stop_ms) > (known.redrive_count, known.stop_ms or 0):
                found[info.execution_arn] = info
        return oldest

    def _process(self, pipeline_id: str, listed: list[ExecutionInfo], budget: _Budget) -> _Processed:
        """Localise the listed executions oldest first within the poll's budget.

        An execution a committed page returned is handled without being returned; a remembered
        one is returned again without I/O and outside the budget; anything else is localised while
        the budget lasts.
        """
        processed = _Processed()
        for info in listed:
            if info.key in self._committed:
                processed.handled(info)
                continue
            localized = self.localizer.remembered(info)
            if localized is None:
                if budget.spent:
                    budget.complete = False
                    processed.pending(info)
                    break
                result = self.localizer.localize(info)
                budget.histories -= result.reads
                if result.localized is None:  # not usable yet: checked again on the next poll
                    processed.pending(info)
                    continue
                localized = result.localized
                if not result.remembered:
                    budget.failures -= 1
            processed.failures.append(
                execution_failure(
                    self.rt,
                    pipeline_id=pipeline_id,
                    run_id=info.name,
                    info=info,
                    localized=localized,
                    source=POLL_SOURCE,
                )
            )
            processed.keys.append(info.key)
            processed.handled(info)
        return processed


def _machine_arn(value: Any) -> bool:
    """True for a (possibly qualified) state machine ARN."""
    if not isinstance(value, str):
        return False
    try:
        arns.parse_state_machine_arn(value)
    except ValueError:
        return False
    return True


__all__ = [
    "COMMITTED_CACHE_SIZE",
    "DEFAULT_POLL_LIMIT",
    "DISCOVERY_PAGE_SIZE",
    "LIST_PAGE_SIZE",
    "LOCALIZATION_CACHE_SIZE",
    "MAX_LIST_PAGES",
    "MAX_UNREADABLE_POLLS",
    "NOT_BEFORE_KEY",
    "POLL_OVERLAP_MS",
    "POLL_SOURCE",
    "WATERMARK_KEY",
    "ExecutionInfo",
    "ExecutionKey",
    "HistoryLocalizer",
    "Localization",
    "SfnFeed",
    "cursor_time",
    "execution_failure",
    "read_cursor",
]
