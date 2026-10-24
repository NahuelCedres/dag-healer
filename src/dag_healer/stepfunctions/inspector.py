"""Evidence of one failed execution: the :class:`~dag_healer.core.ports.RunInspector` (9.6).

``describe(ref, native)``:

* ``DescribeExecution`` (status, dates, redrive count; ``NotFound`` for an unknown execution or
  an attempt the execution never reached), then the history of the attempt's redrive segment
  (``executions.read_history`` with ``redrive_count = ref.attempt - 1``); the step failure is
  the event ``native.failed_event_id`` names, else :func:`~dag_healer.stepfunctions.history.localize`;
* the error and cause come from that event; a cause missing because of
  ``includeExecutionData=false`` [U] falls back to ``DescribeExecution.cause``;
* a Lambda JSON cause is parsed (:func:`~dag_healer.stepfunctions.causes.parse_cause`) and
  rendered as ``traceback_text`` (CPython format); otherwise ``traceback_text=""`` and
  ``raw_error="<error>: <cause>"``;
* ``executor`` is the resource type (``"lambda"``), ``resource`` the function; when the events
  do not name the function, the definition that ran (``DescribeStateMachineForExecution``) does
  (state name -> Task target), and ``native`` gains ``function_name``/``request_id``, plus
  ``log_group`` when the repository's template sets one for the function (the console link;
  read through the code locator, no AWS call).

``logs`` delegates to :mod:`dag_healer.stepfunctions.logs`. ``pipeline_source`` reports the
definition that ran (``DescribeStateMachineForExecution``) for an attempt, else the current one
(``DescribeStateMachine``), as ``language="asl-json"``.

The details:

* **Attempts.** ``attempt = redriveCount + 1``. The current attempt's state follows the
  execution status (``FAILED``/``TIMED_OUT``/``ABORTED`` -> ``FAILED``, final; ``RUNNING`` ->
  ``RUNNING``; ``SUCCEEDED`` -> ``SUCCEEDED``); an earlier attempt failed for good (a redrive is
  not an automatic retry, and Step Functions retries are never visible as attempts). A run id
  with an ``@<startDate ms>`` suffix (a reused execution name) must match the execution's start,
  else the execution is a newer one of the same name (``NotFound``).
* **The failure event.** :func:`~dag_healer.stepfunctions.history.localize` reads the segment;
  when ``native.failed_event_id`` (recorded at detection) names another failure event of the
  segment, that event is the evidence (the incident's own failure; its resource fields are the
  ones recorded with it in ``native``). A :data:`~dag_healer.core.ids.RUN_LEVEL_STEP` ref uses
  the segment's terminal event.
* **The function.** ``native.function_name`` (recorded at detection), else the one the events
  name (``TaskScheduled`` parameters, the legacy ``LambdaFunctionScheduled`` resource), else the
  definition that ran, read once and only for a state that may be a Lambda Task; placeholders
  and paths (``${Fn}``, ``FunctionName.$``) are not function names.
* **Unreadable history.** A refused history (KMS key policy, permissions) is not fatal: the
  current attempt falls back to the execution's own ``error``/``cause``/dates. Transient errors
  and ``NotFound`` propagate.
* **Tracebacks.** A Lambda payload cut by the 32 KB cause limit before its first frame gives
  ``traceback_text=""``, so the core reads the traceback from the log instead.
* **native.** The given ``native`` updated with what this describe found (ids, names, the
  request id; never input, output or cause text); ``status``/``stop_date`` only for the current
  attempt; ``localized`` is left as the incident recorded it. ``native.log_group`` (recorded, else
  the template's ``LoggingConfig.LogGroup``) is also the log group the log lookup falls back to
  when ``GetFunction`` cannot tell.

Errors leave as :mod:`dag_healer.core.errors` exceptions; log lines name ids and exception
classes only, never causes, messages or execution data.
"""

from __future__ import annotations

import logging
from collections.abc import Mapping, Sequence
from dataclasses import dataclass, fields
from datetime import datetime
from pathlib import Path
from typing import TYPE_CHECKING, Any

from dag_healer.core.errors import NotFound, OrchestratorError, PermissionDenied
from dag_healer.core.evidence import AttemptDetail, LogBundle, PipelineSource
from dag_healer.core.failures import AttemptState
from dag_healer.core.ids import RUN_LEVEL_STEP, AttemptRef
from dag_healer.stepfunctions import arns, asl, causes, executions, history
from dag_healer.stepfunctions import logs as lambda_logs
from dag_healer.stepfunctions.runtime import SfnRuntime, native_int, native_ref, native_text, to_utc

if TYPE_CHECKING:
    from dag_healer.stepfunctions.locator import SfnCodeLocator

logger = logging.getLogger(__name__)

DEFAULT_LOG_MAX_CHARS = 2_000_000
#: ``raw_error`` is cut to this many characters (a cause is at most 32 KB).
MAX_RAW_ERROR_CHARS = 64 * 1024
SOURCE_LANGUAGE = "asl-json"
ORIGIN_RAN = "api:DescribeStateMachineForExecution"
ORIGIN_CURRENT = "api:DescribeStateMachine"
LAMBDA = "lambda"

_FAILED_STATUSES = frozenset({"FAILED", "TIMED_OUT", "ABORTED"})
_RUNNING_STATUSES = frozenset({"RUNNING", "PENDING_REDRIVE"})
_SEGMENT_STARTS = frozenset({"ExecutionStarted", "ExecutionRedriven"})
_FAIL_STATE_ENTERED = "FailStateEntered"
#: ``previousEventId`` hops from a failure event back to its state entry, at most.
_MAX_WALK = 64
#: Step failures that come from a Task's resource (the others: Map Run, ASL evaluation, Fail state).
_TASK_FAILURE_PREFIXES = ("Task", "LambdaFunction", "Activity")


@dataclass(frozen=True)
class _Evidence:
    """What the history (or the execution) says about the failed attempt. Texts are RAW."""

    error: str | None = None
    cause: str | None = None
    event_type: str | None = None
    failed_event_id: int | None = None
    entered_event_id: int | None = None
    started_at: datetime | None = None
    failed_at: datetime | None = None
    resource: str | None = None
    resource_type: str | None = None
    function_name: str | None = None
    retry_attempts: int | None = None
    step_id: str | None = None  # the state the evidence is about (None: run level)
    step_path: str | None = None
    map_run_arn: str | None = None


class SfnInspector:
    """``RunInspector`` port."""

    def __init__(self, rt: SfnRuntime, *, locator: SfnCodeLocator | None = None) -> None:
        self.rt = rt
        self.locator = locator

    # ------------------------------------------------------------------ describe
    def describe(self, ref: AttemptRef, native: Mapping[str, Any]) -> AttemptDetail:
        base = dict(native) if isinstance(native, Mapping) else {}
        arn = self._execution_arn(ref, base)
        execution = executions.describe_execution(self.rt, arn)
        attempt, current = _attempt_of(ref, execution)
        state, is_final = _attempt_state(execution.get("status"), current=current)
        evidence = self._evidence(ref, base, arn, attempt - 1, execution, current=current)
        error, cause = _error_and_cause(evidence, execution, current=current)
        info = causes.parse_cause(cause, error=error)
        error_name = error or (info.error_type if info is not None and info.error_type else None)
        function, executor = self._function_and_executor(ref, base, arn, evidence)
        resource = evidence.resource or native_text(base, "resource")
        request_id = (info.request_id if info is not None else None) or native_text(base, "request_id")
        log_group = native_text(base, "log_group")
        if log_group is None and function is not None and executor == LAMBDA:
            log_group = self._template_log_group(ref, evidence)
        values = _native_fields(
            arn, execution, evidence, current=current, redrive_count=attempt - 1, error=error_name
        )
        values.update(
            resource=resource,
            resource_type=executor,
            function_name=function,
            request_id=request_id,
            log_group=log_group,
        )
        traceback_text = _traceback_text(info)
        logger.debug(
            "Step Functions inspector %s: described run %s of %s (attempt %s): %s, %s, traceback %s",
            self.rt.instance,
            ref.run_id,
            ref.pipeline_id,
            ref.attempt,
            state.value,
            evidence.event_type or "no failure event",
            "rendered" if traceback_text else "none",
        )
        return AttemptDetail(
            ref=ref,
            state=state,
            is_final=is_final,
            error_name=error_name,
            error_message=info.error_message if info is not None else cause,
            traceback_text=traceback_text,
            raw_error=_raw_error(error_name, cause),
            executor=executor,
            resource=function or resource,
            started_at=evidence.started_at,
            ended_at=evidence.failed_at,
            max_attempts=None,
            native=_detail_native(base, values),
        )

    def _function_and_executor(
        self, ref: AttemptRef, native: Mapping[str, Any], arn: str, evidence: _Evidence
    ) -> tuple[str | None, str | None]:
        """The Lambda function of the failed state and the executor (resource type).

        ``native.function_name`` (recorded at detection), else the events', else the definition
        that ran (one ``DescribeStateMachineForExecution``) for a state that may be a Lambda Task.
        """
        function = native_text(native, "function_name") or evidence.function_name
        executor = (
            evidence.resource_type
            or native_text(native, "resource_type")
            or _executor_of(evidence.event_type)
        )
        step = _failed_state(ref, evidence)
        if function is None and step is not None and _may_run_lambda(evidence, executor):
            function = self._function_from_definition(arn, step)
        if function is not None and executor is None:
            executor = LAMBDA
        return function, executor

    def _template_log_group(self, ref: AttemptRef, evidence: _Evidence) -> str | None:
        """The log group the repository's template sets for the failed state's function
        (``LoggingConfig.LogGroup``; the code locator reads it, no AWS call); ``None`` when the
        function logs to its default group, or without a locator or a repository."""
        step = _failed_state(ref, evidence)
        repo_root = getattr(self.rt.cfg, "repo_root", None) if self.rt.cfg is not None else None
        if self.locator is None or step is None or repo_root is None:
            return None
        try:
            return self.locator.log_group(Path(repo_root), ref.pipeline_id, step)
        except (OSError, ValueError) as exc:  # an unreadable repository costs the link only
            logger.debug(
                "Step Functions inspector %s: no log group of %s from the repository (%s)",
                self.rt.instance,
                step,
                type(exc).__name__,
            )
            return None

    def _execution_arn(self, ref: AttemptRef, native: Mapping[str, Any]) -> str:
        """The execution of ``ref`` (``NotFound`` when the pipeline's machine is unknown)."""
        try:
            arn = self.rt.execution_arn(ref, native)
        except ValueError:  # a malformed state machine ARN in native or in the options
            arn = None
        if arn is None:
            raise NotFound(
                f"pipeline {ref.pipeline_id} has no known state machine: run {ref.run_id} cannot be found"
            )
        return arn

    def _evidence(
        self,
        ref: AttemptRef,
        native: Mapping[str, Any],
        arn: str,
        redrive_count: int,
        execution: Mapping[str, Any],
        *,
        current: bool,
    ) -> _Evidence:
        """The failure of the attempt's redrive segment (see the module docstring)."""
        try:
            read = executions.read_history(self.rt, arn, redrive_count=redrive_count)
        except OrchestratorError as exc:
            if exc.transient or isinstance(exc, NotFound):
                raise
            logger.debug(
                "Step Functions inspector %s: the history of run %s is unreadable (%s)",
                self.rt.instance,
                ref.run_id,
                type(exc).__name__,
            )
            return _execution_evidence(execution) if current else _Evidence()
        events = read.events
        fallback = _execution_evidence(execution) if current else _Evidence()
        if ref.step_id == RUN_LEVEL_STEP:
            return _terminal_evidence(history.segment(events, redrive_count)) or fallback
        localized = history.localize(events, redrive_count=redrive_count)
        recorded = native_int(native, "failed_event_id")
        if recorded is not None and recorded != localized.failed_event_id:
            evidence = _recorded_evidence(history.segment(events, redrive_count), recorded, localized)
            if evidence is not None:
                return evidence
        if localized.failed_event_id is None and not localized.terminal:
            return _merge(_localized_evidence(localized), fallback)
        return _localized_evidence(localized)

    def _function_from_definition(self, execution_arn: str, state_name: str) -> str | None:
        """The Lambda function the definition that ran gives ``state_name`` (literal names only)."""
        try:
            data = executions.describe_state_machine_for_execution(self.rt, execution_arn)
        except OrchestratorError as exc:
            if exc.transient:
                raise
            logger.debug(
                "Step Functions inspector %s: no definition for the function of %s (%s)",
                self.rt.instance,
                state_name,
                type(exc).__name__,
            )
            return None
        definition = data.get("definition")
        if not isinstance(definition, str) or not definition.strip():
            return None
        try:
            states = asl.index_states(definition)
        except ValueError:
            return None
        state = states.get(state_name)
        target = asl.compute_target(state) if state is not None else None
        if target is None or target.kind != LAMBDA:
            return None
        return _literal_function(target.function)

    # ------------------------------------------------------------------ logs
    def logs(
        self, ref: AttemptRef, detail: AttemptDetail | None, *, max_chars: int = DEFAULT_LOG_MAX_CHARS
    ) -> LogBundle:
        """The CloudWatch log excerpt of the failed Lambda attempt (:mod:`~dag_healer.stepfunctions.logs`).

        Never raises for missing logs: without ``detail`` the attempt is described first, and a
        missing (or unreadable) attempt gives an empty bundle with a note. Transient AWS errors
        propagate.
        """
        if not self.rt.options.log_lookup:
            return lambda_logs.empty_bundle(lambda_logs.LOOKUP_DISABLED_NOTE)
        if detail is None:
            try:
                detail = self.describe(ref, {})
            except OrchestratorError as exc:
                if exc.transient:
                    raise
                return lambda_logs.empty_bundle(_no_attempt_note(ref, exc))
        if ref.step_id == RUN_LEVEL_STEP:
            return lambda_logs.empty_bundle(
                "The execution failed outside any state: there is no Lambda log to correlate."
            )
        if detail.executor not in (None, LAMBDA):
            return lambda_logs.empty_bundle(
                f"State {ref.step_id} ran a {detail.executor} task: only Lambda logs are correlated."
            )
        function = native_text(detail.native, "function_name") or _lambda_arn(detail.resource)
        if function is None:
            return lambda_logs.empty_bundle(
                f"The Lambda function of state {ref.step_id} is unknown: there is no log to correlate."
            )
        return lambda_logs.find_lambda_logs(
            self.rt,
            function=function,
            started_at=detail.started_at,
            failed_at=detail.ended_at,
            request_id=native_text(detail.native, "request_id"),
            max_chars=max_chars,
            log_group=native_text(detail.native, "log_group"),
        )

    # ------------------------------------------------------------------ pipeline source
    def pipeline_source(
        self,
        pipeline_id: str,
        *,
        ref: AttemptRef | None = None,
        native: Mapping[str, Any] | None = None,
        include_text: bool = False,
    ) -> PipelineSource | None:
        """The definition that ran for an attempt (``ref`` or ``native.execution_arn``), else the
        current definition of the pipeline's machine; ``None`` for an unknown pipeline.

        An execution that no longer exists falls back to the current definition;
        ``NotFound`` for a machine that no longer exists.
        """
        execution_arn = self._attempt_arn(ref, native)
        if execution_arn is not None:
            try:
                data = executions.describe_state_machine_for_execution(self.rt, execution_arn)
            except NotFound:
                data = None
            if data is not None:
                return _pipeline_source(data, None, include_text=include_text, origin=ORIGIN_RAN)
        machine = native_text(native, "state_machine_arn") or self.rt.state_machine_arn(pipeline_id)
        if machine is None:
            return None
        data = executions.describe_state_machine(self.rt, machine)
        return _pipeline_source(data, machine, include_text=include_text, origin=ORIGIN_CURRENT)

    def _attempt_arn(self, ref: AttemptRef | None, native: Mapping[str, Any] | None) -> str | None:
        if ref is None:
            return native_text(native, "execution_arn")
        try:
            return self.rt.execution_arn(ref, native)
        except ValueError:
            return None


# =========================================================================== execution helpers


def _text(value: Any) -> str | None:
    """Non-blank text (RAW), else ``None``."""
    return value if isinstance(value, str) and value.strip() else None


def _redrive_count(execution: Mapping[str, Any]) -> int:
    value = execution.get("redriveCount")
    return value if isinstance(value, int) and not isinstance(value, bool) and value > 0 else 0


def _attempt_of(ref: AttemptRef, execution: Mapping[str, Any]) -> tuple[int, bool]:
    """``(attempt, current)`` of ``ref`` (``NotFound`` for an attempt the execution never
    reached, or for an older execution of a reused name)."""
    redrive_count = _redrive_count(execution)
    attempt = max(ref.attempt, 1)
    if attempt > redrive_count + 1:
        raise NotFound(
            f"execution {ref.run_id} of pipeline {ref.pipeline_id} has {redrive_count + 1} "
            f"attempt(s), not attempt {ref.attempt}"
        )
    _check_reused_name(ref, execution)
    return attempt, attempt == redrive_count + 1


def _check_reused_name(ref: AttemptRef, execution: Mapping[str, Any]) -> None:
    """``NotFound`` when ``ref.run_id`` (``<name>@<ms>``) names an older execution of that name.

    A run id that is the execution's own name always matches: ``@`` and digits are valid in an
    execution name (``nightly@1790920800000``).
    """
    name = _execution_name(execution) or arns.execution_name(ref.run_id)
    start = to_utc(execution.get("startDate"))
    if not arns.run_names_execution(ref.run_id, name, start):
        raise NotFound(f"execution {name} was started again under the same name: run {ref.run_id} is gone")


def _execution_name(execution: Mapping[str, Any]) -> str | None:
    """The name ``DescribeExecution`` gives (else the one of its execution ARN)."""
    name = _text(execution.get("name"))
    if name is not None:
        return name
    arn = _text(execution.get("executionArn"))
    try:
        return arns.parse_execution_arn(arn).name if arn is not None else None
    except ValueError:
        return None


def _attempt_state(status: Any, *, current: bool) -> tuple[AttemptState, bool]:
    """``(state, is_final)`` of an attempt (see the module docstring)."""
    if not current:
        return AttemptState.FAILED, True
    value = status.strip().upper() if isinstance(status, str) else ""
    if value in _FAILED_STATUSES:
        return AttemptState.FAILED, True
    if value in _RUNNING_STATUSES:
        return AttemptState.RUNNING, False
    if value == "SUCCEEDED":
        return AttemptState.SUCCEEDED, False
    return AttemptState.UNKNOWN, False


def _execution_evidence(execution: Mapping[str, Any]) -> _Evidence:
    """The execution's own error, cause and dates (no history)."""
    return _Evidence(
        error=_text(execution.get("error")),
        cause=_text(execution.get("cause")),
        started_at=to_utc(execution.get("redriveDate")) or to_utc(execution.get("startDate")),
        failed_at=to_utc(execution.get("stopDate")),
    )


def _error_and_cause(
    evidence: _Evidence, execution: Mapping[str, Any], *, current: bool
) -> tuple[str | None, str | None]:
    """The evidence's error and cause; a missing cause falls back to ``DescribeExecution.cause``
    for the current attempt when the errors agree (or the evidence names none)."""
    error, cause = evidence.error, evidence.cause
    if cause is None and current:
        fallback_error, fallback_cause = _text(execution.get("error")), _text(execution.get("cause"))
        if fallback_cause is not None and (error is None or fallback_error in (None, error)):
            cause, error = fallback_cause, error or fallback_error
    return error, cause


def _native_fields(
    arn: str,
    execution: Mapping[str, Any],
    evidence: _Evidence,
    *,
    current: bool,
    redrive_count: int,
    error: str | None,
) -> dict[str, Any]:
    """The ``native_ref`` fields of the execution and the evidence (``status``, ``stop_date`` and
    the version only for the current attempt: they describe the latest one)."""
    fields: dict[str, Any] = {
        "execution_arn": arn,
        "state_machine_arn": _text(execution.get("stateMachineArn")),
        "start_date": execution.get("startDate"),
        "redrive_count": redrive_count,
        "error": error,
        "failed_event_id": evidence.failed_event_id,
        "entered_event_id": evidence.entered_event_id,
        "event_type": evidence.event_type,
        "retry_attempts": evidence.retry_attempts,
        "step_path": evidence.step_path,
        "map_run_arn": evidence.map_run_arn,
    }
    if current:
        fields.update(
            status=_text(execution.get("status")),
            stop_date=execution.get("stopDate"),
            definition_version_arn=_text(execution.get("stateMachineVersionArn")),
        )
    return fields


def _traceback_text(info: causes.CauseInfo | None) -> str:
    """The rendered Lambda traceback; ``""`` without a payload, or for one cut before its first
    frame (the log then holds the traceback)."""
    if info is None or (info.truncated and not info.frames):
        return ""
    return info.traceback_text


def _raw_error(error: str | None, cause: str | None) -> str:
    """``"<error>: <cause>"`` (or whichever is present), RAW and capped."""
    text = f"{error}: {cause}" if error and cause else (error or cause or "")
    return text[:MAX_RAW_ERROR_CHARS]


def _detail_native(base: Mapping[str, Any], fields: Mapping[str, Any]) -> dict[str, Any]:
    """``base`` updated with the known ``fields`` (``None`` keeps the base value: ``native_ref``
    would drop the key); when that exceeds 4 KB, the fields alone, then the ids."""
    found = {key: value for key, value in fields.items() if value is not None}
    ids = {key: found[key] for key in ("execution_arn", "state_machine_arn", "redrive_count") if key in found}
    for source, values in ((base, found), (None, found), (None, ids)):
        try:
            return native_ref(source, **values)
        except ValueError:
            continue
    return {}


# =========================================================================== history helpers


def _event_id(event: Mapping[str, Any] | None) -> int | None:
    value = event.get("id") if isinstance(event, Mapping) else None
    return value if isinstance(value, int) and not isinstance(value, bool) and value > 0 else None


def _event_type(event: Mapping[str, Any] | None) -> str:
    value = event.get("type") if isinstance(event, Mapping) else None
    return value if isinstance(value, str) else ""


def _event_time(event: Mapping[str, Any] | None) -> datetime | None:
    return to_utc(event.get("timestamp")) if isinstance(event, Mapping) else None


def _localized_evidence(localized: history.Localized) -> _Evidence:
    return _Evidence(
        error=localized.error,
        cause=localized.cause,
        event_type=localized.event_type,
        failed_event_id=localized.failed_event_id,
        entered_event_id=localized.entered_event_id,
        started_at=localized.started_at,
        failed_at=localized.failed_at,
        resource=localized.resource,
        resource_type=localized.resource_type,
        function_name=localized.function_name,
        retry_attempts=localized.retry_attempts,
        step_id=None if localized.step_id == RUN_LEVEL_STEP else localized.step_id,
        step_path=localized.step_path,
        map_run_arn=localized.map_run_arn,
    )


def _merge(evidence: _Evidence, fallback: _Evidence) -> _Evidence:
    """``evidence`` with its missing values (error, cause, dates) taken from ``fallback``."""
    values = {item.name: getattr(evidence, item.name) for item in fields(_Evidence)}
    return _Evidence(
        **{name: value if value is not None else getattr(fallback, name) for name, value in values.items()}
    )


def _terminal_evidence(segment: Sequence[Mapping[str, Any]]) -> _Evidence | None:
    """The terminal event of a segment (``ExecutionFailed``/``TimedOut``/``Aborted``), if any."""
    terminal = next((e for e in reversed(segment) if _event_type(e) in history.TERMINAL_EVENT_TYPES), None)
    if terminal is None:
        return None
    details = history.event_details(terminal)
    first = segment[0] if segment and _event_type(segment[0]) in _SEGMENT_STARTS else None
    return _Evidence(
        error=_text(details.get("error")),
        cause=_text(details.get("cause")),
        event_type=_event_type(terminal),
        failed_event_id=_event_id(terminal),
        started_at=_event_time(first),
        failed_at=_event_time(terminal),
    )


def _recorded_evidence(
    segment: Sequence[Mapping[str, Any]], event_id: int, localized: history.Localized
) -> _Evidence | None:
    """The failure event ``event_id`` of the segment (``None`` when it is not a step failure of
    the segment). Its error/cause fall back to the terminal event's like ``localize`` does; the
    resource fields stay those recorded with the id in ``native``."""
    by_id = _by_id(segment)
    event = by_id.get(event_id)
    kind = _event_type(event)
    if event is None or kind not in history.STEP_FAILURE_TYPES:
        return None
    details = history.event_details(event)
    error, cause = _text(details.get("error")), _text(details.get("cause"))
    if kind == _FAIL_STATE_ENTERED or (error is None and cause is None):
        error, cause = localized.terminal_error, localized.terminal_cause
    elif cause is None and error is not None and error == localized.terminal_error:
        cause = localized.terminal_cause
    started, entry = _attempt_events(event, by_id)
    name = history.event_details(entry).get("name") if entry is not None else None
    if kind == "EvaluationFailed":
        name = details.get("state") or name
    return _Evidence(
        error=error,
        cause=cause,
        event_type=kind,
        failed_event_id=event_id,
        entered_event_id=_event_id(entry),
        started_at=_event_time(started),
        failed_at=_event_time(event),
        step_id=_text(name),
    )


def _by_id(events: Sequence[Mapping[str, Any]]) -> dict[int, Mapping[str, Any]]:
    """The events by id (the first of a repeated id wins; events without an id are dropped)."""
    indexed: dict[int, Mapping[str, Any]] = {}
    for event in events:
        event_id = _event_id(event)
        if event_id is not None:
            indexed.setdefault(event_id, event)
    return indexed


def _attempt_events(
    event: Mapping[str, Any], by_id: Mapping[int, Mapping[str, Any]]
) -> tuple[Mapping[str, Any] | None, Mapping[str, Any] | None]:
    """``(*Started, *StateEntered)`` of a failure event, following ``previousEventId`` back
    (at most :data:`_MAX_WALK` hops; a link that does not point back ends the walk)."""
    if _event_type(event).endswith("StateEntered"):
        return None, event
    started: Mapping[str, Any] | None = None
    current: Mapping[str, Any] | None = event
    for _hop in range(_MAX_WALK):
        previous = _previous_id(current)
        current = by_id.get(previous) if previous is not None else None
        kind = _event_type(current)
        if current is None or kind.endswith("StateExited") or kind in _SEGMENT_STARTS:
            break
        if kind.endswith("StateEntered"):
            return started, current
        if started is None and kind in history.STARTED_TYPES:
            started = current
    return started, None


def _previous_id(event: Mapping[str, Any] | None) -> int | None:
    """``previousEventId`` when it points back to an earlier event (0 and odd links do not)."""
    own = _event_id(event)
    value = event.get("previousEventId") if isinstance(event, Mapping) else None
    if own is None or not isinstance(value, int) or isinstance(value, bool) or not 0 < value < own:
        return None
    return value


def _failed_state(ref: AttemptRef, evidence: _Evidence) -> str | None:
    """The state the evidence is about, else the ref's (``None`` at run level)."""
    if evidence.step_id:
        return evidence.step_id
    return ref.step_id if ref.resolved and ref.step_id != RUN_LEVEL_STEP else None


def _executor_of(event_type: str | None) -> str | None:
    """The resource type implied by a failure event type (legacy Lambda and Activity events)."""
    if not event_type:
        return None
    if event_type.startswith("LambdaFunction"):
        return LAMBDA
    if event_type.startswith("Activity"):
        return "activity"
    return None


def _may_run_lambda(evidence: _Evidence, executor: str | None) -> bool:
    """True when the failed state may be a Lambda Task whose function the events do not name."""
    if executor not in (None, LAMBDA):
        return False
    kind = evidence.event_type
    return kind is None or kind.startswith(_TASK_FAILURE_PREFIXES)


def _literal_function(value: Any) -> str | None:
    """A function name/ARN written literally (not a ``${...}`` placeholder or a path/JSONata)."""
    if not isinstance(value, str) or not value.strip():
        return None
    text = value.strip()
    if text.startswith("$") or "${" in text or "{%" in text:
        return None
    return text


def _lambda_arn(resource: str | None) -> str | None:
    """``resource`` when it is a Lambda function ARN (legacy Lambda Tasks), else ``None``."""
    if not isinstance(resource, str) or not arns.is_arn(resource):
        return None
    try:
        return resource if arns.parse_arn(resource).service == LAMBDA else None
    except ValueError:
        return None


def _no_attempt_note(ref: AttemptRef, exc: OrchestratorError) -> str:
    """Why there is no log for an attempt that cannot be described (ids only, no AWS text)."""
    run = f"run {ref.run_id} of {ref.pipeline_id} (attempt {ref.attempt})"
    if isinstance(exc, NotFound):
        return f"Step Functions knows no {run}: there is no log to correlate."
    if isinstance(exc, PermissionDenied):
        return f"Describing {run} was refused: there is no log to correlate."
    return f"Describing {run} failed ({type(exc).__name__}): there is no log to correlate."


def _pipeline_source(
    data: Mapping[str, Any], machine: str | None, *, include_text: bool, origin: str
) -> PipelineSource:
    definition = data.get("definition")
    return PipelineSource(
        path=_text(data.get("stateMachineArn")) or machine,
        relative_path=None,
        text=definition if include_text and isinstance(definition, str) else None,
        language=SOURCE_LANGUAGE,
        origin=origin,
    )


__all__ = ["DEFAULT_LOG_MAX_CHARS", "MAX_RAW_ERROR_CHARS", "ORIGIN_CURRENT", "ORIGIN_RAN", "SfnInspector"]
