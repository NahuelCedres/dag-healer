"""Push detection: the :class:`~dag_healer.core.ports.PushIntake` of Step Functions (section 9.5).

``normalize`` is :func:`dag_healer.stepfunctions.events.normalize` (pure). ``confirm`` checks a
hint against the API:

1. ``DescribeExecution(execution_arn)``; ``ExecutionDoesNotExist``/``InvalidArn`` ->
   ``discarded`` ("forged or deleted"); throttling, 5xx, network or a permission problem ->
   ``retry_later``;
2. the hint's attempt still running (``RUNNING``, or ``PENDING_REDRIVE`` of a Map Run child;
   ``DescribeExecution`` is eventually consistent) -> ``retry_later`` with
   ``state=AttemptState.RUNNING``: the core checks it again on its next cycle, without counting
   an error; any other status outside ``options.statuses`` -> ``discarded``;
3. ``redriveCount + 1 != hint.ref.attempt`` -> ``discarded`` ("stale; a newer attempt
   exists"); an execution of another machine/name than the hint names -> ``discarded``;
4. localise (``executions.read_history`` + ``history.localize``) and return
   ``confirmed`` with one ``Failure(source=hint.source, is_final=True, state=FAILED)`` whose ref
   is the hint's with the step localised.

An unreadable history (KMS denied, an Express execution, an unusable segment) gives
``retry_later``; after 3 such checks of the same ``(execution, redrive)`` (counted in memory,
together with the feed's checks when the adapter gives both ports one
:class:`~dag_healer.stepfunctions.feed.HistoryLocalizer`) the hint is confirmed with
:data:`~dag_healer.core.ids.RUN_LEVEL_STEP` and a note in the reason. ``trigger`` is ignored:
Step Functions retries are never visible, so every confirmed failure is final.

Details: the execution ARN is the hint's ``native.execution_arn``, else the execution
``ref.run_id`` (an ``@<startDate ms>`` suffix dropped) of the pipeline's state machine; a hint of
another orchestrator instance, or naming no usable execution, is discarded without I/O. The
described execution must be the run: its name is ``ref.run_id``, or ``ref.run_id`` is
``<name>@<startDate ms>`` with the execution's own start (a reused name is another execution;
``@`` and digits are valid in names, so a name such as ``nightly@1790920800000`` is its own run
id). A transient error while reading the history gives ``retry_later`` without counting. Reasons
name statuses, counts and error classes, never AWS messages or causes.
"""

from __future__ import annotations

import logging
from collections.abc import Mapping
from typing import Any

from dag_healer.core.errors import NotFound, OrchestratorError
from dag_healer.core.failures import AttemptState, Confirmation, FailureHint, Trigger
from dag_healer.stepfunctions import arns, events, executions
from dag_healer.stepfunctions.events import CHANNEL, SOURCE_EVENTBRIDGE
from dag_healer.stepfunctions.feed import ExecutionInfo, HistoryLocalizer, execution_failure
from dag_healer.stepfunctions.runtime import SfnRuntime

logger = logging.getLogger(__name__)

MAX_UNREADABLE_CHECKS = 3
#: Incident sources a confirmed hint may keep (anything else becomes ``eventbridge``).
HINT_SOURCES: tuple[str, ...] = ("eventbridge", "sqs", "cli")
#: Statuses of an attempt that has not ended (``PENDING_REDRIVE``: a Map Run child).
RUNNING_STATUSES = frozenset({"RUNNING", "PENDING_REDRIVE"})


def _discarded(reason: str) -> Confirmation:
    return Confirmation(verdict="discarded", reason=reason)


def _retry_later(reason: str) -> Confirmation:
    return Confirmation(verdict="retry_later", reason=reason)


class SfnIntake:
    """``PushIntake`` port: the ``eventbridge`` channel (API destination or SQS)."""

    channels = frozenset({CHANNEL})

    def __init__(self, rt: SfnRuntime, *, localizer: HistoryLocalizer | None = None) -> None:
        self.rt = rt
        self.localizer = (
            localizer if localizer is not None else HistoryLocalizer(rt, max_unusable=MAX_UNREADABLE_CHECKS)
        )

    def __repr__(self) -> str:
        return f"SfnIntake(instance={self.rt.instance!r})"

    def normalize(
        self, channel: str, payload: Mapping[str, Any], headers: Mapping[str, str]
    ) -> list[FailureHint]:
        """PURE. The hints of an EventBridge envelope or an SQS message (``headers`` are not used)."""
        if channel not in self.channels:
            raise ValueError(
                f"unknown push channel {channel!r} (the Step Functions adapter accepts: {CHANNEL})"
            )
        return events.normalize(
            payload,
            instance=self.rt.instance,
            pipelines=self.rt.pipelines,
            statuses=self.rt.options.statuses,
            channel=channel,
        )

    def confirm(self, hint: FailureHint, *, trigger: Trigger) -> Confirmation:
        """Check ``hint`` against the API and localise it (see the module docstring)."""
        result = self._confirm(hint)
        ref = hint.ref
        logger.debug(
            "dag-healer stepfunctions %s: %s hint of %s run %s (attempt %s): %s",
            self.rt.instance,
            hint.source,
            ref.pipeline_id,
            ref.run_id,
            ref.attempt,
            result.verdict,
        )
        return result

    # ------------------------------------------------------------------ the checks
    def _confirm(self, hint: FailureHint) -> Confirmation:
        ref = hint.ref
        if ref.orchestrator != self.rt.instance:
            return _discarded(
                f"the hint is for orchestrator instance {ref.orchestrator!r}, not {self.rt.instance!r}"
            )
        try:
            execution_arn = self.rt.execution_arn(ref, hint.native)
        except OrchestratorError as exc:
            return self._failed_call("resolving the state machine", exc)
        if execution_arn is None:
            return _discarded(f"pipeline {ref.pipeline_id} has no known state machine")
        try:
            target = arns.parse_execution_arn(execution_arn)
        except ValueError:
            return _discarded("the hint does not name a Step Functions execution")
        if target.express:
            return _discarded("Express executions have no execution history (not supported)")
        try:
            described = executions.describe_execution(self.rt, execution_arn)
        except OrchestratorError as exc:
            return self._failed_call("DescribeExecution", exc)
        info = ExecutionInfo.from_api(described)
        if info is None:
            return _retry_later("DescribeExecution gave an answer without execution ARN or status")
        problem = self._not_the_run(hint, info)
        if problem is not None:
            return _discarded(problem)
        if info.status in RUNNING_STATUSES and info.attempt == ref.attempt:
            return Confirmation(
                verdict="retry_later",
                reason=f"attempt {ref.attempt} has not ended yet (the execution is {info.status})",
                state=AttemptState.RUNNING,
            )
        problem = self._not_the_failure(hint, info)
        if problem is not None:
            return _discarded(problem)
        return self._localized(hint, info)

    def _failed_call(self, what: str, exc: OrchestratorError) -> Confirmation:
        if isinstance(exc, NotFound):
            return _discarded(
                f"the execution does not exist (forged or deleted): {what} gave {type(exc).__name__}"
            )
        kind = "transient " if exc.transient else ""
        return _retry_later(f"{what} failed ({kind}{type(exc).__name__})")

    def _not_the_run(self, hint: FailureHint, info: ExecutionInfo) -> str | None:
        """Why the described execution is not the hint's run (``None``: it is)."""
        ref = hint.ref
        pipeline_id = self.rt.pipelines.pipeline_id_for(info.state_machine_arn)
        known = self.rt.pipelines.known_arn(ref.pipeline_id)
        if pipeline_id != ref.pipeline_id or (
            known is not None and _unqualified(info.state_machine_arn) != known
        ):
            return f"the execution belongs to another state machine than pipeline {ref.pipeline_id}"
        if not arns.run_names_execution(ref.run_id, info.name, info.start_date):
            if info.name == arns.execution_name(ref.run_id):
                return f"run {ref.run_id} names an earlier execution of the same name"
            return f"the execution is not run {ref.run_id}"
        return None

    def _not_the_failure(self, hint: FailureHint, info: ExecutionInfo) -> str | None:
        """Why the run's described state is not the hint's failed attempt (``None``: it is)."""
        ref = hint.ref
        if info.status not in self.rt.options.statuses and info.attempt == ref.attempt:
            return f"the execution is {info.status}, not one of {', '.join(self.rt.options.statuses)}"
        if info.attempt != ref.attempt:
            if info.attempt > ref.attempt:
                return f"stale: attempt {ref.attempt} was redriven, attempt {info.attempt} exists"
            return f"the execution has no attempt {ref.attempt} (redrive count {info.redrive_count})"
        return None

    def _localized(self, hint: FailureHint, info: ExecutionInfo) -> Confirmation:
        try:
            outcome = self.localizer.localize(info)
        except OrchestratorError as exc:  # transient: not counted
            return _retry_later(f"reading the execution history failed (transient {type(exc).__name__})")
        localized = outcome.localized
        if localized is None:
            return _retry_later(
                f"the execution history is not usable yet ({outcome.problem}; check {outcome.checks} of "
                f"{MAX_UNREADABLE_CHECKS})"
            )
        ref = hint.ref
        failure = execution_failure(
            self.rt,
            pipeline_id=ref.pipeline_id,
            run_id=ref.run_id,
            info=info,
            localized=localized,
            source=hint.source if hint.source in HINT_SOURCES else SOURCE_EVENTBRIDGE,
        )
        if not localized.from_history:
            note = localized.notes[0] if localized.notes else "The execution history could not be used."
            reason = f"{info.status}; confirmed as a run-level failure. {note}"
        elif localized.run_level:
            reason = f"{info.status}; no failing state in the history: a run-level failure"
        else:
            where = localized.step_path or localized.step_id
            reason = f"{info.status}; localised to {where} ({localized.event_type})"
        return Confirmation(verdict="confirmed", reason=reason, failures=(failure,))


def _unqualified(state_machine_arn: str) -> str | None:
    """The unqualified ARN of a (version- or alias-qualified) state machine ARN."""
    try:
        return arns.parse_state_machine_arn(state_machine_arn).unqualified
    except ValueError:
        return None


__all__ = ["HINT_SOURCES", "MAX_UNREADABLE_CHECKS", "SfnIntake"]
