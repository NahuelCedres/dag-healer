"""Redrive and restart: the :class:`~dag_healer.core.ports.Remediator` (section 9.9).

``available(ref, native)`` (no I/O; a ref whose step is still unresolved gets nothing):

* ``RESUME_RUN`` when ``options.retry_strategy == "redrive"``, the execution is a Standard one
  (``native.execution_arn`` is not an Express ARN), ``native.status`` is a failed status
  (``FAILED``/``TIMED_OUT``/``ABORTED``; ``SUCCEEDED``, ``RUNNING`` ... are not) and
  ``native.stop_date`` is within 14 days of the clock;
* ``RERUN_PIPELINE`` only when ``options.rerun`` is true (and the execution is neither an
  Express execution nor a Distributed Map child, which cannot be started on their own).

A value ``native`` does not hold (an incident without a status or stop date) does not rule a
kind out: AWS decides, and a refusal comes back as ``accepted=False``. A ``native.execution_arn``
that is not an execution ARN rules both kinds out (there is nothing to act on).

``RESUME_RUN``: ``DescribeExecution`` first: ``native`` holds what the execution was when the
incident was detected, and a redrive acts on the execution's *current* attempt. The redrive is
not sent (``accepted=False``, nothing called) when the execution is no longer the incident's
attempt: it was redriven since (attempt ``redriveCount + 1`` is newer: redrive it from that
attempt's incident; a redrive still running is not repeated), it never reached the attempt, or
the run id names an earlier execution of a reused name. Otherwise
``RedriveExecution(executionArn, clientToken=idempotency_key[:64])`` (the API model allows 1-64
characters of ``[!-~]``; other characters become ``-``). ``ExecutionNotRedrivable``,
``ExecutionLimitExceeded`` and ``ValidationException`` give ``accepted=False`` with the AWS message
(RAW: the core masks it); success gives ``new_ref = ref.next_attempt()`` (the next failure is a
new incident with ``attempt + 1``). Every result carries :data:`CI_DEPLOY_WARNING` and
:data:`REDRIVE_SCOPE_WARNING`: a redrive resumes every failed branch or iteration with the
original definition and the code deployed now.

``RERUN_PIPELINE``: ``DescribeExecution.input`` is read in memory only (never stored, returned
or logged), then ``StartExecution(stateMachineArn, name=f"{orig[:52]}-healer-{n}", input=...)``
on the unqualified state machine ARN (its latest revision) with ``n`` the first 12 hex digits of
the SHA-256 of the idempotency key: the same key gives the same name, and an
``ExecutionAlreadyExists`` answer (the execution was started for this key before, by this
process or an earlier one) gives the same result. ``new_ref`` names the new execution
(attempt 1). This is the only option after an ASL change. An input AWS does not return (a KMS
denial answered with metadata only) gives ``accepted=False``.

The same ``idempotency_key`` returns the first result without calling AWS again (remembered in
memory, the last :data:`MAX_REMEMBERED_REMEDIATIONS` keys). A kind :meth:`SfnRemediator.available`
does not offer raises :class:`~dag_healer.core.errors.NotSupported`. Every other AWS error leaves
as a :mod:`dag_healer.core.errors` exception (transient ones stay transient, so the worker tries
the command again later); such a failure is not remembered.
"""

from __future__ import annotations

import hashlib
import logging
import re
import threading
from collections import OrderedDict
from collections.abc import Mapping
from dataclasses import replace
from datetime import timedelta
from typing import Any

from dag_healer.core.errors import NotFound, NotSupported, OrchestratorError
from dag_healer.core.ids import AttemptRef
from dag_healer.core.ports import RemediationResult
from dag_healer.core.vocab import RemediationKind
from dag_healer.stepfunctions import arns
from dag_healer.stepfunctions.aws import call, error_code, error_message
from dag_healer.stepfunctions.executions import describe_execution
from dag_healer.stepfunctions.options import FAILED_STATUSES
from dag_healer.stepfunctions.runtime import SfnRuntime, native_ref, native_text, to_utc

logger = logging.getLogger(__name__)

REDRIVE_WINDOW_DAYS = 14
CLIENT_TOKEN_MAX = 64
RERUN_NAME_PREFIX_CHARS = 52
#: Hex digits of the idempotency key's SHA-256 in the name of a restarted execution.
RERUN_NAME_HASH_CHARS = 12
RERUN_NAME_MARKER = "-healer-"
#: Remediation results remembered for their idempotency keys (per adapter).
MAX_REMEMBERED_REMEDIATIONS = 256
CI_DEPLOY_WARNING = (
    "A redrive reruns the code deployed now with the original definition. Deploy the merged fix first."
)
REDRIVE_SCOPE_WARNING = "A redrive resumes every failed branch or iteration of the execution, not one state."
RERUN_WARNING = (
    "A new execution runs the definition and the code deployed now with the original input. "
    "Deploy the merged fix first."
)
#: AWS error codes of a redrive AWS refused (``accepted=False``; anything else raises).
REDRIVE_REFUSALS = frozenset({"ExecutionNotRedrivable", "ExecutionLimitExceeded", "ValidationException"})
#: AWS error codes of a restart AWS refused (``accepted=False``; anything else raises).
START_REFUSALS = frozenset(
    {
        "ExecutionLimitExceeded",
        "InvalidExecutionInput",
        "InvalidName",
        "StateMachineDeleting",
        "ValidationException",
    }
)
_ALREADY_STARTED = "ExecutionAlreadyExists"
_NOT_TOKEN_CHARACTERS = re.compile(r"[^!-~]")
#: Execution statuses of an attempt that has not ended (a redrive of it is running).
_RUNNING_STATUSES = frozenset({"RUNNING", "PENDING_REDRIVE"})


# =========================================================================== pure helpers


def client_token(idempotency_key: str) -> str:
    """The ``RedriveExecution`` ``clientToken`` of an idempotency key: its first 64 characters,
    each one outside the token alphabet ``[!-~]`` (spaces, non-ASCII) replaced by ``-``."""
    return _NOT_TOKEN_CHARACTERS.sub("-", idempotency_key)[:CLIENT_TOKEN_MAX]


def rerun_name(original: str, idempotency_key: str) -> str:
    """The deterministic name of the execution a restart starts: ``<orig[:52]>-healer-<12 hex>``
    (at most 72 characters; the hex digits are the SHA-256 of ``idempotency_key``)."""
    digest = hashlib.sha256(idempotency_key.encode("utf-8")).hexdigest()[:RERUN_NAME_HASH_CHARS]
    return f"{original[:RERUN_NAME_PREFIX_CHARS]}{RERUN_NAME_MARKER}{digest}"


def _as_mapping(native: Any) -> Mapping[str, Any]:
    return native if isinstance(native, Mapping) else {}


def _parsed_execution(native: Mapping[str, Any]) -> tuple[str | None, arns.ExecutionArn | None]:
    """``(native.execution_arn, its parsed form)``; the parsed form is ``None`` when it is not one."""
    text = native_text(native, "execution_arn")
    if text is None:
        return None, None
    try:
        return text, arns.parse_execution_arn(text)
    except ValueError:
        return text, None


def _aws_code(exc: OrchestratorError) -> str | None:
    """The AWS error code behind a translated error (``call`` chains the botocore error)."""
    return error_code(exc.__cause__) if exc.__cause__ is not None else error_code(exc)


def _aws_message(exc: OrchestratorError) -> str:
    """The AWS message behind a translated error (RAW), else the error's own detail."""
    if exc.__cause__ is not None:
        return error_message(exc.__cause__)
    return exc.detail or type(exc).__name__


def _execution_label(arn: str) -> str:
    """The execution name of an ARN (the ARN itself when it does not parse)."""
    try:
        return arns.parse_execution_arn(arn).name
    except ValueError:
        return arn


def stale_redrive(ref: AttemptRef, described: Mapping[str, Any], label: str) -> tuple[str, str] | None:
    """Why a redrive of ``ref`` must not be sent, as ``(reason, detail)``; ``None`` when the
    described execution still is the incident's attempt (or cannot tell: AWS decides).

    ``described`` is the ``DescribeExecution`` answer, ``label`` the execution's name for the text.
    The reason is a short tag for the log (ids and counts only); the detail is for the person.
    """
    name = (
        described.get("name") if isinstance(described.get("name"), str) and described.get("name") else label
    )
    if not arns.run_names_execution(ref.run_id, name, to_utc(described.get("startDate"))):
        return (
            "reused name",
            f"Execution {name} was started again under the same name: run {ref.run_id} cannot be "
            "redriven any more.",
        )
    count = described.get("redriveCount")
    if not isinstance(count, int) or isinstance(count, bool) or count < 0:
        return None
    current = count + 1
    if current == ref.attempt:
        return None
    if current < ref.attempt:
        return (
            "unknown attempt",
            f"Execution {name} has {current} attempt(s), not attempt {ref.attempt}: nothing was redriven.",
        )
    status = described.get("status")
    if isinstance(status, str) and status.strip().upper() in _RUNNING_STATUSES:
        return (
            "redrive running",
            f"Execution {name} was redriven already: attempt {current} is {status.strip().upper()}. "
            "Its outcome arrives as an incident of its own.",
        )
    return (
        "newer attempt",
        f"Execution {name} is at attempt {current} now, not attempt {ref.attempt}: redrive it from the "
        f"incident of attempt {current}.",
    )


# =========================================================================== the port


class SfnRemediator:
    """``Remediator`` port: redrive (``RESUME_RUN``) and, opt-in, restart (``RERUN_PIPELINE``)."""

    def __init__(self, rt: SfnRuntime) -> None:
        self.rt = rt
        self._lock = threading.RLock()
        self._results: OrderedDict[str, RemediationResult] = OrderedDict()

    # ------------------------------------------------------------------ what can be offered
    def available(self, ref: AttemptRef, native: Mapping[str, Any]) -> tuple[RemediationKind, ...]:
        """No I/O: the kinds that can be offered for this incident now (see the module docstring)."""
        if not ref.resolved:
            return ()
        values = _as_mapping(native)
        text, execution = _parsed_execution(values)
        if text is not None and execution is None:
            return ()  # a pointer that is not an execution ARN: nothing to act on
        if execution is not None and execution.express:
            return ()  # Express executions: no redrive, no DescribeExecution (v1)
        kinds: list[RemediationKind] = []
        if self.rt.options.retry_strategy == "redrive" and self._redrivable(values):
            kinds.append(RemediationKind.RESUME_RUN)
        if self.rt.options.rerun and not (execution is not None and execution.is_map_run_child):
            kinds.append(RemediationKind.RERUN_PIPELINE)
        return tuple(kinds)

    def _redrivable(self, native: Mapping[str, Any]) -> bool:
        """The status and the 14-day window allow a redrive (unknown values allow it)."""
        status = native_text(native, "status")
        if status is not None and status.strip().upper() not in FAILED_STATUSES:
            return False
        stopped = to_utc(native.get("stop_date"))
        if stopped is None:
            return True
        return self.rt.now() - stopped <= timedelta(days=REDRIVE_WINDOW_DAYS)

    # ------------------------------------------------------------------ remediation
    def remediate(
        self,
        ref: AttemptRef,
        native: Mapping[str, Any],
        kind: RemediationKind,
        *,
        idempotency_key: str,
        actor: str,
    ) -> RemediationResult:
        """Redrive the execution or start it again (see the module docstring).

        Raises :class:`~dag_healer.core.errors.NotSupported` for a kind :meth:`available` does not
        offer, :class:`~dag_healer.core.errors.NotFound` when the execution cannot be named, and
        :class:`ValueError` for an empty ``idempotency_key``.
        """
        values = _as_mapping(native)
        key = str(idempotency_key)
        with self._lock:
            previous = self._results.get(key)
            if previous is not None and previous.kind is kind:
                self._results.move_to_end(key)
                return previous
            if kind not in self.available(ref, values):
                raise NotSupported(
                    f"{kind.value} is not available for this AWS Step Functions incident "
                    f"(run {ref.run_id} of {ref.pipeline_id})"
                )
            if not key.strip():
                raise ValueError("idempotency_key must not be empty")
            if kind is RemediationKind.RESUME_RUN:
                result = self._redrive(ref, values, key, actor)
            else:
                result = self._rerun(ref, values, key, actor)
            self._results[key] = result
            self._results.move_to_end(key)
            while len(self._results) > MAX_REMEMBERED_REMEDIATIONS:
                self._results.popitem(last=False)
        return result

    def _execution_arn(self, ref: AttemptRef, native: Mapping[str, Any]) -> str:
        """The ARN of the incident's execution (may resolve the machine with ``ListStateMachines``)."""
        try:
            arn = self.rt.execution_arn(ref, native)
        except ValueError:
            arn = None
        if arn is None:
            raise NotFound(
                f"the execution of run {ref.run_id} of pipeline {ref.pipeline_id} is unknown: "
                "its state machine ARN cannot be resolved"
            )
        return arn

    # ------------------------------------------------------------------ RESUME_RUN
    def _redrive(self, ref: AttemptRef, native: Mapping[str, Any], key: str, actor: str) -> RemediationResult:
        arn = self._execution_arn(ref, native)
        name = _execution_label(arn)
        warnings = (CI_DEPLOY_WARNING, REDRIVE_SCOPE_WARNING)
        stale = stale_redrive(ref, describe_execution(self.rt, arn), name)
        if stale is not None:
            reason, detail = stale
            logger.info(
                "Step Functions adapter %s: the redrive of execution %s (attempt %d) was not sent: %s",
                self.rt.instance,
                name,
                ref.attempt,
                reason,
            )
            return RemediationResult(
                kind=RemediationKind.RESUME_RUN,
                accepted=False,
                detail=detail,
                warnings=warnings,
                native=native_ref(execution_arn=arn),
            )
        try:
            call(
                self.rt.clients.sfn,
                "redrive_execution",
                what="RedriveExecution",
                executionArn=arn,
                clientToken=client_token(key),
            )
        except OrchestratorError as exc:
            code = _aws_code(exc)
            if code not in REDRIVE_REFUSALS:
                raise
            logger.info(
                "Step Functions adapter %s: the redrive of execution %s was refused (%s)",
                self.rt.instance,
                name,
                code,
            )
            return RemediationResult(
                kind=RemediationKind.RESUME_RUN,
                accepted=False,
                detail=(
                    f"AWS Step Functions refused to redrive execution {name} ({code}): {_aws_message(exc)}"
                ),
                warnings=warnings,
                native=native_ref(execution_arn=arn),
            )
        new_ref = ref.next_attempt()
        logger.info(
            "Step Functions adapter %s: redrive of execution %s accepted (attempt %d)",
            self.rt.instance,
            name,
            new_ref.attempt,
        )
        return RemediationResult(
            kind=RemediationKind.RESUME_RUN,
            accepted=True,
            detail=(
                f"{actor} started a redrive of execution {name} (attempt {new_ref.attempt}); AWS Step "
                "Functions runs its failed states again with the original input."
            ),
            new_ref=new_ref,
            warnings=warnings,
            native=native_ref(execution_arn=arn, redrive_count=new_ref.attempt - 1),
        )

    # ------------------------------------------------------------------ RERUN_PIPELINE
    def _rerun(self, ref: AttemptRef, native: Mapping[str, Any], key: str, actor: str) -> RemediationResult:
        arn = self._execution_arn(ref, native)
        described = describe_execution(self.rt, arn)
        original = described.get("name")
        if not isinstance(original, str) or not original.strip():
            original = _execution_label(arn)
        machine = self._machine_of(described, native, arn)
        machine_name = arns.state_machine_name(machine)
        warnings = (RERUN_WARNING,)
        payload = described.get("input")
        if not isinstance(payload, str):
            return RemediationResult(
                kind=RemediationKind.RERUN_PIPELINE,
                accepted=False,
                detail=(
                    f"The input of execution {original} is not readable (AWS returned its metadata "
                    "only, for example because of a KMS key policy), so it cannot be started again "
                    "with the same input."
                ),
                warnings=warnings,
                native=native_ref(execution_arn=arn, state_machine_arn=machine),
            )
        name = rerun_name(original, key)
        already = False
        try:
            response = call(
                self.rt.clients.sfn,
                "start_execution",
                what="StartExecution",
                stateMachineArn=machine,
                name=name,
                input=payload,
            )
        except OrchestratorError as exc:
            code = _aws_code(exc)
            if code == _ALREADY_STARTED:
                response, already = {}, True
            elif code in START_REFUSALS:
                logger.info(
                    "Step Functions adapter %s: the restart of execution %s was refused (%s)",
                    self.rt.instance,
                    original,
                    code,
                )
                return RemediationResult(
                    kind=RemediationKind.RERUN_PIPELINE,
                    accepted=False,
                    detail=(
                        f"AWS Step Functions refused to start execution {name} of {machine_name} "
                        f"({code}): {_aws_message(exc)}"
                    ),
                    warnings=warnings,
                    native=native_ref(execution_arn=arn, state_machine_arn=machine),
                )
            else:
                raise
        started = response.get("executionArn")
        new_arn = started if isinstance(started, str) and started else arns.execution_arn(machine, name)
        new_ref = replace(ref, run_id=name, attempt=1)
        logger.info(
            "Step Functions adapter %s: restart of execution %s as %s %s",
            self.rt.instance,
            original,
            name,
            "already started" if already else "accepted",
        )
        if already:
            detail = (
                f"Execution {name} of {machine_name} was already started for this request "
                f"with the input of execution {original}."
            )
        else:
            detail = (
                f"{actor} started execution {name} of {machine_name} with the input of execution {original}."
            )
        return RemediationResult(
            kind=RemediationKind.RERUN_PIPELINE,
            accepted=True,
            detail=detail,
            new_ref=new_ref,
            warnings=warnings,
            native=native_ref(
                execution_arn=new_arn,
                state_machine_arn=machine,
                start_date=to_utc(response.get("startDate")),
            ),
        )

    @staticmethod
    def _machine_of(described: Mapping[str, Any], native: Mapping[str, Any], execution_arn: str) -> str:
        """The unqualified ARN of the execution's state machine (``StartExecution`` runs its latest
        revision): ``DescribeExecution.stateMachineArn``, else ``native``, else the execution ARN's."""
        for candidate in (described.get("stateMachineArn"), native_text(native, "state_machine_arn")):
            if isinstance(candidate, str) and arns.is_arn(candidate):
                try:
                    return arns.parse_state_machine_arn(candidate).unqualified
                except ValueError:
                    continue
        try:
            return arns.parse_execution_arn(execution_arn).state_machine_arn
        except ValueError:
            raise NotFound(f"the state machine of execution {execution_arn} is unknown") from None


__all__ = [
    "CI_DEPLOY_WARNING",
    "CLIENT_TOKEN_MAX",
    "MAX_REMEMBERED_REMEDIATIONS",
    "REDRIVE_REFUSALS",
    "REDRIVE_SCOPE_WARNING",
    "REDRIVE_WINDOW_DAYS",
    "RERUN_NAME_HASH_CHARS",
    "RERUN_NAME_MARKER",
    "RERUN_NAME_PREFIX_CHARS",
    "RERUN_WARNING",
    "START_REFUSALS",
    "SfnRemediator",
    "client_token",
    "rerun_name",
    "stale_redrive",
]
