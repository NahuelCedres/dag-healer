"""Step Functions API calls shared by the ports (thin wrappers, errors translated).

* :func:`describe_execution` - ``DescribeExecution`` (input/output are never kept by callers);
  a KMS access denial is retried with ``includedData=METADATA_ONLY`` [U];
* :func:`read_history` - the history of one redrive segment, newest first from AWS
  (``reverseOrder=True``, ``maxResults=1000``, ``includeExecutionData=False``) until the
  segment's start or ``options.history_max_events``, returned oldest first;
* :func:`list_executions` - one ``ListExecutions`` page (newest first);
* :func:`describe_state_machine` / :func:`describe_state_machine_for_execution` - definitions.

AWS ``nextToken`` values are used within one call and never persisted (they expire after 24 h).
"""

from __future__ import annotations

import logging
from collections.abc import Mapping
from dataclasses import dataclass
from typing import Any

from dag_healer.core.errors import PermissionDenied
from dag_healer.stepfunctions.aws import call, error_code
from dag_healer.stepfunctions.runtime import SfnRuntime

logger = logging.getLogger(__name__)

HISTORY_PAGE_SIZE = 1000
#: Pages read at most per history (an answer that keeps handing out tokens cannot loop forever).
MAX_HISTORY_PAGES = 100
_KMS_DENIED = frozenset({"KmsAccessDeniedException", "KMSAccessDeniedException", "KmsInvalidStateException"})


@dataclass(frozen=True)
class HistorySegment:
    """The history events of one redrive segment of an execution, oldest first."""

    events: tuple[Mapping[str, Any], ...]
    complete: bool  # False: history_max_events was reached before the segment's first event
    pages: int = 0


def describe_execution(rt: SfnRuntime, execution_arn: str) -> dict[str, Any]:
    """``DescribeExecution``. Callers read status, dates, redrive fields, error and cause only;
    ``input``/``output`` are never stored or logged. A KMS denial is retried once without the
    encrypted data (``includedData=METADATA_ONLY``)."""
    try:
        return call(
            rt.clients.sfn, "describe_execution", what="DescribeExecution", executionArn=execution_arn
        )
    except PermissionDenied as exc:
        if error_code(exc.__cause__ or exc) not in _KMS_DENIED:
            raise
        logger.debug("dag-healer stepfunctions: DescribeExecution retried without encrypted data")
        return call(
            rt.clients.sfn,
            "describe_execution",
            what="DescribeExecution",
            executionArn=execution_arn,
            includedData="METADATA_ONLY",
        )


def _segment_start(event: Mapping[str, Any], redrive_count: int | None) -> bool:
    """True when ``event`` (read newest first) is the first event of the wanted segment."""
    kind = event.get("type")
    if kind == "ExecutionStarted":
        return True
    if kind != "ExecutionRedriven":
        return False
    if redrive_count is None:
        return True
    details = event.get("executionRedrivenEventDetails") or {}
    count = details.get("redriveCount") if isinstance(details, Mapping) else None
    return isinstance(count, int) and count <= redrive_count


def read_history(rt: SfnRuntime, execution_arn: str, *, redrive_count: int | None = None) -> HistorySegment:
    """The events of one redrive segment, oldest first.

    ``redrive_count=None`` reads the latest segment (back to the newest ``ExecutionRedriven``,
    or ``ExecutionStarted``); ``redrive_count=n`` reads back to the ``ExecutionRedriven`` of
    redrive ``n`` (``0``: to ``ExecutionStarted``), so older segments stay readable. The newer
    segments read on the way are returned too (callers pick the segment they need). Reading
    stops after ``options.history_max_events`` events or :data:`MAX_HISTORY_PAGES` pages
    (``complete=False``); events that are not mappings are skipped.
    """
    limit = rt.options.history_max_events
    events: list[Mapping[str, Any]] = []
    token: str | None = None
    pages = 0
    complete = False
    while True:
        params: dict[str, Any] = {
            "executionArn": execution_arn,
            "reverseOrder": True,
            "maxResults": HISTORY_PAGE_SIZE,
            "includeExecutionData": False,
        }
        if token:
            params["nextToken"] = token
        response = call(rt.clients.sfn, "get_execution_history", what="GetExecutionHistory", **params)
        pages += 1
        stop = False
        page = response.get("events")
        for event in page if isinstance(page, list) else ():
            if not isinstance(event, Mapping):
                continue
            events.append(event)
            if _segment_start(event, redrive_count):
                complete, stop = True, True
                break
            if len(events) >= limit:
                stop = True
                break
        next_token = response.get("nextToken")
        token = next_token if isinstance(next_token, str) and next_token else None
        if stop or not token:
            if not token and not stop:
                complete = True  # the whole history was read
            break
        if pages >= MAX_HISTORY_PAGES:
            logger.debug("dag-healer stepfunctions: GetExecutionHistory stopped after %d pages", pages)
            break
    events.reverse()
    return HistorySegment(events=tuple(events), complete=complete, pages=pages)


def list_executions(
    rt: SfnRuntime,
    state_machine_arn: str,
    status: str,
    *,
    next_token: str | None = None,
    max_results: int = 100,
) -> dict[str, Any]:
    """One ``ListExecutions`` page (newest first; finished executions ordered by ``stopDate``)."""
    params: dict[str, Any] = {
        "stateMachineArn": state_machine_arn,
        "statusFilter": status,
        "maxResults": max_results,
    }
    if next_token:
        params["nextToken"] = next_token
    return call(rt.clients.sfn, "list_executions", what="ListExecutions", **params)


def describe_state_machine_for_execution(rt: SfnRuntime, execution_arn: str) -> dict[str, Any]:
    """``DescribeStateMachineForExecution``: the definition the execution actually ran."""
    return call(
        rt.clients.sfn,
        "describe_state_machine_for_execution",
        what="DescribeStateMachineForExecution",
        executionArn=execution_arn,
    )


def describe_state_machine(rt: SfnRuntime, state_machine_arn: str) -> dict[str, Any]:
    """``DescribeStateMachine``: the current definition, type (STANDARD/EXPRESS) and status."""
    return call(
        rt.clients.sfn,
        "describe_state_machine",
        what="DescribeStateMachine",
        stateMachineArn=state_machine_arn,
    )


__all__ = [
    "HISTORY_PAGE_SIZE",
    "MAX_HISTORY_PAGES",
    "HistorySegment",
    "describe_execution",
    "describe_state_machine",
    "describe_state_machine_for_execution",
    "list_executions",
    "read_history",
]
