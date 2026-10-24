"""EventBridge "Step Functions Execution Status Change" events -> push hints (pure, no I/O).

The adapter's push channel is ``eventbridge``. A payload is either the EventBridge envelope
itself (an API destination posts it to ``POST /events/eventbridge``) or an SQS message whose
body is that envelope (the recommended EventBridge -> SQS route; the queue port hands over
``{"MessageId": ..., "Body": "<json>"}``). The envelope::

    {"source": "aws.states", "detail-type": "Step Functions Execution Status Change",
     "id": "...", "time": "2026-10-02T06:00:42Z", "region": "eu-west-1", "account": "...",
     "detail": {"executionArn": ..., "stateMachineArn": ..., "name": ..., "status": "FAILED",
                "startDate": 1790920800000, "stopDate": 1790920842512, "redriveCount": 0,
                "error": "KeyError", "cause": "...", "input": "...", "output": null, ...}}

Rules (section 9.4 of the design):

* not a Step Functions status-change envelope (wrong ``source``/``detail-type``, missing or
  ill-typed ``detail`` fields, an SQS body that is not JSON) -> :class:`ValueError` (HTTP 422);
* a status outside ``statuses`` (``RUNNING``, ``SUCCEEDED`` ...) -> no hint (``[]``);
* a state machine the instance does not watch, or a Distributed Map child machine -> ``[]``;
* otherwise one :class:`~dag_healer.core.failures.FailureHint` with
  ``ref = AttemptRef(instance, pipeline_id, name, UNRESOLVED_STEP, -1, redriveCount + 1)``,
  ``native = native_ref(execution_arn, state_machine_arn, status, redrive_count, error,
  start_date, stop_date, definition_version_arn)``, ``error_text = "<error>: <cause>"`` (RAW,
  capped) and ``source`` ``eventbridge`` (an envelope) or ``sqs`` (an SQS message).

``input``/``output`` (and every other field) are dropped: they never reach the hint.

The fields that make the hint's key (``executionArn``, ``stateMachineArn``, ``name``,
``status``, ``redriveCount``) are validated strictly: the two ARNs must parse and name the same
machine, region and account, and ``name`` must be the execution ARN's name (it is derived from
the ARN when missing). Informational fields (dates, ``error``, ``cause``, version and alias
ARNs, the event id and time) of an unexpected type are ignored. Express executions (they emit
no such events, so an envelope naming one is forged or misrouted) give no hint.
"""

from __future__ import annotations

import json
from collections.abc import Collection, Mapping
from dataclasses import dataclass
from datetime import datetime
from typing import Any

from dag_healer.core.failures import FailureHint
from dag_healer.core.ids import NO_MAP_INDEX, UNRESOLVED_STEP, AttemptRef
from dag_healer.stepfunctions import arns
from dag_healer.stepfunctions.runtime import PipelineIndex, native_ref, to_utc

SOURCE = "aws.states"
DETAIL_TYPE = "Step Functions Execution Status Change"
CHANNEL = "eventbridge"
SOURCE_EVENTBRIDGE = "eventbridge"
SOURCE_SQS = "sqs"
MAX_ERROR_TEXT_CHARS = 64 * 1024
#: Largest SQS message body parsed (SQS allows 256 KiB; EventBridge events are smaller).
MAX_BODY_CHARS = 1024 * 1024

_BODY_KEYS = ("Body", "body")
_ENVELOPE_KEYS = ("source", "detail-type", "detail")


@dataclass(frozen=True)
class ExecutionStatusChange:
    """The whitelisted fields of one status-change event (no input/output)."""

    execution_arn: str
    state_machine_arn: str
    name: str
    status: str
    redrive_count: int = 0
    start_date: datetime | None = None
    stop_date: datetime | None = None
    error: str | None = None
    cause: str | None = None  # RAW
    state_machine_version_arn: str | None = None
    state_machine_alias_arn: str | None = None
    event_id: str | None = None
    event_time: datetime | None = None

    @property
    def attempt(self) -> int:
        """The incident attempt of this status change (``redriveCount + 1``)."""
        return self.redrive_count + 1


# =========================================================================== unwrapping


def _body_text(body: Any) -> str:
    """An SQS body as text (bytes are decoded as UTF-8)."""
    if isinstance(body, (bytes, bytearray)):
        try:
            return bytes(body).decode("utf-8")
        except UnicodeDecodeError:
            raise ValueError("the SQS message body is not UTF-8 text") from None
    if isinstance(body, str):
        return body
    raise ValueError("the SQS message body must be JSON text")


def unwrap(payload: Mapping[str, Any]) -> tuple[Mapping[str, Any], str]:
    """``(envelope, source)``: an SQS message (``Body``/``body`` holding JSON text) gives its parsed
    body and ``"sqs"``; an envelope gives itself and ``"eventbridge"``. :class:`ValueError` otherwise."""
    if not isinstance(payload, Mapping):
        raise ValueError("the payload must be a JSON object")
    key = next((name for name in _BODY_KEYS if name in payload), None)
    if key is None or any(name in payload for name in _ENVELOPE_KEYS):
        return payload, SOURCE_EVENTBRIDGE
    body = payload[key]
    if isinstance(body, Mapping):  # a body some tool already parsed
        return body, SOURCE_SQS
    text = _body_text(body)
    if len(text) > MAX_BODY_CHARS:
        raise ValueError(f"the SQS message body is longer than {MAX_BODY_CHARS} characters")
    try:
        parsed = json.loads(text)
    except (ValueError, RecursionError):  # not JSON, or nested too deeply to parse
        raise ValueError("the SQS message body is not JSON") from None
    if not isinstance(parsed, Mapping):
        raise ValueError("the SQS message body is not a JSON object")
    return parsed, SOURCE_SQS


# =========================================================================== parsing


def _optional_text(value: Any) -> str | None:
    """Non-blank text (RAW, unchanged); anything else is ignored."""
    return value if isinstance(value, str) and value.strip() else None


def _arn(detail: Mapping[str, Any], key: str) -> str:
    value = detail.get(key)
    if not isinstance(value, str) or not value.strip():
        raise ValueError(f"detail.{key} must be a non-empty string")
    return value.strip()


def _parsed_arns(detail: Mapping[str, Any]) -> tuple[str, arns.ExecutionArn, str]:
    """``(execution ARN, parsed, state machine ARN)`` of ``detail``; both name the same machine."""
    execution_text, machine_text = _arn(detail, "executionArn"), _arn(detail, "stateMachineArn")
    try:
        execution = arns.parse_execution_arn(execution_text)
    except ValueError:
        raise ValueError("detail.executionArn is not a Step Functions execution ARN") from None
    try:
        machine = arns.parse_state_machine_arn(machine_text)
    except ValueError:
        raise ValueError("detail.stateMachineArn is not a Step Functions state machine ARN") from None
    same = (
        execution.partition == machine.partition
        and execution.region == machine.region
        and execution.account == machine.account
        and execution.state_machine == machine.name
        and execution.label == machine.label
    )
    if not same:
        raise ValueError("detail.executionArn does not belong to detail.stateMachineArn")
    return execution_text, execution, machine_text


def _name(detail: Mapping[str, Any], execution: arns.ExecutionArn) -> str:
    """``detail.name`` (the execution ARN's name when missing); it must match the ARN."""
    value = detail.get("name")
    if value is None:
        return execution.name
    if not isinstance(value, str) or not value.strip():
        raise ValueError("detail.name must be a non-empty string")
    if not execution.express and value != execution.name:
        raise ValueError("detail.name is not the name of detail.executionArn")
    return value


def _status(detail: Mapping[str, Any]) -> str:
    value = detail.get("status")
    if not isinstance(value, str) or not value.strip():
        raise ValueError("detail.status must be a non-empty string")
    return value.strip()


def _redrive_count(detail: Mapping[str, Any]) -> int:
    """``detail.redriveCount`` (0 when missing or null); an integer >= 0."""
    value = detail.get("redriveCount")
    if value is None:
        return 0
    if isinstance(value, float) and value.is_integer():
        value = int(value)
    if isinstance(value, bool) or not isinstance(value, int) or value < 0:
        raise ValueError("detail.redriveCount must be an integer >= 0")
    return value


def _moment(value: Any) -> datetime | None:
    """An event date (epoch milliseconds or ISO text); anything unusable is ignored."""
    if isinstance(value, bool) or not isinstance(value, (int, float, str, datetime)):
        return None
    return to_utc(value)


def parse_status_change(envelope: Mapping[str, Any]) -> ExecutionStatusChange:
    """Validate an envelope and keep the whitelisted fields; :class:`ValueError` when malformed."""
    if not isinstance(envelope, Mapping):
        raise ValueError("the event must be a JSON object")
    if envelope.get("source") != SOURCE:
        raise ValueError(f"not a Step Functions event: source must be {SOURCE!r}")
    if envelope.get("detail-type") != DETAIL_TYPE:
        raise ValueError(f"not a Step Functions status change: detail-type must be {DETAIL_TYPE!r}")
    detail = envelope.get("detail")
    if not isinstance(detail, Mapping):
        raise ValueError("detail must be a JSON object")
    execution_arn, execution, state_machine_arn = _parsed_arns(detail)
    return ExecutionStatusChange(
        execution_arn=execution_arn,
        state_machine_arn=state_machine_arn,
        name=_name(detail, execution),
        status=_status(detail),
        redrive_count=_redrive_count(detail),
        start_date=_moment(detail.get("startDate")),
        stop_date=_moment(detail.get("stopDate")),
        error=_optional_text(detail.get("error")),
        cause=_optional_text(detail.get("cause")),
        state_machine_version_arn=_optional_text(detail.get("stateMachineVersionArn")),
        state_machine_alias_arn=_optional_text(detail.get("stateMachineAliasArn")),
        event_id=_optional_text(envelope.get("id")),
        event_time=_moment(envelope.get("time")),
    )


def error_text(error: str | None, cause: str | None) -> str:
    """``"<error>: <cause>"`` (or whichever is present; ``""`` for neither), capped at 64 KiB."""
    parts = [text for text in (error, cause) if isinstance(text, str) and text.strip()]
    return ": ".join(parts)[:MAX_ERROR_TEXT_CHARS]


# =========================================================================== hints


def hint_native(change: ExecutionStatusChange) -> dict[str, Any]:
    """The ``native_ref`` of a hint: ids, status, dates and the error name (no cause, no payload)."""
    return native_ref(
        execution_arn=change.execution_arn,
        state_machine_arn=change.state_machine_arn,
        start_date=change.start_date,
        stop_date=change.stop_date,
        status=change.status,
        redrive_count=change.redrive_count,
        error=change.error,
        definition_version_arn=change.state_machine_version_arn,
    )


def normalize(
    payload: Mapping[str, Any],
    *,
    instance: str,
    pipelines: PipelineIndex,
    statuses: Collection[str],
    channel: str = CHANNEL,
) -> list[FailureHint]:
    """The hints of one payload (see the module docstring). Pure: no I/O, no clock."""
    envelope, source = unwrap(payload)
    change = parse_status_change(envelope)
    if change.status not in statuses:
        return []
    execution = arns.parse_execution_arn(change.execution_arn)
    if execution.express or execution.is_map_run_child:
        return []
    pipeline_id = pipelines.pipeline_id_for(change.state_machine_arn)
    if not pipeline_id:
        return []
    ref = AttemptRef(instance, pipeline_id, change.name, UNRESOLVED_STEP, NO_MAP_INDEX, change.attempt)
    return [
        FailureHint(
            ref=ref,
            channel=channel,
            source=source,
            error_text=error_text(change.error, change.cause),
            native=hint_native(change),
        )
    ]


__all__ = [
    "CHANNEL",
    "DETAIL_TYPE",
    "MAX_BODY_CHARS",
    "MAX_ERROR_TEXT_CHARS",
    "SOURCE",
    "SOURCE_EVENTBRIDGE",
    "SOURCE_SQS",
    "ExecutionStatusChange",
    "error_text",
    "hint_native",
    "normalize",
    "parse_status_change",
    "unwrap",
]
