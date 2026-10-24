"""Failures as the orchestrator reports them, push hints and their confirmation (stdlib only).

A :class:`Failure` is the source of truth: it comes from the orchestrator's API (a poll or
the confirmation of a hint). A :class:`FailureHint` is what a push channel delivered (the
Airflow listener/callback, EventBridge, SQS); it can be forged or stale and never becomes
an incident before :meth:`dag_healer.core.ports.PushIntake.confirm` turned it into a
:class:`Confirmation`.

Error texts in these objects are RAW (unmasked): the core masks them before anything is
persisted, logged or sent to an LLM. ``native`` mappings are adapter-owned, JSON-safe
pointers (no payloads; see ``native_ref`` in section 2.2 of the design).
"""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum
from typing import Any, Literal

from dag_healer.core.ids import AttemptRef


class AttemptState(str, Enum):
    """State of one attempt, in neutral terms."""

    FAILED = "failed"  # terminal: no automatic retry left
    RETRYING = "retrying"  # failed; an automatic retry follows (Airflow up_for_retry, SFN Retry)
    CAUGHT = "caught"  # failed; handled by the pipeline itself (SFN Catch)
    RUNNING = "running"
    SUCCEEDED = "succeeded"
    UNKNOWN = "unknown"


Trigger = Literal["final_failure", "every_failure"]
"""``monitor.trigger``: raise incidents for terminal attempts only, or for every failure."""


@dataclass(frozen=True)
class Failure:
    """A confirmed failed attempt as the orchestrator reports it (the source of truth)."""

    ref: AttemptRef  # always resolved (or RUN_LEVEL_STEP)
    state: AttemptState
    is_final: bool  # no automatic retry left; drives monitor.trigger
    source: str  # "rest" | "poll" | "listener" | "callback" | "eventbridge" | "sqs" | "push" | "cli"
    error_name: str | None = None  # "KeyError" | "States.Timeout" | "AirflowTaskTimeout"
    error_summary: str = ""  # RAW (unmasked); the core masks before persisting
    failed_at: datetime | None = None  # aware UTC
    native: Mapping[str, Any] = field(default_factory=dict)  # JSON-safe, no payloads (-> native_ref)


@dataclass(frozen=True)
class FailureHint:
    """An unconfirmed push notification. Forgeable: always confirmed before an incident exists."""

    ref: AttemptRef  # step_id may be UNRESOLVED_STEP
    channel: str  # "task-failed" | "eventbridge"
    source: str  # incident source if confirmed ("listener", "callback", "eventbridge", "sqs")
    error_text: str = ""  # RAW; masked by the core before it is stored
    received_at: datetime | None = None
    native: Mapping[str, Any] = field(default_factory=dict)


@dataclass(frozen=True)
class Confirmation:
    """Result of checking a hint against the orchestrator API.

    ``retry_later`` keeps the hint for the next check. With ``state`` ``RUNNING`` it means the
    attempt has not ended yet (a hint can arrive before the orchestrator records the end of
    the attempt), which is no error; without it the orchestrator could not be asked.
    """

    verdict: Literal["confirmed", "discarded", "retry_later"]
    reason: str  # shown in the pending-event row and logs (masked)
    failures: tuple[Failure, ...] = ()  # confirmed + localised; usually one
    state: AttemptState | None = None  # the attempt's state, when the orchestrator reported one


@dataclass(frozen=True)
class FailurePage:
    """One :meth:`~dag_healer.core.ports.FailureFeed.poll` result."""

    failures: tuple[Failure, ...]
    cursor: Mapping[str, Any]  # opaque, JSON-safe; persisted in healer_kv per instance
    complete: bool = True  # False: page cap hit, poll again on the next cycle


__all__ = [
    "AttemptState",
    "Confirmation",
    "Failure",
    "FailureHint",
    "FailurePage",
    "Trigger",
]
