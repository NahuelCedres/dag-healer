"""The incident pipeline of the dag-healer worker: reconcile failures, then triage each incident.

Three entry points, all called by :func:`dag_healer.worker.loop.run_worker` (and usable on
their own, for example from tests or a one-shot CLI run):

* :func:`reconcile` finds failures. With ``"rest"`` in ``monitor.modes`` it lists the failed
  task instances updated since a watermark kept in ``<healer_home>/state.json`` and creates
  the missing incidents (source ``rest``). With ``"listener"`` / ``"callback"`` it confirms
  each ``pending_check`` event pushed by the task runners through the Airflow REST API
  (``state == "failed"``; with ``monitor.trigger: final_failure`` a try that will be retried
  is discarded) before creating its incident. The REST API is the source of truth, events are
  only a hint (forged events are discarded).
* :func:`apply_commands` applies the human actions queued by the API/CLI (see
  :mod:`dag_healer.worker.commands`).
* :func:`process_incident` runs one claimed incident through the states of the plan::

      triaging -> classified -> reporting -> reported               (DATA)
                             -> repairing -> repaired | repair_failed (CODE)
                             -> waiting_human                         (AMBIGUOUS)

  gather the context (logs, code, contract, masked sample), classify it (policy + LLM;
  skipped when a human already resolved the category), persist the masked results, then
  report, repair or wait for a human, and send the alert of the final state. Every step adds
  a timeline event (``context``, ``classification``, ``repair``, ``alert``...). It never
  raises: an unexpected failure puts the incident in ``error`` with a masked event.

Everything persisted or logged goes through :mod:`dag_healer.masking` first: error texts are
masked by the context gathering, samples are masked before they are stored.

Long operations (Airflow calls, the LLM, repairs, SMTP) never run inside a database
transaction: each step opens a short session from ``session_factory`` (a callable returning
a context manager that commits on success, :func:`dag_healer.store.db.session_scope` by
default), so the API and the UI see every transition as it happens.
"""

from __future__ import annotations

import hashlib
import json
import logging
import os
import posixpath
import re
import tempfile
from collections.abc import Callable, Iterable, Mapping
from contextlib import AbstractContextManager
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from functools import partial
from pathlib import Path
from typing import TYPE_CHECKING, Any

from sqlalchemy import func, select
from sqlalchemy.orm import Session

from dag_healer.masking import mask_text, to_json_safe
from dag_healer.store import repo as store_repo
from dag_healer.store.db import session_scope
from dag_healer.store.models import (
    CLASSIFIED,
    ERROR,
    NEW,
    PENDING_CONFIRMED,
    PENDING_DISCARDED,
    REPAIR_FAILED,
    REPAIRED,
    REPAIRING,
    REPORTED,
    REPORTING,
    RESUMED,
    TRIAGING,
    WAITING_HUMAN,
    Incident,
    IncidentEvent,
    PendingEvent,
    ensure_utc,
    utcnow,
)

if TYPE_CHECKING:
    from dag_healer.agent.classify import ClassificationOutcome
    from dag_healer.agent.context import IncidentContext
    from dag_healer.agent.llm import LLMProvider
    from dag_healer.airflow.client import AirflowClient, TaskInstanceInfo
    from dag_healer.alerts import AlertOutcome
    from dag_healer.config import HealerConfig
    from dag_healer.worker.commands import CommandOutcome

logger = logging.getLogger(__name__)

SessionFactory = Callable[[], AbstractContextManager[Session]]
Notifier = Callable[..., "list[AlertOutcome]"]

#: Actor recorded on the worker's own timeline events.
WORKER_ACTOR = "dag-healer"
#: Event kind recorded when the worker starts the automatic triage of a NEW incident
#: (counted for ``limits.max_incidents_per_hour``; human-resumed incidents are not counted).
TRIAGE_EVENT = "triage"
#: Incidents left in a working status longer than this (crashed worker) are queued again.
STALE_CLAIM_AFTER = timedelta(minutes=30)
#: Without a watermark the first REST poll looks this far back.
INITIAL_LOOKBACK = timedelta(hours=24)
#: Minimum overlap between two REST polls (the API gives no ``updated_at`` per row and the
#: clocks of the worker and Airflow may differ).
MIN_POLL_OVERLAP = timedelta(seconds=60)
#: Pending events that still cannot be confirmed after this long are discarded.
PENDING_MAX_AGE = timedelta(hours=24)
STATE_FILE = "state.json"
WATERMARK_KEY = "rest_watermark"
#: Failures updated before this moment are never listed (set by ``dag-healer store reset``).
NOT_BEFORE_KEY = "rest_not_before"

MAX_ERROR_SUMMARY_CHARS = 8000
MAX_PYTEST_OUTPUT_CHARS = 40_000
MAX_EVENT_TEXT_CHARS = 2000
MAX_EVENT_NOTES = 20
HUMAN_RULE = "human_resolution"

EVENT_SOURCES: tuple[str, ...] = ("listener", "callback")
FORCED_CATEGORIES: tuple[str, ...] = ("data", "code")


def default_session_factory(cfg: HealerConfig) -> SessionFactory:
    """Sessions on the configured store (``cfg.database_url``).

    The worker is one of the processes that migrate the store (section 6.4 of
    ``docs/architecture/orchestrators.md``): its first session creates or upgrades the
    schema, and an attempt that failed (store unreachable) is repeated by the next session.
    """
    return partial(session_scope, cfg.database_url, allow_migrate=True)


# =========================================================================== small helpers


def _now(now: datetime | None) -> datetime:
    return ensure_utc(now) or utcnow()


def _short(incident_id: str | None) -> str:
    return (incident_id or "?")[:8]


def _clip(text: Any, limit: int = MAX_EVENT_TEXT_CHARS) -> str:
    """Masked, bounded free text (``None`` becomes an empty string)."""
    masked = mask_text(text).strip()
    return masked if len(masked) <= limit else masked[: limit - 3] + "..."


def _masked_json(value: Any) -> Any:
    """``mask_text`` on every string of a JSON-like value (for data that came from user code)."""
    if isinstance(value, str):
        return mask_text(value)
    if isinstance(value, Mapping):
        return {str(key): _masked_json(item) for key, item in value.items()}
    if isinstance(value, (list, tuple)):
        return [_masked_json(item) for item in value]
    return to_json_safe(value)


def _error_text(exc: BaseException) -> str:
    """``Type: message`` of an exception, masked and bounded (never a traceback)."""
    message = str(exc).strip().splitlines()[0] if str(exc).strip() else ""
    return _clip(f"{type(exc).__name__}: {message}" if message else type(exc).__name__, 500)


def transition(
    session: Session,
    incident: Incident,
    status: str,
    message: str,
    *,
    actor: str = WORKER_ACTOR,
    data: dict[str, Any] | None = None,
) -> None:
    """Change the status of ``incident`` (timeline event) and log one line for the transition."""
    previous = incident.status
    text = _clip(message)
    store_repo.set_status(session, incident, status, text, actor=actor, data=data)
    logger.info(
        "incident %s %s.%s: %s -> %s: %s",
        _short(incident.id),
        incident.dag_id,
        incident.task_id,
        previous,
        status,
        text,
    )


def log_claim(incident: Incident, previous: str) -> None:
    """Log the one line of the ``previous -> triaging`` transition made by a claim."""
    logger.info(
        "incident %s %s.%s: %s -> %s: claimed by the worker",
        _short(incident.id),
        incident.dag_id,
        incident.task_id,
        previous,
        TRIAGING,
    )


def triaged_since(session: Session, since: datetime) -> int:
    """Number of automatic triages started at or after ``since`` (rate limiting)."""
    query = (
        select(func.count())
        .select_from(IncidentEvent)
        .where(IncidentEvent.kind == TRIAGE_EVENT, IncidentEvent.at >= since)
    )
    return int(session.scalar(query) or 0)


# =========================================================================== error summaries


def compose_error_summary(
    exception_type: str | None,
    exception_message: str | None,
    traceback_text: str | None,
    fallback: str | None = None,
) -> str:
    """The masked ``error_summary`` of an incident: ``Type: message``, then the traceback.

    The first line is what lists and alerts show; the traceback below it keeps the evidence
    for a later re-triage when the Airflow log is no longer reachable.
    """
    head = ""
    if exception_type:
        first = (exception_message or "").strip().splitlines()
        head = f"{exception_type}: {first[0]}" if first else str(exception_type)
    body = (traceback_text or "").strip()
    if not head and not body:
        text = (fallback or "").strip()
        if len(text) > MAX_ERROR_SUMMARY_CHARS:
            text = "[... truncated ...]\n" + text[-(MAX_ERROR_SUMMARY_CHARS - 20) :]
        return mask_text(text)
    summary = f"{head}\n\n{body}" if head and body else (head or body)
    if len(summary) > MAX_ERROR_SUMMARY_CHARS:
        keep = MAX_ERROR_SUMMARY_CHARS - len(head) - 40
        summary = f"{head}\n\n[... truncated ...]\n{body[-max(keep, 0) :]}"
    return mask_text(summary)


def failure_signature(
    dag_id: str,
    task_id: str,
    exception_type: str | None,
    exception_message: str | None,
    frames: Iterable[Mapping[str, Any]] = (),
) -> str | None:
    """Stable fingerprint of a failure (dedupe of repeated failures within 24 hours).

    Built from the DAG, the task, the exception type, the masked message with its numbers
    normalised and the ``file:function`` of every traceback frame (base names only, so the
    mount point and the line numbers do not matter). ``None`` without an exception.
    """
    if not exception_type and not exception_message:
        return None
    message = re.sub(r"\d+", "0", mask_text(exception_message or ""))[:500]
    parts = [dag_id, task_id, str(exception_type or "").rsplit(".", 1)[-1], message]
    for frame in frames:
        name = posixpath.basename(str(frame.get("file") or "").replace("\\", "/"))
        parts.append(f"{name}:{frame.get('function') or ''}")
    return hashlib.sha256("\n".join(parts).encode("utf-8")).hexdigest()[:40]


def summarize_failure(dag_id: str, task_id: str, text: str | None) -> tuple[str, str | None]:
    """``(error_summary, signature)`` of a raw error text or log (masked)."""
    from dag_healer.agent.context import extract_traceback

    exc_type, exc_message, frames, traceback_text = extract_traceback(text or "")
    summary = compose_error_summary(
        exc_type,
        mask_text(exc_message) if exc_message else None,
        mask_text(traceback_text),
        fallback=_last_lines(text or "", 40),
    )
    return summary, failure_signature(dag_id, task_id, exc_type, exc_message, frames)


def _last_lines(text: str, count: int) -> str:
    lines = [line for line in text.replace("\r\n", "\n").split("\n") if line.strip()]
    return "\n".join(lines[-count:])


def combine_pytest_output(before: str | None, after: str | None) -> str | None:
    """The single ``pytest_output`` column: the run before the fix, then the run after it."""
    parts: list[str] = []
    if before:
        parts.append("=== pytest before the fix (the new test must fail) ===\n" + before.strip())
    if after:
        parts.append("=== pytest after the fix ===\n" + after.strip())
    if not parts:
        return None
    text = mask_text("\n\n".join(parts))
    if len(text) > MAX_PYTEST_OUTPUT_CHARS:
        text = "[... truncated ...]\n" + text[-(MAX_PYTEST_OUTPUT_CHARS - 20) :]
    return text


# =========================================================================== state file


def state_path(cfg: HealerConfig) -> Path:
    """``<healer_home>/state.json``: the worker's small persistent state (REST watermark)."""
    return cfg.healer_home / STATE_FILE


def load_state(cfg: HealerConfig) -> dict[str, Any]:
    """Read the state file (an empty dict when it is missing or unreadable)."""
    path = state_path(cfg)
    try:
        data = json.loads(path.read_text(encoding="utf-8"))
    except FileNotFoundError:
        return {}
    except (OSError, ValueError) as exc:
        logger.warning("dag-healer worker: ignoring unreadable state file %s (%s)", path, type(exc).__name__)
        return {}
    return data if isinstance(data, dict) else {}


def save_state(cfg: HealerConfig, state: Mapping[str, Any]) -> None:
    """Write the state file atomically."""
    path = state_path(cfg)
    path.parent.mkdir(parents=True, exist_ok=True)
    fd, tmp_name = tempfile.mkstemp(prefix=f".{path.name}.", suffix=".tmp", dir=path.parent)
    try:
        with os.fdopen(fd, "w", encoding="utf-8", newline="\n") as handle:
            json.dump(to_json_safe(dict(state)), handle, indent=2, sort_keys=True)
        os.replace(tmp_name, path)
    except BaseException:
        Path(tmp_name).unlink(missing_ok=True)
        raise


def _state_time(cfg: HealerConfig, key: str) -> datetime | None:
    value = load_state(cfg).get(key)
    if not isinstance(value, str) or not value:
        return None
    try:
        return ensure_utc(datetime.fromisoformat(value.replace("Z", "+00:00")))
    except ValueError:
        logger.warning("dag-healer worker: ignoring invalid %s %r in the state file", key, value)
        return None


def get_watermark(cfg: HealerConfig) -> datetime | None:
    """When the last successful REST poll started (``None`` before the first one)."""
    return _state_time(cfg, WATERMARK_KEY)


def get_not_before(cfg: HealerConfig) -> datetime | None:
    """The REST poll floor: failures updated before it are ignored (``None``: no floor)."""
    return _state_time(cfg, NOT_BEFORE_KEY)


def reset_state(cfg: HealerConfig, now: datetime | None = None) -> datetime:
    """Restart the REST poll at ``now``: failures updated before that moment are never listed.

    Used after the store was emptied (``dag-healer store reset``) so that old failed task
    instances still in Airflow's history do not come back as new incidents, even within the
    overlap window of the next polls. Returns the moment that was recorded.
    """
    moment = ensure_utc(now) if now is not None else utcnow()
    state = load_state(cfg)
    state[WATERMARK_KEY] = moment.isoformat()
    state[NOT_BEFORE_KEY] = moment.isoformat()
    save_state(cfg, state)
    return moment


def set_watermark(cfg: HealerConfig, value: datetime) -> None:
    """Persist the REST watermark."""
    state = load_state(cfg)
    moment = ensure_utc(value) or utcnow()
    state[WATERMARK_KEY] = moment.isoformat()
    save_state(cfg, state)


# =========================================================================== reconcile


@dataclass
class ReconcileStats:
    """What one :func:`reconcile` pass did."""

    listed: int = 0
    created: list[str] = field(default_factory=list)
    folded: int = 0
    known: int = 0
    unmonitored: int = 0
    confirmed: int = 0
    discarded: int = 0
    pending_left: int = 0
    since: datetime | None = None
    errors: list[str] = field(default_factory=list)


def _poll_since(cfg: HealerConfig, now: datetime) -> datetime:
    watermark = get_watermark(cfg)
    if watermark is None:
        since = now - INITIAL_LOOKBACK
    else:
        overlap = max(MIN_POLL_OVERLAP, timedelta(seconds=2 * cfg.monitor.poll_interval_seconds))
        since = watermark - overlap
    floor = get_not_before(cfg)
    if floor is not None and since < floor:
        since = floor
    return since


def _record_new_incident(
    session_factory: SessionFactory,
    stats: ReconcileStats,
    *,
    key: Mapping[str, Any],
    source: str,
    error_summary: str,
    signature: str | None,
    now: datetime,
    note: str | None = None,
    pending_event_id: int | None = None,
) -> None:
    with session_factory() as session:
        incident, created = store_repo.create_incident(
            session,
            dag_id=key["dag_id"],
            run_id=key["run_id"],
            task_id=key["task_id"],
            map_index=int(key["map_index"]),
            try_number=int(key["try_number"]),
            source=source,
            error_summary=error_summary,
            signature=signature,
            now=now,
        )
        if note and created:
            store_repo.add_event(
                session, incident, "confirmed", note, data={"pending_event_id": pending_event_id}
            )
        if pending_event_id is not None:
            event = session.get(PendingEvent, pending_event_id)
            if event is not None:
                store_repo.mark_pending_event(session, event, PENDING_CONFIRMED)
        incident_id = incident.id
        dedup = not created and (
            incident.dag_id,
            incident.run_id,
            incident.task_id,
            incident.map_index,
            incident.try_number,
        ) != (key["dag_id"], key["run_id"], key["task_id"], int(key["map_index"]), int(key["try_number"]))
    if created:
        stats.created.append(incident_id)
        logger.info(
            "incident %s %s.%s: created from %s (run %s, try %s)",
            _short(incident_id),
            key["dag_id"],
            key["task_id"],
            source,
            key["run_id"],
            key["try_number"],
        )
    elif dedup:
        stats.folded += 1
        logger.info(
            "incident %s %s.%s: same failure seen again in run %s (try %s)",
            _short(incident_id),
            key["dag_id"],
            key["task_id"],
            key["run_id"],
            key["try_number"],
        )
    else:
        stats.known += 1


def _ti_key(ti: TaskInstanceInfo) -> dict[str, Any]:
    return {
        "dag_id": ti.dag_id,
        "run_id": ti.run_id,
        "task_id": ti.task_id,
        "map_index": ti.map_index,
        "try_number": ti.try_number,
    }


def _reconcile_rest(
    cfg: HealerConfig,
    session_factory: SessionFactory,
    client: AirflowClient | Any,
    stats: ReconcileStats,
    now: datetime,
) -> None:
    since = _poll_since(cfg, now)
    stats.since = since
    try:
        failed = client.list_failed_task_instances(updated_since=since)
    except Exception as exc:  # AirflowAPIError, network errors: retried on the next poll
        message = _error_text(exc)
        stats.errors.append(f"listing failed task instances: {message}")
        logger.warning("dag-healer worker: listing failed task instances failed: %s", message)
        return
    stats.listed = len(failed)
    for ti in failed:
        if not ti.dag_id or not ti.task_id or not ti.run_id:
            continue
        if not cfg.is_monitored(ti.dag_id):
            stats.unmonitored += 1
            continue
        key = _ti_key(ti)
        with session_factory() as session:
            existing = store_repo.get_incident_by_key(session, **key)
        if existing is not None:
            stats.known += 1
            continue
        try:
            log_text = client.get_task_log(
                ti.dag_id, ti.run_id, ti.task_id, ti.try_number, map_index=ti.map_index
            )
        except Exception as exc:  # the incident is created anyway; triage fetches the log again
            logger.info(
                "dag-healer worker: could not fetch the log of %s.%s (run %s, try %s): %s",
                ti.dag_id,
                ti.task_id,
                ti.run_id,
                ti.try_number,
                _error_text(exc),
            )
            log_text = ""
        summary, signature = summarize_failure(ti.dag_id, ti.task_id, log_text)
        _record_new_incident(
            session_factory,
            stats,
            key=key,
            source="rest",
            error_summary=summary,
            signature=signature,
            now=now,
        )
    set_watermark(cfg, now)


def _confirm(ti: TaskInstanceInfo, try_number: int, trigger: str) -> tuple[bool, str]:
    """Does the task instance state confirm the reported failure of ``try_number``?"""
    state = (ti.state or "").lower() or "unknown"
    if ti.try_number < try_number:
        return False, f"Airflow knows only {ti.try_number} tries (state {state})"
    if trigger == "every_failure":
        if state in ("failed", "up_for_retry") or ti.try_number > try_number:
            return True, f"state {state}, try {ti.try_number}"
        return False, f"the task instance is {state}, not failed"
    if ti.try_number > try_number:
        return False, f"try {try_number} was retried (try {ti.try_number} is {state})"
    if state == "failed":
        return True, f"state failed, try {ti.try_number}"
    if state == "up_for_retry":
        return False, "the try will be retried (monitor.trigger is final_failure)"
    return False, f"the task instance is {state}, not failed"


def _discard(session_factory: SessionFactory, event_id: int, reason: str, stats: ReconcileStats) -> None:
    with session_factory() as session:
        event = session.get(PendingEvent, event_id)
        if event is not None:
            store_repo.mark_pending_event(session, event, PENDING_DISCARDED)
    stats.discarded += 1
    logger.info("dag-healer worker: discarded pending event %s: %s", event_id, _clip(reason, 300))


def _reconcile_pending(
    cfg: HealerConfig,
    session_factory: SessionFactory,
    client: AirflowClient | Any | None,
    stats: ReconcileStats,
    now: datetime,
) -> None:
    from dag_healer.airflow.client import AirflowAPIError

    modes = set(cfg.monitor.modes)
    with session_factory() as session:
        events = [event.to_dict() for event in store_repo.list_pending_events(session)]
    for event in events:
        event_id = int(event["id"])
        payload = event.get("payload") if isinstance(event.get("payload"), Mapping) else {}
        source = str(payload.get("source") or "listener")
        if source not in EVENT_SOURCES:
            source = "listener"
        if source not in modes:
            _discard(session_factory, event_id, f"{source} events are not enabled in monitor.modes", stats)
            continue
        if not cfg.is_monitored(str(event["dag_id"])):
            _discard(session_factory, event_id, f"DAG {event['dag_id']} is not monitored", stats)
            continue
        if client is None:
            stats.pending_left += 1
            continue
        received = (
            ensure_utc(datetime.fromisoformat(event["received_at"])) if event.get("received_at") else now
        )
        try:
            ti = client.get_task_instance(
                event["dag_id"], event["run_id"], event["task_id"], int(event["map_index"])
            )
        except Exception as exc:
            status_code = exc.status_code if isinstance(exc, AirflowAPIError) else None
            if status_code == 404:
                _discard(session_factory, event_id, "Airflow does not know this task instance", stats)
            elif received is not None and now - received > PENDING_MAX_AGE:
                _discard(session_factory, event_id, f"unconfirmed after {PENDING_MAX_AGE}", stats)
            else:
                stats.pending_left += 1
                stats.errors.append(f"confirming pending event {event_id}: {_error_text(exc)}")
                logger.warning(
                    "dag-healer worker: could not confirm pending event %s: %s", event_id, _error_text(exc)
                )
            continue
        confirmed, reason = _confirm(ti, int(event["try_number"]), cfg.monitor.trigger)
        if not confirmed:
            _discard(session_factory, event_id, reason, stats)
            continue
        key = {name: event[name] for name in ("dag_id", "run_id", "task_id", "map_index", "try_number")}
        summary, signature = summarize_failure(key["dag_id"], key["task_id"], str(payload.get("error") or ""))
        _record_new_incident(
            session_factory,
            stats,
            key=key,
            source=source,
            error_summary=summary,
            signature=signature,
            now=now,
            note=f"The {source} event was confirmed by the Airflow API ({reason}).",
            pending_event_id=event_id,
        )
        stats.confirmed += 1


def reconcile(
    cfg: HealerConfig,
    session_factory: SessionFactory | None,
    client: AirflowClient | Any | None,
    *,
    now: datetime | None = None,
) -> ReconcileStats:
    """Create the incidents of new failures (REST poll and pushed events); see the module docstring.

    ``client`` may be ``None`` (no Airflow API): the REST poll is skipped and pending events
    stay pending. Never raises for Airflow errors (they are logged and retried next time).
    """
    moment = _now(now)
    factory = session_factory or default_session_factory(cfg)
    stats = ReconcileStats()
    modes = set(cfg.monitor.modes)
    if "rest" in modes:
        if client is None:
            stats.errors.append("no Airflow API client: the REST poll was skipped")
        else:
            _reconcile_rest(cfg, factory, client, stats, moment)
    _reconcile_pending(cfg, factory, client, stats, moment)
    if stats.created or stats.folded or stats.confirmed or stats.discarded:
        logger.info(
            "dag-healer worker: reconcile listed %d failed task instances, created %d incidents, "
            "folded %d repeated failures, confirmed %d and discarded %d pushed events",
            stats.listed,
            len(stats.created),
            stats.folded,
            stats.confirmed,
            stats.discarded,
        )
    return stats


def apply_commands(
    cfg: HealerConfig,
    session_factory: SessionFactory | None,
    client: AirflowClient | Any | None,
    *,
    now: datetime | None = None,
) -> list[CommandOutcome]:
    """Apply the pending human commands (see :func:`dag_healer.worker.commands.apply_commands`)."""
    from dag_healer.worker.commands import apply_commands as _apply_commands

    return _apply_commands(cfg, session_factory, client, now=now)


# =========================================================================== process one incident


def _violation_dicts(violations: Iterable[Any], contract_status: str) -> list[dict[str, Any]]:
    result: list[dict[str, Any]] = []
    for violation in violations:
        data = violation.model_dump(mode="json") if hasattr(violation, "model_dump") else dict(violation)
        data = _masked_json(data)
        data["contract_status"] = contract_status
        result.append(data)
    return result


def evidence_violations(ctx: IncidentContext) -> tuple[list[Any], str]:
    """The violations to show for an incident: the approved contract's, else the draft's."""
    if ctx.violations:
        return list(ctx.violations), ctx.contract_status
    if ctx.draft_violations:
        return list(ctx.draft_violations), ctx.contract_status
    return [], ctx.contract_status


def pick_violation(
    ctx: IncidentContext, violations: list[Any], contract_status: str | None = None
) -> dict[str, Any] | None:
    """The violation that best explains the failure: its column is named in the error, else the first.

    The returned dict carries ``contract_status`` so a rule of a draft contract is never
    mistaken for an enforced one.
    """
    if not violations:
        return None
    from dag_healer.agent.policy import compute_signals

    signals = compute_signals(ctx)
    wanted = [signals.missing_column_in_error, *signals.columns_in_error]
    chosen = next(
        (v for column in wanted if column for v in violations if getattr(v, "column", None) == column),
        violations[0],
    )
    data = chosen.model_dump(mode="json") if hasattr(chosen, "model_dump") else dict(chosen)
    data = _masked_json(data)
    data["contract_status"] = contract_status or ctx.contract_status
    return data


class _Pipeline:
    """State of one :func:`process_incident` call."""

    def __init__(
        self,
        cfg: HealerConfig,
        session_factory: SessionFactory,
        incident_id: str,
        *,
        provider: LLMProvider | None,
        client: AirflowClient | Any | None,
        notifier: Notifier | None,
        workspace_root: Path | None,
        now: datetime | None,
    ) -> None:
        self.cfg = cfg
        self.sf = session_factory
        self.incident_id = incident_id
        self.provider = provider
        self.client = client
        self.notifier = notifier
        self.workspace_root = workspace_root
        self.now = now
        self.stage = "starting"
        self.outcome: ClassificationOutcome | None = None

    # ------------------------------------------------------------------ store access
    def _get(self, session: Session) -> Incident:
        incident = store_repo.get_incident(session, self.incident_id)
        if incident is None:
            raise LookupError(f"incident {self.incident_id} disappeared from the store")
        return incident

    def _snapshot(self) -> dict[str, Any]:
        with self.sf() as session:
            return self._get(session).to_dict()

    # ------------------------------------------------------------------ steps
    def _start(self) -> dict[str, Any] | None:
        """Make sure the incident is claimed (``triaging``); ``None`` when it is not processable."""
        with self.sf() as session:
            incident = store_repo.get_incident(session, self.incident_id)
            if incident is None:
                logger.warning("dag-healer worker: incident %s not found", self.incident_id)
                return None
            if incident.status in (NEW, RESUMED):
                previous = incident.status
                claimed = store_repo.try_claim(session, incident.id, (previous,), now=self.now)
                if claimed is None:
                    return None
                if previous == NEW:
                    store_repo.add_event(session, claimed, TRIAGE_EVENT, "Automatic triage started")
                log_claim(claimed, previous)
                incident = claimed
            elif incident.status != TRIAGING:
                logger.info(
                    "incident %s %s.%s: not processed (status %s)",
                    _short(incident.id),
                    incident.dag_id,
                    incident.task_id,
                    incident.status,
                )
                return None
            return incident.to_dict()

    def _gather(self, snapshot: dict[str, Any]) -> IncidentContext:
        from dag_healer.agent.context import gather_context

        self.stage = "gathering the context"
        ctx = gather_context(self.cfg, snapshot, self.client)
        violations, _status = evidence_violations(ctx)
        with self.sf() as session:
            incident = self._get(session)
            if ctx.exception_type:
                incident.error_summary = compose_error_summary(
                    ctx.exception_type, ctx.exception_message, ctx.traceback_text
                )
            elif not incident.error_summary and ctx.error_text:
                incident.error_summary = compose_error_summary(None, None, None, fallback=ctx.error_text)
            if ctx.masked_sample is not None:
                incident.sample = ctx.masked_sample
            frame = ctx.top_repo_frame
            where = f" in {frame['file']}::{frame.get('function') or '?'}" if frame else ""
            store_repo.add_event(
                session,
                incident,
                "context",
                _clip(
                    f"Context gathered: {ctx.exception_type or 'no Python exception found'}{where}; "
                    f"contract {ctx.contract_status}, {len(violations)} violation(s)"
                ),
                data={
                    "exception_type": ctx.exception_type,
                    "exception_message": _clip(ctx.exception_message or "", 500) or None,
                    "repo_frames": [dict(f) for f in ctx.repo_frames],
                    "dag_file": ctx.dag_file,
                    "contract_status": ctx.contract_status,
                    "datasets_checked": list(ctx.datasets_checked),
                    "violation_count": len(ctx.violations),
                    "draft_violation_count": len(ctx.draft_violations),
                    "sample_dataset": ctx.sample_dataset,
                    "notes": [_clip(note, 500) for note in ctx.notes[:MAX_EVENT_NOTES]],
                },
            )
        return ctx

    def _classify(self, ctx: IncidentContext) -> str:
        from dag_healer.agent.classify import classify_incident

        self.stage = "classifying"
        outcome = classify_incident(self.cfg, ctx, self.provider)
        self.outcome = outcome
        decision = outcome.decision
        violations, status = evidence_violations(ctx)
        classification = outcome.classification
        decided_by = (
            getattr(self.provider, "name", "llm")
            if classification is not None and decision.needs_llm_confirmation
            else "policy"
        )
        with self.sf() as session:
            incident = self._get(session)
            incident.category = decision.category
            incident.confidence = round(float(decision.confidence), 4)
            incident.diagnosis = _clip(outcome.diagnosis, 8000) or None
            if outcome.violated_rule:
                incident.violated_rule = {
                    **_masked_json(outcome.violated_rule),
                    "contract_status": "approved",
                }
            elif decision.category != "code":
                incident.violated_rule = pick_violation(ctx, violations, status)
            else:
                incident.violated_rule = None
            incident.evidence = [_clip(item, 500) for item in outcome.evidence]
            incident.tokens_used = (incident.tokens_used or 0) + int(outcome.tokens_used or 0)
            store_repo.add_event(
                session,
                incident,
                "classification",
                _clip(f"Classified as {decision.category} ({decision.rule}): {decision.reason}"),
                data={
                    "category": decision.category,
                    "confidence": round(float(decision.confidence), 4),
                    "rule": decision.rule,
                    "reason": _clip(decision.reason),
                    "decided_by": decided_by,
                    "needs_llm_confirmation": decision.needs_llm_confirmation,
                    "llm_category": classification.category if classification is not None else None,
                    "llm_confidence": classification.confidence if classification is not None else None,
                    "llm_error": _clip(outcome.llm_error, 500) if outcome.llm_error else None,
                    "tokens_used": int(outcome.tokens_used or 0),
                    "contract_status": ctx.contract_status,
                    "error_kind": outcome.signals.error_kind,
                    "violations": _violation_dicts(violations, status),
                },
            )
            transition(
                session,
                incident,
                CLASSIFIED,
                f"Classified as {decision.category} "
                f"(confidence {decision.confidence:.2f}, rule {decision.rule})",
            )
        return decision.category

    def _human_decision(self, ctx: IncidentContext, forced: str, snapshot: Mapping[str, Any]) -> str:
        self.stage = "applying the human decision"
        actor = str(snapshot.get("resolved_by") or "a human")
        violations, status = evidence_violations(ctx)
        reason = f"{actor} resolved the incident as {forced}; the classification was skipped."
        with self.sf() as session:
            incident = self._get(session)
            incident.category = forced
            incident.confidence = 1.0
            if forced == "data":
                incident.violated_rule = pick_violation(ctx, violations, status)
            if not incident.diagnosis:
                error = ctx.exception_type or "the task error"
                incident.diagnosis = _clip(
                    f"{actor} resolved this incident as a {forced} problem. The task failed with {error}"
                    + (f": {ctx.exception_message}" if ctx.exception_message else "")
                    + ".",
                    8000,
                )
            store_repo.add_event(
                session,
                incident,
                "classification",
                _clip(f"Category {forced} set by {actor}"),
                data={
                    "category": forced,
                    "confidence": 1.0,
                    "rule": HUMAN_RULE,
                    "reason": _clip(reason),
                    "decided_by": actor,
                    "contract_status": ctx.contract_status,
                    "violations": _violation_dicts(violations, status),
                },
                actor=actor,
            )
            transition(session, incident, CLASSIFIED, f"Category {forced} set by {actor}")
        return forced

    def _alert(self, event: str, extra: Mapping[str, Any]) -> None:
        """Send the alert of ``event`` (outside any transaction) and record the outcomes."""
        from dag_healer.alerts import notify

        self.stage = f"sending the {event} alert"
        snapshot = self._snapshot()
        notifier = self.notifier or notify
        try:
            outcomes = list(notifier(self.cfg, snapshot, event, extra=dict(extra)))
        except Exception as exc:  # a custom notifier must not break the pipeline
            logger.warning("dag-healer worker: the %s alert failed: %s", event, _error_text(exc))
            outcomes = []
        enabled = event in self.cfg.notify.events
        with self.sf() as session:
            incident = self._get(session)
            if enabled:
                for outcome in outcomes:
                    store_repo.record_alert(session, incident_id=incident.id, **outcome.as_record())
            channels = ", ".join(f"{o.channel} {o.status}" for o in outcomes) or "no channel"
            store_repo.add_event(
                session,
                incident,
                "alert",
                f"{event} alert: {channels}" if enabled else f"{event} alerts are disabled in notify.events",
                data={
                    "event": event,
                    "outcomes": [
                        {
                            "channel": o.channel,
                            "status": o.status,
                            "error": _clip(o.error, 500) if o.error else None,
                        }
                        for o in outcomes
                    ],
                },
            )

    def _report(self, ctx: IncidentContext) -> str:
        self.stage = "reporting"
        violations, _status = evidence_violations(ctx)
        with self.sf() as session:
            incident = self._get(session)
            transition(session, incident, REPORTING, "Reporting the data issue")
            violated_rule = incident.violated_rule
            diagnosis = incident.diagnosis
        self._alert(
            "reported",
            {
                "status": REPORTED,
                "violations": violations,
                "violated_rule": violated_rule,
                "sample": ctx.masked_sample,
                "diagnosis": diagnosis,
            },
        )
        with self.sf() as session:
            incident = self._get(session)
            rule = violated_rule.get("rule") if isinstance(violated_rule, Mapping) else None
            transition(
                session,
                incident,
                REPORTED,
                f"Data issue reported ({rule})" if rule else "Data issue reported",
            )
        return REPORTED

    def _wait_for_human(self, ctx: IncidentContext) -> str:
        self.stage = "waiting for a human"
        violations, _status = evidence_violations(ctx)
        reason = self.outcome.decision.reason if self.outcome is not None else "A human decision is needed."
        with self.sf() as session:
            incident = self._get(session)
            transition(session, incident, WAITING_HUMAN, f"Waiting for a human decision: {reason}")
        self._alert(
            "waiting_human",
            {"reason": reason, "violations": violations, "sample": ctx.masked_sample},
        )
        return WAITING_HUMAN

    def _repair(self, ctx: IncidentContext) -> str:
        from dag_healer.agent.repair import RepairResult, run_repair

        self.stage = "repairing"
        with self.sf() as session:
            incident = self._get(session)
            transition(session, incident, REPAIRING, "Reproducing the failure and fixing the code in a clone")
            incident.claimed_at = _now(None)  # a long repair is not a stale claim
            snapshot = incident.to_dict()
        if self.provider is None:
            result = RepairResult(status=REPAIR_FAILED, error="No LLM provider is configured.")
        else:
            result = run_repair(self.cfg, snapshot, ctx, self.provider, workspace_root=self.workspace_root)
        final = REPAIRED if result.status == REPAIRED else REPAIR_FAILED
        error = _clip(result.error, 2000) if result.error else None
        notes = [_clip(note, 500) for note in result.notes[:MAX_EVENT_NOTES]]
        with self.sf() as session:
            incident = self._get(session)
            incident.test_path = result.test_path
            incident.test_code = result.test_code
            incident.diff = result.diff
            incident.pytest_output = combine_pytest_output(result.pytest_before, result.pytest_after)
            incident.pr_url = result.pr_url
            incident.pr_md_path = result.pr_md_path
            incident.attempts = (incident.attempts or 0) + int(result.attempts or 0)
            incident.tokens_used = (incident.tokens_used or 0) + int(result.tokens_used or 0)
            if final == REPAIRED:
                if result.pr_url:
                    where = f"pull request {result.pr_url}"
                elif result.pr_md_path:
                    where = "PR.md and fix.patch written"
                else:
                    where = "no pull request"
                message = f"Fix proposed on branch {result.branch} ({where})"
            else:
                message = f"Repair failed: {error or 'unknown error'}"
            store_repo.add_event(
                session,
                incident,
                "repair",
                _clip(message),
                data={
                    "status": result.status,
                    "branch": result.branch,
                    "test_path": result.test_path,
                    "created_tests_dir": result.created_tests_dir,
                    "pr_url": result.pr_url,
                    "pr_md_path": result.pr_md_path,
                    "patch_path": result.patch_path,
                    "attempts": int(result.attempts or 0),
                    "tokens_used": int(result.tokens_used or 0),
                    "error": error,
                    "notes": notes,
                },
            )
            transition(session, incident, final, message)
        fix_summary = None
        if self.outcome is not None and self.outcome.classification is not None:
            fix_summary = self.outcome.classification.fix_summary
        extra: dict[str, Any] = {
            "branch": result.branch,
            "test_path": result.test_path,
            "pr_url": result.pr_url,
            "pr_md_path": result.pr_md_path,
            "attempts": int(result.attempts or 0),
            "notes": notes,
        }
        if fix_summary:
            extra["fix_summary"] = fix_summary
        if final == REPAIR_FAILED:
            extra["error"] = error or "The repair failed."
        self._alert(final, extra)
        return final

    # ------------------------------------------------------------------ driver
    def run(self) -> str:
        snapshot = self._start()
        if snapshot is None:
            return self._current_status()
        resolution = snapshot.get("resolution")
        forced = resolution if resolution in FORCED_CATEGORIES else None
        ctx = self._gather(snapshot)
        if forced is not None:
            category = self._human_decision(ctx, forced, snapshot)
        else:
            category = self._classify(ctx)
        if category == "data":
            return self._report(ctx)
        if category == "code":
            return self._repair(ctx)
        return self._wait_for_human(ctx)

    def _current_status(self) -> str:
        try:
            with self.sf() as session:
                incident = store_repo.get_incident(session, self.incident_id)
                return incident.status if incident is not None else ERROR
        except Exception:  # pragma: no cover - store unavailable
            return ERROR

    def fail(self, exc: BaseException) -> str:
        """Put the incident in ``error`` with a masked timeline event (best effort)."""
        message = _error_text(exc)
        logger.error(
            "incident %s: processing failed while %s: %s", _short(self.incident_id), self.stage, message
        )
        logger.debug("incident %s: processing failure details", _short(self.incident_id), exc_info=True)
        try:
            with self.sf() as session:
                incident = store_repo.get_incident(session, self.incident_id)
                if incident is None:
                    return ERROR
                store_repo.add_event(
                    session,
                    incident,
                    "error",
                    _clip(f"Processing failed while {self.stage}: {message}"),
                    data={"stage": self.stage, "error": message},
                )
                transition(session, incident, ERROR, f"Processing failed while {self.stage}")
        except Exception as inner:  # the store itself is failing
            logger.error(
                "incident %s: could not record the processing failure: %s",
                _short(self.incident_id),
                _error_text(inner),
            )
        return ERROR


def process_incident(
    cfg: HealerConfig,
    session_factory: SessionFactory | None,
    incident_id: str,
    *,
    provider: LLMProvider | None,
    client: AirflowClient | Any | None,
    notifier: Notifier | None = None,
    workspace_root: Path | None = None,
    now: datetime | None = None,
) -> str:
    """Triage one incident end to end and return its final status (see the module docstring).

    The incident is normally claimed (``triaging``) by the caller; a ``new`` or ``resumed``
    incident is claimed here. An incident in any other status is left alone. A ``resolution``
    of ``data`` or ``code`` (set by a human command) skips the classification. ``notifier``
    replaces :func:`dag_healer.alerts.notify` (tests). Never raises.
    """
    factory = session_factory or default_session_factory(cfg)
    pipeline = _Pipeline(
        cfg,
        factory,
        incident_id,
        provider=provider,
        client=client,
        notifier=notifier,
        workspace_root=workspace_root,
        now=now,
    )
    try:
        return pipeline.run()
    except Exception as exc:  # noqa: BLE001 - one broken incident never stops the worker
        return pipeline.fail(exc)


__all__ = [
    "INITIAL_LOOKBACK",
    "STALE_CLAIM_AFTER",
    "TRIAGE_EVENT",
    "WORKER_ACTOR",
    "ReconcileStats",
    "SessionFactory",
    "apply_commands",
    "combine_pytest_output",
    "compose_error_summary",
    "default_session_factory",
    "evidence_violations",
    "failure_signature",
    "get_not_before",
    "get_watermark",
    "load_state",
    "log_claim",
    "pick_violation",
    "process_incident",
    "reconcile",
    "reset_state",
    "save_state",
    "set_watermark",
    "state_path",
    "summarize_failure",
    "transition",
    "triaged_since",
]
