"""The incident pipeline of the dag-healer worker: reconcile failures, then triage each incident.

Three entry points, all called by :func:`dag_healer.worker.loop.run_worker` (and usable on
their own, for example from tests or a one-shot CLI run). They reach the orchestrator only
through the ports of its adapter (:class:`~dag_healer.core.ports.OrchestratorAdapter`); a
phase-5 Airflow REST client passed as ``client`` is wrapped by
:func:`dag_healer.orchestrators.compat.as_adapter`, and no adapter at all means offline:

* :func:`reconcile` finds failures of the adapter's orchestrator instance. With a poll mode
  in ``monitor.modes`` (``rest``/``poll``) it lists the failures the adapter's feed reports
  after the poll cursor kept in ``healer_kv`` (``cursor:<instance>``; a phase-5
  ``<healer_home>/state.json`` is imported once) and creates the missing incidents. With
  ``queue`` it drains the adapter's hint queue into pending events. Then it confirms each
  ``pending_check`` event of the instance (pushed by the Airflow listener/callback, drained
  from the queue...) through the orchestrator API (``adapter.push.confirm``; with
  ``monitor.trigger: final_failure`` an attempt that will be retried is discarded) before
  creating its incident. The orchestrator API is the source of truth, events are only a hint
  (forged events are discarded).
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

Long operations (orchestrator calls, the LLM, repairs, SMTP) never run inside a database
transaction: each step opens a short session from ``session_factory`` (a callable returning
a context manager that commits on success, :func:`dag_healer.store.db.session_scope` by
default), so the API and the UI see every transition as it happens.
"""

from __future__ import annotations

import hashlib
import inspect
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
from sqlalchemy.exc import DataError, IntegrityError
from sqlalchemy.orm import Session

from dag_healer import links
from dag_healer.core.errors import OrchestratorError
from dag_healer.core.failures import AttemptState, Confirmation, FailureHint
from dag_healer.core.ids import DEFAULT_ORCHESTRATOR, AttemptRef
from dag_healer.core.vocab import Vocabulary
from dag_healer.ingest import MAX_ERROR_CHARS, TRUNCATION_MARKER
from dag_healer.masking import mask_text, to_json_safe
from dag_healer.orchestrators.compat import as_adapter
from dag_healer.orchestrators.registry import DEFAULT_TYPE, orchestrator_settings
from dag_healer.store import repo as store_repo
from dag_healer.store.db import session_scope
from dag_healer.store.models import (
    CLASSIFIED,
    DEFAULT_CHANNEL,
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
    from dag_healer.alerts import AlertOutcome
    from dag_healer.config import HealerConfig
    from dag_healer.core.ports import FailureFeed, OrchestratorAdapter
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
#: Without a watermark the first Airflow REST poll looks this far back (the Airflow adapter's
#: poll window; kept here for the phase-5 callers).
INITIAL_LOOKBACK = timedelta(hours=24)
#: Minimum overlap between two Airflow REST polls (the API gives no ``updated_at`` per row and
#: the clocks of the worker and Airflow may differ).
MIN_POLL_OVERLAP = timedelta(seconds=60)
#: Pending events that still cannot be confirmed after this long are discarded.
PENDING_MAX_AGE = timedelta(hours=24)
#: The phase-5 worker state file and its keys (imported once into the poll cursor).
STATE_FILE = "state.json"
WATERMARK_KEY = "rest_watermark"
#: Failures updated before this moment are never listed (set by ``dag-healer store reset``).
NOT_BEFORE_KEY = "rest_not_before"
#: Keys of the poll cursor (``healer_kv`` key ``cursor:<instance>``) the core reads and writes:
#: when the last successful poll started, and the floor written by ``dag-healer store reset``
#: (every built-in feed honours it). The rest of a cursor belongs to the adapter's feed.
CURSOR_WATERMARK = "watermark"
CURSOR_NOT_BEFORE = "not_before"

MAX_ERROR_SUMMARY_CHARS = 8000
MAX_PYTEST_OUTPUT_CHARS = 40_000
MAX_EVENT_TEXT_CHARS = 2000
MAX_EVENT_NOTES = 20
HUMAN_RULE = "human_resolution"

#: Sources of pushed events without an adapter (the phase-5 Airflow listener and callback).
EVENT_SOURCES: tuple[str, ...] = ("listener", "callback")
#: Incident sources that come from pushed hints (an adapter's push sources are among these).
PUSH_SOURCES: tuple[str, ...] = ("listener", "callback", "push", "eventbridge", "sqs")
#: ``payload["via"]`` of a pending event drained from the adapter's hint queue.
VIA_QUEUE = "queue"
#: How the store rejects the pending event of one hint (an empty id, a value that does not fit
#: its column...): the queued message is dropped, not delivered again.
_REJECTED_BY_STORE = (ValueError, TypeError, DataError, IntegrityError)
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


def _cause(exc: BaseException) -> BaseException:
    """The error an adapter translated (``raise OrchestratorError(...) from exc``), else ``exc``.

    Texts keep naming the orchestrator's own error (``AirflowAPIError: Airflow API error 503:
    ...``), as in phase 5, rather than the core class that carries it.
    """
    if isinstance(exc, OrchestratorError) and exc.__cause__ is not None:
        return exc.__cause__
    return exc


def _cause_text(exc: BaseException) -> str:
    """:func:`_error_text` of :func:`_cause` (masked and bounded)."""
    return _error_text(_cause(exc))


def _vocabulary(adapter: OrchestratorAdapter | None) -> dict[str, str]:
    """The words of ``adapter`` over the Airflow defaults (:meth:`Vocabulary.as_dict` keys)."""
    words = Vocabulary().as_dict()
    if adapter is not None:
        try:
            own = adapter.vocabulary.as_dict()
        except Exception:  # a broken vocabulary keeps the default words
            own = {}
        words.update({key: str(word) for key, word in own.items() if key in words and word})
    return words


def _instance(cfg: HealerConfig, adapter: OrchestratorAdapter | None) -> str:
    """The orchestrator instance the worker serves: the adapter's, else the configured one."""
    if adapter is not None:
        return adapter.instance
    try:
        return orchestrator_settings(cfg).instance
    except Exception:  # an unreadable orchestrator section: the phase-5 instance
        return DEFAULT_ORCHESTRATOR


def _wants(cfg: HealerConfig, kind: str) -> bool:
    """``monitor.wants(kind)``; a kind the configuration does not know is not wanted."""
    try:
        return bool(cfg.monitor.wants(kind))
    except ValueError:
        return False


def _masked_tail(text: str, limit: int = MAX_ERROR_CHARS) -> str:
    """``text`` masked, then cut to its last ``limit`` characters (where Python puts the error)."""
    masked = mask_text(text or "")
    if len(masked) <= limit:
        return masked
    return TRUNCATION_MARKER + masked[-(limit - len(TRUNCATION_MARKER)) :]


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


def triaged_since(session: Session, since: datetime, *, orchestrator: str | None = None) -> int:
    """Number of automatic triages started at or after ``since`` (rate limiting).

    ``orchestrator`` counts only the incidents of that instance (``None``: every instance).
    """
    query = (
        select(func.count())
        .select_from(IncidentEvent)
        .where(IncidentEvent.kind == TRIAGE_EVENT, IncidentEvent.at >= since)
    )
    if orchestrator is not None:
        query = query.join(Incident, Incident.id == IncidentEvent.incident_id).where(
            Incident.orchestrator == orchestrator
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
    for a later re-triage when the orchestrator's log is no longer reachable.
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


# =========================================================================== poll cursor


def state_path(cfg: HealerConfig) -> Path:
    """``<healer_home>/state.json``: where the phase-5 worker kept its REST watermark.

    The poll position now lives in ``healer_kv`` (``cursor:<instance>``); this file is only
    read once, by the first poll of an Airflow instance (:func:`dag_healer.store.repo.import_state_file`).
    """
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


def _read_cursor(session: Session, cfg: HealerConfig, *, instance: str, kind: str) -> dict[str, Any] | None:
    """The poll cursor of ``instance`` in ``healer_kv`` (``None`` before the first poll).

    The first read of an instance of the default (Airflow) type imports a phase-5
    ``state.json`` (its watermark and its reset floor) once.
    """
    if kind == DEFAULT_TYPE:
        store_repo.import_state_file(session, state_path(cfg), instance=instance)
    value = store_repo.kv_get(session, store_repo.cursor_key(instance))
    return dict(value) if isinstance(value, Mapping) else None


def _configured_cursor(cfg: HealerConfig) -> dict[str, Any] | None:
    settings = orchestrator_settings(cfg)
    with default_session_factory(cfg)() as session:
        return _read_cursor(session, cfg, instance=settings.instance, kind=settings.type)


def _cursor_time(cursor: Mapping[str, Any] | None, name: str) -> datetime | None:
    value = cursor.get(name) if cursor else None
    if value is None or value == "":
        return None
    try:
        return ensure_utc(datetime.fromisoformat(str(value).replace("Z", "+00:00")))
    except ValueError:
        logger.warning("dag-healer worker: ignoring invalid %s %r in the poll cursor", name, value)
        return None


def get_watermark(cfg: HealerConfig) -> datetime | None:
    """When the last successful poll started (``None`` before the first one).

    Read from the poll cursor of the configured orchestrator instance in ``healer_kv``.
    """
    return _cursor_time(_configured_cursor(cfg), CURSOR_WATERMARK)


def get_not_before(cfg: HealerConfig) -> datetime | None:
    """The poll floor: failures updated before it are ignored (``None``: no floor)."""
    return _cursor_time(_configured_cursor(cfg), CURSOR_NOT_BEFORE)


def reset_state(cfg: HealerConfig, now: datetime | None = None) -> datetime:
    """Restart the poll at ``now``: failures updated before that moment are never listed.

    Used after the store was emptied (``dag-healer store reset``) so that old failures still
    in the orchestrator's history do not come back as new incidents, even within the overlap
    window of the next polls. Writes ``{"watermark": now, "not_before": now}`` as the poll
    cursor of the configured instance (a phase-5 ``state.json`` is not imported any more) and
    returns the moment that was recorded.
    """
    moment = ensure_utc(now) if now is not None else utcnow()
    settings = orchestrator_settings(cfg)
    cursor = {CURSOR_WATERMARK: moment.isoformat(), CURSOR_NOT_BEFORE: moment.isoformat()}
    with default_session_factory(cfg)() as session:
        store_repo.kv_set(session, store_repo.cursor_key(settings.instance), cursor)
    return moment


def set_watermark(cfg: HealerConfig, value: datetime) -> None:
    """Persist the watermark of the configured instance's poll cursor (its floor is kept)."""
    moment = ensure_utc(value) or utcnow()
    settings = orchestrator_settings(cfg)
    with default_session_factory(cfg)() as session:
        cursor = _read_cursor(session, cfg, instance=settings.instance, kind=settings.type) or {}
        cursor[CURSOR_WATERMARK] = moment.isoformat()
        cursor.setdefault(CURSOR_NOT_BEFORE, None)
        store_repo.kv_set(session, store_repo.cursor_key(settings.instance), cursor)


# =========================================================================== reconcile


@dataclass
class ReconcileStats:
    """What one :func:`reconcile` pass did.

    ``since`` is the start of the poll window when the feed reports one (the Airflow REST
    poll); ``queued`` counts the pending events stored from the adapter's hint queue.
    """

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
    queued: int = 0


def _record_new_incident(
    session_factory: SessionFactory,
    stats: ReconcileStats,
    *,
    ref: AttemptRef,
    source: str,
    error_summary: str,
    signature: str | None,
    now: datetime,
    native_ref: Mapping[str, Any] | None = None,
    error_name: str | None = None,
    note: str | None = None,
    pending_event_id: int | None = None,
    confirm_event: bool = True,
) -> None:
    """Create the incident of a confirmed failure (masked summary) and count the outcome.

    ``note`` becomes a ``confirmed`` timeline event of a new incident; ``pending_event_id``
    is the pushed event behind it, marked ``confirmed`` in the same transaction when
    ``confirm_event`` is set.
    """
    with session_factory() as session:
        incident, created = store_repo.create_incident(
            session,
            **ref.store_key(),
            source=source,
            error_summary=error_summary,
            signature=signature,
            now=now,
            native_ref=dict(native_ref or {}) or None,
            error_name=mask_text(error_name) if error_name else None,
        )
        if note and created:
            store_repo.add_event(
                session, incident, "confirmed", note, data={"pending_event_id": pending_event_id}
            )
        if pending_event_id is not None and confirm_event:
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
        ) != (ref.pipeline_id, ref.run_id, ref.step_id, int(ref.map_index), int(ref.attempt))
    if created:
        stats.created.append(incident_id)
        logger.info(
            "incident %s %s.%s: created from %s (run %s, try %s)",
            _short(incident_id),
            ref.pipeline_id,
            ref.step_id,
            source,
            ref.run_id,
            ref.attempt,
        )
    elif dedup:
        stats.folded += 1
        logger.info(
            "incident %s %s.%s: same failure seen again in run %s (try %s)",
            _short(incident_id),
            ref.pipeline_id,
            ref.step_id,
            ref.run_id,
            ref.attempt,
        )
    else:
        stats.known += 1


def _accepts_now(poll: Callable[..., Any]) -> bool:
    """True when a feed's ``poll`` takes ``now`` (third-party feeds may predate the keyword)."""
    try:
        parameters = inspect.signature(poll).parameters
    except (TypeError, ValueError):
        return False
    return "now" in parameters or any(p.kind is inspect.Parameter.VAR_KEYWORD for p in parameters.values())


def _window_start(feed: FailureFeed, cursor: Mapping[str, Any] | None, now: datetime) -> datetime | None:
    """Where the poll window starts, for feeds that tell (``poll_since``: the Airflow adapter)."""
    poll_since = getattr(feed, "poll_since", None)
    if not callable(poll_since):
        return None
    try:
        since = poll_since(cursor, now=now)
    except Exception:  # informational only
        return None
    return since if isinstance(since, datetime) else None


def _polled_log(adapter: OrchestratorAdapter, ref: AttemptRef) -> str:
    """The RAW log of a polled failure that came without an error text ("" when there is none).

    The incident is created anyway; the triage asks for the log again.
    """
    try:
        bundle = adapter.inspector.logs(ref, None)
    except Exception as exc:  # the port never raises; a broken adapter must not stop the poll
        text, reason = "", _cause_text(exc)
    else:
        text = bundle.text or ""
        reason = _clip("; ".join(bundle.notes), 500) or "no log"
        if bundle.source != "none":
            return text
    logger.info(
        "dag-healer worker: could not fetch the log of %s.%s (run %s, try %s): %s",
        ref.pipeline_id,
        ref.step_id,
        ref.run_id,
        ref.attempt,
        reason,
    )
    return text


def _reconcile_poll(
    cfg: HealerConfig,
    session_factory: SessionFactory,
    adapter: OrchestratorAdapter,
    stats: ReconcileStats,
    now: datetime,
) -> None:
    """List the failures of the adapter's feed after the cursor; create the incidents of new keys.

    Keys the store already has are skipped before anything else is asked of the orchestrator;
    a failure without an error text gets its summary and signature from the attempt's log (the
    phase-5 REST poll). The cursor advances only after a successful listing.
    """
    feed = adapter.feed
    if feed is None:
        return
    words = _vocabulary(adapter)
    key = store_repo.cursor_key(adapter.instance)
    with session_factory() as session:
        cursor = _read_cursor(session, cfg, instance=adapter.instance, kind=adapter.type)
    stats.since = _window_start(feed, cursor, now)

    def is_monitored(pipeline_id: str) -> bool:
        if cfg.is_monitored(pipeline_id):
            return True
        stats.unmonitored += 1
        return False

    extra: dict[str, Any] = {"now": now} if _accepts_now(feed.poll) else {}
    try:
        page = feed.poll(cursor=cursor, is_monitored=is_monitored, **extra)
    except Exception as exc:  # orchestrator errors: retried on the next poll, the cursor is kept
        message = _cause_text(exc)
        stats.errors.append(f"listing failed {words['step']} instances: {message}")
        logger.warning("dag-healer worker: listing failed %s instances failed: %s", words["step"], message)
        return
    stats.listed = len(page.failures) + stats.unmonitored
    every_failure = cfg.monitor.trigger == "every_failure"
    for failure in page.failures:
        ref = failure.ref
        if not (failure.is_final or every_failure) or not ref.resolved:
            continue
        with session_factory() as session:
            existing = store_repo.get_incident_by_key(session, **ref.store_key())
        if existing is not None:
            stats.known += 1
            continue
        text = failure.error_summary or _polled_log(adapter, ref)
        summary, signature = summarize_failure(ref.pipeline_id, ref.step_id, text)
        _record_new_incident(
            session_factory,
            stats,
            ref=ref,
            source=failure.source,
            error_summary=summary,
            signature=signature,
            now=now,
            native_ref=failure.native,
            error_name=failure.error_name,
        )
    with session_factory() as session:
        store_repo.kv_set(session, key, dict(page.cursor))


class _Rejected(Exception):
    """The store rejected the pending event of a queued hint (raised from the store's error)."""


def _store_queued(
    cfg: HealerConfig, session_factory: SessionFactory, channel: str, hints: Iterable[FailureHint]
) -> int:
    """Store the hints of one queued message in one transaction; how many were stored.

    Raises :class:`_Rejected` (the transaction is rolled back) when the store rejects one of
    them; any other error is the store's own failure.
    """
    stored = 0
    with session_factory() as session:
        for hint in hints:
            if not cfg.is_monitored(hint.ref.pipeline_id):
                continue
            try:
                store_repo.add_pending_event(
                    session,
                    **hint.ref.store_key(),
                    channel=channel,
                    payload={
                        "error": _masked_tail(hint.error_text),
                        "source": hint.source,
                        "via": VIA_QUEUE,
                    },
                    native_ref=dict(hint.native),
                    received_at=hint.received_at,
                )
            except _REJECTED_BY_STORE as exc:
                raise _Rejected(type(exc).__name__) from exc
            stored += 1
    return stored


def _drain_queue(
    cfg: HealerConfig, session_factory: SessionFactory, adapter: OrchestratorAdapter, stats: ReconcileStats
) -> None:
    """Store the hints waiting in the adapter's queue as pending events (``"via": "queue"``).

    Each message is stored in its own transaction; the messages are acknowledged once their
    events are committed. A message that cannot be kept is dropped and acknowledged too, so it
    never blocks the queue: one the push intake cannot read (malformed: ``ValueError``) and one
    whose hints the store rejects (an empty or too long id...). Hints of unmonitored pipelines
    are not stored. When the store itself fails, that message and the ones after it stay
    unacknowledged (the queue delivers them again) and the error is recorded in
    ``stats.errors``.
    """
    queue, push = adapter.queue, adapter.push
    if queue is None or push is None or not _wants(cfg, "queue"):
        return
    try:
        messages = list(queue.receive())
    except Exception as exc:
        message = _cause_text(exc)
        stats.errors.append(f"receiving queued events: {message}")
        logger.warning("dag-healer worker: receiving queued events failed: %s", message)
        return
    done: list[str] = []
    for item in messages:
        try:
            hints = push.normalize(item.channel, item.payload, {})
        except Exception as exc:  # malformed (ValueError) or unreadable: a hint is never the truth
            logger.warning(
                "dag-healer worker: dropping a queued %s event that cannot be read: %s",
                _clip(item.channel, 64),
                _clip(exc if isinstance(exc, ValueError) else _error_text(exc), 300),
            )
            done.append(item.receipt)
            continue
        try:
            stats.queued += _store_queued(cfg, session_factory, item.channel, hints)
        except _Rejected as exc:  # this message only: dropped like an unreadable one
            logger.warning(
                "dag-healer worker: dropping a queued %s event the store cannot keep: %s",
                _clip(item.channel, 64),
                _error_text(exc.__cause__ or exc),
            )
        except Exception as exc:  # the store failed: nothing more is acknowledged, delivered again
            message = _cause_text(exc)
            stats.errors.append(f"storing queued events: {message}")
            logger.warning("dag-healer worker: storing queued events failed: %s", message)
            break
        done.append(item.receipt)
    if not done:
        return
    try:
        queue.ack(done)
    except Exception as exc:  # stored already; a redelivery joins the pending event
        message = _cause_text(exc)
        stats.errors.append(f"acknowledging queued events: {message}")
        logger.warning("dag-healer worker: acknowledging queued events failed: %s", message)


def _discard(session_factory: SessionFactory, event_id: int, reason: str, stats: ReconcileStats) -> None:
    with session_factory() as session:
        event = session.get(PendingEvent, event_id)
        if event is not None:
            store_repo.mark_pending_event(session, event, PENDING_DISCARDED)
    stats.discarded += 1
    logger.info("dag-healer worker: discarded pending event %s: %s", event_id, _clip(reason, 300))


def _push_sources(adapter: OrchestratorAdapter | None) -> tuple[str, ...]:
    """The sources a pushed event may carry: the adapter's push sources (the first is the default)."""
    if adapter is None:
        return EVENT_SOURCES
    sources = tuple(source for source in adapter.capabilities.sources if source in PUSH_SOURCES)
    return sources or EVENT_SOURCES


def _reconcile_pending(
    cfg: HealerConfig,
    session_factory: SessionFactory,
    adapter: OrchestratorAdapter | None,
    stats: ReconcileStats,
    now: datetime,
) -> None:
    """Confirm the pending events of this instance (oldest first) through ``adapter.push.confirm``.

    The core decides first: an event whose source (or queue) is not enabled in
    ``monitor.modes`` and an event of an unmonitored pipeline are discarded; without an
    adapter (or a push intake) events stay pending. The adapter then confirms, discards or
    defers each hint; a deferred one is discarded once it is older than
    :data:`PENDING_MAX_AGE`. A deferral is reported in ``stats.errors`` unless the attempt has
    not ended yet (``state`` ``RUNNING``: the hint arrived before the orchestrator recorded the
    end of the attempt). A confirmed hint creates one incident per confirmed failure, its
    summary and signature taken from the pushed error text.
    """
    instance = _instance(cfg, adapter)
    words = _vocabulary(adapter)
    sources = _push_sources(adapter)
    push = adapter.push if adapter is not None else None
    with session_factory() as session:
        events = [event.to_dict() for event in store_repo.list_pending_events(session, orchestrator=instance)]
    for event in events:
        event_id = int(event["id"])
        payload = event.get("payload") if isinstance(event.get("payload"), Mapping) else {}
        source = str(payload.get("source") or sources[0])
        if source not in sources:
            source = sources[0]
        queued = payload.get("via") == VIA_QUEUE
        if not _wants(cfg, VIA_QUEUE if queued else source):
            mode = VIA_QUEUE if queued else source
            _discard(session_factory, event_id, f"{mode} events are not enabled in monitor.modes", stats)
            continue
        pipeline_id = str(event["dag_id"])
        if not cfg.is_monitored(pipeline_id):
            _discard(session_factory, event_id, f"{words['pipeline']} {pipeline_id} is not monitored", stats)
            continue
        if push is None:
            stats.pending_left += 1
            continue
        received = (
            ensure_utc(datetime.fromisoformat(event["received_at"])) if event.get("received_at") else now
        )
        hint = FailureHint(
            ref=AttemptRef.from_row(event, default_orchestrator=instance),
            channel=str(event.get("channel") or DEFAULT_CHANNEL),
            source=source,
            error_text=str(payload.get("error") or ""),
            received_at=received,
            native=dict(event.get("native_ref") or {}),
        )
        try:
            confirmation = push.confirm(hint, trigger=cfg.monitor.trigger)
        except Exception as exc:  # the port does not raise; a broken adapter: try again later
            confirmation = Confirmation(verdict="retry_later", reason=_cause_text(exc))
        if confirmation.verdict == "discarded":
            _discard(session_factory, event_id, confirmation.reason, stats)
            continue
        if confirmation.verdict != "confirmed":
            if received is not None and now - received > PENDING_MAX_AGE:
                _discard(session_factory, event_id, f"unconfirmed after {PENDING_MAX_AGE}", stats)
                continue
            reason = _clip(confirmation.reason, 500)
            stats.pending_left += 1
            if getattr(confirmation, "state", None) is AttemptState.RUNNING:  # not ended yet: no error
                logger.info(
                    "dag-healer worker: pending event %s is checked again later: %s", event_id, reason
                )
            else:
                stats.errors.append(f"confirming pending event {event_id}: {reason}")
                logger.warning("dag-healer worker: could not confirm pending event %s: %s", event_id, reason)
            continue
        failures = [failure for failure in confirmation.failures if failure.ref.resolved]
        if not failures:
            _discard(session_factory, event_id, "the confirmation named no failed step", stats)
            continue
        note = _clip(
            f"The {source} event was confirmed by the {words['product']} API ({confirmation.reason})."
        )
        for number, failure in enumerate(failures, start=1):
            ref = failure.ref
            summary, signature = summarize_failure(ref.pipeline_id, ref.step_id, failure.error_summary)
            _record_new_incident(
                session_factory,
                stats,
                ref=ref,
                source=failure.source,
                error_summary=summary,
                signature=signature,
                now=now,
                native_ref=failure.native,
                error_name=failure.error_name,
                note=note,
                pending_event_id=event_id,
                confirm_event=number == len(failures),
            )
        stats.confirmed += 1


def reconcile(
    cfg: HealerConfig,
    session_factory: SessionFactory | None,
    client: Any | None,
    *,
    now: datetime | None = None,
    adapter: OrchestratorAdapter | None = None,
) -> ReconcileStats:
    """Create the incidents of new failures (poll, queue and pushed events); see the module docstring.

    ``adapter`` wins; otherwise ``client`` (a phase-5 Airflow REST client or a test double) is
    wrapped by :func:`~dag_healer.orchestrators.compat.as_adapter`. Without either the poll is
    skipped and pending events stay pending. Everything is scoped to the orchestrator instance.
    Never raises for orchestrator errors (they are logged and retried next time).
    """
    moment = _now(now)
    factory = session_factory or default_session_factory(cfg)
    stats = ReconcileStats()
    resolved = adapter if adapter is not None else as_adapter(cfg, client)
    words = _vocabulary(resolved)
    if _wants(cfg, "poll"):
        if resolved is None:
            stats.errors.append(f"no {words['product']} API client: the REST poll was skipped")
        else:
            _reconcile_poll(cfg, factory, resolved, stats, moment)
    if resolved is not None:
        _drain_queue(cfg, factory, resolved, stats)
    _reconcile_pending(cfg, factory, resolved, stats, moment)
    if stats.created or stats.folded or stats.confirmed or stats.discarded or stats.queued:
        logger.info(
            "dag-healer worker: reconcile listed %d failed %s instances, created %d incidents, "
            "folded %d repeated failures, confirmed %d and discarded %d pushed events",
            stats.listed,
            words["step"],
            len(stats.created),
            stats.folded,
            stats.confirmed,
            stats.discarded,
        )
    return stats


def apply_commands(
    cfg: HealerConfig,
    session_factory: SessionFactory | None,
    client: Any | None,
    *,
    now: datetime | None = None,
    adapter: OrchestratorAdapter | None = None,
) -> list[CommandOutcome]:
    """Apply the pending human commands (see :func:`dag_healer.worker.commands.apply_commands`)."""
    from dag_healer.worker.commands import apply_commands as _apply_commands

    return _apply_commands(cfg, session_factory, client, now=now, adapter=adapter)


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
        client: Any | None,
        adapter: OrchestratorAdapter | None,
        notifier: Notifier | None,
        workspace_root: Path | None,
        now: datetime | None,
    ) -> None:
        self.cfg = cfg
        self.sf = session_factory
        self.incident_id = incident_id
        self.provider = provider
        self.client = client
        self.adapter = adapter
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
        ctx = gather_context(self.cfg, snapshot, self.adapter)
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
        taxonomy = self.adapter.taxonomy if self.adapter is not None else None
        outcome = classify_incident(self.cfg, ctx, self.provider, taxonomy=taxonomy)
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
                step = _vocabulary(self.adapter)["step"]
                error = ctx.exception_type or f"the {step} error"
                incident.diagnosis = _clip(
                    f"{actor} resolved this incident as a {forced} problem. The {step} failed with {error}"
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

    def _orchestrator_extra(self, snapshot: Mapping[str, Any]) -> dict[str, Any]:
        """The alert keys of the orchestrator: its words and the links into its own UI.

        ``vocabulary`` is the adapter's (``{}`` offline: the Airflow words); ``native_links``
        are the ones the Healer views show (``{}`` for Airflow and offline).
        """
        adapter = self.adapter
        if adapter is None:
            return {"vocabulary": {}, "native_links": {}}
        try:
            vocabulary = dict(adapter.vocabulary.as_dict())
        except Exception:  # a broken vocabulary: the alert keeps the Airflow words
            vocabulary = {}
        try:
            native = dict(links.shown_native_links(self.cfg, snapshot, adapter=adapter) or {})
        except Exception as exc:  # links are a convenience: the alert goes out without them
            logger.debug("dag-healer worker: no native links for the alert: %s", _error_text(exc))
            native = {}
        return {"vocabulary": vocabulary, "native_links": native}

    def _alert(self, event: str, extra: Mapping[str, Any]) -> None:
        """Send the alert of ``event`` (outside any transaction) and record the outcomes."""
        from dag_healer.alerts import notify

        self.stage = f"sending the {event} alert"
        snapshot = self._snapshot()
        notifier = self.notifier or notify
        details = {**dict(extra), **self._orchestrator_extra(snapshot)}
        try:
            outcomes = list(notifier(self.cfg, snapshot, event, extra=details))
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
            result = run_repair(
                self.cfg,
                snapshot,
                ctx,
                self.provider,
                workspace_root=self.workspace_root,
                extra_env=self._sandbox_env(),
            )
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

    def _sandbox_env(self) -> dict[str, str | None] | None:
        """The adapter's overrides of the pytest sandbox (``None`` offline or when they fail)."""
        if self.adapter is None:
            return None
        try:
            return dict(self.adapter.sandbox_env())
        except Exception as exc:  # the core's own sandbox rules still apply
            logger.warning(
                "dag-healer worker: the pytest sandbox settings of the %s adapter failed: %s",
                self.adapter.type,
                _error_text(exc),
            )
            return None

    # ------------------------------------------------------------------ driver
    def run(self) -> str:
        snapshot = self._start()
        if snapshot is None:
            return self._current_status()
        if self.adapter is None:
            self.adapter = as_adapter(self.cfg, self.client)
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
    client: Any | None = None,
    adapter: OrchestratorAdapter | None = None,
    notifier: Notifier | None = None,
    workspace_root: Path | None = None,
    now: datetime | None = None,
) -> str:
    """Triage one incident end to end and return its final status (see the module docstring).

    The incident is normally claimed (``triaging``) by the caller; a ``new`` or ``resumed``
    incident is claimed here. An incident in any other status is left alone. A ``resolution``
    of ``data`` or ``code`` (set by a human command) skips the classification. ``adapter``
    wins; otherwise ``client`` (a phase-5 Airflow REST client or a test double) is wrapped by
    :func:`~dag_healer.orchestrators.compat.as_adapter`, and neither means an offline triage:
    no orchestrator is asked anything, and only the classification reads the error taxonomy
    of the adapter configured in ``cfg`` (built without I/O and cached, section 3.9).
    ``notifier`` replaces :func:`dag_healer.alerts.notify` (tests). Never raises.
    """
    factory = session_factory or default_session_factory(cfg)
    pipeline = _Pipeline(
        cfg,
        factory,
        incident_id,
        provider=provider,
        client=client,
        adapter=adapter,
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
