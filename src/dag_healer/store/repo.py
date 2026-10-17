"""Data-access functions for the dag-healer store.

Every function takes an open :class:`~sqlalchemy.orm.Session` and never commits:
the caller owns the transaction (usually through
:func:`dag_healer.store.db.session_scope`).
"""

from __future__ import annotations

import logging
from collections.abc import Iterable, Sequence
from datetime import datetime, timedelta
from typing import Any

from sqlalchemy import func, select, update
from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import Session

from dag_healer.store.models import (
    ALL_STATUSES,
    CLASSIFIED,
    COMMAND_KINDS,
    COMMAND_PENDING,
    COMMAND_STATUSES,
    NEW,
    OPEN_STATUSES,
    PENDING_CHECK,
    PENDING_EVENT_STATUSES,
    REPAIRING,
    REPORTING,
    RESUMED,
    SOURCES,
    TRIAGING,
    Alert,
    Command,
    ContractState,
    Incident,
    IncidentEvent,
    PendingEvent,
    ensure_utc,
    new_id,
    utcnow,
)

logger = logging.getLogger(__name__)

DEFAULT_ACTOR = "dag-healer"
#: Window in which a repeated failure signature is folded into an open incident.
DEDUPE_WINDOW = timedelta(hours=24)
#: Statuses a crashed worker can leave behind (see :func:`release_stale_claims`).
WORKING_STATUSES: tuple[str, ...] = (TRIAGING, CLASSIFIED, REPORTING, REPAIRING)

_TI_KEY_COLUMNS: tuple[str, ...] = ("dag_id", "run_id", "task_id", "map_index", "try_number")
_INCIDENT_EXTRA_COLUMNS = frozenset(Incident.__table__.columns.keys()) - {
    *_TI_KEY_COLUMNS,
    "source",
    "error_summary",
    "signature",
}
_PENDING_EVENT_COLUMNS = frozenset(PendingEvent.__table__.columns.keys()) - {"id"}


def _now(now: datetime | None) -> datetime:
    return ensure_utc(now) or utcnow()


def _ti_key(dag_id: str, run_id: str, task_id: str, map_index: int, try_number: int) -> dict:
    return {
        "dag_id": dag_id,
        "run_id": run_id,
        "task_id": task_id,
        "map_index": int(map_index),
        "try_number": int(try_number),
    }


# --------------------------------------------------------------------------- #
# Incidents
# --------------------------------------------------------------------------- #
def get_incident(session: Session, incident_id: str) -> Incident | None:
    """Return the incident with ``incident_id`` or ``None``."""
    return session.get(Incident, incident_id)


def get_incident_by_key(
    session: Session,
    *,
    dag_id: str,
    run_id: str,
    task_id: str,
    map_index: int = -1,
    try_number: int,
) -> Incident | None:
    """Return the incident recorded for exactly this task instance try, if any."""
    key = _ti_key(dag_id, run_id, task_id, map_index, try_number)
    query = select(Incident).filter_by(**key)
    return session.scalars(query).first()


def _find_open_duplicate(
    session: Session, *, dag_id: str, task_id: str, signature: str, since: datetime
) -> Incident | None:
    query = (
        select(Incident)
        .where(
            Incident.dag_id == dag_id,
            Incident.task_id == task_id,
            Incident.signature == signature,
            Incident.status.in_(OPEN_STATUSES),
            Incident.created_at >= since,
        )
        .order_by(Incident.created_at.desc(), Incident.id.desc())
    )
    return session.scalars(query).first()


def _record_occurrence(session: Session, incident: Incident, key: dict, source: str) -> None:
    """Fold another failure with the same signature into ``incident`` (idempotent per key)."""
    query = select(IncidentEvent.data).where(
        IncidentEvent.incident_id == incident.id, IncidentEvent.kind == "occurrence"
    )
    for data in session.scalars(query):
        if isinstance(data, dict) and all(data.get(k) == v for k, v in key.items()):
            return
    incident.occurrences = (incident.occurrences or 1) + 1
    add_event(
        session,
        incident,
        "occurrence",
        f"Same failure seen again in run {key['run_id']} (try {key['try_number']}, "
        f"map_index {key['map_index']}); occurrences={incident.occurrences}",
        data={**key, "source": source},
    )


def _insert_incident(session: Session, values: dict[str, Any]) -> Incident | None:
    """Insert an incident row unless its task instance key exists; ``None`` on conflict.

    SQLite and PostgreSQL use ``INSERT ... ON CONFLICT DO NOTHING``. SAVEPOINTs
    are avoided on purpose: with the default pysqlite transaction handling a
    SAVEPOINT that opens the transaction commits it on release, which would
    defeat the caller's rollback.
    """
    session.flush()
    dialect = session.get_bind().dialect.name
    if dialect in ("sqlite", "postgresql"):
        if dialect == "sqlite":
            from sqlalchemy.dialects.sqlite import insert as dialect_insert
        else:
            from sqlalchemy.dialects.postgresql import insert as dialect_insert
        statement = (
            dialect_insert(Incident.__table__)
            .values(**values)
            .on_conflict_do_nothing(index_elements=list(_TI_KEY_COLUMNS))
        )
        if session.execute(statement).rowcount != 1:
            return None
        return session.get(Incident, values["id"])

    incident = Incident(**values)
    try:
        with session.begin_nested():
            session.add(incident)
    except IntegrityError:
        return None
    return incident


def create_incident(
    session: Session,
    *,
    dag_id: str,
    run_id: str,
    task_id: str,
    map_index: int = -1,
    try_number: int,
    source: str,
    error_summary: str = "",
    signature: str | None = None,
    now: datetime | None = None,
    **extra: Any,
) -> tuple[Incident, bool]:
    """Create the incident for a failed task instance try.

    Returns ``(incident, created)``. The call is idempotent on the
    ``(dag_id, run_id, task_id, map_index, try_number)`` key. When ``signature``
    is given and an open incident with the same dag, task and signature was
    created less than 24 hours before ``now``, that incident's ``occurrences``
    counter is incremented instead (once per task instance try) and it is
    returned with ``created=False``. ``extra`` sets any other Incident column
    (for example ``created_at`` or ``status``).
    """
    if source not in SOURCES:
        raise ValueError(f"Unknown incident source {source!r}; expected one of {SOURCES}")
    unknown = set(extra) - _INCIDENT_EXTRA_COLUMNS
    if unknown:
        raise TypeError(f"Unexpected Incident fields: {', '.join(sorted(unknown))}")
    moment = _now(now)
    key = _ti_key(dag_id, run_id, task_id, map_index, try_number)

    existing = get_incident_by_key(session, **key)
    if existing is not None:
        return existing, False

    if signature:
        duplicate = _find_open_duplicate(
            session,
            dag_id=dag_id,
            task_id=task_id,
            signature=signature,
            since=moment - DEDUPE_WINDOW,
        )
        if duplicate is not None:
            _record_occurrence(session, duplicate, key, source)
            return duplicate, False

    values: dict[str, Any] = {
        **key,
        "id": new_id(),
        "status": NEW,
        "source": source,
        "error_summary": error_summary or "",
        "signature": signature,
        "created_at": moment,
        "updated_at": moment,
        **extra,
    }
    incident = _insert_incident(session, values)
    if incident is None:
        # Another writer inserted the same task instance try concurrently.
        existing = get_incident_by_key(session, **key)
        if existing is None:  # pragma: no cover - conflict without a visible row
            raise RuntimeError(f"Could not create or find the incident for {key}")
        return existing, False

    add_event(
        session,
        incident,
        "created",
        f"Incident created from {source} for {dag_id}.{task_id} "
        f"(run {run_id}, try {try_number}, map_index {map_index})",
        data={**key, "source": source},
    )
    return incident, True


def list_incidents(
    session: Session,
    *,
    dag_ids: set[str] | None = None,
    statuses: Sequence[str] | None = None,
    limit: int = 100,
    run_id: str | None = None,
    task_id: str | None = None,
) -> list[Incident]:
    """Return incidents newest first.

    ``dag_ids=None`` means no DAG filter; an empty set returns nothing (useful
    when the caller may read no DAG at all).
    """
    if dag_ids is not None and not dag_ids:
        return []
    query = select(Incident)
    if dag_ids is not None:
        query = query.where(Incident.dag_id.in_(sorted(dag_ids)))
    if statuses:
        query = query.where(Incident.status.in_(list(statuses)))
    if run_id is not None:
        query = query.where(Incident.run_id == run_id)
    if task_id is not None:
        query = query.where(Incident.task_id == task_id)
    query = query.order_by(Incident.created_at.desc(), Incident.id.desc()).limit(max(1, limit))
    return list(session.scalars(query))


def try_claim(
    session: Session,
    incident_id: str,
    statuses: Sequence[str] = (NEW, RESUMED),
    *,
    now: datetime | None = None,
    actor: str = DEFAULT_ACTOR,
) -> Incident | None:
    """Claim one specific incident if it is still in ``statuses``; ``None`` if it is not.

    Uses a conditional ``UPDATE ... WHERE id = :id AND status IN (...)`` and
    checks the affected row count, so concurrent claimers cannot both win.
    """
    wanted = list(statuses)
    moment = _now(now)
    previous = session.scalar(select(Incident.status).where(Incident.id == incident_id))
    result = session.execute(
        update(Incident)
        .where(Incident.id == incident_id, Incident.status.in_(wanted))
        .values(status=TRIAGING, claimed_at=moment, updated_at=moment)
        .execution_options(synchronize_session=False)
    )
    if result.rowcount != 1:
        return None
    incident = session.get(Incident, incident_id, populate_existing=True)
    if incident is None:  # pragma: no cover - deleted concurrently
        return None
    add_event(
        session,
        incident,
        "status",
        "Claimed by the worker for triage",
        data={"from": previous if previous in wanted else None, "to": TRIAGING},
        actor=actor,
    )
    return incident


def claim_next(
    session: Session,
    statuses: Sequence[str] = (NEW, RESUMED),
    *,
    now: datetime | None = None,
    actor: str = DEFAULT_ACTOR,
) -> Incident | None:
    """Atomically claim the oldest incident in ``statuses`` and mark it ``triaging``.

    The claim is a conditional ``UPDATE`` (see :func:`try_claim`), so two
    workers (even on different engines or hosts) never claim the same row; on
    PostgreSQL the candidate is also selected ``FOR UPDATE SKIP LOCKED``.
    Commit promptly after claiming: on SQLite the open write transaction blocks
    other writers until it ends.
    """
    query = (
        select(Incident.id)
        .where(Incident.status.in_(list(statuses)))
        .order_by(Incident.created_at, Incident.id)
    )
    if session.get_bind().dialect.name == "postgresql":
        # Workers skip rows another worker is claiming instead of queueing behind it.
        query = query.limit(1).with_for_update(skip_locked=True)
    else:
        query = query.limit(25)
    for incident_id in session.scalars(query).all():
        incident = try_claim(session, incident_id, statuses, now=now, actor=actor)
        if incident is not None:
            return incident
    return None


def release_stale_claims(
    session: Session,
    *,
    older_than: timedelta,
    statuses: Sequence[str] = WORKING_STATUSES,
    now: datetime | None = None,
) -> list[Incident]:
    """Put incidents stuck in a working status (crashed worker) back to ``new``."""
    cutoff = _now(now) - older_than
    query = select(Incident).where(
        Incident.status.in_(list(statuses)),
        Incident.claimed_at.is_not(None),
        Incident.claimed_at < cutoff,
    )
    released = list(session.scalars(query))
    for incident in released:
        set_status(
            session,
            incident,
            NEW,
            f"Claim expired while {incident.status}; queued for triage again",
        )
        incident.claimed_at = None
    return released


def set_status(
    session: Session,
    incident: Incident,
    status: str,
    message: str,
    *,
    actor: str = DEFAULT_ACTOR,
    data: dict[str, Any] | None = None,
) -> IncidentEvent:
    """Change the incident status and record a ``status`` timeline event."""
    if status not in ALL_STATUSES:
        raise ValueError(f"Unknown incident status {status!r}")
    previous = incident.status
    incident.status = status
    incident.updated_at = utcnow()
    payload = {"from": previous, "to": status}
    if data:
        payload.update(data)
    return add_event(session, incident, "status", message, data=payload, actor=actor)


def add_event(
    session: Session,
    incident: Incident,
    kind: str,
    message: str,
    *,
    data: dict[str, Any] | None = None,
    actor: str = DEFAULT_ACTOR,
) -> IncidentEvent:
    """Append an event to the incident timeline."""
    event = IncidentEvent(
        incident=incident,
        kind=kind,
        message=message or "",
        data=data,
        actor=actor or DEFAULT_ACTOR,
        at=utcnow(),
    )
    session.add(event)
    return event


def list_events(session: Session, incident_id: str) -> list[IncidentEvent]:
    """Return the timeline of an incident, oldest first."""
    query = select(IncidentEvent).where(IncidentEvent.incident_id == incident_id).order_by(IncidentEvent.id)
    return list(session.scalars(query))


def incidents_created_since(session: Session, since: datetime) -> int:
    """Return how many incidents were created at or after ``since``."""
    query = select(func.count()).select_from(Incident).where(Incident.created_at >= since)
    return int(session.scalar(query) or 0)


# --------------------------------------------------------------------------- #
# Commands
# --------------------------------------------------------------------------- #
def enqueue_command(
    session: Session,
    *,
    kind: str,
    dag_id: str,
    incident_id: str | None = None,
    payload: dict[str, Any] | None = None,
    actor: str,
) -> Command:
    """Queue a human action for the worker to apply."""
    if kind not in COMMAND_KINDS:
        raise ValueError(f"Unknown command kind {kind!r}; expected one of {COMMAND_KINDS}")
    if not actor:
        raise ValueError("A command needs an actor")
    command = Command(
        kind=kind,
        dag_id=dag_id,
        incident_id=incident_id,
        payload=payload or {},
        actor=actor,
        status=COMMAND_PENDING,
        created_at=utcnow(),
    )
    session.add(command)
    session.flush()
    return command


def pending_commands(session: Session, *, limit: int = 100) -> list[Command]:
    """Return pending commands in the order they were queued."""
    query = select(Command).where(Command.status == COMMAND_PENDING).order_by(Command.id).limit(max(1, limit))
    return list(session.scalars(query))


def mark_command(session: Session, command: Command, status: str, error: str | None = None) -> Command:
    """Record the outcome of a command (``applied`` or ``failed``)."""
    if status not in COMMAND_STATUSES:
        raise ValueError(f"Unknown command status {status!r}")
    command.status = status
    command.error = error
    command.applied_at = None if status == COMMAND_PENDING else utcnow()
    session.add(command)
    return command


# --------------------------------------------------------------------------- #
# Contracts
# --------------------------------------------------------------------------- #
def upsert_contract_state(
    session: Session,
    *,
    dag_id: str,
    status: str,
    path: str,
    approved_by: str | None = None,
    approved_at: datetime | None = None,
    source_hash: str | None = None,
) -> ContractState:
    """Insert or update the cached state of a DAG contract."""
    state = session.get(ContractState, dag_id)
    if state is None:
        state = ContractState(dag_id=dag_id)
        session.add(state)
    state.status = status
    state.path = str(path)
    state.approved_by = approved_by
    state.approved_at = ensure_utc(approved_at)
    state.source_hash = source_hash
    state.updated_at = utcnow()
    session.flush()
    return state


def get_contract_state(session: Session, dag_id: str) -> ContractState | None:
    """Return the cached contract state of ``dag_id``."""
    return session.get(ContractState, dag_id)


def list_contract_states(session: Session) -> list[ContractState]:
    """Return every cached contract state ordered by DAG id."""
    return list(session.scalars(select(ContractState).order_by(ContractState.dag_id)))


# --------------------------------------------------------------------------- #
# Alerts
# --------------------------------------------------------------------------- #
def record_alert(
    session: Session,
    *,
    incident_id: str | None,
    channel: str,
    recipients: Iterable[str] | None,
    subject: str,
    status: str,
    error: str | None = None,
) -> Alert:
    """Record a notification attempt."""
    alert = Alert(
        incident_id=incident_id,
        channel=channel,
        recipients=list(recipients or []),
        subject=subject or "",
        status=status,
        error=error,
        sent_at=utcnow(),
    )
    session.add(alert)
    session.flush()
    return alert


def list_alerts(session: Session, incident_id: str) -> list[Alert]:
    """Return the alerts recorded for an incident, oldest first."""
    query = select(Alert).where(Alert.incident_id == incident_id).order_by(Alert.id)
    return list(session.scalars(query))


# --------------------------------------------------------------------------- #
# Pending events (listener / callback deliveries awaiting REST confirmation)
# --------------------------------------------------------------------------- #
def add_pending_event(session: Session, **fields: Any) -> PendingEvent:
    """Store a pushed failure event as ``pending_check``.

    Accepts the ingest payload directly: ``dag_id``, ``run_id``, ``task_id``,
    ``map_index``, ``try_number`` and ``payload`` map to columns; any other key
    is merged into ``payload``. A second delivery of the same task instance try
    while the first is still pending returns the existing row.
    """
    columns = {k: v for k, v in fields.items() if k in _PENDING_EVENT_COLUMNS}
    extras = {k: v for k, v in fields.items() if k not in _PENDING_EVENT_COLUMNS}
    missing = [k for k in ("dag_id", "run_id", "task_id") if not columns.get(k)]
    if missing:
        raise ValueError(f"Pending event is missing {', '.join(missing)}")
    payload = dict(columns.pop("payload", None) or {})
    payload.update(extras)
    map_index = columns.pop("map_index", None)
    try_number = columns.pop("try_number", None)
    key = _ti_key(
        str(columns.pop("dag_id")),
        str(columns.pop("run_id")),
        str(columns.pop("task_id")),
        -1 if map_index is None else int(map_index),
        1 if try_number is None else int(try_number),
    )
    status = columns.pop("status", None) or PENDING_CHECK
    if status not in PENDING_EVENT_STATUSES:
        raise ValueError(f"Unknown pending event status {status!r}")

    if status == PENDING_CHECK:
        query = select(PendingEvent).filter_by(**key, status=PENDING_CHECK)
        existing = session.scalars(query.order_by(PendingEvent.id)).first()
        if existing is not None:
            return existing

    received_at = ensure_utc(columns.pop("received_at", None)) or utcnow()
    event = PendingEvent(**key, payload=payload, status=status, received_at=received_at)
    session.add(event)
    session.flush()
    return event


def list_pending_events(
    session: Session, status: str = PENDING_CHECK, *, limit: int = 500
) -> list[PendingEvent]:
    """Return pending events with ``status``, oldest first."""
    query = (
        select(PendingEvent)
        .where(PendingEvent.status == status)
        .order_by(PendingEvent.id)
        .limit(max(1, limit))
    )
    return list(session.scalars(query))


def mark_pending_event(session: Session, event: PendingEvent, status: str) -> PendingEvent:
    """Set the status of a pending event (``confirmed`` or ``discarded``)."""
    if status not in PENDING_EVENT_STATUSES:
        raise ValueError(f"Unknown pending event status {status!r}")
    event.status = status
    session.add(event)
    return event
