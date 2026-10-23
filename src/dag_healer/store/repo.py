"""Data-access functions for the dag-healer store.

Every function takes an open :class:`~sqlalchemy.orm.Session` and never commits:
the caller owns the transaction (usually through
:func:`dag_healer.store.db.session_scope`).

Store schema v2 keys incidents by ``(orchestrator, dag_id, run_id, task_id, map_index,
try_number)``. ``orchestrator`` is the instance id from ``healer.yaml`` and defaults to
``"airflow"`` everywhere, so every phase-5 call keeps working unchanged; the read helpers
take ``orchestrator=None`` (every instance) unless they look up one key.
"""

from __future__ import annotations

import json
import logging
from collections.abc import Iterable, Mapping, Sequence
from datetime import datetime, timedelta
from os import PathLike
from pathlib import Path
from typing import TYPE_CHECKING, Any

from sqlalchemy import delete, func, select, update
from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import Session

from dag_healer.core.ids import DEFAULT_ORCHESTRATOR, UNRESOLVED_STEP
from dag_healer.masking import mask_text
from dag_healer.store.models import (
    ALL_STATUSES,
    CLASSIFIED,
    COMMAND_KINDS,
    COMMAND_PENDING,
    COMMAND_STATUSES,
    DEFAULT_CHANNEL,
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
    KeyValue,
    PendingEvent,
    ensure_utc,
    json_safe,
    new_id,
    utcnow,
)

if TYPE_CHECKING:
    from dag_healer.core.failures import Failure

logger = logging.getLogger(__name__)

DEFAULT_ACTOR = "dag-healer"
#: Window in which a repeated failure signature is folded into an open incident.
DEDUPE_WINDOW = timedelta(hours=24)
#: Statuses a crashed worker can leave behind (see :func:`release_stale_claims`).
WORKING_STATUSES: tuple[str, ...] = (TRIAGING, CLASSIFIED, REPORTING, REPAIRING)
#: ``healer_kv`` key prefix of the poll cursor of one orchestrator instance.
CURSOR_KEY_PREFIX = "cursor:"
#: Keys of the phase-5 worker state file ``<healer_home>/state.json`` (Airflow REST poll).
STATE_WATERMARK_KEY = "rest_watermark"
STATE_NOT_BEFORE_KEY = "rest_not_before"
#: Length of ``Incident.error_name``.
MAX_ERROR_NAME_CHARS = 256

#: The incident key (``uq_healer_incidents_key``).
_TI_KEY_COLUMNS: tuple[str, ...] = ("orchestrator", "dag_id", "run_id", "task_id", "map_index", "try_number")
#: The key as recorded in timeline event data (phase-5 shape; the incident has the instance).
_EVENT_KEY_COLUMNS: tuple[str, ...] = ("dag_id", "run_id", "task_id", "map_index", "try_number")
_INCIDENT_EXTRA_COLUMNS = frozenset(Incident.__table__.columns.keys()) - {
    *_TI_KEY_COLUMNS,
    "source",
    "error_summary",
    "signature",
    "native_ref",
    "error_name",
}
_PENDING_EVENT_COLUMNS = frozenset(PendingEvent.__table__.columns.keys()) - {"id"}


def _now(now: datetime | None) -> datetime:
    return ensure_utc(now) or utcnow()


def _instance(orchestrator: str | None) -> str:
    """The instance id to store (``None`` / empty: :data:`DEFAULT_ORCHESTRATOR`)."""
    return str(orchestrator) if orchestrator else DEFAULT_ORCHESTRATOR


def _ti_key(
    dag_id: str,
    run_id: str,
    task_id: str,
    map_index: int,
    try_number: int,
    orchestrator: str | None = DEFAULT_ORCHESTRATOR,
) -> dict[str, Any]:
    return {
        "orchestrator": _instance(orchestrator),
        "dag_id": dag_id,
        "run_id": run_id,
        "task_id": task_id,
        "map_index": int(map_index),
        "try_number": int(try_number),
    }


def _event_key(key: Mapping[str, Any]) -> dict[str, Any]:
    return {name: key[name] for name in _EVENT_KEY_COLUMNS}


def _native(value: Any) -> Any:
    """``native_ref`` as stored: JSON-safe, ``None`` for nothing. Never masked (adapter pointers)."""
    if value is None:
        return None
    if isinstance(value, Mapping):
        return json_safe(dict(value)) if value else None
    return json_safe(value)


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
    orchestrator: str = DEFAULT_ORCHESTRATOR,
) -> Incident | None:
    """Return the incident recorded for exactly this attempt (task instance try), if any."""
    key = _ti_key(dag_id, run_id, task_id, map_index, try_number, orchestrator)
    query = select(Incident).filter_by(**key)
    return session.scalars(query).first()


def _find_open_duplicate(
    session: Session, *, orchestrator: str, dag_id: str, task_id: str, signature: str, since: datetime
) -> Incident | None:
    query = (
        select(Incident)
        .where(
            Incident.orchestrator == orchestrator,
            Incident.dag_id == dag_id,
            Incident.task_id == task_id,
            Incident.signature == signature,
            Incident.status.in_(OPEN_STATUSES),
            Incident.created_at >= since,
        )
        .order_by(Incident.created_at.desc(), Incident.id.desc())
    )
    return session.scalars(query).first()


def _record_occurrence(session: Session, incident: Incident, key: Mapping[str, Any], source: str) -> None:
    """Fold another failure with the same signature into ``incident`` (idempotent per key).

    The occurrence is recorded with the phase-5 five-column key: the duplicate always
    belongs to the same orchestrator instance as ``incident``.
    """
    folded = _event_key(key)
    query = select(IncidentEvent.data).where(
        IncidentEvent.incident_id == incident.id, IncidentEvent.kind == "occurrence"
    )
    for data in session.scalars(query):
        if isinstance(data, dict) and all(data.get(k) == v for k, v in folded.items()):
            return
    incident.occurrences = (incident.occurrences or 1) + 1
    add_event(
        session,
        incident,
        "occurrence",
        f"Same failure seen again in run {folded['run_id']} (try {folded['try_number']}, "
        f"map_index {folded['map_index']}); occurrences={incident.occurrences}",
        data={**folded, "source": source},
    )


def _insert_incident(session: Session, values: dict[str, Any]) -> Incident | None:
    """Insert an incident row unless its key exists; ``None`` on conflict.

    SQLite and PostgreSQL use ``INSERT ... ON CONFLICT DO NOTHING`` on exactly the columns
    of ``uq_healer_incidents_key``. SAVEPOINTs are avoided on purpose: with the default
    pysqlite transaction handling a SAVEPOINT that opens the transaction commits it on
    release, which would defeat the caller's rollback.
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
    orchestrator: str = DEFAULT_ORCHESTRATOR,
    native_ref: Mapping[str, Any] | None = None,
    error_name: str | None = None,
    **extra: Any,
) -> tuple[Incident, bool]:
    """Create the incident for a failed attempt (Airflow: a task instance try).

    Returns ``(incident, created)``. The call is idempotent on the
    ``(orchestrator, dag_id, run_id, task_id, map_index, try_number)`` key. When
    ``signature`` is given and an open incident of the same orchestrator instance with the
    same dag, task and signature was created less than 24 hours before ``now``, that
    incident's ``occurrences`` counter is incremented instead (once per attempt) and it is
    returned with ``created=False``. ``native_ref`` (adapter pointers, stored as given:
    JSON-safe, never masked) and ``error_name`` are the v2 columns; ``extra`` sets any
    other Incident column (for example ``created_at`` or ``status``). The texts are stored
    as given: callers pass masked text (see :func:`create_incident_from_failure`).
    """
    if source not in SOURCES:
        raise ValueError(f"Unknown incident source {source!r}; expected one of {SOURCES}")
    unknown = set(extra) - _INCIDENT_EXTRA_COLUMNS
    if unknown:
        raise TypeError(f"Unexpected Incident fields: {', '.join(sorted(unknown))}")
    moment = _now(now)
    key = _ti_key(dag_id, run_id, task_id, map_index, try_number, orchestrator)

    existing = get_incident_by_key(session, **key)
    if existing is not None:
        return existing, False

    if signature:
        duplicate = _find_open_duplicate(
            session,
            orchestrator=key["orchestrator"],
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
        "native_ref": _native(native_ref),
        "error_name": error_name[:MAX_ERROR_NAME_CHARS] if error_name else None,
        "created_at": moment,
        "updated_at": moment,
        **extra,
    }
    incident = _insert_incident(session, values)
    if incident is None:
        # Another writer inserted the same attempt concurrently.
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
        data={**_event_key(key), "source": source},
    )
    return incident, True


def create_incident_from_failure(
    session: Session, f: Failure, *, signature: str | None, now: datetime | None = None
) -> tuple[Incident, bool]:
    """Create the incident of a confirmed :class:`~dag_healer.core.failures.Failure`.

    The key comes from ``f.ref`` (its orchestrator instance included) and the source from
    ``f.source``. ``f.error_summary`` and ``f.error_name`` are RAW and masked here (the
    core is the single masking point); ``f.native`` is stored as ``native_ref`` unchanged
    (adapter pointers are never masked; ``None`` when empty). ``signature`` drives the
    24-hour dedupe exactly as in :func:`create_incident`. A failure without a resolved step
    (``step_id == UNRESOLVED_STEP``) is refused: only push hints may lack a step.
    """
    ref = f.ref
    if ref.step_id == UNRESOLVED_STEP:
        raise ValueError(f"a failure needs a resolved step (or RUN_LEVEL_STEP): {ref}")
    return create_incident(
        session,
        **ref.store_key(),
        source=f.source,
        error_summary=mask_text(f.error_summary or ""),
        signature=signature,
        now=now,
        native_ref=f.native,
        error_name=mask_text(f.error_name) if f.error_name else None,
    )


def list_incidents(
    session: Session,
    *,
    dag_ids: set[str] | None = None,
    statuses: Sequence[str] | None = None,
    limit: int = 100,
    run_id: str | None = None,
    task_id: str | None = None,
    orchestrator: str | None = None,
) -> list[Incident]:
    """Return incidents newest first.

    ``dag_ids=None`` means no DAG filter; an empty set returns nothing (useful
    when the caller may read no DAG at all). ``orchestrator=None`` lists every instance.
    """
    if dag_ids is not None and not dag_ids:
        return []
    query = select(Incident)
    if orchestrator is not None:
        query = query.where(Incident.orchestrator == orchestrator)
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
    orchestrator: str | None = None,
) -> Incident | None:
    """Atomically claim the oldest incident in ``statuses`` and mark it ``triaging``.

    The claim is a conditional ``UPDATE`` (see :func:`try_claim`), so two
    workers (even on different engines or hosts) never claim the same row; on
    PostgreSQL the candidate is also selected ``FOR UPDATE SKIP LOCKED``.
    Commit promptly after claiming: on SQLite the open write transaction blocks
    other writers until it ends. ``orchestrator`` restricts the claim to the incidents of
    one instance (a worker serves one instance); ``None`` considers every incident.
    """
    query = select(Incident.id).where(Incident.status.in_(list(statuses)))
    if orchestrator is not None:
        query = query.where(Incident.orchestrator == orchestrator)
    query = query.order_by(Incident.created_at, Incident.id)
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
    orchestrator: str | None = None,
) -> list[Incident]:
    """Put incidents stuck in a working status (crashed worker) back to ``new``.

    ``orchestrator`` limits the release to one instance's incidents (``None``: all).
    """
    cutoff = _now(now) - older_than
    query = select(Incident).where(
        Incident.status.in_(list(statuses)),
        Incident.claimed_at.is_not(None),
        Incident.claimed_at < cutoff,
    )
    if orchestrator is not None:
        query = query.where(Incident.orchestrator == orchestrator)
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


def incidents_created_since(session: Session, since: datetime, *, orchestrator: str | None = None) -> int:
    """Return how many incidents (of ``orchestrator``, default all) were created at or after ``since``."""
    query = select(func.count()).select_from(Incident).where(Incident.created_at >= since)
    if orchestrator is not None:
        query = query.where(Incident.orchestrator == orchestrator)
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
    orchestrator: str = DEFAULT_ORCHESTRATOR,
) -> Command:
    """Queue a human action for the worker of instance ``orchestrator`` to apply."""
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
        orchestrator=_instance(orchestrator),
    )
    session.add(command)
    session.flush()
    return command


def pending_commands(session: Session, *, limit: int = 100, orchestrator: str | None = None) -> list[Command]:
    """Return pending commands (of ``orchestrator``, default all) in the order they were queued."""
    query = select(Command).where(Command.status == COMMAND_PENDING)
    if orchestrator is not None:
        query = query.where(Command.orchestrator == orchestrator)
    query = query.order_by(Command.id).limit(max(1, limit))
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
    orchestrator: str = DEFAULT_ORCHESTRATOR,
) -> ContractState:
    """Insert or update the cached state of a pipeline (DAG) contract of one instance."""
    instance = _instance(orchestrator)
    state = session.get(ContractState, (instance, dag_id))
    if state is None:
        state = ContractState(orchestrator=instance, dag_id=dag_id)
        session.add(state)
    state.status = status
    state.path = str(path)
    state.approved_by = approved_by
    state.approved_at = ensure_utc(approved_at)
    state.source_hash = source_hash
    state.updated_at = utcnow()
    session.flush()
    return state


def get_contract_state(
    session: Session, dag_id: str, *, orchestrator: str = DEFAULT_ORCHESTRATOR
) -> ContractState | None:
    """Return the cached contract state of ``dag_id`` (of instance ``orchestrator``)."""
    return session.get(ContractState, (_instance(orchestrator), dag_id))


def list_contract_states(session: Session, *, orchestrator: str | None = None) -> list[ContractState]:
    """Return the cached contract states (of ``orchestrator``, default all) ordered by DAG id."""
    query = select(ContractState)
    if orchestrator is not None:
        query = query.where(ContractState.orchestrator == orchestrator)
    return list(session.scalars(query.order_by(ContractState.dag_id, ContractState.orchestrator)))


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
# Pending events (pushed hints awaiting confirmation by the orchestrator API)
# --------------------------------------------------------------------------- #
def add_pending_event(session: Session, **fields: Any) -> PendingEvent:
    """Store a pushed failure hint as ``pending_check``.

    Accepts the ingest payload directly: ``dag_id``, ``run_id``, ``task_id``,
    ``map_index``, ``try_number``, ``payload`` and the v2 columns ``orchestrator``
    (default ``"airflow"``), ``channel`` (default ``"task-failed"``) and ``native_ref`` map
    to columns; any other key is merged into ``payload``. ``task_id`` may be ``""``
    (:data:`~dag_healer.core.ids.UNRESOLVED_STEP`: a run-level hint whose step is not known
    yet) but must be given. A second delivery of the same attempt of the same instance
    while the first is still pending returns the existing row (whatever its channel).
    """
    columns = {k: v for k, v in fields.items() if k in _PENDING_EVENT_COLUMNS}
    extras = {k: v for k, v in fields.items() if k not in _PENDING_EVENT_COLUMNS}
    missing = [k for k in ("dag_id", "run_id") if not columns.get(k)]
    if columns.get("task_id") is None:
        missing.append("task_id")
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
        columns.pop("orchestrator", None),
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
    event = PendingEvent(
        **key,
        payload=payload,
        status=status,
        received_at=received_at,
        channel=str(columns.pop("channel", None) or DEFAULT_CHANNEL),
        native_ref=_native(columns.pop("native_ref", None)),
    )
    session.add(event)
    session.flush()
    return event


def list_pending_events(
    session: Session, status: str = PENDING_CHECK, *, limit: int = 500, orchestrator: str | None = None
) -> list[PendingEvent]:
    """Return pending events with ``status`` (of ``orchestrator``, default all), oldest first."""
    query = select(PendingEvent).where(PendingEvent.status == status)
    if orchestrator is not None:
        query = query.where(PendingEvent.orchestrator == orchestrator)
    query = query.order_by(PendingEvent.id).limit(max(1, limit))
    return list(session.scalars(query))


def mark_pending_event(session: Session, event: PendingEvent, status: str) -> PendingEvent:
    """Set the status of a pending event (``confirmed`` or ``discarded``)."""
    if status not in PENDING_EVENT_STATUSES:
        raise ValueError(f"Unknown pending event status {status!r}")
    event.status = status
    session.add(event)
    return event


# --------------------------------------------------------------------------- #
# Key-value state (healer_kv): poll cursors and other small worker state
# --------------------------------------------------------------------------- #
def cursor_key(instance: str) -> str:
    """``healer_kv`` key of the poll cursor of an orchestrator instance (``cursor:<instance>``)."""
    return f"{CURSOR_KEY_PREFIX}{_instance(instance)}"


def kv_get(session: Session, key: str) -> Any | None:
    """The JSON value stored under ``key`` (``None`` when there is none)."""
    session.flush()
    return session.execute(select(KeyValue.value).where(KeyValue.key == key)).scalar_one_or_none()


def kv_set(session: Session, key: str, value: Any) -> None:
    """Store a JSON-safe copy of ``value`` under ``key`` (insert or replace).

    SQLite and PostgreSQL use ``INSERT ... ON CONFLICT DO UPDATE``, so two writers of the
    same key never fail on the primary key.
    """
    if not key:
        raise ValueError("healer_kv keys must not be empty")
    values = {"key": key, "value": json_safe(value), "updated_at": utcnow()}
    session.flush()
    dialect = session.get_bind().dialect.name
    if dialect in ("sqlite", "postgresql"):
        if dialect == "sqlite":
            from sqlalchemy.dialects.sqlite import insert as dialect_insert
        else:
            from sqlalchemy.dialects.postgresql import insert as dialect_insert
        statement = dialect_insert(KeyValue.__table__).values(**values)
        statement = statement.on_conflict_do_update(
            index_elements=["key"],
            set_={"value": statement.excluded.value, "updated_at": statement.excluded.updated_at},
        )
        session.execute(statement)
    else:
        row = session.get(KeyValue, key)
        if row is None:
            session.add(KeyValue(**values))
        else:
            row.value, row.updated_at = values["value"], values["updated_at"]
        session.flush()
    _expire_cached(session, key)


def kv_delete(session: Session, key: str) -> bool:
    """Delete ``key``; ``True`` when it existed."""
    session.flush()
    deleted = session.execute(delete(KeyValue).where(KeyValue.key == key)).rowcount
    _expire_cached(session, key)
    return bool(deleted)


def kv_items(session: Session, prefix: str = "") -> dict[str, Any]:
    """Every ``{key: value}`` whose key starts with ``prefix``, ordered by key."""
    session.flush()
    query = select(KeyValue.key, KeyValue.value).order_by(KeyValue.key)
    if prefix:
        query = query.where(KeyValue.key.startswith(prefix, autoescape=True))
    return {str(key): value for key, value in session.execute(query)}


def _expire_cached(session: Session, key: str) -> None:
    """Drop a cached :class:`KeyValue` object of ``key`` (the row changed behind the ORM)."""
    for item in list(session.identity_map.values()):
        if isinstance(item, KeyValue) and item.key == key:
            session.expire(item)


def _state_time(state: Mapping[str, Any], name: str, path: Path) -> str | None:
    value = state.get(name)
    if value is None or value == "":
        return None
    try:
        moment = ensure_utc(datetime.fromisoformat(str(value).replace("Z", "+00:00")))
    except ValueError:
        logger.warning("dag-healer store: ignoring invalid %s %r in %s", name, value, path)
        return None
    return moment.isoformat() if moment is not None else None


def import_state_file(
    session: Session, path: str | PathLike[str], *, instance: str = DEFAULT_ORCHESTRATOR
) -> dict[str, Any] | None:
    """One-time import of the phase-5 worker state file into ``healer_kv``.

    The phase-5 worker kept its Airflow REST poll position in ``<healer_home>/state.json``
    (``rest_watermark``: when the last successful poll started; ``rest_not_before``: the
    floor written by ``dag-healer store reset``). The first v2 worker of an Airflow instance
    calls this before its first poll: when ``cursor:<instance>`` does not exist yet and the
    file holds either key, it stores ``{"watermark": ..., "not_before": ...}`` (ISO-8601 UTC
    strings or ``None``) as that cursor and returns it. It returns ``None`` (and changes
    nothing) when the instance already has a cursor, when the file is missing, unreadable or
    holds neither key. The file is left in place.
    """
    key = cursor_key(instance)
    if kv_get(session, key) is not None:
        return None
    source = Path(path)
    try:
        state = json.loads(source.read_text(encoding="utf-8"))
    except FileNotFoundError:
        return None
    except (OSError, ValueError) as exc:
        logger.warning("dag-healer store: not importing the unreadable state file %s (%s)", source, exc)
        return None
    if not isinstance(state, Mapping):
        logger.warning("dag-healer store: not importing the state file %s (not a JSON object)", source)
        return None
    cursor = {
        "watermark": _state_time(state, STATE_WATERMARK_KEY, source),
        "not_before": _state_time(state, STATE_NOT_BEFORE_KEY, source),
    }
    if cursor["watermark"] is None and cursor["not_before"] is None:
        return None
    kv_set(session, key, cursor)
    logger.info("dag-healer store: imported the poll position of %s from %s into %s", instance, source, key)
    return cursor
