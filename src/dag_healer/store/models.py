"""SQLAlchemy 2.x models for the dag-healer store.

All tables are prefixed with ``healer_`` so the store can share a database with
other applications. The schema is portable between SQLite (single process,
tests) and PostgreSQL (multi-container deployments): JSON columns use the
generic :class:`sqlalchemy.JSON` type and timestamps are always timezone-aware
UTC values on the Python side (see :class:`UTCDateTime`).
"""

from __future__ import annotations

import uuid
from datetime import date, datetime, timezone
from decimal import Decimal
from typing import Any

from sqlalchemy import (
    JSON,
    DateTime,
    Float,
    ForeignKey,
    Index,
    Integer,
    String,
    Text,
    UniqueConstraint,
)
from sqlalchemy.engine import Dialect
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column, relationship
from sqlalchemy.types import TypeDecorator

SCHEMA_VERSION = 1

# --------------------------------------------------------------------------- #
# Status constants
# --------------------------------------------------------------------------- #
NEW = "new"
TRIAGING = "triaging"
CLASSIFIED = "classified"
REPORTING = "reporting"
REPORTED = "reported"
REPAIRING = "repairing"
REPAIRED = "repaired"
REPAIR_FAILED = "repair_failed"
WAITING_HUMAN = "waiting_human"
RESUMED = "resumed"
IGNORED = "ignored"
ERROR = "error"

#: Statuses of incidents that still need work (by the worker or by a human).
OPEN_STATUSES: tuple[str, ...] = (
    NEW,
    TRIAGING,
    CLASSIFIED,
    REPORTING,
    REPAIRING,
    WAITING_HUMAN,
    RESUMED,
)
#: Terminal statuses: the incident pipeline has finished.
FINAL_STATUSES: tuple[str, ...] = (REPORTED, REPAIRED, REPAIR_FAILED, IGNORED, ERROR)
ALL_STATUSES: tuple[str, ...] = OPEN_STATUSES + FINAL_STATUSES

CATEGORIES: tuple[str, ...] = ("data", "code", "ambiguous")
SOURCES: tuple[str, ...] = ("listener", "callback", "rest", "cli")

COMMAND_KINDS: tuple[str, ...] = ("resolve", "retry_repair", "retry_task", "approve_contract")
COMMAND_PENDING = "pending"
COMMAND_APPLIED = "applied"
COMMAND_FAILED = "failed"
COMMAND_STATUSES: tuple[str, ...] = (COMMAND_PENDING, COMMAND_APPLIED, COMMAND_FAILED)

PENDING_CHECK = "pending_check"
PENDING_CONFIRMED = "confirmed"
PENDING_DISCARDED = "discarded"
PENDING_EVENT_STATUSES: tuple[str, ...] = (PENDING_CHECK, PENDING_CONFIRMED, PENDING_DISCARDED)


def utcnow() -> datetime:
    """Return the current time as a timezone-aware UTC datetime."""
    return datetime.now(timezone.utc)


def new_id() -> str:
    """Return a new random identifier (uuid4 hex, 32 chars)."""
    return uuid.uuid4().hex


def ensure_utc(value: datetime | None) -> datetime | None:
    """Return ``value`` as an aware UTC datetime (naive values are assumed UTC)."""
    if value is None:
        return None
    if value.tzinfo is None:
        return value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc)


class UTCDateTime(TypeDecorator[datetime]):
    """Timezone-aware UTC datetime that round-trips identically on every backend.

    PostgreSQL stores ``timestamp with time zone``. SQLite has no time zone
    support, so values are stored as naive UTC strings (which keeps lexical
    ordering and comparisons correct) and re-tagged as UTC when loaded.
    """

    impl = DateTime(timezone=True)
    cache_ok = True

    def process_bind_param(self, value: datetime | None, dialect: Dialect) -> datetime | None:
        """Normalise to UTC before sending the value to the database."""
        value = ensure_utc(value)
        if value is not None and dialect.name == "sqlite":
            return value.replace(tzinfo=None)
        return value

    def process_result_value(self, value: datetime | None, dialect: Dialect) -> datetime | None:
        """Return an aware UTC datetime regardless of the backend."""
        return ensure_utc(value)


def json_safe(value: Any) -> Any:
    """Convert ``value`` into something :func:`json.dumps` accepts."""
    if value is None or isinstance(value, (str, int, float, bool)):
        return value
    if isinstance(value, datetime):
        aware = ensure_utc(value)
        return aware.isoformat() if aware is not None else None
    if isinstance(value, date):
        return value.isoformat()
    if isinstance(value, Decimal):
        return float(value)
    if isinstance(value, dict):
        return {str(k): json_safe(v) for k, v in value.items()}
    if isinstance(value, (list, tuple, set, frozenset)):
        return [json_safe(v) for v in value]
    return str(value)


class Base(DeclarativeBase):
    """Declarative base for every dag-healer table."""

    def _columns_dict(self, exclude: frozenset[str] = frozenset()) -> dict[str, Any]:
        """Return the mapped column values as a JSON-safe dict."""
        return {
            column.key: json_safe(getattr(self, column.key))
            for column in self.__table__.columns  # type: ignore[attr-defined]
            if column.key not in exclude
        }


class Incident(Base):
    """One failed task instance try (or a deduplicated group of them)."""

    __tablename__ = "healer_incidents"
    __table_args__ = (
        UniqueConstraint(
            "dag_id", "run_id", "task_id", "map_index", "try_number", name="uq_healer_incidents_ti"
        ),
        Index("ix_healer_incidents_status", "status"),
        Index("ix_healer_incidents_dag_created", "dag_id", "created_at"),
        Index("ix_healer_incidents_signature", "dag_id", "task_id", "signature"),
    )

    #: Large fields left out of ``to_dict(summary=True)``.
    HEAVY_FIELDS = frozenset({"test_code", "diff", "pytest_output", "sample", "evidence"})

    id: Mapped[str] = mapped_column(String(32), primary_key=True, default=new_id)
    dag_id: Mapped[str] = mapped_column(String(250), nullable=False)
    run_id: Mapped[str] = mapped_column(String(250), nullable=False)
    task_id: Mapped[str] = mapped_column(String(250), nullable=False)
    map_index: Mapped[int] = mapped_column(Integer, nullable=False, default=-1)
    try_number: Mapped[int] = mapped_column(Integer, nullable=False, default=1)
    status: Mapped[str] = mapped_column(String(32), nullable=False, default=NEW)
    category: Mapped[str | None] = mapped_column(String(16), nullable=True)
    confidence: Mapped[float | None] = mapped_column(Float, nullable=True)
    source: Mapped[str] = mapped_column(String(16), nullable=False, default="rest")
    error_summary: Mapped[str] = mapped_column(Text, nullable=False, default="")
    diagnosis: Mapped[str | None] = mapped_column(Text, nullable=True)
    violated_rule: Mapped[Any | None] = mapped_column(JSON, nullable=True)
    sample: Mapped[Any | None] = mapped_column(JSON, nullable=True)
    evidence: Mapped[Any | None] = mapped_column(JSON, nullable=True)
    test_path: Mapped[str | None] = mapped_column(String(1024), nullable=True)
    test_code: Mapped[str | None] = mapped_column(Text, nullable=True)
    diff: Mapped[str | None] = mapped_column(Text, nullable=True)
    pytest_output: Mapped[str | None] = mapped_column(Text, nullable=True)
    pr_url: Mapped[str | None] = mapped_column(String(1024), nullable=True)
    pr_md_path: Mapped[str | None] = mapped_column(String(1024), nullable=True)
    signature: Mapped[str | None] = mapped_column(String(128), nullable=True)
    occurrences: Mapped[int] = mapped_column(Integer, nullable=False, default=1)
    attempts: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    tokens_used: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    resolution: Mapped[str | None] = mapped_column(String(32), nullable=True)
    resolved_by: Mapped[str | None] = mapped_column(String(250), nullable=True)
    created_at: Mapped[datetime] = mapped_column(UTCDateTime(), nullable=False, default=utcnow)
    updated_at: Mapped[datetime] = mapped_column(
        UTCDateTime(), nullable=False, default=utcnow, onupdate=utcnow
    )
    claimed_at: Mapped[datetime | None] = mapped_column(UTCDateTime(), nullable=True)

    events: Mapped[list[IncidentEvent]] = relationship(
        back_populates="incident",
        cascade="all, delete-orphan",
        order_by="IncidentEvent.id",
        lazy="select",
    )

    @property
    def is_open(self) -> bool:
        """True while the incident still needs work."""
        return self.status in OPEN_STATUSES

    def to_dict(self, *, summary: bool = False) -> dict[str, Any]:
        """Return a JSON-safe dict; ``summary=True`` omits large fields."""
        return self._columns_dict(self.HEAVY_FIELDS if summary else frozenset())

    def __repr__(self) -> str:
        return (
            f"Incident(id={self.id!r}, dag_id={self.dag_id!r}, task_id={self.task_id!r}, "
            f"run_id={self.run_id!r}, try_number={self.try_number!r}, status={self.status!r})"
        )


class IncidentEvent(Base):
    """Timeline entry of an incident (status changes, notes, actions)."""

    __tablename__ = "healer_incident_events"
    __table_args__ = (Index("ix_healer_incident_events_incident", "incident_id", "id"),)

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    incident_id: Mapped[str] = mapped_column(
        String(32), ForeignKey("healer_incidents.id", ondelete="CASCADE"), nullable=False
    )
    at: Mapped[datetime] = mapped_column(UTCDateTime(), nullable=False, default=utcnow)
    kind: Mapped[str] = mapped_column(String(32), nullable=False)
    message: Mapped[str] = mapped_column(Text, nullable=False, default="")
    data: Mapped[Any | None] = mapped_column(JSON, nullable=True)
    actor: Mapped[str] = mapped_column(String(250), nullable=False, default="dag-healer")

    incident: Mapped[Incident] = relationship(back_populates="events")

    def to_dict(self) -> dict[str, Any]:
        """Return a JSON-safe dict."""
        return self._columns_dict()


class ContractState(Base):
    """Cached state of a DAG contract (the YAML file stays the source of truth)."""

    __tablename__ = "healer_contracts"

    dag_id: Mapped[str] = mapped_column(String(250), primary_key=True)
    status: Mapped[str] = mapped_column(String(16), nullable=False, default="draft")
    approved_by: Mapped[str | None] = mapped_column(String(250), nullable=True)
    approved_at: Mapped[datetime | None] = mapped_column(UTCDateTime(), nullable=True)
    path: Mapped[str] = mapped_column(String(1024), nullable=False, default="")
    source_hash: Mapped[str | None] = mapped_column(String(128), nullable=True)
    updated_at: Mapped[datetime] = mapped_column(
        UTCDateTime(), nullable=False, default=utcnow, onupdate=utcnow
    )

    def to_dict(self) -> dict[str, Any]:
        """Return a JSON-safe dict."""
        return self._columns_dict()


class Alert(Base):
    """A notification attempt (``sent_at`` is the time of the attempt)."""

    __tablename__ = "healer_alerts"
    __table_args__ = (Index("ix_healer_alerts_incident", "incident_id"),)

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    incident_id: Mapped[str | None] = mapped_column(
        String(32), ForeignKey("healer_incidents.id", ondelete="CASCADE"), nullable=True
    )
    channel: Mapped[str] = mapped_column(String(32), nullable=False)
    recipients: Mapped[Any | None] = mapped_column(JSON, nullable=True)
    subject: Mapped[str] = mapped_column(Text, nullable=False, default="")
    status: Mapped[str] = mapped_column(String(16), nullable=False)
    error: Mapped[str | None] = mapped_column(Text, nullable=True)
    sent_at: Mapped[datetime] = mapped_column(UTCDateTime(), nullable=False, default=utcnow)

    def to_dict(self) -> dict[str, Any]:
        """Return a JSON-safe dict."""
        return self._columns_dict()


class Command(Base):
    """A human action queued by the API/CLI and applied by the worker."""

    __tablename__ = "healer_commands"
    __table_args__ = (Index("ix_healer_commands_status", "status", "id"),)

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    kind: Mapped[str] = mapped_column(String(32), nullable=False)
    incident_id: Mapped[str | None] = mapped_column(
        String(32), ForeignKey("healer_incidents.id", ondelete="CASCADE"), nullable=True
    )
    dag_id: Mapped[str] = mapped_column(String(250), nullable=False)
    payload: Mapped[Any | None] = mapped_column(JSON, nullable=True)
    actor: Mapped[str] = mapped_column(String(250), nullable=False)
    status: Mapped[str] = mapped_column(String(16), nullable=False, default=COMMAND_PENDING)
    created_at: Mapped[datetime] = mapped_column(UTCDateTime(), nullable=False, default=utcnow)
    applied_at: Mapped[datetime | None] = mapped_column(UTCDateTime(), nullable=True)
    error: Mapped[str | None] = mapped_column(Text, nullable=True)

    def to_dict(self) -> dict[str, Any]:
        """Return a JSON-safe dict."""
        return self._columns_dict()


class PendingEvent(Base):
    """A failure event pushed by the listener/callback, waiting for REST confirmation."""

    __tablename__ = "healer_pending_events"
    __table_args__ = (Index("ix_healer_pending_events_status", "status", "id"),)

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    dag_id: Mapped[str] = mapped_column(String(250), nullable=False)
    run_id: Mapped[str] = mapped_column(String(250), nullable=False)
    task_id: Mapped[str] = mapped_column(String(250), nullable=False)
    map_index: Mapped[int] = mapped_column(Integer, nullable=False, default=-1)
    try_number: Mapped[int] = mapped_column(Integer, nullable=False, default=1)
    payload: Mapped[Any | None] = mapped_column(JSON, nullable=True)
    received_at: Mapped[datetime] = mapped_column(UTCDateTime(), nullable=False, default=utcnow)
    status: Mapped[str] = mapped_column(String(16), nullable=False, default=PENDING_CHECK)

    def to_dict(self) -> dict[str, Any]:
        """Return a JSON-safe dict."""
        return self._columns_dict()


class SchemaVersion(Base):
    """Single-row table recording the store schema version."""

    __tablename__ = "healer_schema_version"

    version: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=False)
    applied_at: Mapped[datetime] = mapped_column(UTCDateTime(), nullable=False, default=utcnow)
