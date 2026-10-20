"""Request and response models of the dag-healer HTTP API (``/healer/api/v1``).

The shapes are the contract with the React UI (``ui/src/api.ts``); field names must not
change without updating it. Free text and samples are masked before they reach these
models (see :mod:`dag_healer.api.views`).
"""

from __future__ import annotations

from datetime import datetime
from typing import Any, Literal

from pydantic import BaseModel, ConfigDict, Field

ContractStatus = Literal["approved", "draft", "stale", "missing"]
Action = Literal["resolve_data", "resolve_code", "ignore", "retry_repair", "retry_task"]

MAX_NOTE_CHARS = 2000


class _Out(BaseModel):
    """Base of response models."""

    model_config = ConfigDict(extra="forbid")


# --------------------------------------------------------------------------- incidents


class IncidentSummary(_Out):
    """One incident in a list."""

    id: str
    dag_id: str
    run_id: str
    task_id: str
    map_index: int
    try_number: int
    status: str
    category: str | None = None
    confidence: float | None = None
    source: str
    error_summary: str
    created_at: datetime | None = None
    updated_at: datetime | None = None
    occurrences: int
    pr_url: str | None = None
    has_pr_md: bool = False


class Sample(_Out):
    """Masked sample rows of the dataset involved in the failure."""

    dataset: str | None = None
    columns: list[str] = Field(default_factory=list)
    rows: list[dict[str, Any]] = Field(default_factory=list)
    masked: Literal[True] = True


class EventView(_Out):
    """Timeline entry."""

    at: datetime | None = None
    kind: str
    message: str
    actor: str
    data: Any = None


class AlertView(_Out):
    """Notification attempt."""

    channel: str
    recipients: list[str] = Field(default_factory=list)
    subject: str
    status: str
    error: str | None = None
    sent_at: datetime | None = None


class PendingCommandView(_Out):
    """Human action waiting for the worker."""

    id: int
    kind: str
    created_at: datetime | None = None
    actor: str


class IncidentLinks(_Out):
    """Links into the Airflow UI."""

    task_instance: str
    dag: str
    healer: str


class IncidentDetail(IncidentSummary):
    """Everything the task-instance view shows about one incident."""

    diagnosis: str | None = None
    violated_rule: dict[str, Any] | None = None
    violations: list[dict[str, Any]] = Field(default_factory=list)
    sample: Sample | None = None
    evidence: list[str] = Field(default_factory=list)
    test_path: str | None = None
    test_code: str | None = None
    diff: str | None = None
    pytest_output: str | None = None
    pr_md_url: str | None = None
    attempts: int = 0
    tokens_used: int = 0
    resolution: str | None = None
    resolved_by: str | None = None
    events: list[EventView] = Field(default_factory=list)
    alerts: list[AlertView] = Field(default_factory=list)
    links: IncidentLinks
    allowed_actions: list[Action] = Field(default_factory=list)
    pending_commands: list[PendingCommandView] = Field(default_factory=list)
    can_edit: bool = False


class IncidentList(_Out):
    """``GET /incidents``."""

    incidents: list[IncidentSummary]
    total: int


class TaskInstanceIncident(_Out):
    """``GET /dags/{dag_id}/task-instance``."""

    incident: IncidentDetail | None = None


# --------------------------------------------------------------------------- contracts


class LocatorView(_Out):
    """Where a dataset lives."""

    type: str
    path: str


class ColumnView(_Out):
    """Column rule of a dataset contract."""

    name: str
    dtype: str
    nullable: bool
    min: float | None = None
    max: float | None = None
    pii: bool = False
    allowed_values: list[str] | None = None
    note: str | None = None


class RowCountView(_Out):
    """Row-count band."""

    min: int | None = None
    max: int | None = None


class DatasetView(_Out):
    """One dataset of a contract."""

    name: str
    task_id: str | None = None
    locator: LocatorView
    columns: list[ColumnView] = Field(default_factory=list)
    row_count: RowCountView = Field(default_factory=RowCountView)


class ContractView(_Out):
    """A DAG data contract as shown in the UI."""

    dag_id: str
    status: Literal["approved", "draft", "stale"]
    approved_by: str | None = None
    approved_at: datetime | None = None
    stale: bool = False
    path: str
    datasets: list[DatasetView] = Field(default_factory=list)
    can_approve: bool = False
    pending_approval: bool = False


class ContractList(_Out):
    """``GET /contracts``."""

    contracts: list[ContractView]


# --------------------------------------------------------------------------- DAGs


class DagSummary(_Out):
    """A monitored DAG."""

    dag_id: str
    monitored: bool
    contract_status: ContractStatus
    open_incidents: int
    last_incident: IncidentSummary | None = None


class DagList(_Out):
    """``GET /dags``."""

    dags: list[DagSummary]


class DagIncidents(_Out):
    """``GET /dags/{dag_id}/incidents``."""

    incidents: list[IncidentSummary]
    contract: ContractView | None = None
    monitored: bool
    can_edit: bool


# --------------------------------------------------------------------------- misc


class MeView(_Out):
    """``GET /me``."""

    user: str
    base_url: str


class CommandAccepted(_Out):
    """202 answer of the action routes."""

    command_id: int
    status: str


class EventAccepted(_Out):
    """202 answer of ``POST /events/task-failed``.

    ``accepted`` is False (and ``pending_event_id`` null) for DAGs that are not monitored.
    """

    accepted: bool
    pending_event_id: int | None = None


# --------------------------------------------------------------------------- request bodies


class _In(BaseModel):
    """Base of request bodies (unknown keys are rejected)."""

    model_config = ConfigDict(extra="forbid")


class ResolveBody(_In):
    """``POST .../resolve``."""

    resolution: Literal["data", "code", "ignore"]
    note: str | None = Field(default=None, max_length=MAX_NOTE_CHARS)


class NoteBody(_In):
    """Optional body of ``retry-repair``, ``retry-task`` and ``contract/approve``."""

    note: str | None = Field(default=None, max_length=MAX_NOTE_CHARS)


class TaskFailedEvent(BaseModel):
    """Payload of ``POST /events/task-failed`` (``dag_healer.ingest.build_payload``)."""

    model_config = ConfigDict(extra="ignore")

    dag_id: str = Field(min_length=1, max_length=250)
    task_id: str = Field(min_length=1, max_length=250)
    run_id: str = Field(min_length=1, max_length=250)
    map_index: int = -1
    try_number: int | None = Field(default=None, ge=0)
    max_tries: int | None = None
    state: str | None = Field(default=None, max_length=64)
    hostname: str | None = Field(default=None, max_length=255)
    error: str | None = Field(default=None, max_length=64 * 1024)
    source: str | None = Field(default=None, max_length=32)
    reported_at: str | None = Field(default=None, max_length=64)


__all__ = [
    "Action",
    "AlertView",
    "ColumnView",
    "CommandAccepted",
    "ContractList",
    "ContractStatus",
    "ContractView",
    "DagIncidents",
    "DagList",
    "DagSummary",
    "DatasetView",
    "EventAccepted",
    "EventView",
    "IncidentDetail",
    "IncidentLinks",
    "IncidentList",
    "IncidentSummary",
    "LocatorView",
    "MeView",
    "NoteBody",
    "PendingCommandView",
    "ResolveBody",
    "RowCountView",
    "Sample",
    "TaskFailedEvent",
    "TaskInstanceIncident",
]
