"""Request and response models of the dag-healer HTTP API (``/healer/api/v1``).

The shapes are the contract with the React UI (``ui/src/api.ts``); field names must not
change without updating it. Free text and samples are masked before they reach these
models (see :mod:`dag_healer.api.views`).

Wire contract v1 is frozen: later fields are additive only (section 7.4 of
``docs/architecture/orchestrators.md``). Each model lists them in ``ADDITIVE_FIELDS`` with
their neutral value, and a field that holds it is left out of the output, so every response
for the default Airflow instance stays exactly as it was in v1.
"""

from __future__ import annotations

from collections.abc import Mapping
from datetime import datetime
from typing import Any, ClassVar, Literal

from pydantic import BaseModel, ConfigDict, Field, SerializerFunctionWrapHandler, model_serializer

from dag_healer.core.ids import DEFAULT_ORCHESTRATOR

ContractStatus = Literal["approved", "draft", "stale", "missing"]
Action = Literal["resolve_data", "resolve_code", "ignore", "retry_repair", "retry_task"]
Host = Literal["airflow_plugin", "standalone"]

MAX_NOTE_CHARS = 2000


class _Out(BaseModel):
    """Base of response models."""

    model_config = ConfigDict(extra="forbid")

    #: Fields added after wire contract v1 and their neutral values (a neutral field is left out).
    #: A public class variable, annotated in every model that sets it: pydantic releases differ
    #: in how they treat a ``_private`` name assigned again in a subclass.
    ADDITIVE_FIELDS: ClassVar[Mapping[str, Any]] = {}

    # No return annotation: pydantic would build the serialization (OpenAPI) schema of every
    # response model from it instead of from the fields.
    @model_serializer(mode="wrap")
    def _leave_out_neutral_additions(self, handler: SerializerFunctionWrapHandler):
        """Dump as usual, without the additive fields that hold their neutral value.

        The rule of ``HealerConfig._leave_out_default_sections``: what v1 clients receive does
        not change, and a field that differs from its neutral value is always shown.
        """
        data = handler(self)
        if isinstance(data, dict):
            for name, neutral in self.ADDITIVE_FIELDS.items():
                if name in data and getattr(self, name) == neutral:
                    del data[name]
        return data


# --------------------------------------------------------------------------- incidents


class IncidentLinks(_Out):
    """Links to the Healer views of an incident, and into the orchestrator's own UI.

    ``task_instance``, ``dag`` and ``healer`` open the Healer tabs (the same URL shapes in
    the Airflow UI and on the standalone host). ``native`` holds labelled links into the
    orchestrator's own pages (``{"AWS console": url}``); it is empty for Airflow, whose UI
    hosts the Healer views.
    """

    ADDITIVE_FIELDS: ClassVar[Mapping[str, Any]] = {"native": {}}

    task_instance: str
    dag: str
    healer: str
    native: dict[str, str] = Field(default_factory=dict)


class _IncidentFields(_Out):
    """Fields shared by :class:`IncidentSummary` and :class:`IncidentDetail`.

    ``orchestrator`` is the instance id of the incident; ``error_name`` the native error name
    the orchestrator reported (masked). The detail does not derive from the summary, so its
    required ``links`` keeps the v1 position in the JSON (after ``alerts``).
    """

    ADDITIVE_FIELDS: ClassVar[Mapping[str, Any]] = {"orchestrator": DEFAULT_ORCHESTRATOR, "error_name": None}

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
    orchestrator: str = DEFAULT_ORCHESTRATOR
    error_name: str | None = None


class PendingCommandView(_Out):
    """Human action waiting for the worker.

    ``action`` is the action as the person chose it (``resolve_data``, ``retry_task``...): the
    ``kind`` of a ``resolve`` command does not say which resolution it carries.
    """

    ADDITIVE_FIELDS: ClassVar[Mapping[str, Any]] = {"action": None}

    id: int
    kind: str
    created_at: datetime | None = None
    actor: str
    action: str | None = None


class IncidentSummary(_IncidentFields):
    """One incident in a list (``links`` only when there are native links to show).

    ``pending_command`` is the action a person queued that the worker has not applied yet (at
    most one is pending per incident).
    """

    ADDITIVE_FIELDS: ClassVar[Mapping[str, Any]] = {
        **_IncidentFields.ADDITIVE_FIELDS,
        "links": None,
        "pending_command": None,
    }

    links: IncidentLinks | None = None
    pending_command: PendingCommandView | None = None


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


class IncidentDetail(_IncidentFields):
    """Everything the task-instance view shows about one incident.

    It has every field of :class:`IncidentSummary`, with ``links`` always set.
    ``action_warnings`` holds texts to show next to offered actions
    (``{"retry_task": [...]}``).
    """

    ADDITIVE_FIELDS: ClassVar[Mapping[str, Any]] = {**_IncidentFields.ADDITIVE_FIELDS, "action_warnings": {}}

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
    action_warnings: dict[str, list[str]] = Field(default_factory=dict)


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
    """One dataset of a contract.

    ``allow_extra_columns`` says whether columns the contract does not list are accepted.
    """

    ADDITIVE_FIELDS: ClassVar[Mapping[str, Any]] = {"allow_extra_columns": True}

    name: str
    task_id: str | None = None
    locator: LocatorView
    columns: list[ColumnView] = Field(default_factory=list)
    row_count: RowCountView = Field(default_factory=RowCountView)
    allow_extra_columns: bool = True


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


class MeOrchestrator(_Out):
    """The orchestrator of this dag-healer (``GET /me``): its words and what it can do.

    ``vocabulary`` has the keys of :class:`dag_healer.core.vocab.Vocabulary` (``product``,
    ``pipeline``, ``step``, ``retry_action``...); ``capabilities`` those of
    :class:`dag_healer.core.vocab.Capabilities`, with the remediation kinds as their values.
    """

    type: str
    instance: str
    display_name: str
    vocabulary: dict[str, str]
    capabilities: dict[str, Any]


class MeView(_Out):
    """``GET /me``.

    ``host`` is where the views are served (the Airflow plugin or ``dag-healer serve``);
    ``orchestrator`` is left out for the default Airflow instance, whose words are the UI's
    defaults.
    """

    ADDITIVE_FIELDS: ClassVar[Mapping[str, Any]] = {"host": "airflow_plugin", "orchestrator": None}

    user: str
    base_url: str
    host: Host = "airflow_plugin"
    orchestrator: MeOrchestrator | None = None


class AlertSetup(_Out):
    """Who hears about an incident (``notify``): every alert is also written to the worker's log.

    ``events`` are the incident events that send alerts (``notify.events``); ``email`` the
    recipients of the email channel (``notify.email.to``, empty without ``notify.email``).
    """

    events: list[str] = Field(default_factory=list)
    email: list[str] = Field(default_factory=list)


class RepairSetup(_Out):
    """How a CODE fix is delivered and what it may change (``github``, ``repo``, ``limits``).

    ``token_configured`` says whether this process has a GitHub token (never the token): with
    one, a fix becomes a pull request on ``repository`` (``github.repo``, else the repository's
    GitHub origin) against ``pr_base``; without one, it is committed on a local branch and
    described in PR.md. ``skip_ci`` puts ``[skip ci]`` in the fix's commit.
    """

    token_configured: bool
    repository: str | None = None
    base_branch: str
    pr_base: str
    skip_ci: bool
    allowed_paths: list[str] = Field(default_factory=list)
    tests_dir: str | None = None
    max_diff_files: int
    max_diff_lines: int


class SetupView(_Out):
    """``GET /setup``: what the human actions lead to in this deployment.

    The views show it before an action is confirmed: who gets an alert, how a fix is delivered
    and what it may change, and the confidence the model needs to decide on its own
    (``classification.min_confidence``).
    """

    alerts: AlertSetup
    repairs: RepairSetup
    min_confidence: float
    contracts_dir: str


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


class EventsAccepted(_Out):
    """202 answer of ``POST /events/{channel}`` (one payload may carry several failure hints).

    ``pending_event_ids`` are the stored hints, oldest first; ``accepted`` is False when none
    was stored (every hint was of an unmonitored pipeline, or the payload had none).
    """

    accepted: bool
    pending_event_ids: list[int] = Field(default_factory=list)


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
    "AlertSetup",
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
    "EventsAccepted",
    "Host",
    "IncidentDetail",
    "IncidentLinks",
    "IncidentList",
    "IncidentSummary",
    "LocatorView",
    "MeOrchestrator",
    "MeView",
    "NoteBody",
    "PendingCommandView",
    "RepairSetup",
    "ResolveBody",
    "RowCountView",
    "Sample",
    "SetupView",
    "TaskFailedEvent",
    "TaskInstanceIncident",
]
