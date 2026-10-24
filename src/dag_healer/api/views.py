"""Builders that turn store rows and contract files into API response models.

Masking (defense in depth): the worker stores samples and texts already masked, and this
module masks them again on the way out. Free text (error summaries, diagnoses, evidence,
timeline messages, pytest output, violation details) goes through
:func:`dag_healer.masking.mask_text`; sample rows are re-checked column by column (PII
columns of the contract, ``masking.columns`` and sensitive-looking names are hashed unless
the value already is a mask token, other strings are scrubbed with ``mask_text``).
``masking.allow`` columns are left alone and the masking hook is not re-run here.
Test code, diffs and PR.md files are shown as written by the worker (they are the code
of the pull request).

The orchestrator adapter (``adapter``, ``None`` when it cannot be built) only shapes the
views: native links, the actions its remediator can serve, their warnings and the words of
``/me``. The queries take the instance id (``orchestrator``) the process serves.
"""

from __future__ import annotations

import logging
import re
import threading
from collections.abc import Iterable, Mapping
from dataclasses import dataclass
from pathlib import Path
from typing import TYPE_CHECKING, Any

from sqlalchemy import func, select
from sqlalchemy.orm import Session

from dag_healer import links
from dag_healer.api import schemas
from dag_healer.config import HealerConfig
from dag_healer.contracts.io import is_stale, load_contract
from dag_healer.contracts.schema import Contract, ContractError
from dag_healer.core.ids import DEFAULT_ORCHESTRATOR, AttemptRef
from dag_healer.core.vocab import Vocabulary
from dag_healer.masking import (
    HASH_LENGTH,
    HASH_PREFIX,
    REDACTED,
    is_sensitive_column,
    mask_text,
    mask_value,
    masking_settings,
    to_json_safe,
)
from dag_healer.store import repo as store_repo
from dag_healer.store.models import (
    COMMAND_PENDING,
    ERROR,
    OPEN_STATUSES,
    REPAIR_FAILED,
    REPAIRED,
    REPORTED,
    WAITING_HUMAN,
    Command,
    Incident,
    IncidentEvent,
)

if TYPE_CHECKING:
    from dag_healer.core.ports import OrchestratorAdapter

log = logging.getLogger(__name__)

# --------------------------------------------------------------------------- actions

#: Actions a human may request, by incident status.
ALLOWED_ACTIONS_BY_STATUS: dict[str, tuple[str, ...]] = {
    WAITING_HUMAN: ("resolve_data", "resolve_code", "retry_task", "ignore"),
    REPAIR_FAILED: ("retry_repair", "resolve_data", "ignore"),
    ERROR: ("retry_repair", "resolve_data", "ignore"),
    REPORTED: ("resolve_code", "retry_task", "ignore"),
    REPAIRED: ("retry_task",),
}
#: ``resolve`` body ``resolution`` -> action name.
RESOLUTION_ACTIONS: dict[str, str] = {"data": "resolve_data", "code": "resolve_code", "ignore": "ignore"}


def actions_for_status(status: str) -> list[str]:
    """Actions allowed for an incident in ``status`` (ignoring permissions)."""
    return list(ALLOWED_ACTIONS_BY_STATUS.get(status, ()))


def allowed_actions(
    status: str, *, can_edit: bool, has_pending: bool = False, retry_available: bool = True
) -> list[str]:
    """Actions to offer: none without edit permission or while a command is pending.

    ``retry_available=False`` (see :func:`retry_available`) leaves ``retry_task`` out; the
    other actions keep their order.
    """
    if not can_edit or has_pending:
        return []
    return [action for action in actions_for_status(status) if retry_available or action != "retry_task"]


def _key_row(incident: Incident) -> dict[str, Any]:
    """The key columns and ``native_ref`` of an incident: what an adapter needs to address it."""
    return {
        "id": incident.id,
        "orchestrator": incident.orchestrator,
        "dag_id": incident.dag_id,
        "run_id": incident.run_id,
        "task_id": incident.task_id,
        "map_index": incident.map_index,
        "try_number": incident.try_number,
        "native_ref": incident.native_ref,
    }


def retry_available(adapter: OrchestratorAdapter | None, incident: Incident) -> bool:
    """Whether ``retry_task`` can be served for ``incident``.

    True without an adapter (the worker decides, as before adapters existed), False when the
    adapter has no remediator; otherwise ``remediator.available()`` (no I/O) must offer a
    remediation. An error gives False.
    """
    if adapter is None:
        return True
    remediator = getattr(adapter, "remediator", None)
    if remediator is None:
        return False
    native = incident.native_ref if isinstance(incident.native_ref, Mapping) else {}
    try:
        ref = AttemptRef.from_row(_key_row(incident), default_orchestrator=adapter.instance)
        return bool(remediator.available(ref, native))
    except Exception as exc:  # an adapter error only hides the action
        log.warning(
            "dag-healer: cannot tell whether incident %s can be retried: %s",
            incident.id,
            mask_text(f"{type(exc).__name__}: {exc}")[:500],
        )
        return False


def _vocabulary_words(adapter: OrchestratorAdapter | None) -> dict[str, str]:
    """The adapter's words over the Airflow defaults (:class:`~dag_healer.core.vocab.Vocabulary`).

    Missing keys keep the default word and unknown keys are ignored.
    """
    words = Vocabulary().as_dict()
    try:
        given = adapter.vocabulary.as_dict() if adapter is not None else {}
    except Exception:  # a broken vocabulary falls back to the defaults
        given = {}
    words.update({key: str(value) for key, value in given.items() if key in words})
    return words


def action_warnings(
    adapter: OrchestratorAdapter | None, incident: Incident, actions: Iterable[str]
) -> dict[str, list[str]]:
    """Texts to show next to offered actions (``{action: [warning, ...]}``).

    ``retry_task`` of a ``repaired`` incident warns when a merged fix reaches the orchestrator
    only through a deployment (``capabilities.fix_delivery == "ci_deploy"``): retrying before
    the deployment runs the old code.
    """
    if adapter is None or incident.status != REPAIRED or "retry_task" not in actions:
        return {}
    if getattr(getattr(adapter, "capabilities", None), "fix_delivery", None) != "ci_deploy":
        return {}
    words = _vocabulary_words(adapter)
    product, step, label = words["product"], words["step"], words["retry_action"]
    warning = (
        f"A merged fix reaches {product} only through your deployment: deploy it before "
        f'"{label}", or the {step} runs the old code again.'
    )
    return {"retry_task": [warning]}


def _capabilities_view(capabilities: Any) -> dict[str, Any]:
    """:class:`~dag_healer.core.vocab.Capabilities` as JSON (remediation kinds by value)."""
    return {
        "sources": [str(source) for source in capabilities.sources],
        "push_channels": [str(channel) for channel in capabilities.push_channels],
        "attempts_visible": bool(capabilities.attempts_visible),
        "remediations": [str(getattr(kind, "value", kind)) for kind in capabilities.remediations],
        "fix_delivery": str(capabilities.fix_delivery),
        "ui_hosts": [str(host) for host in capabilities.ui_hosts],
        "native_authz": bool(capabilities.native_authz),
    }


def orchestrator_view(adapter: OrchestratorAdapter | None) -> schemas.MeOrchestrator | None:
    """The ``orchestrator`` block of ``/me``.

    ``None`` without an adapter and for the default Airflow instance (type and instance
    ``airflow``): the UI's built-in words and actions are Airflow's.
    """
    if adapter is None or (adapter.type == DEFAULT_ORCHESTRATOR and adapter.instance == DEFAULT_ORCHESTRATOR):
        return None
    return schemas.MeOrchestrator(
        type=str(adapter.type),
        instance=str(adapter.instance),
        display_name=str(adapter.display_name),
        vocabulary=_vocabulary_words(adapter),
        capabilities=_capabilities_view(adapter.capabilities),
    )


# --------------------------------------------------------------------------- masking

_MASK_TOKEN = re.compile(rf"^{re.escape(HASH_PREFIX)}[0-9a-f]{{{HASH_LENGTH}}}$")


def is_mask_token(value: Any) -> bool:
    """True for values produced by :func:`dag_healer.masking.mask_value`."""
    return isinstance(value, str) and (value == REDACTED or bool(_MASK_TOKEN.match(value)))


def mask_free_text(text: Any) -> str | None:
    """``mask_text`` that keeps ``None``."""
    if text is None:
        return None
    return mask_text(text)


def mask_json(value: Any) -> Any:
    """Apply ``mask_text`` to every string inside a JSON-like value."""
    if isinstance(value, str):
        return mask_text(value)
    if isinstance(value, Mapping):
        return {str(key): mask_json(item) for key, item in value.items()}
    if isinstance(value, (list, tuple)):
        return [mask_json(item) for item in value]
    return to_json_safe(value)


def _pii_columns(contract: Contract | None, dataset: str | None) -> set[str]:
    if contract is None:
        return set()
    datasets = [d for d in contract.datasets if dataset is None or d.name == dataset] or contract.datasets
    return {column.name.lower() for d in datasets for column in d.columns if column.pii}


def remask_sample(
    sample: Any, cfg: HealerConfig, contract: Contract | None, *, salt: str
) -> schemas.Sample | None:
    """Return the stored sample as a :class:`schemas.Sample`, masking anything left raw."""
    if not sample:
        return None
    if isinstance(sample, list):
        dataset, columns, rows = None, [], sample
    elif isinstance(sample, Mapping):
        dataset = sample.get("dataset")
        columns = list(sample.get("columns") or [])
        rows = sample.get("rows") or []
    else:
        return None
    if not isinstance(rows, list):
        rows = []
    settings = masking_settings(cfg.masking)
    columns = [str(column) for column in columns]
    sensitive = _pii_columns(contract, dataset) | {c.lower() for c in settings.columns}
    allowed = {c.lower() for c in settings.allow}

    def mask_cell(column: str, value: Any) -> Any:
        key = column.lower()
        if key in allowed:
            return to_json_safe(value)
        if key in sensitive or is_sensitive_column(column):
            return (
                value if value is None or is_mask_token(value) else mask_value(value, settings.default, salt)
            )
        if isinstance(value, str):
            return value if is_mask_token(value) else mask_text(value)
        return mask_json(value)

    masked_rows: list[dict[str, Any]] = []
    for row in rows[: max(0, settings.sample_rows)]:
        if isinstance(row, Mapping):
            items = [(str(k), v) for k, v in row.items()]
        elif isinstance(row, (list, tuple)) and columns:
            items = list(zip(columns, row, strict=False))
        else:
            continue
        masked_rows.append({column: mask_cell(column, value) for column, value in items})
        for column, _ in items:
            if column not in columns:
                columns.append(column)
    return schemas.Sample(
        dataset=None if dataset is None else str(dataset), columns=columns, rows=masked_rows, masked=True
    )


# --------------------------------------------------------------------------- links


def airflow_base_url(cfg: HealerConfig) -> str:
    """Public base URL of the Healer views, the Airflow UI for Airflow (see
    :func:`dag_healer.links.airflow_base_url`)."""
    return links.airflow_base_url(cfg)


def _native_links(
    cfg: HealerConfig, incident: Incident, adapter: OrchestratorAdapter | None
) -> dict[str, str]:
    """The orchestrator's own links to show for ``incident`` (``{}`` without an adapter, and for Airflow)."""
    if adapter is None:
        return {}
    return links.shown_native_links(cfg, _key_row(incident), adapter=adapter)


def _links(cfg: HealerConfig, incident: Incident, native: dict[str, str]) -> schemas.IncidentLinks:
    return schemas.IncidentLinks(
        task_instance=links.task_instance_url(
            cfg, incident.dag_id, incident.run_id, incident.task_id, incident.map_index
        ),
        dag=links.dag_url(cfg, incident.dag_id),
        healer=links.healer_page_url(cfg),
        native=native,
    )


def incident_links(
    cfg: HealerConfig, incident: Incident, *, adapter: OrchestratorAdapter | None = None
) -> schemas.IncidentLinks:
    """Links to the Healer tab of the task instance, the DAG tab and the Healer page.

    ``native`` adds the orchestrator's own pages (:func:`dag_healer.links.shown_native_links`).
    """
    return _links(cfg, incident, _native_links(cfg, incident, adapter))


def pr_md_url(cfg: HealerConfig, incident: Incident) -> str:
    """URL of the PR.md download route of an incident."""
    return links.pr_md_url(cfg, incident.dag_id, incident.id)


def pr_md_file(cfg: HealerConfig, incident: Incident) -> Path | None:
    """The PR.md file of an incident when it exists under ``<healer_home>/prs``."""
    if not incident.pr_md_path:
        return None
    try:
        root = (cfg.healer_home / "prs").resolve()
        path = Path(incident.pr_md_path)
        if not path.is_absolute():
            path = cfg.healer_home / path
        path = path.resolve()
        path.relative_to(root)
    except (OSError, ValueError):
        log.warning(
            "dag-healer: ignoring PR.md path outside %s for incident %s", cfg.healer_home, incident.id
        )
        return None
    return path if path.is_file() else None


# --------------------------------------------------------------------------- incidents


def incident_summary(
    cfg: HealerConfig, incident: Incident, *, adapter: OrchestratorAdapter | None = None
) -> schemas.IncidentSummary:
    """Summary view of an incident (masked error summary and error name).

    ``links`` is set only when the orchestrator has native links to show.
    """
    native = _native_links(cfg, incident, adapter)
    return schemas.IncidentSummary(
        id=incident.id,
        dag_id=incident.dag_id,
        run_id=incident.run_id,
        task_id=incident.task_id,
        map_index=incident.map_index,
        try_number=incident.try_number,
        status=incident.status,
        category=incident.category,
        confidence=incident.confidence,
        source=incident.source,
        error_summary=mask_text(incident.error_summary or ""),
        created_at=incident.created_at,
        updated_at=incident.updated_at,
        occurrences=incident.occurrences or 1,
        pr_url=incident.pr_url,
        has_pr_md=pr_md_file(cfg, incident) is not None,
        orchestrator=incident.orchestrator or DEFAULT_ORCHESTRATOR,
        error_name=mask_free_text(incident.error_name),
        links=_links(cfg, incident, native) if native else None,
    )


def _violated_rule(value: Any) -> dict[str, Any] | None:
    if value is None or value == "":
        return None
    if isinstance(value, Mapping):
        return mask_json({k: v for k, v in value.items() if k != "violations"})
    return {"rule": mask_text(str(value))}


def _violation_list(value: Any) -> list[dict[str, Any]] | None:
    if isinstance(value, list):
        return [mask_json(item) for item in value if isinstance(item, Mapping)]
    return None


def incident_violations(incident: Incident, events: Iterable[IncidentEvent]) -> list[dict[str, Any]]:
    """Contract violations recorded for an incident.

    Looked up in ``violated_rule["violations"]``, then in the newest timeline event whose
    data has a ``violations`` list, then in ``sample["violations"]``; finally a
    ``violated_rule`` that is itself a violation (``rule`` + ``message``) is returned alone.
    """
    rule = incident.violated_rule
    if isinstance(rule, Mapping):
        found = _violation_list(rule.get("violations"))
        if found is not None:
            return found
    for event in sorted(events, key=lambda e: e.id or 0, reverse=True):
        if isinstance(event.data, Mapping):
            found = _violation_list(event.data.get("violations"))
            if found is not None:
                return found
    if isinstance(incident.sample, Mapping):
        found = _violation_list(incident.sample.get("violations"))
        if found is not None:
            return found
    if isinstance(rule, Mapping) and "rule" in rule and "message" in rule:
        return [mask_json({k: v for k, v in rule.items() if k != "violations"})]
    return []


def _evidence(value: Any) -> list[str]:
    if value is None:
        return []
    if isinstance(value, str):
        return [mask_text(value)]
    if isinstance(value, Mapping):
        return [mask_text(f"{key}: {item}") for key, item in value.items()]
    if isinstance(value, (list, tuple)):
        return [mask_text(item if isinstance(item, str) else str(to_json_safe(item))) for item in value]
    return [mask_text(str(value))]


def pending_commands_for(session: Session, incident_id: str) -> list[Command]:
    """Pending commands of one incident, oldest first."""
    query = (
        select(Command)
        .where(Command.incident_id == incident_id, Command.status == COMMAND_PENDING)
        .order_by(Command.id)
    )
    return list(session.scalars(query))


def incident_detail(
    session: Session,
    cfg: HealerConfig,
    incident: Incident,
    *,
    can_edit: bool,
    contract: Contract | None = None,
    adapter: OrchestratorAdapter | None = None,
) -> schemas.IncidentDetail:
    """Full view of an incident (timeline, alerts, pending commands, links, actions).

    ``retry_task`` is offered only when the adapter can serve it (:func:`retry_available`).
    """
    events = store_repo.list_events(session, incident.id)
    alerts = store_repo.list_alerts(session, incident.id)
    pending = pending_commands_for(session, incident.id)
    summary = incident_summary(cfg, incident, adapter=adapter)
    actions = allowed_actions(
        incident.status,
        can_edit=can_edit,
        has_pending=bool(pending),
        retry_available=retry_available(adapter, incident),
    )
    return schemas.IncidentDetail(
        **summary.model_dump(exclude={"links"}),
        diagnosis=mask_free_text(incident.diagnosis),
        violated_rule=_violated_rule(incident.violated_rule),
        violations=incident_violations(incident, events),
        sample=remask_sample(incident.sample, cfg, contract, salt=incident.id),
        evidence=_evidence(incident.evidence),
        test_path=incident.test_path,
        test_code=incident.test_code,
        diff=incident.diff,
        pytest_output=mask_free_text(incident.pytest_output),
        pr_md_url=pr_md_url(cfg, incident) if summary.has_pr_md else None,
        attempts=incident.attempts or 0,
        tokens_used=incident.tokens_used or 0,
        resolution=incident.resolution,
        resolved_by=incident.resolved_by,
        events=[
            schemas.EventView(
                at=event.at,
                kind=event.kind,
                message=mask_text(event.message or ""),
                actor=event.actor,
                data=mask_json(event.data) if event.data is not None else None,
            )
            for event in events
        ],
        alerts=[
            schemas.AlertView(
                channel=alert.channel,
                recipients=[str(r) for r in (alert.recipients or [])],
                subject=mask_text(alert.subject or ""),
                status=alert.status,
                error=mask_free_text(alert.error),
                sent_at=alert.sent_at,
            )
            for alert in alerts
        ],
        links=incident_links(cfg, incident, adapter=adapter),
        allowed_actions=actions,
        pending_commands=[
            schemas.PendingCommandView(id=c.id, kind=c.kind, created_at=c.created_at, actor=c.actor)
            for c in pending
        ],
        can_edit=can_edit,
        action_warnings=action_warnings(adapter, incident, actions),
    )


def count_incidents(
    session: Session,
    *,
    dag_ids: set[str],
    statuses: list[str] | None = None,
    orchestrator: str | None = None,
) -> int:
    """Number of incidents of ``dag_ids`` (optionally only ``statuses`` and one instance)."""
    if not dag_ids:
        return 0
    query = select(func.count()).select_from(Incident).where(Incident.dag_id.in_(sorted(dag_ids)))
    if statuses:
        query = query.where(Incident.status.in_(statuses))
    if orchestrator is not None:
        query = query.where(Incident.orchestrator == orchestrator)
    return int(session.scalar(query) or 0)


def incident_dag_ids(session: Session, *, orchestrator: str | None = None) -> set[str]:
    """Every DAG id that has at least one incident (of one instance when ``orchestrator`` is set)."""
    query = select(Incident.dag_id).distinct()
    if orchestrator is not None:
        query = query.where(Incident.orchestrator == orchestrator)
    return set(session.scalars(query))


def task_instance_incident(
    session: Session,
    *,
    dag_id: str,
    run_id: str,
    task_id: str,
    map_index: int,
    orchestrator: str | None = None,
) -> Incident | None:
    """Latest incident of a task instance (any try), including deduplicated occurrences."""
    candidates = [
        incident
        for incident in store_repo.list_incidents(
            session, dag_ids={dag_id}, run_id=run_id, task_id=task_id, limit=200, orchestrator=orchestrator
        )
        if incident.map_index == map_index
    ]
    if candidates:
        return max(candidates, key=lambda i: (i.try_number, i.created_at, i.id))
    query = (
        select(IncidentEvent)
        .join(Incident, IncidentEvent.incident_id == Incident.id)
        .where(
            Incident.dag_id == dag_id,
            Incident.task_id == task_id,
            IncidentEvent.kind == "occurrence",
        )
        .order_by(IncidentEvent.id.desc())
        .limit(500)
    )
    if orchestrator is not None:
        query = query.where(Incident.orchestrator == orchestrator)
    for event in session.scalars(query):
        data = event.data if isinstance(event.data, Mapping) else {}
        if data.get("run_id") == run_id and int(data.get("map_index", -1)) == map_index:
            return session.get(Incident, event.incident_id)
    return None


def open_incident_counts(
    session: Session, dag_ids: set[str], *, orchestrator: str | None = None
) -> dict[str, int]:
    """Open incidents per DAG."""
    if not dag_ids:
        return {}
    query = (
        select(Incident.dag_id, func.count())
        .where(Incident.dag_id.in_(sorted(dag_ids)), Incident.status.in_(list(OPEN_STATUSES)))
        .group_by(Incident.dag_id)
    )
    if orchestrator is not None:
        query = query.where(Incident.orchestrator == orchestrator)
    return {dag_id: int(count) for dag_id, count in session.execute(query)}


def last_incidents(
    session: Session, dag_ids: set[str], *, orchestrator: str | None = None
) -> dict[str, Incident]:
    """Most recent incident per DAG."""
    if not dag_ids:
        return {}
    latest = select(Incident.dag_id, func.max(Incident.created_at).label("created_at")).where(
        Incident.dag_id.in_(sorted(dag_ids))
    )
    if orchestrator is not None:
        latest = latest.where(Incident.orchestrator == orchestrator)
    newest = latest.group_by(Incident.dag_id).subquery()
    query = select(Incident).join(
        newest, (Incident.dag_id == newest.c.dag_id) & (Incident.created_at == newest.c.created_at)
    )
    if orchestrator is not None:
        query = query.where(Incident.orchestrator == orchestrator)
    result: dict[str, Incident] = {}
    for incident in session.scalars(query):
        current = result.get(incident.dag_id)
        if current is None or incident.id > current.id:
            result[incident.dag_id] = incident
    return result


# --------------------------------------------------------------------------- contracts


@dataclass(frozen=True)
class ContractRecord:
    """A contract file of the repository with its effective status."""

    contract: Contract
    path: Path
    relpath: str

    @property
    def stale(self) -> bool:
        """Approved, then edited."""
        return is_stale(self.contract)

    @property
    def status(self) -> str:
        """``approved``, ``stale`` or ``draft``."""
        if not self.contract.is_approved:
            return "draft"
        return "stale" if self.stale else "approved"


_contract_cache: dict[str, tuple[int, int, Contract | None]] = {}
_contract_cache_lock = threading.Lock()


def _load_cached(path: Path) -> Contract | None:
    try:
        stat = path.stat()
    except OSError:
        return None
    key = str(path)
    with _contract_cache_lock:
        cached = _contract_cache.get(key)
    if cached is not None and cached[0] == stat.st_mtime_ns and cached[1] == stat.st_size:
        return cached[2]
    try:
        contract: Contract | None = load_contract(path)
    except ContractError:
        log.warning("dag-healer: skipping invalid contract file %s", path.name)
        contract = None
    with _contract_cache_lock:
        _contract_cache[key] = (stat.st_mtime_ns, stat.st_size, contract)
    return contract


def _relpath(cfg: HealerConfig, path: Path) -> str:
    try:
        return path.resolve().relative_to(cfg.repo_root).as_posix()
    except ValueError:
        return path.resolve().as_posix()


def load_contracts(cfg: HealerConfig) -> dict[str, ContractRecord]:
    """Every valid contract of ``cfg.contracts_dir`` by DAG id (``<dag_id>.yaml`` wins)."""
    directory = cfg.contracts_dir
    if not directory.is_dir():
        return {}
    files = sorted([*directory.glob("*.yaml"), *directory.glob("*.yml")], key=lambda p: p.name)
    records: dict[str, ContractRecord] = {}
    for path in files:
        contract = _load_cached(path)
        if contract is None:
            continue
        existing = records.get(contract.dag_id)
        if existing is not None and existing.path.stem == contract.dag_id:
            continue
        records[contract.dag_id] = ContractRecord(contract=contract, path=path, relpath=_relpath(cfg, path))
    return records


def contract_status(record: ContractRecord | None) -> str:
    """``approved`` / ``draft`` / ``stale`` / ``missing``."""
    return "missing" if record is None else record.status


def pending_approval_dags(session: Session, *, orchestrator: str | None = None) -> set[str]:
    """DAG ids with a pending ``approve_contract`` command (for one instance's worker when set)."""
    query = select(Command.dag_id).where(
        Command.kind == "approve_contract", Command.status == COMMAND_PENDING
    )
    if orchestrator is not None:
        query = query.where(Command.orchestrator == orchestrator)
    return set(session.scalars(query))


def contract_view(
    record: ContractRecord, *, can_approve: bool, pending_approval: bool
) -> schemas.ContractView:
    """UI view of a contract file."""
    contract = record.contract
    return schemas.ContractView(
        dag_id=contract.dag_id,
        status=record.status,
        approved_by=contract.approved_by,
        approved_at=contract.approved_at,
        stale=record.stale,
        path=record.relpath,
        datasets=[
            schemas.DatasetView(
                name=dataset.name,
                task_id=dataset.task_id,
                locator=schemas.LocatorView(type=dataset.locator.type, path=dataset.locator.path),
                columns=[
                    schemas.ColumnView(
                        name=column.name,
                        dtype=column.dtype,
                        nullable=column.nullable,
                        min=column.min,
                        max=column.max,
                        pii=column.pii,
                        allowed_values=column.allowed_values,
                        note=column.note,
                    )
                    for column in dataset.columns
                ],
                row_count=schemas.RowCountView(min=dataset.row_count.min, max=dataset.row_count.max),
            )
            for dataset in contract.datasets
        ],
        can_approve=can_approve and record.status != "approved" and not pending_approval,
        pending_approval=pending_approval,
    )


__all__ = [
    "ALLOWED_ACTIONS_BY_STATUS",
    "RESOLUTION_ACTIONS",
    "ContractRecord",
    "action_warnings",
    "actions_for_status",
    "airflow_base_url",
    "allowed_actions",
    "contract_status",
    "contract_view",
    "count_incidents",
    "incident_dag_ids",
    "incident_detail",
    "incident_links",
    "incident_summary",
    "incident_violations",
    "is_mask_token",
    "last_incidents",
    "load_contracts",
    "mask_free_text",
    "mask_json",
    "open_incident_counts",
    "orchestrator_view",
    "pending_approval_dags",
    "pending_commands_for",
    "pr_md_file",
    "pr_md_url",
    "remask_sample",
    "retry_available",
    "task_instance_incident",
]
