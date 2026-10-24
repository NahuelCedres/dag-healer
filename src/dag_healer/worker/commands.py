"""Apply the human actions queued in ``healer_commands`` (the worker is their only executor).

The API and the CLI never change incidents or repository files themselves: they queue a
:class:`~dag_healer.store.models.Command` and the worker applies it here, oldest first, each
in its own transaction. Every command ends ``applied`` or ``failed`` (with a masked
``error``) and adds a ``command`` event to the incident timeline.

========================  =====================================================================
Command (payload)         Effect
========================  =====================================================================
``resolve`` data|code     ``resolution`` = the category, ``resolved_by`` = actor, status
                          ``resumed``: the pipeline then skips the classification and runs the
                          DATA (report) or CODE (repair) branch.
``resolve`` ignore        ``resolution`` = ``ignore``, status ``ignored``.
``retry_repair``          ``resolution`` = ``code``, status ``resumed``: a new repair attempt.
``retry_task``            the orchestrator runs the failed step again through the adapter's
                          remediator (Airflow: ``clearTaskInstances`` for the failed task
                          instance and its failed or upstream_failed downstream tasks); the
                          status does not change (for CODE incidents the fix stays on its
                          branch until it is merged).
``approve_contract``      approves ``<contracts dir>/<dag_id>.yaml`` in the working tree
                          (refused when the file changed since the user reviewed it, see
                          ``source_hash``) and caches the state in ``healer_contracts``.
========================  =====================================================================

A command that is no longer allowed for the incident's current status (see
:data:`dag_healer.api.views.ALLOWED_ACTIONS_BY_STATUS`) fails with a clear error. The worker
applies only the commands of its own orchestrator instance.
"""

from __future__ import annotations

import logging
from collections.abc import Mapping
from dataclasses import asdict, dataclass, field
from datetime import datetime
from pathlib import Path
from typing import TYPE_CHECKING, Any

from sqlalchemy.orm import Session

from dag_healer.core.errors import NotSupported
from dag_healer.core.ids import DEFAULT_ORCHESTRATOR, AttemptRef
from dag_healer.core.vocab import RemediationKind
from dag_healer.masking import mask_text
from dag_healer.orchestrators.compat import as_adapter
from dag_healer.store import repo as store_repo
from dag_healer.store.models import (
    COMMAND_APPLIED,
    COMMAND_FAILED,
    COMMAND_PENDING,
    IGNORED,
    RESUMED,
    Command,
    Incident,
)
from dag_healer.worker.pipeline import (
    SessionFactory,
    _cause_text,
    _clip,
    _error_text,
    _instance,
    _vocabulary,
    default_session_factory,
    transition,
)

if TYPE_CHECKING:
    from dag_healer.config import HealerConfig
    from dag_healer.core.ports import OrchestratorAdapter

logger = logging.getLogger(__name__)

RESOLUTIONS: tuple[str, ...] = ("data", "code", "ignore")


class CommandFailed(RuntimeError):
    """A command cannot be applied; the message is shown to the user (masked)."""


@dataclass
class CommandOutcome:
    """What happened to one command."""

    command_id: int
    kind: str
    dag_id: str
    incident_id: str | None
    status: str
    message: str
    data: dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> dict[str, Any]:
        """JSON-safe dict."""
        return asdict(self)


def _allowed_actions(status: str) -> tuple[str, ...]:
    from dag_healer.api.views import ALLOWED_ACTIONS_BY_STATUS

    return tuple(ALLOWED_ACTIONS_BY_STATUS.get(status, ()))


def _action_name(command: Mapping[str, Any]) -> str:
    if command["kind"] == "resolve":
        resolution = str((command.get("payload") or {}).get("resolution") or "")
        return {"data": "resolve_data", "code": "resolve_code", "ignore": "ignore"}.get(resolution, "resolve")
    return str(command["kind"])


def _note(payload: Mapping[str, Any]) -> str | None:
    note = payload.get("note")
    if not isinstance(note, str) or not note.strip():
        return None
    return _clip(note, 1000) or None


def _incident_for(session: Session, command: Mapping[str, Any], action: str) -> Incident:
    """The incident of ``command``, checked against the actions allowed in its status."""
    incident_id = command.get("incident_id")
    if not incident_id:
        raise CommandFailed(f"The {command['kind']} command has no incident.")
    incident = store_repo.get_incident(session, str(incident_id))
    if incident is None:
        raise CommandFailed(f"Incident {incident_id} does not exist.")
    if incident.dag_id != command["dag_id"]:
        raise CommandFailed(f"Incident {incident_id} does not belong to DAG {command['dag_id']}.")
    if action not in _allowed_actions(incident.status):
        raise CommandFailed(
            f"{action.replace('_', ' ').capitalize()} is not allowed while the incident is {incident.status}."
        )
    return incident


def _finish(
    session: Session,
    command_id: int,
    status: str,
    message: str,
    *,
    incident: Incident | None = None,
    actor: str,
    data: dict[str, Any] | None = None,
) -> bool:
    """Mark the command and add the ``command`` timeline event; False if it was already handled."""
    command = session.get(Command, command_id)
    if command is None or command.status != COMMAND_PENDING:
        return False
    store_repo.mark_command(
        session, command, status, None if status == COMMAND_APPLIED else _clip(message, 2000)
    )
    if incident is not None:
        store_repo.add_event(
            session,
            incident,
            "command",
            _clip(message),
            data={"command_id": command_id, "kind": command.kind, "status": status, **(data or {})},
            actor=actor,
        )
    return True


# --------------------------------------------------------------------------- handlers


def _resolve(session: Session, command: Mapping[str, Any]) -> tuple[str, Incident, dict[str, Any]]:
    payload = command.get("payload") or {}
    resolution = str(payload.get("resolution") or "")
    if resolution not in RESOLUTIONS:
        raise CommandFailed(f"Unknown resolution {resolution!r}; expected data, code or ignore.")
    actor = command["actor"]
    incident = _incident_for(session, command, _action_name(command))
    note = _note(payload)
    suffix = f" Note: {note}" if note else ""
    incident.resolution = resolution
    incident.resolved_by = actor
    data = {"resolution": resolution, "command_id": command["id"]}
    if resolution == "ignore":
        transition(session, incident, IGNORED, f"Ignored by {actor}.{suffix}", actor=actor, data=data)
        return f"{actor} ignored the incident.{suffix}", incident, data
    transition(
        session,
        incident,
        RESUMED,
        f"Resolved as {resolution} by {actor}; resuming the {resolution} branch.{suffix}",
        actor=actor,
        data=data,
    )
    return f"{actor} resolved the incident as {resolution}.{suffix}", incident, data


def _retry_repair(session: Session, command: Mapping[str, Any]) -> tuple[str, Incident, dict[str, Any]]:
    payload = command.get("payload") or {}
    actor = command["actor"]
    incident = _incident_for(session, command, "retry_repair")
    note = _note(payload)
    suffix = f" Note: {note}" if note else ""
    if incident.resolution != "code":
        incident.resolution = "code"
        incident.resolved_by = actor
    data = {"command_id": command["id"]}
    transition(
        session, incident, RESUMED, f"{actor} requested a new repair attempt.{suffix}", actor=actor, data=data
    )
    return f"{actor} requested a new repair attempt.{suffix}", incident, data


def _apply_incident_command(
    cfg: HealerConfig, sf: SessionFactory, command: Mapping[str, Any]
) -> CommandOutcome:
    handler = _resolve if command["kind"] == "resolve" else _retry_repair
    with sf() as session:
        message, incident, data = handler(session, command)
        _finish(
            session,
            command["id"],
            COMMAND_APPLIED,
            message,
            incident=incident,
            actor=command["actor"],
            data=data,
        )
    return _outcome(command, COMMAND_APPLIED, message, data)


def _remediation_verb(kind: RemediationKind, words: Mapping[str, str]) -> str:
    """What the orchestrator was asked to do (``clear the task instance``), for error texts."""
    if kind is RemediationKind.RESUME_RUN:
        return f"resume the {words['run']}"
    if kind is RemediationKind.RERUN_PIPELINE:
        return f"start a new {words['run']}"
    return f"clear the {words['step']} instance"


def _retry_task(
    cfg: HealerConfig, sf: SessionFactory, adapter: OrchestratorAdapter | None, command: Mapping[str, Any]
) -> CommandOutcome:
    payload = command.get("payload") or {}
    with sf() as session:
        incident = _incident_for(session, command, "retry_task")
        run_id = str(payload.get("run_id") or incident.run_id)
        task_id = str(payload.get("task_id") or incident.task_id)
        map_index = int(
            payload.get("map_index") if payload.get("map_index") is not None else incident.map_index
        )
        category = incident.category
        branch_note = incident.pr_url or incident.pr_md_path
        native = dict(incident.native_ref or {})
        orchestrator = incident.orchestrator or DEFAULT_ORCHESTRATOR
        try_number = int(incident.try_number)
        incident_id = str(incident.id)
    words = _vocabulary(adapter)
    if adapter is None:
        raise CommandFailed(
            f"No {words['product']} API client is configured, so the {words['step']} cannot be cleared."
        )
    remediator = adapter.remediator
    if remediator is None:
        raise CommandFailed(f"{adapter.display_name} offers no way to run a failed {words['step']} again.")
    ref = AttemptRef(orchestrator, str(command["dag_id"]), run_id, task_id, map_index, try_number)
    kinds = remediator.available(ref, native)
    if not kinds:
        raise CommandFailed(f"{words['retry_action']} is not available for this incident.")
    kind = kinds[0]
    actor = str(command["actor"])
    try:
        result = remediator.remediate(
            ref, native, kind, idempotency_key=f"healer-{command['id']}-{incident_id[:16]}", actor=actor
        )
    except NotSupported as exc:
        raise CommandFailed(f"{words['retry_action']} is not available for this incident: {exc}") from exc
    except Exception as exc:  # orchestrator errors (the adapter's translation of the original)
        verb = _remediation_verb(kind, words)
        raise CommandFailed(f"{words['product']} refused to {verb}: {_cause_text(exc)}") from exc
    if not result.accepted:
        verb = _remediation_verb(kind, words)
        raise CommandFailed(result.detail or f"{words['product']} did not accept the request to {verb}.")
    message = result.detail
    if category == "code" and branch_note:
        message += f" The proposed fix is not merged yet, so the {words['step']} runs the current code."
    for warning in result.warnings:
        message += f" Warning: {warning}"
    note = _note(payload)
    if note:
        message += f" Note: {note}"
    data: dict[str, Any] = {"run_id": run_id, "task_id": task_id, "map_index": map_index}
    if "cleared" in result.native:
        data["cleared"] = result.native.get("cleared")
    if result.kind is not RemediationKind.RETRY_STEP:
        data["remediation"] = result.kind.value
    if result.warnings:
        data["warnings"] = [_clip(warning, 500) for warning in result.warnings]
    with sf() as session:
        incident = store_repo.get_incident(session, str(command["incident_id"]))
        _finish(session, command["id"], COMMAND_APPLIED, message, incident=incident, actor=actor, data=data)
    logger.info("dag-healer worker: %s", _clip(message, 500))
    return _outcome(command, COMMAND_APPLIED, message, data)


def _contract_file(cfg: HealerConfig, command: Mapping[str, Any]) -> Path:
    """The contract file to approve: the payload ``path`` when it is inside the contracts dir."""
    from dag_healer.contracts.io import contract_path

    try:
        default = contract_path(cfg.contracts_dir, str(command["dag_id"]))
    except ValueError as exc:
        raise CommandFailed(str(exc)) from exc
    relpath = (command.get("payload") or {}).get("path")
    if not isinstance(relpath, str) or not relpath.strip():
        return default
    candidate = (cfg.repo_root / relpath).resolve()
    try:
        candidate.relative_to(cfg.contracts_dir)
    except ValueError:
        logger.warning("dag-healer worker: ignoring contract path %r outside %s", relpath, cfg.contracts_dir)
        return default
    if candidate.suffix not in (".yaml", ".yml"):
        return default
    return candidate


def _approve_contract(cfg: HealerConfig, sf: SessionFactory, command: Mapping[str, Any]) -> CommandOutcome:
    from dag_healer.contracts.io import (
        approve_contract,
        compute_source_hash,
        is_stale,
        load_contract,
        save_contract,
    )
    from dag_healer.contracts.schema import ContractError

    payload = command.get("payload") or {}
    dag_id = str(command["dag_id"])
    actor = str(command["actor"])
    path = _contract_file(cfg, command)
    if not path.is_file():
        raise CommandFailed(f"There is no contract file for DAG {dag_id} ({path.name}).")
    try:
        contract = load_contract(path)
    except ContractError as exc:
        raise CommandFailed(f"The contract file is invalid: {_clip(exc, 500)}") from exc
    if contract.dag_id != dag_id:
        raise CommandFailed(f"{path.name} holds the contract of DAG {contract.dag_id}, not {dag_id}.")
    reviewed = payload.get("source_hash")
    if contract.is_approved and not is_stale(contract):
        message = f"The contract of {dag_id} was already approved by {contract.approved_by}."
        approved = contract
    else:
        current = compute_source_hash(contract)
        if reviewed and reviewed != current:
            raise CommandFailed(
                f"The contract of {dag_id} changed after it was reviewed; review it again before approving."
            )
        approved = approve_contract(contract, actor)
        save_contract(approved, path)
        message = f"{actor} approved the contract of {dag_id} ({len(approved.datasets)} dataset(s))."
    try:
        relpath = path.resolve().relative_to(cfg.repo_root).as_posix()
    except ValueError:
        relpath = path.as_posix()
    note = _note(payload)
    if note:
        message += f" Note: {note}"
    data = {"path": relpath, "source_hash": approved.source_hash}
    with sf() as session:
        store_repo.upsert_contract_state(
            session,
            dag_id=dag_id,
            status=approved.status,
            path=str(path),
            approved_by=approved.approved_by,
            approved_at=approved.approved_at,
            source_hash=approved.source_hash,
            orchestrator=str(command.get("orchestrator") or DEFAULT_ORCHESTRATOR),
        )
        _finish(session, command["id"], COMMAND_APPLIED, message, actor=actor, data=data)
    logger.info("dag-healer worker: %s", _clip(message, 500))
    return _outcome(command, COMMAND_APPLIED, message, data)


# --------------------------------------------------------------------------- driver


def _outcome(
    command: Mapping[str, Any], status: str, message: str, data: dict[str, Any] | None = None
) -> CommandOutcome:
    return CommandOutcome(
        command_id=int(command["id"]),
        kind=str(command["kind"]),
        dag_id=str(command["dag_id"]),
        incident_id=command.get("incident_id"),
        status=status,
        message=message,
        data=dict(data or {}),
    )


def _fail(sf: SessionFactory, command: Mapping[str, Any], message: str) -> CommandOutcome:
    text = _clip(message, 2000)
    try:
        with sf() as session:
            incident = None
            if command.get("incident_id"):
                incident = store_repo.get_incident(session, str(command["incident_id"]))
            _finish(
                session, int(command["id"]), COMMAND_FAILED, text, incident=incident, actor=command["actor"]
            )
    except Exception as exc:  # the store is failing: the command stays pending and is retried
        logger.error(
            "dag-healer worker: could not record the failure of command %s: %s",
            command["id"],
            _error_text(exc),
        )
    logger.warning("dag-healer worker: command %s (%s) failed: %s", command["id"], command["kind"], text)
    return _outcome(command, COMMAND_FAILED, text)


def apply_command(
    cfg: HealerConfig,
    session_factory: SessionFactory,
    client: Any | None,
    command: Mapping[str, Any],
    *,
    adapter: OrchestratorAdapter | None = None,
) -> CommandOutcome:
    """Apply one command (``Command.to_dict()`` shape); never raises.

    ``adapter`` wins; otherwise ``client`` (a phase-5 Airflow REST client or a test double) is
    wrapped by :func:`~dag_healer.orchestrators.compat.as_adapter`. Without either a
    ``retry_task`` command fails; the other commands need no orchestrator.
    """
    kind = command.get("kind")
    try:
        if kind in ("resolve", "retry_repair"):
            return _apply_incident_command(cfg, session_factory, command)
        if kind == "retry_task":
            resolved = adapter if adapter is not None else as_adapter(cfg, client)
            return _retry_task(cfg, session_factory, resolved, command)
        if kind == "approve_contract":
            return _approve_contract(cfg, session_factory, command)
        raise CommandFailed(f"Unknown command kind {kind!r}.")
    except CommandFailed as exc:
        return _fail(session_factory, command, mask_text(str(exc)))
    except Exception as exc:  # noqa: BLE001 - one broken command never stops the worker
        logger.debug("command %s failed unexpectedly", command.get("id"), exc_info=True)
        return _fail(session_factory, command, f"Unexpected error: {_error_text(exc)}")


def apply_commands(
    cfg: HealerConfig,
    session_factory: SessionFactory | None,
    client: Any | None,
    *,
    now: datetime | None = None,
    limit: int = 100,
    adapter: OrchestratorAdapter | None = None,
) -> list[CommandOutcome]:
    """Apply every pending command of this orchestrator instance, oldest first.

    The instance is ``adapter.instance`` (``adapter`` wins over ``client``, see
    :func:`apply_command`), else the configured one. Returns one outcome per command.
    """
    factory = session_factory or default_session_factory(cfg)
    resolved = adapter if adapter is not None else as_adapter(cfg, client)
    instance = _instance(cfg, resolved)
    with factory() as session:
        commands = [
            command.to_dict()
            for command in store_repo.pending_commands(session, limit=limit, orchestrator=instance)
        ]
    outcomes = [apply_command(cfg, factory, client, command, adapter=resolved) for command in commands]
    if outcomes:
        applied = sum(1 for outcome in outcomes if outcome.status == COMMAND_APPLIED)
        logger.info("dag-healer worker: applied %d of %d pending command(s)", applied, len(outcomes))
    return outcomes


__all__ = ["CommandFailed", "CommandOutcome", "apply_command", "apply_commands"]
