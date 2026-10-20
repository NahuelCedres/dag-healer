"""Incident routes: lists, details, PR.md and the human actions (resolve / retry)."""

from __future__ import annotations

import logging
from typing import Any

from fastapi import APIRouter, Depends, HTTPException, Query, status
from fastapi.responses import Response
from sqlalchemy.orm import Session

from dag_healer.api import auth, schemas, views
from dag_healer.api.deps import API_PREFIX, get_config, get_session, readable_dag_ids
from dag_healer.config import HealerConfig
from dag_healer.masking import mask_text
from dag_healer.store import repo as store_repo
from dag_healer.store.models import ALL_STATUSES, Incident

log = logging.getLogger(__name__)

router = APIRouter(prefix=API_PREFIX, tags=["incidents"])

DEFAULT_LIMIT = 100
MAX_LIMIT = 500

read_ti = auth.require_dag_access("GET", "TASK_INSTANCE")
edit_ti = auth.require_dag_access("PUT", "TASK_INSTANCE")


def parse_statuses(value: str | None) -> list[str] | None:
    """Parse ``status=a,b`` (422 for unknown statuses)."""
    if value is None or not value.strip():
        return None
    statuses = [part.strip() for part in value.split(",") if part.strip()]
    unknown = sorted(set(statuses) - set(ALL_STATUSES))
    if unknown:
        raise HTTPException(status_code=422, detail=f"Unknown status: {', '.join(unknown)}")
    return statuses


def get_dag_incident(session: Session, dag_id: str, incident_id: str) -> Incident:
    """The incident ``incident_id`` of ``dag_id``; 404 when missing or of another DAG."""
    incident = store_repo.get_incident(session, incident_id)
    if incident is None or incident.dag_id != dag_id:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Incident not found")
    return incident


# --------------------------------------------------------------------------- reads


@router.get("/incidents", response_model=schemas.IncidentList)
def list_incidents(
    status_filter: str | None = Query(default=None, alias="status"),
    dag_id: str | None = Query(default=None),
    limit: int = Query(default=DEFAULT_LIMIT, ge=1, le=MAX_LIMIT),
    user: Any = Depends(auth.require_authenticated),
    authorizer: auth.Authorizer = Depends(auth.get_authorizer),
    cfg: HealerConfig = Depends(get_config),
    session: Session = Depends(get_session),
) -> schemas.IncidentList:
    """Incidents of the DAGs the user can read, newest first."""
    statuses = parse_statuses(status_filter)
    readable = readable_dag_ids(authorizer, user, views.incident_dag_ids(session))
    if dag_id is not None:
        readable &= {dag_id}
    incidents = store_repo.list_incidents(session, dag_ids=readable, statuses=statuses, limit=limit)
    return schemas.IncidentList(
        incidents=[views.incident_summary(cfg, incident) for incident in incidents],
        total=views.count_incidents(session, dag_ids=readable, statuses=statuses),
    )


@router.get("/dags/{dag_id}/incidents", response_model=schemas.DagIncidents)
def list_dag_incidents(
    dag_id: str,
    status_filter: str | None = Query(default=None, alias="status"),
    limit: int = Query(default=DEFAULT_LIMIT, ge=1, le=MAX_LIMIT),
    user: Any = Depends(read_ti),
    authorizer: auth.Authorizer = Depends(auth.get_authorizer),
    cfg: HealerConfig = Depends(get_config),
    session: Session = Depends(get_session),
) -> schemas.DagIncidents:
    """Incidents of one DAG with its contract and the caller's edit permission."""
    statuses = parse_statuses(status_filter)
    incidents = store_repo.list_incidents(session, dag_ids={dag_id}, statuses=statuses, limit=limit)
    record = views.load_contracts(cfg).get(dag_id)
    contract = None
    if record is not None:
        contract = views.contract_view(
            record,
            can_approve=authorizer.can_edit_dag(user, dag_id, None),
            pending_approval=dag_id in views.pending_approval_dags(session),
        )
    return schemas.DagIncidents(
        incidents=[views.incident_summary(cfg, incident) for incident in incidents],
        contract=contract,
        monitored=cfg.is_monitored(dag_id),
        can_edit=authorizer.can_edit_dag(user, dag_id),
    )


@router.get("/dags/{dag_id}/incidents/{incident_id}", response_model=schemas.IncidentDetail)
def get_incident(
    dag_id: str,
    incident_id: str,
    user: Any = Depends(read_ti),
    authorizer: auth.Authorizer = Depends(auth.get_authorizer),
    cfg: HealerConfig = Depends(get_config),
    session: Session = Depends(get_session),
) -> schemas.IncidentDetail:
    """One incident with timeline, alerts, sample and the actions the caller may take."""
    incident = get_dag_incident(session, dag_id, incident_id)
    record = views.load_contracts(cfg).get(dag_id)
    return views.incident_detail(
        session,
        cfg,
        incident,
        can_edit=authorizer.can_edit_dag(user, dag_id),
        contract=record.contract if record else None,
    )


@router.get(
    "/dags/{dag_id}/incidents/{incident_id}/pr.md",
    response_class=Response,
    responses={200: {"content": {"text/markdown": {}}}},
)
def get_pr_md(
    dag_id: str,
    incident_id: str,
    user: Any = Depends(read_ti),
    cfg: HealerConfig = Depends(get_config),
    session: Session = Depends(get_session),
) -> Response:
    """The PR.md written for an incident whose fix could not be opened as a pull request."""
    incident = get_dag_incident(session, dag_id, incident_id)
    path = views.pr_md_file(cfg, incident)
    if path is None:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="PR.md not found")
    try:
        text = path.read_text(encoding="utf-8")
    except OSError:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="PR.md not found") from None
    return Response(
        content=text,
        media_type="text/markdown; charset=utf-8",
        headers={
            "Content-Disposition": f'inline; filename="PR-{incident.id[:8]}.md"',
            "X-Content-Type-Options": "nosniff",
            "Cache-Control": "no-store",
        },
    )


@router.get("/dags/{dag_id}/task-instance", response_model=schemas.TaskInstanceIncident)
def get_task_instance_incident(
    dag_id: str,
    run_id: str = Query(min_length=1),
    task_id: str = Query(min_length=1),
    map_index: int = Query(default=-1),
    user: Any = Depends(read_ti),
    authorizer: auth.Authorizer = Depends(auth.get_authorizer),
    cfg: HealerConfig = Depends(get_config),
    session: Session = Depends(get_session),
) -> schemas.TaskInstanceIncident:
    """Latest incident of a task instance (any try), or ``null``."""
    incident = views.task_instance_incident(
        session, dag_id=dag_id, run_id=run_id, task_id=task_id, map_index=map_index
    )
    if incident is None:
        return schemas.TaskInstanceIncident(incident=None)
    record = views.load_contracts(cfg).get(dag_id)
    detail = views.incident_detail(
        session,
        cfg,
        incident,
        can_edit=authorizer.can_edit_dag(user, dag_id),
        contract=record.contract if record else None,
    )
    return schemas.TaskInstanceIncident(incident=detail)


# --------------------------------------------------------------------------- actions


def enqueue_incident_action(
    session: Session,
    incident: Incident,
    *,
    action: str,
    kind: str,
    payload: dict[str, Any],
    actor: str,
) -> schemas.CommandAccepted:
    """Queue a command for the worker after checking the incident status.

    409 when ``action`` is not allowed in the current status or another command is
    pending for the incident; the same command requested twice returns the pending one.
    """
    if action not in views.actions_for_status(incident.status):
        raise HTTPException(
            status_code=status.HTTP_409_CONFLICT,
            detail=f"Action {action!r} is not allowed while the incident is {incident.status!r}",
        )
    pending = views.pending_commands_for(session, incident.id)
    for command in pending:
        same_resolution = (command.payload or {}).get("resolution") == payload.get("resolution")
        if command.kind == kind and same_resolution:
            return schemas.CommandAccepted(command_id=command.id, status=command.status)
    if pending:
        raise HTTPException(
            status_code=status.HTTP_409_CONFLICT,
            detail="Another action is already pending for this incident",
        )
    command = store_repo.enqueue_command(
        session, kind=kind, dag_id=incident.dag_id, incident_id=incident.id, payload=payload, actor=actor
    )
    store_repo.add_event(
        session,
        incident,
        "command_queued",
        f"{actor} requested {action.replace('_', ' ')}",
        data={"command_id": command.id, "kind": kind, "action": action},
        actor=actor,
    )
    session.commit()
    log.info("dag-healer: %s queued %s for incident %s (command %s)", actor, kind, incident.id, command.id)
    return schemas.CommandAccepted(command_id=command.id, status=command.status)


def _note(body: schemas.NoteBody | schemas.ResolveBody | None) -> str | None:
    note = body.note if body is not None else None
    return mask_text(note.strip()) if note and note.strip() else None


@router.post(
    "/dags/{dag_id}/incidents/{incident_id}/resolve",
    status_code=status.HTTP_202_ACCEPTED,
    response_model=schemas.CommandAccepted,
)
def resolve_incident(
    dag_id: str,
    incident_id: str,
    body: schemas.ResolveBody,
    user: Any = Depends(edit_ti),
    session: Session = Depends(get_session),
) -> schemas.CommandAccepted:
    """Resolve an incident as a data problem, a code problem, or ignore it."""
    incident = get_dag_incident(session, dag_id, incident_id)
    return enqueue_incident_action(
        session,
        incident,
        action=views.RESOLUTION_ACTIONS[body.resolution],
        kind="resolve",
        payload={"resolution": body.resolution, "note": _note(body)},
        actor=auth.user_name(user),
    )


@router.post(
    "/dags/{dag_id}/incidents/{incident_id}/retry-repair",
    status_code=status.HTTP_202_ACCEPTED,
    response_model=schemas.CommandAccepted,
)
def retry_repair(
    dag_id: str,
    incident_id: str,
    body: schemas.NoteBody | None = None,
    user: Any = Depends(edit_ti),
    session: Session = Depends(get_session),
) -> schemas.CommandAccepted:
    """Ask the agent for a new repair attempt."""
    incident = get_dag_incident(session, dag_id, incident_id)
    return enqueue_incident_action(
        session,
        incident,
        action="retry_repair",
        kind="retry_repair",
        payload={"note": _note(body)},
        actor=auth.user_name(user),
    )


@router.post(
    "/dags/{dag_id}/incidents/{incident_id}/retry-task",
    status_code=status.HTTP_202_ACCEPTED,
    response_model=schemas.CommandAccepted,
)
def retry_task(
    dag_id: str,
    incident_id: str,
    body: schemas.NoteBody | None = None,
    user: Any = Depends(edit_ti),
    session: Session = Depends(get_session),
) -> schemas.CommandAccepted:
    """Clear the failed task instance in Airflow so that it runs again."""
    incident = get_dag_incident(session, dag_id, incident_id)
    return enqueue_incident_action(
        session,
        incident,
        action="retry_task",
        kind="retry_task",
        payload={
            "note": _note(body),
            "run_id": incident.run_id,
            "task_id": incident.task_id,
            "map_index": incident.map_index,
        },
        actor=auth.user_name(user),
    )
