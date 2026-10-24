"""``GET /api/v1/dags``: the DAGs dag-healer watches, with contract and incident status."""

from __future__ import annotations

from typing import Any

from fastapi import APIRouter, Depends
from sqlalchemy.orm import Session

from dag_healer.api import auth, schemas, views
from dag_healer.api.deps import (
    API_PREFIX,
    get_adapter,
    get_config,
    get_instance,
    get_session,
    readable_dag_ids,
)
from dag_healer.config import HealerConfig
from dag_healer.core.ports import OrchestratorAdapter

router = APIRouter(prefix=API_PREFIX, tags=["dags"])


@router.get("/dags", response_model=schemas.DagList)
def list_dags(
    user: Any = Depends(auth.require_authenticated),
    authorizer: auth.Authorizer = Depends(auth.get_authorizer),
    cfg: HealerConfig = Depends(get_config),
    adapter: OrchestratorAdapter | None = Depends(get_adapter),
    instance: str = Depends(get_instance),
    session: Session = Depends(get_session),
) -> schemas.DagList:
    """Readable DAGs that are monitored, have a contract or have incidents (of this instance)."""
    contracts = views.load_contracts(cfg)
    with_incidents = views.incident_dag_ids(session, orchestrator=instance)
    readable = readable_dag_ids(authorizer, user, set(contracts) | with_incidents)
    dag_ids = {
        dag_id
        for dag_id in readable
        if cfg.is_monitored(dag_id) or dag_id in contracts or dag_id in with_incidents
    }
    open_counts = views.open_incident_counts(session, dag_ids & with_incidents, orchestrator=instance)
    latest = views.last_incidents(session, dag_ids & with_incidents, orchestrator=instance)
    dags = []
    for dag_id in sorted(dag_ids):
        last = latest.get(dag_id)
        summary = views.incident_summary(cfg, last, adapter=adapter) if last is not None else None
        dags.append(
            schemas.DagSummary(
                dag_id=dag_id,
                monitored=cfg.is_monitored(dag_id),
                contract_status=views.contract_status(contracts.get(dag_id)),
                open_incidents=open_counts.get(dag_id, 0),
                last_incident=summary,
            )
        )
    return schemas.DagList(dags=dags)
