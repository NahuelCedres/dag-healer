"""``POST /api/v1/events/task-failed``: fast-path failure events from task runners.

Authenticated with the shared ``X-Healer-Token`` (no Airflow user: the listener runs in
task processes). The event is only a hint: it is stored as ``pending_check`` and the worker
confirms the failure through the Airflow REST API before acting, which neutralises forged
events. Rate-limited in-process (120 requests per minute per client address).
"""

from __future__ import annotations

import logging

from fastapi import APIRouter, Depends, status
from sqlalchemy.orm import Session

from dag_healer.api import schemas
from dag_healer.api.deps import API_PREFIX, get_config, get_session, ingest_guard, require_ingest_token
from dag_healer.config import HealerConfig
from dag_healer.ingest import MAX_ERROR_CHARS, TRUNCATION_MARKER
from dag_healer.masking import mask_text
from dag_healer.store import repo as store_repo

log = logging.getLogger(__name__)

router = APIRouter(prefix=API_PREFIX, tags=["events"])

EVENT_SOURCES = ("listener", "callback")


def _tail(text: str, limit: int = MAX_ERROR_CHARS) -> str:
    """Keep the end of a long text (where Python puts the exception) within ``limit``."""
    if len(text) <= limit:
        return text
    return TRUNCATION_MARKER + text[-(limit - len(TRUNCATION_MARKER)) :]


@router.post(
    "/events/task-failed",
    status_code=status.HTTP_202_ACCEPTED,
    response_model=schemas.EventAccepted,
    dependencies=[Depends(ingest_guard), Depends(require_ingest_token)],
)
def task_failed(
    event: schemas.TaskFailedEvent,
    cfg: HealerConfig = Depends(get_config),
    session: Session = Depends(get_session),
) -> schemas.EventAccepted:
    """Record a task failure reported by the listener or the failure callback."""
    if not cfg.is_monitored(event.dag_id):
        log.debug("dag-healer: ignoring failure event of unmonitored DAG %s", event.dag_id)
        return schemas.EventAccepted(accepted=False, pending_event_id=None)
    source = event.source if event.source in EVENT_SOURCES else "listener"
    pending = store_repo.add_pending_event(
        session,
        dag_id=event.dag_id,
        run_id=event.run_id,
        task_id=event.task_id,
        map_index=event.map_index,
        try_number=event.try_number,
        payload={
            "error": _tail(mask_text(event.error or "")),
            "source": source,
            "state": event.state,
            "max_tries": event.max_tries,
            "hostname": mask_text(event.hostname) if event.hostname else None,
            "reported_at": event.reported_at,
        },
    )
    session.commit()
    log.info(
        "dag-healer: %s reported a failure of %s.%s (run %s, try %s); pending event %s",
        source,
        event.dag_id,
        event.task_id,
        event.run_id,
        event.try_number,
        pending.id,
    )
    return schemas.EventAccepted(accepted=True, pending_event_id=pending.id)
