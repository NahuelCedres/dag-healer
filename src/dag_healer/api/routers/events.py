"""``POST /api/v1/events/...``: fast-path failure hints pushed to dag-healer.

* ``POST /events/task-failed``: the Airflow listener and failure callback, sent from task
  processes by :mod:`dag_healer.ingest`. Rate-limited in-process (120 requests per minute
  per client address).
* ``POST /events/{channel}``: the other push channels of the configured orchestrator
  adapter (for example ``eventbridge``). The adapter's push intake turns the payload into
  failure hints without any I/O. Rate-limited per channel and ingest token (120 requests per
  minute), because the senders of a channel share their source addresses.

Both are authenticated with the shared ``X-Healer-Token`` (no Airflow user: the listener runs
in task processes) and exempt from the CSRF checks. An event is only a hint: it is stored as
``pending_check`` (error text masked) and the worker confirms the failure through the
orchestrator API before acting, which neutralises forged events. Only the configured
orchestrator instance's channels are accepted; any other channel answers 404.
"""

from __future__ import annotations

import logging
from typing import Any

from fastapi import APIRouter, Body, Depends, HTTPException, Request, status
from sqlalchemy.orm import Session

from dag_healer.api import schemas
from dag_healer.api.deps import (
    API_PREFIX,
    INGEST_TOKEN_HEADER,
    channel_ingest_guard,
    get_adapter,
    get_config,
    get_instance,
    get_session,
    ingest_guard,
    require_ingest_token,
)
from dag_healer.config import HealerConfig
from dag_healer.core.ports import OrchestratorAdapter
from dag_healer.ingest import MAX_ERROR_CHARS, TRUNCATION_MARKER
from dag_healer.masking import mask_text
from dag_healer.store import repo as store_repo

log = logging.getLogger(__name__)

router = APIRouter(prefix=API_PREFIX, tags=["events"])

EVENT_SOURCES = ("listener", "callback")
#: The push channel of the Airflow listener and failure callback.
TASK_FAILED_CHANNEL = "task-failed"
#: Request headers never handed to an adapter's push intake (the shared secret, credentials).
PRIVATE_HEADERS = frozenset({INGEST_TOKEN_HEADER.lower(), "authorization", "cookie"})
#: Longest error detail of a rejected payload.
MAX_DETAIL_CHARS = 500


def _tail(text: str, limit: int = MAX_ERROR_CHARS) -> str:
    """Keep the end of a long text (where Python puts the exception) within ``limit``."""
    if len(text) <= limit:
        return text
    return TRUNCATION_MARKER + text[-(limit - len(TRUNCATION_MARKER)) :]


def _has_channel(adapter: OrchestratorAdapter, channel: str) -> bool:
    """True when the adapter's push intake accepts ``channel``."""
    push = getattr(adapter, "push", None)
    return push is not None and channel in getattr(push, "channels", ())


def _unknown_channel(channel: str) -> HTTPException:
    return HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail=f"Unknown event channel: {channel}")


@router.post(
    "/events/task-failed",
    status_code=status.HTTP_202_ACCEPTED,
    response_model=schemas.EventAccepted,
    dependencies=[Depends(ingest_guard), Depends(require_ingest_token)],
)
def task_failed(
    event: schemas.TaskFailedEvent,
    cfg: HealerConfig = Depends(get_config),
    adapter: OrchestratorAdapter | None = Depends(get_adapter),
    instance: str = Depends(get_instance),
    session: Session = Depends(get_session),
) -> schemas.EventAccepted:
    """Record a task failure reported by the listener or the failure callback.

    404 when the configured orchestrator has no ``task-failed`` channel (it is not Airflow).
    """
    if adapter is not None and not _has_channel(adapter, TASK_FAILED_CHANNEL):
        raise _unknown_channel(TASK_FAILED_CHANNEL)
    if not cfg.is_monitored(event.dag_id):
        log.debug("dag-healer: ignoring failure event of unmonitored DAG %s", event.dag_id)
        return schemas.EventAccepted(accepted=False, pending_event_id=None)
    source = event.source if event.source in EVENT_SOURCES else "listener"
    pending = store_repo.add_pending_event(
        session,
        orchestrator=instance,
        channel=TASK_FAILED_CHANNEL,
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


@router.post(
    "/events/{channel}",
    status_code=status.HTTP_202_ACCEPTED,
    response_model=schemas.EventsAccepted,
    dependencies=[Depends(channel_ingest_guard), Depends(require_ingest_token)],
)
def channel_events(
    channel: str,
    request: Request,
    payload: Any = Body(default=None),
    cfg: HealerConfig = Depends(get_config),
    adapter: OrchestratorAdapter | None = Depends(get_adapter),
    session: Session = Depends(get_session),
) -> schemas.EventsAccepted:
    """Record the failure hints of another push channel of the orchestrator (EventBridge...).

    404 unless the adapter's push intake lists ``channel``; 422 when the body is not a JSON
    object or the intake rejects it. Hints of unmonitored pipelines are skipped.
    """
    if adapter is None or not _has_channel(adapter, channel):
        raise _unknown_channel(channel)
    if not isinstance(payload, dict):
        raise HTTPException(status_code=422, detail="The event body must be a JSON object")
    headers = {name: value for name, value in request.headers.items() if name.lower() not in PRIVATE_HEADERS}
    try:
        hints = adapter.push.normalize(channel, payload, headers)
    except ValueError as exc:
        raise HTTPException(status_code=422, detail=mask_text(str(exc))[:MAX_DETAIL_CHARS]) from None
    except Exception as exc:  # an adapter bug: its message may quote the payload, so it is not logged
        log.error("dag-healer: the %s channel could not read an event: %s", channel, type(exc).__name__)
        raise HTTPException(status_code=500, detail="The event could not be read") from None
    stored: list[int] = []
    for hint in hints:
        if not cfg.is_monitored(hint.ref.pipeline_id):
            log.debug(
                "dag-healer: ignoring a %s hint of unmonitored pipeline %s", channel, hint.ref.pipeline_id
            )
            continue
        pending = store_repo.add_pending_event(
            session,
            **hint.ref.store_key(),
            channel=channel,
            payload={"error": _tail(mask_text(hint.error_text or "")), "source": hint.source},
            native_ref=dict(hint.native),
        )
        if pending.id not in stored:
            stored.append(pending.id)
    session.commit()
    if stored:
        log.info(
            "dag-healer: %d failure hint(s) received on channel %s; pending event(s) %s",
            len(stored),
            channel,
            stored,
        )
    return schemas.EventsAccepted(accepted=bool(stored), pending_event_ids=stored)
