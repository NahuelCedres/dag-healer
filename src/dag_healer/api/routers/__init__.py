"""Routers of the dag-healer API (``/healer/api/v1``) and their wiring.

:func:`include_routers` is called by :func:`dag_healer.api.app.create_app`. Every route
except ``POST /api/v1/events/*`` (shared ingest token) depends on
:func:`dag_healer.api.auth.get_current_user`; DAG-scoped routes live under
``/api/v1/dags/{dag_id}/...`` and depend on :func:`dag_healer.api.auth.require_dag_access`.
"""

from __future__ import annotations

import logging

from fastapi import APIRouter, FastAPI, Request
from fastapi.responses import JSONResponse
from sqlalchemy.exc import SQLAlchemyError

from dag_healer.api.deps import API_PREFIX
from dag_healer.api.routers import contracts, dags, events, incidents, me, setup
from dag_healer.masking import mask_text

log = logging.getLogger(__name__)

#: Routes that authenticate with the ingest token instead of an Airflow user.
PUBLIC_PREFIXES: tuple[str, ...] = (f"{API_PREFIX}/events/",)

routers: list[APIRouter] = [
    me.router,
    setup.router,
    incidents.router,
    dags.router,
    contracts.router,
    events.router,
]


async def _store_unavailable(request: Request, exc: Exception) -> JSONResponse:
    """Answer 503 when the incident store fails (logged without parameters)."""
    log.error(
        "dag-healer: store error on %s %s: %s", request.method, request.url.path, mask_text(str(exc))[:500]
    )
    return JSONResponse(status_code=503, content={"detail": "The dag-healer store is unavailable"})


def include_routers(app: FastAPI) -> None:
    """Attach every ``/api/v1`` router and the store error handler to ``app``."""
    for router in routers:
        app.include_router(router)
    app.add_exception_handler(SQLAlchemyError, _store_unavailable)


__all__ = ["API_PREFIX", "PUBLIC_PREFIXES", "include_routers", "routers"]
