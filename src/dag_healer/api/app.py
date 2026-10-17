"""The dag-healer FastAPI application, mounted by Airflow under ``/healer``.

Airflow mounts plugin sub-applications without authenticating them; every route that
exposes data must declare its own auth dependency (see ``dag_healer.api.auth``).
The only public routes defined here are ``/health`` and the static UI assets.

This module must import and run without Airflow (unit tests on hosts where Airflow
cannot be installed); Airflow-dependent routers are attached by :func:`include_routers`.
"""

from __future__ import annotations

import functools
import importlib
import importlib.util
import logging
import mimetypes
import os
from pathlib import Path
from types import ModuleType
from typing import Any

from fastapi import FastAPI
from fastapi.staticfiles import StaticFiles
from starlette.responses import Response
from starlette.types import Scope

from dag_healer._version import __version__

log = logging.getLogger(__name__)

STATIC_DIR = Path(__file__).resolve().parent / "static"
ROUTERS_MODULE = "dag_healer.api.routers"
DEBUG_ENV = "HEALER_DEBUG"
_TRUTHY = {"1", "true", "yes", "on"}


def _register_mimetypes() -> None:
    """Make sure the UI bundle is served as JavaScript (Windows registries often say text/plain)."""
    mimetypes.add_type("application/javascript", ".cjs")
    mimetypes.add_type("application/javascript", ".js")
    mimetypes.add_type("image/svg+xml", ".svg")


class NoCacheStaticFiles(StaticFiles):
    """Static files that browsers must revalidate (ETag/Last-Modified) on every load."""

    async def get_response(self, path: str, scope: Scope) -> Response:
        response = await super().get_response(path, scope)
        response.headers["Cache-Control"] = "no-cache"
        return response


def debug_enabled() -> bool:
    """True when ``HEALER_DEBUG`` asks for the interactive API docs."""
    return os.environ.get(DEBUG_ENV, "").strip().lower() in _TRUTHY


def create_app() -> FastAPI:
    """Build a new dag-healer FastAPI application."""
    _register_mimetypes()
    debug = debug_enabled()
    app = FastAPI(
        title="dag-healer",
        version=__version__,
        docs_url="/docs" if debug else None,
        redoc_url=None,
        openapi_url="/openapi.json" if debug else None,
    )

    @app.get("/health", tags=["health"])
    def health() -> dict[str, Any]:
        """Liveness probe (public)."""
        return {"status": "ok", "version": __version__}

    app.mount("/static", NoCacheStaticFiles(directory=STATIC_DIR), name="static")

    # ------------------------------------------------------------------ include_routers(app)
    # Later phases register the /api/v1 routers here (see include_routers below).
    include_routers(app)
    return app


def include_routers(app: FastAPI) -> None:
    """Attach the API routers of ``dag_healer.api.routers`` when that package exists.

    Contract for ``dag_healer.api.routers``: expose either a function
    ``include_routers(app: FastAPI) -> None`` or an iterable ``routers`` of
    ``fastapi.APIRouter`` objects (included as-is, so they carry their own prefixes,
    e.g. ``/api/v1``). If Airflow is not installed (hosts without Airflow), the
    authenticated API is skipped with a warning and only ``/health`` and static
    assets are served; any other import error propagates.
    """
    try:
        routers_module = _import_routers_module()
    except ImportError as exc:
        if not _is_missing_airflow(exc):
            raise
        log.warning("dag-healer: Airflow is not installed (%s); the /api/v1 routes are disabled", exc)
        return
    if routers_module is None:
        log.debug("dag-healer: %s not found, serving health and static assets only", ROUTERS_MODULE)
        return
    register = getattr(routers_module, "include_routers", None)
    if callable(register):
        register(app)
        return
    for router in getattr(routers_module, "routers", ()):
        app.include_router(router)


def _import_routers_module() -> ModuleType | None:
    """Import ``dag_healer.api.routers``, or return None when it does not exist (yet)."""
    if importlib.util.find_spec(ROUTERS_MODULE) is None:
        return None
    return importlib.import_module(ROUTERS_MODULE)


def _is_missing_airflow(exc: ImportError) -> bool:
    """True when ``exc`` comes from Airflow not being installed at all.

    A broken or incompatible Airflow installation (a submodule missing while the
    ``airflow`` package exists) is a real error and is not swallowed.
    """
    name = getattr(exc, "name", None) or ""
    return (name == "airflow" or name.startswith("airflow.")) and not _airflow_installed()


def _airflow_installed() -> bool:
    try:
        return importlib.util.find_spec("airflow") is not None
    except (ImportError, ValueError):
        return False


@functools.lru_cache(maxsize=1)
def get_app() -> FastAPI:
    """Return the process-wide application instance (created once)."""
    return create_app()
