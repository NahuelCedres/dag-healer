"""The dag-healer FastAPI application, mounted by Airflow under ``/healer``.

Airflow mounts plugin sub-applications without authenticating them; every route that
exposes data must declare its own auth dependency (see ``dag_healer.api.auth``).
The only public routes defined here are ``/health`` and the static UI assets.

Every state-changing request (POST/PUT/PATCH/DELETE) goes through
:class:`RequestGuardMiddleware`: bodies are capped at 1 MiB and, except for the ingest
route ``/api/v1/events/*`` (shared token, no browser), the request must carry
``X-Requested-With: dag-healer`` and any ``Origin``/``Referer`` must match the Airflow
base URL (``AIRFLOW__API__BASE_URL``) or the request's own host (CSRF protection on top
of the ``SameSite=Lax`` Airflow cookie).

This module must import and run without Airflow (unit tests on hosts where Airflow
cannot be installed); routers are attached by :func:`include_routers` and their
authentication fails closed (503) when Airflow is unavailable.
"""

from __future__ import annotations

import functools
import importlib
import importlib.util
import logging
import mimetypes
import os
from collections.abc import Iterable, Mapping
from pathlib import Path
from types import ModuleType
from typing import Any
from urllib.parse import urlsplit

from fastapi import FastAPI
from fastapi.staticfiles import StaticFiles
from starlette.responses import JSONResponse, Response
from starlette.types import ASGIApp, Message, Receive, Scope, Send

from dag_healer._version import __version__

log = logging.getLogger(__name__)

STATIC_DIR = Path(__file__).resolve().parent / "static"
ROUTERS_MODULE = "dag_healer.api.routers"
DEBUG_ENV = "HEALER_DEBUG"
_TRUTHY = {"1", "true", "yes", "on"}

SAFE_METHODS = frozenset({"GET", "HEAD", "OPTIONS", "TRACE"})
CSRF_HEADER = "x-requested-with"
CSRF_VALUE = "dag-healer"
#: Route prefixes (inside this app) exempt from the CSRF header and origin checks.
CSRF_EXEMPT_PREFIXES: tuple[str, ...] = ("/api/v1/events/",)
MAX_BODY_BYTES = 1024 * 1024
BASE_URL_ENVS: tuple[str, ...] = ("AIRFLOW__API__BASE_URL", "HEALER_PUBLIC_URL")
_DEFAULT_PORTS = {"http": 80, "https": 443}

Origin = tuple[str, str, int]


def parse_origin(value: str | None) -> Origin | None:
    """``(scheme, host, port)`` of an http(s) URL or Origin header; ``None`` if unusable."""
    if not value:
        return None
    try:
        parts = urlsplit(value.strip())
        scheme = parts.scheme.lower()
        host = (parts.hostname or "").lower()
        port = parts.port
    except ValueError:
        return None
    if scheme not in _DEFAULT_PORTS or not host:
        return None
    return scheme, host, port if port is not None else _DEFAULT_PORTS[scheme]


def configured_base_urls() -> list[str]:
    """Public URLs Airflow is served at (``AIRFLOW__API__BASE_URL``, ``HEALER_PUBLIC_URL``).

    Falls back to Airflow's ``[api] base_url`` setting when the environment has none.
    """
    urls = [os.environ[name].strip() for name in BASE_URL_ENVS if os.environ.get(name, "").strip()]
    if urls:
        return urls
    try:
        from dag_healer.api.auth import configured_base_url
    except ImportError:
        return []
    configured = configured_base_url()
    return [configured] if configured else []


def csrf_failure(
    method: str,
    path: str,
    headers: Mapping[str, str],
    *,
    request_origin: Origin | None,
    allowed_urls: Iterable[str],
) -> str | None:
    """Return why a request fails the CSRF checks, or ``None`` when it passes.

    Safe methods and :data:`CSRF_EXEMPT_PREFIXES` always pass. Other requests need
    ``X-Requested-With: dag-healer``; when ``Origin`` (else ``Referer``) is present its
    scheme, host and port must equal those of one of ``allowed_urls`` or of the request
    itself (``request_origin``). ``headers`` keys are lower case.
    """
    if method.upper() in SAFE_METHODS or any(path.startswith(prefix) for prefix in CSRF_EXEMPT_PREFIXES):
        return None
    if headers.get(CSRF_HEADER, "").strip().lower() != CSRF_VALUE:
        return f"Missing or invalid X-Requested-With header (expected {CSRF_VALUE!r})"
    source = headers.get("origin")
    if source is None:
        source = headers.get("referer")
    if source is None:
        return None
    origin = parse_origin(source)
    allowed = {parsed for parsed in (parse_origin(url) for url in allowed_urls) if parsed is not None}
    if request_origin is not None:
        allowed.add(request_origin)
    if origin is None or origin not in allowed:
        return "Cross-origin request rejected"
    return None


def _route_path(scope: Scope) -> str:
    """Path of the request inside this application (mount prefix removed)."""
    path: str = scope.get("path", "") or ""
    root_path: str = scope.get("root_path", "") or ""
    if root_path and path.startswith(root_path):
        return path[len(root_path) :] or "/"
    return path


def _request_origin(scope: Scope, headers: Mapping[str, str]) -> Origin | None:
    """Origin the request was sent to (scheme of the connection + ``Host`` header)."""
    host = headers.get("host")
    if not host:
        server = scope.get("server")
        if not server:
            return None
        host = f"{server[0]}:{server[1]}" if server[1] else str(server[0])
    return parse_origin(f"{scope.get('scheme', 'http')}://{host}")


class RequestGuardMiddleware:
    """ASGI middleware: body size cap and CSRF checks for state-changing requests."""

    def __init__(self, app: ASGIApp, *, max_body_bytes: int = MAX_BODY_BYTES) -> None:
        self.app = app
        self.max_body_bytes = max_body_bytes

    async def __call__(self, scope: Scope, receive: Receive, send: Send) -> None:
        """Reject oversized or cross-site state-changing requests before routing."""
        if scope["type"] != "http" or str(scope.get("method", "GET")).upper() in SAFE_METHODS:
            await self.app(scope, receive, send)
            return
        headers = {key.decode("latin-1").lower(): value.decode("latin-1") for key, value in scope["headers"]}
        length = headers.get("content-length")
        if length is not None and (not length.strip().isdigit() or int(length) > self.max_body_bytes):
            await self._reject(scope, receive, send, 413, "Request body is too large")
            return
        reason = csrf_failure(
            scope["method"],
            _route_path(scope),
            headers,
            request_origin=_request_origin(scope, headers),
            allowed_urls=configured_base_urls(),
        )
        if reason is not None:
            log.info("dag-healer: rejected %s %s: %s", scope["method"], scope.get("path"), reason)
            await self._reject(scope, receive, send, 403, reason)
            return
        messages = await self._read_body(receive)
        if messages is None:
            await self._reject(scope, receive, send, 413, "Request body is too large")
            return
        await self.app(scope, self._replay(messages, receive), send)

    async def _read_body(self, receive: Receive) -> list[Message] | None:
        """Buffer the request body messages (``None`` when the body exceeds the limit)."""
        messages: list[Message] = []
        size = 0
        while True:
            message = await receive()
            messages.append(message)
            if message["type"] != "http.request":
                return messages
            size += len(message.get("body", b""))
            if size > self.max_body_bytes:
                return None
            if not message.get("more_body", False):
                return messages

    @staticmethod
    def _replay(messages: list[Message], receive: Receive) -> Receive:
        """A ``receive`` that first replays the buffered messages."""
        pending = list(messages)

        async def replay() -> Message:
            if pending:
                return pending.pop(0)
            return await receive()

        return replay

    @staticmethod
    async def _reject(scope: Scope, receive: Receive, send: Send, status_code: int, detail: str) -> None:
        response = JSONResponse({"detail": detail}, status_code=status_code)
        await response(scope, receive, send)


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
    app.add_middleware(RequestGuardMiddleware)

    # The authenticated /api/v1 routers (dag_healer.api.routers).
    include_routers(app)
    return app


def include_routers(app: FastAPI) -> None:
    """Attach the API routers of ``dag_healer.api.routers`` when that package exists.

    Contract for ``dag_healer.api.routers``: expose either a function
    ``include_routers(app: FastAPI) -> None`` or an iterable ``routers`` of
    ``fastapi.APIRouter`` objects (included as-is, so they carry their own prefixes,
    e.g. ``/api/v1``). The bundled routers import without Airflow (their auth
    dependencies then answer 503 unless overridden by tests); should importing them
    fail because Airflow is not installed, the authenticated API is skipped with a
    warning and only ``/health`` and static assets are served. Any other import error
    propagates.
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
    """True for a real ``airflow`` package; a bare ``airflow/`` namespace directory does not count."""
    try:
        spec = importlib.util.find_spec("airflow")
    except (ImportError, ValueError):
        return False
    return spec is not None and spec.origin not in (None, "namespace")


@functools.lru_cache(maxsize=1)
def get_app() -> FastAPI:
    """Return the process-wide application instance (created once)."""
    return create_app()
