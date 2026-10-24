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

The standalone host (``dag-healer serve``) mounts ``create_app(auth=backend)`` under
``/healer`` instead: the backend's user dependency and authorizer replace Airflow's
(:func:`dag_healer.api.auth.apply_auth_backend`), the CSRF origins come from the backend
(``HEALER_PUBLIC_URL`` and ``serve.public_url``; the Airflow base URL is not consulted)
and :func:`serving_host` tells the two hosts apart.

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
from collections.abc import Callable, Iterable, Mapping
from pathlib import Path
from types import ModuleType
from typing import TYPE_CHECKING, Any, Literal
from urllib.parse import urlsplit

from fastapi import FastAPI
from fastapi.staticfiles import StaticFiles
from starlette.responses import JSONResponse, Response
from starlette.types import ASGIApp, Message, Receive, Scope, Send

from dag_healer._version import __version__

if TYPE_CHECKING:
    from dag_healer.api.auth import AuthBackend

log = logging.getLogger(__name__)

STATIC_DIR = Path(__file__).resolve().parent / "static"
ROUTERS_MODULE = "dag_healer.api.routers"
DEBUG_ENV = "HEALER_DEBUG"
_TRUTHY = {"1", "true", "yes", "on"}

#: The two hosts of the healer app (the ``host`` of ``/me``).
HostName = Literal["airflow_plugin", "standalone"]
#: ``app.state`` attribute marking an app of the standalone host (see :func:`serving_host`).
HOST_STATE_ATTR = "healer_host"
AIRFLOW_PLUGIN_HOST: HostName = "airflow_plugin"
STANDALONE_HOST: HostName = "standalone"

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
    """ASGI middleware: body size cap and CSRF checks for state-changing requests.

    ``allowed_urls`` gives the URLs whose origins the Origin/Referer check accepts besides
    the request's own; it is called on every checked request. ``None`` (the Airflow host)
    means :func:`configured_base_urls`, looked up per request as well.
    """

    def __init__(
        self,
        app: ASGIApp,
        *,
        max_body_bytes: int = MAX_BODY_BYTES,
        allowed_urls: Callable[[], Iterable[str]] | None = None,
    ) -> None:
        if allowed_urls is not None and not callable(allowed_urls):
            raise TypeError("allowed_urls must be a callable returning the allowed URLs")
        self.app = app
        self.max_body_bytes = max_body_bytes
        self.allowed_urls = allowed_urls

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
        allowed_urls = configured_base_urls if self.allowed_urls is None else self.allowed_urls
        reason = csrf_failure(
            scope["method"],
            _route_path(scope),
            headers,
            request_origin=_request_origin(scope, headers),
            allowed_urls=allowed_urls(),
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


def create_app(*, auth: AuthBackend | None = None) -> FastAPI:
    """Build a new dag-healer FastAPI application.

    With ``auth=None`` it is the Airflow host's app: Airflow's user and authorizer through
    :mod:`dag_healer.api.auth`, the CSRF origins of :func:`configured_base_urls`, no
    dependency override and nothing on ``app.state``. With an
    :class:`~dag_healer.api.auth.AuthBackend` (the standalone host) the same app also gets
    the backend's user dependency and authorizer
    (:func:`~dag_healer.api.auth.apply_auth_backend`) and its ``allowed_origins`` as CSRF
    origins; any backend but ``airflow`` marks the app as the standalone host
    (:func:`serving_host`). ``TypeError`` when ``auth`` is not an ``AuthBackend``.
    """
    if auth is not None:
        # lazy, so that importing this module loads nothing new
        from dag_healer.api.auth import AuthBackend, apply_auth_backend

        if not isinstance(auth, AuthBackend):
            raise TypeError(f"auth must be an AuthBackend, got {type(auth).__name__}")
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
    if auth is None:
        app.add_middleware(RequestGuardMiddleware)
    else:
        app.add_middleware(RequestGuardMiddleware, allowed_urls=auth.allowed_origins)

    # The authenticated /api/v1 routers (dag_healer.api.routers).
    include_routers(app)
    if auth is not None:
        apply_auth_backend(app, auth)
        if auth.name != "airflow":
            setattr(app.state, HOST_STATE_ATTR, STANDALONE_HOST)
    return app


def serving_host(app: Any) -> HostName:
    """The host that serves ``app``: ``standalone`` when :func:`create_app` marked it so.

    Every other app, the Airflow host's included, is ``airflow_plugin``. ``/me`` asks about
    ``request.app``, which is the healer app itself in both hosts (also when mounted).
    """
    state = getattr(app, "state", None)
    if getattr(state, HOST_STATE_ATTR, None) == STANDALONE_HOST:
        return STANDALONE_HOST
    return AIRFLOW_PLUGIN_HOST


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
