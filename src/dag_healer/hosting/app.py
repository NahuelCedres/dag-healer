"""The ASGI app of ``dag-healer serve``: the healer API under ``/healer`` plus the SPA shell.

:func:`create_standalone_app` composes the standalone host (section 7 of
``docs/architecture/orchestrators.md``):

* the app the Airflow plugin mounts, :func:`dag_healer.api.app.create_app`, built with the
  :class:`~dag_healer.api.auth.AuthBackend` of ``serve.auth`` and mounted at
  :data:`URL_PREFIX`: ``/healer/api/v1``, the public ``/healer/health`` and ``/healer/static``,
  which also serves the standalone UI bundle;
* the shell of :mod:`dag_healer.hosting.shell` on the Airflow URL shapes (``/``,
  ``/plugin/{route}``, ``/dags/{rest:path}``), titled after the orchestrator ("Healer ·
  Fakeflow").

The backend comes from :func:`dag_healer.hosting.auth.build_auth_backend` with a
:class:`~dag_healer.hosting.rbac.RuleAuthorizer` of the effective rules, which lists
:func:`pipeline_candidates`; the backend and the rules are read once, from the configuration
given. The API keeps loading ``healer.yaml`` through :func:`dag_healer.api.deps.get_config`
on every request, so the caller points ``HEALER_CONFIG`` at the file that configuration came
from (``dag-healer serve`` does). When the reloaded ``serve.auth`` differs from the one served
(a token removed, a rule narrowed), the host warns that a restart applies it; it notices while
listing the pipelines a user may see, which the Healer page does on every load.

Every answer under ``/healer/api/`` carries ``Cache-Control: no-store`` and
``X-Content-Type-Options: nosniff`` unless it sets them (:class:`ApiResponseHeaders`): behind a
proxy that keeps a session cookie, no cache may keep incidents, error excerpts or PR content.

With the ``dev`` backend, whose user is an admin, the app also answers only requests addressed
to a loopback host name that no proxy forwarded (:class:`LoopbackHostGuard`): binding a loopback
address keeps other machines out, but not a web page that reaches the port through DNS
rebinding, nor a reverse proxy on the same machine.
"""

from __future__ import annotations

import logging
import threading
import types
from collections.abc import Mapping
from pathlib import Path
from typing import TYPE_CHECKING
from urllib.parse import urlsplit

from fastapi import FastAPI
from starlette.datastructures import MutableHeaders
from starlette.responses import PlainTextResponse
from starlette.types import ASGIApp, Message, Receive, Scope, Send

from dag_healer._version import __version__
from dag_healer.api import deps, views
from dag_healer.api.app import STANDALONE_HOST, create_app
from dag_healer.config import HealerConfig, ServeAuthConfig
from dag_healer.hosting import rbac, shell
from dag_healer.hosting.auth.dev import is_loopback_host
from dag_healer.masking import mask_text
from dag_healer.orchestrators import registry

if TYPE_CHECKING:
    from dag_healer.api.auth import AuthBackend
    from dag_healer.core.ports import OrchestratorAdapter

log = logging.getLogger(__name__)

#: Where the healer app is mounted: the prefix of the Airflow plugin (``airflow/plugin.py``), so
#: both hosts serve ``/healer/api/v1`` and ``/healer/static``.
URL_PREFIX = "/healer"
#: Characters that make a ``monitor.dags`` entry a pattern rather than a pipeline id.
_PATTERN_CHARS = frozenset("*?[")
#: The answer of :class:`LoopbackHostGuard` (400) to a request for another host name.
LOOPBACK_HOST_DETAIL = (
    "The dev backend of dag-healer serve only answers requests addressed to localhost or a "
    "loopback address (such as http://127.0.0.1:8090); use another serve.auth.backend to serve "
    "other host names."
)
#: The answer of :class:`LoopbackHostGuard` (400) to a request a proxy forwarded.
FORWARDED_REQUEST_DETAIL = (
    "The dev backend of dag-healer serve does not answer requests forwarded by a proxy, because it "
    "serves every request as an admin; use another serve.auth.backend behind a proxy."
)
#: Request headers that proxies add (lower case, as in ASGI).
FORWARDING_HEADERS = frozenset(
    {b"forwarded", b"via", b"x-forwarded-for", b"x-forwarded-host", b"x-forwarded-proto", b"x-real-ip"}
)
#: Headers of every answer under ``/healer/api/`` that does not set them itself.
API_RESPONSE_HEADERS: Mapping[str, str] = types.MappingProxyType(
    {"Cache-Control": "no-store", "X-Content-Type-Options": "nosniff"}
)
_API_PATH = f"{URL_PREFIX}/api/"


def _candidates_of(cfg: HealerConfig) -> set[str]:
    """The pipeline ids of :func:`pipeline_candidates` in ``cfg``."""
    candidates = set(views.load_contracts(cfg))
    candidates.update(
        entry
        for entry in cfg.monitor.dags
        if entry and not entry.startswith("!") and _PATTERN_CHARS.isdisjoint(entry)
    )
    candidates.update(cfg.datasets)
    return candidates


def pipeline_candidates() -> set[str]:
    """The pipeline ids the standalone authorizer may list, from the current configuration.

    The pipelines of the contracts, the literal ``monitor.dags`` entries (no pattern, no
    ``!`` exclusion) and the ``datasets`` keys, read through
    :func:`dag_healer.api.deps.get_config` at call time. No store query: the list routes pass
    the pipelines of their incidents as candidates themselves.
    """
    return _candidates_of(deps.get_config())


class _ServeAuthWatch:
    """Warns, once per change, when the reloaded ``serve.auth`` is not the one the host serves.

    The backend, its tokens and the rules are built at start, while the API reloads
    ``healer.yaml`` on its own: a token removed or a rule narrowed there applies only once
    ``dag-healer serve`` restarts.
    """

    def __init__(self, served: ServeAuthConfig) -> None:
        self._served = served.model_copy(deep=True)
        self._seen = self._served
        self._lock = threading.Lock()

    def check(self, current: ServeAuthConfig) -> None:
        with self._lock:
            if current is self._seen:
                return
            unchanged = current == self._seen
            self._seen = current
        if not unchanged and current != self._served:
            log.warning(
                "dag-healer: serve.auth changed in healer.yaml: restart dag-healer serve to apply it; "
                "until then the backend, the tokens and the rules it started with stay in force"
            )


def _route_path(scope: Scope) -> str:
    """The request path inside this app (without the ASGI root path, which some servers prepend)."""
    path: str = scope.get("path", "") or ""
    root_path: str = scope.get("root_path", "") or ""
    return path[len(root_path) :] if root_path and path.startswith(root_path) else path


class ApiResponseHeaders:
    """ASGI middleware: :data:`API_RESPONSE_HEADERS` on every answer under ``/healer/api/``.

    The API's answers hold incidents, error excerpts and PR content: behind a proxy that keeps a
    session cookie (an ALB, oauth2-proxy), neither a shared nor the browser's cache may keep
    them. An answer that sets one of these headers keeps its own value.
    """

    def __init__(self, app: ASGIApp) -> None:
        self.app = app

    async def __call__(self, scope: Scope, receive: Receive, send: Send) -> None:
        if scope["type"] != "http" or not _route_path(scope).startswith(_API_PATH):
            await self.app(scope, receive, send)
            return

        async def send_with_headers(message: Message) -> None:
            if message["type"] == "http.response.start":
                headers = MutableHeaders(scope=message)
                for name, value in API_RESPONSE_HEADERS.items():
                    if name not in headers:
                        headers[name] = value
            await send(message)

        await self.app(scope, receive, send_with_headers)


def _host_name(scope: Scope) -> str | None:
    """The host name of the request's one ``Host`` header (``None``: none, several or unparsable)."""
    values = [value for key, value in scope.get("headers") or () if key == b"host"]
    if len(values) != 1:
        return None
    try:
        return urlsplit(f"//{values[0].decode('latin-1').strip()}").hostname
    except ValueError:
        return None


class LoopbackHostGuard:
    """ASGI middleware of the ``dev`` backend: 400 unless the ``Host`` header names a loopback host
    and no proxy forwarded the request.

    ``localhost`` and the loopback addresses pass (``is_loopback_host``, the rule the dev
    backend applies to the bind address). Any other name may be DNS rebinding: a web page whose
    host name resolves to 127.0.0.1 would otherwise act as the dev user, an admin. A request with
    one of the :data:`FORWARDING_HEADERS` came through a proxy, which may run on this machine and
    rewrite ``Host`` to the loopback address it forwards to (nginx does by default).
    """

    def __init__(self, app: ASGIApp) -> None:
        self.app = app

    async def __call__(self, scope: Scope, receive: Receive, send: Send) -> None:
        if scope["type"] == "http":
            detail = _dev_refusal(scope)
            if detail is not None:
                log.debug("dag-healer: the dev backend refused a request: %s", detail)
                response = PlainTextResponse(detail, status_code=400)
                await response(scope, receive, send)
                return
        await self.app(scope, receive, send)


def _dev_refusal(scope: Scope) -> str | None:
    """Why :class:`LoopbackHostGuard` refuses the request, or ``None`` when it may pass."""
    host = _host_name(scope)
    if host is None or not is_loopback_host(host):
        return LOOPBACK_HOST_DETAIL
    if any(key in FORWARDING_HEADERS for key, _ in scope.get("headers") or ()):
        return FORWARDED_REQUEST_DETAIL
    return None


def _adapter(cfg: HealerConfig) -> OrchestratorAdapter | None:
    """The orchestrator adapter of ``cfg`` (cached, built without I/O); ``None`` when it cannot be built."""
    try:
        return registry.get_adapter(cfg)
    except Exception as exc:  # like the API, the host serves its views without the adapter
        log.warning(
            "dag-healer: the orchestrator adapter cannot be built (%s); the standalone UI is "
            "titled %r and the API serves its routes without it",
            mask_text(f"{type(exc).__name__}: {exc}")[:500],
            shell.PRODUCT,
        )
        return None


def create_standalone_app(
    cfg: HealerConfig,
    *,
    backend: AuthBackend | None = None,
    bind_host: str | None = None,
    static_dir: Path | None = None,
) -> FastAPI:
    """The app of ``dag-healer serve`` for ``cfg`` (see the module docstring).

    Without ``backend`` the one ``serve.auth`` names is built, with a
    :class:`~dag_healer.hosting.rbac.RuleAuthorizer` of the effective rules whose candidates
    (:func:`pipeline_candidates`) also watch for a changed ``serve.auth``; ``bind_host``
    (default ``serve.host``) is the address the server binds, which the ``dev`` backend
    requires to be loopback. :class:`~dag_healer.hosting.auth.AuthConfigError` when
    ``serve.auth`` cannot be served safely. A given ``backend`` (tests) is used as it is, with
    its own authorizer. ``static_dir`` is where the shell looks for the standalone bundle
    (default: the API's static directory, which ``/healer/static`` serves). A backend named
    ``dev`` gets :class:`LoopbackHostGuard`, and every app :class:`ApiResponseHeaders`.

    The page title names the orchestrator; an adapter that cannot be built leaves it "Healer"
    (logged). A missing bundle, and an adapter that does not list the standalone host among
    its UI hosts, are logged as warnings.
    """
    adapter = _adapter(cfg)
    title = shell.page_title(adapter.display_name if adapter is not None else None)
    if backend is None:
        from dag_healer.hosting.auth import build_auth_backend

        watch = _ServeAuthWatch(cfg.serve.auth)

        def candidates() -> set[str]:
            current = deps.get_config()
            watch.check(current.serve.auth)
            return _candidates_of(current)

        authorizer = rbac.RuleAuthorizer(rbac.effective_rules(cfg.serve.auth), candidates=candidates)
        backend = build_auth_backend(cfg, authorizer=authorizer, bind_host=bind_host or cfg.serve.host)
    healer = create_app(auth=backend)
    app = FastAPI(title="dag-healer", version=__version__, docs_url=None, redoc_url=None, openapi_url=None)
    app.mount(URL_PREFIX, healer)
    shell.mount_shell(app, title=title, auth_backend=backend.name, static_dir=static_dir)
    if backend.name == "dev":
        app.add_middleware(LoopbackHostGuard)
    app.add_middleware(ApiResponseHeaders)

    bundle = shell.bundle_file(static_dir)
    if not bundle.is_file():
        log.warning(
            "dag-healer: the standalone UI bundle %s is missing: the pages answer 503 until it is "
            "built ('python scripts/tasks.py ui') or a dag-healer wheel is installed; the API "
            "still answers under %s/api/v1",
            bundle,
            URL_PREFIX,
        )
    ui_hosts = tuple(getattr(getattr(adapter, "capabilities", None), "ui_hosts", ()) or ())
    if adapter is not None and STANDALONE_HOST not in ui_hosts:
        log.warning(
            "dag-healer: the %s adapter does not list the standalone host among its UI hosts "
            "(%s); its views may not work in dag-healer serve",
            adapter.display_name,
            ", ".join(ui_hosts) or "none",
        )
    return app


__all__ = [
    "API_RESPONSE_HEADERS",
    "FORWARDED_REQUEST_DETAIL",
    "FORWARDING_HEADERS",
    "LOOPBACK_HOST_DETAIL",
    "URL_PREFIX",
    "ApiResponseHeaders",
    "LoopbackHostGuard",
    "create_standalone_app",
    "pipeline_candidates",
]
