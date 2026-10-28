"""The SPA shell of the standalone host: the ``index.html`` that loads the standalone UI bundle.

``dag-healer serve`` answers the URL shapes of the Airflow host with the same page, so that
``links.py``, ``ui/src/links.ts`` and the UI's pathname parser work unchanged in both hosts:

* ``GET /`` redirects (307) to ``plugin/healer``, the Healer page;
* ``GET /plugin/{route}`` (``healer``, ``healer-dag``, ``healer-ti``) and
  ``GET /dags/{rest:path}`` (``/dags/<d>/plugin/healer-dag``,
  ``/dags/<d>/runs/<r>/tasks/<t>[/mapped/<i>]/plugin/healer-ti``) answer the shell.

The shell is public and holds no data: ``<base href="{root_path}/">``, against which the UI
resolves its relative URLs (the ASGI ``root_path``, empty unless the server sets one, such as
uvicorn's ``--root-path``), ``<div id="root">``, the module script :data:`SCRIPT_PATH` with a
cache-busting ``?v=<version>`` and ``<meta name="dag-healer-auth">`` naming the authentication
backend (the UI shows its token sign-in for ``token``). Every value is HTML-escaped. The
standalone bundle is looked for on every request (:func:`bundle_file`), so a bundle built while
the server runs is picked up; until it exists each route answers 503 with
:data:`MISSING_BUNDLE_DETAIL`.

Responses carry :data:`SHELL_HEADERS`: ``Cache-Control: no-store`` and the security headers.
The Content-Security-Policy allows scripts from this origin only (the shell has no inline
script) and inline styles, which the CSS the bundle injects needs.
"""

from __future__ import annotations

import html
import types
from collections.abc import Mapping
from pathlib import Path

from fastapi import FastAPI, Request
from fastapi.responses import HTMLResponse, PlainTextResponse, RedirectResponse, Response

from dag_healer._version import __version__
from dag_healer.api import app as app_module

PRODUCT = "Healer"
#: ``<meta name>`` of the authentication backend's name (``dev``, ``token``, ...).
AUTH_META = "dag-healer-auth"
STANDALONE_DIR = "standalone"
STANDALONE_ENTRY = "index.js"
#: The module script, relative to ``<base href>``: the ``/healer/static`` mount of the healer app.
SCRIPT_PATH = f"healer/static/{STANDALONE_DIR}/{STANDALONE_ENTRY}"
ICON_PATH = "healer/static/icon.svg"
#: The Healer page, where ``/`` redirects (relative to the root path).
HEALER_PAGE = "plugin/healer"
MISSING_BUNDLE_DETAIL = (
    f"The dag-healer standalone UI bundle is missing (dag_healer/api/static/{STANDALONE_DIR}/"
    f"{STANDALONE_ENTRY}). Build it with 'python scripts/tasks.py ui' in a dag-healer checkout, "
    "or install a dag-healer wheel, which ships it. The API still answers under /healer/api/v1."
)
CONTENT_SECURITY_POLICY = "; ".join(
    (
        "default-src 'self'",
        "script-src 'self'",
        "style-src 'self' 'unsafe-inline'",
        "img-src 'self' data:",
        "font-src 'self' data:",
        "connect-src 'self'",
        "object-src 'none'",
        "base-uri 'self'",
        "frame-ancestors 'none'",
        "form-action 'self'",
    )
)
SHELL_HEADERS: Mapping[str, str] = types.MappingProxyType(
    {
        "Cache-Control": "no-store",
        "X-Content-Type-Options": "nosniff",
        "Referrer-Policy": "same-origin",
        "X-Frame-Options": "DENY",
        "Content-Security-Policy": CONTENT_SECURITY_POLICY,
    }
)


def page_title(display_name: str | None) -> str:
    """``Healer · <orchestrator>`` (for example "Healer · AWS Step Functions"), else ``Healer``."""
    name = (display_name or "").strip()
    return f"{PRODUCT} · {name}" if name else PRODUCT


def bundle_file(static_dir: Path | None = None) -> Path:
    """The entry of the standalone UI bundle under ``static_dir``.

    The default is the API's static directory (``dag_healer.api.app.STATIC_DIR``, read at call
    time), which the healer app serves at ``/healer/static``.
    """
    directory = app_module.STATIC_DIR if static_dir is None else Path(static_dir)
    return directory / STANDALONE_DIR / STANDALONE_ENTRY


def render_shell(*, root_path: str, title: str, auth_backend: str, version: str = __version__) -> str:
    """The shell page (every interpolated value is HTML-escaped; no inline script or style)."""

    def escape(value: str) -> str:
        return html.escape(value, quote=True)

    base = f"{root_path.rstrip('/')}/"
    return (
        "<!doctype html>\n"
        '<html lang="en">\n'
        "  <head>\n"
        '    <meta charset="utf-8" />\n'
        f'    <base href="{escape(base)}" />\n'
        '    <meta name="viewport" content="width=device-width, initial-scale=1" />\n'
        f'    <meta name="{AUTH_META}" content="{escape(auth_backend)}" />\n'
        f'    <link rel="icon" type="image/svg+xml" href="{ICON_PATH}" />\n'
        f"    <title>{escape(title)}</title>\n"
        "  </head>\n"
        "  <body>\n"
        '    <div id="root"></div>\n'
        f'    <script type="module" src="{SCRIPT_PATH}?v={escape(version)}"></script>\n'
        "  </body>\n"
        "</html>\n"
    )


def _root_path(request: Request) -> str:
    return str(request.scope.get("root_path") or "").rstrip("/")


def _missing_bundle() -> Response:
    return PlainTextResponse(MISSING_BUNDLE_DETAIL, status_code=503, headers=dict(SHELL_HEADERS))


def mount_shell(app: FastAPI, *, title: str, auth_backend: str, static_dir: Path | None = None) -> None:
    """Register the shell routes on ``app``, the outer app of the standalone host.

    ``title`` and ``auth_backend`` (the backend's name) are rendered into every page;
    ``static_dir`` is where :func:`bundle_file` looks for the bundle.
    """

    def index(request: Request) -> Response:
        """The Healer page (307), or 503 while the standalone bundle is missing."""
        if not bundle_file(static_dir).is_file():
            return _missing_bundle()
        location = f"{_root_path(request)}/{HEALER_PAGE}"
        return RedirectResponse(location, status_code=307, headers=dict(SHELL_HEADERS))

    def page(request: Request) -> Response:
        """The shell, or 503 while the standalone bundle is missing."""
        if not bundle_file(static_dir).is_file():
            return _missing_bundle()
        body = render_shell(root_path=_root_path(request), title=title, auth_backend=auth_backend)
        return HTMLResponse(body, headers=dict(SHELL_HEADERS))

    app.add_api_route("/", index, methods=["GET"], include_in_schema=False)
    app.add_api_route("/plugin/{route}", page, methods=["GET"], include_in_schema=False)
    app.add_api_route("/dags/{rest:path}", page, methods=["GET"], include_in_schema=False)


__all__ = [
    "AUTH_META",
    "CONTENT_SECURITY_POLICY",
    "HEALER_PAGE",
    "ICON_PATH",
    "MISSING_BUNDLE_DETAIL",
    "PRODUCT",
    "SCRIPT_PATH",
    "SHELL_HEADERS",
    "STANDALONE_DIR",
    "STANDALONE_ENTRY",
    "bundle_file",
    "mount_shell",
    "page_title",
    "render_shell",
]
