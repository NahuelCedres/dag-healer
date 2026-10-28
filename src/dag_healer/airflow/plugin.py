"""Airflow 3 plugin entry point of dag-healer.

Registered through the ``airflow.plugins`` entry point
(``dag_healer = "dag_healer.airflow.plugin:HealerPlugin"``).

Why the imports are lazy: Airflow loads every plugin in *each task runner process*
(the listener manager of the Task SDK loads entry-point plugins to register their
listeners), not only in the api-server. Importing FastAPI, SQLAlchemy or pandas at
module level would therefore slow down and bloat every single task. This module only
imports the lightweight listener; the FastAPI sub-application is built on first
access of :attr:`HealerPlugin.fastapi_apps`, which only the api-server (and
``airflow plugins``) performs. Airflow documents that plugin components may be
class attributes *or properties*.

That first access also logs, once per process, that the FAB auth manager shows the views only
to roles with ``Plugins: can_read`` when Airflow's ``[core] auth_manager`` is FAB
(:func:`note_fab_permission`).
"""

from __future__ import annotations

import logging
from typing import Any

from airflow.plugins_manager import AirflowPlugin

from dag_healer._version import __version__
from dag_healer.airflow import FAB_GRANT_COMMAND, FAB_PLUGINS_PERMISSION, is_fab_auth_manager
from dag_healer.airflow import listener as healer_listener

log = logging.getLogger(__name__)

#: Logged once per process by :func:`note_fab_permission`.
FAB_NOTE = (
    "dag-healer: Airflow's auth manager is FAB, which shows the dag-healer views only to roles "
    f"with the '{FAB_PLUGINS_PERMISSION}' permission (Admin and Op by default); grant it to the "
    f"other roles that should see them, for example: {FAB_GRANT_COMMAND}"
)
_fab_noted = False

URL_PREFIX = "/healer"
# Relative URLs (no leading slash): the Airflow UI resolves them against
# document.baseURI, so they keep working when Airflow is served under a sub-path.
BUNDLE_URL = f"healer/static/main.umd.cjs?v={__version__}"
ICON_URL = "healer/static/icon.svg"


def _react_app(name: str, destination: str, url_route: str, **extra: Any) -> dict[str, Any]:
    """Describe one view of the UI bundle.

    ``name`` doubles as the label shown by Airflow and as the ``globalThis`` key the
    host looks the component up with, so it must match the names the bundle exports.
    """
    return {
        "name": name,
        "bundle_url": BUNDLE_URL,
        "destination": destination,
        "url_route": url_route,
        "icon": ICON_URL,
        **extra,
    }


REACT_APPS: list[dict[str, Any]] = [
    _react_app("Healer", "nav", "healer", nav_top_level=True),
    _react_app("Healer incidents", "dag", "healer-dag"),
    _react_app("Healer incident", "task_instance", "healer-ti"),
]


def note_fab_permission() -> None:
    """Log :data:`FAB_NOTE` once per process when Airflow's ``[core] auth_manager`` is FAB.

    Best effort: a configuration that cannot be read is ignored, and nothing ever raises.
    """
    global _fab_noted
    if _fab_noted:
        return
    _fab_noted = True
    try:
        from airflow.configuration import conf  # lazy: only where the API is built

        auth_manager = conf.get("core", "auth_manager", fallback=None)
    except Exception:
        log.debug("dag-healer: could not read [core] auth_manager", exc_info=True)
        return
    if is_fab_auth_manager(auth_manager):
        log.info(FAB_NOTE)


class HealerPlugin(AirflowPlugin):
    """Registers the dag-healer listener, REST API (``/healer``) and UI views."""

    name = "dag_healer"
    listeners = [healer_listener]
    react_apps = REACT_APPS

    @property
    def fastapi_apps(self) -> list[dict[str, Any]]:  # type: ignore[override]
        """The ``/healer`` FastAPI sub-application, created on first access."""
        from dag_healer.api.app import get_app

        note_fab_permission()
        return [{"app": get_app(), "url_prefix": URL_PREFIX, "name": "dag-healer"}]
