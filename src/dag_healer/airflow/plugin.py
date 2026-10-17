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
"""

from __future__ import annotations

from typing import Any

from airflow.plugins_manager import AirflowPlugin

from dag_healer._version import __version__
from dag_healer.airflow import listener as healer_listener

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
    _react_app("DagHealerNav", "nav", "healer", nav_top_level=True),
    _react_app("DagHealerDag", "dag", "healer-dag"),
    _react_app("DagHealerTaskInstance", "task_instance", "healer-ti"),
]


class HealerPlugin(AirflowPlugin):
    """Registers the dag-healer listener, REST API (``/healer``) and UI views."""

    name = "dag_healer"
    listeners = [healer_listener]
    react_apps = REACT_APPS

    @property
    def fastapi_apps(self) -> list[dict[str, Any]]:  # type: ignore[override]
        """The ``/healer`` FastAPI sub-application, created on first access."""
        from dag_healer.api.app import get_app

        return [{"app": get_app(), "url_prefix": URL_PREFIX, "name": "dag-healer"}]
