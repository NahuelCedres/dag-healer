"""Airflow integration of dag-healer: plugin, listener, failure callback and REST client.

Importing this package must not import Airflow itself (``dag_healer.airflow.callback``
is used on hosts without Airflow, e.g. in unit tests); only ``plugin`` and
``listener`` do, because they are loaded by Airflow.
"""

from __future__ import annotations

import logging

from dag_healer.ingest import api_base_from_execution_url, resolve_api_url

log = logging.getLogger(__name__)

#: The class of Airflow's FAB auth manager, as ``[core] auth_manager`` names it.
FAB_AUTH_MANAGER = "FabAuthManager"
#: The FAB permission a role needs to see the dag-healer views: the Airflow UI builds them from
#: the plugin list (``GET /api/v2/plugins``), which only the Admin and Op roles can read by default.
FAB_PLUGINS_PERMISSION = "Plugins: can_read"
#: Grants :data:`FAB_PLUGINS_PERMISSION` to the built-in roles that lack it (additive, idempotent).
FAB_GRANT_COMMAND = "airflow roles add-perms Viewer User --resource Plugins --action can_read"

__all__ = [
    "FAB_AUTH_MANAGER",
    "FAB_GRANT_COMMAND",
    "FAB_PLUGINS_PERMISSION",
    "airflow_api_url",
    "is_fab_auth_manager",
]


def is_fab_auth_manager(value: object) -> bool:
    """True when ``value`` (Airflow's ``[core] auth_manager``) names the FAB auth manager."""
    return isinstance(value, str) and value.strip().rsplit(".", 1)[-1] == FAB_AUTH_MANAGER


def airflow_api_url() -> str | None:
    """Resolve the api-server URL for fast-path reports from an Airflow process.

    Uses the environment first (see :func:`dag_healer.ingest.resolve_api_url`) and falls
    back to Airflow's own configuration (``[core] execution_api_server_url``, then
    ``[api] base_url``), which already points at the api-server from a task runner.
    Best effort: returns None when nothing usable is configured.
    """
    url = resolve_api_url()
    if url:
        return url
    try:
        from airflow.configuration import conf  # lazy: only inside Airflow processes
    except Exception:
        return None
    for section, key in (("core", "execution_api_server_url"), ("api", "base_url")):
        try:
            value = conf.get(section, key, fallback=None)
        except Exception:
            log.debug("dag-healer: could not read [%s] %s", section, key, exc_info=True)
            continue
        if value and str(value).strip():
            return api_base_from_execution_url(str(value)) or None
    return None
