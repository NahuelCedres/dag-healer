"""Browser-facing URLs of the dag-healer views inside the Airflow UI.

The links end up in alert emails, PR descriptions and API responses, so they must use
the address *people* reach Airflow at, not the internal one the worker talks to:

1. ``$AIRFLOW__API__BASE_URL`` (Airflow's own ``[api] base_url``);
2. ``$HEALER_PUBLIC_URL``;
3. ``airflow.api_url`` from ``healer.yaml``.

Every path segment (DAG, run and task ids) is percent-encoded: run ids such as
``manual__2026-10-17T10:00:00+00:00`` contain ``:`` and ``+``.

The views are the ``react_apps`` of the plugin, mounted by Airflow as tabs at
``plugin/<url_route>`` (routes :data:`NAV_ROUTE`, :data:`DAG_ROUTE`, :data:`TI_ROUTE`).
This module is pure string handling: it never imports Airflow.
"""

from __future__ import annotations

import os
from collections.abc import Mapping
from typing import Any
from urllib.parse import quote

ENV_AIRFLOW_BASE_URL = "AIRFLOW__API__BASE_URL"
ENV_PUBLIC_URL = "HEALER_PUBLIC_URL"
DEFAULT_BASE_URL = "http://localhost:8080"

#: ``url_route`` of the top-level Healer page (``nav`` destination).
NAV_ROUTE = "healer"
#: ``url_route`` of the DAG tab.
DAG_ROUTE = "healer-dag"
#: ``url_route`` of the task-instance tab.
TI_ROUTE = "healer-ti"
#: Prefix of the dag-healer HTTP API (FastAPI app mounted by the plugin).
API_PREFIX = "/healer/api/v1"


def _segment(value: Any) -> str:
    """Percent-encode one path segment (``/``, ``:`` and ``+`` included)."""
    return quote(str(value), safe="")


def _strip(value: str | None) -> str:
    return (value or "").strip().rstrip("/")


def airflow_base_url(cfg: Any = None) -> str:
    """Return the public base URL of the Airflow UI (no trailing slash).

    ``$AIRFLOW__API__BASE_URL``, else ``$HEALER_PUBLIC_URL``, else ``cfg.airflow.api_url``
    (``http://localhost:8080`` when nothing is configured).
    """
    for name in (ENV_AIRFLOW_BASE_URL, ENV_PUBLIC_URL):
        value = _strip(os.environ.get(name))
        if value:
            return value
    api_url = _strip(getattr(getattr(cfg, "airflow", None), "api_url", None))
    return api_url or DEFAULT_BASE_URL


def task_instance_url(cfg: Any, dag_id: str, run_id: str, task_id: str, map_index: int | None = -1) -> str:
    """URL of the "Healer incident" tab of a task instance.

    ``<base>/dags/<dag>/runs/<run>/tasks/<task>[/mapped/<i>]/plugin/healer-ti``; the
    ``mapped`` part is only added for mapped task instances (``map_index >= 0``).
    """
    url = f"{airflow_base_url(cfg)}/dags/{_segment(dag_id)}/runs/{_segment(run_id)}/tasks/{_segment(task_id)}"
    index = _as_map_index(map_index)
    if index >= 0:
        url += f"/mapped/{index}"
    return f"{url}/plugin/{TI_ROUTE}"


def dag_url(cfg: Any, dag_id: str) -> str:
    """URL of the "Healer incidents" tab of a DAG (``<base>/dags/<dag>/plugin/healer-dag``)."""
    return f"{airflow_base_url(cfg)}/dags/{_segment(dag_id)}/plugin/{DAG_ROUTE}"


def healer_page_url(cfg: Any) -> str:
    """URL of the top-level Healer page (``<base>/plugin/healer``)."""
    return f"{airflow_base_url(cfg)}/plugin/{NAV_ROUTE}"


def pr_md_url(cfg: Any, dag_id: str, incident_id: str) -> str:
    """URL of the ``PR.md`` the API serves for an incident without a GitHub pull request."""
    return (
        f"{airflow_base_url(cfg)}{API_PREFIX}/dags/{_segment(dag_id)}/incidents/{_segment(incident_id)}/pr.md"
    )


def incident_links(cfg: Any, incident: Mapping[str, Any]) -> dict[str, str]:
    """Return ``{task_instance, dag, healer}`` links for an incident dict (``Incident.to_dict()``)."""
    dag_id = str(incident.get("dag_id") or "")
    return {
        "task_instance": task_instance_url(
            cfg,
            dag_id,
            str(incident.get("run_id") or ""),
            str(incident.get("task_id") or ""),
            incident.get("map_index", -1),
        ),
        "dag": dag_url(cfg, dag_id),
        "healer": healer_page_url(cfg),
    }


def pr_link(cfg: Any, incident: Mapping[str, Any]) -> str | None:
    """Return the GitHub PR URL of an incident, else its ``PR.md`` URL, else ``None``."""
    pr_url = incident.get("pr_url")
    if isinstance(pr_url, str) and pr_url.strip():
        return pr_url.strip()
    if incident.get("pr_md_path") or incident.get("has_pr_md"):
        return pr_md_url(cfg, str(incident.get("dag_id") or ""), str(incident.get("id") or ""))
    return None


def _as_map_index(value: Any) -> int:
    try:
        return int(value)
    except (TypeError, ValueError):
        return -1


__all__ = [
    "API_PREFIX",
    "DAG_ROUTE",
    "DEFAULT_BASE_URL",
    "ENV_AIRFLOW_BASE_URL",
    "ENV_PUBLIC_URL",
    "NAV_ROUTE",
    "TI_ROUTE",
    "airflow_base_url",
    "dag_url",
    "healer_page_url",
    "incident_links",
    "pr_link",
    "pr_md_url",
    "task_instance_url",
]
