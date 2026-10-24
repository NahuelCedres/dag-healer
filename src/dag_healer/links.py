"""Browser-facing URLs of the dag-healer views, and links into the orchestrator's own UI.

The links end up in alert emails, PR descriptions and API responses, so they must use
the address *people* reach the views at, not the internal one the worker talks to
(:func:`airflow_base_url`, also available as :func:`public_base_url`):

1. ``$AIRFLOW__API__BASE_URL`` (Airflow's own ``[api] base_url``);
2. ``$HEALER_PUBLIC_URL``;
3. ``serve.public_url`` from ``healer.yaml``;
4. ``airflow.api_url`` from ``healer.yaml`` when the orchestrator is Airflow, otherwise the
   address of the standalone host (``http://<serve.host>:<serve.port>``).

Every path segment (DAG, run and task ids) is percent-encoded: run ids such as
``manual__2026-10-17T10:00:00+00:00`` contain ``:`` and ``+``.

Both hosts serve the Healer views on the same URL shapes: the Airflow plugin mounts its
``react_apps`` as tabs at ``plugin/<url_route>`` (routes :data:`NAV_ROUTE`, :data:`DAG_ROUTE`,
:data:`TI_ROUTE`) and the standalone host ``dag-healer serve`` answers the same paths.
:func:`native_links` adds the orchestrator's own pages (``{"AWS console": url}``) through its
adapter; :func:`shown_native_links` leaves them out for Airflow, whose UI hosts the Healer
views (the task instance link already opens it). This module never imports Airflow, and
imports the adapter registry only when native links are asked for.
"""

from __future__ import annotations

import logging
import os
from collections.abc import Mapping
from typing import TYPE_CHECKING, Any
from urllib.parse import quote

from dag_healer.core.ids import DEFAULT_ORCHESTRATOR, AttemptRef

if TYPE_CHECKING:
    from dag_healer.core.ports import OrchestratorAdapter

log = logging.getLogger(__name__)

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

_AIRFLOW_TYPE = DEFAULT_ORCHESTRATOR
#: ``orchestrator.type`` values that select the Airflow adapter: its entry-point name and its
#: explicit factory (``dag_healer.airflow.adapter.FACTORY_TARGET``; not imported, that module
#: imports this one).
_AIRFLOW_TYPES = frozenset({_AIRFLOW_TYPE, "dag_healer.airflow.adapter:FACTORY"})


def _segment(value: Any) -> str:
    """Percent-encode one path segment (``/``, ``:`` and ``+`` included)."""
    return quote(str(value), safe="")


def _strip(value: Any) -> str:
    return value.strip().rstrip("/") if isinstance(value, str) else ""


def _orchestrator_type(cfg: Any) -> str:
    """``cfg.orchestrator.type``; ``"airflow"`` when the configuration has no such section."""
    value = getattr(getattr(cfg, "orchestrator", None), "type", None)
    return value.strip() if isinstance(value, str) and value.strip() else _AIRFLOW_TYPE


def _is_airflow(cfg: Any) -> bool:
    """True when ``cfg`` selects the Airflow adapter (by its type name or its explicit factory)."""
    return _orchestrator_type(cfg) in _AIRFLOW_TYPES


def _standalone_url(serve: Any) -> str:
    """``http://<serve.host>:<serve.port>`` (an IPv6 host in brackets); ``""`` when unset."""
    host = _strip(getattr(serve, "host", None))
    port = getattr(serve, "port", None)
    if not host or isinstance(port, bool) or not isinstance(port, int):
        return ""
    if ":" in host and not host.startswith("["):
        host = f"[{host}]"
    return f"http://{host}:{port}"


def airflow_base_url(cfg: Any = None) -> str:
    """Return the public base URL of the Healer views (no trailing slash).

    ``$AIRFLOW__API__BASE_URL``, else ``$HEALER_PUBLIC_URL``, else ``cfg.serve.public_url``,
    else ``cfg.airflow.api_url`` for Airflow (``type: airflow`` or the adapter's explicit
    ``"dag_healer.airflow.adapter:FACTORY"``, also when ``cfg`` has no ``orchestrator``
    section) or ``http://<serve.host>:<serve.port>`` for any other orchestrator type
    (``http://localhost:8080`` when nothing is configured). ``cfg`` may be ``None`` or any
    object with those attributes.
    """
    for name in (ENV_AIRFLOW_BASE_URL, ENV_PUBLIC_URL):
        value = _strip(os.environ.get(name))
        if value:
            return value
    serve = getattr(cfg, "serve", None)
    public_url = _strip(getattr(serve, "public_url", None))
    if public_url:
        return public_url
    if _is_airflow(cfg):
        fallback = _strip(getattr(getattr(cfg, "airflow", None), "api_url", None))
    else:
        fallback = _standalone_url(serve)
    return fallback or DEFAULT_BASE_URL


#: The neutral name of :func:`airflow_base_url` (the views are not always inside Airflow).
public_base_url = airflow_base_url


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


def _adapter_for(cfg: Any, adapter: OrchestratorAdapter | None) -> OrchestratorAdapter:
    """``adapter``, else the (cached, I/O-free) adapter of ``cfg``; raises when it cannot be built."""
    if adapter is not None:
        return adapter
    from dag_healer.orchestrators import registry  # lazy: airflow/adapter.py imports this module

    return registry.get_adapter(cfg)


def native_links(
    cfg: Any, incident: Mapping[str, Any], *, adapter: OrchestratorAdapter | None = None
) -> dict[str, str]:
    """Labelled links into the orchestrator's own UI for an incident dict (``Incident.to_dict()``).

    ``adapter.links.links(ref, native_ref)``, with the adapter of ``cfg`` when none is given.
    Only absolute http(s) links are kept. Never raises: ``{}`` when the adapter cannot be
    built or its links fail, so a link never breaks a page or an email.
    """
    try:
        resolved = _adapter_for(cfg, adapter)
        ref = AttemptRef.from_row(incident, default_orchestrator=resolved.instance)
        native = incident.get("native_ref")
        found = resolved.links.links(ref, native if isinstance(native, Mapping) else {})
        return {
            str(label): url
            for label, url in dict(found).items()
            if isinstance(url, str) and url.startswith(("http://", "https://"))
        }
    except Exception as exc:  # links are best effort
        incident_id = incident.get("id") if isinstance(incident, Mapping) else None
        log.debug("dag-healer: no native links for incident %s: %s", incident_id, type(exc).__name__)
        return {}


def shown_native_links(
    cfg: Any, incident: Mapping[str, Any], *, adapter: OrchestratorAdapter | None = None
) -> dict[str, str]:
    """The native links to show next to the Healer links (API responses, alert emails).

    ``{}`` for Airflow, whose UI hosts the Healer views (the task instance link already opens
    it), and when the adapter cannot be built; otherwise :func:`native_links`.
    """
    try:
        resolved = _adapter_for(cfg, adapter)
        if resolved.type == _AIRFLOW_TYPE:
            return {}
    except Exception as exc:  # links are best effort
        log.debug("dag-healer: no orchestrator adapter for native links: %s", type(exc).__name__)
        return {}
    return native_links(cfg, incident, adapter=resolved)


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
    "native_links",
    "pr_link",
    "pr_md_url",
    "public_base_url",
    "shown_native_links",
    "task_instance_url",
]
