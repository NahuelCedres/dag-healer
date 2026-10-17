"""Optional ``on_failure_callback`` that reports task failures to dag-healer.

The listener already reports failures without touching DAG code; this callback is
for deployments where the plugin cannot be installed on the workers. Usage::

    import dag_healer

    default_args = {"on_failure_callback": dag_healer.callback}

It never raises (a failing callback must not hide the task error) and has no
dependency on Airflow itself.
"""

from __future__ import annotations

import logging
from collections.abc import Mapping
from typing import Any

from dag_healer.airflow import airflow_api_url
from dag_healer.ingest import build_payload, report_failure

log = logging.getLogger(__name__)

__all__ = ["on_failure_callback"]


def on_failure_callback(context: Mapping[str, Any] | None = None) -> None:
    """Report the failed task instance described by an Airflow task ``context``."""
    try:
        ctx: Mapping[str, Any] = context if isinstance(context, Mapping) else {}
        task_instance = ctx.get("task_instance")
        if task_instance is None:
            task_instance = ctx.get("ti")
        payload = build_payload(task_instance, context=ctx, error=ctx.get("exception"), source="callback")
        report_failure(payload, api_url=airflow_api_url())
    except Exception:
        log.warning("dag-healer: on_failure_callback could not report the failure", exc_info=True)
