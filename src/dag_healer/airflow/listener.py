"""Airflow listener that reports task failures to the dag-healer API.

Registered through ``HealerPlugin.listeners``. ``on_task_instance_failed`` runs in the
task runner of the worker (``RuntimeTaskInstance``) or, when the state is changed via
the REST API, in the api-server (ORM ``TaskInstance``). It fires for both FAILED and
UP_FOR_RETRY, which the worker tells apart when it confirms the event over REST.

This module is imported in every task process: keep imports light and never raise.
Hook parameter names must match the Airflow hookspecs exactly (pluggy matches by name).
"""

from __future__ import annotations

import logging
from typing import Any

from airflow.listeners import hookimpl

from dag_healer.airflow import airflow_api_url
from dag_healer.ingest import build_payload, report_failure

log = logging.getLogger(__name__)


@hookimpl
def on_task_instance_failed(previous_state: Any, task_instance: Any, error: Any) -> None:
    """Send a fast-path failure event for ``task_instance`` (best effort)."""
    try:
        payload = build_payload(task_instance, error=error, source="listener")
        report_failure(payload, api_url=airflow_api_url())
    except Exception:
        log.warning("dag-healer: listener could not report the task failure", exc_info=True)


@hookimpl
def on_dag_run_failed(dag_run: Any, msg: Any) -> None:
    """Log failed DAG runs (runs in the scheduler; incidents come from task failures)."""
    try:
        log.debug(
            "dag-healer: dag run failed dag_id=%s run_id=%s msg=%s",
            getattr(dag_run, "dag_id", None),
            getattr(dag_run, "run_id", None),
            msg,
        )
    except Exception:
        pass
