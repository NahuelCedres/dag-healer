"""Fast-path delivery of task failures from Airflow processes to the dag-healer API.

This module runs inside Airflow task processes (listener and ``on_failure_callback``)
and inside the api-server, so it only uses the standard library and never raises:
a failure to report is logged and the REST poller of the worker picks the failure up
later anyway (events are only a latency hint, REST is the source of truth).

It is the task runners' side of the Airflow adapter's ``task-failed`` push channel
(``POST /events/task-failed``). Other orchestrators push to ``POST /events/{channel}``,
where their adapter reads the payload; nothing here is shared with them.
"""

from __future__ import annotations

import json
import logging
import os
import socket
import traceback
import urllib.error
import urllib.request
from collections.abc import Mapping
from datetime import datetime, timezone
from typing import Any

from dag_healer._version import __version__

log = logging.getLogger(__name__)

INGEST_PATH = "/healer/api/v1/events/task-failed"
TOKEN_HEADER = "X-Healer-Token"
MAX_ERROR_CHARS = 4096
TRUNCATION_MARKER = "...[truncated]...\n"

ENV_API_URL = "HEALER_API_URL"
ENV_TOKEN = "HEALER_INGEST_TOKEN"
ENV_EXECUTION_API_URL = "AIRFLOW__CORE__EXECUTION_API_SERVER_URL"
ENV_API_BASE_URL = "AIRFLOW__API__BASE_URL"

PAYLOAD_KEYS = (
    "dag_id",
    "task_id",
    "run_id",
    "map_index",
    "try_number",
    "max_tries",
    "state",
    "hostname",
    "error",
    "source",
    "reported_at",
)


# --------------------------------------------------------------------------- payload


def build_payload(
    task_instance: Any = None,
    *,
    context: Mapping[str, Any] | None = None,
    error: Any = None,
    source: str,
) -> dict[str, Any]:
    """Build the JSON payload describing a failed task instance.

    Duck-typed: works with the Task SDK ``RuntimeTaskInstance`` (task runner), the ORM
    ``TaskInstance`` (api-server) and a task ``context`` mapping. Missing attributes
    fall back to the context (``ti``/``task_instance``, ``dag_run``, ``run_id``,
    ``try_number``, ``exception``) and then to neutral defaults.
    """
    ctx: Mapping[str, Any] = context if isinstance(context, Mapping) else {}
    ti = task_instance if task_instance is not None else _ti_from_context(ctx)
    dag_run = ctx.get("dag_run")
    if error is None:
        error = ctx.get("exception")
    return {
        "dag_id": _first_str(_attr(ti, "dag_id"), _attr(dag_run, "dag_id"), _dag_id_from_context(ctx)),
        "task_id": _first_str(_attr(ti, "task_id"), _attr(ctx.get("task"), "task_id")),
        "run_id": _first_str(_attr(ti, "run_id"), ctx.get("run_id"), _attr(dag_run, "run_id")),
        "map_index": _to_int(_attr(ti, "map_index"), default=-1),
        "try_number": _to_int(_first(_attr(ti, "try_number"), ctx.get("try_number")), default=None),
        "max_tries": _to_int(_attr(ti, "max_tries"), default=None),
        "state": _state_str(_attr(ti, "state")),
        "hostname": _first_str(_attr(ti, "hostname"), _local_hostname()),
        "error": format_error(error),
        "source": source,
        "reported_at": datetime.now(timezone.utc).isoformat(),
    }


def format_error(error: Any, *, limit: int = MAX_ERROR_CHARS) -> str:
    """Render an exception (with traceback when available) or message as text.

    Long texts keep their tail, where Python puts the failing frame and the
    exception message, and never exceed ``limit`` characters.
    """
    if error is None:
        return ""
    try:
        if isinstance(error, BaseException):
            if error.__traceback__ is not None:
                text = "".join(traceback.format_exception(type(error), error, error.__traceback__))
            else:
                text = "".join(traceback.format_exception_only(type(error), error))
        else:
            text = str(error)
    except Exception:  # a broken __str__ must not break reporting
        text = f"<unprintable {type(error).__name__}>"
    text = text.strip()
    if len(text) <= limit:
        return text
    keep = limit - len(TRUNCATION_MARKER)
    if keep <= 0:
        return text[-limit:] if limit > 0 else ""
    return TRUNCATION_MARKER + text[-keep:]


def _ti_from_context(ctx: Mapping[str, Any]) -> Any:
    ti = ctx.get("task_instance")
    return ti if ti is not None else ctx.get("ti")


def _dag_id_from_context(ctx: Mapping[str, Any]) -> Any:
    dag = ctx.get("dag")
    return _attr(dag, "dag_id")


def _attr(obj: Any, name: str) -> Any:
    if obj is None:
        return None
    try:
        return getattr(obj, name, None)
    except Exception:  # properties of lazily loaded ORM objects may raise
        return None


def _first(*values: Any) -> Any:
    for value in values:
        if value is not None:
            return value
    return None


def _first_str(*values: Any) -> str | None:
    value = _first(*(v for v in values if v != ""))
    return None if value is None else str(value)


def _to_int(value: Any, *, default: int | None) -> int | None:
    if value is None or isinstance(value, bool):
        return default
    try:
        return int(value)
    except (TypeError, ValueError):
        return default


def _state_str(state: Any) -> str | None:
    if state is None:
        return None
    value = getattr(state, "value", state)
    return str(value)


def _local_hostname() -> str | None:
    # gethostname() rather than getfqdn(): no DNS round-trip inside a failing task.
    try:
        return socket.gethostname() or None
    except OSError:
        return None


# --------------------------------------------------------------------------- delivery


def api_base_from_execution_url(url: str) -> str:
    """Turn ``http://host:8080/execution/`` into ``http://host:8080``."""
    base = url.strip().rstrip("/")
    if base.endswith("/execution"):
        base = base[: -len("/execution")]
    return base.rstrip("/")


def resolve_api_url(api_url: str | None = None) -> str | None:
    """Resolve the Airflow api-server base URL the healer API is mounted on.

    Order: explicit argument, ``HEALER_API_URL``, ``AIRFLOW__CORE__EXECUTION_API_SERVER_URL``
    without its ``/execution/`` suffix, ``AIRFLOW__API__BASE_URL``.
    """
    explicit = (api_url or "").strip() or os.environ.get(ENV_API_URL, "").strip()
    if explicit:
        return explicit.rstrip("/")
    execution = os.environ.get(ENV_EXECUTION_API_URL, "").strip()
    if execution:
        return api_base_from_execution_url(execution) or None
    base = os.environ.get(ENV_API_BASE_URL, "").strip()
    return base.rstrip("/") or None


def resolve_token(token: str | None = None) -> str | None:
    """Resolve the ingest token: explicit argument, else ``HEALER_INGEST_TOKEN``."""
    value = (token or "").strip() or os.environ.get(ENV_TOKEN, "").strip()
    return value or None


def report_failure(
    payload: Mapping[str, Any],
    *,
    api_url: str | None = None,
    token: str | None = None,
    timeout: float = 2.0,
) -> bool:
    """POST a failure payload to ``<api_url>/healer/api/v1/events/task-failed``.

    Never raises. Returns True when the API accepted the event (2xx) and False when
    reporting is not configured (no token or no URL) or delivery failed.
    """
    try:
        base = resolve_api_url(api_url)
        secret = resolve_token(token)
        if not secret or not base:
            log.debug("dag-healer: fast-path reporting skipped (missing %s)", "token" if base else "API URL")
            return False
        request = urllib.request.Request(
            base + INGEST_PATH,
            data=json.dumps(dict(payload), default=str).encode("utf-8"),
            headers={
                "Content-Type": "application/json",
                TOKEN_HEADER: secret,
                "User-Agent": f"dag-healer/{__version__}",
            },
            method="POST",
        )
        with urllib.request.urlopen(request, timeout=timeout) as response:
            status = int(getattr(response, "status", 0) or response.getcode() or 0)
        if 200 <= status < 300:
            return True
        log.warning("dag-healer: failure report rejected with HTTP %s", status)
        return False
    except urllib.error.HTTPError as exc:
        log.warning("dag-healer: failure report rejected with HTTP %s", exc.code)
        exc.close()
    except Exception as exc:  # network errors, timeouts, bad URLs, unserializable payloads
        log.warning("dag-healer: could not report task failure: %s", exc)
    return False
