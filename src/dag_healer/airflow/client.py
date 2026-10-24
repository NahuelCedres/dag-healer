"""Small synchronous client for the Airflow 3 REST API (``/api/v2``), built on httpx.

It never imports Airflow, so the worker (and the unit tests) can run anywhere. Shapes were
checked against a running Airflow 3.3.2 api-server:

* ``POST /auth/token {"username", "password"}`` returns ``201 {"access_token": ...}`` with the
  FAB and Simple auth managers. The JWT is cached and requested again once when an API call
  answers ``401``.
* Task instance logs (``GET .../logs/{try_number}?full_content=true`` with
  ``Accept: application/json``) are ``{"content": [StructuredLogMessage...],
  "continuation_token": null}``. A failure is logged as ``{"event": "Task failed with
  exception", "level": "error", "error_detail": [...]}`` where ``error_detail`` is the
  structlog exception chain (outermost exception first, ``is_cause`` set on an exception that
  is the ``__cause__`` of the previous one). :func:`render_log_content` turns that into text
  with Python-style tracebacks so the rest of dag-healer can treat every log the same way.
* Errors are ``{"detail": str | dict | list}``; they become :class:`AirflowAPIError`.

Run ids contain ``:`` and ``+`` (``manual__2026-10-17T18:48:46.093857+00:00``), so every path
segment is percent-encoded.
"""

from __future__ import annotations

import logging
import threading
from collections.abc import Iterable, Mapping, Sequence
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import TYPE_CHECKING, Any
from urllib.parse import quote

import httpx

# Moved to dag_healer.core.tracebacks; re-exported here (same objects).
from dag_healer.core.tracebacks import CAUSE_SEPARATOR, CONTEXT_SEPARATOR, TRACEBACK_HEADER

if TYPE_CHECKING:
    from dag_healer.config import HealerConfig

logger = logging.getLogger(__name__)

API_PREFIX = "/api/v2"
TOKEN_PATH = "/auth/token"
DEFAULT_TIMEOUT = 10.0
MAX_DETAIL_CHARS = 500

_GROUP_START = "::group::"
_GROUP_END = "::endgroup::"


class AirflowAPIError(Exception):
    """An Airflow REST call failed.

    ``status_code`` is the HTTP status (``None`` for network errors and timeouts) and
    ``detail`` the server's ``detail`` message (or the transport error).
    """

    def __init__(self, detail: str = "", status_code: int | None = None) -> None:
        self.detail = detail
        self.status_code = status_code
        prefix = (
            f"Airflow API error {status_code}" if status_code is not None else "Airflow API request failed"
        )
        super().__init__(f"{prefix}: {detail}" if detail else prefix)


@dataclass
class TaskInstanceInfo:
    """The parts of a task instance (``TaskInstanceResponse``) dag-healer uses.

    ``raw`` keeps the full API object.
    """

    dag_id: str
    run_id: str
    task_id: str
    map_index: int = -1
    try_number: int = 0
    max_tries: int = 0
    state: str | None = None
    operator: str | None = None
    start_date: str | None = None
    end_date: str | None = None
    hostname: str | None = None
    raw: dict[str, Any] = field(default_factory=dict, repr=False)

    @classmethod
    def from_api(cls, data: Mapping[str, Any]) -> TaskInstanceInfo:
        """Build from a ``TaskInstanceResponse`` dict (``dag_run_id`` maps to ``run_id``)."""
        return cls(
            dag_id=str(data.get("dag_id") or ""),
            run_id=str(data.get("dag_run_id") or data.get("run_id") or ""),
            task_id=str(data.get("task_id") or ""),
            map_index=_as_int(data.get("map_index"), -1),
            try_number=_as_int(data.get("try_number"), 0),
            max_tries=_as_int(data.get("max_tries"), 0),
            state=_as_opt_str(data.get("state")),
            operator=_as_opt_str(data.get("operator") or data.get("operator_name")),
            start_date=_as_opt_str(data.get("start_date")),
            end_date=_as_opt_str(data.get("end_date")),
            hostname=_as_opt_str(data.get("hostname")),
            raw=dict(data),
        )

    @property
    def is_final_try(self) -> bool:
        """True when no retry is left (``try_number`` > ``max_tries``)."""
        return self.try_number > self.max_tries


def _as_int(value: Any, default: int) -> int:
    try:
        return int(value)
    except (TypeError, ValueError):
        return default


def _as_opt_str(value: Any) -> str | None:
    if value is None:
        return None
    text = str(value)
    return text or None


def _segment(value: Any) -> str:
    """Percent-encode one URL path segment (``~`` stays as Airflow's "all" wildcard)."""
    return quote(str(value), safe="~")


def format_timestamp(value: datetime) -> str:
    """Render a datetime as an ISO-8601 UTC string with a ``Z`` suffix (naive = UTC)."""
    if value.tzinfo is None:
        value = value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc).isoformat().replace("+00:00", "Z")


def _error_detail(response: httpx.Response) -> str:
    """Extract a readable message from an Airflow error response."""
    try:
        payload = response.json()
    except ValueError:
        payload = None
    detail: Any = payload.get("detail", payload) if isinstance(payload, dict) else payload
    if isinstance(detail, list):
        parts = []
        for item in detail:
            if isinstance(item, Mapping):
                location = ".".join(str(part) for part in item.get("loc", ()) or ())
                message = str(item.get("msg", item))
                parts.append(f"{location}: {message}" if location else message)
            else:
                parts.append(str(item))
        text = "; ".join(parts)
    elif isinstance(detail, Mapping):
        text = str(detail.get("message") or detail.get("reason") or detail)
    elif detail is not None:
        text = str(detail)
    else:
        text = response.text.strip() or response.reason_phrase
    return text[:MAX_DETAIL_CHARS]


# =========================================================================== log rendering


def _frame_lines(frames: Iterable[Any]) -> list[str]:
    lines: list[str] = []
    for frame in frames or ():
        if not isinstance(frame, Mapping):
            continue
        filename = frame.get("filename", "<unknown>")
        lineno = frame.get("lineno", "?")
        name = frame.get("name")
        lines.append(f'  File "{filename}", line {lineno}' + (f", in {name}" if name else ""))
        source = frame.get("line")
        if isinstance(source, str) and source.strip():
            lines.append(f"    {source.strip()}")
    return lines


def _syntax_error_lines(info: Any) -> list[str]:
    if not isinstance(info, Mapping):
        return []
    lines = [f'  File "{info.get("filename", "<unknown>")}", line {info.get("lineno", "?")}']
    source = info.get("line")
    if isinstance(source, str) and source.strip():
        stripped = source.rstrip("\n")
        indent = len(stripped) - len(stripped.lstrip())
        lines.append(f"    {stripped.strip()}")
        offset = _as_int(info.get("offset"), 0)
        if offset > 0:
            lines.append("    " + " " * max(0, offset - 1 - indent) + "^")
    return lines


def _exception_line(stack: Mapping[str, Any]) -> list[str]:
    exc_type = str(stack.get("exc_type") or "Exception")
    value = stack.get("exc_value")
    syntax = stack.get("syntax_error")
    if isinstance(syntax, Mapping) and syntax.get("msg") and not value:
        value = syntax.get("msg")
    value_text = "" if value is None else str(value)
    head, *rest = (f"{exc_type}: {value_text}" if value_text else exc_type).split("\n")
    return [head, *rest]


def _stack_lines(stack: Mapping[str, Any]) -> list[str]:
    """Render one exception (no chain) like Python's ``traceback`` module."""
    lines: list[str] = []
    frames = stack.get("frames") or []
    if frames:
        lines.append(TRACEBACK_HEADER)
        lines.extend(_frame_lines(frames))
    lines.extend(_syntax_error_lines(stack.get("syntax_error")))
    lines.extend(_exception_line(stack))
    for note in stack.get("exc_notes") or ():
        lines.append(str(note))
    if stack.get("is_group"):
        for number, sub in enumerate(stack.get("exceptions") or (), start=1):
            sub_stacks = sub.get("stacks", []) if isinstance(sub, Mapping) else sub
            lines.append(f"  +---------------- {number} ----------------")
            lines.extend(f"    | {line}" if line else "    |" for line in render_exception_chain(sub_stacks))
    return lines


def render_exception_chain(stacks: Any) -> list[str]:
    """Render a structlog exception chain (``error_detail``) as Python traceback lines.

    ``stacks`` lists the outermost exception first; like Python, the output prints the root
    cause first and the exception that was actually raised last, joined by the usual
    "direct cause" / "during handling" separators.
    """
    if isinstance(stacks, Mapping):
        stacks = stacks.get("stacks", [stacks]) if "exc_type" not in stacks else [stacks]
    items = [stack for stack in (stacks or []) if isinstance(stack, Mapping)]
    lines: list[str] = []
    ordered = list(reversed(items))
    for position, stack in enumerate(ordered):
        if position:
            previous = ordered[position - 1]
            separator = CAUSE_SEPARATOR if previous.get("is_cause") else CONTEXT_SEPARATOR
            lines.extend(["", separator, ""])
        lines.extend(_stack_lines(stack))
    return lines


def render_log_message(message: Any) -> list[str]:
    """Render one ``StructuredLogMessage`` (or plain string) as text lines.

    Format: ``[timestamp] LEVEL - event``, followed by any exception information
    (``error_detail``, ``exception`` or ``exc_info``) as a Python-style traceback. Airflow's
    ``::group::`` markers are reduced to their title and ``::endgroup::`` lines are dropped.
    """
    if message is None:
        return []
    if not isinstance(message, Mapping):
        return str(message).splitlines()
    event = message.get("event")
    text = "" if event is None else str(event)
    lines: list[str] = []
    if text.strip() != _GROUP_END:
        if text.startswith(_GROUP_START):
            text = text[len(_GROUP_START) :].strip()
        prefix = []
        if message.get("timestamp"):
            prefix.append(f"[{message['timestamp']}]")
        if message.get("level"):
            prefix.append(f"{str(message['level']).upper()} -")
        first, *rest = text.split("\n") if text else [""]
        lines.append(" ".join([*prefix, first]).rstrip() if prefix else first)
        lines.extend(rest)
    for key in ("error_detail", "exception", "exc_info"):
        detail = message.get(key)
        if not detail or detail is True:
            continue
        if isinstance(detail, str):
            lines.extend(detail.rstrip("\n").splitlines())
        else:
            lines.extend(render_exception_chain(detail))
    return lines


def render_log_content(content: Any) -> str:
    """Render the ``content`` of a ``TaskInstancesLogResponse`` (or a plain string) as text."""
    if content is None:
        return ""
    if isinstance(content, str):
        return content
    if isinstance(content, Mapping):
        if "content" in content:
            return render_log_content(content["content"])
        content = [content]
    lines: list[str] = []
    for item in content:
        if isinstance(item, str):
            lines.extend(item.rstrip("\n").split("\n"))
        else:
            lines.extend(render_log_message(item))
    return "\n".join(lines)


# =========================================================================== client


class AirflowClient:
    """Synchronous Airflow REST API v2 client.

    Authentication: a static bearer ``token``, or ``username``/``password`` exchanged at
    ``POST {base_url}/auth/token`` for a JWT that is cached and refreshed once when a call
    answers ``401``. Without credentials requests are sent unauthenticated. ``transport`` is
    an optional httpx transport (``httpx.MockTransport`` in tests).
    """

    def __init__(
        self,
        base_url: str,
        *,
        username: str | None = None,
        password: str | None = None,
        token: str | None = None,
        timeout: float = DEFAULT_TIMEOUT,
        transport: httpx.BaseTransport | None = None,
    ) -> None:
        cleaned = (base_url or "").strip().rstrip("/")
        if cleaned.endswith(API_PREFIX):
            cleaned = cleaned[: -len(API_PREFIX)]
        if not cleaned:
            raise ValueError("Airflow base_url must not be empty")
        self.base_url = cleaned
        self.username = username or None
        self._password = password or None
        self._token: str | None = token or None
        self._lock = threading.Lock()
        from dag_healer import __version__

        self._http = httpx.Client(
            base_url=cleaned,
            timeout=timeout,
            transport=transport,
            headers={"Accept": "application/json", "User-Agent": f"dag-healer/{__version__}"},
        )

    @classmethod
    def from_config(cls, cfg: HealerConfig, **kwargs: Any) -> AirflowClient:
        """Create a client from ``cfg.airflow`` (``api_url``, ``username``, ``password``)."""
        return cls(
            cfg.airflow.api_url,
            username=cfg.airflow.username,
            password=cfg.airflow.password,
            **kwargs,
        )

    # ------------------------------------------------------------------ lifecycle
    def close(self) -> None:
        """Close the underlying HTTP connection pool."""
        self._http.close()

    def __enter__(self) -> AirflowClient:
        return self

    def __exit__(self, *exc_info: object) -> None:
        self.close()

    def __repr__(self) -> str:
        return f"AirflowClient(base_url={self.base_url!r}, username={self.username!r})"

    # ------------------------------------------------------------------ auth
    @property
    def can_login(self) -> bool:
        """True when username and password are configured."""
        return bool(self.username and self._password)

    def login(self) -> str:
        """Exchange username/password for a JWT (``POST /auth/token``) and cache it."""
        if not self.can_login:
            raise AirflowAPIError("no Airflow username/password configured", status_code=401)
        try:
            response = self._http.post(
                TOKEN_PATH,
                json={"username": self.username, "password": self._password},
                headers={"Content-Type": "application/json"},
            )
        except httpx.HTTPError as exc:
            raise AirflowAPIError(f"POST {TOKEN_PATH}: {type(exc).__name__}: {exc}") from exc
        if response.status_code >= 400:
            raise AirflowAPIError(
                f"login failed: {_error_detail(response)}", status_code=response.status_code
            )
        try:
            token = response.json().get("access_token")
        except (ValueError, AttributeError):
            token = None
        if not token:
            raise AirflowAPIError("login response has no access_token", status_code=response.status_code)
        self._token = str(token)
        logger.debug("Obtained an Airflow API token for %s", self.username)
        return self._token

    def _auth_header(self) -> dict[str, str]:
        with self._lock:
            if self._token is None and self.can_login:
                self.login()
            return {"Authorization": f"Bearer {self._token}"} if self._token else {}

    # ------------------------------------------------------------------ transport
    def _request(
        self,
        method: str,
        path: str,
        *,
        params: Any = None,
        json: Any = None,
        headers: Mapping[str, str] | None = None,
        auth: bool = True,
    ) -> httpx.Response:
        """Send one request; re-login once on 401; raise :class:`AirflowAPIError` on errors."""
        for attempt in (1, 2):
            request_headers = dict(headers or {})
            if auth:
                request_headers.update(self._auth_header())
            try:
                response = self._http.request(method, path, params=params, json=json, headers=request_headers)
            except httpx.HTTPError as exc:
                raise AirflowAPIError(f"{method} {path}: {type(exc).__name__}: {exc}") from exc
            logger.debug("Airflow API %s %s -> %s", method, path, response.status_code)
            if response.status_code == 401 and auth and attempt == 1 and self.can_login:
                with self._lock:
                    self._token = None
                continue
            if response.status_code >= 400:
                raise AirflowAPIError(_error_detail(response), status_code=response.status_code)
            return response
        raise AssertionError("unreachable")  # pragma: no cover

    def _json(self, method: str, path: str, **kwargs: Any) -> Any:
        response = self._request(method, path, **kwargs)
        try:
            return response.json()
        except ValueError as exc:
            raise AirflowAPIError(
                f"{method} {path}: response is not JSON", status_code=response.status_code
            ) from exc

    @staticmethod
    def _ti_path(dag_id: str, run_id: str, task_id: str | None = None) -> str:
        path = f"{API_PREFIX}/dags/{_segment(dag_id)}/dagRuns/{_segment(run_id)}/taskInstances"
        return f"{path}/{_segment(task_id)}" if task_id is not None else path

    # ------------------------------------------------------------------ endpoints
    def health(self) -> dict[str, Any]:
        """``GET /api/v2/monitor/health`` (public endpoint, sent without credentials)."""
        data = self._json("GET", f"{API_PREFIX}/monitor/health", auth=False)
        return data if isinstance(data, dict) else {"raw": data}

    def list_failed_task_instances(
        self,
        *,
        updated_since: datetime | None = None,
        dag_ids: Iterable[str] | None = None,
        page_size: int = 100,
        max_pages: int = 20,
    ) -> list[TaskInstanceInfo]:
        """Failed task instances of every DAG, optionally updated since ``updated_since``.

        Pages through ``GET /api/v2/dags/~/dagRuns/~/taskInstances?state=failed`` with
        ``limit``/``offset`` (ordered by id for stable pages) and filters ``dag_ids``
        client-side. Stops after ``max_pages`` pages (logged as a warning).
        """
        wanted = set(dag_ids) if dag_ids is not None else None
        page_size = max(1, int(page_size))
        base_params: list[tuple[str, str | int]] = [("state", "failed")]
        if updated_since is not None:
            base_params.append(("updated_at_gte", format_timestamp(updated_since)))
        order: list[tuple[str, str | int]] = [("order_by", "id")]
        path = self._ti_path("~", "~")
        results: list[TaskInstanceInfo] = []
        offset = 0
        for page in range(max(1, int(max_pages))):
            params = [*base_params, ("limit", page_size), ("offset", offset), *order]
            try:
                data = self._json("GET", path, params=params)
            except AirflowAPIError as exc:
                if exc.status_code == 400 and order and page == 0:
                    logger.debug("Airflow rejected order_by=id (%s); paging without it", exc.detail)
                    order = []
                    data = self._json("GET", path, params=[*base_params, ("limit", page_size), ("offset", 0)])
                else:
                    raise
            items = data.get("task_instances", []) if isinstance(data, dict) else []
            for item in items:
                if not isinstance(item, Mapping):
                    continue
                info = TaskInstanceInfo.from_api(item)
                if wanted is None or info.dag_id in wanted:
                    results.append(info)
            offset += len(items)
            total = data.get("total_entries") if isinstance(data, dict) else None
            if len(items) < page_size or (isinstance(total, int) and offset >= total):
                break
        else:
            logger.warning(
                "Stopped listing failed task instances after %d pages of %d; some were not fetched",
                max_pages,
                page_size,
            )
        return results

    def get_task_instance(
        self, dag_id: str, run_id: str, task_id: str, map_index: int = -1
    ) -> TaskInstanceInfo:
        """``GET .../taskInstances/{task_id}`` (``/{map_index}`` for a mapped instance)."""
        path = self._ti_path(dag_id, run_id, task_id)
        if map_index is not None and int(map_index) >= 0:
            path = f"{path}/{int(map_index)}"
        data = self._json("GET", path)
        if not isinstance(data, Mapping):
            raise AirflowAPIError(f"unexpected task instance response for {task_id}")
        return TaskInstanceInfo.from_api(data)

    def get_task_log(
        self, dag_id: str, run_id: str, task_id: str, try_number: int, map_index: int = -1
    ) -> str:
        """The full log of one try, rendered as text (structured messages and tracebacks)."""
        path = f"{self._ti_path(dag_id, run_id, task_id)}/logs/{int(try_number)}"
        params = {"full_content": "true", "map_index": int(map_index if map_index is not None else -1)}
        response = self._request("GET", path, params=params, headers={"Accept": "application/json"})
        content_type = response.headers.get("content-type", "")
        if "json" not in content_type:
            return response.text
        try:
            data = response.json()
        except ValueError:
            return response.text
        return render_log_content(data)

    def get_dag(self, dag_id: str) -> dict[str, Any]:
        """``GET /api/v2/dags/{dag_id}`` (``fileloc``, ``relative_fileloc``, ``bundle_name``...)."""
        data = self._json("GET", f"{API_PREFIX}/dags/{_segment(dag_id)}")
        return data if isinstance(data, dict) else {}

    def get_dag_source(self, dag_id: str) -> str | None:
        """The DAG file source as parsed by Airflow (``GET /api/v2/dagSources/{dag_id}``).

        Returns ``None`` when Airflow has no source for the DAG (404).
        """
        try:
            response = self._request(
                "GET", f"{API_PREFIX}/dagSources/{_segment(dag_id)}", headers={"Accept": "application/json"}
            )
        except AirflowAPIError as exc:
            if exc.status_code == 404:
                return None
            raise
        if "json" not in response.headers.get("content-type", ""):
            return response.text or None
        try:
            data = response.json()
        except ValueError:
            return response.text or None
        content = data.get("content") if isinstance(data, Mapping) else data
        return content if isinstance(content, str) else None

    def clear_task_instances(
        self,
        dag_id: str,
        run_id: str,
        task_ids: Sequence[str | tuple[str, int]],
        *,
        only_failed: bool = True,
        include_downstream: bool = False,
    ) -> dict[str, Any]:
        """Clear task instances so the scheduler runs them again (not a dry run).

        ``task_ids`` items are task ids or ``(task_id, map_index)`` pairs. With
        ``include_downstream`` Airflow also clears the tasks after them in the run.
        ``only_failed`` keeps the ``failed`` and ``upstream_failed`` task instances only, so
        tasks that succeeded do not run again. The answer lists the cleared task instances
        (``task_instances``, ``total_entries``).
        """
        ids: list[Any] = [list(item) if isinstance(item, (tuple, list)) else str(item) for item in task_ids]
        body = {
            "dry_run": False,
            "dag_run_id": run_id,
            "task_ids": ids,
            "only_failed": bool(only_failed),
            "include_downstream": bool(include_downstream),
            "reset_dag_runs": True,
        }
        data = self._json("POST", f"{API_PREFIX}/dags/{_segment(dag_id)}/clearTaskInstances", json=body)
        return data if isinstance(data, dict) else {"raw": data}

    def trigger_dag_run(self, dag_id: str, conf: Mapping[str, Any] | None = None) -> dict[str, Any]:
        """Trigger a manual DAG run now (``logical_date: null``)."""
        body = {"logical_date": None, "conf": dict(conf or {})}
        data = self._json("POST", f"{API_PREFIX}/dags/{_segment(dag_id)}/dagRuns", json=body)
        return data if isinstance(data, dict) else {"raw": data}

    def get_dag_run(self, dag_id: str, run_id: str) -> dict[str, Any]:
        """``GET /api/v2/dags/{dag_id}/dagRuns/{run_id}``."""
        data = self._json("GET", f"{API_PREFIX}/dags/{_segment(dag_id)}/dagRuns/{_segment(run_id)}")
        return data if isinstance(data, dict) else {}


__all__ = [
    "CAUSE_SEPARATOR",
    "CONTEXT_SEPARATOR",
    "TRACEBACK_HEADER",
    "AirflowAPIError",
    "AirflowClient",
    "TaskInstanceInfo",
    "format_timestamp",
    "render_exception_chain",
    "render_log_content",
    "render_log_message",
]
