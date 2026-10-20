"""Unit tests for :mod:`dag_healer.airflow.client` (httpx.MockTransport, no network, no Airflow).

The JSON fixtures under ``tests/unit/fixtures/logs`` are sanitized responses recorded from a
running Airflow 3.3.2 api-server (``customers_sync`` failing on the day-2 CRM export).
"""

from __future__ import annotations

import copy
import json
import logging
from collections.abc import Callable
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any
from urllib.parse import parse_qs

import httpx
import pytest

from dag_healer.airflow.client import (
    CAUSE_SEPARATOR,
    CONTEXT_SEPARATOR,
    TRACEBACK_HEADER,
    AirflowAPIError,
    AirflowClient,
    TaskInstanceInfo,
    format_timestamp,
    render_exception_chain,
    render_log_content,
    render_log_message,
)
from dag_healer.config import config_from_mapping

FIXTURES = Path(__file__).parent / "fixtures" / "logs"
API_FIXTURES = FIXTURES / "airflow_api"
BASE = "http://airflow.test:8080"
RUN_ID = "manual__2026-10-17T18:48:46.093857+00:00"
QUOTED_RUN_ID = "manual__2026-10-17T18%3A48%3A46.093857%2B00%3A00"


def load(name: str) -> Any:
    """Load a JSON fixture (``airflow_api/x.json`` or ``x.json``)."""
    return json.loads((FIXTURES / name).read_text(encoding="utf-8"))


class FakeAirflow:
    """A tiny Airflow api-server: issues tokens, checks them and serves routes."""

    def __init__(self) -> None:
        self.requests: list[httpx.Request] = []
        self.logins = 0
        self.valid_tokens: set[str] = set()
        self.routes: dict[tuple[str, str], Callable[[httpx.Request], httpx.Response]] = {}
        self.password = "admin"

    def route(
        self, method: str, raw_path: str, handler: Callable[[httpx.Request], httpx.Response] | Any
    ) -> None:
        """Register ``handler`` (a callable or a JSON body) for ``method`` + raw (encoded) path."""
        if callable(handler):
            self.routes[(method, raw_path)] = handler
        else:
            self.routes[(method, raw_path)] = lambda request, body=handler: httpx.Response(200, json=body)

    def __call__(self, request: httpx.Request) -> httpx.Response:
        self.requests.append(request)
        path = request.url.raw_path.decode().split("?", 1)[0]
        if request.method == "POST" and path == "/auth/token":
            body = json.loads(request.content)
            if body != {"username": "admin", "password": self.password}:
                return httpx.Response(401, json={"detail": "Invalid credentials"})
            self.logins += 1
            token = f"token-{self.logins}"
            self.valid_tokens.add(token)
            return httpx.Response(201, json={"access_token": token})
        handler = self.routes.get((request.method, path))
        if handler is None:
            return httpx.Response(404, json={"detail": "Not Found"})
        if path != "/api/v2/monitor/health":
            auth = request.headers.get("Authorization", "")
            if not auth:
                return httpx.Response(401, json={"detail": "Not authenticated"})
            if auth.removeprefix("Bearer ") not in self.valid_tokens:
                return httpx.Response(401, json={"detail": "Token expired"})
        return handler(request)

    @property
    def api_requests(self) -> list[httpx.Request]:
        return [r for r in self.requests if r.url.path != "/auth/token"]


@pytest.fixture
def server() -> FakeAirflow:
    return FakeAirflow()


@pytest.fixture
def client(server: FakeAirflow) -> AirflowClient:
    with AirflowClient(BASE, username="admin", password="admin", transport=httpx.MockTransport(server)) as c:
        yield c


def query(request: httpx.Request) -> dict[str, list[str]]:
    return parse_qs(request.url.query.decode(), keep_blank_values=True)


DAG_PATH = "/api/v2/dags/customers_sync"
TI_PATH = f"{DAG_PATH}/dagRuns/{QUOTED_RUN_ID}/taskInstances/transform"
LIST_PATH = "/api/v2/dags/~/dagRuns/~/taskInstances"


# =========================================================================== auth


def test_token_is_requested_once_and_cached(server: FakeAirflow, client: AirflowClient) -> None:
    server.route("GET", DAG_PATH, load("airflow_api/dag_customers_sync.json"))
    assert client.get_dag("customers_sync")["fileloc"] == "/opt/airflow/repo/dags/customers_sync.py"
    client.get_dag("customers_sync")
    assert server.logins == 1
    assert [r.headers["Authorization"] for r in server.api_requests] == ["Bearer token-1"] * 2
    login = server.requests[0]
    assert login.url.path == "/auth/token" and json.loads(login.content)["username"] == "admin"


def test_relogin_once_on_401(server: FakeAirflow, client: AirflowClient) -> None:
    server.route("GET", DAG_PATH, load("airflow_api/dag_customers_sync.json"))
    client.get_dag("customers_sync")
    server.valid_tokens.clear()  # the cached JWT expired
    assert client.get_dag("customers_sync")["dag_id"] == "customers_sync"
    assert server.logins == 2
    assert server.api_requests[-1].headers["Authorization"] == "Bearer token-2"


def test_persistent_401_raises_after_a_single_relogin(server: FakeAirflow) -> None:
    server.route("GET", DAG_PATH, lambda request: httpx.Response(401, json={"detail": "Not authenticated"}))
    client = AirflowClient(BASE, username="admin", password="admin", transport=httpx.MockTransport(server))
    with pytest.raises(AirflowAPIError) as info:
        client.get_dag("customers_sync")
    assert info.value.status_code == 401
    assert info.value.detail == "Not authenticated"
    assert server.logins == 2
    assert len(server.api_requests) == 2


def test_static_token_is_used_without_login(server: FakeAirflow) -> None:
    server.valid_tokens.add("static")
    server.route("GET", DAG_PATH, {"dag_id": "customers_sync"})
    client = AirflowClient(BASE, token="static", transport=httpx.MockTransport(server))
    assert client.get_dag("customers_sync") == {"dag_id": "customers_sync"}
    assert server.logins == 0
    server.valid_tokens.clear()
    with pytest.raises(AirflowAPIError) as info:  # no credentials to log in again
        client.get_dag("customers_sync")
    assert info.value.status_code == 401
    assert server.logins == 0


def test_expired_static_token_falls_back_to_login(server: FakeAirflow) -> None:
    server.route("GET", DAG_PATH, {"dag_id": "customers_sync"})
    client = AirflowClient(
        BASE, token="expired", username="admin", password="admin", transport=httpx.MockTransport(server)
    )
    assert client.get_dag("customers_sync")["dag_id"] == "customers_sync"
    assert server.logins == 1


def test_wrong_password_raises_with_the_server_detail(server: FakeAirflow) -> None:
    server.route("GET", DAG_PATH, {"dag_id": "customers_sync"})
    client = AirflowClient(BASE, username="admin", password="wrong", transport=httpx.MockTransport(server))
    with pytest.raises(AirflowAPIError) as info:
        client.get_dag("customers_sync")
    assert info.value.status_code == 401
    assert "Invalid credentials" in info.value.detail
    assert "wrong" not in str(info.value)


def test_health_is_sent_without_credentials(server: FakeAirflow, client: AirflowClient) -> None:
    server.route("GET", "/api/v2/monitor/health", load("airflow_api/health.json"))
    health = client.health()
    assert health["metadatabase"]["status"] == "healthy"
    assert server.logins == 0
    assert "Authorization" not in server.requests[0].headers


def test_from_config_reads_the_airflow_section() -> None:
    cfg = config_from_mapping(
        {"airflow": {"api_url": "http://airflow-apiserver:8080/", "username": "svc", "password": "pw"}}
    )
    client = AirflowClient.from_config(cfg, timeout=3.0)
    assert client.base_url == "http://airflow-apiserver:8080"
    assert client.username == "svc" and client.can_login
    assert "pw" not in repr(client)


def test_base_url_with_subpath_and_api_suffix(server: FakeAirflow) -> None:
    seen: list[str] = []

    def handler(request: httpx.Request) -> httpx.Response:
        seen.append(request.url.path)
        return httpx.Response(200, json={"metadatabase": {"status": "healthy"}})

    client = AirflowClient("http://host/airflow/api/v2/", transport=httpx.MockTransport(handler))
    assert client.base_url == "http://host/airflow"
    client.health()
    assert seen == ["/airflow/api/v2/monitor/health"]
    with pytest.raises(ValueError):
        AirflowClient("  ")


# =========================================================================== task instances


def _failed_only(fixture: dict[str, Any]) -> Callable[[httpx.Request], httpx.Response]:
    """Serve the recorded task instances, honouring state/limit/offset like Airflow does."""

    def handler(request: httpx.Request) -> httpx.Response:
        params = query(request)
        items = [ti for ti in fixture["task_instances"] if ti["state"] in params.get("state", [ti["state"]])]
        offset, limit = int(params["offset"][0]), int(params["limit"][0])
        page = items[offset : offset + limit]
        return httpx.Response(200, json={"task_instances": page, "total_entries": len(items)})

    return handler


def test_list_failed_task_instances_on_the_recorded_response(
    server: FakeAirflow, client: AirflowClient
) -> None:
    server.route("GET", LIST_PATH, _failed_only(load("airflow_api/task_instances.json")))
    since = datetime(2026, 10, 17, 15, 40, tzinfo=timezone(timedelta(hours=-3)))
    tis = client.list_failed_task_instances(updated_since=since)
    assert len(tis) == 1
    ti = tis[0]
    assert (ti.dag_id, ti.run_id, ti.task_id, ti.map_index) == ("customers_sync", RUN_ID, "transform", -1)
    assert (ti.try_number, ti.max_tries, ti.state, ti.operator) == (1, 0, "failed", "PythonOperator")
    assert ti.start_date == "2026-10-17T18:48:52.471386Z" and ti.hostname
    assert ti.is_final_try
    assert ti.raw["dag_version"]["version_number"] == 1
    params = query(server.api_requests[0])
    assert params["state"] == ["failed"]
    assert params["updated_at_gte"] == ["2026-10-17T18:40:00Z"]
    assert params["order_by"] == ["id"]
    assert params["limit"] == ["100"] and params["offset"] == ["0"]


def test_list_failed_task_instances_paginates(server: FakeAirflow, client: AirflowClient) -> None:
    template = load("airflow_api/task_instance_failed.json")
    items = []
    for number in range(5):
        item = copy.deepcopy(template)
        item["task_id"] = f"task_{number}"
        item["dag_id"] = "customers_sync" if number % 2 == 0 else "sales_daily_etl"
        items.append(item)
    server.route("GET", LIST_PATH, _failed_only({"task_instances": items}))
    tis = client.list_failed_task_instances(page_size=2)
    assert [ti.task_id for ti in tis] == [f"task_{n}" for n in range(5)]
    assert [query(r)["offset"][0] for r in server.api_requests] == ["0", "2", "4"]
    assert all("updated_at_gte" not in query(r) for r in server.api_requests)

    only = client.list_failed_task_instances(page_size=2, dag_ids=["customers_sync"])
    assert [ti.task_id for ti in only] == ["task_0", "task_2", "task_4"]


def test_list_failed_task_instances_stops_at_max_pages(
    server: FakeAirflow, client: AirflowClient, caplog: pytest.LogCaptureFixture
) -> None:
    template = load("airflow_api/task_instance_failed.json")
    server.route("GET", LIST_PATH, _failed_only({"task_instances": [template] * 10}))
    with caplog.at_level(logging.WARNING, logger="dag_healer.airflow.client"):
        tis = client.list_failed_task_instances(page_size=3, max_pages=2)
    assert len(tis) == 6
    assert len(server.api_requests) == 2
    assert "Stopped listing failed task instances" in caplog.text


def test_list_failed_task_instances_without_order_by_support(
    server: FakeAirflow, client: AirflowClient
) -> None:
    template = load("airflow_api/task_instance_failed.json")
    inner = _failed_only({"task_instances": [template]})

    def handler(request: httpx.Request) -> httpx.Response:
        if "order_by" in query(request):
            return httpx.Response(400, json={"detail": "Ordering with 'id' is disallowed"})
        return inner(request)

    server.route("GET", LIST_PATH, handler)
    assert len(client.list_failed_task_instances()) == 1
    assert "order_by" not in query(server.api_requests[-1])


def test_run_ids_are_url_quoted(server: FakeAirflow, client: AirflowClient) -> None:
    server.route("GET", TI_PATH, load("airflow_api/task_instance_failed.json"))
    server.route(
        "GET", f"{DAG_PATH}/dagRuns/{QUOTED_RUN_ID}", load("airflow_api/dag_run_customers_sync.json")
    )
    ti = client.get_task_instance("customers_sync", RUN_ID, "transform")
    assert isinstance(ti, TaskInstanceInfo) and ti.state == "failed"
    run = client.get_dag_run("customers_sync", RUN_ID)
    assert run["state"] == "failed"
    raw_paths = [r.url.raw_path.decode() for r in server.api_requests]
    assert raw_paths == [TI_PATH, f"{DAG_PATH}/dagRuns/{QUOTED_RUN_ID}"]
    assert all("+" not in path and ":" not in path for path in raw_paths)


def test_odd_identifiers_are_quoted_segment_by_segment(server: FakeAirflow, client: AirflowClient) -> None:
    raw = (
        "/api/v2/dags/my%20dag/dagRuns/scheduled__2026-01-01T00%3A00%3A00%2B00%3A00/taskInstances/group.t%2F1"
    )
    server.route(
        "GET", raw, {"dag_id": "my dag", "dag_run_id": "x", "task_id": "group.t/1", "state": "failed"}
    )
    ti = client.get_task_instance("my dag", "scheduled__2026-01-01T00:00:00+00:00", "group.t/1")
    assert ti.task_id == "group.t/1"


def test_mapped_task_instance(server: FakeAirflow, client: AirflowClient) -> None:
    mapped = load("airflow_api/task_instance_failed.json") | {"map_index": 3, "rendered_map_index": "eu"}
    server.route("GET", f"{TI_PATH}/3", mapped)
    server.route("GET", TI_PATH, load("airflow_api/task_instance_failed.json"))
    assert client.get_task_instance("customers_sync", RUN_ID, "transform", map_index=3).map_index == 3
    assert client.get_task_instance("customers_sync", RUN_ID, "transform", map_index=-1).map_index == -1
    assert [r.url.raw_path.decode() for r in server.api_requests] == [f"{TI_PATH}/3", TI_PATH]


def test_task_instance_not_found_raises(server: FakeAirflow, client: AirflowClient) -> None:
    server.route(
        "GET",
        f"{TI_PATH}/5",
        lambda request: httpx.Response(404, json={"detail": "The Mapped Task Instance ... was not found"}),
    )
    with pytest.raises(AirflowAPIError) as info:
        client.get_task_instance("customers_sync", RUN_ID, "transform", map_index=5)
    assert info.value.status_code == 404
    assert "was not found" in info.value.detail
    assert str(info.value).startswith("Airflow API error 404:")


# =========================================================================== logs


def test_get_task_log_renders_the_recorded_structured_log(server: FakeAirflow, client: AirflowClient) -> None:
    server.route("GET", f"{TI_PATH}/logs/1", load("customers_sync_transform_failed.json"))
    text = client.get_task_log("customers_sync", RUN_ID, "transform", 1)
    request = server.api_requests[0]
    assert request.headers["Accept"] == "application/json"
    assert query(request) == {"full_content": ["true"], "map_index": ["-1"]}
    lines = text.splitlines()
    assert "[2026-10-17T18:48:53.084907Z] ERROR - Task failed with exception" in lines
    header = lines.index(TRACEBACK_HEADER)
    assert lines[header - 1].endswith("Task failed with exception")
    assert '  File "/opt/airflow/repo/src/etl/customers.py", line 19, in parse_signup_date' in lines
    assert '  File "/opt/airflow/repo/src/etl/customers.py", line 36, in transform_customers' in lines
    assert "ValueError: unconverted data remains: T08:30:00Z" in lines
    assert lines.index("ValueError: unconverted data remains: T08:30:00Z") > header
    assert "::endgroup::" not in text and "::group::" not in text
    assert "[2026-10-17T18:26:54.364801Z]" not in text  # only this try's log
    assert "Log message source details" in lines
    assert (
        lines[-1]
        == "[2026-10-17T18:48:53.135488Z] WARNING - dag-healer: failure report rejected with HTTP 404"
    )


def test_get_task_log_of_a_mapped_try(server: FakeAirflow, client: AirflowClient) -> None:
    server.route("GET", f"{TI_PATH}/logs/2", {"content": [{"event": "hello"}], "continuation_token": None})
    assert client.get_task_log("customers_sync", RUN_ID, "transform", 2, map_index=4) == "hello"
    assert query(server.api_requests[0])["map_index"] == ["4"]


def test_successful_log_has_no_traceback(server: FakeAirflow, client: AirflowClient) -> None:
    path = (
        "/api/v2/dags/sales_daily_etl/dagRuns/manual__2026-10-17T18%3A26%3A53.101536%2B00%3A00"
        "/taskInstances/extract_sales/logs/1"
    )
    server.route("GET", path, load("sales_daily_etl_extract_success.json"))
    text = client.get_task_log(
        "sales_daily_etl", "manual__2026-10-17T18:26:53.101536+00:00", "extract_sales", 1
    )
    assert TRACEBACK_HEADER not in text
    assert "[2026-10-17T18:27:00.342710Z] INFO - extracted 30 rows from data/incoming/sales.csv" in text


def test_plain_string_logs(server: FakeAirflow, client: AirflowClient) -> None:
    plain = load("inventory_report_build_report_plain.json")
    server.route("GET", f"{TI_PATH}/logs/1", plain)
    server.route(
        "GET",
        f"{TI_PATH}/logs/2",
        lambda request: httpx.Response(
            200, text="line one\nline two\n", headers={"content-type": "text/plain"}
        ),
    )
    server.route("GET", f"{TI_PATH}/logs/3", {"content": "already a string", "continuation_token": None})
    text = client.get_task_log("customers_sync", RUN_ID, "transform", 1)
    assert text == plain["content"][0].rstrip("\n")
    assert "KeyError: 'qty'" in text.splitlines()
    assert client.get_task_log("customers_sync", RUN_ID, "transform", 2) == "line one\nline two\n"
    assert client.get_task_log("customers_sync", RUN_ID, "transform", 3) == "already a string"


def test_render_chained_exception_like_python() -> None:
    text = render_log_content(load("sales_daily_etl_transform_chained_keyerror.json"))
    lines = text.splitlines()
    assert lines.count(TRACEBACK_HEADER) == 2
    separator = lines.index(CAUSE_SEPARATOR)
    first, second = (i for i, line in enumerate(lines) if line == TRACEBACK_HEADER)
    assert first < separator < second
    assert lines[separator - 2] == "KeyError: 'amount'"  # root cause printed first
    assert lines[separator - 1] == "" and lines[separator + 1] == ""
    assert lines[second + 7] == '  File "/opt/airflow/repo/src/etl/sales.py", line 25, in transform_sales'
    assert lines[second + 10] == "KeyError: 'amount'"
    assert lines[second + 11].endswith("INFO - Post Execute")


def test_render_context_chain_syntax_error_notes_and_groups() -> None:
    outer = {
        "exc_type": "RuntimeError",
        "exc_value": "could not load plugin",
        "is_cause": False,
        "exc_notes": ["while loading dags/x.py"],
        "frames": [
            {"filename": "/opt/airflow/repo/src/loader.py", "lineno": 10, "name": "load", "line": "  run() "}
        ],
    }
    inner = {
        "exc_type": "SyntaxError",
        "exc_value": "",
        "is_cause": False,
        "frames": [],
        "syntax_error": {
            "filename": "/opt/airflow/repo/dags/x.py",
            "lineno": 3,
            "line": "    x = (\n",
            "offset": 9,
            "msg": "'(' was never closed",
        },
    }
    lines = render_exception_chain([outer, inner])
    assert lines[:4] == [
        '  File "/opt/airflow/repo/dags/x.py", line 3',
        "    x = (",
        "        ^",
        "SyntaxError: '(' was never closed",
    ]
    assert CONTEXT_SEPARATOR in lines
    assert lines[-4:] == [
        '  File "/opt/airflow/repo/src/loader.py", line 10, in load',
        "    run()",
        "RuntimeError: could not load plugin",
        "while loading dags/x.py",
    ]

    group = {
        "exc_type": "ExceptionGroup",
        "exc_value": "2 failures (2 sub-exceptions)",
        "is_group": True,
        "frames": [],
        "exceptions": [
            [{"exc_type": "ValueError", "exc_value": "a", "frames": []}],
            {"stacks": [{"exc_type": "KeyError", "exc_value": "'b'", "frames": []}]},
        ],
    }
    rendered = render_exception_chain([group])
    assert rendered[0] == "ExceptionGroup: 2 failures (2 sub-exceptions)"
    assert "    | ValueError: a" in rendered and "    | KeyError: 'b'" in rendered


def test_render_log_message_variants() -> None:
    assert render_log_message({"event": "::endgroup::"}) == []
    assert render_log_message({"event": "::group::Pre Execute", "timestamp": "t", "level": "info"}) == [
        "[t] INFO - Pre Execute"
    ]
    assert render_log_message({"event": "multi\nline", "level": "warning"}) == ["WARNING - multi", "line"]
    assert render_log_message("plain\ntext") == ["plain", "text"]
    assert render_log_message(None) == []
    preformatted = {
        "event": "boom",
        "level": "error",
        "exception": "Traceback (most recent call last):\n  File \"x.py\", line 1, in f\nKeyError: 'k'\n",
    }
    assert render_log_message(preformatted)[-1] == "KeyError: 'k'"
    assert render_log_message({"event": "x", "exc_info": True}) == ["x"]
    assert render_log_content(None) == ""
    assert render_log_content({"event": "single"}) == "single"


def test_format_timestamp() -> None:
    assert format_timestamp(datetime(2026, 10, 17, 18, 40)) == "2026-10-17T18:40:00Z"
    aware = datetime(2026, 10, 17, 15, 40, 0, 123000, tzinfo=timezone(timedelta(hours=-3)))
    assert format_timestamp(aware) == "2026-10-17T18:40:00.123000Z"


# =========================================================================== other endpoints and errors


def test_dag_and_dag_source(server: FakeAirflow, client: AirflowClient) -> None:
    server.route("GET", DAG_PATH, load("airflow_api/dag_customers_sync.json"))
    server.route(
        "GET", "/api/v2/dagSources/customers_sync", load("airflow_api/dag_source_customers_sync.json")
    )
    dag = client.get_dag("customers_sync")
    assert dag["relative_fileloc"] == "customers_sync.py"
    source = client.get_dag_source("customers_sync")
    assert source is not None and 'dag_id="customers_sync"' in source
    assert server.api_requests[-1].headers["Accept"] == "application/json"
    assert client.get_dag_source("missing_dag") is None  # 404


def test_dag_source_as_plain_text(server: FakeAirflow, client: AirflowClient) -> None:
    server.route(
        "GET",
        "/api/v2/dagSources/customers_sync",
        lambda request: httpx.Response(200, text="print('dag')", headers={"content-type": "text/plain"}),
    )
    assert client.get_dag_source("customers_sync") == "print('dag')"


def test_clear_task_instances_and_trigger(server: FakeAirflow, client: AirflowClient) -> None:
    server.route("POST", f"{DAG_PATH}/clearTaskInstances", load("airflow_api/clear_task_instances.json"))
    server.route("POST", f"{DAG_PATH}/dagRuns", load("airflow_api/trigger_dag_run.json"))
    cleared = client.clear_task_instances("customers_sync", RUN_ID, ["transform", ("load", 2)])
    assert cleared["task_instances"][0]["task_id"] == "transform"
    assert json.loads(server.api_requests[0].content) == {
        "dry_run": False,
        "dag_run_id": RUN_ID,
        "task_ids": ["transform", ["load", 2]],
        "only_failed": True,
        "reset_dag_runs": True,
    }
    client.clear_task_instances("customers_sync", RUN_ID, ["transform"], only_failed=False)
    assert json.loads(server.api_requests[1].content)["only_failed"] is False

    run = client.trigger_dag_run("customers_sync")
    assert run["dag_run_id"].startswith("manual__")
    assert json.loads(server.api_requests[2].content) == {"logical_date": None, "conf": {}}
    client.trigger_dag_run("customers_sync", conf={"reason": "dag-healer retry"})
    assert json.loads(server.api_requests[3].content)["conf"] == {"reason": "dag-healer retry"}


def test_validation_errors_are_flattened(server: FakeAirflow, client: AirflowClient) -> None:
    detail = [
        {
            "type": "greater_than_equal",
            "loc": ["query", "limit"],
            "msg": "Input should be greater than or equal to 0",
            "input": "-1",
        }
    ]
    server.route("GET", DAG_PATH, lambda request: httpx.Response(422, json={"detail": detail}))
    with pytest.raises(AirflowAPIError) as info:
        client.get_dag("customers_sync")
    assert info.value.status_code == 422
    assert info.value.detail == "query.limit: Input should be greater than or equal to 0"


def test_non_json_errors_and_bodies(server: FakeAirflow, client: AirflowClient) -> None:
    server.route("GET", DAG_PATH, lambda request: httpx.Response(502, text="Bad gateway from proxy"))
    with pytest.raises(AirflowAPIError) as info:
        client.get_dag("customers_sync")
    assert (info.value.status_code, info.value.detail) == (502, "Bad gateway from proxy")

    server.route(
        "GET", f"{DAG_PATH}/dagRuns/{QUOTED_RUN_ID}", lambda request: httpx.Response(200, text="<html>")
    )
    with pytest.raises(AirflowAPIError, match="not JSON"):
        client.get_dag_run("customers_sync", RUN_ID)


def test_network_errors_have_no_status_code() -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        raise httpx.ConnectError("connection refused", request=request)

    client = AirflowClient(BASE, token="t", transport=httpx.MockTransport(handler))
    with pytest.raises(AirflowAPIError) as info:
        client.get_dag("customers_sync")
    assert info.value.status_code is None
    assert "ConnectError" in info.value.detail
    assert str(info.value).startswith("Airflow API request failed:")

    login_client = AirflowClient(BASE, username="a", password="b", transport=httpx.MockTransport(handler))
    with pytest.raises(AirflowAPIError) as login_info:
        login_client.get_dag("customers_sync")
    assert login_info.value.status_code is None


def test_login_response_without_token(server: FakeAirflow) -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(201, json={"unexpected": True})

    client = AirflowClient(BASE, username="admin", password="admin", transport=httpx.MockTransport(handler))
    with pytest.raises(AirflowAPIError, match="no access_token"):
        client.login()
    with pytest.raises(AirflowAPIError):
        AirflowClient(BASE, transport=httpx.MockTransport(handler)).login()


def test_task_instance_info_tolerates_partial_payloads() -> None:
    info = TaskInstanceInfo.from_api({"dag_id": "d", "run_id": "r", "task_id": "t", "try_number": "2"})
    assert (info.map_index, info.try_number, info.max_tries, info.state, info.operator) == (
        -1,
        2,
        0,
        None,
        None,
    )
    assert info.is_final_try
