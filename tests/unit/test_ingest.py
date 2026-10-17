"""Tests for dag_healer.ingest and the on_failure_callback (no Airflow needed)."""

from __future__ import annotations

import enum
import json
import socket
import threading
from collections.abc import Iterator
from datetime import datetime
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from types import SimpleNamespace
from typing import Any

import pytest

from dag_healer import ingest
from dag_healer.airflow.callback import on_failure_callback
from dag_healer.ingest import (
    INGEST_PATH,
    MAX_ERROR_CHARS,
    PAYLOAD_KEYS,
    TRUNCATION_MARKER,
    api_base_from_execution_url,
    build_payload,
    format_error,
    report_failure,
    resolve_api_url,
)


class FakeState(str, enum.Enum):
    FAILED = "failed"
    UP_FOR_RETRY = "up_for_retry"


def runtime_ti(**overrides: Any) -> SimpleNamespace:
    """A stand-in for the Task SDK RuntimeTaskInstance."""
    fields = {
        "id": "0192-uuid",
        "dag_id": "sales_daily_etl",
        "task_id": "load_sales",
        "run_id": "manual__2026-10-17T10:00:00+00:00",
        "map_index": -1,
        "try_number": 2,
        "max_tries": 3,
        "state": FakeState.FAILED,
        "hostname": "worker-1",
    }
    fields.update(overrides)
    return SimpleNamespace(**fields)


def raised(exc: BaseException) -> BaseException:
    try:
        raise exc
    except BaseException as caught:  # noqa: BLE001 - we want the traceback attached
        return caught


# --------------------------------------------------------------------------- build_payload


def test_build_payload_from_runtime_task_instance() -> None:
    error = raised(ValueError("amount column is missing"))
    payload = build_payload(runtime_ti(), error=error, source="listener")

    assert tuple(payload) == PAYLOAD_KEYS
    assert payload["dag_id"] == "sales_daily_etl"
    assert payload["task_id"] == "load_sales"
    assert payload["run_id"] == "manual__2026-10-17T10:00:00+00:00"
    assert payload["map_index"] == -1
    assert payload["try_number"] == 2
    assert payload["max_tries"] == 3
    assert payload["state"] == "failed"
    assert payload["hostname"] == "worker-1"
    assert payload["source"] == "listener"
    assert "Traceback (most recent call last)" in payload["error"]
    assert payload["error"].endswith("ValueError: amount column is missing")
    reported = datetime.fromisoformat(payload["reported_at"])
    assert reported.tzinfo is not None
    json.dumps(payload)  # JSON-safe


def test_build_payload_from_context_dict() -> None:
    ti = runtime_ti(map_index=None, hostname=None, state="up_for_retry")
    context = {"ti": ti, "exception": raised(KeyError("qty")), "run_id": "ignored"}
    payload = build_payload(context=context, source="callback")

    assert payload["dag_id"] == "sales_daily_etl"
    assert payload["run_id"] == "manual__2026-10-17T10:00:00+00:00"  # the TI wins over the context
    assert payload["map_index"] == -1
    assert payload["state"] == "up_for_retry"
    assert payload["hostname"]  # falls back to the local host name
    assert payload["error"].endswith("KeyError: 'qty'")
    assert payload["source"] == "callback"


def test_build_payload_falls_back_to_context_values() -> None:
    ti = SimpleNamespace(task_id="transform")  # e.g. a partially loaded ORM object
    context = {
        "task_instance": ti,
        "dag_run": SimpleNamespace(dag_id="customers_sync", run_id="scheduled__1"),
        "try_number": "1",
    }
    payload = build_payload(context=context, error="boom", source="callback")

    assert payload["dag_id"] == "customers_sync"
    assert payload["run_id"] == "scheduled__1"
    assert payload["task_id"] == "transform"
    assert payload["try_number"] == 1
    assert payload["max_tries"] is None
    assert payload["state"] is None
    assert payload["error"] == "boom"


def test_build_payload_without_anything_does_not_raise() -> None:
    payload = build_payload(source="cli")
    assert payload["dag_id"] is None
    assert payload["map_index"] == -1
    assert payload["error"] == ""


def test_build_payload_survives_raising_attributes() -> None:
    class Exploding:
        dag_id = "d"
        task_id = "t"

        @property
        def run_id(self) -> str:
            raise RuntimeError("detached instance")

    payload = build_payload(Exploding(), source="listener")
    assert payload["dag_id"] == "d"
    assert payload["run_id"] is None


def test_format_error_truncates_keeping_the_tail() -> None:
    text = "x" * 10_000 + "ValueError: the end"
    formatted = format_error(text)
    assert len(formatted) == MAX_ERROR_CHARS
    assert formatted.startswith(TRUNCATION_MARKER)
    assert formatted.endswith("ValueError: the end")


def test_format_error_variants() -> None:
    assert format_error(None) == ""
    assert format_error(RuntimeError("no traceback")) == "RuntimeError: no traceback"

    class BadStr(Exception):
        def __str__(self) -> str:
            raise TypeError("nope")

    assert "BadStr" in format_error(BadStr())


# --------------------------------------------------------------------------- URL resolution


@pytest.mark.parametrize(
    ("url", "expected"),
    [
        ("http://airflow-apiserver:8080/execution/", "http://airflow-apiserver:8080"),
        ("http://airflow-apiserver:8080/execution", "http://airflow-apiserver:8080"),
        ("https://host/airflow/execution/", "https://host/airflow"),
        ("http://host:8080/", "http://host:8080"),
    ],
)
def test_api_base_from_execution_url(url: str, expected: str) -> None:
    assert api_base_from_execution_url(url) == expected


def test_resolve_api_url_order(healer_env: Any, monkeypatch: pytest.MonkeyPatch) -> None:
    assert resolve_api_url() is None
    monkeypatch.setenv("AIRFLOW__API__BASE_URL", "http://base:8080/")
    assert resolve_api_url() == "http://base:8080"
    monkeypatch.setenv("AIRFLOW__CORE__EXECUTION_API_SERVER_URL", "http://exec:8080/execution/")
    assert resolve_api_url() == "http://exec:8080"
    monkeypatch.setenv("HEALER_API_URL", "http://healer:9000/")
    assert resolve_api_url() == "http://healer:9000"
    assert resolve_api_url("http://explicit") == "http://explicit"


# --------------------------------------------------------------------------- report_failure


class _Recorder:
    def __init__(self) -> None:
        self.requests: list[dict[str, Any]] = []
        self.status = 202


@pytest.fixture
def ingest_server(healer_env: Any, monkeypatch: pytest.MonkeyPatch) -> Iterator[tuple[str, _Recorder]]:
    """A local HTTP server recording POSTs; yields (base_url, recorder)."""
    # Never route test traffic through a system/environment proxy.
    monkeypatch.setenv("NO_PROXY", "*")
    monkeypatch.setenv("no_proxy", "*")
    recorder = _Recorder()

    class Handler(BaseHTTPRequestHandler):
        def do_POST(self) -> None:  # noqa: N802 - http.server API
            length = int(self.headers.get("Content-Length", "0"))
            recorder.requests.append(
                {"path": self.path, "headers": dict(self.headers), "body": self.rfile.read(length)}
            )
            self.send_response(recorder.status)
            self.send_header("Content-Type", "application/json")
            self.end_headers()
            self.wfile.write(b'{"status": "accepted"}')

        def log_message(self, format: str, *args: Any) -> None:
            pass

    server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    try:
        yield f"http://127.0.0.1:{server.server_address[1]}", recorder
    finally:
        server.shutdown()
        server.server_close()
        thread.join(timeout=5)


def test_report_failure_posts_payload(ingest_server: tuple[str, _Recorder]) -> None:
    base_url, recorder = ingest_server
    payload = build_payload(runtime_ti(), error="boom", source="listener")

    assert report_failure(payload, api_url=base_url + "/", token="s3cret") is True

    (request,) = recorder.requests
    headers = {k.lower(): v for k, v in request["headers"].items()}
    assert request["path"] == INGEST_PATH
    assert headers["content-type"] == "application/json"
    assert headers["x-healer-token"] == "s3cret"
    assert headers["user-agent"].startswith("dag-healer/")
    assert json.loads(request["body"]) == payload


def test_report_failure_reads_env(
    ingest_server: tuple[str, _Recorder], monkeypatch: pytest.MonkeyPatch
) -> None:
    base_url, recorder = ingest_server
    monkeypatch.setenv("AIRFLOW__CORE__EXECUTION_API_SERVER_URL", base_url + "/execution/")
    monkeypatch.setenv("HEALER_INGEST_TOKEN", "from-env")

    assert report_failure({"dag_id": "d", "when": datetime(2026, 1, 1)}) is True
    (request,) = recorder.requests
    assert {k.lower(): v for k, v in request["headers"].items()}["x-healer-token"] == "from-env"
    assert json.loads(request["body"])["when"] == "2026-01-01 00:00:00"  # default=str


def test_report_failure_rejected(ingest_server: tuple[str, _Recorder]) -> None:
    base_url, recorder = ingest_server
    recorder.status = 401
    assert report_failure({"dag_id": "d"}, api_url=base_url, token="wrong") is False


def test_report_failure_without_token_or_url(healer_env: Any, ingest_server: tuple[str, _Recorder]) -> None:
    base_url, recorder = ingest_server
    assert report_failure({"dag_id": "d"}, api_url=base_url) is False
    assert report_failure({"dag_id": "d"}, token="t") is False
    assert recorder.requests == []


def _closed_port() -> int:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
        sock.bind(("127.0.0.1", 0))
        return sock.getsockname()[1]


def test_report_failure_never_raises(healer_env: Any, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("NO_PROXY", "*")
    monkeypatch.setenv("no_proxy", "*")
    refused = f"http://127.0.0.1:{_closed_port()}"
    assert report_failure({"dag_id": "d"}, api_url=refused, token="t", timeout=1.0) is False
    assert report_failure({"dag_id": "d"}, api_url="not a url", token="t") is False
    assert report_failure(None, api_url=refused, token="t") is False  # type: ignore[arg-type]


# --------------------------------------------------------------------------- callback


def test_on_failure_callback_reports(
    ingest_server: tuple[str, _Recorder], monkeypatch: pytest.MonkeyPatch
) -> None:
    base_url, recorder = ingest_server
    monkeypatch.setenv("HEALER_API_URL", base_url)
    monkeypatch.setenv("HEALER_INGEST_TOKEN", "tok")
    context = {"task_instance": runtime_ti(), "exception": raised(ZeroDivisionError("division by zero"))}

    assert on_failure_callback(context) is None

    (request,) = recorder.requests
    body = json.loads(request["body"])
    assert body["source"] == "callback"
    assert body["task_id"] == "load_sales"
    assert body["error"].endswith("ZeroDivisionError: division by zero")


def test_on_failure_callback_never_raises(healer_env: Any, monkeypatch: pytest.MonkeyPatch) -> None:
    def explode(*args: Any, **kwargs: Any) -> dict[str, Any]:
        raise RuntimeError("payload bug")

    on_failure_callback({})
    on_failure_callback(None)
    on_failure_callback("not a mapping")  # type: ignore[arg-type]
    monkeypatch.setattr("dag_healer.airflow.callback.build_payload", explode)
    on_failure_callback({"ti": runtime_ti()})


def test_lazy_callback_attribute() -> None:
    import dag_healer

    assert dag_healer.callback is on_failure_callback
    with pytest.raises(AttributeError):
        dag_healer.does_not_exist  # noqa: B018


def test_ingest_module_is_stdlib_only() -> None:
    source = ingest.__file__
    assert source is not None
    with open(source, encoding="utf-8") as handle:
        text = handle.read()
    for heavy in ("import pandas", "import sqlalchemy", "import fastapi", "import httpx", "import airflow"):
        assert heavy not in text
