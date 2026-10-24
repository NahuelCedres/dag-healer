"""Unit tests for scripts/demo.py and scripts/e2e_smoke.py (no Docker needed).

The Docker run itself is exercised by ``python scripts/tasks.py e2e`` (tests/e2e); here the
pure helpers are checked: healer.yaml settings, the demo repository refresh, the HTTP session,
the incident polling, the test-sandbox pre-flight and the e2e evidence parsers.
"""

from __future__ import annotations

import importlib.util
import json
import os
import subprocess
import sys
import threading
from collections.abc import Iterator
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from types import ModuleType
from typing import Any

import pytest
import yaml

from dag_healer.config import config_from_mapping, default_config_yaml

REPO_ROOT = Path(__file__).resolve().parents[2]
SCRIPTS = REPO_ROOT / "scripts"


def _load(name: str, filename: str) -> ModuleType:
    spec = importlib.util.spec_from_file_location(name, SCRIPTS / filename)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    sys.modules[name] = module
    spec.loader.exec_module(module)
    return module


@pytest.fixture(scope="module")
def demo() -> Iterator[ModuleType]:
    module = _load("demo", "demo.py")
    yield module


@pytest.fixture(scope="module")
def e2e(demo: ModuleType) -> Iterator[ModuleType]:
    yield _load("dag_healer_e2e_under_test", "e2e_smoke.py")


@pytest.fixture
def isolated_git(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    """Ignore the developer's global/system git config (signing, hooks, autocrlf)."""
    empty = tmp_path / "empty.gitconfig"
    empty.write_text("", encoding="utf-8")
    monkeypatch.setenv("GIT_CONFIG_GLOBAL", str(empty))
    monkeypatch.setenv("GIT_CONFIG_NOSYSTEM", "1")


def _git(repo: Path, *args: str) -> str:
    return subprocess.run(
        ["git", "-C", str(repo), *args], check=True, capture_output=True, text=True
    ).stdout.strip()


# --------------------------------------------------------------------------- healer.yaml


def test_configure_healer_yaml_applies_the_demo_settings(demo: ModuleType) -> None:
    text = demo.configure_healer_yaml(default_config_yaml())
    cfg = config_from_mapping(yaml.safe_load(text))
    assert cfg.monitor.poll_interval_seconds == 5
    assert list(cfg.monitor.modes) == ["rest", "listener"]
    email = cfg.notify.email
    assert email is not None
    assert (email.host, email.port, list(email.to), email.starttls) == (
        "mailpit",
        1025,
        ["data-team@example.com"],
        False,
    )
    assert (email.username, email.password) == (None, None)
    assert text.startswith("# Demo settings (scripts/demo.py)")
    # comments survive and applying it again changes nothing
    assert "# rest: poll failed task instances" in text
    assert demo.configure_healer_yaml(text) == text


def test_configure_healer_yaml_listener_only_switches_back_and_forth(demo: ModuleType) -> None:
    listener = demo.configure_healer_yaml(default_config_yaml(), listener_only=True)
    assert yaml.safe_load(listener)["monitor"]["modes"] == ["listener"]
    both = demo.configure_healer_yaml(listener)
    assert yaml.safe_load(both)["monitor"]["modes"] == ["rest", "listener"]


def test_configure_healer_yaml_refuses_unknown_files(demo: ModuleType) -> None:
    with pytest.raises(demo.DemoError, match="poll_interval_seconds"):
        demo.configure_healer_yaml("version: 1\n")


# --------------------------------------------------------------------------- demo repository


def _example(tmp_path: Path) -> Path:
    example = tmp_path / "example repo"
    (example / "dags").mkdir(parents=True)
    (example / "dags" / "sales.py").write_text("NEW = 1\n", encoding="utf-8")
    (example / "src").mkdir()
    (example / "src" / "etl.py").write_text("X = 1\n", encoding="utf-8")
    (example / "fixtures" / "fake_llm").mkdir(parents=True)
    (example / "fixtures" / "fake_llm" / "script.yaml").write_text("name: s\n", encoding="utf-8")
    (example / "fixtures" / "day2").mkdir()
    (example / "fixtures" / "day2" / "sales.csv").write_text("a\n2\n", encoding="utf-8")
    (example / "data" / "incoming").mkdir(parents=True)
    (example / "data" / "incoming" / "sales.csv").write_text("a\n1\n", encoding="utf-8")
    return example


def test_refresh_demo_repo_starts_over_without_removing_dags(
    demo: ModuleType, tmp_path: Path, isolated_git: None
) -> None:
    example = _example(tmp_path)
    repo = tmp_path / "demo dir" / "repo"
    demo.refresh_demo_repo(example, repo)

    # leftovers of an earlier demo run
    dags_id = os.stat(repo / "dags").st_ino
    (repo / "dags" / "sales.py").write_text("OLD = 1\n", encoding="utf-8")
    (repo / "dags" / "stale.py").write_text("x\n", encoding="utf-8")
    (repo / "dags" / "__pycache__").mkdir()
    (repo / "healer.yaml").write_text("version: 1\n", encoding="utf-8")
    (repo / "healer" / "contracts").mkdir(parents=True)
    (repo / "data" / "processed").mkdir(parents=True)
    (repo / "data" / "processed" / "out.csv").write_text("o\n", encoding="utf-8")
    (repo / "tests").mkdir()
    _git(repo, "branch", "healer/customers_sync-transform-abc")

    demo.refresh_demo_repo(example, repo)

    assert os.stat(repo / "dags").st_ino == dags_id  # the bind-mounted DAG folder stays
    assert (repo / "dags" / "sales.py").read_text(encoding="utf-8") == "NEW = 1\n"
    assert not (repo / "dags" / "stale.py").exists() and not (repo / "dags" / "__pycache__").exists()
    for gone in ("healer.yaml", "healer", "data/processed", "tests"):
        assert not (repo / gone).exists(), gone
    assert (repo / "fixtures" / "fake_llm" / "script.yaml").is_file()
    assert _git(repo, "log", "--format=%s") == "Initial legacy repo"  # a single, new commit
    assert _git(repo, "rev-list", "--count", "HEAD") == "1"
    assert demo.healer_branches(repo) == []
    assert _git(repo, "status", "--porcelain") == ""


def test_commit_config_day2_and_branches(demo: ModuleType, tmp_path: Path, isolated_git: None) -> None:
    example = _example(tmp_path)
    repo = demo.refresh_demo_repo(example, tmp_path / "repo")
    with pytest.raises(demo.DemoError, match="healer.yaml"):
        demo.commit_config(repo)
    (repo / "healer.yaml").write_text("version: 1\n", encoding="utf-8")
    (repo / "healer" / "contracts").mkdir(parents=True)
    (repo / "healer" / "contracts" / "sales.yaml").write_text("dag_id: sales\n", encoding="utf-8")
    assert demo.commit_config(repo) is True
    assert _git(repo, "log", "-1", "--format=%s") == "Add dag-healer config and contracts"
    assert demo.commit_config(repo) is False

    assert demo.copy_day2(repo, example / "fixtures" / "day2") == ["sales.csv"]
    assert (repo / "data" / "incoming" / "sales.csv").read_text(encoding="utf-8") == "a\n2\n"
    assert _git(repo, "status", "--porcelain") == "M data/incoming/sales.csv"

    _git(repo, "branch", "healer/customers_sync-transform-1234abcd")
    assert demo.healer_branches(repo) == ["healer/customers_sync-transform-1234abcd"]


# --------------------------------------------------------------------------- store


@pytest.mark.parametrize("reset", [True, False])
def test_prepare_store_migrates_before_anything_writes(
    demo: ModuleType, monkeypatch: pytest.MonkeyPatch, reset: bool
) -> None:
    calls: list[tuple[str, list[str]]] = []

    def compose_exec(service: str, command: list[str], *, check: bool = True) -> tuple[int, str]:
        calls.append((service, list(command)))
        return 0, ""

    monkeypatch.setattr(demo, "compose_exec", compose_exec)
    demo.prepare_store(reset)
    expected = [(demo.APISERVER, ["dag-healer", "store", "migrate"])]
    if reset:
        expected.append((demo.APISERVER, ["dag-healer", "store", "reset", "--yes", "--artifacts"]))
    assert calls == expected


# --------------------------------------------------------------------------- HTTP session


class FakeTransport:
    def __init__(self, demo: ModuleType, replies: dict[str, list[tuple[int, Any]]]) -> None:
        self.demo = demo
        self.replies = replies
        self.calls: list[tuple[str, str, dict[str, str], Any]] = []

    def __call__(self, method: str, url: str, *, json_body: Any = None, headers: Any = None, **_: Any) -> Any:
        self.calls.append((method, url, dict(headers or {}), json_body))
        status, body = self.replies[f"{method} {url}"].pop(0)
        return self.demo.Response(status, json.dumps(body))


def test_session_logs_in_sends_csrf_header_and_retries_on_401(demo: ModuleType) -> None:
    base = "http://airflow"
    transport = FakeTransport(
        demo,
        {
            "POST http://airflow/auth/token": [(201, {"access_token": "t1"}), (201, {"access_token": "t2"})],
            "GET http://airflow/healer/api/v1/incidents": [(401, {"detail": "expired"}), (200, {"total": 0})],
            "POST http://airflow/healer/api/v1/x": [(202, {"command_id": 1}), (403, {"detail": "no"})],
        },
    )
    api = demo.AirflowSession("admin", "pw", base_url=base, transport=transport)
    assert api.ok("GET", "/healer/api/v1/incidents") == {"total": 0}
    tokens = [h.get("Authorization") for m, _u, h, _b in transport.calls if m == "GET"]
    assert tokens == ["Bearer t1", "Bearer t2"]  # logged in again once after the 401
    assert transport.calls[0][3] == {"username": "admin", "password": "pw"}

    assert api.healer("POST", "/x", json_body={"a": 1}).status == 202
    assert transport.calls[-1][2]["X-Requested-With"] == "dag-healer"
    reply = api.healer("POST", "/x", csrf=False)
    assert "X-Requested-With" not in transport.calls[-1][2]
    assert (reply.status, reply.detail) == (403, "no")
    get_headers = [h for m, _u, h, _b in transport.calls if m == "GET"]
    assert all("X-Requested-With" not in h for h in get_headers)


def test_session_reports_failed_login_and_unexpected_status(demo: ModuleType) -> None:
    transport = FakeTransport(
        demo,
        {
            "POST http://a/auth/token": [
                (401, {"detail": "Invalid credentials"}),
                (201, {"access_token": "t"}),
            ],
            "GET http://a/api/v2/dags/x": [(404, {"detail": "DAG not found"})],
        },
    )
    api = demo.AirflowSession("admin", "bad", base_url="http://a", transport=transport)
    with pytest.raises(demo.DemoError, match="Invalid credentials"):
        api.login()
    with pytest.raises(demo.DemoError, match="HTTP 404: DAG not found"):
        api.ok("GET", "/api/v2/dags/x")


class _Handler(BaseHTTPRequestHandler):
    def do_GET(self) -> None:  # noqa: N802 - http.server API
        status = 200 if self.path == "/ok" else 404
        body = json.dumps({"path": self.path} if status == 200 else {"detail": "missing"}).encode()
        self.send_response(status)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def log_message(self, *_args: Any) -> None:
        return


def test_http_returns_errors_as_responses(demo: ModuleType) -> None:
    server = ThreadingHTTPServer(("127.0.0.1", 0), _Handler)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    try:
        base = f"http://127.0.0.1:{server.server_address[1]}"
        assert demo.http("GET", f"{base}/ok").json() == {"path": "/ok"}
        missing = demo.http("GET", f"{base}/nope")
        assert (missing.status, missing.detail) == (404, "missing")
    finally:
        server.shutdown()
        server.server_close()
    with pytest.raises(demo.DemoError, match="failed"):
        demo.http("GET", f"{base}/ok", timeout=2)


# --------------------------------------------------------------------------- incidents


def _incident(dag: str, run: str, status: str, category: str | None = None, **extra: Any) -> dict[str, Any]:
    return {
        "id": f"{dag}-{run}",
        "dag_id": dag,
        "run_id": run,
        "task_id": {"sales_daily_etl": "transform_sales", "customers_sync": "transform"}.get(
            dag, "build_report"
        ),
        "status": status,
        "category": category,
        "created_at": extra.pop("created_at", "2026-10-19T10:00:00Z"),
        **extra,
    }


class FakeApi:
    def __init__(self, pages: list[list[dict[str, Any]]]) -> None:
        self.pages = pages
        self.calls = 0

    def ok(self, method: str, path: str, **_: Any) -> dict[str, Any]:
        assert (method, path) == ("GET", "/healer/api/v1/incidents?limit=200")
        page = self.pages[min(self.calls, len(self.pages) - 1)]
        self.calls += 1
        return {"incidents": page, "total": len(page)}


def test_wait_for_incidents_until_every_run_rests(
    demo: ModuleType, capsys: pytest.CaptureFixture[str]
) -> None:
    runs = {"sales_daily_etl": "r1", "customers_sync": "r1", "inventory_report": "r1"}
    old = _incident("sales_daily_etl", "r0", "reported", "data")  # an earlier run: ignored
    pages = [
        [old, _incident("sales_daily_etl", "r1", "triaging")],
        [
            _incident("sales_daily_etl", "r1", "reported", "data"),
            _incident("customers_sync", "r1", "repairing", "code"),
            _incident("inventory_report", "r1", "waiting_human", "ambiguous"),
        ],
        [
            _incident("sales_daily_etl", "r1", "reported", "data"),
            _incident("customers_sync", "r1", "repaired", "code"),
            _incident("inventory_report", "r1", "waiting_human", "ambiguous"),
        ],
    ]
    api = FakeApi(pages)
    found = demo.wait_for_incidents(api, runs, 60, sleep=lambda _s: None, clock=lambda: 0.0)
    assert api.calls == 3
    assert demo.check_outcomes(found) == []
    out = capsys.readouterr().out
    assert "incident of customers_sync.transform: repairing (code)" in out


def test_wait_for_incidents_times_out_with_what_it_saw(demo: ModuleType) -> None:
    ticks = iter([0.0, 0.0, 100.0])
    api = FakeApi([[_incident("sales_daily_etl", "r1", "triaging")]])
    with pytest.raises(demo.DemoError, match="missing: \\['customers_sync'\\]"):
        demo.wait_for_incidents(
            api,
            {"sales_daily_etl": "r1", "customers_sync": "r1"},
            10,
            sleep=lambda _s: None,
            clock=lambda: next(ticks),
        )


def test_check_outcomes_lists_every_difference(demo: ModuleType) -> None:
    found = {
        "sales_daily_etl": _incident("sales_daily_etl", "r", "reported", "data"),
        "customers_sync": _incident("customers_sync", "r", "repair_failed", "code"),
    }
    problems = demo.check_outcomes(found)
    assert problems == [
        "customers_sync: expected task/category/status transform/code/repaired, "
        "got transform/code/repair_failed",
        "inventory_report: no incident",
    ]


def test_incidents_of_runs_keeps_the_newest(demo: ModuleType) -> None:
    first = _incident("inventory_report", "r", "repaired", "code", created_at="2026-10-19T10:00:00Z")
    second = _incident("inventory_report", "r", "new", None, created_at="2026-10-19T11:00:00Z", id="b")
    assert demo.incidents_of_runs([first, second], {"inventory_report": "r"})["inventory_report"]["id"] == "b"


def test_parse_time_accepts_api_formats(demo: ModuleType) -> None:
    assert demo.parse_time("2026-10-19T10:00:00.12Z").microsecond == 120000
    assert demo.parse_time("2026-10-19T10:00:00.123456789+00:00").microsecond == 123456
    assert demo.parse_time("2026-10-19T10:00:00").tzinfo is not None
    assert demo.parse_time("garbage") is None and demo.parse_time(None) is None


# --------------------------------------------------------------------------- e2e helpers


def test_e2e_evidence_parsers(e2e: ModuleType) -> None:
    diff = (
        "diff --git a/src/etl/customers.py b/src/etl/customers.py\n--- a/x\n+++ b/x\n"
        "diff --git a/tests/test_customers_signup_date.py b/tests/test_customers_signup_date.py\n"
        "new file mode 100644\n"
    )
    assert e2e.diff_files(diff) == ["src/etl/customers.py", "tests/test_customers_signup_date.py"]
    assert e2e.diff_files(None) == []
    events = [
        {"kind": "status", "data": {"from": "waiting_human", "to": "resumed"}},
        {"kind": "command", "data": {}},
        {"kind": "status", "data": {"from": "resumed", "to": "triaging"}},
        {"kind": "status", "data": {"from": "triaging", "to": "triaging"}},
        {"kind": "status", "data": {"from": "triaging", "to": "repair_failed"}},
    ]
    assert e2e.status_path(events) == ["resumed", "triaging", "repair_failed"]
    assert e2e.settled({"status": "repair_failed", "pending_commands": []}, previous="waiting_human")
    assert not e2e.settled({"status": "waiting_human", "pending_commands": []}, previous="waiting_human")
    assert not e2e.settled({"status": "repaired", "pending_commands": [{"id": 1}]}, previous="waiting_human")
    lines = e2e.format_timeline(
        [{"at": "2026-10-19T10:11:12Z", "kind": "status", "actor": "x", "message": "a\nb"}]
    )
    assert lines == ["10:11:12  status          x            a b"]


def test_e2e_raw_values_come_from_the_day2_files(e2e: ModuleType) -> None:
    values = e2e.raw_values()
    assert "olivia.martin@example.com" in values  # customers.csv e-mail
    assert "Olivia Martin" in values  # customers.csv full name
    assert "emma.rodriguez@example.com" in values  # sales.csv customer_email
    assert e2e.leaked("hash h:1234 for olivia.martin@example.com", values) == ["olivia.martin@example.com"]
    assert e2e.leaked("only h:abcd and ***", values) == []


def test_e2e_report_counts_failures(e2e: ModuleType, capsys: pytest.CaptureFixture[str]) -> None:
    report = e2e.Report()
    assert report.check("works", True) is True
    assert report.check("broken", False, {"why": 1}) is False
    assert report.failed == ["broken"]
    assert '[e2e] FAIL broken: {"why": 1}' in capsys.readouterr().out


def test_incidents_of_runs_accepts_a_failure_folded_into_an_open_incident(demo: ModuleType) -> None:
    since = demo.parse_time("2026-10-19T12:00:00Z")
    open_one = _incident(
        "inventory_report", "old", "waiting_human", "ambiguous", updated_at="2026-10-19T12:00:09Z"
    )
    stale = _incident("sales_daily_etl", "old", "reported", "data", updated_at="2026-10-19T11:00:00Z")
    runs = {"inventory_report": "new", "sales_daily_etl": "new"}
    assert demo.incidents_of_runs([open_one, stale], runs) == {}
    found = demo.incidents_of_runs([open_one, stale], runs, since=since)
    assert set(found) == {"inventory_report"}  # the folded repeat counts, the stale one does not
    exact = _incident("inventory_report", "new", "new", None, id="exact")
    assert demo.incidents_of_runs([open_one, exact], runs, since=since)["inventory_report"]["id"] == "exact"


# --------------------------------------------------------------------------- test sandbox

DOCTOR_OUTPUT = """\
dag-healer doctor (0.1.0)
[ OK ] config        /opt/airflow/repo/healer.yaml
[WARN] ui bundle     an older bundle
[FAIL] sandbox       repo.sandbox.image is not set; set it (or HEALER_SANDBOX_IMAGE) to an image
[SKIP] github        no token

1 ok, 1 warning(s), 1 error(s), 1 skipped
"""


def test_doctor_check_reads_one_check(demo: ModuleType) -> None:
    assert demo.doctor_check(DOCTOR_OUTPUT) == (
        "FAIL",
        "repo.sandbox.image is not set; set it (or HEALER_SANDBOX_IMAGE) to an image",
    )
    assert demo.doctor_check(DOCTOR_OUTPUT, "ui bundle") == ("WARN", "an older bundle")
    assert demo.doctor_check(DOCTOR_OUTPUT, "config") == ("OK", "/opt/airflow/repo/healer.yaml")
    assert demo.doctor_check(DOCTOR_OUTPUT, "smtp") is None
    assert demo.doctor_check("Error: service healer-worker is not running") is None


@pytest.mark.parametrize("label", ["OK", "WARN", "FAIL"])
def test_check_sandbox_runs_the_doctor_in_the_worker(
    demo: ModuleType, monkeypatch: pytest.MonkeyPatch, label: str
) -> None:
    calls: list[tuple[str, list[str], bool]] = []

    def compose_exec(service: str, command: list[str], *, check: bool = True) -> tuple[int, str]:
        calls.append((service, list(command), check))
        return (1 if label == "FAIL" else 0), f"[{label:^4}] sandbox  docker 29.8.2, image x\n"

    monkeypatch.setattr(demo, "compose_exec", compose_exec)
    if label == "FAIL":
        with pytest.raises(demo.DemoError, match="sandbox does not work in healer-worker: docker 29.8.2"):
            demo.check_sandbox()
    else:
        assert demo.check_sandbox() == (label, "docker 29.8.2, image x")
    assert calls == [(demo.WORKER, ["dag-healer", "doctor"], False)]


def test_check_sandbox_without_a_sandbox_check(demo: ModuleType, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(demo, "compose_exec", lambda service, command, *, check=True: (1, "no such service"))
    with pytest.raises(demo.DemoError, match="printed no sandbox check"):
        demo.check_sandbox()


def test_e2e_sandbox_runs_of_one_incident(e2e: ModuleType) -> None:
    prefix = "healer-worker-1  | 10:00:01 INFO dag_healer.repo.pytest_runner: pytest"
    work = "/opt/airflow/healer/work"
    log = "\n".join(
        [
            f"{prefix} (default discovery) in {work}/abc123 (container sandbox) -> exit 0: 3 passed",
            f"{prefix} tests/test_x.py in {work}/abc123 (container sandbox) -> exit 1: 1 failed",
            f"{prefix} tests/test_y.py in {work}/other (process sandbox) -> exit 0: 1 passed",
            "healer-worker-1  | 10:00:09 INFO dag_healer.worker: incident abc123: repaired",
        ]
    )
    assert e2e.sandbox_runs(log, "abc123") == ["container", "container"]
    assert e2e.sandbox_runs(log, "other") == ["process"]
    assert e2e.sandbox_runs(log, "") == []


def test_e2e_socket_mounts(e2e: ModuleType) -> None:
    compose = yaml.safe_load((REPO_ROOT / "docker" / "compose.yaml").read_text(encoding="utf-8"))
    assert e2e.socket_mounts(compose) == {"healer-worker": ["/var/run/docker.sock"]}
    podman = {"type": "bind", "source": "/run/user/1000/podman/podman.sock", "target": "/var/run/docker.sock"}
    home = {"type": "volume", "source": "healer-home", "target": "/opt/airflow/healer"}
    repo = {"type": "bind", "source": "/srv/repo", "target": "/opt/airflow/repo"}
    rendered = {  # docker compose config --format json writes the long syntax
        "services": {"worker": {"volumes": [podman, home]}, "scheduler": {"volumes": [repo]}, "mailpit": {}}
    }
    assert e2e.socket_mounts(rendered) == {"worker": ["/var/run/docker.sock"]}


def test_e2e_sandbox_label_is_the_one_of_the_sandbox(e2e: ModuleType) -> None:
    from dag_healer.repo.sandbox import CONTAINER_LABEL

    assert e2e.SANDBOX_LABEL == f"{CONTAINER_LABEL}=1"


def test_e2e_reads_the_log_line_of_run_pytest(
    e2e: ModuleType, tmp_path: Path, caplog: pytest.LogCaptureFixture
) -> None:
    from dag_healer.repo.pytest_runner import run_pytest
    from dag_healer.repo.sandbox import SandboxJob, SandboxProbe, SandboxRun

    class OneRun:  # a container sandbox whose run writes a pytest summary
        name = "container"
        isolated = True

        def unavailable_reason(self) -> str | None:
            return None

        def smoke_test(self) -> SandboxProbe:
            return SandboxProbe(True, "ok")

        def run(self, job: SandboxJob) -> SandboxRun:
            job.output.write_text("==== 1 passed in 0.01s ====\n", encoding="utf-8")
            return SandboxRun(0)

    clone = tmp_path / "work" / "abc123"
    clone.mkdir(parents=True)
    with caplog.at_level("INFO", logger="dag_healer.repo.pytest_runner"):
        run_pytest(clone, ["tests/test_x.py"], sandbox=OneRun())
    assert e2e.sandbox_runs(caplog.text, "abc123") == ["container"]
