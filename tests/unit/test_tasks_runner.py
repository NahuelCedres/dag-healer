"""Unit tests for scripts/tasks.py and scripts/build_ui.py (no Docker needed)."""

from __future__ import annotations

import importlib.util
import json
import os
import stat
import subprocess
import sys
import threading
from collections.abc import Iterator
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from types import ModuleType, SimpleNamespace

import pytest

REPO_ROOT = Path(__file__).resolve().parents[2]
SCRIPTS = REPO_ROOT / "scripts"


def _load(path: Path, name: str) -> ModuleType:
    spec = importlib.util.spec_from_file_location(name, path)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    sys.modules[name] = module
    spec.loader.exec_module(module)
    return module


@pytest.fixture
def tasks() -> Iterator[ModuleType]:
    module = _load(SCRIPTS / "tasks.py", "dag_healer_tasks_under_test")
    yield module
    sys.modules.pop("dag_healer_tasks_under_test", None)


@pytest.fixture
def isolated_git(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    """Ignore the developer's global/system git config (signing, hooks, autocrlf)."""
    empty = tmp_path / "empty.gitconfig"
    empty.write_text("", encoding="utf-8")
    monkeypatch.setenv("GIT_CONFIG_GLOBAL", str(empty))
    monkeypatch.setenv("GIT_CONFIG_NOSYSTEM", "1")


def _git_out(repo: Path, *args: str) -> str:
    return subprocess.run(
        ["git", "-C", str(repo), *args], check=True, capture_output=True, text=True
    ).stdout.strip()


# --------------------------------------------------------------------------- argument parsing


def test_verbatim_commands_forward_everything(tasks: ModuleType) -> None:
    args, extra = tasks.parse(["airflow", "dags", "list", "-o", "json"])
    assert args.command == "airflow"
    assert extra == ["dags", "list", "-o", "json"]
    args, extra = tasks.parse(["healer", "--help"])
    assert args.command == "healer"
    assert extra == ["--help"]


def test_forwarding_commands_keep_own_flags(tasks: ModuleType) -> None:
    args, extra = tasks.parse(["test-airflow", "-x", "--no-build", "-k", "auth"])
    assert args.no_build is True
    assert extra == ["-x", "-k", "auth"]
    _, extra = tasks.parse(["test", "--", "-k", "config"])
    assert extra == ["-k", "config"]


def test_strict_commands_reject_unknown_arguments(tasks: ModuleType) -> None:
    with pytest.raises(SystemExit) as excinfo:
        tasks.parse(["up", "--bogus"])
    assert excinfo.value.code == 2


def test_up_and_logs_options(tasks: ModuleType) -> None:
    args, _ = tasks.parse(["up", "--worker", "--timeout", "30", "--build"])
    assert (args.worker, args.timeout, args.build, args.no_wait) == (True, 30, True, False)
    args, _ = tasks.parse(["logs", "-f", "airflow-scheduler", "--tail", "5"])
    assert (args.services, args.follow, args.tail) == (["airflow-scheduler"], True, 5)
    args, _ = tasks.parse(["demo-reset", "--yes"])
    assert args.yes is True and args.func is tasks.cmd_reset


# --------------------------------------------------------------------------- node / UI


@pytest.mark.parametrize(
    ("version", "supported"),
    [
        ((20, 17, 0), False),
        ((20, 19, 0), True),
        ((21, 7, 3), False),
        ((22, 11, 0), False),
        ((22, 12, 0), True),
        ((24, 0, 0), True),
    ],
)
def test_node_version_supported(tasks: ModuleType, version: tuple[int, int, int], supported: bool) -> None:
    assert tasks.node_version_supported(version) is supported


def test_parse_node_version(tasks: ModuleType) -> None:
    assert tasks.parse_node_version("v22.12.0\n") == (22, 12, 0)
    assert tasks.parse_node_version("garbage") is None


def test_install_bundle_prefers_main(
    tasks: ModuleType, monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    dist = tmp_path / "dist"
    dist.mkdir()
    (dist / "aaa.umd.cjs").write_text("other", encoding="utf-8")
    (dist / "main.umd.cjs").write_text("main bundle", encoding="utf-8")
    target = tmp_path / "pkg" / "static" / "main.umd.cjs"
    monkeypatch.setattr(tasks, "BUNDLE_PATH", target)
    assert tasks.install_bundle(dist) == target
    assert target.read_text(encoding="utf-8") == "main bundle"


def test_install_bundle_without_bundle_fails(tasks: ModuleType, tmp_path: Path) -> None:
    with pytest.raises(tasks.TaskError):
        tasks.install_bundle(tmp_path)


def test_build_ui_auto_falls_back_to_docker(
    tasks: ModuleType, monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    (tmp_path / "package.json").write_text("{}", encoding="utf-8")
    monkeypatch.setattr(tasks, "UI_DIR", tmp_path)
    monkeypatch.setattr(tasks, "local_node_status", lambda: (False, "node 20.17.0 is too old"))
    calls: list[str] = []
    monkeypatch.setattr(tasks, "build_ui_docker", lambda: calls.append("docker") or 0)
    monkeypatch.setattr(tasks, "build_ui_local", lambda: calls.append("local") or 0)
    assert tasks.build_ui("auto") == 0
    assert calls == ["docker"]
    with pytest.raises(tasks.TaskError, match="too old"):
        tasks.build_ui("local")


def test_build_ui_auto_prefers_local_node(
    tasks: ModuleType, monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    (tmp_path / "package.json").write_text("{}", encoding="utf-8")
    monkeypatch.setattr(tasks, "UI_DIR", tmp_path)
    monkeypatch.setattr(tasks, "local_node_status", lambda: (True, "node 22.12.0"))
    calls: list[str] = []
    monkeypatch.setattr(tasks, "build_ui_docker", lambda: calls.append("docker") or 0)
    monkeypatch.setattr(tasks, "build_ui_local", lambda: calls.append("local") or 0)
    assert tasks.build_ui() == 0
    assert calls == ["local"]


def test_build_ui_requires_sources(
    tasks: ModuleType, monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    monkeypatch.setattr(tasks, "UI_DIR", tmp_path / "missing-ui")
    with pytest.raises(tasks.TaskError, match="package.json"):
        tasks.build_ui("docker")


def test_build_ui_script_delegates(monkeypatch: pytest.MonkeyPatch) -> None:
    build_ui = _load(SCRIPTS / "build_ui.py", "dag_healer_build_ui_under_test")
    seen: list[str] = []

    class FakeTaskError(Exception):
        code = 3

    def fake_build(mode: str) -> int:
        seen.append(mode)
        if mode == "local":
            raise FakeTaskError("no node")
        return 0

    stub = SimpleNamespace(build_ui=fake_build, TaskError=FakeTaskError)
    monkeypatch.setattr(build_ui, "load_tasks", lambda: stub)
    assert build_ui.main([]) == 0
    assert build_ui.main(["--mode", "local"]) == 3
    assert seen == ["auto", "local"]
    sys.modules.pop("dag_healer_build_ui_under_test", None)


# --------------------------------------------------------------------------- health


@pytest.mark.parametrize(
    ("payload", "healthy"),
    [
        ({"metadatabase": {"status": "healthy"}, "scheduler": {"status": "healthy"}}, True),
        ({"metadatabase": {"status": "healthy"}, "scheduler": {"status": "unhealthy"}}, False),
        ({"metadatabase": {"status": "healthy"}}, False),
        ([], False),
    ],
)
def test_evaluate_health(tasks: ModuleType, payload: object, healthy: bool) -> None:
    assert tasks.evaluate_health(payload)[0] is healthy


def test_evaluate_health_detail_names_component(tasks: ModuleType) -> None:
    ok, detail = tasks.evaluate_health({"metadatabase": {"status": "healthy"}, "scheduler": {"status": None}})
    assert not ok
    assert detail == "scheduler=unknown"


class _HealthHandler(BaseHTTPRequestHandler):
    status_code = 200
    body = b"{}"

    def do_GET(self) -> None:  # noqa: N802 - http.server API
        self.send_response(self.status_code)
        self.send_header("Content-Type", "application/json")
        self.end_headers()
        self.wfile.write(self.body)

    def log_message(self, *args: object) -> None:
        return


def _serve(handler: type[BaseHTTPRequestHandler]) -> tuple[ThreadingHTTPServer, str]:
    server = ThreadingHTTPServer(("127.0.0.1", 0), handler)
    threading.Thread(target=server.serve_forever, daemon=True).start()
    return server, f"http://127.0.0.1:{server.server_address[1]}/api/v2/monitor/health"


def test_check_health_against_http_server(tasks: ModuleType) -> None:
    class Healthy(_HealthHandler):
        body = json.dumps(
            {"metadatabase": {"status": "healthy"}, "scheduler": {"status": "healthy"}}
        ).encode()

    class Broken(_HealthHandler):
        status_code = 503

    server, url = _serve(Healthy)
    try:
        assert tasks.check_health(url) == (True, "healthy")
    finally:
        server.shutdown()
    server, url = _serve(Broken)
    try:
        assert tasks.check_health(url) == (False, "HTTP 503")
    finally:
        server.shutdown()


def test_wait_for_airflow_polls_until_healthy(tasks: ModuleType) -> None:
    answers = iter([(False, "metadatabase=unknown"), (False, "scheduler=unhealthy"), (True, "healthy")])
    assert tasks.wait_for_airflow(10, interval=0, probe=lambda _url: next(answers)) is True


def test_wait_for_airflow_times_out(tasks: ModuleType) -> None:
    assert tasks.wait_for_airflow(0, interval=0, probe=lambda _url: (False, "down")) is False


# --------------------------------------------------------------------------- env file / compose


def test_ensure_env_file_copies_example(
    tasks: ModuleType, monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    example = tmp_path / ".env.example"
    example.write_text("# comment\nAIRFLOW_UID=50000\nHEALER_INGEST_TOKEN=dev\n", encoding="utf-8")
    env_file = tmp_path / ".env"
    monkeypatch.setattr(tasks, "ENV_EXAMPLE", example)
    monkeypatch.setattr(tasks, "ENV_FILE", env_file)
    monkeypatch.setattr(tasks, "host_uid", lambda: 1234)
    tasks.ensure_env_file()
    text = env_file.read_text(encoding="utf-8")
    assert "AIRFLOW_UID=1234\n" in text
    assert "HEALER_INGEST_TOKEN=dev" in text
    env_file.write_text("CUSTOM=1\n", encoding="utf-8")
    tasks.ensure_env_file()
    assert env_file.read_text(encoding="utf-8") == "CUSTOM=1\n"


def test_ensure_env_file_keeps_default_uid(
    tasks: ModuleType, monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    example = tmp_path / ".env.example"
    example.write_text("AIRFLOW_UID=50000\n", encoding="utf-8")
    monkeypatch.setattr(tasks, "ENV_EXAMPLE", example)
    monkeypatch.setattr(tasks, "ENV_FILE", tmp_path / ".env")
    monkeypatch.setattr(tasks, "host_uid", lambda: None)
    tasks.ensure_env_file()
    assert (tmp_path / ".env").read_text(encoding="utf-8") == "AIRFLOW_UID=50000\n"


def test_compose_base_arguments(tasks: ModuleType, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(tasks, "ensure_docker_running", lambda: "docker")
    monkeypatch.setattr(tasks, "ensure_env_file", lambda: tasks.ENV_FILE)
    cmd = tasks.compose_base(["worker", "tests"])  # any profile names are passed through
    assert cmd[:4] == ["docker", "compose", "--project-name", "dag-healer"]
    assert cmd[cmd.index("--file") + 1] == str(REPO_ROOT / "docker" / "compose.yaml")
    assert cmd[cmd.index("--env-file") + 1] == str(REPO_ROOT / "docker" / ".env")
    assert cmd[-4:] == ["--profile", "worker", "--profile", "tests"]


def test_healer_command_execs_in_apiserver(tasks: ModuleType, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(tasks, "compose_base", lambda profiles=(): ["docker", "compose"])
    monkeypatch.setattr(tasks, "is_interactive", lambda: False)
    recorded: list[list[str]] = []
    monkeypatch.setattr(tasks, "run", lambda cmd, **_kw: recorded.append(list(cmd)) or 0)
    assert tasks.main(["healer", "contracts", "list"]) == 0
    assert recorded == [
        ["docker", "compose", "exec", "-T", "airflow-apiserver", "dag-healer", "contracts", "list"]
    ]


def test_up_runs_compose_with_wait(tasks: ModuleType, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(tasks, "prepare_demo_repo", lambda: None)
    monkeypatch.setattr(tasks, "compose_base", lambda profiles=(): ["docker", "compose", *profiles])
    monkeypatch.setattr(tasks, "wait_for_airflow", lambda timeout: True)
    monkeypatch.setattr(tasks, "print_urls", lambda **_kw: None)
    recorded: list[list[str]] = []
    monkeypatch.setattr(tasks, "run", lambda cmd, **_kw: recorded.append(list(cmd)) or 0)
    assert tasks.main(["up", "--timeout", "42"]) == 0
    assert tasks.main(["up", "--worker"]) == 0  # the old flag still works (worker is the default)
    assert tasks.main(["up", "--no-worker"]) == 0
    cmd, legacy, no_worker = recorded
    assert cmd[:3] == ["docker", "compose", "up"]  # no profile: the worker is a default service
    assert ["--wait", "--wait-timeout", "42"] == cmd[cmd.index("--wait") : cmd.index("--wait") + 3]
    assert cmd[-1] == "healer-worker"
    assert "airflow-apiserver" in cmd
    assert legacy[-1] == "healer-worker"
    assert "healer-worker" not in no_worker and no_worker[-1] == "airflow-triggerer"


def test_demo_and_e2e_forward_everything(tasks: ModuleType) -> None:
    args, extra = tasks.parse(["demo", "--listener-only", "--timeout", "60", "--help"])
    assert args.command == "demo"
    assert extra == ["--listener-only", "--timeout", "60", "--help"]
    args, extra = tasks.parse(["e2e", "--no-demo"])
    assert (args.command, extra) == ("e2e", ["--no-demo"])


def test_failed_command_exit_code_propagates(tasks: ModuleType, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(tasks, "compose_base", lambda profiles=(): ["docker", "compose"])
    monkeypatch.setattr(tasks, "run", lambda cmd, **_kw: 17)
    assert tasks.main(["ps"]) == 17


def test_task_error_becomes_exit_code(
    tasks: ModuleType, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    def boom() -> str:
        raise tasks.TaskError("The Docker daemon is not reachable.")

    monkeypatch.setattr(tasks, "ensure_docker_running", boom)
    assert tasks.main(["ps"]) == 1
    assert "Docker daemon is not reachable" in capsys.readouterr().err


def test_test_command_uses_host_python(tasks: ModuleType, monkeypatch: pytest.MonkeyPatch) -> None:
    recorded: list[tuple[list[str], object]] = []
    monkeypatch.setattr(tasks, "run", lambda cmd, cwd=None, **_kw: recorded.append((list(cmd), cwd)) or 0)
    assert tasks.main(["test"]) == 0
    assert tasks.main(["test", "-k", "masking"]) == 0
    python = tasks.host_python()
    assert recorded[0] == ([python, "-m", "pytest", "tests/unit"], tasks.ROOT)
    assert recorded[1] == ([python, "-m", "pytest", "-k", "masking"], tasks.ROOT)


def test_demo_placeholder_without_script(
    tasks: ModuleType, monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    monkeypatch.setattr(tasks, "SCRIPTS_DIR", tmp_path)
    assert tasks.main(["demo"]) == 1


def test_reset_needs_confirmation(tasks: ModuleType, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(tasks, "is_interactive", lambda: False)
    monkeypatch.setattr(tasks, "run", lambda cmd, **_kw: pytest.fail("must not run docker without --yes"))
    assert tasks.main(["reset"]) == 1


# --------------------------------------------------------------------------- demo repository


def test_prepare_demo_repo_copies_and_commits(
    tasks: ModuleType, monkeypatch: pytest.MonkeyPatch, tmp_path: Path, isolated_git: None
) -> None:
    example = tmp_path / "example repo"
    (example / "dags").mkdir(parents=True)
    (example / "dags" / "sales.py").write_text("print('dag')\n", encoding="utf-8")
    (example / "src" / "__pycache__").mkdir(parents=True)
    (example / "src" / "__pycache__" / "x.cpython-313.pyc").write_bytes(b"\x00")
    (example / "src" / "etl.py").write_text("X = 1\r\n", encoding="utf-8", newline="")
    demo = tmp_path / "demo dir"
    monkeypatch.setattr(tasks, "EXAMPLE_REPO", example)
    monkeypatch.setattr(tasks, "DEMO_DIR", demo)
    monkeypatch.setattr(tasks, "DEMO_REPO", demo / "repo")
    monkeypatch.setattr(tasks, "DEMO_LOGS", demo / "logs")

    repo = tasks.prepare_demo_repo()

    assert (demo / "logs").is_dir()
    assert (repo / "dags" / "sales.py").is_file()
    assert not (repo / "src" / "__pycache__").exists()
    assert _git_out(repo, "rev-parse", "--abbrev-ref", "HEAD") == "main"
    assert _git_out(repo, "log", "-1", "--format=%s|%an|%ae") == (
        "Initial legacy repo|dag-healer demo|dag-healer-demo@example.com"
    )
    assert _git_out(repo, "status", "--porcelain") == ""
    # bytes are committed as copied (no CRLF normalisation)
    assert (
        subprocess.run(
            ["git", "-C", str(repo), "show", "HEAD:src/etl.py"], check=True, capture_output=True
        ).stdout
        == b"X = 1\r\n"
    )

    head = _git_out(repo, "rev-parse", "HEAD")
    tasks.prepare_demo_repo()  # idempotent
    assert _git_out(repo, "rev-parse", "HEAD") == head


def test_prepare_demo_repo_without_example(
    tasks: ModuleType, monkeypatch: pytest.MonkeyPatch, tmp_path: Path, isolated_git: None
) -> None:
    demo = tmp_path / "demo"
    monkeypatch.setattr(tasks, "EXAMPLE_REPO", tmp_path / "does-not-exist")
    monkeypatch.setattr(tasks, "DEMO_DIR", demo)
    monkeypatch.setattr(tasks, "DEMO_REPO", demo / "repo")
    monkeypatch.setattr(tasks, "DEMO_LOGS", demo / "logs")
    repo = tasks.prepare_demo_repo()
    assert (repo / "dags").is_dir()
    assert _git_out(repo, "log", "-1", "--format=%s") == "Initial legacy repo"


def test_remove_tree_handles_read_only_files(tasks: ModuleType, tmp_path: Path) -> None:
    target = tmp_path / "tree"
    (target / "objects").mkdir(parents=True)
    locked = target / "objects" / "pack"
    locked.write_text("x", encoding="utf-8")
    os.chmod(locked, stat.S_IREAD)
    tasks.remove_tree(target)
    assert not target.exists()
    tasks.remove_tree(target)  # missing directory is fine


def test_capture_times_out(tasks: ModuleType) -> None:
    code, out = tasks.capture([sys.executable, "-c", "import time; time.sleep(10)"], timeout=0.5)
    assert code == 124
    assert "did not finish within 0.5 s" in out


def test_hanging_docker_daemon_is_reported(tasks: ModuleType, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(tasks, "which", lambda name, hint: "docker")
    seen: list[object] = []
    monkeypatch.setattr(
        tasks, "capture", lambda cmd, **kw: seen.append(kw.get("timeout")) or (124, "timeout")
    )
    with pytest.raises(tasks.TaskError, match="did not answer within 60 s"):
        tasks.ensure_docker_running()
    assert seen == [tasks.DOCKER_INFO_TIMEOUT]
