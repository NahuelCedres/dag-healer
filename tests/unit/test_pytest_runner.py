"""Tests for the pytest runner (dag_healer.repo.pytest_runner) and its process sandbox."""

from __future__ import annotations

import json
import logging
import os
import sys
import textwrap
import time
from pathlib import Path

import pytest

from dag_healer.repo import sandbox as sandbox_module
from dag_healer.repo.pytest_runner import (
    CLOUD_SANDBOX_ENV,
    LAUNCH_FAILED_EXIT_CODE,
    MAX_OUTPUT_CHARS,
    TIMEOUT_EXIT_CODE,
    PytestResult,
    parse_summary,
    scrubbed_env,
    truncate_tail,
)
from dag_healer.repo.pytest_runner import run_pytest as _run_pytest
from dag_healer.repo.sandbox import ProcessSandbox, Sandbox, SandboxJob, SandboxProbe, SandboxRun


def run_pytest(*args: object, **kwargs: object) -> PytestResult:
    """run_pytest with the process sandbox unless a test passes another one (it is required)."""
    kwargs.setdefault("sandbox", ProcessSandbox())
    return _run_pytest(*args, **kwargs)  # type: ignore[arg-type]


PROJECT_FILES = {
    # A pytest.ini keeps the inner run from looking for configuration in parent folders.
    "pytest.ini": "[pytest]\n",
    "pkg/__init__.py": "",
    "pkg/calc.py": "def double(x):\n    return 2 * x\n",
    "tests/test_pass.py": """
        from pkg.calc import double


        def test_double():
            assert double(2) == 4


        def test_double_zero():
            assert double(0) == 0
    """,
    "tests/test_fail.py": """
        from pkg.calc import double


        def test_double_negative():
            assert double(-1) == 2
    """,
    "tests/test_setup_error.py": """
        import pytest


        @pytest.fixture
        def broken():
            raise RuntimeError("fixture exploded")


        def test_uses_broken(broken):
            pass


        @pytest.mark.skip(reason="not today")
        def test_skipped():
            pass
    """,
    "tests/test_import_error.py": "import module_that_does_not_exist\n",
    "tests/test_slow.py": """
        import time


        def test_sleeps():
            time.sleep(60)
    """,
    "tests/test_env.py": """
        import json
        import os


        def test_dump_env():
            with open(os.environ["ENV_DUMP"], "w", encoding="utf-8") as fh:
                json.dump(dict(os.environ), fh)
    """,
    "tests/test_noisy.py": """
        def test_noisy():
            for i in range(3000):
                print(f"line {i:05d} " + "x" * 40)
            assert False, "noisy failure"
    """,
    "tests/test_spawn.py": """
        import os
        import subprocess
        import sys
        import time


        def test_spawns_grandchild():
            child = subprocess.Popen([sys.executable, "-c", "import time; time.sleep(120)"])
            with open(os.environ["PID_DUMP"], "w", encoding="utf-8") as fh:
                fh.write(str(child.pid))
            time.sleep(120)
    """,
    "tests/test_limits.py": """
        import json
        import os


        def test_dump_limits():
            import resource  # POSIX only; this test is only run there

            names = ("RLIMIT_AS", "RLIMIT_NPROC", "RLIMIT_FSIZE")
            data = {name: resource.getrlimit(getattr(resource, name))[0] for name in names}
            data["session_leader"] = os.getsid(0) == os.getpid()
            with open(os.environ["LIMITS_DUMP"], "w", encoding="utf-8") as fh:
                json.dump(data, fh)
    """,
}


@pytest.fixture(scope="module")
def project(tmp_path_factory: pytest.TempPathFactory) -> Path:
    root = tmp_path_factory.mktemp("pytest runner project")
    for rel, content in PROJECT_FILES.items():
        target = root / rel
        target.parent.mkdir(parents=True, exist_ok=True)
        target.write_text(textwrap.dedent(content).lstrip("\n"), encoding="utf-8", newline="\n")
    return root


# --------------------------------------------------------------------------- running


def test_passing_tests(project: Path) -> None:
    result = run_pytest(project, ["tests/test_pass.py"], timeout=120)
    assert result.ok
    assert (result.exit_code, result.passed, result.failed, result.errors) == (0, 2, 0, 0)
    assert result.collected == 2
    assert not result.collection_error and not result.timed_out
    assert result.summary_line.startswith("2 passed in ")
    assert result.command[:9] == [
        sys.executable,
        "-m",
        "pytest",
        "-q",
        "-p",
        "no:cacheprovider",
        "-o",
        "addopts=",
        "tests/test_pass.py",
    ]
    assert result.duration > 0
    # No bytecode or cache left behind in the workspace.
    assert not list(project.rglob("__pycache__")) and not (project / ".pytest_cache").exists()


def test_failing_tests_are_counted(project: Path) -> None:
    result = run_pytest(project, ["tests/test_pass.py", "tests/test_fail.py"], timeout=120)
    assert not result.ok
    assert (result.exit_code, result.passed, result.failed) == (1, 2, 1)
    assert result.collected == 3
    assert not result.collection_error
    assert "test_double_negative" in result.output_tail
    assert result.summary() == f"exit 1: {result.summary_line}"


def test_setup_errors_are_not_collection_errors(project: Path) -> None:
    result = run_pytest(project, ["tests/test_setup_error.py"], timeout=120)
    assert result.exit_code == 1
    assert (result.errors, result.skipped, result.collected) == (1, 1, 2)
    assert not result.collection_error


def test_import_error_is_a_collection_error(project: Path) -> None:
    result = run_pytest(project, ["tests/test_import_error.py"], timeout=120)
    assert result.exit_code == 2
    assert result.errors == 1 and result.collection_errors == 1
    assert result.collected == 0
    assert result.collection_error
    assert "module_that_does_not_exist" in result.output_tail


def test_no_tests_collected(project: Path) -> None:
    result = run_pytest(project, ["tests/test_pass.py"], timeout=120, extra_args=["-k", "nothing_matches"])
    assert result.exit_code == 5
    assert result.deselected == 2 and result.passed == 0
    assert not result.ok


def test_timeout_kills_the_run(project: Path) -> None:
    result = run_pytest(project, ["tests/test_slow.py"], timeout=3)
    assert result.timed_out
    assert result.exit_code == TIMEOUT_EXIT_CODE
    assert not result.ok and not result.collection_error
    assert result.duration < 45
    assert "timeout" in result.output_tail
    assert result.summary().startswith("timed out")


def test_environment_is_scrubbed(project: Path, tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    secrets = {
        "ANTHROPIC_API_KEY": "sk-ant-secret",
        "GITHUB_TOKEN": "ghp_secret",
        "HEALER_INGEST_TOKEN": "ingest-secret",
        "HEALER_DATABASE_URL": "postgresql://u:p@db/healer",
        "AIRFLOW__DATABASE__SQL_ALCHEMY_CONN": "postgresql://u:p@db/airflow",
        "AIRFLOW__API_AUTH__JWT_SECRET": "jwt-secret",
        "AIRFLOW__CORE__FERNET_KEY": "fernet",
        "AIRFLOW__SMTP__SMTP_PASSWORD": "smtp",
        "AIRFLOW_CONN_WAREHOUSE": "postgresql://u:p@warehouse/db",
        "SMTP_PASSWORD": "smtp",
        "MY_SERVICE_SECRET": "s3cr3t",
        "PYTHONPATH": str(tmp_path / "elsewhere"),
        "PYTEST_ADDOPTS": "--this-option-does-not-exist",
    }
    for key, value in secrets.items():
        monkeypatch.setenv(key, value)
    monkeypatch.setenv("HARMLESS_SETTING", "kept")
    dump = tmp_path / "env.json"

    result = run_pytest(project, ["tests/test_env.py"], timeout=120, extra_env={"ENV_DUMP": str(dump)})
    assert result.ok, result.output_tail
    env = json.loads(dump.read_text(encoding="utf-8"))
    upper = {key.upper() for key in env}
    for key in secrets:
        if key != "AIRFLOW__DATABASE__SQL_ALCHEMY_CONN":  # replaced by the SQLite URL below
            assert key not in upper, key
    leaked = [value for value in env.values() for secret in secrets.values() if secret in value]
    assert leaked == []
    assert env["HARMLESS_SETTING"] == "kept"
    assert env["AIRFLOW__CORE__UNIT_TEST_MODE"] == "True"
    assert env["PYTHONDONTWRITEBYTECODE"] == "1"
    home = Path(env["AIRFLOW_HOME"])
    # The URL holds the resolved path (on macOS the temporary directory lives behind /var -> /private/var).
    database = (home / "unittests.db").resolve()
    assert env["AIRFLOW__DATABASE__SQL_ALCHEMY_CONN"] == f"sqlite:///{database.as_posix()}"
    assert not home.exists()  # the throw-away AIRFLOW_HOME is removed after the run


#: Cloud credentials a worker may run with (an AWS task role, a GCP key file, an Azure secret).
CLOUD_CREDENTIALS = {
    "AWS_ACCESS_KEY_ID": "AKIAEXAMPLEKEYID0001",
    "AWS_SECRET_ACCESS_KEY": "example-secret-access-key",
    "AWS_SESSION_TOKEN": "example-session-token",
    "AWS_SECURITY_TOKEN": "example-security-token",
    "AWS_PROFILE": "example-admin-profile",
    "AWS_ROLE_ARN": "arn:aws:iam::123456789012:role/example-healer",
    "AWS_WEB_IDENTITY_TOKEN_FILE": "/var/run/secrets/example/web-identity-token",
    "AWS_CONTAINER_CREDENTIALS_RELATIVE_URI": "/v2/credentials/example-id",
    "AWS_CONTAINER_CREDENTIALS_FULL_URI": "http://169.254.170.23/v1/example-credentials",
    "AWS_CONTAINER_AUTHORIZATION_TOKEN": "example-container-token",
    "AWS_CONTAINER_AUTHORIZATION_TOKEN_FILE": "/var/run/secrets/example/container-token",
    "GOOGLE_APPLICATION_CREDENTIALS": "/secrets/example-gcp-key.json",
    "AZURE_CLIENT_SECRET": "example-azure-client-secret",
}


def test_cloud_credentials_never_reach_the_sandbox(
    project: Path, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    for key, value in CLOUD_CREDENTIALS.items():
        monkeypatch.setenv(key, value)
    monkeypatch.setenv("AWS_EC2_METADATA_DISABLED", "false")
    monkeypatch.setenv("AWS_SHARED_CREDENTIALS_FILE", "/home/airflow/.aws/credentials")
    monkeypatch.setenv("AWS_CONFIG_FILE", "/home/airflow/.aws/config")
    monkeypatch.setenv("AWS_REGION", "eu-west-1")
    dump = tmp_path / "env.json"
    # an orchestrator override can neither bring a credential back nor re-enable the metadata service
    overrides: dict[str, str | None] = {
        "ENV_DUMP": str(dump),
        "AWS_ACCESS_KEY_ID": "AKIAEXAMPLEOVERRIDE1",
        "aws_secret_access_key": "example-lower-case-override",
        "AWS_EC2_METADATA_DISABLED": "false",
        "AWS_CONFIG_FILE": "/home/airflow/.aws/override",
        "FAKEFLOW_TOKEN": None,
    }

    result = run_pytest(project, ["tests/test_env.py"], timeout=120, extra_env=overrides)
    assert result.ok, result.output_tail
    env = json.loads(dump.read_text(encoding="utf-8"))
    upper = {key.upper() for key in env}
    assert not upper & set(CLOUD_CREDENTIALS)
    assert not [key for key in upper if key.startswith(("AWS_CONTAINER_CREDENTIALS_", "AWS_CONTAINER_AUTH"))]
    secrets = [*CLOUD_CREDENTIALS.values(), "AKIAEXAMPLEOVERRIDE1", "example-lower-case-override"]
    assert [value for value in env.values() for secret in secrets if secret in value] == []
    assert env["AWS_EC2_METADATA_DISABLED"] == "true"
    assert (env["AWS_SHARED_CREDENTIALS_FILE"], env["AWS_CONFIG_FILE"]) == ("", "")
    assert env["AWS_REGION"] == "eu-west-1"  # not a credential
    # the Airflow keys of the sandbox are unchanged
    assert (env["AIRFLOW__CORE__UNIT_TEST_MODE"], env["AIRFLOW__CORE__LOAD_EXAMPLES"]) == ("True", "False")
    assert env["AIRFLOW__DATABASE__SQL_ALCHEMY_CONN"].startswith("sqlite:///")
    assert Path(env["AIRFLOW_HOME"]).name == "airflow_home"


def test_output_tail_is_truncated(project: Path) -> None:
    result = run_pytest(project, ["tests/test_noisy.py"], timeout=120, extra_args=["-s"])
    assert result.failed == 1
    assert len(result.output_tail) <= MAX_OUTPUT_CHARS
    assert result.output_tail.startswith("...[output truncated: ")
    assert "line 02999" in result.output_tail and "line 00000" not in result.output_tail
    assert result.summary_line.startswith("1 failed in ")


def _process_gone(pid: int) -> bool:
    """True when ``pid`` no longer runs (zombies waiting for a reaper count as gone)."""
    try:
        os.kill(pid, 0)
    except ProcessLookupError:
        return True
    try:
        status = Path(f"/proc/{pid}/status").read_text(encoding="utf-8")
    except OSError:
        return True
    return "\nState:\tZ" in status


@pytest.mark.skipif(not sys.platform.startswith("linux"), reason="process-group check uses /proc")
def test_timeout_kills_the_whole_process_group(project: Path, tmp_path: Path) -> None:
    pid_file = tmp_path / "grandchild.pid"
    result = run_pytest(project, ["tests/test_spawn.py"], timeout=4, extra_env={"PID_DUMP": str(pid_file)})
    assert result.timed_out
    pid = int(pid_file.read_text(encoding="utf-8"))
    deadline = time.monotonic() + 10
    while not _process_gone(pid) and time.monotonic() < deadline:
        time.sleep(0.2)
    assert _process_gone(pid)


@pytest.mark.skipif(os.name != "posix", reason="resource limits and sessions are POSIX-only")
def test_posix_resource_limits_and_new_session(project: Path, tmp_path: Path) -> None:
    from dag_healer.repo import pytest_runner

    dump = tmp_path / "limits.json"
    result = run_pytest(project, ["tests/test_limits.py"], timeout=120, extra_env={"LIMITS_DUMP": str(dump)})
    assert result.ok, result.output_tail
    data = json.loads(dump.read_text(encoding="utf-8"))
    assert data["session_leader"] is True
    if sys.platform == "darwin":
        assert data["RLIMIT_AS"] > 0  # macOS refuses to lower RLIMIT_AS, so the runner skips it there
    else:
        assert 0 < data["RLIMIT_AS"] <= pytest_runner.RLIMIT_AS_BYTES
    assert 0 < data["RLIMIT_NPROC"] <= pytest_runner.RLIMIT_NPROC_COUNT
    assert 0 < data["RLIMIT_FSIZE"] <= pytest_runner.RLIMIT_FSIZE_BYTES


def test_missing_interpreter_is_reported_not_raised(project: Path) -> None:
    result = run_pytest(project, ["tests/test_pass.py"], python=str(project / "no-such-python"), timeout=30)
    assert result.exit_code == LAUNCH_FAILED_EXIT_CODE
    assert result.collection_error and not result.ok
    assert "could not start pytest" in result.output_tail
    assert result.launch_error is not None and "no-such-python" in result.launch_error
    assert "could not run pytest" in result.summary()


def test_missing_workdir_raises(tmp_path: Path) -> None:
    with pytest.raises(FileNotFoundError):
        run_pytest(tmp_path / "missing")


VANDAL_TEST = """
    import os
    import pathlib


    def test_vandal():
        root = pathlib.Path.cwd()
        assert not (root / ".git").exists()  # the run sees a copy without the .git folder
        (root / "created.txt").write_text("x", encoding="utf-8")
        (root / "pkg" / "calc.py").write_text("def double(x):\\n    return 0\\n", encoding="utf-8")
        os.remove(root / "README.md")
        hooks = root / ".git" / "hooks"
        hooks.mkdir(parents=True)
        (hooks / "pre-commit").write_text("#!/bin/sh\\necho planted\\n", encoding="utf-8")
"""


def _tree(root: Path) -> dict[str, bytes]:
    return {
        path.relative_to(root).as_posix(): path.read_bytes() for path in root.rglob("*") if path.is_file()
    }


def test_a_run_never_changes_the_workdir_nor_sees_its_git_folder(tmp_path: Path) -> None:
    root = tmp_path / "clone"
    files = {
        "pytest.ini": "[pytest]\n",
        "README.md": "readme\n",
        "pkg/__init__.py": "",
        "pkg/calc.py": PROJECT_FILES["pkg/calc.py"],
        "tests/test_vandal.py": VANDAL_TEST,
        ".git/HEAD": "ref: refs/heads/main\n",
        ".git/config": "[core]\n\tbare = false\n",
    }
    for rel, content in files.items():
        target = root / rel
        target.parent.mkdir(parents=True, exist_ok=True)
        target.write_text(textwrap.dedent(content).lstrip("\n"), encoding="utf-8", newline="\n")
    before = _tree(root)

    result = run_pytest(root, ["tests/test_vandal.py"], timeout=120)

    assert result.ok, result.output_tail
    assert _tree(root) == before
    assert not (root / "created.txt").exists() and not (root / ".git" / "hooks").exists()


class StubSandbox:
    """A sandbox that records its jobs and answers with a prepared run (and output)."""

    name = "stub"
    isolated = True

    def __init__(self, run: SandboxRun, output: str = "") -> None:
        self.result = run
        self.output = output
        self.jobs: list[SandboxJob] = []

    def unavailable_reason(self) -> str | None:
        return None

    def smoke_test(self) -> SandboxProbe:
        return SandboxProbe(True, "stub")

    def run(self, job: SandboxJob) -> SandboxRun:
        self.jobs.append(job)
        job.output.write_text(self.output, encoding="utf-8")
        return self.result


def test_a_launch_failure_is_reported_never_as_a_test_result(
    project: Path, caplog: pytest.LogCaptureFixture
) -> None:
    error = "docker cannot reach its engine (no socket at 10.0.0.12); start it"
    stub = StubSandbox(SandboxRun(LAUNCH_FAILED_EXIT_CODE, launch_error=error), output="1 passed in 0.01s\n")
    assert isinstance(stub, Sandbox)

    with caplog.at_level(logging.WARNING, logger="dag_healer.repo.pytest_runner"):
        result = run_pytest(project, ["tests/test_pass.py"], timeout=30, sandbox=stub)

    assert (result.exit_code, result.launch_error) == (LAUNCH_FAILED_EXIT_CODE, error)
    assert result.collection_error and not result.ok and not result.timed_out
    assert (result.passed, result.failed, result.collected) == (0, 0, 0)  # the output is no test result
    assert result.output_tail == f"could not start pytest: {error}"
    assert result.summary() == f"exit 3: the sandbox could not run pytest ({error})"
    assert result.to_dict()["launch_error"] == error
    assert "(stub sandbox)" in caplog.text and "10.0.0.12" not in caplog.text  # the log line is masked


def test_run_pytest_hands_the_sandbox_its_job(project: Path) -> None:
    command = ("python", "-m", "pytest", "-q")
    stub = StubSandbox(SandboxRun(1, command=command), output="F.\n1 failed, 1 passed in 0.10s\n")

    result = run_pytest(
        project,
        ["tests/test_pass.py"],
        timeout=42,
        python="python3",
        extra_args=["-x"],
        extra_env={"ENV_DUMP": "dump.json", "AWS_ACCESS_KEY_ID": "AKIAEXAMPLEOVERRIDE1"},
        sandbox=stub,
    )

    assert (result.exit_code, result.failed, result.passed) == (1, 1, 1)
    assert result.command == list(command)
    (job,) = stub.jobs
    assert job.workdir == project and job.timeout == 42 and job.python == "python3"
    assert job.args == ("-q", "-p", "no:cacheprovider", "-o", "addopts=", "-x", "tests/test_pass.py")
    assert job.env["ENV_DUMP"] == "dump.json" and "AWS_ACCESS_KEY_ID" not in job.env
    assert job.env["AIRFLOW_HOME"] == str(job.scratch / "airflow_home")
    assert job.output.parent == job.scratch and not job.scratch.exists()  # removed after the run


def test_a_timeout_reported_by_the_sandbox(project: Path) -> None:
    stub = StubSandbox(SandboxRun(TIMEOUT_EXIT_CODE, timed_out=True), output="..")
    result = run_pytest(project, ["tests/test_slow.py"], timeout=7, sandbox=stub)
    assert result.timed_out and result.exit_code == TIMEOUT_EXIT_CODE
    assert "killed after exceeding the 7 s timeout" in result.output_tail


def test_the_log_line_names_the_sandbox(project: Path, caplog: pytest.LogCaptureFixture) -> None:
    with caplog.at_level(logging.INFO, logger="dag_healer.repo.pytest_runner"):
        result = run_pytest(project, ["tests/test_pass.py"], timeout=120)
    assert result.ok
    assert f"pytest tests/test_pass.py in {project} (process sandbox) -> exit 0: 2 passed" in caplog.text


def test_the_runner_still_exposes_the_sandbox_constants() -> None:
    from dag_healer.repo import pytest_runner

    assert pytest_runner.TIMEOUT_EXIT_CODE == sandbox_module.TIMEOUT_EXIT_CODE == 124
    assert pytest_runner.LAUNCH_FAILED_EXIT_CODE == sandbox_module.LAUNCH_FAILED_EXIT_CODE == 3
    for name in ("RLIMIT_AS_BYTES", "RLIMIT_NPROC_COUNT", "RLIMIT_FSIZE_BYTES"):
        assert getattr(pytest_runner, name) == getattr(sandbox_module, name)


# --------------------------------------------------------------------------- pure helpers


def test_scrubbed_env_from_explicit_base(tmp_path: Path) -> None:
    base = {
        "PATH": "/usr/bin",
        "HOME": "/home/airflow",
        "GITHUB_TOKEN": "x",
        "healer_debug": "1",
        "AIRFLOW__CORE__DAGS_FOLDER": "/opt/airflow/repo/dags",
        "AIRFLOW_CONFIG": "/opt/airflow/airflow.cfg",
        "SSH_AUTH_SOCK": "/tmp/agent.sock",
        "CI": "true",
        "BUILD_NUMBER": "42",
    }
    env = scrubbed_env(base, airflow_home=tmp_path / "home")
    assert env["PATH"] == "/usr/bin" and env["HOME"] == "/home/airflow"
    assert env["AIRFLOW__CORE__DAGS_FOLDER"] == "/opt/airflow/repo/dags"
    for key in ("GITHUB_TOKEN", "healer_debug", "AIRFLOW_CONFIG", "SSH_AUTH_SOCK", "CI", "BUILD_NUMBER"):
        assert key not in env
    assert env["AIRFLOW_HOME"] == str(tmp_path / "home")
    assert (tmp_path / "home").is_dir()
    assert env["AIRFLOW__CORE__LOAD_EXAMPLES"] == "False"


def test_scrubbed_env_removes_cloud_credentials(tmp_path: Path) -> None:
    base = {
        "PATH": "/usr/bin",
        "AWS_REGION": "eu-west-1",
        "AWS_ACCESS_KEY_ID": "AKIAEXAMPLEKEYID0001",
        "AWS_PROFILE": "example-admin-profile",
        "AWS_ROLE_ARN": "arn:aws:iam::123456789012:role/example-healer",
        "aws_container_credentials_full_uri": "http://169.254.170.23/v1/example-credentials",
        "Aws_Config_File": "/home/airflow/.aws/config",
        "AWS_EC2_METADATA_DISABLED": "false",
    }
    env = scrubbed_env(base, airflow_home=tmp_path / "home")
    removed = {"AWS_ACCESS_KEY_ID", "AWS_PROFILE", "AWS_ROLE_ARN", "AWS_CONTAINER_CREDENTIALS_FULL_URI"}
    assert not {key.upper() for key in env} & removed
    assert "Aws_Config_File" not in env  # a differently cased copy is replaced, not kept beside it
    assert {name: env[name] for name in CLOUD_SANDBOX_ENV} == {
        "AWS_EC2_METADATA_DISABLED": "true",
        "AWS_SHARED_CREDENTIALS_FILE": "",
        "AWS_CONFIG_FILE": "",
    }
    assert (env["PATH"], env["AWS_REGION"]) == ("/usr/bin", "eu-west-1")
    assert env["AIRFLOW__CORE__UNIT_TEST_MODE"] == "True"


@pytest.mark.parametrize(
    ("output", "expected"),
    [
        (
            ".FEsx\n1 failed, 1 passed, 1 skipped, 1 xfailed, 1 error in 0.08s\n",
            {"passed": 1, "failed": 1, "skipped": 1, "xfailed": 1, "errors": 1, "collected": 5},
        ),
        (
            "==================== 3 passed, 2 warnings in 1.20s ====================\n",
            {"passed": 3, "warnings": 2, "collected": 3},
        ),
        (
            "ERROR tests/test_b.py\n!!!!! Interrupted: 2 errors during collection !!!!!\n2 errors in 0.13s\n",
            {"errors": 2, "collection_errors": 2, "collected": 0},
        ),
        ("\n5 deselected in 0.01s\n", {"deselected": 5, "collected": 5}),
        ("no tests ran in 0.01s\n", {"collected": 0, "summary_line": "no tests ran in 0.01s"}),
        ("1 passed, 1 xpassed in 61.23s (0:01:01)\n", {"passed": 1, "xpassed": 1, "collected": 2}),
        ("garbage only\n", {"collected": 0, "summary_line": ""}),
    ],
)
def test_parse_summary(output: str, expected: dict[str, object]) -> None:
    parsed = parse_summary(output)
    for key, value in expected.items():
        assert parsed[key] == value, key


def test_truncate_tail() -> None:
    assert truncate_tail("short", 100) == "short"
    text = "".join(f"{i:04d}\n" for i in range(5000))
    tail = truncate_tail(text, 1000)
    assert len(tail) <= 1000
    assert tail.startswith("...[output truncated: ")
    assert tail.endswith("4999\n")


def test_result_properties() -> None:
    assert PytestResult(exit_code=0, passed=1, collected=1).ok
    assert PytestResult(exit_code=4).collection_error
    assert PytestResult(exit_code=1, errors=1, collected=0).collection_error
    assert not PytestResult(exit_code=1, failed=1, collected=1).collection_error
    data = PytestResult(exit_code=1, failed=1, collected=1, command=["python"]).to_dict()
    assert data["ok"] is False and data["collection_error"] is False and data["command"] == ["python"]
    assert data["launch_error"] is None
    launch = PytestResult(exit_code=LAUNCH_FAILED_EXIT_CODE, launch_error="the image x is not present")
    assert launch.collection_error and launch.summary() == (
        "exit 3: the sandbox could not run pytest (the image x is not present)"
    )
