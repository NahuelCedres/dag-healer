"""Tests for the test sandbox (dag_healer.repo.sandbox), with a fake container engine.

No test here reaches a real docker or podman: the container backend runs a fake engine CLI
(``[sys.executable, fake_engine.py, <state dir>]``) that records every call, the ``-e`` values it
receives and its own environment, and answers from a JSON scenario. Its ``run`` modes:

* ``script``: the bootstrap's start marker, then scripted output and exit code;
* ``fail``: only an engine error on stderr (no marker), like docker before a container starts;
* ``sleep``: the marker, then sleeps (the timeout must kill the container by name);
* ``emulate``: the real :data:`BOOTSTRAP` with this interpreter, the container paths rewritten
  to folders of the state directory, the workspace tar on its stdin.

The real-engine counterparts are in ``test_sandbox_engine.py`` (opt-in).
"""

from __future__ import annotations

import io
import json
import logging
import os
import re
import socket
import stat
import subprocess
import sys
import tarfile
import textwrap
import time
import uuid
from pathlib import Path
from types import SimpleNamespace
from typing import Any

import pytest

from dag_healer.repo import sandbox as sandbox_module
from dag_healer.repo.pytest_runner import run_pytest, scrubbed_env
from dag_healer.repo.sandbox import (
    BOOTSTRAP,
    BOOTSTRAP_FAILED_EXIT_CODE,
    BOOTSTRAP_MARKER,
    CONTAINER_LABEL,
    ENGINE_ENV,
    KILLED_EXIT_CODE,
    LAUNCH_FAILED_EXIT_CODE,
    PROCESS_BACKEND_WARNING,
    SANDBOX_BACKENDS,
    SANDBOX_ENGINES,
    TIMEOUT_EXIT_CODE,
    ContainerSandbox,
    ProcessSandbox,
    Sandbox,
    SandboxJob,
    build_sandbox,
    container_environment,
    copy_workspace,
    engine_environment,
    sandbox_launch_failure,
    sandbox_refusal,
    write_workspace_tar,
)

POSIX = os.name == "posix"
IS_ROOT = hasattr(os, "geteuid") and os.geteuid() == 0
IMAGE = "dag-healer/sandbox-test:dev"
PYTEST_ARGS = ("-q", "-p", "no:cacheprovider", "-o", "addopts=")

FAKE_ENGINE = r'''
"""Fake engine CLI of the sandbox tests: records every call and answers from scenario.json."""
import json
import os
import subprocess
import sys
import time
from pathlib import Path

MARKER = @MARKER@
VALUE_FLAGS = {
    "--name", "--label", "--pull", "--network", "--tmpfs", "--cap-drop", "--security-opt",
    "--pids-limit", "--memory", "--memory-swap", "--shm-size", "--log-driver", "--cpus", "--user",
    "--entrypoint", "-e",
}
#: What an image's own environment gives the interpreter of an emulated container.
IMAGE_ENV = ("PATH", "SYSTEMROOT", "SYSTEMDRIVE", "WINDIR", "COMSPEC", "PATHEXT", "TEMP", "TMP")

state = Path(sys.argv[1])
args = sys.argv[2:]
scenario = json.loads((state / "scenario.json").read_text(encoding="utf-8"))


def record(**extra):
    entry = {"args": args, "env_keys": sorted(os.environ), "docker_host": os.environ.get("DOCKER_HOST")}
    entry.update(extra)
    calls = state / "calls"
    calls.mkdir(exist_ok=True)
    name = "%020d-%d.json" % (time.time_ns(), os.getpid())
    (calls / name).write_text(json.dumps(entry), encoding="utf-8")


def answer(spec, marker=False):
    if marker:
        sys.stderr.write(MARKER + "\n")
    sys.stdout.write(spec.get("stdout", ""))
    sys.stderr.write(spec.get("stderr", ""))
    sys.stdout.flush()
    sys.stderr.flush()
    time.sleep(spec.get("sleep", 0))
    sys.exit(spec.get("code", 0))


def parse_run(rest):
    names, index = [], 0
    while rest[index].startswith("-"):
        if rest[index] in VALUE_FLAGS:
            if rest[index] == "-e":
                names.append(rest[index + 1])
            index += 2
        else:
            index += 1
    return names, rest[index], rest[index + 1 :]


def emulate(names, command):
    run = state / "runs" / str(time.time_ns())
    work, tmp = run / "work", run / "tmp"
    work.mkdir(parents=True)
    tmp.mkdir()
    paths = {"/work": str(work), "/tmp/home": str(tmp / "home"), "/tmp": str(tmp)}

    def rewrite(value):
        if value in paths:
            return paths[value]
        if value.startswith("sqlite:////tmp/"):
            return "sqlite:///" + (tmp / value[len("sqlite:////tmp/") :]).as_posix()
        if value.startswith("/tmp/"):
            return str(tmp / value[len("/tmp/") :])
        return value

    env = {key: value for key, value in os.environ.items() if key.upper() in IMAGE_ENV}
    env.update({name: rewrite(os.environ[name]) for name in names if name in os.environ})
    (state / "last-run").write_text(str(run), encoding="utf-8")
    sys.exit(subprocess.call([sys.executable] + [rewrite(part) for part in command], env=env))


command = args[0] if args else ""
if command == "run":
    names, image, rest = parse_run(args[1:])
    record(env={name: os.environ.get(name) for name in names}, image=image, command=rest)
    spec = scenario.get("run", {})
    mode = spec.get("mode", "script")
    if mode == "emulate":
        emulate(names, rest)
    if mode == "sleep":
        answer({"sleep": 60}, marker=True)
    if spec.get("read_stdin"):
        sys.stdin.buffer.read()
    answer(spec, marker=mode == "script")
record()
if command == "version":
    answer(scenario.get("version", {"stdout": "29.8.2\n"}))
if command == "image":
    answer(scenario.get("inspect", {"stdout": '"sandbox"\n'}))
if command == "ps":
    answer(scenario.get("ps", {}))
answer(scenario.get(command, {}))
'''.replace("@MARKER@", repr(BOOTSTRAP_MARKER))


class FakeEngine:
    """The fake engine CLI of one test: its command, scenario and recorded calls."""

    def __init__(self, state: Path) -> None:
        state.mkdir(parents=True)
        self.state = state
        self.script = state / "fake_engine.py"
        self.script.write_text(FAKE_ENGINE, encoding="utf-8")
        self.scenario()

    @property
    def command(self) -> list[str]:
        return [sys.executable, str(self.script), str(self.state)]

    def scenario(self, **parts: Any) -> None:
        (self.state / "scenario.json").write_text(json.dumps(parts), encoding="utf-8")

    def calls(self) -> list[dict[str, Any]]:
        folder = self.state / "calls"
        return [json.loads(path.read_text(encoding="utf-8")) for path in sorted(folder.glob("*.json"))]

    def commands(self) -> list[list[str]]:
        return [call["args"] for call in self.calls()]

    def runs(self) -> list[dict[str, Any]]:
        return [call for call in self.calls() if call["args"][:1] == ["run"]]

    def last_run_dir(self) -> Path:
        return Path((self.state / "last-run").read_text(encoding="utf-8"))

    def sandbox(self, **settings: Any) -> ContainerSandbox:
        return ContainerSandbox(make_settings(**settings), command=self.command)


@pytest.fixture
def engine(tmp_path: Path) -> FakeEngine:
    return FakeEngine(tmp_path / "engine")


def make_settings(**overrides: Any) -> SimpleNamespace:
    """A ``repo.sandbox`` settings object (duck-typed like config.SandboxConfig)."""
    values: dict[str, Any] = {
        "backend": "container",
        "engine": "docker",
        "image": IMAGE,
        "python": "python",
        "user": None,
        "memory": "2g",
        "cpus": 2.0,
        "pids": 512,
        "work_size": "1g",
        "tmp_size": "512m",
    }
    values.update(overrides)
    return SimpleNamespace(**values)


def make_job(
    workdir: Path,
    tmp_path: Path,
    *targets: str,
    env: dict[str, str] | None = None,
    timeout: float = 120,
    python: str | None = None,
) -> SandboxJob:
    scratch = tmp_path / f"scratch-{uuid.uuid4().hex[:8]}"
    scratch.mkdir()
    return SandboxJob(
        workdir=workdir,
        args=(*PYTEST_ARGS, *targets),
        env=dict(env if env is not None else {"PYTHONDONTWRITEBYTECODE": "1"}),
        timeout=timeout,
        output=scratch / "pytest-output.log",
        scratch=scratch,
        python=python,
    )


def write_files(root: Path, files: dict[str, str]) -> Path:
    for rel, content in files.items():
        target = root / rel
        target.parent.mkdir(parents=True, exist_ok=True)
        target.write_text(textwrap.dedent(content).lstrip("\n"), encoding="utf-8", newline="\n")
    return root


def symlink(target: str, path: Path, *, directory: bool = False) -> bool:
    """Create a symbolic link; False where the platform refuses (Windows without the privilege)."""
    try:
        os.symlink(target, path, target_is_directory=directory)
    except (OSError, NotImplementedError):
        return False
    return True


def _link_target(path: Path) -> str:
    """A link's target with forward slashes (Windows may report backslashes)."""
    return os.readlink(path).replace("\\", "/")


def snapshot(root: Path) -> dict[str, tuple[str, bytes]]:
    """Every entry under ``root`` (links not followed): kind and content."""
    found: dict[str, tuple[str, bytes]] = {}
    for dirpath, dirnames, filenames in os.walk(root):
        for name in [*dirnames, *filenames]:
            path = Path(dirpath) / name
            rel = path.relative_to(root).as_posix()
            mode = os.lstat(path).st_mode
            if stat.S_ISLNK(mode):
                found[rel] = ("link", _link_target(path).encode())
            elif stat.S_ISDIR(mode):
                found[rel] = ("dir", b"")
            elif stat.S_ISREG(mode):
                found[rel] = ("file", path.read_bytes())
            else:
                found[rel] = ("other", b"")
    return found


PROJECT = {
    "pytest.ini": "[pytest]\n",
    "README.md": "readme\n",
    "pkg/__init__.py": "",
    "pkg/calc.py": "def double(x):\n    return 2 * x\n",
    "pkg/data.txt": "data\n",
    "tests/test_ok.py": """
        from pkg.calc import double


        def test_double():
            assert double(2) == 4
    """,
    "tests/test_bad.py": """
        from pkg.calc import double


        def test_double_negative():
            assert double(-1) == 2
    """,
    ".git/config": "[core]\n\tbare = false\n",
    ".git/HEAD": "ref: refs/heads/main\n",
    ".git/hooks/pre-commit": "#!/bin/sh\necho clone hook\n",
}


@pytest.fixture
def project(tmp_path: Path) -> Path:
    return write_files(tmp_path / "clone", PROJECT)


# =========================================================================== choosing a backend


def test_build_sandbox_maps_only_the_configured_backend() -> None:
    assert SANDBOX_BACKENDS == ("container", "process")
    assert SANDBOX_ENGINES == ("docker", "podman")
    process = build_sandbox(make_settings(backend="process"))
    container = build_sandbox(make_settings(backend="container", image=None))
    assert isinstance(process, ProcessSandbox) and (process.name, process.isolated) == ("process", False)
    assert isinstance(container, ContainerSandbox) and (container.name, container.isolated) == (
        "container",
        True,
    )
    assert isinstance(process, Sandbox) and isinstance(container, Sandbox)
    for backend in ("vm", "", None):
        with pytest.raises(ValueError, match="repo.sandbox.backend"):
            build_sandbox(make_settings(backend=backend))
    with pytest.raises(ValueError):
        build_sandbox(SimpleNamespace())


def test_user_facing_texts() -> None:
    assert "without isolation" in PROCESS_BACKEND_WARNING and "section 4.3" in PROCESS_BACKEND_WARNING
    refusal = sandbox_refusal("repo.sandbox.image is not set; set it")
    assert refusal.startswith("The test sandbox is not usable: repo.sandbox.image is not set; set it. ")
    assert "No code was run" in refusal and refusal.endswith("then retry the repair.")
    failure = sandbox_launch_failure("docker exited with code 1")
    assert failure.startswith("The test sandbox could not run pytest: docker exited with code 1. ")
    assert failure.endswith("then retry the repair.")


# =========================================================================== the workspace copy


@pytest.fixture
def tree(tmp_path: Path) -> dict[str, Any]:
    """A clone with .git folders at several depths, links of every kind and an outside secret."""
    outside = tmp_path / "outside"
    write_files(outside, {"secret.txt": "SECRET-OUTSIDE-THE-TREE\n"})
    root = write_files(
        tmp_path / "tree",
        {
            "top.txt": "top\n",
            "pkg/data.txt": "data\n",
            "pkg/run.sh": "#!/bin/sh\necho run\n",
            "pkg/deep/inner.py": "x = 1\n",
            "pkg/sub/mod.py": "y = 2\n",
            ".git/config": "[core]\n",
            "pkg/.git/config": "[core]\n",
            "pkg/deep/.GIT/config": "[core]\n",
            "pkg/sub/.git": "gitdir: ../../.git/modules/sub\n",
        },
    )
    if POSIX:
        (root / "pkg" / "run.sh").chmod(0o755)
    links = (
        symlink("pkg/data.txt", root / "inside_link")
        and symlink("pkg", root / "linked_dir", directory=True)
        and symlink(str(outside / "secret.txt"), root / "absolute_link")
        and symlink("../outside/secret.txt", root / "escaping_link")
        and symlink("../../outside", root / "pkg" / "escaping_dir", directory=True)
    )
    if POSIX:
        os.mkfifo(root / "pkg" / "a_fifo")
    return {"root": root, "links": links, "before": snapshot(root)}


def test_copy_workspace_leaves_out_git_folders_and_unsafe_links(tree: dict[str, Any], tmp_path: Path) -> None:
    root = tree["root"]
    dest = tmp_path / "copy"
    copy_workspace(root, dest)
    copied = snapshot(dest)
    assert {rel for rel, (kind, _) in copied.items() if kind == "file"} == {
        "top.txt",
        "pkg/data.txt",
        "pkg/run.sh",
        "pkg/deep/inner.py",
        "pkg/sub/mod.py",
    }
    assert not [rel for rel in copied if ".git" in rel.lower().split("/")]
    assert not any(b"SECRET-OUTSIDE" in content for _, content in copied.values())
    if tree["links"]:
        assert copied["inside_link"] == ("link", b"pkg/data.txt")
        assert copied["linked_dir"] == ("link", b"pkg")  # kept as a link, never entered
        assert not {"absolute_link", "escaping_link", "pkg/escaping_dir"} & set(copied)
    if POSIX:
        assert "pkg/a_fifo" not in copied
        assert stat.S_IMODE(os.stat(dest / "pkg" / "run.sh").st_mode) == 0o755
        assert stat.S_IMODE(os.stat(dest / "top.txt").st_mode) == 0o644
    assert snapshot(root) == tree["before"]  # the source is never modified
    with pytest.raises(OSError):
        copy_workspace(root, dest)  # the destination must be empty


def test_workspace_tar_has_relative_names_root_owner_and_links_as_links(tree: dict[str, Any]) -> None:
    root = tree["root"]
    buffer = io.BytesIO()
    write_workspace_tar(root, buffer)
    buffer.seek(0)
    with tarfile.open(fileobj=buffer, mode="r:") as archive:
        members = {member.name: member for member in archive.getmembers()}
        contents = {
            name: archive.extractfile(member).read()  # type: ignore[union-attr]
            for name, member in members.items()
            if member.isreg()
        }
    assert set(contents) == {"top.txt", "pkg/data.txt", "pkg/run.sh", "pkg/deep/inner.py", "pkg/sub/mod.py"}
    assert {name for name, member in members.items() if member.isdir()} == {"pkg", "pkg/deep", "pkg/sub"}
    assert not [name for name in members if ".git" in name.lower().split("/")]
    assert not [name for name in members if name.startswith("/") or ".." in name.split("/")]
    assert all((m.uid, m.gid, m.uname, m.gname) == (0, 0, "", "") for m in members.values())
    assert not any(b"SECRET-OUTSIDE" in content for content in contents.values())
    if tree["links"]:
        assert members["inside_link"].issym() and members["inside_link"].linkname == "pkg/data.txt"
        assert members["linked_dir"].issym() and members["linked_dir"].linkname == "pkg"
        assert not {"absolute_link", "escaping_link", "pkg/escaping_dir"} & set(members)
    if POSIX:
        assert members["pkg/run.sh"].mode == 0o755 and members["top.txt"].mode == 0o644
        assert "pkg/a_fifo" not in members
    assert snapshot(root) == tree["before"]


# =========================================================================== the process backend

VANDAL_TEST = """
    import os
    import pathlib


    def test_vandal():
        root = pathlib.Path.cwd()
        assert not (root / ".git").exists()  # the copy has no .git folder
        (root / "created.txt").write_text("x", encoding="utf-8")
        (root / "pkg" / "calc.py").write_text("def double(x):\\n    return 0\\n", encoding="utf-8")
        os.remove(root / "README.md")
        hooks = root / ".git" / "hooks"
        hooks.mkdir(parents=True)
        (hooks / "pre-commit").write_text("#!/bin/sh\\necho planted\\n", encoding="utf-8")
"""


def test_process_sandbox_runs_in_a_throw_away_copy(project: Path, tmp_path: Path) -> None:
    write_files(project, {"tests/test_vandal.py": VANDAL_TEST})
    before = snapshot(project)
    sandbox = ProcessSandbox()
    assert sandbox.unavailable_reason() is None
    assert sandbox.smoke_test().ok
    env = scrubbed_env(airflow_home=tmp_path / "airflow_home")
    job = make_job(project, tmp_path, "tests/test_vandal.py", env=env)

    run = sandbox.run(job)

    output = job.output.read_text(encoding="utf-8")
    assert (run.exit_code, run.timed_out, run.launch_error) == (0, False, None), output
    assert run.command == (sys.executable, "-m", "pytest", *PYTEST_ARGS, "tests/test_vandal.py")
    assert snapshot(project) == before  # the clone, its .git folder and its hooks are untouched
    assert not [path for path in job.scratch.iterdir() if path.name.startswith("work-")]  # copy removed


def test_process_sandbox_reports_a_missing_interpreter(project: Path, tmp_path: Path) -> None:
    job = make_job(project, tmp_path, "tests/test_ok.py", python=str(tmp_path / "no-such-python"))
    run = ProcessSandbox().run(job)
    assert run.exit_code == LAUNCH_FAILED_EXIT_CODE
    assert run.launch_error is not None and run.launch_error.startswith("could not start pytest (")


# =========================================================================== container: the command line


def _pairs(argv: list[str]) -> list[tuple[str, str]]:
    return list(zip(argv, argv[1:], strict=False))


def test_container_argv_has_every_isolation_flag_and_names_only(
    engine: FakeEngine, project: Path, tmp_path: Path
) -> None:
    sandbox = engine.sandbox()
    job = make_job(project, tmp_path, "tests/test_ok.py", timeout=90)
    run = sandbox.run(job)
    assert run.exit_code == 0 and run.launch_error is None
    assert run.command == ("python", "-m", "pytest", *PYTEST_ARGS, "tests/test_ok.py")

    (call,) = engine.runs()
    argv = call["args"]
    pairs = _pairs(argv)
    for flag in ("--rm", "-i", "--read-only", "--init"):
        assert flag in argv
    for pair in [
        ("--network", "none"),
        ("--pull", "never"),
        ("--cap-drop", "ALL"),
        ("--security-opt", "no-new-privileges"),
        ("--pids-limit", "512"),
        ("--memory", "2g"),
        ("--memory-swap", "2g"),
        ("--shm-size", "64m"),
        ("--log-driver", "none"),
        ("--cpus", "2"),
        ("--label", f"{CONTAINER_LABEL}=1"),
        ("--entrypoint", "python"),
        ("--tmpfs", "/work:rw,exec,nosuid,nodev,size=1g,mode=1777"),
        ("--tmpfs", "/tmp:rw,noexec,nosuid,nodev,size=512m,mode=1777"),
    ]:
        assert pair in pairs, pair
    name = argv[argv.index("--name") + 1]
    match = re.fullmatch(r"dag-healer-sandbox-(\d+)-[0-9a-f]{8}", name)
    assert match and abs(int(match.group(1)) - (time.time() + 90 + 60)) < 30
    assert "--user" not in argv
    for forbidden in ("-v", "--volume", "--mount", "--privileged", "--cap-add", "--env-file", "-t"):
        assert forbidden not in argv
    assert not [part for part in argv if part.startswith(("--network=", "--volume=", "-v="))]
    # every variable travels by name: -e NAME, never NAME=value
    names = [value for flag, value in pairs if flag == "-e"]
    assert names and all(re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*", name) for name in names)
    image = argv.index(IMAGE)
    assert argv[image + 1 :] == [
        "-c",
        BOOTSTRAP,
        "pytest",
        "/work",
        "/tmp/home",
        "/tmp",
        *PYTEST_ARGS,
        "tests/test_ok.py",
    ]


def test_container_argv_follows_the_settings(engine: FakeEngine, project: Path, tmp_path: Path) -> None:
    sandbox = engine.sandbox(user="1000:1000", cpus=None, memory=" 512M ", pids=64, python="python3.12")
    assert sandbox.run(make_job(project, tmp_path, "tests/test_ok.py")).exit_code == 0
    argv = engine.runs()[0]["args"]
    pairs = _pairs(argv)
    assert ("--user", "1000:1000") in pairs and "--cpus" not in argv
    assert ("--memory", "512m") in pairs and ("--memory-swap", "512m") in pairs
    assert ("--pids-limit", "64") in pairs and ("--entrypoint", "python3.12") in pairs


def test_container_environment_is_passed_by_name_without_host_settings(
    engine: FakeEngine, project: Path, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setenv("DOCKER_HOST", "unix:///fake/docker.sock")
    monkeypatch.setenv("HTTP_PROXY", "http://user:proxy-secret@proxy.invalid:3128")
    monkeypatch.setenv("GITHUB_TOKEN", "ghp_canary_token_value")
    monkeypatch.setenv("ANTHROPIC_API_KEY", "sk-ant-canary-value")
    monkeypatch.setenv("SANDBOX_HARMLESS_SETTING", "kept")
    monkeypatch.setenv("XDG_RUNTIME_DIR", str(tmp_path / "xdg"))
    monkeypatch.setenv("LC_ALL", "C.UTF-8")
    engine.scenario(run={"mode": "script", "stdout": "1 passed in 0.01s\n"})

    result = run_pytest(project, ["tests/test_ok.py"], timeout=60, sandbox=engine.sandbox())

    assert result.ok and result.passed == 1, result.output_tail
    (call,) = engine.runs()
    names = set(call["env"])
    upper = {name.upper() for name in names}
    for host_only in ("PATH", "HOME", "TMPDIR", "TEMP", "TMP", "LANG", "PWD", "USER", "DOCKER_HOST"):
        assert host_only not in upper, host_only
    assert not [name for name in upper if name.startswith(("DOCKER_", "XDG_", "LC_", "CONTAINER"))]
    assert not {"GITHUB_TOKEN", "ANTHROPIC_API_KEY"} & upper  # scrubbed before the sandbox
    values = [value or "" for value in call["env"].values()]
    for secret in ("ghp_canary_token_value", "sk-ant-canary-value", "proxy-secret", "fake/docker.sock"):
        assert not [value for value in values if secret in value], secret
    env = call["env"]
    assert env["SANDBOX_HARMLESS_SETTING"] == "kept"
    assert env["AIRFLOW_HOME"] == "/tmp/airflow_home"
    assert env["AIRFLOW__DATABASE__SQL_ALCHEMY_CONN"] == "sqlite:////tmp/airflow_home/unittests.db"
    assert env["AIRFLOW__CORE__UNIT_TEST_MODE"] == "True"
    assert env["AWS_EC2_METADATA_DISABLED"] == "true"
    for proxy in ("HTTP_PROXY", "HTTPS_PROXY", "FTP_PROXY", "ALL_PROXY", "NO_PROXY"):
        assert env[proxy] == ""
        if POSIX:
            assert env[proxy.lower()] == ""
    # the engine CLI itself still finds its engine
    assert call["docker_host"] == "unix:///fake/docker.sock"
    assert not {"GITHUB_TOKEN", "ANTHROPIC_API_KEY"} & {key.upper() for key in call["env_keys"]}


def test_container_environment_and_engine_environment() -> None:
    base = {
        "_": "/home/worker/.venv/bin/dag-healer",
        "SHLVL": "2",
        "PATH": "/usr/bin",
        "HOME": "/home/worker",
        "TMPDIR": "/var/tmp",
        "LANG": "C.UTF-8",
        "LC_ALL": "C.UTF-8",
        "XDG_RUNTIME_DIR": "/run/user/1000",
        "DOCKER_HOST": "unix:///var/run/docker.sock",
        "DOCKER_CONFIG": "/home/worker/.docker",
        "CONTAINER_HOST": "unix:///run/podman.sock",
        "CONTAINERS_CONF": "/etc/containers/containers.conf",
        "PODMAN_USERNS": "keep-id",
        "VIRTUAL_ENV": "/opt/venv",
        "PYTHONUSERBASE": "/home/worker/.local",
        "PYTHONHOME": "/opt/python",
        "LD_PRELOAD": "/lib/evil.so",
        "SSH_AUTH_SOCK": "/tmp/agent.sock",
        "__CF_USER_TEXT_ENCODING": "0x1F5",
        "BAD-NAME": "x",
        "1BAD": "x",
        "AIRFLOW_HOME": "/var/tmp/dag-healer-pytest-1/airflow_home",
        "AIRFLOW__DATABASE__SQL_ALCHEMY_CONN": "sqlite:////var/tmp/dag-healer-pytest-1/airflow_home/unittests.db",
        "airflow_home": "/elsewhere",
        "http_proxy": "http://user:pw@proxy.invalid:3128",
        "AIRFLOW__CORE__UNIT_TEST_MODE": "True",
        "PYTHONDONTWRITEBYTECODE": "1",
        "SANDBOX_HARMLESS_SETTING": "kept",
    }
    env = container_environment(base)
    assert {name: env[name] for name in ("AIRFLOW__CORE__UNIT_TEST_MODE", "PYTHONDONTWRITEBYTECODE")} == {
        "AIRFLOW__CORE__UNIT_TEST_MODE": "True",
        "PYTHONDONTWRITEBYTECODE": "1",
    }
    assert env["SANDBOX_HARMLESS_SETTING"] == "kept"
    assert env["AIRFLOW_HOME"] == "/tmp/airflow_home" and "airflow_home" not in env
    assert env["AIRFLOW__DATABASE__SQL_ALCHEMY_CONN"] == "sqlite:////tmp/airflow_home/unittests.db"
    assert env["HTTP_PROXY"] == "" and env.get("http_proxy", "") == ""
    dropped = set(base) - {
        "AIRFLOW__CORE__UNIT_TEST_MODE",
        "PYTHONDONTWRITEBYTECODE",
        "SANDBOX_HARMLESS_SETTING",
    }
    dropped -= {"AIRFLOW_HOME", "AIRFLOW__DATABASE__SQL_ALCHEMY_CONN", "http_proxy"}
    assert not dropped & set(env), dropped & set(env)

    engine_env = engine_environment(base)
    assert set(engine_env) == {
        "PATH",
        "HOME",
        "TMPDIR",
        "LANG",
        "XDG_RUNTIME_DIR",
        "DOCKER_HOST",
        "DOCKER_CONFIG",
        "CONTAINER_HOST",
        "CONTAINERS_CONF",
        "SSH_AUTH_SOCK",
    }
    assert engine_environment({"GITHUB_TOKEN": "x", "HTTP_PROXY": "y", "SANDBOX_HARMLESS_SETTING": "z"}) == {}
    # names compare case-insensitively (Windows)
    assert set(
        container_environment({"Home": "/x", "Path": "/y", "Docker_Host": "z", "Http_Proxy": "w"})
    ) == set(container_environment({}))


def test_engine_and_container_variables_never_share_a_name() -> None:
    everything = {name: "x" for name in ENGINE_ENV}
    everything.update(
        {"DOCKER_TLS_VERIFY": "1", "CONTAINERS_REGISTRIES_CONF": "x", "xdg_config_home": "x", "OTHER": "x"}
    )
    engine_env = engine_environment(everything)
    assert set(engine_env) >= set(ENGINE_ENV)
    shared = {name.upper() for name in engine_env} & {
        name.upper() for name in container_environment(everything)
    }
    assert shared == set()


# =========================================================================== container: outcomes


def test_a_container_run_with_the_marker_is_a_test_result(
    engine: FakeEngine, project: Path, tmp_path: Path
) -> None:
    engine.scenario(
        run={
            "mode": "script",
            "stdout": "F.\nFAILED tests/test_bad.py::x\n1 failed, 1 passed in 0.10s\n",
            "code": 1,
        }
    )
    result = run_pytest(project, ["tests"], timeout=60, sandbox=engine.sandbox())
    assert (result.exit_code, result.failed, result.passed, result.launch_error) == (1, 1, 1, None)
    assert not result.collection_error


@pytest.mark.parametrize(
    ("stderr", "code", "expected"),
    [
        (
            "docker: Error response from daemon: No such image: dag-healer/sandbox-test:dev\n\n"
            "Run 'docker run --help' for more information\n",
            125,
            "the image dag-healer/sandbox-test:dev is not present on the docker engine; build or pull it "
            "there (the sandbox never pulls images)",
        ),
        (
            "failed to connect to the docker API at unix:///nonexistent/docker.sock; check if the path is "
            "correct and if the daemon is running\n",
            1,
            "docker exited with code 1 before the sandbox started: failed to connect to the docker API at "
            "unix:///nonexistent/docker.sock; check if the path is correct and if the daemon is running",
        ),
        ("", 127, "docker exited with code 127 before the sandbox started"),
    ],
)
def test_an_engine_error_is_a_launch_failure_never_a_test_result(
    engine: FakeEngine, project: Path, tmp_path: Path, stderr: str, code: int, expected: str
) -> None:
    engine.scenario(run={"mode": "fail", "stderr": stderr, "code": code})
    run = engine.sandbox().run(make_job(project, tmp_path, "tests/test_ok.py"))
    assert (run.exit_code, run.timed_out, run.launch_error) == (LAUNCH_FAILED_EXIT_CODE, False, expected)

    result = run_pytest(project, ["tests/test_ok.py"], timeout=60, sandbox=engine.sandbox())
    assert result.exit_code == LAUNCH_FAILED_EXIT_CODE and result.launch_error == expected
    assert result.collection_error and not result.ok
    assert "could not start pytest" in result.output_tail


def test_an_engine_error_after_the_start_is_a_launch_failure(
    engine: FakeEngine, project: Path, tmp_path: Path
) -> None:
    """docker exits 125 when it loses its daemon while the container runs."""
    engine.scenario(
        run={"mode": "script", "stderr": "docker: error waiting for container: unexpected EOF\n", "code": 125}
    )
    run = engine.sandbox().run(make_job(project, tmp_path, "tests/test_ok.py"))
    assert (run.exit_code, run.launch_error) == (
        LAUNCH_FAILED_EXIT_CODE,
        "docker failed while the sandbox ran (exit 125): docker: error waiting for container: unexpected EOF",
    )
    argv = engine.runs()[0]["args"]
    assert ["rm", "-f", argv[argv.index("--name") + 1]] in engine.commands()  # never left running
    engine.scenario(run={"mode": "script", "code": 126})  # no engine error: the test's own exit code
    run = engine.sandbox().run(make_job(project, tmp_path, "tests/test_ok.py"))
    assert (run.exit_code, run.launch_error) == (126, None)


def test_a_bootstrap_refusal_is_a_launch_failure(engine: FakeEngine, project: Path, tmp_path: Path) -> None:
    message = "refusing to run as root (uid 0); give the image a non-root USER or set repo.sandbox.user"
    engine.scenario(
        run={
            "mode": "script",
            "stderr": f"dag-healer sandbox: error: {message}\n",
            "code": BOOTSTRAP_FAILED_EXIT_CODE,
        }
    )
    run = engine.sandbox().run(make_job(project, tmp_path, "tests/test_ok.py"))
    assert (run.exit_code, run.launch_error) == (LAUNCH_FAILED_EXIT_CODE, message)

    engine.scenario(run={"mode": "script", "code": BOOTSTRAP_FAILED_EXIT_CODE})
    run = engine.sandbox().run(make_job(project, tmp_path, "tests/test_ok.py"))
    assert run.launch_error == f"the sandbox bootstrap failed (exit {BOOTSTRAP_FAILED_EXIT_CODE})"


def test_an_out_of_memory_kill_keeps_its_exit_code_with_a_hint(
    engine: FakeEngine, project: Path, tmp_path: Path
) -> None:
    engine.scenario(run={"mode": "script", "stdout": "..", "code": 137})
    job = make_job(project, tmp_path, "tests/test_ok.py")
    run = engine.sandbox(memory="256m").run(job)
    assert (run.exit_code, run.launch_error) == (137, None)
    assert "may have run out of memory (repo.sandbox.memory is 256m)" in job.output.read_text(
        encoding="utf-8"
    )


def test_the_timeout_kills_and_removes_the_container_by_name(
    engine: FakeEngine, project: Path, tmp_path: Path
) -> None:
    engine.scenario(run={"mode": "sleep"})
    start = time.monotonic()
    run = engine.sandbox().run(make_job(project, tmp_path, "tests/test_ok.py", timeout=2))
    assert time.monotonic() - start < 45
    assert (run.exit_code, run.timed_out, run.launch_error) == (TIMEOUT_EXIT_CODE, True, None)
    argv = engine.runs()[0]["args"]
    name = argv[argv.index("--name") + 1]
    commands = engine.commands()
    assert ["kill", name] in commands and ["rm", "-f", name] in commands
    assert commands.index(["kill", name]) < commands.index(["rm", "-f", name])

    result = run_pytest(project, ["tests/test_ok.py"], timeout=2, sandbox=engine.sandbox())
    assert result.timed_out and result.exit_code == TIMEOUT_EXIT_CODE
    assert "killed after exceeding the 2 s timeout" in result.output_tail


def test_a_run_whose_output_passes_the_limit_is_killed_by_name(
    engine: FakeEngine, project: Path, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Like the process backend's RLIMIT_FSIZE: a flood of output never fills the worker's disk."""
    monkeypatch.setattr(sandbox_module, "MAX_OUTPUT_BYTES", 4096)
    monkeypatch.setattr(sandbox_module, "OUTPUT_CHECK_SECONDS", 0.05)
    engine.scenario(run={"mode": "script", "stdout": "x" * 20000 + "\n", "sleep": 60})
    job = make_job(project, tmp_path, "tests/test_ok.py")
    start = time.monotonic()
    run = engine.sandbox().run(job)
    assert time.monotonic() - start < 45
    assert (run.exit_code, run.timed_out, run.launch_error) == (KILLED_EXIT_CODE, False, None)
    argv = engine.runs()[0]["args"]
    name = argv[argv.index("--name") + 1]
    commands = engine.commands()
    assert ["kill", name] in commands and ["rm", "-f", name] in commands
    note = "[dag-healer] pytest was killed: its output passed 4096 bytes (only the end of it is kept)\n"
    assert job.output.read_text(encoding="utf-8").endswith(note)

    result = run_pytest(project, ["tests/test_ok.py"], timeout=60, sandbox=engine.sandbox())
    assert (result.exit_code, result.timed_out, result.launch_error) == (KILLED_EXIT_CODE, False, None)
    assert not result.ok and result.output_tail.endswith(note)


def test_output_below_the_limit_is_a_normal_result(
    engine: FakeEngine, project: Path, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setattr(sandbox_module, "OUTPUT_CHECK_SECONDS", 0.05)
    engine.scenario(run={"mode": "script", "stdout": "1 passed in 0.01s\n", "sleep": 0.5})
    run = engine.sandbox().run(make_job(project, tmp_path, "tests/test_ok.py"))
    assert (run.exit_code, run.timed_out, run.launch_error) == (0, False, None)
    assert not any(command[:1] == ["kill"] for command in engine.commands())


@pytest.mark.skipif(not POSIX or IS_ROOT, reason="needs a file the worker cannot read (POSIX, not root)")
def test_a_workspace_that_cannot_be_sent_is_a_launch_failure(
    engine: FakeEngine, project: Path, tmp_path: Path
) -> None:
    unreadable = project / "pkg" / "unreadable.txt"
    unreadable.write_text("x", encoding="utf-8")
    unreadable.chmod(0)
    try:
        engine.scenario(run={"mode": "script", "read_stdin": True, "stdout": "1 passed in 0.01s\n"})
        run = engine.sandbox().run(make_job(project, tmp_path, "tests/test_ok.py"))
    finally:
        unreadable.chmod(0o644)
    assert run.exit_code == LAUNCH_FAILED_EXIT_CODE
    assert run.launch_error is not None
    assert run.launch_error.startswith("could not send the workspace to the sandbox: ")


def test_unusable_settings_are_launch_failures_without_an_engine_call(
    engine: FakeEngine, project: Path, tmp_path: Path
) -> None:
    for settings, expected in [
        ({"image": None}, "repo.sandbox.image is not set"),
        ({"user": "0"}, "repo.sandbox.user '0' is root"),
        ({"python": "-Ic"}, "repo.sandbox.python '-Ic' is not an interpreter name or path"),
        ({"work_size": "1g,suid"}, "repo.sandbox.work_size '1g,suid' is not a size"),
    ]:
        run = engine.sandbox(**settings).run(make_job(project, tmp_path, "tests/test_ok.py"))
        assert run.exit_code == LAUNCH_FAILED_EXIT_CODE
        assert run.launch_error is not None and run.launch_error.startswith(expected), run.launch_error
    assert engine.calls() == []


def test_a_missing_engine_cli_is_a_launch_failure(project: Path, tmp_path: Path) -> None:
    sandbox = ContainerSandbox(make_settings(), command=[str(tmp_path / "no-such-docker")])
    run = sandbox.run(make_job(project, tmp_path, "tests/test_ok.py"))
    assert run.exit_code == LAUNCH_FAILED_EXIT_CODE
    assert run.launch_error == (
        "the docker command was not found on the worker's PATH; install the Docker CLI or set "
        "repo.sandbox.engine"
    )


# =========================================================================== container: availability


@pytest.mark.parametrize(
    ("settings", "expected"),
    [
        (
            {"image": None},
            "repo.sandbox.image is not set; set it in healer.yaml to an image with Python, pytest "
            "and the repository's requirements (the template of 'dag-healer init' reads it from "
            "HEALER_SANDBOX_IMAGE)",
        ),
        ({"image": ""}, "repo.sandbox.image is not set;"),
        ({"image": "--privileged"}, "repo.sandbox.image '--privileged' is not an image reference"),
        ({"image": "my image"}, "repo.sandbox.image 'my image' is not an image reference"),
        (
            {"user": "root"},
            "repo.sandbox.user 'root' is root; use a non-root user, or null for the image's USER",
        ),
        ({"user": "0"}, "repo.sandbox.user '0' is root"),
        ({"user": "0:0"}, "repo.sandbox.user '0:0' is root"),
        ({"user": "root:wheel"}, "repo.sandbox.user 'root:wheel' is root"),
        ({"user": "000"}, "repo.sandbox.user '000' is root"),
    ],
)
def test_settings_problems_come_first_and_call_no_engine(
    engine: FakeEngine, settings: dict[str, Any], expected: str
) -> None:
    reason = engine.sandbox(**settings).unavailable_reason()
    assert reason is not None and reason.startswith(expected)
    assert engine.calls() == []


def test_a_missing_cli_is_named_with_its_fix(tmp_path: Path) -> None:
    missing = [str(tmp_path / "no-such-cli")]
    docker = ContainerSandbox(make_settings(), command=missing).unavailable_reason()
    podman = ContainerSandbox(make_settings(engine="podman"), command=missing).unavailable_reason()
    assert docker == (
        "the docker command was not found on the worker's PATH; install the Docker CLI or set "
        "repo.sandbox.engine"
    )
    assert podman == (
        "the podman command was not found on the worker's PATH; install Podman or set repo.sandbox.engine"
    )


def test_an_unreachable_engine_is_reported_with_its_first_error_line(engine: FakeEngine) -> None:
    engine.scenario(
        version={
            "stdout": "\n",
            "stderr": "\nfailed to connect to the docker API at unix:///nonexistent/docker.sock; check if "
            "the path is correct\nsecond line\n",
            "code": 1,
        }
    )
    reason = engine.sandbox().unavailable_reason()
    assert reason == (
        "docker cannot reach its engine (failed to connect to the docker API at "
        "unix:///nonexistent/docker.sock; check if the path is correct); start it, or give the worker "
        "access to it (DOCKER_HOST or CONTAINER_HOST, the socket and its group)"
    )
    assert engine.commands() == [["version", "--format", "{{.Server.Version}}"]]  # no image inspect


def test_an_engine_that_does_not_answer(engine: FakeEngine, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(sandbox_module, "PROBE_TIMEOUT", 1.0)
    engine.scenario(version={"sleep": 20})
    assert engine.sandbox().unavailable_reason() == "docker did not answer within 1 s"


def test_a_missing_image_is_reported(engine: FakeEngine) -> None:
    engine.scenario(inspect={"stderr": f"Error response from daemon: No such image: {IMAGE}\n", "code": 1})
    assert engine.sandbox().unavailable_reason() == (
        f"the image {IMAGE} is not present on the docker engine; build or pull it there "
        "(the sandbox never pulls images)"
    )
    assert engine.commands()[-1] == ["image", "inspect", "--format", "{{json .Config.User}}", IMAGE]


@pytest.mark.parametrize(
    ("image_user", "settings_user", "root"),
    [
        ('""', None, True),
        ('"root"', None, True),
        ('"0"', None, True),
        ('"0:0"', None, True),
        ('"root:root"', None, True),
        ('"airflow"', None, False),
        ('"50000:0"', None, False),
        ('"sandbox"', None, False),
        ('""', "10001", False),
        ('"root"', "1000:1000", False),
    ],
)
def test_an_image_that_runs_as_root_is_refused(
    engine: FakeEngine, image_user: str, settings_user: str | None, root: bool
) -> None:
    engine.scenario(inspect={"stdout": image_user + "\n"})
    reason = engine.sandbox(user=settings_user).unavailable_reason()
    if root:
        assert reason == f"the image {IMAGE} runs as root; give it a non-root USER or set repo.sandbox.user"
    else:
        assert reason is None


def test_a_success_is_cached_and_a_failure_is_probed_again(engine: FakeEngine) -> None:
    engine.scenario(inspect={"stderr": "No such image\n", "code": 1})
    sandbox = engine.sandbox()
    assert sandbox.unavailable_reason() is not None
    engine.scenario()  # the image is there now ("retry repair" after building it)
    assert sandbox.unavailable_reason() is None
    probes = len(engine.calls())
    assert sandbox.unavailable_reason() is None
    assert len(engine.calls()) == probes  # cached


def test_the_engine_command_is_looked_up_at_call_time(
    engine: FakeEngine, monkeypatch: pytest.MonkeyPatch
) -> None:
    sandbox = ContainerSandbox(make_settings(engine="podman"))
    seen: list[str] = []

    def fake_command(name: str) -> list[str]:
        seen.append(name)
        return engine.command

    monkeypatch.setattr(sandbox_module, "engine_command", fake_command)
    assert sandbox.command == engine.command
    assert sandbox.unavailable_reason() is None
    assert "podman" in seen
    assert engine.commands()[0] == ["version", "--format", "{{.Client.Version}}"]


# =========================================================================== container: smoke test

GOOD_PROBE = {
    "uid": 10001,
    "user": "sandbox",
    "python": "3.12.8",
    "pytest": "9.1.1",
    "interfaces": ["lo"],
    "connect": "blocked: [Errno 101] Network is unreachable",
}


@pytest.mark.parametrize(
    ("changes", "problem"),
    [
        ({}, None),
        ({"interfaces": ["eth0", "lo"]}, "it has a network interface besides lo (eth0)"),
        ({"uid": 0, "user": "root"}, "it runs as root (uid 0)"),
        ({"connect": "open"}, "an outbound connection to 1.1.1.1:53 succeeded"),
        ({"pytest": None}, "pytest is not importable in the image (Python 3.12.8)"),
        ({"interfaces": None}, "the probe could not list the network interfaces (/sys/class/net)"),
    ],
)
def test_smoke_test_judges_the_probe(
    engine: FakeEngine, changes: dict[str, Any], problem: str | None
) -> None:
    engine.scenario(
        run={"mode": "script", "stdout": "noise\n" + json.dumps({**GOOD_PROBE, **changes}) + "\n"}
    )
    probe = engine.sandbox().smoke_test()
    if problem is None:
        assert probe.ok
        assert probe.detail == (
            f"docker 29.8.2, image {IMAGE}: uid 10001 (sandbox), no network (interfaces: lo), "
            "pytest 9.1.1 on Python 3.12.8"
        )
    else:
        assert not probe.ok and problem in probe.detail and probe.detail.startswith("docker 29.8.2, image ")
    argv = engine.runs()[0]["args"]
    assert argv[argv.index(IMAGE) + 1 :] == ["-c", BOOTSTRAP, "probe", "/work", "/tmp/home", "/tmp"]
    assert ("--network", "none") in _pairs(argv) and "--read-only" in argv


def test_smoke_test_failures(engine: FakeEngine) -> None:
    engine.scenario(run={"mode": "script", "stdout": "garbage\n"})
    probe = engine.sandbox().smoke_test()
    assert not probe.ok and "the sandbox probe printed no result" in probe.detail

    engine.scenario(run={"mode": "fail", "stderr": "docker: Error response from daemon: boom\n", "code": 125})
    probe = engine.sandbox().smoke_test()
    assert not probe.ok and "the sandbox did not start: docker exited with code 125" in probe.detail

    assert not engine.sandbox(image=None).smoke_test().ok


# =========================================================================== container: leftovers


def test_remove_expired_removes_only_expired_sandbox_containers(engine: FakeEngine) -> None:
    future = int(time.time()) + 3600
    names = [
        "dag-healer-sandbox-1000-0123abcd",
        f"dag-healer-sandbox-{future}-89abcdef",
        "dag-healer-sandbox-not-a-time",
        "someone-elses-container",
    ]
    engine.scenario(ps={"stdout": "\n".join(names) + "\n"})
    assert engine.sandbox().remove_expired() == ["dag-healer-sandbox-1000-0123abcd"]
    commands = engine.commands()
    assert ["ps", "-a", "--filter", f"label={CONTAINER_LABEL}=1", "--format", "{{.Names}}"] in commands
    assert [command for command in commands if command[:1] == ["rm"]] == [
        ["rm", "-f", "dag-healer-sandbox-1000-0123abcd"]
    ]


def test_runs_sweep_for_leftovers_at_most_once_per_interval(
    engine: FakeEngine, project: Path, tmp_path: Path
) -> None:
    sandbox = engine.sandbox()
    for _ in range(2):
        assert sandbox.run(make_job(project, tmp_path, "tests/test_ok.py")).exit_code == 0
    assert [command[0] for command in engine.commands()].count("ps") == 1


def test_remove_expired_never_raises(tmp_path: Path) -> None:
    assert ContainerSandbox(make_settings(), command=[str(tmp_path / "missing")]).remove_expired() == []


# =========================================================================== the bootstrap itself


def _host_env(**extra: str) -> dict[str, str]:
    keep = ("PATH", "SYSTEMROOT", "SYSTEMDRIVE", "WINDIR", "COMSPEC", "PATHEXT", "TEMP", "TMP")
    env = {key: value for key, value in os.environ.items() if key.upper() in keep}
    env.update({"PYTHONDONTWRITEBYTECODE": "1", **extra})
    return env


def _bootstrap(
    tmp_path: Path,
    mode: str,
    *args: str,
    stdin: bytes = b"",
    source: str = BOOTSTRAP,
    flags: tuple[str, ...] = (),
) -> tuple[subprocess.CompletedProcess[bytes], Path]:
    work, home, tmp = tmp_path / "work", tmp_path / "tmp" / "home", tmp_path / "tmp"
    work.mkdir(exist_ok=True)
    proc = subprocess.run(
        [sys.executable, *flags, "-c", source, mode, str(work), str(home), str(tmp), *args],
        input=stdin,
        capture_output=True,
        env=_host_env(AIRFLOW_HOME=str(tmp / "airflow_home")),
        timeout=120,
        check=False,
    )
    return proc, work


def _tar_of(root: Path) -> bytes:
    buffer = io.BytesIO()
    write_workspace_tar(root, buffer)
    return buffer.getvalue()


def test_the_bootstrap_carries_the_module_constants() -> None:
    assert repr(BOOTSTRAP_MARKER) in BOOTSTRAP and f"FAILED = {BOOTSTRAP_FAILED_EXIT_CODE}" in BOOTSTRAP
    assert "@" not in BOOTSTRAP.split("def fail")[0]  # every placeholder was filled in
    compile(BOOTSTRAP, "<bootstrap>", "exec")


@pytest.mark.skipif(IS_ROOT, reason="the bootstrap refuses to run as root")
def test_the_bootstrap_unpacks_the_workspace_and_runs_pytest(project: Path, tmp_path: Path) -> None:
    proc, work = _bootstrap(
        tmp_path, "pytest", *PYTEST_ARGS, "tests/test_ok.py", "tests/test_bad.py", stdin=_tar_of(project)
    )
    stdout, stderr = proc.stdout.decode(), proc.stderr.decode()
    assert proc.returncode == 1, stdout + stderr
    assert stderr.splitlines()[0] == BOOTSTRAP_MARKER
    assert "1 failed, 1 passed" in stdout and "test_double_negative" in stdout  # stderr merged into stdout
    assert (work / "pkg" / "calc.py").is_file() and not (work / ".git").exists()
    assert (tmp_path / "tmp" / "home").is_dir() and (tmp_path / "tmp" / "airflow_home").is_dir()


@pytest.mark.skipif(IS_ROOT, reason="the bootstrap refuses to run as root")
def test_the_bootstrap_probe_prints_one_json_line(tmp_path: Path) -> None:
    with socket.socket() as probe:  # a local port nobody listens on: the probe stays offline
        probe.bind(("127.0.0.1", 0))
        address = probe.getsockname()
    source = sandbox_module._bootstrap_source(address)
    proc, _ = _bootstrap(tmp_path, "probe", source=source)
    assert proc.returncode == 0, proc.stderr.decode()
    facts = json.loads(proc.stdout.decode().strip().splitlines()[-1])
    assert set(facts) == {"uid", "user", "python", "pytest", "interfaces", "connect"}
    assert facts["pytest"] == pytest.__version__
    assert facts["connect"].startswith("blocked: ")
    if POSIX:
        assert facts["uid"] == os.geteuid()


@pytest.mark.skipif(IS_ROOT, reason="the bootstrap refuses to run as root")
def test_the_bootstrap_refuses_an_image_without_pytest(tmp_path: Path) -> None:
    proc, _ = _bootstrap(tmp_path, "pytest", stdin=b"", flags=("-S",))  # no site-packages: no pytest
    assert proc.returncode == BOOTSTRAP_FAILED_EXIT_CODE
    assert "dag-healer sandbox: error: pytest is not importable in the image" in proc.stderr.decode()


def _malicious_tar(kind: str) -> bytes:
    buffer = io.BytesIO()
    with tarfile.open(fileobj=buffer, mode="w", format=tarfile.PAX_FORMAT) as archive:
        if kind == "device":
            info = tarfile.TarInfo("dev")
            info.type = tarfile.CHRTYPE
            archive.addfile(info)
        elif kind == "link":
            info = tarfile.TarInfo("link")
            info.type, info.linkname = tarfile.SYMTYPE, "../../outside"
            archive.addfile(info)
        elif kind == "absolute-link":
            info = tarfile.TarInfo("link")
            info.type, info.linkname = tarfile.SYMTYPE, "/etc"
            archive.addfile(info)
        else:
            data = b"planted\n"
            info = tarfile.TarInfo({"absolute": "/abs.txt", "parent": "../escape.txt"}[kind])
            info.size = len(data)
            archive.addfile(info, io.BytesIO(data))
    return buffer.getvalue()


@pytest.mark.skipif(IS_ROOT, reason="the bootstrap refuses to run as root")
@pytest.mark.parametrize("kind", ["absolute", "parent", "device", "link", "absolute-link"])
def test_the_bootstrap_refuses_unsafe_archive_members(tmp_path: Path, kind: str) -> None:
    proc, work = _bootstrap(tmp_path, "pytest", *PYTEST_ARGS, stdin=_malicious_tar(kind))
    assert proc.returncode == BOOTSTRAP_FAILED_EXIT_CODE, proc.stdout.decode() + proc.stderr.decode()
    assert "dag-healer sandbox: error: could not unpack the workspace" in proc.stderr.decode()
    assert list(work.iterdir()) == []
    assert not (tmp_path / "escape.txt").exists()


# =========================================================================== container: emulated end to end


@pytest.mark.skipif(IS_ROOT, reason="the bootstrap refuses to run as root")
def test_run_pytest_in_an_emulated_container(
    engine: FakeEngine, project: Path, tmp_path: Path, caplog: pytest.LogCaptureFixture
) -> None:
    links = symlink("pkg/data.txt", project / "inside_link") and symlink(
        str(tmp_path / "secret.txt"), project / "absolute_link"
    )
    (tmp_path / "secret.txt").write_text("SECRET\n", encoding="utf-8")
    write_files(project, {"tests/test_vandal.py": VANDAL_TEST})
    before = snapshot(project)
    engine.scenario(run={"mode": "emulate"})

    with caplog.at_level(logging.INFO, logger="dag_healer.repo.pytest_runner"):
        result = run_pytest(project, ["tests"], timeout=120, sandbox=engine.sandbox())

    assert (result.exit_code, result.passed, result.failed, result.launch_error) == (1, 2, 1, None), (
        result.output_tail
    )
    assert "test_double_negative" in result.output_tail
    assert result.command[:3] == ["python", "-m", "pytest"]
    assert "(container sandbox)" in caplog.text
    assert snapshot(project) == before  # the clone never changes
    work = engine.last_run_dir() / "work"
    assert (work / "created.txt").is_file()  # the vandal test ran in the container's copy
    assert not (work / "absolute_link").exists() and not (work / "absolute_link").is_symlink()
    if links:
        assert (work / "inside_link").is_symlink() and _link_target(work / "inside_link") == "pkg/data.txt"
