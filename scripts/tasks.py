#!/usr/bin/env python3
"""Cross-platform task runner for the dag-healer development stack.

Every Makefile target delegates here, so ``make`` is optional (it is not installed on
Windows by default)::

    python scripts/tasks.py up             # build if needed, start Airflow + dag-healer
    python scripts/tasks.py demo           # the scripted end-to-end demo (scripts/demo.py)
    python scripts/tasks.py doctor         # "dag-healer doctor" in healer-worker (test sandbox too)
    python scripts/tasks.py healer contracts list   # any dag-healer command, in the api-server
    python scripts/tasks.py --help         # every command

Standard library only (Python 3.10+). It works from any working directory and with
repository paths that contain spaces: external commands are always argument lists,
never shell strings, and the exit code of the command is the exit code of the task.
"""

from __future__ import annotations

import argparse
import functools
import json
import os
import re
import shlex
import shutil
import stat
import subprocess
import sys
import tempfile
import time
import urllib.error
import urllib.request
from collections.abc import Callable, Sequence
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parent.parent
DOCKER_DIR = ROOT / "docker"
COMPOSE_FILE = DOCKER_DIR / "compose.yaml"
DOCKERFILE = DOCKER_DIR / "Dockerfile"
ENV_FILE = DOCKER_DIR / ".env"
ENV_EXAMPLE = DOCKER_DIR / ".env.example"
DEMO_DIR = ROOT / ".demo"
DEMO_REPO = DEMO_DIR / "repo"
DEMO_LOGS = DEMO_DIR / "logs"
EXAMPLE_REPO = ROOT / "example_existing_repo"
UI_DIR = ROOT / "ui"
BUNDLE_PATH = ROOT / "src" / "dag_healer" / "api" / "static" / "main.umd.cjs"
#: The standalone bundle (``dag-healer serve``): a directory next to BUNDLE_PATH and its entry.
STANDALONE_DIRNAME = "standalone"
STANDALONE_ENTRY = "index.js"
SCRIPTS_DIR = ROOT / "scripts"

PROJECT_NAME = "dag-healer"
APISERVER = "airflow-apiserver"
WORKER_SERVICE = "healer-worker"
CORE_SERVICES = (
    "postgres",
    "mailpit",
    "airflow-apiserver",
    "airflow-scheduler",
    "airflow-dag-processor",
    "airflow-triggerer",
)
ALL_PROFILES = ("tests",)
CONTAINER_REPO = "/opt/airflow/repo"

AIRFLOW_URL = "http://localhost:8080"
HEALTH_URL = f"{AIRFLOW_URL}/api/v2/monitor/health"
HEALTH_REQUIRED_COMPONENTS = ("metadatabase", "scheduler")
DEFAULT_TIMEOUT = 600
#: Seconds `docker info` may take before the daemon is considered unreachable.
DOCKER_INFO_TIMEOUT = 60
#: Host path of the engine socket that docker/compose.yaml mounts into healer-worker (the test
#: sandbox), unless HEALER_DOCKER_SOCKET names another one.
DEFAULT_DOCKER_SOCKET = "/var/run/docker.sock"

DEMO_GIT_USER = "dag-healer demo"
DEMO_GIT_EMAIL = "dag-healer-demo@example.com"
DEMO_COMMIT_MESSAGE = "Initial legacy repo"
COPY_IGNORE = shutil.ignore_patterns(
    ".git",
    "__pycache__",
    "*.pyc",
    "*.pyo",
    ".pytest_cache",
    ".ruff_cache",
    ".mypy_cache",
    ".venv",
    "venv",
    "node_modules",
)


class TaskError(Exception):
    """A task failed; the message is shown to the user and ``code`` becomes the exit code."""

    def __init__(self, message: str, code: int = 1) -> None:
        super().__init__(message)
        self.code = code


# --------------------------------------------------------------------------- helpers


def info(message: str) -> None:
    """Print a progress message of the task runner."""
    print(f"[tasks] {message}", flush=True)


def rel(path: Path) -> str:
    """Return ``path`` relative to the repository root when possible (for messages)."""
    try:
        return path.resolve().relative_to(ROOT).as_posix()
    except ValueError:
        return str(path)


def display_command(cmd: Sequence[str]) -> str:
    """Render an argument list the way the current platform's shell would quote it."""
    if os.name == "nt":
        return subprocess.list2cmdline(list(cmd))
    return shlex.join(cmd)


def run(cmd: Sequence[str], *, cwd: Path | None = None, env: dict[str, str] | None = None) -> int:
    """Run a command with inherited stdio, echoing it first; return its exit code."""
    print(f"$ {display_command(cmd)}", flush=True)
    try:
        return subprocess.run(list(cmd), cwd=cwd, env=env, check=False).returncode
    except FileNotFoundError as exc:
        raise TaskError(f"cannot run {cmd[0]!r}: {exc}") from exc


def capture(cmd: Sequence[str], *, cwd: Path | None = None, timeout: float | None = None) -> tuple[int, str]:
    """Run a command quietly and return ``(exit code, stdout + stderr)``.

    With ``timeout`` (seconds) a command that does not finish in time is killed and
    reported as exit code 124.
    """
    try:
        proc = subprocess.run(
            list(cmd),
            cwd=cwd,
            check=False,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            text=True,
            encoding="utf-8",
            errors="replace",
            timeout=timeout,
        )
    except FileNotFoundError as exc:
        return 127, str(exc)
    except subprocess.TimeoutExpired:
        return 124, f"{cmd[0]} did not finish within {timeout:g} s"
    return proc.returncode, proc.stdout or ""


def which(name: str, hint: str) -> str:
    """Return the absolute path of an executable or fail with an installation hint."""
    found = shutil.which(name)
    if not found:
        raise TaskError(f"'{name}' was not found on PATH. {hint}")
    return found


def is_interactive() -> bool:
    """True when both stdin and stdout are terminals (a TTY can be allocated)."""
    try:
        return sys.stdin.isatty() and sys.stdout.isatty()
    except (AttributeError, ValueError):
        return False


def confirm(question: str) -> bool:
    """Ask a yes/no question on an interactive terminal; non-interactive means no."""
    if not is_interactive():
        return False
    try:
        answer = input(f"{question} [y/N] ")
    except EOFError:
        return False
    return answer.strip().lower() in {"y", "yes"}


def remove_tree(path: Path) -> None:
    """Delete a directory tree, clearing read-only flags (git objects on Windows)."""
    if not path.exists():
        return

    def _make_writable_and_retry(func: Callable[..., Any], target: str, _exc: Any) -> None:
        os.chmod(target, stat.S_IWRITE | stat.S_IREAD)
        func(target)

    try:
        if sys.version_info >= (3, 12):
            shutil.rmtree(path, onexc=_make_writable_and_retry)
        else:  # pragma: no cover - exercised on Python 3.10/3.11 only
            shutil.rmtree(path, onerror=_make_writable_and_retry)
    except OSError as exc:
        raise TaskError(
            f"could not delete {rel(path)}: {exc}\n"
            "Files written by the containers may belong to another user (Linux): set AIRFLOW_UID "
            "in docker/.env to your uid, or delete the folder with elevated permissions."
        ) from exc


def host_python() -> str:
    """Python of the repository virtualenv (.venv) when present, else the current one."""
    candidates = (
        ROOT / ".venv" / "Scripts" / "python.exe",
        ROOT / ".venv" / "bin" / "python",
    )
    for candidate in candidates:
        if candidate.is_file():
            return str(candidate)
    return sys.executable


def optional_script(name: str) -> Path | None:
    """Return ``scripts/<name>`` when it exists."""
    path = SCRIPTS_DIR / name
    return path if path.is_file() else None


# --------------------------------------------------------------------------- docker


@functools.lru_cache(maxsize=1)
def ensure_docker_running() -> str:
    """Return the docker executable after checking that the daemon answers."""
    docker = which("docker", "Install Docker Desktop (or Docker Engine with the compose plugin).")
    code, out = capture([docker, "info", "--format", "{{.ServerVersion}}"], timeout=DOCKER_INFO_TIMEOUT)
    if code == 124:
        raise TaskError(
            f"The Docker daemon did not answer within {DOCKER_INFO_TIMEOUT} s. Docker Desktop may be "
            "starting or stuck: check its window (or restart it) and retry."
        )
    if code != 0:
        raise TaskError(
            "The Docker daemon is not reachable. Start Docker Desktop (it can take a few "
            f"minutes) and retry.\n{out.strip()}"
        )
    return docker


def host_uid() -> int | None:
    """UID to run the containers with on Linux hosts (None: keep the image default 50000)."""
    if not sys.platform.startswith("linux") or not hasattr(os, "getuid"):
        return None
    uid = os.getuid()
    return uid if uid != 0 else None


def docker_socket_gid() -> int | None:
    """Group of the engine socket on Linux hosts (None elsewhere, or when it cannot be read).

    healer-worker joins it (``HEALER_DOCKER_GID`` in docker/.env) to start the test sandbox.
    The socket is ``HEALER_DOCKER_SOCKET`` from the shell, else :data:`DEFAULT_DOCKER_SOCKET`.
    Elsewhere (Docker Desktop, OrbStack) the compose default, group 0, applies.
    """
    if not sys.platform.startswith("linux"):
        return None
    try:
        return os.stat(os.environ.get("HEALER_DOCKER_SOCKET") or DEFAULT_DOCKER_SOCKET).st_gid
    except OSError:
        return None


def ensure_env_file() -> Path:
    """Create docker/.env from docker/.env.example when it does not exist yet.

    On Linux it fills in ``AIRFLOW_UID`` (:func:`host_uid`) and, when the example has the
    key, ``HEALER_DOCKER_GID`` (:func:`docker_socket_gid`).
    """
    if ENV_FILE.exists():
        return ENV_FILE
    if not ENV_EXAMPLE.is_file():
        raise TaskError(f"{rel(ENV_EXAMPLE)} is missing; cannot create {rel(ENV_FILE)}")
    text = ENV_EXAMPLE.read_text(encoding="utf-8")
    uid = host_uid()
    if uid is not None:
        text = re.sub(r"(?m)^AIRFLOW_UID=.*$", f"AIRFLOW_UID={uid}", text)
    gid = docker_socket_gid()
    if gid is not None:
        text = re.sub(r"(?m)^HEALER_DOCKER_GID=.*$", f"HEALER_DOCKER_GID={gid}", text)
    ENV_FILE.write_text(text, encoding="utf-8", newline="\n")
    info(f"created {rel(ENV_FILE)} from {rel(ENV_EXAMPLE)} (development defaults, edit as needed)")
    return ENV_FILE


def compose_base(profiles: Sequence[str] = ()) -> list[str]:
    """The ``docker compose`` prefix for this project (file, project name, env file, profiles)."""
    docker = ensure_docker_running()
    ensure_env_file()
    cmd = [
        docker,
        "compose",
        "--project-name",
        PROJECT_NAME,
        "--file",
        str(COMPOSE_FILE),
        "--env-file",
        str(ENV_FILE),
    ]
    for profile in profiles:
        cmd += ["--profile", profile]
    return cmd


def compose_exec(service: str, command: Sequence[str], *, user: str | None = None) -> int:
    """Run a command inside a running service container (``docker compose exec``)."""
    cmd = compose_base(ALL_PROFILES) + ["exec"]
    if not is_interactive():
        cmd.append("-T")
    if user:
        cmd += ["--user", user]
    cmd += [service, *command]
    return run(cmd)


# --------------------------------------------------------------------------- health


def evaluate_health(payload: Any) -> tuple[bool, str]:
    """Interpret a ``/api/v2/monitor/health`` payload: ``(healthy, short detail)``."""
    if not isinstance(payload, dict):
        return False, "unexpected health payload"
    problems = []
    for component in HEALTH_REQUIRED_COMPONENTS:
        entry = payload.get(component)
        status = entry.get("status") if isinstance(entry, dict) else None
        if status != "healthy":
            problems.append(f"{component}={status or 'unknown'}")
    if problems:
        return False, ", ".join(problems)
    return True, "healthy"


def check_health(url: str = HEALTH_URL, timeout: float = 5.0) -> tuple[bool, str]:
    """Query the Airflow health endpoint once (no proxies: it is always localhost)."""
    opener = urllib.request.build_opener(urllib.request.ProxyHandler({}))
    request = urllib.request.Request(url, headers={"Accept": "application/json"})
    try:
        with opener.open(request, timeout=timeout) as response:
            body = response.read().decode("utf-8", errors="replace")
    except urllib.error.HTTPError as exc:
        return False, f"HTTP {exc.code}"
    except (urllib.error.URLError, OSError) as exc:
        return False, str(getattr(exc, "reason", exc))
    try:
        payload = json.loads(body or "{}")
    except ValueError:
        return False, "health endpoint did not return JSON"
    return evaluate_health(payload)


def wait_for_airflow(
    timeout: float = DEFAULT_TIMEOUT,
    *,
    url: str = HEALTH_URL,
    interval: float = 5.0,
    probe: Callable[[str], tuple[bool, str]] | None = None,
) -> bool:
    """Poll the Airflow health endpoint until it is healthy or ``timeout`` seconds pass."""
    probe = probe or check_health
    deadline = time.monotonic() + timeout
    last_detail = ""
    info(f"waiting for {url} (up to {int(timeout)} s)")
    while True:
        healthy, detail = probe(url)
        if healthy:
            info("Airflow is healthy")
            return True
        if detail != last_detail:
            info(f"not ready yet: {detail}")
            last_detail = detail
        if time.monotonic() >= deadline:
            info(f"timed out after {int(timeout)} s waiting for {url}")
            return False
        time.sleep(interval)


def print_urls(*, worker: bool) -> None:
    """Show where the running stack can be reached."""
    print()
    print(f"  Airflow UI ......... {AIRFLOW_URL}  (admin/admin, read-only viewer/viewer)")
    print(f"  dag-healer page .... {AIRFLOW_URL}/plugin/healer")
    print("  Mailpit (alerts) ... http://localhost:8025")
    if worker:
        print("  healer worker ...... python scripts/tasks.py logs --follow healer-worker")
        print("                       (it waits for healer.yaml: python scripts/tasks.py init)")
    else:
        print("  healer worker ...... not started (python scripts/tasks.py up)")
    print()


# --------------------------------------------------------------------------- demo repository


def _git(git: str, repo: Path, *args: str) -> None:
    code, out = capture([git, "-C", str(repo), *args])
    if code != 0:
        raise TaskError(f"git {' '.join(args)} failed in {rel(repo)}:\n{out.strip()}")


def init_git_repo(git: str, repo: Path) -> None:
    """``git init -b main`` + local identity + the initial commit of everything in ``repo``."""
    code, _ = capture([git, "-C", str(repo), "init", "-q", "-b", "main"])
    if code != 0:  # git < 2.28 has no -b
        _git(git, repo, "init", "-q")
        _git(git, repo, "symbolic-ref", "HEAD", "refs/heads/main")
    _git(git, repo, "config", "user.name", DEMO_GIT_USER)
    _git(git, repo, "config", "user.email", DEMO_GIT_EMAIL)
    # Commit the bytes exactly as copied, so the container (Linux git) sees a clean tree.
    _git(git, repo, "config", "core.autocrlf", "false")
    _git(git, repo, "add", "-A")
    _git(git, repo, "commit", "-q", "--allow-empty", "-m", DEMO_COMMIT_MESSAGE)


def prepare_demo_repo() -> Path:
    """Create .demo/repo (git copy of example_existing_repo/) and .demo/logs if missing."""
    DEMO_LOGS.mkdir(parents=True, exist_ok=True)
    if (DEMO_REPO / ".git").exists():
        info(f"{rel(DEMO_REPO)} already exists (reset --yes recreates it)")
        return DEMO_REPO
    git = which("git", "Install git from https://git-scm.com/downloads.")
    DEMO_REPO.mkdir(parents=True, exist_ok=True)
    if EXAMPLE_REPO.is_dir():
        info(f"copying {rel(EXAMPLE_REPO)}/ to {rel(DEMO_REPO)}/")
        try:
            shutil.copytree(EXAMPLE_REPO, DEMO_REPO, dirs_exist_ok=True, ignore=COPY_IGNORE)
        except (OSError, shutil.Error) as exc:
            raise TaskError(f"could not copy the example repository: {exc}") from exc
    else:
        info(f"{rel(EXAMPLE_REPO)}/ does not exist yet: creating an empty dags/ folder")
    (DEMO_REPO / "dags").mkdir(exist_ok=True)
    init_git_repo(git, DEMO_REPO)
    info(f"{rel(DEMO_REPO)} is a git repository on branch main ('{DEMO_COMMIT_MESSAGE}')")
    return DEMO_REPO


# --------------------------------------------------------------------------- UI bundle


def parse_node_version(text: str) -> tuple[int, int, int] | None:
    """Parse ``node --version`` output such as ``v22.12.0``."""
    match = re.search(r"v?(\d+)\.(\d+)\.(\d+)", text)
    if not match:
        return None
    major, minor, patch = (int(part) for part in match.groups())
    return major, minor, patch


def node_version_supported(version: tuple[int, int, int]) -> bool:
    """Vite 8 needs Node ``^20.19.0 || >=22.12.0``."""
    major, minor, _ = version
    return (major == 20 and minor >= 19) or (major == 22 and minor >= 12) or major >= 23


def pnpm_command() -> list[str] | None:
    """``pnpm`` itself, or ``corepack pnpm`` (the version pinned in ui/package.json)."""
    pnpm = shutil.which("pnpm")
    if pnpm:
        return [pnpm]
    corepack = shutil.which("corepack")
    if corepack:
        return [corepack, "pnpm"]
    return None


def local_node_status() -> tuple[bool, str]:
    """Whether ui/ can be built with the local Node toolchain, and why not."""
    node = shutil.which("node")
    if not node:
        return False, "node is not installed"
    code, out = capture([node, "--version"])
    version = parse_node_version(out) if code == 0 else None
    if version is None:
        return False, f"cannot read the node version ({out.strip()})"
    shown = ".".join(str(part) for part in version)
    if not node_version_supported(version):
        return False, f"node {shown} is too old for Vite 8 (needs ^20.19 or >=22.12)"
    if pnpm_command() is None:
        return False, f"node {shown} found but neither pnpm nor corepack is available"
    return True, f"node {shown}"


def install_bundle(dist_dir: Path) -> Path:
    """Copy the built ``*.umd.cjs`` from ``dist_dir`` to src/dag_healer/api/static/main.umd.cjs.

    The standalone bundle of ``dist_dir`` is installed next to it when it was built
    (:func:`install_standalone_bundle`). Returns the path of the Airflow plugin bundle.
    """
    candidates = sorted(dist_dir.glob("*.umd.cjs")) or sorted(dist_dir.rglob("*.umd.cjs"))
    if not candidates:
        raise TaskError(f"no *.umd.cjs bundle was produced in {dist_dir}")
    chosen = next((path for path in candidates if path.name == "main.umd.cjs"), candidates[0])
    BUNDLE_PATH.parent.mkdir(parents=True, exist_ok=True)
    shutil.copyfile(chosen, BUNDLE_PATH)
    info(f"UI bundle written to {rel(BUNDLE_PATH)} ({BUNDLE_PATH.stat().st_size} bytes)")
    install_standalone_bundle(dist_dir)
    return BUNDLE_PATH


def install_standalone_bundle(dist_dir: Path) -> Path | None:
    """Replace src/dag_healer/api/static/standalone/ with ``dist_dir/standalone/``.

    The target is the ``standalone`` directory next to :data:`BUNDLE_PATH`, read at call time.
    It is replaced as a whole (copied into a temporary sibling first, then renamed), so no stale
    chunk of an earlier build survives. When ``dist_dir`` has no ``standalone/index.js`` (an
    older dist) the existing directory is left untouched and None is returned; otherwise the
    installed directory is returned.
    """
    source = dist_dir / STANDALONE_DIRNAME
    target = BUNDLE_PATH.parent / STANDALONE_DIRNAME
    if not (source / STANDALONE_ENTRY).is_file():
        info(
            f"no standalone UI bundle in {rel(dist_dir)} ({STANDALONE_DIRNAME}/{STANDALONE_ENTRY}): "
            f"{rel(target)}/ left as it is"
        )
        return None
    target.parent.mkdir(parents=True, exist_ok=True)
    staging = Path(tempfile.mkdtemp(prefix=f".{STANDALONE_DIRNAME}-", dir=target.parent))
    try:
        fresh = staging / STANDALONE_DIRNAME
        shutil.copytree(source, fresh)
        remove_tree(target)
        fresh.rename(target)
    finally:
        remove_tree(staging)
    files = [path for path in target.rglob("*") if path.is_file()]
    size = sum(path.stat().st_size for path in files)
    count = f"{len(files)} file" + ("" if len(files) == 1 else "s")
    info(f"standalone UI bundle written to {rel(target)}/ ({count}, {size} bytes)")
    return target


def build_ui_local() -> int:
    """Build both UI bundles with the local Node + pnpm and install them.

    ``pnpm build`` makes the Airflow plugin bundle and empties ui/dist/ first, so
    ``pnpm build:standalone`` (ui/dist/standalone/) runs after it.
    """
    pnpm = pnpm_command()
    if pnpm is None:
        raise TaskError("neither pnpm nor corepack is available")
    for step in (["install", "--frozen-lockfile"], ["build"], ["build:standalone"]):
        code = run([*pnpm, *step], cwd=UI_DIR)
        if code != 0:
            return code
    install_bundle(UI_DIR / "dist")
    return 0


def build_ui_docker() -> int:
    """Build both UI bundles in the node:22-alpine stage of docker/Dockerfile and install them.

    That stage runs ``pnpm build`` and then ``pnpm build:standalone``, like the local toolchain.
    """
    docker = ensure_docker_running()
    with tempfile.TemporaryDirectory(prefix="dag-healer-ui-") as tmp:
        code = run(
            [
                docker,
                "build",
                "--file",
                str(DOCKERFILE),
                "--target",
                "ui-export",
                "--output",
                f"type=local,dest={tmp}",
                str(ROOT),
            ]
        )
        if code != 0:
            return code
        install_bundle(Path(tmp))
    return 0


def build_ui(mode: str = "auto") -> int:
    """Build the UI bundles. ``mode``: "auto" (local Node if suitable, else Docker), "local", "docker".

    Both modes build both bundles: the Airflow plugin's and the standalone host's.
    """
    if mode not in {"auto", "local", "docker"}:
        raise TaskError(f"unknown UI build mode {mode!r}")
    if not (UI_DIR / "package.json").is_file():
        raise TaskError(f"{rel(UI_DIR)}/package.json not found: the UI sources are missing")
    if mode in {"auto", "local"}:
        usable, reason = local_node_status()
        if usable:
            info(f"building the UI with the local toolchain ({reason})")
            return build_ui_local()
        if mode == "local":
            raise TaskError(f"cannot build the UI locally: {reason}")
        info(f"local Node toolchain not usable ({reason}); building with Docker (node:22-alpine)")
    return build_ui_docker()


# --------------------------------------------------------------------------- commands


def cmd_prepare_demo_repo(args: argparse.Namespace, extra: list[str]) -> int:
    """Create .demo/repo and .demo/logs."""
    prepare_demo_repo()
    return 0


def cmd_build(args: argparse.Namespace, extra: list[str]) -> int:
    """Build the dag-healer/airflow:dev image (UI bundle + wheel + Airflow)."""
    cmd = compose_base() + ["build"]
    if args.no_cache:
        cmd.append("--no-cache")
    if args.pull:
        cmd.append("--pull")
    return run(cmd)


def cmd_up(args: argparse.Namespace, extra: list[str]) -> int:
    """Start the stack (the image is built automatically the first time)."""
    prepare_demo_repo()
    worker = not args.no_worker
    services = [*CORE_SERVICES, *([WORKER_SERVICE] if worker else [])]
    cmd = compose_base() + ["up", "--detach"]
    if args.build:
        cmd.append("--build")
    if not args.no_wait:
        cmd += ["--wait", "--wait-timeout", str(args.timeout)]
    code = run([*cmd, *services])
    if code != 0:
        info("docker compose up failed; see: python scripts/tasks.py logs airflow-init")
        return code
    if not args.no_wait and not wait_for_airflow(args.timeout):
        return 1
    print_urls(worker=worker)
    return 0


def cmd_down(args: argparse.Namespace, extra: list[str]) -> int:
    """Stop and remove the containers (``--volumes`` also drops the databases)."""
    cmd = compose_base(ALL_PROFILES) + ["down", "--remove-orphans"]
    if args.volumes:
        cmd.append("--volumes")
    return run(cmd)


def cmd_reset(args: argparse.Namespace, extra: list[str]) -> int:
    """Remove containers, volumes and .demo/ (asks for confirmation unless --yes)."""
    question = (
        "This stops the stack, deletes its volumes (Airflow and healer databases, healer home) "
        f"and deletes {rel(DEMO_DIR)}/. Continue?"
    )
    if not args.yes and not confirm(question):
        info("reset aborted (pass --yes to confirm non-interactively)")
        return 1
    code = run(compose_base(ALL_PROFILES) + ["down", "--volumes", "--remove-orphans"])
    if code != 0:
        return code
    remove_tree(DEMO_DIR)
    info(f"deleted {rel(DEMO_DIR)}/")
    return 0


def cmd_logs(args: argparse.Namespace, extra: list[str]) -> int:
    """Show service logs."""
    cmd = compose_base(ALL_PROFILES) + ["logs", "--tail", str(args.tail)]
    if args.follow:
        cmd.append("--follow")
    return run([*cmd, *args.services])


def cmd_ps(args: argparse.Namespace, extra: list[str]) -> int:
    """List the containers of the stack."""
    return run(compose_base(ALL_PROFILES) + ["ps", "--all"])


def cmd_shell(args: argparse.Namespace, extra: list[str]) -> int:
    """Open bash in a running service container."""
    return compose_exec(args.service, ["bash"], user="0" if args.root else None)


def cmd_airflow(args: argparse.Namespace, extra: list[str]) -> int:
    """Run the airflow CLI inside the api-server container."""
    return compose_exec(APISERVER, ["airflow", *extra])


def cmd_healer(args: argparse.Namespace, extra: list[str]) -> int:
    """Run the dag-healer CLI inside the api-server container."""
    return compose_exec(APISERVER, ["dag-healer", *extra])


def cmd_init(args: argparse.Namespace, extra: list[str]) -> int:
    """Run "dag-healer init" on the mounted repository."""
    return compose_exec(APISERVER, ["dag-healer", "init", "--repo", CONTAINER_REPO, *extra])


def cmd_doctor(args: argparse.Namespace, extra: list[str]) -> int:
    """Run "dag-healer doctor" inside healer-worker.

    The worker's container is where the test sandbox runs (the api-server has no engine socket),
    so its sandbox check is the one that counts.
    """
    return compose_exec(WORKER_SERVICE, ["dag-healer", "doctor", *extra])


def cmd_ui(args: argparse.Namespace, extra: list[str]) -> int:
    """Build the UI bundles into src/dag_healer/api/static/ (main.umd.cjs and standalone/)."""
    mode = "local" if args.local else "docker" if args.docker else "auto"
    return build_ui(mode)


def cmd_test(args: argparse.Namespace, extra: list[str]) -> int:
    """Run tests/unit with the host Python (no Airflow needed)."""
    targets = extra or ["tests/unit"]
    return run([host_python(), "-m", "pytest", *targets], cwd=ROOT)


def cmd_test_airflow(args: argparse.Namespace, extra: list[str]) -> int:
    """Run tests/airflow inside the image (compose profile "tests")."""
    if not args.no_build:
        code = run(compose_base() + ["build"])
        if code != 0:
            return code
    cmd = compose_base(["tests"]) + ["run", "--rm"]
    if not is_interactive():
        cmd.append("-T")
    cmd.append("tests")
    if extra:
        cmd += ["python", "-m", "pytest", "-p", "no:cacheprovider", "--rootdir=/opt/dag-healer", *extra]
    return run(cmd)


def _run_optional_script(name: str, extra: list[str], later: str) -> int:
    script = optional_script(name)
    if script is None:
        info(f"scripts/{name} does not exist yet: {later}")
        return 1
    return run([sys.executable, str(script), *extra], cwd=ROOT)


def cmd_demo(args: argparse.Namespace, extra: list[str]) -> int:
    """Run the end-to-end demo (scripts/demo.py; --help lists its options)."""
    return _run_optional_script(
        "demo.py",
        extra,
        "the scripted demo arrives in a later phase. Meanwhile: python scripts/tasks.py up, "
        "then trigger the DAGs from the Airflow UI.",
    )


def cmd_e2e(args: argparse.Namespace, extra: list[str]) -> int:
    """Run the end-to-end smoke test (scripts/e2e_smoke.py)."""
    return _run_optional_script("e2e_smoke.py", extra, "the e2e smoke test arrives in a later phase.")


def cmd_wait(args: argparse.Namespace, extra: list[str]) -> int:
    """Wait until the Airflow API reports a healthy metadatabase and scheduler."""
    return 0 if wait_for_airflow(args.timeout) else 1


# --------------------------------------------------------------------------- CLI

# Commands whose unknown arguments are forwarded to the underlying tool.
FORWARDING = {"airflow", "healer", "init", "doctor", "test", "test-airflow", "demo", "e2e"}
# Commands that take everything after their name verbatim (even --help).
VERBATIM = {"airflow", "healer", "demo", "e2e"}


def build_parser() -> argparse.ArgumentParser:
    """Create the argparse parser with one subcommand per task."""
    parser = argparse.ArgumentParser(
        prog="python scripts/tasks.py",
        description="Task runner for the dag-healer development stack (Docker + Airflow 3.3.2).",
        epilog="Commands marked '...' forward extra arguments, e.g. "
        "'python scripts/tasks.py airflow dags list' or 'python scripts/tasks.py test -k config'.",
        allow_abbrev=False,
    )
    sub = parser.add_subparsers(dest="command", metavar="<command>", required=True)

    def add(name: str, func: Callable[..., int], help_text: str, **kwargs: Any) -> argparse.ArgumentParser:
        sub_parser = sub.add_parser(name, help=help_text, description=help_text, allow_abbrev=False, **kwargs)
        sub_parser.set_defaults(func=func)
        return sub_parser

    add("prepare-demo-repo", cmd_prepare_demo_repo, "copy example_existing_repo/ to .demo/repo (git init)")

    p = add("build", cmd_build, "build the dag-healer/airflow:dev image")
    p.add_argument("--no-cache", action="store_true", help="do not use the build cache")
    p.add_argument("--pull", action="store_true", help="pull newer base images first")

    p = add("up", cmd_up, "start the stack and wait until Airflow is healthy")
    p.add_argument("--build", action="store_true", help="rebuild the image before starting")
    group = p.add_mutually_exclusive_group()
    group.add_argument(
        "--worker", action="store_true", help="start healer-worker (the default; kept for compatibility)"
    )
    group.add_argument("--no-worker", action="store_true", help="do not start healer-worker")
    p.add_argument("--no-wait", action="store_true", help="return without waiting for health")
    p.add_argument("--timeout", type=int, default=DEFAULT_TIMEOUT, help="seconds to wait (default 600)")

    p = add("down", cmd_down, "stop and remove the containers")
    p.add_argument("--volumes", action="store_true", help="also delete the named volumes")

    for name in ("reset", "demo-reset"):
        p = add(name, cmd_reset, "delete containers, volumes and .demo/ (asks first)")
        p.add_argument("--yes", action="store_true", help="do not ask for confirmation")

    p = add("logs", cmd_logs, "show service logs")
    p.add_argument("services", nargs="*", help="services (default: all)")
    p.add_argument("-f", "--follow", action="store_true", help="follow the output")
    p.add_argument("--tail", type=int, default=200, help="lines per service (default 200)")

    add("ps", cmd_ps, "list the containers of the stack")

    p = add("shell", cmd_shell, "open bash in a service container")
    p.add_argument("service", nargs="?", default=APISERVER, help=f"service (default {APISERVER})")
    p.add_argument("--root", action="store_true", help="run as root")

    add("airflow", cmd_airflow, "run 'airflow ...' in the api-server", add_help=False)
    add("healer", cmd_healer, "run 'dag-healer ...' in the api-server", add_help=False)
    add("init", cmd_init, f"run 'dag-healer init --repo {CONTAINER_REPO} ...' in the api-server")
    add("doctor", cmd_doctor, f"run 'dag-healer doctor ...' in {WORKER_SERVICE} (it checks the test sandbox)")

    p = add(
        "ui",
        cmd_ui,
        "build the UI bundles into src/dag_healer/api/static/ (main.umd.cjs and standalone/)",
    )
    group = p.add_mutually_exclusive_group()
    group.add_argument("--local", action="store_true", help="force the local Node toolchain")
    group.add_argument("--docker", action="store_true", help="force the node:22-alpine Docker build")

    add("test", cmd_test, "run tests/unit with the host Python ...")
    p = add("test-airflow", cmd_test_airflow, "run tests/airflow in the image (profile 'tests') ...")
    p.add_argument("--no-build", action="store_true", help="do not rebuild the image first")

    add("demo", cmd_demo, "run the end-to-end demo (scripts/demo.py) ...", add_help=False)
    add("e2e", cmd_e2e, "run the e2e smoke test (scripts/e2e_smoke.py) ...", add_help=False)

    p = add("wait", cmd_wait, f"wait for {HEALTH_URL}")
    p.add_argument("--timeout", type=int, default=DEFAULT_TIMEOUT, help="seconds to wait (default 600)")
    return parser


def parse(argv: Sequence[str]) -> tuple[argparse.Namespace, list[str]]:
    """Parse ``argv`` into the namespace and the arguments to forward."""
    parser = build_parser()
    argv = list(argv)
    if argv and argv[0] in VERBATIM:
        return parser.parse_args(argv[:1]), argv[1:]
    args, extra = parser.parse_known_args(argv)
    if extra and args.command not in FORWARDING:
        parser.error(f"unrecognized arguments: {' '.join(extra)}")
    if extra and extra[0] == "--":
        extra = extra[1:]
    return args, extra


def main(argv: Sequence[str] | None = None) -> int:
    """Entry point: run one task and return its exit code."""
    args, extra = parse(sys.argv[1:] if argv is None else argv)
    try:
        return int(args.func(args, extra) or 0)
    except TaskError as exc:
        print(f"error: {exc}", file=sys.stderr, flush=True)
        return exc.code
    except KeyboardInterrupt:
        print("interrupted", file=sys.stderr, flush=True)
        return 130


if __name__ == "__main__":
    sys.exit(main())
