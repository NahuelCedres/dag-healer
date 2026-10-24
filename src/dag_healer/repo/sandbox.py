"""Where pytest runs during a repair: the test sandbox (``repo.sandbox`` in healer.yaml).

A repair runs ``python -m pytest`` several times: the repository's own suite, the generated
test before the fix, the test and the suite after each fix attempt, and every run the agent
asks for with its ``run_tests`` tool. That is code nobody has reviewed yet, so it runs in the
sandbox that ``repo.sandbox.backend`` names (:func:`build_sandbox` never picks one by itself):

* ``container`` (the default, :class:`ContainerSandbox`): a fresh container per run, started
  through the ``docker`` or ``podman`` CLI;
* ``process`` (:class:`ProcessSandbox`): a child process of the worker, as the worker's OS
  user, with its files, network and kernel view. It is a guard against accidents (a process
  group killed at the timeout, lowered resource limits on POSIX), not isolation: development
  and tests only (:data:`PROCESS_BACKEND_WARNING`).

Whatever the backend, every run works on a throw-away copy of the clone without its ``.git``
folder, so test code can never change the clone that the worker commits and pushes, nor its
git configuration and hooks. The copy never follows a symbolic link: only relative links that
stay inside the tree are copied, as links; absolute or escaping links, sockets, FIFOs and
devices are left out, and a linked folder is never entered (:func:`copy_workspace`,
:func:`write_workspace_tar`). Repository tests that need git therefore fail in the sandbox.

The container backend (:meth:`ContainerSandbox.run`):

* **No host path is mounted.** The copy travels as a tar stream on the container's stdin
  (``<engine> run -i``); a small bootstrap (:data:`BOOTSTRAP`, run by the image's own Python)
  unpacks it into a tmpfs at ``/work`` and execs pytest there. This works with a remote or
  rootless engine, and when the worker itself runs in a container (a host path would resolve on
  the engine's host).
* **Flags:** ``--rm``, ``--network none``, a read-only root file system with tmpfs at ``/work``
  and ``/tmp`` (where ``HOME``, ``TMPDIR`` and ``AIRFLOW_HOME`` of the tests live),
  ``--cap-drop ALL``, ``--security-opt no-new-privileges``, ``--pids-limit``, ``--memory``
  without swap, ``--cpus``, ``--init``, ``--pull never`` and an ``--entrypoint`` override.
* **User:** the image's own ``USER`` unless ``repo.sandbox.user`` is set. Root is refused before
  the run (the image's ``Config.User``) and again inside the container by the bootstrap. The
  bootstrap keeps the interpreter's user site-packages (``PYTHONUSERBASE``) visible when it moves
  ``HOME`` to the tmpfs, so images that install packages with ``pip install --user`` keep them.
* **Environment:** variables travel by name only (``-e NAME``; the value sits in the CLI
  process's own environment, never on its command line). The container gets the scrubbed set
  that :func:`dag_healer.repo.pytest_runner.run_pytest` builds, without the host's paths and
  settings and with container paths for ``AIRFLOW_HOME`` and its database
  (:func:`container_environment`); the proxy variables are passed empty, so the CLI cannot inject
  the proxies of its ``config.json``, which may hold credentials. The CLI process gets only what
  the engine CLI needs (:func:`engine_environment`) on top: the two sets never share a name, so no
  ``DOCKER_*`` setting and no credential reaches a container.
* **Start marker:** the bootstrap writes :data:`BOOTSTRAP_MARKER` to stderr before anything else.
  A run is a test result only when the marker appeared on the CLI's stderr, the exit code is
  not :data:`BOOTSTRAP_FAILED_EXIT_CODE`, and the CLI did not report an engine error of its own
  afterwards (:data:`ENGINE_ERROR_EXIT_CODES` with an error line, e.g. when the engine goes away
  mid-run). Everything else (an unreachable engine, a missing image, a root user, pytest missing
  from the image, a workspace that could not be sent) is a launch failure
  (:attr:`SandboxRun.launch_error`), never a test result: exit codes alone cannot tell them apart
  (docker exits 1 when its daemon is unreachable, like pytest with failing tests).
* **Timeouts, output and leftovers:** at the timeout, or once the run's output passes
  :data:`MAX_OUTPUT_BYTES`, the container is killed and removed by its unique name (killing the
  CLI alone would leave the container running). Every container carries the label
  :data:`CONTAINER_LABEL` and its expiry time in its name, so the containers of a crashed worker
  are removed by a later run (:meth:`ContainerSandbox.remove_expired`) without touching the live
  containers of another worker.

Callers check :meth:`Sandbox.unavailable_reason` before a repair runs any code (it runs no
repository code itself), mask the texts of this module before they store or show them, and wrap
them with :func:`sandbox_refusal` / :func:`sandbox_launch_failure`.

Nothing in this module imports Airflow, an orchestrator, the CLI or the configuration: it reads
the ``repo.sandbox`` settings through the :class:`SandboxSettings` protocol.
"""

from __future__ import annotations

import json
import logging
import os
import posixpath
import re
import secrets
import shutil
import stat
import subprocess
import sys
import tarfile
import tempfile
import threading
import time
from collections.abc import Iterator, Mapping, Sequence
from dataclasses import dataclass
from pathlib import Path
from typing import IO, Any, Protocol, runtime_checkable

from dag_healer.repo.git import remove_tree

log = logging.getLogger(__name__)

#: The values of ``repo.sandbox.backend`` and ``repo.sandbox.engine``.
SANDBOX_BACKENDS: tuple[str, ...] = ("container", "process")
SANDBOX_ENGINES: tuple[str, ...] = ("docker", "podman")

#: ``exit_code`` of a run killed by the timeout.
TIMEOUT_EXIT_CODE = 124
#: ``exit_code`` of a run that produced no test result (pytest's "internal error" code, so
#: ``PytestResult.collection_error`` is True for callers that only look at the exit code).
LAUNCH_FAILED_EXIT_CODE = 3
#: Exit code of the in-container bootstrap when it refused to run or failed (internal).
BOOTSTRAP_FAILED_EXIT_CODE = 121
#: First stderr line of the bootstrap: the container started and the bootstrap runs (internal).
BOOTSTRAP_MARKER = "dag-healer sandbox: started"
#: Prefix of the bootstrap's error lines on stderr (internal).
BOOTSTRAP_ERROR_PREFIX = "dag-healer sandbox: error: "
#: Exit codes of the engine CLI itself (docker and podman): an error of the engine, never of
#: pytest, when the CLI also printed an error.
ENGINE_ERROR_EXIT_CODES = frozenset({125, 126, 127})
#: ``exit_code`` of a container killed by the engine or by the sandbox (128 + SIGKILL): out of
#: memory, or past :data:`MAX_OUTPUT_BYTES`.
KILLED_EXIT_CODE = 137

#: Label (value ``1``) and name prefix of every sandbox container.
CONTAINER_LABEL = "dag-healer.sandbox"
CONTAINER_NAME_PREFIX = "dag-healer-sandbox-"
#: Paths inside the container: the tmpfs that receives the copy, and the tmpfs at ``/tmp`` that
#: holds ``HOME``, ``TMPDIR`` and ``AIRFLOW_HOME`` of the tests.
CONTAINER_WORKDIR = "/work"
CONTAINER_TMP = "/tmp"
CONTAINER_HOME = "/tmp/home"
CONTAINER_AIRFLOW_HOME = "/tmp/airflow_home"

#: Seconds allowed to ``<engine> version`` / ``image inspect`` / ``ps``, to the doctor's smoke
#: run, and to ``<engine> kill`` / ``rm -f``.
PROBE_TIMEOUT = 30.0
SMOKE_TIMEOUT = 120.0
KILL_TIMEOUT = 30.0
#: A sandbox container is removed by later runs once this long past its own timeout.
CONTAINER_GRACE_SECONDS = 60
#: A container run is killed once its output (stdout and stderr together) passes this size, as
#: :data:`RLIMIT_FSIZE_BYTES` stops a run of the ``process`` backend: the runner keeps only the end
#: of the output, and the engine may keep a copy of it in its log until the container is removed.
MAX_OUTPUT_BYTES = 256 * 1024**2
#: Seconds between two checks of the output size of a running container.
OUTPUT_CHECK_SECONDS = 0.5
#: At most one sweep for expired containers per engine and process in this many seconds.
SWEEP_INTERVAL = 300.0
#: Where the smoke run's probe tries an outbound TCP connection (it must fail).
PROBE_ADDRESS: tuple[str, int] = ("1.1.1.1", 53)

#: Size of the writable /dev/shm of a sandbox container.
SHM_SIZE = "64m"

#: POSIX resource limits of the ``process`` backend (lowered in the child, never raised).
RLIMIT_AS_BYTES = 8 * 1024**3
RLIMIT_NPROC_COUNT = 4096
RLIMIT_FSIZE_BYTES = 1024**3

#: Variables of the test environment that describe the worker's host, never a container
#: (compared upper-cased). The engine CLI's own variables are among them.
HOST_ONLY_ENV = frozenset(
    (
        "_ SHLVL PATH HOME PWD OLDPWD SHELL USER USERNAME LOGNAME HOSTNAME TERM TMPDIR TEMP TMP DISPLAY LANG "
        "LANGUAGE PYTHONHOME PYTHONUSERBASE PYTHONNOUSERSITE PYTHONEXECUTABLE PYTHONPLATLIBDIR "
        "PYTHONPYCACHEPREFIX VIRTUAL_ENV LD_PRELOAD LD_LIBRARY_PATH SSH_AUTH_SOCK "
        # Windows
        "USERPROFILE APPDATA LOCALAPPDATA PROGRAMDATA PROGRAMFILES PROGRAMW6432 COMMONPROGRAMFILES "
        "COMMONPROGRAMW6432 ALLUSERSPROFILE PUBLIC SYSTEMROOT SYSTEMDRIVE WINDIR COMSPEC PATHEXT "
        "HOMEDRIVE HOMEPATH COMPUTERNAME USERDOMAIN USERDOMAIN_ROAMINGPROFILE LOGONSERVER SESSIONNAME "
        "PSMODULEPATH ONEDRIVE DRIVERDATA OS NUMBER_OF_PROCESSORS PROCESSOR_ARCHITECTURE "
        "PROCESSOR_IDENTIFIER PROCESSOR_LEVEL PROCESSOR_REVISION"
    ).split()
)
#: Prefixes of host-only variables (compared upper-cased).
HOST_ONLY_PREFIXES: tuple[str, ...] = (
    "LC_",
    "XDG_",
    "DOCKER_",
    "CONTAINER_",
    "CONTAINERS_",
    "PODMAN_",
    "BUILDKIT_",
    "COMPOSE_",
    "DYLD_",
    "CONDA_",
    "__",
)
#: Variables the engine CLI process gets from the worker's environment (compared upper-cased),
#: besides every ``DOCKER_*`` and ``CONTAINERS_*CONF`` variable.
ENGINE_ENV = frozenset(
    (
        "PATH HOME USERPROFILE APPDATA LOCALAPPDATA SYSTEMROOT SYSTEMDRIVE WINDIR COMSPEC PATHEXT TEMP TMP "
        "TMPDIR LANG SSH_AUTH_SOCK XDG_RUNTIME_DIR XDG_CONFIG_HOME XDG_DATA_HOME CONTAINER_HOST "
        "CONTAINER_CONNECTION CONTAINER_SSHKEY"
    ).split()
)
ENGINE_ENV_PREFIXES: tuple[str, ...] = ("DOCKER_",)
#: Passed empty to every container (upper case, and lower case off Windows), so the engine CLI
#: does not add the proxies of its ``config.json``.
PROXY_VARIABLES: tuple[str, ...] = ("HTTP_PROXY", "HTTPS_PROXY", "FTP_PROXY", "ALL_PROXY", "NO_PROXY")

PROCESS_BACKEND_WARNING = (
    "repo.sandbox.backend is 'process': the repository's tests and the model-written test and fix "
    "run without isolation, as the worker's OS user with its files, network and environment "
    "(development and tests only; docs/security.md, section 4.3)"
)

_IMAGE_RE = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._/:@-]*$")
_PYTHON_RE = re.compile(r"^[A-Za-z0-9_./+-]+$")
_USER_RE = re.compile(r"^[A-Za-z0-9_][A-Za-z0-9_.-]*(:[A-Za-z0-9_][A-Za-z0-9_.-]*)?$")
_SIZE_RE = re.compile(r"^[1-9][0-9]*[kmg]?$")
_ENV_NAME_RE = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")
_CONTAINER_NAME_RE = re.compile(rf"^{re.escape(CONTAINER_NAME_PREFIX)}([0-9]+)-[0-9a-f]{{8}}$")
_WINDOWS_ABSOLUTE_RE = re.compile(r"^[A-Za-z]:")
#: Characters kept of an engine's error output in a launch failure / an availability reason.
_MAX_LAUNCH_DETAIL = 500
_MAX_REASON_DETAIL = 300


def sandbox_refusal(reason: str) -> str:
    """The error of a repair that ran no code because the sandbox is not usable."""
    return (
        f"The test sandbox is not usable: {reason}. No code was run: neither the repository's tests "
        "nor the generated test or fix. Configure repo.sandbox (`dag-healer doctor` checks it), then "
        "retry the repair."
    )


def sandbox_launch_failure(error: str) -> str:
    """The error of a repair stopped because the sandbox could not run pytest."""
    return (
        f"The test sandbox could not run pytest: {error}. The repair stopped; check repo.sandbox "
        "with `dag-healer doctor`, then retry the repair."
    )


# --------------------------------------------------------------------------- data types


@dataclass(frozen=True)
class SandboxJob:
    """One pytest run: what to run, where, and where its output goes."""

    workdir: Path  # the clone; a run never changes it
    args: tuple[str, ...]  # pytest arguments after '-m pytest'
    env: Mapping[str, str]  # scrubbed environment built by run_pytest
    timeout: float  # seconds before the run is killed
    output: Path  # host file that receives pytest's stdout+stderr
    scratch: Path  # private host directory of this run (deleted by the caller)
    python: str | None = None  # None: sys.executable (process) / settings.python (container)


@dataclass(frozen=True)
class SandboxRun:
    """How a run ended: pytest's exit code, the timeout, or a launch failure (no test result)."""

    exit_code: int
    timed_out: bool = False
    launch_error: str | None = None
    command: tuple[str, ...] = ()  # the pytest argv as run inside the sandbox


@dataclass(frozen=True)
class SandboxProbe:
    """Result of :meth:`Sandbox.smoke_test`: whether the sandbox works, and what was seen."""

    ok: bool
    detail: str


class SandboxSettings(Protocol):
    """The ``repo.sandbox`` settings as the sandbox reads them (``config.SandboxConfig``)."""

    backend: str
    engine: str
    image: str | None
    python: str
    user: str | None
    memory: str
    cpus: float | None
    pids: int
    work_size: str
    tmp_size: str


@runtime_checkable
class Sandbox(Protocol):
    """Where pytest runs (``repo.sandbox.backend``); small so other backends can follow."""

    name: str  # 'container' | 'process'
    isolated: bool  # False for 'process'

    def unavailable_reason(self) -> str | None:
        """Why no run can happen now (``None``: usable). Runs no repository code."""
        ...

    def smoke_test(self) -> SandboxProbe:
        """Start the sandbox once without a workspace and check what it isolates."""
        ...

    def run(self, job: SandboxJob) -> SandboxRun:
        """Run ``python -m pytest <job.args>`` in a throw-away copy of ``job.workdir``; never raises."""
        ...


# --------------------------------------------------------------------------- workspace copy


@dataclass(frozen=True)
class _Entry:
    """One entry of the workspace copy (``rel``: relative posix path)."""

    rel: str
    kind: str  # "dir" | "file" | "link"
    path: str  # the entry on the host
    mode: int  # st_mode of lstat
    mtime: int
    target: str = ""  # link target (posix, relative)
    directory_link: bool = False  # Windows: a link to a folder


def _is_git_name(name: str) -> bool:
    return name.lower() == ".git"


def _inside_link_target(rel: str, raw: str) -> str | None:
    """The target of the link ``rel`` when it is relative and stays inside the tree, else None."""
    target = raw.replace("\\", "/") if os.name == "nt" else raw
    if not target or "\0" in target or target.startswith("/") or _WINDOWS_ABSOLUTE_RE.match(target):
        return None
    resolved = posixpath.normpath(posixpath.join(posixpath.dirname(rel), target))
    if resolved == ".." or resolved.startswith("../"):
        return None
    return target


def _walk_workspace(root: Path) -> Iterator[_Entry]:
    """Yield what the throw-away copy of ``root`` contains, parents before their children.

    Never follows a symbolic link (``lstat`` only; a linked folder is never entered), never
    enters or copies a ``.git`` folder or file (at any depth, any case), keeps only relative links
    that stay inside the tree, and leaves out sockets, FIFOs and devices.
    """
    pending: list[tuple[str, str]] = [("", os.fspath(root))]
    while pending:
        rel_dir, abs_dir = pending.pop()
        with os.scandir(abs_dir) as scan:
            entries = sorted(scan, key=lambda item: item.name)
        folders: list[tuple[str, str]] = []
        for entry in entries:
            if _is_git_name(entry.name):
                continue
            rel = f"{rel_dir}/{entry.name}" if rel_dir else entry.name
            info = entry.stat(follow_symlinks=False)
            attributes = getattr(info, "st_file_attributes", 0)
            if stat.S_ISLNK(info.st_mode):
                target = _inside_link_target(rel, os.readlink(entry.path))
                if target is None:
                    log.debug("workspace copy: link %s left out (absolute or outside the tree)", rel)
                    continue
                yield _Entry(
                    rel,
                    "link",
                    entry.path,
                    info.st_mode,
                    int(info.st_mtime),
                    target,
                    directory_link=os.name == "nt" and bool(attributes & stat.FILE_ATTRIBUTE_DIRECTORY),
                )
            elif stat.S_ISDIR(info.st_mode):
                if attributes & stat.FILE_ATTRIBUTE_REPARSE_POINT:  # a Windows junction: never entered
                    log.debug("workspace copy: %s left out (a junction)", rel)
                    continue
                yield _Entry(rel, "dir", entry.path, info.st_mode, int(info.st_mtime))
                folders.append((rel, entry.path))
            elif stat.S_ISREG(info.st_mode):
                yield _Entry(rel, "file", entry.path, info.st_mode, int(info.st_mtime))
            else:
                log.debug("workspace copy: %s left out (not a file, a folder or a link)", rel)
        pending.extend(reversed(folders))


def _open_regular(path: str) -> tuple[int, os.stat_result] | None:
    """Open a regular file without following a link; ``None`` when it is no longer one."""
    flags = os.O_RDONLY | getattr(os, "O_NOFOLLOW", 0) | getattr(os, "O_NONBLOCK", 0)
    flags |= getattr(os, "O_BINARY", 0)
    try:
        fd = os.open(path, flags)
    except OSError as exc:
        if os.path.islink(path):  # replaced by a link since the walk (ELOOP / EMLINK)
            log.debug("workspace copy: %s left out (it became a link): %s", path, exc)
            return None
        raise
    info = os.fstat(fd)
    if not stat.S_ISREG(info.st_mode):
        os.close(fd)
        log.debug("workspace copy: %s left out (no longer a regular file)", path)
        return None
    return fd, info


def _file_mode(st_mode: int) -> int:
    """0o755 when any execute bit is set, else 0o644 (synthetic modes on Windows)."""
    return 0o755 if st_mode & 0o111 else 0o644


def copy_workspace(root: str | os.PathLike[str], dest: str | os.PathLike[str]) -> None:
    """Copy the tree at ``root`` into the empty or missing folder ``dest`` (see :func:`_walk_workspace`).

    Never writes to ``root``. A link that cannot be created (Windows without the privilege) is
    left out.

    Raises:
        OSError: ``dest`` is not empty, or a file could not be read or written.
    """
    target = Path(dest)
    target.mkdir(parents=True, exist_ok=True)
    if any(target.iterdir()):
        raise FileExistsError(f"the workspace copy destination is not empty: {target}")
    for entry in _walk_workspace(Path(root)):
        out = target.joinpath(*entry.rel.split("/"))
        if entry.kind == "dir":
            out.mkdir()
        elif entry.kind == "link":
            link = entry.target.replace("/", "\\") if os.name == "nt" else entry.target
            try:
                os.symlink(link, out, target_is_directory=entry.directory_link)
            except (OSError, NotImplementedError) as exc:
                if os.name != "nt":
                    raise
                log.debug("workspace copy: link %s left out: %s", entry.rel, exc)
        else:
            opened = _open_regular(entry.path)
            if opened is None:
                continue
            fd, info = opened
            flags = os.O_WRONLY | os.O_CREAT | os.O_EXCL | getattr(os, "O_BINARY", 0)
            with os.fdopen(fd, "rb") as source, os.fdopen(os.open(out, flags, 0o600), "wb") as sink:
                shutil.copyfileobj(source, sink, 1024 * 1024)
            os.chmod(out, _file_mode(info.st_mode))


def write_workspace_tar(root: str | os.PathLike[str], stream: IO[bytes]) -> None:
    """Write the copy of the tree at ``root`` to ``stream`` as a PAX tar stream.

    Relative posix names, owner 0:0 without names, modes 0o755 / 0o644, links as links (see
    :func:`_walk_workspace`). ``stream`` is not closed. Never writes to ``root``.
    """
    with tarfile.open(fileobj=stream, mode="w|", format=tarfile.PAX_FORMAT) as archive:
        for entry in _walk_workspace(Path(root)):
            info = tarfile.TarInfo(entry.rel)
            info.uid = info.gid = 0
            info.uname = info.gname = ""
            info.mtime = entry.mtime
            if entry.kind == "dir":
                info.type = tarfile.DIRTYPE
                info.mode = 0o755
                archive.addfile(info)
            elif entry.kind == "link":
                info.type = tarfile.SYMTYPE
                info.linkname = entry.target
                info.mode = 0o777
                archive.addfile(info)
            else:
                opened = _open_regular(entry.path)
                if opened is None:
                    continue
                fd, stat_result = opened
                with os.fdopen(fd, "rb") as source:
                    info.size = stat_result.st_size
                    info.mode = _file_mode(stat_result.st_mode)
                    archive.addfile(info, source)


# --------------------------------------------------------------------------- environment


def _is_host_only(name: str) -> bool:
    upper = name.upper()
    return upper in HOST_ONLY_ENV or upper.startswith(HOST_ONLY_PREFIXES)


def _is_engine_variable(name: str) -> bool:
    upper = name.upper()
    if upper in ENGINE_ENV or upper.startswith(ENGINE_ENV_PREFIXES):
        return True
    return upper.startswith("CONTAINERS_") and upper.endswith("CONF")


def _proxy_names() -> tuple[str, ...]:
    if os.name == "nt":  # one name per variable: Windows environment names ignore case
        return PROXY_VARIABLES
    return (*PROXY_VARIABLES, *(name.lower() for name in PROXY_VARIABLES))


def container_environment(env: Mapping[str, str]) -> dict[str, str]:
    """The environment a container gets from the scrubbed test environment ``env``.

    Drops the host-only names (:data:`HOST_ONLY_ENV`, :data:`HOST_ONLY_PREFIXES`, compared
    case-insensitively) and names that are not identifiers, points ``AIRFLOW_HOME`` and its SQLite
    database at :data:`CONTAINER_AIRFLOW_HOME`, and sets every proxy variable to ``""``.
    """
    fixed = {
        "AIRFLOW_HOME": CONTAINER_AIRFLOW_HOME,
        "AIRFLOW__DATABASE__SQL_ALCHEMY_CONN": f"sqlite:///{CONTAINER_AIRFLOW_HOME}/unittests.db",
        **dict.fromkeys(_proxy_names(), ""),
    }
    replaced = {name.upper() for name in fixed}
    result = {
        name: str(value)
        for name, value in env.items()
        if _ENV_NAME_RE.match(name) and not _is_host_only(name) and name.upper() not in replaced
    }
    result.update(fixed)
    return result


def engine_environment(base: Mapping[str, str] | None = None) -> dict[str, str]:
    """The variables the engine CLI process gets from ``base`` (default ``os.environ``).

    Only what the CLI needs to find and reach its engine (:data:`ENGINE_ENV`, ``DOCKER_*``,
    ``CONTAINERS_*CONF``). None of these names is ever passed to a container.
    """
    source = os.environ if base is None else base
    return {name: value for name, value in source.items() if _is_engine_variable(name)}


def engine_command(engine: str) -> list[str]:
    """The command that runs the engine CLI (``[engine]``, looked up on ``PATH``)."""
    return [engine]


# --------------------------------------------------------------------------- the bootstrap

_BOOTSTRAP_TEMPLATE = r"""
import json
import os
import platform
import posixpath
import site
import subprocess
import sys
import tarfile

MARKER = @MARKER@
ERROR = @ERROR@
FAILED = @FAILED@
PROBE_ADDRESS = @PROBE_ADDRESS@


def fail(message):
    sys.stderr.write(ERROR + message + "\n")
    sys.stderr.flush()
    sys.exit(FAILED)


def inside(root, path):
    return path == root or path.startswith(root.rstrip(os.sep) + os.sep)


def checked(archive, root):
    for member in archive:
        name = member.name
        parts = name.replace("\\", "/").split("/")
        absolute = name.startswith(("/", "\\")) or os.path.isabs(name) or os.path.splitdrive(name)[0]
        if not name or absolute or ".." in parts:
            raise ValueError("unsafe name %r" % name)
        if not (member.isreg() or member.isdir() or member.issym()):
            raise ValueError("unsupported entry %r" % name)
        if member.issym():
            link = member.linkname
            joined = posixpath.normpath(posixpath.join(posixpath.dirname(name), link))
            if not link or link.startswith("/") or joined == ".." or joined.startswith("../"):
                raise ValueError("unsafe link %r -> %r" % (name, link))
        if not inside(root, os.path.realpath(os.path.join(root, name))):
            raise ValueError("%r is outside the work directory" % name)
        member.mode &= 0o755
        yield member


def unpack(work):
    root = os.path.realpath(work)
    with tarfile.open(fileobj=sys.stdin.buffer, mode="r|") as archive:
        if hasattr(tarfile, "data_filter"):
            archive.extractall(root, members=checked(archive, root), filter="data")
        else:
            archive.extractall(root, members=checked(archive, root))


def probe():
    import socket

    info = {"python": platform.python_version()}
    info["uid"] = os.geteuid() if hasattr(os, "geteuid") else None
    try:
        import pwd

        info["user"] = pwd.getpwuid(os.geteuid()).pw_name
    except Exception:
        info["user"] = None
    try:
        info["interfaces"] = sorted(os.listdir("/sys/class/net"))
    except OSError:
        info["interfaces"] = None
    try:
        socket.create_connection(PROBE_ADDRESS, timeout=3).close()
        info["connect"] = "open"
    except OSError as exc:
        info["connect"] = "blocked: %s" % exc
    try:
        check = subprocess.run(
            [sys.executable, "-c", "import pytest; print(pytest.__version__)"],
            stdin=subprocess.DEVNULL,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            timeout=60,
        )
        version = check.stdout.decode("utf-8", "replace").strip()
        info["pytest"] = version if check.returncode == 0 and version else None
    except (OSError, subprocess.SubprocessError):
        info["pytest"] = None
    sys.stdout.write(json.dumps(info, sort_keys=True) + "\n")
    sys.stdout.flush()


def main():
    sys.stderr.write(MARKER + "\n")
    sys.stderr.flush()
    if len(sys.argv) < 5 or sys.argv[1] not in ("pytest", "probe"):
        fail("usage: pytest|probe <work> <home> <tmp> [pytest arguments]")
    mode, work, home, tmp = sys.argv[1:5]
    if hasattr(os, "geteuid") and os.geteuid() == 0:
        fail("refusing to run as root (uid 0); give the image a non-root USER or set repo.sandbox.user")
    # Packages installed with pip install --user stay importable once HOME moves to the tmpfs.
    os.environ.setdefault("PYTHONUSERBASE", site.getuserbase())
    for path in (home, tmp, os.environ.get("AIRFLOW_HOME", "")):
        if path:
            os.makedirs(path, exist_ok=True)
    os.environ["HOME"] = home
    os.environ["TMPDIR"] = tmp
    if mode == "probe":
        probe()
        return
    import importlib.util

    if importlib.util.find_spec("pytest") is None:
        fail(
            "pytest is not importable in the image (%s, Python %s); install pytest and the "
            "repository's requirements in it" % (sys.executable, platform.python_version())
        )
    try:
        unpack(work)
    except (tarfile.TarError, OSError, ValueError, EOFError) as exc:
        fail("could not unpack the workspace: %s" % exc)
    os.chdir(work)
    command = [sys.executable, "-m", "pytest"] + sys.argv[5:]
    null = os.open(os.devnull, os.O_RDONLY)
    os.dup2(null, 0)
    sys.stdout.flush()
    sys.stderr.flush()
    if os.name == "posix":
        os.dup2(1, 2)
        os.execv(sys.executable, command)
    sys.exit(subprocess.call(command, stdin=subprocess.DEVNULL, stderr=subprocess.STDOUT))


main()
"""


def _bootstrap_source(probe_address: tuple[str, int] = PROBE_ADDRESS) -> str:
    """The bootstrap's source with this module's constants filled in."""
    return (
        _BOOTSTRAP_TEMPLATE.replace("@MARKER@", repr(BOOTSTRAP_MARKER))
        .replace("@ERROR@", repr(BOOTSTRAP_ERROR_PREFIX))
        .replace("@FAILED@", str(BOOTSTRAP_FAILED_EXIT_CODE))
        .replace("@PROBE_ADDRESS@", repr(tuple(probe_address)))
        .lstrip("\n")
    )


#: Run as ``<python> -c BOOTSTRAP pytest|probe <work> <home> <tmp> [pytest arguments]`` by the
#: image's own interpreter (Python >= 3.8; never with ``-I``, ``-s`` or ``-E``, which would hide
#: the user site-packages). It writes :data:`BOOTSTRAP_MARKER` to stderr, refuses uid 0, keeps
#: ``PYTHONUSERBASE`` before it moves ``HOME`` and ``TMPDIR`` to the tmpfs, creates
#: ``AIRFLOW_HOME``, and then either prints the probe's JSON line (``probe``) or checks that pytest
#: is importable, unpacks the tar on stdin into ``<work>`` (refusing absolute names, ``..``, links
#: that leave the tree and anything but files, folders and links; tarfile's ``data`` filter on top
#: when the interpreter has it), points stdin at ``/dev/null``, merges stderr into stdout and
#: execs ``python -m pytest`` there. A refusal or failure is one ``dag-healer sandbox: error:``
#: line on stderr and exit code :data:`BOOTSTRAP_FAILED_EXIT_CODE`.
BOOTSTRAP = _bootstrap_source()


# --------------------------------------------------------------------------- the process backend


def _apply_limits() -> None:  # pragma: no cover - runs in the forked child (POSIX only)
    """preexec_fn: lower RLIMIT_AS / RLIMIT_NPROC / RLIMIT_FSIZE in the child."""
    import resource

    wanted = [
        (getattr(resource, "RLIMIT_AS", None), RLIMIT_AS_BYTES),
        (getattr(resource, "RLIMIT_NPROC", None), RLIMIT_NPROC_COUNT),
        (getattr(resource, "RLIMIT_FSIZE", None), RLIMIT_FSIZE_BYTES),
    ]
    for limit, value in wanted:
        if limit is None:
            continue
        try:
            soft, hard = resource.getrlimit(limit)
            new_soft = value
            if hard != resource.RLIM_INFINITY:
                new_soft = min(new_soft, hard)
            if soft != resource.RLIM_INFINITY:
                new_soft = min(new_soft, soft)
            resource.setrlimit(limit, (new_soft, hard))
        except (ValueError, OSError):
            pass


def _popen_kwargs() -> dict[str, Any]:
    if os.name == "posix":
        return {"start_new_session": True, "preexec_fn": _apply_limits}
    return {}


def _kill_tree(proc: subprocess.Popen[bytes]) -> None:
    """Kill the pytest process and everything it started."""
    if os.name == "posix":
        import signal

        try:
            os.killpg(proc.pid, signal.SIGKILL)
        except (ProcessLookupError, PermissionError):
            pass
    else:  # Windows: best effort
        try:
            subprocess.run(
                ["taskkill", "/F", "/T", "/PID", str(proc.pid)],
                stdin=subprocess.DEVNULL,
                capture_output=True,
                timeout=30,
                check=False,
            )
        except (OSError, subprocess.SubprocessError):
            pass
    try:
        proc.kill()
    except OSError:
        pass


class ProcessSandbox:
    """pytest as a child process of the worker, in a throw-away copy of the clone.

    No isolation beyond that copy: the run has the worker's OS user, files, network and kernel
    view (:data:`PROCESS_BACKEND_WARNING`). The process group is killed at the timeout (POSIX;
    ``taskkill /T`` on Windows) and the POSIX resource limits are lowered in the child.
    """

    name = "process"
    isolated = False

    def unavailable_reason(self) -> str | None:
        """Always usable."""
        return None

    def smoke_test(self) -> SandboxProbe:
        """Nothing to check: the process backend isolates nothing."""
        return SandboxProbe(True, "no isolation")

    def run(self, job: SandboxJob) -> SandboxRun:
        """Run pytest with ``job.python`` (default: this interpreter) in a copy of ``job.workdir``."""
        python = job.python or sys.executable
        command = (python, "-m", "pytest", *job.args)
        job.scratch.mkdir(parents=True, exist_ok=True)
        work = Path(tempfile.mkdtemp(prefix="work-", dir=job.scratch))
        try:
            try:
                copy_workspace(job.workdir, work)
            except OSError as exc:
                return SandboxRun(
                    LAUNCH_FAILED_EXIT_CODE,
                    launch_error=f"could not copy the workspace: {exc}",
                    command=command,
                )
            return self._run_in(work, job, command)
        finally:
            remove_tree(work)

    def _run_in(self, work: Path, job: SandboxJob, command: tuple[str, ...]) -> SandboxRun:
        with open(job.output, "wb") as sink:
            try:
                proc = subprocess.Popen(
                    list(command),
                    cwd=os.fspath(work),
                    env=dict(job.env),
                    stdin=subprocess.DEVNULL,
                    stdout=sink,
                    stderr=subprocess.STDOUT,
                    **_popen_kwargs(),
                )
            except (OSError, subprocess.SubprocessError) as exc:
                return SandboxRun(
                    LAUNCH_FAILED_EXIT_CODE,
                    launch_error=f"could not start pytest ({command[0]}): {exc}",
                    command=command,
                )
            try:
                exit_code = proc.wait(timeout=job.timeout)
            except subprocess.TimeoutExpired:
                _kill_tree(proc)
                try:
                    proc.wait(timeout=15)
                except subprocess.TimeoutExpired:  # pragma: no cover - unkillable child
                    log.warning("pytest process %s did not exit after kill", proc.pid)
                return SandboxRun(TIMEOUT_EXIT_CODE, timed_out=True, command=command)
        return SandboxRun(exit_code, command=command)


# --------------------------------------------------------------------------- the container backend


class _EngineTimeout(Exception):
    """The engine CLI did not answer in time."""


class _EngineMissing(Exception):
    """The engine CLI could not be started."""


class _PipeClosed(Exception):
    """The container's stdin closed while the workspace was being sent."""


class _PipeWriter:
    """Tags write errors of the container's stdin, so read errors of the workspace stand out."""

    def __init__(self, stream: IO[bytes]) -> None:
        self.stream = stream

    def write(self, data: bytes) -> int:
        try:
            return self.stream.write(data)
        except OSError as exc:
            raise _PipeClosed(str(exc)) from exc


class _WorkspaceSender(threading.Thread):
    """Streams the workspace tar into the container's stdin, then closes it."""

    def __init__(self, root: Path, stream: IO[bytes]) -> None:
        super().__init__(name="dag-healer-sandbox-workspace", daemon=True)
        self.root = root
        self.stream = stream
        self.error: str | None = None

    def run(self) -> None:
        try:
            write_workspace_tar(self.root, _PipeWriter(self.stream))
        except _PipeClosed:
            pass  # the container stopped reading: its exit tells why
        except Exception as exc:  # the run becomes a launch failure
            self.error = f"could not send the workspace to the sandbox: {exc}"
        finally:
            try:
                self.stream.close()
            except OSError:
                pass


def _decode(data: bytes | None) -> str:
    return (data or b"").decode("utf-8", errors="replace")


def _first_line(text: str, limit: int = _MAX_REASON_DETAIL) -> str:
    for line in text.splitlines():
        if line.strip():
            return line.strip()[:limit]
    return ""


def _read_tail(path: Path, limit: int = 64 * 1024) -> str:
    try:
        with open(path, "rb") as handle:
            handle.seek(max(0, path.stat().st_size - limit))
            return _decode(handle.read())
    except OSError:
        return ""


def _file_size(handle: IO[bytes]) -> int:
    try:
        return os.fstat(handle.fileno()).st_size
    except (OSError, ValueError):
        return 0


def _bytes_text(count: int) -> str:
    return f"{count // 1024**2} MiB" if count % 1024**2 == 0 else f"{count} bytes"


def _wait_for(proc: subprocess.Popen[bytes], timeout: float, outputs: Sequence[IO[bytes]]) -> int | str:
    """Wait for the engine CLI: its exit code, or why the run must be killed now.

    ``"timeout"`` once ``timeout`` seconds have passed, ``"output"`` once the files that receive
    the CLI's output hold more than :data:`MAX_OUTPUT_BYTES` together (checked every
    :data:`OUTPUT_CHECK_SECONDS`).
    """
    deadline = time.monotonic() + timeout
    while True:
        remaining = deadline - time.monotonic()
        if remaining <= 0:
            return "timeout"
        try:
            return proc.wait(timeout=min(OUTPUT_CHECK_SECONDS, remaining))
        except subprocess.TimeoutExpired:
            pass
        if sum(_file_size(handle) for handle in outputs) > MAX_OUTPUT_BYTES:
            return "output"


def _started(stderr: str) -> bool:
    return any(line.strip() == BOOTSTRAP_MARKER for line in stderr.splitlines())


def _engine_detail(stderr: str) -> str:
    """The engine's last error lines (the marker and docker's usage hint left out)."""
    lines = [
        line.strip()
        for line in stderr.splitlines()
        if line.strip() and line.strip() != BOOTSTRAP_MARKER and not line.strip().startswith("Run '")
    ]
    return " ".join(lines[-3:])[-_MAX_LAUNCH_DETAIL:]


def _after_marker(stderr: str) -> str:
    """What the CLI wrote to stderr once the bootstrap had started (the engine's own errors)."""
    lines = stderr.splitlines()
    starts = [index for index, line in enumerate(lines) if line.strip() == BOOTSTRAP_MARKER]
    return "\n".join(lines[starts[0] + 1 :]) if starts else ""


def _bootstrap_error(stderr: str) -> str | None:
    for line in reversed(stderr.splitlines()):
        if line.startswith(BOOTSTRAP_ERROR_PREFIX):
            return line[len(BOOTSTRAP_ERROR_PREFIX) :].strip()[:_MAX_LAUNCH_DETAIL]
    return None


def _is_root_user(user: str) -> bool:
    """True for a user spec that means root (``''``, ``root``, ``0``, ``0:*``, ``root:*``)."""
    name = user.strip().split(":", 1)[0]
    return name in ("", "root") or (name.isdigit() and int(name) == 0)


def _container_name(timeout: float) -> str:
    expiry = int(time.time() + max(0.0, float(timeout)) + CONTAINER_GRACE_SECONDS)
    return f"{CONTAINER_NAME_PREFIX}{expiry}-{secrets.token_hex(4)}"


def _normalised_size(value: Any) -> str:
    return str(value).strip().lower()


_sweep_lock = threading.Lock()
_last_sweep: dict[tuple[str, ...], float] = {}


class ContainerSandbox:
    """pytest in a fresh container per run, through the ``docker`` or ``podman`` CLI.

    See the module docstring for the flags, the environment rules and the start-marker protocol.
    ``command`` replaces the engine CLI (tests run a fake engine); by default it is
    :func:`engine_command` of ``settings.engine``, looked up at every call.
    """

    name = "container"
    isolated = True

    def __init__(self, settings: SandboxSettings, *, command: Sequence[str] | None = None) -> None:
        self.settings = settings
        self.engine = str(settings.engine)
        self.image = settings.image or None
        self._command = list(command) if command is not None else None
        self._lock = threading.Lock()
        self._ready = False
        self._version: str | None = None

    @property
    def command(self) -> list[str]:
        """The engine CLI command (``[engine]`` unless one was given)."""
        return list(self._command) if self._command is not None else list(engine_command(self.engine))

    def __repr__(self) -> str:
        return f"ContainerSandbox(engine={self.engine!r}, image={self.image!r})"

    # ------------------------------------------------------------------ settings and engine

    def _settings_problem(self, python: str | None = None) -> str | None:
        """Why these settings can never run (checked again before every run; no engine call)."""
        settings = self.settings
        image = self.image
        if not image:
            return (
                "repo.sandbox.image is not set; set it in healer.yaml to an image with Python, pytest "
                "and the repository's requirements (the template of 'dag-healer init' reads it from "
                "HEALER_SANDBOX_IMAGE)"
            )
        if not _IMAGE_RE.match(image):
            return f"repo.sandbox.image '{image}' is not an image reference"
        user = settings.user or None
        if user is not None and _is_root_user(user):
            return f"repo.sandbox.user '{user}' is root; use a non-root user, or null for the image's USER"
        if user is not None and not _USER_RE.match(user):
            return f"repo.sandbox.user '{user}' is not a user name or id"
        if self.engine not in SANDBOX_ENGINES:
            return f"repo.sandbox.engine '{self.engine}' is not one of {', '.join(SANDBOX_ENGINES)}"
        interpreter = python if python is not None else settings.python
        if not interpreter or not _PYTHON_RE.match(interpreter) or interpreter.startswith("-"):
            return f"repo.sandbox.python '{interpreter}' is not an interpreter name or path"
        for key in ("memory", "work_size", "tmp_size"):
            if not _SIZE_RE.match(_normalised_size(getattr(settings, key))):
                return f"repo.sandbox.{key} '{getattr(settings, key)}' is not a size such as 512m or 2g"
        cpus = settings.cpus
        if cpus is not None and not float(cpus) > 0:
            return f"repo.sandbox.cpus '{cpus}' is not a positive number"
        if int(settings.pids) < 16:
            return f"repo.sandbox.pids '{settings.pids}' is below 16"
        return None

    def _missing_cli(self) -> str:
        if self.engine == "podman":
            return (
                "the podman command was not found on the worker's PATH; install Podman or set "
                "repo.sandbox.engine"
            )
        return (
            f"the {self.engine} command was not found on the worker's PATH; install the Docker CLI or set "
            "repo.sandbox.engine"
        )

    def _call(self, args: Sequence[str], *, timeout: float) -> subprocess.CompletedProcess[str]:
        """Run ``<engine> <args>`` with the engine environment and return its decoded output."""
        try:
            proc = subprocess.run(
                [*self.command, *args],
                stdin=subprocess.DEVNULL,
                capture_output=True,
                timeout=timeout,
                env=engine_environment(),
                check=False,
            )
        except subprocess.TimeoutExpired as exc:
            raise _EngineTimeout(str(exc)) from exc
        except OSError as exc:
            raise _EngineMissing(str(exc)) from exc
        return subprocess.CompletedProcess(
            proc.args, proc.returncode, _decode(proc.stdout), _decode(proc.stderr)
        )

    def _version_args(self) -> list[str]:
        # podman answers locally (no service), docker asks its daemon: that is the check
        field = "{{.Client.Version}}" if self.engine == "podman" else "{{.Server.Version}}"
        return ["version", "--format", field]

    def _no_answer(self) -> str:
        return f"{self.engine} did not answer within {PROBE_TIMEOUT:g} s"

    def _probe_engine(self) -> str | None:
        """Check the CLI, its engine and the image (no container is started)."""
        image = self.image
        assert image is not None
        command = self.command
        if not command or shutil.which(command[0]) is None:
            return self._missing_cli()
        try:
            version = self._call(self._version_args(), timeout=PROBE_TIMEOUT)
        except _EngineTimeout:
            return self._no_answer()
        except _EngineMissing:
            return self._missing_cli()
        if version.returncode != 0:
            detail = (
                _first_line(version.stderr)
                or _first_line(version.stdout)
                or f"exit code {version.returncode}"
            )
            return (
                f"{self.engine} cannot reach its engine ({detail}); start it, or give the worker access "
                "to it (DOCKER_HOST or CONTAINER_HOST, the socket and its group)"
            )
        self._version = _first_line(version.stdout, 80) or None
        try:
            inspect = self._call(
                ["image", "inspect", "--format", "{{json .Config.User}}", image], timeout=PROBE_TIMEOUT
            )
        except _EngineTimeout:
            return self._no_answer()
        except _EngineMissing:
            return self._missing_cli()
        if inspect.returncode != 0:
            return self._image_missing()
        if not self.settings.user and _is_root_user(_parse_inspected_user(inspect.stdout)):
            return f"the image {image} runs as root; give it a non-root USER or set repo.sandbox.user"
        return None

    def _image_missing(self) -> str:
        return (
            f"the image {self.image} is not present on the {self.engine} engine; build or pull it there "
            "(the sandbox never pulls images)"
        )

    def unavailable_reason(self) -> str | None:
        """Why no container can run now, or ``None`` (a success is cached on the instance).

        In order: the image is not set or not an image reference, the user is root, the CLI is
        missing, its engine does not answer, the image is not present, the image runs as root.
        """
        with self._lock:
            if self._ready:
                return None
        reason = self._settings_problem() or self._probe_engine()
        if reason is None:
            with self._lock:
                self._ready = True
        return reason

    # ------------------------------------------------------------------ containers

    def _run_argv(
        self, name: str, mode: str, env_names: Sequence[str], python: str, args: Sequence[str]
    ) -> list[str]:
        """The ``<engine> run`` command line (variable names only, never values)."""
        settings = self.settings
        memory = _normalised_size(settings.memory)
        argv = [
            *self.command,
            "run",
            "--rm",
            "-i",
            "--name",
            name,
            "--label",
            f"{CONTAINER_LABEL}=1",
            "--pull",
            "never",
            "--network",
            "none",
            "--read-only",
            "--tmpfs",
            f"{CONTAINER_WORKDIR}:rw,exec,nosuid,nodev,size={_normalised_size(settings.work_size)},mode=1777",
            "--tmpfs",
            f"{CONTAINER_TMP}:rw,noexec,nosuid,nodev,size={_normalised_size(settings.tmp_size)},mode=1777",
            "--cap-drop",
            "ALL",
            "--security-opt",
            "no-new-privileges",
            "--pids-limit",
            str(int(settings.pids)),
            "--memory",
            memory,
            "--memory-swap",
            memory,
            # Engines size /dev/shm differently (OrbStack: half of the VM's memory); 64m is
            # Docker Engine's default.
            "--shm-size",
            SHM_SIZE,
            # No engine log of the output: it is unmasked until the worker reads it.
            "--log-driver",
            "none",
        ]
        if settings.cpus is not None:
            argv += ["--cpus", f"{float(settings.cpus):g}"]
        argv.append("--init")
        if settings.user:
            argv += ["--user", settings.user]
        argv += ["--entrypoint", python]
        for env_name in sorted(env_names):
            argv += ["-e", env_name]
        argv += [
            str(self.image),
            "-c",
            BOOTSTRAP,
            mode,
            CONTAINER_WORKDIR,
            CONTAINER_HOME,
            CONTAINER_TMP,
            *args,
        ]
        return argv

    def _remove_container(self, name: str) -> None:
        """``kill`` and ``rm -f`` a container by name (best effort)."""
        for args in (["kill", name], ["rm", "-f", name]):
            try:
                self._call(args, timeout=KILL_TIMEOUT)
            except (_EngineTimeout, _EngineMissing) as exc:
                log.debug("sandbox: %s %s failed: %s", self.engine, args[0], exc)

    def _launch_failure(self, exit_code: int, stderr: str) -> str:
        """The launch error of a container that never reached the bootstrap."""
        detail = _engine_detail(stderr)
        lowered = detail.lower()
        if "no such image" in lowered or "image not known" in lowered:
            return self._image_missing()
        if exit_code < 0:
            return f"the {self.engine} command was killed by signal {-exit_code}"
        message = f"{self.engine} exited with code {exit_code} before the sandbox started"
        return f"{message}: {detail}" if detail else message

    def remove_expired(self) -> list[str]:
        """Remove the sandbox containers whose expiry time has passed; return their names.

        Containers are found by :data:`CONTAINER_LABEL`; the expiry time is part of the name, so the
        live containers of other workers are never touched. Best effort: never raises.
        """
        removed: list[str] = []
        try:
            listed = self._call(
                ["ps", "-a", "--filter", f"label={CONTAINER_LABEL}=1", "--format", "{{.Names}}"],
                timeout=PROBE_TIMEOUT,
            )
            if listed.returncode != 0:
                return removed
            now = time.time()
            for line in listed.stdout.splitlines():
                name = line.strip()
                match = _CONTAINER_NAME_RE.match(name)
                if match is None or int(match.group(1)) >= now:
                    continue
                if self._call(["rm", "-f", name], timeout=KILL_TIMEOUT).returncode == 0:
                    removed.append(name)
        except Exception as exc:  # cleaning up is best effort
            log.debug("sandbox: looking for expired containers failed: %s", exc)
        if removed:
            log.info("sandbox: removed %d expired container(s): %s", len(removed), ", ".join(removed))
        return removed

    def _sweep(self) -> None:
        """:meth:`remove_expired`, at most once per :data:`SWEEP_INTERVAL` per engine command."""
        key = tuple(self.command)
        now = time.monotonic()
        with _sweep_lock:
            last = _last_sweep.get(key)
            if last is not None and now - last < SWEEP_INTERVAL:
                return
            _last_sweep[key] = now
        self.remove_expired()

    def run(self, job: SandboxJob) -> SandboxRun:
        """Run pytest in a fresh container on a copy of ``job.workdir`` sent as a tar on stdin."""
        python = job.python or str(self.settings.python)
        command = (python, "-m", "pytest", *job.args)
        problem = self._settings_problem(python)
        if problem is not None:
            return SandboxRun(LAUNCH_FAILED_EXIT_CODE, launch_error=problem, command=command)
        self._sweep()
        name = _container_name(job.timeout)
        env = container_environment(job.env)
        argv = self._run_argv(name, "pytest", list(env), python, job.args)
        log.debug("sandbox: %s", " ".join("<bootstrap>" if part == BOOTSTRAP else part for part in argv))
        job.scratch.mkdir(parents=True, exist_ok=True)
        stderr_path = job.scratch / "engine-stderr.log"
        stop: str | None = None
        with open(job.output, "wb") as out, open(stderr_path, "wb") as err:
            try:
                proc = subprocess.Popen(
                    argv, stdin=subprocess.PIPE, stdout=out, stderr=err, env={**engine_environment(), **env}
                )
            except OSError as exc:
                if isinstance(exc, FileNotFoundError):
                    return SandboxRun(
                        LAUNCH_FAILED_EXIT_CODE, launch_error=self._missing_cli(), command=command
                    )
                return SandboxRun(
                    LAUNCH_FAILED_EXIT_CODE,
                    launch_error=f"could not start {self.engine}: {exc}",
                    command=command,
                )
            assert proc.stdin is not None
            sender = _WorkspaceSender(job.workdir, proc.stdin)
            sender.start()
            waited = _wait_for(proc, job.timeout, (out, err))
            if isinstance(waited, str):  # past the timeout or the output limit
                stop = waited
                if stop == "timeout":
                    log.warning(
                        "sandbox: pytest exceeded the %s s timeout; killing container %s",
                        f"{job.timeout:g}",
                        name,
                    )
                else:
                    log.warning(
                        "sandbox: the output of pytest passed %s; killing container %s",
                        _bytes_text(MAX_OUTPUT_BYTES),
                        name,
                    )
                self._remove_container(name)
                proc.kill()
                try:
                    proc.wait(timeout=15)
                except subprocess.TimeoutExpired:  # pragma: no cover - unkillable CLI
                    log.warning("sandbox: the %s command %s did not exit after kill", self.engine, proc.pid)
            sender.join(timeout=30)
        if stop == "timeout":
            return SandboxRun(TIMEOUT_EXIT_CODE, timed_out=True, command=command)
        if stop == "output":
            with open(job.output, "ab") as sink:
                sink.write(
                    f"\n[dag-healer] pytest was killed: its output passed {_bytes_text(MAX_OUTPUT_BYTES)} "
                    "(only the end of it is kept)\n".encode()
                )
            return SandboxRun(KILLED_EXIT_CODE, command=command)
        exit_code = int(waited)
        stderr = _read_tail(stderr_path)
        if sender.error is not None:
            return SandboxRun(LAUNCH_FAILED_EXIT_CODE, launch_error=sender.error, command=command)
        if exit_code < 0:  # the CLI itself was killed: its container may still run
            self._remove_container(name)
        if not _started(stderr) or exit_code < 0:
            return SandboxRun(
                LAUNCH_FAILED_EXIT_CODE, launch_error=self._launch_failure(exit_code, stderr), command=command
            )
        if exit_code == BOOTSTRAP_FAILED_EXIT_CODE:
            error = _bootstrap_error(stderr) or f"the sandbox bootstrap failed (exit {exit_code})"
            return SandboxRun(LAUNCH_FAILED_EXIT_CODE, launch_error=error, command=command)
        failure = _engine_detail(_after_marker(stderr))
        if exit_code in ENGINE_ERROR_EXIT_CODES and failure:  # e.g. the engine went away mid-run
            self._remove_container(name)
            error = f"{self.engine} failed while the sandbox ran (exit {exit_code}): {failure}"
            return SandboxRun(LAUNCH_FAILED_EXIT_CODE, launch_error=error, command=command)
        if exit_code == KILLED_EXIT_CODE:
            memory = _normalised_size(self.settings.memory)
            with open(job.output, "ab") as out:
                out.write(
                    f"\n[dag-healer] the sandbox container was killed (exit {KILLED_EXIT_CODE}); it may have "
                    f"run out of memory (repo.sandbox.memory is {memory})\n".encode()
                )
        return SandboxRun(exit_code, command=command)

    def smoke_test(self) -> SandboxProbe:
        """Start one container with the run's flags and no workspace, and check what it sees.

        OK when the user is not root, no network interface besides ``lo`` exists, an outbound TCP
        connection fails and pytest is importable.
        """
        reason = self.unavailable_reason()
        if reason is not None:
            return SandboxProbe(False, reason)
        python = str(self.settings.python)
        name = _container_name(SMOKE_TIMEOUT)
        env = container_environment({})
        argv = self._run_argv(name, "probe", list(env), python, ())
        where = f"{self.engine} {self._version or '(version unknown)'}, image {self.image}"
        try:
            proc = subprocess.run(
                argv,
                stdin=subprocess.DEVNULL,
                capture_output=True,
                timeout=SMOKE_TIMEOUT,
                env={**engine_environment(), **env},
                check=False,
            )
        except subprocess.TimeoutExpired:
            self._remove_container(name)
            return SandboxProbe(False, f"{where}: the smoke run did not finish within {SMOKE_TIMEOUT:g} s")
        except OSError as exc:
            return SandboxProbe(False, f"{where}: could not start {self.engine}: {exc}")
        stdout, stderr = _decode(proc.stdout), _decode(proc.stderr)
        if not _started(stderr) or proc.returncode < 0:
            return SandboxProbe(
                False, f"{where}: the sandbox did not start: {self._launch_failure(proc.returncode, stderr)}"
            )
        if proc.returncode == BOOTSTRAP_FAILED_EXIT_CODE:
            return SandboxProbe(
                False, f"{where}: {_bootstrap_error(stderr) or 'the sandbox bootstrap failed'}"
            )
        facts = _parse_probe(stdout)
        if facts is None:
            return SandboxProbe(
                False, f"{where}: the sandbox probe printed no result (exit {proc.returncode})"
            )
        return _judge_probe(where, facts)


def _parse_inspected_user(output: str) -> str:
    text = output.strip()
    try:
        value = json.loads(text)
    except ValueError:
        return text
    return value if isinstance(value, str) else ""


def _parse_probe(stdout: str) -> dict[str, Any] | None:
    for line in reversed(stdout.splitlines()):
        line = line.strip()
        if not line.startswith("{"):
            continue
        try:
            value = json.loads(line)
        except ValueError:
            continue
        if isinstance(value, dict):
            return value
    return None


def _judge_probe(where: str, facts: Mapping[str, Any]) -> SandboxProbe:
    """The smoke test's verdict on the probe's JSON line."""
    problems: list[str] = []
    uid = facts.get("uid")
    user = facts.get("user")
    if not isinstance(uid, int) or isinstance(uid, bool):
        problems.append("the probe could not tell the user id")
    elif uid == 0:
        problems.append("it runs as root (uid 0)")
    interfaces = facts.get("interfaces")
    if not isinstance(interfaces, list):
        problems.append("the probe could not list the network interfaces (/sys/class/net)")
    else:
        others = sorted(str(item) for item in interfaces if item != "lo")
        if others:
            problems.append(f"it has a network interface besides lo ({', '.join(others)})")
    if facts.get("connect") == "open":
        problems.append(f"an outbound connection to {PROBE_ADDRESS[0]}:{PROBE_ADDRESS[1]} succeeded")
    pytest_version = facts.get("pytest")
    python = facts.get("python") or "unknown"
    if not pytest_version:
        problems.append(f"pytest is not importable in the image (Python {python})")
    if problems:
        return SandboxProbe(False, f"{where}: " + "; ".join(problems))
    who = f"uid {uid} ({user})" if user else f"uid {uid}"
    networks = ", ".join(str(item) for item in interfaces) if isinstance(interfaces, list) else ""
    return SandboxProbe(
        True,
        f"{where}: {who}, no network (interfaces: {networks}), pytest {pytest_version} on Python {python}",
    )


# --------------------------------------------------------------------------- choosing a backend


def build_sandbox(settings: SandboxSettings) -> Sandbox:
    """The sandbox that ``settings.backend`` names; never a fallback to another one.

    Raises:
        ValueError: the backend is not one of :data:`SANDBOX_BACKENDS`.
    """
    backend = getattr(settings, "backend", None)
    if backend == "process":
        return ProcessSandbox()
    if backend == "container":
        return ContainerSandbox(settings)
    raise ValueError(f"repo.sandbox.backend {backend!r} is not one of {', '.join(SANDBOX_BACKENDS)}")


__all__ = [
    "BOOTSTRAP",
    "BOOTSTRAP_ERROR_PREFIX",
    "BOOTSTRAP_FAILED_EXIT_CODE",
    "BOOTSTRAP_MARKER",
    "CONTAINER_AIRFLOW_HOME",
    "CONTAINER_HOME",
    "CONTAINER_LABEL",
    "CONTAINER_NAME_PREFIX",
    "CONTAINER_TMP",
    "CONTAINER_WORKDIR",
    "ENGINE_ENV",
    "ENGINE_ENV_PREFIXES",
    "ENGINE_ERROR_EXIT_CODES",
    "HOST_ONLY_ENV",
    "HOST_ONLY_PREFIXES",
    "KILLED_EXIT_CODE",
    "KILL_TIMEOUT",
    "LAUNCH_FAILED_EXIT_CODE",
    "MAX_OUTPUT_BYTES",
    "OUTPUT_CHECK_SECONDS",
    "PROBE_TIMEOUT",
    "PROCESS_BACKEND_WARNING",
    "PROXY_VARIABLES",
    "RLIMIT_AS_BYTES",
    "RLIMIT_FSIZE_BYTES",
    "RLIMIT_NPROC_COUNT",
    "SANDBOX_BACKENDS",
    "SANDBOX_ENGINES",
    "SMOKE_TIMEOUT",
    "TIMEOUT_EXIT_CODE",
    "ContainerSandbox",
    "ProcessSandbox",
    "Sandbox",
    "SandboxJob",
    "SandboxProbe",
    "SandboxRun",
    "SandboxSettings",
    "build_sandbox",
    "container_environment",
    "copy_workspace",
    "engine_command",
    "engine_environment",
    "sandbox_launch_failure",
    "sandbox_refusal",
    "write_workspace_tar",
]
