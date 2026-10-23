"""Run pytest inside a repair workspace: scrubbed environment, timeout, resource limits.

The repair agent runs code from the user's repository plus tests written by the LLM, so
the test process gets as little as possible from the worker:

* **Environment** (:func:`scrubbed_env`): no LLM/GitHub/SMTP credentials, no
  ``HEALER_*`` settings, no Airflow metadata-database or JWT settings, no
  ``PYTHONPATH``/``PYTEST_*`` injection, and nothing whose name looks like a secret.
  Airflow is pointed at a throw-away ``AIRFLOW_HOME`` with an SQLite database in
  unit-test mode, so tests that import Airflow never touch the real deployment.
* **Time**: the whole process group is killed when ``timeout`` expires (POSIX:
  ``start_new_session`` + ``os.killpg``; Windows: best-effort ``taskkill /T``).
* **Resources** (POSIX): ``RLIMIT_AS``, ``RLIMIT_NPROC`` and ``RLIMIT_FSIZE`` are lowered
  in the child before ``exec`` (never raised above the current limits). A limit the kernel
  refuses is skipped: macOS does not let a process lower ``RLIMIT_AS``.
* **Output** goes to a temporary file (not memory) and only the last
  :data:`MAX_OUTPUT_CHARS` characters are kept.

There is no network isolation: this is a guard against accidents, not a sandbox for
hostile code (see docs/security.md). The command is ``python -m pytest`` so the
workspace root is importable (``python -m`` puts the working directory on
``sys.path``), exactly like running the suite from a checkout.

Limitation: when the workspace has no pytest configuration file of its own, pytest
looks for one in parent directories; keep workspaces outside other Python projects.
"""

from __future__ import annotations

import logging
import os
import re
import subprocess
import sys
import tempfile
import time
from collections.abc import Mapping, Sequence
from dataclasses import asdict, dataclass, field
from pathlib import Path
from typing import Any

log = logging.getLogger(__name__)

#: Maximum number of characters kept in :attr:`PytestResult.output_tail`.
MAX_OUTPUT_CHARS = 20000
#: ``exit_code`` reported when the run was killed by the timeout.
TIMEOUT_EXIT_CODE = 124
#: ``exit_code`` reported when pytest could not be started at all (pytest's
#: "internal error" code, so :attr:`PytestResult.collection_error` is True).
LAUNCH_FAILED_EXIT_CODE = 3

#: POSIX resource limits applied to the pytest process (lowered, never raised).
RLIMIT_AS_BYTES = 8 * 1024**3
RLIMIT_NPROC_COUNT = 4096
RLIMIT_FSIZE_BYTES = 1024**3

#: Variables always removed from the pytest environment.
SCRUB_EXACT = frozenset(
    {
        "ANTHROPIC_API_KEY",
        "ANTHROPIC_AUTH_TOKEN",
        "GITHUB_TOKEN",
        "GH_TOKEN",
        "GITHUB_REPO",
        "AIRFLOW__CORE__FERNET_KEY",
        "AIRFLOW__CORE__INTERNAL_API_SECRET_KEY",
        "AIRFLOW__WEBSERVER__SECRET_KEY",
        "AIRFLOW__API__SECRET_KEY",
        "AIRFLOW_CONFIG",
        "PYTHONPATH",
        "PYTHONSAFEPATH",
        "PYTHONSTARTUP",
        "PYTHONINSPECT",
        "PY_COLORS",
        "FORCE_COLOR",
        "GIT_ASKPASS",
        "SSH_ASKPASS",
        "SSH_AUTH_SOCK",
    }
)
#: Variable-name prefixes removed from the pytest environment.
SCRUB_PREFIXES: tuple[str, ...] = (
    "HEALER_",
    "DAG_HEALER_",
    "ANTHROPIC_",
    "AIRFLOW__DATABASE__",
    "AIRFLOW__API_AUTH__",
    "AIRFLOW__SMTP__",
    "AIRFLOW__EMAIL__",
    "AIRFLOW__SECRETS__",
    "AIRFLOW__CELERY__",
    "AIRFLOW_CONN_",
    "AIRFLOW_VAR_",
    "SMTP_",
    "PYTEST_",
)
#: Variables whose (upper-cased) name contains one of these are removed too.
SCRUB_MARKERS: tuple[str, ...] = (
    "SECRET",
    "TOKEN",
    "PASSWORD",
    "PASSWD",
    "API_KEY",
    "APIKEY",
    "PRIVATE_KEY",
    "CREDENTIAL",
)

_SUMMARY_RE = re.compile(r"^=*\s*(?P<body>.*?)\s+in\s+(?P<secs>\d+(?:\.\d+)?)s(?:\s+\([^)]*\))?\s*=*\s*$")
_COUNT_RE = re.compile(r"(\d+)\s+([a-z]+(?:\s+passed|\s+failed)?)")
_COLLECTION_ERRORS_RE = re.compile(r"Interrupted:\s+(\d+)\s+errors?\s+during collection")
_COUNT_KEYS = {
    "passed": "passed",
    "failed": "failed",
    "error": "errors",
    "errors": "errors",
    "skipped": "skipped",
    "xfailed": "xfailed",
    "xpassed": "xpassed",
    "deselected": "deselected",
    "warning": "warnings",
    "warnings": "warnings",
}


@dataclass
class PytestResult:
    """Outcome of one pytest run (counts parsed from pytest's final summary line)."""

    exit_code: int
    passed: int = 0
    failed: int = 0
    errors: int = 0
    skipped: int = 0
    collected: int = 0
    duration: float = 0.0
    timed_out: bool = False
    output_tail: str = ""
    command: list[str] = field(default_factory=list)
    xfailed: int = 0
    xpassed: int = 0
    deselected: int = 0
    warnings: int = 0
    collection_errors: int = 0
    summary_line: str = ""

    @property
    def ok(self) -> bool:
        """True when pytest exited with status 0 (every collected test passed)."""
        return self.exit_code == 0

    @property
    def collection_error(self) -> bool:
        """True when the run did not get as far as executing tests properly.

        Exit codes 2 (interrupted, e.g. an import error while collecting), 3 (internal
        error) and 4 (usage error), or errors with nothing collected.
        """
        return self.exit_code in (2, 3, 4) or bool(self.errors and not self.collected)

    def summary(self) -> str:
        """Return a one-line human summary, e.g. ``"exit 1: 1 failed, 2 passed in 0.31s"``."""
        if self.timed_out:
            return f"timed out after {self.duration:.0f} s (killed)"
        if self.summary_line:
            return f"exit {self.exit_code}: {self.summary_line}"
        return f"exit {self.exit_code}: no pytest summary line"

    def to_dict(self) -> dict[str, Any]:
        """Return a JSON-safe dict (``ok`` and ``collection_error`` included)."""
        data = asdict(self)
        data["ok"] = self.ok
        data["collection_error"] = self.collection_error
        return data


# --------------------------------------------------------------------------- environment


def _is_scrubbed(name: str) -> bool:
    upper = name.upper()
    if upper in SCRUB_EXACT or upper.startswith(SCRUB_PREFIXES):
        return True
    return any(marker in upper for marker in SCRUB_MARKERS)


def _sqlite_url(path: Path) -> str:
    """``sqlite:///`` URL for an absolute path (four slashes on POSIX, ``C:/`` on Windows)."""
    return f"sqlite:///{path.resolve().as_posix()}"


def scrubbed_env(
    base: Mapping[str, str] | None = None,
    *,
    airflow_home: str | os.PathLike[str] | None = None,
) -> dict[str, str]:
    """Return the environment for a pytest run.

    Args:
        base: environment to start from (default ``os.environ``).
        airflow_home: directory used as ``AIRFLOW_HOME`` (and for the SQLite metadata
            database). When omitted a new temporary directory is created; the caller is
            responsible for deleting it (:func:`run_pytest` manages its own).

    Removes credentials and settings that must never reach user code (see the
    ``SCRUB_*`` constants: ``ANTHROPIC_API_KEY``, ``GITHUB_TOKEN``, ``HEALER_*``,
    ``AIRFLOW__DATABASE__*``, ``AIRFLOW__API_AUTH__*``, ``AIRFLOW__CORE__FERNET_KEY``, SMTP
    settings, ``PYTHONPATH``, ``PYTEST_*`` and any name containing SECRET/TOKEN/PASSWORD/...)
    and sets ``AIRFLOW_HOME``, ``AIRFLOW__CORE__UNIT_TEST_MODE=True``,
    ``AIRFLOW__DATABASE__SQL_ALCHEMY_CONN=sqlite:///<home>/unittests.db``,
    ``AIRFLOW__CORE__LOAD_EXAMPLES=False``, ``PYTHONDONTWRITEBYTECODE=1``,
    ``PYTHONIOENCODING=utf-8`` and ``PY_COLORS=0``.
    """
    source = os.environ if base is None else base
    env = {key: value for key, value in source.items() if not _is_scrubbed(key)}
    home = (
        Path(airflow_home)
        if airflow_home is not None
        else Path(tempfile.mkdtemp(prefix="dag-healer-airflow-home-"))
    )
    home.mkdir(parents=True, exist_ok=True)
    env.update(
        {
            "AIRFLOW_HOME": str(home),
            "AIRFLOW__CORE__UNIT_TEST_MODE": "True",
            "AIRFLOW__DATABASE__SQL_ALCHEMY_CONN": _sqlite_url(home / "unittests.db"),
            "AIRFLOW__CORE__LOAD_EXAMPLES": "False",
            "PYTHONDONTWRITEBYTECODE": "1",
            "PYTHONIOENCODING": "utf-8",
            "PY_COLORS": "0",
        }
    )
    return env


# --------------------------------------------------------------------------- process control


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


def _truncation_marker(dropped: int) -> str:
    return f"...[output truncated: {dropped} earlier characters omitted]...\n"


def truncate_tail(text: str, limit: int = MAX_OUTPUT_CHARS, *, dropped_before: int = 0) -> str:
    """Return the last characters of ``text`` so that the result has at most ``limit`` chars.

    A marker line is prepended when anything was dropped (``dropped_before`` counts
    characters already discarded by the caller).
    """
    if len(text) <= limit and not dropped_before:
        return text
    # The marker is sized for the largest possible count, so the result never exceeds limit.
    longest = _truncation_marker(dropped_before + len(text))
    keep = min(len(text), max(0, limit - len(longest)))
    marker = _truncation_marker(dropped_before + len(text) - keep)
    return (marker + text[len(text) - keep :])[:limit] if keep else marker[:limit]


def _read_tail(path: Path, limit: int) -> str:
    """Read (at most) the last ``limit`` characters of a possibly huge output file."""
    size = path.stat().st_size if path.exists() else 0
    max_bytes = limit * 4 + 4096  # UTF-8 needs at most 4 bytes per character
    skipped = max(0, size - max_bytes)
    with open(path, "rb") as handle:
        handle.seek(skipped)
        data = handle.read()
    text = data.decode("utf-8", errors="replace").replace("\r\n", "\n")
    if skipped:
        # The first line is probably partial; drop it (bytes approximate characters).
        newline = text.find("\n")
        if newline >= 0:
            skipped += newline + 1
            text = text[newline + 1 :]
    return truncate_tail(text, limit, dropped_before=skipped)


# --------------------------------------------------------------------------- parsing


def parse_summary(output: str) -> dict[str, Any]:
    """Parse pytest's final summary line (``"1 failed, 2 passed in 0.12s"``).

    Returns the counts (``passed, failed, errors, skipped, xfailed, xpassed, deselected,
    warnings``), ``collection_errors``, the derived ``collected`` count and
    ``summary_line``. Missing values are 0 / "".
    """
    counts: dict[str, Any] = {key: 0 for key in set(_COUNT_KEYS.values())}
    counts["collection_errors"] = 0
    counts["summary_line"] = ""
    for raw in reversed(output.splitlines()):
        line = raw.strip()
        match = _SUMMARY_RE.match(line)
        if not match:
            continue
        body = match.group("body").strip()
        found = False
        for number, word in _COUNT_RE.findall(body):
            key = _COUNT_KEYS.get(word)
            if key is not None:
                counts[key] += int(number)
                found = True
        if found or body.startswith("no tests ran"):
            counts["summary_line"] = line.strip("= ").strip()
            break
    collection = _COLLECTION_ERRORS_RE.search(output)
    if collection:
        counts["collection_errors"] = int(collection.group(1))
    test_errors = max(0, counts["errors"] - counts["collection_errors"])
    counts["collected"] = (
        counts["passed"]
        + counts["failed"]
        + counts["skipped"]
        + counts["xfailed"]
        + counts["xpassed"]
        + counts["deselected"]
        + test_errors
    )
    return counts


# --------------------------------------------------------------------------- running


def run_pytest(
    workdir: str | os.PathLike[str],
    targets: Sequence[str] | None = None,
    *,
    timeout: int = 600,
    python: str | None = None,
    extra_args: Sequence[str] = (),
    extra_env: Mapping[str, str | None] | None = None,
) -> PytestResult:
    """Run ``python -m pytest -q -p no:cacheprovider -o addopts= [extra_args] [targets]``.

    Args:
        workdir: directory to run in (the workspace root).
        targets: test files / node ids relative to ``workdir`` (default: pytest's own
            discovery, i.e. the repository's ``testpaths`` or the whole tree).
        timeout: seconds before the whole process tree is killed.
        python: interpreter to use (default: the current one).
        extra_args: extra pytest options, placed before the targets.
        extra_env: variables set on top of :func:`scrubbed_env` (``None`` removes one).

    Returns:
        A :class:`PytestResult`. Never raises for test failures, timeouts or a missing
        interpreter (the latter is reported with ``exit_code`` 3 and the error text).

    Raises:
        FileNotFoundError: ``workdir`` does not exist.
    """
    root = Path(workdir)
    if not root.is_dir():
        raise FileNotFoundError(f"pytest working directory does not exist: {root}")
    interpreter = python or sys.executable
    cmd = [
        interpreter,
        "-m",
        "pytest",
        "-q",
        "-p",
        "no:cacheprovider",
        "-o",
        "addopts=",
        *[str(a) for a in extra_args],
        *[str(t) for t in (targets or [])],
    ]
    with tempfile.TemporaryDirectory(prefix="dag-healer-pytest-", ignore_cleanup_errors=True) as tmp:
        tmpdir = Path(tmp)
        env = scrubbed_env(airflow_home=tmpdir / "airflow_home")
        for key, value in (extra_env or {}).items():
            if value is None:
                env.pop(key, None)
            else:
                env[key] = str(value)
        output_path = tmpdir / "pytest-output.log"
        timed_out = False
        start = time.monotonic()
        with open(output_path, "wb") as sink:
            try:
                proc = subprocess.Popen(
                    cmd,
                    cwd=os.fspath(root),
                    env=env,
                    stdin=subprocess.DEVNULL,
                    stdout=sink,
                    stderr=subprocess.STDOUT,
                    **_popen_kwargs(),
                )
            except (OSError, subprocess.SubprocessError) as exc:
                log.warning("could not start pytest with %s: %s", interpreter, exc)
                return PytestResult(
                    exit_code=LAUNCH_FAILED_EXIT_CODE,
                    duration=time.monotonic() - start,
                    output_tail=f"could not start pytest ({interpreter}): {exc}",
                    command=cmd,
                )
            try:
                exit_code = proc.wait(timeout=timeout)
            except subprocess.TimeoutExpired:
                timed_out = True
                _kill_tree(proc)
                try:
                    proc.wait(timeout=15)
                except subprocess.TimeoutExpired:  # pragma: no cover - unkillable child
                    log.warning("pytest process %s did not exit after kill", proc.pid)
                exit_code = TIMEOUT_EXIT_CODE
        duration = time.monotonic() - start
        output = _read_tail(output_path, MAX_OUTPUT_CHARS)

    if timed_out:
        note = f"\n[dag-healer] pytest was killed after exceeding the {timeout} s timeout\n"
        output = truncate_tail(output.rstrip("\n") + note, MAX_OUTPUT_CHARS)
    counts = parse_summary(output)
    result = PytestResult(
        exit_code=exit_code,
        passed=counts["passed"],
        failed=counts["failed"],
        errors=counts["errors"],
        skipped=counts["skipped"],
        collected=counts["collected"],
        duration=round(duration, 3),
        timed_out=timed_out,
        output_tail=output,
        command=cmd,
        xfailed=counts["xfailed"],
        xpassed=counts["xpassed"],
        deselected=counts["deselected"],
        warnings=counts["warnings"],
        collection_errors=counts["collection_errors"],
        summary_line=counts["summary_line"],
    )
    log.info(
        "pytest %s in %s -> %s",
        " ".join(cmd[8:]) or "(default discovery)",
        root,
        result.summary(),
    )
    return result


__all__ = [
    "LAUNCH_FAILED_EXIT_CODE",
    "MAX_OUTPUT_CHARS",
    "TIMEOUT_EXIT_CODE",
    "PytestResult",
    "parse_summary",
    "run_pytest",
    "scrubbed_env",
    "truncate_tail",
]
