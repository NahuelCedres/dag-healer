"""Run pytest for a repair: the environment it gets, the sandbox it runs in, the output it leaves.

The repair agent runs code from the user's repository plus tests written by the LLM.
:func:`run_pytest` builds what such a run gets from the worker and reads what it printed;
where pytest runs, and how it is isolated from the worker, is the job of the sandbox it is
given (:mod:`dag_healer.repo.sandbox`, chosen by ``repo.sandbox``). Whatever the sandbox, every
run works on a throw-away copy of the workspace without its ``.git`` folder, so a test can never
change the clone that the worker commits and pushes.

* **Environment** (:func:`scrubbed_env`): no LLM/GitHub/SMTP credentials, no
  ``HEALER_*`` settings, no Airflow metadata-database or JWT settings, no
  ``PYTHONPATH``/``PYTEST_*`` injection, no CI markers that change pytest's report
  (``CI``, ``BUILD_NUMBER``), and nothing whose name looks like a secret.
  Airflow is pointed at a throw-away ``AIRFLOW_HOME`` with an SQLite database in
  unit-test mode, so tests that import Airflow never touch the real deployment.
* **Cloud credentials** are always removed, whatever the orchestrator and the LLM provider (the
  worker may run with cloud permissions, e.g. an AWS task or instance role for Amazon Bedrock):
  the AWS, Google and Azure credential variables (the legacy ``AWS_ACCESS_KEY``,
  ``AWS_SECRET_KEY``, ``AWS_CREDENTIAL_FILE`` and ``EC2_*_KEY`` names included), the Amazon
  Bedrock API key, every setting of the Anthropic SDK (Bedrock endpoints included) and
  botocore's Bedrock endpoint overrides (:data:`CLOUD_SCRUB_EXACT`, :data:`CLOUD_SCRUB_PREFIXES`),
  and the AWS SDKs get no instance-metadata credentials, no shared credential or config file
  and no boto config file (:data:`CLOUD_SANDBOX_ENV`). These rules are applied after the
  caller's ``extra_env``, so an orchestrator override can never bring a credential back.
  ``AWS_REGION``, ``AWS_DEFAULT_REGION`` and the generic ``AWS_ENDPOINT_URL`` stay: they are not
  credentials.
* **Output** goes to a temporary file (not memory) and only the last
  :data:`MAX_OUTPUT_CHARS` characters are kept.
* **Launch failures** (the sandbox could not run pytest at all: an unreachable container
  engine, a missing image, a missing interpreter) are reported with ``exit_code``
  :data:`LAUNCH_FAILED_EXIT_CODE` and :attr:`PytestResult.launch_error`, never as test results.

Without a ``sandbox`` argument the run uses the ``process`` sandbox, which keeps the old
guard-against-accidents properties: the whole process group is killed when ``timeout`` expires
(POSIX: ``start_new_session`` + ``os.killpg``; Windows: best-effort ``taskkill /T``), and
``RLIMIT_AS``, ``RLIMIT_NPROC`` and ``RLIMIT_FSIZE`` are lowered in the child before ``exec``
(POSIX; never raised above the current limits, and skipped when the kernel refuses: macOS does
not let a process lower ``RLIMIT_AS``). It isolates nothing else (see docs/security.md, section
4.3). The command is ``python -m pytest`` so the workspace root is importable (``python -m``
puts the working directory on ``sys.path``), exactly like running the suite from a checkout.

Limitation: when the workspace has no pytest configuration file of its own, pytest looks for
one in the parent directories of the copy (the system's temporary directory for the
``process`` sandbox).
"""

from __future__ import annotations

import logging
import os
import re
import sys
import tempfile
import time
from collections.abc import Mapping, Sequence
from dataclasses import asdict, dataclass, field
from pathlib import Path
from typing import Any

from dag_healer.masking import mask_text
from dag_healer.repo.sandbox import (
    LAUNCH_FAILED_EXIT_CODE,
    TIMEOUT_EXIT_CODE,
    Sandbox,
    SandboxJob,
)

# The process sandbox's resource limits, importable from here as before.
from dag_healer.repo.sandbox import RLIMIT_AS_BYTES as RLIMIT_AS_BYTES
from dag_healer.repo.sandbox import RLIMIT_FSIZE_BYTES as RLIMIT_FSIZE_BYTES
from dag_healer.repo.sandbox import RLIMIT_NPROC_COUNT as RLIMIT_NPROC_COUNT

log = logging.getLogger(__name__)

#: Maximum number of characters kept in :attr:`PytestResult.output_tail`.
MAX_OUTPUT_CHARS = 20000

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
        # pytest reports differently when it sees these (full crash messages in the short
        # summary, untruncated diffs), so the output would depend on the worker's host.
        "CI",
        "BUILD_NUMBER",
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
#: Cloud credential variables always removed, even when ``extra_env`` sets them: the AWS keys,
#: profiles and roles (a worker on AWS may run with them, and with an Amazon Bedrock API key),
#: the legacy names of AWS keys and credential files that older AWS tools read, the Google key
#: file and the Azure secret.
CLOUD_SCRUB_EXACT = frozenset(
    {
        "AWS_ACCESS_KEY_ID",
        "AWS_SECRET_ACCESS_KEY",
        "AWS_SESSION_TOKEN",
        "AWS_SECURITY_TOKEN",
        "AWS_BEARER_TOKEN_BEDROCK",
        "AWS_PROFILE",
        "AWS_DEFAULT_PROFILE",
        "AWS_ROLE_ARN",
        "AWS_ROLE_SESSION_NAME",
        "AWS_WEB_IDENTITY_TOKEN_FILE",
        "AWS_ACCESS_KEY",
        "AWS_SECRET_KEY",
        "AWS_CREDENTIAL_FILE",
        "EC2_ACCESS_KEY",
        "EC2_SECRET_KEY",
        "GOOGLE_APPLICATION_CREDENTIALS",
        "AZURE_CLIENT_SECRET",
    }
)
#: Prefixes of cloud credential variables always removed: the ECS/EKS container credentials,
#: every setting of the Anthropic SDK (its API keys, and the base URLs of the Claude API and of
#: Amazon Bedrock) and botocore's Bedrock endpoint overrides (``AWS_ENDPOINT_URL_BEDROCK_RUNTIME``...).
CLOUD_SCRUB_PREFIXES: tuple[str, ...] = (
    "AWS_CONTAINER_CREDENTIALS_",
    "AWS_CONTAINER_AUTHORIZATION_TOKEN",
    "ANTHROPIC_",
    "AWS_ENDPOINT_URL_BEDROCK",
)
#: Always set: no EC2 instance-metadata credentials, no shared credential or config file, no boto
#: config file. The file variables are set to an empty value rather than removed: without them
#: botocore and boto3 under the ``process`` backend would read the worker user's
#: ``~/.aws/credentials`` and ``~/.aws/config``, and its ``~/.boto`` or ``/etc/boto.cfg`` (the
#: ``boto-config`` credential provider); with an empty value they read none of them.
CLOUD_SANDBOX_ENV: dict[str, str] = {
    "AWS_EC2_METADATA_DISABLED": "true",
    "AWS_SHARED_CREDENTIALS_FILE": "",
    "AWS_CONFIG_FILE": "",
    "BOTO_CONFIG": "",
}

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
    #: Why the sandbox could not run pytest at all (no test result; ``exit_code`` is 3).
    launch_error: str | None = None

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
        if self.launch_error is not None:
            return f"exit {self.exit_code}: the sandbox could not run pytest ({self.launch_error})"
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


def _enforce_cloud_sandbox(env: dict[str, str]) -> dict[str, str]:
    """Remove the cloud credentials from ``env`` and set :data:`CLOUD_SANDBOX_ENV` (in place).

    Names compare case-insensitively (Windows), and a differently cased copy of a
    :data:`CLOUD_SANDBOX_ENV` variable is replaced too.
    """
    fixed = {name.upper() for name in CLOUD_SANDBOX_ENV}
    for name in list(env):
        upper = name.upper()
        if upper in CLOUD_SCRUB_EXACT or upper.startswith(CLOUD_SCRUB_PREFIXES) or upper in fixed:
            del env[name]
    env.update(CLOUD_SANDBOX_ENV)
    return env


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
    settings, ``PYTHONPATH``, ``PYTEST_*``, ``CI``, ``BUILD_NUMBER`` and any name containing
    SECRET/TOKEN/PASSWORD/...)
    and the cloud credentials (``CLOUD_SCRUB_*``: ``AWS_ACCESS_KEY_ID``,
    ``AWS_SECRET_ACCESS_KEY``, ``AWS_SESSION_TOKEN``, ``AWS_SECURITY_TOKEN``,
    ``AWS_BEARER_TOKEN_BEDROCK``, ``AWS_PROFILE``, ``AWS_DEFAULT_PROFILE``, ``AWS_ROLE_ARN``,
    ``AWS_ROLE_SESSION_NAME``, ``AWS_WEB_IDENTITY_TOKEN_FILE``, the legacy ``AWS_ACCESS_KEY``,
    ``AWS_SECRET_KEY``, ``AWS_CREDENTIAL_FILE``, ``EC2_ACCESS_KEY`` and ``EC2_SECRET_KEY``,
    ``AWS_CONTAINER_CREDENTIALS_*``, ``AWS_CONTAINER_AUTHORIZATION_TOKEN*``, ``ANTHROPIC_*``
    (``ANTHROPIC_BEDROCK_BASE_URL`` and ``ANTHROPIC_BEDROCK_MANTLE_BASE_URL`` included),
    ``AWS_ENDPOINT_URL_BEDROCK*``, ``GOOGLE_APPLICATION_CREDENTIALS``, ``AZURE_CLIENT_SECRET``), and
    sets ``AIRFLOW_HOME``, ``AIRFLOW__CORE__UNIT_TEST_MODE=True``,
    ``AIRFLOW__DATABASE__SQL_ALCHEMY_CONN=sqlite:///<home>/unittests.db``,
    ``AIRFLOW__CORE__LOAD_EXAMPLES=False``, ``PYTHONDONTWRITEBYTECODE=1``,
    ``PYTHONIOENCODING=utf-8``, ``PY_COLORS=0``, ``AWS_EC2_METADATA_DISABLED=true`` and empty
    ``AWS_SHARED_CREDENTIALS_FILE`` / ``AWS_CONFIG_FILE`` / ``BOTO_CONFIG``.
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
    return _enforce_cloud_sandbox(env)


# --------------------------------------------------------------------------- output


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
    sandbox: Sandbox,
) -> PytestResult:
    """Run ``python -m pytest -q -p no:cacheprovider -o addopts= [extra_args] [targets]``.

    Args:
        workdir: the workspace root; the run works on a throw-away copy of it (without
            ``.git``) and never changes it.
        targets: test files / node ids relative to ``workdir`` (default: pytest's own
            discovery, i.e. the repository's ``testpaths`` or the whole tree).
        timeout: seconds before the run is killed.
        python: interpreter to use (default: the current one for the ``process`` sandbox,
            ``repo.sandbox.python`` for the ``container`` sandbox).
        extra_args: extra pytest options, placed before the targets.
        extra_env: variables set on top of :func:`scrubbed_env` (``None`` removes one); the
            cloud credential rules are applied again afterwards, so it can neither set a
            cloud credential nor enable the instance metadata service.
        sandbox: where pytest runs (:func:`dag_healer.repo.sandbox.build_sandbox`). It has no
            default, so that no caller runs model-written code without isolation by omission.

    Returns:
        A :class:`PytestResult`. Never raises for test failures, timeouts or launch failures
        (an unusable sandbox, a missing interpreter: ``exit_code`` 3 and
        :attr:`PytestResult.launch_error`).

    Raises:
        FileNotFoundError: ``workdir`` does not exist.
    """
    root = Path(workdir)
    if not root.is_dir():
        raise FileNotFoundError(f"pytest working directory does not exist: {root}")
    pytest_args = [
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
        _enforce_cloud_sandbox(env)
        output_path = tmpdir / "pytest-output.log"
        start = time.monotonic()
        run = sandbox.run(
            SandboxJob(
                workdir=root,
                args=tuple(pytest_args),
                env=env,
                timeout=timeout,
                output=output_path,
                scratch=tmpdir,
                python=python,
            )
        )
        duration = time.monotonic() - start
        output = _read_tail(output_path, MAX_OUTPUT_CHARS) if output_path.is_file() else ""
    command = list(run.command) or [python or sys.executable, "-m", "pytest", *pytest_args]
    shown = " ".join(pytest_args[5:]) or "(default discovery)"

    if run.launch_error is not None:
        message = run.launch_error
        log.warning(
            "pytest %s in %s (%s sandbox) could not start: %s", shown, root, sandbox.name, mask_text(message)
        )
        tail = (
            message if message.startswith("could not start pytest") else f"could not start pytest: {message}"
        )
        return PytestResult(
            exit_code=LAUNCH_FAILED_EXIT_CODE,
            duration=round(duration, 3),
            output_tail=truncate_tail(tail, MAX_OUTPUT_CHARS),
            command=command,
            launch_error=message,
        )
    if run.timed_out:
        note = f"\n[dag-healer] pytest was killed after exceeding the {timeout} s timeout\n"
        output = truncate_tail(output.rstrip("\n") + note, MAX_OUTPUT_CHARS)
    counts = parse_summary(output)
    result = PytestResult(
        exit_code=TIMEOUT_EXIT_CODE if run.timed_out else run.exit_code,
        passed=counts["passed"],
        failed=counts["failed"],
        errors=counts["errors"],
        skipped=counts["skipped"],
        collected=counts["collected"],
        duration=round(duration, 3),
        timed_out=run.timed_out,
        output_tail=output,
        command=command,
        xfailed=counts["xfailed"],
        xpassed=counts["xpassed"],
        deselected=counts["deselected"],
        warnings=counts["warnings"],
        collection_errors=counts["collection_errors"],
        summary_line=counts["summary_line"],
    )
    log.info("pytest %s in %s (%s sandbox) -> %s", shown, root, sandbox.name, result.summary())
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
