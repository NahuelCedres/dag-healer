"""The CODE path: reproduce the bug with a failing test, apply a minimal fix, open a PR.

:func:`run_repair` drives the whole flow for one incident:

1. clone the user's repository (committed HEAD of the checked-out branch) into
   ``<workspace_root or healer_home/work>/<incident_id>`` and create the branch
   ``healer/<dag_id>-<task_id>-<incident_id[:8]>``;
2. phase **test** -- the LLM provider runs a tool session with :class:`RepairTools`
   (``phase="test"``, writes only under the tests folder) and must leave ONE new test file
   that is collected and FAILS when run alone (up to two regenerations);
3. phase **fix** -- a new session (``phase="fix"``, writes only under
   ``repo.allowed_paths``, never in tests) must make the new test pass without breaking the
   pre-existing suite, within ``limits.max_diff_files`` / ``max_diff_lines``
   (up to ``limits.max_fix_attempts`` attempts; failed attempts are rolled back);
4. commit, then push + GitHub pull request when a token and a repository are known,
   otherwise push the branch into the source repository (best effort) and write
   ``PR.md`` + ``fix.patch`` under ``<healer_home>/prs/<incident_id>/``.

``limits.max_tokens_per_incident`` is enforced with a :class:`TokenBudget` shared by all
sessions. The workspace is deleted afterwards unless ``debug`` is on. :func:`run_repair`
never raises: every failure becomes a ``repair_failed`` result with a clear ``error``.

Masking: pytest output and error text pass through :func:`dag_healer.masking.mask_text`
before they reach the LLM, the result or the logs, and the tools refuse to read data files.
"""

from __future__ import annotations

import difflib
import fnmatch
import logging
import os
import posixpath
import re
import textwrap
from collections.abc import Callable, Iterable, Mapping, Sequence
from dataclasses import asdict, dataclass, field, replace
from pathlib import Path, PureWindowsPath
from typing import TYPE_CHECKING, Any, Literal

from dag_healer.agent.budget import TokenBudget
from dag_healer.agent.llm import LLMRefusal, LLMUnavailable
from dag_healer.agent.tests_gen import (
    SYSTEM_PROMPT,
    build_fix_prompt,
    build_test_prompt,
    choose_test_location,
    repro_target,
)
from dag_healer.masking import mask_text
from dag_healer.pr.github import GitHubError, create_pull_request
from dag_healer.pr.prmd import render_pr_markdown, write_pr_artifacts
from dag_healer.repo.git import (
    GitError,
    Workspace,
    github_https_url,
    origin_url,
    parse_github_repo,
    read_text,
    remove_tree,
    run_git,
    sanitize_branch_name,
    write_text_preserving_eol,
)
from dag_healer.repo.profile import detect_test_layout
from dag_healer.repo.pytest_runner import PytestResult, run_pytest

if TYPE_CHECKING:  # pragma: no cover - typing only
    from dag_healer.agent.context import IncidentContext
    from dag_healer.agent.llm import LLMProvider, RepairSessionResult
    from dag_healer.config import HealerConfig
    from dag_healer.repo.profile import TestLayout

logger = logging.getLogger(__name__)

REPAIRED = "repaired"
REPAIR_FAILED = "repair_failed"

#: Identity of the healer's commits.
AUTHOR_NAME = "dag-healer"
AUTHOR_EMAIL = "dag-healer@localhost"

#: First attempt + 2 regenerations of the reproducing test.
MAX_TEST_ATTEMPTS = 3

#: Per-session caps of the tools (runaway protection on top of the token budget).
MAX_WRITES_PER_SESSION = 40
MAX_TEST_RUNS_PER_SESSION = 20
MAX_WRITE_BYTES = 200_000
MAX_READ_LINES = 400
MAX_READ_CHARS = 40_000
MAX_LIST_ENTRIES = 300
MAX_SEARCH_MATCHES = 100
MAX_SEARCH_FILE_BYTES = 1_000_000
MAX_TOOL_PYTEST_CHARS = 6_000
MAX_STORED_PYTEST_CHARS = 20_000
MAX_FEEDBACK_DIFF_CHARS = 4_000

#: Directories never listed or searched.
SKIP_DIRS = frozenset(
    {
        ".git",
        "__pycache__",
        ".venv",
        "venv",
        "node_modules",
        ".mypy_cache",
        ".pytest_cache",
        ".ruff_cache",
        ".tox",
        ".nox",
        ".idea",
        ".vscode",
    }
)
#: File extensions treated as data: never read, never written by the agent.
DATA_SUFFIXES = frozenset(
    {
        ".csv",
        ".tsv",
        ".parquet",
        ".pq",
        ".jsonl",
        ".ndjson",
        ".xlsx",
        ".xls",
        ".avro",
        ".orc",
        ".feather",
        ".arrow",
        ".pkl",
        ".pickle",
        ".db",
        ".sqlite",
        ".sqlite3",
        ".gz",
        ".bz2",
        ".zip",
    }
)
#: Root folders the agent can never write; ``data``/``fixtures`` cannot be read either.
DENIED_ROOTS = frozenset({".github", "data", "fixtures", "healer"})
UNREADABLE_ROOTS = frozenset({"data", "fixtures"})
LOCKFILES = frozenset(
    {
        "poetry.lock",
        "pipfile.lock",
        "uv.lock",
        "pdm.lock",
        "package-lock.json",
        "npm-shrinkwrap.json",
        "yarn.lock",
        "pnpm-lock.yaml",
        "cargo.lock",
        "gemfile.lock",
        "composer.lock",
    }
)
MANIFESTS = frozenset({"pyproject.toml", "setup.py", "setup.cfg", "pipfile", "environment.yml", "tox.ini"})
_MANIFEST_PATTERNS = ("requirements*.txt", "requirements*.in", "constraints*.txt")
_TEST_FILE_PATTERNS = ("test_*.py", "*_test.py")

Phase = Literal["test", "fix"]


# --------------------------------------------------------------------------- results


@dataclass
class RepairResult:
    """Outcome of :func:`run_repair`.

    ``attempts`` counts the agent sessions that were run (test and fix phases).
    ``pytest_before`` / ``pytest_after`` are masked pytest summaries plus output tails.
    """

    status: str
    branch: str | None = None
    test_path: str | None = None
    test_code: str | None = None
    diff: str | None = None
    pytest_before: str | None = None
    pytest_after: str | None = None
    pr_url: str | None = None
    pr_md_path: str | None = None
    patch_path: str | None = None
    attempts: int = 0
    tokens_used: int = 0
    created_tests_dir: bool = False
    notes: list[str] = field(default_factory=list)
    error: str | None = None

    @property
    def ok(self) -> bool:
        """True when the incident was repaired."""
        return self.status == REPAIRED

    def to_dict(self) -> dict[str, Any]:
        """Return a JSON-safe dict view."""
        return asdict(self)


class RepairAborted(RuntimeError):
    """Internal: stops the repair with a ``repair_failed`` result and this message."""


# --------------------------------------------------------------------------- path rules


def _norm_prefix(value: str) -> str:
    cleaned = str(value).replace("\\", "/").strip()
    while cleaned.startswith("./"):
        cleaned = cleaned[2:]
    cleaned = cleaned.strip("/")
    return posixpath.normpath(cleaned) if cleaned else "."


def _under(rel: str, prefix: str) -> bool:
    return prefix == "." or rel == prefix or rel.startswith(prefix + "/")


def _is_test_filename(name: str) -> bool:
    return name == "conftest.py" or any(fnmatch.fnmatchcase(name, p) for p in _TEST_FILE_PATTERNS)


def _is_manifest(name: str) -> bool:
    lowered = name.lower()
    return lowered in MANIFESTS or any(fnmatch.fnmatchcase(lowered, p) for p in _MANIFEST_PATTERNS)


def _is_lockfile(name: str) -> bool:
    lowered = name.lower()
    return lowered in LOCKFILES or lowered.endswith(".lock")


def _is_data_file(name: str) -> bool:
    return Path(name.lower()).suffix in DATA_SUFFIXES


def _format_run(result: PytestResult, targets: Sequence[str] | None, *, limit: int) -> str:
    """Masked one-line summary + output tail of a pytest run."""
    what = " ".join(targets) if targets else "(whole suite)"
    summary = (
        f"$ pytest {what}\n"
        f"exit code {result.exit_code}: {result.passed} passed, {result.failed} failed, "
        f"{result.errors} errors, {result.skipped} skipped, {result.collected} collected "
        f"in {result.duration:.1f}s"
    )
    if result.timed_out:
        summary += " (TIMED OUT)"
    tail = mask_text(result.output_tail or "").strip()
    if len(tail) > limit:
        tail = "[... output truncated ...]\n" + tail[-limit:]
    return f"{summary}\n{tail}" if tail else summary


def _tool_doc(summary: str, details: str, args: Sequence[tuple[str, str]], returns: str) -> str:
    """Build a Google-style docstring (one-line summary first) for a tool function."""
    width = 100
    parts = [summary, "", textwrap.fill(details, width), "", "Args:"]
    for name, text in args:
        parts.append(textwrap.fill(text, width, initial_indent=f"    {name}: ", subsequent_indent="        "))
    parts += ["", "Returns:", textwrap.fill(returns, width, initial_indent="    ", subsequent_indent="    ")]
    return "\n".join(parts) + "\n"


# --------------------------------------------------------------------------- tools


@dataclass
class WriteRecord:
    """One file write performed by the agent through :class:`RepairTools`."""

    path: str
    action: str  # "created" | "overwritten" | "replaced"
    phase: str
    lines: int


class RepairTools:
    """The tools the repair agent uses inside the workspace, with guardrails.

    Every method returns a string (results or an ``"Error: ..."`` message) and never
    raises on bad input. Paths are repo-relative; absolute paths, ``..`` and anything
    resolving outside the workspace are rejected. Phase ``"test"`` may write only under
    ``tests_dir``; phase ``"fix"`` only under ``cfg.repo.allowed_paths`` and never in
    ``tests_dir`` or any test file. Both phases are denied ``healer.yaml``, ``healer/**``,
    ``.github/**``, ``.git/**``, lockfiles, dependency manifests, ``.env*``, ``fixtures/**``,
    ``data/**`` and data files; data files, ``.env*`` and ``healer.yaml`` cannot be read
    either. Every write is recorded (:attr:`writes`) and can be rolled back (:meth:`revert`).
    """

    def __init__(
        self,
        workspace: str | Path,
        cfg: HealerConfig,
        *,
        phase: Phase,
        tests_dir: str,
        python: str | None = None,
        timeout: int | None = None,
        max_writes: int = MAX_WRITES_PER_SESSION,
        max_test_runs: int = MAX_TEST_RUNS_PER_SESSION,
    ) -> None:
        if phase not in ("test", "fix"):
            raise ValueError(f"unknown repair phase: {phase!r}")
        self.root = Path(workspace).resolve()
        self.cfg = cfg
        self.phase: Phase = phase
        self.tests_dir = _norm_prefix(tests_dir or ".")
        self.python = python
        self.timeout = int(timeout or cfg.repo.pytest_timeout)
        self.max_writes = max_writes
        self.max_test_runs = max_test_runs
        self.allowed_paths = [_norm_prefix(p) for p in cfg.repo.allowed_paths if str(p).strip()]
        contracts_dir = str(cfg.contracts.dir).replace("\\", "/")
        relative_contracts = not Path(cfg.contracts.dir).is_absolute() and not contracts_dir.startswith("..")
        self._contracts_prefix = _norm_prefix(contracts_dir) if relative_contracts else None
        self.writes: list[WriteRecord] = []
        self.test_runs: list[PytestResult] = []
        self._originals: dict[str, bytes | None] = {}

    # ------------------------------------------------------------------ records

    @property
    def written_paths(self) -> list[str]:
        """Repo-relative paths written in this session, in first-write order."""
        return list(self._originals)

    def revert(self) -> None:
        """Restore every file written in this session to its original content (or delete it)."""
        for rel, original in reversed(list(self._originals.items())):
            target = self.root / rel
            try:
                if original is None:
                    if target.is_file():
                        target.unlink()
                else:
                    target.parent.mkdir(parents=True, exist_ok=True)
                    target.write_bytes(original)
            except OSError as exc:  # pragma: no cover - best effort
                logger.warning("could not roll back %s: %s", rel, exc)
        self._originals.clear()
        self.writes.clear()

    def session_diff(self, *, limit: int = MAX_FEEDBACK_DIFF_CHARS) -> str:
        """Unified diff of the files written in this session (for feedback to the next attempt)."""
        chunks: list[str] = []
        for rel, original in self._originals.items():
            before = (original or b"").decode("utf-8", errors="replace").replace("\r\n", "\n")
            target = self.root / rel
            after = read_text(target) if target.is_file() else ""
            chunks.extend(
                difflib.unified_diff(
                    before.splitlines(keepends=True),
                    after.splitlines(keepends=True),
                    fromfile=f"a/{rel}" if original is not None else "/dev/null",
                    tofile=f"b/{rel}",
                )
            )
        text = "".join(chunks)
        return text if len(text) <= limit else text[:limit] + "\n[... diff truncated ...]\n"

    # ------------------------------------------------------------------ path checks

    def _resolve(self, path: Any) -> tuple[str | None, str | None]:
        """Return ``(repo-relative posix path, None)`` or ``(None, error message)``."""
        if not isinstance(path, str) or not path.strip():
            return None, "Error: the path must be a non-empty string relative to the repository root."
        raw = path.strip().replace("\\", "/")
        windows_absolute = PureWindowsPath(path.strip()).is_absolute() or re.match(r"^[A-Za-z]:", raw)
        if raw.startswith(("/", "~")) or windows_absolute:
            return None, (
                f"Error: absolute paths are not allowed ({path!r}); use a path relative to the "
                "repository root, for example 'src/etl/module.py'."
            )
        if ":" in raw:
            return None, f"Error: ':' is not allowed in paths ({path!r})."
        parts = [part for part in raw.split("/") if part not in ("", ".")]
        if ".." in parts:
            return None, f"Error: '..' is not allowed in paths ({path!r}); stay inside the repository."
        rel = "/".join(parts) or "."
        try:
            resolved = (self.root / rel).resolve()
        except (OSError, RuntimeError) as exc:
            return None, f"Error: cannot resolve {path!r}: {exc}"
        if resolved != self.root and not resolved.is_relative_to(self.root):
            return None, f"Error: {path!r} points outside the repository."
        return rel, None

    def _read_denial(self, rel: str) -> str | None:
        # Deny checks compare case-insensitively: on Windows/macOS ".GIT" is ".git".
        parts = rel.lower().split("/")
        name = parts[-1]
        if ".git" in parts:
            return f"Error: reading {rel} is denied (.git internals)."
        if name.startswith(".env"):
            return f"Error: reading {rel} is denied (environment files may hold secrets)."
        if name == "healer.yaml":
            return f"Error: reading {rel} is denied (dag-healer configuration)."
        if parts[0] in UNREADABLE_ROOTS or _is_data_file(name):
            return (
                f"Error: reading {rel} is denied: data files must not be read. Use the masked sample "
                "provided in the prompt instead."
            )
        return None

    def _write_denial(self, rel: str) -> str | None:
        if rel == ".":
            return "Error: the path must name a file."
        # Deny checks compare case-insensitively (".GIT" is ".git" on Windows/macOS); the
        # allow checks (tests_dir in the test phase, allowed_paths) stay case-sensitive.
        lowered_rel = rel.lower()
        parts = lowered_rel.split("/")
        name = rel.split("/")[-1]
        lowered = parts[-1]
        contracts = self._contracts_prefix.lower() if self._contracts_prefix else None
        if ".git" in parts:
            return f"Error: writing {rel} is denied (.git internals)."
        if (
            lowered == "healer.yaml"
            or parts[0] in DENIED_ROOTS
            or (contracts and contracts != "." and _under(lowered_rel, contracts))
        ):
            return (
                f"Error: writing {rel} is denied (protected path: healer config, contracts, CI, data or "
                "fixtures)."
            )
        if lowered.startswith(".env"):
            return f"Error: writing {rel} is denied (.env files)."
        if _is_lockfile(name):
            return f"Error: writing {rel} is denied (lockfiles are never changed)."
        if _is_manifest(name):
            return f"Error: writing {rel} is denied (dependency and packaging manifests are never changed)."
        if _is_data_file(lowered):
            return f"Error: writing {rel} is denied (data files); hard-code small inputs in the test instead."
        if self.phase == "test":
            if self.tests_dir == ".":
                if not _is_test_filename(name):
                    return (
                        f"Error: in the test phase you may only write test files (test_*.py, *_test.py, "
                        f"conftest.py); {rel} is not one."
                    )
            elif not _under(rel, self.tests_dir):
                return (
                    f"Error: in the test phase you may only write under {self.tests_dir}/ ({rel} is "
                    "outside). Do not change application code yet."
                )
            return None
        # phase "fix"
        tests_dir = self.tests_dir.lower()
        if (tests_dir != "." and _under(lowered_rel, tests_dir)) or _is_test_filename(lowered):
            return (
                f"Error: in the fix phase tests are read-only ({rel}); change the application code, "
                "not the test."
            )
        if not any(_under(rel, prefix) for prefix in self.allowed_paths):
            allowed = ", ".join(f"{p}/" for p in self.allowed_paths) or "(none configured)"
            return f"Error: writing {rel} is denied: the fix may only change files under {allowed}."
        return None

    def _remember(self, rel: str, target: Path) -> None:
        if rel not in self._originals:
            self._originals[rel] = target.read_bytes() if target.is_file() else None

    def _write_budget_error(self) -> str | None:
        if len(self.writes) >= self.max_writes:
            return f"Error: write limit reached ({self.max_writes} writes in this session)."
        return None

    def _walk(self, start: Path) -> Iterable[Path]:
        if start.is_file():
            yield start
            return
        for dirpath, dirnames, filenames in os.walk(start):
            dirnames[:] = sorted(d for d in dirnames if d not in SKIP_DIRS and not d.endswith(".egg-info"))
            for filename in sorted(filenames):
                yield Path(dirpath) / filename

    def _rel(self, path: Path) -> str:
        return path.resolve().relative_to(self.root).as_posix()

    # ------------------------------------------------------------------ tools

    def list_files(self, path: str = ".", pattern: str = "*") -> str:
        """List repository files under ``path`` whose name or relative path matches ``pattern``."""
        rel, error = self._resolve(path)
        if error:
            return error
        assert rel is not None
        start = self.root / rel
        if not start.exists():
            return f"Error: {rel} does not exist."
        pattern = pattern or "*"
        found: list[str] = []
        total = 0
        for file in self._walk(start):
            file_rel = self._rel(file)
            if fnmatch.fnmatchcase(file.name, pattern) or fnmatch.fnmatchcase(file_rel, pattern):
                total += 1
                if len(found) < MAX_LIST_ENTRIES:
                    found.append(file_rel)
        if not found:
            where = "the repository" if rel == "." else rel
            return f"No files in {where} match {pattern!r}."
        if total > len(found):
            found.append(f"... ({total - len(found)} more; narrow the path or the pattern)")
        return "\n".join(found)

    def read_file(self, path: str, start_line: int = 1, end_line: int = MAX_READ_LINES) -> str:
        """Return lines ``start_line``..``end_line`` of a text file, prefixed with line numbers."""
        rel, error = self._resolve(path)
        if error:
            return error
        assert rel is not None
        denial = self._read_denial(rel)
        if denial:
            return denial
        target = self.root / rel
        if target.is_dir():
            return f"Error: {rel} is a directory; use list_files."
        if not target.is_file():
            return f"Error: {rel} does not exist."
        data = target.read_bytes()
        if b"\0" in data[:8192]:
            return f"Error: {rel} is a binary file."
        lines = read_text(target).split("\n")
        if lines and lines[-1] == "":
            lines.pop()
        total = len(lines)
        try:
            start = max(1, int(start_line))
            end = int(end_line)
        except (TypeError, ValueError):
            return "Error: start_line and end_line must be integers."
        end = min(total, end, start + MAX_READ_LINES - 1)
        if total == 0:
            return f"{rel} is empty."
        if start > total:
            return f"Error: {rel} has only {total} lines."
        if end < start:
            return "Error: end_line must be greater than or equal to start_line."
        width = len(str(end))
        out: list[str] = []
        size = 0
        last = start - 1
        for number in range(start, end + 1):
            line = f"{number:>{width}}| {lines[number - 1]}"
            size += len(line) + 1
            if size > MAX_READ_CHARS and out:
                break
            out.append(line)
            last = number
        header = f"{rel} (lines {start}-{last} of {total})"
        if last < total:
            header += f"; call read_file again with start_line={last + 1} for more"
        return header + "\n" + "\n".join(out)

    def search(self, pattern: str, path: str = ".") -> str:
        """Search text files under ``path`` for the regular expression ``pattern``."""
        if not isinstance(pattern, str) or not pattern:
            return "Error: the pattern must be a non-empty regular expression."
        try:
            regex = re.compile(pattern)
        except re.error as exc:
            return f"Error: invalid regular expression {pattern!r}: {exc}"
        rel, error = self._resolve(path)
        if error:
            return error
        assert rel is not None
        start = self.root / rel
        if not start.exists():
            return f"Error: {rel} does not exist."
        matches: list[str] = []
        truncated = False
        for file in self._walk(start):
            file_rel = self._rel(file)
            if self._read_denial(file_rel):
                continue
            try:
                if file.stat().st_size > MAX_SEARCH_FILE_BYTES:
                    continue
                data = file.read_bytes()
            except OSError:
                continue
            if b"\0" in data[:8192]:
                continue
            text = data.decode("utf-8", errors="replace").replace("\r\n", "\n")
            for number, line in enumerate(text.split("\n"), start=1):
                if regex.search(line):
                    if len(matches) >= MAX_SEARCH_MATCHES:
                        truncated = True
                        break
                    matches.append(f"{file_rel}:{number}: {line.strip()[:300]}")
            if truncated:
                break
        if not matches:
            where = "the repository" if rel == "." else rel
            return f"No matches for {pattern!r} in {where}."
        if truncated:
            matches.append(f"... (more than {MAX_SEARCH_MATCHES} matches; narrow the pattern or the path)")
        return "\n".join(matches)

    def write_file(self, path: str, content: str) -> str:
        """Create or overwrite a text file with ``content``."""
        rel, error = self._resolve(path)
        if error:
            return error
        assert rel is not None
        denial = self._write_denial(rel) or self._write_budget_error()
        if denial:
            return denial
        if not isinstance(content, str):
            return "Error: content must be a string."
        if len(content.encode("utf-8", errors="surrogateescape")) > MAX_WRITE_BYTES:
            return f"Error: content is larger than {MAX_WRITE_BYTES} bytes."
        target = self.root / rel
        if target.is_dir():
            return f"Error: {rel} is a directory."
        existed = target.is_file()
        self._remember(rel, target)
        write_text_preserving_eol(target, content)
        lines = content.count("\n") + (0 if content.endswith("\n") or not content else 1)
        action = "overwritten" if existed else "created"
        self.writes.append(WriteRecord(rel, action, self.phase, lines))
        logger.debug("repair tool (%s) %s %s", self.phase, action, rel)
        return f"OK: {rel} {action} ({lines} lines)."

    def replace_in_file(self, path: str, old: str, new: str) -> str:
        """Replace exactly one occurrence of ``old`` with ``new`` in an existing text file."""
        rel, error = self._resolve(path)
        if error:
            return error
        assert rel is not None
        denial = self._write_denial(rel) or self._write_budget_error()
        if denial:
            return denial
        if not isinstance(old, str) or not isinstance(new, str):
            return "Error: old and new must be strings."
        target = self.root / rel
        if not target.is_file():
            return f"Error: {rel} does not exist; use write_file to create new files."
        text = read_text(target)
        old_lf = old.replace("\r\n", "\n")
        new_lf = new.replace("\r\n", "\n")
        if not old_lf:
            return "Error: old must not be empty."
        count = text.count(old_lf)
        if count == 0:
            return (
                f"Error: the old text was not found in {rel}. Read the file again and copy the exact text "
                "(without the line-number prefixes)."
            )
        if count > 1:
            return (
                f"Error: the old text occurs {count} times in {rel}; include more surrounding lines so it "
                "is unique."
            )
        self._remember(rel, target)
        write_text_preserving_eol(target, text.replace(old_lf, new_lf, 1))
        removed = old_lf.count("\n") + 1
        added = new_lf.count("\n") + 1 if new_lf else 0
        self.writes.append(WriteRecord(rel, "replaced", self.phase, added))
        logger.debug("repair tool (%s) replaced text in %s", self.phase, rel)
        return f"OK: replaced 1 occurrence in {rel} (-{removed} +{added} lines)."

    def default_test_targets(self) -> list[str] | None:
        """Targets of ``run_tests()`` without arguments: the tests folder, else the whole suite."""
        if self.tests_dir != "." and (self.root / self.tests_dir).is_dir():
            return [self.tests_dir]
        return None

    def run_tests(self, paths: list[str] | None = None) -> str:
        """Run pytest on ``paths`` (default: the tests folder) and return a masked summary."""
        if len(self.test_runs) >= self.max_test_runs:
            return f"Error: test-run limit reached ({self.max_test_runs} runs in this session)."
        targets: list[str] | None
        if paths:
            if isinstance(paths, str):
                paths = [paths]
            targets = []
            for item in paths:
                if not isinstance(item, str):
                    return "Error: paths must be a list of strings."
                file_part, sep, node = item.partition("::")
                rel, error = self._resolve(file_part)
                if error:
                    return error
                assert rel is not None
                if not (self.root / rel).exists():
                    return f"Error: {rel} does not exist."
                targets.append(rel + (sep + node if sep else ""))
        else:
            targets = self.default_test_targets()
        result = run_pytest(self.root, targets, timeout=self.timeout, python=self.python)
        self.test_runs.append(result)
        return _format_run(result, targets, limit=MAX_TOOL_PYTEST_CHARS)

    # ------------------------------------------------------------------ provider view

    def as_functions(self) -> list[Callable[..., str]]:
        """Return the tools as plain functions (names, type hints, Google-style docstrings).

        The docstrings become the tool descriptions sent to the model, so they spell out the
        guardrails of the current phase. The functions never raise: unexpected errors are
        returned as ``"Error: ..."`` strings.
        """
        tools = self
        if self.phase == "test":
            where = (
                f"under {self.tests_dir}/ only (this is the test phase: application code is read-only)"
                if self.tests_dir != "."
                else "only test files (test_*.py, *_test.py, conftest.py) in this test phase"
            )
        else:
            allowed = ", ".join(f"{p}/" for p in self.allowed_paths) or "(none)"
            where = (
                f"only under {allowed} (this is the fix phase: {self.tests_dir}/ and every test file are "
                "read-only)"
            )
        denied = (
            "healer.yaml, healer/**, .github/**, .git/**, lockfiles, dependency manifests "
            "(requirements*.txt, pyproject.toml, setup.py, setup.cfg), .env*, data/**, fixtures/** "
            "and data files (.csv, .parquet, .jsonl, ...) are always denied"
        )

        def _guard(func: Callable[..., str], *args: Any) -> str:
            try:
                return func(*args)
            except Exception as exc:  # noqa: BLE001 - tools must answer with text
                logger.warning("repair tool %s failed: %s", func.__name__, mask_text(exc))
                return f"Error: {type(exc).__name__}: {mask_text(exc)}"

        def list_files(path: str = ".", pattern: str = "*") -> str:
            return _guard(tools.list_files, path, pattern)

        def read_file(path: str, start_line: int = 1, end_line: int = MAX_READ_LINES) -> str:
            return _guard(tools.read_file, path, start_line, end_line)

        def search(pattern: str, path: str = ".") -> str:
            return _guard(tools.search, pattern, path)

        def write_file(path: str, content: str) -> str:
            return _guard(tools.write_file, path, content)

        def replace_in_file(path: str, old: str, new: str) -> str:
            return _guard(tools.replace_in_file, path, old, new)

        def run_tests(paths: list[str] | None = None) -> str:
            return _guard(tools.run_tests, paths)

        tests_folder = (
            f"the tests folder {self.tests_dir}/ (or the whole suite while that folder does not exist)"
            if self.tests_dir != "."
            else "the whole test suite"
        )
        list_files.__doc__ = _tool_doc(
            "List the files of the repository recursively, as paths relative to the repository root.",
            f"Caches, virtualenvs and .git are skipped; at most {MAX_LIST_ENTRIES} paths are returned.",
            [
                (
                    "path",
                    "Folder (or single file) to list, relative to the repository root, for example "
                    '"src/etl". Defaults to the root ".".',
                ),
                (
                    "pattern",
                    "Shell-style wildcard matched against the file name or the relative path, for "
                    'example "*.py" or "src/etl/*.py". Defaults to "*".',
                ),
            ],
            'One relative path per line, or a message starting with "Error:".',
        )
        read_file.__doc__ = _tool_doc(
            'Read a text file of the repository; every returned line is prefixed with its number and "| ".',
            "The prefix is not part of the file: never copy it into replace_in_file. At most "
            f"{MAX_READ_LINES} lines are returned per call. Data files (.csv, .parquet, ...), data/**, "
            "fixtures/**, .env* and healer.yaml cannot be read: use the masked sample from the prompt "
            "instead.",
            [
                ("path", 'File path relative to the repository root, for example "src/etl/customers.py".'),
                ("start_line", "First line to return (1-based). Defaults to 1."),
                ("end_line", f"Last line to return (inclusive). Defaults to {MAX_READ_LINES}."),
            ],
            "A header with the line range and the total line count followed by the numbered lines, or a "
            'message starting with "Error:".',
        )
        search.__doc__ = _tool_doc(
            "Search the repository's text files for a Python regular expression, line by line.",
            f"Data files and denied paths are skipped; at most {MAX_SEARCH_MATCHES} matches are returned.",
            [
                (
                    "pattern",
                    'Python regular expression, for example "def parse_signup_date" or "strptime\\(".',
                ),
                (
                    "path",
                    'Folder or file to search, relative to the repository root. Defaults to the root ".".',
                ),
            ],
            'One match per line as "path:line: text", or a message starting with "Error:" (for example '
            "for an invalid regular expression).",
        )
        write_file.__doc__ = _tool_doc(
            "Create a text file or overwrite it completely with the given content.",
            f"Allowed {where}; {denied}. Parent folders are created. An existing file keeps its line "
            "endings (CRLF or LF). Prefer replace_in_file for small changes to existing files.",
            [
                ("path", 'File path relative to the repository root. Absolute paths and ".." are rejected.'),
                ("content", "The complete new content of the file."),
            ],
            '"OK: ..." on success, or a message starting with "Error:" explaining why the write was refused.',
        )
        replace_in_file.__doc__ = _tool_doc(
            "Replace exactly one occurrence of a text fragment in an existing file.",
            "The old text must occur exactly once: copy it verbatim from read_file, without the "
            "line-number prefixes, and include surrounding lines to make it unique. "
            f"Allowed {where}; {denied}. The file keeps its line endings.",
            [
                ("path", "File path relative to the repository root."),
                ("old", "The exact text to replace; it must occur exactly once in the file."),
                ("new", "The replacement text."),
            ],
            '"OK: ..." on success, or a message starting with "Error:" (text not found, found several '
            "times, or path refused).",
        )
        run_tests.__doc__ = _tool_doc(
            "Run pytest from the repository root and return a summary line plus the tail of the output.",
            f"Without paths it runs {tests_folder}. Each run is limited to {self.timeout} seconds and "
            f"at most {self.max_test_runs} runs are allowed per session. The output is masked.",
            [
                (
                    "paths",
                    "Test files, folders or node ids relative to the repository root, for example "
                    '["tests/test_customers.py"] or ["tests/test_customers.py::test_parse"]. Defaults to '
                    "the tests folder.",
                ),
            ],
            '"exit code N: X passed, Y failed, ..." followed by the pytest output tail, or a message '
            'starting with "Error:".',
        )
        return [list_files, read_file, search, write_file, replace_in_file, run_tests]


# --------------------------------------------------------------------------- repair flow


def branch_name(dag_id: str, task_id: str, incident_id: str) -> str:
    """``healer/<dag_id>-<task_id>-<incident_id[:8]>`` made safe for git."""

    def _clean(text: str) -> str:
        return re.sub(r"[^A-Za-z0-9_-]+", "-", str(text)).strip("-_")[:60] or "x"

    return sanitize_branch_name(f"healer/{_clean(dag_id)}-{_clean(task_id)}-{_clean(incident_id)[:8]}")


def _safe_dirname(incident_id: str) -> str:
    return re.sub(r"[^A-Za-z0-9_.-]+", "_", incident_id).strip("._") or "incident"


def _reproduction_problem(result: PytestResult, test_rel: str, max_lines: int, test_code: str) -> str | None:
    """Why a reproducing test is not acceptable (None when it fails as required)."""
    if result.timed_out:
        return f"running {test_rel} timed out; the test must be fast and self-contained."
    if result.collection_error:
        return (
            f"{test_rel} could not be collected or imported (pytest exit code {result.exit_code}); it must "
            "import cleanly and FAIL in an assertion or with the original exception."
        )
    if result.collected == 0:
        return f"pytest collected no test from {test_rel}; define a test function named test_..."
    if result.failed == 0 and result.errors:
        return f"{test_rel} errored during setup instead of failing; do not depend on missing fixtures."
    if result.failed == 0:
        return (
            f"{test_rel} PASSES before any fix, so it does not reproduce the failure. Assert the correct "
            "behaviour for the input that broke the task."
        )
    if test_code.count("\n") + 1 > max_lines:
        return f"{test_rel} is too long; the whole change may not exceed {max_lines} lines."
    return None


class _Repair:
    """State of one :func:`run_repair` call."""

    def __init__(
        self,
        cfg: HealerConfig,
        incident: Mapping[str, Any],
        ctx: IncidentContext | Any,
        provider: LLMProvider,
        *,
        workspace_root: Path | None,
        github_token: str | None,
    ) -> None:
        self.cfg = cfg
        self.incident = dict(incident)
        self.ctx = ctx
        self.provider = provider
        self.github_token = github_token
        self.incident_id = str(self.incident.get("id") or getattr(ctx, "incident_id", "") or "")
        self.dag_id = str(self.incident.get("dag_id") or getattr(ctx, "dag_id", "") or "dag")
        self.task_id = str(self.incident.get("task_id") or getattr(ctx, "task_id", "") or "task")
        self.exception_type = getattr(ctx, "exception_type", None)
        root = Path(workspace_root) if workspace_root is not None else cfg.healer_home / "work"
        self.workspace_path = root / _safe_dirname(self.incident_id or "incident")
        self.budget = TokenBudget(cfg.limits.max_tokens_per_incident)
        self.result = RepairResult(status=REPAIR_FAILED)
        self.ws: Workspace | None = None
        self.layout: TestLayout | None = None
        self.tests_dir = "tests"
        self.test_rel: str | None = None
        self.test_code: str | None = None
        self.kept_paths: set[str] = set()
        self.has_suite = False
        self.suite_targets: list[str] | None = None
        self.baseline: PytestResult | None = None
        self.agent_summary: str | None = None

    # ------------------------------------------------------------------ helpers

    @property
    def notes(self) -> list[str]:
        return self.result.notes

    def _pytest(self, targets: list[str] | None) -> PytestResult:
        assert self.ws is not None
        return run_pytest(self.ws.path, targets, timeout=self.cfg.repo.pytest_timeout)

    def _scenario(self, phase: Phase) -> dict[str, Any]:
        return {
            "phase": phase,
            "dag_id": self.dag_id,
            "task_id": self.task_id,
            "exception_type": self.exception_type,
            "incident_id": self.incident_id,
        }

    def _session(self, phase: Phase, tools: RepairTools, prompt: str) -> RepairSessionResult:
        """Run one agent session, charging its tokens to the incident budget."""
        if self.budget.exhausted:
            raise RepairAborted(self._budget_message(phase))
        self.result.attempts += 1
        logger.info(
            "incident %s: repair session %d (phase %s, %d tokens left)",
            self.incident_id,
            self.result.attempts,
            phase,
            self.budget.remaining,
        )
        try:
            session = self.provider.repair_session(
                system=SYSTEM_PROMPT,
                prompt=prompt,
                tools=tools.as_functions(),
                max_tokens_budget=self.budget.remaining,
                scenario=self._scenario(phase),
            )
        except LLMUnavailable as exc:
            partial = getattr(exc, "partial", None)
            if partial is not None:  # tokens spent before the failure still count
                self.budget.add(partial.input_tokens, partial.output_tokens)
            raise
        self.budget.add(session.input_tokens, session.output_tokens)
        self.result.tokens_used = self.budget.used
        if session.final_text:
            self.agent_summary = mask_text(session.final_text).strip()[:1500] or self.agent_summary
        if getattr(session, "budget_exhausted", False) or self.budget.exhausted:
            raise RepairAborted(self._budget_message(phase))
        return session

    def _budget_message(self, phase: str) -> str:
        return (
            f"The token budget of this incident (limits.max_tokens_per_incident = {self.budget.limit}) was "
            f"exhausted during the {phase} phase after {self.budget.used} tokens."
        )

    def _changed_paths(self) -> tuple[list[str], list[str]]:
        """``(untracked files, paths differing from HEAD)`` of the workspace."""
        assert self.ws is not None
        untracked = self.ws.git("ls-files", "--others", "--exclude-standard", "-z").stdout.split("\0")
        changed = self.ws.git("diff", "--name-only", "-z", "HEAD", "--").stdout.split("\0")
        return [p for p in untracked if p], [p for p in changed if p]

    def _discard_unexpected_changes(self, keep: set[str]) -> list[str]:
        """Undo changes the agent did not make through its tools (pytest side effects, ...)."""
        assert self.ws is not None
        untracked, changed = self._changed_paths()
        discarded: list[str] = []
        for rel in untracked:
            if rel not in keep:
                try:
                    (self.ws.path / rel).unlink()
                    discarded.append(rel)
                except OSError as exc:  # pragma: no cover - best effort
                    logger.warning("could not remove stray file %s: %s", rel, exc)
        for rel in changed:
            if rel in keep:
                continue
            in_head = self.ws.git("cat-file", "-e", f"HEAD:{rel}", check=False).returncode == 0
            if in_head:
                self.ws.git("checkout", "HEAD", "--", rel)
            else:  # registered with --intent-to-add by an earlier diff, then removed
                self.ws.git("rm", "--cached", "--quiet", "--force", "--ignore-unmatch", "--", rel)
                target = self.ws.path / rel
                if target.is_file():
                    target.unlink()
            discarded.append(rel)
        if discarded:
            logger.info("incident %s: discarded unexpected changes: %s", self.incident_id, discarded)
        return discarded

    def _source_branch(self) -> str:
        """Branch to clone: the one checked out in the source working tree, else ``base_branch``."""
        source = self.cfg.repo_root
        current = run_git(["symbolic-ref", "--quiet", "--short", "HEAD"], source, check=False)
        name = current.stdout.strip() if current.returncode == 0 else ""
        base = self.cfg.repo.base_branch
        if not name:
            return base
        if name != base:
            self.notes.append(
                f"The fix is based on `{name}`, the branch checked out in the repository "
                f"(repo.base_branch is `{base}`)."
            )
        return name

    def _note_uncommitted_code(self) -> None:
        """Warn when the deployed working tree has uncommitted code changes (not in the clone)."""
        status = run_git(
            ["status", "--porcelain=v1", "-z", "--untracked-files=no"],
            self.cfg.repo_root,
            env={"GIT_OPTIONAL_LOCKS": "0"},  # never refresh (write) the index of the user's repo
            check=False,
        )
        if status.returncode != 0:
            return
        allowed = [_norm_prefix(p) for p in self.cfg.repo.allowed_paths]
        dirty = sorted(
            {
                entry[3:].replace("\\", "/")
                for entry in status.stdout.split("\0")
                if len(entry) > 3 and entry[3:].endswith(".py")
            }
        )
        dirty = [p for p in dirty if any(_under(p, prefix) for prefix in allowed)]
        if dirty:
            shown = ", ".join(f"`{p}`" for p in dirty[:10])
            self.notes.append(
                f"The repository working tree has uncommitted code changes ({shown}); the fix is based on "
                "the last commit."
            )

    def _source_has_branch(self, name: str) -> bool:
        """True when ``refs/heads/<name>`` exists in the source repository."""
        args = ["rev-parse", "--verify", "--quiet", f"refs/heads/{name}"]
        return run_git(args, self.cfg.repo_root, check=False).returncode == 0

    def _unique_branch(self, name: str) -> str:
        """Avoid clobbering a branch of an earlier attempt that already exists in the source."""
        candidate = name
        counter = 2
        while self._source_has_branch(candidate):
            candidate = f"{name}-{counter}"
            counter += 1
        return candidate

    def _find_written_test(self, tools: RepairTools, expected: str) -> str | None:
        assert self.ws is not None
        candidates = [
            rel
            for rel in tools.written_paths
            if rel.endswith(".py")
            and Path(rel).name != "conftest.py"
            and _is_test_filename(Path(rel).name)
            and (self.ws.path / rel).is_file()
        ]
        if expected in candidates:
            return expected
        return candidates[0] if candidates else None

    # ------------------------------------------------------------------ phases

    def prepare(self) -> None:
        """Clone the repository, create the branch and decide where the test goes."""
        source = self.cfg.repo_root
        probe = run_git(["rev-parse", "--is-inside-work-tree"], source, check=False)
        if probe.returncode != 0 or probe.stdout.strip() != "true":
            raise RepairAborted(
                f"The repository at {source} is not a git repository; dag-healer needs git to prepare a fix."
            )
        if self.workspace_path.exists():
            remove_tree(self.workspace_path)
        branch = self._source_branch()
        self._note_uncommitted_code()
        self.ws = Workspace.clone(source, self.workspace_path, branch=branch)
        name = self._unique_branch(branch_name(self.dag_id, self.task_id, self.incident_id))
        self.ws.create_branch(name)
        self.result.branch = name

        detected = detect_test_layout(self.ws.path)
        layout = detected
        if self.cfg.repo.tests_dir:
            configured = _norm_prefix(self.cfg.repo.tests_dir)
            layout = replace(
                detected,
                tests_dir=configured,
                exists=detected.exists and detected.tests_dir == configured,
            )
        self.layout = layout
        self.tests_dir, test_file, created = choose_test_location(layout, self.ctx, self.ws.path)
        self.test_rel = test_file
        self.result.created_tests_dir = created  # PR.md explains why the folder was created
        if detected.exists and detected.tests_dir:
            self.has_suite = True
            suite_dir = _norm_prefix(detected.tests_dir)
            self.suite_targets = [suite_dir] if suite_dir != "." else None
            self.baseline = self._pytest(self.suite_targets)
            if not self.baseline.ok:
                self.notes.append(
                    "The existing test suite already failed before any change "
                    f"({self.baseline.failed} failed, {self.baseline.errors} errors); the fix is only "
                    "required not to add failures."
                )

    def test_phase(self) -> None:
        """Get ONE new test that is collected and fails before the fix."""
        assert self.ws is not None and self.test_rel is not None
        feedback: list[str] = []
        last_problem = "no test was written"
        for _attempt in range(1, MAX_TEST_ATTEMPTS + 1):
            tools = RepairTools(self.ws.path, self.cfg, phase="test", tests_dir=self.tests_dir)
            prompt = build_test_prompt(
                self.ctx,
                self.layout,
                self.test_rel,
                tests_dir=self.tests_dir,
                created_tests_dir=self.result.created_tests_dir,
                incident=self.incident,
                previous_attempts=feedback,
            )
            self._session("test", tools, prompt)
            written = self._find_written_test(tools, self.test_rel)
            if written is None:
                last_problem = f"no test file was written (expected {self.test_rel})"
                feedback.append(
                    f"No test file was written. Create {self.test_rel} with write_file, then run it "
                    "with run_tests."
                )
                tools.revert()
                continue
            code = read_text(self.ws.path / written)
            before = self._pytest([written])
            problem = _reproduction_problem(before, written, self.cfg.limits.max_diff_lines, code)
            if problem is None:
                self.test_rel = written
                self.test_code = code
                self.kept_paths = set(tools.written_paths)
                self.result.test_path = written
                self.result.test_code = code
                self.result.pytest_before = _format_run(before, [written], limit=MAX_STORED_PYTEST_CHARS)
                short_type = (self.exception_type or "").split(".")[-1]
                if short_type and short_type not in (before.output_tail or ""):
                    self.notes.append(
                        f"The reproducing test fails, but its output does not mention {self.exception_type}; "
                        "check that it fails for the same reason as the task."
                    )
                logger.info("incident %s: reproducing test %s fails as expected", self.incident_id, written)
                return
            last_problem = problem
            feedback.append(
                f"Rejected: {problem}\n\nTest output:\n{_format_run(before, [written], limit=2_000)}\n\n"
                f"Rejected test (rolled back):\n{tools.session_diff()}"
            )
            tools.revert()
        raise RepairAborted(
            f"The repair agent could not produce a failing reproducing test after {MAX_TEST_ATTEMPTS} "
            f"attempts: {last_problem.rstrip('.')}."
        )

    def fix_phase(self) -> None:
        """Get the smallest fix that makes the new test pass without breaking the suite."""
        assert self.ws is not None and self.test_rel is not None and self.test_code is not None
        limits = self.cfg.limits
        feedback: list[str] = []
        last_problem = "no attempt was made"
        for _attempt in range(1, limits.max_fix_attempts + 1):
            tools = RepairTools(self.ws.path, self.cfg, phase="fix", tests_dir=self.tests_dir)
            prompt = build_fix_prompt(
                self.ctx,
                self.test_rel,
                self.result.pytest_before or "",
                test_code=self.test_code,
                tests_dir=self.tests_dir,
                allowed_paths=self.cfg.repo.allowed_paths,
                max_diff_files=limits.max_diff_files,
                max_diff_lines=limits.max_diff_lines,
                incident=self.incident,
                previous_attempts=feedback,
            )
            self._session("fix", tools, prompt)
            problem, output = self._check_fix(tools)
            if problem is None:
                self.kept_paths |= set(tools.written_paths)
                self.result.pytest_after = output
                return
            last_problem = problem
            feedback.append(
                f"Rejected: {problem}\n\n"
                + (f"{output}\n\n" if output else "")
                + f"Rejected change (rolled back):\n{tools.session_diff() or '(no change)'}"
            )
            tools.revert()
            self._discard_unexpected_changes(self.kept_paths)
        raise RepairAborted(
            f"No acceptable fix after {limits.max_fix_attempts} attempt(s): {last_problem.rstrip('.')}."
        )

    def _check_fix(self, tools: RepairTools) -> tuple[str | None, str]:
        """Validate one fix attempt: ``(problem or None, masked pytest output)``."""
        assert self.ws is not None and self.test_rel is not None and self.test_code is not None
        limits = self.cfg.limits
        if not tools.written_paths:
            return "no file was changed; apply the fix with replace_in_file or write_file.", ""
        test_file = self.ws.path / self.test_rel
        if not test_file.is_file() or read_text(test_file) != self.test_code:
            write_text_preserving_eol(test_file, self.test_code)
            return "the reproducing test was modified; tests are read-only in the fix phase.", ""
        self._discard_unexpected_changes(self.kept_paths | set(tools.written_paths))
        files, lines = self.ws.diff_stats()
        if files > limits.max_diff_files or lines > limits.max_diff_lines:
            return (
                f"the change is too large: {files} files and {lines} changed lines (limits: "
                f"{limits.max_diff_files} files, {limits.max_diff_lines} lines, the test included). "
                "Make a smaller fix.",
                "",
            )
        after = self._pytest([self.test_rel])
        output = _format_run(after, [self.test_rel], limit=MAX_STORED_PYTEST_CHARS)
        if not after.ok:
            return f"the reproducing test {self.test_rel} still fails after the fix.", output
        if self.has_suite:
            suite = self._pytest(self.suite_targets)
            output += "\n\n" + _format_run(suite, self.suite_targets, limit=MAX_STORED_PYTEST_CHARS)
            if not self._suite_ok(suite):
                return "the existing test suite fails after the fix.", output
        self._discard_unexpected_changes(self.kept_paths | set(tools.written_paths))
        return None, output

    def _suite_ok(self, suite: PytestResult) -> bool:
        if suite.ok:
            return True
        if suite.timed_out or suite.collection_error:
            return False
        baseline = self.baseline
        if baseline is None or baseline.ok or baseline.timed_out:
            return False
        # The suite was already red: accept when the fix adds no failure (the new test passes).
        return suite.failed + suite.errors <= baseline.failed + baseline.errors

    # ------------------------------------------------------------------ publishing

    def _diagnosis(self) -> str:
        diagnosis = self.incident.get("diagnosis")
        if diagnosis:
            return mask_text(diagnosis)
        exc_type = getattr(self.ctx, "exception_type", None) or "The task"
        message = getattr(self.ctx, "exception_message", None) or getattr(self.ctx, "error_text", "") or ""
        return mask_text(f"{exc_type}: {message}".strip(": "))

    def _title(self) -> str:
        target = repro_target(self.ctx)
        where = target.get("symbol") or self.task_id
        what = self.exception_type or "failure"
        title = f"Fix {what} in {where} ({self.dag_id}.{self.task_id})"
        return title if len(title) <= 100 else title[:97] + "..."

    def _commit_message(self) -> str:
        lines = [
            self._title(),
            "",
            f"Reproducing test: {self.test_rel}",
            f"Incident: {self.incident_id} (DAG {self.dag_id}, task {self.task_id}, "
            f"run {self.incident.get('run_id', '?')})",
            "",
            "Generated by dag-healer",
        ]
        return "\n".join(lines)

    def _render_pr(self, diff: str) -> str:
        notes = list(self.notes)
        if self.agent_summary:
            notes.append(f"Repair agent summary: {self.agent_summary}")
        return render_pr_markdown(
            incident=self.incident,
            diagnosis=self._diagnosis(),
            test_path=self.test_rel or "",
            test_code=self.test_code or "",
            diff=diff,
            pytest_before=self.result.pytest_before or "",
            pytest_after=self.result.pytest_after or "",
            created_tests_dir=self.result.created_tests_dir,
            notes=notes,
            branch=self.result.branch,
        )

    def publish(self) -> None:
        """Commit, then open a GitHub PR or fall back to a local branch + PR.md / fix.patch."""
        assert self.ws is not None and self.result.branch is not None
        self._discard_unexpected_changes(self.kept_paths)
        files, lines = self.ws.diff_stats()
        limits = self.cfg.limits
        if files > limits.max_diff_files or lines > limits.max_diff_lines:
            raise RepairAborted(
                f"The final change exceeds the diff limits ({files} files, {lines} lines; limits "
                f"{limits.max_diff_files} files, {limits.max_diff_lines} lines)."
            )
        diff = self.ws.diff()
        self.result.diff = diff
        sha = self.ws.commit_all(self._commit_message(), author_name=AUTHOR_NAME, author_email=AUTHOR_EMAIL)
        logger.info("incident %s: committed fix %s on %s", self.incident_id, sha[:12], self.result.branch)

        branch = self.result.branch
        token = self.github_token or self.cfg.github.token
        origin = origin_url(self.cfg.repo_root)
        repo_name = self.cfg.github.repo or (parse_github_repo(origin) if origin else None)
        if token and repo_name:
            remote = None
            if origin and parse_github_repo(origin) == repo_name:
                remote = github_https_url(origin)
            remote = remote or f"https://github.com/{repo_name}.git"
            try:
                self.ws.push(remote, branch, token=token)
                self.result.pr_url = create_pull_request(
                    repo=repo_name,
                    head=branch,
                    base=self.cfg.github.pr_base,
                    title=self._title(),
                    body=self._render_pr(diff),
                    token=token,
                )
                logger.info("incident %s: opened %s", self.incident_id, self.result.pr_url)
                return
            except (GitError, GitHubError) as exc:
                self.notes.append(
                    f"Opening a GitHub pull request on {repo_name} failed ({mask_text(exc)[:500]}); "
                    "PR.md and fix.patch were written instead."
                )
                logger.warning("incident %s: GitHub PR failed: %s", self.incident_id, mask_text(exc))
        elif token or repo_name:
            missing = "github.repo (or a GitHub origin remote)" if token else "a GitHub token"
            self.notes.append(f"No pull request was opened because {missing} is missing.")

        try:
            self.ws.push_to_path(self.cfg.repo_root, branch)
            self.notes.append(f"Branch `{branch}` was pushed to the local repository.")
        except GitError as exc:
            self.notes.append(
                f"The branch could not be pushed to the local repository: {mask_text(exc)[:500]}"
            )
            logger.warning(
                "incident %s: push to the source repository failed: %s", self.incident_id, mask_text(exc)
            )
        md_path, patch_path = write_pr_artifacts(
            self.cfg.healer_home, self.incident_id, pr_md=self._render_pr(diff), patch=diff
        )
        self.result.pr_md_path = str(md_path)
        self.result.patch_path = str(patch_path)

    # ------------------------------------------------------------------ driver

    def run(self) -> RepairResult:
        try:
            if not self.incident_id:
                raise RepairAborted("The incident has no id.")
            self.prepare()
            self.test_phase()
            self.fix_phase()
            self.publish()
            self.result.status = REPAIRED
            self.result.error = None
        except RepairAborted as exc:
            self._fail(str(exc))
        except LLMUnavailable as exc:
            self._fail(f"The LLM is unavailable: {mask_text(exc)}")
        except LLMRefusal as exc:
            self._fail(f"The LLM declined to repair this incident: {mask_text(exc)}")
        except GitError as exc:
            self._fail(f"A git command failed: {mask_text(exc)}")
        except Exception as exc:  # noqa: BLE001 - run_repair never raises
            logger.debug("incident %s: unexpected repair error", self.incident_id, exc_info=True)
            self._fail(f"Unexpected error during the repair: {type(exc).__name__}: {mask_text(exc)}")
        finally:
            self.result.tokens_used = self.budget.used
            self._cleanup()
        return self.result

    def _fail(self, message: str) -> None:
        self.result.status = REPAIR_FAILED
        self.result.error = message
        logger.warning("incident %s: repair failed: %s", self.incident_id, message)
        if self.result.diff is None and self.ws is not None and self.ws.path.is_dir():
            try:
                self._discard_unexpected_changes(self.kept_paths)
                self.result.diff = self.ws.diff() or None
            except GitError:  # pragma: no cover - best effort
                pass

    def _cleanup(self) -> None:
        if not self.workspace_path.exists():
            return
        if self.cfg.debug:
            self.notes.append(f"Workspace kept at {self.workspace_path} (debug mode).")
            return
        try:
            remove_tree(self.workspace_path)
        except OSError as exc:  # pragma: no cover - best effort
            logger.warning("could not delete the workspace %s: %s", self.workspace_path, exc)


def run_repair(
    cfg: HealerConfig,
    incident: Mapping[str, Any],
    ctx: IncidentContext | Any,
    provider: LLMProvider,
    *,
    workspace_root: Path | None = None,
    github_token: str | None = None,
) -> RepairResult:
    """Reproduce a CODE incident with a failing test, fix it minimally and publish the fix.

    Args:
        cfg: The dag-healer configuration (``repo``, ``limits``, ``github``, ``debug``).
        incident: The incident as ``Incident.to_dict()`` (``id``, ``dag_id``, ``task_id``,
            ``run_id``, ``diagnosis``, ...).
        ctx: The gathered :class:`~dag_healer.agent.context.IncidentContext` (masked).
        provider: The LLM provider that runs the tool sessions.
        workspace_root: Parent folder of the clone (default ``<healer_home>/work``).
        github_token: Token for the push and the pull request (default ``cfg.github.token``).

    Returns:
        A :class:`RepairResult`; ``status`` is ``"repaired"`` or ``"repair_failed"`` (with
        ``error``). Never raises.
    """
    return _Repair(
        cfg, incident, ctx, provider, workspace_root=workspace_root, github_token=github_token
    ).run()


__all__ = [
    "AUTHOR_EMAIL",
    "AUTHOR_NAME",
    "MAX_TEST_ATTEMPTS",
    "REPAIRED",
    "REPAIR_FAILED",
    "RepairAborted",
    "RepairResult",
    "RepairTools",
    "WriteRecord",
    "branch_name",
    "run_repair",
]
