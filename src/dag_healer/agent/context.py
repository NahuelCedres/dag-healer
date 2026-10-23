"""Gather the evidence of one incident: log, traceback, code, data contract and masked sample.

:func:`gather_context` is what the worker calls before classification. It collects, best
effort and without ever raising for a missing piece (each gap becomes a ``notes`` entry):

* the log of the failed try from the Airflow REST API (rendered structured log), with the
  Python traceback extracted from it (:func:`extract_traceback`) and the frames that point
  into the user's repository mapped to repo-relative paths (:func:`map_to_repo_path`);
* the DAG file (Airflow's ``fileloc`` mapped into the repository, else the static DAG scan),
  its source, the task as found by :func:`dag_healer.repo.dagscan.scan_dags` and the source of
  the modules involved (DAG file, task callable module, modules in the traceback);
* the DAG's data contract, read from the repository **working tree** (``cfg.repo_root``), and
  the result of validating *every* dataset of that contract against the files currently in
  the working tree (the failing task is often not the one that reads the file). Only an
  approved, non-stale contract produces ``violations``; a draft or stale one produces
  ``draft_violations``, which are evidence only;
* a masked sample of the most relevant dataset (offending rows first).

Everything that can carry data values (error text, exception message, traceback, log
excerpt, violation messages, sample rows) goes through :mod:`dag_healer.masking` before it is
stored on the context, so the context can be persisted, shown and sent to the LLM as is.
Source code is kept verbatim (the repair agent edits it).
"""

from __future__ import annotations

import logging
import os
import posixpath
import re
from collections.abc import Callable, Mapping
from dataclasses import asdict, dataclass, field
from pathlib import Path
from typing import TYPE_CHECKING, Any, Literal

from dag_healer.contracts.schema import Contract, DatasetContract, DatasetLocator, Violation
from dag_healer.core.tracebacks import CAUSE_SEPARATOR, CONTEXT_SEPARATOR
from dag_healer.masking import mask_text, to_json_safe
from dag_healer.repo.dagscan import TaskInfo

if TYPE_CHECKING:
    import pandas as pd

    from dag_healer.airflow.client import AirflowClient
    from dag_healer.config import HealerConfig

logger = logging.getLogger(__name__)

ContractStatus = Literal["approved", "draft", "stale", "missing"]

#: Where the user's repository is mounted in the dag-healer / Airflow containers.
CONTAINER_REPO_ROOT = "/opt/airflow/repo"
#: Airflow's default DAG folder (``$AIRFLOW_HOME/dags``) in the official image.
DEFAULT_AIRFLOW_DAGS_FOLDER = "/opt/airflow/dags"

MAX_LOG_EXCERPT_LINES = 200
MAX_LINE_CHARS = 2000
MAX_ERROR_CHARS = 8000
MAX_TRACEBACK_CHARS = 20000
MAX_SNIPPET_BYTES = 60_000
SNIPPET_CONTEXT_LINES = 40
_MAX_MESSAGE_LINES = 20
_MAX_SUFFIX_PARTS = 8

_HEADER_RE = re.compile(r"^(?P<prefix>[ \t|+]*)(?:Exception Group )?Traceback \(most recent call last\):\s*$")
_FRAME_RE = re.compile(r'^[ \t]+File "(?P<file>[^"\n]+)", line (?P<line>\d+)(?:, in (?P<function>.+?))?\s*$')
_EXC_LINE_RE = re.compile(r"^(?P<type>[A-Za-z_][\w.]*)(?::(?:[ \t](?P<msg>.*))?)?$")
_BARE_EXC_RE = re.compile(
    r"^\s*(?P<type>(?:[A-Za-z_]\w*\.)*[A-Za-z_]\w*"
    r"(?:Error|Exception|Exit|Interrupt|Timeout|Warning|Failure|Iteration|Abort|Killed))"
    r"(?::[ \t](?P<msg>.*))?\s*$"
)
_LOG_PREFIX_RE = re.compile(r"^\s*(?:\[\d{4}-\d{2}-\d{2}|\d{4}-\d{2}-\d{2}[T ]\d{2}:\d{2})")
_GROUP_MARKER_RE = re.compile(r"^\s+[+|]")
_ERRORISH_RE = re.compile(
    r"\b(?:ERROR|CRITICAL|FATAL)\b|exception|error|killed|SIGTERM|SIGKILL|zombie|timed? ?out|heartbeat",
    re.IGNORECASE,
)
_DRIVE_RE = re.compile(r"^[A-Za-z]:/")
_STDLIB_RE = re.compile(r"/(?:lib|lib64)/python\d|/site-packages/|/dist-packages/", re.IGNORECASE)
_SEPARATORS = frozenset({CAUSE_SEPARATOR, CONTEXT_SEPARATOR})


# =========================================================================== the context


@dataclass
class IncidentContext:
    """Everything known about one incident, already masked (see the module docstring).

    ``repo_frames`` are ``{"file": <repo-relative posix path>, "line": int, "function": str}``
    in traceback order (for chained exceptions the root cause's frames come first, the frames
    of the exception that was finally raised last, as Python prints them). ``code_snippets``
    maps repo-relative paths to their source (at most :data:`MAX_SNIPPET_BYTES` in total).
    ``masked_sample`` is ``{"dataset", "columns", "rows", "masked": True}``.
    """

    incident_id: str
    dag_id: str
    run_id: str
    task_id: str
    map_index: int = -1
    try_number: int = 1
    error_text: str = ""
    exception_type: str | None = None
    exception_message: str | None = None
    traceback_text: str = ""
    repo_frames: list[dict[str, Any]] = field(default_factory=list)
    log_excerpt: str = ""
    dag_file: str | None = None
    dag_source: str | None = None
    task: TaskInfo | None = None
    code_snippets: dict[str, str] = field(default_factory=dict)
    contract: Contract | None = None
    contract_status: ContractStatus = "missing"
    violations: list[Violation] = field(default_factory=list)
    draft_violations: list[Violation] = field(default_factory=list)
    sample_dataset: str | None = None
    masked_sample: dict[str, Any] | None = None
    datasets_checked: list[str] = field(default_factory=list)
    notes: list[str] = field(default_factory=list)

    @property
    def top_repo_frame(self) -> dict[str, Any] | None:
        """The last (innermost) traceback frame inside the repository, if any."""
        return self.repo_frames[-1] if self.repo_frames else None

    def to_prompt_dict(self) -> dict[str, Any]:
        """Return a JSON-safe dict of every field (no DataFrames, no pydantic objects)."""
        from dag_healer.contracts.io import contract_to_dict

        data: dict[str, Any] = {
            "incident_id": self.incident_id,
            "dag_id": self.dag_id,
            "run_id": self.run_id,
            "task_id": self.task_id,
            "map_index": self.map_index,
            "try_number": self.try_number,
            "error_text": self.error_text,
            "exception_type": self.exception_type,
            "exception_message": self.exception_message,
            "traceback_text": self.traceback_text,
            "repo_frames": [dict(frame) for frame in self.repo_frames],
            "log_excerpt": self.log_excerpt,
            "dag_file": self.dag_file,
            "dag_source": self.dag_source,
            "task": asdict(self.task) if self.task is not None else None,
            "code_snippets": dict(self.code_snippets),
            "contract": contract_to_dict(self.contract) if self.contract is not None else None,
            "contract_status": self.contract_status,
            "violations": [violation.model_dump(mode="json") for violation in self.violations],
            "draft_violations": [violation.model_dump(mode="json") for violation in self.draft_violations],
            "sample_dataset": self.sample_dataset,
            "masked_sample": self.masked_sample,
            "datasets_checked": list(self.datasets_checked),
            "notes": list(self.notes),
        }
        return to_json_safe(data)


# =========================================================================== tracebacks


@dataclass
class _Block:
    """One ``Traceback (most recent call last):`` block of a text."""

    start: int
    end: int  # exclusive
    prefix: str
    frames: list[dict[str, Any]]
    exc_type: str | None
    exc_message: str | None


def _strip_prefix(line: str, prefix: str) -> str:
    return line[len(prefix) :] if prefix and line.startswith(prefix) else line


def _parse_block(lines: list[str], start: int, prefix: str, *, has_header: bool = True) -> _Block:
    """Parse frames and the exception line of the block starting at ``start``."""
    frames: list[dict[str, Any]] = []
    index = start + 1 if has_header else start
    total = len(lines)
    while index < total:
        if _HEADER_RE.match(lines[index]):
            break  # a nested traceback (exception group member) starts its own block
        line = _strip_prefix(lines[index], prefix)
        match = _FRAME_RE.match(line)
        if match:
            function = (match.group("function") or "").strip() or None
            frames.append(
                {"file": match.group("file"), "line": int(match.group("line")), "function": function}
            )
            index += 1
            continue
        if line.strip() and line[:1] in (" ", "\t"):
            index += 1  # source line, caret markers, "[Previous line repeated N more times]"
            continue
        break
    exc_type: str | None = None
    exc_message: str | None = None
    if index < total:
        line = _strip_prefix(lines[index], prefix).rstrip()
        match = _EXC_LINE_RE.match(line) if line and not _HEADER_RE.match(line) else None
        if match and line.strip() not in _SEPARATORS:
            exc_type = match.group("type")
            parts = [match.group("msg")] if match.group("msg") is not None else []
            index += 1
            while index < total and len(parts) < _MAX_MESSAGE_LINES:
                raw = lines[index]
                follow = _strip_prefix(raw, prefix)
                if (
                    not follow.strip()
                    or _HEADER_RE.match(raw)
                    or follow.strip() in _SEPARATORS
                    or _LOG_PREFIX_RE.match(follow)
                    or _FRAME_RE.match(follow)
                    or _GROUP_MARKER_RE.match(follow)
                ):
                    break
                parts.append(follow.rstrip())
                index += 1
            exc_message = "\n".join(parts).strip() if parts else None
    return _Block(start, index, prefix, frames, exc_type, exc_message)


def _chained(lines: list[str], previous: _Block, block: _Block) -> bool:
    """True when ``block`` follows ``previous`` with only a chain separator in between."""
    between = [_strip_prefix(line, block.prefix).strip() for line in lines[previous.end : block.start]]
    separators = [line for line in between if line]
    return bool(separators) and all(line in _SEPARATORS for line in separators)


def extract_traceback(text: str | None) -> tuple[str | None, str | None, list[dict[str, Any]], str]:
    """Find the last Python traceback in ``text``.

    Returns ``(exception_type, exception_message, frames, traceback_text)`` where ``frames``
    are ``{"file", "line", "function"}`` dicts with the paths exactly as printed. Chained
    exceptions ("direct cause" / "during handling") are kept together: ``traceback_text``
    spans the whole chain, ``frames`` holds the frames of every exception of the chain in
    printed order and the exception type/message are those of the exception raised last.
    Without a traceback, the last line that looks like ``SomeError: message`` is used. The
    result is not masked: callers mask what they keep.
    """
    if not text:
        return None, None, [], ""
    lines = str(text).replace("\r\n", "\n").replace("\r", "\n").split("\n")
    blocks: list[_Block] = []
    index = 0
    while index < len(lines):
        header = _HEADER_RE.match(lines[index])
        if header:
            # "  + Exception Group Traceback" continues with "  |   File ..." lines
            block = _parse_block(lines, index, header.group("prefix").replace("+", "|"))
            blocks.append(block)
            index = max(block.end, index + 1)
            continue
        index += 1
    if not blocks:
        first_frame = next((i for i, line in enumerate(lines) if _FRAME_RE.match(line)), None)
        if first_frame is not None:  # the header was cut off (truncated error text)
            blocks.append(_parse_block(lines, first_frame, "", has_header=False))
    if not blocks:
        for line in reversed(lines):
            match = _BARE_EXC_RE.match(line)
            if match:
                message = match.group("msg")
                return match.group("type"), message.strip() if message else None, [], line.strip()
        return None, None, [], ""

    candidates = [block for block in blocks if block.exc_type] or blocks
    last = candidates[-1]
    position = blocks.index(last)
    chain = [last]
    while position > 0 and _chained(lines, blocks[position - 1], blocks[position]):
        position -= 1
        chain.insert(0, blocks[position])
    frames = [frame for block in chain for frame in block.frames]
    traceback_text = "\n".join(lines[chain[0].start : chain[-1].end]).strip("\n")
    return last.exc_type, last.exc_message, frames, traceback_text


# =========================================================================== paths


def _posix(path: str | os.PathLike[str]) -> str:
    return str(path).replace("\\", "/")


def _is_windows_path(path: str) -> bool:
    return bool(_DRIVE_RE.match(path))


def _relative_to(path: str, prefix: str) -> str | None:
    """``path`` relative to ``prefix`` (both normalised posix), or ``None``."""
    prefix = prefix.rstrip("/") or "/"
    if _is_windows_path(path) or _is_windows_path(prefix):
        path_cmp, prefix_cmp = path.lower(), prefix.lower()
    else:
        path_cmp, prefix_cmp = path, prefix
    if prefix_cmp == "/":
        return path[1:] if path.startswith("/") else None
    if path_cmp.startswith(prefix_cmp + "/"):
        return path[len(prefix) + 1 :]
    return None


def _join_rel(base: str | None, rel: str) -> str | None:
    """Join a repo-relative base directory and a relative path; ``None`` if it escapes."""
    joined = (
        posixpath.normpath(posixpath.join(base, rel)) if base and base != "." else posixpath.normpath(rel)
    )
    if joined in (".", "") or joined == ".." or joined.startswith("../") or joined.startswith("/"):
        return None
    return joined


def _path_roots(cfg: HealerConfig) -> list[tuple[str, str]]:
    """``(absolute posix prefix, repo-relative base)`` pairs, longest prefix first."""
    dags_dir = cfg.repo.dags_dir or "dags"
    roots: list[tuple[str, str]] = [
        (posixpath.normpath(_posix(cfg.repo_root)), ""),
        (CONTAINER_REPO_ROOT, ""),
    ]
    env_dags = os.environ.get("AIRFLOW__CORE__DAGS_FOLDER")
    if env_dags and env_dags.strip():
        roots.append((posixpath.normpath(_posix(env_dags.strip())), dags_dir))
    roots.append((DEFAULT_AIRFLOW_DAGS_FOLDER, dags_dir))
    unique: dict[str, str] = {}
    for prefix, base in roots:
        unique.setdefault(prefix, base)
    return sorted(unique.items(), key=lambda item: len(item[0]), reverse=True)


def _suffix_match(path: str, cfg: HealerConfig) -> str | None:
    """Map an unknown absolute path by its longest suffix that exists in the repository."""
    root = cfg.repo_root
    if _STDLIB_RE.search(path) or not root.is_dir():
        return None
    parts = [part for part in path.split("/") if part and part not in (".", "..")]
    if _is_windows_path(path):
        parts = parts[1:]
    parts = parts[-_MAX_SUFFIX_PARTS:]
    dags_dir = cfg.repo.dags_dir or "dags"
    for size in range(len(parts), 0, -1):
        candidate = "/".join(parts[-size:])
        if size >= 2 and (root / candidate).is_file():
            return candidate
        in_dags = _join_rel(dags_dir, candidate)
        if in_dags and (root / in_dags).is_file():
            return in_dags
    return None


def map_to_repo_path(path: str | os.PathLike[str] | None, cfg: HealerConfig) -> str | None:
    """Map a file path seen in Airflow (logs, ``fileloc``) to a repo-relative posix path.

    Understands the container mount (``/opt/airflow/repo/src/etl/x.py`` -> ``src/etl/x.py``),
    the Airflow DAG folder (``$AIRFLOW__CORE__DAGS_FOLDER`` or ``/opt/airflow/dags``:
    ``<dags_folder>/x.py`` -> ``<repo.dags_dir>/x.py``), any absolute path under
    ``cfg.repo_root``, relative paths that exist in the repository (or in its DAG folder) and,
    as a last resort, other absolute paths whose tail exists in the repository (DAG bundles
    cloned elsewhere). Library paths (site-packages, the standard library) and pseudo files
    such as ``<frozen ...>`` return ``None``.
    """
    if path is None:
        return None
    raw = _posix(path).strip()
    if not raw or raw.startswith("<"):
        return None
    if "/site-packages/" in raw.lower() or "/dist-packages/" in raw.lower():
        return None
    if raw.startswith("/") or _is_windows_path(raw):
        normalised = posixpath.normpath(raw)
        for prefix, base in _path_roots(cfg):
            rel = _relative_to(normalised, prefix)
            if rel:
                return _join_rel(base, rel)
        return _suffix_match(normalised, cfg)
    rel = _join_rel(None, raw)
    if rel is None:
        return None
    root = cfg.repo_root
    if (root / rel).is_file():
        return rel
    in_dags = _join_rel(cfg.repo.dags_dir or "dags", rel)
    if in_dags and (root / in_dags).is_file():
        return in_dags
    return None


def _inside_repo(cfg: HealerConfig, rel: str) -> Path | None:
    """Absolute path of ``rel`` when it is a file inside the repository."""
    try:
        root = cfg.repo_root
        candidate = (root / rel).resolve()
        candidate.relative_to(root)
    except (OSError, ValueError):
        return None
    return candidate if candidate.is_file() else None


def _read_source(path: Path) -> str:
    return path.read_text(encoding="utf-8", errors="replace")


# =========================================================================== helpers


def _as_int(value: Any, default: int) -> int:
    try:
        return int(value)
    except (TypeError, ValueError):
        return default


def _tail(text: str, limit: int) -> str:
    """Keep the end of ``text`` (where tracebacks put the exception)."""
    if len(text) <= limit:
        return text
    return "[... truncated ...]\n" + text[-(limit - 20) :]


def _note(ctx: IncidentContext, message: str) -> None:
    text = mask_text(message)
    ctx.notes.append(text)
    logger.debug("incident %s: %s", ctx.incident_id, text)


def _safely(ctx: IncidentContext, what: str, func: Callable[[], Any]) -> Any:
    """Run one gathering step; failures become notes (masked, no traceback)."""
    try:
        return func()
    except Exception as exc:
        reason = mask_text(f"{type(exc).__name__}: {exc}")[:500]
        logger.warning("dag-healer: %s failed for incident %s: %s", what, ctx.incident_id, reason)
        ctx.notes.append(f"{what} failed: {reason}")
        return None


def _truncate_line(line: str) -> str:
    return line if len(line) <= MAX_LINE_CHARS else line[:MAX_LINE_CHARS] + " [... line truncated ...]"


def _log_excerpt(text: str) -> str:
    """At most :data:`MAX_LOG_EXCERPT_LINES` lines around the last traceback (masked)."""
    lines = text.replace("\r\n", "\n").split("\n")
    while lines and not lines[-1].strip():
        lines.pop()
    if len(lines) > MAX_LOG_EXCERPT_LINES:
        budget = MAX_LOG_EXCERPT_LINES - 2  # room for the "omitted" markers
        focus = next((i for i in range(len(lines) - 1, -1, -1) if _HEADER_RE.match(lines[i])), None)
        if focus is None:
            focus = next(
                (i for i in range(len(lines) - 1, -1, -1) if re.search(r"\b(?:ERROR|CRITICAL)\b", lines[i])),
                len(lines) - 1,
            )
        start = max(0, focus - 60)
        end = min(len(lines), start + budget)
        start = max(0, end - budget)
        selected = lines[start:end]
        if start:
            selected.insert(0, f"[... {start} earlier log lines omitted ...]")
        if end < len(lines):
            selected.append(f"[... {len(lines) - end} later log lines omitted ...]")
        lines = selected
    return mask_text("\n".join(_truncate_line(line) for line in lines))


def _error_lines(text: str, limit: int = 20) -> str:
    """The last error-looking lines of a log without a traceback."""
    lines = [line for line in text.split("\n") if line.strip()]
    matches = [line for line in lines if _ERRORISH_RE.search(line)]
    return "\n".join((matches or lines)[-limit:])


def _mask_tree(value: Any) -> Any:
    if isinstance(value, str):
        return mask_text(value)
    if isinstance(value, Mapping):
        return {key: _mask_tree(item) for key, item in value.items()}
    if isinstance(value, (list, tuple)):
        return [_mask_tree(item) for item in value]
    return value


def _mask_violation(violation: Violation) -> Violation:
    return violation.model_copy(
        update={"message": mask_text(violation.message), "actual": _mask_tree(violation.actual)}
    )


# =========================================================================== gathering steps


def _fetch_log(ctx: IncidentContext, client: AirflowClient | Any | None) -> str:
    if client is None:
        _note(ctx, "No Airflow API client: the task log was not fetched.")
        return ""
    text = client.get_task_log(ctx.dag_id, ctx.run_id, ctx.task_id, ctx.try_number, map_index=ctx.map_index)
    text = text or ""
    if not text.strip():
        _note(ctx, f"The Airflow log of try {ctx.try_number} is empty.")
    return text


def _apply_error_info(cfg: HealerConfig, ctx: IncidentContext, log_text: str, explicit: str) -> None:
    exc_type, exc_message, frames, traceback_text = extract_traceback(log_text)
    if exc_type is None and not frames and explicit:
        parsed = extract_traceback(explicit)
        if parsed[0] is not None or parsed[2]:
            exc_type, exc_message, frames, traceback_text = parsed
    ctx.exception_type = exc_type
    ctx.exception_message = mask_text(exc_message) if exc_message is not None else None
    ctx.traceback_text = _tail(mask_text(traceback_text), MAX_TRACEBACK_CHARS)
    repo_frames: list[dict[str, Any]] = []
    for frame in frames:
        rel = map_to_repo_path(frame.get("file"), cfg)
        if rel:
            repo_frames.append({"file": rel, "line": frame.get("line"), "function": frame.get("function")})
    ctx.repo_frames = repo_frames
    if frames and not repo_frames:
        _note(ctx, "No traceback frame points into the repository.")
    if exc_type is None and not frames:
        _note(ctx, "No Python traceback was found in the log or the error text.")

    source = explicit.strip() or traceback_text or (_error_lines(log_text) if log_text else "")
    ctx.error_text = _tail(mask_text(source), MAX_ERROR_CHARS)
    excerpt_source = log_text if log_text.strip() else explicit
    ctx.log_excerpt = _log_excerpt(excerpt_source) if excerpt_source.strip() else ""


def _api_dag_file(cfg: HealerConfig, ctx: IncidentContext, client: Any) -> str | None:
    """The DAG file according to Airflow (``fileloc`` / ``relative_fileloc``), repo-relative."""
    if client is None:
        return None
    dag = _safely(ctx, "Reading the DAG from the Airflow API", lambda: client.get_dag(ctx.dag_id))
    if not isinstance(dag, Mapping):
        return None
    mapped = map_to_repo_path(dag.get("fileloc"), cfg) if dag.get("fileloc") else None
    if mapped and _inside_repo(cfg, mapped):
        return mapped
    relative = dag.get("relative_fileloc")
    if isinstance(relative, str) and relative.strip():
        candidate = _join_rel(cfg.repo.dags_dir or "dags", _posix(relative.strip()))
        if candidate and _inside_repo(cfg, candidate):
            return candidate
    if mapped:
        _note(ctx, f"Airflow reports the DAG file {mapped}, which is not in the repository working tree.")
    return None


def _apply_repo_info(cfg: HealerConfig, ctx: IncidentContext, client: Any) -> None:
    from dag_healer.repo.dagscan import scan_dags

    dags = _safely(ctx, "Scanning the DAG folder", lambda: scan_dags(cfg.repo_root, cfg.repo.dags_dir))
    dag_info = next((dag for dag in dags or [] if dag.dag_id == ctx.dag_id), None)
    dag_file = _api_dag_file(cfg, ctx, client)
    if dag_file is None and dag_info is not None:
        dag_file = dag_info.file
    ctx.dag_file = dag_file
    if dag_file is None:
        _note(ctx, f"The file defining DAG {ctx.dag_id} was not found in the repository.")

    local = _inside_repo(cfg, dag_file) if dag_file else None
    if local is not None:
        ctx.dag_source = _safely(ctx, "Reading the DAG file", lambda: _read_source(local))
    if ctx.dag_source is None and client is not None:
        source = _safely(
            ctx, "Reading the DAG source from the Airflow API", lambda: client.get_dag_source(ctx.dag_id)
        )
        ctx.dag_source = source if isinstance(source, str) else None
    if ctx.dag_source is None:
        _note(ctx, "The DAG source is not available.")

    if dag_info is not None:
        ctx.task = dag_info.task(ctx.task_id)
    if ctx.task is None:
        _note(ctx, f"Task {ctx.task_id} was not found by the static scan of DAG {ctx.dag_id}.")


def _snippet_window(text: str, focus_lines: list[int], budget: int) -> str | None:
    """Lines around ``focus_lines`` (1-based) of ``text`` that fit in ``budget`` bytes."""
    lines = text.split("\n")
    for context in (SNIPPET_CONTEXT_LINES, SNIPPET_CONTEXT_LINES // 2, 10, 3):
        wanted: set[int] = set()
        for line in focus_lines:
            low = max(1, line - context)
            wanted.update(range(low, min(len(lines), line + context) + 1))
        if not wanted:
            return None
        parts: list[str] = []
        previous = 0
        for number in sorted(wanted):
            if number != previous + 1:
                parts.append(f"# ... (lines {previous + 1}-{number - 1} omitted by dag-healer) ...")
            parts.append(lines[number - 1])
            previous = number
        if previous < len(lines):
            parts.append(f"# ... (lines {previous + 1}-{len(lines)} omitted by dag-healer) ...")
        snippet = "\n".join(parts)
        if len(snippet.encode("utf-8")) <= budget:
            return snippet
    return None


def _apply_code_snippets(cfg: HealerConfig, ctx: IncidentContext) -> None:
    ordered: list[str] = []
    if ctx.dag_file:
        ordered.append(ctx.dag_file)
    if ctx.task is not None and ctx.task.callable_file:
        ordered.append(_posix(ctx.task.callable_file))
    frame_lines: dict[str, list[int]] = {}
    for frame in reversed(ctx.repo_frames):  # innermost first
        frame_lines.setdefault(frame["file"], [])
        if isinstance(frame.get("line"), int):
            frame_lines[frame["file"]].append(frame["line"])
    ordered.extend(frame_lines)

    remaining = MAX_SNIPPET_BYTES
    seen: set[str] = set()
    for rel in ordered:
        if rel in seen:
            continue
        seen.add(rel)
        path = _inside_repo(cfg, rel)
        if path is None:
            _note(ctx, f"Source file {rel} is not in the repository working tree.")
            continue
        text = _read_source(path)
        size = len(text.encode("utf-8"))
        if size <= remaining:
            ctx.code_snippets[rel] = text
            remaining -= size
            continue
        focus = frame_lines.get(rel) or ([ctx.task.lineno] if ctx.task and rel == ctx.dag_file else [])
        snippet = _snippet_window(text, focus, remaining) if focus else None
        if snippet is None:
            _note(ctx, f"Source file {rel} was left out of the context (size limit).")
            continue
        ctx.code_snippets[rel] = snippet
        remaining -= len(snippet.encode("utf-8"))


def _load_frames(cfg: HealerConfig, datasets: list[DatasetContract]) -> dict[str, Any]:
    """Dataset name -> DataFrame, or the exception raised while loading it."""
    from dag_healer.datasets import load_dataset

    frames: dict[str, Any] = {}
    for dataset in datasets:
        try:
            frames[dataset.name] = load_dataset(dataset.locator, cfg.repo_root)
        except Exception as exc:  # DatasetError and anything a reader raises
            frames[dataset.name] = exc
    return frames


def _sample_rows(df: pd.DataFrame, positions: list[int], limit: int) -> pd.DataFrame:
    """Offending rows first, then the first rows, ``limit`` rows at most."""
    chosen: list[int] = []
    for position in positions:
        if 0 <= position < len(df) and position not in chosen:
            chosen.append(position)
        if len(chosen) >= limit:
            break
    for position in range(len(df)):
        if len(chosen) >= limit:
            break
        if position not in chosen:
            chosen.append(position)
    return df.iloc[chosen]


def _build_sample(
    cfg: HealerConfig,
    ctx: IncidentContext,
    name: str,
    df: pd.DataFrame,
    *,
    pii_columns: list[str],
    positions: list[int],
) -> None:
    from dag_healer.masking import masked_sample, masking_settings

    settings = masking_settings(cfg.masking)
    subset = _sample_rows(df, positions, settings.sample_rows)
    rows = masked_sample(
        subset,
        settings,
        pii_columns=pii_columns,
        salt=ctx.incident_id,
        context={
            "dag_id": ctx.dag_id,
            "task_id": ctx.task_id,
            "incident_id": ctx.incident_id,
            "dataset": name,
        },
        limit=settings.sample_rows,
    )
    columns = list(rows[0].keys()) if rows else [str(column) for column in df.columns]
    ctx.sample_dataset = name
    ctx.masked_sample = {"dataset": name, "columns": columns, "rows": rows, "masked": True}


def _sample_from_contract(
    cfg: HealerConfig,
    ctx: IncidentContext,
    contract: Contract,
    frames: Mapping[str, Any],
    violations: list[Violation],
) -> bool:
    import pandas as pd

    loaded = [dataset for dataset in contract.datasets if isinstance(frames.get(dataset.name), pd.DataFrame)]
    if not loaded:
        return False
    by_name = {dataset.name: dataset for dataset in loaded}
    chosen: DatasetContract | None = None
    for violation in violations:
        if violation.dataset in by_name:
            chosen = by_name[violation.dataset]
            break
    if chosen is None:
        chosen = next((dataset for dataset in loaded if dataset.task_id == ctx.task_id), loaded[0])
    positions = [
        position
        for violation in violations
        if violation.dataset == chosen.name
        for position in violation.row_indexes
    ]
    _build_sample(
        cfg,
        ctx,
        chosen.name,
        frames[chosen.name],
        pii_columns=[column.name for column in chosen.columns if column.pii],
        positions=positions,
    )
    return True


def _sample_from_config(cfg: HealerConfig, ctx: IncidentContext) -> None:
    """Without a usable contract, sample the dataset configured in ``healer.yaml``."""
    from dag_healer.datasets import load_dataset

    locators: dict[str, DatasetLocator] = cfg.dataset_locators(ctx.dag_id)
    if not locators:
        return
    task_id = ctx.task_id if ctx.task_id in locators else next(iter(locators))
    locator = locators[task_id]
    df = load_dataset(locator, cfg.repo_root)
    name = Path(locator.path).stem or task_id
    pii: list[str] = []
    if ctx.contract is not None:  # reuse the PII flags of the (unusable) contract
        pii = [column.name for dataset in ctx.contract.datasets for column in dataset.columns if column.pii]
    _build_sample(cfg, ctx, name, df, pii_columns=pii, positions=[])


def _apply_contract(cfg: HealerConfig, ctx: IncidentContext) -> None:
    from dag_healer.contracts.io import contract_path, is_stale, load_contract
    from dag_healer.contracts.schema import ContractError
    from dag_healer.contracts.validate import validate_contract

    contract: Contract | None = None
    try:
        path = contract_path(cfg.contracts_dir, ctx.dag_id)
    except ValueError as exc:
        _note(ctx, f"No contract lookup for this DAG id: {exc}")
        path = None
    if path is not None and not path.is_file() and path.with_suffix(".yml").is_file():
        path = path.with_suffix(".yml")
    if path is not None and path.is_file():
        try:
            contract = load_contract(path)
        except ContractError as exc:
            _note(ctx, f"The data contract file is invalid and was ignored: {exc}")
    elif path is not None:
        _note(ctx, f"No data contract for DAG {ctx.dag_id} ({path.name} not found in {cfg.contracts.dir}).")

    if contract is None:
        ctx.contract_status = "missing"
        _safely(ctx, "Sampling the configured dataset", lambda: _sample_from_config(cfg, ctx))
        return

    ctx.contract = contract
    if contract.is_approved:
        ctx.contract_status = "stale" if is_stale(contract) else "approved"
    else:
        ctx.contract_status = "draft"
    if ctx.contract_status == "stale":
        _note(
            ctx, "The contract was edited after its approval: it is not enforced until it is approved again."
        )
    elif ctx.contract_status == "draft":
        _note(ctx, "The contract is a draft: its violations are evidence only.")

    ctx.datasets_checked = [dataset.name for dataset in contract.datasets]
    if not contract.datasets:
        _note(ctx, "The contract has no datasets.")
    frames = _load_frames(cfg, contract.datasets)
    violations = [_mask_violation(violation) for violation in validate_contract(contract, frames)]
    if ctx.contract_status == "approved":
        ctx.violations = violations
    else:
        ctx.draft_violations = violations

    sampled = _safely(
        ctx, "Sampling the dataset", lambda: _sample_from_contract(cfg, ctx, contract, frames, violations)
    )
    if not sampled and ctx.masked_sample is None:
        _safely(ctx, "Sampling the configured dataset", lambda: _sample_from_config(cfg, ctx))


# =========================================================================== entry point


def gather_context(
    cfg: HealerConfig,
    incident: Mapping[str, Any],
    client: AirflowClient | Any | None,
    *,
    error_text: str | None = None,
) -> IncidentContext:
    """Collect the evidence of ``incident`` (an ``Incident.to_dict()``-shaped mapping).

    ``client`` is an :class:`~dag_healer.airflow.client.AirflowClient` (or any object with
    ``get_task_log``, ``get_dag`` and ``get_dag_source``) or ``None``; without it the error
    comes from ``error_text`` or the incident's ``error_summary``. Contracts, datasets and code
    are read from ``cfg.repo_root`` (the repository working tree). Never raises for missing
    or broken pieces: they are recorded in ``notes``.
    """
    ctx = IncidentContext(
        incident_id=str(incident.get("id") or ""),
        dag_id=str(incident.get("dag_id") or ""),
        run_id=str(incident.get("run_id") or ""),
        task_id=str(incident.get("task_id") or ""),
        map_index=_as_int(incident.get("map_index"), -1),
        try_number=_as_int(incident.get("try_number"), 1),
    )
    explicit = str(error_text if error_text is not None else (incident.get("error_summary") or ""))
    log_text = _safely(ctx, "Fetching the task log from Airflow", lambda: _fetch_log(ctx, client)) or ""
    _safely(ctx, "Extracting the error", lambda: _apply_error_info(cfg, ctx, log_text, explicit))
    if not ctx.error_text and explicit:
        ctx.error_text = _tail(mask_text(explicit), MAX_ERROR_CHARS)
    _safely(ctx, "Locating the DAG and task", lambda: _apply_repo_info(cfg, ctx, client))
    _safely(ctx, "Collecting source code", lambda: _apply_code_snippets(cfg, ctx))
    _safely(ctx, "Checking the data contract", lambda: _apply_contract(cfg, ctx))
    logger.info(
        "Context for incident %s (%s.%s): exception=%s, repo_frames=%d, contract=%s, "
        "violations=%d, draft_violations=%d, notes=%d",
        ctx.incident_id,
        ctx.dag_id,
        ctx.task_id,
        ctx.exception_type,
        len(ctx.repo_frames),
        ctx.contract_status,
        len(ctx.violations),
        len(ctx.draft_violations),
        len(ctx.notes),
    )
    return ctx


__all__ = [
    "CONTAINER_REPO_ROOT",
    "ContractStatus",
    "IncidentContext",
    "extract_traceback",
    "gather_context",
    "map_to_repo_path",
]
