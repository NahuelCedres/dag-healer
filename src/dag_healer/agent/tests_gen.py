"""Prompts and placement for the reproducing test (phase "test") and the minimal fix (phase "fix").

The repair agent works in two phases (see :mod:`dag_healer.agent.repair`):

1. **test** -- write ONE focused pytest test that reproduces the failure and fails now;
2. **fix** -- make the smallest change that turns that test green without breaking the
   existing suite.

This module decides where the new test goes (:func:`choose_test_location`) and renders
the phase prompts from ``agent/prompts/repair_test.md`` and ``repair_fix.md``. Every piece
of evidence placed in a prompt comes from the (already masked) :class:`IncidentContext`;
free text is passed through :func:`dag_healer.masking.mask_text` once more as a safety net.
Source code is included verbatim (it is code, not data).
"""

from __future__ import annotations

import json
import logging
import posixpath
import re
from collections.abc import Mapping, Sequence
from fnmatch import fnmatchcase
from functools import lru_cache
from pathlib import Path
from typing import TYPE_CHECKING, Any

from dag_healer.masking import mask_text

if TYPE_CHECKING:  # pragma: no cover - typing only
    from dag_healer.agent.context import IncidentContext
    from dag_healer.repo.profile import TestLayout

logger = logging.getLogger(__name__)

PROMPTS_DIR = Path(__file__).resolve().parent / "prompts"
TEST_PROMPT_FILE = "repair_test.md"
FIX_PROMPT_FILE = "repair_fix.md"

#: Default folder for the reproducing test when the repository has no test suite.
DEFAULT_TESTS_DIR = "tests"

#: Names of the tools the repair agent gets (see :class:`dag_healer.agent.repair.RepairTools`).
TOOL_NAMES: tuple[str, ...] = (
    "list_files",
    "read_file",
    "search",
    "write_file",
    "replace_in_file",
    "run_tests",
)

#: Stable system prompt shared by both phases (cache friendly: it never changes per incident).
SYSTEM_PROMPT = """\
You are dag-healer's repair agent. dag-healer watches an existing Apache Airflow deployment; \
when a task fails because of a bug in the repository's code, you reproduce the bug with a \
pytest test and then fix it with the smallest possible change, in a disposable clone of the \
repository. A human reviews your work as a pull request, so be precise, conservative and honest.

Ground rules:
- Work only through the tools you are given; every path is relative to the repository root.
- The evidence you receive is masked: personal data was replaced by placeholders such as \
"h:1a2b3c4d5e", "***", "[email]", "[phone]" or "[number]". Never try to recover the original \
values and never read raw data files.
- Do exactly what the current phase asks and nothing more: no refactoring, no reformatting, \
no new dependencies, no changes outside the files the phase allows.
- If something cannot be done within the rules, stop and say why in your final message \
instead of working around the rules.
- End with a short plain-text summary of what you did and the last test result."""

_MAX_LOG_LINES = 80
_MAX_TRACEBACK_CHARS = 12_000
_MAX_PYTEST_CHARS = 6_000
_MAX_FEEDBACK_CHARS = 5_000
_MAX_SAMPLE_ROWS = 20
_MAX_STEM_CHARS = 60
_PLACEHOLDER = re.compile(r"\{\{\s*([A-Za-z_][A-Za-z0-9_]*)\s*\}\}")
_CAMEL = re.compile(r"(?<=[a-z0-9])(?=[A-Z])")
_NON_IDENT = re.compile(r"[^0-9A-Za-z_]+")
_TEST_NAME_PATTERNS = ("test_*.py", "*_test.py")


# --------------------------------------------------------------------------- templates


@lru_cache(maxsize=8)
def load_prompt(name: str) -> str:
    """Return the text of ``agent/prompts/<name>`` (UTF-8)."""
    return (PROMPTS_DIR / name).read_text(encoding="utf-8")


def render_template(template: str, values: Mapping[str, Any]) -> str:
    """Replace ``{{ name }}`` placeholders with ``values[name]`` (unknown names stay as they are).

    Substituted values are inserted verbatim and never re-scanned, so code containing braces
    or ``{{`` is safe to embed.
    """

    def _replace(match: re.Match[str]) -> str:
        key = match.group(1)
        if key not in values:
            return match.group(0)
        value = values[key]
        return "" if value is None else str(value)

    return _PLACEHOLDER.sub(_replace, template)


# --------------------------------------------------------------------------- helpers


def _get(ctx: Any, name: str, default: Any = None) -> Any:
    """Read ``ctx.<name>`` tolerating missing attributes (and dict-shaped contexts)."""
    if isinstance(ctx, Mapping):
        value = ctx.get(name, default)
    else:
        value = getattr(ctx, name, default)
    return default if value is None else value


def _as_dict(value: Any) -> dict[str, Any]:
    """Turn a pydantic model, dataclass-like object or mapping into a plain dict."""
    if value is None:
        return {}
    if isinstance(value, Mapping):
        return dict(value)
    dump = getattr(value, "model_dump", None)
    if callable(dump):
        try:
            return dict(dump(mode="json"))
        except TypeError:  # pragma: no cover - non-pydantic model_dump
            return dict(dump())
    to_dict = getattr(value, "to_dict", None)
    if callable(to_dict):
        return dict(to_dict())
    return {key: val for key, val in vars(value).items() if not key.startswith("_")}


def _clean_rel(path: str) -> str:
    """Normalise a repo-relative path to posix form without leading ``./``."""
    cleaned = str(path).replace("\\", "/").strip()
    while cleaned.startswith("./"):
        cleaned = cleaned[2:]
    cleaned = cleaned.rstrip("/")
    return posixpath.normpath(cleaned) if cleaned else "."


def _identifier(text: str | None) -> str:
    """Snake-case identifier for file names (``buildReport`` -> ``build_report``)."""
    if not text:
        return ""
    snake = _CAMEL.sub("_", str(text))
    snake = _NON_IDENT.sub("_", snake).strip("_").lower()
    snake = re.sub(r"_+", "_", snake)
    if snake and snake[0].isdigit():
        snake = f"x_{snake}"
    return snake[:_MAX_STEM_CHARS].rstrip("_")


def _module_stem(file: str | None) -> str:
    """``src/etl/customers.py`` -> ``customers`` (``pkg/__init__.py`` -> ``pkg``)."""
    if not file:
        return ""
    path = Path(_clean_rel(file))
    stem = path.stem
    if stem == "__init__":
        stem = path.parent.name
    return _identifier(stem)


def _is_test_name(name: str) -> bool:
    return name == "conftest.py" or any(fnmatchcase(name, pattern) for pattern in _TEST_NAME_PATTERNS)


def _fence(text: str, lang: str = "text") -> str:
    """Wrap ``text`` in a fenced block whose fence is longer than any backtick run inside."""
    longest = max((len(run) for run in re.findall(r"`+", text)), default=0)
    fence = "`" * max(3, longest + 1)
    return f"{fence}{lang}\n{text.rstrip()}\n{fence}"


def _tail(text: str, limit: int) -> str:
    if len(text) <= limit:
        return text
    return "[... output truncated ...]\n" + text[-limit:]


def _json(value: Any) -> str:
    return json.dumps(value, ensure_ascii=False, default=str)


# --------------------------------------------------------------------------- target


def repro_target(ctx: IncidentContext | Any) -> dict[str, Any]:
    """Best guess of the code unit to reproduce: ``{file, module, symbol, line}``.

    Uses the innermost traceback frame inside the repository (frames of test files and
    synthetic functions such as ``<lambda>`` are skipped), then the task's callable as found
    by the DAG scanner, then the DAG file and task id. Any key may be ``None``.
    """
    frames = [f for f in (_get(ctx, "repo_frames", []) or []) if isinstance(f, Mapping) and f.get("file")]
    usable = [f for f in frames if not _is_test_name(Path(str(f["file"])).name)]
    for frame in reversed(usable):
        function = str(frame.get("function") or "")
        if function and not function.startswith("<"):
            file = _clean_rel(str(frame["file"]))
            return {
                "file": file,
                "module": _module_stem(file),
                "symbol": function,
                "line": frame.get("line"),
            }
    task = _get(ctx, "task")
    callable_file = _get(task, "callable_file") if task is not None else None
    callable_name = _get(task, "callable_name") if task is not None else None
    if callable_name or callable_file:
        file = _clean_rel(callable_file) if callable_file else (usable[-1]["file"] if usable else None)
        module_name = str(_get(task, "callable_module", "")).split(".")[-1]
        return {
            "file": file,
            "module": _module_stem(file) or _identifier(module_name),
            "symbol": callable_name,
            "line": _get(task, "lineno") or None,
        }
    if usable:
        file = _clean_rel(str(usable[-1]["file"]))
        return {"file": file, "module": _module_stem(file), "symbol": None, "line": usable[-1].get("line")}
    dag_file = _get(ctx, "dag_file")
    return {
        "file": _clean_rel(dag_file) if dag_file else None,
        "module": _identifier(_get(ctx, "dag_id", "")) or None,
        "symbol": _get(ctx, "task_id") or None,
        "line": None,
    }


def import_hint(ctx: IncidentContext | Any, layout: TestLayout | None = None) -> str:
    """Suggest how the new test should import the code under test.

    The dotted module mirrors the file path (``src/etl/customers.py`` ->
    ``from src.etl.customers import parse_signup_date``) unless the DAG scanner saw the task
    callable imported under a shorter name (``etl.customers`` for a ``src`` layout). The
    style of the existing tests, when known, is shown as an example.
    """
    target = repro_target(ctx)
    file = target.get("file")
    symbol = target.get("symbol")
    hints: list[str] = []
    if file and str(file).endswith(".py"):
        dotted = str(file)[:-3].replace("/", ".")
        if dotted.endswith(".__init__"):
            dotted = dotted[: -len(".__init__")]
        task = _get(ctx, "task")
        callable_module = str(_get(task, "callable_module", "") or "") if task is not None else ""
        if callable_module and dotted.endswith("." + callable_module):
            dotted = callable_module
        if symbol and symbol.isidentifier():
            hints.append(f"`from {dotted} import {symbol}`")
        else:
            hints.append(f"`import {dotted}`")
        hints.append(
            "(tests run from the repository root with `python -m pytest`, so the repository root is "
            "importable)"
        )
    style = getattr(layout, "import_style", None) if layout is not None else None
    if style:
        hints.append(f"; the existing tests import repository code like `{style}`")
    if not hints:
        return "import the module that contains the failing code from the repository root"
    return " ".join(hints)


def describe_test_style(layout: TestLayout | None, tests_dir: str, created_tests_dir: bool) -> str:
    """One paragraph describing the conventions the new test must follow."""
    if layout is None or not getattr(layout, "exists", False) or created_tests_dir:
        return (
            "the repository has no test suite yet, so use plain pytest: a module-level "
            "`def test_...():` function with bare `assert` statements, no classes and no conftest.py"
        )
    parts: list[str] = []
    framework = getattr(layout, "framework", None) or "pytest"
    if framework == "unittest":
        parts.append("the existing tests are `unittest.TestCase` classes; write one in the same style")
    elif getattr(layout, "uses_classes", False):
        parts.append("the existing tests group test functions in plain `class Test...:` classes; do the same")
    else:
        parts.append("the existing tests are module-level pytest functions with bare `assert`; do the same")
    parts.append(f"test files live in `{tests_dir}/` and are named like `{layout.file_pattern}`")
    if getattr(layout, "has_conftest", False):
        parts.append("a conftest.py exists: you may use its fixtures but do not modify it")
    return "; ".join(parts)


# --------------------------------------------------------------------------- location


def choose_test_location(
    layout: TestLayout, ctx: IncidentContext | Any, workspace: str | Path
) -> tuple[str, str, bool]:
    """Decide where the reproducing test goes: ``(tests_dir, test_file, created_tests_dir)``.

    The repository's own tests directory and file-naming style win (``test_*.py`` or
    ``*_test.py``); a repository without tests gets ``tests/`` and
    ``tests/test_<module>_<symbol>.py``. ``created_tests_dir`` is True when that folder does
    not exist in ``workspace`` yet. An existing file is never reused: a ``_regression``
    suffix (then a counter) keeps the name unique. Paths are repo-relative posix strings.
    """
    root = Path(workspace)
    if getattr(layout, "tests_dir", None):
        tests_dir = _clean_rel(str(layout.tests_dir))
    else:
        tests_dir = DEFAULT_TESTS_DIR
    created = tests_dir != "." and not (root / tests_dir).is_dir()

    target = repro_target(ctx)
    module = target.get("module") or ""
    symbol = _identifier(target.get("symbol"))
    pieces = [piece for piece in (module, symbol) if piece]
    if len(pieces) == 2 and pieces[0] == pieces[1]:
        pieces = pieces[:1]
    stem = "_".join(pieces)[:_MAX_STEM_CHARS].rstrip("_")
    if not stem:
        stem = _identifier(f"{_get(ctx, 'dag_id', '')}_{_get(ctx, 'task_id', '')}") or "healer_regression"

    pattern = str(getattr(layout, "file_pattern", None) or "test_*.py")
    if pattern.count("*") != 1 or not pattern.endswith(".py") or any(ch in pattern for ch in "?[]/"):
        pattern = "test_*.py"

    def _candidate(suffix: str) -> str:
        name = pattern.replace("*", stem + suffix)
        return name if tests_dir == "." else f"{tests_dir}/{name}"

    test_file = _candidate("")
    counter = 1
    while (root / test_file).exists():
        test_file = _candidate("_regression" if counter == 1 else f"_regression_{counter}")
        counter += 1
    return tests_dir, test_file, created


# --------------------------------------------------------------------------- evidence


def _contract_summary(ctx: Any) -> str:
    contract = _get(ctx, "contract")
    status = _get(ctx, "contract_status", "missing")
    if contract is None:
        if status == "missing":
            return "Contract status: missing (the DAG has no data contract)."
        return f"Contract status: {status} (contract details not included)."
    data = _as_dict(contract)
    lines = [f"Contract status: {status}."]
    for dataset in data.get("datasets", []) or []:
        dataset = _as_dict(dataset)
        locator = _as_dict(dataset.get("locator"))
        lines.append(
            f"- dataset `{dataset.get('name')}` (task `{dataset.get('task_id')}`, "
            f"{locator.get('type', '?')} `{locator.get('path', '?')}`)"
        )
        for column in dataset.get("columns", []) or []:
            column = _as_dict(column)
            rule = {
                key: column.get(key)
                for key in ("name", "dtype", "nullable", "min", "max", "allowed_values", "pii", "note")
                if column.get(key) not in (None, [], "")
            }
            lines.append(f"  - {_json(rule)}")
    return "\n".join(lines)


def _violations_summary(violations: Sequence[Any], title: str) -> str:
    if not violations:
        return ""
    lines = [title]
    for violation in violations:
        data = _as_dict(violation)
        column = f" column `{data.get('column')}`" if data.get("column") else ""
        lines.append(
            f"- {data.get('rule')} in `{data.get('dataset')}`{column}: {mask_text(data.get('message', ''))}"
        )
    return "\n".join(lines)


def _sample_block(ctx: Any) -> str:
    sample = _get(ctx, "masked_sample")
    if not sample:
        return ""
    sample = _as_dict(sample)
    rows = list(sample.get("rows") or [])[:_MAX_SAMPLE_ROWS]
    columns = sample.get("columns") or (list(rows[0].keys()) if rows and isinstance(rows[0], Mapping) else [])
    body = "\n".join(_json(row) for row in rows) or "(no rows)"
    return (
        f"### Masked sample of dataset `{sample.get('dataset', _get(ctx, 'sample_dataset', '?'))}` "
        f"(columns: {', '.join(map(str, columns))})\n"
        "Already masked; placeholders stand for personal data. One JSON object per row:\n"
        + _fence(body, "json")
    )


def _code_block(ctx: Any) -> str:
    snippets = _get(ctx, "code_snippets", {}) or {}
    if not snippets:
        dag_source = _get(ctx, "dag_source")
        dag_file = _get(ctx, "dag_file") or "DAG file"
        snippets = {dag_file: dag_source} if dag_source else {}
    blocks = []
    for path, source in snippets.items():
        if not source:
            continue
        lang = "python" if str(path).endswith(".py") else "text"
        blocks.append(f"#### `{path}`\n{_fence(str(source), lang)}")
    if not blocks:
        return ""
    return "### Source code from the repository\n" + "\n\n".join(blocks)


def render_evidence(ctx: IncidentContext | Any, *, incident: Mapping[str, Any] | None = None) -> str:
    """Render the masked evidence of an incident as Markdown for the repair prompts."""
    incident = incident or {}
    target = repro_target(ctx)
    dag_id = _get(ctx, "dag_id", incident.get("dag_id", "?"))
    task_id = _get(ctx, "task_id", incident.get("task_id", "?"))
    lines = [
        f"- DAG / task: `{dag_id}` / `{task_id}` (run `{_get(ctx, 'run_id', incident.get('run_id', '?'))}`, "
        f"try {_get(ctx, 'try_number', incident.get('try_number', '?'))}, "
        f"map index {_get(ctx, 'map_index', incident.get('map_index', -1))})",
    ]
    exc_type = _get(ctx, "exception_type")
    exc_message = _get(ctx, "exception_message")
    if exc_type or exc_message:
        lines.append(f"- Exception: `{exc_type or '?'}`: {mask_text(exc_message or '')}".rstrip())
    elif _get(ctx, "error_text"):
        lines.append(f"- Error: {mask_text(_get(ctx, 'error_text'))[:2000]}")
    if target.get("file"):
        where = f"`{target['file']}`"
        if target.get("symbol"):
            where += f", `{target['symbol']}`"
        if target.get("line"):
            where += f" (line {target['line']})"
        lines.append(f"- Most likely location: {where}")
    task = _get(ctx, "task")
    if task is not None and (_get(task, "callable_name") or _get(task, "operator")):
        lines.append(
            f"- Task definition: operator `{_get(task, 'operator', '?')}`, callable "
            f"`{_get(task, 'callable_module', '?')}.{_get(task, 'callable_name', '?')}` in "
            f"`{_get(task, 'callable_file', _get(ctx, 'dag_file', '?'))}`"
        )
    if _get(ctx, "dag_file"):
        lines.append(f"- DAG file: `{_get(ctx, 'dag_file')}`")
    diagnosis = incident.get("diagnosis")
    if diagnosis:
        lines.append(f"- Classifier diagnosis: {mask_text(diagnosis)}")
    evidence = incident.get("evidence") or []
    if isinstance(evidence, (list, tuple)) and evidence:
        lines.append("- Classifier evidence:")
        lines.extend(f"  - {mask_text(item)}" for item in evidence[:20])

    sections = ["\n".join(lines)]
    traceback_text = _get(ctx, "traceback_text")
    if traceback_text:
        sections.append(
            "### Traceback (masked)\n" + _fence(_tail(mask_text(traceback_text), _MAX_TRACEBACK_CHARS))
        )
    frames = _get(ctx, "repo_frames", []) or []
    if frames:
        frame_lines = [
            f"- `{f.get('file')}` line {f.get('line')} in `{f.get('function')}`"
            for f in frames
            if isinstance(f, Mapping)
        ]
        sections.append("### Repository frames (outermost first)\n" + "\n".join(frame_lines))
    sections.append("### Data contract\n" + _contract_summary(ctx))
    for title, key in (
        ("### Contract violations on the current data", "violations"),
        ("### Draft-contract violations (evidence only, not approved)", "draft_violations"),
    ):
        block = _violations_summary(_get(ctx, key, []) or [], title)
        if block:
            sections.append(block)
    if _get(ctx, "datasets_checked"):
        sections.append(
            "Datasets checked against the contract: " + ", ".join(map(str, _get(ctx, "datasets_checked")))
        )
    sample = _sample_block(ctx)
    if sample:
        sections.append(sample)
    log_excerpt = _get(ctx, "log_excerpt")
    if log_excerpt:
        log_lines = mask_text(log_excerpt).splitlines()[-_MAX_LOG_LINES:]
        sections.append(
            f"### Task log excerpt (last {len(log_lines)} lines, masked)\n" + _fence("\n".join(log_lines))
        )
    notes = _get(ctx, "notes", []) or []
    if notes:
        sections.append("### Notes from context gathering\n" + "\n".join(f"- {mask_text(n)}" for n in notes))
    code = _code_block(ctx)
    if code:
        sections.append(code)
    return "\n\n".join(sections)


def _previous_attempts_block(previous_attempts: Sequence[str]) -> str:
    if not previous_attempts:
        return ""
    parts = ["## Previous attempts (they were rolled back; do not repeat their mistakes)"]
    for index, text in enumerate(previous_attempts, start=1):
        parts.append(f"### Attempt {index}\n{_tail(mask_text(text), _MAX_FEEDBACK_CHARS)}")
    return "\n\n".join(parts)


def _format_paths(paths: Sequence[str]) -> str:
    cleaned = [_clean_rel(p) for p in paths if str(p).strip()]
    if not cleaned or "." in cleaned:
        return "any path of the repository (except the denied ones below)"
    return ", ".join(f"`{p}/`" for p in cleaned)


# --------------------------------------------------------------------------- prompts


def build_test_prompt(
    ctx: IncidentContext | Any,
    layout: TestLayout | None,
    test_file: str,
    *,
    tests_dir: str | None = None,
    created_tests_dir: bool | None = None,
    incident: Mapping[str, Any] | None = None,
    previous_attempts: Sequence[str] = (),
) -> str:
    """Render the phase "test" prompt (``prompts/repair_test.md``).

    Asks for ONE focused pytest test at ``test_file`` that reproduces the failure with
    hard-coded inputs taken from the masked evidence, following the repository's test style
    and touching only files under the tests directory. ``previous_attempts`` carries the
    reasons earlier attempts were rejected (for example "the test passed before the fix").
    """
    test_file = _clean_rel(test_file)
    if tests_dir is None:
        tests_dir = posixpath.dirname(test_file) or "."
    tests_dir = _clean_rel(tests_dir)
    if created_tests_dir is None:
        created_tests_dir = layout is None or not getattr(layout, "exists", False)
    if created_tests_dir:
        created_note = (
            f"The repository has no test suite. `{tests_dir}/` is created for this test; keep it minimal "
            "(the test file only: no `__init__.py`, conftest.py or configuration files)."
        )
    else:
        created_note = (
            f"The repository already has tests in `{tests_dir}/`; add a new file, do not edit the others."
        )
    values = {
        "test_file": test_file,
        "tests_dir": tests_dir,
        "test_style": describe_test_style(layout, tests_dir, bool(created_tests_dir)),
        "import_hint": import_hint(ctx, layout),
        "created_note": created_note,
        "tool_names": ", ".join(f"`{name}`" for name in TOOL_NAMES),
        "evidence": render_evidence(ctx, incident=incident),
        "previous_attempts": _previous_attempts_block(previous_attempts),
    }
    return render_template(load_prompt(TEST_PROMPT_FILE), values).rstrip() + "\n"


def build_fix_prompt(
    ctx: IncidentContext | Any,
    test_file: str,
    pytest_before: str,
    *,
    test_code: str | None = None,
    tests_dir: str | None = None,
    allowed_paths: Sequence[str] = ("src/", "dags/"),
    max_diff_files: int | None = None,
    max_diff_lines: int | None = None,
    incident: Mapping[str, Any] | None = None,
    previous_attempts: Sequence[str] = (),
) -> str:
    """Render the phase "fix" prompt (``prompts/repair_fix.md``).

    Asks for the minimal fix (smallest diff, same style and public API, no reformatting, no
    new dependencies) that makes ``test_file`` pass, then a run of the new test and of the
    existing suite. ``pytest_before`` is the (masked) output of the failing run.
    """
    test_file = _clean_rel(test_file)
    if tests_dir is None:
        tests_dir = posixpath.dirname(test_file) or "."
    limits = []
    if max_diff_files is not None:
        limits.append(f"at most {max_diff_files} changed files")
    if max_diff_lines is not None:
        limits.append(f"at most {max_diff_lines} changed lines")
    diff_limits = (" and ".join(limits) + " in total, the new test included") if limits else "keep it small"
    values = {
        "test_file": test_file,
        "tests_dir": _clean_rel(tests_dir),
        "allowed_paths": _format_paths(list(allowed_paths)),
        "diff_limits": diff_limits,
        "pytest_before": _fence(_tail(mask_text(pytest_before or "(no output)"), _MAX_PYTEST_CHARS)),
        "test_code": _fence(test_code, "python") if test_code else f'(read it with read_file("{test_file}"))',
        "tool_names": ", ".join(f"`{name}`" for name in TOOL_NAMES),
        "evidence": render_evidence(ctx, incident=incident),
        "previous_attempts": _previous_attempts_block(previous_attempts),
    }
    return render_template(load_prompt(FIX_PROMPT_FILE), values).rstrip() + "\n"


__all__ = [
    "DEFAULT_TESTS_DIR",
    "FIX_PROMPT_FILE",
    "PROMPTS_DIR",
    "SYSTEM_PROMPT",
    "TEST_PROMPT_FILE",
    "TOOL_NAMES",
    "build_fix_prompt",
    "build_test_prompt",
    "choose_test_location",
    "describe_test_style",
    "import_hint",
    "load_prompt",
    "render_evidence",
    "render_template",
    "repro_target",
]
