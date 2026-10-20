"""Render the pull-request description (``PR.md``) and write the PR artifacts.

The same Markdown is used as the body of a real GitHub pull request and, when no GitHub
token/repository is configured, as ``<healer_home>/prs/<incident_id>/PR.md`` next to
``fix.patch`` (served by the API and linked from the UI).

Free text that can carry data from the failing run (the error summary, the diagnosis,
pytest output and notes) goes through :func:`dag_healer.masking.mask_text` again;
code (the test and the diff) is reproduced verbatim. Large sections are truncated so the
body stays below GitHub's 65536-character limit; the complete change is always in the
branch and in ``fix.patch``.
"""

from __future__ import annotations

import re
from collections.abc import Iterable, Mapping
from pathlib import Path
from typing import Any

from dag_healer._version import __version__
from dag_healer.masking import mask_text

PR_MD_NAME = "PR.md"
PATCH_NAME = "fix.patch"

MAX_ERROR_CHARS = 2000
MAX_DIAGNOSIS_CHARS = 6000
MAX_TEST_CHARS = 15000
MAX_DIFF_CHARS = 30000
MAX_PYTEST_CHARS = 6000

_SAFE_ID_RE = re.compile(r"^[A-Za-z0-9_.-]+$")
_FENCE_RE = re.compile(r"`{3,}")

REVIEW_CHECKLIST = (
    "The failing test reproduces the reported failure and would catch a regression.",
    "The fix addresses the root cause and is limited to what the test needs.",
    "No unrelated files, secrets or data files are changed.",
    "The full test suite passes in CI.",
    'After merging, retry the failed task (Healer tab, "Retry task") to confirm the pipeline recovers.',
)


# --------------------------------------------------------------------------- helpers


def _text(value: Any) -> str:
    return "" if value is None else str(value)


def _head(text: str, limit: int, what: str) -> str:
    """Keep the beginning of ``text`` (diffs and code read top-down)."""
    if len(text) <= limit:
        return text
    cut = text.rfind("\n", 0, limit)
    cut = cut if cut > limit // 2 else limit
    return text[:cut].rstrip("\n") + f"\n... [{what} truncated: {len(text) - cut} more characters]\n"


def _tail(text: str, limit: int) -> str:
    """Keep the end of ``text`` (pytest prints its summary last)."""
    if len(text) <= limit:
        return text
    cut = text.find("\n", len(text) - limit)
    cut = cut + 1 if 0 <= cut < len(text) - limit // 2 else len(text) - limit
    return f"... [output truncated: {cut} earlier characters omitted]\n" + text[cut:]


def _fenced(text: str, lang: str = "") -> str:
    """Wrap ``text`` in a code fence longer than any backtick run it contains."""
    longest = max((len(m) for m in _FENCE_RE.findall(text)), default=0)
    fence = "`" * max(3, longest + 1)
    body = text if text.endswith("\n") else text + "\n"
    return f"{fence}{lang}\n{body}{fence}"


def _code(value: Any) -> str:
    """Inline code span that survives backticks in the value."""
    text = _text(value).replace("\n", " ")
    if "`" in text:
        return f"`` {text} ``"
    return f"`{text}`"


def _cell(value: str) -> str:
    return value.replace("|", "\\|")


def diff_summary(diff: str) -> tuple[int, int, int]:
    """Return ``(files, added_lines, deleted_lines)`` of a unified git diff."""
    files = added = deleted = 0
    for line in diff.splitlines():
        if line.startswith("diff --git "):
            files += 1
        elif line.startswith("+") and not line.startswith("+++ "):
            added += 1
        elif line.startswith("-") and not line.startswith("--- "):
            deleted += 1
    return files, added, deleted


def _tests_dir_of(test_path: str) -> str:
    parts = [p for p in test_path.replace("\\", "/").split("/") if p]
    return f"{parts[0]}/" if len(parts) > 1 else "tests/"


def _classification(incident: Mapping[str, Any]) -> str:
    category = _text(incident.get("category")).upper() or "CODE"
    confidence = incident.get("confidence")
    if isinstance(confidence, (int, float)):
        return f"{category} (confidence {float(confidence):.2f})"
    return category


def render_pr_title(incident: Mapping[str, Any]) -> str:
    """Return a short pull-request title for an incident."""
    dag_id = _text(incident.get("dag_id")) or "dag"
    task_id = _text(incident.get("task_id")) or "task"
    return f"fix({dag_id}): repair failing task {task_id} [dag-healer]"


# --------------------------------------------------------------------------- rendering


def render_pr_markdown(
    *,
    incident: Mapping[str, Any],
    diagnosis: str,
    test_path: str,
    test_code: str,
    diff: str,
    pytest_before: str,
    pytest_after: str,
    created_tests_dir: bool,
    notes: Iterable[str],
    branch: str | None,
) -> str:
    """Return the English pull-request description for a CODE fix.

    Args:
        incident: the incident (``Incident.to_dict()`` shape: ``id``, ``dag_id``,
            ``run_id``, ``task_id``, ``map_index``, ``try_number``, ``category``,
            ``confidence``, ``error_summary``).
        diagnosis: root-cause explanation.
        test_path: repo-relative path of the new failing test.
        test_code: source of that test.
        diff: unified diff of the complete change (test + fix).
        pytest_before: pytest output with the new test before the fix (it fails).
        pytest_after: pytest output after the fix (it passes, with the existing suite).
        created_tests_dir: the test folder did not exist and was created for this fix.
        notes: extra remarks (limits hit, assumptions, follow-ups).
        branch: name of the branch holding the fix, if any.

    Returns:
        Markdown with a summary, the root cause, the failing test, the fix, the test
        results before/after, notes, a review checklist and a "Generated by dag-healer"
        footer.
    """
    incident_id = _text(incident.get("id"))
    dag_id = _text(incident.get("dag_id"))
    task_id = _text(incident.get("task_id"))
    out: list[str] = []

    out.append(f"# dag-healer fix: {_code(task_id)} in {_code(dag_id)}")
    out.append("")
    out.append(
        f"> Automated fix proposed by **dag-healer** for incident {_code(incident_id)}. "
        "Nothing is merged automatically: review the change, the failing test and the test "
        "results below before merging."
    )
    out.append("")

    # -- summary -------------------------------------------------------------
    out.append("## Summary")
    out.append("")
    rows: list[tuple[str, str]] = [
        ("DAG", _code(dag_id)),
        ("Task", _code(task_id)),
        ("Run", _code(incident.get("run_id"))),
        ("Try", _text(incident.get("try_number")) or "-"),
    ]
    map_index = incident.get("map_index")
    if isinstance(map_index, int) and map_index >= 0:
        rows.append(("Map index", str(map_index)))
    rows.append(("Classification", _classification(incident)))
    rows.append(("Incident", _code(incident_id)))
    if branch:
        rows.append(("Branch", _code(branch)))
    files, added, deleted = diff_summary(diff or "")
    rows.append(("Change", f"{files} file(s), +{added} / -{deleted} lines"))
    out.append("| Field | Value |")
    out.append("|---|---|")
    out.extend(f"| {name} | {_cell(value)} |" for name, value in rows)
    out.append("")
    error = mask_text(_text(incident.get("error_summary"))).strip()
    if error:
        out.append("Failure reported by Airflow (masked):")
        out.append("")
        out.append(_fenced(_head(error, MAX_ERROR_CHARS, "error"), "text"))
        out.append("")

    # -- root cause ----------------------------------------------------------
    out.append("## Root cause")
    out.append("")
    cause = mask_text(_text(diagnosis)).strip()
    out.append(_head(cause, MAX_DIAGNOSIS_CHARS, "diagnosis") if cause else "_No diagnosis was recorded._")
    out.append("")

    # -- failing test --------------------------------------------------------
    out.append("## The failing test")
    out.append("")
    out.append(
        f"{_code(test_path)} reproduces the failure: it fails on the base branch and passes with the fix."
    )
    out.append("")
    if created_tests_dir:
        out.append(
            f"> **Note:** the {_tests_dir_of(test_path)} folder was created because the repository "
            "had no test suite. Consider running pytest in CI so this test keeps guarding the fix."
        )
        out.append("")
    if test_code:
        out.append(_fenced(_head(test_code, MAX_TEST_CHARS, "test"), "python"))
        out.append("")

    # -- fix -------------------------------------------------------------------
    out.append("## The fix")
    out.append("")
    if diff:
        if len(diff) > MAX_DIFF_CHARS:
            out.append(
                "The diff is truncated below; the complete change is on the branch and in `fix.patch`."
            )
            out.append("")
        out.append(_fenced(_head(diff, MAX_DIFF_CHARS, "diff"), "diff"))
    else:
        out.append("_No diff was recorded._")
    out.append("")

    # -- test results ---------------------------------------------------------
    out.append("## Test results")
    out.append("")
    for label, output in (
        ("Before the fix (base branch + the new test, expected to fail)", pytest_before),
        ("After the fix (the new test and the existing suite)", pytest_after),
    ):
        out.append(f"**{label}:**")
        out.append("")
        text = mask_text(_text(output)).strip("\n")
        out.append(_fenced(_tail(text, MAX_PYTEST_CHARS), "text") if text.strip() else "_Not available._")
        out.append("")

    # -- notes ----------------------------------------------------------------
    note_lines = [mask_text(_text(n)).strip() for n in notes or ()]
    note_lines = [n for n in note_lines if n]
    if note_lines:
        out.append("## Notes")
        out.append("")
        out.extend(f"- {n}" for n in note_lines)
        out.append("")

    # -- checklist ----------------------------------------------------------------
    out.append("## Review checklist")
    out.append("")
    out.extend(f"- [ ] {item}" for item in REVIEW_CHECKLIST)
    out.append("")
    out.append("---")
    out.append("")
    out.append(f"Generated by dag-healer {__version__} for incident {_code(incident_id)}.")
    return "\n".join(out) + "\n"


# --------------------------------------------------------------------------- artifacts


def pr_artifacts_dir(healer_home: str | Path, incident_id: str) -> Path:
    """Return ``<healer_home>/prs/<incident_id>`` (``incident_id`` must be a safe name)."""
    if not incident_id or not _SAFE_ID_RE.match(incident_id) or set(incident_id) == {"."}:
        raise ValueError(f"unsafe incident id for a file name: {incident_id!r}")
    return Path(healer_home) / "prs" / incident_id


def write_pr_artifacts(
    healer_home: str | Path,
    incident_id: str,
    *,
    pr_md: str,
    patch: str,
) -> tuple[Path, Path]:
    """Write ``PR.md`` and ``fix.patch`` under ``<healer_home>/prs/<incident_id>/``.

    Both files are written as UTF-8 bytes without newline translation (a patch of a CRLF
    file must keep its ``\\r``). Existing files are replaced.

    Returns:
        ``(pr_md_path, patch_path)``.

    Raises:
        ValueError: ``incident_id`` is not a safe file name.
    """
    directory = pr_artifacts_dir(healer_home, incident_id)
    directory.mkdir(parents=True, exist_ok=True)
    pr_md_path = directory / PR_MD_NAME
    patch_path = directory / PATCH_NAME
    pr_md_path.write_bytes(pr_md.encode("utf-8", errors="replace"))
    patch_path.write_bytes(patch.encode("utf-8", errors="replace"))
    return pr_md_path, patch_path


__all__ = [
    "PATCH_NAME",
    "PR_MD_NAME",
    "REVIEW_CHECKLIST",
    "diff_summary",
    "pr_artifacts_dir",
    "render_pr_markdown",
    "render_pr_title",
    "write_pr_artifacts",
]
