"""Render the pull-request description (``PR.md``) and write the PR artifacts.

The same Markdown is used as the body of a real GitHub pull request and, when no GitHub
token/repository is configured, as ``<healer_home>/prs/<incident_id>/PR.md`` next to
``fix.patch`` (served by the API and linked from the UI).

Free text that can carry data from the failing run (the error summary, the diagnosis,
pytest output and notes) goes through :func:`dag_healer.masking.mask_text` again;
code (the test and the diff) is reproduced verbatim. Large sections are truncated, each to
its own limit, and when the whole is still longer than :data:`MAX_BODY_CHARS` (GitHub refuses
a body over 65536 characters) the diff, the test, the pytest output, the diagnosis, the notes
and the error shrink further, in that order; "Before you merge" and "CI" are always complete.
The complete change is always in the branch and in ``fix.patch``.

The orchestrator's words (``vocabulary``: the ``Vocabulary.as_dict()`` shape of
:mod:`dag_healer.core.vocab`) name the pipeline, step and attempt. Missing words fall back to
the Airflow ones, so without a vocabulary the text is exactly the Airflow text.

Two sections depend on how the fix is published. "Before you merge" (right after the intro)
lists the problems the read-only GitHub setup checks found (:mod:`dag_healer.pr.github_checks`),
each as plain one-line text: what is wrong, cut if long, then what to change, whole. "CI" (after
the test results) is there when the fix's commit carries the ``[skip ci]`` marker
(``github.skip_ci``): CI has not run, and the section says how to start it after the review,
whether the branch is already on GitHub, only in the local repository, or nowhere (then it
says how to commit ``fix.patch`` with the marker). Neither section is masked (the branch name
must stay exact in the commands), and dag-healer's own text never spells a skip marker
literally, so a merge commit whose message copies this description does not skip CI because
of it.
"""

from __future__ import annotations

import re
import shlex
from collections.abc import Iterable, Mapping
from pathlib import Path
from typing import Any, Protocol

from dag_healer._version import __version__
from dag_healer.core.vocab import Vocabulary
from dag_healer.masking import mask_text

PR_MD_NAME = "PR.md"
PATCH_NAME = "fix.patch"

MAX_ERROR_CHARS = 2000
MAX_DIAGNOSIS_CHARS = 6000
MAX_TEST_CHARS = 15000
MAX_DIFF_CHARS = 30000
MAX_PYTEST_CHARS = 6000
#: The notes hold at most this many characters in all.
MAX_NOTES_CHARS = 10000
#: At most this many setup problems are listed under "Before you merge" (doctor shows them all).
MAX_SETUP_FINDINGS = 20
#: What a listed setup problem says is wrong is cut to this many characters (before the
#: Markdown escapes).
MAX_SETUP_TEXT_CHARS = 400
#: What to change is cut to this many only: it is dag-healer's own text, with at most the
#: repository's or the base branch's name in it, so it stays whole. With the escapes, 20 problems
#: then leave room enough below :data:`MAX_BODY_CHARS` for the other sections.
MAX_SETUP_FIX_CHARS = 600
#: The description stays below this many characters, counted as UTF-16 code units (never fewer
#: than GitHub counts): GitHub refuses a pull request whose body is longer than 65536.
MAX_BODY_CHARS = 65000

#: Message of the empty commit that starts CI after the review.
RUN_CI_MESSAGE = "Run CI after review"
#: Commit-message markers that make CI skip a push; never written literally in the description.
SKIP_MARKERS = ("[skip ci]", "[ci skip]", "[no ci]", "[skip actions]", "[actions skip]")

_SAFE_ID_RE = re.compile(r"^[A-Za-z0-9_.-]+$")
_FENCE_RE = re.compile(r"`{3,}")
_MARKDOWN_SPECIAL_RE = re.compile(r"([\\`*_\[\]<>])")
_AIRFLOW_WORDS = Vocabulary().as_dict()
#: The limit of each section that can shrink.
_LIMITS = {
    "error": MAX_ERROR_CHARS,
    "diagnosis": MAX_DIAGNOSIS_CHARS,
    "test": MAX_TEST_CHARS,
    "diff": MAX_DIFF_CHARS,
    "pytest": MAX_PYTEST_CHARS,
    "notes": MAX_NOTES_CHARS,
}
#: When the description is too long, these sections shrink, in this order, at most to these sizes.
_SHRINK_ORDER = (
    ("diff", 2000),
    ("test", 2000),
    ("pytest", 1000),
    ("diagnosis", 1000),
    ("notes", 1000),
    ("error", 500),
)
#: Room left for the notes that a cut section adds.
_SHRINK_MARGIN = 300


class SetupProblem(Protocol):
    """A problem in how the repository and its GitHub token are set up.

    :class:`dag_healer.pr.github_checks.SetupFinding` is one: ``level`` is ``"error"`` or
    ``"warn"``, ``message`` says what is wrong and ``fix`` what to change.
    """

    @property
    def level(self) -> str: ...

    @property
    def message(self) -> str: ...

    @property
    def fix(self) -> str: ...


# --------------------------------------------------------------------------- vocabulary


def _words(vocabulary: Mapping[str, str] | None) -> dict[str, str]:
    """``vocabulary`` over the Airflow words (missing or blank words keep the Airflow ones)."""
    words = dict(_AIRFLOW_WORDS)
    for name, value in (vocabulary or {}).items():
        if name in words and isinstance(value, str) and value.strip():
            words[name] = value
    return words


def _upper_first(word: str) -> str:
    """``task`` -> ``Task``, ``map index`` -> ``Map index``; ``DAG`` stays ``DAG``."""
    return word[:1].upper() + word[1:]


def review_checklist(vocabulary: Mapping[str, str] | None = None) -> tuple[str, ...]:
    """The review checklist of PR.md in the orchestrator's words."""
    words = _words(vocabulary)
    return (
        "The failing test reproduces the reported failure and would catch a regression.",
        "The fix addresses the root cause and is limited to what the test needs.",
        "No unrelated files, secrets or data files are changed.",
        "The full test suite passes in CI.",
        f'After merging, retry the failed {words["step"]} (Healer tab, "{words["retry_action"]}") '
        f"to confirm the pipeline recovers. Retrying also runs the {words['steps']} after it that "
        "failed or were blocked by it.",
    )


REVIEW_CHECKLIST = review_checklist()


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


def _plain(text: Any, limit: int) -> str:
    """``text`` as plain Markdown on one line: whitespace collapsed, cut to ``limit``, escaped.

    The escapes keep the text from opening a link, an image, code, emphasis or HTML, and from
    spelling a CI skip marker such as ``[skip ci]`` literally.
    """
    flat = " ".join(_text(text).split())
    if len(flat) > limit:
        flat = flat[: limit - 3] + "..."
    return _MARKDOWN_SPECIAL_RE.sub(r"\\\1", flat)


def _shell_word(value: str) -> str:
    """``value`` quoted for a POSIX shell (a plain branch name such as ``healer/x-1`` stays as is)."""
    return shlex.quote(value)


def _github_length(text: str) -> int:
    """The length of ``text`` in UTF-16 code units: never less than GitHub's count of characters."""
    return len(text.encode("utf-16-le", errors="surrogatepass")) // 2


def _notes(notes: list[str], limit: int) -> list[str]:
    """The notes as list items, holding at most ``limit`` characters of notes in all."""
    items: list[str] = []
    used = 0
    for index, note in enumerate(notes):
        room = limit - used
        if room <= 0:
            items.append(f"- ... [{len(notes) - index} more note(s) truncated]")
            break
        shown = note if len(note) <= room else note[:room] + " ... [note truncated]"
        items.append(f"- {shown}")
        used += len(shown)
    return items


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


def render_pr_title(incident: Mapping[str, Any], *, vocabulary: Mapping[str, str] | None = None) -> str:
    """Return a short pull-request title for an incident."""
    words = _words(vocabulary)
    dag_id = _text(incident.get("dag_id")) or words["pipeline"].lower()
    task_id = _text(incident.get("task_id")) or words["step"]
    return f"fix({dag_id}): repair failing {words['step']} {task_id} [dag-healer]"


# --------------------------------------------------------------------------- sections


def _before_you_merge(findings: Iterable[SetupProblem]) -> list[str]:
    """The "Before you merge" section, the problems in the given order (no lines without any).

    Each item says what is wrong (cut to :data:`MAX_SETUP_TEXT_CHARS`), then what to change,
    whole.
    """
    problems = list(findings)
    if not problems:
        return []
    out = [
        "## Before you merge",
        "",
        "dag-healer found problems in how this repository and its GitHub token are set up, or could not "
        "check them. They did not stop this pull request. Fix them before you merge; `dag-healer doctor` "
        "runs the same checks.",
        "",
    ]
    for problem in problems[:MAX_SETUP_FINDINGS]:
        label = "Error" if problem.level == "error" else "Warning"
        parts = (_plain(problem.message, MAX_SETUP_TEXT_CHARS), _plain(problem.fix, MAX_SETUP_FIX_CHARS))
        out.append(f"- **{label}:** {' '.join(part for part in parts if part)}")
    if len(problems) > MAX_SETUP_FINDINGS:
        more = len(problems) - MAX_SETUP_FINDINGS
        out.append(f"- ... and {more} more: run `dag-healer doctor` to see them all.")
    out.append("")
    return out


def ci_commands(branch: str | None) -> str:
    """The shell commands that start CI on ``branch`` after the review (the branch is shell-quoted).

    They push an empty commit made with ``git commit-tree`` on top of the fetched branch, so the
    branch is never checked out: no hook of the reviewer's runs with the branch's code in the
    working tree, and ``--no-verify`` skips the pre-push hook.
    """
    name = _shell_word(branch) if branch else "<branch>"
    return (
        f"git fetch origin {name}\n"
        f'commit=$(git commit-tree -p FETCH_HEAD -m "{RUN_CI_MESSAGE}" "FETCH_HEAD^{{tree}}")\n'
        f'git push --no-verify origin "$commit":refs/heads/{name}\n'
    )


#: What the "CI" section says once the branch is to be pushed by hand.
_WORKFLOWS_THE_PUSH_STARTS = (
    "The push and the pull request do start the workflows that the marker does not stop (`create` and "
    "`pull_request_target`): run `dag-healer doctor` with a GitHub token first, which reports those that "
    "can run this branch's code with the repository's secrets."
)


def _ci_section(branch: str | None, branch_on_github: bool, branch_in_local_repository: bool) -> list[str]:
    """The "CI" section: CI has not run, and how to start it once the change is reviewed.

    ``branch_on_github`` False means the branch is only in the local repository, so the
    section first says to push it (and to run ``dag-healer doctor`` before); with
    ``branch_in_local_repository`` False too, the change is only in ``fix.patch``, and the
    section says how to commit it, with the skip marker, before the push. The branch name is
    shell-quoted in the commands.
    """
    name = _shell_word(branch) if branch else "<branch>"
    push = _code(f"git push --no-verify origin {name}")
    out = [
        "## CI",
        "",
        "CI has not run for this change. Its commit carries \\[skip ci\\], so GitHub starts no workflow "
        "triggered by `push` or `pull_request` for it, and many other CI services skip it too: the "
        "model-written test and fix do not run in CI, with the repository's secrets, before a person has "
        "reviewed them.",
        "",
    ]
    if not branch_on_github and branch_in_local_repository:
        out.append(
            f"This branch is only in the local repository so far. Push it from there first ({push}; "
            "the `push` and `pull_request` workflows do not run for that push either), then open the pull "
            f"request with this description. {_WORKFLOWS_THE_PUSH_STARTS}"
        )
        out.append("")
    elif not branch_on_github:
        out.append(
            "This branch could not be pushed, not even into the local repository, so the change is only in "
            f"`{PATCH_NAME}`, next to this file. To propose it, apply `{PATCH_NAME}` on a new branch "
            f"{_code(branch or '<branch>')} made from the base branch, and commit it with `git commit "
            "--no-verify` (which skips your pre-commit and commit-msg hooks), with a message whose first "
            "paragraph after the title opens with the words skip ci in square brackets, as dag-healer's "
            f"commit did: CI then waits for the review there too. Push it ({push}; the `push` and "
            "`pull_request` workflows do not run for that push either), then open the pull request with this "
            f"description. {_WORKFLOWS_THE_PUSH_STARTS}"
        )
        out.append("")
    out.append(
        "After your review, start CI with an empty commit on the branch, from a clone of the repository "
        "(in a POSIX shell such as bash):"
    )
    out.append("")
    out.append(_fenced(ci_commands(branch), "sh"))
    out.append("")
    out.append(
        "The commit is made without checking the branch out, so none of your git hooks runs with this "
        "branch's code in your working tree: `git commit-tree` runs no hook, and `--no-verify` skips your "
        "pre-push hook. A base branch that requires the CI checks blocks the merge until they pass. When "
        "you merge, keep the skip marker out of the message of the commit that reaches the base branch (a "
        "squash merge copies the commit messages by default), or CI does not run there either."
    )
    out.append("")
    return out


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
    vocabulary: Mapping[str, str] | None = None,
    skip_ci: bool = False,
    branch_on_github: bool = False,
    setup_findings: Iterable[SetupProblem] = (),
    branch_in_local_repository: bool = True,
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
        vocabulary: the orchestrator's words (``Vocabulary.as_dict()``); ``None`` or ``{}``
            means the Airflow words.
        skip_ci: the fix's commit carries the ``[skip ci]`` marker (``github.skip_ci``): adds
            the "CI" section, which says how to start CI after the review.
        branch_on_github: the branch was pushed to GitHub; otherwise it is only in the local
            repository and the "CI" section says to push it first.
        setup_findings: problems of the GitHub setup (errors first), listed under "Before you
            merge" right after the intro; at most :data:`MAX_SETUP_FINDINGS` are shown.
        branch_in_local_repository: when the branch is not on GitHub, it was pushed into the
            local repository; otherwise it is nowhere (the change is only in ``fix.patch``) and
            the "CI" section says how to commit it with the skip marker first.

    Returns:
        Markdown with a summary, the root cause, the failing test, the fix, the test
        results before/after, notes, a review checklist and a "Generated by dag-healer"
        footer, plus the optional "Before you merge" and "CI" sections. With the defaults
        of ``skip_ci``, ``branch_on_github`` and ``setup_findings`` neither optional section is
        there. Sections shrink, the diff first, while the whole is longer than
        :data:`MAX_BODY_CHARS`.
    """
    words = _words(vocabulary)
    incident_id = _text(incident.get("id"))
    dag_id = _text(incident.get("dag_id"))
    task_id = _text(incident.get("task_id"))
    # The free text is masked once; every rendering below only applies the section limits.
    error = mask_text(_text(incident.get("error_summary"))).strip()
    cause = mask_text(_text(diagnosis)).strip()
    outputs = [
        (label, mask_text(_text(output)).strip("\n"))
        for label, output in (
            ("Before the fix (base branch + the new test, expected to fail)", pytest_before),
            ("After the fix (the new test and the existing suite)", pytest_after),
        )
    ]
    note_lines = [mask_text(_text(n)).strip() for n in notes or ()]
    note_lines = [n for n in note_lines if n]
    before_you_merge = _before_you_merge(setup_findings or ())
    ci = _ci_section(branch, branch_on_github, branch_in_local_repository) if skip_ci else []

    def render(limits: Mapping[str, int]) -> str:
        out: list[str] = []
        out.append(f"# dag-healer fix: {_code(task_id)} in {_code(dag_id)}")
        out.append("")
        out.append(
            f"> Automated fix proposed by **dag-healer** for incident {_code(incident_id)}. "
            "Nothing is merged automatically: review the change, the failing test and the test "
            "results below before merging."
        )
        out.append("")
        out.extend(before_you_merge)

        # -- summary ---------------------------------------------------------
        out.append("## Summary")
        out.append("")
        rows: list[tuple[str, str]] = [
            (_upper_first(words["pipeline"]), _code(dag_id)),
            (_upper_first(words["step"]), _code(task_id)),
            ("Run", _code(incident.get("run_id"))),
            (_upper_first(words["attempt"]), _text(incident.get("try_number")) or "-"),
        ]
        map_index = incident.get("map_index")
        if isinstance(map_index, int) and map_index >= 0:
            rows.append((_upper_first(words["map_index"]), str(map_index)))
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
        if error:
            out.append(f"Failure reported by {words['product']} (masked):")
            out.append("")
            out.append(_fenced(_head(error, limits["error"], "error"), "text"))
            out.append("")

        # -- root cause ------------------------------------------------------
        out.append("## Root cause")
        out.append("")
        out.append(
            _head(cause, limits["diagnosis"], "diagnosis") if cause else "_No diagnosis was recorded._"
        )
        out.append("")

        # -- failing test ----------------------------------------------------
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
            out.append(_fenced(_head(test_code, limits["test"], "test"), "python"))
            out.append("")

        # -- fix ---------------------------------------------------------------
        out.append("## The fix")
        out.append("")
        if diff:
            if len(diff) > limits["diff"]:
                out.append(
                    "The diff is truncated below; the complete change is on the branch and in `fix.patch`."
                )
                out.append("")
            out.append(_fenced(_head(diff, limits["diff"], "diff"), "diff"))
        else:
            out.append("_No diff was recorded._")
        out.append("")

        # -- test results -----------------------------------------------------
        out.append("## Test results")
        out.append("")
        for label, text in outputs:
            out.append(f"**{label}:**")
            out.append("")
            out.append(_fenced(_tail(text, limits["pytest"]), "text") if text.strip() else "_Not available._")
            out.append("")

        # -- CI -----------------------------------------------------------------
        out.extend(ci)

        # -- notes ------------------------------------------------------------
        if note_lines:
            out.append("## Notes")
            out.append("")
            out.extend(_notes(note_lines, limits["notes"]))
            out.append("")

        # -- checklist ------------------------------------------------------------
        out.append("## Review checklist")
        out.append("")
        out.extend(f"- [ ] {item}" for item in review_checklist(words))
        out.append("")
        out.append("---")
        out.append("")
        out.append(f"Generated by dag-healer {__version__} for incident {_code(incident_id)}.")
        return "\n".join(out) + "\n"

    limits = dict(_LIMITS)
    markdown = render(limits)
    for section, smallest in _SHRINK_ORDER:
        over = _github_length(markdown) - MAX_BODY_CHARS
        if over <= 0:
            break
        limits[section] = max(smallest, limits[section] - over - _SHRINK_MARGIN)
        markdown = render(limits)
    return markdown


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
    "MAX_BODY_CHARS",
    "MAX_NOTES_CHARS",
    "MAX_SETUP_FINDINGS",
    "MAX_SETUP_FIX_CHARS",
    "MAX_SETUP_TEXT_CHARS",
    "PATCH_NAME",
    "PR_MD_NAME",
    "REVIEW_CHECKLIST",
    "RUN_CI_MESSAGE",
    "SKIP_MARKERS",
    "SetupProblem",
    "ci_commands",
    "diff_summary",
    "pr_artifacts_dir",
    "render_pr_markdown",
    "render_pr_title",
    "review_checklist",
    "write_pr_artifacts",
]
