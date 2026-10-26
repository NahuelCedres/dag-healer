"""Tests for the GitHub pull-request client and the PR.md renderer (dag_healer.pr)."""

from __future__ import annotations

import json
import os
import re
import shutil
import subprocess
from dataclasses import dataclass
from pathlib import Path

import httpx
import pytest

from dag_healer._version import __version__
from dag_healer.core.vocab import Vocabulary
from dag_healer.orchestrators.fake import FAKE_VOCABULARY
from dag_healer.pr import create_pull_request as lazy_create_pull_request
from dag_healer.pr import review_checklist as lazy_review_checklist
from dag_healer.pr.github import GitHubError, create_pull_request
from dag_healer.pr.github_checks import check_workflows
from dag_healer.pr.prmd import (
    MAX_BODY_CHARS,
    MAX_DIFF_CHARS,
    MAX_SETUP_FINDINGS,
    REVIEW_CHECKLIST,
    SKIP_MARKERS,
    ci_commands,
    diff_summary,
    pr_artifacts_dir,
    render_pr_markdown,
    render_pr_title,
    review_checklist,
    write_pr_artifacts,
)
from dag_healer.repo.git import git_environment

TOKEN = "ghp_TestTokenValue000"
PR_URL = "https://github.com/acme/pipelines/pull/7"


def _client_kwargs(handler) -> dict:
    return {
        "repo": "acme/pipelines",
        "head": "healer/customers_sync-transform-1a2b3c4d",
        "base": "main",
        "title": "fix(customers_sync): repair failing task transform [dag-healer]",
        "body": "## Summary\n\nbody",
        "token": TOKEN,
        "transport": httpx.MockTransport(handler),
    }


# --------------------------------------------------------------------------- GitHub client


def test_create_pull_request_201() -> None:
    seen: list[httpx.Request] = []

    def handler(request: httpx.Request) -> httpx.Response:
        seen.append(request)
        return httpx.Response(201, json={"number": 7, "html_url": PR_URL})

    assert create_pull_request(**_client_kwargs(handler)) == PR_URL
    assert len(seen) == 1
    request = seen[0]
    assert request.method == "POST"
    assert str(request.url) == "https://api.github.com/repos/acme/pipelines/pulls"
    assert request.headers["Authorization"] == f"Bearer {TOKEN}"
    assert request.headers["Accept"] == "application/vnd.github+json"
    assert request.headers["X-GitHub-Api-Version"] == "2022-11-28"
    assert request.headers["User-Agent"].startswith("dag-healer/")
    payload = json.loads(request.content)
    assert payload["head"] == "healer/customers_sync-transform-1a2b3c4d"
    assert payload["base"] == "main"
    assert payload["title"].startswith("fix(customers_sync)")
    assert payload["body"] == "## Summary\n\nbody"


def test_existing_pull_request_is_returned() -> None:
    seen: list[httpx.Request] = []
    existing = "https://github.com/acme/pipelines/pull/3"

    def handler(request: httpx.Request) -> httpx.Response:
        seen.append(request)
        if request.method == "POST":
            return httpx.Response(
                422,
                json={
                    "message": "Validation Failed",
                    "errors": [
                        {
                            "resource": "PullRequest",
                            "code": "custom",
                            "message": "A pull request already exists for acme:healer/customers_sync-transform-1a2b3c4d.",  # noqa: E501
                        }
                    ],
                },
            )
        return httpx.Response(200, json=[{"number": 3, "html_url": existing}])

    assert create_pull_request(**_client_kwargs(handler)) == existing
    lookup = seen[1]
    assert lookup.method == "GET"
    assert lookup.url.path == "/repos/acme/pipelines/pulls"
    assert lookup.url.params["head"] == "acme:healer/customers_sync-transform-1a2b3c4d"
    assert lookup.url.params["state"] == "open"
    assert lookup.headers["Authorization"] == f"Bearer {TOKEN}"


def test_existing_pull_request_that_cannot_be_found() -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        if request.method == "POST":
            return httpx.Response(422, json={"errors": [{"message": "A pull request already exists for x"}]})
        return httpx.Response(200, json=[])

    with pytest.raises(GitHubError) as info:
        create_pull_request(**_client_kwargs(handler))
    assert info.value.status_code == 422


def test_authentication_error_does_not_leak_the_token() -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(401, json={"message": f"Bad credentials for {TOKEN}"})

    with pytest.raises(GitHubError) as info:
        create_pull_request(**_client_kwargs(handler))
    assert info.value.status_code == 401
    assert "authentication failed" in str(info.value)
    assert "Bad credentials" in str(info.value)
    assert TOKEN not in str(info.value)


@pytest.mark.parametrize(
    ("status", "body", "fragment"),
    [
        (403, {"message": "Resource not accessible by integration"}, "refused"),
        (404, {"message": "Not Found"}, "not found"),
        (422, {"message": "Validation Failed", "errors": [{"code": "invalid", "field": "base"}]}, "invalid"),
        (500, {"message": "Server Error"}, "GitHub API error (500)"),
    ],
)
def test_other_api_errors(status: int, body: dict, fragment: str) -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(status, json=body)

    with pytest.raises(GitHubError) as info:
        create_pull_request(**_client_kwargs(handler))
    assert info.value.status_code == status
    assert fragment in str(info.value)


@pytest.mark.parametrize(
    ("body", "detail"),
    [
        pytest.param(
            {"errors": [{"resource": "PullRequest", "field": "base"}]},
            '{"errors": [{"resource": "PullRequest", "field": "base"}]}',
            id="items-without-message",
        ),
        pytest.param(
            {"message": "Validation Failed", "errors": "base is invalid"},
            "Validation Failed; base is invalid",
            id="string",
        ),
        pytest.param({"message": "Validation Failed", "errors": 5}, "Validation Failed; 5", id="number"),
        pytest.param({"errors": {"code": "invalid"}}, "invalid", id="object"),
    ],
)
def test_error_answers_of_any_shape_are_github_errors(body: dict, detail: str) -> None:
    """An answer without the usual message fields still makes a GitHubError, with GitHub's text."""

    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(
            422, content=json.dumps(body).encode(), headers={"Content-Type": "application/json"}
        )

    with pytest.raises(GitHubError) as info:
        create_pull_request(**_client_kwargs(handler))
    assert info.value.status_code == 422
    assert str(info.value) == f"GitHub API error (422): {detail}"


def test_the_token_is_redacted_before_a_long_error_text_is_cut() -> None:
    """A body that echoes the token across the 500th character keeps no part of it."""

    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(500, text="x" * 480 + TOKEN + " echoed")

    with pytest.raises(GitHubError) as info:
        create_pull_request(**_client_kwargs(handler))
    message = str(info.value)
    assert message == "GitHub API error (500): " + "x" * 480 + "*** echoed"
    assert not [n for n in range(len(TOKEN) - 5) if TOKEN[n : n + 6] in message]


def test_network_error_is_wrapped() -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        raise httpx.ConnectError("connection refused", request=request)

    with pytest.raises(GitHubError, match="could not reach") as info:
        create_pull_request(**_client_kwargs(handler))
    assert info.value.status_code is None
    assert str(info.value) == "could not reach the GitHub API at https://api.github.com (ConnectError)"


@pytest.mark.parametrize("end", ["\n", "\r", "\r\n"], ids=["lf", "cr", "crlf"])
def test_a_transport_error_never_quotes_the_token(end: str) -> None:
    """h11 reports an invalid header value as its repr, which a plain replace() of the token misses."""
    token = TOKEN + end

    def handler(request: httpx.Request) -> httpx.Response:
        value = request.headers["Authorization"].encode()
        raise httpx.LocalProtocolError(f"Illegal header value {value!r}")

    with pytest.raises(GitHubError) as info:
        create_pull_request(**{**_client_kwargs(handler), "token": token})
    message = str(info.value)
    assert message == "could not reach the GitHub API at https://api.github.com (LocalProtocolError)"
    assert TOKEN not in message and "ghp_" not in message
    assert info.value.__cause__ is None and info.value.__suppress_context__


def test_invalid_arguments_make_no_request() -> None:
    def handler(request: httpx.Request) -> httpx.Response:  # pragma: no cover - must not run
        raise AssertionError("no request expected")

    kwargs = _client_kwargs(handler)
    with pytest.raises(GitHubError, match="owner/name"):
        create_pull_request(**{**kwargs, "repo": "not-a-repo"})
    with pytest.raises(GitHubError, match="token"):
        create_pull_request(**{**kwargs, "token": ""})


def test_enterprise_api_url_and_lazy_package_export() -> None:
    seen: list[str] = []

    def handler(request: httpx.Request) -> httpx.Response:
        seen.append(str(request.url))
        return httpx.Response(201, json={"html_url": "https://ghe.acme/data/etl/pull/1"})

    kwargs = {**_client_kwargs(handler), "repo": "data/etl", "api_url": "https://ghe.acme/api/v3/"}
    assert lazy_create_pull_request(**kwargs) == "https://ghe.acme/data/etl/pull/1"
    assert seen == ["https://ghe.acme/api/v3/repos/data/etl/pulls"]


# --------------------------------------------------------------------------- PR.md

INCIDENT = {
    "id": "0f3c2a9b8d7e4f6a1b2c3d4e5f60718",
    "dag_id": "customers_sync",
    "run_id": "manual__2026-10-17T10:00:00+00:00",
    "task_id": "transform",
    "map_index": -1,
    "try_number": 2,
    "category": "code",
    "confidence": 0.9,
    "error_summary": "ValueError: time data '2026-10-02T14:03:00Z' does not match format '%Y-%m-%d'",
}
TEST_CODE = (
    "from src.etl.customers import parse_signup_date\n"
    "\n"
    "\n"
    "def test_parse_signup_date_accepts_iso_timestamps():\n"
    "    assert parse_signup_date('2026-10-02T14:03:00Z').year == 2026\n"
)
DIFF = (
    "diff --git a/src/etl/customers.py b/src/etl/customers.py\n"
    "index 1111111..2222222 100644\n"
    "--- a/src/etl/customers.py\n"
    "+++ b/src/etl/customers.py\n"
    "@@ -1,2 +1,2 @@\n"
    " def parse_signup_date(value):\n"
    "-    return datetime.strptime(value, '%Y-%m-%d')\n"
    "+    return pd.to_datetime(value, utc=True)\n"
)
BEFORE = "F\nFAILED tests/test_customers.py::test_parse_signup_date_accepts_iso_timestamps\n1 failed in 0.20s"
AFTER = "..\n2 passed in 0.18s"

EXPECTED_PR_MD = f"""# dag-healer fix: `transform` in `customers_sync`

> Automated fix proposed by **dag-healer** for incident `0f3c2a9b8d7e4f6a1b2c3d4e5f60718`. Nothing is merged automatically: review the change, the failing test and the test results below before merging.

## Summary

| Field | Value |
|---|---|
| DAG | `customers_sync` |
| Task | `transform` |
| Run | `manual__2026-10-17T10:00:00+00:00` |
| Try | 2 |
| Classification | CODE (confidence 0.90) |
| Incident | `0f3c2a9b8d7e4f6a1b2c3d4e5f60718` |
| Branch | `healer/customers_sync-transform-0f3c2a9b` |
| Change | 1 file(s), +1 / -1 lines |

Failure reported by Airflow (masked):

```text
ValueError: time data '2026-10-02T14:03:00Z' does not match format '%Y-%m-%d'
```

## Root cause

`parse_signup_date` only accepts `%Y-%m-%d`, but the approved contract allows ISO-8601 timestamps.

## The failing test

`tests/test_customers.py` reproduces the failure: it fails on the base branch and passes with the fix.

> **Note:** the tests/ folder was created because the repository had no test suite. Consider running pytest in CI so this test keeps guarding the fix.

```python
{TEST_CODE}```

## The fix

```diff
{DIFF}```

## Test results

**Before the fix (base branch + the new test, expected to fail):**

```text
{BEFORE}
```

**After the fix (the new test and the existing suite):**

```text
{AFTER}
```

## Notes

- The repository had no tests; pytest was run on the new test only.

## Review checklist

- [ ] The failing test reproduces the reported failure and would catch a regression.
- [ ] The fix addresses the root cause and is limited to what the test needs.
- [ ] No unrelated files, secrets or data files are changed.
- [ ] The full test suite passes in CI.
- [ ] After merging, retry the failed task (Healer tab, "Retry task") to confirm the pipeline recovers. Retrying also runs the tasks after it that failed or were blocked by it.

---

Generated by dag-healer {__version__} for incident `0f3c2a9b8d7e4f6a1b2c3d4e5f60718`.
"""  # noqa: E501


def _render(**overrides) -> str:
    kwargs = {
        "incident": INCIDENT,
        "diagnosis": "`parse_signup_date` only accepts `%Y-%m-%d`, but the approved contract allows "
        "ISO-8601 timestamps.",
        "test_path": "tests/test_customers.py",
        "test_code": TEST_CODE,
        "diff": DIFF,
        "pytest_before": BEFORE,
        "pytest_after": AFTER,
        "created_tests_dir": True,
        "notes": ["The repository had no tests; pytest was run on the new test only."],
        "branch": "healer/customers_sync-transform-0f3c2a9b",
    }
    kwargs.update(overrides)
    return render_pr_markdown(**kwargs)


def test_pr_markdown_snapshot() -> None:
    assert _render() == EXPECTED_PR_MD


def test_pr_markdown_without_created_dir_notes_or_branch() -> None:
    md = _render(created_tests_dir=False, notes=[], branch=None, pytest_after="")
    assert "folder was created" not in md
    assert "## Notes" not in md
    assert "| Branch |" not in md
    assert "_Not available._" in md
    assert md.rstrip().endswith(f"Generated by dag-healer {__version__} for incident `{INCIDENT['id']}`.")


def test_pr_markdown_masks_free_text_but_not_code() -> None:
    incident = {**INCIDENT, "error_summary": "KeyError for jane.doe@example.com", "map_index": 3}
    md = _render(
        incident=incident,
        diagnosis="Row of customer jane.doe@example.com broke the parser.",
        pytest_before="AssertionError: 'bob@example.com' != 'x'\n1 failed in 0.1s",
        notes=["Contact +1 555 123 4567 for details"],
        test_code="EMAIL = 'fixture@example.com'\n",
    )
    assert "jane.doe@example.com" not in md
    assert "bob@example.com" not in md
    assert "555 123 4567" not in md
    assert "[email]" in md and "[phone]" in md
    assert "EMAIL = 'fixture@example.com'" in md  # code is reproduced verbatim
    assert "| Map index | 3 |" in md


def test_pr_markdown_uses_longer_fences_and_truncates() -> None:
    test_code = 'DOC = """\n```\nnot a fence end\n```\n"""\n'
    big_diff = DIFF + "".join(f"+line {i}\n" for i in range(MAX_DIFF_CHARS // 8))
    md = _render(test_code=test_code, diff=big_diff)
    assert "````python\n" + test_code + "````" in md
    assert "The diff is truncated below" in md
    assert "[diff truncated: " in md
    assert len(md) < 65536


def _github_length(text: str) -> int:
    return len(text.encode("utf-16-le")) // 2


def test_the_largest_description_stays_below_githubs_limit() -> None:
    """Every section at its own limit, the most setup problems and the CI section: still below 65536."""
    # the longest problems, every character of them escaped
    findings = [Finding("error", "[" * 1000, "<" * 1000) for _ in range(MAX_SETUP_FINDINGS + 5)]
    huge = "\U0001f600" * 3000  # characters outside the BMP count twice in UTF-16
    md = _render(
        incident={**INCIDENT, "error_summary": "E" * 5000 + huge},
        diagnosis="D" * 10000 + huge,
        test_code="t = 1\n" * 5000,
        diff=DIFF + "".join(f"+line {i} {huge[:10]}\n" for i in range(10000)),
        pytest_before="B\n" * 10000,
        pytest_after="A\n" * 10000,
        notes=["N" * 3000 for _ in range(10)],
        branch=BRANCH,
        skip_ci=True,
        setup_findings=findings,
        branch_in_local_repository=False,  # the longest CI section
    )
    assert _github_length(md) <= MAX_BODY_CHARS < 65536
    full = _render(branch=BRANCH, skip_ci=True, setup_findings=findings, branch_in_local_repository=False)
    assert _section(md, "Before you merge") == _section(full, "Before you merge")  # always complete
    assert _section(md, "CI") == _section(full, "CI")
    assert "The diff is truncated below" in md and "[test truncated: " in md
    assert md.rstrip().endswith(f"Generated by dag-healer {__version__} for incident `{INCIDENT['id']}`.")


def test_long_notes_are_cut() -> None:
    md = _render(notes=["first " * 1000, "second " * 1000, "third"])
    notes = _section(md, "Notes")
    assert "- first first" in notes and "[note truncated]" in notes
    assert "- ... [1 more note(s) truncated]" in notes and "third" not in notes


@pytest.mark.parametrize(
    "vocabulary",
    [None, {}, Vocabulary().as_dict(), {"unknown": "word", "step": "  ", "product": None}],
    ids=["none", "empty", "airflow", "unknown-and-blank"],
)
def test_pr_markdown_with_the_airflow_words_is_unchanged(vocabulary: dict | None) -> None:
    assert _render(vocabulary=vocabulary) == EXPECTED_PR_MD


def test_pr_markdown_in_the_words_of_another_orchestrator() -> None:
    md = _render(incident={**INCIDENT, "map_index": 3}, vocabulary=FAKE_VOCABULARY.as_dict())
    assert "| Flow | `customers_sync` |" in md
    assert "| Step | `transform` |" in md
    assert "| Run | `manual__2026-10-17T10:00:00+00:00` |" in md
    assert "| Attempt | 2 |" in md
    assert "| Map index | 3 |" in md
    assert "Failure reported by Fakeflow (masked):" in md
    assert '- [ ] After merging, retry the failed step (Healer tab, "Retry step") to confirm' in md
    assert "Retrying also runs the steps after it that failed or were blocked by it." in md
    assert "Airflow" not in md
    assert "| DAG |" not in md and "| Task |" not in md and "| Try |" not in md


def test_labels_keep_upper_case_words() -> None:
    md = _render(vocabulary={"pipeline": "DAG", "step": "state", "attempt": "redrive"})
    assert "| DAG | `customers_sync` |" in md  # never "Dag"
    assert "| State | `transform` |" in md and "| Redrive | 2 |" in md


def test_review_checklist_and_title_follow_the_vocabulary() -> None:
    fake = FAKE_VOCABULARY.as_dict()
    assert REVIEW_CHECKLIST == review_checklist() == review_checklist({}) == lazy_review_checklist(None)
    assert REVIEW_CHECKLIST[-1] == (
        'After merging, retry the failed task (Healer tab, "Retry task") to confirm the pipeline recovers. '
        "Retrying also runs the tasks after it that failed or were blocked by it."
    )
    assert review_checklist(fake)[:-1] == REVIEW_CHECKLIST[:-1]
    assert review_checklist(fake)[-1] == (
        'After merging, retry the failed step (Healer tab, "Retry step") to confirm the pipeline recovers. '
        "Retrying also runs the steps after it that failed or were blocked by it."
    )
    assert render_pr_title(INCIDENT, vocabulary={}) == render_pr_title(INCIDENT)
    assert render_pr_title(INCIDENT, vocabulary=fake) == (
        "fix(customers_sync): repair failing step transform [dag-healer]"
    )
    assert render_pr_title({}) == "fix(dag): repair failing task task [dag-healer]"
    assert render_pr_title({}, vocabulary=fake) == "fix(flow): repair failing step step [dag-healer]"


def test_pr_title_and_diff_summary() -> None:
    assert render_pr_title(INCIDENT) == "fix(customers_sync): repair failing task transform [dag-healer]"
    new_file = (
        "diff --git a/t.py b/t.py\nnew file mode 100644\n--- /dev/null\n+++ b/t.py\n@@ -0,0 +1 @@\n+x\n"
    )
    assert diff_summary(DIFF + new_file) == (2, 2, 1)
    assert diff_summary("") == (0, 0, 0)


def test_write_pr_artifacts(tmp_path: Path) -> None:
    patch = "diff --git a/x.py b/x.py\n--- a/x.py\n+++ b/x.py\n@@ -1 +1 @@\n-a = 1\r\n+a = 2\r\n"
    pr_md_path, patch_path = write_pr_artifacts(tmp_path, INCIDENT["id"], pr_md="# PR\n", patch=patch)
    assert pr_md_path == tmp_path / "prs" / INCIDENT["id"] / "PR.md"
    assert patch_path == tmp_path / "prs" / INCIDENT["id"] / "fix.patch"
    assert pr_md_path.read_bytes() == b"# PR\n"
    assert patch_path.read_bytes() == patch.encode("utf-8")  # CRLF inside the patch is kept
    # Rewriting replaces the previous artifacts.
    write_pr_artifacts(tmp_path, INCIDENT["id"], pr_md="# PR v2\n", patch="")
    assert pr_md_path.read_text(encoding="utf-8") == "# PR v2\n"
    assert pr_artifacts_dir(tmp_path, "abc") == tmp_path / "prs" / "abc"


@pytest.mark.parametrize("bad", ["", "..", ".", "../escape", "a/b", "a\\b"])
def test_write_pr_artifacts_rejects_unsafe_ids(tmp_path: Path, bad: str) -> None:
    with pytest.raises(ValueError):
        write_pr_artifacts(tmp_path, bad, pr_md="x", patch="y")


# --------------------------------------------------------------------------- CI and "Before you merge"


@dataclass(frozen=True)
class Finding:
    """A setup problem as the publish step passes it (the shape of github_checks.SetupFinding)."""

    level: str
    message: str
    fix: str


BRANCH = "healer/customers_sync-transform-00000000"
CI_INTRO = (
    "## CI\n"
    "\n"
    "CI has not run for this change. Its commit carries \\[skip ci\\], so GitHub starts no workflow "
    "triggered by `push` or `pull_request` for it, and many other CI services skip it too: the "
    "model-written test and fix do not run in CI, with the repository's secrets, before a person has "
    "reviewed them.\n"
    "\n"
)
LOCAL_BRANCH = (
    "This branch is only in the local repository so far. Push it from there first "
    f"(`git push --no-verify origin {BRANCH}`; the `push` and `pull_request` workflows do not run for that "
    "push either), then open the pull request with this description. The push and the pull request do "
    "start the workflows that the marker does not stop (`create` and `pull_request_target`): run "
    "`dag-healer doctor` with a GitHub token first, which reports those that can run this branch's code "
    "with the repository's secrets.\n"
    "\n"
)
CI_COMMANDS = (
    "After your review, start CI with an empty commit on the branch, from a clone of the repository (in a "
    "POSIX shell such as bash):\n"
    "\n"
    "```sh\n"
    f"git fetch origin {BRANCH}\n"
    'commit=$(git commit-tree -p FETCH_HEAD -m "Run CI after review" "FETCH_HEAD^{tree}")\n'
    f'git push --no-verify origin "$commit":refs/heads/{BRANCH}\n'
    "```\n"
    "\n"
    "The commit is made without checking the branch out, so none of your git hooks runs with this branch's "
    "code in your working tree: `git commit-tree` runs no hook, and `--no-verify` skips your pre-push hook. "
    "A base branch that requires the CI checks blocks the merge until they pass. When you merge, keep the "
    "skip marker out of the message of the commit that reaches the base branch (a squash merge copies the "
    "commit messages by default), or CI does not run there either.\n"
    "\n"
)
BEFORE_YOU_MERGE_INTRO = (
    "## Before you merge\n"
    "\n"
    "dag-healer found problems in how this repository and its GitHub token are set up, or could not check "
    "them. They did not stop this pull request. Fix them before you merge; `dag-healer doctor` runs the "
    "same checks.\n"
    "\n"
)


def _section(md: str, heading: str) -> str:
    """The text of the ``## heading`` section (up to the next ``## ``), heading included."""
    start = md.index(f"## {heading}\n")
    end = md.find("\n## ", start + 1)
    return md[start : end + 1 if end >= 0 else len(md)]


def _has_skip_marker(text: str) -> bool:
    return any(marker in text.lower() for marker in SKIP_MARKERS)


@pytest.mark.parametrize("on_github", [True, False], ids=["github", "local"])
def test_ci_section_after_the_test_results(on_github: bool) -> None:
    md = _render(branch=BRANCH, skip_ci=True, branch_on_github=on_github)
    expected = CI_INTRO + ("" if on_github else LOCAL_BRANCH) + CI_COMMANDS
    assert _section(md, "CI") == expected
    assert md.index("## Test results") < md.index("## CI\n") < md.index("## Notes")
    # the rest is today's text: removing the section gives the snapshot back (with this branch)
    assert md.replace(expected, "") == _render(branch=BRANCH)
    assert "## Before you merge" not in md
    assert "[number]" not in _section(md, "CI")  # the branch is never masked


def test_ci_section_without_a_branch_name() -> None:
    md = _render(branch=None, skip_ci=True)
    ci = _section(md, "CI")
    assert "(`git push --no-verify origin <branch>`;" in ci
    assert "git fetch origin <branch>\n" in ci
    assert 'git push --no-verify origin "$commit":refs/heads/<branch>\n' in ci


NOWHERE_BRANCH = (
    "This branch could not be pushed, not even into the local repository, so the change is only in "
    "`fix.patch`, next to this file. To propose it, apply `fix.patch` on a new branch "
    f"`{BRANCH}` made from the base branch, and commit it with `git commit --no-verify` (which skips your "
    "pre-commit and commit-msg hooks), with a message whose first paragraph after the title opens with the "
    "words skip ci in square brackets, as dag-healer's commit did: CI then waits for the review there too. "
    f"Push it (`git push --no-verify origin {BRANCH}`; the `push` and `pull_request` workflows do not run "
    "for that push either), then open the pull request with this description. The push and the pull "
    "request do start the workflows that the marker does not stop (`create` and `pull_request_target`): "
    "run `dag-healer doctor` with a GitHub token first, which reports those that can run this branch's "
    "code with the repository's secrets.\n"
    "\n"
)


def test_ci_section_when_the_branch_could_not_be_pushed_anywhere() -> None:
    """Only fix.patch holds the change: the section says how to commit it with the marker first."""
    md = _render(branch=BRANCH, skip_ci=True, branch_in_local_repository=False)
    assert _section(md, "CI") == CI_INTRO + NOWHERE_BRANCH + CI_COMMANDS
    assert not _has_skip_marker(md)
    # on GitHub, the local repository does not matter
    on_github = _render(branch=BRANCH, skip_ci=True, branch_on_github=True, branch_in_local_repository=False)
    assert _section(on_github, "CI") == CI_INTRO + CI_COMMANDS
    assert _render(skip_ci=False, branch_in_local_repository=False) == EXPECTED_PR_MD


@pytest.mark.parametrize(
    ("branch", "quoted"),
    [
        ("healer/x;rm -rf ~", "'healer/x;rm -rf ~'"),
        ("healer/it's", "'healer/it'\"'\"'s'"),
        ("healer/$(id)`id`", "'healer/$(id)`id`'"),
        ("release/2.0", "release/2.0"),
    ],
    ids=["semicolon", "quote", "substitution", "plain"],
)
def test_ci_commands_quote_the_branch(branch: str, quoted: str) -> None:
    ci = _section(_render(branch=branch, skip_ci=True), "CI")
    assert f"git fetch origin {quoted}\n" in ci
    assert f'git push --no-verify origin "$commit":refs/heads/{quoted}\n' in ci
    if "`" in quoted:  # a backtick in the name: the inline code still holds the whole command
        assert f"(`` git push --no-verify origin {quoted} ``;" in ci
    else:
        assert f"(`git push --no-verify origin {quoted}`;" in ci


def _reviewer_env(**extra: str) -> dict[str, str]:
    """A reviewer's git environment: plain git, never pointed at another repository.

    ``git_environment`` drops the inherited ``GIT_DIR``, ``GIT_WORK_TREE``, ``GIT_INDEX_FILE``
    and the like (set when pytest runs from a git hook); dag-healer's own object settings go too.
    """
    identity = {
        "GIT_AUTHOR_NAME": "Reviewer",
        "GIT_AUTHOR_EMAIL": "reviewer@example.invalid",
        "GIT_COMMITTER_NAME": "Reviewer",
        "GIT_COMMITTER_EMAIL": "reviewer@example.invalid",
    }
    return git_environment({**identity, "GIT_NO_REPLACE_OBJECTS": None, "GIT_GRAFT_FILE": None, **extra})


def _git(*args: str, cwd: Path) -> str:
    done = subprocess.run(
        ["git", *args], cwd=cwd, env=_reviewer_env(), capture_output=True, text=True, check=True
    )
    return done.stdout.strip()


@pytest.mark.skipif(os.name == "nt" or shutil.which("sh") is None, reason="the commands are for POSIX sh")
def test_the_ci_commands_push_an_empty_commit_without_a_checkout(tmp_path: Path) -> None:
    origin = tmp_path / "origin.git"
    _git("init", "--quiet", "--bare", "-b", "main", str(origin), cwd=tmp_path)
    work = tmp_path / "work"
    _git("clone", "--quiet", str(origin), str(work), cwd=tmp_path)
    _git("symbolic-ref", "HEAD", "refs/heads/main", cwd=work)  # whatever init.defaultBranch says
    (work / "app.py").write_text("x = 1\n", encoding="utf-8")
    _git("add", "app.py", cwd=work)
    _git("-c", "commit.gpgsign=false", "commit", "--quiet", "-m", "base", cwd=work)
    _git("push", "--quiet", "origin", "main", cwd=work)
    _git("switch", "--quiet", "-c", BRANCH, cwd=work)
    (work / "app.py").write_text("x = 2\n", encoding="utf-8")
    _git("-c", "commit.gpgsign=false", "commit", "--quiet", "-am", "fix", cwd=work)
    _git("push", "--quiet", "origin", BRANCH, cwd=work)
    fix = _git("rev-parse", "HEAD", cwd=work)
    reviewer = tmp_path / "reviewer"
    _git("clone", "--quiet", str(origin), str(reviewer), cwd=tmp_path)
    hooks = reviewer / ".git" / "hooks"
    for name in ("post-checkout", "pre-push", "pre-commit", "post-commit"):
        hook = hooks / name
        hook.write_text(f"#!/bin/sh\necho {name} >> '{tmp_path / 'ran'}'\n", encoding="utf-8")
        hook.chmod(0o755)
    env = _reviewer_env(
        # the reviewer's hooks are the clone's own, whatever the global configuration says
        GIT_CONFIG_COUNT="2",
        GIT_CONFIG_KEY_0="core.hooksPath",
        GIT_CONFIG_VALUE_0=str(hooks),
        GIT_CONFIG_KEY_1="commit.gpgsign",
        GIT_CONFIG_VALUE_1="false",
    )
    subprocess.run(["sh", "-ec", ci_commands(BRANCH)], cwd=reviewer, env=env, check=True, capture_output=True)
    tip = _git("rev-parse", f"refs/heads/{BRANCH}", cwd=origin)
    assert _git("rev-parse", f"{tip}^", cwd=origin) == fix  # one new commit on top of the fix
    tree = _git("rev-parse", f"{fix}^{{tree}}", cwd=origin)
    assert _git("rev-parse", f"{tip}^{{tree}}", cwd=origin) == tree  # empty: the same files
    assert _git("log", "-1", "--format=%s", tip, cwd=origin) == "Run CI after review"
    assert _git("symbolic-ref", "--short", "HEAD", cwd=reviewer) == "main"  # never checked out
    assert not (tmp_path / "ran").exists()  # no hook of the reviewer's ran


def test_no_ci_section_without_skip_ci() -> None:
    assert _render(skip_ci=False, branch_on_github=True) == EXPECTED_PR_MD
    assert _render(skip_ci=False) == EXPECTED_PR_MD


def test_before_you_merge_follows_the_intro_in_the_given_order() -> None:
    findings = [
        Finding("error", "The base branch main can be deleted.", "Restrict deletions."),
        Finding("warn", "The GitHub token is a classic personal access token.", "Use a fine-grained token."),
        Finding("error", "The workflow .github/workflows/x.yml runs on pull_request_target.", "Skip it."),
    ]
    md = _render(setup_findings=findings)
    intro_end = md.index("before merging.\n\n") + len("before merging.\n\n")
    assert md[intro_end:].startswith(
        BEFORE_YOU_MERGE_INTRO + "- **Error:** The base branch main can be deleted. Restrict deletions.\n"
        "- **Warning:** The GitHub token is a classic personal access token. Use a fine-grained token.\n"
        "- **Error:** The workflow .github/workflows/x.yml runs on pull\\_request\\_target. Skip it.\n"
        "\n"
        "## Summary\n"
    )
    assert md.replace(_section(md, "Before you merge"), "") == EXPECTED_PR_MD
    assert _render(setup_findings=[]) == EXPECTED_PR_MD == _render(setup_findings=iter(()))


def test_before_you_merge_shows_twenty_problems_at_most() -> None:
    findings = [Finding("warn", f"Problem {i}.", "Fix it.") for i in range(23)]
    section = _section(_render(setup_findings=findings), "Before you merge")
    items = [line for line in section.splitlines() if line.startswith("- ")]
    assert len(items) == 21
    assert items[0] == "- **Warning:** Problem 0. Fix it."
    assert items[19] == "- **Warning:** Problem 19. Fix it."
    assert items[20] == "- ... and 3 more: run `dag-healer doctor` to see them all."
    assert len(_section(_render(setup_findings=findings[:20]), "Before you merge").splitlines()) == 25


def test_setup_problems_are_plain_one_line_text() -> None:
    long_fix = "Check it by hand. " * 33  # 593 characters: shown whole
    findings = [
        Finding(
            "error", "A [link](https://example.invalid) and ![img](x) in <b>html</b>", "Use `code` *now*_."
        ),
        Finding("warn", "Two\nlines\r\n\n## Heading\n- item", "back\\slash"),
        Finding("warn", "Long.", long_fix),
        Finding("warn", "Wrong. " * 100, "Fix it."),
        Finding("warn", "Odd.", "Fix. " * 300),
    ]
    items = [line for line in _render(setup_findings=findings).splitlines() if line.startswith("- **")]
    assert items[0] == (
        "- **Error:** A \\[link\\](https://example.invalid) and !\\[img\\](x) in \\<b\\>html\\</b\\> "
        "Use \\`code\\` \\*now\\*\\_."
    )
    assert items[1] == "- **Warning:** Two lines ## Heading - item back\\\\slash"
    assert items[2] == "- **Warning:** Long. " + long_fix.strip()
    # what is wrong is cut to 400 characters, what to change follows whole
    wrong = ("Wrong. " * 100)[:397] + "..."
    assert items[3] == f"- **Warning:** {wrong} Fix it."
    # what to change is cut only past 600 characters
    assert items[4] == "- **Warning:** Odd. " + ("Fix. " * 300)[:597] + "..."


def test_each_workflow_problem_ends_with_its_whole_fix(tmp_path: Path) -> None:
    """The longest findings of the workflow scan, with skip_ci off, keep every word of what to change."""
    folder = tmp_path / ".github" / "workflows"
    folder.mkdir(parents=True)
    workflows = {
        "ci.yml": "on: [push, pull_request]\njobs:\n  t:\n    env: {K: '${{ secrets.K }}'}\n",
        "preview-deploy.yml": (
            "on:\n  pull_request_target:\n    types: [opened, synchronize, reopened, labeled, "
            "ready_for_review, review_requested, assigned, edited]\njobs:\n  p:\n    steps:\n"
            "      - run: git checkout ${{ github.event.pull_request.head.sha }}\n"
        ),
        f"release-{'x' * 48}.yml": "on: create\njobs:\n  r:\n    env: {K: '${{ secrets.K }}'}\n",
    }
    for name, text in workflows.items():
        (folder / name).write_text(text, encoding="utf-8", newline="\n")
    findings = check_workflows(tmp_path, skip_ci=False, base="main").findings
    assert len(findings) == 3 and all(len(f"{f.message} {f.fix}") > 400 for f in findings)
    section = _section(_render(setup_findings=findings), "Before you merge")
    items = [line for line in section.splitlines() if line.startswith("- **")]
    assert len(items) == len(findings)
    for item, finding in zip(items, findings, strict=True):
        shown = re.sub(r"\\(.)", r"\1", item)  # the Markdown escapes undone
        assert shown.endswith(" " + finding.fix)
    assert "branches-ignore under pull_request filters the base branch, not the healer branch" in "".join(
        re.sub(r"\\(.)", r"\1", item) for item in items
    )


@pytest.mark.parametrize("on_github", [True, False], ids=["github", "local"])
def test_no_literal_skip_marker_in_the_description(on_github: bool) -> None:
    findings = [
        Finding(
            "error",
            "The workflow .github/workflows/[skip ci].yml is [CI SKIP] and [no ci].",
            "[skip actions]",
        ),
        Finding("warn", "[actions skip]", "[Skip CI]"),
    ]
    md = _render(branch=BRANCH, skip_ci=True, branch_on_github=on_github, setup_findings=findings)
    assert "## Before you merge" in md and "## CI" in md
    assert not _has_skip_marker(md)
    assert "\\[skip ci\\]" in md  # what a reader sees as [skip ci], escaped


def test_new_sections_use_no_orchestrator_words() -> None:
    findings = [Finding("warn", "The GitHub token is a classic personal access token.", "Use another one.")]
    kwargs = {"branch": BRANCH, "skip_ci": True, "setup_findings": findings}
    fake = _render(incident={**INCIDENT, "map_index": 3}, vocabulary=FAKE_VOCABULARY.as_dict(), **kwargs)
    airflow = _render(**kwargs)
    assert "Airflow" not in fake
    for heading in ("Before you merge", "CI"):
        section = _section(fake, heading)
        assert section == _section(airflow, heading)  # the same text for every orchestrator
        assert "Fakeflow" not in section and "DAG" not in section and "task" not in section
