"""Tests for the GitHub pull-request client and the PR.md renderer (dag_healer.pr)."""

from __future__ import annotations

import json
from pathlib import Path

import httpx
import pytest

from dag_healer._version import __version__
from dag_healer.core.vocab import Vocabulary
from dag_healer.orchestrators.fake import FAKE_VOCABULARY
from dag_healer.pr import create_pull_request as lazy_create_pull_request
from dag_healer.pr import review_checklist as lazy_review_checklist
from dag_healer.pr.github import GitHubError, create_pull_request
from dag_healer.pr.prmd import (
    MAX_DIFF_CHARS,
    REVIEW_CHECKLIST,
    diff_summary,
    pr_artifacts_dir,
    render_pr_markdown,
    render_pr_title,
    review_checklist,
    write_pr_artifacts,
)

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


def test_network_error_is_wrapped() -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        raise httpx.ConnectError("connection refused", request=request)

    with pytest.raises(GitHubError, match="could not reach") as info:
        create_pull_request(**_client_kwargs(handler))
    assert info.value.status_code is None


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
