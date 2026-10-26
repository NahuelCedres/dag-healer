"""The GitHub setup docs (``docs/github/``, ``docs/security.md``) against the code they describe.

* ``docs/github/base-branch-ruleset.json`` holds only fields of GitHub's "Create a repository
  ruleset" body, and its rules, served as the base branch's rules, pass dag-healer's own base
  branch check (with the status-check rule that the README shows when ``skip_ci`` is on);
* ``docs/github/README.md`` shows the commit's skip line and the CI commands of the pull
  request's CI section verbatim, the doctor lines as the checks print them, and every protected
  path of the policy;
* the gaps table of ``docs/security.md`` matches its entries and is sorted by severity;
* the relative links of the docs that point at the GitHub setup resolve.

The GitHub API is an ``httpx.MockTransport``: no test touches the network.
"""

from __future__ import annotations

import json
import re
from pathlib import Path
from typing import Any

import httpx
import pytest

from dag_healer.agent.repair import SKIP_CI_LINE
from dag_healer.pr import github_checks
from dag_healer.pr.prmd import RUN_CI_MESSAGE, render_pr_markdown
from dag_healer.repo import protected_paths

REPO_ROOT = Path(__file__).resolve().parents[2]
GITHUB_DOCS = REPO_ROOT / "docs" / "github"
RULESET_FILE = GITHUB_DOCS / "base-branch-ruleset.json"
GITHUB_README = GITHUB_DOCS / "README.md"
SECURITY_DOC = REPO_ROOT / "docs" / "security.md"
ENV_EXAMPLE = REPO_ROOT / "docker" / ".env.example"

REPO = "acme-health/etl"
BASE = "main"
TOKEN = "github_pat_TEST"
RULESET_ID = 42
BRANCH = "healer/customers_sync-transform-0123abcd"

#: The fields of the request body of "Create a repository ruleset"
#: (``POST /repos/{owner}/{repo}/rulesets``); an export adds ``id``, ``source`` and the like.
CREATE_FIELDS = {"name", "target", "enforcement", "bypass_actors", "conditions", "rules"}
#: The parameters that GitHub requires on a ``pull_request`` rule.
PULL_REQUEST_PARAMETERS = {
    "required_approving_review_count": 1,
    "dismiss_stale_reviews_on_push": True,
    "require_code_owner_review": False,
    "require_last_push_approval": True,
    "required_review_thread_resolution": False,
}
SEVERITIES = ("Critical", "High", "Medium", "Low")

_LINK = re.compile(r"\]\(([^)\s]+)\)")
_GAP_ROW = re.compile(r"^\| (G\d+) \| (\w+) \| (.+) \|$", re.MULTILINE)
_GAP_ENTRY = re.compile(r"^\*\*(G\d+) \((\w+)\): (.+?)\.\*\*", re.MULTILINE | re.DOTALL)


def _read(path: Path) -> str:
    return path.read_text(encoding="utf-8").replace("\r\n", "\n")


def _ruleset() -> dict[str, Any]:
    return json.loads(_read(RULESET_FILE))


def _fenced_blocks(text: str, lang: str) -> list[str]:
    return re.findall(rf"^```{lang}\n(.*?)^```$", text, flags=re.MULTILINE | re.DOTALL)


def _status_check_rule() -> dict[str, Any]:
    (block,) = _fenced_blocks(_read(GITHUB_README), "json")
    return json.loads(block)


def _github(rules: list[dict[str, Any]], requests: list[httpx.Request]) -> httpx.MockTransport:
    """GitHub with ``rules`` as the base branch's rules, from one active repository ruleset.

    The branch is protected by that ruleset only: its classic protection answers 404 "Branch
    not protected", as GitHub does. The token's account has the Write role.
    """
    source = {"ruleset_source_type": "Repository", "ruleset_source": REPO, "ruleset_id": RULESET_ID}
    ruleset = {**_ruleset(), "id": RULESET_ID, "rules": rules, "current_user_can_bypass": "never"}
    permissions = {"admin": False, "maintain": False, "push": True, "triage": True, "pull": True}
    repo = f"/repos/{REPO}"
    answers: dict[str, tuple[int, Any]] = {
        repo: (200, {"full_name": REPO, "default_branch": BASE, "permissions": permissions}),
        f"{repo}/rules/branches/{BASE}": (200, [{**rule, **source} for rule in rules]),
        f"{repo}/rulesets/{RULESET_ID}": (200, ruleset),
        f"{repo}/branches/{BASE}": (200, {"name": BASE, "protected": True}),
        f"{repo}/branches/{BASE}/protection": (404, {"message": "Branch not protected"}),
    }

    def handler(request: httpx.Request) -> httpx.Response:
        requests.append(request)
        status, body = answers.get(request.url.path, (404, {"message": "Not Found"}))
        return httpx.Response(status, json=body)

    return httpx.MockTransport(handler)


def _setup(
    rules: list[dict[str, Any]], *, skip_ci: bool, workflows_root: Path | None = None
) -> dict[str, github_checks.SetupCheck]:
    requests: list[httpx.Request] = []
    checks = github_checks.check_github_setup(
        repo=REPO,
        base=BASE,
        token=TOKEN,
        skip_ci=skip_ci,
        workflows_root=workflows_root,
        transport=_github(rules, requests),
    )
    assert requests
    assert {(request.method, request.url.host) for request in requests} == {("GET", "api.github.com")}
    return {check.name: check for check in checks}


# --------------------------------------------------------------------------- the ruleset file


def test_the_ruleset_holds_only_fields_of_the_create_schema() -> None:
    text = _read(RULESET_FILE)
    ruleset = json.loads(text)
    assert text == json.dumps(ruleset, indent=2) + "\n"
    assert set(ruleset) == CREATE_FIELDS
    assert ruleset["target"] == "branch"
    assert ruleset["enforcement"] == "active"
    assert ruleset["bypass_actors"] == []
    assert ruleset["conditions"] == {"ref_name": {"include": ["~DEFAULT_BRANCH"], "exclude": []}}
    assert [rule["type"] for rule in ruleset["rules"]] == ["deletion", "non_fast_forward", "pull_request"]
    deletion, non_fast_forward, pull_request = ruleset["rules"]
    assert set(deletion) == set(non_fast_forward) == {"type"}
    assert pull_request["parameters"] == PULL_REQUEST_PARAMETERS


def test_the_documented_status_check_rule_has_the_required_parameters() -> None:
    rule = _status_check_rule()
    assert rule["type"] == "required_status_checks"
    assert set(rule["parameters"]) == {"strict_required_status_checks_policy", "required_status_checks"}
    checks = rule["parameters"]["required_status_checks"]
    assert checks and all(set(check) == {"context"} and check["context"] for check in checks)


def test_the_ruleset_meets_the_base_branch_check() -> None:
    checks = _setup(_ruleset()["rules"], skip_ci=False)
    base = checks[github_checks.BASE_BRANCH_CHECK]
    assert base.findings == ()
    assert base.level == github_checks.OK


def test_with_skip_ci_the_ruleset_lacks_only_the_status_checks() -> None:
    checks = _setup(_ruleset()["rules"], skip_ci=True)
    base = checks[github_checks.BASE_BRANCH_CHECK]
    assert [finding.level for finding in base.findings] == [github_checks.WARN]
    assert "requires no status checks" in base.findings[0].message


def test_the_documented_doctor_example_matches_the_checks(tmp_path: Path) -> None:
    """The example of "What doctor reports": the ruleset plus a status check, four workflows."""
    workflows = tmp_path / ".github" / "workflows"
    workflows.mkdir(parents=True)
    body = (
        "on:\n  push:\n    branches: [main]\n  pull_request:\n"
        "jobs:\n  unit-tests:\n    runs-on: ubuntu-latest\n    steps:\n      - run: pytest\n"
    )
    for name in ("tests.yml", "lint.yml", "docs.yaml", "build.yml"):
        with (workflows / name).open("w", encoding="utf-8", newline="\n") as handle:
            handle.write(body)
    checks = _setup([*_ruleset()["rules"], _status_check_rule()], skip_ci=True, workflows_root=tmp_path)
    (example,) = (block for block in _fenced_blocks(_read(GITHUB_README), "text") if "github token" in block)
    lines = dict(re.findall(r"^\[ OK \] (github[\w ]*?) {2,}(.+)$", example, flags=re.MULTILINE))
    for check in checks.values():
        assert check.findings == (), check.name
        assert lines[f"github {check.name}"] == check.summary


# --------------------------------------------------------------------------- docs/github/README.md


def test_the_readme_shows_the_skip_line_of_the_commit() -> None:
    assert f"```text\n{SKIP_CI_LINE}\n```" in _read(GITHUB_README)


def test_the_readme_shows_the_ci_commands_of_the_pull_request() -> None:
    markdown = render_pr_markdown(
        incident={"id": "0123abcd", "dag_id": "customers_sync", "task_id": "transform"},
        diagnosis="",
        test_path="tests/test_customers.py",
        test_code="",
        diff="",
        pytest_before="",
        pytest_after="",
        created_tests_dir=False,
        notes=[],
        branch=BRANCH,
        skip_ci=True,
        branch_on_github=True,
    )
    (commands,) = _fenced_blocks(markdown, "sh")
    assert RUN_CI_MESSAGE in commands and BRANCH in commands
    assert commands in _fenced_blocks(_read(GITHUB_README), "sh")


def test_the_readme_names_every_doctor_line() -> None:
    text = _read(GITHUB_README)
    for name in (
        github_checks.TOKEN_CHECK,
        github_checks.REPOSITORY_CHECK,
        github_checks.BASE_BRANCH_CHECK,
        github_checks.WORKFLOWS_CHECK,
    ):
        assert f"`github {name}`" in text, name


def _protected_names() -> set[str]:
    return {
        protected_paths.CONFIG_NAME,
        *protected_paths.DENIED_ROOTS,
        *protected_paths.CI_ROOT_DIRS,
        *protected_paths.CI_ROOT_FILES,
        *protected_paths.HOOK_ROOT_DIRS,
        *protected_paths.HOOK_ROOT_FILES,
        *protected_paths.HOOK_ROOT_PATTERNS,
        *protected_paths.DEV_ROOT_DIRS,
        *protected_paths.DEV_ROOT_FILES,
        *protected_paths.GIT_METADATA_NAMES,
    }


@pytest.mark.parametrize(
    ("doc", "extra"),
    [
        (GITHUB_README, protected_paths.CODEOWNERS_PATHS),
        (SECURITY_DOC, {"codeowners"}),
    ],
    ids=["readme", "security"],
)
def test_every_protected_path_is_documented(doc: Path, extra: set[str]) -> None:
    """Each name appears as a code span, alone or as a folder (``.circleci/``, ``.circleci/**``)."""
    text = _read(doc).lower()
    names = sorted(_protected_names() | set(extra))
    missing = [name for name in names if not re.search(rf"`{re.escape(name)}(/[^`]*)?`", text)]
    assert not missing


# --------------------------------------------------------------------------- docs/security.md


def test_the_gaps_table_matches_the_entries() -> None:
    text = _read(SECURITY_DOC)
    rows = _GAP_ROW.findall(text)
    entries = {gap: (severity, " ".join(title.split())) for gap, severity, title in _GAP_ENTRY.findall(text)}
    assert [gap for gap, _, _ in rows] and {gap for gap, _, _ in rows} == set(entries)
    for gap, severity, title in rows:
        entry_severity, entry_title = entries[gap]
        assert severity == entry_severity, gap
        assert title[:1].lower() + title[1:] == entry_title[:1].lower() + entry_title[1:], gap
    order = [(SEVERITIES.index(severity), int(gap[1:])) for gap, severity, _ in rows]
    assert order == sorted(order)
    assert entries["G2"][0] == "Medium"
    assert entries["G3"][0] == "High"


# --------------------------------------------------------------------------- the other docs


def test_the_env_example_asks_for_a_fine_grained_token() -> None:
    text = _read(ENV_EXAMPLE)
    start = text.index("# Optional: open real GitHub pull requests")
    advice = " ".join(text[start : text.index("\nGITHUB_TOKEN=", start)].replace("#", " ").split())
    for phrase in (
        "fine-grained personal access token limited to that one repository",
        "Contents and Pull requests",
        "no Workflows",
        "github.skip_ci",
        "docs/github/README.md",
    ):
        assert phrase in advice, phrase
    assert "scope" not in advice


@pytest.mark.parametrize(
    "relative",
    [
        "README.md",
        "docs/security.md",
        "docker/README.md",
        "examples/stepfunctions_repo/README.md",
    ],
    ids=["readme", "security", "docker", "sfn-example"],
)
def test_the_docs_link_the_github_setup(relative: str) -> None:
    path = REPO_ROOT / relative
    targets = [(path.parent / target.split("#", 1)[0]).resolve() for target in _LINK.findall(_read(path))]
    assert GITHUB_README.resolve() in targets


def test_the_docs_named_in_the_package_exist() -> None:
    """The ``healer.yaml`` template points at docs/github/README.md, docstrings at other docs."""
    named = {
        name
        for path in (REPO_ROOT / "src" / "dag_healer").rglob("*.py")
        for name in re.findall(r"\bdocs/[\w./-]+\.(?:md|json)\b", _read(path))
    }
    assert "docs/github/README.md" in named
    assert not [name for name in sorted(named) if not (REPO_ROOT / name).is_file()]


@pytest.mark.parametrize(
    "relative",
    [
        "README.md",
        "docs/security.md",
        "docs/github/README.md",
        "docker/README.md",
        "examples/stepfunctions_repo/README.md",
    ],
    ids=["readme", "security", "github", "docker", "sfn-example"],
)
def test_relative_links_resolve(relative: str) -> None:
    path = REPO_ROOT / relative
    broken = [
        target
        for target in _LINK.findall(_read(path))
        if not target.startswith(("http://", "https://", "mailto:", "#"))
        and not (path.parent / target.split("#", 1)[0]).exists()
    ]
    assert not broken
