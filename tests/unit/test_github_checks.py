"""Tests for dag_healer.pr.github_checks: the read-only checks of the GitHub setup.

No test touches the network: every API answer comes from an ``httpx.MockTransport`` routed on
the raw path of the request (the query string included; ``%2F`` stays encoded there, while
``request.url.path`` is decoded). The answers follow the shapes GitHub returns; the names are
fictional.
"""

from __future__ import annotations

import json
import logging
import os
import re
import textwrap
import time
from collections.abc import Mapping
from pathlib import Path
from typing import Any

import httpx
import pytest
import yaml

from dag_healer.pr import check_github_setup as lazy_check_github_setup
from dag_healer.pr import github_checks
from dag_healer.pr.github_checks import (
    BASE_BRANCH_CHECK,
    ERROR,
    MAX_WORKFLOW_BYTES,
    OK,
    REPOSITORY_CHECK,
    SETUP_CHECK,
    TOKEN_CHECK,
    WARN,
    WORKFLOWS_CHECK,
    SetupCheck,
    SetupFinding,
    check_github_setup,
    check_workflows,
    setup_failure,
    setup_findings,
    token_kind,
)

REPO = "acme-health/etl"
API = "/repos/acme-health/etl"
RULES = f"{API}/rules/branches/main?per_page=100"
BRANCH = f"{API}/branches/main"
PROTECTION = f"{API}/branches/main/protection"
INSTALLATION = "/installation/repositories?per_page=1"
FINE_GRAINED = "github_pat_TEST"
CHECK_TIMEOUT_FOR_TESTS = 5.0
HEALER_BRANCH = "healer/customers_sync-transform-1a2b3c4d"
NOT_FOUND = (
    404,
    {"message": "Not Found", "documentation_url": "https://docs.github.com/rest", "status": "404"},
)

#: A route: ``(status, body)``, ``(status, body, headers)``, an exception to raise or a handler.
Route = Any


class FakeGitHub:
    """A GitHub API that answers by raw path (``404 Not Found`` for anything else)."""

    def __init__(self, routes: Mapping[str, Route]) -> None:
        self.routes = dict(routes)
        self.requests: list[httpx.Request] = []

    def __call__(self, request: httpx.Request) -> httpx.Response:
        self.requests.append(request)
        route = self.routes.get(request.url.raw_path.decode("ascii"), NOT_FOUND)
        if isinstance(route, Exception):
            raise route
        if callable(route):
            return route(request)
        status, body, *rest = route
        headers = rest[0] if rest else {}
        if isinstance(body, bytes):
            return httpx.Response(status, content=body, headers=headers)
        return httpx.Response(status, json=body, headers=headers)

    @property
    def paths(self) -> list[str]:
        return [request.url.raw_path.decode("ascii") for request in self.requests]


def repository(**permissions: Any) -> dict[str, Any]:
    """``GET /repos/{owner}/{repo}`` as a token with the Write role sees it."""
    granted = {"admin": False, "maintain": False, "push": True, "triage": True, "pull": True, **permissions}
    return {
        "id": 1296269,
        "full_name": REPO,
        "default_branch": "main",
        "private": True,
        "permissions": granted,
    }


def rule(kind: str, ruleset_id: int = 42, **parameters: Any) -> dict[str, Any]:
    """One item of ``GET .../rules/branches/{branch}``."""
    item: dict[str, Any] = {
        "type": kind,
        "ruleset_source_type": "Repository",
        "ruleset_source": REPO,
        "ruleset_id": ruleset_id,
    }
    if parameters:
        item["parameters"] = parameters
    return item


PULL_REQUEST_RULE = rule(
    "pull_request",
    required_approving_review_count=1,
    dismiss_stale_reviews_on_push=True,
    require_code_owner_review=False,
    require_last_push_approval=True,
    required_review_thread_resolution=False,
)
STATUS_CHECKS_RULE = rule(
    "required_status_checks",
    43,
    strict_required_status_checks_policy=False,
    required_status_checks=[{"context": "unit-tests"}],
)
GOOD_RULES = [rule("deletion"), rule("non_fast_forward"), PULL_REQUEST_RULE, STATUS_CHECKS_RULE]


def ruleset(ruleset_id: int, name: str | None, bypass: str | None = "never") -> dict[str, Any]:
    """``GET /repos/{owner}/{repo}/rulesets/{id}``."""
    data: dict[str, Any] = {
        "id": ruleset_id,
        "target": "branch",
        "source_type": "Repository",
        "source": REPO,
        "enforcement": "active",
        "conditions": {"ref_name": {"include": ["~DEFAULT_BRANCH"], "exclude": []}},
        "rules": [],
    }
    if name is not None:
        data["name"] = name
    if bypass is not None:
        data["current_user_can_bypass"] = bypass
    return data


def branch(protected: bool, name: str = "main") -> dict[str, Any]:
    """``GET /repos/{owner}/{repo}/branches/{branch}``."""
    return {
        "name": name,
        "commit": {"sha": "6dcb09b5b57875f334f61aebed695e2e4193db5e"},
        "_links": {"self": f"https://api.github.com{API}/branches/{name}"},
        "protected": protected,
        "protection": {
            "enabled": protected,
            "required_status_checks": {"enforcement_level": "off", "contexts": [], "checks": []},
        },
        "protection_url": f"https://api.github.com{API}/branches/{name}/protection",
    }


def classic_protection(**changes: Any) -> dict[str, Any]:
    """``GET .../branches/{branch}/protection`` of a branch whose classic protection does it all."""
    protection: dict[str, Any] = {
        "url": f"https://api.github.com{API}/branches/main/protection",
        "required_status_checks": {
            "strict": True,
            "contexts": ["unit-tests"],
            "checks": [{"context": "unit-tests", "app_id": None}],
        },
        "enforce_admins": {"enabled": True},
        "required_pull_request_reviews": {
            "dismiss_stale_reviews": True,
            "require_code_owner_reviews": False,
            "required_approving_review_count": 1,
            "require_last_push_approval": False,
        },
        "required_linear_history": {"enabled": False},
        "allow_force_pushes": {"enabled": False},
        "allow_deletions": {"enabled": False},
        "block_creations": {"enabled": False},
        "lock_branch": {"enabled": False},
    }
    protection.update(changes)
    return protection


def well_set(**changes: Route) -> dict[str, Route]:
    """The routes of a repository whose ruleset does everything dag-healer recommends."""
    routes: dict[str, Route] = {
        API: (200, repository()),
        RULES: (200, GOOD_RULES),
        f"{API}/rulesets/42": (200, ruleset(42, "Protect the default branch")),
        f"{API}/rulesets/43": (200, ruleset(43, "Required checks")),
    }
    routes.update(changes)
    return routes


def unprotected(**changes: Route) -> dict[str, Route]:
    """The routes of a repository whose base branch nothing protects."""
    return {API: (200, repository()), RULES: (200, []), BRANCH: (200, branch(False)), **changes}


def run(
    routes: Mapping[str, Route],
    *,
    token: str = FINE_GRAINED,
    skip_ci: bool = True,
    base: str = "main",
    workflows_root: Path | None = None,
) -> tuple[dict[str, SetupCheck], FakeGitHub]:
    """Run the checks against ``routes``; every request must be a GET to the API host."""
    api = FakeGitHub(routes)
    checks = check_github_setup(
        repo=REPO,
        base=base,
        token=token,
        skip_ci=skip_ci,
        workflows_root=workflows_root,
        transport=httpx.MockTransport(api),
    )
    assert {(r.method, r.url.scheme, r.url.host) for r in api.requests} <= {
        ("GET", "https", "api.github.com")
    }
    assert all(request.headers["Authorization"] == f"Bearer {token}" for request in api.requests)
    return {check.name: check for check in checks}, api


def texts(check: SetupCheck) -> list[tuple[str, str]]:
    return [(finding.level, finding.text) for finding in check.findings]


# =========================================================================== results


def test_a_well_set_repository_passes_every_check() -> None:
    checks, api = run(well_set())
    assert list(checks) == [TOKEN_CHECK, REPOSITORY_CHECK, BASE_BRANCH_CHECK]
    assert {name: (check.level, check.summary) for name, check in checks.items()} == {
        TOKEN_CHECK: (OK, "a fine-grained personal access token"),
        REPOSITORY_CHECK: (OK, "the token's account is not an admin of acme-health/etl"),
        BASE_BRANCH_CHECK: (
            OK,
            "main requires a pull request with an approving review and blocks force pushes and "
            "deletion, and requires status checks",
        ),
    }
    # all the rules are met: the branch and its classic protection are not looked at
    assert api.paths == [API, RULES, f"{API}/rulesets/42", f"{API}/rulesets/43"]
    assert setup_findings(checks.values()) == []


def test_the_workflows_check_comes_last_when_a_clone_is_given(tmp_path: Path) -> None:
    checks, _ = run(well_set(), workflows_root=tmp_path)
    assert list(checks) == [TOKEN_CHECK, REPOSITORY_CHECK, BASE_BRANCH_CHECK, WORKFLOWS_CHECK]
    assert checks[WORKFLOWS_CHECK].summary == "no workflow files in .github/workflows"


def test_setup_findings_puts_the_errors_first_in_check_order() -> None:
    def finding(check: str, level: str, n: int) -> SetupFinding:
        return SetupFinding(check, level, f"message {n}", f"fix {n}")

    checks = [
        SetupCheck(TOKEN_CHECK, "", (finding(TOKEN_CHECK, WARN, 1),)),
        SetupCheck(
            BASE_BRANCH_CHECK, "", (finding(BASE_BRANCH_CHECK, ERROR, 2), finding(BASE_BRANCH_CHECK, WARN, 3))
        ),
        SetupCheck(WORKFLOWS_CHECK, "", (finding(WORKFLOWS_CHECK, ERROR, 4),)),
    ]
    assert [f.message for f in setup_findings(checks)] == ["message 2", "message 4", "message 1", "message 3"]
    assert [check.level for check in checks] == [WARN, ERROR, ERROR]
    assert SetupCheck(TOKEN_CHECK, "fine").level == OK
    assert finding(TOKEN_CHECK, WARN, 5).text == "message 5 fix 5"


def test_the_package_exports_the_checks_lazily() -> None:
    assert lazy_check_github_setup is check_github_setup


# =========================================================================== the token

CLASSIC_FIX = (
    "Use a fine-grained personal access token limited to acme-health/etl, or a GitHub App installed on "
    "acme-health/etl only."
)


@pytest.mark.parametrize(
    ("token", "kind", "message"),
    [
        pytest.param(FINE_GRAINED, "fine-grained", None, id="fine"),
        pytest.param("ghs_TEST", "app", None, id="app"),
        pytest.param(
            "ghp_TEST",
            "classic",
            "The GitHub token is a classic personal access token: its scope covers every repository its "
            "account can write to, not only acme-health/etl.",
            id="classic",
        ),
        pytest.param(
            "gho_TEST",
            "oauth",
            "The GitHub token is an OAuth token: its scope covers every repository its account can write "
            "to, not only acme-health/etl.",
            id="oauth",
        ),
        pytest.param(
            "ghu_TEST",
            "app-user",
            "The GitHub token is a GitHub App user token: it acts for its user on every repository that the "
            "App is installed on and the user can access, not only acme-health/etl.",
            id="user",
        ),
        pytest.param(
            "0123456789abcdef0123456789abcdef01234567",
            "unknown",
            "The kind of the GitHub token cannot be told from its prefix, so which repositories it can "
            "reach was not checked.",
            id="unknown",
        ),
    ],
)
def test_the_token_kind_is_told_from_its_prefix(token: str, kind: str, message: str | None) -> None:
    assert token_kind(token) == kind
    scopes = {"X-OAuth-Scopes": "repo"}  # GitHub lists the scopes of classic and OAuth tokens
    routes = well_set(
        **{API: (200, repository(), scopes), INSTALLATION: (200, {"total_count": 1, "repositories": []})}
    )
    checks, _ = run(routes, token=token)
    found = checks[TOKEN_CHECK]
    if message is None:
        assert found.level == OK
    else:
        assert texts(found) == [(WARN, f"{message} {CLASSIC_FIX}")]


WORKFLOW_SCOPE = (
    "The GitHub token has the workflow scope: it can push changes to .github/workflows/, which GitHub "
    "refuses to a token without it. Remove the workflow scope from the token, or use a fine-grained "
    "token without the Workflows permission."
)
SCOPES_UNKNOWN = (
    "Could not check the scopes of the GitHub token: GitHub's answer does not list them. Check by hand "
    "that the token has no workflow scope and no admin or delete scope."
)


@pytest.mark.parametrize(
    ("token", "scopes", "expected"),
    [
        pytest.param("ghp_TEST", "repo, workflow", ["classic", WORKFLOW_SCOPE], id="workflow"),
        pytest.param(
            "ghp_TEST",
            "admin:org, repo, delete_repo",
            [
                "classic",
                "The GitHub token has scopes that dag-healer never needs: admin:org, delete_repo. Remove "
                "them from the token, or use a fine-grained token limited to acme-health/etl.",
            ],
            id="admin",
        ),
        pytest.param(
            "gho_TEST",
            "repo,workflow,site_admin",
            ["oauth", WORKFLOW_SCOPE, "The GitHub token has scopes that dag-healer never needs: site_admin."],
            id="oauth",
        ),
        pytest.param("ghp_TEST", None, ["classic", SCOPES_UNKNOWN], id="no-header"),
        pytest.param("gho_TEST", None, ["oauth", SCOPES_UNKNOWN], id="oauth-no-header"),
        pytest.param("ghp_TEST", "", ["classic"], id="no-scopes"),
        pytest.param("ghp_TEST", "repo", ["classic"], id="repo-only"),
        pytest.param(FINE_GRAINED, "repo, workflow, admin:org", [], id="fine"),
        pytest.param("ghs_TEST", "workflow, delete_repo", [], id="app"),
        # a GitHub App user token has no scopes: the App's permissions limit it
        pytest.param("ghu_TEST", None, ["is a GitHub App user token"], id="app-user"),
    ],
)
def test_the_scopes_of_classic_and_oauth_tokens(token: str, scopes: str | None, expected: list[str]) -> None:
    headers = {"X-OAuth-Scopes": scopes} if scopes is not None else {}
    routes = well_set(**{API: (200, repository(), headers), INSTALLATION: (200, {"total_count": 1})})
    checks, _ = run(routes, token=token)
    found = [finding.text for finding in checks[TOKEN_CHECK].findings]
    kinds = {"classic": "is a classic personal access token", "oauth": "is an OAuth token"}
    assert len(found) == len(expected)
    for text, wanted in zip(found, expected, strict=True):
        assert kinds.get(wanted, wanted) in text


def test_the_scopes_are_read_from_an_error_answer_too() -> None:
    routes = well_set(**{API: (404, NOT_FOUND[1], {"X-OAuth-Scopes": "repo, workflow"})})
    checks, _ = run(routes, token="ghp_TEST")
    assert [finding.text for finding in checks[TOKEN_CHECK].findings][1] == WORKFLOW_SCOPE


@pytest.mark.parametrize(
    ("answer", "expected"),
    [
        pytest.param((200, {"total_count": 1, "repository_selection": "selected"}), None, id="one"),
        pytest.param(
            (200, {"total_count": 3, "repository_selection": "all"}),
            "The GitHub App installation behind the token can reach 3 repositories, not only "
            "acme-health/etl. Install the GitHub App on acme-health/etl only (Only select repositories).",
            id="three",
        ),
        pytest.param(
            (
                403,
                {
                    "message": "You must authenticate with an installation access token in order to list "
                    "repositories for an installation.",
                    "status": "403",
                },
            ),
            "Could not check how many repositories the GitHub App installation behind the token can "
            "reach: GitHub answered 403: You must authenticate with an installation access token in order "
            "to list repositories for an installation. Check by hand that the GitHub App is installed on "
            "acme-health/etl only.",
            id="forbidden",
        ),
        pytest.param(
            (200, {"total_count": "3"}),
            "Could not check how many repositories the GitHub App installation behind the token can "
            "reach: GitHub's answer could not be read. Check by hand that the GitHub App is installed on "
            "acme-health/etl only.",
            id="wrong-type",
        ),
    ],
)
def test_an_app_installation_should_reach_this_repository_only(answer: Route, expected: str | None) -> None:
    checks, api = run(well_set(**{INSTALLATION: answer}), token="ghs_TEST")
    assert api.paths[:3] == [API, INSTALLATION, RULES]  # right after the repository
    found = checks[TOKEN_CHECK]
    if expected is None:
        assert (found.level, found.summary) == (OK, "a GitHub App installation token limited to 1 repository")
    else:
        assert texts(found) == [(WARN, expected)]


def test_the_installation_is_asked_about_for_app_tokens_only() -> None:
    _, api = run(well_set(), token="ghp_TEST")
    assert INSTALLATION not in api.paths


# =========================================================================== the repository


@pytest.mark.parametrize(
    ("answer", "expected"),
    [
        pytest.param((200, repository()), None, id="write"),
        pytest.param(
            (200, repository(admin=True, maintain=True)),
            "The token's account is an admin of acme-health/etl: it can change or bypass the rules of the "
            "base branch. Use a token whose account has at most the Write role on acme-health/etl, such as "
            "a GitHub App or a dedicated account.",
            id="admin",
        ),
        pytest.param(
            (200, {"full_name": REPO}),
            "Could not check whether the token's account is an admin of acme-health/etl: GitHub's answer "
            "does not say. Check by hand that the token's account has at most the Write role on "
            "acme-health/etl.",
            id="no-permissions",
        ),
        pytest.param(
            (200, {"full_name": REPO, "permissions": ["admin"]}),
            "Could not check whether the token's account is an admin of acme-health/etl: GitHub's answer "
            "does not say. Check by hand that the token's account has at most the Write role on "
            "acme-health/etl.",
            id="odd-permissions",
        ),
        pytest.param(
            NOT_FOUND,
            "Could not check the token's access to acme-health/etl: GitHub answered 404: Not Found. Check "
            "github.repo, and that the token can read acme-health/etl.",
            id="not-found",
        ),
        pytest.param(
            (200, b"<html>maintenance</html>", {"Content-Type": "text/html"}),
            "Could not check the token's access to acme-health/etl: GitHub's answer could not be read. "
            "Check github.repo, and that the token can read acme-health/etl.",
            id="not-json",
        ),
        pytest.param(
            (200, [REPO]),
            "Could not check the token's access to acme-health/etl: GitHub's answer could not be read. "
            "Check github.repo, and that the token can read acme-health/etl.",
            id="wrong-type",
        ),
    ],
)
def test_the_token_account_should_not_be_an_admin(answer: Route, expected: str | None) -> None:
    checks, _ = run(well_set(**{API: answer}))
    found = checks[REPOSITORY_CHECK]
    if expected is None:
        assert (found.level, found.summary) == (OK, "the token's account is not an admin of acme-health/etl")
    else:
        assert texts(found) == [(WARN, expected)]


def test_a_long_error_answer_is_cut_and_kept_on_one_line() -> None:
    message = "API rate limit exceeded for user ID 1.\nIf you reach out to GitHub Support, " + "wait. " * 60
    checks, _ = run(well_set(**{API: (403, {"message": message, "status": "403"})}))
    text = checks[REPOSITORY_CHECK].findings[0].message
    detail = text.removeprefix("Could not check the token's access to acme-health/etl: GitHub answered 403: ")
    assert detail.startswith(
        "API rate limit exceeded for user ID 1. If you reach out to GitHub Support, wait."
    )
    assert "\n" not in detail and detail.endswith("....")  # cut to 200 characters, then the sentence's period
    assert len(detail) == 201


def test_an_error_answer_without_messages_is_shown_as_its_text() -> None:
    """errors[] items without message or code: GitHub's text is shown, and the other checks still run."""
    body = {"errors": [{"resource": "Repository", "field": "name"}]}
    text = json.dumps(body, separators=(",", ":"))  # as httpx sends it
    checks, _ = run(well_set(**{API: (422, body)}))
    assert texts(checks[REPOSITORY_CHECK]) == [
        (
            WARN,
            f"Could not check the token's access to {REPO}: GitHub answered 422: {text}. Check github.repo, "
            f"and that the token can read {REPO}.",
        )
    ]
    assert list(checks) == [TOKEN_CHECK, REPOSITORY_CHECK, BASE_BRANCH_CHECK]
    assert checks[BASE_BRANCH_CHECK].level == OK


def test_the_token_is_redacted_before_an_error_text_is_cut(caplog: pytest.LogCaptureFixture) -> None:
    canary = "ghp_CANARY0123456789abcdefCANARY"
    echo = (500, ("x" * 180 + canary).encode(), {"Content-Type": "text/plain"})
    with caplog.at_level(logging.DEBUG, logger="dag_healer"):
        checks, _ = run(well_set(**{API: echo}), token=canary)
    shown = " ".join(finding.text for check in checks.values() for finding in check.findings)
    assert "GitHub answered 500: " + "x" * 10 in shown
    assert not [n for n in range(len(canary) - 5) if canary[n : n + 6] in shown + caplog.text]


def test_a_token_far_into_a_long_error_body_is_still_redacted() -> None:
    canary = "ghp_CANARY0123456789abcdefCANARY"
    padding = " " * (4096 - len(canary) // 2)  # the token runs across byte 4096 of the body
    echo = (500, (padding + canary).encode(), {"Content-Type": "text/plain"})
    checks, _ = run(well_set(**{API: echo}), token=canary)
    shown = " ".join(finding.text for check in checks.values() for finding in check.findings)
    assert "GitHub answered 500: ***" in shown
    assert not [n for n in range(len(canary) - 5) if canary[n : n + 6] in shown]


@pytest.mark.parametrize(
    "location",
    [
        pytest.param("https://api.github.com/repositories/1296269", id="same-host"),
        pytest.param("https://collector.example.com/repos/acme-health/etl", id="other-host"),
    ],
)
def test_a_redirect_is_reported_and_never_followed(location: str) -> None:
    moved = (301, {"message": "Moved Permanently", "url": location}, {"Location": location})
    checks, api = run(well_set(**{API: moved, RULES: moved, BRANCH: moved}))
    assert "/repositories/1296269" not in api.paths and len(api.paths) == 3  # repository, rules, branch
    assert texts(checks[REPOSITORY_CHECK])[0][1].startswith(
        "Could not check the token's access to acme-health/etl: GitHub answered 301: Moved Permanently."
    )
    assert texts(checks[BASE_BRANCH_CHECK]) == [
        (
            WARN,
            "Could not check the rules of the base branch main: GitHub answered 301: Moved Permanently. "
            "Check by hand that main requires a pull request with at least one approving review and blocks "
            "force pushes and deletion.",
        )
    ]


# =========================================================================== the base branch

NO_REVIEW = (
    "The base branch main does not require a pull request with at least one approving review, so a "
    "change can reach it without review. Add a ruleset (or branch protection) for main that requires a "
    "pull request with at least one approving review."
)
FORCE_PUSHES = (
    "The base branch main does not block force pushes, so its history can be rewritten. Block force "
    "pushes in the ruleset (or branch protection) of main."
)
DELETION = (
    "The base branch main can be deleted. Restrict deletions in the ruleset (or branch protection) of main."
)
NO_STATUS_CHECKS = (
    "The base branch main requires no status checks, so nothing stops a merge before CI has run on the "
    "healer branch. Add the CI's checks to the ruleset (or branch protection) of main as required status "
    "checks."
)
UNPROTECTED = [(ERROR, NO_REVIEW), (ERROR, FORCE_PUSHES), (ERROR, DELETION), (WARN, NO_STATUS_CHECKS)]
CHECK_BY_HAND = (
    "Check by hand that main requires a pull request with at least one approving review and blocks force "
    "pushes and deletion."
)


def test_an_unprotected_base_branch_is_an_error_per_missing_rule() -> None:
    checks, api = run(unprotected())
    assert texts(checks[BASE_BRANCH_CHECK]) == UNPROTECTED
    assert api.paths == [API, RULES, BRANCH]  # not protected: no protection to read, no ruleset


def test_without_skip_ci_status_checks_are_not_required() -> None:
    checks, _ = run(unprotected(), skip_ci=False)
    assert texts(checks[BASE_BRANCH_CHECK]) == UNPROTECTED[:3]
    good, _ = run(well_set(**{RULES: (200, GOOD_RULES[:3])}), skip_ci=False)
    assert (good[BASE_BRANCH_CHECK].level, good[BASE_BRANCH_CHECK].summary) == (
        OK,
        "main requires a pull request with an approving review and blocks force pushes and deletion",
    )


@pytest.mark.parametrize(
    ("rules", "expected"),
    [
        pytest.param(
            [rule("deletion"), rule("non_fast_forward"), PULL_REQUEST_RULE], [UNPROTECTED[3]], id="no-checks"
        ),
        pytest.param(
            [rule("pull_request", required_approving_review_count=0), *GOOD_RULES[:2], STATUS_CHECKS_RULE],
            [UNPROTECTED[0]],
            id="no-approval",
        ),
        pytest.param(
            [*GOOD_RULES[:3], rule("required_status_checks", required_status_checks=[])],
            [UNPROTECTED[3]],
            id="empty-checks",
        ),
        pytest.param(
            [rule("creation"), rule("required_signatures"), PULL_REQUEST_RULE], UNPROTECTED[1:], id="others"
        ),
    ],
)
def test_each_rule_the_rulesets_miss_is_reported(rules: list[dict[str, Any]], expected: list[Any]) -> None:
    checks, _ = run(
        well_set(
            **{
                RULES: (200, rules),
                BRANCH: (200, branch(True)),
                PROTECTION: (404, {"message": "Branch not protected"}),
            }
        )
    )
    assert texts(checks[BASE_BRANCH_CHECK]) == expected


@pytest.mark.parametrize(
    ("protection", "expected"),
    [
        pytest.param((200, classic_protection()), [], id="classic-all"),
        pytest.param(
            (
                200,
                classic_protection(allow_force_pushes={"enabled": True}, required_pull_request_reviews=None),
            ),
            [UNPROTECTED[0], UNPROTECTED[1]],
            id="classic-partly",
        ),
        pytest.param(
            (200, classic_protection(required_status_checks={"strict": True, "contexts": [], "checks": []})),
            [UNPROTECTED[3]],
            id="classic-no-checks",
        ),
        pytest.param(
            (404, {"message": "Branch not protected", "status": "404"}), UNPROTECTED, id="not-protected"
        ),
        pytest.param(
            (403, {"message": "Resource not accessible by personal access token", "status": "403"}),
            [
                (
                    WARN,
                    "The base branch main has branch protection that the token cannot read, so it was not "
                    "checked whether it requires a pull request with at least one approving review, blocks "
                    "force pushes, blocks deletion and requires status checks. Check it by hand in the "
                    "repository's settings (Branches), or protect main with a ruleset, which the token can "
                    "read.",
                )
            ],
            id="unreadable",
        ),
        pytest.param(
            (404, {"message": "Not Found"}),
            [
                (
                    WARN,
                    "The base branch main has branch protection that the token cannot read, so it was not "
                    "checked whether it requires a pull request with at least one approving review, blocks "
                    "force pushes, blocks deletion and requires status checks. Check it by hand in the "
                    "repository's settings (Branches), or protect main with a ruleset, which the token can "
                    "read.",
                )
            ],
            id="other-404",
        ),
    ],
)
def test_classic_branch_protection_counts_when_the_token_can_read_it(
    protection: Route, expected: list[Any]
) -> None:
    checks, api = run(unprotected(**{BRANCH: (200, branch(True)), PROTECTION: protection}))
    assert texts(checks[BASE_BRANCH_CHECK]) == expected
    assert api.paths == [API, RULES, BRANCH, PROTECTION]


def test_unreadable_protection_names_only_the_rules_still_unknown() -> None:
    routes = well_set(
        **{
            RULES: (200, [rule("deletion"), PULL_REQUEST_RULE]),
            BRANCH: (200, branch(True)),
            PROTECTION: (403, {"message": "Resource not accessible by personal access token"}),
        }
    )
    checks, _ = run(routes)
    assert [finding.message for finding in checks[BASE_BRANCH_CHECK].findings] == [
        "The base branch main has branch protection that the token cannot read, so it was not checked "
        "whether it blocks force pushes and requires status checks."
    ]


def test_a_base_branch_that_does_not_exist_is_one_warning() -> None:
    # the rules endpoint answers 200 [] for a branch that does not exist
    routes = unprotected(**{BRANCH: (404, {"message": "Branch not found", "status": "404"})})
    checks, _ = run(routes)
    assert texts(checks[BASE_BRANCH_CHECK]) == [
        (
            WARN,
            "Could not check the base branch main: GitHub answered 404: Branch not found. Check "
            "github.pr_base, and check by hand that main requires a pull request with at least one "
            "approving review and blocks force pushes and deletion.",
        )
    ]


@pytest.mark.parametrize(
    ("branch_answer", "protection", "expected"),
    [
        pytest.param(NOT_FOUND, None, "rules", id="branch-unreadable"),
        pytest.param((200, branch(False)), None, "errors", id="unprotected"),
        pytest.param((200, branch(True)), (200, classic_protection()), "ok", id="classic-all"),
        pytest.param(
            (200, branch(True)),
            (200, classic_protection(allow_deletions={"enabled": True})),
            "rules",
            id="classic-partly",
        ),
        pytest.param((200, branch(True)), (403, {"message": "Forbidden"}), "rules", id="both-unreadable"),
        pytest.param((200, {"name": "main", "protected": "yes"}), None, "rules", id="odd-branch"),
    ],
)
def test_unreadable_rules_are_one_warning_unless_the_branch_settles_it(
    branch_answer: Route, protection: Route | None, expected: str
) -> None:
    routes = {API: (200, repository()), RULES: (500, {"message": "Server Error"}), BRANCH: branch_answer}
    if protection is not None:
        routes[PROTECTION] = protection
    checks, _ = run(routes)
    found = texts(checks[BASE_BRANCH_CHECK])
    if expected == "rules":
        assert found == [
            (
                WARN,
                "Could not check the rules of the base branch main: GitHub answered 500: Server Error. "
                + CHECK_BY_HAND,
            )
        ]
    elif expected == "errors":  # nothing protects the branch: no ruleset either
        assert found == UNPROTECTED
    else:
        assert found == []


def test_the_rules_are_read_page_by_page() -> None:
    first_page = [rule("creation", 7) for _ in range(100)]
    routes = well_set(**{RULES: (200, first_page), f"{RULES}&page=2": (200, GOOD_RULES)})
    checks, api = run(routes)
    assert checks[BASE_BRANCH_CHECK].level == OK
    assert api.paths[:3] == [API, RULES, f"{RULES}&page=2"]


def test_a_later_page_that_fails_makes_the_missing_rules_unknown() -> None:
    first_page = [PULL_REQUEST_RULE] + [rule("creation", 7) for _ in range(99)]
    routes = well_set(
        **{
            RULES: (200, first_page),
            f"{RULES}&page=2": (502, {"message": "Bad Gateway"}),
            BRANCH: (200, branch(True)),
            PROTECTION: (404, {"message": "Branch not protected"}),
        }
    )
    checks, _ = run(routes)
    assert texts(checks[BASE_BRANCH_CHECK]) == [
        (
            WARN,
            "Could not check the rules of the base branch main: GitHub answered 502: Bad Gateway. "
            + CHECK_BY_HAND,
        )
    ]


def test_a_branch_name_with_a_slash_is_one_path_segment() -> None:
    rules = f"{API}/rules/branches/release%2F2.0?per_page=100"
    branch_path = f"{API}/branches/release%2F2.0"
    routes = {API: (200, repository()), rules: (200, []), branch_path: (200, branch(True, "release/2.0"))}
    checks, api = run(routes, base="release/2.0")
    assert api.paths == [API, rules, branch_path, f"{branch_path}/protection"]
    assert [request.url.path for request in api.requests][1] == f"{API}/rules/branches/release/2.0"  # decoded
    assert (
        checks[BASE_BRANCH_CHECK]
        .findings[0]
        .message.startswith("The base branch release/2.0 has branch protection")
    )


@pytest.mark.parametrize(
    ("answer", "expected"),
    [
        pytest.param(
            (200, ruleset(42, "Protect the default branch", "always")),
            'The token\'s account can bypass the ruleset "Protect the default branch" of main at any time.',
            id="always",
        ),
        pytest.param(
            (200, ruleset(42, "Protect the default branch", "pull_requests_only")),
            'The token\'s account can bypass the ruleset "Protect the default branch" of main for pull '
            "requests.",
            id="pull-requests",
        ),
        pytest.param(
            (200, ruleset(42, None, "exempt")),
            'The token\'s account is exempt from the ruleset "#42" of main.',
            id="exempt",
        ),
        pytest.param((200, ruleset(42, "Protect the default branch", "never")), None, id="never"),
        pytest.param((200, ruleset(42, "Protect the default branch", None)), None, id="not-returned"),
        pytest.param(NOT_FOUND, None, id="organization"),
        pytest.param(
            (403, {"message": "Resource not accessible by personal access token"}), None, id="refused"
        ),
    ],
)
def test_a_ruleset_the_token_account_can_bypass_is_a_warning(answer: Route, expected: str | None) -> None:
    checks, _ = run(well_set(**{f"{API}/rulesets/42": answer}))
    found = texts(checks[BASE_BRANCH_CHECK])
    if expected is None:
        assert found == []
    else:
        fix = "Remove the account, or its role, team or app, from the ruleset's bypass list."
        assert found == [(WARN, f"{expected} {fix}")]


def test_a_ruleset_answer_that_cannot_be_read_is_one_warning() -> None:
    routes = well_set(
        **{f"{API}/rulesets/42": (200, ["not", "a", "ruleset"]), f"{API}/rulesets/43": (200, b"{")}
    )
    checks, _ = run(routes)
    assert texts(checks[BASE_BRANCH_CHECK]) == [
        (
            WARN,
            "Could not check whether the token's account can bypass the rulesets of main: GitHub's answer "
            "could not be read. Check by hand that the token's account is in the bypass list of no ruleset "
            "of main.",
        )
    ]


@pytest.mark.parametrize(
    ("answer", "why"),
    [
        pytest.param(
            (500, {"message": "Server Error"}), "GitHub answered 500: Server Error", id="server-error"
        ),
        pytest.param(
            (429, {"message": "API rate limit exceeded for user ID 1."}, {"Retry-After": "60"}),
            "GitHub answered 429: API rate limit exceeded for user ID 1",
            id="rate-limit",
        ),
        pytest.param(
            (502, b"<html>Bad Gateway</html>"), "GitHub answered 502: <html>Bad Gateway</html>", id="html"
        ),
    ],
)
def test_a_ruleset_error_other_than_refused_is_one_warning(answer: Route, why: str) -> None:
    """Only 403 and 404 mean that GitHub does not show the ruleset; any other error leaves it unchecked."""
    checks, api = run(well_set(**{f"{API}/rulesets/42": answer}))
    assert f"{API}/rulesets/43" in api.paths  # the next ruleset is still read
    assert checks[BASE_BRANCH_CHECK].level == WARN
    assert texts(checks[BASE_BRANCH_CHECK]) == [
        (
            WARN,
            f"Could not check whether the token's account can bypass the rulesets of main: {why}. Check by "
            "hand that the token's account is in the bypass list of no ruleset of main.",
        )
    ]


@pytest.mark.parametrize(
    "answer",
    [
        pytest.param(
            (
                403,
                {"message": "API rate limit exceeded for user ID 1.", "status": "403"},
                {"X-RateLimit-Remaining": "0", "X-RateLimit-Reset": "1700000000"},
            ),
            id="primary",
        ),
        pytest.param(
            (403, {"message": "You have exceeded a secondary rate limit. Please wait a few minutes."}),
            id="secondary",
        ),
    ],
)
def test_a_ruleset_refused_by_the_rate_limit_is_one_warning(answer: Route) -> None:
    """A 403 of the rate limits is no refusal to show the ruleset: its bypass list is not known."""
    checks, _ = run(well_set(**{f"{API}/rulesets/42": answer}))
    found = texts(checks[BASE_BRANCH_CHECK])
    assert len(found) == 1 and found[0][0] == WARN
    assert found[0][1].startswith(
        "Could not check whether the token's account can bypass the rulesets of main: GitHub answered 403: "
    )
    assert "rate limit" in found[0][1]


def test_at_most_ten_rulesets_are_read() -> None:
    rules = [rule("deletion", ruleset_id) for ruleset_id in range(1, 13)] + GOOD_RULES
    checks, api = run(well_set(**{RULES: (200, rules)}))
    assert [path for path in api.paths if "/rulesets/" in path] == [
        f"{API}/rulesets/{n}" for n in range(1, 11)
    ]
    # the rulesets 11, 12, 42 and 43 are not read: one warning says so
    assert texts(checks[BASE_BRANCH_CHECK]) == [
        (
            WARN,
            "Could not check whether the token's account can bypass 4 more rulesets of main: only the "
            "first 10 are read. Check by hand that the token's account is in the bypass list of no ruleset "
            "of main.",
        )
    ]
    one_more = [rule("deletion", ruleset_id) for ruleset_id in range(1, 10)] + GOOD_RULES
    checks, _ = run(well_set(**{RULES: (200, one_more)}))
    assert [finding.message for finding in checks[BASE_BRANCH_CHECK].findings] == [
        "Could not check whether the token's account can bypass 1 more ruleset of main: only the first 10 "
        "are read."
    ]


def test_rulesets_of_rules_that_are_not_required_are_not_read() -> None:
    routes = well_set(**{RULES: (200, [*GOOD_RULES[:3], rule("required_signatures", 99)])})
    _, api = run(routes, skip_ci=False)
    assert f"{API}/rulesets/99" not in api.paths and f"{API}/rulesets/43" not in api.paths


# =========================================================================== could not check


@pytest.mark.parametrize(
    ("error", "name"),
    [
        pytest.param(httpx.ConnectError("connection refused"), "ConnectError", id="connect"),
        pytest.param(httpx.ConnectTimeout("timed out"), "ConnectTimeout", id="timeout"),
    ],
)
def test_after_a_transport_error_nothing_more_is_sent(error: Exception, name: str) -> None:
    why = f"the GitHub API could not be reached ({name})"
    checks, api = run({API: error}, token="ghs_TEST")
    assert api.paths == [API]
    assert texts(checks[TOKEN_CHECK]) == [
        (
            WARN,
            f"Could not check how many repositories the GitHub App installation behind the token can reach: "
            f"{why}. Check by hand that the GitHub App is installed on acme-health/etl only.",
        )
    ]
    assert texts(checks[REPOSITORY_CHECK]) == [
        (
            WARN,
            f"Could not check the token's access to acme-health/etl: {why}. Check github.repo, and that the "
            "token can read acme-health/etl.",
        )
    ]
    assert texts(checks[BASE_BRANCH_CHECK]) == [
        (WARN, f"Could not check the rules of the base branch main: {why}. {CHECK_BY_HAND}")
    ]


def test_a_timeout_later_on_stops_the_requests_there() -> None:
    checks, api = run(well_set(**{RULES: httpx.ReadTimeout("read timed out")}))
    assert api.paths == [API, RULES]
    assert checks[REPOSITORY_CHECK].level == OK
    assert texts(checks[BASE_BRANCH_CHECK]) == [
        (
            WARN,
            "Could not check the rules of the base branch main: the GitHub API could not be reached "
            f"(ReadTimeout). {CHECK_BY_HAND}",
        )
    ]


def test_an_unexpected_failure_of_a_request_stops_the_requests(caplog: pytest.LogCaptureFixture) -> None:
    def explode(request: httpx.Request) -> httpx.Response:
        raise RuntimeError(f"unexpected, token {request.headers['Authorization']}")

    with caplog.at_level(logging.DEBUG, logger="dag_healer"):
        checks, api = run({API: explode}, token="ghp_TEST")
    assert api.paths == [API]
    why = "the request to the GitHub API failed (RuntimeError)"
    assert texts(checks[TOKEN_CHECK])[1] == (
        WARN,
        f"Could not check the scopes of the GitHub token: {why}. Check by hand that the token has no "
        "workflow scope and no admin or delete scope.",
    )
    assert texts(checks[REPOSITORY_CHECK])[0][1].startswith(
        f"Could not check the token's access to {REPO}: {why}."
    )
    assert texts(checks[BASE_BRANCH_CHECK]) == [
        (WARN, f"Could not check the rules of the base branch main: {why}. {CHECK_BY_HAND}")
    ]
    assert "ghp_TEST" not in caplog.text and "unexpected" not in caplog.text


def test_a_check_that_fails_unexpectedly_is_one_warning_and_the_others_still_run(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, caplog: pytest.LogCaptureFixture
) -> None:
    def broken(*_args: Any, **_kwargs: Any) -> SetupCheck:
        raise RecursionError(f"maximum recursion depth exceeded near {FINE_GRAINED}")

    monkeypatch.setattr(github_checks, "_base_branch_check", broken)
    write_workflow(tmp_path, "prt.yml", target())
    with caplog.at_level(logging.DEBUG, logger="dag_healer"):
        checks, _ = run(well_set(), workflows_root=tmp_path)
    assert list(checks) == [TOKEN_CHECK, REPOSITORY_CHECK, BASE_BRANCH_CHECK, WORKFLOWS_CHECK]
    assert checks[REPOSITORY_CHECK].level == OK
    assert texts(checks[BASE_BRANCH_CHECK]) == [
        (WARN, f"The rules of the base branch main could not be checked (RecursionError). {CHECK_BY_HAND}")
    ]
    assert labels(checks[WORKFLOWS_CHECK]) == ["error:target"]  # the local workflow error stays
    assert FINE_GRAINED not in caplog.text and "RecursionError" in caplog.text


def test_a_broken_workflows_check_is_one_warning(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    def broken(*_args: Any, **_kwargs: Any) -> SetupCheck:
        raise MemoryError

    monkeypatch.setattr(github_checks, "check_workflows", broken)
    checks, _ = run(well_set(), workflows_root=tmp_path)
    assert texts(checks[WORKFLOWS_CHECK]) == [
        (
            WARN,
            "The workflow files could not be checked (MemoryError). Check them by hand: no "
            "pull_request_target or create workflow may run the healer branch's code with the repository's "
            "secrets.",
        )
    ]
    assert checks[BASE_BRANCH_CHECK].level == OK


def test_a_deeply_nested_answer_is_one_that_cannot_be_read(tmp_path: Path) -> None:
    nested = b"[" * 100_000 + b"]" * 100_000  # json.loads raises RecursionError on it
    write_workflow(tmp_path, "prt.yml", target())
    checks, _ = run(well_set(**{RULES: (200, nested)}), workflows_root=tmp_path)
    assert texts(checks[BASE_BRANCH_CHECK]) == [
        (
            WARN,
            "Could not check the rules of the base branch main: GitHub's answer could not be read. "
            + CHECK_BY_HAND,
        )
    ]
    assert labels(checks[WORKFLOWS_CHECK]) == ["error:target"]
    assert checks[REPOSITORY_CHECK].level == OK


def test_an_answer_is_read_up_to_a_limit(monkeypatch: pytest.MonkeyPatch) -> None:
    pulled: list[int] = []

    def huge(request: httpx.Request) -> httpx.Response:
        def chunks() -> Any:
            for number in range(1000):  # 64 MB if it were all read
                pulled.append(number)
                yield b" " * 65536

        return httpx.Response(200, content=chunks())

    monkeypatch.setattr(github_checks, "MAX_ANSWER_BYTES", 300_000)
    checks, api = run(well_set(**{f"{API}/rulesets/42": huge}))
    assert len(pulled) <= 6
    assert texts(checks[BASE_BRANCH_CHECK]) == [
        (
            WARN,
            "Could not check whether the token's account can bypass the rulesets of main: GitHub's answer "
            "was too large to read. Check by hand that the token's account is in the bypass list of no "
            "ruleset of main.",
        )
    ]
    assert f"{API}/rulesets/43" in api.paths  # the next request is still sent
    checks, _ = run(well_set(**{RULES: huge}))
    assert texts(checks[BASE_BRANCH_CHECK]) == [
        (
            WARN,
            "Could not check the rules of the base branch main: GitHub's answer was too large to read. "
            + CHECK_BY_HAND,
        )
    ]


def _checks_with_timeout(
    routes: Mapping[str, Route], timeout: float
) -> tuple[dict[str, SetupCheck], FakeGitHub]:
    api = FakeGitHub(routes)
    checks = check_github_setup(
        repo=REPO,
        base="main",
        token=FINE_GRAINED,
        skip_ci=True,
        workflows_root=None,
        transport=httpx.MockTransport(api),
        timeout=timeout,
    )
    return {check.name: check for check in checks}, api


def test_an_answer_that_comes_too_slowly_stops_the_requests(monkeypatch: pytest.MonkeyPatch) -> None:
    def slow(request: httpx.Request) -> httpx.Response:
        def chunks() -> Any:
            for _ in range(200):  # a byte every 10 ms: each wait is shorter than the timeout
                time.sleep(0.01)
                yield b" "

        return httpx.Response(200, content=chunks())

    monkeypatch.setattr(github_checks, "_RUN_TIMEOUTS", 10_000)  # only the request's own time counts here
    found, api = _checks_with_timeout({**well_set(), RULES: slow}, timeout=0.05)
    assert api.paths == [API, RULES]  # nothing more after it
    assert texts(found[BASE_BRANCH_CHECK]) == [
        (
            WARN,
            "Could not check the rules of the base branch main: the GitHub API answered too slowly. "
            + CHECK_BY_HAND,
        )
    ]


def test_the_requests_of_a_run_share_one_deadline(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(github_checks, "_RUN_TIMEOUTS", -1)  # already over
    found, api = _checks_with_timeout(well_set(), timeout=CHECK_TIMEOUT_FOR_TESTS)
    assert api.paths == []
    why = "the GitHub API did not answer in time"
    assert texts(found[REPOSITORY_CHECK])[0][1].startswith(
        f"Could not check the token's access to {REPO}: {why}."
    )
    assert texts(found[BASE_BRANCH_CHECK]) == [
        (WARN, f"Could not check the rules of the base branch main: {why}. {CHECK_BY_HAND}")
    ]


def test_a_compressed_answer_is_never_decompressed() -> None:
    bomb = (200, b"\x1f\x8b\x08\x00not really gzip", {"Content-Encoding": "gzip"})
    checks, api = run(well_set(**{API: bomb}))
    assert all(request.headers["Accept-Encoding"] == "identity" for request in api.requests)
    assert texts(checks[REPOSITORY_CHECK]) == [
        (
            WARN,
            f"Could not check the token's access to {REPO}: GitHub's answer could not be read. Check "
            f"github.repo, and that the token can read {REPO}.",
        )
    ]


def test_a_ruleset_without_an_answer_is_one_warning() -> None:
    checks, _ = run(well_set(**{f"{API}/rulesets/42": httpx.ReadTimeout("read timed out")}))
    assert texts(checks[BASE_BRANCH_CHECK]) == [
        (
            WARN,
            "Could not check whether the token's account can bypass the rulesets of main: the GitHub API "
            "could not be reached (ReadTimeout). Check by hand that the token's account is in the bypass "
            "list of no ruleset of main.",
        )
    ]


def test_scopes_that_could_not_be_read_are_a_warning() -> None:
    checks, _ = run({API: httpx.ConnectError("connection refused")}, token="ghp_TEST")
    assert texts(checks[TOKEN_CHECK])[1] == (
        WARN,
        "Could not check the scopes of the GitHub token: the GitHub API could not be reached "
        "(ConnectError). Check by hand that the token has no workflow scope and no admin or delete scope.",
    )


def test_a_token_that_is_no_header_value_is_one_setup_warning(tmp_path: Path) -> None:
    write_workflow(tmp_path, "prt.yml", target())
    checks, api = run(well_set(), token="ghp_TÉST", workflows_root=tmp_path)
    assert list(checks) == [SETUP_CHECK, WORKFLOWS_CHECK] and api.requests == []
    assert checks[SETUP_CHECK].findings == (setup_failure(UnicodeEncodeError("ascii", "", 0, 1, "")),)
    assert texts(checks[SETUP_CHECK]) == [
        (
            WARN,
            "The GitHub setup could not be checked (UnicodeEncodeError). Check the network settings and "
            "github.repo, then check the token, the base branch's rules and the workflows by hand.",
        )
    ]
    assert "doctor" not in checks[SETUP_CHECK].findings[0].fix  # doctor prints it too
    assert labels(checks[WORKFLOWS_CHECK]) == ["error:target"]


def test_an_invalid_repository_name_sends_nothing() -> None:
    api = FakeGitHub({})
    checks = check_github_setup(
        repo="no-slash", base="main", token=FINE_GRAINED, skip_ci=True, workflows_root=None,
        transport=httpx.MockTransport(api),
    )  # fmt: skip
    assert [check.name for check in checks] == [SETUP_CHECK] and api.requests == []


def test_the_token_never_appears_in_a_finding_or_a_log(caplog: pytest.LogCaptureFixture) -> None:
    canary = "ghp_CANARY0123456789abcdefCANARY"
    echo = {"message": f"Bad credentials for {canary}"}
    routes = {
        API: (401, echo, {"X-OAuth-Scopes": "repo, workflow"}),
        RULES: (200, [rule("deletion", 5)]),
        BRANCH: (200, branch(True)),
        PROTECTION: (403, {"message": f"token {canary} cannot read this"}),
        f"{API}/rulesets/5": (200, ruleset(5, f"named after {canary}", "always")),
    }
    with caplog.at_level(logging.DEBUG, logger="dag_healer"):
        checks, _ = run(routes, token=canary)
    shown = [check.summary for check in checks.values()]
    shown += [
        text
        for check in checks.values()
        for finding in check.findings
        for text in (finding.message, finding.fix)
    ]
    assert any("named after ***" in text for text in shown)  # the answers were shown, redacted
    assert not [text for text in shown if canary in text or "ghp_" in text]
    assert canary not in caplog.text


def test_control_characters_from_github_are_not_shown() -> None:
    routes = well_set(**{f"{API}/rulesets/42": (200, ruleset(42, "main\n[ OK ] github\x1b[2J", "always"))})
    checks, _ = run(routes)
    message = checks[BASE_BRANCH_CHECK].findings[0].message
    assert "\n" not in message and "\x1b" not in message
    assert '"main [ OK ] github?[2J"' in message


# =========================================================================== the workflows


def write_workflow(root: Path, name: str, content: str | bytes) -> Path:
    folder = root / ".github" / "workflows"
    folder.mkdir(parents=True, exist_ok=True)
    path = folder / name
    if isinstance(content, bytes):
        path.write_bytes(content)
    else:
        path.write_text(textwrap.dedent(content), encoding="utf-8", newline="\n")
    return path


def labels(check: SetupCheck) -> list[str]:
    """``level:kind`` per finding: ``target``, ``create``, the events of a secrets one, or ``not checked``."""
    found = []
    for finding in check.findings:
        events = re.search(r" runs on (push|pull_request|push and pull_request) for healer", finding.message)
        if "runs on pull_request_target" in finding.message:
            kind = "target"
        elif "runs on create" in finding.message:
            kind = "create"
        elif events:
            kind = events.group(1)
        else:
            assert finding.message.startswith("Some workflow files were not checked: ")
            kind = "not checked"
        found.append(f"{finding.level}:{kind}")
    return found


SKIP_PULL_REQUEST = "${{ !startsWith(github.head_ref, 'healer/') }}"
SKIP_BRANCH = "${{ !startsWith(github.ref, 'refs/heads/healer/') }}"


def target(
    *,
    trigger: str = "pull_request_target:",
    condition: str | None = None,
    head: str = "${{ github.event.pull_request.head.sha }}",
) -> str:
    condition_line = f"\n    if: {condition}" if condition else ""
    return (
        f"on:\n  {trigger}\njobs:\n  test:{condition_line}\n    runs-on: ubuntu-latest\n    steps:\n"
        f"      - uses: actions/checkout@v4\n        with:\n          ref: {head}\n      - run: make test\n"
    )


def create(step: str = "run: make release", condition: str | None = None) -> str:
    condition_line = f"\n    if: {condition}" if condition else ""
    job = f"  release:{condition_line}\n    runs-on: ubuntu-latest\n    steps:\n      - {step}\n"
    return f"on: create\njobs:\n{job}"


def on_event(trigger: str, *, condition: str | None = None, secret: bool = True) -> str:
    condition_line = f"\n    if: {condition}" if condition else ""
    env = "\n        env:\n          TOKEN: ${{ secrets.DEPLOY_TOKEN }}" if secret else ""
    job = (
        f"  deploy:{condition_line}\n    runs-on: ubuntu-latest\n    steps:\n      - run: make deploy{env}\n"
    )
    return f"on:\n{trigger}\njobs:\n{job}"


WORKFLOWS = [
    pytest.param(target(), True, ["error:target"], id="target-head-sha"),
    pytest.param(target(head="${{ github.head_ref }}"), True, ["error:target"], id="target-head-ref"),
    pytest.param(
        target(head="${{ github.event.pull_request.head.ref }}"), True, ["error:target"], id="target-ref"
    ),
    pytest.param(target(head='"$GITHUB_HEAD_REF"'), True, ["error:target"], id="target-env"),
    pytest.param(
        target(head="refs/pull/${{ github.event.number }}/merge"),
        True,
        ["error:target"],
        id="target-pull-ref",
    ),
    pytest.param(
        target(head="${{ github.event.pull_request.merge_commit_sha }}"),
        True,
        ["error:target"],
        id="target-merge",
    ),
    pytest.param(target(head="main"), True, [], id="target-base-code"),
    pytest.param(
        target(trigger="pull_request_target: {types: [opened]}"), False, ["error:target"], id="target-opened"
    ),
    # review requests (CODEOWNERS asks for them when the pull request opens), assignments and edits
    # can happen before the review without anyone choosing to run the workflow
    pytest.param(
        target(trigger="pull_request_target: {types: [review_requested]}"),
        True,
        ["error:target"],
        id="target-review-requested",
    ),
    pytest.param(
        target(trigger="pull_request_target: {types: [labeled, assigned]}"),
        True,
        ["error:target"],
        id="target-assigned",
    ),
    # a label (a bot can add it before the review) or closing the pull request (a reviewer
    # rejects it so): an error too
    pytest.param(
        target(trigger="pull_request_target: {types: [closed, labeled]}"),
        True,
        ["error:target"],
        id="target-closed",
    ),
    pytest.param(
        target(trigger="pull_request_target: {types: labeled}"), True, ["error:target"], id="target-labeled"
    ),
    pytest.param(
        target(head="${{ github.event.pull_request.head.repo.full_name }}"),
        True,
        ["error:target"],
        id="target-head-repo",
    ),
    pytest.param(
        target(head="x\n      - run: gh pr checkout ${{ github.event.pull_request.number }}"),
        True,
        ["error:target"],
        id="target-gh-checkout",
    ),
    pytest.param(
        target(head="x\n      - run: git fetch origin pull/${{ github.event.number }}/head:pr"),
        True,
        ["error:target"],
        id="target-pull-head",
    ),
    pytest.param(
        target(head="x\n      - run: git fetch origin pull/$PR/merge"),
        True,
        ["error:target"],
        id="target-pull-merge",
    ),
    pytest.param(
        target(head="x\n      - run: echo ${{ github.event.number }} pulled"),
        True,
        [],
        id="target-number-only",
    ),
    pytest.param(
        target(trigger="pull_request_target: {branches: ['release/**']}"), True, [], id="target-other-base"
    ),
    pytest.param(
        target(trigger="pull_request_target: {branches-ignore: [main]}"), True, [], id="target-base-ignored"
    ),
    pytest.param(
        target(trigger="pull_request_target: {branches-ignore: ['healer/**']}"),
        True,
        ["error:target"],
        id="target-ignore-healer",
    ),
    pytest.param(target(condition=SKIP_PULL_REQUEST), True, [], id="target-skipped"),
    pytest.param(
        target(condition="'!startsWith( github.head_ref , \"healer/\" )'"), True, [], id="target-bare"
    ),
    pytest.param(
        target(condition=SKIP_PULL_REQUEST.replace("}}", "|| true }}")),
        True,
        ["error:target"],
        id="target-skip-or",
    ),
    pytest.param(target(condition=SKIP_BRANCH), True, ["error:target"], id="target-wrong-skip"),
    # the documented form must be the whole condition or one of its top-level && conditions
    pytest.param(
        target(condition="${{ github.actor != 'renovate' && !startsWith(github.head_ref, 'healer/') }}"),
        True,
        [],
        id="target-skip-and",
    ),
    pytest.param(
        target(condition="${{ (github.actor != 'x' || true) && (!startsWith(github.head_ref, 'healer/')) }}"),
        True,
        [],
        id="target-skip-and-or",
    ),
    pytest.param(
        target(condition="${{ ((github.actor != 'x') && (!startsWith(github.head_ref, 'healer/'))) }}"),
        True,
        [],
        id="target-skip-nested",
    ),
    # && binds more tightly than ||: (a || b && skip) runs whenever a is true
    pytest.param(
        target(
            condition="${{ (github.actor == 'x' || github.actor != 'y' && " + SKIP_PULL_REQUEST[4:-3] + ") }}"
        ),
        True,
        ["error:target"],
        id="target-skip-in-or",
    ),
    pytest.param(
        target(
            condition="${{ (github.actor == 'x' || github.actor != 'y' && "
            + SKIP_PULL_REQUEST[4:-3]
            + ") && github.event.action != 'closed' }}"
        ),
        True,
        ["error:target"],
        id="target-skip-in-or-and",
    ),
    pytest.param(
        target(condition="${{ !(!startsWith(github.head_ref, 'healer/')) }}"),
        True,
        ["error:target"],
        id="target-double-negation",
    ),
    pytest.param(
        target(condition="${{ !startsWith(github.head_ref, 'healer/') == false }}"),
        True,
        ["error:target"],
        id="target-compared",
    ),
    pytest.param(
        target(condition="${{ !startsWith(github.head_ref, 'healer/') }} && true"),
        True,
        ["error:target"],
        id="target-text-around",
    ),
    pytest.param(
        target(condition="${{ !startsWith(github.head_ref, 'it''s && healer/') }}"),
        True,
        ["error:target"],
        id="target-quoted-and",
    ),
    pytest.param(
        "on: pull_request_target\nenv:\n  REF: ${{ github.event.pull_request.head.ref }}\n"
        "jobs:\n  a:\n    steps: [{run: make}]\n",
        True,
        ["error:target"],
        id="target-rest",
    ),
    pytest.param(
        "on: pull_request_target\nenv:\n  REF: ${{ github.head_ref }}\n"
        f"jobs:\n  a:\n    if: {SKIP_PULL_REQUEST}\n    steps: [{{run: make}}]\n",
        True,
        [],
        id="target-rest-skipped",
    ),
    pytest.param(create(), True, ["warn:create"], id="create"),
    pytest.param(create("run: deploy ${{ secrets.DEPLOY_KEY }}"), True, ["error:create"], id="create-secret"),
    pytest.param(
        create("run: echo ${{ secrets['NPM_TOKEN'] }}"), True, ["error:create"], id="create-bracket"
    ),
    pytest.param(create("run: echo '${{ toJSON(secrets) }}'"), True, ["error:create"], id="create-tojson"),
    pytest.param(
        create("run: gh release create ${{ secrets.GITHUB_TOKEN }}"), True, ["warn:create"], id="create-token"
    ),
    pytest.param(create("run: cat secrets.txt"), True, ["warn:create"], id="create-plain-word"),
    pytest.param(create(condition="secrets.DEPLOY_KEY != ''"), True, ["error:create"], id="create-if-secret"),
    pytest.param(
        "on: [create]\njobs:\n  call:\n    uses: ./.github/workflows/deploy.yml\n    secrets: inherit\n",
        True,
        ["error:create"],
        id="create-inherit",
    ),
    pytest.param(create(condition="startsWith(github.ref, 'refs/tags/')"), True, [], id="create-tags"),
    pytest.param(create(condition="${{ github.ref_type == 'tag' }}"), True, [], id="create-ref-type"),
    pytest.param(
        create("run: deploy ${{ secrets.DEPLOY_KEY }}", condition='github.event.ref_type == "tag"'),
        True,
        [],
        id="create-event-ref-type",
    ),
    pytest.param(
        create(condition="github.event.ref_type == 'branch'"), True, ["warn:create"], id="create-event-branch"
    ),
    pytest.param(
        create(condition="${{ !startsWith(github.ref, 'refs/tags/') }}"),
        True,
        ["warn:create"],
        id="create-not-tags",
    ),
    pytest.param(
        create(condition="\"!(github.ref_type == 'tag')\""), True, ["warn:create"], id="create-not-tag"
    ),
    pytest.param(
        create(condition="startsWith(github.ref, 'refs/tags/') == false"),
        True,
        ["warn:create"],
        id="create-tags-compared",
    ),
    pytest.param(create(condition=SKIP_BRANCH), True, [], id="create-skipped"),
    pytest.param(create(condition=SKIP_PULL_REQUEST), True, ["warn:create"], id="create-wrong-skip"),
    # write permissions count like secrets: OIDC cloud credentials, a GITHUB_TOKEN that can push
    pytest.param(
        "on: create\npermissions:\n  id-token: write\n  contents: read\n"
        "jobs:\n  a:\n    steps: [{run: make deploy}]\n",
        True,
        ["error:create"],
        id="create-oidc",
    ),
    pytest.param(
        "on: create\njobs:\n  a:\n    permissions: write-all\n    steps: [{run: make}]\n",
        True,
        ["error:create"],
        id="create-write-all",
    ),
    pytest.param(
        "on: create\njobs:\n  a:\n    permissions: {contents: write}\n    steps: [{run: make}]\n",
        True,
        ["error:create"],
        id="create-contents-write",
    ),
    pytest.param(
        "on: create\npermissions: read-all\njobs:\n  a:\n    permissions: {contents: read}\n"
        "    steps: [{run: make}]\n",
        True,
        ["warn:create"],
        id="create-read-only",
    ),
    pytest.param(
        "on: create\nx-defaults: &defaults\n  env:\n    KEY: ${{ secrets.DEPLOY_KEY }}\n"
        "jobs:\n  a:\n    <<: *defaults\n    steps: [{run: make}]\n",
        True,
        ["error:create"],
        id="create-anchor",
    ),
    # a node that an alias shares is read again where its key changes what it means
    pytest.param(
        "on: create\njobs:\n  a:\n    permissions: &p {contents: write}\n    env: *p\n"
        "    steps: [{run: make}]\n",
        True,
        ["error:create"],
        id="create-permissions-alias",
    ),
    pytest.param(
        "on: create\njobs:\n  a:\n    env: {CHECK: &c \"secrets.DEPLOY_KEY != ''\"}\n    if: *c\n"
        "    steps: [{run: make}]\n",
        True,
        ["error:create"],
        id="create-if-alias",
    ),
    pytest.param(on_event("  push:"), False, ["warn:push"], id="push"),
    pytest.param(on_event("  push:"), True, [], id="push-skip-ci"),
    pytest.param(on_event("  push:", secret=False), False, [], id="push-no-secrets"),
    pytest.param(
        on_event("  push:", secret=False).replace("jobs:", "permissions: {id-token: write}\njobs:"),
        False,
        ["warn:push"],
        id="push-oidc",
    ),
    pytest.param(on_event("  push: {branches-ignore: ['healer/**']}"), False, [], id="push-ignored"),
    pytest.param(on_event("  push: {branches: [main]}"), False, [], id="push-main"),
    pytest.param(on_event("  push: {tags: ['v*']}"), False, [], id="push-tags"),
    pytest.param(on_event("  push: {paths: ['src/**']}"), False, ["warn:push"], id="push-paths"),
    pytest.param(on_event("  push:", condition=SKIP_BRANCH), False, [], id="push-skipped"),
    # only create recognises a tags-only job: a push workflow filters tags with on.push.tags
    pytest.param(
        on_event("  push:", condition="startsWith(github.ref, 'refs/tags/')"),
        False,
        ["warn:push"],
        id="push-tags-job",
    ),
    pytest.param(
        on_event("  pull_request: {branches-ignore: ['healer/**']}"),
        False,
        ["warn:pull_request"],
        id="pr-ignore-healer",
    ),
    pytest.param(on_event("  pull_request: {branches: [release]}"), False, [], id="pr-other-base"),
    pytest.param(on_event("  pull_request:", condition=SKIP_PULL_REQUEST), False, [], id="pr-skipped"),
    pytest.param(
        on_event("  pull_request:", condition=SKIP_BRANCH), False, ["warn:pull_request"], id="pr-wrong-skip"
    ),
    pytest.param(
        on_event("  push:\n  pull_request:"), False, ["warn:push and pull_request"], id="push-and-pr"
    ),
    pytest.param(
        on_event("  [push, pull_request]").replace("on:\n  [", "on: ["),
        False,
        ["warn:push and pull_request"],
        id="list",
    ),
    pytest.param("name: notes\njobs: {}\n", True, [], id="no-on"),
    pytest.param(
        "on: workflow_dispatch\njobs:\n  a:\n    steps: [{run: '${{ secrets.X }}'}]\n",
        False,
        [],
        id="dispatch",
    ),
]


@pytest.mark.parametrize(("text", "skip_ci", "expected"), WORKFLOWS)
def test_the_workflows_that_skip_ci_does_not_stop(
    tmp_path: Path, text: str, skip_ci: bool, expected: list[str]
) -> None:
    write_workflow(tmp_path, "ci.yml", text)
    found = check_workflows(tmp_path, skip_ci=skip_ci, base="main")
    assert labels(found) == expected
    assert found.summary == "1 workflow file checked"


def test_the_workflow_finding_texts(tmp_path: Path) -> None:
    write_workflow(tmp_path, "a-target.yml", target())
    write_workflow(tmp_path, "b-create.yaml", create("run: deploy ${{ secrets.DEPLOY_KEY }}"))
    write_workflow(tmp_path, "c-create.yml", create())
    write_workflow(tmp_path, "d-push.yml", on_event("  push:\n  pull_request:"))
    found = check_workflows(tmp_path, skip_ci=False, base="main")
    assert found.summary == "4 workflow files checked"
    assert texts(found) == [
        (
            ERROR,
            "The workflow .github/workflows/a-target.yml runs on pull_request_target and refers to the pull "
            "request's head: it can run the healer branch's code with the repository's secrets, whatever "
            "the commit message says. Never check out or run the pull request's head in a "
            "pull_request_target workflow, or skip its jobs for healer branches with if: "
            "${{ !startsWith(github.head_ref, 'healer/') }}.",
        ),
        (
            ERROR,
            "The workflow .github/workflows/b-create.yaml runs on create, which [skip ci] does not stop: it "
            "starts when dag-healer pushes its branch, with that branch's code checked out by default. It "
            "uses the repository's secrets or write permissions. Skip its jobs for healer branches with if: "
            "${{ !startsWith(github.ref, 'refs/heads/healer/') }}, or run it for tags only.",
        ),
        (
            WARN,
            "The workflow .github/workflows/c-create.yml runs on create, which [skip ci] does not stop: it "
            "starts when dag-healer pushes its branch, with that branch's code checked out by default. Skip "
            "its jobs for healer branches with if: ${{ !startsWith(github.ref, 'refs/heads/healer/') }}, or "
            "run it for tags only.",
        ),
        (
            WARN,
            "The workflow .github/workflows/d-push.yml runs on push and pull_request for healer branches and "
            "uses the repository's secrets or write permissions, and github.skip_ci is false: CI runs the "
            "model-written test and fix with them before review. Set github.skip_ci to true, or skip the "
            "workflow for healer branches: branches-ignore: ['healer/**'] under push, and if: "
            "${{ !startsWith(github.head_ref, 'healer/') }} on its jobs for pull_request (branches-ignore "
            "under pull_request filters the base branch, not the healer branch).",
        ),
    ]


def test_a_pull_request_target_workflow_on_labels_only_is_an_error_too(tmp_path: Path) -> None:
    write_workflow(tmp_path, "label.yml", target(trigger="pull_request_target: {types: [labeled, closed]}"))
    write_workflow(tmp_path, "review.yml", target(trigger="pull_request_target: {types: [review_requested]}"))
    found = check_workflows(tmp_path, skip_ci=True, base="main")
    fix = (
        "Never check out or run the pull request's head in a pull_request_target workflow, or skip its jobs "
        "for healer branches with if: ${{ !startsWith(github.head_ref, 'healer/') }}."
    )
    assert texts(found) == [
        (
            ERROR,
            "The workflow .github/workflows/label.yml runs on pull_request_target (closed, labeled) and "
            "refers to the pull request's head: when the pull request is closed or a label is added, it runs "
            "the healer branch's code with the repository's secrets, whether or not a person has approved "
            f"the change. {fix}",
        ),
        (
            ERROR,
            "The workflow .github/workflows/review.yml runs on pull_request_target (review_requested) and "
            "refers to the pull request's head: it can run the healer branch's code with the repository's "
            f"secrets, whatever the commit message says. {fix}",
        ),
    ]


def test_a_called_workflow_counts_for_the_job_that_calls_it(tmp_path: Path) -> None:
    caller = (
        "on: pull_request_target\njobs:\n  call:\n    uses: ./.github/workflows/build.yml\n"
        "    secrets: inherit\n"
    )
    callee = (
        "on: workflow_call\njobs:\n  build:\n    steps:\n      - uses: actions/checkout@v4\n"
        "        with: {ref: '${{ github.event.pull_request.head.sha }}'}\n"
    )
    write_workflow(tmp_path, "caller.yml", caller)
    write_workflow(tmp_path, "build.yml", callee)
    found = check_workflows(tmp_path, skip_ci=True, base="main")
    assert labels(found) == ["error:target"]
    assert ".github/workflows/caller.yml runs on pull_request_target" in found.findings[0].message
    assert found.summary == "2 workflow files checked"
    # through two levels, and a create workflow whose callee uses OIDC
    write_workflow(tmp_path, "caller.yml", caller.replace("build.yml", "outer.yml"))
    write_workflow(
        tmp_path, "outer.yml", "on: workflow_call\njobs:\n  o:\n    uses: ./.github/workflows/build.yml\n"
    )
    write_workflow(
        tmp_path,
        "release.yml",
        "on: create\njobs:\n  r:\n    uses: ./.github/workflows/deploy.yml\n",
    )
    write_workflow(
        tmp_path,
        "deploy.yml",
        "on: workflow_call\npermissions: {id-token: write}\njobs:\n  d:\n    steps: [{run: x}]\n",
    )
    assert labels(check_workflows(tmp_path, skip_ci=True, base="main")) == ["error:target", "error:create"]
    # a job condition on the calling job still skips it
    write_workflow(
        tmp_path, "caller.yml", caller.replace("    uses:", f"    if: {SKIP_PULL_REQUEST}\n    uses:")
    )
    assert labels(check_workflows(tmp_path, skip_ci=True, base="main")) == ["error:create"]


def test_workflows_that_call_each_other_are_read_once(tmp_path: Path) -> None:
    for name, other in (("a.yml", "b.yml"), ("b.yml", "a.yml")):
        write_workflow(
            tmp_path,
            name,
            f"on: [pull_request_target, workflow_call]\njobs:\n  j:\n    uses: ./.github/workflows/{other}\n",
        )
    found = check_workflows(tmp_path, skip_ci=True, base="main")
    assert labels(found) == [] and found.summary == "2 workflow files checked"


@pytest.mark.parametrize(
    ("text", "label"),
    [
        pytest.param("on:\n  pull_request_target:\n", "on-key", id="on-key"),
        pytest.param('"on":\n  pull_request_target:\n', "quoted", id="quoted"),
        pytest.param("on: pull_request_target\n", "string", id="string"),
        pytest.param("on: [push, pull_request_target]\n", "list", id="list"),
        pytest.param("on: {pull_request_target: null}\n", "flow", id="flow"),
    ],
)
def test_on_in_every_form(tmp_path: Path, text: str, label: str) -> None:
    jobs = "jobs:\n  t:\n    steps:\n      - run: git checkout ${{ github.head_ref }}\n"
    write_workflow(tmp_path, f"{label}.yml", text + jobs)
    assert labels(check_workflows(tmp_path, skip_ci=True, base="main")) == ["error:target"]


@pytest.mark.parametrize(
    ("event", "runs"),
    [
        pytest.param({"branches": ["healer/**"]}, True, id="globstar"),
        pytest.param({"branches": ["healer/*"]}, True, id="star"),
        pytest.param({"branches": ["healer"]}, False, id="exact"),
        pytest.param({"branches": "healer/**"}, True, id="string"),
        pytest.param({"branches": ["main", "release/*"]}, False, id="others"),
        pytest.param({"branches": ["**", "!healer/**"]}, False, id="negated"),
        pytest.param({"branches": ["**", "!healer/**", "healer/customers_*"]}, True, id="negated-back"),
        pytest.param({"branches": ["heal?er/**"]}, True, id="optional"),
        pytest.param({"branches": ["he+aler/**"]}, True, id="repeated"),
        pytest.param({"branches": ["[gh]ealer/**"]}, True, id="class"),
        pytest.param({"branches": ["[a-f]ealer/**"]}, False, id="range"),
        pytest.param({"branches": ["[z-a]ealer/**"]}, False, id="invalid"),
        pytest.param({"branches": ["healer\\/customers_sync-*"]}, True, id="escaped"),
        pytest.param({"branches": []}, True, id="empty"),
        pytest.param({"branches-ignore": ["healer/**"]}, False, id="ignored"),
        pytest.param({"branches-ignore": ["feature/**"]}, True, id="not-ignored"),
        pytest.param({"branches-ignore": ["*"]}, True, id="star-no-slash"),
        pytest.param({"branches-ignore": ["**"]}, False, id="ignore-all"),
        pytest.param({"tags": ["v*"], "branches": ["**"]}, True, id="tags-and-branches"),
        pytest.param({"tags-ignore": ["v*"]}, False, id="tags-ignore-only"),
    ],
)
def test_push_filters_follow_github_patterns(tmp_path: Path, event: dict[str, Any], runs: bool) -> None:
    document = {
        "on": {"push": event},
        "jobs": {"deploy": {"steps": [{"run": "make deploy", "env": {"KEY": "${{ secrets.DEPLOY_KEY }}"}}]}},
    }
    write_workflow(tmp_path, "deploy.yml", json.dumps(document))  # JSON is YAML; "on" quoted stays a string
    found = check_workflows(tmp_path, skip_ci=False, base="main", branch=HEALER_BRANCH)
    assert labels(found) == (["warn:push"] if runs else [])


def test_push_filters_use_an_example_branch_until_the_branch_is_known(tmp_path: Path) -> None:
    document = {
        "on": {"push": {"branches": ["healer/dag-*"]}},
        "jobs": {"a": {"env": {"K": "${{ secrets.K }}"}}},
    }
    write_workflow(tmp_path, "deploy.yml", json.dumps(document))
    assert labels(check_workflows(tmp_path, skip_ci=False, base="main")) == ["warn:push"]
    assert labels(check_workflows(tmp_path, skip_ci=False, base="main", branch=HEALER_BRANCH)) == []


def test_files_that_cannot_be_checked_are_listed_in_one_warning(tmp_path: Path) -> None:
    write_workflow(tmp_path, "big.yml", "on: create\n" + "#" * MAX_WORKFLOW_BYTES + "\n")
    write_workflow(tmp_path, "bom.yml", b"\xef\xbb\xbfon: create\njobs:\n  a:\n    steps: [{run: make}]\n")
    write_workflow(tmp_path, "broken.yml", "on: [create\n")
    write_workflow(tmp_path, "empty.yml", "")
    write_workflow(tmp_path, "latin1.yml", b"on: create\nname: caf\xe9\n")
    write_workflow(tmp_path, "list.yaml", "- on\n- create\n")
    write_workflow(tmp_path, "README.md", "on: create\n")  # not a workflow file name
    (tmp_path / ".github" / "workflows" / "folder.yml").mkdir()
    found = check_workflows(tmp_path, skip_ci=True, base="main")
    assert labels(found) == ["warn:create", "warn:not checked"]
    assert texts(found)[1] == (
        WARN,
        "Some workflow files were not checked: .github/workflows/big.yml (larger than 1 MB), "
        ".github/workflows/broken.yml (not valid YAML), .github/workflows/latin1.yml (not UTF-8), "
        ".github/workflows/list.yaml (not a workflow). Check them by hand: no pull_request_target or "
        "create workflow may run the healer branch's code with the repository's secrets.",
    )
    assert found.summary == "1 workflow file checked"


def test_the_list_of_unchecked_files_is_cut_after_ten(tmp_path: Path) -> None:
    for number in range(12):
        write_workflow(tmp_path, f"broken-{number:02d}.yml", "on: [push\n")
    message = check_workflows(tmp_path, skip_ci=True, base="main").findings[0].message
    assert message.count("(not valid YAML)") == 10
    assert message.endswith(".github/workflows/broken-09.yml (not valid YAML), and 2 more.")


def test_at_most_the_first_files_are_read(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(github_checks, "MAX_WORKFLOW_FILES", 2)
    for name in ("a.yml", "b.yml", "c.yml"):
        write_workflow(tmp_path, name, create())
    found = check_workflows(tmp_path, skip_ci=True, base="main")
    assert labels(found) == ["warn:create", "warn:create", "warn:not checked"]
    assert found.findings[-1].message == (
        "Some workflow files were not checked: .github/workflows (1 file beyond the first 2)."
    )


def symlink(target: Path, path: Path, *, directory: bool = False) -> None:
    """Create a symbolic link, or skip the test where the platform refuses (Windows without the privilege)."""
    try:
        os.symlink(target, path, target_is_directory=directory)
    except (OSError, NotImplementedError):
        pytest.skip("symbolic links are not available here")


def test_a_linked_workflow_file_is_never_read(tmp_path: Path) -> None:
    outside = tmp_path / "outside.yml"
    outside.write_text(target(), encoding="utf-8", newline="\n")
    root = tmp_path / "repo"
    write_workflow(root, "ci.yml", create())
    symlink(outside, root / ".github" / "workflows" / "linked.yml")
    found = check_workflows(root, skip_ci=True, base="main")
    assert labels(found) == ["warn:create", "warn:not checked"]
    assert found.findings[1].message == (
        "Some workflow files were not checked: .github/workflows/linked.yml (a symbolic link)."
    )


@pytest.mark.parametrize("linked", ["github", "workflows"])
def test_a_linked_workflows_folder_is_never_followed(tmp_path: Path, linked: str) -> None:
    outside = tmp_path / "outside"
    write_workflow(outside, "ci.yml", target())
    root = tmp_path / "repo"
    if linked == "github":
        root.mkdir()
        symlink(outside / ".github", root / ".github", directory=True)
    else:
        (root / ".github").mkdir(parents=True)
        symlink(outside / ".github" / "workflows", root / ".github" / "workflows", directory=True)
    found = check_workflows(root, skip_ci=True, base="main")
    assert texts(found) == [
        (
            WARN,
            "Some workflow files were not checked: .github/workflows (a symbolic link). Check them by hand: "
            "no pull_request_target or create workflow may run the healer branch's code with the "
            "repository's secrets.",
        )
    ]


@pytest.mark.skipif(not hasattr(os, "mkfifo"), reason="no FIFOs on this platform")
def test_a_fifo_named_like_a_workflow_is_never_opened(tmp_path: Path) -> None:
    write_workflow(tmp_path, "ci.yml", create())
    os.mkfifo(tmp_path / ".github" / "workflows" / "pipe.yml")
    found = check_workflows(tmp_path, skip_ci=True, base="main")  # would block if it opened the FIFO
    assert labels(found) == ["warn:create"]


def test_yaml_aliases_cannot_make_the_analysis_huge(tmp_path: Path) -> None:
    # nine levels of ten aliases each: a billion strings if expanded, a few nodes as shared objects
    levels = ["l0: &l0 [x, x, x, x, x, x, x, x, x, x]"]
    levels += [f"l{n}: &l{n} [{', '.join([f'*l{n - 1}'] * 10)}]" for n in range(1, 9)]
    bomb = "on: create\n" + "\n".join(levels) + "\njobs:\n  a:\n    steps: [*l8]\n"
    write_workflow(tmp_path, "bomb.yml", bomb)
    write_workflow(
        tmp_path, "loop.yml", "on: create\nloop: &loop [a, *loop]\njobs:\n  a:\n    steps: *loop\n"
    )
    started = time.monotonic()
    found = check_workflows(tmp_path, skip_ci=True, base="main")
    assert time.monotonic() - started < 10
    assert labels(found) == ["warn:create", "warn:create"]


def test_merge_keys_cannot_make_the_loading_huge(tmp_path: Path) -> None:
    # each level merges the previous one ten times: 10**12 entries for twelve levels if loaded
    levels = ["a0: &a0 {x: 1}"] + [
        f"a{n}: &a{n}\n  <<: [{', '.join([f'*a{n - 1}'] * 10)}]" for n in range(1, 13)
    ]
    write_workflow(tmp_path, "merge.yml", "on: create\n" + "\n".join(levels) + "\njobs: {}\n")
    write_workflow(tmp_path, "ok.yml", create())
    started = time.monotonic()
    found = check_workflows(tmp_path, skip_ci=True, base="main")
    assert time.monotonic() - started < 10
    assert labels(found) == ["warn:create", "warn:not checked"]
    assert found.findings[1].message == (
        "Some workflow files were not checked: .github/workflows/merge.yml (too large or complex to analyse)."
    )


def test_merge_keys_are_merged_as_pyyaml_does() -> None:
    text = (
        "a: &a {x: 1, y: 1}\nb: &b {x: 2, z: 2}\n"
        "c: {<<: [*a, *b], y: 3}\n"  # the first merged mapping wins, the mapping's own keys win over both
        "d: {<<: *a, <<: *b}\n"  # of two merge keys, the later one wins
        "e: {<<: {<<: *a, w: 4}}\n"
    )
    assert yaml.load(text, Loader=github_checks._WorkflowLoader) == {
        "a": {"x": "1", "y": "1"},
        "b": {"x": "2", "z": "2"},
        "c": {"x": "1", "y": "3", "z": "2"},
        "d": {"x": "2", "y": "1", "z": "2"},
        "e": {"x": "1", "y": "1", "w": "4"},
    }


def test_numbers_and_dates_are_loaded_as_text() -> None:
    """The analysis never reads them, and PyYAML builds some in quadratic time; on is still True."""
    text = (
        "on: create\nx: 1.5\nd: 2001-12-14\nh: 0x1f\ns: 1:20\nn: ~\nb: yes\n"
        f"k: {{0: a, {2**61 - 1}: b, {2 * (2**61 - 1)}: c}}\n"  # integer keys whose hashes are all 0
    )
    assert yaml.load(text, Loader=github_checks._WorkflowLoader) == {
        True: "create",
        "x": "1.5",
        "d": "2001-12-14",
        "h": "0x1f",
        "s": "1:20",
        "n": None,
        "b": True,
        "k": {"0": "a", str(2**61 - 1): "b", str(2 * (2**61 - 1)): "c"},
    }


def test_numbers_that_pyyaml_builds_slowly_are_loaded_quickly(tmp_path: Path) -> None:
    # YAML 1.1 reads 1:9:9:... as a number in base 60: PyYAML took 6 seconds for 320 KB of it
    # (68 seconds for 1 MB), and integer keys whose hashes collide fill a dict in quadratic time
    write_workflow(tmp_path, "sexagesimal.yml", "on: create\nx: 1" + ":9" * 160_000 + "\n")
    write_workflow(tmp_path, "tagged.yml", "on: create\nx: !!int '1" + ":9" * 160_000 + "'\n")
    keys = ", ".join(f"{n * (2**61 - 1)}: x" for n in range(12_000))  # every hash is 0
    write_workflow(
        tmp_path, "keys.yml", f"on: create\nx: {{{keys}}}\njobs:\n  a:\n    steps: [{{run: make}}]\n"
    )
    started = time.monotonic()
    found = check_workflows(tmp_path, skip_ci=True, base="main")
    assert time.monotonic() - started < QUICKLY
    assert labels(found) == ["warn:create", "warn:not checked"]  # keys.yml is checked
    assert found.findings[1].message == too_complex("sexagesimal.yml", "tagged.yml")  # 320 KB of text


def test_the_loading_of_a_file_has_a_budget_and_a_deadline(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """The loading counts every mapping entry, merged or not, and stops at its own deadline."""
    entries = "".join(f"  k{n}: v\n" for n in range(1500))
    path = write_workflow(tmp_path, "plain.yml", f"on: create\nx:\n{entries}")
    monkeypatch.setattr(github_checks, "MAX_WORKFLOW_NODES", 1000)
    assert github_checks._load_workflow(os.fspath(path))[1] == "too large or complex to analyse"
    monkeypatch.setattr(github_checks, "MAX_WORKFLOW_NODES", 100_000)
    assert github_checks._load_workflow(os.fspath(path))[1] is None
    monkeypatch.setattr(github_checks, "MAX_WORKFLOW_SECONDS", -1.0)  # over before it starts
    assert github_checks._load_workflow(os.fspath(path))[1] == "too large or complex to analyse"


def test_a_mapping_that_merges_itself_is_not_valid_yaml(tmp_path: Path) -> None:
    write_workflow(tmp_path, "self.yml", "on: create\nx: &x {<<: *x, k: v}\njobs: {}\n")
    message = check_workflows(tmp_path, skip_ci=True, base="main").findings[0].message
    assert message == "Some workflow files were not checked: .github/workflows/self.yml (not valid YAML)."


def test_the_workflow_files_read_hold_a_bounded_size(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    limit = 2 * len(create()) + 10
    monkeypatch.setattr(github_checks, "MAX_WORKFLOW_TOTAL_BYTES", limit)
    for name in ("a.yml", "b.yml", "c.yml", "d.yml"):
        write_workflow(tmp_path, name, create())
    found = check_workflows(tmp_path, skip_ci=True, base="main")
    assert labels(found) == ["warn:create", "warn:create", "warn:not checked"]
    why = f"not read: more than {limit} bytes of workflow files"
    assert found.findings[-1].message == (
        f"Some workflow files were not checked: .github/workflows/c.yml ({why}), "
        f".github/workflows/d.yml ({why})."
    )
    assert found.summary == "2 workflow files checked"
    assert github_checks._size_text(2_000_000) == "2 MB"


def test_a_workflow_too_large_to_analyse_is_not_checked(tmp_path: Path) -> None:
    steps = ", ".join(f"step{n}" for n in range(1000))
    jobs = "\n".join(f"  job{n}: {{steps: *steps}}" for n in range(200))  # 200 jobs x 1000 shared steps
    write_workflow(tmp_path, "huge.yml", f"on: create\nx-steps: &steps [{steps}]\njobs:\n{jobs}\n")
    write_workflow(tmp_path, "deep.yml", "on: create\nx: " + "[" * 5000 + "]" * 5000 + "\n")
    found = check_workflows(tmp_path, skip_ci=True, base="main")
    assert found.findings[0].message == (
        "Some workflow files were not checked: .github/workflows/deep.yml (not valid YAML), "
        ".github/workflows/huge.yml (too large or complex to analyse)."
    )


#: Seconds for work of a few milliseconds: generous, so that slow CI runners stay green.
QUICKLY = 5.0


def too_complex(*names: str) -> str:
    """The "not checked" message for workflow files too large or complex to analyse."""
    listed = ", ".join(f".github/workflows/{name} (too large or complex to analyse)" for name in names)
    return f"Some workflow files were not checked: {listed}."


def test_a_long_scalar_aliased_many_times_is_not_checked_quickly(tmp_path: Path) -> None:
    # a scalar of 250 KB aliased 3,000 times: it took 22 seconds to read it each time, then said OK
    run = "echo " + "x" * 250_000
    steps = ", ".join(["{run: *run}"] * 3000)
    write_workflow(
        tmp_path, "aliased.yml", f"on: create\nx-run: &run '{run}'\njobs:\n  a:\n    steps: [{steps}]\n"
    )
    started = time.monotonic()
    found = check_workflows(tmp_path, skip_ci=True, base="main")
    assert time.monotonic() - started < QUICKLY
    assert [finding.message for finding in found.findings] == [too_complex("aliased.yml")]
    assert found.summary == "0 workflow files checked"


def test_a_string_shared_by_jobs_is_read_once_by_each_job(tmp_path: Path) -> None:
    # 20 KB: read once in one job, read again by each of 3,000 jobs, which is too much for one file
    run = "echo " + "x" * 20_000
    steps = ", ".join(["{run: *run}"] * 3000)
    write_workflow(
        tmp_path, "one-job.yml", f"on: create\nx-run: &run '{run}'\njobs:\n  a:\n    steps: [{steps}]\n"
    )
    jobs = "\n".join(f"  j{n}: {{steps: [{{run: *run}}]}}" for n in range(3000))
    write_workflow(tmp_path, "many-jobs.yml", f"on: create\nx-run: &run '{run}'\njobs:\n{jobs}\n")
    started = time.monotonic()
    found = check_workflows(tmp_path, skip_ci=True, base="main")
    assert time.monotonic() - started < QUICKLY
    assert labels(found) == ["warn:create", "warn:not checked"]
    assert found.findings[1].message == too_complex("many-jobs.yml")


def test_long_conditions_shared_by_jobs_are_not_checked_quickly(tmp_path: Path) -> None:
    # every event parses the condition of every job again: 40 jobs, 4 events, 1,800 characters each
    condition = "${{ " + "(" * 900 + "github.actor != 'renovate'" + ")" * 900 + " }}"
    jobs = "\n".join(f"  j{n}: {{if: *c, steps: [{{run: make}}]}}" for n in range(40))
    events = "[create, push, pull_request, pull_request_target]"
    write_workflow(tmp_path, "conditions.yml", f'on: {events}\nx-if: &c "{condition}"\njobs:\n{jobs}\n')
    started = time.monotonic()
    found = check_workflows(tmp_path, skip_ci=False, base="main")
    assert time.monotonic() - started < QUICKLY
    assert [finding.message for finding in found.findings] == [too_complex("conditions.yml")]


def test_the_head_reference_pattern_takes_linear_time() -> None:
    # an unbounded [^}]* read the rest of the string again after each "pull/${{": 30 seconds for 1 MB
    started = time.monotonic()
    assert github_checks._HEAD_REFERENCE.search("pull/${{" * 125_000) is None
    assert time.monotonic() - started < QUICKLY
    assert github_checks._HEAD_REFERENCE.search("git fetch origin pull/${{ github.event.number }}/head:pr")


def test_a_branch_pattern_costs_the_states_it_keeps_alive(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    # "**" 500 times keeps 501 states alive for each of the 36 characters of the branch name
    monkeypatch.setattr(github_checks, "MAX_WORKFLOW_NODES", 20_000)
    document = {"on": {"push": {"branches": ["**" * 500] * 2}}, "jobs": {"a": {"steps": [{"run": "make"}]}}}
    write_workflow(tmp_path, "push.yml", json.dumps(document))
    found = check_workflows(tmp_path, skip_ci=False, base="main", branch=HEALER_BRANCH)
    assert [finding.message for finding in found.findings] == [too_complex("push.yml")]
    document["on"] = {"push": {"branches": ["healer/**"] * 2}}
    write_workflow(tmp_path, "push.yml", json.dumps(document))
    assert check_workflows(tmp_path, skip_ci=False, base="main", branch=HEALER_BRANCH).findings == ()


def test_aliased_event_names_and_types_are_read_each_time(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setattr(github_checks, "MAX_WORKFLOW_NODES", 5_000)
    name = "x" * 1000
    aliases = ", ".join(["*n"] * 10)  # 10,000 characters to read, from 1,000 in the file
    jobs = "jobs:\n  a:\n    steps: [{run: 'git checkout ${{ github.head_ref }}'}]\n"
    write_workflow(tmp_path, "names.yml", f"x-n: &n {name}\non: [create, {aliases}]\n{jobs}")
    write_workflow(tmp_path, "once.yml", f"x-n: &n {name}\non: [create, *n]\n{jobs}")
    write_workflow(
        tmp_path, "types.yml", f"x-n: &n {name}\non:\n  pull_request_target:\n    types: [{aliases}]\n{jobs}"
    )
    found = check_workflows(tmp_path, skip_ci=True, base="main")
    assert labels(found) == ["warn:create", "warn:not checked"]
    assert found.findings[1].message == too_complex("names.yml", "types.yml")


def test_the_analysis_of_a_file_stops_at_its_deadline(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setattr(github_checks, "MAX_WORKFLOW_SECONDS", -1.0)  # over before it starts
    write_workflow(tmp_path, "ci.yml", create())
    found = check_workflows(tmp_path, skip_ci=True, base="main")
    assert texts(found) == [
        (
            WARN,
            too_complex("ci.yml")
            + " Check them by hand: no pull_request_target or create workflow may run the "
            "healer branch's code with the repository's secrets.",
        )
    ]
    assert found.summary == "0 workflow files checked"


def test_no_workflows_folder_is_fine(tmp_path: Path) -> None:
    found = check_workflows(tmp_path, skip_ci=True, base="main")
    assert (found.name, found.level, found.summary) == (
        WORKFLOWS_CHECK,
        OK,
        "no workflow files in .github/workflows",
    )
    write_workflow(tmp_path, "notes.txt", "on: create\n")
    assert (
        check_workflows(tmp_path, skip_ci=False, base="main").summary
        == "no workflow files in .github/workflows"
    )


def test_workflow_file_names_are_shown_on_one_line(tmp_path: Path) -> None:
    if os.name == "nt":
        pytest.skip("Windows file names cannot hold control characters")
    write_workflow(tmp_path, "x\n[FAIL] github\x1b.yml", create())
    message = check_workflows(tmp_path, skip_ci=True, base="main").findings[0].message
    assert message.startswith("The workflow .github/workflows/x [FAIL] github?.yml runs on create")


def test_activity_types_are_shown_on_one_line(tmp_path: Path) -> None:
    write_workflow(
        tmp_path, "prt.yml", target(trigger='pull_request_target: {types: ["opened\\n[ OK ] github"]}')
    )
    message = check_workflows(tmp_path, skip_ci=True, base="main").findings[0].message
    assert message.startswith(
        "The workflow .github/workflows/prt.yml runs on pull_request_target (opened [ ok ] github) and refers"
    )


def test_the_github_setup_scans_the_given_clone(tmp_path: Path) -> None:
    write_workflow(tmp_path, "ci.yml", target())
    checks, _ = run(well_set(), workflows_root=tmp_path)
    assert labels(checks[WORKFLOWS_CHECK]) == ["error:target"]
    assert [finding.check for finding in setup_findings(checks.values())] == [WORKFLOWS_CHECK]
