"""Read-only checks of how a GitHub repository and its token are set up.

dag-healer pushes its ``healer/...`` branch to the repository itself and opens the pull request
from it. What then protects the base branch and the repository's secrets is GitHub's own
configuration, which dag-healer cannot enforce: the token's scope, the base branch's rules and
the repository's workflows. These checks report that configuration. They never change it and
never block a pull request: ``dag-healer doctor`` runs them, and so does a repair right before
it pushes, which lists the findings under "Before you merge" at the top of the pull request.

The API checks send read-only ``GET`` requests to ``api_url`` only, with the token in the
``Authorization`` header (the headers of :mod:`dag_healer.pr.github`), a short timeout and
redirects never followed (the ``301`` of a renamed repository is reported, not followed). The
owner, the name and the branch are percent-encoded in the path, a ``/`` included
(``release/2.0`` is ``release%2F2.0``); links in the answers are never followed. An answer is
read uncompressed and up to :data:`MAX_ANSWER_BYTES` only, each request within twice the
timeout and all of them within six times the timeout:

* ``/repos/{owner}/{name}``: whether the token's account is an admin of the repository and, for
  classic and OAuth tokens, the scopes in the ``X-OAuth-Scopes`` response header;
* ``/installation/repositories?per_page=1``: for a GitHub App installation token only, how many
  repositories the installation can reach;
* ``/repos/{owner}/{name}/rules/branches/{base}``: the rules of every active ruleset that
  applies to the base branch (paginated);
* ``/repos/{owner}/{name}/branches/{base}`` and ``.../branches/{base}/protection``: only when
  the rules leave a requirement unmet, the branch and its classic branch protection;
* ``/repos/{owner}/{name}/rulesets/{id}``: whether the token's account can bypass a ruleset that
  holds one of the required rules (the first :data:`MAX_RULESETS` of them; the others make one
  warning).

The base branch must require a pull request with at least one approving review and block force
pushes and deletion (each missing one is an error); with ``skip_ci`` it should also require
status checks (a warning). :func:`check_workflows` reads ``.github/workflows/*.yml`` and
``*.yaml`` of a local clone, without network, for the workflows ``[skip ci]`` does not stop:
``pull_request_target`` workflows that refer to the pull request's head, ``create`` workflows
and, with ``skip_ci`` false, the ``push`` and ``pull_request`` workflows that run for healer
branches with the repository's secrets or write permissions. YAML aliases and merge keys can
make a small file huge, so the loading and the analysis of each file each have a budget
(:data:`MAX_WORKFLOW_NODES`) and a deadline (:data:`MAX_WORKFLOW_SECONDS`), and numbers and
dates are loaded as the text they are written in (PyYAML builds some of them in quadratic
time); a file past either limit is reported as not checked, never as OK.

Each check runs on its own: one that cannot run, or fails unexpectedly, reports one warning that
says what was not checked, and the others still run. Nothing is reported ok without having been
checked, and :func:`check_github_setup` never raises. No finding, log record or exception text
holds the token or any part of it, only its kind (told from its prefix); GitHub's error texts
are redacted and exceptions are named by their type only.
"""

from __future__ import annotations

import functools
import json
import logging
import os
import re
import stat
import time
from collections.abc import Callable, Iterable, Mapping
from dataclasses import dataclass
from typing import Any
from urllib.parse import quote

import httpx
import yaml

from dag_healer.pr.github import DEFAULT_API_URL, _error_message, _headers

log = logging.getLogger(__name__)

#: Finding levels (the statuses of ``dag-healer doctor``).
ERROR = "error"
WARN = "warn"
OK = "ok"

#: Check names (``dag-healer doctor`` shows them as ``github <name>``).
TOKEN_CHECK = "token"
REPOSITORY_CHECK = "repository"
BASE_BRANCH_CHECK = "base branch"
WORKFLOWS_CHECK = "workflows"
SETUP_CHECK = "setup"

#: Seconds per request operation (connect, read, ...); a whole request gets twice as much, and all
#: the requests of one run six times as much.
CHECK_TIMEOUT = 5.0
#: An answer of the API is read up to this many bytes; a longer one is not understood.
MAX_ANSWER_BYTES = 2_000_000
#: Workflow files larger than this many bytes are not read.
MAX_WORKFLOW_BYTES = 1_000_000
#: The workflow files read hold at most this many bytes in all (PyYAML parses about 0.3 MB/s).
MAX_WORKFLOW_TOTAL_BYTES = 2_000_000
#: At most this many workflow files are read.
MAX_WORKFLOW_FILES = 200
#: The loading and the analysis of one workflow file each stop after this much work (YAML
#: aliases and merge keys can make a small file huge): the loading counts the entries of every
#: mapping it builds, the ones that merge keys copy included, the analysis one per node it visits
#: or item it reads and the length of every string it reads, a string or a container that several
#: jobs share once for each of them.
MAX_WORKFLOW_NODES = 100_000
#: The loading and the analysis of one workflow file each stop after this many seconds, whatever
#: is left of the budget.
MAX_WORKFLOW_SECONDS = 2.0
#: At most this many rulesets are read to see whether the token's account can bypass them (the
#: others make one warning).
MAX_RULESETS = 10
#: The branch the ``push`` filters are matched against when the healer branch is not known yet.
HEALER_BRANCH_EXAMPLE = "healer/dag-task-0123abcd"

_RULES_PER_PAGE = 100
_MAX_RULE_PAGES = 10
_REQUEST_TIMEOUTS = 2
_RUN_TIMEOUTS = 6
_DETAIL_CHARS = 200
_NAME_CHARS = 100
_MAX_LISTED = 10
_MAX_PATTERN_CHARS = 1000
_MAX_CONDITION_CHARS = 2000
_MAX_CALL_DEPTH = 10
_UNREADABLE = "GitHub's answer could not be read"
_NOT_PROTECTED = "Branch not protected"
_TOO_COMPLEX = "too large or complex to analyse"
#: The answers GitHub gives for a ruleset it does not show to the token (an organization one).
_REFUSED = frozenset({403, 404})

#: Token kinds by prefix (the prefix is never shown, only the kind).
_TOKEN_PREFIXES = (
    ("github_pat_", "fine-grained"),
    ("ghs_", "app"),
    ("ghp_", "classic"),
    ("gho_", "oauth"),
    ("ghu_", "app-user"),
)
#: Kinds whose scopes GitHub reports in the ``X-OAuth-Scopes`` response header (a GitHub App
#: user token has no scopes: the App's permissions limit it).
_SCOPED_KINDS = frozenset({"classic", "oauth", "unknown"})
_KIND_SUMMARIES = {
    "fine-grained": "a fine-grained personal access token",
    "app": "a GitHub App installation token limited to 1 repository",
    "classic": "a classic personal access token",
    "oauth": "an OAuth token",
    "app-user": "a GitHub App user token",
    "unknown": "a token of an unknown kind",
}
_SCOPE = re.compile(r"^[a-z0-9_:.-]+$")
_CHECK_BASE_BY_HAND = (
    "Check by hand that {base} requires a pull request with at least one approving review and blocks "
    "force pushes and deletion."
)
_CHECK_WORKFLOWS_BY_HAND = (
    "Check them by hand: no pull_request_target or create workflow may run the healer branch's code with "
    "the repository's secrets."
)


@dataclass(frozen=True)
class SetupFinding:
    """One problem in the GitHub setup: what is wrong (``message``) and what to change (``fix``)."""

    check: str
    level: str
    message: str
    fix: str

    @property
    def text(self) -> str:
        return f"{self.message} {self.fix}"


@dataclass(frozen=True)
class SetupCheck:
    """The result of one check: ``summary`` says what was found when there are no findings."""

    name: str
    summary: str
    findings: tuple[SetupFinding, ...] = ()

    @property
    def level(self) -> str:
        """``error`` if any finding is an error, ``warn`` if any is a warning, else ``ok``."""
        levels = {finding.level for finding in self.findings}
        if ERROR in levels:
            return ERROR
        if WARN in levels:
            return WARN
        return OK


def token_kind(token: str) -> str:
    """The kind of a GitHub token, told from its prefix only.

    ``fine-grained`` (a fine-grained personal access token), ``app`` (a GitHub App installation
    token), ``classic`` (a classic personal access token), ``oauth`` (an OAuth token),
    ``app-user`` (a GitHub App user token) or ``unknown``.
    """
    for prefix, kind in _TOKEN_PREFIXES:
        if token.startswith(prefix):
            return kind
    return "unknown"


def setup_failure(exc: BaseException) -> SetupFinding:
    """The one warning reported when the setup checks fail unexpectedly (the type name only).

    A pull request and ``dag-healer doctor`` both show it, so its fix names no command.
    """
    return SetupFinding(
        SETUP_CHECK,
        WARN,
        f"The GitHub setup could not be checked ({type(exc).__name__}).",
        "Check the network settings and github.repo, then check the token, the base branch's rules "
        "and the workflows by hand.",
    )


def setup_findings(checks: Iterable[SetupCheck]) -> list[SetupFinding]:
    """Every error finding, then every warning, each group in check order."""
    findings = [finding for check in checks for finding in check.findings]
    return [f for f in findings if f.level == ERROR] + [f for f in findings if f.level != ERROR]


def _guarded(name: str, message: str, fix: str, run: Callable[[], SetupCheck]) -> SetupCheck:
    """``run()``, or one warning when it fails unexpectedly: the other checks still run."""
    try:
        return run()
    except Exception as exc:  # noqa: BLE001 - a broken check reports what it could not check
        log.debug("the GitHub %s check failed (%s)", name, type(exc).__name__)  # never the message
        finding = SetupFinding(name, WARN, f"{message} ({type(exc).__name__}).", fix)
        return SetupCheck(name, "", (finding,))


# --------------------------------------------------------------------------- the API


def _one_line(text: str, limit: int) -> str:
    """``text`` on one line, control and format characters replaced, at most ``limit`` characters."""
    flat = "".join(char if char.isprintable() else "?" for char in " ".join(str(text).split()))
    return flat if len(flat) <= limit else flat[: limit - 3] + "..."


def _decoded(body: bytes) -> Any:
    """The JSON of an answer's body; None when it has none (nesting too deep to decode included)."""
    try:
        return json.loads(body)
    except (ValueError, RecursionError):
        return None


@dataclass(frozen=True)
class _Answer:
    """One answer of the API: ``data`` is its JSON (None if it has none), ``why`` its problem.

    ``status`` is None when no answer could be read at all (``why`` says why).
    """

    status: int | None
    data: Any = None
    headers: httpx.Headers | None = None
    why: str = ""

    def why_not(self, expected: type) -> str | None:
        """Why this answer is not a ``200`` whose JSON is an ``expected``; None when it is."""
        if self.status != 200:
            return self.why
        return None if isinstance(self.data, expected) else _UNREADABLE

    @property
    def message(self) -> str | None:
        """GitHub's ``message`` (the reason of an error answer), if any."""
        return self.data.get("message") if isinstance(self.data, dict) else None


class _Api:
    """Read-only GETs to the GitHub API.

    An answer is read uncompressed (``Accept-Encoding: identity``; an encoded one is not
    understood) and up to :data:`MAX_ANSWER_BYTES`. A request gets twice ``timeout`` in all,
    and the requests of one run six times ``timeout``. After a transport error, an answer that
    took too long or the end of that time, no further request is sent.
    """

    def __init__(self, client: httpx.Client, api_url: str, token: str, timeout: float) -> None:
        self._client = client
        self._root = api_url.rstrip("/")
        self._token = token
        self._request_seconds = _REQUEST_TIMEOUTS * timeout
        self._deadline = time.monotonic() + _RUN_TIMEOUTS * timeout
        self._unreachable: str | None = None

    def _stop(self, why: str) -> _Answer:
        self._unreachable = why
        return _Answer(None, why=why)

    def get(self, path: str, params: Mapping[str, Any] | None = None) -> _Answer:
        if self._unreachable is None and time.monotonic() > self._deadline:
            self._unreachable = "the GitHub API did not answer in time"
        if self._unreachable is not None:
            return _Answer(None, why=self._unreachable)
        started = time.monotonic()
        body = bytearray()
        try:
            with self._client.stream("GET", self._root + path, params=params) as response:
                status, headers, reason = response.status_code, response.headers, response.reason_phrase
                if headers.get("content-encoding", "identity").strip().lower() not in ("", "identity"):
                    return _Answer(None, why=_UNREADABLE)  # never decompressed: it could be a bomb
                for chunk in response.iter_bytes():  # as the chunks arrive: never waits to fill one
                    body += chunk
                    if len(body) > MAX_ANSWER_BYTES:
                        return _Answer(None, why="GitHub's answer was too large to read")
                    if time.monotonic() - started > self._request_seconds:
                        return self._stop("the GitHub API answered too slowly")
        except httpx.TransportError as exc:
            return self._stop(f"the GitHub API could not be reached ({type(exc).__name__})")
        except httpx.HTTPError:  # the answer arrived but could not be read
            return _Answer(None, why=_UNREADABLE)
        except Exception as exc:  # noqa: BLE001 - never more requests after an unexpected failure
            return self._stop(f"the request to the GitHub API failed ({type(exc).__name__})")
        data = _decoded(bytes(body))
        if status == 200:
            return _Answer(200, data, headers)
        # The whole body: _error_message redacts the token before it cuts the text, and a cut
        # made here first could leave a prefix of the token behind.
        text = bytes(body).decode("utf-8", errors="replace")
        detail = self.clean(
            _error_message(data, text, reason, self._token).strip().rstrip("."), _DETAIL_CHARS
        )
        return _Answer(status, data, headers, f"GitHub answered {status}: {detail}")

    def clean(self, text: str, limit: int) -> str:
        """Text from GitHub, made safe to show: the token redacted, on one line, cut to ``limit``."""
        return _one_line(text.replace(self._token, "***") if self._token else text, limit)


# --------------------------------------------------------------------------- the token


def _scopes(headers: httpx.Headers | None) -> set[str] | None:
    """The scopes of the ``X-OAuth-Scopes`` response header; None when there is no such header."""
    if headers is None or "x-oauth-scopes" not in headers:
        return None
    items = (item.strip().lower() for item in headers.get("x-oauth-scopes", "").split(","))
    return {item for item in items if _SCOPE.match(item)}


def _installation_count(api: _Api) -> tuple[int | None, str | None]:
    """How many repositories the App installation behind the token reaches, or why it is unknown."""
    answer = api.get("/installation/repositories", {"per_page": 1})
    why = answer.why_not(dict)
    if why is not None:
        return None, why
    total = answer.data.get("total_count")
    if not isinstance(total, int) or isinstance(total, bool) or total < 1:
        return None, _UNREADABLE
    return total, None


def _token_check(api: _Api, token: str, repo: str, repo_answer: _Answer) -> SetupCheck:
    kind = token_kind(token)
    narrow = (
        f"Use a fine-grained personal access token limited to {repo}, or a GitHub App installed on "
        f"{repo} only."
    )
    findings: list[SetupFinding] = []

    def warn(message: str, fix: str) -> None:
        findings.append(SetupFinding(TOKEN_CHECK, WARN, message, fix))

    if kind == "classic":
        warn(
            "The GitHub token is a classic personal access token: its scope covers every repository "
            f"its account can write to, not only {repo}.",
            narrow,
        )
    elif kind == "oauth":
        warn(
            "The GitHub token is an OAuth token: its scope covers every repository its account can "
            f"write to, not only {repo}.",
            narrow,
        )
    elif kind == "app-user":
        warn(
            "The GitHub token is a GitHub App user token: it acts for its user on every repository that "
            f"the App is installed on and the user can access, not only {repo}.",
            narrow,
        )
    elif kind == "unknown":
        warn(
            "The kind of the GitHub token cannot be told from its prefix, so which repositories it can "
            "reach was not checked.",
            narrow,
        )
    if kind in _SCOPED_KINDS:
        scopes = _scopes(repo_answer.headers)
        if scopes is None:
            why = repo_answer.why if repo_answer.headers is None else "GitHub's answer does not list them"
            warn(
                f"Could not check the scopes of the GitHub token: {why}.",
                "Check by hand that the token has no workflow scope and no admin or delete scope.",
            )
            scopes = set()
        if "workflow" in scopes:
            warn(
                "The GitHub token has the workflow scope: it can push changes to .github/workflows/, "
                "which GitHub refuses to a token without it.",
                "Remove the workflow scope from the token, or use a fine-grained token without the "
                "Workflows permission.",
            )
        extra = sorted(s for s in scopes if s.startswith(("admin:", "delete")) or s == "site_admin")
        if extra:
            warn(
                f"The GitHub token has scopes that dag-healer never needs: {', '.join(extra)}.",
                f"Remove them from the token, or use a fine-grained token limited to {repo}.",
            )
    if kind == "app":
        total, why = _installation_count(api)
        if why is not None:
            warn(
                "Could not check how many repositories the GitHub App installation behind the token "
                f"can reach: {why}.",
                f"Check by hand that the GitHub App is installed on {repo} only.",
            )
        elif total is not None and total > 1:
            warn(
                f"The GitHub App installation behind the token can reach {total} repositories, not "
                f"only {repo}.",
                f"Install the GitHub App on {repo} only (Only select repositories).",
            )
    return SetupCheck(TOKEN_CHECK, _KIND_SUMMARIES[kind], tuple(findings))


# --------------------------------------------------------------------------- the repository


def _repository_check(repo: str, answer: _Answer) -> SetupCheck:
    summary = f"the token's account is not an admin of {repo}"
    why = answer.why_not(dict)
    if why is not None:
        finding = SetupFinding(
            REPOSITORY_CHECK,
            WARN,
            f"Could not check the token's access to {repo}: {why}.",
            f"Check github.repo, and that the token can read {repo}.",
        )
        return SetupCheck(REPOSITORY_CHECK, summary, (finding,))
    permissions = answer.data.get("permissions")
    admin = permissions.get("admin") if isinstance(permissions, dict) else None
    if admin is False:
        return SetupCheck(REPOSITORY_CHECK, summary)
    if admin is True:
        finding = SetupFinding(
            REPOSITORY_CHECK,
            WARN,
            f"The token's account is an admin of {repo}: it can change or bypass the rules of the base "
            "branch.",
            f"Use a token whose account has at most the Write role on {repo}, such as a GitHub App or a "
            "dedicated account.",
        )
    else:
        finding = SetupFinding(
            REPOSITORY_CHECK,
            WARN,
            f"Could not check whether the token's account is an admin of {repo}: GitHub's answer does "
            "not say.",
            f"Check by hand that the token's account has at most the Write role on {repo}.",
        )
    return SetupCheck(REPOSITORY_CHECK, summary, (finding,))


# --------------------------------------------------------------------------- the base branch


@dataclass(frozen=True)
class _Requirement:
    """What the base branch must enforce: the rule type that does it, and the texts if it does not."""

    rule: str
    level: str
    message: str
    fix: str
    phrase: str

    def finding(self, base: str) -> SetupFinding:
        return SetupFinding(
            BASE_BRANCH_CHECK, self.level, self.message.format(base=base), self.fix.format(base=base)
        )


_PULL_REQUEST, _NO_FORCE_PUSHES, _NO_DELETION, _STATUS_CHECKS = (
    "pull_request",
    "non_fast_forward",
    "deletion",
    "required_status_checks",
)
_REQUIREMENTS = (
    _Requirement(
        _PULL_REQUEST,
        ERROR,
        "The base branch {base} does not require a pull request with at least one approving review, so "
        "a change can reach it without review.",
        "Add a ruleset (or branch protection) for {base} that requires a pull request with at least one "
        "approving review.",
        "requires a pull request with at least one approving review",
    ),
    _Requirement(
        _NO_FORCE_PUSHES,
        ERROR,
        "The base branch {base} does not block force pushes, so its history can be rewritten.",
        "Block force pushes in the ruleset (or branch protection) of {base}.",
        "blocks force pushes",
    ),
    _Requirement(
        _NO_DELETION,
        ERROR,
        "The base branch {base} can be deleted.",
        "Restrict deletions in the ruleset (or branch protection) of {base}.",
        "blocks deletion",
    ),
    _Requirement(
        _STATUS_CHECKS,
        WARN,
        "The base branch {base} requires no status checks, so nothing stops a merge before CI has run "
        "on the healer branch.",
        "Add the CI's checks to the ruleset (or branch protection) of {base} as required status checks.",
        "requires status checks",
    ),
)
_BYPASS_MESSAGES = {
    "always": 'The token\'s account can bypass the ruleset "{name}" of {base} at any time.',
    "pull_requests_only": 'The token\'s account can bypass the ruleset "{name}" of {base} for pull requests.',
    "exempt": 'The token\'s account is exempt from the ruleset "{name}" of {base}.',
}


def _count(value: Any) -> int:
    return value if isinstance(value, int) and not isinstance(value, bool) else 0


def _non_empty_list(value: Any) -> bool:
    return isinstance(value, list) and bool(value)


def _rule_requirement(rule: Mapping[str, Any]) -> str | None:
    """The requirement a rule of ``GET .../rules/branches/{branch}`` meets, if any."""
    kind = rule.get("type")
    parameters = rule.get("parameters")
    parameters = parameters if isinstance(parameters, dict) else {}
    if kind == _PULL_REQUEST:
        return _PULL_REQUEST if _count(parameters.get("required_approving_review_count")) >= 1 else None
    if kind in (_NO_FORCE_PUSHES, _NO_DELETION):
        return str(kind)
    if kind == _STATUS_CHECKS:
        return _STATUS_CHECKS if _non_empty_list(parameters.get("required_status_checks")) else None
    return None


def _classic_requirements(protection: Mapping[str, Any]) -> set[str]:
    """The requirements a classic branch protection (``GET .../branches/{b}/protection``) meets."""

    def disabled(name: str) -> bool:
        setting = protection.get(name)
        return isinstance(setting, dict) and setting.get("enabled") is False

    met: set[str] = set()
    reviews = protection.get("required_pull_request_reviews")
    if isinstance(reviews, dict) and _count(reviews.get("required_approving_review_count")) >= 1:
        met.add(_PULL_REQUEST)
    if disabled("allow_force_pushes"):
        met.add(_NO_FORCE_PUSHES)
    if disabled("allow_deletions"):
        met.add(_NO_DELETION)
    checks = protection.get("required_status_checks")
    if isinstance(checks, dict) and (
        _non_empty_list(checks.get("contexts")) or _non_empty_list(checks.get("checks"))
    ):
        met.add(_STATUS_CHECKS)
    return met


def _branch_rules(api: _Api, path: str) -> tuple[list[dict[str, Any]], str | None]:
    """The rules that apply to a branch (every page), and why they could not all be read."""
    rules: list[dict[str, Any]] = []
    for page in range(1, _MAX_RULE_PAGES + 1):
        params: dict[str, int] = {"per_page": _RULES_PER_PAGE}
        if page > 1:
            params["page"] = page
        answer = api.get(path, params)
        why = answer.why_not(list)
        if why is not None:
            return rules, why
        rules += [item for item in answer.data if isinstance(item, dict)]
        if len(answer.data) < _RULES_PER_PAGE:
            break
    return rules, None


def _join(items: list[str]) -> str:
    return items[0] if len(items) == 1 else ", ".join(items[:-1]) + " and " + items[-1]


def _rules_unreadable(base: str, why: str) -> SetupFinding:
    return SetupFinding(
        BASE_BRANCH_CHECK,
        WARN,
        f"Could not check the rules of the base branch {base}: {why}.",
        _CHECK_BASE_BY_HAND.format(base=base),
    )


def _unmet_findings(
    api: _Api, branch_path: str, base: str, unmet: list[_Requirement], rules_why: str | None
) -> list[SetupFinding]:
    """What the branch and its classic protection say about the requirements the rules leave unmet."""
    branch = api.get(branch_path)
    why = branch.why_not(dict)
    protected = branch.data.get("protected") if why is None else None
    if why is None and not isinstance(protected, bool):
        why = _UNREADABLE
    if why is not None:
        if rules_why is not None:
            return [_rules_unreadable(base, rules_why)]
        finding = SetupFinding(
            BASE_BRANCH_CHECK,
            WARN,
            f"Could not check the base branch {base}: {why}.",
            f"Check github.pr_base, and check by hand that {base} requires a pull request with at least "
            "one approving review and blocks force pushes and deletion.",
        )
        return [finding]
    if not protected:  # neither a ruleset nor classic branch protection applies to the branch
        return [requirement.finding(base) for requirement in unmet]
    protection = api.get(f"{branch_path}/protection")
    if protection.status == 404 and protection.message == _NOT_PROTECTED:
        classic: set[str] = set()
    elif protection.why_not(dict) is None:
        classic = _classic_requirements(protection.data)
    elif rules_why is not None:
        return [_rules_unreadable(base, rules_why)]
    else:
        finding = SetupFinding(
            BASE_BRANCH_CHECK,
            WARN,
            f"The base branch {base} has branch protection that the token cannot read, so it was not "
            f"checked whether it {_join([requirement.phrase for requirement in unmet])}.",
            f"Check it by hand in the repository's settings (Branches), or protect {base} with a ruleset, "
            "which the token can read.",
        )
        return [finding]
    still_unmet = [requirement for requirement in unmet if requirement.rule not in classic]
    if still_unmet and rules_why is not None:
        return [_rules_unreadable(base, rules_why)]
    return [requirement.finding(base) for requirement in still_unmet]


def _rate_limited(answer: _Answer) -> bool:
    """True for an answer of GitHub's rate limits, which can be a ``403``.

    It says so in ``X-RateLimit-Remaining: 0`` (the primary rate limit) or in its message (a
    secondary rate limit, for example).
    """
    remaining = answer.headers.get("x-ratelimit-remaining") if answer.headers is not None else None
    message = answer.message
    return (remaining or "").strip() == "0" or (isinstance(message, str) and "rate limit" in message.lower())


def _bypass_findings(api: _Api, repo_path: str, base: str, ruleset_ids: list[int]) -> list[SetupFinding]:
    """A warning for each ruleset the token's account can bypass.

    A ruleset GitHub refuses to show (a ``403`` or ``404`` answer: an organization ruleset, for
    example), or whose answer does not say (``current_user_can_bypass`` missing), adds nothing;
    one that got no answer (a transport error, a timeout), any other error answer (a server
    error, a rate limit, ``403`` included, a redirect) or an answer that could not be read makes
    one warning for them all. Only the first :data:`MAX_RULESETS` are read: the others make one
    warning more.
    """
    findings: list[SetupFinding] = []
    unanswered: str | None = None
    for ruleset_id in ruleset_ids[:MAX_RULESETS]:
        answer = api.get(f"{repo_path}/rulesets/{ruleset_id}")
        if answer.status in _REFUSED and not _rate_limited(answer):
            continue
        why = answer.why_not(dict)
        if why is not None:
            unanswered = unanswered or why
            continue
        mode = answer.data.get("current_user_can_bypass")
        template = _BYPASS_MESSAGES.get(mode) if isinstance(mode, str) else None
        if template is None:
            continue
        name = answer.data.get("name")
        shown = api.clean(name, _NAME_CHARS) if isinstance(name, str) and name.strip() else f"#{ruleset_id}"
        findings.append(
            SetupFinding(
                BASE_BRANCH_CHECK,
                WARN,
                template.format(name=shown, base=base),
                "Remove the account, or its role, team or app, from the ruleset's bypass list.",
            )
        )
    by_hand = f"Check by hand that the token's account is in the bypass list of no ruleset of {base}."
    if unanswered is not None:
        findings.append(
            SetupFinding(
                BASE_BRANCH_CHECK,
                WARN,
                f"Could not check whether the token's account can bypass the rulesets of {base}: "
                f"{unanswered}.",
                by_hand,
            )
        )
    beyond = len(ruleset_ids) - MAX_RULESETS
    if beyond > 0:
        more = "1 more ruleset" if beyond == 1 else f"{beyond} more rulesets"
        findings.append(
            SetupFinding(
                BASE_BRANCH_CHECK,
                WARN,
                f"Could not check whether the token's account can bypass {more} of {base}: only the "
                f"first {MAX_RULESETS} are read.",
                by_hand,
            )
        )
    return findings


def _base_branch_check(api: _Api, repo_path: str, base: str, *, skip_ci: bool) -> SetupCheck:
    required = [r for r in _REQUIREMENTS if skip_ci or r.rule != _STATUS_CHECKS]
    summary = f"{base} requires a pull request with an approving review and blocks force pushes and deletion"
    if skip_ci:
        summary += ", and requires status checks"
    branch_path = f"{repo_path}/branches/{quote(base, safe='')}"
    rules, rules_why = _branch_rules(api, f"{repo_path}/rules/branches/{quote(base, safe='')}")
    wanted = {requirement.rule for requirement in required}
    met: set[str] = set()
    ruleset_ids: dict[int, None] = {}  # the rulesets that hold a required rule, in the order seen
    for rule in rules:
        requirement = _rule_requirement(rule)
        if requirement in wanted:
            met.add(requirement)
            ruleset_id = rule.get("ruleset_id")
            if isinstance(ruleset_id, int) and not isinstance(ruleset_id, bool) and ruleset_id > 0:
                ruleset_ids.setdefault(ruleset_id, None)
    unmet = [requirement for requirement in required if requirement.rule not in met]
    findings = _unmet_findings(api, branch_path, base, unmet, rules_why) if unmet else []
    findings += _bypass_findings(api, repo_path, base, list(ruleset_ids))
    return SetupCheck(BASE_BRANCH_CHECK, summary, tuple(findings))


# --------------------------------------------------------------------------- the workflows

_WORKFLOWS_DIR = ".github/workflows"
#: pull_request_target activity types that start only when someone, or another tool, acts on the
#: pull request (a bot can add a label before the review; closing is how a reviewer rejects it),
#: named in the finding's text; every other type (the default ones, review_requested, ...) can
#: start on its own. Both are errors.
_ON_REQUEST = {
    "labeled": "a label is added",
    "unlabeled": "a label is removed",
    "closed": "the pull request is closed",
}
#: The job conditions that skip a job for healer pull requests, and for healer branch pushes and
#: creates, and the ones that run a create job for tags only (``github.event.ref_type`` is the
#: create event's own field). Each must be the whole condition, or one of the conditions joined
#: by ``&&`` at its top level (:func:`_conjuncts`).
_SKIPS_HEALER_PULL_REQUESTS = re.compile(
    r"!\s*startsWith\(\s*github\.head_ref\s*,\s*(?:'healer/'|\"healer/\")\s*\)", re.IGNORECASE
)
_SKIPS_HEALER_BRANCHES = re.compile(
    r"!\s*startsWith\(\s*github\.ref\s*,\s*(?:'refs/heads/healer/'|\"refs/heads/healer/\")\s*\)",
    re.IGNORECASE,
)
_TAGS_ONLY = re.compile(
    r"startsWith\(\s*github\.ref\s*,\s*(?:'refs/tags/'|\"refs/tags/\")\s*\)"
    r"|github\.(?:event\.)?ref_type\s*==\s*(?:'tag'|\"tag\")",
    re.IGNORECASE,
)
#: References to the pull request's head: the head object (sha, ref, repo, ...), the head branch
#: (``GITHUB_HEAD_REF`` included), its merge commit, a pull request ref (``refs/pull/...``,
#: ``pull/<number>/head``) and ``gh pr checkout``. Each repetition stops at a character that it
#: cannot cross (``/``, a white space, a non-space) or after a fixed length, so that a search
#: takes linear time: an unbounded ``[^}]*`` made a megabyte of ``pull/${{`` take half a minute.
_HEAD_REFERENCE = re.compile(
    r"github\.event\.pull_request\.head\b|github[._]head_ref\b"
    r"|github\.event\.pull_request\.merge_commit_sha\b|refs/pull/"
    r"|\bpull/(?:\$\{\{[^}]{0,200}\}\}|[^\s/]+)/(?:head|merge)\b"
    r"|\b(?:gh|hub)\s+pr\s+checkout\b",
    re.IGNORECASE,
)
#: The secrets context, other than ``secrets.GITHUB_TOKEN``.
_SECRETS = re.compile(r"\bsecrets\b(?!\s*\.\s*github_token\b)(?=\s*[.\[),}])", re.IGNORECASE)
#: A job that calls a reusable workflow of the same repository.
_LOCAL_CALL = re.compile(r"^\./\.github/workflows/([^/\\@\s]+)$")
#: open() flags of a workflow file: never follow a link, never wait on a FIFO.
_OPEN_FLAGS = (
    os.O_RDONLY | getattr(os, "O_BINARY", 0) | getattr(os, "O_NOFOLLOW", 0) | getattr(os, "O_NONBLOCK", 0)
)


class _TooLarge(Exception):
    """The loading or the analysis of one workflow file went past its budget or its deadline."""


class _Budget:
    """What the analysis of one workflow file may still do, and until when.

    :meth:`spend` takes the cost of a piece of work before it is done: one for each node
    visited and each item of a list or mapping read, the length of each string read (the
    regular expressions and the condition parser go through each of its characters), and the
    states that a branch pattern keeps alive. Past :data:`MAX_WORKFLOW_NODES`, or
    :data:`MAX_WORKFLOW_SECONDS` after the analysis started, the file is too large or complex
    to analyse.
    """

    def __init__(self) -> None:
        self.left = MAX_WORKFLOW_NODES
        self.deadline = time.monotonic() + MAX_WORKFLOW_SECONDS

    def spend(self, count: int) -> None:
        self.left -= count
        if self.left < 0 or time.monotonic() > self.deadline:
            raise _TooLarge


_MERGE_TAG = "tag:yaml.org,2002:merge"
_VALUE_TAG = "tag:yaml.org,2002:value"
_STR_TAG = "tag:yaml.org,2002:str"
#: Scalars that a workflow file loads as the text they are written in (see :class:`_WorkflowLoader`).
_TEXT_TAGS = ("tag:yaml.org,2002:int", "tag:yaml.org,2002:float", "tag:yaml.org,2002:timestamp")


class _WorkflowLoader(yaml.SafeLoader):
    """PyYAML's safe loader (pure Python), with a budget and a deadline, numbers kept as text.

    The loading stops, the file being too large or complex to analyse, past
    :data:`MAX_WORKFLOW_NODES` mapping entries in all (each ``<<`` merge key copies the entries
    of the mappings it names, so a chain of them grows a small file exponentially) or
    :data:`MAX_WORKFLOW_SECONDS` after it started. Integers, floats and timestamps stay the text
    they are written in: the analysis never reads them, and PyYAML builds some in quadratic
    time (a YAML 1.1 sexagesimal integer such as ``1:9:9:9``, or integer keys whose hashes
    collide). Booleans and null are loaded as usual, so the key ``on`` is still True.

    Aliases are not copied: they share the object they name, which the analysis then pays for
    each time it reads it (:class:`_Budget`). libyaml's loader is not used: its recursion
    crashes the process on a deeply nested file, where the pure Python one raises
    ``RecursionError``.
    """

    def __init__(self, stream: str) -> None:
        super().__init__(stream)
        self._entries_left = MAX_WORKFLOW_NODES
        self._deadline = time.monotonic() + MAX_WORKFLOW_SECONDS
        self._merging: set[int] = set()

    def _spend(self, entries: int = 0) -> None:
        """Take ``entries`` mapping entries from the budget; past it, or past the deadline, stop."""
        self._entries_left -= entries
        if self._entries_left < 0 or time.monotonic() > self._deadline:
            raise _TooLarge

    def compose_node(self, parent: yaml.Node | None, index: Any) -> yaml.Node:
        self._spend()  # the deadline, once per node
        return super().compose_node(parent, index)

    def construct_object(self, node: yaml.Node, deep: bool = False) -> Any:
        self._spend()  # the deadline, once per object
        return super().construct_object(node, deep=deep)

    def flatten_mapping(self, node: yaml.MappingNode) -> None:
        """Merge the ``<<`` entries of ``node`` as PyYAML does, in linear time and within the budget.

        The mapping's own entries win over the merged ones, and the first mapping of a merged
        list wins over the next. The entries of ``node`` and those that each merge copies are
        spent before they are read; a mapping that merges itself is not valid YAML here.
        """
        self._spend(len(node.value))
        if id(node) in self._merging:
            raise yaml.constructor.ConstructorError(
                "while constructing a mapping", node.start_mark, "found a mapping that merges itself"
            )
        self._merging.add(id(node))
        try:
            merged: list[tuple[yaml.Node, yaml.Node]] = []
            kept: list[tuple[yaml.Node, yaml.Node]] = []
            for key_node, value_node in node.value:
                if key_node.tag != _MERGE_TAG:
                    if key_node.tag == _VALUE_TAG:
                        key_node.tag = _STR_TAG
                    kept.append((key_node, value_node))
                    continue
                sources = value_node.value if isinstance(value_node, yaml.SequenceNode) else [value_node]
                if not all(isinstance(source, yaml.MappingNode) for source in sources):
                    raise yaml.constructor.ConstructorError(
                        "while constructing a mapping",
                        node.start_mark,
                        "expected a mapping or list of mappings for merging",
                        value_node.start_mark,
                    )
                for source in sources:
                    self.flatten_mapping(source)
                for source in reversed(sources):  # later entries win: the first mapping goes last
                    self._spend(len(source.value))
                    merged.extend(source.value)
            node.value = merged + kept
        finally:
            self._merging.discard(id(node))


def _construct_text(loader: yaml.SafeLoader, node: yaml.ScalarNode) -> str:
    """A scalar as the text it is written in (an integer, a float or a timestamp of a workflow)."""
    return loader.construct_scalar(node)


for _tag in _TEXT_TAGS:  # add_constructor changes this loader only, never yaml.SafeLoader
    _WorkflowLoader.add_constructor(_tag, _construct_text)
del _tag


def _expressions(text: str) -> list[str]:
    """The ``${{ ... }}`` expressions of a string, braces included (a linear scan)."""
    found: list[str] = []
    start = text.find("${{")
    while start != -1:
        end = text.find("}}", start + 3)
        if end == -1:
            break
        found.append(text[start : end + 2])
        start = text.find("${{", end + 2)
    return found


def _uses_secrets(text: str, *, condition: bool) -> bool:
    if "secrets" not in text.lower():
        return False
    if condition and _SECRETS.search(text):
        return True
    return any(_SECRETS.search(expression) for expression in _expressions(text))


Flags = tuple[bool, bool]


def _scan(root: Any, budget: _Budget) -> Flags:
    """(refers to the pull request's head, uses credentials) anywhere in ``root``.

    Credentials are the repository's secrets (other than ``secrets.GITHUB_TOKEN``, or
    ``secrets: inherit``) and write permissions: ``permissions: write-all``, or a scope set to
    ``write`` (``id-token: write`` gets OIDC tokens for cloud credentials, ``contents: write`` a
    ``GITHUB_TOKEN`` that can push). An iterative walk that reads each string and expands each
    container once (YAML aliases share, and may nest, them), or twice when the key it is found
    under changes what it means: a string as an ``if`` (a whole condition) or not, a mapping
    under ``permissions`` or not. A node met again costs one.
    """
    head = credentials = False
    seen: set[tuple[int, bool]] = set()
    stack: list[tuple[Any, Any]] = [(None, root)]
    while stack and not (head and credentials):
        key, node = stack.pop()
        if not isinstance(node, (str, dict, list)):
            budget.spend(1)
            continue
        # whether the key changes the node's meaning: a whole condition, a mapping of permissions
        role = key == "if" if isinstance(node, str) else (key == "permissions" and isinstance(node, dict))
        if (id(node), role) in seen:
            budget.spend(1)
            continue
        seen.add((id(node), role))
        budget.spend(1 + len(node))
        if isinstance(node, str):
            head = head or bool(_HEAD_REFERENCE.search(node))
            credentials = credentials or _uses_secrets(node, condition=role)
        elif isinstance(node, dict):
            for item_key, value in node.items():
                if isinstance(value, str) and (role or item_key in ("secrets", "permissions")):
                    budget.spend(len(value))
                    word = value.strip().lower()
                    if (
                        (item_key == "secrets" and word == "inherit")
                        or (item_key == "permissions" and word == "write-all")
                        or (role and word == "write")
                    ):
                        credentials = True
                stack.append((item_key, value))
        else:
            stack.extend((None, item) for item in node)
    return head, credentials


def _either(*flags: Flags) -> Flags:
    return any(head for head, _ in flags), any(credentials for _, credentials in flags)


def _rest(document: Mapping[Any, Any], budget: _Budget) -> dict[Any, Any]:
    """The workflow without its events and its jobs (``env``, ``permissions``, ``defaults``, ...)."""
    budget.spend(len(document))
    return {key: value for key, value in document.items() if not (key in ("jobs", "on") or key is True)}


def _called_workflow(job: Any, budget: _Budget) -> str | None:
    """The file name of the reusable workflow of this repository that a job calls, if any."""
    uses = job.get("uses") if isinstance(job, dict) else None
    if not isinstance(uses, str):
        return None
    budget.spend(len(uses))
    match = _LOCAL_CALL.match(uses.strip())
    return match.group(1) if match else None


class _Callees:
    """What the reusable workflows of the clone do (``uses: ./.github/workflows/x.yml``).

    A called workflow runs in its caller's event, with the caller's ``github`` context and the
    secrets the caller passes: its head references and credentials count for the calling job.
    Its own jobs and the workflows it calls (up to ten levels, as GitHub allows) are scanned,
    with the caller's budget; job conditions inside it are not read.
    """

    def __init__(self, documents: Mapping[str, Mapping[Any, Any]]) -> None:
        self._documents = documents
        self._flags: dict[str, Flags] = {}
        self._active: list[str] = []

    def flags(self, name: str | None, budget: _Budget) -> Flags:
        if name is None or name not in self._documents or name in self._active:
            return False, False
        if len(self._active) >= _MAX_CALL_DEPTH:
            return False, False  # GitHub refuses deeper nesting: nothing runs
        if name not in self._flags:
            self._active.append(name)
            try:
                document = self._documents[name]
                jobs = document.get("jobs")
                found = [_scan(_rest(document, budget), budget)]
                for job in jobs.values() if isinstance(jobs, dict) else [jobs]:
                    scanned = _scan(job, budget)
                    found.append(_either(scanned, self.flags(_called_workflow(job, budget), budget)))
                self._flags[name] = _either(*found)
            finally:
                self._active.pop()
        return self._flags[name]


@dataclass(frozen=True)
class _Token:
    """One element of a GitHub filter pattern, matching one character.

    ``chars`` None matches any character (``/`` only with ``slash``); ``optional`` and ``repeat``
    let it match zero times or several times.
    """

    chars: frozenset[str] | None
    slash: bool = False
    optional: bool = False
    repeat: bool = False

    def accepts(self, char: str) -> bool:
        if self.chars is None:
            return self.slash or char != "/"
        return char in self.chars


def _class_chars(content: str) -> frozenset[str] | None:
    """The characters of a ``[...]`` class (letters and digits, ranges within a-z, A-Z or 0-9)."""
    chars: set[str] = set()
    index = 0
    while index < len(content):
        char = content[index]
        if not (char.isascii() and char.isalnum()):
            return None
        if index + 2 < len(content) and content[index + 1] == "-":
            last = content[index + 2]
            same_kind = any(test(char) and test(last) for test in (str.isdigit, str.islower, str.isupper))
            if not (last.isascii() and same_kind) or last < char:
                return None
            chars.update(chr(code) for code in range(ord(char), ord(last) + 1))
            index += 3
        else:
            chars.add(char)
            index += 1
    return frozenset(chars) or None


@functools.lru_cache(maxsize=256)
def _filter_tokens(pattern: str) -> tuple[_Token, ...] | None:
    """A GitHub filter pattern as tokens; None when it is invalid (it is then matched literally).

    ``**`` is any characters, ``*`` any characters but ``/``, ``?`` and ``+`` make the previous
    character optional or repeated, ``[...]`` is a class and ``\\`` escapes the next character.
    """
    if len(pattern) > _MAX_PATTERN_CHARS:
        return None
    tokens: list[_Token] = []
    index = 0
    while index < len(pattern):
        char = pattern[index]
        if char == "\\" and index + 1 < len(pattern):
            tokens.append(_Token(frozenset(pattern[index + 1])))
            index += 2
        elif pattern.startswith("**", index):
            tokens.append(_Token(None, slash=True, optional=True, repeat=True))
            index += 2
        elif char == "*":
            tokens.append(_Token(None, optional=True, repeat=True))
            index += 1
        elif char in "?+":
            if not tokens:
                return None
            last = tokens[-1]
            tokens[-1] = _Token(
                last.chars, last.slash, last.optional or char == "?", last.repeat or char == "+"
            )
            index += 1
        elif char == "[":
            end = pattern.find("]", index + 1)
            chars = _class_chars(pattern[index + 1 : end]) if end != -1 else None
            if chars is None:
                return None
            tokens.append(_Token(chars))
            index = end + 1
        else:
            tokens.append(_Token(frozenset(char)))
            index += 1
    return tuple(tokens)


def _pattern_matches(pattern: str, name: str, budget: _Budget) -> bool:
    """True when the filter pattern matches the whole of ``name`` (no regex).

    The states that each character of ``name`` moves on are spent from ``budget``: a pattern of
    many ``*`` keeps as many states alive, for every character.
    """
    tokens = _filter_tokens(pattern)
    if tokens is None:
        return pattern == name
    end = len(tokens)

    def closure(states: set[int]) -> set[int]:
        result: set[int] = set()
        pending = list(states)
        while pending:
            state = pending.pop()
            if state not in result:
                result.add(state)
                if state < end and tokens[state].optional:
                    pending.append(state + 1)
        return result

    states = closure({0})
    for char in name:
        budget.spend(len(states))
        moved: set[int] = set()
        for state in states:
            if state < end and tokens[state].accepts(char):
                moved.add(state + 1)
                if tokens[state].repeat:
                    moved.add(state)
        if not moved:
            return False
        states = closure(moved)
    return end in states


def _patterns(value: Any, budget: _Budget) -> list[str]:
    if isinstance(value, str):
        return [value]
    if isinstance(value, list):
        budget.spend(len(value))
        return [item for item in value if isinstance(item, str)]
    return []


def _filter_matches(patterns: list[str], name: str, budget: _Budget) -> bool:
    """GitHub's filter semantics: patterns in order, a leading ``!`` excludes what matched before."""
    matched = False
    for pattern in patterns:
        budget.spend(len(pattern) + 1)
        if pattern.startswith("!"):
            if _pattern_matches(pattern[1:], name, budget):
                matched = False
        elif _pattern_matches(pattern, name, budget):
            matched = True
    return matched


def _branch_filter_allows(config: Mapping[str, Any], name: str, budget: _Budget) -> bool:
    """Whether the ``branches`` / ``branches-ignore`` filters of an event let ``name`` through."""
    branches = _patterns(config.get("branches"), budget)
    if branches:
        return _filter_matches(branches, name, budget)
    ignored = _patterns(config.get("branches-ignore"), budget)
    if ignored:
        return not _filter_matches(ignored, name, budget)
    return True


def _push_runs(config: Any, branch: str, budget: _Budget) -> bool:
    """A ``push`` event runs for the healer branch (its filters match the pushed branch)."""
    if not isinstance(config, dict):
        return True
    has_branch_filter = "branches" in config or "branches-ignore" in config
    if not has_branch_filter and ("tags" in config or "tags-ignore" in config):
        return False  # tags only
    return _branch_filter_allows(config, branch, budget)


def _pull_request_runs(config: Any, base: str, budget: _Budget) -> bool:
    """A ``pull_request(_target)`` event runs for a healer pull request (filters match the BASE branch).

    Every activity type counts: which ones start before the review is the caller's question.
    """
    if not isinstance(config, dict):
        return True
    return _branch_filter_allows(config, base, budget)


def _activity_types(config: Any, budget: _Budget) -> list[str]:
    """The ``types`` of an event, lowercase and sorted (empty: GitHub's default ones)."""
    if not isinstance(config, dict):
        return []
    types: set[str] = set()
    for item in _patterns(config.get("types"), budget):
        budget.spend(len(item))
        if item.strip():
            types.add(item.strip().lower())
    return sorted(types)


def _quote_end(text: str, index: int) -> int:
    """The index of the quote that closes the string opened at ``index`` (``''`` escapes); -1: none."""
    quote = text[index]
    index += 1
    while index < len(text):
        if text[index] == quote:
            if text.startswith(quote * 2, index):
                index += 2
                continue
            return index
        index += 1
    return -1


def _closing_parentheses(text: str) -> dict[int, int] | None:
    """The index of the ``)`` that closes each ``(`` outside string literals; None: unbalanced.

    A string literal that is never closed makes the text unbalanced too.
    """
    closing: dict[int, int] = {}
    opened: list[int] = []
    index = 0
    while index < len(text):
        char = text[index]
        if char in "'\"":
            index = _quote_end(text, index)
            if index < 0:
                return None
        elif char == "(":
            opened.append(index)
        elif char == ")":
            if not opened:
                return None
            closing[opened.pop()] = index
        index += 1
    return None if opened else closing


def _unwrapped(text: str, closing: Mapping[int, int], start: int, end: int) -> tuple[int, int]:
    """The (start, end) of ``text[start:end]`` without the white space and parentheses around it all.

    ``((a && b))`` is ``a && b``; ``(a) && (b)`` stays as it is.
    """
    while True:
        while start < end and text[start].isspace():
            start += 1
        while end > start and text[end - 1].isspace():
            end -= 1
        if start < end and text[start] == "(" and closing.get(start) == end - 1:
            start, end = start + 1, end - 1
        else:
            return start, end


def _split_top(
    text: str, closing: Mapping[int, int], start: int, end: int, operator: str
) -> list[tuple[int, int]]:
    """``text[start:end]`` split on ``operator`` outside parentheses and string literals.

    Only the top level is read: a parenthesised part is passed over in one step.
    """
    parts: list[tuple[int, int]] = []
    part = index = start
    while index < end:
        char = text[index]
        if char in "'\"":
            index = _quote_end(text, index) + 1
        elif char == "(":
            index = closing[index] + 1
        elif text.startswith(operator, index, end):
            parts.append((part, index))
            index += len(operator)
            part = index
        else:
            index += 1
    parts.append((part, end))
    return parts


def _conjuncts(condition: str, budget: _Budget) -> list[str] | None:
    """The conditions joined by ``&&`` at the top level of a job's ``if``; None for any other form.

    The value must be one expression: bare, or one ``${{ ... }}`` with nothing around it (text
    around an expression makes the condition a non-empty string, which is always true). A ``||``
    at the top level makes it None; parentheses that enclose a condition are dropped, so
    ``(a && b) && c`` gives ``a``, ``b`` and ``c``, while ``!(a && b)`` stays one condition and
    so does ``(a || b && c)``, where ``&&`` binds more tightly than ``||``. The length of the
    condition is spent first; it is then read in linear time, each part at its own level only.
    """
    budget.spend(len(condition))
    text = condition.strip()
    if len(text) > _MAX_CONDITION_CHARS:
        return None
    if "${{" in text:
        if not (text.startswith("${{") and text.endswith("}}")) or text.count("${{") != 1:
            return None
        text = text[3:-2]
    closing = _closing_parentheses(text)
    if closing is None:
        return None
    whole = _unwrapped(text, closing, 0, len(text))
    if len(_split_top(text, closing, *whole, "||")) != 1:
        return None
    found: list[str] = []
    pending = [whole]
    while pending:
        start, end = _unwrapped(text, closing, *pending.pop())
        parts = _split_top(text, closing, start, end, "&&")
        if len(parts) == 1 or len(_split_top(text, closing, start, end, "||")) > 1:
            found.append(text[start:end])
        else:
            pending.extend(parts)
    return found


def _job_skipped(job: Any, event: str, budget: _Budget) -> bool:
    """True when the job's ``if`` skips it for the healer branch on ``event`` (documented forms only).

    The form must be the whole condition or one of its top-level ``&&`` conditions, exactly:
    negated again, compared (``== false``) or inside ``||`` it skips nothing.
    """
    budget.spend(1)
    condition = job.get("if") if isinstance(job, dict) else None
    conjuncts = _conjuncts(condition, budget) if isinstance(condition, str) else None
    if not conjuncts:
        return False
    if event in ("pull_request", "pull_request_target"):
        return any(_SKIPS_HEALER_PULL_REQUESTS.fullmatch(part) for part in conjuncts)
    if any(_SKIPS_HEALER_BRANCHES.fullmatch(part) for part in conjuncts):
        return True
    if event == "create":  # a job for tags only does not run for a new branch
        return any(_TAGS_ONLY.fullmatch(part) for part in conjuncts)
    return False


def _events(document: Mapping[Any, Any], budget: _Budget) -> dict[str, Any] | None:
    """The workflow's events (name -> configuration), from ``on`` (PyYAML loads the key as True)."""
    if "on" in document:
        on = document["on"]
    elif True in document:
        on = document[True]
    else:
        return None
    if isinstance(on, str):
        items: Iterable[tuple[Any, Any]] = [(on, None)]
    elif isinstance(on, list):
        budget.spend(len(on))
        items = ((item, None) for item in on)
    elif isinstance(on, dict):
        budget.spend(len(on))
        items = on.items()
    else:
        return None
    events: dict[str, Any] = {}
    for name, config in items:
        if isinstance(name, str):
            budget.spend(len(name))
            events[name.strip().lower()] = config
    return events


def _display_path(name: str) -> str:
    return _one_line(f"{_WORKFLOWS_DIR}/{name}", 300)


def _target_finding(path: str, types: list[str]) -> SetupFinding:
    """The error of a pull_request_target workflow that refers to the pull request's head.

    Whatever its activity types: one that runs only when a label is added or removed or the
    pull request is closed still runs the healer branch's code with the repository's secrets
    when a bot labels the pull request before the review, or a reviewer closes it to reject it.
    """
    shown = f" ({_one_line(', '.join(types), _DETAIL_CHARS)})" if types else ""
    fix = (
        "Never check out or run the pull request's head in a pull_request_target workflow, or skip its "
        "jobs for healer branches with if: ${{ !startsWith(github.head_ref, 'healer/') }}."
    )
    if types and set(types) <= set(_ON_REQUEST):
        when = " or ".join(_ON_REQUEST[kind] for kind in types)
        message = (
            f"The workflow {path} runs on pull_request_target{shown} and refers to the pull request's head: "
            f"when {when}, it runs the healer branch's code with the repository's secrets, whether or not a "
            "person has approved the change."
        )
    else:
        message = (
            f"The workflow {path} runs on pull_request_target{shown} and refers to the pull request's head: "
            "it can run the healer branch's code with the repository's secrets, whatever the commit message "
            "says."
        )
    return SetupFinding(WORKFLOWS_CHECK, ERROR, message, fix)


def _workflow_findings(
    path: str,
    document: Mapping[Any, Any],
    *,
    skip_ci: bool,
    base: str,
    branch: str,
    callees: _Callees | None = None,
) -> list[SetupFinding]:
    """The findings of one parsed workflow file (raises :class:`_TooLarge`)."""
    budget = _Budget()
    events = _events(document, budget)
    if not events:
        return []
    callees = callees or _Callees({})
    jobs = document.get("jobs")
    rest_flags = _scan(_rest(document, budget), budget)
    job_list = list(jobs.values()) if isinstance(jobs, dict) and jobs else None
    if job_list is None:
        job_flags = [_scan(jobs, budget)]
    else:
        job_flags = [
            _either(_scan(job, budget), callees.flags(_called_workflow(job, budget), budget))
            for job in job_list
        ]

    def what_runs(event: str) -> Flags | None:
        """(refers to the head, uses credentials) of what runs on ``event``; None when nothing runs."""
        if job_list is None:
            flags = job_flags
        else:
            flags = [
                f for job, f in zip(job_list, job_flags, strict=True) if not _job_skipped(job, event, budget)
            ]
            if not flags:
                return None
        return _either(*flags, rest_flags)

    findings: list[SetupFinding] = []
    if "pull_request_target" in events and _pull_request_runs(events["pull_request_target"], base, budget):
        runs = what_runs("pull_request_target")
        if runs is not None and runs[0]:
            findings.append(_target_finding(path, _activity_types(events["pull_request_target"], budget)))
    if "create" in events:
        runs = what_runs("create")
        if runs is not None:
            message = (
                f"The workflow {path} runs on create, which [skip ci] does not stop: it starts when "
                "dag-healer pushes its branch, with that branch's code checked out by default."
            )
            findings.append(
                SetupFinding(
                    WORKFLOWS_CHECK,
                    ERROR if runs[1] else WARN,
                    message + (" It uses the repository's secrets or write permissions." if runs[1] else ""),
                    "Skip its jobs for healer branches with if: ${{ !startsWith(github.ref, "
                    "'refs/heads/healer/') }}, or run it for tags only.",
                )
            )
    if not skip_ci:
        with_credentials: list[str] = []
        if "push" in events and _push_runs(events["push"], branch, budget):
            runs = what_runs("push")
            if runs is not None and runs[1]:
                with_credentials.append("push")
        if "pull_request" in events and _pull_request_runs(events["pull_request"], base, budget):
            runs = what_runs("pull_request")
            if runs is not None and runs[1]:
                with_credentials.append("pull_request")
        if with_credentials:
            findings.append(
                SetupFinding(
                    WORKFLOWS_CHECK,
                    WARN,
                    f"The workflow {path} runs on {' and '.join(with_credentials)} for healer branches and "
                    "uses the repository's secrets or write permissions, and github.skip_ci is false: CI "
                    "runs the model-written test and fix with them before review.",
                    "Set github.skip_ci to true, or skip the workflow for healer branches: branches-ignore: "
                    "['healer/**'] under push, and if: ${{ !startsWith(github.head_ref, 'healer/') }} on its "
                    "jobs for pull_request (branches-ignore under pull_request filters the base branch, not "
                    "the healer branch).",
                )
            )
    return findings


def _read_bytes(path: str) -> bytes | None:
    """The bytes of a regular file, None when it has more than :data:`MAX_WORKFLOW_BYTES`.

    Raises:
        OSError: the file cannot be opened, or it is no longer a regular file.
    """
    descriptor = os.open(path, _OPEN_FLAGS)
    try:
        info = os.fstat(descriptor)
        if not stat.S_ISREG(info.st_mode):
            raise OSError(f"not a regular file: {path}")
        if info.st_size > MAX_WORKFLOW_BYTES:
            return None
        chunks: list[bytes] = []
        left = MAX_WORKFLOW_BYTES + 1
        while left > 0:
            chunk = os.read(descriptor, min(left, 1 << 16))
            if not chunk:
                break
            chunks.append(chunk)
            left -= len(chunk)
        data = b"".join(chunks)
        return data if len(data) <= MAX_WORKFLOW_BYTES else None
    finally:
        os.close(descriptor)


def _size_text(size: int) -> str:
    """``2 MB`` for a whole number of megabytes, else the number of bytes."""
    return f"{size // 1_000_000} MB" if size >= 1_000_000 and size % 1_000_000 == 0 else f"{size} bytes"


def _load_workflow(path: str, room: int = MAX_WORKFLOW_TOTAL_BYTES) -> tuple[Any, str | None, int]:
    """(the parsed document, None, bytes read), or (None, why it was not checked, bytes read).

    ``why`` "" skips the file silently. ``room`` is how many bytes may still be read.
    """
    try:
        info = os.lstat(path)
    except OSError:
        return None, "", 0  # gone since the listing
    if stat.S_ISLNK(info.st_mode):
        return None, "a symbolic link", 0
    if not stat.S_ISREG(info.st_mode):
        return None, "", 0  # a folder or a FIFO named *.yml: never opened
    if info.st_size > MAX_WORKFLOW_BYTES:
        return None, "larger than 1 MB", 0
    if info.st_size > room:
        return None, f"not read: more than {_size_text(MAX_WORKFLOW_TOTAL_BYTES)} of workflow files", 0
    try:
        data = _read_bytes(path)
    except OSError:
        return None, "not readable", 0
    if data is None:
        return None, "larger than 1 MB", MAX_WORKFLOW_BYTES
    try:
        text = data.decode("utf-8-sig")
    except UnicodeDecodeError:
        return None, "not UTF-8", len(data)
    try:
        document = yaml.load(text, Loader=_WorkflowLoader)  # noqa: S506 - a safe loader with a budget
    except _TooLarge:
        return None, _TOO_COMPLEX, len(data)
    except Exception:  # noqa: BLE001 - any parser failure (RecursionError included) means not checked
        return None, "not valid YAML", len(data)
    if document is None:
        return None, "", len(data)  # an empty file runs nothing
    if not isinstance(document, dict):
        return None, "not a workflow", len(data)
    return document, None, len(data)


def _inside(path: str, root: str) -> bool:
    try:
        return os.path.commonpath([path, root]) == root
    except ValueError:  # another drive (Windows)
        return False


def check_workflows(
    root: str | os.PathLike[str], *, skip_ci: bool, base: str, branch: str | None = None
) -> SetupCheck:
    """Scan the workflow files of the repository at ``root`` (local files only, no network).

    Reports, for the direct children of ``.github/workflows`` whose name ends in ``.yml`` or
    ``.yaml`` (at most :data:`MAX_WORKFLOW_FILES`, holding at most
    :data:`MAX_WORKFLOW_TOTAL_BYTES` in all): ``pull_request_target`` workflows that refer to
    the pull request's head (an error, whatever their activity types), ``create`` workflows (a
    warning, an error when they use the repository's secrets or write permissions) and, with
    ``skip_ci`` false, the ``push`` and ``pull_request`` workflows that run for healer branches
    and use secrets or write permissions (a warning). A job that calls a reusable workflow of
    the folder (``uses: ./.github/workflows/x.yml``) counts what that workflow does. ``push``
    filters are matched against ``branch`` (default :data:`HEALER_BRANCH_EXAMPLE`),
    ``pull_request`` ones against ``base``. Links, files over :data:`MAX_WORKFLOW_BYTES` or
    beyond the total, files that are not UTF-8 or valid YAML, files too large or complex to
    load or analyse (past :data:`MAX_WORKFLOW_NODES` or :data:`MAX_WORKFLOW_SECONDS`), and a
    ``.github/workflows`` that is a link or leads out of ``root`` are reported in one "not
    checked" warning; nothing is ever followed out of the repository.
    """
    branch = branch or HEALER_BRANCH_EXAMPLE
    root_path = os.fspath(root)
    github_dir = os.path.join(root_path, ".github")
    folder = os.path.join(github_dir, "workflows")
    findings: list[SetupFinding] = []
    not_checked: list[str] = []
    names: list[str] = []
    if os.path.islink(github_dir) or os.path.islink(folder):
        not_checked.append(f"{_WORKFLOWS_DIR} (a symbolic link)")
    elif os.path.isdir(folder):
        if not _inside(os.path.realpath(folder), os.path.realpath(root_path)):
            not_checked.append(f"{_WORKFLOWS_DIR} (outside the repository)")
        else:
            try:
                with os.scandir(folder) as entries:
                    names = sorted(e.name for e in entries if e.name.lower().endswith((".yml", ".yaml")))
            except OSError:
                not_checked.append(f"{_WORKFLOWS_DIR} (not readable)")
    beyond = len(names) - MAX_WORKFLOW_FILES
    if beyond > 0:
        files = "1 file" if beyond == 1 else f"{beyond} files"
        not_checked.append(f"{_WORKFLOWS_DIR} ({files} beyond the first {MAX_WORKFLOW_FILES})")
        names = names[:MAX_WORKFLOW_FILES]
    documents: dict[str, Any] = {}
    unchecked: dict[str, str] = {}  # file name -> why, shown in name order
    room = MAX_WORKFLOW_TOTAL_BYTES
    for name in names:
        document, why, size = _load_workflow(os.path.join(folder, name), room)
        room -= size
        if why is None:
            documents[name] = document
        elif why:
            unchecked[name] = why
    callees = _Callees(documents)
    checked = 0
    for name, document in documents.items():
        try:
            findings += _workflow_findings(
                _display_path(name), document, skip_ci=skip_ci, base=base, branch=branch, callees=callees
            )
        except _TooLarge:
            unchecked[name] = _TOO_COMPLEX
        else:
            checked += 1
    not_checked += [f"{_display_path(name)} ({unchecked[name]})" for name in sorted(unchecked)]
    if not_checked:
        listing = ", ".join(not_checked[:_MAX_LISTED])
        if len(not_checked) > _MAX_LISTED:
            listing += f", and {len(not_checked) - _MAX_LISTED} more"
        findings.append(
            SetupFinding(
                WORKFLOWS_CHECK,
                WARN,
                f"Some workflow files were not checked: {listing}.",
                _CHECK_WORKFLOWS_BY_HAND,
            )
        )
    if checked == 0 and not names:
        summary = f"no workflow files in {_WORKFLOWS_DIR}"
    else:
        summary = f"{checked} workflow file{'' if checked == 1 else 's'} checked"
    return SetupCheck(WORKFLOWS_CHECK, summary, tuple(findings))


# --------------------------------------------------------------------------- all of it


def _api_checks(
    *,
    repo: str,
    base: str,
    token: str,
    skip_ci: bool,
    api_url: str,
    transport: httpx.BaseTransport | None,
    timeout: float,
) -> list[SetupCheck]:
    """The token, repository and base branch checks, each guarded on its own."""
    owner, name = repo.split("/", 1)
    repo_path = f"/repos/{quote(owner, safe='')}/{quote(name, safe='')}"
    headers = {**_headers(token), "Accept-Encoding": "identity"}
    with httpx.Client(
        headers=headers, timeout=timeout, transport=transport, follow_redirects=False
    ) as client:
        api = _Api(client, api_url, token, timeout)
        repo_answer = api.get(repo_path)
        return [
            _guarded(
                TOKEN_CHECK,
                "The GitHub token could not be checked",
                f"Check by hand that the token is a fine-grained token or a GitHub App limited to {repo}, "
                "without the workflow scope.",
                lambda: _token_check(api, token, repo, repo_answer),
            ),
            _guarded(
                REPOSITORY_CHECK,
                f"The token's access to {repo} could not be checked",
                f"Check by hand that the token's account has at most the Write role on {repo}.",
                lambda: _repository_check(repo, repo_answer),
            ),
            _guarded(
                BASE_BRANCH_CHECK,
                f"The rules of the base branch {base} could not be checked",
                _CHECK_BASE_BY_HAND.format(base=base),
                lambda: _base_branch_check(api, repo_path, base, skip_ci=skip_ci),
            ),
        ]


def check_github_setup(
    *,
    repo: str,
    base: str,
    token: str,
    skip_ci: bool,
    workflows_root: str | os.PathLike[str] | None,
    branch: str | None = None,
    api_url: str = DEFAULT_API_URL,
    transport: httpx.BaseTransport | None = None,
    timeout: float = CHECK_TIMEOUT,
) -> list[SetupCheck]:
    """Check the token, the repository, the base branch and (without network) the workflows.

    Args:
        repo: ``"owner/name"``.
        base: the base branch of the pull requests (``github.pr_base``).
        token: the GitHub token; it is sent to ``api_url`` only and never shown.
        skip_ci: ``github.skip_ci``: with it the base branch should require status checks, and
            the ``push`` / ``pull_request`` workflows are not reported (``[skip ci]`` stops them).
        workflows_root: the clone whose ``.github/workflows`` is scanned; None leaves the
            workflows out.
        branch: the healer branch (the ``push`` filters are matched against it).
        api_url: REST API root.
        transport: optional httpx transport (tests use ``httpx.MockTransport``).
        timeout: seconds per request operation.

    Returns:
        ``[token, repository, base branch, workflows]`` checks (workflows only with
        ``workflows_root``). It never raises: a check that fails unexpectedly is one warning of
        its own, and when the API checks cannot start at all (an invalid ``repo``, a token that
        is no valid header value), one ``setup`` check holding :func:`setup_failure` takes their
        place. The workflows are checked in every case.
    """
    try:
        checks = _api_checks(
            repo=repo,
            base=base,
            token=token,
            skip_ci=skip_ci,
            api_url=api_url,
            transport=transport,
            timeout=timeout,
        )
    except Exception as exc:  # noqa: BLE001 - the checks never block a pull request
        log.debug("the GitHub setup could not be checked (%s)", type(exc).__name__)  # never the message
        checks = [SetupCheck(SETUP_CHECK, "", (setup_failure(exc),))]
    if workflows_root is not None:
        checks.append(
            _guarded(
                WORKFLOWS_CHECK,
                "The workflow files could not be checked",
                _CHECK_WORKFLOWS_BY_HAND,
                lambda: check_workflows(workflows_root, skip_ci=skip_ci, base=base, branch=branch),
            )
        )
    return checks


__all__ = [
    "BASE_BRANCH_CHECK",
    "CHECK_TIMEOUT",
    "ERROR",
    "HEALER_BRANCH_EXAMPLE",
    "MAX_ANSWER_BYTES",
    "MAX_RULESETS",
    "MAX_WORKFLOW_BYTES",
    "MAX_WORKFLOW_FILES",
    "MAX_WORKFLOW_NODES",
    "MAX_WORKFLOW_SECONDS",
    "MAX_WORKFLOW_TOTAL_BYTES",
    "OK",
    "REPOSITORY_CHECK",
    "SETUP_CHECK",
    "TOKEN_CHECK",
    "WARN",
    "WORKFLOWS_CHECK",
    "SetupCheck",
    "SetupFinding",
    "check_github_setup",
    "check_workflows",
    "setup_failure",
    "setup_findings",
    "token_kind",
]
