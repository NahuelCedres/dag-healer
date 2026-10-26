"""Minimal GitHub REST client: open (or find) the pull request of a healer branch.

Only two endpoints are used here (``POST /repos/{owner}/{repo}/pulls`` and
``GET /repos/{owner}/{repo}/pulls?head=owner:branch``). The token is sent as a bearer
token header and never appears in exceptions or log records: GitHub's error texts are
redacted, and a request that fails before GitHub answers is named by its exception type
only (httpx can quote the request's headers in its own messages).

The read-only checks of how the repository and the token are set up (the token's kind and
scopes, the base branch's rules, the workflows) live in :mod:`dag_healer.pr.github_checks`,
which shares this module's request headers and error texts.
"""

from __future__ import annotations

import logging
import re
from typing import Any
from urllib.parse import quote

import httpx

from dag_healer._version import __version__

log = logging.getLogger(__name__)

DEFAULT_API_URL = "https://api.github.com"
GITHUB_API_VERSION = "2022-11-28"
DEFAULT_TIMEOUT = 15.0

_REPO_RE = re.compile(r"^[A-Za-z0-9_.-]+/[A-Za-z0-9_.-]+$")
_ALREADY_EXISTS = "a pull request already exists"


class GitHubError(RuntimeError):
    """A GitHub API call failed; ``status_code`` is None for transport errors."""

    def __init__(self, message: str, *, status_code: int | None = None) -> None:
        super().__init__(message)
        self.status_code = status_code


def _headers(token: str) -> dict[str, str]:
    return {
        "Accept": "application/vnd.github+json",
        "Authorization": f"Bearer {token}",
        "X-GitHub-Api-Version": GITHUB_API_VERSION,
        "User-Agent": f"dag-healer/{__version__}",
    }


def _json(response: httpx.Response) -> Any:
    """The JSON of an answer; None when it has none (nesting too deep to decode included)."""
    try:
        return response.json()
    except (ValueError, RecursionError):
        return None


def _error_text(response: httpx.Response, token: str) -> str:
    """Return GitHub's error message (``message`` + ``errors[].message``), token redacted."""
    return _error_message(_json(response), response.text or "", response.reason_phrase, token)


def _error_message(data: Any, text: str, reason: str, token: str) -> str:
    """GitHub's error message from an answer's JSON ``data``, else its ``text`` or ``reason``.

    The token is redacted first: the text is cut to 500 characters afterwards, so no part of
    the token is left.
    """

    def redacted(value: str) -> str:
        return value.replace(token, "***") if token else value

    parts: list[str] = []
    if isinstance(data, dict):
        if data.get("message"):
            parts.append(str(data["message"]))
        errors = data.get("errors")
        if not isinstance(errors, list):  # GitHub sends a list; anything else is one item
            errors = [errors] if errors else []
        for item in errors:
            if isinstance(item, dict):
                detail = item.get("message") or item.get("code")
                if detail:
                    parts.append(str(detail))
            elif item:
                parts.append(str(item))
    if parts:
        return redacted("; ".join(parts))
    return redacted(text).strip()[:500] or redacted(reason)


def _describe_failure(response: httpx.Response, token: str, repo: str) -> GitHubError:
    status = response.status_code
    detail = _error_text(response, token)
    if status == 401:
        message = f"GitHub authentication failed (401): {detail}. Check GITHUB_TOKEN."
    elif status == 403:
        message = (
            f"GitHub refused the request (403): {detail}. A fine-grained token needs Contents and "
            "Pull requests: read and write on the repository (a classic token: the repo scope), or "
            "the rate limit was hit."
        )
    elif status == 404:
        message = (
            f"GitHub repository {repo!r} not found (404): {detail}. Check github.repo and "
            "that the token can access it."
        )
    else:
        message = f"GitHub API error ({status}): {detail}"
    return GitHubError(message, status_code=status)


def _pulls_url(api_url: str, repo: str) -> str:
    owner, name = repo.split("/", 1)
    return f"{api_url.rstrip('/')}/repos/{quote(owner, safe='')}/{quote(name, safe='')}/pulls"


def _qualified_head(repo: str, head: str) -> str:
    """``branch`` -> ``owner:branch`` (the form the ``head`` list filter needs)."""
    return head if ":" in head else f"{repo.split('/', 1)[0]}:{head}"


def find_pull_request(
    client: httpx.Client,
    *,
    repo: str,
    head: str,
    token: str,
    api_url: str = DEFAULT_API_URL,
    state: str = "open",
) -> str | None:
    """Return the ``html_url`` of a pull request whose head is ``head`` (None if none)."""
    response = client.get(
        _pulls_url(api_url, repo),
        params={"head": _qualified_head(repo, head), "state": state, "per_page": 10},
    )
    if response.status_code != 200:
        raise _describe_failure(response, token, repo)
    data = _json(response)
    if isinstance(data, list):
        for item in data:
            if isinstance(item, dict) and item.get("html_url"):
                return str(item["html_url"])
    return None


def create_pull_request(
    *,
    repo: str,
    head: str,
    base: str,
    title: str,
    body: str,
    token: str,
    api_url: str = DEFAULT_API_URL,
    transport: httpx.BaseTransport | None = None,
    timeout: float = DEFAULT_TIMEOUT,
) -> str:
    """Open a pull request ``head`` -> ``base`` in ``repo`` and return its ``html_url``.

    If GitHub answers 422 "A pull request already exists", the existing open pull
    request for ``head`` is looked up and its URL returned instead.

    Args:
        repo: ``"owner/name"``.
        head: branch name (``owner:branch`` for a fork) containing the fix.
        base: branch the pull request targets.
        title: pull request title.
        body: Markdown description (GitHub limits it to 65536 characters).
        token: GitHub token with permission to create pull requests.
        api_url: REST API root (GitHub Enterprise: ``https://<host>/api/v3``).
        transport: optional httpx transport (tests use ``httpx.MockTransport``).
        timeout: per-request timeout in seconds.

    Raises:
        GitHubError: invalid arguments, authentication/permission problems, unknown
            repository, other API errors or network failures.
    """
    if not _REPO_RE.match(repo or ""):
        raise GitHubError(f"invalid GitHub repository {repo!r}: expected 'owner/name'")
    if not token:
        raise GitHubError("a GitHub token is required to open a pull request")
    if not head or not base:
        raise GitHubError("both head and base branches are required")
    payload = {"title": title, "head": head, "base": base, "body": body, "maintainer_can_modify": True}
    try:
        with httpx.Client(headers=_headers(token), timeout=timeout, transport=transport) as client:
            response = client.post(_pulls_url(api_url, repo), json=payload)
            if response.status_code == 201:
                data = _json(response) or {}
                url = data.get("html_url") if isinstance(data, dict) else None
                if not url:
                    raise GitHubError(
                        "GitHub created the pull request but returned no html_url", status_code=201
                    )
                log.info("opened pull request %s (%s -> %s)", url, head, base)
                return str(url)
            if response.status_code == 422 and _ALREADY_EXISTS in _error_text(response, token).lower():
                existing = find_pull_request(client, repo=repo, head=head, token=token, api_url=api_url)
                if existing:
                    log.info("pull request for %s already exists: %s", head, existing)
                    return existing
                raise GitHubError(
                    f"GitHub reports a pull request for {head!r} already exists but it could not be found",
                    status_code=422,
                )
            raise _describe_failure(response, token, repo)
    except httpx.HTTPError as exc:
        # The type only: httpx and h11 can quote the Authorization header (for example as the
        # repr of an invalid header value), which a plain replace() of the token would miss.
        raise GitHubError(f"could not reach the GitHub API at {api_url} ({type(exc).__name__})") from None


__all__ = ["DEFAULT_API_URL", "GitHubError", "create_pull_request", "find_pull_request"]
