"""Publishing a repair: GitHub pull requests (:mod:`.github`) and ``PR.md`` (:mod:`.prmd`).

:mod:`.github_checks` holds the read-only checks of how the repository and its GitHub token
are set up (``dag-healer doctor`` and the "Before you merge" section of a pull request).

Submodules are imported lazily so that ``import dag_healer.pr`` stays cheap; use
``from dag_healer.pr.github import create_pull_request``,
``from dag_healer.pr.github_checks import check_github_setup`` and
``from dag_healer.pr.prmd import render_pr_markdown, write_pr_artifacts``.
"""

from __future__ import annotations

import importlib
from typing import Any

_EXPORTS = {
    "GitHubError": "dag_healer.pr.github",
    "create_pull_request": "dag_healer.pr.github",
    "find_pull_request": "dag_healer.pr.github",
    "SetupCheck": "dag_healer.pr.github_checks",
    "SetupFinding": "dag_healer.pr.github_checks",
    "check_github_setup": "dag_healer.pr.github_checks",
    "check_workflows": "dag_healer.pr.github_checks",
    "setup_findings": "dag_healer.pr.github_checks",
    "token_kind": "dag_healer.pr.github_checks",
    "render_pr_markdown": "dag_healer.pr.prmd",
    "render_pr_title": "dag_healer.pr.prmd",
    "review_checklist": "dag_healer.pr.prmd",
    "write_pr_artifacts": "dag_healer.pr.prmd",
    "pr_artifacts_dir": "dag_healer.pr.prmd",
}

__all__ = sorted(_EXPORTS)


def __getattr__(name: str) -> Any:
    module = _EXPORTS.get(name)
    if module is None:
        raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
    return getattr(importlib.import_module(module), name)
