"""The paths a fix never changes: one definition of dag-healer's protected-path policy.

The same policy is applied in three places: the repair agent's write tools refuse these
paths (:class:`dag_healer.agent.repair.RepairTools`), the final checks before and after the
fix is committed refuse a change that touches one of them, whatever wrote it, and
``repo.allowed_paths`` cannot name one (:class:`dag_healer.config.RepoConfig`).

Protected, by :func:`protected_reason`:

* ``.git`` internals, at any depth;
* dag-healer's configuration and inputs: ``healer.yaml`` at any depth, the root folders
  ``.github``, ``data``, ``fixtures`` and ``healer``, and the contracts folder;
* the CI definitions of other services (``.gitlab-ci.yml``, ``.gitlab/``, ``.circleci/``,
  ``Jenkinsfile``, ``azure-pipelines.yml``, ``.buildkite/``, ...) at the root of the
  repository;
* git hook managers (``.pre-commit-config.yaml``, ``.husky/``, ``.githooks/``, every
  ``lefthook*`` and ``.lefthook*`` file or folder, ``.overcommit.yml``, ...) and the
  configuration their hooks run (lint-staged, commitlint) at the root;
* development environments that run commands when someone opens the repository
  (``.devcontainer/``, ``.devcontainer.json``, ``.vscode/``, ``.gitpod.yml``) at the root;
* git metadata (``.gitattributes``, ``.gitmodules`` and ``.lfsconfig``, at any depth) and
  ``CODEOWNERS`` in the three places GitHub reads it (the root, ``docs/`` and ``.github/``);
* ``.env*`` files, lockfiles, dependency and packaging manifests, and data files;
* with ``linked``: what a protected path that is a symbolic link leads to inside the
  repository (:func:`linked_paths`), whole folders included, so ``.husky -> src/hooks``
  protects ``src/hooks/``.

Paths are repository-relative and compared case-insensitively, folded at least as Windows and
macOS fold names (``.GITHUB`` is ``.github``, and so is ``.gıthub`` with a dotless i;
``requirementſ.txt`` with a long s is ``requirements.txt``: :func:`fold_name`), ignoring the
trailing dots and spaces Windows drops from names (``.github.`` is ``.github``), so a variant
spelling cannot reach a protected path. ``..`` components are resolved first
(``src/../.github`` is ``.github``); a path that leaves the repository is protected. A folder
is protected exactly when the files under it are. :func:`compared_parts` gives the components
as the policy compares them, for the other deny checks (the repair tools' read rules).

Only the standard library is imported: :mod:`dag_healer.config` uses this module.
"""

from __future__ import annotations

import fnmatch
import os
import posixpath
import re
from collections.abc import Iterable, Mapping
from pathlib import Path

#: File extensions treated as data: never read, never written by the agent.
DATA_SUFFIXES = frozenset(
    {
        ".csv",
        ".tsv",
        ".parquet",
        ".pq",
        ".jsonl",
        ".ndjson",
        ".xlsx",
        ".xls",
        ".avro",
        ".orc",
        ".feather",
        ".arrow",
        ".pkl",
        ".pickle",
        ".db",
        ".sqlite",
        ".sqlite3",
        ".gz",
        ".bz2",
        ".zip",
    }
)
#: Root folders a fix never changes (GitHub's own folder, dag-healer's, data and fixtures).
DENIED_ROOTS = frozenset({".github", "data", "fixtures", "healer"})
#: dag-healer's configuration file, protected at any depth.
CONFIG_NAME = "healer.yaml"
#: The CI definitions of other services: root folders and root files.
CI_ROOT_DIRS = frozenset(
    {
        ".azure-pipelines",
        ".buildkite",
        ".builds",
        ".circleci",
        ".forgejo",
        ".gitea",
        ".gitlab",
        ".semaphore",
        ".teamcity",
        ".tekton",
        ".woodpecker",
        "buildkite",
    }
)
CI_ROOT_FILES = frozenset(
    {
        ".appveyor.yml",
        ".build.yml",
        ".cirrus.star",
        ".cirrus.yml",
        ".drone.jsonnet",
        ".drone.star",
        ".drone.yaml",
        ".drone.yml",
        ".gitlab-ci.yaml",
        ".gitlab-ci.yml",
        ".travis.yml",
        ".woodpecker.yaml",
        ".woodpecker.yml",
        "appveyor.yml",
        "azure-pipelines.yaml",
        "azure-pipelines.yml",
        "bitbucket-pipelines.yml",
        "buildkite.json",
        "buildkite.yaml",
        "buildkite.yml",
        "buildspec.yaml",
        "buildspec.yml",
        "cloudbuild.json",
        "cloudbuild.yaml",
        "cloudbuild.yml",
        "jenkinsfile",
    }
)
#: git hook managers and the configuration that their hooks run: root folders, root files, and
#: patterns matched against the first path component (the first two for ``.config/``):
#: lefthook reads ``lefthook.yml``, ``.lefthook-local.toml``, ``.config/lefthook.yaml`` and the
#: like, husky 4 ``.huskyrc*``, overcommit ``.overcommit.yml``, simple-git-hooks
#: ``.simple-git-hooks.json`` or ``simple-git-hooks.js`` and the like; hooks often run
#: lint-staged (``.lintstagedrc*``, ``lint-staged.config.*``) and commitlint
#: (``.commitlintrc*``, ``commitlint.config.*``).
HOOK_ROOT_DIRS = frozenset({".git-hooks", ".githooks", ".husky"})
HOOK_ROOT_FILES = frozenset({".overcommit.yml", ".pre-commit-config.yaml", ".pre-commit-config.yml"})
HOOK_ROOT_PATTERNS = (
    ".huskyrc*",
    ".lefthook*",
    "lefthook*",
    ".config/lefthook*",
    ".simple-git-hooks*",
    "simple-git-hooks*",
    ".lintstagedrc*",
    "lint-staged.config.*",
    ".commitlintrc*",
    "commitlint.config.*",
)
#: Development environments that run commands when the repository is opened: root folders and
#: root files (dev containers and Codespaces, VS Code tasks, Gitpod).
DEV_ROOT_DIRS = frozenset({".devcontainer", ".vscode"})
DEV_ROOT_FILES = frozenset({".devcontainer.json", ".gitpod.dockerfile", ".gitpod.yml"})
#: git metadata files, protected at any depth.
GIT_METADATA_NAMES = frozenset({".gitattributes", ".gitmodules", ".lfsconfig"})
#: The places GitHub reads the code owners from.
CODEOWNERS_PATHS = frozenset({"codeowners", "docs/codeowners", ".github/codeowners"})
LOCKFILES = frozenset(
    {
        "poetry.lock",
        "pipfile.lock",
        "uv.lock",
        "pdm.lock",
        "package-lock.json",
        "npm-shrinkwrap.json",
        "yarn.lock",
        "pnpm-lock.yaml",
        "cargo.lock",
        "gemfile.lock",
        "composer.lock",
    }
)
MANIFESTS = frozenset(
    {"pyproject.toml", "setup.py", "setup.cfg", "pipfile", "environment.yml", "tox.ini", "package.json"}
)
MANIFEST_PATTERNS = ("requirements*.txt", "requirements*.in", "constraints*.txt")
#: A path is resolved through at most this many symbolic links (the limit of most systems).
MAX_LINK_HOPS = 40

#: Why a path is protected (the texts the write tools show in parentheses).
GIT_INTERNALS = ".git internals"
HEALER_CONFIG = "protected path: healer config, contracts, CI, data or fixtures"
CI_CONFIG = "CI configuration"
HOOK_CONFIG = "git hook configuration"
DEV_ENV_CONFIG = "development environment configuration"
GIT_METADATA = "git metadata"
CODE_OWNERS = "code owners"
ENV_FILES = ".env files"
LOCKFILE = "lockfiles are never changed"
MANIFEST = "dependency and packaging manifests are never changed"
DATA_FILES = "data files"
OUTSIDE_REPOSITORY = "outside the repository"
LINK_TARGET = "a protected path links to it"

_ABSOLUTE_TARGET = re.compile(r"^(?:/|[A-Za-z]:)")


def normalise_prefix(value: str) -> str:
    """``value`` as a repository-relative posix path: ``./src/`` is ``src``, ``""`` and ``/`` are ``.``."""
    cleaned = str(value).replace("\\", "/").strip()
    while cleaned.startswith("./"):
        cleaned = cleaned[2:]
    cleaned = cleaned.strip("/")
    return posixpath.normpath(cleaned) if cleaned else "."


def contracts_prefix(
    contracts_dir: str | os.PathLike[str], repo_root: str | os.PathLike[str] | None = None
) -> str | None:
    """The contracts folder as a repository-relative prefix; None when it is outside the repository.

    An absolute ``contracts_dir`` counts when it lies inside ``repo_root`` (both resolved);
    without ``repo_root`` it is taken to be outside.
    """
    posix = str(contracts_dir).replace("\\", "/")
    if Path(contracts_dir).is_absolute():
        if repo_root is None:
            return None
        try:
            relative = Path(contracts_dir).resolve().relative_to(Path(repo_root).resolve())
        except (OSError, RuntimeError, ValueError):  # outside the repository, or unresolvable
            return None
        return normalise_prefix(relative.as_posix())
    if posix.startswith(".."):
        return None
    prefix = normalise_prefix(posix)
    return None if prefix == ".." or prefix.startswith("../") else prefix


def fold_name(name: str) -> str:
    """``name`` with its case folded, as case-insensitive file systems compare names, or more.

    ``upper()`` then ``casefold()``: APFS folds ``ſ`` (U+017F) to ``s`` and the Kelvin sign to
    ``k``, NTFS upper-cases ``ı`` (U+0131) to ``I``; ``lower()`` does neither. Folding more than
    a file system does only protects more names.
    """
    return name.upper().casefold()


def is_lockfile(name: str) -> bool:
    """True for a lockfile name (a known one, or any name ending in ``.lock``)."""
    folded = fold_name(name)
    return folded in LOCKFILES or folded.endswith(".lock")


def is_manifest(name: str) -> bool:
    """True for a dependency or packaging manifest name (``pyproject.toml``, ``requirements*.txt``, ...)."""
    folded = fold_name(name)
    return folded in MANIFESTS or any(fnmatch.fnmatchcase(folded, p) for p in MANIFEST_PATTERNS)


def is_data_file(name: str) -> bool:
    """True for a data file name (``.csv``, ``.parquet``, ``.jsonl``, ...)."""
    return Path(fold_name(name)).suffix in DATA_SUFFIXES


def compared_parts(path: str) -> tuple[str, ...] | None:
    """The components of ``path`` as the policy compares them; None when it leaves the repository.

    Each component is case-folded (:func:`fold_name`) and loses the trailing dots and spaces
    that Windows drops from names (``.github.`` is ``.github``; a component made of nothing
    else is dropped, and so are empty components and ``.``); ``..`` is resolved.
    """
    parts: list[str] = []
    for part in str(path).replace("\\", "/").split("/"):
        if part == "..":
            if not parts:
                return None
            parts.pop()
            continue
        cleaned = fold_name(part).rstrip(". ")
        if cleaned:
            parts.append(cleaned)
    return tuple(parts)


def _under(rel: tuple[str, ...], prefix: tuple[str, ...]) -> bool:
    """True when ``rel`` is ``prefix`` or under it (an empty prefix contains nothing)."""
    return bool(prefix) and rel[: len(prefix)] == prefix


def _matches_root_pattern(parts: tuple[str, ...], patterns: Iterable[str]) -> bool:
    """True when the first components of ``parts`` match one of ``patterns`` (``*`` stays in one)."""
    for pattern in patterns:
        depth = pattern.count("/") + 1
        if len(parts) >= depth and all(
            fnmatch.fnmatchcase(part, piece) for part, piece in zip(parts, pattern.split("/"), strict=False)
        ):
            return True
    return False


def _linked_reason(parts: tuple[str, ...], linked: Iterable[str]) -> str | None:
    for target in linked:
        target_parts = compared_parts(target)
        if target_parts is None:
            continue
        if not target_parts or _under(parts, target_parts):  # a link to the root protects it all
            return LINK_TARGET
    return None


def protected_reason(
    path: str, *, contracts_dir: str | None = None, linked: Iterable[str] = ()
) -> str | None:
    """Why a fix may never change ``path``, or None when the policy allows it.

    Args:
        path: A repository-relative path (``/`` or ``\\`` separators), a file or a folder.
        contracts_dir: The contracts folder as a repository-relative prefix
            (:func:`contracts_prefix`); None, ``""`` and ``"."`` protect nothing more.
        linked: Repository-relative paths that protected links lead to (:func:`linked_paths`);
            each one is protected with everything under it (``""`` is the root: everything).

    Returns:
        One of the reason constants of this module (the first rule that matches), or None.
    """
    parts = compared_parts(path)
    if parts is None:
        return OUTSIDE_REPOSITORY
    if not parts:
        return None
    first, name = parts[0], parts[-1]
    if ".git" in parts:
        return GIT_INTERNALS
    contracts = (compared_parts(contracts_dir) or ()) if contracts_dir else ()
    if name == CONFIG_NAME or first in DENIED_ROOTS or _under(parts, contracts):
        return HEALER_CONFIG
    if first in CI_ROOT_DIRS or (len(parts) == 1 and first in CI_ROOT_FILES):
        return CI_CONFIG
    if (
        first in HOOK_ROOT_DIRS
        or (len(parts) == 1 and first in HOOK_ROOT_FILES)
        or _matches_root_pattern(parts, HOOK_ROOT_PATTERNS)
    ):
        return HOOK_CONFIG
    if first in DEV_ROOT_DIRS or (len(parts) == 1 and first in DEV_ROOT_FILES):
        return DEV_ENV_CONFIG
    if name in GIT_METADATA_NAMES:
        return GIT_METADATA
    if "/".join(parts) in CODEOWNERS_PATHS:
        return CODE_OWNERS
    if name.startswith(".env"):
        return ENV_FILES
    if is_lockfile(name):
        return LOCKFILE
    if is_manifest(name):
        return MANIFEST
    if is_data_file(name):
        return DATA_FILES
    return _linked_reason(parts, linked)


def protected_changes(
    paths: Iterable[str], *, contracts_dir: str | None = None, linked: Iterable[str] = ()
) -> list[tuple[str, str]]:
    """``(path, reason)`` for every protected path of ``paths``, without duplicates, sorted by path."""
    linked = tuple(linked)
    found: dict[str, str] = {}
    for path in paths:
        if path in found:
            continue
        reason = protected_reason(path, contracts_dir=contracts_dir, linked=linked)
        if reason is not None:
            found[path] = reason
    return sorted(found.items())


def resolve_links(path: str, links: Mapping[str, str]) -> tuple[str | None, tuple[str, ...]]:
    """Where ``path`` leads inside the repository through its symbolic links.

    Components are followed one by one, as the operating system does: a link's target replaces
    the link, and ``..`` then goes up from where the target led.

    Args:
        path: A repository-relative posix path.
        links: The repository's symbolic links: repository-relative posix path -> target as
            stored in the link (relative to the link's folder; an absolute target leaves the
            repository). Only the targets of the links on the way are read.

    Returns:
        ``(the repository-relative path it leads to, "" for the root, or None when it leaves
        the repository or passes more than MAX_LINK_HOPS links; the links followed, in order)``.
    """
    pending = [part for part in path.split("/") if part not in ("", ".")]
    done: list[str] = []
    followed: list[str] = []
    while pending:
        part = pending.pop(0)
        if part == "..":
            if not done:
                return None, tuple(followed)
            done.pop()
            continue
        done.append(part)
        current = "/".join(done)
        if current not in links:
            continue
        if len(followed) >= MAX_LINK_HOPS:
            return None, tuple(followed)
        followed.append(current)
        target = str(links[current]).replace("\\", "/")
        if _ABSOLUTE_TARGET.match(target):
            return None, tuple(followed)
        done.pop()
        pending[:0] = [piece for piece in target.split("/") if piece not in ("", ".")]
    return "/".join(done), tuple(followed)


def linked_paths(links: Mapping[str, str], *, contracts_dir: str | None = None) -> frozenset[str]:
    """What the protected links of a repository lead to, and the links on the way there.

    A hook manager or a CI service that reads a protected file reads what its link leads to:
    ``.pre-commit-config.yaml -> src/ci/hooks.yaml`` makes ``src/ci/hooks.yaml`` part of the
    hook configuration. Each protected link (:func:`protected_reason`) is resolved through
    every link of ``links`` (:func:`resolve_links`); the path it leads to inside the repository
    (``""`` for the root) and every link followed on the way are returned, to be passed as
    ``linked`` to :func:`protected_reason`. A link that leads outside is left out: the write
    tools never write outside the repository.
    """
    found: set[str] = set()
    for link in links:
        if protected_reason(link, contracts_dir=contracts_dir) is None:
            continue
        real, followed = resolve_links(link, links)
        found.update(followed)
        if real is not None:
            found.add(real)
    return frozenset(found)


def allowed_path_reason(entry: str, *, contracts_dir: str | None = None) -> str | None:
    """Why ``entry`` cannot be a ``repo.allowed_paths`` entry, or None (``.`` is always allowed).

    ``contracts_dir`` (a repository-relative prefix) makes the contracts folder, and every
    folder inside it, a protected entry too.
    """
    prefix = normalise_prefix(entry)
    if prefix == ".":
        return None
    return protected_reason(prefix, contracts_dir=contracts_dir)


__all__ = [
    "CI_CONFIG",
    "CI_ROOT_DIRS",
    "CI_ROOT_FILES",
    "CODEOWNERS_PATHS",
    "CODE_OWNERS",
    "CONFIG_NAME",
    "DATA_FILES",
    "DATA_SUFFIXES",
    "DENIED_ROOTS",
    "DEV_ENV_CONFIG",
    "DEV_ROOT_DIRS",
    "DEV_ROOT_FILES",
    "ENV_FILES",
    "GIT_INTERNALS",
    "GIT_METADATA",
    "GIT_METADATA_NAMES",
    "HEALER_CONFIG",
    "HOOK_CONFIG",
    "HOOK_ROOT_DIRS",
    "HOOK_ROOT_FILES",
    "HOOK_ROOT_PATTERNS",
    "LINK_TARGET",
    "LOCKFILE",
    "LOCKFILES",
    "MANIFEST",
    "MANIFESTS",
    "MANIFEST_PATTERNS",
    "MAX_LINK_HOPS",
    "OUTSIDE_REPOSITORY",
    "allowed_path_reason",
    "compared_parts",
    "contracts_prefix",
    "fold_name",
    "is_data_file",
    "is_lockfile",
    "is_manifest",
    "linked_paths",
    "normalise_prefix",
    "protected_changes",
    "protected_reason",
    "resolve_links",
]
