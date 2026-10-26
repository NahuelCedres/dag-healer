"""Tests for the protected-path policy (dag_healer.repo.protected_paths).

The policy is shared by the repair agent's write tools, the final check before a fix is
committed and the ``repo.allowed_paths`` validation; these tests pin its rules directly.
"""

from __future__ import annotations

from pathlib import Path

import pytest

from dag_healer.repo.protected_paths import (
    CI_CONFIG,
    CI_ROOT_DIRS,
    CI_ROOT_FILES,
    CODE_OWNERS,
    CODEOWNERS_PATHS,
    DATA_FILES,
    DATA_SUFFIXES,
    DENIED_ROOTS,
    DEV_ENV_CONFIG,
    DEV_ROOT_DIRS,
    DEV_ROOT_FILES,
    ENV_FILES,
    GIT_INTERNALS,
    GIT_METADATA,
    GIT_METADATA_NAMES,
    HEALER_CONFIG,
    HOOK_CONFIG,
    HOOK_ROOT_DIRS,
    HOOK_ROOT_FILES,
    LINK_TARGET,
    LOCKFILE,
    LOCKFILES,
    MANIFEST,
    MANIFESTS,
    MAX_LINK_HOPS,
    OUTSIDE_REPOSITORY,
    allowed_path_reason,
    compared_parts,
    contracts_prefix,
    fold_name,
    is_data_file,
    is_lockfile,
    is_manifest,
    linked_paths,
    normalise_prefix,
    protected_changes,
    protected_reason,
    resolve_links,
)

REASONS = [
    (".git/config", GIT_INTERNALS),
    ("src/.git/hooks/pre-commit", GIT_INTERNALS),
    ("healer.yaml", HEALER_CONFIG),
    ("src/etl/healer.yaml", HEALER_CONFIG),
    ("healer/contracts/customers.yaml", HEALER_CONFIG),
    (".github/workflows/ci.yml", HEALER_CONFIG),
    (".github/CODEOWNERS", HEALER_CONFIG),  # the first rule that matches wins
    ("data/helper.py", HEALER_CONFIG),
    ("fixtures/day2/x.py", HEALER_CONFIG),
    ("Jenkinsfile", CI_CONFIG),
    (".circleci/config.yml", CI_CONFIG),
    (".gitea/workflows/x.yml", CI_CONFIG),
    (".forgejo/workflows/x.yml", CI_CONFIG),
    (".gitlab-ci.yml", CI_CONFIG),
    (".pre-commit-config.yaml", HOOK_CONFIG),
    (".husky/pre-commit", HOOK_CONFIG),
    (".lefthook/x", HOOK_CONFIG),
    ("lefthook.yml", HOOK_CONFIG),
    # every lefthook configuration file, husky 4, and the folders core.hooksPath often names
    ("lefthook.yaml", HOOK_CONFIG),
    (".lefthook-local.yml", HOOK_CONFIG),
    ("lefthook-local.toml", HOOK_CONFIG),
    (".config/lefthook.yml", HOOK_CONFIG),
    (".huskyrc.json", HOOK_CONFIG),
    (".githooks/post-checkout", HOOK_CONFIG),
    (".git-hooks/pre-push", HOOK_CONFIG),
    (".pre-commit-config.yml", HOOK_CONFIG),
    # overcommit, simple-git-hooks, and the configuration that hooks often run
    (".overcommit.yml", HOOK_CONFIG),
    (".simple-git-hooks.json", HOOK_CONFIG),
    ("simple-git-hooks.cjs", HOOK_CONFIG),
    (".lintstagedrc", HOOK_CONFIG),
    (".lintstagedrc.yaml", HOOK_CONFIG),
    ("lint-staged.config.mjs", HOOK_CONFIG),
    (".commitlintrc.json", HOOK_CONFIG),
    ("commitlint.config.ts", HOOK_CONFIG),
    # CI definitions beyond GitHub's
    (".gitlab/ci/build.yml", CI_CONFIG),
    (".gitlab/CODEOWNERS", CI_CONFIG),
    (".woodpecker.yaml", CI_CONFIG),
    (".appveyor.yml", CI_CONFIG),
    ("azure-pipelines.yaml", CI_CONFIG),
    ("buildkite.yml", CI_CONFIG),
    ("buildkite/pipeline.yml", CI_CONFIG),
    ("cloudbuild.json", CI_CONFIG),
    (".cirrus.yml", CI_CONFIG),
    (".semaphore/semaphore.yml", CI_CONFIG),
    (".tekton/pull-request.yaml", CI_CONFIG),
    # development environments that run commands when the repository is opened
    (".devcontainer/devcontainer.json", DEV_ENV_CONFIG),
    (".devcontainer.json", DEV_ENV_CONFIG),
    (".vscode/tasks.json", DEV_ENV_CONFIG),
    (".gitpod.yml", DEV_ENV_CONFIG),
    (".gitattributes", GIT_METADATA),
    ("a/b/c/.gitattributes", GIT_METADATA),
    (".gitmodules", GIT_METADATA),
    (".lfsconfig", GIT_METADATA),
    ("CODEOWNERS", CODE_OWNERS),
    ("CodeOwners", CODE_OWNERS),
    ("docs/CODEOWNERS", CODE_OWNERS),
    ("Docs/codeOwners", CODE_OWNERS),
    (".env", ENV_FILES),
    ("src/.env.local", ENV_FILES),
    ("poetry.lock", LOCKFILE),
    ("src/custom.lock", LOCKFILE),
    ("requirements-dev.txt", MANIFEST),
    ("src/pyproject.toml", MANIFEST),
    ("ui/package.json", MANIFEST),
    ("src/etl/sample.csv", DATA_FILES),
    ("tests/data.jsonl", DATA_FILES),
]


@pytest.mark.parametrize(("path", "reason"), REASONS, ids=[path for path, _ in REASONS])
def test_protected_paths_and_their_reasons(path: str, reason: str) -> None:
    assert protected_reason(path) == reason


@pytest.mark.parametrize(
    ("path", "reason"),
    [
        (".GitHub/workflows/x.yml", HEALER_CONFIG),
        ("DATA/x.py", HEALER_CONFIG),
        ("HEALER.YAML", HEALER_CONFIG),
        (".GIT/config", GIT_INTERNALS),
        (".CircleCI/config.yml", CI_CONFIG),
        ("JENKINSFILE", CI_CONFIG),
        ("src/.GitAttributes", GIT_METADATA),
        ("src/Data.CSV", DATA_FILES),
    ],
    ids=["github", "data", "config", "git", "circleci", "jenkinsfile", "gitattributes", "csv"],
)
def test_the_policy_ignores_case(path: str, reason: str) -> None:
    assert protected_reason(path) == reason


@pytest.mark.parametrize(
    ("path", "reason"),
    [
        ("src/requirementſ.txt", MANIFEST),  # a long s: APFS and NTFS read requirements.txt
        (".huſky/pre-commit", HOOK_CONFIG),
        ("src/.gitattributeſ", GIT_METADATA),
        (".gıthub/workflows/x.yml", HEALER_CONFIG),  # a dotless i: NTFS upper-cases it to I
        ("JenKinsfile", CI_CONFIG),  # the Kelvin sign: APFS folds it to k
        ("src/requirementß.txt", MANIFEST),  # folding more than a file system protects more
    ],
    ids=["long-s", "long-s-folder", "long-s-metadata", "dotless-i", "kelvin", "sharp-s"],
)
def test_the_policy_folds_names_as_case_insensitive_file_systems_do(path: str, reason: str) -> None:
    assert protected_reason(path) == reason


def test_fold_name_and_compared_parts() -> None:
    assert fold_name("Requirementſ.TXT") == "requirements.txt"
    assert fold_name(".GıtHub") == ".github"
    assert fold_name("K") == "k"
    assert compared_parts("./SRC//Data.ſ./../.Huſky./x ") == ("src", ".husky", "x")
    assert compared_parts("../x") is None
    assert compared_parts(".") == ()
    assert is_data_file("sample.cſv") and is_manifest("requirementſ.txt") and is_lockfile("x.LOCK")


@pytest.mark.parametrize(
    ("path", "reason"),
    [
        (".github./x.yml", HEALER_CONFIG),
        (".github ./x.yml", HEALER_CONFIG),
        ("healer.yaml.", HEALER_CONFIG),
        (".env ", ENV_FILES),
        ("Jenkinsfile.", CI_CONFIG),
        (".git./config", GIT_INTERNALS),
        ("src/.gitattributes . ", GIT_METADATA),
        ("poetry.lock.", LOCKFILE),
        ("src/x.csv.", DATA_FILES),
    ],
    ids=["github", "github-space", "config", "env", "jenkinsfile", "git", "gitattributes", "lock", "csv"],
)
def test_trailing_dots_and_spaces_are_ignored(path: str, reason: str) -> None:
    assert protected_reason(path) == reason


@pytest.mark.parametrize(
    ("path", "reason"),
    [
        (".github\\workflows\\ci.yml", HEALER_CONFIG),
        ("src\\.gitattributes", GIT_METADATA),
        ("./.husky//pre-commit", HOOK_CONFIG),
        ("docs/./CODEOWNERS", CODE_OWNERS),
    ],
    ids=["backslashes", "nested-backslash", "dot-and-empty", "dot-inside"],
)
def test_separators_and_dot_components(path: str, reason: str) -> None:
    assert protected_reason(path) == reason


@pytest.mark.parametrize(
    "path",
    [
        "src/.github/x.yml",
        "src/CODEOWNERS",
        "src/docs/CODEOWNERS",
        "src/data/x.py",
        "src/fixtures/x.py",
        "src/Jenkinsfile",
        "src/.circleci/config.yml",
        "src/.husky/pre-commit",
        "src/lefthook.yml",
        "src/.devcontainer/x.json",
        "src/.vscode/settings.json",
        "src/.config/lefthook.yml",
        "src/.overcommit.yml",
        "src/commitlint.config.js",
        "src/.lintstagedrc",
        ".config/app.yml",
        "src/buildkite/x.py",
        "src/app.py",
        "tests/test_x.py",
        "docs/index.md",
        ".gitignore",
        "src",
        ".",
        "",
    ],
    ids=lambda value: value or "empty",
)
def test_paths_that_are_not_protected(path: str) -> None:
    assert protected_reason(path) is None


def test_every_listed_name_is_protected_where_it_counts() -> None:
    for root in DENIED_ROOTS:
        assert protected_reason(f"{root}/x.py") == HEALER_CONFIG, root
        assert protected_reason(root) == HEALER_CONFIG, root
        assert protected_reason(f"src/{root}/x.py") is None, root
    for folder in CI_ROOT_DIRS:
        assert protected_reason(f"{folder}/x.yml") == CI_CONFIG, folder
        assert protected_reason(folder.upper()) == CI_CONFIG, folder
        assert protected_reason(f"src/{folder}/x.yml") is None, folder
    for name in CI_ROOT_FILES:
        assert protected_reason(name) == CI_CONFIG, name
        assert protected_reason(name.upper()) == CI_CONFIG, name
        assert protected_reason(f"src/{name}") is None, name
    for folder in HOOK_ROOT_DIRS:
        assert protected_reason(f"{folder}/pre-commit") == HOOK_CONFIG, folder
        assert protected_reason(f"src/{folder}/pre-commit") is None, folder
    for folder in DEV_ROOT_DIRS:
        assert protected_reason(f"{folder}/x.json") == DEV_ENV_CONFIG, folder
        assert protected_reason(f"src/{folder}/x.json") is None, folder
    for name in DEV_ROOT_FILES:
        assert protected_reason(name) == DEV_ENV_CONFIG, name
        assert protected_reason(f"src/{name}") is None, name
    for name in HOOK_ROOT_FILES:
        assert protected_reason(name) == HOOK_CONFIG, name
        assert protected_reason(name.upper()) == HOOK_CONFIG, name
        assert protected_reason(f"src/{name}") is None, name
    for name in GIT_METADATA_NAMES:
        assert protected_reason(name) == GIT_METADATA, name
        assert protected_reason(f"src/etl/deep/{name}") == GIT_METADATA, name
    for path in CODEOWNERS_PATHS - {".github/codeowners"}:
        assert protected_reason(path.upper()) == CODE_OWNERS, path
    for name in LOCKFILES:
        assert protected_reason(f"src/{name}") == LOCKFILE, name
    for name in MANIFESTS:
        assert protected_reason(f"src/{name}") == MANIFEST, name
    for suffix in DATA_SUFFIXES:
        assert protected_reason(f"src/sample{suffix}") == DATA_FILES, suffix


def test_the_contracts_folder_is_protected_when_it_is_inside_the_repository() -> None:
    assert protected_reason("src/contracts/x.yaml", contracts_dir="src/contracts") == HEALER_CONFIG
    assert protected_reason("SRC/Contracts/x.yaml", contracts_dir="src/contracts") == HEALER_CONFIG
    assert protected_reason("src/contracts", contracts_dir="src/contracts/") == HEALER_CONFIG
    assert protected_reason("src/contracts_old/x.yaml", contracts_dir="src/contracts") is None
    for nothing in (None, "", "."):
        assert protected_reason("src/contracts/x.yaml", contracts_dir=nothing) is None, nothing
    assert contracts_prefix("healer/contracts") == "healer/contracts"
    assert contracts_prefix("./src/contracts/") == "src/contracts"
    assert contracts_prefix(Path("src") / "contracts") == "src/contracts"
    assert contracts_prefix("src\\contracts") == "src/contracts"
    assert contracts_prefix(".") == "."
    assert contracts_prefix("../shared/contracts") is None
    assert contracts_prefix("a/../../contracts") is None
    assert contracts_prefix(Path.cwd() / "contracts") is None  # absolute on every platform


def test_an_absolute_contracts_folder_counts_inside_the_repository(tmp_path: Path) -> None:
    repo = tmp_path / "repo"
    (repo / "src" / "contracts").mkdir(parents=True)
    assert contracts_prefix(repo / "src" / "contracts", repo) == "src/contracts"
    assert contracts_prefix(str(repo / "src" / ".." / "contracts"), repo) == "contracts"
    assert contracts_prefix(tmp_path / "elsewhere", repo) is None
    assert contracts_prefix(repo, repo) == "."


def test_protected_changes_are_deduplicated_and_sorted() -> None:
    paths = [
        "src/app.py",
        "src/.gitattributes",
        ".github/workflows/ci.yml",
        "src/.gitattributes",
        "Jenkinsfile",
    ]
    assert protected_changes(paths) == [
        (".github/workflows/ci.yml", HEALER_CONFIG),
        ("Jenkinsfile", CI_CONFIG),
        ("src/.gitattributes", GIT_METADATA),
    ]
    assert protected_changes(["x/contracts/a.yaml"], contracts_dir="x/contracts") == [
        ("x/contracts/a.yaml", HEALER_CONFIG)
    ]
    assert protected_changes([]) == [] and protected_changes(["src/app.py", "tests/test_app.py"]) == []


@pytest.mark.parametrize(
    ("entry", "reason"),
    [
        (".", None),
        ("./", None),
        ("/", None),
        ("", None),
        ("src/", None),
        ("docs/", None),
        ("src/.github", None),
        (".github/", HEALER_CONFIG),
        (".circleci", CI_CONFIG),
        ("data", HEALER_CONFIG),
        ("./.husky/", HOOK_CONFIG),
        ("Jenkinsfile", CI_CONFIG),
        ("src/../.github", HEALER_CONFIG),
        ("../elsewhere", OUTSIDE_REPOSITORY),
        (".vscode/", DEV_ENV_CONFIG),
        (".hu\u017fky", HOOK_CONFIG),
    ],
    ids=[
        "dot",
        "dot-slash",
        "slash",
        "empty",
        "src",
        "docs",
        "nested-github",
        "github",
        "circleci",
        "data",
        "husky",
        "jenkinsfile",
        "dot-dot",
        "outside",
        "vscode",
        "long-s",
    ],
)
def test_allowed_path_reason(entry: str, reason: str | None) -> None:
    assert allowed_path_reason(entry) == reason


def test_allowed_paths_cannot_name_the_contracts_folder() -> None:
    assert allowed_path_reason("contracts/", contracts_dir="contracts") == HEALER_CONFIG
    assert allowed_path_reason("contracts/orders", contracts_dir="contracts") == HEALER_CONFIG
    assert allowed_path_reason("src", contracts_dir="src/contracts") is None  # the folder stays protected
    assert allowed_path_reason(".", contracts_dir="contracts") is None


@pytest.mark.parametrize(
    ("path", "reason"),
    [
        ("src/../.github/workflows/x.yml", HEALER_CONFIG),
        ("src/etl/../../Jenkinsfile", CI_CONFIG),
        ("../x.py", OUTSIDE_REPOSITORY),
        ("src/../../x.py", OUTSIDE_REPOSITORY),
        ("src/../src/app.py", None),
    ],
    ids=["github", "jenkinsfile", "outside", "outside-deeper", "back-in"],
)
def test_parent_components_are_resolved_first(path: str, reason: str | None) -> None:
    assert protected_reason(path) == reason


def test_links_are_followed_component_by_component() -> None:
    links = {
        ".husky": "src/hooks",
        "src/hooks": "../tools/hooks",  # a link on the way
        "src/up": "../..",
        "loop/a": "b",
        "loop/b": "a",
        "abs": "/etc",
        "win": "C:\\Windows",
        "rel": "./src/./app.py",
    }
    assert resolve_links(".husky/pre-commit", links) == ("tools/hooks/pre-commit", (".husky", "src/hooks"))
    assert resolve_links("src/hooks/../app.py", links) == (
        "tools/app.py",
        ("src/hooks",),
    )  # .. after the link
    assert resolve_links("src/up/x", links) == (None, ("src/up",))
    assert resolve_links("loop/a", links) == (None, ("loop/a", "loop/b") * (MAX_LINK_HOPS // 2))
    assert resolve_links("abs/passwd", links) == (None, ("abs",))
    assert resolve_links("win/x", links) == (None, ("win",))
    assert resolve_links("rel", links) == ("src/app.py", ("rel",))
    assert resolve_links("plain/file.py", links) == ("plain/file.py", ())
    assert resolve_links(".", {}) == ("", ())


def test_what_protected_links_lead_to_is_protected() -> None:
    links = {
        ".pre-commit-config.yaml": "src/ci/pre-commit.yaml",
        "Jenkinsfile": "ci/Jenkinsfile",
        "ci": "src/ci",  # the Jenkinsfile link goes on through this one
        ".husky": "src/hooks",
        ".github/workflows/x.yml": "../../../outside.yml",
        "src/settings.yaml": "../config.yaml",  # not protected: what it leads to is not either
        "docs/codeowners-link": "../CODEOWNERS",
    }
    linked = linked_paths(links)
    assert linked == frozenset(
        {
            ".pre-commit-config.yaml",
            "src/ci/pre-commit.yaml",
            "Jenkinsfile",
            "ci",
            "src/ci/Jenkinsfile",
            ".husky",
            "src/hooks",
            ".github/workflows/x.yml",
        }
    )
    assert protected_reason("src/ci/pre-commit.yaml", linked=linked) == LINK_TARGET
    assert protected_reason("SRC/CI/Jenkinsfile", linked=linked) == LINK_TARGET  # case-insensitive
    assert protected_reason("src/hooks/post-checkout", linked=linked) == LINK_TARGET  # a whole folder
    assert protected_reason("src/hooks", linked=linked) == LINK_TARGET
    assert protected_reason("src/ci/other.py", linked=linked) is None
    assert protected_reason("config.yaml", linked=linked) is None
    assert protected_changes(["src/hooks/x", "src/app.py"], linked=linked) == [("src/hooks/x", LINK_TARGET)]
    # a protected link to the root protects everything
    root = linked_paths({".husky": "."})
    assert "" in root and protected_reason("src/app.py", linked=root) == LINK_TARGET
    assert linked_paths({}) == frozenset() and linked_paths({"src/a": "b"}) == frozenset()


@pytest.mark.parametrize(
    ("value", "expected"),
    [
        ("./src/", "src"),
        ("src\\etl", "src/etl"),
        ("/", "."),
        ("", "."),
        (" ./a/./b/ ", "a/b"),
        ("a/../b", "b"),
    ],
    ids=["dot-slash", "backslash", "slash", "empty", "spaces", "dot-dot"],
)
def test_normalise_prefix(value: str, expected: str) -> None:
    assert normalise_prefix(value) == expected


def test_name_helpers() -> None:
    assert is_lockfile("Poetry.lock") and is_lockfile("anything.lock") and not is_lockfile("lock.py")
    assert is_manifest("Requirements-Dev.txt") and is_manifest("constraints.txt") and is_manifest("setup.cfg")
    assert not is_manifest("requirements.py")
    assert is_data_file("x.Parquet") and is_data_file("dump.sqlite3") and not is_data_file("x.py")
