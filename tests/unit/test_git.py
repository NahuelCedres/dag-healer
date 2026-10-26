"""Tests for the git workspace helpers (dag_healer.repo.git) on real temporary repositories."""

from __future__ import annotations

import hashlib
import logging
import os
import shlex
import stat
import subprocess
from pathlib import Path

import pytest

from dag_healer.repo import git as gitmod
from dag_healer.repo.git import (
    ASKPASS_SCRIPT,
    ASKPASS_TOKEN_ENV,
    GIT_SAFE_CONFIG,
    GitError,
    Workspace,
    askpass_env,
    detect_eol,
    empty_hooks_dir,
    git_environment,
    github_https_url,
    is_git_repo,
    origin_url,
    parse_github_repo,
    read_text,
    run_git,
    sanitize_branch_name,
    write_text_preserving_eol,
)

IDENTITY = {
    "GIT_AUTHOR_NAME": "Repo Owner",
    "GIT_AUTHOR_EMAIL": "owner@example.com",
    "GIT_COMMITTER_NAME": "Repo Owner",
    "GIT_COMMITTER_EMAIL": "owner@example.com",
}
CRLF_SOURCE = b"def legacy():\r\n    return 1\r\n\r\n\r\ndef other():\r\n    return 2\r\n"
#: Index of the first git argument of a command built by run_git: "git", GIT_SAFE_CONFIG, then
#: "-c core.hooksPath=<empty dir>", "-c core.fsmonitor=false", "-c core.useReplaceRefs=false" and
#: "-c core.commitGraph=false".
FIRST_ARG = 1 + len(GIT_SAFE_CONFIG) + 8


def _args(cmd: list[str]) -> list[str]:
    """The git arguments of a command built by run_git (its configuration prefix removed)."""
    return cmd[FIRST_ARG:]


@pytest.fixture(autouse=True)
def isolated_git(monkeypatch: pytest.MonkeyPatch, tmp_path_factory: pytest.TempPathFactory) -> Path:
    """Ignore the host's git configuration; enable autocrlf to prove it is overridden."""
    config = tmp_path_factory.mktemp("gitconfig") / "gitconfig"
    config.write_text("[core]\n\tautocrlf = true\n[init]\n\tdefaultBranch = master\n", encoding="utf-8")
    monkeypatch.setenv("GIT_CONFIG_GLOBAL", str(config))
    monkeypatch.setenv("GIT_CONFIG_NOSYSTEM", "1")
    for name in ("GIT_DIR", "GIT_WORK_TREE", "GIT_INDEX_FILE"):
        monkeypatch.delenv(name, raising=False)
    return config


def _write(root: Path, files: dict[str, str | bytes]) -> None:
    for rel, content in files.items():
        target = root / rel
        target.parent.mkdir(parents=True, exist_ok=True)
        if isinstance(content, bytes):
            target.write_bytes(content)
        else:
            target.write_bytes(content.encode("utf-8"))


def _commit(root: Path, message: str = "commit") -> str:
    run_git(["add", "-A"], root)
    run_git(["commit", "-q", "-m", message], root, env=IDENTITY)
    return run_git(["rev-parse", "HEAD"], root).stdout.strip()


@pytest.fixture
def source_repo(tmp_path: Path) -> Path:
    """A user repository on ``main`` with one commit plus uncommitted working-tree changes."""
    root = tmp_path / "user repo"  # a space in the path, like the real project
    root.mkdir()
    run_git(["init", "-q", "-b", "main"], root)
    _write(
        root,
        {
            "README.md": "committed readme\n",
            "dags/sales.py": "from etl.sales import load\n",
            "src/etl/sales.py": "def load():\n    return 'v1'\n",
            "src/etl/legacy.py": CRLF_SOURCE,
            ".gitignore": "__pycache__/\n*.pyc\n",
        },
    )
    _commit(root, "initial")
    # Uncommitted state that must NOT reach a clone.
    (root / "README.md").write_bytes(b"uncommitted edit\n")
    _write(root, {"data/incoming/day2.csv": "a,b\n1,2\n"})
    return root


@pytest.fixture
def workspace(source_repo: Path, tmp_path: Path) -> Workspace:
    return Workspace.clone(source_repo, tmp_path / "work" / "inc1", branch="main")


# --------------------------------------------------------------------------- run_git


def test_run_git_always_passes_safe_config(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    seen: list[list[str]] = []

    def fake_run(cmd, **kwargs):
        seen.append(cmd)
        return subprocess.CompletedProcess(cmd, 0, b"ok\r\n", b"")

    monkeypatch.setattr(gitmod.subprocess, "run", fake_run)
    result = run_git(["status"], tmp_path)
    assert seen[0][:7] == [
        "git",
        "-c",
        "safe.directory=*",
        "-c",
        "core.autocrlf=false",
        "-c",
        "core.filemode=false",
    ]
    assert seen[0][7:FIRST_ARG] == [
        "-c",
        f"core.hooksPath={empty_hooks_dir().as_posix()}",
        "-c",
        "core.fsmonitor=false",
        "-c",
        "core.useReplaceRefs=false",
        "-c",
        "core.commitGraph=false",
    ]
    assert _args(seen[0]) == ["status"]
    assert result.stdout == "ok\r\n"  # no newline translation


def test_run_git_raises_with_stderr(tmp_path: Path) -> None:
    with pytest.raises(GitError) as info:
        run_git(["rev-parse", "HEAD"], tmp_path)
    assert info.value.returncode not in (None, 0)
    assert "rev-parse" in str(info.value)
    assert run_git(["rev-parse", "HEAD"], tmp_path, check=False).returncode != 0


def test_run_git_missing_cwd(tmp_path: Path) -> None:
    with pytest.raises(GitError, match="does not exist"):
        run_git(["status"], tmp_path / "missing")


def test_git_environment_drops_repository_overrides(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("GIT_DIR", "/elsewhere/.git")
    monkeypatch.setenv("GIT_WORK_TREE", "/elsewhere")
    monkeypatch.setenv("GIT_GRAFT_FILE", "/elsewhere/grafts")
    env = git_environment({"EXTRA": "1", "PATH_TO_DROP": None})
    assert "GIT_DIR" not in env and "GIT_WORK_TREE" not in env
    assert env["GIT_TERMINAL_PROMPT"] == "0"
    assert env["GIT_NO_REPLACE_OBJECTS"] == "1"
    if os.name == "nt":
        assert Path(env["GIT_GRAFT_FILE"]) == empty_hooks_dir() / gitmod.NO_GRAFTS_NAME
    else:  # /dev/null is no folder: nobody can create the file
        assert env["GIT_GRAFT_FILE"] == os.path.join(os.devnull, gitmod.NO_GRAFTS_NAME)
    assert env["GIT_GRAFT_FILE"] == gitmod.no_grafts_file()
    assert not Path(env["GIT_GRAFT_FILE"]).exists()
    assert env["EXTRA"] == "1"
    assert "PATH_TO_DROP" not in env


# --------------------------------------------------------------------------- clone


def test_clone_contains_committed_head_only(source_repo: Path, workspace: Workspace) -> None:
    source_head = run_git(["rev-parse", "HEAD"], source_repo).stdout.strip()
    assert workspace.head_sha() == source_head
    assert workspace.current_branch() == "main"
    assert workspace.base_branch == "main"
    # Uncommitted and untracked changes of the source working tree are not cloned.
    assert (workspace.path / "README.md").read_bytes() == b"committed readme\n"
    assert not (workspace.path / "data" / "incoming" / "day2.csv").exists()
    assert workspace.changed_files() == []
    # The source keeps its own uncommitted state.
    assert (source_repo / "README.md").read_bytes() == b"uncommitted edit\n"


def test_clone_keeps_crlf_bytes_despite_global_autocrlf(workspace: Workspace) -> None:
    assert (workspace.path / "src" / "etl" / "legacy.py").read_bytes() == CRLF_SOURCE


def test_clone_falls_back_to_default_branch(source_repo: Path, tmp_path: Path) -> None:
    ws = Workspace.clone(source_repo, tmp_path / "ws", branch="does-not-exist")
    assert ws.current_branch() == "main"


def test_clone_from_file_url(source_repo: Path, tmp_path: Path) -> None:
    ws = Workspace.clone(source_repo.resolve().as_uri(), tmp_path / "ws-url", branch="main")
    assert (ws.path / "src" / "etl" / "sales.py").is_file()


def test_clone_refuses_non_empty_destination(source_repo: Path, tmp_path: Path) -> None:
    dest = tmp_path / "busy"
    dest.mkdir()
    (dest / "file.txt").write_text("x", encoding="utf-8")
    with pytest.raises(GitError, match="not empty"):
        Workspace.clone(source_repo, dest, branch="main")


def test_clone_into_existing_empty_directory(source_repo: Path, tmp_path: Path) -> None:
    dest = tmp_path / "empty"
    dest.mkdir()
    ws = Workspace.clone(source_repo, dest, branch="main")
    assert (ws.path / ".git").exists()


# --------------------------------------------------------------------------- edit + diff + commit


def test_branch_diff_stats_and_commit(workspace: Workspace) -> None:
    base_sha = workspace.head_sha()
    workspace.create_branch("healer/sales-load-abcd1234")
    assert workspace.current_branch() == "healer/sales-load-abcd1234"

    write_text_preserving_eol(workspace.path / "src/etl/sales.py", "def load():\n    return 'v2'\n")
    write_text_preserving_eol(
        workspace.path / "tests/test_sales.py",
        "from src.etl.sales import load\n\n\ndef test_load():\n    assert load() == 'v2'\n",
    )
    assert workspace.changed_files() == ["src/etl/sales.py", "tests/test_sales.py"]

    diff = workspace.diff()
    assert "diff --git a/src/etl/sales.py b/src/etl/sales.py" in diff
    assert "-    return 'v1'" in diff and "+    return 'v2'" in diff
    # New untracked files are part of the diff.
    assert "diff --git a/tests/test_sales.py b/tests/test_sales.py" in diff
    assert "new file mode 100644" in diff
    assert "+def test_load():" in diff
    assert workspace.diff_stats() == (2, 2 + 5)

    sha = workspace.commit_all(
        "fix(sales): repair load", author_name="dag-healer", author_email="healer@example.com"
    )
    assert sha == workspace.head_sha() != base_sha
    assert workspace.changed_files() == []
    assert workspace.diff_stats() == (0, 0)
    log = workspace.git("log", "-1", "--format=%an <%ae>|%cn|%s").stdout.strip()
    assert log == "dag-healer <healer@example.com>|dag-healer|fix(sales): repair load"
    # The committed diff against the base matches what was reviewed.
    assert workspace.diff(base_sha) == diff


def test_changed_files_reports_deletions_and_ignores_ignored(workspace: Workspace) -> None:
    (workspace.path / "dags" / "sales.py").unlink()
    (workspace.path / "src" / "etl" / "__pycache__").mkdir()
    (workspace.path / "src" / "etl" / "__pycache__" / "sales.cpython-313.pyc").write_bytes(b"\0")
    assert workspace.changed_files() == ["dags/sales.py"]


def test_commit_all_without_changes_raises(workspace: Workspace) -> None:
    with pytest.raises(GitError, match="nothing to commit"):
        workspace.commit_all("empty", author_name="a", author_email="a@example.com")


def test_create_branch_rejects_invalid_names(workspace: Workspace) -> None:
    for bad in ("", "HEAD", "-x", "a..b", "a b", "x.lock", "a/.hidden", "trailing/"):
        with pytest.raises(GitError):
            workspace.create_branch(bad)


def test_crlf_file_edit_produces_minimal_applicable_patch(
    workspace: Workspace, source_repo: Path, tmp_path: Path
) -> None:
    target = workspace.path / "src" / "etl" / "legacy.py"
    text = read_text(target)
    assert "\r" not in text  # tools see LF text
    write_text_preserving_eol(target, text.replace("return 1", "return 10"))
    assert target.read_bytes() == CRLF_SOURCE.replace(b"return 1", b"return 10")
    assert workspace.diff_stats() == (1, 2)
    patch = workspace.diff()
    assert "-    return 1\r\n+    return 10\r\n" in patch

    other = Workspace.clone(source_repo, tmp_path / "apply-check", branch="main")
    patch_file = tmp_path / "fix.patch"
    patch_file.write_bytes(patch.encode("utf-8"))
    other.git("apply", "--whitespace=nowarn", str(patch_file))
    assert (other.path / "src" / "etl" / "legacy.py").read_bytes() == target.read_bytes()


def test_workspace_remove_deletes_read_only_objects(workspace: Workspace) -> None:
    path = workspace.path
    workspace.remove()
    assert not path.exists()
    workspace.remove()  # idempotent


# --------------------------------------------------------------------------- push


def _prepare_fix(ws: Workspace, branch: str = "healer/sales-load-abcd1234", value: str = "fixed") -> str:
    ws.create_branch(branch)
    write_text_preserving_eol(ws.path / "src/etl/sales.py", f"def load():\n    return '{value}'\n")
    return ws.commit_all("fix", author_name="dag-healer", author_email="healer@example.com")


def test_push_to_path_creates_branch_without_touching_checkout(
    workspace: Workspace, source_repo: Path
) -> None:
    sha = _prepare_fix(workspace)
    status_before = run_git(["status", "--porcelain"], source_repo).stdout
    workspace.push_to_path(source_repo, "healer/sales-load-abcd1234")

    ref = run_git(["rev-parse", "refs/heads/healer/sales-load-abcd1234"], source_repo).stdout.strip()
    assert ref == sha
    assert run_git(["symbolic-ref", "--short", "HEAD"], source_repo).stdout.strip() == "main"
    assert (source_repo / "src" / "etl" / "sales.py").read_bytes() == b"def load():\n    return 'v1'\n"
    assert (source_repo / "README.md").read_bytes() == b"uncommitted edit\n"
    assert run_git(["status", "--porcelain"], source_repo).stdout == status_before

    # A second fix attempt on the same branch needs force (history differs).
    retry = Workspace.clone(source_repo, workspace.path.parent / "retry", branch="main")
    _prepare_fix(retry, value="fixed differently")
    with pytest.raises(GitError):
        retry.push_to_path(source_repo, "healer/sales-load-abcd1234")
    retry.push_to_path(source_repo, "healer/sales-load-abcd1234", force=True)
    ref = run_git(["rev-parse", "refs/heads/healer/sales-load-abcd1234"], source_repo).stdout.strip()
    assert ref == retry.head_sha()


def test_push_to_path_refuses_base_and_checked_out_branch(workspace: Workspace, source_repo: Path) -> None:
    _prepare_fix(workspace)
    with pytest.raises(GitError, match="base branch"):
        workspace.push_to_path(source_repo, "main")
    run_git(["checkout", "-q", "-b", "feature"], source_repo)
    with pytest.raises(GitError, match="checked out"):
        workspace.push_to_path(source_repo, "feature")


def test_push_to_bare_remote_without_token(workspace: Workspace, tmp_path: Path) -> None:
    bare = tmp_path / "remote.git"
    run_git(["init", "-q", "--bare", str(bare)], tmp_path)
    sha = _prepare_fix(workspace)
    workspace.push(str(bare), "healer/sales-load-abcd1234")
    assert run_git(["rev-parse", "refs/heads/healer/sales-load-abcd1234"], bare).stdout.strip() == sha
    with pytest.raises(GitError, match="base branch"):
        workspace.push(str(bare), "main")
    with pytest.raises(GitError, match="does not exist"):
        workspace.push(str(bare), "healer/unknown")


def test_a_given_commit_is_pushed_whatever_the_branch_points_to(
    workspace: Workspace, source_repo: Path, tmp_path: Path
) -> None:
    checked = _prepare_fix(workspace)
    write_text_preserving_eol(workspace.path / "src/etl/sales.py", "def load():\n    return 'later'\n")
    later = workspace.commit_all("later", author_name="dag-healer", author_email="healer@example.com")
    assert later != checked
    bare = tmp_path / "remote.git"
    run_git(["init", "-q", "--bare", str(bare)], tmp_path)
    workspace.push(str(bare), "healer/sales-load-abcd1234", commit=checked)
    assert run_git(["rev-parse", "refs/heads/healer/sales-load-abcd1234"], bare).stdout.strip() == checked
    workspace.push_to_path(source_repo, "healer/sales-load-abcd1234", commit=checked)
    ref = run_git(["rev-parse", "refs/heads/healer/sales-load-abcd1234"], source_repo).stdout.strip()
    assert ref == checked
    for bad in ("HEAD", checked[:12], f"{checked}:refs/heads/main", "refs/heads/main"):
        with pytest.raises(GitError, match="not a full commit sha"):
            workspace.push(str(bare), "healer/sales-load-abcd1234", commit=bad)


def test_the_commit_message_is_stored_verbatim(workspace: Workspace, monkeypatch: pytest.MonkeyPatch) -> None:
    # "[" as the comment character and commit.cleanup=strip would drop the second paragraph
    for index, (key, value) in enumerate((("core.commentChar", "["), ("commit.cleanup", "strip"))):
        monkeypatch.setenv(f"GIT_CONFIG_KEY_{index}", key)
        monkeypatch.setenv(f"GIT_CONFIG_VALUE_{index}", value)
    monkeypatch.setenv("GIT_CONFIG_COUNT", "2")
    workspace.create_branch("healer/sales-load-abcd1234")
    write_text_preserving_eol(workspace.path / "src/etl/sales.py", "def load():\n    return 'fixed'\n")
    message = "Fix it\n\n[skip ci] CI runs after review.\n\nGenerated by dag-healer"
    sha = workspace.commit_all(message, author_name="dag-healer", author_email="healer@example.com")
    stored = run_git(["cat-file", "commit", sha], workspace.path).stdout.partition("\n\n")[2]
    assert stored == message + "\n"


def test_a_diff_between_two_commits(workspace: Workspace) -> None:
    base = workspace.head_sha()
    sha = _prepare_fix(workspace)
    write_text_preserving_eol(workspace.path / "src/etl/sales.py", "def load():\n    return 'uncommitted'\n")
    between = workspace.diff(base, target=sha)
    assert "+    return 'fixed'" in between and "uncommitted" not in between
    assert workspace.diff_stats(base, target=sha) == (1, 2)


def test_push_with_token_uses_askpass_and_never_argv(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    token = "ghp_SuperSecretToken123"
    calls: list[tuple[list[str], dict[str, str]]] = []
    seen_script: dict[str, str] = {}

    def fake_run(cmd, **kwargs):
        env = kwargs["env"]
        calls.append((cmd, env))
        args = _args(cmd)
        stdout = b""
        if args == ["remote"]:
            stdout = b"origin\n"
        elif args[:2] == ["remote", "get-url"]:
            stdout = b"git@github.com:acme/pipelines.git\n"
        elif "push" in args:
            script = Path(env["GIT_ASKPASS"])
            seen_script["content"] = script.read_text(encoding="utf-8")
            seen_script["path"] = str(script)
        return subprocess.CompletedProcess(cmd, 0, stdout, b"")

    monkeypatch.setattr(gitmod.subprocess, "run", fake_run)
    ws = Workspace(tmp_path, base_branch="main")
    ws.push("origin", "healer/fix-1", token=token)

    push_cmd, push_env = next((c, e) for c, e in calls if "push" in c)
    for cmd, _env in calls:
        assert all(token not in part for part in cmd), cmd
    assert "https://github.com/acme/pipelines.git" in push_cmd
    assert "refs/heads/healer/fix-1:refs/heads/healer/fix-1" in push_cmd
    assert push_cmd[push_cmd.index("credential.helper=") - 1] == "-c"
    assert push_env[ASKPASS_TOKEN_ENV] == token
    assert push_env["GIT_TERMINAL_PROMPT"] == "0"
    assert seen_script["content"] == ASKPASS_SCRIPT
    assert token not in seen_script["content"]
    assert not Path(seen_script["path"]).exists()  # helper removed after the push


def test_push_with_token_strips_url_credentials_and_redacts_errors(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    token = "ghp_AnotherSecret456"

    def fake_run(cmd, **kwargs):
        args = _args(cmd)
        if "push" in args:
            return subprocess.CompletedProcess(cmd, 128, b"", f"fatal: auth failed for {token}\n".encode())
        return subprocess.CompletedProcess(cmd, 0, b"", b"")

    monkeypatch.setattr(gitmod.subprocess, "run", fake_run)
    ws = Workspace(tmp_path, base_branch="main")
    with pytest.raises(GitError) as info:
        ws.push("https://olduser:oldpass@github.com/acme/pipelines.git", "healer/fix-1", token=token)
    message = str(info.value) + info.value.stderr + " ".join(info.value.command)
    assert token not in message
    assert "***" in str(info.value)
    assert "oldpass" not in " ".join(info.value.command)


def test_push_with_token_rejects_non_https_remote(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    monkeypatch.setattr(
        gitmod.subprocess, "run", lambda cmd, **kw: subprocess.CompletedProcess(cmd, 0, b"", b"")
    )
    ws = Workspace(tmp_path, base_branch="main")
    with pytest.raises(GitError, match="https"):
        ws.push("/srv/git/repo.git", "healer/fix-1", token="tok")


def test_askpass_script_answers_git_credential_prompts(tmp_path: Path) -> None:
    """Real git asks the helper for the user name and the token (no network involved)."""
    with askpass_env("tok-123") as env:
        script = Path(env["GIT_ASKPASS"])
        assert script.read_bytes() == ASKPASS_SCRIPT.encode("utf-8")
        assert b"\r" not in script.read_bytes()
        proc = subprocess.run(
            ["git", "-c", "credential.helper=", "credential", "fill"],
            cwd=tmp_path,
            env=git_environment(env),
            input=b"protocol=https\nhost=example.com\n\n",
            capture_output=True,
            timeout=60,
            check=False,
        )
    assert proc.returncode == 0, proc.stderr.decode(errors="replace")
    out = proc.stdout.decode()
    assert "username=x-access-token" in out
    assert "password=tok-123" in out
    assert not script.exists()


# --------------------------------------------------------------------------- no hooks, no fsmonitor

#: Hooks git runs while the worker branches, stages and commits in its clone.
COMMIT_HOOKS = (
    "post-checkout",
    "pre-commit",
    "prepare-commit-msg",
    "commit-msg",
    "post-commit",
    "reference-transaction",
    "post-index-change",
)
#: Hooks the receiving repository runs when the worker pushes into it.
RECEIVE_HOOKS = ("pre-receive", "update", "post-receive", "post-update", "reference-transaction")


def _plant_hook(path: Path, marker: Path) -> None:
    """A hook (or fsmonitor program) that appends its own name to ``marker``.

    Written with LF line endings and a ``#!/bin/sh`` line: git runs it through sh, also on Windows.
    """
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(f"#!/bin/sh\necho {path.name} >> '{marker.as_posix()}'\n".encode())
    path.chmod(0o755)


def _plain_git(args: list[str], cwd: Path) -> subprocess.CompletedProcess[str]:
    """git without dag-healer's settings: the control that shows a planted program or view works."""
    return subprocess.run(
        ["git", *args],
        cwd=cwd,
        env=git_environment({**IDENTITY, "GIT_NO_REPLACE_OBJECTS": None, "GIT_GRAFT_FILE": None}),
        stdin=subprocess.DEVNULL,
        capture_output=True,
        text=True,
        timeout=120,
        check=False,
    )


def _ran(marker: Path) -> list[str]:
    return marker.read_text(encoding="utf-8").split() if marker.exists() else []


def test_no_hook_planted_in_the_clone_runs_when_the_worker_commits(
    workspace: Workspace, tmp_path: Path
) -> None:
    marker = tmp_path / "hooks-ran.txt"
    for name in COMMIT_HOOKS:
        _plant_hook(workspace.path / ".git" / "hooks" / name, marker)

    sha = _prepare_fix(workspace)
    assert workspace.changed_files() == [] and workspace.diff(sha) == ""
    assert _ran(marker) == []

    # control: plain git runs the planted hooks
    write_text_preserving_eol(workspace.path / "src/etl/sales.py", "def load():\n    return 'v3'\n")
    control = _plain_git(["commit", "-q", "-am", "control"], workspace.path)
    assert control.returncode == 0, control.stderr
    assert "pre-commit" in _ran(marker)


def test_a_hooks_path_planted_in_the_clone_config_is_ignored(workspace: Workspace, tmp_path: Path) -> None:
    marker = tmp_path / "hooks-ran.txt"
    planted = tmp_path / "planted hooks"
    for name in COMMIT_HOOKS:
        _plant_hook(planted / name, marker)
    run_git(["config", "core.hooksPath", planted.as_posix()], workspace.path)

    _prepare_fix(workspace)
    assert _ran(marker) == []

    write_text_preserving_eol(workspace.path / "src/etl/sales.py", "def load():\n    return 'v3'\n")
    control = _plain_git(["commit", "-q", "-am", "control"], workspace.path)
    assert control.returncode == 0, control.stderr
    assert "pre-commit" in _ran(marker)


def test_no_hook_of_the_receiving_repository_runs_when_the_worker_pushes(
    workspace: Workspace, source_repo: Path, tmp_path: Path
) -> None:
    """``receive-pack`` runs in the target repository, with git's ``-c`` settings cleared."""
    marker = tmp_path / "hooks-ran.txt"
    bare = tmp_path / "remote.git"
    run_git(["init", "-q", "--bare", str(bare)], tmp_path)
    for name in RECEIVE_HOOKS:
        _plant_hook(source_repo / ".git" / "hooks" / name, marker)
        _plant_hook(bare / "hooks" / name, marker)
    sha = _prepare_fix(workspace)

    workspace.push_to_path(source_repo, "healer/sales-load-abcd1234")
    workspace.push(str(bare), "healer/sales-load-abcd1234")
    for repo in (source_repo, bare):
        assert run_git(["rev-parse", "refs/heads/healer/sales-load-abcd1234"], repo).stdout.strip() == sha
    assert _ran(marker) == []

    control = _plain_git(["push", "-q", os.fspath(source_repo), "HEAD:refs/heads/control"], workspace.path)
    assert control.returncode == 0, control.stderr
    assert "pre-receive" in _ran(marker)


def test_no_fsmonitor_planted_in_the_clone_config_runs(workspace: Workspace, tmp_path: Path) -> None:
    marker = tmp_path / "fsmonitor-ran.txt"
    program = tmp_path / "fsmonitor-hook.sh"
    _plant_hook(program, marker)
    run_git(["config", "core.fsmonitor", program.as_posix()], workspace.path)
    # control first: plain git status runs the program (and stores fsmonitor data in the index)
    for _ in range(2):
        _plain_git(["status", "--porcelain"], workspace.path)
    if not _ran(marker) and os.name == "nt":
        pytest.skip("this git does not run an fsmonitor hook program")
    assert _ran(marker)
    marker.unlink()

    write_text_preserving_eol(workspace.path / "src/etl/sales.py", "def load():\n    return 'v2'\n")
    for _ in range(2):
        assert workspace.changed_files() == ["src/etl/sales.py"]
        assert "+    return 'v2'" in workspace.diff()
        assert workspace.diff_stats() == (1, 2)
    workspace.create_branch("healer/fsmonitor")
    workspace.commit_all("fix", author_name="dag-healer", author_email="healer@example.com")
    assert _ran(marker) == []


def test_empty_hooks_dir_is_replaced_when_something_is_planted_in_it(
    caplog: pytest.LogCaptureFixture,
) -> None:
    first = empty_hooks_dir()
    assert first.is_dir() and not any(first.iterdir())
    assert first.name.startswith(gitmod.NO_HOOKS_PREFIX)
    assert empty_hooks_dir() == first  # stable while it stays empty
    (first / "pre-commit").write_bytes(b"#!/bin/sh\nexit 0\n")

    with caplog.at_level(logging.WARNING, logger="dag_healer.repo.git"):
        second = empty_hooks_dir()
    assert second != first and second.is_dir() and not any(second.iterdir())
    assert not first.exists()
    assert "was changed or removed" in caplog.text


def test_local_transport_commands_carry_safe_directory_and_the_hook_settings(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    """Shell-quoted and with forward slashes: Windows temporary paths hold ``~`` (8.3 names)."""
    hooks = tmp_path / "RUNNER~1" / "no hooks"
    hooks.mkdir(parents=True)
    monkeypatch.setattr(gitmod, "empty_hooks_dir", lambda: hooks)
    for program, command in (
        ("upload-pack", gitmod._local_upload_pack()),
        ("receive-pack", gitmod._local_receive_pack()),
    ):
        assert shlex.split(command) == [
            "git",
            "-c",
            "safe.directory=*",
            "-c",
            f"core.hooksPath={hooks.as_posix()}",
            "-c",
            "core.fsmonitor=false",
            program,
        ]


# --------------------------------------------------------------------------- objects as stored


def _fix_on_an_extra_commit(ws: Workspace) -> tuple[str, str, str]:
    """(base, extra, fix): a commit that adds a file, and a fix with a workflow file on top of it."""
    base = ws.head_sha()
    ws.create_branch("healer/sales-load-abcd1234")
    write_text_preserving_eol(ws.path / "src/extra.py", "X = 1\n")
    extra = ws.commit_all("extra", author_name="dag-healer", author_email="healer@example.com")
    _write(ws.path, {".github/workflows/race.yml": "on: push\n"})
    write_text_preserving_eol(ws.path / "src/etl/sales.py", "def load():\n    return 'fixed'\n")
    fix = ws.commit_all("fix", author_name="dag-healer", author_email="healer@example.com")
    return base, extra, fix


def _parents(output: str) -> list[str]:
    return output.split()[1:]


def _fake_commit_graph(repo: Path, commit: str, parent: str, tree: str) -> None:
    """A commit-graph file that gives ``commit`` the parent ``parent`` and the root tree ``tree``.

    git writes the file, then the entry of ``commit`` is rewritten and the file's checksum
    updated (the format of gitformat-commit-graph(5)): git trusts this cache over the objects.
    """
    written = _plain_git(["commit-graph", "write", "--reachable"], repo)
    assert written.returncode == 0, written.stderr
    path = repo / ".git" / "objects" / "info" / "commit-graph"
    data = bytearray(path.read_bytes())
    hash_len = {1: 20, 2: 32}[data[5]]
    chunks = {}
    for index in range(data[6] + 1):
        start = 8 + 12 * index
        chunks[bytes(data[start : start + 4])] = int.from_bytes(data[start + 4 : start + 12], "big")
    count = int.from_bytes(data[chunks[b"OIDF"] + 1020 : chunks[b"OIDF"] + 1024], "big")
    oids = [
        data[chunks[b"OIDL"] + n * hash_len : chunks[b"OIDL"] + (n + 1) * hash_len].hex()
        for n in range(count)
    ]
    entry = chunks[b"CDAT"] + oids.index(commit) * (hash_len + 16)
    data[entry : entry + hash_len] = bytes.fromhex(tree)
    data[entry + hash_len : entry + hash_len + 4] = oids.index(parent).to_bytes(4, "big")
    digest = hashlib.sha1 if hash_len == 20 else hashlib.sha256
    data[-hash_len:] = digest(bytes(data[:-hash_len])).digest()
    path.chmod(stat.S_IREAD | stat.S_IWRITE)  # git writes it read-only
    path.write_bytes(bytes(data))


@pytest.mark.parametrize("config", [False, True], ids=["replace-ref", "use-replace-refs-config"])
def test_a_replace_ref_never_changes_what_the_worker_reads(workspace: Workspace, config: bool) -> None:
    base, extra, fix = _fix_on_an_extra_commit(workspace)
    real_diff = workspace.diff(base, target=fix)
    real_stats = workspace.diff_stats(base, target=fix)
    # what code left in the clone could plant: the fix replaced by a harmless commit on the base
    harmless = run_git(
        ["commit-tree", "-p", base, "-m", "harmless", f"{base}^{{tree}}"], workspace.path, env=IDENTITY
    ).stdout.strip()
    run_git(["replace", fix, harmless], workspace.path)
    if config:  # before git 2.42 this line overrode GIT_NO_REPLACE_OBJECTS
        run_git(["config", "core.useReplaceRefs", "true"], workspace.path)
    control = _plain_git(["diff-tree", "-r", "--name-only", base, fix], workspace.path)
    assert control.returncode == 0 and control.stdout == ""  # plain git sees the harmless commit

    assert workspace.diff(base, target=fix) == real_diff
    assert ".github/workflows/race.yml" in real_diff and "src/extra.py" in real_diff
    assert workspace.diff_stats(base, target=fix) == real_stats
    assert _parents(workspace.git("rev-list", "--parents", "--max-count=1", fix).stdout) == [extra]
    assert workspace.git("cat-file", "commit", fix).stdout.endswith("\n\nfix\n")
    names = workspace.git("diff-tree", "-r", "--no-renames", "--name-only", base, fix).stdout.split()
    assert names == [".github/workflows/race.yml", "src/etl/sales.py", "src/extra.py"]


def test_a_graft_file_never_changes_the_parents_the_worker_reads(workspace: Workspace) -> None:
    base, extra, fix = _fix_on_an_extra_commit(workspace)
    grafts = workspace.path / ".git" / "info" / "grafts"
    grafts.parent.mkdir(exist_ok=True)
    grafts.write_bytes(f"{fix} {base}\n".encode())
    control = _plain_git(["rev-list", "--parents", "--max-count=1", fix], workspace.path)
    assert control.returncode == 0 and _parents(control.stdout) == [base]

    assert _parents(workspace.git("rev-list", "--parents", "--max-count=1", fix).stdout) == [extra]
    assert workspace.git("log", "--format=%s", f"{base}..{fix}").stdout.split() == ["fix", "extra"]


def test_a_commit_graph_never_changes_the_parents_or_the_tree_the_worker_reads(workspace: Workspace) -> None:
    base, extra, fix = _fix_on_an_extra_commit(workspace)
    real_diff = workspace.diff(base, target=fix)
    _fake_commit_graph(
        workspace.path, fix, base, workspace.git("rev-parse", f"{base}^{{tree}}").stdout.strip()
    )
    control = _plain_git(["rev-list", "--parents", "--max-count=1", fix], workspace.path)
    if control.returncode != 0 or _parents(control.stdout) != [base]:
        pytest.skip("this git does not read the planted commit-graph")
    assert _plain_git(["diff", "--name-only", base, fix, "--"], workspace.path).stdout == ""

    assert _parents(workspace.git("rev-list", "--parents", "--max-count=1", fix).stdout) == [extra]
    assert workspace.diff(base, target=fix) == real_diff
    assert workspace.diff_stats(base, target=fix)[0] == 3


# --------------------------------------------------------------------------- remotes


def test_origin_url_and_is_git_repo(source_repo: Path, tmp_path: Path) -> None:
    assert origin_url(source_repo) is None
    run_git(["remote", "add", "origin", "git@github.com:acme/pipelines.git"], source_repo)
    assert origin_url(source_repo) == "git@github.com:acme/pipelines.git"
    plain = tmp_path / "plain"
    plain.mkdir()
    assert origin_url(plain) is None
    assert origin_url(tmp_path / "missing") is None
    assert is_git_repo(source_repo) is True
    assert is_git_repo(plain) is False


@pytest.mark.parametrize(
    ("url", "https", "repo"),
    [
        ("git@github.com:acme/pipelines.git", "https://github.com/acme/pipelines.git", "acme/pipelines"),
        ("git@github.com:acme/pipelines", "https://github.com/acme/pipelines.git", "acme/pipelines"),
        (
            "ssh://git@github.com/acme/pipelines.git",
            "https://github.com/acme/pipelines.git",
            "acme/pipelines",
        ),
        (
            "ssh://git@github.com:22/acme/pipe.lines",
            "https://github.com/acme/pipe.lines.git",
            "acme/pipe.lines",
        ),
        ("https://github.com/acme/pipelines", "https://github.com/acme/pipelines.git", "acme/pipelines"),
        ("https://github.com/acme/pipelines.git/", "https://github.com/acme/pipelines.git", "acme/pipelines"),
        ("https://x-access-token:tok@github.com/acme/p.git", "https://github.com/acme/p.git", "acme/p"),
        ("https://www.github.com/Acme/My-Repo", "https://github.com/Acme/My-Repo.git", "Acme/My-Repo"),
        ("git://github.com/acme/pipelines.git", "https://github.com/acme/pipelines.git", "acme/pipelines"),
        ("  git@GitHub.com:acme/pipelines.git\n", "https://github.com/acme/pipelines.git", "acme/pipelines"),
        ("git@gitlab.com:acme/pipelines.git", None, None),
        ("https://github.com/acme", None, None),
        ("https://github.com/acme/pipelines/tree/main", None, None),
        ("file:///opt/airflow/repo", None, None),
        ("/opt/airflow/repo", None, None),
        ("C:\\Users\\me\\repo", None, None),
        ("", None, None),
    ],
)
def test_github_url_helpers(url: str, https: str | None, repo: str | None) -> None:
    assert github_https_url(url) == https
    assert parse_github_repo(url) == repo


def test_github_enterprise_hosts() -> None:
    url = "git@github.acme.internal:data/etl.git"
    assert parse_github_repo(url) is None
    assert parse_github_repo(url, hosts=["github.acme.internal"]) == "data/etl"
    assert (
        github_https_url(url, hosts=["github.acme.internal"]) == "https://github.acme.internal/data/etl.git"
    )


# --------------------------------------------------------------------------- text helpers


def test_write_text_preserving_eol(tmp_path: Path) -> None:
    crlf = tmp_path / "crlf.py"
    crlf.write_bytes(b"a = 1\r\nb = 2\r\n")
    assert detect_eol(crlf) == "\r\n"
    assert read_text(crlf) == "a = 1\nb = 2\n"
    write_text_preserving_eol(crlf, "a = 1\nb = 3\n")
    assert crlf.read_bytes() == b"a = 1\r\nb = 3\r\n"
    write_text_preserving_eol(crlf, "a = 1\r\nb = 4\r\n")  # already CRLF: no doubling
    assert crlf.read_bytes() == b"a = 1\r\nb = 4\r\n"

    lf = tmp_path / "lf.py"
    lf.write_bytes(b"x = 1\n")
    write_text_preserving_eol(lf, "x = 2\r\n")
    assert lf.read_bytes() == b"x = 2\n"
    assert detect_eol(lf) == "\n"

    new = tmp_path / "pkg" / "new_test.py"
    write_text_preserving_eol(new, "def test():\n    pass\n")
    assert new.read_bytes() == b"def test():\n    pass\n"
    assert detect_eol(tmp_path / "missing.py") is None


def test_bom_and_undecodable_bytes_round_trip(tmp_path: Path) -> None:
    bom = tmp_path / "bom.py"
    bom.write_bytes(b"\xef\xbb\xbfname = 'caf\xc3\xa9'\r\n")
    text = read_text(bom)
    assert text == "name = 'café'\n"
    write_text_preserving_eol(bom, text.replace("café", "cafe"))
    assert bom.read_bytes() == b"\xef\xbb\xbfname = 'cafe'\r\n"

    latin = tmp_path / "latin.py"
    latin.write_bytes(b"# caf\xe9\nx = 1\n")
    write_text_preserving_eol(latin, read_text(latin).replace("x = 1", "x = 2"))
    assert latin.read_bytes() == b"# caf\xe9\nx = 2\n"


@pytest.mark.parametrize(
    ("raw", "expected"),
    [
        ("healer/sales_daily_etl-load-abcd1234", "healer/sales_daily_etl-load-abcd1234"),
        ("healer/my dag:task*1", "healer/my-dag-task-1"),
        ("healer//x..y/.hidden/", "healer/x.y/hidden"),
        ("healer/name.lock", "healer/name"),
        ("~^:?", "healer"),
    ],
)
def test_sanitize_branch_name(raw: str, expected: str) -> None:
    assert sanitize_branch_name(raw) == expected


def test_local_clone_and_push_work_when_the_repository_belongs_to_another_user(
    source_repo: Path, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """A bind-mounted repository owned by another uid ("dubious ownership", e.g. Docker Desktop).

    Git starts the upload-pack / receive-pack of local transports without the ``-c`` config of
    the calling command, so ``-c safe.directory=*`` alone is not enough there; git's own test
    knob makes every repository look foreign to reproduce it on any host.
    """
    monkeypatch.setenv("GIT_TEST_ASSUME_DIFFERENT_OWNER", "1")
    plain = subprocess.run(
        ["git", "-c", "safe.directory=*", "ls-remote", "--heads", str(source_repo)],
        capture_output=True,
        text=True,
        check=False,
    )
    # git 2.39 (the Airflow image) fails here: the failure being fixed. Some newer git builds
    # (Git for Windows 2.49) do not reproduce it; the checks below must pass either way.
    if plain.returncode != 0:
        assert "dubious ownership" in plain.stderr

    workspace = Workspace.clone(source_repo, tmp_path / "foreign-clone", branch="main")
    assert workspace.base_branch == "main"
    sha = _prepare_fix(workspace)
    workspace.push_to_path(source_repo, "healer/sales-load-abcd1234")
    bare = tmp_path / "foreign-remote.git"
    run_git(["init", "-q", "--bare", str(bare)], tmp_path)
    workspace.push(str(bare), "healer/sales-load-abcd1234")
    for repo in (source_repo, bare):
        assert run_git(["rev-parse", "refs/heads/healer/sales-load-abcd1234"], repo).stdout.strip() == sha
    clone_from_url = Workspace.clone(source_repo.as_uri(), tmp_path / "foreign-url-clone", branch="main")
    assert clone_from_url.current_branch() == "main"
