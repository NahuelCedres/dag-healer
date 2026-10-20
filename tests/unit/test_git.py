"""Tests for the git workspace helpers (dag_healer.repo.git) on real temporary repositories."""

from __future__ import annotations

import subprocess
from pathlib import Path

import pytest

from dag_healer.repo import git as gitmod
from dag_healer.repo.git import (
    ASKPASS_SCRIPT,
    ASKPASS_TOKEN_ENV,
    GitError,
    Workspace,
    askpass_env,
    detect_eol,
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
    assert seen[0][-1] == "status"
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
    env = git_environment({"EXTRA": "1", "PATH_TO_DROP": None})
    assert "GIT_DIR" not in env and "GIT_WORK_TREE" not in env
    assert env["GIT_TERMINAL_PROMPT"] == "0"
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


def test_push_with_token_uses_askpass_and_never_argv(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    token = "ghp_SuperSecretToken123"
    calls: list[tuple[list[str], dict[str, str]]] = []
    seen_script: dict[str, str] = {}

    def fake_run(cmd, **kwargs):
        env = kwargs["env"]
        calls.append((cmd, env))
        args = cmd[7:]
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
        args = cmd[7:]
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
