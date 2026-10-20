"""Git helpers for the repair workspace (clone, branch, diff, commit, push).

The worker never edits the user's repository in place. It clones it into a throw-away
workspace, lets the repair agent work there, commits the result on a ``healer/...``
branch and then pushes that branch either to GitHub (token through ``GIT_ASKPASS``) or
back into the user's repository as a plain local branch.

Important properties:

* **A clone contains committed history only.** ``git clone`` copies the HEAD of the
  requested branch; uncommitted or untracked changes in the source working tree are not
  part of the workspace. Callers that need the *current* data files (contracts,
  datasets) must read them from the source working tree, not from the clone.
* Every git invocation passes ``-c safe.directory=* -c core.autocrlf=false
  -c core.filemode=false`` so that bind-mounted repositories owned by another uid work,
  files are checked out byte-for-byte and Windows bind mounts (which report every file
  as 0777) do not produce mode-only diffs. Git does **not** pass ``-c`` values to the
  ``upload-pack`` / ``receive-pack`` it starts for local clones and pushes (it clears the
  config environment of local transports), so those commands are given explicitly with
  the same ``safe.directory`` override (:data:`LOCAL_UPLOAD_PACK`,
  :data:`LOCAL_RECEIVE_PACK`); without it a clone of a bind mount owned by another uid
  fails with "detected dubious ownership".
* Output is decoded as UTF-8 **without** newline translation, so diffs of CRLF files
  survive intact (``fix.patch`` must apply cleanly).
* Tokens never appear on a command line or inside a URL: :meth:`Workspace.push` writes a
  tiny ``GIT_ASKPASS`` script (which itself contains no secret) into a private temporary
  directory and hands the token to it through the environment of the git process only.
  Credential helpers are disabled for that push so the token is never stored.

Nothing in this module imports Airflow.
"""

from __future__ import annotations

import codecs
import logging
import os
import re
import shutil
import stat
import subprocess
import sys
import tempfile
from collections.abc import Iterable, Iterator, Mapping, Sequence
from contextlib import contextmanager
from pathlib import Path
from typing import Any
from urllib.parse import urlsplit, urlunsplit

log = logging.getLogger(__name__)

#: Configuration passed to every git command (see the module docstring).
GIT_SAFE_CONFIG: tuple[str, ...] = (
    "-c",
    "safe.directory=*",
    "-c",
    "core.autocrlf=false",
    "-c",
    "core.filemode=false",
)
#: Remote-side commands of local transports (run through ``sh`` by git, which starts them
#: without the ``-c`` configuration of the calling command).
LOCAL_UPLOAD_PACK = "git -c safe.directory='*' upload-pack"
LOCAL_RECEIVE_PACK = "git -c safe.directory='*' receive-pack"
#: Extra presentation settings: never quote non-ASCII paths in status/diff output.
_GIT_DISPLAY_CONFIG: tuple[str, ...] = ("-c", "core.quotepath=false")

#: Default timeout (seconds) of a single git command.
DEFAULT_GIT_TIMEOUT = 600.0

#: Environment variables read by the askpass helper script.
ASKPASS_TOKEN_ENV = "DAG_HEALER_GIT_TOKEN"
ASKPASS_USERNAME_ENV = "DAG_HEALER_GIT_USERNAME"
#: User name sent with a GitHub token (works for classic/fine-grained PATs and app tokens).
DEFAULT_TOKEN_USERNAME = "x-access-token"

#: Hosts recognised as GitHub by :func:`github_https_url` / :func:`parse_github_repo`.
GITHUB_HOSTS: tuple[str, ...] = ("github.com", "www.github.com")

#: The askpass helper. It answers "Username for ..." with the user name and any other
#: prompt with the token, both read from the environment. It contains no secret.
ASKPASS_SCRIPT = f"""#!/bin/sh
# dag-healer GIT_ASKPASS helper: answers git credential prompts from the environment.
# The token is never written to disk nor passed on a command line.
case "$1" in
  [Uu]sername*) printf '%s\\n' "${{{ASKPASS_USERNAME_ENV}:-{DEFAULT_TOKEN_USERNAME}}}" ;;
  *) printf '%s\\n' "${{{ASKPASS_TOKEN_ENV}}}" ;;
esac
"""

# Inherited variables that would redirect git to another repository or index.
_GIT_LOCATION_ENV = (
    "GIT_DIR",
    "GIT_WORK_TREE",
    "GIT_INDEX_FILE",
    "GIT_OBJECT_DIRECTORY",
    "GIT_ALTERNATE_OBJECT_DIRECTORIES",
    "GIT_COMMON_DIR",
    "GIT_NAMESPACE",
    "GIT_PREFIX",
    "GIT_CEILING_DIRECTORIES",
)

_URL_SCHEME_RE = re.compile(r"^[A-Za-z][A-Za-z0-9+.-]*://")
_SCP_LIKE_RE = re.compile(r"^(?:(?P<user>[^@/\s]+)@)?(?P<host>[^:/\s\\]+):(?P<path>[^\s]+)$")
_REPO_SEGMENT_RE = re.compile(r"^[A-Za-z0-9_.-]+$")
_BRANCH_BAD_CHARS_RE = re.compile(r"[^A-Za-z0-9._/-]+")


class GitError(RuntimeError):
    """A git command failed (non-zero exit, timeout, missing executable, bad input)."""

    def __init__(
        self,
        message: str,
        *,
        command: Sequence[str] = (),
        returncode: int | None = None,
        stderr: str = "",
    ) -> None:
        super().__init__(message)
        self.command = list(command)
        self.returncode = returncode
        self.stderr = stderr


# --------------------------------------------------------------------------- running git


def _decode(data: bytes | None) -> str:
    """Decode git output as UTF-8 without translating newlines."""
    if not data:
        return ""
    return data.decode("utf-8", errors="replace")


def _redact(text: str, secrets: Iterable[str]) -> str:
    for secret in secrets:
        if secret:
            text = text.replace(secret, "***")
    return text


def git_environment(extra: Mapping[str, str | None] | None = None) -> dict[str, str]:
    """Return the environment used for git commands.

    Starts from ``os.environ``, removes variables that would point git at another
    repository (``GIT_DIR``, ``GIT_WORK_TREE``, ...), disables terminal prompts and finally
    applies ``extra`` (a ``None`` value removes the variable).
    """
    env = {key: value for key, value in os.environ.items() if key not in _GIT_LOCATION_ENV}
    env["GIT_TERMINAL_PROMPT"] = "0"
    for key, value in (extra or {}).items():
        if value is None:
            env.pop(key, None)
        else:
            env[key] = str(value)
    return env


def run_git(
    args: Sequence[str | os.PathLike[str]],
    cwd: str | os.PathLike[str],
    *,
    env: Mapping[str, str | None] | None = None,
    check: bool = True,
    timeout: float | None = DEFAULT_GIT_TIMEOUT,
    redact: Iterable[str] = (),
) -> subprocess.CompletedProcess[str]:
    """Run ``git <GIT_SAFE_CONFIG> <args>`` in ``cwd`` and return the completed process.

    Args:
        args: git arguments (the subcommand and its options); may start with extra
            ``-c key=value`` pairs.
        cwd: working directory of the command.
        env: variables merged over the process environment (``None`` values remove a
            variable); see :func:`git_environment`.
        check: raise :class:`GitError` when git exits with a non-zero status.
        timeout: seconds before the command is killed (``None`` = no limit).
        redact: secrets replaced by ``***`` in the captured output and error messages.

    Returns:
        A ``CompletedProcess`` whose ``stdout``/``stderr`` are ``str`` decoded as UTF-8
        with newlines left untouched.

    Raises:
        GitError: git is missing, timed out, ``cwd`` does not exist, or (with ``check``)
            git exited with a non-zero status.
    """
    secrets = [s for s in redact if s]
    cmd = ["git", *GIT_SAFE_CONFIG, *(os.fspath(a) for a in args)]
    shown = _redact(" ".join(cmd[len(GIT_SAFE_CONFIG) + 1 :]), secrets)
    log.debug("git %s (cwd=%s)", shown, cwd)
    if not Path(os.fspath(cwd)).is_dir():
        raise GitError(f"git working directory does not exist: {cwd}", command=cmd)
    try:
        proc = subprocess.run(
            cmd,
            cwd=os.fspath(cwd),
            env=git_environment(env),
            stdin=subprocess.DEVNULL,
            capture_output=True,
            timeout=timeout,
            check=False,
        )
    except subprocess.TimeoutExpired as exc:
        raise GitError(f"git {shown} timed out after {timeout} s", command=cmd) from exc
    except FileNotFoundError as exc:
        raise GitError("the git executable was not found on PATH", command=cmd) from exc
    except OSError as exc:
        raise GitError(f"could not run git {shown}: {exc}", command=cmd) from exc

    stdout = _redact(_decode(proc.stdout), secrets)
    stderr = _redact(_decode(proc.stderr), secrets)
    result = subprocess.CompletedProcess(cmd, proc.returncode, stdout, stderr)
    if check and proc.returncode != 0:
        detail = (stderr.strip() or stdout.strip() or "no output")[-2000:]
        raise GitError(
            f"git {shown} failed (exit {proc.returncode}): {detail}",
            command=cmd,
            returncode=proc.returncode,
            stderr=stderr,
        )
    return result


# --------------------------------------------------------------------------- askpass


@contextmanager
def askpass_env(token: str, *, username: str = DEFAULT_TOKEN_USERNAME) -> Iterator[dict[str, str]]:
    """Yield environment variables that make git answer credential prompts with ``token``.

    A private temporary directory receives the :data:`ASKPASS_SCRIPT` helper (mode 0700);
    the token travels only in the returned environment (``DAG_HEALER_GIT_TOKEN``) and the
    directory is deleted when the context exits.
    """
    if not token:
        raise GitError("askpass_env() needs a non-empty token")
    tmpdir = Path(tempfile.mkdtemp(prefix="dag-healer-askpass-"))
    try:
        os.chmod(tmpdir, stat.S_IRWXU)
        script = tmpdir / "askpass.sh"
        # Written as bytes: the script must keep LF line endings on every platform.
        script.write_bytes(ASKPASS_SCRIPT.encode("utf-8"))
        os.chmod(script, stat.S_IRWXU)
        yield {
            "GIT_ASKPASS": str(script),
            ASKPASS_TOKEN_ENV: token,
            ASKPASS_USERNAME_ENV: username,
            "GIT_TERMINAL_PROMPT": "0",
            # Git Credential Manager (Windows/macOS) must never open a dialog.
            "GCM_INTERACTIVE": "never",
        }
    finally:
        shutil.rmtree(tmpdir, ignore_errors=True)


# --------------------------------------------------------------------------- branch names


def sanitize_branch_name(name: str, *, max_length: int = 120) -> str:
    """Turn arbitrary text into a valid git branch name (``/`` separators are kept).

    Disallowed characters become ``-``; empty components, ``..``, leading dots/dashes and
    ``.lock`` suffixes are removed. Returns ``"healer"`` when nothing usable remains.
    """
    parts: list[str] = []
    for raw in name.replace("\\", "/").split("/"):
        part = _BRANCH_BAD_CHARS_RE.sub("-", raw)
        part = re.sub(r"\.{2,}", ".", part)
        part = re.sub(r"-{2,}", "-", part)
        part = part.strip(".-")
        while part.endswith(".lock"):
            part = part[: -len(".lock")].rstrip(".-")
        if part:
            parts.append(part)
    result = "/".join(parts)[:max_length].strip("./-")
    return result or "healer"


def _validate_branch(name: str) -> None:
    """Reject names git would refuse (a conservative subset of ``check-ref-format``)."""
    bad = (
        not name
        or name == "HEAD"
        or name.startswith(("-", "/"))
        or name.endswith(("/", "."))
        or ".." in name
        or "//" in name
        or _BRANCH_BAD_CHARS_RE.search(name) is not None
        or any(part.startswith(".") or part.endswith(".lock") for part in name.split("/"))
    )
    if bad:
        raise GitError(f"invalid branch name: {name!r}")


# --------------------------------------------------------------------------- workspace


def _is_local_source(source: str | os.PathLike[str]) -> bool:
    if isinstance(source, os.PathLike):
        return True
    text = str(source)
    if _URL_SCHEME_RE.match(text):
        return False
    if re.match(r"^[A-Za-z]:[\\/]", text):
        return True
    return os.path.exists(text)


def _uses_local_transport(target: str | os.PathLike[str]) -> bool:
    """True for a local path or ``file://`` URL (git starts upload/receive-pack itself)."""
    return _is_local_source(target) or str(target).lower().startswith("file://")


def _remove_readonly(func: Any, path: str, _exc: Any) -> None:
    """rmtree error handler: git object files are read-only on Windows."""
    try:
        os.chmod(path, stat.S_IWRITE | stat.S_IREAD)
        func(path)
    except OSError:  # pragma: no cover - best effort
        log.debug("could not remove %s", path)


def remove_tree(path: str | os.PathLike[str]) -> None:
    """Delete a directory tree, including read-only git object files (Windows)."""
    target = Path(path)
    if not target.exists():
        return
    if sys.version_info >= (3, 12):
        shutil.rmtree(target, onexc=_remove_readonly)
    else:  # pragma: no cover - Python < 3.12
        shutil.rmtree(target, onerror=_remove_readonly)


class Workspace:
    """A git working copy used by the repair agent (normally a fresh clone).

    ``base_branch`` is the branch the clone was made from; :meth:`push` and
    :meth:`push_to_path` refuse to push to it, so the healer can only ever create or
    update its own branches.
    """

    def __init__(self, path: str | os.PathLike[str], *, base_branch: str | None = None) -> None:
        self.path = Path(path)
        self.base_branch = base_branch

    def __repr__(self) -> str:
        return f"Workspace(path={str(self.path)!r}, base_branch={self.base_branch!r})"

    # -- plumbing ----------------------------------------------------------

    def git(
        self,
        *args: str | os.PathLike[str],
        env: Mapping[str, str | None] | None = None,
        check: bool = True,
        redact: Iterable[str] = (),
    ) -> subprocess.CompletedProcess[str]:
        """Run a git command inside the workspace (see :func:`run_git`)."""
        return run_git(list(args), self.path, env=env, check=check, redact=redact)

    # -- creation ----------------------------------------------------------

    @classmethod
    def clone(
        cls,
        source: str | os.PathLike[str],
        dest: str | os.PathLike[str],
        *,
        branch: str | None,
    ) -> Workspace:
        """Clone ``source`` (local path or URL) into ``dest`` and return the workspace.

        Only committed history is cloned: uncommitted changes in a local source's working
        tree are **not** part of the clone. ``branch`` selects the branch to check out;
        when it is ``None`` or does not exist in the source, the source's HEAD branch is
        used instead (a warning is logged). Local sources are cloned with
        ``--no-hardlinks`` so the workspace never shares object files with the user's
        repository.

        Raises:
            GitError: ``dest`` exists and is not empty, or git fails.
        """
        dest_path = Path(dest)
        if dest_path.exists() and (not dest_path.is_dir() or any(dest_path.iterdir())):
            raise GitError(f"clone destination already exists and is not empty: {dest_path}")
        dest_path.parent.mkdir(parents=True, exist_ok=True)
        local = _is_local_source(source)
        src = os.fspath(Path(source).resolve()) if local else os.fspath(source)

        args: list[str] = ["clone", "--quiet", "--single-branch"]
        if local:
            args.append("--no-hardlinks")
        pack: list[str] = ["--upload-pack", LOCAL_UPLOAD_PACK] if _uses_local_transport(src) else []
        args += pack
        if branch:
            listed = run_git(
                ["ls-remote", *pack, "--heads", "--", src, f"refs/heads/{branch}"],
                dest_path.parent,
                check=False,
            )
            if listed.returncode == 0 and listed.stdout.strip():
                args += ["--branch", branch]
            else:
                log.warning(
                    "branch %r not found in %s; cloning the source's default branch instead",
                    branch,
                    src,
                )
        args += ["--", src, os.fspath(dest_path)]
        run_git(args, dest_path.parent)
        workspace = cls(dest_path)
        workspace.base_branch = workspace.current_branch()
        log.info("cloned %s (branch %s) into %s", src, workspace.base_branch, dest_path)
        return workspace

    def create_branch(self, name: str) -> None:
        """Create branch ``name`` from the current HEAD and check it out."""
        _validate_branch(name)
        self.git("checkout", "--quiet", "-b", name)

    # -- inspection --------------------------------------------------------

    def current_branch(self) -> str:
        """Return the checked-out branch name (``"HEAD"`` when detached)."""
        result = self.git("symbolic-ref", "--quiet", "--short", "HEAD", check=False)
        name = result.stdout.strip()
        return name if result.returncode == 0 and name else "HEAD"

    def head_sha(self) -> str:
        """Return the full sha of HEAD."""
        return self.git("rev-parse", "HEAD").stdout.strip()

    def changed_files(self) -> list[str]:
        """Return repo-relative posix paths that differ from HEAD (untracked included).

        Ignored files are not reported. Renames report the new path; deletions are listed.
        """
        out = self.git(*_GIT_DISPLAY_CONFIG, "status", "--porcelain=v1", "-z", "--untracked-files=all").stdout
        entries = out.split("\0")
        files: set[str] = set()
        index = 0
        while index < len(entries):
            entry = entries[index]
            index += 1
            if len(entry) < 4:
                continue
            code, path = entry[:2], entry[3:]
            files.add(path.replace("\\", "/"))
            if code[0] in "RC":
                index += 1  # skip the original path of a rename/copy
        return sorted(files)

    def _intent_to_add_untracked(self) -> None:
        out = self.git("ls-files", "--others", "--exclude-standard", "-z").stdout
        untracked = [p for p in out.split("\0") if p]
        for start in range(0, len(untracked), 200):
            self.git("add", "--intent-to-add", "--", *untracked[start : start + 200])

    def diff(self, base: str = "HEAD") -> str:
        """Return the unified diff of the working tree against ``base``.

        New untracked (non-ignored) files are included: they are first registered with
        ``git add --intent-to-add`` (this touches the index of the workspace only).
        Output is suitable for ``git apply`` (fixed ``a/`` ``b/`` prefixes, no textconv,
        no external diff, CRLF preserved).
        """
        self._intent_to_add_untracked()
        return self.git(
            *_GIT_DISPLAY_CONFIG,
            "diff",
            "--no-color",
            "--no-ext-diff",
            "--no-textconv",
            "--src-prefix=a/",
            "--dst-prefix=b/",
            base,
            "--",
        ).stdout

    def diff_stats(self, base: str = "HEAD") -> tuple[int, int]:
        """Return ``(files_changed, lines_changed)`` against ``base`` (added + deleted lines).

        Untracked files count as new files; binary files count as one file and zero lines.
        """
        self._intent_to_add_untracked()
        out = self.git(*_GIT_DISPLAY_CONFIG, "diff", "--numstat", "--no-ext-diff", base, "--").stdout
        files = 0
        lines = 0
        for row in out.splitlines():
            parts = row.split("\t", 2)
            if len(parts) < 3:
                continue
            files += 1
            for value in parts[:2]:
                if value.isdigit():
                    lines += int(value)
        return files, lines

    # -- writing -----------------------------------------------------------

    def commit_all(self, message: str, *, author_name: str, author_email: str) -> str:
        """Stage every change (``git add -A``), commit it and return the new HEAD sha.

        The author and committer identity come from the arguments, so no ``user.name``
        configuration is needed.

        Raises:
            GitError: there is nothing to commit, or git fails.
        """
        self.git("add", "-A")
        staged = self.git("diff", "--cached", "--quiet", check=False)
        if staged.returncode == 0:
            raise GitError("nothing to commit: the workspace has no changes")
        identity = {
            "GIT_AUTHOR_NAME": author_name,
            "GIT_AUTHOR_EMAIL": author_email,
            "GIT_COMMITTER_NAME": author_name,
            "GIT_COMMITTER_EMAIL": author_email,
        }
        self.git("commit", "--quiet", "-m", message, env=identity)
        sha = self.head_sha()
        log.info("committed %s on %s in %s", sha[:12], self.current_branch(), self.path)
        return sha

    def _check_push_target(self, branch: str) -> None:
        _validate_branch(branch)
        if self.base_branch and branch == self.base_branch:
            raise GitError(f"refusing to push to the base branch {branch!r}")
        exists = self.git("rev-parse", "--verify", "--quiet", f"refs/heads/{branch}", check=False)
        if exists.returncode != 0:
            raise GitError(f"local branch {branch!r} does not exist in the workspace")

    def _resolve_remote(self, remote: str) -> str:
        names = self.git("remote", check=False).stdout.split()
        if remote in names:
            return self.git("remote", "get-url", remote).stdout.strip()
        return remote

    def push(self, remote: str, branch: str, *, token: str | None = None, force: bool = False) -> None:
        """Push local ``branch`` to ``remote`` (a remote name or URL) under the same name.

        With ``token``: ssh/scp-style GitHub URLs are converted to https, user info is
        stripped from https URLs, credential helpers are disabled and the token is
        provided through ``GIT_ASKPASS`` (see :func:`askpass_env`); it never appears on
        the command line, in the URL or in error messages. Without a token the ambient
        git credentials (ssh keys, helpers) are used. ``force`` overwrites the remote
        branch (only ever a healer branch: pushing to the base branch is refused).
        """
        self._check_push_target(branch)
        target = self._resolve_remote(remote)
        refspec = f"{'+' if force else ''}refs/heads/{branch}:refs/heads/{branch}"
        if not token:
            pack = ["--receive-pack", LOCAL_RECEIVE_PACK] if _uses_local_transport(target) else []
            self.git("push", "--quiet", *pack, target, refspec)
            log.info("pushed %s to %s", branch, _strip_userinfo(target))
            return
        if not target.lower().startswith(("https://", "http://")):
            converted = github_https_url(target)
            if converted is None:
                raise GitError(
                    "a token can only be used with an https remote (or a GitHub ssh URL convertible to https)"
                )
            target = converted
        target = _strip_userinfo(target)
        with askpass_env(token) as env:
            self.git("-c", "credential.helper=", "push", "--quiet", target, refspec, env=env, redact=[token])
        log.info("pushed %s to %s", branch, target)

    def push_to_path(self, repo_path: str | os.PathLike[str], branch: str, *, force: bool = False) -> None:
        """Push HEAD into the local repository ``repo_path`` as ``refs/heads/<branch>``.

        Runs ``git push <repo_path> HEAD:refs/heads/<branch>``: the branch is created (or
        fast-forwarded) in the target repository without touching its checked-out branch
        or working tree. Pushing to the base branch or to the branch currently checked out
        in the target is refused.
        """
        _validate_branch(branch)
        if self.base_branch and branch == self.base_branch:
            raise GitError(f"refusing to push to the base branch {branch!r}")
        target = Path(repo_path).resolve()
        checked_out = run_git(["symbolic-ref", "--quiet", "--short", "HEAD"], target, check=False)
        if checked_out.returncode == 0 and checked_out.stdout.strip() == branch:
            raise GitError(f"refusing to push to {branch!r}: it is checked out in {target}")
        refspec = f"{'+' if force else ''}HEAD:refs/heads/{branch}"
        self.git("push", "--quiet", "--receive-pack", LOCAL_RECEIVE_PACK, os.fspath(target), refspec)
        log.info("pushed %s into %s", branch, target)

    def remove(self) -> None:
        """Delete the workspace directory (read-only git objects included)."""
        remove_tree(self.path)


# --------------------------------------------------------------------------- remotes


def is_git_repo(path: str | os.PathLike[str]) -> bool:
    """Return True when ``path`` is inside a git working tree."""
    try:
        result = run_git(["rev-parse", "--is-inside-work-tree"], path, check=False, timeout=30)
    except GitError:
        return False
    return result.returncode == 0 and result.stdout.strip() == "true"


def origin_url(repo_path: str | os.PathLike[str]) -> str | None:
    """Return ``remote.origin.url`` of the repository at ``repo_path`` (None if unset)."""
    try:
        result = run_git(["config", "--get", "remote.origin.url"], repo_path, check=False, timeout=30)
    except GitError:
        return None
    url = result.stdout.strip()
    return url if result.returncode == 0 and url else None


def _strip_userinfo(url: str) -> str:
    """Remove ``user:password@`` from a URL (non-URLs are returned unchanged)."""
    if not _URL_SCHEME_RE.match(url):
        return url
    parts = urlsplit(url)
    if "@" not in parts.netloc:
        return url
    netloc = parts.netloc.rsplit("@", 1)[1]
    return urlunsplit((parts.scheme, netloc, parts.path, parts.query, parts.fragment))


def _split_remote(url: str) -> tuple[str, str] | None:
    """Return ``(host, path)`` of a remote URL (https/ssh/git/scp-like), else None."""
    text = (url or "").strip()
    if not text:
        return None
    if _URL_SCHEME_RE.match(text):
        parts = urlsplit(text)
        if parts.scheme.lower() not in {"https", "http", "ssh", "git", "git+ssh", "ssh+git"}:
            return None
        host = (parts.hostname or "").lower()
        return (host, parts.path) if host else None
    match = _SCP_LIKE_RE.match(text)
    if match is None or len(match.group("host")) == 1:  # "C:\..." is a Windows path
        return None
    return match.group("host").lower(), match.group("path")


def _owner_and_name(url: str, hosts: Iterable[str]) -> tuple[str, str, str] | None:
    split = _split_remote(url)
    if split is None:
        return None
    host, path = split
    allowed = {h.lower() for h in hosts}
    if host not in allowed:
        return None
    segments = [s for s in path.strip("/").split("/") if s]
    if len(segments) != 2:
        return None
    owner, name = segments
    if name.endswith(".git"):
        name = name[: -len(".git")]
    if not owner or not name or not _REPO_SEGMENT_RE.match(owner) or not _REPO_SEGMENT_RE.match(name):
        return None
    if host == "www.github.com":
        host = "github.com"
    return host, owner, name


def github_https_url(url: str, *, hosts: Iterable[str] = GITHUB_HOSTS) -> str | None:
    """Normalise a GitHub remote URL to ``https://<host>/<owner>/<name>.git``.

    Accepts scp-like (``git@github.com:o/r.git``), ``ssh://``, ``git://`` and ``https://``
    URLs (credentials are dropped). Returns None for other hosts, local paths and
    ``file://`` URLs. Pass ``hosts`` to accept a GitHub Enterprise host.
    """
    parsed = _owner_and_name(url, hosts)
    if parsed is None:
        return None
    host, owner, name = parsed
    return f"https://{host}/{owner}/{name}.git"


def parse_github_repo(url: str, *, hosts: Iterable[str] = GITHUB_HOSTS) -> str | None:
    """Return ``"owner/name"`` for a GitHub remote URL, else None (see :func:`github_https_url`)."""
    parsed = _owner_and_name(url, hosts)
    if parsed is None:
        return None
    _host, owner, name = parsed
    return f"{owner}/{name}"


# --------------------------------------------------------------------------- text files


def _uses_crlf(data: bytes) -> bool:
    crlf = data.count(b"\r\n")
    lf_only = data.count(b"\n") - crlf
    return crlf > 0 and crlf >= lf_only


def detect_eol(path: str | os.PathLike[str]) -> str | None:
    """Return ``"\\r\\n"`` or ``"\\n"`` for an existing file (None if missing or no newline)."""
    target = Path(path)
    if not target.is_file():
        return None
    data = target.read_bytes()
    if b"\n" not in data:
        return None
    return "\r\n" if _uses_crlf(data) else "\n"


def read_text(path: str | os.PathLike[str]) -> str:
    """Read a UTF-8 text file with newlines normalised to ``\\n`` (BOM stripped).

    Undecodable bytes are kept as surrogate escapes so that a read/modify/write cycle
    through :func:`write_text_preserving_eol` never corrupts them.
    """
    data = Path(path).read_bytes()
    text = data.decode("utf-8-sig", errors="surrogateescape")
    return text.replace("\r\n", "\n")


def write_text_preserving_eol(path: str | os.PathLike[str], text: str) -> None:
    """Write ``text`` keeping the line-ending style (and UTF-8 BOM) of the existing file.

    ``text`` may use ``\\n`` or ``\\r\\n``; it is normalised first. Existing files that
    mostly use CRLF are written with CRLF; new files (and LF files) are written with LF.
    Parent directories are created.
    """
    target = Path(path)
    normalised = text.replace("\r\n", "\n")
    crlf = False
    bom = False
    if target.is_file():
        existing = target.read_bytes()
        crlf = _uses_crlf(existing)
        bom = existing.startswith(codecs.BOM_UTF8)
    if crlf:
        normalised = normalised.replace("\n", "\r\n")
    encoded = normalised.encode("utf-8", errors="surrogateescape")
    if bom and not encoded.startswith(codecs.BOM_UTF8):
        encoded = codecs.BOM_UTF8 + encoded
    target.parent.mkdir(parents=True, exist_ok=True)
    target.write_bytes(encoded)


__all__ = [
    "ASKPASS_SCRIPT",
    "ASKPASS_TOKEN_ENV",
    "ASKPASS_USERNAME_ENV",
    "DEFAULT_TOKEN_USERNAME",
    "GITHUB_HOSTS",
    "GIT_SAFE_CONFIG",
    "GitError",
    "Workspace",
    "askpass_env",
    "detect_eol",
    "git_environment",
    "github_https_url",
    "is_git_repo",
    "origin_url",
    "parse_github_repo",
    "read_text",
    "remove_tree",
    "run_git",
    "sanitize_branch_name",
    "write_text_preserving_eol",
]
