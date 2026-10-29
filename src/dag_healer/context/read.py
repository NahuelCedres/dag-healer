"""Read the Organization Context and the ETL Information from the base branch's commit.

:func:`read_context` is what the worker calls for every incident (from
:func:`dag_healer.agent.context.gather_context`), and :func:`read_all` what ``dag-healer
doctor`` calls. Both read the repository of ``repo.path`` through git only:

1. the commit is the newer of ``refs/heads/<repo.base_branch>`` and
   ``refs/remotes/origin/<base_branch>`` (the local branch when they diverged, with a note), else
   ``HEAD`` (a detached checkout, such as git-sync's); the working tree is never read: what is
   there was never reviewed, and anything that runs in the checkout can write there. A file that
   is in the working tree but not in that commit gets a note;
2. each file is looked up with ``git ls-tree`` (``context.organization`` and
   ``<context.etl_dir>/<pipeline id>.md``); only a regular file is read (``git cat-file blob``),
   and only when it is at most ``context.max_file_bytes`` long;
3. a file is **refused**, and nothing of it is sent to the model or stored, when it is over the
   size cap, is not UTF-8 text, is a symbolic link or a gitlink, or holds what looks like a
   secret (:func:`dag_healer.masking.find_secret`, with the values of the configured secrets and
   of the credential variables of the worker's environment). The secret check looks at the file
   as it is, without its HTML comments (a comment can split a value), and at what the parsing
   gives (a rule whose lines are joined, the owner, the text, the problems). The reason never
   quotes the value;
4. a file that is read is parsed (:mod:`dag_healer.context.parse`), and its text and its rules
   are masked with :func:`dag_healer.masking.mask_text` (e-mail addresses, phone numbers and the
   like) before anything else sees them.

Every file read is recorded with its path, the commit (``path@sha``) and its blob id, and the
last commit that changed it when git can tell. Nothing here raises for a broken repository:
problems become notes of the snapshot.
"""

from __future__ import annotations

import logging
import os
import re
from collections.abc import Iterable
from dataclasses import dataclass
from datetime import date, datetime, timezone
from pathlib import Path
from typing import TYPE_CHECKING

from dag_healer.context.model import ContextFile, ContextSnapshot, Refusal, Rule, short_sha
from dag_healer.context.parse import ParsedFile, clean_problem, parse_context_file, review_due, strip_comments
from dag_healer.masking import find_secret, mask_text
from dag_healer.repo.git import GitError, git_bytes, run_git

if TYPE_CHECKING:
    from dag_healer.config import HealerConfig

logger = logging.getLogger(__name__)

#: Credential variables of the worker's environment whose values a context file may never hold
#: (besides the secret fields of ``healer.yaml``).
CREDENTIAL_ENVS: tuple[str, ...] = (
    "ANTHROPIC_API_KEY",
    "ANTHROPIC_AUTH_TOKEN",
    "AWS_SECRET_ACCESS_KEY",
    "AWS_SESSION_TOKEN",
    "AWS_BEARER_TOKEN_BEDROCK",
    "GITHUB_TOKEN",
    "GH_TOKEN",
    "HEALER_INGEST_TOKEN",
    "HEALER_AIRFLOW_PASSWORD",
    "HEALER_SMTP_PASSWORD",
)
#: Seconds for one git command of the context reading.
GIT_TIMEOUT = 30.0
#: A note of a snapshot is one line of at most this many characters ...
MAX_NOTE_CHARS = 2000
#: ... and the notes of a snapshot hold at most this many characters together (the rest are counted).
MAX_NOTES_CHARS = 4000
#: A pipeline id that names an ETL Information file: no separator, no leading dot (whole, see
#: :func:`etl_path`: ``$`` would also take an id that ends with a line break).
_PIPELINE_ID_RE = re.compile(r"[A-Za-z0-9_][A-Za-z0-9_.@+-]{0,249}")
_SHA_RE = re.compile(r"^[0-9a-f]{40}(?:[0-9a-f]{24})?$")
_REGULAR_MODES = ("100644", "100755")


@dataclass(frozen=True)
class _Entry:
    """One ``git ls-tree -l`` entry."""

    mode: str
    kind: str
    oid: str
    size: int | None
    path: str


# --------------------------------------------------------------------------- git


def _git(repo: Path, *args: str) -> str | None:
    """The output of a git command, ``None`` when it fails."""
    try:
        result = run_git(list(args), repo, check=False, timeout=GIT_TIMEOUT)
    except GitError as exc:
        logger.debug("dag-healer context: git %s failed: %s", " ".join(args[:2]), exc)
        return None
    return result.stdout if result.returncode == 0 else None


def is_repository(repo: Path) -> bool:
    out = _git(repo, "rev-parse", "--is-inside-work-tree") if repo.is_dir() else None
    return out is not None and out.strip() == "true"


def _commit_of(repo: Path, ref: str) -> str | None:
    out = _git(repo, "rev-parse", "--verify", "--quiet", f"{ref}^{{commit}}")
    sha = (out or "").strip()
    return sha if _SHA_RE.match(sha) else None


def _is_ancestor(repo: Path, older: str, newer: str) -> bool:
    return _git(repo, "merge-base", "--is-ancestor", older, newer) is not None


def base_commit(repo: Path, base_branch: str) -> tuple[str, str, str | None] | None:
    """``(sha, ref, note)`` of the commit to read: the newer of the base branch and its ``origin``
    copy, else ``HEAD``.

    A local branch behind ``origin/<base_branch>`` (a checkout that fetches without updating it)
    gives way to it; when the two have diverged, the local branch is read and ``note`` says so.
    """
    found = [
        (sha, ref)
        for ref in (
            [f"refs/heads/{base_branch}", f"refs/remotes/origin/{base_branch}"] if base_branch else []
        )
        if (sha := _commit_of(repo, ref)) is not None
    ]
    if len(found) == 2 and found[0][0] != found[1][0]:
        (local, local_ref), (remote, remote_ref) = found
        if _is_ancestor(repo, local, remote):
            return remote, remote_ref, None
        if not _is_ancestor(repo, remote, local):
            note = (
                f"{base_branch} ({short_sha(local)}) and origin/{base_branch} ({short_sha(remote)}) have "
                f"diverged; the context files were read from {base_branch}."
            )
            return local, local_ref, note
    if found:
        return found[0][0], found[0][1], None
    head = _commit_of(repo, "HEAD")
    return (head, "HEAD", None) if head is not None else None


def _entries(repo: Path, commit: str, path: str) -> list[_Entry] | None:
    """The ``ls-tree`` entries of ``path`` in ``commit`` (relative to ``repo``); None on failure."""
    out = _git(repo, "ls-tree", "-z", "-l", commit, "--", path)
    if out is None:
        return None
    entries: list[_Entry] = []
    for record in out.split("\0"):
        if not record or "\t" not in record:
            continue
        meta, name = record.split("\t", 1)
        fields = meta.split()
        if len(fields) != 4:
            continue
        mode, kind, oid, size = fields
        entries.append(_Entry(mode, kind, oid, int(size) if size.isdigit() else None, name))
    return entries


def _last_change(repo: Path, commit: str, path: str) -> tuple[str | None, str | None]:
    """``(sha, ISO date)`` of the last commit that changed ``path`` (best effort)."""
    out = _git(repo, "log", "-1", "--format=%H%x00%cI", commit, "--", path)
    sha, _sep, when = (out or "").strip().partition("\0")
    if not _SHA_RE.match(sha):
        return None, None
    return sha, when or None


def known_secret_values(cfg: HealerConfig | None) -> list[str]:
    """The values a context file may never hold: the configured secrets and :data:`CREDENTIAL_ENVS`."""
    values = [os.environ.get(name) or "" for name in CREDENTIAL_ENVS]
    if cfg is not None:
        for dotted in cfg.secret_fields:
            node: object = cfg
            for part in dotted.split("."):
                node = node.get(part) if isinstance(node, dict) else getattr(node, part, None)
            if isinstance(node, str):
                values.append(node)
    return [value for value in values if value]


# --------------------------------------------------------------------------- one file


def _today(today: date | None) -> date:
    return today or datetime.now(timezone.utc).date()


def _refuse(item: ContextFile, refusal: Refusal, reason: str) -> ContextFile:
    item.status = "refused"
    item.refusal = refusal
    item.reason = reason
    return item


def _secret_in_lines(text: str, known: list[str]) -> str | None:
    """What looks like a secret in ``text``, as it is and without its HTML comments (where, never
    the value): a comment inside a value (``AKIA<!-- -->...``) hides it from the first look only.

    The comments are taken out twice: joining the text around them (the line of a value split by
    one), then leaving a space, as the model reads them.
    """
    for variant in (text, strip_comments(text, filler=""), strip_comments(text)):
        finding = find_secret(variant, known)
        if finding is not None:
            return finding.describe()
    return None


def _secret_once_parsed(parsed: ParsedFile, known: list[str]) -> str | None:
    """What looks like a secret in what the model, the store and the PR would get of a parsed file
    (a rule whose lines are joined, a folded owner, the text, the problems), with where it is."""
    for rule in parsed.rules:
        finding = find_secret(rule.text, known)
        if finding is not None:
            return f"{finding.kind} in {rule.id} (line {rule.line}, with its lines joined)"
    if parsed.front.owner and (finding := find_secret(parsed.front.owner, known)) is not None:
        return f"{finding.kind} in its owner"
    for text in (parsed.text, *parsed.problems):
        finding = find_secret(text, known)
        if finding is not None:
            return f"{finding.kind} in what the model would read of it"
    return None


def _secret_reason(where: str) -> str:
    return f"it holds what looks like {where}; remove it from the file (and rotate it if it is real)"


def _read_entry(
    repo: Path,
    commit: str,
    item: ContextFile,
    entry: _Entry,
    *,
    max_bytes: int,
    known: Iterable[str],
    today: date,
) -> ContextFile:
    """Read one committed file into ``item`` (refused, error or read)."""
    known = list(known)
    item.blob = entry.oid
    item.size = entry.size
    if entry.kind == "tree":
        item.status = "error"
        item.reason = "it is a folder, not a file"
        return item
    if entry.mode == "120000":
        return _refuse(item, "link", "it is a symbolic link; dag-healer reads regular files only")
    if entry.kind != "blob" or entry.mode not in _REGULAR_MODES:
        return _refuse(item, "type", "it is not a regular file")
    if entry.size is not None and entry.size > max_bytes:
        return _refuse(
            item,
            "size",
            f"it is {entry.size:,} bytes, over the {max_bytes:,}-byte limit (context.max_file_bytes); "
            "split it or make it shorter",
        )
    try:
        data = git_bytes(["cat-file", "blob", entry.oid], repo, timeout=GIT_TIMEOUT)
    except GitError as exc:
        item.status = "error"
        item.reason = f"git could not read it ({clean_problem(mask_text(exc), limit=200)})"
        return item
    if len(data) > max_bytes:
        return _refuse(item, "size", f"it is over the {max_bytes:,}-byte limit (context.max_file_bytes)")
    try:
        text = data.decode("utf-8-sig")
    except UnicodeDecodeError:
        return _refuse(item, "encoding", "it is not UTF-8 text")
    text = text.replace("\r\n", "\n").replace("\r", "\n")
    found = _secret_in_lines(text, known)
    if found is not None:
        return _refuse(item, "secret", _secret_reason(found))
    parsed = parse_context_file(text, kind=item.kind, pipeline=item.pipeline)
    found = _secret_once_parsed(parsed, known)
    if found is not None:
        return _refuse(item, "secret", _secret_reason(found))
    front = parsed.front
    item.status = "read"
    item.owner = mask_text(front.owner) if front.owner else None
    item.reviewed = front.reviewed.isoformat() if front.reviewed else None
    item.review_every = front.review_every
    item.rule_prefix = "ORG" if item.kind == "organization" else front.rule_prefix
    if front.reviewed is not None and front.review_every:
        due = review_due(front.reviewed, front.review_every)
        if due is not None:
            item.review_due = due.isoformat()
            item.stale = due < today
        else:
            parsed.problems.append(
                "front matter: the next review date (reviewed + review_every) is out of range"
            )
    if front.reviewed is not None and front.reviewed > today:
        parsed.problems.append(f"front matter: reviewed ({item.reviewed}) is in the future")
    item.rules = [Rule(rule.id, mask_text(rule.text), rule.line, rule.overrides) for rule in parsed.rules]
    item.problems = [mask_text(problem) for problem in parsed.problems]
    item.text = mask_text(parsed.text)
    return item


def _read_file(
    repo: Path,
    commit: str,
    item: ContextFile,
    *,
    max_bytes: int,
    known: Iterable[str],
    today: date,
    history: bool,
) -> ContextFile:
    entries = _entries(repo, commit, item.path)
    if entries is None:
        item.status = "error"
        item.reason = "git could not list it"
        return item
    entry = next((candidate for candidate in entries if candidate.path == item.path), None)
    if entry is None:
        item.status = "missing"
        item.uncommitted = (repo / item.path).exists()
        return item
    _read_entry(repo, commit, item, entry, max_bytes=max_bytes, known=known, today=today)
    if history and item.status in ("read", "refused"):
        item.changed_in, item.changed_at = _last_change(repo, commit, item.path)
    return item


# --------------------------------------------------------------------------- notes


def clean_notes(notes: Iterable[str]) -> list[str]:
    """``notes`` as the incident gets them: each one line of :data:`MAX_NOTE_CHARS` characters at
    most, without control characters or prompt tags (:func:`~dag_healer.context.parse.clean_problem`),
    and :data:`MAX_NOTES_CHARS` characters at most together (a last note counts the others)."""
    cleaned = [clean_problem(note, limit=MAX_NOTE_CHARS) for note in notes]
    kept: list[str] = []
    total = 0
    for index, note in enumerate(cleaned):
        if total + len(note) > MAX_NOTES_CHARS:
            kept.append(f"... and {len(cleaned) - index} more notes about the context files")
            break
        kept.append(note)
        total += len(note)
    return kept


def file_note(item: ContextFile, where: str) -> str | None:
    """What the incident says about one file (``None`` when there is nothing to say), as one line
    of :data:`MAX_NOTE_CHARS` characters at most (:func:`clean_notes`)."""
    note = _file_note(item, where)
    return clean_notes([note])[0] if note else None


def _file_note(item: ContextFile, where: str) -> str | None:
    title = item.title
    if item.kind == "etl":
        title = f"ETL Information of {item.pipeline}" if item.pipeline else title
    if item.status == "refused":
        return f"The {title} ({item.path}, {where}) was not sent to the model: {item.reason}."
    if item.status == "error":
        return f"The {title} ({item.path}, {where}) could not be read: {item.reason}."
    if item.status == "missing":
        if item.uncommitted:
            return (
                f"{item.path} is in the working tree but not in {where}: dag-healer reads the context "
                "files of the base branch only (commit it)."
            )
        return None
    notes: list[str] = []
    if item.problems:
        shown = "; ".join(item.problems[:5])
        more = f" (and {len(item.problems) - 5} more)" if len(item.problems) > 5 else ""
        notes.append(
            f"The {title} ({item.path}) has problems: {shown}{more}. Its well-formed rules are used."
        )
    if item.stale:
        notes.append(
            f"The {title} ({item.path}) was last reviewed on {item.reviewed} (review every "
            f"{item.review_every}): it is overdue for a review."
        )
    return " ".join(notes) or None


def _cross_check(snapshot: ContextSnapshot) -> None:
    """Report the overrides of the ETL Information that name no rule of the Organization Context."""
    etl, org = snapshot.etl, snapshot.organization
    if etl is None or not etl.is_read or (org is not None and org.status in ("refused", "error")):
        return  # an Organization Context that could not be read cannot tell whether an override is right
    org_ids = set(org.rule_ids()) if org is not None and org.is_read else set()
    for rule in etl.rules:
        for target in rule.overrides:
            if target not in org_ids:
                etl.problems.append(
                    f"line {rule.line}: {rule.id} overrides {target}, which the Organization Context "
                    "does not define"
                )


def _notes(snapshot: ContextSnapshot) -> list[str]:
    where = f"{snapshot.ref_name}@{short_sha(snapshot.commit)}"
    return [note for item in snapshot.files() if (note := file_note(item, where))]


# --------------------------------------------------------------------------- entry points


def etl_path(cfg: HealerConfig, pipeline_id: str) -> str | None:
    """``<context.etl_dir>/<pipeline_id>.md``, or ``None`` for an id that cannot name a file."""
    if not _PIPELINE_ID_RE.fullmatch(pipeline_id or "") or ".." in pipeline_id:
        return None
    return f"{cfg.context.etl_dir}/{pipeline_id}.md"


def _in_working_tree(cfg: HealerConfig, paths: Iterable[str]) -> list[str]:
    root = cfg.repo_root
    return [path for path in paths if path and (root / path).exists()]


def read_context(
    cfg: HealerConfig,
    pipeline_id: str | None,
    *,
    known_secrets: Iterable[str] | None = None,
    today: date | None = None,
    history: bool = True,
) -> ContextSnapshot:
    """The Organization Context and the ETL Information of ``pipeline_id`` (see the module docstring).

    ``known_secrets`` defaults to :func:`known_secret_values` of ``cfg``; ``today`` (UTC) decides
    which files are overdue for a review; ``history`` adds the last commit that changed each file.
    Never raises.
    """
    snapshot = ContextSnapshot()
    repo = cfg.repo_root
    org_path = cfg.context.organization
    pipeline_path = etl_path(cfg, pipeline_id) if pipeline_id else None
    wanted = [org_path, *([pipeline_path] if pipeline_path else [])]
    known = list(known_secrets) if known_secrets is not None else known_secret_values(cfg)
    if not is_repository(repo):
        present = _in_working_tree(cfg, wanted)
        if present:
            snapshot.notes.append(
                f"{', '.join(present)} {'is' if len(present) == 1 else 'are'} not read: {repo} is not a git "
                "repository, and dag-healer reads the context files from the base branch's commit only."
            )
        snapshot.notes = clean_notes(snapshot.notes)
        return snapshot
    found = base_commit(repo, cfg.repo.base_branch)
    if found is None:
        present = _in_working_tree(cfg, wanted)
        if present:
            snapshot.notes.append(
                f"{', '.join(present)} {'is' if len(present) == 1 else 'are'} not read: the repository "
                f"has no commit of {cfg.repo.base_branch}."
            )
        snapshot.notes = clean_notes(snapshot.notes)
        return snapshot
    snapshot.commit, snapshot.ref, ref_note = found
    day = _today(today)
    max_bytes = cfg.context.max_file_bytes
    snapshot.organization = _read_file(
        repo,
        snapshot.commit,
        ContextFile(kind="organization", path=org_path),
        max_bytes=max_bytes,
        known=known,
        today=day,
        history=history,
    )
    if pipeline_path is not None:
        snapshot.etl = _read_file(
            repo,
            snapshot.commit,
            ContextFile(kind="etl", path=pipeline_path, pipeline=pipeline_id),
            max_bytes=max_bytes,
            known=known,
            today=day,
            history=history,
        )
    _cross_check(snapshot)
    present = any(item.status != "missing" for item in snapshot.files())
    if ref_note and present:
        snapshot.notes.append(ref_note)
    if snapshot.ref == "HEAD" and present:
        snapshot.notes.append(
            f"repo.base_branch {cfg.repo.base_branch} was not found in the repository; the context files "
            "were read from HEAD."
        )
    snapshot.notes = clean_notes([*snapshot.notes, *_notes(snapshot)])
    return snapshot


def edited_in_working_tree(cfg: HealerConfig, commit: str, paths: Iterable[str]) -> list[str]:
    """The ``paths`` whose working-tree copy differs from ``commit`` (tracked files; best effort)."""
    wanted = [path for path in paths if path]
    if not wanted:
        return []
    out = _git(cfg.repo_root, "diff", "--name-only", "--relative", "-z", commit, "--", *wanted)
    return sorted(name for name in (out or "").split("\0") if name)


@dataclass
class ContextReport:
    """Every context file of the base branch's commit (what ``dag-healer doctor`` shows)."""

    commit: str | None
    ref: str | None
    organization: ContextFile | None
    etl: list[ContextFile]
    #: ETL Information files of the working tree that the commit does not have.
    uncommitted: list[str]
    #: Set when there is no commit to read (not a repository, no commit of the base branch).
    problem: str | None = None
    #: Why the base branch and its ``origin`` copy were not the same commit, when it matters.
    note: str | None = None


def read_all(
    cfg: HealerConfig, *, known_secrets: Iterable[str] | None = None, today: date | None = None
) -> ContextReport:
    """Read the Organization Context and every ETL Information of the base branch's commit.

    ``<context.etl_dir>/*.md`` of the commit, in name order; a file of the working tree that is
    not in the commit is listed in ``uncommitted``. Never raises.
    """
    repo = cfg.repo_root
    known = list(known_secrets) if known_secrets is not None else known_secret_values(cfg)
    etl_dir = cfg.context.etl_dir
    folder = repo / etl_dir
    local = sorted(path.name for path in folder.glob("*.md") if path.is_file()) if folder.is_dir() else []
    if not is_repository(repo):
        return ContextReport(None, None, None, [], [], f"{repo} is not a git repository")
    found = base_commit(repo, cfg.repo.base_branch)
    if found is None:
        return ContextReport(
            None, None, None, [], [], f"the repository has no commit of {cfg.repo.base_branch}"
        )
    commit, ref, ref_note = found
    day = _today(today)
    max_bytes = cfg.context.max_file_bytes
    organization = _read_file(
        repo,
        commit,
        ContextFile(kind="organization", path=cfg.context.organization),
        max_bytes=max_bytes,
        known=known,
        today=day,
        history=False,
    )
    files: list[ContextFile] = []
    entries = _entries(repo, commit, etl_dir + "/") or []
    committed: set[str] = set()
    for entry in sorted(entries, key=lambda item: item.path):
        name = entry.path.rsplit("/", 1)[-1]
        if not name.endswith(".md"):
            continue
        committed.add(name)
        if entry.kind == "tree":
            continue
        pipeline = name[: -len(".md")]
        item = ContextFile(kind="etl", path=f"{etl_dir}/{name}", pipeline=pipeline)
        if etl_path(cfg, pipeline) is None:
            item.status = "error"
            item.reason = "its name is not a pipeline id"
        else:
            _read_entry(repo, commit, item, entry, max_bytes=max_bytes, known=known, today=day)
        files.append(item)
    for item in files:
        _cross_check(ContextSnapshot(commit=commit, ref=ref, organization=organization, etl=item))
    uncommitted = [f"{etl_dir}/{name}" for name in local if name not in committed]
    return ContextReport(commit, ref, organization, files, uncommitted, note=ref_note)


__all__ = [
    "CREDENTIAL_ENVS",
    "ContextReport",
    "base_commit",
    "MAX_NOTES_CHARS",
    "MAX_NOTE_CHARS",
    "clean_notes",
    "edited_in_working_tree",
    "etl_path",
    "file_note",
    "is_repository",
    "known_secret_values",
    "read_all",
    "read_context",
]
