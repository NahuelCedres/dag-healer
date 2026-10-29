"""What dag-healer read of the Organization Context and the ETL Information for one pipeline.

:class:`ContextSnapshot` is the result of :func:`dag_healer.context.read.read_context`: the commit
it read (``commit``, ``ref``), the Organization Context and the pipeline's ETL Information
(:class:`ContextFile`, each ``read``, ``refused``, ``missing`` or in ``error``) and notes for the
incident. Everything in it is already masked, so it can be stored as is (``to_dict`` /
``from_dict``: the store's ``healer_context_snapshots`` rows), shown and sent to the model.

The rules of the files that were read are the ones the model may cite. :meth:`ContextSnapshot.check`
splits a list of cited IDs into the applied rules (:class:`RuleRef`: the ID, a short text and
``path@sha``) and the unknown ones, which are flagged and never count as applied.
"""

from __future__ import annotations

import re
from collections.abc import Iterable, Mapping
from dataclasses import dataclass, field
from typing import Any, Literal

from dag_healer.masking import mask_text

FileKind = Literal["organization", "etl"]
FileStatus = Literal["read", "refused", "missing", "error"]
#: Why a file was refused: it holds a secret, it is too large, it is no UTF-8 text, it is a
#: symbolic link or no regular file.
Refusal = Literal["secret", "size", "encoding", "link", "type"]
_REFUSALS: tuple[str, ...] = ("secret", "size", "encoding", "link", "type")

#: The rule ID prefix of the Organization Context.
ORG_PREFIX = "ORG"
#: A rule ID: a prefix of 2 to 10 capital letters and digits (a letter first), ``-``, a number
#: from 1 to 9999 without leading zeros (``ORG-1``, ``LAB-12``).
RULE_ID_PATTERN = r"[A-Z][A-Z0-9]{1,9}-[1-9][0-9]{0,3}"
RULE_ID_RE = re.compile(rf"^{RULE_ID_PATTERN}$")
#: A rule's short text (PR.md, the decision log) has at most this many characters.
SHORT_TEXT_CHARS = 200
#: At most this many cited IDs of one answer are kept (applied and unknown together).
MAX_CITATIONS = 50
#: An unknown cited ID is kept masked and cut to this many characters.
MAX_UNKNOWN_ID_CHARS = 40
#: The names of the two files in sentences.
FILE_TITLES: dict[str, str] = {"organization": "Organization Context", "etl": "ETL Information"}


def short_text(text: str, limit: int = SHORT_TEXT_CHARS) -> str:
    """``text`` on one line, cut to ``limit`` characters (with ``...``)."""
    line = " ".join(str(text).split())
    return line if len(line) <= limit else line[: limit - 3].rstrip() + "..."


def short_sha(sha: str | None) -> str:
    """The first 7 characters of a commit sha (``""`` without one)."""
    return (sha or "")[:7]


def normalise_id(value: Any) -> str:
    """A cited ID as compared: without whitespace, backticks, brackets or bold, in capitals."""
    text = str(value).strip().strip("`*_[]()<>\"'.,;:").strip()
    return text.upper()


@dataclass(frozen=True)
class Rule:
    """One rule of a context file: its ID, its text (masked, on one line) and its line."""

    id: str
    text: str
    line: int
    overrides: tuple[str, ...] = ()

    def to_dict(self) -> dict[str, Any]:
        data: dict[str, Any] = {"id": self.id, "text": self.text, "line": self.line}
        if self.overrides:
            data["overrides"] = list(self.overrides)
        return data

    @classmethod
    def from_dict(cls, data: Mapping[str, Any]) -> Rule:
        return cls(
            id=str(data.get("id") or ""),
            text=str(data.get("text") or ""),
            line=int(data.get("line") or 0),
            overrides=tuple(str(item) for item in data.get("overrides") or ()),
        )


@dataclass
class ContextFile:
    """One context file as read from the base branch's commit.

    ``status``: ``read`` (parsed and masked; ``text`` is what the model reads), ``refused`` (it
    holds a secret, is over the size cap, is no UTF-8 text or no regular file: never sent;
    ``refusal`` says which, ``reason`` says why in a sentence), ``missing`` (not in the commit) or
    ``error`` (it could not be read).
    ``problems`` are what the parser reports (front matter, malformed or duplicate IDs, overrides);
    the file is still used, and only its well-formed rules can be cited. ``blob`` is the git blob
    id; ``changed_in`` / ``changed_at`` the last commit that changed it (best effort).
    """

    kind: FileKind
    path: str
    pipeline: str | None = None
    status: FileStatus = "missing"
    reason: str | None = None
    refusal: Refusal | None = None
    blob: str | None = None
    size: int | None = None
    changed_in: str | None = None
    changed_at: str | None = None
    owner: str | None = None
    reviewed: str | None = None
    review_every: str | None = None
    review_due: str | None = None
    stale: bool = False
    rule_prefix: str | None = None
    rules: list[Rule] = field(default_factory=list)
    problems: list[str] = field(default_factory=list)
    text: str = ""
    #: Set by :func:`dag_healer.context.read.read_context`: the file is in the working tree but
    #: not in the commit (it is not read from the working tree).
    uncommitted: bool = False

    @property
    def title(self) -> str:
        """``Organization Context`` or ``ETL Information``."""
        return FILE_TITLES[self.kind]

    @property
    def is_read(self) -> bool:
        return self.status == "read"

    @property
    def valid(self) -> bool:
        """Read, and the parser reported no problem."""
        return self.is_read and not self.problems

    def rule_ids(self) -> list[str]:
        return [rule.id for rule in self.rules]

    def to_dict(self) -> dict[str, Any]:
        data: dict[str, Any] = {"kind": self.kind, "path": self.path, "status": self.status}
        for name in (
            "pipeline",
            "reason",
            "refusal",
            "blob",
            "size",
            "changed_in",
            "changed_at",
            "owner",
            "reviewed",
            "review_every",
            "review_due",
            "rule_prefix",
        ):
            value = getattr(self, name)
            if value is not None:
                data[name] = value
        if self.stale:
            data["stale"] = True
        if self.uncommitted:
            data["uncommitted"] = True
        if self.is_read:
            data["rules"] = [rule.to_dict() for rule in self.rules]
            data["problems"] = list(self.problems)
            data["text"] = self.text
        return data

    @classmethod
    def from_dict(cls, data: Mapping[str, Any]) -> ContextFile:
        kind = "etl" if data.get("kind") == "etl" else "organization"
        status = (
            data.get("status") if data.get("status") in ("read", "refused", "missing", "error") else "error"
        )
        refusal = data.get("refusal")
        size = data.get("size")
        return cls(
            kind=kind,
            path=str(data.get("path") or ""),
            pipeline=_optional(data.get("pipeline")),
            status=status,
            reason=_optional(data.get("reason")),
            refusal=refusal if refusal in _REFUSALS else None,
            blob=_optional(data.get("blob")),
            size=int(size) if isinstance(size, int) else None,
            changed_in=_optional(data.get("changed_in")),
            changed_at=_optional(data.get("changed_at")),
            owner=_optional(data.get("owner")),
            reviewed=_optional(data.get("reviewed")),
            review_every=_optional(data.get("review_every")),
            review_due=_optional(data.get("review_due")),
            stale=bool(data.get("stale")),
            rule_prefix=_optional(data.get("rule_prefix")),
            rules=[Rule.from_dict(rule) for rule in data.get("rules") or () if isinstance(rule, Mapping)],
            problems=[str(problem) for problem in data.get("problems") or ()],
            text=str(data.get("text") or ""),
            uncommitted=bool(data.get("uncommitted")),
        )


def _optional(value: Any) -> str | None:
    return None if value is None else str(value)


@dataclass(frozen=True)
class RuleRef:
    """A cited rule that exists in what was read: what PR.md and the decision log show."""

    id: str
    text: str
    path: str
    sha: str | None
    overridden_by: str | None = None

    @property
    def where(self) -> str:
        """``healer/etl/lab_results_daily.md@71c3d0e`` (the path alone without a commit)."""
        return f"{self.path}@{short_sha(self.sha)}" if self.sha else self.path

    def to_dict(self) -> dict[str, Any]:
        data: dict[str, Any] = {"id": self.id, "text": self.text, "path": self.path, "sha": self.sha}
        if self.overridden_by:
            data["overridden_by"] = self.overridden_by
        return data

    @classmethod
    def from_dict(cls, data: Mapping[str, Any]) -> RuleRef:
        return cls(
            id=str(data.get("id") or ""),
            text=str(data.get("text") or ""),
            path=str(data.get("path") or ""),
            sha=_optional(data.get("sha")),
            overridden_by=_optional(data.get("overridden_by")),
        )


@dataclass
class Citations:
    """Cited rule IDs checked against what was read: ``applied`` exist, ``unknown`` do not."""

    applied: list[RuleRef] = field(default_factory=list)
    unknown: list[str] = field(default_factory=list)

    def __bool__(self) -> bool:
        return bool(self.applied or self.unknown)

    @property
    def applied_ids(self) -> list[str]:
        return [ref.id for ref in self.applied]

    def merge(self, other: Citations) -> Citations:
        """The citations of both, each ID once, in first-seen order."""
        applied = {ref.id: ref for ref in [*self.applied, *other.applied]}
        unknown = list(dict.fromkeys([*self.unknown, *other.unknown]))
        return Citations(list(applied.values()), [item for item in unknown if item not in applied])

    def to_dict(self) -> dict[str, Any]:
        return {"applied": [ref.to_dict() for ref in self.applied], "unknown": list(self.unknown)}

    @classmethod
    def from_dict(cls, data: Mapping[str, Any] | None) -> Citations:
        if not isinstance(data, Mapping):
            return cls()
        return cls(
            applied=[RuleRef.from_dict(ref) for ref in data.get("applied") or () if isinstance(ref, Mapping)],
            unknown=[str(item) for item in data.get("unknown") or ()],
        )


@dataclass
class ContextSnapshot:
    """The context files read for one pipeline at one commit (see the module docstring)."""

    commit: str | None = None
    ref: str | None = None
    organization: ContextFile | None = None
    etl: ContextFile | None = None
    notes: list[str] = field(default_factory=list)

    def files(self) -> list[ContextFile]:
        return [item for item in (self.organization, self.etl) if item is not None]

    def read_files(self) -> list[ContextFile]:
        return [item for item in self.files() if item.is_read]

    @property
    def has_files(self) -> bool:
        """True when a file was read (the model can then cite and stop for its rules)."""
        return bool(self.read_files())

    def overrides(self) -> dict[str, str]:
        """ORG ID -> the ETL rule that overrides it for this pipeline (well-formed overrides only)."""
        found: dict[str, str] = {}
        if self.etl is None or not self.etl.is_read:
            return found
        org_ids = (
            set(self.organization.rule_ids()) if self.organization and self.organization.is_read else set()
        )
        for rule in self.etl.rules:
            for target in rule.overrides:
                if target in org_ids:
                    found.setdefault(target, rule.id)
        return found

    def rules(self) -> dict[str, RuleRef]:
        """Every rule that can be cited: ID -> :class:`RuleRef` (the first definition of an ID)."""
        found: dict[str, RuleRef] = {}
        overridden = self.overrides()
        for item in self.read_files():
            for rule in item.rules:
                if rule.id in found:
                    continue
                found[rule.id] = RuleRef(
                    id=rule.id,
                    text=short_text(rule.text),
                    path=item.path,
                    sha=self.commit,
                    overridden_by=overridden.get(rule.id),
                )
        return found

    def check(self, cited: Iterable[Any]) -> Citations:
        """Split ``cited`` into the rules that exist in what was read and the unknown IDs.

        IDs are compared as :func:`normalise_id` writes them; each one counts once, at most
        :data:`MAX_CITATIONS` are kept, and an unknown one is masked and cut.
        """
        known = self.rules()
        applied: dict[str, RuleRef] = {}
        unknown: list[str] = []
        for value in cited or ():
            if len(applied) + len(unknown) >= MAX_CITATIONS:
                break
            rule_id = normalise_id(value)
            if not rule_id:
                continue
            if rule_id in known:
                applied.setdefault(rule_id, known[rule_id])
                continue
            shown = mask_text(rule_id)[:MAX_UNKNOWN_ID_CHARS]
            if shown and shown not in unknown:
                unknown.append(shown)
        return Citations(list(applied.values()), unknown)

    def summary(self) -> str:
        """``main@9e41c07: healer/organization.md (6 rules), healer/etl/x.md (3 rules)``."""
        where = f"{self.ref_name}@{short_sha(self.commit)}" if self.commit else "no commit"
        parts = []
        for item in self.files():
            if item.is_read:
                parts.append(f"{item.path} ({len(item.rules)} rule{'s' if len(item.rules) != 1 else ''})")
            elif item.status != "missing":
                parts.append(f"{item.path} ({item.status})")
        return f"{where}: {', '.join(parts)}" if parts else f"{where}: no context file"

    @property
    def ref_name(self) -> str:
        """The ref without ``refs/heads/`` or ``refs/remotes/`` (``main``, ``origin/main``, ``HEAD``)."""
        ref = self.ref or "HEAD"
        for prefix in ("refs/heads/", "refs/remotes/"):
            if ref.startswith(prefix):
                return ref[len(prefix) :]
        return ref

    def to_dict(self) -> dict[str, Any]:
        """JSON-safe dict (the ``data`` of a ``healer_context_snapshots`` row)."""
        return {
            "commit": self.commit,
            "ref": self.ref,
            "files": [item.to_dict() for item in self.files()],
            "notes": list(self.notes),
        }

    @classmethod
    def from_dict(cls, data: Mapping[str, Any] | None) -> ContextSnapshot:
        if not isinstance(data, Mapping):
            return cls()
        snapshot = cls(
            commit=_optional(data.get("commit")),
            ref=_optional(data.get("ref")),
            notes=[str(note) for note in data.get("notes") or ()],
        )
        for raw in data.get("files") or ():
            if not isinstance(raw, Mapping):
                continue
            item = ContextFile.from_dict(raw)
            if item.kind == "organization" and snapshot.organization is None:
                snapshot.organization = item
            elif item.kind == "etl" and snapshot.etl is None:
                snapshot.etl = item
        return snapshot


__all__ = [
    "FILE_TITLES",
    "MAX_CITATIONS",
    "ORG_PREFIX",
    "RULE_ID_PATTERN",
    "RULE_ID_RE",
    "Citations",
    "ContextFile",
    "ContextSnapshot",
    "FileKind",
    "FileStatus",
    "Refusal",
    "Rule",
    "RuleRef",
    "normalise_id",
    "short_sha",
    "short_text",
]
