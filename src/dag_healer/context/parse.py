"""Parse an Organization Context or an ETL Information file: its front matter and its rules.

The syntax (``docs/context.md``):

* the file opens with a YAML front matter between two ``---`` lines: ``owner``, ``reviewed`` (a
  date) and ``review_every`` (``90d``, ``12w``, ``6m``, ``1y``); an ETL Information also declares
  its ``rule_prefix`` (``LAB``) and may repeat its ``pipeline`` id;
* a rule is a list item (``-``, ``*`` or ``+``, indented 3 spaces at most) whose text starts with
  its ID and a colon: ``- LAB-1: A finalized result always has a value.`` An ID is a prefix
  (``ORG`` in the Organization Context, the ``rule_prefix`` in an ETL Information), ``-`` and a
  number from 1 to 9999 without leading zeros. The lines indented under the item, and the lines
  that follow it without a blank line, belong to the rule;
* an ETL Information overrides a rule of the Organization Context by naming it:
  ``- PHA-2 (overrides ORG-3): ...``;
* fenced code blocks and HTML comments (``<!-- ... -->``) hold no rules; comments are also left
  out of what the model reads. A comment that is never closed runs to the end of the file, as
  in a browser (a problem says so).

Parsing is strict: a list item or a heading that looks like a rule ID but is not one (``ORG1``,
``org-1``, ``ORG-01``, ``ORG-0``, ``**ORG-1**``, no colon, a heading, a nested item, a numbered
item), an ID of another file's
prefix, a duplicate ID (the first definition counts), an override of something other than an ORG
rule and every front matter problem are reported as ``problems``, with their line. Only
well-formed rules are kept. Nothing here reads files, git or the clock.
"""

from __future__ import annotations

import calendar
import re
import unicodedata
from collections.abc import Iterable, Mapping
from dataclasses import dataclass, field
from datetime import date, datetime
from typing import Any

import yaml

from dag_healer.context.model import ORG_PREFIX, RULE_ID_PATTERN, Rule
from dag_healer.context.prompt import neutralise_tags

#: The front matter keys of each kind of file (``rule_prefix`` is required in an ETL Information).
ORGANIZATION_KEYS: tuple[str, ...] = ("owner", "reviewed", "review_every")
ETL_KEYS: tuple[str, ...] = ("owner", "reviewed", "review_every", "rule_prefix", "pipeline")
#: A rule prefix: 2 to 10 capital letters and digits, a letter first.
PREFIX_RE = re.compile(r"^[A-Z][A-Z0-9]{1,9}$")
#: ``review_every``: a number and a unit (days, weeks, months, years).
PERIOD_RE = re.compile(r"^\s*(?P<count>[1-9][0-9]{0,3})\s*(?P<unit>[dwmy])\s*$", re.IGNORECASE)
#: At most this many problems are reported per file (the rest are counted).
MAX_PROBLEMS = 30
#: A problem is one line of at most this many characters (:func:`clean_problem`).
MAX_PROBLEM_CHARS = 300

_DELIMITER = "---"
_FENCE_RE = re.compile(r"^[ ]{0,3}(?P<fence>`{3,}|~{3,})")
_BULLET_RE = re.compile(r"^(?P<indent>[ ]{0,3})[-*+](?:[ \t]+(?P<rest>.*))?$")
_ORDERED_RE = re.compile(r"^[ ]{0,3}[0-9]{1,9}[.)](?:[ \t]+(?P<rest>.*))?$")
_DEEP_BULLET_RE = re.compile(r"^(?:[ ]{4,}|\t)[ \t]*[-*+][ \t]+(?P<rest>.*)$")
_HEADING_RE = re.compile(r"^[ ]{0,3}#{1,6}[ \t]+(?P<rest>.*)$")
_RULE_RE = re.compile(
    rf"^(?P<id>{RULE_ID_PATTERN})(?:[ \t]*\((?P<clause>[^()\n]*)\))?[ \t]*:(?:[ \t]*(?P<text>.*))?$"
)
_LOOSE_ID_RE = re.compile(
    r"^(?P<bold>\*\*|__)?(?P<letters>[A-Za-z][A-Za-z0-9]{1,9}?)(?P<sep>[-_ ]?)(?P<number>[0-9]{1,6})"
    r"(?![0-9])(?P<after>.*)$"
)
_OVERRIDES_RE = re.compile(
    rf"^overrides[ \t]+(?P<ids>{RULE_ID_PATTERN}(?:[ \t]*,[ \t]*{RULE_ID_PATTERN})*)[ \t]*$",
    re.IGNORECASE,
)
_ID_IN_TEXT_RE = re.compile(RULE_ID_PATTERN)
_COMMENT_OPEN = "<!--"
_COMMENT_CLOSE = "-->"
_BLANK_RUNS_RE = re.compile(r"\n{3,}")
#: Characters that end a line in a text (they become spaces in a problem).
_LINE_BREAKS = frozenset("\n\r\x0b\x0c\x1c\x1d\x1e\x85\u2028\u2029")
#: Front matter keys whose values are text even when YAML reads them as something else
#: (``rule_prefix: OFF`` is the boolean False, ``pipeline: 2024_01`` the number 202401).
_TEXT_KEYS: tuple[str, ...] = ("rule_prefix", "pipeline")


@dataclass
class FrontMatter:
    """The front matter of a context file, as read (values only when they are valid)."""

    owner: str | None = None
    reviewed: date | None = None
    review_every: str | None = None
    rule_prefix: str | None = None
    pipeline: str | None = None


@dataclass
class ParsedFile:
    """A parsed context file: its front matter, its rules, the text the model reads, the problems."""

    front: FrontMatter
    rules: list[Rule] = field(default_factory=list)
    problems: list[str] = field(default_factory=list)
    #: The body without the front matter and without HTML comments (not masked).
    text: str = ""


# --------------------------------------------------------------------------- helpers


def review_due(reviewed: date, every: str) -> date | None:
    """When a file reviewed on ``reviewed`` must be reviewed again (``None`` for a bad period).

    Months and years are calendar ones: a day that the target month lacks becomes its last day.
    A due date after 9999-12-31 is ``None`` too.
    """
    match = PERIOD_RE.match(every or "")
    if match is None:
        return None
    count, unit = int(match.group("count")), match.group("unit").lower()
    try:
        if unit in ("d", "w"):
            from datetime import timedelta

            return reviewed + timedelta(days=count * (7 if unit == "w" else 1))
        months = count * (12 if unit == "y" else 1)
        year, month = divmod(reviewed.month - 1 + months, 12)
        year += reviewed.year
        month += 1
        day = min(reviewed.day, calendar.monthrange(year, month)[1])
        return date(year, month, day)
    except (OverflowError, ValueError):  # past the last date Python knows (9999-12-31)
        return None


def _blank(comment: str, filler: str) -> str:
    """What a comment leaves behind: its line breaks, or ``filler`` when it holds none."""
    breaks = comment.count("\n")
    return "\n" * breaks if breaks else filler


def _comments_out(text: str, filler: str = " ") -> tuple[str, int | None]:
    """``text`` without its HTML comments (:func:`_blank`), and where a comment never closed opens.

    One pass over the text: a comment that is never closed runs to its end, as in a browser.
    """
    parts: list[str] = []
    position = 0
    while True:
        start = text.find(_COMMENT_OPEN, position)
        if start < 0:
            parts.append(text[position:])
            return "".join(parts), None
        parts.append(text[position:start])
        end = text.find(_COMMENT_CLOSE, start + len(_COMMENT_OPEN))
        if end < 0:
            parts.append(_blank(text[start:], filler))
            return "".join(parts), start
        parts.append(_blank(text[start : end + len(_COMMENT_CLOSE)], filler))
        position = end + len(_COMMENT_CLOSE)


def unclosed_comment_line(text: str) -> int | None:
    """The line (1-based) where a comment that is never closed opens, or ``None``."""
    _without, start = _comments_out(text)
    return None if start is None else text[:start].count("\n") + 1


def strip_comments(text: str, *, filler: str = " ") -> str:
    """``text`` with every HTML comment replaced by its line breaks, or by ``filler`` on one line.

    The model reads the text with one space for such a comment, so a comment never joins the
    words around it; ``filler=""`` joins them (the secret check of
    :mod:`dag_healer.context.read` looks at both). A comment that is never closed runs to the
    end of the text.
    """
    return _comments_out(text, filler)[0]


def clean_problem(text: str, limit: int = MAX_PROBLEM_CHARS) -> str:
    """A problem (or a note) as one short line that no reader can mistake for something else.

    Line breaks become spaces, other control and format characters (escapes of a terminal, bidi
    overrides) their ``\\x``/``\\u`` code, the tags of prompt blocks are neutralised
    (:func:`~dag_healer.context.prompt.neutralise_tags`), and the line is cut to ``limit``
    characters (:data:`MAX_PROBLEM_CHARS`).
    """
    shown: list[str] = []
    for char in neutralise_tags(str(text)):
        if char in _LINE_BREAKS:
            shown.append(" ")
        elif unicodedata.category(char) in ("Cc", "Cf"):
            code = ord(char)
            shown.append(f"\\x{code:02x}" if code < 0x100 else f"\\u{code:04x}")
        else:
            shown.append(char)
    line = "".join(shown)
    return line if len(line) <= limit else line[: limit - 3] + "..."


def model_text(body: str) -> str:
    """What the model reads of a body: no HTML comments, no runs of blank lines, no edge blanks."""
    lines = [line.rstrip() for line in strip_comments(body).split("\n")]
    return _BLANK_RUNS_RE.sub("\n\n", "\n".join(lines)).strip("\n")


def _as_date(value: Any) -> date | None:
    if isinstance(value, datetime):
        return value.date()
    if isinstance(value, date):
        return value
    if isinstance(value, str):
        try:
            return date.fromisoformat(value.strip())
        except ValueError:
            return None
    return None


def _as_text(raw: str) -> Any:
    """``raw`` read as YAML with every value as text (``None`` when it cannot be read at all)."""
    try:
        return yaml.load(raw, Loader=yaml.BaseLoader)
    except (yaml.YAMLError, ValueError, TypeError, RecursionError):
        return None


class AliasesRefused(ValueError):
    """The front matter uses YAML anchors or aliases (they could expand it many times over)."""


def _refuse_aliases(raw: str) -> None:
    """Raise :class:`AliasesRefused` when ``raw`` defines an anchor or uses an alias.

    Only the parser's events are read, so nothing is expanded; invalid YAML raises its
    :class:`yaml.YAMLError` here.
    """
    for event in yaml.parse(raw, Loader=yaml.SafeLoader):
        if isinstance(event, yaml.AliasEvent) or getattr(event, "anchor", None) is not None:
            raise AliasesRefused("the front matter uses YAML anchors or aliases")


def _key_lines(raw: str) -> dict[str, int]:
    """The line (1-based, within ``raw``) of each top-level key of a front matter."""
    try:
        node = yaml.compose(raw, Loader=yaml.SafeLoader)
    except (yaml.YAMLError, ValueError, RecursionError):
        return {}
    if not isinstance(node, yaml.MappingNode):
        return {}
    return {
        str(key.value): key.start_mark.line + 1
        for key, _value in node.value
        if isinstance(key, yaml.ScalarNode)
    }


def _load_front(raw: str) -> Any:
    """The front matter as YAML reads it; raises :class:`yaml.YAMLError` for invalid YAML and
    :class:`AliasesRefused` for anchors and aliases.

    A value YAML cannot build (``2026-02-30``, ``!!int abc``) makes every value text, so its key is
    reported, not the whole front matter; the values of :data:`_TEXT_KEYS` are always text.
    """
    _refuse_aliases(raw)
    try:
        data = yaml.safe_load(raw)
    except yaml.YAMLError:
        raise
    except (ValueError, TypeError, OverflowError, RecursionError) as exc:
        text = _as_text(raw)
        if text is None:
            raise yaml.YAMLError(str(exc)) from exc
        return text
    if isinstance(data, Mapping):
        odd = [key for key in _TEXT_KEYS if data.get(key) is not None and not isinstance(data.get(key), str)]
        text = _as_text(raw) if odd else None
        if isinstance(text, Mapping):
            data = {**data, **{key: text[key] for key in odd if isinstance(text.get(key), str)}}
    return data


def split_front_matter(text: str) -> tuple[Mapping[str, Any] | None, str, int, list[str]]:
    """``(front matter, body, first line of the body, problems)``.

    The front matter is the YAML between a first line ``---`` and the next ``---`` (or ``...``)
    line. Without one the whole text is the body. A front matter with YAML anchors or aliases is
    refused whole. Never raises. The line of each key is in :func:`front_matter_lines`.
    """
    lines = text.split("\n")
    if not lines or lines[0].strip() != _DELIMITER:
        return None, text, 1, ["no front matter: the file must start with a --- line (see docs/context.md)"]
    for index in range(1, len(lines)):
        if lines[index].strip() in (_DELIMITER, "..."):
            raw = "\n".join(lines[1:index])
            body = "\n".join(lines[index + 1 :])
            try:
                data = _load_front(raw) if raw.strip() else {}
            except AliasesRefused:
                return None, body, index + 2, [ALIASES_PROBLEM]
            except yaml.YAMLError as exc:
                mark = getattr(exc, "problem_mark", None)
                where = f" (line {mark.line + 2})" if mark is not None else ""
                return None, body, index + 2, [f"the front matter is not valid YAML{where}"]
            except (ValueError, TypeError, RecursionError):
                return None, body, index + 2, ["the front matter is not valid YAML"]
            if not isinstance(data, Mapping):
                return None, body, index + 2, ["the front matter must be a mapping of keys and values"]
            return data, body, index + 2, []
    return None, text, 1, ["the front matter has no closing --- line"]


#: The problem of a front matter with YAML anchors or aliases.
ALIASES_PROBLEM = "the front matter uses YAML anchors or aliases, which dag-healer does not read"


def front_matter_lines(text: str) -> dict[str, int]:
    """The line (1-based, within the file) of each top-level key of the front matter of ``text``."""
    lines = text.split("\n")
    if not lines or lines[0].strip() != _DELIMITER:
        return {}
    for index in range(1, len(lines)):
        if lines[index].strip() in (_DELIMITER, "..."):
            return {key: line + 1 for key, line in _key_lines("\n".join(lines[1:index])).items()}
    return {}


def read_front_matter(
    data: Mapping[str, Any] | None,
    *,
    kind: str,
    pipeline: str | None,
    key_lines: Mapping[str, int] | None = None,
) -> tuple[FrontMatter, list[str]]:
    """The valid values of a front matter and the problems of the others.

    The problems never quote the file: an unknown key is named by its line (``key_lines``).
    """
    front = FrontMatter()
    problems: list[str] = []
    if data is None:
        return front, problems
    known = ETL_KEYS if kind == "etl" else ORGANIZATION_KEYS
    for key in data:
        if str(key) not in known:
            line = (key_lines or {}).get(str(key))
            where = f" on line {line}" if line else ""
            problems.append(f"front matter: unknown key{where} (known: {', '.join(known)})")
    owner = data.get("owner")
    if isinstance(owner, str) and owner.strip():
        front.owner = owner.strip()
    else:
        problems.append(
            "front matter: owner is missing (who keeps the file up to date, such as @acme/data-eng)"
        )
    reviewed = _as_date(data.get("reviewed"))
    if reviewed is not None:
        front.reviewed = reviewed
    else:
        problems.append("front matter: reviewed must be the date of the last review (YYYY-MM-DD)")
    every = data.get("review_every")
    if isinstance(every, str) and PERIOD_RE.match(every):
        front.review_every = every.strip().lower()
    else:
        problems.append("front matter: review_every must be a period such as 90d, 12w, 6m or 1y")
    if kind == "etl":
        prefix = data.get("rule_prefix")
        if not isinstance(prefix, str) or not PREFIX_RE.match(prefix.strip()):
            problems.append(
                "front matter: rule_prefix must be 2 to 10 capital letters or digits, a letter first "
                "(such as LAB); no rule of the file can be cited without it"
            )
        elif prefix.strip() == ORG_PREFIX:
            problems.append("front matter: rule_prefix cannot be ORG (the Organization Context's prefix)")
        else:
            front.rule_prefix = prefix.strip()
        named = data.get("pipeline")
        if named is not None and not isinstance(named, str):
            problems.append("front matter: pipeline must be text, the pipeline id of the file")
        elif named is not None:
            if named.strip() != (pipeline or ""):
                wanted = f"{pipeline!r}, " if pipeline else ""
                problems.append(f"front matter: pipeline is not {wanted}the pipeline id of this file")
            else:
                front.pipeline = named.strip()
    return front, problems


# --------------------------------------------------------------------------- rules


def _loose_problem(text: str, prefixes: Iterable[str], line: int, *, where: str) -> str | None:
    """A problem for text that starts like a rule ID of ``prefixes`` but is not one (else None)."""
    match = _LOOSE_ID_RE.match(text)
    if match is None:
        return None
    letters, sep, number = match.group("letters"), match.group("sep"), match.group("number")
    wanted = {prefix.upper() for prefix in prefixes}
    if letters.upper() not in wanted:
        return None
    after = match.group("after")
    bold = match.group("bold") or after.startswith(("**", "__"))
    marked = after.lstrip().startswith((":", "(")) or (bool(bold) and ":" in after[:4])
    exact = letters == letters.upper() and sep in ("-", "_", "")
    if not (marked or bold or exact):
        return None  # prose that starts with the prefix as a word: "Lab 2 sends its results by 06:00"
    written = f"{letters}{sep}{number}"
    if int(number) == 0:
        return f"line {line}: {written}: rule numbers start at 1"
    fixed = f"{letters.upper()}-{int(number)}"
    if bold:
        return f"line {line}: write the rule ID without bold, as in - {fixed}: ..."
    if letters != letters.upper() or sep != "-" or number != str(int(number)):
        return f"line {line}: {written} is not a rule ID; write it as {fixed}"
    if int(number) > 9999:
        return f"line {line}: {letters}{sep}{number}: rule numbers go up to 9999"
    if where == "heading":
        return f"line {line}: {fixed} is in a heading; a rule is a list item: - {fixed}: ..."
    if where == "nested":
        return f"line {line}: {fixed} is indented; a rule is a list item of its own (3 spaces at most)"
    if not after.lstrip().startswith((":", "(")):
        return f"line {line}: put a colon after the rule ID, as in - {fixed}: ..."
    return f"line {line}: {fixed} is not a well-formed rule; write it as - {fixed}: ..."


class _Rules:
    """The state of one pass over a body (see :func:`parse_rules`)."""

    def __init__(self, kind: str, prefix: str | None) -> None:
        self.kind = kind
        self.prefix = prefix
        self.rules: list[Rule] = []
        self.problems: list[str] = []
        self.seen: dict[str, int] = {}
        self.current: dict[str, Any] | None = None
        self.indent = 0  # of the current rule's item

    @property
    def prefixes(self) -> tuple[str, ...]:
        if self.kind == "organization":
            return (ORG_PREFIX,)
        return (ORG_PREFIX, self.prefix) if self.prefix else (ORG_PREFIX,)

    def finish(self) -> None:
        current, self.current = self.current, None
        if current is None:
            return
        text = " ".join(part for part in current["parts"] if part).strip()
        if not text:
            self.problems.append(f"line {current['line']}: {current['id']} has no text after the colon")
            self.seen.pop(current["id"], None)  # a later definition of the ID counts
            return
        self.rules.append(Rule(current["id"], text, current["line"], tuple(current["overrides"])))

    def _overrides(self, rule_id: str, clause: str | None, line: int) -> list[str] | None:
        """The ORG IDs a rule overrides (``None``: the clause is a problem, already reported)."""
        if clause is None:
            return []
        match = _OVERRIDES_RE.match(clause.strip())
        if match is None:
            self.problems.append(
                f"line {line}: the note in parentheses after {rule_id} is not understood; the only note is "
                "(overrides ORG-n)"
            )
            return None
        if self.kind == "organization":
            self.problems.append(
                f"line {line}: {rule_id} overrides a rule, but only an ETL Information overrides rules "
                "of the Organization Context"
            )
            return None
        targets = [target.upper() for target in _ID_IN_TEXT_RE.findall(match.group("ids").upper())]
        wrong = [target for target in targets if not target.startswith(ORG_PREFIX + "-")]
        if wrong:
            self.problems.append(
                f"line {line}: {rule_id} overrides {', '.join(wrong)}; an ETL Information overrides "
                "rules of the Organization Context (ORG-n) only"
            )
            return None
        return list(dict.fromkeys(targets))

    def item(self, rest: str, line: int, indent: int = 0) -> None:
        self.finish()
        self.indent = indent
        match = _RULE_RE.match(rest)
        if match is None:
            problem = _loose_problem(rest, self.prefixes, line, where="item")
            if problem:
                self.problems.append(problem)
            return
        rule_id = match.group("id")
        prefix = rule_id.split("-", 1)[0]
        if self.kind == "organization" and prefix != ORG_PREFIX:
            self.problems.append(
                f"line {line}: {rule_id} is not an ID of the Organization Context, whose rules are ORG-1, "
                "ORG-2, ... (write a code that is not a rule in backticks)"
            )
            return
        if self.kind == "etl" and prefix == ORG_PREFIX:
            self.problems.append(
                f"line {line}: {rule_id} is an ID of the Organization Context; an ETL Information "
                f"overrides it with - {self.prefix or 'ABC'}-n (overrides {rule_id}): ..."
            )
            return
        if self.kind == "etl" and self.prefix is None:  # the front matter problem says why
            return
        if self.kind == "etl" and prefix != self.prefix:
            self.problems.append(
                f"line {line}: {rule_id} does not use the rule_prefix of this file ({self.prefix}-1, "
                f"{self.prefix}-2, ...); write a code that is not a rule in backticks"
            )
            return
        overrides = self._overrides(rule_id, match.group("clause"), line)
        if overrides is None:
            return
        if rule_id in self.seen:
            self.problems.append(
                f"line {line}: {rule_id} is defined twice (lines {self.seen[rule_id]} and {line}); the first "
                "definition counts"
            )
            return
        self.seen[rule_id] = line
        self.current = {
            "id": rule_id,
            "line": line,
            "parts": [match.group("text") or ""],
            "overrides": overrides,
        }

    def ordered(self, rest: str, line: int) -> None:
        """A numbered item: never a rule; one that starts like a rule is reported."""
        self.finish()
        match = _RULE_RE.match(rest)
        if match is not None and match.group("id").split("-", 1)[0] in self.prefixes:
            rule_id = match.group("id")
            self.problems.append(
                f"line {line}: {rule_id} is in a numbered list; a rule is a bullet item: - {rule_id}: ..."
            )
            return
        problem = _loose_problem(rest, self.prefixes, line, where="item")
        if problem:
            self.problems.append(problem)

    def heading(self, rest: str, line: int) -> None:
        self.finish()
        match = _RULE_RE.match(rest)
        if match is not None and match.group("id").split("-", 1)[0] in self.prefixes:
            rule_id = match.group("id")
            self.problems.append(
                f"line {line}: {rule_id} is in a heading; a rule is a list item: - {rule_id}: ..."
            )
            return
        problem = _loose_problem(rest, self.prefixes, line, where="heading")
        if problem:
            self.problems.append(problem)

    def nested(self, rest: str, line: int) -> bool:
        """Report a nested item that looks like a rule; True when it did."""
        match = _RULE_RE.match(rest)
        if match is not None and match.group("id").split("-", 1)[0] in self.prefixes:
            rule_id = match.group("id")
            self.problems.append(
                f"line {line}: {rule_id} is indented; a rule is a list item of its own (3 spaces at most)"
            )
            return True
        problem = _loose_problem(rest, self.prefixes, line, where="nested")
        if problem:
            self.problems.append(problem)
            return True
        return False


def parse_rules(
    body: str, *, kind: str, prefix: str | None, first_line: int = 1
) -> tuple[list[Rule], list[str]]:
    """The well-formed rules of ``body`` and the problems found (lines count from ``first_line``).

    ``kind`` is ``organization`` (rules ``ORG-n``) or ``etl`` (rules ``<prefix>-n``; without a
    valid ``prefix`` no rule is kept). Rule texts are not masked here.
    """
    state = _Rules(kind, prefix)
    fence: str | None = None
    for offset, raw in enumerate(strip_comments(body).split("\n")):
        line = first_line + offset
        text = raw.rstrip()
        fence_match = _FENCE_RE.match(text)
        if fence_match is not None:
            marker = fence_match.group("fence")
            if fence is None:
                state.finish()
                fence = marker[0] * len(marker)
            elif marker[0] == fence[0] and len(marker) >= len(fence):
                fence = None
            continue
        if fence is not None:
            continue
        if not text.strip():
            state.finish()
            continue
        bullet = _BULLET_RE.match(text)
        indent = len(bullet.group("indent")) if bullet is not None else 0
        if bullet is not None and (state.current is None or indent < state.indent + 2):
            state.item((bullet.group("rest") or "").strip(), line, indent)
            continue
        if bullet is not None:  # an item of a list inside the rule (indented under its text)
            if not state.nested((bullet.group("rest") or "").strip(), line):
                state.current["parts"].append(text.strip())  # type: ignore[index]
            continue
        ordered = _ORDERED_RE.match(text)
        if ordered is not None and (
            state.current is None or len(text) - len(text.lstrip()) < state.indent + 2
        ):
            state.ordered((ordered.group("rest") or "").strip(), line)
            continue
        heading = _HEADING_RE.match(text)
        if heading is not None:
            state.heading(heading.group("rest").strip(), line)
            continue
        deep = _DEEP_BULLET_RE.match(text)
        if deep is not None and state.nested(deep.group("rest").strip(), line):
            continue
        if state.current is not None:  # an indented line, or a lazy continuation of the item
            state.current["parts"].append(text.strip())
    state.finish()
    if kind == "etl" and not prefix:
        state.rules = []
    return state.rules, state.problems


def parse_context_file(text: str, *, kind: str, pipeline: str | None = None) -> ParsedFile:
    """Parse a whole context file (front matter, rules, the model's text); never raises."""
    data, body, first_line, problems = split_front_matter(text)
    key_lines = front_matter_lines(text) if data else {}
    front, front_problems = read_front_matter(data, kind=kind, pipeline=pipeline, key_lines=key_lines)
    problems += front_problems
    prefix = ORG_PREFIX if kind == "organization" else front.rule_prefix
    rules, rule_problems = parse_rules(body, kind=kind, prefix=prefix, first_line=first_line)
    problems += rule_problems
    unclosed = unclosed_comment_line(body)
    if unclosed is not None:
        problems.append(
            f"line {first_line + unclosed - 1}: an HTML comment opens here and is never closed: the rest of "
            "the file is left out"
        )
    problems = [clean_problem(problem) for problem in problems]
    if len(problems) > MAX_PROBLEMS:
        problems = [*problems[:MAX_PROBLEMS], f"... and {len(problems) - MAX_PROBLEMS} more problems"]
    return ParsedFile(front=front, rules=rules, problems=problems, text=model_text(body))


__all__ = [
    "ALIASES_PROBLEM",
    "ETL_KEYS",
    "MAX_PROBLEMS",
    "MAX_PROBLEM_CHARS",
    "ORGANIZATION_KEYS",
    "PERIOD_RE",
    "PREFIX_RE",
    "FrontMatter",
    "ParsedFile",
    "clean_problem",
    "front_matter_lines",
    "model_text",
    "parse_context_file",
    "parse_rules",
    "read_front_matter",
    "review_due",
    "split_front_matter",
    "strip_comments",
    "unclosed_comment_line",
]
