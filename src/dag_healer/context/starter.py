"""Starter files of ``dag-healer init --context``: the Organization Context and the ETL Information.

``init --context`` writes one Organization Context and one ETL Information per pipeline it found.
The files hold the front matter (an owner to fill in, today's date as ``reviewed``, ``review_every:
90d``; an ETL Information also its ``pipeline`` and a ``rule_prefix`` of its own), the sections of
``docs/context.md`` and guiding questions as HTML comments. Comments are never parsed for rules and
never reach the model, so a starter file has no rule until the team writes one. An existing file is
never overwritten: :func:`write_starter_files` creates a file only when the path is free.
"""

from __future__ import annotations

import re
from collections.abc import Callable, Iterable
from dataclasses import dataclass
from datetime import date
from pathlib import Path

from dag_healer.context.model import ORG_PREFIX
from dag_healer.context.parse import PREFIX_RE, split_front_matter

WRITTEN = "written"
KEPT = "kept (the file exists)"
SKIPPED = "skipped (the pipeline id cannot name a file)"
#: What the owner placeholder says until the team names one.
OWNER_PLACEHOLDER = "@your-team"

_WORD_RE = re.compile(r"[A-Za-z0-9]+")

_ORGANIZATION = """\
---
owner: "{owner}"         # who keeps this file up to date
reviewed: {today}        # the date of the last review
review_every: 90d           # dag-healer flags the file once this period has passed
---

# Organization Context

<!--
dag-healer reads this file before it classifies a failure or proposes a fix, in every pipeline.
Each pipeline's ETL Information ({etl_dir}/<pipeline>.md) adds what is specific to it, and can
replace one of the rules below for that pipeline by naming its ID.

Answer the questions in plain language, then delete the comments you answered. A rule is a list
item that starts with its ID and a colon, such as "- ORG-1: ...". The model cites the IDs of the
rules it applies. Rules can make dag-healer more careful, never less: they never turn a failure
into DATA or CODE on their own.

Never write secrets here: a file that holds one is not sent to the model. Personal data is
masked before any prompt, but the file is better without it. See docs/context.md.
-->

## What we do

<!-- What does the business do? Who uses the data that these pipelines produce, and what for? -->

## Systems and owners

<!--
Which systems produce the data, and which team owns each one? How do you reach them?
Which producers are external (a client, a vendor)? Who talks to them, and who never should?
-->

## Glossary

<!-- The words, codes and abbreviations a newcomer would not know, one line each. -->

## Regulations

<!--
Which rules apply to the data (privacy, health, finance)? What must never appear in a sample,
a log, a test or a pull request?
-->

## Rules for every pipeline

<!--
What must always hold, whatever the pipeline? What may a fix never do? For example:

- ORG-1: Personal data (names, e-mail addresses, phone numbers) is masked everywhere: samples,
  alerts, logs, tests and pull requests. Tests use made-up values.
- ORG-2: A wrong number in a report is worse than a late one. Invalid rows fail the task with a
  clear message; they are never dropped or fixed silently.
- ORG-3: A fix is the smallest change that makes the failing test pass, and files from before a
  change must still load.
-->
"""

_ETL = """\
---
pipeline: "{pipeline}"
owner: "{owner}"         # who keeps this file up to date
reviewed: {today}        # the date of the last review
review_every: 90d           # dag-healer flags the file once this period has passed
rule_prefix: "{prefix}"          # the IDs of this file's rules: {prefix}-1, {prefix}-2, ...
---

# {pipeline}

Builds on the Organization Context ({organization}).

<!--
dag-healer reads this file, with the Organization Context, before it classifies a failure of
{pipeline} or proposes a fix. Answer the questions in plain language, then delete the comments
you answered. A rule is a list item that starts with its ID and a colon, such as
"- {prefix}-1: ...". To replace a rule of the Organization Context for this pipeline only, name it:
"- {prefix}-2 (overrides ORG-3): ...".

What does this pipeline produce, for whom, and by when?
-->

## Sources

<!-- Which files or tables does it read? Who produces each one? Is the producer external? -->

## Rules

<!--
What does a valid input look like, beyond its data contract? Values that depend on each other,
codes, units, values that may be empty and when. What may a fix never do here? For example:

- {prefix}-1: Amounts are whole cents: a fix never converts them to float.
-->

## Known changes

<!-- Which planned changes of the sources should dag-healer know about, and until when? -->

## Recovery

<!-- After a failure, what must run again, and from which step? -->
"""


@dataclass(frozen=True)
class StarterOutcome:
    """What :func:`write_starter_files` did with one file (``path`` is repository-relative)."""

    path: str
    outcome: str
    pipeline: str | None = None
    rule_prefix: str | None = None


def rule_prefix_for(pipeline_id: str, taken: Iterable[str] = ()) -> str:
    """A rule prefix for ``pipeline_id`` that is not in ``taken`` and is not ``ORG``.

    The first three letters (``lab_results_daily`` -> ``LAB``), else the initials of its words, the
    first four letters, then a number (``LAB2``).
    """
    used = {item.upper() for item in taken} | {ORG_PREFIX}
    letters = re.sub(r"[^A-Za-z]", "", pipeline_id).upper()
    words = [word for word in _WORD_RE.findall(pipeline_id) if word[:1].isalpha()]
    initials = "".join(word[0] for word in words).upper()
    candidates = [letters[:3], initials[:10], letters[:4], letters[:5]]
    for candidate in candidates:
        if PREFIX_RE.match(candidate) and candidate not in used:
            return candidate
    base = (letters[:3] if len(letters) >= 2 else "ETL")[:8]
    for number in range(2, 1000):
        candidate = f"{base}{number}"
        if candidate not in used:
            return candidate
    return "ETL"  # pragma: no cover - a thousand pipelines with one prefix


def _existing_prefix(path: Path) -> str | None:
    """The ``rule_prefix`` of an existing ETL Information of the working tree (best effort)."""
    try:
        data, _body, _line, _problems = split_front_matter(path.read_text(encoding="utf-8-sig"))
    except (OSError, ValueError):  # UnicodeDecodeError is a ValueError
        return None
    prefix = data.get("rule_prefix") if data else None
    return prefix.strip().upper() if isinstance(prefix, str) else None


def organization_starter(*, today: date, etl_dir: str, owner: str = OWNER_PLACEHOLDER) -> str:
    """The text of a starter Organization Context."""
    return _ORGANIZATION.format(owner=owner, today=today.isoformat(), etl_dir=etl_dir)


def etl_starter(
    pipeline: str, prefix: str, *, today: date, organization: str, owner: str = OWNER_PLACEHOLDER
) -> str:
    """The text of a starter ETL Information for ``pipeline``."""
    return _ETL.format(
        pipeline=pipeline, prefix=prefix, owner=owner, today=today.isoformat(), organization=organization
    )


def _create(target: Path, text: str) -> bool:
    """Write ``text`` to ``target`` only when it does not exist yet (True when written)."""
    target.parent.mkdir(parents=True, exist_ok=True)
    try:
        with target.open("x", encoding="utf-8", newline="\n") as handle:
            handle.write(text)
    except FileExistsError:
        return False
    return True


def write_starter_files(
    repo_root: Path,
    *,
    organization: str,
    etl_dir: str,
    pipelines: Iterable[str],
    today: date,
    valid_pipeline: Callable[[str], bool] | None = None,
) -> list[StarterOutcome]:
    """Write the starter Organization Context and one ETL Information per pipeline, never overwriting.

    ``organization`` and ``etl_dir`` are repository-relative (``context:`` of ``healer.yaml``);
    ``valid_pipeline(id)`` says whether an id can name a file (default: no ``/``, no ``..``). The
    prefixes of the ETL Information files that exist already are never given to a new one.
    """
    outcomes: list[StarterOutcome] = []
    org_target = repo_root / organization
    written = _create(org_target, organization_starter(today=today, etl_dir=etl_dir))
    outcomes.append(StarterOutcome(organization, WRITTEN if written else KEPT))
    folder = repo_root / etl_dir
    taken: set[str] = set()
    if folder.is_dir():
        taken = {prefix for path in folder.glob("*.md") if (prefix := _existing_prefix(path))}
    for pipeline in dict.fromkeys(pipelines):
        relative = f"{etl_dir}/{pipeline}.md"
        if valid_pipeline is not None:
            allowed = valid_pipeline(pipeline)
        else:
            allowed = bool(pipeline) and "/" not in pipeline and "\\" not in pipeline and ".." not in pipeline
        if not allowed:
            outcomes.append(StarterOutcome(relative, SKIPPED, pipeline))
            continue
        target = repo_root / relative
        if target.exists():
            outcomes.append(StarterOutcome(relative, KEPT, pipeline, _existing_prefix(target)))
            continue
        prefix = rule_prefix_for(pipeline, taken)
        if _create(target, etl_starter(pipeline, prefix, today=today, organization=organization)):
            taken.add(prefix)
            outcomes.append(StarterOutcome(relative, WRITTEN, pipeline, prefix))
        else:  # created meanwhile
            outcomes.append(StarterOutcome(relative, KEPT, pipeline, _existing_prefix(target)))
    return outcomes


__all__ = [
    "KEPT",
    "OWNER_PLACEHOLDER",
    "SKIPPED",
    "WRITTEN",
    "StarterOutcome",
    "etl_starter",
    "organization_starter",
    "rule_prefix_for",
    "write_starter_files",
]
