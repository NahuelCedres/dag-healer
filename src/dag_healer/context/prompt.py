"""The prompt blocks of the context files: ``<organization_context>`` and ``<etl_information>``.

:func:`context_blocks` renders the files a snapshot read (refused and missing files have no block):
each block names the file, the commit it was read from, its owner and its last review in its
attributes, and holds the masked text the model reads. The text cannot close its block or open
another one: a tag of a prompt block inside it (``</organization_context>``, ``<signals>``, a
full-width ``\uff1c`` or a name split by a zero-width character ...) loses its ``<`` (``&lt;``).
The instructions on how to use the blocks are in the prompts themselves
(``agent/prompts/classify.md`` and the repair prompts).
"""

from __future__ import annotations

import re

from dag_healer.context.model import ContextFile, ContextSnapshot

ORGANIZATION_TAG = "organization_context"
ETL_TAG = "etl_information"
#: The tags of every prompt block a context file must not open or close.
PROMPT_TAGS: tuple[str, ...] = (ORGANIZATION_TAG, ETL_TAG, "incident", "signals", "evidence")

#: Characters that show nothing: they are taken out, so they cannot hide a tag (``inci\u200bdent``).
_INVISIBLE_RE = re.compile("[\u200b-\u200d\u2060\ufeff]")
_TAG_RE = re.compile(r"[<\uff1c](?=\s*[/\uff0f]?\s*(?:" + "|".join(PROMPT_TAGS) + r")\b)", re.IGNORECASE)
_ATTRIBUTE_ESCAPES = str.maketrans({'"': "&quot;", "<": "&lt;", ">": "&gt;", "\n": " ", "\r": " "})


def neutralise_tags(text: str) -> str:
    """``text`` with the ``<`` (or a full-width ``\uff1c``) of every prompt block tag written ``&lt;``.

    Zero-width characters are taken out first, so they cannot split a tag's name.
    """
    return _TAG_RE.sub("&lt;", _INVISIBLE_RE.sub("", text))


def _attribute(value: str) -> str:
    return str(value).translate(_ATTRIBUTE_ESCAPES)


def _attributes(item: ContextFile, commit: str | None) -> str:
    pairs: list[tuple[str, str]] = []
    if item.kind == "etl" and item.pipeline:
        pairs.append(("pipeline", item.pipeline))
    pairs.append(("path", item.path))
    if commit:
        pairs.append(("commit", commit))
    if item.rule_prefix:
        pairs.append(("rule_prefix", item.rule_prefix))
    if item.owner:
        pairs.append(("owner", item.owner))
    if item.reviewed:
        pairs.append(("reviewed", item.reviewed))
    if item.stale and item.review_due:
        pairs.append(("review", f"overdue since {item.review_due}"))
    return " ".join(f'{name}="{_attribute(value)}"' for name, value in pairs)


def file_block(item: ContextFile, commit: str | None) -> str:
    """The block of one file that was read (``""`` otherwise)."""
    if not item.is_read:
        return ""
    tag = ORGANIZATION_TAG if item.kind == "organization" else ETL_TAG
    body = neutralise_tags(item.text).strip("\n")
    return f"<{tag} {_attributes(item, commit)}>\n{body}\n</{tag}>"


def context_blocks(snapshot: ContextSnapshot | None) -> str:
    """The blocks of the files ``snapshot`` read, the Organization Context first (``""`` for none)."""
    if snapshot is None:
        return ""
    blocks = [file_block(item, snapshot.commit) for item in snapshot.read_files()]
    return "\n\n".join(block for block in blocks if block)


__all__ = [
    "ETL_TAG",
    "ORGANIZATION_TAG",
    "PROMPT_TAGS",
    "context_blocks",
    "file_block",
    "neutralise_tags",
]
