"""The Organization Context and the ETL Information: the team's knowledge that the agent reads and cites.

Two kinds of Markdown files live in the DAG repository (``docs/context.md``):

* the **Organization Context** (``context.organization``, default ``healer/organization.md``):
  what the business does, its systems and their owners, a glossary, regulations and the rules
  for every pipeline, with the IDs ``ORG-1``, ``ORG-2``, ...;
* the **ETL Information** of each pipeline (``<context.etl_dir>/<pipeline id>.md``, default
  ``healer/etl/``): what that pipeline needs, with rule IDs of its own prefix (``LAB-1``); it
  builds on the Organization Context and can override one of its rules by naming it.

The package reads them from the base branch's commit (:mod:`dag_healer.context.read`), parses
them strictly (:mod:`dag_healer.context.parse`), refuses the ones that hold a secret or are too
large, masks the rest, renders their prompt blocks (:mod:`dag_healer.context.prompt`) and checks
the rule IDs the model cites (:class:`ContextSnapshot.check`). ``dag-healer init --context``
writes starter files (:mod:`dag_healer.context.starter`). The context files are protected
paths: a fix never changes them (:mod:`dag_healer.repo.protected_paths`).
"""

from __future__ import annotations

from dag_healer.context.model import (
    ORG_PREFIX,
    RULE_ID_PATTERN,
    Citations,
    ContextFile,
    ContextSnapshot,
    Rule,
    RuleRef,
    normalise_id,
    short_sha,
    short_text,
)
from dag_healer.context.prompt import context_blocks, neutralise_tags
from dag_healer.context.read import ContextReport, known_secret_values, read_all, read_context

__all__ = [
    "ORG_PREFIX",
    "RULE_ID_PATTERN",
    "Citations",
    "ContextFile",
    "ContextReport",
    "ContextSnapshot",
    "Rule",
    "RuleRef",
    "context_blocks",
    "known_secret_values",
    "neutralise_tags",
    "normalise_id",
    "read_all",
    "read_context",
    "short_sha",
    "short_text",
]
