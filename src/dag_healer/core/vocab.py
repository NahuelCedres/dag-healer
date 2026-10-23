"""Orchestrator vocabulary, capabilities and error taxonomy (stdlib only).

* :class:`Vocabulary` holds the words used in prompts, PR.md, emails and the UI. Its
  defaults are the Airflow words, so rendering a text with the default vocabulary
  reproduces today's text exactly.
* :class:`Capabilities` says what an adapter can do (sources, push channels, remediation,
  how a merged fix reaches the orchestrator, which UI hosts it supports).
* :class:`ErrorTaxonomy` classifies native error names and event types for the
  deterministic policy (``None`` lets the generic policy decide).
"""

from __future__ import annotations

from dataclasses import asdict, dataclass
from enum import Enum
from typing import Literal

_TOKEN_OPEN = "{{"
_TOKEN_CLOSE = "}}"


@dataclass(frozen=True)
class Vocabulary:
    """Words used in prompts, PR.md, emails and the UI. Airflow defaults reproduce today's text."""

    product: str = "Airflow"
    pipeline: str = "DAG"
    pipelines: str = "DAGs"
    run: str = "DAG run"
    step: str = "task"
    steps: str = "tasks"
    attempt: str = "try"
    map_index: str = "map index"
    retry_action: str = "Retry task"

    def as_dict(self) -> dict[str, str]:
        """``{field name: word}`` (JSON-safe; the ``/me`` payload and ``render_vocabulary``)."""
        return {name: str(value) for name, value in asdict(self).items()}


def render_vocabulary(text: str, vocabulary: Vocabulary) -> str:
    """Replace ``{{product}}``-style tokens (plain str.replace; JSON braces in prompts are safe).

    Only the exact tokens ``{{<field>}}`` of :class:`Vocabulary` fields are replaced (no
    whitespace inside the braces); every other brace sequence, including single braces of
    JSON examples and unknown ``{{tokens}}``, is left untouched. A text without tokens is
    returned unchanged.
    """
    if _TOKEN_OPEN not in text:
        return text
    for name, word in vocabulary.as_dict().items():
        text = text.replace(f"{_TOKEN_OPEN}{name}{_TOKEN_CLOSE}", word)
    return text


class RemediationKind(str, Enum):
    """What a human can ask the orchestrator to do after an incident."""

    RETRY_STEP = "retry_step"  # Airflow: clear exactly this task instance
    RESUME_RUN = "resume_run"  # SFN: RedriveExecution (all failed branches/iterations)
    RERUN_PIPELINE = "rerun_pipeline"  # new run with the same input/conf (opt-in)


@dataclass(frozen=True)
class Capabilities:
    """What one adapter can do; the core and the UI follow it (section 3.5 of the design)."""

    sources: tuple[str, ...]  # incident sources the adapter can produce
    push_channels: tuple[str, ...] = ()  # ingest channels its PushIntake accepts
    attempts_visible: bool = True  # every automatic retry is an observable failure
    remediations: tuple[RemediationKind, ...] = ()
    fix_delivery: Literal["repo_sync", "ci_deploy"] = "repo_sync"
    ui_hosts: tuple[Literal["airflow_plugin", "standalone"], ...] = ("standalone",)
    native_authz: bool = False  # the host can answer per-pipeline permissions


ErrorKind = Literal["data_shape", "code", "infra", "definition", "aggregate"]
"""Classification of a native error name for the deterministic policy."""


def _short_name(name: str) -> str:
    """``airflow.exceptions.AirflowTaskTimeout`` -> ``AirflowTaskTimeout``."""
    return name.rsplit(".", 1)[-1]


@dataclass(frozen=True)
class ErrorTaxonomy:
    """Native error names and event types, classified for the deterministic policy."""

    infra: frozenset[str] = frozenset()
    data_shape: frozenset[str] = frozenset()
    code: frozenset[str] = frozenset()
    definition: frozenset[str] = frozenset()  # bug in the pipeline definition (SFN States.Runtime)
    aggregate: frozenset[str] = frozenset()  # summary of child failures (ExceedToleratedFailureThreshold)
    infra_event_types: frozenset[str] = frozenset()  # SFN TaskStartFailed, LambdaFunctionStartFailed ...

    def kind_of(self, error_name: str | None, *, event_type: str | None = None) -> ErrorKind | None:
        """Classify a native error; ``None`` lets the generic policy decide.

        Order: an ``event_type`` listed in ``infra_event_types`` is ``"infra"`` whatever the
        error name (the step never started). Then ``error_name`` is looked up exactly, in the
        order infra, definition, aggregate, data_shape, code; when the exact name is in no
        set, its last dotted component is looked up the same way (so a qualified Python
        name such as ``airflow.exceptions.AirflowTaskTimeout`` matches ``AirflowTaskTimeout``).
        """
        if event_type and event_type in self.infra_event_types:
            return "infra"
        if not error_name:
            return None
        name = error_name.strip()
        candidates = [name]
        short = _short_name(name)
        if short and short != name:
            candidates.append(short)
        kinds: tuple[tuple[ErrorKind, frozenset[str]], ...] = (
            ("infra", self.infra),
            ("definition", self.definition),
            ("aggregate", self.aggregate),
            ("data_shape", self.data_shape),
            ("code", self.code),
        )
        for candidate in candidates:
            for kind, names in kinds:
                if candidate in names:
                    return kind
        return None


__all__ = [
    "Capabilities",
    "ErrorKind",
    "ErrorTaxonomy",
    "RemediationKind",
    "Vocabulary",
    "render_vocabulary",
]
