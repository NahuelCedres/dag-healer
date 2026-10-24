"""The ports the core uses to talk to an orchestrator (stdlib only).

One :class:`OrchestratorAdapter` bundles small optional ports; an adapter implements only
what its orchestrator offers and the capabilities follow from which ports are present:

* :class:`FailureFeed` - pull detection (the source of truth);
* :class:`PushIntake` - push hints: pure normalisation in the API process, confirmation
  against the orchestrator API in the worker;
* :class:`HintQueue` - a durable queue the worker drains (EventBridge -> SQS);
* :class:`RunInspector` - evidence of one failed attempt (error, log, pipeline definition);
* :class:`CodeLocator` - static mapping from pipelines and steps to repository code;
* :class:`Remediator` - retry / resume / rerun;
* :class:`NativeLinks` - deep links into the orchestrator's own UI.

All protocols are ``runtime_checkable`` (``isinstance`` checks the presence of the members,
not their signatures). Text returned by the ports is RAW: the core masks it. Adapters raise
only :mod:`dag_healer.core.errors` exceptions. See section 3.7 of
``docs/architecture/orchestrators.md``.
"""

from __future__ import annotations

from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any, Protocol, runtime_checkable

from dag_healer.core.evidence import AttemptDetail, LogBundle, PipelineSource
from dag_healer.core.failures import Confirmation, FailureHint, FailurePage, Trigger
from dag_healer.core.ids import AttemptRef
from dag_healer.core.specs import PathMapping, PipelineSpec, StepSpec
from dag_healer.core.vocab import Capabilities, ErrorTaxonomy, RemediationKind, Vocabulary

if TYPE_CHECKING:  # annotations only: keeps ``import dag_healer.core`` free of pathlib
    from datetime import datetime
    from pathlib import Path

# =========================================================================== detection


@runtime_checkable
class FailureFeed(Protocol):
    """Pull detection: failures read from the orchestrator API (the source of truth)."""

    def poll(
        self,
        *,
        cursor: Mapping[str, Any] | None,
        is_monitored: Callable[[str], bool],
        limit: int = 200,
        now: datetime | None = None,
    ) -> FailurePage:
        """Terminal or retrying failures observed after ``cursor`` (overlap allowed: the store key
        dedupes). ``is_final`` must be set on every item; refs must be resolved.

        ``now`` is the moment the poll starts (aware UTC): feeds use it for their window and
        their new cursor, and use their own clock when it is ``None``. The worker passes it only
        to feeds whose ``poll`` accepts it, so feeds written without the keyword keep working.
        A cursor may hold ``"not_before"`` (ISO-8601), the floor written by
        ``dag-healer store reset``: failures from before that moment are not listed again.
        """
        ...


@runtime_checkable
class PushIntake(Protocol):
    """Push detection: hints delivered by the orchestrator, confirmed before they count."""

    channels: frozenset[str]

    def normalize(
        self, channel: str, payload: Mapping[str, Any], headers: Mapping[str, str]
    ) -> list[FailureHint]:
        """PURE (no I/O; runs in the API process). Keep only whitelisted fields (never execution
        input/output). Raise ValueError for a malformed payload (-> HTTP 422)."""
        ...

    def confirm(self, hint: FailureHint, *, trigger: Trigger) -> Confirmation:
        """I/O: verify the hint against the API, decide final vs retrying, localise the step.
        Transient errors -> ``retry_later``; an attempt that has not ended yet -> ``retry_later``
        with ``state=RUNNING`` (no error: the core checks it again); unknown/forged/stale ->
        ``discarded``."""
        ...


@dataclass(frozen=True)
class QueuedMessage:
    """One message received from a :class:`HintQueue`."""

    receipt: str  # opaque ack handle (SQS receipt handle)
    channel: str  # PushIntake channel used to normalise ``payload``
    payload: Mapping[str, Any]


@runtime_checkable
class HintQueue(Protocol):
    """A durable queue the worker drains (EventBridge -> SQS). At-least-once delivery."""

    def receive(self, *, max_items: int = 10) -> list[QueuedMessage]:
        """Up to ``max_items`` messages (an empty list when the queue is empty)."""
        ...

    def ack(self, receipts: Sequence[str]) -> None:
        """Delete messages once their pending events are committed."""
        ...


# =========================================================================== evidence and code


@runtime_checkable
class RunInspector(Protocol):
    """Evidence of one failed attempt."""

    def describe(self, ref: AttemptRef, native: Mapping[str, Any]) -> AttemptDetail:
        """The orchestrator's view of the attempt (state, error, executor, timing)."""
        ...

    def logs(self, ref: AttemptRef, detail: AttemptDetail | None, *, max_chars: int = 2_000_000) -> LogBundle:
        """Never raises for missing logs: returns
        ``LogBundle(text="", source="none", confidence="none", notes=...)``."""
        ...

    def pipeline_source(
        self,
        pipeline_id: str,
        *,
        ref: AttemptRef | None = None,
        native: Mapping[str, Any] | None = None,
        include_text: bool = False,
    ) -> PipelineSource | None:
        """Where the orchestrator says the pipeline is defined (and its text when asked for)."""
        ...


@runtime_checkable
class CodeLocator(Protocol):
    """Static mapping from pipelines and steps to repo code. No network I/O, never executes user code.

    A locator may also offer ``reported_file(repo_root, source) -> str | None``: the repository
    file the orchestrator's report (``source``) points at, or ``None``, without falling back to
    a scan. It is optional and not a member of this protocol (``isinstance`` keeps accepting
    locators without it). When it exists, the context gathering uses it and its own static
    scan for the definition file instead of :meth:`pipeline_file`.
    """

    def scan(self, repo_root: Path) -> list[PipelineSpec]:
        """Every pipeline in the repository with its steps and dataset references (used by init)."""
        ...

    def path_mappings(self, repo_root: Path, step: StepSpec | None) -> list[PathMapping]:
        """Runtime-prefix -> repo-base pairs for this step (the core adds ``(repo_root, "")``)."""
        ...

    def pipeline_file(self, repo_root: Path, pipeline_id: str, source: PipelineSource | None) -> str | None:
        """Repo-relative definition file of a pipeline, using the orchestrator's report when given."""
        ...


# =========================================================================== remediation and links


@dataclass(frozen=True)
class RemediationResult:
    """Outcome of :meth:`Remediator.remediate`."""

    kind: RemediationKind
    accepted: bool
    detail: str  # human text, masked by the core before it is stored as an event
    new_ref: AttemptRef | None = None  # e.g. same run, attempt + 1 after a redrive
    warnings: tuple[str, ...] = ()  # e.g. "redrive reruns the deployed code with the original definition"
    native: Mapping[str, Any] = field(default_factory=dict)


@runtime_checkable
class Remediator(Protocol):
    """Asks the orchestrator to run (part of) a pipeline again."""

    def available(self, ref: AttemptRef, native: Mapping[str, Any]) -> tuple[RemediationKind, ...]:
        """No I/O: what can be offered for this incident now (e.g. SFN redrive window of 14 days)."""
        ...

    def remediate(
        self,
        ref: AttemptRef,
        native: Mapping[str, Any],
        kind: RemediationKind,
        *,
        idempotency_key: str,
        actor: str,
    ) -> RemediationResult:
        """Raise NotSupported when ``kind`` is not in ``available``; idempotent for the same key."""
        ...


@runtime_checkable
class NativeLinks(Protocol):
    """Deep links into the orchestrator's own UI."""

    def links(self, ref: AttemptRef, native: Mapping[str, Any]) -> dict[str, str]:
        """Labelled absolute links into the orchestrator's own UI, e.g. {"AWS console": url}."""
        ...


# =========================================================================== the adapter


@dataclass(frozen=True)
class HealthReport:
    """Result of :meth:`OrchestratorAdapter.health`."""

    ok: bool
    detail: str
    checks: Mapping[str, str] = field(default_factory=dict)


@dataclass(frozen=True)
class DoctorCheck:
    """One ``dag-healer doctor`` check contributed by an adapter.

    ``run`` takes no argument and returns a ``dag_healer.cli.doctor_cmd.CheckResult`` (typed
    ``Any`` here because the core never imports the CLI).
    """

    name: str
    run: Callable[[], Any]


@runtime_checkable
class OrchestratorAdapter(Protocol):
    """Everything the core needs from one orchestrator instance.

    One class may implement several ports and point several attributes at ``self``.
    Construction does no network I/O (the API process builds one per config).
    """

    type: str  # "airflow" | "stepfunctions" | "fake" | <entry point name>
    instance: str  # stored in incidents.orchestrator
    display_name: str  # "Apache Airflow" | "AWS Step Functions"
    vocabulary: Vocabulary
    capabilities: Capabilities
    taxonomy: ErrorTaxonomy
    feed: FailureFeed | None
    push: PushIntake | None
    queue: HintQueue | None
    inspector: RunInspector
    code: CodeLocator
    remediator: Remediator | None
    links: NativeLinks

    def health(self) -> HealthReport:
        """Reachability and authentication of the orchestrator. Never raises."""
        ...

    def doctor_checks(self) -> list[DoctorCheck]:
        """Adapter-specific ``dag-healer doctor`` checks (``DoctorCheck(name, run)``)."""
        ...

    def sandbox_env(self) -> Mapping[str, str | None]:
        """Environment overrides for the pytest sandbox (``None`` removes a variable)."""
        ...

    def close(self) -> None:
        """Release clients and connections (idempotent)."""
        ...


__all__ = [
    "CodeLocator",
    "DoctorCheck",
    "FailureFeed",
    "HealthReport",
    "HintQueue",
    "NativeLinks",
    "OrchestratorAdapter",
    "PushIntake",
    "QueuedMessage",
    "Remediator",
    "RemediationResult",
    "RunInspector",
]
