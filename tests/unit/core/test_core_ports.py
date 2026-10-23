"""Tests for dag_healer.core.ports: a minimal in-test adapter satisfies every protocol."""

from __future__ import annotations

from collections.abc import Callable, Mapping, Sequence
from pathlib import Path
from typing import Any

import pytest

from dag_healer.core import ports
from dag_healer.core.errors import NotSupported
from dag_healer.core.evidence import AttemptDetail, LogBundle, PipelineSource
from dag_healer.core.failures import AttemptState, Confirmation, Failure, FailureHint, FailurePage, Trigger
from dag_healer.core.ids import AttemptRef
from dag_healer.core.specs import DagInfo, PathMapping, TaskInfo
from dag_healer.core.vocab import Capabilities, ErrorTaxonomy, RemediationKind, Vocabulary

REF = AttemptRef("mini", "flow", "run-1", "step")


class MiniLinks:
    """NativeLinks on its own object: ``adapter.links`` is an attribute of the bundle, so the
    ``links()`` method cannot live on the adapter object itself."""

    def links(self, ref: AttemptRef, native: Mapping[str, Any]) -> dict[str, str]:
        return {"Mini": f"https://mini.example/runs/{ref.run_id}"}


class MiniAdapter:
    """Implements every other port on one object (as the design allows)."""

    type = "mini"
    display_name = "Mini orchestrator"
    vocabulary = Vocabulary(product="Mini")
    capabilities = Capabilities(
        sources=("poll", "push"), push_channels=("mini",), remediations=(RemediationKind.RETRY_STEP,)
    )
    taxonomy = ErrorTaxonomy()
    channels = frozenset({"mini"})

    def __init__(self) -> None:
        self.instance = "mini"
        self.feed = self.push = self.queue = self.inspector = self.remediator = self.code = self
        self.links = MiniLinks()
        self.acked: list[str] = []
        self.remediated: dict[str, ports.RemediationResult] = {}

    # FailureFeed
    def poll(
        self, *, cursor: Mapping[str, Any] | None, is_monitored: Callable[[str], bool], limit: int = 200
    ) -> FailurePage:
        failure = Failure(ref=REF, state=AttemptState.FAILED, is_final=True, source="poll")
        return FailurePage(failures=(failure,) if is_monitored(REF.pipeline_id) else (), cursor={"n": 1})

    # PushIntake
    def normalize(
        self, channel: str, payload: Mapping[str, Any], headers: Mapping[str, str]
    ) -> list[FailureHint]:
        if "run" not in payload:
            raise ValueError("malformed")
        return [FailureHint(ref=REF, channel=channel, source="push")]

    def confirm(self, hint: FailureHint, *, trigger: Trigger) -> Confirmation:
        failure = Failure(ref=hint.ref, state=AttemptState.FAILED, is_final=True, source=hint.source)
        return Confirmation(verdict="confirmed", reason="ok", failures=(failure,))

    # HintQueue
    def receive(self, *, max_items: int = 10) -> list[ports.QueuedMessage]:
        return [ports.QueuedMessage(receipt="r-1", channel="mini", payload={"run": "run-1"})][:max_items]

    def ack(self, receipts: Sequence[str]) -> None:
        self.acked.extend(receipts)

    # RunInspector
    def describe(self, ref: AttemptRef, native: Mapping[str, Any]) -> AttemptDetail:
        return AttemptDetail(ref, AttemptState.FAILED, True, "KeyError", "'amount'")

    def logs(self, ref: AttemptRef, detail: AttemptDetail | None, *, max_chars: int = 2_000_000) -> LogBundle:
        return LogBundle(text="", source="none", confidence="none", notes=("no log",))

    def pipeline_source(
        self,
        pipeline_id: str,
        *,
        ref: AttemptRef | None = None,
        native: Mapping[str, Any] | None = None,
        include_text: bool = False,
    ) -> PipelineSource | None:
        return PipelineSource(path=f"flows/{pipeline_id}.py", origin="repo:flows")

    # CodeLocator
    def scan(self, repo_root: Path) -> list[DagInfo]:
        return [DagInfo("flow", "flows/flow.py", tasks=[TaskInfo("step", "python")])]

    def path_mappings(self, repo_root: Path, step: TaskInfo | None) -> list[PathMapping]:
        return [PathMapping("/app", "")]

    def pipeline_file(self, repo_root: Path, pipeline_id: str, source: PipelineSource | None) -> str | None:
        return source.path if source else None

    # Remediator
    def available(self, ref: AttemptRef, native: Mapping[str, Any]) -> tuple[RemediationKind, ...]:
        return (RemediationKind.RETRY_STEP,)

    def remediate(
        self,
        ref: AttemptRef,
        native: Mapping[str, Any],
        kind: RemediationKind,
        *,
        idempotency_key: str,
        actor: str,
    ) -> ports.RemediationResult:
        if kind not in self.available(ref, native):
            raise NotSupported(f"{kind.value} is not available")
        if idempotency_key not in self.remediated:
            self.remediated[idempotency_key] = ports.RemediationResult(
                kind=kind, accepted=True, detail=f"{actor} retried {ref.step_id}", new_ref=ref.next_attempt()
            )
        return self.remediated[idempotency_key]

    # OrchestratorAdapter
    def health(self) -> ports.HealthReport:
        return ports.HealthReport(ok=True, detail="fine")

    def doctor_checks(self) -> list[ports.DoctorCheck]:
        return [ports.DoctorCheck("mini api", lambda: "ok")]

    def sandbox_env(self) -> Mapping[str, str | None]:
        return {"MINI_TOKEN": None}

    def close(self) -> None:
        return None


PROTOCOLS = [
    ports.FailureFeed,
    ports.PushIntake,
    ports.HintQueue,
    ports.RunInspector,
    ports.CodeLocator,
    ports.Remediator,
    ports.NativeLinks,
]


def test_mini_adapter_satisfies_every_port() -> None:
    adapter = MiniAdapter()
    bundle = {
        ports.FailureFeed: adapter.feed,
        ports.PushIntake: adapter.push,
        ports.HintQueue: adapter.queue,
        ports.RunInspector: adapter.inspector,
        ports.CodeLocator: adapter.code,
        ports.Remediator: adapter.remediator,
        ports.NativeLinks: adapter.links,
    }
    for protocol, port in bundle.items():
        assert isinstance(port, protocol), protocol.__name__
    assert isinstance(adapter, ports.OrchestratorAdapter)
    assert adapter.links.links(REF, {}) == {"Mini": "https://mini.example/runs/run-1"}


@pytest.mark.parametrize("protocol", PROTOCOLS)
def test_objects_missing_members_are_rejected(protocol: type) -> None:
    assert not isinstance(object(), protocol)


def test_adapter_without_bundle_attributes_is_rejected() -> None:
    class FeedOnly:
        def poll(self, *, cursor: Any, is_monitored: Any, limit: int = 200) -> FailurePage:
            return FailurePage(failures=(), cursor={})

    assert isinstance(FeedOnly(), ports.FailureFeed)
    assert not isinstance(FeedOnly(), ports.OrchestratorAdapter)


def test_port_calls_through_the_protocol_types() -> None:
    adapter = MiniAdapter()

    page = adapter.poll(cursor=None, is_monitored=lambda pipeline: pipeline == "flow")
    assert [f.ref for f in page.failures] == [REF]
    assert adapter.poll(cursor=page.cursor, is_monitored=lambda _p: False).failures == ()

    (message,) = adapter.receive()
    (hint,) = adapter.normalize(message.channel, message.payload, {})
    assert adapter.confirm(hint, trigger="final_failure").verdict == "confirmed"
    adapter.ack([message.receipt])
    assert adapter.acked == ["r-1"]
    with pytest.raises(ValueError):
        adapter.normalize("mini", {}, {})

    first = adapter.remediate(REF, {}, RemediationKind.RETRY_STEP, idempotency_key="k", actor="alice")
    again = adapter.remediate(REF, {}, RemediationKind.RETRY_STEP, idempotency_key="k", actor="bob")
    assert first is again and first.new_ref == REF.next_attempt() and first.warnings == ()
    with pytest.raises(NotSupported):
        adapter.remediate(REF, {}, RemediationKind.RESUME_RUN, idempotency_key="k2", actor="alice")


def test_port_dataclass_defaults() -> None:
    assert dict(ports.HealthReport(ok=False, detail="down").checks) == {}
    result = ports.RemediationResult(kind=RemediationKind.RESUME_RUN, accepted=False, detail="window closed")
    assert (result.new_ref, result.warnings, dict(result.native)) == (None, (), {})
    check = ports.DoctorCheck("x", lambda: 42)
    assert check.run() == 42
