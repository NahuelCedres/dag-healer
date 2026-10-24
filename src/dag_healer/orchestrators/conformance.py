"""The adapter conformance suite, packaged (section 11.1 of ``docs/architecture/orchestrators.md``).

Every orchestrator adapter, built-in or third-party, proves that it fits the core by passing
this suite. Subclass :class:`OrchestratorConformance` in a test module and provide a
``harness`` fixture returning a :class:`ConformanceHarness`::

    import pytest

    from dag_healer.orchestrators.conformance import OrchestratorConformance


    class TestMyAdapterConformance(OrchestratorConformance):
        @pytest.fixture
        def harness(self):
            return MyHarness()

The harness builds the adapter on an offline transport (an ``httpx.MockTransport``, botocore's
``Stubber``, an in-memory world), seeds one terminal failure (plus a retrying one when the
orchestrator shows automatic retries as failed attempts) and counts the transport calls.
Optional harness hooks enable the checks that need them; a missing hook skips its check:

* ``simulate_outage(kind)`` -- from now on every transport call fails with an HTTP 500
  (``kind == "server_error"``) or a connection error (``kind == "network"``): check 11;
* ``pii_strings()`` -- data values the seeded failure carries (e-mail addresses...): check 12;
* ``link_ref()`` -- ``(ref, native)`` whose ``run_id`` contains ``:``, ``+``, ``/`` and a space,
  when the adapter's links need more than the seeded ``native`` (default: the seeded ref with
  :data:`ODD_RUN_ID`): check 9;
* ``adapter_modules()`` -- modules whose import must stay light (default: the adapter class's
  module): check 13;
* ``end_to_end(tmp_path)`` -- an :class:`EndToEndSetup` with the three demo categories: check 14.
  Its configuration selects the test sandbox of the CODE repair (``repo.sandbox``): the
  ``process`` backend for an offline run, or a container image the engine holds. Otherwise the
  CODE case ends ``repair_failed``, by design.

The checks are numbered as in the design. pytest (and the store, the masking and the worker)
are imported inside the tests, so importing this module never needs pytest.
"""

from __future__ import annotations

import contextlib
import copy
import json
import os
import posixpath
import subprocess
import sys
from collections.abc import Iterable, Iterator, Mapping, Sequence
from dataclasses import dataclass, replace
from pathlib import Path
from typing import TYPE_CHECKING, Any, Literal, Protocol
from urllib.parse import unquote, urlsplit

from dag_healer.core.errors import NotSupported, OrchestratorError
from dag_healer.core.evidence import AttemptDetail, LogBundle
from dag_healer.core.failures import AttemptState, Confirmation, Failure, FailureHint, FailurePage
from dag_healer.core.ids import UNRESOLVED_STEP, AttemptRef
from dag_healer.core.ports import (
    CodeLocator,
    DoctorCheck,
    FailureFeed,
    HealthReport,
    HintQueue,
    NativeLinks,
    OrchestratorAdapter,
    PushIntake,
    RemediationResult,
    Remediator,
    RunInspector,
)
from dag_healer.core.specs import DagInfo, PathMapping, TaskInfo
from dag_healer.core.vocab import Capabilities, ErrorTaxonomy, RemediationKind, Vocabulary

if TYPE_CHECKING:
    from dag_healer.config import HealerConfig

#: ``store.models.SOURCES`` of store schema v2 (section 6.2): every incident source the store accepts.
STORE_SOURCES: tuple[str, ...] = ("listener", "callback", "rest", "cli", "poll", "push", "eventbridge", "sqs")
#: Sources of pull detection (a FailureFeed produces one of them).
PULL_SOURCES: tuple[str, ...] = ("rest", "poll")
#: Sources of push detection (hints confirmed by a PushIntake).
PUSH_SOURCES: tuple[str, ...] = ("listener", "callback", "push", "eventbridge", "sqs")
#: Keys that hold execution payloads; they never appear in ``native`` (section 2.2).
PAYLOAD_KEYS = frozenset({"input", "output", "parameters"})
MAX_NATIVE_BYTES = 4096
MAX_ID_CHARS = 250
MAX_INSTANCE_CHARS = 64
#: Top-level modules an adapter package must not import (check 13).
FORBIDDEN_IMPORTS: tuple[str, ...] = ("airflow", "boto3", "botocore", "fastapi", "pandas")
#: A run id with ``:``, ``+``, ``/`` and a space (check 9).
ODD_RUN_ID = "manual__2026-10-18T09:00:00+00:00 a/b"
#: Phase-5 equivalents of the neutral sources (store schema v1 accepts only the phase-5 names).
_V1_SOURCES = {"poll": "rest", "push": "listener", "eventbridge": "listener", "sqs": "listener"}

OutageKind = Literal["server_error", "network"]


@dataclass(frozen=True)
class SeededFailure:
    """A failure the harness put into the orchestrator, and what the core must find out about it."""

    ref: AttemptRef
    is_final: bool
    exception_type: str  # expected from extract_traceback
    repo_frame: str | None  # repo-relative file of the innermost repo frame
    step_file: str | None  # TaskInfo.callable_file the CodeLocator must find


@dataclass(frozen=True)
class EndToEndCase:
    """One incident of the end-to-end check and the outcome the worker must reach."""

    ref: AttemptRef
    category: Literal["data", "code", "ambiguous"]
    expected_status: str  # "reported" | "repaired" | "waiting_human"


@dataclass(frozen=True)
class EndToEndSetup:
    """A repository, a configuration (store included) and an adapter whose feed lists the cases.

    ``cfg.repo.sandbox`` must name a usable test sandbox (``backend: process`` offline), or the
    CODE case ends ``repair_failed`` without running any code.
    """

    cfg: HealerConfig
    adapter: OrchestratorAdapter
    cases: tuple[EndToEndCase, ...]
    provider: Any | None = None  # default: FakeProvider(cfg) replaying <repo>/fixtures/fake_llm


class ConformanceHarness(Protocol):
    """What a test module provides for one adapter (optional hooks: see the module docstring)."""

    def build(self, tmp_path: Path) -> tuple[HealerConfig, OrchestratorAdapter]: ...

    def seeded_failure(self) -> SeededFailure: ...

    def seeded_retrying(self) -> SeededFailure | None: ...  # None when attempts are not visible

    def forged_hint(self) -> FailureHint | None: ...

    def push_samples(self) -> list[tuple[str, dict, dict]]: ...  # (channel, payload, headers)

    def io_calls(self) -> int: ...  # transport/Stubber call counter


# =========================================================================== helpers


def _pytest() -> Any:
    import pytest

    return pytest


def _short(name: str | None) -> str:
    return (name or "").strip().rsplit(".", 1)[-1]


def _require(adapter: OrchestratorAdapter, port: str) -> Any:
    value = getattr(adapter, port)
    if value is None:
        _pytest().skip(f"the {adapter.type} adapter has no {port} port")
    return value


def _hook(harness: Any, name: str) -> Any:
    hook = getattr(harness, name, None)
    return hook if callable(hook) else None


def _all_monitored(_pipeline_id: str) -> bool:
    return True


def _assert_json_safe(value: Any, what: str) -> str:
    try:
        return json.dumps(dict(value), allow_nan=False, sort_keys=True)
    except (TypeError, ValueError) as exc:
        raise AssertionError(f"{what} is not JSON-safe: {exc}") from None


def _payload_keys(value: Any, path: str = "") -> list[str]:
    found: list[str] = []
    if isinstance(value, Mapping):
        for key, item in value.items():
            where = f"{path}.{key}" if path else str(key)
            if str(key).lower() in PAYLOAD_KEYS:
                found.append(where)
            found.extend(_payload_keys(item, where))
    elif isinstance(value, (list, tuple)):
        for index, item in enumerate(value):
            found.extend(_payload_keys(item, f"{path}[{index}]"))
    return found


def _payload_strings(value: Any, inside: bool = False) -> list[str]:
    """String leaves found under payload keys (``input``/``output``/``parameters``) of ``value``."""
    found: list[str] = []
    if isinstance(value, Mapping):
        for key, item in value.items():
            found.extend(_payload_strings(item, inside or str(key).lower() in PAYLOAD_KEYS))
    elif isinstance(value, (list, tuple)):
        for item in value:
            found.extend(_payload_strings(item, inside))
    elif inside and isinstance(value, str) and len(value) >= 4:
        found.append(value)
    return found


def assert_native_fits(native: Any, what: str = "native") -> None:
    """``native`` is JSON-safe, at most 4 KB and free of payload keys (section 2.2)."""
    assert isinstance(native, Mapping), f"{what} must be a mapping, not {type(native).__name__}"
    text = _assert_json_safe(native, what)
    assert len(text.encode("utf-8")) <= MAX_NATIVE_BYTES, f"{what} is larger than {MAX_NATIVE_BYTES} bytes"
    keys = _payload_keys(native)
    assert not keys, f"{what} holds execution payloads: {keys}"


def assert_ref_fits(ref: Any, instance: str, *, hint: bool = False) -> None:
    """``ref`` fits the store key (lengths, ``map_index >= -1``, ``attempt >= 1``, the instance)."""
    assert isinstance(ref, AttemptRef), f"expected an AttemptRef, got {type(ref).__name__}"
    assert ref.orchestrator == instance, (
        f"ref of instance {ref.orchestrator!r}, adapter instance {instance!r}"
    )
    assert 0 < len(ref.orchestrator) <= MAX_INSTANCE_CHARS
    for name in ("pipeline_id", "run_id"):
        value = getattr(ref, name)
        assert isinstance(value, str) and 0 < len(value) <= MAX_ID_CHARS, f"{name} {value!r} does not fit"
    assert isinstance(ref.step_id, str) and len(ref.step_id) <= MAX_ID_CHARS
    if not hint:
        assert ref.step_id != UNRESOLVED_STEP, "only push hints may leave the step unresolved"
    assert isinstance(ref.map_index, int) and not isinstance(ref.map_index, bool) and ref.map_index >= -1
    assert isinstance(ref.attempt, int) and not isinstance(ref.attempt, bool) and ref.attempt >= 1


def assert_absolute_url(url: Any) -> None:
    """An absolute http(s) URL."""
    assert isinstance(url, str), f"a link must be a string, not {type(url).__name__}"
    parts = urlsplit(url)
    assert parts.scheme in ("http", "https") and parts.netloc, f"not an absolute http(s) URL: {url!r}"


def map_frame(path: str, mappings: Sequence[PathMapping]) -> str | None:
    """Map a traceback path to a repo-relative posix path with ``mappings`` (longest prefix first)."""
    raw = posixpath.normpath(str(path).replace("\\", "/"))
    for mapping in sorted(mappings, key=lambda m: len(m.runtime_prefix), reverse=True):
        prefix = posixpath.normpath(mapping.runtime_prefix.replace("\\", "/")).rstrip("/")
        if not prefix:
            continue
        windows = len(prefix) > 1 and prefix[1] == ":"
        candidate, base = (raw.lower(), prefix.lower()) if windows else (raw, prefix)
        if not candidate.startswith(base + "/"):
            continue
        relative = raw[len(prefix) + 1 :]
        joined = (
            posixpath.normpath(posixpath.join(mapping.repo_base, relative)) if mapping.repo_base else relative
        )
        if joined.startswith("../") or joined in ("..", "."):
            return None
        return joined
    return None


@contextlib.contextmanager
def no_network() -> Iterator[list[str]]:
    """Refuse every socket connection and DNS lookup inside the block (pure or offline code).

    Yields the list of refused attempts; the call also raises, so the code under test fails.
    """
    import socket

    attempts: list[str] = []

    def refuse(name: str) -> Any:
        def _refused(*_args: Any, **_kwargs: Any) -> Any:
            attempts.append(name)
            raise AssertionError(f"network I/O ({name}) inside a block that must stay offline")

        return _refused

    patched: list[tuple[Any, str, bool, Any]] = []
    for owner, name in (
        (socket.socket, "connect"),
        (socket.socket, "connect_ex"),
        (socket, "create_connection"),
        (socket, "getaddrinfo"),
    ):
        patched.append((owner, name, name in vars(owner), getattr(owner, name)))
        setattr(owner, name, refuse(name))
    try:
        yield attempts
    finally:
        for owner, name, own, original in patched:
            if own:
                setattr(owner, name, original)
            else:
                delattr(owner, name)
    assert not attempts, f"network I/O attempted: {attempts}"


def _seeded_native(adapter: OrchestratorAdapter, seeded: SeededFailure) -> Mapping[str, Any]:
    """The ``native`` the store would keep for the seeded failure (from the feed when there is one)."""
    if adapter.feed is not None:
        page = adapter.feed.poll(cursor=None, is_monitored=_all_monitored)
        for failure in page.failures:
            if failure.ref.key() == seeded.ref.key():
                return failure.native
    return {}


def _hint_source(adapter: OrchestratorAdapter) -> str:
    sources = adapter.capabilities.sources
    return next((source for source in sources if source in PUSH_SOURCES), sources[0] if sources else "push")


def _step_spec(adapter: OrchestratorAdapter, repo_root: Path, ref: AttemptRef) -> TaskInfo | None:
    for spec in adapter.code.scan(repo_root):
        if spec.dag_id == ref.pipeline_id:
            return spec.task(ref.step_id)
    return None


def _store_url(tmp_path: Path) -> str:
    return f"sqlite:///{(tmp_path / 'conformance-store.db').as_posix()}"


def store_failure(session: Any, failure: Failure) -> tuple[Any, bool]:
    """Create the incident of a confirmed failure through the real store (``(incident, created)``).

    Store schema v2 has ``create_incident_from_failure`` (section 6.2, it masks the summary and
    keeps ``native``); a schema-v1 store (before step 4 of the refactor) is fed through
    ``create_incident`` with the five-column key and the phase-5 name of the source.
    """
    from dag_healer.masking import mask_text
    from dag_healer.store import repo as store_repo
    from dag_healer.store.models import SOURCES

    from_failure = getattr(store_repo, "create_incident_from_failure", None)
    if from_failure is not None:
        return from_failure(session, failure, signature=None)
    key = failure.ref.store_key()
    key.pop("orchestrator")
    source = failure.source if failure.source in SOURCES else _V1_SOURCES.get(failure.source, "cli")
    return store_repo.create_incident(
        session, **key, source=source, error_summary=mask_text(failure.error_summary)
    )


def _store_failures(url: str, failures: Iterable[Failure]) -> list[tuple[str, bool]]:
    from dag_healer.store import session_scope

    stored: list[tuple[str, bool]] = []
    with session_scope(url) as session:
        for failure in failures:
            incident, created = store_failure(session, failure)
            stored.append((incident.id, created))
    return stored


def _incident_count(url: str) -> int:
    from sqlalchemy import func, select

    from dag_healer.store import Incident, session_scope

    with session_scope(url) as session:
        return int(session.scalar(select(func.count()).select_from(Incident)) or 0)


def _exception_and_frames(
    detail: AttemptDetail, bundle: LogBundle
) -> tuple[str | None, list[dict[str, Any]]]:
    """What the core extracts: the attempt's traceback first, then the log (section 3.9)."""
    from dag_healer.agent.context import extract_traceback

    for text in (detail.traceback_text, bundle.text):
        exception_type, _message, frames, _text = extract_traceback(text or "")
        if exception_type:
            return exception_type, frames
    return None, []


# =========================================================================== the suite


class OrchestratorConformance:
    """Subclass and provide a ``harness`` fixture (see the module docstring)."""

    # ------------------------------------------------------------------ 1. protocols and capabilities
    def test_01_ports_satisfy_their_protocols(self, harness: ConformanceHarness, tmp_path: Path) -> None:
        _cfg, adapter = harness.build(tmp_path)
        assert isinstance(adapter, OrchestratorAdapter)
        for name, protocol in (("inspector", RunInspector), ("code", CodeLocator), ("links", NativeLinks)):
            assert isinstance(getattr(adapter, name), protocol), (
                f"adapter.{name} is not a {protocol.__name__}"
            )
        for name, protocol in (
            ("feed", FailureFeed),
            ("push", PushIntake),
            ("queue", HintQueue),
            ("remediator", Remediator),
        ):
            port = getattr(adapter, name)
            assert port is None or isinstance(port, protocol), f"adapter.{name} is not a {protocol.__name__}"
        assert callable(adapter.links.links), "adapter.links.links must be callable (not the adapter itself)"
        for name in ("type", "instance", "display_name"):
            value = getattr(adapter, name)
            assert isinstance(value, str) and value.strip(), f"adapter.{name} must be a non-empty string"
        assert len(adapter.instance) <= MAX_INSTANCE_CHARS
        assert isinstance(adapter.vocabulary, Vocabulary) and all(adapter.vocabulary.as_dict().values())
        assert isinstance(adapter.capabilities, Capabilities)
        assert isinstance(adapter.taxonomy, ErrorTaxonomy)
        env = adapter.sandbox_env()
        assert isinstance(env, Mapping)
        assert all(isinstance(k, str) and (v is None or isinstance(v, str)) for k, v in env.items())
        checks = adapter.doctor_checks()
        assert isinstance(checks, list) and all(
            isinstance(c, DoctorCheck) and callable(c.run) for c in checks
        )

    def test_01_capabilities_are_consistent(self, harness: ConformanceHarness, tmp_path: Path) -> None:
        from dag_healer.store.models import SOURCES

        _cfg, adapter = harness.build(tmp_path)
        caps = adapter.capabilities
        assert bool(caps.push_channels) == (adapter.push is not None), "push_channels <=> push"
        if adapter.push is not None:
            assert set(caps.push_channels) == set(adapter.push.channels)
        assert bool(caps.remediations) == (adapter.remediator is not None), "remediations <=> remediator"
        assert all(isinstance(kind, RemediationKind) for kind in caps.remediations)
        if adapter.feed is not None:
            assert set(caps.sources) & set(PULL_SOURCES), "a feed produces a pull source (rest/poll)"
        if adapter.queue is not None:
            assert adapter.push is not None, "queued messages are normalised by the PushIntake"
        assert caps.sources, "an adapter produces at least one incident source"
        assert set(caps.sources) <= set(STORE_SOURCES) | set(SOURCES), "sources must fit store.SOURCES"
        assert caps.fix_delivery in ("repo_sync", "ci_deploy")
        assert set(caps.ui_hosts) <= {"airflow_plugin", "standalone"}
        if not caps.attempts_visible:
            assert harness.seeded_retrying() is None, "attempts are not visible: no retrying attempt to seed"

    def test_01_factory_is_registered(self, harness: ConformanceHarness, tmp_path: Path) -> None:
        from dag_healer.orchestrators.registry import AdapterFactory, load_factory

        _cfg, adapter = harness.build(tmp_path)
        factory = load_factory(adapter.type)
        assert isinstance(factory, AdapterFactory)
        assert factory.type == adapter.type
        assert factory.display_name == adapter.display_name
        assert isinstance(factory.secret_options, tuple)
        assert all(isinstance(name, str) and name for name in factory.secret_options)
        if factory.options_model is not None:
            from pydantic import BaseModel

            assert isinstance(factory.options_model, type) and issubclass(factory.options_model, BaseModel)

    def test_01_close_is_idempotent(self, harness: ConformanceHarness, tmp_path: Path) -> None:
        _cfg, adapter = harness.build(tmp_path)
        adapter.close()
        adapter.close()

    # ------------------------------------------------------------------ 2. construction
    def test_02_construction_does_no_io(self, harness: ConformanceHarness, tmp_path: Path) -> None:
        _cfg, adapter = harness.build(tmp_path)
        assert harness.io_calls() == 0, "building the adapter called the orchestrator"
        _ = (adapter.vocabulary, adapter.capabilities, adapter.taxonomy, adapter.links, adapter.code)
        adapter.sandbox_env()
        adapter.doctor_checks()
        assert harness.io_calls() == 0, "reading static adapter attributes called the orchestrator"

    # ------------------------------------------------------------------ 3. poll
    def test_03_poll_returns_the_seeded_failure(self, harness: ConformanceHarness, tmp_path: Path) -> None:
        _cfg, adapter = harness.build(tmp_path)
        feed = _require(adapter, "feed")
        seeded = harness.seeded_failure()
        page = feed.poll(cursor=None, is_monitored=_all_monitored)
        assert isinstance(page, FailurePage)
        _assert_json_safe(page.cursor, "the poll cursor")
        by_key = {failure.ref.key(): failure for failure in page.failures}
        assert seeded.ref.key() in by_key, f"poll did not return {seeded.ref}"
        assert by_key[seeded.ref.key()].is_final is seeded.is_final
        for failure in page.failures:
            assert isinstance(failure, Failure) and isinstance(failure.state, AttemptState)
            assert isinstance(failure.is_final, bool), "is_final must be set on every item"
            assert failure.ref.resolved, "polled refs must be resolved"
            assert failure.source in adapter.capabilities.sources
        unmonitored = feed.poll(cursor=None, is_monitored=lambda pipeline: pipeline != seeded.ref.pipeline_id)
        assert seeded.ref.key() not in {failure.ref.key() for failure in unmonitored.failures}

    def test_03_overlapping_polls_create_no_new_incident(
        self, harness: ConformanceHarness, tmp_path: Path
    ) -> None:
        from dag_healer.store import dispose_engines

        _cfg, adapter = harness.build(tmp_path)
        feed = _require(adapter, "feed")
        url = _store_url(tmp_path)
        try:
            first = feed.poll(cursor=None, is_monitored=_all_monitored)
            created = _store_failures(url, [f for f in first.failures if f.is_final])
            assert any(was_created for _id, was_created in created), "the first poll created no incident"
            count = _incident_count(url)
            again = feed.poll(cursor=None, is_monitored=_all_monitored)
            _store_failures(url, [f for f in again.failures if f.is_final])
            following = feed.poll(cursor=first.cursor, is_monitored=_all_monitored)
            _store_failures(url, [f for f in following.failures if f.is_final])
            assert _incident_count(url) == count, "an overlapping poll created a new incident"
        finally:
            dispose_engines()

    # ------------------------------------------------------------------ 4. push
    def test_04_normalize_is_pure(self, harness: ConformanceHarness, tmp_path: Path) -> None:
        _cfg, adapter = harness.build(tmp_path)
        push = _require(adapter, "push")
        samples = harness.push_samples()
        assert samples, "a harness whose adapter has a PushIntake provides push samples"
        before = harness.io_calls()
        with no_network():
            for channel, payload, headers in samples:
                assert channel in push.channels
                hints = push.normalize(channel, copy.deepcopy(payload), dict(headers))
                again = push.normalize(channel, copy.deepcopy(payload), dict(headers))
                assert isinstance(hints, list) and hints
                assert [replace(h, received_at=None) for h in hints] == [
                    replace(h, received_at=None) for h in again
                ]
                leaked = _payload_strings(payload)
                for hint in hints:
                    assert isinstance(hint, FailureHint) and hint.channel == channel
                    assert hint.source in adapter.capabilities.sources
                    assert_ref_fits(hint.ref, adapter.instance, hint=True)
                    assert_native_fits(hint.native, "hint.native")
                    text = json.dumps(dict(hint.native), sort_keys=True)
                    assert not [value for value in leaked if value in text], (
                        "payload values leaked into native"
                    )
        assert harness.io_calls() == before, "normalize called the orchestrator"

    def test_04_normalize_rejects_malformed_payloads(
        self, harness: ConformanceHarness, tmp_path: Path
    ) -> None:
        pytest = _pytest()
        _cfg, adapter = harness.build(tmp_path)
        push = _require(adapter, "push")
        for channel in sorted(push.channels):
            with pytest.raises(ValueError):
                push.normalize(channel, {}, {})

    def test_04_push_samples_confirm_to_the_polled_key(
        self, harness: ConformanceHarness, tmp_path: Path
    ) -> None:
        _cfg, adapter = harness.build(tmp_path)
        push = _require(adapter, "push")
        seeded = harness.seeded_failure()
        for channel, payload, headers in harness.push_samples():
            for hint in push.normalize(channel, copy.deepcopy(payload), dict(headers)):
                confirmation = push.confirm(hint, trigger="final_failure")
                assert isinstance(confirmation, Confirmation)
                assert confirmation.verdict == "confirmed", f"{channel} sample: {confirmation.reason}"
                keys = [failure.ref.key() for failure in confirmation.failures]
                assert seeded.ref.key() in keys, f"{channel} sample confirmed {keys}, not {seeded.ref.key()}"
                for failure in confirmation.failures:
                    assert isinstance(failure, Failure) and failure.ref.resolved
                    assert failure.is_final and failure.source in adapter.capabilities.sources
        if adapter.feed is not None:
            page = adapter.feed.poll(cursor=None, is_monitored=_all_monitored)
            assert seeded.ref.key() in {failure.ref.key() for failure in page.failures}

    def test_04_forged_hint_is_discarded(self, harness: ConformanceHarness, tmp_path: Path) -> None:
        _cfg, adapter = harness.build(tmp_path)
        push = _require(adapter, "push")
        forged = harness.forged_hint()
        if forged is None:
            _pytest().skip("the harness provides no forged hint")
        for trigger in ("final_failure", "every_failure"):
            confirmation = push.confirm(forged, trigger=trigger)
            assert confirmation.verdict == "discarded", confirmation.reason
            assert confirmation.failures == ()

    def test_04_retrying_attempt_is_not_final(self, harness: ConformanceHarness, tmp_path: Path) -> None:
        _cfg, adapter = harness.build(tmp_path)
        retrying = harness.seeded_retrying()
        if retrying is None:
            assert not adapter.capabilities.attempts_visible, "attempts are visible: seed a retrying attempt"
            _pytest().skip("the orchestrator does not show automatic retries as failed attempts")
        assert retrying.is_final is False
        native: Mapping[str, Any] = {}
        if adapter.feed is not None:
            page = adapter.feed.poll(cursor=None, is_monitored=_all_monitored)
            for failure in page.failures:
                if failure.ref.key() == retrying.ref.key():
                    assert failure.is_final is False and failure.state is AttemptState.RETRYING
                    native = failure.native
        detail = adapter.inspector.describe(retrying.ref, native)
        assert detail.is_final is False and detail.state is AttemptState.RETRYING
        if adapter.push is not None:
            hint = FailureHint(
                ref=retrying.ref,
                channel=sorted(adapter.push.channels)[0],
                source=_hint_source(adapter),
                native=native,
            )
            assert adapter.push.confirm(hint, trigger="final_failure").verdict != "confirmed"
            confirmation = adapter.push.confirm(hint, trigger="every_failure")
            assert confirmation.verdict == "confirmed", confirmation.reason
            matching = [f for f in confirmation.failures if f.ref.key() == retrying.ref.key()]
            assert len(matching) == 1, f"every_failure confirmed {[f.ref for f in confirmation.failures]}"
            assert matching[0].is_final is False

    # ------------------------------------------------------------------ 5. evidence
    def test_05_evidence_gives_the_exception_and_the_repo_frame(
        self, harness: ConformanceHarness, tmp_path: Path
    ) -> None:
        cfg, adapter = harness.build(tmp_path)
        seeded = harness.seeded_failure()
        native = _seeded_native(adapter, seeded)
        detail = adapter.inspector.describe(seeded.ref, native)
        assert isinstance(detail, AttemptDetail) and isinstance(detail.state, AttemptState)
        assert detail.ref.key() == seeded.ref.key()
        assert detail.is_final is seeded.is_final
        assert_native_fits(detail.native, "detail.native")
        bundle = adapter.inspector.logs(seeded.ref, detail)
        assert isinstance(bundle, LogBundle) and bundle.confidence in ("exact", "heuristic", "none")
        exception_type, frames = _exception_and_frames(detail, bundle)
        assert _short(exception_type) == _short(seeded.exception_type), f"found {exception_type!r}"
        if seeded.repo_frame is None:
            return
        repo_root = Path(cfg.repo_root)
        assert (repo_root / seeded.repo_frame).is_file(), f"{seeded.repo_frame} is not in the fixture repo"
        step = _step_spec(adapter, repo_root, seeded.ref)
        mappings = [PathMapping(repo_root.as_posix(), ""), *adapter.code.path_mappings(repo_root, step)]
        mapped = [map_frame(str(frame.get("file") or ""), mappings) for frame in frames]
        existing = [path for path in mapped if path and (repo_root / path).is_file()]
        assert existing, f"no traceback frame maps into the repository: {[f.get('file') for f in frames]}"
        assert existing[-1] == seeded.repo_frame

    # ------------------------------------------------------------------ 6. logs of a missing attempt
    def test_06_logs_never_raise_for_a_missing_attempt(
        self, harness: ConformanceHarness, tmp_path: Path
    ) -> None:
        _cfg, adapter = harness.build(tmp_path)
        seeded = harness.seeded_failure()
        missing = replace(seeded.ref, run_id="conformance-missing-run", attempt=seeded.ref.attempt + 41)
        bundle = adapter.inspector.logs(missing, None)
        assert isinstance(bundle, LogBundle)
        assert (bundle.text, bundle.source, bundle.confidence) == ("", "none", "none")
        assert bundle.notes and all(isinstance(note, str) and note for note in bundle.notes)

    # ------------------------------------------------------------------ 7. code locator
    def test_07_scan_is_offline_and_finds_the_step(self, harness: ConformanceHarness, tmp_path: Path) -> None:
        cfg, adapter = harness.build(tmp_path)
        seeded = harness.seeded_failure()
        repo_root = Path(cfg.repo_root)
        before = harness.io_calls()
        with no_network():
            specs = adapter.code.scan(repo_root)
            assert isinstance(specs, list) and all(isinstance(spec, DagInfo) for spec in specs)
            spec = next((s for s in specs if s.dag_id == seeded.ref.pipeline_id), None)
            assert spec is not None, f"scan did not find {seeded.ref.pipeline_id}"
            step = spec.task(seeded.ref.step_id)
            assert step is not None, f"scan did not find step {seeded.ref.step_id}"
            mappings = adapter.code.path_mappings(repo_root, step)
            assert isinstance(mappings, list) and all(isinstance(m, PathMapping) for m in mappings)
            relative = adapter.code.pipeline_file(repo_root, seeded.ref.pipeline_id, None)
        assert harness.io_calls() == before, "the code locator called the orchestrator"
        if seeded.step_file is not None:
            assert step.callable_file == seeded.step_file
            assert (repo_root / seeded.step_file).is_file(), f"{seeded.step_file} is not in the fixture repo"
        if relative is not None:
            assert (repo_root / relative).is_file(), f"pipeline_file returned {relative!r}, not a repo file"

    # ------------------------------------------------------------------ 8. remediation
    def test_08_remediation_respects_available_and_is_idempotent(
        self, harness: ConformanceHarness, tmp_path: Path
    ) -> None:
        pytest = _pytest()
        _cfg, adapter = harness.build(tmp_path)
        remediator = _require(adapter, "remediator")
        seeded = harness.seeded_failure()
        native = _seeded_native(adapter, seeded)
        before = harness.io_calls()
        kinds = remediator.available(seeded.ref, native)
        assert harness.io_calls() == before, "available() called the orchestrator"
        assert isinstance(kinds, tuple) and all(isinstance(kind, RemediationKind) for kind in kinds)
        assert set(kinds) <= set(adapter.capabilities.remediations)
        for kind in RemediationKind:
            if kind not in kinds:
                with pytest.raises(NotSupported):
                    remediator.remediate(
                        seeded.ref,
                        native,
                        kind,
                        idempotency_key=f"conformance-{kind.value}",
                        actor="conformance",
                    )
        if not kinds:
            pytest.skip("no remediation is available for the seeded failure")
        first = remediator.remediate(
            seeded.ref, native, kinds[0], idempotency_key="conformance-remediation-1", actor="conformance"
        )
        assert isinstance(first, RemediationResult) and first.kind is kinds[0]
        assert isinstance(first.accepted, bool) and isinstance(first.detail, str)
        assert all(isinstance(warning, str) for warning in first.warnings)
        assert_native_fits(first.native, "result.native")
        if first.new_ref is not None:
            assert_ref_fits(first.new_ref, adapter.instance)
        again = remediator.remediate(
            seeded.ref, native, kinds[0], idempotency_key="conformance-remediation-1", actor="someone-else"
        )
        assert (again.kind, again.accepted, again.new_ref) == (first.kind, first.accepted, first.new_ref)

    # ------------------------------------------------------------------ 9. links
    def test_09_links_are_absolute_and_ids_round_trip(
        self, harness: ConformanceHarness, tmp_path: Path
    ) -> None:
        _cfg, adapter = harness.build(tmp_path)
        seeded = harness.seeded_failure()
        native = _seeded_native(adapter, seeded)
        links = adapter.links.links(seeded.ref, native)
        assert isinstance(links, dict) and links, "an adapter links at least to its own UI"
        for label, url in links.items():
            assert isinstance(label, str) and label.strip()
            assert_absolute_url(url)
        hook = _hook(harness, "link_ref")
        odd_ref, odd_native = hook() if hook else (replace(seeded.ref, run_id=ODD_RUN_ID), native)
        for url in adapter.links.links(odd_ref, odd_native).values():
            assert_absolute_url(url)
            assert not any(character.isspace() for character in url), f"unencoded id in {url!r}"
            assert odd_ref.run_id in unquote(url), f"{odd_ref.run_id!r} does not round-trip through {url!r}"

    # ------------------------------------------------------------------ 10. store fit
    def test_10_refs_and_native_fit_the_store(self, harness: ConformanceHarness, tmp_path: Path) -> None:
        _cfg, adapter = harness.build(tmp_path)
        seeded = harness.seeded_failure()
        assert_ref_fits(seeded.ref, adapter.instance)
        native: Mapping[str, Any] = {}
        if adapter.feed is not None:
            page = adapter.feed.poll(cursor=None, is_monitored=_all_monitored)
            _assert_json_safe(page.cursor, "the poll cursor")
            for failure in page.failures:
                assert_ref_fits(failure.ref, adapter.instance)
                assert_native_fits(failure.native, "failure.native")
                if failure.ref.key() == seeded.ref.key():
                    native = failure.native
        if adapter.push is not None:
            for channel, payload, headers in harness.push_samples():
                for hint in adapter.push.normalize(channel, copy.deepcopy(payload), dict(headers)):
                    assert_ref_fits(hint.ref, adapter.instance, hint=True)
                    assert_native_fits(hint.native, "hint.native")
                    for failure in adapter.push.confirm(hint, trigger="final_failure").failures:
                        assert_ref_fits(failure.ref, adapter.instance)
                        assert_native_fits(failure.native, "confirmed failure.native")
        detail = adapter.inspector.describe(seeded.ref, native)
        assert_ref_fits(detail.ref, adapter.instance)
        assert_native_fits(detail.native, "detail.native")

    # ------------------------------------------------------------------ 11. errors
    def test_11_transport_errors_are_transient_orchestrator_errors(
        self, harness: ConformanceHarness, tmp_path: Path
    ) -> None:
        pytest = _pytest()
        if _hook(harness, "simulate_outage") is None:
            pytest.skip("the harness cannot simulate an outage (simulate_outage)")
        for kind in ("server_error", "network"):
            directory = tmp_path / kind
            directory.mkdir()
            _cfg, adapter = harness.build(directory)
            seeded = harness.seeded_failure()
            native = _seeded_native(adapter, seeded)
            hints: list[FailureHint] = []
            samples = harness.push_samples() if adapter.push is not None else []
            if samples:
                channel, payload, headers = samples[0]
                hints = adapter.push.normalize(channel, copy.deepcopy(payload), dict(headers))
            _hook(harness, "simulate_outage")(kind)
            if adapter.feed is not None:
                with pytest.raises(OrchestratorError) as raised:
                    adapter.feed.poll(cursor=None, is_monitored=_all_monitored)
                assert raised.value.transient, f"{kind}: poll raised a non-transient error"
            with pytest.raises(OrchestratorError) as raised:
                adapter.inspector.describe(seeded.ref, native)
            assert raised.value.transient, f"{kind}: describe raised a non-transient error"
            try:
                bundle = adapter.inspector.logs(seeded.ref, None)
            except OrchestratorError as exc:
                assert exc.transient, f"{kind}: logs raised a non-transient error"
            else:
                assert isinstance(bundle, LogBundle) and bundle.confidence == "none" and bundle.notes
            for hint in hints:
                assert adapter.push.confirm(hint, trigger="final_failure").verdict == "retry_later"
            if adapter.remediator is not None:
                kinds = adapter.remediator.available(seeded.ref, native)
                if kinds:
                    with pytest.raises(OrchestratorError) as raised:
                        adapter.remediator.remediate(
                            seeded.ref,
                            native,
                            kinds[0],
                            idempotency_key=f"conformance-{kind}",
                            actor="conformance",
                        )
                    assert raised.value.transient, f"{kind}: remediate raised a non-transient error"
            report = adapter.health()
            assert isinstance(report, HealthReport) and report.ok is False

    def test_11_health_never_raises(self, harness: ConformanceHarness, tmp_path: Path) -> None:
        _cfg, adapter = harness.build(tmp_path)
        report = adapter.health()
        assert (
            isinstance(report, HealthReport)
            and isinstance(report.ok, bool)
            and isinstance(report.detail, str)
        )

    # ------------------------------------------------------------------ 12. no payload leaks
    def test_12_no_payload_leaks_into_adapter_logs(
        self, harness: ConformanceHarness, tmp_path: Path, caplog: Any
    ) -> None:
        import logging

        pii_hook = _hook(harness, "pii_strings")
        if pii_hook is None:
            _pytest().skip("the harness names no seeded PII (pii_strings)")
        caplog.set_level(logging.DEBUG)
        _cfg, adapter = harness.build(tmp_path)
        pii = [value for value in pii_hook() if value]
        if not pii:
            _pytest().skip("the harness seeded no PII")
        seeded = harness.seeded_failure()
        with contextlib.suppress(OrchestratorError):
            native = _seeded_native(adapter, seeded)
            detail = adapter.inspector.describe(seeded.ref, native)
            adapter.inspector.logs(seeded.ref, detail)
            if adapter.push is not None:
                for channel, payload, headers in harness.push_samples():
                    for hint in adapter.push.normalize(channel, copy.deepcopy(payload), dict(headers)):
                        adapter.push.confirm(hint, trigger="final_failure")
            adapter.links.links(seeded.ref, native)
            adapter.health()
        texts = [record.getMessage() for record in caplog.records]
        texts += [record.exc_text for record in caplog.records if record.exc_text]
        leaks = sorted({value for value in pii for text in texts if value in text})
        assert not leaks, f"adapter log records contain seeded data values: {leaks}"

    # ------------------------------------------------------------------ 13. import hygiene
    def test_13_import_hygiene(self, harness: ConformanceHarness, tmp_path: Path) -> None:
        _cfg, adapter = harness.build(tmp_path)
        hook = _hook(harness, "adapter_modules")
        modules = sorted(hook() if hook else {type(adapter).__module__})
        code = "\n".join(
            [
                "import importlib, json, sys",
                f"for name in {modules!r}:",
                "    importlib.import_module(name)",
                f"forbidden = {sorted(FORBIDDEN_IMPORTS)!r}",
                "print(json.dumps(sorted({m.split('.')[0] for m in sys.modules} & set(forbidden))))",
            ]
        )
        env = dict(os.environ)
        env["PYTHONPATH"] = os.pathsep.join(entry for entry in sys.path if entry)
        result = subprocess.run(
            [sys.executable, "-c", code], capture_output=True, text=True, env=env, timeout=180, check=False
        )
        assert result.returncode == 0, result.stderr
        loaded = json.loads(result.stdout.strip().splitlines()[-1])
        assert loaded == [], f"importing {modules} imports {loaded}"

    # ------------------------------------------------------------------ 14. end to end
    def test_14_end_to_end(self, harness: ConformanceHarness, tmp_path: Path) -> None:
        from dag_healer.agent.fake import FakeProvider
        from dag_healer.store import dispose_engines, session_scope
        from dag_healer.store import repo as store_repo
        from dag_healer.worker.pipeline import process_incident

        pytest = _pytest()
        hook = _hook(harness, "end_to_end")
        setup = hook(tmp_path) if hook else None
        if setup is None:
            pytest.skip("the harness has no end-to-end scenario (end_to_end)")
        cfg, adapter = setup.cfg, setup.adapter
        assert {case.category for case in setup.cases} == {"data", "code", "ambiguous"}
        feed = _require(adapter, "feed")
        page = feed.poll(cursor=None, is_monitored=cfg.is_monitored)
        polled = {failure.ref.key(): failure for failure in page.failures}
        for case in setup.cases:
            assert case.ref.key() in polled, f"the feed does not list {case.ref}"
            detail = adapter.inspector.describe(case.ref, polled[case.ref.key()].native)
            exception_type, _frames = _exception_and_frames(detail, adapter.inspector.logs(case.ref, detail))
            assert exception_type, f"no exception in the evidence of {case.ref}"

        provider = setup.provider if setup.provider is not None else FakeProvider(cfg)
        try:
            ids = {}
            with session_scope(cfg.database_url) as session:
                for case in setup.cases:
                    incident, _created = store_failure(session, polled[case.ref.key()])
                    ids[case.ref.key()] = incident.id
            for case in setup.cases:
                status = process_incident(
                    cfg, None, ids[case.ref.key()], provider=provider, client=None, adapter=adapter
                )
                assert status == case.expected_status, f"{case.ref.pipeline_id}: {status}"
                with session_scope(cfg.database_url) as session:
                    incident = store_repo.get_incident(session, ids[case.ref.key()])
                    assert incident is not None and incident.category == case.category
                    if case.category != "code":
                        continue
                    assert incident.pr_url or (incident.pr_md_path and Path(incident.pr_md_path).is_file())
                    assert incident.test_path, "a CODE repair adds a test"
                    if incident.pr_md_path:
                        text = Path(incident.pr_md_path).read_text(encoding="utf-8")
                        product = adapter.vocabulary.product
                        assert product in text, f"PR.md does not name the orchestrator ({product})"
        finally:
            dispose_engines()


__all__ = [
    "FORBIDDEN_IMPORTS",
    "MAX_NATIVE_BYTES",
    "ODD_RUN_ID",
    "PAYLOAD_KEYS",
    "PULL_SOURCES",
    "PUSH_SOURCES",
    "STORE_SOURCES",
    "ConformanceHarness",
    "EndToEndCase",
    "EndToEndSetup",
    "OrchestratorConformance",
    "OutageKind",
    "SeededFailure",
    "assert_absolute_url",
    "assert_native_fits",
    "assert_ref_fits",
    "map_frame",
    "no_network",
    "store_failure",
]
