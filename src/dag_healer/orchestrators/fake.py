"""Fakeflow: an in-memory orchestrator for tests and the Docker-free demo (section 11.2).

* :class:`FakeWorld` is the "orchestrator": flows (pipelines) with their steps, failed
  attempts, queued push messages and the remediations it was asked for. Tests drive it
  directly (:meth:`FakeWorld.fail` formats a real traceback from an exception raised by the
  fixture repository's code in-process, so the frames point at real files); a YAML world file
  (``orchestrator.options.world``) describes one for the demo (:meth:`FakeWorld.load`).
* :class:`FakeAdapter` implements every port of :class:`~dag_healer.core.ports.OrchestratorAdapter`
  on a world, with all capabilities on: a poll feed, a push channel (``fake``) with
  confirmation, a durable queue, evidence, step/run/pipeline remediation and links. Every call
  that a real orchestrator would answer over the network counts in ``world.io_calls`` (the
  conformance suite checks that construction and ``normalize`` do none), and
  ``world.outage`` simulates a 500 answer or an unreachable orchestrator.
* :class:`StaticCodeLocator` maps flows and steps to repository code from the declared flows
  (no repository scan, no I/O).
* :data:`FACTORY` is the ``fake`` entry point of the ``dag_healer.orchestrators`` group.

``healer.yaml``::

    orchestrator:
      type: fake
      options:
        world: fixtures/fake_orchestrator/day2.yaml   # relative to this file's directory
        base_url: http://fakeflow.localhost            # links
        token: ${FAKEFLOW_TOKEN}                        # unused; exercises secret options

The vocabulary speaks of flows, runs, steps and attempts ("Fakeflow"), so a prompt or a PR.md
written for a Fakeflow incident never says "Airflow".
"""

from __future__ import annotations

import itertools
import logging
import re
import threading
import traceback
from collections.abc import Callable, Iterable, Mapping, Sequence
from dataclasses import dataclass, field, fields, replace
from datetime import datetime, timezone
from pathlib import Path
from typing import TYPE_CHECKING, Any, Literal
from urllib.parse import quote, urlencode

from pydantic import BaseModel, ConfigDict, field_validator

from dag_healer.core.errors import NotFound, NotSupported, OrchestratorError
from dag_healer.core.evidence import AttemptDetail, LogBundle, PipelineSource
from dag_healer.core.failures import AttemptState, Confirmation, Failure, FailureHint, FailurePage, Trigger
from dag_healer.core.ids import NO_MAP_INDEX, RUN_LEVEL_STEP, UNRESOLVED_STEP, AttemptRef
from dag_healer.core.ports import DoctorCheck, HealthReport, QueuedMessage, RemediationResult
from dag_healer.core.specs import DagInfo, DatasetRef, PathMapping, TaskInfo
from dag_healer.core.tracebacks import TRACEBACK_HEADER
from dag_healer.core.vocab import Capabilities, ErrorTaxonomy, RemediationKind, Vocabulary
from dag_healer.orchestrators.registry import DEFAULT_TYPE, orchestrator_settings, parse_options

if TYPE_CHECKING:
    from dag_healer.config import HealerConfig

logger = logging.getLogger(__name__)

FAKE_TYPE = "fake"
DEFAULT_INSTANCE = "fake"
PUSH_CHANNEL = "fake"
POLL_SOURCE = "poll"
PUSH_SOURCE = "push"
DEFAULT_BASE_URL = "http://fakeflow.localhost"
LOG_SOURCE = "fake:step-log"
MAX_ID_CHARS = 250
MAX_HINT_ERROR_CHARS = 16_000
MAX_EVENT_ID_CHARS = 128
DEFAULT_POLL_LIMIT = 200

OutageKind = Literal["server_error", "network"]

FAKE_VOCABULARY = Vocabulary(
    product="Fakeflow",
    pipeline="flow",
    pipelines="flows",
    run="run",
    step="step",
    steps="steps",
    attempt="attempt",
    retry_action="Retry step",
)
FAKE_CAPABILITIES = Capabilities(
    sources=(POLL_SOURCE, PUSH_SOURCE),
    push_channels=(PUSH_CHANNEL,),
    attempts_visible=True,
    remediations=(RemediationKind.RETRY_STEP, RemediationKind.RESUME_RUN, RemediationKind.RERUN_PIPELINE),
    fix_delivery="repo_sync",
    ui_hosts=("standalone",),
    native_authz=False,
)
#: Fakeflow's own error names, one per kind, so the policy's taxonomy rules can be exercised.
FAKE_TAXONOMY = ErrorTaxonomy(
    infra=frozenset({"FakeflowWorkerLost", "FakeflowTimeout"}),
    definition=frozenset({"FakeflowDefinitionError"}),
    aggregate=frozenset({"FakeflowFanOutFailed"}),
    infra_event_types=frozenset({"StepStartFailed"}),
)

_FAILED_STATES = (AttemptState.FAILED, AttemptState.RETRYING)
_EXCEPTION_LINE = re.compile(r"^(?P<type>[A-Za-z_][\w.]*)(?::[ \t]?(?P<message>.*))?$")
_HINT_KEYS = frozenset({"pipeline_id", "run_id", "step_id", "map_index", "attempt", "error", "event_id"})
_FAILURE_KEYS = frozenset(
    {"pipeline", "run", "step", "map_index", "attempt", "final", "error_name", "text", "at"}
)


def _utcnow() -> datetime:
    return datetime.now(timezone.utc)


def _as_utc(value: datetime) -> datetime:
    return value.replace(tzinfo=timezone.utc) if value.tzinfo is None else value.astimezone(timezone.utc)


def _stamp(value: datetime | None) -> str:
    return _as_utc(value or _utcnow()).isoformat(timespec="seconds")


def _cursor_floor(value: Any) -> datetime | None:
    """The ``not_before`` floor of a poll cursor (ISO-8601); ``None`` when absent or invalid."""
    if isinstance(value, datetime):
        return _as_utc(value)
    if not isinstance(value, str) or not value.strip():
        return None
    try:
        return _as_utc(datetime.fromisoformat(value.strip().replace("Z", "+00:00")))
    except ValueError:
        logger.debug("Fakeflow: ignoring an invalid not_before in the poll cursor")
        return None


def _check_id(value: Any, what: str) -> str:
    if not isinstance(value, str):
        raise ValueError(f"{what} must be a string, not {type(value).__name__}")
    if not value.strip():
        raise ValueError(f"{what} must not be empty")
    if len(value) > MAX_ID_CHARS:
        raise ValueError(f"{what} is longer than {MAX_ID_CHARS} characters")
    return value


def _check_int(value: Any, what: str, minimum: int) -> int:
    if isinstance(value, bool) or not isinstance(value, int):
        raise ValueError(f"{what} must be an integer")
    if value < minimum:
        raise ValueError(f"{what} must be >= {minimum}")
    return value


def _exception_line(text: str) -> tuple[str | None, str | None]:
    """``(type, message)`` of the exception line closing the last traceback of ``text``."""
    if TRACEBACK_HEADER not in text:
        return None, None
    tail = text[text.rindex(TRACEBACK_HEADER) :].splitlines()[1:]
    for line in tail:
        if not line.strip() or line[:1] in (" ", "\t"):
            continue
        match = _EXCEPTION_LINE.match(line.rstrip())
        if match:
            message = match.group("message")
            return match.group("type"), message if message else None
    return None, None


# =========================================================================== the world


@dataclass
class FakePipeline:
    """One flow of a :class:`FakeWorld`."""

    pipeline_id: str
    file: str  # repo-relative definition file
    steps: dict[str, TaskInfo]
    datasets: list[DatasetRef] = field(default_factory=list)
    source: str | None = None  # definition text the "orchestrator" serves (pipeline_source)

    def spec(self) -> DagInfo:
        """The flow as a :class:`~dag_healer.core.specs.DagInfo` (copies, safe to mutate)."""
        return DagInfo(
            dag_id=self.pipeline_id,
            file=self.file,
            tasks=[replace(step) for step in self.steps.values()],
            datasets=[replace(dataset) for dataset in self.datasets],
        )


@dataclass
class FakeAttempt:
    """One attempt of one step as Fakeflow records it."""

    ref: AttemptRef
    state: AttemptState
    seq: int  # order of recording (the poll cursor)
    error_name: str | None = None
    error_message: str | None = None
    error_text: str = ""  # RAW: the formatted traceback, or the text given to fail()
    traceback_text: str = ""
    started_at: datetime | None = None
    ended_at: datetime | None = None
    max_attempts: int | None = None

    @property
    def is_final(self) -> bool:
        """No automatic retry follows."""
        return self.state is AttemptState.FAILED

    @property
    def is_failure(self) -> bool:
        """The attempt failed (terminally or before a retry)."""
        return self.state in _FAILED_STATES


@dataclass(frozen=True)
class FakeRemediation:
    """One remediation Fakeflow performed (``FakeWorld.remediations``)."""

    kind: RemediationKind
    ref: AttemptRef
    actor: str
    idempotency_key: str
    result: RemediationResult


def _key(pipeline_id: str, run_id: str, step_id: str, map_index: int, attempt: int) -> tuple[Any, ...]:
    return (pipeline_id, run_id, step_id, int(map_index), int(attempt))


class FakeWorld:
    """The in-memory state of one Fakeflow instance (see the module docstring)."""

    def __init__(
        self, *, instance: str = DEFAULT_INSTANCE, clock: Callable[[], datetime] | None = None
    ) -> None:
        self.instance = instance
        self.clock: Callable[[], datetime] = clock or _utcnow
        self.pipelines: dict[str, FakePipeline] = {}
        self.attempts: dict[tuple[Any, ...], FakeAttempt] = {}
        self.queue: list[QueuedMessage] = []
        self.remediations: list[FakeRemediation] = []
        self.path_mappings: list[PathMapping] = []
        self.io_calls = 0
        self.outage: OutageKind | None = None
        self.lock = threading.RLock()
        self._results: dict[str, RemediationResult] = {}
        self._seq = itertools.count(1)
        self._receipts = itertools.count(1)
        self._reruns = itertools.count(1)

    # ------------------------------------------------------------------ building the world
    def add_pipeline(
        self,
        pipeline_id: str,
        *,
        file: str,
        steps: Mapping[str, TaskInfo],
        datasets: Iterable[DatasetRef] = (),
        source: str | None = None,
    ) -> FakePipeline:
        """Declare a flow, its steps (``task_id`` = step id) and the datasets it touches."""
        _check_id(pipeline_id, "pipeline_id")
        for step_id, step in steps.items():
            _check_id(step_id, "step id")
            if not isinstance(step, TaskInfo):
                raise ValueError(f"step {step_id!r} must be a TaskInfo")
            if step.task_id != step_id:
                raise ValueError(f"step {step_id!r} is declared with task_id {step.task_id!r}")
        pipeline = FakePipeline(
            pipeline_id=pipeline_id,
            file=str(file).replace("\\", "/"),
            steps=dict(steps),
            datasets=list(datasets),
            source=source,
        )
        with self.lock:
            self.pipelines[pipeline_id] = pipeline
        return pipeline

    def add_path_mapping(self, runtime_prefix: str, repo_base: str = "") -> PathMapping:
        """Declare where the step code runs (``runtime_prefix``) relative to the repository."""
        mapping = PathMapping(runtime_prefix=str(runtime_prefix).replace("\\", "/"), repo_base=repo_base)
        with self.lock:
            self.path_mappings.append(mapping)
        return mapping

    def fail(
        self,
        pipeline_id: str,
        run_id: str,
        step_id: str,
        *,
        exc: BaseException | None = None,
        text: str | None = None,
        map_index: int = NO_MAP_INDEX,
        attempt: int = 1,
        final: bool = True,
        error_name: str | None = None,
        max_attempts: int | None = None,
        at: datetime | None = None,
    ) -> AttemptRef:
        """Record a failed attempt and return its ref (``final=False``: a retry follows).

        ``exc`` is formatted as a real CPython traceback (raise it from the fixture repository's
        code so the frames point at real files); ``text`` is taken as the raw error output. The
        step must be declared (``RUN_LEVEL_STEP`` is always accepted).
        """
        _check_id(run_id, "run_id")
        _check_int(map_index, "map_index", NO_MAP_INDEX)
        _check_int(attempt, "attempt", 1)
        with self.lock:
            pipeline = self.pipelines.get(pipeline_id)
            if pipeline is None:
                raise ValueError(f"unknown flow {pipeline_id!r}: call add_pipeline() first")
            if step_id != RUN_LEVEL_STEP and step_id not in pipeline.steps:
                raise ValueError(f"flow {pipeline_id!r} has no step {step_id!r}")
            key = _key(pipeline_id, run_id, step_id, map_index, attempt)
            if key in self.attempts:
                raise ValueError(
                    f"attempt {attempt} of {pipeline_id}.{step_id} in run {run_id} already exists"
                )
            if exc is not None:
                raw = "".join(traceback.format_exception(type(exc), exc, exc.__traceback__)).rstrip("\n")
                name, message, traceback_text = error_name or type(exc).__name__, str(exc) or None, raw
            else:
                raw = (text or "").rstrip("\n")
                parsed_name, message = _exception_line(raw)
                name = error_name or parsed_name
                traceback_text = raw[raw.index(TRACEBACK_HEADER) :] if TRACEBACK_HEADER in raw else ""
            moment = _as_utc(at) if at is not None else _as_utc(self.clock())
            ref = AttemptRef(self.instance, pipeline_id, run_id, step_id, map_index, attempt)
            self.attempts[key] = FakeAttempt(
                ref=ref,
                state=AttemptState.FAILED if final else AttemptState.RETRYING,
                seq=next(self._seq),
                error_name=name,
                error_message=message,
                error_text=raw,
                traceback_text=traceback_text,
                started_at=moment,
                ended_at=moment,
                max_attempts=max_attempts,
            )
        return ref

    def set_state(self, ref: AttemptRef, state: AttemptState) -> None:
        """Change the state of a recorded attempt (e.g. it was retried and succeeded)."""
        with self.lock:
            attempt = self.attempt(ref)
            if attempt is None:
                raise KeyError(f"no such attempt: {ref}")
            attempt.state = state

    def push(self, channel: str, payload: Mapping[str, Any]) -> str:
        """Queue a push notification (what the worker drains through the HintQueue); its receipt."""
        with self.lock:
            receipt = f"fakeflow-receipt-{next(self._receipts)}"
            self.queue.append(QueuedMessage(receipt=receipt, channel=channel, payload=dict(payload)))
        return receipt

    # ------------------------------------------------------------------ reading the world
    def attempt(self, ref: AttemptRef) -> FakeAttempt | None:
        """The attempt with exactly this key (``None`` for another instance or an unknown key)."""
        if ref.orchestrator != self.instance:
            return None
        with self.lock:
            return self.attempts.get(
                _key(ref.pipeline_id, ref.run_id, ref.step_id, ref.map_index, ref.attempt)
            )

    def run_attempts(self, pipeline_id: str, run_id: str) -> list[FakeAttempt]:
        """Every attempt of one run, in recording order."""
        with self.lock:
            found = [
                a
                for a in self.attempts.values()
                if a.ref.pipeline_id == pipeline_id and a.ref.run_id == run_id
            ]
        return sorted(found, key=lambda a: a.seq)

    def step_attempts(self, pipeline_id: str, run_id: str, step_id: str, map_index: int) -> list[FakeAttempt]:
        """Every attempt of one step (fan-out element) of one run, oldest first."""
        return sorted(
            (
                a
                for a in self.run_attempts(pipeline_id, run_id)
                if (a.ref.step_id, a.ref.map_index) == (step_id, map_index)
            ),
            key=lambda a: a.ref.attempt,
        )

    def ordered(self) -> list[FakeAttempt]:
        """Every attempt in recording order."""
        with self.lock:
            return sorted(self.attempts.values(), key=lambda a: a.seq)

    # ------------------------------------------------------------------ remediation bookkeeping
    def remediation_result(self, idempotency_key: str) -> RemediationResult | None:
        """The result already recorded for ``idempotency_key``, if any."""
        with self.lock:
            return self._results.get(idempotency_key)

    def record_remediation(self, remediation: FakeRemediation) -> None:
        """Store a performed remediation (and its result for the idempotency key)."""
        with self.lock:
            self.remediations.append(remediation)
            self._results[remediation.idempotency_key] = remediation.result

    def next_rerun_id(self, run_id: str) -> str:
        """Run id of a new run started by RERUN_PIPELINE."""
        with self.lock:
            return f"{run_id}__rerun{next(self._reruns)}"

    def start_attempt(self, ref: AttemptRef) -> AttemptRef:
        """Record a new RUNNING attempt (what a retry, a resume or a rerun starts)."""
        with self.lock:
            key = _key(ref.pipeline_id, ref.run_id, ref.step_id, ref.map_index, ref.attempt)
            if key not in self.attempts:
                moment = _as_utc(self.clock())
                self.attempts[key] = FakeAttempt(
                    ref=replace(ref, orchestrator=self.instance),
                    state=AttemptState.RUNNING,
                    seq=next(self._seq),
                    started_at=moment,
                )
        return replace(ref, orchestrator=self.instance)

    # ------------------------------------------------------------------ world files
    @classmethod
    def from_mapping(
        cls,
        data: Mapping[str, Any],
        *,
        instance: str = DEFAULT_INSTANCE,
        clock: Callable[[], datetime] | None = None,
    ) -> FakeWorld:
        """Build a world from parsed YAML::

            path_mappings:                 # optional: where the step code runs
              - {runtime_prefix: /opt/fakeflow/repo, repo_base: ""}
            pipelines:
              sales_daily_etl:
                file: dags/sales_daily_etl.py
                source: null               # optional definition text
                steps:                     # TaskInfo fields (task_id is the key)
                  transform_sales: {operator: PythonOperator, callable_name: transform_sales,
                                    callable_module: src.etl.sales, callable_file: src/etl/sales.py}
                datasets:                  # optional, DatasetRef fields
                  - {task_id: extract_sales, path: data/incoming/sales.csv, type: csv,
                     resolved: true, source: "dags/sales_daily_etl.py:10"}
            failures:
              - {pipeline: sales_daily_etl, run: manual__1, step: transform_sales, attempt: 1,
                 map_index: -1, final: true, error_name: KeyError, at: 2026-10-18T09:00:00Z,
                 text: "Traceback (most recent call last): ..."}

        Unknown keys are errors (:class:`ValueError`).
        """
        if not isinstance(data, Mapping):
            raise ValueError("a Fakeflow world must be a mapping")
        _only(data, {"pipelines", "failures", "path_mappings"}, "the world")
        world = cls(instance=instance, clock=clock)
        for index, item in enumerate(_as_list(data.get("path_mappings"), "path_mappings")):
            item = _as_mapping(item, f"path_mappings[{index}]")
            _only(item, {"runtime_prefix", "repo_base"}, f"path_mappings[{index}]")
            world.add_path_mapping(str(item.get("runtime_prefix") or ""), str(item.get("repo_base") or ""))
        pipelines = _as_mapping(_or_empty(data.get("pipelines")), "pipelines")
        for pipeline_id, spec in pipelines.items():
            spec = _as_mapping(spec, f"pipelines.{pipeline_id}")
            _only(spec, {"file", "steps", "datasets", "source"}, f"pipelines.{pipeline_id}")
            declared = _as_mapping(_or_empty(spec.get("steps")), f"pipelines.{pipeline_id}.steps")
            steps = {
                str(step_id): _task_info(str(step_id), step, f"pipelines.{pipeline_id}.steps.{step_id}")
                for step_id, step in declared.items()
            }
            datasets = [
                _dataset_ref(item, f"pipelines.{pipeline_id}.datasets[{index}]")
                for index, item in enumerate(
                    _as_list(spec.get("datasets"), f"pipelines.{pipeline_id}.datasets")
                )
            ]
            source = spec.get("source")
            world.add_pipeline(
                str(pipeline_id),
                file=str(spec.get("file") or ""),
                steps=steps,
                datasets=datasets,
                source=None if source is None else str(source),
            )
        for index, item in enumerate(_as_list(data.get("failures"), "failures")):
            where = f"failures[{index}]"
            item = _as_mapping(item, where)
            _only(item, set(_FAILURE_KEYS), where)
            error_name = item.get("error_name")
            world.fail(
                str(item.get("pipeline") or ""),
                str(item.get("run") or ""),
                str(item.get("step") or ""),
                text=str(item.get("text") or ""),
                map_index=item.get("map_index", NO_MAP_INDEX),
                attempt=item.get("attempt", 1),
                final=bool(item.get("final", True)),
                error_name=None if error_name is None else str(error_name),
                at=_as_datetime(item.get("at"), f"{where}.at"),
            )
        return world

    @classmethod
    def load(
        cls,
        path: str | Path,
        *,
        instance: str = DEFAULT_INSTANCE,
        clock: Callable[[], datetime] | None = None,
    ) -> FakeWorld:
        """Read a YAML world file (see :meth:`from_mapping`); :class:`ValueError` when invalid."""
        import yaml

        source = Path(path)
        try:
            data = yaml.safe_load(source.read_text(encoding="utf-8"))
        except OSError as exc:
            raise ValueError(f"cannot read the Fakeflow world file {source}: {exc}") from None
        except yaml.YAMLError as exc:
            raise ValueError(f"invalid YAML in the Fakeflow world file {source}: {exc}") from None
        try:
            return cls.from_mapping(data or {}, instance=instance, clock=clock)
        except ValueError as exc:
            raise ValueError(f"invalid Fakeflow world file {source}: {exc}") from None


def _or_empty(value: Any) -> Any:
    """``{}`` for a missing (``None``) section, the value itself otherwise (even if falsy)."""
    return {} if value is None else value


def _as_mapping(value: Any, where: str) -> Mapping[str, Any]:
    if not isinstance(value, Mapping):
        raise ValueError(f"{where} must be a mapping")
    return value


def _as_list(value: Any, where: str) -> list[Any]:
    if value is None:
        return []
    if not isinstance(value, list):
        raise ValueError(f"{where} must be a list")
    return value


def _only(data: Mapping[str, Any], allowed: set[str], where: str) -> None:
    unknown = sorted(str(key) for key in data if key not in allowed)
    if unknown:
        raise ValueError(f"{where}: unknown key(s) {', '.join(unknown)}")


def _task_info(step_id: str, value: Any, where: str) -> TaskInfo:
    data = dict(_as_mapping(value if value is not None else {}, where))
    allowed = {f.name for f in fields(TaskInfo)} - {"task_id"}
    _only(data, allowed, where)
    data.setdefault("operator", "python")
    return TaskInfo(task_id=step_id, **data)


def _dataset_ref(value: Any, where: str) -> DatasetRef:
    data = dict(_as_mapping(value, where))
    names = {f.name for f in fields(DatasetRef)}
    _only(data, names, where)
    missing = sorted(names - set(data) - {"task_id"})
    if missing:
        raise ValueError(f"{where}: missing {', '.join(missing)}")
    data.setdefault("task_id", None)
    return DatasetRef(**data)


def _as_datetime(value: Any, where: str) -> datetime | None:
    if value is None or isinstance(value, datetime):
        return value
    try:
        return datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    except ValueError:
        raise ValueError(f"{where}: invalid timestamp {value!r}") from None


# =========================================================================== code locator and links


class StaticCodeLocator:
    """A :class:`~dag_healer.core.ports.CodeLocator` over declared flows (no repository scan, no I/O)."""

    def __init__(self, world: FakeWorld | Callable[[], FakeWorld]) -> None:
        self._world = world

    def _get(self) -> FakeWorld:
        return self._world if isinstance(self._world, FakeWorld) else self._world()

    def scan(self, repo_root: Path) -> list[DagInfo]:
        """The declared flows with their steps and datasets, sorted by id."""
        world = self._get()
        with world.lock:
            pipelines = sorted(world.pipelines.values(), key=lambda p: p.pipeline_id)
        return [pipeline.spec() for pipeline in pipelines]

    def path_mappings(self, repo_root: Path, step: TaskInfo | None) -> list[PathMapping]:
        """The declared runtime prefixes (the core adds ``(repo_root, "")`` itself)."""
        world = self._get()
        with world.lock:
            return list(world.path_mappings)

    def pipeline_file(self, repo_root: Path, pipeline_id: str, source: PipelineSource | None) -> str | None:
        """The reported definition file when it exists in the repository, else the declared one."""
        world = self._get()
        declared = world.pipelines.get(pipeline_id)
        candidates = [source.relative_path, source.path] if source is not None else []
        if declared is not None:
            candidates.append(declared.file)
        root = Path(repo_root)
        for candidate in candidates:
            if candidate and not Path(candidate).is_absolute() and (root / candidate).is_file():
                return str(candidate).replace("\\", "/")
        return declared.file if declared is not None and declared.file else None


class FakeLinks:
    """:class:`~dag_healer.core.ports.NativeLinks` of Fakeflow (ids are percent-encoded)."""

    def __init__(self, base_url: str = DEFAULT_BASE_URL) -> None:
        self.base_url = base_url.rstrip("/")

    def links(self, ref: AttemptRef, native: Mapping[str, Any]) -> dict[str, str]:
        """``{"Fakeflow": <run or step page>}``."""
        url = f"{self.base_url}/flows/{quote(ref.pipeline_id, safe='')}/runs/{quote(ref.run_id, safe='')}"
        if ref.resolved and ref.step_id != RUN_LEVEL_STEP:
            url += f"/steps/{quote(ref.step_id, safe='')}"
        query = {"attempt": str(ref.attempt)}
        if ref.map_index >= 0:
            query["map_index"] = str(ref.map_index)
        return {"Fakeflow": f"{url}?{urlencode(query)}"}


# =========================================================================== the adapter


class FakeOptions(BaseModel):
    """``orchestrator.options`` of the ``fake`` type."""

    model_config = ConfigDict(extra="forbid")

    world: str | None = None
    base_url: str = DEFAULT_BASE_URL
    token: str | None = None

    @field_validator("base_url")
    @classmethod
    def _absolute(cls, value: str) -> str:
        value = value.strip().rstrip("/")
        if not value.startswith(("http://", "https://")):
            raise ValueError("expected an absolute http(s) URL")
        return value


class FakeAdapter:
    """Every port of Fakeflow on one object (``links`` and ``code`` are small companions)."""

    type = FAKE_TYPE
    display_name = "Fakeflow"
    vocabulary = FAKE_VOCABULARY
    capabilities = FAKE_CAPABILITIES
    taxonomy = FAKE_TAXONOMY
    channels = frozenset({PUSH_CHANNEL})

    def __init__(
        self,
        cfg: HealerConfig | None = None,
        *,
        world: FakeWorld | None = None,
        instance: str | None = None,
        options: FakeOptions | None = None,
    ) -> None:
        self._cfg = cfg
        self._options = options or FakeOptions()
        self.instance = instance or (world.instance if world is not None else DEFAULT_INSTANCE)
        if world is not None and world.instance != self.instance:
            raise ValueError(f"the world belongs to instance {world.instance!r}, not {self.instance!r}")
        self._world = world
        self._world_lock = threading.Lock()
        self.closed = False
        self.feed = self
        self.push = self
        self.queue = self
        self.inspector = self
        self.remediator = self
        self.code = StaticCodeLocator(lambda: self.world)
        self.links = FakeLinks(self._options.base_url)

    # ------------------------------------------------------------------ the world (lazy)
    def _world_path(self) -> Path | None:
        if not self._options.world:
            return None
        path = Path(self._options.world).expanduser()
        if path.is_absolute():
            return path
        source = getattr(self._cfg, "source_path", None)
        if source is not None:
            return Path(source).parent / path
        repo_root = getattr(self._cfg, "repo_root", None)
        return (Path(repo_root) if repo_root is not None else Path.cwd()) / path

    @property
    def world(self) -> FakeWorld:
        """The world (a ``options.world`` file is read on first use, never at construction)."""
        if self._world is None:
            with self._world_lock:
                if self._world is None:
                    path = self._world_path()
                    if path is None:
                        self._world = FakeWorld(instance=self.instance)
                    else:
                        try:
                            self._world = FakeWorld.load(path, instance=self.instance)
                        except ValueError as exc:
                            raise OrchestratorError(str(exc), transient=False) from None
        return self._world

    def _io(self, what: str) -> FakeWorld:
        """Count one orchestrator API call; raise the simulated outage, if any."""
        world = self.world
        with world.lock:
            world.io_calls += 1
            outage = world.outage
        if outage == "server_error":
            raise OrchestratorError(f"Fakeflow answered HTTP 500 to {what}", transient=True, status_code=500)
        if outage == "network":
            raise OrchestratorError(f"cannot reach Fakeflow for {what}: connection refused", transient=True)
        return world

    def _native(self, attempt: FakeAttempt) -> dict[str, Any]:
        return {"seq": attempt.seq}

    def _failure(self, attempt: FakeAttempt, source: str) -> Failure:
        return Failure(
            ref=attempt.ref,
            state=attempt.state,
            is_final=attempt.is_final,
            source=source,
            error_name=attempt.error_name,
            error_summary=attempt.error_text,
            failed_at=attempt.ended_at,
            native=self._native(attempt),
        )

    # ------------------------------------------------------------------ FailureFeed
    def poll(
        self,
        *,
        cursor: Mapping[str, Any] | None,
        is_monitored: Callable[[str], bool],
        limit: int = DEFAULT_POLL_LIMIT,
        now: datetime | None = None,
    ) -> FailurePage:
        """Failed and retrying attempts recorded after ``cursor`` (``{"seq": n}``), oldest first.

        A cursor's ``not_before`` (ISO-8601: the reset floor written by ``dag-healer store
        reset``) is honoured by every poll: only the attempts that ended at or after it are
        listed, and the returned cursor keeps it (``{"seq": n, "not_before": ...}``), so a poll
        that lists nothing does not lose it. An attempt without an end time counts as ending at
        ``now`` (the poll start, default: the world's clock). Without a floor the returned
        cursor is ``{"seq": n}``.
        """
        world = self._io("poll")
        after = 0
        floor: datetime | None = None
        if cursor:
            if "seq" in cursor:
                try:
                    after = max(0, int(cursor.get("seq", 0)))
                except (TypeError, ValueError):
                    logger.debug("Fakeflow: ignoring an invalid poll cursor")
            floor = _cursor_floor(cursor.get("not_before"))
        candidates = [a for a in world.ordered() if a.seq > after and a.is_failure]
        if floor is not None:
            moment = _as_utc(now) if now is not None else _as_utc(world.clock())
            candidates = [a for a in candidates if _as_utc(a.ended_at or a.started_at or moment) >= floor]
        size = max(1, int(limit))
        page, rest = candidates[:size], candidates[size:]
        failures = tuple(self._failure(a, POLL_SOURCE) for a in page if is_monitored(a.ref.pipeline_id))
        last = page[-1].seq if page else after
        logger.debug("Fakeflow: poll after %d returned %d failure(s)", after, len(failures))
        next_cursor: dict[str, Any] = {"seq": last}
        if floor is not None:
            next_cursor["not_before"] = floor.isoformat()
        return FailurePage(failures=failures, cursor=next_cursor, complete=not rest)

    # ------------------------------------------------------------------ PushIntake
    def normalize(
        self, channel: str, payload: Mapping[str, Any], headers: Mapping[str, str]
    ) -> list[FailureHint]:
        """PURE. ``{"pipeline_id", "run_id", "step_id"?, "map_index"?, "attempt"?, "error"?,
        "event_id"?}``; every other key (``input``, ``output``...) is dropped."""
        if channel not in self.channels:
            raise ValueError(f"unknown push channel {channel!r} (Fakeflow accepts: {PUSH_CHANNEL})")
        if not isinstance(payload, Mapping):
            raise ValueError("the payload must be a JSON object")
        kept = {key: payload[key] for key in _HINT_KEYS if key in payload}
        pipeline_id = _check_id(kept.get("pipeline_id"), "pipeline_id")
        run_id = _check_id(kept.get("run_id"), "run_id")
        step_id = kept.get("step_id")
        step_id = UNRESOLVED_STEP if step_id in (None, "") else _check_id(step_id, "step_id")
        map_index = _check_int(kept.get("map_index", NO_MAP_INDEX), "map_index", NO_MAP_INDEX)
        attempt = _check_int(kept.get("attempt", 1), "attempt", 1)
        error = kept.get("error", "")
        if not isinstance(error, str):
            raise ValueError("error must be a string")
        if len(error) > MAX_HINT_ERROR_CHARS:
            error = error[-MAX_HINT_ERROR_CHARS:]
        native: dict[str, Any] = {}
        event_id = kept.get("event_id")
        if event_id is not None:
            if not isinstance(event_id, str):
                raise ValueError("event_id must be a string")
            if event_id.strip():
                native["event_id"] = event_id.strip()[:MAX_EVENT_ID_CHARS]
        ref = AttemptRef(self.instance, pipeline_id, run_id, step_id, map_index, attempt)
        return [FailureHint(ref=ref, channel=channel, source=PUSH_SOURCE, error_text=error, native=native)]

    def confirm(self, hint: FailureHint, *, trigger: Trigger) -> Confirmation:
        """Check a hint against the world (the Airflow adapter's rules in neutral terms).

        The latest attempt still ``RUNNING`` is checked again later (``retry_later`` with state
        ``RUNNING``), like a task instance that has not ended.
        """
        try:
            world = self._io("confirm")
        except OrchestratorError as exc:
            return Confirmation(verdict="retry_later", reason=f"Fakeflow is not reachable: {exc.detail}")
        ref = hint.ref
        if ref.orchestrator != self.instance:
            return Confirmation(verdict="discarded", reason=f"the hint is for instance {ref.orchestrator!r}")
        run = world.run_attempts(ref.pipeline_id, ref.run_id)
        if not run:
            return Confirmation(verdict="discarded", reason="Fakeflow does not know this run")
        if not ref.resolved:
            return self._localise(run, hint, trigger)
        attempts = world.step_attempts(ref.pipeline_id, ref.run_id, ref.step_id, ref.map_index)
        if not attempts:
            return Confirmation(verdict="discarded", reason="Fakeflow does not know this step attempt")
        latest = attempts[-1]
        current = next((a for a in attempts if a.ref.attempt == ref.attempt), None)
        if current is None:
            if latest.ref.attempt < ref.attempt:
                return Confirmation(
                    verdict="discarded", reason=f"Fakeflow knows only {latest.ref.attempt} attempt(s)"
                )
            return Confirmation(verdict="discarded", reason=f"Fakeflow has no attempt {ref.attempt}")
        state = current.state.value
        if current.state is AttemptState.RUNNING and latest.ref.attempt == ref.attempt:
            return Confirmation(
                verdict="retry_later", reason="the attempt is running, not failed yet", state=current.state
            )
        if trigger == "every_failure":
            if current.is_failure:
                return self._confirmed(current, hint, f"state {state}, attempt {ref.attempt}")
            return Confirmation(verdict="discarded", reason=f"the attempt is {state}, not failed")
        if latest.ref.attempt > ref.attempt:
            retried = f"attempt {latest.ref.attempt} is {latest.state.value}"
            return Confirmation(verdict="discarded", reason=f"attempt {ref.attempt} was retried ({retried})")
        if current.state is AttemptState.FAILED:
            return self._confirmed(current, hint, f"state failed, attempt {ref.attempt}")
        if current.state is AttemptState.RETRYING:
            return Confirmation(
                verdict="discarded", reason="the attempt will be retried (monitor.trigger is final_failure)"
            )
        return Confirmation(verdict="discarded", reason=f"the attempt is {state}, not failed")

    def _confirmed(self, attempt: FakeAttempt, hint: FailureHint, reason: str) -> Confirmation:
        return Confirmation(
            verdict="confirmed", reason=reason, failures=(self._failure(attempt, hint.source),)
        )

    def _localise(self, run: Sequence[FakeAttempt], hint: FailureHint, trigger: Trigger) -> Confirmation:
        """Find the failed step(s) of a run-level hint (the latest attempt of each step)."""
        latest: dict[tuple[str, int], FakeAttempt] = {}
        for attempt in sorted(run, key=lambda a: a.ref.attempt):
            latest[(attempt.ref.step_id, attempt.ref.map_index)] = attempt
        wanted = _FAILED_STATES if trigger == "every_failure" else (AttemptState.FAILED,)
        found = sorted((a for a in latest.values() if a.state in wanted), key=lambda a: a.seq)
        if not found:
            return Confirmation(verdict="discarded", reason="no step of this run failed")
        steps = ", ".join(a.ref.step_id for a in found)
        return Confirmation(
            verdict="confirmed",
            reason=f"localised to {steps}",
            failures=tuple(self._failure(a, hint.source) for a in found),
        )

    # ------------------------------------------------------------------ HintQueue
    def receive(self, *, max_items: int = 10) -> list[QueuedMessage]:
        """Messages not acknowledged yet (redelivered until :meth:`ack`: at-least-once)."""
        world = self._io("receive")
        with world.lock:
            return list(world.queue[: max(0, int(max_items))])

    def ack(self, receipts: Sequence[str]) -> None:
        """Delete the acknowledged messages."""
        world = self._io("ack")
        done = set(receipts)
        with world.lock:
            world.queue = [message for message in world.queue if message.receipt not in done]

    # ------------------------------------------------------------------ RunInspector
    def describe(self, ref: AttemptRef, native: Mapping[str, Any]) -> AttemptDetail:
        """The recorded attempt (:class:`~dag_healer.core.errors.NotFound` when unknown)."""
        world = self._io("describe")
        attempt = world.attempt(ref)
        if attempt is None:
            raise NotFound(
                f"Fakeflow has no attempt {ref.attempt} of step {ref.step_id} in run {ref.run_id} "
                f"of flow {ref.pipeline_id}"
            )
        pipeline = world.pipelines.get(ref.pipeline_id)
        step = pipeline.steps.get(ref.step_id) if pipeline is not None else None
        resource = None
        if step is not None and step.callable_module and step.callable_name:
            resource = f"{step.callable_module}:{step.callable_name}"
        return AttemptDetail(
            ref=attempt.ref,
            state=attempt.state,
            is_final=attempt.is_final,
            error_name=attempt.error_name,
            error_message=attempt.error_message,
            traceback_text=attempt.traceback_text,
            raw_error=attempt.error_text,
            executor="python",
            resource=resource,
            started_at=attempt.started_at,
            ended_at=attempt.ended_at,
            max_attempts=attempt.max_attempts,
            native=self._native(attempt),
        )

    def logs(self, ref: AttemptRef, detail: AttemptDetail | None, *, max_chars: int = 2_000_000) -> LogBundle:
        """The step log; never raises (an unknown attempt or an outage gives an empty bundle)."""
        try:
            world = self._io("logs")
        except OrchestratorError as exc:
            return LogBundle(
                text="",
                source="none",
                confidence="none",
                notes=(f"The Fakeflow log is unavailable: {exc.detail}",),
            )
        attempt = world.attempt(ref)
        if attempt is None:
            return LogBundle(
                text="",
                source="none",
                confidence="none",
                notes=(
                    f"Fakeflow has no log for attempt {ref.attempt} of {ref.step_id} in run {ref.run_id}.",
                ),
            )
        text = _render_log(attempt)
        if 0 < max_chars < len(text):
            return LogBundle(
                text=text[-max_chars:],
                source=LOG_SOURCE,
                confidence="exact",
                truncated=True,
                notes=(f"The log was cut to its last {max_chars} characters.",),
            )
        return LogBundle(text=text, source=LOG_SOURCE, confidence="exact")

    def pipeline_source(
        self,
        pipeline_id: str,
        *,
        ref: AttemptRef | None = None,
        native: Mapping[str, Any] | None = None,
        include_text: bool = False,
    ) -> PipelineSource | None:
        """Where the flow is defined (``None`` for an unknown flow)."""
        world = self._io("pipeline_source")
        pipeline = world.pipelines.get(pipeline_id)
        if pipeline is None:
            return None
        return PipelineSource(
            path=pipeline.file or None,
            relative_path=pipeline.file or None,
            text=pipeline.source if include_text else None,
            language="python" if pipeline.file.endswith(".py") else "unknown",
            origin="fake:world",
        )

    # ------------------------------------------------------------------ Remediator
    def available(self, ref: AttemptRef, native: Mapping[str, Any]) -> tuple[RemediationKind, ...]:
        """No I/O: retry or resume a step failure; resume or rerun a run-level failure."""
        if not ref.resolved:
            return ()
        if ref.step_id == RUN_LEVEL_STEP:
            return (RemediationKind.RESUME_RUN, RemediationKind.RERUN_PIPELINE)
        return (RemediationKind.RETRY_STEP, RemediationKind.RESUME_RUN)

    def remediate(
        self,
        ref: AttemptRef,
        native: Mapping[str, Any],
        kind: RemediationKind,
        *,
        idempotency_key: str,
        actor: str,
    ) -> RemediationResult:
        """Start the new attempt(s) or run; the same ``idempotency_key`` returns the first result."""
        if kind not in self.available(ref, native):
            raise NotSupported(f"{kind.value} is not available for this Fakeflow incident")
        world = self.world
        previous = world.remediation_result(idempotency_key)
        if previous is not None:
            return previous
        world = self._io("remediate")
        if world.attempt(ref) is None:
            raise NotFound(f"Fakeflow has no attempt {ref.attempt} of step {ref.step_id} in run {ref.run_id}")
        warnings: tuple[str, ...] = ()
        if kind is RemediationKind.RETRY_STEP:
            new_ref = world.start_attempt(ref.next_attempt())
            detail = (
                f"{actor} retried step {ref.step_id} in run {ref.run_id} (attempt {new_ref.attempt}); "
                "Fakeflow will run it again."
            )
        elif kind is RemediationKind.RESUME_RUN:
            failed = [a for a in world.run_attempts(ref.pipeline_id, ref.run_id) if a.is_failure]
            latest = {
                (a.ref.step_id, a.ref.map_index): a for a in sorted(failed, key=lambda a: a.ref.attempt)
            }
            for attempt in latest.values():
                world.start_attempt(attempt.ref.next_attempt())
            new_ref = replace(ref.next_attempt(), orchestrator=self.instance)
            detail = f"{actor} resumed run {ref.run_id} ({len(latest)} failed step(s) run again)."
            warnings = ("Resuming reruns every failed step of the run, not only this one.",)
        else:
            run_id = world.next_rerun_id(ref.run_id)
            new_ref = world.start_attempt(replace(ref, run_id=run_id, attempt=1))
            detail = f"{actor} started run {run_id} of flow {ref.pipeline_id}."
        result = RemediationResult(
            kind=kind,
            accepted=True,
            detail=detail,
            new_ref=new_ref,
            warnings=warnings,
            native={"remediation": len(world.remediations) + 1},
        )
        world.record_remediation(FakeRemediation(kind, ref, actor, idempotency_key, result))
        logger.info("Fakeflow: %s for %s.%s (run %s)", kind.value, ref.pipeline_id, ref.step_id, ref.run_id)
        return result

    # ------------------------------------------------------------------ the adapter
    def health(self) -> HealthReport:
        """Reachability of the world. Never raises."""
        try:
            world = self._io("health")
            with world.lock:
                flows = len(world.pipelines)
                failed = sum(1 for attempt in world.attempts.values() if attempt.is_failure)
            return HealthReport(
                ok=True,
                detail=f"Fakeflow world: {flows} flow(s), {failed} failed attempt(s)",
                checks={"world": "ok"},
            )
        except Exception as exc:  # health never raises
            detail = exc.detail if isinstance(exc, OrchestratorError) else f"{type(exc).__name__}: {exc}"
            return HealthReport(
                ok=False, detail=f"Fakeflow is not reachable: {detail}", checks={"world": "error"}
            )

    def doctor_checks(self) -> list[DoctorCheck]:
        """One ``dag-healer doctor`` check: the world answers."""

        def run() -> Any:
            from dag_healer.cli.doctor_cmd import ERROR, OK, CheckResult

            report = self.health()
            return CheckResult("fakeflow", OK if report.ok else ERROR, report.detail)

        return [DoctorCheck("fakeflow", run)]

    def sandbox_env(self) -> Mapping[str, str | None]:
        """Fakeflow's own credential never reaches the pytest sandbox."""
        return {"FAKEFLOW_TOKEN": None}

    def close(self) -> None:
        """Nothing to release (idempotent)."""
        self.closed = True


def _render_log(attempt: FakeAttempt) -> str:
    """The step log of one attempt: two log lines around the raw error output."""
    ref = attempt.ref
    where = f"step {ref.step_id} of flow {ref.pipeline_id} (run {ref.run_id}, attempt {ref.attempt}"
    where += f", map index {ref.map_index})" if ref.map_index >= 0 else ")"
    start, end = _stamp(attempt.started_at), _stamp(attempt.ended_at or attempt.started_at)
    lines = [f"[{start}] INFO - Fakeflow: starting {where}"]
    if attempt.is_failure:
        failed = f"[{end}] ERROR - Fakeflow: step {ref.step_id} failed"
        lines.append(failed + (f" with {attempt.error_name}" if attempt.error_name else ""))
        if attempt.error_text:
            lines.append(attempt.error_text)
        outcome = "no retry left" if attempt.is_final else "a retry follows"
        lines.append(f"[{end}] INFO - Fakeflow: attempt {ref.attempt} {attempt.state.value} ({outcome})")
    else:
        lines.append(f"[{end}] INFO - Fakeflow: attempt {ref.attempt} is {attempt.state.value}")
    return "\n".join(lines) + "\n"


# =========================================================================== the factory


class FakeFactory:
    """:class:`~dag_healer.orchestrators.registry.AdapterFactory` of the ``fake`` type."""

    type = FAKE_TYPE
    display_name = "Fakeflow"
    options_model = FakeOptions
    secret_options: tuple[str, ...] = ("token",)

    def create(self, cfg: HealerConfig, *, client: Any | None = None) -> FakeAdapter:
        """No I/O (a world file is read on first use). ``client`` may be a :class:`FakeWorld`.

        The ``orchestrator:`` settings apply when the configuration selects an orchestrator; a
        configuration without that section (the default ``airflow``) gives instance ``fake``
        with no options (tests building Fakeflow from a plain configuration).
        """
        if client is not None and not isinstance(client, FakeWorld):
            raise TypeError(f"the fake adapter takes a FakeWorld as client, not {type(client).__name__}")
        settings = orchestrator_settings(cfg)
        if settings.type == DEFAULT_TYPE:
            instance, options = (client.instance if client is not None else DEFAULT_INSTANCE), FakeOptions()
        else:
            instance, options = settings.instance, parse_options(self, settings.options)
        return FakeAdapter(cfg, world=client, instance=instance, options=options)


FACTORY = FakeFactory()

__all__ = [
    "DEFAULT_BASE_URL",
    "FACTORY",
    "FAKE_CAPABILITIES",
    "FAKE_TAXONOMY",
    "FAKE_TYPE",
    "FAKE_VOCABULARY",
    "PUSH_CHANNEL",
    "FakeAdapter",
    "FakeAttempt",
    "FakeFactory",
    "FakeLinks",
    "FakeOptions",
    "FakePipeline",
    "FakeRemediation",
    "FakeWorld",
    "OutageKind",
    "StaticCodeLocator",
]
