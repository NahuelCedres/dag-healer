"""What the ports of one Step Functions adapter share, and small helpers (no I/O at import).

* :class:`SfnRuntime` bundles the options, the instance id, the AWS clients, the pipeline index,
  the clock and the configuration; every port takes one.
* :class:`PipelineIndex` maps pipeline ids to state machines and back without I/O (configured
  ``options.pipelines``, plus the machines ``discover: true`` found).
* :func:`native_ref` builds the adapter-owned ``native_ref`` mapping: JSON-safe, known keys
  only, never execution input/output or cause text (section 2.2 of the design).
* :func:`to_utc` / :func:`iso` / :func:`epoch_ms` convert the timestamps of AWS answers
  (datetimes), EventBridge events (epoch milliseconds, ISO text) and fixtures.
"""

from __future__ import annotations

import json
import logging
import threading
from collections.abc import Callable, Iterable, Mapping
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import TYPE_CHECKING, Any

from dag_healer.core.ids import AttemptRef
from dag_healer.stepfunctions import arns

if TYPE_CHECKING:
    from dag_healer.stepfunctions.aws import AwsClients
    from dag_healer.stepfunctions.options import StepFunctionsOptions

logger = logging.getLogger(__name__)

UTC = timezone.utc
_EPOCH = datetime(1970, 1, 1, tzinfo=UTC)
_ONE_MS = timedelta(milliseconds=1)
MAX_NATIVE_BYTES = 4096
MAX_NATIVE_TEXT = 512
MAX_FAILED_CHILDREN = 5

#: Keys of ``native_ref`` (section 9.2 of the design, plus ``error`` and ``log_group``).
NATIVE_KEYS: tuple[str, ...] = (
    "execution_arn",
    "state_machine_arn",
    "start_date",
    "stop_date",
    "status",
    "redrive_count",
    "error",
    "failed_event_id",
    "entered_event_id",
    "event_type",
    "resource",
    "resource_type",
    "function_name",
    "request_id",
    "retry_attempts",
    "step_path",
    "map_run_arn",
    "failed_children",
    "definition_version_arn",
    "log_group",
    "localized",
)


# =========================================================================== time


def utcnow() -> datetime:
    """The current time, aware UTC."""
    return datetime.now(UTC)


def to_utc(value: Any) -> datetime | None:
    """An aware UTC datetime from a datetime (naive = UTC), ISO-8601 text (``Z`` accepted) or
    epoch milliseconds (int/float, EventBridge ``startDate``); ``None`` for anything else."""
    if isinstance(value, datetime):
        return value.replace(tzinfo=UTC) if value.tzinfo is None else value.astimezone(UTC)
    if isinstance(value, bool):
        return None
    if isinstance(value, (int, float)):
        try:
            return datetime.fromtimestamp(float(value) / 1000.0, tz=UTC)
        except (OverflowError, OSError, ValueError):
            return None
    if isinstance(value, str) and value.strip():
        text = value.strip()
        if text.isdigit():
            return to_utc(int(text))
        if text[-1:] in ("Z", "z"):
            text = text[:-1] + "+00:00"
        try:
            return to_utc(datetime.fromisoformat(text))
        except ValueError:
            return None
    return None


def iso(value: Any) -> str | None:
    """ISO-8601 UTC text with milliseconds (``2026-10-02T06:00:41.512+00:00``); ``None`` if unknown."""
    moment = to_utc(value)
    return moment.isoformat(timespec="milliseconds") if moment is not None else None


def epoch_ms(value: Any) -> int | None:
    """Epoch milliseconds of a timestamp (``None`` if unknown).

    Computed exactly and truncated like :func:`iso` (so ``epoch_ms(iso(x)) == epoch_ms(x)``).
    """
    moment = to_utc(value)
    return (moment - _EPOCH) // _ONE_MS if moment is not None else None


# =========================================================================== native_ref


def _json_safe(value: Any) -> Any:
    if value is None or isinstance(value, (bool, int, str)):
        return value[:MAX_NATIVE_TEXT] if isinstance(value, str) else value
    if isinstance(value, float):
        return value if value == value and value not in (float("inf"), float("-inf")) else None
    if isinstance(value, datetime):
        return iso(value)
    if isinstance(value, Mapping):
        return {str(k): _json_safe(v) for k, v in value.items() if str(k).lower() not in _PAYLOAD_KEYS}
    if isinstance(value, (list, tuple)):
        return [_json_safe(item) for item in value]
    return str(value)[:MAX_NATIVE_TEXT]


_PAYLOAD_KEYS = frozenset({"input", "output", "parameters"})


def native_ref(base: Mapping[str, Any] | None = None, /, **fields: Any) -> dict[str, Any]:
    """The JSON-safe ``native_ref`` of an attempt: ``base`` updated with ``fields``.

    Only :data:`NATIVE_KEYS` are kept (anything else is dropped with a debug line), ``None``
    values are dropped, datetimes become ISO text, strings are cut to 512 characters and
    ``failed_children`` to 5 items. Payload keys (``input``/``output``/``parameters``) never
    survive, at any depth. Raises :class:`ValueError` when the result is larger than 4 KB.
    """
    merged: dict[str, Any] = {}
    for source in (base or {}, fields):
        for key, value in source.items():
            if key not in NATIVE_KEYS:
                logger.debug("dag-healer stepfunctions: dropping unknown native_ref key %r", key)
                continue
            if value is None:
                merged.pop(key, None)
                continue
            if key == "failed_children" and isinstance(value, (list, tuple)):
                value = list(value)[:MAX_FAILED_CHILDREN]
            merged[key] = _json_safe(value)
    ordered = {key: merged[key] for key in NATIVE_KEYS if key in merged}
    size = len(json.dumps(ordered, sort_keys=True).encode("utf-8"))
    if size > MAX_NATIVE_BYTES:
        raise ValueError(f"native_ref is {size} bytes (limit {MAX_NATIVE_BYTES})")
    return ordered


def native_int(native: Mapping[str, Any] | None, key: str) -> int | None:
    """``native[key]`` as an int (``None`` when missing or not an integer)."""
    value = (native or {}).get(key)
    if isinstance(value, bool) or value is None:
        return None
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def native_text(native: Mapping[str, Any] | None, key: str) -> str | None:
    """``native[key]`` as non-empty text (``None`` otherwise)."""
    value = (native or {}).get(key)
    if isinstance(value, str) and value.strip():
        return value
    return None


# =========================================================================== pipelines


@dataclass(frozen=True)
class PipelineEntry:
    """One watched pipeline: its id and its state machine (configured or discovered)."""

    pipeline_id: str
    state_machine: str  # ARN or name, as configured (discovered: the ARN)
    logical_id: str | None = None
    definition: str | None = None  # repo-relative ASL file override
    discovered: bool = False

    @property
    def name(self) -> str:
        """The state machine name."""
        return arns.state_machine_name(self.state_machine)

    @property
    def arn(self) -> str | None:
        """The unqualified state machine ARN when configured as an ARN (else ``None``)."""
        if arns.is_arn(self.state_machine):
            return arns.parse_state_machine_arn(self.state_machine).unqualified
        return None


class PipelineIndex:
    """Pipeline id <-> state machine, without I/O.

    Lookups by ARN ignore version/alias qualifiers. A Distributed Map child machine
    (``stateMachine:<name>/<label>``) never maps to a pipeline: its failures are reported on the
    parent execution. With ``discover`` an unknown machine maps to its name as pipeline id
    (:meth:`add_discovered` records it with its ARN), unless that name already is the pipeline
    id of another machine: such a machine maps to no pipeline, so it can never take the
    incidents (or the contracts) of the pipeline that owns the id.
    """

    def __init__(self, entries: Iterable[PipelineEntry] = (), *, discover: bool = False) -> None:
        self.discover = discover
        self._lock = threading.RLock()
        self._entries: dict[str, PipelineEntry] = {}
        self._arns: dict[str, str] = {}  # pipeline id -> resolved unqualified ARN
        for entry in entries:
            self._entries[entry.pipeline_id] = entry
            if entry.arn:
                self._arns[entry.pipeline_id] = entry.arn

    @classmethod
    def from_options(cls, options: StepFunctionsOptions) -> PipelineIndex:
        """The configured ``options.pipelines`` (in configuration order)."""
        return cls(
            (
                PipelineEntry(pid, item.state_machine, item.logical_id, item.definition)
                for pid, item in options.pipelines.items()
            ),
            discover=options.discover,
        )

    def entries(self) -> list[PipelineEntry]:
        """Configured then discovered pipelines."""
        with self._lock:
            return list(self._entries.values())

    def get(self, pipeline_id: str) -> PipelineEntry | None:
        with self._lock:
            return self._entries.get(pipeline_id)

    def pipeline_id_for(self, state_machine_arn: str) -> str | None:
        """The pipeline of a state machine ARN (or name); ``None`` when not watched."""
        try:
            machine = (
                arns.parse_state_machine_arn(state_machine_arn) if arns.is_arn(state_machine_arn) else None
            )
        except ValueError:
            return None
        if machine is not None and machine.is_map_run_child:
            return None
        name = machine.name if machine is not None else state_machine_arn.strip()
        unqualified = machine.unqualified if machine is not None else None
        with self._lock:
            if unqualified is not None:
                for pipeline_id, arn in self._arns.items():
                    if arn == unqualified:
                        return pipeline_id
            for entry in self._entries.values():
                if entry.arn is None and entry.name == name:
                    return entry.pipeline_id
                if unqualified is None and entry.name == name:
                    return entry.pipeline_id
            if not self.discover or not name or name in self._entries:
                return None  # not watched, or the name is the pipeline id of another machine
        return name

    def add_discovered(self, state_machine_arn: str) -> str:
        """Record a discovered machine (``pipeline_id`` = its name); returns the pipeline id.

        A configured pipeline of the same machine wins (its id is returned). A machine whose
        name is the pipeline id of another machine is refused with :class:`ValueError`, like a
        string that is not a state machine ARN.
        """
        known = self.pipeline_id_for(state_machine_arn)
        machine = arns.parse_state_machine_arn(state_machine_arn)
        with self._lock:
            if known is not None and known in self._entries:
                self._arns.setdefault(known, machine.unqualified)
                return known
            pipeline_id = machine.name
            if pipeline_id in self._entries:
                raise ValueError(
                    f"state machine {pipeline_id} cannot be watched under its name: {pipeline_id!r} "
                    "is the pipeline id of another state machine"
                )
            self._entries[pipeline_id] = PipelineEntry(pipeline_id, machine.unqualified, discovered=True)
            self._arns[pipeline_id] = machine.unqualified
            return pipeline_id

    def known_arn(self, pipeline_id: str) -> str | None:
        """The unqualified ARN of a pipeline's machine when known without I/O."""
        with self._lock:
            return self._arns.get(pipeline_id)

    def remember_arn(self, pipeline_id: str, arn: str) -> None:
        """Cache the resolved ARN of a pipeline configured by name."""
        unqualified = arns.parse_state_machine_arn(arn).unqualified
        with self._lock:
            self._arns[pipeline_id] = unqualified


# =========================================================================== the runtime


@dataclass
class SfnRuntime:
    """What the ports of one adapter share. ``cfg`` is the ``HealerConfig`` (may be ``None`` in
    unit tests); ``clock`` returns aware UTC datetimes."""

    options: StepFunctionsOptions
    instance: str
    clients: AwsClients
    pipelines: PipelineIndex
    clock: Callable[[], datetime] = utcnow
    cfg: Any | None = None

    def now(self) -> datetime:
        """The clock's time as aware UTC."""
        moment = to_utc(self.clock())
        return moment if moment is not None else utcnow()

    @property
    def region(self) -> str | None:
        """``options.region`` (``None``: the boto3 default region)."""
        return self.options.region

    def is_monitored(self, pipeline_id: str) -> bool:
        """``cfg.is_monitored`` (everything is monitored without a configuration)."""
        check = getattr(self.cfg, "is_monitored", None)
        return bool(check(pipeline_id)) if callable(check) else True

    def state_machine_arn(self, pipeline_id: str) -> str | None:
        """The unqualified ARN of a pipeline's state machine.

        Configured or discovered ARNs need no I/O. A pipeline configured by name is resolved
        once with ``ListStateMachines`` (then cached). ``None`` for an unknown pipeline or an
        unresolvable name; AWS errors propagate as :mod:`dag_healer.core.errors` exceptions.
        """
        known = self.pipelines.known_arn(pipeline_id)
        if known is not None:
            return known
        entry = self.pipelines.get(pipeline_id)
        if entry is None:
            return None
        from dag_healer.stepfunctions.aws import call

        token: str | None = None
        for _page in range(100):
            params: dict[str, Any] = {"maxResults": 1000}
            if token:
                params["nextToken"] = token
            response = call(self.clients.sfn, "list_state_machines", what="ListStateMachines", **params)
            for item in response.get("stateMachines") or ():
                if item.get("name") == entry.name and item.get("stateMachineArn"):
                    self.pipelines.remember_arn(pipeline_id, str(item["stateMachineArn"]))
                    return self.pipelines.known_arn(pipeline_id)
            token = response.get("nextToken")
            if not token:
                break
        return None

    def execution_arn(self, ref: AttemptRef, native: Mapping[str, Any] | None = None) -> str | None:
        """``native.execution_arn``, else the ARN of execution ``ref.run_id`` of the pipeline's
        machine (:meth:`state_machine_arn`, which may do I/O). An ``@<ms>`` reuse suffix is
        dropped, unless ``native.start_date`` shows it is part of the name
        (:func:`~dag_healer.stepfunctions.arns.execution_name`)."""
        known = native_text(native, "execution_arn")
        if known is not None:
            return known
        machine = native_text(native, "state_machine_arn") or self.state_machine_arn(ref.pipeline_id)
        if machine is None:
            return None
        start = epoch_ms((native or {}).get("start_date"))
        return arns.execution_arn(machine, arns.execution_name(ref.run_id, start=start))


__all__ = [
    "MAX_NATIVE_BYTES",
    "NATIVE_KEYS",
    "UTC",
    "PipelineEntry",
    "PipelineIndex",
    "SfnRuntime",
    "epoch_ms",
    "iso",
    "native_int",
    "native_ref",
    "native_text",
    "to_utc",
    "utcnow",
]
