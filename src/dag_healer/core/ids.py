"""Neutral identifiers (stdlib only).

An incident is keyed by ``(orchestrator, pipeline_id, run_id, step_id, map_index, attempt)``.
The store keeps the phase-5 physical column names, so the mapping is::

    pipeline_id -> dag_id      step_id -> task_id      attempt -> try_number

(``orchestrator``, ``run_id`` and ``map_index`` keep their names). See section 2 of
``docs/architecture/orchestrators.md``.
"""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass, replace
from typing import Any

NO_MAP_INDEX = -1
"""``map_index`` of a step that is not a fan-out element."""
UNRESOLVED_STEP = ""
"""``step_id`` of a push hint whose failing step is not known yet (pending events only)."""
RUN_LEVEL_STEP = "__run__"
"""``step_id`` of a failure without a step (execution timeout/abort, unreadable history)."""
DEFAULT_ORCHESTRATOR = "airflow"
"""Instance id assumed for rows written before the store knew about orchestrators."""


def _as_int(value: Any, default: int) -> int:
    """``int(value)``, or ``default`` for ``None``/empty/invalid values (bools excluded)."""
    if value is None or isinstance(value, bool):
        return default
    try:
        return int(value)
    except (TypeError, ValueError):
        return default


def _first_text(row: Mapping[str, Any], *keys: str) -> str:
    """The first of ``keys`` present in ``row`` with a non-``None`` value, as text ("" if none)."""
    for key in keys:
        value = row.get(key)
        if value is not None:
            return str(value)
    return ""


@dataclass(frozen=True, slots=True)
class PipelineRef:
    """A pipeline of one orchestrator instance (Airflow DAG, state machine, Dagster job)."""

    orchestrator: str
    pipeline_id: str


@dataclass(frozen=True, slots=True)
class RunRef:
    """One run of a pipeline (DAG run, execution, flow run)."""

    orchestrator: str
    pipeline_id: str
    run_id: str

    @property
    def pipeline(self) -> PipelineRef:
        """The pipeline this run belongs to."""
        return PipelineRef(self.orchestrator, self.pipeline_id)


@dataclass(frozen=True, slots=True)
class AttemptRef:
    """One attempt of one step (fan-out element) of one run: the incident key.

    Store columns: dag_id = pipeline_id, task_id = step_id, map_index = fan-out index
    (-1: none), try_number = attempt (1-based).
    """

    orchestrator: str
    pipeline_id: str
    run_id: str
    step_id: str
    map_index: int = NO_MAP_INDEX
    attempt: int = 1

    @property
    def run(self) -> RunRef:
        """The run this attempt belongs to."""
        return RunRef(self.orchestrator, self.pipeline_id, self.run_id)

    @property
    def pipeline(self) -> PipelineRef:
        """The pipeline this attempt belongs to."""
        return PipelineRef(self.orchestrator, self.pipeline_id)

    @property
    def resolved(self) -> bool:
        """False while the failing step is unknown (``step_id == UNRESOLVED_STEP``)."""
        return self.step_id != UNRESOLVED_STEP

    def key(self) -> tuple[str, str, str, str, int, int]:
        """``(orchestrator, pipeline_id, run_id, step_id, map_index, attempt)``."""
        return (
            self.orchestrator,
            self.pipeline_id,
            self.run_id,
            self.step_id,
            self.map_index,
            self.attempt,
        )

    def store_key(self) -> dict[str, Any]:
        """Physical store columns for ``store.repo`` calls."""
        return {
            "orchestrator": self.orchestrator,
            "dag_id": self.pipeline_id,
            "run_id": self.run_id,
            "task_id": self.step_id,
            "map_index": int(self.map_index),
            "try_number": int(self.attempt),
        }

    def with_step(self, step_id: str, *, map_index: int = NO_MAP_INDEX) -> AttemptRef:
        """Copy with the step localised (resolving an ``UNRESOLVED_STEP`` hint)."""
        return replace(self, step_id=step_id, map_index=map_index)

    def next_attempt(self) -> AttemptRef:
        """Copy with ``attempt + 1`` (e.g. the same run after a retry or a redrive)."""
        return replace(self, attempt=self.attempt + 1)

    @classmethod
    def from_row(
        cls, row: Mapping[str, Any], *, default_orchestrator: str = DEFAULT_ORCHESTRATOR
    ) -> AttemptRef:
        """Build from ``Incident.to_dict()`` / ``PendingEvent.to_dict()`` (physical keys).

        The physical keys (``dag_id``, ``task_id``, ``try_number``) win; the neutral names
        (``pipeline_id``, ``step_id``, ``attempt``) are accepted as fallbacks. A missing or
        empty ``orchestrator`` (phase-5 rows) becomes ``default_orchestrator``; a missing
        ``map_index`` becomes :data:`NO_MAP_INDEX` and a missing ``try_number`` becomes 1.
        """
        orchestrator = _first_text(row, "orchestrator") or default_orchestrator
        attempt = row.get("try_number")
        if attempt is None:
            attempt = row.get("attempt")
        return cls(
            orchestrator=orchestrator,
            pipeline_id=_first_text(row, "dag_id", "pipeline_id"),
            run_id=_first_text(row, "run_id"),
            step_id=_first_text(row, "task_id", "step_id"),
            map_index=_as_int(row.get("map_index"), NO_MAP_INDEX),
            attempt=_as_int(attempt, 1),
        )


__all__ = [
    "DEFAULT_ORCHESTRATOR",
    "NO_MAP_INDEX",
    "RUN_LEVEL_STEP",
    "UNRESOLVED_STEP",
    "AttemptRef",
    "PipelineRef",
    "RunRef",
]
