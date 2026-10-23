"""Static code model: pipelines, their steps and the datasets they touch (stdlib only).

:class:`TaskInfo`, :class:`DatasetRef` and :class:`DagInfo` are the dataclasses that
:func:`dag_healer.repo.dagscan.scan_dags` has always returned; they moved here unchanged
(same fields, same behaviour) and :mod:`dag_healer.repo.dagscan` re-exports the very same
objects, so ``dag_healer.repo.dagscan.TaskInfo is dag_healer.core.specs.TaskInfo``.

The neutral names :data:`StepSpec` and :data:`PipelineSpec` are aliases used by the
orchestrator ports (:class:`dag_healer.core.ports.CodeLocator`): ``task_id`` is the step id,
``operator`` the step kind (``"PythonOperator"``, ``"lambda"``), ``dag_id`` the pipeline id
and ``file`` the definition file (DAG ``.py``, ``*.asl.json``, ``template.yaml``).
"""

from __future__ import annotations

from dataclasses import asdict, dataclass, field
from typing import Any


@dataclass
class TaskInfo:
    """One task of a DAG as found in the source."""

    task_id: str
    operator: str
    callable_name: str | None = None
    callable_module: str | None = None
    callable_file: str | None = None
    lineno: int = 0


@dataclass
class DatasetRef:
    """A dataset path referenced by a DAG (or one of its tasks).

    ``resolved`` is True when the repo-relative location is known: the path was computed
    from ``__file__`` into the repository, or a literal was matched to an existing file.
    """

    task_id: str | None
    path: str
    type: str  # "csv" | "parquet" | "json"
    resolved: bool
    source: str  # "<repo-relative file>:<line>"


@dataclass
class DagInfo:
    """A DAG discovered in the repository."""

    dag_id: str
    file: str
    tasks: list[TaskInfo] = field(default_factory=list)
    datasets: list[DatasetRef] = field(default_factory=list)
    imports: list[str] = field(default_factory=list)

    def task(self, task_id: str) -> TaskInfo | None:
        """Return the task with ``task_id`` or None."""
        return next((t for t in self.tasks if t.task_id == task_id), None)

    def to_dict(self) -> dict[str, Any]:
        """Return a JSON-safe dict view."""
        return asdict(self)


StepSpec = TaskInfo
"""Neutral alias: one step of a pipeline (``task_id`` = step id, ``operator`` = step kind)."""
PipelineSpec = DagInfo
"""Neutral alias: one pipeline (``dag_id`` = pipeline id, ``file`` = definition file)."""


@dataclass(frozen=True)
class PathMapping:
    """Maps a runtime path prefix seen in tracebacks to a repo-relative base."""

    runtime_prefix: str  # "/opt/airflow/repo" | "/var/task"
    repo_base: str  # "" | "functions/transform"


__all__ = ["DagInfo", "DatasetRef", "PathMapping", "PipelineSpec", "StepSpec", "TaskInfo"]
