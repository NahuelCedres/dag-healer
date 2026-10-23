"""Evidence about one failed attempt: error details, logs and the pipeline definition (stdlib only).

Everything here is RAW text from the orchestrator. :mod:`dag_healer.agent.context` masks it
(:func:`dag_healer.masking.mask_text`) before it is stored on an incident context.
``traceback_text`` uses the CPython format (``Traceback (most recent call last):``) so that
``agent.context.extract_traceback`` parses it unchanged whatever the orchestrator; adapters
render it with :func:`dag_healer.core.tracebacks.render_python_traceback` when the native
error is structured (e.g. a Lambda ``stackTrace``).
"""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass, field
from datetime import datetime
from typing import Any, Literal

from dag_healer.core.failures import AttemptState
from dag_healer.core.ids import AttemptRef


@dataclass(frozen=True)
class AttemptDetail:
    """Everything the orchestrator knows about one failed attempt (RAW text)."""

    ref: AttemptRef
    state: AttemptState
    is_final: bool
    error_name: str | None
    error_message: str | None
    traceback_text: str = ""  # Python format ("Traceback (most recent call last):") when derivable
    raw_error: str = ""  # orchestrator-native error text (Airflow error_detail, SFN cause)
    executor: str | None = None  # operator class | "lambda" | "ecs" | "glue" ...
    resource: str | None = None  # Lambda function ARN/name, operator path ...
    started_at: datetime | None = None
    ended_at: datetime | None = None
    max_attempts: int | None = None
    native: Mapping[str, Any] = field(default_factory=dict)


@dataclass(frozen=True)
class LogBundle:
    """The log of one attempt, already rendered to text lines."""

    text: str  # RAW, chronological, already rendered to lines
    source: str  # "airflow:task-log" | "cloudwatch:/aws/lambda/fn" | "none"
    confidence: Literal["exact", "heuristic", "none"] = "exact"
    truncated: bool = False
    notes: tuple[str, ...] = ()


@dataclass(frozen=True)
class PipelineSource:
    """Where (and optionally what) the orchestrator says a pipeline is."""

    path: str | None = None  # as reported (runtime absolute path, repo-relative path or ARN)
    relative_path: str | None = None  # relative to the orchestrator's code root (Airflow relative_fileloc)
    text: str | None = None  # DAG source / ASL JSON, when asked for and available
    language: Literal["python", "asl-json", "asl-yaml", "unknown"] = "python"
    origin: str = ""  # "api:dagSources" | "api:DescribeStateMachineForExecution" | "repo:<path>"


__all__ = ["AttemptDetail", "LogBundle", "PipelineSource"]
