"""Pydantic models for dag-healer data contracts.

A data contract describes what the datasets read or written by a DAG's tasks are expected
to look like: columns, *logical* dtypes, nullability, numeric ranges, PII flags and a
row-count band. Contracts live as YAML files in the user's repository
(``healer/contracts/<dag_id>.yaml`` by default) and only ``approved`` contracts are
enforced by the classification policy.

This module is intentionally light: it only depends on pydantic, so it can be imported by
``dag_healer.config`` without pulling pandas in.

``dag_id`` and ``task_id`` stay the canonical keys. The orchestrator-neutral names
``pipeline_id`` and ``step_id`` are accepted as input aliases only: contracts are always
written with the canonical keys, so ``compute_source_hash`` (and every approval) is unchanged.
"""

from __future__ import annotations

from collections.abc import Mapping
from datetime import datetime
from typing import Any, Literal

from pydantic import AliasChoices, BaseModel, ConfigDict, Field, field_validator, model_validator

LogicalDtype = Literal["string", "integer", "float", "boolean", "datetime", "date", "unknown"]
"""Logical column types. They are storage-independent: pandas 2 ``object`` columns and pandas 3
``str`` columns are both ``"string"``; ``float64`` columns holding only integral values (an
integer column with nulls) and ``Int64`` columns are both ``"integer"``."""

DatasetType = Literal["csv", "parquet", "json"]

ViolationRule = Literal[
    "missing_column",
    "dtype_mismatch",
    "nulls_not_allowed",
    "out_of_range",
    "row_count_out_of_band",
    "unexpected_column",
    "not_allowed_value",
    "dataset_unreadable",
]

MAX_ROW_INDEXES = 20
"""Maximum number of offending row positions kept on a single violation."""

LOGICAL_DTYPES: tuple[str, ...] = ("string", "integer", "float", "boolean", "datetime", "date", "unknown")


class _StrictModel(BaseModel):
    """Base for user-authored models: unknown keys are rejected so YAML typos surface."""

    model_config = ConfigDict(extra="forbid")


def _one_of(data: Any, name: str, alias: str) -> Any:
    """Refuse a mapping that sets both the canonical key ``name`` and its input ``alias``."""
    if isinstance(data, Mapping) and name in data and alias in data:
        raise ValueError(f"'{name}' and '{alias}' are the same field ('{alias}' is an alias): set only one")
    return data


class DatasetLocator(_StrictModel):
    """Where a dataset lives.

    ``path`` is repo-relative and stored in posix form (backslashes are normalised and a
    leading ``./`` is dropped). Absolute paths are accepted for data stored outside the
    repository. ``options`` are passed verbatim to the pandas reader (``read_csv``,
    ``read_parquet`` or ``read_json``).
    """

    type: DatasetType
    path: str
    options: dict[str, Any] = Field(default_factory=dict)

    @field_validator("path")
    @classmethod
    def _normalise_path(cls, value: str) -> str:
        """Normalise the path to posix form without a leading ``./``."""
        path = value.strip().replace("\\", "/")
        while path.startswith("./"):
            path = path[2:]
        if not path:
            raise ValueError("dataset path must not be empty")
        return path


class ColumnRule(_StrictModel):
    """Expectations for one column.

    ``min``/``max`` only apply to numeric columns. ``note`` is free text left by the
    inference step (for example "looks like a date") to help the reviewer; it is not
    enforced.
    """

    name: str
    dtype: LogicalDtype = "unknown"
    nullable: bool = True
    min: float | None = None
    max: float | None = None
    pii: bool = False
    allowed_values: list[str] | None = None
    note: str | None = None

    @field_validator("allowed_values", mode="before")
    @classmethod
    def _stringify_allowed(cls, value: Any) -> Any:
        """Allowed values are compared as strings; accept numbers/bools written in YAML."""
        if value is None:
            return None
        if isinstance(value, (list, tuple)):
            return [_allowed_value_to_str(item) for item in value]
        return value


def _allowed_value_to_str(value: Any) -> str:
    """Render an allowed value the same way validation renders data values."""
    if isinstance(value, bool):
        return str(value)
    if isinstance(value, float) and value.is_integer():
        return str(int(value))
    return str(value)


class RowCountRule(_StrictModel):
    """Inclusive band for the number of rows of a dataset (``None`` = unbounded)."""

    min: int | None = None
    max: int | None = None


class DatasetContract(_StrictModel):
    """Contract for one dataset (usually the input or output of one task).

    ``task_id`` also accepts the input alias ``step_id``.
    """

    name: str
    task_id: str | None = Field(default=None, validation_alias=AliasChoices("task_id", "step_id"))
    locator: DatasetLocator
    columns: list[ColumnRule] = Field(default_factory=list)
    row_count: RowCountRule = Field(default_factory=RowCountRule)
    allow_extra_columns: bool = True

    @model_validator(mode="before")
    @classmethod
    def _single_task_key(cls, data: Any) -> Any:
        return _one_of(data, "task_id", "step_id")

    def column(self, name: str) -> ColumnRule | None:
        """Return the rule for column ``name`` or ``None``."""
        for rule in self.columns:
            if rule.name == name:
                return rule
        return None


class Contract(_StrictModel):
    """Data contract of one DAG (one YAML file); ``dag_id`` also accepts the alias ``pipeline_id``."""

    version: int = 1
    dag_id: str = Field(validation_alias=AliasChoices("dag_id", "pipeline_id"))
    status: Literal["draft", "approved"] = "draft"
    review: str = "required"
    approved_by: str | None = None
    approved_at: datetime | None = None
    source_hash: str | None = None
    inferred_from: list[str] = Field(default_factory=list)
    notes: str | None = None
    datasets: list[DatasetContract] = Field(default_factory=list)

    @model_validator(mode="before")
    @classmethod
    def _single_dag_key(cls, data: Any) -> Any:
        return _one_of(data, "dag_id", "pipeline_id")

    @field_validator("dag_id")
    @classmethod
    def _dag_id_not_empty(cls, value: str) -> str:
        """Reject empty DAG ids."""
        if not value or not value.strip():
            raise ValueError("dag_id must not be empty")
        return value.strip()

    @property
    def is_approved(self) -> bool:
        """True when a human approved the contract (staleness is checked separately)."""
        return self.status == "approved"

    def dataset(self, name: str) -> DatasetContract | None:
        """Return the dataset contract called ``name`` or ``None``."""
        for dataset in self.datasets:
            if dataset.name == name:
                return dataset
        return None

    def datasets_for_task(self, task_id: str) -> list[DatasetContract]:
        """Return the dataset contracts attached to ``task_id``."""
        return [dataset for dataset in self.datasets if dataset.task_id == task_id]


class Violation(BaseModel):
    """One broken contract rule.

    ``row_indexes`` are 0-based row *positions* in the validated DataFrame (for CSV files:
    data rows after the header), capped at :data:`MAX_ROW_INDEXES`. ``expected`` and
    ``actual`` are JSON-safe. For PII columns ``actual`` never contains raw values.
    """

    model_config = ConfigDict(extra="ignore")

    rule: ViolationRule
    dataset: str
    column: str | None = None
    message: str
    expected: Any = None
    actual: Any = None
    row_indexes: list[int] = Field(default_factory=list)

    @field_validator("row_indexes")
    @classmethod
    def _cap_row_indexes(cls, value: list[int]) -> list[int]:
        """Keep at most :data:`MAX_ROW_INDEXES` positions."""
        return list(value[:MAX_ROW_INDEXES])


class ContractError(ValueError):
    """Raised when a contract file cannot be read or does not match the schema."""


__all__ = [
    "LOGICAL_DTYPES",
    "MAX_ROW_INDEXES",
    "ColumnRule",
    "Contract",
    "ContractError",
    "DatasetContract",
    "DatasetLocator",
    "DatasetType",
    "LogicalDtype",
    "RowCountRule",
    "Violation",
    "ViolationRule",
]
