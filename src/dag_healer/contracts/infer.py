"""Infer draft data contracts from sample DataFrames.

Inference is deliberately *permissive*: a draft contract must not flag the next day's
normal data as broken, otherwise the classification policy would report data problems that
do not exist. The rules (all documented here because reviewers read the resulting YAML):

* **Logical dtypes** are storage independent and work the same on pandas 2 (``object``
  string columns) and pandas 3 (default ``str`` dtype):

  - bool / ``boolean`` columns -> ``"boolean"``;
  - integer (``int64``, ``Int64`` ...) columns, and ``float64`` columns whose non-null values
    are all integral (an integer column with nulls) -> ``"integer"``; other floats ->
    ``"float"``;
  - ``datetime64`` columns (naive or tz-aware) -> ``"datetime"``; columns of
    ``datetime.date`` objects (parquet ``date32``) -> ``"date"``;
  - string-like columns whose non-null values *all* look like ISO dates (``YYYY-MM-DD``
    prefix) and *all* parse with ``pandas.to_datetime(format="ISO8601")``:

    + if any value has a time component (``2026-10-02T14:03:00Z``) -> ``"datetime"``;
    + if the column name ends with ``_at``, ``_ts`` or ``_time`` -> ``"datetime"``;
    + otherwise (date-only values such as ``2026-09-01``) -> ``"string"`` with the note
      ``"looks like a date"``. Date-only samples are kept as strings on purpose: upstream
      systems often start sending full timestamps for the same field, and that is a
      perfectly valid value which code (not data) must handle. The reviewer can tighten
      the rule to ``date``/``datetime`` (validated by ISO-8601 parseability).

  - any other string-like column -> ``"string"``; all-null columns -> ``"unknown"``.

* **Nullability**: ``nullable`` is true when the sample has at least one null.
* **Numeric ranges** (only non-PII ``integer``/``float`` columns) are derived from the data with a
  sign + magnitude guard instead of the exact observed extremes: ``min`` is ``0`` when every
  observed value is non-negative (otherwise 10x the observed minimum) and ``max`` is 10x the
  observed maximum (``0`` when every value is non-positive). Identifier-like columns
  (``id``, ``*_id``, ``*_key``, ``*_code``, ``*_number``, ``*_no``) get no ``max``
  because they grow over time.
* **Row count** band: ``min = max(1, floor(n * 0.5))`` and ``max = ceil(n * 3) + 10``.
* **PII**: a column is flagged ``pii`` when its name looks sensitive
  (:func:`dag_healer.masking.is_sensitive_column`) or it is listed in ``pii_columns``.
"""

from __future__ import annotations

import math
import re
from collections.abc import Iterable, Mapping
from dataclasses import dataclass
from datetime import date, datetime
from decimal import Decimal
from typing import TYPE_CHECKING, Any

import numpy as np
import pandas as pd
from pandas.api import types as ptypes

from dag_healer.contracts.schema import (
    ColumnRule,
    Contract,
    DatasetContract,
    DatasetLocator,
    RowCountRule,
)

if TYPE_CHECKING:
    from collections.abc import Sequence

DATE_PREFIX_RE = re.compile(r"^\d{4}-\d{2}-\d{2}")
TIME_PART_RE = re.compile(r"^\d{4}-\d{2}-\d{2}[T ]\d{1,2}:\d{2}")
TIMESTAMP_NAME_SUFFIXES: tuple[str, ...] = ("_at", "_ts", "_time")
IDENTIFIER_SUFFIXES: tuple[str, ...] = ("_id", "-id", "_key", "_code", "_number", "_no", "_uuid")
_CAMEL_ID_RE = re.compile(r"[a-z0-9](?:Id|ID|Key|Code|Number)$")
RANGE_HEADROOM = 10.0
LOOKS_LIKE_DATE_NOTE = (
    "looks like a date (YYYY-MM-DD); inferred as 'string' so other date/time formats are not "
    "rejected. Change dtype to 'date' or 'datetime' to require ISO-8601 parseable values."
)

_INFER_NOTES = (
    "Inferred automatically from sample data. Review dtypes, nullability, numeric ranges "
    "(inferred with a 10x headroom) and row-count bands before approving."
)


@dataclass(frozen=True)
class DtypeInference:
    """Result of logical dtype inference for one column."""

    dtype: str
    note: str | None = None


# --------------------------------------------------------------------------- dtype helpers


def normalize_series(series: pd.Series) -> pd.Series:
    """Return ``series`` with categoricals expanded to object values (other dtypes untouched)."""
    if isinstance(series.dtype, pd.CategoricalDtype):
        return series.astype(object)
    return series


def storage_kind(series: pd.Series) -> str:
    """Classify the *storage* dtype of a series.

    Returns one of ``boolean``, ``integer``, ``float``, ``datetime``, ``timedelta``,
    ``complex`` or ``object`` (strings, pandas 3 ``str``, ``string[python]``, mixed objects).
    Categoricals must be normalised first (:func:`normalize_series`).
    """
    dtype = series.dtype
    if ptypes.is_bool_dtype(dtype):
        return "boolean"
    if ptypes.is_integer_dtype(dtype):
        return "integer"
    if ptypes.is_float_dtype(dtype):
        return "float"
    if ptypes.is_datetime64_any_dtype(dtype):
        return "datetime"
    if ptypes.is_timedelta64_dtype(dtype):
        return "timedelta"
    if ptypes.is_complex_dtype(dtype):
        return "complex"
    return "object"


def value_kind(value: Any) -> str:
    """Logical kind of one non-null Python/numpy value."""
    if isinstance(value, (bool, np.bool_)):
        return "boolean"
    if isinstance(value, (int, np.integer)):
        return "integer"
    if isinstance(value, (float, np.floating, Decimal)):
        number = float(value)
        return "integer" if math.isfinite(number) and number.is_integer() else "float"
    if isinstance(value, (datetime, np.datetime64)):
        return "datetime"
    if isinstance(value, date):
        return "date"
    if isinstance(value, str):
        return "string"
    return "other"


def _non_null(series: pd.Series) -> pd.Series:
    """Non-null values of ``series`` with a fresh positional index."""
    return series[series.notna()].reset_index(drop=True)


def _all_integral(values: pd.Series) -> bool:
    """True when every value of a float series is finite and integral."""
    array = values.to_numpy(dtype="float64", na_value=np.nan)
    if array.size == 0:
        return False
    finite = np.isfinite(array)
    return bool(finite.all() and (array == np.floor(array)).all())


def parse_iso_datetimes(values: pd.Series) -> pd.Series:
    """Parse values with ``pandas.to_datetime(format="ISO8601", utc=True)``; failures -> NaT.

    Mixed offsets and naive/aware mixes are normalised to UTC so a single bad value never
    makes the whole column fail. Works on pandas 2 and 3.
    """
    try:
        return pd.to_datetime(values, format="ISO8601", errors="coerce", utc=True)
    except (TypeError, ValueError, OverflowError):
        parsed = []
        for value in values:
            try:
                parsed.append(pd.to_datetime(value, format="ISO8601", utc=True))
            except (TypeError, ValueError, OverflowError):
                parsed.append(pd.NaT)
        return pd.Series(parsed, index=values.index, dtype="datetime64[ns, UTC]")


def _infer_string_dtype(values: pd.Series, name: str | None) -> DtypeInference:
    """Infer the logical type of non-null string values (see the module docstring)."""
    stripped = values.astype(str).str.strip()
    if not bool(stripped.str.match(DATE_PREFIX_RE.pattern).all()):
        return DtypeInference("string")
    if bool(parse_iso_datetimes(stripped).isna().any()):
        return DtypeInference("string")
    if bool(stripped.str.match(TIME_PART_RE.pattern).any()):
        return DtypeInference("datetime")
    if name is not None and name.lower().endswith(TIMESTAMP_NAME_SUFFIXES):
        return DtypeInference("datetime")
    return DtypeInference("string", LOOKS_LIKE_DATE_NOTE)


def _combine_kinds(kinds: set[str]) -> str:
    """Collapse the set of value kinds of an object column into one logical dtype."""
    if kinds == {"boolean"}:
        return "boolean"
    if kinds == {"integer"}:
        return "integer"
    if kinds <= {"integer", "float"}:
        return "float"
    if kinds == {"date"}:
        return "date"
    if kinds <= {"datetime", "date"}:
        return "datetime"
    if kinds == {"string"}:
        return "string"
    return "string" if "string" in kinds else "unknown"


def infer_logical_dtype(series: pd.Series, *, name: str | None = None) -> DtypeInference:
    """Infer the logical dtype (and an optional reviewer note) of a column."""
    series = normalize_series(series)
    kind = storage_kind(series)
    values = _non_null(series)
    if values.empty:
        return DtypeInference("unknown")
    if kind in ("boolean", "integer", "datetime"):
        return DtypeInference(kind)
    if kind == "float":
        return DtypeInference("integer" if _all_integral(values) else "float")
    if kind in ("timedelta", "complex"):
        return DtypeInference("unknown")
    if isinstance(series.dtype, pd.StringDtype):
        return _infer_string_dtype(values, name)
    kinds = {value_kind(value) for value in values}
    combined = _combine_kinds(kinds)
    if combined == "string" and kinds == {"string"}:
        return _infer_string_dtype(values, name)
    return DtypeInference(combined)


def logical_dtype(series: pd.Series, *, name: str | None = None) -> str:
    """Return the logical dtype of ``series`` (one of :data:`schema.LOGICAL_DTYPES`)."""
    return infer_logical_dtype(series, name=name).dtype


def coerce_numeric(series: pd.Series) -> np.ndarray:
    """Return a float64 array with NaN wherever a value is null or not a number.

    Booleans are *not* numbers here; numeric strings (``"12.5"``) are.
    """
    series = normalize_series(series)
    kind = storage_kind(series)
    if kind in ("integer", "float"):
        return series.to_numpy(dtype="float64", na_value=np.nan)
    if kind != "object":
        return np.full(len(series), np.nan)
    out = np.full(len(series), np.nan)
    for position, value in enumerate(series.to_numpy(dtype=object)):
        out[position] = _to_float(value)
    return out


def _to_float(value: Any) -> float:
    """Convert one value to float, NaN when it is null, boolean or not numeric."""
    if value is None or isinstance(value, (bool, np.bool_)):
        return math.nan
    if isinstance(value, (int, float, np.integer, np.floating, Decimal)):
        return float(value)
    if isinstance(value, str):
        try:
            return float(value.strip())
        except ValueError:
            return math.nan
    return math.nan


# --------------------------------------------------------------------------- bounds


def is_identifier_column(name: str) -> bool:
    """True for identifier-like names (``id``, ``order_id``, ``customerId``, ``sku_code`` ...)."""
    lowered = name.strip().lower()
    if lowered in ("id", "uuid", "key", "code"):
        return True
    return lowered.endswith(IDENTIFIER_SUFFIXES) or bool(_CAMEL_ID_RE.search(name.strip()))


def _round_bound(value: float) -> float:
    """Round a bound to 6 significant digits to keep the YAML readable."""
    return float(f"{value:.6g}")


def numeric_bounds(values: np.ndarray, *, name: str) -> tuple[float | None, float | None]:
    """Derive ``(min, max)`` for a numeric column (see the module docstring)."""
    finite = values[np.isfinite(values)]
    if finite.size == 0:
        return None, None
    observed_min, observed_max = float(finite.min()), float(finite.max())
    lower = 0.0 if observed_min >= 0 else _round_bound(observed_min * RANGE_HEADROOM)
    if is_identifier_column(name):
        upper = None
    elif observed_max > 0:
        upper = _round_bound(observed_max * RANGE_HEADROOM)
    else:
        upper = 0.0
    return lower, upper


def row_count_band(n_rows: int) -> RowCountRule:
    """Row-count band for a sample of ``n_rows`` rows."""
    return RowCountRule(min=max(1, math.floor(n_rows * 0.5)), max=math.ceil(n_rows * 3) + 10)


# --------------------------------------------------------------------------- inference


def infer_column_rule(series: pd.Series, *, name: str, pii: bool = False) -> ColumnRule:
    """Infer a :class:`ColumnRule` for one column."""
    inference = infer_logical_dtype(series, name=name)
    lower: float | None = None
    upper: float | None = None
    if inference.dtype in ("integer", "float") and not pii:  # bounds would leak PII values
        lower, upper = numeric_bounds(coerce_numeric(series), name=name)
    return ColumnRule(
        name=name,
        dtype=inference.dtype,
        nullable=bool(series.isna().any()),
        min=lower,
        max=upper,
        pii=pii,
        note=inference.note,
    )


def _as_locator(locator: DatasetLocator | Mapping[str, Any]) -> DatasetLocator:
    """Accept a :class:`DatasetLocator` or a plain mapping."""
    if isinstance(locator, DatasetLocator):
        return locator
    return DatasetLocator.model_validate(dict(locator))


def infer_dataset_contract(
    df: pd.DataFrame,
    *,
    name: str,
    task_id: str | None,
    locator: DatasetLocator | Mapping[str, Any],
    pii_columns: Iterable[str] = (),
) -> DatasetContract:
    """Infer a :class:`DatasetContract` from a sample DataFrame."""
    from dag_healer.masking import is_sensitive_column

    listed_pii = {str(column).lower() for column in pii_columns}
    rules: list[ColumnRule] = []
    seen: set[str] = set()
    for position, label in enumerate(df.columns):
        column = str(label)
        if column in seen:
            continue
        seen.add(column)
        pii = is_sensitive_column(column) or column.lower() in listed_pii
        rules.append(infer_column_rule(df.iloc[:, position], name=column, pii=pii))
    return DatasetContract(
        name=name,
        task_id=task_id,
        locator=_as_locator(locator),
        columns=rules,
        row_count=row_count_band(len(df)),
        allow_extra_columns=True,
    )


def infer_contract(
    dag_id: str,
    datasets: Iterable[DatasetContract],
    *,
    inferred_from: Sequence[str] | None = None,
    notes: str | None = None,
) -> Contract:
    """Build a draft :class:`Contract` for ``dag_id`` from inferred dataset contracts."""
    return Contract(
        dag_id=dag_id,
        status="draft",
        review="required",
        inferred_from=list(inferred_from or []),
        notes=notes if notes is not None else _INFER_NOTES,
        datasets=list(datasets),
    )


__all__ = [
    "LOOKS_LIKE_DATE_NOTE",
    "DtypeInference",
    "coerce_numeric",
    "infer_column_rule",
    "infer_contract",
    "infer_dataset_contract",
    "infer_logical_dtype",
    "is_identifier_column",
    "logical_dtype",
    "normalize_series",
    "numeric_bounds",
    "parse_iso_datetimes",
    "row_count_band",
    "storage_kind",
    "value_kind",
]
