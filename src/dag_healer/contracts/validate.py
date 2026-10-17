"""Validate DataFrames against data contracts.

Validation reports *every* violation it finds (not only the first one). Dtype checks are
tolerant, because the same data is stored differently by pandas 2 and pandas 3 and by the
CSV/parquet/JSON readers:

* ``integer`` accepts integer columns and float columns whose non-null values are integral
  (an integer column with nulls); ``float`` accepts integer and float columns; numeric
  strings in object columns count as numbers.
* ``date`` and ``datetime`` are checked by *parseability* (``datetime64`` storage, Python
  ``date``/``datetime`` objects, or strings that parse with
  ``pandas.to_datetime(format="ISO8601")``), not by storage dtype.
* ``string`` accepts any non-numeric column (object, pandas ``str``/``string``, dates ...);
  only a numeric or boolean column fails it.
* ``unknown`` accepts anything. All-null columns are only checked for nullability.

Row positions in ``Violation.row_indexes`` are 0-based positions in the DataFrame, capped
at 20. For PII columns, violations never carry raw values.
"""

from __future__ import annotations

import math
from collections.abc import Mapping
from typing import Any

import numpy as np
import pandas as pd

from dag_healer.contracts.infer import (
    coerce_numeric,
    infer_logical_dtype,
    normalize_series,
    parse_iso_datetimes,
    storage_kind,
    value_kind,
)
from dag_healer.contracts.schema import (
    MAX_ROW_INDEXES,
    ColumnRule,
    Contract,
    DatasetContract,
    Violation,
)

_BOOLEAN_STRINGS = frozenset({"true", "false"})
_MAX_REPORTED_VALUES = 5
_MAX_VALUE_CHARS = 60


# --------------------------------------------------------------------------- helpers


def _positions(mask: np.ndarray) -> list[int]:
    """First :data:`MAX_ROW_INDEXES` positions where ``mask`` is true."""
    return [int(position) for position in np.flatnonzero(mask)[:MAX_ROW_INDEXES]]


def _python_number(value: float) -> int | float | None:
    """JSON-safe number: integral floats become ints, non-finite values become ``None``."""
    if not math.isfinite(value):
        return None
    return int(value) if float(value).is_integer() else float(value)


def _render_value(value: Any) -> str:
    """Render a data value as a string, the same way ``allowed_values`` are written."""
    if isinstance(value, (bool, np.bool_)):
        return str(bool(value))
    if isinstance(value, (float, np.floating)) and float(value).is_integer():
        return str(int(value))
    if isinstance(value, np.integer):
        return str(int(value))
    return str(value)


def _is_pii(rule: ColumnRule) -> bool:
    """True when raw values of the column must not appear in a violation."""
    from dag_healer.masking import is_sensitive_column

    return rule.pii or is_sensitive_column(rule.name)


def _column_positions(df: pd.DataFrame) -> dict[str, int]:
    """Map column name (as str) -> first position in ``df``."""
    positions: dict[str, int] = {}
    for position, label in enumerate(df.columns):
        positions.setdefault(str(label), position)
    return positions


# --------------------------------------------------------------------------- dtype checks


def _object_mismatch_mask(values: np.ndarray, expected: str) -> np.ndarray:
    """Element-wise mismatch mask for an object column (``values`` are all non-null)."""
    mask = np.zeros(len(values), dtype=bool)
    if expected in ("integer", "float"):
        for position, value in enumerate(values):
            kind = value_kind(value)
            if kind == "string":
                number = _string_to_float(value)
                kind = "other" if number is None else ("integer" if number.is_integer() else "float")
            mask[position] = kind not in (("integer",) if expected == "integer" else ("integer", "float"))
        return mask
    if expected == "boolean":
        for position, value in enumerate(values):
            kind = value_kind(value)
            ok = kind == "boolean" or (kind == "string" and value.strip().lower() in _BOOLEAN_STRINGS)
            mask[position] = not ok
        return mask
    # date / datetime
    string_positions: list[int] = []
    for position, value in enumerate(values):
        kind = value_kind(value)
        if kind == "string":
            string_positions.append(position)
        else:
            mask[position] = kind not in ("date", "datetime")
    if string_positions:
        strings = pd.Series([values[position].strip() for position in string_positions], dtype=object)
        failed = parse_iso_datetimes(strings).isna().to_numpy()
        for offset, position in enumerate(string_positions):
            mask[position] = bool(failed[offset])
    return mask


def _string_to_float(value: str) -> float | None:
    """Parse a numeric string; ``None`` when it is not a finite number."""
    try:
        number = float(value.strip())
    except ValueError:
        return None
    return number if math.isfinite(number) else None


def _whole_column_mismatch(kind: str, expected: str) -> bool | None:
    """Decide a dtype check from the storage kind alone (``None`` = inspect values)."""
    if expected == "unknown":
        return False
    if kind == "object":
        return None
    if expected == "string":
        return kind in ("boolean", "integer", "float", "complex")
    if expected == "integer":
        return None if kind == "float" else kind != "integer"
    if expected == "float":
        return kind not in ("integer", "float")
    if expected == "boolean":
        return kind != "boolean"
    # date / datetime
    return kind != "datetime"


def dtype_mismatch_mask(series: pd.Series, expected: str) -> tuple[bool, np.ndarray]:
    """Return ``(mismatch, row_mask)`` for a column against an expected logical dtype.

    ``row_mask`` flags the offending rows when they can be identified (all false for a
    whole-column mismatch such as a numeric column under a ``string`` rule).
    """
    series = normalize_series(series).reset_index(drop=True)
    notna = series.notna().to_numpy()
    empty = np.zeros(len(series), dtype=bool)
    if not notna.any():
        return False, empty
    kind = storage_kind(series)
    decided = _whole_column_mismatch(kind, expected)
    if decided is not None:
        return decided, empty
    if kind == "float":  # integer rule on a float column: every value must be integral
        array = series.to_numpy(dtype="float64", na_value=np.nan)
        with np.errstate(invalid="ignore"):
            bad = notna & ~(np.isfinite(array) & (array == np.floor(array)))
        return bool(bad.any()), bad
    if expected == "string":
        if isinstance(series.dtype, pd.StringDtype):  # pandas 3 "str" / "string[python]"
            return False, empty
        kinds = {value_kind(value) for value in series[notna]}
        numeric_only = kinds <= {"integer", "float"} or kinds == {"boolean"}
        return numeric_only, empty
    values = series.to_numpy(dtype=object)[notna]
    sub_mask = _object_mismatch_mask(values, expected)
    full = empty.copy()
    full[np.flatnonzero(notna)] = sub_mask
    return bool(full.any()), full


# --------------------------------------------------------------------------- column checks


def _check_nulls(series: pd.Series, rule: ColumnRule, dataset: str) -> list[Violation]:
    """``nulls_not_allowed`` when a non-nullable column has nulls."""
    if rule.nullable:
        return []
    mask = series.isna().to_numpy()
    count = int(mask.sum())
    if count == 0:
        return []
    return [
        Violation(
            rule="nulls_not_allowed",
            dataset=dataset,
            column=rule.name,
            message=(
                f"Column '{rule.name}' has {count} null value(s) out of {len(series)} row(s) "
                "but the contract does not allow nulls."
            ),
            expected={"nullable": False},
            actual={"null_count": count, "rows": len(series)},
            row_indexes=_positions(mask),
        )
    ]


def _check_dtype(series: pd.Series, rule: ColumnRule, dataset: str) -> list[Violation]:
    """``dtype_mismatch`` when values do not match the logical dtype."""
    mismatch, mask = dtype_mismatch_mask(series, rule.dtype)
    if not mismatch:
        return []
    actual = infer_logical_dtype(series, name=rule.name).dtype
    bad = int(mask.sum())
    detail = f"{bad} value(s) are not {rule.dtype}" if bad else f"the column is {actual}"
    return [
        Violation(
            rule="dtype_mismatch",
            dataset=dataset,
            column=rule.name,
            message=f"Column '{rule.name}' should be {rule.dtype} but {detail}.",
            expected=rule.dtype,
            actual=actual,
            row_indexes=_positions(mask),
        )
    ]


def _check_range(series: pd.Series, rule: ColumnRule, dataset: str) -> list[Violation]:
    """``out_of_range`` for numeric values outside ``[min, max]``."""
    if rule.min is None and rule.max is None:
        return []
    numbers = coerce_numeric(series)
    finite = np.isfinite(numbers)
    if not finite.any():
        return []
    with np.errstate(invalid="ignore"):
        below = finite & (numbers < rule.min) if rule.min is not None else np.zeros(len(numbers), bool)
        above = finite & (numbers > rule.max) if rule.max is not None else np.zeros(len(numbers), bool)
    mask = below | above
    count = int(mask.sum())
    if count == 0:
        return []
    observed = numbers[finite]
    actual: dict[str, Any] = {"count": count}
    if not _is_pii(rule):
        actual["min"] = _python_number(float(observed.min()))
        actual["max"] = _python_number(float(observed.max()))
    return [
        Violation(
            rule="out_of_range",
            dataset=dataset,
            column=rule.name,
            message=(
                f"Column '{rule.name}' has {count} value(s) outside the allowed range "
                f"[{_python_number(rule.min) if rule.min is not None else '-inf'}, "
                f"{_python_number(rule.max) if rule.max is not None else 'inf'}]."
            ),
            expected={
                "min": _python_number(rule.min) if rule.min is not None else None,
                "max": _python_number(rule.max) if rule.max is not None else None,
            },
            actual=actual,
            row_indexes=_positions(mask),
        )
    ]


def _check_allowed(series: pd.Series, rule: ColumnRule, dataset: str) -> list[Violation]:
    """``not_allowed_value`` for values outside ``allowed_values``."""
    if rule.allowed_values is None:
        return []
    allowed = set(rule.allowed_values)
    values = normalize_series(series).to_numpy(dtype=object)
    notna = series.notna().to_numpy()
    mask = np.zeros(len(values), dtype=bool)
    offending: list[str] = []
    for position in np.flatnonzero(notna):
        rendered = _render_value(values[position])
        if rendered not in allowed:
            mask[position] = True
            if len(offending) < _MAX_REPORTED_VALUES and rendered not in offending:
                offending.append(rendered)
    count = int(mask.sum())
    if count == 0:
        return []
    actual: dict[str, Any] = {"count": count}
    if not _is_pii(rule):
        actual["values"] = [value[:_MAX_VALUE_CHARS] for value in offending]
    return [
        Violation(
            rule="not_allowed_value",
            dataset=dataset,
            column=rule.name,
            message=f"Column '{rule.name}' has {count} value(s) that are not in the allowed values.",
            expected={"allowed_values": list(rule.allowed_values)},
            actual=actual,
            row_indexes=_positions(mask),
        )
    ]


def validate_column(series: pd.Series, rule: ColumnRule, dataset: str) -> list[Violation]:
    """Validate one column against its rule (nulls, dtype, range, allowed values)."""
    series = series.reset_index(drop=True)
    violations = _check_nulls(series, rule, dataset)
    dtype_violations = _check_dtype(series, rule, dataset)
    violations.extend(dtype_violations)
    numeric_rule = rule.dtype in ("integer", "float", "unknown")
    if not dtype_violations or numeric_rule:
        violations.extend(_check_range(series, rule, dataset))
    violations.extend(_check_allowed(series, rule, dataset))
    return violations


# --------------------------------------------------------------------------- dataset checks


def _check_row_count(df: pd.DataFrame, dataset: DatasetContract) -> list[Violation]:
    """``row_count_out_of_band`` when the number of rows is outside the band."""
    band = dataset.row_count
    rows = len(df)
    too_few = band.min is not None and rows < band.min
    too_many = band.max is not None and rows > band.max
    if not (too_few or too_many):
        return []
    return [
        Violation(
            rule="row_count_out_of_band",
            dataset=dataset.name,
            column=None,
            message=(
                f"Dataset '{dataset.name}' has {rows} row(s); the contract expects between "
                f"{band.min if band.min is not None else 0} and "
                f"{band.max if band.max is not None else 'unlimited'}."
            ),
            expected={"min": band.min, "max": band.max},
            actual=rows,
        )
    ]


def validate_dataset(df: pd.DataFrame, dataset: DatasetContract) -> list[Violation]:
    """Validate ``df`` against ``dataset`` and return every violation found."""
    violations = _check_row_count(df, dataset)
    positions = _column_positions(df)
    for rule in dataset.columns:
        position = positions.get(rule.name)
        if position is None:
            violations.append(
                Violation(
                    rule="missing_column",
                    dataset=dataset.name,
                    column=rule.name,
                    message=f"Column '{rule.name}' is missing from dataset '{dataset.name}'.",
                    expected=rule.name,
                    actual=sorted(positions),
                )
            )
            continue
        violations.extend(validate_column(df.iloc[:, position], rule, dataset.name))
    if not dataset.allow_extra_columns:
        expected = {rule.name for rule in dataset.columns}
        for column in positions:
            if column not in expected:
                violations.append(
                    Violation(
                        rule="unexpected_column",
                        dataset=dataset.name,
                        column=column,
                        message=f"Column '{column}' is not part of the contract of '{dataset.name}'.",
                        expected=sorted(expected),
                        actual=column,
                    )
                )
    return violations


def validate_contract(contract: Contract, frames: Mapping[str, Any]) -> list[Violation]:
    """Validate every dataset of ``contract``.

    ``frames`` maps dataset name -> DataFrame. A missing entry, ``None`` or an exception
    instance (for example the :class:`dag_healer.datasets.DatasetError` raised while
    loading it) yields a ``dataset_unreadable`` violation.
    """
    violations: list[Violation] = []
    for dataset in contract.datasets:
        frame = frames.get(dataset.name)
        if isinstance(frame, pd.DataFrame):
            violations.extend(validate_dataset(frame, dataset))
            continue
        reason = str(frame) if isinstance(frame, BaseException) else "no data was loaded for it"
        violations.append(
            Violation(
                rule="dataset_unreadable",
                dataset=dataset.name,
                column=None,
                message=f"Dataset '{dataset.name}' ({dataset.locator.path}) could not be read: {reason}",
                expected=dataset.locator.path,
                actual=None,
            )
        )
    return violations


__all__ = ["dtype_mismatch_mask", "validate_column", "validate_contract", "validate_dataset"]
