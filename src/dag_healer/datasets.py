"""Load datasets described by :class:`~dag_healer.contracts.schema.DatasetLocator`.

v1 reads local files and S3 objects (``csv``, ``parquet`` and ``json``); the locator
indirection is the extension point for SQL sources later. A path ``s3://bucket/key`` is read
from S3 by :func:`dag_healer.storage.s3.load_s3_dataset` (the ``aws`` extra; capped downloads,
see that module). Other paths are local: relative ones are resolved against the repository
root, absolute ones are used as-is.
"""

from __future__ import annotations

import logging
from collections.abc import Mapping
from pathlib import Path
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    import pandas as pd

    from dag_healer.contracts.schema import DatasetLocator

log = logging.getLogger(__name__)

_JSON_LINES_SUFFIXES = (".jsonl", ".ndjson")


class DatasetError(Exception):
    """Raised when a dataset is missing, unreadable or its locator is invalid."""


def _as_locator(locator: DatasetLocator | Mapping[str, Any]) -> DatasetLocator:
    """Accept a :class:`DatasetLocator` or a plain mapping (``{"type": ..., "path": ...}``)."""
    from pydantic import ValidationError

    from dag_healer.contracts.schema import DatasetLocator

    if isinstance(locator, DatasetLocator):
        return locator
    try:
        return DatasetLocator.model_validate(dict(locator))
    except (TypeError, ValueError, ValidationError) as exc:
        raise DatasetError(f"invalid dataset locator {locator!r}: {exc}") from exc


def _is_remote(path: str) -> bool:
    """True for a locator path of a remote store (``s3://``), which has no local path."""
    from dag_healer.storage.s3 import is_s3_uri

    return is_s3_uri(path)


def resolve_dataset_path(locator: DatasetLocator | Mapping[str, Any], repo_root: str | Path) -> Path:
    """Return the absolute path of the dataset described by ``locator``.

    Raises :class:`DatasetError` for an ``s3://`` locator: an S3 object has no local path.
    """
    resolved = _as_locator(locator)
    if _is_remote(resolved.path):
        raise DatasetError(f"dataset {resolved.path} is an S3 object, not a local file")
    path = Path(resolved.path)
    if path.is_absolute():
        return path
    return Path(repo_root) / path


def _read(kind: str, path: Path, options: dict[str, Any]) -> pd.DataFrame:
    """Call the pandas reader for ``kind`` with ``options``."""
    import pandas as pd

    if kind == "csv":
        return pd.read_csv(path, **options)
    if kind == "parquet":
        return pd.read_parquet(path, **options)
    if kind == "json":
        if "lines" not in options and path.suffix.lower() in _JSON_LINES_SUFFIXES:
            options = {**options, "lines": True}
        return pd.read_json(path, **options)
    raise DatasetError(f"unsupported dataset type {kind!r} (expected csv, parquet or json)")


def load_dataset(locator: DatasetLocator | Mapping[str, Any], repo_root: str | Path) -> pd.DataFrame:
    """Load the dataset described by ``locator`` into a pandas DataFrame.

    ``locator.options`` are passed to ``pandas.read_csv`` / ``read_parquet`` / ``read_json``
    (``.jsonl``/``.ndjson`` files default to ``lines=True``). Raises :class:`DatasetError`
    when the file does not exist or cannot be parsed; the message names the file and the
    reason, with emails/phone numbers/long digit runs redacted (reader errors can quote
    cell values). An ``s3://`` locator is read from S3 (``repo_root`` is not used).
    """
    resolved = _as_locator(locator)
    if _is_remote(resolved.path):
        from dag_healer.storage.s3 import load_s3_dataset

        return load_s3_dataset(resolved)
    path = resolve_dataset_path(resolved, repo_root)
    if not path.exists():
        raise DatasetError(f"dataset file not found: {resolved.path} (resolved to {path})")
    if path.is_dir() and resolved.type != "parquet":
        raise DatasetError(f"dataset path is a directory, not a {resolved.type} file: {resolved.path}")
    try:
        frame = _read(resolved.type, path, dict(resolved.options))
    except DatasetError:
        raise
    except Exception as exc:  # pandas/pyarrow raise many unrelated exception types
        from dag_healer.masking import mask_text

        reason = mask_text(f"{type(exc).__name__}: {exc}")
        raise DatasetError(f"cannot read {resolved.type} dataset {resolved.path}: {reason}") from exc
    log.debug("Loaded dataset %s (%d rows, %d columns)", resolved.path, len(frame), frame.shape[1])
    return frame


__all__ = ["DatasetError", "load_dataset", "resolve_dataset_path"]
