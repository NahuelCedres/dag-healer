"""Data contracts: schema, YAML I/O, inference and validation.

Importing this package is cheap (pydantic + PyYAML only). The pandas-based helpers
(:mod:`dag_healer.contracts.infer`, :mod:`dag_healer.contracts.validate`) are loaded lazily
on first attribute access so that ``dag_healer.config`` can import the schema without
pulling pandas into every Airflow task process.
"""

from __future__ import annotations

import importlib
from typing import Any

from dag_healer.contracts.io import (
    approve_contract,
    compute_source_hash,
    contract_path,
    dump_contract_yaml,
    is_stale,
    list_contracts,
    load_contract,
    save_contract,
)
from dag_healer.contracts.schema import (
    ColumnRule,
    Contract,
    ContractError,
    DatasetContract,
    DatasetLocator,
    RowCountRule,
    Violation,
)

_LAZY: dict[str, str] = {
    "infer_contract": "dag_healer.contracts.infer",
    "infer_dataset_contract": "dag_healer.contracts.infer",
    "logical_dtype": "dag_healer.contracts.infer",
    "validate_contract": "dag_healer.contracts.validate",
    "validate_dataset": "dag_healer.contracts.validate",
}


def __getattr__(name: str) -> Any:
    """Load pandas-dependent helpers on first use."""
    module_name = _LAZY.get(name)
    if module_name is None:
        raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
    value = getattr(importlib.import_module(module_name), name)
    globals()[name] = value
    return value


__all__ = [
    "ColumnRule",
    "Contract",
    "ContractError",
    "DatasetContract",
    "DatasetLocator",
    "RowCountRule",
    "Violation",
    "approve_contract",
    "compute_source_hash",
    "contract_path",
    "dump_contract_yaml",
    "infer_contract",
    "infer_dataset_contract",
    "is_stale",
    "list_contracts",
    "load_contract",
    "logical_dtype",
    "save_contract",
    "validate_contract",
    "validate_dataset",
]
