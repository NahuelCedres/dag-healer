"""Static analysis of the user's repository (DAG discovery, test layout).

Only standard-library modules are imported here; nothing in this package imports or
executes user code.
"""

from __future__ import annotations

from dag_healer.repo.dagscan import DagInfo, DatasetRef, TaskInfo, scan_dags
from dag_healer.repo.profile import TestLayout, detect_test_layout, file_sha256, snapshot_hashes

__all__ = [
    "DagInfo",
    "DatasetRef",
    "TaskInfo",
    "TestLayout",
    "detect_test_layout",
    "file_sha256",
    "scan_dags",
    "snapshot_hashes",
]
