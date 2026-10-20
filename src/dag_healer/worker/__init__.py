"""The dag-healer worker: reconcile failures, triage incidents, apply human commands.

* :mod:`dag_healer.worker.loop` - :func:`run_worker`, the polling loop (``dag-healer worker``).
* :mod:`dag_healer.worker.pipeline` - :func:`reconcile` and :func:`process_incident`.
* :mod:`dag_healer.worker.commands` - :func:`apply_commands` (resolve, retry, approve).

The names below are imported lazily so ``import dag_healer.worker`` stays cheap.
"""

from __future__ import annotations

import importlib
from typing import Any

_EXPORTS: dict[str, str] = {
    "run_worker": "dag_healer.worker.loop",
    "Worker": "dag_healer.worker.loop",
    "WorkerStats": "dag_healer.worker.loop",
    "process_incident": "dag_healer.worker.pipeline",
    "reconcile": "dag_healer.worker.pipeline",
    "apply_commands": "dag_healer.worker.pipeline",
    "CommandOutcome": "dag_healer.worker.commands",
}

__all__ = sorted(_EXPORTS)


def __getattr__(name: str) -> Any:
    """Import the public names on first use."""
    module = _EXPORTS.get(name)
    if module is None:
        raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
    return getattr(importlib.import_module(module), name)
