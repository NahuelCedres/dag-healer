"""dag-healer: diagnoses and repairs failing tasks of existing Airflow 3 DAGs.

This module is imported in every Airflow task process (through the plugin and the
listener), so it must stay free of heavy imports. ``dag_healer.callback`` is
resolved lazily so DAG authors can write::

    import dag_healer

    default_args = {"on_failure_callback": dag_healer.callback}
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

from dag_healer._version import __version__

if TYPE_CHECKING:
    from dag_healer.airflow.callback import on_failure_callback as callback

__all__ = ["__version__", "callback"]


def __getattr__(name: str) -> Any:
    """Lazily expose ``dag_healer.callback`` (the task ``on_failure_callback``)."""
    if name == "callback":
        from dag_healer.airflow.callback import on_failure_callback

        return on_failure_callback
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
