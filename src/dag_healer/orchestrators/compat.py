"""Bridge from the phase-5 ``client=`` arguments to orchestrator adapters.

Phase-5 entry points take an Airflow REST client (``gather_context(cfg, incident, client)``,
``run_worker(..., client=...)``), and the tests pass in-memory doubles of it. They keep working
unchanged once the core talks to ports: :func:`as_adapter` returns an adapter as is, wraps an
Airflow client (or any double of one) in the Airflow adapter, and keeps ``None`` (offline
context gathering, today's behaviour). See section 3.8 of ``docs/architecture/orchestrators.md``.
"""

from __future__ import annotations

import inspect
from typing import TYPE_CHECKING, Any

from dag_healer.core.ports import OrchestratorAdapter
from dag_healer.orchestrators.registry import ensure_adapter, load_factory

if TYPE_CHECKING:
    from dag_healer.config import HealerConfig

#: Methods of :class:`dag_healer.airflow.client.AirflowClient` the phase-5 core calls; an object
#: with any of them is a legacy client (the doubles in the tests implement only what they need).
LEGACY_CLIENT_METHODS: tuple[str, ...] = (
    "get_task_log",
    "get_dag",
    "get_dag_source",
    "get_task_instance",
    "list_failed_task_instances",
    "clear_task_instances",
    "trigger_dag_run",
)
#: The factory that wraps a legacy client (the Airflow adapter, whatever ``orchestrator.type`` says).
LEGACY_CLIENT_FACTORY = "dag_healer.airflow.adapter:FACTORY"
#: Members every adapter defines as real (class or instance) attributes.
ADAPTER_MEMBERS: tuple[str, ...] = (
    "type",
    "instance",
    "display_name",
    "vocabulary",
    "capabilities",
    "taxonomy",
    "feed",
    "push",
    "queue",
    "inspector",
    "code",
    "remediator",
    "links",
    "health",
    "doctor_checks",
    "sandbox_env",
    "close",
)


def _defines(obj: Any, name: str) -> bool:
    """True when ``name`` is a real attribute (no ``__getattr__`` magic, so mocks do not count)."""
    try:
        inspect.getattr_static(obj, name)
    except AttributeError:
        return False
    return True


def is_adapter(obj: Any) -> bool:
    """True for an :class:`~dag_healer.core.ports.OrchestratorAdapter` with real attributes."""
    return isinstance(obj, OrchestratorAdapter) and all(_defines(obj, name) for name in ADAPTER_MEMBERS)


def is_legacy_client(obj: Any) -> bool:
    """True for an Airflow REST client or a test double of one (duck typed)."""
    return any(callable(getattr(obj, name, None)) for name in LEGACY_CLIENT_METHODS)


def as_adapter(cfg: HealerConfig, obj: Any | None) -> OrchestratorAdapter | None:
    """OrchestratorAdapter -> itself; a legacy Airflow client (duck type with get_task_log /
    get_dag / get_dag_source, e.g. the phase-5 FakeAirflowClient) -> AirflowAdapter(cfg, client=obj);
    None -> None (offline gathering, today's behaviour).

    Raises :class:`TypeError` for any other object and
    :class:`~dag_healer.orchestrators.registry.AdapterLoadError` when the Airflow adapter cannot
    be loaded. Wrapping does no I/O.
    """
    if obj is None:
        return None
    if is_adapter(obj):
        return obj
    if is_legacy_client(obj):
        factory = load_factory(LEGACY_CLIENT_FACTORY)
        return ensure_adapter(factory.create(cfg, client=obj), LEGACY_CLIENT_FACTORY)
    raise TypeError(
        f"expected an orchestrator adapter, an Airflow REST client or None, got {type(obj).__name__}"
    )


__all__ = [
    "ADAPTER_MEMBERS",
    "LEGACY_CLIENT_FACTORY",
    "LEGACY_CLIENT_METHODS",
    "as_adapter",
    "is_adapter",
    "is_legacy_client",
]
