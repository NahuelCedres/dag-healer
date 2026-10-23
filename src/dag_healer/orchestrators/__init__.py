"""Orchestrator adapters: the registry, the legacy-client bridge, the fake and the conformance suite.

* :mod:`~dag_healer.orchestrators.registry` resolves ``orchestrator.type`` from ``healer.yaml``
  to an :class:`~dag_healer.orchestrators.registry.AdapterFactory` registered in the
  ``dag_healer.orchestrators`` entry-point group (or named as ``"pkg.mod:factory"``) and
  caches one adapter per configuration (:func:`get_adapter`);
* :mod:`~dag_healer.orchestrators.compat` turns the phase-5 ``client=`` arguments (an
  Airflow REST client or a test double of it) into an adapter (:func:`as_adapter`);
* :mod:`~dag_healer.orchestrators.fake` is an in-memory orchestrator ("Fakeflow") for tests
  and the Docker-free demo;
* :mod:`~dag_healer.orchestrators.conformance` is the packaged pytest suite every adapter
  must pass (third-party adapters included).

This package module is light on purpose (the core imports it in every process): it imports
nothing at import time and resolves the public names lazily. See section 3.8 of
``docs/architecture/orchestrators.md``.
"""

from __future__ import annotations

import importlib
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from dag_healer.orchestrators.compat import as_adapter
    from dag_healer.orchestrators.registry import (
        ENTRY_POINT_GROUP,
        AdapterFactory,
        AdapterLoadError,
        AdapterOptionsError,
        OrchestratorSettings,
        Registration,
        available_types,
        get_adapter,
        load_factory,
        orchestrator_settings,
        parse_options,
        registrations,
        reset_adapters,
    )

_REGISTRY = "dag_healer.orchestrators.registry"
_LAZY: dict[str, str] = {
    "ENTRY_POINT_GROUP": _REGISTRY,
    "AdapterFactory": _REGISTRY,
    "AdapterLoadError": _REGISTRY,
    "AdapterOptionsError": _REGISTRY,
    "OrchestratorSettings": _REGISTRY,
    "Registration": _REGISTRY,
    "available_types": _REGISTRY,
    "get_adapter": _REGISTRY,
    "load_factory": _REGISTRY,
    "orchestrator_settings": _REGISTRY,
    "parse_options": _REGISTRY,
    "registrations": _REGISTRY,
    "reset_adapters": _REGISTRY,
    "as_adapter": "dag_healer.orchestrators.compat",
}

__all__ = [
    "ENTRY_POINT_GROUP",
    "AdapterFactory",
    "AdapterLoadError",
    "AdapterOptionsError",
    "OrchestratorSettings",
    "Registration",
    "as_adapter",
    "available_types",
    "get_adapter",
    "load_factory",
    "orchestrator_settings",
    "parse_options",
    "registrations",
    "reset_adapters",
]


def __getattr__(name: str) -> Any:
    """Resolve the public names of the registry and compat modules on first use."""
    module_name = _LAZY.get(name)
    if module_name is None:
        raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
    value = getattr(importlib.import_module(module_name), name)
    globals()[name] = value
    return value


def __dir__() -> list[str]:
    return sorted({*globals(), *__all__})
