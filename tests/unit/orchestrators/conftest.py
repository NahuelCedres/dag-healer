"""Fixtures of the orchestrator registry, compat, fake and conformance tests."""

from __future__ import annotations

from collections.abc import Callable, Iterator
from pathlib import Path
from typing import Any

import pytest
from pydantic import BaseModel, ConfigDict, Field

from dag_healer.config import HealerConfig
from dag_healer.orchestrators.registry import reset_adapters

MakeConfig = Callable[..., HealerConfig]


class OrchestratorSection(BaseModel):
    """A permissive ``orchestrator:`` section shaped like ``config.OrchestratorConfig`` (section 5.1).

    The registry reads the section by duck typing (``type``, ``instance_id``/``instance``,
    ``options``). Using this stand-in keeps these tests about the registry and the factories:
    whatever load-time validation ``config.py`` applies to the real section (unknown types,
    options, secrets) never runs here.
    """

    model_config = ConfigDict(extra="forbid")

    type: str = "airflow"
    instance: str | None = None
    options: dict[str, Any] = Field(default_factory=dict)

    @property
    def instance_id(self) -> str:
        return self.instance or self.type


class ConfigWithOrchestrator(HealerConfig):
    """A HealerConfig whose ``orchestrator:`` section is the permissive stand-in."""

    orchestrator: OrchestratorSection = Field(default_factory=OrchestratorSection)  # type: ignore[assignment]


@pytest.fixture(autouse=True)
def _fresh_registry() -> Iterator[None]:
    """Every test starts and ends with empty registry caches (monkeypatched tables included)."""
    reset_adapters()
    yield
    reset_adapters()


@pytest.fixture
def make_config(tmp_path: Path) -> MakeConfig:
    """``make_config({"type": "fake", "options": {...}}, repo=..., source_path=...)``."""

    def _make(
        orchestrator: dict[str, Any] | None = None,
        *,
        repo: Path | None = None,
        source_path: Path | None = None,
        **sections: Any,
    ) -> HealerConfig:
        data: dict[str, Any] = {"repo": {"path": str(repo or tmp_path)}, **sections}
        if orchestrator is not None:
            data["orchestrator"] = orchestrator
        cfg = ConfigWithOrchestrator.model_validate(data)
        cfg.source_path = source_path
        return cfg

    return _make
