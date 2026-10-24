"""The resolver protocol shared by the IaC resolvers (SAM in v1)."""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any, Protocol, runtime_checkable

if TYPE_CHECKING:
    from dag_healer.stepfunctions.asl import TaskTarget


@dataclass(frozen=True)
class ResolvedFunction:
    """A Lambda function as the template describes it (repo-relative posix paths)."""

    logical_id: str | None
    function_name: str | None = None  # literal FunctionName (deployed names are usually generated)
    code_uri: str | None = None  # code root directory; "" is the repository root
    handler: str | None = None  # "app.handler"
    runtime: str | None = None  # "python3.11"
    layers: tuple[str, ...] = field(default_factory=tuple)  # code roots of local layers (/opt/python)
    log_group: str | None = None  # literal LoggingConfig.LogGroup

    @property
    def is_python(self) -> bool:
        return bool(self.runtime and self.runtime.startswith("python"))


@dataclass(frozen=True)
class ResolvedStateMachine:
    """A state machine resource of a template."""

    logical_id: str
    template_file: str  # repo-relative template path
    name: str | None = None  # literal Name
    definition_file: str | None = None  # repo-relative DefinitionUri
    definition: Mapping[str, Any] | None = None  # the definition, substitutions applied when literal
    substitutions: Mapping[str, Any] = field(default_factory=dict)  # raw DefinitionSubstitutions


@runtime_checkable
class IacResolver(Protocol):
    """Reads one IaC project (no network I/O, never executes user code)."""

    def state_machines(self) -> list[ResolvedStateMachine]:
        """Every state machine resource."""
        ...

    def functions(self) -> list[ResolvedFunction]:
        """Every Lambda function resource."""
        ...

    def function_for(self, machine: ResolvedStateMachine, target: TaskTarget) -> ResolvedFunction | None:
        """The function a Task target of ``machine`` invokes (``${X}`` substitutions, ``!GetAtt
        Fn.Arn``, ``!Ref Fn``, literal names/ARNs), or ``None``."""
        ...


__all__ = ["IacResolver", "ResolvedFunction", "ResolvedStateMachine"]
