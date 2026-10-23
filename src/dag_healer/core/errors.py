"""Errors raised by orchestrator adapters (stdlib only).

Adapters translate their transport errors (``httpx`` errors, ``AirflowAPIError``,
``botocore.exceptions.ClientError`` ...) into these classes, so the core never needs to know
the transport. ``transient=True`` means "try again later": the worker keeps the state as it
is and retries on its next cycle (throttling, timeouts, 5xx answers).
"""

from __future__ import annotations

from typing import Any


class OrchestratorError(RuntimeError):
    """An orchestrator call failed. ``transient`` -> the worker retries later (no state change)."""

    def __init__(self, detail: str = "", *, transient: bool = False, status_code: int | None = None) -> None:
        self.detail = detail
        self.transient = transient
        self.status_code = status_code
        super().__init__(detail or self._default_message())

    def _default_message(self) -> str:
        """Message used when no ``detail`` is given."""
        if self.status_code is not None:
            return f"{type(self).__name__} (status {self.status_code})"
        return type(self).__name__

    def __reduce__(self) -> tuple[Any, ...]:
        """Pickle with the keyword-only attributes (exceptions cross process boundaries)."""
        return (_rebuild, (type(self), self.detail, self.transient, self.status_code))


def _rebuild(
    cls: type[OrchestratorError], detail: str, transient: bool, status_code: int | None
) -> OrchestratorError:
    return cls(detail, transient=transient, status_code=status_code)


class NotSupported(OrchestratorError):
    """The capability is missing (e.g. redrive on an Express execution)."""


class NotFound(OrchestratorError):
    """The pipeline, run or attempt does not exist (or no longer exists)."""


class PermissionDenied(OrchestratorError):
    """The orchestrator refused the call for lack of permissions."""


__all__ = ["NotFound", "NotSupported", "OrchestratorError", "PermissionDenied"]
