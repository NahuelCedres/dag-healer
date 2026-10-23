"""Airflow error names for the policy, and Airflow REST errors translated into core errors.

* :data:`AIRFLOW_TAXONOMY` classifies the Airflow exception names that mean "the platform
  failed, not the task's code or data". They are exactly the Airflow names of
  ``agent.policy.INFRA_TYPES`` (which keeps them too), so the deterministic policy takes the
  same decisions whether it reads the taxonomy first or not (section 3.9 of
  ``docs/architecture/orchestrators.md``).
* :func:`translate_error` turns what an Airflow client call raised
  (:class:`~dag_healer.airflow.client.AirflowAPIError`, an ``httpx`` error, or anything a test
  double raises) into the :mod:`dag_healer.core.errors` classes the core understands:
  ``404`` -> :class:`~dag_healer.core.errors.NotFound`, ``401``/``403`` ->
  :class:`~dag_healer.core.errors.PermissionDenied`, anything else ->
  :class:`~dag_healer.core.errors.OrchestratorError` with ``transient`` set for a network
  failure (no status), ``429`` and ``5xx`` answers (:func:`is_transient_status`).

The module imports only the standard library and :mod:`dag_healer.core` at import time; the
client and httpx classes are looked up only when an error is translated.
"""

from __future__ import annotations

import sys

from dag_healer.core.errors import NotFound, OrchestratorError, PermissionDenied
from dag_healer.core.vocab import ErrorTaxonomy

#: Airflow exceptions raised by the platform itself (timeouts, killed or lost task processes).
AIRFLOW_INFRA_ERRORS: frozenset[str] = frozenset(
    {
        "AirflowTaskTimeout",
        "AirflowTaskTerminated",
        "AirflowZombieTaskException",
        "TaskDeferralTimeout",
    }
)

AIRFLOW_TAXONOMY = ErrorTaxonomy(infra=AIRFLOW_INFRA_ERRORS)
"""The error taxonomy of the Airflow adapter (the Airflow part of ``policy.INFRA_TYPES``)."""

_CLIENT_MODULE = "dag_healer.airflow.client"


def is_transient_status(status_code: int | None) -> bool:
    """True when a call that failed with ``status_code`` is worth retrying later.

    ``None`` (a network error or a timeout: no answer at all), ``429`` (rate limited) and
    every ``5xx`` answer are transient; any other status means the request itself is wrong
    and repeating it would fail the same way.
    """
    return status_code is None or status_code == 429 or status_code >= 500


def _airflow_api_error(exc: BaseException) -> bool:
    """True for :class:`~dag_healer.airflow.client.AirflowAPIError` (without importing httpx)."""
    module = sys.modules.get(_CLIENT_MODULE)
    error_class = getattr(module, "AirflowAPIError", None) if module is not None else None
    return isinstance(error_class, type) and isinstance(exc, error_class)


def status_code_of(exc: BaseException) -> int | None:
    """The HTTP status an exception carries (``None`` for transport errors and plain exceptions).

    Reads ``status_code`` (:class:`~dag_healer.airflow.client.AirflowAPIError`, core errors)
    or ``response.status_code`` (``httpx.HTTPStatusError``).
    """
    for holder in (exc, getattr(exc, "response", None)):
        status = getattr(holder, "status_code", None)
        if isinstance(status, int) and not isinstance(status, bool):
            return status
    return None


def error_text(exc: BaseException) -> str:
    """``Type: message`` of an exception (``Type`` alone without a message); RAW, not masked."""
    message = str(exc).strip()
    return f"{type(exc).__name__}: {message}" if message else type(exc).__name__


def translate_error(exc: BaseException) -> OrchestratorError:
    """The core error for ``exc`` (an :class:`OrchestratorError` is returned unchanged).

    The detail is the client's own message for an ``AirflowAPIError`` (``"Airflow API error
    503: Service Unavailable"``, ``"Airflow API request failed: GET ...: ConnectError: ..."``)
    and ``Type: message`` otherwise; it is RAW (the core masks before storing or logging it).
    The caller raises the result ``from exc`` so the original stays chained.
    """
    if isinstance(exc, OrchestratorError):
        return exc
    status = status_code_of(exc)
    detail = str(exc).strip() if _airflow_api_error(exc) else error_text(exc)
    detail = detail or error_text(exc)
    if status == 404:
        return NotFound(detail, transient=False, status_code=status)
    if status in (401, 403):
        return PermissionDenied(detail, transient=False, status_code=status)
    return OrchestratorError(detail, transient=is_transient_status(status), status_code=status)


__all__ = [
    "AIRFLOW_INFRA_ERRORS",
    "AIRFLOW_TAXONOMY",
    "error_text",
    "is_transient_status",
    "status_code_of",
    "translate_error",
]
