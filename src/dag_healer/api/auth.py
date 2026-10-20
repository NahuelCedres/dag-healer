"""Airflow authentication and authorization for the dag-healer API.

This is the ONLY module of the API that imports Airflow. Airflow mounts plugin
sub-applications without authenticating them, so every ``/api/v1`` route declares
one of the dependencies below:

* :func:`get_current_user` wraps ``airflow.api_fastapi.core_api.security.get_user``
  (``GetUserDep``): it reads ``Authorization: Bearer <jwt>`` and then the ``_token``
  cookie of the Airflow UI and resolves the user through the ``get_auth_manager()``
  singleton (``request.app.state`` is empty in a mounted sub-app, so
  ``AuthManagerDep`` / ``auth_manager_from_app`` must not be used). Airflow answers 401
  without credentials or with an expired token and 403 for an invalid token.
* :func:`require_authenticated` only requires a valid user (``requires_authenticated``).
* :func:`require_dag_access` builds on ``requires_access_dag(method, DagAccessEntity.X)``,
  which reads ``dag_id`` from the PATH parameters (hence every DAG-scoped route lives
  under ``/dags/{dag_id}/...``), resolves the DAG team and raises 403 "Forbidden".
* :class:`Authorizer` (dependency :func:`get_authorizer`) answers the list-filtering and
  UI-hint questions: :meth:`Authorizer.authorized_dag_ids`, :meth:`Authorizer.can_read_dag`
  and :meth:`Authorizer.can_edit_dag`.

Indirection for tests: route code only depends on :func:`get_current_user` and
:func:`get_authorizer` (``require_dag_access`` dependencies use both), so tests on hosts
without Airflow override exactly these two with ``app.dependency_overrides``. Without
Airflow and without overrides every dependency fails closed with 503.
"""

from __future__ import annotations

import functools
import importlib.util
import inspect
import logging
import os
from collections.abc import Callable
from types import ModuleType
from typing import Any, Literal, Protocol, runtime_checkable

from fastapi import Depends, HTTPException, Request, status
from starlette.concurrency import run_in_threadpool

log = logging.getLogger(__name__)

Method = Literal["GET", "PUT"]
#: ``DagAccessEntity`` member names accepted by :func:`require_dag_access`.
DAG_ACCESS_ENTITIES: frozenset[str] = frozenset(
    {
        "AUDIT_LOG",
        "CODE",
        "DEPENDENCIES",
        "HITL_DETAIL",
        "RUN",
        "TASK",
        "TASK_INSTANCE",
        "TASK_LOGS",
        "VERSION",
        "WARNING",
        "XCOM",
    }
)
_METHODS: frozenset[str] = frozenset({"GET", "POST", "PUT", "DELETE"})
UNAVAILABLE_DETAIL = "Airflow authentication is not available in this process"


def airflow_installed() -> bool:
    """True when a real ``airflow`` package (with ``__init__.py``) is importable.

    A bare ``airflow/`` directory on ``sys.path`` (for example this project's
    ``tests/airflow`` folder when pytest puts ``tests/`` on the path) is only a namespace
    package and does not count.
    """
    try:
        spec = importlib.util.find_spec("airflow")
    except (ImportError, ValueError):
        return False
    return spec is not None and spec.origin not in (None, "namespace")


def _is_missing_airflow(exc: ImportError) -> bool:
    """True when ``exc`` means Airflow is not installed at all (not a broken install)."""
    name = getattr(exc, "name", None) or ""
    if not (name == "airflow" or name.startswith("airflow.")):
        return False
    return not airflow_installed()


def _load_airflow_security() -> ModuleType | None:
    """Import ``airflow.api_fastapi.core_api.security``; ``None`` when Airflow is absent."""
    try:
        from airflow.api_fastapi.core_api import security
    except ImportError as exc:
        if _is_missing_airflow(exc):
            log.debug("dag-healer: Airflow is not installed; API authentication fails closed")
            return None
        raise
    return security


_security: ModuleType | None = _load_airflow_security()
AIRFLOW_AVAILABLE: bool = _security is not None


def airflow_available() -> bool:
    """True when the Airflow API security helpers could be imported."""
    return AIRFLOW_AVAILABLE


async def _unavailable_user() -> Any:
    """Stand-in for Airflow's ``get_user`` when Airflow is missing: always 503."""
    raise HTTPException(status_code=status.HTTP_503_SERVICE_UNAVAILABLE, detail=UNAVAILABLE_DETAIL)


_airflow_get_user: Callable[..., Any] = _security.get_user if _security is not None else _unavailable_user


# --------------------------------------------------------------------------- users


async def get_current_user(user: Any = Depends(_airflow_get_user)) -> Any:
    """FastAPI dependency returning the authenticated Airflow user (``GetUserDep``).

    401 without credentials or with an expired token, 403 with an invalid token.
    """
    return user


def require_authenticated(user: Any = Depends(get_current_user)) -> Any:
    """Dependency that only requires an authenticated user (``requires_authenticated``)."""
    return user


def user_name(user: Any) -> str:
    """Display name of an Airflow user (``BaseUser.get_name()``) used as command actor."""
    for getter in ("get_name", "get_id"):
        method = getattr(user, getter, None)
        if callable(method):
            try:
                value = method()
            except Exception:  # a broken user model must not break the request
                value = None
            if value:
                return str(value)
    for attribute in ("username", "name"):
        value = getattr(user, attribute, None)
        if value:
            return str(value)
    return "unknown"


# --------------------------------------------------------------------------- authorizer


@runtime_checkable
class Authorizer(Protocol):
    """Authorization questions the API asks about DAGs."""

    async def check_dag_access(self, request: Request, user: Any, method: str, entity: str | None) -> None:
        """Raise ``HTTPException(403)`` unless ``user`` may ``method`` the DAG in the path."""
        ...

    def authorized_dag_ids(self, user: Any, method: str = "GET") -> set[str]:
        """DAG ids (known to Airflow) the user may access with ``method``."""
        ...

    def can_read_dag(self, user: Any, dag_id: str) -> bool:
        """True when the user may read ``dag_id`` (also for DAGs Airflow no longer knows)."""
        ...

    def can_edit_dag(self, user: Any, dag_id: str, entity: str | None = "TASK_INSTANCE") -> bool:
        """True when the user may change ``entity`` of ``dag_id`` (``PUT``; ``None``: the DAG itself)."""
        ...


class AirflowAuthorizer:
    """:class:`Authorizer` backed by the Airflow auth manager singleton."""

    def __init__(self, security: ModuleType) -> None:
        self._security = security

    # helpers -----------------------------------------------------------------
    @staticmethod
    def _entity(entity: str | None) -> Any:
        if entity is None:
            return None
        from airflow.api_fastapi.auth.managers.models.resource_details import DagAccessEntity

        return DagAccessEntity[entity]

    @functools.lru_cache(maxsize=64)  # noqa: B019 - one instance per process
    def _dag_dependency(self, method: str, entity: str | None) -> Callable[..., Any]:
        return self._security.requires_access_dag(method, self._entity(entity))

    @staticmethod
    def _team_name(dag_id: str) -> str | None:
        from airflow.models.dag import DagModel

        return DagModel.get_team_name(dag_id)

    def _is_authorized(self, user: Any, dag_id: str, method: str, entity: str | None) -> bool:
        from airflow.api_fastapi.auth.managers.models.resource_details import DagDetails

        try:
            details = DagDetails(id=dag_id, team_name=self._team_name(dag_id))
            return bool(
                self._security.get_auth_manager().is_authorized_dag(
                    method=method, access_entity=self._entity(entity), details=details, user=user
                )
            )
        except Exception:
            log.warning("dag-healer: authorization check for DAG %r failed; denying", dag_id, exc_info=True)
            return False

    # Authorizer ----------------------------------------------------------------
    async def check_dag_access(self, request: Request, user: Any, method: str, entity: str | None) -> None:
        """Run Airflow's ``requires_access_dag(method, entity)`` check for ``user``."""
        check = self._dag_dependency(method, entity)
        if inspect.iscoroutinefunction(check):
            await check(request=request, user=user)
            return
        result = await run_in_threadpool(check, request=request, user=user)
        if inspect.isawaitable(result):  # a future Airflow may return a coroutine
            await result

    def authorized_dag_ids(self, user: Any, method: str = "GET") -> set[str]:
        """``get_auth_manager().get_authorized_dag_ids(user=..., method=...)``."""
        return set(self._security.get_auth_manager().get_authorized_dag_ids(user=user, method=method))

    def can_read_dag(self, user: Any, dag_id: str) -> bool:
        """``is_authorized_dag(method="GET", details=DagDetails(id=dag_id, ...))``."""
        return self._is_authorized(user, dag_id, "GET", None)

    def can_edit_dag(self, user: Any, dag_id: str, entity: str | None = "TASK_INSTANCE") -> bool:
        """``is_authorized_dag(method="PUT", access_entity=DagAccessEntity.<entity>, ...)``."""
        return self._is_authorized(user, dag_id, "PUT", entity)


class UnavailableAuthorizer:
    """:class:`Authorizer` used when Airflow is not installed: denies everything."""

    async def check_dag_access(self, request: Request, user: Any, method: str, entity: str | None) -> None:
        """Always 503."""
        raise HTTPException(status_code=status.HTTP_503_SERVICE_UNAVAILABLE, detail=UNAVAILABLE_DETAIL)

    def authorized_dag_ids(self, user: Any, method: str = "GET") -> set[str]:
        """No DAG."""
        return set()

    def can_read_dag(self, user: Any, dag_id: str) -> bool:
        """Never."""
        return False

    def can_edit_dag(self, user: Any, dag_id: str, entity: str | None = "TASK_INSTANCE") -> bool:
        """Never."""
        return False


_AUTHORIZER: Authorizer = AirflowAuthorizer(_security) if _security is not None else UnavailableAuthorizer()


def get_authorizer() -> Authorizer:
    """FastAPI dependency returning the process-wide :class:`Authorizer`."""
    return _AUTHORIZER


def authorized_dag_ids(user: Any, method: str = "GET") -> set[str]:
    """DAG ids the user may access (``get_auth_manager().get_authorized_dag_ids``)."""
    return _AUTHORIZER.authorized_dag_ids(user, method)


def can_read_dag(user: Any, dag_id: str) -> bool:
    """True when the user may read ``dag_id``."""
    return _AUTHORIZER.can_read_dag(user, dag_id)


def can_edit_dag(user: Any, dag_id: str, entity: str | None = "TASK_INSTANCE") -> bool:
    """True when the user may act on ``dag_id`` (``PUT`` on ``DagAccessEntity.<entity>``)."""
    return _AUTHORIZER.can_edit_dag(user, dag_id, entity)


@functools.cache
def require_dag_access(method: str, entity: str | None = "TASK_INSTANCE") -> Callable[..., Any]:
    """Dependency enforcing ``requires_access_dag(method, DagAccessEntity.<entity>)``.

    The DAG id is read from the ``dag_id`` PATH parameter by Airflow. Returns the same
    callable for the same arguments (so it can be used as a ``dependency_overrides`` key)
    and the dependency evaluates to the authenticated user.
    """
    method = method.upper()
    if method not in _METHODS:
        raise ValueError(f"unsupported method {method!r}")
    if entity is not None and entity not in DAG_ACCESS_ENTITIES:
        raise ValueError(f"unknown DagAccessEntity {entity!r}")

    async def dependency(
        request: Request,
        user: Any = Depends(get_current_user),
        authorizer: Authorizer = Depends(get_authorizer),
    ) -> Any:
        await authorizer.check_dag_access(request, user, method, entity)
        return user

    suffix = (entity or "dag").lower()
    dependency.__name__ = dependency.__qualname__ = f"require_dag_{method.lower()}_{suffix}"
    dependency.__doc__ = f"Require Airflow permission {method} on DagAccessEntity.{entity} of the path DAG."
    dependency.healer_dag_access = (method, entity)  # type: ignore[attr-defined]  # guard test marker
    return dependency


def configured_base_url() -> str | None:
    """The Airflow ``[api] base_url`` (env ``AIRFLOW__API__BASE_URL``, else Airflow config)."""
    value = os.environ.get("AIRFLOW__API__BASE_URL", "").strip()
    if value:
        return value.rstrip("/")
    if _security is None:
        return None
    try:
        from airflow.configuration import conf

        value = (conf.get("api", "base_url", fallback="") or "").strip()
    except Exception:
        return None
    return value.rstrip("/") or None


__all__ = [
    "AIRFLOW_AVAILABLE",
    "DAG_ACCESS_ENTITIES",
    "AirflowAuthorizer",
    "Authorizer",
    "UnavailableAuthorizer",
    "airflow_available",
    "airflow_installed",
    "authorized_dag_ids",
    "can_edit_dag",
    "can_read_dag",
    "configured_base_url",
    "get_authorizer",
    "get_current_user",
    "require_authenticated",
    "require_dag_access",
    "user_name",
]
