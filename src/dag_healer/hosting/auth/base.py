"""What the authentication backends of the standalone host share.

* :class:`StandaloneAuthBackend` holds the host's :class:`~dag_healer.api.auth.Authorizer` (the
  RBAC rules of ``serve.auth.rules``, built by the host and handed in) and the CSRF origins;
  each backend adds its own user dependency.
* Every backend answers missing, malformed, duplicated, unknown or expired credentials with the
  same 401 ``{"detail": "Not authenticated"}`` (:func:`not_authenticated`), so a client never
  learns which check failed; the token backend adds ``WWW-Authenticate: Bearer``. Failures are
  logged at debug level without header values, tokens or claims.
* :class:`AuthConfigError` is raised when a backend is built, so ``dag-healer serve`` stops
  before it binds a socket when ``serve.auth`` cannot be served safely. Its message is shown to
  the user and never holds a secret.
"""

from __future__ import annotations

import os
from collections.abc import Callable, Sequence
from typing import TYPE_CHECKING, Any

from fastapi import HTTPException, status

from dag_healer.config import ConfigError

if TYPE_CHECKING:
    from dag_healer.api.auth import Authorizer
    from dag_healer.config import HealerConfig

#: The environment variable that overrides ``serve.public_url`` (also read by ``links.py``).
PUBLIC_URL_ENV = "HEALER_PUBLIC_URL"
#: The ``detail`` of every 401 of the standalone host (Airflow's text for missing credentials).
NOT_AUTHENTICATED = "Not authenticated"
#: The longest user or group name a backend takes from a request header or a claim.
MAX_NAME_LENGTH = 256


class AuthConfigError(ConfigError):
    """``serve.auth`` cannot be served safely; raised when the backend is built (``serve`` exits 1)."""


def not_authenticated(*, bearer: bool = False) -> HTTPException:
    """The 401 every backend raises; ``bearer`` adds ``WWW-Authenticate: Bearer`` (token backend)."""
    headers = {"WWW-Authenticate": "Bearer"} if bearer else None
    return HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail=NOT_AUTHENTICATED, headers=headers)


def standalone_origins(cfg: HealerConfig) -> list[str]:
    """The URLs the CSRF guard of the standalone host accepts besides the request's own origin.

    ``$HEALER_PUBLIC_URL`` then ``serve.public_url``, stripped, blank ones left out, each once.
    ``AIRFLOW__API__BASE_URL`` is not consulted: it names the Airflow host.
    """
    origins: list[str] = []
    for value in (os.environ.get(PUBLIC_URL_ENV), cfg.serve.public_url):
        url = (value or "").strip()
        if url and url not in origins:
            origins.append(url)
    return origins


def clean_name(value: Any) -> str | None:
    """A user or group name from a header or a claim, stripped; ``None`` when it is unusable.

    Usable: a string that is not blank, has at most :data:`MAX_NAME_LENGTH` characters and holds
    printable characters only (no control characters, so it is safe in logs and the store).
    """
    if not isinstance(value, str):
        return None
    name = value.strip()
    if not name or len(name) > MAX_NAME_LENGTH or not name.isprintable():
        return None
    return name


class StandaloneAuthBackend:
    """Base of the standalone :class:`~dag_healer.api.auth.AuthBackend` implementations.

    It keeps the host's authorizer, returned by :meth:`authorizer` on every call (no I/O), and
    the CSRF origins of :meth:`allowed_origins`. Subclasses set ``name`` (the
    ``serve.auth.backend`` value) and implement :meth:`user_dependency`.
    """

    name: str

    def __init__(self, *, authorizer: Authorizer, origins: Sequence[str] = ()) -> None:
        if authorizer is None:
            raise TypeError("an Authorizer is required (the host's RuleAuthorizer)")
        if isinstance(origins, str):
            raise TypeError("origins must be a sequence of URLs, not a string")
        self._authorizer = authorizer
        self._origins = tuple(origins)

    def user_dependency(self) -> Callable[..., Any]:
        """The FastAPI dependency returning the request's :class:`~dag_healer.api.auth.Principal`."""
        raise NotImplementedError

    def authorizer(self) -> Authorizer:
        """The authorizer the host handed in (the same object on every call)."""
        return self._authorizer

    def allowed_origins(self) -> list[str]:
        """The CSRF origins given at construction (:func:`standalone_origins` for ``serve``)."""
        return list(self._origins)


__all__ = [
    "MAX_NAME_LENGTH",
    "NOT_AUTHENTICATED",
    "PUBLIC_URL_ENV",
    "AuthConfigError",
    "StandaloneAuthBackend",
    "clean_name",
    "not_authenticated",
    "standalone_origins",
]
