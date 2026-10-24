"""Authentication backends of the standalone host ``dag-healer serve``.

Section 7.3 of ``docs/architecture/orchestrators.md``. Each backend implements
:class:`dag_healer.api.auth.AuthBackend` and authenticates requests as a
:class:`dag_healer.api.auth.Principal`; ``serve.auth.backend`` picks one:

* ``dev`` (:mod:`~dag_healer.hosting.auth.dev`): every request is the user ``dev`` in group
  ``admin``. Local development only: it refuses a bind address that is not loopback.
* ``token`` (:mod:`~dag_healer.hosting.auth.token`): ``Authorization: Bearer <token>`` for the
  CLI, bots and the standalone UI. ``serve.auth.tokens`` lists the sha256 hex digest of each
  token, compared in constant time; the principal is the entry's name with its groups.
* ``trusted_header`` (:mod:`~dag_healer.hosting.auth.trusted_header`): the user and groups
  headers of an authenticating proxy, believed only when the socket peer is inside
  ``serve.auth.trusted_proxies`` (``X-Forwarded-For`` is never consulted).
* ``alb_oidc`` (:mod:`~dag_healer.hosting.auth.alb_oidc`): the ``x-amzn-oidc-data`` JWT an AWS
  Application Load Balancer signs (ES256 with the regional key, ``signer == serve.auth.alb_arn``,
  the header ``exp`` enforced); the user is ``user_claim`` (an ``email`` only when the identity
  provider verified it), else ``sub``, the groups ``groups_claim``.

Bad credentials of any kind get the same 401 ``{"detail": "Not authenticated"}``, and nothing a
client sent is logged. :func:`build_auth_backend` builds the configured backend with the host's
authorizer (the :class:`~dag_healer.api.auth.Authorizer` of ``serve.auth.rules``) and the CSRF
origins of :func:`~dag_healer.hosting.auth.base.standalone_origins`. It does no I/O and imports
only the chosen backend's module, so PyJWT is loaded for ``alb_oidc`` alone and importing this
package stays light. Settings that cannot be served safely raise :class:`AuthConfigError`
before ``serve`` binds a socket. The backends never import the host modules (``hosting.app``,
``hosting.rbac``, ``hosting.shell``): the authorizer is handed in.
"""

from __future__ import annotations

from typing import TYPE_CHECKING

from dag_healer.hosting.auth.base import (
    NOT_AUTHENTICATED,
    AuthConfigError,
    StandaloneAuthBackend,
    not_authenticated,
    standalone_origins,
)

if TYPE_CHECKING:
    from dag_healer.api.auth import AuthBackend, Authorizer
    from dag_healer.config import HealerConfig

#: The ``serve.auth.backend`` values (the same as ``dag_healer.config.ServeAuthBackend``).
BACKEND_NAMES: tuple[str, ...] = ("dev", "token", "trusted_header", "alb_oidc")


def build_auth_backend(
    cfg: HealerConfig, *, authorizer: Authorizer, bind_host: str | None = None
) -> AuthBackend:
    """The :class:`~dag_healer.api.auth.AuthBackend` that ``cfg.serve.auth.backend`` names.

    It is built from ``cfg.serve.auth`` with ``authorizer`` (the host's ``RuleAuthorizer``) and
    the origins of :func:`standalone_origins`. ``bind_host`` (default ``cfg.serve.host``) is the
    address ``serve`` binds, which the ``dev`` backend requires to be loopback. No I/O.
    :class:`AuthConfigError` when the settings cannot be served safely (``dev`` on a public
    address, ``token`` without tokens, ``trusted_header`` without ``trusted_proxies`` or with
    networks that are too wide, ``alb_oidc`` without ``alb_arn`` and ``region``, in a partition
    whose key endpoint is unknown or without PyJWT[crypto], an unknown name).
    """
    auth = cfg.serve.auth
    backend = auth.backend
    origins = standalone_origins(cfg)
    if backend == "dev":
        from dag_healer.hosting.auth.dev import DevAuthBackend

        host = cfg.serve.host if bind_host is None else bind_host
        return DevAuthBackend(bind_host=host, authorizer=authorizer, origins=origins)
    if backend == "token":
        from dag_healer.hosting.auth.token import TokenAuthBackend

        return TokenAuthBackend(auth.tokens, authorizer=authorizer, origins=origins)
    if backend == "trusted_header":
        from dag_healer.hosting.auth.trusted_header import TrustedHeaderAuthBackend

        return TrustedHeaderAuthBackend(
            trusted_proxies=auth.trusted_proxies,
            user_header=auth.user_header,
            groups_header=auth.groups_header,
            authorizer=authorizer,
            origins=origins,
        )
    if backend == "alb_oidc":
        from dag_healer.hosting.auth.alb_oidc import AlbOidcAuthBackend

        return AlbOidcAuthBackend(
            alb_arn=auth.alb_arn or "",
            region=auth.region or "",
            user_claim=auth.user_claim,
            groups_claim=auth.groups_claim,
            authorizer=authorizer,
            origins=origins,
        )
    raise AuthConfigError(
        f"unknown serve.auth.backend {backend!r}: expected one of {', '.join(BACKEND_NAMES)}"
    )


__all__ = [
    "BACKEND_NAMES",
    "NOT_AUTHENTICATED",
    "AuthConfigError",
    "StandaloneAuthBackend",
    "build_auth_backend",
    "not_authenticated",
    "standalone_origins",
]
