"""The ``dev`` backend: every request is the user ``dev`` in group ``admin``. Local development only.

It reads no header and checks no credential, so whoever can reach the port is an admin. The
backend therefore refuses to be built for a bind address that is not loopback
(:func:`is_loopback_host`): ``dag-healer serve`` passes the effective host (``--host``, else
``serve.host``) and ``create_standalone_app`` passes its ``bind_host`` or ``serve.host``, so every
path that builds it enforces the check. Never expose it through a port forward or a proxy: the
host also refuses requests with forwarding headers, but a TCP port forward adds none.
Without ``serve.auth.rules`` the host gives ``group:admin`` every action, so the dev user may do
everything.
"""

from __future__ import annotations

import ipaddress
from collections.abc import Callable, Sequence
from typing import TYPE_CHECKING, Any

from dag_healer.api.auth import Principal
from dag_healer.hosting.auth.base import AuthConfigError, StandaloneAuthBackend

if TYPE_CHECKING:
    from dag_healer.api.auth import Authorizer

#: The user of every request served by the dev backend.
DEV_PRINCIPAL = Principal(id="dev", name="dev", groups=frozenset({"admin"}))
#: Why the dev backend refuses a bind address (the address follows).
DEV_LOOPBACK_ONLY = (
    "the dev backend serves every request as the user 'dev' (group admin): bind it to a loopback "
    "address such as 127.0.0.1"
)


def is_loopback_host(host: str) -> bool:
    """True when binding ``host`` only accepts connections from this machine.

    That is ``localhost`` (any case) and the literal loopback addresses: ``127.0.0.0/8``, ``::1``
    and the IPv4-mapped ``::ffff:127.x.y.z``, with or without IPv6 brackets. False for ``""``,
    ``0.0.0.0``, ``::``, every other host name (it could resolve anywhere) and anything that does
    not parse.
    """
    if not isinstance(host, str):
        return False
    value = host.strip()
    if value.lower() == "localhost":
        return True
    if value.startswith("[") and value.endswith("]"):
        value = value[1:-1]
    try:
        address = ipaddress.ip_address(value)
    except ValueError:
        return False
    if isinstance(address, ipaddress.IPv6Address) and address.ipv4_mapped is not None:
        address = address.ipv4_mapped
    return address.is_loopback


async def _dev_user() -> Principal:
    """Every request is the dev user; no header is read."""
    return DEV_PRINCIPAL


class DevAuthBackend(StandaloneAuthBackend):
    """``serve.auth.backend: dev``: the fixed user ``dev`` (group ``admin``), loopback binds only.

    :class:`~dag_healer.hosting.auth.base.AuthConfigError` when ``bind_host`` is not a loopback
    address (:func:`is_loopback_host`).
    """

    name = "dev"

    def __init__(self, *, bind_host: str, authorizer: Authorizer, origins: Sequence[str] = ()) -> None:
        if not is_loopback_host(bind_host):
            raise AuthConfigError(f"{DEV_LOOPBACK_ONLY}, not {bind_host!r}")
        super().__init__(authorizer=authorizer, origins=origins)
        self.bind_host = bind_host

    def user_dependency(self) -> Callable[..., Any]:
        """A dependency that returns :data:`DEV_PRINCIPAL` for every request."""
        return _dev_user


__all__ = ["DEV_LOOPBACK_ONLY", "DEV_PRINCIPAL", "DevAuthBackend", "is_loopback_host"]
