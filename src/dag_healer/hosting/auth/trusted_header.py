"""The ``trusted_header`` backend: the user (and groups) an authenticating proxy puts in headers.

For oauth2-proxy, an identity-aware proxy and the like. The headers are believed ONLY when the
direct TCP peer of the request (``request.client.host``) is inside ``serve.auth.trusted_proxies``
(IPv4 and IPv6 networks; an IPv4-mapped IPv6 peer counts as its IPv4 address; a peer that is not
an IP address is never trusted). ``dag-healer serve`` runs uvicorn with ``proxy_headers=False``,
so the peer is the socket address and ``X-Forwarded-For`` is never consulted. The networks must
name the proxies (:func:`parse_networks`): none wider than an IPv4 /8 or an IPv6 /32, alone or
merged (a unique local fc00::/7 network may be a /8, like 10.0.0.0/8), and no IPv6 network that
holds the IPv4-mapped addresses (``::/64`` would trust every IPv4 client).

A request from a trusted peer must carry the user header (``X-Forwarded-User`` by default)
exactly once, not blank, at most 256 printable characters and no comma (a proxy that appends to
a header the client sent, instead of overwriting it, would produce one). The groups header
(``X-Forwarded-Groups``; ``None`` turns it off) is optional, at most once, a comma-separated
list of at most :data:`MAX_GROUPS` names. Anything else answers 401 ``Not authenticated``; the
failure is logged at debug level with the peer address only.

This is only safe behind a proxy that overwrites or removes BOTH headers on every request it
forwards, with the port reachable from the proxy alone: anybody else who reaches a trusted
address could send them. A proxy that sets the user header only (an identity-aware proxy that
sets ``X-Goog-Authenticated-User-Email``, a forward-auth setup that maps the user alone) passes a
groups header the client sent through, so ``groups_header`` must then be ``None``
(``serve.auth.groups_header: null``); ``dag-healer serve`` warns at start while it is set.
"""

from __future__ import annotations

import ipaddress
import logging
import re
from collections.abc import Callable, Iterable, Sequence
from ipaddress import IPv4Address, IPv4Network, IPv6Address, IPv6Network
from typing import TYPE_CHECKING, Any

from fastapi import Request

from dag_healer.api.auth import Principal
from dag_healer.hosting.auth.base import (
    AuthConfigError,
    StandaloneAuthBackend,
    clean_name,
    not_authenticated,
)

if TYPE_CHECKING:
    from dag_healer.api.auth import Authorizer

log = logging.getLogger(__name__)

Network = IPv4Network | IPv6Network

DEFAULT_USER_HEADER = "X-Forwarded-User"
DEFAULT_GROUPS_HEADER = "X-Forwarded-Groups"
#: The most groups one request may name.
MAX_GROUPS = 64
#: The shortest prefix of a trusted network: wider ones hold whole providers' addresses. The
#: unique local IPv6 addresses (fc00::/7, the private ranges of IPv6) may be as wide as 10.0.0.0/8.
MIN_IPV4_PREFIX = 8
MIN_IPV6_PREFIX = 32
MIN_UNIQUE_LOCAL_PREFIX = 8
_UNIQUE_LOCAL = IPv6Network("fc00::/7")
_IPV4_MAPPED_TEXT = "::ffff:0:0/96"  # str() of the network differs between Python versions
_IPV4_MAPPED = IPv6Network(_IPV4_MAPPED_TEXT)
_WIDEST = (
    f"at most an IPv4 /{MIN_IPV4_PREFIX}, an IPv6 /{MIN_IPV6_PREFIX} or a /{MIN_UNIQUE_LOCAL_PREFIX} "
    "in fc00::/7"
)
_HEADER_NAME = re.compile(r"[!#$%&'*+.^_`|~0-9A-Za-z-]+")  # an HTTP token (RFC 9110)


def _network(value: str) -> Network:
    """One ``trusted_proxies`` item (an address or a CIDR); IPv4-mapped IPv6 becomes IPv4."""
    network = ipaddress.ip_network(value.strip(), strict=False)
    if isinstance(network, IPv6Network) and network.prefixlen >= 96:
        mapped = network.network_address.ipv4_mapped
        if mapped is not None:  # inside ::ffff:0:0/96
            return IPv4Network((mapped, network.prefixlen - 96))
    return network


def _too_wide(network: Network) -> bool:
    """True for a network with a shorter prefix than its ``MIN_*_PREFIX``."""
    if isinstance(network, IPv4Network):
        return network.prefixlen < MIN_IPV4_PREFIX
    if network.subnet_of(_UNIQUE_LOCAL):
        return network.prefixlen < MIN_UNIQUE_LOCAL_PREFIX
    return network.prefixlen < MIN_IPV6_PREFIX


def parse_networks(values: Iterable[str] | None) -> tuple[Network, ...]:
    """The ``serve.auth.trusted_proxies`` networks; :class:`AuthConfigError` when one is unusable.

    Empty is refused (nobody could be trusted, and the backend would answer 401 forever), and so
    is any setting that trusts far more addresses than proxies have, because anybody there could
    send the headers: a network holding every address (``0.0.0.0/0``, ``::/0``), an IPv6 network
    holding the IPv4-mapped addresses ``::ffff:0:0/96`` (``::/64``: every IPv4 client of a
    dual-stack socket) and networks wider than an IPv4 /8 or an IPv6 /32 (/8 inside the unique
    local fc00::/7), alone or merged with the others (``0.0.0.0/1`` with ``128.0.0.0/1``).
    """
    if values is None:
        values = ()
    elif isinstance(values, str):
        values = [values]
    networks: list[Network] = []
    for value in values:
        try:
            network = _network(value) if isinstance(value, str) else None
        except ValueError:
            network = None
        if network is None:
            raise AuthConfigError(
                f"serve.auth.trusted_proxies: {value!r} is not an IP address or a CIDR such as 10.0.0.0/8"
            )
        if network.prefixlen == 0:
            raise AuthConfigError(
                f"serve.auth.trusted_proxies: {value!r} trusts every address, so anybody could send the "
                "user header; list the addresses of the proxies that set it"
            )
        if isinstance(network, IPv6Network) and network.overlaps(_IPV4_MAPPED):
            raise AuthConfigError(
                f"serve.auth.trusted_proxies: {value!r} holds the IPv4-mapped addresses "
                f"({_IPV4_MAPPED_TEXT}), so it trusts every IPv4 client of a dual-stack socket; list "
                "IPv4 proxies by their IPv4 addresses"
            )
        if _too_wide(network):
            raise AuthConfigError(
                f"serve.auth.trusted_proxies: {value!r} trusts too many addresses ({_WIDEST}); list "
                "the addresses of the proxies that set the user header"
            )
        networks.append(network)
    if not networks:
        raise AuthConfigError(
            "the trusted_header backend requires serve.auth.trusted_proxies (the addresses or CIDRs "
            "of the proxies that set the user header)"
        )
    for version in (IPv4Network, IPv6Network):
        same_version = [item for item in networks if isinstance(item, version)]
        for merged in ipaddress.collapse_addresses(same_version):
            if _too_wide(merged):
                raise AuthConfigError(
                    f"serve.auth.trusted_proxies together trust {merged}, too many addresses "
                    f"({_WIDEST}); list the addresses of the proxies that set the user header"
                )
    return tuple(networks)


def peer_is_trusted(peer: str | None, networks: Sequence[IPv4Network | IPv6Network]) -> bool:
    """True when ``peer`` (the socket address of the client) is inside one of ``networks``.

    An IPv4-mapped IPv6 peer is also checked as its IPv4 address. A missing peer or one that is
    not an IP address (a Unix socket, a test client name) is never trusted.
    """
    if not peer:
        return False
    try:
        address = ipaddress.ip_address(peer)
    except ValueError:
        return False
    candidates: list[IPv4Address | IPv6Address] = [address]
    if isinstance(address, IPv6Address) and address.ipv4_mapped is not None:
        candidates.append(address.ipv4_mapped)
    return any(candidate in network for candidate in candidates for network in networks)


def _header_name(field: str, value: Any) -> str:
    if not isinstance(value, str) or not _HEADER_NAME.fullmatch(value.strip()):
        raise AuthConfigError(f"serve.auth.{field}: {value!r} is not an HTTP header name")
    return value.strip()


class TrustedHeaderAuthBackend(StandaloneAuthBackend):
    """``serve.auth.backend: trusted_header``: identity headers from the listed proxies only.

    :class:`~dag_healer.hosting.auth.base.AuthConfigError` when ``trusted_proxies`` is empty, holds
    something that is not an address or a CIDR or trusts too many addresses
    (:func:`parse_networks`), or when a header name is invalid.
    """

    name = "trusted_header"

    def __init__(
        self,
        *,
        trusted_proxies: Sequence[str],
        user_header: str = DEFAULT_USER_HEADER,
        groups_header: str | None = DEFAULT_GROUPS_HEADER,
        authorizer: Authorizer,
        origins: Sequence[str] = (),
    ) -> None:
        super().__init__(authorizer=authorizer, origins=origins)
        self._networks = parse_networks(trusted_proxies)
        self._user_header = _header_name("user_header", user_header)
        self._groups_header = None if groups_header is None else _header_name("groups_header", groups_header)
        if self._groups_header is not None and self._groups_header.lower() == self._user_header.lower():
            raise AuthConfigError("serve.auth.groups_header must differ from serve.auth.user_header")

    @property
    def networks(self) -> tuple[Network, ...]:
        """The trusted proxy networks."""
        return self._networks

    def user_dependency(self) -> Callable[..., Any]:
        """A dependency that reads the identity headers of requests from a trusted proxy."""
        return self._current_user

    async def _current_user(self, request: Request) -> Principal:
        peer = request.client.host if request.client is not None else None
        if not peer_is_trusted(peer, self._networks):
            log.debug("dag-healer: identity headers ignored: the peer %r is not a trusted proxy", peer)
            raise not_authenticated()
        users = request.headers.getlist(self._user_header)
        user = clean_name(users[0]) if len(users) == 1 else None
        if user is None or "," in user:  # a comma: a proxy appended to a header the client sent
            log.debug("dag-healer: no usable %s header in a request from %r", self._user_header, peer)
            raise not_authenticated()
        groups = self._groups(request)
        if groups is None:
            log.debug("dag-healer: unusable %s header in a request from %r", self._groups_header, peer)
            raise not_authenticated()
        return Principal(id=user, name=user, groups=groups)

    def _groups(self, request: Request) -> frozenset[str] | None:
        """The groups of the request (none without the header); ``None`` when they are unusable."""
        if self._groups_header is None:
            return frozenset()
        values = request.headers.getlist(self._groups_header)
        if not values:
            return frozenset()
        if len(values) > 1:
            return None
        items = [item for item in (part.strip() for part in values[0].split(",")) if item]
        if len(items) > MAX_GROUPS:
            return None
        groups = [clean_name(item) for item in items]
        if any(group is None for group in groups):
            return None
        return frozenset(group for group in groups if group is not None)


__all__ = [
    "DEFAULT_GROUPS_HEADER",
    "DEFAULT_USER_HEADER",
    "MAX_GROUPS",
    "MIN_IPV4_PREFIX",
    "MIN_IPV6_PREFIX",
    "MIN_UNIQUE_LOCAL_PREFIX",
    "TrustedHeaderAuthBackend",
    "parse_networks",
    "peer_is_trusted",
]
