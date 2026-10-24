"""The ``token`` backend: ``Authorization: Bearer <token>`` for the CLI, bots and the standalone UI.

``serve.auth.tokens`` lists ``{name, sha256, groups}`` entries: only the sha256 hex digest of a
token is configured (:func:`token_sha256`) and only digests are kept. A request is accepted
when:

* it carries exactly one ``Authorization`` header, with the ``Bearer`` scheme (any case), one
  or more spaces and a token of 1 to :data:`MAX_TOKEN_LENGTH` visible ASCII characters;
* the sha256 digest of that token equals a configured one. The digest is compared with every
  configured digest through :func:`hmac.compare_digest`, without an early exit, so the time
  taken does not depend on which entry (if any) matches.

The principal is the entry's ``name`` (id and name) with its ``groups``; two entries may share
a name, which is how a token is rotated. Nothing else is read: no cookie, no query parameter.
Any failure answers 401 ``Not authenticated`` with ``WWW-Authenticate: Bearer``; neither the
token nor its digest is ever logged or put in an error. A browser cannot send the header by
itself, so the standalone UI asks for the token and adds the header (sessionStorage only).

A token must be a random secret, such as the output of
``python -c "import secrets; print(secrets.token_urlsafe(32))"``: its sha256 is fast and unsalted,
so a short or guessable token could be found by trying candidates against the server or against
a leaked ``healer.yaml``. The entries are read once, when the backend is built: removing one
from ``healer.yaml`` revokes its token when ``dag-healer serve`` restarts.
"""

from __future__ import annotations

import hashlib
import hmac
import logging
import re
from collections.abc import Callable, Iterable, Mapping, Sequence
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any

from fastapi import Request

from dag_healer.api.auth import Principal
from dag_healer.hosting.auth.base import AuthConfigError, StandaloneAuthBackend, not_authenticated

if TYPE_CHECKING:
    from dag_healer.api.auth import Authorizer

log = logging.getLogger(__name__)

#: The longest bearer token accepted (longer ones are rejected before they are hashed).
MAX_TOKEN_LENGTH = 4096
AUTHORIZATION_HEADER = "authorization"
BEARER_SCHEME = "bearer"
_TOKEN_CHARS = re.compile(r"[!-~]+")  # visible ASCII: RFC 6750 b64token and the like
_SHA256_HEX = re.compile(r"[0-9a-fA-F]{64}")
_TOKEN_KEYS = frozenset({"name", "sha256", "groups"})


def token_sha256(token: str) -> str:
    """The sha256 hex digest of ``token``, the value ``serve.auth.tokens`` lists for it."""
    return hashlib.sha256(token.encode("utf-8")).hexdigest()


def bearer_token(values: Sequence[str]) -> str | None:
    """The token of the ``Authorization`` header values of a request; ``None`` unless usable.

    Exactly one value, the ``Bearer`` scheme in any case, one or more spaces, then a token of 1
    to :data:`MAX_TOKEN_LENGTH` visible ASCII characters (no space inside).
    """
    if len(values) != 1:
        return None
    value = values[0].strip()
    if len(value) > len(BEARER_SCHEME) + 1 + MAX_TOKEN_LENGTH + 64:
        return None  # far too long: not worth parsing
    scheme, separator, rest = value.partition(" ")
    token = rest.lstrip(" ")
    if scheme.lower() != BEARER_SCHEME or not separator or len(token) > MAX_TOKEN_LENGTH:
        return None
    if not _TOKEN_CHARS.fullmatch(token):
        return None
    return token


@dataclass(frozen=True)
class _Entry:
    """One configured token: its digest (left out of ``repr``) and the principal it authenticates."""

    sha256: str = field(repr=False)
    principal: Principal


def _groups(where: str, value: Any) -> frozenset[str]:
    if value is None:
        return frozenset()
    if isinstance(value, (str, bytes)) or not isinstance(value, Iterable):
        raise AuthConfigError(f"{where}: groups must be a list of strings")
    groups = list(value)
    if not all(isinstance(group, str) for group in groups):
        raise AuthConfigError(f"{where}: groups must be a list of strings")
    return frozenset(group.strip() for group in groups if group.strip())


def _entries(tokens: Sequence[Mapping[str, Any]]) -> tuple[_Entry, ...]:
    """Validate ``serve.auth.tokens`` again (the backend can be built without the config).

    The messages name the entry and the field, never a value that could be a token.
    """
    if isinstance(tokens, (str, bytes, Mapping)) or not isinstance(tokens, Iterable):
        raise AuthConfigError("serve.auth.tokens must be a list of {name, sha256, groups} entries")
    entries: list[_Entry] = []
    owners: dict[str, str] = {}
    for index, item in enumerate(tokens):
        where = f"serve.auth.tokens item {index}"
        if not isinstance(item, Mapping):
            raise AuthConfigError(f"{where}: expected a mapping with name, sha256 and groups")
        unknown = sorted(str(key) for key in item if key not in _TOKEN_KEYS)
        if unknown:
            raise AuthConfigError(
                f"{where}: unknown key(s) {', '.join(unknown)}; a token is configured by the sha256 hex "
                "digest of its value only (name, sha256, groups), never by the token itself"
            )
        name = item.get("name")
        if not isinstance(name, str) or not name.strip():
            raise AuthConfigError(f"{where}: name must be a non-empty string")
        name = name.strip()
        digest = item.get("sha256")
        if not isinstance(digest, str) or not _SHA256_HEX.fullmatch(digest.strip()):
            raise AuthConfigError(f"{where} ({name!r}): sha256 must be the 64-character hex sha256 digest")
        digest = digest.strip().lower()
        if digest in owners:
            raise AuthConfigError(f"serve.auth.tokens {owners[digest]!r} and {name!r} have the same sha256")
        owners[digest] = name
        groups = _groups(f"{where} ({name!r})", item.get("groups"))
        entries.append(_Entry(sha256=digest, principal=Principal(id=name, name=name, groups=groups)))
    if not entries:
        raise AuthConfigError(
            "the token backend needs at least one serve.auth.tokens entry: {name, sha256, groups}, "
            "where sha256 is the hex sha256 digest of the token"
        )
    return tuple(entries)


class TokenAuthBackend(StandaloneAuthBackend):
    """``serve.auth.backend: token``: bearer tokens configured by their sha256 digest.

    ``tokens`` holds ``{name, sha256, groups}`` entries (``serve.auth.tokens``);
    :class:`~dag_healer.hosting.auth.base.AuthConfigError` when there is none or one is invalid.
    """

    name = "token"

    def __init__(
        self,
        tokens: Sequence[Mapping[str, Any]],
        *,
        authorizer: Authorizer,
        origins: Sequence[str] = (),
    ) -> None:
        super().__init__(authorizer=authorizer, origins=origins)
        self._entries = _entries(tokens)

    def user_dependency(self) -> Callable[..., Any]:
        """A dependency that authenticates the ``Authorization: Bearer`` header of the request."""
        return self._current_user

    async def _current_user(self, request: Request) -> Principal:
        token = bearer_token(request.headers.getlist(AUTHORIZATION_HEADER))
        if token is None:
            log.debug("dag-healer: no usable bearer token in the request")
            raise not_authenticated(bearer=True)
        digest = token_sha256(token)
        found: Principal | None = None
        for entry in self._entries:  # every entry is compared: no early exit
            if hmac.compare_digest(digest, entry.sha256) and found is None:
                found = entry.principal
        if found is None:
            log.debug("dag-healer: the bearer token matches no serve.auth.tokens entry")
            raise not_authenticated(bearer=True)
        return found


__all__ = ["MAX_TOKEN_LENGTH", "TokenAuthBackend", "bearer_token", "token_sha256"]
