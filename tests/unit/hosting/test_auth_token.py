"""The ``token`` backend: ``Authorization: Bearer`` tokens configured by their sha256 (section 7.3).

Through the ``dependency_overrides`` wiring of ``dag-healer serve``: a valid token gives the
entry's name and groups; anything else gives the same 401 with ``WWW-Authenticate: Bearer``.
The backend keeps digests only, compares every entry with ``hmac.compare_digest`` and never
logs a token or a digest.
"""

from __future__ import annotations

import dataclasses
import hmac
import logging
from collections.abc import Iterator, Mapping
from pathlib import Path
from typing import Any

import pytest
from fastapi import Depends, FastAPI
from fastapi.testclient import TestClient

from dag_healer.api import auth
from dag_healer.api.auth import AuthBackend, apply_auth_backend
from dag_healer.config import config_from_mapping
from dag_healer.hosting.auth import AuthConfigError, build_auth_backend
from dag_healer.hosting.auth.token import MAX_TOKEN_LENGTH, TokenAuthBackend, bearer_token, token_sha256

AUTHORIZER: Any = object()
PUBLIC_URL = "https://healer.example.com"
CI_TOKEN = "ci-0123456789abcdef-token"
OPS_TOKEN = "ops-fedcba9876543210-token"
DENIED = {"detail": "Not authenticated"}


def entry(name: str, token: str, groups: list[str] | None = None) -> dict[str, Any]:
    return {"name": name, "sha256": token_sha256(token), "groups": groups or []}


TOKENS = [entry("ci", CI_TOKEN, ["bots"]), entry("ops", OPS_TOKEN, ["oncall", "admins"])]


def whoami_client(backend: AuthBackend) -> TestClient:
    """An app whose route depends on ``get_current_user``, with ``backend`` applied like ``serve``."""
    app = FastAPI()

    @app.get("/whoami")
    def whoami(user: Any = Depends(auth.get_current_user)) -> dict[str, Any]:
        return {"id": user.id, "name": user.name, "groups": sorted(user.groups)}

    apply_auth_backend(app, backend)
    return TestClient(app)


def bearer(token: str, scheme: str = "Bearer") -> dict[str, str]:
    return {"Authorization": f"{scheme} {token}"}


@pytest.fixture
def backend() -> TokenAuthBackend:
    return TokenAuthBackend(TOKENS, authorizer=AUTHORIZER, origins=[PUBLIC_URL])


@pytest.fixture
def client(backend: TokenAuthBackend) -> TestClient:
    return whoami_client(backend)


def strings_in(value: Any, seen: set[int] | None = None) -> Iterator[str]:
    """Every string reachable from ``value`` (attributes, containers, dataclass fields)."""
    seen = set() if seen is None else seen
    if isinstance(value, str):
        yield value
        return
    if id(value) in seen:
        return
    seen.add(id(value))
    if isinstance(value, Mapping):
        items: list[Any] = [*value.keys(), *value.values()]
    elif isinstance(value, (list, tuple, set, frozenset)):
        items = list(value)
    elif dataclasses.is_dataclass(value) or hasattr(value, "__dict__"):
        items = list(vars(value).values())
    else:
        return
    for item in items:
        yield from strings_in(item, seen)


# =========================================================================== authentication


def test_a_configured_token_is_its_entry(client: TestClient) -> None:
    response = client.get("/whoami", headers=bearer(CI_TOKEN))
    assert response.status_code == 200
    assert response.json() == {"id": "ci", "name": "ci", "groups": ["bots"]}
    assert client.get("/whoami", headers=bearer(OPS_TOKEN)).json() == {
        "id": "ops",
        "name": "ops",
        "groups": ["admins", "oncall"],
    }


@pytest.mark.parametrize("scheme", ["bearer", "BEARER", "BeArEr"])
def test_the_scheme_is_case_insensitive(client: TestClient, scheme: str) -> None:
    assert client.get("/whoami", headers=bearer(CI_TOKEN, scheme)).json()["id"] == "ci"


def test_spaces_around_the_token_are_ignored(client: TestClient) -> None:
    assert client.get("/whoami", headers={"Authorization": f"Bearer   {CI_TOKEN} "}).json()["id"] == "ci"


@pytest.mark.parametrize(
    "headers",
    [
        pytest.param({}, id="no header"),
        pytest.param(bearer("wrong-token"), id="unknown token"),
        pytest.param(bearer(CI_TOKEN.upper()), id="case of the token"),
        pytest.param(bearer(token_sha256(CI_TOKEN)), id="the digest instead of the token"),
        pytest.param(bearer(CI_TOKEN, "Basic"), id="Basic scheme"),
        pytest.param(bearer(CI_TOKEN, "Token"), id="another scheme"),
        pytest.param({"Authorization": CI_TOKEN}, id="no scheme"),
        pytest.param({"Authorization": "Bearer"}, id="no token"),
        pytest.param({"Authorization": "Bearer "}, id="empty token"),
        pytest.param({"Authorization": f"Bearer\t{CI_TOKEN}"}, id="tab separator"),
        pytest.param({"Authorization": f"Bearer {CI_TOKEN} extra"}, id="space inside"),
        pytest.param({"Authorization": f"Bearer {CI_TOKEN}é".encode()}, id="not visible ASCII"),
        pytest.param(
            [("Authorization", f"Bearer {CI_TOKEN}"), ("Authorization", "Bearer x")], id="two headers"
        ),
        pytest.param(
            [("Authorization", f"Bearer {CI_TOKEN}"), ("Authorization", f"Bearer {CI_TOKEN}")],
            id="the same header twice",
        ),
        pytest.param({"Cookie": f"token={CI_TOKEN}"}, id="cookie"),
        pytest.param({"X-Healer-Token": CI_TOKEN}, id="another header"),
    ],
)
def test_anything_else_is_the_same_401(client: TestClient, headers: Any) -> None:
    response = client.get("/whoami", headers=headers)
    assert response.status_code == 401
    assert response.json() == DENIED
    assert response.headers["www-authenticate"] == "Bearer"


def test_the_token_is_never_read_from_the_query(client: TestClient) -> None:
    assert client.get("/whoami", params={"token": CI_TOKEN}).status_code == 401


def test_tokens_longer_than_the_limit_are_refused() -> None:
    longest = "t" * MAX_TOKEN_LENGTH
    backend = TokenAuthBackend(
        [entry("long", longest), entry("longer", longest + "t")], authorizer=AUTHORIZER
    )
    client = whoami_client(backend)
    assert client.get("/whoami", headers=bearer(longest)).json()["id"] == "long"
    assert client.get("/whoami", headers=bearer(longest + "t")).status_code == 401
    assert client.get("/whoami", headers=bearer("t" * 100_000)).status_code == 401


def test_two_tokens_may_share_a_name_while_one_is_rotated() -> None:
    backend = TokenAuthBackend(
        [entry("ci", "old-ci-token", ["bots"]), entry("ci", "new-ci-token", ["bots"])], authorizer=AUTHORIZER
    )
    client = whoami_client(backend)
    for token in ("old-ci-token", "new-ci-token"):
        assert client.get("/whoami", headers=bearer(token)).json() == {
            "id": "ci",
            "name": "ci",
            "groups": ["bots"],
        }


@pytest.mark.parametrize(
    ("values", "expected"),
    [
        ([f"Bearer {CI_TOKEN}"], CI_TOKEN),
        ([f"bearer  {CI_TOKEN}"], CI_TOKEN),
        ([], None),
        ([f"Bearer {CI_TOKEN}", f"Bearer {CI_TOKEN}"], None),
        (["Bearer"], None),
        (["Bearer x y"], None),
        ([f"Bearer {'t' * (MAX_TOKEN_LENGTH + 1)}"], None),
        (["Bearerx token"], None),
    ],
)
def test_bearer_token(values: list[str], expected: str | None) -> None:
    assert bearer_token(values) == expected


def test_token_sha256_is_the_hex_digest_of_the_utf8_token() -> None:
    assert token_sha256("test") == "9f86d081884c7d659a2feaa0c55ad015a3bf4f1b2b0b822cd15d6c15b0f00a08"


# =========================================================================== secrets


def test_the_backend_keeps_digests_only(backend: TokenAuthBackend, client: TestClient) -> None:
    assert client.get("/whoami", headers=bearer(CI_TOKEN)).status_code == 200
    kept = list(strings_in(backend))
    assert token_sha256(CI_TOKEN) in kept and token_sha256(OPS_TOKEN) in kept  # the walk does see them
    assert not [text for text in kept if CI_TOKEN in text or OPS_TOKEN in text]
    assert token_sha256(CI_TOKEN) not in repr(vars(backend))  # nor in a repr (tracebacks, debuggers)


def test_every_entry_is_compared_with_compare_digest_without_an_early_exit(
    backend: TokenAuthBackend, client: TestClient, monkeypatch: pytest.MonkeyPatch
) -> None:
    compared: list[str] = []
    compare_digest = hmac.compare_digest

    def counting(a: Any, b: Any) -> bool:
        compared.append(b)
        return compare_digest(a, b)

    monkeypatch.setattr(hmac, "compare_digest", counting)
    assert client.get("/whoami", headers=bearer(CI_TOKEN)).json()["id"] == "ci"  # the first entry matches
    assert compared == [token_sha256(CI_TOKEN), token_sha256(OPS_TOKEN)]
    compared.clear()
    assert client.get("/whoami", headers=bearer("wrong-token")).status_code == 401
    assert len(compared) == 2


def test_tokens_and_digests_are_never_logged(client: TestClient, caplog: pytest.LogCaptureFixture) -> None:
    caplog.set_level(logging.DEBUG, logger="dag_healer")
    client.get("/whoami", headers=bearer(CI_TOKEN))
    client.get("/whoami", headers=bearer("wrong-token"))
    client.get("/whoami", headers=bearer(OPS_TOKEN, "Basic"))
    assert [record.name for record in caplog.records] == ["dag_healer.hosting.auth.token"] * 2
    for secret in (CI_TOKEN, OPS_TOKEN, "wrong-token", token_sha256(CI_TOKEN), token_sha256("wrong-token")):
        assert secret not in caplog.text


# =========================================================================== configuration


def test_the_token_backend_is_an_auth_backend(backend: TokenAuthBackend) -> None:
    assert isinstance(backend, AuthBackend)
    assert backend.name == "token"
    assert backend.authorizer() is AUTHORIZER
    assert backend.allowed_origins() == [PUBLIC_URL]


def test_entries_are_validated_again() -> None:
    backend = TokenAuthBackend(
        [{"name": " ci ", "sha256": token_sha256(CI_TOKEN).upper(), "groups": [" bots ", ""]}],
        authorizer=AUTHORIZER,
    )
    assert whoami_client(backend).get("/whoami", headers=bearer(CI_TOKEN)).json() == {
        "id": "ci",
        "name": "ci",
        "groups": ["bots"],
    }


def test_no_token_entry_is_refused() -> None:
    with pytest.raises(AuthConfigError, match="the token backend needs at least one serve.auth.tokens entry"):
        TokenAuthBackend([], authorizer=AUTHORIZER)


@pytest.mark.parametrize(
    ("tokens", "problem"),
    [
        ("ci-token", "serve.auth.tokens must be a list"),
        ({"name": "ci", "sha256": "a" * 64}, "serve.auth.tokens must be a list"),
        (["ci-token"], "item 0: expected a mapping with name, sha256 and groups"),
        ([{"name": "ci", "token": "plain-secret"}], "item 0: unknown key(s) token; a token is configured"),
        ([{"name": "ci", "sha256": "plain-secret"}], "item 0 ('ci'): sha256 must be the 64-character hex"),
        ([{"name": "ci", "sha256": "g" * 64}], "sha256 must be the 64-character hex sha256 digest"),
        ([{"name": "ci"}], "sha256 must be the 64-character hex sha256 digest"),
        ([{"name": " ", "sha256": "a" * 64}], "item 0: name must be a non-empty string"),
        ([{"name": "ci", "sha256": "a" * 64, "groups": "bots"}], "groups must be a list of strings"),
        ([{"name": "ci", "sha256": "a" * 64, "groups": [1]}], "groups must be a list of strings"),
        (
            [{"name": "a", "sha256": "a" * 64}, {"name": "b", "sha256": "A" * 64}],
            "serve.auth.tokens 'a' and 'b' have the same sha256",
        ),
    ],
)
def test_invalid_entries_are_refused_without_echoing_values(tokens: Any, problem: str) -> None:
    with pytest.raises(AuthConfigError) as excinfo:
        TokenAuthBackend(tokens, authorizer=AUTHORIZER)
    message = str(excinfo.value)
    assert problem in message
    assert "plain-secret" not in message and "a" * 64 not in message


def test_the_factory_builds_it_from_serve_auth_tokens(healer_env: Path) -> None:
    cfg = config_from_mapping(
        {
            "serve": {
                "public_url": PUBLIC_URL,
                "auth": {
                    "backend": "token",
                    "tokens": [
                        {"name": "alice", "sha256": token_sha256("alice-token"), "groups": "data-eng"}
                    ],
                },
            }
        }
    )
    backend = build_auth_backend(cfg, authorizer=AUTHORIZER, bind_host="0.0.0.0")  # any address
    assert isinstance(backend, TokenAuthBackend)
    assert backend.authorizer() is AUTHORIZER
    assert backend.allowed_origins() == [PUBLIC_URL]
    response = whoami_client(backend).get("/whoami", headers=bearer("alice-token"))
    assert response.json() == {"id": "alice", "name": "alice", "groups": ["data-eng"]}


def test_the_factory_refuses_the_token_backend_without_tokens(healer_env: Path) -> None:
    cfg = config_from_mapping({"serve": {"auth": {"backend": "token"}}})
    with pytest.raises(AuthConfigError, match="at least one serve.auth.tokens entry"):
        build_auth_backend(cfg, authorizer=AUTHORIZER)
