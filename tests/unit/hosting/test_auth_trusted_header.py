"""The ``trusted_header`` backend: identity headers believed from trusted proxies only (section 7.3).

The peer is the socket address (``TestClient(client=...)``), never ``X-Forwarded-For``: an
untrusted peer gets 401 whatever it sends, a trusted one must send the user header exactly once.
"""

from __future__ import annotations

import ipaddress
import logging
from pathlib import Path
from typing import Any

import pytest
from fastapi import Depends, FastAPI
from fastapi.testclient import TestClient

from dag_healer.api import auth
from dag_healer.api.auth import AuthBackend, apply_auth_backend
from dag_healer.config import config_from_mapping
from dag_healer.hosting.auth import AuthConfigError, build_auth_backend
from dag_healer.hosting.auth.trusted_header import (
    MAX_GROUPS,
    TrustedHeaderAuthBackend,
    parse_networks,
    peer_is_trusted,
)

AUTHORIZER: Any = object()
PUBLIC_URL = "https://healer.example.com"
PROXIES = ["10.0.0.0/8", "192.168.0.7", "fd00::/8"]
TRUSTED = "10.1.2.3"
UNTRUSTED = "192.0.2.10"
DENIED = {"detail": "Not authenticated"}
ALICE = {"X-Forwarded-User": "alice", "X-Forwarded-Groups": "data-eng, ops ,, "}


def whoami_client(backend: AuthBackend, peer: str) -> TestClient:
    """An app whose route depends on ``get_current_user``, with ``backend`` applied like ``serve``,
    reached from the socket address ``peer``."""
    app = FastAPI()

    @app.get("/whoami")
    def whoami(user: Any = Depends(auth.get_current_user)) -> dict[str, Any]:
        return {"id": user.id, "name": user.name, "groups": sorted(user.groups)}

    apply_auth_backend(app, backend)
    return TestClient(app, client=(peer, 1234))


@pytest.fixture
def backend() -> TrustedHeaderAuthBackend:
    return TrustedHeaderAuthBackend(trusted_proxies=PROXIES, authorizer=AUTHORIZER, origins=[PUBLIC_URL])


def get(backend: AuthBackend, peer: str, headers: Any) -> Any:
    return whoami_client(backend, peer).get("/whoami", headers=headers)


# =========================================================================== trusted peers


@pytest.mark.parametrize(
    "peer", [TRUSTED, "10.255.0.1", "192.168.0.7", "fd00::1", "fdab:1::7", "::ffff:10.0.0.5"]
)
def test_a_trusted_peer_gives_the_user_and_groups(backend: TrustedHeaderAuthBackend, peer: str) -> None:
    response = get(backend, peer, ALICE)
    assert response.status_code == 200
    assert response.json() == {"id": "alice", "name": "alice", "groups": ["data-eng", "ops"]}


def test_the_groups_header_is_optional(backend: TrustedHeaderAuthBackend) -> None:
    response = get(backend, TRUSTED, {"X-Forwarded-User": " alice@example.com "})
    assert response.json() == {"id": "alice@example.com", "name": "alice@example.com", "groups": []}
    empty = get(backend, TRUSTED, {"X-Forwarded-User": "alice", "X-Forwarded-Groups": ""})
    assert empty.json()["groups"] == []


def test_header_names_are_case_insensitive(backend: TrustedHeaderAuthBackend) -> None:
    response = get(backend, TRUSTED, {"x-forwarded-user": "alice", "X-FORWARDED-GROUPS": "ops"})
    assert response.json() == {"id": "alice", "name": "alice", "groups": ["ops"]}


@pytest.mark.parametrize(
    "peer", [UNTRUSTED, "10.0.0.1.5", "192.168.0.8", "fe80::1", "::ffff:192.0.2.10", "testclient"]
)
def test_any_other_peer_gets_401_whatever_it_sends(backend: TrustedHeaderAuthBackend, peer: str) -> None:
    response = get(
        backend,
        peer,
        {**ALICE, "X-Forwarded-For": TRUSTED, "X-Real-IP": TRUSTED, "Forwarded": f"for={TRUSTED}"},
    )
    assert response.status_code == 401
    assert response.json() == DENIED
    assert "www-authenticate" not in response.headers


@pytest.mark.parametrize(
    "headers",
    [
        pytest.param({}, id="no user header"),
        pytest.param({"X-Forwarded-Groups": "ops"}, id="groups only"),
        pytest.param({"X-Forwarded-User": ""}, id="empty user"),
        pytest.param({"X-Forwarded-User": "   "}, id="blank user"),
        pytest.param({"X-Forwarded-User": "a" * 257}, id="user too long"),
        pytest.param({"X-Forwarded-User": "admin, alice"}, id="appended user header"),
        pytest.param([("X-Forwarded-User", "alice"), ("X-Forwarded-User", "admin")], id="two user headers"),
        pytest.param(
            [("X-Forwarded-User", "alice"), ("X-Forwarded-User", "alice")], id="the same user twice"
        ),
        pytest.param(
            [("X-Forwarded-User", "alice"), ("X-Forwarded-Groups", "ops"), ("X-Forwarded-Groups", "admin")],
            id="two groups headers",
        ),
        pytest.param(
            {
                "X-Forwarded-User": "alice",
                "X-Forwarded-Groups": ",".join(f"g{n}" for n in range(MAX_GROUPS + 1)),
            },
            id="too many groups",
        ),
        pytest.param(
            {"X-Forwarded-User": "alice", "X-Forwarded-Groups": "ops," + "g" * 257}, id="group too long"
        ),
    ],
)
def test_a_trusted_peer_must_send_usable_headers(backend: TrustedHeaderAuthBackend, headers: Any) -> None:
    response = get(backend, TRUSTED, headers)
    assert response.status_code == 401
    assert response.json() == DENIED


def test_the_most_groups_a_request_may_name(backend: TrustedHeaderAuthBackend) -> None:
    groups = [f"g{n}" for n in range(MAX_GROUPS)]
    response = get(backend, TRUSTED, {"X-Forwarded-User": "alice", "X-Forwarded-Groups": ",".join(groups)})
    assert response.json()["groups"] == sorted(groups)


def test_custom_header_names_and_no_groups_header() -> None:
    backend = TrustedHeaderAuthBackend(
        trusted_proxies=["127.0.0.1"],
        user_header="X-Auth-Request-Email",
        groups_header=None,
        authorizer=AUTHORIZER,
    )
    headers = {
        "X-Auth-Request-Email": "alice@example.com",
        "X-Forwarded-User": "admin",
        "X-Forwarded-Groups": "admin",
    }
    assert get(backend, "127.0.0.1", headers).json() == {
        "id": "alice@example.com",
        "name": "alice@example.com",
        "groups": [],
    }
    assert get(backend, "127.0.0.1", {"X-Forwarded-User": "admin"}).status_code == 401


def test_header_values_are_never_logged(
    backend: TrustedHeaderAuthBackend, caplog: pytest.LogCaptureFixture
) -> None:
    caplog.set_level(logging.DEBUG, logger="dag_healer")
    get(backend, UNTRUSTED, {"X-Forwarded-User": "mallory", "X-Forwarded-Groups": "admins"})
    get(backend, TRUSTED, [("X-Forwarded-User", "mallory"), ("X-Forwarded-User", "eve")])
    get(
        backend,
        TRUSTED,
        [("X-Forwarded-User", "mallory"), ("X-Forwarded-Groups", "a"), ("X-Forwarded-Groups", "b")],
    )
    assert len(caplog.records) == 3
    assert UNTRUSTED in caplog.text and TRUSTED in caplog.text  # the peer address only
    for value in ("mallory", "eve", "admins"):
        assert value not in caplog.text


# =========================================================================== peers and networks


def test_peer_is_trusted() -> None:
    networks = parse_networks(PROXIES)
    assert peer_is_trusted("10.9.8.7", networks)
    assert peer_is_trusted("::ffff:10.9.8.7", networks)
    assert peer_is_trusted("fd00::42", networks)
    assert not peer_is_trusted("11.0.0.1", networks)
    assert not peer_is_trusted("::ffff:11.0.0.1", networks)
    for peer in (None, "", "testclient", "[fd00::42]", "10.0.0.0/8", "10.0.0.1:8080"):
        assert not peer_is_trusted(peer, networks)
    assert not peer_is_trusted("10.9.8.7", ())


def test_networks_are_parsed_and_ipv4_mapped_ones_normalised() -> None:
    assert parse_networks(["10.1.2.3/8", " 192.168.0.7 ", "fd00::/8", "::ffff:172.16.0.0/108"]) == (
        ipaddress.ip_network("10.0.0.0/8"),
        ipaddress.ip_network("192.168.0.7/32"),
        ipaddress.ip_network("fd00::/8"),
        ipaddress.ip_network("172.16.0.0/12"),
    )
    assert parse_networks("127.0.0.1") == (ipaddress.ip_network("127.0.0.1/32"),)
    backend = TrustedHeaderAuthBackend(trusted_proxies=["::ffff:172.16.0.0/108"], authorizer=AUTHORIZER)
    assert get(backend, "172.16.5.4", {"X-Forwarded-User": "alice"}).status_code == 200
    assert get(backend, "::ffff:172.16.5.4", {"X-Forwarded-User": "alice"}).status_code == 200


@pytest.mark.parametrize(
    ("proxies", "problem"),
    [
        ([], "the trusted_header backend requires serve.auth.trusted_proxies"),
        (None, "the trusted_header backend requires serve.auth.trusted_proxies"),
        (["proxy.local"], "serve.auth.trusted_proxies: 'proxy.local' is not an IP address or a CIDR"),
        (["10.0.0.0/33"], "'10.0.0.0/33' is not an IP address or a CIDR"),
        ([10], "10 is not an IP address or a CIDR"),
        (["0.0.0.0/0"], "'0.0.0.0/0' trusts every address, so anybody could send the user header"),
        (["10.0.0.0/8", "::/0"], "'::/0' trusts every address"),
        (["::ffff:0:0/96"], "trusts every address"),
        (["0.0.0.0/1", "128.0.0.0/1"], "'0.0.0.0/1' trusts too many addresses (at most an IPv4 /8,"),
        (["10.0.0.0/7"], "'10.0.0.0/7' trusts too many addresses"),
        (["::ffff:0:0/97"], "'::ffff:0:0/97' trusts too many addresses"),  # 0.0.0.0/1
        (["8000::/1"], "'8000::/1' trusts too many addresses"),
        (["2001::/16"], "'2001::/16' trusts too many addresses"),
        (["fc00::/7"], "'fc00::/7' trusts too many addresses"),
        (["fe80::/10"], "'fe80::/10' trusts too many addresses"),
        (["::/64"], "'::/64' holds the IPv4-mapped addresses (::ffff:0:0/96), so it trusts every IPv4"),
        (["::/80"], "holds the IPv4-mapped addresses"),
        (["::/1"], "holds the IPv4-mapped addresses"),
        (["::fffe:0:0/95"], "holds the IPv4-mapped addresses"),
        (["10.0.0.0/8", "11.0.0.0/8"], "serve.auth.trusted_proxies together trust 10.0.0.0/7, too many"),
        (["fc00::/8", "fd00::/8"], "together trust fc00::/7"),
        ([f"{n}.0.0.0/8" for n in range(256)], "together trust 0.0.0.0/0"),
    ],
)
def test_unusable_trusted_proxies_are_refused(proxies: Any, problem: str) -> None:
    with pytest.raises(AuthConfigError) as excinfo:
        TrustedHeaderAuthBackend(trusted_proxies=proxies, authorizer=AUTHORIZER)
    assert problem in str(excinfo.value)


def test_the_widest_trusted_networks() -> None:
    proxies = ["10.0.0.0/8", "2001:db8::/32", "fd00::/8", "::ffff:172.0.0.0/104", "::1", "::/96"]
    assert parse_networks(proxies) == (
        ipaddress.ip_network("10.0.0.0/8"),
        ipaddress.ip_network("2001:db8::/32"),
        ipaddress.ip_network("fd00::/8"),
        ipaddress.ip_network("172.0.0.0/8"),
        ipaddress.ip_network("::1/128"),
        ipaddress.ip_network("::/96"),  # IPv4-compatible, not IPv4-mapped: no IPv4 client is in it
    )
    assert not peer_is_trusted("::ffff:8.8.8.8", parse_networks(["::/96"]))


@pytest.mark.parametrize(
    ("headers", "problem"),
    [
        ({"user_header": "X User"}, "serve.auth.user_header: 'X User' is not an HTTP header name"),
        ({"user_header": ""}, "serve.auth.user_header: '' is not an HTTP header name"),
        ({"groups_header": "X:Groups"}, "serve.auth.groups_header: 'X:Groups' is not an HTTP header name"),
        (
            {"groups_header": "x-forwarded-user"},
            "serve.auth.groups_header must differ from serve.auth.user_header",
        ),
    ],
)
def test_invalid_header_names_are_refused(headers: dict[str, Any], problem: str) -> None:
    with pytest.raises(AuthConfigError) as excinfo:
        TrustedHeaderAuthBackend(trusted_proxies=PROXIES, authorizer=AUTHORIZER, **headers)
    assert problem in str(excinfo.value)


# =========================================================================== configuration


def test_the_trusted_header_backend_is_an_auth_backend(backend: TrustedHeaderAuthBackend) -> None:
    assert isinstance(backend, AuthBackend)
    assert backend.name == "trusted_header"
    assert backend.authorizer() is AUTHORIZER
    assert backend.allowed_origins() == [PUBLIC_URL]
    assert backend.networks == parse_networks(PROXIES)


def test_the_factory_builds_it_from_serve_auth(healer_env: Path) -> None:
    cfg = config_from_mapping(
        {
            "serve": {
                "public_url": PUBLIC_URL,
                "auth": {
                    "backend": "trusted_header",
                    "trusted_proxies": ["10.0.0.0/8"],
                    "user_header": "X-Auth-Request-User",
                    "groups_header": "X-Auth-Request-Groups",
                },
            }
        }
    )
    backend = build_auth_backend(cfg, authorizer=AUTHORIZER)
    assert isinstance(backend, TrustedHeaderAuthBackend)
    assert backend.authorizer() is AUTHORIZER
    assert backend.allowed_origins() == [PUBLIC_URL]
    headers = {"X-Auth-Request-User": "alice", "X-Auth-Request-Groups": "data-eng"}
    assert get(backend, "10.0.0.9", headers).json() == {
        "id": "alice",
        "name": "alice",
        "groups": ["data-eng"],
    }
    assert get(backend, UNTRUSTED, headers).status_code == 401
