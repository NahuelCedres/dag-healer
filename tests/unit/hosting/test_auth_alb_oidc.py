"""The ``alb_oidc`` backend: the ALB-signed ``x-amzn-oidc-data`` JWT (sections 7.3 and 11.7).

Tokens are signed with a locally generated ES256 key and the key endpoint is a stub
(``key_fetcher``) that records the URLs it is asked for, so the tests can tell that the header
checks (``alg``, ``kid``, ``signer``, ``exp``) run before any fetch. ALB-style tokens carry base64
padding; both the padding-stripped and the as-received forms verify. Their email is verified
unless a test says otherwise. ``fetch_public_key`` is tested against an ``httpx.MockTransport``.
"""

from __future__ import annotations

import base64
import json
import logging
import sys
import threading
import time
from collections.abc import Callable, Mapping
from pathlib import Path
from types import MappingProxyType, SimpleNamespace
from typing import Any

import httpx
import jwt
import pytest
from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.primitives.asymmetric import ec
from fastapi import Depends, FastAPI, HTTPException
from fastapi.testclient import TestClient
from jwt.algorithms import ECAlgorithm
from starlette.requests import Request

from dag_healer.api import auth
from dag_healer.api.auth import AuthBackend, Principal, apply_auth_backend
from dag_healer.config import config_from_mapping
from dag_healer.hosting.auth import AuthConfigError, alb_oidc, build_auth_backend
from dag_healer.hosting.auth.alb_oidc import (
    FAILURE_TTL,
    KEY_FETCH_WINDOW,
    KEY_UNAVAILABLE_DETAIL,
    PYJWT_HINT,
    AlbOidcAuthBackend,
    SigningKeyNotFound,
    SigningKeyUnavailable,
    alb_key_url,
    fetch_public_key,
    load_public_key,
    strip_jwt_padding,
)

AUTHORIZER: Any = object()
PUBLIC_URL = "https://healer.example.com"
ALB_ARN = "arn:aws:elasticloadbalancing:eu-west-1:123456789012:loadbalancer/app/healer/50dc6c495c0c9188"
OTHER_ARN = "arn:aws:elasticloadbalancing:eu-west-1:210987654321:loadbalancer/app/other/0123456789abcdef"
GOV_ARN = "arn:aws-us-gov:elasticloadbalancing:us-gov-west-1:123456789012:loadbalancer/app/healer/0123abcd"
REGION = "eu-west-1"
KID = "8b4e7f1a-2c3d-4e5f-a6b7-c8d9e0f1a2b3"
KEY_URL = f"https://public-keys.auth.elb.eu-west-1.amazonaws.com/{KID}"
ISSUER = "https://cognito-idp.eu-west-1.amazonaws.com/eu-west-1_Example"
SUB = "6c1f2a7e-90b4-4d3c-8a51-2f0e9d7c6b5a"
EMAIL = "alice@example.com"
DENIED = {"detail": "Not authenticated"}
DROP = object()  # a header field or claim left out of the token


# --------------------------------------------------------------------------- keys and tokens


def new_key(curve: ec.EllipticCurve | None = None) -> ec.EllipticCurvePrivateKey:
    return ec.generate_private_key(curve or ec.SECP256R1())


def public_pem(key: ec.EllipticCurvePrivateKey) -> str:
    return (
        key.public_key()
        .public_bytes(serialization.Encoding.PEM, serialization.PublicFormat.SubjectPublicKeyInfo)
        .decode("ascii")
    )


@pytest.fixture(scope="module")
def key() -> ec.EllipticCurvePrivateKey:
    return new_key()


def pad(token: str) -> str:
    """Add the base64 padding the ALB puts on every segment."""
    return ".".join(segment + "=" * (-len(segment) % 4) for segment in token.split("."))


def fields(base: dict[str, Any], changes: Mapping[str, Any] | None) -> dict[str, Any]:
    result = {**base, **(changes or {})}
    return {name: value for name, value in result.items() if value is not DROP}


def alb_token(
    key: ec.EllipticCurvePrivateKey,
    *,
    claims: Mapping[str, Any] | None = None,
    header: Mapping[str, Any] | None = None,
    padded: bool = True,
    lifetime: int = 120,
) -> str:
    """An ``x-amzn-oidc-data`` JWT as an ALB sends it: padded segments over an unpadded signature."""
    now = int(time.time())
    headers = fields(
        {"kid": KID, "signer": ALB_ARN, "iss": ISSUER, "client": "healer", "exp": now + lifetime}, header
    )
    payload = fields(
        {"sub": SUB, "email": EMAIL, "email_verified": True, "exp": now + lifetime, "iss": ISSUER}, claims
    )
    token = jwt.encode(payload, key, algorithm="ES256", headers=headers)
    return pad(token) if padded else token


def segment(data: Mapping[str, Any]) -> str:
    return base64.urlsafe_b64encode(json.dumps(data, separators=(",", ":")).encode()).decode()


def token_signed_padded(key: ec.EllipticCurvePrivateKey) -> str:
    """A token whose signature covers the padded segments (the other reading of the ALB format)."""
    now = int(time.time())
    header = {"typ": "JWT", "alg": "ES256", "kid": KID, "signer": ALB_ARN, "iss": ISSUER, "exp": now + 120}
    payload = {"sub": SUB, "email": EMAIL, "email_verified": "true", "exp": now + 120, "iss": ISSUER}
    signing_input = f"{segment(header)}.{segment(payload)}"
    signature = ECAlgorithm(ECAlgorithm.SHA256).sign(signing_input.encode(), key)
    return f"{signing_input}.{base64.urlsafe_b64encode(signature).decode()}"


def unsigned_token(header: Mapping[str, Any], payload: Mapping[str, Any], signature: str = "") -> str:
    def raw(data: Mapping[str, Any]) -> str:
        return segment(data).rstrip("=")

    return f"{raw(header)}.{raw(payload)}.{signature}"


class KeyEndpoint:
    """A stub of the regional key endpoint (``key_fetcher``) recording the URLs it is asked for."""

    def __init__(self, keys: Mapping[str, str] | None = None, *, error: BaseException | None = None) -> None:
        self.keys = dict(keys or {})
        self.error = error
        self.urls: list[str] = []

    def __call__(self, url: str) -> str:
        self.urls.append(url)
        if self.error is not None:
            raise self.error
        kid = url.rsplit("/", 1)[-1]
        if kid not in self.keys:
            raise SigningKeyNotFound("HTTP 404")
        return self.keys[kid]


class Clock:
    """A settable time source for the header ``exp`` check and the failure memory."""

    def __init__(self) -> None:
        self.now = time.time()

    def __call__(self) -> float:
        return self.now


@pytest.fixture
def endpoint(key: ec.EllipticCurvePrivateKey) -> KeyEndpoint:
    return KeyEndpoint({KID: public_pem(key)})


def make_backend(endpoint: Callable[[str], str], **kwargs: Any) -> AlbOidcAuthBackend:
    options: dict[str, Any] = {"alb_arn": ALB_ARN, "region": REGION, "authorizer": AUTHORIZER}
    options.update(kwargs)
    return AlbOidcAuthBackend(key_fetcher=endpoint, **options)


@pytest.fixture
def backend(endpoint: KeyEndpoint) -> AlbOidcAuthBackend:
    return make_backend(endpoint, groups_claim="groups", origins=[PUBLIC_URL])


def whoami_client(backend: AuthBackend) -> TestClient:
    """An app whose route depends on ``get_current_user``, with ``backend`` applied like ``serve``."""
    app = FastAPI()

    @app.get("/whoami")
    def whoami(user: Any = Depends(auth.get_current_user)) -> dict[str, Any]:
        return {"id": user.id, "name": user.name, "groups": sorted(user.groups)}

    apply_auth_backend(app, backend)
    return TestClient(app)


def data(token: str) -> dict[str, str]:
    return {"x-amzn-oidc-data": token}


def principal_of(backend: AlbOidcAuthBackend, *headers: tuple[str, str]) -> Principal:
    """Call the user dependency directly with a request carrying ``headers``."""
    scope = {
        "type": "http",
        "method": "GET",
        "path": "/whoami",
        "headers": [(name.lower().encode("latin-1"), value.encode("latin-1")) for name, value in headers],
        "client": ("10.0.0.10", 443),
    }
    return backend.user_dependency()(Request(scope))


def rejection(backend: AlbOidcAuthBackend, *headers: tuple[str, str]) -> HTTPException:
    with pytest.raises(HTTPException) as excinfo:
        principal_of(backend, *headers)
    return excinfo.value


def rejected_status(backend: AlbOidcAuthBackend, token: str) -> int:
    return rejection(backend, ("x-amzn-oidc-data", token)).status_code


# =========================================================================== valid tokens


def test_a_valid_token_gives_the_email_and_the_groups(
    backend: AlbOidcAuthBackend, endpoint: KeyEndpoint, key: ec.EllipticCurvePrivateKey
) -> None:
    response = whoami_client(backend).get(
        "/whoami", headers=data(alb_token(key, claims={"groups": ["ops", "data-eng"]}))
    )
    assert response.status_code == 200
    assert response.json() == {"id": EMAIL, "name": EMAIL, "groups": ["data-eng", "ops"]}
    assert endpoint.urls == [KEY_URL]


def test_groups_may_be_a_comma_separated_string(
    backend: AlbOidcAuthBackend, key: ec.EllipticCurvePrivateKey
) -> None:
    principal = principal_of(
        backend, ("x-amzn-oidc-data", alb_token(key, claims={"groups": "data-eng, ops,,"}))
    )
    assert principal.groups == frozenset({"data-eng", "ops"})


@pytest.mark.parametrize(
    ("groups", "expected"),
    [
        pytest.param(DROP, set(), id="no claim"),
        pytest.param(None, set(), id="null"),
        pytest.param(42, set(), id="a number"),
        pytest.param({"ops": True}, set(), id="an object"),
        pytest.param(["ops", 7, "", "x" * 300, "data-eng"], {"ops", "data-eng"}, id="unusable names"),
    ],
)
def test_odd_groups_claims(
    backend: AlbOidcAuthBackend, key: ec.EllipticCurvePrivateKey, groups: Any, expected: set[str]
) -> None:
    principal = principal_of(backend, ("x-amzn-oidc-data", alb_token(key, claims={"groups": groups})))
    assert principal.groups == frozenset(expected)


def test_without_groups_claim_no_groups_are_read(
    endpoint: KeyEndpoint, key: ec.EllipticCurvePrivateKey
) -> None:
    principal = principal_of(
        make_backend(endpoint), ("x-amzn-oidc-data", alb_token(key, claims={"groups": ["ops"]}))
    )
    assert principal.groups == frozenset()


def test_the_principal_keeps_the_verified_claims_read_only(
    backend: AlbOidcAuthBackend, key: ec.EllipticCurvePrivateKey
) -> None:
    principal = principal_of(backend, ("x-amzn-oidc-data", alb_token(key, claims={"name": "Alice"})))
    assert principal == Principal(id=EMAIL, name=EMAIL)
    assert isinstance(principal.claims, MappingProxyType)
    assert (principal.claims["sub"], principal.claims["name"], principal.claims["iss"]) == (
        SUB,
        "Alice",
        ISSUER,
    )
    with pytest.raises(TypeError):
        principal.claims["email"] = "mallory@example.com"  # type: ignore[index]
    assert auth.user_name(principal) == EMAIL


@pytest.mark.parametrize(
    "token",
    [
        pytest.param(lambda key: alb_token(key, padded=True), id="padded segments, signed unpadded"),
        pytest.param(lambda key: alb_token(key, padded=False), id="no padding"),
        pytest.param(token_signed_padded, id="signed over the padded segments"),
    ],
)
def test_both_forms_of_the_padded_token_verify(
    backend: AlbOidcAuthBackend, key: ec.EllipticCurvePrivateKey, token: Callable[[Any], str]
) -> None:
    assert principal_of(backend, ("x-amzn-oidc-data", token(key))).id == EMAIL


def test_the_alb_tokens_of_the_tests_do_carry_padding(key: ec.EllipticCurvePrivateKey) -> None:
    for token in (alb_token(key), token_signed_padded(key)):
        assert "=" in token and strip_jwt_padding(token) != token
        assert "=" not in strip_jwt_padding(token)


def test_the_user_falls_back_to_sub(backend: AlbOidcAuthBackend, key: ec.EllipticCurvePrivateKey) -> None:
    for email in (DROP, "", "   ", ["alice@example.com"], "a\nb"):
        assert principal_of(backend, ("x-amzn-oidc-data", alb_token(key, claims={"email": email}))).id == SUB


@pytest.mark.parametrize(
    "verified",
    [
        pytest.param(False, id="false"),
        pytest.param("false", id="the string false (Cognito)"),
        pytest.param("False", id="False"),
        pytest.param(DROP, id="missing"),
        pytest.param(None, id="null"),
        pytest.param(1, id="a number"),
        pytest.param("yes", id="another string"),
        pytest.param(["true"], id="a list"),
    ],
)
def test_an_unverified_email_is_not_the_user(
    backend: AlbOidcAuthBackend, key: ec.EllipticCurvePrivateKey, verified: Any
) -> None:
    # A user of the identity provider set somebody else's address and never verified it.
    token = alb_token(key, claims={"email": "admin@example.com", "email_verified": verified})
    principal = principal_of(backend, ("x-amzn-oidc-data", token))
    assert principal == Principal(id=SUB, name=SUB)  # sub: assigned by the identity provider
    assert auth.user_name(principal) == SUB  # the actor of the commands this user queues


@pytest.mark.parametrize("verified", [True, "true", "TRUE"])
def test_a_verified_email_is_the_user(
    backend: AlbOidcAuthBackend, key: ec.EllipticCurvePrivateKey, verified: Any
) -> None:
    token = alb_token(key, claims={"email_verified": verified})
    assert principal_of(backend, ("x-amzn-oidc-data", token)).id == EMAIL


def test_an_unverified_email_and_no_sub_are_refused(
    backend: AlbOidcAuthBackend, key: ec.EllipticCurvePrivateKey
) -> None:
    token = alb_token(key, claims={"email_verified": False, "sub": DROP})
    assert rejection(backend, ("x-amzn-oidc-data", token)).status_code == 401


def test_a_phone_number_counts_only_when_verified(
    endpoint: KeyEndpoint, key: ec.EllipticCurvePrivateKey
) -> None:
    backend = make_backend(endpoint, user_claim="phone_number")
    phone = "+15550100"
    for verified, user in ((True, phone), ("true", phone), (False, SUB), (DROP, SUB)):
        token = alb_token(key, claims={"phone_number": phone, "phone_number_verified": verified})
        assert principal_of(backend, ("x-amzn-oidc-data", token)).id == user


def test_the_unverified_email_is_not_logged(
    backend: AlbOidcAuthBackend, key: ec.EllipticCurvePrivateKey, caplog: pytest.LogCaptureFixture
) -> None:
    caplog.set_level(logging.DEBUG, logger="dag_healer")
    token = alb_token(key, claims={"email": "admin@example.com", "email_verified": False})
    assert principal_of(backend, ("x-amzn-oidc-data", token)).id == SUB
    assert "the email claim is not verified" in caplog.text
    assert "admin@example.com" not in caplog.text and SUB not in caplog.text


def test_a_custom_user_claim(endpoint: KeyEndpoint, key: ec.EllipticCurvePrivateKey) -> None:
    backend = make_backend(endpoint, user_claim="username")
    token = alb_token(key, claims={"username": "alice"})
    assert principal_of(backend, ("x-amzn-oidc-data", token)) == Principal(id="alice", name="alice")


def test_without_a_usable_user_the_token_is_refused(
    backend: AlbOidcAuthBackend, key: ec.EllipticCurvePrivateKey
) -> None:
    token = alb_token(key, claims={"email": DROP, "sub": DROP})
    assert rejection(backend, ("x-amzn-oidc-data", token)).status_code == 401


def test_an_audience_claim_does_not_refuse_the_token(
    backend: AlbOidcAuthBackend, key: ec.EllipticCurvePrivateKey
) -> None:
    token = alb_token(key, claims={"aud": "the-identity-provider-client"})
    assert principal_of(backend, ("x-amzn-oidc-data", token)).id == EMAIL


def test_the_identity_headers_are_never_read(
    backend: AlbOidcAuthBackend, key: ec.EllipticCurvePrivateKey
) -> None:
    client = whoami_client(backend)
    headers = {
        "x-amzn-oidc-data": alb_token(key),
        "x-amzn-oidc-identity": "mallory@example.com",
        "x-amzn-oidc-accesstoken": "access-token",
    }
    assert client.get("/whoami", headers=headers).json()["id"] == EMAIL
    response = client.get("/whoami", headers={"x-amzn-oidc-identity": "mallory@example.com"})
    assert (response.status_code, response.json()) == (401, DENIED)


# =========================================================================== refused tokens


def assert_refused_before_any_fetch(backend: AlbOidcAuthBackend, endpoint: KeyEndpoint, token: str) -> None:
    response = whoami_client(backend).get("/whoami", headers=data(token))
    assert (response.status_code, response.json()) == (401, DENIED)
    assert "www-authenticate" not in response.headers
    assert endpoint.urls == []


def test_a_wrong_signer_is_refused_before_any_fetch(
    backend: AlbOidcAuthBackend, endpoint: KeyEndpoint, key: ec.EllipticCurvePrivateKey
) -> None:
    for signer in (OTHER_ARN, ALB_ARN.upper(), f"{ALB_ARN} ", "", None, [ALB_ARN], DROP):
        assert_refused_before_any_fetch(backend, endpoint, alb_token(key, header={"signer": signer}))


@pytest.mark.parametrize(
    "exp",
    [
        pytest.param(-1, id="past"),
        pytest.param(0, id="now"),
        pytest.param(DROP, id="missing"),
        pytest.param(None, id="null"),
        pytest.param("9999999999", id="string"),
        pytest.param(True, id="bool"),
        pytest.param(float("nan"), id="NaN"),
        pytest.param(float("inf"), id="infinity"),
    ],
)
def test_an_expired_or_missing_header_exp_is_refused_before_any_fetch(
    backend: AlbOidcAuthBackend, endpoint: KeyEndpoint, key: ec.EllipticCurvePrivateKey, exp: Any
) -> None:
    if isinstance(exp, int) and not isinstance(exp, bool) and exp <= 0:
        exp = int(time.time()) + exp
    assert_refused_before_any_fetch(backend, endpoint, alb_token(key, header={"exp": exp}))


def test_the_header_exp_follows_the_clock(endpoint: KeyEndpoint, key: ec.EllipticCurvePrivateKey) -> None:
    clock = Clock()
    backend = make_backend(endpoint, clock=clock)
    token = alb_token(key, lifetime=600)
    assert principal_of(backend, ("x-amzn-oidc-data", token)).id == EMAIL
    clock.now += 601
    assert rejection(backend, ("x-amzn-oidc-data", token)).status_code == 401


def test_an_expired_payload_exp_is_refused(
    backend: AlbOidcAuthBackend, endpoint: KeyEndpoint, key: ec.EllipticCurvePrivateKey
) -> None:
    token = alb_token(key, claims={"exp": int(time.time()) - 10})
    response = whoami_client(backend).get("/whoami", headers=data(token))
    assert (response.status_code, response.json()) == (401, DENIED)
    assert endpoint.urls == [KEY_URL]  # the header passed, the payload did not


@pytest.mark.parametrize("kid", ["../x", "", "-abc", "a/b", "a.b", "a?b", "a b", "k" * 129, "abc\n", DROP])
def test_a_bad_kid_is_refused_before_any_fetch(
    backend: AlbOidcAuthBackend, endpoint: KeyEndpoint, key: ec.EllipticCurvePrivateKey, kid: Any
) -> None:
    assert_refused_before_any_fetch(backend, endpoint, alb_token(key, header={"kid": kid}))


def test_a_kid_that_is_not_a_string_is_refused_before_any_fetch(
    backend: AlbOidcAuthBackend, endpoint: KeyEndpoint, key: ec.EllipticCurvePrivateKey
) -> None:
    now = int(time.time())
    header = {"alg": "ES256", "kid": 123, "signer": ALB_ARN, "exp": now + 120}
    token = unsigned_token(header, {"email": EMAIL, "exp": now + 120}, signature="c2ln")
    assert_refused_before_any_fetch(backend, endpoint, token)


def test_other_algorithms_are_refused_before_any_fetch(
    backend: AlbOidcAuthBackend, endpoint: KeyEndpoint
) -> None:
    now = int(time.time())
    header = {"kid": KID, "signer": ALB_ARN, "exp": now + 120}
    payload = {"sub": SUB, "email": EMAIL, "exp": now + 120}
    secret = "a-shared-secret-of-at-least-32-bytes!"
    tokens = [
        jwt.encode(payload, secret, algorithm="HS256", headers=header),
        jwt.encode(payload, new_key(ec.SECP384R1()), algorithm="ES384", headers=header),
        unsigned_token({**header, "alg": "none"}, payload),
        unsigned_token({**header, "alg": "ES256 "}, payload),
        unsigned_token(header, payload),  # no alg
    ]
    for token in tokens:
        assert_refused_before_any_fetch(backend, endpoint, token)


def test_a_token_signed_by_another_key_is_refused(
    backend: AlbOidcAuthBackend, endpoint: KeyEndpoint, key: ec.EllipticCurvePrivateKey
) -> None:
    client = whoami_client(backend)
    for token in (alb_token(new_key()), token_signed_padded(new_key())):
        response = client.get("/whoami", headers=data(token))
        assert (response.status_code, response.json()) == (401, DENIED)


def test_a_tampered_payload_is_refused(backend: AlbOidcAuthBackend, key: ec.EllipticCurvePrivateKey) -> None:
    header, _, signature = alb_token(key).split(".")
    forged = segment({"sub": SUB, "email": "admin@example.com", "exp": int(time.time()) + 120})
    assert rejection(backend, ("x-amzn-oidc-data", f"{header}.{forged}.{signature}")).status_code == 401


@pytest.mark.parametrize(
    "headers",
    [
        pytest.param({}, id="no header"),
        pytest.param({"x-amzn-oidc-data": ""}, id="empty"),
        pytest.param({"x-amzn-oidc-data": "not-a-jwt"}, id="not a JWT"),
        pytest.param({"x-amzn-oidc-data": "a.b.c"}, id="garbage segments"),
        pytest.param({"x-amzn-oidc-data": "x" * (16 * 1024 + 1)}, id="too long"),
        pytest.param({"Authorization": "Bearer x"}, id="a bearer token"),
    ],
)
def test_malformed_headers_are_refused(
    backend: AlbOidcAuthBackend, endpoint: KeyEndpoint, headers: dict[str, str]
) -> None:
    response = whoami_client(backend).get("/whoami", headers=headers)
    assert (response.status_code, response.json()) == (401, DENIED)
    assert endpoint.urls == []


def test_two_data_headers_are_refused(
    backend: AlbOidcAuthBackend, endpoint: KeyEndpoint, key: ec.EllipticCurvePrivateKey
) -> None:
    token = alb_token(key)
    response = whoami_client(backend).get(
        "/whoami", headers=[("x-amzn-oidc-data", token), ("x-amzn-oidc-data", token)]
    )
    assert (response.status_code, response.json()) == (401, DENIED)
    assert endpoint.urls == []


def test_claims_and_tokens_are_never_logged(
    backend: AlbOidcAuthBackend, key: ec.EllipticCurvePrivateKey, caplog: pytest.LogCaptureFixture
) -> None:
    caplog.set_level(logging.DEBUG, logger="dag_healer")
    client = whoami_client(backend)
    tokens = [
        alb_token(key),
        alb_token(key, header={"signer": OTHER_ARN}),
        alb_token(key, claims={"exp": int(time.time()) - 10}),
        alb_token(new_key()),
    ]
    for token in tokens:
        client.get("/whoami", headers=data(token))
    assert len(caplog.records) == 3  # one debug line per refused token
    for secret in (EMAIL, SUB, KID, ALB_ARN, OTHER_ARN, *tokens, *(strip_jwt_padding(t) for t in tokens)):
        assert secret not in caplog.text


# =========================================================================== the key endpoint


def test_a_key_is_fetched_once_per_kid(
    backend: AlbOidcAuthBackend, endpoint: KeyEndpoint, key: ec.EllipticCurvePrivateKey
) -> None:
    client = whoami_client(backend)
    for _ in range(3):
        assert client.get("/whoami", headers=data(alb_token(key))).status_code == 200
    assert endpoint.urls == [KEY_URL]


def test_the_key_cache_is_bounded(monkeypatch: pytest.MonkeyPatch, key: ec.EllipticCurvePrivateKey) -> None:
    monkeypatch.setattr(alb_oidc, "KEY_CACHE_SIZE", 2)
    kids = ["kid-a", "kid-b", "kid-c"]
    endpoint = KeyEndpoint({kid: public_pem(key) for kid in kids})
    backend = make_backend(endpoint)
    for kid in [*kids, "kid-c", "kid-a"]:
        assert principal_of(backend, ("x-amzn-oidc-data", alb_token(key, header={"kid": kid}))).id == EMAIL
    assert [url.rsplit("/", 1)[-1] for url in endpoint.urls] == ["kid-a", "kid-b", "kid-c", "kid-a"]


def test_a_refused_kid_is_401_and_is_not_asked_for_again_for_a_while(key: ec.EllipticCurvePrivateKey) -> None:
    clock = Clock()
    endpoint = KeyEndpoint({})  # every kid: 404
    backend = make_backend(endpoint, clock=clock)
    token = alb_token(key, lifetime=600)
    for _ in range(2):
        refused = rejection(backend, ("x-amzn-oidc-data", token))
        assert (refused.status_code, refused.detail) == (401, "Not authenticated")
    assert endpoint.urls == [KEY_URL]
    clock.now += FAILURE_TTL + 1
    endpoint.keys[KID] = public_pem(key)
    assert principal_of(backend, ("x-amzn-oidc-data", token)).id == EMAIL
    assert endpoint.urls == [KEY_URL, KEY_URL]


@pytest.mark.parametrize(
    "error",
    [
        pytest.param(SigningKeyUnavailable("HTTP 503"), id="5xx"),
        pytest.param(SigningKeyUnavailable("ConnectError"), id="network"),
        pytest.param(RuntimeError("boom"), id="any other error"),
    ],
)
def test_an_unreachable_key_endpoint_is_503(key: ec.EllipticCurvePrivateKey, error: BaseException) -> None:
    clock = Clock()
    endpoint = KeyEndpoint(error=error)
    backend = make_backend(endpoint, clock=clock)
    response = whoami_client(backend).get("/whoami", headers=data(alb_token(key, lifetime=600)))
    assert (response.status_code, response.json()) == (503, {"detail": KEY_UNAVAILABLE_DETAIL})
    assert rejected_status(backend, alb_token(key, lifetime=600)) == 503
    assert endpoint.urls == [KEY_URL]  # remembered: not asked again at once
    clock.now += FAILURE_TTL + 1
    assert rejected_status(backend, alb_token(key, lifetime=600)) == 503
    assert endpoint.urls == [KEY_URL, KEY_URL]


@pytest.mark.parametrize(
    "document",
    [
        pytest.param("not a key", id="not PEM"),
        pytest.param("-----BEGIN PUBLIC KEY-----\nAAAA\n-----END PUBLIC KEY-----", id="broken PEM"),
        pytest.param(public_pem(new_key(ec.SECP384R1())), id="P-384"),
    ],
)
def test_a_key_document_that_is_not_a_p256_key_is_503(key: ec.EllipticCurvePrivateKey, document: str) -> None:
    backend = make_backend(KeyEndpoint({KID: document}))
    assert rejected_status(backend, alb_token(key)) == 503


def test_new_kids_are_fetched_within_a_budget(
    monkeypatch: pytest.MonkeyPatch, key: ec.EllipticCurvePrivateKey, caplog: pytest.LogCaptureFixture
) -> None:
    monkeypatch.setattr(alb_oidc, "MAX_KEY_FETCHES", 2)
    clock = Clock()
    endpoint = KeyEndpoint({kid: public_pem(key) for kid in ("kid-a", "kid-c")})
    backend = make_backend(endpoint, clock=clock)

    def status(kid: str) -> int:
        try:
            principal_of(backend, ("x-amzn-oidc-data", alb_token(key, header={"kid": kid}, lifetime=600)))
        except HTTPException as exc:
            return exc.status_code
        return 200

    with caplog.at_level(logging.WARNING, logger="dag_healer"):
        statuses = [status(kid) for kid in ("kid-a", "kid-x", "kid-c", "kid-c", "kid-a")]
    assert statuses == [200, 401, 503, 503, 200]  # kid-x is unknown: 404, a fetch all the same
    assert [url.rsplit("/", 1)[-1] for url in endpoint.urls] == ["kid-a", "kid-x"]  # none past the budget
    assert caplog.text.count("signing keys were fetched") == 1  # warned once per window
    clock.now += KEY_FETCH_WINDOW + 1
    assert status("kid-c") == 200  # the refusal was not remembered as a failure of kid-c
    assert [url.rsplit("/", 1)[-1] for url in endpoint.urls] == ["kid-a", "kid-x", "kid-c"]


class BlockingEndpoint(KeyEndpoint):
    """A key endpoint that answers only once ``release`` is set; ``entered`` tells it was asked."""

    def __init__(self, keys: Mapping[str, str]) -> None:
        super().__init__(keys)
        self.entered = threading.Event()
        self.release = threading.Event()

    def __call__(self, url: str) -> str:
        self.entered.set()
        assert self.release.wait(10)
        return super().__call__(url)


def test_one_key_is_fetched_at_a_time(key: ec.EllipticCurvePrivateKey) -> None:
    endpoint = BlockingEndpoint({KID: public_pem(key), "kid-b": public_pem(key)})
    backend = make_backend(endpoint)
    results: dict[str, Any] = {}

    def sign_in(name: str, token: str) -> None:
        try:
            results[name] = principal_of(backend, ("x-amzn-oidc-data", token)).id
        except HTTPException as exc:
            results[name] = exc.status_code

    first = threading.Thread(target=sign_in, args=("first", alb_token(key)))
    first.start()
    assert endpoint.entered.wait(10)
    sign_in("other kid", alb_token(key, header={"kid": "kid-b"}))
    assert results["other kid"] == 503  # another new kid while a fetch runs: at once, no fetch
    second = threading.Thread(target=sign_in, args=("same kid", alb_token(key)))
    second.start()
    time.sleep(0.05)  # it waits for the running fetch of its kid
    endpoint.release.set()
    first.join(10)
    second.join(10)
    assert results["first"] == results["same kid"] == EMAIL
    assert endpoint.urls == [KEY_URL]  # one fetch for both
    sign_in("other kid", alb_token(key, header={"kid": "kid-b"}))
    assert results["other kid"] == EMAIL  # the 503 was not remembered as a failure of kid-b


def test_waiting_for_a_running_fetch_is_bounded(
    monkeypatch: pytest.MonkeyPatch, key: ec.EllipticCurvePrivateKey
) -> None:
    monkeypatch.setattr(alb_oidc, "KEY_FETCH_TIMEOUT", 0.05)
    endpoint = BlockingEndpoint({KID: public_pem(key)})
    backend = make_backend(endpoint)
    first = threading.Thread(target=principal_of, args=(backend, ("x-amzn-oidc-data", alb_token(key))))
    first.start()
    assert endpoint.entered.wait(10)
    assert rejected_status(backend, alb_token(key)) == 503  # gave up waiting
    endpoint.release.set()
    first.join(10)
    assert principal_of(backend, ("x-amzn-oidc-data", alb_token(key))).id == EMAIL
    assert endpoint.urls == [KEY_URL]


def test_load_public_key() -> None:
    key = new_key()
    loaded = load_public_key(public_pem(key))
    assert loaded.public_numbers() == key.public_key().public_numbers()
    private_pem = key.private_bytes(
        serialization.Encoding.PEM, serialization.PrivateFormat.PKCS8, serialization.NoEncryption()
    ).decode()
    for document in (private_pem, b"bytes", None):
        with pytest.raises(SigningKeyUnavailable):
            load_public_key(document)


# =========================================================================== URLs and padding


def test_alb_key_url() -> None:
    assert alb_key_url("eu-west-1", KID) == KEY_URL
    assert alb_key_url("us-east-2", "abc") == "https://public-keys.auth.elb.us-east-2.amazonaws.com/abc"
    assert alb_key_url("us-gov-west-1", KID) == (
        f"https://s3-us-gov-west-1.amazonaws.com/aws-elb-public-keys-prod-us-gov-west-1/{KID}"
    )
    assert alb_key_url("us-gov-east-1", "abc") == (
        "https://s3-us-gov-east-1.amazonaws.com/aws-elb-public-keys-prod-us-gov-east-1/abc"
    )
    for region, kid in (
        ("eu-west-1.evil.example", KID),
        ("EU-WEST-1", KID),
        ("eu-west-1", "../x"),
        ("eu-west-1", "a\n"),
    ):
        with pytest.raises(ValueError):
            alb_key_url(region, kid)


def test_a_govcloud_backend_fetches_from_s3(key: ec.EllipticCurvePrivateKey) -> None:
    endpoint = KeyEndpoint({KID: public_pem(key)})
    backend = AlbOidcAuthBackend(
        alb_arn=GOV_ARN, region="us-gov-west-1", authorizer=AUTHORIZER, key_fetcher=endpoint
    )
    token = alb_token(key, header={"signer": GOV_ARN})
    assert principal_of(backend, ("x-amzn-oidc-data", token)).id == EMAIL
    assert endpoint.urls == [
        f"https://s3-us-gov-west-1.amazonaws.com/aws-elb-public-keys-prod-us-gov-west-1/{KID}"
    ]


@pytest.mark.parametrize(
    ("token", "expected"),
    [("ab==.cd=.ef", "ab.cd.ef"), ("ab.cd.ef", "ab.cd.ef"), ("a=b.c", "a=b.c"), ("", ""), ("x==", "x")],
)
def test_strip_jwt_padding(token: str, expected: str) -> None:
    assert strip_jwt_padding(token) == expected


# =========================================================================== fetch_public_key


def answer(status: int, body: str | bytes = b"", headers: Mapping[str, str] | None = None) -> httpx.Response:
    """A response whose body is still to be streamed, as a network transport gives it."""
    content = body.encode() if isinstance(body, str) else body
    return httpx.Response(status, headers=headers, stream=httpx.ByteStream(content))


@pytest.fixture
def key_server(monkeypatch: pytest.MonkeyPatch) -> SimpleNamespace:
    """Route the httpx client of ``fetch_public_key`` to ``server.handler``; record its options."""
    server = SimpleNamespace(options=[], requests=[], handler=None)
    real_client = httpx.Client

    def client(**options: Any) -> httpx.Client:
        server.options.append(options)

        def handle(request: httpx.Request) -> httpx.Response:
            server.requests.append(request)
            return server.handler(request)

        # trust_env off: a proxy from the environment would otherwise bypass the mock transport
        return real_client(**{**options, "transport": httpx.MockTransport(handle), "trust_env": False})

    monkeypatch.setattr(httpx, "Client", client)
    return server


def test_fetch_public_key_gets_the_pem_without_redirects(key_server: SimpleNamespace) -> None:
    pem = public_pem(new_key())
    key_server.handler = lambda request: answer(200, pem + "\n")
    assert fetch_public_key(KEY_URL) == pem.strip()
    assert key_server.options == [{"follow_redirects": False, "verify": True, "timeout": 5.0}]
    (request,) = key_server.requests
    assert (request.method, str(request.url)) == ("GET", KEY_URL)
    assert request.headers["accept-encoding"] == "identity"


@pytest.mark.parametrize(
    ("status", "body", "headers", "error"),
    [
        pytest.param(404, "", None, SigningKeyNotFound, id="404"),
        pytest.param(403, "", None, SigningKeyNotFound, id="403 (S3)"),
        pytest.param(408, "", None, SigningKeyUnavailable, id="408 (try later)"),
        pytest.param(429, "", {"Retry-After": "1"}, SigningKeyUnavailable, id="429 (throttled)"),
        pytest.param(500, "", None, SigningKeyUnavailable, id="500"),
        pytest.param(503, "", None, SigningKeyUnavailable, id="503"),
        pytest.param(204, "", None, SigningKeyUnavailable, id="204"),
        pytest.param(302, "", {"Location": "https://evil.example/key"}, SigningKeyUnavailable, id="redirect"),
        pytest.param(200, "<html>maintenance</html>", None, SigningKeyUnavailable, id="not PEM"),
        pytest.param(200, "-----BEGIN PUBLIC KEY----- é".encode(), None, SigningKeyUnavailable, id="UTF-8"),
        pytest.param(
            200,
            "-----BEGIN PUBLIC KEY-----\n" + "A" * (16 * 1024) + "\n-----END PUBLIC KEY-----",
            None,
            SigningKeyUnavailable,
            id="too large",
        ),
    ],
)
def test_fetch_public_key_failures(
    key_server: SimpleNamespace,
    status: int,
    body: str | bytes,
    headers: dict[str, str] | None,
    error: type[Exception],
) -> None:
    key_server.handler = lambda request: answer(status, body, headers)
    with pytest.raises(error):
        fetch_public_key(KEY_URL)
    assert len(key_server.requests) == 1  # a redirect is never followed


def test_fetch_public_key_network_errors(key_server: SimpleNamespace) -> None:
    def refuse(request: httpx.Request) -> httpx.Response:
        raise httpx.ConnectError("connection refused", request=request)

    key_server.handler = refuse
    with pytest.raises(SigningKeyUnavailable, match="ConnectError"):
        fetch_public_key(KEY_URL)

    def slow(request: httpx.Request) -> httpx.Response:
        raise httpx.ReadTimeout("timed out", request=request)

    key_server.handler = slow
    with pytest.raises(SigningKeyUnavailable, match="ReadTimeout"):
        fetch_public_key(KEY_URL, timeout=0.5)
    assert key_server.options[-1]["timeout"] == 0.5


def test_the_backend_fetches_with_fetch_public_key_by_default(
    key_server: SimpleNamespace, key: ec.EllipticCurvePrivateKey
) -> None:
    key_server.handler = lambda request: answer(200, public_pem(key))
    backend = AlbOidcAuthBackend(alb_arn=ALB_ARN, region=REGION, authorizer=AUTHORIZER)
    assert principal_of(backend, ("x-amzn-oidc-data", alb_token(key))).id == EMAIL
    assert principal_of(backend, ("x-amzn-oidc-data", alb_token(key))).id == EMAIL
    assert [str(request.url) for request in key_server.requests] == [KEY_URL]


# =========================================================================== configuration


def partition_arn(partition: str, region: str) -> dict[str, str]:
    """The ``alb_arn`` and ``region`` settings of a load balancer in ``partition``."""
    arn = f"arn:{partition}:elasticloadbalancing:{region}:123456789012:loadbalancer/app/healer/0123abcd"
    return {"alb_arn": arn, "region": region}


def test_the_alb_backend_is_an_auth_backend(backend: AlbOidcAuthBackend) -> None:
    assert isinstance(backend, AuthBackend)
    assert backend.name == "alb_oidc"
    assert (backend.alb_arn, backend.region) == (ALB_ARN, REGION)
    assert backend.authorizer() is AUTHORIZER
    assert backend.allowed_origins() == [PUBLIC_URL]


@pytest.mark.parametrize(
    ("options", "problem"),
    [
        ({"alb_arn": ""}, "the alb_oidc backend requires serve.auth.alb_arn"),
        ({"alb_arn": None}, "the alb_oidc backend requires serve.auth.alb_arn"),
        ({"alb_arn": "healer-alb"}, "requires serve.auth.alb_arn"),
        (
            {"alb_arn": "arn:aws:elasticloadbalancing:eu-west-1:1:loadbalancer/net/x/y"},
            "requires serve.auth.alb_arn",
        ),
        ({"region": ""}, "the alb_oidc backend requires serve.auth.region, an AWS region such as eu-west-1"),
        ({"region": "eu-west-1.evil.example"}, "requires serve.auth.region"),
        (
            {"region": "us-east-1"},
            "serve.auth.region 'us-east-1' is not the region of serve.auth.alb_arn ('eu-west-1')",
        ),
        ({"user_claim": " "}, "serve.auth.user_claim must name a claim"),
        ({"groups_claim": ""}, "serve.auth.groups_claim must name a claim"),
        (
            partition_arn("aws-cn", "cn-north-1"),
            "serve.auth.alb_arn is in the 'aws-cn' partition: the alb_oidc backend knows the signing "
            "key endpoints of the aws and aws-us-gov partitions only",
        ),
        (partition_arn("aws-iso", "us-iso-east-1"), "serve.auth.alb_arn is in the 'aws-iso' partition"),
        (
            partition_arn("aws", "us-gov-west-1"),
            "serve.auth.alb_arn: the 'aws' partition has no region 'us-gov-west-1'",
        ),
        (
            partition_arn("aws-us-gov", "eu-west-1"),
            "serve.auth.alb_arn: the 'aws-us-gov' partition has no region 'eu-west-1'",
        ),
    ],
)
def test_invalid_settings_are_refused(options: dict[str, Any], problem: str) -> None:
    settings: dict[str, Any] = {"alb_arn": ALB_ARN, "region": REGION, "authorizer": AUTHORIZER}
    settings.update(options)
    with pytest.raises(AuthConfigError) as excinfo:
        AlbOidcAuthBackend(**settings)
    assert problem in str(excinfo.value)


def test_pyjwt_with_its_crypto_backend_is_required(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(jwt.algorithms, "has_crypto", False)
    with pytest.raises(AuthConfigError) as excinfo:
        AlbOidcAuthBackend(alb_arn=ALB_ARN, region=REGION, authorizer=AUTHORIZER)
    assert str(excinfo.value) == PYJWT_HINT
    assert "pip install 'dag-healer[serve]'" in PYJWT_HINT
    monkeypatch.setattr(jwt.algorithms, "has_crypto", True)
    monkeypatch.setitem(sys.modules, "jwt", None)
    monkeypatch.setitem(sys.modules, "jwt.algorithms", None)
    with pytest.raises(AuthConfigError, match="needs PyJWT with its crypto extra"):
        AlbOidcAuthBackend(alb_arn=ALB_ARN, region=REGION, authorizer=AUTHORIZER)


def test_the_factory_builds_it_from_serve_auth(
    healer_env: Path, monkeypatch: pytest.MonkeyPatch, key: ec.EllipticCurvePrivateKey
) -> None:
    endpoint = KeyEndpoint({KID: public_pem(key)})
    monkeypatch.setattr(alb_oidc, "fetch_public_key", endpoint)
    cfg = config_from_mapping(
        {
            "serve": {
                "public_url": PUBLIC_URL,
                "auth": {
                    "backend": "alb_oidc",
                    "alb_arn": ALB_ARN,  # the region comes from the ARN
                    "user_claim": "username",
                    "groups_claim": "cognito:groups",
                },
            }
        }
    )
    backend = build_auth_backend(cfg, authorizer=AUTHORIZER, bind_host="0.0.0.0")
    assert isinstance(backend, AlbOidcAuthBackend)
    assert (backend.alb_arn, backend.region) == (ALB_ARN, REGION)
    assert backend.authorizer() is AUTHORIZER
    assert backend.allowed_origins() == [PUBLIC_URL]
    token = alb_token(key, claims={"username": "alice", "cognito:groups": ["data-eng"]})
    response = whoami_client(backend).get("/whoami", headers=data(token))
    assert response.json() == {"id": "alice", "name": "alice", "groups": ["data-eng"]}
    assert endpoint.urls == [KEY_URL]
