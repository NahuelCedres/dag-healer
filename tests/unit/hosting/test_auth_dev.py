"""The ``dev`` backend, the backend factory and what the standalone backends share (section 7.3).

* ``is_loopback_host`` and the dev backend's refusal of a public bind address, also through
  :func:`build_auth_backend`;
* the dev user, through the same ``dependency_overrides`` wiring as ``dag-healer serve``;
* the factory: names, origins (``$HEALER_PUBLIC_URL`` and ``serve.public_url``), an unknown
  backend, and a light import (PyJWT only for ``alb_oidc``, never the host modules);
* ``base``: the 401 helper, name cleaning and the backend base class.
"""

from __future__ import annotations

import json
import os
import subprocess
import sys
from pathlib import Path
from types import SimpleNamespace
from typing import Any, get_args

import pytest
from fastapi import Depends, FastAPI
from fastapi.testclient import TestClient

from dag_healer.api import auth
from dag_healer.api.auth import AuthBackend, Principal, apply_auth_backend
from dag_healer.config import ConfigError, HealerConfig, ServeAuthBackend, config_from_mapping
from dag_healer.hosting.auth import BACKEND_NAMES, AuthConfigError, StandaloneAuthBackend, build_auth_backend
from dag_healer.hosting.auth.base import (
    MAX_NAME_LENGTH,
    NOT_AUTHENTICATED,
    clean_name,
    not_authenticated,
    standalone_origins,
)
from dag_healer.hosting.auth.dev import DEV_PRINCIPAL, DevAuthBackend, is_loopback_host

SRC = Path(__file__).resolve().parents[3] / "src"
#: The backends only keep the host's authorizer and hand it back.
AUTHORIZER: Any = object()
PUBLIC_URL = "https://healer.example.com"
PROXY_URL = "https://proxy.example.com"


def whoami_client(backend: AuthBackend, **kwargs: Any) -> TestClient:
    """An app whose route depends on ``get_current_user``, with ``backend`` applied like ``serve``."""
    app = FastAPI()

    @app.get("/whoami")
    def whoami(user: Any = Depends(auth.get_current_user)) -> dict[str, Any]:
        return {"id": user.id, "name": user.name, "groups": sorted(user.groups)}

    apply_auth_backend(app, backend)
    return TestClient(app, **kwargs)


def config(serve: dict[str, Any]) -> HealerConfig:
    return config_from_mapping({"serve": serve})


# =========================================================================== loopback


@pytest.mark.parametrize(
    "host",
    ["127.0.0.1", "127.1.2.3", "::1", "[::1]", "localhost", "LOCALHOST", " localhost ", "::ffff:127.0.0.1"],
)
def test_loopback_hosts(host: str) -> None:
    assert is_loopback_host(host) is True


@pytest.mark.parametrize(
    "host",
    [
        "0.0.0.0",
        "::",
        "[::]",
        "192.168.1.5",
        "10.0.0.1",
        "example.com",
        "localhost.",
        "[localhost]",
        "",
        "127.1",
    ],
)
def test_other_hosts_are_not_loopback(host: str) -> None:
    assert is_loopback_host(host) is False


def test_a_host_that_is_not_a_string_is_not_loopback() -> None:
    assert is_loopback_host(None) is False  # type: ignore[arg-type]


# =========================================================================== the dev backend


@pytest.mark.parametrize("host", ["0.0.0.0", "::", "192.168.1.5", ""])
def test_dev_refuses_a_bind_address_that_is_not_loopback(host: str) -> None:
    with pytest.raises(AuthConfigError) as excinfo:
        DevAuthBackend(bind_host=host, authorizer=AUTHORIZER)
    message = str(excinfo.value)
    assert "the dev backend serves every request as the user 'dev' (group admin)" in message
    assert "bind it to a loopback address such as 127.0.0.1" in message
    assert repr(host) in message
    assert isinstance(excinfo.value, ConfigError) and isinstance(excinfo.value, ValueError)


def test_every_request_is_the_dev_user_in_group_admin() -> None:
    backend = DevAuthBackend(bind_host="127.0.0.1", authorizer=AUTHORIZER)
    client = whoami_client(backend)
    expected = {"id": "dev", "name": "dev", "groups": ["admin"]}
    assert client.get("/whoami").json() == expected
    # It reads no header: nothing a client sends changes the user.
    headers = {"X-Forwarded-User": "mallory", "Authorization": "Bearer x", "x-amzn-oidc-data": "x.y.z"}
    assert client.get("/whoami", headers=headers).json() == expected
    assert DEV_PRINCIPAL == Principal(id="dev", name="dev", groups=frozenset({"admin"}))
    assert auth.user_name(DEV_PRINCIPAL) == "dev"


def test_the_dev_backend_is_an_auth_backend() -> None:
    backend = DevAuthBackend(bind_host="localhost", authorizer=AUTHORIZER, origins=[PUBLIC_URL])
    assert isinstance(backend, AuthBackend)
    assert isinstance(backend, StandaloneAuthBackend)
    assert backend.name == "dev"
    assert backend.authorizer() is AUTHORIZER
    assert backend.authorizer() is backend.authorizer()
    assert backend.allowed_origins() == [PUBLIC_URL]
    assert backend.user_dependency() is backend.user_dependency()


# =========================================================================== the factory


def test_the_factory_builds_dev_only_for_a_loopback_bind(
    healer_env: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setenv("HEALER_PUBLIC_URL", PROXY_URL)
    cfg = config({"public_url": PUBLIC_URL, "auth": {"backend": "dev"}})
    with pytest.raises(AuthConfigError, match="loopback"):
        build_auth_backend(cfg, authorizer=AUTHORIZER, bind_host="0.0.0.0")
    backend = build_auth_backend(cfg, authorizer=AUTHORIZER, bind_host="127.0.0.1")
    assert isinstance(backend, DevAuthBackend)
    assert backend.authorizer() is AUTHORIZER
    assert backend.allowed_origins() == [PROXY_URL, PUBLIC_URL]
    assert whoami_client(backend).get("/whoami").json()["id"] == "dev"


def test_the_factory_checks_serve_host_without_a_bind_host(healer_env: Path) -> None:
    with pytest.raises(AuthConfigError, match="not '0.0.0.0'"):
        build_auth_backend(config({"host": "0.0.0.0"}), authorizer=AUTHORIZER)
    backend = build_auth_backend(config({}), authorizer=AUTHORIZER)  # serve.host 127.0.0.1, backend dev
    assert isinstance(backend, DevAuthBackend) and backend.bind_host == "127.0.0.1"
    with pytest.raises(AuthConfigError):  # an explicit bind host wins, even an empty one
        build_auth_backend(config({}), authorizer=AUTHORIZER, bind_host="")


def test_backend_names_are_the_configuration_values() -> None:
    assert BACKEND_NAMES == ("dev", "token", "trusted_header", "alb_oidc")
    assert set(BACKEND_NAMES) == set(get_args(ServeAuthBackend))


def test_the_factory_refuses_an_unknown_backend() -> None:
    cfg: Any = SimpleNamespace(
        serve=SimpleNamespace(host="127.0.0.1", public_url=None, auth=SimpleNamespace(backend="kerberos"))
    )
    with pytest.raises(AuthConfigError) as excinfo:
        build_auth_backend(cfg, authorizer=AUTHORIZER)
    assert str(excinfo.value) == (
        "unknown serve.auth.backend 'kerberos': expected one of dev, token, trusted_header, alb_oidc"
    )


def test_importing_and_building_load_neither_pyjwt_nor_the_host_modules() -> None:
    digest = "9f86d081884c7d659a2feaa0c55ad015a3bf4f1b2b0b822cd15d6c15b0f00a08"
    code = f"""
import json, sys
import dag_healer.hosting.auth as hosting_auth
from dag_healer.config import config_from_mapping

def loaded():
    return sorted(
        name for name in sys.modules
        if name.split(".")[0] == "jwt" or name.startswith("dag_healer.hosting")
    )

report = {{"import": loaded()}}
backends = {{
    "dev": {{"backend": "dev"}},
    "token": {{"backend": "token", "tokens": [{{"name": "ci", "sha256": "{digest}"}}]}},
    "trusted_header": {{"backend": "trusted_header", "trusted_proxies": ["10.0.0.0/8"]}},
}}
for name, settings in backends.items():
    cfg = config_from_mapping({{"serve": {{"auth": settings}}}})
    assert hosting_auth.build_auth_backend(cfg, authorizer=object()).name == name
report["built"] = loaded()
print(json.dumps(report))
"""
    env = dict(os.environ)
    env["PYTHONPATH"] = os.pathsep.join([str(SRC), env.get("PYTHONPATH", "")]).rstrip(os.pathsep)
    env.pop("HEALER_PUBLIC_URL", None)
    result = subprocess.run(
        [sys.executable, "-c", code], capture_output=True, text=True, env=env, timeout=120, check=False
    )
    assert result.returncode == 0, result.stderr
    report = json.loads(result.stdout)
    assert report["import"] == [
        "dag_healer.hosting",
        "dag_healer.hosting.auth",
        "dag_healer.hosting.auth.base",
    ]
    assert report["built"] == [
        "dag_healer.hosting",
        "dag_healer.hosting.auth",
        "dag_healer.hosting.auth.base",
        "dag_healer.hosting.auth.dev",
        "dag_healer.hosting.auth.token",
        "dag_healer.hosting.auth.trusted_header",
    ]


# =========================================================================== base


def test_standalone_origins(healer_env: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    assert standalone_origins(config({})) == []
    assert standalone_origins(config({"public_url": PUBLIC_URL})) == [PUBLIC_URL]
    monkeypatch.setenv("HEALER_PUBLIC_URL", f" {PUBLIC_URL} ")
    assert standalone_origins(config({"public_url": PUBLIC_URL})) == [PUBLIC_URL]
    monkeypatch.setenv("HEALER_PUBLIC_URL", PROXY_URL)
    assert standalone_origins(config({"public_url": PUBLIC_URL})) == [PROXY_URL, PUBLIC_URL]
    monkeypatch.setenv("HEALER_PUBLIC_URL", "  ")
    assert standalone_origins(config({})) == []
    monkeypatch.setenv("AIRFLOW__API__BASE_URL", "https://airflow.example.com")
    assert standalone_origins(config({})) == []  # the Airflow host's URL is not a standalone origin


def test_not_authenticated_is_the_same_401_for_every_backend() -> None:
    plain = not_authenticated()
    assert (plain.status_code, plain.detail, plain.headers) == (401, NOT_AUTHENTICATED, None)
    bearer = not_authenticated(bearer=True)
    assert (bearer.status_code, bearer.detail) == (401, "Not authenticated")
    assert bearer.headers == {"WWW-Authenticate": "Bearer"}


@pytest.mark.parametrize(
    ("value", "expected"),
    [
        ("alice", "alice"),
        ("  alice@example.com ", "alice@example.com"),
        ("Data Eng", "Data Eng"),
        ("x" * MAX_NAME_LENGTH, "x" * MAX_NAME_LENGTH),
        ("x" * (MAX_NAME_LENGTH + 1), None),
        ("", None),
        ("   ", None),
        ("alice\nadmin", None),
        ("alice\x00", None),
        (42, None),
        (None, None),
        (["alice"], None),
    ],
)
def test_clean_name(value: Any, expected: str | None) -> None:
    assert clean_name(value) == expected


def test_the_base_backend_needs_an_authorizer_and_a_list_of_origins() -> None:
    with pytest.raises(TypeError, match="an Authorizer is required"):
        StandaloneAuthBackend(authorizer=None)  # type: ignore[arg-type]
    with pytest.raises(TypeError, match="not a string"):
        StandaloneAuthBackend(authorizer=AUTHORIZER, origins=PUBLIC_URL)
    base = StandaloneAuthBackend(authorizer=AUTHORIZER, origins=(PUBLIC_URL, PROXY_URL))
    assert base.allowed_origins() == [PUBLIC_URL, PROXY_URL]
    base.allowed_origins().append("https://evil.example")  # a copy: the backend keeps its own
    assert base.allowed_origins() == [PUBLIC_URL, PROXY_URL]
    with pytest.raises(NotImplementedError):
        base.user_dependency()
