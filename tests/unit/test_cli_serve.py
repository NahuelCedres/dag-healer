"""``dag-healer serve`` (``cli/serve_cmd.py``; sections 7.3, 7.7 and 11.7 of
``docs/architecture/orchestrators.md``).

``serve_cmd.run_server`` is replaced by a recorder, so no server starts: the tests check what
would be served, with which address, and why the command refuses to start.
"""

from __future__ import annotations

import hashlib
import os
import shutil
import sys
import types
from collections.abc import Iterator
from pathlib import Path
from typing import Any

import pytest
import yaml
from fastapi.testclient import TestClient

from dag_healer.api import deps
from dag_healer.cli import serve_cmd
from dag_healer.cli.main import COMMANDS, build_parser, main
from dag_healer.orchestrators import registry
from dag_healer.store import db
from dag_healer.store.migrations import current_version
from dag_healer.store.models import SCHEMA_VERSION

V1_STORE = Path(__file__).parent / "fixtures" / "store" / "healer_v1.db"
CI_TOKEN_SHA256 = hashlib.sha256(b"ci-token").hexdigest()
TOKEN_AUTH: dict[str, Any] = {
    "backend": "token",
    "tokens": [{"name": "ci", "sha256": CI_TOKEN_SHA256, "groups": ["bots"]}],
    "rules": [{"principals": ["group:bots"], "pipelines": ["*"], "actions": ["read"]}],
}


@pytest.fixture
def served(healer_env: Path, monkeypatch: pytest.MonkeyPatch) -> Iterator[list[dict[str, Any]]]:
    """The calls ``serve`` makes to ``run_server`` (recorded instead of serving)."""
    calls: list[dict[str, Any]] = []

    def record(app: Any, *, host: str, port: int, log_level: str = "info") -> None:
        calls.append({"app": app, "host": host, "port": port, "log_level": log_level})

    monkeypatch.setattr(serve_cmd, "run_server", record)
    db.dispose_engines()
    deps.reset_config_cache()
    registry.reset_adapters()
    yield calls
    deps.reset_config_cache()
    registry.reset_adapters()
    db.dispose_engines()


def write_config(
    directory: Path,
    *,
    auth: dict[str, Any] | None = None,
    orchestrator: str = "fake",
    **settings: Any,
) -> Path:
    """A healer.yaml for a Fakeflow instance with the given ``serve`` settings (dev backend by default)."""
    directory.mkdir(parents=True, exist_ok=True)
    config = directory / "healer.yaml"
    data = {
        "version": 1,
        "repo": {"path": "."},
        "orchestrator": {"type": orchestrator},
        "monitor": {"modes": ["poll", "push"]},
        "serve": {"port": 8123, **settings, "auth": auth or {"backend": "dev"}},
    }
    config.write_text(yaml.safe_dump(data, sort_keys=False), encoding="utf-8")
    return config


def serve(config: Path, *extra: str) -> int:
    return main(["-c", str(config), "serve", *extra])


# --------------------------------------------------------------------------- the command


def test_serve_is_registered(capsys: pytest.CaptureFixture[str]) -> None:
    build_parser()
    assert "serve" in COMMANDS
    with pytest.raises(SystemExit) as exited:
        main(["serve", "--help"])
    assert exited.value.code == 0
    out = capsys.readouterr().out
    assert "--host" in out and "--port" in out and "standalone host" in out


@pytest.mark.parametrize("value", ["0", "65536", "http", "-1"])
def test_invalid_ports_are_refused(value: str, capsys: pytest.CaptureFixture[str]) -> None:
    with pytest.raises(SystemExit) as exited:
        main(["serve", "--port", value])
    assert exited.value.code == 2
    assert "expected an integer from 1 to 65535" in capsys.readouterr().err


def test_an_empty_host_is_refused(capsys: pytest.CaptureFixture[str]) -> None:
    with pytest.raises(SystemExit) as exited:
        main(["serve", "--host", " "])
    assert exited.value.code == 2
    assert "expected a host name or an IP address" in capsys.readouterr().err


# --------------------------------------------------------------------------- refusing to start


@pytest.mark.parametrize(
    ("serve_settings", "flags"),
    [
        ({}, ("--host", "0.0.0.0")),
        ({"host": "0.0.0.0"}, ()),
        ({"host": "192.168.1.5"}, ()),
        ({}, ("--host", "::")),
    ],
    ids=["flag", "serve.host", "lan-address", "ipv6-any"],
)
def test_the_dev_backend_refuses_a_non_loopback_address(
    served: list[dict[str, Any]],
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
    serve_settings: dict[str, Any],
    flags: tuple[str, ...],
) -> None:
    assert serve(write_config(tmp_path, **serve_settings), *flags) == 1
    err = capsys.readouterr().err
    assert "error: " in err and "loopback" in err
    assert served == []


def test_a_token_backend_without_tokens_cannot_start(
    served: list[dict[str, Any]], tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    assert serve(write_config(tmp_path, auth={"backend": "token"})) == 1
    assert "error: " in capsys.readouterr().err
    assert served == []


def test_an_unknown_orchestrator_cannot_start(
    served: list[dict[str, Any]], tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    assert serve(write_config(tmp_path, orchestrator="nosuch")) == 1
    err = capsys.readouterr().err
    assert "error: the orchestrator adapter cannot be loaded" in err and "'nosuch'" in err
    assert served == []


def test_an_invalid_configuration_cannot_start(
    served: list[dict[str, Any]], tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    config = write_config(tmp_path)
    config.write_text(config.read_text(encoding="utf-8") + "unknown_section: 1\n", encoding="utf-8")
    assert serve(config) == 1
    assert "Invalid dag-healer configuration" in capsys.readouterr().err
    assert served == []


def test_an_alb_in_a_partition_without_a_known_key_endpoint_cannot_start(
    served: list[dict[str, Any]], tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    china = "arn:aws-cn:elasticloadbalancing:cn-north-1:123456789012:loadbalancer/app/healer/0123abcd"
    auth = {"backend": "alb_oidc", "alb_arn": china, "rules": TOKEN_AUTH["rules"]}
    assert serve(write_config(tmp_path, auth=auth)) == 1  # not a 503 on every request once it runs
    assert "error: serve.auth.alb_arn is in the 'aws-cn' partition" in capsys.readouterr().err
    assert served == []


def test_without_uvicorn_serve_explains_how_to_install_it(
    served: list[dict[str, Any]],
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    monkeypatch.setitem(sys.modules, "uvicorn", None)  # import uvicorn now raises ImportError
    assert serve(write_config(tmp_path)) == 1
    assert "error: dag-healer serve needs uvicorn: pip install 'dag-healer[serve]'" in capsys.readouterr().err
    assert served == []


# --------------------------------------------------------------------------- starting


def test_serve_starts_with_the_configured_address(
    served: list[dict[str, Any]],
    healer_env: Path,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    config = write_config(tmp_path / "repo")
    monkeypatch.chdir(config.parent)  # found as ./healer.yaml, not through -c
    assert main(["serve"]) == 0
    (call,) = served
    assert (call["host"], call["port"], call["log_level"]) == ("127.0.0.1", 8123, "info")
    assert os.environ["HEALER_CONFIG"] == str(config.resolve())  # the API loads the same file
    store = healer_env / "healer.db"
    assert store.is_file()
    assert current_version(db.get_engine(f"sqlite:///{store.as_posix()}")) == SCHEMA_VERSION
    captured = capsys.readouterr()
    startup = "dag-healer serve: http://127.0.0.1:8123/plugin/healer (auth: dev, orchestrator: Fakeflow)"
    assert startup in captured.out
    assert "warning: the dev backend serves every request as the user 'dev'" in captured.err
    with TestClient(call["app"], base_url="http://127.0.0.1:8123") as client:  # dev: loopback names only
        assert client.get("/healer/health").json()["status"] == "ok"
        assert client.get("/healer/api/v1/me").json()["user"] == "dev"


def test_flags_override_the_configured_address(
    served: list[dict[str, Any]], tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    config = write_config(tmp_path, host="localhost")
    assert main(["-vv", "serve", "--host", "127.0.0.2", "--port", "9001", "-c", str(config)]) == 0
    (call,) = served
    assert (call["host"], call["port"], call["log_level"]) == ("127.0.0.2", 9001, "debug")
    assert "http://127.0.0.2:9001/plugin/healer" in capsys.readouterr().out


def test_localhost_binds_the_loopback_address(
    served: list[dict[str, Any]], tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    # The name could resolve to another address; the dev backend must only ever bind loopback.
    assert serve(write_config(tmp_path / "a", host="localhost")) == 0
    assert serve(write_config(tmp_path / "b"), "--host", "LOCALHOST") == 0
    assert [call["host"] for call in served] == ["127.0.0.1", "127.0.0.1"]
    assert "dag-healer serve: http://127.0.0.1:8123/plugin/healer" in capsys.readouterr().out


@pytest.mark.parametrize("flag", ["::1", "[::1]"])
def test_an_ipv6_loopback_address_is_bound_without_brackets(
    served: list[dict[str, Any]], tmp_path: Path, capsys: pytest.CaptureFixture[str], flag: str
) -> None:
    assert serve(write_config(tmp_path), "--host", flag) == 0
    assert served[0]["host"] == "::1"
    assert "http://[::1]:8123/plugin/healer" in capsys.readouterr().out


def test_an_outdated_store_is_migrated(
    served: list[dict[str, Any]], tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    store = tmp_path / "store" / "healer.db"
    store.parent.mkdir()
    shutil.copyfile(V1_STORE, store)
    url = f"sqlite:///{store.as_posix()}"
    monkeypatch.setenv("HEALER_DATABASE_URL", url)
    assert current_version(db.get_engine(url)) == 1
    assert serve(write_config(tmp_path / "repo")) == 0
    assert current_version(db.get_engine(url)) == SCHEMA_VERSION


def test_a_store_that_cannot_be_prepared_is_only_a_warning(
    served: list[dict[str, Any]],
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    blocker = tmp_path / "not-a-dir"
    blocker.write_text("x", encoding="utf-8")
    monkeypatch.setenv("HEALER_DATABASE_URL", f"sqlite:///{(blocker / 'healer.db').as_posix()}")
    assert serve(write_config(tmp_path / "repo")) == 0
    assert "warning: the store sqlite:///" in capsys.readouterr().err
    assert len(served) == 1  # the host still starts; its store routes answer 503


def test_a_public_address_without_a_public_url_is_a_warning(
    served: list[dict[str, Any]],
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    warning = "0.0.0.0 is not a loopback address and neither serve.public_url nor HEALER_PUBLIC_URL is set"
    assert serve(write_config(tmp_path / "a", auth=TOKEN_AUTH), "--host", "0.0.0.0") == 0
    err = capsys.readouterr().err
    assert warning in err and "the dev backend" not in err
    with_public_url = write_config(tmp_path / "b", auth=TOKEN_AUTH, public_url="https://healer.example.com")
    assert serve(with_public_url, "--host", "0.0.0.0") == 0
    assert warning not in capsys.readouterr().err
    monkeypatch.setenv("HEALER_PUBLIC_URL", "https://healer.example.com")
    assert serve(write_config(tmp_path / "c", auth=TOKEN_AUTH), "--host", "0.0.0.0") == 0
    assert warning not in capsys.readouterr().err
    assert [call["host"] for call in served] == ["0.0.0.0"] * 3


def test_a_trusted_groups_header_is_a_warning(
    served: list[dict[str, Any]], tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    proxy = {"backend": "trusted_header", "trusted_proxies": ["127.0.0.1"], "rules": TOKEN_AUTH["rules"]}
    warning = "from the trusted proxies: they must set or strip it on every request"
    # An identity-aware proxy that sets only its user header passes X-Forwarded-Groups through.
    iap = {**proxy, "user_header": "X-Goog-Authenticated-User-Email"}
    assert serve(write_config(tmp_path / "a", auth=iap)) == 0
    err = capsys.readouterr().err
    assert f"warning: serve.auth trusts the X-Forwarded-Groups header (the default) {warning}" in err
    assert "set serve.auth.groups_header: null if they send no groups" in err
    explicit = {**proxy, "groups_header": "X-Auth-Request-Groups"}
    assert serve(write_config(tmp_path / "b", auth=explicit)) == 0
    assert f"serve.auth trusts the X-Auth-Request-Groups header {warning}" in capsys.readouterr().err
    assert serve(write_config(tmp_path / "c", auth={**iap, "groups_header": None})) == 0
    assert warning not in capsys.readouterr().err
    assert len(served) == 3


# --------------------------------------------------------------------------- run_server


def test_run_server_runs_uvicorn_without_proxy_headers(monkeypatch: pytest.MonkeyPatch) -> None:
    calls: list[tuple[Any, dict[str, Any]]] = []
    fake = types.ModuleType("uvicorn")
    fake.run = lambda app, **kwargs: calls.append((app, kwargs))  # type: ignore[attr-defined]
    monkeypatch.setitem(sys.modules, "uvicorn", fake)
    app = object()
    serve_cmd.run_server(app, host="127.0.0.1", port=8090)
    assert calls == [
        (
            app,
            {
                "host": "127.0.0.1",
                "port": 8090,
                "proxy_headers": False,
                "server_header": False,
                "log_level": "info",
            },
        )
    ]
