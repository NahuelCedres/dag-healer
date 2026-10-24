"""The standalone host (``hosting/app.py`` and ``hosting/shell.py``; sections 7.1, 7.6 and 11.7 of
``docs/architecture/orchestrators.md``).

``create_standalone_app`` mounts the healer app at ``/healer`` with an authentication backend
and serves the SPA shell on the Airflow URL shapes. Most tests inject a stub backend (the
``X-Stub-User`` header names the user) whose authorizer is a recording
:class:`~dag_healer.hosting.rbac.RuleAuthorizer`; the static directory is a temporary one with
a stand-in standalone bundle. The gate of step 7, at the end, builds the host the way
``dag-healer serve`` does: a Fakeflow instance with the token backend and the rules of its
healer.yaml.
"""

from __future__ import annotations

import logging
import textwrap
from collections.abc import Iterator
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import pytest
from fastapi import FastAPI, HTTPException, Request
from fastapi.routing import Mount
from fastapi.testclient import TestClient
from sqlalchemy import select
from test_api_security import iter_api_routes  # the route guard of the Airflow host, reused

from dag_healer._version import __version__
from dag_healer.api import app as app_module
from dag_healer.api import auth, deps
from dag_healer.api.auth import Principal
from dag_healer.config import ConfigError, load_config
from dag_healer.contracts.io import save_contract
from dag_healer.contracts.schema import ColumnRule, Contract, DatasetContract, DatasetLocator
from dag_healer.core.vocab import Capabilities
from dag_healer.hosting import app as hosting_app
from dag_healer.hosting import shell
from dag_healer.hosting.app import URL_PREFIX, create_standalone_app, pipeline_candidates
from dag_healer.hosting.auth.token import token_sha256
from dag_healer.hosting.rbac import DEV_RULES, RuleAuthorizer
from dag_healer.orchestrators import registry
from dag_healer.orchestrators.fake import FAKE_VOCABULARY
from dag_healer.store import db
from dag_healer.store import repo as store_repo
from dag_healer.store.models import Command

PUBLIC_URL = "https://healer.example.com"
CSRF = {"X-Requested-With": "dag-healer"}
BUNDLE = "export const healer = 1;\n"
ICON = '<svg xmlns="http://www.w3.org/2000/svg"></svg>\n'
SECURITY_HEADERS = {
    "cache-control": "no-store",
    "x-content-type-options": "nosniff",
    "referrer-policy": "same-origin",
    "x-frame-options": "DENY",
}
SHELL_PATHS = (
    "/plugin/healer",
    "/plugin/healer-dag",
    "/dags/sales_daily_etl/plugin/healer-dag",
    "/dags/x/runs/y/tasks/z/plugin/healer-ti",
    "/dags/x/runs/manual__2026-10-01T12:00:00+00:00/tasks/z/mapped/3/plugin/healer-ti",
)
USERS = {
    "alice": Principal(id="alice", name="alice", groups=frozenset({"data-eng"})),
    "bob": Principal(id="bob", name="bob", groups=frozenset({"analysts"})),
}
RULES = [
    {"principals": ["group:data-eng"], "pipelines": ["*"], "actions": ["read", "act", "approve_contract"]},
    {"principals": ["user:bob"], "pipelines": ["customers_*"], "actions": ["read"]},
]


# --------------------------------------------------------------------------- stub backend


class RecordingAuthorizer(RuleAuthorizer):
    """The host's RuleAuthorizer, recording what the API asks it."""

    def __init__(self) -> None:
        super().__init__(RULES, candidates=pipeline_candidates)
        self.calls: list[tuple[Any, ...]] = []

    async def check_dag_access(self, request: Request, user: Any, method: str, entity: str | None) -> None:
        self.calls.append(("check_dag_access", user, method, entity, request.path_params.get("dag_id")))
        await super().check_dag_access(request, user, method, entity)

    def authorized_dag_ids(self, user: Any, method: str = "GET") -> set[str]:
        self.calls.append(("authorized_dag_ids", user, method))
        return super().authorized_dag_ids(user, method)


async def stub_user(request: Request) -> Principal:
    user = USERS.get(request.headers.get("x-stub-user", ""))
    if user is None:
        raise HTTPException(status_code=401, detail="Not authenticated")
    return user


class StubBackend:
    """An AuthBackend whose users come from the ``X-Stub-User`` header."""

    name = "stub"

    def __init__(self, authorizer: RuleAuthorizer, origins: list[str]) -> None:
        self._authorizer = authorizer
        self._origins = origins

    def user_dependency(self) -> Any:
        return stub_user

    def authorizer(self) -> RuleAuthorizer:
        return self._authorizer

    def allowed_origins(self) -> list[str]:
        return list(self._origins)


# --------------------------------------------------------------------------- fixtures


def write_repo(root: Path) -> Path:
    """A repository with a Fakeflow healer.yaml (token backend), a draft contract and a datasets entry."""
    (root / "dags").mkdir(parents=True)
    contract = Contract(
        dag_id="customers_sync",
        datasets=[
            DatasetContract(
                name="customers",
                task_id="transform",
                locator=DatasetLocator(type="csv", path="data/customers.csv"),
                columns=[ColumnRule(name="signup_date", dtype="string")],
            )
        ],
    )
    save_contract(contract, root / "healer" / "contracts" / "customers_sync.yaml")
    config = root / "healer.yaml"
    config.write_text(
        textwrap.dedent(
            f"""\
            version: 1
            repo: {{path: ., dags_dir: dags}}
            orchestrator: {{type: fake}}
            monitor: {{dags: ["sales_daily_etl", "customers_*", "!secret_*"], modes: [poll, push]}}
            contracts: {{dir: healer/contracts}}
            datasets:
              inventory_report:
                build_report: {{type: csv, path: data/inventory.csv}}
            serve:
              public_url: {PUBLIC_URL}
              auth: {{backend: token}}
            """
        ),
        encoding="utf-8",
    )
    return config


def healer_app_of(app: FastAPI) -> FastAPI:
    """The healer app the standalone app mounts at ``/healer``."""
    (mount,) = [route for route in app.routes if isinstance(route, Mount) and route.path == URL_PREFIX]
    return mount.app  # type: ignore[return-value]


@dataclass
class Host:
    client: TestClient
    app: FastAPI
    authorizer: RecordingAuthorizer
    static: Path
    incident_id: str

    @property
    def healer(self) -> FastAPI:
        return healer_app_of(self.app)

    def get(self, path: str, user: str | None = "alice", **kwargs: Any) -> Any:
        headers = dict(kwargs.pop("headers", {}) or {})
        if user:
            headers["X-Stub-User"] = user
        return self.client.get(path, headers=headers, **kwargs)

    def post(self, path: str, user: str | None = "alice", **kwargs: Any) -> Any:
        headers = {**CSRF, **dict(kwargs.pop("headers", {}) or {})}
        if user:
            headers["X-Stub-User"] = user
        return self.client.post(path, headers=headers, **kwargs)


@pytest.fixture
def static_dir(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Path:
    """The API's static directory, replaced by one holding a stand-in standalone bundle."""
    static = tmp_path / "static"
    (static / "standalone").mkdir(parents=True)
    (static / "standalone" / "index.js").write_text(BUNDLE, encoding="utf-8")
    (static / "icon.svg").write_text(ICON, encoding="utf-8")
    monkeypatch.setattr(app_module, "STATIC_DIR", static)
    return static


@pytest.fixture
def standalone_env(healer_env: Path, tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Iterator[Path]:
    """A Fakeflow repository whose healer.yaml HEALER_CONFIG points at, and a temporary store."""
    config = write_repo(tmp_path / "repo")
    monkeypatch.setenv("HEALER_CONFIG", str(config))
    monkeypatch.setenv("HEALER_DATABASE_URL", f"sqlite:///{(tmp_path / 'healer.db').as_posix()}")
    db.dispose_engines()
    deps.reset_config_cache()
    registry.reset_adapters()
    yield config
    deps.reset_config_cache()
    registry.reset_adapters()
    db.dispose_engines()


@pytest.fixture
def host(standalone_env: Path, static_dir: Path) -> Iterator[Host]:
    with db.session_scope() as session:
        incident, _ = store_repo.create_incident(
            session,
            orchestrator="fake",
            dag_id="sales_daily_etl",
            run_id="run-7",
            task_id="load_sales",
            try_number=1,
            source="poll",
            error_summary="KeyError: 'amount'",
            status="waiting_human",
        )
        incident_id = incident.id
    authorizer = RecordingAuthorizer()
    cfg = load_config(standalone_env)
    app = create_standalone_app(cfg, backend=StubBackend(authorizer, [PUBLIC_URL]))
    with TestClient(app) as client:
        yield Host(client, app, authorizer, static_dir, incident_id)


# --------------------------------------------------------------------------- the shell


@pytest.mark.parametrize("path", SHELL_PATHS)
def test_the_shell_is_served_on_the_airflow_url_shapes(host: Host, path: str) -> None:
    response = host.get(path, user=None)  # public: the shell holds no data
    assert response.status_code == 200
    assert response.headers["content-type"].startswith("text/html")
    page = response.text
    assert '<base href="/" />' in page
    assert '<div id="root"></div>' in page
    assert f'<script type="module" src="healer/static/standalone/index.js?v={__version__}"></script>' in page
    assert "<title>Healer · Fakeflow</title>" in page
    assert '<meta name="dag-healer-auth" content="stub" />' in page
    assert '<link rel="icon" type="image/svg+xml" href="healer/static/icon.svg" />' in page
    assert "<style" not in page and page.count("<script") == 1  # no inline script or style
    for name, value in SECURITY_HEADERS.items():
        assert response.headers[name] == value
    csp = response.headers["content-security-policy"]
    assert "script-src 'self';" in csp and "frame-ancestors 'none'" in csp and "object-src 'none'" in csp


def test_the_root_redirects_to_the_healer_page(host: Host) -> None:
    response = host.get("/", user=None, follow_redirects=False)
    assert response.status_code == 307
    assert response.headers["location"] == "/plugin/healer"
    assert response.headers["cache-control"] == "no-store"
    assert host.get("/", user=None).url.path == "/plugin/healer"


def test_the_shell_follows_the_root_path(host: Host) -> None:
    client = TestClient(host.app, root_path="/tools/healer")
    page = client.get("/plugin/healer")
    assert page.status_code == 200
    assert '<base href="/tools/healer/" />' in page.text
    redirect = client.get("/", follow_redirects=False)
    assert redirect.headers["location"] == "/tools/healer/plugin/healer"


def test_without_the_bundle_the_pages_answer_503(host: Host) -> None:
    bundle = host.static / "standalone" / "index.js"
    bundle.unlink()
    for path in ("/", "/plugin/healer", "/dags/x/runs/y/tasks/z/plugin/healer-ti"):
        response = host.get(path, user=None, follow_redirects=False)
        assert response.status_code == 503, path
        assert response.headers["content-type"].startswith("text/plain")
        assert response.text == shell.MISSING_BUNDLE_DETAIL
        assert response.headers["cache-control"] == "no-store"
    assert "standalone/index.js" in shell.MISSING_BUNDLE_DETAIL
    assert "python scripts/tasks.py ui" in shell.MISSING_BUNDLE_DETAIL
    assert "/healer/api/v1" in shell.MISSING_BUNDLE_DETAIL
    assert host.get("/healer/api/v1/incidents").status_code == 200  # the API keeps answering
    bundle.write_text(BUNDLE, encoding="utf-8")  # a bundle built while the server runs is picked up
    assert host.get("/plugin/healer", user=None).status_code == 200


def test_the_standalone_bundle_is_served_as_javascript(host: Host) -> None:
    response = host.get("/healer/static/standalone/index.js", user=None)
    assert response.status_code == 200
    assert response.headers["content-type"].split(";")[0] in ("application/javascript", "text/javascript")
    assert response.headers["cache-control"] == "no-cache"
    assert response.text == BUNDLE
    icon = host.get("/healer/static/icon.svg", user=None)
    assert icon.status_code == 200 and icon.headers["content-type"].startswith("image/svg+xml")


def test_api_answers_are_never_stored(host: Host) -> None:
    answers = {
        "a list": host.get("/healer/api/v1/incidents"),
        "/me": host.get("/healer/api/v1/me"),
        "401": host.get("/healer/api/v1/incidents", user=None),
        "403": host.get("/healer/api/v1/dags/sales_daily_etl/incidents", user="bob"),
        "404": host.get("/healer/api/v1/nosuch"),
        "a root path": TestClient(host.app, root_path="/tools/healer").get(
            "/healer/api/v1/me", headers={"X-Stub-User": "alice"}
        ),
    }
    for name, response in answers.items():
        assert response.headers["cache-control"] == "no-store", name
        assert response.headers["x-content-type-options"] == "nosniff", name
    assert host.get("/healer/static/standalone/index.js", user=None).headers["cache-control"] == "no-cache"
    assert "cache-control" not in host.get("/healer/health", user=None).headers


def test_api_answers_keep_their_own_cache_headers() -> None:
    async def answer(scope: Any, receive: Any, send: Any) -> None:
        headers = [(b"cache-control", b"private, max-age=5")]
        await send({"type": "http.response.start", "status": 200, "headers": headers})
        await send({"type": "http.response.body", "body": b"{}"})

    client = TestClient(hosting_app.ApiResponseHeaders(answer))
    response = client.get("/healer/api/v1/incidents")
    assert response.headers["cache-control"] == "private, max-age=5"
    assert response.headers["x-content-type-options"] == "nosniff"
    assert "x-content-type-options" not in client.get("/healer/health").headers


def test_health_is_public(host: Host) -> None:
    response = host.get("/healer/health", user=None)
    assert response.status_code == 200
    assert response.json() == {"status": "ok", "version": __version__}


def test_the_outer_app_has_no_docs_and_no_other_pages(host: Host) -> None:
    for path in ("/docs", "/redoc", "/openapi.json", "/healer/docs", "/healer/openapi.json", "/static/x"):
        assert host.get(path, user=None).status_code == 404, path


def test_page_title() -> None:
    assert shell.page_title(None) == "Healer"
    assert shell.page_title("") == "Healer"
    assert shell.page_title("AWS Step Functions") == "Healer · AWS Step Functions"
    assert shell.page_title("Apache Airflow") == "Healer · Apache Airflow"


def test_the_shell_escapes_every_value() -> None:
    page = shell.render_shell(
        root_path='/a"><script>x()</script>', title="<b>Healer</b> & co", auth_backend='"token', version='1"2'
    )
    assert "<script>x()" not in page and "<b>" not in page
    assert '<base href="/a&quot;&gt;&lt;script&gt;x()&lt;/script&gt;/" />' in page
    assert "<title>&lt;b&gt;Healer&lt;/b&gt; &amp; co</title>" in page
    assert '<meta name="dag-healer-auth" content="&quot;token" />' in page
    assert 'index.js?v=1&quot;2"' in page


def test_the_healer_app_is_mounted_at_the_prefix_of_the_airflow_plugin() -> None:
    plugin = Path(app_module.__file__).resolve().parents[1] / "airflow" / "plugin.py"
    assert f'URL_PREFIX = "{URL_PREFIX}"' in plugin.read_text(encoding="utf-8")  # same /healer/... URLs


def test_bundle_file_reads_the_static_directory_at_call_time(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setattr(app_module, "STATIC_DIR", tmp_path)
    assert shell.bundle_file() == tmp_path / "standalone" / "index.js"
    assert shell.bundle_file(tmp_path / "other") == tmp_path / "other" / "standalone" / "index.js"
    assert shell.SCRIPT_PATH == "healer/static/" + "standalone/index.js"


# --------------------------------------------------------------------------- the healer app


def test_every_mounted_api_route_requires_the_backend_user(host: Host) -> None:
    routes = iter_api_routes(host.healer)
    assert len(routes) >= 14
    unprotected = [
        route.path
        for route in routes
        if not route.path.startswith("/api/v1/events/") and auth.get_current_user not in route.calls
    ]
    assert unprotected == []
    assert [route.path for route in routes if route.path.startswith("/api/v1/events/")] == [
        "/api/v1/events/task-failed",
        "/api/v1/events/{channel}",
    ]


def test_the_backend_is_installed_through_the_two_overrides_only(host: Host) -> None:
    healer = host.healer
    assert set(healer.dependency_overrides) == {auth.get_current_user, auth.get_authorizer}
    assert healer.dependency_overrides[auth.get_current_user] is stub_user
    assert healer.dependency_overrides[auth.get_authorizer]() is host.authorizer
    assert host.app.dependency_overrides == {}
    assert app_module.serving_host(healer) == app_module.STANDALONE_HOST


def test_requests_through_the_outer_app_reach_the_backend(host: Host) -> None:
    assert host.get("/healer/api/v1/incidents", user=None).json() == {"detail": "Not authenticated"}
    response = host.get("/healer/api/v1/incidents")
    assert response.status_code == 200
    assert [item["id"] for item in response.json()["incidents"]] == [host.incident_id]
    assert ("authorized_dag_ids", USERS["alice"], "GET") in host.authorizer.calls
    me = host.get("/healer/api/v1/me").json()
    assert me["user"] == "alice"
    assert me["orchestrator"]["display_name"] == "Fakeflow"


def test_dag_routes_are_checked_by_the_rules(host: Host) -> None:
    path = "/healer/api/v1/dags/sales_daily_etl/incidents"
    assert host.get(path).status_code == 200
    check = ("check_dag_access", USERS["alice"], "GET", "TASK_INSTANCE", "sales_daily_etl")
    assert check in host.authorizer.calls
    denied = host.get(path, user="bob")
    assert denied.status_code == 403
    assert denied.json() == {"detail": "Forbidden"}
    assert host.get("/healer/api/v1/dags/customers_sync/contract", user="bob").status_code == 200
    resolve = f"/healer/api/v1/dags/sales_daily_etl/incidents/{host.incident_id}/resolve"
    assert host.post(resolve, user="bob", json={"resolution": "ignore"}).status_code == 403


def test_lists_take_their_candidates_from_the_configuration(host: Host) -> None:
    assert pipeline_candidates() == {"customers_sync", "sales_daily_etl", "inventory_report"}
    alice = {item["dag_id"] for item in host.get("/healer/api/v1/dags").json()["dags"]}
    assert alice == {"customers_sync", "sales_daily_etl"}  # inventory_report is not monitored
    bob = {item["dag_id"] for item in host.get("/healer/api/v1/dags", user="bob").json()["dags"]}
    assert bob == {"customers_sync"}
    contracts = host.get("/healer/api/v1/contracts", user="bob").json()["contracts"]
    assert [item["dag_id"] for item in contracts] == ["customers_sync"]


def test_state_changing_requests_check_the_backend_origins(host: Host) -> None:
    resolve = f"/healer/api/v1/dags/sales_daily_etl/incidents/{host.incident_id}/resolve"
    body = {"resolution": "ignore"}
    foreign = host.post(resolve, json=body, headers={"Origin": "https://evil.example"})
    assert foreign.status_code == 403
    assert foreign.json() == {"detail": "Cross-origin request rejected"}
    assert host.post(resolve, json=body, headers={"Origin": PUBLIC_URL}).status_code == 202
    referer = {"Referer": f"{PUBLIC_URL}/plugin/healer"}
    assert host.post(resolve, json=body, headers=referer).status_code == 202
    own_origin = {"Origin": "http://testserver"}  # the request's own origin
    assert host.post(resolve, json=body, headers=own_origin).status_code == 202
    without_csrf_header = {"X-Stub-User": "alice", "Origin": PUBLIC_URL}
    assert host.client.post(resolve, json=body, headers=without_csrf_header).status_code == 403


def test_pipeline_candidates_leave_out_patterns(standalone_env: Path) -> None:
    standalone_env.write_text(
        textwrap.dedent(
            """\
            version: 1
            repo: {path: .}
            monitor: {dags: ["a", "b_*", "c?", "[d]", "!e", "f"]}
            contracts: {dir: healer/contracts}
            """
        ),
        encoding="utf-8",
    )
    deps.reset_config_cache()
    assert pipeline_candidates() == {"a", "f", "customers_sync"}


# --------------------------------------------------------------------------- building the app


def test_the_warnings_of_a_missing_bundle_and_an_unknown_adapter(
    standalone_env: Path, tmp_path: Path, monkeypatch: pytest.MonkeyPatch, caplog: pytest.LogCaptureFixture
) -> None:
    monkeypatch.setattr(app_module, "STATIC_DIR", tmp_path / "empty")
    (tmp_path / "empty").mkdir()
    standalone_env.write_text("version: 1\norchestrator: {type: nosuch}\n", encoding="utf-8")
    cfg = load_config(standalone_env)
    with caplog.at_level(logging.WARNING, logger="dag_healer.hosting.app"):
        app = create_standalone_app(cfg, backend=StubBackend(RuleAuthorizer([]), []))
    assert "the orchestrator adapter cannot be built" in caplog.text and "nosuch" in caplog.text
    assert "standalone UI bundle" in caplog.text and "is missing" in caplog.text
    client = TestClient(app)
    assert client.get("/plugin/healer").status_code == 503
    (tmp_path / "empty" / "standalone").mkdir()
    (tmp_path / "empty" / "standalone" / "index.js").write_text(BUNDLE, encoding="utf-8")
    assert "<title>Healer</title>" in client.get("/plugin/healer").text


def test_an_adapter_without_the_standalone_host_is_a_warning(
    standalone_env: Path, static_dir: Path, monkeypatch: pytest.MonkeyPatch, caplog: pytest.LogCaptureFixture
) -> None:
    @dataclass
    class PluginOnlyAdapter:
        display_name: str = "Plugin Only"
        capabilities: Capabilities = Capabilities(sources=("poll",), ui_hosts=("airflow_plugin",))

    monkeypatch.setattr(hosting_app.registry, "get_adapter", lambda cfg: PluginOnlyAdapter())
    with caplog.at_level(logging.WARNING, logger="dag_healer.hosting.app"):
        app = create_standalone_app(load_config(standalone_env), backend=StubBackend(RuleAuthorizer([]), []))
    assert "does not list the standalone host among its UI hosts (airflow_plugin)" in caplog.text
    assert "is missing" not in caplog.text
    assert "<title>Healer · Plugin Only</title>" in TestClient(app).get("/plugin/healer").text


def _use_backend(config: Path, auth_section: str) -> None:
    text = config.read_text(encoding="utf-8").replace("auth: {backend: token}", f"auth: {auth_section}")
    config.write_text(text, encoding="utf-8")
    deps.reset_config_cache()


def test_the_configured_backend_is_built_with_the_rule_authorizer(
    standalone_env: Path, static_dir: Path
) -> None:
    _use_backend(standalone_env, "{backend: dev}")
    cfg = load_config(standalone_env)
    app = create_standalone_app(cfg, bind_host="127.0.0.1")
    authorizer = healer_app_of(app).dependency_overrides[auth.get_authorizer]()
    assert isinstance(authorizer, RuleAuthorizer)
    assert authorizer.rules == DEV_RULES  # dev without rules: the built-in rule
    with TestClient(app, base_url="http://127.0.0.1:8090") as client:
        me = client.get("/healer/api/v1/me")
        assert me.status_code == 200 and me.json()["user"] == "dev"
        assert client.get("/healer/api/v1/dags").status_code == 200
        assert '<meta name="dag-healer-auth" content="dev" />' in client.get("/plugin/healer").text


@pytest.mark.parametrize(
    ("host_header", "allowed"),
    [
        ("127.0.0.1:8090", True),
        ("localhost:8090", True),
        ("LOCALHOST", True),
        ("[::1]:8090", True),
        ("127.0.0.2", True),
        ("attacker.example:8090", False),  # DNS rebinding: a name that resolves to 127.0.0.1
        ("192.168.1.5:8090", False),
        ("localhost.attacker.example", False),
        ("", False),
        ("[::1", False),
    ],
)
def test_the_dev_backend_answers_loopback_host_names_only(
    standalone_env: Path, static_dir: Path, host_header: str, allowed: bool
) -> None:
    _use_backend(standalone_env, "{backend: dev}")
    client = TestClient(create_standalone_app(load_config(standalone_env), bind_host="127.0.0.1"))
    for path in ("/healer/api/v1/me", "/plugin/healer", "/healer/health"):
        response = client.get(path, headers={"Host": host_header})
        if allowed:
            assert response.status_code == 200, (host_header, path)
        else:
            assert response.status_code == 400, (host_header, path)
            assert response.text == hosting_app.LOOPBACK_HOST_DETAIL


@pytest.mark.parametrize(
    ("header", "value"),
    [
        ("X-Forwarded-For", "203.0.113.9"),
        ("X-Forwarded-Host", "healer.example.com"),
        ("X-Forwarded-Proto", "https"),
        ("X-Real-IP", "203.0.113.9"),
        ("Forwarded", "for=203.0.113.9;host=healer.example.com"),
        ("Via", "1.1 proxy"),
    ],
)
def test_the_dev_backend_refuses_requests_a_proxy_forwarded(
    standalone_env: Path, static_dir: Path, header: str, value: str
) -> None:
    _use_backend(standalone_env, "{backend: dev}")
    app = create_standalone_app(load_config(standalone_env), bind_host="127.0.0.1")
    client = TestClient(app, base_url="http://127.0.0.1:8090")
    assert client.get("/healer/api/v1/me").status_code == 200
    # A reverse proxy on this machine that sets Host to the address it forwards to (nginx's default).
    for path in ("/healer/api/v1/me", "/plugin/healer"):
        response = client.get(path, headers={header: value})
        assert (response.status_code, response.text) == (400, hosting_app.FORWARDED_REQUEST_DETAIL)


def test_other_backends_answer_every_host_name(host: Host) -> None:
    response = host.get("/healer/api/v1/me", headers={"Host": "healer.example.com"})
    assert response.status_code == 200


def test_the_dev_backend_needs_a_loopback_bind_address(standalone_env: Path, static_dir: Path) -> None:
    _use_backend(standalone_env, "{backend: dev}")
    cfg = load_config(standalone_env)
    with pytest.raises(ConfigError, match="loopback"):
        create_standalone_app(cfg, bind_host="0.0.0.0")
    cfg.serve.host = "0.0.0.0"
    with pytest.raises(ConfigError, match="loopback"):
        create_standalone_app(cfg)  # bind_host defaults to serve.host


# --------------------------------------------------------------------------- the step-7 gate

ALICE_TOKEN = "alice-token"
BOB_TOKEN = "bob-token"
#: serve.auth of the gate: alice (data-eng) may do everything, bob is authenticated but in no rule.
GATE_AUTH = (
    "{backend: token, tokens: ["
    f"{{name: alice, sha256: {token_sha256(ALICE_TOKEN)}, groups: [data-eng]}}, "
    f"{{name: bob, sha256: {token_sha256(BOB_TOKEN)}, groups: [analysts]}}], "
    "rules: [{principals: ['group:data-eng'], pipelines: ['*'], actions: [read, act, approve_contract]}]}"
)
API = f"{URL_PREFIX}/api/v1"


@dataclass
class Gate:
    client: TestClient
    ids: dict[str, str]


@pytest.fixture
def gate(standalone_env: Path, static_dir: Path) -> Iterator[Gate]:
    """The host of the gate: Fakeflow incidents and the real token backend of :data:`GATE_AUTH`."""
    _use_backend(standalone_env, GATE_AUTH)
    seeds = {
        "ambiguous": ("sales_daily_etl", "run-7", "load_sales", "waiting_human", "ambiguous"),
        "repair_failed": ("customers_sync", "run-7", "transform", "repair_failed", "code"),
        "reported": ("sales_daily_etl", "run-8", "load_sales", "reported", "data"),
    }
    ids: dict[str, str] = {}
    with db.session_scope() as session:
        for name, (dag_id, run_id, task_id, status, category) in seeds.items():
            incident, _ = store_repo.create_incident(
                session,
                orchestrator="fake",
                dag_id=dag_id,
                run_id=run_id,
                task_id=task_id,
                try_number=1,
                source="poll",
                error_summary=f"{category} failure of {task_id}",
                status=status,
                category=category,
            )
            ids[name] = incident.id
    app = create_standalone_app(load_config(standalone_env))
    with TestClient(app) as client:
        yield Gate(client, ids)


def test_the_gate_serves_the_three_views_with_working_actions(gate: Gate) -> None:
    pages = (
        "/plugin/healer",
        "/dags/sales_daily_etl/plugin/healer-dag",
        "/dags/sales_daily_etl/runs/run-7/tasks/load_sales/plugin/healer-ti",
    )
    for path in pages:
        page = gate.client.get(path)
        assert page.status_code == 200, path
        assert "<title>Healer · Fakeflow</title>" in page.text
        assert '<meta name="dag-healer-auth" content="token" />' in page.text  # the UI asks for a token

    alice = {"Authorization": f"Bearer {ALICE_TOKEN}"}
    me = gate.client.get(f"{API}/me", headers=alice).json()
    assert (me["user"], me["base_url"], me["host"]) == ("alice", PUBLIC_URL, "standalone")
    assert me["orchestrator"]["display_name"] == "Fakeflow"
    assert me["orchestrator"]["vocabulary"] == FAKE_VOCABULARY.as_dict()

    # what the three views ask for: the Healer page, the DAG tab and the task-instance tab
    listed = gate.client.get(f"{API}/incidents", headers=alice).json()
    assert {item["id"] for item in listed["incidents"]} == set(gate.ids.values())
    assert gate.client.get(f"{API}/dags", headers=alice).status_code == 200
    assert gate.client.get(f"{API}/contracts", headers=alice).status_code == 200
    dag = gate.client.get(f"{API}/dags/sales_daily_etl/incidents", headers=alice).json()
    assert {item["id"] for item in dag["incidents"]} == {gate.ids["ambiguous"], gate.ids["reported"]}
    assert dag["can_edit"] is True
    task = {"run_id": "run-7", "task_id": "load_sales"}
    ti = gate.client.get(f"{API}/dags/sales_daily_etl/task-instance", params=task, headers=alice).json()
    assert ti["incident"]["id"] == gate.ids["ambiguous"]
    assert ti["incident"]["allowed_actions"] == ["resolve_data", "resolve_code", "retry_task", "ignore"]

    # the actions, as the UI sends them
    ui = {**alice, **CSRF, "Origin": "http://testserver"}
    actions = [
        (f"dags/sales_daily_etl/incidents/{gate.ids['ambiguous']}/resolve", {"resolution": "data"}),
        (f"dags/customers_sync/incidents/{gate.ids['repair_failed']}/retry-repair", {}),
        (f"dags/sales_daily_etl/incidents/{gate.ids['reported']}/retry-task", {}),
        ("dags/customers_sync/contract/approve", {}),
    ]
    for path, body in actions:
        response = gate.client.post(f"{API}/{path}", json=body, headers=ui)
        assert response.status_code == 202, path
    with db.session_scope() as session:
        commands = session.scalars(select(Command).order_by(Command.id)).all()
        queued = [(command.kind, command.orchestrator, command.actor) for command in commands]
    assert queued == [
        ("resolve", "fake", "alice"),
        ("retry_repair", "fake", "alice"),
        ("retry_task", "fake", "alice"),
        ("approve_contract", "fake", "alice"),
    ]


def test_a_changed_serve_auth_is_a_warning_until_a_restart(
    gate: Gate, standalone_env: Path, caplog: pytest.LogCaptureFixture
) -> None:
    alice = {"Authorization": f"Bearer {ALICE_TOKEN}"}
    bob = {"Authorization": f"Bearer {BOB_TOKEN}"}
    warning = "serve.auth changed in healer.yaml: restart dag-healer serve to apply it"
    with caplog.at_level(logging.WARNING, logger="dag_healer.hosting.app"):
        deps.reset_config_cache()  # reloaded, unchanged
        assert gate.client.get(f"{API}/incidents", headers=alice).status_code == 200
        assert warning not in caplog.text
        bob_entry = f", {{name: bob, sha256: {token_sha256(BOB_TOKEN)}, groups: [analysts]}}"
        assert bob_entry in GATE_AUTH
        config = standalone_env.read_text(encoding="utf-8")  # bob's token is removed while serving
        standalone_env.write_text(
            config.replace(GATE_AUTH, GATE_AUTH.replace(bob_entry, "")), encoding="utf-8"
        )
        deps.reset_config_cache()
        for _ in range(2):  # the Healer page lists the incidents on every load
            assert gate.client.get(f"{API}/incidents", headers=alice).status_code == 200
    assert caplog.text.count(warning) == 1
    assert gate.client.get(f"{API}/me", headers=bob).json()["user"] == "bob"  # until the restart


def test_the_gate_needs_a_valid_token_and_a_rule(gate: Gate) -> None:
    refused = ({}, {"Authorization": "Bearer wrong-token"}, {"Authorization": f"Basic {ALICE_TOKEN}"})
    for credentials in refused:
        response = gate.client.get(f"{API}/incidents", headers=credentials)
        assert response.status_code == 401
        assert response.json() == {"detail": "Not authenticated"}
        assert response.headers["www-authenticate"] == "Bearer"
    bob = {"Authorization": f"Bearer {BOB_TOKEN}"}
    assert gate.client.get(f"{API}/me", headers=bob).json()["user"] == "bob"
    assert gate.client.get(f"{API}/incidents", headers=bob).json() == {"incidents": [], "total": 0}
    denied = gate.client.get(f"{API}/dags/sales_daily_etl/incidents", headers=bob)
    assert denied.status_code == 403
    assert denied.json() == {"detail": "Forbidden"}
    resolve = f"{API}/dags/sales_daily_etl/incidents/{gate.ids['ambiguous']}/resolve"
    assert gate.client.post(resolve, json={"resolution": "data"}, headers={**bob, **CSRF}).status_code == 403
