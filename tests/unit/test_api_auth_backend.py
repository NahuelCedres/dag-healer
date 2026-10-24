"""Tests of the host seam of the dag-healer API: ``create_app(auth=...)`` and the auth backends.

``create_app()`` builds the Airflow host's app: no dependency override, nothing on
``app.state`` and the bare ``RequestGuardMiddleware``. An :class:`AuthBackend` given to
``create_app(auth=...)`` (the standalone host) reaches every route through the two
overrides of :func:`dag_healer.api.auth.apply_auth_backend`, also when the app is mounted,
and supplies the CSRF origins. A local stub stands in for the standalone backends.
"""

from __future__ import annotations

import dataclasses
import textwrap
from collections.abc import Callable, Iterator
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

import pytest
from fastapi import FastAPI, HTTPException, Request
from fastapi.testclient import TestClient
from test_api_security import iter_api_routes

from dag_healer.api import app as app_module
from dag_healer.api import auth, deps
from dag_healer.api.app import RequestGuardMiddleware, create_app, get_app, serving_host
from dag_healer.api.auth import AirflowAuthBackend, AuthBackend, Principal, apply_auth_backend
from dag_healer.contracts.io import save_contract
from dag_healer.contracts.schema import ColumnRule, Contract, DatasetContract, DatasetLocator
from dag_healer.store import db
from dag_healer.store import repo as store_repo
from dag_healer.store.models import Command

DAG = "inventory_report"
PUBLIC_URL = "https://healer.example"
AIRFLOW_URL = "https://airflow.example"
CSRF = {"X-Requested-With": "dag-healer"}

ALICE = Principal(id="alice", name="Alice", groups=frozenset({"editors"}))
BOB = Principal(id="bob", name="Bob", groups=frozenset({"viewers"}))
USERS = {"alice": ALICE, "bob": BOB}


# --------------------------------------------------------------------------- stub backend


def may(user: Any, method: str) -> bool:
    """Editors may do everything; any other principal may only read."""
    return isinstance(user, Principal) and (method == "GET" or "editors" in user.groups)


class RecordingAuthorizer:
    """An :class:`auth.Authorizer` that records every question it is asked."""

    def __init__(self) -> None:
        self.questions: list[tuple[Any, ...]] = []

    async def check_dag_access(self, request: Request, user: Any, method: str, entity: str | None) -> None:
        self.questions.append(("check_dag_access", user, method, entity, request.path_params.get("dag_id")))
        if not may(user, method):
            raise HTTPException(status_code=403, detail="Forbidden")

    def authorized_dag_ids(self, user: Any, method: str = "GET") -> set[str]:
        self.questions.append(("authorized_dag_ids", user, method))
        return {DAG} if may(user, method) else set()

    def can_read_dag(self, user: Any, dag_id: str) -> bool:
        self.questions.append(("can_read_dag", user, dag_id))
        return may(user, "GET")

    def can_edit_dag(self, user: Any, dag_id: str, entity: str | None = "TASK_INSTANCE") -> bool:
        self.questions.append(("can_edit_dag", user, dag_id, entity))
        return may(user, "PUT")

    def asked(self, question: str) -> list[tuple[Any, ...]]:
        return [entry for entry in self.questions if entry[0] == question]


@dataclass
class StubBackend:
    """A standalone-style backend: the ``X-Stub-User`` header names the user."""

    name: str = "stub"
    origins: list[str] = field(default_factory=lambda: [PUBLIC_URL])
    recorder: RecordingAuthorizer = field(default_factory=RecordingAuthorizer)
    origin_calls: int = 0

    def user_dependency(self) -> Callable[..., Any]:
        async def stub_user(request: Request) -> Principal:
            user = USERS.get(request.headers.get("x-stub-user", ""))
            if user is None:
                raise HTTPException(status_code=401, detail="Not authenticated")
            return user

        return stub_user

    def authorizer(self) -> auth.Authorizer:
        return self.recorder

    def allowed_origins(self) -> list[str]:
        self.origin_calls += 1
        return list(self.origins)


# --------------------------------------------------------------------------- fixtures


@dataclass
class Store:
    db_url: str
    incident_id: str


@pytest.fixture
def store(healer_env: Path, tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Iterator[Store]:
    """A healer.yaml with the inventory_report contract and a store with one waiting incident."""
    repo = tmp_path / "repo"
    (repo / "dags").mkdir(parents=True)
    contract = Contract(
        dag_id=DAG,
        datasets=[
            DatasetContract(
                name="inventory",
                task_id="build_report",
                locator=DatasetLocator(type="csv", path="data/incoming/inventory.csv"),
                columns=[ColumnRule(name="qty", dtype="integer")],
            )
        ],
    )
    save_contract(contract, repo / "healer" / "contracts" / f"{DAG}.yaml")
    (repo / "healer.yaml").write_text(
        textwrap.dedent(
            """\
            version: 1
            repo: {path: .}
            contracts: {dir: healer/contracts}
            """
        ),
        encoding="utf-8",
    )
    db_url = f"sqlite:///{(tmp_path / 'healer.db').as_posix()}"
    monkeypatch.setenv("HEALER_CONFIG", str(repo / "healer.yaml"))
    monkeypatch.setenv("HEALER_DATABASE_URL", db_url)
    # The Airflow host's CSRF origin: a standalone app must not accept it.
    monkeypatch.setenv("AIRFLOW__API__BASE_URL", AIRFLOW_URL)
    db.dispose_engines()
    deps.reset_config_cache()
    with db.session_scope(db_url) as session:
        incident, _ = store_repo.create_incident(
            session,
            dag_id=DAG,
            run_id="manual__1",
            task_id="build_report",
            try_number=1,
            source="rest",
            status="waiting_human",
        )
        incident_id = incident.id
    yield Store(db_url, incident_id)
    deps.reset_config_cache()
    db.dispose_engines()


@dataclass
class Standalone:
    app: FastAPI
    client: TestClient
    backend: StubBackend
    incident_id: str

    @property
    def resolve_url(self) -> str:
        return f"/api/v1/dags/{DAG}/incidents/{self.incident_id}/resolve"

    def call(self, method: str, url: str, user: str | None = "alice", **kwargs: Any) -> Any:
        headers = dict(kwargs.pop("headers", {}) or {})
        if user is not None:
            headers["X-Stub-User"] = user
        if method == "GET":
            return self.client.get(url, headers=headers, **kwargs)
        return self.client.post(url, headers={**CSRF, **headers}, **kwargs)

    def resolve(self, headers: dict[str, str]) -> Any:
        """Resolve the incident as alice, sending ``headers`` (CSRF headers included) as given."""
        sent = {"X-Stub-User": "alice", **headers}
        return self.client.post(self.resolve_url, json={"resolution": "ignore"}, headers=sent)


@pytest.fixture
def standalone(store: Store) -> Iterator[Standalone]:
    backend = StubBackend()
    app = create_app(auth=backend)
    with TestClient(app) as client:
        yield Standalone(app, client, backend, store.incident_id)


def _route_table(app: FastAPI) -> dict[tuple[str, frozenset[str]], set[Any]]:
    return {(route.path, frozenset(route.methods)): route.calls for route in iter_api_routes(app)}


# --------------------------------------------------------------------------- the Airflow host's app


def test_the_default_app_is_the_airflow_hosts_app(healer_env: Path) -> None:
    app = create_app()
    assert app.dependency_overrides == {}
    (middleware,) = app.user_middleware
    assert middleware.cls is RequestGuardMiddleware
    assert middleware.args == () and middleware.kwargs == {}
    assert app.state._state == {}  # nothing on app.state, not even the host marker
    assert serving_host(app) == app_module.AIRFLOW_PLUGIN_HOST == "airflow_plugin"
    # risk 3 of the design: the process-wide app of the Airflow plugin has no override
    assert get_app() is get_app()
    assert get_app().dependency_overrides == {}
    assert serving_host(get_app()) == "airflow_plugin"


def test_a_standalone_app_leaves_the_airflow_app_alone(store: Store) -> None:
    airflow_app = get_app()
    with TestClient(create_app(auth=StubBackend())) as client:
        assert client.get("/api/v1/me", headers={"X-Stub-User": "alice"}).status_code == 200
    assert airflow_app.dependency_overrides == {}
    assert serving_host(airflow_app) == "airflow_plugin"


def test_serving_host_reads_only_the_standalone_marker() -> None:
    assert serving_host(object()) == "airflow_plugin"
    app = FastAPI()
    setattr(app.state, app_module.HOST_STATE_ATTR, "something else")
    assert serving_host(app) == "airflow_plugin"
    setattr(app.state, app_module.HOST_STATE_ATTR, app_module.STANDALONE_HOST)
    assert serving_host(app) == "standalone"


# --------------------------------------------------------------------------- a backend on the app


def test_create_app_installs_the_backend(standalone: Standalone) -> None:
    app, backend = standalone.app, standalone.backend
    assert isinstance(backend, AuthBackend)
    assert set(app.dependency_overrides) == {auth.get_current_user, auth.get_authorizer}
    assert app.dependency_overrides[auth.get_authorizer]() is backend.recorder
    (middleware,) = app.user_middleware
    assert middleware.cls is RequestGuardMiddleware
    assert middleware.kwargs == {"allowed_urls": backend.allowed_origins}
    assert getattr(app.state, app_module.HOST_STATE_ATTR) == "standalone"
    assert serving_host(app) == "standalone"


def test_a_backend_changes_no_route(store: Store) -> None:
    routes = _route_table(create_app(auth=StubBackend()))
    assert len(routes) >= 14
    assert routes == _route_table(create_app())


def test_the_backend_guards_every_api_route(standalone: Standalone) -> None:
    recorder = standalone.backend.recorder
    routes = [r for r in iter_api_routes(standalone.app) if not r.path.startswith("/api/v1/events/")]
    assert len(routes) >= 12
    for route in routes:
        assert auth.get_current_user in route.calls, route.path
        method = "POST" if "POST" in route.methods else "GET"
        url = route.path.format(dag_id=DAG, incident_id=standalone.incident_id)
        options: dict[str, Any] = {}
        if url.endswith("/resolve"):
            options["json"] = {"resolution": "ignore"}
        if url.endswith("/task-instance"):
            options["params"] = {"run_id": "manual__1", "task_id": "build_report"}

        anonymous = standalone.call(method, url, user=None, **options)
        assert anonymous.status_code == 401, url
        assert anonymous.json() == {"detail": "Not authenticated"}

        recorder.questions.clear()
        response = standalone.call(method, url, **options)
        assert response.status_code not in (401, 403), (url, response.status_code)
        markers = {getattr(call, "healer_dag_access", None) for call in route.calls} - {None}
        if markers:  # require_dag_access asks the backend's authorizer with the path DAG
            ((dag_method, entity),) = markers
            expected = ("check_dag_access", ALICE, dag_method, entity, DAG)
            assert recorder.asked("check_dag_access")[0] == expected, url
            if dag_method == "PUT":
                denied = standalone.call(method, url, user="bob", **options)
                assert denied.status_code == 403, url
                assert denied.json() == {"detail": "Forbidden"}
        elif url != "/api/v1/me":  # the lists filter through the backend's authorizer
            assert recorder.asked("authorized_dag_ids")[0] == ("authorized_dag_ids", ALICE, "GET"), url


def test_the_principal_reaches_me_and_the_command_actor(standalone: Standalone, store: Store) -> None:
    me = standalone.call("GET", "/api/v1/me")
    assert me.status_code == 200
    assert me.json()["user"] == "Alice"
    response = standalone.resolve(CSRF)
    assert response.status_code == 202
    with db.session_scope(store.db_url) as session:
        command = session.get(Command, response.json()["command_id"])
        assert command is not None and command.actor == "Alice"
    detail = standalone.call("GET", f"/api/v1/dags/{DAG}/incidents/{standalone.incident_id}").json()
    assert detail["can_edit"] is True
    assert ("can_edit_dag", ALICE, DAG, "TASK_INSTANCE") in standalone.backend.recorder.questions


def test_the_overrides_apply_when_the_app_is_mounted(store: Store) -> None:
    backend = StubBackend()
    healer = create_app(auth=backend)

    @healer.get("/probe")
    def probe(request: Request) -> dict[str, str]:
        return {"host": serving_host(request.app)}

    outer = FastAPI()
    outer.mount("/healer", healer)
    alice = {"X-Stub-User": "alice"}
    with TestClient(outer) as client:
        assert client.get("/healer/api/v1/me").status_code == 401
        me = client.get("/healer/api/v1/me", headers=alice)
        assert me.status_code == 200 and me.json()["user"] == "Alice"
        assert client.get(f"/healer/api/v1/dags/{DAG}/incidents", headers=alice).status_code == 200
        assert backend.recorder.asked("check_dag_access") == [
            ("check_dag_access", ALICE, "GET", "TASK_INSTANCE", DAG)
        ]
        resolve = f"/healer/api/v1/dags/{DAG}/incidents/{store.incident_id}/resolve"
        body = {"resolution": "ignore"}
        accepted = client.post(resolve, json=body, headers={**alice, **CSRF, "Origin": PUBLIC_URL})
        assert accepted.status_code == 202
        foreign = client.post(resolve, json=body, headers={**alice, **CSRF, "Origin": "https://evil.example"})
        assert foreign.status_code == 403
        # request.app is the healer app itself, also when mounted
        assert client.get("/healer/probe").json() == {"host": "standalone"}


# --------------------------------------------------------------------------- CSRF origins of a backend


@pytest.mark.parametrize(
    ("headers", "expected"),
    [
        ({"Origin": PUBLIC_URL}, 202),  # the backend's origin
        ({"Origin": f"{PUBLIC_URL}:443"}, 202),
        ({"Referer": f"{PUBLIC_URL}/plugin/healer"}, 202),
        ({"Origin": "http://testserver"}, 202),  # the request's own origin
        ({}, 202),  # no Origin and no Referer: the header check alone
        ({"Origin": "https://evil.example"}, 403),
        ({"Origin": "http://healer.example"}, 403),  # scheme differs
        ({"Origin": AIRFLOW_URL}, 403),  # AIRFLOW__API__BASE_URL is not consulted
        ({"Origin": "null"}, 403),
        ({"Origin": "https://evil.example", "Referer": f"{PUBLIC_URL}/x"}, 403),  # Origin wins
    ],
)
def test_the_backend_origins_are_the_csrf_allow_list(
    standalone: Standalone, headers: dict[str, str], expected: int
) -> None:
    response = standalone.resolve({**CSRF, **headers})
    assert response.status_code == expected
    if expected == 403:
        assert response.json() == {"detail": "Cross-origin request rejected"}


def test_the_backend_alone_decides_the_extra_origins(
    standalone: Standalone, monkeypatch: pytest.MonkeyPatch
) -> None:
    # The standalone backends list HEALER_PUBLIC_URL themselves; the guard only asks them.
    monkeypatch.setenv("HEALER_PUBLIC_URL", "https://public.example")
    rejected = standalone.resolve({**CSRF, "Origin": "https://public.example"})
    assert rejected.status_code == 403


def test_the_backend_origins_are_read_on_every_checked_request(standalone: Standalone) -> None:
    backend = standalone.backend
    me = standalone.call("GET", "/api/v1/me", headers={"Origin": "https://later.example"})
    assert me.status_code == 200
    assert backend.origin_calls == 0  # safe methods are not checked
    assert standalone.resolve({**CSRF, "Origin": "https://later.example"}).status_code == 403
    backend.origins.append("https://later.example")
    assert standalone.resolve({**CSRF, "Origin": "https://later.example"}).status_code == 202
    assert backend.origin_calls == 2


def test_without_allowed_urls_the_guard_asks_the_airflow_base_urls_per_request(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    asked: list[int] = []

    def base_urls() -> list[str]:
        asked.append(1)
        return [AIRFLOW_URL]

    monkeypatch.setattr(app_module, "configured_base_urls", base_urls)
    inner = FastAPI()

    @inner.post("/x")
    def accept() -> dict[str, bool]:
        return {"ok": True}

    inner.add_middleware(RequestGuardMiddleware)
    with TestClient(inner) as client:
        assert client.post("/x", headers={**CSRF, "Origin": AIRFLOW_URL}).status_code == 200
        assert client.post("/x", headers={**CSRF, "Origin": PUBLIC_URL}).status_code == 403
    assert len(asked) == 2
    with pytest.raises(TypeError, match="callable"):
        RequestGuardMiddleware(inner, allowed_urls=[PUBLIC_URL])  # type: ignore[arg-type]


def test_the_other_request_guards_are_unchanged(standalone: Standalone) -> None:
    # bots that send a bearer token must send X-Requested-With on POST too
    missing = standalone.resolve({})
    assert missing.status_code == 403
    assert "X-Requested-With" in missing.json()["detail"]
    big = b'{"resolution": "ignore", "note": "' + b"x" * (1024 * 1024 + 10) + b'"}'
    response = standalone.client.post(
        standalone.resolve_url,
        content=big,
        headers={"X-Stub-User": "alice", "Content-Type": "application/json", **CSRF},
    )
    assert response.status_code == 413


# --------------------------------------------------------------------------- the Airflow backend


def test_the_airflow_backend_packages_the_airflow_wiring(
    healer_env: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    backend = AirflowAuthBackend()
    assert isinstance(backend, AuthBackend)
    assert backend.name == "airflow"
    assert backend.user_dependency() is auth._airflow_get_user
    assert backend.authorizer() is auth.get_authorizer()
    monkeypatch.setenv("AIRFLOW__API__BASE_URL", AIRFLOW_URL)
    monkeypatch.setenv("HEALER_PUBLIC_URL", PUBLIC_URL)
    assert backend.allowed_origins() == app_module.configured_base_urls() == [AIRFLOW_URL, PUBLIC_URL]
    monkeypatch.delenv("AIRFLOW__API__BASE_URL")
    monkeypatch.delenv("HEALER_PUBLIC_URL")
    monkeypatch.setattr(auth, "configured_base_url", lambda: "http://from-airflow:8080")
    assert backend.allowed_origins() == ["http://from-airflow:8080"]


def test_create_app_with_the_airflow_backend_sets_no_host_marker(store: Store) -> None:
    app = create_app(auth=AirflowAuthBackend())
    assert app.state._state == {}
    assert serving_host(app) == "airflow_plugin"
    assert app.dependency_overrides[auth.get_current_user] is auth._airflow_get_user
    assert app.dependency_overrides[auth.get_authorizer]() is auth.get_authorizer()


def test_the_airflow_backend_fails_closed_without_airflow(store: Store) -> None:
    if auth.AIRFLOW_AVAILABLE:  # pragma: no cover - only meaningful on hosts without Airflow
        pytest.skip("Airflow is installed")
    with TestClient(create_app(auth=AirflowAuthBackend())) as client:
        response = client.get("/api/v1/incidents", headers={"Authorization": "Bearer anything"})
        assert response.status_code == 503
        assert response.json() == {"detail": auth.UNAVAILABLE_DETAIL}
        resolve = f"/api/v1/dags/{DAG}/incidents/{store.incident_id}/resolve"
        body = {"resolution": "ignore"}
        # the Airflow base URL passes the CSRF guard (then authentication fails closed)
        assert client.post(resolve, json=body, headers={**CSRF, "Origin": AIRFLOW_URL}).status_code == 503
        foreign = client.post(resolve, json=body, headers={**CSRF, "Origin": "https://evil.example"})
        assert foreign.status_code == 403
        assert foreign.json() == {"detail": "Cross-origin request rejected"}


# --------------------------------------------------------------------------- apply_auth_backend


def test_apply_auth_backend_sets_exactly_the_two_overrides() -> None:
    app = FastAPI()
    routes = list(app.routes)
    backend = StubBackend()
    apply_auth_backend(app, backend)
    assert set(app.dependency_overrides) == {auth.get_current_user, auth.get_authorizer}
    assert app.dependency_overrides[auth.get_current_user].__name__ == "stub_user"
    assert app.dependency_overrides[auth.get_authorizer] == backend.authorizer
    assert app.routes == routes and app.user_middleware == [] and app.state._state == {}


def test_apply_auth_backend_needs_an_auth_backend() -> None:
    app = FastAPI()
    with pytest.raises(TypeError, match="AuthBackend"):
        apply_auth_backend(app, object())  # type: ignore[arg-type]

    class NoDependency(StubBackend):
        def user_dependency(self) -> Any:
            return "not a dependency"

    with pytest.raises(TypeError, match="callable"):
        apply_auth_backend(app, NoDependency())
    assert app.dependency_overrides == {}
    with pytest.raises(TypeError, match="AuthBackend"):
        create_app(auth=object())  # type: ignore[arg-type]


# --------------------------------------------------------------------------- Principal


def test_principal_is_a_frozen_hashable_user() -> None:
    claims = {"email": "alice@example.com", "sub": "0123"}
    alice = Principal(id="alice", name="Alice", groups=frozenset({"editors"}), claims=claims)
    with pytest.raises(dataclasses.FrozenInstanceError):
        alice.name = "Mallory"  # type: ignore[misc]
    same = Principal(id="alice", name="Alice", groups=frozenset({"editors"}))
    assert alice == same and hash(alice) == hash(same)  # claims are left out
    assert {alice, same} == {alice}
    assert alice != Principal(id="alice", name="Alice")
    assert alice.get_id() == "alice" and alice.get_name() == "Alice"
    assert alice.claims == claims
    assert "alice@example.com" not in repr(alice)
    plain = Principal(id="x", name="x")
    assert plain.groups == frozenset() and plain.claims == {}
    assert plain.claims is not Principal(id="y", name="y").claims


def test_principal_groups_are_a_frozenset() -> None:
    assert Principal(id="x", name="x", groups=["b", "a"]).groups == frozenset({"a", "b"})  # type: ignore[arg-type]
    hash(Principal(id="x", name="x", groups={"a"}))  # type: ignore[arg-type]
    with pytest.raises(TypeError, match="not a string"):
        Principal(id="x", name="x", groups="admin")  # type: ignore[arg-type]


def test_user_name_of_a_principal() -> None:
    assert auth.user_name(ALICE) == "Alice"
    assert auth.user_name(Principal(id="a", name="")) == "a"
    assert auth.user_name(Principal(id="", name="")) == "unknown"
