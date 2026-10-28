"""Security tests of the dag-healer API on hosts without Airflow.

Covers the route guard (every ``/api/v1`` route except ``/events/*`` depends on the Airflow
user), 401/403 handling with faked Airflow auth, the CSRF header and Origin/Referer checks,
fail-closed behaviour without Airflow and the Airflow-backed authorizer (with fake modules).
"""

from __future__ import annotations

import enum
import sys
import textwrap
import types
from collections.abc import Iterator
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

import pytest
from fastapi import Depends, FastAPI, HTTPException, Request
from fastapi.routing import APIRoute
from fastapi.testclient import TestClient

from dag_healer.api import app as app_module
from dag_healer.api import auth, deps
from dag_healer.api.app import create_app, csrf_failure, parse_origin
from dag_healer.contracts.io import save_contract
from dag_healer.contracts.schema import ColumnRule, Contract, DatasetContract, DatasetLocator
from dag_healer.store import db
from dag_healer.store import repo as store_repo

BASE_URL = "https://airflow.example.com"
TOKEN = "security-test-token"
CSRF = {"X-Requested-With": "dag-healer"}


# --------------------------------------------------------------------------- fakes


@dataclass
class FakeUser:
    name: str
    readable: set[str] | None = None
    editable: set[str] = field(default_factory=set)

    def get_name(self) -> str:
        return self.name

    def can(self, method: str, dag_id: str | None) -> bool:
        if method == "GET":
            return self.readable is None or dag_id in self.readable
        return "*" in self.editable or dag_id in self.editable


USERS = {
    "admin": FakeUser("admin", None, {"*"}),
    "viewer": FakeUser("viewer", None, set()),
    "outsider": FakeUser("outsider", {"other_dag"}, {"other_dag"}),
}


class FakeAuthorizer:
    def __init__(self) -> None:
        self.checks: list[tuple[str, str | None, str | None]] = []

    async def check_dag_access(self, request: Request, user: Any, method: str, entity: str | None) -> None:
        dag_id = request.path_params.get("dag_id")
        self.checks.append((method, entity, dag_id))
        if not user.can(method, dag_id):
            raise HTTPException(status_code=403, detail="Forbidden")

    def authorized_dag_ids(self, user: Any, method: str = "GET") -> set[str]:
        return {d for d in ("inventory_report", "other_dag") if user.can(method, d)}

    def can_read_dag(self, user: Any, dag_id: str) -> bool:
        return user.can("GET", dag_id)

    def can_edit_dag(self, user: Any, dag_id: str, entity: str = "TASK_INSTANCE") -> bool:
        return user.can("PUT", dag_id)


async def fake_current_user(request: Request) -> Any:
    name = request.headers.get("x-test-user")
    if not name:
        raise HTTPException(status_code=401, detail="Not authenticated")
    if name not in USERS:
        raise HTTPException(status_code=403, detail="Invalid JWT token")
    return USERS[name]


@dataclass
class Env:
    client: TestClient
    app: FastAPI
    authorizer: FakeAuthorizer
    incident_id: str


@pytest.fixture
def env(healer_env: Path, tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Iterator[Env]:
    repo = tmp_path / "repo"
    (repo / "dags").mkdir(parents=True)
    contract = Contract(
        dag_id="inventory_report",
        datasets=[
            DatasetContract(
                name="inventory",
                task_id="build_report",
                locator=DatasetLocator(type="csv", path="data/incoming/inventory.csv"),
                columns=[ColumnRule(name="qty", dtype="integer")],
            )
        ],
    )
    save_contract(contract, repo / "healer" / "contracts" / "inventory_report.yaml")
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
    monkeypatch.setenv("HEALER_INGEST_TOKEN", TOKEN)
    monkeypatch.setenv("AIRFLOW__API__BASE_URL", BASE_URL)
    monkeypatch.delenv("HEALER_PUBLIC_URL", raising=False)
    db.dispose_engines()
    deps.reset_config_cache()
    deps.ingest_rate_limiter.reset()
    with db.session_scope(db_url) as session:
        incident, _ = store_repo.create_incident(
            session,
            dag_id="inventory_report",
            run_id="manual__1",
            task_id="build_report",
            try_number=1,
            source="rest",
            status="waiting_human",
        )
        incident_id = incident.id
    authorizer = FakeAuthorizer()
    app = create_app()
    app.dependency_overrides[auth.get_current_user] = fake_current_user
    app.dependency_overrides[auth.get_authorizer] = lambda: authorizer
    with TestClient(app) as client:
        yield Env(client, app, authorizer, incident_id)
    deps.reset_config_cache()
    db.dispose_engines()


# --------------------------------------------------------------------------- route guard


@dataclass
class RouteInfo:
    path: str
    methods: set[str]
    calls: set[Any]


def _dependency_calls(dependant: Any) -> set[Any]:
    calls: set[Any] = set()
    stack = list(getattr(dependant, "dependencies", []) or [])
    while stack:
        current = stack.pop()
        if current.call is not None:
            calls.add(current.call)
        stack.extend(current.dependencies or [])
    return calls


def iter_api_routes(app: FastAPI) -> list[RouteInfo]:
    """Every API route of ``app`` (works with flattened and lazily included routers)."""
    try:
        from fastapi.routing import iter_route_contexts  # FastAPI >= 0.140
    except ImportError:  # pragma: no cover - older FastAPI (e.g. the Airflow image)
        candidates: list[Any] = [route for route in app.routes if isinstance(route, APIRoute)]
    else:
        candidates = [c for c in iter_route_contexts(app.routes) if getattr(c, "dependant", None) is not None]
    return [
        RouteInfo(path=route.path, methods=set(route.methods or ()), calls=_dependency_calls(route.dependant))
        for route in candidates
        if route.path.startswith("/api/")
    ]


def test_every_api_route_requires_an_airflow_user() -> None:
    routes = iter_api_routes(create_app())
    assert len(routes) >= 14
    unprotected = [
        route.path
        for route in routes
        if not route.path.startswith("/api/v1/events/") and auth.get_current_user not in route.calls
    ]
    assert unprotected == []
    events = [route for route in routes if route.path.startswith("/api/v1/events/")]
    # the static task-failed route comes first, so it keeps matching before the generic one
    assert [route.path for route in events] == ["/api/v1/events/task-failed", "/api/v1/events/{channel}"]
    assert all(deps.require_ingest_token in route.calls for route in events)
    assert all(route.path.startswith("/api/v1/") for route in routes)


def test_dag_routes_check_dag_permissions() -> None:
    for route in iter_api_routes(create_app()):
        if "{dag_id}" not in route.path:
            assert not route.path.startswith("/api/v1/dags/"), route.path
            continue
        markers = {getattr(call, "healer_dag_access", None) for call in route.calls} - {None}
        assert len(markers) == 1, route.path
        ((method, entity),) = markers
        expected_method = "PUT" if "POST" in route.methods else "GET"
        assert method == expected_method, route.path
        # reading a contract (its view or its file) needs DAG Code; approving needs edit on the
        # DAG itself (FAB has no "can_edit on DAG Code", so PUT on CODE would be denied to everyone)
        if route.path.endswith("/contract/approve"):
            expected_entity = None
        elif route.path.endswith(("/contract", "/contract.yaml")):
            expected_entity = "CODE"
        else:
            expected_entity = "TASK_INSTANCE"
        assert entity == expected_entity, route.path


def _route_samples(incident_id: str) -> list[tuple[str, str, dict[str, Any] | None]]:
    base = f"/api/v1/dags/inventory_report/incidents/{incident_id}"
    return [
        ("GET", "/api/v1/me", None),
        ("GET", "/api/v1/setup", None),
        ("GET", "/api/v1/incidents", None),
        ("GET", "/api/v1/dags", None),
        ("GET", "/api/v1/contracts", None),
        ("GET", "/api/v1/dags/inventory_report/incidents", None),
        ("GET", base, None),
        ("GET", f"{base}/pr.md", None),
        ("GET", "/api/v1/dags/inventory_report/task-instance?run_id=manual__1&task_id=build_report", None),
        ("GET", "/api/v1/dags/inventory_report/contract", None),
        ("GET", "/api/v1/dags/inventory_report/contract.yaml", None),
        ("POST", f"{base}/resolve", {"resolution": "ignore"}),
        ("POST", f"{base}/retry-repair", None),
        ("POST", f"{base}/retry-task", None),
        ("POST", "/api/v1/dags/inventory_report/contract/approve", None),
    ]


def _call(env: Env, method: str, path: str, body: Any, headers: dict[str, str]) -> Any:
    if method == "GET":
        return env.client.get(path, headers=headers)
    return env.client.post(path, json=body, headers={**CSRF, **headers})


def test_unauthenticated_requests_get_401(env: Env) -> None:
    for method, path, body in _route_samples(env.incident_id):
        response = _call(env, method, path, body, {})
        assert response.status_code == 401, (method, path)
        assert response.json() == {"detail": "Not authenticated"}


def test_invalid_token_gets_403(env: Env) -> None:
    for method, path, body in _route_samples(env.incident_id):
        assert _call(env, method, path, body, {"X-Test-User": "forged"}).status_code == 403, path


def test_user_without_dag_access_gets_403(env: Env) -> None:
    for method, path, body in _route_samples(env.incident_id):
        response = _call(env, method, path, body, {"X-Test-User": "outsider"})
        if "/dags/inventory_report" in path:
            assert response.status_code == 403, path
            assert response.json() == {"detail": "Forbidden"}
        else:
            assert response.status_code == 200, path
    assert env.client.get("/api/v1/incidents", headers={"X-Test-User": "outsider"}).json()["total"] == 0


def test_viewer_cannot_act(env: Env) -> None:
    for method, path, body in _route_samples(env.incident_id):
        if method != "POST":
            continue
        assert _call(env, method, path, body, {"X-Test-User": "viewer"}).status_code == 403, path
    env.authorizer.checks.clear()
    response = _call(
        env, "POST", f"/api/v1/dags/inventory_report/incidents/{env.incident_id}/resolve",
        {"resolution": "ignore"}, {"X-Test-User": "admin"},
    )  # fmt: skip
    assert response.status_code == 202
    assert env.authorizer.checks == [("PUT", "TASK_INSTANCE", "inventory_report")]
    approve = _call(
        env, "POST", "/api/v1/dags/inventory_report/contract/approve", None, {"X-Test-User": "admin"}
    )
    assert approve.status_code == 202
    assert env.authorizer.checks[-1] == ("PUT", None, "inventory_report")


def test_a_pending_command_in_a_list_says_who_and_what_only(env: Env) -> None:
    note = "call the vendor on +44 20 7946 0958 about the qty column"
    queued = _call(
        env, "POST", f"/api/v1/dags/inventory_report/incidents/{env.incident_id}/resolve",
        {"resolution": "data", "note": note}, {"X-Test-User": "admin"},
    )  # fmt: skip
    assert queued.status_code == 202
    for path in ("/api/v1/incidents", "/api/v1/dags/inventory_report/incidents", "/api/v1/dags"):
        response = env.client.get(path, headers={"X-Test-User": "viewer"})
        assert response.status_code == 200, path
        assert "vendor" not in response.text and "7946" not in response.text, path
    (listed,) = env.client.get("/api/v1/incidents", headers={"X-Test-User": "viewer"}).json()["incidents"]
    assert set(listed["pending_command"]) == {"id", "kind", "created_at", "actor", "action"}
    outsider = env.client.get("/api/v1/incidents", headers={"X-Test-User": "outsider"}).json()
    assert outsider == {"incidents": [], "total": 0}


# --------------------------------------------------------------------------- CSRF


def _resolve(env: Env, headers: dict[str, str]) -> Any:
    return env.client.post(
        f"/api/v1/dags/inventory_report/incidents/{env.incident_id}/resolve",
        json={"resolution": "ignore"},
        headers={"X-Test-User": "admin", **headers},
    )


def test_post_requires_requested_with_header(env: Env) -> None:
    missing = _resolve(env, {})
    assert missing.status_code == 403
    assert "X-Requested-With" in missing.json()["detail"]
    assert _resolve(env, {"X-Requested-With": "XMLHttpRequest"}).status_code == 403
    assert env.client.post("/api/v1/dags/inventory_report/contract/approve").status_code == 403
    assert _resolve(env, {"X-Requested-With": "Dag-Healer"}).status_code == 202


@pytest.mark.parametrize(
    ("headers", "expected"),
    [
        ({"Origin": BASE_URL}, 202),
        ({"Origin": "https://airflow.example.com:443"}, 202),
        ({"Origin": "http://testserver"}, 202),  # the request's own host
        ({"Referer": f"{BASE_URL}/dags/x/plugin/healer-ti"}, 202),
        ({"Origin": "https://evil.example.com"}, 403),
        ({"Origin": "http://airflow.example.com"}, 403),  # scheme differs
        ({"Origin": "https://airflow.example.com:8443"}, 403),  # port differs
        ({"Origin": "null"}, 403),
        ({"Referer": "https://evil.example.com/page"}, 403),
        ({"Origin": "https://evil.example.com", "Referer": f"{BASE_URL}/x"}, 403),  # Origin wins
    ],
)
def test_post_origin_checks(env: Env, headers: dict[str, str], expected: int) -> None:
    response = _resolve(env, {**CSRF, **headers})
    assert response.status_code == expected
    if expected == 403:
        assert response.json() == {"detail": "Cross-origin request rejected"}


def test_events_are_exempt_from_csrf_but_need_the_token(env: Env) -> None:
    payload = {"dag_id": "inventory_report", "task_id": "build_report", "run_id": "manual__2"}
    url = "/api/v1/events/task-failed"
    assert env.client.post(url, json=payload).status_code == 401
    assert env.client.post(url, json=payload, headers={"X-Healer-Token": "nope"}).status_code == 401
    response = env.client.post(
        url, json=payload, headers={"X-Healer-Token": TOKEN, "Origin": "https://evil.x"}
    )
    assert response.status_code == 202


def test_channel_events_are_exempt_from_csrf_but_need_the_token(env: Env) -> None:
    from dag_healer.orchestrators.fake import FakeAdapter, FakeWorld

    deps.channel_rate_limiter.reset()
    env.app.dependency_overrides[deps.get_adapter] = lambda: FakeAdapter(world=FakeWorld())
    payload = {"pipeline_id": "inventory_report", "run_id": "run-1", "step_id": "build_report"}
    url = "/api/v1/events/fake"
    assert env.client.post(url, json=payload).status_code == 401
    assert env.client.post(url, json=payload, headers={"X-Healer-Token": "nope"}).status_code == 401
    response = env.client.post(
        url, json=payload, headers={"X-Healer-Token": TOKEN, "Origin": "https://evil.x"}
    )
    assert response.status_code == 202
    assert response.json()["accepted"] is True
    # an unknown channel is only revealed to callers that hold the token
    assert env.client.post("/api/v1/events/eventbridge", json=payload).status_code == 401


def test_request_body_is_capped(env: Env) -> None:
    big = b'{"resolution": "ignore", "note": "' + b"x" * (1024 * 1024 + 10) + b'"}'
    response = env.client.post(
        f"/api/v1/dags/inventory_report/incidents/{env.incident_id}/resolve",
        content=big,
        headers={"X-Test-User": "admin", "Content-Type": "application/json", **CSRF},
    )
    assert response.status_code == 413


def test_get_requests_are_not_csrf_checked(env: Env) -> None:
    response = env.client.get("/api/v1/me", headers={"X-Test-User": "viewer", "Origin": "https://evil.x"})
    assert response.status_code == 200


def test_csrf_failure_unit() -> None:
    own = ("http", "testserver", 80)
    ok = {"x-requested-with": "dag-healer"}
    assert csrf_failure("GET", "/api/v1/x", {}, request_origin=own, allowed_urls=[]) is None
    assert csrf_failure("POST", "/api/v1/events/task-failed", {}, request_origin=own, allowed_urls=[]) is None
    assert csrf_failure("POST", "/api/v1/x", {}, request_origin=own, allowed_urls=[]) is not None
    assert csrf_failure("DELETE", "/api/v1/x", {}, request_origin=own, allowed_urls=[]) is not None
    assert csrf_failure("POST", "/api/v1/x", ok, request_origin=own, allowed_urls=[]) is None
    assert (
        csrf_failure("POST", "/x", {**ok, "origin": "http://testserver"}, request_origin=own, allowed_urls=[])
        is None
    )
    assert (
        csrf_failure("POST", "/x", {**ok, "origin": "http://other"}, request_origin=None, allowed_urls=[])
        is not None
    )
    assert (
        csrf_failure(
            "POST",
            "/x",
            {**ok, "origin": "http://a:8080"},
            request_origin=None,
            allowed_urls=["http://a:8080/sub"],
        )
        is None
    )


def test_parse_origin() -> None:
    assert parse_origin("https://Example.com") == ("https", "example.com", 443)
    assert parse_origin("http://example.com:8080/path?q=1") == ("http", "example.com", 8080)
    assert parse_origin("null") is None
    assert parse_origin("ftp://example.com") is None
    assert parse_origin("http://example.com:99999") is None
    assert parse_origin("") is None and parse_origin(None) is None


def test_configured_base_urls_fall_back_to_airflow_config(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("AIRFLOW__API__BASE_URL", raising=False)
    monkeypatch.setenv("HEALER_PUBLIC_URL", "https://public.example")
    assert app_module.configured_base_urls() == ["https://public.example"]
    monkeypatch.delenv("HEALER_PUBLIC_URL")
    monkeypatch.setattr(auth, "configured_base_url", lambda: "http://from-airflow:8080")
    assert app_module.configured_base_urls() == ["http://from-airflow:8080"]


# --------------------------------------------------------------------------- fail closed


def test_without_airflow_auth_fails_closed(env: Env) -> None:
    if auth.AIRFLOW_AVAILABLE:  # pragma: no cover - only meaningful on hosts without Airflow
        pytest.skip("Airflow is installed")
    env.app.dependency_overrides.clear()
    response = env.client.get("/api/v1/incidents", headers={"Authorization": "Bearer anything"})
    assert response.status_code == 503
    response = env.client.get("/api/v1/dags/inventory_report/incidents")
    assert response.status_code == 503
    assert env.client.get("/api/v1/me").status_code == 503
    payload = {"dag_id": "inventory_report", "task_id": "build_report", "run_id": "manual__3"}
    events = env.client.post("/api/v1/events/task-failed", json=payload, headers={"X-Healer-Token": TOKEN})
    assert events.status_code == 202
    unavailable = auth.UnavailableAuthorizer()
    assert unavailable.authorized_dag_ids(object()) == set()
    assert unavailable.can_read_dag(object(), "x") is False
    assert unavailable.can_edit_dag(object(), "x") is False


def test_require_dag_access_is_cached_and_validated() -> None:
    first = auth.require_dag_access("GET", "TASK_INSTANCE")
    assert auth.require_dag_access("GET", "TASK_INSTANCE") is first
    assert first.healer_dag_access == ("GET", "TASK_INSTANCE")
    assert auth.require_dag_access("PUT", "CODE") is not first
    with pytest.raises(ValueError):
        auth.require_dag_access("PATCH", "CODE")
    with pytest.raises(ValueError):
        auth.require_dag_access("GET", "NOPE")


def test_user_name_fallbacks() -> None:
    assert auth.user_name(FakeUser("ann")) == "ann"

    class Broken:
        def get_name(self) -> str:
            raise RuntimeError("boom")

        username = "fallback"

    assert auth.user_name(Broken()) == "fallback"
    assert auth.user_name(object()) == "unknown"


# --------------------------------------------------------------------------- Airflow authorizer


class _Entity(enum.Enum):
    CODE = "CODE"
    TASK_INSTANCE = "TASK_INSTANCE"


@dataclass
class _DagDetails:
    id: str | None = None
    team_name: str | None = None


@pytest.fixture
def fake_airflow_modules(monkeypatch: pytest.MonkeyPatch) -> dict[str, Any]:
    """Minimal ``airflow`` modules the authorizer imports lazily."""
    state: dict[str, Any] = {"team_lookups": []}
    names = [
        "airflow",
        "airflow.api_fastapi",
        "airflow.api_fastapi.auth",
        "airflow.api_fastapi.auth.managers",
        "airflow.api_fastapi.auth.managers.models",
        "airflow.api_fastapi.auth.managers.models.resource_details",
        "airflow.models",
        "airflow.models.dag",
    ]
    modules = {name: types.ModuleType(name) for name in names}
    modules["airflow.api_fastapi.auth.managers.models.resource_details"].DagAccessEntity = _Entity
    modules["airflow.api_fastapi.auth.managers.models.resource_details"].DagDetails = _DagDetails

    class DagModel:
        @staticmethod
        def get_team_name(dag_id: str) -> str | None:
            state["team_lookups"].append(dag_id)
            if dag_id == "explode":
                raise RuntimeError("db down")
            return "team-a" if dag_id == "team_dag" else None

    modules["airflow.models.dag"].DagModel = DagModel
    for name, module in modules.items():
        monkeypatch.setitem(sys.modules, name, module)
    return state


class _FakeAuthManager:
    def __init__(self) -> None:
        self.calls: list[dict[str, Any]] = []

    def get_authorized_dag_ids(self, *, user: Any, method: str = "GET") -> set[str]:
        self.calls.append({"fn": "get_authorized_dag_ids", "user": user, "method": method})
        return {"a", "b"}

    def is_authorized_dag(self, *, method: str, access_entity: Any, details: Any, user: Any) -> bool:
        self.calls.append(
            {"fn": "is_authorized_dag", "method": method, "entity": access_entity, "details": details}
        )
        return method == "GET" or details.id == "team_dag"


def _fake_security(manager: _FakeAuthManager, *, async_inner: bool) -> types.ModuleType:
    security = types.ModuleType("fake_security")
    seen: list[tuple[str, Any, Any, Any]] = []

    def requires_access_dag(method: str, access_entity: Any = None, param_dag_id: str | None = None) -> Any:
        if async_inner:

            async def inner(request: Request, user: Any) -> None:
                seen.append((method, access_entity, request.path_params.get("dag_id"), user))
                if user == "denied":
                    raise HTTPException(403, "Forbidden")

            return inner

        def inner_sync(request: Request, user: Any) -> None:
            seen.append((method, access_entity, request.path_params.get("dag_id"), user))
            if user == "denied":
                raise HTTPException(403, "Forbidden")

        return inner_sync

    security.requires_access_dag = requires_access_dag  # type: ignore[attr-defined]
    security.get_auth_manager = lambda: manager  # type: ignore[attr-defined]
    security.seen = seen  # type: ignore[attr-defined]
    return security


@pytest.mark.parametrize("async_inner", [False, True])
def test_airflow_authorizer_delegates_to_requires_access_dag(
    fake_airflow_modules: dict[str, Any], async_inner: bool
) -> None:
    manager = _FakeAuthManager()
    security = _fake_security(manager, async_inner=async_inner)
    authorizer = auth.AirflowAuthorizer(security)
    app = FastAPI()
    dependency = auth.require_dag_access("PUT", "CODE")

    @app.get("/dags/{dag_id}/thing")
    def thing(dag_id: str, user: Any = Depends(dependency)) -> dict[str, Any]:
        return {"user": user}

    current = {"user": "alice"}
    app.dependency_overrides[auth.get_current_user] = lambda: current["user"]
    app.dependency_overrides[auth.get_authorizer] = lambda: authorizer
    with TestClient(app) as client:
        assert client.get("/dags/my_dag/thing").json() == {"user": "alice"}
        current["user"] = "denied"
        assert client.get("/dags/my_dag/thing").status_code == 403
    assert security.seen[0] == ("PUT", _Entity.CODE, "my_dag", "alice")


def test_airflow_authorizer_permission_helpers(fake_airflow_modules: dict[str, Any]) -> None:
    manager = _FakeAuthManager()
    authorizer = auth.AirflowAuthorizer(_fake_security(manager, async_inner=False))
    assert authorizer.authorized_dag_ids("u") == {"a", "b"}
    assert manager.calls[-1] == {"fn": "get_authorized_dag_ids", "user": "u", "method": "GET"}
    assert authorizer.can_read_dag("u", "x") is True
    assert manager.calls[-1]["entity"] is None
    assert authorizer.can_edit_dag("u", "x") is False
    assert authorizer.can_edit_dag("u", "team_dag") is True
    call = manager.calls[-1]
    assert call["method"] == "PUT" and call["entity"] is _Entity.TASK_INSTANCE
    assert call["details"] == _DagDetails(id="team_dag", team_name="team-a")
    assert authorizer.can_edit_dag("u", "team_dag", "CODE") is True
    assert manager.calls[-1]["entity"] is _Entity.CODE
    assert authorizer.can_edit_dag("u", "explode") is False  # lookup failure: deny
    assert "explode" in fake_airflow_modules["team_lookups"]


def test_namespace_airflow_directory_is_not_an_installation(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    if auth.AIRFLOW_AVAILABLE:  # pragma: no cover - only meaningful on hosts without Airflow
        pytest.skip("Airflow is installed")
    (tmp_path / "airflow").mkdir()  # like tests/airflow when pytest puts tests/ on sys.path
    monkeypatch.syspath_prepend(str(tmp_path))
    monkeypatch.delitem(sys.modules, "airflow", raising=False)
    assert auth.airflow_installed() is False
    assert app_module._airflow_installed() is False
    error = ModuleNotFoundError("No module named 'airflow.api_fastapi'", name="airflow.api_fastapi")
    assert auth._is_missing_airflow(error) is True
    assert auth._is_missing_airflow(ModuleNotFoundError("x", name="pandas")) is False
