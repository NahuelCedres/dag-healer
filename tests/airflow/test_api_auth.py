"""Authentication and authorization of the /healer API inside the real Airflow app.

SimpleAuthManager: ``admin`` may do everything, ``viewer`` may only read (GET) DAGs.
"""

from __future__ import annotations

import os
from typing import Any

import pytest

API = "/healer/api/v1"
CSRF = {"X-Requested-With": "dag-healer"}
INGEST_TOKEN = os.environ["HEALER_INGEST_TOKEN"]


def bearer(token: str) -> dict[str, str]:
    return {"Authorization": f"Bearer {token}"}


def incident_url(incident_id: str, suffix: str = "") -> str:
    return f"{API}/dags/inventory_report/incidents/{incident_id}{suffix}"


# --------------------------------------------------------------------------- public routes


def test_health_is_public(client: Any) -> None:
    response = client.get("/healer/health")
    assert response.status_code == 200
    assert response.json()["status"] == "ok"


def test_bundle_is_served_as_javascript(client: Any) -> None:
    response = client.get("/healer/static/main.umd.cjs")
    assert response.status_code == 200
    assert response.headers["content-type"].startswith("application/javascript")
    assert response.headers["cache-control"] == "no-cache"


# --------------------------------------------------------------------------- authentication


@pytest.mark.parametrize(
    "path",
    [
        f"{API}/incidents",
        f"{API}/me",
        f"{API}/dags",
        f"{API}/contracts",
        f"{API}/dags/inventory_report/incidents",
        f"{API}/dags/inventory_report/contract",
    ],
)
def test_no_token_is_401(client: Any, seeded: dict[str, str], path: str) -> None:
    response = client.get(path)
    assert response.status_code == 401
    assert response.json() == {"detail": "Not authenticated"}


def test_no_token_is_401_on_incident_detail(client: Any, seeded: dict[str, str]) -> None:
    assert client.get(incident_url(seeded["waiting"])).status_code == 401


def test_garbage_token_is_rejected(client: Any, seeded: dict[str, str]) -> None:
    for path in (f"{API}/incidents", f"{API}/dags/inventory_report/incidents"):
        response = client.get(path, headers=bearer("not-a-jwt"))
        assert response.status_code in (401, 403)


def test_cookie_authentication_works_like_the_ui(client: Any, tokens: dict[str, str]) -> None:
    response = client.get(f"{API}/me", headers={"Cookie": f"_token={tokens['viewer']}"})
    assert response.status_code == 200
    assert response.json()["user"] == "viewer"


# --------------------------------------------------------------------------- viewer


def test_viewer_can_read(client: Any, tokens: dict[str, str], seeded: dict[str, str]) -> None:
    headers = bearer(tokens["viewer"])
    me = client.get(f"{API}/me", headers=headers)
    assert me.status_code == 200
    assert me.json() == {"user": "viewer", "base_url": "http://localhost:8080"}

    listing = client.get(f"{API}/incidents", headers=headers)
    assert listing.status_code == 200
    assert seeded["waiting"] in {item["id"] for item in listing.json()["incidents"]}
    assert "ops@example.com" not in listing.text  # error summaries are masked

    dag = client.get(f"{API}/dags/inventory_report/incidents", headers=headers)
    assert dag.status_code == 200
    assert dag.json()["can_edit"] is False
    assert dag.json()["contract"]["status"] == "draft"
    assert dag.json()["contract"]["can_approve"] is False

    detail = client.get(incident_url(seeded["waiting"]), headers=headers)
    assert detail.status_code == 200
    assert detail.json()["allowed_actions"] == []
    assert detail.json()["can_edit"] is False

    for path in (f"{API}/dags", f"{API}/contracts", f"{API}/dags/inventory_report/contract"):
        assert client.get(path, headers=headers).status_code == 200, path
    ti = client.get(
        f"{API}/dags/inventory_report/task-instance",
        params={"run_id": "manual__2026-10-01T00:00:00+00:00", "task_id": "build_report"},
        headers=headers,
    )
    assert ti.status_code == 200
    assert ti.json()["incident"]["id"] == seeded["waiting"]


def test_viewer_cannot_act(client: Any, tokens: dict[str, str], seeded: dict[str, str]) -> None:
    headers = {**bearer(tokens["viewer"]), **CSRF}
    requests = [
        (incident_url(seeded["waiting"], "/resolve"), {"resolution": "data"}),
        (incident_url(seeded["failed_repair"], "/retry-repair"), None),
        (incident_url(seeded["reported"], "/retry-task"), None),
        (f"{API}/dags/inventory_report/contract/approve", None),
    ]
    for path, body in requests:
        response = client.post(path, json=body, headers=headers)
        assert response.status_code == 403, path
        assert response.json() == {"detail": "Forbidden"}


# --------------------------------------------------------------------------- admin


def test_admin_can_act(client: Any, tokens: dict[str, str], seeded: dict[str, str]) -> None:
    headers = {**bearer(tokens["admin"]), **CSRF}
    detail = client.get(incident_url(seeded["waiting_admin"]), headers=headers)
    assert detail.json()["can_edit"] is True
    assert detail.json()["allowed_actions"] == ["resolve_data", "resolve_code", "retry_task", "ignore"]

    resolve = client.post(
        incident_url(seeded["waiting_admin"], "/resolve"), json={"resolution": "code"}, headers=headers
    )
    assert resolve.status_code == 202
    assert resolve.json()["status"] == "pending"
    retry = client.post(incident_url(seeded["failed_repair"], "/retry-repair"), headers=headers)
    assert retry.status_code == 202
    task = client.post(incident_url(seeded["reported"], "/retry-task"), headers=headers)
    assert task.status_code == 202
    approve = client.post(
        f"{API}/dags/inventory_report/contract/approve", json={"note": "ok"}, headers=headers
    )
    assert approve.status_code == 202

    after = client.get(incident_url(seeded["waiting_admin"]), headers=headers).json()
    assert after["allowed_actions"] == []
    assert after["pending_commands"][0]["actor"] == "admin"
    conflict = client.post(
        incident_url(seeded["reported"], "/resolve"), json={"resolution": "data"}, headers=headers
    )
    assert conflict.status_code == 409


def test_post_without_requested_with_is_403(
    client: Any, tokens: dict[str, str], seeded: dict[str, str]
) -> None:
    response = client.post(
        incident_url(seeded["waiting"], "/resolve"),
        json={"resolution": "ignore"},
        headers=bearer(tokens["admin"]),
    )
    assert response.status_code == 403
    assert "X-Requested-With" in response.json()["detail"]


def test_cookie_post_checks_origin(client: Any, tokens: dict[str, str], seeded: dict[str, str]) -> None:
    cookie = {"Cookie": f"_token={tokens['admin']}", **CSRF}
    cross = client.post(
        incident_url(seeded["waiting"], "/resolve"),
        json={"resolution": "ignore"},
        headers={**cookie, "Origin": "http://evil.example"},
    )
    assert cross.status_code == 403
    same = client.post(
        incident_url(seeded["waiting"], "/resolve"),
        json={"resolution": "ignore"},
        headers={**cookie, "Origin": "http://localhost:8080"},
    )
    assert same.status_code == 202


# --------------------------------------------------------------------------- ingest


def test_events_require_the_ingest_token(client: Any, airflow_db: None) -> None:
    payload = {
        "dag_id": "inventory_report",
        "task_id": "build_report",
        "run_id": "manual__2026-10-09T00:00:00+00:00",
        "try_number": 1,
        "error": "KeyError: 'qty'",
        "source": "listener",
    }
    url = f"{API}/events/task-failed"
    assert client.post(url, json=payload).status_code == 401
    assert client.post(url, json=payload, headers={"X-Healer-Token": "wrong"}).status_code == 401
    accepted = client.post(url, json=payload, headers={"X-Healer-Token": INGEST_TOKEN})
    assert accepted.status_code == 202
    assert accepted.json()["accepted"] is True
    assert isinstance(accepted.json()["pending_event_id"], int)


# --------------------------------------------------------------------------- route guard


def test_every_mounted_route_requires_authentication(airflow_app: Any) -> None:
    from fastapi.routing import APIRoute, Mount

    from dag_healer.api import auth

    (mount,) = [r for r in airflow_app.routes if isinstance(r, Mount) and r.path == "/healer"]
    routes = [route for route in mount.app.routes if isinstance(route, APIRoute)]
    api_routes = [route for route in routes if route.path.startswith("/api/v1/")]
    assert len(api_routes) >= 14

    def calls(dependant: Any) -> set[Any]:
        found: set[Any] = set()
        stack = list(dependant.dependencies)
        while stack:
            current = stack.pop()
            found.add(current.call)
            stack.extend(current.dependencies)
        return found

    unprotected = [
        route.path
        for route in api_routes
        if not route.path.startswith("/api/v1/events/")
        and auth.get_current_user not in calls(route.dependant)
    ]
    assert unprotected == []
    assert auth.AIRFLOW_AVAILABLE is True
