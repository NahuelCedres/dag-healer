"""Host tests of the dag-healer HTTP API routes (no Airflow: auth is faked with overrides).

Users (header ``X-Test-User``):

* ``admin`` reads and edits every DAG;
* ``viewer`` reads every DAG and edits none;
* ``limited`` reads ``sales_daily_etl`` and ``customers_sync`` and edits ``customers_sync``;
* no header -> 401 (as Airflow's ``get_user`` without credentials).
"""

from __future__ import annotations

import json
import logging
import shutil
import textwrap
from collections.abc import Iterator
from dataclasses import dataclass, field, replace
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

import pytest
from fastapi import HTTPException, Request
from fastapi.testclient import TestClient
from sqlalchemy import select

from dag_healer.airflow.adapter import AirflowAdapter
from dag_healer.api import auth, deps, schemas, views
from dag_healer.api.app import create_app
from dag_healer.config import config_from_mapping
from dag_healer.contracts.io import approve_contract, compute_source_hash, save_contract
from dag_healer.contracts.schema import ColumnRule, Contract, DatasetContract, DatasetLocator
from dag_healer.core.specs import TaskInfo
from dag_healer.orchestrators import registry
from dag_healer.orchestrators.fake import FAKE_CAPABILITIES, FAKE_VOCABULARY, FakeAdapter, FakeWorld
from dag_healer.store import db
from dag_healer.store import repo as store_repo
from dag_healer.store.migrations import backup_path, current_version
from dag_healer.store.models import Command, IncidentEvent, PendingEvent

RAW_EMAIL = "jane.doe@example.com"
RAW_PHONE = "+1 555 123 4567"
INGEST_TOKEN = "unit-test-ingest-token"
BASE_URL = "http://airflow.example:8080"
T0 = datetime(2026, 10, 1, 12, 0, tzinfo=timezone.utc)
AIRFLOW_DAGS = {"sales_daily_etl", "customers_sync", "inventory_report", "secret_dag"}
CSRF = {"X-Requested-With": "dag-healer"}


# --------------------------------------------------------------------------- fake Airflow auth


@dataclass
class FakeUser:
    name: str
    readable: set[str] | None = None  # None: every DAG
    editable: set[str] = field(default_factory=set)

    def get_name(self) -> str:
        return self.name

    def get_id(self) -> str:
        return self.name

    def can(self, method: str, dag_id: str | None) -> bool:
        if method == "GET":
            return self.readable is None or dag_id in self.readable
        return "*" in self.editable or dag_id in self.editable


USERS = {
    "admin": FakeUser("admin", None, {"*"}),
    "viewer": FakeUser("viewer", None, set()),
    "limited": FakeUser("limited", {"sales_daily_etl", "customers_sync"}, {"customers_sync"}),
}


class FakeAuthorizer:
    """Mirrors the Airflow authorizer: path ``dag_id`` + method checks."""

    def __init__(self) -> None:
        self.checks: list[tuple[str, str | None, str | None]] = []

    async def check_dag_access(self, request: Request, user: Any, method: str, entity: str | None) -> None:
        dag_id = request.path_params.get("dag_id")
        self.checks.append((method, entity, dag_id))
        if not user.can(method, dag_id):
            raise HTTPException(status_code=403, detail="Forbidden")

    def authorized_dag_ids(self, user: Any, method: str = "GET") -> set[str]:
        return {dag_id for dag_id in AIRFLOW_DAGS if user.can(method, dag_id)}

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


# --------------------------------------------------------------------------- fixtures


def _contract(dag_id: str, dataset: str, task_id: str, columns: list[ColumnRule]) -> Contract:
    return Contract(
        dag_id=dag_id,
        datasets=[
            DatasetContract(
                name=dataset,
                task_id=task_id,
                locator=DatasetLocator(type="csv", path=f"data/incoming/{dataset}.csv"),
                columns=columns,
            )
        ],
    )


def write_repo(root: Path) -> Path:
    (root / "dags").mkdir(parents=True)
    contracts = root / "healer" / "contracts"
    sales = _contract(
        "sales_daily_etl",
        "sales",
        "load_sales",
        [
            ColumnRule(name="order_id", dtype="integer", nullable=False),
            ColumnRule(name="customer_email", dtype="string", pii=True),
            ColumnRule(name="amount", dtype="float", nullable=False, min=0),
            ColumnRule(name="note", dtype="string"),
        ],
    )
    save_contract(approve_contract(sales, "alice"), contracts / "sales_daily_etl.yaml")
    customers = approve_contract(
        _contract(
            "customers_sync", "customers", "transform", [ColumnRule(name="signup_date", dtype="string")]
        ),
        "bob",
    )
    edited = customers.model_copy(update={"notes": "edited after approval"})
    save_contract(edited, contracts / "customers_sync.yaml")
    save_contract(
        _contract("inventory_report", "inventory", "build_report", [ColumnRule(name="qty", dtype="integer")]),
        contracts / "inventory_report.yaml",
    )
    save_contract(
        _contract("secret_dag", "secret", "t", [ColumnRule(name="x", dtype="integer")]),
        contracts / "secret_dag.yaml",
    )
    (contracts / "broken.yaml").write_text("dag_id: [unclosed\n", encoding="utf-8")
    config = root / "healer.yaml"
    config.write_text(
        textwrap.dedent(
            """\
            version: 1
            repo: {path: ., dags_dir: dags}
            monitor: {dags: ["*", "!unmonitored_*"]}
            contracts: {dir: healer/contracts}
            masking: {sample_rows: 5}
            """
        ),
        encoding="utf-8",
    )
    return config


@dataclass
class Api:
    client: TestClient
    repo: Path
    home: Path
    db_url: str
    authorizer: FakeAuthorizer
    ids: dict[str, str]

    def get(self, path: str, user: str | None = "admin", **kwargs: Any) -> Any:
        headers = dict(kwargs.pop("headers", {}) or {})
        if user:
            headers["X-Test-User"] = user
        return self.client.get(f"/api/v1{path}", headers=headers, **kwargs)

    def post(self, path: str, user: str | None = "admin", *, csrf: bool = True, **kwargs: Any) -> Any:
        headers = dict(kwargs.pop("headers", {}) or {})
        if csrf:
            headers.update(CSRF)
        if user:
            headers["X-Test-User"] = user
        return self.client.post(f"/api/v1{path}", headers=headers, **kwargs)

    def session(self) -> Any:
        return db.session_scope(self.db_url)


def seed(db_url: str, home: Path) -> dict[str, str]:
    ids: dict[str, str] = {}
    with db.session_scope(db_url) as session:
        data, _ = store_repo.create_incident(
            session,
            dag_id="sales_daily_etl",
            run_id="manual__2026-10-01T12:00:00+00:00",
            task_id="load_sales",
            try_number=1,
            source="rest",
            error_summary=f"KeyError: 'amount' (row of {RAW_EMAIL})",
            now=T0,
            status="reported",
            category="data",
            confidence=0.9,
            diagnosis=f"Column amount was renamed; first offending row belongs to {RAW_EMAIL}",
            violated_rule={
                "rule": "missing_column",
                "dataset": "sales",
                "column": "amount",
                "message": "column 'amount' is missing",
            },
            sample={
                "dataset": "sales",
                "columns": ["order_id", "customer_email", "amount", "note"],
                "rows": [
                    {"order_id": 1, "customer_email": RAW_EMAIL, "amount": 10.5, "note": f"call {RAW_PHONE}"},
                    {"order_id": 2, "customer_email": "h:0123456789", "amount": None, "note": "ok"},
                ],
                "masked": True,
            },
            evidence=[f"contract violation for {RAW_EMAIL}", "amount missing"],
        )
        store_repo.add_event(
            session,
            data,
            "classified",
            f"Classified as data ({RAW_EMAIL})",
            data={
                "violations": [
                    {
                        "rule": "missing_column",
                        "dataset": "sales",
                        "column": "amount",
                        "message": "column 'amount' is missing",
                    },
                    {
                        "rule": "nulls_not_allowed",
                        "dataset": "sales",
                        "column": "order_id",
                        "message": f"nulls near {RAW_EMAIL}",
                    },
                ]
            },
        )
        store_repo.record_alert(
            session,
            incident_id=data.id,
            channel="email",
            recipients=["data-team@example.com"],
            subject="[dag-healer] DATA sales_daily_etl.load_sales",
            status="failed",
            error=f"SMTP refused for {RAW_EMAIL}",
        )
        ids["data"] = data.id

        code, _ = store_repo.create_incident(
            session,
            dag_id="customers_sync",
            run_id="manual__2026-10-01T13:00:00+00:00",
            task_id="transform",
            try_number=1,
            source="listener",
            error_summary="ValueError: time data '2026-10-02T14:03:00Z' does not match format",
            now=T0 + timedelta(minutes=1),
            status="repaired",
            category="code",
            confidence=0.92,
            test_path="tests/test_customers_parse_signup_date.py",
            test_code="def test_x():\n    assert 1\n",
            diff="--- a/src/etl/customers.py\n+++ b/src/etl/customers.py\n",
            pytest_output="1 passed",
            attempts=1,
            tokens_used=1234,
        )
        pr_dir = home / "prs" / code.id
        pr_dir.mkdir(parents=True)
        (pr_dir / "PR.md").write_text(
            "# Fix parse_signup_date\n\nGenerated by dag-healer\n", encoding="utf-8"
        )
        code.pr_md_path = str(pr_dir / "PR.md")
        ids["code"] = code.id

        code_retry, _ = store_repo.create_incident(
            session,
            dag_id="customers_sync",
            run_id="manual__2026-10-01T13:00:00+00:00",
            task_id="transform",
            try_number=2,
            source="rest",
            error_summary="ValueError again",
            now=T0 + timedelta(minutes=2),
            status="repair_failed",
            category="code",
        )
        ids["code_retry"] = code_retry.id

        amb, _ = store_repo.create_incident(
            session,
            dag_id="inventory_report",
            run_id="scheduled__2026-10-01T00:00:00+00:00",
            task_id="build_report",
            map_index=3,
            try_number=1,
            source="rest",
            error_summary="KeyError: \"['qty'] not in index\"",
            now=T0 + timedelta(minutes=3),
            status="waiting_human",
            category="ambiguous",
            confidence=0.5,
            signature="sig-inventory",
        )
        ids["ambiguous"] = amb.id
        # A later failure with the same signature is folded into the ambiguous incident.
        folded, created = store_repo.create_incident(
            session,
            dag_id="inventory_report",
            run_id="scheduled__2026-10-02T00:00:00+00:00",
            task_id="build_report",
            map_index=3,
            try_number=1,
            source="rest",
            signature="sig-inventory",
            now=T0 + timedelta(hours=1),
        )
        assert not created and folded.id == amb.id

        secret, _ = store_repo.create_incident(
            session,
            dag_id="secret_dag",
            run_id="manual__1",
            task_id="t",
            try_number=1,
            source="rest",
            error_summary="boom",
            now=T0 + timedelta(minutes=4),
            status="waiting_human",
        )
        ids["secret"] = secret.id

        gone, _ = store_repo.create_incident(
            session,
            dag_id="deleted_dag",
            run_id="manual__1",
            task_id="t",
            try_number=1,
            source="rest",
            error_summary="old failure",
            now=T0 + timedelta(minutes=5),
            status="ignored",
        )
        ids["deleted"] = gone.id
    return ids


@pytest.fixture
def api(healer_env: Path, tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Iterator[Api]:
    repo = tmp_path / "repo"
    config = write_repo(repo)
    db_url = f"sqlite:///{(tmp_path / 'store' / 'healer.db').as_posix()}"
    monkeypatch.setenv("HEALER_CONFIG", str(config))
    monkeypatch.setenv("HEALER_DATABASE_URL", db_url)
    monkeypatch.setenv("HEALER_INGEST_TOKEN", INGEST_TOKEN)
    monkeypatch.setenv("AIRFLOW__API__BASE_URL", BASE_URL)
    monkeypatch.delenv("HEALER_PUBLIC_URL", raising=False)
    db.dispose_engines()
    deps.reset_config_cache()
    deps.ingest_rate_limiter.reset()
    deps.channel_rate_limiter.reset()
    ids = seed(db_url, healer_env)
    authorizer = FakeAuthorizer()
    app = create_app()
    app.dependency_overrides[auth.get_current_user] = fake_current_user
    app.dependency_overrides[auth.get_authorizer] = lambda: authorizer
    with TestClient(app) as client:
        yield Api(client, repo, healer_env, db_url, authorizer, ids)
    deps.reset_config_cache()
    deps.ingest_rate_limiter.reset()
    deps.channel_rate_limiter.reset()
    db.dispose_engines()


# --------------------------------------------------------------------------- /me


def test_me(api: Api) -> None:
    response = api.get("/me", user="viewer")
    assert response.status_code == 200
    assert response.json() == {"user": "viewer", "base_url": BASE_URL}


# --------------------------------------------------------------------------- incident lists


def test_list_incidents_admin(api: Api) -> None:
    response = api.get("/incidents")
    assert response.status_code == 200
    body = response.json()
    ids = [item["id"] for item in body["incidents"]]
    # Newest first; the deleted DAG is readable through can_read_dag.
    assert ids == [
        api.ids["deleted"],
        api.ids["secret"],
        api.ids["ambiguous"],
        api.ids["code_retry"],
        api.ids["code"],
        api.ids["data"],
    ]
    assert body["total"] == 6
    item = next(i for i in body["incidents"] if i["id"] == api.ids["code"])
    assert set(item) == {
        "id", "dag_id", "run_id", "task_id", "map_index", "try_number", "status", "category",
        "confidence", "source", "error_summary", "created_at", "updated_at", "occurrences",
        "pr_url", "has_pr_md",
    }  # fmt: skip
    assert item["has_pr_md"] is True
    assert item["source"] == "listener"
    amb = next(i for i in body["incidents"] if i["id"] == api.ids["ambiguous"])
    assert amb["occurrences"] == 2


def test_list_incidents_filters(api: Api) -> None:
    body = api.get("/incidents", params={"status": "waiting_human"}).json()
    assert {i["id"] for i in body["incidents"]} == {api.ids["ambiguous"], api.ids["secret"]}
    assert body["total"] == 2
    body = api.get("/incidents", params={"status": "repaired,repair_failed"}).json()
    assert {i["id"] for i in body["incidents"]} == {api.ids["code"], api.ids["code_retry"]}
    body = api.get("/incidents", params={"dag_id": "customers_sync", "limit": 1}).json()
    assert [i["id"] for i in body["incidents"]] == [api.ids["code_retry"]]
    assert body["total"] == 2
    assert api.get("/incidents", params={"status": "bogus"}).status_code == 422
    assert api.get("/incidents", params={"limit": 0}).status_code == 422
    assert api.get("/incidents", params={"limit": 501}).status_code == 422


def test_list_incidents_only_readable_dags(api: Api) -> None:
    body = api.get("/incidents", user="limited").json()
    assert {i["dag_id"] for i in body["incidents"]} == {"sales_daily_etl", "customers_sync"}
    assert body["total"] == 3
    hidden = api.get("/incidents", user="limited", params={"dag_id": "secret_dag"}).json()
    assert hidden == {"incidents": [], "total": 0}


def test_dag_incidents(api: Api) -> None:
    body = api.get("/dags/sales_daily_etl/incidents").json()
    assert [i["id"] for i in body["incidents"]] == [api.ids["data"]]
    assert body["monitored"] is True
    assert body["can_edit"] is True
    contract = body["contract"]
    assert contract["status"] == "approved" and contract["stale"] is False
    assert contract["approved_by"] == "alice"
    assert contract["path"] == "healer/contracts/sales_daily_etl.yaml"
    assert contract["can_approve"] is False and contract["pending_approval"] is False
    (dataset,) = contract["datasets"]
    assert dataset["locator"] == {"type": "csv", "path": "data/incoming/sales.csv"}
    assert dataset["row_count"] == {"min": None, "max": None}
    email = next(c for c in dataset["columns"] if c["name"] == "customer_email")
    assert email == {
        "name": "customer_email", "dtype": "string", "nullable": True, "min": None, "max": None,
        "pii": True, "allowed_values": None, "note": None,
    }  # fmt: skip

    viewer = api.get("/dags/inventory_report/incidents", user="viewer").json()
    assert viewer["can_edit"] is False
    assert viewer["contract"]["status"] == "draft"
    assert viewer["contract"]["can_approve"] is False

    unmonitored = api.get("/dags/unmonitored_x/incidents").json()
    assert unmonitored == {"incidents": [], "contract": None, "monitored": False, "can_edit": True}
    assert api.get("/dags/sales_daily_etl/incidents", params={"status": "nope"}).status_code == 422


# --------------------------------------------------------------------------- incident detail


def _dump(response: Any) -> str:
    return json.dumps(response.json())


def test_incident_detail(api: Api) -> None:
    response = api.get(f"/dags/sales_daily_etl/incidents/{api.ids['data']}")
    assert response.status_code == 200
    body = response.json()
    assert body["status"] == "reported" and body["category"] == "data"
    assert body["violated_rule"]["rule"] == "missing_column"
    assert [v["rule"] for v in body["violations"]] == ["missing_column", "nulls_not_allowed"]
    assert body["allowed_actions"] == ["resolve_code", "retry_task", "ignore"]
    assert body["can_edit"] is True
    assert body["pending_commands"] == []
    assert body["links"] == {
        "task_instance": f"{BASE_URL}/dags/sales_daily_etl/runs/"
        "manual__2026-10-01T12%3A00%3A00%2B00%3A00/tasks/load_sales/plugin/healer-ti",
        "dag": f"{BASE_URL}/dags/sales_daily_etl/plugin/healer-dag",
        "healer": f"{BASE_URL}/plugin/healer",
    }
    assert body["pr_md_url"] is None
    assert [e["kind"] for e in body["events"]] == ["created", "classified"]
    (alert,) = body["alerts"]
    assert alert["recipients"] == ["data-team@example.com"]
    assert alert["status"] == "failed"
    sample = body["sample"]
    assert sample["masked"] is True
    assert sample["dataset"] == "sales"
    assert sample["columns"] == ["order_id", "customer_email", "amount", "note"]
    first, second = sample["rows"]
    assert first["order_id"] == 1 and first["amount"] == 10.5
    assert first["customer_email"].startswith("h:")
    assert "[phone]" in first["note"]
    assert second["customer_email"] == "h:0123456789"  # already masked: kept
    assert second["amount"] is None
    assert body["evidence"][1] == "amount missing"


def test_incident_detail_never_leaks_raw_values(api: Api) -> None:
    responses = [
        api.get(f"/dags/sales_daily_etl/incidents/{api.ids['data']}"),
        api.get("/incidents"),
        api.get("/dags"),
        api.get("/dags/sales_daily_etl/incidents"),
        api.get(
            "/dags/sales_daily_etl/task-instance",
            params={"run_id": "manual__2026-10-01T12:00:00+00:00", "task_id": "load_sales"},
        ),
    ]
    for response in responses:
        assert response.status_code == 200
        text = _dump(response)
        assert RAW_EMAIL not in text
        assert "555 123 4567" not in text
    assert "[email]" in _dump(responses[0])


def test_incident_detail_actions_by_permission_and_status(api: Api) -> None:
    viewer = api.get(f"/dags/sales_daily_etl/incidents/{api.ids['data']}", user="viewer").json()
    assert viewer["allowed_actions"] == [] and viewer["can_edit"] is False
    code = api.get(f"/dags/customers_sync/incidents/{api.ids['code']}", user="limited").json()
    assert code["allowed_actions"] == ["retry_task"] and code["can_edit"] is True
    assert (
        code["pr_md_url"] == f"{BASE_URL}/healer/api/v1/dags/customers_sync/incidents/{api.ids['code']}/pr.md"
    )
    assert code["test_code"].startswith("def test_x")
    assert code["attempts"] == 1 and code["tokens_used"] == 1234
    failed = api.get(f"/dags/customers_sync/incidents/{api.ids['code_retry']}").json()
    assert failed["allowed_actions"] == ["retry_repair", "resolve_data", "ignore"]
    amb = api.get(f"/dags/inventory_report/incidents/{api.ids['ambiguous']}").json()
    assert amb["allowed_actions"] == ["resolve_data", "resolve_code", "ignore"]
    assert amb["links"]["task_instance"].endswith("/tasks/build_report/mapped/3/plugin/healer-ti")
    ignored = api.get(f"/dags/deleted_dag/incidents/{api.ids['deleted']}").json()
    assert ignored["allowed_actions"] == []


def test_incident_detail_not_found(api: Api) -> None:
    assert api.get("/dags/sales_daily_etl/incidents/nope").status_code == 404
    # The incident exists but belongs to another DAG.
    response = api.get(f"/dags/customers_sync/incidents/{api.ids['data']}")
    assert response.status_code == 404
    assert response.json() == {"detail": "Incident not found"}


def test_pr_md(api: Api) -> None:
    response = api.get(f"/dags/customers_sync/incidents/{api.ids['code']}/pr.md")
    assert response.status_code == 200
    assert response.headers["content-type"].startswith("text/markdown")
    assert response.headers["x-content-type-options"] == "nosniff"
    assert response.text.startswith("# Fix parse_signup_date")
    assert api.get(f"/dags/sales_daily_etl/incidents/{api.ids['data']}/pr.md").status_code == 404
    assert api.get(f"/dags/sales_daily_etl/incidents/{api.ids['code']}/pr.md").status_code == 404


def test_pr_md_outside_healer_home_is_not_served(api: Api, tmp_path: Path) -> None:
    outside = tmp_path / "outside.md"
    outside.write_text("secret", encoding="utf-8")
    with api.session() as session:
        store_repo.get_incident(session, api.ids["data"]).pr_md_path = str(outside)
    assert api.get(f"/dags/sales_daily_etl/incidents/{api.ids['data']}/pr.md").status_code == 404
    summary = api.get(f"/dags/sales_daily_etl/incidents/{api.ids['data']}").json()
    assert summary["has_pr_md"] is False


def test_task_instance_incident(api: Api) -> None:
    run_id = "manual__2026-10-01T13:00:00+00:00"
    body = api.get(
        "/dags/customers_sync/task-instance", params={"run_id": run_id, "task_id": "transform"}
    ).json()
    assert body["incident"]["id"] == api.ids["code_retry"]  # latest try
    assert body["incident"]["allowed_actions"] == ["retry_repair", "resolve_data", "ignore"]
    missing = api.get("/dags/customers_sync/task-instance", params={"run_id": run_id, "task_id": "other"})
    assert missing.json() == {"incident": None}
    mapped = api.get(
        "/dags/inventory_report/task-instance",
        params={"run_id": "scheduled__2026-10-01T00:00:00+00:00", "task_id": "build_report", "map_index": 3},
    ).json()
    assert mapped["incident"]["id"] == api.ids["ambiguous"]
    unmapped = api.get(
        "/dags/inventory_report/task-instance",
        params={"run_id": "scheduled__2026-10-01T00:00:00+00:00", "task_id": "build_report"},
    ).json()
    assert unmapped == {"incident": None}
    folded = api.get(
        "/dags/inventory_report/task-instance",
        params={"run_id": "scheduled__2026-10-02T00:00:00+00:00", "task_id": "build_report", "map_index": 3},
    ).json()
    assert folded["incident"]["id"] == api.ids["ambiguous"]
    assert api.get("/dags/customers_sync/task-instance", params={"task_id": "transform"}).status_code == 422


# --------------------------------------------------------------------------- DAGs and contracts


def test_dags_list(api: Api) -> None:
    dags = {d["dag_id"]: d for d in api.get("/dags").json()["dags"]}
    assert set(dags) == {"sales_daily_etl", "customers_sync", "inventory_report", "secret_dag", "deleted_dag"}
    assert dags["sales_daily_etl"]["contract_status"] == "approved"
    assert dags["customers_sync"]["contract_status"] == "stale"
    assert dags["inventory_report"]["contract_status"] == "draft"
    assert dags["deleted_dag"]["contract_status"] == "missing"
    assert dags["customers_sync"]["open_incidents"] == 0
    assert dags["inventory_report"]["open_incidents"] == 1
    assert dags["customers_sync"]["last_incident"]["id"] == api.ids["code_retry"]
    assert dags["sales_daily_etl"]["monitored"] is True
    limited = {d["dag_id"] for d in api.get("/dags", user="limited").json()["dags"]}
    assert limited == {"sales_daily_etl", "customers_sync"}


def test_contracts_list_and_detail(api: Api) -> None:
    contracts = {c["dag_id"]: c for c in api.get("/contracts").json()["contracts"]}
    assert set(contracts) == {"sales_daily_etl", "customers_sync", "inventory_report", "secret_dag"}
    assert contracts["customers_sync"]["status"] == "stale"
    assert contracts["customers_sync"]["stale"] is True
    assert contracts["customers_sync"]["can_approve"] is True
    assert contracts["inventory_report"]["can_approve"] is True
    assert contracts["sales_daily_etl"]["can_approve"] is False
    viewer = api.get("/contracts", user="viewer").json()["contracts"]
    assert all(c["can_approve"] is False for c in viewer)
    limited = {c["dag_id"]: c for c in api.get("/contracts", user="limited").json()["contracts"]}
    assert set(limited) == {"sales_daily_etl", "customers_sync"}
    assert limited["customers_sync"]["can_approve"] is True

    detail = api.get("/dags/inventory_report/contract").json()
    assert detail["status"] == "draft"
    assert detail["datasets"][0]["columns"][0]["name"] == "qty"
    assert api.get("/dags/deleted_dag/contract").status_code == 404


def test_contract_reload_picks_up_file_edits(api: Api) -> None:
    path = api.repo / "healer" / "contracts" / "inventory_report.yaml"
    text = path.read_text(encoding="utf-8").replace("name: qty", "name: quantity")
    path.write_text(text + "\n", encoding="utf-8")
    detail = api.get("/dags/inventory_report/contract").json()
    assert detail["datasets"][0]["columns"][0]["name"] == "quantity"


# --------------------------------------------------------------------------- actions


def _commands(api: Api) -> list[Command]:
    with api.session() as session:
        return list(session.scalars(select(Command).order_by(Command.id)))


def test_resolve_enqueues_command(api: Api) -> None:
    path = f"/dags/inventory_report/incidents/{api.ids['ambiguous']}/resolve"
    response = api.post(path, json={"resolution": "code", "note": f"looks like code, ask {RAW_EMAIL}"})
    assert response.status_code == 202
    body = response.json()
    assert body["status"] == "pending"
    (command,) = _commands(api)
    assert command.id == body["command_id"]
    assert command.kind == "resolve"
    assert command.actor == "admin"
    assert command.dag_id == "inventory_report"
    assert command.incident_id == api.ids["ambiguous"]
    assert command.payload["resolution"] == "code"
    assert RAW_EMAIL not in command.payload["note"]
    with api.session() as session:
        kinds = [e.kind for e in store_repo.list_events(session, api.ids["ambiguous"])]
    assert kinds[-1] == "command_queued"

    # Same request again: idempotent. Another action while pending: 409.
    again = api.post(path, json={"resolution": "code"})
    assert again.status_code == 202 and again.json()["command_id"] == body["command_id"]
    other = api.post(path, json={"resolution": "ignore"})
    assert other.status_code == 409
    detail = api.get(f"/dags/inventory_report/incidents/{api.ids['ambiguous']}").json()
    assert detail["allowed_actions"] == []
    assert detail["pending_commands"][0]["kind"] == "resolve"
    assert detail["pending_commands"][0]["actor"] == "admin"
    assert len(_commands(api)) == 1


def test_resolve_validates_status_and_body(api: Api) -> None:
    # reported -> resolve_code/ignore allowed, resolve_data not.
    data_path = f"/dags/sales_daily_etl/incidents/{api.ids['data']}/resolve"
    conflict = api.post(data_path, json={"resolution": "data"})
    assert conflict.status_code == 409
    assert "not allowed" in conflict.json()["detail"]
    assert api.post(data_path, json={"resolution": "maybe"}).status_code == 422
    assert api.post(data_path, json={"resolution": "code", "extra": 1}).status_code == 422
    assert api.post(data_path, json={}).status_code == 422
    assert api.post(data_path, json={"resolution": "code", "note": "x" * 2001}).status_code == 422
    # repaired -> no resolution at all.
    code_path = f"/dags/customers_sync/incidents/{api.ids['code']}/resolve"
    assert api.post(code_path, json={"resolution": "ignore"}).status_code == 409
    # Incident of another DAG.
    assert (
        api.post(
            f"/dags/customers_sync/incidents/{api.ids['data']}/resolve", json={"resolution": "code"}
        ).status_code
        == 404
    )
    assert _commands(api) == []


def test_retry_repair_and_retry_task(api: Api) -> None:
    retry = api.post(f"/dags/customers_sync/incidents/{api.ids['code_retry']}/retry-repair", user="limited")
    assert retry.status_code == 202
    task = api.post(f"/dags/customers_sync/incidents/{api.ids['code']}/retry-task", json={"note": "merged"})
    assert task.status_code == 202
    assert (
        api.post(f"/dags/inventory_report/incidents/{api.ids['ambiguous']}/retry-repair").status_code == 409
    )
    assert api.post(f"/dags/inventory_report/incidents/{api.ids['ambiguous']}/retry-task").status_code == 409
    first, second = _commands(api)
    assert (first.kind, first.actor, first.incident_id) == ("retry_repair", "limited", api.ids["code_retry"])
    assert second.kind == "retry_task"
    assert second.payload == {
        "note": "merged",
        "run_id": "manual__2026-10-01T13:00:00+00:00",
        "task_id": "transform",
        "map_index": -1,
    }


def test_approve_contract(api: Api) -> None:
    response = api.post("/dags/inventory_report/contract/approve", json={"note": "reviewed"})
    assert response.status_code == 202
    (command,) = _commands(api)
    assert command.kind == "approve_contract" and command.incident_id is None
    assert command.dag_id == "inventory_report" and command.actor == "admin"
    assert command.payload["path"] == "healer/contracts/inventory_report.yaml"
    assert len(command.payload["source_hash"]) == 64
    again = api.post("/dags/inventory_report/contract/approve")
    assert again.status_code == 202 and again.json()["command_id"] == command.id
    view = api.get("/dags/inventory_report/contract").json()
    assert view["pending_approval"] is True and view["can_approve"] is False
    assert api.post("/dags/customers_sync/contract/approve").status_code == 202  # stale: re-approval
    assert api.post("/dags/sales_daily_etl/contract/approve").status_code == 409
    assert api.post("/dags/deleted_dag/contract/approve").status_code == 404
    assert len(_commands(api)) == 2


def test_approve_carries_hash_of_reviewed_file(api: Api) -> None:
    from dag_healer.contracts.io import load_contract

    path = api.repo / "healer" / "contracts" / "inventory_report.yaml"
    api.post("/dags/inventory_report/contract/approve")
    (command,) = _commands(api)
    assert command.payload["source_hash"] == compute_source_hash(load_contract(path))


# --------------------------------------------------------------------------- events


EVENT = {
    "dag_id": "sales_daily_etl",
    "task_id": "load_sales",
    "run_id": "manual__2026-10-03T00:00:00+00:00",
    "map_index": -1,
    "try_number": 2,
    "max_tries": 1,
    "state": "failed",
    "hostname": "worker-1",
    "error": f"Traceback ...\nKeyError: 'amount' for {RAW_EMAIL}",
    "source": "listener",
    "reported_at": "2026-10-03T00:00:05+00:00",
}


def _post_event(api: Api, payload: Any = None, token: str | None = INGEST_TOKEN, **kwargs: Any) -> Any:
    headers = {"X-Healer-Token": token} if token is not None else {}
    return api.client.post("/api/v1/events/task-failed", json=payload or EVENT, headers=headers, **kwargs)


def test_event_is_stored_masked(api: Api) -> None:
    response = _post_event(api)
    assert response.status_code == 202
    body = response.json()
    assert body["accepted"] is True
    with api.session() as session:
        event = session.get(PendingEvent, body["pending_event_id"])
        assert (event.dag_id, event.task_id, event.try_number, event.status) == (
            "sales_daily_etl",
            "load_sales",
            2,
            "pending_check",
        )
        assert RAW_EMAIL not in event.payload["error"]
        assert "[email]" in event.payload["error"]
        assert event.payload["source"] == "listener"
    # A repeated delivery of the same try returns the same pending row.
    assert _post_event(api).json()["pending_event_id"] == body["pending_event_id"]


def test_event_token_checks(api: Api, monkeypatch: pytest.MonkeyPatch) -> None:
    assert _post_event(api, token=None).status_code == 401
    assert _post_event(api, token="wrong").status_code == 401
    assert _post_event(api, token="").status_code == 401
    monkeypatch.delenv("HEALER_INGEST_TOKEN")
    deps.reset_config_cache()
    assert _post_event(api).status_code == 503
    with api.session() as session:
        assert session.scalars(select(PendingEvent)).all() == []


def test_event_validation_and_unmonitored(api: Api) -> None:
    assert _post_event(api, {"dag_id": "x"}).status_code == 422
    unmonitored = _post_event(api, {**EVENT, "dag_id": "unmonitored_dag"})
    assert unmonitored.status_code == 202
    assert unmonitored.json() == {"accepted": False, "pending_event_id": None}
    long_error = _post_event(api, {**EVENT, "error": "x" * 10000, "source": "weird"})
    assert long_error.status_code == 202
    with api.session() as session:
        stored = session.get(PendingEvent, long_error.json()["pending_event_id"])
        assert len(stored.payload["error"]) <= 4096
        assert stored.payload["source"] == "listener"


def test_event_rate_limit(api: Api, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(deps, "ingest_rate_limiter", deps.SlidingWindowRateLimiter(2, 60))
    assert _post_event(api).status_code == 202
    assert _post_event(api, token="wrong").status_code == 401
    limited = _post_event(api)
    assert limited.status_code == 429
    assert int(limited.headers["retry-after"]) >= 1


def test_event_body_too_large(api: Api) -> None:
    response = api.client.post(
        "/api/v1/events/task-failed",
        content=b"{" + b" " * (70 * 1024) + b"}",
        headers={"X-Healer-Token": INGEST_TOKEN, "Content-Type": "application/json"},
    )
    assert response.status_code == 413


# --------------------------------------------------------------------------- helpers


def test_rate_limiter_window() -> None:
    limiter = deps.SlidingWindowRateLimiter(2, 10, max_keys=2)
    assert limiter.hit("a", now=0) is None
    assert limiter.hit("a", now=1) is None
    assert limiter.hit("a", now=2) == pytest.approx(8)
    assert limiter.hit("a", now=10.5) is None  # first hit expired
    assert limiter.hit("b", now=11) is None
    assert limiter.hit("c", now=12) is None  # prunes to stay within max_keys
    assert len(limiter._hits) <= 2


@pytest.mark.parametrize(
    ("status", "expected"),
    [
        ("waiting_human", ["resolve_data", "resolve_code", "ignore"]),
        ("repair_failed", ["retry_repair", "resolve_data", "ignore"]),
        ("error", ["retry_repair", "resolve_data", "ignore"]),
        ("reported", ["resolve_code", "retry_task", "ignore"]),
        ("repaired", ["retry_task"]),
        ("new", []),
        ("triaging", []),
        ("ignored", []),
    ],
)
def test_allowed_actions_matrix(status: str, expected: list[str]) -> None:
    assert views.allowed_actions(status, can_edit=True) == expected
    assert views.allowed_actions(status, can_edit=False) == []
    assert views.allowed_actions(status, can_edit=True, has_pending=True) == []


def test_remask_sample_rules(api: Api) -> None:
    cfg = deps.get_config()
    sample = {
        "dataset": "sales",
        "columns": ["customer_email", "first_name", "city", "score", "allowed_email"],
        "rows": [
            {
                "customer_email": RAW_EMAIL,
                "first_name": "Jane",
                "city": "Mail me at jane@x.io",
                "score": 3,
                "allowed_email": "x@y.io",
            },
            ["h:aaaaaaaaaa", "***", "plain", 4.5, None],
        ],
    }
    cfg.masking.allow = ["allowed_email"]
    masked = views.remask_sample(sample, cfg, None, salt="s")
    assert masked is not None
    first, second = masked.rows
    assert first["customer_email"].startswith("h:") and first["first_name"].startswith("h:")
    assert first["city"] == "Mail me at [email]"
    assert first["score"] == 3
    assert first["allowed_email"] == "x@y.io"
    assert second == {
        "customer_email": "h:aaaaaaaaaa",
        "first_name": "***",
        "city": "plain",
        "score": 4.5,
        "allowed_email": None,
    }
    assert views.remask_sample(None, cfg, None, salt="s") is None
    legacy = views.remask_sample([{"email": RAW_EMAIL}], cfg, None, salt="s")
    assert legacy is not None and legacy.columns == ["email"] and legacy.rows[0]["email"].startswith("h:")


def test_violations_fallbacks() -> None:
    from dag_healer.store.models import Incident

    rule = {"rule": "missing_column", "dataset": "d", "message": "m"}
    incident = Incident(id="x", violated_rule={**rule, "violations": [rule, rule]}, sample=None)
    assert len(views.incident_violations(incident, [])) == 2
    incident = Incident(id="x", violated_rule=rule, sample={"violations": [rule]})
    assert views.incident_violations(incident, []) == [rule]
    incident = Incident(id="x", violated_rule=rule, sample=None)
    assert views.incident_violations(incident, []) == [rule]
    event = IncidentEvent(id=1, kind="classified", message="", data={"violations": []})
    assert views.incident_violations(incident, [event]) == []
    assert views.incident_violations(Incident(id="x", violated_rule="text"), []) == []


def test_config_is_cached_with_ttl(api: Api, monkeypatch: pytest.MonkeyPatch) -> None:
    clock = [1000.0]
    monkeypatch.setattr(deps.time, "monotonic", lambda: clock[0])
    deps.reset_config_cache()
    first = deps.get_config()
    assert deps.get_config() is first
    config = api.repo / "healer.yaml"
    config.write_text(config.read_text(encoding="utf-8") + "debug: true\n", encoding="utf-8")
    assert deps.get_config() is first  # within the TTL
    clock[0] += deps.CONFIG_TTL_SECONDS + 1
    second = deps.get_config()
    assert second is not first and second.debug is True
    config.write_text("version: [broken\n", encoding="utf-8")
    clock[0] += deps.CONFIG_TTL_SECONDS + 1
    assert deps.get_config() is second  # invalid file: keep the last good configuration
    deps.reset_config_cache()
    with pytest.raises(HTTPException) as excinfo:
        deps.get_config()
    assert excinfo.value.status_code == 503


def test_store_errors_answer_503(api: Api) -> None:
    from sqlalchemy.exc import OperationalError

    def broken_session() -> Iterator[Any]:
        raise OperationalError("SELECT 1", {"email": RAW_EMAIL}, Exception("database is down"))
        yield  # pragma: no cover

    api.client.app.dependency_overrides[deps.get_session] = broken_session
    response = api.get("/incidents")
    assert response.status_code == 503
    assert response.json() == {"detail": "The dag-healer store is unavailable"}


# --------------------------------------------------------------------------- orchestrator instances


def test_default_responses_leave_out_the_additive_fields(api: Api) -> None:
    summary = api.get("/incidents").json()["incidents"][0]
    assert not {"orchestrator", "error_name", "links"} & set(summary)
    detail = api.get(f"/dags/customers_sync/incidents/{api.ids['code']}").json()
    assert not {"orchestrator", "error_name", "action_warnings"} & set(detail)
    assert set(detail["links"]) == {"task_instance", "dag", "healer"}


def test_wire_fields_are_left_out_only_while_neutral() -> None:
    base = {
        "id": "i", "dag_id": "d", "run_id": "r", "task_id": "t", "map_index": -1, "try_number": 1,
        "status": "reported", "source": "rest", "error_summary": "", "occurrences": 1,
    }  # fmt: skip
    additive = {"orchestrator", "error_name", "links"}
    neutral = schemas.IncidentSummary(**base)
    assert not additive & set(neutral.model_dump())
    assert not additive & set(json.loads(neutral.model_dump_json()))
    links = schemas.IncidentLinks(task_instance="a", dag="b", healer="c", native={"Fakeflow": "http://x/1"})
    shown = schemas.IncidentSummary(**base, orchestrator="fake", error_name="KeyError", links=links)
    assert additive <= set(shown.model_dump())
    assert json.loads(shown.model_dump_json())["links"]["native"] == {"Fakeflow": "http://x/1"}
    me = schemas.MeView(user="u", base_url="http://b")
    assert json.loads(me.model_dump_json()) == {"user": "u", "base_url": "http://b"}
    standalone = schemas.MeView(user="u", base_url="http://b", host="standalone")
    assert json.loads(standalone.model_dump_json())["host"] == "standalone"
    # the omitted fields stay documented in the response schemas (OpenAPI)
    properties = schemas.IncidentDetail.model_json_schema(mode="serialization")["properties"]
    assert {"links", "orchestrator", "error_name", "action_warnings"} <= set(properties)


def _seed_other_instance(api: Api) -> str:
    """An incident of another orchestrator instance, under a DAG this instance also has."""
    with api.session() as session:
        incident, _ = store_repo.create_incident(
            session,
            orchestrator="sfn-prod",
            dag_id="sales_daily_etl",
            run_id="exec-1",
            task_id="load_sales",
            try_number=1,
            source="poll",
            error_summary="States.TaskFailed",
            signature="sig-sfn",
            now=T0 + timedelta(hours=2),
            status="waiting_human",
        )
        # a later failure folded into it: the task-instance lookup must not find it either
        _, created = store_repo.create_incident(
            session,
            orchestrator="sfn-prod",
            dag_id="sales_daily_etl",
            run_id="exec-2",
            task_id="load_sales",
            try_number=1,
            source="poll",
            signature="sig-sfn",
            now=T0 + timedelta(hours=3),
        )
        assert not created
        return incident.id


def test_incidents_of_another_instance_are_neither_listed_nor_shown(api: Api) -> None:
    pages = ["/incidents", "/dags", "/dags/sales_daily_etl/incidents"]
    before = {page: api.get(page).json() for page in pages}
    other = _seed_other_instance(api)
    assert {page: api.get(page).json() for page in pages} == before
    assert api.get(f"/dags/sales_daily_etl/incidents/{other}").status_code == 404
    assert api.get(f"/dags/sales_daily_etl/incidents/{other}/pr.md").status_code == 404
    for run_id in ("exec-1", "exec-2"):
        found = api.get(
            "/dags/sales_daily_etl/task-instance", params={"run_id": run_id, "task_id": "load_sales"}
        )
        assert found.json() == {"incident": None}
    base = f"/dags/sales_daily_etl/incidents/{other}"
    assert api.post(f"{base}/resolve", json={"resolution": "ignore"}).status_code == 404
    assert api.post(f"{base}/retry-repair").status_code == 404
    assert api.post(f"{base}/retry-task").status_code == 404
    assert _commands(api) == []


def test_commands_and_events_carry_the_instance(api: Api) -> None:
    api.post(f"/dags/inventory_report/incidents/{api.ids['ambiguous']}/resolve", json={"resolution": "code"})
    api.post("/dags/inventory_report/contract/approve")
    assert [(c.kind, c.orchestrator) for c in _commands(api)] == [
        ("resolve", "airflow"),
        ("approve_contract", "airflow"),
    ]
    pending = _post_event(api).json()["pending_event_id"]
    with api.session() as session:
        event = session.get(PendingEvent, pending)
        assert (event.orchestrator, event.channel) == ("airflow", "task-failed")
        assert set(event.payload) == {"error", "source", "state", "max_tries", "hostname", "reported_at"}


def test_without_an_adapter_the_routes_behave_as_before(api: Api) -> None:
    api.client.app.dependency_overrides[deps.get_adapter] = lambda: None
    assert api.get("/me", user="viewer").json() == {"user": "viewer", "base_url": BASE_URL}
    detail = api.get(f"/dags/sales_daily_etl/incidents/{api.ids['data']}").json()
    assert detail["allowed_actions"] == ["resolve_code", "retry_task", "ignore"]
    assert set(detail["links"]) == {"task_instance", "dag", "healer"}
    assert _post_event(api).status_code == 202
    unknown = api.client.post("/api/v1/events/fake", json={}, headers={"X-Healer-Token": INGEST_TOKEN})
    assert unknown.status_code == 404


def test_adapter_failures_are_logged_once(
    api: Api, monkeypatch: pytest.MonkeyPatch, caplog: pytest.LogCaptureFixture
) -> None:
    def broken(cfg: Any) -> Any:
        raise registry.AdapterLoadError("orchestrator type 'acme': module 'acme' is not installed")

    monkeypatch.setattr(registry, "get_adapter", broken)
    monkeypatch.setattr(deps, "_adapter_failures", set())
    cfg = deps.get_config()
    with caplog.at_level(logging.WARNING, logger="dag_healer.api.deps"):
        assert deps.get_adapter(cfg) is None
        assert deps.get_adapter(cfg) is None
    assert sum("cannot be built" in record.getMessage() for record in caplog.records) == 1
    assert deps.get_instance(cfg, None) == "airflow"
    assert deps.get_instance(config_from_mapping({"orchestrator": {"instance": "prod"}}), None) == "prod"
    assert api.get("/me", user="viewer").json() == {"user": "viewer", "base_url": BASE_URL}


def test_orchestrator_view_of_airflow_instances() -> None:
    assert views.orchestrator_view(None) is None
    assert views.orchestrator_view(AirflowAdapter(config_from_mapping({}))) is None
    named = views.orchestrator_view(
        AirflowAdapter(config_from_mapping({"orchestrator": {"instance": "prod"}}))
    )
    assert named is not None
    assert (named.type, named.instance, named.display_name) == ("airflow", "prod", "Apache Airflow")
    assert named.vocabulary["retry_action"] == "Retry task"
    assert named.capabilities["remediations"] == ["retry_step"]
    assert named.capabilities["ui_hosts"] == ["airflow_plugin", "standalone"]


@pytest.mark.parametrize(
    ("status", "expected"),
    [
        ("reported", ["resolve_code", "ignore"]),
        ("repaired", []),
        ("waiting_human", ["resolve_data", "resolve_code", "ignore"]),
    ],
)
def test_allowed_actions_without_retry(status: str, expected: list[str]) -> None:
    assert views.allowed_actions(status, can_edit=True, retry_available=False) == expected


# --------------------------------------------------------------------------- Fakeflow (a non-Airflow adapter)

FAKE_RUN = "run-7"


def fakeflow_world() -> FakeWorld:
    """The Fakeflow side of the seeded Fakeflow incidents."""
    world = FakeWorld()
    world.add_pipeline(
        "sales_daily_etl",
        file="dags/sales_daily_etl.py",
        steps={"load_sales": TaskInfo("load_sales", "python")},
    )
    world.add_pipeline(
        "customers_sync", file="dags/customers_sync.py", steps={"transform": TaskInfo("transform", "python")}
    )
    world.fail("sales_daily_etl", FAKE_RUN, "load_sales", text="KeyError: 'amount'", error_name="KeyError")
    world.fail("customers_sync", FAKE_RUN, "transform", map_index=2, text="ValueError: bad date")
    return world


def seed_fakeflow(db_url: str) -> dict[str, str]:
    with db.session_scope(db_url) as session:
        reported, _ = store_repo.create_incident(
            session,
            orchestrator="fake",
            dag_id="sales_daily_etl",
            run_id=FAKE_RUN,
            task_id="load_sales",
            try_number=1,
            source="poll",
            error_summary=f"KeyError: 'amount' ({RAW_EMAIL})",
            error_name="KeyError",
            native_ref={"seq": 1},
            now=T0 + timedelta(hours=2),
            status="reported",
            category="data",
        )
        repaired, _ = store_repo.create_incident(
            session,
            orchestrator="fake",
            dag_id="customers_sync",
            run_id=FAKE_RUN,
            task_id="transform",
            map_index=2,
            try_number=1,
            source="push",
            error_summary="ValueError: bad date",
            error_name=f"ValueError({RAW_EMAIL})",
            now=T0 + timedelta(hours=3),
            status="repaired",
            category="code",
        )
        return {"reported": reported.id, "repaired": repaired.id}


@dataclass
class Fakeflow:
    adapter: FakeAdapter
    ids: dict[str, str]


@pytest.fixture
def fakeflow(api: Api) -> Fakeflow:
    """The API serving a Fakeflow instance (``deps.get_adapter`` overridden)."""
    adapter = FakeAdapter(world=fakeflow_world())
    api.client.app.dependency_overrides[deps.get_adapter] = lambda: adapter
    return Fakeflow(adapter, seed_fakeflow(api.db_url))


def test_fakeflow_lists_and_details_carry_the_additive_fields(api: Api, fakeflow: Fakeflow) -> None:
    body = api.get("/incidents").json()
    assert [item["id"] for item in body["incidents"]] == [fakeflow.ids["repaired"], fakeflow.ids["reported"]]
    assert body["total"] == 2
    repaired, reported = body["incidents"]
    assert reported["orchestrator"] == "fake" and reported["error_name"] == "KeyError"
    assert reported["links"]["native"] == {
        "Fakeflow": f"http://fakeflow.localhost/flows/sales_daily_etl/runs/{FAKE_RUN}/steps/load_sales?attempt=1"
    }
    assert reported["links"]["task_instance"].startswith(f"{BASE_URL}/dags/sales_daily_etl/runs/{FAKE_RUN}/")
    assert repaired["links"]["native"]["Fakeflow"].endswith("/steps/transform?attempt=1&map_index=2")
    assert RAW_EMAIL not in json.dumps(body) and repaired["error_name"] == "ValueError([email])"

    detail = api.get(f"/dags/sales_daily_etl/incidents/{fakeflow.ids['reported']}").json()
    assert detail["orchestrator"] == "fake" and detail["error_name"] == "KeyError"
    assert detail["links"]["native"] == reported["links"]["native"]
    assert detail["allowed_actions"] == ["resolve_code", "retry_task", "ignore"]  # same ids and order
    assert "action_warnings" not in detail  # repo_sync: a merged fix is picked up without a deploy
    dag = api.get("/dags/customers_sync/incidents").json()
    assert [item["id"] for item in dag["incidents"]] == [fakeflow.ids["repaired"]]
    dags = {item["dag_id"]: item for item in api.get("/dags").json()["dags"]}
    assert dags["customers_sync"]["last_incident"]["orchestrator"] == "fake"
    ti = api.get(
        "/dags/customers_sync/task-instance",
        params={"run_id": FAKE_RUN, "task_id": "transform", "map_index": 2},
    ).json()
    assert ti["incident"]["id"] == fakeflow.ids["repaired"]
    # the Airflow incidents of the same store belong to another instance now
    assert api.get(f"/dags/sales_daily_etl/incidents/{api.ids['data']}").status_code == 404


def test_fakeflow_me_has_the_orchestrator_block(api: Api, fakeflow: Fakeflow) -> None:
    assert api.get("/me", user="viewer").json() == {
        "user": "viewer",
        "base_url": BASE_URL,
        "orchestrator": {
            "type": "fake",
            "instance": "fake",
            "display_name": "Fakeflow",
            "vocabulary": FAKE_VOCABULARY.as_dict(),
            "capabilities": {
                "sources": ["poll", "push"],
                "push_channels": ["fake"],
                "attempts_visible": True,
                "remediations": ["retry_step", "resume_run", "rerun_pipeline"],
                "fix_delivery": "repo_sync",
                "ui_hosts": ["standalone"],
                "native_authz": False,
            },
        },
    }


def test_fakeflow_commands_are_queued_for_its_worker(api: Api, fakeflow: Fakeflow) -> None:
    reported = f"/dags/sales_daily_etl/incidents/{fakeflow.ids['reported']}"
    assert api.post(f"{reported}/resolve", json={"resolution": "ignore"}).status_code == 202
    repaired = f"/dags/customers_sync/incidents/{fakeflow.ids['repaired']}"
    assert api.post(f"{repaired}/retry-task").status_code == 202
    assert api.post("/dags/inventory_report/contract/approve").status_code == 202
    commands = _commands(api)
    assert [(c.kind, c.orchestrator) for c in commands] == [
        ("resolve", "fake"),
        ("retry_task", "fake"),
        ("approve_contract", "fake"),
    ]
    assert commands[1].payload == {"note": None, "run_id": FAKE_RUN, "task_id": "transform", "map_index": 2}
    assert api.get("/dags/inventory_report/contract").json()["pending_approval"] is True
    # the approval pending for the Fakeflow worker is not pending for the Airflow one
    api.client.app.dependency_overrides.pop(deps.get_adapter)
    assert api.get("/dags/inventory_report/contract").json()["pending_approval"] is False
    assert api.post("/dags/inventory_report/contract/approve").json()["command_id"] != commands[2].id


def test_retry_task_needs_a_remediator_that_offers_it(
    api: Api, fakeflow: Fakeflow, caplog: pytest.LogCaptureFixture
) -> None:
    path = f"/dags/sales_daily_etl/incidents/{fakeflow.ids['reported']}"
    fakeflow.adapter.remediator = None
    assert api.get(path).json()["allowed_actions"] == ["resolve_code", "ignore"]
    refused = api.post(f"{path}/retry-task")
    assert refused.status_code == 409
    assert refused.json() == {"detail": "Action retry_task is not available for this incident"}

    fakeflow.adapter.remediator = fakeflow.adapter
    fakeflow.adapter.available = lambda ref, native: ()  # type: ignore[method-assign]
    assert api.get(path).json()["allowed_actions"] == ["resolve_code", "ignore"]
    assert api.post(f"{path}/retry-task").status_code == 409

    def broken(ref: Any, native: Any) -> Any:
        raise RuntimeError(f"cannot decide for {RAW_EMAIL}")

    fakeflow.adapter.available = broken  # type: ignore[method-assign]
    with caplog.at_level(logging.WARNING, logger="dag_healer.api.views"):
        assert api.get(path).json()["allowed_actions"] == ["resolve_code", "ignore"]
    assert RAW_EMAIL not in caplog.text and "[email]" in caplog.text
    # the status check comes first
    other = api.post(
        f"/dags/customers_sync/incidents/{fakeflow.ids['repaired']}/resolve", json={"resolution": "code"}
    )
    assert other.status_code == 409 and "not allowed" in other.json()["detail"]
    assert _commands(api) == []


def test_ci_deploy_warns_before_retrying_a_repaired_incident(api: Api, fakeflow: Fakeflow) -> None:
    fakeflow.adapter.capabilities = replace(FAKE_CAPABILITIES, fix_delivery="ci_deploy")
    repaired = api.get(f"/dags/customers_sync/incidents/{fakeflow.ids['repaired']}").json()
    assert repaired["allowed_actions"] == ["retry_task"]
    assert repaired["action_warnings"] == {
        "retry_task": [
            "A merged fix reaches Fakeflow only through your deployment: deploy it before "
            '"Retry step", or the step runs the old code again.'
        ]
    }
    reported = api.get(f"/dags/sales_daily_etl/incidents/{fakeflow.ids['reported']}").json()
    assert "action_warnings" not in reported
    viewer = api.get(f"/dags/customers_sync/incidents/{fakeflow.ids['repaired']}", user="viewer").json()
    assert "action_warnings" not in viewer  # nothing offered, nothing to warn about


# --------------------------------------------------------------------------- push channels

HINT = {
    "pipeline_id": "sales_daily_etl",
    "run_id": "run-9",
    "step_id": "load_sales",
    "attempt": 2,
    "error": f"Traceback ...\nKeyError: 'amount' for {RAW_EMAIL}",
    "event_id": "evt-1",
    "input": {"customer_email": RAW_EMAIL},
}


def _post_hint(
    api: Api,
    payload: Any = None,
    *,
    channel: str = "fake",
    token: str | None = INGEST_TOKEN,
    headers: dict[str, str] | None = None,
) -> Any:
    sent = {"X-Healer-Token": token} if token is not None else {}
    sent.update(headers or {})
    return api.client.post(
        f"/api/v1/events/{channel}", json=HINT if payload is None else payload, headers=sent
    )


def test_channel_event_is_stored_masked(api: Api, fakeflow: Fakeflow) -> None:
    response = _post_hint(api)
    assert response.status_code == 202
    body = response.json()
    assert body["accepted"] is True
    (pending_id,) = body["pending_event_ids"]
    with api.session() as session:
        event = session.get(PendingEvent, pending_id)
        assert (event.orchestrator, event.channel, event.status) == ("fake", "fake", "pending_check")
        assert (event.dag_id, event.run_id, event.task_id, event.map_index, event.try_number) == (
            "sales_daily_etl",
            "run-9",
            "load_sales",
            -1,
            2,
        )
        assert set(event.payload) == {"error", "source"}  # never the execution input
        assert event.payload["source"] == "push"
        assert RAW_EMAIL not in event.payload["error"] and "[email]" in event.payload["error"]
        assert event.native_ref == {"event_id": "evt-1"}
    # a repeated delivery of the same attempt returns the same pending row
    assert _post_hint(api).json() == body
    long_error = _post_hint(api, {**HINT, "run_id": "run-10", "error": "x" * 10_000})
    with api.session() as session:
        stored = session.get(PendingEvent, long_error.json()["pending_event_ids"][0])
        assert len(stored.payload["error"]) <= 4096


def test_channel_event_validation(api: Api, fakeflow: Fakeflow) -> None:
    assert _post_hint(api, [HINT]).status_code == 422
    empty = api.client.post("/api/v1/events/fake", headers={"X-Healer-Token": INGEST_TOKEN})
    assert empty.status_code == 422
    assert empty.json() == {"detail": "The event body must be a JSON object"}
    malformed = _post_hint(api, {"run_id": "run-9", "error": RAW_EMAIL})
    assert malformed.status_code == 422
    assert malformed.json() == {"detail": "pipeline_id must be a string, not NoneType"}
    unknown = _post_hint(api, channel="eventbridge")
    assert unknown.status_code == 404
    assert unknown.json() == {"detail": "Unknown event channel: eventbridge"}
    unmonitored = _post_hint(api, {**HINT, "pipeline_id": "unmonitored_flow"})
    assert unmonitored.status_code == 202
    assert unmonitored.json() == {"accepted": False, "pending_event_ids": []}
    assert _post_hint(api, token="wrong").status_code == 401
    with api.session() as session:
        assert session.scalars(select(PendingEvent)).all() == []


def test_channel_event_answers_500_without_echoing_an_intake_bug(
    api: Api, fakeflow: Fakeflow, caplog: pytest.LogCaptureFixture
) -> None:
    def broken(channel: str, payload: Any, headers: Any) -> Any:
        raise KeyError(payload["error"])

    fakeflow.adapter.normalize = broken  # type: ignore[method-assign]
    with caplog.at_level(logging.ERROR, logger="dag_healer.api.routers.events"):
        response = _post_hint(api)
    assert response.status_code == 500
    assert response.json() == {"detail": "The event could not be read"}
    assert RAW_EMAIL not in caplog.text and "KeyError" in caplog.text


def test_push_intake_never_sees_the_ingest_token(api: Api, fakeflow: Fakeflow) -> None:
    seen: list[dict[str, str]] = []
    normalize = fakeflow.adapter.normalize

    def recording(channel: str, payload: Any, headers: Any) -> Any:
        seen.append(dict(headers))
        return normalize(channel, payload, headers)

    fakeflow.adapter.normalize = recording  # type: ignore[method-assign]
    response = _post_hint(api, headers={"X-Event-Signature": "sig", "Cookie": "_token=x"})
    assert response.status_code == 202
    (headers,) = seen
    assert headers["x-event-signature"] == "sig"
    assert not {"x-healer-token", "cookie", "authorization"} & set(headers)


def test_task_failed_needs_the_task_failed_channel(api: Api, fakeflow: Fakeflow) -> None:
    response = _post_event(api)
    assert response.status_code == 404
    assert response.json() == {"detail": "Unknown event channel: task-failed"}
    assert _post_event(api, token="wrong").status_code == 401  # the token check comes first
    with api.session() as session:
        assert session.scalars(select(PendingEvent)).all() == []


def test_channel_route_serves_only_the_adapter_channels(api: Api) -> None:
    # the default Airflow adapter has only task-failed, which the static route serves
    response = _post_hint(api)
    assert response.status_code == 404
    assert response.json() == {"detail": "Unknown event channel: fake"}
    assert _post_event(api).status_code == 202


def test_channel_rate_limit_is_keyed_by_channel_and_token(
    api: Api, fakeflow: Fakeflow, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setattr(deps, "channel_rate_limiter", deps.SlidingWindowRateLimiter(2, 60))
    assert _post_hint(api).status_code == 202
    assert _post_hint(api, {**HINT, "run_id": "run-11"}).status_code == 202
    limited = _post_hint(api, {**HINT, "run_id": "run-12"})
    assert limited.status_code == 429
    assert int(limited.headers["retry-after"]) >= 1
    assert _post_hint(api, token="another-token").status_code == 401  # another token: its own budget
    assert _post_hint(api, channel="eventbridge").status_code == 404  # another channel: its own budget
    assert _post_event(api).status_code == 404  # task-failed keeps its per-client limiter


def test_channel_event_body_too_large(api: Api, fakeflow: Fakeflow) -> None:
    response = api.client.post(
        "/api/v1/events/fake",
        content=b"{" + b" " * (70 * 1024) + b"}",
        headers={"X-Healer-Token": INGEST_TOKEN, "Content-Type": "application/json"},
    )
    assert response.status_code == 413


# --------------------------------------------------------------------------- outdated store

V1_STORE = Path(__file__).parent / "fixtures" / "store" / "healer_v1.db"
OUTDATED = "dag-healer store schema v1: start the worker or run 'dag-healer store migrate'"


def test_an_outdated_store_answers_503_and_is_never_migrated(
    api: Api, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    store = tmp_path / "v1" / "healer.db"
    store.parent.mkdir()
    shutil.copyfile(V1_STORE, store)
    url = f"sqlite:///{store.as_posix()}"
    monkeypatch.setenv("HEALER_DATABASE_URL", url)
    deps.reset_config_cache()
    outdated = (503, {"detail": OUTDATED})
    for path in ("/incidents", "/dags", "/contracts", "/dags/sales_daily_etl/incidents"):
        response = api.get(path)
        assert (response.status_code, response.json()) == outdated, path
    response = _post_event(api)
    assert (response.status_code, response.json()) == outdated
    api.client.app.dependency_overrides[deps.get_adapter] = lambda: FakeAdapter(world=FakeWorld())
    response = _post_hint(api)
    assert (response.status_code, response.json()) == outdated
    assert api.get("/me", user="viewer").status_code == 200  # /me does not read the store
    engine = db.get_engine(url)
    assert current_version(engine) == 1  # web processes never migrate: the worker does
    backup = backup_path(engine, 1)
    assert backup is not None and not backup.exists()
