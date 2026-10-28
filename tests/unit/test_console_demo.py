"""scripts/console_demo.py: the scratch dag-healer that lets someone try the console.

The script seeds a store and writes two configurations; here they are served by the standalone
host, as ``dag-healer serve`` would, so the demo keeps working while the store, the golden
snapshots it reads and the API evolve.
"""

from __future__ import annotations

import importlib.util
import sys
from collections.abc import Iterator
from pathlib import Path
from types import ModuleType

import pytest
from fastapi.testclient import TestClient
from sqlalchemy import select

from dag_healer.api import deps
from dag_healer.config import load_config
from dag_healer.contracts.io import load_contract
from dag_healer.hosting.app import create_standalone_app
from dag_healer.store import db
from dag_healer.store.models import Incident

REPO_ROOT = Path(__file__).resolve().parents[2]
API = "/healer/api/v1"
CSRF = {"X-Requested-With": "dag-healer"}


@pytest.fixture(scope="module")
def demo() -> Iterator[ModuleType]:
    spec = importlib.util.spec_from_file_location("console_demo", REPO_ROOT / "scripts" / "console_demo.py")
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    sys.modules["console_demo"] = module
    spec.loader.exec_module(module)
    yield module
    sys.modules.pop("console_demo", None)


@pytest.fixture
def seeded(demo: ModuleType, tmp_path: Path, capsys: pytest.CaptureFixture[str]) -> Iterator[Path]:
    directory = tmp_path / "console"
    assert demo.main(["--dir", str(directory)]) == 0
    assert "dag-healer serve -c" in capsys.readouterr().out
    yield directory
    db.dispose_engines()


def _serve(directory: Path, instance: str, monkeypatch: pytest.MonkeyPatch) -> TestClient:
    """The standalone host of one instance, as ``dag-healer serve -c <healer.yaml>`` builds it."""
    config = directory / instance / "healer.yaml"
    monkeypatch.setenv("HEALER_CONFIG", str(config))
    monkeypatch.setenv("HEALER_HOME", str(directory / "home"))
    monkeypatch.delenv("HEALER_DATABASE_URL", raising=False)
    deps.reset_config_cache()
    app = create_standalone_app(load_config(config), bind_host="127.0.0.1")
    return TestClient(app, base_url="http://127.0.0.1:8090")


def test_the_store_holds_both_instances(seeded: Path) -> None:
    with db.session_scope(f"sqlite:///{(seeded / 'healer.db').as_posix()}") as session:
        rows = [(i.orchestrator, i.dag_id, i.task_id, i.status) for i in session.scalars(select(Incident))]
    airflow = {status for orchestrator, _, _, status in rows if orchestrator == "airflow"}
    assert airflow == {
        "triaging",
        "repairing",
        "waiting_human",
        "repaired",
        "reported",
        "error",
        "repair_failed",
        "ignored",
    }
    sfn = sorted((dag, task, status) for instance, dag, task, status in rows if instance == "sfn-etl-euw1")
    assert sfn == [
        ("customers_sync", "TransformCustomers", "waiting_human"),
        ("inventory_report", "BuildReport", "waiting_human"),
        ("sales_daily_etl", "TransformSales", "reported"),
    ]


def test_the_contracts_are_approved_or_drafts_as_in_the_demo(seeded: Path) -> None:
    contracts = seeded / "airflow" / "repo" / "healer" / "contracts"
    status = {path.stem: load_contract(path).status for path in contracts.glob("*.yaml")}
    assert status == {
        "customers_sync": "approved",
        "inventory_report": "draft",
        "sales_daily_etl": "approved",
    }
    sfn = seeded / "stepfunctions" / "repo" / "healer" / "contracts"
    assert load_contract(sfn / "sales_daily_etl.yaml").status == "approved"
    assert load_contract(sfn / "inventory_report.yaml").status == "draft"


def test_serve_shows_the_airflow_instance(seeded: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    with _serve(seeded, "airflow", monkeypatch) as client:
        listed = client.get(f"{API}/incidents", headers=CSRF).json()
        assert listed["total"] == 8
        repaired = next(item for item in listed["incidents"] if item["status"] == "repaired")
        assert repaired["has_pr_md"] is True
        pr_md = client.get(f"{API}/dags/customers_sync/incidents/{repaired['id']}/pr.md", headers=CSRF)
        assert pr_md.status_code == 200
        assert repaired["id"] in pr_md.text
        assert "[number]" not in pr_md.text and "<HEALER_HOME>" not in pr_md.text
        waiting = next(item for item in listed["incidents"] if item["status"] == "waiting_human")
        detail = client.get(f"{API}/dags/inventory_report/incidents/{waiting['id']}", headers=CSRF).json()
        assert detail["allowed_actions"] == ["resolve_data", "resolve_code", "retry_task", "ignore"]
        assert [event["data"]["rule"] for event in detail["events"] if event["kind"] == "classification"] == [
            "c_no_approved_contract"
        ]
        assert client.get(f"{API}/dags/inventory_report/contract.yaml", headers=CSRF).status_code == 200
        assert client.get(f"{API}/setup", headers=CSRF).json()["alerts"]["email"] == ["data-team@example.com"]
        assert client.get("/plugin/healer").status_code in (200, 503)  # 503 only without a built bundle


def test_serve_shows_the_step_functions_instance(seeded: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    with _serve(seeded, "stepfunctions", monkeypatch) as client:
        me = client.get(f"{API}/me", headers=CSRF).json()
        assert me["orchestrator"]["vocabulary"]["retry_action"] == "Redrive execution"
        listed = client.get(f"{API}/incidents", headers=CSRF).json()
        assert listed["total"] == 3
        links = [(item.get("links") or {}).get("native", {}) for item in listed["incidents"]]
        native = {label for found in links for label in found}
        assert native == {"AWS console", "CloudWatch logs"}
