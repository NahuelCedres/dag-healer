"""Who migrates the store (section 6.4 of ``docs/architecture/orchestrators.md``).

The worker's sessions create or upgrade the schema (``allow_migrate=True``); every other
process, the Airflow api-server plugin included, never does: on an older store the API
answers 503 with the ``StoreSchemaOutdated`` text until the worker (or
``dag-healer store migrate``) has migrated it. The store is a private copy of the frozen
phase-5 store ``tests/unit/fixtures/store/healer_v1.db``.
"""

from __future__ import annotations

import shutil
import sqlite3
import textwrap
from collections.abc import Iterator
from contextlib import closing
from pathlib import Path
from typing import Any

import pytest
from fastapi import HTTPException, Request
from fastapi.testclient import TestClient

from dag_healer.agent.fake import FakeProvider
from dag_healer.airflow.client import AirflowAPIError, TaskInstanceInfo
from dag_healer.api import auth, deps
from dag_healer.api.app import create_app
from dag_healer.config import HealerConfig, config_from_mapping
from dag_healer.store import db
from dag_healer.store.db import StoreSchemaOutdated
from dag_healer.store.migrations import backup_path, current_version
from dag_healer.store.models import Incident
from dag_healer.worker.loop import run_worker
from dag_healer.worker.pipeline import default_session_factory

V1_DB = Path(__file__).parent / "fixtures" / "store" / "healer_v1.db"
OUTDATED = "dag-healer store schema v1: start the worker or run 'dag-healer store migrate'"
V1_INCIDENTS = 4


@pytest.fixture(autouse=True)
def _fresh_engines() -> Iterator[None]:
    db.dispose_engines()
    yield
    db.dispose_engines()


@pytest.fixture
def v1_url(tmp_path: Path) -> str:
    """URL of a private copy of the frozen v1 store."""
    path = tmp_path / "store" / "healer.db"
    path.parent.mkdir()
    shutil.copyfile(V1_DB, path)
    return f"sqlite:///{path.as_posix()}"


def version_of(url: str) -> int:
    return current_version(db.get_engine(url))


def incident_count(url: str) -> int:
    database = db.get_engine(url).url.database
    assert database is not None
    with closing(sqlite3.connect(database)) as conn:
        return int(conn.execute("SELECT count(*) FROM healer_incidents").fetchone()[0])


def make_cfg(tmp_path: Path, url: str) -> HealerConfig:
    repo = tmp_path / "repo"
    (repo / "dags").mkdir(parents=True, exist_ok=True)
    return config_from_mapping(
        {"repo": {"path": str(repo)}, "store": {"url": url}, "monitor": {"poll_interval_seconds": 3}}
    )


# =========================================================================== sessions


def test_a_plain_session_never_migrates_an_older_store(v1_url: str) -> None:
    with pytest.raises(StoreSchemaOutdated) as caught, db.session_scope(v1_url):
        pass  # pragma: no cover - never reached
    assert str(caught.value) == OUTDATED
    assert version_of(v1_url) == 1
    backup = backup_path(db.get_engine(v1_url), 1)
    assert backup is not None and not backup.exists()


def test_a_migrating_session_upgrades_the_store_first(v1_url: str) -> None:
    with db.session_scope(v1_url, allow_migrate=True) as session:
        assert session.query(Incident).count() == V1_INCIDENTS
    assert version_of(v1_url) == 2
    backup = backup_path(db.get_engine(v1_url), 1)
    assert backup is not None and backup.is_file()
    with db.session_scope(v1_url) as session:  # the API's sessions work from now on
        assert {incident.orchestrator for incident in session.query(Incident)} == {"airflow"}


def test_the_worker_sessions_migrate(tmp_path: Path, healer_env: Path, v1_url: str) -> None:
    factory = default_session_factory(make_cfg(tmp_path, v1_url))
    with factory() as session:
        assert session.query(Incident).count() == V1_INCIDENTS
    assert version_of(v1_url) == 2


class QuietClient:
    """Airflow stand-in: no failed task instance, unknown pushed task instances."""

    base_url = "http://airflow.test"

    def __init__(self) -> None:
        self.cleared: list[tuple[Any, ...]] = []

    def list_failed_task_instances(self, **_: Any) -> list[TaskInstanceInfo]:
        return []

    def get_task_instance(self, *_: Any, **__: Any) -> TaskInstanceInfo:
        raise AirflowAPIError("not found", status_code=404)

    def clear_task_instances(self, *args: Any, **kwargs: Any) -> dict[str, Any]:
        self.cleared.append((*args, *sorted(kwargs.items())))
        return {"task_instances": [{"task_id": args[2]}]}

    def health(self) -> dict[str, Any]:
        return {"metadatabase": {"status": "healthy"}}


def test_run_worker_migrates_a_v1_store_at_start(tmp_path: Path, healer_env: Path, v1_url: str) -> None:
    cfg = make_cfg(tmp_path, v1_url)
    client = QuietClient()
    stats = run_worker(
        cfg, once=True, provider=FakeProvider(cfg), client=client, install_signal_handlers=False
    )
    assert stats.errors == []
    assert version_of(v1_url) == 2
    assert incident_count(v1_url) == V1_INCIDENTS
    # the pending retry_task command of the v1 store was applied after the migration
    assert stats.commands == 1 and stats.commands_failed == 0
    assert len(client.cleared) == 1


# =========================================================================== API (web process)


class AllowAll:
    """Authorizer stand-in: every user reads and edits every DAG."""

    async def check_dag_access(self, request: Request, user: Any, method: str, entity: str | None) -> None:
        return None

    def authorized_dag_ids(self, user: Any, method: str = "GET") -> set[str]:
        return {"sales_daily_etl", "customers_sync", "inventory_report"}

    def can_read_dag(self, user: Any, dag_id: str) -> bool:
        return True

    def can_edit_dag(self, user: Any, dag_id: str, entity: str = "TASK_INSTANCE") -> bool:
        return True


class User:
    def get_name(self) -> str:
        return "admin"

    def get_id(self) -> str:
        return "admin"


async def current_user(request: Request) -> Any:
    if not request.headers.get("x-test-user"):
        raise HTTPException(status_code=401, detail="Not authenticated")
    return User()


@pytest.fixture
def api(
    tmp_path: Path, healer_env: Path, v1_url: str, monkeypatch: pytest.MonkeyPatch
) -> Iterator[TestClient]:
    repo = tmp_path / "repo"
    (repo / "dags").mkdir(parents=True)
    config = repo / "healer.yaml"
    config.write_text(
        textwrap.dedent(
            """\
            version: 1
            repo: {path: ., dags_dir: dags}
            """
        ),
        encoding="utf-8",
    )
    monkeypatch.setenv("HEALER_CONFIG", str(config))
    monkeypatch.setenv("HEALER_DATABASE_URL", v1_url)
    deps.reset_config_cache()
    app = create_app()
    app.dependency_overrides[auth.get_current_user] = current_user
    app.dependency_overrides[auth.get_authorizer] = AllowAll
    with TestClient(app) as client:
        yield client
    deps.reset_config_cache()


def test_the_api_answers_503_until_the_worker_migrated(api: TestClient, v1_url: str) -> None:
    headers = {"X-Test-User": "admin"}
    outdated = api.get("/api/v1/incidents", headers=headers)
    assert outdated.status_code == 503
    assert outdated.json() == {"detail": OUTDATED}
    again = api.get("/api/v1/dags/sales_daily_etl/incidents", headers=headers)
    assert (again.status_code, again.json()) == (503, {"detail": OUTDATED})
    assert version_of(v1_url) == 1  # the web process changed nothing
    backup = backup_path(db.get_engine(v1_url), 1)
    assert backup is not None and not backup.exists()

    db.init_db(db.get_engine(v1_url), allow_migrate=True)  # what the worker does at start

    listed = api.get("/api/v1/incidents", headers=headers)
    assert listed.status_code == 200
    assert listed.json()["total"] == V1_INCIDENTS
