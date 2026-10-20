"""Fixtures of the Airflow integration tests (run inside the image, compose profile "tests").

Run with ``python scripts/tasks.py test-airflow``. The compose service provides a standalone
environment (SQLite metadata DB under ``/tmp/airflow-tests``, SimpleAuthManager with users
``admin`` (role admin) and ``viewer`` (role viewer), ``ALL_ADMINS`` off); this module fixes
the passwords of those users, creates a throw-away dag-healer repository (``healer.yaml`` +
a draft contract) and store, builds the real Airflow FastAPI app with the plugin mounted
and logs both users in through ``POST /auth/token``.

The environment is prepared at import time because Airflow reads its configuration when
``airflow`` is first imported. Without a real Airflow installation the tests are not
collected at all.
"""

from __future__ import annotations

import importlib.util
import json
import os
import shutil
import textwrap
from collections.abc import Iterator
from pathlib import Path
from typing import Any

import pytest

PASSWORDS = {"admin": "admin", "viewer": "viewer"}
BASE_URL = "http://localhost:8080"


def _real_airflow_installed() -> bool:
    """True for an installed Airflow (``tests/airflow`` itself is only a namespace package)."""
    try:
        spec = importlib.util.find_spec("airflow")
    except (ImportError, ValueError):
        return False
    return spec is not None and spec.origin not in (None, "namespace")


def _write_healer_repo(root: Path) -> Path:
    """A minimal repository: healer.yaml and a draft contract for ``inventory_report``."""
    if root.exists():
        shutil.rmtree(root)
    (root / "dags").mkdir(parents=True)
    contracts = root / "healer" / "contracts"
    contracts.mkdir(parents=True)
    (contracts / "inventory_report.yaml").write_text(
        textwrap.dedent(
            """\
            version: 1
            dag_id: inventory_report
            status: draft
            datasets:
              - name: inventory
                task_id: build_report
                locator: {type: csv, path: data/incoming/inventory.csv}
                columns:
                  - {name: sku, dtype: string, nullable: false}
                  - {name: qty, dtype: integer, nullable: false, min: 0}
            """
        ),
        encoding="utf-8",
    )
    config = root / "healer.yaml"
    config.write_text(
        textwrap.dedent(
            """\
            version: 1
            repo: {path: ., dags_dir: dags}
            monitor: {dags: ["*"]}
            contracts: {dir: healer/contracts}
            """
        ),
        encoding="utf-8",
    )
    return config


def _prepare_environment() -> None:
    """Configure Airflow and dag-healer for the tests (before ``airflow`` is imported)."""
    home = Path(os.environ.setdefault("AIRFLOW_HOME", "/tmp/airflow-tests"))
    home.mkdir(parents=True, exist_ok=True)
    passwords_file = home / "simple_auth_manager_passwords.json.generated"
    passwords_file.write_text(json.dumps(PASSWORDS) + "\n", encoding="utf-8")
    defaults = {
        "AIRFLOW__CORE__LOAD_EXAMPLES": "false",
        "AIRFLOW__DATABASE__SQL_ALCHEMY_CONN": f"sqlite:///{(home / 'airflow.db').as_posix()}",
        "AIRFLOW__API_AUTH__JWT_SECRET": "dag-healer-tests-jwt-secret",
        "HEALER_HOME": str(home / "healer"),
        "HEALER_DATABASE_URL": f"sqlite:///{(home / 'healer.db').as_posix()}",
        "HEALER_INGEST_TOKEN": "dag-healer-tests-ingest-token",
        "HEALER_LLM_PROVIDER": "fake",
    }
    for name, value in defaults.items():
        os.environ.setdefault(name, value)
    # The tests rely on these exact settings.
    os.environ.update(
        {
            "AIRFLOW__CORE__AUTH_MANAGER": (
                "airflow.api_fastapi.auth.managers.simple.simple_auth_manager.SimpleAuthManager"
            ),
            "AIRFLOW__CORE__SIMPLE_AUTH_MANAGER_USERS": "admin:admin,viewer:viewer",
            "AIRFLOW__CORE__SIMPLE_AUTH_MANAGER_ALL_ADMINS": "false",
            "AIRFLOW__CORE__SIMPLE_AUTH_MANAGER_PASSWORDS_FILE": str(passwords_file),
            "AIRFLOW__API__BASE_URL": BASE_URL,
        }
    )
    os.environ["HEALER_CONFIG"] = str(_write_healer_repo(home / "healer-repo"))
    url = os.environ["HEALER_DATABASE_URL"]
    if url.startswith("sqlite:///"):
        Path(url.removeprefix("sqlite:///")).unlink(missing_ok=True)


if _real_airflow_installed():
    _prepare_environment()
else:  # hosts without Airflow (e.g. `pytest tests/` on Windows): nothing to run here
    collect_ignore_glob = ["test_*.py"]


@pytest.fixture(scope="session")
def airflow_db() -> None:
    """Create the Airflow metadata tables (SQLite, from the ORM)."""
    from airflow.utils.db import initdb

    initdb()


@pytest.fixture(scope="session")
def seeded(airflow_db: None) -> dict[str, str]:
    """Incidents in the dag-healer store, one per action under test."""
    from dag_healer.store import db
    from dag_healer.store import repo as store_repo

    specs = {
        "waiting": ("inventory_report", "build_report", "waiting_human"),
        "waiting_admin": ("inventory_report", "build_report", "waiting_human"),
        "failed_repair": ("inventory_report", "build_report", "repair_failed"),
        "reported": ("inventory_report", "build_report", "reported"),
    }
    ids: dict[str, str] = {}
    with db.session_scope(os.environ["HEALER_DATABASE_URL"]) as session:
        for number, (key, (dag_id, task_id, status)) in enumerate(specs.items(), start=1):
            incident, _ = store_repo.create_incident(
                session,
                dag_id=dag_id,
                run_id=f"manual__2026-10-0{number}T00:00:00+00:00",
                task_id=task_id,
                try_number=1,
                source="rest",
                error_summary="KeyError: \"['qty'] not in index\" (reported by ops@example.com)",
                status=status,
                category="ambiguous",
            )
            ids[key] = incident.id
    return ids


@pytest.fixture(scope="session")
def airflow_app(airflow_db: None) -> Any:
    """The real Airflow API application (``airflow.api_fastapi.app.create_app``)."""
    from airflow.api_fastapi.app import create_app, purge_cached_app

    purge_cached_app()
    return create_app()


@pytest.fixture(scope="session")
def client(airflow_app: Any) -> Iterator[Any]:
    """A TestClient running the Airflow app lifespan (plugin sub-apps included)."""
    from fastapi.testclient import TestClient

    with TestClient(airflow_app) as test_client:
        yield test_client


@pytest.fixture(scope="session")
def tokens(client: Any) -> dict[str, str]:
    """JWTs of ``admin`` and ``viewer`` from ``POST /auth/token``."""
    result: dict[str, str] = {}
    for username, password in PASSWORDS.items():
        response = client.post("/auth/token", json={"username": username, "password": password})
        assert response.status_code == 201, response.text
        result[username] = response.json()["access_token"]
    return result
