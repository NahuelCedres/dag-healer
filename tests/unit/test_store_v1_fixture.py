"""The frozen phase-5 store (schema v1) fixtures used by the store v2 migration tests.

Files under ``tests/unit/fixtures/store/``:

* ``schema_v1.sql`` - the SQLite DDL that ``init_db`` creates for schema v1 (the
  ``sqlite_master`` statements in creation order);
* ``schema_v1.postgresql.sql`` - the PostgreSQL DDL dumped from the demo stack
  (``pg_dump --schema-only``; see its header);
* ``healer_v1.db`` - a small populated v1 SQLite store: incidents (DATA, CODE, AMBIGUOUS and
  a mapped, ignored one), timeline events, alerts, commands, pending events and contract
  states, all with fixed ids and timestamps.

These are a frozen baseline: section 11.4 of ``docs/architecture/orchestrators.md`` migrates
copies of them. The checks against the *current* models only run while
``SCHEMA_VERSION == 1``; once the store moves to v2 they are skipped and the files must not
be regenerated. ``HEALER_UPDATE_GOLDEN=1`` rewrites ``schema_v1.sql`` and ``healer_v1.db``
(refused unless the models are still at v1).
"""

from __future__ import annotations

import os
import re
import shutil
import sqlite3
from collections.abc import Iterator
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

import pytest
from sqlalchemy import create_engine

from dag_healer.store.models import SCHEMA_VERSION

FIXTURES = Path(__file__).parent / "fixtures" / "store"
SCHEMA_SQL = FIXTURES / "schema_v1.sql"
SCHEMA_PG_SQL = FIXTURES / "schema_v1.postgresql.sql"
V1_DB = FIXTURES / "healer_v1.db"
UPDATE = os.environ.get("HEALER_UPDATE_GOLDEN") == "1"
V1_ONLY = pytest.mark.skipif(
    SCHEMA_VERSION != 1, reason=f"schema v1 is frozen; the models are at v{SCHEMA_VERSION}"
)

TABLES = (
    "healer_incidents",
    "healer_incident_events",
    "healer_contracts",
    "healer_alerts",
    "healer_commands",
    "healer_pending_events",
    "healer_schema_version",
)
EXPECTED_COUNTS = {
    "healer_incidents": 4,
    "healer_incident_events": 12,
    "healer_contracts": 3,
    "healer_alerts": 3,
    "healer_commands": 3,
    "healer_pending_events": 3,
    "healer_schema_version": 1,
}
SCHEMA_HEADER = (
    "-- dag-healer store schema v1 (phase 5), SQLite: the frozen DDL created by init_db.\n"
    "--\n"
    "-- The sqlite_master statements of a v1 store, in creation order. Regenerate only while\n"
    "-- dag_healer.store.models.SCHEMA_VERSION == 1 (HEALER_UPDATE_GOLDEN=1 pytest\n"
    "-- tests/unit/test_store_v1_fixture.py); after that it is the baseline of the v2 migration.\n"
)

T0 = datetime(2026, 10, 17, 18, 30, tzinfo=timezone.utc)
SALES_ID = "a1" * 16
CUSTOMERS_ID = "b2" * 16
INVENTORY_ID = "c3" * 16
MAPPED_ID = "d4" * 16
RUN = "manual__2026-10-17T18:28:46.093857+00:00"


# =========================================================================== helpers


def sqlite_schema(path: Path) -> list[str]:
    """The DDL statements of an SQLite file, in creation order (auto-indexes excluded)."""
    with sqlite3.connect(path) as conn:
        rows = conn.execute("SELECT sql FROM sqlite_master WHERE sql IS NOT NULL ORDER BY rowid").fetchall()
    return [_tidy(str(sql)) for (sql,) in rows]


def _tidy(statement: str) -> str:
    """A statement without trailing spaces (SQLAlchemy's DDL ends column lines with ", ")."""
    return "\n".join(line.rstrip() for line in statement.strip().splitlines())


def parse_schema_file(path: Path) -> list[str]:
    """Statements of ``schema_v1.sql`` (comments dropped, split on ``;`` at line ends)."""
    text = "\n".join(
        line for line in path.read_text(encoding="utf-8").splitlines() if not line.startswith("--")
    )
    return [_tidy(statement) for statement in re.split(r";[ \t]*(?:\n|$)", text) if statement.strip()]


def same_schema(actual: list[str], expected: list[str]) -> bool:
    """Equal DDL: tables in the same order, indexes as a set.

    SQLAlchemy creates the indexes of a table in ``Table.indexes`` order, which is a set and
    varies between processes; the order of the tables is deterministic (dependency order).
    """

    def tables(statements: list[str]) -> list[str]:
        return [s for s in statements if s.startswith("CREATE TABLE")]

    return tables(actual) == tables(expected) and sorted(actual) == sorted(expected)


def render_schema_file(statements: list[str]) -> str:
    return SCHEMA_HEADER + "\n" + "".join(f"{statement};\n\n" for statement in statements).rstrip("\n") + "\n"


def dump_rows(path: Path) -> dict[str, list[tuple[Any, ...]]]:
    """Every row of every table (ordered), as stored."""
    with sqlite3.connect(path) as conn:
        return {table: conn.execute(f"SELECT * FROM {table} ORDER BY 1").fetchall() for table in TABLES}


def build_v1_db(path: Path) -> None:
    """Create a populated v1 store at ``path`` from the current models (v1 only)."""
    from sqlalchemy.orm import Session

    from dag_healer.store.db import init_db
    from dag_healer.store.models import (
        Alert,
        Command,
        ContractState,
        Incident,
        IncidentEvent,
        PendingEvent,
        SchemaVersion,
    )

    if SCHEMA_VERSION != 1:  # pragma: no cover - guarded by V1_ONLY
        raise RuntimeError("the v1 fixture can only be built from the v1 models")
    path.unlink(missing_ok=True)
    engine = create_engine(f"sqlite:///{path.as_posix()}")
    try:
        init_db(engine)
        with Session(engine) as session, session.begin():
            version = session.get(SchemaVersion, 1)
            assert version is not None
            version.applied_at = T0 - timedelta(hours=1)

            def at(minutes: int) -> datetime:
                return T0 + timedelta(minutes=minutes)

            session.add_all(
                [
                    Incident(
                        id=SALES_ID,
                        dag_id="sales_daily_etl",
                        run_id=RUN,
                        task_id="transform_sales",
                        map_index=-1,
                        try_number=1,
                        status="reported",
                        category="data",
                        confidence=0.95,
                        source="rest",
                        error_summary="KeyError: 'amount'\n\nTraceback (most recent call last):\n"
                        '  File "/opt/airflow/repo/src/etl/sales.py", line 26, in transform_sales\n'
                        "KeyError: 'amount'",
                        diagnosis="Column amount is missing from dataset sales (approved contract).",
                        violated_rule={
                            "rule": "missing_column",
                            "dataset": "sales",
                            "column": "amount",
                            "message": "Column 'amount' is missing from dataset 'sales'.",
                            "contract_status": "approved",
                        },
                        sample={
                            "dataset": "sales",
                            "columns": ["order_id", "customer_email", "amt"],
                            "rows": [
                                {"order_id": "ORD-50031", "customer_email": "h:038151c9d1", "amt": 58.0}
                            ],
                            "masked": True,
                        },
                        evidence=["approved contract violated: missing_column(sales.amount)"],
                        signature="5e1f0c4c0a1d2b3c4d5e6f708192a3b4c5d6e7f8",
                        occurrences=1,
                        attempts=0,
                        tokens_used=0,
                        created_at=at(0),
                        updated_at=at(1),
                        claimed_at=at(0),
                    ),
                    Incident(
                        id=CUSTOMERS_ID,
                        dag_id="customers_sync",
                        run_id=RUN,
                        task_id="transform",
                        map_index=-1,
                        try_number=1,
                        status="repaired",
                        category="code",
                        confidence=0.9,
                        source="listener",
                        error_summary="ValueError: unconverted data remains: T08:30:00Z",
                        diagnosis="parse_signup_date does not accept ISO-8601 timestamps.",
                        evidence=["valid data, error in src/etl/customers.py::parse_signup_date"],
                        test_path="tests/test_customers_signup_date.py",
                        test_code="def test_parse_signup_date_accepts_iso_timestamps():\n    assert True\n",
                        diff="diff --git a/src/etl/customers.py b/src/etl/customers.py\n",
                        pytest_output="=== pytest before the fix (the new test must fail) ===\n1 failed\n\n"
                        "=== pytest after the fix ===\n2 passed",
                        pr_md_path=f"/opt/airflow/healer/prs/{CUSTOMERS_ID}/PR.md",
                        signature="0a9b8c7d6e5f40312a3b4c5d6e7f8091a2b3c4d5",
                        occurrences=1,
                        attempts=2,
                        tokens_used=0,
                        created_at=at(2),
                        updated_at=at(5),
                        claimed_at=at(2),
                    ),
                    Incident(
                        id=INVENTORY_ID,
                        dag_id="inventory_report",
                        run_id=RUN,
                        task_id="build_report",
                        map_index=-1,
                        try_number=1,
                        status="waiting_human",
                        category="ambiguous",
                        confidence=0.5,
                        source="rest",
                        error_summary="KeyError: 'qty'",
                        diagnosis="No approved contract (status: draft).",
                        violated_rule={
                            "rule": "missing_column",
                            "dataset": "inventory",
                            "column": "qty",
                            "message": "Column 'qty' is missing from dataset 'inventory'.",
                            "contract_status": "draft",
                        },
                        signature="77aa66bb55cc44dd33ee22ff1100998877665544",
                        occurrences=2,
                        attempts=0,
                        tokens_used=0,
                        created_at=at(3),
                        updated_at=at(20),
                        claimed_at=None,
                    ),
                    Incident(
                        id=MAPPED_ID,
                        dag_id="inventory_report",
                        run_id="scheduled__2026-10-17T00:00:00+00:00",
                        task_id="publish_report",
                        map_index=3,
                        try_number=2,
                        status="ignored",
                        category=None,
                        confidence=None,
                        source="callback",
                        error_summary="the job died",
                        occurrences=1,
                        attempts=0,
                        tokens_used=0,
                        resolution="ignore",
                        resolved_by="alice",
                        created_at=at(4),
                        updated_at=at(30),
                        claimed_at=None,
                    ),
                ]
            )
            session.flush()
            events = [
                (SALES_ID, 0, "triage", "Automatic triage started", None, "dag-healer"),
                (
                    SALES_ID,
                    0,
                    "classification",
                    "Classified as data",
                    {"rule": "a_contract_violated"},
                    "policy",
                ),
                (SALES_ID, 1, "status", "reported", {"from": "reporting", "to": "reported"}, "dag-healer"),
                (SALES_ID, 1, "alert", "Alert sent", {"channels": ["log", "email"]}, "dag-healer"),
                (CUSTOMERS_ID, 2, "confirmed", "The listener event was confirmed", None, "dag-healer"),
                (
                    CUSTOMERS_ID,
                    3,
                    "classification",
                    "Classified as code",
                    {"rule": "b_code_with_valid_data"},
                    "fake",
                ),
                (
                    CUSTOMERS_ID,
                    5,
                    "repair",
                    "Fix proposed",
                    {"branch": "healer/customers_sync-transform-b2b2b2b2"},
                    "dag-healer",
                ),
                (
                    CUSTOMERS_ID,
                    5,
                    "status",
                    "repaired",
                    {"from": "repairing", "to": "repaired"},
                    "dag-healer",
                ),
                (
                    INVENTORY_ID,
                    3,
                    "classification",
                    "Needs a human",
                    {"rule": "c_no_approved_contract"},
                    "policy",
                ),
                (
                    INVENTORY_ID,
                    20,
                    "occurrence",
                    "Same failure seen again",
                    {"run_id": "manual__2"},
                    "dag-healer",
                ),
                (
                    MAPPED_ID,
                    4,
                    "status",
                    "waiting_human",
                    {"from": "classified", "to": "waiting_human"},
                    "dag-healer",
                ),
                (
                    MAPPED_ID,
                    30,
                    "command",
                    "alice resolved the incident as ignore",
                    {"resolution": "ignore"},
                    "alice",
                ),
            ]
            session.add_all(
                IncidentEvent(id=n, incident_id=i, at=at(m), kind=k, message=msg, data=d, actor=a)
                for n, (i, m, k, msg, d, a) in enumerate(events, start=1)
            )
            session.add_all(
                [
                    Alert(
                        id=1,
                        incident_id=SALES_ID,
                        channel="log",
                        recipients=[],
                        subject="[dag-healer] DATA issue in sales_daily_etl.transform_sales",
                        status="sent",
                        sent_at=at(1),
                    ),
                    Alert(
                        id=2,
                        incident_id=SALES_ID,
                        channel="email",
                        recipients=["data-team@example.com"],
                        subject="[dag-healer] DATA issue in sales_daily_etl.transform_sales",
                        status="sent",
                        sent_at=at(1),
                    ),
                    Alert(
                        id=3,
                        incident_id=CUSTOMERS_ID,
                        channel="email",
                        recipients=["data-team@example.com"],
                        subject="[dag-healer] Fix proposed: customers_sync.transform",
                        status="failed",
                        error="SMTP connection refused",
                        sent_at=at(5),
                    ),
                    Command(
                        id=1,
                        kind="resolve",
                        incident_id=MAPPED_ID,
                        dag_id="inventory_report",
                        payload={"resolution": "ignore", "note": "known flaky export"},
                        actor="alice",
                        status="applied",
                        created_at=at(29),
                        applied_at=at(30),
                    ),
                    Command(
                        id=2,
                        kind="retry_task",
                        incident_id=CUSTOMERS_ID,
                        dag_id="customers_sync",
                        payload={"run_id": RUN, "task_id": "transform", "map_index": -1},
                        actor="bob",
                        status="pending",
                        created_at=at(31),
                    ),
                    Command(
                        id=3,
                        kind="approve_contract",
                        incident_id=None,
                        dag_id="inventory_report",
                        payload={"path": "healer/contracts/inventory_report.yaml", "source_hash": "f" * 64},
                        actor="carol",
                        status="failed",
                        created_at=at(32),
                        applied_at=at(33),
                        error="The contract changed since it was reviewed.",
                    ),
                    PendingEvent(
                        id=1,
                        dag_id="customers_sync",
                        run_id=RUN,
                        task_id="transform",
                        map_index=-1,
                        try_number=1,
                        payload={"source": "listener", "error": "ValueError: unconverted data remains"},
                        received_at=at(1),
                        status="confirmed",
                    ),
                    PendingEvent(
                        id=2,
                        dag_id="sales_daily_etl",
                        run_id=RUN,
                        task_id="extract_sales",
                        map_index=-1,
                        try_number=1,
                        payload={"source": "callback", "state": "success"},
                        received_at=at(2),
                        status="discarded",
                    ),
                    PendingEvent(
                        id=3,
                        dag_id="inventory_report",
                        run_id="scheduled__2026-10-17T00:00:00+00:00",
                        task_id="publish_report",
                        map_index=4,
                        try_number=1,
                        payload={"source": "listener", "error": "TimeoutError"},
                        received_at=at(40),
                        status="pending_check",
                    ),
                    ContractState(
                        dag_id="sales_daily_etl",
                        status="approved",
                        approved_by="data-lead",
                        approved_at=T0 - timedelta(minutes=30),
                        path="healer/contracts/sales_daily_etl.yaml",
                        source_hash="1" * 64,
                        updated_at=T0 - timedelta(minutes=30),
                    ),
                    ContractState(
                        dag_id="customers_sync",
                        status="approved",
                        approved_by="data-lead",
                        approved_at=T0 - timedelta(minutes=30),
                        path="healer/contracts/customers_sync.yaml",
                        source_hash="2" * 64,
                        updated_at=T0 - timedelta(minutes=30),
                    ),
                    ContractState(
                        dag_id="inventory_report",
                        status="draft",
                        path="healer/contracts/inventory_report.yaml",
                        source_hash=None,
                        updated_at=T0 - timedelta(minutes=30),
                    ),
                ]
            )
    finally:
        engine.dispose()


# =========================================================================== update mode


@pytest.fixture(scope="module", autouse=True)
def _maybe_update(tmp_path_factory: pytest.TempPathFactory) -> Iterator[None]:
    if UPDATE:
        if SCHEMA_VERSION != 1:
            pytest.fail(f"refusing to regenerate the frozen v1 fixtures from v{SCHEMA_VERSION} models")
        FIXTURES.mkdir(parents=True, exist_ok=True)
        fresh = tmp_path_factory.mktemp("store_v1") / "fresh.db"
        _create_empty_v1(fresh)
        SCHEMA_SQL.write_text(render_schema_file(sqlite_schema(fresh)), encoding="utf-8", newline="\n")
        build_v1_db(V1_DB)
    yield


def _create_empty_v1(path: Path) -> None:
    from dag_healer.store.db import init_db

    engine = create_engine(f"sqlite:///{path.as_posix()}")
    try:
        init_db(engine)
    finally:
        engine.dispose()


# =========================================================================== the frozen files


def test_v1_db_fixture_has_the_frozen_schema() -> None:
    assert same_schema(sqlite_schema(V1_DB), parse_schema_file(SCHEMA_SQL))


def test_v1_db_fixture_contents_are_consistent() -> None:
    with sqlite3.connect(V1_DB) as conn:
        counts = {table: conn.execute(f"SELECT count(*) FROM {table}").fetchone()[0] for table in TABLES}
        assert conn.execute("PRAGMA integrity_check").fetchone() == ("ok",)
        assert conn.execute("PRAGMA foreign_key_check").fetchall() == []
        assert conn.execute("SELECT version FROM healer_schema_version").fetchall() == [(1,)]
        keys = conn.execute(
            "SELECT dag_id, run_id, task_id, map_index, try_number FROM healer_incidents ORDER BY id"
        ).fetchall()
    assert counts == EXPECTED_COUNTS
    assert len(set(keys)) == len(keys)
    assert (V1_DB.parent / (V1_DB.name + "-wal")).exists() is False


def test_v1_unique_key_is_the_five_column_task_instance_key(tmp_path: Path) -> None:
    copy = tmp_path / "v1.db"
    shutil.copyfile(V1_DB, copy)
    with sqlite3.connect(copy) as conn:
        row = conn.execute("SELECT * FROM healer_incidents WHERE id = ?", (SALES_ID,)).fetchone()
        columns = [d[0] for d in conn.execute("SELECT * FROM healer_incidents LIMIT 0").description]
        duplicate = dict(zip(columns, row, strict=True)) | {"id": "e5" * 16}
        placeholders = ", ".join("?" for _ in columns)
        with pytest.raises(sqlite3.IntegrityError, match="UNIQUE"):
            conn.execute(f"INSERT INTO healer_incidents VALUES ({placeholders})", list(duplicate.values()))


def test_postgresql_schema_dump_matches_the_v1_layout() -> None:
    text = SCHEMA_PG_SQL.read_text(encoding="utf-8")
    for table in TABLES:
        assert f"CREATE TABLE public.{table} (" in text
    assert (
        "ADD CONSTRAINT uq_healer_incidents_ti UNIQUE (dag_id, run_id, task_id, map_index, try_number)"
        in text
    )
    assert "ADD CONSTRAINT healer_contracts_pkey PRIMARY KEY (dag_id)" in text
    for index in (
        "ix_healer_incidents_dag_created",
        "ix_healer_incidents_signature",
        "ix_healer_incidents_status",
    ):
        assert f"CREATE INDEX {index} ON public.healer_incidents" in text
    assert re.search(r"^\s+orchestrator\s", text, re.MULTILINE) is None  # v1 has no orchestrator column


# =========================================================================== drift guards (v1 models only)


@V1_ONLY
def test_current_models_still_create_the_frozen_sqlite_schema(tmp_path: Path) -> None:
    fresh = tmp_path / "fresh.db"
    _create_empty_v1(fresh)
    assert same_schema(sqlite_schema(fresh), parse_schema_file(SCHEMA_SQL))


@V1_ONLY
def test_postgresql_dump_has_every_column_of_the_current_models() -> None:
    from dag_healer.store.models import Base

    text = SCHEMA_PG_SQL.read_text(encoding="utf-8")
    for table in Base.metadata.sorted_tables:
        block = text.split(f"CREATE TABLE public.{table.name} (", 1)[1].split(");", 1)[0]
        dumped = {line.strip().split(" ", 1)[0] for line in block.strip().splitlines()}
        assert dumped == {column.name for column in table.columns}, table.name


@V1_ONLY
def test_the_builder_reproduces_the_committed_db(tmp_path: Path) -> None:
    rebuilt = tmp_path / "rebuilt.db"
    build_v1_db(rebuilt)
    assert dump_rows(rebuilt) == dump_rows(V1_DB)


@V1_ONLY
def test_the_current_store_code_reads_the_v1_db(tmp_path: Path, healer_env: Path) -> None:
    from dag_healer.store import dispose_engines, session_scope
    from dag_healer.store import repo as store_repo

    copy = tmp_path / "v1.db"
    shutil.copyfile(V1_DB, copy)
    try:
        with session_scope(f"sqlite:///{copy.as_posix()}") as session:
            incidents = {incident.id: incident.to_dict() for incident in store_repo.list_incidents(session)}
            assert set(incidents) == {SALES_ID, CUSTOMERS_ID, INVENTORY_ID, MAPPED_ID}
            assert incidents[SALES_ID]["created_at"] == T0.isoformat()
            assert incidents[MAPPED_ID]["map_index"] == 3
            assert [e.kind for e in store_repo.list_events(session, CUSTOMERS_ID)][-1] == "status"
            assert [c.id for c in store_repo.pending_commands(session)] == [2]
    finally:
        dispose_engines()
