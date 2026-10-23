"""Store schema v2 migration on PostgreSQL (section 11.4 of docs/architecture/orchestrators.md).

Runs in the compose ``tests`` profile (``python scripts/tasks.py test-airflow``). The tests
container reaches the stack's PostgreSQL server as ``postgres``; ``HEALER_TEST_POSTGRES_URL``
overrides the URL (a database of a user that may ``CREATE DATABASE``). Without a reachable
server the tests are skipped.

Every test works in its own throw-away database (``healer_migtest_<random>``): it loads the
frozen phase-5 schema (``tests/unit/fixtures/store/schema_v1.postgresql.sql``, run on a
dedicated connection because it empties ``search_path``) and the rows of the frozen v1
SQLite store (``tests/unit/fixtures/store/healer_v1.db``), migrates, checks, and drops the
database. The stack's own databases are never written.

``HEALER_TEST_POSTGRES_COPY_OF=<database>`` (for example ``healer``) also copies that
database into a throw-away one (read only on the source: a ``READ ONLY`` transaction,
rows copied into the frozen v1 schema), migrates the copy twice and compares the row counts,
the keys and every row. Use it to check a live store before rolling out the migration.
"""

from __future__ import annotations

import os
import sqlite3
import threading
import uuid
from collections.abc import Callable, Iterator
from contextlib import closing
from pathlib import Path
from typing import Any

import pytest
from sqlalchemy import Engine, create_engine, inspect, text
from sqlalchemy.engine import make_url
from sqlalchemy.exc import IntegrityError, OperationalError, ProgrammingError
from sqlalchemy.pool import NullPool

from dag_healer.store import db, migrations
from dag_healer.store import repo as store_repo
from dag_healer.store.db import StoreSchemaOutdated

FIXTURES = Path(__file__).resolve().parents[1] / "unit" / "fixtures" / "store"
SCHEMA_V1 = FIXTURES / "schema_v1.postgresql.sql"
V1_DB = FIXTURES / "healer_v1.db"
ADMIN_URL = os.environ.get(
    "HEALER_TEST_POSTGRES_URL", "postgresql+psycopg2://airflow:airflow@postgres/postgres"
)
COPY_OF = os.environ.get("HEALER_TEST_POSTGRES_COPY_OF", "")

#: v1 tables in an order that satisfies the foreign keys, with their sort key.
TABLES: dict[str, str] = {
    "healer_incidents": "id",
    "healer_contracts": "dag_id",
    "healer_pending_events": "id",
    "healer_schema_version": "version",
    "healer_incident_events": "id",
    "healer_alerts": "id",
    "healer_commands": "id",
}
DATA_TABLES = tuple(table for table in TABLES if table != "healer_schema_version")
SERIAL_TABLES = ("healer_alerts", "healer_commands", "healer_incident_events", "healer_pending_events")
V1_COUNTS = {
    "healer_incidents": 4,
    "healer_contracts": 3,
    "healer_pending_events": 3,
    "healer_schema_version": 1,
    "healer_incident_events": 12,
    "healer_alerts": 3,
    "healer_commands": 3,
}
V2_COLUMNS = {
    "healer_incidents": ["orchestrator", "native_ref", "error_name"],
    "healer_pending_events": ["orchestrator", "channel", "native_ref"],
    "healer_commands": ["orchestrator"],
    "healer_contracts": ["orchestrator"],
}
SALES_KEY = {
    "dag_id": "sales_daily_etl",
    "run_id": "manual__2026-10-17T18:28:46.093857+00:00",
    "task_id": "transform_sales",
    "map_index": -1,
    "try_number": 1,
}
SALES_ID = "a1" * 16


# =========================================================================== server and databases


def _masked(url: str) -> str:
    return make_url(url).render_as_string(hide_password=True)


@pytest.fixture(scope="module")
def admin() -> Iterator[Engine]:
    """An AUTOCOMMIT engine on the server's maintenance database (skips without a server)."""
    engine = create_engine(
        ADMIN_URL, isolation_level="AUTOCOMMIT", poolclass=NullPool, connect_args={"connect_timeout": 5}
    )
    try:
        with engine.connect() as conn:
            conn.execute(text("SELECT 1"))
    except OperationalError as exc:
        engine.dispose()
        pytest.skip(f"no PostgreSQL server at {_masked(ADMIN_URL)} ({type(exc).__name__})")
    yield engine
    engine.dispose()


@pytest.fixture
def new_database(admin: Engine) -> Iterator[Callable[[], str]]:
    """Factory of empty throw-away databases; returns their URLs and drops them afterwards."""
    created: list[str] = []

    def make() -> str:
        name = f"healer_migtest_{uuid.uuid4().hex[:12]}"
        with admin.connect() as conn:
            conn.execute(text(f'CREATE DATABASE "{name}"'))
        created.append(name)
        return make_url(ADMIN_URL).set(database=name).render_as_string(hide_password=False)

    try:
        yield make
    finally:
        db.dispose_engines()
        with admin.connect() as conn:
            for name in created:
                conn.execute(text(f'DROP DATABASE IF EXISTS "{name}" WITH (FORCE)'))


def _raw(url: str) -> Any:
    """A psycopg2 connection of its own (never pooled: the v1 script empties search_path)."""
    return create_engine(url, poolclass=NullPool).raw_connection()


def load_v1_schema(url: str) -> None:
    raw = _raw(url)
    try:
        with raw.cursor() as cursor:
            cursor.execute(SCHEMA_V1.read_text(encoding="utf-8"))
        raw.commit()
    finally:
        raw.close()


def insert_rows(url: str, rows: dict[str, tuple[list[str], list[tuple[Any, ...]]]]) -> None:
    """Insert ``{table: (columns, rows)}`` (JSON as text, naive timestamps as UTC), fix the sequences."""
    raw = _raw(url)
    try:
        with raw.cursor() as cursor:
            cursor.execute("SET TIME ZONE 'UTC'")
            for table in TABLES:
                columns, values = rows.get(table, ([], []))
                if values:
                    marks = ", ".join(["%s"] * len(columns))
                    cursor.executemany(f"INSERT INTO {table} ({', '.join(columns)}) VALUES ({marks})", values)
            for table in SERIAL_TABLES:
                cursor.execute(
                    f"SELECT setval(pg_get_serial_sequence('{table}', 'id'), "
                    f"COALESCE((SELECT max(id) FROM {table}), 1), (SELECT count(*) > 0 FROM {table}))"
                )
        raw.commit()
    finally:
        raw.close()


def sqlite_rows(path: Path) -> dict[str, tuple[list[str], list[tuple[Any, ...]]]]:
    with closing(sqlite3.connect(path)) as conn:
        result = {}
        for table, order in TABLES.items():
            columns = [row[1] for row in conn.execute(f"PRAGMA table_info({table})")]
            result[table] = (columns, conn.execute(f"SELECT * FROM {table} ORDER BY {order}").fetchall())
        return result


def postgres_rows(url: str, *, read_only: bool = False) -> dict[str, tuple[list[str], list[tuple[Any, ...]]]]:
    """Rows of the v1 tables with JSON columns as text (SQL NULL and JSON null stay distinct)."""
    raw = _raw(url)
    try:
        with raw.cursor() as cursor:
            if read_only:
                cursor.execute("SET TRANSACTION READ ONLY")
            result = {}
            for table, order in TABLES.items():
                cursor.execute(
                    "SELECT column_name, data_type FROM information_schema.columns "
                    "WHERE table_schema = 'public' AND table_name = %s ORDER BY ordinal_position",
                    (table,),
                )
                described = cursor.fetchall()
                columns = [name for name, _type in described]
                select = ", ".join(f"{name}::text" if kind == "json" else name for name, kind in described)
                cursor.execute(f"SELECT {select} FROM {table} ORDER BY {order}")
                result[table] = (columns, cursor.fetchall())
        raw.rollback()
        return result
    finally:
        raw.close()


@pytest.fixture
def v1_store(new_database: Callable[[], str]) -> str:
    """A throw-away PostgreSQL store at schema v1 holding the rows of the v1 fixture."""
    url = new_database()
    load_v1_schema(url)
    insert_rows(url, sqlite_rows(V1_DB))
    return url


def engine_of(url: str) -> Engine:
    return db.get_engine(url)


def counts(url: str) -> dict[str, int]:
    with create_engine(url, poolclass=NullPool).connect() as conn:
        names = inspect(conn).get_table_names()
        return {name: int(conn.execute(text(f"SELECT count(*) FROM {name}")).scalar_one()) for name in names}


def snapshot(url: str, columns: dict[str, list[str]] | None = None) -> dict[str, list[tuple[Any, ...]]]:
    """Data rows restricted to ``columns`` (default: all), for before/after comparisons."""
    rows = postgres_rows(url)
    result = {}
    for table in DATA_TABLES:
        names, values = rows[table]
        keep = [names.index(name) for name in (columns or {}).get(table, names)]
        result[table] = [tuple(row[index] for index in keep) for row in values]
    return result


def schema_of(engine: Engine) -> dict[str, dict[str, Any]]:
    insp = inspect(engine)
    result: dict[str, dict[str, Any]] = {}
    for table in sorted(insp.get_table_names()):
        pk = insp.get_pk_constraint(table)
        result[table] = {
            "columns": [
                (
                    column["name"],
                    column["type"].compile(dialect=engine.dialect),
                    column["nullable"],
                    None if column.get("default") is None else str(column["default"]),
                )
                for column in insp.get_columns(table)
            ],
            "pk": (pk.get("name"), tuple(pk["constrained_columns"])),
            "unique": sorted(
                (item["name"], tuple(item["column_names"])) for item in insp.get_unique_constraints(table)
            ),
            "indexes": sorted(
                (item["name"], tuple(item["column_names"]), bool(item["unique"]))
                for item in insp.get_indexes(table)
            ),
            "fks": sorted(
                (
                    item.get("name"),
                    tuple(item["constrained_columns"]),
                    item["referred_table"],
                    tuple(item["referred_columns"]),
                    (item.get("options") or {}).get("ondelete"),
                )
                for item in insp.get_foreign_keys(table)
            ),
        }
    return result


def v1_columns(url: str) -> dict[str, list[str]]:
    return {table: columns for table, (columns, _rows) in postgres_rows(url).items()}


# =========================================================================== the frozen v1 store


def test_the_v1_store_migrates_losslessly_and_idempotently(v1_store: str) -> None:
    old_columns = v1_columns(v1_store)
    before = snapshot(v1_store)
    assert counts(v1_store) == V1_COUNTS
    engine = engine_of(v1_store)
    assert migrations.current_version(engine) == 1
    assert [migration.version for migration in migrations.pending(engine)] == [2]

    assert migrations.migrate(engine) == [2]
    assert migrations.migrate(engine) == []
    db.init_db(engine, allow_migrate=True)

    assert migrations.current_version(engine) == 2
    assert counts(v1_store) == {**V1_COUNTS, "healer_schema_version": 2, "healer_kv": 0}
    assert snapshot(v1_store, old_columns) == before
    after = v1_columns(v1_store)
    for table, added in V2_COLUMNS.items():
        assert after[table] == [*old_columns[table], *added], table
    with engine.connect() as conn:
        for table in V2_COLUMNS:
            assert conn.execute(text(f"SELECT DISTINCT orchestrator FROM {table}")).scalars().all() == [
                "airflow"
            ]
        assert conn.execute(text("SELECT DISTINCT channel FROM healer_pending_events")).scalars().all() == [
            "task-failed"
        ]
        assert conn.execute(text("SELECT version FROM healer_schema_version ORDER BY 1")).scalars().all() == [
            1,
            2,
        ]
        defaults = dict(
            conn.execute(
                text(
                    "SELECT table_name || '.' || column_name, column_default FROM information_schema.columns "
                    "WHERE table_schema = 'public' AND column_name IN ('orchestrator', 'channel') "
                    "AND table_name <> 'healer_alerts'"  # healer_alerts.channel is the v1 alert channel
                )
            ).all()
        )
    assert defaults == {
        "healer_incidents.orchestrator": "'airflow'::character varying",
        "healer_pending_events.orchestrator": "'airflow'::character varying",
        "healer_pending_events.channel": "'task-failed'::character varying",
        "healer_commands.orchestrator": "'airflow'::character varying",
        "healer_contracts.orchestrator": "'airflow'::character varying",
    }


def test_the_migrated_store_has_the_layout_of_a_new_v2_store(
    v1_store: str, new_database: Callable[[], str]
) -> None:
    migrated = engine_of(v1_store)
    migrations.migrate(migrated)
    fresh = engine_of(new_database())
    db.init_db(fresh)
    assert migrations.current_version(fresh) == 2
    assert schema_of(migrated) == schema_of(fresh)


def test_the_key_now_includes_the_orchestrator(v1_store: str) -> None:
    migrations.migrate(engine_of(v1_store))
    with db.session_scope(v1_store) as session:
        existing, created = store_repo.create_incident(session, **SALES_KEY, source="rest")
        assert (existing.id, created) == (SALES_ID, False)  # ON CONFLICT matches the new unique key
        other, created = store_repo.create_incident(session, **SALES_KEY, source="poll", orchestrator="sfn-x")
        assert created and other.id != SALES_ID
    copied = [*SALES_KEY, "status", "source", "error_summary", "occurrences", "attempts", "tokens_used"]
    copied += ["created_at", "updated_at"]
    copy = (  # the same five-column key under the default instance "airflow"
        f"INSERT INTO healer_incidents (id, {', '.join(copied)}) "
        f"SELECT 'f6f6', {', '.join(copied)} FROM healer_incidents WHERE id = '{SALES_ID}'"
    )
    with pytest.raises(IntegrityError, match="uq_healer_incidents_key"), engine_of(v1_store).begin() as conn:
        conn.execute(text(copy))
    with db.session_scope(v1_store) as session:
        assert len(store_repo.list_incidents(session, orchestrator="airflow")) == 4
        assert len(store_repo.list_incidents(session, orchestrator="sfn-x")) == 1
        store_repo.upsert_contract_state(
            session, dag_id="sales_daily_etl", status="draft", path="x", orchestrator="sfn-x"
        )
        assert store_repo.get_contract_state(session, "sales_daily_etl").status == "approved"
        store_repo.kv_set(session, store_repo.cursor_key("sfn-x"), {"arn": 1})
        store_repo.kv_set(session, store_repo.cursor_key("sfn-x"), {"arn": 2})
        assert store_repo.kv_get(session, "cursor:sfn-x") == {"arn": 2}


def test_concurrent_migrations_apply_each_version_once(
    v1_store: str, monkeypatch: pytest.MonkeyPatch
) -> None:
    calls: list[str] = []
    original = migrations.MIGRATIONS[0]

    def slow_upgrade(conn: Any, dialect: str) -> None:
        calls.append(threading.current_thread().name)
        threading.Event().wait(0.5)
        original.upgrade(conn, dialect)

    monkeypatch.setattr(
        migrations,
        "MIGRATIONS",
        (migrations.Migration(original.version, original.description, slow_upgrade, original.plan),),
    )
    engines = [create_engine(v1_store, poolclass=NullPool) for _ in range(2)]
    results: dict[int, list[int]] = {}
    errors: list[BaseException] = []
    start = threading.Barrier(len(engines))

    def work(index: int) -> None:
        try:
            start.wait()
            results[index] = migrations.migrate(engines[index])
        except BaseException as exc:  # pragma: no cover - reported below
            errors.append(exc)

    threads = [threading.Thread(target=work, args=(index,), name=f"migrator-{index}") for index in range(2)]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join(timeout=60)
    for engine in engines:
        engine.dispose()
    assert not errors, errors
    assert sorted(results.values()) == [[], [2]]
    assert len(calls) == 1
    assert counts(v1_store)["healer_schema_version"] == 2


def test_a_failed_migration_is_rolled_back(v1_store: str, monkeypatch: pytest.MonkeyPatch) -> None:
    engine = engine_of(v1_store)
    before = (schema_of(engine), snapshot(v1_store))
    real_plan = migrations._plan_v2
    monkeypatch.setattr(
        migrations,
        "_plan_v2",
        lambda conn, dialect: [*real_plan(conn, dialect), "SELECT * FROM no_such_table"],
    )
    with pytest.raises(ProgrammingError, match="no_such_table"):
        migrations.migrate(engine)
    assert (schema_of(engine), snapshot(v1_store)) == before  # transactional DDL: nothing left behind
    monkeypatch.setattr(migrations, "_plan_v2", real_plan)
    assert migrations.migrate(engine) == [2]


def test_web_processes_never_migrate(v1_store: str) -> None:
    engine = engine_of(v1_store)
    before = schema_of(engine)
    with pytest.raises(StoreSchemaOutdated, match="store schema v1"):
        db.init_db(engine)
    assert schema_of(engine) == before


def test_the_dry_run_prints_the_postgresql_sql(v1_store: str) -> None:
    engine = engine_of(v1_store)
    before = schema_of(engine)
    assert migrations.migrate(engine, dry_run=True) == [2]
    assert schema_of(engine) == before
    script = migrations.migration_script(engine)
    for expected in (
        "BEGIN;",
        "SELECT pg_advisory_xact_lock(hashtext('dag_healer_schema'));",
        "ALTER TABLE healer_incidents ADD COLUMN IF NOT EXISTS orchestrator VARCHAR(64) "
        "DEFAULT 'airflow' NOT NULL;",
        "ALTER TABLE healer_incidents DROP CONSTRAINT IF EXISTS uq_healer_incidents_ti;",
        "ALTER TABLE healer_incidents ADD CONSTRAINT uq_healer_incidents_key "
        "UNIQUE (orchestrator, dag_id, run_id, task_id, map_index, try_number);",
        "DROP INDEX IF EXISTS ix_healer_incidents_dag_created;",
        "DROP INDEX IF EXISTS ix_healer_incidents_signature;",
        "ALTER TABLE healer_contracts DROP CONSTRAINT IF EXISTS healer_contracts_pkey;",
        "ALTER TABLE healer_contracts ADD PRIMARY KEY (orchestrator, dag_id);",
        "COMMIT;",
    ):
        assert expected in script, expected
    assert any(line.startswith("CREATE TABLE IF NOT EXISTS healer_kv") for line in script)


# =========================================================================== a copy of a live store


@pytest.mark.skipif(not COPY_OF, reason="set HEALER_TEST_POSTGRES_COPY_OF=<database> to check a copy of it")
def test_a_copy_of_a_live_store_migrates_losslessly(admin: Engine, new_database: Callable[[], str]) -> None:
    source = make_url(ADMIN_URL).set(database=COPY_OF).render_as_string(hide_password=False)
    source_engine = create_engine(source, poolclass=NullPool)
    try:
        version = migrations.current_version(source_engine)
    finally:
        source_engine.dispose()
    if version != 1:
        pytest.skip(f"{COPY_OF} is at schema v{version}, not v1")
    rows = postgres_rows(source, read_only=True)
    url = new_database()
    load_v1_schema(url)
    insert_rows(url, rows)
    expected_counts = {table: len(values) for table, (_columns, values) in rows.items()}
    assert counts(url) == expected_counts
    old_columns = v1_columns(url)
    before = snapshot(url)
    with engine_of(url).connect() as conn:
        keys_before = conn.execute(
            text(
                "SELECT id, dag_id, run_id, task_id, map_index, try_number FROM healer_incidents ORDER BY id"
            )
        ).all()

    engine = engine_of(url)
    assert migrations.migrate(engine) == [2]
    assert migrations.migrate(engine) == []

    assert counts(url) == {
        **expected_counts,
        "healer_schema_version": expected_counts["healer_schema_version"] + 1,
        "healer_kv": 0,
    }
    assert snapshot(url, old_columns) == before
    with engine.connect() as conn:
        keys_after = conn.execute(
            text(
                "SELECT id, dag_id, run_id, task_id, map_index, try_number FROM healer_incidents "
                "WHERE orchestrator = 'airflow' ORDER BY id"
            )
        ).all()
    assert keys_after == keys_before
