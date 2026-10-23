"""Store schema v2: the migration runner, ``init_db(allow_migrate=...)`` and the v2 repository.

Section 11.4 of ``docs/architecture/orchestrators.md``. The frozen phase-5 store
``tests/unit/fixtures/store/healer_v1.db`` (schema v1, see ``test_store_v1_fixture.py``) is
copied and migrated; the migrated store must hold exactly the v1 rows, have the same layout
as a new v2 store, and accept the same attempt key under a second orchestrator instance.
PostgreSQL runs the same checks in ``tests/airflow/test_store_migration_pg.py``.
"""

from __future__ import annotations

import json
import logging
import pickle
import shutil
import sqlite3
import threading
from collections.abc import Iterator
from contextlib import closing
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

import pytest
from sqlalchemy import Engine, create_engine, inspect, select, text
from sqlalchemy.exc import OperationalError

from dag_healer.cli.main import main
from dag_healer.config import config_from_mapping
from dag_healer.core.failures import AttemptState, Failure
from dag_healer.core.ids import RUN_LEVEL_STEP, UNRESOLVED_STEP, AttemptRef
from dag_healer.store import db, migrations
from dag_healer.store import repo as store_repo
from dag_healer.store.db import StoreSchemaOutdated
from dag_healer.store.migrations import MigrationError
from dag_healer.store.models import (
    SCHEMA_VERSION,
    SOURCES,
    Base,
    Command,
    Incident,
    IncidentEvent,
    KeyValue,
    PendingEvent,
)

FIXTURES = Path(__file__).parent / "fixtures" / "store"
V1_DB = FIXTURES / "healer_v1.db"

V1_COUNTS = {
    "healer_incidents": 4,
    "healer_incident_events": 12,
    "healer_contracts": 3,
    "healer_alerts": 3,
    "healer_commands": 3,
    "healer_pending_events": 3,
    "healer_schema_version": 1,
}
#: The tables holding data (the version table gains a row when migrating).
DATA_TABLES = tuple(table for table in V1_COUNTS if table != "healer_schema_version")
ORDER = {"healer_contracts": "dag_id", "healer_schema_version": "version"}
V2_COLUMNS = {
    "healer_incidents": ("orchestrator", "native_ref", "error_name"),
    "healer_pending_events": ("orchestrator", "channel", "native_ref"),
    "healer_commands": ("orchestrator",),
    "healer_contracts": ("orchestrator",),
}
SALES_ID = "a1" * 16
CUSTOMERS_ID = "b2" * 16
MAPPED_ID = "d4" * 16
RUN = "manual__2026-10-17T18:28:46.093857+00:00"
T0 = datetime(2026, 10, 20, 9, 0, tzinfo=timezone.utc)
OUTDATED = "dag-healer store schema v1: start the worker or run 'dag-healer store migrate'"


@pytest.fixture(autouse=True)
def _fresh_engines() -> Iterator[None]:
    db.dispose_engines()
    yield
    db.dispose_engines()


@pytest.fixture
def v1_store(tmp_path: Path) -> Path:
    """A private copy of the frozen v1 store."""
    path = tmp_path / "store" / "healer.db"
    path.parent.mkdir()
    shutil.copyfile(V1_DB, path)
    return path


def url_of(path: Path) -> str:
    return f"sqlite:///{path.as_posix()}"


def engine_of(path: Path) -> Engine:
    """The store engine of ``path`` (``db.get_engine``: WAL, foreign keys on, busy timeout)."""
    return db.get_engine(url_of(path))


# =========================================================================== helpers


def table_columns(path: Path, table: str) -> list[str]:
    with closing(sqlite3.connect(path)) as conn:
        return [row[1] for row in conn.execute(f"PRAGMA table_info({table})")]


def dump(
    path: Path, columns: dict[str, list[str]] | None = None, tables: tuple[str, ...] = tuple(V1_COUNTS)
) -> dict[str, list[tuple[Any, ...]]]:
    """Rows of the v1 tables as stored (raw SQLite values), ordered by primary key."""
    result: dict[str, list[tuple[Any, ...]]] = {}
    with closing(sqlite3.connect(path)) as conn:
        for table in tables:
            names = (columns or {}).get(table) or [
                row[1] for row in conn.execute(f"PRAGMA table_info({table})")
            ]
            order = ORDER.get(table, "id")
            result[table] = conn.execute(
                f"SELECT {', '.join(names)} FROM {table} ORDER BY {order}"
            ).fetchall()
    return result


def v1_columns(path: Path) -> dict[str, list[str]]:
    return {table: table_columns(path, table) for table in V1_COUNTS}


def counts(path: Path) -> dict[str, int]:
    with closing(sqlite3.connect(path)) as conn:
        names = [row[0] for row in conn.execute("SELECT name FROM sqlite_master WHERE type = 'table'")]
        return {name: conn.execute(f"SELECT count(*) FROM {name}").fetchone()[0] for name in names}


def ddl_statements(path: Path) -> list[str]:
    with closing(sqlite3.connect(path)) as conn:
        return sorted(
            str(sql) for (sql,) in conn.execute("SELECT sql FROM sqlite_master WHERE sql IS NOT NULL")
        )


def _default(value: Any) -> str | None:
    return None if value is None else str(value).strip()


def schema_of(engine: Engine) -> dict[str, dict[str, Any]]:
    """A dialect-neutral description of the schema (columns in order, keys, indexes, FKs)."""
    insp = inspect(engine)
    result: dict[str, dict[str, Any]] = {}
    for table in sorted(insp.get_table_names()):
        result[table] = {
            "columns": [
                (
                    column["name"],
                    column["type"].compile(dialect=engine.dialect),
                    column["nullable"],
                    _default(column.get("default")),
                )
                for column in insp.get_columns(table)
            ],
            "pk": tuple(insp.get_pk_constraint(table)["constrained_columns"]),
            "unique": sorted(
                (item["name"], tuple(item["column_names"])) for item in insp.get_unique_constraints(table)
            ),
            "indexes": sorted(
                (item["name"], tuple(item["column_names"]), bool(item["unique"]))
                for item in insp.get_indexes(table)
            ),
            "fks": sorted(
                (
                    tuple(item["constrained_columns"]),
                    item["referred_table"],
                    tuple(item["referred_columns"]),
                    (item.get("options") or {}).get("ondelete"),
                )
                for item in insp.get_foreign_keys(table)
            ),
        }
    return result


def new_v2_store(path: Path) -> Engine:
    path.parent.mkdir(parents=True, exist_ok=True)
    engine = engine_of(path)
    db.init_db(engine)
    return engine


def version_rows(path: Path) -> list[int]:
    with closing(sqlite3.connect(path)) as conn:
        return [row[0] for row in conn.execute("SELECT version FROM healer_schema_version ORDER BY version")]


def raw_insert_incident(path: Path, source_id: str, **changes: Any) -> None:
    """Copy an incident row with raw SQL (no ORM), changing some columns."""
    with closing(sqlite3.connect(path)) as conn:
        conn.row_factory = sqlite3.Row
        row = dict(conn.execute("SELECT * FROM healer_incidents WHERE id = ?", (source_id,)).fetchone())
        row.update(changes)
        names = ", ".join(row)
        marks = ", ".join("?" for _ in row)
        with conn:
            conn.execute(f"INSERT INTO healer_incidents ({names}) VALUES ({marks})", list(row.values()))


# =========================================================================== a new store


def test_a_new_store_is_created_at_v2_with_every_version_row(tmp_path: Path) -> None:
    path = tmp_path / "new" / "healer.db"
    engine = new_v2_store(path)
    assert migrations.current_version(engine) == SCHEMA_VERSION == 2
    assert version_rows(path) == [1, 2]
    assert migrations.pending(engine) == []
    assert migrations.migrate(engine) == []
    schema = schema_of(engine)
    assert set(schema) == set(Base.metadata.tables)
    assert schema["healer_kv"]["pk"] == ("key",)
    assert schema["healer_incidents"]["unique"] == [
        (
            "uq_healer_incidents_key",
            ("orchestrator", "dag_id", "run_id", "task_id", "map_index", "try_number"),
        )
    ]
    assert {name for name, _cols, _unique in schema["healer_incidents"]["indexes"]} == {
        "ix_healer_incidents_status",
        "ix_healer_incidents_orch_dag_created",
        "ix_healer_incidents_signature",
    }
    assert schema["healer_contracts"]["pk"] == ("orchestrator", "dag_id")
    for table, added in V2_COLUMNS.items():
        assert [column[0] for column in schema[table]["columns"]][-len(added) :] == list(added), table
    assert not (path.parent / "healer.db.bak-v1").exists()


def test_an_empty_store_needs_no_migration(tmp_path: Path) -> None:
    engine = engine_of(tmp_path / "empty.db")
    assert migrations.current_version(engine) == 0
    assert migrations.pending(engine) == []
    assert migrations.migrate(engine) == []
    assert inspect(engine).get_table_names() == []
    script = migrations.migration_script(engine)
    assert script[0].startswith("-- the store is empty")
    assert any(line.startswith("CREATE TABLE IF NOT EXISTS healer_incidents") for line in script)


# =========================================================================== migrating the v1 store


def test_the_v1_store_migrates_losslessly(v1_store: Path) -> None:
    old_columns = v1_columns(v1_store)
    before = dump(v1_store, tables=DATA_TABLES)
    version_1 = dump(v1_store, tables=("healer_schema_version",))["healer_schema_version"]
    engine = engine_of(v1_store)
    assert migrations.current_version(engine) == 1
    assert [migration.version for migration in migrations.pending(engine)] == [2]

    assert migrations.migrate(engine) == [2]

    assert migrations.current_version(engine) == 2
    assert version_rows(v1_store) == [1, 2]
    assert counts(v1_store) == {**V1_COUNTS, "healer_schema_version": 2, "healer_kv": 0}
    assert dump(v1_store, old_columns, DATA_TABLES) == before  # every v1 value, unchanged
    assert dump(v1_store, tables=("healer_schema_version",))["healer_schema_version"][:1] == version_1
    with closing(sqlite3.connect(v1_store)) as conn:
        assert conn.execute("PRAGMA integrity_check").fetchall() == [("ok",)]
        assert conn.execute("PRAGMA foreign_key_check").fetchall() == []
        for table, added in V2_COLUMNS.items():
            assert table_columns(v1_store, table) == [*old_columns[table], *added], table
            assert conn.execute(f"SELECT DISTINCT orchestrator FROM {table}").fetchall() == [("airflow",)]
        assert conn.execute("SELECT DISTINCT channel FROM healer_pending_events").fetchall() == [
            ("task-failed",)
        ]
        assert conn.execute(
            "SELECT count(*) FROM healer_incidents WHERE native_ref IS NULL AND error_name IS NULL"
        ).fetchone() == (4,)
        assert conn.execute(
            "SELECT count(*) FROM healer_pending_events WHERE native_ref IS NULL"
        ).fetchone() == (3,)
    # the engine's connections enforce foreign keys again
    with engine.connect() as conn:
        assert conn.execute(text("PRAGMA foreign_keys")).scalar() == 1


def test_the_migrated_store_has_the_layout_of_a_new_v2_store(v1_store: Path, tmp_path: Path) -> None:
    migrated = engine_of(v1_store)
    migrations.migrate(migrated)
    fresh = new_v2_store(tmp_path / "fresh" / "healer.db")
    assert schema_of(migrated) == schema_of(fresh)


def test_the_second_run_is_a_no_op_and_the_backup_is_the_v1_store(v1_store: Path) -> None:
    engine = engine_of(v1_store)
    assert migrations.migrate(engine) == [2]
    after_first = (ddl_statements(v1_store), dump(v1_store))

    assert migrations.migrate(engine) == []
    db.init_db(engine, allow_migrate=True)
    assert (ddl_statements(v1_store), dump(v1_store)) == after_first

    backup = v1_store.with_name("healer.db.bak-v1")
    assert backup.is_file()
    assert ddl_statements(backup) == ddl_statements(V1_DB)
    assert dump(backup) == dump(V1_DB)
    with closing(sqlite3.connect(backup)) as conn:
        assert conn.execute("PRAGMA journal_mode").fetchone() == ("delete",)  # one self-contained file
    assert not backup.with_name(backup.name + ".tmp").exists()


def test_the_key_now_includes_the_orchestrator(v1_store: Path) -> None:
    migrations.migrate(engine_of(v1_store))
    # the same (dag, run, task, map_index, try) under another instance is a new incident ...
    raw_insert_incident(v1_store, SALES_ID, id="e5" * 16, orchestrator="sfn-x")
    # ... while a duplicate under "airflow" is still rejected
    with pytest.raises(sqlite3.IntegrityError, match="UNIQUE"):
        raw_insert_incident(v1_store, SALES_ID, id="f6" * 16)

    with db.session_scope(url_of(v1_store)) as session:
        key = {"dag_id": "sales_daily_etl", "run_id": RUN, "task_id": "transform_sales", "try_number": 1}
        existing, created = store_repo.create_incident(session, **key, source="rest")
        assert (existing.id, created) == (SALES_ID, False)
        other = store_repo.get_incident_by_key(session, **key, orchestrator="sfn-x")
        assert other is not None and other.id == "e5" * 16
        third, created = store_repo.create_incident(session, **key, source="poll", orchestrator="sfn-y")
        assert created and third.orchestrator == "sfn-y"


def test_foreign_keys_follow_the_rebuilt_table(v1_store: Path) -> None:
    migrations.migrate(engine_of(v1_store))
    with closing(sqlite3.connect(v1_store)) as conn:
        for child in ("healer_incident_events", "healer_alerts", "healer_commands"):
            targets = {row[2] for row in conn.execute(f"PRAGMA foreign_key_list({child})")}
            assert targets == {"healer_incidents"}, child
    with db.session_scope(url_of(v1_store)) as session:
        session.delete(session.get(Incident, CUSTOMERS_ID))
    remaining = counts(v1_store)
    # the customers incident had 4 events, 1 alert and 1 command: ON DELETE CASCADE still works
    assert remaining["healer_incidents"] == 3
    assert remaining["healer_incident_events"] == 8
    assert remaining["healer_alerts"] == 2
    assert remaining["healer_commands"] == 2


def test_the_store_code_reads_and_writes_the_migrated_store(v1_store: Path) -> None:
    migrations.migrate(engine_of(v1_store))
    with db.session_scope(url_of(v1_store)) as session:
        incidents = {incident.id: incident for incident in store_repo.list_incidents(session)}
        assert len(incidents) == 4
        assert store_repo.list_incidents(session, orchestrator="airflow") == list(incidents.values())
        assert store_repo.list_incidents(session, orchestrator="sfn-x") == []
        mapped = incidents[MAPPED_ID].to_dict()
        assert (mapped["orchestrator"], mapped["native_ref"], mapped["error_name"]) == ("airflow", None, None)
        assert AttemptRef.from_row(mapped) == AttemptRef(
            "airflow", "inventory_report", "scheduled__2026-10-17T00:00:00+00:00", "publish_report", 3, 2
        )
        assert [command.id for command in store_repo.pending_commands(session, orchestrator="airflow")] == [2]
        [pending] = store_repo.list_pending_events(session, orchestrator="airflow")
        assert (pending.id, pending.channel, pending.map_index) == (3, "task-failed", 4)
        state = store_repo.get_contract_state(session, "sales_daily_etl")
        assert state is not None and state.status == "approved" and state.orchestrator == "airflow"
        store_repo.upsert_contract_state(session, dag_id="sales_daily_etl", status="draft", path="x.yaml")
        assert store_repo.kv_get(session, store_repo.cursor_key("airflow")) is None
        store_repo.kv_set(session, store_repo.cursor_key("airflow"), {"watermark": T0.isoformat()})
    with db.session_scope(url_of(v1_store)) as session:
        assert store_repo.get_contract_state(session, "sales_daily_etl").status == "draft"
        assert len(store_repo.list_contract_states(session)) == 3
        assert store_repo.kv_get(session, "cursor:airflow") == {"watermark": T0.isoformat()}


# =========================================================================== locking and failures


def test_concurrent_migrations_apply_each_version_once(
    v1_store: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    calls: list[str] = []
    original = migrations.MIGRATIONS[0]

    def slow_upgrade(conn: Any, dialect: str) -> None:
        calls.append(threading.current_thread().name)
        threading.Event().wait(0.3)  # keep the lock while the other engine tries to migrate
        original.upgrade(conn, dialect)

    monkeypatch.setattr(
        migrations,
        "MIGRATIONS",
        (migrations.Migration(original.version, original.description, slow_upgrade, original.plan),),
    )
    engines = [
        create_engine(url_of(v1_store), connect_args={"check_same_thread": False, "timeout": 30})
        for _ in range(2)
    ]
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
    assert version_rows(v1_store) == [1, 2]
    assert counts(v1_store)["healer_incidents"] == 4


def test_a_failed_migration_is_rolled_back(v1_store: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    before = (ddl_statements(v1_store), dump(v1_store))
    real_plan = migrations._plan_v2

    def broken_plan(conn: Any, dialect: str) -> list[str]:
        return [*real_plan(conn, dialect), "INSERT INTO no_such_table VALUES (1)"]

    monkeypatch.setattr(migrations, "_plan_v2", broken_plan)
    engine = engine_of(v1_store)
    with pytest.raises(OperationalError, match="no_such_table"):
        migrations.migrate(engine)
    assert (ddl_statements(v1_store), dump(v1_store)) == before
    assert migrations.current_version(engine) == 1
    with engine.connect() as conn:  # the pooled connection got its foreign keys back
        assert conn.execute(text("PRAGMA foreign_keys")).scalar() == 1

    monkeypatch.setattr(migrations, "_plan_v2", real_plan)
    assert migrations.migrate(engine) == [2]
    assert counts(v1_store)["healer_incident_events"] == 12


def test_a_half_applied_migration_can_be_repeated(v1_store: Path, tmp_path: Path) -> None:
    before = dump(v1_store, tables=DATA_TABLES)
    old_columns = v1_columns(v1_store)
    with closing(sqlite3.connect(v1_store)) as conn, conn:
        # an interrupted earlier attempt outside a transaction: some columns, healer_kv, a temp table
        conn.execute(
            "ALTER TABLE healer_pending_events ADD COLUMN orchestrator VARCHAR(64) DEFAULT 'airflow' NOT NULL"
        )
        conn.execute(
            "ALTER TABLE healer_commands ADD COLUMN orchestrator VARCHAR(64) DEFAULT 'airflow' NOT NULL"
        )
        conn.execute("CREATE TABLE healer_incidents__v2 (id VARCHAR(32) NOT NULL PRIMARY KEY)")
        conn.execute(
            'CREATE TABLE healer_kv ("key" VARCHAR(250) NOT NULL, value JSON, updated_at DATETIME NOT NULL, '
            'PRIMARY KEY ("key"))'
        )
    engine = engine_of(v1_store)
    assert migrations.migrate(engine) == [2]
    assert dump(v1_store, old_columns, DATA_TABLES) == before
    assert schema_of(engine) == schema_of(new_v2_store(tmp_path / "fresh" / "healer.db"))


def test_a_rebuild_never_drops_an_unknown_column(v1_store: Path) -> None:
    with closing(sqlite3.connect(v1_store)) as conn, conn:
        conn.execute("ALTER TABLE healer_incidents ADD COLUMN ticket TEXT")
    before = ddl_statements(v1_store)
    with pytest.raises(MigrationError, match="ticket"):
        migrations.migrate(engine_of(v1_store))
    assert ddl_statements(v1_store) == before


def test_a_foreign_key_violation_stops_the_migration(v1_store: Path) -> None:
    with closing(sqlite3.connect(v1_store)) as conn, conn:  # foreign keys are off on this connection
        conn.execute(
            "INSERT INTO healer_incident_events (id, incident_id, at, kind, message, actor) "
            "VALUES (99, 'gone', '2026-10-17 18:30:00.000000', 'note', 'orphan', 'x')"
        )
    before = (ddl_statements(v1_store), dump(v1_store))
    with pytest.raises(MigrationError, match="foreign key check failed"):
        migrations.migrate(engine_of(v1_store))
    assert (ddl_statements(v1_store), dump(v1_store)) == before


def test_the_dry_run_prints_the_sql_and_changes_nothing(v1_store: Path) -> None:
    before = ddl_statements(v1_store)
    engine = engine_of(v1_store)
    assert migrations.migrate(engine, dry_run=True) == [2]
    assert ddl_statements(v1_store) == before
    assert not v1_store.with_name("healer.db.bak-v1").exists()
    script = migrations.migration_script(engine)
    assert script[0] == "-- dag-healer store schema v1 -> v2"
    for expected in (
        "PRAGMA foreign_keys=OFF;",
        "BEGIN IMMEDIATE;",
        "ALTER TABLE healer_pending_events ADD COLUMN orchestrator VARCHAR(64) DEFAULT 'airflow' NOT NULL;",
        "ALTER TABLE healer_pending_events ADD COLUMN channel VARCHAR(32) DEFAULT 'task-failed' NOT NULL;",
        "DROP TABLE healer_incidents;",
        "ALTER TABLE healer_incidents__v2 RENAME TO healer_incidents;",
        "ALTER TABLE healer_contracts__v2 RENAME TO healer_contracts;",
        "PRAGMA foreign_key_check;",
        "COMMIT;",
    ):
        assert expected in script, expected
    assert any(line.startswith("CREATE TABLE healer_incidents__v2") for line in script)
    migrations.migrate(engine)
    assert migrations.migration_script(engine) == ["-- the store is at schema v2: nothing to migrate"]


def test_the_models_are_not_changed_by_planning(v1_store: Path, tmp_path: Path) -> None:
    """Compiling ADD CONSTRAINT must not drop the constraint from later CREATE TABLEs."""
    engine = engine_of(v1_store)
    with engine.connect() as conn:
        migrations._plan_v2_postgresql(conn)  # builds AddConstraint from table copies
    fresh = new_v2_store(tmp_path / "fresh" / "healer.db")
    assert schema_of(fresh)["healer_incidents"]["unique"] == [
        (
            "uq_healer_incidents_key",
            ("orchestrator", "dag_id", "run_id", "task_id", "map_index", "try_number"),
        )
    ]
    assert schema_of(fresh)["healer_contracts"]["pk"] == ("orchestrator", "dag_id")


# =========================================================================== who migrates


def test_init_db_refuses_to_migrate_unless_allowed(v1_store: Path) -> None:
    before = ddl_statements(v1_store)
    engine = engine_of(v1_store)
    with pytest.raises(StoreSchemaOutdated) as caught:
        db.init_db(engine)
    assert str(caught.value) == OUTDATED
    assert (caught.value.current, caught.value.required) == (1, 2)
    for _attempt in range(2):  # not cached: every call checks again
        with pytest.raises(StoreSchemaOutdated):
            db.ensure_initialized(engine)
    with pytest.raises(StoreSchemaOutdated), db.session_scope(url_of(v1_store)):
        pass  # pragma: no cover - the scope does not open
    assert ddl_statements(v1_store) == before
    assert not v1_store.with_name("healer.db.bak-v1").exists()

    db.init_db(engine, allow_migrate=True)  # what the worker does
    assert migrations.current_version(engine) == 2
    assert v1_store.with_name("healer.db.bak-v1").is_file()
    with db.session_scope(url_of(v1_store)) as session:  # web processes recover without a restart
        assert len(store_repo.list_incidents(session)) == 4


def test_store_schema_outdated_pickles() -> None:
    error = pickle.loads(pickle.dumps(StoreSchemaOutdated(1)))
    assert isinstance(error, StoreSchemaOutdated)
    assert (error.current, error.required, str(error)) == (1, SCHEMA_VERSION, OUTDATED)


def test_a_newer_schema_is_left_alone(tmp_path: Path, caplog: pytest.LogCaptureFixture) -> None:
    path = tmp_path / "newer" / "healer.db"
    new_v2_store(path)
    with closing(sqlite3.connect(path)) as conn, conn:
        conn.execute("INSERT INTO healer_schema_version VALUES (3, '2027-01-01 00:00:00.000000')")
    db.dispose_engines()
    engine = engine_of(path)
    with caplog.at_level(logging.WARNING, logger="dag_healer.store.db"):
        db.init_db(engine)
    assert "newer than this dag-healer" in caplog.text
    assert migrations.current_version(engine) == 3
    assert migrations.pending(engine) == []
    assert migrations.migrate(engine) == []


def test_migrate_rejects_an_unknown_target(v1_store: Path) -> None:
    with pytest.raises(ValueError, match="target"):
        migrations.migrate(engine_of(v1_store), target=SCHEMA_VERSION + 1)
    assert migrations.migrate(engine_of(v1_store), target=1) == []


# =========================================================================== the v2 repository


@pytest.fixture
def session(tmp_path: Path) -> Iterator[Any]:
    with db.session_scope(url_of(tmp_path / "v2" / "healer.db")) as s:
        yield s


def test_the_same_attempt_under_two_instances_is_two_incidents(session: Any) -> None:
    key = {"dag_id": "etl", "run_id": "r1", "task_id": "t", "try_number": 1}
    airflow, created = store_repo.create_incident(session, **key, source="rest", signature="sig", now=T0)
    assert created and airflow.orchestrator == "airflow"
    sfn, created = store_repo.create_incident(
        session, **key, source="poll", signature="sig", now=T0, orchestrator="sfn-prod"
    )
    assert created and sfn.id != airflow.id  # the signature dedupe never crosses instances
    assert store_repo.get_incident_by_key(session, **key).id == airflow.id
    assert store_repo.get_incident_by_key(session, **key, orchestrator="sfn-prod").id == sfn.id
    folded, created = store_repo.create_incident(
        session, **{**key, "run_id": "r2"}, source="poll", signature="sig", now=T0, orchestrator="sfn-prod"
    )
    assert (folded.id, created, folded.occurrences, airflow.occurrences) == (sfn.id, False, 2, 1)
    assert [i.id for i in store_repo.list_incidents(session, orchestrator="sfn-prod")] == [sfn.id]
    assert store_repo.incidents_created_since(session, T0) == 2
    assert store_repo.incidents_created_since(session, T0, orchestrator="sfn-prod") == 1

    claimed = store_repo.claim_next(session, now=T0, orchestrator="sfn-prod")
    assert claimed is not None and claimed.id == sfn.id
    assert store_repo.claim_next(session, now=T0, orchestrator="sfn-prod") is None
    assert (
        store_repo.release_stale_claims(
            session, older_than=timedelta(minutes=1), now=T0 + timedelta(hours=1), orchestrator="airflow"
        )
        == []
    )
    released = store_repo.release_stale_claims(
        session, older_than=timedelta(minutes=1), now=T0 + timedelta(hours=1), orchestrator="sfn-prod"
    )
    assert [i.id for i in released] == [sfn.id]


def test_timeline_events_keep_the_phase5_key_shape(session: Any) -> None:
    incident, _ = store_repo.create_incident(
        session, dag_id="d", run_id="r", task_id="t", try_number=1, source="rest", signature="s", now=T0
    )
    store_repo.create_incident(
        session, dag_id="d", run_id="r2", task_id="t", try_number=1, source="rest", signature="s", now=T0
    )
    created, occurrence = store_repo.list_events(session, incident.id)
    assert created.data == {
        "dag_id": "d",
        "run_id": "r",
        "task_id": "t",
        "map_index": -1,
        "try_number": 1,
        "source": "rest",
    }
    assert occurrence.kind == "occurrence" and "orchestrator" not in occurrence.data


def test_create_incident_from_failure_masks_and_keeps_native(session: Any) -> None:
    arn = "arn:aws:states:eu-west-1:123456789012:execution:etl:run-1"
    failure = Failure(
        ref=AttemptRef("sfn-prod", "etl", "run-1", "Transform", -1, 2),
        state=AttemptState.FAILED,
        is_final=True,
        source="poll",
        error_name="KeyError",
        error_summary="KeyError: 'amount' (row of alice@example.com)",
        native={"execution_arn": arn, "redrive_count": 1},
    )
    incident, created = store_repo.create_incident_from_failure(session, failure, signature="sig", now=T0)
    assert created
    assert (incident.orchestrator, incident.dag_id, incident.task_id, incident.try_number) == (
        "sfn-prod",
        "etl",
        "Transform",
        2,
    )
    assert (incident.pipeline_id, incident.step_id, incident.attempt) == ("etl", "Transform", 2)
    assert incident.source == "poll" and incident.error_name == "KeyError"
    assert incident.error_summary == "KeyError: 'amount' (row of [email])"
    assert incident.native_ref == {"execution_arn": arn, "redrive_count": 1}  # pointers are not masked
    again, created = store_repo.create_incident_from_failure(session, failure, signature="sig")
    assert (again.id, created) == (incident.id, False)

    run_level = Failure(
        ref=AttemptRef("sfn-prod", "etl", "run-2", RUN_LEVEL_STEP),
        state=AttemptState.FAILED,
        is_final=True,
        source="sqs",
        error_name="States.Timeout " + "x" * 400,
    )
    timeout, created = store_repo.create_incident_from_failure(session, run_level, signature=None)
    assert created and timeout.task_id == RUN_LEVEL_STEP and timeout.native_ref is None
    assert len(timeout.error_name) == store_repo.MAX_ERROR_NAME_CHARS
    unresolved = Failure(
        ref=AttemptRef("sfn-prod", "etl", "run-3", UNRESOLVED_STEP),
        state=AttemptState.FAILED,
        is_final=True,
        source="poll",
    )
    with pytest.raises(ValueError, match="resolved step"):
        store_repo.create_incident_from_failure(session, unresolved, signature=None)


def test_sources_include_the_neutral_names() -> None:
    from dag_healer.orchestrators.conformance import STORE_SOURCES

    assert SOURCES == STORE_SOURCES
    assert all(len(source) <= Incident.__table__.c.source.type.length for source in SOURCES)


def test_pending_events_carry_the_instance_and_the_channel(session: Any) -> None:
    hint = store_repo.add_pending_event(
        session,
        dag_id="etl",
        run_id="run-1",
        task_id=UNRESOLVED_STEP,
        try_number=1,
        orchestrator="sfn-prod",
        channel="eventbridge",
        native_ref={"execution_arn": "arn:x"},
        payload={"error": "States.TaskFailed", "source": "eventbridge"},
    )
    assert (hint.orchestrator, hint.channel, hint.task_id, hint.step_id) == (
        "sfn-prod",
        "eventbridge",
        "",
        "",
    )
    assert hint.native_ref == {"execution_arn": "arn:x"}
    again = store_repo.add_pending_event(
        session,
        dag_id="etl",
        run_id="run-1",
        task_id="",
        try_number=1,
        orchestrator="sfn-prod",
        channel="sqs",
    )
    assert again.id == hint.id  # one pending row per attempt and instance, whatever the channel
    airflow = store_repo.add_pending_event(session, dag_id="etl", run_id="run-1", task_id="t", try_number=1)
    assert (airflow.orchestrator, airflow.channel, airflow.native_ref) == ("airflow", "task-failed", None)
    assert [e.id for e in store_repo.list_pending_events(session, orchestrator="sfn-prod")] == [hint.id]
    assert [e.id for e in store_repo.list_pending_events(session, orchestrator="airflow")] == [airflow.id]
    assert len(store_repo.list_pending_events(session)) == 2
    with pytest.raises(ValueError, match="task_id"):
        store_repo.add_pending_event(session, dag_id="etl", run_id="run-1", task_id=None)


def test_commands_and_contract_states_belong_to_an_instance(session: Any) -> None:
    first = store_repo.enqueue_command(session, kind="resolve", dag_id="etl", actor="ann")
    second = store_repo.enqueue_command(
        session, kind="retry_task", dag_id="etl", actor="bob", orchestrator="sfn"
    )
    assert (first.orchestrator, second.orchestrator) == ("airflow", "sfn")
    assert [c.id for c in store_repo.pending_commands(session, orchestrator="sfn")] == [second.id]
    assert [c.id for c in store_repo.pending_commands(session)] == [first.id, second.id]

    store_repo.upsert_contract_state(session, dag_id="etl", status="approved", path="a.yaml")
    store_repo.upsert_contract_state(session, dag_id="etl", status="draft", path="b.yaml", orchestrator="sfn")
    assert store_repo.get_contract_state(session, "etl").status == "approved"
    assert store_repo.get_contract_state(session, "etl", orchestrator="sfn").status == "draft"
    assert [(s.orchestrator, s.dag_id) for s in store_repo.list_contract_states(session)] == [
        ("airflow", "etl"),
        ("sfn", "etl"),
    ]
    assert [s.path for s in store_repo.list_contract_states(session, orchestrator="sfn")] == ["b.yaml"]


def test_orm_synonyms_and_to_dict(session: Any) -> None:
    incident, _ = store_repo.create_incident(
        session, dag_id="etl", run_id="r", task_id="load", map_index=2, try_number=3, source="cli"
    )
    session.commit()
    found = session.scalars(
        select(Incident).where(
            Incident.pipeline_id == "etl", Incident.step_id == "load", Incident.attempt == 3
        )
    ).one()
    assert found is incident
    data = incident.to_dict()
    assert {"orchestrator", "native_ref", "error_name"} <= set(data)
    assert not {"pipeline_id", "step_id", "attempt"} & set(data)
    assert data["attempts"] == 0 and data["try_number"] == 3  # "attempts" stays the repair counter
    event = store_repo.add_pending_event(session, dag_id="etl", run_id="r", task_id="load", try_number=3)
    assert (event.pipeline_id, event.step_id, event.attempt) == ("etl", "load", 3)
    assert {"orchestrator", "channel", "native_ref"} <= set(event.to_dict())
    command = store_repo.enqueue_command(session, kind="resolve", dag_id="etl", actor="x")
    assert session.get(Command, command.id).to_dict()["orchestrator"] == "airflow"
    created = session.scalars(select(IncidentEvent).where(IncidentEvent.incident_id == incident.id)).one()
    assert created.kind == "created" and "orchestrator" not in created.data


# =========================================================================== healer_kv and state.json


def test_kv_round_trip(session: Any) -> None:
    assert store_repo.kv_get(session, "cursor:airflow") is None
    store_repo.kv_set(session, "cursor:airflow", {"watermark": T0, "pages": (1, 2)})
    assert store_repo.kv_get(session, "cursor:airflow") == {"watermark": T0.isoformat(), "pages": [1, 2]}
    store_repo.kv_set(session, "cursor:airflow", {"watermark": "later"})
    store_repo.kv_set(session, "cursor:sfn_1", {"arn": 1})
    store_repo.kv_set(session, "cursorXsfn", 1)
    assert store_repo.kv_items(session, "cursor:") == {
        "cursor:airflow": {"watermark": "later"},
        "cursor:sfn_1": {"arn": 1},
    }
    assert store_repo.kv_items(session, "cursor_") == {}  # "_" is not a wildcard
    cached = session.get(KeyValue, "cursor:airflow")
    store_repo.kv_set(session, "cursor:airflow", {"watermark": "latest"})
    assert cached.value == {"watermark": "latest"}  # ORM objects never go stale
    assert store_repo.kv_delete(session, "cursor:airflow") is True
    assert store_repo.kv_delete(session, "cursor:airflow") is False
    assert store_repo.kv_get(session, "cursor:airflow") is None
    with pytest.raises(ValueError):
        store_repo.kv_set(session, "", 1)
    assert store_repo.cursor_key("") == "cursor:airflow"


def test_import_state_file_imports_the_phase5_poll_position_once(
    session: Any, tmp_path: Path, healer_env: Path
) -> None:
    from dag_healer.worker import pipeline

    cfg = config_from_mapping({"repo": {"path": str(tmp_path)}})
    pipeline.reset_state(cfg, T0)  # what the phase-5 `store reset` wrote
    pipeline.set_watermark(cfg, T0 + timedelta(minutes=5))  # ... and a later poll
    path = pipeline.state_path(cfg)

    cursor = store_repo.import_state_file(session, path)
    assert cursor == {"watermark": (T0 + timedelta(minutes=5)).isoformat(), "not_before": T0.isoformat()}
    assert store_repo.kv_get(session, "cursor:airflow") == cursor
    pipeline.set_watermark(cfg, T0 + timedelta(hours=1))
    assert store_repo.import_state_file(session, path) is None  # one time only: the cursor wins
    assert store_repo.kv_get(session, "cursor:airflow") == cursor
    assert path.is_file()

    assert store_repo.import_state_file(session, tmp_path / "missing.json", instance="other") is None
    odd = tmp_path / "odd.json"
    for content in ("{not json", "[1, 2]", json.dumps({"something": "else"})):
        odd.write_text(content, encoding="utf-8")
        assert store_repo.import_state_file(session, odd, instance="other") is None
    odd.write_text(
        json.dumps({"rest_watermark": "yesterday", "rest_not_before": "2026-10-20T09:00:00Z"}), "utf-8"
    )
    assert store_repo.import_state_file(session, odd, instance="other") == {
        "watermark": None,
        "not_before": T0.isoformat(),
    }
    assert set(store_repo.kv_items(session, "cursor:")) == {"cursor:airflow", "cursor:other"}


# =========================================================================== the CLI


@pytest.fixture
def config_for(tmp_path: Path, healer_env: Path) -> Any:
    def make(store: Path) -> Path:
        repo = tmp_path / "repo"
        (repo / "dags").mkdir(parents=True, exist_ok=True)
        path = repo / "healer.yaml"
        path.write_text(f"version: 1\nstore:\n  url: {url_of(store)}\n", encoding="utf-8")
        return path

    return make


def test_cli_status_and_migrate_a_v1_store(
    v1_store: Path, config_for: Any, capsys: pytest.CaptureFixture[str]
) -> None:
    config = str(config_for(v1_store))
    assert main(["-c", config, "store", "status"]) == 0
    out = capsys.readouterr().out
    assert "Schema: v1 (this dag-healer uses v2)" in out
    assert "  v2  orchestrator-aware keys" in out
    assert "healer.db.bak-v1" in out

    assert main(["-c", config, "store", "stats"]) == 1  # the store code needs v2 first
    assert "store migrate" in capsys.readouterr().err

    before = ddl_statements(v1_store)
    assert main(["-c", config, "store", "migrate", "--dry-run"]) == 0
    out = capsys.readouterr().out
    assert "dry run: nothing was changed" in out and "BEGIN IMMEDIATE;" in out
    assert ddl_statements(v1_store) == before

    assert main(["-c", config, "store", "migrate"]) == 0
    out = capsys.readouterr().out
    assert "migrated from schema v1 to v2 (applied v2)" in out
    assert "healer.db.bak-v1" in out
    assert main(["-c", config, "store", "migrate"]) == 0
    assert "already at schema v2: nothing to migrate" in capsys.readouterr().out
    assert main(["-c", config, "store", "status"]) == 0
    out = capsys.readouterr().out
    assert "Schema: v2 (up to date)" in out and "Pending" not in out
    assert main(["-c", config, "store", "stats"]) == 0
    assert "healer_kv" in capsys.readouterr().out


def test_cli_migrate_creates_an_empty_store(
    tmp_path: Path, config_for: Any, capsys: pytest.CaptureFixture[str]
) -> None:
    store = tmp_path / "new" / "healer.db"
    config = str(config_for(store))
    assert main(["-c", config, "store", "status"]) == 0
    assert "Schema: none yet (empty store)" in capsys.readouterr().out
    assert main(["-c", config, "store", "migrate", "--dry-run"]) == 0
    assert "CREATE TABLE IF NOT EXISTS healer_kv" in capsys.readouterr().out
    assert main(["-c", config, "store", "migrate"]) == 0
    assert "created at schema v2" in capsys.readouterr().out
    assert version_rows(store) == [1, 2]


def test_cli_migrate_reports_a_failure(
    v1_store: Path, config_for: Any, capsys: pytest.CaptureFixture[str]
) -> None:
    with closing(sqlite3.connect(v1_store)) as conn, conn:
        conn.execute("ALTER TABLE healer_contracts ADD COLUMN owner TEXT")
    assert main(["-c", str(config_for(v1_store)), "store", "migrate"]) == 1
    err = capsys.readouterr().err
    assert "MigrationError" in err and "owner" in err
    assert migrations.current_version(engine_of(v1_store)) == 1


def test_cli_reset_clears_the_poll_cursors_unless_kept(
    tmp_path: Path, config_for: Any, capsys: pytest.CaptureFixture[str]
) -> None:
    store = tmp_path / "kv" / "healer.db"
    config = str(config_for(store))
    with db.session_scope(url_of(store)) as session:
        store_repo.kv_set(session, "cursor:airflow", {"watermark": T0.isoformat()})
        store_repo.create_incident(session, dag_id="d", run_id="r", task_id="t", try_number=1, source="rest")

    assert main(["-c", config, "store", "reset", "--yes", "--keep-state"]) == 0
    assert "reset: 2 row(s) deleted" in capsys.readouterr().out  # the incident and its event
    assert counts(store)["healer_kv"] == 1

    assert main(["-c", config, "store", "reset", "--yes"]) == 0
    assert "healer_kv: 1" in capsys.readouterr().out
    assert counts(store)["healer_kv"] == 0
    assert set(counts(store).items()) >= {("healer_incidents", 0), ("healer_schema_version", 2)}


def test_pending_event_columns_match_the_ingest_payload_contract() -> None:
    """The ingest route passes payload keys that must never collide with the v2 columns."""
    from dag_healer.api.schemas import TaskFailedEvent

    v2 = {"orchestrator", "channel", "native_ref"}
    assert not v2 & set(TaskFailedEvent.model_fields)
    assert v2 <= set(PendingEvent.__table__.columns.keys())
