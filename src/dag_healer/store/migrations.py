"""Version-gated migrations of the dag-healer store schema (no Alembic).

The store records every applied schema version in ``healer_schema_version``. A new store is
created at the latest version directly (:func:`dag_healer.store.db.init_db`); an existing
store below :data:`~dag_healer.store.models.SCHEMA_VERSION` is upgraded by :func:`migrate`,
which applies the pending :data:`MIGRATIONS` in order, in one transaction, under a lock:

* **PostgreSQL:** ``SELECT pg_advisory_xact_lock(hashtext('dag_healer_schema'))`` at the start
  of the transaction (DDL is transactional, so a failed run leaves nothing behind);
* **SQLite:** an ``AUTOCOMMIT`` connection, ``PRAGMA foreign_keys=OFF`` *outside* the
  transaction, then ``BEGIN IMMEDIATE`` (the write lock); the previous ``foreign_keys``
  setting is restored after ``COMMIT``. Before the first table rebuild the database file is
  copied to ``<db>.bak-v<version>`` with SQLite's online backup API.

Every migration re-reads :func:`current_version` after taking the lock, and every DDL step is
derived from a fresh :func:`sqlalchemy.inspect` of the database (does the column, index or
constraint exist already?), so a half-applied or concurrent run is harmless: two processes
calling :func:`migrate` apply each version exactly once.

Migration v2 (section 6 of ``docs/architecture/orchestrators.md``) adds the orchestrator
instance id to the incident key: ``healer_incidents``, ``healer_pending_events``,
``healer_commands`` and ``healer_contracts`` get an ``orchestrator`` column (``'airflow'``
for existing rows), the incident unique key and two indexes are replaced, the contract
cache is keyed by ``(orchestrator, dag_id)`` and ``healer_kv`` is created. PostgreSQL uses
``ALTER TABLE``; SQLite cannot drop an inline ``UNIQUE`` or a primary key, so it rebuilds
``healer_incidents`` and ``healer_contracts`` with SQLite's documented procedure (create the
new table, copy the rows, drop the old table, rename the new one, recreate the indexes,
``PRAGMA foreign_key_check``). Row values are copied unchanged.
"""

from __future__ import annotations

import logging
import os
import sqlite3
from collections.abc import Callable, Iterator
from contextlib import contextmanager
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Any

from sqlalchemy import Engine, MetaData, Table, UniqueConstraint, inspect, select, text
from sqlalchemy.engine import Connection
from sqlalchemy.engine.reflection import Inspector
from sqlalchemy.schema import AddConstraint, CreateColumn, CreateIndex, CreateTable

from dag_healer.store.models import (
    SCHEMA_VERSION,
    Base,
    Command,
    ContractState,
    Incident,
    PendingEvent,
    SchemaVersion,
    ensure_utc,
    utcnow,
)

logger = logging.getLogger(__name__)

#: Name hashed into the PostgreSQL advisory lock that serialises schema changes.
SCHEMA_LOCK = "dag_healer_schema"
#: Suffix of the SQLite copy taken before a migration (``healer.db.bak-v1``).
BACKUP_SUFFIX = ".bak-v{version}"
#: Suffix of the temporary table of an SQLite table rebuild.
REBUILD_SUFFIX = "__v2"
#: Dialects the migrations support.
SUPPORTED_DIALECTS: tuple[str, ...] = ("sqlite", "postgresql")
SQLITE_BACKUP_TIMEOUT_SECONDS = 30

_V1_KEY_COLUMNS: tuple[str, ...] = ("dag_id", "run_id", "task_id", "map_index", "try_number")
_V1_UNIQUE = "uq_healer_incidents_ti"
_V2_UNIQUE = "uq_healer_incidents_key"
#: Columns schema v2 adds to existing tables, in the order they are added (and declared).
_V2_ADDED_COLUMNS: dict[str, tuple[str, ...]] = {
    Incident.__tablename__: ("orchestrator", "native_ref", "error_name"),
    PendingEvent.__tablename__: ("orchestrator", "channel", "native_ref"),
    Command.__tablename__: ("orchestrator",),
    ContractState.__tablename__: ("orchestrator",),
}
#: Indexes schema v2 drops (replaced by ``ix_healer_incidents_orch_dag_created``).
_V2_OBSOLETE_INDEXES: dict[str, tuple[str, ...]] = {
    Incident.__tablename__: ("ix_healer_incidents_dag_created",),
}


class MigrationError(RuntimeError):
    """A schema migration cannot run, or stopped (its transaction was rolled back)."""


Upgrade = Callable[[Connection, str], None]
Plan = Callable[[Connection, str], list[str]]


@dataclass(frozen=True)
class Migration:
    """One schema version step.

    ``upgrade(conn, dialect_name)`` runs inside the locked migration transaction and
    re-inspects the database itself. ``plan(conn, dialect_name)`` returns the SQL that
    ``upgrade`` would run on the database as it is now (``store migrate --dry-run``).
    """

    version: int
    description: str
    upgrade: Upgrade
    plan: Plan | None = None


@dataclass(frozen=True)
class SchemaState:
    """What a store database holds, as far as the schema version is concerned."""

    version: int
    """:func:`current_version` (0: empty database)."""
    applied: tuple[tuple[int, datetime | None], ...]
    """``(version, applied_at)`` rows of ``healer_schema_version``, oldest version first."""
    tables: frozenset[str]
    """Names of the dag-healer tables present."""

    @property
    def missing_tables(self) -> frozenset[str]:
        """Tables of the current models that do not exist yet."""
        return frozenset(Base.metadata.tables) - self.tables


# =========================================================================== version


@contextmanager
def _connected(bind: Engine | Connection) -> Iterator[Connection]:
    if isinstance(bind, Connection):
        yield bind
        return
    with bind.connect() as conn:
        yield conn


def schema_state(bind: Engine | Connection) -> SchemaState:
    """Inspect the version table and the dag-healer tables (read only)."""
    with _connected(bind) as conn:
        names = set(inspect(conn).get_table_names())
        applied: list[tuple[int, datetime | None]] = []
        if SchemaVersion.__tablename__ in names:
            table = SchemaVersion.__table__
            rows = conn.execute(select(table.c.version, table.c.applied_at).order_by(table.c.version))
            applied = [(int(version), ensure_utc(at)) for version, at in rows]
    newest = max((version for version, _at in applied), default=0)
    if newest >= 2:
        version = newest
    else:
        version = 1 if Incident.__tablename__ in names else 0
    tables = frozenset(name for name in names if name in Base.metadata.tables)
    return SchemaState(version=version, applied=tuple(applied), tables=tables)


def current_version(bind: Engine | Connection) -> int:
    """0 = empty DB; 1 = ``healer_incidents`` exists and no version row >= 2; else max(version)."""
    return schema_state(bind).version


def pending(bind: Engine | Connection, *, target: int = SCHEMA_VERSION) -> list[Migration]:
    """Migrations that :func:`migrate` would apply now (none for an empty database).

    An empty database needs no migration: :func:`dag_healer.store.db.init_db` creates the
    latest schema directly.
    """
    _check_target(target)
    version = current_version(bind)
    if version == 0:
        return []
    return [migration for migration in MIGRATIONS if version < migration.version <= target]


def _check_target(target: int) -> None:
    if not 1 <= target <= SCHEMA_VERSION:
        raise ValueError(f"target schema version must be between 1 and {SCHEMA_VERSION}, not {target}")


def stamp(conn: Connection, version: int) -> None:
    """Record ``version`` in ``healer_schema_version`` (idempotent)."""
    table = SchemaVersion.__table__
    table.create(conn, checkfirst=True)
    found = conn.execute(select(table.c.version).where(table.c.version == version)).first()
    if found is None:
        conn.execute(table.insert().values(version=version, applied_at=utcnow()))


# =========================================================================== locking


def _dbapi(conn: Connection) -> Any:
    return conn.connection.dbapi_connection


def _foreign_keys_enabled(conn: Connection) -> bool:
    return bool(conn.exec_driver_sql("PRAGMA foreign_keys").scalar())


@contextmanager
def schema_lock(engine: Engine) -> Iterator[Connection]:
    """A connection in an exclusive schema-change transaction (commits on success).

    PostgreSQL: a transaction holding ``pg_advisory_xact_lock(hashtext('dag_healer_schema'))``.
    SQLite: an ``AUTOCOMMIT`` connection with ``PRAGMA foreign_keys=OFF`` (set outside the
    transaction, restored afterwards) running ``BEGIN IMMEDIATE`` ... ``COMMIT``. Other
    dialects get a plain transaction.
    """
    dialect = engine.dialect.name
    if dialect != "sqlite":
        with engine.begin() as conn:
            if dialect == "postgresql":
                conn.execute(text("SELECT pg_advisory_xact_lock(hashtext(:name))"), {"name": SCHEMA_LOCK})
            yield conn
        return

    with engine.connect() as conn:
        conn.execution_options(isolation_level="AUTOCOMMIT")
        raw = _dbapi(conn)
        foreign_keys = _foreign_keys_enabled(conn)
        conn.exec_driver_sql("PRAGMA foreign_keys=OFF")
        committed = False
        try:
            conn.exec_driver_sql("BEGIN IMMEDIATE")
            yield conn
            conn.exec_driver_sql("COMMIT")
            committed = True
        finally:
            if not committed and raw.in_transaction:
                raw.rollback()
            conn.exec_driver_sql(f"PRAGMA foreign_keys={'ON' if foreign_keys else 'OFF'}")


# =========================================================================== SQLite backup


def sqlite_database_file(engine: Engine) -> Path | None:
    """The database file of an SQLite engine (``None`` for memory databases and ``file:`` URIs)."""
    url = engine.url
    if url.get_backend_name() != "sqlite":
        return None
    database = url.database or ""
    if database in ("", ":memory:") or database.startswith("file:"):
        return None
    return Path(database)


def backup_path(engine: Engine, version: int) -> Path | None:
    """Where :func:`migrate` copies an SQLite store of schema ``version`` (``<db>.bak-v<version>``)."""
    database = sqlite_database_file(engine)
    if database is None:
        return None
    return database.with_name(database.name + BACKUP_SUFFIX.format(version=version))


def backup_sqlite(engine: Engine, version: int) -> Path | None:
    """Copy the SQLite database file to ``<db>.bak-v<version>`` (online backup, WAL-safe).

    Called by :func:`migrate` while it holds the write lock and before it writes anything,
    so the copy is exactly the store before the migration. The copy is a self-contained
    file (rollback journal mode); an earlier copy of the same version is replaced
    atomically. Returns ``None`` when there is no file (memory database).
    """
    database = sqlite_database_file(engine)
    target = backup_path(engine, version)
    if database is None or target is None:
        logger.info("dag-healer store: in-memory SQLite database, no backup taken")
        return None
    partial = target.with_name(target.name + ".tmp")
    try:
        partial.unlink(missing_ok=True)
        source = sqlite3.connect(str(database), timeout=SQLITE_BACKUP_TIMEOUT_SECONDS)
        try:
            copy = sqlite3.connect(str(partial))
            try:
                source.backup(copy)
                copy.execute("PRAGMA journal_mode=DELETE")
            finally:
                copy.close()
        finally:
            source.close()
        os.replace(partial, target)
    except (OSError, sqlite3.Error) as exc:
        partial.unlink(missing_ok=True)
        raise MigrationError(f"cannot back up {database} to {target}: {exc}") from exc
    logger.info("dag-healer store: copied %s to %s before migrating", database, target)
    return target


# =========================================================================== running migrations


def migrate(engine: Engine, *, target: int = SCHEMA_VERSION, dry_run: bool = False) -> list[int]:
    """Apply pending migrations in order under a lock; idempotent; returns applied versions.

    ``dry_run=True`` changes nothing: it logs the SQL (see :func:`migration_script`) and
    returns the versions that would be applied. An empty database is left alone (``[]``):
    :func:`dag_healer.store.db.init_db` creates the latest schema directly. Raises
    :class:`MigrationError` when a migration cannot run (for example an unsupported
    dialect, an unknown column that a rebuild would drop, or a failed foreign key check);
    the transaction is then rolled back and the store keeps its previous schema.
    """
    _check_target(target)
    todo = pending(engine, target=target)
    if dry_run:
        for line in migration_script(engine, target=target):
            logger.info("dag-healer store migration (dry run): %s", line)
        return [migration.version for migration in todo]
    if not todo:
        return []
    dialect = engine.dialect.name
    if dialect not in SUPPORTED_DIALECTS:
        raise MigrationError(f"store migrations support SQLite and PostgreSQL, not {dialect}")

    applied: list[int] = []
    with schema_lock(engine) as conn:
        version = current_version(conn)
        steps = [m for m in MIGRATIONS if version < m.version <= target] if version else []
        if not steps:
            logger.info("dag-healer store: schema v%d already migrated by another process", version)
            return []
        if dialect == "sqlite":
            backup_sqlite(engine, version)
        for migration in steps:
            if current_version(conn) >= migration.version:  # pragma: no cover - cannot change under the lock
                continue
            logger.info(
                "dag-healer store: applying schema migration v%d (%s)",
                migration.version,
                migration.description,
            )
            migration.upgrade(conn, dialect)
            stamp(conn, migration.version)
            applied.append(migration.version)
        if dialect == "sqlite":
            _check_foreign_keys(conn)
    if applied:
        logger.info("dag-healer store: schema migrated from v%d to v%d", version, applied[-1])
    return applied


def migration_script(engine: Engine, *, target: int = SCHEMA_VERSION) -> list[str]:
    """The SQL :func:`migrate` would run now, as printable lines (comments start with ``--``).

    Read only. For an empty database it lists the ``CREATE`` statements of the latest
    schema (what :func:`dag_healer.store.db.init_db` runs). Each step is planned against the
    database as it is now, so with several pending steps the later ones are indicative.
    """
    _check_target(target)
    dialect = engine.dialect.name
    with engine.connect() as conn:
        version = current_version(conn)
        if version == 0:
            lines = [f"-- the store is empty: it is created at schema v{SCHEMA_VERSION} directly"]
            lines += [f"{statement};" for statement in _create_missing(conn, frozenset())]
            return lines
        steps = [m for m in MIGRATIONS if version < m.version <= target]
        if not steps:
            return [f"-- the store is at schema v{version}: nothing to migrate"]
        lines = [f"-- dag-healer store schema v{version} -> v{steps[-1].version}"]
        if dialect == "sqlite":
            copy = backup_path(engine, version)
            lines.append(
                f"-- copy the database file to {copy} (SQLite online backup)"
                if copy is not None
                else "-- in-memory database: no backup"
            )
            lines += ["PRAGMA foreign_keys=OFF;", "BEGIN IMMEDIATE;"]
        else:
            lines.append("BEGIN;")
            if dialect == "postgresql":
                lines.append(f"SELECT pg_advisory_xact_lock(hashtext('{SCHEMA_LOCK}'));")
        for migration in steps:
            lines.append(f"-- v{migration.version}: {migration.description}")
            statements = migration.plan(conn, dialect) if migration.plan else []
            lines += [f"{statement};" for statement in statements]
            lines.append(
                f"INSERT INTO {SchemaVersion.__tablename__} (version, applied_at) "
                f"VALUES ({migration.version}, CURRENT_TIMESTAMP);"
            )
        if dialect == "sqlite":
            lines += ["PRAGMA foreign_key_check;", "COMMIT;", "PRAGMA foreign_keys=ON;"]
        else:
            lines.append("COMMIT;")
        return lines


def _check_foreign_keys(conn: Connection) -> None:
    """``PRAGMA foreign_key_check``: abort (and roll back) when a row is reported."""
    problems = conn.exec_driver_sql("PRAGMA foreign_key_check").fetchall()
    if problems:
        sample = ", ".join(f"{table} rowid {rowid} -> {parent}" for table, rowid, parent, _fk in problems[:5])
        raise MigrationError(
            f"foreign key check failed after the migration ({len(problems)} row(s): {sample}); "
            "the migration was rolled back"
        )


# =========================================================================== DDL helpers


def _quote(conn: Connection, name: str) -> str:
    return conn.dialect.identifier_preparer.quote(name)


def _ddl(conn: Connection, element: Any) -> str:
    """Compile a DDL construct for the connection's dialect."""
    return str(element.compile(dialect=conn.dialect)).strip()


def _sorted_indexes(table: Table) -> list[Any]:
    """The indexes of a model table in name order (``Table.indexes`` is a set)."""
    return sorted(table.indexes, key=lambda index: str(index.name))


def _drop_constraint(conn: Connection, table: Table, name: str) -> str:
    return f"ALTER TABLE {_quote(conn, table.name)} DROP CONSTRAINT IF EXISTS {_quote(conn, name)}"


def _table_copy(table: Table, name: str | None = None) -> Table:
    """A detached copy of a model table.

    DDL constructs such as ``AddConstraint`` mark the constraint they are given as "not part
    of CREATE TABLE"; compiling them from a copy keeps the models intact.
    """
    return table.to_metadata(MetaData(), name=name)


def _default_literal(column: Any) -> str | None:
    """SQL literal of a column's server default (``'airflow'``), or ``None``."""
    default = column.server_default
    argument = getattr(default, "arg", None)
    if argument is None:
        return None
    if isinstance(argument, str):
        return "'" + argument.replace("'", "''") + "'"
    return str(argument)


def _add_columns(conn: Connection, insp: Inspector, table: Table, *, if_not_exists: bool) -> list[str]:
    """``ALTER TABLE ... ADD COLUMN`` for the v2 columns that ``table`` does not have yet."""
    present = {column["name"] for column in insp.get_columns(table.name)}
    guard = "IF NOT EXISTS " if if_not_exists else ""
    return [
        f"ALTER TABLE {_quote(conn, table.name)} ADD COLUMN {guard}{_ddl(conn, CreateColumn(table.c[name]))}"
        for name in _V2_ADDED_COLUMNS.get(table.name, ())
        if name not in present
    ]


def _index_fixes(conn: Connection, insp: Inspector, table: Table) -> list[str]:
    """Drop the obsolete indexes of ``table`` and (re)create the model's indexes that differ."""
    present = {index["name"]: tuple(index["column_names"]) for index in insp.get_indexes(table.name)}
    statements = [
        f"DROP INDEX IF EXISTS {_quote(conn, name)}"
        for name in _V2_OBSOLETE_INDEXES.get(table.name, ())
        if name in present
    ]
    for index in _sorted_indexes(table):
        wanted = tuple(column.name for column in index.columns)
        found = present.get(str(index.name))
        if found == wanted:
            continue
        if found is not None:
            statements.append(f"DROP INDEX IF EXISTS {_quote(conn, str(index.name))}")
        statements.append(_ddl(conn, CreateIndex(index, if_not_exists=True)))
    return statements


def _create_missing(conn: Connection, present: frozenset[str] | set[str]) -> list[str]:
    """``CREATE TABLE`` / ``CREATE INDEX`` (``IF NOT EXISTS``) for model tables not in ``present``."""
    statements: list[str] = []
    for table in Base.metadata.sorted_tables:
        if table.name in present:
            continue
        statements.append(_ddl(conn, CreateTable(table, if_not_exists=True)))
        statements += [_ddl(conn, CreateIndex(index, if_not_exists=True)) for index in _sorted_indexes(table)]
    return statements


def _v2_unique(table: Table) -> UniqueConstraint:
    for constraint in table.constraints:
        if isinstance(constraint, UniqueConstraint) and constraint.name == _V2_UNIQUE:
            return constraint
    raise MigrationError(f"{table.name} has no {_V2_UNIQUE} constraint")  # pragma: no cover - model bug


def _unique_keys(insp: Inspector, table: str) -> list[tuple[str | None, tuple[str, ...]]]:
    return [(item.get("name"), tuple(item["column_names"])) for item in insp.get_unique_constraints(table)]


# =========================================================================== migration v2


def _incidents_need_rebuild(insp: Inspector) -> bool:
    """SQLite: the incident key is not exactly ``uq_healer_incidents_key`` (or no orchestrator)."""
    table = Incident.__table__
    columns = {column["name"] for column in insp.get_columns(table.name)}
    wanted = (_V2_UNIQUE, tuple(column.name for column in _v2_unique(table).columns))
    return "orchestrator" not in columns or _unique_keys(insp, table.name) != [wanted]


def _contracts_need_rebuild(insp: Inspector) -> bool:
    """The contract cache is not keyed by ``(orchestrator, dag_id)`` yet."""
    table = ContractState.__table__
    found = tuple(insp.get_pk_constraint(table.name).get("constrained_columns") or ())
    return found != tuple(column.name for column in table.primary_key.columns)


def _sqlite_rebuild(conn: Connection, insp: Inspector, table: Table, present: set[str]) -> list[str]:
    """SQLite's documented table rebuild: create ``<t>__v2``, copy, drop ``<t>``, rename, indexes.

    Never renames the *old* table (SQLite >= 3.26 would rewrite the children's foreign keys
    to follow it). Every v1 column is copied as stored; new columns get their server
    default. A column the model does not know would be lost, so it stops the migration.
    """
    name = table.name
    temporary = f"{name}{REBUILD_SUFFIX}"
    old_columns = [column["name"] for column in insp.get_columns(name)]
    new_columns = [column.name for column in table.columns]
    unknown = [column for column in old_columns if column not in new_columns]
    if unknown:
        raise MigrationError(
            f"{name} has column(s) {', '.join(unknown)} that schema v2 does not define; "
            "rebuilding the table would drop them"
        )
    filled: list[tuple[str, str]] = []
    for column in table.columns:
        if column.name in old_columns:
            continue
        literal = _default_literal(column)
        if literal is not None:
            filled.append((column.name, literal))
        elif not column.nullable:  # pragma: no cover - every NOT NULL v2 column has a default
            raise MigrationError(f"cannot fill the new NOT NULL column {name}.{column.name}")
    copied = [column for column in new_columns if column in old_columns]
    known = {str(index.name) for index in table.indexes} | set(_V2_OBSOLETE_INDEXES.get(name, ()))
    for index in insp.get_indexes(name):
        if index["name"] not in known:
            logger.warning(
                "dag-healer store: index %s on %s is not part of schema v2 and is dropped by the rebuild",
                index["name"],
                name,
            )

    targets = ", ".join(_quote(conn, column) for column in [*copied, *(column for column, _ in filled)])
    sources = ", ".join([*(_quote(conn, column) for column in copied), *(literal for _, literal in filled)])
    statements: list[str] = []
    if temporary in present:  # left behind by an interrupted rebuild outside a transaction
        statements.append(f"DROP TABLE {_quote(conn, temporary)}")
    statements += [
        _ddl(conn, CreateTable(_table_copy(table, temporary))),
        f"INSERT INTO {_quote(conn, temporary)} ({targets}) SELECT {sources} FROM {_quote(conn, name)}",
        f"DROP TABLE {_quote(conn, name)}",
        f"ALTER TABLE {_quote(conn, temporary)} RENAME TO {_quote(conn, name)}",
    ]
    statements += [_ddl(conn, CreateIndex(index)) for index in _sorted_indexes(table)]
    return statements


def _plan_v2_sqlite(conn: Connection) -> list[str]:
    insp = inspect(conn)
    present = set(insp.get_table_names())
    statements: list[str] = []
    for model in (PendingEvent, Command):
        if model.__tablename__ in present:
            statements += _add_columns(conn, insp, model.__table__, if_not_exists=False)
    incidents = Incident.__table__
    if incidents.name in present:
        if _incidents_need_rebuild(insp):
            statements += _sqlite_rebuild(conn, insp, incidents, present)
        else:
            statements += _add_columns(conn, insp, incidents, if_not_exists=False)
            statements += _index_fixes(conn, insp, incidents)
    contracts = ContractState.__table__
    if contracts.name in present and _contracts_need_rebuild(insp):
        statements += _sqlite_rebuild(conn, insp, contracts, present)
    return statements + _create_missing(conn, present)


def _plan_v2_postgresql(conn: Connection) -> list[str]:
    insp = inspect(conn)
    present = set(insp.get_table_names())
    statements: list[str] = []

    incidents = Incident.__table__
    if incidents.name in present:
        statements += _add_columns(conn, insp, incidents, if_not_exists=True)
        key = _v2_unique(incidents)
        wanted = tuple(column.name for column in key.columns)
        has_key = False
        for name, columns in _unique_keys(insp, incidents.name):
            if name == _V2_UNIQUE and columns == wanted:
                has_key = True
            elif name is not None and (name in (_V1_UNIQUE, _V2_UNIQUE) or columns == _V1_KEY_COLUMNS):
                statements.append(_drop_constraint(conn, incidents, name))
        if not has_key:
            copy = _table_copy(incidents)
            statements.append(_ddl(conn, AddConstraint(_v2_unique(copy))))
        statements += _index_fixes(conn, insp, incidents)

    for model in (PendingEvent, Command):
        if model.__tablename__ in present:
            statements += _add_columns(conn, insp, model.__table__, if_not_exists=True)

    contracts = ContractState.__table__
    if contracts.name in present:
        statements += _add_columns(conn, insp, contracts, if_not_exists=True)
        if _contracts_need_rebuild(insp):
            primary = insp.get_pk_constraint(contracts.name).get("name")
            if primary:
                statements.append(_drop_constraint(conn, contracts, primary))
            statements.append(_ddl(conn, AddConstraint(_table_copy(contracts).primary_key)))
    return statements + _create_missing(conn, present)


def _plan_v2(conn: Connection, dialect: str) -> list[str]:
    """The SQL of migration v2 for the database as it is now (empty when already applied)."""
    if dialect == "sqlite":
        return _plan_v2_sqlite(conn)
    if dialect == "postgresql":
        return _plan_v2_postgresql(conn)
    raise MigrationError(f"store migrations support SQLite and PostgreSQL, not {dialect}")


def _upgrade_v2(conn: Connection, dialect: str) -> None:
    """Migration v2: orchestrator-aware keys (section 6.2 of the design)."""
    for statement in _plan_v2(conn, dialect):
        if dialect == "sqlite" and statement.startswith("DROP TABLE ") and _foreign_keys_enabled(conn):
            # With foreign keys on, DROP TABLE deletes the rows first and cascades to the
            # children (timeline events, alerts, commands).
            raise MigrationError("refusing to rebuild an SQLite table while foreign keys are enforced")
        logger.debug("dag-healer store migration v2: %s", statement)
        conn.exec_driver_sql(statement)


MIGRATIONS: tuple[Migration, ...] = (Migration(2, "orchestrator-aware keys", _upgrade_v2, _plan_v2),)


__all__ = [
    "BACKUP_SUFFIX",
    "MIGRATIONS",
    "SCHEMA_LOCK",
    "Migration",
    "MigrationError",
    "SchemaState",
    "backup_path",
    "backup_sqlite",
    "current_version",
    "migrate",
    "migration_script",
    "pending",
    "schema_lock",
    "schema_state",
    "sqlite_database_file",
    "stamp",
]
