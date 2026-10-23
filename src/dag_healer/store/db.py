"""Engine, schema initialisation and session helpers for the dag-healer store.

Who creates and who migrates the schema (section 6.4 of ``docs/architecture/orchestrators.md``):

* an **empty** database is created at the latest schema by any process (``create_all`` plus
  one ``healer_schema_version`` row per version);
* an **older** schema is migrated only by processes that pass ``allow_migrate=True`` to
  :func:`init_db` (``dag-healer worker``, ``serve``, ``init`` and ``store migrate``). Every
  other process (the Airflow api-server plugin) gets :class:`StoreSchemaOutdated` and never
  rebuilds a table; :func:`ensure_initialized` does not cache that outcome, so it recovers
  as soon as the worker has migrated the store.
"""

from __future__ import annotations

import logging
import os
import threading
import weakref
from collections.abc import Iterator
from contextlib import contextmanager
from pathlib import Path
from typing import Any

from sqlalchemy import Engine, create_engine, event
from sqlalchemy.engine import make_url
from sqlalchemy.exc import DBAPIError
from sqlalchemy.orm import Session, sessionmaker
from sqlalchemy.pool import StaticPool

from dag_healer.store.migrations import current_version, migrate, schema_lock, schema_state, stamp
from dag_healer.store.models import SCHEMA_VERSION, Base

logger = logging.getLogger(__name__)

DATABASE_URL_ENV = "HEALER_DATABASE_URL"
SQLITE_BUSY_TIMEOUT_SECONDS = 30
POSTGRES_CONNECT_TIMEOUT_SECONDS = 5

_lock = threading.RLock()
_engines: dict[str, Engine] = {}
_sessionmakers: weakref.WeakKeyDictionary[Engine, sessionmaker[Session]] = weakref.WeakKeyDictionary()
_initialized: weakref.WeakSet[Engine] = weakref.WeakSet()


def resolve_database_url(url: str | None = None) -> str:
    """Return the store URL: explicit ``url``, then ``$HEALER_DATABASE_URL``, then config."""
    if url:
        return url
    env_url = os.environ.get(DATABASE_URL_ENV)
    if env_url:
        return env_url
    from dag_healer.config import load_config  # local import: config is optional here

    return load_config().database_url


def _is_memory_sqlite(url: str) -> bool:
    parsed = make_url(url)
    return parsed.get_backend_name() == "sqlite" and parsed.database in (None, "", ":memory:")


def _sqlite_file(url: str) -> Path | None:
    parsed = make_url(url)
    if parsed.get_backend_name() != "sqlite" or _is_memory_sqlite(url):
        return None
    database = parsed.database or ""
    if database.startswith("file:"):
        return None
    return Path(database)


def _install_sqlite_pragmas(engine: Engine, *, wal: bool) -> None:
    """Enable foreign keys, a busy timeout and (for files) WAL on every connection."""

    @event.listens_for(engine, "connect")
    def _on_connect(dbapi_connection: Any, _record: Any) -> None:
        cursor = dbapi_connection.cursor()
        try:
            cursor.execute("PRAGMA foreign_keys=ON")
            cursor.execute(f"PRAGMA busy_timeout={SQLITE_BUSY_TIMEOUT_SECONDS * 1000}")
            if wal:
                cursor.execute("PRAGMA journal_mode=WAL")
                cursor.execute("PRAGMA synchronous=NORMAL")
        finally:
            cursor.close()


def _postgres_connect_args(url: str) -> dict[str, Any]:
    """Connection timeout for PostgreSQL drivers, so an unreachable store fails fast.

    libpq-based drivers (psycopg2, psycopg 3) take ``connect_timeout``; pg8000 takes
    ``timeout``. A value given in the URL query string wins.
    """
    parsed = make_url(url)
    if parsed.get_backend_name() != "postgresql":
        return {}
    driver = parsed.get_driver_name()
    key = "timeout" if driver == "pg8000" else "connect_timeout"
    if driver not in ("psycopg2", "psycopg", "psycopg2cffi", "pg8000") or key in parsed.query:
        return {}
    return {key: POSTGRES_CONNECT_TIMEOUT_SECONDS}


def _create_engine(url: str) -> Engine:
    backend = make_url(url).get_backend_name()
    if backend != "sqlite":
        return create_engine(
            url, pool_pre_ping=True, pool_recycle=1800, connect_args=_postgres_connect_args(url)
        )

    connect_args: dict[str, Any] = {
        "check_same_thread": False,
        "timeout": SQLITE_BUSY_TIMEOUT_SECONDS,
    }
    if _is_memory_sqlite(url):
        engine = create_engine(url, connect_args=connect_args, poolclass=StaticPool, pool_pre_ping=True)
        _install_sqlite_pragmas(engine, wal=False)
        return engine

    db_file = _sqlite_file(url)
    if db_file is not None:
        db_file.expanduser().parent.mkdir(parents=True, exist_ok=True)
    engine = create_engine(url, connect_args=connect_args, pool_pre_ping=True)
    _install_sqlite_pragmas(engine, wal=True)
    return engine


def get_engine(url: str | None = None) -> Engine:
    """Return a cached engine for the resolved store URL (one engine per URL)."""
    resolved = resolve_database_url(url)
    with _lock:
        engine = _engines.get(resolved)
        if engine is None:
            engine = _create_engine(resolved)
            _engines[resolved] = engine
            logger.debug("Created store engine for %s", engine.url.render_as_string())
        return engine


class StoreSchemaOutdated(RuntimeError):
    """The store schema is older than this dag-healer, and this process may not migrate it.

    ``str(exc)`` is the message the API answers with (HTTP 503), for example
    ``dag-healer store schema v1: start the worker or run 'dag-healer store migrate'``.
    """

    def __init__(self, current: int, required: int = SCHEMA_VERSION) -> None:
        self.current = current
        self.required = required
        super().__init__(
            f"dag-healer store schema v{current}: start the worker or run 'dag-healer store migrate'"
        )

    def __reduce__(self) -> tuple[Any, ...]:
        return (type(self), (self.current, self.required))


def init_db(engine: Engine, *, allow_migrate: bool = False) -> None:
    """Create or upgrade the store schema to :data:`SCHEMA_VERSION` (safe to call repeatedly).

    * Empty database: ``create_all`` builds the latest schema and records every version
      (1 and 2), under the schema lock (:func:`dag_healer.store.migrations.schema_lock`), so
      concurrent processes never see a half-created store.
    * Older schema: with ``allow_migrate=True`` it runs
      :func:`dag_healer.store.migrations.migrate`; otherwise it raises
      :class:`StoreSchemaOutdated` without changing anything.
    * Up to date: missing tables are created (nothing else runs when every table exists).
    * Newer schema (written by a newer dag-healer): logged, left alone.

    Raises :class:`dag_healer.store.migrations.MigrationError` when a migration cannot run.
    Two processes initialising the same database at once can still race on ``CREATE TABLE``
    on a backend without the lock; the losing side retries once.
    """
    for attempt in (1, 2):
        try:
            _initialise(engine, allow_migrate=allow_migrate)
            break
        except DBAPIError:
            if attempt == 2:
                raise
            logger.info("Store schema initialisation raced with another process; retrying")
    with _lock:
        _initialized.add(engine)


def _initialise(engine: Engine, *, allow_migrate: bool) -> None:
    state = schema_state(engine)
    if state.version == SCHEMA_VERSION and not state.missing_tables:
        return
    if state.version > SCHEMA_VERSION:
        logger.warning(
            "dag-healer store schema v%d is newer than this dag-healer (v%d); upgrade dag-healer",
            state.version,
            SCHEMA_VERSION,
        )
        return
    if 0 < state.version < SCHEMA_VERSION:
        if not allow_migrate:
            raise StoreSchemaOutdated(state.version)
        migrate(engine)
    with schema_lock(engine) as conn:
        version = current_version(conn)
        if 0 < version < SCHEMA_VERSION:  # replaced by an older store meanwhile
            raise StoreSchemaOutdated(version)
        if version > SCHEMA_VERSION:  # pragma: no cover - a newer dag-healer migrated meanwhile
            return
        Base.metadata.create_all(conn)
        if version == 0:
            for number in range(1, SCHEMA_VERSION + 1):
                stamp(conn, number)
            logger.info("dag-healer store: created schema v%d", SCHEMA_VERSION)


def ensure_initialized(engine: Engine, *, allow_migrate: bool = False) -> None:
    """Run :func:`init_db` once per engine for the lifetime of the process.

    A failure (for example :class:`StoreSchemaOutdated`) is not remembered: the next call
    checks the schema again.
    """
    if engine in _initialized:
        return
    with _lock:
        if engine in _initialized:
            return
        init_db(engine, allow_migrate=allow_migrate)


def get_sessionmaker(engine: Engine) -> sessionmaker[Session]:
    """Return the cached session factory bound to ``engine``."""
    with _lock:
        factory = _sessionmakers.get(engine)
        if factory is None:
            factory = sessionmaker(bind=engine, expire_on_commit=False)
            _sessionmakers[engine] = factory
        return factory


@contextmanager
def session_scope(url: str | None = None, *, allow_migrate: bool = False) -> Iterator[Session]:
    """Yield a session that commits on success and rolls back on error.

    The schema is created on first use of each engine, so API processes and the
    worker can start in any order. Objects stay usable after the scope closes
    (``expire_on_commit=False``). ``allow_migrate=True`` (the worker's sessions) also
    upgrades an older schema, see :func:`init_db`; otherwise an older schema raises
    :class:`StoreSchemaOutdated`.
    """
    engine = get_engine(url)
    ensure_initialized(engine, allow_migrate=allow_migrate)
    session = get_sessionmaker(engine)()
    try:
        yield session
        session.commit()
    except BaseException:
        session.rollback()
        raise
    finally:
        session.close()


def dispose_engines() -> None:
    """Dispose and forget every cached engine (used by tests and on shutdown)."""
    with _lock:
        for engine in _engines.values():
            engine.dispose()
        _engines.clear()
        _sessionmakers.clear()
        _initialized.clear()
