"""Engine, schema initialisation and session helpers for the dag-healer store."""

from __future__ import annotations

import logging
import os
import threading
import weakref
from collections.abc import Iterator
from contextlib import contextmanager
from pathlib import Path
from typing import Any

from sqlalchemy import Engine, create_engine, event, select
from sqlalchemy.engine import make_url
from sqlalchemy.exc import DBAPIError
from sqlalchemy.orm import Session, sessionmaker
from sqlalchemy.pool import StaticPool

from dag_healer.store.models import SCHEMA_VERSION, Base, SchemaVersion

logger = logging.getLogger(__name__)

DATABASE_URL_ENV = "HEALER_DATABASE_URL"
SQLITE_BUSY_TIMEOUT_SECONDS = 30

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


def _create_engine(url: str) -> Engine:
    backend = make_url(url).get_backend_name()
    if backend != "sqlite":
        return create_engine(url, pool_pre_ping=True, pool_recycle=1800)

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


def init_db(engine: Engine) -> None:
    """Create missing tables and record the schema version (safe to call repeatedly).

    Two processes initialising the same PostgreSQL database at once can race on
    ``CREATE TABLE``; the losing side retries once, after which the tables exist.
    """
    for attempt in (1, 2):
        try:
            Base.metadata.create_all(engine)
            with Session(engine) as session, session.begin():
                query = select(SchemaVersion).where(SchemaVersion.version == SCHEMA_VERSION)
                current = session.scalar(query)
                if current is None:
                    session.add(SchemaVersion(version=SCHEMA_VERSION))
            break
        except DBAPIError:
            if attempt == 2:
                raise
            logger.info("Store schema initialisation raced with another process; retrying")
    with _lock:
        _initialized.add(engine)


def ensure_initialized(engine: Engine) -> None:
    """Run :func:`init_db` once per engine for the lifetime of the process."""
    if engine in _initialized:
        return
    with _lock:
        if engine in _initialized:
            return
        init_db(engine)


def get_sessionmaker(engine: Engine) -> sessionmaker[Session]:
    """Return the cached session factory bound to ``engine``."""
    with _lock:
        factory = _sessionmakers.get(engine)
        if factory is None:
            factory = sessionmaker(bind=engine, expire_on_commit=False)
            _sessionmakers[engine] = factory
        return factory


@contextmanager
def session_scope(url: str | None = None) -> Iterator[Session]:
    """Yield a session that commits on success and rolls back on error.

    The schema is created on first use of each engine, so API processes and the
    worker can start in any order. Objects stay usable after the scope closes
    (``expire_on_commit=False``).
    """
    engine = get_engine(url)
    ensure_initialized(engine)
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
