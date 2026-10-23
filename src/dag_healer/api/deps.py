"""FastAPI dependencies of the dag-healer API: configuration, store sessions, ingest guards.

Everything here works without Airflow; authentication lives in :mod:`dag_healer.api.auth`.
"""

from __future__ import annotations

import hmac
import logging
import os
import threading
import time
from collections import deque
from collections.abc import Iterable, Iterator
from typing import Any

from fastapi import Depends, HTTPException, Request, status
from sqlalchemy.orm import Session

from dag_healer.api import auth
from dag_healer.config import ConfigError, HealerConfig, load_config
from dag_healer.store.db import StoreSchemaOutdated, ensure_initialized, get_engine, session_scope

log = logging.getLogger(__name__)

API_PREFIX = "/api/v1"
CONFIG_TTL_SECONDS = 10.0
INGEST_TOKEN_HEADER = "X-Healer-Token"
INGEST_TOKEN_ENV = "HEALER_INGEST_TOKEN"
INGEST_RATE_LIMIT = 120
INGEST_RATE_WINDOW_SECONDS = 60.0
#: Largest accepted ingest request body (the listener sends ~5 KB at most).
INGEST_MAX_BODY_BYTES = 64 * 1024

# --------------------------------------------------------------------------- configuration

_config_lock = threading.Lock()
_config_cache: dict[str, Any] = {"cfg": None, "loaded_at": 0.0, "key": None}


def _config_key() -> tuple[str | None, ...]:
    """Inputs of :func:`load_config` besides the file contents."""
    return (os.environ.get("HEALER_CONFIG"), os.environ.get("AIRFLOW_HOME"), os.getcwd())


def reset_config_cache() -> None:
    """Forget the cached configuration (the next request reloads ``healer.yaml``)."""
    with _config_lock:
        _config_cache.update(cfg=None, loaded_at=0.0, key=None)


def get_config() -> HealerConfig:
    """Dependency: the dag-healer configuration, reloaded at most every 10 seconds.

    An invalid ``healer.yaml`` keeps the last good configuration (logged); without one
    the request fails with 503.
    """
    now = time.monotonic()
    key = _config_key()
    with _config_lock:
        cached = _config_cache["cfg"]
        fresh = now - _config_cache["loaded_at"] < CONFIG_TTL_SECONDS
        if cached is not None and fresh and _config_cache["key"] == key:
            return cached
    try:
        cfg = load_config()
    except ConfigError as exc:
        log.error("dag-healer: cannot load the configuration: %s", exc)
        if cached is not None and _config_cache["key"] == key:
            with _config_lock:
                _config_cache["loaded_at"] = now
            return cached
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail="The dag-healer configuration is invalid; run `dag-healer doctor`.",
        ) from None
    with _config_lock:
        _config_cache.update(cfg=cfg, loaded_at=now, key=key)
    return cfg


def get_session(cfg: HealerConfig = Depends(get_config)) -> Iterator[Session]:
    """Dependency: a store session (``store.db.session_scope``) for the request.

    Write handlers commit explicitly before answering; the scope commits again (no-op)
    or rolls back when the handler raised. A store whose schema is older than this
    dag-healer answers 503: web processes never migrate it (the worker does).
    """
    try:
        ensure_initialized(get_engine(cfg.database_url))
    except StoreSchemaOutdated as exc:
        log.warning("dag-healer: %s", exc)
        raise HTTPException(status_code=status.HTTP_503_SERVICE_UNAVAILABLE, detail=str(exc)) from None
    with session_scope(cfg.database_url) as session:
        yield session


# --------------------------------------------------------------------------- permissions


def readable_dag_ids(authorizer: auth.Authorizer, user: Any, candidates: Iterable[str] = ()) -> set[str]:
    """DAG ids the user may read: Airflow's authorized ids plus readable ``candidates``.

    ``candidates`` are DAG ids known to dag-healer (incidents, contracts) that Airflow may
    not list (for example DAGs deleted from Airflow); each one is checked individually.
    """
    readable = set(authorizer.authorized_dag_ids(user, "GET"))
    for dag_id in set(candidates) - readable:
        if dag_id and authorizer.can_read_dag(user, dag_id):
            readable.add(dag_id)
    return readable


# --------------------------------------------------------------------------- ingest guards


class SlidingWindowRateLimiter:
    """In-process sliding-window rate limiter keyed by client address."""

    def __init__(self, limit: int, window_seconds: float, *, max_keys: int = 10_000) -> None:
        self.limit = int(limit)
        self.window = float(window_seconds)
        self.max_keys = int(max_keys)
        self._hits: dict[str, deque[float]] = {}
        self._lock = threading.Lock()

    def hit(self, key: str, *, now: float | None = None) -> float | None:
        """Record one request; return ``None`` when allowed, else seconds to wait."""
        moment = time.monotonic() if now is None else now
        cutoff = moment - self.window
        with self._lock:
            hits = self._hits.get(key)
            if hits is None:
                if len(self._hits) >= self.max_keys:
                    self._prune(cutoff)
                hits = self._hits.setdefault(key, deque())
            while hits and hits[0] <= cutoff:
                hits.popleft()
            if len(hits) >= self.limit:
                return max(0.0, hits[0] + self.window - moment)
            hits.append(moment)
            return None

    def _prune(self, cutoff: float) -> None:
        for key in [k for k, v in self._hits.items() if not v or v[-1] <= cutoff]:
            del self._hits[key]
        if len(self._hits) >= self.max_keys:  # still full: forget the oldest key
            oldest = min(self._hits, key=lambda k: self._hits[k][-1])
            del self._hits[oldest]

    def reset(self) -> None:
        """Forget every recorded request."""
        with self._lock:
            self._hits.clear()


ingest_rate_limiter = SlidingWindowRateLimiter(INGEST_RATE_LIMIT, INGEST_RATE_WINDOW_SECONDS)


def ingest_guard(request: Request) -> None:
    """Dependency: rate limit (120/min per client) and body size cap of the ingest route."""
    client = request.client.host if request.client else "unknown"
    retry_after = ingest_rate_limiter.hit(client)
    if retry_after is not None:
        raise HTTPException(
            status_code=status.HTTP_429_TOO_MANY_REQUESTS,
            detail="Too many failure events; slow down.",
            headers={"Retry-After": str(max(1, int(retry_after + 0.999)))},
        )
    length = request.headers.get("content-length")
    if length is not None:
        try:
            too_large = int(length) > INGEST_MAX_BODY_BYTES
        except ValueError:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST, detail="Bad Content-Length"
            ) from None
        if too_large:
            raise HTTPException(status_code=413, detail="Failure event payload is too large")


def configured_ingest_token(cfg: HealerConfig) -> str | None:
    """The shared ingest secret: ``ingest.token`` or ``$HEALER_INGEST_TOKEN``."""
    token = (cfg.ingest.token or "").strip() or os.environ.get(INGEST_TOKEN_ENV, "").strip()
    return token or None


def require_ingest_token(request: Request, cfg: HealerConfig = Depends(get_config)) -> None:
    """Dependency: check ``X-Healer-Token`` in constant time (503 when unconfigured)."""
    expected = configured_ingest_token(cfg)
    if not expected:
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail="Failure ingestion is disabled: no ingest token is configured.",
        )
    supplied = request.headers.get(INGEST_TOKEN_HEADER)
    if not supplied or not hmac.compare_digest(supplied.encode("utf-8"), expected.encode("utf-8")):
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED, detail="Invalid or missing ingest token"
        )


__all__ = [
    "API_PREFIX",
    "CONFIG_TTL_SECONDS",
    "INGEST_TOKEN_HEADER",
    "SlidingWindowRateLimiter",
    "configured_ingest_token",
    "get_config",
    "get_session",
    "ingest_guard",
    "ingest_rate_limiter",
    "readable_dag_ids",
    "require_ingest_token",
    "reset_config_cache",
]
