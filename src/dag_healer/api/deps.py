"""FastAPI dependencies of the dag-healer API: configuration, orchestrator, store sessions,
ingest guards.

Everything here works without Airflow; authentication lives in :mod:`dag_healer.api.auth`.
The process serves one orchestrator instance (:func:`get_instance`): lists, filters and the
commands it enqueues are scoped to it. :func:`get_adapter` gives the instance's adapter for
capability checks and native links; the API never calls the orchestrator itself.
"""

from __future__ import annotations

import hashlib
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
from dag_healer.core.ids import DEFAULT_ORCHESTRATOR
from dag_healer.core.ports import OrchestratorAdapter
from dag_healer.masking import mask_text
from dag_healer.orchestrators import registry
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


# --------------------------------------------------------------------------- orchestrator

_adapter_failures: set[tuple[str, ...]] = set()
_adapter_failures_lock = threading.Lock()
_MAX_REMEMBERED_FAILURES = 64


def _log_adapter_failure(cfg: HealerConfig, exc: Exception) -> None:
    """Warn once per configuration (file, type, instance) and error."""
    section = getattr(cfg, "orchestrator", None)
    key = (
        str(getattr(cfg, "source_path", None)),
        str(getattr(section, "type", None)),
        str(getattr(section, "instance", None)),
        f"{type(exc).__name__}: {exc}",
    )
    with _adapter_failures_lock:
        if key in _adapter_failures:
            return
        if len(_adapter_failures) >= _MAX_REMEMBERED_FAILURES:
            _adapter_failures.clear()
        _adapter_failures.add(key)
    log.warning(
        "dag-healer: the orchestrator adapter cannot be built (%s); the API serves its routes "
        "without it (no capability filtering, no native links)",
        mask_text(key[-1])[:500],
    )


def get_adapter(cfg: HealerConfig = Depends(get_config)) -> OrchestratorAdapter | None:
    """Dependency: the orchestrator adapter of the configuration (cached, built without I/O).

    ``None`` when it cannot be built (logged once per configuration): the routes then behave
    as before adapters existed (no capability filtering, no native links, ``/me`` without
    ``orchestrator``). Tests override this dependency to inject an adapter.
    """
    try:
        return registry.get_adapter(cfg)
    except Exception as exc:  # a broken adapter must not take the Healer views down
        _log_adapter_failure(cfg, exc)
        return None


def get_instance(
    cfg: HealerConfig = Depends(get_config),
    adapter: OrchestratorAdapter | None = Depends(get_adapter),
) -> str:
    """Dependency: the orchestrator instance id this process serves (``"airflow"`` by default).

    The adapter's instance, else ``orchestrator.instance`` (or its type) from the configuration.
    """
    if adapter is not None:
        return str(adapter.instance)
    instance = getattr(getattr(cfg, "orchestrator", None), "instance_id", None)
    return str(instance) if instance else DEFAULT_ORCHESTRATOR


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
    """In-process sliding-window rate limiter keyed by a string (client address, channel and token)."""

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
#: Rate limiter of ``POST /events/{channel}`` (keyed by channel and ingest token).
channel_rate_limiter = SlidingWindowRateLimiter(INGEST_RATE_LIMIT, INGEST_RATE_WINDOW_SECONDS)
#: Characters of a channel name used in a rate-limiter key (the path segment is caller-chosen).
_MAX_CHANNEL_KEY_CHARS = 64


def _rate_limit(limiter: SlidingWindowRateLimiter, key: str) -> None:
    """Count one ingest request under ``key``; 429 with ``Retry-After`` above the limit."""
    retry_after = limiter.hit(key)
    if retry_after is not None:
        raise HTTPException(
            status_code=status.HTTP_429_TOO_MANY_REQUESTS,
            detail="Too many failure events; slow down.",
            headers={"Retry-After": str(max(1, int(retry_after + 0.999)))},
        )


def _check_body_size(request: Request) -> None:
    """413 when ``Content-Length`` announces more than :data:`INGEST_MAX_BODY_BYTES` (400 if invalid)."""
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


def ingest_guard(request: Request) -> None:
    """Dependency: rate limit (120/min per client) and body size cap of the ingest route."""
    _rate_limit(ingest_rate_limiter, request.client.host if request.client else "unknown")
    _check_body_size(request)


def channel_ingest_guard(request: Request, channel: str) -> None:
    """Dependency: rate limit (120/min per channel and token) and body size cap of ``/events/{channel}``.

    Keyed by the channel and a digest of ``X-Healer-Token`` rather than by the client address:
    the senders of a channel (EventBridge API destinations, for example) share addresses.
    """
    token = request.headers.get(INGEST_TOKEN_HEADER) or ""
    digest = hashlib.sha256(token.encode("utf-8")).hexdigest()[:16]
    _rate_limit(channel_rate_limiter, f"{channel[:_MAX_CHANNEL_KEY_CHARS]}:{digest}")
    _check_body_size(request)


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
    "channel_ingest_guard",
    "channel_rate_limiter",
    "configured_ingest_token",
    "get_adapter",
    "get_config",
    "get_instance",
    "get_session",
    "ingest_guard",
    "ingest_rate_limiter",
    "readable_dag_ids",
    "require_ingest_token",
    "reset_config_cache",
]
