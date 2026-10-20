"""The dag-healer worker loop (``dag-healer worker``).

Every ``monitor.poll_interval_seconds`` one iteration runs:

1. release claims older than 30 minutes (a worker that crashed mid-incident);
2. :func:`~dag_healer.worker.pipeline.apply_commands` - the human actions queued by the UI/CLI;
3. :func:`~dag_healer.worker.pipeline.reconcile` - new failures from the REST poll and the
   pushed listener/callback events;
4. the claimed incidents, one at a time: incidents resumed by a human first, then new ones.
   Each claim is committed right away (so another worker never takes the same incident) and
   :func:`~dag_healer.worker.pipeline.process_incident` runs it to its next resting state.
   Pending commands are applied again between incidents.

``limits.max_incidents_per_hour`` caps the automatic triages started in any rolling hour:
beyond it new incidents stay ``new`` (and are logged) until the window frees up; incidents a
human resumed are always processed. SIGTERM / SIGINT stop the loop gracefully after the
current step (a second signal interrupts it). A failing incident never stops the worker: it
ends in ``error`` with a masked timeline event.
"""

from __future__ import annotations

import logging
import signal
import threading
import time
from collections.abc import Callable
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from pathlib import Path
from types import FrameType
from typing import TYPE_CHECKING, Any

from sqlalchemy import func, select

from dag_healer.store import repo as store_repo
from dag_healer.store.models import NEW, RESUMED, Incident, utcnow
from dag_healer.worker.pipeline import (
    STALE_CLAIM_AFTER,
    TRIAGE_EVENT,
    Notifier,
    ReconcileStats,
    SessionFactory,
    _error_text,
    apply_commands,
    default_session_factory,
    log_claim,
    process_incident,
    reconcile,
    triaged_since,
)

if TYPE_CHECKING:
    from dag_healer.agent.llm import LLMProvider
    from dag_healer.airflow.client import AirflowClient
    from dag_healer.config import HealerConfig

logger = logging.getLogger(__name__)

RATE_WINDOW = timedelta(hours=1)
#: How often the "rate limit reached" warning is repeated while it lasts.
RATE_LOG_EVERY = timedelta(minutes=10)
#: Safety cap on the incidents processed in one iteration.
MAX_INCIDENTS_PER_ITERATION = 100


@dataclass
class IterationStats:
    """What one iteration of the loop did."""

    started_at: datetime
    released: int = 0
    commands: int = 0
    commands_failed: int = 0
    reconcile: ReconcileStats | None = None
    processed: list[tuple[str, str]] = field(default_factory=list)
    rate_limited: int = 0
    errors: list[str] = field(default_factory=list)


@dataclass
class WorkerStats:
    """Totals over the life of :func:`run_worker`."""

    iterations: int = 0
    created: int = 0
    processed: dict[str, int] = field(default_factory=dict)
    commands: int = 0
    commands_failed: int = 0
    errors: list[str] = field(default_factory=list)
    last: IterationStats | None = None

    def add(self, iteration: IterationStats) -> None:
        """Fold one iteration into the totals."""
        self.iterations += 1
        self.last = iteration
        if iteration.reconcile is not None:
            self.created += len(iteration.reconcile.created)
        for _incident_id, status in iteration.processed:
            self.processed[status] = self.processed.get(status, 0) + 1
        self.commands += iteration.commands
        self.commands_failed += iteration.commands_failed
        self.errors.extend(iteration.errors)


def _masked_url(url: str) -> str:
    try:
        from sqlalchemy.engine import make_url

        return make_url(url).render_as_string(hide_password=True)
    except Exception:  # pragma: no cover - unparsable URL
        return "<store url>"


class Worker:
    """The polling loop; see the module docstring. :func:`run_worker` is the usual entry point."""

    def __init__(
        self,
        cfg: HealerConfig,
        *,
        provider: LLMProvider | None,
        client: AirflowClient | Any | None,
        session_factory: SessionFactory | None = None,
        notifier: Notifier | None = None,
        workspace_root: Path | None = None,
        sleep: Callable[[float], Any] = time.sleep,
        clock: Callable[[], datetime] = utcnow,
    ) -> None:
        self.cfg = cfg
        self.provider = provider
        self.client = client
        self.sf = session_factory or default_session_factory(cfg)
        self.notifier = notifier
        self.workspace_root = workspace_root
        self.sleep = sleep
        self.clock = clock
        self.stats = WorkerStats()
        self._stop = threading.Event()
        self._last_rate_log: datetime | None = None

    # ------------------------------------------------------------------ control
    def stop(self) -> None:
        """Ask the loop to stop after the current step."""
        self._stop.set()

    @property
    def stopping(self) -> bool:
        """True once a stop was requested."""
        return self._stop.is_set()

    def _on_signal(self, signum: int, _frame: FrameType | None) -> None:
        if self._stop.is_set():
            raise KeyboardInterrupt
        try:
            name = signal.Signals(signum).name
        except ValueError:
            name = str(signum)
        logger.info("dag-healer worker: received %s, stopping after the current step", name)
        self._stop.set()

    def _install_signal_handlers(self) -> dict[int, Any]:
        previous: dict[int, Any] = {}
        if threading.current_thread() is not threading.main_thread():
            return previous
        for name in ("SIGTERM", "SIGINT"):
            signum = getattr(signal, name, None)
            if signum is None:
                continue
            try:
                previous[signum] = signal.signal(signum, self._on_signal)
            except (ValueError, OSError):  # pragma: no cover - platform restrictions
                continue
        return previous

    @staticmethod
    def _restore_signal_handlers(previous: dict[int, Any]) -> None:
        for signum, handler in previous.items():
            try:
                signal.signal(signum, handler)
            except (ValueError, OSError, TypeError):  # pragma: no cover
                continue

    # ------------------------------------------------------------------ claiming
    def _waiting_new(self) -> int:
        with self.sf() as session:
            query = select(func.count()).select_from(Incident).where(Incident.status == NEW)
            return int(session.scalar(query) or 0)

    def claim(self, iteration: IterationStats) -> str | None:
        """Claim the next incident (resumed first, then new within the hourly limit); its id."""
        now = self.clock()
        with self.sf() as session:
            incident = store_repo.claim_next(session, (RESUMED,), now=now)
            if incident is not None:
                log_claim(incident, RESUMED)
                return incident.id
            limit = self.cfg.limits.max_incidents_per_hour
            used = triaged_since(session, now - RATE_WINDOW)
            if used >= limit:
                rate_limited = True
            else:
                rate_limited = False
                incident = store_repo.claim_next(session, (NEW,), now=now)
                if incident is not None:
                    store_repo.add_event(
                        session,
                        incident,
                        TRIAGE_EVENT,
                        f"Automatic triage started ({used + 1} of {limit} this hour)",
                        data={"window_count": used + 1, "limit": limit},
                    )
                    log_claim(incident, NEW)
                    return incident.id
        if rate_limited:
            waiting = self._waiting_new()
            iteration.rate_limited = waiting
            if waiting and (self._last_rate_log is None or now - self._last_rate_log >= RATE_LOG_EVERY):
                self._last_rate_log = now
                logger.warning(
                    "dag-healer worker: rate limit reached (%d automatic triages in the last hour, "
                    "limits.max_incidents_per_hour=%d); %d new incident(s) wait",
                    used,
                    limit,
                    waiting,
                )
        return None

    # ------------------------------------------------------------------ one iteration
    def _phase(self, iteration: IterationStats, name: str, func: Callable[[], Any]) -> Any:
        try:
            return func()
        except Exception as exc:  # the store or Airflow is down: retried next iteration
            message = f"{name}: {_error_text(exc)}"
            iteration.errors.append(message)
            logger.error("dag-healer worker: %s", message)
            logger.debug("dag-healer worker: %s failed", name, exc_info=True)
            return None

    def _commands(self, iteration: IterationStats) -> None:
        outcomes = self._phase(
            iteration, "applying commands", lambda: apply_commands(self.cfg, self.sf, self.client)
        )
        for outcome in outcomes or []:
            iteration.commands += 1
            if outcome.status != "applied":
                iteration.commands_failed += 1

    def _release(self) -> int:
        with self.sf() as session:
            released = store_repo.release_stale_claims(
                session, older_than=STALE_CLAIM_AFTER, now=self.clock()
            )
            for incident in released:
                logger.warning(
                    "incident %s %s.%s: claim expired, queued for triage again",
                    incident.id[:8],
                    incident.dag_id,
                    incident.task_id,
                )
            return len(released)

    def run_iteration(self) -> IterationStats:
        """Release stale claims, apply commands, reconcile, then process the claimable incidents."""
        iteration = IterationStats(started_at=self.clock())
        iteration.released = self._phase(iteration, "releasing stale claims", self._release) or 0
        self._commands(iteration)
        if not self.stopping:
            iteration.reconcile = self._phase(
                iteration, "reconciling", lambda: reconcile(self.cfg, self.sf, self.client, now=self.clock())
            )
            if iteration.reconcile is not None:
                iteration.errors.extend(iteration.reconcile.errors)
        while not self.stopping and len(iteration.processed) < MAX_INCIDENTS_PER_ITERATION:
            incident_id = self._phase(iteration, "claiming an incident", lambda: self.claim(iteration))
            if incident_id is None:
                break
            status = process_incident(
                self.cfg,
                self.sf,
                incident_id,
                provider=self.provider,
                client=self.client,
                notifier=self.notifier,
                workspace_root=self.workspace_root,
            )
            iteration.processed.append((incident_id, status))
            if not self.stopping:
                self._commands(iteration)
        self.stats.add(iteration)
        return iteration

    def _wait(self, seconds: float) -> None:
        remaining = float(seconds)
        while remaining > 0 and not self.stopping:
            step = min(1.0, remaining)
            self.sleep(step)
            remaining -= step

    def run(
        self, *, once: bool = False, max_iterations: int | None = None, install_signal_handlers: bool = True
    ) -> WorkerStats:
        """Loop until stopped (or once / ``max_iterations`` times); returns the totals."""
        previous = self._install_signal_handlers() if install_signal_handlers else {}
        logger.info(
            "dag-healer worker started: store %s, Airflow %s, modes %s, LLM provider %s, poll every %ss",
            _masked_url(self.cfg.database_url),
            getattr(self.client, "base_url", None) or "not configured",
            ",".join(self.cfg.monitor.modes),
            getattr(self.provider, "name", None) or "none",
            self.cfg.monitor.poll_interval_seconds,
        )
        try:
            while not self.stopping:
                iteration = self.run_iteration()
                if iteration.processed or iteration.commands:
                    logger.info(
                        "dag-healer worker: iteration done: %d incident(s) processed, %d command(s)",
                        len(iteration.processed),
                        iteration.commands,
                    )
                if once or (max_iterations is not None and self.stats.iterations >= max_iterations):
                    break
                self._wait(self.cfg.monitor.poll_interval_seconds)
        finally:
            self._restore_signal_handlers(previous)
            logger.info("dag-healer worker stopped after %d iteration(s)", self.stats.iterations)
        return self.stats


def _default_provider(cfg: HealerConfig) -> LLMProvider | None:
    from dag_healer.agent.llm import get_provider

    try:
        return get_provider(cfg)
    except Exception as exc:  # unknown provider name
        logger.error("dag-healer worker: no LLM provider: %s", _error_text(exc))
        return None


def _default_client(cfg: HealerConfig) -> AirflowClient | None:
    from dag_healer.airflow.client import AirflowClient

    try:
        client = AirflowClient.from_config(cfg)
    except Exception as exc:  # invalid api_url
        logger.error("dag-healer worker: no Airflow API client: %s", _error_text(exc))
        return None
    try:
        client.health()
    except Exception as exc:  # not fatal: the poll retries every iteration
        logger.warning(
            "dag-healer worker: the Airflow API at %s is not reachable yet: %s",
            client.base_url,
            _error_text(exc),
        )
    return client


def run_worker(
    cfg: HealerConfig,
    *,
    once: bool = False,
    provider: LLMProvider | None = None,
    client: AirflowClient | Any | None = None,
    sleep: Callable[[float], Any] = time.sleep,
    session_factory: SessionFactory | None = None,
    notifier: Notifier | None = None,
    workspace_root: Path | None = None,
    max_iterations: int | None = None,
    install_signal_handlers: bool = True,
) -> WorkerStats:
    """Run the worker loop (see the module docstring) and return its totals.

    ``provider`` defaults to :func:`dag_healer.agent.llm.get_provider` and ``client`` to
    :meth:`AirflowClient.from_config <dag_healer.airflow.client.AirflowClient.from_config>`
    (closed on exit when created here). ``once`` runs a single iteration. ``sleep``,
    ``session_factory`` and ``notifier`` are injection points for tests.
    """
    own_client = client is None
    if provider is None:
        provider = _default_provider(cfg)
    if client is None:
        client = _default_client(cfg)
    worker = Worker(
        cfg,
        provider=provider,
        client=client,
        session_factory=session_factory,
        notifier=notifier,
        workspace_root=workspace_root,
        sleep=sleep,
    )
    try:
        return worker.run(
            once=once, max_iterations=max_iterations, install_signal_handlers=install_signal_handlers
        )
    finally:
        if own_client and client is not None:
            close = getattr(client, "close", None)
            if callable(close):
                close()


__all__ = ["IterationStats", "Worker", "WorkerStats", "run_worker"]
