"""Tests for dag_healer.worker.loop (run_worker / Worker), the worker CLI and worker plumbing.

Incidents are created directly in a SQLite store; processing runs the real pipeline on an
empty repository (no DAGs, no contracts), which ends quickly in ``waiting_human``. The
orchestrator is a small Airflow REST client double wrapped in the Airflow adapter.
"""

from __future__ import annotations

import os
import re
import signal
from collections.abc import Iterator
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

import pytest

from dag_healer.agent.fake import FakeProvider
from dag_healer.agent.llm import AnthropicProvider
from dag_healer.airflow.adapter import AirflowAdapter
from dag_healer.airflow.client import AirflowAPIError, TaskInstanceInfo
from dag_healer.cli.main import main
from dag_healer.config import HealerConfig, config_from_mapping
from dag_healer.orchestrators import registry
from dag_healer.orchestrators.compat import as_adapter
from dag_healer.orchestrators.fake import FakeAdapter, FakeWorld
from dag_healer.store import dispose_engines, session_scope
from dag_healer.store import repo as store_repo
from dag_healer.store.db import _postgres_connect_args
from dag_healer.store.models import Incident
from dag_healer.worker import loop as worker_loop
from dag_healer.worker.loop import Worker, WorkerStartError, instance_key, run_worker
from dag_healer.worker.pipeline import get_watermark

RUN = "manual__2026-10-17T10:00:00+00:00"


class QuietClient:
    """Airflow stand-in: no failed task instances, optional listing error."""

    base_url = "http://airflow.test"

    def __init__(self, *, list_error: Exception | None = None) -> None:
        self.list_error = list_error
        self.list_calls = 0
        self.closed = False

    def list_failed_task_instances(self, **_: Any) -> list[TaskInstanceInfo]:
        self.list_calls += 1
        if self.list_error is not None:
            raise self.list_error
        return []

    def get_task_instance(self, *_: Any, **__: Any) -> TaskInstanceInfo:
        raise AirflowAPIError("not found", status_code=404)

    def get_task_log(self, *_: Any, **__: Any) -> str:
        return ""

    def get_dag(self, dag_id: str) -> dict[str, Any]:
        return {"dag_id": dag_id}

    def get_dag_source(self, dag_id: str) -> str | None:
        return None

    def health(self) -> dict[str, Any]:
        return {"metadatabase": {"status": "healthy"}}

    def close(self) -> None:
        self.closed = True


class RecordingAdapter(AirflowAdapter):
    """The Airflow adapter on a :class:`QuietClient`, counting its ``close()`` calls."""

    def __init__(self, cfg: HealerConfig, client: Any | None = None) -> None:
        super().__init__(cfg, client=client or QuietClient())
        self.closes = 0

    def close(self) -> None:
        self.closes += 1
        super().close()


@pytest.fixture(autouse=True)
def _dispose_engines() -> Iterator[None]:
    yield
    dispose_engines()
    registry.reset_adapters()


def make_cfg(tmp_path: Path, **sections: Any) -> HealerConfig:
    repo = tmp_path / "repo"
    (repo / "dags").mkdir(parents=True, exist_ok=True)
    data: dict[str, Any] = {
        "repo": {"path": str(repo)},
        "store": {"url": f"sqlite:///{(tmp_path / 'store.db').as_posix()}"},
        "monitor": {"poll_interval_seconds": 3},
    }
    data.update(sections)
    return config_from_mapping(data)


@pytest.fixture
def cfg(tmp_path: Path, healer_env: Path) -> HealerConfig:
    return make_cfg(tmp_path)


def add_incidents(cfg: HealerConfig, count: int, *, status: str = "new", prefix: str = "t") -> list[str]:
    ids: list[str] = []
    with session_scope(cfg.database_url) as session:
        for index in range(count):
            incident, _created = store_repo.create_incident(
                session,
                dag_id="etl",
                run_id=RUN,
                task_id=f"{prefix}{index}",
                try_number=1,
                source="rest",
                error_summary="the job died",
            )
            if status != "new":
                store_repo.set_status(session, incident, status, f"{status} for the test")
            ids.append(incident.id)
    return ids


def statuses(cfg: HealerConfig) -> dict[str, str]:
    with session_scope(cfg.database_url) as session:
        return {incident.task_id: incident.status for incident in session.query(Incident).all()}


def worker(cfg: HealerConfig, client: Any | None = None, **kwargs: Any) -> Worker:
    return Worker(cfg, provider=FakeProvider(cfg), client=client or QuietClient(), **kwargs)


# --------------------------------------------------------------------------- iterations


def test_run_worker_once_on_an_empty_store(cfg: HealerConfig) -> None:
    client = QuietClient()
    stats = run_worker(
        cfg, once=True, provider=FakeProvider(cfg), client=client, install_signal_handlers=False
    )
    assert (stats.iterations, stats.created, stats.processed, stats.errors) == (1, 0, {}, [])
    assert client.list_calls == 1
    assert get_watermark(cfg) is not None
    assert client.closed is False  # a client passed in is not closed by the worker


def test_incidents_are_processed_one_by_one(cfg: HealerConfig) -> None:
    add_incidents(cfg, 3)
    stats = run_worker(
        cfg, once=True, provider=FakeProvider(cfg), client=QuietClient(), install_signal_handlers=False
    )
    assert stats.processed == {"waiting_human": 3}
    assert set(statuses(cfg).values()) == {"waiting_human"}


def test_each_transition_is_logged_once(cfg: HealerConfig, caplog: Any) -> None:
    (incident_id,) = add_incidents(cfg, 1)
    with caplog.at_level("INFO", logger="dag_healer.worker"):
        worker(cfg).run_iteration()
    lines = [
        r.getMessage() for r in caplog.records if r.getMessage().startswith(f"incident {incident_id[:8]}")
    ]
    transitions = [re.search(r": (\w+) -> (\w+)", line).groups() for line in lines if " -> " in line]  # type: ignore[union-attr]
    assert transitions == [("new", "triaging"), ("triaging", "classified"), ("classified", "waiting_human")]


def test_rate_limit_leaves_new_incidents_waiting(tmp_path: Path, healer_env: Path, caplog: Any) -> None:
    cfg = make_cfg(tmp_path, limits={"max_incidents_per_hour": 2})
    add_incidents(cfg, 4)
    clock = {"now": datetime.now(timezone.utc)}
    runner = worker(cfg, clock=lambda: clock["now"])

    with caplog.at_level("WARNING", logger="dag_healer.worker.loop"):
        first = runner.run_iteration()
    assert len(first.processed) == 2 and first.rate_limited == 2
    assert sorted(statuses(cfg).values()) == ["new", "new", "waiting_human", "waiting_human"]
    assert "rate limit reached" in caplog.text

    # a human-resumed incident is processed even while the limit is reached
    (resumed,) = add_incidents(cfg, 1, status="resumed", prefix="resumed")
    second = runner.run_iteration()
    assert [incident_id for incident_id, _status in second.processed] == [resumed]
    assert statuses(cfg)["resumed0"] == "waiting_human"

    # once the hour has passed, the waiting incidents are triaged
    clock["now"] += timedelta(hours=1, minutes=1)
    third = runner.run_iteration()
    assert len(third.processed) == 2
    assert set(statuses(cfg).values()) == {"waiting_human"}


def test_stale_claims_are_released_and_processed_again(cfg: HealerConfig) -> None:
    (incident_id,) = add_incidents(cfg, 1)
    with session_scope(cfg.database_url) as session:
        incident = session.get(Incident, incident_id)
        assert incident is not None
        store_repo.set_status(session, incident, "repairing", "worker crashed mid-repair")
        incident.claimed_at = datetime.now(timezone.utc) - timedelta(hours=2)

    iteration = worker(cfg).run_iteration()

    assert iteration.released == 1
    assert iteration.processed == [(incident_id, "waiting_human")]


def test_a_failing_phase_or_incident_never_stops_the_iteration(
    cfg: HealerConfig, monkeypatch: pytest.MonkeyPatch
) -> None:
    broken, healthy = add_incidents(cfg, 2)
    from dag_healer.agent import context

    real_gather = context.gather_context

    def gather(cfg_: Any, incident: dict[str, Any], client: Any, **kwargs: Any) -> Any:
        if incident["id"] == broken:
            raise OSError("disk full while reading /data/x.csv")
        return real_gather(cfg_, incident, client, **kwargs)

    monkeypatch.setattr(context, "gather_context", gather)
    client = QuietClient(list_error=AirflowAPIError("connection refused"))

    iteration = worker(cfg, client).run_iteration()

    assert dict(iteration.processed) == {broken: "error", healthy: "waiting_human"}
    assert any("listing failed task instances" in error for error in iteration.errors)


def test_waiting_between_iterations_uses_small_sleeps_and_honours_stop(cfg: HealerConfig) -> None:
    sleeps: list[float] = []
    runner: Worker | None = None

    def fake_sleep(seconds: float) -> None:
        sleeps.append(seconds)
        if len(sleeps) == 5 and runner is not None:
            runner.stop()

    runner = worker(cfg, sleep=fake_sleep)
    stats = runner.run(max_iterations=10, install_signal_handlers=False)

    assert sleeps == [1.0, 1.0, 1.0, 1.0, 1.0]  # 3 s poll interval: one full wait, then stopped
    assert stats.iterations == 2


def test_max_iterations(cfg: HealerConfig) -> None:
    sleeps: list[float] = []
    stats = run_worker(
        cfg,
        provider=FakeProvider(cfg),
        client=QuietClient(),
        sleep=sleeps.append,
        max_iterations=2,
        install_signal_handlers=False,
    )
    assert stats.iterations == 2 and sum(sleeps) == 3.0


def test_signal_handlers_stop_gracefully_then_interrupt(cfg: HealerConfig) -> None:
    runner = worker(cfg)
    previous = runner._install_signal_handlers()
    try:
        assert signal.getsignal(signal.SIGINT) == runner._on_signal
        runner._on_signal(signal.SIGTERM, None)
        assert runner.stopping
        with pytest.raises(KeyboardInterrupt):
            runner._on_signal(signal.SIGTERM, None)
    finally:
        runner._restore_signal_handlers(previous)
    assert signal.getsignal(signal.SIGINT) == previous[signal.SIGINT]


def test_run_worker_builds_and_closes_its_own_adapter(
    cfg: HealerConfig, monkeypatch: pytest.MonkeyPatch
) -> None:
    created: list[RecordingAdapter] = []

    def factory(cfg_: Any) -> RecordingAdapter:
        created.append(RecordingAdapter(cfg_))
        return created[-1]

    monkeypatch.setattr(worker_loop, "_default_adapter", factory)
    monkeypatch.setenv("HEALER_LLM_PROVIDER", "fake")
    stats = run_worker(cfg, once=True, install_signal_handlers=False)
    assert stats.errors == []
    assert len(created) == 1 and created[0].closes == 1


def test_an_injected_client_or_adapter_is_never_closed(cfg: HealerConfig) -> None:
    client = QuietClient()
    run_worker(cfg, once=True, provider=FakeProvider(cfg), client=client, install_signal_handlers=False)
    adapter = RecordingAdapter(cfg)
    stats = run_worker(
        cfg, once=True, provider=FakeProvider(cfg), adapter=adapter, install_signal_handlers=False
    )
    assert stats.errors == [] and adapter.closes == 0 and client.closed is False


def test_the_default_adapter_is_a_new_one_and_its_health_is_only_a_warning(
    tmp_path: Path, healer_env: Path, caplog: Any
) -> None:
    cfg = make_cfg(
        tmp_path, orchestrator={"type": "fake", "options": {"world": str(tmp_path / "missing.yaml")}}
    )
    with caplog.at_level("WARNING", logger="dag_healer.worker.loop"):
        adapter = worker_loop._default_adapter(cfg)
    assert isinstance(adapter, FakeAdapter) and adapter is not registry.get_adapter(cfg)
    assert "dag-healer worker: Fakeflow is not reachable yet" in caplog.text
    unknown = make_cfg(tmp_path, orchestrator={"type": "no_such_package.adapter:FACTORY", "instance": "x"})
    with pytest.raises(WorkerStartError, match="cannot be loaded.*no_such_package"):
        worker_loop._default_adapter(unknown)


def test_a_mode_the_adapter_cannot_serve_refuses_to_start(tmp_path: Path, healer_env: Path) -> None:
    cfg = make_cfg(tmp_path, monitor={"modes": ["rest", "queue"]})
    client = QuietClient()
    with pytest.raises(WorkerStartError) as raised:
        run_worker(cfg, once=True, provider=FakeProvider(cfg), client=client, install_signal_handlers=False)
    assert str(raised.value) == (
        "the worker cannot start: monitor.modes includes queue, but Apache Airflow has no hint queue to drain"
    )
    assert client.list_calls == 0


def test_an_instance_id_of_another_orchestrator_type_refuses_to_start(cfg: HealerConfig) -> None:
    run_worker(
        cfg, once=True, provider=FakeProvider(cfg), client=QuietClient(), install_signal_handlers=False
    )
    with session_scope(cfg.database_url) as session:
        assert store_repo.kv_get(session, instance_key("airflow")) == {"type": "airflow"}
    impostor = FakeAdapter(cfg, world=FakeWorld(instance="airflow"))
    with pytest.raises(
        WorkerStartError, match="'airflow' already belongs to an orchestrator of type 'airflow'"
    ):
        run_worker(
            cfg, once=True, provider=FakeProvider(cfg), adapter=impostor, install_signal_handlers=False
        )
    assert impostor.world.io_calls == 0  # refused before the first poll


def test_the_worker_serves_only_its_own_instance(cfg: HealerConfig) -> None:
    (own,) = add_incidents(cfg, 1)
    with session_scope(cfg.database_url) as session:
        other, _created = store_repo.create_incident(
            session,
            dag_id="etl",
            run_id=RUN,
            task_id="x",
            try_number=1,
            source="cli",
            orchestrator="airflow-2",
        )
        other_id = other.id
        store_repo.enqueue_command(
            session,
            kind="retry_repair",
            dag_id="etl",
            incident_id=other_id,
            actor="ann",
            orchestrator="airflow-2",
        )
    iteration = worker(cfg).run_iteration()
    assert iteration.processed == [(own, "waiting_human")] and iteration.commands == 0
    with session_scope(cfg.database_url) as session:
        assert store_repo.get_incident(session, other_id).status == "new"  # type: ignore[union-attr]
        assert [c.status for c in store_repo.pending_commands(session)] == ["pending"]


# --------------------------------------------------------------------------- CLI


@pytest.fixture
def config_file(tmp_path: Path, healer_env: Path) -> Path:
    repo = tmp_path / "repo"
    (repo / "dags").mkdir(parents=True)
    path = repo / "healer.yaml"
    path.write_text(
        "version: 1\n"
        "repo: {path: .}\n"
        f"store: {{url: 'sqlite:///{(tmp_path / 'cli.db').as_posix()}'}}\n"
        "llm: {provider: anthropic}\n",
        encoding="utf-8",
    )
    return path


def test_worker_cli_once_with_the_fake_provider(
    config_file: Path, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    providers: list[Any] = []
    real_run = worker_loop.run_worker

    def spy(cfg: Any, **kwargs: Any) -> Any:
        providers.append(kwargs["provider"])
        return real_run(cfg, install_signal_handlers=False, **kwargs)

    monkeypatch.setattr(worker_loop, "_default_adapter", lambda cfg_: as_adapter(cfg_, QuietClient()))
    monkeypatch.setattr(worker_loop, "run_worker", spy)
    cfg = config_from_mapping(
        {"store": {"url": f"sqlite:///{(config_file.parent.parent / 'cli.db').as_posix()}"}}
    )
    with session_scope(cfg.database_url) as session:
        store_repo.create_incident(session, dag_id="etl", run_id=RUN, task_id="t", try_number=1, source="cli")

    assert main(["-c", str(config_file), "worker", "--once", "--provider", "fake"]) == 0

    assert isinstance(providers[0], FakeProvider)
    out = capsys.readouterr().out
    assert "0 incident(s) created, 1 processed (waiting_human: 1)" in out


def test_worker_cli_once_reports_errors(
    config_file: Path, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    monkeypatch.setattr(
        worker_loop,
        "_default_adapter",
        lambda cfg_: as_adapter(cfg_, QuietClient(list_error=AirflowAPIError("down", 503))),
    )
    monkeypatch.setattr(worker_loop.Worker, "_install_signal_handlers", lambda self: {})
    assert main(["-c", str(config_file), "worker", "--once"]) == 1
    assert (
        "listing failed task instances: AirflowAPIError: Airflow API error 503: down"
        in capsys.readouterr().err
    )


def test_build_provider_names(cfg: HealerConfig, monkeypatch: pytest.MonkeyPatch) -> None:
    from dag_healer.cli.worker_cmd import build_provider

    assert isinstance(build_provider(cfg, "fake"), FakeProvider)
    assert isinstance(build_provider(cfg, "anthropic"), AnthropicProvider)
    monkeypatch.setenv("HEALER_LLM_PROVIDER", "fake")
    assert isinstance(build_provider(cfg, None), FakeProvider)
    monkeypatch.setenv("HEALER_LLM_PROVIDER", "nope")
    with pytest.raises(ValueError):
        build_provider(cfg, None)


# --------------------------------------------------------------------------- plumbing


@pytest.mark.parametrize(
    ("url", "expected"),
    [
        ("postgresql+psycopg2://u:p@db/healer", {"connect_timeout": 5}),
        ("postgresql://u:p@db/healer", {"connect_timeout": 5}),
        ("postgresql+psycopg://u:p@db/healer", {"connect_timeout": 5}),
        ("postgresql+pg8000://u:p@db/healer", {"timeout": 5}),
        ("postgresql+psycopg2://u:p@db/healer?connect_timeout=20", {}),
        ("sqlite:///x.db", {}),
    ],
)
def test_postgres_store_connections_time_out(url: str, expected: dict[str, int]) -> None:
    assert _postgres_connect_args(url) == expected


def test_healer_env_restores_variables_written_directly_part_1(healer_env: Path) -> None:
    # what main(["-c", path, ...]) does: a direct write that monkeypatch did not make
    os.environ["HEALER_CONFIG"] = "/tmp/leaked-healer.yaml"
    os.environ["HEALER_LLM_PROVIDER"] = "fake"


def test_healer_env_restores_variables_written_directly_part_2(healer_env: Path) -> None:
    assert "HEALER_CONFIG" not in os.environ
    assert "HEALER_LLM_PROVIDER" not in os.environ
