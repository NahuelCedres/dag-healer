"""Tests of ``dag-healer store stats|reset`` and the poll floor it records in ``healer_kv``."""

from __future__ import annotations

import re
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

import pytest

from dag_healer.cli.main import main
from dag_healer.config import HealerConfig, config_from_mapping
from dag_healer.store import dispose_engines, session_scope
from dag_healer.store import repo as store_repo
from dag_healer.store.models import Alert, Command, ContractState, Incident, IncidentEvent, PendingEvent
from dag_healer.worker import pipeline

NOW = datetime(2026, 10, 19, 12, 0, tzinfo=timezone.utc)


@pytest.fixture(autouse=True)
def _dispose() -> Any:
    yield
    dispose_engines()


@pytest.fixture
def config_file(tmp_path: Path, healer_env: Path) -> Path:
    repo = tmp_path / "repo"
    (repo / "dags").mkdir(parents=True)
    db = (tmp_path / "store.db").as_posix()
    path = repo / "healer.yaml"
    path.write_text(f"version: 1\nstore:\n  url: sqlite:///{db}\n", encoding="utf-8")
    return path


def _cfg(config_file: Path) -> HealerConfig:
    return config_from_mapping(
        {"repo": {"path": str(config_file.parent)}, "store": {"url": _url(config_file)}}
    )


def _url(config_file: Path) -> str:
    return f"sqlite:///{(config_file.parent.parent / 'store.db').as_posix()}"


def _populate(url: str) -> None:
    with session_scope(url) as session:
        incident, _ = store_repo.create_incident(
            session, dag_id="etl", run_id="r1", task_id="t", try_number=1, source="rest"
        )
        store_repo.add_event(session, incident, "triage", "started")
        store_repo.enqueue_command(
            session, kind="resolve", dag_id="etl", incident_id=incident.id, actor="ann"
        )
        store_repo.record_alert(
            session, incident_id=incident.id, channel="log", recipients=[], subject="s", status="sent"
        )
        store_repo.add_pending_event(session, dag_id="etl", run_id="r2", task_id="t", try_number=1)
        store_repo.upsert_contract_state(session, dag_id="etl", status="approved", path="c.yaml")


def _counts(url: str) -> dict[str, int]:
    with session_scope(url) as session:
        return {
            model.__tablename__: session.query(model).count()
            for model in (Incident, IncidentEvent, Command, Alert, PendingEvent, ContractState)
        }


def test_reset_requires_yes(config_file: Path, capsys: pytest.CaptureFixture[str]) -> None:
    _populate(_url(config_file))
    assert main(["-c", str(config_file), "store", "reset"]) == 2
    assert "--yes" in capsys.readouterr().err
    assert _counts(_url(config_file))["healer_incidents"] == 1


def test_reset_empties_every_table_and_restarts_the_rest_poll(
    config_file: Path, healer_env: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    url = _url(config_file)
    _populate(url)
    cfg = _cfg(config_file)
    pipeline.set_watermark(cfg, NOW - timedelta(days=3))
    (healer_env / "prs" / "abc").mkdir(parents=True)
    (healer_env / "prs" / "abc" / "PR.md").write_text("x", encoding="utf-8")
    (healer_env / "work" / "abc").mkdir(parents=True)

    assert main(["-c", str(config_file), "store", "stats"]) == 0
    assert re.search(r"(?m)^healer_incidents +1$", capsys.readouterr().out)

    assert main(["-c", str(config_file), "store", "reset", "--yes", "--artifacts"]) == 0
    out = capsys.readouterr().out
    assert "reset: 8 row(s) deleted" in out  # the 7 rows above and the poll cursor
    assert "healer_kv: 1" in out
    assert "(healer_kv cursor:airflow): earlier failures are not picked up again" in out
    assert set(_counts(url).values()) == {0}
    assert not (healer_env / "prs").exists() and not (healer_env / "work").exists()

    floor = pipeline.get_not_before(cfg)
    assert floor is not None and abs(datetime.now(timezone.utc) - floor) < timedelta(minutes=1)
    assert pipeline.get_watermark(cfg) == floor


def test_reset_keep_state_leaves_the_watermark(config_file: Path, healer_env: Path) -> None:
    cfg = _cfg(config_file)
    pipeline.set_watermark(cfg, NOW)
    assert main(["-c", str(config_file), "store", "reset", "--yes", "--keep-state"]) == 0
    assert pipeline.get_watermark(cfg) == NOW
    assert pipeline.get_not_before(cfg) is None
    with session_scope(_url(config_file)) as session:
        cursor = store_repo.kv_get(session, store_repo.cursor_key("airflow"))
    assert cursor == {"watermark": NOW.isoformat(), "not_before": None}
    assert not (healer_env / "state.json").exists()  # the poll position lives in healer_kv


def test_reset_works_without_a_config_file(
    tmp_path: Path, healer_env: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    url = f"sqlite:///{(tmp_path / 'env.db').as_posix()}"
    monkeypatch.setenv("HEALER_DATABASE_URL", url)
    monkeypatch.setenv("HEALER_CONFIG", str(tmp_path / "missing" / "healer.yaml"))
    _populate(url)
    assert main(["store", "reset", "--yes"]) == 0
    assert set(_counts(url).values()) == {0}


class _ListingClient:
    def __init__(self) -> None:
        self.since: list[datetime | None] = []

    def list_failed_task_instances(self, *, updated_since: datetime | None = None, **_: Any) -> list[Any]:
        self.since.append(updated_since)
        return []


def test_rest_poll_never_looks_before_the_reset_floor(config_file: Path) -> None:
    cfg = _cfg(config_file)
    client = _ListingClient()
    pipeline.reset_state(cfg, NOW)

    pipeline.reconcile(cfg, None, client, now=NOW + timedelta(seconds=10))
    pipeline.reconcile(cfg, None, client, now=NOW + timedelta(seconds=20))
    pipeline.reconcile(cfg, None, client, now=NOW + timedelta(minutes=5))
    pipeline.reconcile(cfg, None, client, now=NOW + timedelta(minutes=10))

    # the usual 60 s overlap would reach back before the reset; the floor clamps it
    assert client.since == [NOW, NOW, NOW, NOW + timedelta(minutes=4)]
