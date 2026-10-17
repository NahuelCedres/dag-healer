"""Tests for dag_healer.store: engine/session helpers, models and repository functions."""

from __future__ import annotations

import json
import threading
from collections.abc import Iterator
from datetime import datetime, timedelta, timezone
from pathlib import Path

import pytest
from sqlalchemy import create_engine, inspect, select, text
from sqlalchemy.orm import Session

from dag_healer.store import db, models, repo
from dag_healer.store.models import (
    NEW,
    REPORTED,
    RESUMED,
    TRIAGING,
    WAITING_HUMAN,
    Incident,
    IncidentEvent,
    SchemaVersion,
)

T0 = datetime(2026, 10, 1, 12, 0, tzinfo=timezone.utc)


@pytest.fixture(autouse=True)
def _fresh_engines() -> Iterator[None]:
    db.dispose_engines()
    yield
    db.dispose_engines()


@pytest.fixture
def db_url(tmp_path: Path) -> str:
    return f"sqlite:///{(tmp_path / 'store' / 'healer.db').as_posix()}"


@pytest.fixture
def session(db_url: str) -> Iterator[Session]:
    with db.session_scope(db_url) as s:
        yield s


def make_incident(session: Session, **overrides) -> Incident:
    fields = {
        "dag_id": "sales_daily_etl",
        "run_id": "manual__1",
        "task_id": "load",
        "try_number": 1,
        "source": "rest",
        "error_summary": "KeyError: 'amount'",
    }
    fields.update(overrides)
    incident, created = repo.create_incident(session, **fields)
    assert created
    return incident


# --------------------------------------------------------------------------- engine / schema
def test_get_engine_is_cached_creates_parent_dir_and_uses_wal(db_url: str, tmp_path: Path) -> None:
    engine = db.get_engine(db_url)
    assert db.get_engine(db_url) is engine
    with engine.connect() as conn:
        assert conn.execute(text("PRAGMA journal_mode")).scalar() == "wal"
        assert conn.execute(text("PRAGMA foreign_keys")).scalar() == 1
    assert (tmp_path / "store").is_dir()


def test_init_db_creates_tables_and_schema_version_idempotently(db_url: str) -> None:
    engine = db.get_engine(db_url)
    db.init_db(engine)
    db.init_db(engine)
    tables = set(inspect(engine).get_table_names())
    assert {
        "healer_incidents",
        "healer_incident_events",
        "healer_contracts",
        "healer_alerts",
        "healer_commands",
        "healer_pending_events",
        "healer_schema_version",
    } <= tables
    with Session(engine) as s:
        assert s.scalars(select(SchemaVersion.version)).all() == [models.SCHEMA_VERSION]


def test_in_memory_sqlite_is_shared_between_sessions() -> None:
    with db.session_scope("sqlite://") as s:
        make_incident(s)
    with db.session_scope("sqlite://") as s:
        assert len(repo.list_incidents(s)) == 1


def test_session_scope_rolls_back_on_error(db_url: str) -> None:
    with pytest.raises(RuntimeError), db.session_scope(db_url) as s:
        make_incident(s)
        raise RuntimeError("boom")
    with db.session_scope(db_url) as s:
        assert repo.list_incidents(s) == []


def test_resolve_database_url_order(
    healer_env: Path, monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    monkeypatch.chdir(tmp_path)
    assert db.resolve_database_url("sqlite:///explicit.db") == "sqlite:///explicit.db"
    expected_default = f"sqlite:///{(healer_env / 'healer.db').resolve().as_posix()}"
    assert db.resolve_database_url() == expected_default
    monkeypatch.setenv("HEALER_DATABASE_URL", "sqlite:///from-env.db")
    assert db.resolve_database_url() == "sqlite:///from-env.db"


def test_session_scope_without_url_uses_config_default(
    healer_env: Path, monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    monkeypatch.chdir(tmp_path)
    with db.session_scope() as s:
        make_incident(s)
    assert (healer_env / "healer.db").is_file()


# --------------------------------------------------------------------------- incidents
def test_create_incident_is_idempotent_on_the_task_instance_key(session: Session) -> None:
    first, created = repo.create_incident(
        session, dag_id="d", run_id="r", task_id="t", try_number=1, source="listener", error_summary="boom"
    )
    assert created
    assert len(first.id) == 32
    assert first.status == NEW and first.map_index == -1 and first.occurrences == 1
    again, created_again = repo.create_incident(
        session, dag_id="d", run_id="r", task_id="t", try_number=1, source="rest"
    )
    assert again is first and not created_again
    other_try, created_other = repo.create_incident(
        session, dag_id="d", run_id="r", task_id="t", try_number=2, source="rest"
    )
    assert created_other and other_try.id != first.id
    mapped, created_mapped = repo.create_incident(
        session, dag_id="d", run_id="r", task_id="t", map_index=3, try_number=1, source="rest"
    )
    assert created_mapped and mapped.map_index == 3
    kinds = [event.kind for event in repo.list_events(session, first.id)]
    assert kinds == ["created"]


def test_create_incident_is_idempotent_across_sessions(db_url: str) -> None:
    with db.session_scope(db_url) as s:
        incident_id = make_incident(s).id
    with db.session_scope(db_url) as s:
        incident, created = repo.create_incident(
            s, dag_id="sales_daily_etl", run_id="manual__1", task_id="load", try_number=1, source="cli"
        )
        assert not created and incident.id == incident_id


def test_create_incident_recovers_from_a_concurrent_insert(
    session: Session, monkeypatch: pytest.MonkeyPatch
) -> None:
    original = make_incident(session)
    session.commit()
    real_lookup = repo.get_incident_by_key
    calls = {"n": 0}

    def stale_lookup(s: Session, **key):
        calls["n"] += 1
        return None if calls["n"] == 1 else real_lookup(s, **key)

    monkeypatch.setattr(repo, "get_incident_by_key", stale_lookup)
    incident, created = repo.create_incident(
        session, dag_id="sales_daily_etl", run_id="manual__1", task_id="load", try_number=1, source="rest"
    )
    assert not created and incident.id == original.id
    assert len(repo.list_incidents(session)) == 1


def test_create_incident_validates_arguments(session: Session) -> None:
    with pytest.raises(ValueError, match="source"):
        repo.create_incident(session, dag_id="d", run_id="r", task_id="t", try_number=1, source="pigeon")
    with pytest.raises(TypeError, match="colour"):
        repo.create_incident(
            session, dag_id="d", run_id="r", task_id="t", try_number=1, source="rest", colour=1
        )


def test_create_incident_accepts_extra_columns(session: Session) -> None:
    incident = make_incident(
        session, status=WAITING_HUMAN, category="ambiguous", confidence=0.4, created_at=T0, sample=[{"a": 1}]
    )
    assert incident.status == WAITING_HUMAN
    assert incident.created_at == T0
    session.commit()
    reloaded = session.get(Incident, incident.id, populate_existing=True)
    assert reloaded is not None
    assert reloaded.sample == [{"a": 1}]
    assert reloaded.created_at == T0
    assert reloaded.created_at.tzinfo is not None


def test_signature_dedupe_within_24_hours(session: Session) -> None:
    base = dict(dag_id="d", task_id="t", source="rest", signature="sig-1")
    first, created = repo.create_incident(session, run_id="r1", try_number=1, now=T0, **base)
    assert created

    dup, created = repo.create_incident(
        session, run_id="r2", try_number=1, now=T0 + timedelta(hours=23), **base
    )
    assert not created and dup is first and first.occurrences == 2

    # The same task instance try seen twice does not inflate the counter.
    dup, created = repo.create_incident(
        session, run_id="r2", try_number=1, now=T0 + timedelta(hours=23), **base
    )
    assert not created and first.occurrences == 2

    later, created = repo.create_incident(
        session, run_id="r3", try_number=1, now=T0 + timedelta(hours=25), **base
    )
    assert created and later.id != first.id

    other_sig, created = repo.create_incident(
        session, run_id="r4", try_number=1, now=T0 + timedelta(hours=1), **{**base, "signature": "sig-2"}
    )
    assert created

    occurrence_events = [e for e in repo.list_events(session, first.id) if e.kind == "occurrence"]
    assert len(occurrence_events) == 1
    assert occurrence_events[0].data["run_id"] == "r2"


def test_signature_dedupe_ignores_closed_incidents(session: Session) -> None:
    base = dict(dag_id="d", task_id="t", source="rest", signature="sig")
    first, _ = repo.create_incident(session, run_id="r1", try_number=1, now=T0, **base)
    repo.set_status(session, first, REPORTED, "Reported as a data issue")
    second, created = repo.create_incident(
        session, run_id="r2", try_number=1, now=T0 + timedelta(hours=1), **base
    )
    assert created and second.id != first.id


def test_get_incident_and_by_key(session: Session) -> None:
    incident = make_incident(session)
    assert repo.get_incident(session, incident.id) is incident
    assert repo.get_incident(session, "missing") is None
    found = repo.get_incident_by_key(
        session, dag_id="sales_daily_etl", run_id="manual__1", task_id="load", try_number=1
    )
    assert found is incident


def test_list_incidents_filters_and_orders_newest_first(session: Session) -> None:
    a = make_incident(session, dag_id="a", run_id="r1", created_at=T0)
    b = make_incident(session, dag_id="b", run_id="r1", created_at=T0 + timedelta(minutes=1))
    c = make_incident(session, dag_id="a", run_id="r2", created_at=T0 + timedelta(minutes=2), status=REPORTED)
    assert [i.id for i in repo.list_incidents(session)] == [c.id, b.id, a.id]
    assert [i.id for i in repo.list_incidents(session, dag_ids={"a"})] == [c.id, a.id]
    assert repo.list_incidents(session, dag_ids=set()) == []
    assert [i.id for i in repo.list_incidents(session, statuses=[NEW])] == [b.id, a.id]
    assert [i.id for i in repo.list_incidents(session, limit=1)] == [c.id]
    assert [i.id for i in repo.list_incidents(session, run_id="r2")] == [c.id]
    assert [i.id for i in repo.list_incidents(session, dag_ids={"b"}, task_id="load")] == [b.id]


def test_incidents_created_since(session: Session) -> None:
    for minutes in (0, 30, 90):
        make_incident(session, run_id=f"r{minutes}", created_at=T0 + timedelta(minutes=minutes))
    assert repo.incidents_created_since(session, T0) == 3
    assert repo.incidents_created_since(session, T0 + timedelta(minutes=30)) == 2
    assert repo.incidents_created_since(session, T0 + timedelta(hours=2)) == 0
    naive = (T0 + timedelta(minutes=60)).replace(tzinfo=None)
    assert repo.incidents_created_since(session, naive) == 1


# --------------------------------------------------------------------------- status / events
def test_set_status_writes_status_events(session: Session) -> None:
    incident = make_incident(session)
    event = repo.set_status(
        session, incident, WAITING_HUMAN, "Needs a human", actor="alice", data={"why": "x"}
    )
    assert incident.status == WAITING_HUMAN
    assert event.kind == "status"
    assert event.data == {"from": NEW, "to": WAITING_HUMAN, "why": "x"}
    assert event.actor == "alice"
    repo.add_event(session, incident, "note", "Looked at it", data={"n": 1})
    session.commit()
    events = repo.list_events(session, incident.id)
    assert [(e.kind, e.message) for e in events] == [
        ("created", events[0].message),
        ("status", "Needs a human"),
        ("note", "Looked at it"),
    ]
    assert all(e.at.tzinfo is not None for e in events)
    with pytest.raises(ValueError):
        repo.set_status(session, incident, "on_fire", "nope")


# --------------------------------------------------------------------------- claims
def test_claim_next_claims_oldest_first_and_records_an_event(session: Session) -> None:
    newer = make_incident(session, run_id="r2", created_at=T0 + timedelta(minutes=5))
    older = make_incident(session, run_id="r1", created_at=T0)
    claimed = repo.claim_next(session, now=T0 + timedelta(hours=1))
    assert claimed is older
    assert claimed.status == TRIAGING
    assert claimed.claimed_at == T0 + timedelta(hours=1)
    last = repo.list_events(session, older.id)[-1]
    assert (last.kind, last.data) == ("status", {"from": NEW, "to": TRIAGING})
    assert repo.claim_next(session) is newer
    assert repo.claim_next(session) is None


def test_claim_next_takes_resumed_incidents_and_custom_statuses(session: Session) -> None:
    incident = make_incident(session, status=RESUMED)
    waiting = make_incident(session, run_id="r9", status=WAITING_HUMAN)
    assert repo.claim_next(session) is incident
    assert repo.claim_next(session) is None
    assert repo.claim_next(session, statuses=(WAITING_HUMAN,)) is waiting


def test_claim_is_exclusive_across_engines(db_url: str) -> None:
    with db.session_scope(db_url) as s:
        incident_id = make_incident(s).id

    other_engine = create_engine(db_url, connect_args={"check_same_thread": False, "timeout": 30})
    db.init_db(other_engine)
    try:
        with Session(other_engine) as stale:
            # Worker B loaded the row while it was still new.
            assert stale.get(Incident, incident_id).status == NEW
            with db.session_scope(db_url) as worker_a:
                assert repo.claim_next(worker_a) is not None
            # B's conditional update must not win, even for the id it saw as claimable.
            assert repo.try_claim(stale, incident_id) is None
            assert repo.claim_next(stale) is None
            stale.commit()
    finally:
        other_engine.dispose()


def test_claim_next_concurrent_workers_never_share_an_incident(db_url: str) -> None:
    total = 30
    with db.session_scope(db_url) as s:
        for n in range(total):
            make_incident(s, run_id=f"r{n}", created_at=T0 + timedelta(seconds=n))

    engines = [
        create_engine(db_url, connect_args={"check_same_thread": False, "timeout": 30}) for _ in range(3)
    ]
    claimed: list[list[str]] = [[] for _ in engines]
    errors: list[BaseException] = []
    start = threading.Barrier(len(engines))

    def work(index: int) -> None:
        try:
            start.wait()
            while True:
                with Session(engines[index]) as s:
                    incident = repo.claim_next(s)
                    if incident is None:
                        s.commit()
                        return
                    claimed[index].append(incident.id)
                    s.commit()
        except BaseException as exc:  # pragma: no cover - reported below
            errors.append(exc)

    threads = [threading.Thread(target=work, args=(i,)) for i in range(len(engines))]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join(timeout=120)
    for engine in engines:
        engine.dispose()

    assert not errors, errors
    all_claimed = [incident_id for ids in claimed for incident_id in ids]
    assert len(all_claimed) == total
    assert len(set(all_claimed)) == total
    with db.session_scope(db_url) as s:
        assert {i.status for i in repo.list_incidents(s, limit=total)} == {TRIAGING}
        claim_events = s.scalars(
            select(IncidentEvent).where(
                IncidentEvent.kind == "status", IncidentEvent.message.like("Claimed%")
            )
        ).all()
        assert len(claim_events) == total


def test_release_stale_claims(session: Session) -> None:
    incident = make_incident(session)
    assert repo.claim_next(session, now=T0) is incident
    assert (
        repo.release_stale_claims(session, older_than=timedelta(hours=1), now=T0 + timedelta(minutes=30))
        == []
    )
    released = repo.release_stale_claims(session, older_than=timedelta(hours=1), now=T0 + timedelta(hours=2))
    assert released == [incident]
    assert incident.status == NEW and incident.claimed_at is None
    assert repo.claim_next(session) is incident


# --------------------------------------------------------------------------- commands
def test_command_queue(session: Session) -> None:
    incident = make_incident(session)
    first = repo.enqueue_command(
        session,
        kind="resolve",
        dag_id=incident.dag_id,
        incident_id=incident.id,
        payload={"resolution": "data"},
        actor="alice",
    )
    second = repo.enqueue_command(session, kind="approve_contract", dag_id="sales_daily_etl", actor="bob")
    assert first.status == "pending" and first.created_at.tzinfo is not None
    assert second.payload == {}
    assert [c.id for c in repo.pending_commands(session)] == [first.id, second.id]

    repo.mark_command(session, first, "applied")
    repo.mark_command(session, second, "failed", error="contract file missing")
    session.commit()
    assert repo.pending_commands(session) == []
    assert first.applied_at is not None and second.error == "contract file missing"

    with pytest.raises(ValueError):
        repo.enqueue_command(session, kind="launch_rockets", dag_id="d", actor="x")
    with pytest.raises(ValueError):
        repo.enqueue_command(session, kind="resolve", dag_id="d", actor="")
    with pytest.raises(ValueError):
        repo.mark_command(session, first, "maybe")


# --------------------------------------------------------------------------- pending events
def test_pending_events_lifecycle(session: Session) -> None:
    payload = {
        "dag_id": "d",
        "run_id": "r",
        "task_id": "t",
        "map_index": -1,
        "try_number": 2,
        "max_tries": 3,
        "state": "failed",
        "error": "boom",
        "source": "listener",
    }
    event = repo.add_pending_event(session, **payload)
    assert event.status == "pending_check"
    assert event.try_number == 2
    assert event.payload["max_tries"] == 3 and event.payload["error"] == "boom"
    assert event.received_at.tzinfo is not None

    duplicate = repo.add_pending_event(session, **{**payload, "source": "callback"})
    assert duplicate.id == event.id
    assert [e.id for e in repo.list_pending_events(session)] == [event.id]

    repo.mark_pending_event(session, event, "confirmed")
    session.commit()
    assert repo.list_pending_events(session) == []
    assert [e.id for e in repo.list_pending_events(session, "confirmed")] == [event.id]

    again = repo.add_pending_event(
        session, dag_id="d", run_id="r", task_id="t", try_number=2, payload={"x": 1}
    )
    assert again.id != event.id and again.payload == {"x": 1}

    defaults = repo.add_pending_event(
        session, dag_id="d", run_id="r", task_id="u", map_index=None, try_number=None
    )
    assert (defaults.map_index, defaults.try_number) == (-1, 1)

    with pytest.raises(ValueError, match="task_id"):
        repo.add_pending_event(session, dag_id="d", run_id="r")


# --------------------------------------------------------------------------- contracts / alerts
def test_upsert_contract_state(session: Session) -> None:
    state = repo.upsert_contract_state(
        session, dag_id="sales", status="draft", path="healer/contracts/sales.yaml"
    )
    assert state.approved_by is None
    approved_at = datetime(2026, 10, 2, 9, 30, tzinfo=timezone(timedelta(hours=-3)))
    updated = repo.upsert_contract_state(
        session,
        dag_id="sales",
        status="approved",
        path="healer/contracts/sales.yaml",
        approved_by="alice",
        approved_at=approved_at,
        source_hash="abc",
    )
    assert updated is state
    session.commit()
    reloaded = repo.get_contract_state(session, "sales")
    session.refresh(reloaded)
    assert reloaded.status == "approved" and reloaded.approved_by == "alice"
    assert reloaded.approved_at == approved_at
    assert reloaded.approved_at.utcoffset() == timedelta(0)
    assert [c.dag_id for c in repo.list_contract_states(session)] == ["sales"]


def test_record_alert(session: Session) -> None:
    incident = make_incident(session)
    alert = repo.record_alert(
        session,
        incident_id=incident.id,
        channel="email",
        recipients=("a@example.com",),
        subject="[dag-healer] reported",
        status="sent",
    )
    failed = repo.record_alert(
        session,
        incident_id=incident.id,
        channel="email",
        recipients=None,
        subject="s",
        status="failed",
        error="refused",
    )
    assert alert.recipients == ["a@example.com"] and alert.sent_at.tzinfo is not None
    assert [a.id for a in repo.list_alerts(session, incident.id)] == [alert.id, failed.id]


# --------------------------------------------------------------------------- serialisation
def test_to_dict_is_json_serialisable(session: Session) -> None:
    incident = make_incident(
        session,
        violated_rule={"rule": "missing_column", "column": "amount"},
        sample=[{"email": "h:0123456789", "amount": None}],
        evidence=["traceback line"],
        diff="--- a\n+++ b\n",
        confidence=0.9,
    )
    repo.set_status(session, incident, WAITING_HUMAN, "Waiting")
    command = repo.enqueue_command(session, kind="retry_task", dag_id="d", incident_id=incident.id, actor="a")
    alert = repo.record_alert(
        session, incident_id=incident.id, channel="log", recipients=[], subject="s", status="sent"
    )
    state = repo.upsert_contract_state(session, dag_id="d", status="approved", path="p", approved_at=T0)
    pending = repo.add_pending_event(session, dag_id="d", run_id="r", task_id="t", try_number=1)
    session.commit()

    data = incident.to_dict()
    json.dumps(data)
    assert data["status"] == WAITING_HUMAN
    assert data["sample"] == [{"email": "h:0123456789", "amount": None}]
    assert datetime.fromisoformat(data["created_at"]).tzinfo is not None
    summary = incident.to_dict(summary=True)
    assert "diff" not in summary and "sample" not in summary and summary["id"] == incident.id

    for row in (*repo.list_events(session, incident.id), command, alert, state, pending):
        json.dumps(row.to_dict())
    assert state.to_dict()["approved_at"] == T0.isoformat()


def test_json_safe_helper() -> None:
    from decimal import Decimal

    value = {"when": T0, "day": T0.date(), "n": Decimal("1.5"), "items": (1, {2}), "obj": Path("x")}
    assert json.loads(json.dumps(models.json_safe(value))) == {
        "when": T0.isoformat(),
        "day": "2026-10-01",
        "n": 1.5,
        "items": [1, [2]],
        "obj": "x",
    }
