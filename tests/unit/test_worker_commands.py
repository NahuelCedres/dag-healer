"""Tests for dag_healer.worker.commands: the human actions the worker applies.

Each test uses a SQLite store under a temporary directory and hand-made incidents; the
Airflow API is a tiny in-memory stand-in (wrapped in the Airflow adapter), and the neutral
cases run on the fake orchestrator ("Fakeflow"). The full resolve -> resume -> repair/report
flow on the example repository is covered by test_worker_pipeline.py.
"""

from __future__ import annotations

from collections.abc import Iterator, Mapping
from pathlib import Path
from typing import Any

import pytest

from dag_healer.agent.fake import FakeProvider
from dag_healer.airflow.client import AirflowAPIError
from dag_healer.config import HealerConfig, config_from_mapping
from dag_healer.contracts.io import (
    approve_contract,
    compute_source_hash,
    contract_path,
    load_contract,
    save_contract,
)
from dag_healer.contracts.schema import ColumnRule, Contract, DatasetContract, DatasetLocator
from dag_healer.core.errors import NotSupported
from dag_healer.core.ids import AttemptRef
from dag_healer.core.specs import TaskInfo
from dag_healer.core.vocab import RemediationKind
from dag_healer.orchestrators.fake import FakeAdapter, FakeWorld
from dag_healer.store import dispose_engines, session_scope
from dag_healer.store import repo as store_repo
from dag_healer.store.models import Command, Incident
from dag_healer.worker.commands import apply_commands
from dag_healer.worker.pipeline import process_incident

RUN = "manual__2026-10-17T10:00:00+00:00"


class ClearingClient:
    """Airflow stand-in that records ``clearTaskInstances`` calls."""

    def __init__(self, error: Exception | None = None) -> None:
        self.error = error
        self.calls: list[tuple[str, str, list[Any], bool]] = []

    def clear_task_instances(
        self, dag_id: str, run_id: str, task_ids: list[Any], *, only_failed: bool = True
    ) -> dict[str, Any]:
        self.calls.append((dag_id, run_id, list(task_ids), only_failed))
        if self.error is not None:
            raise self.error
        return {"task_instances": [{"task_id": "t"}], "total_entries": 1}


@pytest.fixture(autouse=True)
def _dispose_engines() -> Iterator[None]:
    yield
    dispose_engines()


@pytest.fixture
def cfg(tmp_path: Path, healer_env: Path) -> HealerConfig:
    repo = tmp_path / "repo"
    (repo / "dags").mkdir(parents=True)
    return config_from_mapping(
        {"repo": {"path": str(repo)}, "store": {"url": f"sqlite:///{(tmp_path / 'store.db').as_posix()}"}}
    )


def incident(
    cfg: HealerConfig,
    status: str,
    *,
    dag_id: str = "etl",
    map_index: int = -1,
    source: str = "rest",
    **fields: Any,
) -> str:
    with session_scope(cfg.database_url) as session:
        row, _created = store_repo.create_incident(
            session,
            dag_id=dag_id,
            run_id=RUN,
            task_id="load",
            map_index=map_index,
            try_number=1,
            source=source,
            error_summary="KeyError: 'amount'",
            **fields,
        )
        if status != row.status:
            store_repo.set_status(session, row, status, f"set to {status} by the test")
        return row.id


def command(
    cfg: HealerConfig,
    kind: str,
    *,
    dag_id: str = "etl",
    incident_id: str | None = None,
    orchestrator: str = "airflow",
    **payload: Any,
) -> int:
    with session_scope(cfg.database_url) as session:
        return store_repo.enqueue_command(
            session,
            kind=kind,
            dag_id=dag_id,
            incident_id=incident_id,
            payload=payload,
            actor="alice",
            orchestrator=orchestrator,
        ).id


def fakeflow() -> FakeAdapter:
    """Fakeflow with one failed attempt: step ``load`` of flow ``etl`` in run ``RUN``."""
    world = FakeWorld()
    world.add_pipeline("etl", file="dags/etl.py", steps={"load": TaskInfo("load", "python")})
    world.fail("etl", RUN, "load", text="KeyError: 'amount'")
    return FakeAdapter(world=world)


def load(cfg: HealerConfig, incident_id: str) -> dict[str, Any]:
    with session_scope(cfg.database_url) as session:
        row = session.get(Incident, incident_id)
        assert row is not None
        data = row.to_dict()
        data["events"] = [event.to_dict() for event in store_repo.list_events(session, incident_id)]
        return data


def command_row(cfg: HealerConfig, command_id: int) -> dict[str, Any]:
    with session_scope(cfg.database_url) as session:
        row = session.get(Command, command_id)
        assert row is not None
        return row.to_dict()


def last_event(data: dict[str, Any], kind: str) -> dict[str, Any]:
    return [event for event in data["events"] if event["kind"] == kind][-1]


# --------------------------------------------------------------------------- resolve


@pytest.mark.parametrize("resolution", ["data", "code"])
def test_resolve_data_or_code_resumes_with_the_forced_category(cfg: HealerConfig, resolution: str) -> None:
    incident_id = incident(cfg, "waiting_human")
    command_id = command(
        cfg, "resolve", incident_id=incident_id, resolution=resolution, note="see jane@example.com"
    )

    (outcome,) = apply_commands(cfg, None, None)

    assert (outcome.status, outcome.command_id) == ("applied", command_id)
    data = load(cfg, incident_id)
    assert (data["status"], data["resolution"], data["resolved_by"]) == ("resumed", resolution, "alice")
    event = last_event(data, "command")
    assert event["actor"] == "alice" and event["data"]["status"] == "applied"
    assert "jane@example.com" not in event["message"] and "[email]" in event["message"]
    row = command_row(cfg, command_id)
    assert (row["status"], row["error"]) == ("applied", None) and row["applied_at"]
    # a second pass has nothing left to do
    assert apply_commands(cfg, None, None) == []


def test_resolve_ignore_closes_the_incident(cfg: HealerConfig) -> None:
    incident_id = incident(cfg, "reported", category="data")
    command(cfg, "resolve", incident_id=incident_id, resolution="ignore")
    (outcome,) = apply_commands(cfg, None, None)
    assert outcome.status == "applied"
    data = load(cfg, incident_id)
    assert (data["status"], data["resolution"], data["resolved_by"]) == ("ignored", "ignore", "alice")


def test_command_not_allowed_in_the_current_status_fails(cfg: HealerConfig) -> None:
    incident_id = incident(cfg, "repaired", category="code")
    command_id = command(cfg, "resolve", incident_id=incident_id, resolution="data")

    (outcome,) = apply_commands(cfg, None, None)

    assert outcome.status == "failed"
    row = command_row(cfg, command_id)
    assert row["status"] == "failed"
    assert row["error"] == "Resolve data is not allowed while the incident is repaired."
    data = load(cfg, incident_id)
    assert data["status"] == "repaired"
    assert last_event(data, "command")["data"]["status"] == "failed"


def test_commands_for_unknown_incidents_or_other_dags_fail(cfg: HealerConfig) -> None:
    incident_id = incident(cfg, "waiting_human")
    other = command(cfg, "resolve", dag_id="other_dag", incident_id=incident_id, resolution="code")
    bad = command(cfg, "resolve", incident_id=incident_id, resolution="maybe")

    outcomes = apply_commands(cfg, None, None)

    assert [o.status for o in outcomes] == ["failed", "failed"]
    assert "does not belong to DAG other_dag" in command_row(cfg, other)["error"]
    assert "Unknown resolution 'maybe'" in command_row(cfg, bad)["error"]
    assert load(cfg, incident_id)["status"] == "waiting_human"


# --------------------------------------------------------------------------- retry_repair / retry_task


def test_retry_repair_resumes_as_code(cfg: HealerConfig) -> None:
    incident_id = incident(cfg, "repair_failed", category="code")
    command(cfg, "retry_repair", incident_id=incident_id)
    (outcome,) = apply_commands(cfg, None, None)
    assert outcome.status == "applied"
    data = load(cfg, incident_id)
    assert (data["status"], data["resolution"], data["resolved_by"]) == ("resumed", "code", "alice")
    assert "new repair attempt" in last_event(data, "command")["message"]


def test_retry_task_clears_the_task_instance_and_keeps_the_status(cfg: HealerConfig) -> None:
    incident_id = incident(cfg, "repaired", category="code", pr_md_path="/x/PR.md")
    mapped_id = incident(cfg, "reported", dag_id="mapped", map_index=3, category="data")
    command(cfg, "retry_task", incident_id=incident_id, run_id=RUN, task_id="load", map_index=-1)
    command(cfg, "retry_task", dag_id="mapped", incident_id=mapped_id)
    client = ClearingClient()

    outcomes = apply_commands(cfg, None, client)

    assert [o.status for o in outcomes] == ["applied", "applied"]
    assert client.calls == [("etl", RUN, ["load"], True), ("mapped", RUN, [("load", 3)], True)]
    data = load(cfg, incident_id)
    assert data["status"] == "repaired"
    event = last_event(data, "command")
    assert "Airflow will run it again" in event["message"] and "not merged yet" in event["message"]
    assert event["data"]["cleared"] == 1
    assert load(cfg, mapped_id)["status"] == "reported"


def test_retry_task_without_client_or_with_an_api_error_fails(cfg: HealerConfig) -> None:
    incident_id = incident(cfg, "reported", category="data")
    first = command(cfg, "retry_task", incident_id=incident_id)
    (outcome,) = apply_commands(cfg, None, None)
    assert outcome.status == "failed"
    assert "No Airflow API client" in command_row(cfg, first)["error"]

    second = command(cfg, "retry_task", incident_id=incident_id)
    error = AirflowAPIError("forbidden for bob@example.com", status_code=403)
    (outcome,) = apply_commands(cfg, None, ClearingClient(error))
    assert outcome.status == "failed"
    message = command_row(cfg, second)["error"]
    assert "Airflow refused to clear the task instance" in message and "403" in message
    assert "bob@example.com" not in message


def test_retry_task_runs_through_the_adapter_remediator_with_its_words(cfg: HealerConfig) -> None:
    adapter = fakeflow()
    incident_id = incident(
        cfg, "repaired", source="poll", orchestrator="fake", category="code", pr_md_path="/x/PR.md"
    )
    command_id = command(cfg, "retry_task", incident_id=incident_id, orchestrator="fake", note="after deploy")
    command(cfg, "retry_task", incident_id=incident(cfg, "reported", dag_id="other"), dag_id="other")

    (outcome,) = apply_commands(cfg, None, None, adapter=adapter)  # the airflow command is not this worker's

    assert (outcome.status, outcome.command_id) == ("applied", command_id)
    assert outcome.message == (
        f"alice retried step load in run {RUN} (attempt 2); Fakeflow will run it again. The proposed fix "
        "is not merged yet, so the step runs the current code. Note: after deploy"
    )
    assert outcome.data == {"run_id": RUN, "task_id": "load", "map_index": -1}  # no "cleared" count
    (done,) = adapter.world.remediations
    assert (done.kind, done.ref, done.actor) == (
        RemediationKind.RETRY_STEP,
        AttemptRef("fake", "etl", RUN, "load"),
        "alice",
    )
    assert done.idempotency_key == f"healer-{command_id}-{incident_id[:16]}"
    event = last_event(load(cfg, incident_id), "command")
    assert event["data"] == {
        "command_id": command_id,
        "kind": "retry_task",
        "status": "applied",
        "run_id": RUN,
        "task_id": "load",
        "map_index": -1,
    }


class NoRetry(FakeAdapter):
    """Fakeflow that offers no remediation for any incident."""

    def available(self, ref: AttemptRef, native: Mapping[str, Any]) -> tuple[RemediationKind, ...]:
        return ()


class RefusingRetry(FakeAdapter):
    """Fakeflow whose remediation turns out not to be possible."""

    def remediate(self, ref: AttemptRef, native: Mapping[str, Any], kind: RemediationKind, **_: Any) -> Any:
        raise NotSupported("the run is older than the redrive window")


def test_retry_task_fails_clearly_when_the_orchestrator_cannot_retry(cfg: HealerConfig) -> None:
    incident_id = incident(cfg, "reported", source="poll", orchestrator="fake", category="data")
    without = FakeAdapter()
    without.remediator = None
    cases = [
        (without, "Fakeflow offers no way to run a failed step again."),
        (NoRetry(), "Retry step is not available for this incident."),
        (
            RefusingRetry(),
            "Retry step is not available for this incident: the run is older than the redrive window",
        ),
    ]
    for adapter, expected in cases:
        command_id = command(cfg, "retry_task", incident_id=incident_id, orchestrator="fake")
        (outcome,) = apply_commands(cfg, None, None, adapter=adapter)
        assert outcome.status == "failed"
        assert command_row(cfg, command_id)["error"] == expected
    assert load(cfg, incident_id)["status"] == "reported"


# --------------------------------------------------------------------------- approve_contract


def write_contract(cfg: HealerConfig, dag_id: str = "etl") -> Path:
    contract = Contract(
        dag_id=dag_id,
        status="draft",
        datasets=[
            DatasetContract(
                name="sales",
                task_id="load",
                locator=DatasetLocator(type="csv", path="data/incoming/sales.csv"),
                columns=[ColumnRule(name="amount", dtype="float", nullable=False)],
            )
        ],
    )
    return save_contract(contract, contract_path(cfg.contracts_dir, dag_id))


def test_approve_contract_writes_the_yaml_and_caches_the_state(cfg: HealerConfig) -> None:
    path = write_contract(cfg)
    reviewed = compute_source_hash(load_contract(path))
    command_id = command(
        cfg, "approve_contract", path="healer/contracts/etl.yaml", source_hash=reviewed, note="looks right"
    )

    (outcome,) = apply_commands(cfg, None, None)

    assert outcome.status == "applied", outcome.message
    approved = load_contract(path)
    assert (approved.status, approved.approved_by, approved.source_hash) == ("approved", "alice", reviewed)
    with session_scope(cfg.database_url) as session:
        state = store_repo.get_contract_state(session, "etl")
        assert state is not None and (state.status, state.approved_by) == ("approved", "alice")
    assert command_row(cfg, command_id)["status"] == "applied"

    # approving an approved contract again is a no-op that still succeeds
    command(cfg, "approve_contract", source_hash=reviewed)
    (outcome,) = apply_commands(cfg, None, None)
    assert outcome.status == "applied" and "already approved" in outcome.message


def test_approve_contract_caches_the_state_under_the_command_instance(cfg: HealerConfig) -> None:
    path = write_contract(cfg)
    command_id = command(cfg, "approve_contract", orchestrator="fake")

    (outcome,) = apply_commands(cfg, None, None, adapter=fakeflow())

    assert (outcome.status, outcome.command_id) == ("applied", command_id)
    assert load_contract(path).status == "approved"
    with session_scope(cfg.database_url) as session:
        state = store_repo.get_contract_state(session, "etl", orchestrator="fake")
        assert state is not None and state.approved_by == "alice"
        assert store_repo.get_contract_state(session, "etl") is None  # not the airflow instance's


def test_approve_contract_refuses_a_file_edited_after_the_review(cfg: HealerConfig) -> None:
    path = write_contract(cfg)
    reviewed = compute_source_hash(load_contract(path))
    edited = load_contract(path).model_copy(update={"notes": "edited after the review"})
    save_contract(edited, path)
    command_id = command(cfg, "approve_contract", source_hash=reviewed)

    (outcome,) = apply_commands(cfg, None, None)

    assert outcome.status == "failed"
    assert "changed after it was reviewed" in command_row(cfg, command_id)["error"]
    assert load_contract(path).status == "draft"


def test_approve_contract_fails_without_a_file_and_ignores_paths_outside_the_contracts_dir(
    cfg: HealerConfig,
) -> None:
    missing = command(cfg, "approve_contract", dag_id="nope")
    write_contract(cfg)
    escaped = command(cfg, "approve_contract", path="../outside.yaml")

    outcomes = apply_commands(cfg, None, None)

    assert [o.status for o in outcomes] == ["failed", "applied"]
    assert "no contract file for DAG nope" in command_row(cfg, missing)["error"]
    assert command_row(cfg, escaped)["status"] == "applied"
    assert load_contract(contract_path(cfg.contracts_dir, "etl")).status == "approved"


def test_approve_contract_of_a_stale_contract(cfg: HealerConfig) -> None:
    path = write_contract(cfg)
    approved = approve_contract(load_contract(path), "bob")
    save_contract(approved.model_copy(update={"notes": "edited later"}), path)
    current = compute_source_hash(load_contract(path))
    command(cfg, "approve_contract", source_hash=current)
    (outcome,) = apply_commands(cfg, None, None)
    assert outcome.status == "applied"
    assert load_contract(path).approved_by == "alice"


# --------------------------------------------------------------------------- resume -> pipeline


def test_resolved_data_incident_is_reported_without_classification(cfg: HealerConfig) -> None:
    incident_id = incident(cfg, "waiting_human", category="ambiguous", diagnosis="unclear")
    command(cfg, "resolve", incident_id=incident_id, resolution="data")
    apply_commands(cfg, None, None)

    status = process_incident(cfg, None, incident_id, provider=FakeProvider(cfg), client=None)

    assert status == "reported"
    data = load(cfg, incident_id)
    assert (data["category"], data["confidence"], data["diagnosis"]) == ("data", 1.0, "unclear")
    classification = last_event(data, "classification")
    assert classification["data"]["rule"] == "human_resolution"
    assert classification["actor"] == "alice"
    statuses = [e["data"]["to"] for e in data["events"] if e["kind"] == "status"]
    assert statuses[-5:] == ["resumed", "triaging", "classified", "reporting", "reported"]
    # human-resumed work does not count against the automatic triage limit
    assert "triage" not in [e["kind"] for e in data["events"]]
