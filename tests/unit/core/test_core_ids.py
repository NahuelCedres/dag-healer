"""Tests for dag_healer.core.ids (neutral identifiers and the incident key)."""

from __future__ import annotations

import dataclasses
from collections.abc import Iterator
from pathlib import Path
from typing import Any

import pytest

from dag_healer.core.ids import (
    DEFAULT_ORCHESTRATOR,
    NO_MAP_INDEX,
    RUN_LEVEL_STEP,
    UNRESOLVED_STEP,
    AttemptRef,
    PipelineRef,
    RunRef,
)

RUN = "manual__2026-10-17T18:48:46.093857+00:00"


def test_sentinels_have_the_documented_values() -> None:
    assert NO_MAP_INDEX == -1
    assert UNRESOLVED_STEP == ""
    assert RUN_LEVEL_STEP == "__run__"
    assert DEFAULT_ORCHESTRATOR == "airflow"


def test_attempt_ref_defaults_key_and_parents() -> None:
    ref = AttemptRef("airflow", "customers_sync", RUN, "transform")

    assert (ref.map_index, ref.attempt) == (-1, 1)
    assert ref.key() == ("airflow", "customers_sync", RUN, "transform", -1, 1)
    assert ref.run == RunRef("airflow", "customers_sync", RUN)
    assert ref.pipeline == PipelineRef("airflow", "customers_sync")
    assert ref.run.pipeline == ref.pipeline
    assert ref.resolved is True


def test_store_key_uses_the_physical_column_names() -> None:
    ref = AttemptRef("sfn-prod-euw1", "etl", "exec-1", "Transform", map_index=3, attempt=2)

    assert ref.store_key() == {
        "orchestrator": "sfn-prod-euw1",
        "dag_id": "etl",
        "run_id": "exec-1",
        "task_id": "Transform",
        "map_index": 3,
        "try_number": 2,
    }
    assert all(type(ref.store_key()[name]) is int for name in ("map_index", "try_number"))


def test_unresolved_hint_is_localised_with_with_step() -> None:
    hint = AttemptRef("sfn", "etl", "exec-1", UNRESOLVED_STEP, attempt=2)
    assert hint.resolved is False

    localised = hint.with_step("Transform", map_index=4)

    assert localised == AttemptRef("sfn", "etl", "exec-1", "Transform", 4, 2)
    assert localised.resolved is True
    assert hint.step_id == UNRESOLVED_STEP  # the original is unchanged
    assert hint.with_step(RUN_LEVEL_STEP).map_index == NO_MAP_INDEX


def test_next_attempt_increments_only_the_attempt() -> None:
    ref = AttemptRef("airflow", "d", "r", "t", map_index=2, attempt=1)
    assert ref.next_attempt() == AttemptRef("airflow", "d", "r", "t", map_index=2, attempt=2)


def test_refs_are_frozen_hashable_and_slotted() -> None:
    ref = AttemptRef("airflow", "d", "r", "t")
    with pytest.raises(dataclasses.FrozenInstanceError):
        ref.step_id = "other"  # type: ignore[misc]
    assert {ref, AttemptRef("airflow", "d", "r", "t")} == {ref}
    assert ref != AttemptRef("sfn", "d", "r", "t")  # the instance is part of the key
    for value in (ref, ref.run, ref.pipeline):
        assert not hasattr(value, "__dict__")


def test_from_row_reads_the_physical_keys() -> None:
    row = {
        "id": "abc",
        "dag_id": "sales_daily_etl",
        "run_id": RUN,
        "task_id": "transform_sales",
        "map_index": 2,
        "try_number": 3,
        "status": "new",
    }

    ref = AttemptRef.from_row(row)

    assert ref == AttemptRef("airflow", "sales_daily_etl", RUN, "transform_sales", 2, 3)
    assert AttemptRef.from_row(row, default_orchestrator="prod").orchestrator == "prod"
    explicit = AttemptRef.from_row({**row, "orchestrator": "sfn-x"}, default_orchestrator="prod")
    assert explicit.orchestrator == "sfn-x"
    assert AttemptRef.from_row({**row, "orchestrator": ""}).orchestrator == "airflow"


def test_from_row_accepts_neutral_names_and_tolerates_missing_values() -> None:
    neutral = {"pipeline_id": "etl", "run_id": "exec-1", "step_id": "Transform", "attempt": 2}
    assert AttemptRef.from_row(neutral) == AttemptRef("airflow", "etl", "exec-1", "Transform", -1, 2)

    physical_wins = {**neutral, "dag_id": "dag", "task_id": "task", "try_number": 5}
    assert AttemptRef.from_row(physical_wins).key() == ("airflow", "dag", "exec-1", "task", -1, 5)
    null_physical = {**neutral, "dag_id": None, "task_id": None, "try_number": None}
    assert AttemptRef.from_row(null_physical) == AttemptRef.from_row(neutral)

    sparse = {"dag_id": "d", "run_id": "r", "task_id": "", "map_index": None, "try_number": "2"}
    ref = AttemptRef.from_row(sparse)
    assert (ref.step_id, ref.map_index, ref.attempt, ref.resolved) == ("", -1, 2, False)

    garbage = {"dag_id": "d", "run_id": "r", "task_id": "t", "map_index": "x", "try_number": True}
    assert AttemptRef.from_row(garbage).key()[-2:] == (-1, 1)
    assert AttemptRef.from_row({}).key() == ("airflow", "", "", "", -1, 1)


# --------------------------------------------------------------------------- real store rows


@pytest.fixture
def store_url(tmp_path: Path, healer_env: Path) -> Iterator[str]:
    from dag_healer.store import dispose_engines

    yield f"sqlite:///{(tmp_path / 'healer.db').as_posix()}"
    dispose_engines()


def test_from_row_round_trips_incident_and_pending_event_rows(store_url: str) -> None:
    from dag_healer.store import repo as store_repo
    from dag_healer.store import session_scope

    with session_scope(store_url) as session:
        incident, _created = store_repo.create_incident(
            session, dag_id="d", run_id=RUN, task_id="t", map_index=1, try_number=2, source="rest"
        )
        event = store_repo.add_pending_event(session, dag_id="d", run_id=RUN, task_id="t", try_number=2)
        rows: list[dict[str, Any]] = [incident.to_dict(), event.to_dict()]

    incident_ref = AttemptRef.from_row(rows[0])
    assert incident_ref == AttemptRef("airflow", "d", RUN, "t", 1, 2)
    expected_key = {k: v for k, v in incident_ref.store_key().items() if k != "orchestrator"}
    assert expected_key == {k: rows[0][k] for k in expected_key}
    assert AttemptRef.from_row(rows[1]) == AttemptRef("airflow", "d", RUN, "t", -1, 2)
