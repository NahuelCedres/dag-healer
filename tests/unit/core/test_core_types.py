"""Tests for dag_healer.core.failures, .evidence and .specs (the plain data types)."""

from __future__ import annotations

import dataclasses
import pickle
from datetime import datetime, timezone
from pathlib import Path

import pytest

from dag_healer.core import specs
from dag_healer.core.evidence import AttemptDetail, LogBundle, PipelineSource
from dag_healer.core.failures import AttemptState, Confirmation, Failure, FailureHint, FailurePage
from dag_healer.core.ids import UNRESOLVED_STEP, AttemptRef
from dag_healer.core.specs import DagInfo, DatasetRef, PathMapping, PipelineSpec, StepSpec, TaskInfo

REF = AttemptRef("airflow", "customers_sync", "manual__1", "transform")
FIXTURE_REPO = Path(__file__).resolve().parents[1] / "fixtures" / "repos" / "legacy_etl"


# =========================================================================== failures


def test_attempt_state_values_are_stable_strings() -> None:
    assert [state.value for state in AttemptState] == [
        "failed",
        "retrying",
        "caught",
        "running",
        "succeeded",
        "unknown",
    ]
    assert AttemptState("failed") is AttemptState.FAILED
    assert AttemptState.RETRYING == "retrying"  # str enum: JSON-friendly


def test_failure_defaults_and_immutability() -> None:
    failure = Failure(ref=REF, state=AttemptState.FAILED, is_final=True, source="rest")

    assert (failure.error_name, failure.error_summary, failure.failed_at) == (None, "", None)
    assert dict(failure.native) == {}
    with pytest.raises(dataclasses.FrozenInstanceError):
        failure.is_final = False  # type: ignore[misc]
    other = Failure(ref=REF, state=AttemptState.FAILED, is_final=True, source="rest")
    assert failure.native is not other.native  # no shared mutable default


def test_hint_confirmation_and_page() -> None:
    hint = FailureHint(
        ref=AttemptRef("sfn", "etl", "exec-1", UNRESOLVED_STEP, attempt=1),
        channel="eventbridge",
        source="sqs",
        error_text="States.TaskFailed: boom",
        received_at=datetime(2026, 10, 18, tzinfo=timezone.utc),
        native={"execution_arn": "arn:aws:states:eu-west-1:1:execution:etl:exec-1"},
    )
    assert hint.ref.resolved is False

    confirmed = Confirmation(
        verdict="confirmed",
        reason="execution FAILED",
        failures=(
            Failure(
                ref=hint.ref.with_step("Transform"),
                state=AttemptState.FAILED,
                is_final=True,
                source=hint.source,
                error_name="KeyError",
            ),
        ),
    )
    assert confirmed.failures[0].ref.step_id == "Transform"
    assert Confirmation(verdict="discarded", reason="forged").failures == ()

    page = FailurePage(failures=confirmed.failures, cursor={"watermark": "2026-10-18T00:00:00+00:00"})
    assert page.complete is True and len(page.failures) == 1


# =========================================================================== evidence


def test_evidence_defaults() -> None:
    detail = AttemptDetail(
        ref=REF, state=AttemptState.FAILED, is_final=True, error_name="ValueError", error_message="bad"
    )
    assert (detail.traceback_text, detail.raw_error, detail.executor, detail.resource) == ("", "", None, None)
    assert (detail.started_at, detail.ended_at, detail.max_attempts, dict(detail.native)) == (
        None,
        None,
        None,
        {},
    )

    log = LogBundle(text="", source="none", confidence="none", notes=("no log yet",))
    assert (log.truncated, LogBundle(text="x", source="airflow:task-log").confidence) == (False, "exact")

    source = PipelineSource()
    assert (source.path, source.relative_path, source.text, source.language, source.origin) == (
        None,
        None,
        None,
        "python",
        "",
    )


# =========================================================================== specs


def test_dagscan_reexports_the_same_dataclasses() -> None:
    from dag_healer.repo import dagscan

    assert dagscan.TaskInfo is TaskInfo
    assert dagscan.DatasetRef is DatasetRef
    assert dagscan.DagInfo is DagInfo
    assert StepSpec is TaskInfo and PipelineSpec is DagInfo
    assert [f.name for f in dataclasses.fields(TaskInfo)] == [
        "task_id",
        "operator",
        "callable_name",
        "callable_module",
        "callable_file",
        "lineno",
    ]
    assert [f.name for f in dataclasses.fields(DatasetRef)] == [
        "task_id",
        "path",
        "type",
        "resolved",
        "source",
    ]
    assert [f.name for f in dataclasses.fields(DagInfo)] == ["dag_id", "file", "tasks", "datasets", "imports"]


def test_dag_info_behaviour_is_unchanged() -> None:
    task = TaskInfo(
        "transform", "PythonOperator", "transform_customers", "src.etl.customers", "src/etl/c.py", 7
    )
    dataset = DatasetRef("transform", "data/incoming/customers.csv", "csv", True, "dags/customers_sync.py:12")
    dag = DagInfo("customers_sync", "dags/customers_sync.py", tasks=[task], datasets=[dataset])

    assert dag.task("transform") is task
    assert dag.task("missing") is None
    assert dag.to_dict() == {
        "dag_id": "customers_sync",
        "file": "dags/customers_sync.py",
        "tasks": [dataclasses.asdict(task)],
        "datasets": [dataclasses.asdict(dataset)],
        "imports": [],
    }
    assert DagInfo("a", "b").tasks is not DagInfo("a", "b").tasks


def test_pickles_of_the_old_module_path_still_load() -> None:
    task = TaskInfo("t", "PythonOperator", lineno=3)
    assert pickle.loads(pickle.dumps(task)) == task

    # A pickle written before the move names dag_healer.repo.dagscan (protocol 0 is text).
    legacy = pickle.dumps(task, protocol=0).replace(b"dag_healer.core.specs", b"dag_healer.repo.dagscan")
    assert b"dag_healer.repo.dagscan" in legacy
    restored = pickle.loads(legacy)
    assert type(restored) is TaskInfo and restored == task


def test_scan_dags_returns_core_spec_instances() -> None:
    from dag_healer.repo.dagscan import scan_dags

    dags = scan_dags(FIXTURE_REPO, "dags")

    assert dags and all(type(dag) is specs.DagInfo for dag in dags)
    assert all(type(task) is specs.TaskInfo for dag in dags for task in dag.tasks)


def test_path_mapping_is_frozen() -> None:
    mapping = PathMapping("/var/task", "functions/transform")
    with pytest.raises(dataclasses.FrozenInstanceError):
        mapping.repo_base = ""  # type: ignore[misc]
    assert mapping == PathMapping(runtime_prefix="/var/task", repo_base="functions/transform")
