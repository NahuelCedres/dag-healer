"""Tests for dag_healer.worker.pipeline: reconcile, process_incident and the full demo flow.

The flow test runs on a git copy of ``example_existing_repo`` prepared like the demo: the
real ``dag-healer init``, the sales and customers contracts approved with the real CLI, the
day-2 files copied over ``data/incoming``. The Airflow API is a ``FakeAirflowClient`` whose
task logs are rendered (with the real ``render_log_content``) from the REAL exceptions raised
by running the example ETL functions in-process on the day-2 data, so the tracebacks point
at the copy's files. The LLM is the real ``FakeProvider`` replaying the repository's
``fixtures/fake_llm`` scripts, and the store is SQLite under a temporary ``HEALER_HOME``.
The demo outcomes are also played through the fake orchestrator ("Fakeflow"), which reports
the same real exceptions, to check that the worker only speaks to the adapter's ports and in
the adapter's words. The repairs run pytest in the ``process`` test sandbox (the unit tests
have no container engine).
"""

from __future__ import annotations

import csv
import dataclasses
import importlib
import json
import logging
import re
import shutil
import sys
import traceback
from collections.abc import Iterator
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

import pytest

from dag_healer.agent.fake import FakeProvider
from dag_healer.airflow.client import AirflowAPIError, TaskInstanceInfo, render_log_content
from dag_healer.cli.main import main
from dag_healer.config import HealerConfig, SandboxConfig, config_from_mapping, load_config
from dag_healer.contracts.io import contract_path, load_contract
from dag_healer.core.specs import TaskInfo
from dag_healer.masking import mask_text
from dag_healer.orchestrators.fake import FakeAdapter, FakeWorld
from dag_healer.repo.dagscan import scan_dags
from dag_healer.repo.git import run_git
from dag_healer.store import dispose_engines, session_scope
from dag_healer.store import repo as store_repo
from dag_healer.store.models import Alert, Incident, IncidentEvent, PendingEvent
from dag_healer.worker import pipeline
from dag_healer.worker.loop import run_worker
from dag_healer.worker.pipeline import (
    compose_error_summary,
    failure_signature,
    get_not_before,
    get_watermark,
    process_incident,
    reconcile,
    reset_state,
    set_watermark,
    summarize_failure,
)

EXAMPLE_REPO = Path(__file__).resolve().parents[2] / "example_existing_repo"
GIT_IDENTITY = {
    "GIT_AUTHOR_NAME": "Test",
    "GIT_AUTHOR_EMAIL": "test@example.invalid",
    "GIT_COMMITTER_NAME": "Test",
    "GIT_COMMITTER_EMAIL": "test@example.invalid",
}
RUN_1 = "manual__2026-10-17T10:00:00+00:00"
RUN_2 = "manual__2026-10-18T10:00:00+00:00"
NOW = datetime(2026, 10, 17, 12, 0, tzinfo=timezone.utc)
EMAIL_RE = re.compile(r"[\w.+-]+@[\w-]+\.[\w.]+")

#: dag_id -> (module, [(task_id, callable)...]) mirroring the example DAG files.
ETL_TASKS: dict[str, tuple[str, list[tuple[str, str]]]] = {
    "sales_daily_etl": (
        "src.etl.sales",
        [
            ("extract_sales", "extract_sales"),
            ("transform_sales", "transform_sales"),
            ("load_sales", "load_sales"),
        ],
    ),
    "customers_sync": (
        "src.etl.customers",
        [("extract", "extract_customers"), ("transform", "transform_customers"), ("load", "load_customers")],
    ),
    "inventory_report": (
        "src.etl.inventory",
        [
            ("get_inventory", "getInventory"),
            ("build_report", "buildReport"),
            ("publish_report", "publishReport"),
        ],
    ),
}


# =========================================================================== fakes


class FakeAirflowClient:
    """In-memory stand-in for :class:`dag_healer.airflow.client.AirflowClient`."""

    base_url = "http://airflow.test"

    def __init__(self) -> None:
        self.instances: dict[tuple[str, str, str, int], TaskInstanceInfo] = {}
        self.listed: list[tuple[str, str, str, int]] = []
        self.logs: dict[tuple[str, str, str, int, int], str] = {}
        self.cleared: list[dict[str, Any]] = []
        self.list_calls: list[datetime | None] = []
        self.list_error: Exception | None = None
        self.instance_errors: dict[tuple[str, str, str, int], Exception] = {}

    def add_failure(
        self,
        dag_id: str,
        run_id: str,
        task_id: str,
        log_text: str,
        *,
        try_number: int = 1,
        max_tries: int = 0,
        map_index: int = -1,
        state: str = "failed",
        listed: bool = True,
    ) -> TaskInstanceInfo:
        key = (dag_id, run_id, task_id, map_index)
        info = TaskInstanceInfo(
            dag_id=dag_id,
            run_id=run_id,
            task_id=task_id,
            map_index=map_index,
            try_number=try_number,
            max_tries=max_tries,
            state=state,
            operator="PythonOperator",
        )
        self.instances[key] = info
        self.logs[(dag_id, run_id, task_id, try_number, map_index)] = log_text
        if listed and key not in self.listed:
            self.listed.append(key)
        return info

    def list_failed_task_instances(
        self, *, updated_since: datetime | None = None, **_: Any
    ) -> list[TaskInstanceInfo]:
        self.list_calls.append(updated_since)
        if self.list_error is not None:
            raise self.list_error
        return [self.instances[key] for key in self.listed if self.instances[key].state == "failed"]

    def get_task_instance(
        self, dag_id: str, run_id: str, task_id: str, map_index: int = -1
    ) -> TaskInstanceInfo:
        key = (dag_id, run_id, task_id, int(map_index))
        if key in self.instance_errors:
            raise self.instance_errors[key]
        if key not in self.instances:
            raise AirflowAPIError("The Task Instance was not found", status_code=404)
        return self.instances[key]

    def get_task_log(
        self, dag_id: str, run_id: str, task_id: str, try_number: int, map_index: int = -1
    ) -> str:
        try:
            return self.logs[(dag_id, run_id, task_id, int(try_number), int(map_index))]
        except KeyError:
            raise AirflowAPIError("log not found", status_code=404) from None

    def get_dag(self, dag_id: str) -> dict[str, Any]:
        return {
            "dag_id": dag_id,
            "fileloc": f"/opt/airflow/repo/dags/{dag_id}.py",
            "relative_fileloc": f"{dag_id}.py",
        }

    def get_dag_source(self, dag_id: str) -> str | None:
        return None

    def clear_task_instances(
        self,
        dag_id: str,
        run_id: str,
        task_ids: list[Any],
        *,
        only_failed: bool = True,
        include_downstream: bool = False,
    ) -> dict[str, Any]:
        self.cleared.append(
            {
                "dag_id": dag_id,
                "run_id": run_id,
                "task_ids": list(task_ids),
                "only_failed": only_failed,
                "include_downstream": include_downstream,
            }
        )
        return {"task_instances": [{"task_id": str(t if isinstance(t, str) else t[0])} for t in task_ids]}


# =========================================================================== real failures


def _purge_src_modules() -> dict[str, Any]:
    removed = {
        name: module for name, module in sys.modules.items() if name == "src" or name.startswith("src.")
    }
    for name in removed:
        del sys.modules[name]
    return removed


def run_dag_in_process(repo: Path, dag_id: str) -> tuple[str, BaseException]:
    """Run the DAG's task callables in order on the repo copy; return the failing task and exception."""
    module_name, tasks = ETL_TASKS[dag_id]
    saved = _purge_src_modules()
    sys.path.insert(0, str(repo))
    try:
        module = importlib.import_module(module_name)
        for task_id, func in tasks:
            try:
                getattr(module, func)()
            except Exception as exc:  # the real failure of the day-2 data
                return task_id, exc
    finally:
        sys.path.remove(str(repo))
        _purge_src_modules()
        sys.modules.update(saved)
    raise AssertionError(f"{dag_id} did not fail on the day-2 data")


def error_detail(exc: BaseException) -> list[dict[str, Any]]:
    """The structlog exception chain Airflow logs as ``error_detail`` (outermost first)."""
    stacks: list[dict[str, Any]] = []
    current: BaseException | None = exc
    is_cause = False
    while current is not None:
        frames = [
            {"filename": frame.filename, "lineno": frame.lineno, "name": frame.name}
            for frame in traceback.extract_tb(current.__traceback__)
        ]
        stacks.append(
            {
                "exc_type": type(current).__name__,
                "exc_value": str(current),
                "exc_notes": [],
                "syntax_error": None,
                "is_cause": is_cause,
                "frames": frames,
                "is_group": False,
                "exceptions": [],
            }
        )
        if current.__cause__ is not None:
            current, is_cause = current.__cause__, True
        elif current.__context__ is not None and not current.__suppress_context__:
            current, is_cause = current.__context__, False
        else:
            current = None
    return stacks


def airflow_log(dag_id: str, task_id: str, exc: BaseException, *, extra_line: str | None = None) -> str:
    """The text the real client returns for a failed try (structured log -> render_log_content)."""
    ts = "2026-10-17T10:00:0{}.000000Z"
    content: list[dict[str, Any]] = [
        {"timestamp": ts.format(1), "event": "::group::Log message source details", "level": "info"},
        {"timestamp": ts.format(1), "event": "::endgroup::", "level": "info"},
        {
            "timestamp": ts.format(2),
            "event": f"Executing {dag_id}.{task_id}",
            "level": "info",
            "logger": "task",
        },
    ]
    if extra_line:
        content.append({"timestamp": ts.format(3), "event": extra_line, "level": "info", "logger": "etl"})
    content.append(
        {
            "timestamp": ts.format(4),
            "event": "Task failed with exception",
            "level": "error",
            "logger": "task",
            "error_detail": error_detail(exc),
        }
    )
    return render_log_content({"content": content, "continuation_token": None})


def raw_emails(repo: Path) -> set[str]:
    """Every e-mail address of the day-2 files (none may be persisted unmasked)."""
    found: set[str] = set()
    for name in ("sales", "customers"):
        with (repo / "fixtures" / "day2" / f"{name}.csv").open(encoding="utf-8") as handle:
            for row in csv.DictReader(handle):
                for value in row.values():
                    found.update(EMAIL_RE.findall(value or ""))
    assert found
    return found


# =========================================================================== bench


@dataclass
class Bench:
    repo: Path
    cfg: HealerConfig
    client: FakeAirflowClient
    home: Path
    failures: dict[str, tuple[str, BaseException]] = field(default_factory=dict)
    world: FakeWorld | None = None

    def use_fakeflow(self) -> FakeWorld:
        """Report the failures through Fakeflow instead of the Airflow client double.

        The flows are the repository's DAGs as the static scan sees them (like the end-to-end
        world of the conformance harness); the worker then runs on a ``FakeAdapter``.
        """
        world = FakeWorld()
        for dag in scan_dags(self.repo, self.cfg.repo.dags_dir):
            world.add_pipeline(
                dag.dag_id,
                file=dag.file,
                steps={task.task_id: task for task in dag.tasks},
                datasets=dag.datasets,
            )
        self.world = world
        return world

    def fail(self, dag_id: str, run_id: str = RUN_1, *, extra_line: str | None = None) -> str:
        """Run ``dag_id`` on the day-2 data and report its real failure (``extra_line``: Airflow log)."""
        task_id, exc = self.failures.get(dag_id) or run_dag_in_process(self.repo, dag_id)
        self.failures[dag_id] = (task_id, exc)
        if self.world is not None:
            self.world.fail(dag_id, run_id, task_id, exc=exc)
        else:
            self.client.add_failure(
                dag_id, run_id, task_id, airflow_log(dag_id, task_id, exc, extra_line=extra_line)
            )
        return task_id

    def run_once(self, provider: Any | None = None, **kwargs: Any) -> Any:
        orchestrator: dict[str, Any] = (
            {"adapter": FakeAdapter(self.cfg, world=self.world)}
            if self.world is not None
            else {"client": self.client}
        )
        return run_worker(
            self.cfg,
            once=True,
            provider=provider or FakeProvider(self.cfg),
            install_signal_handlers=False,
            **orchestrator,
            **kwargs,
        )

    def incidents(self, dag_id: str) -> list[dict[str, Any]]:
        with session_scope(self.cfg.database_url) as session:
            rows = store_repo.list_incidents(session, dag_ids={dag_id}, limit=50)
            return [row.to_dict() for row in sorted(rows, key=lambda r: r.created_at)]

    def incident(self, dag_id: str, index: int = -1) -> dict[str, Any]:
        return self.incidents(dag_id)[index]

    def events(self, incident_id: str) -> list[dict[str, Any]]:
        with session_scope(self.cfg.database_url) as session:
            return [event.to_dict() for event in store_repo.list_events(session, incident_id)]

    def alerts(self, incident_id: str) -> list[dict[str, Any]]:
        with session_scope(self.cfg.database_url) as session:
            return [alert.to_dict() for alert in store_repo.list_alerts(session, incident_id)]

    def command(
        self, kind: str, dag_id: str, *, incident_id: str | None = None, actor: str, **payload: Any
    ) -> int:
        with session_scope(self.cfg.database_url) as session:
            command = store_repo.enqueue_command(
                session, kind=kind, dag_id=dag_id, incident_id=incident_id, payload=payload, actor=actor
            )
            return command.id


def git_commit_all(root: Path) -> None:
    run_git(["init", "--quiet", "-b", "main"], root)
    run_git(["add", "-A"], root)
    run_git(["-c", "commit.gpgsign=false", "commit", "--quiet", "-m", "day 1"], root, env=GIT_IDENTITY)


@pytest.fixture(autouse=True)
def _dispose_engines() -> Iterator[None]:
    yield
    dispose_engines()


@pytest.fixture
def bench(tmp_path: Path, healer_env: Path, capsys: pytest.CaptureFixture[str]) -> Bench:
    """The demo set-up on a git copy of example_existing_repo (see the module docstring)."""
    repo = tmp_path / "repo"
    shutil.copytree(EXAMPLE_REPO, repo, ignore=shutil.ignore_patterns("__pycache__", "processed", ".git"))
    git_commit_all(repo)
    assert main(["init", "--repo", str(repo)]) == 0
    for dag_id in ("sales_daily_etl", "customers_sync"):
        assert (
            main(["-c", str(repo / "healer.yaml"), "contracts", "approve", dag_id, "--by", "data-lead"]) == 0
        )
    for name in ("sales", "customers", "inventory"):
        shutil.copyfile(
            repo / "fixtures" / "day2" / f"{name}.csv", repo / "data" / "incoming" / f"{name}.csv"
        )
    capsys.readouterr()
    cfg = load_config(repo / "healer.yaml")
    assert cfg.repo_root == repo.resolve()
    assert cfg.llm.provider == "anthropic"  # the demo overrides it with the fake provider below
    assert cfg.repo.sandbox.backend == "container"  # the template's choice
    cfg.repo.sandbox.backend = "process"  # the unit tests have no container engine
    return Bench(repo=repo, cfg=cfg, client=FakeAirflowClient(), home=healer_env)


def _diff_files(diff: str | None) -> set[str]:
    return set(re.findall(r"^diff --git a/(\S+) b/", diff or "", re.MULTILINE))


def _kinds(events: list[dict[str, Any]]) -> list[str]:
    return [event["kind"] for event in events]


def _statuses(events: list[dict[str, Any]]) -> list[str]:
    return [event["data"]["to"] for event in events if event["kind"] == "status"]


def _classification(events: list[dict[str, Any]]) -> dict[str, Any]:
    return [event for event in events if event["kind"] == "classification"][-1]["data"]


def _stored_text(bench: Bench) -> str:
    """Everything the store holds, as one JSON string."""
    with session_scope(bench.cfg.database_url) as session:
        rows: list[Any] = []
        for model in (Incident, IncidentEvent, Alert, PendingEvent):
            rows.extend(item.to_dict() for item in session.query(model).all())
    return json.dumps(rows, default=str)


# =========================================================================== the demo flow


def test_full_flow_on_the_example_repo(bench: Bench, tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    for dag_id in ETL_TASKS:
        bench.fail(dag_id, extra_line="loaded the CRM export for olivia.martin@example.com")
    assert {dag: task for dag, (task, _exc) in bench.failures.items()} == {
        "sales_daily_etl": "transform_sales",
        "customers_sync": "transform",
        "inventory_report": "build_report",
    }

    stats = bench.run_once()

    assert stats.errors == []
    assert stats.created == 3
    assert stats.processed == {"reported": 1, "repaired": 1, "waiting_human": 1}

    # ---------------------------------------------------------------- DATA: sales
    sales = bench.incident("sales_daily_etl")
    assert (sales["status"], sales["category"], sales["source"]) == ("reported", "data", "rest")
    assert sales["task_id"] == "transform_sales"
    assert sales["error_summary"].startswith("KeyError: 'amount'")
    assert sales["violated_rule"]["rule"] == "missing_column"
    assert sales["violated_rule"]["column"] == "amount"
    assert sales["violated_rule"]["contract_status"] == "approved"
    sample = sales["sample"]
    assert sample["masked"] is True and sample["rows"]
    assert "customer_email" in sample["columns"]
    assert all(str(row["customer_email"]).startswith("h:") for row in sample["rows"])
    sales_events = bench.events(sales["id"])
    assert _classification(sales_events)["rule"] == "a_contract_violated"
    assert _classification(sales_events)["decided_by"] == "policy"
    violations = _classification(sales_events)["violations"]
    assert {(v["rule"], v["column"]) for v in violations} >= {("missing_column", "amount")}
    assert _statuses(sales_events) == ["triaging", "classified", "reporting", "reported"]
    assert {"triage", "context", "classification", "alert"} <= set(_kinds(sales_events))
    sales_alerts = bench.alerts(sales["id"])
    assert [(a["channel"], a["status"]) for a in sales_alerts] == [("log", "sent")]
    assert "DATA issue" in sales_alerts[0]["subject"]

    # ---------------------------------------------------------------- CODE: customers
    customers = bench.incident("customers_sync")
    assert (customers["status"], customers["category"]) == ("repaired", "code"), bench.events(customers["id"])
    assert customers["error_summary"].startswith("ValueError: ")
    assert customers["test_path"].startswith("tests/test_")
    assert "parse_signup_date" in customers["test_code"]
    assert _diff_files(customers["diff"]) == {"src/etl/customers.py", customers["test_path"]}
    assert "fromisoformat" in customers["diff"]
    assert "pytest before the fix" in customers["pytest_output"]
    assert "pytest after the fix" in customers["pytest_output"]
    assert customers["pr_url"] is None
    pr_md = Path(customers["pr_md_path"])
    assert pr_md == bench.home / "prs" / customers["id"] / "PR.md"
    assert pr_md.is_file() and (pr_md.parent / "fix.patch").is_file()
    pr_text = pr_md.read_text(encoding="utf-8")
    assert "had no test suite" in pr_text and "Generated by dag-healer" in pr_text
    assert customers["attempts"] == 2
    customers_events = bench.events(customers["id"])
    assert _classification(customers_events)["rule"] == "b_code_with_valid_data"
    assert _classification(customers_events)["decided_by"] == "fake"
    assert _statuses(customers_events) == ["triaging", "classified", "repairing", "repaired"]
    repair_event = next(e for e in customers_events if e["kind"] == "repair")
    branch = repair_event["data"]["branch"]
    assert branch == f"healer/customers_sync-transform-{customers['id'][:8]}"
    # CI has not run (the commit carries [skip ci]) and the branch is only in the local repository
    assert "\n## CI\n" in pr_text and "## Before you merge" not in pr_text
    assert f"Push it from there first (`git push --no-verify origin {branch}`;" in pr_text
    branches = run_git(["for-each-ref", "--format=%(refname:short)", "refs/heads"], bench.repo).stdout
    assert branch in branches.split()
    assert "strptime" in (bench.repo / "src/etl/customers.py").read_text(
        encoding="utf-8"
    )  # working tree untouched
    assert [a["subject"] for a in bench.alerts(customers["id"])][0].startswith("[dag-healer] Fix proposed")
    assert not (bench.home / "work" / customers["id"]).exists()

    # ---------------------------------------------------------------- AMBIGUOUS: inventory
    inventory = bench.incident("inventory_report")
    assert (inventory["status"], inventory["category"]) == ("waiting_human", "ambiguous")
    assert inventory["error_summary"].startswith("KeyError: 'qty'")
    inventory_events = bench.events(inventory["id"])
    assert _classification(inventory_events)["rule"] == "c_no_approved_contract"
    assert inventory["violated_rule"]["column"] == "qty"
    assert inventory["violated_rule"]["contract_status"] == "draft"
    assert [a["subject"] for a in bench.alerts(inventory["id"])] == [
        "[dag-healer] Human decision needed: inventory_report.build_report"
    ]

    # ---------------------------------------------------------------- resolve(code) without a script
    empty_scripts = tmp_path / "no_scripts"
    empty_scripts.mkdir()
    monkeypatch.setenv("HEALER_FAKE_LLM_DIR", str(empty_scripts))
    bench.command(
        "resolve", "inventory_report", incident_id=inventory["id"], actor="alice", resolution="code"
    )
    stats = bench.run_once()
    assert stats.commands == 1 and stats.commands_failed == 0
    inventory = bench.incident("inventory_report")
    assert (inventory["status"], inventory["category"]) == ("repair_failed", "code")
    assert (inventory["resolution"], inventory["resolved_by"]) == ("code", "alice")
    inventory_events = bench.events(inventory["id"])
    assert _statuses(inventory_events)[-5:] == [
        "resumed",
        "triaging",
        "classified",
        "repairing",
        "repair_failed",
    ]
    assert _classification(inventory_events)["rule"] == "human_resolution"
    assert _classification(inventory_events)["decided_by"] == "alice"
    failed_repair = [e for e in inventory_events if e["kind"] == "repair"][-1]["data"]
    assert "failing reproducing test" in failed_repair["error"]
    assert bench.alerts(inventory["id"])[-1]["subject"].startswith("[dag-healer] Repair failed")

    # ---------------------------------------------------------------- retry_repair with the scripts
    monkeypatch.delenv("HEALER_FAKE_LLM_DIR")
    bench.command("retry_repair", "inventory_report", incident_id=inventory["id"], actor="bob")
    bench.run_once()
    inventory = bench.incident("inventory_report")
    assert inventory["status"] == "repaired", bench.events(inventory["id"])[-3:]
    assert _diff_files(inventory["diff"]) == {"src/etl/inventory.py", "tests/test_inventory_build_report.py"}
    assert Path(inventory["pr_md_path"]).is_file()

    # ---------------------------------------------------------------- resolve(data) on another incident
    bench.fail("inventory_report", RUN_2)
    bench.run_once()
    second = bench.incident("inventory_report")
    assert second["id"] != inventory["id"] and second["run_id"] == RUN_2
    assert second["status"] == "waiting_human"
    bench.command("resolve", "inventory_report", incident_id=second["id"], actor="carol", resolution="data")
    bench.run_once()
    second = bench.incident("inventory_report")
    assert (second["status"], second["category"], second["resolution"]) == ("reported", "data", "data")
    assert second["violated_rule"]["rule"] == "missing_column"
    assert second["violated_rule"]["column"] == "qty"
    assert (
        bench.alerts(second["id"])[-1]["subject"]
        == "[dag-healer] DATA issue in inventory_report.build_report"
    )

    # ---------------------------------------------------------------- ignore, retry_task, approve_contract
    bench.command("resolve", "sales_daily_etl", incident_id=sales["id"], actor="dave", resolution="ignore")
    bench.command("retry_task", "customers_sync", incident_id=customers["id"], actor="erin", run_id=RUN_1)
    contract_file = contract_path(bench.cfg.contracts_dir, "inventory_report")
    assert load_contract(contract_file).status == "draft"
    from dag_healer.contracts.io import compute_source_hash

    bench.command(
        "approve_contract",
        "inventory_report",
        actor="frank",
        path="healer/contracts/inventory_report.yaml",
        source_hash=compute_source_hash(load_contract(contract_file)),
    )
    stats = bench.run_once()
    assert (stats.commands, stats.commands_failed) == (3, 0)
    sales = bench.incident("sales_daily_etl")
    assert (sales["status"], sales["resolution"], sales["resolved_by"]) == ("ignored", "ignore", "dave")
    assert bench.client.cleared == [
        {
            "dag_id": "customers_sync",
            "run_id": RUN_1,
            "task_ids": ["transform"],
            "only_failed": True,
            "include_downstream": True,
        }
    ]
    assert bench.incident("customers_sync")["status"] == "repaired"
    retry_event = [e for e in bench.events(customers["id"]) if e["kind"] == "command"][-1]
    assert "not merged yet" in retry_event["message"]
    approved = load_contract(contract_file)
    assert (approved.status, approved.approved_by) == ("approved", "frank")
    with session_scope(bench.cfg.database_url) as session:
        state = store_repo.get_contract_state(session, "inventory_report")
        assert state is not None and state.status == "approved" and state.approved_by == "frank"

    # ---------------------------------------------------------------- nothing raw was persisted
    stored = _stored_text(bench)
    leaked = sorted(email for email in raw_emails(bench.repo) if email in stored)
    assert leaked == []
    for path in (bench.home / "prs").rglob("PR.md"):
        text = path.read_text(encoding="utf-8")
        assert not [email for email in raw_emails(bench.repo) if email in text]


def test_listener_event_is_confirmed_then_triaged(bench: Bench) -> None:
    """A pushed listener event becomes an incident only once the REST API confirms the failure."""
    task_id = bench.fail("sales_daily_etl")
    bench.client.listed.clear()  # the REST poll does not see it (yet)
    _task, exc = bench.failures["sales_daily_etl"]
    with session_scope(bench.cfg.database_url) as session:
        store_repo.add_pending_event(
            session,
            dag_id="sales_daily_etl",
            run_id=RUN_1,
            task_id=task_id,
            map_index=-1,
            try_number=1,
            payload={
                "source": "listener",
                "error": "".join(traceback.format_exception(exc)),
                "state": "failed",
            },
        )

    stats = bench.run_once()

    assert stats.created == 1
    incident = bench.incident("sales_daily_etl")
    assert (incident["source"], incident["status"], incident["category"]) == ("listener", "reported", "data")
    assert incident["error_summary"].startswith("KeyError: 'amount'")
    assert "confirmed" in _kinds(bench.events(incident["id"]))
    with session_scope(bench.cfg.database_url) as session:
        assert [e.status for e in session.query(PendingEvent).all()] == ["confirmed"]


#: The demo's outcome of each DAG: (status, category).
DEMO_OUTCOMES: dict[str, tuple[str, str]] = {
    "sales_daily_etl": ("reported", "data"),
    "customers_sync": ("repaired", "code"),
    "inventory_report": ("waiting_human", "ambiguous"),
}
#: Where the repair prompts start quoting the evidence (the repository's code and files).
EVIDENCE_HEADING = "## Evidence (masked)"
#: A fenced block of a prompt or of PR.md (its fence is longer than any backtick run inside).
FENCED_BLOCK = re.compile(r"^(?P<fence>`{3,})[^`\n]*\n.*?^(?P=fence)[ \t]*$", re.DOTALL | re.MULTILINE)


def own_words(text: str) -> str:
    """``text`` without its fenced blocks, which quote code, diffs, tracebacks and pytest output."""
    return FENCED_BLOCK.sub("", text)


class RecordingProvider(FakeProvider):
    """The fake LLM provider, keeping every ``(kind, system prompt, user prompt)`` it is given."""

    def __init__(self, cfg: Any) -> None:
        super().__init__(cfg)
        self.prompts: list[tuple[str, str, str]] = []

    def classify(self, *, system: str, prompt: str) -> Any:
        self.prompts.append(("classify", system, prompt))
        return super().classify(system=system, prompt=prompt)

    def repair_session(self, *, system: str, prompt: str, **kwargs: Any) -> Any:
        self.prompts.append(("repair", system, prompt))
        return super().repair_session(system=system, prompt=prompt, **kwargs)

    def worded_texts(self) -> list[tuple[str, str]]:
        """The labelled prompt texts dag-healer writes in the orchestrator's words.

        The classify prompt leaves out its ``<incident>`` JSON, a repair prompt its evidence
        section and its fenced blocks: they quote the example repository (DAG files,
        ``src/etl/common.py``) and the machine (absolute paths), which may mention Airflow
        whatever the orchestrator.
        """
        texts: list[tuple[str, str]] = []
        for number, (kind, system, prompt) in enumerate(self.prompts):
            texts.append((f"{kind} system prompt {number}", system))
            if kind == "classify":
                own = re.sub(r"<incident>.*?</incident>", "", prompt, flags=re.DOTALL)
            else:
                own = own_words(prompt.split(EVIDENCE_HEADING, 1)[0])
            texts.append((f"{kind} prompt {number}", own))
        return texts


@pytest.mark.parametrize("orchestrator", ["airflow", "fake"])
def test_the_demo_outcomes_through_each_orchestrator(bench: Bench, orchestrator: str) -> None:
    """The same three outcomes through the Airflow adapter and through Fakeflow, each in its own words."""
    from dag_healer.alerts import notify, render_alert

    if orchestrator == "fake":
        bench.use_fakeflow()
    for dag_id in ETL_TASKS:
        bench.fail(dag_id)
    provider = RecordingProvider(bench.cfg)
    alerts: list[tuple[str, dict[str, Any], str]] = []

    def notifier(cfg: Any, incident: dict[str, Any], event: str, *, extra: dict[str, Any]) -> Any:
        alerts.append((event, dict(extra), render_alert(cfg, incident, event, extra=extra).text))
        return notify(cfg, incident, event, extra=extra)

    stats = bench.run_once(provider, notifier=notifier)

    assert stats.errors == [] and stats.created == 3
    assert stats.processed == {"reported": 1, "repaired": 1, "waiting_human": 1}
    instance, source = ("airflow", "rest") if orchestrator == "airflow" else ("fake", "poll")
    for dag_id, outcome in DEMO_OUTCOMES.items():
        incident = bench.incident(dag_id)
        assert (incident["status"], incident["category"]) == outcome, bench.events(incident["id"])
        assert (incident["orchestrator"], incident["source"]) == (instance, source)
        assert incident["task_id"] == bench.failures[dag_id][0]
    customers = bench.incident("customers_sync")
    assert _diff_files(customers["diff"]) == {"src/etl/customers.py", customers["test_path"]}
    pr_md = Path(customers["pr_md_path"]).read_text(encoding="utf-8")

    # prompts and PR.md name the orchestrator, never the other one
    own, other = ("Airflow", "Fakeflow") if orchestrator == "airflow" else ("Fakeflow", "Airflow")
    assert [kind for kind, _system, _prompt in provider.prompts].count("classify") == 3
    assert "repair" in [kind for kind, _system, _prompt in provider.prompts]
    for label, text in [*provider.worded_texts(), ("PR.md", own_words(pr_md))]:
        assert own in text, label
        assert other not in text, label

    # every alert gets the orchestrator's words and the links into its own UI (none for Airflow,
    # whose UI hosts the Healer views)
    assert sorted(event for event, _extra, _text in alerts) == ["repaired", "reported", "waiting_human"]
    for _event, extra, text in alerts:
        assert extra["vocabulary"]["product"] == own
        if orchestrator == "airflow":
            assert extra["native_links"] == {} and "Open in" not in text
        else:
            (url,) = extra["native_links"].values()
            assert url.startswith("http://fakeflow.localhost/flows/") and "Open in Fakeflow" in text


def test_retry_repair_works_once_the_test_sandbox_does(bench: Bench) -> None:
    """Without a usable sandbox a CODE incident runs no code and fails; a retry later repairs it."""
    bench.cfg.repo.sandbox = SandboxConfig()  # the default container backend, without an image
    bench.fail("customers_sync")
    provider = RecordingProvider(bench.cfg)

    stats = bench.run_once(provider)

    assert stats.errors == [] and stats.processed == {"repair_failed": 1}
    customers = bench.incident("customers_sync")
    assert (customers["status"], customers["category"]) == ("repair_failed", "code")
    assert (customers["attempts"], customers["test_path"], customers["diff"]) == (0, None, None)
    failed = [event for event in bench.events(customers["id"]) if event["kind"] == "repair"][-1]["data"]
    assert "repo.sandbox.image is not set" in failed["error"] and "retry the repair" in failed["error"]
    assert [kind for kind, _system, _prompt in provider.prompts] == ["classify"]  # no repair session

    bench.cfg.repo.sandbox.backend = "process"  # the sandbox works now
    bench.command("retry_repair", "customers_sync", incident_id=customers["id"], actor="bob")
    stats = bench.run_once(provider)

    assert stats.errors == [] and stats.processed == {"repaired": 1}
    customers = bench.incident("customers_sync")
    assert (customers["status"], customers["category"]) == ("repaired", "code")
    assert _diff_files(customers["diff"]) == {"src/etl/customers.py", customers["test_path"]}


# =========================================================================== reconcile (no repo needed)


def make_cfg(tmp_path: Path, **sections: Any) -> HealerConfig:
    repo = tmp_path / "repo"
    (repo / "dags").mkdir(parents=True, exist_ok=True)
    data: dict[str, Any] = {
        # the unit tests have no container engine: pytest runs in the unisolated process sandbox
        "repo": {"path": str(repo), "sandbox": {"backend": "process"}},
        "store": {"url": f"sqlite:///{(tmp_path / 'h.db').as_posix()}"},
    }
    data.update(sections)
    return config_from_mapping(data)


SIMPLE_LOG = (
    "[2026-10-17T10:00:00Z] ERROR - Task failed with exception\n"
    "Traceback (most recent call last):\n"
    '  File "/opt/airflow/repo/src/etl/job.py", line 3, in run\n'
    "    raise ValueError('bad row for jane.doe@example.com')\n"
    "ValueError: bad row for jane.doe@example.com\n"
)


@pytest.fixture
def cfg(tmp_path: Path, healer_env: Path) -> HealerConfig:
    return make_cfg(tmp_path)


def _incidents(cfg: HealerConfig) -> list[Incident]:
    with session_scope(cfg.database_url) as session:
        return sorted(session.query(Incident).all(), key=lambda i: (i.dag_id, i.run_id, i.try_number))


def _pending(cfg: HealerConfig) -> list[str]:
    with session_scope(cfg.database_url) as session:
        return [event.status for event in session.query(PendingEvent).order_by(PendingEvent.id).all()]


def test_reconcile_creates_masked_incidents_and_advances_the_watermark(cfg: HealerConfig) -> None:
    client = FakeAirflowClient()
    client.add_failure("etl", RUN_1, "run", SIMPLE_LOG)

    stats = reconcile(cfg, None, client, now=NOW)

    assert stats.errors == [] and len(stats.created) == 1
    assert client.list_calls == [NOW - pipeline.INITIAL_LOOKBACK]
    (incident,) = _incidents(cfg)
    assert (incident.source, incident.status, incident.dag_id, incident.task_id) == (
        "rest",
        "new",
        "etl",
        "run",
    )
    assert incident.error_summary.startswith("ValueError: bad row for [email]")
    assert "jane.doe@example.com" not in incident.error_summary
    assert incident.signature
    assert get_watermark(cfg) == NOW

    # the next poll overlaps the previous one and is idempotent
    later = NOW + timedelta(minutes=5)
    stats = reconcile(cfg, None, client, now=later)
    assert stats.created == [] and stats.known == 1
    assert client.list_calls[-1] == NOW - timedelta(seconds=60)
    assert get_watermark(cfg) == later


def test_reconcile_skips_unmonitored_dags_and_survives_api_errors(tmp_path: Path, healer_env: Path) -> None:
    cfg = make_cfg(tmp_path, monitor={"dags": ["etl*", "!etl_private"]})
    client = FakeAirflowClient()
    client.add_failure("etl_private", RUN_1, "run", SIMPLE_LOG)
    client.add_failure("other", RUN_1, "run", SIMPLE_LOG)
    stats = reconcile(cfg, None, client, now=NOW)
    assert stats.unmonitored == 2 and stats.created == []

    client.list_error = AirflowAPIError("boom", status_code=503)
    stats = reconcile(cfg, None, client, now=NOW + timedelta(minutes=1))
    assert stats.errors and "503" in stats.errors[0]
    assert get_watermark(cfg) == NOW  # not advanced by the failed poll


def test_reconcile_folds_repeated_failures_into_the_open_incident(cfg: HealerConfig) -> None:
    client = FakeAirflowClient()
    client.add_failure("etl", RUN_1, "run", SIMPLE_LOG)
    reconcile(cfg, None, client, now=NOW)
    client.add_failure("etl", RUN_2, "run", SIMPLE_LOG.replace("line 3", "line 4"))

    stats = reconcile(cfg, None, client, now=NOW + timedelta(minutes=1))

    assert stats.folded == 1 and stats.created == []
    (incident,) = _incidents(cfg)
    assert incident.occurrences == 2


def test_reconcile_without_a_client_keeps_pending_events(cfg: HealerConfig) -> None:
    with session_scope(cfg.database_url) as session:
        store_repo.add_pending_event(session, dag_id="etl", run_id=RUN_1, task_id="run", try_number=1)
    stats = reconcile(cfg, None, None, now=NOW)
    assert stats.pending_left == 1 and stats.errors
    assert _pending(cfg) == ["pending_check"]


@pytest.mark.parametrize(
    ("trigger", "state", "ti_try", "event_try", "expected"),
    [
        ("final_failure", "failed", 1, 1, "confirmed"),
        ("final_failure", "up_for_retry", 1, 1, "discarded"),
        ("final_failure", "failed", 2, 1, "discarded"),  # try 1 was retried
        ("final_failure", "success", 1, 1, "discarded"),  # forged / stale event
        ("final_failure", "failed", 1, 2, "discarded"),  # a try Airflow never ran
        ("every_failure", "up_for_retry", 1, 1, "confirmed"),
        ("every_failure", "running", 2, 1, "confirmed"),
        ("every_failure", "running", 1, 1, "pending_check"),  # not ended yet: checked again later
        ("final_failure", "running", 1, 1, "pending_check"),
    ],
)
def test_pending_event_confirmation(
    tmp_path: Path, healer_env: Path, trigger: str, state: str, ti_try: int, event_try: int, expected: str
) -> None:
    cfg = make_cfg(tmp_path, monitor={"modes": ["listener"], "trigger": trigger})
    client = FakeAirflowClient()
    client.add_failure(
        "etl", RUN_1, "run", SIMPLE_LOG, try_number=ti_try, max_tries=1, state=state, listed=False
    )
    with session_scope(cfg.database_url) as session:
        store_repo.add_pending_event(
            session,
            dag_id="etl",
            run_id=RUN_1,
            task_id="run",
            try_number=event_try,
            payload={"source": "listener", "error": SIMPLE_LOG},
        )

    stats = reconcile(cfg, None, client, now=NOW)

    assert _pending(cfg) == [expected]
    incidents = _incidents(cfg)
    if expected == "confirmed":
        assert stats.confirmed == 1
        assert [(i.source, i.try_number) for i in incidents] == [("listener", event_try)]
        assert "jane.doe@example.com" not in incidents[0].error_summary
    elif expected == "pending_check":
        assert (stats.pending_left, stats.discarded, stats.errors, incidents) == (1, 0, [], [])
    else:
        assert stats.discarded == 1 and incidents == []


def test_a_listener_event_that_beats_the_end_of_the_try_is_confirmed_later(
    tmp_path: Path, healer_env: Path, caplog: pytest.LogCaptureFixture
) -> None:
    """Airflow records the end of a try a few seconds after the listener posted its failure."""
    cfg = make_cfg(tmp_path, monitor={"modes": ["listener"]})
    client = FakeAirflowClient()
    client.add_failure("etl", RUN_1, "run", SIMPLE_LOG, state="running", listed=False)
    with session_scope(cfg.database_url) as session:
        store_repo.add_pending_event(
            session,
            dag_id="etl",
            run_id=RUN_1,
            task_id="run",
            try_number=1,
            payload={"source": "listener", "error": SIMPLE_LOG},
        )

    with caplog.at_level(logging.INFO, logger="dag_healer.worker.pipeline"):
        early = reconcile(cfg, None, client, now=NOW)
    assert (early.pending_left, early.discarded, early.errors) == (1, 0, [])  # no error: not ended yet
    assert _pending(cfg) == ["pending_check"] and _incidents(cfg) == []
    assert not [r for r in caplog.records if r.name == pipeline.__name__ and r.levelno >= logging.WARNING]
    assert (
        "pending event 1 is checked again later: the task instance is running, not failed yet" in caplog.text
    )

    client.instances[("etl", RUN_1, "run", -1)].state = "failed"
    later = reconcile(cfg, None, client, now=NOW + timedelta(seconds=5))
    assert later.confirmed == 1 and _pending(cfg) == ["confirmed"]
    assert [(i.source, i.task_id) for i in _incidents(cfg)] == [("listener", "run")]


def test_pending_events_are_discarded_for_unknown_tis_disabled_modes_and_unmonitored_dags(
    tmp_path: Path, healer_env: Path
) -> None:
    cfg = make_cfg(tmp_path, monitor={"modes": ["rest", "listener"], "dags": ["etl"]})
    client = FakeAirflowClient()
    client.add_failure("etl", RUN_1, "known", SIMPLE_LOG, listed=False)
    client.instance_errors[("etl", RUN_1, "flaky", -1)] = AirflowAPIError("timeout")
    with session_scope(cfg.database_url) as session:
        for task_id, source, dag_id in [
            ("missing", "listener", "etl"),  # 404 -> discarded
            ("known", "callback", "etl"),  # callback mode disabled -> discarded
            ("known", "listener", "other"),  # not monitored -> discarded
            ("flaky", "listener", "etl"),  # API error -> stays pending
        ]:
            store_repo.add_pending_event(
                session,
                dag_id=dag_id,
                run_id=RUN_1,
                task_id=task_id,
                try_number=1,
                payload={"source": source},
            )

    stats = reconcile(cfg, None, client, now=NOW)

    assert _pending(cfg) == ["discarded", "discarded", "discarded", "pending_check"]
    assert stats.discarded == 3 and stats.pending_left == 1
    # the phase-5 text of an Airflow API error
    assert stats.errors == [
        "confirming pending event 4: AirflowAPIError: Airflow API request failed: timeout"
    ]
    assert _incidents(cfg) == []

    # still unconfirmed a day later: discarded
    reconcile(cfg, None, client, now=datetime.now(timezone.utc) + timedelta(days=2))
    assert _pending(cfg)[-1] == "discarded"


def test_pending_events_of_other_instances_are_left_alone(cfg: HealerConfig) -> None:
    client = FakeAirflowClient()
    client.add_failure("etl", RUN_1, "run", SIMPLE_LOG, listed=False)
    with session_scope(cfg.database_url) as session:
        for orchestrator in ("airflow-staging", "airflow"):
            store_repo.add_pending_event(
                session,
                orchestrator=orchestrator,
                dag_id="etl",
                run_id=RUN_1,
                task_id="run",
                try_number=1,
                payload={"source": "listener", "error": SIMPLE_LOG},
            )

    stats = reconcile(cfg, None, client, now=NOW)

    assert (stats.confirmed, stats.discarded, stats.pending_left) == (1, 0, 0)
    assert _pending(cfg) == ["pending_check", "confirmed"]  # the staging worker's event is untouched
    assert [(i.orchestrator, i.source) for i in _incidents(cfg)] == [("airflow", "listener")]


def test_queued_hints_are_stored_then_confirmed_by_the_adapter(tmp_path: Path, healer_env: Path) -> None:
    cfg = make_cfg(tmp_path, monitor={"modes": ["poll", "queue"]})
    world = FakeWorld(clock=lambda: NOW)
    world.add_pipeline("etl", file="dags/etl.py", steps={"run": TaskInfo("run", "python")})
    world.fail("etl", RUN_1, "run", text=SIMPLE_LOG)
    with session_scope(cfg.database_url) as session:  # the poll is already past this failure
        store_repo.kv_set(session, store_repo.cursor_key("fake"), {"seq": 99})
    hint = {"pipeline_id": "etl", "run_id": RUN_1, "step_id": "run", "error": SIMPLE_LOG}
    world.push("fake", {**hint, "input": {"rows": [{"email": "jane.doe@example.com"}]}})
    world.push("fake", {**hint, "run_id": "forged__2026-10-17"})
    world.push("fake", {"run_id": RUN_1})  # malformed: dropped (and acknowledged)

    stats = reconcile(cfg, None, None, now=NOW, adapter=FakeAdapter(cfg, world=world))

    assert stats.errors == [] and (stats.queued, stats.confirmed, stats.discarded) == (2, 1, 1)
    assert world.queue == []
    with session_scope(cfg.database_url) as session:
        rows = [event.to_dict() for event in session.query(PendingEvent).order_by(PendingEvent.id)]
    assert [(r["orchestrator"], r["channel"], r["run_id"], r["status"]) for r in rows] == [
        ("fake", "fake", RUN_1, "confirmed"),
        ("fake", "fake", "forged__2026-10-17", "discarded"),  # Fakeflow does not know this run
    ]
    assert rows[0]["payload"] == {"error": mask_text(SIMPLE_LOG), "source": "push", "via": "queue"}
    assert "jane.doe@example.com" not in json.dumps(rows)
    (incident,) = _incidents(cfg)
    assert (incident.orchestrator, incident.source, incident.dag_id, incident.run_id) == (
        "fake",
        "push",
        "etl",
        RUN_1,
    )
    assert incident.error_summary.startswith("ValueError: bad row for [email]")
    with session_scope(cfg.database_url) as session:
        notes = [e.message for e in store_repo.list_events(session, incident.id) if e.kind == "confirmed"]
    assert notes == ["The push event was confirmed by the Fakeflow API (state failed, attempt 1)."]


def test_a_queued_message_the_store_rejects_does_not_block_the_others(
    tmp_path: Path, healer_env: Path, monkeypatch: pytest.MonkeyPatch, caplog: pytest.LogCaptureFixture
) -> None:
    cfg = make_cfg(tmp_path, monitor={"modes": ["queue"]})
    world = FakeWorld(clock=lambda: NOW)
    world.add_pipeline("etl", file="dags/etl.py", steps={"run": TaskInfo("run", "python")})
    world.fail("etl", RUN_1, "run", text=SIMPLE_LOG)
    adapter = FakeAdapter(cfg, world=world)
    normalize = adapter.normalize

    def losing_the_run_id(channel: str, payload: Any, headers: Any) -> list[Any]:
        hints = normalize(channel, payload, headers)
        if payload.get("event_id") != "broken":
            return hints
        return [dataclasses.replace(h, ref=dataclasses.replace(h.ref, run_id="")) for h in hints]

    monkeypatch.setattr(adapter, "normalize", losing_the_run_id)
    hint = {"pipeline_id": "etl", "run_id": RUN_1, "step_id": "run", "error": SIMPLE_LOG}
    world.push("fake", {**hint, "event_id": "broken"})  # the store rejects its pending event
    world.push("fake", hint)

    with caplog.at_level(logging.WARNING, logger="dag_healer.worker.pipeline"):
        stats = reconcile(cfg, None, None, now=NOW, adapter=adapter)

    assert stats.errors == [] and (stats.queued, stats.confirmed) == (1, 1)
    assert world.queue == []  # both acknowledged: the rejected message is dropped, not redelivered
    assert _pending(cfg) == ["confirmed"]
    assert (
        "dropping a queued fake event the store cannot keep: ValueError: Pending event is missing run_id"
        in (caplog.text)
    )


def test_queued_messages_stay_in_the_queue_while_the_store_fails(
    tmp_path: Path, healer_env: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    cfg = make_cfg(tmp_path, monitor={"modes": ["queue"]})
    world = FakeWorld(clock=lambda: NOW)
    world.add_pipeline("etl", file="dags/etl.py", steps={"run": TaskInfo("run", "python")})
    for run_id in (RUN_1, RUN_2):
        world.push("fake", {"pipeline_id": "etl", "run_id": run_id, "step_id": "run"})
    adapter = FakeAdapter(cfg, world=world)
    add_pending_event = store_repo.add_pending_event

    def store_down(*_args: Any, **_kwargs: Any) -> Any:
        raise RuntimeError("the store is down")

    monkeypatch.setattr(store_repo, "add_pending_event", store_down)
    stats = reconcile(cfg, None, None, now=NOW, adapter=adapter)
    assert stats.errors == ["storing queued events: RuntimeError: the store is down"]
    assert stats.queued == 0 and len(world.queue) == 2  # nothing acknowledged: delivered again

    monkeypatch.setattr(store_repo, "add_pending_event", add_pending_event)
    stats = reconcile(cfg, None, None, now=NOW, adapter=adapter)
    assert stats.errors == [] and stats.queued == 2 and world.queue == []


def test_a_reset_keeps_old_fakeflow_failures_out_of_every_poll(tmp_path: Path, healer_env: Path) -> None:
    cfg = make_cfg(tmp_path, orchestrator={"type": "fake"}, monitor={"modes": ["poll"]})
    world = FakeWorld(clock=lambda: NOW)
    world.add_pipeline("etl", file="dags/etl.py", steps={"run": TaskInfo("run", "python")})
    world.fail("etl", RUN_1, "run", text=SIMPLE_LOG, at=NOW - timedelta(hours=2))
    adapter = FakeAdapter(cfg, world=world)
    floor = reset_state(cfg, NOW - timedelta(hours=1))  # what dag-healer store reset records

    for minutes in (0, 1, 2):  # a poll that lists nothing keeps the floor for the next one
        stats = reconcile(cfg, None, None, now=NOW + timedelta(minutes=minutes), adapter=adapter)
        assert (stats.errors, stats.created) == ([], [])
    assert _incidents(cfg) == [] and get_not_before(cfg) == floor

    world.fail("etl", RUN_2, "run", text=SIMPLE_LOG, at=NOW + timedelta(minutes=3))
    stats = reconcile(cfg, None, None, now=NOW + timedelta(minutes=4), adapter=adapter)
    assert len(stats.created) == 1 and [i.run_id for i in _incidents(cfg)] == [RUN_2]


# =========================================================================== process_incident


def _new_incident(cfg: HealerConfig, *, error_summary: str = "", **extra: Any) -> str:
    with session_scope(cfg.database_url) as session:
        incident, _created = store_repo.create_incident(
            session,
            dag_id="etl",
            run_id=RUN_1,
            task_id="run",
            try_number=1,
            source="rest",
            error_summary=error_summary,
            **extra,
        )
        return incident.id


def test_unexpected_failures_put_the_incident_in_error_with_a_masked_event(
    cfg: HealerConfig, monkeypatch: pytest.MonkeyPatch
) -> None:
    incident_id = _new_incident(cfg)

    def broken(*_args: Any, **_kwargs: Any) -> Any:
        raise RuntimeError("cannot read the export of jane.doe@example.com\nsecond line")

    monkeypatch.setattr("dag_healer.agent.context.gather_context", broken)

    status = process_incident(cfg, None, incident_id, provider=FakeProvider(cfg), client=None)

    assert status == "error"
    (incident,) = _incidents(cfg)
    assert incident.status == "error"
    with session_scope(cfg.database_url) as session:
        events = store_repo.list_events(session, incident_id)
        error = [e for e in events if e.kind == "error"][-1]
        assert error.message == (
            "Processing failed while gathering the context: RuntimeError: cannot read the export of [email]"
        )
        assert error.data["stage"] == "gathering the context"
        assert "jane.doe" not in json.dumps([e.to_dict() for e in events])


def test_process_incident_without_evidence_waits_for_a_human(cfg: HealerConfig) -> None:
    incident_id = _new_incident(cfg, error_summary="the job died")
    outcomes: list[tuple[str, dict[str, Any]]] = []

    def notifier(_cfg: Any, incident: dict[str, Any], event: str, *, extra: dict[str, Any]) -> list[Any]:
        from dag_healer.alerts import AlertOutcome

        outcomes.append((event, extra))
        return [AlertOutcome(channel="log", recipients=[], subject=f"[x] {event}", status="sent")]

    status = process_incident(
        cfg, None, incident_id, provider=FakeProvider(cfg), client=None, notifier=notifier
    )

    assert status == "waiting_human"
    assert [event for event, _extra in outcomes] == ["waiting_human"]
    assert outcomes[0][1]["reason"]
    with session_scope(cfg.database_url) as session:
        assert [a.subject for a in store_repo.list_alerts(session, incident_id)] == ["[x] waiting_human"]
    # an incident that is no longer claimable is left alone
    assert (
        process_incident(cfg, None, incident_id, provider=FakeProvider(cfg), client=None) == "waiting_human"
    )


def test_disabled_alert_events_are_not_recorded(tmp_path: Path, healer_env: Path) -> None:
    cfg = make_cfg(tmp_path, notify={"events": ["reported"]})
    incident_id = _new_incident(cfg, error_summary="the job died")
    assert (
        process_incident(cfg, None, incident_id, provider=FakeProvider(cfg), client=None) == "waiting_human"
    )
    with session_scope(cfg.database_url) as session:
        assert store_repo.list_alerts(session, incident_id) == []
        alert_event = [e for e in store_repo.list_events(session, incident_id) if e.kind == "alert"][-1]
        assert "disabled" in alert_event.message


def test_missing_provider_makes_code_repairs_fail_clearly(cfg: HealerConfig) -> None:
    incident_id = _new_incident(cfg, error_summary=SIMPLE_LOG, resolution="code", resolved_by="alice")
    with session_scope(cfg.database_url) as session:
        incident = store_repo.get_incident(session, incident_id)
        assert incident is not None
        store_repo.set_status(session, incident, "resumed", "resumed by a test")

    status = process_incident(cfg, None, incident_id, provider=None, client=None)

    assert status == "repair_failed"
    with session_scope(cfg.database_url) as session:
        repair = [e for e in store_repo.list_events(session, incident_id) if e.kind == "repair"][-1]
        assert repair.data["error"] == "No LLM provider is configured."


def _resumed_code_incident(cfg: HealerConfig) -> str:
    """An incident a human resolved as CODE (``resumed``): processing it goes straight to the repair."""
    incident_id = _new_incident(cfg, error_summary=SIMPLE_LOG, resolution="code", resolved_by="alice")
    with session_scope(cfg.database_url) as session:
        incident = store_repo.get_incident(session, incident_id)
        assert incident is not None
        store_repo.set_status(session, incident, "resumed", "resumed by a test")
    return incident_id


def test_a_sandbox_check_that_fails_runs_no_code(cfg: HealerConfig, monkeypatch: pytest.MonkeyPatch) -> None:
    from dag_healer.repo.sandbox import ContainerSandbox, sandbox_refusal

    def crashing(_self: ContainerSandbox) -> str | None:
        raise RuntimeError("the probe crashed")

    def no_repair(*_args: Any, **_kwargs: Any) -> Any:
        raise AssertionError("no clone, no session and no pytest run when the sandbox check fails")

    cfg.repo.sandbox = SandboxConfig(image="dag-healer/sandbox-test:dev")
    monkeypatch.setattr(ContainerSandbox, "unavailable_reason", crashing)
    monkeypatch.setattr("dag_healer.agent.repair.run_repair", no_repair)
    incident_id = _resumed_code_incident(cfg)
    provider = RecordingProvider(cfg)
    status = process_incident(
        cfg, None, incident_id, provider=provider, client=None, notifier=lambda *_a, **_k: []
    )
    assert status == "repair_failed" and provider.prompts == []
    with session_scope(cfg.database_url) as session:
        repair = [e for e in store_repo.list_events(session, incident_id) if e.kind == "repair"][-1]
        error = dict(repair.data)["error"]
    assert error == sandbox_refusal("the check of the test sandbox failed (RuntimeError: the probe crashed)")


@pytest.mark.parametrize(
    ("sandbox", "reason"),
    [
        ({}, "repo.sandbox.image is not set"),  # the default container backend, without an image
        ({"backend": "vm"}, "'vm'"),  # a backend the sandbox module does not know
    ],
)
def test_an_unusable_sandbox_fails_code_repairs_before_any_code_runs(
    cfg: HealerConfig,
    monkeypatch: pytest.MonkeyPatch,
    caplog: pytest.LogCaptureFixture,
    sandbox: dict[str, str],
    reason: str,
) -> None:
    cfg.repo.sandbox = SandboxConfig()
    for name, value in sandbox.items():  # set without validation, like a duck-typed configuration
        setattr(cfg.repo.sandbox, name, value)
    incident_id = _resumed_code_incident(cfg)

    def no_repair(*_args: Any, **_kwargs: Any) -> Any:
        raise AssertionError("no clone, no session and no pytest run without a usable sandbox")

    monkeypatch.setattr("dag_healer.agent.repair.run_repair", no_repair)
    alerts: list[tuple[str, dict[str, Any]]] = []

    def notifier(_cfg: Any, _incident: dict[str, Any], event: str, *, extra: dict[str, Any]) -> list[Any]:
        alerts.append((event, extra))
        return []

    provider = RecordingProvider(cfg)
    with caplog.at_level(logging.WARNING, logger="dag_healer.worker.pipeline"):
        status = process_incident(cfg, None, incident_id, provider=provider, client=None, notifier=notifier)

    assert status == "repair_failed" and provider.prompts == []
    with session_scope(cfg.database_url) as session:
        incident = store_repo.get_incident(session, incident_id)
        assert incident is not None and incident.category == "code"
        repair = [e for e in store_repo.list_events(session, incident_id) if e.kind == "repair"][-1]
        message, data = repair.message, dict(repair.data)
    error = data["error"]
    assert error.startswith("The test sandbox is not usable: ") and reason in error
    assert "No code was run" in error and "dag-healer doctor" in error and "retry the repair" in error
    assert message == f"Repair failed: {error}"
    assert data == {
        "status": "repair_failed",
        "branch": None,
        "test_path": None,
        "created_tests_dir": False,
        "pr_url": None,
        "pr_md_path": None,
        "patch_path": None,
        "attempts": 0,
        "tokens_used": 0,
        "error": error,
        "notes": [],
    }
    assert [(event, extra["error"]) for event, extra in alerts] == [("repair_failed", error)]
    assert f"incident {incident_id[:8]}: no repair: " in caplog.text


# =========================================================================== helpers


def test_compose_error_summary_puts_the_exception_first_and_masks() -> None:
    summary = compose_error_summary(
        "KeyError",
        "'amount'\nmore",
        "Traceback (most recent call last):\n  File \"x.py\", line 1\nKeyError: 'amount'",
    )
    assert summary.splitlines()[0] == "KeyError: 'amount'"
    assert "Traceback" in summary
    assert compose_error_summary(None, None, None, fallback="mail bob@example.com") == "mail [email]"
    long = compose_error_summary("ValueError", "x", "y" * 20_000)
    assert len(long) <= pipeline.MAX_ERROR_SUMMARY_CHARS and long.startswith("ValueError: x")


def test_failure_signature_ignores_line_numbers_paths_and_numbers() -> None:
    frames_a = [{"file": "/opt/airflow/repo/src/etl/job.py", "line": 3, "function": "run"}]
    frames_b = [{"file": "C:\\work\\repo\\src\\etl\\job.py", "line": 9, "function": "run"}]
    first = failure_signature("etl", "run", "ValueError", "row 12 is bad", frames_a)
    assert first == failure_signature("etl", "run", "builtins.ValueError", "row 345 is bad", frames_b)
    assert first != failure_signature("etl", "other", "ValueError", "row 12 is bad", frames_a)
    assert failure_signature("etl", "run", None, None) is None
    summary, signature = summarize_failure("etl", "run", SIMPLE_LOG)
    assert summary.startswith("ValueError: bad row for [email]") and signature


def test_the_poll_cursor_is_kept_in_healer_kv(cfg: HealerConfig) -> None:
    assert get_watermark(cfg) is None and get_not_before(cfg) is None
    set_watermark(cfg, NOW)
    assert get_watermark(cfg) == NOW and get_not_before(cfg) is None
    floor = reset_state(cfg, NOW + timedelta(hours=1))
    assert get_watermark(cfg) == get_not_before(cfg) == floor == NOW + timedelta(hours=1)
    set_watermark(cfg, NOW + timedelta(hours=2))  # keeps the floor of the reset
    with session_scope(cfg.database_url) as session:
        assert store_repo.kv_get(session, store_repo.cursor_key("airflow")) == {
            "watermark": "2026-10-17T14:00:00+00:00",
            "not_before": "2026-10-17T13:00:00+00:00",
        }
    assert not pipeline.state_path(cfg).exists()  # the phase-5 state file is only ever read


def test_a_phase5_state_file_is_imported_once(tmp_path: Path, healer_env: Path) -> None:
    corrupt = make_cfg(tmp_path / "corrupt")
    pipeline.state_path(corrupt).write_text("{not json", encoding="utf-8")
    assert get_watermark(corrupt) is None  # an unreadable file is not imported

    floor = NOW - timedelta(seconds=30)
    state = {pipeline.WATERMARK_KEY: NOW.isoformat(), pipeline.NOT_BEFORE_KEY: floor.isoformat()}
    pipeline.save_state(corrupt, state)
    cfg = make_cfg(tmp_path / "fresh")
    assert (get_watermark(cfg), get_not_before(cfg)) == (NOW, floor)
    pipeline.save_state(cfg, {pipeline.WATERMARK_KEY: (NOW - timedelta(days=1)).isoformat()})
    assert get_watermark(cfg) == NOW  # imported once: the file no longer counts

    client = FakeAirflowClient()
    later = NOW + timedelta(seconds=10)
    reconcile(cfg, None, client, now=later)
    assert client.list_calls == [floor]  # the window starts at the imported floor
    assert (get_watermark(cfg), get_not_before(cfg)) == (later, floor)
