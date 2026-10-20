"""Tests for dag_healer.worker.pipeline: reconcile, process_incident and the full demo flow.

The flow test runs on a git copy of ``example_existing_repo`` prepared like the demo: the
real ``dag-healer init``, the sales and customers contracts approved with the real CLI, the
day-2 files copied over ``data/incoming``. The Airflow API is a ``FakeAirflowClient`` whose
task logs are rendered (with the real ``render_log_content``) from the REAL exceptions raised
by running the example ETL functions in-process on the day-2 data, so the tracebacks point
at the copy's files. The LLM is the real ``FakeProvider`` replaying the repository's
``fixtures/fake_llm`` scripts, and the store is SQLite under a temporary ``HEALER_HOME``.
"""

from __future__ import annotations

import csv
import importlib
import json
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
from dag_healer.config import HealerConfig, config_from_mapping, load_config
from dag_healer.contracts.io import contract_path, load_contract
from dag_healer.repo.git import run_git
from dag_healer.store import dispose_engines, session_scope
from dag_healer.store import repo as store_repo
from dag_healer.store.models import Alert, Incident, IncidentEvent, PendingEvent
from dag_healer.worker import pipeline
from dag_healer.worker.loop import run_worker
from dag_healer.worker.pipeline import (
    compose_error_summary,
    failure_signature,
    get_watermark,
    process_incident,
    reconcile,
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
        self, dag_id: str, run_id: str, task_ids: list[Any], *, only_failed: bool = True
    ) -> dict[str, Any]:
        self.cleared.append(
            {"dag_id": dag_id, "run_id": run_id, "task_ids": list(task_ids), "only_failed": only_failed}
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

    def fail(self, dag_id: str, run_id: str = RUN_1, *, extra_line: str | None = None) -> str:
        task_id, exc = self.failures.get(dag_id) or run_dag_in_process(self.repo, dag_id)
        self.failures[dag_id] = (task_id, exc)
        self.client.add_failure(
            dag_id, run_id, task_id, airflow_log(dag_id, task_id, exc, extra_line=extra_line)
        )
        return task_id

    def run_once(self, provider: Any | None = None) -> Any:
        return run_worker(
            self.cfg,
            once=True,
            provider=provider or FakeProvider(self.cfg),
            client=self.client,
            install_signal_handlers=False,
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
        {"dag_id": "customers_sync", "run_id": RUN_1, "task_ids": ["transform"], "only_failed": True}
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


# =========================================================================== reconcile (no repo needed)


def make_cfg(tmp_path: Path, **sections: Any) -> HealerConfig:
    repo = tmp_path / "repo"
    (repo / "dags").mkdir(parents=True, exist_ok=True)
    data: dict[str, Any] = {
        "repo": {"path": str(repo)},
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
        ("every_failure", "running", 1, 1, "discarded"),
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
    else:
        assert stats.discarded == 1 and incidents == []


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
    assert _incidents(cfg) == []

    # still unconfirmed a day later: discarded
    reconcile(cfg, None, client, now=datetime.now(timezone.utc) + timedelta(days=2))
    assert _pending(cfg)[-1] == "discarded"


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


def test_watermark_state_file_round_trip_and_corruption(cfg: HealerConfig) -> None:
    assert get_watermark(cfg) is None
    set_watermark(cfg, NOW)
    assert get_watermark(cfg) == NOW
    pipeline.state_path(cfg).write_text("{not json", encoding="utf-8")
    assert get_watermark(cfg) is None
