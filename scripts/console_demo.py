#!/usr/bin/env python3
"""Seed a scratch dag-healer to try the console with ``dag-healer serve``: no Airflow, AWS or LLM.

    python scripts/console_demo.py [--dir .demo/console]

It writes two configurations into DIR, one store shared by both (``DIR/healer.db``, SQLite) and
the files the incidents point at (``DIR/home``, the ``HEALER_HOME`` that holds PR.md files):

* ``airflow/healer.yaml``: the default Airflow instance with the demo's three DAGs
  (``example_existing_repo``). Their contracts are inferred from the demo's day-1 files, as
  ``dag-healer init`` drafts them; the ``sales_daily_etl`` and ``customers_sync`` ones are
  approved, ``inventory_report`` stays a draft.
* ``stepfunctions/healer.yaml``: the Step Functions instance ``sfn-etl-euw1`` of
  ``examples/stepfunctions_repo`` (its SAM template and contracts; the sales and customers
  contracts approved). Nothing calls AWS: ``serve`` only builds the adapter for its words and
  links.

The incidents tell the demo's story. The three of the golden snapshots
(``tests/unit/fixtures/golden/<case>/incident_detail.json``: what dag-healer records when the
demo runs) come first: ``inventory_report`` waiting for a decision, ``customers_sync`` with a
proposed fix and its PR.md, ``sales_daily_etl`` reported as a data problem. More incidents of
the same pipelines show the other statuses (classifying, fixing, a fix that failed, a worker
error, an ignored run), and the Step Functions instance has its own (a data problem, a
low-confidence decision, a timeout). Times are relative to now.

Then serve one instance at a time (``HEALER_HOME`` lets the API find the PR.md files)::

    HEALER_HOME=DIR/home dag-healer serve -c DIR/airflow/healer.yaml
    HEALER_HOME=DIR/home dag-healer serve -c DIR/stepfunctions/healer.yaml --port 8091

and open http://127.0.0.1:8090/plugin/healer (the dev backend signs every request in as ``dev``,
an admin, on a loopback address only). Actions are queued as on a real deployment; no worker runs
here, so they stay queued. Running the script again starts from scratch (``--keep`` refuses to
touch an existing DIR).

Standard library plus dag-healer itself (``pip install -e ".[dev]"``).
"""

from __future__ import annotations

import argparse
import json
import shutil
import sys
from collections.abc import Callable, Iterable, Mapping
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parents[1]
GOLDEN = ROOT / "tests" / "unit" / "fixtures" / "golden"
DEMO_REPO = ROOT / "example_existing_repo"
SFN_REPO = ROOT / "examples" / "stepfunctions_repo"
DEFAULT_DIR = ROOT / ".demo" / "console"

AIRFLOW = "airflow"
SFN = "sfn-etl-euw1"
SFN_ACCOUNT = "123456789012"
SFN_REGION = "eu-west-1"
SFN_MACHINES = {
    "sales_daily_etl": "SalesDailyEtlStateMachine-AbC123",
    "customers_sync": "customers-sync",
    "inventory_report": "InventoryReportStateMachine-XyZ789",
}
APPROVER = "dev"
#: The golden snapshots normalise the branch name and reduce the pytest output (see their README).
GOLDEN_BRANCH = "healer/customers_sync-transform-[number]"
#: The policy's reason when no fixed rule applies and the model decides (``agent/policy.py``).
LLM_DECIDES = (
    "No deterministic gate applies (contract: {contract}, error kind: code); the LLM classification decides."
)
REDUCED = "[pytest output reduced by the golden snapshot test to its E/FAILED/ERROR lines]\n"

AIRFLOW_CONFIG = """\
# dag-healer of the console demo (scripts/console_demo.py): the Airflow instance.
version: 1
repo:
  path: "{repo}"
contracts:
  dir: healer/contracts
monitor:
  dags: ["*"]
store:
  url: "{store}"
notify:
  email:
    host: smtp.example.com
    from: dag-healer@example.com
    to: [data-team@example.com]
serve:
  auth:
    backend: dev
"""

SFN_CONFIG = """\
# dag-healer of the console demo (scripts/console_demo.py): the Step Functions instance.
version: 1
repo:
  path: "{repo}"
  dags_dir: statemachine
  allowed_paths: ["functions/", "src/"]
orchestrator:
  type: stepfunctions
  instance: {instance}
  options:
    region: {region}
    template: template.yaml
    pipelines:
{pipelines}
monitor:
  dags: ["*"]
  modes: [poll]
contracts:
  dir: healer/contracts
datasets:
  sales_daily_etl:
    TransformSales: {{"type": "csv", "path": "s3://acme-landing/incoming/sales.csv"}}
  customers_sync:
    TransformCustomers: {{"type": "csv", "path": "s3://acme-landing/incoming/customers.csv"}}
  inventory_report:
    BuildReport: {{"type": "csv", "path": "s3://acme-landing/incoming/inventory.csv"}}
store:
  url: "{store}"
serve:
  port: 8091
  auth:
    backend: dev
"""


# --------------------------------------------------------------------------- files


def _contracts_airflow(repo: Path) -> None:
    """The demo's contracts, inferred from its day-1 files like ``dag-healer init`` does."""
    import pandas as pd

    from dag_healer.contracts.infer import infer_contract, infer_dataset_contract
    from dag_healer.contracts.io import approve_contract, save_contract

    sources = {
        "sales_daily_etl": ("sales", "extract_sales"),
        "customers_sync": ("customers", "extract"),
        "inventory_report": ("inventory", "get_inventory"),
    }
    for dag_id, (dataset, task_id) in sources.items():
        relative = f"data/incoming/{dataset}.csv"
        frame = pd.read_csv(DEMO_REPO / relative)
        contract = infer_contract(
            dag_id,
            [
                infer_dataset_contract(
                    frame, name=dataset, task_id=task_id, locator={"type": "csv", "path": relative}
                )
            ],
            inferred_from=[f"{relative} ({len(frame)} rows)"],
        )
        if dag_id != "inventory_report":
            contract = approve_contract(contract, APPROVER)
        save_contract(contract, repo / "healer" / "contracts" / f"{dag_id}.yaml")


def _contracts_sfn(repo: Path) -> None:
    """The Step Functions example's contracts, with sales and customers approved."""
    from dag_healer.contracts.io import approve_contract, load_contract, save_contract

    for dag_id in ("sales_daily_etl", "customers_sync"):
        path = repo / "healer" / "contracts" / f"{dag_id}.yaml"
        save_contract(approve_contract(load_contract(path), APPROVER), path)


def prepare(directory: Path) -> dict[str, Any]:
    """The two repositories, configurations and the home of DIR."""
    store = f"sqlite:///{(directory / 'healer.db').as_posix()}"
    airflow_repo = directory / "airflow" / "repo"
    (airflow_repo / "dags").mkdir(parents=True)
    _contracts_airflow(airflow_repo)
    sfn_repo = directory / "stepfunctions" / "repo"
    sfn_repo.mkdir(parents=True)
    for name in ("template.yaml", "statemachine", "healer"):
        source = SFN_REPO / name
        if source.is_dir():
            shutil.copytree(source, sfn_repo / name)
        else:
            shutil.copy2(source, sfn_repo / name)
    _contracts_sfn(sfn_repo)
    pipelines = "\n".join(
        f"      {dag_id}:\n"
        f"        state_machine: arn:aws:states:{SFN_REGION}:{SFN_ACCOUNT}:stateMachine:{machine}"
        for dag_id, machine in SFN_MACHINES.items()
    )
    airflow_config = directory / "airflow" / "healer.yaml"
    airflow_config.write_text(
        AIRFLOW_CONFIG.format(repo=airflow_repo.as_posix(), store=store), encoding="utf-8"
    )
    sfn_config = directory / "stepfunctions" / "healer.yaml"
    sfn_config.write_text(
        SFN_CONFIG.format(
            repo=sfn_repo.as_posix(), store=store, instance=SFN, region=SFN_REGION, pipelines=pipelines
        ),
        encoding="utf-8",
    )
    home = directory / "home"
    (home / "prs").mkdir(parents=True)
    return {"airflow": airflow_config, "home": home, "stepfunctions": sfn_config, "store": store}


# --------------------------------------------------------------------------- incidents


class Seeder:
    """Writes incidents with their timeline, alerts and files, at times relative to ``now``."""

    def __init__(self, session: Any, home: Path, now: datetime) -> None:
        self.session = session
        self.home = home
        self.now = now
        self.prs = 0

    def at(self, minutes_ago: float) -> datetime:
        return self.now - timedelta(minutes=minutes_ago)

    def incident(
        self,
        *,
        dag_id: str,
        task_id: str,
        run_id: str,
        source: str,
        error_summary: str,
        minutes_ago: float,
        orchestrator: str = AIRFLOW,
        try_number: int = 1,
        native_ref: Mapping[str, Any] | None = None,
        error_name: str | None = None,
        **fields: Any,
    ) -> Any:
        from dag_healer.store import repo as store_repo

        incident, _ = store_repo.create_incident(
            self.session,
            dag_id=dag_id,
            run_id=run_id,
            task_id=task_id,
            try_number=try_number,
            source=source,
            error_summary=error_summary,
            now=self.at(minutes_ago),
            orchestrator=orchestrator,
            native_ref=native_ref,
            error_name=error_name,
        )
        for name, value in fields.items():
            setattr(incident, name, value)
        incident.updated_at = self.at(minutes_ago)
        self.session.flush()
        created = incident.events[-1] if incident.events else None
        if created is not None:
            created.at = self.at(minutes_ago)
        return incident

    def event(
        self,
        incident: Any,
        kind: str,
        message: str,
        minutes_ago: float,
        data: Any = None,
        actor: str = "dag-healer",
    ) -> None:
        from dag_healer.store import repo as store_repo

        event = store_repo.add_event(self.session, incident, kind, message, data=data, actor=actor)
        event.at = self.at(minutes_ago)
        incident.updated_at = max(incident.updated_at, event.at)

    def status(self, incident: Any, old: str, new: str, message: str, minutes_ago: float) -> None:
        self.event(incident, "status", message, minutes_ago, {"from": old, "to": new})
        incident.status = new

    def alert(self, incident: Any, subject: str, minutes_ago: float, recipients: Iterable[str] = ()) -> None:
        from dag_healer.store import repo as store_repo

        for channel, to in (("log", []), ("email", list(recipients))):
            if channel == "email" and not to:
                continue
            alert = store_repo.record_alert(
                self.session,
                incident_id=incident.id,
                channel=channel,
                recipients=to,
                subject=subject,
                status="sent",
            )
            alert.sent_at = self.at(minutes_ago)

    def pr_md(self, text: str) -> tuple[str, str]:
        """Write PR.md and fix.patch under ``home/prs/<n>/``; their paths."""
        self.prs += 1
        folder = self.home / "prs" / str(self.prs)
        folder.mkdir(parents=True)
        (folder / "PR.md").write_text(text, encoding="utf-8")
        (folder / "fix.patch").write_text("", encoding="utf-8")
        return (folder / "PR.md").as_posix(), (folder / "fix.patch").as_posix()


def _golden(case: str) -> dict[str, Any]:
    return json.loads((GOLDEN / case / "incident_detail.json").read_text(encoding="utf-8"))


def _replace(value: Any, swap: Callable[[str], str]) -> Any:
    """``swap`` applied to every string of a JSON-like value."""
    if isinstance(value, str):
        return swap(value)
    if isinstance(value, list):
        return [_replace(item, swap) for item in value]
    if isinstance(value, dict):
        return {key: _replace(item, swap) for key, item in value.items()}
    return value


def seed_golden(seeder: Seeder, case: str, minutes_ago: float, run_id: str) -> Any:
    """One incident of the golden snapshots, its timeline spread over the minute after it failed."""
    detail = _golden(case)
    stamp = f"{seeder.now.date().isoformat()}T05:42:00Z"

    def swap(text: str) -> str:
        return text.replace("<timestamp>", stamp).replace(detail["run_id"], run_id)

    incident = seeder.incident(
        dag_id=detail["dag_id"],
        task_id=detail["task_id"],
        run_id=run_id,
        source=detail["source"],
        error_summary=swap(detail["error_summary"]),
        minutes_ago=minutes_ago,
        category=detail["category"],
        confidence=detail["confidence"],
        diagnosis=detail["diagnosis"],
        violated_rule=detail["violated_rule"],
        sample=_replace(detail["sample"], swap),
        evidence=detail["evidence"],
        test_path=detail["test_path"],
        test_code=detail["test_code"],
        diff=detail["diff"],
        pytest_output=(detail["pytest_output"] or "").replace(REDUCED, "") or None,
        attempts=detail["attempts"],
        tokens_used=detail["tokens_used"],
    )
    branch = f"healer/{incident.dag_id}-{incident.task_id}-{incident.id[:8]}"
    files: dict[str, str] = {}
    if detail["has_pr_md"]:
        text = (GOLDEN / case / "PR.md").read_text(encoding="utf-8").replace(REDUCED, "")
        text = text.replace(f"healer/{incident.dag_id}-{incident.task_id}-00000000", branch)
        text = text.replace(GOLDEN_BRANCH, branch)
        text = text.replace(detail["id"], incident.id).replace(detail["run_id"], run_id)
        files["pr_md_path"], files["patch_path"] = seeder.pr_md(text)
        incident.pr_md_path = files["pr_md_path"]
    events = [event for event in detail["events"] if event["kind"] != "created"]
    for index, event in enumerate(events, start=1):
        data = _replace(event["data"], swap)
        if isinstance(data, dict):
            data.update({key: value for key, value in files.items() if data.get(key)})
            if data.get("branch"):
                data["branch"] = branch
            if isinstance(data.get("notes"), list):
                data["notes"] = [note.replace(GOLDEN_BRANCH, branch) for note in data["notes"]]
        message = swap(event["message"]).replace(GOLDEN_BRANCH, branch)
        seeder.event(incident, event["kind"], message, minutes_ago - index * 0.2, data, event["actor"])
        if event["kind"] == "status":
            incident.status = event["data"]["to"]
    for alert in detail["alerts"]:
        seeder.alert(incident, alert["subject"], minutes_ago - len(events) * 0.2, ["data-team@example.com"])
    return incident


def _classified(
    seeder: Seeder,
    incident: Any,
    minutes_ago: float,
    *,
    category: str,
    confidence: float,
    rule: str,
    reason: str,
    contract_status: str,
    decided_by: str = "policy",
    llm: tuple[str, float] | None = None,
    error_kind: str = "code",
    violations: list[dict[str, Any]] | None = None,
) -> None:
    seeder.status(incident, "new", "triaging", "Claimed by the worker for triage", minutes_ago)
    seeder.event(incident, "triage", "Automatic triage started", minutes_ago - 0.05)
    data = {
        "category": category,
        "confidence": confidence,
        "contract_status": contract_status,
        "decided_by": decided_by,
        "error_kind": error_kind,
        "llm_category": llm[0] if llm else None,
        "llm_confidence": llm[1] if llm else None,
        "llm_error": None,
        "needs_llm_confirmation": decided_by != "policy",
        "reason": reason,
        "rule": rule,
        "tokens_used": 0,
        "violations": violations or [],
    }
    seeder.event(
        incident, "classification", f"Classified as {category} ({rule}): {reason}", minutes_ago - 0.4, data
    )
    incident.category = category
    incident.confidence = confidence
    seeder.status(
        incident,
        "triaging",
        "classified",
        f"Classified as {category} (confidence {confidence:.2f}, rule {rule})",
        minutes_ago - 0.45,
    )


def _context(
    seeder: Seeder,
    incident: Any,
    minutes_ago: float,
    *,
    exception: tuple[str, str],
    frames: list[dict[str, Any]],
    contract_status: str,
    dataset: str,
) -> None:
    where = f" in {frames[-1]['file']}::{frames[-1]['function']}" if frames else ""
    seeder.event(
        incident,
        "context",
        f"Context gathered: {exception[0]}{where}; contract {contract_status}, 0 violation(s)",
        minutes_ago,
        {
            "contract_status": contract_status,
            "datasets_checked": [dataset],
            "draft_violation_count": 0,
            "exception_message": exception[1],
            "exception_type": exception[0],
            "notes": [],
            "repo_frames": frames,
            "sample_dataset": dataset,
            "violation_count": 0,
        },
    )


def seed_airflow(seeder: Seeder) -> None:
    today = seeder.now.date().isoformat()
    yesterday = (seeder.now - timedelta(days=1)).date().isoformat()

    seed_golden(seeder, "inventory", 14, f"scheduled__{today}T06:00:00+00:00")
    seed_golden(seeder, "customers", 52, f"scheduled__{today}T05:30:00+00:00")
    seed_golden(seeder, "sales", 95, f"scheduled__{today}T04:00:00+00:00")

    # Just detected: the CRM export did not arrive.
    extract = seeder.incident(
        dag_id="customers_sync",
        task_id="extract",
        run_id=f"manual__{today}T09:12:00+00:00",
        source="listener",
        error_summary=(
            "FileNotFoundError: input file not found: data/incoming/customers.csv "
            "(did the upstream drop run?)"
        ),
        minutes_ago=1,
    )
    seeder.status(extract, "new", "triaging", "Claimed by the worker for triage", 0.5)

    # Fixing: a Timestamp the JSON writer cannot serialise.
    publish = seeder.incident(
        dag_id="inventory_report",
        task_id="publish_report",
        run_id=f"scheduled__{today}T08:00:00+00:00",
        source="rest",
        error_summary=(
            "TypeError: Object of type Timestamp is not JSON serializable\n\n"
            "Traceback (most recent call last):\n"
            '  File "/opt/airflow/repo/dags/../src/etl/inventory.py", line 40, in publishReport\n'
            "    json.dump(rows, handle)\n"
            "TypeError: Object of type Timestamp is not JSON serializable"
        ),
        minutes_ago=6,
    )
    frames = [{"file": "src/etl/inventory.py", "function": "publishReport", "line": 40}]
    _context(
        seeder,
        publish,
        5.9,
        exception=("TypeError", "Object of type Timestamp is not JSON serializable"),
        frames=frames,
        contract_status="draft",
        dataset="inventory",
    )
    _classified(
        seeder,
        publish,
        5.8,
        category="code",
        confidence=0.86,
        rule="llm",
        decided_by="anthropic",
        llm=("code", 0.86),
        contract_status="draft",
        reason=LLM_DECIDES.format(contract="draft"),
    )
    publish.diagnosis = (
        "publishReport writes the report rows with json.dump, and the rows hold pandas Timestamp values from "
        "updated_at, which the json module cannot serialise. The input is fine; the code must convert the "
        "timestamps to text before writing."
    )
    seeder.status(
        publish, "classified", "repairing", "Reproducing the failure and fixing the code in a clone", 5
    )

    # The fix failed: the agent could not reproduce the failure with a test.
    empty = seeder.incident(
        dag_id="inventory_report",
        task_id="get_inventory",
        run_id=f"scheduled__{today}T03:00:00+00:00",
        source="rest",
        error_summary=(
            "Exception: no inventory data today\n\n"
            "Traceback (most recent call last):\n"
            '  File "/opt/airflow/repo/dags/../src/etl/inventory.py", line 23, in getInventory\n'
            '    raise Exception("no inventory data today")\n'
            "Exception: no inventory data today"
        ),
        minutes_ago=190,
        attempts=3,
        tokens_used=48210,
    )
    _context(
        seeder,
        empty,
        189.9,
        exception=("Exception", "no inventory data today"),
        frames=[{"file": "src/etl/inventory.py", "function": "getInventory", "line": 23}],
        contract_status="draft",
        dataset="inventory",
    )
    _classified(
        seeder,
        empty,
        189.8,
        category="code",
        confidence=0.78,
        rule="llm",
        decided_by="anthropic",
        llm=("code", 0.78),
        contract_status="draft",
        reason=LLM_DECIDES.format(contract="draft"),
    )
    empty.diagnosis = (
        "getInventory raises a bare Exception when the inventory file has no rows. The model read it as a "
        "code problem: the task could skip an empty day instead of failing."
    )
    seeder.status(
        empty, "classified", "repairing", "Reproducing the failure and fixing the code in a clone", 189
    )
    reason = (
        "The repair agent could not produce a failing reproducing test after 3 attempts: the new test "
        "passes on the base branch, so it does not reproduce the failure."
    )
    seeder.event(
        empty,
        "repair",
        f"Repair failed: {reason}",
        181,
        {"attempts": 3, "error": reason, "notes": [], "status": "repair_failed", "tokens_used": 48210},
    )
    seeder.status(empty, "repairing", "repair_failed", f"Repair failed: {reason}", 181)
    seeder.alert(
        empty, "[dag-healer] Repair failed for inventory_report.get_inventory", 181, ["data-team@example.com"]
    )

    # A worker error: the Airflow API was down while the worker read the log.
    load = seeder.incident(
        dag_id="customers_sync",
        task_id="load",
        run_id=f"scheduled__{today}T02:30:00+00:00",
        source="rest",
        error_summary="AirflowException: Task received SIGTERM signal",
        minutes_ago=130,
    )
    seeder.status(load, "new", "triaging", "Claimed by the worker for triage", 129.9)
    failure = (
        "Processing failed while gathering the context: the Airflow API answered 503 Service Unavailable for "
        "the task's log"
    )
    seeder.event(
        load, "error", failure, 129.5, {"error": "503 Service Unavailable", "stage": "gathering the context"}
    )
    seeder.status(load, "triaging", "error", failure, 129.5)

    # Ignored: a manual run with the wrong report currency.
    eur = seeder.incident(
        dag_id="sales_daily_etl",
        task_id="transform_sales",
        run_id=f"manual__{yesterday}T16:20:00+00:00",
        source="listener",
        error_summary=(
            "NotImplementedError: only USD reports for now\n\n"
            "Traceback (most recent call last):\n"
            '  File "/opt/airflow/repo/dags/../src/etl/sales.py", line 30, in transform_sales\n'
            '    raise NotImplementedError("only USD reports for now")\n'
            "NotImplementedError: only USD reports for now"
        ),
        minutes_ago=60 * 18,
    )
    _context(
        seeder,
        eur,
        60 * 18 - 0.05,
        exception=("NotImplementedError", "only USD reports for now"),
        frames=[{"file": "src/etl/sales.py", "function": "transform_sales", "line": 30}],
        contract_status="approved",
        dataset="sales",
    )
    _classified(
        seeder,
        eur,
        60 * 18 - 0.1,
        category="ambiguous",
        confidence=0.5,
        rule="llm",
        decided_by="anthropic",
        llm=("ambiguous", 0.55),
        contract_status="approved",
        reason=LLM_DECIDES.format(contract="approved"),
    )
    seeder.status(eur, "classified", "waiting_human", "Waiting for a human decision", 60 * 18 - 0.5)
    note = "Manual run with report_currency=EUR by mistake; the daily run uses USD."
    seeder.event(
        eur,
        "command",
        f"admin ignored the incident. Note: {note}",
        60 * 17,
        {"kind": "resolve", "status": "applied"},
        "admin",
    )
    eur.resolution = "ignore"
    eur.resolved_by = "admin"
    seeder.status(eur, "waiting_human", "ignored", f"Ignored by admin. Note: {note}", 60 * 17)


def _sfn_native(dag_id: str, execution: str, function: str) -> dict[str, str]:
    machine = SFN_MACHINES[dag_id]
    return {
        "execution_arn": f"arn:aws:states:{SFN_REGION}:{SFN_ACCOUNT}:execution:{machine}:{execution}",
        "function_name": function,
    }


def seed_stepfunctions(seeder: Seeder) -> None:
    # A data problem: the sales file lost its amount column (rule a, approved contract).
    sales = seeder.incident(
        orchestrator=SFN,
        dag_id="sales_daily_etl",
        task_id="TransformSales",
        run_id="sales-2026-day2-0a41",
        source="eventbridge",
        error_name="KeyError",
        error_summary="KeyError: 'amount'",
        minutes_ago=25,
        native_ref=_sfn_native("sales_daily_etl", "sales-2026-day2-0a41", "SalesFunction"),
        sample={
            "columns": ["order_id", "customer_email", "amt", "currency", "order_date"],
            "dataset": "sales",
            "masked": True,
            "rows": [
                {"order_id": "ORD-50031", "customer_email": "h:038151c9d1", "amt": 58.0, "currency": "EUR"},
                {"order_id": "ORD-50032", "customer_email": "h:0f62d3a9dc", "amt": None, "currency": "GBP"},
                {"order_id": "ORD-50033", "customer_email": "h:e0efe16d47", "amt": 1249.0, "currency": "USD"},
            ],
        },
    )
    missing = {
        "actual": ["amt", "currency", "customer_email", "order_date", "order_id"],
        "column": "amount",
        "dataset": "sales",
        "expected": "amount",
        "message": "Column 'amount' is missing from dataset 'sales'.",
        "row_indexes": [],
        "rule": "missing_column",
    }
    _classified(
        seeder,
        sales,
        24.9,
        category="data",
        confidence=0.95,
        rule="a_contract_violated",
        contract_status="approved",
        error_kind="data_shape",
        reason="The approved contract is violated (missing_column: sales.amount) and the state failed with a "
        "data-shaped error (KeyError).",
        violations=[{**missing, "contract_status": "approved"}],
    )
    sales.violated_rule = {**missing, "contract_status": "approved"}
    sales.diagnosis = (
        "State sales_daily_etl.TransformSales failed with KeyError: 'amount' because its input broke the "
        "approved data contract (missing_column: sales.amount). The code is behaving as designed: the "
        "upstream delivery must be fixed, then the execution can be redriven."
    )
    seeder.status(sales, "classified", "reporting", "Reporting the data issue", 24.5)
    seeder.alert(sales, "[dag-healer] DATA issue in sales_daily_etl.TransformSales", 24.4)
    seeder.status(sales, "reporting", "reported", "Data issue reported (missing_column)", 24.4)

    # The model was not sure: a human decides.
    customers = seeder.incident(
        orchestrator=SFN,
        dag_id="customers_sync",
        task_id="TransformCustomers",
        run_id="customers-2026-day2-77c2",
        source="sqs",
        error_name="ValueError",
        error_summary="ValueError: unconverted data remains: T08:30:00Z",
        minutes_ago=9,
        native_ref=_sfn_native("customers_sync", "customers-2026-day2-77c2", "CustomersFunction"),
    )
    _context(
        seeder,
        customers,
        8.9,
        exception=("ValueError", "unconverted data remains: T08:30:00Z"),
        frames=[{"file": "src/etl/customers.py", "function": "parse_signup_date", "line": 19}],
        contract_status="approved",
        dataset="customers",
    )
    reason = (
        "The LLM classified the failure as code with confidence 0.62, below the 0.75 threshold; a human "
        "must decide."
    )
    _classified(
        seeder,
        customers,
        8.8,
        category="ambiguous",
        confidence=0.62,
        rule="d_low_confidence",
        decided_by="anthropic",
        llm=("code", 0.62),
        contract_status="approved",
        reason=reason,
    )
    customers.diagnosis = (
        "parse_signup_date only accepts YYYY-MM-DD and today's export has ISO-8601 timestamps. The contract "
        "keeps signup_date as text, so it does not say which format is right."
    )
    seeder.status(customers, "classified", "waiting_human", f"Waiting for a human decision: {reason}", 8.5)
    seeder.alert(customers, "[dag-healer] Human decision needed: customers_sync.TransformCustomers", 8.5)

    # A timeout: an infrastructure problem, never fixed in code.
    inventory = seeder.incident(
        orchestrator=SFN,
        dag_id="inventory_report",
        task_id="BuildReport",
        run_id="inventory-2026-day2-3b90",
        source="eventbridge",
        error_name="States.Timeout",
        error_summary="States.Timeout: Task timed out after 300.00 seconds",
        minutes_ago=41,
        native_ref=_sfn_native("inventory_report", "inventory-2026-day2-3b90", "InventoryFunction"),
    )
    reason = (
        "The failure looks like an infrastructure problem (States.Timeout); neither the data nor the code is "
        "implicated: retry the state or ask an operator."
    )
    _classified(
        seeder,
        inventory,
        40.9,
        category="ambiguous",
        confidence=0.5,
        rule="infra",
        contract_status="draft",
        error_kind="infra",
        reason=reason,
    )
    inventory.diagnosis = (
        "The BuildReport Lambda ran past its 300-second timeout; nothing points at the data or the code."
    )
    seeder.status(inventory, "classified", "waiting_human", f"Waiting for a human decision: {reason}", 40.5)


def seed(directory: Path) -> None:
    from dag_healer.store import db

    paths = prepare(directory)
    url = paths["store"]
    db.ensure_initialized(db.get_engine(url))
    with db.session_scope(url) as session:
        seeder = Seeder(session, paths["home"], datetime.now(timezone.utc))
        seed_airflow(seeder)
        seed_stepfunctions(seeder)
    db.dispose_engines()


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    parser.add_argument(
        "--dir", type=Path, default=DEFAULT_DIR, help=f"Where to write it (default {DEFAULT_DIR})."
    )
    parser.add_argument("--keep", action="store_true", help="Refuse to replace an existing DIR.")
    args = parser.parse_args(argv)
    directory: Path = args.dir.expanduser().resolve()
    if directory.exists():
        if args.keep:
            print(f"{directory} exists; remove it or drop --keep.", file=sys.stderr)
            return 1
        shutil.rmtree(directory)
    seed(directory)
    home = (directory / "home").as_posix()
    print(f"Seeded {directory} (store {directory / 'healer.db'}).\n")
    print("Airflow instance (http://127.0.0.1:8090/plugin/healer):")
    print(f"  HEALER_HOME={home} dag-healer serve -c {(directory / 'airflow' / 'healer.yaml').as_posix()}\n")
    print("Step Functions instance (http://127.0.0.1:8091/plugin/healer):")
    print(
        f"  HEALER_HOME={home} dag-healer serve -c {(directory / 'stepfunctions' / 'healer.yaml').as_posix()}"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
