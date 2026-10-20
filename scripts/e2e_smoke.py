#!/usr/bin/env python3
"""End-to-end smoke test of dag-healer on the local Docker stack (exit 0 = every check passed).

``python scripts/tasks.py e2e`` (or ``make e2e``, or ``python scripts/e2e_smoke.py``):

1. runs the demo (``scripts/demo.py`` with a reset; ``--no-demo`` checks the current state of
   the last demo run instead, ``--listener-only`` runs it with ``monitor.modes: [listener]``);
2. checks the outcome through public interfaces only (healer API, Airflow REST API, Mailpit
   API, git): exactly the three incidents with categories data / code / ambiguous and statuses
   reported / repaired / waiting_human; the CODE fix touches only ``src/etl/customers.py`` plus
   the new ``tests/`` file and its PR.md says the tests folder was created; the ``healer/*``
   branch exists in ``.demo/repo`` and the working tree was not touched; the alert e-mails
   reached Mailpit with masked samples (no raw e-mail address, name or phone number of the
   day-2 files anywhere);
3. checks the security of the API: unauthenticated 401, viewer GET 200 / POST 403, a POST
   without ``X-Requested-With`` 403, a cross-origin POST 403, ingest without token 401;
4. resumes the AMBIGUOUS incident as ``code`` (admin, through the API) and follows it until
   the worker settles it (``resumed -> ... -> repaired`` with the fake scripts, or
   ``repair_failed`` when no script matches), printing its timeline;
5. fails ``inventory_report`` once more and resolves that new AMBIGUOUS incident as ``data``
   (``--skip-data-path`` skips it), which must end ``reported``;
6. approves the ``inventory_report`` draft contract through the API (admin may, viewer may
   not) and waits until the worker has written the approval.

Standard library only (it reuses scripts/demo.py and scripts/tasks.py).
"""

from __future__ import annotations

import argparse
import json
import re
import sys
import time
from collections.abc import Callable, Iterable, Mapping, Sequence
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any

SCRIPTS_DIR = Path(__file__).resolve().parent
if str(SCRIPTS_DIR) not in sys.path:
    sys.path.insert(0, str(SCRIPTS_DIR))

import demo  # noqa: E402
import tasks  # noqa: E402

RESUME_TIMEOUT = 600
EMAIL_RE = re.compile(r"[A-Za-z0-9._%+-]+@[A-Za-z0-9-]+(?:\.[A-Za-z0-9-]+)+")
PII_COLUMNS = ("email", "customer_email", "full_name", "phone")
SUBJECTS = {
    "sales_daily_etl": "[dag-healer] DATA issue in sales_daily_etl.transform_sales",
    "customers_sync": "[dag-healer] Fix proposed for customers_sync.transform",
    "inventory_report": "[dag-healer] Human decision needed: inventory_report.build_report",
}
CODE_FILE = "src/etl/customers.py"


# --------------------------------------------------------------------------- report


@dataclass
class Report:
    """PASS/FAIL lines collected while the checks run."""

    results: list[tuple[str, bool, str]] = field(default_factory=list)

    def check(self, name: str, ok: bool, detail: Any = "") -> bool:
        """Record and print one check; returns ``ok``."""
        text = detail if isinstance(detail, str) else json.dumps(detail, default=str)
        self.results.append((name, bool(ok), text))
        mark = "PASS" if ok else "FAIL"
        print(f"[e2e] {mark} {name}" + (f": {text}" if text else ""), flush=True)
        return bool(ok)

    @property
    def failed(self) -> list[str]:
        """Names of the failed checks."""
        return [name for name, ok, _ in self.results if not ok]


# --------------------------------------------------------------------------- pure helpers


def raw_values(day2: Path = demo.DAY2_DIR) -> set[str]:
    """PII values of the day-2 files that must never show up unmasked (e-mails, names, phones)."""
    import csv

    values: set[str] = set()
    for path in sorted(day2.glob("*.csv")):
        with path.open(encoding="utf-8", newline="") as handle:
            for row in csv.DictReader(handle):
                for column in PII_COLUMNS:
                    value = (row.get(column) or "").strip()
                    if len(value) >= 6:
                        values.add(value)
                for value in row.values():
                    values.update(EMAIL_RE.findall(value or ""))
    return values


def leaked(text: str, values: Iterable[str]) -> list[str]:
    """The raw values that appear in ``text``."""
    return sorted(value for value in values if value and value in text)


def diff_files(diff: str | None) -> list[str]:
    """Files touched by a unified git diff (``b/`` side of each ``diff --git`` header)."""
    files = []
    for line in (diff or "").splitlines():
        match = re.match(r"^diff --git a/(\S+) b/(\S+)$", line)
        if match:
            files.append(match.group(2))
    return files


def status_path(events: Sequence[Mapping[str, Any]]) -> list[str]:
    """Statuses an incident went through, from its ``status`` timeline events."""
    path: list[str] = []
    for event in events:
        if event.get("kind") != "status":
            continue
        data = event.get("data") if isinstance(event.get("data"), Mapping) else {}
        target = data.get("to")
        if target and (not path or path[-1] != target):
            path.append(str(target))
    return path


def format_timeline(events: Sequence[Mapping[str, Any]]) -> list[str]:
    """One printable line per timeline event."""
    lines = []
    for event in events:
        at = str(event.get("at") or "")[11:19]
        message = " ".join(str(event.get("message") or "").split())
        lines.append(f"{at}  {event.get('kind', '?'):<15} {event.get('actor', ''):<12} {message[:150]}")
    return lines


# --------------------------------------------------------------------------- API helpers


def incident_detail(api: demo.AirflowSession, incident: Mapping[str, Any]) -> dict[str, Any]:
    """The IncidentDetail of an incident."""
    return api.ok(
        "GET",
        f"{demo.HEALER_API}/dags/{demo.quote(incident['dag_id'])}/incidents/{demo.quote(incident['id'])}",
    )


def wait_until(
    probe: Callable[[], dict[str, Any]],
    done: Callable[[dict[str, Any]], bool],
    *,
    timeout: float,
    label: str,
    interval: float = 2.0,
) -> tuple[dict[str, Any], list[str]]:
    """Poll ``probe`` until ``done``; returns the last value and the statuses seen on the way."""
    deadline = time.monotonic() + timeout
    seen: list[str] = []
    while True:
        value = probe()
        status = str(value.get("status"))
        if not seen or seen[-1] != status:
            seen.append(status)
            print(f"[e2e] {label}: {status}", flush=True)
        if done(value):
            return value, seen
        if time.monotonic() >= deadline:
            raise demo.DemoError(f"{label}: still {status} after {int(timeout)} s (seen {seen})")
        time.sleep(interval)


def settled(detail: Mapping[str, Any], *, previous: str) -> bool:
    """True once the worker applied the command and the incident rests in a new status."""
    return (
        not detail.get("pending_commands")
        and detail.get("status") in demo.RESTING_STATUSES
        and detail.get("status") != previous
    )


def mailpit_messages(since: datetime) -> list[dict[str, Any]]:
    """Mailpit messages received at or after ``since`` (newest first)."""
    reply = demo.http("GET", f"{demo.MAILPIT_URL}/api/v1/messages?limit=500")
    if reply.status != 200:
        raise demo.DemoError(f"Mailpit API answered HTTP {reply.status}")
    messages = (reply.json() or {}).get("messages") or []
    floor = since - timedelta(seconds=5)
    return [m for m in messages if (demo.parse_time(m.get("Created")) or floor) >= floor]


# --------------------------------------------------------------------------- checks


def check_incidents(
    report: Report, result: demo.DemoResult, *, strict_total: bool, listener_only: bool
) -> None:
    """The three incidents, their classification and the CODE / DATA / AMBIGUOUS evidence."""
    admin = demo.AirflowSession("admin", "admin")
    body = admin.ok("GET", f"{demo.HEALER_API}/incidents?limit=200")
    incidents = body.get("incidents") or []
    if strict_total:
        report.check("exactly 3 incidents in the store", body.get("total") == 3, f"total={body.get('total')}")
    summary = {
        dag: f"{i.get('category')}/{i.get('status')}/{i.get('source')}" for dag, i in result.incidents.items()
    }
    report.check(
        "classified data / code / ambiguous as expected", not result.problems, result.problems or summary
    )
    if listener_only:
        sources = {i.get("source") for i in result.incidents.values()}
        report.check("incidents created by the listener only", sources == {"listener"}, sorted(sources))
    values = raw_values()
    everything = json.dumps({"list": incidents, "details": result.details}, default=str)
    report.check(
        "no raw PII in the API responses", not leaked(everything, values), leaked(everything, values)[:5]
    )

    code = result.details.get("customers_sync") or {}
    files = diff_files(code.get("diff"))
    test_path = code.get("test_path") or ""
    report.check(
        "CODE diff touches only src/etl/customers.py and the new tests/ file",
        bool(files) and set(files) == {CODE_FILE, test_path} and test_path.startswith("tests/"),
        {"files": files, "test_path": test_path},
    )
    output = code.get("pytest_output") or ""
    report.check(
        "CODE pytest output: red before the fix, green after",
        "failed" in output and "passed" in output and "pytest after the fix" in output,
        output[-300:] if "failed" not in output else "",
    )
    pr_md = admin.healer("GET", f"/dags/customers_sync/incidents/{demo.quote(code.get('id', ''))}/pr.md")
    report.check(
        "PR.md says the tests folder was created",
        pr_md.status == 200 and "had no test suite" in pr_md.text and "Generated by dag-healer" in pr_md.text,
        f"HTTP {pr_md.status}",
    )
    report.check("PR.md copied to the host", "customers_sync" in result.pr_files, str(result.pr_files))

    data = result.details.get("sales_daily_etl") or {}
    rule = data.get("violated_rule") or {}
    sample = data.get("sample") or {}
    report.check(
        "DATA incident names the violated rule (missing_column amount)",
        rule.get("rule") == "missing_column" and "amount" in json.dumps(rule),
        rule,
    )
    report.check(
        "DATA incident has a masked sample",
        sample.get("masked") is True and bool(sample.get("rows")),
        {"dataset": sample.get("dataset"), "rows": len(sample.get("rows") or [])},
    )
    ambiguous = result.details.get("inventory_report") or {}
    report.check(
        "AMBIGUOUS incident offers resolve_data / resolve_code / ignore to the admin",
        {"resolve_data", "resolve_code", "ignore"} <= set(ambiguous.get("allowed_actions") or []),
        ambiguous.get("allowed_actions"),
    )


def check_repository(report: Report, result: demo.DemoResult) -> None:
    """The healer/* branch is in .demo/repo and the working tree only has the day-2 files."""
    branches = [b for b in result.branches if b.startswith("healer/customers_sync-transform-")]
    report.check("healer/customers_sync-transform-* branch in .demo/repo", bool(branches), result.branches)
    if branches:
        changed = demo.git_out(demo.DEMO_REPO, "diff", "--name-only", f"main...{branches[0]}").splitlines()
        report.check(
            "the branch changes only the fix and the new test",
            set(changed) == {CODE_FILE, (result.details.get("customers_sync") or {}).get("test_path")},
            changed,
        )
    status = demo.git_out(demo.DEMO_REPO, "status", "--porcelain", "--untracked-files=all").splitlines()
    paths = [line.strip().split(maxsplit=1)[-1] for line in status if line.strip()]
    touched = sorted(path for path in paths if not path.startswith("data/incoming/"))
    report.check("the worker never wrote into the repository working tree", not touched, touched)


def check_mail(report: Report, result: demo.DemoResult) -> None:
    """The alert e-mails reached Mailpit, addressed to the data team, with masked samples."""
    deadline = time.monotonic() + 60
    messages = mailpit_messages(result.started_at)
    while time.monotonic() < deadline and not set(SUBJECTS.values()) <= {m.get("Subject") for m in messages}:
        time.sleep(3)
        messages = mailpit_messages(result.started_at)
    subjects = {m.get("Subject") for m in messages}
    missing = sorted(set(SUBJECTS.values()) - subjects)
    report.check("Mailpit received the 3 alert e-mails", not missing, {"missing": missing})
    values = raw_values()
    for dag_id, subject in SUBJECTS.items():
        message = next((m for m in messages if m.get("Subject") == subject), None)
        if message is None:
            continue
        reply = demo.http("GET", f"{demo.MAILPIT_URL}/api/v1/message/{message['ID']}")
        body = reply.json() or {}
        text = f"{body.get('Text', '')}\n{body.get('HTML', '')}"
        recipients = [item.get("Address") for item in body.get("To") or []]
        report.check(
            f"{dag_id} alert: to data-team@example.com, link to the Healer tab, no raw PII",
            demo.ALERT_TO in recipients and "plugin/healer-ti" in text and not leaked(text, values),
            {"to": recipients, "leaked": leaked(text, values)[:3]},
        )
        if dag_id == "sales_daily_etl":
            report.check(
                "DATA alert shows the violated rule and the masked sample",
                "missing_column" in text and "h:" in text,
                "" if "h:" in text else "no hashed value found in the e-mail",
            )


def check_security(report: Report, result: demo.DemoResult) -> None:
    """401 / 403 answers of the healer API."""
    ambiguous = result.incidents["inventory_report"]
    path = f"/dags/inventory_report/incidents/{demo.quote(ambiguous['id'])}"
    anonymous = demo.http("GET", f"{demo.AIRFLOW_URL}{demo.HEALER_API}/incidents")
    report.check("unauthenticated GET /incidents -> 401", anonymous.status == 401, f"HTTP {anonymous.status}")
    viewer = demo.AirflowSession("viewer", "viewer")
    seen = viewer.healer("GET", path)
    viewer_detail = seen.json() if seen.status == 200 else {}
    report.check(
        "viewer GET incident -> 200 without actions",
        seen.status == 200
        and not viewer_detail.get("allowed_actions")
        and viewer_detail.get("can_edit") is False,
        f"HTTP {seen.status}",
    )
    denied = viewer.healer("POST", f"{path}/resolve", json_body={"resolution": "code"})
    report.check("viewer POST resolve -> 403", denied.status == 403, f"HTTP {denied.status} {denied.detail}")
    admin = demo.AirflowSession("admin", "admin")
    no_header = admin.healer("POST", f"{path}/resolve", json_body={"resolution": "code"}, csrf=False)
    report.check(
        "POST without X-Requested-With -> 403",
        no_header.status == 403,
        f"HTTP {no_header.status} {no_header.detail}",
    )
    cross = admin.healer(
        "POST", f"{path}/resolve", json_body={"resolution": "code"}, headers={"Origin": "http://evil.example"}
    )
    report.check("cross-origin POST -> 403", cross.status == 403, f"HTTP {cross.status} {cross.detail}")
    ingest = demo.http(
        "POST",
        f"{demo.AIRFLOW_URL}{demo.HEALER_API}/events/task-failed",
        json_body={"dag_id": "sales_daily_etl", "task_id": "x", "run_id": "forged"},
    )
    report.check("ingest without X-Healer-Token -> 401", ingest.status == 401, f"HTTP {ingest.status}")
    after = admin.ok("GET", f"{demo.HEALER_API}{path}")
    report.check(
        "the rejected requests changed nothing", after.get("status") == "waiting_human", after.get("status")
    )


def resolve(api: demo.AirflowSession, incident: Mapping[str, Any], resolution: str, note: str) -> int:
    """POST resolve as ``api``'s user; returns the command id."""
    body = api.ok(
        "POST",
        f"{demo.HEALER_API}/dags/{demo.quote(incident['dag_id'])}/incidents/{demo.quote(incident['id'])}/resolve",
        json_body={"resolution": resolution, "note": note},
        expect=(202,),
    )
    return int(body["command_id"])


def check_resume_as_code(report: Report, result: demo.DemoResult) -> None:
    """Resolve the AMBIGUOUS incident as code and follow the worker until it settles."""
    admin = demo.AirflowSession("admin", "admin")
    incident = result.incidents["inventory_report"]
    command_id = resolve(admin, incident, "code", "e2e: the qty -> quantity rename is intended")
    report.check("admin POST resolve {resolution: code} -> 202", True, f"command {command_id}")
    final, seen = wait_until(
        lambda: incident_detail(admin, incident),
        lambda d: settled(d, previous="waiting_human"),
        timeout=RESUME_TIMEOUT,
        label="inventory_report after resolve(code)",
    )
    path = status_path(final.get("events") or [])
    print("[e2e] timeline of the inventory_report incident:")
    for line in format_timeline(final.get("events") or []):
        print(f"        {line}")
    kinds = {event.get("kind") for event in final.get("events") or []}
    report.check(
        "resume as code: resumed -> triaging -> classified -> repairing -> repaired | repair_failed",
        {"resumed", "repairing"} <= set(path) and path[-1] in ("repaired", "repair_failed"),
        {"status path": path, "polled": seen},
    )
    report.check(
        "the timeline records the queued and applied command",
        {"command_queued", "command"} <= kinds
        and final.get("resolution") == "code"
        and final.get("resolved_by"),
        {"kinds": sorted(k for k in kinds if k), "resolved_by": final.get("resolved_by")},
    )
    if final.get("status") == "repaired":
        report.check(
            "the resumed CODE repair produced a PR.md", bool(final.get("has_pr_md")), final.get("pr_md_url")
        )
    else:
        failure = next((e for e in reversed(final.get("events") or []) if e.get("kind") == "repair"), {})
        report.check("repair_failed explains why in the timeline", bool(failure), failure.get("message", ""))


def check_resolve_as_data(report: Report, result: demo.DemoResult) -> None:
    """Fail inventory_report again and resolve the new AMBIGUOUS incident as data."""
    admin = demo.AirflowSession("admin", "admin")
    run_id = demo.trigger_dag(admin, "inventory_report")
    print(f"[e2e] triggered inventory_report again: {run_id}")
    demo.wait_for_runs(admin, {"inventory_report": run_id})
    found = demo.wait_for_incidents(admin, {"inventory_report": run_id}, timeout=RESUME_TIMEOUT)
    incident = found["inventory_report"]
    report.check(
        "second inventory failure is a new AMBIGUOUS incident waiting for a human",
        (incident.get("category"), incident.get("status")) == ("ambiguous", "waiting_human"),
        f"{incident.get('category')}/{incident.get('status')}",
    )
    if incident.get("status") != "waiting_human":
        return
    resolve(admin, incident, "data", "e2e: the upstream export is wrong")
    final, _seen = wait_until(
        lambda: incident_detail(admin, incident),
        lambda d: settled(d, previous="waiting_human"),
        timeout=RESUME_TIMEOUT,
        label="inventory_report after resolve(data)",
    )
    path = status_path(final.get("events") or [])
    report.check(
        "resume as data: resumed -> ... -> reported as DATA",
        (final.get("status"), final.get("category"), final.get("resolution")) == ("reported", "data", "data")
        and "resumed" in path,
        {"status path": path, "status": final.get("status"), "category": final.get("category")},
    )


def check_contract_approval(report: Report) -> None:
    """Approve the inventory_report draft contract through the API (worker writes the YAML)."""
    admin = demo.AirflowSession("admin", "admin")
    viewer = demo.AirflowSession("viewer", "viewer")

    def contract(api: demo.AirflowSession) -> dict[str, Any]:
        return api.ok("GET", f"{demo.HEALER_API}/dags/inventory_report/contract")

    before_admin, before_viewer = contract(admin), contract(viewer)
    report.check(
        "the admin may approve the inventory_report draft, the viewer may not",
        before_admin.get("status") == "draft"
        and before_admin.get("can_approve") is True
        and before_viewer.get("can_approve") is False,
        {"status": before_admin.get("status"), "admin": before_admin.get("can_approve"),
         "viewer": before_viewer.get("can_approve")},
    )  # fmt: skip
    path = "/dags/inventory_report/contract/approve"
    denied = viewer.healer("POST", path, json_body={"note": "e2e"})
    report.check("viewer POST contract/approve -> 403", denied.status == 403, f"HTTP {denied.status}")
    accepted = admin.healer("POST", path, json_body={"note": "e2e: reviewed"})
    report.check("admin POST contract/approve -> 202", accepted.status == 202, f"HTTP {accepted.status}")
    if accepted.status != 202:
        return
    final, _seen = wait_until(
        lambda: contract(admin),
        lambda c: c.get("status") == "approved" and not c.get("pending_approval"),
        timeout=120,
        label="inventory_report contract",
    )
    report.check(
        "the worker approved the contract file (approved_by admin)",
        final.get("approved_by") == "admin",
        {"status": final.get("status"), "approved_by": final.get("approved_by")},
    )


# --------------------------------------------------------------------------- main


def current_state() -> demo.DemoResult:
    """Rebuild a DemoResult from the healer API (``--no-demo``): the newest incident per DAG."""
    admin = demo.AirflowSession("admin", "admin")
    incidents = admin.ok("GET", f"{demo.HEALER_API}/incidents?limit=200").get("incidents") or []
    newest: dict[str, dict] = {}
    for incident in incidents:
        dag_id = incident.get("dag_id")
        if dag_id in {case.dag_id for case in demo.CASES} and dag_id not in newest:
            newest[dag_id] = incident  # the list is newest first
    runs = {dag_id: str(incident["run_id"]) for dag_id, incident in newest.items()}
    found = demo.incidents_of_runs(incidents, runs)
    details = {dag_id: incident_detail(admin, incident) for dag_id, incident in found.items()}
    created = [demo.parse_time(i.get("created_at")) for i in found.values()]
    started = min((c for c in created if c is not None), default=demo.utcnow()) - timedelta(minutes=5)
    pr_files = {
        dag_id: demo.DEMO_PRS / incident["id"] / "PR.md"
        for dag_id, incident in found.items()
        if (demo.DEMO_PRS / incident["id"] / "PR.md").is_file()
    }
    return demo.DemoResult(
        started_at=started,
        runs=runs,
        incidents=found,
        details=details,
        pr_files=pr_files,
        branches=demo.healer_branches(),
        problems=demo.check_outcomes(found),
    )


def parse_args(argv: Sequence[str] | None = None) -> argparse.Namespace:
    """Command line of the smoke test."""
    parser = argparse.ArgumentParser(prog="python scripts/e2e_smoke.py", description=__doc__.split("\n")[0])
    parser.add_argument("--no-demo", action="store_true", help="check the state of the last demo run")
    parser.add_argument(
        "--listener-only", action="store_true", help="run the demo with monitor.modes [listener]"
    )
    parser.add_argument("--build", action="store_true", help="rebuild the image before the demo")
    parser.add_argument("--timeout", type=float, default=demo.DEFAULT_TIMEOUT, help="demo timeout in seconds")
    parser.add_argument(
        "--skip-resume",
        action="store_true",
        help="read-only checks: do not resolve the AMBIGUOUS incident nor approve its contract",
    )
    parser.add_argument("--skip-data-path", action="store_true", help="skip the second run resolved as data")
    return parser.parse_args(argv)


def main(argv: Sequence[str] | None = None) -> int:
    """Entry point; 0 when every check passed."""
    args = parse_args(argv)
    report = Report()
    try:
        if args.no_demo:
            result = current_state()
        else:
            result = demo.run_demo(
                demo.DemoOptions(
                    reset=True, build=args.build, listener_only=args.listener_only, timeout=args.timeout
                )
            )
            demo.print_summary(result)
        check_incidents(report, result, strict_total=not args.no_demo, listener_only=args.listener_only)
        check_repository(report, result)
        check_mail(report, result)
        if "inventory_report" in result.incidents:
            check_security(report, result)
            if not args.skip_resume:  # the steps below change the state: resolve, approve
                check_resume_as_code(report, result)
                if not args.skip_data_path:
                    check_resolve_as_data(report, result)
                check_contract_approval(report)
    except (demo.DemoError, tasks.TaskError) as exc:
        report.check("e2e run completed", False, str(exc))
    except KeyboardInterrupt:
        print("[e2e] interrupted", file=sys.stderr)
        return 130
    passed = len(report.results) - len(report.failed)
    print(f"\n[e2e] {passed}/{len(report.results)} checks passed")
    for name in report.failed:
        print(f"[e2e]   FAILED: {name}")
    return 0 if not report.failed else 1


if __name__ == "__main__":
    sys.exit(main())
