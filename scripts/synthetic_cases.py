#!/usr/bin/env python3
"""Add the synthetic example cases (``examples/synthetic/``) to the running demo stack.

Run the base demo once first (``python scripts/tasks.py demo``): this script builds on the
repository it leaves in ``.demo/repo``. Then it

1. **data** - writes the synthetic day-1 and day-2 files (``examples/synthetic/generate.py``,
   fixed seed) under ``.demo/synthetic/``;
2. **pipelines** - copies the three DAGs, their modules, the day-1 data and the fake LLM
   scripts into ``.demo/repo`` and commits them there, like a team adding pipelines to its
   repository;
3. **contracts** - runs ``dag-healer init`` (it keeps the existing contracts and drafts the
   new ones) and approves the three new contracts;
4. **day 2** - copies the day-2 files over ``data/incoming/`` (uncommitted), triggers the three
   DAGs and waits until dag-healer has handled every failure.

Expected outcomes: ``payments_settlement`` DATA reported, ``orders_enrichment`` CODE repaired
(with a PR.md) and ``fx_rates_publish`` AMBIGUOUS waiting for a human, because the pricing
service it publishes to is down. ``pricing-up`` starts a stand-in for that service in the
scheduler container (LocalExecutor runs the tasks there), so that clearing the failed task in
Airflow succeeds; ``pricing-down`` stops it.

Running it again is safe: unchanged files are not committed again, approved contracts stay
approved and a repeated failure is folded into its open incident.

Standard library only; it reuses the helpers of ``scripts/demo.py`` and ``scripts/tasks.py``.
"""

from __future__ import annotations

import argparse
import importlib.util
import shutil
import subprocess
import sys
import time
from collections.abc import Mapping, Sequence
from datetime import timedelta
from pathlib import Path
from types import ModuleType
from typing import Any

SCRIPTS_DIR = Path(__file__).resolve().parent
if str(SCRIPTS_DIR) not in sys.path:
    sys.path.insert(0, str(SCRIPTS_DIR))

import demo  # noqa: E402  (scripts/demo.py: Airflow and healer API helpers)
import tasks  # noqa: E402  (scripts/tasks.py: docker compose helpers)

PACK = tasks.ROOT / "examples" / "synthetic"
PACK_DIRS = ("dags", "src", "fixtures")
DEMO_REPO = tasks.DEMO_REPO
WORK_DIR = tasks.DEMO_DIR / "synthetic"
INCOMING = Path("data") / "incoming"
SCHEDULER = "airflow-scheduler"

PIPELINES_MESSAGE = "Add the payments, orders and FX rates pipelines"
CONTRACTS_MESSAGE = "Approve the data contracts of the new pipelines"

CASES: tuple[demo.Case, ...] = (
    demo.Case(
        "payments_settlement",
        "settle_payments",
        "data",
        "reported",
        True,
        "decimal-comma amounts, no currency",
    ),
    demo.Case(
        "orders_enrichment", "enrich_orders", "code", "repaired", True, "cancelled orders have 0 items"
    ),
    demo.Case(
        "fx_rates_publish", "publish_rates", "ambiguous", "waiting_human", True, "pricing service is down"
    ),
)

# A stand-in for the pricing service of fx_rates_publish (PUT /v1/fx-rates on 127.0.0.1:8907).
PRICING_MARKER = "dag-healer-pricing-stand-in"
PRICING_SERVER = """\
import http.server, json

class Handler(http.server.BaseHTTPRequestHandler):
    def do_PUT(self):
        size = int(self.headers.get("Content-Length") or 0)
        rates = json.loads(self.rfile.read(size) or b"{}").get("rates", [])
        body = json.dumps({"accepted": len(rates)}).encode()
        self.send_response(200)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def log_message(self, *args):
        pass

http.server.ThreadingHTTPServer(("127.0.0.1", 8907), Handler).serve_forever()
"""
PRICING_PROBE = (
    "import socket; s = socket.socket(); s.settimeout(2); "
    "print('up' if s.connect_ex(('127.0.0.1', 8907)) == 0 else 'down')"
)
PRICING_STOP = f"""\
import os, signal
for pid in filter(str.isdigit, os.listdir("/proc")):
    try:
        with open(f"/proc/{{pid}}/cmdline", "rb") as handle:
            if b"{PRICING_MARKER}" in handle.read() and int(pid) != os.getpid():
                os.kill(int(pid), signal.SIGTERM)
                print("stopped", pid)
    except OSError:
        pass
"""


def say(message: str) -> None:
    """Print a progress line."""
    print(f"[cases] {message}", flush=True)


# --------------------------------------------------------------------------- the pack


def load_generator() -> ModuleType:
    """``examples/synthetic/generate.py`` as a module."""
    path = PACK / "generate.py"
    spec = importlib.util.spec_from_file_location("synthetic_generate", path)
    if spec is None or spec.loader is None:
        raise demo.DemoError(f"cannot load {tasks.rel(path)}")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def pack_files() -> list[Path]:
    """The pack's pipeline files (DAGs, modules, fake LLM scripts), relative to the pack."""
    files = [
        path.relative_to(PACK)
        for top in PACK_DIRS
        for path in sorted((PACK / top).rglob("*"))
        if path.is_file() and "__pycache__" not in path.parts
    ]
    if not files:
        raise demo.DemoError(f"{tasks.rel(PACK)} has no pipeline files")
    return files


def check_demo_repo() -> None:
    """The base demo must have run: the pipelines reuse its ``src/etl/common.py`` and config."""
    if not (DEMO_REPO / "healer.yaml").is_file() or not (DEMO_REPO / "src" / "etl" / "common.py").is_file():
        raise demo.DemoError(
            f"{tasks.rel(DEMO_REPO)} is not ready: run the base demo first (python scripts/tasks.py demo)"
        )


def install(day1: Path) -> list[str]:
    """Copy the pipelines and the day-1 data into the demo repository; returns the copied paths."""
    copied: list[str] = []
    for relative in pack_files():
        target = DEMO_REPO / relative
        target.parent.mkdir(parents=True, exist_ok=True)
        shutil.copyfile(PACK / relative, target)
        copied.append(relative.as_posix())
    (DEMO_REPO / INCOMING).mkdir(parents=True, exist_ok=True)
    for source in sorted(day1.glob("*.csv")):
        shutil.copyfile(source, DEMO_REPO / INCOMING / source.name)
        copied.append((INCOMING / source.name).as_posix())
    return copied


def commit(paths: Sequence[str], message: str) -> bool:
    """Commit only ``paths`` in the demo repository; False when none of them changed."""
    demo.git_out(DEMO_REPO, "add", "--", *paths)
    if not demo.git_out(DEMO_REPO, "diff", "--cached", "--name-only", "--", *paths):
        return False
    demo.git_out(DEMO_REPO, "commit", "-q", "-m", message, "--", *paths)
    return True


def adopt() -> None:
    """Draft the new contracts with ``dag-healer init``, approve them and commit them."""
    demo.compose_exec(demo.APISERVER, ["dag-healer", "init", "--repo", demo.CONTAINER_REPO])
    paths = []
    for case in CASES:
        contract = Path("healer") / "contracts" / f"{case.dag_id}.yaml"
        if not (DEMO_REPO / contract).is_file():
            raise demo.DemoError(
                f"dag-healer init wrote no contract for {case.dag_id} (see its report above)"
            )
        if case.approve:
            demo.compose_exec(
                demo.APISERVER,
                ["dag-healer", "contracts", "approve", case.dag_id, "--by", demo.DEMO_APPROVER],
            )
        paths.append(contract.as_posix())
    if commit(paths, CONTRACTS_MESSAGE):
        say(f"committed the new contracts in {tasks.rel(DEMO_REPO)} ('{CONTRACTS_MESSAGE}')")


# --------------------------------------------------------------------------- pricing stand-in


def _scheduler_python(code: str, *args: str, detach: bool = False) -> tuple[int, str]:
    flags = ["-d", "-T"] if detach else ["-T"]
    return tasks.capture(
        tasks.compose_base() + ["exec", *flags, SCHEDULER, "python", "-c", code, *args], timeout=60
    )


def pricing_running() -> bool:
    """True when something listens on the pricing service port in the scheduler container."""
    code, out = _scheduler_python(PRICING_PROBE)
    return code == 0 and out.strip().endswith("up")


def pricing_up() -> None:
    """Start the pricing stand-in so that fx_rates_publish can publish again."""
    if pricing_running():
        say("the pricing stand-in is already running")
        return
    code, out = _scheduler_python(PRICING_SERVER, PRICING_MARKER, detach=True)
    if code != 0:
        raise demo.DemoError(f"could not start the pricing stand-in in {SCHEDULER}: {out.strip()}")
    for _ in range(20):
        if pricing_running():
            say(f"pricing stand-in listening on 127.0.0.1:8907 in {SCHEDULER}")
            say("clearing the failed fx_rates_publish.publish_rates task in Airflow should now succeed")
            return
        time.sleep(0.5)
    raise demo.DemoError("the pricing stand-in did not start listening")


def pricing_down() -> None:
    """Stop the pricing stand-in (fx_rates_publish fails again)."""
    code, out = _scheduler_python(PRICING_STOP)
    if code != 0:
        raise demo.DemoError(f"could not stop the pricing stand-in: {out.strip()}")
    say("pricing stand-in stopped" if "stopped" in out else "the pricing stand-in was not running")


# --------------------------------------------------------------------------- day 2


def run_day2(api: demo.AirflowSession, day2: Path, timeout: float) -> dict[str, dict]:
    """Drop the day-2 files, trigger the DAGs and wait for the incidents."""
    names = []
    for source in sorted(day2.glob("*.csv")):
        shutil.copyfile(source, DEMO_REPO / INCOMING / source.name)
        names.append(source.name)
    say(f"copied the day-2 files {{{','.join(names)}}} to {tasks.rel(DEMO_REPO / INCOMING)}/ (uncommitted)")
    runs: dict[str, str] = {}
    triggered_at = demo.utcnow() - timedelta(seconds=5)
    for case in CASES:
        demo.wait_for_dag(api, case.dag_id)
        runs[case.dag_id] = demo.trigger_dag(api, case.dag_id)
        say(f"triggered {case.dag_id}: {runs[case.dag_id]}")
    states = demo.wait_for_runs(api, runs)
    succeeded = sorted(dag_id for dag_id, state in states.items() if state != "failed")
    if succeeded:
        raise demo.DemoError(f"these runs did not fail on the day-2 files: {', '.join(succeeded)}")
    say(f"waiting for dag-healer (up to {int(timeout)} s)")
    return demo.wait_for_incidents(api, runs, timeout, since=triggered_at)


def print_summary(api: demo.AirflowSession, found: Mapping[str, Mapping[str, Any]]) -> None:
    """The incidents table, the links of each incident and where the PR.md files are."""
    rows, links, prs = [], {}, {}
    for case in CASES:
        incident = found.get(case.dag_id) or {}
        if incident:
            path = f"{demo.HEALER_API}/dags/{demo.quote(case.dag_id)}/incidents/{demo.quote(incident['id'])}"
            links[case.dag_id] = ((api.ok("GET", path) or {}).get("links") or {}).get("task_instance")
            if incident.get("has_pr_md"):
                prs[case.dag_id] = demo.copy_pr_artifacts(api, incident)
        rows.append(
            (
                case.dag_id,
                str(incident.get("task_id", "-")),
                str(incident.get("category") or "-"),
                str(incident.get("status", "-")),
                case.story,
            )
        )
    headers = ("DAG", "TASK", "CATEGORY", "STATUS", "STORY")
    widths = [max(len(row[i]) for row in [headers, *rows]) for i in range(len(headers))]
    print()
    for row in [headers, tuple("-" * width for width in widths), *rows]:
        print("  " + "  ".join(cell.ljust(width) for cell, width in zip(row, widths, strict=True)).rstrip())
    print()
    print(f"  Healer page ............ {demo.AIRFLOW_URL}/plugin/healer")
    for dag_id, link in links.items():
        if link:
            print(f"  {dag_id + ' ':.<23} {link}")
    print(f"  Mailpit (alerts) ....... {demo.MAILPIT_URL}")
    for dag_id, pr_path in prs.items():
        if pr_path is not None:
            print(f"  PR.md ({dag_id}) {tasks.rel(pr_path)}")
    print()
    print("  fx_rates_publish waits for a human: there is nothing to fix in its data or code. Bring the")
    print("  pricing service back (python scripts/synthetic_cases.py pricing-up), clear the failed task")
    print("  in Airflow and close the incident with 'Ignore'.")
    print()


# --------------------------------------------------------------------------- command line


def run(args: argparse.Namespace) -> None:
    """Add the cases to the demo repository and, unless --no-trigger, play day 2."""
    tasks.ensure_docker_running()
    if not tasks.wait_for_airflow(60):
        raise demo.DemoError("Airflow is not healthy: start the stack with python scripts/tasks.py up")
    check_demo_repo()

    say("1/4 data: writing the synthetic day-1 and day-2 files (fixed seed)")
    day1, day2 = WORK_DIR / "day1", WORK_DIR / "day2"
    load_generator().generate(day1, day2)

    say(f"2/4 pipelines: copying {tasks.rel(PACK)} into {tasks.rel(DEMO_REPO)}")
    if commit(install(day1), PIPELINES_MESSAGE):
        say(f"committed the pipelines in {tasks.rel(DEMO_REPO)} ('{PIPELINES_MESSAGE}')")
    else:
        say("the pipelines are already committed")

    say("3/4 contracts: dag-healer init (keeps the existing contracts) and approvals")
    adopt()
    if args.no_trigger:
        say("stopping before day 2 (--no-trigger)")
        return

    say("4/4 day 2: new files in data/incoming/, triggering the three DAGs")
    if pricing_running():
        say("stopping the pricing stand-in so that fx_rates_publish fails as in the story")
        pricing_down()
    api = demo.AirflowSession(args.user, args.password)
    found = run_day2(api, day2, args.timeout)
    print_summary(api, found)
    problems = demo.check_outcomes(found, CASES)
    if problems:
        raise demo.DemoError("unexpected outcomes: " + "; ".join(problems))


def parse_args(argv: Sequence[str] | None = None) -> argparse.Namespace:
    """Command line of the script."""
    parser = argparse.ArgumentParser(
        prog="python scripts/synthetic_cases.py",
        description="Add the synthetic example cases to the running demo (see the module docstring).",
    )
    parser.add_argument(
        "command",
        nargs="?",
        default="run",
        choices=("run", "pricing-up", "pricing-down"),
        help="run (default): add the cases and play day 2; pricing-up/pricing-down: the FX pricing stand-in",
    )
    parser.add_argument("--no-trigger", action="store_true", help="add pipelines and contracts, skip day 2")
    parser.add_argument(
        "--timeout", type=float, default=demo.DEFAULT_TIMEOUT, help="seconds to wait for the incidents"
    )
    parser.add_argument("--user", default="admin", help="Airflow user that triggers the DAGs (default admin)")
    parser.add_argument("--password", default="admin", help="its password (default admin)")
    return parser.parse_args(argv)


def main(argv: Sequence[str] | None = None) -> int:
    """Entry point; returns the exit code."""
    args = parse_args(argv)
    try:
        if args.command == "pricing-up":
            pricing_up()
        elif args.command == "pricing-down":
            pricing_down()
        else:
            run(args)
    except (demo.DemoError, tasks.TaskError, subprocess.SubprocessError) as exc:
        print(f"[cases] error: {exc}", file=sys.stderr, flush=True)
        return 1
    except KeyboardInterrupt:
        print("[cases] interrupted", file=sys.stderr, flush=True)
        return 130
    return 0


if __name__ == "__main__":
    sys.exit(main())
