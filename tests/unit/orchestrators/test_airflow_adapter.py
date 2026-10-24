"""The packaged conformance suite against the Airflow adapter, then the adapter's own rules.

The conformance harness serves the recorded Airflow 3.3.2 REST fixtures of
``tests/unit/fixtures/logs`` (``airflow_api/*`` and the real failed ``customers_sync`` log, plus
one synthetic stdout line carrying an e-mail address for check 12) from an
``httpx.MockTransport`` behind a real :class:`~dag_healer.airflow.client.AirflowClient` that
logs in with a username and a password. The repository is a copy of ``example_existing_repo``.
The seeded terminal failure is the recorded one (``customers_sync.transform``, try 1, a
``ValueError`` in ``parse_signup_date``); the retrying one is the same task in another run,
``up_for_retry`` with retries left. The end-to-end scenario serves the three demo failures with
the logs of the golden snapshots.

The rest of the module checks what the suite cannot see: the phase-5 rules that moved into the
adapter (poll window, confirmation, the retry message), compared with frozen copies of the
phase-5 code they came from, the payload checks of ``normalize``, the code locator against
``agent.context``, the error translation and the factory.
"""

from __future__ import annotations

import copy
import json
import logging
import posixpath
import shutil
from collections.abc import Callable, Mapping
from datetime import datetime, timedelta, timezone
from pathlib import Path
from types import SimpleNamespace
from typing import Any
from unittest import mock
from urllib.parse import parse_qs, quote

import httpx
import pytest

from dag_healer.airflow.adapter import (
    AIRFLOW_CAPABILITIES,
    AIRFLOW_VOCABULARY,
    CONTAINER_REPO_ROOT,
    DEFAULT_AIRFLOW_DAGS_FOLDER,
    EVENT_SOURCES,
    FACTORY,
    FACTORY_TARGET,
    INITIAL_LOOKBACK,
    LOG_SOURCE,
    MAX_EVENT_ERROR_CHARS,
    MAX_PIPELINE_MEMOS,
    MIN_POLL_OVERLAP,
    PIPELINE_MEMO_SECONDS,
    SANDBOX_ENV,
    STATE_NOT_BEFORE_KEY,
    STATE_WATERMARK_KEY,
    AirflowAdapter,
    AirflowCodeLocator,
    AirflowNativeLinks,
    attempt_state,
    cleared_count,
    cleared_task_instances,
    confirm_try,
    cursor_from_state,
    parse_timestamp,
    poll_cursor,
    reset_cursor,
)
from dag_healer.airflow.client import AirflowAPIError, AirflowClient, TaskInstanceInfo
from dag_healer.airflow.errors import (
    AIRFLOW_INFRA_ERRORS,
    AIRFLOW_TAXONOMY,
    error_text,
    is_transient_status,
    status_code_of,
    translate_error,
)
from dag_healer.config import HealerConfig, config_from_mapping, load_config
from dag_healer.core.errors import NotFound, NotSupported, OrchestratorError, PermissionDenied
from dag_healer.core.evidence import PipelineSource
from dag_healer.core.failures import AttemptState, FailureHint
from dag_healer.core.ids import RUN_LEVEL_STEP, UNRESOLVED_STEP, AttemptRef
from dag_healer.core.ports import OrchestratorAdapter
from dag_healer.core.specs import PathMapping
from dag_healer.core.vocab import RemediationKind, Vocabulary
from dag_healer.orchestrators import compat, registry
from dag_healer.orchestrators.conformance import (
    EndToEndCase,
    EndToEndSetup,
    OrchestratorConformance,
    SeededFailure,
    map_frame,
)
from dag_healer.orchestrators.registry import AdapterOptionsError

MakeConfig = Callable[..., HealerConfig]  # the make_config fixture of conftest.py
Handler = Callable[[httpx.Request], httpx.Response]

ROOT = Path(__file__).resolve().parents[3]
EXAMPLE_REPO = ROOT / "example_existing_repo"
LOGS = Path(__file__).resolve().parents[1] / "fixtures" / "logs"
BASE = "http://airflow.test:8080"
RUN = "manual__2026-10-17T18:48:46.093857+00:00"  # the recorded failed run of customers_sync
QUOTED_RUN = "manual__2026-10-17T18%3A48%3A46.093857%2B00%3A00"
RETRY_RUN = "scheduled__2026-10-18T06:00:00+00:00"
FORGED_RUN = "forged__2026-10-18T06:00:00+00:00"
E2E_RUN = "manual__2026-10-18T09:00:00+00:00"
PII_EMAIL = "alice.pii@example.com"
NOW = datetime(2026, 10, 18, 9, 0, tzinfo=timezone.utc)
SEEDED = AttemptRef("airflow", "customers_sync", RUN, "transform")
RETRYING = AttemptRef("airflow", "customers_sync", RETRY_RUN, "transform")
LIST_PATH = "/api/v2/dags/~/dagRuns/~/taskInstances"
GIT_IDENTITY = {
    "GIT_AUTHOR_NAME": "Test",
    "GIT_AUTHOR_EMAIL": "test@example.invalid",
    "GIT_COMMITTER_NAME": "Test",
    "GIT_COMMITTER_EMAIL": "test@example.invalid",
}
#: dag_id -> (task_id, log fixture, category, final status): the demo, as the golden snapshots play it.
E2E_CASES: dict[str, tuple[str, str, str, str]] = {
    "sales_daily_etl": (
        "transform_sales",
        "sales_daily_etl_transform_chained_keyerror.json",
        "data",
        "reported",
    ),
    "customers_sync": ("transform", "customers_sync_transform_failed.json", "code", "repaired"),
    "inventory_report": (
        "build_report",
        "inventory_report_build_report_plain.json",
        "ambiguous",
        "waiting_human",
    ),
}


# =========================================================================== helpers


def load(name: str) -> Any:
    """A JSON fixture of ``tests/unit/fixtures/logs`` (``airflow_api/x.json`` or ``x.json``)."""
    return json.loads((LOGS / name).read_text(encoding="utf-8"))


def segment(value: str) -> str:
    """One path segment encoded like the client does (``~`` stays Airflow's wildcard)."""
    return quote(value, safe="~")


def ti_path(dag_id: str, run_id: str, task_id: str) -> str:
    return f"/api/v2/dags/{segment(dag_id)}/dagRuns/{segment(run_id)}/taskInstances/{segment(task_id)}"


def task_instance(dag_id: str, run_id: str, task_id: str, **fields: Any) -> dict[str, Any]:
    """The recorded failed ``TaskInstanceResponse`` with other ids (and fields)."""
    data = copy.deepcopy(load("airflow_api/task_instance_failed.json"))
    data.update(dag_id=dag_id, dag_run_id=run_id, task_id=task_id, **fields)
    return data


def query(request: httpx.Request) -> dict[str, list[str]]:
    return parse_qs(request.url.query.decode(), keep_blank_values=True)


# =========================================================================== phase-5 references
# Frozen copies of the phase-5 worker and context code the adapter's rules were moved from (the
# worker and the context now ask the adapter), so the parity checks keep comparing against them.


def phase5_confirm(ti: TaskInstanceInfo, try_number: int, trigger: str) -> tuple[bool, str]:
    """``worker.pipeline._confirm`` of phase 5."""
    state = (ti.state or "").lower() or "unknown"
    if ti.try_number < try_number:
        return False, f"Airflow knows only {ti.try_number} tries (state {state})"
    if trigger == "every_failure":
        if state in ("failed", "up_for_retry") or ti.try_number > try_number:
            return True, f"state {state}, try {ti.try_number}"
        return False, f"the task instance is {state}, not failed"
    if ti.try_number > try_number:
        return False, f"try {try_number} was retried (try {ti.try_number} is {state})"
    if state == "failed":
        return True, f"state failed, try {ti.try_number}"
    if state == "up_for_retry":
        return False, "the try will be retried (monitor.trigger is final_failure)"
    return False, f"the task instance is {state}, not failed"


def phase5_poll_since(
    watermark: datetime | None, floor: datetime | None, interval: int, now: datetime
) -> datetime:
    """``worker.pipeline._poll_since`` of phase 5, on the values it read from ``state.json``."""
    if watermark is None:
        since = now - timedelta(hours=24)
    else:
        since = watermark - max(timedelta(seconds=60), timedelta(seconds=2 * interval))
    if floor is not None and since < floor:
        since = floor
    return since


def phase5_api_dag_file(cfg: HealerConfig, dag: Mapping[str, Any]) -> str | None:
    """``agent.context._api_dag_file`` of phase 5 for one ``get_dag`` answer (without its note)."""
    from dag_healer.agent.context import map_to_repo_path

    def inside_repo(rel: str) -> bool:
        try:
            root = cfg.repo_root
            candidate = (root / rel).resolve()
            candidate.relative_to(root)
        except (OSError, ValueError):
            return False
        return candidate.is_file()

    def join_rel(base: str | None, rel: str) -> str | None:
        joined = (
            posixpath.normpath(posixpath.join(base, rel)) if base and base != "." else posixpath.normpath(rel)
        )
        if joined in (".", "") or joined == ".." or joined.startswith("../") or joined.startswith("/"):
            return None
        return joined

    mapped = map_to_repo_path(dag.get("fileloc"), cfg) if dag.get("fileloc") else None
    if mapped and inside_repo(mapped):
        return mapped
    relative = dag.get("relative_fileloc")
    if isinstance(relative, str) and relative.strip():
        candidate = join_rel(cfg.repo.dags_dir or "dags", relative.strip().replace("\\", "/"))
        if candidate and inside_repo(candidate):
            return candidate
    return None


class AirflowServer:
    """A small Airflow 3 api-server for ``httpx.MockTransport``: tokens, routes, an outage switch.

    Routes match the method and the raw (percent-encoded) path; an unknown route answers 404.
    Handlers never assert: the adapter would turn an AssertionError into an orchestrator error.
    """

    def __init__(self, *, auth: bool = True, password: str = "admin") -> None:
        self.auth = auth
        self.password = password
        self.routes: dict[tuple[str, str], Handler] = {}
        self.requests: list[httpx.Request] = []
        self.tokens: set[str] = set()
        self.outage: str | None = None

    def route(self, method: str, path: str, response: Any) -> None:
        """Serve ``response`` (a handler, or a JSON body answered with 200) at ``method`` + ``path``."""
        if callable(response):
            self.routes[(method, path)] = response
        else:
            self.routes[(method, path)] = lambda _request, body=response: httpx.Response(200, json=body)

    def __call__(self, request: httpx.Request) -> httpx.Response:
        self.requests.append(request)
        if self.outage == "network":
            raise httpx.ConnectError("connection refused", request=request)
        if self.outage == "server_error":
            return httpx.Response(500, json={"detail": "Internal Server Error"})
        path = request.url.raw_path.decode().split("?", 1)[0]
        if request.method == "POST" and path == "/auth/token":
            if json.loads(request.content) != {"username": "admin", "password": self.password}:
                return httpx.Response(401, json={"detail": "Invalid credentials"})
            token = f"token-{len(self.tokens) + 1}"
            self.tokens.add(token)
            return httpx.Response(201, json={"access_token": token})
        if self.auth and path != "/api/v2/monitor/health":
            if request.headers.get("Authorization", "").removeprefix("Bearer ") not in self.tokens:
                return httpx.Response(401, json={"detail": "Not authenticated"})
        handler = self.routes.get((request.method, path))
        if handler is None:
            return httpx.Response(404, json={"detail": "Not Found"})
        return handler(request)

    @property
    def api_calls(self) -> list[tuple[str, str]]:
        """``(method, raw path)`` of every request except the logins."""
        return [
            (r.method, r.url.raw_path.decode().split("?", 1)[0])
            for r in self.requests
            if r.url.path != "/auth/token"
        ]

    def bodies(self, method: str, path: str) -> list[Any]:
        return [
            json.loads(r.content)
            for r in self.requests
            if r.method == method and r.url.raw_path.decode().split("?", 1)[0] == path
        ]


def listing(items: list[dict[str, Any]]) -> Handler:
    """``GET .../taskInstances`` honouring ``state``, ``limit`` and ``offset`` like Airflow."""

    def handler(request: httpx.Request) -> httpx.Response:
        params = query(request)
        wanted = params.get("state")
        rows = [item for item in items if wanted is None or item["state"] in wanted]
        offset = int(params.get("offset", ["0"])[0])
        limit = int(params.get("limit", ["100"])[0])
        return httpx.Response(
            200, json={"task_instances": rows[offset : offset + limit], "total_entries": len(rows)}
        )

    return handler


def customers_log() -> dict[str, Any]:
    """The recorded failed ``customers_sync`` log plus one stdout line with an e-mail address."""
    log = load("customers_sync_transform_failed.json")
    position = next(i for i, m in enumerate(log["content"]) if m.get("event") == "Task failed with exception")
    log["content"].insert(
        position,
        {
            "timestamp": "2026-10-17T18:48:53.080000Z",
            "event": f"first row: signup_date of {PII_EMAIL} is 2026-10-01T08:30:00Z",
            "level": "info",
            "logger": "task.stdout",
        },
    )
    return log


def conformance_server() -> AirflowServer:
    """The recorded ``customers_sync`` failure, a retrying try, the DAG, its source and actions."""
    server = AirflowServer()
    server.route("GET", LIST_PATH, listing(load("airflow_api/task_instances.json")["task_instances"]))
    server.route("GET", ti_path(*SEEDED.key()[1:4]), load("airflow_api/task_instance_failed.json"))
    server.route("GET", ti_path(*SEEDED.key()[1:4]) + "/logs/1", customers_log())
    retrying = task_instance("customers_sync", RETRY_RUN, "transform", state="up_for_retry", max_tries=2)
    server.route("GET", ti_path(*RETRYING.key()[1:4]), retrying)
    server.route("GET", "/api/v2/dags/customers_sync", load("airflow_api/dag_customers_sync.json"))
    server.route(
        "GET", "/api/v2/dagSources/customers_sync", load("airflow_api/dag_source_customers_sync.json")
    )
    server.route(
        "GET",
        f"/api/v2/dags/customers_sync/dagRuns/{QUOTED_RUN}",
        load("airflow_api/dag_run_customers_sync.json"),
    )
    server.route(
        "POST",
        "/api/v2/dags/customers_sync/clearTaskInstances",
        load("airflow_api/clear_task_instances.json"),
    )
    server.route("POST", "/api/v2/dags/customers_sync/dagRuns", load("airflow_api/trigger_dag_run.json"))
    server.route("GET", "/api/v2/monitor/health", load("airflow_api/health.json"))
    return server


def copy_repo(target: Path) -> Path:
    shutil.copytree(EXAMPLE_REPO, target, ignore=shutil.ignore_patterns("__pycache__", "processed", ".git"))
    return target


def airflow_config(repo: Path, **sections: Any) -> HealerConfig:
    data: dict[str, Any] = {
        "repo": {"path": str(repo)},
        "airflow": {"api_url": BASE, "username": "admin", "password": "admin"},
        **sections,
    }
    return config_from_mapping(data)


def served_adapter(tmp_path: Path, **kwargs: Any) -> tuple[AirflowAdapter, AirflowServer, HealerConfig]:
    """An adapter on the conformance server (a copy of the example repository)."""
    cfg = airflow_config(copy_repo(tmp_path / "repo"))
    server = conformance_server()
    client = AirflowClient.from_config(cfg, transport=httpx.MockTransport(server))
    return AirflowAdapter(cfg, client=client, **kwargs), server, cfg


def prepare_demo_repo(repo: Path) -> None:
    """A git copy of example_existing_repo prepared like the demo (init, approvals, day-2 files)."""
    from dag_healer.cli.init_cmd import run_init
    from dag_healer.contracts.io import approve_contract, contract_path, load_contract, save_contract
    from dag_healer.repo.git import run_git

    copy_repo(repo)
    run_git(["init", "--quiet", "-b", "main"], repo)
    run_git(["add", "-A"], repo)
    run_git(["-c", "commit.gpgsign=false", "commit", "--quiet", "-m", "day 1"], repo, env=GIT_IDENTITY)
    assert run_init(repo).ok
    for dag_id in ("sales_daily_etl", "customers_sync"):
        path = contract_path(repo / "healer" / "contracts", dag_id)
        save_contract(
            approve_contract(load_contract(path), "data-lead").model_copy(update={"approved_at": NOW}), path
        )
    for name in ("sales", "customers", "inventory"):
        shutil.copyfile(
            repo / "fixtures" / "day2" / f"{name}.csv", repo / "data" / "incoming" / f"{name}.csv"
        )


class PhaseFiveClient:
    """A phase-5 style double of AirflowClient: TaskInstanceInfo rows, logs and recorded calls."""

    base_url = "http://airflow.double:8080"

    def __init__(self) -> None:
        self.instances: dict[tuple[str, str, str, int], TaskInstanceInfo] = {}
        self.listed: list[TaskInstanceInfo] = []
        self.logs: dict[tuple[str, str, str, int, int], str] = {}
        self.errors: dict[str, Exception] = {}
        self.calls: list[tuple[Any, ...]] = []
        self.closed = False

    def add(
        self,
        dag_id: str,
        run_id: str,
        task_id: str,
        *,
        try_number: int = 1,
        max_tries: int = 0,
        map_index: int = -1,
        state: str | None = "failed",
        end_date: str | None = "2026-10-18T08:59:00Z",
        log: str | None = None,
        listed: bool = True,
    ) -> TaskInstanceInfo:
        info = TaskInstanceInfo(
            dag_id=dag_id,
            run_id=run_id,
            task_id=task_id,
            map_index=map_index,
            try_number=try_number,
            max_tries=max_tries,
            state=state,
            operator="PythonOperator",
            start_date="2026-10-18T08:58:00Z",
            end_date=end_date,
        )
        self.instances[(dag_id, run_id, task_id, map_index)] = info
        if listed:
            self.listed.append(info)
        if log is not None:
            self.logs[(dag_id, run_id, task_id, try_number, map_index)] = log
        return info

    @property
    def list_calls(self) -> list[datetime | None]:
        return [call[1] for call in self.calls if call[0] == "list"]

    def _fail(self, name: str) -> None:
        error = self.errors.get(name)
        if error is not None:
            raise error

    def list_failed_task_instances(
        self, *, updated_since: datetime | None = None, **kwargs: Any
    ) -> list[TaskInstanceInfo]:
        self.calls.append(("list", updated_since, kwargs))
        self._fail("list")
        return list(self.listed)

    def get_task_instance(
        self, dag_id: str, run_id: str, task_id: str, map_index: int = -1
    ) -> TaskInstanceInfo:
        self.calls.append(("ti", dag_id, run_id, task_id, map_index))
        self._fail("ti")
        try:
            return self.instances[(dag_id, run_id, task_id, int(map_index))]
        except KeyError:
            raise AirflowAPIError("The Task Instance was not found", status_code=404) from None

    def get_task_log(
        self, dag_id: str, run_id: str, task_id: str, try_number: int, map_index: int = -1
    ) -> str:
        self.calls.append(("log", dag_id, run_id, task_id, try_number, map_index))
        self._fail("log")
        try:
            return self.logs[(dag_id, run_id, task_id, int(try_number), int(map_index))]
        except KeyError:
            raise AirflowAPIError("log not found", status_code=404) from None

    def clear_task_instances(
        self,
        dag_id: str,
        run_id: str,
        task_ids: list[Any],
        *,
        only_failed: bool = True,
        include_downstream: bool = False,
    ) -> dict[str, Any]:
        self.calls.append(("clear", dag_id, run_id, list(task_ids), only_failed, include_downstream))
        self._fail("clear")
        return {
            "task_instances": [
                {"task_id": t, "map_index": -1}
                if isinstance(t, str)
                else {"task_id": t[0], "map_index": t[1]}
                for t in task_ids
            ]
        }

    def close(self) -> None:
        self.closed = True


def hint(ref: AttemptRef, *, source: str = "listener", error: str = "") -> FailureHint:
    return FailureHint(ref=ref, channel="task-failed", source=source, error_text=error)


# =========================================================================== the harness


class AirflowHarness:
    """:class:`~dag_healer.orchestrators.conformance.ConformanceHarness` of the Airflow adapter."""

    def __init__(self) -> None:
        self.server: AirflowServer | None = None

    def build(self, tmp_path: Path) -> tuple[HealerConfig, OrchestratorAdapter]:
        cfg = airflow_config(copy_repo(tmp_path / "repo"))
        self.server = conformance_server()
        client = AirflowClient.from_config(cfg, transport=httpx.MockTransport(self.server))
        return cfg, FACTORY.create(cfg, client=client)

    def seeded_failure(self) -> SeededFailure:
        return SeededFailure(SEEDED, True, "ValueError", "src/etl/customers.py", "src/etl/customers.py")

    def seeded_retrying(self) -> SeededFailure | None:
        return SeededFailure(RETRYING, False, "ValueError", "src/etl/customers.py", "src/etl/customers.py")

    def forged_hint(self) -> FailureHint | None:
        forged = AttemptRef("airflow", "customers_sync", FORGED_RUN, "transform")
        return hint(forged, error="ValueError: forged")

    def push_samples(self) -> list[tuple[str, dict, dict]]:
        error = (
            "Traceback (most recent call last):\n"
            '  File "/opt/airflow/repo/src/etl/customers.py", line 19, in parse_signup_date\n'
            f"ValueError: time data '{PII_EMAIL}' does not match format '%Y-%m-%d'"
        )
        listener = {
            "dag_id": "customers_sync",
            "task_id": "transform",
            "run_id": RUN,
            "map_index": -1,
            "try_number": 1,
            "max_tries": 0,
            "state": "failed",
            "hostname": "9549ef6aaa86",
            "error": error,
            "source": "listener",
            "reported_at": "2026-10-17T18:48:53.140000+00:00",
        }
        callback = {
            **listener,
            "source": "callback",
            "input": {"rows": [{"email": PII_EMAIL, "signup_date": "2026-10-01T08:30:00Z"}]},
            "output": {"note": "partial output of the failed task"},
            "parameters": {"api_token": "not-a-real-token"},
        }
        return [("task-failed", listener, {"X-Healer-Token": "ingest-token"}), ("task-failed", callback, {})]

    def io_calls(self) -> int:
        assert self.server is not None
        return len(self.server.requests)

    def simulate_outage(self, kind: str) -> None:
        assert self.server is not None
        self.server.outage = kind

    def pii_strings(self) -> list[str]:
        return [PII_EMAIL]

    def adapter_modules(self) -> list[str]:
        return ["dag_healer.airflow.adapter", "dag_healer.airflow.errors"]

    def end_to_end(self, tmp_path: Path) -> EndToEndSetup:
        repo = tmp_path / "e2e_repo"
        prepare_demo_repo(repo)
        cfg = load_config(repo / "healer.yaml")
        cfg.repo.sandbox.backend = "process"  # the unit tests have no container engine
        server = AirflowServer(auth=False)
        rows = []
        for dag_id, (task_id, log_name, _category, _status) in E2E_CASES.items():
            row = task_instance(dag_id, E2E_RUN, task_id)
            rows.append(row)
            server.route("GET", ti_path(dag_id, E2E_RUN, task_id), row)
            server.route("GET", ti_path(dag_id, E2E_RUN, task_id) + "/logs/1", load(log_name))
            server.route(
                "GET",
                f"/api/v2/dags/{dag_id}",
                {
                    "dag_id": dag_id,
                    "fileloc": f"/opt/airflow/repo/dags/{dag_id}.py",
                    "relative_fileloc": f"{dag_id}.py",
                },
            )
        server.route("GET", LIST_PATH, listing(rows))
        adapter = FACTORY.create(cfg, client=AirflowClient(BASE, transport=httpx.MockTransport(server)))
        cases = tuple(
            EndToEndCase(AttemptRef("airflow", dag_id, E2E_RUN, task_id), category, status)  # type: ignore[arg-type]
            for dag_id, (task_id, _log, category, status) in E2E_CASES.items()
        )
        return EndToEndSetup(cfg=cfg, adapter=adapter, cases=cases)


class TestAirflowConformance(OrchestratorConformance):
    """The packaged suite against Airflow (check 14 plays the demo's three incidents end to end)."""

    @pytest.fixture
    def harness(self, healer_env: Path) -> AirflowHarness:
        return AirflowHarness()


# =========================================================================== static parts and factory


def test_static_attributes_and_ports() -> None:
    adapter = AirflowAdapter(HealerConfig())
    assert (adapter.type, adapter.instance, adapter.display_name) == ("airflow", "airflow", "Apache Airflow")
    assert adapter.vocabulary is AIRFLOW_VOCABULARY and adapter.vocabulary == Vocabulary()
    assert adapter.capabilities is AIRFLOW_CAPABILITIES
    assert AIRFLOW_CAPABILITIES.sources == ("rest", "listener", "callback", "cli")
    assert AIRFLOW_CAPABILITIES.push_channels == ("task-failed",) and adapter.channels == {"task-failed"}
    assert AIRFLOW_CAPABILITIES.remediations == (RemediationKind.RETRY_STEP,)
    assert AIRFLOW_CAPABILITIES.ui_hosts == ("airflow_plugin", "standalone")
    assert AIRFLOW_CAPABILITIES.native_authz and AIRFLOW_CAPABILITIES.fix_delivery == "repo_sync"
    assert adapter.taxonomy is AIRFLOW_TAXONOMY and adapter.queue is None
    assert all(
        port is adapter for port in (adapter.feed, adapter.push, adapter.inspector, adapter.remediator)
    )
    assert isinstance(adapter.links, AirflowNativeLinks) and isinstance(adapter.code, AirflowCodeLocator)
    assert isinstance(adapter, OrchestratorAdapter) and compat.is_adapter(adapter)
    assert not compat.is_legacy_client(adapter)
    assert adapter.sandbox_env() == SANDBOX_ENV and adapter.sandbox_env() is not SANDBOX_ENV
    assert adapter._client is None  # nothing was built
    assert repr(adapter) == "AirflowAdapter(instance='airflow', base_url='http://localhost:8080')"


def test_taxonomy_repeats_the_airflow_infra_types_of_the_policy() -> None:
    from dag_healer.agent.policy import INFRA_TYPES

    assert AIRFLOW_INFRA_ERRORS == {
        "AirflowTaskTimeout",
        "AirflowTaskTerminated",
        "AirflowZombieTaskException",
        "TaskDeferralTimeout",
    }
    assert AIRFLOW_TAXONOMY.infra == AIRFLOW_INFRA_ERRORS <= INFRA_TYPES
    assert {
        name for name in INFRA_TYPES if name.startswith(("Airflow", "TaskDeferral"))
    } == AIRFLOW_INFRA_ERRORS
    empty = (AIRFLOW_TAXONOMY.data_shape, AIRFLOW_TAXONOMY.code, AIRFLOW_TAXONOMY.definition)
    assert empty == (frozenset(), frozenset(), frozenset())
    assert AIRFLOW_TAXONOMY.aggregate == AIRFLOW_TAXONOMY.infra_event_types == frozenset()
    assert AIRFLOW_TAXONOMY.kind_of("AirflowTaskTimeout") == "infra"
    assert AIRFLOW_TAXONOMY.kind_of("airflow.exceptions.AirflowZombieTaskException") == "infra"
    assert AIRFLOW_TAXONOMY.kind_of("KeyError") is None and AIRFLOW_TAXONOMY.kind_of(None) is None


def test_factory_and_registry(make_config: MakeConfig) -> None:
    assert (FACTORY.type, FACTORY.display_name, FACTORY.options_model, FACTORY.secret_options) == (
        "airflow",
        "Apache Airflow",
        None,
        (),
    )
    assert registry.load_factory("airflow") is FACTORY
    assert registry.load_factory(FACTORY_TARGET) is FACTORY
    default = registry.get_adapter(HealerConfig())
    assert isinstance(default, AirflowAdapter) and default.instance == "airflow"
    assert registry.get_adapter(HealerConfig()) is default  # cached, no I/O
    assert (
        FACTORY.create(make_config({"type": "airflow", "instance": "airflow-prod"})).instance
        == "airflow-prod"
    )
    with pytest.raises(AdapterOptionsError, match="takes no orchestrator.options"):
        FACTORY.create(make_config({"type": "airflow", "options": {"api_url": "http://x"}}))
    with pytest.raises(AdapterOptionsError):
        registry.get_adapter(make_config({"type": "airflow", "options": {"x": 1}}))
    assert FACTORY.create(make_config({"type": FACTORY_TARGET})).instance == "airflow"
    assert FACTORY.create(make_config({"type": FACTORY_TARGET, "instance": "af-2"})).instance == "af-2"
    # a legacy client wrapped for a configuration of another type: the phase-5 instance, options ignored
    other = make_config({"type": "fake", "instance": "fake-dev", "options": {"world": "w.yaml"}})
    assert FACTORY.create(other, client=PhaseFiveClient()).instance == "airflow"
    # the real orchestrator: section of healer.yaml (config.OrchestratorConfig)
    real = config_from_mapping({"orchestrator": {"type": "airflow", "instance": "airflow-eu"}})
    assert registry.get_adapter(real).instance == "airflow-eu"
    assert FACTORY.create(config_from_mapping({})).instance == "airflow"


def test_compat_wraps_phase5_doubles(make_config: MakeConfig) -> None:
    class ClearOnly:
        def clear_task_instances(
            self, dag_id: str, run_id: str, task_ids: list[Any], **_: Any
        ) -> dict[str, Any]:
            return {"total_entries": 2}

    cfg = make_config()
    adapter = compat.as_adapter(cfg, ClearOnly())
    assert isinstance(adapter, AirflowAdapter) and adapter.client.__class__ is ClearOnly
    result = adapter.remediate(SEEDED, {}, RemediationKind.RETRY_STEP, idempotency_key="k", actor="ann")
    assert result.detail == (
        f"ann cleared transform and any failed downstream tasks in run {RUN} (2 task instance(s)); "
        "Airflow will run them again."
    )
    with pytest.raises(NotSupported, match="get_task_instance"):
        adapter.describe(SEEDED, {})
    with pytest.raises(NotSupported) as raised:
        adapter.poll(cursor=None, is_monitored=lambda _dag: True)
    assert not raised.value.transient
    bundle = adapter.logs(SEEDED, None)
    assert (bundle.source, bundle.confidence) == ("none", "none") and "get_task_log" in bundle.notes[0]
    assert adapter.confirm(hint(SEEDED), trigger="final_failure").verdict == "retry_later"
    assert compat.as_adapter(cfg, adapter) is adapter
    mocked = compat.as_adapter(cfg, mock.MagicMock())
    assert isinstance(mocked, AirflowAdapter)
    double = PhaseFiveClient()
    wrapped = compat.as_adapter(cfg, double)
    assert wrapped is not None
    wrapped.close()
    assert not double.closed  # an injected client belongs to the caller


def test_the_client_is_built_on_first_use_and_closed(monkeypatch: pytest.MonkeyPatch) -> None:
    cfg = config_from_mapping(
        {"airflow": {"api_url": "http://airflow-apiserver:8080/", "username": "svc", "password": "pw"}}
    )
    adapter = AirflowAdapter(cfg)
    assert adapter._client is None and adapter.base_url == "http://airflow-apiserver:8080"
    client = adapter.client
    assert isinstance(client, AirflowClient) and adapter.client is client
    assert (client.base_url, client.username, client.can_login) == (
        "http://airflow-apiserver:8080",
        "svc",
        True,
    )
    adapter.close()
    adapter.close()
    assert client._http.is_closed and adapter._client is None
    assert adapter.client is not client  # rebuilt on the next use

    def broken(cls: Any, cfg: Any, **kwargs: Any) -> Any:
        raise ValueError("Airflow base_url must not be empty")

    monkeypatch.setattr(AirflowClient, "from_config", classmethod(broken))
    failing = AirflowAdapter(cfg)
    with pytest.raises(OrchestratorError, match="cannot create the Airflow API client") as raised:
        failing.poll(cursor=None, is_monitored=lambda _dag: True)
    assert not raised.value.transient
    assert failing.logs(SEEDED, None).confidence == "none"
    assert failing.confirm(hint(SEEDED), trigger="final_failure").verdict == "retry_later"
    assert not failing.health().ok


# =========================================================================== FailureFeed


def test_poll_lists_failed_task_instances_in_the_phase5_window() -> None:
    client = PhaseFiveClient()
    client.add("etl", "r1", "t1", try_number=2, max_tries=1)
    cfg = config_from_mapping({"monitor": {"poll_interval_seconds": 30}})
    adapter = AirflowAdapter(cfg, client=client, clock=lambda: NOW)
    page = adapter.poll(cursor=None, is_monitored=lambda _dag: True)
    assert client.list_calls == [NOW - INITIAL_LOOKBACK]
    assert client.calls[0][2] == {}  # exactly list_failed_task_instances(updated_since=since)
    assert page.cursor == {"watermark": "2026-10-18T09:00:00+00:00", "not_before": None} and page.complete
    (failure,) = page.failures
    assert failure.ref == AttemptRef("airflow", "etl", "r1", "t1", -1, 2)
    assert (failure.state, failure.is_final, failure.source) == (AttemptState.FAILED, True, "rest")
    assert (failure.error_name, failure.error_summary, dict(failure.native)) == (None, "", {})
    assert failure.failed_at == datetime(2026, 10, 18, 8, 59, tzinfo=timezone.utc)
    later = NOW + timedelta(minutes=5)
    second = adapter.poll(cursor=page.cursor, is_monitored=lambda _dag: True, now=later)
    assert client.list_calls[-1] == NOW - MIN_POLL_OVERLAP
    assert second.cursor["watermark"] == later.isoformat()
    slow = AirflowAdapter(config_from_mapping({"monitor": {"poll_interval_seconds": 90}}), client=client)
    assert slow.poll_since(page.cursor, now=later) == NOW - timedelta(seconds=180)


def test_poll_never_lists_before_the_floor(caplog: pytest.LogCaptureFixture) -> None:
    client = PhaseFiveClient()
    adapter = AirflowAdapter(HealerConfig(), client=client, clock=lambda: NOW)
    floor = NOW - timedelta(seconds=30)
    page = adapter.poll(cursor=poll_cursor(NOW, floor), is_monitored=lambda _dag: True)
    assert client.list_calls == [floor] and page.cursor == poll_cursor(NOW, floor)
    assert adapter.poll_since({"not_before": floor.isoformat()}) == floor  # floor without a watermark
    old_floor = NOW - timedelta(days=3)
    assert adapter.poll_since({"not_before": old_floor.isoformat()}) == NOW - INITIAL_LOOKBACK
    assert adapter.poll_since({"watermark": "2026-10-18T08:00:00Z"}) == datetime(
        2026, 10, 18, 7, 59, tzinfo=timezone.utc
    )
    with caplog.at_level(logging.WARNING, logger="dag_healer.airflow.adapter"):
        assert adapter.poll_since({"watermark": "garbage", "not_before": ""}) == NOW - INITIAL_LOOKBACK
    assert "ignoring the invalid watermark 'garbage'" in caplog.text
    assert adapter.poll_since(["not", "a", "mapping"]) == NOW - INITIAL_LOOKBACK  # type: ignore[arg-type]


def test_poll_window_matches_the_phase5_worker(healer_env: Path) -> None:
    from dag_healer.worker import pipeline

    cases = [
        (None, None, 30),
        (NOW - timedelta(minutes=10), None, 30),
        (NOW - timedelta(minutes=10), None, 120),
        (NOW - timedelta(minutes=10), NOW - timedelta(minutes=9, seconds=30), 30),
        (None, NOW - timedelta(hours=2), 30),
        (NOW - timedelta(minutes=10), NOW - timedelta(days=2), 30),
    ]
    for watermark, floor, interval in cases:
        cfg = config_from_mapping({"monitor": {"poll_interval_seconds": interval}})
        state = {}
        if watermark is not None:
            state[pipeline.WATERMARK_KEY] = watermark.isoformat()
        if floor is not None:
            state[pipeline.NOT_BEFORE_KEY] = floor.isoformat()
        pipeline.save_state(cfg, state)
        cursor = cursor_from_state(pipeline.load_state(cfg))
        adapter = AirflowAdapter(cfg, client=PhaseFiveClient())
        expected = phase5_poll_since(watermark, floor, interval, NOW)
        assert adapter.poll_since(cursor, now=NOW) == expected, (watermark, floor, interval)


def test_the_worker_writes_and_imports_the_adapter_cursor(tmp_path: Path, healer_env: Path) -> None:
    """``healer_kv`` holds the very cursor text the adapter reads and writes."""
    from dag_healer.store import dispose_engines, session_scope
    from dag_healer.store import repo as store_repo
    from dag_healer.worker import pipeline

    def cursor_of(cfg: HealerConfig) -> Any:
        with session_scope(cfg.database_url) as session:
            return store_repo.kv_get(session, store_repo.cursor_key("airflow"))

    try:
        reset = config_from_mapping({"store": {"url": f"sqlite:///{(tmp_path / 'reset.db').as_posix()}"}})
        assert cursor_of(reset) is None
        moment = pipeline.reset_state(reset, NOW)
        assert cursor_of(reset) == reset_cursor(moment)

        state = {STATE_WATERMARK_KEY: "2026-10-18T08:00:00Z", STATE_NOT_BEFORE_KEY: "2026-10-18T07:00:00Z"}
        imported = config_from_mapping({"store": {"url": f"sqlite:///{(tmp_path / 'import.db').as_posix()}"}})
        pipeline.save_state(imported, state)
        assert pipeline.get_watermark(imported) == datetime(2026, 10, 18, 8, 0, tzinfo=timezone.utc)
        assert cursor_of(imported) == cursor_from_state(state)
    finally:
        dispose_engines()
    assert (pipeline.WATERMARK_KEY, pipeline.NOT_BEFORE_KEY) == (STATE_WATERMARK_KEY, STATE_NOT_BEFORE_KEY)
    assert (pipeline.INITIAL_LOOKBACK, pipeline.MIN_POLL_OVERLAP) == (INITIAL_LOOKBACK, MIN_POLL_OVERLAP)


def test_cursor_helpers() -> None:
    assert reset_cursor(NOW) == {
        "watermark": "2026-10-18T09:00:00+00:00",
        "not_before": "2026-10-18T09:00:00+00:00",
    }
    assert poll_cursor(None) == {"watermark": None, "not_before": None}
    naive = datetime(2026, 10, 18, 9, 0)
    assert poll_cursor(naive)["watermark"] == "2026-10-18T09:00:00+00:00"
    assert cursor_from_state({}) is None
    assert cursor_from_state({"rest_watermark": "nonsense"}) is None
    assert cursor_from_state({"rest_watermark": "2026-10-18T09:00:00Z", "other": 1}) == {
        "watermark": "2026-10-18T09:00:00+00:00",
        "not_before": None,
    }
    assert parse_timestamp("2026-10-17T18:48:53.147334Z") == datetime(
        2026, 10, 17, 18, 48, 53, 147334, tzinfo=timezone.utc
    )
    assert parse_timestamp("2026-10-17T15:48:53-03:00") == datetime(
        2026, 10, 17, 18, 48, 53, tzinfo=timezone.utc
    )
    assert (
        parse_timestamp(None) is parse_timestamp("") is parse_timestamp(17) is parse_timestamp("soon") is None
    )


def test_poll_skips_unmonitored_dags_and_incomplete_rows() -> None:
    client = PhaseFiveClient()
    client.add("etl", RUN, "t1")
    client.add("other", RUN, "t1")
    client.add("etl", RUN, "", map_index=2)
    client.add("etl", RUN, "t2", map_index=3, end_date=None)
    adapter = AirflowAdapter(HealerConfig(), client=client)
    page = adapter.poll(cursor=None, is_monitored=lambda dag_id: dag_id == "etl")
    assert [f.ref for f in page.failures] == [
        AttemptRef("airflow", "etl", RUN, "t1"),
        AttemptRef("airflow", "etl", RUN, "t2", 3, 1),
    ]
    assert page.failures[1].failed_at is None


@pytest.mark.parametrize(
    ("error", "kind", "transient"),
    [
        (AirflowAPIError("Service Unavailable", status_code=503), OrchestratorError, True),
        (AirflowAPIError("GET /api/v2/...: ConnectError: refused"), OrchestratorError, True),
        (AirflowAPIError("query.limit: bad", status_code=422), OrchestratorError, False),
        (AirflowAPIError("Forbidden", status_code=403), PermissionDenied, False),
        (RuntimeError("the double broke"), OrchestratorError, True),
    ],
)
def test_poll_errors_are_core_errors(error: Exception, kind: type, transient: bool) -> None:
    client = PhaseFiveClient()
    client.errors["list"] = error
    with pytest.raises(OrchestratorError) as raised:
        AirflowAdapter(HealerConfig(), client=client).poll(cursor=None, is_monitored=lambda _dag: True)
    assert type(raised.value) is kind and raised.value.transient is transient
    assert raised.value.__cause__ is error and raised.value.status_code == getattr(error, "status_code", None)


# =========================================================================== PushIntake


def test_normalize_reads_the_ingest_payload() -> None:
    from dag_healer.ingest import build_payload

    ti = SimpleNamespace(
        dag_id="etl", task_id="group.load", run_id=RUN, map_index=4, try_number=2, max_tries=3, state="failed"
    )
    payload = build_payload(ti, error=KeyError("amount"), source="callback")
    adapter = AirflowAdapter(HealerConfig())
    (result,) = adapter.normalize("task-failed", payload, {"X-Healer-Token": "secret"})
    assert result.ref == AttemptRef("airflow", "etl", RUN, "group.load", 4, 2)
    assert (result.channel, result.source, result.error_text) == (
        "task-failed",
        "callback",
        "KeyError: 'amount'",
    )
    assert dict(result.native) == {} and result.received_at is None
    assert adapter._client is None  # pure: no client, no I/O


@pytest.mark.parametrize(
    ("changes", "source", "map_index", "try_number", "error"),
    [
        ({}, "listener", -1, 1, ""),
        ({"source": "rest"}, "listener", -1, 1, ""),
        ({"source": "callback", "map_index": "3", "try_number": "2"}, "callback", 3, 2, ""),
        ({"source": None, "try_number": None, "error": None}, "listener", -1, 1, ""),
        (
            {"error": "x" * MAX_EVENT_ERROR_CHARS, "input": {"secret": 1}},
            "listener",
            -1,
            1,
            "x" * MAX_EVENT_ERROR_CHARS,
        ),
    ],
    ids=["defaults", "unknown-source", "numeric-strings", "nulls", "longest-error"],
)
def test_normalize_defaults(
    changes: dict[str, Any], source: str, map_index: int, try_number: int, error: str
) -> None:
    payload = {"dag_id": "etl", "run_id": "r", "task_id": "t", **changes}
    (result,) = AirflowAdapter(HealerConfig()).normalize("task-failed", payload, {})
    assert (result.source, result.ref.map_index, result.ref.attempt, result.error_text) == (
        source,
        map_index,
        try_number,
        error,
    )
    assert dict(result.native) == {}


@pytest.mark.parametrize(
    "payload",
    [
        {},
        {"run_id": "r", "task_id": "t"},
        {"dag_id": "", "run_id": "r", "task_id": "t"},
        {"dag_id": "  ", "run_id": "r", "task_id": "t"},
        {"dag_id": "d", "run_id": 7, "task_id": "t"},
        {"dag_id": "d" * 251, "run_id": "r", "task_id": "t"},
        {"dag_id": "d", "run_id": "r", "task_id": "t", "try_number": 0},
        {"dag_id": "d", "run_id": "r", "task_id": "t", "try_number": True},
        {"dag_id": "d", "run_id": "r", "task_id": "t", "try_number": 1.5},
        {"dag_id": "d", "run_id": "r", "task_id": "t", "try_number": "--1"},
        {"dag_id": "d", "run_id": "r", "task_id": "t", "map_index": -2},
        {"dag_id": "d", "run_id": "r", "task_id": "t", "error": 3},
        {"dag_id": "d", "run_id": "r", "task_id": "t", "error": "x" * (MAX_EVENT_ERROR_CHARS + 1)},
        {"dag_id": "d", "run_id": "r", "task_id": "t", "source": 1},
        {"dag_id": "d", "run_id": "r", "task_id": "t", "source": "s" * 33},
    ],
)
def test_normalize_rejects_malformed_payloads(payload: dict[str, Any]) -> None:
    with pytest.raises(ValueError):
        AirflowAdapter(HealerConfig()).normalize("task-failed", payload, {})


def test_normalize_rejects_other_channels_and_shapes() -> None:
    adapter = AirflowAdapter(HealerConfig())
    with pytest.raises(ValueError, match="unknown push channel 'eventbridge'"):
        adapter.normalize("eventbridge", {"dag_id": "d", "run_id": "r", "task_id": "t"}, {})
    with pytest.raises(ValueError, match="JSON object"):
        adapter.normalize("task-failed", ["dag_id"], {})  # type: ignore[arg-type]


CONFIRM_CASES = [
    ("final_failure", "failed", 1, 1, "confirmed", "state failed, try 1"),
    (
        "final_failure",
        "up_for_retry",
        1,
        1,
        "discarded",
        "the try will be retried (monitor.trigger is final_failure)",
    ),
    ("final_failure", "failed", 2, 1, "discarded", "try 1 was retried (try 2 is failed)"),
    ("final_failure", "success", 1, 1, "discarded", "the task instance is success, not failed"),
    ("final_failure", "failed", 1, 2, "discarded", "Airflow knows only 1 tries (state failed)"),
    ("every_failure", "up_for_retry", 1, 1, "confirmed", "state up_for_retry, try 1"),
    ("every_failure", "failed", 1, 1, "confirmed", "state failed, try 1"),
    ("every_failure", "success", 2, 1, "confirmed", "state success, try 2"),
    ("every_failure", "running", 2, 1, "confirmed", "state running, try 2"),
    ("final_failure", "running", 2, 1, "discarded", "try 1 was retried (try 2 is running)"),
    ("every_failure", None, 1, 1, "discarded", "the task instance is unknown, not failed"),
    ("every_failure", "failed", 1, 3, "discarded", "Airflow knows only 1 tries (state failed)"),
]
#: What the phase-5 rules say about a try that has not ended: the adapter checks such a hint again
#: later instead (:func:`test_confirm_checks_a_try_that_has_not_ended_again_later`).
UNFINISHED_CASES = [
    ("every_failure", "running", 1, 1, "discarded", "the task instance is running, not failed"),
    ("final_failure", "running", 1, 1, "discarded", "the task instance is running, not failed"),
    ("final_failure", "queued", 2, 2, "discarded", "the task instance is queued, not failed"),
]


@pytest.mark.parametrize(("trigger", "state", "ti_try", "hint_try", "verdict", "reason"), CONFIRM_CASES)
def test_confirm_applies_the_phase5_rules(
    trigger: Any, state: str | None, ti_try: int, hint_try: int, verdict: str, reason: str
) -> None:
    client = PhaseFiveClient()
    client.add("etl", RUN, "t", try_number=ti_try, state=state)
    adapter = AirflowAdapter(HealerConfig(), client=client)
    ref = AttemptRef("airflow", "etl", RUN, "t", -1, hint_try)
    result = adapter.confirm(hint(ref, source="callback", error="KeyError: 'amount'"), trigger=trigger)
    assert (result.verdict, result.reason) == (verdict, reason)
    if verdict != "confirmed":
        assert result.failures == ()
        return
    (failure,) = result.failures
    assert (
        failure.ref == ref and failure.source == "callback" and failure.error_summary == "KeyError: 'amount'"
    )
    final = state == "failed" and ti_try == hint_try
    assert failure.is_final is final
    assert failure.state is (AttemptState.FAILED if final else AttemptState.RETRYING)
    expected_at = datetime(2026, 10, 18, 8, 59, tzinfo=timezone.utc) if ti_try == hint_try else None
    assert failure.failed_at == expected_at and failure.error_name is None and dict(failure.native) == {}


@pytest.mark.parametrize(
    ("trigger", "state", "ti_try", "hint_try", "verdict", "reason"), CONFIRM_CASES + UNFINISHED_CASES
)
def test_confirm_try_matches_the_phase5_worker(
    trigger: str, state: str | None, ti_try: int, hint_try: int, verdict: str, reason: str
) -> None:
    ti = TaskInstanceInfo(dag_id="etl", run_id=RUN, task_id="t", try_number=ti_try, state=state)
    assert confirm_try(ti, hint_try, trigger) == phase5_confirm(ti, hint_try, trigger)
    assert confirm_try(ti, hint_try, trigger) == (verdict == "confirmed", reason)


@pytest.mark.parametrize("trigger", ["final_failure", "every_failure"])
@pytest.mark.parametrize(
    "state", ["running", "queued", "scheduled", "deferred", "up_for_reschedule", "restarting"]
)
def test_confirm_checks_a_try_that_has_not_ended_again_later(trigger: Any, state: str) -> None:
    client = PhaseFiveClient()
    client.add("etl", RUN, "t", try_number=2, state=state, end_date=None)
    adapter = AirflowAdapter(HealerConfig(), client=client)
    ref = AttemptRef("airflow", "etl", RUN, "t", -1, 2)
    result = adapter.confirm(hint(ref), trigger=trigger)
    assert (result.verdict, result.reason, result.state, result.failures) == (
        "retry_later",
        f"the task instance is {state}, not failed yet",
        AttemptState.RUNNING,
        (),
    )
    # the listener posts before Airflow records the end of the try: a later check confirms it
    client.add("etl", RUN, "t", try_number=2, state="failed")
    later = adapter.confirm(hint(ref), trigger=trigger)
    assert (later.verdict, later.reason, later.state) == ("confirmed", "state failed, try 2", None)


def test_confirm_errors_and_foreign_hints() -> None:
    client = PhaseFiveClient()
    client.add("etl", RUN, "t", map_index=3)
    adapter = AirflowAdapter(HealerConfig(), client=client)
    unknown = adapter.confirm(hint(AttemptRef("airflow", "etl", "nope", "t")), trigger="final_failure")
    assert (unknown.verdict, unknown.reason) == ("discarded", "Airflow does not know this task instance")
    mapped = adapter.confirm(hint(AttemptRef("airflow", "etl", RUN, "t", 3)), trigger="final_failure")
    assert mapped.verdict == "confirmed" and client.calls[-1] == ("ti", "etl", RUN, "t", 3)
    client.errors["ti"] = AirflowAPIError("Service Unavailable", status_code=503)
    later = adapter.confirm(hint(AttemptRef("airflow", "etl", RUN, "t", 3)), trigger="final_failure")
    # the phase-5 worker's text ("confirming pending event <id>: <Type>: <first line>")
    assert (later.verdict, later.reason, later.state) == (
        "retry_later",
        "AirflowAPIError: Airflow API error 503: Service Unavailable",
        None,
    )
    client.errors["ti"] = RuntimeError("boom\nsecond line")
    failing = adapter.confirm(hint(AttemptRef("airflow", "etl", RUN, "t", 3)), trigger="final_failure")
    assert (failing.verdict, failing.reason) == ("retry_later", "RuntimeError: boom")
    calls = len(client.calls)
    foreign = adapter.confirm(hint(AttemptRef("airflow-staging", "etl", RUN, "t")), trigger="final_failure")
    assert foreign.verdict == "discarded" and "airflow-staging" in foreign.reason
    for step in (UNRESOLVED_STEP, RUN_LEVEL_STEP):
        result = adapter.confirm(hint(AttemptRef("airflow", "etl", RUN, step)), trigger="every_failure")
        assert (result.verdict, result.reason) == ("discarded", "the event names no task")
    assert len(client.calls) == calls  # decided without calling Airflow
    del client.errors["ti"]
    odd_source = adapter.confirm(
        hint(AttemptRef("airflow", "etl", RUN, "t", 3), source="sqs"), trigger="final_failure"
    )
    assert [failure.source for failure in odd_source.failures] == ["listener"]  # the phase-5 mapping


# =========================================================================== RunInspector


@pytest.mark.parametrize(
    ("state", "ti_try", "attempt", "expected", "final"),
    [
        ("failed", 1, 1, AttemptState.FAILED, True),
        ("up_for_retry", 1, 1, AttemptState.RETRYING, False),
        ("success", 1, 1, AttemptState.SUCCEEDED, False),
        ("running", 2, 2, AttemptState.RUNNING, False),
        ("queued", 2, 2, AttemptState.RUNNING, False),
        ("upstream_failed", 1, 1, AttemptState.UNKNOWN, False),
        (None, 1, 1, AttemptState.UNKNOWN, False),
        ("failed", 3, 1, AttemptState.RETRYING, False),  # try 1 failed and Airflow ran the task again
    ],
)
def test_describe_maps_the_task_instance(
    state: str | None, ti_try: int, attempt: int, expected: AttemptState, final: bool
) -> None:
    client = PhaseFiveClient()
    client.add("etl", RUN, "t", try_number=ti_try, max_tries=2, state=state)
    detail = AirflowAdapter(HealerConfig(), client=client).describe(
        AttemptRef("airflow", "etl", RUN, "t", -1, attempt), {}
    )
    assert (detail.state, detail.is_final) == (expected, final) == attempt_state(client.listed[0], attempt)
    assert (detail.executor, detail.max_attempts, detail.error_name, detail.error_message) == (
        "PythonOperator",
        3,
        None,
        None,
    )
    assert (detail.traceback_text, detail.raw_error, dict(detail.native)) == ("", "", {})
    if ti_try == attempt:
        assert detail.started_at == datetime(2026, 10, 18, 8, 58, tzinfo=timezone.utc)
        assert detail.ended_at == datetime(2026, 10, 18, 8, 59, tzinfo=timezone.utc)
    else:  # the task instance shows a later try: its dates are not this try's
        assert detail.started_at is None and detail.ended_at is None


def test_describe_errors(tmp_path: Path) -> None:
    client = PhaseFiveClient()
    client.add("etl", RUN, "t")
    adapter = AirflowAdapter(HealerConfig(), client=client)
    with pytest.raises(NotFound, match="knows only 1 tries"):
        adapter.describe(AttemptRef("airflow", "etl", RUN, "t", -1, 2), {})
    with pytest.raises(NotFound) as raised:
        adapter.describe(AttemptRef("airflow", "etl", "nope", "t"), {})
    assert raised.value.status_code == 404 and not raised.value.transient
    served, server, _cfg = served_adapter(tmp_path)
    with pytest.raises(NotFound):
        served.describe(SEEDED.with_step("transform", map_index=3), {})
    assert server.api_calls[-1] == ("GET", ti_path(*SEEDED.key()[1:4]) + "/3")
    detail = served.describe(SEEDED, {})
    assert detail.ended_at == datetime(2026, 10, 17, 18, 48, 53, 147334, tzinfo=timezone.utc)
    assert (detail.max_attempts, detail.executor) == (1, "PythonOperator")


def test_logs_render_the_task_log_and_never_raise(tmp_path: Path) -> None:
    adapter, server, _cfg = served_adapter(tmp_path)
    bundle = adapter.logs(SEEDED, None)
    assert (bundle.source, bundle.confidence, bundle.truncated, bundle.notes) == (
        LOG_SOURCE,
        "exact",
        False,
        (),
    )
    assert "ValueError: unconverted data remains: T08:30:00Z" in bundle.text.splitlines()
    request = server.requests[-1]
    assert query(request) == {"full_content": ["true"], "map_index": ["-1"]}
    cut = adapter.logs(SEEDED, None, max_chars=50)
    assert cut.truncated and len(cut.text) == 50 and bundle.text.endswith(cut.text)
    assert cut.notes == ("The Airflow log was cut to its last 50 characters.",)
    missing = adapter.logs(SEEDED.next_attempt(), None)
    assert (missing.text, missing.source, missing.confidence) == ("", "none", "none")
    # the phase-5 context note, so the context can keep it word for word (masked by the core)
    assert missing.notes == (
        "Fetching the task log from Airflow failed: AirflowAPIError: Airflow API error 404: Not Found",
    )
    server.outage = "network"
    (note,) = adapter.logs(SEEDED, None).notes
    assert note.startswith(
        "Fetching the task log from Airflow failed: AirflowAPIError: Airflow API request failed"
    )


def test_a_failed_log_fetch_hands_its_whole_reason_to_the_core() -> None:
    client = PhaseFiveClient()
    client.errors["log"] = error = AirflowAPIError("x" * 2000, status_code=500)
    (note,) = AirflowAdapter(HealerConfig(), client=client).logs(SEEDED, None).notes
    # RAW and whole: the core masks the note, then cuts its reason to 500 characters (phase 5)
    assert note == f"Fetching the task log from Airflow failed: AirflowAPIError: {error}"
    assert len(note) > 2000


def test_logs_of_an_empty_or_mapped_try() -> None:
    client = PhaseFiveClient()
    client.add("etl", RUN, "t", map_index=5, try_number=2, log="")
    adapter = AirflowAdapter(HealerConfig(), client=client)
    bundle = adapter.logs(AttemptRef("airflow", "etl", RUN, "t", 5, 2), None)
    assert (bundle.text, bundle.source, bundle.notes) == (
        "",
        LOG_SOURCE,
        ("The Airflow log of try 2 is empty.",),
    )
    assert client.calls[-1] == ("log", "etl", RUN, "t", 2, 5)


def test_pipeline_source_reads_the_dag(tmp_path: Path) -> None:
    adapter, server, _cfg = served_adapter(tmp_path)
    source = adapter.pipeline_source("customers_sync")
    assert source == PipelineSource(
        path="/opt/airflow/repo/dags/customers_sync.py",
        relative_path="customers_sync.py",
        text=None,
        language="python",
        origin="api:dags",
    )
    assert server.api_calls == [("GET", "/api/v2/dags/customers_sync")]  # no dagSources call
    with_text = adapter.pipeline_source("customers_sync", include_text=True)
    assert with_text is not None and 'dag_id="customers_sync"' in (with_text.text or "")
    assert with_text.origin == "api:dagSources"
    with pytest.raises(NotFound):
        adapter.pipeline_source("missing_dag")
    server.route("GET", "/api/v2/dags/no_source", {"dag_id": "no_source", "fileloc": None})
    bare = adapter.pipeline_source("no_source", include_text=True)  # dagSources answers 404
    assert bare == PipelineSource(origin="api:dags")


def test_pipeline_source_reads_the_dag_once_per_gathering(tmp_path: Path) -> None:
    """The text asked for right after the location reuses its get_dag (the phase-5 call pattern)."""
    adapter, server, _cfg = served_adapter(tmp_path)
    dag, source = ("GET", "/api/v2/dags/customers_sync"), ("GET", "/api/v2/dagSources/customers_sync")
    located = adapter.pipeline_source("customers_sync")
    with_text = adapter.pipeline_source("customers_sync", include_text=True)
    assert server.api_calls == [dag, source]
    assert located is not None and with_text is not None and with_text.text
    assert (with_text.path, with_text.relative_path) == (located.path, located.relative_path)
    adapter.pipeline_source("customers_sync", include_text=True)  # the outcome was used up
    assert server.api_calls[2:] == [dag, source]

    # a DAG that could not be read: the text comes alone, without a second get_dag and no error
    server.route("GET", "/api/v2/dagSources/broken", {"content": "dag = DAG('broken')"})
    with pytest.raises(NotFound):
        adapter.pipeline_source("broken")
    calls = len(server.api_calls)
    recovered = adapter.pipeline_source("broken", include_text=True)
    assert recovered == PipelineSource(text="dag = DAG('broken')", origin="api:dagSources")
    assert server.api_calls[calls:] == [("GET", "/api/v2/dagSources/broken")]

    # an outcome older than PIPELINE_MEMO_SECONDS is read again
    adapter.pipeline_source("customers_sync")
    moment, outcome = adapter._dags["customers_sync"]
    adapter._dags["customers_sync"] = (moment - PIPELINE_MEMO_SECONDS - 1, outcome)
    calls = len(server.api_calls)
    adapter.pipeline_source("customers_sync", include_text=True)
    assert server.api_calls[calls:] == [dag, source]


def test_pipeline_source_remembers_a_bounded_number_of_dags() -> None:
    class Unknown(PhaseFiveClient):
        def get_dag(self, dag_id: str) -> dict[str, Any]:
            raise AirflowAPIError("DAG not found", status_code=404)

    adapter = AirflowAdapter(HealerConfig(), client=Unknown())
    for number in range(MAX_PIPELINE_MEMOS + 5):
        with pytest.raises(NotFound):
            adapter.pipeline_source(f"dag_{number}")
    assert len(adapter._dags) == MAX_PIPELINE_MEMOS and "dag_0" not in adapter._dags
    adapter.close()
    assert not adapter._dags


# =========================================================================== CodeLocator


def test_code_locator_scans_the_configured_dag_folder(tmp_path: Path) -> None:
    repo = copy_repo(tmp_path / "repo")
    locator = AirflowCodeLocator(config_from_mapping({"repo": {"path": str(repo)}}))
    assert [dag.dag_id for dag in locator.scan(repo)] == [
        "customers_sync",
        "inventory_report",
        "sales_daily_etl",
    ]
    (repo / "dags").rename(repo / "pipelines")
    moved = config_from_mapping({"repo": {"path": str(repo), "dags_dir": "pipelines"}})
    assert len(AirflowCodeLocator(moved).scan(repo)) == 3
    assert AirflowCodeLocator(HealerConfig(), dags_dir=".\\pipelines\\").dags_dir == "pipelines"
    assert len(AirflowCodeLocator(HealerConfig(), dags_dir="./pipelines/").scan(repo)) == 3
    assert AirflowCodeLocator(HealerConfig(), dags_dir="").dags_dir == "dags"


def test_path_mappings_are_the_phase5_roots(healer_env: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    cfg = config_from_mapping({"repo": {"path": str(EXAMPLE_REPO), "dags_dir": "dags"}})
    locator = AirflowCodeLocator(cfg)
    assert locator.path_mappings(cfg.repo_root, None) == [
        PathMapping(CONTAINER_REPO_ROOT, ""),
        PathMapping(DEFAULT_AIRFLOW_DAGS_FOLDER, "dags"),
    ]
    monkeypatch.setenv("AIRFLOW__CORE__DAGS_FOLDER", " /srv/airflow/dags/ ")
    assert locator.path_mappings(cfg.repo_root, None)[1] == PathMapping("/srv/airflow/dags", "dags")
    monkeypatch.setenv(
        "AIRFLOW__CORE__DAGS_FOLDER", "/opt/airflow/repo"
    )  # the first mapping of a prefix wins
    assert locator.path_mappings(cfg.repo_root, None) == [
        PathMapping(CONTAINER_REPO_ROOT, ""),
        PathMapping(DEFAULT_AIRFLOW_DAGS_FOLDER, "dags"),
    ]


@pytest.mark.parametrize("dags_folder", [None, "/srv/airflow/dags"])
def test_path_mappings_map_frames_like_the_phase5_context(
    dags_folder: str | None, healer_env: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    from dag_healer.agent import context

    if dags_folder:
        monkeypatch.setenv("AIRFLOW__CORE__DAGS_FOLDER", dags_folder)
    cfg = config_from_mapping({"repo": {"path": str(EXAMPLE_REPO)}})
    root = cfg.repo_root
    mappings = [PathMapping(root.as_posix(), ""), *AirflowCodeLocator(cfg).path_mappings(root, None)]
    # the phase-5 roots, still the default of map_to_repo_path (mappings=None)
    assert sorted((m.runtime_prefix, m.repo_base) for m in mappings) == sorted(context._path_roots(cfg))
    frames = [
        "/opt/airflow/repo/src/etl/customers.py",
        "/opt/airflow/repo/dags/../src/etl/inventory.py",
        "/opt/airflow/dags/customers_sync.py",
        f"{root.as_posix()}/src/etl/sales.py",
        "/home/airflow/.local/lib/python3.13/site-packages/pandas/core/series.py",
        "/usr/python/lib/python3.13/_strptime.py",
    ]
    if dags_folder:  # without the variable the context still finds it by its tail (not a prefix mapping)
        frames.append(f"{dags_folder}/sales_daily_etl.py")
    for frame in frames:
        assert map_frame(frame, mappings) == context.map_to_repo_path(frame, cfg), frame


def test_pipeline_file_follows_the_phase5_order(tmp_path: Path) -> None:
    repo = copy_repo(tmp_path / "repo")
    cfg = config_from_mapping({"repo": {"path": str(repo)}})
    locator = AirflowCodeLocator(cfg)

    def source(path: str | None, relative: str | None = None) -> PipelineSource:
        return PipelineSource(path=path, relative_path=relative)

    root = cfg.repo_root
    assert locator.pipeline_file(
        root, "customers_sync", source("/opt/airflow/repo/dags/customers_sync.py")
    ) == ("dags/customers_sync.py")
    assert (
        locator.pipeline_file(root, "x", source("/elsewhere/x.py", "customers_sync.py"))
        == "dags/customers_sync.py"
    )
    bundle = source(
        "/tmp/airflow/dag_bundles/git/tracking/dags/sales_daily_etl.py"
    )  # a DAG bundle cloned elsewhere
    assert locator.pipeline_file(root, "sales_daily_etl", bundle) == "dags/sales_daily_etl.py"
    assert locator.pipeline_file(root, "inventory_report", source(None)) == "dags/inventory_report.py"
    assert locator.pipeline_file(root, "customers_sync", None) == "dags/customers_sync.py"
    gone = source("/opt/airflow/repo/dags/gone.py", "../../outside.py")
    assert locator.reported_file(root, gone) is None
    assert locator.pipeline_file(root, "customers_sync", gone) == "dags/customers_sync.py"
    assert locator.pipeline_file(root, "unknown_dag", gone) is None
    # another checkout (a repair clone): paths under it map against it, not against repo.path
    clone = copy_repo(tmp_path / "clone")
    (repo / "dags" / "customers_sync.py").unlink()
    in_clone = source(f"{clone.resolve().as_posix()}/dags/customers_sync.py")
    assert locator.reported_file(root, in_clone) is None
    assert locator.reported_file(clone, in_clone) == "dags/customers_sync.py"


def test_reported_file_matches_the_phase5_context(tmp_path: Path) -> None:
    repo = copy_repo(tmp_path / "repo")
    cfg = config_from_mapping({"repo": {"path": str(repo)}})
    locator = AirflowCodeLocator(cfg)
    dags = [
        load("airflow_api/dag_customers_sync.json"),
        {"fileloc": "/opt/airflow/dags/customers_sync.py"},
        {"fileloc": "/elsewhere/bundle/dags/inventory_report.py"},
        {"fileloc": "/opt/airflow/repo/dags/gone.py", "relative_fileloc": "sales_daily_etl.py"},
        {"fileloc": "/opt/airflow/repo/dags/gone.py"},
        {"relative_fileloc": "../escape.py"},
        {},
    ]
    for dag in dags:
        reported = PipelineSource(path=dag.get("fileloc"), relative_path=dag.get("relative_fileloc"))
        assert locator.reported_file(cfg.repo_root, reported) == phase5_api_dag_file(cfg, dag), dag


# =========================================================================== Remediator


def test_retry_step_clears_the_task_instance_and_its_failed_downstream_tasks(tmp_path: Path) -> None:
    adapter, server, _cfg = served_adapter(tmp_path)
    path = "/api/v2/dags/customers_sync/clearTaskInstances"
    assert adapter.available(SEEDED, {}) == (RemediationKind.RETRY_STEP,)
    result = adapter.remediate(SEEDED, {}, RemediationKind.RETRY_STEP, idempotency_key="k1", actor="ann")
    # the recorded answer lists transform only (nothing after it needed clearing)
    assert result.detail == f"ann cleared transform in run {RUN}; Airflow will run it again."
    assert (result.kind, result.accepted, result.new_ref, result.warnings) == (
        RemediationKind.RETRY_STEP,
        True,
        None,
        (),
    )
    assert dict(result.native) == {"cleared": 1}
    assert server.bodies("POST", path) == [
        {
            "dry_run": False,
            "dag_run_id": RUN,
            "task_ids": ["transform"],
            "only_failed": True,  # failed and upstream_failed only: tasks that succeeded do not run again
            "include_downstream": True,
            "reset_dag_runs": True,
        }
    ]
    again = adapter.remediate(SEEDED, {}, RemediationKind.RETRY_STEP, idempotency_key="k1", actor="bob")
    assert again is result and len(server.bodies("POST", path)) == 1
    # load was upstream_failed after transform failed: Airflow clears it too and lists both
    server.route("POST", path, load("airflow_api/clear_task_instances_downstream.json"))
    both = adapter.remediate(SEEDED, {}, RemediationKind.RETRY_STEP, idempotency_key="k2", actor="ann")
    assert both.detail == (
        f"ann cleared transform and 1 downstream task (load) in run {RUN}; Airflow will run them again."
    )
    assert dict(both.native) == {"cleared": 2}
    mapped = SEEDED.with_step("transform", map_index=2)
    adapter.remediate(mapped, {}, RemediationKind.RETRY_STEP, idempotency_key="k3", actor="ann")
    assert server.bodies("POST", path)[-1]["task_ids"] == [["transform", 2]]
    assert server.bodies("POST", path)[-1]["include_downstream"] is True
    for kind in (RemediationKind.RESUME_RUN, RemediationKind.RERUN_PIPELINE):
        with pytest.raises(NotSupported):
            adapter.remediate(SEEDED, {}, kind, idempotency_key=f"k-{kind.value}", actor="ann")
    for step in (UNRESOLVED_STEP, RUN_LEVEL_STEP):
        assert adapter.available(SEEDED.with_step(step), {}) == ()


class AnsweringClient(PhaseFiveClient):
    """Answers ``clearTaskInstances`` with a fixed body (what Airflow says it cleared)."""

    def __init__(self, answer: Any) -> None:
        super().__init__()
        self.answer = answer

    def clear_task_instances(self, *args: Any, **kwargs: Any) -> Any:
        super().clear_task_instances(*args, **kwargs)
        return self.answer


MAPPED = SEEDED.with_step("transform", map_index=2)
#: twelve downstream tasks, listed in no particular order (Airflow does not sort its answer)
MANY_DOWNSTREAM = [
    {"task_id": f"step_{n:02d}", "map_index": -1} for n in (7, 12, 1, 3, 10, 2, 11, 5, 9, 4, 8, 6)
]


@pytest.mark.parametrize(
    ("ref", "answer", "detail"),
    [
        pytest.param(
            MAPPED,
            {
                "task_instances": [
                    {"task_id": "load", "map_index": 2},
                    {"task_id": "transform", "map_index": 2},
                ]
            },
            f"ann cleared transform[2] and 1 downstream task (load[2]) in run {RUN}; "
            "Airflow will run them again.",
            id="mapped",
        ),
        pytest.param(
            SEEDED,
            {"task_instances": [*MANY_DOWNSTREAM, {"task_id": "transform"}, MANY_DOWNSTREAM[0]]},
            "ann cleared transform and 12 downstream tasks (step_01, step_02, step_03, step_04, step_05, "
            f"step_06, step_07, step_08, step_09, step_10 and 2 more) in run {RUN}; "
            "Airflow will run them again.",
            id="many-downstream",
        ),
        pytest.param(
            SEEDED,
            {"task_instances": [{"task_id": "load", "map_index": -1}], "total_entries": 1},
            f"ann cleared 1 downstream task (load) in run {RUN}; Airflow will run it again. "
            "transform itself was not cleared because it is not failed.",
            id="failed-task-already-rerun",
        ),
        pytest.param(
            SEEDED,
            {"task_instances": [], "total_entries": 0},
            f"ann asked Airflow to clear transform in run {RUN}, but nothing was cleared: "
            "neither transform nor a task after it is failed or upstream_failed.",
            id="nothing-cleared",
        ),
        pytest.param(
            SEEDED,
            {"total_entries": 3},
            f"ann cleared transform and any failed downstream tasks in run {RUN} (3 task instance(s)); "
            "Airflow will run them again.",
            id="count-only",
        ),
        pytest.param(
            SEEDED,
            ["not", "a", "mapping"],
            f"ann cleared transform and any failed downstream tasks in run {RUN}; "
            "Airflow will run them again.",
            id="unreadable",
        ),
    ],
)
def test_retry_step_names_what_airflow_cleared(ref: AttemptRef, answer: Any, detail: str) -> None:
    client = AnsweringClient(answer)
    adapter = AirflowAdapter(HealerConfig(), client=client)
    result = adapter.remediate(ref, {}, RemediationKind.RETRY_STEP, idempotency_key="k", actor="ann")
    assert result.detail == detail
    assert result.accepted is True
    expected_ids: list[Any] = ["transform"] if ref.map_index < 0 else [("transform", ref.map_index)]
    assert client.calls == [("clear", "customers_sync", RUN, expected_ids, True, True)]


def test_cleared_task_instances_reads_the_answer() -> None:
    answer = {
        "task_instances": [
            {"task_id": "load", "map_index": 1},
            {"task_id": "extract"},
            {"task_id": "load", "map_index": 1},
            {"task_id": "load", "map_index": "0"},
            {"map_index": 3},
            {"task_id": ""},
            "transform",
        ]
    }
    assert cleared_task_instances(answer) == [("extract", -1), ("load", 0), ("load", 1)]
    assert cleared_task_instances({"task_instances": []}) == []
    assert cleared_task_instances({"total_entries": 2}) is None
    assert cleared_task_instances({"task_instances": None}) is None


def test_retry_step_errors_are_not_remembered() -> None:
    client = AnsweringClient({})
    adapter = AirflowAdapter(HealerConfig(), client=client)
    client.errors["clear"] = AirflowAPIError("Forbidden", status_code=403)
    with pytest.raises(PermissionDenied):
        adapter.remediate(SEEDED, {}, RemediationKind.RETRY_STEP, idempotency_key="k", actor="ann")
    del client.errors["clear"]
    result = adapter.remediate(SEEDED, {}, RemediationKind.RETRY_STEP, idempotency_key="k", actor="ann")
    assert result.detail == (
        f"ann cleared transform and any failed downstream tasks in run {RUN}; Airflow will run them again."
    )
    assert dict(result.native) == {"cleared": None}
    assert [call[0] for call in client.calls] == ["clear", "clear"]
    assert cleared_count({"total_entries": 3}) == 3 and cleared_count({"total_entries": True}) is None


def test_rerun_pipeline_is_opt_in(tmp_path: Path) -> None:
    adapter, server, _cfg = served_adapter(tmp_path, allow_rerun=True)
    assert adapter.capabilities.remediations == (RemediationKind.RETRY_STEP, RemediationKind.RERUN_PIPELINE)
    assert AirflowAdapter.capabilities is AIRFLOW_CAPABILITIES  # the class default is untouched
    assert adapter.available(SEEDED, {}) == (RemediationKind.RETRY_STEP, RemediationKind.RERUN_PIPELINE)
    run_path = f"/api/v2/dags/customers_sync/dagRuns/{QUOTED_RUN}"
    server.route(
        "GET", run_path, {**load("airflow_api/dag_run_customers_sync.json"), "conf": {"day": "2026-10-17"}}
    )
    result = adapter.remediate(SEEDED, {}, RemediationKind.RERUN_PIPELINE, idempotency_key="r1", actor="ann")
    assert server.bodies("POST", "/api/v2/dags/customers_sync/dagRuns") == [
        {"logical_date": None, "conf": {"day": "2026-10-17"}}
    ]
    assert result.new_ref == AttemptRef("airflow", "customers_sync", RUN, "transform", -1, 1)
    assert (
        result.detail == f"ann triggered run {RUN} of DAG customers_sync with the configuration of run {RUN}."
    )
    assert "2026-10-17" not in json.dumps(dict(result.native))  # the conf goes to Airflow only


# =========================================================================== links, health, doctor, sandbox


def test_links_point_at_the_task_instance_page(healer_env: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    links = AirflowAdapter(config_from_mapping({"airflow": {"api_url": BASE}})).links
    assert links.links(SEEDED, {}) == {
        "Airflow task": f"{BASE}/dags/customers_sync/runs/{QUOTED_RUN}/tasks/transform"
    }
    odd = AttemptRef("airflow", "my dag", "run/1 +x:y", "group.task", 3)
    assert links.links(odd, {}) == {
        "Airflow task": f"{BASE}/dags/my%20dag/runs/run%2F1%20%2Bx%3Ay/tasks/group.task/mapped/3"
    }
    assert links.links(SEEDED.with_step(RUN_LEVEL_STEP), {}) == {
        "Airflow DAG run": f"{BASE}/dags/customers_sync/runs/{QUOTED_RUN}"
    }
    monkeypatch.setenv("AIRFLOW__API__BASE_URL", "https://airflow.example.com/")
    assert links.links(SEEDED, {})["Airflow task"].startswith(
        "https://airflow.example.com/dags/customers_sync/"
    )


def test_health_checks_the_api_and_the_login(tmp_path: Path) -> None:
    adapter, server, cfg = served_adapter(tmp_path)
    report = adapter.health()
    assert report.ok and report.checks == {
        "api": "ok",
        "metadatabase": "healthy",
        "scheduler": "healthy",
        "triggerer": "healthy",
        "dag_processor": "healthy",
        "auth": "ok",
    }
    assert report.detail.startswith(f"The Airflow API at {BASE} answers: metadatabase=healthy")
    degraded = load("airflow_api/health.json")
    degraded["scheduler"]["status"] = "unhealthy"
    server.route("GET", "/api/v2/monitor/health", degraded)
    assert adapter.health().ok and "(not healthy: scheduler)" in adapter.health().detail
    server.password = "rotated"
    wrong = adapter.health()
    assert not wrong.ok and wrong.checks["auth"] == "error" and "the login failed" in wrong.detail
    server.outage = "server_error"
    down = adapter.health()
    assert not down.ok and down.checks == {"api": "error"} and "is not reachable" in down.detail
    static = AirflowAdapter(
        cfg, client=AirflowClient(BASE, token="t", transport=httpx.MockTransport(conformance_server()))
    )
    assert "auth" not in static.health().checks
    assert not AirflowAdapter(cfg, client=PhaseFiveClient()).health().ok  # the double has no health()


def test_doctor_checks_reuse_the_doctor_functions(monkeypatch: pytest.MonkeyPatch) -> None:
    from dag_healer.cli import doctor_cmd

    gets: list[str] = []

    class Response:
        def __init__(self, status_code: int, body: Any) -> None:
            self.status_code, self._body = status_code, body

        def json(self) -> Any:
            return self._body

    def http_get(url: str) -> Response:
        gets.append(url)
        return Response(200, {"metadatabase": {"status": "healthy"}})

    monkeypatch.setattr(doctor_cmd, "_http_get", http_get)
    monkeypatch.setattr(doctor_cmd, "_http_post", lambda url, payload: Response(201, {"access_token": "t"}))
    cfg = config_from_mapping({"airflow": {"api_url": BASE, "username": "admin", "password": "admin"}})
    adapter = AirflowAdapter(cfg)
    checks = adapter.doctor_checks()
    assert [check.name for check in checks] == ["airflow api", "airflow auth", "airflow plugin"]
    assert gets == [] and adapter._client is None  # building the checks does nothing
    api, auth, plugin = (check.run() for check in checks)
    assert api == doctor_cmd.check_airflow_api(cfg)
    assert auth == doctor_cmd.check_airflow_auth(cfg, True) and auth.status == doctor_cmd.OK
    assert plugin == doctor_cmd.check_plugin_entry_point()
    assert len(gets) == 2  # one for the api check, one for the comparison: the auth check reused the first
    (_api, alone, _plugin) = adapter.doctor_checks()
    assert alone.run().status == doctor_cmd.OK and len(gets) == 3


def test_sandbox_env_matches_the_pytest_sandbox(tmp_path: Path) -> None:
    from dag_healer.repo.pytest_runner import scrubbed_env

    env = scrubbed_env({}, airflow_home=tmp_path / "airflow_home")
    assert {name: env.get(name) for name in SANDBOX_ENV} == SANDBOX_ENV


def test_phase5_constants_are_unchanged() -> None:
    from dag_healer.agent import context
    from dag_healer.api import schemas
    from dag_healer.api.routers import events
    from dag_healer.worker import pipeline

    assert (context.CONTAINER_REPO_ROOT, context.DEFAULT_AIRFLOW_DAGS_FOLDER) == (
        CONTAINER_REPO_ROOT,
        DEFAULT_AIRFLOW_DAGS_FOLDER,
    )
    assert pipeline.EVENT_SOURCES == events.EVENT_SOURCES == EVENT_SOURCES
    error_field = schemas.TaskFailedEvent.model_fields["error"]
    assert any(getattr(item, "max_length", None) == MAX_EVENT_ERROR_CHARS for item in error_field.metadata)


# =========================================================================== errors


@pytest.mark.parametrize(
    ("status", "kind", "transient"),
    [
        (None, OrchestratorError, True),
        (429, OrchestratorError, True),
        (500, OrchestratorError, True),
        (503, OrchestratorError, True),
        (400, OrchestratorError, False),
        (409, OrchestratorError, False),
        (422, OrchestratorError, False),
        (404, NotFound, False),
        (401, PermissionDenied, False),
        (403, PermissionDenied, False),
    ],
)
def test_translate_airflow_api_errors(status: int | None, kind: type, transient: bool) -> None:
    original = AirflowAPIError("detail text", status_code=status)
    error = translate_error(original)
    assert type(error) is kind and (error.transient, error.status_code) == (transient, status)
    assert error.detail == str(original) and is_transient_status(status) is (status in (None, 429, 500, 503))


def test_translate_other_errors() -> None:
    request = httpx.Request("GET", f"{BASE}/api/v2/dags")
    connect = translate_error(httpx.ConnectError("connection refused", request=request))
    assert (type(connect), connect.transient, connect.status_code) == (OrchestratorError, True, None)
    assert connect.detail == "ConnectError: connection refused"
    response = httpx.Response(502, request=request)
    status_error = httpx.HTTPStatusError("bad gateway", request=request, response=response)
    assert (status_code_of(status_error), translate_error(status_error).transient) == (502, True)
    assert translate_error(RuntimeError()).detail == "RuntimeError"
    same = NotSupported("no")
    assert translate_error(same) is same
    assert status_code_of(SimpleNamespace(status_code=True)) is None  # type: ignore[arg-type]
    assert error_text(KeyError("amount")) == "KeyError: 'amount'" and error_text(ValueError()) == "ValueError"
