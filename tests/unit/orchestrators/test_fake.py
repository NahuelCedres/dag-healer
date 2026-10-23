"""The packaged conformance suite against the fake orchestrator, then Fakeflow's own behaviour.

The conformance harness builds a tiny repository whose step code runs from a "deployed" copy
(so the traceback paths need the world's path mapping to reach the checkout), raises a real
``ValueError`` from that code in-process (the message carries an e-mail address: check 12) and
seeds it as a terminal failure plus a retrying one. The end-to-end scenario plays the demo on a
git copy of ``example_existing_repo`` (``init``, approved sales/customers contracts, day-2
files): the three DAGs run in-process and fail for real, and Fakeflow reports those failures.
"""

from __future__ import annotations

import importlib
import importlib.util
import shutil
import sys
import uuid
from collections.abc import Callable
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import pytest

from dag_healer.config import HealerConfig, config_from_mapping, load_config
from dag_healer.core.errors import NotFound, NotSupported, OrchestratorError
from dag_healer.core.failures import AttemptState, FailureHint
from dag_healer.core.ids import RUN_LEVEL_STEP, UNRESOLVED_STEP, AttemptRef
from dag_healer.core.ports import OrchestratorAdapter
from dag_healer.core.specs import DatasetRef, PathMapping, TaskInfo
from dag_healer.core.vocab import RemediationKind
from dag_healer.orchestrators.conformance import (
    EndToEndCase,
    EndToEndSetup,
    OrchestratorConformance,
    SeededFailure,
)
from dag_healer.orchestrators.fake import (
    FACTORY,
    FAKE_CAPABILITIES,
    FAKE_VOCABULARY,
    MAX_HINT_ERROR_CHARS,
    FakeAdapter,
    FakeOptions,
    FakeWorld,
)
from dag_healer.orchestrators.registry import AdapterOptionsError

MakeConfig = Callable[..., HealerConfig]  # the make_config fixture of conftest.py

ROOT = Path(__file__).resolve().parents[3]
EXAMPLE_REPO = ROOT / "example_existing_repo"
T0 = datetime(2026, 10, 18, 9, 0, tzinfo=timezone.utc)
RUN = "scheduled__2026-10-18T09:00:00+00:00"
RETRY_RUN = "scheduled__2026-10-19T09:00:00+00:00"
E2E_RUN = "manual__2026-10-18T09:00:00+00:00"
PII_EMAIL = "alice.pii@example.com"
GIT_IDENTITY = {
    "GIT_AUTHOR_NAME": "Test",
    "GIT_AUTHOR_EMAIL": "test@example.invalid",
    "GIT_COMMITTER_NAME": "Test",
    "GIT_COMMITTER_EMAIL": "test@example.invalid",
}

ORDERS_MODULE = '''\
"""Orders ETL (dag-healer conformance fixture)."""


def parse_amount(row):
    value = row["amount"]
    try:
        return float(value)
    except ValueError:
        raise ValueError(f"order of {row['email']}: amount {value!r} is not a number") from None


def transform(rows):
    return [parse_amount(row) for row in rows]
'''
FLOW_FILE = '''\
"""Fakeflow flow definition (dag-healer conformance fixture)."""
from etl.orders import transform

STEPS = {"transform": transform}
'''
TRANSFORM = TaskInfo("transform", "python", "transform", "etl.orders", "etl/orders.py", 12)

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
#: dag_id -> (category, final status) of the demo.
E2E_EXPECTED: dict[str, tuple[str, str]] = {
    "sales_daily_etl": ("data", "reported"),
    "customers_sync": ("code", "repaired"),
    "inventory_report": ("ambiguous", "waiting_human"),
}


# =========================================================================== helpers


def write_orders_repo(root: Path) -> Path:
    (root / "etl").mkdir(parents=True)
    (root / "flows").mkdir()
    (root / "etl" / "__init__.py").write_text("", encoding="utf-8")
    (root / "etl" / "orders.py").write_text(ORDERS_MODULE, encoding="utf-8")
    (root / "flows" / "orders_flow.py").write_text(FLOW_FILE, encoding="utf-8")
    return root


def raise_in_process(path: Path, function: str, *args: Any) -> BaseException:
    """Load ``path`` under a unique module name, call ``function`` and return what it raised."""
    spec = importlib.util.spec_from_file_location(f"_fakeflow_fixture_{uuid.uuid4().hex}", path)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    try:
        getattr(module, function)(*args)
    except Exception as exc:  # the real failure of the fixture code
        return exc
    raise AssertionError(f"{function} did not fail")


def orders_world(tmp_path: Path) -> tuple[Path, FakeWorld, AttemptRef, AttemptRef]:
    """The conformance world: the repo, the world, a terminal failure and a retrying one."""
    repo = write_orders_repo(tmp_path / "repo")
    deployed = tmp_path / "deployed"
    shutil.copytree(repo, deployed)
    exc = raise_in_process(
        deployed / "etl" / "orders.py", "transform", [{"amount": "n/a", "email": PII_EMAIL}]
    )
    world = FakeWorld(clock=lambda: T0)
    world.add_pipeline(
        "orders_flow", file="flows/orders_flow.py", steps={"transform": TRANSFORM}, source=FLOW_FILE
    )
    world.add_path_mapping(deployed.as_posix())
    seeded = world.fail("orders_flow", RUN, "transform", exc=exc, max_attempts=1)
    retrying = world.fail("orders_flow", RETRY_RUN, "transform", exc=exc, final=False, max_attempts=2)
    return repo, world, seeded, retrying


def _purge_src_modules() -> dict[str, Any]:
    removed = {
        name: module for name, module in sys.modules.items() if name == "src" or name.startswith("src.")
    }
    for name in removed:
        del sys.modules[name]
    return removed


def run_dag_in_process(repo: Path, dag_id: str) -> tuple[str, BaseException]:
    """Run the DAG's task callables in order on the repo copy; the failing task and its exception."""
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


def prepare_example_repo(repo: Path) -> None:
    """A git copy of example_existing_repo prepared like the demo (init, approvals, day-2 files)."""
    from dag_healer.cli.init_cmd import run_init
    from dag_healer.contracts.io import approve_contract, contract_path, load_contract, save_contract
    from dag_healer.repo.git import run_git

    shutil.copytree(EXAMPLE_REPO, repo, ignore=shutil.ignore_patterns("__pycache__", "processed", ".git"))
    run_git(["init", "--quiet", "-b", "main"], repo)
    run_git(["add", "-A"], repo)
    run_git(["-c", "commit.gpgsign=false", "commit", "--quiet", "-m", "day 1"], repo, env=GIT_IDENTITY)
    assert run_init(repo).ok
    for dag_id in ("sales_daily_etl", "customers_sync"):
        path = contract_path(repo / "healer" / "contracts", dag_id)
        save_contract(
            approve_contract(load_contract(path), "data-lead").model_copy(update={"approved_at": T0}), path
        )
    for name in ("sales", "customers", "inventory"):
        shutil.copyfile(
            repo / "fixtures" / "day2" / f"{name}.csv", repo / "data" / "incoming" / f"{name}.csv"
        )


# =========================================================================== the harness


class FakeHarness:
    """:class:`~dag_healer.orchestrators.conformance.ConformanceHarness` of Fakeflow."""

    def __init__(self) -> None:
        self.world: FakeWorld | None = None
        self.seeded: SeededFailure | None = None
        self.retrying: SeededFailure | None = None

    def build(self, tmp_path: Path) -> tuple[HealerConfig, OrchestratorAdapter]:
        repo, world, seeded, retrying = orders_world(tmp_path)
        cfg = config_from_mapping({"repo": {"path": str(repo)}})
        adapter = FACTORY.create(cfg, client=world)
        self.world = world
        self.seeded = SeededFailure(seeded, True, "ValueError", "etl/orders.py", "etl/orders.py")
        self.retrying = SeededFailure(retrying, False, "ValueError", "etl/orders.py", "etl/orders.py")
        return cfg, adapter

    def seeded_failure(self) -> SeededFailure:
        assert self.seeded is not None
        return self.seeded

    def seeded_retrying(self) -> SeededFailure | None:
        return self.retrying

    def forged_hint(self) -> FailureHint | None:
        ref = AttemptRef("fake", "orders_flow", "forged__2026-10-18", "transform")
        return FailureHint(ref=ref, channel="fake", source="push", error_text="ValueError: forged")

    def push_samples(self) -> list[tuple[str, dict, dict]]:
        error = f"ValueError: order of {PII_EMAIL}: amount 'n/a' is not a number"
        step_hint = {
            "pipeline_id": "orders_flow",
            "run_id": RUN,
            "step_id": "transform",
            "attempt": 1,
            "error": error,
            "input": {"rows": [{"email": PII_EMAIL, "amount": "n/a"}]},
            "output": {"note": "partial output of the failed step"},
            "parameters": {"api_token": "not-a-real-token"},
        }
        run_hint = {"pipeline_id": "orders_flow", "run_id": RUN, "event_id": "fakeflow-event-1"}
        return [("fake", step_hint, {}), ("fake", run_hint, {"X-Fakeflow-Delivery": "1"})]

    def io_calls(self) -> int:
        assert self.world is not None
        return self.world.io_calls

    def simulate_outage(self, kind: str) -> None:
        assert self.world is not None
        self.world.outage = kind  # type: ignore[assignment]

    def pii_strings(self) -> list[str]:
        return [PII_EMAIL]

    def end_to_end(self, tmp_path: Path) -> EndToEndSetup:
        from dag_healer.repo.dagscan import scan_dags

        repo = tmp_path / "e2e_repo"
        prepare_example_repo(repo)
        cfg = load_config(repo / "healer.yaml")
        world = FakeWorld(clock=lambda: T0)
        for dag in scan_dags(repo, cfg.repo.dags_dir):
            world.add_pipeline(
                dag.dag_id,
                file=dag.file,
                steps={task.task_id: task for task in dag.tasks},
                datasets=dag.datasets,
            )
        cases = []
        for dag_id, (category, status) in E2E_EXPECTED.items():
            task_id, exc = run_dag_in_process(repo, dag_id)
            cases.append(EndToEndCase(world.fail(dag_id, E2E_RUN, task_id, exc=exc), category, status))  # type: ignore[arg-type]
        return EndToEndSetup(cfg=cfg, adapter=FakeAdapter(cfg, world=world), cases=tuple(cases))


class TestFakeConformance(OrchestratorConformance):
    """The packaged suite against Fakeflow (check 14 runs its setup, then waits for step 6)."""

    @pytest.fixture
    def harness(self, healer_env: Path) -> FakeHarness:
        return FakeHarness()


# =========================================================================== Fakeflow itself


@pytest.fixture
def orders(tmp_path: Path) -> tuple[Path, FakeWorld, AttemptRef, AttemptRef, FakeAdapter]:
    repo, world, seeded, retrying = orders_world(tmp_path)
    return repo, world, seeded, retrying, FakeAdapter(world=world)


def _hint(adapter: FakeAdapter, ref: AttemptRef) -> FailureHint:
    return FailureHint(ref=ref, channel="fake", source="push")


def test_vocabulary_capabilities_and_static_attributes() -> None:
    adapter = FakeAdapter()
    assert adapter.vocabulary is FAKE_VOCABULARY
    assert FAKE_VOCABULARY.product == "Fakeflow" and FAKE_VOCABULARY.retry_action == "Retry step"
    assert (FAKE_VOCABULARY.pipeline, FAKE_VOCABULARY.run, FAKE_VOCABULARY.step) == ("flow", "run", "step")
    assert not [word for word in FAKE_VOCABULARY.as_dict().values() if "Airflow" in word or "DAG" in word]
    assert FAKE_CAPABILITIES.push_channels == ("fake",) and len(FAKE_CAPABILITIES.remediations) == 3
    assert all(port is adapter for port in (adapter.feed, adapter.push, adapter.queue, adapter.remediator))
    assert adapter.links is not adapter and adapter.code is not adapter
    assert (adapter.type, adapter.instance, adapter.display_name) == ("fake", "fake", "Fakeflow")
    assert adapter.sandbox_env() == {"FAKEFLOW_TOKEN": None}


def test_fail_validates_the_world(tmp_path: Path) -> None:
    world = FakeWorld()
    with pytest.raises(ValueError, match="add_pipeline"):
        world.fail("nope", "r", "s")
    world.add_pipeline("flow", file="flows/flow.py", steps={"s": TaskInfo("s", "python")})
    with pytest.raises(ValueError, match="no step"):
        world.fail("flow", "r", "other")
    with pytest.raises(ValueError, match="attempt"):
        world.fail("flow", "r", "s", attempt=0)
    with pytest.raises(ValueError, match="map_index"):
        world.fail("flow", "r", "s", map_index=-2)
    with pytest.raises(ValueError, match="run_id"):
        world.fail("flow", "", "s")
    world.fail("flow", "r", "s")
    with pytest.raises(ValueError, match="already exists"):
        world.fail("flow", "r", "s")
    assert world.fail("flow", "r", RUN_LEVEL_STEP).step_id == RUN_LEVEL_STEP
    with pytest.raises(ValueError, match="task_id"):
        world.add_pipeline("bad", file="x.py", steps={"a": TaskInfo("b", "python")})


def test_fail_with_text_parses_the_exception_line() -> None:
    world = FakeWorld()
    world.add_pipeline("flow", file="flows/flow.py", steps={"s": TaskInfo("s", "python")})
    text = (
        "[2026-10-18T09:00:00+00:00] INFO - starting\n"
        "Traceback (most recent call last):\n"
        '  File "/opt/fakeflow/repo/etl/x.py", line 3, in f\n'
        "    return row['qty']\n"
        "KeyError: 'qty'\n"
    )
    ref = world.fail("flow", "r1", "s", text=text)
    attempt = world.attempt(ref)
    assert attempt is not None
    assert (attempt.error_name, attempt.error_message) == ("KeyError", "'qty'")
    assert attempt.traceback_text.startswith("Traceback (most recent call last):")
    assert attempt.error_text == text.rstrip("\n")
    plain = world.attempt(world.fail("flow", "r2", "s", text="worker lost", error_name="FakeflowWorkerLost"))
    assert plain is not None and (plain.error_name, plain.error_message, plain.traceback_text) == (
        "FakeflowWorkerLost",
        None,
        "",
    )


def test_poll_pages_with_a_cursor_and_filters_unmonitored_flows(
    orders: tuple[Path, FakeWorld, AttemptRef, AttemptRef, FakeAdapter],
) -> None:
    _repo, world, seeded, retrying, adapter = orders
    world.add_pipeline("other", file="flows/other.py", steps={"s": TaskInfo("s", "python")})
    other = world.fail("other", "r", "s", text="boom")
    page = adapter.poll(cursor=None, is_monitored=lambda _p: True, limit=2)
    assert [f.ref for f in page.failures] == [seeded, retrying] and not page.complete
    assert [f.is_final for f in page.failures] == [True, False]
    assert {f.source for f in page.failures} == {"poll"}
    rest = adapter.poll(cursor=page.cursor, is_monitored=lambda _p: True, limit=2)
    assert [f.ref for f in rest.failures] == [other] and rest.complete
    assert adapter.poll(cursor=rest.cursor, is_monitored=lambda _p: True).failures == ()
    assert adapter.poll(cursor={"seq": "garbage"}, is_monitored=lambda _p: True).failures[0].ref == seeded
    monitored = adapter.poll(cursor=None, is_monitored=lambda pipeline: pipeline == "other")
    assert [f.ref for f in monitored.failures] == [other]
    world.set_state(other, AttemptState.SUCCEEDED)
    assert other not in [f.ref for f in adapter.poll(cursor=None, is_monitored=lambda _p: True).failures]


def test_confirm_applies_the_phase5_rules(
    orders: tuple[Path, FakeWorld, AttemptRef, AttemptRef, FakeAdapter],
) -> None:
    _repo, world, seeded, retrying, adapter = orders
    assert adapter.confirm(_hint(adapter, seeded), trigger="final_failure").verdict == "confirmed"
    retried = adapter.confirm(_hint(adapter, retrying), trigger="final_failure")
    assert retried.verdict == "discarded" and "will be retried" in retried.reason
    every = adapter.confirm(_hint(adapter, retrying), trigger="every_failure")
    assert every.verdict == "confirmed" and every.failures[0].is_final is False
    # a later attempt exists: the earlier one was retried
    world.fail("orders_flow", RETRY_RUN, "transform", text="still failing", attempt=2)
    later = adapter.confirm(_hint(adapter, retrying), trigger="final_failure")
    assert later.verdict == "discarded" and "was retried" in later.reason
    unknown = adapter.confirm(_hint(adapter, seeded.next_attempt().next_attempt()), trigger="final_failure")
    assert unknown.verdict == "discarded" and "knows only 1" in unknown.reason
    world.set_state(seeded, AttemptState.RUNNING)
    running = adapter.confirm(_hint(adapter, seeded), trigger="every_failure")
    assert running.verdict == "discarded" and "running" in running.reason
    foreign = adapter.confirm(
        _hint(adapter, AttemptRef("elsewhere", "orders_flow", RUN, "transform")), trigger="final_failure"
    )
    assert foreign.verdict == "discarded"
    no_step = adapter.confirm(
        _hint(adapter, AttemptRef("fake", "orders_flow", RUN, "nope")), trigger="final_failure"
    )
    assert no_step.verdict == "discarded"


def test_confirm_localises_a_run_level_hint(tmp_path: Path) -> None:
    world = FakeWorld()
    steps = {name: TaskInfo(name, "python") for name in ("a", "b", "c")}
    world.add_pipeline("flow", file="flows/flow.py", steps=steps)
    first = world.fail("flow", "r", "a", text="boom")
    world.fail("flow", "r", "b", text="flaky", final=False)
    second = world.fail("flow", "r", "b", text="boom again", attempt=2)
    adapter = FakeAdapter(world=world)
    hint = _hint(adapter, AttemptRef("fake", "flow", "r", UNRESOLVED_STEP))
    confirmation = adapter.confirm(hint, trigger="final_failure")
    assert confirmation.verdict == "confirmed"
    assert [f.ref for f in confirmation.failures] == [first, second]
    assert "localised to a, b" in confirmation.reason
    world.add_pipeline("quiet", file="flows/quiet.py", steps=steps)
    world.fail("quiet", "r", "c", text="flaky", final=False)
    quiet = adapter.confirm(
        _hint(adapter, AttemptRef("fake", "quiet", "r", UNRESOLVED_STEP)), trigger="final_failure"
    )
    assert quiet.verdict == "discarded"
    assert adapter.confirm(
        _hint(adapter, AttemptRef("fake", "quiet", "r", UNRESOLVED_STEP)), trigger="every_failure"
    ).failures


def test_queue_redelivers_until_acknowledged() -> None:
    world = FakeWorld()
    adapter = FakeAdapter(world=world)
    first = world.push("fake", {"pipeline_id": "flow", "run_id": "r1"})
    world.push("fake", {"pipeline_id": "flow", "run_id": "r2"})
    assert [m.receipt for m in adapter.receive(max_items=1)] == [first]
    assert len(adapter.receive()) == 2
    adapter.ack([first])
    (left,) = adapter.receive()
    assert left.payload == {"pipeline_id": "flow", "run_id": "r2"} and left.channel == "fake"
    assert world.io_calls == 4


def test_normalize_validates_and_whitelists() -> None:
    adapter = FakeAdapter()
    (hint,) = adapter.normalize(
        "fake",
        {
            "pipeline_id": "f",
            "run_id": "r",
            "map_index": 3,
            "attempt": 2,
            "event_id": "e" * 300,
            "input": {"x": 1},
        },
        {},
    )
    assert hint.ref == AttemptRef("fake", "f", "r", UNRESOLVED_STEP, 3, 2)
    assert hint.native == {"event_id": "e" * 128} and hint.source == "push" and hint.error_text == ""
    (long,) = adapter.normalize(
        "fake", {"pipeline_id": "f", "run_id": "r", "error": "x" * (MAX_HINT_ERROR_CHARS + 5)}, {}
    )
    assert len(long.error_text) == MAX_HINT_ERROR_CHARS
    bad_payloads: list[Any] = [
        [],
        {"run_id": "r"},
        {"pipeline_id": "f", "run_id": ""},
        {"pipeline_id": "f", "run_id": "r", "attempt": True},
        {"pipeline_id": "f", "run_id": "r", "attempt": 0},
        {"pipeline_id": "f", "run_id": "r", "map_index": -2},
        {"pipeline_id": "f", "run_id": "r", "error": 3},
        {"pipeline_id": "f" * 251, "run_id": "r"},
    ]
    for payload in bad_payloads:
        with pytest.raises(ValueError):
            adapter.normalize("fake", payload, {})
    with pytest.raises(ValueError, match="unknown push channel"):
        adapter.normalize("task-failed", {"pipeline_id": "f", "run_id": "r"}, {})
    assert adapter.world.io_calls == 0


def test_remediation_kinds_and_effects(
    orders: tuple[Path, FakeWorld, AttemptRef, AttemptRef, FakeAdapter],
) -> None:
    _repo, world, seeded, _retrying, adapter = orders
    assert adapter.available(seeded, {}) == (RemediationKind.RETRY_STEP, RemediationKind.RESUME_RUN)
    run_level = AttemptRef("fake", "orders_flow", RUN, RUN_LEVEL_STEP)
    assert adapter.available(run_level, {}) == (RemediationKind.RESUME_RUN, RemediationKind.RERUN_PIPELINE)
    assert adapter.available(AttemptRef("fake", "orders_flow", RUN, UNRESOLVED_STEP), {}) == ()
    with pytest.raises(NotSupported):
        adapter.remediate(seeded, {}, RemediationKind.RERUN_PIPELINE, idempotency_key="k0", actor="ann")

    retry = adapter.remediate(seeded, {}, RemediationKind.RETRY_STEP, idempotency_key="k1", actor="ann")
    assert (
        retry.accepted
        and retry.new_ref == seeded.next_attempt()
        and "ann retried step transform" in retry.detail
    )
    new_attempt = world.attempt(seeded.next_attempt())
    assert new_attempt is not None and new_attempt.state is AttemptState.RUNNING
    again = adapter.remediate(seeded, {}, RemediationKind.RETRY_STEP, idempotency_key="k1", actor="bob")
    assert again is retry and len(world.remediations) == 1
    assert world.remediations[0].actor == "ann" and world.remediations[0].kind is RemediationKind.RETRY_STEP

    resume = adapter.remediate(seeded, {}, RemediationKind.RESUME_RUN, idempotency_key="k2", actor="ann")
    assert resume.warnings and resume.new_ref == seeded.next_attempt()
    world.fail("orders_flow", RUN, RUN_LEVEL_STEP, text="run timed out", error_name="FakeflowTimeout")
    rerun = adapter.remediate(
        run_level, {}, RemediationKind.RERUN_PIPELINE, idempotency_key="k3", actor="ann"
    )
    assert (
        rerun.new_ref is not None and rerun.new_ref.run_id == f"{RUN}__rerun1" and rerun.new_ref.attempt == 1
    )
    assert world.attempt(rerun.new_ref) is not None
    with pytest.raises(NotFound):
        adapter.remediate(
            AttemptRef("fake", "orders_flow", "nope", "transform"),
            {},
            RemediationKind.RETRY_STEP,
            idempotency_key="k4",
            actor="ann",
        )


def test_links_encode_every_id() -> None:
    links = FakeAdapter(options=FakeOptions(base_url="https://fakeflow.example/ui/")).links
    ref = AttemptRef("fake", "my flow", "manual__2026-10-18T09:00:00+00:00 a/b", "group.step", 4, 2)
    (url,) = links.links(ref, {}).values()
    assert url == (
        "https://fakeflow.example/ui/flows/my%20flow/runs/manual__2026-10-18T09%3A00%3A00%2B00%3A00%20a%2Fb"
        "/steps/group.step?attempt=2&map_index=4"
    )
    (run_url,) = links.links(AttemptRef("fake", "f", "r", RUN_LEVEL_STEP), {}).values()
    assert run_url == "https://fakeflow.example/ui/flows/f/runs/r?attempt=1"


def test_inspector_details_logs_and_sources(
    orders: tuple[Path, FakeWorld, AttemptRef, AttemptRef, FakeAdapter],
) -> None:
    repo, world, seeded, _retrying, adapter = orders
    detail = adapter.describe(seeded, {})
    assert (detail.error_name, detail.executor, detail.resource) == (
        "ValueError",
        "python",
        "etl.orders:transform",
    )
    assert detail.is_final and detail.max_attempts == 1 and detail.native == {"seq": 1}
    assert PII_EMAIL in (detail.error_message or "")
    bundle = adapter.logs(seeded, detail)
    assert bundle.source == "fake:step-log" and bundle.confidence == "exact" and not bundle.truncated
    lines = bundle.text.splitlines()
    assert lines[0].startswith("[2026-10-18T09:00:00+00:00] INFO - Fakeflow: starting step transform")
    assert "Traceback (most recent call last):" in bundle.text and lines[-1].endswith(
        "failed (no retry left)"
    )
    cut = adapter.logs(seeded, detail, max_chars=40)
    assert cut.truncated and len(cut.text) == 40 and cut.notes
    with pytest.raises(NotFound):
        adapter.describe(AttemptRef("fake", "orders_flow", "nope", "transform"), {})
    source = adapter.pipeline_source("orders_flow", include_text=True)
    assert source is not None and source.text == FLOW_FILE and source.relative_path == "flows/orders_flow.py"
    assert adapter.pipeline_source("orders_flow").text is None  # type: ignore[union-attr]
    assert adapter.pipeline_source("nope") is None
    assert adapter.code.pipeline_file(repo, "orders_flow", source) == "flows/orders_flow.py"
    assert adapter.code.pipeline_file(repo, "nope", None) is None
    assert adapter.code.path_mappings(repo, TRANSFORM) == world.path_mappings


def test_outages_health_doctor_and_close(
    orders: tuple[Path, FakeWorld, AttemptRef, AttemptRef, FakeAdapter],
) -> None:
    _repo, world, seeded, _retrying, adapter = orders
    assert adapter.health().ok
    (check,) = adapter.doctor_checks()
    assert check.run().status == "ok"
    world.outage = "server_error"
    with pytest.raises(OrchestratorError) as raised:
        adapter.poll(cursor=None, is_monitored=lambda _p: True)
    assert raised.value.transient and raised.value.status_code == 500
    world.outage = "network"
    report = adapter.health()
    assert not report.ok and "connection refused" in report.detail and check.run().status == "error"
    assert adapter.confirm(_hint(adapter, seeded), trigger="final_failure").verdict == "retry_later"
    assert adapter.logs(seeded, None).notes
    adapter.close()
    adapter.close()
    assert adapter.closed


def test_factory_applies_the_configured_section(tmp_path: Path, make_config: MakeConfig) -> None:
    cfg = make_config({"type": "fake", "instance": "fake-dev", "options": {"base_url": "https://ff.test/"}})
    adapter = FACTORY.create(cfg)
    assert (adapter.instance, adapter.links.base_url) == ("fake-dev", "https://ff.test")
    assert adapter.world.instance == "fake-dev"
    with pytest.raises(AdapterOptionsError, match="orchestrator.options.nope"):
        FACTORY.create(make_config({"type": "fake", "options": {"nope": 1}}))
    with pytest.raises(AdapterOptionsError, match="base_url"):
        FACTORY.create(make_config({"type": "fake", "options": {"base_url": "ftp://x"}}))
    with pytest.raises(TypeError):
        FACTORY.create(cfg, client=object())
    with pytest.raises(ValueError, match="belongs to instance"):
        FACTORY.create(cfg, client=FakeWorld())
    plain = FACTORY.create(
        config_from_mapping({"repo": {"path": str(tmp_path)}}), client=FakeWorld(instance="w")
    )
    assert plain.instance == "w"
    assert FACTORY.secret_options == ("token",) and FACTORY.options_model is FakeOptions


def test_world_file_is_read_on_first_use(tmp_path: Path, make_config: MakeConfig) -> None:
    config_dir = tmp_path / "config"
    config_dir.mkdir()
    healer_yaml = config_dir / "healer.yaml"
    healer_yaml.write_text("version: 1\n", encoding="utf-8")
    cfg = make_config({"type": "fake", "options": {"world": "worlds/day2.yaml"}}, source_path=healer_yaml)
    adapter = FACTORY.create(cfg)  # the file does not exist yet: construction must not read it
    with pytest.raises(OrchestratorError, match="cannot read the Fakeflow world file") as raised:
        adapter.poll(cursor=None, is_monitored=lambda _p: True)
    assert not raised.value.transient
    assert not adapter.health().ok
    world_file = config_dir / "worlds" / "day2.yaml"
    world_file.parent.mkdir()
    world_file.write_text(
        "pipelines:\n"
        "  sales:\n"
        "    file: dags/sales.py\n"
        "    steps:\n"
        "      transform:\n"
        "        {callable_name: transform, callable_module: src.sales, callable_file: src/sales.py}\n"
        "failures:\n"
        "  - {pipeline: sales, run: r1, step: transform, error_name: KeyError, at: 2026-10-18T09:00:00Z,\n"
        "     text: \"KeyError: 'amount'\"}\n",
        encoding="utf-8",
    )
    page = adapter.poll(cursor=None, is_monitored=lambda _p: True)
    (failure,) = page.failures
    assert failure.ref == AttemptRef("fake", "sales", "r1", "transform") and failure.error_name == "KeyError"
    assert failure.failed_at == T0
    (spec,) = adapter.code.scan(tmp_path)
    assert spec.task("transform") == TaskInfo("transform", "python", "transform", "src.sales", "src/sales.py")


def test_world_from_mapping_builds_and_validates() -> None:
    world = FakeWorld.from_mapping(
        {
            "path_mappings": [{"runtime_prefix": "/opt/fakeflow/repo"}],
            "pipelines": {
                "inv": {
                    "file": "dags/inv.py",
                    "source": "flow source",
                    "steps": {"build": None},
                    "datasets": [
                        {
                            "task_id": "build",
                            "path": "data/inv.csv",
                            "type": "csv",
                            "resolved": True,
                            "source": "x:1",
                        }
                    ],
                }
            },
            "failures": [
                {
                    "pipeline": "inv",
                    "run": "r",
                    "step": "build",
                    "final": False,
                    "map_index": 2,
                    "attempt": 3,
                    "text": "x",
                }
            ],
        },
        instance="fake-x",
    )
    assert world.path_mappings == [PathMapping("/opt/fakeflow/repo", "")]
    assert world.pipelines["inv"].datasets == [DatasetRef("build", "data/inv.csv", "csv", True, "x:1")]
    assert world.pipelines["inv"].steps["build"] == TaskInfo("build", "python")
    (attempt,) = world.attempts.values()
    assert (
        attempt.ref == AttemptRef("fake-x", "inv", "r", "build", 2, 3)
        and attempt.state is AttemptState.RETRYING
    )
    invalid: list[Any] = [
        [],
        {"unknown": 1},
        {"pipelines": []},
        {"pipelines": {"p": {"file": "f.py", "bogus": 1}}},
        {"pipelines": {"p": {"steps": {"s": {"task_id": "s"}}}}},
        {"pipelines": {"p": {"datasets": [{"path": "x"}]}}},
        {
            "pipelines": {"p": {"steps": {"s": None}}},
            "failures": [{"pipeline": "p", "run": "r", "step": "s", "at": "soon"}],
        },
        {"failures": [{"pipeline": "p"}]},
        {"path_mappings": [{"runtime_prefix": "/x", "extra": 1}]},
        {"path_mappings": {"runtime_prefix": "/x"}},
    ]
    for data in invalid:
        with pytest.raises(ValueError):
            FakeWorld.from_mapping(data)


def test_world_load_reports_the_file(tmp_path: Path) -> None:
    broken = tmp_path / "broken.yaml"
    broken.write_text("pipelines: [unclosed\n", encoding="utf-8")
    with pytest.raises(ValueError, match="invalid YAML"):
        FakeWorld.load(broken)
    wrong = tmp_path / "wrong.yaml"
    wrong.write_text("pipelines: 3\n", encoding="utf-8")
    with pytest.raises(ValueError, match="wrong.yaml"):
        FakeWorld.load(wrong)
    empty = tmp_path / "empty.yaml"
    empty.write_text("", encoding="utf-8")
    assert FakeWorld.load(empty).pipelines == {}


def test_end_to_end_scenario_fails_like_the_demo(tmp_path: Path, healer_env: Path) -> None:
    """The scenario of check 14: each DAG fails with the demo's exception in the demo's file."""
    setup = FakeHarness().end_to_end(tmp_path)
    world = setup.adapter.world  # type: ignore[attr-defined]
    found = {}
    for case in setup.cases:
        detail = setup.adapter.inspector.describe(case.ref, {})
        frames = "\n".join(
            line for line in detail.traceback_text.splitlines() if line.strip().startswith('File "')
        )
        found[case.ref.pipeline_id] = (case.ref.step_id, detail.error_name, frames.replace("\\", "/"))
    assert found["sales_daily_etl"][:2] == ("transform_sales", "KeyError")
    assert found["customers_sync"][:2] == ("transform", "ValueError")
    assert found["inventory_report"][:2] == ("build_report", "KeyError")
    repo = (tmp_path / "e2e_repo").as_posix()
    assert f"{repo}/src/etl/sales.py" in found["sales_daily_etl"][2]
    assert f"{repo}/src/etl/customers.py" in found["customers_sync"][2]
    assert f"{repo}/src/etl/inventory.py" in found["inventory_report"][2]
    assert all(attempt.ref.run_id == E2E_RUN for attempt in world.ordered())
    assert setup.adapter.inspector.logs(setup.cases[0].ref, None).text.endswith("(no retry left)\n")
    assert [case.expected_status for case in setup.cases] == ["reported", "repaired", "waiting_human"]
    assert setup.cfg.repo_root == (tmp_path / "e2e_repo").resolve()
    assert Path(setup.cfg.database_url.removeprefix("sqlite:///")).is_relative_to(healer_env.resolve())
