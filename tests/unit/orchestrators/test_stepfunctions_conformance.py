"""The packaged conformance suite against the AWS Step Functions adapter, then its wiring.

The harness builds the adapter with ``FACTORY.create(cfg, client=world, clock=lambda: NOW)`` on
a copy of the fixture repository ``tests/unit/fixtures/repos/sfn_etl`` (its own ``healer.yaml``)
and the offline world ``tests/unit/fixtures/stepfunctions/world/sfn_etl.json``: an
:class:`~dag_healer.stepfunctions.testing.OfflineAws` whose real botocore clients are answered in
memory, without credentials or network. The world holds the day-2 state of the example one hour
after its three executions failed:

* ``sales_daily_etl`` fails in ``TransformSales`` with ``KeyError: 'amount'`` (DATA: the approved
  contract on ``s3://acme-landing/incoming/sales.csv`` is violated);
* ``customers_sync`` fails in ``TransformCustomers`` with a ``ValueError`` of the latent bug in
  ``parse_signup_date`` (CODE). It is the seeded failure: its execution input and its Lambda log
  carry the address :data:`PII_EMAIL`, which must never reach ``native`` or a log record;
* ``inventory_report`` fails in ``BuildReport``, iteration 0 of the inline Map ``PerWarehouse``
  (``map_index=0``), with ``KeyError: 'qty'`` (AMBIGUOUS: its contract is only a draft).

Step Functions retries are not visible as failed attempts, so there is no retrying failure to
seed. The end-to-end check (14) runs the feed, the inspector and the logs of the three cases,
then the worker's ``process_incident`` through the adapter: DATA -> reported, CODE -> repaired
(a PR.md and a test), AMBIGUOUS -> waiting for a human.

The rest of the module checks what the suite cannot see, against the same world: the push
samples and the forged hint, the seeded native, queue mode end to end, the inventory function's
own log group, the definitions that ran against the repository, a redrive that fails again
(attempt 2), the doctor checks and the health, the fixture repository's ``healer.yaml``
building the adapter through the registry without I/O, and ``dag-healer worker --once`` handling
the three cases from the command line (the CODE fix limited to the customers code and its test).
"""

from __future__ import annotations

import copy
import functools
import json
import pkgutil
import re
import shutil
from collections.abc import Iterator
from dataclasses import replace
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import pytest

from dag_healer import stepfunctions
from dag_healer.config import HealerConfig, load_config
from dag_healer.core.failures import FailureHint
from dag_healer.core.ids import NO_MAP_INDEX, UNRESOLVED_STEP, AttemptRef
from dag_healer.core.ports import OrchestratorAdapter
from dag_healer.core.vocab import RemediationKind
from dag_healer.orchestrators import registry
from dag_healer.orchestrators.conformance import (
    ODD_RUN_ID,
    EndToEndCase,
    EndToEndSetup,
    OrchestratorConformance,
    SeededFailure,
    assert_native_fits,
    no_network,
)
from dag_healer.stepfunctions import FACTORY, arns
from dag_healer.stepfunctions.adapter import StepFunctionsAdapter
from dag_healer.stepfunctions.aws import AwsClients
from dag_healer.stepfunctions.testing import OfflineAws, refuse_aws_requests
from dag_healer.storage import s3 as s3_storage

TESTS = Path(__file__).resolve().parents[1]
SFN_REPO = TESTS / "fixtures" / "repos" / "sfn_etl"
WORLD_DIR = TESTS / "fixtures" / "stepfunctions" / "world"
WORLD_FILE = WORLD_DIR / "sfn_etl.json"
#: The FAILED status change of the customers execution, as the world's SQS queue holds it.
ENVELOPE_FILE = WORLD_DIR / "sfn_etl" / "sqs" / "customers-sync-failed.json"

INSTANCE = "sfn-test"
REGION = "eu-west-1"
ACCOUNT = "123456789012"
#: The world clock: day 2 of the example, one hour after the failures.
NOW = datetime(2026, 10, 2, 7, 0, tzinfo=timezone.utc)
PII_EMAIL = "alice.pii@example.com"
QUEUE_URL = f"https://sqs.{REGION}.amazonaws.com/{ACCOUNT}/healer-sfn-events"
CUSTOMERS_SM = arns.state_machine_arn(REGION, ACCOUNT, "customers-sync")
CUSTOMERS_RUN = "customers-2026-10-02"
CUSTOMERS_FUNCTION = f"arn:aws:lambda:{REGION}:{ACCOUNT}:function:sfn-etl-CustomersFunction-7G8H9I0J1K2L"
FORGED_RUN = "customers-2026-10-02-forged"
SEEDED = AttemptRef(INSTANCE, "customers_sync", CUSTOMERS_RUN, "TransformCustomers", NO_MAP_INDEX, 1)
GIT_IDENTITY = {
    "GIT_AUTHOR_NAME": "Test",
    "GIT_AUTHOR_EMAIL": "test@example.invalid",
    "GIT_COMMITTER_NAME": "Test",
    "GIT_COMMITTER_EMAIL": "test@example.invalid",
}
#: pipeline -> (run, failing state, map_index, category, final status of the worker).
E2E_CASES: dict[str, tuple[str, str, int, str, str]] = {
    "sales_daily_etl": ("sales-2026-10-02", "TransformSales", NO_MAP_INDEX, "data", "reported"),
    "customers_sync": (CUSTOMERS_RUN, "TransformCustomers", NO_MAP_INDEX, "code", "repaired"),
    "inventory_report": ("inventory-2026-10-02", "BuildReport", 0, "ambiguous", "waiting_human"),
}


@pytest.fixture(autouse=True)
def no_aws_requests() -> Iterator[list[str]]:
    """No request leaves the process: botocore's sender refuses to send (``refuse_aws_requests``),
    and a test that tried fails at teardown."""
    with refuse_aws_requests() as refused:
        yield refused
    assert refused == [], f"requests that would have left the process: {refused}"


# =========================================================================== helpers


def copy_repo(target: Path) -> Path:
    """A copy of the fixture repository (its ``healer.yaml`` points at the copy: ``repo.path: .``)."""
    shutil.copytree(SFN_REPO, target, ignore=shutil.ignore_patterns("__pycache__", ".pytest_cache"))
    return target


def envelope() -> dict[str, Any]:
    """The EventBridge envelope of the customers failure (its ``detail.input`` holds the address)."""
    return json.loads(ENVELOPE_FILE.read_text(encoding="utf-8"))


def plain_envelope() -> dict[str, Any]:
    """The envelope as an API destination posts it, without execution data."""
    data = envelope()
    for key in ("input", "output", "inputDetails", "outputDetails"):
        data["detail"].pop(key, None)
    return data


def sqs_message() -> dict[str, Any]:
    """What the queue port hands over: the SQS message id and the envelope as its body text."""
    return {"MessageId": "0b5c2f4e-6a1d-4a8e-9f3b-2c7d8e9f0a1b", "Body": ENVELOPE_FILE.read_text("utf-8")}


def envelope_with_data() -> dict[str, Any]:
    """The envelope carrying execution input and output that name the address."""
    data = envelope()
    data["detail"]["input"] = json.dumps({"run_date": "2026-10-02", "requested_by": PII_EMAIL})
    data["detail"]["output"] = json.dumps({"notified": [PII_EMAIL]})
    data["detail"]["inputDetails"] = {"included": True}
    data["detail"]["outputDetails"] = {"included": True}
    return data


def polled(adapter: OrchestratorAdapter, cursor: dict[str, Any] | None = None) -> dict[tuple[Any, ...], Any]:
    """The failures one poll returns, by incident key."""
    assert adapter.feed is not None
    page = adapter.feed.poll(cursor=cursor, is_monitored=lambda _pipeline: True)
    return {failure.ref.key(): failure for failure in page.failures}


def stepfunctions_modules() -> list[str]:
    """Every module of the ``dag_healer.stepfunctions`` package, and the S3 dataset reader."""
    names = {stepfunctions.__name__}
    for module in pkgutil.walk_packages(stepfunctions.__path__, prefix=f"{stepfunctions.__name__}."):
        names.add(module.name)
    names.add(s3_storage.__name__)
    return sorted(names)


def prepare_e2e_repo(repo: Path) -> None:
    """A git copy of the fixture repository with the sales and customers contracts approved.

    The contracts are the drafts dag-healer inferred from the day-1 exports; approving two of
    them makes the day-2 cases DATA (sales: the contract is violated), CODE (customers: the data
    is valid, the code is not) and AMBIGUOUS (inventory: a draft decides nothing).
    """
    from dag_healer.contracts.io import approve_contract, contract_path, load_contract, save_contract
    from dag_healer.repo.git import run_git

    copy_repo(repo)
    for pipeline in ("sales_daily_etl", "customers_sync"):
        path = contract_path(repo / "healer" / "contracts", pipeline)
        save_contract(
            approve_contract(load_contract(path), "data-lead").model_copy(update={"approved_at": NOW}), path
        )
    run_git(["init", "--quiet", "-b", "main"], repo)
    run_git(["add", "-A"], repo)
    run_git(["-c", "commit.gpgsign=false", "commit", "--quiet", "-m", "day 2"], repo, env=GIT_IDENTITY)


# =========================================================================== the harness


class StepFunctionsHarness:
    """:class:`~dag_healer.orchestrators.conformance.ConformanceHarness` of the Step Functions adapter."""

    def __init__(self) -> None:
        self.world: OfflineAws | None = None
        self.adapters: list[OrchestratorAdapter] = []
        self.serving_s3 = False

    def _adapter(self, cfg: HealerConfig) -> OrchestratorAdapter:
        """A fresh world (nothing called yet) and the adapter on it."""
        self.world = OfflineAws.load(WORLD_FILE)
        adapter = FACTORY.create(cfg, client=self.world, clock=lambda: NOW)
        self.adapters.append(adapter)
        return adapter

    def build(self, tmp_path: Path) -> tuple[HealerConfig, OrchestratorAdapter]:
        cfg = load_config(copy_repo(tmp_path / "repo") / "healer.yaml")
        return cfg, self._adapter(cfg)

    def seeded_failure(self) -> SeededFailure:
        return SeededFailure(SEEDED, True, "ValueError", "src/etl/customers.py", "functions/customers/app.py")

    def seeded_retrying(self) -> SeededFailure | None:
        return None  # Step Functions retries are internal to an attempt (native.retry_attempts)

    def forged_hint(self) -> FailureHint | None:
        forged = replace(SEEDED, run_id=FORGED_RUN, step_id=UNRESOLVED_STEP)
        native = {
            "execution_arn": arns.execution_arn(CUSTOMERS_SM, FORGED_RUN),
            "state_machine_arn": CUSTOMERS_SM,
            "status": "FAILED",
            "redrive_count": 0,
        }
        return FailureHint(
            forged, "eventbridge", "eventbridge", error_text="ValueError: forged", native=native
        )

    def push_samples(self) -> list[tuple[str, dict, dict]]:
        return [
            ("eventbridge", plain_envelope(), {"X-Healer-Token": "ingest-token"}),
            ("eventbridge", sqs_message(), {}),
            ("eventbridge", envelope_with_data(), {}),
        ]

    def io_calls(self) -> int:
        assert self.world is not None
        return self.world.io_calls

    def simulate_outage(self, kind: str) -> None:
        assert self.world is not None
        self.world.outage = kind  # type: ignore[assignment]

    def pii_strings(self) -> list[str]:
        return [PII_EMAIL]

    def link_ref(self) -> tuple[AttemptRef, dict[str, Any]]:
        """An execution ARN whose name is the odd run id (``:``, ``+``, ``/`` and a space)."""
        odd = replace(SEEDED, run_id=ODD_RUN_ID)
        return odd, {"execution_arn": arns.execution_arn(CUSTOMERS_SM, ODD_RUN_ID)}

    def adapter_modules(self) -> list[str]:
        return stepfunctions_modules()

    def end_to_end(self, tmp_path: Path) -> EndToEndSetup:
        repo = tmp_path / "e2e_repo"
        prepare_e2e_repo(repo)
        cfg = load_config(repo / "healer.yaml")
        adapter = self._adapter(cfg)
        world = self.world
        assert world is not None
        s3_storage.set_client_factory(lambda: world.client_factory("s3"))  # the contracts' s3:// inputs
        self.serving_s3 = True
        cases = tuple(
            EndToEndCase(AttemptRef(INSTANCE, pipeline, run, step, index), category, status)  # type: ignore[arg-type]
            for pipeline, (run, step, index, category, status) in E2E_CASES.items()
        )
        return EndToEndSetup(cfg=cfg, adapter=adapter, cases=cases)

    def close(self) -> None:
        """Close the adapters and give the S3 dataset reader its default client back."""
        for adapter in self.adapters:
            adapter.close()
        self.adapters.clear()
        if self.serving_s3:
            s3_storage.set_client_factory(None)
            self.serving_s3 = False


class TestStepFunctionsConformance(OrchestratorConformance):
    """The packaged suite against Step Functions (every check; 04's retrying case does not apply)."""

    @pytest.fixture
    def harness(self, healer_env: Path) -> Iterator[StepFunctionsHarness]:
        harness = StepFunctionsHarness()
        yield harness
        harness.close()


# =========================================================================== beyond the suite


@pytest.fixture
def harness(healer_env: Path) -> Iterator[StepFunctionsHarness]:
    harness = StepFunctionsHarness()
    yield harness
    harness.close()


def test_every_push_sample_names_the_seeded_execution(harness: StepFunctionsHarness, tmp_path: Path) -> None:
    _cfg, adapter = harness.build(tmp_path)
    assert adapter.push is not None
    sources = []
    for channel, payload, headers in harness.push_samples():
        (hint,) = adapter.push.normalize(channel, copy.deepcopy(payload), headers)
        assert hint.ref == replace(SEEDED, step_id=UNRESOLVED_STEP)
        assert hint.native["execution_arn"] == arns.execution_arn(CUSTOMERS_SM, CUSTOMERS_RUN)
        assert PII_EMAIL not in json.dumps(dict(hint.native)) and PII_EMAIL not in hint.error_text
        sources.append(hint.source)
    assert sources == ["eventbridge", "sqs", "eventbridge"]
    assert harness.io_calls() == 0


def test_the_seeded_failure_is_polled_with_ids_only(harness: StepFunctionsHarness, tmp_path: Path) -> None:
    _cfg, adapter = harness.build(tmp_path)
    assert adapter.feed is not None
    page = adapter.feed.poll(cursor=None, is_monitored=lambda _pipeline: True)
    by_key = {failure.ref.key(): failure for failure in page.failures}
    assert set(by_key) == {
        AttemptRef(INSTANCE, pipeline, run, step, index).key()
        for pipeline, (run, step, index, _category, _status) in E2E_CASES.items()
    }
    failure = by_key[SEEDED.key()]
    assert (failure.source, failure.error_name, failure.is_final) == ("poll", "ValueError", True)
    assert failure.failed_at == datetime(2026, 10, 2, 6, 0, 10, 284000, tzinfo=timezone.utc)
    native = dict(failure.native)
    assert_native_fits(native)
    assert native["execution_arn"] == arns.execution_arn(CUSTOMERS_SM, CUSTOMERS_RUN)
    assert (native["status"], native["redrive_count"], native["event_type"]) == ("FAILED", 0, "TaskFailed")
    assert PII_EMAIL not in json.dumps(native) and "unconverted data" not in json.dumps(native)
    assert set(page.cursor) == {
        arns.state_machine_arn(REGION, ACCOUNT, name)
        for name in (
            "SalesDailyEtlStateMachine-AbC123",
            "customers-sync",
            "InventoryReportStateMachine-XyZ789",
        )
    } | {"watermark"}
    assert page.cursor["watermark"] == NOW.isoformat(timespec="milliseconds")


def test_the_forged_hint_is_discarded_after_one_describe(
    harness: StepFunctionsHarness, tmp_path: Path
) -> None:
    _cfg, adapter = harness.build(tmp_path)
    assert adapter.push is not None and harness.world is not None
    forged = harness.forged_hint()
    assert forged is not None
    confirmation = adapter.push.confirm(forged, trigger="final_failure")
    assert confirmation.verdict == "discarded" and "forged or deleted" in confirmation.reason
    assert [(call.service, call.operation) for call in harness.world.calls] == [
        ("stepfunctions", "DescribeExecution")
    ]


def test_the_queued_message_confirms_to_the_seeded_failure(
    harness: StepFunctionsHarness, tmp_path: Path
) -> None:
    """Queue mode end to end: receive the world's SQS message, normalise, confirm, acknowledge."""
    _cfg, adapter = harness.build(tmp_path)
    assert adapter.queue is not None and adapter.push is not None and harness.world is not None
    (message,) = adapter.queue.receive()
    assert adapter.queue.receive() == []  # in flight: invisible until acknowledged or timed out
    (hint,) = adapter.push.normalize(message.channel, message.payload, {})
    assert (hint.channel, hint.source) == ("eventbridge", "sqs")
    confirmation = adapter.push.confirm(hint, trigger="final_failure")
    assert confirmation.verdict == "confirmed", confirmation.reason
    (failure,) = confirmation.failures
    assert (failure.ref, failure.source, failure.error_name) == (SEEDED, "sqs", "ValueError")
    adapter.queue.ack([message.receipt])
    assert harness.world.queue_messages(QUEUE_URL) == []


def test_the_inventory_evidence_names_its_own_log_group(
    harness: StepFunctionsHarness, tmp_path: Path
) -> None:
    """``InventoryFunction`` logs to ``/sfn-etl/inventory``: describe records it from the template,
    the console link and the log lookup use it."""
    _cfg, adapter = harness.build(tmp_path)
    run, step, index, _category, _status = E2E_CASES["inventory_report"]
    ref = AttemptRef(INSTANCE, "inventory_report", run, step, index)
    detail = adapter.inspector.describe(ref, polled(adapter)[ref.key()].native)
    assert detail.native["log_group"] == "/sfn-etl/inventory"
    assert detail.native["step_path"] == "PerWarehouse[0]/BuildReport"
    assert adapter.links.links(ref, detail.native)["CloudWatch logs"].endswith(
        "log-group/$252Fsfn-etl$252Finventory"
    )
    bundle = adapter.inspector.logs(ref, detail)
    assert (bundle.source, bundle.confidence) == ("cloudwatch:/sfn-etl/inventory", "exact")


def test_the_definitions_that_ran_match_the_repository(harness: StepFunctionsHarness, tmp_path: Path) -> None:
    cfg, adapter = harness.build(tmp_path)
    for pipeline, (run, step, index, _category, _status) in E2E_CASES.items():
        ref = AttemptRef(INSTANCE, pipeline, run, step, index)
        source = adapter.inspector.pipeline_source(pipeline, ref=ref, include_text=True)
        assert source is not None and source.text
        assert (source.language, source.origin) == ("asl-json", "api:DescribeStateMachineForExecution")
        assert (
            adapter.code.pipeline_file(cfg.repo_root, pipeline, source) == f"statemachine/{pipeline}.asl.json"
        )
        assert adapter.code.drift(cfg.repo_root, pipeline, source.text) == []


def test_a_redriven_execution_that_fails_again_is_attempt_two(
    harness: StepFunctionsHarness, tmp_path: Path
) -> None:
    _cfg, adapter = harness.build(tmp_path)
    world = harness.world
    assert world is not None and adapter.feed is not None and adapter.remediator is not None
    first = adapter.feed.poll(cursor=None, is_monitored=lambda _pipeline: True)
    native = {failure.ref.key(): failure for failure in first.failures}[SEEDED.key()].native
    result = adapter.remediator.remediate(
        SEEDED, native, RemediationKind.RESUME_RUN, idempotency_key="healer-7-conformance", actor="ops"
    )
    assert result.accepted and result.new_ref == SEEDED.next_attempt()
    execution = arns.execution_arn(CUSTOMERS_SM, CUSTOMERS_RUN)
    assert world.calls_to("stepfunctions", "RedriveExecution")[0].params == {
        "executionArn": execution,
        "clientToken": "healer-7-conformance",
    }
    running = polled(adapter, first.cursor)
    assert SEEDED.key() not in running and SEEDED.next_attempt().key() not in running  # RUNNING again
    cause = json.dumps({"errorMessage": "unconverted data remains: T09:15:00Z", "errorType": "ValueError"})
    world.finish_execution(
        execution,
        status="FAILED",
        error="ValueError",
        cause=cause,
        events=[
            {"type": "TaskStateEntered", "stateEnteredEventDetails": {"name": "TransformCustomers"}},
            {
                "type": "TaskScheduled",
                "taskScheduledEventDetails": {
                    "resourceType": "lambda",
                    "resource": "invoke",
                    "region": REGION,
                    "parameters": json.dumps({"FunctionName": CUSTOMERS_FUNCTION}),
                },
            },
            {
                "type": "TaskStarted",
                "taskStartedEventDetails": {"resourceType": "lambda", "resource": "invoke"},
            },
            {
                "type": "TaskFailed",
                "taskFailedEventDetails": {
                    "resourceType": "lambda",
                    "resource": "invoke",
                    "error": "ValueError",
                    "cause": cause,
                },
            },
        ],
    )
    second = polled(adapter, first.cursor)
    again = second[SEEDED.next_attempt().key()]
    assert (again.native["redrive_count"], again.native["status"]) == (1, "FAILED")
    assert SEEDED.key() not in second, "attempt 1 stays the incident it already is"
    current = adapter.inspector.describe(again.ref, again.native)
    assert (current.is_final, current.error_message) == (True, "unconverted data remains: T09:15:00Z")
    earlier = adapter.inspector.describe(SEEDED, native)
    assert (earlier.is_final, earlier.error_message) == (True, "unconverted data remains: T08:30:00Z")

    # attempt 1's incident still offers the redrive (its native is from detection), but it would
    # redrive attempt 2: refused without a RedriveExecution; attempt 2's own incident redrives it
    assert RemediationKind.RESUME_RUN in adapter.remediator.available(SEEDED, native)
    stale = adapter.remediator.remediate(
        SEEDED, native, RemediationKind.RESUME_RUN, idempotency_key="healer-8-conformance", actor="ops"
    )
    assert (stale.accepted, stale.new_ref) == (False, None) and "attempt 2" in stale.detail
    assert len(world.calls_to("stepfunctions", "RedriveExecution")) == 1
    current_redrive = adapter.remediator.remediate(
        again.ref,
        again.native,
        RemediationKind.RESUME_RUN,
        idempotency_key="healer-9-conformance",
        actor="ops",
    )
    assert current_redrive.accepted and current_redrive.new_ref == again.ref.next_attempt()
    assert current_redrive.new_ref is not None and current_redrive.new_ref.attempt == 3


def test_the_doctor_checks_and_the_health_against_the_world(
    harness: StepFunctionsHarness, tmp_path: Path
) -> None:
    from dag_healer.cli.doctor_cmd import OK, WARN

    _cfg, adapter = harness.build(tmp_path)
    results = {check.name: check.run() for check in adapter.doctor_checks()}
    assert {name: result.status for name, result in results.items()} == {
        "aws credentials": OK,
        "state machines": OK,
        "code mapping": OK,
        "monitor modes": OK,
        "event queue": WARN,  # the world's queue has no dead-letter queue
    }
    assert results["code mapping"].detail == "3 pipeline(s), 9 Lambda state(s) mapped to repository code"
    report = adapter.health()
    assert report.ok and report.checks == {"credentials": "ok", "states": "ok"}
    assert PII_EMAIL not in json.dumps([result.detail for result in results.values()])


def test_the_fixture_healer_yaml_builds_the_adapter_through_the_registry(
    healer_env: Path, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    def no_client(self: AwsClients, service: str) -> Any:
        raise AssertionError(f"building the adapter created the {service} client")

    monkeypatch.setattr(AwsClients, "client", no_client)
    cfg = load_config(copy_repo(tmp_path / "repo") / "healer.yaml")
    with no_network():
        adapter = registry.get_adapter(cfg)
        assert registry.get_adapter(cfg) is adapter  # cached per configuration
        _ = (adapter.vocabulary, adapter.capabilities, adapter.taxonomy, adapter.links, adapter.code)
        adapter.sandbox_env()
        assert [check.name for check in adapter.doctor_checks()] == [
            "aws credentials",
            "state machines",
            "code mapping",
            "monitor modes",
            "event queue",
        ]
    assert isinstance(adapter, StepFunctionsAdapter)
    assert (adapter.type, adapter.instance, adapter.display_name) == (
        "stepfunctions",
        INSTANCE,
        "AWS Step Functions",
    )
    assert adapter.options.region == REGION and adapter.options.queue_url == QUEUE_URL
    assert [entry.pipeline_id for entry in adapter.runtime.pipelines.entries()] == list(E2E_CASES)
    assert adapter.queue is not None and adapter.remediator is not None
    assert adapter.capabilities.remediations == (RemediationKind.RESUME_RUN,)
    assert not adapter.clients.injected
    assert repr(adapter) == f"StepFunctionsAdapter(instance={INSTANCE!r}, region={REGION!r})"


# =========================================================================== the worker gate

#: Files the CODE repair of ``customers_sync`` may change, besides the regression test it adds.
CODE_FIX_SCOPE = ("functions/customers/", "src/etl/customers.py")
_DIFF_FILE = re.compile(r"^diff --git a/(\S+) b/(\S+)$", re.MULTILINE)


def changed_files(pr_md: str) -> set[str]:
    """The files of the unified diff a PR.md reproduces (its ``diff --git a/... b/...`` lines)."""
    return {match.group(2) for match in _DIFF_FILE.finditer(pr_md)}


def test_worker_once_handles_the_three_cases(
    healer_env: Path, tmp_path: Path, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    """``dag-healer worker --once`` with ``type: stepfunctions`` on the offline world, without AWS
    credentials: DATA (the S3 contract is violated), CODE (a fix of the customers code plus a new
    test) and AMBIGUOUS.

    The command builds its adapter through the registry from the repository's ``healer.yaml``;
    the test only hands the factory the offline world and its clock, the worker the same clock
    (the poll window follows the worker's iteration time) and the S3 dataset reader the world's
    S3 client. The poll and the queue (the world's SQS message of the customers failure) both
    run; the queued hint confirms to the polled incident key.
    """
    from dag_healer.cli.main import main
    from dag_healer.store import dispose_engines, session_scope
    from dag_healer.store import repo as store_repo
    from dag_healer.worker import loop

    repo = tmp_path / "gate_repo"
    prepare_e2e_repo(repo)
    world = OfflineAws.load(WORLD_FILE)
    monkeypatch.setattr(
        FACTORY, "create", functools.partial(type(FACTORY).create, FACTORY, client=world, clock=lambda: NOW)
    )
    monkeypatch.setattr(loop, "Worker", functools.partial(loop.Worker, clock=lambda: NOW))
    s3_storage.set_client_factory(lambda: world.client_factory("s3"))
    try:
        assert main(["-c", str(repo / "healer.yaml"), "worker", "--once"]) == 0
        cfg = load_config(repo / "healer.yaml")
        with session_scope(cfg.database_url) as session:
            incidents = {
                incident.dag_id: incident.to_dict()
                for incident in store_repo.list_incidents(session, orchestrator=INSTANCE)
            }
    finally:
        s3_storage.set_client_factory(None)
        dispose_engines()
    out = capsys.readouterr().out
    assert "3 incident(s) created" in out, out

    assert set(incidents) == set(E2E_CASES)
    for pipeline, (run, step, index, category, status) in E2E_CASES.items():
        incident = incidents[pipeline]
        assert (incident["run_id"], incident["task_id"], incident["map_index"], incident["try_number"]) == (
            run,
            step,
            index,
            1,
        )
        assert (incident["category"], incident["status"]) == (category, status), pipeline
        assert incident["orchestrator"] == INSTANCE and incident["source"] in ("poll", "sqs")

    customers = incidents["customers_sync"]
    assert customers["test_path"], "a CODE repair adds a test"
    pr_md = Path(customers["pr_md_path"]).read_text(encoding="utf-8")
    assert "AWS Step Functions" in pr_md
    changed = changed_files(pr_md)
    tests = {path for path in changed if path.startswith("tests/")}
    fixed = changed - tests
    assert customers["test_path"] in tests, changed
    assert fixed and all(path.startswith(CODE_FIX_SCOPE) for path in fixed), changed
    assert {call.operation for call in world.calls} >= {
        "ListExecutions",
        "GetExecutionHistory",
        "ReceiveMessage",
        "DeleteMessageBatch",
        "GetObject",
    }
    assert not world.calls_to("stepfunctions", "RedriveExecution")  # nothing is retried by itself
