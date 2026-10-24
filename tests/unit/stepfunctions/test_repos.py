"""The sfn_etl fixture repository, the deployable example and the offline world scenario.

* the example (``examples/stepfunctions_repo``) shares every file of the fixture repository
  except its guide and ``healer.yaml``;
* ``healer.yaml``, the draft contracts (dag-healer's own inference from the day-1 exports,
  regenerated here and compared), the exports and the fake LLM scripts agree with each other;
* the Lambda steps run in a subprocess on the day-2 exports, the way Lambda runs them, with the
  payloads of the world's histories: the errors are the documented ones and every repository
  frame of the world's causes points at the same line and code; the fake repairs fix them;
* the world scenario (``tests/unit/fixtures/stepfunctions/world/sfn_etl.json``): histories that
  botocore's output shapes accept, definitions that are the repository ASL with the deployed
  ARNs, the seeded e-mail address only where execution data lives, and ``OfflineAws.load``.
"""

from __future__ import annotations

import ast
import copy
import json
import os
import re
import shutil
import subprocess
import sys
from collections.abc import Iterator
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

import pytest

from dag_healer.stepfunctions import asl
from dag_healer.stepfunctions.iac.sam import load_template

from .conftest import ACCOUNT, FIXTURES, INSTANCE, NOW, PIPELINES, REGION, SFN_REPO

ROOT = Path(__file__).resolve().parents[3]
EXAMPLE = ROOT / "examples" / "stepfunctions_repo"
EXISTING = ROOT / "example_existing_repo"
WORLD_FILE = FIXTURES / "world" / "sfn_etl.json"
WORLD_DIR = FIXTURES / "world" / "sfn_etl"
PII = "alice.pii@example.com"
BUCKET = "acme-landing"
QUEUE_URL = f"https://sqs.{REGION}.amazonaws.com/{ACCOUNT}/healer-sfn-events"
FUNCTIONS = {
    "sales": "sfn-etl-SalesFunction-1A2B3C4D5E6F",
    "customers": "sfn-etl-CustomersFunction-7G8H9I0J1K2L",
    "inventory": "sfn-etl-InventoryFunction-3M4N5O6P7Q8R",
}


def _function_arn(folder: str) -> str:
    return f"arn:aws:lambda:{REGION}:{ACCOUNT}:function:{FUNCTIONS[folder]}"


#: pipeline -> what the world holds for it and how it fails on day 2
CASES: dict[str, dict[str, Any]] = {
    "sales_daily_etl": {
        "folder": "sales",
        "machine": "SalesDailyEtlStateMachine-AbC123",
        "asl": "sales_daily_etl.asl.json",
        "substitution": "SalesFunctionArn",
        "execution": "sales-2026-10-02",
        "state": "TransformSales",
        "error": ("KeyError", "'amount'"),
        "module": "/opt/python/etl/sales.py",
    },
    "customers_sync": {
        "folder": "customers",
        "machine": "customers-sync",
        "asl": "customers_sync.asl.json",
        "substitution": "CustomersFunctionArn",
        "execution": "customers-2026-10-02",
        "state": "TransformCustomers",
        "error": ("ValueError", "unconverted data remains: T08:30:00Z"),
        "module": "/opt/python/etl/customers.py",
    },
    "inventory_report": {
        "folder": "inventory",
        "machine": "InventoryReportStateMachine-XyZ789",
        "asl": "inventory_report.asl.json",
        "substitution": "InventoryFunctionArn",
        "execution": "inventory-2026-10-02",
        "state": "BuildReport",
        "error": ("KeyError", "'qty'"),
        "module": "/opt/python/etl/inventory.py",
    },
}

#: Runs the steps of one function on the day-2 exports, the way Lambda runs them: /var/task is
#: functions/<name>, /opt/python is src, LANDING_BUCKET is set and the S3 client is in memory.
RUNNER = r"""
import io, json, logging, os, sys, traceback

repo, function, events, day2 = sys.argv[1:5]
sys.path[:0] = [os.path.join(repo, "functions", function), os.path.join(repo, "src")]
os.environ["LANDING_BUCKET"] = "acme-landing"
lines = []


class Capture(logging.Handler):
    def emit(self, record):
        lines.append(record.getMessage())


logging.getLogger().handlers[:] = [Capture()]
import etl.common as common

store = {}
for name in os.listdir(day2):
    with open(os.path.join(day2, name), "rb") as handle:
        store[("acme-landing", "incoming/" + name)] = handle.read()


class MemoryS3:
    def get_object(self, Bucket, Key):
        return {"Body": io.BytesIO(store[(Bucket, Key)])}

    def put_object(self, Bucket, Key, Body):
        store[(Bucket, Key)] = Body


common._s3 = lambda: MemoryS3()
import app

results = []
for event in json.loads(events):
    lines = []
    stdout, sys.stdout = sys.stdout, io.StringIO()
    try:
        output = app.handler(event, None)
    except Exception as exc:
        sys.stdout = stdout
        frames = traceback.extract_tb(exc.__traceback__)[1:]
        error = {"type": type(exc).__name__, "message": str(exc),
                 "frames": [[f.filename, f.lineno, f.name, f.line] for f in frames]}
        results.append({"lines": lines, "error": error})
        break
    sys.stdout = stdout
    results.append({"lines": lines, "output": output})
print(json.dumps(results))
"""


def _files(root: Path) -> set[str]:
    return {
        path.relative_to(root).as_posix()
        for path in root.rglob("*")
        if path.is_file() and "__pycache__" not in path.parts and path.name != ".DS_Store"
    }


def _world() -> dict[str, Any]:
    return json.loads(WORLD_FILE.read_text(encoding="utf-8"))


def _execution(world: dict[str, Any], pipeline: str) -> dict[str, Any]:
    return next(item for item in world["executions"] if item["name"] == CASES[pipeline]["execution"])


def _strings(value: Any, path: str = "") -> Iterator[tuple[str, str]]:
    """``(path, text)`` of every string leaf of a JSON value."""
    if isinstance(value, dict):
        for key, item in value.items():
            yield from _strings(item, f"{path}.{key}" if path else str(key))
    elif isinstance(value, list):
        for index, item in enumerate(value):
            yield from _strings(item, f"{path}[{index}]")
    elif isinstance(value, str):
        yield path, value


_FRAME = re.compile(
    r'^  File "(?P<file>[^"]+)", line (?P<line>\d+), in (?P<name>[^\n]+)\n(?:    (?P<code>[^\n]*)\n)?$'
)


def _cause_frames(cause: str) -> list[tuple[str, int, str, str]]:
    frames = []
    for element in json.loads(cause)["stackTrace"]:
        match = _FRAME.match(element)
        assert match, element
        frames.append((match["file"], int(match["line"]), match["name"], match["code"] or ""))
    return frames


def _copy(repo: Path, tmp_path: Path) -> Path:
    target = tmp_path / "sfn_etl"
    shutil.copytree(repo, target)
    return target.resolve()


# =========================================================================== layout and the example


def test_the_fixture_repository_layout() -> None:
    expected = {
        "README.md",
        "healer.yaml",
        "pyproject.toml",
        "template.yaml",
        "statemachine/sales_daily_etl.asl.json",
        "statemachine/customers_sync.asl.json",
        "statemachine/inventory_report.asl.json",
        "src/requirements.txt",
        "src/etl/__init__.py",
        "src/etl/common.py",
        "src/etl/sales.py",
        "src/etl/customers.py",
        "src/etl/inventory.py",
        "healer/contracts/sales_daily_etl.yaml",
        "healer/contracts/customers_sync.yaml",
        "healer/contracts/inventory_report.yaml",
        "fixtures/fake_llm/customers_sync_test.yaml",
        "fixtures/fake_llm/customers_sync_fix.yaml",
        "fixtures/fake_llm/inventory_report_test.yaml",
        "fixtures/fake_llm/inventory_report_fix.yaml",
    }
    for folder in ("sales", "customers", "inventory"):
        expected |= {f"functions/{folder}/app.py", f"functions/{folder}/requirements.txt"}
        expected |= {f"fixtures/day1/{folder}.csv", f"fixtures/day2/{folder}.csv"}
    assert _files(SFN_REPO) == expected


def test_the_example_shares_every_file_of_the_fixture_repository() -> None:
    own = {"README.md", "healer.yaml"}
    shared = _files(SFN_REPO) - own
    for relative in sorted(shared):
        assert (EXAMPLE / relative).read_bytes() == (SFN_REPO / relative).read_bytes(), relative
    assert _files(EXAMPLE) - shared == own | {
        ".gitignore",
        "events/eventbridge-rule.json",
        "scripts/start_executions.sh",
    }


def test_the_example_guide_script_and_rule() -> None:
    guide = (EXAMPLE / "README.md").read_text(encoding="utf-8")
    for text in (
        "sam build",
        "sam deploy --guided",
        "HEALER_SFN_LIVE=1",
        "scripts/start_executions.sh",
        "events/eventbridge-rule.json",
        "fixtures/day1/",
        "fixtures/day2/",
        "dag-healer contracts approve sales_daily_etl",
        "dag-healer worker --once",
    ):
        assert text in guide, text
    template = load_template(EXAMPLE / "template.yaml")
    script = EXAMPLE / "scripts" / "start_executions.sh"
    body = script.read_text(encoding="utf-8")
    assert body.startswith("#!/usr/bin/env bash\n") and "set -euo pipefail" in body
    assert os.access(script, os.X_OK)
    for output in re.findall(r"\$\(output (\w+)\)", body):
        assert output in template["Outputs"], output
    rule = json.loads((EXAMPLE / "events" / "eventbridge-rule.json").read_text(encoding="utf-8"))
    pattern = template["Resources"]["HealerEventsRule"]["Properties"]["EventPattern"]
    assert {key: rule[key] for key in ("source", "detail-type")} == {
        key: pattern[key] for key in ("source", "detail-type")
    }
    assert rule["detail"]["status"] == pattern["detail"]["status"] == ["FAILED", "TIMED_OUT"]
    assert sorted(arn.rsplit(":", 1)[-1] for arn in rule["detail"]["stateMachineArn"]) == sorted(
        case["machine"] for case in CASES.values()
    )


def test_no_credentials_or_other_account_ids() -> None:
    accounts = re.compile(r"(?<![\w.-])\d{12}(?![\w.-])")
    keys = re.compile(r"\b(?:AKIA|ASIA)[0-9A-Z]{16}\b|aws_secret_access_key", re.IGNORECASE)
    for root in (SFN_REPO, EXAMPLE, WORLD_DIR):
        for relative in _files(root):
            text = (root / relative).read_text(encoding="utf-8")
            assert set(accounts.findall(text)) <= {ACCOUNT}, relative
            assert not keys.search(text), relative
    world = WORLD_FILE.read_text(encoding="utf-8")
    assert set(accounts.findall(world)) <= {ACCOUNT}
    assert not keys.search(world)


# =========================================================================== configuration and contracts


def test_the_fixture_healer_yaml(monkeypatch: pytest.MonkeyPatch) -> None:
    from dag_healer.config import load_config_file
    from dag_healer.stepfunctions import FACTORY

    monkeypatch.delenv("HEALER_DATABASE_URL", raising=False)
    cfg = load_config_file(SFN_REPO / "healer.yaml")
    assert cfg.repo_root == SFN_REPO
    assert (cfg.repo.dags_dir, cfg.repo.allowed_paths) == ("statemachine", ["functions/", "src/"])
    assert (cfg.orchestrator.type, cfg.orchestrator.instance_id) == ("stepfunctions", INSTANCE)
    assert cfg.monitor.modes == ["poll", "queue"] and cfg.llm.provider == "fake"
    options = FACTORY.options_model.model_validate(cfg.orchestrator.options)
    assert (options.region, options.template, options.queue_url) == (REGION, "template.yaml", QUEUE_URL)
    assert {
        pipeline: {"state_machine": item.state_machine, "logical_id": item.logical_id}
        for pipeline, item in options.pipelines.items()
    } == PIPELINES
    assert {
        pipeline: {state: locator.path for state, locator in states.items()}
        for pipeline, states in cfg.datasets.items()
    } == PIPELINE_DATASETS
    assert all(locator.type == "csv" for states in cfg.datasets.values() for locator in states.values())
    assert cfg.secret_literals() == []


def test_the_example_healer_yaml(monkeypatch: pytest.MonkeyPatch) -> None:
    from dag_healer.config import load_config_file
    from dag_healer.stepfunctions import FACTORY

    for name in (
        "HEALER_LLM_PROVIDER",
        "HEALER_SFN_QUEUE_URL",
        "HEALER_SANDBOX_IMAGE",
        "GITHUB_REPO",
        "GITHUB_TOKEN",
    ):
        monkeypatch.delenv(name, raising=False)
    cfg = load_config_file(EXAMPLE / "healer.yaml")
    options = FACTORY.options_model.model_validate(cfg.orchestrator.options)
    assert {pipeline: item.logical_id for pipeline, item in options.pipelines.items()} == {
        pipeline: item["logical_id"] for pipeline, item in PIPELINES.items()
    }
    assert options.queue_url is None and cfg.llm.provider == "fake"
    assert cfg.secret_literals() == []
    fixture = load_config_file(SFN_REPO / "healer.yaml")
    assert cfg.datasets == fixture.datasets
    assert cfg.repo.model_dump(exclude={"path", "sandbox"}) == fixture.repo.model_dump(
        exclude={"path", "sandbox"}
    )
    # The example runs its tests in containers (the image comes from the environment); the
    # offline tests have no container engine.
    assert (cfg.repo.sandbox.backend, cfg.repo.sandbox.image) == ("container", None)
    assert fixture.repo.sandbox.backend == "process"
    assert cfg.repo_root == EXAMPLE.resolve()


#: pipeline -> {state: S3 locator} of healer.yaml (and of the contracts)
PIPELINE_DATASETS = {
    "sales_daily_etl": {"TransformSales": f"s3://{BUCKET}/incoming/sales.csv"},
    "customers_sync": {"TransformCustomers": f"s3://{BUCKET}/incoming/customers.csv"},
    "inventory_report": {"BuildReport": f"s3://{BUCKET}/incoming/inventory.csv"},
}


def _rows(pipeline: str) -> int:
    path = SFN_REPO / "fixtures" / "day1" / f"{CASES[pipeline]['folder']}.csv"
    return len(path.read_text(encoding="utf-8").splitlines()) - 1


def _infer_contracts(repo: Path) -> None:
    """The steps of ``dag-healer init --samples fixtures/day1``, with the adapter's code locator."""
    from dag_healer.cli.init_cmd import DagReport, dataset_candidates, profile_datasets
    from dag_healer.config import load_config_file
    from dag_healer.contracts.infer import infer_contract
    from dag_healer.contracts.io import contract_path, save_contract
    from dag_healer.stepfunctions.locator import SfnCodeLocator
    from dag_healer.stepfunctions.options import StepFunctionsOptions

    cfg = load_config_file(repo / "healer.yaml")
    locator = SfnCodeLocator(cfg, StepFunctionsOptions.model_validate(cfg.orchestrator.options))
    for dag in locator.scan(repo):
        candidates, unresolved = dataset_candidates(dag, dict(cfg.datasets.get(dag.dag_id, {})))
        report = DagReport(dag.dag_id, dag.file, len(dag.tasks), len(candidates), len(unresolved))
        datasets, sources = profile_datasets(
            candidates,
            repo_root=repo,
            samples_dir=repo / "fixtures" / "day1",
            pii_columns=list(cfg.masking.columns),
            report=report,
        )
        assert datasets and not report.problems, report.problems
        save_contract(
            infer_contract(dag.dag_id, datasets, inferred_from=sources),
            contract_path(cfg.contracts_dir, dag.dag_id),
        )


def test_the_contracts_are_dag_healers_own_inference_from_day_1(tmp_path: Path) -> None:
    from dag_healer.contracts.io import load_contract

    repo = _copy(SFN_REPO, tmp_path)
    shutil.rmtree(repo / "healer" / "contracts")
    _infer_contracts(repo)
    for pipeline in CASES:
        committed = SFN_REPO / "healer" / "contracts" / f"{pipeline}.yaml"
        assert (repo / "healer" / "contracts" / f"{pipeline}.yaml").read_text() == committed.read_text(), (
            pipeline
        )
        contract = load_contract(committed)
        assert contract.status == "draft" and not contract.is_approved
        (dataset,) = contract.datasets
        state = next(iter(PIPELINE_DATASETS[pipeline]))
        assert (dataset.task_id, dataset.locator.path) == (state, PIPELINE_DATASETS[pipeline][state])
        assert contract.inferred_from == [
            f"fixtures/day1/{CASES[pipeline]['folder']}.csv ({_rows(pipeline)} rows)"
        ]


def test_the_approved_contracts_tell_day_2_apart(tmp_path: Path) -> None:
    import pandas as pd

    from dag_healer.contracts.io import load_contract
    from dag_healer.contracts.validate import validate_contract

    outcomes = {}
    for pipeline, case in CASES.items():
        contract = load_contract(SFN_REPO / "healer" / "contracts" / f"{pipeline}.yaml")
        (dataset,) = contract.datasets
        day2 = pd.read_csv(SFN_REPO / "fixtures" / "day2" / f"{case['folder']}.csv")
        outcomes[pipeline] = sorted(
            {violation.rule for violation in validate_contract(contract, {dataset.name: day2})}
        )
    assert "missing_column" in outcomes["sales_daily_etl"]  # amount -> amt: DATA once approved
    assert outcomes["customers_sync"] == []  # the day-2 file is valid: the bug is in the code
    assert "missing_column" in outcomes["inventory_report"]  # qty -> quantity, but only a draft


def test_the_exports_are_those_of_example_existing_repo() -> None:
    for name in ("sales", "customers", "inventory"):
        assert (SFN_REPO / "fixtures" / "day1" / f"{name}.csv").read_bytes() == (
            EXISTING / "data" / "incoming" / f"{name}.csv"
        ).read_bytes()
        assert (SFN_REPO / "fixtures" / "day2" / f"{name}.csv").read_bytes() == (
            EXISTING / "fixtures" / "day2" / f"{name}.csv"
        ).read_bytes()


@pytest.mark.parametrize("module", ["sales", "customers", "inventory"])
def test_the_job_code_is_example_existing_repos(module: str) -> None:
    def functions(path: Path) -> dict[str, str]:
        tree = ast.parse(path.read_text(encoding="utf-8"))
        return {node.name: ast.dump(node) for node in tree.body if isinstance(node, ast.FunctionDef)}

    original = functions(EXISTING / "src" / "etl" / f"{module}.py")
    ours = functions(SFN_REPO / "src" / "etl" / f"{module}.py")
    assert original and set(original) == set(ours)
    for name, dump in original.items():
        assert ours[name] == dump, f"{module}.{name} differs from example_existing_repo"


# =========================================================================== the fake LLM scripts


@pytest.mark.parametrize(
    ("scenario", "expected"),
    [
        (
            {
                "dag_id": "customers_sync",
                "task_id": "TransformCustomers",
                "exception_type": "ValueError",
                "phase": "test",
            },
            "customers_sync_test",
        ),
        (
            {
                "dag_id": "customers_sync",
                "task_id": "TransformCustomers",
                "exception_type": "ValueError",
                "phase": "fix",
            },
            "customers_sync_fix",
        ),
        (
            {
                "dag_id": "inventory_report",
                "task_id": "BuildReport",
                "exception_type": "KeyError",
                "phase": "test",
            },
            "inventory_report_test",
        ),
        (
            {
                "dag_id": "inventory_report",
                "task_id": "BuildReport",
                "exception_type": "builtins.KeyError",
                "phase": "fix",
            },
            "inventory_report_fix",
        ),
        (
            {
                "dag_id": "sales_daily_etl",
                "task_id": "TransformSales",
                "exception_type": "KeyError",
                "phase": "fix",
            },
            None,
        ),
    ],
)
def test_the_fake_llm_scripts_match_the_states(scenario: dict[str, str], expected: str | None) -> None:
    from dag_healer.agent.fake import load_scripts, match_script

    scripts = load_scripts(SFN_REPO / "fixtures" / "fake_llm")
    assert len(scripts) == 4
    found = match_script(scripts, scenario)
    assert (found["name"] if found else None) == expected


def _pytest(repo: Path, test: str) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        [sys.executable, "-I", "-B", "-m", "pytest", "-q", "-p", "no:cacheprovider", test],
        cwd=repo,
        capture_output=True,
        text=True,
        timeout=300,
        check=False,
    )


@pytest.mark.parametrize("pipeline", ["customers_sync", "inventory_report"])
def test_the_fake_repairs_reproduce_and_fix_the_failures(tmp_path: Path, pipeline: str) -> None:
    import yaml

    repo = _copy(SFN_REPO, tmp_path)
    test_script = yaml.safe_load((repo / "fixtures" / "fake_llm" / f"{pipeline}_test.yaml").read_text())
    fix_script = yaml.safe_load((repo / "fixtures" / "fake_llm" / f"{pipeline}_fix.yaml").read_text())
    (written,) = [step["input"] for step in test_script["steps"] if step["tool"] == "write_file"]
    target = repo / written["path"]
    target.parent.mkdir(parents=True, exist_ok=True)
    target.write_text(written["content"], encoding="utf-8")
    before = _pytest(repo, written["path"])
    assert before.returncode == 1, before.stdout + before.stderr
    assert ("ValueError" if pipeline == "customers_sync" else "KeyError") in before.stdout
    for step in fix_script["steps"]:
        if step["tool"] != "replace_in_file":
            continue
        path = repo / step["input"]["path"]
        text = path.read_text(encoding="utf-8")
        assert text.count(step["input"]["old"]) == 1, step["input"]["old"]
        path.write_text(text.replace(step["input"]["old"], step["input"]["new"]), encoding="utf-8")
    after = _pytest(repo, written["path"])
    assert after.returncode == 0, after.stdout + after.stderr


# =========================================================================== day 2, for real


def _scheduled_payloads(execution: dict[str, Any]) -> list[dict[str, Any]]:
    payloads = []
    for event in execution["history"]:
        if event["type"] == "TaskScheduled":
            payloads.append(json.loads(event["taskScheduledEventDetails"]["parameters"])["Payload"])
        elif event["type"] == "LambdaFunctionScheduled":
            payloads.append(json.loads(event["lambdaFunctionScheduledEventDetails"]["input"]))
    return payloads


def _run_steps(repo: Path, folder: str, events: list[dict[str, Any]], tmp_path: Path) -> list[dict[str, Any]]:
    runner = tmp_path / "runner.py"
    runner.write_text(RUNNER, encoding="utf-8")
    result = subprocess.run(
        [
            sys.executable,
            "-I",
            "-B",
            str(runner),
            str(repo),
            folder,
            json.dumps(events),
            str(repo / "fixtures" / "day2"),
        ],
        capture_output=True,
        text=True,
        timeout=300,
        check=False,
    )
    assert result.returncode == 0, result.stderr
    return json.loads(result.stdout)


def _lambda_path(repo: Path, folder: str, file: str) -> str | None:
    """A local frame of the copied repository as Lambda shows it (``None`` for other files)."""
    real = os.path.realpath(file)
    handler_root = str(repo / "functions" / folder) + os.sep
    layer_root = str(repo / "src") + os.sep
    if real.startswith(handler_root):
        return "/var/task/" + real[len(handler_root) :].replace(os.sep, "/")
    if real.startswith(layer_root):
        return "/opt/python/" + real[len(layer_root) :].replace(os.sep, "/")
    return None


@pytest.mark.parametrize("pipeline", sorted(CASES))
def test_day_2_fails_as_the_world_says_and_its_frames_match_the_code(tmp_path: Path, pipeline: str) -> None:
    case = CASES[pipeline]
    execution = _execution(_world(), pipeline)
    repo = _copy(SFN_REPO, tmp_path)
    results = _run_steps(repo, case["folder"], _scheduled_payloads(execution), tmp_path)
    assert [("error" in result) for result in results] == [False] * (len(results) - 1) + [True]
    error = results[-1]["error"]
    assert (error["type"], error["message"]) == case["error"]
    payload = json.loads(execution["cause"])
    assert (payload["errorType"], payload["errorMessage"]) == case["error"]
    assert execution["error"] == case["error"][0]
    ran = [
        (_lambda_path(repo, case["folder"], file), line, name, code or "")
        for file, line, name, code in error["frames"]
        if _lambda_path(repo, case["folder"], file)
    ]
    recorded = [
        frame
        for frame in _cause_frames(execution["cause"])
        if frame[0].startswith(("/var/task/", "/opt/python/etl/"))
    ]
    assert recorded == ran
    assert recorded[0][:3] == ("/var/task/app.py", recorded[0][1], "handler")
    assert recorded[-1][0] == case["module"]
    messages = [event["message"] for group in _world()["log_events"] for event in group["events"]]
    for line in (line for result in results for line in result["lines"]):
        assert any(line in message or json.dumps(line)[1:-1] in message for message in messages), line


# =========================================================================== the world scenario


def test_the_world_definitions_are_the_repository_asl_with_the_deployed_arns() -> None:
    world = _world()
    machines = {item["name"]: item for item in world["state_machines"]}
    assert set(machines) == {case["machine"] for case in CASES.values()}
    for case in CASES.values():
        repo = asl.load_definition((SFN_REPO / "statemachine" / case["asl"]).read_text(encoding="utf-8"))
        expected = asl.substitute(repo, {case["substitution"]: _function_arn(case["folder"])})
        item = machines[case["machine"]]
        assert item["definition"] == expected
        assert item["type"] == "STANDARD"
        assert item["role_arn"].startswith(f"arn:aws:iam::{ACCOUNT}:role/sfn-etl-")


def test_the_world_histories_fit_the_api() -> None:
    import botocore.session
    from botocore.validate import ParamValidator

    shape = (
        botocore.session.get_session()
        .get_service_model("stepfunctions")
        .operation_model("GetExecutionHistory")
        .output_shape
    )
    for execution in _world()["executions"]:
        events = copy.deepcopy(execution["history"])
        for event in events:
            event["timestamp"] = datetime.fromisoformat(event["timestamp"].replace("Z", "+00:00"))
        report = ParamValidator().validate({"events": events}, shape)
        assert not report.has_errors(), report.generate_report()


def _moment(text: str) -> datetime:
    return datetime.fromisoformat(text.replace("Z", "+00:00"))


@pytest.mark.parametrize("pipeline", sorted(CASES))
def test_the_world_histories_are_consistent(pipeline: str) -> None:
    case = CASES[pipeline]
    world = _world()
    execution = _execution(world, pipeline)
    machine = next(item for item in world["state_machines"] if item["name"] == case["machine"])
    states = asl.index_states(machine["definition"])
    history = execution["history"]
    assert execution["state_machine"] == case["machine"] and execution["status"] == "FAILED"
    assert execution["redrive_count"] == 0
    assert [event["id"] for event in history] == list(range(1, len(history) + 1))
    assert all(0 <= event["previousEventId"] < event["id"] for event in history)
    moments = [_moment(event["timestamp"]) for event in history]
    assert moments == sorted(moments)
    assert moments[0] == _moment(execution["start_date"]) and moments[-1] == _moment(execution["stop_date"])
    assert _moment(execution["stop_date"]) - _moment(execution["start_date"]) < timedelta(minutes=1)
    assert _moment(execution["stop_date"]) < NOW
    assert history[0]["type"] == "ExecutionStarted"
    assert history[0]["executionStartedEventDetails"]["input"] == execution["input"]
    terminal = history[-1]
    assert terminal["type"] == "ExecutionFailed"
    assert terminal["executionFailedEventDetails"] == {
        "error": execution["error"],
        "cause": execution["cause"],
    }
    entered = [
        event["stateEnteredEventDetails"]["name"]
        for event in history
        if event["type"].endswith("StateEntered")
    ]
    assert entered and set(entered) <= set(states)
    by_id = {event["id"]: event for event in history}
    failed = next(event for event in history if event["type"] in ("TaskFailed", "LambdaFunctionFailed"))
    cursor = failed
    while not cursor["type"].endswith("StateEntered"):
        cursor = by_id[cursor["previousEventId"]]
    assert cursor["stateEnteredEventDetails"]["name"] == case["state"]
    details = failed.get("taskFailedEventDetails") or failed["lambdaFunctionFailedEventDetails"]
    assert (details["error"], details["cause"]) == (execution["error"], execution["cause"])
    scheduled = [event for event in history if event["type"] in ("TaskScheduled", "LambdaFunctionScheduled")]
    for event in scheduled:
        if event["type"] == "TaskScheduled":
            details = event["taskScheduledEventDetails"]
            assert (details["resourceType"], details["resource"], details["region"]) == (
                "lambda",
                "invoke",
                REGION,
            )
            assert json.loads(details["parameters"])["FunctionName"] == _function_arn(case["folder"])
        else:
            assert event["lambdaFunctionScheduledEventDetails"]["resource"] == _function_arn(case["folder"])
    if pipeline == "inventory_report":
        types = [event["type"] for event in history]
        assert (
            types.index("MapIterationStarted")
            < types.index("LambdaFunctionFailed")
            < types.index("MapIterationFailed")
        )
        iteration = next(event for event in history if event["type"] == "MapIterationFailed")
        assert iteration["mapIterationFailedEventDetails"] == {"name": "PerWarehouse", "index": 0}


def test_the_world_functions_logs_objects_and_queue() -> None:
    world = _world()
    assert (world["region"], world["account"], _moment(world["now"])) == (REGION, ACCOUNT, NOW)
    template = load_template(SFN_REPO / "template.yaml")
    globals_function = template["Globals"]["Function"]
    functions = {item["name"]: item for item in world["functions"]}
    assert set(functions) == set(FUNCTIONS.values())
    for folder, name in FUNCTIONS.items():
        item = functions[name]
        assert (item["handler"], item["runtime"]) == (
            globals_function["Handler"],
            globals_function["Runtime"],
        )
        json_logs = folder == "inventory"
        assert item["log_format"] == ("JSON" if json_logs else "Text")
        assert item["log_group"] == ("/sfn-etl/inventory" if json_logs else None)
    groups = {item["group"]: item for item in world["log_events"]}
    assert set(groups) == {
        f"/aws/lambda/{FUNCTIONS['sales']}",
        f"/aws/lambda/{FUNCTIONS['customers']}",
        "/sfn-etl/inventory",
    }
    for pipeline, case in CASES.items():
        execution = _execution(world, pipeline)
        request_id = json.loads(execution["cause"])["requestId"]
        group = (
            "/sfn-etl/inventory"
            if case["folder"] == "inventory"
            else f"/aws/lambda/{FUNCTIONS[case['folder']]}"
        )
        messages = [event["message"] for event in groups[group]["events"]]
        assert any(
            request_id in message and ("START" in message or "platform.start" in message)
            for message in messages
        )
        moments = [_moment(event["timestamp"]) for event in groups[group]["events"]]
        assert moments == sorted(moments)
        assert _moment(execution["start_date"]) <= moments[0] and moments[-1] <= _moment(
            execution["stop_date"]
        )
    for item in world["objects"]:
        assert item["bucket"] == BUCKET
        name = item["key"].removeprefix("incoming/")
        assert (WORLD_FILE.parent / item["file"]).read_bytes() == (
            SFN_REPO / "fixtures" / "day2" / name
        ).read_bytes()
    assert sorted(item["key"] for item in world["objects"]) == [
        "incoming/customers.csv",
        "incoming/inventory.csv",
        "incoming/sales.csv",
    ]
    (queue,) = world["queues"]
    assert queue["url"] == QUEUE_URL
    (message,) = queue["messages"]
    envelope = json.loads((WORLD_FILE.parent / message["body_file"]).read_text(encoding="utf-8"))
    customers = _execution(world, "customers_sync")
    machine_arn = PIPELINES["customers_sync"]["state_machine"]
    assert (envelope["source"], envelope["detail-type"]) == (
        "aws.states",
        "Step Functions Execution Status Change",
    )
    detail = envelope["detail"]
    assert detail["stateMachineArn"] == machine_arn and detail["name"] == customers["name"]
    assert (
        detail["executionArn"]
        == f"arn:aws:states:{REGION}:{ACCOUNT}:execution:customers-sync:{customers['name']}"
    )
    assert envelope["resources"] == [detail["executionArn"]]
    assert (detail["status"], detail["redriveCount"]) == ("FAILED", 0)
    assert (detail["error"], detail["cause"], detail["input"]) == (
        customers["error"],
        customers["cause"],
        customers["input"],
    )
    epoch = datetime(1970, 1, 1, tzinfo=timezone.utc)
    for key, value in (("startDate", customers["start_date"]), ("stopDate", customers["stop_date"])):
        assert detail[key] == (_moment(value) - epoch) // timedelta(milliseconds=1)


#: Where execution data lives: the seeded address may appear there and nowhere else.
_EXECUTION_DATA = re.compile(
    r"(^executions\[\d+\]\.input$)"
    r"|(\.executionStartedEventDetails\.input$)"
    r"|(\.stateEnteredEventDetails\.input$)"
    r"|(\.stateExitedEventDetails\.output$)"
    r"|(\.taskScheduledEventDetails\.parameters$)"
    r"|(\.lambdaFunctionScheduledEventDetails\.input$)"
    r"|(^log_events\[\d+\]\.events\[\d+\]\.message$)"
)


def test_the_seeded_address_only_appears_in_execution_data() -> None:
    world = _world()
    found = [path for path, text in _strings(world) if PII in text]
    assert found, "the customers execution carries the seeded address"
    assert [path for path in found if not _EXECUTION_DATA.search(path)] == []
    customers = _execution(world, "customers_sync")
    assert PII in customers["input"] and PII not in customers["cause"]
    assert any(PII in path_text for _path, path_text in _strings(world["log_events"]))
    envelope = json.loads((WORLD_DIR / "sqs" / "customers-sync-failed.json").read_text(encoding="utf-8"))
    assert [path for path, text in _strings(envelope) if PII in text] == ["detail.input"]


def test_offline_aws_loads_the_world() -> None:
    from dag_healer.stepfunctions.testing import OfflineAws

    world = OfflineAws.load(WORLD_FILE)
    assert world.io_calls == 0
    sfn = world.client_factory("stepfunctions")
    for pipeline, case in CASES.items():
        execution_arn = f"arn:aws:states:{REGION}:{ACCOUNT}:execution:{case['machine']}:{case['execution']}"
        described = sfn.describe_execution(executionArn=execution_arn)
        assert described["status"] == "FAILED" and described["error"] == case["error"][0], pipeline
        history = sfn.get_execution_history(executionArn=execution_arn, maxResults=1000)
        assert history["events"][-1]["type"] == "ExecutionFailed"
    logs = world.client_factory("logs")
    assert logs.filter_log_events(logGroupName="/sfn-etl/inventory")["events"]
    body = world.client_factory("s3").get_object(Bucket=BUCKET, Key="incoming/sales.csv")["Body"].read()
    assert body == (SFN_REPO / "fixtures" / "day2" / "sales.csv").read_bytes()
    received = world.client_factory("sqs").receive_message(QueueUrl=QUEUE_URL, MaxNumberOfMessages=10)
    assert len(received.get("Messages", [])) == 1
