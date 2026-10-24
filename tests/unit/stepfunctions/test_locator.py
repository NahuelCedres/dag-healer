"""locator: the Step Functions CodeLocator on the fixture repository and on built repositories.

The fixture repository (``tests/unit/fixtures/repos/sfn_etl``) is the main case, with the
tracebacks of the offline world mapped through ``path_mappings``; small repositories built in
``tmp_path`` cover the handler forms, ``code_map``, the machine matching rules, the dataset
references and the odd inputs.
"""

from __future__ import annotations

import copy
import json
import logging
import os
import re
import shutil
import subprocess
import sys
from collections.abc import Mapping
from pathlib import Path
from typing import Any

import pytest

from dag_healer.core.evidence import PipelineSource
from dag_healer.core.specs import DagInfo, DatasetRef, PathMapping, TaskInfo
from dag_healer.orchestrators.conformance import map_frame, no_network
from dag_healer.stepfunctions.locator import (
    LAMBDA_LAYER_ROOT,
    LAMBDA_TASK_ROOT,
    MAX_DRIFT_NOTES,
    SfnCodeLocator,
    split_handler,
)
from dag_healer.stepfunctions.options import StepFunctionsOptions
from dag_healer.stepfunctions.runtime import PipelineEntry, PipelineIndex

from .conftest import FIXTURES, REGION, SFN_REPO, sfn_options

ARN = f"arn:aws:states:{REGION}:123456789012:stateMachine:"
INVOKE = "arn:aws:states:::lambda:invoke"
WORLD = FIXTURES / "world" / "sfn_etl.json"
#: pipeline -> (handler folder, failing state, module of the innermost repository frame)
CASES = {
    "sales_daily_etl": ("sales", "TransformSales", "src/etl/sales.py"),
    "customers_sync": ("customers", "TransformCustomers", "src/etl/customers.py"),
    "inventory_report": ("inventory", "BuildReport", "src/etl/inventory.py"),
}


# =========================================================================== helpers


def _locator(pipelines: Mapping[str, Mapping[str, Any]], **options: Any) -> SfnCodeLocator:
    data = {"region": REGION, "pipelines": dict(pipelines), **options}
    return SfnCodeLocator(None, StepFunctionsOptions.model_validate(data))


def _write(root: Path, files: Mapping[str, str]) -> Path:
    for relative, text in files.items():
        path = root / relative
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(text, encoding="utf-8")
    return root


def _task(name: str, function: str) -> dict[str, Any]:
    return {"Type": "Task", "Resource": INVOKE, "Parameters": {"FunctionName": function, "Payload.$": "$"}}


def _machine(*states: tuple[str, Mapping[str, Any]]) -> str:
    """A definition chaining ``states`` in order."""
    names = [name for name, _state in states]
    body: dict[str, Any] = {}
    for index, (name, state) in enumerate(states):
        item = dict(state)
        if item.get("Type") not in ("Succeed", "Fail"):
            if index + 1 < len(names):
                item["Next"] = names[index + 1]
            else:
                item["End"] = True
        body[name] = item
    return json.dumps({"StartAt": names[0], "States": body}, indent=2)


def _line(path: Path, needle: str) -> int:
    """The 1-based line of the first occurrence of ``needle`` in a file."""
    for number, text in enumerate(path.read_text(encoding="utf-8").splitlines(), start=1):
        if needle in text:
            return number
    raise AssertionError(f"{needle!r} not in {path}")


ONE_FUNCTION = """\
Resources:
  Fn:
    Type: AWS::Serverless::Function
    Properties:
      CodeUri: code/
      Handler: {handler}
      Runtime: {runtime}
      Layers: [!Ref Shared]
  Shared:
    Type: AWS::Serverless::LayerVersion
    Properties: {{ContentUri: shared/}}
    Metadata: {{BuildMethod: python3.11}}
  Machine:
    Type: AWS::Serverless::StateMachine
    Properties:
      DefinitionUri: machine.asl.json
      DefinitionSubstitutions:
        FnArn: !GetAtt Fn.Arn
"""
ONE_PIPELINE = {"p": {"state_machine": ARN + "Machine-AbC123", "logical_id": "Machine"}}


def _one_function_repo(
    root: Path, files: Mapping[str, str], *, handler: str, runtime: str = "python3.11"
) -> Path:
    return _write(
        root,
        {
            "template.yaml": ONE_FUNCTION.format(handler=handler, runtime=runtime),
            "machine.asl.json": _machine(("Run", _task("Run", "${FnArn}"))),
            **files,
        },
    )


# =========================================================================== the fixture repository


def _def_line(relative: str) -> int:
    return _line(SFN_REPO / relative, "def handler(")


def test_scan_finds_the_three_pipelines_of_the_fixture_repository() -> None:
    specs = SfnCodeLocator(None, sfn_options()).scan(SFN_REPO)
    assert all(isinstance(spec, DagInfo) for spec in specs)
    assert [(spec.dag_id, spec.file) for spec in specs] == [
        ("sales_daily_etl", "statemachine/sales_daily_etl.asl.json"),
        ("customers_sync", "statemachine/customers_sync.asl.json"),
        ("inventory_report", "statemachine/inventory_report.asl.json"),
    ]
    states = {
        "sales_daily_etl": ("sales", ["ExtractSales", "TransformSales", "LoadSales"]),
        "customers_sync": ("customers", ["ExtractCustomers", "TransformCustomers", "LoadCustomers"]),
        "inventory_report": ("inventory", ["GetInventory", "BuildReport", "PublishReport"]),
    }
    for spec in specs:
        folder, names = states[spec.dag_id]
        handler = f"functions/{folder}/app.py"
        assert spec.tasks == [
            TaskInfo(
                task_id=name,
                operator="lambda",
                callable_name="handler",
                callable_module="app",
                callable_file=handler,
                lineno=_def_line(handler),
            )
            for name in names
        ]
        assert spec.datasets == []  # the inputs are healer.yaml locators (built at run time in the code)
        assert spec.imports == ["etl", f"etl.{folder}"]


def test_the_fixture_repository_is_fully_mapped() -> None:
    locator = SfnCodeLocator(None, sfn_options())
    assert locator.unmapped_states(SFN_REPO) == {}
    assert locator.missing_pipelines(SFN_REPO) == []


def test_the_log_groups_the_fixture_template_sets() -> None:
    """``InventoryFunction`` logs to ``/sfn-etl/inventory`` (``LoggingConfig``); the others to the
    default ``/aws/lambda/<name>`` group, which the template does not name."""
    locator = SfnCodeLocator(None, sfn_options())
    with no_network():
        assert locator.log_group(SFN_REPO, "inventory_report", "BuildReport") == "/sfn-etl/inventory"
        assert locator.log_group(SFN_REPO, "inventory_report", "GetInventory") == "/sfn-etl/inventory"
        assert locator.log_group(SFN_REPO, "customers_sync", "TransformCustomers") is None
        assert locator.log_group(SFN_REPO, "customers_sync", "NoSuchState") is None
        assert locator.log_group(SFN_REPO, "not_watched", "BuildReport") is None
        assert locator.log_groups(SFN_REPO) == ["/sfn-etl/inventory"]


def test_log_groups_of_an_odd_repository(tmp_path: Path) -> None:
    root = _one_function_repo(
        tmp_path / "repo", {"code/app.py": "def handler(e, c):\n    pass\n"}, handler="app.handler"
    )
    locator = _locator(ONE_PIPELINE)
    assert locator.log_groups(root) == [] and locator.log_group(root, "p", "Run") is None
    (root / "template.yaml").write_text("Resources: [not, a, mapping", encoding="utf-8")
    assert locator.log_groups(root) == [] and locator.log_group(root, "p", "Run") is None
    assert _locator(ONE_PIPELINE).log_groups(tmp_path / "missing") == []


def test_an_invalid_template_is_reported_not_raised(tmp_path: Path, caplog: pytest.LogCaptureFixture) -> None:
    """Scanning goes on without the template (nothing is found); ``template_problem`` names the
    file and the line for the doctor, and the log says it once, without the template's text."""
    caplog.set_level(logging.DEBUG, logger="dag_healer.stepfunctions.locator")
    root = _one_function_repo(
        tmp_path / "repo", {"code/app.py": "def handler(e, c):\n    pass\n"}, handler="app.handler"
    )
    locator = _locator(ONE_PIPELINE)
    assert locator.template_problem(root) is None
    (root / "template.yaml").write_text("Resources:\n  Fn: [unclosed, secret-token\n", encoding="utf-8")
    assert locator.scan(root) == [] and locator.missing_pipelines(root) == ["p"]
    locator.scan(root)
    problem = locator.template_problem(root)
    assert problem is not None and problem.startswith("template.yaml: invalid template: ")
    assert "(line 3)" in problem and "secret-token" not in problem and str(tmp_path) not in problem
    warnings = [record for record in caplog.records if "cannot be read" in record.getMessage()]
    assert [record.levelno for record in warnings][:2] == [logging.WARNING, logging.DEBUG]
    assert not [record for record in caplog.records if "secret-token" in record.getMessage()]
    assert _locator(ONE_PIPELINE).template_problem(tmp_path / "missing") is None  # no template: no problem
    assert _locator(ONE_PIPELINE, template=None).template_problem(root) is None


def test_scan_is_offline_and_never_imports_the_repository_code() -> None:
    locator = SfnCodeLocator(None, sfn_options())
    before = set(sys.modules)
    with no_network():
        locator.scan(SFN_REPO)
        locator.path_mappings(SFN_REPO, None)
        locator.pipeline_file(SFN_REPO, "customers_sync", None)
        locator.unmapped_states(SFN_REPO)
    loaded = set(sys.modules) - before
    assert not [name for name in loaded if name in ("app", "etl") or name.startswith("etl.")]


@pytest.mark.parametrize(
    ("pipeline", "state", "folder"),
    [
        ("sales_daily_etl", "ExtractSales", "sales"),
        ("sales_daily_etl", "TransformSales", "sales"),
        ("customers_sync", "TransformCustomers", "customers"),
        ("customers_sync", "LoadCustomers", "customers"),
        ("inventory_report", "GetInventory", "inventory"),
        ("inventory_report", "BuildReport", "inventory"),
    ],
)
def test_path_mappings_of_a_step(pipeline: str, state: str, folder: str) -> None:
    """``/var/task`` is the function's code root (its handler is a top-level module); the layer
    ``src/`` holds only the ``etl`` package: ``sam build`` installs pandas next to it, so only
    ``/opt/python/etl`` maps (a pandas frame must not map to ``src/pandas/...``)."""
    locator = SfnCodeLocator(None, sfn_options())
    spec = next(spec for spec in locator.scan(SFN_REPO) if spec.dag_id == pipeline)
    step = spec.task(state)
    assert step is not None
    assert locator.path_mappings(SFN_REPO, step) == [
        PathMapping(LAMBDA_TASK_ROOT, f"functions/{folder}"),
        PathMapping(f"{LAMBDA_LAYER_ROOT}/etl", "src/etl"),
    ]


def test_path_mappings_without_a_step_leave_out_what_the_functions_map_differently() -> None:
    """Three functions map ``/var/task`` to three code roots: which one ran is unknown, so the
    prefix is left to the core's suffix match (which checks the file exists); the shared layer
    maps the same for all."""
    locator = SfnCodeLocator(None, sfn_options())
    assert locator.path_mappings(SFN_REPO, None) == [PathMapping("/opt/python/etl", "src/etl")]


def test_path_mappings_of_a_step_the_scan_does_not_know() -> None:
    locator = SfnCodeLocator(None, sfn_options())
    derived = TaskInfo(
        "Elsewhere", "lambda", callable_module="app", callable_file="functions/customers/app.py"
    )
    assert locator.path_mappings(SFN_REPO, derived) == [
        PathMapping("/var/task", "functions/customers"),
        PathMapping("/opt/python/etl", "src/etl"),
    ]
    other = TaskInfo(
        "TransformCustomers", "lambda", callable_module="pkg.app", callable_file="lambdas/x/pkg/app.py"
    )
    assert locator.path_mappings(SFN_REPO, other) == []  # lambdas/x is not in the repository
    at_root = TaskInfo("Elsewhere", "lambda", callable_module="app", callable_file="app.py")
    found = locator.path_mappings(SFN_REPO, at_root)  # the repository root has no module of its own
    assert PathMapping("/var/task", "") not in found
    assert {PathMapping("/var/task/src", "src"), PathMapping("/var/task/functions", "functions")} <= set(
        found
    )
    assert all(mapping.runtime_prefix.startswith("/var/task/") for mapping in found)
    unknown = TaskInfo("Elsewhere", "lambda")
    assert locator.path_mappings(SFN_REPO, unknown) == locator.path_mappings(SFN_REPO, None)
    mismatched = TaskInfo(
        "Elsewhere", "lambda", callable_module="other", callable_file="functions/sales/app.py"
    )
    assert locator.path_mappings(SFN_REPO, mismatched) == locator.path_mappings(SFN_REPO, None)


def _stack_files(cause: str) -> list[str]:
    files = []
    for element in json.loads(cause)["stackTrace"]:
        match = re.match(r'\s*File "(?P<file>[^"]+)", line \d+, in ', element)
        assert match, element
        files.append(match.group("file"))
    return files


@pytest.mark.parametrize("pipeline", sorted(CASES))
def test_the_world_tracebacks_map_into_the_repository(pipeline: str) -> None:
    folder, state, module = CASES[pipeline]
    world = json.loads(WORLD.read_text(encoding="utf-8"))
    execution = next(item for item in world["executions"] if item["name"].startswith(folder))
    locator = SfnCodeLocator(None, sfn_options())
    spec = next(spec for spec in locator.scan(SFN_REPO) if spec.dag_id == pipeline)
    step = spec.task(state)
    assert step is not None and step.callable_file == f"functions/{folder}/app.py"
    mappings = [PathMapping(SFN_REPO.as_posix(), ""), *locator.path_mappings(SFN_REPO, step)]
    mapped = [map_frame(file, mappings) for file in _stack_files(execution["cause"])]
    existing = [path for path in mapped if path and (SFN_REPO / path).is_file()]
    assert existing[0] == f"functions/{folder}/app.py"
    assert existing[-1] == module
    assert set(existing) == {f"functions/{folder}/app.py", module}  # pandas and stdlib frames stay out


@pytest.mark.parametrize("pipeline", sorted(CASES))
def test_no_world_frame_maps_to_a_file_the_repository_does_not_have(pipeline: str) -> None:
    """The core takes the first prefix that matches and does not check that the file exists (the
    ``map_frame`` rule): the pandas frames under ``/opt/python`` must map to nothing, so the
    innermost repository frame is the module of the failing state."""
    folder, state, module = CASES[pipeline]
    world = json.loads(WORLD.read_text(encoding="utf-8"))
    execution = next(item for item in world["executions"] if item["name"].startswith(folder))
    locator = SfnCodeLocator(None, sfn_options())
    step = next(spec for spec in locator.scan(SFN_REPO) if spec.dag_id == pipeline).task(state)
    files = _stack_files(execution["cause"])
    assert any(file.startswith("/opt/python/pandas/") for file in files) or pipeline == "customers_sync"
    for mappings in (locator.path_mappings(SFN_REPO, step), locator.path_mappings(SFN_REPO, None)):
        mapped = [map_frame(file, [PathMapping(SFN_REPO.as_posix(), ""), *mappings]) for file in files]
        assert all((SFN_REPO / path).is_file() for path in mapped if path), mapped
        assert [path for path in mapped if path][-1] == module


def test_pipeline_file_of_the_fixture_repository() -> None:
    locator = SfnCodeLocator(None, sfn_options())
    assert locator.pipeline_file(SFN_REPO, "sales_daily_etl", None) == "statemachine/sales_daily_etl.asl.json"
    assert locator.pipeline_file(SFN_REPO, "nope", None) is None
    reported = PipelineSource(
        path=ARN + "customers-sync", origin="api:DescribeStateMachineForExecution", language="asl-json"
    )
    assert (
        locator.pipeline_file(SFN_REPO, "customers_sync", reported) == "statemachine/customers_sync.asl.json"
    )


@pytest.mark.parametrize(
    ("source", "expected"),
    [
        (
            PipelineSource(relative_path="statemachine/sales_daily_etl.asl.json"),
            "statemachine/sales_daily_etl.asl.json",
        ),
        (PipelineSource(path="./template.yaml"), "template.yaml"),
        (
            PipelineSource(origin="repo:statemachine/inventory_report.asl.json"),
            "statemachine/inventory_report.asl.json",
        ),
        (
            PipelineSource(relative_path="statemachine/missing.asl.json"),
            "statemachine/customers_sync.asl.json",
        ),
        (PipelineSource(path="../outside.asl.json"), "statemachine/customers_sync.asl.json"),
        (PipelineSource(path="/var/task/app.py"), "statemachine/customers_sync.asl.json"),
        (PipelineSource(path="statemachine"), "statemachine/customers_sync.asl.json"),
    ],
)
def test_pipeline_file_prefers_a_reported_repository_file(source: PipelineSource, expected: str) -> None:
    assert SfnCodeLocator(None, sfn_options()).pipeline_file(SFN_REPO, "customers_sync", source) == expected


# =========================================================================== drift


def _repo_definition(name: str) -> dict[str, Any]:
    return json.loads((SFN_REPO / "statemachine" / name).read_text(encoding="utf-8"))


def test_drift_is_empty_for_the_definition_that_was_deployed() -> None:
    locator = SfnCodeLocator(None, sfn_options())
    world = json.loads(WORLD.read_text(encoding="utf-8"))
    for machine, pipeline in (
        ("SalesDailyEtlStateMachine-AbC123", "sales_daily_etl"),
        ("customers-sync", "customers_sync"),
        ("InventoryReportStateMachine-XyZ789", "inventory_report"),
    ):
        deployed = next(item["definition"] for item in world["state_machines"] if item["name"] == machine)
        assert locator.drift(SFN_REPO, pipeline, deployed) == []
        assert locator.drift(SFN_REPO, pipeline, json.dumps(deployed)) == []


def test_drift_notes_added_removed_retyped_and_moved_states() -> None:
    locator = SfnCodeLocator(None, sfn_options())
    where = "statemachine/customers_sync.asl.json"
    added = _repo_definition("customers_sync.asl.json")
    added["States"]["Audit"] = {"Type": "Pass", "End": True}
    assert locator.drift(SFN_REPO, "customers_sync", added) == [
        f"state 'Audit' is in the deployed definition but not in {where}"
    ]
    removed = _repo_definition("customers_sync.asl.json")
    del removed["States"]["LoadCustomers"]
    assert locator.drift(SFN_REPO, "customers_sync", removed) == [
        f"state 'LoadCustomers' is in {where} but not in the deployed definition"
    ]
    retyped = _repo_definition("customers_sync.asl.json")
    retyped["States"]["LoadCustomers"] = {"Type": "Pass", "End": True}
    assert locator.drift(SFN_REPO, "customers_sync", retyped) == [
        f"state 'LoadCustomers' is a Pass state in the deployed definition but a Task state in {where}"
    ]
    moved = _repo_definition("inventory_report.asl.json")
    inner = moved["States"]["PerWarehouse"]["ItemProcessor"]
    inner["States"] = {"Inner": {"Type": "Pass", "End": True}}
    inner["StartAt"] = "Inner"
    moved["States"]["BuildReport"] = {"Type": "Task", "Resource": "${InventoryFunctionArn}", "End": True}
    assert locator.drift(SFN_REPO, "inventory_report", moved) == [
        "state 'Inner' is in the deployed definition but not in statemachine/inventory_report.asl.json",
        "state 'BuildReport' is at the top level in the deployed definition but inside PerWarehouse in "
        "statemachine/inventory_report.asl.json",
    ]


def test_drift_caps_its_notes() -> None:
    deployed = _repo_definition("sales_daily_etl.asl.json")
    for index in range(30):
        deployed["States"][f"Extra{index:02d}"] = {"Type": "Pass", "End": True}
    notes = SfnCodeLocator(None, sfn_options()).drift(SFN_REPO, "sales_daily_etl", deployed)
    assert len(notes) == MAX_DRIFT_NOTES
    assert notes[-1] == f"... and {30 - (MAX_DRIFT_NOTES - 1)} more differences"


def test_drift_when_a_side_cannot_be_read(tmp_path: Path) -> None:
    locator = SfnCodeLocator(None, sfn_options())
    assert locator.drift(SFN_REPO, "customers_sync", "{not a definition") == [
        "the deployed definition could not be read"
    ]
    assert locator.drift(SFN_REPO, "nope", _repo_definition("customers_sync.asl.json")) == [
        "no definition of pipeline nope was found in the repository"
    ]
    empty = _write(tmp_path / "empty", {"template.yaml": "Resources: {}\n"})
    assert locator.drift(empty, "customers_sync", {}) == [
        "no definition of pipeline customers_sync was found in the repository"
    ]


# =========================================================================== handlers


@pytest.mark.parametrize(
    ("handler", "files", "expected", "mapped"),
    [
        (
            "app.handler",
            {"code/app.py": "import os\n\n\ndef handler(event, context):\n    return event\n"},
            ("handler", "app", "code/app.py", 4),
            True,
        ),
        (
            "pkg.mod.run",
            {"code/pkg/__init__.py": "", "code/pkg/mod.py": "def run(event, context):\n    return 1\n"},
            ("run", "pkg.mod", "code/pkg/mod.py", 1),
            True,
        ),
        (
            "handlers/sales.main",
            {"code/handlers/sales.py": "\n\nasync def main(event, context):\n    return 2\n"},
            ("main", "handlers.sales", "code/handlers/sales.py", 3),
            True,
        ),
        (
            "pkg.handler",
            {"code/pkg/__init__.py": "def handler(event, context):\n    return 3\n"},
            ("handler", "pkg", "code/pkg/__init__.py", 1),
            True,
        ),
        (
            "etl.entry.handler",
            {
                "shared/etl/__init__.py": "",
                "shared/etl/entry.py": "def handler(event, context):\n    return 4\n",
            },
            ("handler", "etl.entry", "shared/etl/entry.py", 1),
            True,
        ),
        (
            "app.handler",
            {
                "code/app.py": "import functools\n\n\n@functools.lru_cache\n"
                "def handler(event, context):\n    return 5\n"
            },
            ("handler", "app", "code/app.py", 5),
            True,
        ),
        (
            "app.handler",
            {"code/app.py": "def _impl(event, context):\n    return 6\n\n\nhandler = _impl\n"},
            ("handler", "app", "code/app.py", 5),
            True,
        ),
        (
            "app.handler",
            {
                "code/app.py": "from impl import run as handler\n",
                "code/impl.py": "def run(event, context):\n    return 7\n",
            },
            ("handler", "app", "code/app.py", 1),
            True,
        ),
        (
            "app.handler",
            {"code/app.py": "handler = None\n\n\ndef handler(event, context):\n    return 8\n"},
            ("handler", "app", "code/app.py", 4),
            True,
        ),
        (
            "app.handler",
            {
                "code/app.py": "import os\n\nif os.environ.get('X'):\n    def handler(event, context):\n"
                "        return 9\nelse:\n    handler = None\n"
            },
            ("handler", "app", "code/app.py", 4),
            True,
        ),
        (
            "app.handler",
            {"code/app.py": "try:\n    from fast import handler\nexcept ImportError:\n    handler = None\n"},
            ("handler", "app", "code/app.py", 4),
            True,
        ),
        (
            "missing.handler",
            {"code/app.py": "def handler(event, context):\n    pass\n"},
            ("handler", "missing", None, 0),
            False,
        ),
        (
            "app.nothing",
            {"code/app.py": "def handler(event, context):\n    pass\n"},
            ("nothing", "app", "code/app.py", 0),
            False,
        ),
        (
            "app.handler",
            {"code/app.py": "def handler(event, context)\n    pass\n"},
            ("handler", "app", "code/app.py", 0),
            False,
        ),
        ("nohandler", {"code/app.py": ""}, (None, None, None, 0), False),
    ],
)
def test_handler_forms(
    tmp_path: Path, handler: str, files: dict[str, str], expected: tuple[Any, ...], mapped: bool
) -> None:
    root = _one_function_repo(tmp_path / "repo", files, handler=handler)
    locator = _locator(ONE_PIPELINE)
    (spec,) = locator.scan(root)
    assert spec.tasks == [TaskInfo("Run", "lambda", *expected)]
    assert locator.unmapped_states(root) == ({} if mapped else {"p": ["Run"]})
    mappings = locator.path_mappings(root, spec.tasks[0])
    assert all((root / mapping.repo_base).is_dir() for mapping in mappings), "only roots the repository has"
    file = spec.tasks[0].callable_file
    if (
        file is not None
    ):  # a frame of the handler module maps to its file (code/ is /var/task, shared/ /opt/python)
        top, _, rest = file.partition("/")
        runtime = f"{LAMBDA_TASK_ROOT if top == 'code' else LAMBDA_LAYER_ROOT}/{rest}"
        assert map_frame(runtime, mappings) == file


def test_a_handler_module_is_read_not_imported(tmp_path: Path) -> None:
    marker = tmp_path / "imported"
    root = _one_function_repo(
        tmp_path / "repo",
        {"code/app.py": f"open({str(marker)!r}, 'w').close()\n\n\ndef handler(event, context):\n    pass\n"},
        handler="app.handler",
    )
    (spec,) = _locator(ONE_PIPELINE).scan(root)
    assert spec.tasks[0].lineno == 4
    assert not marker.exists()


def test_a_non_python_function_has_a_code_root_but_no_callable_file(tmp_path: Path) -> None:
    root = _one_function_repo(
        tmp_path / "repo",
        {"code/index.js": "exports.handler = async () => 1;\n"},
        handler="index.handler",
        runtime="nodejs20.x",
    )
    locator = _locator(ONE_PIPELINE)
    (spec,) = locator.scan(root)
    assert spec.tasks == [TaskInfo("Run", "lambda", callable_name="handler", callable_module="index")]
    assert locator.unmapped_states(root) == {}
    assert locator.path_mappings(root, spec.tasks[0]) == []  # no Python module to map a frame into


@pytest.mark.parametrize(
    ("handler", "expected"),
    [
        ("app.handler", ("app", "handler")),
        ("pkg.mod.fn", ("pkg.mod", "fn")),
        ("dir/mod.fn", ("dir.mod", "fn")),
        ("functions/x/app.handler", ("functions.x.app", "handler")),
        ("dir\\mod.fn", ("dir.mod", "fn")),
        (" app.handler ", ("app", "handler")),
        ("my-module.handler", ("my-module", "handler")),
        ("handler", (None, None)),
        ("app.", (None, None)),
        (".handler", (None, None)),
        ("pkg..mod.fn", (None, None)),
        ("", (None, None)),
        (None, (None, None)),
    ],
)
def test_split_handler(handler: str | None, expected: tuple[str | None, str | None]) -> None:
    assert split_handler(handler) == expected


# =========================================================================== code_map


def test_code_map_by_logical_id_overrides_the_template(tmp_path: Path) -> None:
    root = _one_function_repo(
        tmp_path / "repo",
        {
            "code/app.py": "def handler(event, context):\n    pass\n",
            "src/fn/main.py": "\n\ndef run(event, context):\n    pass\n",
            "shared/etl/__init__.py": "",
        },
        handler="app.handler",
    )
    locator = _locator(ONE_PIPELINE, code_map={"Fn": {"path": "./src/fn/", "handler": "main.run"}})
    (spec,) = locator.scan(root)
    assert spec.tasks == [TaskInfo("Run", "lambda", "run", "main", "src/fn/main.py", 3)]
    assert locator.path_mappings(root, spec.tasks[0]) == [
        PathMapping("/var/task", "src/fn"),
        PathMapping("/opt/python/etl", "shared/etl"),  # the template's layers when the entry names none
    ]
    keeps_handler = _locator(ONE_PIPELINE, code_map={"Fn": {"path": "src/fn", "layers": ["libs"]}})
    root_two = _one_function_repo(
        tmp_path / "two",
        {"src/fn/app.py": "def handler(event, context):\n    pass\n", "libs/helpers.py": ""},
        handler="app.handler",
    )
    (spec,) = keeps_handler.scan(root_two)
    assert spec.tasks == [TaskInfo("Run", "lambda", "handler", "app", "src/fn/app.py", 1)]
    assert keeps_handler.path_mappings(root_two, spec.tasks[0]) == [
        PathMapping("/var/task", "src/fn"),
        PathMapping("/opt/python", "libs"),  # a top-level module: the whole layer maps
    ]


def test_code_map_without_a_template(tmp_path: Path) -> None:
    deployed_arn = "arn:aws:lambda:eu-west-1:123456789012:function:acme-run:live"
    root = _write(
        tmp_path / "repo",
        {
            "flows/run.asl.json": _machine(
                ("Run", _task("Run", deployed_arn)),
                ("Other", _task("Other", "unknown-fn")),
                ("AtRoot", _task("AtRoot", "acme-root")),
                ("NoHandler", _task("NoHandler", "acme-bare")),
                ("Report", {"Type": "Task", "Resource": "arn:aws:states:::glue:startJobRun.sync"}),
            ),
            "lambdas/run/handler.py": "def main(event, context):\n    pass\n",
            "libs/common.py": "",
            "functions/root/app.py": "\ndef handler(event, context):\n    pass\n",
        },
    )
    locator = _locator(
        {"runs": {"state_machine": "runs", "definition": "./flows/run.asl.json"}},
        template=None,
        code_map={
            "acme-run": {"path": "lambdas/run", "handler": "handler.main", "layers": ["libs"]},
            "acme-root": {"path": ".", "handler": "functions/root/app.handler"},
            "acme-bare": {"path": "bare"},
            "Unused": {"path": "unused/code"},
        },
    )
    (spec,) = locator.scan(root)
    assert spec.dag_id == "runs" and spec.file == "flows/run.asl.json"
    assert spec.tasks == [
        TaskInfo("Run", "lambda", "main", "handler", "lambdas/run/handler.py", 1),
        TaskInfo("Other", "lambda"),
        TaskInfo("AtRoot", "lambda", "handler", "functions.root.app", "functions/root/app.py", 2),
        TaskInfo("NoHandler", "lambda"),
        TaskInfo("Report", "glue"),
    ]
    assert locator.unmapped_states(root) == {"runs": ["Other", "NoHandler"]}
    assert locator.path_mappings(root, spec.tasks[0]) == [
        PathMapping("/var/task", "lambdas/run"),
        PathMapping("/opt/python", "libs"),
    ]
    at_root = [  # the repository root holds no module of its own: its directories map
        PathMapping("/var/task/flows", "flows"),
        PathMapping("/var/task/functions", "functions"),
        PathMapping("/var/task/lambdas", "lambdas"),
        PathMapping("/var/task/libs", "libs"),
    ]
    assert locator.path_mappings(root, spec.tasks[2]) == at_root
    assert locator.path_mappings(root, None) == [  # bare/ and unused/code are not in the repository
        PathMapping("/var/task", "lambdas/run"),
        *at_root,
        PathMapping("/opt/python", "libs"),
    ]
    assert locator.pipeline_file(root, "runs", None) == "flows/run.asl.json"


# =========================================================================== matching state machines

MATCHING_TEMPLATE = """\
Resources:
  Fn:
    Type: AWS::Serverless::Function
    Properties: {CodeUri: code/, Handler: app.handler, Runtime: python3.11}
  Named:
    Type: AWS::Serverless::StateMachine
    Properties: {Name: literal-name, DefinitionUri: flows/named.asl.json}
  Generated:
    Type: AWS::Serverless::StateMachine
    Properties: {DefinitionUri: flows/generated.asl.json}
  Other:
    Type: AWS::Serverless::StateMachine
    Properties: {DefinitionUri: flows/other.asl.json}
  ByFile:
    Type: AWS::Serverless::StateMachine
    Properties: {DefinitionUri: flows/by_file.asl.json}
  Extra:
    Type: AWS::Serverless::StateMachine
    Properties: {Name: extra-machine, DefinitionUri: flows/extra.asl.json}
  Broken:
    Type: AWS::Serverless::StateMachine
    Properties: {DefinitionUri: flows/broken.asl.json}
"""


def _matching_repo(root: Path) -> Path:
    files = {"template.yaml": MATCHING_TEMPLATE, "code/app.py": "def handler(event, context):\n    pass\n"}
    for name in ("named", "generated", "other", "by_file", "extra", "standalone"):
        files[f"flows/{name}.asl.json"] = _machine((f"{name.title()}Step", _task("x", "${Fn.Arn}")))
    files["flows/broken.asl.json"] = "{broken"
    return _write(root, files)


MATCHING_PIPELINES: dict[str, dict[str, str]] = {
    "by_logical": {"state_machine": ARN + "renamed-AbC123", "logical_id": "Generated"},
    "by_name": {"state_machine": "literal-name"},
    "by_generated": {"state_machine": ARN + "Other-Q1W2E3R4T5Y6"},
    "by_definition": {"state_machine": "unknown-machine", "definition": "flows/by_file.asl.json"},
    "definition_only": {"state_machine": "standalone", "definition": "flows/standalone.asl.json"},
    "bad_logical_id": {
        "state_machine": "nowhere",
        "logical_id": "Nope",
        "definition": "flows/standalone.asl.json",
    },
    "broken": {"state_machine": "broken", "logical_id": "Broken"},
    "missing": {"state_machine": "nowhere"},
    "missing_logical": {"state_machine": ARN + "Other-Q1W2E3R4T5Y6", "logical_id": "Nope"},
}


def test_state_machines_match_by_logical_id_name_generated_name_or_definition(tmp_path: Path) -> None:
    root = _matching_repo(tmp_path / "repo")
    locator = _locator(MATCHING_PIPELINES)
    specs = {spec.dag_id: spec for spec in locator.scan(root)}
    assert {name: spec.file for name, spec in specs.items()} == {
        "by_logical": "flows/generated.asl.json",
        "by_name": "flows/named.asl.json",
        "by_generated": "flows/other.asl.json",
        "by_definition": "flows/by_file.asl.json",
        "definition_only": "flows/standalone.asl.json",
        "bad_logical_id": "flows/standalone.asl.json",
        "broken": "flows/broken.asl.json",
    }
    assert [task.task_id for task in specs["by_generated"].tasks] == ["OtherStep"]
    assert specs["by_generated"].tasks[0].callable_file == "code/app.py"  # ${Fn.Arn} resolves to Fn
    assert specs["definition_only"].tasks[0].callable_file == "code/app.py"  # a ${...} the template names
    assert specs["broken"].tasks == []  # the machine is found, its definition is not readable
    assert locator.missing_pipelines(root) == ["missing", "missing_logical"]
    assert locator.pipeline_file(root, "broken", None) == "flows/broken.asl.json"
    assert locator.drift(root, "broken", {"States": {}}) == [
        "no definition of pipeline broken was found in the repository"
    ]


def test_discover_reports_the_named_machines_no_pipeline_claims(tmp_path: Path) -> None:
    root = _matching_repo(tmp_path / "repo")
    locator = _locator({"by_name": {"state_machine": "literal-name"}}, discover=True)
    assert [spec.dag_id for spec in locator.scan(root)] == ["by_name", "extra-machine"]
    claimed = _locator({"mine": {"state_machine": "x", "logical_id": "Extra"}}, discover=True)
    assert [spec.dag_id for spec in claimed.scan(root)] == ["mine", "literal-name"]


def test_discovered_pipelines_of_the_index_are_matched(tmp_path: Path) -> None:
    root = _matching_repo(tmp_path / "repo")
    options = StepFunctionsOptions.model_validate({"region": REGION, "discover": True})
    index = PipelineIndex.from_options(options)
    assert index.add_discovered(ARN + "Generated-ZXCVBN123456") == "Generated-ZXCVBN123456"
    locator = SfnCodeLocator(None, options, pipelines=index)
    specs = {spec.dag_id: spec.file for spec in locator.scan(root)}
    assert specs == {
        "Generated-ZXCVBN123456": "flows/generated.asl.json",
        "literal-name": "flows/named.asl.json",
        "extra-machine": "flows/extra.asl.json",
    }


def test_two_pipelines_may_share_a_state_name(tmp_path: Path) -> None:
    root = _write(
        tmp_path / "repo",
        {
            "template.yaml": """\
Resources:
  FnA:
    Type: AWS::Serverless::Function
    Properties: {CodeUri: a/, Handler: app.handler, Runtime: python3.11}
  FnB:
    Type: AWS::Serverless::Function
    Properties: {CodeUri: b/, Handler: app.handler, Runtime: python3.11}
  MachineA:
    Type: AWS::Serverless::StateMachine
    Properties: {DefinitionUri: a.asl.json, DefinitionSubstitutions: {Fn: !GetAtt FnA.Arn}}
  MachineB:
    Type: AWS::Serverless::StateMachine
    Properties: {DefinitionUri: b.asl.json, DefinitionSubstitutions: {Fn: !Ref FnB}}
""",
            "a.asl.json": _machine(("Transform", _task("Transform", "${Fn}"))),
            "b.asl.json": _machine(("Transform", _task("Transform", "${Fn}"))),
            "a/app.py": "def handler(event, context):\n    pass\n",
            "b/app.py": "\ndef handler(event, context):\n    pass\n",
        },
    )
    locator = _locator(
        {
            "a": {"state_machine": "a-machine", "logical_id": "MachineA"},
            "b": {"state_machine": "b-machine", "logical_id": "MachineB"},
        }
    )
    specs = {spec.dag_id: spec for spec in locator.scan(root)}
    step_a, step_b = specs["a"].task("Transform"), specs["b"].task("Transform")
    assert step_a is not None and step_b is not None
    assert (step_a.callable_file, step_b.callable_file) == ("a/app.py", "b/app.py")
    assert locator.path_mappings(root, step_a) == [PathMapping("/var/task", "a")]
    assert locator.path_mappings(root, step_b) == [PathMapping("/var/task", "b")]
    # a step known by its name only: a/ or b/ ran, so /var/task is left to the core's suffix match
    # (mapping it to a/ would put b's frames into a's files, which exist)
    assert locator.path_mappings(root, TaskInfo("Transform", "lambda")) == []


# =========================================================================== datasets


def test_datasets_come_from_the_definition_and_the_handler_modules(tmp_path: Path) -> None:
    definition = {
        "StartAt": "Scan",
        "States": {
            "Scan": {
                "Type": "Map",
                "ItemReader": {
                    "Resource": "arn:aws:states:::s3:getObject",
                    "ReaderConfig": {"InputType": "CSV"},
                    "Parameters": {"Bucket": "acme", "Key": "incoming/orders"},
                },
                "ItemProcessor": {
                    "ProcessorConfig": {"Mode": "DISTRIBUTED", "ExecutionType": "STANDARD"},
                    "StartAt": "Score",
                    "States": {"Score": {**_task("Score", "${FnArn}"), "End": True}},
                },
                "ResultWriter": {
                    "Resource": "arn:aws:states:::s3:putObject",
                    "Parameters": {"Bucket": "acme", "Prefix": "results/"},
                },
                "Next": "Lookup",
            },
            "Lookup": {
                "Type": "Pass",
                "Parameters": {"Bucket": "acme", "Key": "lookup/codes.json"},
                "Next": "Daily",
            },
            "Daily": {
                "Type": "Task",
                "Resource": "arn:aws:states:::aws-sdk:s3:getObject",
                "Parameters": {"Bucket.$": "$.bucket", "Key": "daily/report.parquet"},
                "Next": "Notes",
            },
            "Notes": {
                "Type": "Task",
                "Resource": "arn:aws:states:::aws-sdk:s3:getObject",
                "Parameters": {"Bucket": "acme", "Key": "notes/readme.txt"},
                "End": True,
            },
        },
    }
    app = (
        "from etl import paths\n"
        "\n"
        'ARCHIVE = "s3://acme/archive/orders.csv"\n'
        "\n"
        "\n"
        "def handler(event, context):\n"
        '    bucket = event["bucket"]\n'
        '    return f"s3://{bucket}/incoming/orders.csv", paths.LOOKUP, "s3://acme/a b.csv"\n'
    )
    root = _one_function_repo(
        tmp_path / "repo",
        {
            "code/app.py": app,
            "shared/etl/__init__.py": "",
            "shared/etl/paths.py": 'LOOKUP = "s3://acme/lookup/codes.json"\nPREFIX = "s3://acme/prefix/"\n',
        },
        handler="app.handler",
    )
    (root / "machine.asl.json").write_text(json.dumps(definition, indent=2), encoding="utf-8")
    (spec,) = _locator(ONE_PIPELINE).scan(root)
    asl_file = root / "machine.asl.json"
    assert spec.datasets == [
        DatasetRef(
            "Scan",
            "s3://acme/incoming/orders",
            "csv",
            True,
            f"machine.asl.json:{_line(asl_file, 'incoming/orders')}",
        ),
        DatasetRef(
            "Lookup",
            "s3://acme/lookup/codes.json",
            "json",
            True,
            f"machine.asl.json:{_line(asl_file, 'lookup/codes')}",
        ),
        DatasetRef(
            "Daily",
            "s3://<dynamic>/daily/report.parquet",
            "parquet",
            False,
            f"machine.asl.json:{_line(asl_file, 'daily/')}",
        ),
        DatasetRef("Score", "s3://acme/archive/orders.csv", "csv", True, "code/app.py:3"),
        DatasetRef("Score", "s3://{bucket}/incoming/orders.csv", "csv", False, "code/app.py:8"),
        DatasetRef("Score", "s3://acme/lookup/codes.json", "json", True, "shared/etl/paths.py:1"),
    ]
    assert spec.imports == ["etl", "etl.paths"]


# =========================================================================== odd repositories


def test_an_invalid_template_is_logged_and_skipped(tmp_path: Path, caplog: pytest.LogCaptureFixture) -> None:
    root = _write(
        tmp_path / "repo",
        {
            "template.yaml": "Resources: [secret-content",
            "flows/x.asl.json": _machine(("A", {"Type": "Succeed"})),
        },
    )
    locator = _locator(
        {
            "templated": {"state_machine": "templated", "logical_id": "Machine"},
            "standalone": {"state_machine": "standalone", "definition": "flows/x.asl.json"},
        }
    )
    caplog.set_level(logging.DEBUG)
    assert [spec.dag_id for spec in locator.scan(root)] == ["standalone"]
    assert locator.missing_pipelines(root) == ["templated"]
    warnings = [record for record in caplog.records if record.levelno == logging.WARNING]
    assert warnings and all("secret-content" not in record.getMessage() for record in caplog.records)


@pytest.mark.parametrize("template", [None, "missing.yaml"])
def test_without_a_template_only_definitions_and_code_map_apply(tmp_path: Path, template: str | None) -> None:
    root = _write(
        tmp_path / "repo",
        {
            "flows/x.asl.json": _machine(("Run", _task("Run", "${FnArn}"))),
            "code/app.py": "def handler(e, c):\n    pass\n",
        },
    )
    locator = _locator(
        {
            "x": {"state_machine": "x", "definition": "flows/x.asl.json"},
            "y": {"state_machine": "y", "logical_id": "Machine"},
        },
        template=template,
    )
    (spec,) = locator.scan(root)
    assert spec.tasks == [TaskInfo("Run", "lambda")]
    assert locator.unmapped_states(root) == {"x": ["Run"]}
    assert locator.path_mappings(root, None) == []


def test_a_definition_option_outside_the_repository_is_ignored(tmp_path: Path) -> None:
    _write(tmp_path, {"outside.asl.json": _machine(("A", {"Type": "Succeed"}))})
    root = _write(tmp_path / "repo", {"template.yaml": "Resources: {}\n"})
    locator = _locator({"x": {"state_machine": "x", "definition": "../outside.asl.json"}})
    assert locator.scan(root) == []
    assert locator.missing_pipelines(root) == ["x"]


class _Entries:
    """A pipeline index stand-in (the locator only lists the entries)."""

    def __init__(self, *entries: PipelineEntry) -> None:
        self._entries = list(entries)

    def entries(self) -> list[PipelineEntry]:
        return list(self._entries)


def test_an_entry_whose_name_cannot_be_read_never_raises(tmp_path: Path) -> None:
    root = _matching_repo(tmp_path / "repo")
    options = StepFunctionsOptions.model_validate({"region": REGION})
    odd = PipelineEntry("odd", "arn:aws:states:eu-west-1:123456789012:execution:x:y")  # not a machine ARN
    locator = SfnCodeLocator(None, options, pipelines=_Entries(odd))  # type: ignore[arg-type]
    assert locator.scan(root) == []
    assert locator.missing_pipelines(root) == ["odd"]


def test_the_locator_works_on_a_copy_of_the_fixture_repository(tmp_path: Path) -> None:
    copy_root = tmp_path / "checkout"
    shutil.copytree(SFN_REPO, copy_root)
    locator = SfnCodeLocator(None, sfn_options())
    original = [copy.deepcopy(spec.to_dict()) for spec in locator.scan(SFN_REPO)]
    assert [spec.to_dict() for spec in locator.scan(copy_root)] == original
    (copy_root / "functions" / "customers" / "app.py").unlink()
    assert locator.unmapped_states(copy_root) == {
        "customers_sync": ["ExtractCustomers", "TransformCustomers", "LoadCustomers"]
    }


# =========================================================================== import hygiene


def test_the_code_mapping_modules_stay_light() -> None:
    code = (
        "import json, sys\n"
        "import dag_healer.stepfunctions.asl\n"
        "import dag_healer.stepfunctions.iac.sam\n"
        "import dag_healer.stepfunctions.locator\n"
        "heavy = {'airflow', 'boto3', 'botocore', 'fastapi', 'pandas', 'yaml', 'pydantic'}\n"
        "print(json.dumps(sorted({name.split('.')[0] for name in sys.modules} & heavy)))\n"
    )
    env = {**os.environ, "PYTHONPATH": os.pathsep.join(entry for entry in sys.path if entry)}
    result = subprocess.run(
        [sys.executable, "-c", code], capture_output=True, text=True, timeout=120, check=False, env=env
    )
    assert result.returncode == 0, result.stderr
    assert json.loads(result.stdout.strip().splitlines()[-1]) == []
