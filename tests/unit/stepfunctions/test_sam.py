"""iac.sam: CloudFormation short tags, the SAM resolver and function references.

The templates of ``tests/unit/fixtures/stepfunctions/sam`` are copied into throwaway
repositories (with the folders their paths point at); the fixture repository's template is
resolved as it is.
"""

from __future__ import annotations

import os
import shutil
from pathlib import Path
from typing import Any

import pytest

from dag_healer.stepfunctions.asl import TaskTarget
from dag_healer.stepfunctions.iac import IacResolver, ResolvedFunction, ResolvedStateMachine, sam
from dag_healer.stepfunctions.iac.sam import SamResolver, generated_name_matches, load_template, merge_globals

from .conftest import FIXTURES, SFN_REPO

SAM = FIXTURES / "sam"
INVOKE = "arn:aws:states:::lambda:invoke"


def _repo(
    tmp_path: Path, template: str, *, name: str = "template.yaml", folders: tuple[str, ...] = ()
) -> Path:
    root = tmp_path / "repo"
    target = root / name
    target.parent.mkdir(parents=True, exist_ok=True)
    shutil.copyfile(SAM / template, target)
    for folder in folders:
        (root / folder).mkdir(parents=True, exist_ok=True)
    return root


def _write(root: Path, relative: str, text: str) -> Path:
    path = root / relative
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(text, encoding="utf-8")
    return path


def _functions(resolver: SamResolver) -> dict[str, ResolvedFunction]:
    return {function.logical_id or "": function for function in resolver.functions()}


def _machines(resolver: SamResolver) -> dict[str, ResolvedStateMachine]:
    return {machine.logical_id: machine for machine in resolver.state_machines()}


# =========================================================================== load_template


def test_load_template_turns_short_tags_into_intrinsic_mappings() -> None:
    template = load_template(SAM / "short_tags.yaml")
    assert template["AWSTemplateFormatVersion"] == "2010-09-09"  # timestamps stay text
    assert template["Conditions"] == {
        "IsProd": {"Fn::Equals": [{"Ref": "Stage"}, "prod"]},
        "NotProd": {"Fn::Not": [{"Condition": "IsProd"}]},
        "Both": {"Fn::And": [{"Condition": "IsProd"}, {"Condition": "NotProd"}]},
        "Either": {"Fn::Or": [{"Condition": "IsProd"}, {"Condition": "NotProd"}]},
    }
    properties = template["Resources"]["Fn"]["Properties"]
    assert properties["MemorySize"] == {"Fn::FindInMap": ["Sizes", {"Ref": "Stage"}, "Memory"]}
    assert properties["Role"] == {"Fn::GetAtt": ["FnRole", "Arn"]}
    assert properties["Description"] == {"Fn::Sub": "fn of ${Stage}"}
    assert properties["Tags"] == {
        "Name": {"Fn::Join": ["-", [{"Ref": "AWS::StackName"}, "fn"]]},
        "Zone": {"Fn::Select": [0, {"Fn::GetAZs": ""}]},
        "Parts": {"Fn::Split": [",", "a,b"]},
        "Data": {"Fn::Base64": "hello"},
        "Shared": {"Fn::ImportValue": "shared-output"},
        "Mode": {"Fn::If": ["IsProd", "big", "small"]},
        "Network": {"Fn::Cidr": ["10.0.0.0/16", 4, 8]},
        "Label": {"Fn::Sub": ["${Prefix}-${Stage}", {"Prefix": "etl"}]},
        "Count": {"Fn::Length": [1, 2]},
        "Created": "2026-10-02",
        "Arn": {"Fn::GetAtt": ["Fn", "Arn"]},
        "Nested": {"Fn::GetAtt": ["Stack", "Outputs.Name"]},
    }


def test_every_documented_short_tag_is_understood() -> None:
    text = "\n".join(f"T{index}: !{tag} value" for index, tag in enumerate(sam.SHORT_TAGS))
    parsed = sam.parse_template(text)
    assert parsed["T0"] == {"Ref": "value"}
    assert parsed[f"T{sam.SHORT_TAGS.index('Condition')}"] == {"Condition": "value"}
    assert parsed[f"T{sam.SHORT_TAGS.index('GetAtt')}"] == {"Fn::GetAtt": ["value", ""]}
    others = [tag for tag in sam.SHORT_TAGS if tag not in ("Ref", "Condition", "GetAtt")]
    assert all(parsed[f"T{sam.SHORT_TAGS.index(tag)}"] == {f"Fn::{tag}": "value"} for tag in others)


def test_load_template_reads_json_templates() -> None:
    template = load_template(SAM / "template.json")
    assert template["Resources"]["Scoring"]["Properties"]["StateMachineName"] == "scoring"
    assert sam.parse_template("\ufeff" + (SAM / "template.json").read_text())["Resources"]


@pytest.mark.parametrize(
    ("text", "problem"),
    [
        ("Resources: [unclosed", "invalid template"),
        ("- just\n- a list\n", "not a SAM/CloudFormation template"),
        ("", "not a SAM/CloudFormation template"),
        ("plain text", "not a SAM/CloudFormation template"),
        ("[" * 50_000, "invalid template"),
    ],
)
def test_load_template_refuses_invalid_templates(tmp_path: Path, text: str, problem: str) -> None:
    path = _write(tmp_path, "template.yaml", text)
    with pytest.raises(ValueError, match=problem) as raised:
        load_template(path)
    assert str(path) in str(raised.value)


def test_load_template_names_the_file_without_quoting_it(tmp_path: Path) -> None:
    path = _write(tmp_path, "template.yaml", "Resources:\n  X: secret-value-123: here\n")
    with pytest.raises(ValueError) as raised:
        load_template(path)
    assert "line 2" in str(raised.value) and "secret-value-123" not in str(raised.value)


def test_load_template_refuses_unreadable_files(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    with pytest.raises(ValueError, match="cannot read the template"):
        load_template(tmp_path / "missing.yaml")
    binary = tmp_path / "binary.yaml"
    binary.write_bytes(b"\xff\xfe\x00\x01")
    with pytest.raises(ValueError, match="not UTF-8"):
        load_template(binary)
    big = _write(tmp_path, "big.yaml", "Resources: {}\n")
    monkeypatch.setattr(sam, "MAX_TEMPLATE_BYTES", 4)
    with pytest.raises(ValueError, match="larger than"):
        load_template(big)


# =========================================================================== the fixture repository


def test_the_fixture_repository_functions() -> None:
    resolver = SamResolver(SFN_REPO)
    assert isinstance(resolver, IacResolver)
    assert resolver.exists and resolver.template_file == "template.yaml" and resolver.template_dir == ""
    assert resolver.functions() == [
        ResolvedFunction(
            "SalesFunction", None, "functions/sales", "app.handler", "python3.11", ("src",), None
        ),
        ResolvedFunction(
            "CustomersFunction", None, "functions/customers", "app.handler", "python3.11", ("src",), None
        ),
        ResolvedFunction(
            "InventoryFunction",
            None,
            "functions/inventory",
            "app.handler",
            "python3.11",
            ("src",),
            "/sfn-etl/inventory",  # a !Ref to the AWS::Logs::LogGroup of the template
        ),
    ]
    assert all(function.is_python for function in resolver.functions())


def test_the_fixture_repository_state_machines() -> None:
    machines = _machines(SamResolver(SFN_REPO))
    assert list(machines) == [
        "SalesDailyEtlStateMachine",
        "CustomersSyncStateMachine",
        "InventoryReportStateMachine",
    ]
    sales, customers, inventory = machines.values()
    assert (sales.name, customers.name, inventory.name) == (None, "customers-sync", None)
    assert sales.definition_file == "statemachine/sales_daily_etl.asl.json"
    assert sales.template_file == "template.yaml"
    assert sales.substitutions == {"SalesFunctionArn": {"Fn::GetAtt": ["SalesFunction", "Arn"]}}
    assert customers.substitutions == {"CustomersFunctionArn": {"Fn::Sub": "${CustomersFunction.Arn}"}}
    assert inventory.definition is not None
    assert list(inventory.definition["States"]) == ["GetInventory", "PerWarehouse", "PublishReport"]
    # the !GetAtt substitutions are not literal: the placeholders stay
    task = inventory.definition["States"]["PerWarehouse"]["ItemProcessor"]["States"]["BuildReport"]
    assert task["Resource"] == "${InventoryFunctionArn}"


@pytest.mark.parametrize(
    ("machine", "target", "function"),
    [
        (
            "SalesDailyEtlStateMachine",
            TaskTarget("lambda", "invoke", "${SalesFunctionArn}", INVOKE),
            "SalesFunction",
        ),
        (
            "CustomersSyncStateMachine",
            TaskTarget("lambda", "invoke", "${CustomersFunctionArn}", INVOKE),
            "CustomersFunction",
        ),
        (
            "InventoryReportStateMachine",
            TaskTarget("lambda", "legacy", "${InventoryFunctionArn}", "${InventoryFunctionArn}"),
            "InventoryFunction",
        ),
        (
            "InventoryReportStateMachine",
            TaskTarget("lambda", "legacy", None, "${InventoryFunctionArn}"),
            "InventoryFunction",
        ),
        (
            "SalesDailyEtlStateMachine",
            TaskTarget("lambda", "invoke", "${InventoryFunctionArn}", INVOKE),
            None,
        ),
        ("SalesDailyEtlStateMachine", TaskTarget("lambda", "invoke", None, INVOKE), None),
        (
            "SalesDailyEtlStateMachine",
            TaskTarget("ecs", "runTask.sync", None, "arn:aws:states:::ecs:runTask.sync"),
            None,
        ),
        (
            "SalesDailyEtlStateMachine",
            TaskTarget(
                "lambda",
                "invoke",
                "arn:aws:lambda:eu-west-1:123456789012:function:sfn-etl-SalesFunction-1A2B3C4D5E6F",
                INVOKE,
            ),
            "SalesFunction",  # the deployed (generated) name of the definition that ran
        ),
    ],
)
def test_function_for_the_fixture_repository(machine: str, target: TaskTarget, function: str | None) -> None:
    resolver = SamResolver(SFN_REPO)
    found = resolver.function_for(_machines(resolver)[machine], target)
    assert (found.logical_id if found else None) == function


# =========================================================================== function references


def _reference_repo(tmp_path: Path) -> SamResolver:
    root = tmp_path / "refs"
    _write(
        root,
        "template.yaml",
        """\
Resources:
  Alpha:
    Type: AWS::Serverless::Function
    Properties: {CodeUri: a/, Handler: app.handler, Runtime: python3.11}
  Beta:
    Type: AWS::Serverless::Function
    Properties: {FunctionName: acme-beta, CodeUri: b/, Handler: app.handler, Runtime: python3.11}
  Gamma:
    Type: AWS::Lambda::Function
    Properties: {Code: c/, Handler: app.handler, Runtime: python3.11}
  Queue:
    Type: AWS::SQS::Queue
""",
    )
    return SamResolver(root)


@pytest.mark.parametrize(
    ("reference", "substitutions", "expected"),
    [
        ("${A}", {"A": {"Fn::GetAtt": ["Alpha", "Arn"]}}, "Alpha"),
        ("${A}", {"A": {"Fn::GetAtt": "Alpha.Arn"}}, "Alpha"),
        ("${A}", {"A": {"Ref": "Beta"}}, "Beta"),
        ("${A}", {"A": {"Fn::Sub": "${Gamma.Arn}"}}, "Gamma"),
        (
            "${A}",
            {"A": {"Fn::Sub": "arn:aws:lambda:${AWS::Region}:${AWS::AccountId}:function:${Alpha}"}},
            "Alpha",
        ),
        ("${A}", {"A": {"Fn::Sub": ["${Fn}", {"Fn": {"Fn::GetAtt": ["Beta", "Arn"]}}]}}, "Beta"),
        ("${A}", {"A": "${B}", "B": {"Ref": "Alpha"}}, "Alpha"),
        ("${A}", {"A": "arn:aws:lambda:eu-west-1:123456789012:function:acme-beta:live"}, "Beta"),
        ("${A}", {"A": {"Fn::GetAtt": ["Alpha", "Version"]}}, None),
        ("${A}", {"A": {"Fn::GetAtt": ["Queue", "Arn"]}}, None),
        ("${A}", {"A": {"Fn::Join": ["", ["arn", "x"]]}}, None),
        ("${A}", {"A": {"Ref": "Alpha", "Extra": 1}}, None),
        ("${A}", {"A": "${A}"}, None),  # a loop ends at the resolution depth
        ("${A}", {}, None),
        ("${Alpha.Arn}", {}, "Alpha"),
        ("${Gamma}", {}, "Gamma"),
        ("${Alpha.Version}", {}, None),
        ("arn:aws:lambda:${AWS::Region}:${AWS::AccountId}:function:${Beta}", {}, "Beta"),
        (
            "arn:aws:lambda:${Region}:123456789012:function:${Name}",
            {"Region": "eu-west-1", "Name": {"Ref": "Alpha"}},
            "Alpha",
        ),
        ("${Alpha}-${Beta}", {}, None),  # two functions: ambiguous
        ("acme-beta", {}, "Beta"),
        ("arn:aws:lambda:eu-west-1:123456789012:function:acme-beta", {}, "Beta"),
        ("123456789012:function:acme-beta", {}, "Beta"),
        ("acme-beta:$LATEST", {}, "Beta"),
        ("sfn-etl-Alpha-1A2B3C4D5E6F", {}, "Alpha"),
        ("arn:aws:lambda:eu-west-1:123456789012:function:stack-Gamma-ZZZ999", {}, "Gamma"),
        ("sfn-etl-Beta-1A2B3C4D5E6F", {}, None),  # Beta has a literal name: no generated one
        ("Alpha", {}, None),
        ("arn:aws:sqs:eu-west-1:123456789012:queue", {}, None),
        ("", {}, None),
        (None, {}, None),
    ],
)
def test_resolve_function(
    tmp_path: Path, reference: str | None, substitutions: dict[str, Any], expected: str | None
) -> None:
    found = _reference_repo(tmp_path).resolve_function(reference, substitutions)
    assert (found.logical_id if found else None) == expected


def test_resolve_function_without_a_template(tmp_path: Path) -> None:
    resolver = SamResolver(tmp_path, "missing.yaml")
    assert not resolver.exists
    assert resolver.functions() == [] and resolver.state_machines() == [] and resolver.resource_types() == {}
    assert resolver.resolve_function("${A}", {"A": {"Ref": "X"}}) is None


@pytest.mark.parametrize(
    ("name", "logical_id", "expected"),
    [
        ("sfn-etl-SalesFunction-1A2B3C4D5E6F", "SalesFunction", True),
        ("SalesDailyEtlStateMachine-AbC123", "SalesDailyEtlStateMachine", True),
        ("SalesDailyEtlStateMachine", "SalesDailyEtlStateMachine", False),
        ("sfn-etl-SalesFunction-1A2B3C4D5E6F", "Sales", False),
        ("sfn-etl-SalesFunction-1A2B-3C4D", "SalesFunction", False),
        ("sfn-etl-SalesFunction-suffix_x", "SalesFunction", False),
        ("x-SalesFunctio-1A2B3C4D5E6F", "SalesFunction", False),  # a truncated logical id
        ("", "SalesFunction", False),
        ("a-b", None, False),
    ],
)
def test_generated_name_matches(name: str, logical_id: str | None, expected: bool) -> None:
    assert generated_name_matches(name, logical_id) is expected


# =========================================================================== Globals, layers, log groups


def test_merge_globals_follows_the_sam_rules() -> None:
    merged = merge_globals(
        {
            "Runtime": "python3.11",
            "Layers": [{"Ref": "Base"}],
            "Environment": {"Variables": {"STAGE": "dev", "BUCKET": {"Ref": "Bucket"}}},
            "Role": {"Fn::GetAtt": ["Shared", "Arn"]},
            "Tags": {"team": "data"},
        },
        {
            "Runtime": "python3.12",
            "Layers": [{"Ref": "Own"}],
            "Environment": {"Variables": {"STAGE": "prod"}},
            "Role": {"Fn::GetAtt": ["Own", "Arn"]},
            "Tags": {"Ref": "TagsParameter"},
        },
    )
    assert merged == {
        "Runtime": "python3.12",
        "Layers": [{"Ref": "Base"}, {"Ref": "Own"}],
        "Environment": {"Variables": {"STAGE": "prod", "BUCKET": {"Ref": "Bucket"}}},
        "Role": {"Fn::GetAtt": ["Own", "Arn"]},
        "Tags": {"Ref": "TagsParameter"},
    }


def test_globals_layers_and_log_groups(tmp_path: Path) -> None:
    root = _repo(
        tmp_path,
        "globals.yaml",
        folders=("shared", "functions/own", "layers/base", "layers/extra/python", "layers/plain"),
    )
    functions = _functions(SamResolver(root))
    assert functions["Defaults"] == ResolvedFunction(
        "Defaults", None, "shared", "app.handler", "python3.11", ("layers/base",), "/acme/default"
    )
    assert functions["Own"] == ResolvedFunction(
        "Own",
        None,
        "functions/own",
        "own.main",
        "python3.12",
        ("layers/base", "layers/extra/python", "layers/plain"),  # global layers first, no duplicates
        "/acme/own",
    )
    assert functions["RefWithoutName"].log_group is None
    assert functions["SubLogGroup"].log_group is None


def test_functions_without_local_code(tmp_path: Path) -> None:
    root = _repo(tmp_path, "cloudformation.yaml", folders=("src/ingest",))
    functions = _functions(SamResolver(root))
    assert functions["Ingest"] == ResolvedFunction(
        "Ingest", "acme-ingest", "src/ingest", "handlers/ingest.run", "python3.11", (), None
    )
    assert (
        functions["Report"].code_uri is None and functions["Report"].handler == "index.handler"
    )  # InlineCode
    assert functions["Render"].code_uri is None and not functions["Render"].is_python  # an image
    assert functions["Archived"].code_uri is None  # an S3 location
    assert functions["Zipped"].code_uri is None and not functions["Zipped"].is_python  # an archive


def test_plain_cloudformation_functions_and_layers(tmp_path: Path) -> None:
    root = _repo(
        tmp_path, "template.json", name="template.json", folders=("lambdas/score", "layers/shared/python")
    )
    resolver = SamResolver(root, "template.json")
    assert resolver.functions() == [
        ResolvedFunction(
            "ScoreFunction",
            None,
            "lambdas/score",
            "score.main.handler",
            "python3.11",
            ("layers/shared/python",),
            "/acme/score",
        )
    ]
    assert resolver.resource_types() == {
        "ScoreFunction": "AWS::Lambda::Function",
        "Shared": "AWS::Lambda::LayerVersion",
        "Scoring": "AWS::StepFunctions::StateMachine",
    }


# =========================================================================== state machines


def test_definition_strings_and_their_sub_variables(tmp_path: Path) -> None:
    root = _repo(tmp_path, "template.json", name="template.json")
    resolver = SamResolver(root, "template.json")
    (scoring,) = resolver.state_machines()
    assert scoring.name == "scoring" and scoring.definition_file is None
    assert scoring.substitutions == {"Target": {"Fn::GetAtt": ["ScoreFunction", "Arn"]}}
    assert scoring.definition is not None
    assert scoring.definition["States"]["Score"]["Resource"] == "${Target}"
    for state, target in (
        ("Score", TaskTarget("lambda", "legacy", "${Target}", "${Target}")),
        ("Rank", TaskTarget("lambda", "invoke", "${ScoreFunction.Arn}", INVOKE)),
    ):
        found = resolver.function_for(scoring, target)
        assert found is not None and found.logical_id == "ScoreFunction", state


def test_inline_definitions_with_intrinsic_functions(tmp_path: Path) -> None:
    root = _repo(tmp_path, "cloudformation.yaml", folders=("src/ingest",))
    resolver = SamResolver(root)
    machines = _machines(resolver)
    ingestion = machines["Ingestion"]
    assert ingestion.definition_file is None and ingestion.template_file == "template.yaml"
    assert ingestion.definition is not None
    states = ingestion.definition["States"]
    assert states["Ingest"]["Resource"] == "${Ingest.Arn}"
    assert states["Notify"]["Parameters"]["FunctionName"] == "${Ingest}"
    assert states["Notify"]["Parameters"]["Payload"]["text"] == "ingested by ${Ingest}"
    for target in (
        TaskTarget("lambda", "legacy", "${Ingest.Arn}", "${Ingest.Arn}"),
        TaskTarget("lambda", "invoke", "${Ingest}", INVOKE),
    ):
        found = resolver.function_for(ingestion, target)
        assert found is not None and found.logical_id == "Ingest"
    mapped = machines["Mapped"]
    assert mapped.definition is not None and mapped.definition["States"]["Go"]["Resource"] == "${IngestArn}"
    plain = machines["Plain"]
    assert plain.name == "plain" and plain.definition is not None
    literal = plain.definition["States"]["Run"]["Resource"]
    found = resolver.function_for(plain, TaskTarget("lambda", "legacy", literal, literal))
    assert found is not None and found.logical_id == "Ingest"  # its literal FunctionName
    assert machines["Remote"].definition is None and machines["Remote"].definition_file is None
    assert machines["Broken"].definition is None


def test_literal_substitutions_are_applied_to_the_definition(tmp_path: Path) -> None:
    root = tmp_path / "literal"
    _write(
        root,
        "template.yaml",
        """\
Resources:
  Machine:
    Type: AWS::Serverless::StateMachine
    Properties:
      DefinitionUri: sm.asl.json
      DefinitionSubstitutions:
        Fn: arn:aws:lambda:eu-west-1:123456789012:function:literal-fn
        Concurrency: "4"
        Other: !Ref Something
""",
    )
    _write(
        root,
        "sm.asl.json",
        '{"StartAt": "M", "States": {"M": {"Type": "Map", "MaxConcurrency": ${Concurrency}, "ItemProcessor":'
        ' {"StartAt": "T", "States": {"T": {"Type": "Task", "Resource": "${Fn}", "Comment": "${Other}",'
        ' "End": true}}}, "End": true}}}',
    )
    (machine,) = SamResolver(root).state_machines()
    assert machine.definition is not None
    inner = machine.definition["States"]["M"]
    assert inner["MaxConcurrency"] == "4"
    assert (
        inner["ItemProcessor"]["States"]["T"]["Resource"]
        == "arn:aws:lambda:eu-west-1:123456789012:function:literal-fn"
    )
    assert inner["ItemProcessor"]["States"]["T"]["Comment"] == "${Other}"


# =========================================================================== paths


def test_paths_are_relative_to_the_template_folder(tmp_path: Path) -> None:
    root = tmp_path / "nested"
    _write(
        root,
        "infra/template.yaml",
        """\
Resources:
  Up:
    Type: AWS::Serverless::Function
    Properties: {CodeUri: ../functions/up/, Handler: app.handler, Runtime: python3.11}
  Here:
    Type: AWS::Serverless::Function
    Properties: {CodeUri: ., Handler: app.handler, Runtime: python3.11}
  Outside:
    Type: AWS::Serverless::Function
    Properties: {CodeUri: ../../elsewhere, Handler: app.handler, Runtime: python3.11}
  Machine:
    Type: AWS::Serverless::StateMachine
    Properties:
      DefinitionUri: ../statemachine/up.asl.json
""",
    )
    _write(root, "statemachine/up.asl.json", '{"StartAt": "A", "States": {"A": {"Type": "Succeed"}}}')
    resolver = SamResolver(root, "./infra/template.yaml")
    assert resolver.template_file == "infra/template.yaml" and resolver.template_dir == "infra"
    functions = _functions(resolver)
    assert functions["Up"].code_uri == "functions/up"
    assert functions["Here"].code_uri == "infra"
    assert functions["Outside"].code_uri is None
    (machine,) = resolver.state_machines()
    assert machine.definition_file == "statemachine/up.asl.json"
    assert machine.template_file == "infra/template.yaml"
    assert machine.definition == {"StartAt": "A", "States": {"A": {"Type": "Succeed"}}}


@pytest.mark.parametrize(
    ("value", "directory", "expected"),
    [
        ("functions/x/", True, "functions/x"),
        ("./functions/x", True, "functions/x"),
        ("functions\\x\\", True, "functions/x"),
        (".", True, ""),
        ("./", True, ""),
        (".", False, None),
        ("a/../b", True, "b"),
        ("../x", True, None),
        ("..", True, None),
        ("s3://bucket/code.zip", True, None),
        ("dist/app.zip", True, None),
        ("dist/app.JAR", True, None),
        ("dist/app.zip", False, "dist/app.zip"),
        ("   ", True, None),
        ({"Bucket": "b", "Key": "k"}, True, None),
        (None, True, None),
        ("/elsewhere/code", True, None),
    ],
)
def test_local_path(tmp_path: Path, value: Any, directory: bool, expected: str | None) -> None:
    assert SamResolver(tmp_path).local_path(value, directory=directory) == expected


def test_local_path_accepts_absolute_paths_inside_the_repository(tmp_path: Path) -> None:
    (tmp_path / "functions" / "x").mkdir(parents=True)
    resolver = SamResolver(tmp_path)
    assert resolver.local_path(str(tmp_path / "functions" / "x"), directory=True) == "functions/x"
    assert resolver.local_path(str(tmp_path), directory=True) == ""


def test_absolute_template_path(tmp_path: Path) -> None:
    _write(tmp_path, "infra/template.yaml", "Resources: {}\n")
    resolver = SamResolver(tmp_path, str(tmp_path / "infra" / "template.yaml"))
    assert resolver.exists and resolver.template_file == "infra/template.yaml"


# =========================================================================== caching


def test_the_template_and_the_definitions_are_cached_per_mtime(tmp_path: Path) -> None:
    root = tmp_path / "cache"
    template = _write(
        root,
        "template.yaml",
        "Resources:\n  M:\n    Type: AWS::Serverless::StateMachine\n"
        "    Properties: {DefinitionUri: sm.asl.json}\n",
    )
    definition = _write(root, "sm.asl.json", '{"StartAt": "A", "States": {"A": {"Type": "Succeed"}}}')
    resolver = SamResolver(root)
    assert resolver.state_machines()[0].definition == {"StartAt": "A", "States": {"A": {"Type": "Succeed"}}}
    assert resolver._parsed() is resolver._parsed()  # served from the cache

    definition.write_text('{"StartAt": "B", "States": {"B": {"Type": "Fail"}}}', encoding="utf-8")
    os.utime(definition, ns=(1, 10**18))
    assert resolver.state_machines()[0].definition == {"StartAt": "B", "States": {"B": {"Type": "Fail"}}}

    template.write_text(template.read_text().replace("M:", "Renamed:"), encoding="utf-8")
    os.utime(template, ns=(1, 2 * 10**18))
    assert [machine.logical_id for machine in resolver.state_machines()] == ["Renamed"]

    definition.write_text("{broken", encoding="utf-8")
    os.utime(definition, ns=(1, 3 * 10**18))
    assert resolver.state_machines()[0].definition is None
    assert resolver.read_definition("missing.asl.json") is None


def test_a_definition_may_not_leave_the_repository_through_a_symlink(tmp_path: Path) -> None:
    outside = _write(tmp_path, "outside.asl.json", '{"StartAt": "A", "States": {"A": {"Type": "Succeed"}}}')
    root = tmp_path / "repo"
    _write(root, "inside.asl.json", '{"StartAt": "B", "States": {"B": {"Type": "Succeed"}}}')
    try:
        (root / "linked.asl.json").symlink_to(outside)
        (root / "alias.asl.json").symlink_to(root / "inside.asl.json")
    except OSError:
        pytest.skip("symlinks are not available")
    resolver = SamResolver(root)
    assert resolver.read_definition("linked.asl.json") is None
    assert resolver.read_definition("alias.asl.json") == {
        "StartAt": "B",
        "States": {"B": {"Type": "Succeed"}},
    }


def test_an_invalid_template_raises_on_use(tmp_path: Path) -> None:
    _write(tmp_path, "template.yaml", "Resources: [")
    resolver = SamResolver(tmp_path)
    assert resolver.exists
    with pytest.raises(ValueError, match="invalid template"):
        resolver.state_machines()
    with pytest.raises(ValueError, match="invalid template"):
        resolver.functions()


def test_function_for_ignores_non_lambda_targets(tmp_path: Path) -> None:
    resolver = _reference_repo(tmp_path)
    machine = ResolvedStateMachine("M", "template.yaml", substitutions={"A": {"Ref": "Alpha"}})
    assert resolver.function_for(machine, TaskTarget("lambda", "invoke", "${A}", INVOKE)) is not None
    assert resolver.function_for(machine, TaskTarget("activity", None, None, "${A}")) is None
    assert resolver.function_for(machine, TaskTarget("lambda", "invoke", None, INVOKE)) is None
