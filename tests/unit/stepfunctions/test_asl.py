"""asl: loading definitions, the state index, Task targets, S3 references and substitutions.

Table tests on the definitions of ``tests/unit/fixtures/stepfunctions/asl`` (every state type,
Map/Parallel nesting, JSONata, YAML) and on the ASL files of the fixture repository.
"""

from __future__ import annotations

import json
from types import MappingProxyType
from typing import Any

import pytest

from dag_healer.stepfunctions import asl
from dag_healer.stepfunctions.asl import S3Reference, StateInfo, TaskTarget

from .conftest import FIXTURES, SFN_REPO

ASL = FIXTURES / "asl"
INVOKE = "arn:aws:states:::lambda:invoke"
FN_ARN = "arn:aws:lambda:eu-west-1:123456789012:function:sales"


def _definition(name: str) -> str:
    return (ASL / name).read_text(encoding="utf-8")


def _task(resource: Any = INVOKE, **fields: Any) -> StateInfo:
    return StateInfo(name="Step", type="Task", resource=resource, **fields)


# =========================================================================== load_definition


def test_load_definition_reads_json_text_bytes_and_mappings() -> None:
    text = '{"StartAt": "A", "States": {"A": {"Type": "Succeed"}}}'
    expected = {"StartAt": "A", "States": {"A": {"Type": "Succeed"}}}
    assert asl.load_definition(text) == expected
    assert asl.load_definition(("\ufeff" + text).encode("utf-8")) == expected
    assert asl.load_definition(bytearray(text, "utf-8")) == expected
    frozen = MappingProxyType(
        {"StartAt": "A", "States": MappingProxyType({"A": {"Type": "Pass", "Next": ()}})}
    )
    loaded = asl.load_definition(frozen)
    assert loaded == {"StartAt": "A", "States": {"A": {"Type": "Pass", "Next": []}}}
    assert type(loaded["States"]) is dict


def test_load_definition_returns_a_private_copy_of_a_mapping() -> None:
    source = {"StartAt": "A", "States": {"A": {"Type": "Pass", "Parameters": {"x": [1]}}}}
    loaded = asl.load_definition(source)
    loaded["States"]["A"]["Parameters"]["x"].append(2)
    assert source["States"]["A"]["Parameters"]["x"] == [1]


def test_load_definition_reads_yaml_and_keeps_timestamps_as_text() -> None:
    definition = asl.load_definition(_definition("definition.asl.yaml"))
    choice = definition["States"]["Check"]["Choices"][0]
    assert choice["TimestampGreaterThan"] == "2026-10-01T00:00:00Z"
    assert definition["States"]["Refresh"]["Parameters"]["FunctionName"] == "${RefreshFunctionArn}"


def test_load_definition_quotes_placeholders_written_outside_strings() -> None:
    text = """{
      "Comment": "keep \\"${Stage}\\" and ${Stage} inside strings",
      "StartAt": "Run",
      "States": {
        "Run": {
          "Type": "Map",
          "MaxConcurrency": ${Concurrency},
          "ItemProcessor": {
            "StartAt": "Step",
            "States": {
              "Step": {
                "Type": "Task", "Resource": "${StepFunctionArn}", "TimeoutSeconds": ${Timeout}, "End": true
              }
            }
          },
          "End": true
        }
      }
    }"""
    definition = asl.load_definition(text)
    run = definition["States"]["Run"]
    assert run["MaxConcurrency"] == "${Concurrency}"
    assert run["ItemProcessor"]["States"]["Step"]["TimeoutSeconds"] == "${Timeout}"
    assert run["ItemProcessor"]["States"]["Step"]["Resource"] == "${StepFunctionArn}"
    assert definition["Comment"] == 'keep "${Stage}" and ${Stage} inside strings'


@pytest.mark.parametrize(
    "definition",
    [
        "",
        "[]",
        "{}",
        '{"StartAt": "A"}',
        '{"StartAt": "A", "States": []}',
        '{"StartAt": "A", "States": "A"}',
        "{ not json, : nor yaml [",
        "just a sentence",
        b"\xff\xfe\x00not utf-8",
        42,
        None,
        ["States"],
        "[" * 100_000,
    ],
    ids=lambda value: repr(value)[:24],
)
def test_load_definition_refuses_what_is_not_a_definition(definition: Any) -> None:
    with pytest.raises(ValueError, match="not a state machine definition"):
        asl.load_definition(definition)


def test_load_definition_errors_never_quote_the_definition() -> None:
    with pytest.raises(ValueError) as raised:
        asl.load_definition('{"States": "secret-value-123"}')
    assert "secret-value-123" not in str(raised.value)


# =========================================================================== index_states


def test_index_states_flattens_every_nested_state_in_definition_order() -> None:
    states = asl.index_states(_definition("nested.asl.json"))
    assert list(states) == [
        "Fetch",
        "Fan",
        "Clean",
        "PerItem",
        "Score",
        "Distribute",
        "Enrich",
        "Route",
        "Pause",
        "Flags",
        "Publish",
        "Archive",
        "Notify",
        "Container",
        "Batch",
        "Call",
        "Approve",
        "Child",
        "Topic",
        "Done",
        "Failed",
    ]
    table = {name: (state.type, state.parent_path, state.map_mode) for name, state in states.items()}
    assert table["Fan"] == ("Parallel", (), None)
    assert table["Clean"] == ("Task", ("Fan",), None)
    assert table["PerItem"] == ("Map", ("Fan",), "INLINE")  # the legacy Iterator is inline
    assert table["Score"] == ("Task", ("Fan", "PerItem"), None)
    assert table["Distribute"] == ("Map", (), "DISTRIBUTED")
    assert table["Enrich"] == ("Task", ("Distribute",), None)
    assert {table[name][0] for name in ("Route", "Pause", "Flags", "Done", "Failed")} == {
        "Choice",
        "Wait",
        "Pass",
        "Succeed",
        "Fail",
    }


def test_index_states_keeps_the_static_parts_of_a_state() -> None:
    states = asl.index_states(_definition("nested.asl.json"))
    fetch = states["Fetch"]
    assert fetch.resource == INVOKE
    assert fetch.parameters == {
        "FunctionName": "arn:aws:lambda:eu-west-1:123456789012:function:fetch:live",
        "Payload.$": "$",
    }
    assert fetch.arguments is None
    assert fetch.retry == (
        {"ErrorEquals": ["Lambda.ServiceException"], "MaxAttempts": 2},
    )  # odd items dropped
    assert fetch.catch == ({"ErrorEquals": ["States.ALL"], "Next": "Failed"},)
    distribute = states["Distribute"]
    assert (
        distribute.item_reader is not None
        and distribute.item_reader["Resource"] == "arn:aws:states:::s3:getObject"
    )
    assert (
        distribute.result_writer is not None and distribute.result_writer["Parameters"]["Prefix"] == "orders/"
    )
    assert states["Flags"].resource is None  # only Task states have a Resource
    assert states["Flags"].parameters == {"Bucket": "acme-landing", "Key": "lookup/flags.json"}
    assert states["Route"].item_reader is None and states["Route"].map_mode is None


def test_index_states_follows_the_query_language() -> None:
    states = asl.index_states(_definition("jsonata.asl.json"))
    assert {name: state.query_language for name, state in states.items()} == {
        "Score": "JSONata",
        "Pick": "JSONata",
        "Legacy": "JSONPath",
        "Each": "JSONata",
        "Load": "JSONata",
    }
    assert states["Each"].map_mode == "DISTRIBUTED"  # "distributed" is normalised
    nested = {
        "StartAt": "M",
        "States": {
            "M": {
                "Type": "Map",
                "QueryLanguage": "jsonata",
                "ItemProcessor": {"StartAt": "In", "States": {"In": {"Type": "Pass", "End": True}}},
                "End": True,
            }
        },
    }
    assert asl.index_states(nested)["In"].query_language == "JSONata"  # inherited from the Map


def test_index_states_tolerates_odd_definitions() -> None:
    definition = {
        "StartAt": "A",
        "States": {
            "A": {"Type": "Task", "Resource": 42, "Parameters": "not a template", "Retry": "x", "Next": "B"},
            "B": "not a state",
            "C": {"Type": "Parallel", "Branches": ["not a branch", {"States": {"A": {"Type": "Pass"}}}]},
            "D": {"Comment": "no Type"},
            "E": {
                "Type": "Map",
                "ItemProcessor": "not a machine",
                "Iterator": {"States": {"F": {"Type": "Pass"}}},
            },
            7: {"Type": "Pass"},
        },
    }
    states = asl.index_states(definition)
    assert list(states) == ["A", "C", "D", "E", "F", "7"]  # B skipped; names are text, as in JSON
    assert states["A"].resource is None and states["A"].parameters is None and states["A"].retry == ()
    assert states["A"].parent_path == ()  # the duplicate "A" inside C keeps the first occurrence
    assert states["D"].type == ""
    assert states["F"].parent_path == ("E",)


def test_index_states_stops_at_the_nesting_limit() -> None:
    machine: dict[str, Any] = {"StartAt": "Leaf", "States": {"Leaf": {"Type": "Pass", "End": True}}}
    for depth in range(asl.MAX_NESTING + 5):
        machine = {
            "StartAt": f"Map{depth}",
            "States": {f"Map{depth}": {"Type": "Map", "ItemProcessor": machine, "End": True}},
        }
    states = asl.index_states(machine)
    assert "Leaf" not in states
    assert len(states) == asl.MAX_NESTING + 1


def test_index_states_refuses_a_non_definition() -> None:
    with pytest.raises(ValueError):
        asl.index_states('{"no": "states"}')


# =========================================================================== compute_target


@pytest.mark.parametrize(
    ("state", "expected"),
    [
        (
            _task(parameters={"FunctionName": FN_ARN, "Payload.$": "$"}),
            TaskTarget("lambda", "invoke", FN_ARN, INVOKE),
        ),
        (
            _task(parameters={"FunctionName": "${SalesFunctionArn}"}),
            TaskTarget("lambda", "invoke", "${SalesFunctionArn}", INVOKE),
        ),
        (_task(parameters={"FunctionName.$": "$.function"}), TaskTarget("lambda", "invoke", None, INVOKE)),
        (_task(parameters={"FunctionName": 7}), TaskTarget("lambda", "invoke", None, INVOKE)),
        (_task(parameters={"Payload": {}}), TaskTarget("lambda", "invoke", None, INVOKE)),
        (_task(), TaskTarget("lambda", "invoke", None, INVOKE)),
        (
            _task(f"{INVOKE}.waitForTaskToken", parameters={"FunctionName": " score "}),
            TaskTarget("lambda", "invoke.waitForTaskToken", "score", f"{INVOKE}.waitForTaskToken"),
        ),
        (
            _task(arguments={"FunctionName": FN_ARN}, query_language="JSONata"),
            TaskTarget("lambda", "invoke", FN_ARN, INVOKE),
        ),
        (
            _task(arguments={"FunctionName": "{% $states.input.fn %}"}, query_language="JSONata"),
            TaskTarget("lambda", "invoke", None, INVOKE),
        ),
        (
            _task(
                arguments={"FunctionName": "jsonata-fn"},
                parameters={"FunctionName": "jsonpath-fn"},
                query_language="JSONata",
            ),
            TaskTarget("lambda", "invoke", "jsonata-fn", INVOKE),
        ),
        (
            _task(arguments={"FunctionName": "jsonata-fn"}, parameters={"FunctionName": "jsonpath-fn"}),
            TaskTarget("lambda", "invoke", "jsonpath-fn", INVOKE),
        ),
        (
            _task("arn:aws-cn:states:::lambda:invoke", parameters={"FunctionName": "cn-fn"}),
            TaskTarget("lambda", "invoke", "cn-fn", "arn:aws-cn:states:::lambda:invoke"),
        ),
        (_task(FN_ARN), TaskTarget("lambda", "legacy", FN_ARN, FN_ARN)),
        (_task(f"{FN_ARN}:live"), TaskTarget("lambda", "legacy", f"{FN_ARN}:live", f"{FN_ARN}:live")),
        (
            _task("arn:aws-us-gov:lambda:us-gov-west-1:123456789012:function:gov"),
            TaskTarget(
                "lambda",
                "legacy",
                "arn:aws-us-gov:lambda:us-gov-west-1:123456789012:function:gov",
                "arn:aws-us-gov:lambda:us-gov-west-1:123456789012:function:gov",
            ),
        ),
        (
            _task(" ${InventoryFunctionArn} "),
            TaskTarget("lambda", "legacy", "${InventoryFunctionArn}", "${InventoryFunctionArn}"),
        ),
        (
            _task("arn:aws:lambda:${AWS::Region}:${AWS::AccountId}:function:${Fn}"),
            TaskTarget(
                "lambda",
                "legacy",
                "arn:aws:lambda:${AWS::Region}:${AWS::AccountId}:function:${Fn}",
                "arn:aws:lambda:${AWS::Region}:${AWS::AccountId}:function:${Fn}",
            ),
        ),
        (
            _task("arn:aws:states:::aws-sdk:lambda:invoke", parameters={"FunctionName": "notify"}),
            TaskTarget("lambda", "aws-sdk:invoke", "notify", "arn:aws:states:::aws-sdk:lambda:invoke"),
        ),
        (
            _task("arn:aws:states:::aws-sdk:s3:getObject", parameters={"FunctionName": "ignored"}),
            TaskTarget("other", "aws-sdk:s3:getObject", None, "arn:aws:states:::aws-sdk:s3:getObject"),
        ),
        (
            _task("arn:aws:states:::ecs:runTask.sync"),
            TaskTarget("ecs", "runTask.sync", None, "arn:aws:states:::ecs:runTask.sync"),
        ),
        (
            _task("arn:aws:states:::batch:submitJob.sync"),
            TaskTarget("batch", "submitJob.sync", None, "arn:aws:states:::batch:submitJob.sync"),
        ),
        (
            _task("arn:aws:states:::glue:startJobRun"),
            TaskTarget("glue", "startJobRun", None, "arn:aws:states:::glue:startJobRun"),
        ),
        (
            _task("arn:aws:states:::states:startExecution.sync:2"),
            TaskTarget(
                "states", "startExecution.sync:2", None, "arn:aws:states:::states:startExecution.sync:2"
            ),
        ),
        (
            _task("arn:aws:states:::http:invoke"),
            TaskTarget("http", "invoke", None, "arn:aws:states:::http:invoke"),
        ),
        (
            _task("arn:aws:states:::sns:publish"),
            TaskTarget("other", "publish", None, "arn:aws:states:::sns:publish"),
        ),
        (
            _task("arn:aws:states:eu-west-1:123456789012:activity:approve"),
            TaskTarget("activity", None, None, "arn:aws:states:eu-west-1:123456789012:activity:approve"),
        ),
        (_task("not-an-arn"), TaskTarget("other", None, None, "not-an-arn")),
        (_task(None), TaskTarget("other")),
        (_task("   "), TaskTarget("other")),
    ],
    ids=lambda value: value.resource if isinstance(value, StateInfo) else "",
)
def test_compute_target(state: StateInfo, expected: TaskTarget) -> None:
    assert asl.compute_target(state) == expected


@pytest.mark.parametrize("kind", ["Pass", "Map", "Parallel", "Choice", "Wait", "Succeed", "Fail", ""])
def test_compute_target_is_none_for_other_state_types(kind: str) -> None:
    assert asl.compute_target(StateInfo(name="S", type=kind, resource=INVOKE)) is None


def test_compute_target_on_the_nested_fixture() -> None:
    states = asl.index_states(_definition("nested.asl.json"))
    targets = {name: asl.compute_target(state) for name, state in states.items()}
    kinds = {name: target.kind for name, target in targets.items() if target is not None}
    assert kinds == {
        "Fetch": "lambda",
        "Clean": "lambda",
        "Score": "lambda",
        "Enrich": "lambda",
        "Publish": "glue",
        "Archive": "other",
        "Notify": "lambda",
        "Container": "ecs",
        "Batch": "batch",
        "Call": "http",
        "Approve": "activity",
        "Child": "states",
        "Topic": "other",
    }
    assert targets["Enrich"].function is None  # FunctionName.$ is a JSONPath


# =========================================================================== s3_references


def test_s3_references_of_the_nested_fixture() -> None:
    references = asl.s3_references(asl.index_states(_definition("nested.asl.json")))
    assert references == [
        S3Reference("Distribute", "item_reader", "acme-landing", "incoming/orders.csv", False, "CSV"),
        S3Reference("Distribute", "result_writer", "acme-results", "orders/", False, None),
        S3Reference("Flags", "parameters", "acme-landing", "lookup/flags.json", False, None),
        S3Reference("Publish", "parameters", "acme-results", "orders/manifest.json", False, None),
        S3Reference("Publish", "parameters", "acme-logs", "publish/", False, None),
        S3Reference("Archive", "parameters", "acme-archive", None, True, None),  # Key.$ is a JSONPath
    ]
    assert [reference.uri for reference in references] == [
        "s3://acme-landing/incoming/orders.csv",
        "s3://acme-results/orders/",
        "s3://acme-landing/lookup/flags.json",
        "s3://acme-results/orders/manifest.json",
        "s3://acme-logs/publish/",
        None,
    ]


def test_s3_references_of_jsonata_expressions_are_dynamic() -> None:
    references = asl.s3_references(asl.index_states(_definition("jsonata.asl.json")))
    assert references == [
        S3Reference("Each", "item_reader", "acme-landing", None, True, None),
        S3Reference("Load", "parameters", None, "incoming/data.parquet", True, None),
    ]


@pytest.mark.parametrize(
    ("template", "expected"),
    [
        ({"Bucket": "b", "Key": "k.csv"}, [S3Reference("S", "parameters", "b", "k.csv")]),
        ({"Bucket": "b", "Prefix": "daily/"}, [S3Reference("S", "parameters", "b", "daily/")]),
        ({"Bucket": "b"}, [S3Reference("S", "parameters", "b", None)]),
        ({"Bucket.$": "$.bucket", "Key": "k.csv"}, [S3Reference("S", "parameters", None, "k.csv", True)]),
        (
            {"Bucket": "${LandingBucket}", "Key": "k.csv"},
            [S3Reference("S", "parameters", None, "k.csv", True)],
        ),
        (
            {"Bucket": {"Ref": "LandingBucket"}, "Key": "k.csv"},
            [S3Reference("S", "parameters", None, "k.csv", True)],
        ),
        ({"Key": "k.csv"}, []),
        (
            {"Payload": {"inputs": ["s3://b/one.csv", "s3://b/two.json"], "path.$": "s3://b/never.csv"}},
            [S3Reference("S", "parameters", "b", "one.csv"), S3Reference("S", "parameters", "b", "two.json")],
        ),
        (
            {"Payload": {"input": "s3://${Bucket}/x.csv"}},
            [S3Reference("S", "parameters", "${Bucket}", "x.csv", True)],
        ),
        ({"Payload": {"input": "s3://"}}, []),
        ({"Payload": {"input": "s3://b/with space.csv"}}, []),
        ({"Payload": {"input": "see s3://b/x.csv"}}, []),
        (
            {"Bucket": "b", "Key": "k.csv", "Copy": "s3://b/k.csv"},
            [S3Reference("S", "parameters", "b", "k.csv")],  # the same location once
        ),
    ],
)
def test_s3_references_of_parameters(template: dict[str, Any], expected: list[S3Reference]) -> None:
    state = StateInfo(
        name="S", type="Task", resource="arn:aws:states:::aws-sdk:s3:getObject", parameters=template
    )
    assert asl.s3_references({"S": state}) == expected


def test_s3_references_ignore_deeply_nested_parameters() -> None:
    template: Any = "s3://b/deep.csv"
    for _ in range(40):
        template = {"x": template}
    state = StateInfo(name="S", type="Pass", parameters=template)
    assert asl.s3_references({"S": state}) == []


def test_s3_reference_uri() -> None:
    assert S3Reference("S", "parameters", "b", "k.csv").uri == "s3://b/k.csv"
    assert S3Reference("S", "parameters", "b").uri == "s3://b/"
    assert S3Reference("S", "parameters", "b", "k.csv", dynamic=True).uri is None
    assert S3Reference("S", "parameters", None, "k.csv").uri is None


# =========================================================================== substitute and placeholders


def test_substitute_replaces_string_placeholders_everywhere() -> None:
    value = {
        "Resource": "${FnArn}",
        "Parameters": {"FunctionName": "${FnArn}", "Note": "${Stage}-${Stage} ${Missing}", "Count": 3},
        "List": ["${Stage}", ("${FnArn}", None)],
        "${Stage}Key": True,
    }
    out = asl.substitute(value, {"FnArn": FN_ARN, "Stage": "prod", "Ref": {"Ref": "Fn"}, "Number": 5})
    assert out == {
        "Resource": FN_ARN,
        "Parameters": {"FunctionName": FN_ARN, "Note": "prod-prod ${Missing}", "Count": 3},
        "List": ["prod", (FN_ARN, None)],
        "prodKey": True,
    }
    assert value["Resource"] == "${FnArn}"  # the input is untouched


def test_substitute_leaves_non_string_substitutions_and_pseudo_parameters() -> None:
    value = "arn:aws:lambda:${AWS::Region}:${AWS::AccountId}:function:${Fn}"
    assert asl.substitute(value, {"Fn": {"Fn::GetAtt": ["Fn", "Arn"]}, "AWS::Region": 1}) == value
    assert asl.substitute(value, {}) == value
    assert asl.substitute(["x"], None) == ["x"]  # type: ignore[arg-type]


def test_substitute_copies_containers() -> None:
    value = {"a": ["x"]}
    out = asl.substitute(value, {})
    assert out == value and out is not value and out["a"] is not value["a"]


@pytest.mark.parametrize(
    ("text", "name", "names", "dynamic"),
    [
        ("${FnArn}", "FnArn", ["FnArn"], True),
        (" ${Fn.Arn} ", "Fn.Arn", ["Fn.Arn"], True),
        ("${A}-${B}-${A}", None, ["A", "B"], True),
        ("arn:aws:lambda:eu-west-1:1:function:x", None, [], False),
        ("{% $states.input.x %}", None, [], True),
        ("${}", None, [], True),
        (42, None, [], True),
    ],
)
def test_placeholder_helpers(text: Any, name: str | None, names: list[str], dynamic: bool) -> None:
    assert asl.placeholder_name(text) == name
    assert asl.placeholder_names(text) == names
    assert asl.is_dynamic(text) is dynamic


# =========================================================================== the fixture repository


@pytest.mark.parametrize(
    ("file", "states", "placeholder"),
    [
        ("sales_daily_etl.asl.json", ["ExtractSales", "TransformSales", "LoadSales"], "${SalesFunctionArn}"),
        (
            "customers_sync.asl.json",
            ["ExtractCustomers", "TransformCustomers", "LoadCustomers"],
            "${CustomersFunctionArn}",
        ),
        (
            "inventory_report.asl.json",
            ["GetInventory", "PerWarehouse", "BuildReport", "PublishReport"],
            "${InventoryFunctionArn}",
        ),
    ],
)
def test_the_fixture_repository_definitions(file: str, states: list[str], placeholder: str) -> None:
    index = asl.index_states((SFN_REPO / "statemachine" / file).read_bytes())
    assert list(index) == states
    targets = {name: asl.compute_target(state) for name, state in index.items()}
    lambdas = {name: target for name, target in targets.items() if target is not None}
    assert all(target.kind == "lambda" and target.function == placeholder for target in lambdas.values())
    assert asl.s3_references(index) == []
    if "BuildReport" in index:
        assert index["BuildReport"].parent_path == ("PerWarehouse",)
        assert index["PerWarehouse"].map_mode == "INLINE"
        assert lambdas["BuildReport"].integration == "legacy"
        assert lambdas["GetInventory"].integration == "invoke"
    deployed = asl.substitute(
        asl.load_definition(json.loads((SFN_REPO / "statemachine" / file).read_text())),
        {placeholder[2:-1]: FN_ARN},
    )
    assert all(
        asl.compute_target(state).function == FN_ARN  # type: ignore[union-attr]
        for state in asl.index_states(deployed).values()
        if state.type == "Task"
    )
