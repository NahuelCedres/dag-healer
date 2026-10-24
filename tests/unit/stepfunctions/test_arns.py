"""ARNs of the Step Functions adapter (``stepfunctions/arns.py``): parsing, building, run ids."""

from __future__ import annotations

from datetime import datetime, timedelta, timezone
from typing import Any

import pytest

from dag_healer.stepfunctions import arns
from dag_healer.stepfunctions.arns import (
    Arn,
    ExecutionArn,
    MapRunArn,
    StateMachineArn,
    execution_arn,
    execution_name,
    is_arn,
    lambda_function_arn,
    lambda_function_name,
    parse_arn,
    parse_execution_arn,
    parse_map_run_arn,
    parse_state_machine_arn,
    partition_for_region,
    run_id_for,
    run_names_execution,
    state_machine_arn,
    state_machine_name,
)

from .conftest import ACCOUNT, CUSTOMERS_SM, REGION

PREFIX = f"arn:aws:states:{REGION}:{ACCOUNT}"


# =========================================================================== generic ARNs


@pytest.mark.parametrize(
    ("text", "expected"),
    [
        (CUSTOMERS_SM, True),
        ("arn:aws:s3:::bucket/key", True),
        ("arn:aws:iam::123456789012:role/x", True),
        ("arn:aws:states", False),
        ("not-an-arn:a:b:c:d:e", False),
        ("", False),
        (None, False),
        (42, False),
    ],
)
def test_is_arn(text: object, expected: bool) -> None:
    assert is_arn(text) is expected


def test_parse_arn_splits_on_the_first_five_colons() -> None:
    arn = parse_arn(f"{PREFIX}:execution:customers-sync:a:b")
    assert arn == Arn("aws", "states", REGION, ACCOUNT, "execution:customers-sync:a:b")
    assert str(arn) == f"{PREFIX}:execution:customers-sync:a:b"
    assert parse_arn("arn:aws:s3:::bucket/key") == Arn("aws", "s3", "", "", "bucket/key")


@pytest.mark.parametrize(
    "text", ["arn:aws:states", "arn::states:r:a:x", "arn:aws::r:a:x", "arn:aws:states:r:a:", "x"]
)
def test_parse_arn_errors(text: str) -> None:
    with pytest.raises(ValueError, match="not an ARN"):
        parse_arn(text)


@pytest.mark.parametrize(
    ("region", "partition"),
    [
        ("eu-west-1", "aws"),
        ("cn-north-1", "aws-cn"),
        ("cn-northwest-1", "aws-cn"),
        ("us-gov-west-1", "aws-us-gov"),
        (None, "aws"),
        ("", "aws"),
    ],
)
def test_partition_for_region(region: str | None, partition: str) -> None:
    assert partition_for_region(region) == partition


# =========================================================================== state machines


@pytest.mark.parametrize(
    ("text", "expected"),
    [
        (CUSTOMERS_SM, StateMachineArn("aws", REGION, ACCOUNT, "customers-sync")),
        (f"{CUSTOMERS_SM}:7", StateMachineArn("aws", REGION, ACCOUNT, "customers-sync", qualifier="7")),
        (f"{CUSTOMERS_SM}:prod", StateMachineArn("aws", REGION, ACCOUNT, "customers-sync", qualifier="prod")),
        (f"{CUSTOMERS_SM}/Items", StateMachineArn("aws", REGION, ACCOUNT, "customers-sync", label="Items")),
        (
            "arn:aws-cn:states:cn-north-1:123456789012:stateMachine:etl",
            StateMachineArn("aws-cn", "cn-north-1", "123456789012", "etl"),
        ),
    ],
)
def test_parse_state_machine_arn(text: str, expected: StateMachineArn) -> None:
    machine = parse_state_machine_arn(text)
    assert machine == expected
    assert str(machine) == text
    scope = f"arn:{expected.partition}:states:{expected.region}:{expected.account}"
    assert machine.unqualified == f"{scope}:stateMachine:{expected.name}"
    assert machine.is_map_run_child is (expected.label is not None)


@pytest.mark.parametrize(
    "text",
    [
        f"{PREFIX}:execution:customers-sync:run",
        f"{PREFIX}:stateMachine:",
        f"{PREFIX}:stateMachine:/label",
        f"arn:aws:lambda:{REGION}:{ACCOUNT}:function:fn",
        f"{PREFIX}:activity:x",
        "customers-sync",
    ],
)
def test_parse_state_machine_arn_errors(text: str) -> None:
    with pytest.raises(ValueError):
        parse_state_machine_arn(text)


def test_state_machine_arn_builder() -> None:
    assert state_machine_arn(REGION, ACCOUNT, "customers-sync") == CUSTOMERS_SM
    assert (
        state_machine_arn("cn-north-1", ACCOUNT, "x")
        == f"arn:aws-cn:states:cn-north-1:{ACCOUNT}:stateMachine:x"
    )
    assert (
        state_machine_arn(REGION, ACCOUNT, "x", partition="aws-iso")
        == f"arn:aws-iso:states:{REGION}:{ACCOUNT}:stateMachine:x"
    )


@pytest.mark.parametrize(
    ("text", "name"),
    [
        (CUSTOMERS_SM, "customers-sync"),
        (f"{CUSTOMERS_SM}:3", "customers-sync"),
        ("  plain-name ", "plain-name"),
    ],
)
def test_state_machine_name(text: str, name: str) -> None:
    assert state_machine_name(text) == name


# =========================================================================== executions


@pytest.mark.parametrize(
    ("text", "expected"),
    [
        (
            f"{PREFIX}:execution:customers-sync:customers-2026-10-02",
            ExecutionArn("aws", REGION, ACCOUNT, "customers-sync", "customers-2026-10-02"),
        ),
        (
            f"{PREFIX}:execution:customers-sync/Items:0b6c2f",
            ExecutionArn("aws", REGION, ACCOUNT, "customers-sync", "0b6c2f", label="Items"),
        ),
        (
            f"{PREFIX}:express:customers-sync:run:4f1d",
            ExecutionArn("aws", REGION, ACCOUNT, "customers-sync", "run:4f1d", express=True),
        ),
        (
            f"{PREFIX}:execution:customers-sync:manual__2026-10-18T09:00:00+00:00 a/b",
            ExecutionArn("aws", REGION, ACCOUNT, "customers-sync", "manual__2026-10-18T09:00:00+00:00 a/b"),
        ),
    ],
    ids=["standard", "map run child", "express", "odd name"],
)
def test_parse_execution_arn(text: str, expected: ExecutionArn) -> None:
    execution = parse_execution_arn(text)
    assert execution == expected
    assert str(execution) == text
    assert execution.state_machine_arn == CUSTOMERS_SM
    assert execution.is_map_run_child is (expected.label is not None)


@pytest.mark.parametrize(
    "text",
    [
        CUSTOMERS_SM,
        f"{PREFIX}:execution:customers-sync",
        f"{PREFIX}:execution::run",
        f"{PREFIX}:execution:customers-sync:",
        f"{PREFIX}:mapRun:customers-sync/Items:1",
        "customers-2026-10-02",
    ],
)
def test_parse_execution_arn_errors(text: str) -> None:
    with pytest.raises(ValueError):
        parse_execution_arn(text)


@pytest.mark.parametrize(
    ("machine", "expected"),
    [
        (CUSTOMERS_SM, f"{PREFIX}:execution:customers-sync:run-1"),
        (f"{CUSTOMERS_SM}:7", f"{PREFIX}:execution:customers-sync:run-1"),
        (f"{CUSTOMERS_SM}/Items", f"{PREFIX}:execution:customers-sync/Items:run-1"),
    ],
)
def test_execution_arn_builder(machine: str, expected: str) -> None:
    assert execution_arn(machine, "run-1") == expected
    assert parse_execution_arn(expected).name == "run-1"


def test_execution_arn_builder_needs_a_state_machine_arn() -> None:
    with pytest.raises(ValueError):
        execution_arn("customers-sync", "run-1")


# =========================================================================== map runs


def test_parse_map_run_arn() -> None:
    text = f"{PREFIX}:mapRun:InventoryReportStateMachine-XyZ789/PerWarehouse:3c39a231-0a2b"
    assert parse_map_run_arn(text) == MapRunArn(
        "aws", REGION, ACCOUNT, "InventoryReportStateMachine-XyZ789", "PerWarehouse", "3c39a231-0a2b"
    )


@pytest.mark.parametrize(
    "text", [f"{PREFIX}:mapRun:machine", f"{PREFIX}:mapRun:/label:1", f"{PREFIX}:mapRun:m/:1", CUSTOMERS_SM]
)
def test_parse_map_run_arn_errors(text: str) -> None:
    with pytest.raises(ValueError):
        parse_map_run_arn(text)


# =========================================================================== lambda


@pytest.mark.parametrize(
    ("text", "name"),
    [
        (
            f"arn:aws:lambda:{REGION}:{ACCOUNT}:function:sfn-etl-SalesFunction-1A2B3C4D5E6F",
            "sfn-etl-SalesFunction-1A2B3C4D5E6F",
        ),
        (f"arn:aws:lambda:{REGION}:{ACCOUNT}:function:fn:live", "fn"),
        (f"arn:aws:lambda:{REGION}:{ACCOUNT}:function:fn:$LATEST", "fn"),
        (f"{ACCOUNT}:function:fn", "fn"),
        (f"{ACCOUNT}:function:fn:3", "fn"),
        ("fn:live", "fn"),
        ("  fn  ", "fn"),
    ],
)
def test_lambda_function_name(text: str, name: str) -> None:
    assert lambda_function_name(text) == name


@pytest.mark.parametrize(
    "text",
    [
        CUSTOMERS_SM,
        f"arn:aws:lambda:{REGION}:{ACCOUNT}:layer:etl:3",
        f"arn:aws:lambda:{REGION}:{ACCOUNT}:function:",
    ],
)
def test_lambda_function_name_errors(text: str) -> None:
    with pytest.raises(ValueError):
        lambda_function_name(text)


def test_lambda_function_arn_builder() -> None:
    assert lambda_function_arn(REGION, ACCOUNT, "fn") == f"arn:aws:lambda:{REGION}:{ACCOUNT}:function:fn"
    assert lambda_function_arn("us-gov-east-1", ACCOUNT, "fn").startswith("arn:aws-us-gov:lambda:")


# =========================================================================== run ids


START = datetime(2026, 10, 2, 6, 0, tzinfo=timezone.utc)


def test_run_id_for() -> None:
    assert run_id_for("customers-2026-10-02") == "customers-2026-10-02"
    assert run_id_for("customers-2026-10-02", START) == "customers-2026-10-02"
    assert run_id_for("customers-2026-10-02", START, with_start=True) == "customers-2026-10-02@1790920800000"
    assert run_id_for("customers-2026-10-02", None, with_start=True) == "customers-2026-10-02"
    naive = START.replace(tzinfo=None)
    assert run_id_for("x", naive, with_start=True) == "x@1790920800000", "a naive datetime is UTC"
    other_zone = START.astimezone(timezone(timedelta(hours=-3)))
    assert run_id_for("x", other_zone, with_start=True) == "x@1790920800000"


@pytest.mark.parametrize(
    ("moment", "ms"),
    [
        (datetime(2004, 3, 15, 7, 55, 47, 472000, tzinfo=timezone.utc), 1079337347472),
        (datetime(2039, 3, 8, 19, 46, 13, 411000, tzinfo=timezone.utc), 2183226373411),
        (datetime(2026, 10, 2, 6, 0, 41, 512999, tzinfo=timezone.utc), 1790920841512),
    ],
)
def test_run_id_milliseconds_are_exact(moment: datetime, ms: int) -> None:
    """``timestamp() * 1000`` truncated the first two to the millisecond before."""
    assert run_id_for("x", moment, with_start=True) == f"x@{ms}"
    assert execution_name(f"x@{ms}") == "x"


@pytest.mark.parametrize(
    ("run_id", "name"),
    [
        ("customers-2026-10-02@1790920800000", "customers-2026-10-02"),
        ("customers-2026-10-02", "customers-2026-10-02"),
        ("name@with-at", "name@with-at"),
        ("name@123", "name@123"),
        ("a@b@1790920800000", "a@b"),
        ("@1790920800000", "@1790920800000"),
    ],
)
def test_execution_name(run_id: str, name: str) -> None:
    assert execution_name(run_id) == name


def test_run_ids_round_trip() -> None:
    for offset in range(0, 5000, 7):
        moment = START + timedelta(milliseconds=offset)
        assert execution_name(run_id_for("etl-run", moment, with_start=True)) == "etl-run"
        assert execution_name(run_id_for("etl-run", moment, with_start=True), start=moment) == "etl-run"
    assert arns._epoch_ms(START) == 1790920800000


@pytest.mark.parametrize(
    ("run_id", "start", "name"),
    [
        ("nightly@1790920800000", START, "nightly"),  # the reuse marker of an execution of START
        ("nightly@1790920800000", 1790920800000, "nightly"),
        ("nightly@1790920800000", START.replace(tzinfo=None), "nightly"),
        ("nightly@1790920800000", START + timedelta(milliseconds=1), "nightly@1790920800000"),  # a name
        ("nightly@1790920800000", 42, "nightly@1790920800000"),
        ("nightly@1790920800000", None, "nightly"),  # unknown start: taken for the marker
        ("nightly@1790920800000", True, "nightly"),  # not a start
        ("nightly", START, "nightly"),
    ],
)
def test_execution_name_with_the_start_date(run_id: str, start: Any, name: str) -> None:
    """``@`` and digits are valid in an execution name: the suffix is a marker only when it is
    the execution's start."""
    assert execution_name(run_id, start=start) == name


@pytest.mark.parametrize(
    ("run_id", "name", "start", "expected"),
    [
        ("nightly", "nightly", START, True),
        ("nightly", "nightly", None, True),
        ("nightly@1790920800000", "nightly@1790920800000", START, True),  # the name itself
        ("nightly@1790920800000", "nightly@1790920800000", None, True),
        ("nightly@1790920800000", "nightly", START, True),  # the marker of this start
        ("nightly@1790920800000", "nightly", 1790920800000, True),
        ("nightly@1790920800000", "nightly", None, True),  # cannot be checked: the name decides
        ("nightly@1790920800000", "nightly", START + timedelta(days=91), False),  # a later execution
        ("nightly@1790920800000", "other", START, False),
        ("nightly", "nightly@1790920800000", START, False),
        ("nightly", "other", None, False),
    ],
)
def test_run_names_execution(run_id: str, name: str, start: Any, expected: bool) -> None:
    assert run_names_execution(run_id, name, start) is expected
