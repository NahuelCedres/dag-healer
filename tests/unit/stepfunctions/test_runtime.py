"""What the ports share (``stepfunctions/runtime.py``): timestamps, ``native_ref``, the pipeline
index and :class:`SfnRuntime` (``state_machine_arn`` through a stubbed ``ListStateMachines``)."""

from __future__ import annotations

import json
import logging
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

import pytest

from dag_healer.config import config_from_mapping
from dag_healer.core.errors import OrchestratorError, PermissionDenied
from dag_healer.core.ids import AttemptRef
from dag_healer.stepfunctions.runtime import (
    MAX_NATIVE_BYTES,
    NATIVE_KEYS,
    PipelineEntry,
    PipelineIndex,
    epoch_ms,
    iso,
    native_int,
    native_ref,
    native_text,
    to_utc,
)

from .conftest import (
    ACCOUNT,
    CUSTOMERS_SM,
    INSTANCE,
    INVENTORY_SM,
    NOW,
    PIPELINES,
    REGION,
    SALES_SM,
    sfn_options,
)

UTC = timezone.utc
MOMENT = datetime(2026, 10, 2, 6, 0, 41, 512000, tzinfo=UTC)
EXECUTION = f"arn:aws:states:{REGION}:{ACCOUNT}:execution:customers-sync:customers-2026-10-02"


# =========================================================================== timestamps


@pytest.mark.parametrize(
    ("value", "expected"),
    [
        (MOMENT, MOMENT),
        (MOMENT.replace(tzinfo=None), MOMENT),
        (MOMENT.astimezone(timezone(timedelta(hours=-3))), MOMENT),
        ("2026-10-02T06:00:41.512Z", MOMENT),
        ("2026-10-02T06:00:41.512z", MOMENT),
        ("2026-10-02T03:00:41.512-03:00", MOMENT),
        ("2026-10-02T06:00:41.512", MOMENT),
        (" 2026-10-02T06:00:41.512+00:00 ", MOMENT),
        (1790920841512, MOMENT),
        (1790920841512.0, MOMENT),
        ("1790920841512", MOMENT),
        (0, datetime(1970, 1, 1, tzinfo=UTC)),
        (True, None),
        (None, None),
        ("", None),
        ("   ", None),
        ("yesterday", None),
        ("-5", None),
        (float("nan"), None),
        (float("inf"), None),
        (10**30, None),
        ([2026, 10, 2], None),
    ],
)
def test_to_utc(value: Any, expected: datetime | None) -> None:
    result = to_utc(value)
    assert result == expected
    if result is not None:
        assert result.tzinfo is not None and result.utcoffset() == timedelta(0)


def test_iso_has_milliseconds() -> None:
    assert iso(MOMENT) == "2026-10-02T06:00:41.512+00:00"
    assert iso("2026-10-02T06:00:00Z") == "2026-10-02T06:00:00.000+00:00"
    assert iso(1790920841512) == "2026-10-02T06:00:41.512+00:00"
    assert iso("garbage") is None and iso(None) is None


def test_epoch_ms() -> None:
    assert epoch_ms(MOMENT) == 1790920841512
    assert epoch_ms("2026-10-02T06:00:41.512Z") == 1790920841512
    assert epoch_ms(1790920841512) == 1790920841512
    assert epoch_ms(None) is None and epoch_ms("x") is None


@pytest.mark.parametrize("microseconds", [0, 1, 499, 500, 999, 512999])
def test_epoch_ms_truncates_like_iso(microseconds: int) -> None:
    moment = MOMENT.replace(microsecond=microseconds)
    assert epoch_ms(moment) == epoch_ms(iso(moment))
    assert epoch_ms(moment) == 1790920841000 + microseconds // 1000


def test_epoch_ms_round_trips() -> None:
    for ms in (0, 1, 999, 1079337347472, 1790920841512, 2183226373411, 4102444800000):
        assert epoch_ms(to_utc(ms)) == ms
        assert epoch_ms(iso(ms)) == ms


# =========================================================================== native_ref


def test_native_ref_keeps_known_keys_in_order() -> None:
    native = native_ref(
        {"status": "FAILED", "execution_arn": EXECUTION},
        stop_date=MOMENT,
        redrive_count=0,
        localized=True,
    )
    assert list(native) == ["execution_arn", "stop_date", "status", "redrive_count", "localized"]
    assert native == {
        "execution_arn": EXECUTION,
        "stop_date": "2026-10-02T06:00:41.512+00:00",
        "status": "FAILED",
        "redrive_count": 0,
        "localized": True,
    }
    assert [key for key in NATIVE_KEYS if key in native] == list(native)


def test_native_ref_drops_unknown_keys_without_logging_values(caplog: pytest.LogCaptureFixture) -> None:
    caplog.set_level(logging.DEBUG)
    native = native_ref(input='{"email": "alice.pii@example.com"}', cause="secret cause", status="FAILED")
    assert native == {"status": "FAILED"}
    logged = " ".join(record.getMessage() for record in caplog.records)
    assert "'input'" in logged and "'cause'" in logged
    assert "alice.pii@example.com" not in logged and "secret cause" not in logged


def test_native_ref_none_removes_a_base_key_and_leaves_the_base_alone() -> None:
    base = {"status": "FAILED", "error": "KeyError"}
    assert native_ref(base, error=None) == {"status": "FAILED"}
    assert base == {"status": "FAILED", "error": "KeyError"}


def test_native_ref_drops_payload_keys_at_any_depth() -> None:
    native = native_ref(
        failed_children=[
            {
                "execution_arn": "a",
                "Input": "{...}",
                "detail": {"output": "x", "parameters": {"k": 1}, "ok": 1},
            },
            {"execution_arn": "b"},
        ]
    )
    assert native == {
        "failed_children": [{"execution_arn": "a", "detail": {"ok": 1}}, {"execution_arn": "b"}]
    }


def test_native_ref_caps_text_and_children() -> None:
    native = native_ref(error="E" * 2000, failed_children=[{"n": index} for index in range(9)])
    assert native["error"] == "E" * 512
    assert native["failed_children"] == [{"n": index} for index in range(5)]


def test_native_ref_is_json_safe() -> None:
    native = native_ref(
        retry_attempts=float("nan"),
        resource=float("inf"),
        request_id=object(),
        step_path=("Map[0]", "State"),
        start_date=MOMENT,
    )
    assert native["retry_attempts"] is None and native["resource"] is None
    assert native["request_id"].startswith("<object object")
    assert native["step_path"] == ["Map[0]", "State"]
    assert json.loads(json.dumps(native, allow_nan=False)) == native


def test_native_ref_refuses_more_than_4_kb() -> None:
    with pytest.raises(ValueError, match=f"limit {MAX_NATIVE_BYTES}"):
        native_ref(failed_children=[{"execution_arn": "x" * 500, "error": "y" * 500}] * 5)


@pytest.mark.parametrize(
    ("value", "as_int", "as_text"),
    [
        (3, 3, None),
        ("3", 3, "3"),
        (True, None, None),
        (None, None, None),
        ("x", None, "x"),
        ("  ", None, None),
        (2.0, 2, None),
    ],
)
def test_native_int_and_text(value: Any, as_int: int | None, as_text: str | None) -> None:
    assert native_int({"k": value}, "k") == as_int
    assert native_text({"k": value}, "k") == as_text
    assert native_int(None, "k") is None and native_text(None, "k") is None


# =========================================================================== pipeline index


def test_the_index_of_the_configured_pipelines() -> None:
    index = PipelineIndex.from_options(sfn_options())
    assert [entry.pipeline_id for entry in index.entries()] == list(PIPELINES)
    sales = index.get("sales_daily_etl")
    assert sales == PipelineEntry("sales_daily_etl", SALES_SM, "SalesDailyEtlStateMachine")
    assert (sales.name, sales.arn, sales.discovered) == ("SalesDailyEtlStateMachine-AbC123", SALES_SM, False)
    assert index.known_arn("customers_sync") == CUSTOMERS_SM
    assert index.get("nope") is None and index.known_arn("nope") is None
    assert index.discover is False


@pytest.mark.parametrize(
    ("lookup", "pipeline_id"),
    [
        (CUSTOMERS_SM, "customers_sync"),
        (f"{CUSTOMERS_SM}:7", "customers_sync"),
        (f"{CUSTOMERS_SM}:prod", "customers_sync"),
        ("customers-sync", "customers_sync"),
        (f"{CUSTOMERS_SM}/Items", None),
        (f"arn:aws:states:{REGION}:{ACCOUNT}:stateMachine:other", None),
        (f"arn:aws:states:{REGION}:210987654321:stateMachine:customers-sync", None),
        (f"arn:aws:states:{REGION}:{ACCOUNT}:stateMachine", None),
        ("other", None),
    ],
)
def test_pipeline_id_for(lookup: str, pipeline_id: str | None) -> None:
    assert PipelineIndex.from_options(sfn_options()).pipeline_id_for(lookup) == pipeline_id


def test_pipelines_configured_by_name_match_any_arn_of_that_name() -> None:
    index = PipelineIndex.from_options(
        sfn_options(pipelines={"customers_sync": {"state_machine": "customers-sync"}})
    )
    assert index.known_arn("customers_sync") is None
    assert index.pipeline_id_for(CUSTOMERS_SM) == "customers_sync"
    assert index.pipeline_id_for("customers-sync") == "customers_sync"
    index.remember_arn("customers_sync", f"{CUSTOMERS_SM}:3")
    assert index.known_arn("customers_sync") == CUSTOMERS_SM


def test_discovery() -> None:
    index = PipelineIndex.from_options(sfn_options(discover=True))
    other = f"arn:aws:states:{REGION}:{ACCOUNT}:stateMachine:other-machine"
    assert index.pipeline_id_for(other) == "other-machine", "an unknown machine maps to its name"
    assert index.pipeline_id_for(f"{CUSTOMERS_SM}/Items") is None, "never a Distributed Map child"
    assert index.add_discovered(other) == "other-machine"
    assert index.add_discovered(f"{other}:2") == "other-machine"
    assert index.add_discovered(CUSTOMERS_SM) == "customers_sync", "a configured pipeline wins"
    entries = index.entries()
    assert [entry.pipeline_id for entry in entries] == [*PIPELINES, "other-machine"]
    assert entries[-1] == PipelineEntry("other-machine", other, discovered=True)
    assert index.known_arn("other-machine") == other


def test_a_discovered_name_never_takes_the_id_of_another_machines_pipeline() -> None:
    """``customers-sync`` is the configured id of the sales machine: the machine named
    ``customers-sync`` maps to no pipeline and cannot be added under that id."""
    index = PipelineIndex.from_options(
        sfn_options(discover=True, pipelines={"customers-sync": {"state_machine": SALES_SM}})
    )
    assert index.pipeline_id_for(SALES_SM) == "customers-sync"
    assert index.pipeline_id_for(CUSTOMERS_SM) is None
    assert index.pipeline_id_for(f"{CUSTOMERS_SM}:3") is None
    with pytest.raises(ValueError, match="is the pipeline id of another state machine"):
        index.add_discovered(CUSTOMERS_SM)
    assert [entry.pipeline_id for entry in index.entries()] == ["customers-sync"]
    assert index.known_arn("customers-sync") == SALES_SM
    assert index.add_discovered(SALES_SM) == "customers-sync"


def test_a_discovered_machine_keeps_its_id_once_added() -> None:
    index = PipelineIndex.from_options(sfn_options(discover=True, pipelines={}))
    assert index.add_discovered(CUSTOMERS_SM) == "customers-sync"
    assert index.pipeline_id_for(CUSTOMERS_SM) == "customers-sync"
    assert index.pipeline_id_for("customers-sync") == "customers-sync"
    other_account = CUSTOMERS_SM.replace(ACCOUNT, "210987654321")
    assert index.pipeline_id_for(other_account) is None, "the name belongs to the first machine"
    with pytest.raises(ValueError):
        index.add_discovered(other_account)


def test_discovery_resolves_a_pipeline_configured_by_name() -> None:
    options = sfn_options(discover=True, pipelines={"customers_sync": {"state_machine": "customers-sync"}})
    index = PipelineIndex.from_options(options)
    assert index.add_discovered(CUSTOMERS_SM) == "customers_sync"
    assert index.known_arn("customers_sync") == CUSTOMERS_SM


# =========================================================================== the runtime


def test_now_is_aware_utc(make_runtime: Any) -> None:
    runtime = make_runtime(now=NOW.replace(tzinfo=None))
    assert runtime.now() == NOW and runtime.now().tzinfo is not None
    runtime.clock = lambda: "not a time"
    assert abs(runtime.now() - datetime.now(UTC)) < timedelta(minutes=1)
    assert runtime.region == REGION and runtime.instance == INSTANCE


def test_is_monitored(make_runtime: Any, tmp_path: Path) -> None:
    assert make_runtime().is_monitored("anything") is True
    cfg = config_from_mapping({"monitor": {"dags": ["sales_*", "!sales_test"]}}, base_dir=tmp_path)
    runtime = make_runtime(cfg=cfg)
    assert runtime.is_monitored("sales_daily_etl") is True
    assert runtime.is_monitored("sales_test") is False and runtime.is_monitored("customers_sync") is False


def _listed(*names: str) -> list[dict[str, Any]]:
    return [
        {
            "stateMachineArn": f"arn:aws:states:{REGION}:{ACCOUNT}:stateMachine:{name}",
            "name": name,
            "type": "STANDARD",
            "creationDate": NOW - timedelta(days=30),
        }
        for name in names
    ]


def test_state_machine_arn_of_a_configured_arn_needs_no_io(make_runtime: Any, stubbed: Any) -> None:
    stubbed("stepfunctions")  # no answer queued: a call would fail
    runtime = make_runtime()
    assert runtime.state_machine_arn("inventory_report") == INVENTORY_SM
    assert runtime.state_machine_arn("unknown") is None


def test_state_machine_arn_resolves_a_name_once(make_runtime: Any, stubbed: Any) -> None:
    options = sfn_options(pipelines={"customers_sync": {"state_machine": "customers-sync"}})
    _client, stubber = stubbed("stepfunctions")
    stubber.add_response(
        "list_state_machines", {"stateMachines": _listed("a", "b"), "nextToken": "t1"}, {"maxResults": 1000}
    )
    stubber.add_response(
        "list_state_machines",
        {"stateMachines": _listed("customers-sync")},
        {"maxResults": 1000, "nextToken": "t1"},
    )
    runtime = make_runtime(options)
    assert runtime.state_machine_arn("customers_sync") == CUSTOMERS_SM
    assert runtime.state_machine_arn("customers_sync") == CUSTOMERS_SM  # cached: no third call
    assert runtime.pipelines.known_arn("customers_sync") == CUSTOMERS_SM


def test_state_machine_arn_of_a_missing_name(make_runtime: Any, stubbed: Any) -> None:
    options = sfn_options(pipelines={"ghost": {"state_machine": "ghost"}})
    _client, stubber = stubbed("stepfunctions")
    stubber.add_response("list_state_machines", {"stateMachines": _listed("a")}, {"maxResults": 1000})
    assert make_runtime(options).state_machine_arn("ghost") is None


def test_state_machine_arn_errors_are_core_errors(make_runtime: Any, stubbed: Any) -> None:
    options = sfn_options(pipelines={"customers_sync": {"state_machine": "customers-sync"}})
    _client, stubber = stubbed("stepfunctions")
    stubber.add_client_error("list_state_machines", service_error_code="AccessDeniedException")
    stubber.add_client_error("list_state_machines", service_error_code="ThrottlingException")
    runtime = make_runtime(options)
    with pytest.raises(PermissionDenied):
        runtime.state_machine_arn("customers_sync")
    with pytest.raises(OrchestratorError) as raised:
        runtime.state_machine_arn("customers_sync")
    assert raised.value.transient is True


@pytest.mark.parametrize(
    ("ref", "native", "expected"),
    [
        (AttemptRef(INSTANCE, "customers_sync", "x", "S"), {"execution_arn": EXECUTION}, EXECUTION),
        (AttemptRef(INSTANCE, "customers_sync", "customers-2026-10-02", "S"), {}, EXECUTION),
        (AttemptRef(INSTANCE, "customers_sync", "customers-2026-10-02@1790920800000", "S"), {}, EXECUTION),
        (
            AttemptRef(INSTANCE, "customers_sync", "customers-2026-10-02@1790920800000", "S"),
            {"start_date": "2026-10-02T06:00:00.000+00:00"},  # the marker of this start
            EXECUTION,
        ),
        (
            AttemptRef(INSTANCE, "customers_sync", "nightly@1790920800000", "S"),
            {"start_date": "2026-10-02T06:30:00.000+00:00"},  # another start: part of the name
            f"{EXECUTION.rsplit(':', 1)[0]}:nightly@1790920800000",
        ),
        (
            AttemptRef(INSTANCE, "other", "customers-2026-10-02", "S"),
            {"state_machine_arn": f"{CUSTOMERS_SM}:4"},
            EXECUTION,
        ),
        (AttemptRef(INSTANCE, "unknown", "run", "S"), {}, None),
    ],
)
def test_execution_arn(
    make_runtime: Any, stubbed: Any, ref: AttemptRef, native: dict[str, Any], expected: str | None
) -> None:
    stubbed("stepfunctions")
    assert make_runtime().execution_arn(ref, native) == expected
