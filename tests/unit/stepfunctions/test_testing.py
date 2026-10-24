"""The offline AWS world (``stepfunctions/testing.py``): real botocore clients answered in memory.

The world must be as strict as AWS where AWS is strict: botocore's own parameter validation,
server-side constraints (maximum lengths and values, enums), output-shape validation of every
answer and of every record when it is added, the AWS error codes of unknown resources, and the
Python types botocore parses (``tzlocal()`` datetimes for Step Functions, epoch milliseconds for
CloudWatch Logs, text for Lambda's ``LastModified``). Then the semantics: paging tokens,
newest-first listings, history order and execution data, the redrive window and idempotency,
log filter patterns and late logs, SQS visibility and receipts, S3 byte ranges, outages, one-shot
errors, the scenario-file loader and the skeleton's ``executions`` helpers on top of the world.
"""

from __future__ import annotations

import hashlib
import json
import threading
from collections.abc import Iterator
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

import pytest
from botocore.client import BaseClient
from botocore.exceptions import ClientError, EndpointConnectionError, ParamValidationError
from botocore.stub import Stubber
from dateutil.tz import tzlocal, tzutc

from dag_healer.core.errors import NotFound, OrchestratorError, PermissionDenied
from dag_healer.stepfunctions import executions
from dag_healer.stepfunctions.aws import AwsClients, translate_error
from dag_healer.stepfunctions.runtime import epoch_ms
from dag_healer.stepfunctions.testing import (
    CLIENT_TOKEN_TTL,
    DEFAULT_INGESTION_DELAY_MS,
    MACHINE_AGE,
    REDRIVE_WINDOW,
    AwsCall,
    OfflineAws,
    OfflineNetworkError,
    dummy_client,
)

from .conftest import ACCOUNT, FIXTURES, NOW, REGION, MakeRuntime, sfn_options

WORLD_DIR = FIXTURES / "world"
T0 = NOW - timedelta(hours=1)  # 2026-10-02T06:00Z: the executions start then
MACHINE = "orders-etl"
MACHINE_ARN = f"arn:aws:states:{REGION}:{ACCOUNT}:stateMachine:{MACHINE}"
ROLE = f"arn:aws:iam::{ACCOUNT}:role/orders-etl-role"
DEFINITION = {"StartAt": "Work", "States": {"Work": {"Type": "Pass", "End": True}}}
CAUSE = json.dumps({"errorMessage": "'amount'", "errorType": "KeyError", "stackTrace": []})
FUNCTION = "orders-fn"
GROUP = f"/aws/lambda/{FUNCTION}"
QUEUE = f"https://sqs.{REGION}.amazonaws.com/{ACCOUNT}/healer-events"
UTC = timezone.utc


# =========================================================================== helpers


def _iso(moment: datetime) -> str:
    return moment.astimezone(UTC).isoformat(timespec="milliseconds").replace("+00:00", "Z")


def make_world(**kwargs: Any) -> OfflineAws:
    """A world at :data:`NOW` with the state machine :data:`MACHINE`."""
    world = OfflineAws(now=kwargs.pop("now", NOW), **kwargs)
    world.add_state_machine(MACHINE, DEFINITION, role_arn=ROLE)
    return world


def add_failed(world: OfflineAws, name: str, *, start: datetime = T0, seconds: int = 5, **kwargs: Any) -> str:
    kwargs.setdefault("error", "KeyError")
    kwargs.setdefault("cause", CAUSE)
    return world.add_execution(
        MACHINE,
        name,
        status="FAILED",
        start_date=start,
        stop_date=start + timedelta(seconds=seconds),
        **kwargs,
    )


def task_history(
    start: datetime = T0,
    *,
    payload: str = '{"run":"2026-10-02"}',
    error: str = "KeyError",
    cause: str = CAUSE,
) -> list[dict[str, Any]]:
    """A Lambda Task that fails once, then the execution (ISO timestamps, as fixtures write them)."""

    def at(ms: int) -> str:
        return _iso(start + timedelta(milliseconds=ms))

    lambda_details = {"resourceType": "lambda", "resource": "invoke"}
    return [
        {
            "timestamp": at(0),
            "type": "ExecutionStarted",
            "id": 1,
            "previousEventId": 0,
            "executionStartedEventDetails": {
                "input": payload,
                "inputDetails": {"truncated": False},
                "roleArn": ROLE,
            },
        },
        {
            "timestamp": at(10),
            "type": "TaskStateEntered",
            "id": 2,
            "previousEventId": 0,
            "stateEnteredEventDetails": {
                "name": "Work",
                "input": payload,
                "inputDetails": {"truncated": False},
            },
        },
        {
            "timestamp": at(10),
            "type": "TaskScheduled",
            "id": 3,
            "previousEventId": 2,
            "taskScheduledEventDetails": {
                **lambda_details,
                "region": REGION,
                "parameters": '{"FunctionName":"fn"}',
            },
        },
        {
            "timestamp": at(40),
            "type": "TaskStarted",
            "id": 4,
            "previousEventId": 3,
            "taskStartedEventDetails": dict(lambda_details),
        },
        {
            "timestamp": at(900),
            "type": "TaskFailed",
            "id": 5,
            "previousEventId": 4,
            "taskFailedEventDetails": {**lambda_details, "error": error, "cause": cause},
        },
        {
            "timestamp": at(910),
            "type": "ExecutionFailed",
            "id": 6,
            "previousEventId": 5,
            "executionFailedEventDetails": {"error": error, "cause": cause},
        },
    ]


def pass_history(start: datetime, states: int) -> list[dict[str, Any]]:
    """``ExecutionStarted``, ``states`` Pass states (entered/exited), then ``ExecutionFailed``."""
    events: list[dict[str, Any]] = [
        {
            "timestamp": start,
            "type": "ExecutionStarted",
            "id": 1,
            "previousEventId": 0,
            "executionStartedEventDetails": {"input": "{}", "roleArn": ROLE},
        }
    ]
    for index in range(states):
        moment = start + timedelta(milliseconds=index + 1)
        entered = len(events) + 1
        events.append(
            {
                "timestamp": moment,
                "type": "PassStateEntered",
                "id": entered,
                "previousEventId": entered - 1,
                "stateEnteredEventDetails": {"name": f"Step{index}", "input": "{}"},
            }
        )
        events.append(
            {
                "timestamp": moment,
                "type": "PassStateExited",
                "id": entered + 1,
                "previousEventId": entered,
                "stateExitedEventDetails": {"name": f"Step{index}", "output": "{}"},
            }
        )
    events.append(
        {
            "timestamp": start + timedelta(seconds=30),
            "type": "ExecutionFailed",
            "id": len(events) + 1,
            "previousEventId": len(events),
            "executionFailedEventDetails": {"error": "States.Runtime", "cause": "boom"},
        }
    )
    return events


def error_of(raised: pytest.ExceptionInfo[ClientError]) -> tuple[str, int]:
    response = raised.value.response
    return response["Error"]["Code"], response["ResponseMetadata"]["HTTPStatusCode"]


@pytest.fixture
def world() -> OfflineAws:
    return make_world()


@pytest.fixture
def sfn(world: OfflineAws) -> Any:
    return world.client_factory("stepfunctions")


# =========================================================================== clients


def test_dummy_client_never_reaches_the_network() -> None:
    client = dummy_client("stepfunctions")
    assert isinstance(client, BaseClient)
    assert client.meta.region_name == REGION
    with pytest.raises(OfflineNetworkError, match="refused to send"):
        client.describe_execution(executionArn=f"arn:aws:states:{REGION}:{ACCOUNT}:execution:{MACHINE}:x")


def test_no_client_sends_a_request_while_requests_are_refused() -> None:
    """Whatever built the client (here a plain botocore session, as boto3 does): the request is
    refused before it leaves, recorded, and the sender is back to normal afterwards."""
    import botocore.session
    from botocore.httpsession import URLLib3Session

    from dag_healer.stepfunctions.testing import refuse_aws_requests

    plain = botocore.session.Session().create_client(
        "sts", region_name=REGION, aws_access_key_id="testing", aws_secret_access_key="testing"
    )
    before = URLLib3Session.send
    with refuse_aws_requests() as refused:
        with pytest.raises(OfflineNetworkError, match="would have left the process"):
            plain.get_caller_identity()
        with Stubber(plain) as stubber:  # a Stubber answers before anything is sent
            stubber.add_response("get_caller_identity", {"Account": ACCOUNT}, {})
            assert plain.get_caller_identity()["Account"] == ACCOUNT
    assert refused == [f"https://sts.{REGION}.amazonaws.com/"]
    assert URLLib3Session.send is before


def test_dummy_client_works_with_a_stubber() -> None:
    client = dummy_client("sfn", region="us-east-1")
    arn = f"arn:aws:states:us-east-1:{ACCOUNT}:execution:{MACHINE}:x"
    with Stubber(client) as stubber:
        stubber.add_response(
            "redrive_execution", {"redriveDate": NOW}, {"executionArn": arn, "clientToken": "t"}
        )
        assert client.redrive_execution(executionArn=arn, clientToken="t")["redriveDate"] == NOW
        stubber.assert_no_pending_responses()


def test_clients_ignore_the_host_aws_setup(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    monkeypatch.setenv("AWS_PROFILE", "a-profile-that-does-not-exist")
    monkeypatch.setenv("AWS_CONFIG_FILE", str(tmp_path / "missing-config"))
    monkeypatch.setenv("AWS_CA_BUNDLE", str(tmp_path / "missing-ca.pem"))
    monkeypatch.setenv("AWS_ENDPOINT_URL", "http://localhost:1")
    monkeypatch.setenv("AWS_USE_FIPS_ENDPOINT", "true")
    client = make_world().client_factory("stepfunctions")
    assert client.meta.endpoint_url == f"https://states.{REGION}.amazonaws.com"
    assert client.list_state_machines()["stateMachines"][0]["name"] == MACHINE
    assert dummy_client("s3").meta.endpoint_url == f"https://s3.{REGION}.amazonaws.com"


def test_the_world_clock_accepts_datetimes_callables_and_real_time() -> None:
    moments = iter([NOW, NOW + timedelta(seconds=1)])
    world = OfflineAws(now=lambda: next(moments))
    assert world.now() == NOW and world.now() == NOW + timedelta(seconds=1)
    world.set_now(datetime(2026, 10, 2, 9, 0, 0, 123_456))  # naive means UTC
    assert world.now() == datetime(2026, 10, 2, 9, 0, 0, 123_000, tzinfo=UTC)
    world.set_now(None)
    assert abs(world.now() - datetime.now(UTC)) < timedelta(minutes=1)
    with pytest.raises(TypeError, match="now must be"):
        OfflineAws(now="2026-10-02")


def test_a_world_needs_a_region_and_an_account() -> None:
    with pytest.raises(ValueError, match="region"):
        OfflineAws(region="eu_west_1")
    with pytest.raises(ValueError, match="12 digits"):
        OfflineAws(account="1234")


def test_world_clients_are_real_botocore_clients(world: OfflineAws) -> None:
    for service in ("stepfunctions", "logs", "lambda", "sqs", "s3", "sts"):
        client = world.client_factory(service)
        assert isinstance(client, BaseClient)
        assert client.meta.service_model.service_name == service
        assert client.meta.region_name == REGION
    assert world.io_calls == 0, "building clients is no call"


def test_two_clients_of_one_world_share_its_state(world: OfflineAws) -> None:
    first, second = world.client_factory("stepfunctions"), world.client_factory("stepfunctions")
    assert first is not second
    arn = first.start_execution(stateMachineArn=MACHINE_ARN, name="shared")["executionArn"]
    assert second.describe_execution(executionArn=arn)["status"] == "RUNNING"
    assert [call.operation for call in world.calls] == ["StartExecution", "DescribeExecution"]


def test_botocore_validates_parameters_before_the_world_sees_a_call(sfn: Any, world: OfflineAws) -> None:
    with pytest.raises(ParamValidationError, match="executionArn"):
        sfn.describe_execution()
    with pytest.raises(ParamValidationError, match="Unknown parameter"):
        sfn.describe_execution(executionArn="x", nosuch=1)
    with pytest.raises(ParamValidationError, match="Invalid type"):
        sfn.list_executions(stateMachineArn=MACHINE_ARN, maxResults="10")
    assert world.calls == [], "a call rejected client-side never reaches AWS"


@pytest.mark.parametrize(
    ("service", "method", "params", "code"),
    [
        (
            "stepfunctions",
            "list_executions",
            {"stateMachineArn": MACHINE_ARN, "maxResults": 1001},
            "ValidationException",
        ),
        (
            "stepfunctions",
            "list_executions",
            {"stateMachineArn": MACHINE_ARN, "statusFilter": "FAILD"},
            "ValidationException",
        ),
        (
            "stepfunctions",
            "start_execution",
            {"stateMachineArn": MACHINE_ARN, "name": "n" * 81},
            "ValidationException",
        ),
        (
            "stepfunctions",
            "redrive_execution",
            {"executionArn": "a", "clientToken": "t" * 65},
            "ValidationException",
        ),
        ("logs", "filter_log_events", {"logGroupName": GROUP, "limit": 10_001}, "InvalidParameterException"),
    ],
)
def test_server_side_constraints_are_checked(
    world: OfflineAws, service: str, method: str, params: dict[str, Any], code: str
) -> None:
    client = world.client_factory(service)
    with pytest.raises(ClientError) as raised:
        getattr(client, method)(**params)
    assert error_of(raised) == (code, 400)
    assert "validation error detected" in raised.value.response["Error"]["Message"]


def test_a_constraint_message_names_the_member_but_never_the_value(sfn: Any) -> None:
    secret = "alice.pii@example.com" * 20_000
    with pytest.raises(ClientError) as raised:
        sfn.start_execution(stateMachineArn=MACHINE_ARN, input=json.dumps({"who": secret}))
    message = raised.value.response["Error"]["Message"]
    assert "'input'" in message and "alice.pii" not in message


def test_unsupported_operations_answer_an_error(world: OfflineAws) -> None:
    sfn = world.client_factory("stepfunctions")
    with pytest.raises(ClientError) as raised:
        sfn.list_activities()
    assert error_of(raised) == ("UnsupportedOperation", 400)
    with pytest.raises(world.client_factory("sqs").exceptions.UnsupportedOperation):
        world.client_factory("sqs").list_queues()
    assert [call.operation for call in world.calls] == ["ListActivities", "ListQueues"]


def test_every_call_is_recorded_with_the_caller_params(world: OfflineAws) -> None:
    arn = add_failed(world, "recorded")
    sfn = world.client_factory("stepfunctions")
    sfn.redrive_execution(executionArn=arn)
    assert world.calls == [AwsCall("stepfunctions", "RedriveExecution", {"executionArn": arn})], (
        "botocore's generated clientToken is not part of what the caller passed"
    )
    assert world.io_calls == 1
    assert world.calls_to("sfn", "RedriveExecution") == world.calls
    assert world.calls_to(service="logs") == []


def test_the_world_answers_concurrent_calls(world: OfflineAws) -> None:
    arn = add_failed(world, "concurrent")
    sfn = world.client_factory("stepfunctions")
    failures: list[BaseException] = []

    def work() -> None:
        try:
            for _ in range(20):
                assert sfn.describe_execution(executionArn=arn)["status"] == "FAILED"
        except BaseException as exc:  # collected for the main thread
            failures.append(exc)

    threads = [threading.Thread(target=work) for _ in range(6)]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join()
    assert failures == []
    assert world.io_calls == 120


# =========================================================================== types and shapes


def test_step_functions_timestamps_are_what_botocore_parses(world: OfflineAws, sfn: Any) -> None:
    arn = add_failed(world, "typed", start=datetime(2026, 10, 2, 6, 0, 41, 512_999, tzinfo=UTC))
    answer = sfn.describe_execution(executionArn=arn)
    started = answer["startDate"]
    assert isinstance(started.tzinfo, tzlocal), "botocore parses JSON epoch timestamps into tzlocal()"
    assert started == datetime(2026, 10, 2, 6, 0, 41, 512_000, tzinfo=UTC), "milliseconds, like AWS"
    assert isinstance(answer["redriveCount"], int)
    event = sfn.get_execution_history(executionArn=arn)["events"][0]
    assert isinstance(event["timestamp"].tzinfo, tzlocal) and isinstance(event["id"], int)
    assert answer["ResponseMetadata"]["HTTPStatusCode"] == 200
    assert answer["ResponseMetadata"]["RequestId"]


def test_other_services_keep_their_own_timestamp_types(world: OfflineAws) -> None:
    world.add_function(FUNCTION)
    world.add_log_events(GROUP, "s", [{"timestamp": "2026-10-02T06:00:00.250Z", "message": "hello"}])
    world.put_object("acme-landing", "incoming/a.csv", "a\n1\n")
    event = world.client_factory("logs").filter_log_events(logGroupName=GROUP)["events"][0]
    assert event["timestamp"] == epoch_ms(T0) + 250 and isinstance(event["timestamp"], int)
    assert event["ingestionTime"] == event["timestamp"] + DEFAULT_INGESTION_DELAY_MS
    modified = world.client_factory("lambda").get_function(FunctionName=FUNCTION)["Configuration"][
        "LastModified"
    ]
    assert modified == "2026-10-01T07:00:00.000+0000"
    last = world.client_factory("s3").get_object(Bucket="acme-landing", Key="incoming/a.csv")["LastModified"]
    assert last == NOW and isinstance(last.tzinfo, tzutc)


@pytest.mark.parametrize(
    ("event", "message"),
    [
        ({"executionStartedEventDetail": {}}, "Unknown parameter"),
        ({"type": "ExecutionBegan"}, "enum value set"),
        ({"id": "1"}, "id must be an integer"),
        ({"timestamp": "yesterday"}, "ISO-8601"),
        ({"executionStartedEventDetails": {"input": 1}}, "Invalid type"),
        ({"executionStartedEventDetails": {"roleArn": ""}}, "Invalid length"),
    ],
)
def test_history_events_are_validated_when_they_are_added(
    world: OfflineAws, event: dict[str, Any], message: str
) -> None:
    history = task_history()
    history[0] = {**history[0], **event}
    with pytest.raises(ValueError, match=message) as raised:
        world.add_execution(MACHINE, "broken", status="FAILED", start_date=T0, history=history)
    assert "execution broken: history[0]" in str(raised.value)


def test_records_are_validated_when_they_are_added(world: OfflineAws) -> None:
    with pytest.raises(ValueError, match="enum value set"):
        world.add_function("fn-bad-runtime", runtime="python2.0")
    with pytest.raises(ValueError, match="Invalid length"):
        world.add_log_events(GROUP, "s", [{"timestamp": T0, "message": ""}])
    with pytest.raises(ValueError, match="unknown key"):
        world.add_log_events(GROUP, "s", [{"timestamp": T0, "message": "x", "level": "INFO"}])
    with pytest.raises(ValueError, match="enum value set"):
        world.add_state_machine("logged", DEFINITION, logging={"level": "VERBOSE"})
    with pytest.raises(ValueError, match="invalid"):
        world.add_execution(MACHINE, "white space", status="RUNNING", start_date=T0)
    with pytest.raises(ValueError, match="characters SQS does not accept"):
        world.send_message(QUEUE, "bad \x00 body")


# =========================================================================== errors, outages, planned errors


@pytest.mark.parametrize(
    ("service", "method", "params", "code", "status", "core"),
    [
        (
            "stepfunctions",
            "describe_execution",
            {"executionArn": f"arn:aws:states:{REGION}:{ACCOUNT}:execution:{MACHINE}:nope"},
            "ExecutionDoesNotExist",
            400,
            NotFound,
        ),
        ("stepfunctions", "describe_execution", {"executionArn": "not-an-arn"}, "InvalidArn", 400, NotFound),
        (
            "stepfunctions",
            "describe_state_machine",
            {"stateMachineArn": f"arn:aws:states:{REGION}:{ACCOUNT}:stateMachine:nope"},
            "StateMachineDoesNotExist",
            400,
            NotFound,
        ),
        (
            "stepfunctions",
            "describe_state_machine",
            {"stateMachineArn": f"arn:aws:states:{REGION}:{ACCOUNT}:execution:{MACHINE}:x"},
            "InvalidArn",
            400,
            NotFound,
        ),
        (
            "logs",
            "filter_log_events",
            {"logGroupName": "/aws/lambda/nope"},
            "ResourceNotFoundException",
            400,
            NotFound,
        ),
        ("lambda", "get_function", {"FunctionName": "nope"}, "ResourceNotFoundException", 404, NotFound),
        ("s3", "get_object", {"Bucket": "nope-bucket", "Key": "k"}, "NoSuchBucket", 404, NotFound),
        (
            "sqs",
            "receive_message",
            {"QueueUrl": QUEUE + "-nope"},
            "AWS.SimpleQueueService.NonExistentQueue",
            400,
            NotFound,
        ),
    ],
)
def test_unknown_resources_answer_the_aws_error_codes(
    world: OfflineAws, service: str, method: str, params: dict[str, Any], code: str, status: int, core: type
) -> None:
    with pytest.raises(ClientError) as raised:
        getattr(world.client_factory(service), method)(**params)
    assert error_of(raised) == (code, status)
    assert isinstance(translate_error(raised.value), core)


def test_modeled_errors_raise_the_modeled_exception_classes(world: OfflineAws) -> None:
    sfn, sqs = world.client_factory("stepfunctions"), world.client_factory("sqs")
    with pytest.raises(sfn.exceptions.ExecutionDoesNotExist):
        sfn.describe_execution(executionArn=world.execution_arn(MACHINE, "missing"))
    with pytest.raises(sqs.exceptions.QueueDoesNotExist) as raised:
        sqs.get_queue_attributes(QueueUrl=QUEUE, AttributeNames=["All"])
    error = raised.value.response["Error"]
    assert (error["Code"], error["QueryErrorCode"]) == (
        "AWS.SimpleQueueService.NonExistentQueue",
        "QueueDoesNotExist",
    )


def test_a_server_error_outage_is_a_transient_500(world: OfflineAws, sfn: Any) -> None:
    world.outage = "server_error"
    with pytest.raises(ClientError) as raised:
        sfn.list_state_machines()
    assert error_of(raised) == ("InternalFailure", 500)
    assert translate_error(raised.value).transient
    with pytest.raises(ClientError) as raised:
        world.client_factory("s3").get_object(Bucket="b-bucket", Key="k")
    assert error_of(raised) == ("InternalError", 500)
    world.outage = None
    assert sfn.list_state_machines()["stateMachines"]


def test_a_network_outage_raises_a_transient_connection_error(world: OfflineAws, sfn: Any) -> None:
    world.outage = "network"
    with pytest.raises(EndpointConnectionError, match=f"states.{REGION}.amazonaws.com"):
        sfn.list_state_machines()
    error = translate_error(EndpointConnectionError(endpoint_url="https://x"))
    assert isinstance(error, OrchestratorError) and error.transient
    assert [call.operation for call in world.calls] == ["ListStateMachines"], "the attempt is recorded"


def test_fail_next_is_one_shot_and_first_in_first_out(world: OfflineAws, sfn: Any) -> None:
    world.fail_next("stepfunctions", "ListStateMachines", "ThrottlingException", "Rate exceeded")
    world.fail_next("sfn", "list_state_machines", "AccessDeniedException", status=403)
    with pytest.raises(ClientError) as raised:
        sfn.list_state_machines()
    assert error_of(raised) == ("ThrottlingException", 400)
    assert raised.value.response["Error"]["Message"] == "Rate exceeded"
    assert translate_error(raised.value).transient
    with pytest.raises(ClientError) as raised:
        sfn.list_state_machines()
    assert error_of(raised) == ("AccessDeniedException", 403)
    assert isinstance(translate_error(raised.value), PermissionDenied)
    assert sfn.list_state_machines()["stateMachines"], "the errors were used up"


def test_fail_next_only_hits_its_operation_and_survives_an_outage(world: OfflineAws, sfn: Any) -> None:
    world.fail_next("stepfunctions", "DescribeStateMachine", "KmsThrottlingException")
    assert sfn.list_state_machines()["stateMachines"]
    world.outage = "server_error"
    with pytest.raises(ClientError, match="InternalFailure"):
        sfn.describe_state_machine(stateMachineArn=MACHINE_ARN)
    world.outage = None
    with pytest.raises(ClientError, match="KmsThrottlingException"):
        sfn.describe_state_machine(stateMachineArn=MACHINE_ARN)


def test_fail_next_rejects_unknown_names(world: OfflineAws) -> None:
    with pytest.raises(ValueError, match="no operation"):
        world.fail_next("stepfunctions", "DescribeExecutions", "X")
    with pytest.raises(ValueError, match="unknown AWS service"):
        world.fail_next("step-functions", "DescribeExecution", "X")
    with pytest.raises(ValueError, match="needs a code"):
        world.fail_next("stepfunctions", "DescribeExecution", " ")
    with pytest.raises(ValueError, match="HTTP status"):
        world.fail_next("stepfunctions", "DescribeExecution", "X", status=200)


# =========================================================================== state machines


def test_list_state_machines_pages_with_tokens(world: OfflineAws, sfn: Any) -> None:
    world.add_state_machine("b-machine", DEFINITION)
    world.add_state_machine("c-express", DEFINITION, type="EXPRESS")
    first = sfn.list_state_machines(maxResults=2)
    assert [m["name"] for m in first["stateMachines"]] == [MACHINE, "b-machine"]
    second = sfn.list_state_machines(maxResults=2, nextToken=first["nextToken"])
    assert [(m["name"], m["type"]) for m in second["stateMachines"]] == [("c-express", "EXPRESS")]
    assert "nextToken" not in second
    assert first["stateMachines"][0]["creationDate"] == NOW - MACHINE_AGE
    assert len(sfn.list_state_machines(maxResults=0)["stateMachines"]) == 3, "0 means the default page size"


def test_page_tokens_are_bound_to_their_request_and_expire(world: OfflineAws, sfn: Any) -> None:
    for name in ("one", "two", "three"):
        add_failed(world, name)
    token = sfn.list_executions(stateMachineArn=MACHINE_ARN, maxResults=1)["nextToken"]
    for bad in (
        "garbage",
        token[:-3],
        sfn.list_state_machines(maxResults=1).get("nextToken") or "OFFLINEe30",
    ):
        with pytest.raises(ClientError) as raised:
            sfn.list_executions(stateMachineArn=MACHINE_ARN, maxResults=1, nextToken=bad)
        assert error_of(raised) == ("InvalidToken", 400)
    with pytest.raises(sfn.exceptions.InvalidToken):
        sfn.list_executions(stateMachineArn=MACHINE_ARN, statusFilter="FAILED", nextToken=token)
    world.set_now(NOW + timedelta(hours=24, seconds=1))
    with pytest.raises(sfn.exceptions.InvalidToken, match="expired"):
        sfn.list_executions(stateMachineArn=MACHINE_ARN, maxResults=1, nextToken=token)


def test_describe_state_machine_returns_the_substituted_definition(world: OfflineAws, sfn: Any) -> None:
    definition = {
        "StartAt": "Call",
        "States": {"Call": {"Type": "Task", "Resource": "${FnArn}", "End": True}},
    }
    function = world.function_arn(FUNCTION)
    arn = world.add_state_machine("deployed", definition, substitutions={"FnArn": function})
    answer = sfn.describe_state_machine(stateMachineArn=arn)
    assert json.loads(answer["definition"])["States"]["Call"]["Resource"] == function
    assert (answer["name"], answer["type"], answer["status"]) == ("deployed", "STANDARD", "ACTIVE")
    assert answer["roleArn"].startswith(f"arn:aws:iam::{ACCOUNT}:role/")
    assert answer["loggingConfiguration"] == {"level": "OFF", "includeExecutionData": False}
    child = sfn.describe_state_machine(stateMachineArn=f"{arn}/Workers")
    assert (child["stateMachineArn"], child["label"]) == (f"{arn}/Workers", "Workers")


@pytest.mark.parametrize(
    ("definition", "substitutions", "message"),
    [
        (
            '{"StartAt": "A", "States": {"A": {"Type": "Task", "Resource": "${Missing}", "End": true}}}',
            None,
            "unsubstituted",
        ),
        ('{"StartAt": "A", "States": ', None, "not valid JSON"),
        ({"StartAt": "B", "States": {"A": {"Type": "Pass", "End": True}}}, None, "StartAt"),
        ({"StartAt": "A", "States": {"A": {"End": True}}}, None, "needs a Type"),
        ({"StartAt": "A", "States": {"A": {"Type": "Pass", "End": True}}}, {"": "x"}, "substitution key"),
    ],
)
def test_invalid_definitions_are_refused(
    world: OfflineAws, definition: Any, substitutions: dict[str, str] | None, message: str
) -> None:
    with pytest.raises(ValueError, match=message):
        world.add_state_machine("refused", definition, substitutions=substitutions)


def test_state_machine_names_types_and_roles_are_checked(world: OfflineAws) -> None:
    with pytest.raises(ValueError, match="already exists"):
        world.add_state_machine(MACHINE, DEFINITION)
    with pytest.raises(ValueError, match="invalid"):
        world.add_state_machine("bad:name", DEFINITION)
    with pytest.raises(ValueError, match="STANDARD or EXPRESS"):
        world.add_state_machine("typed", DEFINITION, type="standard")
    with pytest.raises(ValueError, match="IAM role ARN"):
        world.add_state_machine("roled", DEFINITION, role_arn="arn:aws:iam::123:user/x")


def test_update_state_machine_needs_a_known_machine_and_a_valid_definition(world: OfflineAws) -> None:
    with pytest.raises(ValueError, match="unknown state machine"):
        world.update_state_machine("nope", DEFINITION)
    with pytest.raises(ValueError, match="not valid JSON"):
        world.update_state_machine(MACHINE_ARN, "{")
    assert world.update_state_machine(MACHINE_ARN, DEFINITION) == MACHINE_ARN


def test_an_execution_keeps_the_definition_it_ran(world: OfflineAws, sfn: Any) -> None:
    old = add_failed(world, "before-deploy")
    new_definition = {"StartAt": "Fixed", "States": {"Fixed": {"Type": "Pass", "End": True}}}
    world.update_state_machine(MACHINE, new_definition)
    ran = sfn.describe_state_machine_for_execution(executionArn=old)
    current = sfn.describe_state_machine(stateMachineArn=MACHINE_ARN)
    assert json.loads(ran["definition"]) == DEFINITION
    assert json.loads(current["definition"]) == new_definition
    assert ran["revisionId"] != current["revisionId"]
    assert (ran["stateMachineArn"], ran["name"], ran["roleArn"]) == (MACHINE_ARN, MACHINE, ROLE)
    assert ran["updateDate"] == NOW - MACHINE_AGE
    started = sfn.start_execution(stateMachineArn=MACHINE_ARN, name="after-deploy")["executionArn"]
    assert (
        json.loads(sfn.describe_state_machine_for_execution(executionArn=started)["definition"])
        == new_definition
    )


# =========================================================================== executions


def test_list_executions_is_newest_first_with_one_status_filter(world: OfflineAws, sfn: Any) -> None:
    add_failed(world, "early", start=T0, seconds=50)  # stops last
    add_failed(world, "late", start=T0 + timedelta(seconds=10), seconds=5)
    world.add_execution(
        MACHINE, "timed-out", status="TIMED_OUT", start_date=T0, stop_date=T0 + timedelta(seconds=20)
    )
    world.add_execution(MACHINE, "running", status="RUNNING", start_date=T0 + timedelta(seconds=30))
    names = [e["name"] for e in sfn.list_executions(stateMachineArn=MACHINE_ARN)["executions"]]
    assert names == ["early", "running", "timed-out", "late"], "finished by stop date, running by start date"
    failed = sfn.list_executions(stateMachineArn=MACHINE_ARN, statusFilter="FAILED")["executions"]
    assert [e["name"] for e in failed] == ["early", "late"]
    item = failed[0]
    assert item["executionArn"] == world.execution_arn(MACHINE, "early")
    assert (item["stateMachineArn"], item["status"], item["redriveCount"]) == (MACHINE_ARN, "FAILED", 0)
    assert item["stopDate"] == T0 + timedelta(seconds=50)
    running = sfn.list_executions(stateMachineArn=MACHINE_ARN, statusFilter="RUNNING")["executions"]
    assert "stopDate" not in running[0]


def test_list_executions_pages_and_filters_redrives(world: OfflineAws, sfn: Any) -> None:
    for index in range(5):
        add_failed(world, f"run-{index}", start=T0 + timedelta(minutes=index))
    add_failed(world, "redriven", start=T0 - timedelta(minutes=5), redrive_count=1)
    seen: list[str] = []
    token = None
    while True:
        page = sfn.list_executions(
            stateMachineArn=MACHINE_ARN, maxResults=2, **({"nextToken": token} if token else {})
        )
        seen += [e["name"] for e in page["executions"]]
        token = page.get("nextToken")
        if not token:
            break
    assert seen == ["run-4", "run-3", "run-2", "run-1", "run-0", "redriven"]
    redriven = sfn.list_executions(stateMachineArn=MACHINE_ARN, redriveFilter="REDRIVEN")["executions"]
    assert [(e["name"], e["redriveCount"]) for e in redriven] == [("redriven", 1)]
    assert "redriveDate" in redriven[0]
    assert (
        len(sfn.list_executions(stateMachineArn=MACHINE_ARN, redriveFilter="NOT_REDRIVEN")["executions"]) == 5
    )


def test_list_executions_refusals(world: OfflineAws, sfn: Any) -> None:
    express = world.add_state_machine("fast", DEFINITION, type="EXPRESS")
    with pytest.raises(sfn.exceptions.StateMachineTypeNotSupported):
        sfn.list_executions(stateMachineArn=express)
    with pytest.raises(sfn.exceptions.StateMachineDoesNotExist):
        sfn.list_executions(stateMachineArn=world.state_machine_arn("nope"))
    with pytest.raises(sfn.exceptions.ValidationException):
        sfn.list_executions()
    with pytest.raises(sfn.exceptions.ValidationException):
        sfn.list_executions(stateMachineArn=MACHINE_ARN, statusFilter="PENDING_REDRIVE")
    with pytest.raises(sfn.exceptions.ResourceNotFound):
        sfn.list_executions(mapRunArn=f"arn:aws:states:{REGION}:{ACCOUNT}:mapRun:{MACHINE}/Workers:1a2b")


def test_versions_and_map_run_children(world: OfflineAws, sfn: Any) -> None:
    version = f"{MACHINE_ARN}:3"
    world.add_execution(version, "on-version", status="RUNNING", start_date=T0)
    add_failed(world, "unversioned")
    on_version = sfn.list_executions(stateMachineArn=version)["executions"]
    assert [(e["name"], e["stateMachineVersionArn"]) for e in on_version] == [("on-version", version)]
    run = f"arn:aws:states:{REGION}:{ACCOUNT}:mapRun:{MACHINE}/Workers:5f0c"
    child = add_failed(world, "item-7", map_run_arn=run)
    assert child == f"arn:aws:states:{REGION}:{ACCOUNT}:execution:{MACHINE}/Workers:item-7"
    assert "item-7" not in [e["name"] for e in sfn.list_executions(stateMachineArn=MACHINE_ARN)["executions"]]
    children = sfn.list_executions(mapRunArn=run, statusFilter="FAILED")["executions"]
    assert [(e["name"], e["mapRunArn"], e["stateMachineArn"]) for e in children] == [
        ("item-7", run, f"{MACHINE_ARN}/Workers")
    ]
    described = sfn.describe_execution(executionArn=child)
    assert (described["mapRunArn"], described["redriveStatus"]) == (run, "REDRIVABLE_BY_MAP_RUN")
    ran = sfn.describe_state_machine_for_execution(executionArn=child)
    assert (ran["label"], ran["mapRunArn"]) == ("Workers", run)
    with pytest.raises(sfn.exceptions.ExecutionNotRedrivable, match="Map Run"):
        sfn.redrive_execution(executionArn=child)
    with pytest.raises(ValueError, match="does not belong"):
        world.add_execution(
            MACHINE, "x", status="RUNNING", start_date=T0, map_run_arn=run.replace(MACHINE, "other")
        )
    with pytest.raises(ValueError, match="not a version ARN"):
        world.add_execution(MACHINE, "y", status="RUNNING", start_date=T0, version_arn=f"{MACHINE_ARN}:live")


def test_describe_execution_answers_status_dates_and_payloads(world: OfflineAws, sfn: Any) -> None:
    arn = add_failed(world, "described", input='{"who": "alice.pii@example.com"}')
    answer = sfn.describe_execution(executionArn=arn)
    assert (answer["executionArn"], answer["stateMachineArn"], answer["name"]) == (
        arn,
        MACHINE_ARN,
        "described",
    )
    assert (answer["status"], answer["error"], answer["cause"]) == ("FAILED", "KeyError", CAUSE)
    assert (answer["startDate"], answer["stopDate"]) == (T0, T0 + timedelta(seconds=5))
    assert answer["input"] == '{"who": "alice.pii@example.com"}' and answer["inputDetails"] == {
        "included": True
    }
    assert (answer["redriveCount"], answer["redriveStatus"]) == (0, "REDRIVABLE")
    assert answer["redriveStatusReason"] == "Execution is FAILED and can be redriven"
    metadata = sfn.describe_execution(executionArn=arn, includedData="METADATA_ONLY")
    assert "input" not in metadata and "inputDetails" not in metadata
    assert metadata["error"] == "KeyError"


@pytest.mark.parametrize(
    ("status", "stopped_days_ago", "redrivable"),
    [
        ("FAILED", 0, "REDRIVABLE"),
        ("TIMED_OUT", 13, "REDRIVABLE"),
        ("ABORTED", 14, "REDRIVABLE"),
        ("FAILED", 15, "NOT_REDRIVABLE"),
        ("SUCCEEDED", 0, "NOT_REDRIVABLE"),
    ],
)
def test_the_redrive_status_follows_the_status_and_the_world_clock(
    world: OfflineAws, sfn: Any, status: str, stopped_days_ago: int, redrivable: str
) -> None:
    stop = NOW - timedelta(days=stopped_days_ago)
    arn = world.add_execution(
        MACHINE, "aged", status=status, start_date=stop - timedelta(seconds=5), stop_date=stop
    )
    assert sfn.describe_execution(executionArn=arn)["redriveStatus"] == redrivable


def test_get_execution_history_order_paging_and_execution_data(world: OfflineAws, sfn: Any) -> None:
    arn = world.add_execution(MACHINE, "historic", status="FAILED", start_date=T0, history=task_history())
    oldest_first = sfn.get_execution_history(executionArn=arn)["events"]
    assert [e["id"] for e in oldest_first] == [1, 2, 3, 4, 5, 6]
    first = sfn.get_execution_history(executionArn=arn, reverseOrder=True, maxResults=4)
    assert [e["type"] for e in first["events"]] == [
        "ExecutionFailed",
        "TaskFailed",
        "TaskStarted",
        "TaskScheduled",
    ]
    rest = sfn.get_execution_history(
        executionArn=arn, reverseOrder=True, maxResults=4, nextToken=first["nextToken"]
    )
    assert [e["id"] for e in rest["events"]] == [2, 1] and "nextToken" not in rest
    with pytest.raises(sfn.exceptions.InvalidToken):
        sfn.get_execution_history(executionArn=arn, nextToken=first["nextToken"])  # another order
    lean = sfn.get_execution_history(executionArn=arn, includeExecutionData=False)["events"]
    assert "input" not in lean[0]["executionStartedEventDetails"]
    assert lean[0]["executionStartedEventDetails"]["roleArn"] == ROLE
    assert "input" not in lean[1]["stateEnteredEventDetails"]
    assert lean[1]["stateEnteredEventDetails"]["name"] == "Work"
    assert lean[2]["taskScheduledEventDetails"]["parameters"] == '{"FunctionName":"fn"}', (
        "[U] parameters stay"
    )
    assert lean[4]["taskFailedEventDetails"]["cause"] == CAUSE, "a cause is not execution data"
    assert oldest_first[1]["stateEnteredEventDetails"]["input"] == '{"run":"2026-10-02"}'


def test_get_execution_history_default_page_is_100_events(world: OfflineAws, sfn: Any) -> None:
    arn = world.add_execution(MACHINE, "long", status="FAILED", start_date=T0, history=pass_history(T0, 60))
    page = sfn.get_execution_history(executionArn=arn)
    assert len(page["events"]) == 100 and page["nextToken"]
    assert len(sfn.get_execution_history(executionArn=arn, nextToken=page["nextToken"])["events"]) == 22


def test_an_execution_is_completed_from_its_history(world: OfflineAws, sfn: Any) -> None:
    arn = world.add_execution(MACHINE, "derived", status="FAILED", start_date=T0, history=task_history())
    answer = sfn.describe_execution(executionArn=arn)
    assert answer["stopDate"] == T0 + timedelta(milliseconds=910)
    assert (answer["error"], answer["cause"]) == ("KeyError", CAUSE)
    assert answer["input"] == '{"run":"2026-10-02"}', "the input of ExecutionStarted"


@pytest.mark.parametrize(
    ("change", "kwargs", "message"),
    [
        (lambda h: h, {"start_date": T0 + timedelta(seconds=1)}, "start_date does not match"),
        (lambda h: h, {"stop_date": T0 + timedelta(seconds=9)}, "stop_date does not match"),
        (lambda h: h, {"error": "ValueError"}, "error does not match"),
        (lambda h: h, {"input": '{"other": 1}'}, "input does not match"),
        (lambda h: h, {"status": "TIMED_OUT"}, "ends with ExecutionTimedOut"),
        (lambda h: h, {"status": "RUNNING"}, "cannot end with ExecutionFailed"),
        (lambda h: h, {"redrive_count": 1}, "does not match the ExecutionRedriven"),
        (lambda h: h[1:], {}, "sequential from 1"),
        (
            lambda h: (
                [{**h[0], "type": "TaskStateEntered", "stateEnteredEventDetails": {"name": "X"}}] + h[1:]
            ),
            {},
            "starts with ExecutionStarted",
        ),
        (lambda h: h[:3] + [{**h[3], "previousEventId": 9}] + h[4:], {}, "does not precede it"),
        (lambda h: h[:5] + [{**h[5], "timestamp": _iso(T0)}], {}, "older than the event before it"),
    ],
)
def test_an_execution_must_agree_with_its_history(
    world: OfflineAws, change: Any, kwargs: dict[str, Any], message: str
) -> None:
    options: dict[str, Any] = {"status": "FAILED", "start_date": T0, **kwargs}
    with pytest.raises(ValueError, match=message):
        world.add_execution(MACHINE, "disagrees", history=change(task_history()), **options)


def test_executions_without_a_history_get_a_minimal_one(world: OfflineAws, sfn: Any) -> None:
    arn = add_failed(world, "minimal", redrive_count=2, redrive_date=T0 + timedelta(seconds=3))
    events = sfn.get_execution_history(executionArn=arn)["events"]
    assert [e["type"] for e in events] == [
        "ExecutionStarted",
        "ExecutionFailed",
        "ExecutionRedriven",
        "ExecutionFailed",
        "ExecutionRedriven",
        "ExecutionFailed",
    ]
    assert [
        e["executionRedrivenEventDetails"]["redriveCount"] for e in events if e["type"] == "ExecutionRedriven"
    ] == [1, 2]
    assert events[-1]["executionFailedEventDetails"] == {"error": "KeyError", "cause": CAUSE}
    succeeded = world.add_execution(
        MACHINE, "ok", status="SUCCEEDED", start_date=T0, stop_date=T0 + timedelta(seconds=1)
    )
    assert sfn.describe_execution(executionArn=succeeded)["output"] == "{}"


def test_execution_fields_are_checked(world: OfflineAws) -> None:
    with pytest.raises(ValueError, match="status must be one of"):
        world.add_execution(MACHINE, "a", status="DONE", start_date=T0)
    with pytest.raises(ValueError, match="needs a stop_date"):
        world.add_execution(MACHINE, "b", status="FAILED", start_date=T0)
    with pytest.raises(ValueError, match="has no stop_date"):
        world.add_execution(MACHINE, "c", status="RUNNING", start_date=T0, stop_date=NOW)
    with pytest.raises(ValueError, match="not valid JSON"):
        world.add_execution(MACHINE, "d", status="RUNNING", start_date=T0, input="{")
    with pytest.raises(ValueError, match="unknown state machine"):
        world.add_execution("nope", "e", status="RUNNING", start_date=T0)
    world.add_state_machine("express", DEFINITION, type="EXPRESS")
    with pytest.raises(ValueError, match="EXPRESS"):
        world.add_execution("express", "f", status="RUNNING", start_date=T0)
    add_failed(world, "twice")
    with pytest.raises(ValueError, match="already exists"):
        add_failed(world, "twice")


# =========================================================================== redrive and restart


def test_a_redrive_makes_the_execution_run_again(world: OfflineAws, sfn: Any) -> None:
    arn = world.add_execution(MACHINE, "again", status="FAILED", start_date=T0, history=task_history())
    answer = sfn.redrive_execution(executionArn=arn, clientToken="incident-42")
    assert answer["redriveDate"] == NOW
    described = sfn.describe_execution(executionArn=arn)
    assert (described["status"], described["redriveCount"], described["redriveDate"]) == ("RUNNING", 1, NOW)
    assert "stopDate" not in described and "error" not in described
    assert described["redriveStatus"] == "NOT_REDRIVABLE"
    last = sfn.get_execution_history(executionArn=arn, reverseOrder=True, maxResults=1)["events"][0]
    assert (last["type"], last["id"], last["previousEventId"]) == ("ExecutionRedriven", 7, 6)
    assert last["executionRedrivenEventDetails"] == {"redriveCount": 1}


def test_redrive_is_idempotent_per_client_token_for_15_minutes(world: OfflineAws, sfn: Any) -> None:
    arn = add_failed(world, "idempotent")
    first = sfn.redrive_execution(executionArn=arn, clientToken="key-1")["redriveDate"]
    world.set_now(NOW + timedelta(minutes=10))
    assert sfn.redrive_execution(executionArn=arn, clientToken="key-1")["redriveDate"] == first
    assert sfn.describe_execution(executionArn=arn)["redriveCount"] == 1
    with pytest.raises(sfn.exceptions.ExecutionNotRedrivable, match="RUNNING"):
        sfn.redrive_execution(executionArn=arn, clientToken="key-2")
    world.set_now(NOW + CLIENT_TOKEN_TTL + timedelta(seconds=1))
    with pytest.raises(sfn.exceptions.ExecutionNotRedrivable):
        sfn.redrive_execution(executionArn=arn, clientToken="key-1")


@pytest.mark.parametrize(
    ("status", "stopped", "started", "reason"),
    [
        ("SUCCEEDED", NOW, None, "SUCCEEDED"),
        ("FAILED", NOW - REDRIVE_WINDOW - timedelta(seconds=1), None, "14 days"),
        (
            "FAILED",
            datetime(2023, 11, 14, 12, 0, 5, tzinfo=UTC),
            datetime(2023, 11, 14, 12, tzinfo=UTC),
            "2023-11-15",
        ),
    ],
)
def test_redrive_refusals(
    world: OfflineAws, sfn: Any, status: str, stopped: datetime, started: datetime | None, reason: str
) -> None:
    arn = world.add_execution(
        MACHINE,
        "refused",
        status=status,
        start_date=started or stopped - timedelta(seconds=5),
        stop_date=stopped,
    )
    with pytest.raises(sfn.exceptions.ExecutionNotRedrivable, match=reason):
        sfn.redrive_execution(executionArn=arn)
    assert sfn.describe_execution(executionArn=arn)["status"] == status


def test_a_redriven_execution_can_fail_again(world: OfflineAws, sfn: Any, make_runtime: MakeRuntime) -> None:
    arn = world.add_execution(MACHINE, "twice", status="FAILED", start_date=T0, history=task_history())
    sfn.redrive_execution(executionArn=arn)
    world.set_now(NOW + timedelta(minutes=2))
    retried = [
        {"type": "TaskStateEntered", "stateEnteredEventDetails": {"name": "Work"}, "timestamp": NOW},
        {
            "type": "TaskFailed",
            "taskFailedEventDetails": {
                "resourceType": "lambda",
                "resource": "invoke",
                "error": "KeyError",
                "cause": CAUSE,
            },
        },
    ]
    world.finish_execution(arn, status="FAILED", events=retried, error="KeyError", cause=CAUSE)
    item = sfn.list_executions(stateMachineArn=MACHINE_ARN, statusFilter="FAILED")["executions"][0]
    assert (item["name"], item["redriveCount"], item["stopDate"]) == ("twice", 1, NOW + timedelta(minutes=2))
    rt = make_runtime(sfn_options(), factory=world.client_factory)
    latest = executions.read_history(rt, arn)
    assert [e["type"] for e in latest.events] == [
        "ExecutionRedriven",
        "TaskStateEntered",
        "TaskFailed",
        "ExecutionFailed",
    ]
    assert latest.complete
    first = executions.read_history(rt, arn, redrive_count=0)
    assert first.events[0]["type"] == "ExecutionStarted" and len(first.events) == 10
    with pytest.raises(ValueError, match="not RUNNING"):
        world.finish_execution(arn, status="FAILED")


def test_finish_execution_checks_its_arguments(world: OfflineAws, sfn: Any) -> None:
    arn = sfn.start_execution(stateMachineArn=MACHINE_ARN, name="to-finish")["executionArn"]
    with pytest.raises(ValueError, match="status must be one of"):
        world.finish_execution(arn, status="RUNNING")
    with pytest.raises(ValueError, match="unknown execution"):
        world.finish_execution(arn + "-nope", status="FAILED")
    with pytest.raises(ValueError, match="sequential"):
        world.finish_execution(
            arn,
            status="FAILED",
            events=[{"id": 9, "type": "TaskStateEntered", "stateEnteredEventDetails": {"name": "W"}}],
        )
    world.finish_execution(arn, status="SUCCEEDED", output='{"ok":true}')
    done = sfn.describe_execution(executionArn=arn)
    assert (done["status"], done["output"], done["stopDate"]) == ("SUCCEEDED", '{"ok":true}', NOW)


def test_start_execution_is_idempotent_for_a_running_execution(world: OfflineAws, sfn: Any) -> None:
    started = sfn.start_execution(stateMachineArn=MACHINE_ARN, name="restart-1", input='{"a":1}')
    assert started == {
        "executionArn": world.execution_arn(MACHINE, "restart-1"),
        "startDate": NOW,
        "ResponseMetadata": started["ResponseMetadata"],
    }
    world.set_now(NOW + timedelta(seconds=30))
    again = sfn.start_execution(stateMachineArn=MACHINE_ARN, name="restart-1", input='{"a":1}')
    assert (again["executionArn"], again["startDate"]) == (started["executionArn"], NOW)
    with pytest.raises(sfn.exceptions.ExecutionAlreadyExists):
        sfn.start_execution(stateMachineArn=MACHINE_ARN, name="restart-1", input='{"a":2}')
    described = sfn.describe_execution(executionArn=started["executionArn"])
    assert (described["status"], described["input"]) == ("RUNNING", '{"a":1}')
    history = sfn.get_execution_history(executionArn=started["executionArn"])["events"]
    assert [e["type"] for e in history] == ["ExecutionStarted"]
    assert history[0]["executionStartedEventDetails"]["roleArn"] == ROLE


def test_start_execution_refusals_and_express_machines(world: OfflineAws, sfn: Any) -> None:
    add_failed(world, "closed")
    with pytest.raises(sfn.exceptions.ExecutionAlreadyExists):
        sfn.start_execution(stateMachineArn=MACHINE_ARN, name="closed", input="{}")
    with pytest.raises(sfn.exceptions.InvalidName):
        sfn.start_execution(stateMachineArn=MACHINE_ARN, name="has space")
    with pytest.raises(sfn.exceptions.InvalidExecutionInput):
        sfn.start_execution(stateMachineArn=MACHINE_ARN, input="{not json")
    with pytest.raises(sfn.exceptions.StateMachineDoesNotExist):
        sfn.start_execution(stateMachineArn=world.state_machine_arn("nope"))
    generated = sfn.start_execution(stateMachineArn=MACHINE_ARN)["executionArn"]
    assert len(generated.rsplit(":", 1)[1]) == 36, "a generated name is a UUID"
    express = world.add_state_machine("fast", DEFINITION, type="EXPRESS")
    answer = sfn.start_execution(stateMachineArn=express, name="go")
    assert answer["executionArn"].startswith(f"arn:aws:states:{REGION}:{ACCOUNT}:express:fast:go:")
    with pytest.raises(sfn.exceptions.ExecutionDoesNotExist):
        sfn.describe_execution(executionArn=answer["executionArn"])


# =========================================================================== CloudWatch Logs


def _log_world() -> OfflineAws:
    world = make_world()
    world.add_function(FUNCTION)
    world.add_log_events(
        GROUP,
        "2026/10/02/[$LATEST]aaa",
        [
            {"timestamp": T0, "message": "START RequestId: req-1 Version: $LATEST\n"},
            {
                "timestamp": T0 + timedelta(milliseconds=500),
                "message": "[ERROR] KeyError: 'amount'\nTraceback ...",
            },
            {"timestamp": T0 + timedelta(seconds=1), "message": "END RequestId: req-1\n"},
        ],
    )
    world.add_log_events(
        GROUP,
        "2026/10/02/[$LATEST]bbb",
        [
            {
                "timestamp": T0 + timedelta(milliseconds=250),
                "message": "START RequestId: req-2 Version: $LATEST\n",
            },
            {"timestamp": T0 + timedelta(milliseconds=750), "message": "[INFO] done for req-2"},
        ],
    )
    world.add_log_events(
        "/sfn-etl/json",
        "s",
        [
            {"timestamp": T0, "message": json.dumps({"level": "INFO", "requestId": "r-1", "count": 3})},
            {"timestamp": T0, "message": json.dumps({"level": "ERROR", "requestId": "r-1", "count": 12})},
            {
                "timestamp": T0,
                "message": json.dumps({"level": "ERROR", "requestId": "r-2", "errorType": None}),
            },
            {
                "timestamp": T0,
                "message": json.dumps({"level": "WARN", "tags": ["etl", "sales"], "detail": {"rows": 7}}),
            },
            {"timestamp": T0, "message": "not json"},
        ],
    )
    return world


def _messages(world: OfflineAws, **params: Any) -> list[str]:
    params.setdefault("logGroupName", GROUP)
    return [e["message"] for e in world.client_factory("logs").filter_log_events(**params)["events"]]


def test_filter_log_events_orders_events_across_streams_within_the_window() -> None:
    world = _log_world()
    answer = world.client_factory("logs").filter_log_events(logGroupName=GROUP)
    assert [(e["logStreamName"][-3:], e["timestamp"] - epoch_ms(T0)) for e in answer["events"]] == [
        ("aaa", 0),
        ("bbb", 250),
        ("aaa", 500),
        ("bbb", 750),
        ("aaa", 1000),
    ]
    assert len({e["eventId"] for e in answer["events"]}) == 5
    window = _messages(world, startTime=epoch_ms(T0) + 250, endTime=epoch_ms(T0) + 750)
    assert window == [
        "START RequestId: req-2 Version: $LATEST\n",
        "[ERROR] KeyError: 'amount'\nTraceback ...",
        "[INFO] done for req-2",
    ]
    streams = _messages(world, logStreamNames=["2026/10/02/[$LATEST]bbb"])
    assert len(streams) == 2
    assert len(_messages(world, logStreamNamePrefix="2026/10/02/[$LATEST]a")) == 3


@pytest.mark.parametrize(
    ("pattern", "expected"),
    [
        ('"req-1"', 2),
        ("RequestId req-2", 1),
        ("?KeyError ?done", 2),
        ("RequestId -END", 2),
        ('"[ERROR] KeyError"', 1),
        ("%req-[12]%", 4),
        ("", 5),
    ],
)
def test_text_filter_patterns(pattern: str, expected: int) -> None:
    assert len(_messages(_log_world(), filterPattern=pattern)) == expected


@pytest.mark.parametrize(
    ("pattern", "expected"),
    [
        ('{ $.level = "ERROR" }', 2),
        ('{ $.level = "ERROR" && $.requestId = "r-1" }', 1),
        ('{ $.requestId = "r-2" || $.count > 10 }', 2),
        ("{ $.count >= 3 && $.count < 12 }", 1),
        ("{ $.level = ERR* }", 2),
        ('{ $.level != "ERROR" }', 2),
        ("{ $.errorType IS NULL }", 1),
        ("{ $.errorType NOT EXISTS }", 3),
        ('{ ($.level = "INFO") || ($.count = 12) }', 2),
        ('{ $.tags[1] = "sales" }', 1),
        ("{ $.tags[5] = * }", 0),
        ("{ $.detail.rows > 5 && $.detail.rows <= 7 }", 1),
        ("{ $.detail EXISTS }", 1),
    ],
)
def test_json_filter_patterns(pattern: str, expected: int) -> None:
    assert len(_messages(_log_world(), logGroupName="/sfn-etl/json", filterPattern=pattern)) == expected


@pytest.mark.parametrize(
    "pattern", ["[ip, user]", "{ $.level = }", "{ level = 1 }", '"open', "{ $.a < x }", "%(%"]
)
def test_unreadable_filter_patterns_are_invalid_parameters(pattern: str) -> None:
    with pytest.raises(ClientError) as raised:
        _messages(_log_world(), filterPattern=pattern)
    assert error_of(raised) == ("InvalidParameterException", 400)


def test_filter_log_events_parameters_and_paging() -> None:
    world = _log_world()
    logs = world.client_factory("logs")
    arn = f"arn:aws:logs:{REGION}:{ACCOUNT}:log-group:{GROUP}:*"
    assert len(logs.filter_log_events(logGroupIdentifier=arn)["events"]) == 5
    for params in ({}, {"logGroupName": GROUP, "logGroupIdentifier": GROUP}):
        with pytest.raises(logs.exceptions.InvalidParameterException):
            logs.filter_log_events(**params)
    with pytest.raises(logs.exceptions.InvalidParameterException):
        logs.filter_log_events(logGroupName=GROUP, logStreamNames=["a"], logStreamNamePrefix="a")
    with pytest.raises(logs.exceptions.InvalidParameterException):
        logs.filter_log_events(logGroupIdentifier=f"arn:aws:s3:::{GROUP.strip('/')}")
    first = logs.filter_log_events(logGroupName=GROUP, limit=2)
    second = logs.filter_log_events(logGroupName=GROUP, limit=2, nextToken=first["nextToken"])
    third = logs.filter_log_events(logGroupName=GROUP, limit=2, nextToken=second["nextToken"])
    assert [len(page["events"]) for page in (first, second, third)] == [2, 2, 1] and "nextToken" not in third
    with pytest.raises(logs.exceptions.InvalidParameterException):
        logs.filter_log_events(logGroupName=GROUP, filterPattern="ERROR", nextToken=first["nextToken"])


def test_late_logs_appear_once_ingested() -> None:
    world = make_world()
    late = {"timestamp": T0, "message": "late line", "ingestion_time": NOW + timedelta(minutes=7)}
    world.add_log_events(GROUP, "s", [late])
    assert _messages(world) == []
    world.set_now(NOW + timedelta(minutes=7))
    assert _messages(world) == ["late line"]
    with pytest.raises(ValueError, match="before the event's timestamp"):
        world.add_log_events(
            GROUP, "s", [{"timestamp": T0, "message": "x", "ingestion_time": T0 - timedelta(seconds=1)}]
        )


# =========================================================================== Lambda


@pytest.mark.parametrize(
    "reference",
    [
        FUNCTION,
        f"{FUNCTION}:$LATEST",
        f"arn:aws:lambda:{REGION}:{ACCOUNT}:function:{FUNCTION}",
        f"arn:aws:lambda:{REGION}:{ACCOUNT}:function:{FUNCTION}:$LATEST",
        f"{ACCOUNT}:function:{FUNCTION}",
    ],
)
def test_get_function_accepts_names_and_arns(reference: str) -> None:
    world = make_world()
    world.add_function(FUNCTION, handler="app.handler", layers=["etl-libs"])
    configuration = world.client_factory("lambda").get_function(FunctionName=reference)["Configuration"]
    assert configuration["FunctionName"] == FUNCTION
    assert configuration["FunctionArn"] == world.function_arn(FUNCTION)
    assert (configuration["Handler"], configuration["Runtime"], configuration["Version"]) == (
        "app.handler",
        "python3.11",
        "$LATEST",
    )
    assert configuration["LoggingConfig"] == {"LogFormat": "Text", "LogGroup": GROUP}
    assert configuration["Layers"] == [
        {"Arn": f"arn:aws:lambda:{REGION}:{ACCOUNT}:layer:etl-libs:1", "CodeSize": 1024}
    ]
    assert configuration["CodeSha256"]


def test_get_function_logging_config_and_refusals() -> None:
    world = make_world()
    world.add_function("json-fn", log_format="JSON", log_group="/sfn-etl/inventory", code_sha256="abc=")
    lam = world.client_factory("lambda")
    configuration = lam.get_function(FunctionName="json-fn")["Configuration"]
    assert configuration["LoggingConfig"] == {
        "LogFormat": "JSON",
        "LogGroup": "/sfn-etl/inventory",
        "ApplicationLogLevel": "INFO",
        "SystemLogLevel": "INFO",
    }
    assert configuration["CodeSha256"] == "abc="
    for params in ({"FunctionName": "json-fn", "Qualifier": "3"}, {"FunctionName": "json-fn:live"}):
        with pytest.raises(lam.exceptions.ResourceNotFoundException):
            lam.get_function(**params)
    with pytest.raises(ClientError, match="ValidationException"):
        lam.get_function(FunctionName="no spaces allowed")
    with pytest.raises(lam.exceptions.ResourceNotFoundException):
        lam.get_function(FunctionName=f"arn:aws:lambda:us-east-1:{ACCOUNT}:function:json-fn")
    with pytest.raises(ValueError, match="already exists"):
        world.add_function("json-fn")
    with pytest.raises(ValueError, match="Text or JSON"):
        world.add_function("other", log_format="json")


# =========================================================================== SQS


def test_sqs_receive_hides_messages_for_the_visibility_timeout() -> None:
    world = make_world()
    first_id = world.send_message(QUEUE, '{"n": 1}')
    world.send_message(QUEUE, '{"n": 2}')
    sqs = world.client_factory("sqs")
    received = sqs.receive_message(QueueUrl=QUEUE, MaxNumberOfMessages=1, WaitTimeSeconds=1)["Messages"]
    assert [(m["MessageId"], m["Body"]) for m in received] == [(first_id, '{"n": 1}')]
    assert received[0]["MD5OfBody"] == hashlib.md5(b'{"n": 1}', usedforsecurity=False).hexdigest()
    second = sqs.receive_message(QueueUrl=QUEUE, MaxNumberOfMessages=10)["Messages"]
    assert [m["Body"] for m in second] == ['{"n": 2}'], "the first one is in flight"
    assert "Messages" not in sqs.receive_message(QueueUrl=QUEUE), "an empty receive has no Messages"
    world.set_now(NOW + timedelta(seconds=31))
    again = sqs.receive_message(QueueUrl=QUEUE, MaxNumberOfMessages=10, MessageSystemAttributeNames=["All"])[
        "Messages"
    ]
    assert [m["Body"] for m in again] == ['{"n": 1}', '{"n": 2}']
    assert again[0]["ReceiptHandle"] != received[0]["ReceiptHandle"]
    assert again[0]["Attributes"]["ApproximateReceiveCount"] == "2"
    assert again[0]["Attributes"]["SentTimestamp"] == str(epoch_ms(NOW))


def test_sqs_delete_message_batch_reports_each_entry() -> None:
    world = make_world()
    for index in range(3):
        world.send_message(QUEUE, f"message {index}")
    sqs = world.client_factory("sqs")
    handles = [
        m["ReceiptHandle"] for m in sqs.receive_message(QueueUrl=QUEUE, MaxNumberOfMessages=10)["Messages"]
    ]
    answer = sqs.delete_message_batch(
        QueueUrl=QUEUE,
        Entries=[{"Id": "0", "ReceiptHandle": handles[0]}, {"Id": "1", "ReceiptHandle": "forged"}],
    )
    assert answer["Successful"] == [{"Id": "0"}]
    assert [(f["Id"], f["Code"], f["SenderFault"]) for f in answer["Failed"]] == [
        ("1", "ReceiptHandleIsInvalid", True)
    ]
    sqs.delete_message(QueueUrl=QUEUE, ReceiptHandle=handles[1])
    sqs.delete_message(QueueUrl=QUEUE, ReceiptHandle=handles[1])  # deleting twice succeeds
    assert world.queue_messages(QUEUE) == ["message 2"]
    with pytest.raises(sqs.exceptions.ReceiptHandleIsInvalid):
        sqs.delete_message(QueueUrl=QUEUE, ReceiptHandle="forged")


@pytest.mark.parametrize(
    ("entries", "error"),
    [
        ([], "EmptyBatchRequest"),
        ([{"Id": str(i), "ReceiptHandle": "h"} for i in range(11)], "TooManyEntriesInBatchRequest"),
        ([{"Id": "a", "ReceiptHandle": "h"}, {"Id": "a", "ReceiptHandle": "h"}], "BatchEntryIdsNotDistinct"),
        ([{"Id": "not ok!", "ReceiptHandle": "h"}], "InvalidBatchEntryId"),
    ],
)
def test_sqs_batch_refusals(entries: list[dict[str, str]], error: str) -> None:
    world = make_world()
    world.add_queue(QUEUE)
    sqs = world.client_factory("sqs")
    with pytest.raises(getattr(sqs.exceptions, error)):
        sqs.delete_message_batch(QueueUrl=QUEUE, Entries=entries)


def test_sqs_receive_parameters_are_bounded() -> None:
    world = make_world()
    world.add_queue(QUEUE)
    sqs = world.client_factory("sqs")
    for params in ({"MaxNumberOfMessages": 11}, {"MaxNumberOfMessages": 0}, {"WaitTimeSeconds": 21}):
        with pytest.raises(ClientError) as raised:
            sqs.receive_message(QueueUrl=QUEUE, **params)
        assert error_of(raised) == ("InvalidParameterValue", 400)


def test_sqs_delayed_messages_wait_for_the_queue_delay() -> None:
    world = make_world()
    world.add_queue(QUEUE, attributes={"DelaySeconds": 30})
    world.send_message(QUEUE, "later")
    sqs = world.client_factory("sqs")
    assert "Messages" not in sqs.receive_message(QueueUrl=QUEUE)
    counts = sqs.get_queue_attributes(QueueUrl=QUEUE, AttributeNames=["All"])["Attributes"]
    assert (counts["ApproximateNumberOfMessages"], counts["ApproximateNumberOfMessagesDelayed"]) == ("0", "1")
    world.set_now(NOW + timedelta(seconds=30))
    assert [m["Body"] for m in sqs.receive_message(QueueUrl=QUEUE)["Messages"]] == ["later"]
    assert world.queue_messages(QUEUE + "-unknown") == []


def test_sqs_queue_attributes_count_messages() -> None:
    world = make_world()
    policy = {
        "deadLetterTargetArn": f"arn:aws:sqs:{REGION}:{ACCOUNT}:healer-events-dlq",
        "maxReceiveCount": 5,
    }
    assert (
        world.add_queue(QUEUE, attributes={"RedrivePolicy": policy})
        == f"arn:aws:sqs:{REGION}:{ACCOUNT}:healer-events"
    )
    world.send_message(QUEUE, "a")
    world.send_message(QUEUE, "b")
    sqs = world.client_factory("sqs")
    sqs.receive_message(QueueUrl=QUEUE)
    wanted = ["ApproximateNumberOfMessages", "ApproximateNumberOfMessagesNotVisible", "RedrivePolicy"]
    attributes = sqs.get_queue_attributes(QueueUrl=QUEUE, AttributeNames=wanted)["Attributes"]
    assert attributes == {
        "ApproximateNumberOfMessages": "1",
        "ApproximateNumberOfMessagesNotVisible": "1",
        "RedrivePolicy": json.dumps(policy),
    }
    every = sqs.get_queue_attributes(QueueUrl=QUEUE, AttributeNames=["All"])["Attributes"]
    assert every["QueueArn"].endswith(":healer-events") and every["VisibilityTimeout"] == "30"
    assert "Attributes" not in sqs.get_queue_attributes(QueueUrl=QUEUE)
    with pytest.raises(sqs.exceptions.InvalidAttributeName):
        sqs.get_queue_attributes(QueueUrl=QUEUE, AttributeNames=["Colour"])
    with pytest.raises(ValueError, match="unknown attribute"):
        world.add_queue(QUEUE + "-2", attributes={"Colour": "blue"})
    with pytest.raises(ValueError, match="not an SQS queue URL"):
        world.add_queue("https://sqs.example.com/queue")


# =========================================================================== S3


@pytest.fixture
def s3_world() -> OfflineAws:
    world = make_world()
    world.put_object("acme-landing", "incoming/digits.csv", b"0123456789")
    world.put_object("acme-landing", "empty.csv", b"")
    return world


@pytest.mark.parametrize(
    ("byte_range", "status", "body", "content_range"),
    [
        (None, 200, b"0123456789", None),
        ("bytes=0-3", 206, b"0123", "bytes 0-3/10"),
        ("bytes=7-", 206, b"789", "bytes 7-9/10"),
        ("bytes=-4", 206, b"6789", "bytes 6-9/10"),
        ("bytes=-40", 206, b"0123456789", "bytes 0-9/10"),
        ("bytes=2-99999", 206, b"23456789", "bytes 2-9/10"),
        ("bytes=5-2", 200, b"0123456789", None),
        ("bytes=0-1,4-5", 200, b"0123456789", None),
    ],
)
def test_get_object_byte_ranges(
    s3_world: OfflineAws, byte_range: str | None, status: int, body: bytes, content_range: str | None
) -> None:
    params = {"Range": byte_range} if byte_range else {}
    answer = s3_world.client_factory("s3").get_object(
        Bucket="acme-landing", Key="incoming/digits.csv", **params
    )
    assert answer["ResponseMetadata"]["HTTPStatusCode"] == status
    assert answer["Body"].read() == body
    assert answer["ContentLength"] == len(body)
    assert answer.get("ContentRange") == content_range
    assert answer["ContentType"] == "text/csv" and answer["AcceptRanges"] == "bytes"
    assert answer["ETag"] == '"781e5e245d69b566979b86e28d23f2c7"'


@pytest.mark.parametrize(
    ("key", "params", "code", "status"),
    [
        ("incoming/digits.csv", {"Range": "bytes=10-"}, "InvalidRange", 416),
        ("empty.csv", {"Range": "bytes=0-99"}, "InvalidRange", 416),
        ("incoming/missing.csv", {}, "NoSuchKey", 404),
        ("incoming/digits.csv", {"IfMatch": '"other"'}, "PreconditionFailed", 412),
        ("incoming/digits.csv", {"IfNoneMatch": '"781e5e245d69b566979b86e28d23f2c7"'}, "304", 304),
        ("incoming/digits.csv", {"VersionId": "3HL4kqtJlcpXroDTDmjVBH40Nrjfkd"}, "InvalidArgument", 400),
    ],
)
def test_get_object_refusals(
    s3_world: OfflineAws, key: str, params: dict[str, str], code: str, status: int
) -> None:
    with pytest.raises(ClientError) as raised:
        s3_world.client_factory("s3").get_object(Bucket="acme-landing", Key=key, **params)
    assert error_of(raised) == (code, status)


def test_get_object_conditions_that_hold(s3_world: OfflineAws) -> None:
    s3 = s3_world.client_factory("s3")
    etag = '"781e5e245d69b566979b86e28d23f2c7"'
    assert s3.get_object(Bucket="acme-landing", Key="incoming/digits.csv", IfMatch=etag)["ETag"] == etag
    answer = s3.get_object(
        Bucket="acme-landing", Key="incoming/digits.csv", IfNoneMatch='"other"', PartNumber=1
    )
    assert answer["Body"].read() == b"0123456789"


def test_get_object_of_an_unversioned_bucket_accepts_the_null_version(s3_world: OfflineAws) -> None:
    s3 = s3_world.client_factory("s3")
    assert s3.get_object(Bucket="acme-landing", Key="empty.csv", VersionId="null")["Body"].read() == b""
    with pytest.raises(s3.exceptions.NoSuchKey):
        s3.get_object(Bucket="acme-landing", Key="incoming/missing.csv")


def test_put_object_checks_bucket_names_and_bodies(world: OfflineAws) -> None:
    for bucket in ("Upper-Case", "ab", "under_score", "a..b"):
        with pytest.raises(ValueError, match="bucket name"):
            world.put_object(bucket, "k", b"x")
    with pytest.raises(ValueError, match="object key"):
        world.put_object("acme-landing", "", b"x")
    with pytest.raises(ValueError, match="bytes or text"):
        world.put_object("acme-landing", "k", 1)


def test_serve_s3_datasets_installs_and_removes_the_factory(world: OfflineAws) -> None:
    from dag_healer.storage import s3

    world.put_object("acme-landing", "a.csv", "x\n1\n")
    with world.serve_s3_datasets():
        assert s3.s3_client().get_object(Bucket="acme-landing", Key="a.csv")["Body"].read() == b"x\n1\n"
    assert s3._factory is None


# =========================================================================== STS and the skeleton's clients


def test_get_caller_identity_names_the_world_account(world: OfflineAws) -> None:
    identity = world.client_factory("sts").get_caller_identity()
    assert identity["Account"] == ACCOUNT and identity["Arn"].startswith(f"arn:aws:iam::{ACCOUNT}:")


def test_aws_clients_and_read_history_page_through_a_long_history(
    world: OfflineAws, make_runtime: MakeRuntime
) -> None:
    arn = world.add_execution(MACHINE, "long", status="FAILED", start_date=T0, history=pass_history(T0, 1200))
    clients = AwsClients(sfn_options(), client_factory=world.client_factory)
    assert clients.sfn.describe_execution(executionArn=arn)["status"] == "FAILED"
    rt = make_runtime(sfn_options(), factory=world.client_factory)
    segment = executions.read_history(rt, arn)
    assert (len(segment.events), segment.pages, segment.complete) == (2402, 3, True)
    assert [e["id"] for e in segment.events[:3]] == [1, 2, 3]
    pages = world.calls_to("stepfunctions", "GetExecutionHistory")
    assert pages[0].params == {
        "executionArn": arn,
        "reverseOrder": True,
        "maxResults": executions.HISTORY_PAGE_SIZE,
        "includeExecutionData": False,
    }
    assert "nextToken" in pages[1].params


# =========================================================================== scenario files


def test_load_reads_every_section_of_a_scenario_file() -> None:
    world = OfflineAws.load(WORLD_DIR / "offline_world.json")
    assert (world.region, world.account, world.now()) == (REGION, ACCOUNT, NOW)
    sfn = world.client_factory("stepfunctions")
    machines = {m["name"]: m["type"] for m in sfn.list_state_machines()["stateMachines"]}
    assert machines == {"orders-etl": "STANDARD", "orders-express": "EXPRESS"}
    definition = json.loads(
        sfn.describe_state_machine(stateMachineArn=world.state_machine_arn("orders-etl"))["definition"]
    )
    assert definition["States"]["LoadOrders"]["Parameters"]["FunctionName"] == world.function_arn("orders-fn")
    failed = sfn.describe_execution(executionArn=world.execution_arn("orders-etl", "orders-2026-10-02"))
    assert (failed["status"], failed["error"]) == ("FAILED", "KeyError")
    assert "orders.owner@example.com" in failed["input"], "the input comes from ExecutionStarted"
    older = sfn.describe_execution(executionArn=world.execution_arn("orders-etl", "orders-2026-10-01"))
    assert (older["status"], older["input"]) == ("SUCCEEDED", '{"run_date":"2026-10-01"}')
    configuration = world.client_factory("lambda").get_function(FunctionName="orders-fn")["Configuration"]
    assert (configuration["Runtime"], configuration["LoggingConfig"]["LogGroup"]) == (
        "python3.12",
        "/offline/orders",
    )
    logs = world.client_factory("logs").filter_log_events(
        logGroupName="/offline/orders", filterPattern='{ $.log_level = "ERROR" }'
    )
    assert len(logs["events"]) == 1
    s3 = world.client_factory("s3")
    assert (
        s3.get_object(Bucket="offline-landing", Key="incoming/orders.csv")["Body"]
        .read()
        .startswith(b"order_id,")
    )
    assert (
        s3.get_object(Bucket="offline-landing", Key="notes/readme.txt")["Body"].read()
        == b"orders land here every morning\n"
    )
    queue = "https://sqs.eu-west-1.amazonaws.com/123456789012/offline-events"
    bodies = world.queue_messages(queue)
    assert json.loads(bodies[0])["detail"]["name"] == "orders-2026-10-02" and bodies[1] == '{"ping": true}'
    attributes = world.client_factory("sqs").get_queue_attributes(QueueUrl=queue, AttributeNames=["All"])[
        "Attributes"
    ]
    assert attributes["VisibilityTimeout"] == "60" and "offline-events-dlq" in attributes["RedrivePolicy"]
    assert world.io_calls == 9


def test_load_overrides_and_keyword_checks() -> None:
    later = NOW + timedelta(days=1)
    world = OfflineAws.load(WORLD_DIR / "offline_world.json", now=later)
    assert world.now() == later
    with pytest.raises(TypeError, match="clock"):
        OfflineAws.load(WORLD_DIR / "offline_world.json", clock=later)


def _write(tmp_path: Path, data: Any, name: str = "world.json") -> Path:
    path = tmp_path / name
    path.write_text(json.dumps(data) if not isinstance(data, str) else data, encoding="utf-8")
    return path


@pytest.mark.parametrize(
    ("data", "message"),
    [
        ("{not json", "invalid JSON"),
        ([], "must be a JSON object"),
        ({"regions": "x"}, "unknown key"),
        ({"state_machines": {"name": "x"}}, "must be a list of objects"),
        ({"state_machines": [{"name": "x"}]}, "definition or definition_file is required"),
        ({"state_machines": [{"name": "x", "definition": {}, "definition_file": "a.json"}]}, "not both"),
        ({"state_machines": [{"name": "x", "definition_file": "missing.asl.json"}]}, "not found"),
        (
            {"state_machines": [{"name": "x", "definition": DEFINITION, "role": "r"}]},
            r"state_machines\[0\]: unknown key",
        ),
        (
            {
                "executions": [
                    {
                        "state_machine": "x",
                        "name": "e",
                        "status": "RUNNING",
                        "start_date": "2026-10-02T06:00:00Z",
                    }
                ]
            },
            r"executions\[0\]: execution e: unknown state machine",
        ),
        ({"objects": [{"bucket": "b-bucket", "key": "k"}]}, "body or file is required"),
        ({"queues": [{"url": QUEUE, "messages": [{"text": "x"}]}]}, r"messages\[0\]: unknown key"),
        ({"now": "soon"}, "now: expected an ISO-8601 timestamp"),
        ({"region": "Europe"}, "not an AWS region name"),
    ],
)
def test_load_refuses_broken_scenario_files(tmp_path: Path, data: Any, message: str) -> None:
    with pytest.raises(ValueError, match=message):
        OfflineAws.load(_write(tmp_path, data))


def test_load_reads_yaml_definitions_and_event_files(tmp_path: Path) -> None:
    (tmp_path / "machine.asl.yaml").write_text(
        "StartAt: A\nStates:\n  A: {Type: Pass, End: true}\n", encoding="utf-8"
    )
    (tmp_path / "history.json").write_text(json.dumps(task_history()), encoding="utf-8")
    (tmp_path / "events.json").write_text(
        json.dumps({"events": [{"timestamp": 1790920800000, "message": "m"}]}), encoding="utf-8"
    )
    data = {
        "now": _iso(NOW),
        "state_machines": [{"name": MACHINE, "definition_file": "machine.asl.yaml"}],
        "executions": [
            {
                "state_machine": MACHINE,
                "name": "e",
                "status": "FAILED",
                "start_date": _iso(T0),
                "history_file": "history.json",
            }
        ],
        "log_events": [{"group": GROUP, "stream": "s", "events_file": "events.json"}],
    }
    world = OfflineAws.load(_write(tmp_path, data))
    sfn = world.client_factory("stepfunctions")
    assert json.loads(sfn.describe_state_machine(stateMachineArn=MACHINE_ARN)["definition"])["StartAt"] == "A"
    assert len(sfn.get_execution_history(executionArn=world.execution_arn(MACHINE, "e"))["events"]) == 6
    assert _messages(world) == ["m"]


@pytest.fixture
def sfn_etl_world() -> Iterator[OfflineAws]:
    yield OfflineAws.load(WORLD_DIR / "sfn_etl.json")


def test_the_sfn_etl_scenario_loads_and_answers(sfn_etl_world: OfflineAws) -> None:
    world = sfn_etl_world
    sfn = world.client_factory("stepfunctions")
    machines = sfn.list_state_machines()["stateMachines"]
    assert len(machines) == 3 and all(m["type"] == "STANDARD" for m in machines)
    failed = [
        item
        for machine in machines
        for item in sfn.list_executions(stateMachineArn=machine["stateMachineArn"], statusFilter="FAILED")[
            "executions"
        ]
    ]
    assert sorted(item["name"] for item in failed) == [
        "customers-2026-10-02",
        "inventory-2026-10-02",
        "sales-2026-10-02",
    ]
    for item in failed:
        history = sfn.get_execution_history(
            executionArn=item["executionArn"], reverseOrder=True, includeExecutionData=False
        )
        assert history["events"][0]["type"] == "ExecutionFailed"
        assert all(
            "input" not in value
            for event in history["events"]
            for value in event.values()
            if isinstance(value, dict)
        )
    assert world.queue_messages("https://sqs.eu-west-1.amazonaws.com/123456789012/healer-sfn-events")
