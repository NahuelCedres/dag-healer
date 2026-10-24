"""AWS clients and error translation (``stepfunctions/aws.py``).

``translate_error`` as a matrix (codes, HTTP statuses, connection and credential errors), ``call``
(results, translated errors, what is logged), the lazy imports (building ``AwsClients`` imports
nothing), the boto3 clients (configured endpoint, region, retries and timeouts), profiles, the
AssumeRole wiring (the STS call answered by a ``Stubber``: no network), client creation errors
and ``close``.
"""

from __future__ import annotations

import json
import logging
import os
import subprocess
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

import pytest
from botocore.exceptions import (
    ClientError,
    ConnectionClosedError,
    ConnectTimeoutError,
    EndpointConnectionError,
    NoCredentialsError,
    NoRegionError,
    ParamValidationError,
    PartialCredentialsError,
    ReadTimeoutError,
)

from dag_healer.core.errors import NotFound, NotSupported, OrchestratorError, PermissionDenied
from dag_healer.stepfunctions.aws import (
    CONNECT_TIMEOUT_SECONDS,
    MAX_ATTEMPTS,
    NOT_FOUND_CODES,
    PERMISSION_CODES,
    READ_TIMEOUT_SECONDS,
    SERVICES,
    TRANSIENT_CODES,
    AwsClients,
    call,
    client_config,
    error_code,
    error_message,
    is_transient,
    status_code,
    translate_error,
)
from dag_healer.stepfunctions.options import StepFunctionsOptions

from .conftest import ACCOUNT, REGION, make_client

ROLE = f"arn:aws:iam::{ACCOUNT}:role/healer-read"


def _client_error(
    code: str | None, status: int | None = 400, message: str | None = "it failed"
) -> ClientError:
    error: dict[str, Any] = {}
    if code is not None:
        error["Code"] = code
    if message is not None:
        error["Message"] = message
    response: dict[str, Any] = {"Error": error}
    if status is not None:
        response["ResponseMetadata"] = {"HTTPStatusCode": status}
    return ClientError(response, "DescribeExecution")  # type: ignore[arg-type]


# =========================================================================== translate_error


@pytest.mark.parametrize(
    ("code", "status", "kind", "transient"),
    [
        ("ExecutionDoesNotExist", 400, NotFound, False),
        ("StateMachineDoesNotExist", 400, NotFound, False),
        ("InvalidArn", 400, NotFound, False),
        ("ResourceNotFoundException", 400, NotFound, False),
        ("NoSuchKey", 404, NotFound, False),
        ("AWS.SimpleQueueService.NonExistentQueue", 400, NotFound, False),
        ("SomethingElse", 404, NotFound, False),
        ("AccessDeniedException", 400, PermissionDenied, False),
        ("AccessDenied", 403, PermissionDenied, False),
        ("KmsAccessDeniedException", 400, PermissionDenied, False),
        ("UnrecognizedClientException", 400, PermissionDenied, False),
        ("InvalidSignatureException", 400, PermissionDenied, False),
        ("MissingAuthenticationToken", 403, PermissionDenied, False),
        ("ExpiredTokenException", 400, PermissionDenied, False),
        ("SomethingElse", 401, PermissionDenied, False),
        ("SomethingElse", 403, PermissionDenied, False),
        ("ThrottlingException", 400, OrchestratorError, True),
        ("Throttling", 400, OrchestratorError, True),
        ("TooManyRequestsException", 429, OrchestratorError, True),
        ("KmsThrottlingException", 400, OrchestratorError, True),
        ("SlowDown", 503, OrchestratorError, True),
        ("ServiceUnavailable", 503, OrchestratorError, True),
        ("InternalFailure", 500, OrchestratorError, True),
        ("SomethingElse", 500, OrchestratorError, True),
        ("SomethingElse", 502, OrchestratorError, True),
        ("SomethingElse", 429, OrchestratorError, True),
        ("ValidationException", 400, OrchestratorError, False),
        ("ExecutionNotRedrivable", 400, OrchestratorError, False),
        ("SomethingElse", None, OrchestratorError, False),
    ],
)
def test_translate_client_errors(
    code: str, status: int | None, kind: type[OrchestratorError], transient: bool
) -> None:
    error = translate_error(_client_error(code, status), what="DescribeExecution")
    assert type(error) is kind
    assert error.transient is transient
    assert error.status_code == status
    assert error.detail == f"DescribeExecution failed: {code}: it failed"


def test_the_code_sets_are_disjoint() -> None:
    assert not TRANSIENT_CODES & PERMISSION_CODES
    assert not TRANSIENT_CODES & NOT_FOUND_CODES
    assert not PERMISSION_CODES & NOT_FOUND_CODES


@pytest.mark.parametrize(
    ("exc", "kind", "transient"),
    [
        (
            EndpointConnectionError(endpoint_url="https://states.eu-west-1.amazonaws.com"),
            OrchestratorError,
            True,
        ),
        (ConnectTimeoutError(endpoint_url="https://x.test"), OrchestratorError, True),
        (ReadTimeoutError(endpoint_url="https://x.test"), OrchestratorError, True),
        (ConnectionClosedError(endpoint_url="https://x.test"), OrchestratorError, True),
        (ConnectionRefusedError("refused"), OrchestratorError, True),
        (NoCredentialsError(), PermissionDenied, False),
        (PartialCredentialsError(provider="env", cred_var="AWS_SECRET_ACCESS_KEY"), PermissionDenied, False),
        (NoRegionError(), OrchestratorError, False),  # a configuration problem, not a permission one
        (ParamValidationError(report="Invalid length for parameter executionArn"), OrchestratorError, False),
        (ValueError("odd"), OrchestratorError, False),
    ],
)
def test_translate_other_errors(exc: BaseException, kind: type[OrchestratorError], transient: bool) -> None:
    error = translate_error(exc, what="GetExecutionHistory")
    assert type(error) is kind and error.transient is transient
    assert error.detail.startswith(f"GetExecutionHistory failed: {type(exc).__name__}")
    assert is_transient(exc) is transient


def test_translate_without_what_and_without_message() -> None:
    assert translate_error(ValueError()).detail == "ValueError"
    assert translate_error(_client_error("Throttling")).detail == "Throttling: it failed"


@pytest.mark.parametrize("error", [NotFound("x"), NotSupported("y"), OrchestratorError("z", transient=True)])
def test_core_errors_are_returned_unchanged(error: OrchestratorError) -> None:
    assert translate_error(error, what="anything") is error
    assert is_transient(error) is error.transient


def test_error_helpers_on_odd_objects() -> None:
    assert error_code(_client_error("Code1")) == "Code1"
    assert error_code(_client_error(None)) is None
    assert error_code(ValueError("x")) is None
    assert status_code(_client_error("C", 503)) == 503
    assert status_code(_client_error("C", None)) is None
    assert error_message(_client_error("C", message="the message")) == "the message"
    assert "C" in error_message(_client_error("C", message=None))
    assert error_message(ValueError()) == "ValueError"
    assert error_message(ValueError("text")) == "text"

    class Odd(Exception):
        response = {"Error": "not a mapping", "ResponseMetadata": {"HTTPStatusCode": True}}

    assert (error_code(Odd()), status_code(Odd())) == (None, None)


# =========================================================================== call


class FakeClient:
    """A client with one operation (``describe_execution``) that returns or raises what it is given."""

    def __init__(self, result: Any = None, error: BaseException | None = None) -> None:
        self.result = result
        self.error = error
        self.calls: list[dict[str, Any]] = []

    def describe_execution(self, **params: Any) -> Any:
        self.calls.append(params)
        if self.error is not None:
            raise self.error
        return self.result


def test_call_returns_the_answer_as_a_dict() -> None:
    client = FakeClient({"status": "FAILED"})
    assert call(client, "describe_execution", what="DescribeExecution", executionArn="arn:x") == {
        "status": "FAILED"
    }
    assert client.calls == [{"executionArn": "arn:x"}]
    assert call(FakeClient(None), "describe_execution") == {}


def test_call_raises_translated_errors_and_logs_no_message(caplog: pytest.LogCaptureFixture) -> None:
    caplog.set_level(logging.DEBUG)
    original = _client_error("ExecutionDoesNotExist", message="Execution Does Not Exist: 'secret-name'")
    with pytest.raises(NotFound) as raised:
        call(FakeClient(error=original), "describe_execution", what="DescribeExecution", executionArn="arn:p")
    assert raised.value.__cause__ is original
    assert "secret-name" in raised.value.detail  # RAW for the core, which masks it
    logged = " ".join(record.getMessage() for record in caplog.records)
    assert "describe_execution" in logged and "ExecutionDoesNotExist" in logged
    assert "secret-name" not in logged and "arn:p" not in logged


def test_call_keeps_core_errors() -> None:
    error = NotSupported("not here")
    with pytest.raises(NotSupported) as raised:
        call(FakeClient(error=error), "describe_execution")
    assert raised.value is error


def test_call_of_an_unknown_operation_is_an_orchestrator_error() -> None:
    with pytest.raises(OrchestratorError) as raised:
        call(FakeClient(), "no_such_operation")
    assert raised.value.transient is False and "AttributeError" in raised.value.detail


def test_call_with_a_stubbed_botocore_client(stubbed: Any) -> None:
    client, stubber = stubbed("stepfunctions")
    stubber.add_client_error(
        "describe_execution",
        service_error_code="ThrottlingException",
        expected_params={"executionArn": "arn:x"},
    )
    with pytest.raises(OrchestratorError) as raised:
        call(client, "describe_execution", what="DescribeExecution", executionArn="arn:x")
    assert raised.value.transient is True


# =========================================================================== lazy imports


def test_building_clients_and_the_adapter_imports_no_boto3() -> None:
    code = "\n".join(
        [
            "import json, sys",
            "from dag_healer.stepfunctions.aws import AwsClients",
            "from dag_healer.stepfunctions.options import StepFunctionsOptions",
            "from dag_healer.stepfunctions.adapter import StepFunctionsAdapter",
            "options = StepFunctionsOptions(region='eu-west-1', queue_url='https://sqs.eu-west-1.amazonaws.com/1/q')",
            "clients = AwsClients(options)",
            "repr(clients); clients.injected; clients.close(); clients.close()",
            "adapter = StepFunctionsAdapter(None, options=options)",
            "adapter.capabilities; adapter.doctor_checks(); adapter.sandbox_env(); adapter.close()",
            "print(json.dumps(sorted(m for m in ('boto3', 'botocore') if m in sys.modules)))",
        ]
    )
    env = dict(os.environ)
    env["PYTHONPATH"] = os.pathsep.join(entry for entry in sys.path if entry)
    result = subprocess.run(
        [sys.executable, "-c", code], capture_output=True, text=True, env=env, timeout=180, check=False
    )
    assert result.returncode == 0, result.stderr
    assert json.loads(result.stdout.strip().splitlines()[-1]) == []


# =========================================================================== boto3 clients


def test_client_config() -> None:
    config = client_config(StepFunctionsOptions(region="eu-central-1"))
    assert config.region_name == "eu-central-1"
    assert config.retries == {"mode": "adaptive", "max_attempts": MAX_ATTEMPTS} and MAX_ATTEMPTS == 10
    assert (
        (config.connect_timeout, config.read_timeout)
        == (CONNECT_TIMEOUT_SECONDS, READ_TIMEOUT_SECONDS)
        == (5, 20)
    )
    assert config.user_agent_extra.startswith("dag-healer/")
    assert client_config().region_name is None


def test_boto3_clients_use_the_configured_endpoint_and_region() -> None:
    clients = AwsClients(StepFunctionsOptions(region="eu-central-1", endpoint_url="http://localhost:4566"))
    sfn = clients.sfn
    assert sfn.meta.service_model.service_name == "stepfunctions"
    assert sfn.meta.region_name == "eu-central-1"
    assert sfn.meta.endpoint_url == "http://localhost:4566"
    assert sfn.meta.config.retries["mode"] == "adaptive"
    assert sfn.meta.config.connect_timeout == 5 and sfn.meta.config.read_timeout == 20
    assert clients.sfn is sfn, "clients are cached"
    names = {
        name: getattr(clients, attribute).meta.service_model.service_name
        for name, attribute in zip(SERVICES, ("sfn", "logs", "lambda_", "sqs", "s3", "sts"), strict=True)
    }
    assert names == {service: service for service in SERVICES}
    clients.close()
    clients.close()
    assert clients.closed is True


def test_boto3_clients_use_the_regional_endpoint_by_default() -> None:
    sfn = AwsClients(StepFunctionsOptions(region="eu-central-1")).sfn
    assert sfn.meta.endpoint_url == "https://states.eu-central-1.amazonaws.com"


def test_a_service_endpoint_serves_its_own_service_only() -> None:
    """A VPC interface endpoint serves one service: the other clients keep theirs."""
    vpce = "https://vpce-0a1b2c3d4e5f.states.eu-central-1.vpce.amazonaws.com"
    clients = AwsClients(StepFunctionsOptions(region="eu-central-1", endpoint_urls={"stepfunctions": vpce}))
    assert clients.sfn.meta.endpoint_url == vpce
    assert clients.sts.meta.endpoint_url == "https://sts.eu-central-1.amazonaws.com"
    assert clients.logs.meta.endpoint_url == "https://logs.eu-central-1.amazonaws.com"
    shared = AwsClients(
        StepFunctionsOptions(
            region="eu-central-1",
            endpoint_url="http://localhost:4566",
            endpoint_urls={"logs": "http://logs.localhost:4566"},
        )
    )
    assert shared.logs.meta.endpoint_url == "http://logs.localhost:4566"  # the service's own wins
    assert shared.sqs.meta.endpoint_url == "http://localhost:4566"


def _write_profile(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    config = tmp_path / "config"
    config.write_text("[profile dev]\nregion = us-east-2\n", encoding="utf-8")
    credentials = tmp_path / "credentials"
    credentials.write_text(
        "[dev]\naws_access_key_id = AKIDEXAMPLEDEV\naws_secret_access_key = dev-secret\n", encoding="utf-8"
    )
    monkeypatch.setenv("AWS_CONFIG_FILE", str(config))
    monkeypatch.setenv("AWS_SHARED_CREDENTIALS_FILE", str(credentials))
    for name in ("AWS_ACCESS_KEY_ID", "AWS_SECRET_ACCESS_KEY", "AWS_SESSION_TOKEN", "AWS_DEFAULT_REGION"):
        monkeypatch.delenv(name, raising=False)


def test_a_named_profile(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    _write_profile(tmp_path, monkeypatch)
    clients = AwsClients(StepFunctionsOptions(profile="dev"))
    assert clients.sfn.meta.region_name == "us-east-2", "the profile's region without options.region"
    assert clients._boto3_session().get_credentials().access_key == "AKIDEXAMPLEDEV"
    explicit = AwsClients(StepFunctionsOptions(profile="dev", region=REGION))
    assert explicit.sfn.meta.region_name == REGION, "options.region wins over the profile's"


def test_an_unknown_profile_is_a_core_error() -> None:
    clients = AwsClients(StepFunctionsOptions(profile="missing", region=REGION))
    with pytest.raises(OrchestratorError) as raised:
        clients.client("stepfunctions")
    assert raised.value.transient is False
    assert raised.value.detail.startswith("creating the stepfunctions client failed: ProfileNotFound")


def test_no_region_is_a_configuration_error(monkeypatch: pytest.MonkeyPatch) -> None:
    """Not a permission problem: the detail says what to set, so nobody goes looking at IAM."""
    monkeypatch.delenv("AWS_DEFAULT_REGION", raising=False)
    with pytest.raises(OrchestratorError) as raised:
        AwsClients(StepFunctionsOptions()).client("stepfunctions")
    assert type(raised.value) is OrchestratorError and raised.value.transient is False
    assert raised.value.detail == (
        "creating the stepfunctions client failed: NoRegionError: You must specify a region. "
        "(set orchestrator.options.region or AWS_DEFAULT_REGION)"
    )


def test_a_failing_factory_is_a_core_error() -> None:
    def factory(service: str) -> Any:
        raise EndpointConnectionError(endpoint_url=f"https://{service}.test")

    with pytest.raises(OrchestratorError) as raised:
        AwsClients(StepFunctionsOptions(), client_factory=factory).client("stepfunctions")
    assert raised.value.transient is True


# =========================================================================== AssumeRole


def _assumed(stubber: Any, expected: dict[str, Any]) -> None:
    stubber.add_response(
        "assume_role",
        {
            "Credentials": {
                "AccessKeyId": "ASIAEXAMPLEASSUMED",
                "SecretAccessKey": "assumed-secret",
                "SessionToken": "assumed-token",
                "Expiration": datetime.now(timezone.utc) + timedelta(hours=1),
            },
            "AssumedRoleUser": {
                "AssumedRoleId": "AROAEXAMPLE:dag-healer",
                "Arn": f"arn:aws:sts::{ACCOUNT}:assumed-role/healer-read/dag-healer",
            },
        },
        expected,
    )


@pytest.mark.parametrize(
    ("options", "expected"),
    [
        (
            {"role_external_id": "external-123", "role_session_name": "healer-worker"},
            {"RoleArn": ROLE, "RoleSessionName": "healer-worker", "ExternalId": "external-123"},
        ),
        ({}, {"RoleArn": ROLE, "RoleSessionName": "dag-healer"}),
    ],
    ids=["with an external id", "without"],
)
def test_assume_role_wiring(
    monkeypatch: pytest.MonkeyPatch, options: dict[str, Any], expected: dict[str, Any]
) -> None:
    from botocore.credentials import AssumeRoleCredentialFetcher, DeferredRefreshableCredentials
    from botocore.stub import Stubber

    sts = make_client("sts")
    stubber = Stubber(sts)
    _assumed(stubber, expected)
    stubber.activate()
    monkeypatch.setattr(AssumeRoleCredentialFetcher, "_create_client", lambda self: sts)
    clients = AwsClients(StepFunctionsOptions(region=REGION, role_arn=ROLE, **options))
    sfn = clients.sfn  # no AssumeRole yet: the credentials are fetched on first use
    credentials = clients._boto3_session().get_credentials()
    assert isinstance(credentials, DeferredRefreshableCredentials) and credentials.method == "assume-role"
    assert sfn.meta.region_name == REGION
    frozen = credentials.get_frozen_credentials()
    assert (frozen.access_key, frozen.secret_key, frozen.token) == (
        "ASIAEXAMPLEASSUMED",
        "assumed-secret",
        "assumed-token",
    )
    stubber.assert_no_pending_responses()
    assert "external-123" not in repr(clients)


def test_assume_role_without_source_credentials(monkeypatch: pytest.MonkeyPatch) -> None:
    for name in ("AWS_ACCESS_KEY_ID", "AWS_SECRET_ACCESS_KEY", "AWS_SESSION_TOKEN"):
        monkeypatch.delenv(name, raising=False)
    clients = AwsClients(StepFunctionsOptions(region=REGION, role_arn=ROLE))
    with pytest.raises(PermissionDenied) as raised:
        clients.client("stepfunctions")
    assert "NoCredentialsError" in raised.value.detail


# =========================================================================== injection and close


class Closable:
    def __init__(self, service: str, *, fail: bool = False) -> None:
        self.service = service
        self.fail = fail
        self.closed = 0

    def close(self) -> None:
        self.closed += 1
        if self.fail:
            raise RuntimeError("cannot close")


def test_an_injected_factory_is_called_once_per_service_and_never_closed() -> None:
    made: list[Closable] = []

    def factory(service: str) -> Closable:
        made.append(Closable(service))
        return made[-1]

    clients = AwsClients(StepFunctionsOptions(), client_factory=factory)
    assert clients.injected is True and "injected" in repr(clients)
    assert clients.sfn is clients.client("stepfunctions") and clients.sqs.service == "sqs"
    assert [client.service for client in made] == ["stepfunctions", "sqs"]
    clients.close()
    clients.close()
    assert [client.closed for client in made] == [0, 0], "the injector owns its clients"
    assert clients.sfn is not made[0], "a closed AwsClients builds new clients on demand"


def test_owned_clients_are_closed_once(monkeypatch: pytest.MonkeyPatch) -> None:
    made: list[tuple[Closable, dict[str, Any]]] = []

    class Session:
        def client(self, service: str, **kwargs: Any) -> Closable:
            made.append((Closable(service, fail=service == "logs"), kwargs))
            return made[-1][0]

    clients = AwsClients(StepFunctionsOptions(region=REGION, endpoint_url="https://vpce.example.test"))
    monkeypatch.setattr(clients, "_boto3_session", lambda: Session())
    assert clients.injected is False and "boto3" in repr(clients)
    assert (clients.sfn.service, clients.logs.service) == ("stepfunctions", "logs")
    assert [kwargs["endpoint_url"] for _client, kwargs in made] == ["https://vpce.example.test"] * 2
    assert all(kwargs["config"].region_name == REGION for _client, kwargs in made)
    clients.close()  # closing "logs" raises: best effort
    clients.close()
    assert [client.closed for client, _kwargs in made] == [1, 1]


# =========================================================================== botocore's own logs

PII = "alice.pii@example.com"
EXECUTION = f"arn:aws:states:{REGION}:{ACCOUNT}:execution:customers-sync:customers-2026-10-02"


class _Raw:
    """The raw body of an HTTP answer (what ``AWSResponse.content`` streams)."""

    def __init__(self, body: bytes) -> None:
        self.body = body

    def stream(self, **_kwargs: Any) -> Any:
        yield self.body


def _answered_over_http(client: Any, body: dict[str, Any]) -> None:
    """Answer every call of ``client`` at the HTTP level (``before-send``): unlike a ``Stubber``
    or the offline world (``before-call``), botocore then parses the body, and logs it at DEBUG,
    as it does with a real AWS answer."""
    from botocore.awsrequest import AWSResponse

    payload = json.dumps(body).encode("utf-8")
    headers = {"Content-Type": "application/x-amz-json-1.0", "x-amzn-RequestId": "0e8a3c2b-request"}

    def send(request: Any, **_kwargs: Any) -> Any:
        return AWSResponse(request.url, 200, headers, _Raw(payload))

    client.meta.events.register("before-send", send)


def _leaks(caplog: pytest.LogCaptureFixture) -> list[str]:
    return [record.name for record in caplog.records if PII in record.getMessage()]


def test_botocore_never_logs_execution_data(caplog: pytest.LogCaptureFixture) -> None:
    """``dag-healer -vv`` puts the root logger at DEBUG: botocore would log the request parameters
    (``StartExecution`` input) and the response body (``DescribeExecution`` input/output). The
    adapter's clients install the filter that drops those records; without it they leak."""
    from dag_healer.storage import s3

    caplog.set_level(logging.DEBUG)
    client = make_client("stepfunctions")
    _answered_over_http(
        client,
        {
            "executionArn": EXECUTION,
            "stateMachineArn": EXECUTION.replace(":execution:", ":stateMachine:").rsplit(":", 1)[0],
            "name": "customers-2026-10-02",
            "status": "FAILED",
            "startDate": 1790920800.0,
            "input": json.dumps({"requested_by": PII}),
        },
    )
    clients = AwsClients(StepFunctionsOptions(region=REGION), client_factory=lambda _service: client)
    described = call(clients.sfn, "describe_execution", what="DescribeExecution", executionArn=EXECUTION)
    assert PII in described["input"], "the answer went through botocore's parser"
    call(
        clients.sfn,
        "start_execution",
        what="StartExecution",
        stateMachineArn=described["stateMachineArn"],
        name="customers-2026-10-02-healer-588bd3c9f929",
        input=json.dumps({"requested_by": PII}),
    )
    assert _leaks(caplog) == []
    sent = [record for record in caplog.records if record.getMessage().startswith("Sending http request")]
    assert sent and sent[0].name == "botocore.endpoint", "botocore logged at DEBUG, without the data"

    parsers, endpoint = logging.getLogger("botocore.parsers"), logging.getLogger("botocore.endpoint")
    installed = [(target, item) for target in (parsers, endpoint) for item in target.filters]
    assert {type(item) for _target, item in installed} == {s3.PayloadLogFilter}
    for target, item in installed:
        target.removeFilter(item)
    try:  # the control: botocore itself writes the data into the log
        caplog.clear()
        call(clients.sfn, "describe_execution", what="DescribeExecution", executionArn=EXECUTION)
        assert "botocore.parsers" in _leaks(caplog)
    finally:
        for target, item in installed:
            target.addFilter(item)


def test_the_payload_filter_is_installed_once_and_keeps_the_other_records() -> None:
    from dag_healer.storage import s3

    s3.quiet_botocore_payloads()
    s3.quiet_botocore_payloads()
    for name in s3.PAYLOAD_LOG_RECORDS:
        assert sum(isinstance(item, s3.PayloadLogFilter) for item in logging.getLogger(name).filters) == 1
    keep = logging.LogRecord("botocore.parsers", logging.DEBUG, __file__, 1, "Event %s: calling", (), None)
    drop = logging.LogRecord(
        "botocore.parsers", logging.DEBUG, __file__, 1, "Response body:\n%r", (b"x",), None
    )
    other = logging.LogRecord(
        "botocore.hooks", logging.DEBUG, __file__, 1, "Response body:\n%r", (b"x",), None
    )
    assert [s3.PayloadLogFilter().filter(record) for record in (keep, drop, other)] == [True, False, True]
