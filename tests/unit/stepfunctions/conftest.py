"""Fixtures of the Step Functions adapter tests: no AWS credentials, no network.

* every test runs with dummy AWS credentials, ``AWS_EC2_METADATA_DISABLED=true`` and empty
  AWS config files (:func:`aws_env`), and botocore's HTTP sender refuses to send
  (:func:`no_aws_requests`), so nothing can reach a real account;
* :func:`stubbed` builds botocore clients with an active ``botocore.stub.Stubber`` (exact request
  assertions) and checks at teardown that every queued response was used;
* :func:`make_runtime` builds the :class:`~dag_healer.stepfunctions.runtime.SfnRuntime` the ports
  take, on those clients (or any client factory), with the fixed clock :data:`NOW`;
* :func:`load_json` / :func:`api_response` read ``tests/unit/fixtures/stepfunctions`` (the latter
  turns the ISO timestamps of Step Functions answers into datetimes, as botocore returns them;
  CloudWatch Logs and Lambda fixtures are read with :func:`load_json`).
"""

from __future__ import annotations

import copy
import json
from collections.abc import Callable, Iterator, Mapping
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import pytest

from dag_healer.stepfunctions.aws import AwsClients
from dag_healer.stepfunctions.options import StepFunctionsOptions
from dag_healer.stepfunctions.runtime import PipelineIndex, SfnRuntime, to_utc

REGION = "eu-west-1"
ACCOUNT = "123456789012"
#: The fixed "now" of the fixtures (day 2 of the example, one hour after the failures).
NOW = datetime(2026, 10, 2, 7, 0, tzinfo=timezone.utc)
FIXTURES = Path(__file__).resolve().parents[1] / "fixtures" / "stepfunctions"
SFN_REPO = Path(__file__).resolve().parents[1] / "fixtures" / "repos" / "sfn_etl"
INSTANCE = "sfn-test"

#: The state machines of the fixture repository (tests/unit/fixtures/repos/sfn_etl).
SALES_SM = f"arn:aws:states:{REGION}:{ACCOUNT}:stateMachine:SalesDailyEtlStateMachine-AbC123"
CUSTOMERS_SM = f"arn:aws:states:{REGION}:{ACCOUNT}:stateMachine:customers-sync"
INVENTORY_SM = f"arn:aws:states:{REGION}:{ACCOUNT}:stateMachine:InventoryReportStateMachine-XyZ789"
PIPELINES: dict[str, dict[str, str]] = {
    "sales_daily_etl": {"state_machine": SALES_SM, "logical_id": "SalesDailyEtlStateMachine"},
    "customers_sync": {"state_machine": CUSTOMERS_SM, "logical_id": "CustomersSyncStateMachine"},
    "inventory_report": {"state_machine": INVENTORY_SM, "logical_id": "InventoryReportStateMachine"},
}

_AWS_VARIABLES = (
    "AWS_ACCESS_KEY_ID",
    "AWS_SECRET_ACCESS_KEY",
    "AWS_SESSION_TOKEN",
    "AWS_SECURITY_TOKEN",
    "AWS_PROFILE",
    "AWS_DEFAULT_PROFILE",
    "AWS_ROLE_ARN",
    "AWS_ROLE_SESSION_NAME",
    "AWS_WEB_IDENTITY_TOKEN_FILE",
    "AWS_CONTAINER_CREDENTIALS_RELATIVE_URI",
    "AWS_CONTAINER_CREDENTIALS_FULL_URI",
    "AWS_CONTAINER_AUTHORIZATION_TOKEN",
    "AWS_CONTAINER_AUTHORIZATION_TOKEN_FILE",
    "AWS_ENDPOINT_URL",
    "AWS_REGION",
    "AWS_DEFAULT_REGION",
    "HEALER_SFN_QUEUE_URL",
)
#: Keys whose ISO-8601 values botocore returns as datetimes (Step Functions and S3 answers;
#: CloudWatch Logs timestamps are epoch milliseconds and Lambda's LastModified is text, so
#: their fixtures are read with load_json).
_TIMESTAMP_KEYS = frozenset(
    {"timestamp", "startDate", "stopDate", "redriveDate", "creationDate", "updateDate"}
)


@pytest.fixture(autouse=True)
def no_aws_requests() -> Iterator[list[str]]:
    """No request leaves the process: botocore's HTTP sender refuses to send, and a test that
    tried fails at teardown (a ``Stubber``, the offline world and ``before-send`` answers stop
    a call before it is sent)."""
    from dag_healer.stepfunctions.testing import refuse_aws_requests

    with refuse_aws_requests() as refused:
        yield refused
    assert refused == [], f"requests that would have left the process: {refused}"


@pytest.fixture(autouse=True)
def aws_env(monkeypatch: pytest.MonkeyPatch, tmp_path_factory: pytest.TempPathFactory) -> None:
    """Dummy credentials, IMDS off and empty AWS config files for every test."""
    for name in _AWS_VARIABLES:
        monkeypatch.delenv(name, raising=False)
    empty = tmp_path_factory.mktemp("aws") / "empty"
    empty.write_text("", encoding="utf-8")
    monkeypatch.setenv("AWS_ACCESS_KEY_ID", "testing")
    monkeypatch.setenv("AWS_SECRET_ACCESS_KEY", "testing")
    monkeypatch.setenv("AWS_SESSION_TOKEN", "testing")
    monkeypatch.setenv("AWS_DEFAULT_REGION", REGION)
    monkeypatch.setenv("AWS_EC2_METADATA_DISABLED", "true")
    monkeypatch.setenv("AWS_CONFIG_FILE", str(empty))
    monkeypatch.setenv("AWS_SHARED_CREDENTIALS_FILE", str(empty))


def make_client(service: str, *, region: str = REGION) -> Any:
    """A botocore client with dummy credentials (nothing is sent until a call is made)."""
    import botocore.session

    session = botocore.session.Session()
    return session.create_client(
        service,
        region_name=region,
        aws_access_key_id="testing",
        aws_secret_access_key="testing",
        aws_session_token="testing",
    )


Stubbed = Callable[[str], tuple[Any, Any]]


@pytest.fixture
def stubbed() -> Iterator[Stubbed]:
    """``client, stubber = stubbed("stepfunctions")``: an active Stubber per service (cached per
    test, so every port of a runtime shares it); every queued response must be consumed."""
    from botocore.stub import Stubber

    made: dict[str, tuple[Any, Any]] = {}

    def _make(service: str) -> tuple[Any, Any]:
        if service not in made:
            client = make_client(service)
            stubber = Stubber(client)
            stubber.activate()
            made[service] = (client, stubber)
        return made[service]

    yield _make
    for _client, stubber in made.values():
        stubber.assert_no_pending_responses()
        stubber.deactivate()


def sfn_options(**overrides: Any) -> StepFunctionsOptions:
    """Options of the fixture repository's three pipelines (``overrides`` replace keys)."""
    data: dict[str, Any] = {
        "region": REGION,
        "template": "template.yaml",
        "pipelines": copy.deepcopy(PIPELINES),
        "statuses": ["FAILED", "TIMED_OUT"],
    }
    data.update(overrides)
    return StepFunctionsOptions.model_validate(data)


MakeRuntime = Callable[..., SfnRuntime]


@pytest.fixture
def make_runtime(stubbed: Stubbed) -> MakeRuntime:
    """``make_runtime(options=None, *, factory=None, now=NOW, cfg=None, instance=INSTANCE)``.

    Without ``factory`` the clients are the :func:`stubbed` ones (queue responses on the stubber
    returned by ``stubbed(service)``); a ``factory(service) -> client`` (e.g.
    ``OfflineAws.client_factory``) replaces them.
    """

    def _make(
        options: StepFunctionsOptions | None = None,
        *,
        factory: Callable[[str], Any] | None = None,
        now: datetime = NOW,
        cfg: Any | None = None,
        instance: str = INSTANCE,
    ) -> SfnRuntime:
        opts = options or sfn_options()
        client_factory = factory or (lambda service: stubbed(service)[0])
        return SfnRuntime(
            options=opts,
            instance=instance,
            clients=AwsClients(opts, client_factory=client_factory),
            pipelines=PipelineIndex.from_options(opts),
            clock=lambda: now,
            cfg=cfg,
        )

    return _make


def load_json(relative: str) -> Any:
    """A JSON fixture of ``tests/unit/fixtures/stepfunctions`` as written (ISO text stays text)."""
    return json.loads((FIXTURES / relative).read_text(encoding="utf-8"))


def _datetimes(value: Any) -> Any:
    if isinstance(value, Mapping):
        out = {}
        for key, item in value.items():
            if key in _TIMESTAMP_KEYS and isinstance(item, (str, int, float)) and not isinstance(item, bool):
                out[key] = to_utc(item)
            else:
                out[key] = _datetimes(item)
        return out
    if isinstance(value, list):
        return [_datetimes(item) for item in value]
    return value


def api_response(relative_or_data: str | Any) -> Any:
    """An API response fixture with its timestamps as aware datetimes (what botocore returns)."""
    data = load_json(relative_or_data) if isinstance(relative_or_data, str) else relative_or_data
    return _datetimes(copy.deepcopy(data))
