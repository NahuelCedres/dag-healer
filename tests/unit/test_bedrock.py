"""Unit tests for dag_healer.agent.bedrock: Claude on Amazon Bedrock (``llm.provider: bedrock``).

The provider runs end to end through the real SDK (``anthropic.AnthropicBedrock``) and
botocore's SigV4 signer, offline: fictional static credentials, an ``httpx2.MockTransport`` for
the HTTP client and answers shaped like Bedrock's. Every test runs without the AWS settings of
the machine (``no_machine_aws`` of tests/conftest.py: no AWS variable, no shared AWS file, no
instance metadata service), so a developer's or CI's real AWS setup never reaches a test, and
``aws_env`` gives a test the AWS files of its own. No test touches the network: the tests of the
client that the provider builds itself (redirects, proxies) patch
``httpx2.HTTPTransport.handle_request``, the network transport, so no request leaves the
process, and those of the HTTP/1.1 stack itself (httpcore2 and h11, with their DEBUG records) run
on httpcore2's in-memory network.
"""

from __future__ import annotations

import importlib.util
import inspect
import json
import logging
import os
import subprocess
import sys
import types
from collections.abc import Callable, Iterator
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any
from urllib.parse import quote, unquote

import anthropic
import h11
import httpcore2
import httpx2
import pytest

from dag_healer.agent import bedrock
from dag_healer.agent.bedrock import (
    AWS_SDK_MISSING,
    BASE_URL_EMPTY,
    BASE_URL_INVALID,
    CREDENTIAL_NOT_PRINTABLE,
    EMPTY_API_KEY_WITH_PROFILE,
    FALLBACKS_IGNORED,
    JSON_ANSWER_INSTRUCTION,
    JSON_ANSWER_INVALID,
    JSON_RETRY_INSTRUCTION,
    MESSAGE_LEFT_OUT,
    MODEL_NOT_PRINTABLE,
    NO_CREDENTIALS,
    NO_REGION,
    PROFILE_AND_API_KEY,
    PROFILE_REGION,
    REDIRECTED,
    REGION_BLANK,
    REGION_INVALID,
    UNKNOWN_ERROR_TYPE,
    BedrockProvider,
    bedrock_errors,
    configured_region,
    hide_credentials,
    missing_aws_sdk,
    model_alias_problem,
    region_setting,
    settings_problems,
)
from dag_healer.agent.classify import classify_incident
from dag_healer.agent.context import IncidentContext
from dag_healer.agent.llm import Classification, LLMRefusal, LLMUnavailable, RepairSessionResult, get_provider
from dag_healer.config import HealerConfig, config_from_mapping
from dag_healer.storage import s3

SRC = Path(__file__).resolve().parents[2] / "src"
TESTS = Path(__file__).resolve().parents[1]
REGION = "eu-west-1"
MODEL = "eu.anthropic.claude-opus-5-5"
HOST = f"bedrock-runtime.{REGION}.amazonaws.com"
INVOKE_PATH = f"/model/{MODEL}/invoke"
ACCESS_KEY = "AKIDEXAMPLE"
SECRET_KEY = "wJalrXUtnFEMI/K7MDENG+bPxRfiCYEXAMPLEKEY"
SESSION_TOKEN = "example-session-token"
API_KEY = "bedrock-api-key-EXAMPLE"
SECRETS = (SECRET_KEY, SESSION_TOKEN, API_KEY)
VPC_ENDPOINT = "https://vpce-0123456789abcdef0-abcdefgh.bedrock-runtime.eu-west-1.vpce.amazonaws.com"
SIGNATURE = "EqQBCkYIDRgCKkBexampleSignatureOfAThinkingBlockEXAMPLE=="

CLASSIFICATION = {
    "category": "code",
    "confidence": 0.88,
    "diagnosis": "parse_signup_date rejects ISO-8601 timestamps such as 2026-10-01T08:30:00Z.",
    "violated_rule": None,
    "suspected_file": "src/etl/customers.py",
    "suspected_symbol": "parse_signup_date",
    "evidence": ["ValueError: unconverted data remains: T08:30:00Z"],
    "fix_summary": "Parse the value with datetime.fromisoformat.",
}
CLASSIFICATION_JSON = json.dumps(CLASSIFICATION)


# --------------------------------------------------------------------------- isolation


@dataclass
class AwsFiles:
    """The shared AWS config and credentials files of a test (absent until written)."""

    config: Path
    credentials: Path

    def write(self, *, config: str = "", credentials: str = "") -> None:
        self.config.write_text(config, encoding="utf-8", newline="\n")
        self.credentials.write_text(credentials, encoding="utf-8", newline="\n")


@pytest.fixture
def aws_env(isolated_aws: Path) -> AwsFiles:
    """The AWS files of the test, and no AWS setting of the machine (``isolated_aws`` of
    tests/conftest.py: variables unset, shared files absent, instance metadata service disabled,
    no cached SDK session)."""
    return AwsFiles(isolated_aws / "aws-config", isolated_aws / "aws-credentials")


@pytest.fixture
def aws_sdk(aws_env: AwsFiles) -> AwsFiles:
    """``aws_env`` where the SDK can build a client and sign (boto3 and botocore installed)."""
    pytest.importorskip("boto3")
    pytest.importorskip("botocore")
    return aws_env


@pytest.fixture
def aws_keys(aws_sdk: AwsFiles, monkeypatch: pytest.MonkeyPatch) -> AwsFiles:
    """``aws_sdk`` with fictional temporary credentials in the environment."""
    monkeypatch.setenv("AWS_ACCESS_KEY_ID", ACCESS_KEY)
    monkeypatch.setenv("AWS_SECRET_ACCESS_KEY", SECRET_KEY)
    monkeypatch.setenv("AWS_SESSION_TOKEN", SESSION_TOKEN)
    return aws_sdk


DEV_PROFILE = {
    "config": "[profile dev]\nregion = eu-central-1\n",
    "credentials": f"[dev]\naws_access_key_id = {ACCESS_KEY}\naws_secret_access_key = {SECRET_KEY}\n",
}
#: Temporary credentials held by a profile of the shared files only, as an instance or container
#: role's are held by no variable. Lower case on purpose: no pattern for random-looking strings
#: sees them, so only the provider's knowledge of the credentials it signs with hides them.
ROLE_KEY_ID = "ASIAEXAMPLEROLEKEY01"
ROLE_SECRET = "example/profile+role=private/held-in-no-variable"
ROLE_TOKEN = "example/profile+role=value/held-in-no-variable"
ROLE_PROFILE = {
    "config": "[profile role]\nregion = eu-west-1\n",
    "credentials": (
        f"[role]\naws_access_key_id = {ROLE_KEY_ID}\naws_secret_access_key = {ROLE_SECRET}\n"
        f"aws_session_token = {ROLE_TOKEN}\n"
    ),
}


@pytest.fixture
def exported_shell(monkeypatch: pytest.MonkeyPatch) -> None:
    """A shell hours after ``eval "$(aws configure export-credentials --format env)"``: the exported
    keys and their expiry, long past, next to other settings of a developer's machine. Requested
    before ``aws_env``, it shows what the isolation leaves of such a shell."""
    for name, value in {
        "AWS_ACCESS_KEY_ID": "ASIAEXAMPLEEXPORTED1",
        "AWS_SECRET_ACCESS_KEY": "example-exported-secret-key",
        "AWS_SESSION_TOKEN": "example-exported-session-value",
        "AWS_CREDENTIAL_EXPIRATION": "2020-01-01T00:00:00Z",
        "AWS_PROFILE": "example-developer-profile",
        "AWS_REGION": "us-east-1",
        "AWS_MAX_ATTEMPTS": "9",
        "AWS_EXAMPLE_FUTURE_SETTING": "x",
        "BOTO_EXAMPLE_SETTING": "x",
    }.items():
        monkeypatch.setenv(name, value)


# --------------------------------------------------------------------------- helpers


def bedrock_cfg(**llm: Any) -> HealerConfig:
    """A configuration with ``llm.provider: bedrock``; a ``None`` setting is left out of the file."""
    settings = {"provider": "bedrock", "model": MODEL, "aws_region": REGION, **llm}
    return config_from_mapping({"llm": {key: value for key, value in settings.items() if value is not None}})


def answer(
    content: list[dict[str, Any]],
    *,
    stop_reason: str = "end_turn",
    usage: dict[str, Any] | None = None,
    **extra: Any,
) -> dict[str, Any]:
    """An InvokeModel answer body (the Messages API shape Bedrock returns for Claude)."""
    return {
        "id": "msg_bdrk_01ExampleAnswer",
        "type": "message",
        "role": "assistant",
        "model": "claude-opus-5-5",
        "content": content,
        "stop_reason": stop_reason,
        "stop_sequence": None,
        "usage": usage or {"input_tokens": 100, "output_tokens": 20},
        **extra,
    }


def thinking() -> dict[str, Any]:
    """The thinking block Claude Opus 5.5 returns (empty text by default, always signed)."""
    return {"type": "thinking", "thinking": "", "signature": SIGNATURE}


def text(value: str) -> dict[str, Any]:
    return {"type": "text", "text": value}


def aws_error(status: int, message: str, error_type: str | None = None) -> tuple[Any, ...]:
    """An error answer: ``{"message": ...}``, with the ``x-amzn-ErrorType`` header AWS sends."""
    if error_type is None:
        return status, {"message": message}
    return (
        status,
        {"message": message},
        {"x-amzn-errortype": f"{error_type}:http://internal.amazon.com/coral/"},
    )


class FakeBedrock:
    """The bedrock-runtime endpoint: queued answers through an httpx2.MockTransport, requests kept."""

    def __init__(self, *answers: dict[str, Any] | tuple[Any, ...] | Exception) -> None:
        self.answers = list(answers)
        self.requests: list[httpx2.Request] = []

    def handler(self, request: httpx2.Request) -> httpx2.Response:
        self.requests.append(request)
        if not self.answers:
            raise AssertionError("unexpected extra request")
        item = self.answers.pop(0)
        if isinstance(item, Exception):
            raise item
        status, body, *extra = item if isinstance(item, tuple) else (200, item)
        headers = {"x-amzn-requestid": "0123abcd-example", **(extra[0] if extra else {})}
        return httpx2.Response(status, json=body, headers=headers)

    def http_client(self) -> httpx2.Client:
        return httpx2.Client(transport=httpx2.MockTransport(self.handler))

    def client(self, **kwargs: Any) -> anthropic.AnthropicBedrock:
        """A client without retries (for the error answers), signed with the environment's keys."""
        return anthropic.AnthropicBedrock(
            aws_region=REGION, http_client=self.http_client(), max_retries=0, **kwargs
        )

    def body(self, index: int = 0) -> dict[str, Any]:
        return json.loads(self.requests[index].content)


def provider(api: FakeBedrock, cfg: HealerConfig | None = None, **kwargs: Any) -> BedrockProvider:
    """The provider building its own client (the SDK's defaults) on the fake endpoint."""
    return BedrockProvider(cfg or bedrock_cfg(), http_client=api.http_client(), **kwargs)


def assert_sigv4(request: httpx2.Request, region: str = REGION) -> None:
    authorization = request.headers["authorization"]
    assert authorization.startswith(f"AWS4-HMAC-SHA256 Credential={ACCESS_KEY}/")
    assert f"/{region}/bedrock/aws4_request" in authorization
    assert "x-amz-date" in request.headers


def assert_bedrock_request(api: FakeBedrock, index: int = 0) -> dict[str, Any]:
    """What every request to Bedrock looks like: InvokeModel, no beta, no fallback, effort sent."""
    request = api.requests[index]
    body = api.body(index)
    assert (request.method, request.url.host, request.url.path) == ("POST", HOST, INVOKE_PATH)
    assert "anthropic-beta" not in request.headers
    for absent in ("model", "betas", "anthropic_beta", "fallbacks", "stream", "thread"):
        assert absent not in body
    for absent in ("thinking", "tool_choice", "temperature", "top_p", "top_k"):
        assert absent not in body
    assert "anthropic_version" in body  # the SDK's value; not asserted, it changes with the SDK
    assert body["max_tokens"] == 16000
    assert body["output_config"]["effort"] == "high"
    assert body["system"][0]["type"] == "text"
    assert body["system"][0]["cache_control"] == {"type": "ephemeral"}
    assert body["messages"][0]["role"] == "user"
    return body


def structured_format() -> dict[str, Any]:
    return {"type": "json_schema", "schema": anthropic.transform_schema(Classification.model_json_schema())}


def json_instruction() -> dict[str, Any]:
    schema = json.dumps(Classification.model_json_schema(), sort_keys=True)
    return {"type": "text", "text": JSON_ANSWER_INSTRUCTION.format(schema=schema)}


def leaked(text: str, *values: str) -> list[str]:
    """The ``values`` that ``text`` still holds: whole or as a fragment of 16 characters, as it is,
    URL-decoded or JSON-unescaped."""
    forms = (text, unquote(text), text.replace("\\/", "/"), "".join(text.split()))
    return [
        value
        for value in values
        if any(
            value[start : start + 16] in form for form in forms for start in range(max(1, len(value) - 15))
        )
    ]


def customers_incident() -> IncidentContext:
    """A CODE failure of the classification tests (no real data)."""
    traceback = (
        "Traceback (most recent call last):\n"
        '  File "/opt/airflow/repo/src/etl/customers.py", line 19, in parse_signup_date\n'
        "ValueError: unconverted data remains: T08:30:00Z"
    )
    return IncidentContext(
        incident_id="0123456789abcdef0123456789abcdef",
        dag_id="customers_sync",
        run_id="manual__2026-10-26T08:30:00+00:00",
        task_id="transform",
        error_text=traceback,
        exception_type="ValueError",
        exception_message="unconverted data remains: T08:30:00Z",
        traceback_text=traceback,
        repo_frames=[{"file": "src/etl/customers.py", "line": 19, "function": "parse_signup_date"}],
        contract_status="approved",
    )


# --------------------------------------------------------------------------- settings


def test_get_provider_builds_the_bedrock_provider_without_io(
    aws_env: AwsFiles, monkeypatch: pytest.MonkeyPatch
) -> None:
    def no_client(*args: Any, **kwargs: Any) -> None:
        raise AssertionError("the client is built on the first call only")

    monkeypatch.setattr(anthropic, "AnthropicBedrock", no_client)
    prov = get_provider(bedrock_cfg(aws_profile="dev", endpoint_url=VPC_ENDPOINT + "/"))
    assert isinstance(prov, BedrockProvider)
    assert (prov.name, prov.model, prov.region, prov.profile) == ("bedrock", MODEL, REGION, "dev")
    assert prov.endpoint_url == VPC_ENDPOINT
    assert (prov.fallbacks, prov.structured_outputs, prov._client) == (False, True, None)
    monkeypatch.setenv("HEALER_LLM_PROVIDER", "Bedrock")
    assert isinstance(get_provider(HealerConfig()), BedrockProvider)


def test_settings_come_from_the_config(aws_env: AwsFiles) -> None:
    prov = BedrockProvider(bedrock_cfg(model=f"  {MODEL} ", effort="medium", max_tokens=4000))
    assert (prov.model, prov.effort, prov.max_tokens) == (MODEL, "medium", 4000)
    assert (prov.profile, prov.endpoint_url) == (None, None)
    bare = BedrockProvider()
    assert (bare.region, bare.profile, bare.fallbacks) == (None, None, False)


@pytest.mark.parametrize(
    ("environ", "configured", "expected"),
    [
        ({"AWS_REGION": "us-east-1", "AWS_DEFAULT_REGION": "us-west-2"}, "eu-central-1", "eu-central-1"),
        ({"AWS_REGION": " us-east-1 ", "AWS_DEFAULT_REGION": "us-west-2"}, None, "us-east-1"),
        ({"AWS_REGION": "", "AWS_DEFAULT_REGION": "us-west-2"}, None, "us-west-2"),
        ({"AWS_REGION": " ", "AWS_DEFAULT_REGION": ""}, "", None),
    ],
    ids=["llm", "aws-region", "default-region", "blank"],
)
def test_configured_region(environ: dict[str, str], configured: str | None, expected: str | None) -> None:
    llm = type("Llm", (), {"aws_region": configured})()
    assert configured_region(llm, environ) == expected
    assert configured_region(None, {}) is None


@pytest.mark.parametrize(
    "model",
    [
        "anthropic.claude-opus-5-5",
        MODEL,
        "global.anthropic.claude-opus-5-5",
        "arn:aws:bedrock:eu-west-1:123456789012:application-inference-profile/a1b2c3d4e5f6",
    ],
    ids=["model-id", "geo", "global", "arn"],
)
def test_bedrock_model_ids_pass_the_model_check(model: str) -> None:
    assert model_alias_problem(model) is None


@pytest.mark.parametrize(
    ("model", "shown"),
    [("claude-opus-5-5", "claude-opus-5-5"), (" Claude-Sonnet-5-5", "Claude-Sonnet-5-5"), ("", "(empty)")],
    ids=["default", "sonnet", "empty"],
)
def test_claude_api_model_ids_fail_the_model_check(model: str, shown: str) -> None:
    problem = model_alias_problem(model)
    assert problem is not None
    assert problem.startswith(f"llm.model {shown} is a Claude API model ID;")
    assert "eu.anthropic.claude-opus-5-5" in problem and "docs/bedrock.md" in problem
    assert len(problem) < 300
    assert model_alias_problem(None) is not None


def test_missing_aws_sdk_finds_the_installed_packages() -> None:
    pytest.importorskip("boto3")
    assert missing_aws_sdk() is False
    assert bedrock._importable("dag_healer_test_no_such_package") is False


def test_the_provider_installs_the_shared_botocore_filter_before_any_client(
    aws_keys: AwsFiles, monkeypatch: pytest.MonkeyPatch
) -> None:
    """The filter of the S3 and Step Functions clients, with its signing records (one rule for
    every botocore logger), installed before the injected-client return as well."""
    for name in s3.BOTOCORE_LOGGERS:
        target = logging.getLogger(name)
        monkeypatch.setattr(
            target, "filters", [f for f in target.filters if not isinstance(f, s3.PayloadLogFilter)]
        )
    api = FakeBedrock(answer([text(CLASSIFICATION_JSON)]))
    BedrockProvider(bedrock_cfg(), client=api.client()).classify(system="S", prompt="P")
    for name in s3.BOTOCORE_LOGGERS:
        assert sum(isinstance(item, s3.PayloadLogFilter) for item in logging.getLogger(name).filters) == 1


def test_the_isolation_clears_an_exported_shell(exported_shell: None, aws_keys: AwsFiles) -> None:
    """No AWS setting of the shell is left (an expired AWS_CREDENTIAL_EXPIRATION would make every
    signature fail), and the test's own keys sign."""
    own = {"AWS_EC2_METADATA_DISABLED", "AWS_CONFIG_FILE", "AWS_SHARED_CREDENTIALS_FILE", "BOTO_CONFIG"}
    own |= {"AWS_ACCESS_KEY_ID", "AWS_SECRET_ACCESS_KEY", "AWS_SESSION_TOKEN"}  # set again by aws_keys
    left = [
        name for name in os.environ if name.upper().startswith(("AWS_", "BOTO_")) and name.upper() not in own
    ]
    assert left == []
    api = FakeBedrock(answer([text(CLASSIFICATION_JSON)]))
    provider(api).classify(system="S", prompt="P")
    assert_sigv4(api.requests[0])


def test_no_test_sees_the_aws_settings_of_the_machine() -> None:
    """``no_machine_aws`` of tests/conftest.py, with no fixture requested: whatever the shell that
    runs pytest exports (AWS's example keys of a moto setup, a profile, a region, and the
    provider and the model that docs/bedrock.md has a worker's shell export), only the
    isolation's own settings are left, and the files they name do not exist."""
    files = ("AWS_CONFIG_FILE", "AWS_SHARED_CREDENTIALS_FILE", "BOTO_CONFIG")
    prefixes = ("AWS_", "BOTO_", "ANTHROPIC_BEDROCK_", "ANTHROPIC_AWS_")
    left = {name.upper(): value for name, value in os.environ.items() if name.upper().startswith(prefixes)}
    assert sorted(left) == sorted(["AWS_EC2_METADATA_DISABLED", *files])
    assert left["AWS_EC2_METADATA_DISABLED"] == "true"
    assert [name for name in files if Path(left[name]).exists()] == []
    assert [name for name in ("HEALER_LLM_PROVIDER", "HEALER_LLM_MODEL") if name in os.environ] == []


def test_only_the_tests_of_real_services_keep_the_aws_settings_of_the_machine() -> None:
    """The live test (tests/live) and the end-to-end test of the Docker stack (tests/e2e) use the
    usual AWS chain and the shell's AWS settings: the isolation leaves their tests alone."""
    spec = importlib.util.spec_from_file_location("tests_conftest", TESTS / "conftest.py")
    assert spec is not None and spec.loader is not None
    conftest = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(conftest)
    assert conftest._uses_machine_settings(TESTS / "live" / "test_bedrock_live.py")
    assert conftest._uses_machine_settings(TESTS / "e2e" / "test_stack_e2e.py")
    for isolated in (Path(__file__), TESTS / "airflow" / "test_plugin.py", TESTS / "conftest.py"):
        assert not conftest._uses_machine_settings(isolated), isolated


def test_importing_the_providers_loads_neither_the_sdk_nor_boto3() -> None:
    """The aws extra is optional and the SDK is imported on the first call only."""
    code = (
        "import sys, dag_healer.agent.llm, dag_healer.agent.bedrock\n"
        "print(sorted({name.split('.')[0] for name in sys.modules} & {'anthropic', 'boto3', 'botocore'}))\n"
    )
    path = os.pathsep.join([str(SRC), os.environ.get("PYTHONPATH", "")]).rstrip(os.pathsep)
    env = {**os.environ, "PYTHONPATH": path}
    proc = subprocess.run(
        [sys.executable, "-c", code], capture_output=True, text=True, env=env, timeout=120, check=False
    )
    assert proc.returncode == 0, proc.stderr
    assert proc.stdout.strip() == "[]"


# --------------------------------------------------------------------------- classify: the request


def test_classify_request_shape(aws_keys: AwsFiles) -> None:
    api = FakeBedrock(
        answer(
            [thinking(), text(CLASSIFICATION_JSON)],
            usage={
                "input_tokens": 1200,
                "output_tokens": 150,
                "cache_creation_input_tokens": 900,
                "cache_read_input_tokens": 0,
            },
        )
    )
    result, tokens = provider(api).classify(system="SYSTEM PROMPT", prompt="PROMPT <signals>{}</signals>")
    assert isinstance(result, Classification)
    assert (result.category, result.confidence, result.suspected_symbol) == (
        "code",
        0.88,
        "parse_signup_date",
    )
    assert tokens == 1200 + 150 + 900
    assert len(api.requests) == 1
    assert_sigv4(api.requests[0])
    assert api.requests[0].headers["x-amz-security-token"] == SESSION_TOKEN
    body = assert_bedrock_request(api)
    assert body["system"] == [
        {"type": "text", "text": "SYSTEM PROMPT", "cache_control": {"type": "ephemeral"}}
    ]
    assert body["messages"] == [{"role": "user", "content": "PROMPT <signals>{}</signals>"}]
    assert body["output_config"]["format"] == structured_format()
    assert body["output_config"]["format"]["schema"]["additionalProperties"] is False


ABSENT = object()


@pytest.mark.parametrize(
    ("written", "warned"),
    [(ABSENT, False), (True, True), (False, False), (None, False), ("${HEALER_TEST_UNSET}", False)],
    ids=["default", "true", "false", "empty", "unset-var"],
)
def test_fallbacks_are_never_sent_and_warn_only_when_written(
    aws_keys: AwsFiles,
    caplog: pytest.LogCaptureFixture,
    monkeypatch: pytest.MonkeyPatch,
    written: Any,
    warned: bool,
) -> None:
    monkeypatch.delenv("HEALER_TEST_UNSET", raising=False)
    caplog.set_level(logging.WARNING, logger="dag_healer.agent.bedrock")
    llm: dict[str, Any] = {"provider": "bedrock", "model": MODEL, "aws_region": REGION}
    if written is not ABSENT:
        llm["fallbacks"] = written
    cfg = config_from_mapping({"llm": llm})
    api = FakeBedrock(answer([text(CLASSIFICATION_JSON)]))
    prov = provider(api, cfg)
    assert prov.fallbacks is False
    assert (FALLBACKS_IGNORED in caplog.messages) is warned
    prov.classify(system="S", prompt="P")
    assert_bedrock_request(api)


@pytest.mark.parametrize("model", [None, "claude-sonnet-5-5"], ids=["default", "sonnet"])
def test_claude_api_model_ids_are_refused_before_any_request(
    aws_keys: AwsFiles, caplog: pytest.LogCaptureFixture, model: str | None
) -> None:
    caplog.set_level(logging.WARNING, logger="dag_healer.agent.bedrock")
    cfg = config_from_mapping({"llm": {"provider": "bedrock", "aws_region": REGION, "model": model}})
    shown = model or "claude-opus-5-5"
    api = FakeBedrock()
    prov = provider(api, cfg)
    assert any(f"llm.model {shown} is a Claude API model ID" in item for item in caplog.messages)
    with pytest.raises(LLMUnavailable, match=f"llm.model {shown} is a Claude API model ID"):
        prov.classify(system="S", prompt="P")
    with pytest.raises(LLMUnavailable, match="Claude API model ID"):
        prov.repair_session(system="S", prompt="P", tools=[], max_tokens_budget=10, scenario={})
    injected = BedrockProvider(cfg, client=api.client())
    with pytest.raises(LLMUnavailable, match="Claude API model ID"):
        injected.classify(system="S", prompt="P")
    assert api.requests == []


def test_too_large_max_tokens_is_unavailable(aws_keys: AwsFiles) -> None:
    api = FakeBedrock()
    with pytest.raises(LLMUnavailable, match="max_tokens is too large"):
        provider(api, max_tokens=64_000).classify(system="S", prompt="P")
    assert api.requests == []


# --------------------------------------------------------------------------- region, endpoint, credentials


@pytest.mark.parametrize(
    ("configured", "environ"),
    [
        ("eu-central-1", {"AWS_REGION": "us-east-1", "AWS_DEFAULT_REGION": "us-west-2"}),
        (None, {"AWS_REGION": "eu-central-1", "AWS_DEFAULT_REGION": "us-west-2"}),
        (None, {"AWS_REGION": "", "AWS_DEFAULT_REGION": "eu-central-1"}),
    ],
    ids=["llm", "aws-region", "default-region"],
)
def test_the_region_order(
    aws_keys: AwsFiles, monkeypatch: pytest.MonkeyPatch, configured: str | None, environ: dict[str, str]
) -> None:
    for name, value in environ.items():
        monkeypatch.setenv(name, value)
    api = FakeBedrock(answer([text(CLASSIFICATION_JSON)]))
    provider(api, bedrock_cfg(aws_region=configured)).classify(system="S", prompt="P")
    assert api.requests[0].url.host == "bedrock-runtime.eu-central-1.amazonaws.com"
    assert_sigv4(api.requests[0], "eu-central-1")


def test_the_profile_gives_the_region_and_the_credentials(aws_sdk: AwsFiles) -> None:
    aws_sdk.write(**DEV_PROFILE)
    api = FakeBedrock(answer([text(CLASSIFICATION_JSON)]))
    prov = provider(api, bedrock_cfg(aws_region=None, aws_profile="dev"))
    prov.classify(system="S", prompt="P")
    assert api.requests[0].url.host == "bedrock-runtime.eu-central-1.amazonaws.com"
    assert_sigv4(api.requests[0], "eu-central-1")
    assert "x-amz-security-token" not in api.requests[0].headers  # long-term profile keys


def test_no_region_is_unavailable(aws_keys: AwsFiles) -> None:
    api = FakeBedrock()
    with pytest.raises(LLMUnavailable) as info:
        provider(api, bedrock_cfg(aws_region=None)).classify(system="S", prompt="P")
    assert str(info.value) == NO_REGION
    assert api.requests == []


def test_the_endpoint_override_keeps_the_signing_region(aws_keys: AwsFiles) -> None:
    api = FakeBedrock(answer([text(CLASSIFICATION_JSON)]))
    provider(api, bedrock_cfg(endpoint_url=VPC_ENDPOINT)).classify(system="S", prompt="P")
    request = api.requests[0]
    assert str(request.url) == VPC_ENDPOINT + INVOKE_PATH
    assert_sigv4(request)


def test_an_inference_profile_arn_goes_into_the_path(aws_keys: AwsFiles) -> None:
    arn = "arn:aws:bedrock:eu-west-1:123456789012:application-inference-profile/a1b2c3d4e5f6"
    api = FakeBedrock(answer([text(CLASSIFICATION_JSON)]))
    provider(api, bedrock_cfg(model=arn)).classify(system="S", prompt="P")
    request = api.requests[0]
    assert str(request.url) == (
        f"https://{HOST}/model/arn:aws:bedrock:eu-west-1:123456789012:application-inference-profile"
        "%2Fa1b2c3d4e5f6/invoke"
    )
    assert "model" not in api.body()
    assert_sigv4(request)


def test_the_sdk_base_url_variable_is_honoured(aws_keys: AwsFiles, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("ANTHROPIC_BEDROCK_BASE_URL", "https://bedrock.gateway.example.com")
    api = FakeBedrock(answer([text(CLASSIFICATION_JSON)]))
    provider(api).classify(system="S", prompt="P")
    assert api.requests[0].url.host == "bedrock.gateway.example.com"


def test_an_empty_sdk_base_url_is_refused_unless_the_endpoint_is_set(
    aws_keys: AwsFiles, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setenv("ANTHROPIC_BEDROCK_BASE_URL", "")
    api = FakeBedrock(answer([text(CLASSIFICATION_JSON)]))
    with pytest.raises(LLMUnavailable) as info:
        provider(api).classify(system="S", prompt="P")
    assert str(info.value) == BASE_URL_EMPTY
    assert api.requests == []
    provider(api, bedrock_cfg(endpoint_url=VPC_ENDPOINT)).classify(system="S", prompt="P")
    assert str(api.requests[0].url).startswith(VPC_ENDPOINT)


def test_a_bedrock_api_key_replaces_sigv4(aws_sdk: AwsFiles, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("AWS_BEARER_TOKEN_BEDROCK", API_KEY)
    api = FakeBedrock(answer([text(CLASSIFICATION_JSON)]))
    provider(api).classify(system="S", prompt="P")
    request = api.requests[0]
    assert request.headers["authorization"] == f"Bearer {API_KEY}"
    assert "x-amz-date" not in request.headers
    assert_bedrock_request(api)


def test_an_empty_api_key_counts_as_unset(aws_keys: AwsFiles, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("AWS_BEARER_TOKEN_BEDROCK", "")
    api = FakeBedrock(answer([text(CLASSIFICATION_JSON)]))
    provider(api).classify(system="S", prompt="P")
    assert_sigv4(api.requests[0])


def test_an_empty_api_key_next_to_a_profile(aws_sdk: AwsFiles, monkeypatch: pytest.MonkeyPatch) -> None:
    aws_sdk.write(**DEV_PROFILE)
    monkeypatch.setenv("AWS_BEARER_TOKEN_BEDROCK", "")
    api = FakeBedrock(answer([text(CLASSIFICATION_JSON)]))
    provider(api, bedrock_cfg(aws_profile="dev")).classify(system="S", prompt="P")
    assert_sigv4(api.requests[0])  # the profile's keys, the configured region
    with pytest.raises(LLMUnavailable) as info:
        provider(api, bedrock_cfg(aws_profile="dev", aws_region=None)).classify(system="S", prompt="P")
    assert str(info.value) == EMPTY_API_KEY_WITH_PROFILE
    assert len(api.requests) == 1


def test_a_profile_and_an_api_key_are_refused(aws_sdk: AwsFiles, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("AWS_BEARER_TOKEN_BEDROCK", API_KEY)
    api = FakeBedrock()
    with pytest.raises(LLMUnavailable) as info:
        provider(api, bedrock_cfg(aws_profile="dev")).classify(system="S", prompt="P")
    assert str(info.value) == PROFILE_AND_API_KEY
    assert api.requests == []


def test_no_credentials_is_unavailable_before_any_request(aws_sdk: AwsFiles) -> None:
    api = FakeBedrock()
    prov = provider(api)
    with pytest.raises(LLMUnavailable) as info:
        prov.classify(system="S", prompt="P")
    assert str(info.value) == NO_CREDENTIALS
    with pytest.raises(LLMUnavailable) as repair:
        prov.repair_session(
            system="S", prompt="P", tools=[], max_tokens_budget=10, scenario={"phase": "test"}
        )
    assert str(repair.value) == NO_CREDENTIALS
    assert repair.value.partial is not None and repair.value.partial.tokens_used == 0
    assert api.requests == []


def test_the_aws_extra_is_named_when_it_is_missing(
    aws_env: AwsFiles, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setattr(bedrock, "missing_aws_sdk", lambda: True)
    with pytest.raises(LLMUnavailable) as info:
        BedrockProvider(bedrock_cfg()).classify(system="S", prompt="P")
    assert str(info.value) == AWS_SDK_MISSING
    assert 'pip install "dag-healer[aws]"' in AWS_SDK_MISSING


@pytest.mark.parametrize(
    ("region", "pattern"),
    [
        (REGION, "cannot be resolved during the incident classification: ProfileNotFound: "),
        (None, "the Amazon Bedrock client cannot be built: ProfileNotFound: "),
    ],
    ids=["at-the-call", "at-the-build"],
)
def test_an_unknown_profile_is_named(aws_sdk: AwsFiles, region: str | None, pattern: str) -> None:
    api = FakeBedrock()
    with pytest.raises(LLMUnavailable, match=pattern) as info:
        provider(api, bedrock_cfg(aws_region=region, aws_profile="nope")).classify(system="S", prompt="P")
    assert "nope" in str(info.value)
    assert api.requests == []


# --------------------------------------------------------------------------- classify: errors


@pytest.mark.parametrize(
    ("status", "message", "expected"),
    [
        (
            401,
            "The model is not available to this caller.",
            "Amazon Bedrock rejected the AWS credentials during the incident classification (401): "
            "check the worker's AWS credentials; The model is not available to this caller.",
        ),
        (
            403,
            "You don't have access to the model with the specified model ID.",
            "Amazon Bedrock refused the incident classification (403): You don't have access to the model "
            "with the specified model ID.; check the worker's AWS credentials, its permission "
            f"bedrock:InvokeModel on {MODEL} and the model access (docs/bedrock.md)",
        ),
        (
            404,
            "Model use case details have not been submitted for this account.",
            f"Amazon Bedrock returned 404 during the incident classification: check llm.model ({MODEL}) and "
            "the region (eu-west-1); Model use case details have not been submitted for this account.",
        ),
        (
            429,
            "Too many requests, please wait before trying again.",
            "Amazon Bedrock throttled the incident classification (429); retry later or raise the quota of "
            f"{MODEL} in eu-west-1",
        ),
        (
            500,
            "The server encountered an error processing your request.",
            "Amazon Bedrock error 500 during the incident classification: The server encountered an error "
            "processing your request.",
        ),
        (
            400,
            "Invocation of model ID anthropic.claude-opus-5-5 with on-demand throughput isn't supported. "
            "Retry your request with the ID or ARN of an inference profile that contains this model.",
            "(use an inference profile ID in llm.model, such as eu.anthropic.claude-opus-5-5: "
            "docs/bedrock.md)",
        ),
        (
            400,
            "anthropic_version: the value is not supported by this model",
            "(the model does not accept the API version of the installed anthropic SDK: use Claude Opus 4.7 "
            "or later, docs/bedrock.md)",
        ),
        (
            400,
            "The provided model identifier is invalid.",
            f"(check that llm.model ({MODEL}) is a Bedrock model or inference profile ID available in "
            "eu-west-1: docs/bedrock.md)",
        ),
    ],
    ids=["401", "403", "404", "429", "500", "400-profile", "400-version", "400-model-id"],
)
def test_bedrock_errors_say_what_to_fix(aws_keys: AwsFiles, status: int, message: str, expected: str) -> None:
    api = FakeBedrock(aws_error(status, message))
    prov = BedrockProvider(bedrock_cfg(), client=api.client())
    with pytest.raises(LLMUnavailable) as info:
        prov.classify(system="S", prompt="P")
    if expected.startswith("("):  # a hint after AWS's own message
        expected = f"Amazon Bedrock error 400 during the incident classification: {message} {expected}"
    assert str(info.value) == expected
    assert prov.structured_outputs is True
    assert len(api.requests) == 1


def test_aws_messages_about_the_credentials_give_way_to_the_error_type(aws_keys: AwsFiles) -> None:
    """AWS's own wording about tokens and signatures holds no value, but an echo of the request
    would: such a message is never shown, the answer's error type stands in for it."""
    api = FakeBedrock(
        aws_error(
            401, "The security token included in the request is invalid.", "UnrecognizedClientException"
        ),
        aws_error(403, "The request signature we calculated does not match the signature you provided."),
        (500, {"Output": {"__type": "com.amazon.coral.service#InternalFailure"}}),
        (500, {"detail": "an answer without a message"}, {"x-amzn-errortype": "InternalFailure:http://x/"}),
    )
    prov = BedrockProvider(bedrock_cfg(), client=api.client())
    texts = []
    for _ in range(4):
        with pytest.raises(LLMUnavailable) as info:
            prov.classify(system="S", prompt="P")
        texts.append(str(info.value))
    assert texts == [
        "Amazon Bedrock rejected the AWS credentials during the incident classification (401): check the "
        f"worker's AWS credentials; UnrecognizedClientException ({MESSAGE_LEFT_OUT})",
        f"Amazon Bedrock refused the incident classification (403): {MESSAGE_LEFT_OUT}; check the worker's "
        "AWS "
        f"credentials, its permission bedrock:InvokeModel on {MODEL} and the model access (docs/bedrock.md)",
        "Amazon Bedrock error 500 during the incident classification",  # the SDK's text quotes the body
        "Amazon Bedrock error 500 during the incident classification: InternalFailure",
    ]


def test_connection_errors_name_the_host(aws_keys: AwsFiles) -> None:
    api = FakeBedrock(httpx2.ConnectError("connection refused"))
    with pytest.raises(LLMUnavailable) as info:
        BedrockProvider(bedrock_cfg(), client=api.client()).classify(system="S", prompt="P")
    assert str(info.value) == (
        f"Cannot reach Amazon Bedrock ({HOST}) during the incident classification: APIConnectionError"
    )


def test_errors_raised_before_sending_are_translated() -> None:
    def raised(error: Exception) -> str:
        with pytest.raises(LLMUnavailable) as info, bedrock_errors("the test", model=MODEL, region=REGION):
            raise error
        return str(info.value)

    botocore_error = {"__module__": "botocore.exceptions"}
    no_credentials = type("NoCredentialsError", (Exception,), botocore_error)
    sso_expired = type("SSOTokenLoadError", (Exception,), botocore_error)
    assert raised(RuntimeError("could not resolve credentials from session")) == NO_CREDENTIALS
    assert raised(no_credentials("Unable to locate credentials")) == NO_CREDENTIALS
    assert raised(sso_expired("Error loading SSO Token: the token has expired")) == (
        "the AWS credentials for the bedrock provider cannot be resolved during the test: SSOTokenLoadError: "
        "Error loading SSO Token: the token has expired"
    )
    # botocore's own RuntimeError: credentials found, but expired (not "no credentials")
    expired = "Credentials were refreshed, but the refreshed credentials are still expired."
    assert raised(RuntimeError(expired)) == (
        "the AWS credentials for the bedrock provider cannot be resolved during the test: "
        f"RuntimeError: {expired}"
    )
    assert raised(ModuleNotFoundError("No module named 'botocore'", name="botocore")) == AWS_SDK_MISSING
    for unrelated in (
        KeyError("x"),
        RuntimeError("boom"),
        ValueError("bad"),
        ModuleNotFoundError(name="yaml"),
    ):
        with pytest.raises(type(unrelated)), bedrock_errors("the test", model=MODEL, region=REGION):
            raise unrelated


# --------------------------------------------------------------------------- classify: degraded paths


def test_a_rejected_structured_output_switches_to_json_answers(
    aws_keys: AwsFiles, caplog: pytest.LogCaptureFixture
) -> None:
    caplog.set_level(logging.WARNING, logger="dag_healer.agent.bedrock")
    api = FakeBedrock(
        aws_error(400, "output_config.format: structured outputs are not supported for this model"),
        answer(
            [thinking(), text(f"```json\n{CLASSIFICATION_JSON}\n```")],
            usage={"input_tokens": 1500, "output_tokens": 200, "cache_read_input_tokens": 1000},
        ),
        answer([text(CLASSIFICATION_JSON)], usage={"input_tokens": 1500, "output_tokens": 100}),
    )
    prov = provider(api)
    result, tokens = prov.classify(system="SYSTEM", prompt="PROMPT")
    assert result.category == "code"
    assert tokens == 2700
    assert prov.structured_outputs is False
    assert any("asking eu.anthropic.claude-opus-5-5 for a JSON answer" in item for item in caplog.messages)
    structured = assert_bedrock_request(api, 0)
    assert structured["output_config"]["format"] == structured_format()
    degraded = assert_bedrock_request(api, 1)
    assert degraded["output_config"] == {"effort": "high"}
    assert degraded["system"] == [
        {"type": "text", "text": "SYSTEM", "cache_control": {"type": "ephemeral"}},
        json_instruction(),
    ]
    assert degraded["messages"] == [{"role": "user", "content": "PROMPT"}]

    # Remembered: the next classification asks for JSON at once.
    assert prov.classify(system="SYSTEM", prompt="PROMPT")[1] == 1600
    assert api.body(2) == degraded
    assert len(api.requests) == 3


def test_an_answer_that_ignores_the_structured_output_switches_to_json_answers(aws_keys: AwsFiles) -> None:
    api = FakeBedrock(
        answer([text("This is a code defect in parse_signup_date.")]),
        answer([text(CLASSIFICATION_JSON)]),
    )
    prov = provider(api)
    result, tokens = prov.classify(system="S", prompt="P")
    assert (result.category, tokens) == ("code", 240)
    assert prov.structured_outputs is False
    assert "format" in api.body(0)["output_config"]
    assert "format" not in api.body(1)["output_config"]
    assert api.body(1)["system"][1] == json_instruction()


def test_the_json_retry_is_append_only(aws_keys: AwsFiles) -> None:
    api = FakeBedrock(
        answer([thinking(), text('Here it is: {"category": "maybe", "confidence": 0.5}')]),
        answer([thinking(), text(CLASSIFICATION_JSON)]),
    )
    prov = provider(api)
    prov.structured_outputs = False
    result, tokens = prov.classify(system="S", prompt="P")
    assert (result.category, tokens) == ("code", 240)
    first, retry = api.body(0), api.body(1)
    assert retry["system"] == first["system"]
    assert retry["messages"][:1] == first["messages"]
    assert retry["messages"][1] == {
        "role": "assistant",
        "content": [thinking(), text('Here it is: {"category": "maybe", "confidence": 0.5}')],
    }
    instruction = retry["messages"][2]
    assert instruction["role"] == "user"
    problem = "category: Input should be 'data', 'code' or 'ambiguous'; diagnosis: Field required"
    assert instruction["content"] == JSON_RETRY_INSTRUCTION.format(problem=problem)
    assert len(retry["messages"]) == 3


def test_expired_credentials_are_named_as_expired(
    aws_keys: AwsFiles, monkeypatch: pytest.MonkeyPatch
) -> None:
    """What stays in a shell once credentials exported with ``aws configure export-credentials
    --format env`` expire: the error says so, and does not ask for credentials that exist."""
    monkeypatch.setenv("AWS_CREDENTIAL_EXPIRATION", "2020-01-01T00:00:00Z")
    api = FakeBedrock()
    with pytest.raises(LLMUnavailable) as info:
        provider(api).classify(system="S", prompt="P")
    assert str(info.value) == (
        "the AWS credentials for the bedrock provider cannot be resolved during the incident classification: "
        "RuntimeError: Credentials were refreshed, but the refreshed credentials are still expired."
    )
    assert api.requests == []


def test_a_second_invalid_json_answer_is_unavailable(aws_keys: AwsFiles) -> None:
    api = FakeBedrock(answer([text("No JSON here.")]), answer([text("{still not json")]))
    prov = provider(api)
    prov.structured_outputs = False
    with pytest.raises(LLMUnavailable) as info:
        prov.classify(system="S", prompt="P")
    assert str(info.value) == JSON_ANSWER_INVALID
    assert api.body(1)["messages"][-1]["content"] == JSON_RETRY_INSTRUCTION.format(
        problem="no JSON object in the answer"
    )
    assert len(api.requests) == 2


@pytest.mark.parametrize("structured", [True, False], ids=["structured", "json"])
@pytest.mark.parametrize(
    ("reply", "error", "pattern"),
    [
        (
            answer(
                [],
                stop_reason="refusal",
                stop_details={"type": "refusal", "category": "cyber", "explanation": "declined"},
            ),
            LLMRefusal,
            "refusal category: cyber",
        ),
        (
            answer([text('{"category": "code", "confid')], stop_reason="max_tokens"),
            LLMUnavailable,
            "max_tokens",
        ),
    ],
    ids=["refusal", "max-tokens"],
)
def test_refusals_and_cut_answers_are_not_retried(
    aws_keys: AwsFiles, structured: bool, reply: dict[str, Any], error: type[Exception], pattern: str
) -> None:
    api = FakeBedrock(reply)
    prov = provider(api)
    prov.structured_outputs = structured
    with pytest.raises(error, match=pattern):
        prov.classify(system="S", prompt="P")
    assert prov.structured_outputs is structured
    assert len(api.requests) == 1


@pytest.mark.parametrize(
    ("message", "rejected"),
    [
        ("output_config.format: Extra inputs are not permitted", True),
        (
            "Malformed input request: #/output_config: extraneous key [format] is not permitted, please "
            "reformat your input and try again.",
            True,
        ),
        ("output_config.format.json_schema: unsupported schema keyword 'anyOf'", True),
        ("Structured outputs are not supported for this model.", True),
        (
            "Malformed input request: #/output_config: extraneous key [effort] is not permitted, please "
            "reformat your input and try again.",
            False,
        ),
        ("The provided model identifier is invalid.", False),
    ],
    ids=["dotted", "extraneous-format", "schema", "named", "extraneous-effort", "model-id"],
)
def test_which_400s_reject_the_structured_output(aws_keys: AwsFiles, message: str, rejected: bool) -> None:
    api = FakeBedrock(aws_error(400, message), answer([text(CLASSIFICATION_JSON)]))
    prov = BedrockProvider(bedrock_cfg(), client=api.client())
    if rejected:
        assert prov.classify(system="S", prompt="P")[0].category == "code"
        assert "format" not in api.body(1)["output_config"]
    else:
        with pytest.raises(
            LLMUnavailable, match="Amazon Bedrock error 400 during the incident classification"
        ):
            prov.classify(system="S", prompt="P")
        assert len(api.requests) == 1
    assert prov.structured_outputs is not rejected


def test_other_400s_keep_their_error_and_the_structured_output(aws_keys: AwsFiles) -> None:
    api = FakeBedrock(aws_error(400, "output_config.effort: Input should be 'low', 'medium' or 'high'"))
    prov = BedrockProvider(bedrock_cfg(), client=api.client())
    with pytest.raises(LLMUnavailable, match="Amazon Bedrock error 400 during the incident classification"):
        prov.classify(system="S", prompt="P")
    assert prov.structured_outputs is True


# --------------------------------------------------------------------------- repair session


def make_tools(calls: list[str]) -> list[Callable[..., str]]:
    def read_file(path: str) -> str:
        """Read a file of the repository.

        Args:
            path: Repository-relative path.
        """
        calls.append(f"read_file {path}")
        return f"contents of {path}"

    def run_tests(paths: list[str] | None = None) -> str:
        """Run pytest.

        Args:
            paths: Test files to run (all tests when omitted).
        """
        calls.append("run_tests")
        return "1 failed, 1 passed"

    return [read_file, run_tests]


def tool_use(tool_id: str, name: str, tool_input: dict[str, Any]) -> dict[str, Any]:
    return {"type": "tool_use", "id": tool_id, "name": name, "input": tool_input}


def test_a_repair_session_over_bedrock(aws_keys: AwsFiles) -> None:
    api = FakeBedrock(
        answer(
            [
                thinking(),
                text("Let me read the module and run the tests."),
                tool_use("toolu_bdrk_1", "read_file", {"path": "src/etl/customers.py"}),
                tool_use("toolu_bdrk_2", "run_tests", {"paths": ["tests/test_customers.py"]}),
            ],
            stop_reason="tool_use",
            usage={"input_tokens": 1000, "output_tokens": 100, "cache_read_input_tokens": 500},
        ),
        answer(
            [thinking(), text("Done: the failing test is written.")],
            usage={"input_tokens": 1500, "output_tokens": 50},
        ),
    )
    calls: list[str] = []
    result = provider(api).repair_session(
        system="REPAIR SYSTEM",
        prompt="Write a failing test",
        tools=make_tools(calls),
        max_tokens_budget=100_000,
        scenario={"phase": "test", "dag_id": "customers_sync", "task_id": "transform"},
    )
    assert isinstance(result, RepairSessionResult)
    assert calls == ["read_file src/etl/customers.py", "run_tests"]
    assert (result.final_text, result.stop_reason) == ("Done: the failing test is written.", "end_turn")
    assert (result.input_tokens, result.output_tokens, result.budget_exhausted) == (3000, 150, False)
    assert [(record.name, record.is_error) for record in result.tool_calls] == [
        ("read_file", False),
        ("run_tests", False),
    ]
    assert result.tool_calls[0].output_excerpt == "contents of src/etl/customers.py"

    assert len(api.requests) == 2
    first, followup = assert_bedrock_request(api, 0), assert_bedrock_request(api, 1)
    for request in api.requests:
        assert_sigv4(request)
    assert "format" not in first["output_config"]
    assert [tool["name"] for tool in first["tools"]] == ["read_file", "run_tests"]
    assert followup["tools"] == first["tools"]  # the whole conversation is resent every turn
    assert followup["system"] == first["system"]
    assert [message["role"] for message in followup["messages"]] == ["user", "assistant", "user"]
    assert followup["messages"][1]["content"][0] == thinking()
    results = {block["tool_use_id"]: block for block in followup["messages"][2]["content"]}
    assert results["toolu_bdrk_1"]["content"] == "contents of src/etl/customers.py"


def test_a_repair_session_error_keeps_the_partial_result(aws_keys: AwsFiles) -> None:
    api = FakeBedrock(
        answer([tool_use("toolu_bdrk_1", "read_file", {"path": "a.py"})], stop_reason="tool_use"),
        aws_error(403, "You don't have access to the model with the specified model ID."),
    )
    with pytest.raises(LLMUnavailable, match=r"refused the repair session \(fix\) \(403\)") as info:
        BedrockProvider(bedrock_cfg(), client=api.client()).repair_session(
            system="S", prompt="P", tools=make_tools([]), max_tokens_budget=10_000, scenario={"phase": "fix"}
        )
    partial = info.value.partial
    assert partial is not None
    assert [record.name for record in partial.tool_calls] == ["read_file"]
    assert partial.tokens_used == 120


# --------------------------------------------------------------------------- secrets


def test_signing_logs_are_quiet_at_debug(aws_keys: AwsFiles, caplog: pytest.LogCaptureFixture) -> None:
    caplog.set_level(logging.DEBUG)
    api = FakeBedrock(answer([text(CLASSIFICATION_JSON)]))
    provider(api).classify(system="S", prompt="P")
    signing = [record for record in caplog.records if record.name == "botocore.auth"]
    assert any("v4 auth" in record.getMessage() for record in signing)  # botocore did log at DEBUG
    assert not any(str(record.msg).startswith(("CanonicalRequest", "StringToSign")) for record in signing)
    logged = "\n".join(record.getMessage() for record in caplog.records)
    for secret in SECRETS:
        assert secret not in logged


def test_no_credential_appears_in_an_error_a_log_or_a_result(
    aws_keys: AwsFiles, caplog: pytest.LogCaptureFixture, monkeypatch: pytest.MonkeyPatch
) -> None:
    caplog.set_level(logging.DEBUG)
    api = FakeBedrock(
        aws_error(403, f"Request from AKIAEXAMPLEKEYID0001 ({SECRET_KEY}) rejected"),
        aws_error(403, f"Request with {SESSION_TOKEN} rejected"),  # its fictional value names a token
        answer([text(CLASSIFICATION_JSON)]),
    )
    prov = provider(api)
    errors = []
    for _ in range(2):
        with pytest.raises(LLMUnavailable) as info:
            prov.classify(system="S", prompt="P")
        errors.append(str(info.value))
    result, _ = prov.classify(system="S", prompt="P")
    monkeypatch.setenv("AWS_BEARER_TOKEN_BEDROCK", API_KEY)
    bearer = FakeBedrock(aws_error(403, f"Invalid API key {API_KEY}"), aws_error(403, f"Unknown {API_KEY}"))
    bearer_prov = BedrockProvider(bedrock_cfg(), client=bearer.client())
    for _ in range(2):
        with pytest.raises(LLMUnavailable) as bearer_info:
            bearer_prov.classify(system="S", prompt="P")
        errors.append(str(bearer_info.value))
    assert any(record.name.startswith("anthropic") for record in caplog.records)  # the SDK's DEBUG logs
    for found in [*errors, result.model_dump_json(), caplog.text]:
        for secret in (*SECRETS, "AKIAEXAMPLEKEYID0001"):
            assert secret not in found
    assert "(403): Request from [hidden] ([hidden]) rejected;" in errors[0]
    assert f"(403): {MESSAGE_LEFT_OUT};" in errors[1] and f"(403): {MESSAGE_LEFT_OUT};" in errors[2]
    assert f"(403): {MESSAGE_LEFT_OUT};" in errors[3]  # the fictional key's value names an API key


def test_aws_signing_details_never_reach_a_message(aws_keys: AwsFiles) -> None:
    """AWS's answer to a signature that does not match can quote the canonical request, with the
    session token of temporary credentials that no variable holds (an instance role's)."""
    role_token = "IQoJb3JpZ2luX2VjEXAMPLEinstanceRoleSessionToken0123456789"
    mismatch = (
        "The request signature we calculated does not match the signature you provided. Check your AWS "
        "Secret Access Key and signing method. Consult the service documentation for details.\n\n"
        f"The Canonical String for this request should have been\n'POST\n{INVOKE_PATH}\n\nhost:{HOST}\n"
        f"x-amz-date:20261026T120000Z\nx-amz-security-token:{role_token}\n\n"
        f"host;x-amz-date;x-amz-security-token\n{'0' * 64}'\n\n"
        "The String-to-Sign should have been\n'AWS4-HMAC-SHA256\n20261026T120000Z\n"
        f"20261026/{REGION}/bedrock/aws4_request\n{'1' * 64}'\n"
    )
    api = FakeBedrock(
        aws_error(403, mismatch, "InvalidSignatureException"),
        aws_error(400, f"x-amz-security-token: {role_token} is bad"),
    )
    prov = BedrockProvider(bedrock_cfg(), client=api.client())
    with pytest.raises(LLMUnavailable) as signature:
        prov.classify(system="S", prompt="P")
    with pytest.raises(LLMUnavailable) as invalid:
        prov.classify(system="S", prompt="P")
    assert str(signature.value).startswith(
        "Amazon Bedrock refused the incident classification (403): "
        f"InvalidSignatureException ({MESSAGE_LEFT_OUT})"
    )
    assert (
        str(invalid.value)
        == f"Amazon Bedrock error 400 during the incident classification: {MESSAGE_LEFT_OUT}"
    )
    for error in (signature.value, invalid.value):
        assert leaked(str(error), role_token) == []
    # the same text through the shared helper: the quote of the signed request is cut off whole
    hidden = hide_credentials(mismatch)
    assert hidden.endswith("Consult the service documentation for details. [signing details hidden]")
    assert leaked(hidden, role_token) == []


# --------------------------------------------------------------------------- redirects

REDIRECT_TARGET = "https://collector.attacker.example.net/steal"


class Wire:
    """httpx2's network transport (``HTTPTransport.handle_request``), patched: every request the
    provider's own HTTP client sends is kept and answered with a redirect; nothing leaves the
    process."""

    def __init__(self, status: int, location: str) -> None:
        self.status, self.location = status, location
        self.requests: list[httpx2.Request] = []

    def handle_request(self, request: httpx2.Request) -> httpx2.Response:
        self.requests.append(request)
        return httpx2.Response(self.status, headers={"location": self.location}, request=request)


#: The proxy settings that the SDK's default HTTP client reads (cleared: the wire is direct).
PROXY_VARIABLES = ("HTTP_PROXY", "HTTPS_PROXY", "ALL_PROXY", "NO_PROXY")


@pytest.fixture
def wire(monkeypatch: pytest.MonkeyPatch) -> Callable[[int, str], Wire]:
    for name in PROXY_VARIABLES:
        monkeypatch.delenv(name, raising=False)
        monkeypatch.delenv(name.lower(), raising=False)

    def install(status: int, location: str) -> Wire:
        fake = Wire(status, location)
        monkeypatch.setattr(
            httpx2.HTTPTransport, "handle_request", lambda _transport, request: fake.handle_request(request)
        )
        return fake

    return install


@pytest.mark.parametrize("location", [REDIRECT_TARGET, "/model/other/invoke"], ids=["other-host", "relative"])
@pytest.mark.parametrize("status", [301, 302, 303, 307, 308])
def test_the_provider_never_follows_a_redirect(
    aws_keys: AwsFiles, wire: Callable[[int, str], Wire], status: int, location: str
) -> None:
    """The provider's own client (no ``http_client`` injected): one request, then LLMUnavailable;
    the signed request and its session token never reach another host, nor a second request the
    first one's prompt."""
    sent = wire(status, location)
    prov = BedrockProvider(bedrock_cfg())
    with pytest.raises(LLMUnavailable) as info:
        prov.classify(system="S", prompt="P")
    assert str(info.value) == REDIRECTED.format(action="the incident classification", status=status)
    assert [request.url.host for request in sent.requests] == [HOST]
    assert sent.requests[0].headers["x-amz-security-token"] == SESSION_TOKEN


def test_a_redirect_is_not_followed_with_an_api_key_or_in_a_repair(
    aws_sdk: AwsFiles, wire: Callable[[int, str], Wire], monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setenv("AWS_BEARER_TOKEN_BEDROCK", API_KEY)
    sent = wire(307, REDIRECT_TARGET)
    prov = BedrockProvider(bedrock_cfg())
    with pytest.raises(LLMUnavailable, match=r"with a redirect \(HTTP 307\)"):
        prov.classify(system="S", prompt="P")
    with pytest.raises(LLMUnavailable, match=r"answered the repair session \(fix\) with a redirect") as info:
        prov.repair_session(system="S", prompt="P", tools=[], max_tokens_budget=10, scenario={"phase": "fix"})
    assert info.value.partial is not None
    assert [request.url.host for request in sent.requests] == [HOST, HOST]


# --------------------------------------------------------------------------- credentials in messages

#: A credential of the profile echoed back by an answer, in every form a probe used (and spaced
#: out); none of them talks about a token, so the hiding itself is what keeps them out.
ECHOES = {
    "json-list": f'Invalid request: ["{ROLE_TOKEN}"]',
    "tab-separated": f"value\t{ROLE_TOKEN}\tis not valid",
    "after-%3D": f"Invalid parameter X%3D{ROLE_TOKEN}",
    "bare": f"Invalid request {ROLE_TOKEN} was rejected",
    "two-halves": f"Invalid request {ROLE_TOKEN[:24]} and {ROLE_TOKEN[24:]}",
    "url-encoded": f"Invalid request {quote(ROLE_TOKEN, safe='')}",
    "json-escaped": "Invalid request " + ROLE_TOKEN.replace("/", "\\/"),
    "u-escaped": "Invalid request " + "".join(f"\\u{ord(char):04x}" for char in ROLE_TOKEN),
    "spaced-out": "Invalid request " + " ".join(ROLE_TOKEN[i : i + 4] for i in range(0, len(ROLE_TOKEN), 4)),
    "secret": f"Invalid request for {ROLE_SECRET}",
    "secret-url-encoded": f"Invalid request for {quote(ROLE_SECRET, safe='')}",
    "key-id": f"Invalid request from {ROLE_KEY_ID}",
}


@pytest.mark.parametrize("status", [400, 403, 500])
@pytest.mark.parametrize("message", list(ECHOES.values()), ids=list(ECHOES))
def test_an_echoed_credential_of_a_profile_never_reaches_a_message(
    aws_sdk: AwsFiles, status: int, message: str
) -> None:
    """The temporary credentials are in a profile file only (like an instance role's, in no
    variable): the provider hides the credentials its client signs with."""
    aws_sdk.write(**ROLE_PROFILE)
    api = FakeBedrock(aws_error(status, message))
    prov = BedrockProvider(bedrock_cfg(aws_profile="role"), client=api.client(aws_profile="role"))
    with pytest.raises(LLMUnavailable) as info:
        prov.classify(system="S", prompt="P")
    assert api.requests[0].headers["x-amz-security-token"] == ROLE_TOKEN  # signed with the profile's
    assert leaked(str(info.value), ROLE_TOKEN, ROLE_SECRET, ROLE_KEY_ID) == []
    assert "[hidden]" in str(info.value)


#: Messages that talk about the credentials: never shown, whatever they hold.
TALKS = {
    "sentence": f"The security token {ROLE_TOKEN} included in the request is invalid.",
    "quoted": f"security token '{ROLE_TOKEN}' is not valid",
    "json-field": json.dumps({"SessionToken": ROLE_TOKEN}),
    "xml-field": f"<SessionToken>{ROLE_TOKEN}</SessionToken>",
    "setting": f"aws_session_token = {ROLE_TOKEN}",
    "header": f"X-Amz-Security-Token: {ROLE_TOKEN}",
    "signature": f"Signature expired for {ROLE_TOKEN}",
    "authorization": f"Authorization header {ROLE_SECRET} is malformed",
}


@pytest.mark.parametrize("message", list(TALKS.values()), ids=list(TALKS))
def test_a_message_about_the_credentials_is_left_out(aws_sdk: AwsFiles, message: str) -> None:
    aws_sdk.write(**ROLE_PROFILE)
    api = FakeBedrock(aws_error(403, message, "AccessDeniedException"))
    with pytest.raises(LLMUnavailable) as info:
        BedrockProvider(bedrock_cfg(), client=api.client(aws_profile="role")).classify(system="S", prompt="P")
    assert str(info.value).startswith(
        "Amazon Bedrock refused the incident classification (403): "
        f"AccessDeniedException ({MESSAGE_LEFT_OUT}); "
    )
    assert leaked(str(info.value), ROLE_TOKEN, ROLE_SECRET) == []


def test_an_answer_without_a_message_is_never_quoted(aws_sdk: AwsFiles) -> None:
    aws_sdk.write(**ROLE_PROFILE)
    api = FakeBedrock((403, {"detail": f"echo of {ROLE_TOKEN}"}), (400, [f"echo of {ROLE_TOKEN}"]))
    prov = BedrockProvider(bedrock_cfg(), client=api.client(aws_profile="role"))
    texts = []
    for _ in range(2):
        with pytest.raises(LLMUnavailable) as info:
            prov.classify(system="S", prompt="P")
        texts.append(str(info.value))
    assert texts[0].startswith(
        "Amazon Bedrock refused the incident classification (403); check the worker's "
    )
    assert texts[1] == "Amazon Bedrock error 400 during the incident classification"
    assert leaked(" ".join(texts), ROLE_TOKEN) == []


#: A session token held by a profile only, shaped like a real one (letters and digits), with a run
#: of capitalised words and a run of lower-case letters in it: cuts of it pass for an AWS error
#: type (``AccessDeniedException``) or a refusal category (``cyber``).
TYPED_TOKEN = "IQoJb3JpZ2luX2VjEXAMPLE0wDAYDVR0TokenHeldInNoVariableAnywhere0lowercaserunofthetoken1EXAMPLE"
TYPED_PROFILE = {
    "config": "[profile typed]\nregion = eu-west-1\n",
    "credentials": (
        f"[typed]\naws_access_key_id = {ROLE_KEY_ID}\naws_secret_access_key = {ROLE_SECRET}\n"
        f"aws_session_token = {TYPED_TOKEN}\n"
    ),
}
#: Error types made of the token: 60 characters as they come, and its run of capitalised words.
TOKEN_ERROR_TYPES = {"cut": TYPED_TOKEN[:60], "capitalised-words": "TokenHeldInNoVariableAnywhere"}
ABOUT_THE_TOKEN = "The security token included in the request is invalid."


def error_type_answer(status: int, form: str, value: str, message: str | None) -> tuple[Any, ...]:
    """An error answer whose AWS error type is ``value``: in the ``x-amzn-ErrorType`` header
    (``header``) or in the body's ``__type`` (``type-field``), with or without AWS's message."""
    body: dict[str, Any] = {} if message is None else {"message": message}
    if form == "header":
        return (
            status,
            body,
            {"x-amzn-errortype": f"{value}:http://internal.amazon.com/coral/com.amazon.coral/"},
        )
    return status, {**body, "__type": f"com.amazon.coral.service#{value}"}


def classification_error(status: int, detail: str) -> str:
    """The provider's message for a classification that Bedrock answered with ``status``."""
    if status == 401:
        return (
            "Amazon Bedrock rejected the AWS credentials during the incident classification (401): check the "
            f"worker's AWS credentials; {detail}"
        )
    if status == 403:
        return (
            f"Amazon Bedrock refused the incident classification (403): {detail}; check the worker's AWS "
            f"credentials, its permission bedrock:InvokeModel on {MODEL} and the model access "
            "(docs/bedrock.md)"
        )
    return f"Amazon Bedrock error {status} during the incident classification: {detail}"


@pytest.mark.parametrize("message", [None, ABOUT_THE_TOKEN], ids=["no-message", "about-the-token"])
@pytest.mark.parametrize("form", ["header", "type-field"])
@pytest.mark.parametrize("status", [401, 403, 500])
@pytest.mark.parametrize("value", list(TOKEN_ERROR_TYPES.values()), ids=list(TOKEN_ERROR_TYPES))
def test_an_error_type_made_of_a_credential_never_reaches_a_message(
    aws_sdk: AwsFiles, value: str, status: int, form: str, message: str | None
) -> None:
    """The session token is in a profile file only (as an instance role's is in no variable): an
    error type cut from it, in the header or in the body, gives way to UNKNOWN_ERROR_TYPE."""
    aws_sdk.write(**TYPED_PROFILE)
    api = FakeBedrock(error_type_answer(status, form, value, message))
    prov = BedrockProvider(bedrock_cfg(aws_profile="typed"), client=api.client(aws_profile="typed"))
    with pytest.raises(LLMUnavailable) as info:
        prov.classify(system="S", prompt="P")
    assert api.requests[0].headers["x-amz-security-token"] == TYPED_TOKEN  # signed with the profile's
    detail = UNKNOWN_ERROR_TYPE if message is None else f"{UNKNOWN_ERROR_TYPE} ({MESSAGE_LEFT_OUT})"
    assert str(info.value) == classification_error(status, detail)
    assert leaked(str(info.value), TYPED_TOKEN) == []


@pytest.mark.parametrize(
    ("body", "headers", "shown"),
    [
        (
            {},
            {"x-amzn-errortype": "ValidationException:http://internal.amazon.com/coral/"},
            "ValidationException",
        ),
        ({}, {"x-amzn-errortype": "AccessDeniedException"}, "AccessDeniedException"),
        ({"__type": "com.amazon.coral.service#InternalFailure"}, {}, "InternalFailure"),
        ({"__type": "ThrottlingException"}, {"x-amzn-errortype": " "}, "ThrottlingException"),
        ({}, {"x-amzn-errortype": "access_denied"}, UNKNOWN_ERROR_TYPE),
        ({}, {"x-amzn-errortype": "Bad Request"}, UNKNOWN_ERROR_TYPE),
        ({}, {"x-amzn-errortype": "ValidationException2"}, UNKNOWN_ERROR_TYPE),
        ({}, {"x-amzn-errortype": "IQoJbZpZluXVjEXAMPLEtoken"}, UNKNOWN_ERROR_TYPE),
        ({"__type": "A" + "b" * 64}, {}, UNKNOWN_ERROR_TYPE),
        ({"__type": 7}, {"x-amzn-errortype": ""}, ""),
    ],
    ids=[
        "header",
        "bare-header",
        "type-field",
        "blank-header",
        "snake-case",
        "words",
        "digit",
        "mixed-capitals",
        "too-long",
        "none",
    ],
)
def test_only_names_shaped_like_aws_errors_are_shown(
    aws_keys: AwsFiles, body: dict[str, Any], headers: dict[str, str], shown: str
) -> None:
    """AWS names its errors with capitalised words; a run of letters cut from a key or a token that
    the provider does not know mixes the capitals at random (``mixed-capitals``)."""
    api = FakeBedrock((500, body, headers))
    with pytest.raises(LLMUnavailable) as info:
        BedrockProvider(bedrock_cfg(), client=api.client()).classify(system="S", prompt="P")
    expected = "Amazon Bedrock error 500 during the incident classification"
    assert str(info.value) == (f"{expected}: {shown}" if shown else expected)


def test_classify_incident_keeps_an_error_type_made_of_a_token_out_of_the_incident(
    aws_sdk: AwsFiles, caplog: pytest.LogCaptureFixture
) -> None:
    """End to end: a 403 whose ``x-amzn-ErrorType`` holds 60 characters of the profile's session
    token (the records from INFO up: the SDK's DEBUG record of an answer quotes its headers)."""
    aws_sdk.write(**TYPED_PROFILE)
    caplog.set_level(logging.INFO)
    cfg = bedrock_cfg(aws_profile="typed")
    api = FakeBedrock(error_type_answer(403, "header", TYPED_TOKEN[:60], None))
    outcome = classify_incident(cfg, customers_incident(), provider(api, cfg))
    assert api.requests[0].headers["x-amz-security-token"] == TYPED_TOKEN
    assert outcome.llm_error is not None and f"(403): {UNKNOWN_ERROR_TYPE};" in outcome.llm_error
    assert any("LLM classification unavailable" in record.getMessage() for record in caplog.records)
    for found in (outcome.diagnosis, outcome.llm_error, json.dumps(outcome.to_dict()), caplog.text):
        assert leaked(found, TYPED_TOKEN, ROLE_SECRET, ROLE_KEY_ID) == []


@pytest.mark.parametrize("structured", [True, False], ids=["structured", "json"])
@pytest.mark.parametrize(
    ("category", "shown"),
    [
        ("cyber", "cyber"),
        ("bio", "bio"),
        ("lowercaserunofthetoken", None),
        (TYPED_TOKEN[:32], None),
        ("Cyber", None),
        ("cyber security", None),
        ("x" * 33, None),
        ("", None),
        (7, None),
    ],
    ids=["cyber", "bio", "token-run", "token-cut", "capital", "words", "too-long", "empty", "number"],
)
def test_a_refusal_names_its_category_only_when_it_is_shaped_like_one(
    aws_sdk: AwsFiles, structured: bool, category: Any, shown: str | None
) -> None:
    """The category goes into the refusal's message, which the incident stores and the worker logs:
    lower-case letters and ``_`` only, 32 at most, and no credential (``token-run`` is shaped
    like a category, but cut from the profile's session token). The explanation is kept without
    any credential."""
    aws_sdk.write(**TYPED_PROFILE)
    details = {"type": "refusal", "category": category, "explanation": f"declined, echo of {TYPED_TOKEN}"}
    api = FakeBedrock(answer([], stop_reason="refusal", stop_details=details))
    prov = BedrockProvider(bedrock_cfg(aws_profile="typed"), client=api.client(aws_profile="typed"))
    prov.structured_outputs = structured
    with pytest.raises(LLMRefusal) as info:
        prov.classify(system="S", prompt="P")
    assert str(info.value) == (
        f"Claude declined the incident classification (refusal category: {shown or 'unspecified'})"
    )
    assert (info.value.category, info.value.explanation) == (shown, "declined, echo of [hidden]")
    assert len(api.requests) == 1


def test_classify_incident_keeps_a_refusal_category_made_of_a_token_out_of_the_incident(
    aws_sdk: AwsFiles, caplog: pytest.LogCaptureFixture
) -> None:
    aws_sdk.write(**TYPED_PROFILE)
    caplog.set_level(logging.DEBUG)
    cfg = bedrock_cfg(aws_profile="typed")
    details = {"type": "refusal", "category": TYPED_TOKEN[:32], "explanation": TYPED_TOKEN}
    outcome = classify_incident(
        cfg,
        customers_incident(),
        provider(FakeBedrock(answer([], stop_reason="refusal", stop_details=details)), cfg),
    )
    assert outcome.llm_error == (
        "the model declined the request: Claude declined the incident classification "
        "(refusal category: unspecified)"
    )
    for found in (outcome.diagnosis, json.dumps(outcome.to_dict()), caplog.text):
        assert leaked(found, TYPED_TOKEN) == []


def test_a_cut_api_key_never_reaches_a_message(aws_sdk: AwsFiles, monkeypatch: pytest.MonkeyPatch) -> None:
    key = "absk-example-caller-value-held-in-the-environment"  # no credential word in it, no pattern
    monkeypatch.setenv("AWS_BEARER_TOKEN_BEDROCK", key)
    api = FakeBedrock(
        aws_error(401, f"Unknown caller {key[:-6]}"), aws_error(400, f"Unknown {quote(key[6:])}")
    )
    prov = BedrockProvider(bedrock_cfg(), client=api.client())
    texts = []
    for _ in range(2):
        with pytest.raises(LLMUnavailable) as info:
            prov.classify(system="S", prompt="P")
        texts.append(str(info.value))
    assert api.requests[0].headers["authorization"] == f"Bearer {key}"
    assert leaked(" ".join(texts), key) == []
    assert all("[hidden]" in found for found in texts)  # hidden, not left out


def test_a_credential_process_error_never_shows_its_output() -> None:
    """botocore quotes the process's output as it is; the values are known to no one here."""
    botocore_exceptions = pytest.importorskip("botocore.exceptions")
    output = json.dumps(
        {"Version": 1, "AccessKeyId": ROLE_KEY_ID, "SecretAccessKey": ROLE_SECRET, "SessionToken": ROLE_TOKEN}
    )
    error = botocore_exceptions.CredentialRetrievalError(
        provider="custom-process", error_msg=f"bad: {output}"
    )
    with pytest.raises(LLMUnavailable) as info, bedrock_errors("the test", model=MODEL, region=REGION):
        raise error
    assert str(info.value).startswith(
        "the AWS credentials for the bedrock provider cannot be resolved during the test: "
        "CredentialRetrievalError: Error when retrieving credentials from custom-process: bad: "
    )
    assert leaked(str(info.value), ROLE_TOKEN, ROLE_SECRET, ROLE_KEY_ID) == []


def test_classify_incident_keeps_a_role_token_out_of_the_incident_and_the_logs(
    aws_sdk: AwsFiles, caplog: pytest.LogCaptureFixture
) -> None:
    """End to end: a 403 that echoes the token of the profile as a JSON list."""
    aws_sdk.write(**ROLE_PROFILE)
    caplog.set_level(logging.DEBUG)
    cfg = bedrock_cfg(aws_profile="role")
    api = FakeBedrock(aws_error(403, json.dumps([ROLE_TOKEN])))
    outcome = classify_incident(cfg, customers_incident(), provider(api, cfg))
    assert api.requests[0].headers["x-amz-security-token"] == ROLE_TOKEN
    assert outcome.llm_error is not None and "(403)" in outcome.llm_error
    assert any("LLM classification unavailable" in record.getMessage() for record in caplog.records)
    for found in (outcome.diagnosis, outcome.llm_error, json.dumps(outcome.to_dict()), caplog.text):
        assert leaked(found, ROLE_TOKEN, ROLE_SECRET, ROLE_KEY_ID) == []


def test_no_debug_record_holds_a_credential_of_the_profile(
    aws_sdk: AwsFiles, caplog: pytest.LogCaptureFixture
) -> None:
    aws_sdk.write(**ROLE_PROFILE)
    caplog.set_level(logging.DEBUG)
    api = FakeBedrock(answer([text(CLASSIFICATION_JSON)]))
    provider(api, bedrock_cfg(aws_profile="role")).classify(system="S", prompt="P")
    assert api.requests[0].headers["x-amz-security-token"] == ROLE_TOKEN
    assert any(record.name == "botocore.auth" for record in caplog.records)  # botocore did log at DEBUG
    for record in caplog.records:
        assert leaked(record.getMessage(), ROLE_TOKEN, ROLE_SECRET) == [], record.name


@pytest.mark.parametrize(
    ("text", "expected"),
    [
        pytest.param(
            "User: arn:aws:sts::123456789012:assumed-role/dag-healer-worker/i-0123456789abcdef0 is not "
            "authorized to perform: bedrock:InvokeModel on resource: arn:aws:bedrock:eu-west-1:123456789012:"
            "inference-profile/eu.anthropic.claude-opus-5-5",
            "User: arn:aws:sts::123456789012:assumed-role/dag-healer-worker/i-0123456789abcdef0 is not "
            "authorized to perform: bedrock:InvokeModel on resource: arn:aws:bedrock:eu-west-1:123456789012:"
            "inference-profile/eu.anthropic.claude-opus-5-5",
            id="access-denied-kept",
        ),
        pytest.param(
            "Authorization: AWS4-HMAC-SHA256 Credential=AKIDEXAMPLE/20261026/eu-west-1/bedrock/aws4_request, "
            "SignedHeaders=host;x-amz-date, "
            "Signature=fe5f80f77d5fa3beca038a248ff027d0445342fe2855ddc963176630326f1024",
            "Authorization: [hidden] Credential=[hidden], SignedHeaders=host;x-amz-date, Signature=[hidden]",
            id="authorization",
        ),
        pytest.param(
            "Authorization: Bearer ABSKQmVkcm9ja0FQSUtleS1leGFtcGxl",
            "Authorization: Bearer [hidden]",
            id="bearer",
        ),
        pytest.param('{"SessionToken": "abc-def-ghi-jkl"}', '{"SessionToken": "[hidden]"}', id="json-field"),
        pytest.param(
            "<SecretAccessKey>abc/def</SecretAccessKey>",
            "<SecretAccessKey>[hidden]</SecretAccessKey>",
            id="xml",
        ),
        pytest.param("aws_session_token = abc-def", "aws_session_token = [hidden]", id="setting"),
        pytest.param("seen IQoJb3JpZ2luX2VjEXAMPLE0123token", "seen [hidden]", id="random-looking"),
        pytest.param(
            "line one\r\nline\x1b[31m two %0D%0Aforged", "line one line [31m two forged", id="one-line"
        ),
    ],
)
def test_hide_credentials(text: str, expected: str) -> None:
    assert hide_credentials(text) == expected


def test_hide_credentials_hides_known_values_in_any_form(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("AWS_SECRET_ACCESS_KEY", raising=False)
    secret = ROLE_SECRET
    for form in (
        secret,
        quote(secret, safe=""),
        secret.replace("/", "\\/"),
        secret[3:-3],
        f"{secret[:20]} {secret[20:]}",
    ):
        assert leaked(hide_credentials(f"echo {form} end", [secret]), secret) == [], form
    assert hide_credentials("e x a m p l e / p r o f i l e + r o l e = p r i v a t e", [secret]) == "[hidden]"
    assert hide_credentials(f"echo {secret}", [secret], mask="***") == "echo ***"
    monkeypatch.setenv("AWS_SECRET_ACCESS_KEY", secret)  # the environment's are always looked for
    assert hide_credentials(f"echo {secret}") == "echo [hidden]"


# --------------------------------------------------------------------------- the settings rules


@pytest.mark.parametrize(
    ("environ", "region", "source"),
    [
        ({"AWS_REGION": " eu-west-1 "}, "eu-west-1", "AWS_REGION"),
        ({"AWS_REGION": "", "AWS_DEFAULT_REGION": "eu-west-3"}, "eu-west-3", "AWS_DEFAULT_REGION"),
        ({"AWS_REGION": " "}, None, None),
    ],
    ids=["aws-region", "default-region", "blank"],
)
def test_region_setting_names_its_source(
    environ: dict[str, str], region: str | None, source: str | None
) -> None:
    assert region_setting(None, environ) == (region, source)
    llm = type("Llm", (), {"aws_region": "eu-central-1"})()
    assert region_setting(llm, environ) == ("eu-central-1", "llm.aws_region")


@pytest.mark.parametrize(
    ("environ", "problem"),
    [
        ({"AWS_REGION": "collector.example.net/"}, REGION_INVALID.format(source="AWS_REGION")),
        ({"AWS_REGION": "Europe West"}, REGION_INVALID.format(source="AWS_REGION")),
        (
            {"AWS_DEFAULT_REGION": "eu-west-1.collector.example.net#"},
            REGION_INVALID.format(source="AWS_DEFAULT_REGION"),
        ),
        ({"AWS_REGION": " "}, REGION_BLANK.format(name="AWS_REGION")),
        ({"AWS_REGION": "", "AWS_DEFAULT_REGION": "\t"}, REGION_BLANK.format(name="AWS_DEFAULT_REGION")),
        (
            {"AWS_REGION": "eu-west-1", "ANTHROPIC_BEDROCK_BASE_URL": "http://collector.example.net"},
            BASE_URL_INVALID,
        ),
        ({"AWS_REGION": "eu-west-1", "ANTHROPIC_BEDROCK_BASE_URL": " "}, BASE_URL_EMPTY),
    ],
    ids=["host", "words", "default-region", "blank", "blank-default", "base-url-http", "base-url-blank"],
)
def test_settings_problems(environ: dict[str, str], problem: str) -> None:
    llm = config_from_mapping({"llm": {"provider": "bedrock", "model": MODEL}}).llm
    assert settings_problems(llm, environ) == [problem]
    assert settings_problems(llm, {"AWS_REGION": "eusc-de-east-1"}) == []
    assert settings_problems(llm, {"AWS_REGION": " ", "AWS_DEFAULT_REGION": "eu-west-1"}) == []  # a region


@pytest.mark.parametrize(
    "region",
    ["collector.example.net/", "Europe West", "eu-west-1.collector.example.net#"],
    ids=["host", "words", "suffix"],
)
@pytest.mark.parametrize("variable", ["AWS_REGION", "AWS_DEFAULT_REGION"])
def test_a_region_variable_that_is_no_region_sends_nothing(
    aws_keys: AwsFiles, monkeypatch: pytest.MonkeyPatch, variable: str, region: str
) -> None:
    monkeypatch.setenv(variable, region)
    api = FakeBedrock()
    with pytest.raises(LLMUnavailable) as info:
        provider(api, bedrock_cfg(aws_region=None)).classify(system="S", prompt="P")
    assert str(info.value) == REGION_INVALID.format(source=variable)
    assert api.requests == []


def test_a_profile_region_that_is_no_region_sends_nothing(aws_keys: AwsFiles) -> None:
    aws_keys.write(config="[default]\nregion = eu-west-1.collector.example.net#\n")
    api = FakeBedrock()
    with pytest.raises(LLMUnavailable) as info:
        provider(api, bedrock_cfg(aws_region=None)).classify(system="S", prompt="P")
    assert str(info.value) == REGION_INVALID.format(source=PROFILE_REGION)
    assert api.requests == []


def test_a_blank_region_variable_is_refused_next_to_a_profile_region(
    aws_keys: AwsFiles, monkeypatch: pytest.MonkeyPatch
) -> None:
    """The SDK would read the blank value as the region (``bedrock-runtime.%20.amazonaws.com``)."""
    aws_keys.write(config="[default]\nregion = eu-west-1\n")
    monkeypatch.setenv("AWS_REGION", " ")
    api = FakeBedrock()
    with pytest.raises(LLMUnavailable) as info:
        provider(api, bedrock_cfg(aws_region=None)).classify(system="S", prompt="P")
    assert str(info.value) == REGION_BLANK.format(name="AWS_REGION")
    assert api.requests == []


@pytest.mark.parametrize(
    "base_url",
    [
        "http://collector.example.net",
        "https://collector.example.net\\.bedrock-runtime.eu-west-1.amazonaws.com",
        "https://bedrock.example.com/x?y#z",
        "https://user:pw@bedrock.example.com",
    ],
    ids=["http", "backslash", "query-fragment", "user-info"],
)
def test_an_invalid_sdk_base_url_sends_nothing(
    aws_keys: AwsFiles, monkeypatch: pytest.MonkeyPatch, base_url: str
) -> None:
    monkeypatch.setenv("ANTHROPIC_BEDROCK_BASE_URL", base_url)
    api = FakeBedrock()
    with pytest.raises(LLMUnavailable) as info:
        provider(api).classify(system="S", prompt="P")
    assert str(info.value) == BASE_URL_INVALID
    assert api.requests == []


@pytest.mark.parametrize(
    ("base_url", "url"),
    [
        (" https://bedrock.gateway.example.com/ ", "https://bedrock.gateway.example.com"),
        ("http://127.0.0.1:8080/bedrock", "http://127.0.0.1:8080/bedrock"),
        ("http://localhost:8080", "http://localhost:8080"),
    ],
    ids=["spaces", "loopback", "localhost"],
)
def test_a_valid_sdk_base_url_is_handed_to_the_client(
    aws_keys: AwsFiles, monkeypatch: pytest.MonkeyPatch, base_url: str, url: str
) -> None:
    monkeypatch.setenv("ANTHROPIC_BEDROCK_BASE_URL", base_url)
    api = FakeBedrock(answer([text(CLASSIFICATION_JSON)]))
    provider(api).classify(system="S", prompt="P")
    assert str(api.requests[0].url) == url + INVOKE_PATH


def test_a_model_with_control_characters_is_refused_without_its_value(
    aws_keys: AwsFiles, caplog: pytest.LogCaptureFixture
) -> None:
    caplog.set_level(logging.WARNING, logger="dag_healer.agent.bedrock")
    api = FakeBedrock()
    prov = provider(api, model=f"{MODEL}\r\n2026-10-27 WARNING dag_healer: forged")
    with pytest.raises(LLMUnavailable) as info:
        prov.classify(system="S", prompt="P")
    assert str(info.value) == MODEL_NOT_PRINTABLE
    assert caplog.messages == [MODEL_NOT_PRINTABLE]
    assert model_alias_problem(f"{MODEL} beta") == MODEL_NOT_PRINTABLE
    assert api.requests == []


# --------------------------------------------------------------------------- tokens of failed classifications


def test_a_failed_degraded_classification_counts_every_billed_answer(aws_keys: AwsFiles) -> None:
    usage = {"input_tokens": 1000, "output_tokens": 100}
    answers = [answer([text("not a classification")], usage=usage) for _ in range(3)]
    api = FakeBedrock(*answers)
    with pytest.raises(LLMUnavailable) as info:
        provider(api).classify(system="S", prompt="P")
    assert (str(info.value), info.value.tokens_used, len(api.requests)) == (JSON_ANSWER_INVALID, 3300, 3)
    # the incident's tokens_used counts them too (limits.max_tokens_per_incident applies to a repair run)
    cfg = bedrock_cfg()
    outcome = classify_incident(cfg, customers_incident(), provider(FakeBedrock(*answers), cfg))
    assert (outcome.tokens_used, outcome.classification) == (3300, None)
    assert outcome.llm_error == JSON_ANSWER_INVALID


def test_a_refusal_after_a_billed_answer_keeps_its_tokens(aws_keys: AwsFiles) -> None:
    refusal = answer(
        [],
        stop_reason="refusal",
        stop_details={"type": "refusal", "category": "cyber", "explanation": "declined"},
        usage={"input_tokens": 900, "output_tokens": 10},
    )
    api = FakeBedrock(answer([text("not JSON")], usage={"input_tokens": 1000, "output_tokens": 100}), refusal)
    prov = provider(api)
    prov.structured_outputs = False
    with pytest.raises(LLMRefusal) as info:
        prov.classify(system="S", prompt="P")
    assert info.value.tokens_used == 2010
    cut = FakeBedrock(
        answer([text('{"category"')], stop_reason="max_tokens", usage={"input_tokens": 5, "output_tokens": 7})
    )
    with pytest.raises(LLMUnavailable, match="max_tokens") as cut_info:
        provider(cut).classify(system="S", prompt="P")
    assert cut_info.value.tokens_used == 12
    with pytest.raises(LLMUnavailable) as unbilled:
        provider(FakeBedrock(), model="claude-opus-5-5").classify(system="S", prompt="P")
    assert unbilled.value.tokens_used == 0


# --------------------------------------------------------------------------- the credentials that signed

#: A credential process that prints new temporary credentials on every run, as one that assumes a
#: role or fetches single sign-on credentials does, and counts its runs in ``runs`` next to it.
#: ``plain`` values are lower-case on purpose: no pattern sees them and none talks about a token,
#: so only the provider's knowledge of the credentials it signed with hides them; the run's number
#: between their words leaves two runs' values no fragment of 16 characters in common. ``random``
#: ones look like real session tokens (letters and digits), which the patterns hide on their own.
CREDENTIAL_PROCESS = """\
import json
import pathlib
import sys

runs = pathlib.Path(__file__).with_name("runs")
run = int(runs.read_text(encoding="utf-8")) + 1 if runs.exists() else 1
runs.write_text(str(run), encoding="utf-8")
secret = str(run).join(["process", "private", "key", "held", "in", "no", "variable", "example"])
if sys.argv[1] == "random":
    token = f"IQoJb3JpZ2luX2VjEXAMPLE{run}ProcessSessionValue0123456789"
else:
    token = str(run).join(["process", "session", "value", "held", "in", "no", "variable", "example"])
print(json.dumps({
    "Version": 1,
    "AccessKeyId": f"ASIAEXAMPLEPROCESS{run:02d}",
    "SecretAccessKey": secret,
    "SessionToken": token,
    "Expiration": "2099-01-01T00:00:00Z",
}))
"""


@dataclass
class CredentialProcess:
    """The ``process`` profile of the shared AWS config file, whose ``credential_process`` prints
    new credentials on every run (:data:`CREDENTIAL_PROCESS`)."""

    folder: Path

    @property
    def runs(self) -> int:
        path = self.folder / "runs"
        return int(path.read_text(encoding="utf-8")) if path.exists() else 0


def credential_process(aws: AwsFiles, folder: Path, values: str = "plain") -> CredentialProcess:
    """Write the ``process`` profile (``values``: ``plain`` or ``random``)."""
    folder.mkdir()
    script = folder / "credentials.py"
    script.write_text(CREDENTIAL_PROCESS, encoding="utf-8")
    command = f'"{sys.executable}" "{script}" {values}'
    aws.write(config=f"[profile process]\nregion = {REGION}\ncredential_process = {command}\n")
    return CredentialProcess(folder)


class EchoingBedrock:
    """An endpoint that answers 403 with AWS's message quoting the session token that each
    request was signed with (``bare``, or as a JSON list); the tokens are kept."""

    def __init__(self, form: str = "bare") -> None:
        self.form = form
        self.tokens: list[str] = []

    def handler(self, request: httpx2.Request) -> httpx2.Response:
        token = request.headers["x-amz-security-token"]
        self.tokens.append(token)
        echo = (
            f"Invalid request {token} was rejected"
            if self.form == "bare"
            else f'Invalid request: ["{token}"]'
        )
        return httpx2.Response(403, json={"message": echo})

    def client(self) -> anthropic.AnthropicBedrock:
        """A client of the ``process`` profile, without retries."""
        http_client = httpx2.Client(transport=httpx2.MockTransport(self.handler))
        return anthropic.AnthropicBedrock(
            aws_region=REGION, aws_profile="process", http_client=http_client, max_retries=0
        )


def sdk_session_cache() -> Any:
    """The SDK's cache of boto3 sessions that the provider reads
    (``anthropic.lib.bedrock._auth._get_session``: an ``lru_cache`` with its five keywords), or
    ``None`` when the installed SDK has none of that form."""
    try:
        from anthropic.lib.bedrock import _auth
    except ImportError:
        return None
    cache = getattr(_auth, "_get_session", None)
    try:
        parameters = set(inspect.signature(cache).parameters)
    except (TypeError, ValueError):
        return None
    expected = {"aws_access_key", "aws_secret_key", "aws_session_token", "region", "profile"}
    return cache if parameters == expected and callable(getattr(cache, "cache_clear", None)) else None


#: The session token of the process's first run (``plain``).
PROCESS_TOKEN = "process1session1value1held1in1no1variable1example"
NO_SDK_SESSION_CACHE = (
    "the installed anthropic SDK has no anthropic.lib.bedrock._auth._get_session of the form the "
    "provider reads, so the provider cannot know the credentials the SDK signed with; these are "
    "not random-looking, and only the patterns of hide_credentials remain"
)


@pytest.mark.parametrize("form", ["bare", "json-list"])
def test_the_session_token_that_signed_is_hidden_and_the_chain_runs_once(
    aws_sdk: AwsFiles, tmp_path: Path, form: str
) -> None:
    """A credential process that prints new credentials on every run, as one that assumes a role
    or fetches single sign-on credentials does: the provider hides the session token that the
    request was signed with, read from the very session the SDK signed with, and never runs the
    chain again (for an assume-role profile with MFA: no second AssumeRole call, no second
    prompt)."""
    process = credential_process(aws_sdk, tmp_path / "process")
    api = EchoingBedrock(form)
    prov = BedrockProvider(bedrock_cfg(aws_profile="process"), client=api.client())
    errors = []
    for _ in range(2):
        with pytest.raises(LLMUnavailable) as info:
            prov.classify(system="S", prompt="P")
        errors.append(str(info.value))
    # One run for each session the SDK signed with, whatever the SDK: the provider ran nothing.
    assert process.runs == len(set(api.tokens))
    if sdk_session_cache() is None:
        pytest.skip(NO_SDK_SESSION_CACHE)
    assert process.runs == 1  # the SDK's first signature resolved the chain; nothing else did
    assert api.tokens == [PROCESS_TOKEN] * 2  # the first run's, from the session the SDK keeps
    for error in errors:
        assert error.startswith("Amazon Bedrock refused the incident classification (403): Invalid request")
        assert leaked(error, api.tokens[0]) == []
        assert "[hidden]" in error


def test_a_session_that_did_not_sign_is_never_resolved(aws_sdk: AwsFiles, tmp_path: Path) -> None:
    """Should the lookup find another session than the one the SDK signed with (an SDK whose
    cache key differs; here the cache is cleared), nothing is read from it: resolving its
    credentials would run the chain again, for values that did not sign."""
    cache = sdk_session_cache()
    if cache is None:
        pytest.skip(NO_SDK_SESSION_CACHE)
    process = credential_process(aws_sdk, tmp_path / "process")
    api = EchoingBedrock()
    client = api.client()
    with pytest.raises(LLMUnavailable):
        BedrockProvider(bedrock_cfg(aws_profile="process"), client=client).classify(system="S", prompt="P")
    signed = ["ASIAEXAMPLEPROCESS01", "process1private1key1held1in1no1variable1example", PROCESS_TOKEN]
    assert bedrock._session_credentials(client) == signed  # the session that signed
    cache.cache_clear()
    assert bedrock._session_credentials(client) == []  # a new session: not resolved
    assert process.runs == 1


def test_the_credentials_that_signed_are_read_without_a_refresh(monkeypatch: pytest.MonkeyPatch) -> None:
    """Credentials near their expiry (inside botocore's refresh windows) refresh on the next read
    of their properties or of ``get_frozen_credentials()``: another run of the chain (a
    credential process, an AssumeRole call and its MFA prompt) for values that did not sign. The
    lookup reads the values of their last refresh as botocore keeps them; deferred credentials
    that never refreshed did not sign, and give none. The SDK's session is stood in for, so the
    test holds whatever the SDK's cache looks like."""
    botocore_credentials = pytest.importorskip("botocore.credentials")
    auth = pytest.importorskip("anthropic.lib.bedrock._auth")
    refreshes: list[str] = []

    def refresh() -> dict[str, str]:
        refreshes.append("refresh")
        raise AssertionError("the lookup refreshed the credentials")

    near_expiry = botocore_credentials.RefreshableCredentials(
        access_key=ROLE_KEY_ID,
        secret_key=ROLE_SECRET,
        token=ROLE_TOKEN,
        expiry_time=datetime.now(timezone.utc) + timedelta(minutes=5),
        refresh_using=refresh,
        method="custom-process",
    )
    assert near_expiry.refresh_needed()  # botocore would refresh them on the next read
    botocore_session = types.SimpleNamespace(_credentials=near_expiry)
    monkeypatch.setattr(
        auth, "_get_session", lambda **_: types.SimpleNamespace(_session=botocore_session), raising=False
    )
    client = types.SimpleNamespace(
        aws_profile="process",
        aws_region=REGION,
        aws_access_key=None,
        aws_secret_key=None,
        aws_session_token=None,
    )
    assert bedrock._session_credentials(client) == [ROLE_KEY_ID, ROLE_SECRET, ROLE_TOKEN]
    botocore_session._credentials = botocore_credentials.DeferredRefreshableCredentials(
        refresh_using=refresh, method="assume-role"
    )
    assert bedrock._session_credentials(client) == []
    assert refreshes == []
    botocore_session._credentials = botocore_credentials.Credentials(ACCESS_KEY, SECRET_KEY, SESSION_TOKEN)
    assert bedrock._session_credentials(client) == [ACCESS_KEY, SECRET_KEY, SESSION_TOKEN]


def test_without_the_sdk_session_cache_the_patterns_still_hide_a_real_looking_token(
    aws_sdk: AwsFiles, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """An SDK whose ``_auth`` module offers ``get_auth_headers`` only (as the provider's lookup
    sees it; the SDK still signs): nothing is resolved anew, and a session token that looks like
    a real one, letters and digits at random, is hidden by the patterns all the same."""
    auth = pytest.importorskip("anthropic.lib.bedrock._auth")
    if not callable(getattr(auth, "get_auth_headers", None)):
        pytest.skip(
            "the installed anthropic SDK does not sign with anthropic.lib.bedrock._auth.get_auth_headers"
        )
    stub = types.ModuleType(auth.__name__)
    stub.get_auth_headers = auth.get_auth_headers  # type: ignore[attr-defined]
    monkeypatch.setitem(sys.modules, auth.__name__, stub)
    process = credential_process(aws_sdk, tmp_path / "process", "random")
    api = EchoingBedrock()
    client = api.client()
    with pytest.raises(LLMUnavailable) as info:
        BedrockProvider(bedrock_cfg(aws_profile="process"), client=client).classify(system="S", prompt="P")
    assert bedrock._session_credentials(client) == []
    assert process.runs == 1
    assert leaked(str(info.value), api.tokens[0]) == []
    assert "[hidden]" in str(info.value)


# --------------------------------------------------------------------------- the credential variables


def assert_valid_http11(request: httpx2.Request) -> None:
    """h11, the HTTP/1.1 library under the SDK, takes every header of ``request``: it refuses an
    invalid one, and its error quotes the value whole."""
    h11.Request(method=request.method, target=request.url.raw_path, headers=request.headers.raw)


@pytest.mark.parametrize(
    ("name", "value"),
    [
        ("AWS_SESSION_TOKEN", f"{SESSION_TOKEN}\n"),
        ("AWS_SECURITY_TOKEN", f"{SESSION_TOKEN}\n"),
        ("AWS_ACCESS_KEY_ID", f" {ACCESS_KEY}\t"),
        ("AWS_SECRET_ACCESS_KEY", f"{SECRET_KEY}\r\n"),
    ],
    ids=["token-newline", "legacy-token-newline", "key-id-spaces", "secret-crlf"],
)
@pytest.mark.parametrize("api_key", [None, "", " \n"], ids=["no-api-key", "empty-api-key", "blank-api-key"])
def test_whitespace_around_an_aws_key_of_the_environment_falls_away(
    aws_keys: AwsFiles, monkeypatch: pytest.MonkeyPatch, name: str, value: str, api_key: str | None
) -> None:
    """As a file or a secret store often leaves it: botocore would sign with the value as it is,
    and the newline would make an invalid header (the token, the key ID) or a wrong signature
    (the secret). The token is AWS_SECURITY_TOKEN's when it is set (botocore reads its legacy
    name first), AWS_SESSION_TOKEN's otherwise. The provider hands the stripped keys to the
    client (after building it, next to an empty or blank AWS_BEARER_TOKEN_BEDROCK, which counts
    as unset: the SDK would take it as the key, and refuses one next to AWS keys)."""
    monkeypatch.setenv(name, value)
    if api_key is not None:
        monkeypatch.setenv("AWS_BEARER_TOKEN_BEDROCK", api_key)
    api = FakeBedrock(answer([text(CLASSIFICATION_JSON)]))
    prov = provider(api)
    prov.classify(system="S", prompt="P")
    request = api.requests[0]
    assert_sigv4(request)
    assert request.headers["x-amz-security-token"] == SESSION_TOKEN
    assert_valid_http11(request)
    signing = (prov._client.aws_access_key, prov._client.aws_secret_key, prov._client.aws_session_token)
    assert signing == (ACCESS_KEY, SECRET_KEY, SESSION_TOKEN)
    assert prov._client.api_key is None


def test_aws_keys_without_whitespace_stay_the_chains(aws_keys: AwsFiles) -> None:
    """The provider hands no key to the client then: botocore's chain reads them, with its own
    rules (an expiry in AWS_CREDENTIAL_EXPIRATION among them)."""
    api = FakeBedrock(answer([text(CLASSIFICATION_JSON)]))
    prov = provider(api)
    prov.classify(system="S", prompt="P")
    assert (prov._client.aws_access_key, prov._client.aws_secret_key, prov._client.aws_session_token) == (
        None,
        None,
        None,
    )
    assert_sigv4(api.requests[0])


def test_whitespace_around_the_api_key_falls_away(aws_sdk: AwsFiles, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("AWS_BEARER_TOKEN_BEDROCK", f" {API_KEY}\n")
    api = FakeBedrock(answer([text(CLASSIFICATION_JSON)]))
    provider(api).classify(system="S", prompt="P")
    assert api.requests[0].headers["authorization"] == f"Bearer {API_KEY}"
    assert_valid_http11(api.requests[0])


@pytest.mark.parametrize(
    "name",
    [
        "AWS_ACCESS_KEY_ID",
        "AWS_SECRET_ACCESS_KEY",
        "AWS_SESSION_TOKEN",
        "AWS_SECURITY_TOKEN",
        "AWS_BEARER_TOKEN_BEDROCK",
    ],
)
@pytest.mark.parametrize(
    "within", ["\n", "\t", " ", "\x1b", "é"], ids=["newline", "tab", "space", "escape", "non-ascii"]
)
def test_a_credential_variable_with_whitespace_within_is_refused_without_its_value(
    aws_keys: AwsFiles,
    monkeypatch: pytest.MonkeyPatch,
    caplog: pytest.LogCaptureFixture,
    name: str,
    within: str,
) -> None:
    """No credential holds such a character, and the SDK would send it in a header that the HTTP
    library refuses and quotes whole: nothing is sent, and the message names the variable only."""
    value = f"example-credential-value{within}of-the-caller\n"
    monkeypatch.setenv(name, value)
    caplog.set_level(logging.DEBUG)
    api = FakeBedrock()
    with pytest.raises(LLMUnavailable) as info:
        provider(api).classify(system="S", prompt="P")
    assert str(info.value) == CREDENTIAL_NOT_PRINTABLE.format(name=name)
    assert api.requests == []
    assert leaked(str(info.value) + caplog.text, value.strip()) == []
    assert settings_problems(bedrock_cfg().llm) == [CREDENTIAL_NOT_PRINTABLE.format(name=name)]  # doctor's


# --------------------------------------------------------------------------- debug records


def http_answer(
    status: int, body: Any, headers: dict[str, str] | None = None, *, reason: str | None = None
) -> bytes:
    """The bytes of an HTTP/1.1 answer with a JSON body (``Connection: close``: every request
    gets a new connection, which reads the answer again); ``reason`` replaces the standard reason
    phrase of ``status``."""
    content = json.dumps(body).encode("utf-8")
    lines = [
        f"HTTP/1.1 {status} { ({200: 'OK', 403: 'Forbidden'}[status] if reason is None else reason) }",
        "Content-Type: application/json",
        f"Content-Length: {len(content)}",
        "Connection: close",
        "x-amzn-RequestId: 0123abcd-example",
        *(f"{name}: {value}" for name, value in (headers or {}).items()),
    ]
    return ("\r\n".join(lines) + "\r\n\r\n").encode("utf-8") + content


class WireBody(httpx2.SyncByteStream):
    """The body of an answer that :class:`MemoryWire` receives, read from httpcore2."""

    def __init__(self, stream: Any) -> None:
        self.stream = stream

    def __iter__(self) -> Iterator[bytes]:
        yield from self.stream

    def close(self) -> None:
        close = getattr(self.stream, "close", None)
        if callable(close):
            close()


class MemoryWire(httpx2.BaseTransport):
    """A transport on httpcore2's in-memory network (``MockBackend``), through public APIs only: a
    request goes through httpcore2's connection pool and h11 as on a socket, their DEBUG records
    included, and every connection reads ``answer`` as the server's bytes. The pool has no other
    network backend, so nothing leaves the process, whatever the internals of httpx2."""

    def __init__(self, answer: bytes) -> None:
        self.pool = httpcore2.ConnectionPool(network_backend=httpcore2.MockBackend([answer]))

    def handle_request(self, request: httpx2.Request) -> httpx2.Response:
        url = request.url
        received = self.pool.handle_request(
            httpcore2.Request(
                method=request.method,
                url=httpcore2.URL(
                    scheme=url.raw_scheme, host=url.raw_host, port=url.port, target=url.raw_path
                ),
                headers=request.headers.raw,
                content=request.stream,
                extensions=request.extensions,
            )
        )
        return httpx2.Response(
            received.status,
            headers=received.headers,
            stream=WireBody(received.stream),
            extensions=received.extensions,
        )

    def close(self) -> None:
        self.pool.close()


def install_cli_logging(monkeypatch: pytest.MonkeyPatch, verbosity: int) -> Any:
    """The logging of ``dag-healer`` with ``verbosity`` ``-v`` flags, without ``ANTHROPIC_LOG``: the
    CLI's filter on the loggers of the SDK, its HTTP clients and their transports (taken off again
    at teardown). Returns ``dag_healer.cli.main``."""
    from dag_healer.cli import main as cli

    monkeypatch.delenv("ANTHROPIC_LOG", raising=False)
    names = [*cli.FILTERED_LOGGERS]
    names += [name for name in list(logging.Logger.manager.loggerDict) if name.startswith("httpcore")]
    for name in dict.fromkeys(names):
        target = logging.getLogger(name)
        kept = [item for item in target.filters if not isinstance(item, cli._SdkRequestFilter)]
        monkeypatch.setattr(target, "filters", kept)
    monkeypatch.setattr(cli._SDK_FILTER, "keep_requests", cli._SDK_FILTER.keep_requests)
    cli._configure_logging(verbosity)
    return cli


@pytest.fixture
def cli_logging(monkeypatch: pytest.MonkeyPatch) -> Any:
    """The logging of ``dag-healer -vv`` (:func:`install_cli_logging`)."""
    return install_cli_logging(monkeypatch, 2)


def records_of(caplog: pytest.LogCaptureFixture, *prefixes: str) -> list[str]:
    """The messages of the captured records whose logger's name starts with one of ``prefixes``."""
    return [record.getMessage() for record in caplog.records if record.name.startswith(prefixes)]


def test_the_log_filter_knows_the_credential_variables_of_both_providers() -> None:
    """The CLI spells the variables out so that it imports neither provider: the same list."""
    from dag_healer.agent import llm
    from dag_healer.cli import main as cli

    assert set(cli._CREDENTIAL_ENVS) == set(llm._CREDENTIAL_ENVS) | set(bedrock._SECRET_ENVS)
    assert len(set(cli._CREDENTIAL_ENVS)) == len(cli._CREDENTIAL_ENVS)


@pytest.mark.parametrize("name", ["AWS_SESSION_TOKEN", "AWS_BEARER_TOKEN_BEDROCK"])
def test_a_credential_that_ends_with_a_newline_is_sent_valid_and_logged_nowhere(
    aws_keys: AwsFiles,
    monkeypatch: pytest.MonkeyPatch,
    caplog: pytest.LogCaptureFixture,
    cli_logging: Any,
    name: str,
) -> None:
    """At DEBUG, through httpcore2 and h11 as on a socket: the value goes without its newline.
    With it, h11 would refuse the header, and the SDK's ``Encountered Exception`` record and
    httpcore2's trace of the failure would quote the whole credential, once per retry."""
    value = SESSION_TOKEN if name == "AWS_SESSION_TOKEN" else API_KEY
    monkeypatch.setenv(name, f"{value}\n")
    caplog.set_level(logging.DEBUG)
    wire = MemoryWire(http_answer(200, answer([text(CLASSIFICATION_JSON)])))
    prov = BedrockProvider(bedrock_cfg(), http_client=httpx2.Client(transport=wire))
    assert prov.classify(system="S", prompt="P")[0].category == "code"
    assert any(record.getMessage() == "send_request_headers.complete" for record in caplog.records)  # h11's
    assert leaked(caplog.text, value, SECRET_KEY) == []


def test_debug_records_never_quote_a_header_value_that_h11_refused(
    aws_sdk: AwsFiles, caplog: pytest.LogCaptureFixture, cli_logging: Any
) -> None:
    """A client injected with a session token that ends with a newline (no check of the provider
    applies to it), held in no variable: h11 refuses the header, and neither the SDK's
    ``Encountered Exception`` record nor httpcore2's trace of the failure reaches the log."""
    token = "example/injected+value/held-in-no-variable"
    caplog.set_level(logging.DEBUG)
    client = anthropic.AnthropicBedrock(
        aws_region=REGION,
        aws_access_key=ROLE_KEY_ID,
        aws_secret_key=ROLE_SECRET,
        aws_session_token=f"{token}\n",
        http_client=httpx2.Client(transport=MemoryWire(b"")),
        max_retries=0,
    )
    with pytest.raises(LLMUnavailable) as info:
        BedrockProvider(bedrock_cfg(), client=client).classify(system="S", prompt="P")
    assert str(info.value) == (
        f"Cannot reach Amazon Bedrock ({HOST}) during the incident classification: APIConnectionError"
    )
    assert any(record.getMessage().startswith("send_request_headers.started") for record in caplog.records)
    assert leaked(caplog.text, token) == []
    assert "Illegal header" not in caplog.text


def test_debug_records_never_show_the_headers_of_an_answer(
    aws_sdk: AwsFiles, caplog: pytest.LogCaptureFixture, cli_logging: Any
) -> None:
    """At ``-vv``, the SDK's ``HTTP Response`` record and httpcore2's ``receive_response_headers``
    trace would print every header of an answer as it came: here an ``x-amzn-ErrorType`` cut from
    the profile's session token and a gateway's header that echoes it whole (in no variable).
    Whatever the libraries' wording of the rest, each kind of record is kept, with
    ``[headers not shown]`` for the headers."""
    aws_sdk.write(**ROLE_PROFILE)
    caplog.set_level(logging.DEBUG)
    headers = {"x-amzn-ErrorType": f"AccessDeniedException:{ROLE_TOKEN[:30]}", "x-echo": ROLE_TOKEN}
    wire = MemoryWire(http_answer(403, {"message": "Access denied."}, headers))
    prov = BedrockProvider(bedrock_cfg(aws_profile="role"), http_client=httpx2.Client(transport=wire))
    with pytest.raises(LLMUnavailable) as info:
        prov.classify(system="S", prompt="P")
    assert "(403): Access denied.;" in str(info.value)
    for prefix in (cli_logging.SDK_REQUEST_LOGGER, "httpcore2"):
        assert any(cli_logging.HEADERS_NOT_SHOWN in item for item in records_of(caplog, prefix)), prefix
    assert leaked(caplog.text, ROLE_TOKEN, ROLE_SECRET) == []


@pytest.mark.parametrize("verbosity", [1, 2], ids=["v", "vv"])
@pytest.mark.parametrize("held", ["variable", "profile"])
def test_the_reason_phrase_of_an_answer_never_reaches_the_log(
    aws_sdk: AwsFiles,
    monkeypatch: pytest.MonkeyPatch,
    caplog: pytest.LogCaptureFixture,
    verbosity: int,
    held: str,
) -> None:
    """The endpoint writes the reason phrase of its status line, and httpx2's ``HTTP Request``
    record (INFO: ``-v``, or ``ANTHROPIC_LOG=info``), the SDK's ``HTTP Response`` record and
    httpcore2's trace of the answer (DEBUG: ``-vv``) would print it as it came: here an echo of
    the session token, held in AWS_SESSION_TOKEN or only by a profile. Each record is kept (the
    filter puts the standard reason phrase of the status code in its place, as
    tests/unit/test_cli_commands.py shows record by record), and none holds the token."""
    if held == "variable":
        token, cfg = SESSION_TOKEN, bedrock_cfg()
        for name, value in (
            ("AWS_ACCESS_KEY_ID", ACCESS_KEY),
            ("AWS_SECRET_ACCESS_KEY", SECRET_KEY),
            ("AWS_SESSION_TOKEN", SESSION_TOKEN),
        ):
            monkeypatch.setenv(name, value)
    else:
        token, cfg = ROLE_TOKEN, bedrock_cfg(aws_profile="role")
        aws_sdk.write(**ROLE_PROFILE)
    cli = install_cli_logging(monkeypatch, verbosity)
    caplog.set_level(logging.INFO if verbosity == 1 else logging.DEBUG)
    wire = MemoryWire(http_answer(403, {"message": "Access denied."}, reason=f"Forbidden {token}"))
    with pytest.raises(LLMUnavailable) as info:
        BedrockProvider(cfg, http_client=httpx2.Client(transport=wire)).classify(system="S", prompt="P")
    assert "(403): Access denied.;" in str(info.value)
    loggers = ["httpx2"] + ([cli.SDK_REQUEST_LOGGER, "httpcore2"] if verbosity == 2 else [])
    for prefix in loggers:
        assert records_of(caplog, prefix), prefix
    assert leaked(caplog.text, token, SECRET_KEY, ROLE_SECRET) == []


#: Answers that h11 cannot read, whose bytes its error quotes as they came: a status line whose
#: code is no number, and a chunk header that is no chunk size, each made of the profile's token.
MALFORMED_ANSWERS = {
    "status-line": f"HTTP/1.1 4x3 {ROLE_TOKEN}\r\nContent-Length: 0\r\nConnection: close\r\n\r\n",
    "chunk-header": (
        "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nTransfer-Encoding: chunked\r\n"
        f"Connection: close\r\n\r\n{ROLE_TOKEN}\r\n"
    ),
}


@pytest.mark.parametrize("form", list(MALFORMED_ANSWERS))
def test_h11_quotes_of_a_malformed_answer_never_reach_the_log(
    aws_sdk: AwsFiles, caplog: pytest.LogCaptureFixture, cli_logging: Any, form: str
) -> None:
    """h11 refuses the answer and quotes the bytes it refused (``illegal status line: ...``,
    ``illegal chunk header: ...``), in httpcore2's trace of the failure and in the SDK's
    ``Encountered Exception`` traceback: neither record reaches the log, and the error names the
    host only."""
    aws_sdk.write(**ROLE_PROFILE)
    caplog.set_level(logging.DEBUG)
    wire = MemoryWire(MALFORMED_ANSWERS[form].encode("ascii"))
    client = anthropic.AnthropicBedrock(
        aws_region=REGION, aws_profile="role", http_client=httpx2.Client(transport=wire), max_retries=0
    )
    with pytest.raises(LLMUnavailable) as info:
        BedrockProvider(bedrock_cfg(aws_profile="role"), client=client).classify(system="S", prompt="P")
    assert str(info.value) == (
        f"Cannot reach Amazon Bedrock ({HOST}) during the incident classification: APIConnectionError"
    )
    assert records_of(caplog, "httpcore2")  # the transport did log the request
    assert leaked(caplog.text, ROLE_TOKEN, ROLE_SECRET) == []
    assert "illegal" not in caplog.text.lower()


def test_a_long_aws_message_stays_on_one_line(aws_keys: AwsFiles) -> None:
    """AWS's message, cut to about 300 characters, keeps its head and its tail on one line: the
    error goes into the incident and the worker's warning, where a line break would let the tail
    start a log line of its own."""
    message = "x" * 200 + " " + "y" * 150 + " 2026-10-28 08:30:00 WARNING dag_healer: forged entry"
    api = FakeBedrock(aws_error(400, message))
    with pytest.raises(LLMUnavailable) as info:
        provider(api).classify(system="S", prompt="P")
    error = str(info.value)
    assert error.startswith("Amazon Bedrock error 400 during the incident classification: xxx")
    assert "...[truncated]..." in error and error.endswith("forged entry")
    assert "\n" not in error and "\r" not in error


# --------------------------------------------------------------------------- proxies


class ProxyWire:
    """httpx2's network transport, patched through its public constructor and method: each
    transport keeps whether it was built for a proxy (its ``proxy`` argument), and every request
    is answered with a classification and kept with its URL and that flag. Nothing leaves the
    process."""

    def __init__(self, monkeypatch: pytest.MonkeyPatch) -> None:
        self.sent: list[tuple[str, bool]] = []
        self.proxied: dict[int, bool] = {}
        build = httpx2.HTTPTransport.__init__
        signature = inspect.signature(build)

        def init(transport: httpx2.HTTPTransport, *args: Any, **kwargs: Any) -> None:
            proxy = signature.bind(transport, *args, **kwargs).arguments.get("proxy")
            self.proxied[id(transport)] = proxy is not None
            build(transport, *args, **kwargs)

        monkeypatch.setattr(httpx2.HTTPTransport, "__init__", init)
        monkeypatch.setattr(
            httpx2.HTTPTransport,
            "handle_request",
            lambda transport, request: self.respond(transport, request),
        )

    def respond(self, transport: httpx2.HTTPTransport, request: httpx2.Request) -> httpx2.Response:
        self.sent.append((str(request.url), self.proxied[id(transport)]))
        return httpx2.Response(200, json=answer([text(CLASSIFICATION_JSON)]), request=request)


@pytest.mark.parametrize(
    ("setting", "endpoint", "proxied"),
    [
        ("llm", "http://127.0.0.1:8080", False),
        ("llm", "http://localhost:8080/bedrock", False),
        ("llm", "http://[::1]:8080", False),
        ("variable", "http://127.0.0.1:8080", False),
        ("llm", VPC_ENDPOINT, True),
    ],
    ids=["loopback", "localhost", "ipv6-loopback", "sdk-variable", "https-keeps-the-proxy"],
)
def test_a_loopback_http_endpoint_never_goes_through_a_proxy(
    aws_keys: AwsFiles, monkeypatch: pytest.MonkeyPatch, setting: str, endpoint: str, proxied: bool
) -> None:
    """With ``HTTP_PROXY`` and ``ALL_PROXY`` set, the proxy would receive the signed requests and
    the prompts of an ``http`` endpoint in cleartext; an ``https`` endpoint keeps the proxy, through
    which TLS runs end to end (that case also proves that the wire tells a proxy apart)."""
    for name in PROXY_VARIABLES:
        monkeypatch.delenv(name, raising=False)
        monkeypatch.delenv(name.lower(), raising=False)
    for name in ("HTTP_PROXY", "HTTPS_PROXY", "ALL_PROXY"):
        monkeypatch.setenv(name, "http://proxy.example.net:3128")
    wire = ProxyWire(monkeypatch)
    if setting == "variable":
        monkeypatch.setenv("ANTHROPIC_BEDROCK_BASE_URL", endpoint)
    cfg = bedrock_cfg(endpoint_url=endpoint if setting == "llm" else None)
    assert BedrockProvider(cfg).classify(system="S", prompt="P")[0].category == "code"
    assert wire.sent == [(endpoint + INVOKE_PATH, proxied)]


# --------------------------------------------------------------------------- refusals in a repair


@pytest.mark.parametrize(
    ("category", "shown"),
    [
        ("cyber", "cyber"),
        ("lowercaserunofthetoken", "unspecified"),
        (TYPED_TOKEN[:32], "unspecified"),
        ("Cyber", "unspecified"),
        ("x" * 33, "unspecified"),
        (None, "unspecified"),
    ],
    ids=["cyber", "token-run", "token-cut", "capital", "too-long", "none"],
)
def test_a_refusal_in_a_repair_logs_its_category_only_when_it_is_shaped_like_one(
    aws_sdk: AwsFiles, caplog: pytest.LogCaptureFixture, category: str | None, shown: str
) -> None:
    """The log line of a refusal in a repair session goes through the check of the classification
    (one helper for both providers): ``token-run`` is shaped like a category, but cut from the
    session token of the profile."""
    aws_sdk.write(**TYPED_PROFILE)
    caplog.set_level(logging.WARNING, logger="dag_healer.agent.llm")
    details = {"type": "refusal", "category": category, "explanation": None}
    api = FakeBedrock(answer([], stop_reason="refusal", stop_details=details))
    prov = BedrockProvider(bedrock_cfg(aws_profile="typed"), client=api.client(aws_profile="typed"))
    result = prov.repair_session(
        system="S", prompt="P", tools=make_tools([]), max_tokens_budget=10_000, scenario={"phase": "fix"}
    )
    assert result.stop_reason == "refusal"
    declined = [
        record.getMessage()
        for record in caplog.records
        if record.name == "dag_healer.agent.llm" and record.levelno == logging.WARNING
    ]
    assert declined == [f"Claude declined the repair session (refusal category: {shown})"]
    assert leaked(caplog.text, TYPED_TOKEN) == []


# --------------------------------------------------------------------------- other fields of an answer


@pytest.mark.parametrize("stop_reason", ["end_turn", TYPED_TOKEN])
def test_a_repair_logs_only_the_answer_fields_it_knows(
    aws_sdk: AwsFiles, caplog: pytest.LogCaptureFixture, cli_logging: Any, stop_reason: str
) -> None:
    """The SDK builds an answer without checking its fields: the endpoint could put an echo of a
    credential, here the profile's session token, in the stop reason, in a fallback block and in
    the model of an answer that a fallback served. The worker's line at the end of the session
    names a stop reason of Claude's only (``other`` otherwise), the bedrock provider logs no
    server-side fallback (Bedrock has none), and the SDK's own record of the stop reason
    (``-vv``) is dropped."""
    aws_sdk.write(**TYPED_PROFILE)
    caplog.set_level(logging.DEBUG)
    body = answer(
        [{"type": "fallback", "from": {"model": TYPED_TOKEN}, "to": {"model": TYPED_TOKEN}}, text("done")],
        stop_reason=stop_reason,
        model=TYPED_TOKEN,
        usage={
            "input_tokens": 10,
            "output_tokens": 5,
            "iterations": [{"type": "fallback_message", "input_tokens": 10, "output_tokens": 5}],
        },
    )
    api = FakeBedrock(body)
    prov = BedrockProvider(bedrock_cfg(aws_profile="typed"), client=api.client(aws_profile="typed"))
    result = prov.repair_session(
        system="S", prompt="P", tools=make_tools([]), max_tokens_budget=10_000, scenario={"phase": "fix"}
    )
    assert (result.final_text, result.tokens_used) == ("done", 15)
    shown = "end_turn" if stop_reason == "end_turn" else "other"
    finished = [item for item in records_of(caplog, "dag_healer") if item.startswith("Claude repair session")]
    assert finished[-1] == f"Claude repair session finished: stop_reason={shown}, 0 tool calls, 15 tokens"
    assert not [item for item in records_of(caplog, "dag_healer") if "fallback" in item.lower()]
    assert leaked(caplog.text, TYPED_TOKEN) == []
