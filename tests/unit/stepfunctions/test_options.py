"""``orchestrator.options`` of the ``stepfunctions`` type (``stepfunctions/options.py``, section 9.14).

The options of the ``dag-healer init`` template and of the design's example ``healer.yaml``
validate (and build an adapter); defaults, normalisation and every validation error.
"""

from __future__ import annotations

from collections.abc import Iterator
from pathlib import Path
from typing import Any

import pytest
import yaml
from pydantic import ValidationError

from dag_healer import config as config_module
from dag_healer.config import config_from_mapping, default_config_yaml
from dag_healer.orchestrators import registry
from dag_healer.stepfunctions import FACTORY
from dag_healer.stepfunctions.options import (
    DEFAULT_EXECUTION_URL,
    DEFAULT_LOG_URL,
    DEFAULT_STATE_MACHINE_URL,
    DEFAULT_STATUSES,
    FAILED_STATUSES,
    LINK_PLACEHOLDERS,
    CodeMapEntry,
    LinkTemplates,
    PipelineOptions,
    StepFunctionsOptions,
)

from .conftest import ACCOUNT, CUSTOMERS_SM, PIPELINES, REGION, SALES_SM

QUEUE_URL = f"https://sqs.{REGION}.amazonaws.com/{ACCOUNT}/healer-sfn-events"

#: The example ``healer.yaml`` of section 9.14 of the design, verbatim.
SECTION_9_14 = """\
version: 1
repo: {path: ., base_branch: main, dags_dir: statemachines, allowed_paths: ["functions/", "src/"]}
orchestrator:
  type: stepfunctions
  instance: sfn-prod-euw1
  options:
    region: eu-west-1
    profile: null                   # local dev; else the default boto3 chain (ECS task role, IRSA, env)
    role_arn: null                  # optional AssumeRole (+ role_external_id, a secret option)
    endpoint_url: null              # VPC endpoint / LocalStack (NOT Step Functions Local: unsupported)
    template: template.yaml         # SAM template for the CodeLocator
    pipelines:
      sales_daily_etl:
        state_machine: arn:aws:states:eu-west-1:123456789012:stateMachine:sales-daily-etl-AbC123
        logical_id: SalesDailyEtlStateMachine
    discover: false
    statuses: [FAILED, TIMED_OUT]
    lookback_hours: 24
    history_max_events: 5000
    max_histories_per_poll: 50
    queue_url: "${HEALER_SFN_QUEUE_URL}"
    log_lookup: true
    retry_strategy: redrive         # redrive | disabled
    rerun: false
    code_map: {}                    # {LogicalIdOrFunctionName: {path: functions/x, handler: app.handler}}
    links: {}                       # overrides of the templates in 9.11
monitor: {dags: ["*"], modes: [poll, queue], poll_interval_seconds: 60, trigger: final_failure}
datasets:
  sales_daily_etl:
    TransformSales: {type: csv, path: "s3://acme-landing/incoming/sales.csv"}
serve:
  public_url: https://healer.example.com
  auth:
    backend: alb_oidc
    alb_arn: arn:aws:elasticloadbalancing:eu-west-1:123456789012:loadbalancer/app/healer/abc
    region: eu-west-1
    groups_claim: "cognito:groups"
    rules:
      - {principals: ["group:data-eng"], pipelines: ["*"], actions: [read, act, approve_contract]}
      - {principals: ["authenticated"], pipelines: ["sales_*"], actions: [read]}
store: {url: "${HEALER_DATABASE_URL}"}
ingest: {token: "${HEALER_INGEST_TOKEN}"}
"""


@pytest.fixture(autouse=True)
def _restore_shared_state(monkeypatch: pytest.MonkeyPatch) -> Iterator[None]:
    """Loading a configuration extends ``config.SECRET_FIELDS`` and may cache an adapter factory."""
    monkeypatch.setattr(config_module, "SECRET_FIELDS", config_module.SECRET_FIELDS)
    yield
    registry.reset_adapters()


def _load(text: str, tmp_path: Path, **env: str) -> Any:
    return config_from_mapping(yaml.safe_load(text), base_dir=tmp_path, env=env)


def _invalid(**data: Any) -> str:
    with pytest.raises(ValidationError) as raised:
        StepFunctionsOptions.model_validate(data)
    return str(raised.value)


# =========================================================================== the two healer.yaml files


def test_the_init_template_options_validate_and_build_an_adapter(tmp_path: Path) -> None:
    cfg = _load(default_config_yaml(orchestrator="stepfunctions"), tmp_path)
    options = StepFunctionsOptions.model_validate(cfg.orchestrator.options)
    assert options.region == "eu-west-1" and options.template == "template.yaml"
    assert list(options.pipelines) == ["sales_daily_etl"]
    assert options.pipelines["sales_daily_etl"].logical_id == "SalesDailyEtlStateMachine"
    assert options.statuses == ["FAILED", "TIMED_OUT"]
    assert (options.discover, options.rerun, options.retry_strategy, options.log_lookup) == (
        False,
        False,
        "redrive",
        True,
    )
    assert (options.lookback_hours, options.history_max_events, options.max_histories_per_poll) == (
        24,
        5000,
        50,
    )
    assert options.queue_url is None  # ${HEALER_SFN_QUEUE_URL} is unset: the option is left out
    assert (options.profile, options.role_arn, options.endpoint_url) == (None, None, None)
    assert options.code_map == {} and options.links == LinkTemplates()
    adapter = FACTORY.create(cfg, client=lambda service: None)
    assert adapter.instance == "sfn-prod-euw1" and adapter.options == options


def test_the_section_9_14_example_validates(tmp_path: Path) -> None:
    cfg = _load(SECTION_9_14, tmp_path, HEALER_SFN_QUEUE_URL=QUEUE_URL)
    options = StepFunctionsOptions.model_validate(cfg.orchestrator.options)
    assert options.queue_url == QUEUE_URL
    machine = options.pipelines["sales_daily_etl"].state_machine
    assert machine == "arn:aws:states:eu-west-1:123456789012:stateMachine:sales-daily-etl-AbC123"
    assert (
        cfg.dataset_locators("sales_daily_etl")["TransformSales"].path
        == "s3://acme-landing/incoming/sales.csv"
    )
    adapter = FACTORY.create(cfg, client=lambda service: None)
    assert adapter.instance == "sfn-prod-euw1"
    assert adapter.queue is not None and adapter.queue.queue_url == QUEUE_URL
    assert cfg.secret_literals() == []


def test_a_literal_external_id_is_a_secret_literal(tmp_path: Path) -> None:
    data = yaml.safe_load(SECTION_9_14)
    data["orchestrator"]["options"].update(
        role_arn="arn:aws:iam::123456789012:role/healer", role_external_id="s3cr3t"
    )
    cfg = config_from_mapping(data, base_dir=tmp_path, env={})
    assert "orchestrator.options.role_external_id" in cfg.secret_fields
    assert cfg.secret_literals() == ["orchestrator.options.role_external_id"]
    from_env = dict(data)
    from_env["orchestrator"] = {
        **data["orchestrator"],
        "options": {**data["orchestrator"]["options"], "role_external_id": "${HEALER_SFN_EXTERNAL_ID}"},
    }
    cfg = config_from_mapping(from_env, base_dir=tmp_path, env={"HEALER_SFN_EXTERNAL_ID": "from-env"})
    assert cfg.orchestrator.options["role_external_id"] == "from-env"
    assert cfg.secret_literals() == []


# =========================================================================== defaults and normalisation


def test_defaults() -> None:
    options = StepFunctionsOptions()
    assert options.region is None and options.pipelines == {} and options.discover is False
    assert options.template == "template.yaml" and options.role_session_name == "dag-healer"
    assert options.statuses == list(DEFAULT_STATUSES) == ["FAILED", "TIMED_OUT"]
    assert set(DEFAULT_STATUSES) < set(FAILED_STATUSES) == {"FAILED", "TIMED_OUT", "ABORTED"}
    assert (options.retry_strategy, options.rerun, options.log_lookup, options.queue_url) == (
        "redrive",
        False,
        True,
        None,
    )
    assert options.links.console_execution_url == DEFAULT_EXECUTION_URL
    assert options.links.console_state_machine_url == DEFAULT_STATE_MACHINE_URL
    assert options.links.cloudwatch_log_url == DEFAULT_LOG_URL


@pytest.mark.parametrize("name", ["region", "profile", "role_arn", "endpoint_url", "template", "queue_url"])
def test_blank_text_means_unset(name: str) -> None:
    assert getattr(StepFunctionsOptions.model_validate({name: "   "}), name) is None


def test_endpoints_per_service() -> None:
    """``endpoint_urls`` names one endpoint per service (VPC endpoints) and wins over the shared
    ``endpoint_url`` (LocalStack); a service with neither is left to botocore."""
    vpce = "https://vpce-0a1b2c3d.states.eu-west-1.vpce.amazonaws.com"
    options = StepFunctionsOptions.model_validate(
        {"endpoint_urls": {"stepfunctions": f" {vpce} ", "logs": " "}}
    )
    assert options.endpoint_urls == {"stepfunctions": vpce}  # stripped; a blank value is no endpoint
    assert (
        options.endpoint_for("stepfunctions"),
        options.endpoint_for("logs"),
        options.endpoint_for("sts"),
    ) == (
        vpce,
        None,
        None,
    )
    shared = StepFunctionsOptions(
        endpoint_url="http://localhost:4566", endpoint_urls={"sts": "http://sts.test"}
    )
    assert (shared.endpoint_for("sts"), shared.endpoint_for("sqs")) == (
        "http://sts.test",
        "http://localhost:4566",
    )
    assert StepFunctionsOptions().endpoint_urls == {}


@pytest.mark.parametrize(
    ("template", "expected"),
    [
        ("./template.yaml", "template.yaml"),
        (".\\infra\\template.yaml", "infra/template.yaml"),
        ("././sam/template.yml", "sam/template.yml"),
        ("./", None),
        (None, None),
    ],
)
def test_the_template_path_is_posix_and_repo_relative(template: str | None, expected: str | None) -> None:
    assert StepFunctionsOptions.model_validate({"template": template}).template == expected


def test_statuses_are_deduplicated_in_order() -> None:
    options = StepFunctionsOptions.model_validate({"statuses": ["ABORTED", "FAILED", "ABORTED"]})
    assert options.statuses == ["ABORTED", "FAILED"]


def test_pipelines() -> None:
    options = StepFunctionsOptions.model_validate(
        {
            "pipelines": {
                **PIPELINES,
                "by_name": {"state_machine": "  customers-sync  ", "logical_id": " ", "definition": ""},
                "qualified": {"state_machine": f"{SALES_SM}:7"},
                "aliased": {"state_machine": f"{CUSTOMERS_SM}:prod", "definition": "statemachine/x.asl.json"},
                "a.b-c_9": {"state_machine": "x"},
            }
        }
    )
    assert list(options.pipelines)[:3] == list(PIPELINES)
    by_name = options.pipelines["by_name"]
    assert (by_name.state_machine, by_name.logical_id, by_name.definition) == ("customers-sync", None, None)
    assert options.pipelines["qualified"].state_machine == f"{SALES_SM}:7"
    assert options.pipelines["aliased"].definition == "statemachine/x.asl.json"


def test_code_map_paths_are_posix_and_repo_relative() -> None:
    options = StepFunctionsOptions.model_validate(
        {
            "code_map": {
                "SalesFunction": {
                    "path": "./functions/sales/",
                    "handler": "app.handler",
                    "layers": ["./src/", "."],
                },
                "sfn-etl-CustomersFunction-7G8H9I0J1K2L": {"path": "."},
                "Root": {"path": ".\\functions\\inventory"},
            }
        }
    )
    sales = options.code_map["SalesFunction"]
    assert (sales.path, sales.handler, sales.layers) == ("functions/sales", "app.handler", ["src", ""])
    assert options.code_map["sfn-etl-CustomersFunction-7G8H9I0J1K2L"] == CodeMapEntry(path="")
    assert options.code_map["Root"].path == "functions/inventory"


def test_link_templates_are_partial_overrides() -> None:
    links = LinkTemplates.model_validate(
        {"cloudwatch_log_url": " https://logs.example.test/{log_group_enc} "}
    )
    assert links.cloudwatch_log_url == "https://logs.example.test/{log_group_enc}"
    assert links.console_execution_url == DEFAULT_EXECUTION_URL
    every = "https://x.test/" + "/".join(f"{{{name}}}" for name in sorted(LINK_PLACEHOLDERS))
    assert LinkTemplates(console_execution_url=every).console_execution_url == every


def test_the_external_id_is_kept_but_never_shown() -> None:
    options = StepFunctionsOptions.model_validate(
        {"role_arn": "arn:aws:iam::123456789012:role/healer", "role_external_id": "s3cr3t-external-id"}
    )
    assert options.role_external_id == "s3cr3t-external-id"
    assert "s3cr3t-external-id" not in repr(options) and "s3cr3t-external-id" not in str(options)
    assert options.model_dump()["role_external_id"] == "s3cr3t-external-id"


def test_a_rejected_external_id_never_appears_in_the_validation_error() -> None:
    message = _invalid(
        role_arn="arn:aws:iam::123456789012:role/healer", role_external_id=["s3cr3t-external-id"]
    )
    assert "role_external_id" in message and "s3cr3t-external-id" not in message


def test_the_factory_names_its_model_and_its_secret() -> None:
    assert FACTORY.options_model is StepFunctionsOptions
    assert FACTORY.secret_options == ("role_external_id",)


# =========================================================================== validation errors


@pytest.mark.parametrize(
    ("data", "message"),
    [
        ({"region": "eu-west"}, "not an AWS region name"),
        ({"region": "EU-WEST-1"}, "not an AWS region name"),
        ({"region": "europe"}, "not an AWS region name"),
        ({"role_arn": "arn:aws:iam::123:role/x"}, "role_arn must be an IAM role ARN"),
        ({"role_arn": "arn:aws:iam::123456789012:group/x"}, "role_arn must be an IAM role ARN"),
        ({"role_external_id": "x"}, "role_external_id is set but role_arn is not"),
        ({"role_session_name": "x"}, "role_session_name must be 2-64 characters"),
        ({"role_session_name": "dag healer"}, "role_session_name must be 2-64 characters"),
        ({"role_session_name": "s" * 65}, "role_session_name must be 2-64 characters"),
        ({"endpoint_url": "localhost:4566"}, "expected an absolute http(s) URL"),
        (
            {"endpoint_urls": {"stepfunctions": "vpce.test"}},
            "endpoint_urls.stepfunctions: expected an absolute",
        ),
        ({"endpoint_urls": {"states": "https://x.test"}}, "endpoint_urls.states"),
        ({"queue_url": f"sqs.{REGION}.amazonaws.com/{ACCOUNT}/q"}, "expected an absolute http(s) URL"),
        ({"statuses": []}, "at least one status is needed"),
        ({"statuses": ["RUNNING"]}, "statuses.0"),
        ({"retry_strategy": "retry"}, "retry_strategy"),
        ({"lookback_hours": 0}, "lookback_hours"),
        ({"lookback_hours": 24 * 90 + 1}, "lookback_hours"),
        ({"history_max_events": 99}, "history_max_events"),
        ({"history_max_events": 25_001}, "history_max_events"),
        ({"max_histories_per_poll": 0}, "max_histories_per_poll"),
        ({"max_histories_per_poll": 1001}, "max_histories_per_poll"),
        ({"unknown_option": True}, "Extra inputs are not permitted"),
        ({"pipelines": {".hidden": {"state_machine": "x"}}}, "invalid pipeline id"),
        ({"pipelines": {"-dash": {"state_machine": "x"}}}, "invalid pipeline id"),
        ({"pipelines": {"with space": {"state_machine": "x"}}}, "invalid pipeline id"),
        ({"pipelines": {"p" * 251: {"state_machine": "x"}}}, "invalid pipeline id"),
        ({"pipelines": {"p": {}}}, "state_machine"),
        ({"pipelines": {"p": {"state_machine": "  "}}}, "state_machine must not be empty"),
        ({"pipelines": {"p": {"state_machine": "x", "type": "STANDARD"}}}, "Extra inputs are not permitted"),
        ({"code_map": {"Fn": {"handler": "app.handler"}}}, "code_map.Fn.path"),
        ({"code_map": {"Fn": {"path": "x", "runtime": "python3.11"}}}, "Extra inputs are not permitted"),
        ({"links": {"console_execution_url": "/relative/{execution_arn}"}}, "absolute http(s) URL"),
        (
            {"links": {"console_execution_url": "https://x.test/{execution_id}"}},
            "unknown placeholder(s) execution_id",
        ),
        ({"links": {"cloudwatch_log_url": "https://x.test/a b/{log_group}"}}, "must not contain whitespace"),
        ({"links": {"unknown_url": "https://x.test/"}}, "Extra inputs are not permitted"),
    ],
)
def test_invalid_options(data: dict[str, Any], message: str) -> None:
    assert message in _invalid(**data)


@pytest.mark.parametrize(
    ("state_machine", "message"),
    [
        (f"arn:aws:lambda:{REGION}:{ACCOUNT}:function:fn", "state_machine must be a state machine ARN"),
        (
            f"arn:aws:states:{REGION}:{ACCOUNT}:execution:customers-sync:run",
            "state_machine must be a state machine ARN",
        ),
        (f"arn:aws:states:{REGION}:{ACCOUNT}:stateMachine", "state_machine must be a state machine ARN"),
        ("arn:aws:states", "state_machine must be a state machine ARN"),
        (f"{CUSTOMERS_SM}/ItemsLabel", "Distributed Map child state machine cannot be a pipeline"),
        ("my machine", "is not a state machine name"),
        ("etl/daily", "is not a state machine name"),
        ("etl:daily", "is not a state machine name"),
        ("etl$", "is not a state machine name"),
        ("x" * 81, "is not a state machine name"),
    ],
)
def test_invalid_state_machines(state_machine: str, message: str) -> None:
    with pytest.raises(ValidationError) as raised:
        PipelineOptions(state_machine=state_machine)
    assert message in str(raised.value)
    assert message in _invalid(pipelines={"p": {"state_machine": state_machine}})


@pytest.mark.parametrize(
    "name", ["customers-sync", "SalesDailyEtlStateMachine-AbC123", "etl_v2.daily", "x" * 80, "état"]
)
def test_valid_state_machine_names(name: str) -> None:
    assert PipelineOptions(state_machine=name).state_machine == name


@pytest.mark.parametrize("name", ["dag-healer", " healer-worker ", "a.b@example,x=y+z_1", "s" * 64])
def test_valid_role_session_names(name: str) -> None:
    assert StepFunctionsOptions(role_session_name=name).role_session_name == name.strip()
