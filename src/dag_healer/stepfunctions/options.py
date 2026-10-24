"""``orchestrator.options`` of the ``stepfunctions`` type (section 9.14 of the design).

:class:`StepFunctionsOptions` validates the options block of ``healer.yaml``::

    orchestrator:
      type: stepfunctions
      instance: sfn-prod-euw1
      options:
        region: eu-west-1
        template: template.yaml
        pipelines:
          sales_daily_etl:
            state_machine: arn:aws:states:eu-west-1:123456789012:stateMachine:sales-daily-etl-AbC123
            logical_id: SalesDailyEtlStateMachine
        statuses: [FAILED, TIMED_OUT]
        queue_url: "${HEALER_SFN_QUEUE_URL}"

Every key is optional. ``role_external_id`` is the only secret option (``FACTORY.secret_options``).
The console link templates of section 9.11 live here as well, because they are option defaults.

Endpoints: ``endpoint_url`` sends every client (Step Functions, CloudWatch Logs, Lambda, SQS, S3,
STS) to one URL, as LocalStack serves them; ``endpoint_urls`` names one endpoint per service (VPC
interface endpoints without private DNS) and wins over ``endpoint_url`` for its services. A
service with neither uses botocore's own resolution (``AWS_ENDPOINT_URL_<SERVICE>``, the
``services`` section of the AWS config file, else the regional endpoint).
"""

from __future__ import annotations

import re
from typing import Literal

from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator

from dag_healer.stepfunctions import arns

#: Console page of one execution [U: a commonly used pattern, not a documented contract].
DEFAULT_EXECUTION_URL = (
    "https://{region}.console.aws.amazon.com/states/home?region={region}"
    "#/v2/executions/details/{execution_arn}"
)
#: Console page of one state machine [U].
DEFAULT_STATE_MACHINE_URL = (
    "https://{region}.console.aws.amazon.com/states/home?region={region}"
    "#/statemachines/view/{state_machine_arn}"
)
#: CloudWatch Logs console page of one log group [U]; ``{log_group_enc}`` is the console encoding.
DEFAULT_LOG_URL = (
    "https://{region}.console.aws.amazon.com/cloudwatch/home?region={region}"
    "#logsV2:log-groups/log-group/{log_group_enc}"
)

#: The AWS services the adapter calls (botocore names: the keys of ``endpoint_urls``).
AwsService = Literal["stepfunctions", "logs", "lambda", "sqs", "s3", "sts"]

#: Execution statuses that can raise an incident (``ABORTED``: a person stopped the execution).
FAILED_STATUSES: tuple[str, ...] = ("FAILED", "TIMED_OUT", "ABORTED")
DEFAULT_STATUSES: tuple[str, ...] = ("FAILED", "TIMED_OUT")

_REGION = re.compile(r"^[a-z]{2}(-[a-z]+)+-\d+$")
_PIPELINE_ID = re.compile(r"^[A-Za-z0-9_][A-Za-z0-9_.-]{0,249}$")
_PLACEHOLDER = re.compile(r"\{([A-Za-z_][A-Za-z0-9_]*)\}")
#: A state machine name: 1-80 characters without whitespace, brackets, wildcards, the special
#: characters ``" # % \ ^ | ~ ` $ & , ; : /`` or control characters (the AWS naming rules).
_STATE_MACHINE_NAME = re.compile(r"^[^\s<>{}\[\]?*\"#%\\^|~`$&,;:/\x00-\x1f\x7f-\x9f]{1,80}$")
#: An AssumeRole ``RoleSessionName`` (the STS naming rules).
_SESSION_NAME = re.compile(r"^[A-Za-z0-9_+=,.@-]{2,64}$")
#: Placeholders a link template may use (see ``stepfunctions.links``).
LINK_PLACEHOLDERS = frozenset(
    {
        "region",
        "account",
        "partition",
        "execution_arn",
        "execution_name",
        "state_machine_arn",
        "state_machine_name",
        "log_group",
        "log_group_enc",
    }
)


class _Options(BaseModel):
    model_config = ConfigDict(extra="forbid")


def _blank_to_none(value: object) -> object:
    if isinstance(value, str) and not value.strip():
        return None
    return value


class PipelineOptions(_Options):
    """One watched pipeline: ``pipelines.<pipeline_id>``."""

    state_machine: str  # state machine ARN (preferred) or name
    logical_id: str | None = None  # SAM/CloudFormation logical id of the state machine resource
    definition: str | None = None  # repo-relative ASL file, when no IaC template describes the machine

    @field_validator("state_machine")
    @classmethod
    def _state_machine_text(cls, value: str) -> str:
        """A state machine ARN (a version or alias qualifier is accepted) or a state machine name."""
        value = value.strip()
        if not value:
            raise ValueError("state_machine must not be empty")
        if value.startswith("arn:"):
            try:
                machine = arns.parse_state_machine_arn(value)
            except ValueError:
                raise ValueError(
                    "state_machine must be a state machine ARN "
                    "(arn:aws:states:<region>:<account>:stateMachine:<name>) or a state machine name"
                ) from None
            if machine.is_map_run_child:
                raise ValueError(
                    "a Distributed Map child state machine cannot be a pipeline: configure its parent"
                )
            return value
        if not _STATE_MACHINE_NAME.match(value):
            raise ValueError(
                f"{value!r} is not a state machine name (1-80 characters without whitespace, brackets, "
                'wildcards or the characters " # % \\ ^ | ~ ` $ & , ; : /)'
            )
        return value

    @field_validator("logical_id", "definition", mode="before")
    @classmethod
    def _optional_text(cls, value: object) -> object:
        return _blank_to_none(value)


class CodeMapEntry(_Options):
    """``code_map.<LogicalIdOrFunctionName>``: where a Lambda function's code lives in the repository."""

    path: str  # repo-relative code root (what SAM calls CodeUri), "" or "." for the repository root
    handler: str | None = None  # "app.handler" / "pkg.mod.fn" / "functions/x/app.handler"
    layers: list[str] = Field(default_factory=list)  # repo-relative layer roots mapped to /opt/python

    @field_validator("path")
    @classmethod
    def _posix_path(cls, value: str) -> str:
        value = value.strip().replace("\\", "/").rstrip("/")
        while value.startswith("./"):
            value = value[2:]
        return "" if value == "." else value

    @field_validator("layers")
    @classmethod
    def _posix_layers(cls, value: list[str]) -> list[str]:
        cleaned = []
        for item in value:
            item = item.strip().replace("\\", "/").rstrip("/")
            while item.startswith("./"):
                item = item[2:]
            cleaned.append("" if item == "." else item)
        return cleaned


class LinkTemplates(_Options):
    """``links``: overrides of the console link templates (section 9.11); unset keys keep the default."""

    console_execution_url: str = DEFAULT_EXECUTION_URL
    console_state_machine_url: str = DEFAULT_STATE_MACHINE_URL
    cloudwatch_log_url: str = DEFAULT_LOG_URL

    @field_validator("console_execution_url", "console_state_machine_url", "cloudwatch_log_url")
    @classmethod
    def _template(cls, value: str) -> str:
        value = value.strip()
        if not value.startswith(("https://", "http://")):
            raise ValueError("a link template must be an absolute http(s) URL")
        if any(character.isspace() for character in value):
            raise ValueError("a link template must not contain whitespace")
        unknown = sorted(set(_PLACEHOLDER.findall(value)) - LINK_PLACEHOLDERS)
        if unknown:
            raise ValueError(
                f"unknown placeholder(s) {', '.join(unknown)}; known: {', '.join(sorted(LINK_PLACEHOLDERS))}"
            )
        return value


class StepFunctionsOptions(_Options):
    """``orchestrator.options`` of ``type: stepfunctions``. One instance is one account and region."""

    region: str | None = None
    profile: str | None = None  # local development; None: the default boto3 credential chain
    role_arn: str | None = None  # optional AssumeRole
    role_external_id: str | None = Field(default=None, repr=False)  # secret option (sts:ExternalId)
    role_session_name: str = "dag-healer"
    endpoint_url: str | None = None  # one endpoint for every client: LocalStack (not Step Functions Local)
    endpoint_urls: dict[AwsService, str] = Field(default_factory=dict)  # per service: VPC endpoints
    template: str | None = "template.yaml"  # repo-relative SAM template; None: no IaC resolver
    pipelines: dict[str, PipelineOptions] = Field(default_factory=dict)
    discover: bool = False  # also watch every STANDARD state machine (pipeline id = its name)
    statuses: list[Literal["FAILED", "TIMED_OUT", "ABORTED"]] = Field(
        default_factory=lambda: list(DEFAULT_STATUSES)
    )
    lookback_hours: int = Field(default=24, ge=1, le=24 * 90)
    history_max_events: int = Field(default=5000, ge=100, le=25_000)
    max_histories_per_poll: int = Field(default=50, ge=1, le=1000)
    queue_url: str | None = None  # SQS queue of the EventBridge rule (monitor mode "queue")
    log_lookup: bool = True  # add the Lambda's CloudWatch log excerpt to the evidence
    retry_strategy: Literal["redrive", "disabled"] = "redrive"
    rerun: bool = False  # also offer a new execution with the same input
    code_map: dict[str, CodeMapEntry] = Field(default_factory=dict)
    links: LinkTemplates = Field(default_factory=LinkTemplates)

    @field_validator(
        "region",
        "profile",
        "role_arn",
        "role_external_id",
        "endpoint_url",
        "template",
        "queue_url",
        mode="before",
    )
    @classmethod
    def _optional_text(cls, value: object) -> object:
        value = _blank_to_none(value)
        return value.strip() if isinstance(value, str) else value

    @field_validator("region")
    @classmethod
    def _region(cls, value: str | None) -> str | None:
        if value is not None and not _REGION.match(value):
            raise ValueError(f"not an AWS region name: {value!r}")
        return value

    @field_validator("role_arn")
    @classmethod
    def _role(cls, value: str | None) -> str | None:
        if value is not None and not re.match(r"^arn:aws[a-z-]*:iam::\d{12}:role/.+$", value):
            raise ValueError("role_arn must be an IAM role ARN (arn:aws:iam::<account>:role/<name>)")
        return value

    @field_validator("role_session_name")
    @classmethod
    def _session_name(cls, value: str) -> str:
        """The ``RoleSessionName`` of AssumeRole: 2-64 characters of ``[A-Za-z0-9_+=,.@-]``."""
        value = value.strip()
        if not _SESSION_NAME.match(value):
            raise ValueError("role_session_name must be 2-64 characters of letters, digits and _+=,.@-")
        return value

    @field_validator("endpoint_url", "queue_url")
    @classmethod
    def _url(cls, value: str | None) -> str | None:
        if value is not None and not value.startswith(("https://", "http://")):
            raise ValueError("expected an absolute http(s) URL")
        return value

    @field_validator("endpoint_urls")
    @classmethod
    def _service_urls(cls, value: dict[str, str]) -> dict[str, str]:
        cleaned: dict[str, str] = {}
        for service, url in value.items():
            text = url.strip() if isinstance(url, str) else url
            if not text:
                continue  # a blank value leaves the service to botocore
            if not text.startswith(("https://", "http://")):
                raise ValueError(f"endpoint_urls.{service}: expected an absolute http(s) URL")
            cleaned[service] = text
        return cleaned

    def endpoint_for(self, service: str) -> str | None:
        """The endpoint of an AWS service: ``endpoint_urls[service]``, else ``endpoint_url``."""
        own = next((url for name, url in self.endpoint_urls.items() if name == service), None)
        return own or self.endpoint_url

    @field_validator("template")
    @classmethod
    def _template(cls, value: str | None) -> str | None:
        if value is None:
            return None
        value = value.replace("\\", "/")
        while value.startswith("./"):
            value = value[2:]
        return value or None

    @field_validator("pipelines")
    @classmethod
    def _pipeline_ids(cls, value: dict[str, PipelineOptions]) -> dict[str, PipelineOptions]:
        for pipeline_id in value:
            if not _PIPELINE_ID.match(pipeline_id):
                raise ValueError(
                    f"invalid pipeline id {pipeline_id!r}: letters, digits, '_', '.', '-' "
                    "(at most 250 characters, not starting with '.' or '-')"
                )
        return value

    @field_validator("statuses")
    @classmethod
    def _statuses(cls, value: list[str]) -> list[str]:
        unique = list(dict.fromkeys(value))
        if not unique:
            raise ValueError("at least one status is needed (FAILED, TIMED_OUT, ABORTED)")
        return unique

    @model_validator(mode="after")
    def _external_id_needs_a_role(self) -> StepFunctionsOptions:
        if self.role_external_id and not self.role_arn:
            raise ValueError("role_external_id is set but role_arn is not")
        return self


__all__ = [
    "AwsService",
    "DEFAULT_EXECUTION_URL",
    "DEFAULT_LOG_URL",
    "DEFAULT_STATE_MACHINE_URL",
    "DEFAULT_STATUSES",
    "FAILED_STATUSES",
    "LINK_PLACEHOLDERS",
    "CodeMapEntry",
    "LinkTemplates",
    "PipelineOptions",
    "StepFunctionsOptions",
]
