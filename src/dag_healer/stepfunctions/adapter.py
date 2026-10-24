"""The AWS Step Functions adapter: one account and region as an :class:`OrchestratorAdapter`.

=====================  ===========================================================================
Port                   Step Functions (``docs/architecture/orchestrators.md``, section 9)
=====================  ===========================================================================
``feed``               :class:`~dag_healer.stepfunctions.feed.SfnFeed`: ``ListExecutions`` per
                       watched machine and status, localised from the history
``push``               :class:`~dag_healer.stepfunctions.intake.SfnIntake`: EventBridge status
                       changes (API destination or SQS), confirmed with ``DescribeExecution``
``queue``              :class:`~dag_healer.stepfunctions.queue.SfnQueue` (with ``queue_url``)
``inspector``          :class:`~dag_healer.stepfunctions.inspector.SfnInspector`: history, Lambda
                       cause -> traceback, CloudWatch log excerpt, the definition that ran
``code``               :class:`~dag_healer.stepfunctions.locator.SfnCodeLocator`: SAM + ``code_map``
``remediator``         :class:`~dag_healer.stepfunctions.remediation.SfnRemediator`: redrive,
                       opt-in restart (``None`` when both are disabled)
``links``              :class:`~dag_healer.stepfunctions.links.SfnLinks`: console templates
=====================  ===========================================================================

Construction does no I/O: the AWS clients are built on first use (or injected: an
:class:`~dag_healer.stepfunctions.aws.AwsClients`, a ``client_factory`` callable, or an object
with a ``client_factory`` method such as :class:`~dag_healer.stepfunctions.testing.OfflineAws`).
The feed and the push intake share one
:class:`~dag_healer.stepfunctions.feed.HistoryLocalizer`: a failure localised by one is known to
the other, and both count the checks of an unusable history together.
Retries are invisible (``attempts_visible=False``) and a merged fix reaches AWS only through CI
(``fix_delivery="ci_deploy"``). Texts handed to the core are RAW: the core masks them, and the
adapter never logs causes, log lines or execution input.
"""

from __future__ import annotations

import logging
from collections.abc import Callable, Mapping
from datetime import datetime
from pathlib import Path
from typing import TYPE_CHECKING, Any

from dag_healer.core.ports import DoctorCheck, HealthReport
from dag_healer.core.vocab import Capabilities, RemediationKind, Vocabulary
from dag_healer.stepfunctions.aws import AwsClients, call, translate_error
from dag_healer.stepfunctions.doctor import doctor_checks
from dag_healer.stepfunctions.feed import HistoryLocalizer, SfnFeed
from dag_healer.stepfunctions.inspector import SfnInspector
from dag_healer.stepfunctions.intake import SfnIntake
from dag_healer.stepfunctions.links import SfnLinks
from dag_healer.stepfunctions.locator import SfnCodeLocator
from dag_healer.stepfunctions.options import StepFunctionsOptions
from dag_healer.stepfunctions.queue import SfnQueue
from dag_healer.stepfunctions.remediation import SfnRemediator
from dag_healer.stepfunctions.runtime import PipelineIndex, SfnRuntime, utcnow
from dag_healer.stepfunctions.taxonomy import SFN_TAXONOMY

if TYPE_CHECKING:
    from dag_healer.config import HealerConfig

logger = logging.getLogger(__name__)

SFN_TYPE = "stepfunctions"
DISPLAY_NAME = "AWS Step Functions"
FACTORY_TARGET = "dag_healer.stepfunctions:FACTORY"
PUSH_CHANNEL = "eventbridge"
SOURCES: tuple[str, ...] = ("poll", "eventbridge", "sqs", "cli")

SFN_VOCABULARY = Vocabulary(
    product="AWS Step Functions",
    pipeline="state machine",
    pipelines="state machines",
    run="execution",
    step="state",
    steps="states",
    attempt="redrive attempt",
    map_index="item index",
    retry_action="Redrive execution",
)

#: Cloud credentials never reach the pytest sandbox (the core scrubs them too; belt and braces).
SANDBOX_ENV: dict[str, str | None] = {
    "AWS_EC2_METADATA_DISABLED": "true",
    "AWS_ACCESS_KEY_ID": None,
    "AWS_SECRET_ACCESS_KEY": None,
    "AWS_SESSION_TOKEN": None,
    "AWS_SECURITY_TOKEN": None,
    "AWS_PROFILE": None,
    "AWS_DEFAULT_PROFILE": None,
    "AWS_ROLE_ARN": None,
    "AWS_ROLE_SESSION_NAME": None,
    "AWS_WEB_IDENTITY_TOKEN_FILE": None,
    "AWS_CONTAINER_CREDENTIALS_RELATIVE_URI": None,
    "AWS_CONTAINER_CREDENTIALS_FULL_URI": None,
    "AWS_CONTAINER_AUTHORIZATION_TOKEN": None,
    "AWS_CONTAINER_AUTHORIZATION_TOKEN_FILE": None,
    "HEALER_SFN_QUEUE_URL": None,
}


def capabilities_for(options: StepFunctionsOptions) -> Capabilities:
    """The capabilities of an adapter with ``options`` (remediations follow the options)."""
    remediations: list[RemediationKind] = []
    if options.retry_strategy == "redrive":
        remediations.append(RemediationKind.RESUME_RUN)
    if options.rerun:
        remediations.append(RemediationKind.RERUN_PIPELINE)
    return Capabilities(
        sources=SOURCES,
        push_channels=(PUSH_CHANNEL,),
        attempts_visible=False,
        remediations=tuple(remediations),
        fix_delivery="ci_deploy",
        ui_hosts=("standalone",),
        native_authz=False,
    )


def as_clients(options: StepFunctionsOptions, client: Any | None) -> AwsClients:
    """An :class:`AwsClients` from what a caller injected (see the module docstring)."""
    if client is None:
        return AwsClients(options)
    if isinstance(client, AwsClients):
        return client
    factory = getattr(client, "client_factory", None)
    if callable(factory):
        return AwsClients(options, client_factory=factory)
    if callable(client):
        return AwsClients(options, client_factory=client)
    raise TypeError(
        "the stepfunctions adapter takes AwsClients, a client factory or an object with a "
        f"client_factory method as client, not {type(client).__name__}"
    )


class StepFunctionsAdapter:
    """Every port of one Step Functions instance (each port is its own small object)."""

    type = SFN_TYPE
    display_name = DISPLAY_NAME
    vocabulary = SFN_VOCABULARY
    taxonomy = SFN_TAXONOMY

    def __init__(
        self,
        cfg: HealerConfig | None,
        *,
        options: StepFunctionsOptions | None = None,
        instance: str | None = None,
        clients: AwsClients | Any | None = None,
        clock: Callable[[], datetime] | None = None,
    ) -> None:
        self._cfg = cfg
        self.options = options or StepFunctionsOptions()
        self.instance = instance or SFN_TYPE
        self.capabilities = capabilities_for(self.options)
        self.runtime = SfnRuntime(
            options=self.options,
            instance=self.instance,
            clients=as_clients(self.options, clients),
            pipelines=PipelineIndex.from_options(self.options),
            clock=clock or utcnow,
            cfg=cfg,
        )
        rt = self.runtime
        localizer = HistoryLocalizer(rt)
        self.feed = SfnFeed(rt, localizer=localizer)
        self.push = SfnIntake(rt, localizer=localizer)
        self.queue = SfnQueue(rt, self.options.queue_url) if self.options.queue_url else None
        self.code = SfnCodeLocator(cfg, self.options, pipelines=rt.pipelines)
        self.inspector = SfnInspector(rt, locator=self.code)
        self.remediator = SfnRemediator(rt) if self.capabilities.remediations else None
        self.links = SfnLinks(rt)

    def __repr__(self) -> str:
        return f"StepFunctionsAdapter(instance={self.instance!r}, region={self.options.region!r})"

    @property
    def base_url(self) -> str:
        """The Step Functions endpoint the adapter talks to (no I/O; the worker's start line):
        ``endpoint_urls.stepfunctions`` / ``endpoint_url``, else the regional endpoint, else
        ``"the default AWS region"``."""
        endpoint = self.options.endpoint_for("stepfunctions")
        if endpoint:
            return endpoint
        region = self.options.region
        if not region:
            return "the default AWS region"
        suffix = "amazonaws.com.cn" if region.startswith("cn-") else "amazonaws.com"
        return f"https://states.{region}.{suffix}"

    @property
    def clients(self) -> AwsClients:
        return self.runtime.clients

    # ------------------------------------------------------------------ the adapter
    def health(self) -> HealthReport:
        """``sts:GetCallerIdentity`` and ``DescribeStateMachine`` of the first pipeline. Never raises."""
        checks: dict[str, str] = {}
        try:
            identity = call(self.clients.sts, "get_caller_identity", what="GetCallerIdentity")
        except Exception as exc:  # health never raises
            error = translate_error(exc, what="GetCallerIdentity")
            logger.debug(
                "Step Functions adapter %s: health check failed: %s", self.instance, type(exc).__name__
            )
            return HealthReport(
                ok=False, detail=f"AWS is not reachable: {error.detail}", checks={"credentials": "error"}
            )
        checks["credentials"] = "ok"
        detail = f"AWS account {identity.get('Account') or '?'}"
        entries = self.runtime.pipelines.entries()
        if entries:
            pipeline_id = entries[0].pipeline_id
            try:
                arn = self.runtime.state_machine_arn(pipeline_id)
                if arn is None:
                    raise LookupError(f"state machine of pipeline {pipeline_id} not found")
                call(
                    self.clients.sfn,
                    "describe_state_machine",
                    what="DescribeStateMachine",
                    stateMachineArn=arn,
                )
            except Exception as exc:  # health never raises
                checks["states"] = "error"
                error = (
                    translate_error(exc, what="DescribeStateMachine")
                    if not isinstance(exc, LookupError)
                    else exc
                )
                text = getattr(error, "detail", None) or str(error)
                return HealthReport(ok=False, detail=f"{detail}; Step Functions: {text}", checks=checks)
            checks["states"] = "ok"
            detail += f"; Step Functions answers for {pipeline_id}"
        return HealthReport(ok=True, detail=detail, checks=checks)

    def doctor_checks(self) -> list[DoctorCheck]:
        """The ``dag-healer doctor`` checks (:func:`dag_healer.stepfunctions.doctor.doctor_checks`)."""
        return doctor_checks(self.runtime, self.code)

    def sandbox_env(self) -> Mapping[str, str | None]:
        """Cloud credentials out of the pytest sandbox (:data:`SANDBOX_ENV`)."""
        return dict(SANDBOX_ENV)

    def iam_policy(self, *, account: str | None = None) -> str:
        """The least-privilege IAM policies as JSON text (``doctor --print-iam-policy``).

        No AWS call: the policies come from the options, the configuration and the log groups
        the repository's SAM template sets for the functions (``LoggingConfig.LogGroup``).
        """
        from dag_healer.stepfunctions.iam import render_iam_policy

        repo_root = getattr(self._cfg, "repo_root", None) if self._cfg is not None else None
        log_groups = self.code.log_groups(Path(repo_root)) if repo_root is not None else []
        return render_iam_policy(
            self.options, cfg=self._cfg, account=account, region=self.options.region, log_groups=log_groups
        )

    def close(self) -> None:
        """Close the AWS clients (idempotent)."""
        self.clients.close()


__all__ = [
    "DISPLAY_NAME",
    "FACTORY_TARGET",
    "PUSH_CHANNEL",
    "SANDBOX_ENV",
    "SFN_TYPE",
    "SFN_VOCABULARY",
    "SOURCES",
    "StepFunctionsAdapter",
    "as_clients",
    "capabilities_for",
]
