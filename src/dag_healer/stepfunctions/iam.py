"""The least-privilege IAM policies of the adapter (section 9.10; printed by
``dag-healer doctor --print-iam-policy``). Verify them against the Service Authorization
Reference before use.

Two roles: a **read** role for the worker (and the standalone host) and an opt-in **write** role
(``states:RedriveExecution``, and ``states:StartExecution`` when ``rerun`` is on). The write role
adds to the read role, it does not replace it: both remediations read the execution first
(``states:DescribeExecution``: a redrive checks that the incident's attempt is still the
execution's current one, a restart reads the input). Resources are
scoped to the configured state machines (``stateMachine:<name>``, ``execution:<name>:*``);
``discover: true`` adds ``states:ListStateMachines`` on ``*`` and widens the machine resources
to ``*`` (a pipeline configured by name needs ``ListStateMachines`` too: its ARN is resolved
with it). Queue mode adds the SQS actions on the queue; ``log_lookup`` adds
``logs:FilterLogEvents`` (and ``lambda:GetFunction`` for the log group) on the Lambda log groups
(``/aws/lambda/*``) and functions of the account, because their deployed names are only known
at run time (narrow them to your functions), plus the ``log_groups`` functions set with
``LoggingConfig.LogGroup`` (the adapter passes the ones its SAM template names). S3 datasets
(``s3://`` locators of ``healer.yaml`` for the watched pipelines, every pipeline with
``discover``) add ``s3:GetObject``/``s3:GetObjectVersion`` on their objects (``<prefix>*`` for a
key ending in ``/``); KMS key ARNs add ``kms:Decrypt``.

Statements, actions and resources come in a fixed order (configuration order for resources),
so the printed policy is stable. Nothing here does I/O: the policies are built from the options
and the configuration only.
"""

from __future__ import annotations

import json
import re
from collections.abc import Iterable, Mapping, Sequence
from dataclasses import dataclass
from typing import Any
from urllib.parse import urlsplit

from dag_healer.stepfunctions import arns
from dag_healer.stepfunctions.options import StepFunctionsOptions

POLICY_VERSION = "2012-10-17"
ACCOUNT_PLACEHOLDER = "<account-id>"
REGION_PLACEHOLDER = "<region>"
#: The machine name used when no pipeline is configured (a template to fill in).
STATE_MACHINE_PLACEHOLDER = "<state-machine-name>"
QUEUE_PLACEHOLDER = "<queue-name>"

READ_STATE_MACHINE_ACTIONS: tuple[str, ...] = ("states:DescribeStateMachine", "states:ListExecutions")
READ_EXECUTION_ACTIONS: tuple[str, ...] = (
    "states:DescribeExecution",
    "states:DescribeStateMachineForExecution",
    "states:GetExecutionHistory",
)
LIST_STATE_MACHINES_ACTIONS: tuple[str, ...] = ("states:ListStateMachines",)
LOGS_ACTIONS: tuple[str, ...] = ("logs:FilterLogEvents",)
LAMBDA_ACTIONS: tuple[str, ...] = ("lambda:GetFunction",)
QUEUE_ACTIONS: tuple[str, ...] = (
    "sqs:ChangeMessageVisibility",
    "sqs:DeleteMessage",
    "sqs:GetQueueAttributes",
    "sqs:ReceiveMessage",
)
DATASET_ACTIONS: tuple[str, ...] = ("s3:GetObject", "s3:GetObjectVersion")
KMS_ACTIONS: tuple[str, ...] = ("kms:Decrypt",)
REDRIVE_ACTIONS: tuple[str, ...] = ("states:RedriveExecution",)
START_ACTIONS: tuple[str, ...] = ("states:StartExecution",)

#: The default log groups of Lambda functions (``/aws/lambda/<function name>``).
LAMBDA_LOG_GROUP_PREFIX = "/aws/lambda/"
LAMBDA_LOG_GROUPS = f"{LAMBDA_LOG_GROUP_PREFIX}*"

_S3_SCHEME = "s3://"
_REGION_IN_HOST = re.compile(r"(?:^|\.)([a-z]{2}(?:-[a-z]+)+-\d+)\.")
_ACCOUNT = re.compile(r"^\d{12}$")


@dataclass(frozen=True)
class _Scope:
    """Where a resource lives: partition, region and account (values or placeholders)."""

    partition: str
    region: str
    account: str

    def arn(self, service: str, resource: str, *, region: bool = True, account: bool = True) -> str:
        return (
            f"arn:{self.partition}:{service}:{self.region if region else ''}:"
            f"{self.account if account else ''}:{resource}"
        )


def _configured_machines(options: StepFunctionsOptions) -> list[arns.StateMachineArn | str]:
    """Each pipeline's machine: the parsed ARN, or its name when configured by name (a value
    that looks like an ARN but is not a state machine ARN names no resource and is skipped)."""
    machines: list[arns.StateMachineArn | str] = []
    for item in options.pipelines.values():
        value = item.state_machine
        if not arns.is_arn(value):
            machines.append(value.strip())
            continue
        try:
            machines.append(arns.parse_state_machine_arn(value))
        except ValueError:
            continue
    return machines


def _default_scope(options: StepFunctionsOptions, account: str | None, region: str | None) -> _Scope:
    """The scope of everything the configured ARNs do not name (explicit values first)."""
    parsed = [m for m in _configured_machines(options) if isinstance(m, arns.StateMachineArn)]
    chosen_region = (
        region or options.region or next((m.region for m in parsed if m.region), None) or REGION_PLACEHOLDER
    )
    chosen_account = account or next((m.account for m in parsed if m.account), None) or ACCOUNT_PLACEHOLDER
    partition = next((m.partition for m in parsed if m.partition), None)
    if partition is None:
        partition = arns.partition_for_region(chosen_region if chosen_region != REGION_PLACEHOLDER else None)
    return _Scope(partition, chosen_region, chosen_account)


def _unique(values: Iterable[str]) -> list[str]:
    return list(dict.fromkeys(value for value in values if value))


def _statement(sid: str, actions: Sequence[str], resources: Sequence[str] | str) -> dict[str, Any]:
    resource: list[str] | str = resources if isinstance(resources, str) else _unique(resources)
    return {"Sid": sid, "Effect": "Allow", "Action": sorted(set(actions)), "Resource": resource}


def _policy(statements: Sequence[dict[str, Any]]) -> dict[str, Any]:
    return {"Version": POLICY_VERSION, "Statement": list(statements)}


# =========================================================================== resources


def _machine_resources(options: StepFunctionsOptions, scope: _Scope) -> tuple[list[str], list[str]]:
    """``(state machine ARNs, execution ARN patterns)`` of the watched machines."""
    machines: list[str] = []
    executions: list[str] = []
    if options.discover:
        machines.append(scope.arn("states", "stateMachine:*"))
        executions.append(scope.arn("states", "execution:*"))
    configured = _configured_machines(options)
    if not configured and not options.discover:
        configured = [STATE_MACHINE_PLACEHOLDER]
    for machine in configured:
        if isinstance(machine, arns.StateMachineArn):
            own = _Scope(machine.partition, machine.region, machine.account)
            name = machine.name
        else:
            own, name = scope, machine
        if options.discover and own == scope:
            continue  # covered by the wildcards above
        machines.append(own.arn("states", f"stateMachine:{name}"))
        executions.append(own.arn("states", f"execution:{name}:*"))
    return machines, executions


def _needs_name_resolution(options: StepFunctionsOptions) -> bool:
    """True when a pipeline names its machine (``ListStateMachines`` resolves the ARN)."""
    return any(not arns.is_arn(item.state_machine) for item in options.pipelines.values())


def queue_arn(queue_url: str, *, partition: str = "aws", region: str, account: str) -> str:
    """The ARN of an SQS queue URL (``https://sqs.<region>.amazonaws.com/<account>/<name>``,
    legacy ``<region>.queue.amazonaws.com``, VPC endpoint or LocalStack URLs); the region and
    the account fall back to the given values when the URL does not name them."""
    parts = urlsplit(queue_url.strip())
    segments = [segment for segment in parts.path.split("/") if segment]
    name = segments[-1] if segments else QUEUE_PLACEHOLDER
    owner = segments[-2] if len(segments) >= 2 and _ACCOUNT.match(segments[-2]) else account
    match = _REGION_IN_HOST.search((parts.hostname or "").lower())
    where = match.group(1) if match else region
    return f"arn:{partition}:sqs:{where}:{owner}:{name}"


def s3_object_arn(uri: str, *, partition: str = "aws") -> str | None:
    """``arn:<partition>:s3:::<bucket>/<key>`` of an ``s3://bucket/key`` URI (a key ending in
    ``/`` is a prefix: ``<key>*``); ``None`` for anything else."""
    text = uri.strip()
    if not text.lower().startswith(_S3_SCHEME):
        return None
    bucket, _, key = text[len(_S3_SCHEME) :].partition("/")
    if not bucket or not key:
        return None
    if key.endswith("/"):
        key += "*"
    return f"arn:{partition}:s3:::{bucket}/{key}"


def dataset_uris(cfg: Any | None, options: StepFunctionsOptions) -> list[str]:
    """The ``s3://`` dataset locators of ``cfg.datasets`` for the watched pipelines (every
    pipeline with ``discover: true``), in configuration order."""
    datasets = getattr(cfg, "datasets", None) if cfg is not None else None
    if not isinstance(datasets, Mapping):
        return []
    found: list[str] = []
    for pipeline_id, steps in datasets.items():
        if not options.discover and pipeline_id not in options.pipelines:
            continue
        if not isinstance(steps, Mapping):
            continue
        for locator in steps.values():
            path = locator.get("path") if isinstance(locator, Mapping) else getattr(locator, "path", None)
            if isinstance(path, str) and path.strip().lower().startswith(_S3_SCHEME):
                found.append(path.strip())
    return _unique(found)


def _log_group_resources(log_groups: Sequence[str], scope: _Scope) -> list[str]:
    """``log-group:/aws/lambda/*`` plus each custom log group (the group and its streams)."""
    resources = [scope.arn("logs", f"log-group:{LAMBDA_LOG_GROUPS}")]
    for group in log_groups:
        name = str(group).strip()
        if not name or name.startswith(LAMBDA_LOG_GROUP_PREFIX) or any(c.isspace() for c in name):
            continue  # the wildcard covers the default groups; a name with spaces is no log group
        resources.append(scope.arn("logs", f"log-group:{name}"))
        resources.append(scope.arn("logs", f"log-group:{name}:*"))
    return _unique(resources)


def _kms_resources(kms_key_arns: Sequence[str], scope: _Scope) -> list[str]:
    """Key ARNs as given; a bare key id becomes the key ARN of the scope."""
    resources: list[str] = []
    for key in kms_key_arns:
        text = str(key).strip()
        if not text:
            continue
        resources.append(text if arns.is_arn(text) else scope.arn("kms", f"key/{text}"))
    return _unique(resources)


# =========================================================================== the policies


def _read_statements(
    options: StepFunctionsOptions,
    cfg: Any | None,
    scope: _Scope,
    kms_key_arns: Sequence[str],
    log_groups: Sequence[str],
) -> list[dict[str, Any]]:
    machines, executions = _machine_resources(options, scope)
    statements = [
        _statement("ReadStateMachines", READ_STATE_MACHINE_ACTIONS, machines),
        _statement("ReadExecutions", READ_EXECUTION_ACTIONS, executions),
    ]
    if options.discover or _needs_name_resolution(options):
        statements.append(_statement("ListStateMachines", LIST_STATE_MACHINES_ACTIONS, "*"))
    if options.log_lookup:
        statements.append(_statement("ReadLambdaLogs", LOGS_ACTIONS, _log_group_resources(log_groups, scope)))
        statements.append(
            _statement("ReadLambdaConfiguration", LAMBDA_ACTIONS, [scope.arn("lambda", "function:*")])
        )
    if options.queue_url:
        queue = queue_arn(
            options.queue_url, partition=scope.partition, region=scope.region, account=scope.account
        )
        statements.append(_statement("ReadEventQueue", QUEUE_ACTIONS, [queue]))
    objects = [s3_object_arn(uri, partition=scope.partition) for uri in dataset_uris(cfg, options)]
    datasets = [arn for arn in objects if arn]
    if datasets:
        statements.append(_statement("ReadDatasets", DATASET_ACTIONS, datasets))
    keys = _kms_resources(kms_key_arns, scope)
    if keys:
        statements.append(_statement("DecryptExecutionData", KMS_ACTIONS, keys))
    return statements


def _write_statements(options: StepFunctionsOptions, scope: _Scope) -> list[dict[str, Any]]:
    machines, executions = _machine_resources(options, scope)
    statements: list[dict[str, Any]] = []
    if options.retry_strategy == "redrive":
        statements.append(_statement("RedriveExecutions", REDRIVE_ACTIONS, executions))
    if options.rerun:
        statements.append(_statement("StartExecutions", START_ACTIONS, machines))
    return statements


def iam_policies(
    options: StepFunctionsOptions,
    *,
    cfg: Any | None = None,
    account: str | None = None,
    region: str | None = None,
    kms_key_arns: Sequence[str] = (),
    log_groups: Sequence[str] = (),
) -> dict[str, dict[str, Any]]:
    """``{"read": <policy document>, "write": <policy document>}`` (``write`` only when a
    remediation is enabled). ``account``/``region`` default to the configured ARNs' values,
    else placeholders. ``log_groups``: the custom log groups of the functions (``LoggingConfig``)."""
    scope = _default_scope(options, account, region)
    policies: dict[str, dict[str, Any]] = {
        "read": _policy(_read_statements(options, cfg, scope, kms_key_arns, log_groups))
    }
    write = _write_statements(options, scope)
    if write:
        policies["write"] = _policy(write)
    return policies


def render_iam_policy(
    options: StepFunctionsOptions,
    *,
    cfg: Any | None = None,
    account: str | None = None,
    region: str | None = None,
    kms_key_arns: Sequence[str] = (),
    log_groups: Sequence[str] = (),
) -> str:
    """The policies as indented JSON text (``{"read": ..., "write": ...}``)."""
    policies = iam_policies(
        options, cfg=cfg, account=account, region=region, kms_key_arns=kms_key_arns, log_groups=log_groups
    )
    return json.dumps(policies, indent=2)


__all__ = [
    "ACCOUNT_PLACEHOLDER",
    "LAMBDA_LOG_GROUPS",
    "LAMBDA_LOG_GROUP_PREFIX",
    "POLICY_VERSION",
    "QUEUE_PLACEHOLDER",
    "REGION_PLACEHOLDER",
    "STATE_MACHINE_PLACEHOLDER",
    "dataset_uris",
    "iam_policies",
    "queue_arn",
    "render_iam_policy",
    "s3_object_arn",
]
