"""Parse and build the ARNs the Step Functions adapter deals with (pure, stdlib only).

Shapes (``stepfunctions-facts.md`` sections 3 and 8):

* state machine: ``arn:aws:states:<region>:<account>:stateMachine:<name>`` with an optional
  ``:<version>`` or ``:<alias>`` qualifier; a Distributed Map child machine is
  ``...:stateMachine:<name>/<label>``;
* execution: ``arn:aws:states:<region>:<account>:execution:<state machine>:<name>``, a Map Run
  child ``...:execution:<state machine>/<label>:<name>``; Express executions use
  ``...:express:<state machine>:<name>:<id>`` [U];
* Map Run: ``arn:aws:states:<region>:<account>:mapRun:<state machine>/<label>:<id>``;
* Lambda function: ``arn:aws:lambda:<region>:<account>:function:<name>[:<qualifier>]``.

``run_id`` of an incident is the execution name, or ``<name>@<startDate in ms>`` when an
execution name was reused (section 2.2 of the design): :func:`run_id_for` and
:func:`execution_name` convert between the two, and :func:`run_names_execution` tells whether a
run id names a given execution. ``@`` and digits are valid in execution names, so a suffix is a
reuse marker only when it is the execution's start.
"""

from __future__ import annotations

import re
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone

_RUN_SUFFIX = re.compile(r"^(?P<name>.+)@(?P<ms>\d{10,16})$")
_ACCOUNT = re.compile(r"^\d{12}$")
_EPOCH = datetime(1970, 1, 1, tzinfo=timezone.utc)
_ONE_MS = timedelta(milliseconds=1)


@dataclass(frozen=True)
class Arn:
    """A generic ARN: ``arn:<partition>:<service>:<region>:<account>:<resource>``."""

    partition: str
    service: str
    region: str
    account: str
    resource: str

    def __str__(self) -> str:
        return f"arn:{self.partition}:{self.service}:{self.region}:{self.account}:{self.resource}"


def is_arn(text: object) -> bool:
    """True for a string that looks like an ARN (``arn:<partition>:<service>:...``)."""
    return isinstance(text, str) and text.startswith("arn:") and text.count(":") >= 5


def parse_arn(text: str) -> Arn:
    """Split an ARN into its parts. Raises :class:`ValueError` for anything else."""
    if not is_arn(text):
        raise ValueError(f"not an ARN: {text!r}")
    _arn, partition, service, region, account, resource = text.split(":", 5)
    if not partition or not service or not resource:
        raise ValueError(f"not an ARN: {text!r}")
    return Arn(partition, service, region, account, resource)


def partition_for_region(region: str | None) -> str:
    """``aws``, ``aws-cn`` (``cn-*``) or ``aws-us-gov`` (``us-gov-*``)."""
    if region and region.startswith("cn-"):
        return "aws-cn"
    if region and region.startswith("us-gov-"):
        return "aws-us-gov"
    return "aws"


# =========================================================================== state machines


@dataclass(frozen=True)
class StateMachineArn:
    """A state machine ARN, possibly qualified (version/alias) or labelled (Map Run child)."""

    partition: str
    region: str
    account: str
    name: str
    qualifier: str | None = None  # version number or alias name
    label: str | None = None  # Distributed Map child: Map Run label (or id)

    @property
    def unqualified(self) -> str:
        """``arn:<p>:states:<r>:<a>:stateMachine:<name>`` (no label, no qualifier)."""
        return f"arn:{self.partition}:states:{self.region}:{self.account}:stateMachine:{self.name}"

    @property
    def is_map_run_child(self) -> bool:
        return self.label is not None

    def __str__(self) -> str:
        text = self.unqualified
        if self.label is not None:
            text += f"/{self.label}"
        if self.qualifier is not None:
            text += f":{self.qualifier}"
        return text


def parse_state_machine_arn(text: str) -> StateMachineArn:
    """Parse a (possibly qualified or labelled) state machine ARN; :class:`ValueError` otherwise."""
    arn = parse_arn(text)
    kind, _, rest = arn.resource.partition(":")
    if arn.service != "states" or kind != "stateMachine" or not rest:
        raise ValueError(f"not a state machine ARN: {text!r}")
    head, _, qualifier = rest.partition(":")
    name, _, label = head.partition("/")
    if not name:
        raise ValueError(f"not a state machine ARN: {text!r}")
    return StateMachineArn(
        partition=arn.partition,
        region=arn.region,
        account=arn.account,
        name=name,
        qualifier=qualifier or None,
        label=label or None,
    )


def state_machine_arn(region: str, account: str, name: str, *, partition: str | None = None) -> str:
    """Build an unqualified state machine ARN."""
    part = partition or partition_for_region(region)
    return f"arn:{part}:states:{region}:{account}:stateMachine:{name}"


def state_machine_name(text: str) -> str:
    """The state machine name of an ARN (labels and qualifiers dropped) or of a plain name."""
    if is_arn(text):
        return parse_state_machine_arn(text).name
    return text.strip()


# =========================================================================== executions


@dataclass(frozen=True)
class ExecutionArn:
    """An execution ARN (Standard; Map Run children carry the label; Express is [U])."""

    partition: str
    region: str
    account: str
    state_machine: str  # state machine name
    name: str  # execution name
    label: str | None = None  # Map Run child executions
    express: bool = False

    @property
    def state_machine_arn(self) -> str:
        """The unqualified ARN of the (parent) state machine."""
        return f"arn:{self.partition}:states:{self.region}:{self.account}:stateMachine:{self.state_machine}"

    @property
    def is_map_run_child(self) -> bool:
        return self.label is not None

    def __str__(self) -> str:
        machine = self.state_machine + (f"/{self.label}" if self.label else "")
        kind = "express" if self.express else "execution"
        return f"arn:{self.partition}:states:{self.region}:{self.account}:{kind}:{machine}:{self.name}"


def parse_execution_arn(text: str) -> ExecutionArn:
    """Parse an execution ARN; :class:`ValueError` otherwise.

    ``express`` ARNs are accepted with the [U] shape ``express:<machine>:<name>:<id>`` (``name``
    is ``<name>:<id>`` there, so an express ARN never builds back to a Standard one).
    """
    arn = parse_arn(text)
    kind, _, rest = arn.resource.partition(":")
    if arn.service != "states" or kind not in ("execution", "express") or ":" not in rest:
        raise ValueError(f"not an execution ARN: {text!r}")
    machine, _, name = rest.partition(":")
    sm_name, _, label = machine.partition("/")
    if not sm_name or not name:
        raise ValueError(f"not an execution ARN: {text!r}")
    return ExecutionArn(
        partition=arn.partition,
        region=arn.region,
        account=arn.account,
        state_machine=sm_name,
        name=name,
        label=label or None,
        express=kind == "express",
    )


def execution_arn(state_machine: str, name: str) -> str:
    """Build the ARN of execution ``name`` of an (unqualified or qualified) state machine ARN."""
    machine = parse_state_machine_arn(state_machine)
    path = machine.name + (f"/{machine.label}" if machine.label else "")
    return f"arn:{machine.partition}:states:{machine.region}:{machine.account}:execution:{path}:{name}"


# =========================================================================== map runs


@dataclass(frozen=True)
class MapRunArn:
    """``arn:<p>:states:<r>:<a>:mapRun:<state machine>/<label>:<id>``."""

    partition: str
    region: str
    account: str
    state_machine: str
    label: str
    run: str


def parse_map_run_arn(text: str) -> MapRunArn:
    """Parse a Map Run ARN; :class:`ValueError` otherwise."""
    arn = parse_arn(text)
    kind, _, rest = arn.resource.partition(":")
    if arn.service != "states" or kind != "mapRun" or "/" not in rest:
        raise ValueError(f"not a Map Run ARN: {text!r}")
    machine, _, tail = rest.partition("/")
    label, _, run = tail.partition(":")
    if not machine or not label:
        raise ValueError(f"not a Map Run ARN: {text!r}")
    return MapRunArn(arn.partition, arn.region, arn.account, machine, label, run)


# =========================================================================== lambda


def lambda_function_name(text: str) -> str:
    """The function name of a Lambda function ARN, partial ARN (``<account>:function:<name>``),
    ``name:qualifier`` or plain name. Qualifiers (version, alias, ``$LATEST``) are dropped."""
    value = text.strip()
    if is_arn(value):
        arn = parse_arn(value)
        kind, _, rest = arn.resource.partition(":")
        if arn.service == "lambda" and kind == "function" and rest:
            return rest.split(":", 1)[0]
        raise ValueError(f"not a Lambda function ARN: {text!r}")
    parts = value.split(":")
    if len(parts) >= 3 and parts[1] == "function" and _ACCOUNT.match(parts[0]):
        return parts[2]
    return parts[0]


def lambda_function_arn(region: str, account: str, name: str, *, partition: str | None = None) -> str:
    """Build an unqualified Lambda function ARN."""
    part = partition or partition_for_region(region)
    return f"arn:{part}:lambda:{region}:{account}:function:{name}"


# =========================================================================== run ids


def _epoch_ms(value: datetime) -> int:
    """Epoch milliseconds, computed exactly (``timestamp() * 1000`` is off by one for some dates)."""
    moment = value if value.tzinfo is not None else value.replace(tzinfo=timezone.utc)
    return (moment - _EPOCH) // _ONE_MS


def run_id_for(name: str, start_date: datetime | None = None, *, with_start: bool = False) -> str:
    """The incident ``run_id`` of an execution: its name, or ``<name>@<startDate ms>`` when the
    name was reused (``with_start``; section 2.2 of the design)."""
    if with_start and start_date is not None:
        return f"{name}@{_epoch_ms(start_date)}"
    return name


def _start_ms(start: datetime | int | None) -> int | None:
    """A start date (a datetime, naive = UTC, or epoch milliseconds) in epoch milliseconds."""
    if isinstance(start, datetime):
        return _epoch_ms(start)
    if isinstance(start, int) and not isinstance(start, bool):
        return start
    return None


def execution_name(run_id: str, *, start: datetime | int | None = None) -> str:
    """The execution name of a ``run_id``: an ``@<startDate ms>`` suffix is dropped.

    ``@`` and digits are valid in an execution name, so when the execution's ``start`` (a
    datetime or epoch milliseconds) is known and is not the suffix's milliseconds, the suffix is
    part of the name and ``run_id`` is returned as it is. Without ``start`` a suffix is taken for
    the reuse marker.
    """
    match = _RUN_SUFFIX.match(run_id)
    if match is None:
        return run_id
    known = _start_ms(start)
    if known is not None and known != int(match.group("ms")):
        return run_id
    return match.group("name")


def run_names_execution(run_id: str, name: str, start: datetime | int | None = None) -> bool:
    """True when ``run_id`` names the execution called ``name`` that started at ``start``.

    It does when the run id is that name, or ``<name>@<startDate ms>`` (a reused name, section 2.2
    of the design) with the execution's start; without ``start`` the marker cannot be checked and
    the name decides.
    """
    if run_id == name:
        return True
    match = _RUN_SUFFIX.match(run_id)
    if match is None or match.group("name") != name:
        return False
    known = _start_ms(start)
    return known is None or known == int(match.group("ms"))


__all__ = [
    "Arn",
    "ExecutionArn",
    "MapRunArn",
    "StateMachineArn",
    "execution_arn",
    "execution_name",
    "is_arn",
    "lambda_function_arn",
    "lambda_function_name",
    "parse_arn",
    "parse_execution_arn",
    "parse_map_run_arn",
    "parse_state_machine_arn",
    "partition_for_region",
    "run_id_for",
    "run_names_execution",
    "state_machine_arn",
    "state_machine_name",
]
