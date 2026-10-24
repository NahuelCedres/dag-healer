"""An offline AWS for tests and demos: no credentials, no network (section 11.3 of the design).

:class:`OfflineAws` is a small in-memory AWS account: Step Functions state machines and
executions with their histories, Lambda functions, CloudWatch log events, S3 objects and SQS
queues. Its clients are **real botocore clients** (dummy credentials, an isolated botocore
session that reads no AWS profile, config or credentials file) whose calls are answered through
botocore's ``before-call`` event hook, the mechanism ``botocore.stub.Stubber`` uses. Nothing
leaves the process, and the world is strict where AWS is strict, so fixture mistakes surface:

* botocore validates the request parameters (types, required members, minimum lengths); the
  world checks what AWS checks on its side (maximum lengths and values and enum values for the
  services that validate against their model, ARN shapes, execution names, JSON input, SQS's
  parameter ranges and attribute names);
* every answer is validated against the operation's output shape
  (``botocore.validate.validate_parameters``, plus enums and maximum lengths) and carries the
  Python types botocore parses: Step Functions timestamps are ``tzlocal()`` datetimes with
  milliseconds, CloudWatch Logs timestamps epoch milliseconds, Lambda's ``LastModified`` text,
  S3's ``LastModified`` a UTC datetime (seconds);
* records are validated when they are added (history events against ``HistoryEvent``, an
  execution against its own history, log events, message bodies), so a broken fixture fails
  where it is written, with the place named;
* errors are AWS error answers (``{"Error": {"Code", "Message"}, "ResponseMetadata":
  {"HTTPStatusCode"}}``) that botocore raises as the modeled ``ClientError`` subclasses (SQS
  errors carry the legacy query code in ``Code``, as SQS's JSON protocol does).

Unlike a ``Stubber`` it answers any call order, so the packaged conformance suite and
``dag-healer worker --once`` can run against it. Every call that reaches the world is recorded
in :attr:`OfflineAws.calls` with the parameters as the caller passed them (a call botocore
rejects client-side never reaches it); :meth:`OfflineAws.fail_next` queues one-shot error answers
and :attr:`OfflineAws.outage` fails every call (``"server_error"``: HTTP 500 ``InternalFailure``;
``"network"``: ``botocore.exceptions.EndpointConnectionError``).

Operations: ``stepfunctions`` ListStateMachines, DescribeStateMachine, ListExecutions,
DescribeExecution, GetExecutionHistory, DescribeStateMachineForExecution, RedriveExecution,
StartExecution; ``logs`` FilterLogEvents; ``lambda`` GetFunction; ``sqs`` ReceiveMessage,
DeleteMessageBatch, DeleteMessage, GetQueueAttributes; ``s3`` GetObject; ``sts``
GetCallerIdentity. Anything else answers ``UnsupportedOperation`` (HTTP 400). Semantics follow
the verified facts (``stepfunctions-facts.md``):

* ``ListExecutions``: newest first (running executions by start date, finished ones by stop
  date), one optional ``statusFilter``, ``redriveFilter``, ``mapRunArn`` (Map Run children are
  listed by their Map Run only); an EXPRESS machine answers ``StateMachineTypeNotSupported``.
* ``GetExecutionHistory`` honours ``reverseOrder``, ``maxResults`` (``0`` or absent: 100),
  ``nextToken`` and ``includeExecutionData`` (``False`` drops the ``input`` and ``output`` of every
  event; causes and task ``parameters`` stay [U]).
* Page tokens are opaque, bound to the request that produced them, and expire after 24 hours of
  the world clock (``InvalidToken``; ``InvalidParameterException`` for CloudWatch Logs).
* ``DescribeExecution`` with ``includedData=METADATA_ONLY`` leaves out ``input`` and ``output`` [U].
* ``RedriveExecution`` is idempotent per ``clientToken`` (the last 10 tokens of an execution, for
  15 minutes) and answers ``ExecutionNotRedrivable`` for RUNNING or SUCCEEDED executions,
  executions that stopped more than 14 days before the world clock (``now``), histories of
  24,999 events or more, executions started before 2023-11-15 and Map Run children. A redrive
  makes the execution RUNNING again (``redriveCount + 1``, an ``ExecutionRedriven`` event);
  :meth:`OfflineAws.finish_execution` ends it.
* ``StartExecution`` is idempotent for a RUNNING execution of the same name and input and answers
  ``ExecutionAlreadyExists`` otherwise; an EXPRESS machine returns an ``express`` ARN and keeps
  nothing (Express executions have no history API).
* ``FilterLogEvents`` supports text patterns (terms, ``"quoted terms"``, ``?either``,
  ``-excluded``), JSON patterns (``{ $.level = "ERROR" && $.requestId = "..." }``, ``IS NULL``,
  ``NOT EXISTS``, numeric comparisons, ``*`` wildcards) and ``%regex%``; space-delimited
  ``[...]`` patterns answer ``InvalidParameterException``. An event is visible once its
  ingestion time (by default 500 ms after its timestamp) is not after ``now``: late logs can
  be simulated.
* ``GetObject`` honours ``Range`` (HTTP 206 with ``ContentRange``; ``InvalidRange``, HTTP 416),
  ``IfMatch``/``IfNoneMatch`` and ``VersionId="null"``; a missing bucket or key answers
  ``NoSuchBucket``/``NoSuchKey`` (HTTP 404).
* SQS messages stay invisible for the visibility timeout once received, and every receive hands
  out a new receipt handle; deleting with any handle a message was given succeeds.

Scenario files (JSON; paths relative to the file) describe a world for :meth:`OfflineAws.load`::

    {"region": "eu-west-1", "account": "123456789012", "now": "2026-10-02T07:00:00Z",
     "state_machines": [{"name": ..., "type": "STANDARD", "definition_file": "...asl.json" |
                         "definition": {...}, "substitutions": {"X": "..."}, "role_arn": ...,
                         "logging": {"level": "OFF", "includeExecutionData": false}}],
     "functions": [{"name": ..., "handler": "app.handler", "runtime": "python3.11",
                    "log_group": null, "log_format": "Text", "code_sha256": null, "layers": []}],
     "executions": [{"state_machine": <name>, "name": ..., "status": "FAILED",
                     "start_date": ..., "stop_date": ..., "redrive_count": 0, "error": ...,
                     "cause": ..., "input": "{...}", "history_file": "..." | "history": [...]}],
     "log_events": [{"group": ..., "stream": ..., "events": [{"timestamp": ..., "message": ...}]
                     | "events_file": "..."}],
     "objects": [{"bucket": ..., "key": ..., "file": "..." | "body": "..."}],
     "queues": [{"url": ..., "attributes": {...}, "messages": [{"body_file": "..."} | {"body": "..."}]}]}

Timestamps are ISO-8601 text (``Z`` accepted) or epoch milliseconds. A history file holds a list
of events or a ``{"events": [...]}`` object (a ``GetExecutionHistory`` answer); an execution's
``stop_date``, ``error``, ``cause``, ``input`` and ``redrive_date`` default to what its history
says, and must agree with it when given. Unknown keys are errors.

botocore is imported lazily, on first use (validating a record or building a client).
"""

from __future__ import annotations

import base64
import contextlib
import copy
import email.utils
import hashlib
import io
import itertools
import json
import logging
import mimetypes
import os
import re
import threading
import uuid
from collections import deque
from collections.abc import Callable, Iterable, Iterator, Mapping, Sequence
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Literal

from dag_healer.stepfunctions import arns
from dag_healer.stepfunctions.runtime import epoch_ms, to_utc

logger = logging.getLogger(__name__)

DEFAULT_REGION = "eu-west-1"
DEFAULT_ACCOUNT = "123456789012"
OutageKind = Literal["server_error", "network"]

#: The services whose operations the world answers.
SERVICES: tuple[str, ...] = ("stepfunctions", "logs", "lambda", "sqs", "s3", "sts")
EXECUTION_STATUSES: tuple[str, ...] = (
    "RUNNING",
    "SUCCEEDED",
    "FAILED",
    "TIMED_OUT",
    "ABORTED",
    "PENDING_REDRIVE",
)
#: A finished execution can be redriven for 14 days after it stopped [V].
REDRIVE_WINDOW = timedelta(days=14)
#: Executions with this many history events or more cannot be redriven [V].
MAX_REDRIVE_EVENTS = 24_999
#: Executions started before this date cannot be redriven [V].
REDRIVE_AVAILABLE_SINCE = datetime(2023, 11, 15, tzinfo=timezone.utc)
CLIENT_TOKEN_TTL = timedelta(minutes=15)
CLIENT_TOKENS_KEPT = 10
PAGE_TOKEN_TTL = timedelta(hours=24)
DEFAULT_PAGE_SIZE = 100
DEFAULT_LOG_LIMIT = 10_000
MAX_EXECUTION_INPUT_BYTES = 256 * 1024
DEFAULT_INGESTION_DELAY_MS = 500
DEFAULT_VISIBILITY_TIMEOUT = 30
MAX_SQS_MESSAGE_BYTES = 256 * 1024
#: State machines of a world were "created" this long before its clock.
MACHINE_AGE = timedelta(days=30)

_UTC = timezone.utc
_EPOCH = datetime(1970, 1, 1, tzinfo=_UTC)
_DUMMY_CREDENTIALS = {
    "aws_access_key_id": "offline-access-key",
    "aws_secret_access_key": "offline-secret-key",
    "aws_session_token": "offline-session-token",
}
_CONTEXT_KEY = "dag_healer_offline_aws"
_SERVICE_ALIASES = {"sfn": "stepfunctions", "states": "stepfunctions", "cloudwatch-logs": "logs"}
#: The error code of a request that breaks a modeled constraint (maximum, enum), for the services
#: that validate their input against the model; SQS and S3 check their parameters in the handlers.
_VALIDATION_CODES = {
    "stepfunctions": "ValidationException",
    "logs": "InvalidParameterException",
    "lambda": "ValidationException",
}
#: SQS's JSON protocol answers the legacy query code in ``Code`` (``x-amzn-query-error``) and
#: the modeled error name in ``QueryErrorCode``, which botocore uses to pick the exception class.
_SQS_QUERY_CODES = {
    "QueueDoesNotExist": "AWS.SimpleQueueService.NonExistentQueue",
    "ReceiptHandleIsInvalid": "ReceiptHandleIsInvalid",
    "TooManyEntriesInBatchRequest": "AWS.SimpleQueueService.TooManyEntriesInBatchRequest",
    "EmptyBatchRequest": "AWS.SimpleQueueService.EmptyBatchRequest",
    "BatchEntryIdsNotDistinct": "AWS.SimpleQueueService.BatchEntryIdsNotDistinct",
    "InvalidBatchEntryId": "AWS.SimpleQueueService.InvalidBatchEntryId",
    "InvalidAttributeName": "InvalidAttributeName",
    "UnsupportedOperation": "AWS.SimpleQueueService.UnsupportedOperation",
    "OverLimit": "OverLimit",
    "RequestThrottled": "RequestThrottled",
    "InvalidSecurity": "InvalidSecurity",
    "InvalidAddress": "InvalidAddress",
}
_TERMINAL_EVENTS = {
    "SUCCEEDED": "ExecutionSucceeded",
    "FAILED": "ExecutionFailed",
    "TIMED_OUT": "ExecutionTimedOut",
    "ABORTED": "ExecutionAborted",
}
_TERMINAL_DETAILS = {
    "ExecutionSucceeded": "executionSucceededEventDetails",
    "ExecutionFailed": "executionFailedEventDetails",
    "ExecutionTimedOut": "executionTimedOutEventDetails",
    "ExecutionAborted": "executionAbortedEventDetails",
}
#: Execution and state machine names: 1-80 characters without whitespace, brackets, wildcards,
#: the characters ``" # % \\ ^ | ~ ` $ & , ; : /`` or control characters.
_NAME = re.compile(r"^[^\s<>{}\[\]?*\"#%\\^|~`$&,;:/\x00-\x1f\x7f-\x9f]{1,80}$")
_FUNCTION_NAME = re.compile(r"^[A-Za-z0-9_-]{1,64}$")
_FUNCTION_REF = re.compile(
    r"^(?:arn:(?P<partition>aws[a-zA-Z-]*):lambda:(?P<region>[a-z0-9-]+):(?P<account>\d{12}):function:"
    r"|(?P<short_account>\d{12}):function:)?(?P<name>[A-Za-z0-9_.-]+)(?::(?P<qualifier>\$LATEST|[A-Za-z0-9_-]+))?$"
)
_LAYER_NAME = re.compile(r"^[A-Za-z0-9_-]{1,140}$")
_ROLE_ARN = re.compile(r"^arn:aws[a-z-]*:iam::\d{12}:role/[A-Za-z0-9_+=,.@/-]+$")
_REGION = re.compile(r"^[a-z]{2}(-[a-z]+)+-\d+$")
_ACCOUNT = re.compile(r"^\d{12}$")
_BUCKET = re.compile(r"^[a-z0-9][a-z0-9.-]{1,61}[a-z0-9]$")
_LOG_GROUP = re.compile(r"^[.\-_/#A-Za-z0-9]{1,512}$")
_LOG_STREAM = re.compile(r"^[^:*]{1,512}$")
_QUEUE_URL = re.compile(r"^https?://[^/\s]+/(?P<account>\d{12})/(?P<name>[A-Za-z0-9_-]{1,80}(?:\.fifo)?)$")
_BATCH_ENTRY_ID = re.compile(r"^[A-Za-z0-9_-]{1,80}$")
_PLACEHOLDER = re.compile(r"\$\{[A-Za-z0-9_:.-]+\}")
_RANGE = re.compile(r"^bytes=(\d*)-(\d*)$")
#: Characters SQS refuses in a message body (it accepts #x9, #xA, #xD, #x20-#xD7FF, #xE000-#xFFFD and
#: #x10000-#x10FFFF).
_SQS_INVALID_CHARACTERS = re.compile(r"[^\t\n\r\x20-\ud7ff\ue000-\ufffd\U00010000-\U0010ffff]")
_COMPARISONS: dict[str, Callable[[Any, Any], bool]] = {
    "=": lambda left, right: left == right,
    "!=": lambda left, right: left != right,
    "<": lambda left, right: left < right,
    "<=": lambda left, right: left <= right,
    ">": lambda left, right: left > right,
    ">=": lambda left, right: left >= right,
}


class OfflineNetworkError(RuntimeError):
    """A client of :func:`dummy_client` or :class:`OfflineAws` was about to send a request."""


@dataclass(frozen=True)
class AwsCall:
    """One call a client of the world made (``params`` as the caller passed them)."""

    service: str
    operation: str
    params: Mapping[str, Any] = field(default_factory=dict)


class _AwsError(Exception):
    """An AWS error answer: code, message, HTTP status and extra ``Error`` fields."""

    def __init__(self, code: str, message: str, status: int = 400, **extra: str) -> None:
        super().__init__(f"{code}: {message}")
        self.code = code
        self.message = message
        self.status = status
        self.extra = extra


@dataclass(frozen=True)
class _Answer:
    """A successful answer before it is validated and typed like botocore's parsed responses."""

    body: dict[str, Any]
    status: int = 200
    headers: Mapping[str, str] = field(default_factory=dict)


# =========================================================================== botocore plumbing

_BOTOCORE_LOCK = threading.RLock()
_BOTOCORE: dict[str, Any] = {}
#: Session variables that would let the host's AWS setup leak into the offline clients.
_ISOLATED_SESSION_VARS: dict[str, tuple[Any, Any, Any, Any]] = {
    "profile": (None, None, None, None),
    "region": (None, None, None, None),
    "data_path": (None, None, None, None),
    "ca_bundle": (None, None, None, None),
    "config_file": (None, None, os.devnull, None),
    "credentials_file": (None, None, os.devnull, None),
}


def _botocore_session() -> Any:
    """One isolated botocore session (it reads no profile, config, credentials or CA file)."""
    with _BOTOCORE_LOCK:
        session = _BOTOCORE.get("session")
        if session is None:
            import botocore.session

            session = botocore.session.Session(session_vars=_ISOLATED_SESSION_VARS)
            _BOTOCORE["session"] = session
        return session


def _service_model(service: str) -> Any:
    """The botocore service model of ``service`` (:class:`ValueError` for an unknown service)."""
    with _BOTOCORE_LOCK:
        models = _BOTOCORE.setdefault("models", {})
        model = models.get(service)
        if model is None:
            from botocore.exceptions import UnknownServiceError

            try:
                model = _botocore_session().get_service_model(service)
            except UnknownServiceError:
                raise ValueError(f"unknown AWS service {service!r}") from None
            models[service] = model
        return model


def _operation_model(service: str, operation: str) -> Any:
    return _service_model(service).operation_model(operation)


def _service_name(service: Any) -> str:
    """The botocore name of a service (``sfn`` and ``states`` mean ``stepfunctions``)."""
    if not isinstance(service, str) or not service.strip():
        raise ValueError(f"expected an AWS service name, got {service!r}")
    name = service.strip().lower()
    return _SERVICE_ALIASES.get(name, name)


def _operation_name(service: str, operation: Any) -> str:
    """The API name of an operation given as ``DescribeExecution`` or ``describe_execution``."""
    if not isinstance(operation, str) or not operation.strip():
        raise ValueError(f"expected an operation name, got {operation!r}")
    from botocore import xform_name

    names = list(_service_model(service).operation_names)
    if operation in names:
        return operation
    by_method = {xform_name(name): name for name in names}
    if operation in by_method:
        return by_method[operation]
    raise ValueError(f"{service} has no operation {operation!r}")


def _client_config(region: str) -> Any:
    """No retries (nothing is sent), no endpoint, FIPS or dual-stack setting from the host."""
    from botocore.config import Config

    return Config(
        region_name=region,
        retries={"mode": "standard", "total_max_attempts": 1},
        connect_timeout=1,
        read_timeout=1,
        ignore_configured_endpoint_urls=True,
        use_fips_endpoint=False,
        use_dualstack_endpoint=False,
        user_agent_extra="dag-healer-offline",
    )


def _refuse_network(event_name: str = "", **_kwargs: Any) -> None:
    """``before-send`` guard: an offline client never sends a request."""
    operation = event_name.split(".", 1)[-1] if event_name else "a request"
    raise OfflineNetworkError(
        f"an offline AWS client refused to send {operation} over the network: nothing answered "
        "the call (activate a botocore Stubber, or use OfflineAws.client_factory)"
    )


def _new_client(service: str, region: str) -> Any:
    """A botocore client with dummy credentials whose requests never leave the process."""
    session = _botocore_session()
    with _BOTOCORE_LOCK:  # creating clients from one session is not thread-safe
        client = session.create_client(
            service, region_name=region, config=_client_config(region), **_DUMMY_CREDENTIALS
        )
    client.meta.events.register_last("before-send.*.*", _refuse_network, unique_id="dag-healer-offline-send")
    return client


def dummy_client(service: str, *, region: str = DEFAULT_REGION) -> Any:
    """A botocore client with dummy credentials that never uses the network (for
    ``botocore.stub.Stubber`` tests): a call that no Stubber answers raises
    :class:`OfflineNetworkError` instead of reaching AWS.

    The client comes from an isolated botocore session, so ``AWS_PROFILE``, AWS config files,
    CA bundles and configured endpoint URLs of the host change nothing.
    """
    return _new_client(_service_name(service), region)


@contextlib.contextmanager
def refuse_aws_requests() -> Iterator[list[str]]:
    """Inside the block no botocore client sends an HTTP request, whichever way it was built
    (boto3, a plain botocore session): botocore's sender raises :class:`OfflineNetworkError`
    instead, and the yielded list records the URL of every refused request (a caller may assert
    it stays empty, since a refusal can be caught as an AWS error on its way up). A ``Stubber``,
    this module's clients and ``before-send`` answers stop a call before it is sent and work as
    usual.
    """
    from botocore.httpsession import URLLib3Session

    refused: list[str] = []
    original = URLLib3Session.send

    def send(self: Any, request: Any) -> Any:
        url = str(getattr(request, "url", "") or "?")
        refused.append(url)
        raise OfflineNetworkError(f"a request to {url} would have left the process")

    URLLib3Session.send = send  # type: ignore[method-assign]
    try:
        yield refused
    finally:
        URLLib3Session.send = original  # type: ignore[method-assign]


# =========================================================================== small helpers


def _ms(moment: datetime) -> datetime:
    """``moment`` truncated to milliseconds (AWS timestamps have millisecond precision)."""
    return moment.replace(microsecond=moment.microsecond // 1000 * 1000)


def _moment(value: Any, what: str) -> datetime:
    """An aware UTC datetime (milliseconds) from a datetime, ISO-8601 text or epoch milliseconds."""
    moment = None if isinstance(value, bool) or value is None else to_utc(value)
    if moment is None:
        raise ValueError(f"{what}: expected an ISO-8601 timestamp or epoch milliseconds, got {value!r}")
    return _ms(moment)


def _optional_moment(value: Any, what: str) -> datetime | None:
    return None if value is None else _moment(value, what)


def _millis(moment: datetime) -> int:
    value = epoch_ms(moment)
    assert value is not None
    return value


def _clock_of(now: datetime | Callable[[], datetime] | None) -> Callable[[], datetime]:
    if now is None:
        return lambda: datetime.now(_UTC)
    if isinstance(now, datetime):
        fixed = _moment(now, "now")
        return lambda: fixed
    if callable(now):
        return now
    raise TypeError(f"now must be a datetime, a callable returning one, or None, not {type(now).__name__}")


def _digest(value: Any) -> str:
    text = json.dumps(value, sort_keys=True, separators=(",", ":"), default=str)
    return hashlib.sha256(text.encode("utf-8")).hexdigest()[:24]


def _uuid_for(*parts: Any) -> str:
    """A deterministic version-4 UUID text (request ids, message ids, generated names)."""
    digest = hashlib.sha256(":".join(str(part) for part in parts).encode("utf-8")).digest()
    return str(uuid.UUID(bytes=digest[:16], version=4))


def _snapshot(params: Mapping[str, Any]) -> dict[str, Any]:
    """A deep copy of request parameters (a shallow one when they hold uncopyable streams)."""
    try:
        return copy.deepcopy(dict(params))
    except Exception:  # file objects and other streams cannot be deep-copied
        return dict(params)


def _json_text(value: Any, what: str) -> str:
    """``value`` as JSON text (a mapping or a list is dumped compactly); :class:`ValueError` if invalid."""
    if isinstance(value, (Mapping, list)):
        return json.dumps(value, separators=(",", ":"))
    if not isinstance(value, str):
        raise ValueError(f"{what} must be JSON text, not {type(value).__name__}")
    try:
        json.loads(value)
    except ValueError as exc:
        raise ValueError(f"{what} is not valid JSON: {exc}") from None
    return value


def _check_name(value: Any, what: str) -> str:
    if not isinstance(value, str) or not _NAME.match(value):
        raise ValueError(
            f"{what} {value!r} is invalid: 1-80 characters without whitespace, brackets, wildcards "
            'or the characters " # % \\ ^ | ~ ` $ & , ; : /'
        )
    return value


def _check_int(value: Any, what: str, minimum: int) -> int:
    if isinstance(value, bool) or not isinstance(value, int) or value < minimum:
        raise ValueError(f"{what} must be an integer >= {minimum}, got {value!r}")
    return value


def _http_date(moment: datetime) -> str:
    return email.utils.format_datetime(moment.astimezone(_UTC).replace(microsecond=0), usegmt=True)


def _lambda_time(moment: datetime) -> str:
    """Lambda's ``LastModified`` format (``2026-10-01T08:00:00.000+0000``)."""
    utc = moment.astimezone(_UTC)
    return utc.strftime("%Y-%m-%dT%H:%M:%S.") + f"{utc.microsecond // 1000:03d}+0000"


def _page_size(value: Any) -> int:
    return (
        value if isinstance(value, int) and not isinstance(value, bool) and value > 0 else DEFAULT_PAGE_SIZE
    )


def _validation_message(problems: Sequence[str]) -> str:
    count = len(problems)
    return f"{count} validation error{'s' if count != 1 else ''} detected: " + "; ".join(problems)


# =========================================================================== shapes


def _violation(where: str, rule: str) -> str:
    return f"Value at '{where}' failed to satisfy constraint: Member must {rule}"


def _constraint_violations(value: Any, shape: Any, path: str = "") -> Iterator[str]:
    """What AWS checks on its side and botocore does not: maximum lengths/values and enums.

    Lengths and values are never echoed (they may be execution data); enum values are.
    """
    kind = shape.type_name
    limit = shape.metadata.get("max")
    where = path or shape.name
    too_long = f"have length less than or equal to {limit}"
    if kind == "structure" and isinstance(value, Mapping):
        for name, item in value.items():
            member = shape.members.get(name)
            if member is not None and item is not None:
                yield from _constraint_violations(item, member, f"{path}.{name}" if path else name)
    elif kind == "list" and isinstance(value, (list, tuple)):
        if limit is not None and len(value) > limit:
            yield _violation(where, too_long)
        for index, item in enumerate(value):
            yield from _constraint_violations(item, shape.member, f"{where}.{index + 1}.member")
    elif kind == "map" and isinstance(value, Mapping):
        if limit is not None and len(value) > limit:
            yield _violation(where, too_long)
        for key, item in value.items():
            yield from _constraint_violations(item, shape.value, f"{where}.{key}")
    elif kind == "string" and isinstance(value, str):
        if limit is not None and len(value) > limit:
            yield _violation(where, too_long)
        enum = shape.enum
        if enum and value not in enum:
            yield _violation(where, f"satisfy enum value set: [{', '.join(enum)}] (got '{value[:80]}')")
    elif kind in ("integer", "long") and isinstance(value, int) and not isinstance(value, bool):
        if limit is not None and value > limit:
            yield _violation(where, f"have value less than or equal to {limit}")


def _check_shape(value: Any, shape: Any, what: str) -> None:
    """``value`` fits ``shape`` (botocore's validator, then enums and maximums); :class:`ValueError`."""
    from botocore.exceptions import ParamValidationError
    from botocore.validate import validate_parameters

    try:
        validate_parameters(value, shape)
    except ParamValidationError as exc:
        raise ValueError(f"{what} does not fit the AWS model ({shape.name}): {exc}") from None
    problems = list(_constraint_violations(value, shape))
    if problems:
        raise ValueError(f"{what} does not fit the AWS model ({shape.name}): {'; '.join(problems)}")


def _check_answer(service: str, operation: str, body: Mapping[str, Any], what: str) -> None:
    shape = _operation_model(service, operation).output_shape
    if shape is not None:
        _check_shape(dict(body), shape, f"{what}: the {operation} answer")


def _botocore_timestamp(value: Any, shape: Any, protocol: str, location: str | None) -> Any:
    """What botocore's parser makes of ``value`` sent in the protocol's wire format."""
    from botocore.parsers import DEFAULT_TIMESTAMP_PARSER

    moment = _ms(to_utc(value) or _EPOCH)
    wire = shape.serialization.get("timestampFormat")
    if wire is None:
        if location in ("header", "headers"):
            wire = "rfc822"
        elif protocol in ("json", "rest-json"):
            wire = "unixTimestamp"
        else:
            wire = "iso8601"
    if wire == "unixTimestamp":
        return DEFAULT_TIMESTAMP_PARSER(_millis(moment) / 1000.0)
    if wire == "rfc822":
        return DEFAULT_TIMESTAMP_PARSER(_http_date(moment))
    return DEFAULT_TIMESTAMP_PARSER(moment.strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3] + "Z")


def _as_botocore(value: Any, shape: Any, protocol: str, location: str | None = None) -> Any:
    """``value`` with the Python types botocore parses for ``shape`` (timestamps above all)."""
    kind = shape.type_name
    if kind == "structure" and isinstance(value, Mapping):
        typed: dict[str, Any] = {}
        for name, item in value.items():
            member = shape.members.get(name)
            typed[name] = (
                item
                if member is None
                else _as_botocore(item, member, protocol, member.serialization.get("location"))
            )
        return typed
    if kind == "list" and isinstance(value, list):
        return [_as_botocore(item, shape.member, protocol) for item in value]
    if kind == "map" and isinstance(value, Mapping):
        return {key: _as_botocore(item, shape.value, protocol) for key, item in value.items()}
    if kind == "timestamp" and value is not None:
        return _botocore_timestamp(value, shape, protocol, location)
    return value


# =========================================================================== CloudWatch filter patterns


def _filter_error(reason: str) -> _AwsError:
    return _AwsError("InvalidParameterException", f"Invalid filter pattern: {reason}")


def _quoted(text: str, start: int) -> tuple[str, int]:
    """The quoted string of ``text`` opening at ``start`` (``\\"`` and ``\\\\`` escapes) and the
    position after its closing quote."""
    out: list[str] = []
    position = start + 1
    while position < len(text):
        character = text[position]
        if character == "\\" and position + 1 < len(text):
            out.append(text[position + 1])
            position += 2
            continue
        if character == '"':
            return "".join(out), position + 1
        out.append(character)
        position += 1
    raise _filter_error("unterminated quoted term")


def _text_filter(pattern: str) -> Callable[[str], bool]:
    """Terms that must all appear, ``?terms`` of which one must, ``-terms`` that must not."""
    required: list[str] = []
    either: list[str] = []
    excluded: list[str] = []
    position = 0
    while position < len(pattern):
        if pattern[position].isspace():
            position += 1
            continue
        target = required
        if pattern[position] in "?-" and position + 1 < len(pattern) and not pattern[position + 1].isspace():
            target = either if pattern[position] == "?" else excluded
            position += 1
        if pattern[position] == '"':
            term, position = _quoted(pattern, position)
        else:
            end = position
            while end < len(pattern) and not pattern[end].isspace():
                end += 1
            term, position = pattern[position:end], end
        target.append(term)

    def matches(message: str) -> bool:
        if any(term not in message for term in required):
            return False
        if either and not any(term in message for term in either):
            return False
        return not any(term in message for term in excluded)

    return matches


_JSON_TOKEN = re.compile(
    r"\s*(?:(?P<op>&&|\|\||!=|<=|>=|=|<|>|\(|\))|(?P<string>\")|(?P<selector>\$(?:\.[^\s.\[\]()=!<>&|]+|\[\d+\])*)"
    r"|(?P<word>[^\s()=!<>&|\"]+))"
)


def _json_tokens(text: str) -> list[tuple[str, Any]]:
    tokens: list[tuple[str, Any]] = []
    position = 0
    while position < len(text):
        if text[position].isspace():
            position += 1
            continue
        match = _JSON_TOKEN.match(text, position)
        if match is None or match.end() == position:
            raise _filter_error(f"unexpected character {text[position]!r}")
        if match.group("string") is not None:
            value, position = _quoted(text, match.start("string"))
            tokens.append(("string", value))
            continue
        kind = next(name for name in ("op", "selector", "word") if match.group(name) is not None)
        tokens.append((kind, match.group(kind)))
        position = match.end()
    return tokens


def _selector_path(selector: str) -> list[str | int]:
    path: list[str | int] = []
    for part in re.findall(r"\.([^\s.\[\]]+)|\[(\d+)\]", selector[1:]):
        path.append(part[0] if part[0] else int(part[1]))
    return path


def _select(data: Any, path: Sequence[str | int]) -> tuple[bool, Any]:
    current = data
    for step in path:
        if isinstance(step, int):
            if not isinstance(current, list) or step >= len(current):
                return False, None
            current = current[step]
        else:
            if not isinstance(current, Mapping) or step not in current:
                return False, None
            current = current[step]
    return True, current


def _wildcard(expected: str) -> re.Pattern[str]:
    return re.compile("^" + ".*".join(re.escape(part) for part in expected.split("*")) + "$", re.DOTALL)


def _scalar_text(value: Any) -> str | None:
    if isinstance(value, str):
        return value
    if isinstance(value, bool):
        return "true" if value else "false"
    if value is None:
        return "null"
    if isinstance(value, (int, float)):
        return json.dumps(value)
    return None


class _JsonFilter:
    """A CloudWatch JSON filter pattern: ``{ <selector> <op> <value> && ... || ... }``."""

    def __init__(self, pattern: str) -> None:
        if not pattern.endswith("}"):
            raise _filter_error("a JSON pattern must end with '}'")
        self._tokens = _json_tokens(pattern[1:-1])
        self._position = 0
        self._test = self._disjunction()
        if self._position != len(self._tokens):
            raise _filter_error(f"unexpected {self._tokens[self._position][1]!r}")

    def __call__(self, message: str) -> bool:
        try:
            data = json.loads(message)
        except ValueError:
            return False
        return isinstance(data, (Mapping, list)) and self._test(data)

    def _peek(self) -> tuple[str, Any] | None:
        return self._tokens[self._position] if self._position < len(self._tokens) else None

    def _take(self) -> tuple[str, Any]:
        token = self._peek()
        if token is None:
            raise _filter_error("the JSON pattern ends too early")
        self._position += 1
        return token

    def _disjunction(self) -> Callable[[Any], bool]:
        parts = [self._conjunction()]
        while self._peek() == ("op", "||"):
            self._take()
            parts.append(self._conjunction())
        return parts[0] if len(parts) == 1 else (lambda data: any(part(data) for part in parts))

    def _conjunction(self) -> Callable[[Any], bool]:
        parts = [self._unary()]
        while self._peek() == ("op", "&&"):
            self._take()
            parts.append(self._unary())
        return parts[0] if len(parts) == 1 else (lambda data: all(part(data) for part in parts))

    def _unary(self) -> Callable[[Any], bool]:
        if self._peek() == ("op", "("):
            self._take()
            inner = self._disjunction()
            if self._take() != ("op", ")"):
                raise _filter_error("missing ')'")
            return inner
        kind, selector = self._take()
        if kind != "selector":
            raise _filter_error(f"expected a $ selector, got {selector!r}")
        return self._comparison(_selector_path(selector))

    def _comparison(self, path: list[str | int]) -> Callable[[Any], bool]:
        kind, value = self._take()
        if kind == "word" and value.upper() in ("IS", "NOT", "EXISTS"):
            return self._keyword(path, value.upper())
        if kind != "op" or value not in _COMPARISONS:
            raise _filter_error(f"expected a comparison after the selector, got {value!r}")
        operator = value
        value_kind, expected = self._take()
        if value_kind not in ("string", "word"):
            raise _filter_error(f"expected a value after {operator}")
        number = _number(expected) if value_kind == "word" else None
        compare = _COMPARISONS[operator]
        if number is not None:

            def numeric(data: Any) -> bool:
                found, actual = _select(data, path)
                if not found or isinstance(actual, bool) or not isinstance(actual, (int, float)):
                    return False
                return compare(actual, number)

            return numeric
        if operator not in ("=", "!="):
            raise _filter_error(f"{operator} needs a number")
        regex = _wildcard(str(expected))

        def textual(data: Any) -> bool:
            found, actual = _select(data, path)
            text = _scalar_text(actual) if found else None
            if text is None:
                return False
            return bool(regex.match(text)) is (operator == "=")

        return textual

    def _keyword(self, path: list[str | int], keyword: str) -> Callable[[Any], bool]:
        if keyword == "EXISTS":
            return lambda data: _select(data, path)[0]
        if keyword == "NOT":
            kind, value = self._take()
            if kind != "word" or str(value).upper() != "EXISTS":
                raise _filter_error("expected EXISTS after NOT")
            return lambda data: not _select(data, path)[0]
        kind, value = self._take()
        wanted = {"TRUE": True, "FALSE": False, "NULL": None}
        if kind != "word" or value.upper() not in wanted:
            raise _filter_error("expected TRUE, FALSE or NULL after IS")
        target = wanted[value.upper()]

        def test(data: Any) -> bool:
            found, actual = _select(data, path)
            return found and actual is target

        return test


def _number(text: str) -> float | None:
    try:
        return float(text)
    except ValueError:
        return None


def _log_filter(pattern: str | None) -> Callable[[str], bool]:
    """A predicate on log messages for a CloudWatch Logs filter pattern (see the module docstring).

    Raises the world's ``InvalidParameterException`` answer for a pattern it cannot read.
    """
    text = (pattern or "").strip()
    if not text:
        return lambda _message: True
    if text.startswith("{"):
        return _JsonFilter(text)
    if text.startswith("["):
        raise _filter_error("space-delimited patterns ([...]) are not supported by the offline world")
    if len(text) >= 2 and text.startswith("%") and text.endswith("%"):
        try:
            regex = re.compile(text[1:-1])
        except re.error as exc:
            raise _filter_error(f"invalid regular expression: {exc}") from None
        return lambda message: regex.search(message) is not None
    return _text_filter(text)


# =========================================================================== records


@dataclass
class _StateMachine:
    name: str
    arn: str
    type: str
    definition: str
    role_arn: str
    logging: dict[str, Any]
    created: datetime
    updated: datetime
    revision: str


@dataclass
class _ClientToken:
    token: str
    first_used: datetime
    redrive_date: datetime


@dataclass
class _Execution:
    arn: str
    name: str
    machine: _StateMachine
    machine_arn: str  # unqualified, or labelled for a Map Run child
    status: str
    start: datetime
    stop: datetime | None
    events: list[dict[str, Any]]
    error: str | None
    cause: str | None
    input: str
    output: str | None
    redrive_count: int
    redrive_date: datetime | None
    map_run_arn: str | None
    label: str | None
    version_arn: str | None
    definition: str  # the definition the execution ran
    definition_updated: datetime
    revision: str
    seq: int
    client_tokens: list[_ClientToken] = field(default_factory=list)


@dataclass
class _Function:
    name: str
    arn: str
    handler: str
    runtime: str
    log_group: str
    log_format: str
    code_sha256: str
    layers: tuple[str, ...]
    last_modified: datetime
    revision: str


@dataclass(frozen=True)
class _LogEvent:
    stream: str
    timestamp: int  # epoch ms
    ingestion: int  # epoch ms
    message: str
    event_id: str
    seq: int


@dataclass
class _S3Object:
    body: bytes
    etag: str
    last_modified: datetime
    content_type: str


@dataclass
class _Message:
    message_id: str
    body: str
    md5: str
    sent: datetime
    visible_at: datetime
    receives: int = 0
    first_received: datetime | None = None
    deleted: bool = False


@dataclass
class _Queue:
    url: str
    name: str
    arn: str
    attributes: dict[str, str]
    created: datetime
    messages: list[_Message] = field(default_factory=list)
    receipts: dict[str, _Message] = field(default_factory=dict)


def _definition_text(definition: str | Mapping[str, Any], substitutions: Mapping[str, str] | None) -> str:
    """The deployed definition: ``${Key}`` placeholders substituted, valid JSON, minimal ASL."""
    if isinstance(definition, Mapping):
        text = json.dumps(definition, indent=2)
    elif isinstance(definition, str):
        text = definition
    else:
        raise ValueError(f"a definition must be JSON text or a mapping, not {type(definition).__name__}")
    for key, value in (substitutions or {}).items():
        if not isinstance(key, str) or not key:
            raise ValueError(f"invalid substitution key {key!r}")
        text = text.replace("${" + key + "}", str(value))
    left = sorted(set(_PLACEHOLDER.findall(text)))
    if left:
        raise ValueError(f"the definition still has unsubstituted placeholders: {', '.join(left)}")
    try:
        parsed = json.loads(text)
    except ValueError as exc:
        raise ValueError(f"the definition is not valid JSON: {exc}") from None
    states = parsed.get("States") if isinstance(parsed, Mapping) else None
    start = parsed.get("StartAt") if isinstance(parsed, Mapping) else None
    if not isinstance(states, Mapping) or not states or not isinstance(start, str) or start not in states:
        raise ValueError("the definition is not a state machine: it needs StartAt naming one of its States")
    if not all(
        isinstance(state, Mapping) and isinstance(state.get("Type"), str) for state in states.values()
    ):
        raise ValueError("every state of the definition needs a Type")
    return text


def _revision(text: str) -> str:
    return _uuid_for("revision", text)


def _event_id(event: Mapping[str, Any]) -> int:
    return int(event["id"])


def _details(event: Mapping[str, Any]) -> Mapping[str, Any]:
    key = _TERMINAL_DETAILS.get(str(event.get("type")))
    details = event.get(key) if key else None
    return details if isinstance(details, Mapping) else {}


def _without_execution_data(event: Mapping[str, Any]) -> dict[str, Any]:
    """A history event without the ``input``/``output`` of its details (``includeExecutionData=False``)."""
    out: dict[str, Any] = {}
    for key, value in event.items():
        if key.endswith("EventDetails") and isinstance(value, Mapping):
            value = {name: item for name, item in value.items() if name not in ("input", "output")}
        out[key] = value
    return out


def _byte_range(header: Any, size: int) -> tuple[int, int] | None:
    """The ``(first, last)`` bytes of a ``Range`` header; ``None`` for the whole object (no header,
    or one S3 ignores); the world's ``InvalidRange`` answer when nothing of the object is in range."""
    if not isinstance(header, str):
        return None
    match = _RANGE.match(header.strip())
    if match is None or (not match.group(1) and not match.group(2)):
        return None
    first_text, last_text = match.groups()
    unsatisfiable = _AwsError(
        "InvalidRange",
        "The requested range is not satisfiable",
        416,
        ActualObjectSize=str(size),
        RangeRequested=header,
    )
    if not first_text:  # suffix range: the last N bytes
        length = int(last_text)
        if length == 0 or size == 0:
            raise unsatisfiable
        return max(size - length, 0), size - 1
    first = int(first_text)
    last = int(last_text) if last_text else size - 1
    if last_text and last < first:
        return None
    if first >= size:
        raise unsatisfiable
    return first, min(last, size - 1)


# =========================================================================== the world


class OfflineAws:
    """An in-memory AWS account (see the module docstring)."""

    def __init__(
        self,
        *,
        region: str = DEFAULT_REGION,
        account: str = DEFAULT_ACCOUNT,
        now: datetime | Callable[[], datetime] | None = None,
    ) -> None:
        if not isinstance(region, str) or not _REGION.match(region):
            raise ValueError(f"not an AWS region name: {region!r}")
        if not isinstance(account, str) or not _ACCOUNT.match(account):
            raise ValueError(f"not an AWS account id (12 digits): {account!r}")
        self.region = region
        self.account = account
        self.partition = arns.partition_for_region(region)
        self.calls: list[AwsCall] = []
        self.outage: OutageKind | None = None
        self._clock = _clock_of(now)
        self._lock = threading.RLock()
        self._machines: dict[str, _StateMachine] = {}  # unqualified ARN -> machine
        self._executions: dict[str, _Execution] = {}  # execution ARN -> execution
        self._functions: dict[str, _Function] = {}  # function name -> function
        self._log_groups: dict[str, dict[str, list[_LogEvent]]] = {}  # group -> stream -> events
        self._buckets: dict[str, dict[str, _S3Object]] = {}  # bucket -> key -> object
        self._queues: dict[str, _Queue] = {}  # queue URL -> queue
        self._failures: dict[tuple[str, str], deque[_AwsError]] = {}
        self._seq = itertools.count(1)

    def __repr__(self) -> str:
        return (
            f"OfflineAws(region={self.region!r}, account={self.account!r}, "
            f"state_machines={len(self._machines)}, executions={len(self._executions)})"
        )

    # ------------------------------------------------------------------ the clock
    def now(self) -> datetime:
        """The world clock: aware UTC, milliseconds (drives redrives, tokens, visibility)."""
        return _moment(self._clock(), "the world clock")

    def set_now(self, now: datetime | Callable[[], datetime] | None) -> None:
        """Move the world clock (a datetime, a callable returning one, or ``None``: real time)."""
        self._clock = _clock_of(now)

    # ------------------------------------------------------------------ scenario files
    @classmethod
    def load(cls, path: str | Path, **kwargs: Any) -> OfflineAws:
        """A world from a scenario file (keyword arguments override ``region``/``account``/``now``)."""
        _check_overrides(kwargs)
        source = Path(path)
        try:
            data = json.loads(source.read_text(encoding="utf-8"))
        except OSError as exc:
            raise ValueError(f"cannot read the offline AWS world file {source}: {exc}") from None
        except ValueError as exc:
            raise ValueError(f"invalid JSON in the offline AWS world file {source}: {exc}") from None
        try:
            return cls.from_mapping(data, base_dir=source.parent, **kwargs)
        except ValueError as exc:
            raise ValueError(f"invalid offline AWS world file {source}: {exc}") from None

    @classmethod
    def from_mapping(
        cls, data: Mapping[str, Any], *, base_dir: str | Path = ".", **kwargs: Any
    ) -> OfflineAws:
        """A world from parsed scenario JSON (see the module docstring); file references are
        relative to ``base_dir``. Keyword arguments override ``region``/``account``/``now``."""
        _check_overrides(kwargs)
        if not isinstance(data, Mapping):
            raise ValueError("an offline AWS world must be a JSON object")
        _only(
            data,
            {
                "description",
                "region",
                "account",
                "now",
                "state_machines",
                "functions",
                "executions",
                "log_events",
                "objects",
                "queues",
            },
            "the world",
        )
        now = kwargs["now"] if "now" in kwargs else _optional_moment(data.get("now"), "now")
        world = cls(
            region=kwargs.get("region") or data.get("region") or DEFAULT_REGION,
            account=kwargs.get("account") or data.get("account") or DEFAULT_ACCOUNT,
            now=now,
        )
        _Loader(world, Path(base_dir)).load(data)
        return world

    # ------------------------------------------------------------------ building the world
    def add_state_machine(
        self,
        name: str,
        definition: str | Mapping[str, Any],
        *,
        type: str = "STANDARD",
        substitutions: Mapping[str, str] | None = None,
        role_arn: str | None = None,
        logging: Mapping[str, Any] | None = None,
    ) -> str:
        """Add a state machine; returns its ARN.

        ``definition`` is JSON text or a mapping; ``${Key}`` placeholders are replaced by
        ``substitutions`` (CloudFormation's ``DefinitionSubstitutions``) and none may be left.
        The machine was created :data:`MACHINE_AGE` before the world clock.
        """
        _check_name(name, "state machine name")
        if type not in ("STANDARD", "EXPRESS"):
            raise ValueError(f"state machine {name}: type must be STANDARD or EXPRESS, not {type!r}")
        try:
            text = _definition_text(definition, substitutions)
        except ValueError as exc:
            raise ValueError(f"state machine {name}: {exc}") from None
        arn = arns.state_machine_arn(self.region, self.account, name, partition=self.partition)
        role = (
            role_arn
            or f"arn:{self.partition}:iam::{self.account}:role/service-role/StepFunctions-{name}-role"
        )
        if not _ROLE_ARN.match(role):
            raise ValueError(f"state machine {name}: role_arn {role!r} is not an IAM role ARN")
        with self._lock:
            if arn in self._machines:
                raise ValueError(f"state machine {name} already exists (update_state_machine redeploys it)")
            created = self.now() - MACHINE_AGE
            machine = _StateMachine(
                name=name,
                arn=arn,
                type=type,
                definition=text,
                role_arn=role,
                logging=dict(logging)
                if logging is not None
                else {"level": "OFF", "includeExecutionData": False},
                created=created,
                updated=created,
                revision=_revision(text),
            )
            _check_answer(
                "stepfunctions",
                "DescribeStateMachine",
                self._machine_answer(machine, arn, None),
                f"state machine {name}",
            )
            self._machines[arn] = machine
        return arn

    def update_state_machine(
        self,
        state_machine: str,
        definition: str | Mapping[str, Any],
        *,
        substitutions: Mapping[str, str] | None = None,
    ) -> str:
        """Redeploy a state machine (name or ARN) with a new definition; returns its ARN.

        Executions keep the definition they ran (``DescribeStateMachineForExecution``).
        """
        with self._lock:
            machine = self._machine_record(state_machine)
            try:
                text = _definition_text(definition, substitutions)
            except ValueError as exc:
                raise ValueError(f"state machine {machine.name}: {exc}") from None
            machine.definition = text
            machine.updated = max(self.now(), machine.created)
            machine.revision = _revision(f"{text}:{next(self._seq)}")
            return machine.arn

    def add_function(
        self,
        name: str,
        *,
        handler: str = "app.handler",
        runtime: str = "python3.11",
        log_group: str | None = None,
        log_format: str = "Text",
        code_sha256: str | None = None,
        layers: Iterable[str] = (),
    ) -> str:
        """Add a Lambda function; returns its ARN.

        ``log_group`` overrides ``/aws/lambda/<name>`` (``LoggingConfig.LogGroup``); ``layers`` are
        layer version ARNs or layer names (version 1).
        """
        if not isinstance(name, str) or not _FUNCTION_NAME.match(name):
            raise ValueError(f"invalid Lambda function name {name!r}: 1-64 letters, digits, '-' or '_'")
        if log_format not in ("Text", "JSON"):
            raise ValueError(f"function {name}: log_format must be Text or JSON, not {log_format!r}")
        group = log_group or f"/aws/lambda/{name}"
        if not _LOG_GROUP.match(group):
            raise ValueError(f"function {name}: invalid log group name {group!r}")
        arn = arns.lambda_function_arn(self.region, self.account, name, partition=self.partition)
        sha = code_sha256 or base64.b64encode(hashlib.sha256(name.encode("utf-8")).digest()).decode("ascii")
        function = _Function(
            name=name,
            arn=arn,
            handler=handler,
            runtime=runtime,
            log_group=group,
            log_format=log_format,
            code_sha256=sha,
            layers=tuple(self._layer_arn(layer) for layer in layers),
            last_modified=self.now() - timedelta(days=1),
            revision=_uuid_for("function", name, sha),
        )
        with self._lock:
            if name in self._functions:
                raise ValueError(f"function {name} already exists")
            _check_answer("lambda", "GetFunction", self._function_answer(function), f"function {name}")
            self._functions[name] = function
        return arn

    def add_execution(
        self,
        state_machine: str,
        name: str,
        *,
        status: str,
        start_date: Any,
        stop_date: Any = None,
        history: Iterable[Mapping[str, Any]] = (),
        error: str | None = None,
        cause: str | None = None,
        input: str = "{}",
        redrive_count: int = 0,
        redrive_date: Any = None,
        map_run_arn: str | None = None,
        version_arn: str | None = None,
    ) -> str:
        """Add an execution of a state machine (name or ARN) with its history; returns its ARN.

        A ``history`` is checked like AWS writes one: sequential event ids from 1, links to
        earlier events, non-decreasing timestamps, ``ExecutionStarted`` first, one
        ``ExecutionRedriven`` per redrive, the terminal event of ``status`` last; the dates,
        error, cause and input of the execution must agree with it (missing ones are taken from
        it). Without a history a minimal one is made up (started, redriven, terminal).
        ``map_run_arn`` makes the execution a child of that Distributed Map run
        (``...:execution:<machine>/<label>:<name>``).
        """
        _check_name(name, "execution name")
        what = f"execution {name}"
        if status not in EXECUTION_STATUSES:
            raise ValueError(f"{what}: status must be one of {', '.join(EXECUTION_STATUSES)}, not {status!r}")
        _check_int(redrive_count, f"{what}: redrive_count", 0)
        payload = _json_text(input, f"{what}: input")
        with self._lock:
            machine, qualifier, label = self._machine_reference(state_machine, what)
            if machine.type != "STANDARD":
                raise ValueError(f"{what}: the offline world keeps no executions of EXPRESS state machines")
            label, map_run = self._map_run(machine, label, map_run_arn, what)
            version = self._version(machine, qualifier, version_arn, what)
            machine_arn = f"{machine.arn}/{label}" if label else machine.arn
            arn = arns.execution_arn(machine_arn, name)
            if arn in self._executions:
                raise ValueError(f"{what} of {machine.name} already exists")
            execution = _Execution(
                arn=arn,
                name=name,
                machine=machine,
                machine_arn=machine_arn,
                status=status,
                start=_moment(start_date, f"{what}: start_date"),
                stop=_optional_moment(stop_date, f"{what}: stop_date"),
                events=[],
                error=error,
                cause=cause,
                input=payload,
                output=None,
                redrive_count=redrive_count,
                redrive_date=_optional_moment(redrive_date, f"{what}: redrive_date"),
                map_run_arn=map_run,
                label=label,
                version_arn=version,
                definition=machine.definition,
                definition_updated=machine.updated,
                revision=machine.revision,
                seq=next(self._seq),
            )
            events = [
                _normalized_event(raw, f"{what}: history[{index}]") for index, raw in enumerate(history)
            ]
            if events:
                _reconcile(execution, events, what)
            else:
                _check_dates(execution, what)
                execution.events = _synthetic_history(execution)
            _check_answer(
                "stepfunctions",
                "DescribeExecution",
                self._execution_answer(execution, metadata_only=False),
                what,
            )
            self._executions[arn] = execution
        return arn

    def finish_execution(
        self,
        execution_arn: str,
        *,
        status: str,
        stop_date: Any = None,
        error: str | None = None,
        cause: str | None = None,
        output: str | None = None,
        events: Iterable[Mapping[str, Any]] = (),
    ) -> None:
        """End a RUNNING execution (one that was started or redriven).

        ``events`` are appended to its history (``id``/``previousEventId`` may be left out: they
        continue the numbering), then the terminal event of ``status`` unless the last given
        event is one. ``stop_date`` defaults to the world clock.
        """
        if status not in _TERMINAL_EVENTS:
            raise ValueError(f"status must be one of {', '.join(_TERMINAL_EVENTS)}, not {status!r}")
        with self._lock:
            execution = self._executions.get(execution_arn)
            if execution is None:
                raise ValueError(f"unknown execution {execution_arn!r}")
            what = f"execution {execution.name}"
            if execution.status != "RUNNING":
                raise ValueError(f"{what} is {execution.status}, not RUNNING")
            stop = _moment(stop_date, f"{what}: stop_date") if stop_date is not None else self.now()
            added: list[dict[str, Any]] = []
            next_id = _event_id(execution.events[-1]) + 1
            for index, raw in enumerate(events):
                event = dict(raw) if isinstance(raw, Mapping) else raw
                if isinstance(event, dict):
                    event.setdefault("id", next_id)
                    event.setdefault("previousEventId", event["id"] - 1)
                    event.setdefault("timestamp", stop)
                added.append(_normalized_event(event, f"{what}: events[{index}]"))
                next_id = _event_id(added[-1]) + 1
            if not added or added[-1]["type"] != _TERMINAL_EVENTS[status]:
                added.append(
                    _terminal_event(
                        next_id, status, max(stop, _last_time(execution, added)), error, cause, output
                    )
                )
            candidate = execution.events + added
            _check_sequence(candidate, what)
            terminal = candidate[-1]
            details = _details(terminal)
            execution.events = candidate
            execution.status = status
            execution.stop = terminal["timestamp"]
            execution.error = error if error is not None else details.get("error")
            execution.cause = cause if cause is not None else details.get("cause")
            execution.output = output if output is not None else details.get("output")
            _check_answer(
                "stepfunctions",
                "DescribeExecution",
                self._execution_answer(execution, metadata_only=False),
                what,
            )

    def add_log_events(self, group: str, stream: str, events: Iterable[Mapping[str, Any]]) -> None:
        """Add CloudWatch log events (``{"timestamp": ..., "message": ...}``) to a log stream.

        An event may carry ``ingestion_time`` (default: :data:`DEFAULT_INGESTION_DELAY_MS` after
        its timestamp); ``FilterLogEvents`` shows it once the world clock reaches that time.
        """
        if not isinstance(group, str) or not _LOG_GROUP.match(group):
            raise ValueError(f"invalid log group name {group!r}")
        if not isinstance(stream, str) or not _LOG_STREAM.match(stream):
            raise ValueError(f"invalid log stream name {stream!r}")
        shape = _service_model("logs").shape_for("FilteredLogEvent")
        added: list[_LogEvent] = []
        with self._lock:
            for index, raw in enumerate(events):
                where = f"log event {index} of {group} / {stream}"
                if not isinstance(raw, Mapping):
                    raise ValueError(f"{where} must be a mapping")
                _only(raw, {"timestamp", "message", "ingestion_time"}, where)
                timestamp = _millis(_moment(raw.get("timestamp"), f"{where}: timestamp"))
                ingestion = (
                    _millis(_moment(raw["ingestion_time"], f"{where}: ingestion_time"))
                    if raw.get("ingestion_time") is not None
                    else timestamp + DEFAULT_INGESTION_DELAY_MS
                )
                if ingestion < timestamp:
                    raise ValueError(f"{where}: ingestion_time is before the event's timestamp")
                seq = next(self._seq)
                event = _LogEvent(
                    stream=stream,
                    timestamp=timestamp,
                    ingestion=ingestion,
                    message=raw.get("message"),
                    event_id=f"{timestamp:020d}{seq:036d}",
                    seq=seq,
                )
                _check_shape(_log_answer(event), shape, where)
                added.append(event)
            self._log_groups.setdefault(group, {}).setdefault(stream, []).extend(added)

    def put_object(self, bucket: str, key: str, body: bytes | str) -> None:
        """Store an S3 object (the bucket is created on first use)."""
        if not isinstance(bucket, str) or not _BUCKET.match(bucket) or ".." in bucket:
            raise ValueError(f"invalid S3 bucket name {bucket!r}")
        if not isinstance(key, str) or not key or len(key.encode("utf-8")) > 1024:
            raise ValueError(f"invalid S3 object key {key!r}: 1-1024 bytes")
        if isinstance(body, str):
            data = body.encode("utf-8")
        elif isinstance(body, (bytes, bytearray)):
            data = bytes(body)
        else:
            raise ValueError(f"an S3 object body must be bytes or text, not {type(body).__name__}")
        content_type = mimetypes.guess_type(key)[0] or "binary/octet-stream"
        etag = '"' + hashlib.md5(data, usedforsecurity=False).hexdigest() + '"'
        with self._lock:
            self._buckets.setdefault(bucket, {})[key] = _S3Object(data, etag, self.now(), content_type)

    def add_queue(self, queue_url: str, *, attributes: Mapping[str, Any] | None = None) -> str:
        """Add an SQS queue; returns its ARN. ``attributes`` override the defaults (values are
        text; a mapping, such as a ``RedrivePolicy``, is stored as JSON text)."""
        match = _QUEUE_URL.match(queue_url) if isinstance(queue_url, str) else None
        if match is None:
            raise ValueError(f"not an SQS queue URL: {queue_url!r}")
        valid = set(_service_model("sqs").shape_for("QueueAttributeName").enum) - {"All"}
        overrides: dict[str, str] = {}
        for name, value in (attributes or {}).items():
            if name not in valid:
                raise ValueError(f"queue {match.group('name')}: unknown attribute {name!r}")
            overrides[name] = json.dumps(value) if isinstance(value, Mapping) else str(value)
        with self._lock:
            if queue_url in self._queues:
                raise ValueError(f"queue {queue_url} already exists")
            created = self.now()
            arn = f"arn:{self.partition}:sqs:{self.region}:{match.group('account')}:{match.group('name')}"
            seconds = str(_millis(created) // 1000)
            defaults = {
                "QueueArn": arn,
                "VisibilityTimeout": str(DEFAULT_VISIBILITY_TIMEOUT),
                "MaximumMessageSize": str(MAX_SQS_MESSAGE_BYTES),
                "MessageRetentionPeriod": "345600",
                "DelaySeconds": "0",
                "ReceiveMessageWaitTimeSeconds": "0",
                "SqsManagedSseEnabled": "true",
                "CreatedTimestamp": seconds,
                "LastModifiedTimestamp": seconds,
            }
            self._queues[queue_url] = _Queue(
                queue_url, match.group("name"), arn, {**defaults, **overrides}, created
            )
        return arn

    def send_message(self, queue_url: str, body: str) -> str:
        """Queue an SQS message (the queue is created with default attributes on first use);
        returns its message id."""
        if not isinstance(body, str) or not body:
            raise ValueError("an SQS message body must be non-empty text")
        if len(body.encode("utf-8")) > MAX_SQS_MESSAGE_BYTES:
            raise ValueError(f"an SQS message body is at most {MAX_SQS_MESSAGE_BYTES} bytes")
        if _SQS_INVALID_CHARACTERS.search(body):
            raise ValueError("an SQS message body holds characters SQS does not accept")
        with self._lock:
            if queue_url not in self._queues:
                self.add_queue(queue_url)
            queue = self._queues[queue_url]
            sent = self.now()
            delay = int(queue.attributes.get("DelaySeconds") or 0)
            message = _Message(
                message_id=_uuid_for("message", queue_url, next(self._seq)),
                body=body,
                md5=hashlib.md5(body.encode("utf-8"), usedforsecurity=False).hexdigest(),
                sent=sent,
                visible_at=sent + timedelta(seconds=delay),
            )
            queue.messages.append(message)
            return message.message_id

    def fail_next(
        self, service: str, operation: str, code: str, message: str = "", *, status: int = 400
    ) -> None:
        """Answer the next call of ``operation`` with an AWS error (one-shot).

        Several calls queue several errors (first in, first out). ``operation`` is the API name
        (``DescribeExecution``) or the client method (``describe_execution``); ``service`` the
        botocore name (``stepfunctions``, ``logs``, ``lambda``, ``sqs``, ``s3``, ``sts``).
        """
        name = _service_name(service)
        api = _operation_name(name, operation)
        if not isinstance(code, str) or not code.strip():
            raise ValueError("an AWS error needs a code")
        if isinstance(status, bool) or not isinstance(status, int) or not 300 <= status <= 599:
            raise ValueError(f"an AWS error answer has an HTTP status from 300 to 599, not {status!r}")
        with self._lock:
            self._failures.setdefault((name, api), deque()).append(_AwsError(code, message or code, status))

    # ------------------------------------------------------------------ reading the world
    def state_machine_arn(self, name: str) -> str:
        """The ARN of state machine ``name`` in this world's region and account (no I/O)."""
        return arns.state_machine_arn(self.region, self.account, name, partition=self.partition)

    def execution_arn(self, state_machine: str, name: str) -> str:
        """The ARN of execution ``name`` of a state machine (name or ARN), known or not."""
        machine = state_machine if arns.is_arn(state_machine) else self.state_machine_arn(state_machine)
        return arns.execution_arn(machine, name)

    def function_arn(self, name: str) -> str:
        """The ARN of Lambda function ``name`` in this world."""
        return arns.lambda_function_arn(self.region, self.account, name, partition=self.partition)

    def queue_messages(self, queue_url: str) -> list[str]:
        """The bodies of the messages still in a queue (received or not), oldest first."""
        with self._lock:
            queue = self._queues.get(queue_url)
            return [message.body for message in queue.messages if not message.deleted] if queue else []

    def calls_to(self, service: str | None = None, operation: str | None = None) -> list[AwsCall]:
        """The recorded calls of a service and/or an operation (API name)."""
        wanted = _service_name(service) if service is not None else None
        with self._lock:
            return [
                call
                for call in self.calls
                if (wanted is None or call.service == wanted)
                and (operation is None or call.operation == operation)
            ]

    @contextlib.contextmanager
    def serve_s3_datasets(self) -> Iterator[OfflineAws]:
        """Inside the block, ``s3://`` dataset locators (:mod:`dag_healer.storage.s3`) read this
        world's objects; afterwards the default S3 client is used again."""
        from dag_healer.storage import s3

        s3.set_client_factory(lambda: self.client_factory("s3"))
        try:
            yield self
        finally:
            s3.set_client_factory(None)

    # ------------------------------------------------------------------ clients
    def client_factory(self, service: str) -> Any:
        """A botocore client of ``service`` answered by this world (an ``AwsClients`` factory)."""
        name = _service_name(service)
        client = _new_client(name, self.region)

        def caller_params(params: Mapping[str, Any], context: dict[str, Any] | None = None, **_: Any) -> None:
            if context is not None:
                context[_CONTEXT_KEY] = {"caller": _snapshot(params)}

        def effective_params(
            params: Mapping[str, Any], context: dict[str, Any] | None = None, **_: Any
        ) -> None:
            if context is not None:
                context.setdefault(_CONTEXT_KEY, {})["effective"] = params

        def answer(
            model: Any, params: Mapping[str, Any], context: dict[str, Any] | None = None, **_: Any
        ) -> Any:
            return self._answer(name, model, params, context or {})

        events = client.meta.events
        events.register_first(
            "before-parameter-build.*.*", caller_params, unique_id="dag-healer-offline-caller"
        )
        events.register_last(
            "before-parameter-build.*.*", effective_params, unique_id="dag-healer-offline-params"
        )
        events.register("before-call.*.*", answer, unique_id="dag-healer-offline-answer")
        return client

    @property
    def io_calls(self) -> int:
        """How many calls the world's clients made."""
        return len(self.calls)

    # ------------------------------------------------------------------ answering
    def _answer(
        self, service: str, model: Any, request: Mapping[str, Any], context: Mapping[str, Any]
    ) -> Any:
        """The ``before-call`` handler: ``(AWSResponse, parsed)`` for one call."""
        from botocore.awsrequest import AWSResponse, HeadersDict

        captured = context.get(_CONTEXT_KEY) or {}
        caller = captured.get("caller") or {}
        params = captured.get("effective") or caller
        operation = model.name
        url = str(request.get("url") or "")
        with self._lock:
            self.calls.append(AwsCall(service, operation, caller))
            outage = self.outage
            queued = self._failures.get((service, operation)) if outage is None else None
            planned = queued.popleft() if queued else None  # an outage leaves planned errors queued
        if outage == "network":
            from botocore.exceptions import EndpointConnectionError

            raise EndpointConnectionError(
                endpoint_url=url or f"https://{service}.{self.region}.amazonaws.com/"
            )
        try:
            if outage == "server_error":
                code = "InternalError" if service == "s3" else "InternalFailure"
                raise _AwsError(code, "We encountered an internal error. Please try again.", 500)
            if planned is not None:
                raise planned
            result = self._dispatch(service, model, params)
            parsed, headers = self._finish(service, model, result)
        except _AwsError as error:
            parsed, headers = self._error(service, error)
        logger.debug(
            "offline AWS: %s.%s answered HTTP %s",
            service,
            operation,
            parsed["ResponseMetadata"]["HTTPStatusCode"],
        )
        status = parsed["ResponseMetadata"]["HTTPStatusCode"]
        return AWSResponse(url, status, HeadersDict(headers), None), parsed

    def _dispatch(self, service: str, model: Any, params: Mapping[str, Any]) -> _Answer:
        handler = _HANDLERS.get((service, model.name))
        if handler is None:
            raise _AwsError(
                "UnsupportedOperation", f"the offline AWS world does not answer {service}:{model.name}"
            )
        code = _VALIDATION_CODES.get(service)
        if code is not None and model.input_shape is not None:
            problems = list(_constraint_violations(dict(params), model.input_shape))
            if problems:
                raise _AwsError(code, _validation_message(problems))
        with self._lock:
            return handler(self, params)

    def _finish(self, service: str, model: Any, result: _Answer) -> tuple[dict[str, Any], dict[str, str]]:
        """Validate an answer against the output shape and type it like botocore's parser."""
        shape = model.output_shape
        body = result.body
        if shape is not None:
            try:
                _check_shape(body, shape, f"the offline answer of {service}:{model.name}")
            except ValueError as exc:  # the world built an answer AWS would not send
                raise AssertionError(str(exc)) from None
            body = _as_botocore(body, shape, str(model.service_model.protocol))
        elif body:
            raise AssertionError(
                f"{service}:{model.name} answers nothing but the offline world answered {body}"
            )
        headers = self._headers(service, result.headers)
        parsed = dict(body)
        parsed["ResponseMetadata"] = self._metadata(service, result.status, headers)
        return parsed, headers

    def _error(self, service: str, error: _AwsError) -> tuple[dict[str, Any], dict[str, str]]:
        payload: dict[str, Any] = {"Code": error.code, "Message": error.message, **error.extra}
        if service == "sqs":
            reverse = {legacy: modeled for modeled, legacy in _SQS_QUERY_CODES.items()}
            modeled = error.code if error.code in _SQS_QUERY_CODES else reverse.get(error.code)
            if modeled is not None:
                payload.update(Code=_SQS_QUERY_CODES[modeled], QueryErrorCode=modeled, Type="Sender")
        headers = self._headers(service, {})
        return {"Error": payload, "ResponseMetadata": self._metadata(service, error.status, headers)}, headers

    def _headers(self, service: str, extra: Mapping[str, str]) -> dict[str, str]:
        request_id = _uuid_for("request", next(self._seq))
        headers = {"date": _http_date(self.now())}
        if service == "s3":
            headers.update(
                {"x-amz-request-id": request_id.replace("-", "")[:16].upper(), "server": "AmazonS3"}
            )
            headers["x-amz-id-2"] = base64.b64encode(hashlib.sha256(request_id.encode()).digest()).decode()
        else:
            headers["x-amzn-requestid"] = request_id
            headers["content-type"] = _CONTENT_TYPES.get(service, "application/x-amz-json-1.0")
        headers.update(extra)
        return headers

    @staticmethod
    def _metadata(service: str, status: int, headers: Mapping[str, str]) -> dict[str, Any]:
        metadata: dict[str, Any] = {
            "RequestId": headers.get("x-amz-request-id") or headers.get("x-amzn-requestid") or "",
            "HTTPStatusCode": status,
            "HTTPHeaders": dict(headers),
            "RetryAttempts": 0,
        }
        if service == "s3":
            metadata["HostId"] = headers.get("x-amz-id-2", "")
        return metadata

    # ------------------------------------------------------------------ lookups
    def _machine_record(self, state_machine: str) -> _StateMachine:
        machine, _qualifier, _label = self._machine_reference(state_machine, "state machine")
        return machine

    def _machine_reference(self, reference: str, what: str) -> tuple[_StateMachine, str | None, str | None]:
        """The machine of a name or a (qualified or labelled) ARN, its qualifier and label."""
        if not isinstance(reference, str) or not reference.strip():
            raise ValueError(f"{what}: expected a state machine name or ARN, got {reference!r}")
        if arns.is_arn(reference):
            try:
                parsed = arns.parse_state_machine_arn(reference)
            except ValueError:
                raise ValueError(f"{what}: {reference!r} is not a state machine ARN") from None
            machine = self._machines.get(parsed.unqualified)
            qualifier, label = parsed.qualifier, parsed.label
        else:
            name, _, label_text = reference.strip().partition("/")
            machine = self._machines.get(self.state_machine_arn(name))
            qualifier, label = None, label_text or None
        if machine is None:
            raise ValueError(f"{what}: unknown state machine {reference!r} (add_state_machine first)")
        return machine, qualifier, label

    def _map_run(
        self, machine: _StateMachine, label: str | None, map_run_arn: str | None, what: str
    ) -> tuple[str | None, str | None]:
        if map_run_arn is None:
            if label is not None:
                raise ValueError(f"{what}: a Map Run child execution needs map_run_arn")
            return None, None
        try:
            run = arns.parse_map_run_arn(map_run_arn)
        except ValueError:
            raise ValueError(f"{what}: {map_run_arn!r} is not a Map Run ARN") from None
        if (run.state_machine, run.region, run.account) != (machine.name, self.region, self.account):
            raise ValueError(f"{what}: Map Run {map_run_arn} does not belong to {machine.name}")
        if label is not None and label != run.label:
            raise ValueError(f"{what}: label {label!r} is not the label of Map Run {map_run_arn}")
        return run.label, map_run_arn

    def _version(
        self, machine: _StateMachine, qualifier: str | None, version_arn: str | None, what: str
    ) -> str | None:
        if version_arn is None:
            return f"{machine.arn}:{qualifier}" if qualifier and qualifier.isdigit() else None
        try:
            parsed = arns.parse_state_machine_arn(version_arn)
        except ValueError:
            raise ValueError(f"{what}: {version_arn!r} is not a state machine version ARN") from None
        if parsed.unqualified != machine.arn or not (parsed.qualifier or "").isdigit() or parsed.label:
            raise ValueError(f"{what}: {version_arn} is not a version ARN of {machine.name}")
        return version_arn

    def _layer_arn(self, layer: str) -> str:
        if not isinstance(layer, str) or not layer:
            raise ValueError(f"invalid Lambda layer {layer!r}")
        if layer.startswith("arn:"):
            return layer
        if not _LAYER_NAME.match(layer):
            raise ValueError(f"invalid Lambda layer name {layer!r}")
        return f"arn:{self.partition}:lambda:{self.region}:{self.account}:layer:{layer}:1"

    def _machine_by_arn(self, text: str) -> tuple[_StateMachine, arns.StateMachineArn]:
        try:
            parsed = arns.parse_state_machine_arn(text)
        except ValueError:
            raise _AwsError("InvalidArn", f"Invalid Arn: '{text}'") from None
        machine = None
        if (parsed.region, parsed.account) == (self.region, self.account):
            machine = self._machines.get(parsed.unqualified)
        if machine is None:
            raise _AwsError("StateMachineDoesNotExist", f"State Machine Does Not Exist: '{text}'")
        return machine, parsed

    def _execution_by_arn(self, text: str) -> _Execution:
        try:
            parsed = arns.parse_execution_arn(text)
        except ValueError:
            raise _AwsError("InvalidArn", f"Invalid Arn: '{text}'") from None
        execution = self._executions.get(str(parsed)) if not parsed.express else None
        if execution is None:
            raise _AwsError("ExecutionDoesNotExist", f"Execution Does Not Exist: '{text}'")
        return execution

    def _queue(self, url: Any) -> _Queue:
        queue = self._queues.get(url) if isinstance(url, str) else None
        if queue is None:
            raise _AwsError("QueueDoesNotExist", "The specified queue does not exist.")
        return queue

    # ------------------------------------------------------------------ page tokens
    def _page_token(self, operation: str, identity: Mapping[str, Any], offset: int) -> str:
        payload = {"o": operation, "k": _digest(identity), "n": offset, "t": _millis(self.now())}
        raw = json.dumps(payload, sort_keys=True, separators=(",", ":")).encode("utf-8")
        return "OFFLINE" + base64.urlsafe_b64encode(raw).decode("ascii").rstrip("=")

    def _page_offset(
        self, token: Any, operation: str, identity: Mapping[str, Any], *, code: str = "InvalidToken"
    ) -> int:
        """The offset a ``nextToken`` stands for (0 without one)."""
        if token is None:
            return 0
        try:
            if not isinstance(token, str) or not token.startswith("OFFLINE"):
                raise ValueError("unknown token")
            raw = token[len("OFFLINE") :]
            payload = json.loads(base64.urlsafe_b64decode(raw + "=" * (-len(raw) % 4)))
            offset, issued = payload["n"], payload["t"]
            if payload["o"] != operation or payload["k"] != _digest(identity):
                raise ValueError("the token belongs to another request")
            if not isinstance(offset, int) or offset < 0 or not isinstance(issued, int):
                raise ValueError("unknown token")
        except (ValueError, TypeError, KeyError) as exc:
            raise _AwsError(code, f"Invalid Token: '{exc}'") from None
        if self.now() - (_EPOCH + timedelta(milliseconds=issued)) > PAGE_TOKEN_TTL:
            raise _AwsError(code, "Invalid Token: 'the token has expired'")
        return offset

    def _page(
        self,
        items: Sequence[Any],
        params: Mapping[str, Any],
        operation: str,
        identity: Mapping[str, Any],
        *,
        size: int,
        token_key: str = "nextToken",
        code: str = "InvalidToken",
    ) -> tuple[Sequence[Any], str | None]:
        offset = self._page_offset(params.get(token_key), operation, identity, code=code)
        page = items[offset : offset + size]
        following = offset + size
        return page, self._page_token(operation, identity, following) if following < len(items) else None

    # ------------------------------------------------------------------ Step Functions answers
    def _machine_answer(
        self, machine: _StateMachine, requested: str, parsed: arns.StateMachineArn | None
    ) -> dict[str, Any]:
        answer: dict[str, Any] = {
            "stateMachineArn": requested,
            "name": machine.name,
            "status": "ACTIVE",
            "definition": machine.definition,
            "roleArn": machine.role_arn,
            "type": machine.type,
            "creationDate": machine.created,
            "loggingConfiguration": copy.deepcopy(machine.logging),
            "tracingConfiguration": {"enabled": False},
            "revisionId": machine.revision,
        }
        if parsed is not None and parsed.label:
            answer["label"] = parsed.label
        return answer

    def _execution_answer(self, execution: _Execution, *, metadata_only: bool) -> dict[str, Any]:
        redrive_status, reason = self._redrive_status(execution)
        answer: dict[str, Any] = {
            "executionArn": execution.arn,
            "stateMachineArn": execution.machine_arn,
            "name": execution.name,
            "status": execution.status,
            "startDate": execution.start,
            "redriveCount": execution.redrive_count,
            "redriveStatus": redrive_status,
            "redriveStatusReason": reason,
        }
        if execution.stop is not None:
            answer["stopDate"] = execution.stop
        if not metadata_only:
            answer["input"] = execution.input
            answer["inputDetails"] = {"included": True}
            if execution.output is not None:
                answer["output"] = execution.output
                answer["outputDetails"] = {"included": True}
        if execution.error is not None:
            answer["error"] = execution.error
        if execution.cause is not None:
            answer["cause"] = execution.cause
        if execution.redrive_date is not None:
            answer["redriveDate"] = execution.redrive_date
        if execution.map_run_arn is not None:
            answer["mapRunArn"] = execution.map_run_arn
        if execution.version_arn is not None:
            answer["stateMachineVersionArn"] = execution.version_arn
        return answer

    def _redrive_refusal(self, execution: _Execution) -> str | None:
        """Why an execution cannot be redriven now (``None``: it can)."""
        if execution.map_run_arn is not None:
            return "it was started by a Map Run: redrive the Map Run of its parent execution"
        if execution.status not in ("FAILED", "TIMED_OUT", "ABORTED"):
            return f"Execution is {execution.status} and cannot be redriven"
        if execution.start < REDRIVE_AVAILABLE_SINCE:
            return "Execution started before 2023-11-15 and cannot be redriven"
        if execution.stop is not None and self.now() - execution.stop > REDRIVE_WINDOW:
            return "Execution stopped more than 14 days ago and can no longer be redriven"
        if len(execution.events) >= MAX_REDRIVE_EVENTS:
            return f"Execution has {MAX_REDRIVE_EVENTS} or more history events and cannot be redriven"
        return None

    def _redrive_status(self, execution: _Execution) -> tuple[str, str]:
        if execution.map_run_arn is not None and execution.status in ("FAILED", "TIMED_OUT", "ABORTED"):
            return "REDRIVABLE_BY_MAP_RUN", "Execution can be redriven by redriving its Map Run"
        refusal = self._redrive_refusal(execution)
        if refusal is None:
            return "REDRIVABLE", f"Execution is {execution.status} and can be redriven"
        return "NOT_REDRIVABLE", refusal

    def _sfn_list_state_machines(self, params: Mapping[str, Any]) -> _Answer:
        machines = list(self._machines.values())
        page, token = self._page(
            machines, params, "ListStateMachines", {}, size=_page_size(params.get("maxResults"))
        )
        body: dict[str, Any] = {
            "stateMachines": [
                {"stateMachineArn": m.arn, "name": m.name, "type": m.type, "creationDate": m.created}
                for m in page
            ]
        }
        if token:
            body["nextToken"] = token
        return _Answer(body)

    def _sfn_describe_state_machine(self, params: Mapping[str, Any]) -> _Answer:
        machine, parsed = self._machine_by_arn(params["stateMachineArn"])
        return _Answer(self._machine_answer(machine, str(parsed), parsed))

    def _sfn_list_executions(self, params: Mapping[str, Any]) -> _Answer:
        machine_arn, map_run_arn = params.get("stateMachineArn"), params.get("mapRunArn")
        if bool(machine_arn) == bool(map_run_arn):
            raise _AwsError(
                "ValidationException", "Exactly one of stateMachineArn or mapRunArn must be specified"
            )
        status = params.get("statusFilter")
        if status == "PENDING_REDRIVE" and not map_run_arn:
            raise _AwsError("ValidationException", "statusFilter PENDING_REDRIVE needs a mapRunArn")
        if machine_arn:
            machine, parsed = self._machine_by_arn(machine_arn)
            if parsed.label:
                raise _AwsError(
                    "InvalidArn",
                    f"Invalid Arn: 'list the executions of a Map Run by mapRunArn: {machine_arn}'",
                )
            if machine.type != "STANDARD":
                raise _AwsError(
                    "StateMachineTypeNotSupported",
                    "This operation is not supported by EXPRESS state machines",
                )
            found = [e for e in self._executions.values() if e.machine is machine and e.map_run_arn is None]
            if parsed.qualifier:
                found = [e for e in found if e.version_arn == str(parsed)]
        else:
            try:
                arns.parse_map_run_arn(map_run_arn)
            except ValueError:
                raise _AwsError("InvalidArn", f"Invalid Arn: '{map_run_arn}'") from None
            found = [e for e in self._executions.values() if e.map_run_arn == map_run_arn]
            if not found:
                raise _AwsError("ResourceNotFound", f"Resource not found: '{map_run_arn}'")
        if status:
            found = [e for e in found if e.status == status]
        redrive = params.get("redriveFilter")
        if redrive == "REDRIVEN":
            found = [e for e in found if e.redrive_count > 0]
        elif redrive == "NOT_REDRIVEN":
            found = [e for e in found if e.redrive_count == 0]
        found.sort(key=lambda e: (e.stop or e.start, e.seq), reverse=True)
        identity = {"sm": machine_arn, "run": map_run_arn, "status": status, "redrive": redrive}
        page, token = self._page(
            found, params, "ListExecutions", identity, size=_page_size(params.get("maxResults"))
        )
        body: dict[str, Any] = {"executions": [_execution_item(e) for e in page]}
        if token:
            body["nextToken"] = token
        return _Answer(body)

    def _sfn_describe_execution(self, params: Mapping[str, Any]) -> _Answer:
        execution = self._execution_by_arn(params["executionArn"])
        metadata_only = params.get("includedData") == "METADATA_ONLY"
        return _Answer(self._execution_answer(execution, metadata_only=metadata_only))

    def _sfn_get_execution_history(self, params: Mapping[str, Any]) -> _Answer:
        execution = self._execution_by_arn(params["executionArn"])
        reverse = bool(params.get("reverseOrder", False))
        include = params.get("includeExecutionData", True) is not False
        events = list(reversed(execution.events)) if reverse else list(execution.events)
        identity = {"execution": execution.arn, "reverse": reverse}
        page, token = self._page(
            events, params, "GetExecutionHistory", identity, size=_page_size(params.get("maxResults"))
        )
        body: dict[str, Any] = {
            "events": [copy.deepcopy(event if include else _without_execution_data(event)) for event in page]
        }
        if token:
            body["nextToken"] = token
        return _Answer(body)

    def _sfn_describe_state_machine_for_execution(self, params: Mapping[str, Any]) -> _Answer:
        execution = self._execution_by_arn(params["executionArn"])
        machine = execution.machine
        answer: dict[str, Any] = {
            "stateMachineArn": execution.machine_arn,
            "name": machine.name,
            "definition": execution.definition,
            "roleArn": machine.role_arn,
            "updateDate": execution.definition_updated,
            "loggingConfiguration": copy.deepcopy(machine.logging),
            "tracingConfiguration": {"enabled": False},
            "revisionId": execution.revision,
        }
        if execution.map_run_arn is not None:
            answer["mapRunArn"] = execution.map_run_arn
            answer["label"] = execution.label
        return _Answer(answer)

    def _sfn_redrive_execution(self, params: Mapping[str, Any]) -> _Answer:
        execution = self._execution_by_arn(params["executionArn"])
        now = self.now()
        token = params.get("clientToken")
        if token:
            execution.client_tokens = [
                kept for kept in execution.client_tokens if now - kept.first_used <= CLIENT_TOKEN_TTL
            ][-CLIENT_TOKENS_KEPT:]
            for kept in execution.client_tokens:
                if kept.token == token:
                    return _Answer({"redriveDate": kept.redrive_date})
        refusal = self._redrive_refusal(execution)
        if refusal is not None:
            raise _AwsError(
                "ExecutionNotRedrivable", f"Execution with arn {execution.arn} cannot be redriven: {refusal}"
            )
        last = execution.events[-1]
        moment = max(now, last["timestamp"])
        execution.redrive_count += 1
        execution.redrive_date = moment
        execution.status = "RUNNING"
        execution.stop = None
        execution.error = execution.cause = execution.output = None
        execution.events.append(
            {
                "timestamp": moment,
                "type": "ExecutionRedriven",
                "id": _event_id(last) + 1,
                "previousEventId": _event_id(last),
                "executionRedrivenEventDetails": {"redriveCount": execution.redrive_count},
            }
        )
        if token:
            execution.client_tokens = [*execution.client_tokens, _ClientToken(token, now, moment)][
                -CLIENT_TOKENS_KEPT:
            ]
        return _Answer({"redriveDate": moment})

    def _sfn_start_execution(self, params: Mapping[str, Any]) -> _Answer:
        machine, parsed = self._machine_by_arn(params["stateMachineArn"])
        if parsed.label:
            raise _AwsError(
                "InvalidArn", f"Invalid Arn: 'a Map Run child state machine cannot be started: {parsed}'"
            )
        name = params.get("name") or _uuid_for("execution", machine.arn, next(self._seq))
        if not _NAME.match(name):
            raise _AwsError("InvalidName", f"Invalid Name: '{name}'")
        payload = params.get("input", "{}")
        try:
            json.loads(payload)
        except ValueError:
            raise _AwsError(
                "InvalidExecutionInput", "Invalid State Machine Execution Input: 'not valid JSON'"
            ) from None
        if len(payload.encode("utf-8")) > MAX_EXECUTION_INPUT_BYTES:
            raise _AwsError(
                "InvalidExecutionInput", "Invalid State Machine Execution Input: 'larger than 256 KiB'"
            )
        now = self.now()
        if machine.type == "EXPRESS":
            express = (
                f"arn:{self.partition}:states:{self.region}:{self.account}:express:{machine.name}:{name}"
            )
            return _Answer({"executionArn": f"{express}:{_uuid_for('express', name, now)}", "startDate": now})
        arn = arns.execution_arn(machine.arn, name)
        existing = self._executions.get(arn)
        if existing is not None:
            if existing.status == "RUNNING" and existing.input == payload:
                return _Answer({"executionArn": arn, "startDate": existing.start})
            raise _AwsError("ExecutionAlreadyExists", f"Execution Already Exists: '{arn}'")
        version = str(parsed) if parsed.qualifier and parsed.qualifier.isdigit() else None
        execution = _Execution(
            arn=arn,
            name=name,
            machine=machine,
            machine_arn=machine.arn,
            status="RUNNING",
            start=now,
            stop=None,
            events=[],
            error=None,
            cause=None,
            input=payload,
            output=None,
            redrive_count=0,
            redrive_date=None,
            map_run_arn=None,
            label=None,
            version_arn=version,
            definition=machine.definition,
            definition_updated=machine.updated,
            revision=machine.revision,
            seq=next(self._seq),
        )
        execution.events = _synthetic_history(execution)
        self._executions[arn] = execution
        return _Answer({"executionArn": arn, "startDate": now})

    # ------------------------------------------------------------------ CloudWatch Logs answers
    def _log_group_of(self, params: Mapping[str, Any]) -> str:
        name, identifier = params.get("logGroupName"), params.get("logGroupIdentifier")
        if bool(name) == bool(identifier):
            raise _AwsError(
                "InvalidParameterException", "Exactly one of logGroupName or logGroupIdentifier is required"
            )
        group = name or identifier
        if identifier and identifier.startswith("arn:"):
            parts = identifier.split(":", 6)
            if len(parts) < 7 or parts[2] != "logs" or parts[5] != "log-group":
                raise _AwsError("InvalidParameterException", "logGroupIdentifier is not a log group ARN")
            group = parts[6][: -len(":*")] if parts[6].endswith(":*") else parts[6]
        if group not in self._log_groups:
            raise _AwsError("ResourceNotFoundException", "The specified log group does not exist.")
        return group

    def _logs_filter_log_events(self, params: Mapping[str, Any]) -> _Answer:
        group = self._log_group_of(params)
        names, prefix = params.get("logStreamNames"), params.get("logStreamNamePrefix")
        if names and prefix:
            raise _AwsError(
                "InvalidParameterException", "logStreamNames and logStreamNamePrefix cannot be given together"
            )
        matches = _log_filter(params.get("filterPattern"))
        start, end = params.get("startTime"), params.get("endTime")
        visible_until = _millis(self.now())
        found: list[_LogEvent] = []
        for stream, events in self._log_groups[group].items():
            if (names and stream not in names) or (prefix and not stream.startswith(prefix)):
                continue
            found.extend(
                event
                for event in events
                if event.ingestion <= visible_until
                and (start is None or event.timestamp >= start)
                and (end is None or event.timestamp <= end)
                and matches(event.message)
            )
        found.sort(key=lambda event: (event.timestamp, event.seq))
        identity = {
            "group": group,
            "streams": sorted(names or []),
            "prefix": prefix,
            "start": start,
            "end": end,
            "pattern": params.get("filterPattern"),
        }
        size = params.get("limit") or DEFAULT_LOG_LIMIT
        page, token = self._page(
            found, params, "FilterLogEvents", identity, size=size, code="InvalidParameterException"
        )
        body: dict[str, Any] = {"events": [_log_answer(event) for event in page]}
        if token:
            body["nextToken"] = token
        return _Answer(body)

    # ------------------------------------------------------------------ Lambda answers
    def _function_answer(self, function: _Function) -> dict[str, Any]:
        logging_config: dict[str, Any] = {"LogFormat": function.log_format, "LogGroup": function.log_group}
        if function.log_format == "JSON":
            logging_config.update(ApplicationLogLevel="INFO", SystemLogLevel="INFO")
        code_location = (
            f"https://awslambda-{self.region}-tasks.s3.{self.region}.amazonaws.com/snapshots/{self.account}/"
            f"{function.name}-{function.revision}"
        )
        return {
            "Configuration": {
                "FunctionName": function.name,
                "FunctionArn": function.arn,
                "Runtime": function.runtime,
                "Role": f"arn:{self.partition}:iam::{self.account}:role/{function.name[:59]}-role",
                "Handler": function.handler,
                "CodeSize": 4096,
                "Description": "",
                "Timeout": 3,
                "MemorySize": 128,
                "LastModified": _lambda_time(function.last_modified),
                "CodeSha256": function.code_sha256,
                "Version": "$LATEST",
                "TracingConfig": {"Mode": "PassThrough"},
                "RevisionId": function.revision,
                "Layers": [{"Arn": layer, "CodeSize": 1024} for layer in function.layers],
                "State": "Active",
                "LastUpdateStatus": "Successful",
                "PackageType": "Zip",
                "Architectures": ["x86_64"],
                "EphemeralStorage": {"Size": 512},
                "SnapStart": {"ApplyOn": "None", "OptimizationStatus": "Off"},
                "LoggingConfig": logging_config,
            },
            "Code": {"RepositoryType": "S3", "Location": code_location},
        }

    def _lambda_get_function(self, params: Mapping[str, Any]) -> _Answer:
        reference = params["FunctionName"]
        match = _FUNCTION_REF.match(reference)
        if match is None:
            raise _AwsError(
                "ValidationException",
                _validation_message([_violation("functionName", "satisfy regular expression pattern")]),
            )
        account = match.group("account") or match.group("short_account") or self.account
        region = match.group("region") or self.region
        function = None
        if (region, account) == (self.region, self.account):
            function = self._functions.get(match.group("name"))
        wanted = f"arn:{self.partition}:lambda:{region}:{account}:function:{match.group('name')}"
        if function is None:
            raise _AwsError("ResourceNotFoundException", f"Function not found: {wanted}", 404, Type="User")
        qualifier = params.get("Qualifier") or match.group("qualifier")
        if qualifier not in (None, "$LATEST"):
            raise _AwsError(
                "ResourceNotFoundException", f"Function not found: {wanted}:{qualifier}", 404, Type="User"
            )
        return _Answer(self._function_answer(function))

    # ------------------------------------------------------------------ SQS answers
    def _queue_attributes(self, queue: _Queue) -> dict[str, str]:
        """The queue's attributes with the message counts of the world clock's moment."""
        now = self.now()
        live = [message for message in queue.messages if not message.deleted]
        attributes = dict(queue.attributes)
        attributes["ApproximateNumberOfMessages"] = str(sum(1 for m in live if m.visible_at <= now))
        attributes["ApproximateNumberOfMessagesNotVisible"] = str(
            sum(1 for m in live if m.receives and m.visible_at > now)
        )
        attributes["ApproximateNumberOfMessagesDelayed"] = str(
            sum(1 for m in live if not m.receives and m.visible_at > now)
        )
        return attributes

    def _sqs_receive_message(self, params: Mapping[str, Any]) -> _Answer:
        queue = self._queue(params["QueueUrl"])
        count = _sqs_bounded(params, "MaxNumberOfMessages", 1, 10, default=1)
        _sqs_bounded(params, "WaitTimeSeconds", 0, 20, default=0)
        default_visibility = int(queue.attributes.get("VisibilityTimeout") or DEFAULT_VISIBILITY_TIMEOUT)
        visibility = _sqs_bounded(params, "VisibilityTimeout", 0, 43_200, default=default_visibility)
        wanted = set(params.get("AttributeNames") or ()) | set(
            params.get("MessageSystemAttributeNames") or ()
        )
        now = self.now()
        received: list[dict[str, Any]] = []
        for message in queue.messages:
            if len(received) >= count:
                break
            if message.deleted or message.visible_at > now:
                continue
            message.receives += 1
            message.first_received = message.first_received or now
            message.visible_at = now + timedelta(seconds=visibility)
            receipt = "AQEB" + base64.b64encode(
                f"{message.message_id}:{message.receives}:{next(self._seq)}".encode()
            ).decode("ascii")
            queue.receipts[receipt] = message
            item: dict[str, Any] = {
                "MessageId": message.message_id,
                "ReceiptHandle": receipt,
                "MD5OfBody": message.md5,
                "Body": message.body,
            }
            attributes = _message_attributes(message, wanted, self.account)
            if attributes:
                item["Attributes"] = attributes
            received.append(item)
        return _Answer({"Messages": received} if received else {})

    def _sqs_delete_message_batch(self, params: Mapping[str, Any]) -> _Answer:
        queue = self._queue(params["QueueUrl"])
        entries = list(params["Entries"])
        if not entries:
            raise _AwsError(
                "EmptyBatchRequest",
                "There should be at least one DeleteMessageBatchRequestEntry in the request.",
            )
        if len(entries) > 10:
            raise _AwsError(
                "TooManyEntriesInBatchRequest",
                f"Maximum number of entries per request are 10. You have sent {len(entries)}.",
            )
        ids = [entry["Id"] for entry in entries]
        for entry_id in ids:
            if not _BATCH_ENTRY_ID.match(entry_id):
                raise _AwsError(
                    "InvalidBatchEntryId",
                    "A batch entry id can only contain alphanumeric characters, hyphens and underscores. "
                    "It can be at most 80 letters long.",
                )
        repeated = sorted({entry_id for entry_id in ids if ids.count(entry_id) > 1})
        if repeated:
            raise _AwsError("BatchEntryIdsNotDistinct", f"Id {repeated[0]} repeated.")
        successful: list[dict[str, Any]] = []
        failed: list[dict[str, Any]] = []
        for entry in entries:
            if self._delete(queue, entry["ReceiptHandle"]):
                successful.append({"Id": entry["Id"]})
            else:
                failed.append(
                    {
                        "Id": entry["Id"],
                        "SenderFault": True,
                        "Code": "ReceiptHandleIsInvalid",
                        "Message": "The input receipt handle is not a valid receipt handle.",
                    }
                )
        return _Answer({"Successful": successful, "Failed": failed})

    def _sqs_delete_message(self, params: Mapping[str, Any]) -> _Answer:
        queue = self._queue(params["QueueUrl"])
        if not self._delete(queue, params["ReceiptHandle"]):
            raise _AwsError(
                "ReceiptHandleIsInvalid", "The input receipt handle is not a valid receipt handle."
            )
        return _Answer({})

    @staticmethod
    def _delete(queue: _Queue, receipt: Any) -> bool:
        message = queue.receipts.get(receipt) if isinstance(receipt, str) else None
        if message is None:
            return False
        message.deleted = True
        return True

    def _sqs_get_queue_attributes(self, params: Mapping[str, Any]) -> _Answer:
        queue = self._queue(params["QueueUrl"])
        names = list(params.get("AttributeNames") or ())
        valid = set(_service_model("sqs").shape_for("QueueAttributeName").enum)
        unknown = [name for name in names if name not in valid]
        if unknown:
            raise _AwsError("InvalidAttributeName", f"Unknown Attribute {unknown[0]}.")
        attributes = self._queue_attributes(queue)
        selected = (
            attributes if "All" in names else {name: attributes[name] for name in names if name in attributes}
        )
        return _Answer({"Attributes": selected} if selected else {})

    # ------------------------------------------------------------------ S3 and STS answers
    def _s3_get_object(self, params: Mapping[str, Any]) -> _Answer:
        from botocore.response import StreamingBody

        bucket, key = params["Bucket"], params["Key"]
        objects = self._buckets.get(bucket)
        if objects is None:
            raise _AwsError("NoSuchBucket", "The specified bucket does not exist", 404, BucketName=bucket)
        stored = objects.get(key)
        if stored is None:
            raise _AwsError("NoSuchKey", "The specified key does not exist.", 404, Key=key)
        version = params.get("VersionId")
        if version not in (None, "null"):
            raise _AwsError("InvalidArgument", "Invalid version id specified", 400, ArgumentName="versionId")
        if params.get("IfMatch") not in (None, "*", stored.etag, stored.etag.strip('"')):
            raise _AwsError(
                "PreconditionFailed", "At least one of the pre-conditions you specified did not hold", 412
            )
        if params.get("IfNoneMatch") in ("*", stored.etag, stored.etag.strip('"')):
            raise _AwsError("304", "Not Modified", 304)
        if params.get("PartNumber") not in (None, 1):
            raise _AwsError("InvalidPartNumber", "The requested partnumber is not satisfiable", 416)
        size = len(stored.body)
        span = _byte_range(params.get("Range"), size)
        data = stored.body if span is None else stored.body[span[0] : span[1] + 1]
        body: dict[str, Any] = {
            "Body": StreamingBody(io.BytesIO(data), len(data)),
            "AcceptRanges": "bytes",
            "LastModified": stored.last_modified,
            "ContentLength": len(data),
            "ETag": stored.etag,
            "ContentType": stored.content_type,
            "Metadata": {},
        }
        headers = {
            "accept-ranges": "bytes",
            "last-modified": _http_date(stored.last_modified),
            "content-length": str(len(data)),
            "etag": stored.etag,
            "content-type": stored.content_type,
        }
        if span is None:
            return _Answer(body, headers=headers)
        body["ContentRange"] = headers["content-range"] = f"bytes {span[0]}-{span[1]}/{size}"
        return _Answer(body, status=206, headers=headers)

    def _sts_get_caller_identity(self, params: Mapping[str, Any]) -> _Answer:
        return _Answer(
            {
                "UserId": "AIDAOFFLINEWORLD0000001",
                "Account": self.account,
                "Arn": f"arn:{self.partition}:iam::{self.account}:user/dag-healer-offline",
            }
        )


_Handler = Callable[[OfflineAws, Mapping[str, Any]], _Answer]
_HANDLERS: dict[tuple[str, str], _Handler] = {
    ("stepfunctions", "ListStateMachines"): OfflineAws._sfn_list_state_machines,
    ("stepfunctions", "DescribeStateMachine"): OfflineAws._sfn_describe_state_machine,
    ("stepfunctions", "ListExecutions"): OfflineAws._sfn_list_executions,
    ("stepfunctions", "DescribeExecution"): OfflineAws._sfn_describe_execution,
    ("stepfunctions", "GetExecutionHistory"): OfflineAws._sfn_get_execution_history,
    (
        "stepfunctions",
        "DescribeStateMachineForExecution",
    ): OfflineAws._sfn_describe_state_machine_for_execution,
    ("stepfunctions", "RedriveExecution"): OfflineAws._sfn_redrive_execution,
    ("stepfunctions", "StartExecution"): OfflineAws._sfn_start_execution,
    ("logs", "FilterLogEvents"): OfflineAws._logs_filter_log_events,
    ("lambda", "GetFunction"): OfflineAws._lambda_get_function,
    ("sqs", "ReceiveMessage"): OfflineAws._sqs_receive_message,
    ("sqs", "DeleteMessageBatch"): OfflineAws._sqs_delete_message_batch,
    ("sqs", "DeleteMessage"): OfflineAws._sqs_delete_message,
    ("sqs", "GetQueueAttributes"): OfflineAws._sqs_get_queue_attributes,
    ("s3", "GetObject"): OfflineAws._s3_get_object,
    ("sts", "GetCallerIdentity"): OfflineAws._sts_get_caller_identity,
}
_CONTENT_TYPES = {
    "stepfunctions": "application/x-amz-json-1.0",
    "logs": "application/x-amz-json-1.1",
    "sqs": "application/x-amz-json-1.0",
    "lambda": "application/json",
    "sts": "text/xml",
}


def _execution_item(execution: _Execution) -> dict[str, Any]:
    item: dict[str, Any] = {
        "executionArn": execution.arn,
        "stateMachineArn": execution.machine_arn,
        "name": execution.name,
        "status": execution.status,
        "startDate": execution.start,
        "redriveCount": execution.redrive_count,
    }
    if execution.stop is not None:
        item["stopDate"] = execution.stop
    if execution.redrive_date is not None:
        item["redriveDate"] = execution.redrive_date
    if execution.map_run_arn is not None:
        item["mapRunArn"] = execution.map_run_arn
    if execution.version_arn is not None:
        item["stateMachineVersionArn"] = execution.version_arn
    return item


def _sqs_bounded(params: Mapping[str, Any], name: str, low: int, high: int, *, default: int) -> int:
    """An SQS integer parameter within ``[low, high]`` (``default`` when absent)."""
    value = params.get(name)
    if value is None:
        return default
    if not low <= value <= high:
        reason = f"Must be between {low} and {high}, if provided."
        raise _AwsError(
            "InvalidParameterValue", f"Value {value} for parameter {name} is invalid. Reason: {reason}"
        )
    return int(value)


def _log_answer(event: _LogEvent) -> dict[str, Any]:
    return {
        "logStreamName": event.stream,
        "timestamp": event.timestamp,
        "message": event.message,
        "ingestionTime": event.ingestion,
        "eventId": event.event_id,
    }


def _message_attributes(message: _Message, wanted: set[str], account: str) -> dict[str, str]:
    every = {
        "SenderId": f"AIDAOFFLINE{account}",
        "SentTimestamp": str(_millis(message.sent)),
        "ApproximateReceiveCount": str(message.receives),
        "ApproximateFirstReceiveTimestamp": str(_millis(message.first_received or message.sent)),
    }
    if "All" in wanted:
        return every
    return {name: value for name, value in every.items() if name in wanted}


# =========================================================================== histories


def _normalized_event(raw: Any, where: str) -> dict[str, Any]:
    """A copy of a history event with an aware ``timestamp``, validated against ``HistoryEvent``."""
    if not isinstance(raw, Mapping):
        raise ValueError(f"{where}: a history event must be a mapping")
    event = copy.deepcopy(dict(raw))
    event["timestamp"] = _moment(raw.get("timestamp"), f"{where}: timestamp")
    for key in ("id", "previousEventId"):
        if key in event and (isinstance(event[key], bool) or not isinstance(event[key], int)):
            raise ValueError(f"{where}: {key} must be an integer")
    _check_shape(event, _service_model("stepfunctions").shape_for("HistoryEvent"), where)
    return event


def _check_sequence(events: Sequence[Mapping[str, Any]], what: str) -> None:
    """Sequential ids from 1, links to earlier events, non-decreasing timestamps."""
    previous_time: datetime | None = None
    for position, event in enumerate(events, start=1):
        event_id = _event_id(event)
        if event_id != position:
            raise ValueError(
                f"{what}: history event ids must be sequential from 1 (found {event_id} at {position})"
            )
        link = event.get("previousEventId")
        if link is not None and not 0 <= link < event_id:
            raise ValueError(f"{what}: event {event_id} links to event {link}, which does not precede it")
        if previous_time is not None and event["timestamp"] < previous_time:
            raise ValueError(f"{what}: event {event_id} is older than the event before it")
        previous_time = event["timestamp"]
    if events and events[0].get("type") != "ExecutionStarted":
        raise ValueError(f"{what}: a history starts with ExecutionStarted, not {events[0].get('type')}")


def _check_dates(execution: _Execution, what: str) -> None:
    finished = execution.status in _TERMINAL_EVENTS
    if finished and execution.stop is None:
        raise ValueError(f"{what}: a {execution.status} execution needs a stop_date (or a history)")
    if not finished and execution.stop is not None:
        raise ValueError(f"{what}: a {execution.status} execution has no stop_date")
    if execution.stop is not None and execution.stop < execution.start:
        raise ValueError(f"{what}: stop_date is before start_date")
    if execution.redrive_count and execution.redrive_date is None:
        execution.redrive_date = execution.stop or execution.start
    if execution.redrive_date is not None:
        if not execution.redrive_count:
            raise ValueError(f"{what}: redrive_date is set but redrive_count is 0")
        if execution.redrive_date < execution.start or (
            execution.stop and execution.redrive_date > execution.stop
        ):
            raise ValueError(f"{what}: redrive_date must fall between start_date and stop_date")


#: ``add_execution`` parameters of the ``_Execution`` attributes a history can fill in.
_PARAMETERS = {"stop": "stop_date"}


def _agree(execution: _Execution, attribute: str, value: Any, what: str, source: str) -> None:
    """Take ``value`` from the history for an unset attribute; refuse a different given one."""
    given = getattr(execution, attribute)
    if value is None:
        return
    if given is None:
        setattr(execution, attribute, value)
    elif given != value:
        name = _PARAMETERS.get(attribute, attribute)
        raise ValueError(f"{what}: {name} does not match the {source} of its history")


def _reconcile(execution: _Execution, events: list[dict[str, Any]], what: str) -> None:
    """Check an execution against its history and complete it from there."""
    _check_sequence(events, what)
    started, last = events[0], events[-1]
    if execution.start != started["timestamp"]:
        raise ValueError(f"{what}: start_date does not match the ExecutionStarted event")
    details = started.get("executionStartedEventDetails")
    recorded_input = details.get("input") if isinstance(details, Mapping) else None
    if isinstance(recorded_input, str):
        if execution.input == "{}":
            execution.input = recorded_input
        elif execution.input != recorded_input:
            raise ValueError(f"{what}: input does not match the input of the ExecutionStarted event")
    redriven = [event for event in events if event.get("type") == "ExecutionRedriven"]
    counts = [(event.get("executionRedrivenEventDetails") or {}).get("redriveCount") for event in redriven]
    if len(redriven) != execution.redrive_count or counts != list(range(1, len(redriven) + 1)):
        raise ValueError(
            f"{what}: redrive_count {execution.redrive_count} does not match the ExecutionRedriven events "
            f"of its history (redriveCount {counts})"
        )
    if redriven:
        _agree(execution, "redrive_date", redriven[-1]["timestamp"], what, "last ExecutionRedriven event")
    terminal_type = _TERMINAL_EVENTS.get(execution.status)
    if terminal_type is None:
        if last.get("type") in _TERMINAL_DETAILS:
            raise ValueError(f"{what}: a {execution.status} execution cannot end with {last.get('type')}")
        if execution.stop is not None:
            raise ValueError(f"{what}: a {execution.status} execution has no stop_date")
    else:
        if last.get("type") != terminal_type:
            raise ValueError(
                f"{what}: a {execution.status} history ends with {terminal_type}, not {last.get('type')}"
            )
        _agree(execution, "stop", last["timestamp"], what, "terminal event")
        terminal = _details(last)
        _agree(execution, "error", terminal.get("error"), what, "terminal event")
        _agree(execution, "cause", terminal.get("cause"), what, "terminal event")
        _agree(execution, "output", terminal.get("output"), what, "terminal event")
    execution.events = events


def _last_time(execution: _Execution, added: Sequence[Mapping[str, Any]]) -> datetime:
    last = added[-1] if added else execution.events[-1]
    return last["timestamp"]


def _terminal_event(
    event_id: int,
    status: str,
    at: datetime,
    error: str | None,
    cause: str | None,
    output: str | None,
) -> dict[str, Any]:
    kind = _TERMINAL_EVENTS[status]
    if kind == "ExecutionSucceeded":
        details: dict[str, Any] = {
            "output": output if output is not None else "{}",
            "outputDetails": {"truncated": False},
        }
    else:
        details = {name: value for name, value in (("error", error), ("cause", cause)) if value is not None}
    return {
        "timestamp": at,
        "type": kind,
        "id": event_id,
        "previousEventId": event_id - 1,
        _TERMINAL_DETAILS[kind]: details,
    }


def _synthetic_history(execution: _Execution) -> list[dict[str, Any]]:
    """A minimal history: started, failed and redriven once per redrive, then the terminal event."""
    started: dict[str, Any] = {"input": execution.input, "inputDetails": {"truncated": False}}
    started["roleArn"] = execution.machine.role_arn
    if execution.version_arn is not None:
        started["stateMachineVersionArn"] = execution.version_arn
    events: list[dict[str, Any]] = [
        {
            "timestamp": execution.start,
            "type": "ExecutionStarted",
            "id": 1,
            "previousEventId": 0,
            "executionStartedEventDetails": started,
        }
    ]
    for count in range(1, execution.redrive_count + 1):
        at = execution.redrive_date or execution.start
        events.append(_terminal_event(len(events) + 1, "FAILED", at, execution.error, execution.cause, None))
        events.append(
            {
                "timestamp": at,
                "type": "ExecutionRedriven",
                "id": len(events) + 1,
                "previousEventId": len(events),
                "executionRedrivenEventDetails": {"redriveCount": count},
            }
        )
    if execution.status in _TERMINAL_EVENTS:
        assert execution.stop is not None
        events.append(
            _terminal_event(
                len(events) + 1,
                execution.status,
                execution.stop,
                execution.error,
                execution.cause,
                execution.output,
            )
        )
        if execution.status == "SUCCEEDED" and execution.output is None:
            execution.output = "{}"
    return events


# =========================================================================== scenario files


def _check_overrides(overrides: Mapping[str, Any]) -> None:
    unknown = sorted(set(overrides) - {"region", "account", "now"})
    if unknown:
        raise TypeError(f"unexpected keyword argument(s) {', '.join(unknown)} (region, account, now)")


def _only(data: Mapping[str, Any], allowed: set[str], where: str) -> None:
    unknown = sorted(str(key) for key in data if key not in allowed)
    if unknown:
        raise ValueError(f"{where}: unknown key(s) {', '.join(unknown)}")


def _items(data: Mapping[str, Any], key: str) -> list[Mapping[str, Any]]:
    value = data.get(key)
    if value is None:
        return []
    if not isinstance(value, list) or not all(isinstance(item, Mapping) for item in value):
        raise ValueError(f"{key} must be a list of objects")
    return value


def _exclusive(item: Mapping[str, Any], inline: str, from_file: str, where: str, *, required: bool) -> None:
    if inline in item and from_file in item:
        raise ValueError(f"{where}: give {inline} or {from_file}, not both")
    if required and inline not in item and from_file not in item:
        raise ValueError(f"{where}: {inline} or {from_file} is required")


class _Loader:
    """Turns scenario JSON into ``add_*`` calls (see the module docstring)."""

    def __init__(self, world: OfflineAws, base_dir: Path) -> None:
        self.world = world
        self.base_dir = base_dir

    def load(self, data: Mapping[str, Any]) -> None:
        for index, item in enumerate(_items(data, "state_machines")):
            self._state_machine(item, f"state_machines[{index}]")
        for index, item in enumerate(_items(data, "functions")):
            self._function(item, f"functions[{index}]")
        for index, item in enumerate(_items(data, "executions")):
            self._execution(item, f"executions[{index}]")
        for index, item in enumerate(_items(data, "log_events")):
            self._log_events(item, f"log_events[{index}]")
        for index, item in enumerate(_items(data, "objects")):
            self._object(item, f"objects[{index}]")
        for index, item in enumerate(_items(data, "queues")):
            self._queue(item, f"queues[{index}]")

    def _path(self, value: Any, where: str) -> Path:
        if not isinstance(value, str) or not value.strip():
            raise ValueError(f"{where}: expected a file path")
        path = Path(value)
        resolved = path if path.is_absolute() else self.base_dir / path
        if not resolved.is_file():
            raise ValueError(f"{where}: file {value} not found (resolved to {resolved})")
        return resolved

    def _json_file(self, value: Any, where: str) -> Any:
        path = self._path(value, where)
        text = path.read_text(encoding="utf-8")
        try:
            if path.suffix.lower() in (".yaml", ".yml"):
                import yaml

                return yaml.safe_load(text)
            return json.loads(text)
        except ValueError as exc:  # yaml.YAMLError derives from Exception, not ValueError
            raise ValueError(f"{where}: {value} is not valid JSON: {exc}") from None
        except Exception as exc:
            raise ValueError(f"{where}: {value} cannot be parsed: {type(exc).__name__}: {exc}") from None

    def _events_of(self, value: Any, where: str) -> list[Any]:
        events = value.get("events") if isinstance(value, Mapping) else value
        if not isinstance(events, list):
            raise ValueError(f"{where}: expected a list of events (or an object with an 'events' list)")
        return events

    def _state_machine(self, item: Mapping[str, Any], where: str) -> None:
        _only(
            item,
            {"name", "type", "definition", "definition_file", "substitutions", "role_arn", "logging"},
            where,
        )
        _exclusive(item, "definition", "definition_file", where, required=True)
        if "definition_file" in item:
            path = self._path(item["definition_file"], f"{where}.definition_file")
            definition: Any = path.read_text(encoding="utf-8")
            if path.suffix.lower() in (".yaml", ".yml"):
                definition = self._json_file(item["definition_file"], f"{where}.definition_file")
        else:
            definition = item["definition"]
        try:
            self.world.add_state_machine(
                item.get("name"),
                definition,
                type=item.get("type", "STANDARD"),
                substitutions=item.get("substitutions"),
                role_arn=item.get("role_arn"),
                logging=item.get("logging"),
            )
        except ValueError as exc:
            raise ValueError(f"{where}: {exc}") from None

    def _function(self, item: Mapping[str, Any], where: str) -> None:
        _only(item, {"name", "handler", "runtime", "log_group", "log_format", "code_sha256", "layers"}, where)
        try:
            self.world.add_function(
                item.get("name"),
                handler=item.get("handler", "app.handler"),
                runtime=item.get("runtime", "python3.11"),
                log_group=item.get("log_group"),
                log_format=item.get("log_format", "Text"),
                code_sha256=item.get("code_sha256"),
                layers=item.get("layers") or (),
            )
        except ValueError as exc:
            raise ValueError(f"{where}: {exc}") from None

    def _execution(self, item: Mapping[str, Any], where: str) -> None:
        _only(
            item,
            {
                "state_machine",
                "name",
                "status",
                "start_date",
                "stop_date",
                "history",
                "history_file",
                "error",
                "cause",
                "input",
                "redrive_count",
                "redrive_date",
                "map_run_arn",
                "version_arn",
            },
            where,
        )
        _exclusive(item, "history", "history_file", where, required=False)
        if "history_file" in item:
            history = self._events_of(self._json_file(item["history_file"], f"{where}.history_file"), where)
        else:
            history = self._events_of(item.get("history") or [], where)
        try:
            self.world.add_execution(
                item.get("state_machine"),
                item.get("name"),
                status=item.get("status"),
                start_date=item.get("start_date"),
                stop_date=item.get("stop_date"),
                history=history,
                error=item.get("error"),
                cause=item.get("cause"),
                input=item.get("input", "{}"),
                redrive_count=item.get("redrive_count", 0),
                redrive_date=item.get("redrive_date"),
                map_run_arn=item.get("map_run_arn"),
                version_arn=item.get("version_arn"),
            )
        except ValueError as exc:
            raise ValueError(f"{where}: {exc}") from None

    def _log_events(self, item: Mapping[str, Any], where: str) -> None:
        _only(item, {"group", "stream", "events", "events_file"}, where)
        _exclusive(item, "events", "events_file", where, required=True)
        if "events_file" in item:
            events = self._events_of(self._json_file(item["events_file"], f"{where}.events_file"), where)
        else:
            events = self._events_of(item["events"], where)
        try:
            self.world.add_log_events(item.get("group"), item.get("stream"), events)
        except ValueError as exc:
            raise ValueError(f"{where}: {exc}") from None

    def _object(self, item: Mapping[str, Any], where: str) -> None:
        _only(item, {"bucket", "key", "file", "body"}, where)
        _exclusive(item, "body", "file", where, required=True)
        body = self._path(item["file"], f"{where}.file").read_bytes() if "file" in item else item["body"]
        try:
            self.world.put_object(item.get("bucket"), item.get("key"), body)
        except ValueError as exc:
            raise ValueError(f"{where}: {exc}") from None

    def _queue(self, item: Mapping[str, Any], where: str) -> None:
        _only(item, {"url", "attributes", "messages"}, where)
        try:
            self.world.add_queue(item.get("url"), attributes=item.get("attributes"))
        except ValueError as exc:
            raise ValueError(f"{where}: {exc}") from None
        for index, message in enumerate(_items(item, "messages")):
            place = f"{where}.messages[{index}]"
            _only(message, {"body", "body_file"}, place)
            _exclusive(message, "body", "body_file", place, required=True)
            body = (
                self._path(message["body_file"], f"{place}.body_file").read_text(encoding="utf-8")
                if "body_file" in message
                else message["body"]
            )
            try:
                self.world.send_message(item["url"], body)
            except ValueError as exc:
                raise ValueError(f"{place}: {exc}") from None


__all__ = [
    "CLIENT_TOKENS_KEPT",
    "CLIENT_TOKEN_TTL",
    "DEFAULT_ACCOUNT",
    "DEFAULT_INGESTION_DELAY_MS",
    "DEFAULT_LOG_LIMIT",
    "DEFAULT_PAGE_SIZE",
    "DEFAULT_REGION",
    "DEFAULT_VISIBILITY_TIMEOUT",
    "EXECUTION_STATUSES",
    "MACHINE_AGE",
    "MAX_EXECUTION_INPUT_BYTES",
    "MAX_REDRIVE_EVENTS",
    "MAX_SQS_MESSAGE_BYTES",
    "PAGE_TOKEN_TTL",
    "REDRIVE_AVAILABLE_SINCE",
    "REDRIVE_WINDOW",
    "SERVICES",
    "AwsCall",
    "OfflineAws",
    "OfflineNetworkError",
    "OutageKind",
    "dummy_client",
    "refuse_aws_requests",
]
