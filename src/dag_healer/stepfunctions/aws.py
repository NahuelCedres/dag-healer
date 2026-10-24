"""AWS clients of the Step Functions adapter and the translation of their errors.

:class:`AwsClients` builds the botocore clients the adapter uses (``stepfunctions``, ``logs``,
``lambda``, ``sqs``, ``s3`` and ``sts``) on first use, from one boto3 session: the default
credential chain (ECS task role, IRSA, environment), a named ``profile`` for local
development, or an AssumeRole of ``role_arn`` (with ``role_external_id``) whose temporary
credentials refresh themselves. Every client uses adaptive retries (10 attempts) and short
timeouts. Building :class:`AwsClients` does no I/O and imports nothing from boto3: boto3 and
botocore are imported here only, and only when the first client is needed.

``client_factory`` replaces boto3 entirely: it is called with a service name and returns a
client. Tests pass botocore clients with dummy credentials and a ``botocore.stub.Stubber``,
or the clients of :class:`dag_healer.stepfunctions.testing.OfflineAws`.

Endpoints: ``options.endpoint_urls[service]``, else ``options.endpoint_url`` (one URL for every
client, as LocalStack serves them), else botocore's own resolution (``AWS_ENDPOINT_URL_<SERVICE>``,
the AWS config file, the regional endpoint): a VPC interface endpoint serves one service only.

botocore logs request parameters and response bodies at DEBUG level; building a client installs
:func:`dag_healer.storage.s3.quiet_botocore_payloads`, so those records (execution input and
output, causes, log lines) never reach the logs.

:func:`translate_error` turns botocore errors into :mod:`dag_healer.core.errors` exceptions:
throttling, 5xx answers and connection problems are transient; access denials (KMS included)
and missing credentials are :class:`~dag_healer.core.errors.PermissionDenied`; missing
executions, state machines, functions, objects and queues are
:class:`~dag_healer.core.errors.NotFound`; a configuration problem (no region, an unknown profile)
is a non-transient :class:`~dag_healer.core.errors.OrchestratorError` whose detail says what to
set.
"""

from __future__ import annotations

import logging
import threading
from collections.abc import Callable, Mapping
from typing import TYPE_CHECKING, Any

from dag_healer.core.errors import NotFound, OrchestratorError, PermissionDenied
from dag_healer.storage.s3 import quiet_botocore_payloads

if TYPE_CHECKING:
    from dag_healer.stepfunctions.options import StepFunctionsOptions

logger = logging.getLogger(__name__)

ClientFactory = Callable[[str], Any]
"""``factory(service_name) -> client`` (the Stubber / offline injection point)."""

SERVICES: tuple[str, ...] = ("stepfunctions", "logs", "lambda", "sqs", "s3", "sts")
MAX_ATTEMPTS = 10
CONNECT_TIMEOUT_SECONDS = 5
READ_TIMEOUT_SECONDS = 20

#: Error codes that mean "try again later".
TRANSIENT_CODES = frozenset(
    {
        "Throttling",
        "ThrottlingException",
        "ThrottledException",
        "TooManyRequestsException",
        "RequestLimitExceeded",
        "RequestThrottled",
        "RequestThrottledException",
        "ProvisionedThroughputExceededException",
        "SlowDown",
        "KmsThrottlingException",
        "ServiceUnavailable",
        "ServiceUnavailableException",
        "InternalFailure",
        "InternalServerError",
        "InternalError",
        "ServiceException",
        "RequestTimeout",
        "RequestTimeoutException",
        "PriorRequestNotComplete",
    }
)
#: Error codes of a refused call (credentials, IAM, KMS key policy).
PERMISSION_CODES = frozenset(
    {
        "AccessDenied",
        "AccessDeniedException",
        "UnauthorizedOperation",
        "UnrecognizedClientException",
        "InvalidClientTokenId",
        "InvalidAccessKeyId",
        "InvalidSignatureException",
        "IncompleteSignature",
        "MissingAuthenticationToken",
        "NotAuthorized",
        "OptInRequired",
        "SignatureDoesNotMatch",
        "AuthFailure",
        "ExpiredToken",
        "ExpiredTokenException",
        "KmsAccessDeniedException",
        "KMSAccessDeniedException",
        "KmsInvalidStateException",
        "InvalidSecurity",
    }
)
#: Error codes of a missing (or malformed, hence unknown) resource.
NOT_FOUND_CODES = frozenset(
    {
        "ExecutionDoesNotExist",
        "StateMachineDoesNotExist",
        "ActivityDoesNotExist",
        "ResourceNotFound",
        "ResourceNotFoundException",
        "InvalidArn",
        "NoSuchKey",
        "NoSuchBucket",
        "NotFound",
        "404",
        "QueueDoesNotExist",
        "AWS.SimpleQueueService.NonExistentQueue",
    }
)
_CONNECTION_ERRORS = (
    "EndpointConnectionError",
    "ConnectionClosedError",
    "ConnectTimeoutError",
    "ReadTimeoutError",
    "ConnectionError",
    "HTTPClientError",
    "ProxyConnectionError",
    "SSLError",
)
_CREDENTIAL_ERRORS = ("NoCredentialsError", "PartialCredentialsError")
#: botocore errors of a configuration problem, and what to set (a permission problem they are not).
CONFIGURATION_HINTS: Mapping[str, str] = {
    "NoRegionError": "set orchestrator.options.region or AWS_DEFAULT_REGION",
    "ProfileNotFound": "check orchestrator.options.profile and the AWS config files",
}


def client_config(options: StepFunctionsOptions | None = None) -> Any:
    """The ``botocore.config.Config`` of every adapter client (adaptive retries, short timeouts)."""
    from botocore.config import Config

    from dag_healer import __version__

    return Config(
        region_name=options.region if options is not None else None,
        retries={"mode": "adaptive", "max_attempts": MAX_ATTEMPTS},
        connect_timeout=CONNECT_TIMEOUT_SECONDS,
        read_timeout=READ_TIMEOUT_SECONDS,
        user_agent_extra=f"dag-healer/{__version__}",
    )


class AwsClients:
    """The adapter's AWS clients, built on first use (construction does no I/O).

    ``client_factory`` (tests, offline worlds) replaces the boto3 session: it is called once
    per service. Without it, one boto3 session is built from ``options`` (``profile``,
    ``region``, ``role_arn`` + ``role_external_id``; each client's endpoint is
    ``options.endpoint_for(service)``).
    """

    def __init__(self, options: StepFunctionsOptions, *, client_factory: ClientFactory | None = None) -> None:
        self.options = options
        self._factory = client_factory
        self._clients: dict[str, Any] = {}
        self._session: Any | None = None
        self._lock = threading.RLock()
        self.closed = False

    def __repr__(self) -> str:
        source = "injected" if self._factory is not None else "boto3"
        return f"AwsClients(region={self.options.region!r}, {source})"

    @property
    def injected(self) -> bool:
        """True when the clients come from a ``client_factory``."""
        return self._factory is not None

    def _boto3_session(self) -> Any:
        """The boto3 session (profile, region and an optional refreshing AssumeRole)."""
        if self._session is not None:
            return self._session
        import boto3

        options = self.options
        base = boto3.session.Session(profile_name=options.profile, region_name=options.region)
        if not options.role_arn:
            self._session = base
            return base
        import botocore.session
        from botocore.credentials import AssumeRoleCredentialFetcher, DeferredRefreshableCredentials
        from botocore.exceptions import NoCredentialsError

        source = base.get_credentials()
        if source is None:  # nothing to assume the role with: fail now, not at the first refresh
            raise NoCredentialsError()
        extra: dict[str, Any] = {"RoleSessionName": options.role_session_name}
        if options.role_external_id:
            extra["ExternalId"] = options.role_external_id
        fetcher = AssumeRoleCredentialFetcher(
            client_creator=base._session.create_client,  # botocore's documented AssumeRole recipe
            source_credentials=source,
            role_arn=options.role_arn,
            extra_args=extra,
        )
        credentials = DeferredRefreshableCredentials(
            method="assume-role", refresh_using=fetcher.fetch_credentials
        )
        core = botocore.session.Session(profile=options.profile)
        core._credentials = credentials
        self._session = boto3.session.Session(botocore_session=core, region_name=options.region)
        return self._session

    def client(self, service: str) -> Any:
        """The client of ``service`` (built on first use, then cached).

        A client that cannot be built (no region, an unknown profile, no credentials to assume
        ``role_arn`` with, a failing factory) raises a :mod:`dag_healer.core.errors` exception,
        like a failed call.
        """
        with self._lock:
            client = self._clients.get(service)
            if client is not None:
                return client
            quiet_botocore_payloads()
            try:
                if self._factory is not None:
                    client = self._factory(service)
                else:
                    kwargs: dict[str, Any] = {"config": client_config(self.options)}
                    endpoint = self.options.endpoint_for(service)
                    if endpoint:
                        kwargs["endpoint_url"] = endpoint
                    client = self._boto3_session().client(service, **kwargs)
            except Exception as exc:
                error = translate_error(exc, what=f"creating the {service} client")
                logger.debug(
                    "dag-healer stepfunctions: creating the %s client failed: %s", service, type(exc).__name__
                )
                if error is exc:
                    raise
                raise error from exc
            self._clients[service] = client
            logger.debug("dag-healer stepfunctions: created the %s client", service)
            return client

    @property
    def sfn(self) -> Any:
        return self.client("stepfunctions")

    @property
    def logs(self) -> Any:
        return self.client("logs")

    @property
    def lambda_(self) -> Any:
        return self.client("lambda")

    @property
    def sqs(self) -> Any:
        return self.client("sqs")

    @property
    def s3(self) -> Any:
        return self.client("s3")

    @property
    def sts(self) -> Any:
        return self.client("sts")

    def close(self) -> None:
        """Close the clients built so far (idempotent; the clients of an injected factory belong
        to whoever injected it and are only forgotten, not closed)."""
        with self._lock:
            clients = list(self._clients.values()) if self._factory is None else []
            self._clients.clear()
            self._session = None
            self.closed = True
        for client in clients:
            close = getattr(client, "close", None)
            if callable(close):
                try:
                    close()
                except Exception as exc:  # closing is best effort
                    logger.debug("dag-healer stepfunctions: closing a client failed: %s", type(exc).__name__)


# =========================================================================== errors


def error_code(exc: BaseException) -> str | None:
    """The AWS error code of a ``ClientError`` (``None`` for other exceptions)."""
    response = getattr(exc, "response", None)
    if isinstance(response, Mapping):
        error = response.get("Error")
        if isinstance(error, Mapping):
            code = error.get("Code")
            if code is not None:
                return str(code)
    return None


def status_code(exc: BaseException) -> int | None:
    """The HTTP status of a ``ClientError`` (``None`` when unknown)."""
    response = getattr(exc, "response", None)
    if isinstance(response, Mapping):
        meta = response.get("ResponseMetadata")
        if isinstance(meta, Mapping):
            status = meta.get("HTTPStatusCode")
            if isinstance(status, int) and not isinstance(status, bool):
                return status
    return None


def error_message(exc: BaseException) -> str:
    """The AWS error message of a ``ClientError`` (RAW: it may name resources), else ``str(exc)``."""
    response = getattr(exc, "response", None)
    if isinstance(response, Mapping):
        error = response.get("Error")
        if isinstance(error, Mapping) and error.get("Message"):
            return str(error["Message"])
    return str(exc) or type(exc).__name__


def is_transient(exc: BaseException) -> bool:
    """True for throttling, 5xx/429 answers and connection problems."""
    if isinstance(exc, OrchestratorError):
        return exc.transient
    if type(exc).__name__ in _CONNECTION_ERRORS or any(
        base.__name__ in _CONNECTION_ERRORS for base in type(exc).__mro__
    ):
        return True
    code = error_code(exc)
    status = status_code(exc)
    return (code in TRANSIENT_CODES) or (status is not None and (status >= 500 or status == 429))


def translate_error(exc: BaseException, *, what: str = "") -> OrchestratorError:
    """The :mod:`dag_healer.core.errors` exception for a botocore/boto3 error.

    The detail is ``"<what> failed: <Code>: <message>"`` (RAW AWS text; the core masks it).
    An :class:`~dag_healer.core.errors.OrchestratorError` is returned unchanged.
    """
    if isinstance(exc, OrchestratorError):
        return exc
    prefix = f"{what} failed: " if what else ""
    code = error_code(exc)
    status = status_code(exc)
    name = type(exc).__name__
    if code is None:
        detail = f"{prefix}{name}: {exc}" if str(exc) else f"{prefix}{name}"
        if is_transient(exc):
            return OrchestratorError(detail, transient=True)
        if name in _CREDENTIAL_ERRORS:
            return PermissionDenied(detail)
        hint = CONFIGURATION_HINTS.get(name)
        return OrchestratorError(f"{detail} ({hint})" if hint else detail, transient=False)
    detail = f"{prefix}{code}: {error_message(exc)}"
    if code in NOT_FOUND_CODES or status == 404:
        return NotFound(detail, status_code=status)
    if code in PERMISSION_CODES or status in (401, 403):
        return PermissionDenied(detail, status_code=status)
    return OrchestratorError(detail, transient=is_transient(exc), status_code=status)


def call(client: Any, operation: str, /, *, what: str = "", **params: Any) -> dict[str, Any]:
    """``client.<operation>(**params)`` with errors raised as :mod:`dag_healer.core.errors`.

    The log line names the operation and the error code, never the parameters or the message
    (they may carry identifiers or data).
    """
    try:
        method = getattr(client, operation)
        response = method(**params)
    except Exception as exc:
        error = translate_error(exc, what=what or operation)
        logger.debug(
            "dag-healer stepfunctions: %s failed: %s (%s, transient %s)",
            operation,
            type(exc).__name__,
            error_code(exc),
            error.transient,
        )
        if error is exc:
            raise
        raise error from exc
    return response if isinstance(response, dict) else dict(response or {})


__all__ = [
    "CONFIGURATION_HINTS",
    "CONNECT_TIMEOUT_SECONDS",
    "MAX_ATTEMPTS",
    "NOT_FOUND_CODES",
    "PERMISSION_CODES",
    "READ_TIMEOUT_SECONDS",
    "SERVICES",
    "TRANSIENT_CODES",
    "AwsClients",
    "ClientFactory",
    "call",
    "client_config",
    "error_code",
    "error_message",
    "is_transient",
    "status_code",
    "translate_error",
]
