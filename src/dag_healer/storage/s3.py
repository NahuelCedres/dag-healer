"""Datasets on S3 (section 9.13 of the design, v1, small).

``load_s3_dataset(locator)`` reads ``s3://bucket/key`` with ``GetObject``:

* CSV and JSON Lines are read with ``Range: bytes=0-<max_bytes - 1>``: an object larger than
  ``max_bytes`` gives its first records, the cut last line is dropped, a warning is logged and the
  frame's ``attrs["truncated"]`` is ``True`` (a row-count rule then sees a sample, not the whole
  object);
* JSON, Parquet and compressed objects (``.gz``, ``.bz2``, ``.zip``, ``.xz``, ``.zst`` or a
  ``compression`` option) are a capped full download: an object larger than ``max_bytes`` is a
  :class:`~dag_healer.datasets.DatasetError`.

The pandas readers and ``locator.options`` are the ones of local files (a ``.jsonl``/``.ndjson``
key defaults to ``lines=True``). One object is read: a "folder" of partitioned Parquet files is
not supported in v1. Errors are :class:`~dag_healer.datasets.DatasetError` naming the dataset and
a reason built from the AWS error code, never the AWS message (``NoSuchKey`` -> "dataset object
not found", ``AccessDenied`` -> "access denied", unreachable S3, missing credentials); reader
errors are masked like local ones. Masking of the rows is unchanged (the core masks).

The S3 client: :func:`set_client_factory` installs a process-wide factory (tests, offline worlds,
an adapter's session); it is called once and its client kept until the next
:func:`set_client_factory`. Otherwise a boto3 client of the default credential chain (adaptive
retries, short timeouts) is built on first use. boto3 (the ``aws`` extra) is imported here only,
lazily; pandas only when an object is parsed.

botocore logs whole request parameters and response bodies at DEBUG level (``dag-healer -vv``
puts the root logger there), every signed request with its headers (``Sending http request``:
``Authorization`` and the ``X-Amz-Security-Token`` of temporary credentials, an instance or task
role's), and the canonical request of every SigV4 signature: :func:`quiet_botocore_payloads`
installs :class:`PayloadLogFilter`, which drops those records, on every botocore logger. As a
backstop, the filter also drops any botocore record whose text holds an ``Authorization`` or
``X-Amz-Security-Token`` header or an AWS credential field (``SessionToken``,
``SecretAccessKey``...). It is installed when a default client is built here, when the Step
Functions adapter builds its clients and when the Amazon Bedrock LLM provider builds its
client; a client of :func:`set_client_factory` installs nothing by itself. From then on, a
debug run writes none of those records: no execution input or output, cause, log line or data
row that botocore logs, no signed request or signature, and no botocore record that holds one
of those credential headers or fields. The filter sees botocore's records only: those of the
anthropic SDK and of the HTTP transports under it have a filter of their own
(:mod:`dag_healer.cli.main`).
"""

from __future__ import annotations

import io
import logging
import posixpath
import re
import threading
from collections.abc import Callable, Mapping
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    import pandas as pd

    from dag_healer.contracts.schema import DatasetLocator

log = logging.getLogger(__name__)

S3_SCHEME = "s3://"
DEFAULT_MAX_BYTES = 50 * 1024 * 1024
MAX_ATTEMPTS = 10
CONNECT_TIMEOUT_SECONDS = 5
READ_TIMEOUT_SECONDS = 20

#: Bucket names botocore accepts (``validate_bucket_name``); AWS rules are stricter for new buckets.
_BUCKET = re.compile(r"^[A-Za-z0-9.\-_]{1,255}$")
_JSON_LINES_SUFFIXES = (".jsonl", ".ndjson")
#: Key suffixes pandas would infer a compression from, had it a file name.
_COMPRESSIONS = {".gz": "gzip", ".bz2": "bz2", ".zip": "zip", ".xz": "xz", ".zst": "zstd", ".tar": "tar"}
_CONTENT_RANGE = re.compile(r"^bytes (\d+)-(\d+)/(\d+|\*)$")
_NOT_FOUND = frozenset({"NoSuchKey", "404", "NotFound"})
_DENIED = frozenset(
    {
        "AccessDenied",
        "403",
        "AllAccessDisabled",
        "InvalidAccessKeyId",
        "SignatureDoesNotMatch",
        "ExpiredToken",
        "InvalidToken",
        "AccountProblem",
        "KMS.AccessDeniedException",
    }
)
_UNAVAILABLE = frozenset({"SlowDown", "ServiceUnavailable", "InternalError", "500", "503", "RequestTimeout"})
_CONNECTION_ERRORS = frozenset(
    {
        "EndpointConnectionError",
        "ConnectionClosedError",
        "ConnectTimeoutError",
        "ReadTimeoutError",
        "ProxyConnectionError",
        "SSLError",
        "HTTPClientError",
    }
)
_CREDENTIAL_ERRORS = frozenset({"NoCredentialsError", "PartialCredentialsError", "CredentialRetrievalError"})

#: botocore DEBUG records that carry request parameters, response bodies or the signed request
#: (its ``Authorization`` and ``X-Amz-Security-Token`` headers, the canonical request of its
#: signature): ``logger name -> message prefixes`` (the format strings botocore logs them with).
PAYLOAD_LOG_RECORDS: Mapping[str, tuple[str, ...]] = {
    "botocore.endpoint": ("Making request for", "Sending http request"),
    "botocore.parsers": ("Response body", "Response headers"),
    "botocore.auth": ("CanonicalRequest", "StringToSign", "Signature", "String to sign"),
}
#: Every logger of botocore (one per module that logs): :func:`quiet_botocore_payloads` installs
#: the filter on each of them, ahead of their creation, and on any other ``botocore.*`` logger
#: that already exists then. A logger's filters see only the records logged on that logger.
BOTOCORE_LOGGERS: tuple[str, ...] = (
    "botocore",
    "botocore.args",
    "botocore.auth",
    "botocore.awsrequest",
    "botocore.client",
    "botocore.compat",
    "botocore.compress",
    "botocore.configprovider",
    "botocore.credentials",
    "botocore.discovery",
    "botocore.endpoint",
    "botocore.endpoint_provider",
    "botocore.handlers",
    "botocore.history",
    "botocore.hooks",
    "botocore.httpchecksum",
    "botocore.httpsession",
    "botocore.loaders",
    "botocore.monitoring",
    "botocore.paginate",
    "botocore.parsers",
    "botocore.plugin",
    "botocore.regions",
    "botocore.response",
    "botocore.retries.adaptive",
    "botocore.retries.special",
    "botocore.retries.standard",
    "botocore.retryhandler",
    "botocore.session",
    "botocore.signers",
    "botocore.tokens",
    "botocore.useragent",
    "botocore.utils",
    "botocore.waiter",
)
#: The backstop: a header or a field that holds a credential, in any botocore record's text
#: (``'Authorization': b'AWS4-HMAC-SHA256 ...'``, ``x-amz-security-token:...``,
#: ``"SessionToken": "..."``, ``<SecretAccessKey>...``, ``aws_session_token = ...``).
_CREDENTIAL_FIELD = re.compile(
    r"\b(?:authorization|x-amz-security-token|(?:aws_)?session_?token|(?:aws_)?secret_?access_?key|token)\b"
    r"['\"]?\s*(?:[:=>]|%3D)",
    re.I,
)

_lock = threading.Lock()
_factory: Callable[[], Any] | None = None
_client: Any | None = None


class PayloadLogFilter(logging.Filter):
    """Drops the botocore records of :data:`PAYLOAD_LOG_RECORDS`, and any botocore record whose
    text holds a credential header or field (the backstop); the others pass."""

    def filter(self, record: logging.LogRecord) -> bool:
        if not record.name.startswith("botocore"):
            return True
        prefixes = PAYLOAD_LOG_RECORDS.get(record.name)
        message = record.msg
        if prefixes and isinstance(message, str) and message.startswith(prefixes):
            return False
        try:
            text = record.getMessage()
        except Exception:  # a record whose arguments do not format: judge its format string
            text = str(message)
        return _CREDENTIAL_FIELD.search(text) is None


_PAYLOAD_FILTER = PayloadLogFilter()


def quiet_botocore_payloads() -> None:
    """Install :class:`PayloadLogFilter` on every botocore logger (idempotent; no import of
    botocore: loggers are found, or made ahead of botocore, by name)."""
    existing = [
        name
        for name in list(logging.Logger.manager.loggerDict)
        if name == "botocore" or name.startswith("botocore.")
    ]
    for name in dict.fromkeys([*BOTOCORE_LOGGERS, *existing]):
        target = logging.getLogger(name)
        if not any(isinstance(item, PayloadLogFilter) for item in target.filters):
            target.addFilter(_PAYLOAD_FILTER)


def is_s3_uri(path: object) -> bool:
    """True for an ``s3://bucket/key`` string."""
    return isinstance(path, str) and path.strip().lower().startswith(S3_SCHEME)


def parse_s3_uri(uri: str) -> tuple[str, str]:
    """``(bucket, key)``; :class:`ValueError` without a bucket or a key.

    The scheme is case-insensitive; the key is kept as written (no URL decoding).
    """
    if not is_s3_uri(uri):
        raise ValueError(f"not an s3:// URI: {uri!r}")
    rest = uri.strip()[len(S3_SCHEME) :]
    bucket, _, key = rest.partition("/")
    if not bucket:
        raise ValueError(f"the S3 URI {uri!r} names no bucket")
    if not _BUCKET.match(bucket):
        raise ValueError(f"the S3 URI {uri!r} has an invalid bucket name {bucket!r}")
    if not key:
        raise ValueError(f"the S3 URI {uri!r} names no object key")
    return bucket, key


def set_client_factory(factory: Callable[[], Any] | None) -> None:
    """Use ``factory()`` for the S3 client from now on (``None``: back to the default chain)."""
    global _factory, _client
    if factory is not None and not callable(factory):
        raise TypeError(f"an S3 client factory must be callable, not {type(factory).__name__}")
    with _lock:
        _factory = factory
        _client = None


def s3_client() -> Any:
    """The S3 client: the installed factory's, else a cached default-chain boto3 client."""
    global _client
    with _lock:
        if _client is None:
            _client = _factory() if _factory is not None else _default_client()
        return _client


def _default_client() -> Any:
    """A boto3 S3 client of the default credential chain (adaptive retries, short timeouts)."""
    import boto3
    from botocore.config import Config

    from dag_healer import __version__

    quiet_botocore_payloads()
    config = Config(
        retries={"mode": "adaptive", "max_attempts": MAX_ATTEMPTS},
        connect_timeout=CONNECT_TIMEOUT_SECONDS,
        read_timeout=READ_TIMEOUT_SECONDS,
        user_agent_extra=f"dag-healer/{__version__}",
    )
    return boto3.session.Session().client("s3", config=config)


def load_s3_dataset(
    locator: DatasetLocator | Mapping[str, Any],
    *,
    client: Any | None = None,
    max_bytes: int = DEFAULT_MAX_BYTES,
) -> pd.DataFrame:
    """Load the S3 object of ``locator`` into a DataFrame (see the module docstring)."""
    from dag_healer.datasets import DatasetError, _as_locator

    if isinstance(max_bytes, bool) or not isinstance(max_bytes, int) or max_bytes < 1:
        raise ValueError(f"max_bytes must be a positive integer, not {max_bytes!r}")
    resolved = _as_locator(locator)
    kind = resolved.type
    try:
        bucket, key = parse_s3_uri(resolved.path)
    except ValueError as exc:
        raise DatasetError(f"invalid S3 dataset locator {resolved.path!r}: {exc}") from None
    options = dict(resolved.options)
    suffix = posixpath.splitext(key)[1].lower()
    if kind == "json" and "lines" not in options and suffix in _JSON_LINES_SUFFIXES:
        options["lines"] = True
    compressed = _compressed(options, suffix)
    ranged = not compressed and (kind == "csv" or (kind == "json" and bool(options.get("lines"))))
    what = f"{kind} dataset {resolved.path}"
    try:
        s3 = client if client is not None else s3_client()
        data, truncated = (
            _ranged(s3, bucket, key, max_bytes) if ranged else _capped(s3, bucket, key, max_bytes)
        )
    except _TooLarge as exc:
        raise DatasetError(f"cannot read {what}: {exc}") from None
    except ImportError:
        raise DatasetError(
            f"cannot read {what}: reading s3:// datasets needs boto3 (pip install 'dag-healer[aws]')"
        ) from None
    except Exception as exc:  # botocore raises ClientError and many connection/credential errors
        raise DatasetError(f"cannot read {what}: {_reason(exc)}") from exc
    if truncated:
        data = _whole_lines(data, what, max_bytes)
    frame = _parse(kind, data, options, what)
    if truncated:
        frame.attrs["truncated"] = True
        log.warning(
            "S3 dataset %s is larger than %d bytes: only its first %d rows were read",
            resolved.path,
            max_bytes,
            len(frame),
        )
    log.debug("Loaded S3 dataset %s (%d rows, %d columns)", resolved.path, len(frame), frame.shape[1])
    return frame


class _TooLarge(Exception):
    """An object above the cap of a full download."""


def _compressed(options: dict[str, Any], suffix: str) -> bool:
    """Whether the object is compressed (a byte range would cut the stream).

    pandas infers a compression from a file name, which a downloaded buffer lacks: for the
    default ``compression="infer"`` the key's suffix decides, and its compression is set on
    ``options``; an explicit ``compression`` option is kept as written.
    """
    value = options.get("compression", "infer")
    if value != "infer":
        return value is not None
    inferred = _COMPRESSIONS.get(suffix)
    if inferred is not None:
        options["compression"] = inferred
    return inferred is not None


def _get(s3: Any, bucket: str, key: str, **extra: Any) -> Mapping[str, Any]:
    response = s3.get_object(Bucket=bucket, Key=key, **extra)
    if not isinstance(response, Mapping) or not hasattr(response.get("Body"), "read"):
        raise TypeError("S3 returned no object body")
    return response


def _close(response: Mapping[str, Any]) -> None:
    close = getattr(response.get("Body"), "close", None)
    if callable(close):
        close()


def _ranged(s3: Any, bucket: str, key: str, max_bytes: int) -> tuple[bytes, bool]:
    """The first ``max_bytes`` bytes of an object and whether the object is larger."""
    from botocore.exceptions import ClientError

    try:
        response = _get(s3, bucket, key, Range=f"bytes=0-{max_bytes - 1}")
    except ClientError as exc:
        if _code(exc) != "InvalidRange":  # S3 answers 416 for a range request on an empty object
            raise
        return _capped(s3, bucket, key, max_bytes)
    try:
        data = response["Body"].read(max_bytes + 1)  # one byte more: a server that ignored the Range
    finally:
        _close(response)
    total = _content_range_total(response.get("ContentRange"))
    truncated = len(data) > max_bytes or (total is not None and total > len(data))
    return data[:max_bytes], truncated


def _capped(s3: Any, bucket: str, key: str, max_bytes: int) -> tuple[bytes, bool]:
    """The whole object; :class:`_TooLarge` above ``max_bytes``."""
    response = _get(s3, bucket, key)
    try:
        length = response.get("ContentLength")
        if isinstance(length, int) and not isinstance(length, bool) and length > max_bytes:
            raise _TooLarge(f"the object has {length} bytes, more than the {max_bytes}-byte limit")
        data = response["Body"].read(max_bytes + 1)
    finally:
        _close(response)
    if len(data) > max_bytes:
        raise _TooLarge(f"the object has more than the {max_bytes}-byte limit")
    return data, False


def _content_range_total(value: Any) -> int | None:
    """The object size of a ``ContentRange`` header (``bytes 0-99/1234``)."""
    match = _CONTENT_RANGE.match(value.strip()) if isinstance(value, str) else None
    if match is None or match.group(3) == "*":
        return None
    return int(match.group(3))


def _whole_lines(data: bytes, what: str, max_bytes: int) -> bytes:
    """``data`` without the line the byte range cut."""
    from dag_healer.datasets import DatasetError

    cut = data.rfind(b"\n")
    if cut < 0:
        raise DatasetError(f"cannot read {what}: its first line is longer than the {max_bytes}-byte limit")
    return data[: cut + 1]


def _parse(kind: str, data: bytes, options: dict[str, Any], what: str) -> pd.DataFrame:
    """The pandas reader of ``kind`` on the downloaded bytes."""
    import pandas as pd

    from dag_healer.datasets import DatasetError

    buffer = io.BytesIO(data)
    try:
        if kind == "csv":
            return pd.read_csv(buffer, **options)
        if kind == "parquet":
            return pd.read_parquet(buffer, **options)
        if kind == "json":
            return pd.read_json(buffer, **options)
    except Exception as exc:  # pandas/pyarrow raise many unrelated exception types
        from dag_healer.masking import mask_text

        reason = mask_text(f"{type(exc).__name__}: {exc}")
        raise DatasetError(f"cannot read {what}: {reason}") from exc
    raise DatasetError(f"unsupported dataset type {kind!r} (expected csv, parquet or json)")


def _code(exc: BaseException) -> str | None:
    response = getattr(exc, "response", None)
    error = response.get("Error") if isinstance(response, Mapping) else None
    code = error.get("Code") if isinstance(error, Mapping) else None
    return str(code) if code is not None else None


def _reason(exc: BaseException) -> str:
    """Why a read failed, from the AWS error code or the exception class (never AWS text)."""
    name = type(exc).__name__
    code = _code(exc)
    if code is None:
        names = {base.__name__ for base in type(exc).__mro__}
        if names & _CONNECTION_ERRORS:
            return f"S3 is not reachable ({name})"
        if names & _CREDENTIAL_ERRORS:
            return f"no usable AWS credentials ({name})"
        if name == "ParamValidationError":
            from dag_healer.masking import mask_text

            return mask_text(f"invalid S3 request: {exc}")
        return f"S3 read failed ({name})"
    if code in _NOT_FOUND:
        return f"dataset object not found ({code})"
    if code == "NoSuchBucket":
        return f"bucket not found ({code})"
    if code in _DENIED:
        return f"access denied ({code})"
    if code == "InvalidObjectState":
        return f"the object is archived and must be restored first ({code})"
    if code in _UNAVAILABLE or code.startswith("5"):
        return f"S3 is unavailable, try again later ({code})"
    return f"S3 refused the read ({code})"


__all__ = [
    "DEFAULT_MAX_BYTES",
    "PAYLOAD_LOG_RECORDS",
    "S3_SCHEME",
    "PayloadLogFilter",
    "is_s3_uri",
    "load_s3_dataset",
    "parse_s3_uri",
    "quiet_botocore_payloads",
    "s3_client",
    "set_client_factory",
]
