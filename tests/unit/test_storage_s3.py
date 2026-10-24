"""Datasets on S3 (``dag_healer.storage.s3``): byte ranges, capped downloads, errors, the client.

Request assertions use botocore's ``Stubber`` with exact parameters (the ``Range`` header above
all); behaviour against a whole S3 uses the offline world of ``stepfunctions.testing``. Every
test runs with dummy AWS credentials and empty AWS config files, and starts and ends with the
default client factory.
"""

from __future__ import annotations

import gzip
import io
import logging
import os
import re
import sys
from collections.abc import Iterator
from pathlib import Path
from typing import Any

import pandas as pd
import pytest
from botocore.exceptions import EndpointConnectionError, NoCredentialsError, ParamValidationError
from botocore.response import StreamingBody
from botocore.stub import Stubber

from dag_healer.datasets import DatasetError
from dag_healer.stepfunctions.testing import OfflineAws, dummy_client, refuse_aws_requests
from dag_healer.storage.s3 import (
    DEFAULT_MAX_BYTES,
    is_s3_uri,
    load_s3_dataset,
    parse_s3_uri,
    s3_client,
    set_client_factory,
)

S3_FIXTURES = Path(__file__).parent / "fixtures" / "stepfunctions" / "s3"
ORDERS_CSV = (S3_FIXTURES / "orders.csv").read_bytes()
ORDERS_JSONL = (S3_FIXTURES / "orders.jsonl").read_bytes()
ORDERS_JSON = (S3_FIXTURES / "orders.json").read_bytes()
BUCKET = "acme-landing"
KEY = "incoming/orders.csv"
URI = f"s3://{BUCKET}/{KEY}"
FULL_RANGE = f"bytes=0-{DEFAULT_MAX_BYTES - 1}"
#: Bytes of the header line and of the first data row of orders.csv.
HEADER_AND_ONE_ROW = 48 + 41


@pytest.fixture(autouse=True)
def no_aws_requests() -> Iterator[list[str]]:
    """No request leaves the process: botocore's sender refuses to send (``refuse_aws_requests``),
    and a test that tried fails at teardown."""
    with refuse_aws_requests() as refused:
        yield refused
    assert refused == [], f"requests that would have left the process: {refused}"


@pytest.fixture(autouse=True)
def aws_env(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> Iterator[None]:
    """Dummy credentials, IMDS off, empty AWS config files; the default S3 client factory."""
    for name in (
        "AWS_PROFILE",
        "AWS_DEFAULT_PROFILE",
        "AWS_ROLE_ARN",
        "AWS_WEB_IDENTITY_TOKEN_FILE",
        "AWS_ENDPOINT_URL",
    ):
        monkeypatch.delenv(name, raising=False)
    empty = tmp_path / "aws-config"
    empty.write_text("", encoding="utf-8")
    monkeypatch.setenv("AWS_ACCESS_KEY_ID", "testing")
    monkeypatch.setenv("AWS_SECRET_ACCESS_KEY", "testing")
    monkeypatch.setenv("AWS_SESSION_TOKEN", "testing")
    monkeypatch.setenv("AWS_DEFAULT_REGION", "eu-west-1")
    monkeypatch.setenv("AWS_EC2_METADATA_DISABLED", "true")
    monkeypatch.setenv("AWS_CONFIG_FILE", str(empty))
    monkeypatch.setenv("AWS_SHARED_CREDENTIALS_FILE", str(empty))
    set_client_factory(None)
    yield
    set_client_factory(None)


@pytest.fixture
def stubbed() -> Iterator[tuple[Any, Stubber]]:
    """An S3 client with an active Stubber; every queued answer must be used."""
    client = dummy_client("s3")
    with Stubber(client) as stubber:
        yield client, stubber
        stubber.assert_no_pending_responses()


def _answer(data: bytes, *, total: int | None = None, length: int | None = None) -> dict[str, Any]:
    """A ``GetObject`` answer (``total``: a ranged answer of an object of that size)."""
    answer: dict[str, Any] = {
        "Body": StreamingBody(io.BytesIO(data), len(data)),
        "ContentLength": len(data) if length is None else length,
        "ContentType": "text/csv",
    }
    if total is not None:
        answer["ContentRange"] = f"bytes 0-{max(len(data) - 1, 0)}/{total}"
    return answer


def _params(key: str = KEY, **extra: str) -> dict[str, str]:
    return {"Bucket": BUCKET, "Key": key, **extra}


@pytest.fixture
def world() -> OfflineAws:
    world = OfflineAws()
    world.put_object(BUCKET, KEY, ORDERS_CSV)
    return world


# =========================================================================== URIs


@pytest.mark.parametrize(
    ("value", "expected"),
    [
        ("s3://bucket/key.csv", True),
        ("  S3://bucket/key.csv ", True),
        ("s3:/bucket/key.csv", False),
        ("data/s3/key.csv", False),
        ("s3data/key.csv", False),
        (None, False),
        (Path("s3://bucket/key.csv"), False),
    ],
)
def test_is_s3_uri(value: object, expected: bool) -> None:
    assert is_s3_uri(value) is expected


@pytest.mark.parametrize(
    ("uri", "expected"),
    [
        ("s3://acme-landing/incoming/sales.csv", ("acme-landing", "incoming/sales.csv")),
        (" s3://acme-landing/a b/%20.csv ", ("acme-landing", "a b/%20.csv")),
        ("S3://Legacy_Bucket.1/k", ("Legacy_Bucket.1", "k")),
        ("s3://bucket//leading-slash", ("bucket", "/leading-slash")),
        ("s3://bucket/folder/", ("bucket", "folder/")),
    ],
)
def test_parse_s3_uri(uri: str, expected: tuple[str, str]) -> None:
    assert parse_s3_uri(uri) == expected


@pytest.mark.parametrize(
    ("uri", "message"),
    [
        ("s3://", "no bucket"),
        ("s3:///key", "no bucket"),
        ("s3://bucket", "no object key"),
        ("s3://bucket/", "no object key"),
        ("s3://bad bucket/key", "invalid bucket name"),
        ("https://bucket/key", "not an s3:// URI"),
    ],
)
def test_parse_s3_uri_refusals(uri: str, message: str) -> None:
    with pytest.raises(ValueError, match=message):
        parse_s3_uri(uri)


# =========================================================================== requests


def test_csv_is_read_with_a_byte_range(stubbed: tuple[Any, Stubber]) -> None:
    client, stubber = stubbed
    stubber.add_response("get_object", _answer(ORDERS_CSV, total=len(ORDERS_CSV)), _params(Range=FULL_RANGE))
    frame = load_s3_dataset({"type": "csv", "path": URI}, client=client)
    assert frame.equals(pd.read_csv(io.BytesIO(ORDERS_CSV)))
    assert "truncated" not in frame.attrs


def test_max_bytes_sets_the_range_and_a_cut_last_line_is_dropped(
    stubbed: tuple[Any, Stubber], caplog: pytest.LogCaptureFixture
) -> None:
    client, stubber = stubbed
    stubber.add_response(
        "get_object", _answer(ORDERS_CSV[:100], total=len(ORDERS_CSV)), _params(Range="bytes=0-99")
    )
    with caplog.at_level(logging.DEBUG, logger="dag_healer.storage.s3"):
        frame = load_s3_dataset({"type": "csv", "path": URI}, client=client, max_bytes=100)
    assert frame["order_id"].tolist() == [1001], "the second row was cut by the range"
    assert frame.attrs["truncated"] is True
    assert any("larger than 100 bytes" in record.getMessage() for record in caplog.records)
    assert not any("C-01" in record.getMessage() for record in caplog.records), "rows are never logged"


def test_a_server_that_ignores_the_range_is_still_capped(stubbed: tuple[Any, Stubber]) -> None:
    client, stubber = stubbed
    stubber.add_response(
        "get_object", _answer(ORDERS_CSV), _params(Range=f"bytes=0-{HEADER_AND_ONE_ROW + 9}")
    )
    frame = load_s3_dataset({"type": "csv", "path": URI}, client=client, max_bytes=HEADER_AND_ONE_ROW + 10)
    assert len(frame) == 1 and frame.attrs["truncated"] is True


def test_an_object_that_fits_exactly_is_not_truncated(stubbed: tuple[Any, Stubber]) -> None:
    client, stubber = stubbed
    size = len(ORDERS_CSV)
    stubber.add_response("get_object", _answer(ORDERS_CSV, total=size), _params(Range=f"bytes=0-{size - 1}"))
    frame = load_s3_dataset({"type": "csv", "path": URI}, client=client, max_bytes=size)
    assert len(frame) == 6 and "truncated" not in frame.attrs


def test_a_first_line_longer_than_the_cap_is_an_error(stubbed: tuple[Any, Stubber]) -> None:
    client, stubber = stubbed
    stubber.add_response(
        "get_object", _answer(ORDERS_CSV[:20], total=len(ORDERS_CSV)), _params(Range="bytes=0-19")
    )
    with pytest.raises(DatasetError, match="first line is longer than the 20-byte limit"):
        load_s3_dataset({"type": "csv", "path": URI}, client=client, max_bytes=20)


def test_an_empty_object_answers_invalid_range_and_is_read_whole(stubbed: tuple[Any, Stubber]) -> None:
    client, stubber = stubbed
    stubber.add_client_error(
        "get_object",
        "InvalidRange",
        "The requested range is not satisfiable",
        416,
        expected_params=_params(Range=FULL_RANGE),
    )
    stubber.add_response("get_object", _answer(b""), _params())
    with pytest.raises(DatasetError, match=f"cannot read csv dataset {URI}: EmptyDataError"):
        load_s3_dataset({"type": "csv", "path": URI}, client=client)


@pytest.mark.parametrize(
    ("key", "options", "data", "ranged"),
    [
        ("incoming/orders.jsonl", {}, ORDERS_JSONL, True),
        ("incoming/orders.ndjson", {}, ORDERS_JSONL, True),
        ("incoming/orders.json", {"lines": True}, ORDERS_JSONL, True),
        ("incoming/orders.json", {}, ORDERS_JSON, False),
        ("incoming/orders.jsonl", {"lines": False}, ORDERS_JSON, False),
    ],
)
def test_json_lines_are_ranged_and_json_is_downloaded_whole(
    stubbed: tuple[Any, Stubber], key: str, options: dict[str, Any], data: bytes, ranged: bool
) -> None:
    client, stubber = stubbed
    params = _params(key, Range=FULL_RANGE) if ranged else _params(key)
    stubber.add_response("get_object", _answer(data, total=len(data) if ranged else None), params)
    frame = load_s3_dataset(
        {"type": "json", "path": f"s3://{BUCKET}/{key}", "options": options}, client=client
    )
    assert frame["order_id"].tolist()[:2] == [1001, 1002]


def test_parquet_is_downloaded_whole(stubbed: tuple[Any, Stubber]) -> None:
    client, stubber = stubbed
    buffer = io.BytesIO()
    pd.DataFrame({"sku": ["A", "B"], "qty": [3, 4]}).to_parquet(buffer)
    stubber.add_response("get_object", _answer(buffer.getvalue()), _params("incoming/stock.parquet"))
    locator = {
        "type": "parquet",
        "path": f"s3://{BUCKET}/incoming/stock.parquet",
        "options": {"columns": ["qty"]},
    }
    frame = load_s3_dataset(locator, client=client)
    assert list(frame.columns) == ["qty"] and frame["qty"].tolist() == [3, 4]


@pytest.mark.parametrize(
    ("key", "options"),
    [("incoming/orders.csv.gz", {}), ("incoming/orders.bin", {"compression": "gzip"})],
)
def test_compressed_objects_are_downloaded_whole(
    stubbed: tuple[Any, Stubber], key: str, options: dict[str, Any]
) -> None:
    client, stubber = stubbed
    stubber.add_response("get_object", _answer(gzip.compress(ORDERS_CSV)), _params(key))
    frame = load_s3_dataset(
        {"type": "csv", "path": f"s3://{BUCKET}/{key}", "options": options}, client=client
    )
    assert len(frame) == 6


def test_a_full_download_above_the_cap_is_refused_by_its_length(stubbed: tuple[Any, Stubber]) -> None:
    client, stubber = stubbed
    stubber.add_response("get_object", _answer(b"[]", length=10**9), _params("incoming/orders.json"))
    with pytest.raises(DatasetError) as raised:
        load_s3_dataset({"type": "json", "path": f"s3://{BUCKET}/incoming/orders.json"}, client=client)
    assert str(raised.value) == (
        f"cannot read json dataset s3://{BUCKET}/incoming/orders.json: "
        f"the object has 1000000000 bytes, more than the {DEFAULT_MAX_BYTES}-byte limit"
    )


def test_a_full_download_above_the_cap_is_refused_by_its_body(stubbed: tuple[Any, Stubber]) -> None:
    client, stubber = stubbed
    answer = _answer(ORDERS_JSON)
    del answer["ContentLength"]
    stubber.add_response("get_object", answer, _params("incoming/orders.json"))
    with pytest.raises(DatasetError, match="the object has more than the 10-byte limit"):
        load_s3_dataset(
            {"type": "json", "path": f"s3://{BUCKET}/incoming/orders.json"}, client=client, max_bytes=10
        )


# =========================================================================== errors


@pytest.mark.parametrize(
    ("code", "status", "reason"),
    [
        ("NoSuchKey", 404, "dataset object not found (NoSuchKey)"),
        ("NoSuchBucket", 404, "bucket not found (NoSuchBucket)"),
        ("AccessDenied", 403, "access denied (AccessDenied)"),
        ("ExpiredToken", 400, "access denied (ExpiredToken)"),
        ("InvalidObjectState", 403, "the object is archived and must be restored first (InvalidObjectState)"),
        ("SlowDown", 503, "S3 is unavailable, try again later (SlowDown)"),
        ("InternalError", 500, "S3 is unavailable, try again later (InternalError)"),
        ("RequestTimeTooSkewed", 403, "S3 refused the read (RequestTimeTooSkewed)"),
    ],
)
def test_aws_errors_become_dataset_errors_without_the_aws_message(
    stubbed: tuple[Any, Stubber], code: str, status: int, reason: str
) -> None:
    client, stubber = stubbed
    stubber.add_client_error(
        "get_object",
        code,
        "detail for alice.pii@example.com",
        status,
        expected_params=_params(Range=FULL_RANGE),
    )
    with pytest.raises(DatasetError) as raised:
        load_s3_dataset({"type": "csv", "path": URI}, client=client)
    assert str(raised.value) == f"cannot read csv dataset {URI}: {reason}"
    assert "alice.pii" not in str(raised.value)


class _Failing:
    """An S3 client whose ``get_object`` raises ``error``."""

    def __init__(self, error: BaseException) -> None:
        self.error = error

    def get_object(self, **_params: Any) -> Any:
        raise self.error


@pytest.mark.parametrize(
    ("error", "reason"),
    [
        (
            EndpointConnectionError(endpoint_url="https://s3.eu-west-1.amazonaws.com"),
            "S3 is not reachable (EndpointConnectionError)",
        ),
        (NoCredentialsError(), "no usable AWS credentials (NoCredentialsError)"),
        (ParamValidationError(report="Invalid bucket name for alice.pii@example.com"), "invalid S3 request"),
        (RuntimeError("boom"), "S3 read failed (RuntimeError)"),
    ],
)
def test_other_failures_become_dataset_errors(error: BaseException, reason: str) -> None:
    with pytest.raises(DatasetError, match=re.escape(reason)) as raised:
        load_s3_dataset({"type": "csv", "path": URI}, client=_Failing(error))
    assert "alice.pii@example.com" not in str(raised.value)


def test_an_answer_without_a_body_is_an_error() -> None:
    class NoBody:
        def get_object(self, **_params: Any) -> dict[str, Any]:
            return {"ContentLength": 3}

    with pytest.raises(DatasetError, match=r"S3 read failed \(TypeError\)"):
        load_s3_dataset({"type": "csv", "path": URI}, client=NoBody())


def test_a_network_outage_of_the_offline_world(world: OfflineAws) -> None:
    world.outage = "network"
    with pytest.raises(DatasetError, match="S3 is not reachable"):
        load_s3_dataset({"type": "csv", "path": URI}, client=world.client_factory("s3"))


@pytest.mark.parametrize(
    ("locator", "error", "message"),
    [
        ({"type": "csv", "path": "s3://acme-landing"}, DatasetError, "invalid S3 dataset locator"),
        ({"type": "xlsx", "path": URI}, DatasetError, "invalid dataset locator"),
    ],
)
def test_invalid_locators(locator: dict[str, Any], error: type, message: str) -> None:
    with pytest.raises(error, match=message):
        load_s3_dataset(locator, client=_Failing(AssertionError("no call expected")))


@pytest.mark.parametrize("max_bytes", [0, -1, True, 1.5])
def test_max_bytes_must_be_a_positive_integer(max_bytes: Any) -> None:
    with pytest.raises(ValueError, match="max_bytes"):
        load_s3_dataset({"type": "csv", "path": URI}, client=_Failing(AssertionError()), max_bytes=max_bytes)


def test_reader_errors_are_masked(world: OfflineAws) -> None:
    world.put_object(BUCKET, "incoming/people.csv", "email,n\nana@example.com,1\n")
    locator = {
        "type": "csv",
        "path": f"s3://{BUCKET}/incoming/people.csv",
        "options": {"dtype": {"email": "float64"}},
    }
    with pytest.raises(DatasetError, match="cannot read csv dataset") as raised:
        load_s3_dataset(locator, client=world.client_factory("s3"))
    assert "ana@example.com" not in str(raised.value)


def test_reader_options_are_the_local_ones(world: OfflineAws) -> None:
    world.put_object(BUCKET, "incoming/semi.csv", "a;b\n1;x\n2;y\n")
    locator = {
        "type": "csv",
        "path": f"s3://{BUCKET}/incoming/semi.csv",
        "options": {"sep": ";", "usecols": ["b"]},
    }
    frame = load_s3_dataset(locator, client=world.client_factory("s3"))
    assert list(frame.columns) == ["b"] and frame["b"].tolist() == ["x", "y"]


# =========================================================================== the client


def test_the_installed_factory_is_called_once_and_kept(world: OfflineAws) -> None:
    made: list[Any] = []

    def factory() -> Any:
        made.append(world.client_factory("s3"))
        return made[-1]

    set_client_factory(factory)
    assert load_s3_dataset({"type": "csv", "path": URI})["order_id"].tolist()[0] == 1001
    assert load_s3_dataset({"type": "csv", "path": URI}).shape == (6, 5)
    assert len(made) == 1 and s3_client() is made[0]
    set_client_factory(factory)
    assert s3_client() is made[1], "installing a factory again drops the kept client"


def test_an_explicit_client_wins_over_the_factory(world: OfflineAws) -> None:
    set_client_factory(lambda: _Failing(AssertionError("the factory must not be used")))
    assert len(load_s3_dataset({"type": "csv", "path": URI}, client=world.client_factory("s3"))) == 6


def test_set_client_factory_refuses_non_callables() -> None:
    with pytest.raises(TypeError, match="callable"):
        set_client_factory("not a factory")


def test_the_default_client_is_a_cached_boto3_client() -> None:
    client = s3_client()
    assert client.meta.service_model.service_name == "s3"
    assert client.meta.config.retries == {"mode": "adaptive", "total_max_attempts": 11}
    assert (client.meta.config.connect_timeout, client.meta.config.read_timeout) == (5, 20)
    assert client.meta.config.user_agent_extra.startswith("dag-healer/")
    assert s3_client() is client


def test_the_default_client_keeps_payloads_out_of_botocore_debug_logs(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Building the default client installs the filter that drops botocore's request and response
    body records (a debug run would log them)."""
    import logging

    from dag_healer.storage import s3 as s3_module

    for name in s3_module.PAYLOAD_LOG_RECORDS:
        target = logging.getLogger(name)
        monkeypatch.setattr(
            target, "filters", [f for f in target.filters if not isinstance(f, s3_module.PayloadLogFilter)]
        )
    set_client_factory(None)  # forget a cached client: the next one is built
    s3_client()
    for name in s3_module.PAYLOAD_LOG_RECORDS:
        assert any(isinstance(item, s3_module.PayloadLogFilter) for item in logging.getLogger(name).filters)


def test_without_boto3_the_error_names_the_extra(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setitem(sys.modules, "boto3", None)
    with pytest.raises(DatasetError, match=r"needs boto3 \(pip install 'dag-healer\[aws\]'\)"):
        load_s3_dataset({"type": "csv", "path": URI})


def test_importing_the_module_imports_neither_boto3_nor_pandas() -> None:
    import subprocess

    code = (
        "import sys, dag_healer.storage.s3, dag_healer.datasets\n"
        "print(sorted({'boto3', 'botocore', 'pandas'} & {m.split('.')[0] for m in sys.modules}))"
    )
    env = {**os.environ, "PYTHONPATH": os.pathsep.join(entry for entry in sys.path if entry)}
    result = subprocess.run(
        [sys.executable, "-c", code], capture_output=True, text=True, check=True, timeout=120, env=env
    )
    assert result.stdout.strip() == "[]"
