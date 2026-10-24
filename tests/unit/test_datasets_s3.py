"""``dag_healer.datasets`` with ``s3://`` locators: the scheme dispatch to ``storage.s3``.

``load_dataset`` sends ``s3://bucket/key`` locators to S3 before any local path logic (the
repository root is not used), ``resolve_dataset_path`` refuses them, and local paths behave as
before (``tests/unit/test_datasets.py`` covers them unchanged). A contract on an S3 input is
validated like a local one: the renamed column of a day-2 export is a ``missing_column``
violation, an object that cannot be read a ``dataset_unreadable`` one.
"""

from __future__ import annotations

from collections.abc import Iterator
from pathlib import Path

import pandas as pd
import pytest

from dag_healer.contracts.schema import ColumnRule, Contract, DatasetContract, DatasetLocator
from dag_healer.contracts.validate import validate_contract
from dag_healer.datasets import DatasetError, load_dataset, resolve_dataset_path
from dag_healer.stepfunctions.testing import OfflineAws, refuse_aws_requests
from dag_healer.storage.s3 import set_client_factory

S3_FIXTURES = Path(__file__).parent / "fixtures" / "stepfunctions" / "s3"
BUCKET = "acme-landing"
URI = f"s3://{BUCKET}/incoming/orders.csv"


@pytest.fixture(autouse=True)
def no_aws_requests() -> Iterator[list[str]]:
    """No request leaves the process: botocore's sender refuses to send (``refuse_aws_requests``),
    and a test that tried fails at teardown."""
    with refuse_aws_requests() as refused:
        yield refused
    assert refused == [], f"requests that would have left the process: {refused}"


@pytest.fixture(autouse=True)
def aws_env(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> Iterator[None]:
    """Dummy AWS credentials and config files; the default S3 client factory before and after."""
    empty = tmp_path / "aws-config"
    empty.write_text("", encoding="utf-8")
    monkeypatch.delenv("AWS_PROFILE", raising=False)
    monkeypatch.setenv("AWS_ACCESS_KEY_ID", "testing")
    monkeypatch.setenv("AWS_SECRET_ACCESS_KEY", "testing")
    monkeypatch.setenv("AWS_DEFAULT_REGION", "eu-west-1")
    monkeypatch.setenv("AWS_EC2_METADATA_DISABLED", "true")
    monkeypatch.setenv("AWS_CONFIG_FILE", str(empty))
    monkeypatch.setenv("AWS_SHARED_CREDENTIALS_FILE", str(empty))
    set_client_factory(None)
    yield
    set_client_factory(None)


@pytest.fixture
def world() -> Iterator[OfflineAws]:
    """The day-2 export (``amount`` renamed to ``amt``) and the day-1 one, served as S3 datasets."""
    day1 = (S3_FIXTURES / "orders.csv").read_text(encoding="utf-8")
    world = OfflineAws()
    world.put_object(BUCKET, "incoming/orders.csv", day1.replace("amount", "amt"))
    world.put_object(BUCKET, "archive/orders-day1.csv", day1)
    world.put_object(BUCKET, "incoming/orders.jsonl", (S3_FIXTURES / "orders.jsonl").read_bytes())
    with world.serve_s3_datasets():
        yield world


def test_s3_locators_are_read_from_s3_not_from_the_repository(world: OfflineAws, tmp_path: Path) -> None:
    frame = load_dataset({"type": "csv", "path": URI}, tmp_path)
    assert list(frame.columns) == ["order_id", "customer_id", "amt", "currency", "ordered_at"]
    assert len(frame) == 6
    assert [(call.operation, call.params["Key"]) for call in world.calls] == [
        ("GetObject", "incoming/orders.csv")
    ]


def test_s3_locators_accept_models_mappings_and_reader_options(world: OfflineAws) -> None:
    locator = DatasetLocator(type="json", path=f"s3://{BUCKET}/incoming/orders.jsonl")
    assert load_dataset(locator, ".")["currency"].tolist() == ["EUR", "EUR", "EUR", "USD"]
    frame = load_dataset({"type": "csv", "path": URI, "options": {"usecols": ["order_id"]}}, ".")
    assert list(frame.columns) == ["order_id"]
    same = load_dataset({"type": "csv", "path": f"S3://{BUCKET}/archive/orders-day1.csv"}, ".")
    assert same.equals(pd.read_csv(S3_FIXTURES / "orders.csv"))


def test_a_missing_s3_object_is_a_dataset_error(world: OfflineAws) -> None:
    with pytest.raises(DatasetError, match="dataset object not found"):
        load_dataset({"type": "csv", "path": f"s3://{BUCKET}/incoming/missing.csv"}, ".")


def test_resolve_dataset_path_refuses_s3_locators_and_keeps_local_ones(tmp_path: Path) -> None:
    with pytest.raises(DatasetError, match="is an S3 object, not a local file"):
        resolve_dataset_path({"type": "csv", "path": URI}, tmp_path)
    assert (
        resolve_dataset_path({"type": "csv", "path": "s3data/orders.csv"}, tmp_path)
        == tmp_path / "s3data/orders.csv"
    )


def test_local_paths_that_only_look_like_s3_stay_local(tmp_path: Path) -> None:
    folder = tmp_path / "s3"
    folder.mkdir()
    (folder / "orders.csv").write_bytes((S3_FIXTURES / "orders.csv").read_bytes())
    assert len(load_dataset({"type": "csv", "path": "s3/orders.csv"}, tmp_path)) == 6
    with pytest.raises(DatasetError, match="not found"):
        load_dataset({"type": "csv", "path": "s3:/orders.csv"}, tmp_path)


def _contract() -> Contract:
    columns = [
        ColumnRule(name="order_id", dtype="integer", nullable=False),
        ColumnRule(name="amount", dtype="float", nullable=False, min=0.0),
    ]
    return Contract(
        dag_id="orders_etl",
        status="approved",
        datasets=[
            DatasetContract(
                name="orders",
                task_id="LoadOrders",
                locator=DatasetLocator(type="csv", path=URI),
                columns=columns,
            ),
            DatasetContract(
                name="returns",
                task_id="LoadOrders",
                locator=DatasetLocator(type="csv", path=f"s3://{BUCKET}/incoming/returns.csv"),
            ),
        ],
    )


def test_a_contract_on_s3_inputs_finds_the_renamed_column(world: OfflineAws) -> None:
    contract = _contract()
    frames: dict[str, object] = {}
    for dataset in contract.datasets:
        try:
            frames[dataset.name] = load_dataset(dataset.locator, ".")
        except DatasetError as exc:
            frames[dataset.name] = exc
    violations = {(v.dataset, v.rule, v.column) for v in validate_contract(contract, frames)}
    assert violations == {("orders", "missing_column", "amount"), ("returns", "dataset_unreadable", None)}
