"""Unit tests for dag_healer.datasets."""

from __future__ import annotations

import shutil
from pathlib import Path

import pandas as pd
import pytest

from dag_healer.contracts.schema import DatasetLocator
from dag_healer.datasets import DatasetError, load_dataset, resolve_dataset_path

FIXTURES = Path(__file__).parent / "fixtures" / "contracts"


def test_load_csv() -> None:
    df = load_dataset(DatasetLocator(type="csv", path="customers_day1.csv"), FIXTURES)
    assert list(df.columns)[:3] == ["customer_id", "email", "full_name"]
    assert len(df) == 6


def test_load_accepts_mapping_and_windows_separators(tmp_path: Path) -> None:
    target = tmp_path / "data" / "incoming"
    target.mkdir(parents=True)
    shutil.copy(FIXTURES / "sales_day1.csv", target / "sales.csv")
    df = load_dataset({"type": "csv", "path": "data\\incoming\\sales.csv"}, tmp_path)
    assert len(df) == 5


def test_options_are_passed_to_the_reader(tmp_path: Path) -> None:
    (tmp_path / "semi.csv").write_text("a;b\n1;x\n2;y\n", encoding="utf-8")
    df = load_dataset(
        {"type": "csv", "path": "semi.csv", "options": {"sep": ";", "usecols": ["b"]}}, tmp_path
    )
    assert list(df.columns) == ["b"]
    assert df["b"].tolist() == ["x", "y"]


def test_load_parquet() -> None:
    df = load_dataset(DatasetLocator(type="parquet", path="products.parquet"), FIXTURES)
    assert list(df.columns) == [
        "product_id",
        "product_name",
        "price",
        "launched_on",
        "updated_at",
        "in_stock",
    ]
    assert len(df) == 4


def test_load_parquet_written_by_pandas(tmp_path: Path) -> None:
    pd.DataFrame({"a": [1, 2], "b": ["x", None]}).to_parquet(tmp_path / "t.parquet")
    df = load_dataset({"type": "parquet", "path": "t.parquet", "options": {"columns": ["a"]}}, tmp_path)
    assert df["a"].tolist() == [1, 2]
    assert list(df.columns) == ["a"]


def test_load_json_records_and_lines() -> None:
    records = load_dataset(DatasetLocator(type="json", path="inventory.json"), FIXTURES)
    assert records["sku"].tolist() == ["SKU-1", "SKU-2", "SKU-3"]
    lines = load_dataset(DatasetLocator(type="json", path="inventory.jsonl"), FIXTURES)
    assert lines["qty"].tolist() == [10, 4]


def test_json_lines_option_can_be_overridden(tmp_path: Path) -> None:
    (tmp_path / "x.jsonl").write_text('[{"a": 1}]', encoding="utf-8")
    df = load_dataset({"type": "json", "path": "x.jsonl", "options": {"lines": False}}, tmp_path)
    assert df["a"].tolist() == [1]


def test_absolute_paths_are_used_as_is(tmp_path: Path) -> None:
    absolute = (FIXTURES / "sales_day1.csv").resolve()
    locator = DatasetLocator(type="csv", path=absolute.as_posix())
    assert resolve_dataset_path(locator, tmp_path) == Path(absolute.as_posix())
    assert len(load_dataset(locator, tmp_path)) == 5


def test_missing_file(tmp_path: Path) -> None:
    with pytest.raises(DatasetError, match="not found: data/missing.csv"):
        load_dataset({"type": "csv", "path": "data/missing.csv"}, tmp_path)


@pytest.mark.parametrize(
    ("kind", "name"),
    [("csv", "empty.csv"), ("parquet", "broken.parquet"), ("json", "customers_day1.csv")],
)
def test_unreadable_files(kind: str, name: str) -> None:
    with pytest.raises(DatasetError, match=f"cannot read {kind} dataset {name}"):
        load_dataset({"type": kind, "path": name}, FIXTURES)


def test_bad_reader_option_is_a_dataset_error() -> None:
    with pytest.raises(DatasetError, match="cannot read csv"):
        load_dataset({"type": "csv", "path": "sales_day1.csv", "options": {"no_such_option": 1}}, FIXTURES)


def test_reader_error_messages_are_redacted(tmp_path: Path) -> None:
    (tmp_path / "p.csv").write_text("email,n\nana@example.com,1\n", encoding="utf-8")
    with pytest.raises(DatasetError) as excinfo:
        load_dataset({"type": "csv", "path": "p.csv", "options": {"dtype": {"email": "float64"}}}, tmp_path)
    assert "ana@example.com" not in str(excinfo.value)


def test_invalid_locator() -> None:
    with pytest.raises(DatasetError, match="invalid dataset locator"):
        load_dataset({"type": "xlsx", "path": "a.xlsx"}, FIXTURES)
    with pytest.raises(DatasetError, match="invalid dataset locator"):
        load_dataset({"path": "a.csv"}, FIXTURES)


def test_directory_is_rejected_for_csv(tmp_path: Path) -> None:
    (tmp_path / "folder.csv").mkdir()
    with pytest.raises(DatasetError, match="directory"):
        load_dataset({"type": "csv", "path": "folder.csv"}, tmp_path)


def test_partitioned_parquet_directory(tmp_path: Path) -> None:
    folder = tmp_path / "parts"
    folder.mkdir()
    pd.DataFrame({"a": [1]}).to_parquet(folder / "part-0.parquet")
    pd.DataFrame({"a": [2]}).to_parquet(folder / "part-1.parquet")
    df = load_dataset({"type": "parquet", "path": "parts"}, tmp_path)
    assert sorted(df["a"].tolist()) == [1, 2]
