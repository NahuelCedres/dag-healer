"""Unit tests for dag_healer.contracts (schema, YAML I/O, inference and validation)."""

from __future__ import annotations

import datetime as dt
from pathlib import Path

import numpy as np
import pandas as pd
import pytest
import yaml

from dag_healer.contracts import (
    ColumnRule,
    Contract,
    ContractError,
    DatasetContract,
    DatasetLocator,
    RowCountRule,
    Violation,
    approve_contract,
    compute_source_hash,
    contract_path,
    dump_contract_yaml,
    infer_contract,
    infer_dataset_contract,
    is_stale,
    list_contracts,
    load_contract,
    logical_dtype,
    save_contract,
    validate_contract,
    validate_dataset,
)
from dag_healer.contracts.infer import (
    LOOKS_LIKE_DATE_NOTE,
    infer_logical_dtype,
    is_identifier_column,
    numeric_bounds,
    row_count_band,
)
from dag_healer.contracts.io import DRAFT_HEADER
from dag_healer.datasets import load_dataset

FIXTURES = Path(__file__).parent / "fixtures" / "contracts"


def _load(name: str) -> pd.DataFrame:
    kind = name.rsplit(".", 1)[1].replace("jsonl", "json")
    return load_dataset(DatasetLocator(type=kind, path=name), FIXTURES)


def _dataset(name: str = "ds", *columns: ColumnRule, **kwargs) -> DatasetContract:
    return DatasetContract(
        name=name,
        task_id="t1",
        locator=DatasetLocator(type="csv", path="data/x.csv"),
        columns=list(columns),
        **kwargs,
    )


def _sample_contract() -> Contract:
    df = _load("customers_day1.csv")
    dataset = infer_dataset_contract(
        df,
        name="customers",
        task_id="extract_customers",
        locator={"type": "csv", "path": "data/incoming/customers.csv", "options": {"sep": ","}},
    )
    return infer_contract("customers_sync", [dataset], inferred_from=["data/incoming/customers.csv"])


# --------------------------------------------------------------------------- schema


class TestSchema:
    def test_locator_path_is_normalised_to_posix(self) -> None:
        locator = DatasetLocator(type="csv", path=".\\data\\incoming\\x.csv")
        assert locator.path == "data/incoming/x.csv"

    def test_locator_rejects_empty_path_and_unknown_type(self) -> None:
        with pytest.raises(ValueError):
            DatasetLocator(type="csv", path="  ")
        with pytest.raises(ValueError):
            DatasetLocator(type="xlsx", path="a.xlsx")

    def test_unknown_keys_are_rejected(self) -> None:
        with pytest.raises(ValueError):
            ColumnRule.model_validate({"name": "a", "dtype": "string", "nullable": True, "nulable": False})

    def test_allowed_values_are_stringified(self) -> None:
        rule = ColumnRule(name="code", dtype="integer", nullable=False, allowed_values=[1, 2.0, "x", True])
        assert rule.allowed_values == ["1", "2", "x", "True"]

    def test_is_approved_and_lookup_helpers(self) -> None:
        contract = _sample_contract()
        assert not contract.is_approved
        assert contract.dataset("customers") is not None
        assert contract.dataset("nope") is None
        assert [d.name for d in contract.datasets_for_task("extract_customers")] == ["customers"]
        assert contract.datasets[0].column("email") is not None

    def test_violation_caps_row_indexes(self) -> None:
        violation = Violation(
            rule="out_of_range", dataset="d", column="c", message="m", row_indexes=range(50)
        )
        assert violation.row_indexes == list(range(20))


# --------------------------------------------------------------------------- io


class TestIO:
    def test_contract_path(self, tmp_path: Path) -> None:
        assert contract_path(tmp_path, "sales_daily_etl") == tmp_path / "sales_daily_etl.yaml"
        assert contract_path(str(tmp_path), "a.b-c") == tmp_path / "a.b-c.yaml"

    @pytest.mark.parametrize("dag_id", ["", "..", "../etc/passwd", "a/b", "a\\b"])
    def test_contract_path_rejects_traversal(self, tmp_path: Path, dag_id: str) -> None:
        with pytest.raises(ValueError):
            contract_path(tmp_path, dag_id)

    def test_roundtrip_draft(self, tmp_path: Path) -> None:
        contract = _sample_contract()
        path = save_contract(contract, tmp_path / "nested" / "customers_sync.yaml")
        text = path.read_text(encoding="utf-8")
        assert text.startswith(DRAFT_HEADER)
        assert "\r\n" not in path.read_bytes().decode("utf-8")
        assert load_contract(path) == contract

    def test_roundtrip_approved(self, tmp_path: Path) -> None:
        contract = approve_contract(_sample_contract(), "alice")
        path = save_contract(contract, tmp_path / "c.yaml")
        loaded = load_contract(path)
        assert loaded == contract
        assert loaded.approved_at is not None and loaded.approved_at.tzinfo is not None
        assert not path.read_text(encoding="utf-8").startswith(DRAFT_HEADER)

    def test_key_order_is_logical(self, tmp_path: Path) -> None:
        text = dump_contract_yaml(_sample_contract())
        data = yaml.safe_load(text)
        assert list(data) == [
            "version",
            "dag_id",
            "status",
            "review",
            "approved_by",
            "approved_at",
            "source_hash",
            "inferred_from",
            "notes",
            "datasets",
        ]
        assert list(data["datasets"][0]) == [
            "name",
            "task_id",
            "locator",
            "row_count",
            "allow_extra_columns",
            "columns",
        ]
        assert list(data["datasets"][0]["columns"][0])[:3] == ["name", "dtype", "nullable"]
        assert "{" not in text  # block style only

    def test_save_is_deterministic(self, tmp_path: Path) -> None:
        contract = _sample_contract()
        first = save_contract(contract, tmp_path / "a.yaml").read_text(encoding="utf-8")
        second = save_contract(contract.model_copy(deep=True), tmp_path / "b.yaml").read_text(
            encoding="utf-8"
        )
        assert first == second

    def test_load_errors_are_contract_errors(self, tmp_path: Path) -> None:
        missing = tmp_path / "missing.yaml"
        with pytest.raises(ContractError, match="cannot read"):
            load_contract(missing)
        bad_yaml = tmp_path / "bad.yaml"
        bad_yaml.write_text("dag_id: [unclosed", encoding="utf-8")
        with pytest.raises(ContractError, match="invalid YAML"):
            load_contract(bad_yaml)
        empty = tmp_path / "empty.yaml"
        empty.write_text("# only a comment\n", encoding="utf-8")
        with pytest.raises(ContractError, match="empty"):
            load_contract(empty)
        not_mapping = tmp_path / "list.yaml"
        not_mapping.write_text("- a\n- b\n", encoding="utf-8")
        with pytest.raises(ContractError, match="mapping"):
            load_contract(not_mapping)
        invalid = tmp_path / "invalid.yaml"
        invalid.write_text("dag_id: x\nstatus: maybe\n", encoding="utf-8")
        with pytest.raises(ContractError, match="invalid contract"):
            load_contract(invalid)

    def test_list_contracts_skips_invalid_files(
        self, tmp_path: Path, caplog: pytest.LogCaptureFixture
    ) -> None:
        save_contract(Contract(dag_id="b_dag"), tmp_path / "b_dag.yaml")
        save_contract(Contract(dag_id="a_dag"), tmp_path / "a_dag.yml")
        (tmp_path / "broken.yaml").write_text("status: nope\n", encoding="utf-8")
        (tmp_path / "readme.txt").write_text("ignored", encoding="utf-8")
        contracts = list_contracts(tmp_path)
        assert [c.dag_id for c in contracts] == ["a_dag", "b_dag"]
        assert "broken.yaml" in caplog.text
        assert list_contracts(tmp_path / "does-not-exist") == []

    def test_hand_written_contract_loads(self, tmp_path: Path) -> None:
        path = tmp_path / "manual.yaml"
        path.write_text(
            "dag_id: manual\n"
            "datasets:\n"
            "  - name: orders\n"
            "    locator: {type: parquet, path: data/orders.parquet}\n"
            "    columns:\n"
            "      - {name: id, dtype: integer, nullable: false, min: 1}\n",
            encoding="utf-8",
        )
        contract = load_contract(path)
        assert contract.status == "draft"
        assert contract.datasets[0].columns[0].min == 1.0
        assert contract.datasets[0].row_count == RowCountRule()


class TestApproval:
    def test_approve_sets_fields_without_mutating_input(self) -> None:
        draft = _sample_contract()
        approved = approve_contract(draft, "  bob  ")
        assert draft.status == "draft" and draft.approved_by is None
        assert approved.is_approved
        assert approved.approved_by == "bob"
        assert approved.approved_at is not None
        assert approved.approved_at.utcoffset() == dt.timedelta(0)
        assert approved.source_hash == compute_source_hash(draft)
        assert not is_stale(approved)

    def test_approve_requires_actor(self) -> None:
        with pytest.raises(ValueError):
            approve_contract(_sample_contract(), " ")

    def test_hash_ignores_approval_fields(self) -> None:
        draft = _sample_contract()
        approved = approve_contract(draft, "alice")
        assert compute_source_hash(draft) == compute_source_hash(approved)

    def test_edit_after_approval_is_stale(self, tmp_path: Path) -> None:
        approved = approve_contract(_sample_contract(), "alice")
        path = save_contract(approved, tmp_path / "c.yaml")
        text = path.read_text(encoding="utf-8").replace("nullable: true", "nullable: false", 1)
        path.write_text(text, encoding="utf-8")
        assert is_stale(load_contract(path))

    def test_draft_is_never_stale_and_unhashed_approval_is(self) -> None:
        draft = _sample_contract()
        assert not is_stale(draft)
        assert is_stale(draft.model_copy(update={"status": "approved"}))


# --------------------------------------------------------------------------- dtype inference


class TestLogicalDtype:
    @pytest.mark.parametrize(
        ("values", "dtype", "expected"),
        [
            ([1, 2, 3], None, "integer"),
            ([1.0, None, 3.0], None, "integer"),  # integer column with nulls (float64)
            ([1, None, 3], "Int64", "integer"),
            ([1.5, 2.0], None, "float"),
            ([True, False], None, "boolean"),
            ([True, None], "boolean", "boolean"),
            ([True, None, False], object, "boolean"),
            (["a", "b"], None, "string"),
            (["a", None], object, "string"),  # pandas 2 style
            (["a", None], "string", "string"),  # string[python]
            (["a", None], "str", "string"),  # pandas 3 default
            ([None, None], object, "unknown"),
            ([np.nan, np.nan], None, "unknown"),
            ([1, 2], object, "integer"),  # pandas 2 read_json can yield object ints
            ([1, 2.5], object, "float"),
            (["a", 1], object, "string"),
            ([dt.date(2026, 1, 1), None], object, "date"),
            ([dt.datetime(2026, 1, 1, 10), dt.date(2026, 1, 2)], object, "datetime"),
            ([pd.Timedelta(1, "s")], None, "unknown"),
        ],
    )
    def test_dtypes(self, values: list, dtype: object, expected: str) -> None:
        series = pd.Series(values, dtype=dtype) if dtype is not None else pd.Series(values)
        assert logical_dtype(series) == expected

    def test_datetime64_and_tz_aware(self) -> None:
        naive = pd.Series(pd.to_datetime(["2026-01-01", "2026-01-02"]))
        aware = pd.Series(pd.to_datetime(["2026-01-01T00:00:00Z"], utc=True))
        assert logical_dtype(naive) == "datetime"
        assert logical_dtype(aware) == "datetime"

    def test_categorical_uses_category_values(self) -> None:
        assert logical_dtype(pd.Series(pd.Categorical(["a", "b", None]))) == "string"
        assert logical_dtype(pd.Series(pd.Categorical([1, 2]))) == "integer"

    @pytest.mark.parametrize("dtype", [object, "str", "string"])
    def test_date_only_strings_stay_string_with_note(self, dtype: object) -> None:
        series = pd.Series(["2026-09-01", "2026-09-02", None], dtype=dtype)
        result = infer_logical_dtype(series, name="signup_date")
        assert result.dtype == "string"
        assert result.note == LOOKS_LIKE_DATE_NOTE
        assert "looks like a date" in result.note

    @pytest.mark.parametrize("name", ["created_at", "event_ts", "pickup_time", "UPDATED_AT"])
    def test_date_only_strings_with_timestamp_name_are_datetime(self, name: str) -> None:
        series = pd.Series(["2026-09-01", "2026-09-02"], dtype=object)
        assert infer_logical_dtype(series, name=name).dtype == "datetime"

    @pytest.mark.parametrize("dtype", [object, "str"])
    def test_strings_with_time_part_are_datetime(self, dtype: object) -> None:
        series = pd.Series(["2026-09-01", "2026-10-02T14:03:00Z", "2026-10-02 15:00:00+03:00"], dtype=dtype)
        result = infer_logical_dtype(series, name="signup")
        assert result.dtype == "datetime"
        assert result.note is None

    def test_unparseable_date_like_strings_are_string(self) -> None:
        series = pd.Series(["2026-09-01", "2026-13-45"], dtype=object)
        assert logical_dtype(series, name="d") == "string"
        assert logical_dtype(pd.Series(["2026", "2027"], dtype=object)) == "string"


class TestInference:
    def test_customers_fixture(self) -> None:
        contract = _sample_contract()
        columns = {c.name: c for c in contract.datasets[0].columns}
        assert columns["customer_id"].dtype == "integer"
        assert columns["customer_id"].min == 0.0
        assert columns["customer_id"].max is None  # identifiers grow
        assert columns["email"].pii and columns["full_name"].pii
        assert not columns["country"].pii
        assert columns["signup_date"].dtype == "string"
        assert columns["signup_date"].note
        assert columns["lifetime_value"].dtype == "float"
        assert columns["lifetime_value"].nullable
        assert columns["lifetime_value"].max == 3000.0
        assert columns["is_active"].dtype == "boolean"
        assert columns["is_active"].min is None
        assert not columns["email"].nullable

    def test_contract_metadata(self) -> None:
        contract = _sample_contract()
        assert contract.status == "draft"
        assert contract.review == "required"
        assert contract.inferred_from == ["data/incoming/customers.csv"]
        assert contract.notes
        assert contract.datasets[0].locator.options == {"sep": ","}
        assert contract.datasets[0].task_id == "extract_customers"

    @pytest.mark.parametrize(("rows", "low", "high"), [(0, 1, 10), (1, 1, 13), (6, 3, 28), (101, 50, 313)])
    def test_row_count_band(self, rows: int, low: int, high: int) -> None:
        band = row_count_band(rows)
        assert (band.min, band.max) == (low, high)

    def test_numeric_bounds(self) -> None:
        assert numeric_bounds(np.array([5.0, 12.0, np.nan]), name="amount") == (0.0, 120.0)
        assert numeric_bounds(np.array([-2.0, 3.0]), name="delta") == (-20.0, 30.0)
        assert numeric_bounds(np.array([-4.0, -1.0]), name="loss") == (-40.0, 0.0)
        assert numeric_bounds(np.array([3.0, 9.0]), name="order_id") == (0.0, None)
        assert numeric_bounds(np.array([np.nan]), name="x") == (None, None)

    @pytest.mark.parametrize(
        ("name", "expected"),
        [("id", True), ("order_id", True), ("customerId", True), ("sku_code", True), ("paid", False)],
    )
    def test_identifier_columns(self, name: str, expected: bool) -> None:
        assert is_identifier_column(name) is expected

    def test_pii_columns_argument_and_string_columns_have_no_range(self) -> None:
        df = pd.DataFrame({"Notes": ["a", "b"], "score": [1, 2]})
        dataset = infer_dataset_contract(
            df,
            name="d",
            task_id=None,
            locator=DatasetLocator(type="csv", path="d.csv"),
            pii_columns=["notes"],
        )
        notes, score = dataset.columns
        assert notes.pii and notes.min is None and notes.max is None
        assert not score.pii and score.min == 0.0 and score.max == 20.0

    def test_parquet_fixture(self) -> None:
        df = _load("products.parquet")
        dataset = infer_dataset_contract(
            df, name="products", task_id="t", locator={"type": "parquet", "path": "products.parquet"}
        )
        columns = {c.name: c for c in dataset.columns}
        assert columns["product_id"].dtype == "integer" and columns["product_id"].nullable
        assert not columns["product_name"].pii
        assert columns["launched_on"].dtype == "date"
        assert columns["updated_at"].dtype == "datetime"
        assert columns["in_stock"].dtype == "boolean"

    def test_inferred_contract_accepts_its_own_sample(self) -> None:
        for name in ["customers_day1.csv", "sales_day1.csv", "inventory.json", "products.parquet"]:
            df = _load(name)
            dataset = infer_dataset_contract(
                df, name="d", task_id=None, locator={"type": "csv", "path": name}
            )
            assert validate_dataset(df, dataset) == [], name


# --------------------------------------------------------------------------- validation


def _rules(violations: list[Violation]) -> list[tuple[str, str | None]]:
    return [(v.rule, v.column) for v in violations]


class TestValidation:
    def test_code_case_new_timestamp_format_passes_the_contract(self) -> None:
        contract = approve_contract(_sample_contract(), "alice")
        frames = {"customers": _load("customers_day2.csv")}
        assert validate_contract(contract, frames) == []

    def test_data_case_reports_every_violation(self) -> None:
        day1 = _load("sales_day1.csv")
        dataset = infer_dataset_contract(
            day1, name="sales", task_id="extract", locator={"type": "csv", "path": "s"}
        )
        violations = validate_dataset(_load("sales_day2_bad.csv"), dataset)
        assert _rules(violations) == [
            ("missing_column", "amount"),
            ("nulls_not_allowed", "quantity"),
            ("dtype_mismatch", "sold_at"),
        ]
        missing, nulls, dtype = violations
        assert "amt" in missing.actual
        assert nulls.row_indexes == [1, 3]
        assert nulls.actual == {"null_count": 2, "rows": 4}
        assert dtype.row_indexes == [3]
        assert dtype.expected == "datetime"
        for violation in violations:
            assert violation.message

    def test_violations_are_json_safe(self) -> None:
        import json

        day1 = _load("sales_day1.csv")
        dataset = infer_dataset_contract(
            day1, name="sales", task_id="extract", locator={"type": "csv", "path": "s"}
        )
        violations = validate_dataset(_load("sales_day2_bad.csv"), dataset)
        json.dumps([v.model_dump(mode="json") for v in violations])

    def test_row_count_out_of_band(self) -> None:
        dataset = _dataset("d", row_count=RowCountRule(min=3, max=4))
        assert _rules(validate_dataset(pd.DataFrame({"a": [1]}), dataset)) == [
            ("row_count_out_of_band", None)
        ]
        assert validate_dataset(pd.DataFrame({"a": [1, 2, 3]}), dataset) == []
        too_many = validate_dataset(pd.DataFrame({"a": range(5)}), dataset)
        assert too_many[0].actual == 5 and too_many[0].expected == {"min": 3, "max": 4}

    def test_out_of_range_with_row_indexes(self) -> None:
        dataset = _dataset("d", ColumnRule(name="amount", dtype="float", nullable=True, min=0, max=100))
        df = pd.DataFrame({"amount": [5.0, -1.0, None, 150.0, 99.0]})
        (violation,) = validate_dataset(df, dataset)
        assert violation.rule == "out_of_range"
        assert violation.row_indexes == [1, 3]
        assert violation.expected == {"min": 0, "max": 100}
        assert violation.actual == {"min": -1, "max": 150, "count": 2}

    def test_row_indexes_capped_at_20(self) -> None:
        dataset = _dataset("d", ColumnRule(name="a", dtype="integer", nullable=False))
        df = pd.DataFrame({"a": [None] * 50 + [1]})
        (violation,) = validate_dataset(df, dataset)
        assert violation.rule == "nulls_not_allowed"
        assert violation.row_indexes == list(range(20))
        assert violation.actual["null_count"] == 50

    def test_row_indexes_are_positions_not_labels(self) -> None:
        dataset = _dataset("d", ColumnRule(name="a", dtype="integer", nullable=False))
        df = pd.DataFrame({"a": [1.0, None]}, index=[100, 200])
        assert validate_dataset(df, dataset)[0].row_indexes == [1]

    def test_unexpected_column_only_when_extra_columns_forbidden(self) -> None:
        rule = ColumnRule(name="a", dtype="integer", nullable=False)
        df = pd.DataFrame({"a": [1], "warehouse_code": ["x"]})
        assert validate_dataset(df, _dataset("d", rule)) == []
        strict = validate_dataset(df, _dataset("d", rule, allow_extra_columns=False))
        assert _rules(strict) == [("unexpected_column", "warehouse_code")]

    def test_not_allowed_value(self) -> None:
        rule = ColumnRule(name="status", dtype="string", nullable=True, allowed_values=["new", "done"])
        df = pd.DataFrame({"status": ["new", "lost", None, "done", "weird"]})
        (violation,) = validate_dataset(df, _dataset("d", rule))
        assert violation.rule == "not_allowed_value"
        assert violation.row_indexes == [1, 4]
        assert violation.actual == {"count": 2, "values": ["lost", "weird"]}

    def test_not_allowed_value_numbers_match_their_string_form(self) -> None:
        rule = ColumnRule(name="level", dtype="integer", nullable=True, allowed_values=[1, 2])
        df = pd.DataFrame({"level": [1.0, 2.0, None]})
        assert validate_dataset(df, _dataset("d", rule)) == []

    def test_not_allowed_value_never_leaks_pii(self) -> None:
        rule = ColumnRule(name="email", dtype="string", nullable=True, allowed_values=["a@example.com"])
        df = pd.DataFrame({"email": ["a@example.com", "secret.person@example.com"]})
        (violation,) = validate_dataset(df, _dataset("d", rule))
        assert "secret.person" not in str(violation.model_dump())

    def test_dataset_unreadable(self) -> None:
        contract = Contract(
            dag_id="x",
            datasets=[_dataset("a"), _dataset("b"), _dataset("c")],
        )
        frames = {"a": pd.DataFrame({"z": [1]}), "c": RuntimeError("file not found: data/x.csv")}
        violations = validate_contract(contract, frames)
        assert _rules(violations) == [("dataset_unreadable", None), ("dataset_unreadable", None)]
        assert violations[0].dataset == "b"
        assert "file not found" in violations[1].message


class TestTolerantDtypes:
    @pytest.mark.parametrize(
        ("rule_dtype", "values", "dtype"),
        [
            ("integer", [1, 2], None),
            ("integer", [1.0, None], None),
            ("integer", [1, None], "Int64"),
            ("integer", ["1", "2"], object),
            ("float", [1, 2], None),
            ("float", [1.5, None], None),
            ("float", ["1.5", None], object),
            ("boolean", [True, False], None),
            ("boolean", [True, None], object),
            ("boolean", ["true", "False"], object),
            ("string", ["a", None], object),
            ("string", ["a", None], "str"),
            ("string", ["a", None], "string"),
            ("string", ["a", 1], object),
            ("string", ["2026-01-01", "2026-01-02T10:00:00Z"], object),
            ("datetime", ["2026-01-01", "2026-01-02T10:00:00Z", "2026-01-02T10:00:00+03:00"], "str"),
            ("datetime", ["2026-01-01T10:00:00"], object),
            ("date", ["2026-01-01", "2026-10-02T14:03:00Z"], object),
            ("date", [dt.date(2026, 1, 1), None], object),
            ("unknown", [object(), 1], object),
            ("integer", [None, None], object),  # all-null: only nullability matters
            ("string", [np.nan, np.nan], None),
        ],
    )
    def test_accepted(self, rule_dtype: str, values: list, dtype: object) -> None:
        series = pd.Series(values, dtype=dtype) if dtype is not None else pd.Series(values)
        dataset = _dataset("d", ColumnRule(name="c", dtype=rule_dtype, nullable=True))
        assert validate_dataset(pd.DataFrame({"c": series}), dataset) == []

    def test_datetime_columns_accepted_for_date_rules(self) -> None:
        series = pd.Series(pd.to_datetime(["2026-01-01", None]))
        for rule_dtype in ("date", "datetime", "string"):
            dataset = _dataset("d", ColumnRule(name="c", dtype=rule_dtype, nullable=True))
            assert validate_dataset(pd.DataFrame({"c": series}), dataset) == []

    @pytest.mark.parametrize(
        ("rule_dtype", "values", "dtype", "rows"),
        [
            ("integer", [1.0, 2.5, None], None, [1]),
            ("integer", ["1", "x", "3.5"], object, [1, 2]),
            ("integer", [True, False], None, []),
            ("float", ["1.5", "n/a"], "str", [1]),
            ("float", [True, False], None, []),
            ("boolean", [1, 0], None, []),
            ("boolean", ["yes", "true"], object, [0]),
            ("string", [1, 2], None, []),
            ("string", [1.5, 2.5], object, []),
            ("datetime", ["2026-01-01", "not-a-date", None, "31/01/2026"], object, [1, 3]),
            ("date", [1, 2], None, []),
        ],
    )
    def test_rejected(self, rule_dtype: str, values: list, dtype: object, rows: list[int]) -> None:
        series = pd.Series(values, dtype=dtype) if dtype is not None else pd.Series(values)
        dataset = _dataset("d", ColumnRule(name="c", dtype=rule_dtype, nullable=True))
        (violation,) = validate_dataset(pd.DataFrame({"c": series}), dataset)
        assert violation.rule == "dtype_mismatch"
        assert violation.expected == rule_dtype
        assert violation.row_indexes == rows

    def test_range_checked_on_numeric_strings_and_skipped_for_text(self) -> None:
        dataset = _dataset("d", ColumnRule(name="c", dtype="float", nullable=True, min=0, max=10))
        violations = validate_dataset(
            pd.DataFrame({"c": pd.Series(["5", "50", "n/a"], dtype=object)}), dataset
        )
        assert _rules(violations) == [("dtype_mismatch", "c"), ("out_of_range", "c")]
        assert violations[1].row_indexes == [1]


# --------------------------------------------------------------------------- portability


def _as_pandas2_frame(df: pd.DataFrame) -> pd.DataFrame:
    """Simulate pandas 2 ``read_csv`` output: string columns stored as ``object``."""
    string_columns = [c for c in df.columns if pd.api.types.is_string_dtype(df[c].dtype)]
    return df.astype({c: object for c in string_columns})


def test_pandas2_style_object_columns_infer_and_validate_identically() -> None:
    day1 = _load("customers_day1.csv")
    locator = {"type": "csv", "path": "c.csv"}
    native = infer_dataset_contract(day1, name="c", task_id=None, locator=locator)
    legacy = infer_dataset_contract(_as_pandas2_frame(day1), name="c", task_id=None, locator=locator)
    assert native == legacy
    assert validate_dataset(_as_pandas2_frame(_load("customers_day2.csv")), legacy) == []
    sales = infer_dataset_contract(_load("sales_day1.csv"), name="s", task_id=None, locator=locator)
    bad = _as_pandas2_frame(_load("sales_day2_bad.csv"))
    assert [v.rule for v in validate_dataset(bad, sales)] == [
        "missing_column",
        "nulls_not_allowed",
        "dtype_mismatch",
    ]


def test_schema_import_does_not_load_pandas() -> None:
    import os
    import subprocess
    import sys

    src = Path(__file__).resolve().parents[2] / "src"
    code = (
        "import sys, dag_healer.contracts, dag_healer.masking, dag_healer.datasets; "
        "sys.exit(1 if 'pandas' in sys.modules else 0)"
    )
    env = {**os.environ, "PYTHONPATH": str(src)}
    assert subprocess.run([sys.executable, "-c", code], env=env, check=False).returncode == 0


def test_pii_numeric_columns_get_no_bounds_and_range_violations_hide_values() -> None:
    df = pd.DataFrame({"phone": [541155551234, 541155550000], "amount": [1.0, 2.0]})
    dataset = infer_dataset_contract(df, name="d", task_id=None, locator={"type": "csv", "path": "d"})
    phone = dataset.column("phone")
    assert phone is not None and phone.pii and phone.min is None and phone.max is None
    rule = ColumnRule(name="phone", dtype="integer", nullable=False, min=0, max=10, pii=True)
    (violation,) = validate_dataset(df, _dataset("d", rule))
    assert violation.rule == "out_of_range"
    assert violation.actual == {"count": 2}
    assert "5411" not in str(violation.model_dump())
