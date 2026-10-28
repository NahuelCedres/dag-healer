"""Unit tests for dag_healer.masking."""

from __future__ import annotations

import datetime as dt
import json
import random
import string
import sys
import uuid
from decimal import Decimal
from pathlib import Path
from types import SimpleNamespace

import numpy as np
import pandas as pd
import pytest

from dag_healer.masking import (
    MaskingHookError,
    dataframe_to_records,
    hide_secrets,
    holds_secret,
    is_sensitive_column,
    known_secrets,
    load_hook,
    mask_dataframe,
    mask_text,
    mask_value,
    masked_sample,
    masking_settings,
    to_json_safe,
)


def _cfg(**overrides: object) -> SimpleNamespace:
    base = {"default": "hash", "columns": [], "allow": [], "hook": None, "sample_rows": 5}
    base.update(overrides)
    return SimpleNamespace(**base)


def _people() -> pd.DataFrame:
    return pd.DataFrame(
        {
            "customer_id": [1, 2, 3],
            "email": ["ana@example.com", "bob@example.org", None],
            "full_name": ["Ana Perez", "Bob Stone", "Carl Moe"],
            "phone": [541155551234, 541155550000, 541100000000],
            "product_name": ["Widget", "Gadget", "Gizmo"],
            "comment": ["call me at +54 11 5555-1234", "mail bob@example.org", "ok"],
            "amount": [10.5, 20.0, np.nan],
            "signup_date": pd.to_datetime(["2026-01-01", "2026-01-02", None]),
            "country": ["AR", "US", "UY"],
        }
    )


# --------------------------------------------------------------------------- names


@pytest.mark.parametrize(
    "name",
    [
        "email",
        "E-Mail",
        "customerEmail",
        "email_address",
        "phone",
        "phone_number",
        "mobile",
        "name",
        "Name",
        "first_name",
        "lastName",
        "full name",
        "user_name",
        "address",
        "street",
        "billing_address",
        "ssn",
        "dni",
        "document_number",
        "passport_no",
        "iban",
        "credit_card",
        "card_number",
        "ip",
        "ip_address",
        "birth_date",
        "date_of_birth",
        "dob",
        "zip",
        "password",
        "api_key",
    ],
)
def test_sensitive_column_names(name: str) -> None:
    assert is_sensitive_column(name)


@pytest.mark.parametrize(
    "name",
    [
        "amount",
        "quantity",
        "qty",
        "sku",
        "product_name",
        "dag_name",
        "host_name",
        "hostname",
        "file_name",
        "warehouse_code",
        "signup_date",
        "country",
        "classname",
        "description",
        "status",
        "customer_id",
        "",
    ],
)
def test_non_sensitive_column_names(name: str) -> None:
    assert not is_sensitive_column(name)


# --------------------------------------------------------------------------- values


class TestMaskValue:
    def test_hash_format_and_determinism(self) -> None:
        first = mask_value("ana@example.com", "hash", salt="incident-1")
        assert first is not None and first.startswith("h:") and len(first) == 12
        assert all(char in "0123456789abcdef" for char in first[2:])
        assert mask_value("ana@example.com", "hash", salt="incident-1") == first
        assert mask_value("ana@example.com", "hash", salt="incident-2") != first
        assert mask_value("ana@example.com") != first

    def test_numbers_hash_by_value(self) -> None:
        assert mask_value(1) == mask_value(1.0) == mask_value(np.int64(1)) == mask_value("1")

    def test_redact(self) -> None:
        assert mask_value("anything", "redact") == "***"

    @pytest.mark.parametrize("missing", [None, float("nan"), np.nan, pd.NaT, pd.NA, np.float32("nan")])
    def test_missing_values_stay_none(self, missing: object) -> None:
        assert mask_value(missing) is None
        assert mask_value(missing, "redact") is None

    def test_unknown_mode(self) -> None:
        with pytest.raises(ValueError):
            mask_value("x", "encrypt")  # type: ignore[arg-type]

    def test_property_raw_value_never_survives(self) -> None:
        rng = random.Random(1234)
        alphabet = string.ascii_letters + string.digits + string.punctuation + " áéñü"
        for _ in range(2000):
            raw = "".join(rng.choice(alphabet) for _ in range(rng.randint(4, 40)))
            for mode in ("hash", "redact"):
                masked = mask_value(raw, mode, salt="s")
                assert masked is not None
                assert raw not in masked


# --------------------------------------------------------------------------- free text


class TestMaskText:
    @pytest.mark.parametrize(
        ("text", "secret"),
        [
            ("Contact ana.perez+test@example.co.uk now", "ana.perez"),
            ("phone +54 11 5555-1234 please", "5555-1234"),
            ("call (011) 4555-1234", "4555-1234"),
            ("us 202-555-0143 office", "555-0143"),
            ("intl +5491155551234", "5491155551234"),
            ("account 12345678901", "12345678901"),
            ("card 4111 1111 1111 1111", "4111 1111"),
            ("iban DE89370400440532013000", "DE89370400440532013000"),
            ("client 192.168.10.20 rejected", "192.168.10.20"),
        ],
    )
    def test_redacts(self, text: str, secret: str) -> None:
        masked = mask_text(text)
        assert secret not in masked
        assert masked != text

    @pytest.mark.parametrize(
        "text",
        [
            "time data '2026-10-02T14:03:00Z' does not match format '%Y-%m-%d'",
            "run manual__2026-10-17T12:00:00.123456+00:00 failed at 2026-10-17 12:00:00",
            "KeyError: \"['qty'] not in index\" at line 42",
            "pandas 2.2.3 / airflow 3.3.2, rows=1234, amount=12.50",
            "dates 02/10/2026 and 2026/10/02",
            "uuid a3f2c9d1-2b3c-4d5e-8f90-1234567890ab",
        ],
    )
    def test_keeps_diagnostic_text(self, text: str) -> None:
        assert mask_text(text) == text

    def test_labels_and_non_strings(self) -> None:
        assert mask_text("a@b.io and +54 11 5555-1234 and 1234567") == "[email] and [phone] and [number]"
        assert mask_text(None) == ""
        assert mask_text(12345678) == "[number]"

    def test_long_input_is_fast(self) -> None:
        text = ("x" * 5000 + " ") * 100
        assert mask_text(text) == text


# --------------------------------------------------------------------------- known secrets

#: A fictional value of a credential variable.
SECRET_VALUE = "example-value/held+in=a-variable"


def test_known_secrets_are_the_long_values_without_their_whitespace() -> None:
    longer = SECRET_VALUE + "-and-more"
    assert known_secrets([f" {SECRET_VALUE}\n", "short", None, 7, SECRET_VALUE, longer]) == [
        longer,
        SECRET_VALUE,
    ]
    assert known_secrets([]) == []


def test_hide_secrets_hides_a_secret_whole_or_cut() -> None:
    """A fragment of 16 or more characters counts as the secret: the run it makes is hidden once."""
    secrets = known_secrets([SECRET_VALUE])
    text = f"echo {SECRET_VALUE} and {SECRET_VALUE[3:-3]}; {SECRET_VALUE[:15]} stays"
    assert hide_secrets(text, secrets, "[hidden]") == f"echo [hidden] and [hidden]; {SECRET_VALUE[:15]} stays"
    assert hide_secrets("nothing to hide", secrets, "***") == "nothing to hide"
    assert hide_secrets(text, [], "[hidden]") == text


def test_holds_secret_finds_a_secret_whole_or_as_a_fragment() -> None:
    secrets = known_secrets([SECRET_VALUE])
    assert holds_secret(f"x{SECRET_VALUE}y", secrets)
    assert holds_secret(f"x{SECRET_VALUE[5:21]}y", secrets)
    assert not holds_secret(SECRET_VALUE[:15], secrets)
    assert not holds_secret(SECRET_VALUE, [])


# --------------------------------------------------------------------------- DataFrames


class TestMaskDataFrame:
    def test_default_masking(self) -> None:
        df = _people()
        masked = mask_dataframe(df, None, salt="inc")
        assert masked["email"].tolist()[2] is None
        assert all(str(v).startswith("h:") for v in masked["email"].tolist()[:2])
        assert all(str(v).startswith("h:") for v in masked["full_name"])
        assert all(str(v).startswith("h:") for v in masked["phone"])  # sensitive name wins over dtype
        assert masked["product_name"].tolist() == ["Widget", "Gadget", "Gizmo"]
        assert masked["comment"].tolist() == ["call me at [phone]", "mail [email]", "ok"]
        pd.testing.assert_series_equal(masked["amount"], df["amount"])
        pd.testing.assert_series_equal(masked["signup_date"], df["signup_date"])
        pd.testing.assert_series_equal(masked["customer_id"], df["customer_id"])
        assert masked["country"].tolist() == ["AR", "US", "UY"]

    def test_input_frame_is_not_modified(self) -> None:
        df = _people()
        original = df.copy()
        mask_dataframe(df, _cfg(columns=["amount"]))
        pd.testing.assert_frame_equal(df, original)

    def test_no_raw_value_survives(self) -> None:
        df = _people()
        masked = mask_dataframe(df, None, salt="inc")
        dumped = json.dumps(dataframe_to_records(masked))
        for raw in [
            "ana@example.com",
            "bob@example.org",
            "Ana Perez",
            "Bob Stone",
            "541155551234",
            "5555-1234",
        ]:
            assert raw not in dumped

    def test_deterministic_per_salt(self) -> None:
        df = _people()
        a = mask_dataframe(df, None, salt="s1")
        b = mask_dataframe(df, None, salt="s1")
        c = mask_dataframe(df, None, salt="s2")
        assert a["email"].tolist() == b["email"].tolist()
        assert a["email"].tolist()[:2] != c["email"].tolist()[:2]

    def test_explicit_columns_mask_numeric_and_dates(self) -> None:
        df = _people()
        masked = mask_dataframe(df, _cfg(columns=["Amount", "signup_date"]))
        assert masked["amount"].tolist()[2] is None
        assert all(str(v).startswith("h:") for v in masked["amount"].tolist()[:2])
        assert all(str(v).startswith("h:") for v in masked["signup_date"].tolist()[:2])

    def test_pii_columns_argument(self) -> None:
        df = _people()
        masked = mask_dataframe(df, None, pii_columns=["country"])
        assert all(str(v).startswith("h:") for v in masked["country"])

    def test_allow_list_wins(self) -> None:
        df = _people()
        masked = mask_dataframe(
            df, _cfg(allow=["EMAIL", "comment"], columns=["email"]), pii_columns=["email"]
        )
        assert masked["email"].tolist()[:2] == ["ana@example.com", "bob@example.org"]
        assert masked["comment"].tolist() == df["comment"].tolist()
        assert str(masked["full_name"].iloc[0]).startswith("h:")

    def test_redact_mode_from_mapping_config(self) -> None:
        masked = mask_dataframe(_people(), {"default": "redact"})
        assert masked["email"].tolist() == ["***", "***", None]

    def test_pandas2_object_and_pandas3_string_columns(self) -> None:
        for dtype in (object, "str", "string"):
            df = pd.DataFrame({"email": pd.Series(["a@x.io", None], dtype=dtype), "note": ["x@y.io", "n"]})
            masked = mask_dataframe(df)
            assert masked["email"].tolist()[1] is None
            assert masked["email"].tolist()[0].startswith("h:")
            assert masked["note"].tolist() == ["[email]", "n"]

    def test_duplicate_and_non_string_column_labels(self) -> None:
        df = pd.DataFrame([["a@x.io", "b@x.io", 1]], columns=["email", "email", 0])
        masked = mask_dataframe(df)
        assert masked.iloc[0, 0].startswith("h:") and masked.iloc[0, 1].startswith("h:")
        assert masked.iloc[0, 2] == 1

    def test_empty_frame(self) -> None:
        df = pd.DataFrame({"email": pd.Series([], dtype=object)})
        assert mask_dataframe(df).empty

    def test_invalid_mode(self) -> None:
        with pytest.raises(ValueError):
            mask_dataframe(_people(), _cfg(default="encrypt"))


class TestHook:
    @pytest.fixture()
    def hook_module(self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> str:
        name = f"healer_test_hook_{uuid.uuid4().hex[:8]}"
        (tmp_path / f"{name}.py").write_text(
            "import pandas as pd\n"
            "def drop_country(df, context):\n"
            "    out = df.drop(columns=['country'])\n"
            "    out['ctx'] = context.get('dag_id')\n"
            "    return out\n"
            "def boom(df, context):\n"
            "    raise RuntimeError('cannot mask ana@example.com')\n"
            "def not_a_frame(df, context):\n"
            "    return 42\n"
            "class Holder:\n"
            "    @staticmethod\n"
            "    def hook(df, context):\n"
            "        return df.head(1)\n"
            "VALUE = 3\n",
            encoding="utf-8",
        )
        monkeypatch.syspath_prepend(str(tmp_path))
        yield name
        sys.modules.pop(name, None)
        load_hook.cache_clear()

    def test_hook_runs_last_with_context(self, hook_module: str) -> None:
        masked = mask_dataframe(_people(), _cfg(hook=f"{hook_module}:drop_country"), context={"dag_id": "d1"})
        assert "country" not in masked.columns
        assert masked["ctx"].tolist() == ["d1"] * 3
        assert str(masked["email"].iloc[0]).startswith("h:")  # default masking ran first

    def test_nested_attribute(self, hook_module: str) -> None:
        assert len(mask_dataframe(_people(), _cfg(hook=f"{hook_module}:Holder.hook"))) == 1

    @pytest.mark.parametrize(
        ("spec", "message"),
        [
            ("no_colon_here", "expected 'package.module:function'"),
            (":func", "expected 'package.module:function'"),
            ("definitely_missing_module_xyz:func", "cannot import masking hook module"),
        ],
    )
    def test_loading_errors(self, spec: str, message: str) -> None:
        with pytest.raises(MaskingHookError, match=message):
            mask_dataframe(_people(), _cfg(hook=spec))

    def test_missing_attribute_and_not_callable(self, hook_module: str) -> None:
        with pytest.raises(MaskingHookError, match="has no attribute"):
            mask_dataframe(_people(), _cfg(hook=f"{hook_module}:nope"))
        with pytest.raises(MaskingHookError, match="not callable"):
            mask_dataframe(_people(), _cfg(hook=f"{hook_module}:VALUE"))

    def test_failing_hook_fails_closed_without_leaking(self, hook_module: str) -> None:
        with pytest.raises(MaskingHookError, match="failed") as excinfo:
            mask_dataframe(_people(), _cfg(hook=f"{hook_module}:boom"))
        assert "ana@example.com" not in str(excinfo.value)
        with pytest.raises(MaskingHookError, match="must return a pandas DataFrame"):
            mask_dataframe(_people(), _cfg(hook=f"{hook_module}:not_a_frame"))


# --------------------------------------------------------------------------- records


class TestRecords:
    def test_json_safe_conversions(self) -> None:
        df = pd.DataFrame(
            {
                "i": pd.Series([1, None], dtype="Int64"),
                "f": [1.5, np.nan],
                "b": [True, False],
                "ts": pd.to_datetime(["2026-10-02T14:03:00Z", None], utc=True),
                "s": pd.Series(["x", None], dtype="str"),
                "o": [Decimal("1.10"), {"k": np.int64(2)}],
                "td": pd.to_timedelta(["1h", None]),
                "inf": [np.inf, -np.inf],
            }
        )
        records = dataframe_to_records(df)
        json.dumps(records, allow_nan=False)
        first, second = records
        assert first == {
            "i": 1,
            "f": 1.5,
            "b": True,
            "ts": "2026-10-02T14:03:00+00:00",
            "s": "x",
            "o": "1.10",
            "td": "0 days 01:00:00",
            "inf": "inf",
        }
        assert second["i"] is None and second["f"] is None and second["ts"] is None
        assert second["s"] is None and second["td"] is None
        assert second["o"] == {"k": 2} and second["inf"] == "-inf"
        assert type(first["i"]) is int and type(first["b"]) is bool

    def test_limit_and_duplicate_columns(self) -> None:
        df = pd.DataFrame([[1, 2], [3, 4], [5, 6]], columns=["a", "a"])
        assert dataframe_to_records(df, 2) == [{"a": 1, "a.1": 2}, {"a": 3, "a.1": 4}]
        assert dataframe_to_records(df, 0) == []

    @pytest.mark.parametrize(
        ("value", "expected"),
        [
            (np.float64(2.5), 2.5),
            (np.bool_(True), True),
            (dt.date(2026, 1, 2), "2026-01-02"),
            (dt.time(10, 30), "10:30:00"),
            (np.datetime64("2026-01-02T03:04:05"), "2026-01-02T03:04:05"),
            (np.array([1, 2]), [1, 2]),
            (b"\x00\x01", "<2 bytes>"),
            ((1, np.nan), [1, None]),
            (uuid.UUID(int=0), "00000000-0000-0000-0000-000000000000"),
        ],
    )
    def test_to_json_safe(self, value: object, expected: object) -> None:
        assert to_json_safe(value) == expected

    def test_masked_sample(self) -> None:
        records = masked_sample(_people(), _cfg(sample_rows=2), salt="inc")
        assert len(records) == 2
        assert records[0]["email"].startswith("h:")
        assert records[0]["product_name"] == "Widget"
        assert records[0]["signup_date"] == "2026-01-01T00:00:00"
        assert len(masked_sample(_people(), None, limit=1)) == 1

    def test_masking_settings_defaults(self) -> None:
        settings = masking_settings(None)
        assert settings.default == "hash" and settings.sample_rows == 5 and settings.hook is None
        partial = masking_settings(SimpleNamespace(columns=["a"]))
        assert partial.columns == ("a",) and partial.allow == ()
