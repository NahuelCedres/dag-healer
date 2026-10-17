"""Masking of sensitive data before it is stored, shown, logged, e-mailed or sent to the LLM.

Default behaviour of :func:`mask_dataframe`:

* columns flagged as PII (``pii_columns``, e.g. from the contract), columns listed in
  ``masking.columns`` and columns whose *name* looks sensitive (:func:`is_sensitive_column`)
  are masked value by value with :func:`mask_value` (``hash`` -> ``"h:" + 10 hex chars`` of a
  salted sha256, or ``redact`` -> ``"***"``), whatever their dtype;
* the remaining free-text columns (string/object/categorical) are scrubbed with
  :func:`mask_text` (e-mails, phone numbers, IPv4 addresses, IBANs and runs of 7+ digits);
* other numeric, boolean and date/datetime columns are left untouched;
* columns in ``masking.allow`` are never modified (the allow-list wins over everything);
* finally ``masking.hook`` (``"package.module:function"``, called as ``function(df, context)``
  and returning a DataFrame) can replace or extend the result.

Hashes are deterministic for a given salt (dag-healer uses the incident id), so equal
values stay joinable inside one incident without revealing them.

pandas is imported lazily: :func:`mask_value`, :func:`mask_text` and
:func:`is_sensitive_column` are cheap enough for log/e-mail code paths.
"""

from __future__ import annotations

import hashlib
import importlib
import math
import numbers
import re
import uuid
from collections.abc import Callable, Iterable, Mapping
from dataclasses import dataclass
from datetime import date, datetime, time, timedelta
from decimal import Decimal
from functools import lru_cache
from typing import TYPE_CHECKING, Any, Literal

if TYPE_CHECKING:
    import pandas as pd

MaskMode = Literal["hash", "redact"]

HASH_PREFIX = "h:"
HASH_LENGTH = 10
REDACTED = "***"

# --------------------------------------------------------------------------- column names

# Exact tokens of a column name (``customer_email`` -> ``customer``, ``email``).
_SENSITIVE_TOKENS = frozenset(
    {
        "email", "emails", "mail", "phone", "phones", "mobile", "telephone", "tel", "fax",
        "name", "names", "firstname", "lastname", "fullname", "surname", "middlename",
        "username", "address", "addr", "street", "zip", "zipcode", "postcode",
        "postal", "ssn", "dni", "cuit", "cuil", "nif", "nie", "cpf", "rut", "curp",
        "document", "passport", "iban", "card", "cvv", "cvc", "ip", "ipv4", "ipv6",
        "birth", "birthdate", "birthday", "dob", "password", "passwd", "pwd", "secret",
        "token", "apikey", "credential", "credentials",
    }
)  # fmt: skip

# Substrings of the compacted name (``E-Mail Address`` -> ``emailaddress``).
_SENSITIVE_SUBSTRINGS = (
    "email", "phone", "passport", "iban", "firstname", "lastname", "fullname", "surname",
    "middlename", "birthdate", "dateofbirth", "birthday", "creditcard", "cardnumber",
    "password", "nationalid", "taxid", "socialsecurity", "address", "apikey", "accesstoken",
    "secretkey", "privatekey",
)  # fmt: skip

# ``name`` next to one of these tokens is not a person's name (``product_name``, ``dag_name``).
_NON_PERSON_QUALIFIERS = frozenset(
    {
        "product", "item", "file", "sku", "category", "warehouse", "store", "shop", "brand",
        "model", "table", "column", "col", "field", "schema", "database", "db", "dag", "task",
        "job", "host", "server", "service", "project", "event", "type", "status", "plan",
        "city", "country", "region", "state", "province", "department", "dept", "team",
        "metric", "bucket", "queue", "topic", "app", "application", "package", "module",
        "class", "function", "method", "variable", "tag", "label", "stage", "step",
        "pipeline", "dataset", "source", "sheet", "tab", "page", "site", "domain", "channel",
        "campaign", "segment", "tier", "level", "unit", "currency", "language", "locale",
        "color", "colour", "size", "material", "feature", "attribute", "index", "key",
    }
)  # fmt: skip

_CAMEL_RE = re.compile(r"(?<=[a-z0-9])(?=[A-Z])")
_NON_ALNUM_RE = re.compile(r"[^a-z0-9]+")


def _name_tokens(name: str) -> list[str]:
    """Split a column name into lowercase tokens (handles snake, kebab and camelCase)."""
    spaced = _CAMEL_RE.sub("_", str(name))
    return [token for token in _NON_ALNUM_RE.split(spaced.lower()) if token]


def is_sensitive_column(name: Any) -> bool:
    """True when a column name suggests personal or secret data.

    Matches e-mails, phones, person names (but not ``product_name``-style names), postal
    addresses, national/tax/document ids (ssn, dni, cuit, passport ...), bank/card numbers,
    IP addresses, birth dates and credentials.
    """
    tokens = _name_tokens(str(name))
    if not tokens:
        return False
    compact = "".join(tokens)
    if any(fragment in compact for fragment in _SENSITIVE_SUBSTRINGS):
        return True
    for token in tokens:
        if token in ("name", "names"):
            if not any(other in _NON_PERSON_QUALIFIERS for other in tokens if other != token):
                return True
        elif token in _SENSITIVE_TOKENS:
            return True
    return False


# --------------------------------------------------------------------------- values


def _is_bool(value: Any) -> bool:
    """True for Python and numpy booleans (``numpy.bool_`` is ``numpy.bool`` on numpy 2)."""
    kind = type(value)
    return isinstance(value, bool) or (kind.__module__ == "numpy" and kind.__name__ in ("bool", "bool_"))


def _is_missing(value: Any) -> bool:
    """True for None, NaN, NaT and pandas.NA (without importing pandas)."""
    if value is None:
        return True
    if type(value).__name__ in ("NAType", "NaTType"):
        return True
    if isinstance(value, float):
        return math.isnan(value)
    try:
        return bool(value != value)
    except (TypeError, ValueError):
        return False


def _value_text(value: Any) -> str:
    """Canonical text of a value so that ``1``, ``1.0`` and ``np.int64(1)`` hash the same."""
    if isinstance(value, str):
        return value
    if _is_bool(value):
        return str(bool(value))
    if isinstance(value, numbers.Integral):
        return str(int(value))
    if isinstance(value, numbers.Real):
        number = float(value)
        if math.isfinite(number) and number.is_integer():
            return str(int(number))
        return str(number)
    if isinstance(value, (datetime, date, time)):
        return value.isoformat()
    return str(value)


def mask_value(value: Any, mode: MaskMode = "hash", salt: str = "") -> str | None:
    """Mask one value.

    ``hash`` -> ``"h:" + first 10 hex chars of sha256(salt + value)``; ``redact`` -> ``"***"``.
    ``None``/NaN/NaT stay ``None``. The raw value never appears in the output.
    """
    if _is_missing(value):
        return None
    if mode == "redact":
        return REDACTED
    if mode != "hash":
        raise ValueError(f"unknown masking mode {mode!r} (expected 'hash' or 'redact')")
    digest = hashlib.sha256(f"{salt}{_value_text(value)}".encode()).hexdigest()
    return f"{HASH_PREFIX}{digest[:HASH_LENGTH]}"


# --------------------------------------------------------------------------- free text

_TEXT_RE = re.compile(
    r"""
    (?P<datetime>(?<!\d)\d{4}-\d{2}-\d{2}
        (?:[T\s]\d{1,2}:\d{2}(?::\d{2}(?:[.,]\d+)?)?(?:\s?(?:Z|[+-]\d{2}:?\d{2}))?)?(?!\d))
  | (?P<date>(?<!\d)(?:\d{1,2}[/.-]\d{1,2}[/.-]\d{4}|\d{4}[/.]\d{1,2}[/.]\d{1,2})(?!\d))
  | (?P<email>[A-Za-z0-9._%+-]{1,64}@[A-Za-z0-9-]{1,63}(?:\.[A-Za-z0-9-]{1,63}){0,8}\.[A-Za-z]{2,24})
  | (?P<ip>(?<![\d.])(?:\d{1,3}\.){3}\d{1,3}(?![\d.]))
  | (?P<phone>
        (?<![\w+])(?:\+\d{1,3}[\s.-]?)?(?:\(\d{1,4}\)[\s.-]?)?\d{2,5}(?:[\s.-]\d{2,5}){1,5}(?!\w)
      | (?<![\w+])\+\d{7,15}(?!\d))
  | (?P<iban>(?<![A-Za-z0-9])[A-Z]{2}\d{2}[A-Z0-9]{11,30}(?![A-Za-z0-9]))
  | (?P<digits>(?<![A-Za-z0-9])\d{7,}(?![A-Za-z0-9]))
    """,
    re.VERBOSE,
)
_MIN_PHONE_DIGITS = 7
_TEXT_REPLACEMENTS = {
    "email": "[email]",
    "ip": "[ip]",
    "phone": "[phone]",
    "iban": "[iban]",
    "digits": "[number]",
}


def _replace_sensitive(match: re.Match[str]) -> str:
    """Replacement callback for :data:`_TEXT_RE`; dates are kept for diagnosis."""
    kind = match.lastgroup or ""
    text = match.group(0)
    if kind in ("datetime", "date"):
        return text
    if kind in ("phone", "iban") and sum(char.isdigit() for char in text) < _MIN_PHONE_DIGITS:
        return text
    return _TEXT_REPLACEMENTS.get(kind, text)


def mask_text(text: Any) -> str:
    """Redact e-mails, phone numbers, IPv4 addresses, IBANs and runs of 7+ digits in free text.

    Meant for log lines, tracebacks and error messages: ISO dates/timestamps (which are often
    the key evidence of a failure) and short numbers are preserved.
    """
    if text is None:
        return ""
    if not isinstance(text, str):
        text = str(text)
    return _TEXT_RE.sub(_replace_sensitive, text)


# --------------------------------------------------------------------------- settings


class MaskingHookError(RuntimeError):
    """Raised when the configured masking hook cannot be loaded or fails."""


@dataclass(frozen=True)
class MaskingSettings:
    """Normalised masking settings (mirrors ``masking:`` in healer.yaml)."""

    default: MaskMode = "hash"
    columns: tuple[str, ...] = ()
    allow: tuple[str, ...] = ()
    hook: str | None = None
    sample_rows: int = 5


def masking_settings(cfg: Any = None) -> MaskingSettings:
    """Build :class:`MaskingSettings` from a config object, a mapping or ``None``.

    Any object with ``default``, ``columns``, ``allow``, ``hook`` and ``sample_rows``
    attributes works (``HealerConfig.masking``); missing attributes take the defaults.
    """
    if isinstance(cfg, MaskingSettings):
        return cfg

    def read(key: str, fallback: Any) -> Any:
        if cfg is None:
            return fallback
        if isinstance(cfg, Mapping):
            value = cfg.get(key, fallback)
        else:
            value = getattr(cfg, key, fallback)
        return fallback if value is None and key != "hook" else value

    mode = read("default", "hash")
    if mode not in ("hash", "redact"):
        raise ValueError(f"unknown masking mode {mode!r} (expected 'hash' or 'redact')")
    return MaskingSettings(
        default=mode,
        columns=tuple(str(column) for column in read("columns", ()) or ()),
        allow=tuple(str(column) for column in read("allow", ()) or ()),
        hook=read("hook", None) or None,
        sample_rows=int(read("sample_rows", 5)),
    )


@lru_cache(maxsize=32)
def load_hook(spec: str) -> Callable[..., Any]:
    """Import ``"package.module:function"`` and return the callable.

    Raises :class:`MaskingHookError` with a clear message when the spec is malformed, the
    module cannot be imported, the attribute does not exist or is not callable.
    """
    module_name, sep, attr_path = spec.strip().partition(":")
    if not sep or not module_name or not attr_path:
        raise MaskingHookError(f"invalid masking hook {spec!r}: expected 'package.module:function'")
    try:
        target: Any = importlib.import_module(module_name)
    except Exception as exc:  # ImportError, SyntaxError, errors raised at import time
        raise MaskingHookError(
            f"cannot import masking hook module {module_name!r} ({spec!r}): {type(exc).__name__}: {exc}"
        ) from exc
    for part in attr_path.split("."):
        try:
            target = getattr(target, part)
        except AttributeError as exc:
            raise MaskingHookError(
                f"masking hook {spec!r}: module {module_name!r} has no attribute {attr_path!r}"
            ) from exc
    if not callable(target):
        raise MaskingHookError(f"masking hook {spec!r} is not callable")
    return target


def _apply_hook(spec: str, df: pd.DataFrame, context: Mapping[str, Any]) -> pd.DataFrame:
    """Run the masking hook; any failure raises :class:`MaskingHookError` (fail closed)."""
    import pandas as pd

    hook = load_hook(spec)
    try:
        result = hook(df, dict(context))
    except Exception as exc:
        raise MaskingHookError(
            f"masking hook {spec!r} failed: {type(exc).__name__}: {mask_text(str(exc))}"
        ) from exc
    if not isinstance(result, pd.DataFrame):
        raise MaskingHookError(
            f"masking hook {spec!r} must return a pandas DataFrame, got {type(result).__name__}"
        )
    return result


# --------------------------------------------------------------------------- DataFrames


def _is_free_text(series: pd.Series) -> bool:
    """True for string/object/categorical columns (not numeric, boolean or datetime)."""
    from pandas.api import types as ptypes

    dtype = series.dtype
    return not (
        ptypes.is_numeric_dtype(dtype)
        or ptypes.is_datetime64_any_dtype(dtype)
        or ptypes.is_timedelta64_dtype(dtype)
    )


def _mask_series(series: pd.Series, mode: MaskMode, salt: str) -> pd.Series:
    """Mask every value of a column (result dtype: object)."""
    import pandas as pd

    values = [mask_value(value, mode, salt) for value in series.to_numpy(dtype=object)]
    return pd.Series(values, index=series.index, name=series.name, dtype=object)


def _mask_text_series(series: pd.Series) -> pd.Series:
    """Apply :func:`mask_text` to the string values of a column (result dtype: object)."""
    import pandas as pd

    values = [
        mask_text(value) if isinstance(value, str) else value for value in series.to_numpy(dtype=object)
    ]
    return pd.Series(values, index=series.index, name=series.name, dtype=object)


def mask_dataframe(
    df: pd.DataFrame,
    cfg: Any = None,
    *,
    pii_columns: Iterable[str] = (),
    salt: str = "",
    context: Mapping[str, Any] | None = None,
) -> pd.DataFrame:
    """Return a masked copy of ``df`` (see the module docstring for the rules).

    ``cfg`` is ``HealerConfig.masking`` (or any object/mapping with the same attributes) or
    ``None`` for defaults. Column names are matched case-insensitively. ``context`` is passed
    to the hook (for example ``{"dag_id": ..., "task_id": ..., "incident_id": ...}``).
    """
    settings = masking_settings(cfg)
    explicit = {str(column).lower() for column in (*pii_columns, *settings.columns)}
    allowed = {str(column).lower() for column in settings.allow}
    out = df.copy()
    for position, label in enumerate(df.columns):
        name = str(label)
        key = name.lower()
        if key in allowed:
            continue
        series = df.iloc[:, position]
        if key in explicit or is_sensitive_column(name):
            out.isetitem(position, _mask_series(series, settings.default, salt))
        elif _is_free_text(series):
            out.isetitem(position, _mask_text_series(series))
    if settings.hook:
        out = _apply_hook(settings.hook, out, context or {})
    return out


def to_json_safe(value: Any) -> Any:
    """Convert a pandas/numpy/Python value into something ``json.dumps`` accepts.

    NaN/NaT/NA -> ``None``; numpy scalars -> Python scalars; timestamps, dates and times ->
    ISO strings; timedeltas, decimals and UUIDs -> strings; +/-inf -> ``"inf"``/``"-inf"``;
    containers are converted recursively; anything else -> ``str(value)``.
    """
    if value is None:
        return None
    if isinstance(value, str):
        return str(value)
    if isinstance(value, Mapping):
        return {str(key): to_json_safe(item) for key, item in value.items()}
    if isinstance(value, (list, tuple, set, frozenset)):
        return [to_json_safe(item) for item in value]
    if type(value).__name__ == "ndarray":
        return [to_json_safe(item) for item in value.tolist()]
    if _is_missing(value):
        return None
    if _is_bool(value):
        return bool(value)
    if isinstance(value, int):
        return int(value)
    if isinstance(value, (datetime, date, time)):
        return value.isoformat()
    if isinstance(value, (timedelta, Decimal, uuid.UUID)):
        return str(value)
    if isinstance(value, bytes):
        return f"<{len(value)} bytes>"
    return _numpy_scalar_to_json(value)


def _numpy_scalar_to_json(value: Any) -> Any:
    """Convert numpy scalars and floats; falls back to ``str``."""
    import numpy as np

    if isinstance(value, np.integer):
        return int(value)
    if isinstance(value, (float, np.floating)):
        number = float(value)
        if math.isinf(number):
            return "inf" if number > 0 else "-inf"
        return number
    if isinstance(value, np.datetime64):
        import pandas as pd

        return pd.Timestamp(value).isoformat()
    if isinstance(value, np.timedelta64):
        import pandas as pd

        return str(pd.Timedelta(value))
    if isinstance(value, np.generic):
        return to_json_safe(value.item())
    return str(value)


def _unique_keys(columns: Iterable[Any]) -> list[str]:
    """String column keys, de-duplicated with ``.1``, ``.2`` suffixes."""
    keys: list[str] = []
    seen: dict[str, int] = {}
    for label in columns:
        key = str(label)
        if key in seen:
            seen[key] += 1
            key = f"{key}.{seen[key]}"
        else:
            seen[key] = 0
        keys.append(key)
    return keys


def dataframe_to_records(df: pd.DataFrame, limit: int | None = None) -> list[dict[str, Any]]:
    """Return up to ``limit`` rows of ``df`` as JSON-safe dicts (column name -> value)."""
    frame = df if limit is None else df.head(max(0, int(limit)))
    keys = _unique_keys(frame.columns)
    return [
        {key: to_json_safe(value) for key, value in zip(keys, row, strict=True)}
        for row in frame.itertuples(index=False, name=None)
    ]


def masked_sample(
    df: pd.DataFrame,
    cfg: Any = None,
    *,
    pii_columns: Iterable[str] = (),
    salt: str = "",
    context: Mapping[str, Any] | None = None,
    limit: int | None = None,
) -> list[dict[str, Any]]:
    """Mask the first ``limit`` (default ``masking.sample_rows``) rows and return records."""
    settings = masking_settings(cfg)
    rows = settings.sample_rows if limit is None else limit
    head = df.head(max(0, int(rows)))
    masked = mask_dataframe(head, settings, pii_columns=pii_columns, salt=salt, context=context)
    return dataframe_to_records(masked)


__all__ = [
    "HASH_PREFIX",
    "REDACTED",
    "MaskingHookError",
    "MaskingSettings",
    "dataframe_to_records",
    "is_sensitive_column",
    "load_hook",
    "mask_dataframe",
    "mask_text",
    "mask_value",
    "masked_sample",
    "masking_settings",
    "to_json_safe",
]
