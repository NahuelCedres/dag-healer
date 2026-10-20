"""Deterministic classification policy: the gates that decide DATA / CODE / AMBIGUOUS.

:func:`compute_signals` condenses an incident context into :class:`Signals`;
:func:`policy_hint` applies the deterministic part of the gates; :func:`decide` combines the
hint with the LLM classification. The gates, in order:

(a) ``a_contract_violated`` - an **approved** contract is violated and the error is data-shaped
    (or a violated column is named in the error) -> **data**. Deterministic: the LLM only writes
    the diagnosis and its confidence does not matter.
``infra`` - connection refused, timeouts, OOM, SIGTERM/SIGKILL, ``AirflowTaskTimeout``, zombies
    -> **ambiguous** (nothing to fix in data or code; retry or ask an operator).
(b) ``b_code_with_valid_data`` - approved contract, no violations and a traceback frame inside
    the repository -> **code** when the LLM agrees with confidence >= ``min_confidence``, else
    ambiguous.
(c) ``c_no_approved_contract`` - no approved contract (draft, stale or missing) and a data-shaped
    error -> **ambiguous**.
``llm`` - everything else is decided by the LLM, constrained: ``data`` needs a violated approved
    contract (otherwise ambiguous), ``code`` is refused while an approved contract is violated.
(d) ``d_low_confidence`` - any LLM-dependent decision with confidence < ``min_confidence``
    -> **ambiguous**.

Data-shaped errors: a ``KeyError`` naming a column, pandas ``ParserError`` / ``EmptyDataError``,
"not in index", "missing column", dtype/conversion errors raised by pandas/numpy/pyarrow on
dataframe columns, missing data files and contract violations.
"""

from __future__ import annotations

import re
from collections.abc import Iterable, Mapping
from dataclasses import asdict, dataclass, field
from typing import Any, Literal

from dag_healer.masking import mask_text

ErrorKind = Literal["data_shape", "code", "infra", "unknown"]
Category = Literal["data", "code", "ambiguous"]

RULE_A = "a_contract_violated"
RULE_B = "b_code_with_valid_data"
RULE_C = "c_no_approved_contract"
RULE_D = "d_low_confidence"
RULE_INFRA = "infra"
RULE_LLM = "llm"
RULES: tuple[str, ...] = (RULE_A, RULE_B, RULE_C, RULE_D, RULE_INFRA, RULE_LLM)

DATA_GATE_CONFIDENCE = 0.95
"""Confidence reported for decisions taken by gate (a)."""
AMBIGUOUS_CONFIDENCE = 0.5
"""Confidence reported for ambiguous decisions taken without the LLM."""

_MESSAGE_CHARS = 300


# --------------------------------------------------------------------------- patterns

_FRAME_RE = re.compile(r'File "(?P<file>[^"]+)", line (?P<line>\d+)(?:, in (?P<function>\S+))?')
_LIBRARY_RE = re.compile(r"(?:^|[\\/])(?:pandas|numpy|pyarrow)[\\/]", re.IGNORECASE)
_EXCEPTION_LINE_RE = re.compile(
    r"^(?P<type>[A-Za-z_][\w.]*(?:Error|Exception|Exit|Interrupt|Timeout|Terminated|Killed|Warning))"
    r"(?::\s?(?P<message>.*))?$"
)

INFRA_TYPES = frozenset(
    {
        "AirflowTaskTimeout",
        "AirflowTaskTerminated",
        "AirflowZombieTaskException",
        "TaskDeferralTimeout",
        "SoftTimeLimitExceeded",
        "TimeLimitExceeded",
        "WorkerLostError",
        "ConnectionError",
        "ConnectionRefusedError",
        "ConnectionResetError",
        "ConnectionAbortedError",
        "BrokenPipeError",
        "TimeoutError",
        "timeout",
        "gaierror",
        "ConnectTimeout",
        "ReadTimeout",
        "ConnectError",
        "RemoteDisconnected",
        "MemoryError",
    }
)
_INFRA_MESSAGE_RE = re.compile(
    r"connection (?:refused|reset|aborted|timed out)|\btimed? ?out\b|temporary failure in name resolution"
    r"|name or service not known|could not connect to|failed to establish a new connection"
    r"|max retries exceeded|out of memory|cannot allocate memory|\boom(?:killed)?\b|\bsig(?:term|kill)\b"
    r"|negsignal|\bzombie\b|killed by signal|no space left on device|service unavailable",
    re.IGNORECASE,
)
_INFRA_LOG_RE = re.compile(
    r"\bsig(?:term|kill)\b|negsignal|\boom(?:killed)?\b|out of memory|\bzombie\b|killed by signal"
    r"|exited with return code -9|cannot allocate memory",
    re.IGNORECASE,
)

DATA_TYPES = frozenset(
    {
        "ParserError",
        "EmptyDataError",
        "ArrowInvalid",
        "ArrowTypeError",
        "IntCastingNaNError",
        "OutOfBoundsDatetime",
        "DateParseError",
        "SchemaError",
        "SchemaErrors",
        "ContractViolation",
    }
)
_DATA_MESSAGE_RE = re.compile(
    r"not in index|missing (?:required )?columns?|none of \[|are in the \[(?:columns|index)\]"
    r"|no columns to parse|error tokenizing data|usecols do not match|columns? .{0,60}"
    r"(?:not found|do(?:es)? not exist|is missing|are missing)"
    r"|contract violation|violates the (?:data )?contract",
    re.IGNORECASE,
)
CONVERSION_TYPES = frozenset({"ValueError", "TypeError", "OverflowError", "UnicodeDecodeError"})
_CONVERSION_RE = re.compile(
    r"could not convert|invalid literal for int\(\)|unable to parse string"
    r"|cannot convert float nan to integer"
    r"|cannot convert non-finite values|cannot (?:safely )?cast|unknown datetime string format"
    r"|does(?:n't| not) match format|unknown string format|out of bounds nanosecond|unsupported operand type"
    r"|cannot mask with non-boolean array|boolean value of na is ambiguous|invalid value .{0,40} for dtype"
    r"|codec can't decode",
    re.IGNORECASE,
)
_DATA_FILE_RE = re.compile(r"\.(?:csv|tsv|parquet|json|jsonl|xlsx?)\b", re.IGNORECASE)
CODE_TYPES = frozenset(
    {
        "NameError",
        "AttributeError",
        "ImportError",
        "ModuleNotFoundError",
        "SyntaxError",
        "IndentationError",
        "TabError",
        "TypeError",
        "UnboundLocalError",
        "NotImplementedError",
        "RecursionError",
        "ZeroDivisionError",
        "AssertionError",
        "IndexError",
        "KeyError",
        "ValueError",
        "LookupError",
        "StopIteration",
    }
)

_NAME_LIST_PATTERNS = (
    re.compile(r"\[(?P<names>[^\]]*)\] not in index"),
    re.compile(r"None of \[Index\(\[(?P<names>[^\]]*)\]"),
    re.compile(r"Columns?\(s\) \[(?P<names>[^\]]*)\] do(?:es)? not exist"),
    re.compile(r"[Mm]issing (?:required )?columns?\W+\[(?P<names>[^\]]*)\]"),
)
_NAME_PATTERNS = (
    re.compile(r"[Mm]issing (?:required )?columns?\s*[:=]?\s*['\"](?P<name>[^'\"]+)['\"]"),
    re.compile(r"[Mm]issing (?:required )?columns?\s*:\s*(?P<name>[A-Za-z_][\w.-]*)"),
    re.compile(r"[Cc]olumns? ['\"](?P<name>[^'\"]+)['\"] (?:not found|does not exist|is missing)"),
)
_QUOTED_RE = re.compile(r"['\"]([^'\"]+)['\"]")
_IDENTIFIER_RE = re.compile(r"^[A-Za-z_][\w .-]{0,99}$")


# --------------------------------------------------------------------------- data classes


@dataclass
class Signals:
    """Facts about an incident that the policy (and the LLM prompt) reason about."""

    contract_status: str
    violation_rules: list[str]
    violation_columns: list[str]
    draft_violation_rules: list[str]
    exception_type: str | None
    error_kind: ErrorKind
    repo_frames_present: bool
    top_repo_frame: dict[str, Any] | None
    missing_column_in_error: str | None
    #: Violated (approved) columns quoted in the exception message or traceback.
    columns_in_error: list[str] = field(default_factory=list)
    #: ``"rule: dataset.column"`` per approved-contract violation, the ones named in the error first.
    violation_details: list[str] = field(default_factory=list)
    #: The exception message, masked and truncated.
    exception_message: str | None = None
    #: What made the error look like an infrastructure problem.
    infra_marker: str | None = None

    def to_dict(self) -> dict[str, Any]:
        """JSON-safe dict."""
        return asdict(self)


@dataclass
class PolicyHint:
    """The deterministic part of the decision: the gate that applies and its candidate category."""

    category: Category
    rule: str
    reason: str
    needs_llm_confirmation: bool

    def to_dict(self) -> dict[str, Any]:
        """JSON-safe dict."""
        return asdict(self)


@dataclass
class Decision:
    """Final category of an incident and why."""

    category: Category
    confidence: float
    reason: str
    rule: str
    needs_llm_confirmation: bool

    def to_dict(self) -> dict[str, Any]:
        """JSON-safe dict."""
        return asdict(self)


# --------------------------------------------------------------------------- helpers


def _attr(obj: Any, name: str, default: Any = None) -> Any:
    if isinstance(obj, Mapping):
        return obj.get(name, default)
    return getattr(obj, name, default)


def _unique(values: Iterable[Any]) -> list[str]:
    seen: list[str] = []
    for value in values:
        if value is None or value == "":
            continue
        text = str(value)
        if text not in seen:
            seen.append(text)
    return seen


def short_type(exception_type: str | None) -> str | None:
    """``"pandas.errors.ParserError"`` -> ``"ParserError"``."""
    if not exception_type:
        return None
    return str(exception_type).strip().rsplit(".", 1)[-1] or None


def frame_files(traceback_text: str) -> list[str]:
    """File paths of the ``File "...", line N`` frames of a traceback, in order."""
    return [match.group("file") for match in _FRAME_RE.finditer(traceback_text or "")]


def last_exception(text: str) -> tuple[str | None, str | None]:
    """``(type, message)`` of the last ``SomeError: message`` line of ``text``."""
    for line in reversed((text or "").splitlines()):
        match = _EXCEPTION_LINE_RE.match(line.strip())
        if match:
            return match.group("type"), (match.group("message") or "").strip() or None
    return None, None


def _names(raw: str) -> list[str]:
    quoted = _QUOTED_RE.findall(raw)
    if quoted:
        return [name.strip() for name in quoted if name.strip()]
    return [part.strip() for part in raw.split(",") if part.strip()]


def missing_column(
    exception_type: str | None,
    message: str | None,
    *,
    library_involved: bool = False,
    known_columns: Iterable[str] = (),
) -> str | None:
    """Column name a data-shaped error complains about (``KeyError: 'amount'``, "not in index"...).

    A bare ``KeyError`` only counts when pandas/numpy is in the traceback or the key is a
    known column (from the contract or the masked sample).
    """
    text = message or ""
    for pattern in _NAME_LIST_PATTERNS:
        match = pattern.search(text)
        if match:
            names = _names(match.group("names"))
            if names:
                return names[0]
    for pattern in _NAME_PATTERNS:
        match = pattern.search(text)
        if match:
            return match.group("name").strip()
    if short_type(exception_type) == "KeyError" and text:
        key = text.strip()
        if len(key) >= 2 and key[0] == key[-1] and key[0] in "'\"":
            key = key[1:-1]
        if _IDENTIFIER_RE.match(key) and (library_involved or key in set(known_columns)):
            return key
    return None


def classify_error(
    exception_type: str | None,
    exception_message: str | None,
    *,
    traceback_text: str = "",
    log_text: str = "",
    known_columns: Iterable[str] = (),
    repo_frames_present: bool = False,
) -> tuple[ErrorKind, str | None, str | None]:
    """Return ``(error_kind, missing_column, infra_marker)`` for a failure.

    Order: infrastructure markers on the exception itself, data-shaped errors, code errors,
    infrastructure markers in the log (a process killed without a Python exception), unknown.
    """
    short = short_type(exception_type)
    message = exception_message or ""
    head = f"{short or ''}: {message}" if short else message
    frames = frame_files(traceback_text)
    library_involved = any(_LIBRARY_RE.search(path) for path in frames)
    library_raised = bool(frames) and bool(_LIBRARY_RE.search(frames[-1]))

    if short in INFRA_TYPES:
        return "infra", None, short
    infra = _INFRA_MESSAGE_RE.search(head) if head else None
    if infra:
        return "infra", None, infra.group(0)

    column = missing_column(short, message, library_involved=library_involved, known_columns=known_columns)
    if column is not None:
        return "data_shape", column, None
    if short in DATA_TYPES:
        return "data_shape", None, None
    if message and _DATA_MESSAGE_RE.search(message):
        return "data_shape", None, None
    if short in CONVERSION_TYPES and _CONVERSION_RE.search(message):
        # Only conversions raised by pandas/numpy/pyarrow on a column are data-shaped; the same
        # message raised by the repository's own parsing code (e.g. strptime) is a code error.
        if library_raised or (short == "UnicodeDecodeError" and library_involved):
            return "data_shape", None, None
    if short == "FileNotFoundError" and _DATA_FILE_RE.search(message):
        return "data_shape", None, None

    if short and (repo_frames_present or short in CODE_TYPES):
        return "code", None, None
    log_marker = _INFRA_LOG_RE.search(f"{log_text}\n{traceback_text}")
    if log_marker:
        return "infra", None, log_marker.group(0)
    return "unknown", None, None


def _contract_columns(contract: Any) -> list[str]:
    columns: list[str] = []
    for dataset in _attr(contract, "datasets", None) or []:
        for column in _attr(dataset, "columns", None) or []:
            name = _attr(column, "name")
            if name:
                columns.append(str(name))
    return columns


def _sample_columns(sample: Any) -> list[str]:
    if isinstance(sample, Mapping):
        return [str(column) for column in sample.get("columns") or []]
    return []


def _quoted_in(column: str, text: str) -> bool:
    return bool(re.search(r"['\"\[]" + re.escape(column) + r"['\"\]]", text))


# --------------------------------------------------------------------------- signals


def compute_signals(ctx: Any) -> Signals:
    """Condense an incident context (``IncidentContext`` or a duck-typed equivalent) into signals."""
    contract_status = str(_attr(ctx, "contract_status") or "missing")
    violations = list(_attr(ctx, "violations") or [])
    draft_violations = list(_attr(ctx, "draft_violations") or [])
    violation_rules = _unique(_attr(v, "rule") for v in violations)
    violation_columns = _unique(_attr(v, "column") for v in violations)
    draft_rules = _unique(_attr(v, "rule") for v in draft_violations)

    traceback_text = str(_attr(ctx, "traceback_text") or "")
    error_text = str(_attr(ctx, "error_text") or "")
    log_excerpt = str(_attr(ctx, "log_excerpt") or "")
    tb_source = traceback_text or error_text or log_excerpt

    exception_type = _attr(ctx, "exception_type")
    exception_message = _attr(ctx, "exception_message")
    if not exception_type:
        exception_type, parsed_message = last_exception(tb_source)
        exception_message = exception_message or parsed_message

    repo_frames = [dict(frame) for frame in _attr(ctx, "repo_frames") or [] if isinstance(frame, Mapping)]
    top_frame = repo_frames[-1] if repo_frames else None

    known_columns = set(violation_columns)
    known_columns.update(_unique(_attr(v, "column") for v in draft_violations))
    known_columns.update(_contract_columns(_attr(ctx, "contract")))
    known_columns.update(_sample_columns(_attr(ctx, "masked_sample")))

    kind, column, marker = classify_error(
        exception_type,
        exception_message,
        traceback_text=tb_source,
        log_text=f"{error_text}\n{log_excerpt}",
        known_columns=known_columns,
        repo_frames_present=bool(repo_frames),
    )
    tail = "\n".join(tb_source.splitlines()[-30:])
    haystack = f"{exception_message or ''}\n{tail}"
    columns_in_error = [col for col in violation_columns if _quoted_in(col, haystack)]
    named = {column, *columns_in_error} - {None}
    ordered = sorted(violations, key=lambda v: 0 if _attr(v, "column") in named else 1)
    details = _unique(_violation_detail(v) for v in ordered)

    message = mask_text(exception_message).strip() if exception_message else None
    if message and len(message) > _MESSAGE_CHARS:
        message = message[: _MESSAGE_CHARS - 3] + "..."
    return Signals(
        contract_status=contract_status,
        violation_rules=violation_rules,
        violation_columns=violation_columns,
        draft_violation_rules=draft_rules,
        exception_type=str(exception_type) if exception_type else None,
        error_kind=kind,
        repo_frames_present=bool(repo_frames),
        top_repo_frame=top_frame,
        missing_column_in_error=column,
        columns_in_error=columns_in_error,
        violation_details=details,
        exception_message=message or None,
        infra_marker=marker,
    )


# --------------------------------------------------------------------------- decision


def _where(signals: Signals) -> str:
    frame = signals.top_repo_frame or {}
    if not frame.get("file"):
        return "the repository"
    return f"{frame['file']}::{frame['function']}" if frame.get("function") else str(frame["file"])


def _violation_detail(violation: Any) -> str | None:
    rule = _attr(violation, "rule")
    if not rule:
        return None
    target = ".".join(str(part) for part in (_attr(violation, "dataset"), _attr(violation, "column")) if part)
    return f"{rule}: {target}" if target else str(rule)


def _rules(signals: Signals) -> str:
    if signals.violation_details:
        return "; ".join(signals.violation_details)
    return ", ".join(signals.violation_rules)


def policy_hint(signals: Signals) -> PolicyHint:
    """Apply the deterministic gates: which gate applies and its candidate category."""
    approved = signals.contract_status == "approved"
    violated = approved and bool(signals.violation_rules)
    named_column = signals.missing_column_in_error in signals.violation_columns
    data_evidence = signals.error_kind == "data_shape" or named_column or bool(signals.columns_in_error)
    error = short_type(signals.exception_type) or "an error"

    if violated and data_evidence:
        return PolicyHint(
            "data",
            RULE_A,
            f"The approved contract is violated ({_rules(signals)}) and the task failed with a data-shaped "
            f"error ({error}).",
            False,
        )
    if signals.error_kind == "infra":
        return PolicyHint(
            "ambiguous",
            RULE_INFRA,
            f"The failure looks like an infrastructure problem ({signals.infra_marker or error}); "
            "neither the data nor the code is implicated: retry the task or ask an operator.",
            False,
        )
    if approved and not signals.violation_rules and signals.repo_frames_present:
        return PolicyHint(
            "code",
            RULE_B,
            f"The data satisfies the approved contract and the traceback ({error}) points into "
            f"{_where(signals)}: candidate code defect, to be confirmed by the LLM.",
            True,
        )
    if not approved and signals.error_kind == "data_shape":
        draft = ""
        if signals.draft_violation_rules:
            findings = ", ".join(signals.draft_violation_rules)
            draft = f"; the {signals.contract_status} contract reports {findings}"
        return PolicyHint(
            "ambiguous",
            RULE_C,
            f"There is no approved data contract (status: {signals.contract_status}) and the task failed "
            f"with a data-shaped error ({error}){draft}: a human must decide whether the data or the code "
            "is wrong.",
            False,
        )
    if violated:
        candidate: Category = "data"
    elif signals.error_kind == "code":
        candidate = "code"
    else:
        candidate = "ambiguous"
    return PolicyHint(
        candidate,
        RULE_LLM,
        f"No deterministic gate applies (contract: {signals.contract_status}, "
        f"error kind: {signals.error_kind}); the LLM classification decides.",
        True,
    )


def decide(signals: Signals, llm: Any, *, min_confidence: float) -> Decision:
    """Final decision from the signals and the LLM classification (``None`` when unavailable).

    ``llm`` is a :class:`dag_healer.agent.llm.Classification` (any object with ``category`` and
    ``confidence`` works).
    """
    hint = policy_hint(signals)
    if not hint.needs_llm_confirmation:
        confidence = DATA_GATE_CONFIDENCE if hint.category == "data" else AMBIGUOUS_CONFIDENCE
        return Decision(hint.category, confidence, hint.reason, hint.rule, False)

    if llm is None:
        return Decision(
            "ambiguous",
            AMBIGUOUS_CONFIDENCE,
            f"{hint.reason} No LLM classification is available to confirm it, so a human must decide.",
            hint.rule,
            True,
        )

    category = str(getattr(llm, "category", "ambiguous"))
    try:
        confidence = float(getattr(llm, "confidence", 0.0))
    except (TypeError, ValueError):
        confidence = 0.0
    if category not in ("data", "code", "ambiguous"):
        category = "ambiguous"
    if confidence < min_confidence:
        return Decision(
            "ambiguous",
            confidence,
            f"The LLM classified the failure as {category} with confidence {confidence:.2f}, below the "
            f"{min_confidence:.2f} threshold; a human must decide.",
            RULE_D,
            True,
        )

    if hint.rule == RULE_B:
        if category == "code":
            return Decision(
                "code",
                confidence,
                f"The data satisfies the approved contract, the traceback points into {_where(signals)} and "
                "the LLM confirmed a code defect.",
                RULE_B,
                True,
            )
        return Decision(
            "ambiguous",
            confidence,
            f"The data satisfies the approved contract and the traceback points into {_where(signals)}, but "
            f"the LLM classified the failure as {category}; a human must decide.",
            RULE_B,
            True,
        )

    approved = signals.contract_status == "approved"
    violated = approved and bool(signals.violation_rules)
    if category == "data" and not approved:
        return Decision(
            "ambiguous",
            confidence,
            "The LLM points at the data, but there is no approved contract "
            f"(status: {signals.contract_status}) to confirm it; a human must decide.",
            RULE_C,
            True,
        )
    if category == "data" and not violated:
        return Decision(
            "ambiguous",
            confidence,
            "The LLM points at the data, but the data satisfies the approved contract; a human must decide "
            "(the contract may need a new rule).",
            RULE_LLM,
            True,
        )
    if category == "code" and violated:
        return Decision(
            "ambiguous",
            confidence,
            f"The LLM points at the code, but the approved contract is violated ({_rules(signals)}); a human "
            "must decide.",
            RULE_LLM,
            True,
        )
    return Decision(
        category,  # type: ignore[arg-type]
        confidence,
        f"No deterministic gate applies; the LLM classified the failure as {category} "
        f"(confidence {confidence:.2f}).",
        RULE_LLM,
        True,
    )


__all__ = [
    "AMBIGUOUS_CONFIDENCE",
    "DATA_GATE_CONFIDENCE",
    "RULES",
    "RULE_A",
    "RULE_B",
    "RULE_C",
    "RULE_D",
    "RULE_INFRA",
    "RULE_LLM",
    "Decision",
    "ErrorKind",
    "PolicyHint",
    "Signals",
    "classify_error",
    "compute_signals",
    "decide",
    "frame_files",
    "last_exception",
    "missing_column",
    "policy_hint",
    "short_type",
]
