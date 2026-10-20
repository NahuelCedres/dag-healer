"""Unit tests for dag_healer.agent.policy (signals, error kinds and the decision gates)."""

from __future__ import annotations

from dataclasses import dataclass, field
from types import SimpleNamespace
from typing import Any

import pytest

from dag_healer.agent.llm import Classification
from dag_healer.agent.policy import (
    RULE_A,
    RULE_B,
    RULE_C,
    RULE_D,
    RULE_INFRA,
    RULE_LLM,
    Signals,
    classify_error,
    compute_signals,
    decide,
    frame_files,
    last_exception,
    missing_column,
    policy_hint,
)
from dag_healer.contracts.schema import Violation

SITE = "/home/airflow/.local/lib/python3.13/site-packages"
REPO = "/opt/airflow/repo"

SALES_TRACEBACK = f"""Traceback (most recent call last):
  File "{SITE}/pandas/core/indexes/base.py", line 3641, in get_loc
    return self._engine.get_loc(casted_key)
  File "pandas/_libs/index.pyx", line 168, in pandas._libs.index.IndexEngine.get_loc
  File "pandas/_libs/hashtable_class_helper.pxi", line 7668, in get_item
KeyError: 'amount'

The above exception was the direct cause of the following exception:

Traceback (most recent call last):
  File "{SITE}/airflow/sdk/execution_time/task_runner.py", line 1010, in run
    result = _execute_task(context=context, ti=ti, log=log)
  File "{REPO}/src/etl/sales.py", line 26, in transform_sales
    df["amount"] = pd.to_numeric(df["amount"], errors="coerce")
  File "{SITE}/pandas/core/frame.py", line 4378, in __getitem__
    indexer = self.columns.get_loc(key)
  File "{SITE}/pandas/core/indexes/base.py", line 3648, in get_loc
    raise KeyError(key) from err
KeyError: 'amount'
"""

CUSTOMERS_TRACEBACK = f"""Traceback (most recent call last):
  File "{SITE}/airflow/sdk/execution_time/task_runner.py", line 1010, in run
    result = _execute_task(context=context, ti=ti, log=log)
  File "{REPO}/src/etl/customers.py", line 36, in transform_customers
    df["signup_date"] = df["signup_date"].apply(parse_signup_date)
  File "{SITE}/pandas/core/series.py", line 5084, in apply
    ).apply()
  File "{SITE}/pandas/core/algorithms.py", line 1715, in map_array
    return lib.map_infer(values, mapper)
  File "pandas/_libs/lib.pyx", line 3071, in pandas._libs.lib.map_infer
  File "{REPO}/src/etl/customers.py", line 19, in parse_signup_date
    return datetime.strptime(value.strip(), "%Y-%m-%d").date()
  File "/usr/local/lib/python3.13/_strptime.py", line 558, in _strptime
    raise ValueError("unconverted data remains: %s" %
ValueError: unconverted data remains: T08:30:00Z
"""

INVENTORY_TRACEBACK = f"""Traceback (most recent call last):
  File "{SITE}/pandas/core/indexes/base.py", line 3641, in get_loc
    return self._engine.get_loc(casted_key)
KeyError: 'qty'

The above exception was the direct cause of the following exception:

Traceback (most recent call last):
  File "{REPO}/src/etl/inventory.py", line 29, in buildReport
    df["qty"] = df["qty"].fillna(0).astype(int)
  File "{SITE}/pandas/core/frame.py", line 4378, in __getitem__
    indexer = self.columns.get_loc(key)
  File "{SITE}/pandas/core/indexes/base.py", line 3648, in get_loc
    raise KeyError(key) from err
KeyError: 'qty'
"""


@dataclass
class FakeContext:
    """Duck-typed stand-in for dag_healer.agent.context.IncidentContext."""

    incident_id: str = "0123456789abcdef0123456789abcdef"
    dag_id: str = "some_dag"
    run_id: str = "manual__2026-10-17T10:00:00+00:00"
    task_id: str = "some_task"
    map_index: int = -1
    try_number: int = 1
    error_text: str = ""
    exception_type: str | None = None
    exception_message: str | None = None
    traceback_text: str = ""
    repo_frames: list[dict[str, Any]] = field(default_factory=list)
    log_excerpt: str = ""
    dag_file: str | None = None
    dag_source: str | None = None
    task: Any = None
    code_snippets: dict[str, str] = field(default_factory=dict)
    contract: Any = None
    contract_status: str = "missing"
    violations: list[Violation] = field(default_factory=list)
    draft_violations: list[Violation] = field(default_factory=list)
    sample_dataset: str | None = None
    masked_sample: dict[str, Any] | None = None
    datasets_checked: list[str] = field(default_factory=list)
    notes: list[str] = field(default_factory=list)


def sales_context() -> FakeContext:
    return FakeContext(
        dag_id="sales_daily_etl",
        task_id="transform_sales",
        exception_type="KeyError",
        exception_message="'amount'",
        traceback_text=SALES_TRACEBACK,
        error_text=SALES_TRACEBACK,
        repo_frames=[{"file": "src/etl/sales.py", "line": 26, "function": "transform_sales"}],
        contract_status="approved",
        violations=[
            Violation(
                rule="missing_column", dataset="sales", column="amount", message="column 'amount' is missing"
            ),
            Violation(
                rule="unexpected_column", dataset="sales", column="amt", message="unexpected column 'amt'"
            ),
        ],
        masked_sample={"dataset": "sales", "columns": ["order_id", "amt"], "rows": [], "masked": True},
    )


def customers_context() -> FakeContext:
    return FakeContext(
        dag_id="customers_sync",
        task_id="transform",
        exception_type="ValueError",
        exception_message="unconverted data remains: T08:30:00Z",
        traceback_text=CUSTOMERS_TRACEBACK,
        repo_frames=[
            {"file": "src/etl/customers.py", "line": 36, "function": "transform_customers"},
            {"file": "src/etl/customers.py", "line": 19, "function": "parse_signup_date"},
        ],
        contract_status="approved",
    )


def inventory_context() -> FakeContext:
    return FakeContext(
        dag_id="inventory_report",
        task_id="build_report",
        exception_type="KeyError",
        exception_message="'qty'",
        traceback_text=INVENTORY_TRACEBACK,
        repo_frames=[{"file": "src/etl/inventory.py", "line": 29, "function": "buildReport"}],
        contract_status="draft",
        draft_violations=[
            Violation(
                rule="missing_column", dataset="inventory", column="qty", message="column 'qty' is missing"
            ),
            Violation(
                rule="unexpected_column", dataset="inventory", column="warehouse_code", message="unexpected"
            ),
        ],
    )


def llm(category: str, confidence: float) -> Classification:
    return Classification(category=category, confidence=confidence, diagnosis="d")


def signals(**overrides: Any) -> Signals:
    base: dict[str, Any] = {
        "contract_status": "approved",
        "violation_rules": [],
        "violation_columns": [],
        "draft_violation_rules": [],
        "exception_type": "ValueError",
        "error_kind": "code",
        "repo_frames_present": True,
        "top_repo_frame": {"file": "src/etl/x.py", "line": 3, "function": "f"},
        "missing_column_in_error": None,
    }
    base.update(overrides)
    return Signals(**base)


# --------------------------------------------------------------------------- demo scenarios


def test_sales_scenario_is_data_by_gate_a() -> None:
    sig = compute_signals(sales_context())
    assert sig.contract_status == "approved"
    assert sig.violation_rules == ["missing_column", "unexpected_column"]
    assert sig.violation_columns == ["amount", "amt"]
    assert sig.error_kind == "data_shape"
    assert sig.missing_column_in_error == "amount"
    assert sig.columns_in_error == ["amount"]
    assert sig.top_repo_frame == {"file": "src/etl/sales.py", "line": 26, "function": "transform_sales"}
    for answer in (None, llm("code", 0.99), llm("ambiguous", 0.1)):
        decision = decide(sig, answer, min_confidence=0.75)
        assert (decision.category, decision.rule) == ("data", RULE_A)
        assert decision.needs_llm_confirmation is False
        assert decision.confidence == pytest.approx(0.95)


def test_customers_scenario_is_code_by_gate_b_when_the_llm_agrees() -> None:
    sig = compute_signals(customers_context())
    assert sig.error_kind == "code"
    assert sig.violation_rules == []
    assert sig.missing_column_in_error is None
    assert sig.top_repo_frame == {"file": "src/etl/customers.py", "line": 19, "function": "parse_signup_date"}
    hint = policy_hint(sig)
    assert (hint.category, hint.rule, hint.needs_llm_confirmation) == ("code", RULE_B, True)

    decision = decide(sig, llm("code", 0.9), min_confidence=0.75)
    assert (decision.category, decision.rule, decision.confidence) == ("code", RULE_B, 0.9)
    assert "parse_signup_date" in decision.reason


def test_inventory_scenario_is_ambiguous_by_gate_c() -> None:
    sig = compute_signals(inventory_context())
    assert sig.contract_status == "draft"
    assert sig.violation_rules == []
    assert sig.draft_violation_rules == ["missing_column", "unexpected_column"]
    assert sig.error_kind == "data_shape"
    assert sig.missing_column_in_error == "qty"
    for answer in (None, llm("data", 0.99), llm("code", 0.99)):
        decision = decide(sig, answer, min_confidence=0.75)
        assert (decision.category, decision.rule) == ("ambiguous", RULE_C)
        assert decision.needs_llm_confirmation is False
    assert "draft" in decide(sig, None, min_confidence=0.75).reason


# --------------------------------------------------------------------------- decision table


@pytest.mark.parametrize(
    ("sig", "answer", "category", "rule"),
    [
        # (a) approved contract violated + data-shaped error -> data, whatever the LLM says
        (
            signals(
                violation_rules=["missing_column"], violation_columns=["amount"], error_kind="data_shape"
            ),
            llm("code", 0.99),
            "data",
            RULE_A,
        ),
        # (a) the violated column is quoted in the error even though the error looks like code
        (
            signals(
                violation_rules=["nulls_not_allowed"],
                violation_columns=["amount"],
                columns_in_error=["amount"],
            ),
            None,
            "data",
            RULE_A,
        ),
        (
            signals(
                violation_rules=["missing_column"],
                violation_columns=["amount"],
                error_kind="code",
                missing_column_in_error="amount",
            ),
            None,
            "data",
            RULE_A,
        ),
        # infra -> ambiguous, without the LLM
        (signals(error_kind="infra", infra_marker="SIGKILL"), llm("code", 0.99), "ambiguous", RULE_INFRA),
        (signals(contract_status="missing", error_kind="infra"), None, "ambiguous", RULE_INFRA),
        # (b) approved, no violations, repo frame: code only when the LLM confirms
        (signals(), llm("code", 0.9), "code", RULE_B),
        (signals(), llm("code", 0.75), "code", RULE_B),  # threshold is inclusive
        (signals(), llm("code", 0.74), "ambiguous", RULE_D),
        (signals(), llm("data", 0.9), "ambiguous", RULE_B),
        (signals(), llm("ambiguous", 0.9), "ambiguous", RULE_B),
        (signals(), None, "ambiguous", RULE_B),
        (signals(error_kind="data_shape"), llm("code", 0.9), "code", RULE_B),
        # (c) no approved contract + data-shaped error -> ambiguous
        (signals(contract_status="draft", error_kind="data_shape"), llm("data", 0.99), "ambiguous", RULE_C),
        (signals(contract_status="stale", error_kind="data_shape"), llm("code", 0.99), "ambiguous", RULE_C),
        (signals(contract_status="missing", error_kind="data_shape"), None, "ambiguous", RULE_C),
        # fallthrough: the LLM decides, constrained by the contract
        (signals(contract_status="missing"), llm("code", 0.9), "code", RULE_LLM),
        (signals(contract_status="missing"), llm("code", 0.5), "ambiguous", RULE_D),
        (signals(contract_status="missing"), llm("data", 0.9), "ambiguous", RULE_C),
        (signals(contract_status="missing"), None, "ambiguous", RULE_LLM),
        (
            signals(violation_rules=["out_of_range"], violation_columns=["price"]),
            llm("data", 0.9),
            "data",
            RULE_LLM,
        ),
        (
            signals(violation_rules=["out_of_range"], violation_columns=["price"]),
            llm("code", 0.9),
            "ambiguous",
            RULE_LLM,
        ),
        (
            signals(repo_frames_present=False, top_repo_frame=None, error_kind="unknown"),
            llm("data", 0.9),
            "ambiguous",
            RULE_LLM,
        ),
        (
            signals(repo_frames_present=False, top_repo_frame=None, error_kind="unknown"),
            llm("ambiguous", 0.9),
            "ambiguous",
            RULE_LLM,
        ),
        (signals(repo_frames_present=False, top_repo_frame=None), llm("code", 0.8), "code", RULE_LLM),
    ],
)
def test_decision_table(sig: Signals, answer: Classification | None, category: str, rule: str) -> None:
    decision = decide(sig, answer, min_confidence=0.75)
    assert (decision.category, decision.rule) == (category, rule)
    assert decision.reason
    assert 0.0 <= decision.confidence <= 1.0


def test_low_confidence_reports_the_llm_confidence() -> None:
    decision = decide(signals(), llm("code", 0.4), min_confidence=0.75)
    assert decision.confidence == pytest.approx(0.4)
    assert "0.40" in decision.reason and "0.75" in decision.reason


def test_decide_accepts_duck_typed_llm_answers() -> None:
    decision = decide(signals(), SimpleNamespace(category="code", confidence="0.8"), min_confidence=0.75)
    assert decision.category == "code"
    weird = decide(signals(), SimpleNamespace(category="banana", confidence=0.99), min_confidence=0.75)
    assert weird.category == "ambiguous"


def test_policy_hint_candidates_for_the_llm() -> None:
    assert policy_hint(signals(contract_status="missing")).category == "code"
    violated = policy_hint(signals(violation_rules=["out_of_range"], violation_columns=["price"]))
    assert (violated.category, violated.rule) == ("data", RULE_LLM)
    unknown = policy_hint(signals(contract_status="missing", error_kind="unknown", repo_frames_present=False))
    assert (unknown.category, unknown.needs_llm_confirmation) == ("ambiguous", True)


# --------------------------------------------------------------------------- error kinds


@pytest.mark.parametrize(
    ("exc_type", "message", "traceback", "log", "known", "repo", "kind", "column"),
    [
        ("KeyError", "'amount'", SALES_TRACEBACK, "", (), True, "data_shape", "amount"),
        ("KeyError", "'region'", "", "", (), True, "code", None),  # a dict key, not a column
        ("KeyError", "'region'", "", "", ("region",), True, "data_shape", "region"),
        ("KeyError", "\"['qty'] not in index\"", "", "", (), True, "data_shape", "qty"),
        (
            "KeyError",
            "\"None of [Index(['sku', 'qty'], dtype='object')] are in the [columns]\"",
            "",
            "",
            (),
            True,
            "data_shape",
            "sku",
        ),
        ("ValueError", "missing column 'amount' in input", "", "", (), True, "data_shape", "amount"),
        (
            "pandas.errors.ParserError",
            "Error tokenizing data. C error: Expected 3 fields in line 5, saw 4",
            "",
            "",
            (),
            False,
            "data_shape",
            None,
        ),
        (
            "pandas.errors.EmptyDataError",
            "No columns to parse from file",
            "",
            "",
            (),
            False,
            "data_shape",
            None,
        ),
        (
            "FileNotFoundError",
            "input file not found: /opt/airflow/repo/data/incoming/sales.csv",
            "",
            "",
            (),
            True,
            "data_shape",
            None,
        ),
        (
            "FileNotFoundError",
            "[Errno 2] No such file or directory: 'config/settings.ini'",
            "",
            "",
            (),
            True,
            "code",
            None,
        ),
        (
            "FileNotFoundError",
            "[Errno 2] No such file or directory: 'config/settings.ini'",
            "",
            "",
            (),
            False,
            "unknown",
            None,
        ),
        (
            "ValueError",
            "unconverted data remains: T08:30:00Z",
            CUSTOMERS_TRACEBACK,
            "",
            (),
            True,
            "code",
            None,
        ),
        (
            "TypeError",
            "unsupported operand type(s) for +: 'int' and 'str'",
            f'  File "{REPO}/src/etl/x.py", line 3, in f\n',
            "",
            (),
            True,
            "code",
            None,
        ),
        ("ConnectionRefusedError", "[Errno 111] Connection refused", "", "", (), True, "infra", None),
        ("airflow.exceptions.AirflowTaskTimeout", "Timeout, PID: 4242", "", "", (), True, "infra", None),
        ("AirflowException", "Task received SIGTERM signal", "", "", (), True, "infra", None),
        (
            "OperationalError",
            "could not connect to server: Connection timed out",
            "",
            "",
            (),
            True,
            "infra",
            None,
        ),
        (None, None, "", "Task exited with return code Negsignal.SIGKILL", (), False, "infra", None),
        (None, None, "", "", (), False, "unknown", None),
        ("AttributeError", "'NoneType' object has no attribute 'strip'", "", "", (), False, "code", None),
        ("CustomVendorError", "boom", "", "", (), False, "unknown", None),
        ("CustomVendorError", "boom", "", "", (), True, "code", None),
    ],
)
def test_classify_error(
    exc_type: str | None,
    message: str | None,
    traceback: str,
    log: str,
    known: tuple[str, ...],
    repo: bool,
    kind: str,
    column: str | None,
) -> None:
    got_kind, got_column, _marker = classify_error(
        exc_type,
        message,
        traceback_text=traceback,
        log_text=log,
        known_columns=known,
        repo_frames_present=repo,
    )
    assert (got_kind, got_column) == (kind, column)


def test_conversion_errors_are_data_shaped_only_when_raised_by_pandas() -> None:
    raised_by_pandas = (
        f'  File "{REPO}/src/etl/sales.py", line 26, in transform_sales\n'
        f'  File "{SITE}/pandas/core/tools/numeric.py", line 232, in to_numeric\n'
        f'  File "{SITE}/pandas/_libs/lib.pyx", line 2391, in maybe_convert_numeric\n'
    )
    raised_by_repo = f'  File "{REPO}/src/etl/sales.py", line 30, in to_amount\n'
    message = 'could not convert string to float: "12,50"'
    assert classify_error("ValueError", message, traceback_text=raised_by_pandas, repo_frames_present=True)[
        0
    ] == ("data_shape")
    assert classify_error("ValueError", message, traceback_text=raised_by_repo, repo_frames_present=True)[
        0
    ] == ("code")


def test_infra_markers_in_the_log_do_not_override_a_python_exception() -> None:
    kind, _, _ = classify_error(
        "ZeroDivisionError", "division by zero", log_text="Received SIGTERM. Terminating subprocesses."
    )
    assert kind == "code"


def test_missing_column_helper() -> None:
    assert missing_column("KeyError", "'amount'", library_involved=True) == "amount"
    assert missing_column("KeyError", "'amount'") is None
    assert missing_column("KeyError", "'amount'", known_columns=["amount"]) == "amount"
    assert missing_column("ValueError", "Missing columns: ['a', 'b']") == "a"
    assert missing_column("ValueError", "column 'price' does not exist") == "price"
    assert missing_column("ValueError", "something else") is None


def test_traceback_helpers() -> None:
    files = frame_files(SALES_TRACEBACK)
    assert files[0].endswith("pandas/core/indexes/base.py")
    assert f"{REPO}/src/etl/sales.py" in files
    assert last_exception(SALES_TRACEBACK) == ("KeyError", "'amount'")
    assert last_exception("nothing here") == (None, None)
    assert last_exception("pandas.errors.EmptyDataError: No columns to parse from file") == (
        "pandas.errors.EmptyDataError",
        "No columns to parse from file",
    )


# --------------------------------------------------------------------------- compute_signals


def test_compute_signals_parses_the_exception_from_the_text_when_missing() -> None:
    ctx = sales_context()
    ctx.exception_type = None
    ctx.exception_message = None
    ctx.traceback_text = ""
    sig = compute_signals(ctx)
    assert sig.exception_type == "KeyError"
    assert sig.exception_message == "'amount'"
    assert sig.error_kind == "data_shape"


def test_compute_signals_masks_the_exception_message() -> None:
    ctx = customers_context()
    ctx.exception_message = "bad customer olivia.martin@example.com phone 2025550101 " + "x" * 400
    sig = compute_signals(ctx)
    assert "olivia.martin@example.com" not in (sig.exception_message or "")
    assert "2025550101" not in (sig.exception_message or "")
    assert len(sig.exception_message or "") <= 300


def test_compute_signals_uses_contract_columns_to_recognise_keyerrors() -> None:
    contract = SimpleNamespace(datasets=[SimpleNamespace(columns=[SimpleNamespace(name="region")])])
    ctx = FakeContext(
        exception_type="KeyError",
        exception_message="'region'",
        repo_frames=[{"file": "src/etl/x.py", "line": 1, "function": "f"}],
        contract=contract,
        contract_status="approved",
    )
    sig = compute_signals(ctx)
    assert (sig.error_kind, sig.missing_column_in_error) == ("data_shape", "region")


def test_compute_signals_accepts_mappings_and_dict_violations() -> None:
    ctx = {
        "contract_status": "approved",
        "violations": [{"rule": "nulls_not_allowed", "column": "amount", "dataset": "sales", "message": "m"}],
        "exception_type": "TypeError",
        "exception_message": "unsupported operand type(s) for +: 'float' and 'NoneType'",
        "repo_frames": [{"file": "src/etl/sales.py", "line": 3, "function": "f"}],
    }
    sig = compute_signals(ctx)
    assert sig.violation_rules == ["nulls_not_allowed"]
    assert sig.violation_columns == ["amount"]
    assert sig.error_kind == "code"
    assert decide(sig, llm("data", 0.9), min_confidence=0.75).category == "data"


def test_compute_signals_defaults_for_an_empty_context() -> None:
    sig = compute_signals(SimpleNamespace())
    assert sig.contract_status == "missing"
    assert sig.error_kind == "unknown"
    assert sig.repo_frames_present is False
    assert sig.top_repo_frame is None
    assert decide(sig, None, min_confidence=0.75).category == "ambiguous"
    assert sig.to_dict()["contract_status"] == "missing"
