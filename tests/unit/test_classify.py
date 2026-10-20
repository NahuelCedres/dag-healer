"""Unit tests for dag_healer.agent.classify (policy + LLM, with the fake and failing providers)."""

from __future__ import annotations

import json
from dataclasses import dataclass, field
from typing import Any

import pytest

from dag_healer.agent.classify import (
    ClassificationOutcome,
    build_classify_prompt,
    classify_incident,
    context_payload,
)
from dag_healer.agent.fake import FakeProvider, extract_signals
from dag_healer.agent.llm import Classification, LLMRefusal, LLMUnavailable
from dag_healer.agent.policy import RULE_A, RULE_B, RULE_C, RULE_D, compute_signals, policy_hint
from dag_healer.agent.prompts import load_prompt
from dag_healer.config import HealerConfig
from dag_healer.contracts.schema import Violation

try:  # the real context dataclass, when the context piece is installed
    from dag_healer.agent.context import IncidentContext
except ImportError:  # pragma: no cover - duck-typed stand-in with the same attribute names
    IncidentContext = None  # type: ignore[assignment,misc]

SITE = "/home/airflow/.local/lib/python3.13/site-packages"
REPO = "/opt/airflow/repo"


@dataclass
class _StandInContext:
    incident_id: str
    dag_id: str
    run_id: str
    task_id: str
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

    def to_prompt_dict(self) -> dict[str, Any]:
        return {
            "incident_id": self.incident_id,
            "dag_id": self.dag_id,
            "task_id": self.task_id,
            "error_text": self.error_text,
            "contract_status": self.contract_status,
            "violations": [v.model_dump(mode="json") for v in self.violations],
        }


def make_context(**kwargs: Any) -> Any:
    base: dict[str, Any] = {
        "incident_id": "0123456789abcdef0123456789abcdef",
        "dag_id": "some_dag",
        "run_id": "manual__2026-10-17T10:00:00+00:00",
        "task_id": "some_task",
    }
    base.update(kwargs)
    cls = IncidentContext if IncidentContext is not None else _StandInContext
    return cls(**base)


def sales() -> Any:
    traceback = (
        "Traceback (most recent call last):\n"
        f'  File "{REPO}/src/etl/sales.py", line 26, in transform_sales\n'
        '    df["amount"] = pd.to_numeric(df["amount"], errors="coerce")\n'
        f'  File "{SITE}/pandas/core/frame.py", line 4378, in __getitem__\n'
        f'  File "{SITE}/pandas/core/indexes/base.py", line 3648, in get_loc\n'
        "KeyError: 'amount'"
    )
    return make_context(
        dag_id="sales_daily_etl",
        task_id="transform_sales",
        error_text=traceback,
        exception_type="KeyError",
        exception_message="'amount'",
        traceback_text=traceback,
        repo_frames=[{"file": "src/etl/sales.py", "line": 26, "function": "transform_sales"}],
        contract_status="approved",
        violations=[
            Violation(
                rule="unexpected_column", dataset="sales", column="amt", message="unexpected column 'amt'"
            ),
            Violation(
                rule="missing_column", dataset="sales", column="amount", message="column 'amount' is missing"
            ),
        ],
        masked_sample={"dataset": "sales", "columns": ["order_id", "amt"], "rows": [], "masked": True},
    )


def customers() -> Any:
    traceback = (
        "Traceback (most recent call last):\n"
        f'  File "{REPO}/src/etl/customers.py", line 36, in transform_customers\n'
        f'  File "{SITE}/pandas/core/series.py", line 5084, in apply\n'
        '  File "pandas/_libs/lib.pyx", line 3071, in pandas._libs.lib.map_infer\n'
        f'  File "{REPO}/src/etl/customers.py", line 19, in parse_signup_date\n'
        '  File "/usr/local/lib/python3.13/_strptime.py", line 558, in _strptime\n'
        "ValueError: unconverted data remains: T08:30:00Z"
    )
    return make_context(
        dag_id="customers_sync",
        task_id="transform",
        error_text=traceback,
        exception_type="ValueError",
        exception_message="unconverted data remains: T08:30:00Z",
        traceback_text=traceback,
        repo_frames=[
            {"file": "src/etl/customers.py", "line": 36, "function": "transform_customers"},
            {"file": "src/etl/customers.py", "line": 19, "function": "parse_signup_date"},
        ],
        code_snippets={
            "src/etl/customers.py": 'return datetime.strptime(value.strip(), "%Y-%m-%d").date()\n'
        },
        contract_status="approved",
    )


def inventory() -> Any:
    traceback = (
        "Traceback (most recent call last):\n"
        f'  File "{REPO}/src/etl/inventory.py", line 29, in buildReport\n'
        f'  File "{SITE}/pandas/core/frame.py", line 4378, in __getitem__\n'
        "KeyError: 'qty'"
    )
    return make_context(
        dag_id="inventory_report",
        task_id="build_report",
        error_text=traceback,
        exception_type="KeyError",
        exception_message="'qty'",
        traceback_text=traceback,
        repo_frames=[{"file": "src/etl/inventory.py", "line": 29, "function": "buildReport"}],
        contract_status="draft",
        draft_violations=[
            Violation(
                rule="missing_column", dataset="inventory", column="qty", message="column 'qty' is missing"
            )
        ],
    )


@pytest.fixture
def cfg(tmp_path: Any) -> HealerConfig:
    config = HealerConfig()
    config.repo.path = tmp_path
    return config


class RecordingProvider:
    """Provider returning a fixed answer (or raising) and recording the calls."""

    name = "recording"

    def __init__(self, answer: Any = None, error: BaseException | None = None, tokens: int = 123) -> None:
        self.answer = answer
        self.error = error
        self.tokens = tokens
        self.calls: list[dict[str, str]] = []

    def classify(self, *, system: str, prompt: str) -> tuple[Any, int]:
        self.calls.append({"system": system, "prompt": prompt})
        if self.error is not None:
            raise self.error
        return self.answer, self.tokens

    def repair_session(self, **_: Any) -> Any:  # pragma: no cover - not used here
        raise NotImplementedError


# --------------------------------------------------------------------------- demo scenarios with the fake LLM


@pytest.mark.parametrize(
    ("factory", "category", "rule"),
    [(sales, "data", RULE_A), (customers, "code", RULE_B), (inventory, "ambiguous", RULE_C)],
)
def test_demo_scenarios_with_the_fake_provider(
    cfg: HealerConfig, factory: Any, category: str, rule: str
) -> None:
    outcome = classify_incident(cfg, factory(), FakeProvider(cfg))
    assert isinstance(outcome, ClassificationOutcome)
    assert (outcome.category, outcome.decision.rule) == (category, rule)
    assert outcome.classification is not None
    assert outcome.classification.category == category
    assert outcome.tokens_used == 0
    assert outcome.llm_error is None
    assert outcome.diagnosis.strip()
    assert outcome.evidence
    json.dumps(outcome.to_dict())


def test_data_outcome_points_at_the_violated_rule(cfg: HealerConfig) -> None:
    outcome = classify_incident(cfg, sales(), FakeProvider(cfg))
    assert outcome.violated_rule is not None
    assert (outcome.violated_rule["rule"], outcome.violated_rule["column"]) == ("missing_column", "amount")
    assert outcome.confidence == pytest.approx(0.95)
    assert "missing_column" in outcome.diagnosis


def test_code_outcome_names_the_suspected_function(cfg: HealerConfig) -> None:
    outcome = classify_incident(cfg, customers(), FakeProvider(cfg))
    assert outcome.violated_rule is None
    assert outcome.classification is not None
    assert outcome.classification.suspected_file == "src/etl/customers.py"
    assert outcome.classification.suspected_symbol == "parse_signup_date"
    assert outcome.classification.fix_summary
    assert outcome.confidence == pytest.approx(0.9)


def test_ambiguous_outcome_has_no_violated_rule_without_an_approved_contract(cfg: HealerConfig) -> None:
    outcome = classify_incident(cfg, inventory(), FakeProvider(cfg))
    assert outcome.violated_rule is None
    assert outcome.classification is not None and outcome.classification.confidence == pytest.approx(0.5)
    assert "draft" in outcome.diagnosis


# --------------------------------------------------------------------------- prompt


def test_prompt_carries_the_context_and_a_signals_block(cfg: HealerConfig) -> None:
    provider = RecordingProvider(Classification(category="code", confidence=0.9, diagnosis="bug"))
    classify_incident(cfg, customers(), provider)
    call = provider.calls[0]
    assert call["system"] == load_prompt("classify.md")
    assert "<incident>" in call["prompt"]
    assert call["prompt"].rstrip().endswith("</signals>")
    signals = extract_signals(call["prompt"])
    assert signals["policy_hint"] == {
        "category": "code",
        "rule": RULE_B,
        "reason": signals["policy_hint"]["reason"],
        "llm_decides": True,
    }
    assert signals["min_confidence"] == pytest.approx(0.75)
    assert signals["dag_id"] == "customers_sync"
    assert signals["top_repo_frame"]["function"] == "parse_signup_date"
    incident = json.loads(call["prompt"].split("<incident>")[1].split("</incident>")[0])
    assert incident["dag_id"] == "customers_sync"


def test_build_prompt_is_json_safe_for_duck_typed_contexts() -> None:
    @dataclass
    class Bare:
        dag_id: str = "d"
        task_id: str = "t"
        exception_type: str = "KeyError"
        exception_message: str = "'x'"
        violations: list[Violation] = field(
            default_factory=lambda: [Violation(rule="missing_column", dataset="ds", column="x", message="m")]
        )
        contract_status: str = "approved"

    ctx = Bare()
    sig = compute_signals(ctx)
    prompt = build_classify_prompt(ctx, sig, policy_hint(sig), min_confidence=0.6)
    payload = context_payload(ctx)
    assert payload["violations"][0]["rule"] == "missing_column"
    assert extract_signals(prompt)["policy_hint"]["rule"] == RULE_A


def test_prompt_excerpt_is_masked_and_bounded(cfg: HealerConfig) -> None:
    ctx = customers()
    ctx.error_text = "contact olivia.martin@example.com\n" + "line\n" * 3000
    outcome = classify_incident(cfg, ctx, FakeProvider(cfg))
    assert "olivia.martin@example.com" not in outcome.prompt_excerpt
    assert len(outcome.prompt_excerpt) < 4200
    assert "</signals>" in outcome.prompt_excerpt


# --------------------------------------------------------------------------- LLM failures


@pytest.mark.parametrize(
    "error",
    [
        LLMUnavailable("The Anthropic provider needs ANTHROPIC_API_KEY"),
        LLMRefusal("Claude declined", category="cyber"),
        RuntimeError("socket exploded for user olivia.martin@example.com"),
    ],
)
def test_provider_errors_fall_back_to_the_policy(cfg: HealerConfig, error: BaseException) -> None:
    code = classify_incident(cfg, customers(), RecordingProvider(error=error))
    assert (code.category, code.decision.rule) == ("ambiguous", RULE_B)
    assert code.classification is None
    assert code.tokens_used == 0
    assert code.llm_error
    assert "olivia.martin@example.com" not in code.llm_error
    assert "LLM was unavailable" in code.diagnosis
    assert "LLM unavailable" in code.decision.reason

    data = classify_incident(cfg, sales(), RecordingProvider(error=error))
    assert (data.category, data.decision.rule) == ("data", RULE_A)
    assert data.violated_rule is not None and data.violated_rule["column"] == "amount"
    assert "LLM was unavailable" in data.diagnosis


def test_refusal_is_reported_as_declined(cfg: HealerConfig) -> None:
    outcome = classify_incident(cfg, customers(), RecordingProvider(error=LLMRefusal("Claude declined")))
    assert outcome.llm_error and "declined" in outcome.llm_error


def test_without_provider_the_policy_decides(cfg: HealerConfig) -> None:
    outcome = classify_incident(cfg, inventory(), None)
    assert (outcome.category, outcome.decision.rule) == ("ambiguous", RULE_C)
    assert outcome.llm_error == "no LLM provider is configured"
    assert outcome.diagnosis.startswith("The LLM was unavailable")


def test_low_confidence_answer_goes_to_a_human(cfg: HealerConfig) -> None:
    answer = Classification(category="code", confidence=0.6, diagnosis="maybe a bug")
    outcome = classify_incident(cfg, customers(), RecordingProvider(answer))
    assert (outcome.category, outcome.decision.rule) == ("ambiguous", RULE_D)
    assert outcome.tokens_used == 123
    assert "maybe a bug" in outcome.diagnosis
    assert "Policy decision: ambiguous" in outcome.diagnosis


def test_min_confidence_comes_from_the_config(cfg: HealerConfig) -> None:
    cfg.classification.min_confidence = 0.5
    answer = Classification(category="code", confidence=0.6, diagnosis="bug")
    outcome = classify_incident(cfg, customers(), RecordingProvider(answer))
    assert outcome.category == "code"


def test_dict_answers_are_validated(cfg: HealerConfig) -> None:
    answer = {"category": "code", "confidence": 0.95, "diagnosis": "bug in parse_signup_date"}
    outcome = classify_incident(cfg, customers(), RecordingProvider(answer))
    assert outcome.category == "code"
    assert isinstance(outcome.classification, Classification)

    broken = classify_incident(cfg, customers(), RecordingProvider({"category": "nonsense"}))
    assert broken.classification is None
    assert broken.category == "ambiguous"


def test_llm_diagnosis_and_evidence_are_masked(cfg: HealerConfig) -> None:
    answer = Classification(
        category="code",
        confidence=0.9,
        diagnosis="Row of olivia.martin@example.com has a timestamp.",
        evidence=["phone +1 202-555-0101 in row 3"],
    )
    outcome = classify_incident(cfg, customers(), RecordingProvider(answer))
    assert "olivia.martin@example.com" not in outcome.diagnosis
    assert all("202-555-0101" not in item for item in outcome.evidence)


def test_context_without_to_prompt_dict(cfg: HealerConfig) -> None:
    @dataclass
    class Minimal:
        incident_id: str = "abc"
        dag_id: str = "d"
        task_id: str = "t"
        exception_type: str = "ConnectionRefusedError"
        exception_message: str = "[Errno 111] Connection refused"

    outcome = classify_incident(cfg, Minimal(), FakeProvider(cfg))
    assert (outcome.category, outcome.decision.rule) == ("ambiguous", "infra")


@pytest.mark.skipif(IncidentContext is None, reason="dag_healer.agent.context is not available")
def test_works_with_tracebacks_parsed_by_the_context_module(cfg: HealerConfig) -> None:
    from dag_healer.agent.context import extract_traceback

    ctx = customers()
    exc_type, exc_message, frames, traceback_text = extract_traceback(ctx.error_text)
    ctx.exception_type, ctx.exception_message, ctx.traceback_text = exc_type, exc_message, traceback_text
    assert frames
    outcome = classify_incident(cfg, ctx, FakeProvider(cfg))
    assert (outcome.category, outcome.decision.rule) == ("code", RULE_B)
