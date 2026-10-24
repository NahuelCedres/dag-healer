"""Incident classification: the deterministic policy combined with the LLM.

:func:`classify_incident` computes the policy signals of an incident context (with the
orchestrator's error taxonomy), asks the LLM provider for a structured
:class:`~dag_healer.agent.llm.Classification` (the prompt is ``prompts/classify.md`` rendered
with the orchestrator's words + the masked context + a ``<signals>{json}</signals>`` block
carrying the policy hint) and lets :func:`dag_healer.agent.policy.decide` take the final
decision. When the provider is missing or fails (no API key, refusal, network error...), the
decision comes from the policy alone: LLM-dependent gates fall back to ``ambiguous`` and the
diagnosis says that the LLM was unavailable.
"""

from __future__ import annotations

import json
import logging
from collections.abc import Mapping
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any

from dag_healer.agent.context import prompt_words
from dag_healer.agent.llm import Classification, LLMProvider, LLMRefusal, LLMUnavailable
from dag_healer.agent.policy import (
    Decision,
    PolicyHint,
    Signals,
    compute_signals,
    decide,
    policy_hint,
    short_type,
)
from dag_healer.agent.prompts import load_prompt
from dag_healer.agent.tests_gen import render_template
from dag_healer.masking import mask_text, to_json_safe

if TYPE_CHECKING:
    from dag_healer.core.vocab import ErrorTaxonomy

logger = logging.getLogger(__name__)

PROMPT_FILE = "classify.md"
DEFAULT_MIN_CONFIDENCE = 0.75
PROMPT_EXCERPT_HEAD = 2500
PROMPT_EXCERPT_TAIL = 1500
_ERROR_CHARS = 300

#: Attributes of a context used when it has no ``to_prompt_dict()`` (duck-typed contexts).
_CONTEXT_FIELDS: tuple[str, ...] = (
    "incident_id",
    "dag_id",
    "run_id",
    "task_id",
    "map_index",
    "try_number",
    "error_text",
    "exception_type",
    "exception_message",
    "traceback_text",
    "repo_frames",
    "log_excerpt",
    "dag_file",
    "dag_source",
    "code_snippets",
    "contract_status",
    "violations",
    "draft_violations",
    "sample_dataset",
    "masked_sample",
    "datasets_checked",
    "notes",
)


@dataclass
class ClassificationOutcome:
    """Result of :func:`classify_incident`.

    ``classification`` is the LLM answer (``None`` when the LLM was not available).
    ``diagnosis`` is always filled: the LLM diagnosis, or a policy-only explanation.
    ``violated_rule`` is the primary violation of the approved contract (a ``Violation`` dict)
    or ``None``; ``evidence`` merges policy facts and the LLM evidence (masked).
    """

    decision: Decision
    classification: Classification | None
    tokens_used: int
    signals: Signals
    prompt_excerpt: str
    diagnosis: str = ""
    violated_rule: dict[str, Any] | None = None
    evidence: list[str] = field(default_factory=list)
    llm_error: str | None = None

    @property
    def category(self) -> str:
        """Final category (``data``, ``code`` or ``ambiguous``)."""
        return self.decision.category

    @property
    def confidence(self) -> float:
        """Confidence of the final category."""
        return self.decision.confidence

    def to_dict(self) -> dict[str, Any]:
        """JSON-safe summary (for incident events)."""
        return {
            "decision": self.decision.to_dict(),
            "classification": self.classification.model_dump(mode="json") if self.classification else None,
            "tokens_used": self.tokens_used,
            "signals": self.signals.to_dict(),
            "diagnosis": self.diagnosis,
            "violated_rule": self.violated_rule,
            "evidence": list(self.evidence),
            "llm_error": self.llm_error,
        }


# --------------------------------------------------------------------------- prompt


def _attr(obj: Any, name: str, default: Any = None) -> Any:
    if isinstance(obj, Mapping):
        return obj.get(name, default)
    return getattr(obj, name, default)


def context_payload(ctx: Any) -> dict[str, Any]:
    """JSON-safe dict of the context (``ctx.to_prompt_dict()`` when available)."""
    to_prompt = getattr(ctx, "to_prompt_dict", None)
    if callable(to_prompt):
        try:
            data = to_prompt()
            if isinstance(data, Mapping):
                return dict(to_json_safe(dict(data)))
        except Exception as exc:  # a broken context must not stop the classification
            logger.warning(
                "Incident context to_prompt_dict() failed (%s); using its attributes", type(exc).__name__
            )
    payload: dict[str, Any] = {}
    for name in _CONTEXT_FIELDS:
        value = _attr(ctx, name)
        if value is None:
            continue
        if hasattr(value, "model_dump"):
            value = value.model_dump(mode="json")
        elif isinstance(value, list):
            value = [item.model_dump(mode="json") if hasattr(item, "model_dump") else item for item in value]
        payload[name] = to_json_safe(value)
    contract = _attr(ctx, "contract")
    if contract is not None and hasattr(contract, "model_dump"):
        payload["contract"] = to_json_safe(contract.model_dump(mode="json"))
    return payload


def signals_payload(
    signals: Signals, hint: PolicyHint, *, min_confidence: float, ctx: Any = None
) -> dict[str, Any]:
    """The JSON object placed between ``<signals>`` and ``</signals>`` in the prompt."""
    data = signals.to_dict()
    data["dag_id"] = _attr(ctx, "dag_id") if ctx is not None else None
    data["task_id"] = _attr(ctx, "task_id") if ctx is not None else None
    data["min_confidence"] = min_confidence
    data["policy_hint"] = {
        "category": hint.category,
        "rule": hint.rule,
        "reason": hint.reason,
        "llm_decides": hint.needs_llm_confirmation,
    }
    return to_json_safe(data)


def build_classify_prompt(ctx: Any, signals: Signals, hint: PolicyHint, *, min_confidence: float) -> str:
    """User prompt: the masked context as JSON, then the ``<signals>`` block (always last)."""
    words = prompt_words(ctx)
    incident = json.dumps(context_payload(ctx), ensure_ascii=False, indent=1, default=str)
    block = json.dumps(
        signals_payload(signals, hint, min_confidence=min_confidence, ctx=ctx),
        ensure_ascii=False,
        indent=1,
        default=str,
    )
    return (
        f"Classify the failed {words['product']} {words['step']} described below.\n\n"
        f"<incident>\n{incident}\n</incident>\n\n"
        f"<signals>\n{block}\n</signals>\n"
    )


def system_prompt(ctx: Any = None) -> str:
    """``prompts/classify.md`` rendered with the words of ``ctx``'s orchestrator (one pass).

    Without a vocabulary (Airflow, or a context gathered offline) this is the phase-5 prompt.
    Raises :class:`FileNotFoundError` when the prompt file is missing from the installation.
    """
    return render_template(load_prompt(PROMPT_FILE), prompt_words(ctx))


def _excerpt(prompt: str) -> str:
    masked = mask_text(prompt)
    if len(masked) <= PROMPT_EXCERPT_HEAD + PROMPT_EXCERPT_TAIL:
        return masked
    return masked[:PROMPT_EXCERPT_HEAD] + "\n...[truncated]...\n" + masked[-PROMPT_EXCERPT_TAIL:]


# --------------------------------------------------------------------------- outcome pieces


def _violation_dict(violation: Any) -> dict[str, Any] | None:
    if hasattr(violation, "model_dump"):
        return dict(violation.model_dump(mode="json"))
    if isinstance(violation, Mapping):
        return dict(to_json_safe(dict(violation)))
    return None


def primary_violation(ctx: Any, signals: Signals) -> dict[str, Any] | None:
    """The approved-contract violation that best explains the failure (``None`` if there is none).

    Prefers the violation on the column named in the error, then a violated column quoted in
    the traceback, then the first violation.
    """
    if signals.contract_status != "approved":
        return None
    violations = list(_attr(ctx, "violations") or [])
    if not violations:
        return None
    wanted = [signals.missing_column_in_error, *signals.columns_in_error]
    for column in wanted:
        if not column:
            continue
        for violation in violations:
            if _attr(violation, "column") == column:
                return _violation_dict(violation)
    return _violation_dict(violations[0])


def _facts(signals: Signals) -> list[str]:
    facts = [f"policy: {signals.contract_status} contract, {signals.error_kind} error"]
    if signals.exception_type:
        error = short_type(signals.exception_type) or signals.exception_type
        message = signals.exception_message
        facts.append(f"exception: {error}: {message}" if message else f"exception: {error}")
    if signals.violation_details or signals.violation_rules:
        facts.append("violations: " + "; ".join(signals.violation_details or signals.violation_rules))
    if signals.top_repo_frame and signals.top_repo_frame.get("file"):
        frame = signals.top_repo_frame
        where = str(frame["file"]) + (f"::{frame['function']}" if frame.get("function") else "")
        facts.append(f"repository frame: {where}")
    return facts


def _policy_diagnosis(decision: Decision, signals: Signals, llm_error: str | None, *, step: str) -> str:
    parts: list[str] = []
    if llm_error:
        parts.append(
            f"The LLM was unavailable ({llm_error}), so this classification comes from the deterministic "
            "policy alone."
        )
    parts.append(decision.reason)
    if signals.exception_type:
        error = short_type(signals.exception_type) or signals.exception_type
        text = f"The {step} failed with {error}"
        if signals.exception_message:
            text += f": {signals.exception_message}"
        frame = signals.top_repo_frame or {}
        if frame.get("file"):
            function = f"::{frame['function']}" if frame.get("function") else ""
            text += f" (in {frame['file']}{function})"
        parts.append(text.rstrip(".") + ".")
    return " ".join(part.strip() for part in parts if part.strip())


def _diagnosis(
    decision: Decision,
    classification: Classification | None,
    signals: Signals,
    llm_error: str | None,
    *,
    step: str,
) -> str:
    if classification is None or not classification.diagnosis.strip():
        return _policy_diagnosis(decision, signals, llm_error, step=step)
    text = mask_text(classification.diagnosis).strip()
    if decision.category != classification.category:
        text += f"\n\nPolicy decision: {decision.category}. {decision.reason}"
    return text


# --------------------------------------------------------------------------- entry point


def _min_confidence(cfg: Any) -> float:
    value = getattr(getattr(cfg, "classification", None), "min_confidence", None)
    try:
        return float(value) if value is not None else DEFAULT_MIN_CONFIDENCE
    except (TypeError, ValueError):
        return DEFAULT_MIN_CONFIDENCE


def _configured_taxonomy(cfg: Any) -> ErrorTaxonomy | None:
    """The error taxonomy of ``cfg``'s orchestrator (``registry.get_adapter``: cached, no I/O).

    ``None`` when ``cfg`` has no ``orchestrator`` section or the adapter cannot be built.
    """
    if getattr(cfg, "orchestrator", None) is None:
        return None
    try:
        from dag_healer.orchestrators.registry import get_adapter

        return get_adapter(cfg).taxonomy
    except Exception as exc:  # no taxonomy: the generic policy decides
        logger.debug("No orchestrator error taxonomy for the classification: %s", type(exc).__name__)
        return None


def classify_incident(
    cfg: Any, ctx: Any, provider: LLMProvider | None, *, taxonomy: ErrorTaxonomy | None = None
) -> ClassificationOutcome:
    """Classify one incident context; never raises because of the LLM provider.

    ``taxonomy`` is the orchestrator's :class:`~dag_healer.core.vocab.ErrorTaxonomy` (the worker
    passes its adapter's); ``None`` reads the configured orchestrator's taxonomy from ``cfg``.
    """
    min_confidence = _min_confidence(cfg)
    words = prompt_words(ctx)
    signals = compute_signals(ctx, taxonomy=taxonomy if taxonomy is not None else _configured_taxonomy(cfg))
    hint = policy_hint(signals, step=words["step"])
    prompt = build_classify_prompt(ctx, signals, hint, min_confidence=min_confidence)
    classification: Classification | None = None
    tokens_used = 0
    llm_error: str | None = None

    system: str | None = None
    if provider is None:
        llm_error = "no LLM provider is configured"
    else:
        try:
            system = system_prompt(ctx)
        except FileNotFoundError:
            llm_error = f"the {PROMPT_FILE} prompt is missing from the dag-healer installation"
            logger.error("Prompt %s not found in dag_healer.agent.prompts", PROMPT_FILE)
    if provider is not None and system is not None:
        try:
            result, tokens_used = provider.classify(system=system, prompt=prompt)
            classification = (
                result if isinstance(result, Classification) else Classification.model_validate(result)
            )
        except LLMRefusal as exc:
            llm_error = f"the model declined the request: {exc}"
        except LLMUnavailable as exc:
            llm_error = str(exc)
        except Exception as exc:  # any provider failure falls back to the policy
            llm_error = f"the {getattr(provider, 'name', 'LLM')} provider failed with {type(exc).__name__}"
            logger.warning(
                "LLM classification failed: %s: %s", type(exc).__name__, mask_text(str(exc))[:_ERROR_CHARS]
            )
        if llm_error:
            llm_error = mask_text(llm_error)[:_ERROR_CHARS]
            logger.warning("LLM classification unavailable, using the policy alone: %s", llm_error)
        tokens_used = int(tokens_used or 0)

    decision = decide(signals, classification, min_confidence=min_confidence, step=words["step"])
    diagnosis = _diagnosis(decision, classification, signals, llm_error, step=words["step"])
    if llm_error and decision.needs_llm_confirmation:
        decision.reason = f"{decision.reason} (LLM unavailable: {llm_error})"

    evidence = _facts(signals)
    if classification is not None:
        for item in classification.evidence:
            text = mask_text(item).strip()
            if text and text not in evidence:
                evidence.append(text)

    outcome = ClassificationOutcome(
        decision=decision,
        classification=classification,
        tokens_used=tokens_used,
        signals=signals,
        prompt_excerpt=_excerpt(prompt),
        diagnosis=diagnosis,
        violated_rule=primary_violation(ctx, signals),
        evidence=evidence,
        llm_error=llm_error,
    )
    logger.info(
        "Incident %s classified as %s (rule %s, confidence %.2f, %d tokens)",
        _attr(ctx, "incident_id") or "?",
        decision.category,
        decision.rule,
        decision.confidence,
        tokens_used,
    )
    return outcome


__all__ = [
    "ClassificationOutcome",
    "build_classify_prompt",
    "classify_incident",
    "context_payload",
    "primary_violation",
    "signals_payload",
    "system_prompt",
]
