"""LLM providers: the Anthropic (Claude) provider and the provider interface.

Two operations are needed by the agent:

* :meth:`LLMProvider.classify` - one structured-output call that returns a
  :class:`Classification` (``client.messages.parse(..., output_format=Classification)``);
* :meth:`LLMProvider.repair_session` - an agentic tool loop (``client.beta.messages.tool_runner``)
  in which Claude reads the cloned repository, writes a failing test or a fix and runs pytest
  through the plain Python functions it is given.

:func:`get_provider` picks the implementation: ``$HEALER_LLM_PROVIDER`` overrides
``llm.provider``; ``"fake"`` returns the offline :class:`dag_healer.agent.fake.FakeProvider`
used by CI and the keyless demo.

Request shape for Claude Opus 5.5 (``claude-opus-5-5``): no ``thinking`` parameter (thinking is
adaptive and cannot be disabled), ``output_config.effort`` always sent (the model default is
``medium``), no forced ``tool_choice``, no assistant prefill, no sampling parameters, and the
stable system prompt sent as a text block with ``cache_control``. With ``llm.fallbacks`` the
requests go through ``client.beta.messages`` with the server-side fallback beta
(``fallbacks="default"``), so a request declined by a safety classifier is retried server-side
on the model Anthropic recommends for that refusal category.
"""

from __future__ import annotations

import logging
import os
from collections.abc import Callable, Iterator, Mapping
from contextlib import contextmanager
from typing import Any, Literal, Protocol, runtime_checkable

from pydantic import BaseModel, Field, field_validator

from dag_healer.agent.budget import TokenBudget, usage_tokens
from dag_healer.masking import mask_text

logger = logging.getLogger(__name__)

PROVIDER_ENV = "HEALER_LLM_PROVIDER"
DEFAULT_MODEL = "claude-opus-5-5"
FALLBACK_BETA = "server-side-fallback-2026-07-01"
EFFORT_LEVELS: tuple[str, ...] = ("low", "medium", "high", "xhigh", "max")
MAX_RUNNER_MESSAGES = 40
"""Maximum number of assistant messages of one repair session."""

OUTPUT_EXCERPT_CHARS = 2000
INPUT_VALUE_CHARS = 4000
_CREDENTIAL_ENVS = ("ANTHROPIC_API_KEY", "ANTHROPIC_AUTH_TOKEN")


# --------------------------------------------------------------------------- models


class Classification(BaseModel):
    """Structured classification of one failed task, as returned by the LLM."""

    category: Literal["data", "code", "ambiguous"] = Field(
        description=(
            "data: the input data broke its contract and the upstream must fix it; "
            "code: a defect in the repository code that a minimal code change fixes; "
            "ambiguous: the evidence does not settle it and a human must decide."
        )
    )
    confidence: float = Field(ge=0.0, le=1.0, description="Confidence in the category, from 0 to 1.")
    diagnosis: str = Field(
        description="Two to five sentences for the on-call data engineer: what failed, why, what to do."
    )
    violated_rule: str | None = Field(
        default=None,
        description="For data failures: the violated contract rule, e.g. 'missing_column: amount'.",
    )
    suspected_file: str | None = Field(
        default=None, description="Repository-relative path of the file that most likely holds the defect."
    )
    suspected_symbol: str | None = Field(
        default=None, description="Function or class in suspected_file that most likely holds the defect."
    )
    evidence: list[str] = Field(
        default_factory=list, description="Short facts from the incident context that support the category."
    )
    fix_summary: str | None = Field(
        default=None, description="For code failures: one sentence describing the minimal fix."
    )

    @field_validator("confidence", mode="before")
    @classmethod
    def _clamp_confidence(cls, value: Any) -> Any:
        """Clamp numeric confidences into ``[0, 1]`` instead of rejecting the whole answer."""
        if isinstance(value, bool):
            return value
        try:
            number = float(value)
        except (TypeError, ValueError):
            return value
        if number != number:  # NaN
            return 0.0
        return min(1.0, max(0.0, number))


class LLMUnavailable(RuntimeError):
    """The LLM could not be used (no credentials, network or API error, truncated answer...).

    ``partial`` carries what a repair session produced before the failure, when known.
    """

    def __init__(self, message: str, *, partial: RepairSessionResult | None = None) -> None:
        super().__init__(message)
        self.partial = partial


class LLMRefusal(RuntimeError):
    """The model declined the request (``stop_reason == "refusal"``)."""

    def __init__(self, message: str, *, category: str | None = None, explanation: str | None = None) -> None:
        super().__init__(message)
        self.category = category
        self.explanation = explanation


class ToolCallRecord(BaseModel):
    """One tool call made during a repair session (inputs truncated, output masked)."""

    name: str
    input: dict[str, Any] = Field(default_factory=dict)
    output_excerpt: str = ""
    is_error: bool = False


class RepairSessionResult(BaseModel):
    """Outcome of one agentic repair session."""

    final_text: str = ""
    tool_calls: list[ToolCallRecord] = Field(default_factory=list)
    input_tokens: int = 0
    output_tokens: int = 0
    stop_reason: str | None = None
    budget_exhausted: bool = False

    @property
    def tokens_used(self) -> int:
        """Input plus output tokens of the session."""
        return self.input_tokens + self.output_tokens


@runtime_checkable
class LLMProvider(Protocol):
    """What the agent needs from a language model."""

    name: str

    def classify(self, *, system: str, prompt: str) -> tuple[Classification, int]:
        """Classify an incident; returns the classification and the tokens used."""
        ...

    def repair_session(
        self,
        *,
        system: str,
        prompt: str,
        tools: list[Callable[..., str]],
        max_tokens_budget: int,
        scenario: dict[str, Any],
    ) -> RepairSessionResult:
        """Run a tool-using session with the given plain Python tool functions."""
        ...


# --------------------------------------------------------------------------- helpers


def excerpt(text: Any, limit: int = OUTPUT_EXCERPT_CHARS) -> str:
    """Mask free text with :func:`dag_healer.masking.mask_text` and cut it to about ``limit`` chars.

    Long texts keep their beginning and their end (where pytest prints its summary).
    """
    masked = mask_text(text)
    if len(masked) <= limit:
        return masked
    marker = "\n...[truncated]...\n"
    head = max(0, (limit - len(marker)) // 3)
    tail = max(0, limit - len(marker) - head)
    return masked[:head] + marker + (masked[-tail:] if tail else "")


def _truncate_value(value: Any, limit: int = INPUT_VALUE_CHARS) -> Any:
    if isinstance(value, str):
        return value if len(value) <= limit else value[:limit] + "...[truncated]"
    if isinstance(value, Mapping):
        return {str(key): _truncate_value(item, limit) for key, item in value.items()}
    if isinstance(value, (list, tuple)):
        return [_truncate_value(item, limit) for item in value]
    if value is None or isinstance(value, (bool, int, float)):
        return value
    return _truncate_value(str(value), limit)


def tool_call_record(name: str, tool_input: Any, output: Any, *, is_error: bool) -> ToolCallRecord:
    """Build a :class:`ToolCallRecord` (long input strings truncated, output masked and truncated)."""
    data = _truncate_value(tool_input) if isinstance(tool_input, Mapping) else {}
    return ToolCallRecord(
        name=str(name),
        input=data,
        output_excerpt=excerpt("" if output is None else output),
        is_error=bool(is_error),
    )


def _get(obj: Any, name: str) -> Any:
    if isinstance(obj, Mapping):
        return obj.get(name)
    return getattr(obj, name, None)


def _content_text(content: Any) -> str:
    """Text of a ``tool_result`` content (a string or a list of content blocks)."""
    if content is None:
        return ""
    if isinstance(content, str):
        return content
    parts: list[str] = []
    for block in content if isinstance(content, (list, tuple)) else [content]:
        if _get(block, "type") == "text":
            parts.append(str(_get(block, "text") or ""))
    return "\n".join(parts)


def _message_text(message: Any) -> str:
    """Concatenated text blocks of an assistant message."""
    parts = [
        str(_get(block, "text") or "")
        for block in (_get(message, "content") or [])
        if _get(block, "type") == "text"
    ]
    return "\n".join(part for part in parts if part).strip()


def _log_fallbacks(message: Any) -> None:
    """Log server-side fallback hops (``fallback`` content blocks / ``fallback_message`` usage)."""
    for block in _get(message, "content") or []:
        if _get(block, "type") != "fallback":
            continue
        source = _get(_get(block, "from_") or _get(block, "from"), "model")
        target = _get(_get(block, "to"), "model")
        logger.info("Server-side fallback: %s declined, %s continued", source or "?", target or "?")
    usage = _get(message, "usage")
    iterations = _get(usage, "iterations") if usage is not None else None
    if iterations and any(_get(entry, "type") == "fallback_message" for entry in iterations):
        logger.info("Response served by fallback model %s", _get(message, "model") or "?")


def _has_credentials() -> bool:
    return any(os.environ.get(name) for name in _CREDENTIAL_ENVS)


@contextmanager
def _api_errors(action: str) -> Iterator[None]:
    """Translate Anthropic SDK errors into :class:`LLMUnavailable` (most specific first)."""
    import anthropic

    try:
        yield
    except (LLMUnavailable, LLMRefusal):
        raise
    except anthropic.AuthenticationError as exc:
        raise LLMUnavailable(
            f"Anthropic rejected the credentials during {action}: check ANTHROPIC_API_KEY"
        ) from exc
    except anthropic.PermissionDeniedError as exc:
        raise LLMUnavailable(f"The Anthropic API key is not allowed to run {action} (403)") from exc
    except anthropic.NotFoundError as exc:
        raise LLMUnavailable(f"Anthropic returned 404 during {action}: check llm.model") from exc
    except anthropic.RateLimitError as exc:
        raise LLMUnavailable(f"Anthropic rate limit reached during {action} (429); retry later") from exc
    except anthropic.APIStatusError as exc:
        detail = excerpt(getattr(exc, "message", "") or "", 300)
        raise LLMUnavailable(f"Anthropic API error {exc.status_code} during {action}: {detail}") from exc
    except anthropic.APIConnectionError as exc:
        raise LLMUnavailable(f"Cannot reach the Anthropic API during {action}: {type(exc).__name__}") from exc
    except ValueError as exc:
        if "Streaming is required" not in str(exc):
            raise
        # The SDK refuses non-streaming requests whose max_tokens could exceed its 10-minute timeout.
        raise LLMUnavailable(
            f"llm.max_tokens is too large for a non-streaming request during {action}; lower it "
            "(about 21000 at most)"
        ) from exc


# --------------------------------------------------------------------------- Anthropic


class AnthropicProvider:
    """Claude through the official ``anthropic`` SDK (1.x).

    The client is created lazily on the first call; without ``ANTHROPIC_API_KEY`` (or
    ``ANTHROPIC_AUTH_TOKEN``) every call raises :class:`LLMUnavailable` with a clear message.
    Pass ``client`` to inject a pre-built ``anthropic.Anthropic`` (tests use one built on an
    ``httpx2.MockTransport``).
    """

    name = "anthropic"

    def __init__(
        self,
        cfg: Any = None,
        *,
        client: Any = None,
        model: str | None = None,
        effort: str | None = None,
        max_tokens: int | None = None,
        fallbacks: bool | None = None,
    ) -> None:
        llm = getattr(cfg, "llm", None)
        self.model = model or getattr(llm, "model", None) or DEFAULT_MODEL
        self.effort = (effort or getattr(llm, "effort", None) or "high").strip()
        self.max_tokens = int(max_tokens or getattr(llm, "max_tokens", None) or 16000)
        self.fallbacks = bool(getattr(llm, "fallbacks", True) if fallbacks is None else fallbacks)
        self._client = client
        if self.effort not in EFFORT_LEVELS:
            logger.warning(
                "llm.effort %r is not one of %s; sending it as configured", self.effort, EFFORT_LEVELS
            )

    # ------------------------------------------------------------------ plumbing
    def _get_client(self) -> Any:
        if self._client is not None:
            return self._client
        if not _has_credentials():
            raise LLMUnavailable(
                "The Anthropic provider needs ANTHROPIC_API_KEY in the environment of the dag-healer "
                "worker. Set it, or use the offline provider (llm.provider: fake or "
                "HEALER_LLM_PROVIDER=fake)."
            )
        try:
            import anthropic
        except ImportError as exc:  # pragma: no cover - anthropic is a hard dependency
            raise LLMUnavailable("The 'anthropic' package is not installed") from exc
        self._client = anthropic.Anthropic()
        return self._client

    def _request_kwargs(self, system: str, prompt: str) -> dict[str, Any]:
        kwargs: dict[str, Any] = {
            "model": self.model,
            "max_tokens": self.max_tokens,
            "system": [{"type": "text", "text": system, "cache_control": {"type": "ephemeral"}}],
            "messages": [{"role": "user", "content": prompt}],
            "output_config": {"effort": self.effort},
        }
        if self.fallbacks:
            kwargs["betas"] = [FALLBACK_BETA]
            kwargs["fallbacks"] = "default"
        return kwargs

    def _raise_for_stop(self, stop_reason: Any, stop_details: Any, action: str) -> None:
        if stop_reason == "refusal":
            category = _get(stop_details, "category") if stop_details is not None else None
            explanation = _get(stop_details, "explanation") if stop_details is not None else None
            raise LLMRefusal(
                f"Claude declined {action} (refusal category: {category or 'unspecified'})",
                category=category,
                explanation=explanation,
            )
        if stop_reason == "max_tokens":
            raise LLMUnavailable(
                f"Claude's answer for {action} was cut at max_tokens={self.max_tokens}; raise llm.max_tokens"
            )
        if stop_reason == "model_context_window_exceeded":
            raise LLMUnavailable(f"The prompt for {action} exceeded the model context window")

    # ------------------------------------------------------------------ classify
    def classify(self, *, system: str, prompt: str) -> tuple[Classification, int]:
        """One structured-output call; returns the parsed classification and the tokens used."""
        import pydantic

        client = self._get_client()
        kwargs = self._request_kwargs(system, prompt)
        action = "the incident classification"
        with _api_errors(action):
            if self.fallbacks:
                # The raw response keeps stop_reason/stop_details readable when the SDK cannot
                # parse a truncated or refused answer into the schema.
                raw = client.beta.messages.with_raw_response.parse(output_format=Classification, **kwargs)
                try:
                    message = raw.parse()
                except pydantic.ValidationError as exc:
                    body = raw.json()
                    body = body if isinstance(body, dict) else {}
                    self._raise_for_stop(body.get("stop_reason"), body.get("stop_details"), action)
                    raise LLMUnavailable("Claude's classification did not match the expected schema") from exc
            else:
                try:
                    message = client.messages.parse(output_format=Classification, **kwargs)
                except pydantic.ValidationError as exc:
                    raise LLMUnavailable(
                        "Claude's classification did not match the expected schema (truncated or refused)"
                    ) from exc

        input_tokens, output_tokens = usage_tokens(getattr(message, "usage", None))
        _log_fallbacks(message)
        self._raise_for_stop(
            getattr(message, "stop_reason", None), getattr(message, "stop_details", None), action
        )
        parsed = getattr(message, "parsed_output", None)
        if parsed is None:
            raise LLMUnavailable("Claude returned no structured classification")
        if not isinstance(parsed, Classification):
            parsed = Classification.model_validate(parsed)
        logger.debug(
            "Claude classification: %s (confidence %.2f, %d tokens)",
            parsed.category,
            parsed.confidence,
            input_tokens + output_tokens,
        )
        return parsed, input_tokens + output_tokens

    # ------------------------------------------------------------------ repair
    def repair_session(
        self,
        *,
        system: str,
        prompt: str,
        tools: list[Callable[..., str]],
        max_tokens_budget: int,
        scenario: dict[str, Any],
    ) -> RepairSessionResult:
        """Drive ``client.beta.messages.tool_runner`` with the given tool functions.

        The token budget is checked after every assistant message (inside the
        ``for message in runner`` loop): once it is exhausted, or after
        :data:`MAX_RUNNER_MESSAGES` messages, the loop stops before running further tools.
        ``pause_turn`` turns are resumed by the runner itself. Every tool call is recorded.
        Raises :class:`LLMUnavailable` (with ``partial``) on API errors.
        """
        from anthropic import beta_tool

        client = self._get_client()
        runnable = [fn if hasattr(fn, "to_dict") and hasattr(fn, "call") else beta_tool(fn) for fn in tools]
        budget = TokenBudget(max(0, int(max_tokens_budget)))
        result = RepairSessionResult()
        phase = scenario.get("phase") if isinstance(scenario, Mapping) else None
        action = f"the repair session ({phase or 'repair'})"
        logger.info(
            "Starting Claude repair session (%s) for %s.%s with %d tools, budget %d tokens",
            phase or "repair",
            scenario.get("dag_id") if isinstance(scenario, Mapping) else "?",
            scenario.get("task_id") if isinstance(scenario, Mapping) else "?",
            len(runnable),
            budget.limit,
        )
        try:
            with _api_errors(action):
                runner = client.beta.messages.tool_runner(
                    tools=runnable, max_iterations=MAX_RUNNER_MESSAGES, **self._request_kwargs(system, prompt)
                )
                try:
                    self._drive(runner, budget, result)
                finally:
                    runner.close()
        except LLMUnavailable as exc:
            result.input_tokens, result.output_tokens = budget.input_tokens, budget.output_tokens
            exc.partial = result
            raise
        result.input_tokens, result.output_tokens = budget.input_tokens, budget.output_tokens
        logger.info(
            "Claude repair session finished: stop_reason=%s, %d tool calls, %d tokens%s",
            result.stop_reason,
            len(result.tool_calls),
            result.tokens_used,
            " (budget exhausted)" if result.budget_exhausted else "",
        )
        return result

    @staticmethod
    def _drive(runner: Any, budget: TokenBudget, result: RepairSessionResult) -> None:
        count = 0
        for message in runner:
            count += 1
            budget.add_usage(getattr(message, "usage", None))
            result.stop_reason = getattr(message, "stop_reason", None)
            _log_fallbacks(message)
            text = _message_text(message)
            if text:
                result.final_text = text
            if result.stop_reason == "refusal":
                details = getattr(message, "stop_details", None)
                logger.warning(
                    "Claude declined the repair session (refusal category: %s)",
                    _get(details, "category") if details is not None else None,
                )
                break
            if budget.exhausted:
                result.budget_exhausted = True
                logger.warning(
                    "Token budget exhausted (%d >= %d); stopping the repair session",
                    budget.used,
                    budget.limit,
                )
                break
            if count >= MAX_RUNNER_MESSAGES:
                logger.warning("Repair session reached %d messages; stopping", MAX_RUNNER_MESSAGES)
                break
            if result.stop_reason == "tool_use":
                # Cached by the runner: the tools run once, here, and the runner reuses the results.
                response = runner.generate_tool_call_response()
                result.tool_calls.extend(_records_for(message, response))


def _records_for(message: Any, response: Any) -> list[ToolCallRecord]:
    results: dict[str, Any] = {}
    for block in (_get(response, "content") or []) if response is not None else []:
        if _get(block, "type") == "tool_result":
            results[str(_get(block, "tool_use_id"))] = block
    records: list[ToolCallRecord] = []
    for block in _get(message, "content") or []:
        if _get(block, "type") != "tool_use":
            continue
        outcome = results.get(str(_get(block, "id")))
        if outcome is None:
            name = _get(block, "name") or "?"
            records.append(tool_call_record(name, _get(block, "input"), "(not executed)", is_error=True))
            continue
        records.append(
            tool_call_record(
                _get(block, "name") or "?",
                _get(block, "input"),
                _content_text(_get(outcome, "content")),
                is_error=bool(_get(outcome, "is_error")),
            )
        )
    return records


# --------------------------------------------------------------------------- factory


def get_provider(cfg: Any) -> LLMProvider:
    """Return the configured provider (``$HEALER_LLM_PROVIDER`` overrides ``llm.provider``).

    ``"anthropic"`` never fails here: a missing ``ANTHROPIC_API_KEY`` surfaces as
    :class:`LLMUnavailable` when the provider is first called. Unknown names raise
    :class:`ValueError`.
    """
    configured = getattr(getattr(cfg, "llm", None), "provider", None)
    name = (os.environ.get(PROVIDER_ENV) or configured or "anthropic").strip().lower()
    if name == "anthropic":
        return AnthropicProvider(cfg)
    if name == "fake":
        from dag_healer.agent.fake import FakeProvider

        return FakeProvider(cfg)
    raise ValueError(f"Unknown LLM provider {name!r}: expected 'anthropic' or 'fake'")


__all__ = [
    "DEFAULT_MODEL",
    "EFFORT_LEVELS",
    "FALLBACK_BETA",
    "MAX_RUNNER_MESSAGES",
    "PROVIDER_ENV",
    "AnthropicProvider",
    "Classification",
    "LLMProvider",
    "LLMRefusal",
    "LLMUnavailable",
    "RepairSessionResult",
    "ToolCallRecord",
    "excerpt",
    "get_provider",
    "tool_call_record",
]
