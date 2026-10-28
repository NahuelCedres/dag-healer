"""LLM providers: the Anthropic (Claude) provider and the provider interface.

Two operations are needed by the agent:

* :meth:`LLMProvider.classify` - one structured-output call that returns a
  :class:`Classification` (``client.messages.parse(..., output_format=Classification)``);
* :meth:`LLMProvider.repair_session` - an agentic tool loop (``client.beta.messages.tool_runner``)
  in which Claude reads the cloned repository, writes a failing test or a fix and runs pytest
  through the plain Python functions it is given.

:func:`get_provider` picks the implementation: ``$HEALER_LLM_PROVIDER`` overrides
``llm.provider``; ``"bedrock"`` returns :class:`dag_healer.agent.bedrock.BedrockProvider` and
``"fake"`` the offline :class:`dag_healer.agent.fake.FakeProvider` used by CI and the keyless
demo.

Request shape for Claude Opus 5.5 (``claude-opus-5-5``): no ``thinking`` parameter (thinking is
adaptive and cannot be disabled), ``output_config.effort`` always sent (the model default is
``medium``), no forced ``tool_choice``, no assistant prefill, no sampling parameters, and the
stable system prompt sent as a text block with ``cache_control``. With ``llm.fallbacks`` the
requests go through ``client.beta.messages`` with the server-side fallback beta
(``fallbacks="default"``), so a request declined by a safety classifier is retried server-side
on the model Anthropic recommends for that refusal category.

The ``bedrock`` provider runs Claude on Amazon Bedrock with the same prompts, tools and repair
flow, through the SDK's ``anthropic.AnthropicBedrock`` client (the ``bedrock-runtime``
endpoint, AWS credentials from the default AWS chain). It subclasses
:class:`AnthropicProvider`, never sends the server-side fallbacks (Bedrock has none) and asks for
a JSON answer when the endpoint does not apply structured outputs.

Both providers name a refusal's category, in its error and in the warning of a repair, only when
it is shaped like one of Claude's and holds none of their credentials (:func:`_shown_category`).
"""

from __future__ import annotations

import inspect
import logging
import os
import re
from collections.abc import Callable, Iterator, Mapping
from contextlib import AbstractContextManager, contextmanager
from typing import Any, Literal, Protocol, runtime_checkable
from urllib.parse import unquote

from pydantic import BaseModel, Field, field_validator

from dag_healer.agent.budget import TokenBudget, usage_tokens
from dag_healer.masking import hide_secrets, holds_secret, known_secrets, mask_text

logger = logging.getLogger(__name__)

PROVIDER_ENV = "HEALER_LLM_PROVIDER"
#: The providers :func:`get_provider` builds: the values of ``llm.provider`` (``config.LlmProvider``)
#: and of ``dag-healer worker --provider``, which tests keep equal to this list.
PROVIDER_NAMES: tuple[str, ...] = ("anthropic", "bedrock", "fake")
DEFAULT_MODEL = "claude-opus-5-5"
FALLBACK_BETA = "server-side-fallback-2026-07-01"
EFFORT_LEVELS: tuple[str, ...] = ("low", "medium", "high", "xhigh", "max")
MAX_RUNNER_MESSAGES = 40
"""Maximum number of assistant messages of one repair session."""

OUTPUT_EXCERPT_CHARS = 2000
INPUT_VALUE_CHARS = 4000
_CREDENTIAL_ENVS = ("ANTHROPIC_API_KEY", "ANTHROPIC_AUTH_TOKEN")
#: Stands for the value of a variable of ``_CREDENTIAL_ENVS`` in an error message.
_HIDDEN = "[hidden]"
#: The SDK refuses non-streaming requests whose max_tokens could exceed its 10-minute timeout.
_MAX_TOKENS_TOO_LARGE = (
    "llm.max_tokens is too large for a non-streaming request during {action}; lower it (about 21000 at most)"
)
#: The categories of Claude's refusals (``cyber``, ``bio``): lower-case letters and ``_``, 32 at
#: most. A refusal's category is shown in no other shape, by both providers (:func:`_shown_category`).
_REFUSAL_CATEGORY = re.compile(r"[a-z_]{1,32}")
#: The stop reasons of Claude's answers (the SDK's ``StopReason`` and ``BetaStopReason``).
_STOP_REASONS: tuple[str, ...] = (
    "end_turn",
    "tool_use",
    "max_tokens",
    "stop_sequence",
    "pause_turn",
    "refusal",
    "model_context_window_exceeded",
    "compaction",
)


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
    ``tokens_used`` counts the tokens of the answers already billed for the failed call, when the
    provider knows them (the bedrock provider's classification); 0 otherwise.
    """

    tokens_used: int = 0

    def __init__(self, message: str, *, partial: RepairSessionResult | None = None) -> None:
        super().__init__(message)
        self.partial = partial


class LLMRefusal(RuntimeError):
    """The model declined the request (``stop_reason == "refusal"``).

    ``tokens_used``: as for :class:`LLMUnavailable`.
    """

    tokens_used: int = 0

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


def _log_fallback_hops(message: Any) -> None:
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


def _hide_api_keys(text: str) -> str:
    """``text`` without the values of ``ANTHROPIC_API_KEY`` and ``ANTHROPIC_AUTH_TOKEN``.

    Each value, and each run of ``text`` made of fragments of 16 or more characters of one (a
    cut or a split echo), becomes ``[hidden]``; the rest of ``text`` stays as it is. When a value
    or such a fragment can still be read once ``text`` is URL-decoded and its whitespace is taken
    out, nothing of ``text`` is kept but ``[hidden]`` (:func:`dag_healer.masking.hide_secrets`,
    shared with the bedrock provider).
    """
    keys = known_secrets(os.environ.get(name) for name in _CREDENTIAL_ENVS)
    if not keys:
        return text
    shown = hide_secrets(text, keys, _HIDDEN)
    compact = "".join(unquote(shown.replace("\\/", "/")).split())
    return _HIDDEN if holds_secret(compact, keys) else shown


def _shown_category(category: Any, hide: Callable[[str], str] = _hide_api_keys) -> str | None:
    """A refusal's ``category`` as a message or a log line may show it (``None``: ``unspecified``).

    Shared by the providers: the category itself when the whole of it is shaped like one of
    Claude's (:data:`_REFUSAL_CATEGORY`) and holds no credential (``hide``, which takes the
    provider's credentials out of a text, leaves it as it is), ``None`` for any other value.
    """
    if isinstance(category, str) and _REFUSAL_CATEGORY.fullmatch(category) and hide(category) == category:
        return category
    return None


def _known_stop_reason(stop_reason: Any) -> Any:
    """``stop_reason`` when it is one of Claude's (:data:`_STOP_REASONS`) or ``None``, else
    ``"other"``: what a log line may show of a stop reason that the answer could fill with anything."""
    return stop_reason if stop_reason is None or stop_reason in _STOP_REASONS else "other"


def _unthreaded(tool_runner: Callable[..., Any]) -> dict[str, Any]:
    """``{"thread": False}`` when the SDK's tool runner can use server-held threads.

    Since anthropic 2.2 the runner keeps the conversation on the server by default and drops
    ``system`` and ``tools`` from the follow-up requests. The repair loop resends the whole
    conversation instead, so every request keeps the shape described in the module docstring.
    """
    try:
        parameters = inspect.signature(tool_runner).parameters
    except (TypeError, ValueError):
        return {}
    return {"thread": False} if "thread" in parameters else {}


@contextmanager
def _api_errors(action: str) -> Iterator[None]:
    """Translate Anthropic SDK errors into :class:`LLMUnavailable` (most specific first).

    An API error's message is quoted without the values of the key variables (:func:`_hide_api_keys`).
    """
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
        # The SDK's message quotes the whole answer: a gateway may echo the key in it.
        detail = excerpt(_hide_api_keys(str(getattr(exc, "message", "") or "")), 300)
        raise LLMUnavailable(f"Anthropic API error {exc.status_code} during {action}: {detail}") from exc
    except anthropic.APIConnectionError as exc:
        raise LLMUnavailable(f"Cannot reach the Anthropic API during {action}: {type(exc).__name__}") from exc
    except ValueError as exc:
        if "Streaming is required" not in str(exc):
            raise
        raise LLMUnavailable(_MAX_TOKENS_TOO_LARGE.format(action=action)) from exc


# --------------------------------------------------------------------------- Anthropic


class AnthropicProvider:
    """Claude through the official ``anthropic`` SDK (1.x).

    The client is created lazily on the first call; without ``ANTHROPIC_API_KEY`` (or
    ``ANTHROPIC_AUTH_TOKEN``) every call raises :class:`LLMUnavailable` with a clear message.
    Its HTTP client is the SDK's default one with ``follow_redirects=False``: a redirect (from a
    gateway in ``ANTHROPIC_BASE_URL``, say) ends the call, and the prompt never goes to another
    host. Pass ``client`` to inject a pre-built ``anthropic.Anthropic`` (tests use one built on
    an ``httpx2.MockTransport``).
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
        self._client = anthropic.Anthropic(http_client=anthropic.DefaultHttpxClient(follow_redirects=False))
        return self._client

    def _errors(self, action: str) -> AbstractContextManager[None]:
        """The context manager that turns the SDK's errors into :class:`LLMUnavailable`.

        The provider of another platform overrides it with that platform's messages.
        """
        return _api_errors(action)

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
        """Raise for an answer that cannot be used: :class:`LLMRefusal` for a refusal (its category
        as :meth:`_refusal_category` shows it, its explanation without the provider's credentials),
        :class:`LLMUnavailable` for a cut answer or an exceeded context window."""
        if stop_reason == "refusal":
            category = self._refusal_category(stop_details)
            explanation = _get(stop_details, "explanation") if stop_details is not None else None
            raise LLMRefusal(
                f"Claude declined {action} (refusal category: {category or 'unspecified'})",
                category=category,
                explanation=self._without_credentials(explanation) if isinstance(explanation, str) else None,
            )
        if stop_reason == "max_tokens":
            raise LLMUnavailable(
                f"Claude's answer for {action} was cut at max_tokens={self.max_tokens}; raise llm.max_tokens"
            )
        if stop_reason == "model_context_window_exceeded":
            raise LLMUnavailable(f"The prompt for {action} exceeded the model context window")

    def _without_credentials(self, text: str) -> str:
        """``text`` without the provider's credentials: the values of ``ANTHROPIC_API_KEY`` and
        ``ANTHROPIC_AUTH_TOKEN`` (:func:`_hide_api_keys`). The provider of another platform hides
        its own."""
        return _hide_api_keys(text)

    def _refusal_category(self, details: Any) -> str | None:
        """The category of a refusal's ``stop_details`` as its error and the log line of a repair
        may show it: the providers' shared check (:func:`_shown_category`), with the provider's
        credentials (:meth:`_without_credentials`); ``None``: ``unspecified``."""
        category = _get(details, "category") if details is not None else None
        return _shown_category(category, self._without_credentials)

    def _shown_stop_reason(self, stop_reason: Any) -> Any:
        """The stop reason of a repair session as its last log line shows it: as the answer gave
        it. The provider of another platform shows only one of Claude's (:func:`_known_stop_reason`)."""
        return stop_reason

    def _log_fallbacks(self, message: Any) -> None:
        """Log the server-side fallback hops of ``message`` (:func:`_log_fallback_hops`). The
        provider of a platform without server-side fallbacks logs none."""
        _log_fallback_hops(message)

    # ------------------------------------------------------------------ classify
    def classify(self, *, system: str, prompt: str) -> tuple[Classification, int]:
        """One structured-output call; returns the parsed classification and the tokens used."""
        import pydantic

        client = self._get_client()
        kwargs = self._request_kwargs(system, prompt)
        action = "the incident classification"
        with self._errors(action):
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
        self._log_fallbacks(message)
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
            with self._errors(action):
                tool_runner = client.beta.messages.tool_runner
                runner = tool_runner(
                    tools=runnable,
                    max_iterations=MAX_RUNNER_MESSAGES,
                    **_unthreaded(tool_runner),
                    **self._request_kwargs(system, prompt),
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
            self._shown_stop_reason(result.stop_reason),
            len(result.tool_calls),
            result.tokens_used,
            " (budget exhausted)" if result.budget_exhausted else "",
        )
        return result

    def _drive(self, runner: Any, budget: TokenBudget, result: RepairSessionResult) -> None:
        count = 0
        for message in runner:
            count += 1
            budget.add_usage(getattr(message, "usage", None))
            result.stop_reason = getattr(message, "stop_reason", None)
            self._log_fallbacks(message)
            text = _message_text(message)
            if text:
                result.final_text = text
            if result.stop_reason == "refusal":
                category = self._refusal_category(getattr(message, "stop_details", None))
                logger.warning(
                    "Claude declined the repair session (refusal category: %s)", category or "unspecified"
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


def provider_name(cfg: Any) -> str:
    """The name of the provider the worker runs: ``$HEALER_LLM_PROVIDER``, else ``llm.provider``.

    Stripped and lower-cased; ``"anthropic"`` when neither is set (``cfg`` may be ``None``). The
    name is not checked: it may be one that :func:`get_provider` refuses.
    """
    configured = getattr(getattr(cfg, "llm", None), "provider", None)
    return str(os.environ.get(PROVIDER_ENV) or configured or "anthropic").strip().lower()


def get_provider(cfg: Any) -> LLMProvider:
    """Return the configured provider (``$HEALER_LLM_PROVIDER`` overrides ``llm.provider``).

    ``"anthropic"`` and ``"bedrock"`` never fail here: missing credentials (an unset
    ``ANTHROPIC_API_KEY``, no AWS credentials), a missing AWS region or ``aws`` extra, or a
    Claude API model ID given to Bedrock surface as :class:`LLMUnavailable` when the provider is
    first called. Unknown names raise :class:`ValueError`.
    """
    name = provider_name(cfg)
    if name == "anthropic":
        return AnthropicProvider(cfg)
    if name == "bedrock":
        from dag_healer.agent.bedrock import BedrockProvider

        return BedrockProvider(cfg)
    if name == "fake":
        from dag_healer.agent.fake import FakeProvider

        return FakeProvider(cfg)
    expected = ", ".join(repr(known) for known in PROVIDER_NAMES[:-1]) + f" or {PROVIDER_NAMES[-1]!r}"
    raise ValueError(f"Unknown LLM provider {name!r}: expected {expected}")


__all__ = [
    "DEFAULT_MODEL",
    "EFFORT_LEVELS",
    "FALLBACK_BETA",
    "MAX_RUNNER_MESSAGES",
    "PROVIDER_ENV",
    "PROVIDER_NAMES",
    "AnthropicProvider",
    "Classification",
    "LLMProvider",
    "LLMRefusal",
    "LLMUnavailable",
    "RepairSessionResult",
    "ToolCallRecord",
    "excerpt",
    "get_provider",
    "provider_name",
    "tool_call_record",
]
