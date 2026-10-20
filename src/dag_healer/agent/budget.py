"""Token budget of one incident (``limits.max_tokens_per_incident``)."""

from __future__ import annotations

from typing import Any


class TokenBudget:
    """Counts the tokens spent on an incident against a fixed limit.

    ``used`` includes input tokens (cached or not) and output tokens. The budget is
    *exhausted* once ``used >= limit``; callers check it between LLM turns, so the last
    turn may overshoot the limit by up to one response.
    """

    def __init__(self, limit: int) -> None:
        if limit < 0:
            raise ValueError("the token budget limit must be >= 0")
        self.limit = int(limit)
        self.input_tokens = 0
        self.output_tokens = 0

    def add(self, input_tokens: int = 0, output_tokens: int = 0) -> int:
        """Record tokens spent by one LLM call and return the new total."""
        self.input_tokens += max(0, int(input_tokens or 0))
        self.output_tokens += max(0, int(output_tokens or 0))
        return self.used

    def add_usage(self, usage: Any) -> int:
        """Record an SDK ``usage`` object (``input``, ``output`` and cache token counts)."""
        input_tokens, output_tokens = usage_tokens(usage)
        return self.add(input_tokens, output_tokens)

    @property
    def used(self) -> int:
        """Tokens spent so far."""
        return self.input_tokens + self.output_tokens

    @property
    def remaining(self) -> int:
        """Tokens left before the budget is exhausted (never negative)."""
        return max(0, self.limit - self.used)

    @property
    def exhausted(self) -> bool:
        """True once the spent tokens reach the limit."""
        return self.used >= self.limit

    def __repr__(self) -> str:
        return f"TokenBudget(limit={self.limit}, used={self.used})"


def usage_tokens(usage: Any) -> tuple[int, int]:
    """Return ``(input_tokens, output_tokens)`` of an Anthropic ``usage`` object or dict.

    Input tokens include cache creation and cache read tokens, which are billed (at
    different rates) on top of the uncached ``input_tokens``.
    """
    if usage is None:
        return 0, 0

    def _get(name: str) -> int:
        value = usage.get(name) if isinstance(usage, dict) else getattr(usage, name, None)
        try:
            return max(0, int(value or 0))
        except (TypeError, ValueError):
            return 0

    input_tokens = (
        _get("input_tokens") + _get("cache_creation_input_tokens") + _get("cache_read_input_tokens")
    )
    return input_tokens, _get("output_tokens")


__all__ = ["TokenBudget", "usage_tokens"]
