"""Prompt templates of the dag-healer agent (Markdown files shipped as package data).

Load them with :func:`load_prompt` (or ``importlib.resources``) instead of reading paths
relative to the source tree, so they also work from an installed wheel.
"""

from __future__ import annotations

from functools import cache
from importlib import resources


@cache
def load_prompt(name: str) -> str:
    """Return the text of the prompt file ``name`` (for example ``"classify.md"``).

    Raises :class:`FileNotFoundError` when no such prompt is shipped.
    """
    if "/" in name or "\\" in name or name.startswith("."):
        raise FileNotFoundError(f"invalid prompt name: {name!r}")
    resource = resources.files(__name__).joinpath(name)
    if not resource.is_file():
        raise FileNotFoundError(f"prompt not found: {name}")
    return resource.read_text(encoding="utf-8")


__all__ = ["load_prompt"]
