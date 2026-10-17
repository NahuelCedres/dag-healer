"""Small helpers shared by the CLI commands: tables, secret masking, config loading.

Only the standard library is imported at module level so ``dag-healer --help`` stays fast.
"""

from __future__ import annotations

import argparse
import sys
from collections.abc import Iterable, Sequence
from typing import TYPE_CHECKING, Any, TextIO
from urllib.parse import urlsplit, urlunsplit

if TYPE_CHECKING:
    from dag_healer.config import HealerConfig

MASK = "***"


def render_table(headers: Sequence[str], rows: Iterable[Sequence[Any]]) -> str:
    """Render ``rows`` as a plain-text table with left-aligned columns."""
    cells = [[_cell(value) for value in row] for row in rows]
    widths = [len(header) for header in headers]
    for row in cells:
        for index, value in enumerate(row):
            widths[index] = max(widths[index], len(value))
    lines = [_join(headers, widths), _join(["-" * width for width in widths], widths)]
    lines.extend(_join(row, widths) for row in cells)
    return "\n".join(lines)


def _cell(value: Any) -> str:
    if value is None:
        return "-"
    if isinstance(value, bool):
        return "yes" if value else "no"
    return str(value)


def _join(values: Sequence[str], widths: Sequence[int]) -> str:
    return "  ".join(value.ljust(width) for value, width in zip(values, widths, strict=False)).rstrip()


def error(message: str, *, stream: TextIO | None = None) -> None:
    """Print an error message for the user (stderr)."""
    print(f"error: {message}", file=stream or sys.stderr)


def warn(message: str, *, stream: TextIO | None = None) -> None:
    """Print a warning for the user (stderr)."""
    print(f"warning: {message}", file=stream or sys.stderr)


def mask_url(url: str | None) -> str | None:
    """Hide the password of a URL (``postgresql://user:***@host/db``)."""
    if not url:
        return url
    try:
        parts = urlsplit(url)
        password = parts.password
    except ValueError:
        return MASK
    if not password:
        return url
    userinfo, _, hostinfo = parts.netloc.rpartition("@")
    user = userinfo.split(":", 1)[0]
    return urlunsplit(parts._replace(netloc=f"{user}:{MASK}@{hostinfo}"))


def add_config_option(parser: argparse.ArgumentParser, help_text: str | None = None) -> None:
    """Add ``--config PATH`` to a sub-command without clobbering the global ``-c/--config``."""
    parser.add_argument(
        "-c",
        "--config",
        dest="config",
        metavar="PATH",
        default=argparse.SUPPRESS,
        help=help_text or "Path to healer.yaml (same as the global -c/--config).",
    )


def config_arg(args: argparse.Namespace) -> str | None:
    """Return the configuration path given on the command line, if any."""
    value = getattr(args, "config", None)
    return str(value) if value else None


def load_cli_config(args: argparse.Namespace) -> HealerConfig:
    """Load the configuration named on the command line (or the default resolution order).

    Raises :class:`dag_healer.config.ConfigError` when the file is missing or invalid.
    """
    from dag_healer.config import load_config

    return load_config(config_arg(args))


__all__ = [
    "MASK",
    "add_config_option",
    "config_arg",
    "error",
    "load_cli_config",
    "mask_url",
    "render_table",
    "warn",
]
