"""Entry point of the ``dag-healer`` command.

Commands live in a registry so other modules can contribute them without touching
the parser code::

    from dag_healer.cli.main import register_command

    def _configure(parser: argparse.ArgumentParser) -> None:
        parser.add_argument("--repo", default=".")

    def _run(args: argparse.Namespace) -> int:
        ...
        return 0

    register_command("init", help="Discover DAGs and write draft contracts.",
                     handler=_run, configure=_configure)

Modules listed in :data:`COMMAND_MODULES` are imported before the parser is built,
so registering a new command module is a one-line change there. Command modules
should keep heavy imports (pandas, SQLAlchemy, ...) inside their handlers so that
``dag-healer version`` and ``--help`` stay fast. argparse is used on purpose: it adds
no dependency that could conflict with Airflow's own pins.
"""

from __future__ import annotations

import argparse
import importlib
import logging
import os
import sys
from collections.abc import Callable, Sequence
from dataclasses import dataclass
from pathlib import Path

from dag_healer._version import __version__

log = logging.getLogger(__name__)

Handler = Callable[[argparse.Namespace], int | None]
Configure = Callable[[argparse.ArgumentParser], None]

#: Modules that register commands on import (they keep heavy imports inside handlers).
COMMAND_MODULES: tuple[str, ...] = (
    "dag_healer.cli.init_cmd",
    "dag_healer.cli.contracts_cmd",
    "dag_healer.cli.incidents_cmd",
    "dag_healer.cli.doctor_cmd",
    "dag_healer.cli.worker_cmd",
    "dag_healer.cli.store_cmd",
    "dag_healer.cli.orchestrators_cmd",
    "dag_healer.cli.serve_cmd",
)


@dataclass(frozen=True)
class Command:
    """A registered sub-command."""

    name: str
    help: str
    handler: Handler
    configure: Configure | None = None
    aliases: tuple[str, ...] = ()


COMMANDS: dict[str, Command] = {}


def register_command(
    name: str,
    *,
    help: str,
    handler: Handler,
    configure: Configure | None = None,
    aliases: Sequence[str] = (),
) -> Command:
    """Register (or replace) the sub-command ``name``."""
    command = Command(name=name, help=help, handler=handler, configure=configure, aliases=tuple(aliases))
    COMMANDS[name] = command
    return command


def _load_command_modules() -> None:
    for module_name in COMMAND_MODULES:
        try:
            importlib.import_module(module_name)
        except ModuleNotFoundError as exc:
            if exc.name != module_name:
                raise
            log.debug("dag-healer: command module %s is not available", module_name)


def build_parser() -> argparse.ArgumentParser:
    """Build the argument parser from the command registry."""
    _load_command_modules()
    parser = argparse.ArgumentParser(
        prog="dag-healer",
        description="Diagnose and repair failing tasks of existing Airflow DAGs.",
    )
    parser.add_argument("--version", action="version", version=f"dag-healer {__version__}")
    parser.add_argument(
        "-c",
        "--config",
        metavar="PATH",
        default=None,
        help="Path to healer.yaml (default: $HEALER_CONFIG, $AIRFLOW_HOME/healer.yaml, ./healer.yaml). "
        "Exported as HEALER_CONFIG for the rest of the process.",
    )
    parser.add_argument(
        "-v", "--verbose", action="count", default=0, help="Increase log verbosity (-v info, -vv debug)."
    )
    subparsers = parser.add_subparsers(dest="command", metavar="<command>")
    for command in sorted(COMMANDS.values(), key=lambda c: c.name):
        sub = subparsers.add_parser(
            command.name, help=command.help, description=command.help, aliases=list(command.aliases)
        )
        if command.configure is not None:
            command.configure(sub)
        sub.set_defaults(_handler=command.handler)
    return parser


def _configure_logging(verbosity: int) -> None:
    level = logging.WARNING if verbosity <= 0 else logging.INFO if verbosity == 1 else logging.DEBUG
    logging.basicConfig(level=level, format="%(asctime)s %(levelname)s %(name)s: %(message)s")


def main(argv: Sequence[str] | None = None) -> int:
    """Run the CLI and return its exit code."""
    parser = build_parser()
    args = parser.parse_args(list(argv) if argv is not None else None)
    _configure_logging(args.verbose)
    if args.config:
        os.environ["HEALER_CONFIG"] = str(Path(args.config).expanduser().resolve())
    handler: Handler | None = getattr(args, "_handler", None)
    if handler is None:
        parser.print_help(sys.stderr)
        return 2
    try:
        result = handler(args)
    except KeyboardInterrupt:
        return 130
    return 0 if result is None else int(result)


# --------------------------------------------------------------------------- built-ins


def _cmd_version(args: argparse.Namespace) -> int:
    print(f"dag-healer {__version__}")
    return 0


register_command("version", help="Show the dag-healer version.", handler=_cmd_version)


if __name__ == "__main__":
    # Re-import under the canonical name so commands registered by other modules
    # (which import dag_healer.cli.main) land in the same registry.
    from dag_healer.cli.main import main as _canonical_main

    raise SystemExit(_canonical_main())
