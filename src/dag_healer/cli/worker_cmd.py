"""``dag-healer worker``: placeholder until the worker lands in phase 2."""

from __future__ import annotations

import argparse
import sys

from dag_healer.cli._output import add_config_option
from dag_healer.cli.main import register_command

PLACEHOLDER_MESSAGE = "worker arrives in phase 2"
PLACEHOLDER_EXIT_CODE = 2


def _configure(parser: argparse.ArgumentParser) -> None:
    parser.add_argument("--once", action="store_true", help="Process pending work once and exit.")
    add_config_option(parser)


def _run(args: argparse.Namespace) -> int:
    print(PLACEHOLDER_MESSAGE, file=sys.stderr)
    return PLACEHOLDER_EXIT_CODE


register_command(
    "worker",
    help="Run the dag-healer worker (reconcile, classify, repair).",
    handler=_run,
    configure=_configure,
)
