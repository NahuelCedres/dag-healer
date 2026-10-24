"""``dag-healer worker [--once] [--provider fake|anthropic]``: run the incident worker.

Without ``--once`` the worker polls forever (stop it with SIGTERM / Ctrl+C); with it, one
iteration runs (apply commands, reconcile, process every claimable incident) and the exit
code is 1 when that iteration hit errors. ``--provider`` overrides ``llm.provider`` and
``$HEALER_LLM_PROVIDER``. A worker that cannot start (the orchestrator adapter does not load,
it cannot serve ``monitor.modes`` / ``monitor.trigger``, or the store already belongs to
another orchestrator type under this instance id) prints the reason and exits with 1. Heavy
imports stay inside the handler so ``--help`` is fast.
"""

from __future__ import annotations

import argparse
import logging

from dag_healer.cli._output import add_config_option, error, load_cli_config
from dag_healer.cli.main import register_command

PROVIDERS = ("fake", "anthropic")


def _configure(parser: argparse.ArgumentParser) -> None:
    parser.add_argument("--once", action="store_true", help="Run one iteration and exit.")
    parser.add_argument(
        "--provider",
        choices=PROVIDERS,
        default=None,
        help="LLM provider (overrides llm.provider and $HEALER_LLM_PROVIDER).",
    )
    add_config_option(parser)


def build_provider(cfg: object, name: str | None) -> object:
    """The LLM provider named on the command line, else the configured one."""
    if name == "fake":
        from dag_healer.agent.fake import FakeProvider

        return FakeProvider(cfg)
    if name == "anthropic":
        from dag_healer.agent.llm import AnthropicProvider

        return AnthropicProvider(cfg)
    from dag_healer.agent.llm import get_provider

    return get_provider(cfg)


def _run(args: argparse.Namespace) -> int:
    from dag_healer.config import ConfigError
    from dag_healer.worker.loop import WorkerStartError, run_worker

    try:
        cfg = load_cli_config(args)
    except ConfigError as exc:
        error(str(exc))
        return 1
    try:
        provider = build_provider(cfg, args.provider)
    except ValueError as exc:
        error(str(exc))
        return 1

    # The worker reports one line per incident transition at INFO level.
    package_logger = logging.getLogger("dag_healer")
    previous_level = package_logger.level
    if not getattr(args, "verbose", 0) and package_logger.getEffectiveLevel() > logging.INFO:
        package_logger.setLevel(logging.INFO)
    try:
        stats = run_worker(cfg, once=args.once, provider=provider)  # type: ignore[arg-type]
    except WorkerStartError as exc:
        error(str(exc))
        return 1
    finally:
        package_logger.setLevel(previous_level)
    if args.once:
        processed = ", ".join(f"{status}: {count}" for status, count in sorted(stats.processed.items()))
        print(
            f"dag-healer worker: {stats.created} incident(s) created, "
            f"{sum(stats.processed.values())} processed ({processed or 'none'}), "
            f"{stats.commands - stats.commands_failed} command(s) applied, {stats.commands_failed} failed"
        )
        if stats.errors:
            for message in stats.errors:
                error(message)
            return 1
    return 0


register_command(
    "worker",
    help="Run the dag-healer worker (reconcile, classify, repair).",
    handler=_run,
    configure=_configure,
)

__all__ = ["PROVIDERS", "build_provider"]
