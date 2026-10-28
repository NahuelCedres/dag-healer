"""``dag-healer worker [--once] [--provider fake|anthropic|bedrock]``: run the incident worker.

Without ``--once`` the worker polls forever (stop it with SIGTERM / Ctrl+C); with it, one
iteration runs (apply commands, reconcile, process every claimable incident) and the exit
code is 1 when that iteration hit errors. ``--provider`` overrides ``llm.provider`` and
``$HEALER_LLM_PROVIDER``: ``fake`` (offline), ``anthropic`` (the Claude API) or ``bedrock``
(Claude on Amazon Bedrock). A worker that cannot start (the orchestrator adapter does not load,
it cannot serve ``monitor.modes`` / ``monitor.trigger``, or the store already belongs to
another orchestrator type under this instance id) prints the reason and exits with 1. Heavy
imports stay inside the handler so ``--help`` is fast.

At start-up the worker logs where the CODE repairs will run their tests: a warning for the
``process`` backend (no isolation) and for a container sandbox that cannot run (the reason; CODE
repairs fail until it works, DATA and AMBIGUOUS incidents are not affected). It starts anyway.
"""

from __future__ import annotations

import argparse
import logging
from typing import TYPE_CHECKING

from dag_healer.cli._output import add_config_option, error, load_cli_config
from dag_healer.cli.main import register_command

if TYPE_CHECKING:
    from dag_healer.config import HealerConfig

logger = logging.getLogger(__name__)

#: The names of ``dag_healer.agent.llm.PROVIDER_NAMES`` (a test keeps them equal), spelled out here
#: so that ``--help`` imports no pydantic; ``fake`` first, as the offline default of the demo.
PROVIDERS = ("fake", "anthropic", "bedrock")


def _configure(parser: argparse.ArgumentParser) -> None:
    parser.add_argument("--once", action="store_true", help="Run one iteration and exit.")
    parser.add_argument(
        "--provider",
        choices=PROVIDERS,
        default=None,
        help=(
            "LLM provider: fake (offline), anthropic (the Claude API) or bedrock (Claude on Amazon "
            "Bedrock); overrides llm.provider and $HEALER_LLM_PROVIDER."
        ),
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
    if name == "bedrock":
        from dag_healer.agent.bedrock import BedrockProvider

        return BedrockProvider(cfg)
    from dag_healer.agent.llm import get_provider

    return get_provider(cfg)


def _log_sandbox(cfg: HealerConfig) -> None:
    """Log where the CODE repairs will run pytest (``repo.sandbox``); never raises.

    Only the cheap availability probe runs here (no container is started); without an image
    no engine is asked anything.
    """
    from dag_healer.masking import mask_text
    from dag_healer.repo.sandbox import PROCESS_BACKEND_WARNING, build_sandbox

    settings = cfg.repo.sandbox
    try:
        sandbox = build_sandbox(settings)
        if not sandbox.isolated:
            logger.warning("dag-healer worker: %s", PROCESS_BACKEND_WARNING)
            return
        reason = sandbox.unavailable_reason()
    except Exception as exc:  # the worker starts anyway: every repair checks its sandbox again
        reason = f"{type(exc).__name__}: {exc}"
    if reason is not None:
        logger.warning(
            "dag-healer worker: CODE repairs will fail until the test sandbox works: %s", mask_text(reason)
        )
        return
    logger.info(
        "dag-healer worker: test sandbox: %s (%s, image %s)", sandbox.name, settings.engine, settings.image
    )


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
        _log_sandbox(cfg)
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
