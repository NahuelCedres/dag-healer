"""``dag-healer store stats|reset``: inspect or empty the incident store.

``reset --yes`` deletes every incident, timeline event, alert, command, pending event and
cached contract state (the schema and the contract YAML files are kept). It also restarts
the worker's REST poll at the moment of the reset, so failed task instances that are still
in Airflow's history do not come back as new incidents; ``--keep-state`` leaves the poll
watermark alone. ``--artifacts`` additionally deletes the PR.md / fix.patch files and any
leftover repair workspaces under ``$HEALER_HOME``.

Meant for demos and test environments: stop the worker first (it is the only writer of
the repository and of most rows). The command works without a ``healer.yaml`` as long as
the store URL is known (``$HEALER_DATABASE_URL``, ``store.url`` or the SQLite default).
"""

from __future__ import annotations

import argparse
import logging
from typing import TYPE_CHECKING

from dag_healer.cli._output import add_config_option, error, load_cli_config, mask_url, render_table
from dag_healer.cli.main import register_command

if TYPE_CHECKING:
    from dag_healer.config import HealerConfig

log = logging.getLogger(__name__)

#: Directories under ``$HEALER_HOME`` that only hold per-incident artifacts.
ARTIFACT_DIRS: tuple[str, ...] = ("prs", "work")


def _data_tables() -> list[object]:
    """Store tables holding data, children first (the schema version table is kept)."""
    from dag_healer.store.models import Base, SchemaVersion

    return [
        table for table in reversed(Base.metadata.sorted_tables) if table.name != SchemaVersion.__tablename__
    ]


def table_counts(url: str) -> dict[str, int]:
    """Number of rows per store table."""
    from sqlalchemy import func, select

    from dag_healer.store import session_scope

    counts: dict[str, int] = {}
    with session_scope(url) as session:
        for table in reversed(_data_tables()):
            counts[table.name] = int(session.scalar(select(func.count()).select_from(table)) or 0)
    return counts


def reset_store(url: str) -> dict[str, int]:
    """Delete every row of the store's data tables; returns the deleted rows per table."""
    from dag_healer.store import session_scope

    deleted: dict[str, int] = {}
    with session_scope(url) as session:
        for table in _data_tables():
            result = session.execute(table.delete())
            deleted[table.name] = max(int(result.rowcount or 0), 0)
    log.info("dag-healer: store reset, deleted %s", deleted)
    return deleted


def remove_artifacts(cfg: HealerConfig) -> list[str]:
    """Delete ``<healer_home>/prs`` and ``<healer_home>/work``; returns the removed paths."""
    from dag_healer.repo.git import remove_tree

    removed: list[str] = []
    for name in ARTIFACT_DIRS:
        path = cfg.healer_home / name
        if path.exists():
            remove_tree(path)
            removed.append(str(path))
    return removed


def _load(args: argparse.Namespace) -> HealerConfig | None:
    from dag_healer.config import ConfigError

    try:
        return load_cli_config(args)
    except ConfigError as exc:
        error(str(exc))
        return None


def _cmd_stats(args: argparse.Namespace) -> int:
    cfg = _load(args)
    if cfg is None:
        return 1
    url = cfg.database_url
    try:
        counts = table_counts(url)
    except Exception as exc:  # driver-specific connection errors
        error(f"cannot read the store at {mask_url(url)}: {type(exc).__name__}: {exc}")
        return 1
    print(f"Store: {mask_url(url)}")
    print(render_table(("TABLE", "ROWS"), sorted(counts.items())))
    return 0


def _cmd_reset(args: argparse.Namespace) -> int:
    cfg = _load(args)
    if cfg is None:
        return 1
    url = cfg.database_url
    if not args.yes:
        error(
            f"this deletes every incident, event, alert and command in {mask_url(url)}; pass --yes to confirm"
        )
        return 2
    try:
        deleted = reset_store(url)
    except Exception as exc:  # driver-specific connection errors
        error(f"cannot reset the store at {mask_url(url)}: {type(exc).__name__}: {exc}")
        return 1
    total = sum(deleted.values())
    details = ", ".join(f"{name}: {count}" for name, count in sorted(deleted.items()) if count)
    print(f"Store {mask_url(url)} reset: {total} row(s) deleted" + (f" ({details})" if details else ""))

    if not args.keep_state:
        from dag_healer.worker.pipeline import reset_state, state_path

        try:
            moment = reset_state(cfg)
        except OSError as exc:
            error(f"cannot write the worker state file {state_path(cfg)}: {exc}")
            return 1
        print(
            f"Worker REST poll restarts at {moment.isoformat(timespec='seconds')} "
            f"({state_path(cfg)}): earlier failures are not picked up again."
        )
    if args.artifacts:
        try:
            removed = remove_artifacts(cfg)
        except OSError as exc:
            error(f"cannot delete the artifacts under {cfg.healer_home}: {exc}")
            return 1
        print(f"Removed {', '.join(removed)}" if removed else f"No artifacts under {cfg.healer_home}")
    return 0


def _configure(parser: argparse.ArgumentParser) -> None:
    add_config_option(parser)
    actions = parser.add_subparsers(dest="store_action", metavar="<action>")
    stats = actions.add_parser(
        "stats", help="Show the number of rows per store table.", description="Show row counts."
    )
    add_config_option(stats)
    reset = actions.add_parser(
        "reset",
        help="Delete every incident, event, alert, command and cached contract state.",
        description="Delete every incident, event, alert, command and cached contract state. "
        "Stop the worker first.",
    )
    reset.add_argument("--yes", action="store_true", help="Confirm the deletion (required).")
    reset.add_argument(
        "--keep-state",
        action="store_true",
        help="Keep the worker's REST poll watermark (by default the poll restarts now).",
    )
    reset.add_argument(
        "--artifacts",
        action="store_true",
        help="Also delete $HEALER_HOME/prs (PR.md, fix.patch) and $HEALER_HOME/work.",
    )
    add_config_option(reset)
    parser.set_defaults(_store_parser=parser)


def _run(args: argparse.Namespace) -> int:
    action = getattr(args, "store_action", None)
    if action == "stats":
        return _cmd_stats(args)
    if action == "reset":
        return _cmd_reset(args)
    parser: argparse.ArgumentParser | None = getattr(args, "_store_parser", None)
    if parser is not None:
        parser.print_help()
    return 2


register_command("store", help="Inspect or reset the incident store.", handler=_run, configure=_configure)

__all__ = ["ARTIFACT_DIRS", "remove_artifacts", "reset_store", "table_counts"]
