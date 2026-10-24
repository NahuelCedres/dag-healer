"""``dag-healer store stats|status|migrate|reset``: inspect, upgrade or empty the incident store.

``status`` shows the schema version of the store and the migrations this dag-healer would
apply; ``migrate`` applies them (``--dry-run`` prints the SQL and changes nothing). An
empty store is created at the latest schema. Stop the worker first and take a backup of a
PostgreSQL store; an SQLite store is copied to ``<db>.bak-v<version>`` automatically before
its tables are rebuilt. The worker also migrates on start, the Airflow api-server never does.

``reset --yes`` deletes every incident, timeline event, alert, command, pending event,
cached contract state and ``healer_kv`` row (the worker's poll cursors and its
``instance:<id>`` record); the schema and the contract YAML files are kept. It also restarts
the worker's poll at the moment of the reset (a new ``cursor:<instance>`` in ``healer_kv``),
so failures that are still in the orchestrator's history do not come back as new incidents;
``--keep-state`` leaves the ``healer_kv`` rows alone. ``--artifacts`` additionally deletes
the PR.md / fix.patch files and any leftover repair workspaces under ``$HEALER_HOME``.

Meant for demos and test environments: stop the worker first (it is the only writer of
the repository and of most rows). The commands work without a ``healer.yaml`` as long as
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


def _data_tables(*, keep_state: bool = False) -> list[object]:
    """Store tables holding data, children first (the schema version table is kept).

    ``keep_state=True`` also keeps ``healer_kv`` (the worker's poll cursors and instance record).
    """
    from dag_healer.store.models import Base, KeyValue, SchemaVersion

    kept = {SchemaVersion.__tablename__} | ({KeyValue.__tablename__} if keep_state else set())
    return [table for table in reversed(Base.metadata.sorted_tables) if table.name not in kept]


def table_counts(url: str) -> dict[str, int]:
    """Number of rows per store table."""
    from sqlalchemy import func, select

    from dag_healer.store import session_scope

    counts: dict[str, int] = {}
    with session_scope(url) as session:
        for table in reversed(_data_tables()):
            counts[table.name] = int(session.scalar(select(func.count()).select_from(table)) or 0)
    return counts


def reset_store(url: str, *, keep_state: bool = False) -> dict[str, int]:
    """Delete every row of the store's data tables; returns the deleted rows per table.

    ``keep_state=True`` keeps the worker's poll cursors and instance record (``healer_kv``).
    """
    from dag_healer.store import session_scope

    deleted: dict[str, int] = {}
    with session_scope(url) as session:
        for table in _data_tables(keep_state=keep_state):
            result = session.execute(table.delete())
            deleted[table.name] = max(int(result.rowcount or 0), 0)
    log.info("dag-healer: store reset, deleted %s", deleted)
    return deleted


def describe_schema(url: str) -> list[str]:
    """Lines of ``dag-healer store status``: schema version, applied rows and pending migrations.

    Read only: the store is neither created nor migrated.
    """
    from dag_healer.store import get_engine
    from dag_healer.store.migrations import backup_path, pending, schema_state
    from dag_healer.store.models import SCHEMA_VERSION

    engine = get_engine(url)
    state = schema_state(engine)
    lines = [f"Store: {mask_url(url)}"]
    if state.version == 0:
        lines.append(
            f"Schema: none yet (empty store). The worker, 'dag-healer init' or "
            f"'dag-healer store migrate' creates schema v{SCHEMA_VERSION}."
        )
        return lines
    if state.version > SCHEMA_VERSION:
        lines.append(
            f"Schema: v{state.version}, newer than this dag-healer (v{SCHEMA_VERSION}): upgrade dag-healer."
        )
    elif state.version == SCHEMA_VERSION:
        lines.append(f"Schema: v{state.version} (up to date)")
    else:
        lines.append(f"Schema: v{state.version} (this dag-healer uses v{SCHEMA_VERSION})")
    if state.applied:
        rows = [
            (f"v{number}", at.isoformat(timespec="seconds") if at else None) for number, at in state.applied
        ]
        lines.append(render_table(("VERSION", "APPLIED AT"), rows))
    todo = pending(engine)
    if todo:
        lines.append("Pending migrations:")
        lines += [f"  v{migration.version}  {migration.description}" for migration in todo]
        copy = backup_path(engine, state.version)
        lines.append(
            "Stop the worker, then run 'dag-healer store migrate' (or start the worker, which migrates)."
        )
        lines.append(
            f"The SQLite file is copied to {copy} first."
            if copy is not None
            else "Take a backup first (for PostgreSQL: pg_dump)."
        )
    if state.missing_tables and state.version <= SCHEMA_VERSION:
        lines.append(f"Missing tables (created on the next start): {', '.join(sorted(state.missing_tables))}")
    return lines


def migrate_store(url: str, *, dry_run: bool = False) -> tuple[int, int, list[int], list[str]]:
    """Migrate (or create) the store at ``url``; ``(before, after, applied, lines)``.

    ``dry_run=True`` changes nothing and returns the SQL that would run as ``lines``.
    """
    from dag_healer.store import get_engine
    from dag_healer.store.db import init_db
    from dag_healer.store.migrations import current_version, migrate, migration_script

    engine = get_engine(url)
    before = current_version(engine)
    if dry_run:
        return before, before, [], migration_script(engine)
    applied = migrate(engine)
    init_db(engine, allow_migrate=True)
    return before, current_version(engine), applied, []


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


def _cmd_status(args: argparse.Namespace) -> int:
    cfg = _load(args)
    if cfg is None:
        return 1
    url = cfg.database_url
    try:
        lines = describe_schema(url)
    except Exception as exc:  # driver-specific connection errors
        error(f"cannot read the store at {mask_url(url)}: {type(exc).__name__}: {exc}")
        return 1
    print("\n".join(lines))
    return 0


def _cmd_migrate(args: argparse.Namespace) -> int:
    from dag_healer.store import get_engine
    from dag_healer.store.migrations import backup_path
    from dag_healer.store.models import SCHEMA_VERSION

    cfg = _load(args)
    if cfg is None:
        return 1
    url = cfg.database_url
    try:
        before, after, applied, lines = migrate_store(url, dry_run=args.dry_run)
    except Exception as exc:  # MigrationError, driver-specific connection errors
        error(f"cannot migrate the store at {mask_url(url)}: {type(exc).__name__}: {exc}")
        return 1
    if args.dry_run:
        print(f"-- Store: {mask_url(url)} (dry run: nothing was changed)")
        print("\n".join(lines))
        return 0
    if before > SCHEMA_VERSION:
        print(f"Store {mask_url(url)} is at schema v{before}, newer than this dag-healer (v{SCHEMA_VERSION})")
        return 0
    if before == 0:
        print(f"Store {mask_url(url)} created at schema v{after}.")
    elif applied:
        steps = ", ".join(f"v{version}" for version in applied)
        print(f"Store {mask_url(url)} migrated from schema v{before} to v{after} (applied {steps}).")
        copy = backup_path(get_engine(url), before)
        if copy is not None and copy.is_file():
            print(f"The schema v{before} database was copied to {copy} first.")
    else:
        print(f"Store {mask_url(url)} is already at schema v{after}: nothing to migrate.")
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
        deleted = reset_store(url, keep_state=args.keep_state)
    except Exception as exc:  # driver-specific connection errors
        error(f"cannot reset the store at {mask_url(url)}: {type(exc).__name__}: {exc}")
        return 1
    total = sum(deleted.values())
    details = ", ".join(f"{name}: {count}" for name, count in sorted(deleted.items()) if count)
    print(f"Store {mask_url(url)} reset: {total} row(s) deleted" + (f" ({details})" if details else ""))

    if not args.keep_state:
        from dag_healer.orchestrators.registry import orchestrator_settings
        from dag_healer.store.repo import cursor_key
        from dag_healer.worker.pipeline import reset_state

        try:
            key = cursor_key(orchestrator_settings(cfg).instance)
            moment = reset_state(cfg)
        except Exception as exc:  # driver-specific connection errors, an unreadable orchestrator section
            error(f"cannot restart the worker poll in {mask_url(url)}: {type(exc).__name__}: {exc}")
            return 1
        print(
            f"Worker poll restarts at {moment.isoformat(timespec='seconds')} "
            f"(healer_kv {key}): earlier failures are not picked up again."
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
    status = actions.add_parser(
        "status",
        help="Show the store schema version and the pending migrations.",
        description="Show the store schema version and the migrations this dag-healer would apply "
        "(read only).",
    )
    add_config_option(status)
    migrate = actions.add_parser(
        "migrate",
        help="Upgrade the store schema to this dag-healer's version.",
        description="Upgrade the store schema to this dag-healer's version (an empty store is "
        "created). Stop the worker and back up a PostgreSQL store first; an SQLite store is "
        "copied to <db>.bak-v<version> automatically.",
    )
    migrate.add_argument(
        "--dry-run", action="store_true", help="Print the SQL that would run and change nothing."
    )
    add_config_option(migrate)
    reset = actions.add_parser(
        "reset",
        help="Delete every incident, event, alert, command and cached contract state.",
        description="Delete every incident, event, alert, command, cached contract state and "
        "worker poll cursor. Stop the worker first.",
    )
    reset.add_argument("--yes", action="store_true", help="Confirm the deletion (required).")
    reset.add_argument(
        "--keep-state",
        action="store_true",
        help="Keep the worker's poll position (the healer_kv rows; by default the poll restarts now).",
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
    handlers = {"stats": _cmd_stats, "status": _cmd_status, "migrate": _cmd_migrate, "reset": _cmd_reset}
    handler = handlers.get(action or "")
    if handler is not None:
        return handler(args)
    parser: argparse.ArgumentParser | None = getattr(args, "_store_parser", None)
    if parser is not None:
        parser.print_help()
    return 2


register_command(
    "store", help="Inspect, migrate or reset the incident store.", handler=_run, configure=_configure
)

__all__ = [
    "ARTIFACT_DIRS",
    "describe_schema",
    "migrate_store",
    "remove_artifacts",
    "reset_store",
    "table_counts",
]
