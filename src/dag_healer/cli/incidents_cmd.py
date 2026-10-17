"""``dag-healer incidents list``: show incidents recorded in the store."""

from __future__ import annotations

import argparse
import json
from collections.abc import Iterable
from datetime import datetime
from typing import TYPE_CHECKING, Any

from dag_healer.cli._output import add_config_option, error, load_cli_config, mask_url, render_table, warn
from dag_healer.cli.main import register_command

if TYPE_CHECKING:
    from dag_healer.store.models import Incident

HEADERS = ("ID", "CREATED_AT", "DAG_ID", "TASK_ID", "RUN_ID", "TRY", "STATUS", "CATEGORY", "CONF", "SEEN")


def _split_values(values: Iterable[str] | None) -> list[str]:
    """Flatten repeated and comma-separated option values."""
    result: list[str] = []
    for value in values or ():
        result.extend(part.strip() for part in value.split(",") if part.strip())
    return result


def _format_time(value: datetime | None) -> str | None:
    return value.strftime("%Y-%m-%d %H:%M:%S") if value is not None else None


def incident_row(incident: Incident) -> tuple[Any, ...]:
    """One table row of ``incidents list``."""
    task = (
        incident.task_id if incident.map_index in (None, -1) else f"{incident.task_id}[{incident.map_index}]"
    )
    confidence = f"{incident.confidence:.2f}" if incident.confidence is not None else None
    return (
        incident.id[:8],
        _format_time(incident.created_at),
        incident.dag_id,
        task,
        incident.run_id,
        incident.try_number,
        incident.status,
        incident.category,
        confidence,
        incident.occurrences,
    )


def _known_statuses() -> set[str]:
    from dag_healer.store import models

    return set(models.OPEN_STATUSES) | set(models.FINAL_STATUSES)


def _cmd_list(args: argparse.Namespace) -> int:
    from dag_healer.config import ConfigError
    from dag_healer.store import list_incidents, session_scope

    try:
        cfg = load_cli_config(args)
    except ConfigError as exc:
        error(str(exc))
        return 1
    statuses = _split_values(args.status)
    unknown = sorted(set(statuses) - _known_statuses())
    if unknown:
        warn(f"unknown status(es): {', '.join(unknown)} (known: {', '.join(sorted(_known_statuses()))})")
    dag_ids = set(_split_values(args.dag)) or None
    url = cfg.database_url
    try:
        with session_scope(url) as session:
            incidents = list_incidents(session, dag_ids=dag_ids, statuses=statuses or None, limit=args.limit)
    except Exception as exc:  # connection problems surface as many driver-specific types
        error(f"cannot read incidents from {mask_url(url)}: {type(exc).__name__}: {exc}")
        return 1
    if args.json:
        print(json.dumps([incident.to_dict(summary=True) for incident in incidents], indent=2))
        return 0
    if not incidents:
        print("No incidents found.")
        return 0
    print(render_table(HEADERS, [incident_row(incident) for incident in incidents]))
    return 0


def _configure(parser: argparse.ArgumentParser) -> None:
    add_config_option(parser)
    actions = parser.add_subparsers(dest="incidents_action", metavar="<action>")
    list_parser = actions.add_parser(
        "list", help="List incidents, newest first.", description="List incidents, newest first."
    )
    list_parser.add_argument(
        "--status", action="append", metavar="S", help="Only these statuses (repeat or comma-separate)."
    )
    list_parser.add_argument(
        "--dag", action="append", metavar="D", help="Only these DAG ids (repeat or comma-separate)."
    )
    list_parser.add_argument("--limit", type=int, default=50, help="Maximum rows (default: 50).")
    list_parser.add_argument("--json", action="store_true", help="Print JSON instead of a table.")
    add_config_option(list_parser)
    parser.set_defaults(_incidents_parser=parser)


def _run(args: argparse.Namespace) -> int:
    if getattr(args, "incidents_action", None) == "list":
        return _cmd_list(args)
    parser: argparse.ArgumentParser | None = getattr(args, "_incidents_parser", None)
    if parser is not None:
        parser.print_help()
    return 2


register_command(
    "incidents", help="Inspect incidents recorded by dag-healer.", handler=_run, configure=_configure
)

__all__ = ["HEADERS", "incident_row"]
