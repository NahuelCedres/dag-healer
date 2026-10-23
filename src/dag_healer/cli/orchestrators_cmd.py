"""``dag-healer orchestrators list``: the orchestrator types this installation can use.

Lists every type registered in the ``dag_healer.orchestrators`` entry-point group (and the
built-in ones), the distribution that provides it, its factory and whether it loads here (an
adapter whose module or optional dependency is missing is shown as unavailable, with the
reason and the ``pip install`` command when one helps). The configured ``orchestrator.type``
of ``healer.yaml`` is shown below the table. ``--json`` prints the same data as JSON.

Exit status: 0, or 1 when a type is registered by more than one distribution (an
installation error: the registry refuses to pick one).
"""

from __future__ import annotations

import argparse
import json
from typing import Any

from dag_healer.cli._output import add_config_option, config_arg, render_table, warn
from dag_healer.cli.main import register_command

AVAILABLE = "available"
UNAVAILABLE = "unavailable"
CONFLICT = "conflict"


def describe_types() -> list[dict[str, Any]]:
    """One row per registered orchestrator type (loading each factory to check it)."""
    from dag_healer.orchestrators.registry import (
        AdapterLoadError,
        conflict_message,
        conflicts,
        load_factory,
        registrations,
    )

    items = registrations()
    duplicated = conflicts(items)
    rows: list[dict[str, Any]] = []
    for type_ in sorted({item.type for item in items}):
        group = [item for item in items if item.type == type_]
        row: dict[str, Any] = {
            "type": type_,
            "display_name": None,
            "distribution": ", ".join(_provider(item) for item in group),
            "target": ", ".join(item.target for item in group),
            "status": AVAILABLE,
            "detail": None,
            "secret_options": [],
        }
        if type_ in duplicated:
            row.update(status=CONFLICT, detail=conflict_message(type_, group))
        else:
            try:
                factory = load_factory(type_)
            except AdapterLoadError as exc:
                row.update(status=UNAVAILABLE, detail=str(exc))
            else:
                row.update(
                    display_name=factory.display_name,
                    secret_options=[f"orchestrator.options.{name}" for name in factory.secret_options],
                )
        rows.append(row)
    return rows


def _provider(item: Any) -> str:
    return f"{item.distribution} (built-in)" if item.builtin else item.distribution


def configured(args: argparse.Namespace) -> dict[str, Any] | None:
    """The configured orchestrator, or ``None`` (with a warning) when the configuration is invalid."""
    from dag_healer.config import ConfigError, load_config
    from dag_healer.orchestrators.registry import AdapterLoadError, orchestrator_settings

    try:
        cfg = load_config(config_arg(args))
        settings = orchestrator_settings(cfg)
    except (ConfigError, AdapterLoadError) as exc:
        warn(f"cannot read the configured orchestrator: {exc}")
        return None
    return {
        "type": settings.type,
        "instance": settings.instance,
        "source": str(cfg.source_path) if cfg.source_path else None,
    }


def _print_table(rows: list[dict[str, Any]], current: dict[str, Any] | None) -> None:
    table = [
        (row["type"], row["display_name"], row["distribution"], row["target"], row["status"]) for row in rows
    ]
    print(render_table(("TYPE", "DISPLAY NAME", "PROVIDED BY", "FACTORY", "STATUS"), table))
    problems = [row for row in rows if row["detail"]]
    if problems:
        print()
        for row in problems:
            prefix = f"orchestrator type {row['type']!r}: "
            detail = str(row["detail"])
            print(f"{row['type']}: {detail[len(prefix) :] if detail.startswith(prefix) else detail}")
    if current is not None:
        where = current["source"] or "no healer.yaml found, defaults"
        print(f"\nConfigured: orchestrator.type {current['type']} (instance {current['instance']}; {where})")


def _cmd_list(args: argparse.Namespace) -> int:
    rows = describe_types()
    current = configured(args)
    if getattr(args, "json", False):
        print(json.dumps({"types": rows, "configured": current}, indent=2))
    else:
        _print_table(rows, current)
    return 1 if any(row["status"] == CONFLICT for row in rows) else 0


def _configure(parser: argparse.ArgumentParser) -> None:
    add_config_option(parser)
    actions = parser.add_subparsers(dest="orchestrators_action", metavar="<action>")
    listing = actions.add_parser(
        "list",
        help="List the orchestrator types this installation can use.",
        description="List the registered orchestrator types, where they come from and whether they load.",
    )
    listing.add_argument("--json", action="store_true", help="Print the list as JSON.")
    add_config_option(listing)
    parser.set_defaults(_orchestrators_parser=parser)


def _run(args: argparse.Namespace) -> int:
    if getattr(args, "orchestrators_action", None) == "list":
        return _cmd_list(args)
    parser: argparse.ArgumentParser | None = getattr(args, "_orchestrators_parser", None)
    if parser is not None:
        parser.print_help()
    return 2


register_command(
    "orchestrators",
    help="List the orchestrator types (adapters) this installation can use.",
    handler=_run,
    configure=_configure,
)

__all__ = ["AVAILABLE", "CONFLICT", "UNAVAILABLE", "configured", "describe_types"]
