"""``dag-healer contracts list|approve``: review and approve data contracts.

The YAML files under the contracts directory are the source of truth. Approving writes
the YAML (status, approver, timestamp and source hash) and then, best effort, caches the
state in the incident store; an unreachable store never makes the approval fail.
"""

from __future__ import annotations

import argparse
import logging
from datetime import datetime
from pathlib import Path
from typing import TYPE_CHECKING

from dag_healer.cli._output import add_config_option, error, load_cli_config, render_table, warn
from dag_healer.cli.main import register_command

if TYPE_CHECKING:
    from dag_healer.config import HealerConfig
    from dag_healer.contracts.schema import Contract

log = logging.getLogger(__name__)


def _format_time(value: datetime | None) -> str | None:
    if value is None:
        return None
    return value.isoformat(timespec="seconds")


def _load_config_or_report(args: argparse.Namespace) -> HealerConfig | None:
    from dag_healer.config import ConfigError

    try:
        return load_cli_config(args)
    except ConfigError as exc:
        error(str(exc))
        return None


def contract_rows(contracts: list[Contract]) -> list[tuple[object, ...]]:
    """Table rows for ``contracts list`` (stale only applies to approved contracts)."""
    from dag_healer.contracts.io import is_stale

    rows: list[tuple[object, ...]] = []
    for contract in sorted(contracts, key=lambda item: item.dag_id):
        stale = is_stale(contract) if contract.is_approved else None
        rows.append(
            (
                contract.dag_id,
                contract.status,
                len(contract.datasets),
                contract.approved_by,
                _format_time(contract.approved_at),
                stale,
            )
        )
    return rows


def _cmd_list(args: argparse.Namespace) -> int:
    from dag_healer.contracts.io import list_contracts

    cfg = _load_config_or_report(args)
    if cfg is None:
        return 1
    directory = cfg.contracts_dir
    contracts = list_contracts(directory)
    if not contracts:
        print(f"No contracts in {directory} (run: dag-healer init).")
        return 0
    rows = contract_rows(contracts)
    print(render_table(("DAG_ID", "STATUS", "DATASETS", "APPROVED_BY", "APPROVED_AT", "STALE"), rows))
    if any(row[5] for row in rows):
        print(
            "\nStale contracts were edited after approval; approve them again "
            "(dag-healer contracts approve <dag_id> --by NAME)."
        )
    drafts = [row[0] for row in rows if row[1] == "draft"]
    if drafts:
        print(f"\nDrafts are not enforced until approved: {', '.join(str(d) for d in drafts)}")
    return 0


def sync_contract_state(cfg: HealerConfig, contract: Contract, path: Path) -> str | None:
    """Cache the contract state in the store; returns an error message instead of raising."""
    try:
        from dag_healer.store import session_scope, upsert_contract_state

        with session_scope(cfg.database_url) as session:
            upsert_contract_state(
                session,
                dag_id=contract.dag_id,
                status=contract.status,
                path=str(path),
                approved_by=contract.approved_by,
                approved_at=contract.approved_at,
                source_hash=contract.source_hash,
            )
    except Exception as exc:  # the store is optional for approvals
        first_line = str(exc).strip().splitlines()[0] if str(exc).strip() else ""
        return f"{type(exc).__name__}: {first_line}"[:300]
    return None


def _cmd_approve(args: argparse.Namespace) -> int:
    from dag_healer.contracts.io import (
        approve_contract,
        contract_path,
        is_stale,
        load_contract,
        save_contract,
    )
    from dag_healer.contracts.schema import ContractError

    cfg = _load_config_or_report(args)
    if cfg is None:
        return 1
    approver = (args.by or "").strip()
    if not approver:
        error("--by NAME must not be empty")
        return 1
    try:
        path = contract_path(cfg.contracts_dir, args.dag_id)
    except ValueError as exc:
        error(str(exc))
        return 1
    if not path.is_file():
        error(f"no contract for DAG '{args.dag_id}' at {path} (run: dag-healer init)")
        return 1
    try:
        contract = load_contract(path)
    except ContractError as exc:
        error(str(exc))
        return 1

    if contract.is_approved and not is_stale(contract):
        print(
            f"Contract for '{contract.dag_id}' is already approved by {contract.approved_by} "
            f"at {_format_time(contract.approved_at)}; nothing to do."
        )
        approved = contract
    else:
        if not contract.datasets:
            warn(f"the contract for '{contract.dag_id}' has no datasets; approving it enforces nothing")
        approved = approve_contract(contract, approver)
        save_contract(approved, path)
        print(
            f"Approved contract for '{approved.dag_id}' ({len(approved.datasets)} dataset(s)) "
            f"by {approved.approved_by} at {_format_time(approved.approved_at)}: {path}"
        )

    problem = sync_contract_state(cfg, approved, path)
    if problem:
        warn(f"could not record the approval in the store ({problem}); the YAML file is the source of truth")
    return 0


def _configure(parser: argparse.ArgumentParser) -> None:
    add_config_option(parser)
    actions = parser.add_subparsers(dest="contracts_action", metavar="<action>")
    list_parser = actions.add_parser(
        "list", help="List contracts with their status.", description="List contracts with their status."
    )
    add_config_option(list_parser)
    approve_parser = actions.add_parser(
        "approve",
        help="Approve the contract of a DAG so it is enforced.",
        description="Approve the contract of a DAG so it is enforced.",
    )
    approve_parser.add_argument("dag_id", help="DAG whose contract is approved.")
    approve_parser.add_argument("--by", required=True, metavar="NAME", help="Who approves (recorded).")
    add_config_option(approve_parser)
    parser.set_defaults(_contracts_parser=parser)


def _run(args: argparse.Namespace) -> int:
    action = getattr(args, "contracts_action", None)
    if action == "list":
        return _cmd_list(args)
    if action == "approve":
        return _cmd_approve(args)
    parser: argparse.ArgumentParser | None = getattr(args, "_contracts_parser", None)
    if parser is not None:
        parser.print_help()
    return 2


register_command("contracts", help="List and approve data contracts.", handler=_run, configure=_configure)

__all__ = ["contract_rows", "sync_contract_state"]
