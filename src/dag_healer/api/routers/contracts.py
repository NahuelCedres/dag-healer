"""Contract routes: list, show and approve the data contracts of the repository.

The API never writes contract files: an approval is queued as an ``approve_contract``
command that the worker (single writer of the repository) applies.

Reading a contract needs ``GET`` on ``DagAccessEntity.CODE``. Approving one needs ``PUT`` on
the DAG itself (``requires_access_dag("PUT")``, no sub-entity): the FAB auth manager defines
only ``can_read`` for "DAG Code", so ``PUT`` on ``CODE`` would be denied to every user,
Admin included, while editing the DAG is granted to Admin and Op and denied to Viewer.
"""

from __future__ import annotations

import logging
from typing import Any

from fastapi import APIRouter, Depends, HTTPException, status
from sqlalchemy import Select, select
from sqlalchemy.orm import Session

from dag_healer.api import auth, schemas, views
from dag_healer.api.deps import API_PREFIX, get_config, get_session, readable_dag_ids
from dag_healer.config import HealerConfig
from dag_healer.contracts.io import compute_source_hash
from dag_healer.masking import mask_text
from dag_healer.store import repo as store_repo
from dag_healer.store.models import COMMAND_PENDING, Command

log = logging.getLogger(__name__)

router = APIRouter(prefix=API_PREFIX, tags=["contracts"])

read_code = auth.require_dag_access("GET", "CODE")
edit_dag = auth.require_dag_access("PUT", None)


@router.get("/contracts", response_model=schemas.ContractList)
def list_contracts(
    user: Any = Depends(auth.require_authenticated),
    authorizer: auth.Authorizer = Depends(auth.get_authorizer),
    cfg: HealerConfig = Depends(get_config),
    session: Session = Depends(get_session),
) -> schemas.ContractList:
    """Contracts of the DAGs the user can read."""
    records = views.load_contracts(cfg)
    readable = readable_dag_ids(authorizer, user, records)
    pending = views.pending_approval_dags(session)
    return schemas.ContractList(
        contracts=[
            views.contract_view(
                records[dag_id],
                can_approve=authorizer.can_edit_dag(user, dag_id, None),
                pending_approval=dag_id in pending,
            )
            for dag_id in sorted(records)
            if dag_id in readable
        ]
    )


@router.get("/dags/{dag_id}/contract", response_model=schemas.ContractView)
def get_contract(
    dag_id: str,
    user: Any = Depends(read_code),
    authorizer: auth.Authorizer = Depends(auth.get_authorizer),
    cfg: HealerConfig = Depends(get_config),
    session: Session = Depends(get_session),
) -> schemas.ContractView:
    """The contract of one DAG (404 when it has none)."""
    record = views.load_contracts(cfg).get(dag_id)
    if record is None:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Contract not found")
    return views.contract_view(
        record,
        can_approve=authorizer.can_edit_dag(user, dag_id, None),
        pending_approval=dag_id in views.pending_approval_dags(session),
    )


@router.post(
    "/dags/{dag_id}/contract/approve",
    status_code=status.HTTP_202_ACCEPTED,
    response_model=schemas.CommandAccepted,
)
def approve_contract(
    dag_id: str,
    body: schemas.NoteBody | None = None,
    user: Any = Depends(edit_dag),
    cfg: HealerConfig = Depends(get_config),
    session: Session = Depends(get_session),
) -> schemas.CommandAccepted:
    """Queue the approval of a draft (or stale) contract.

    The command carries the ``source_hash`` of the reviewed file so that the worker can
    refuse to approve a file that changed after the user looked at it.
    """
    record = views.load_contracts(cfg).get(dag_id)
    if record is None:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Contract not found")
    if record.status == "approved":
        raise HTTPException(status_code=status.HTTP_409_CONFLICT, detail="The contract is already approved")
    query = select_pending_approval(dag_id)
    existing = session.scalars(query).first()
    if existing is not None:
        return schemas.CommandAccepted(command_id=existing.id, status=existing.status)
    note = body.note.strip() if body is not None and body.note else None
    actor = auth.user_name(user)
    command = store_repo.enqueue_command(
        session,
        kind="approve_contract",
        dag_id=dag_id,
        payload={
            "path": record.relpath,
            "source_hash": compute_source_hash(record.contract),
            "note": mask_text(note) if note else None,
        },
        actor=actor,
    )
    session.commit()
    log.info("dag-healer: %s queued the approval of the %s contract (command %s)", actor, dag_id, command.id)
    return schemas.CommandAccepted(command_id=command.id, status=command.status)


def select_pending_approval(dag_id: str) -> Select[tuple[Command]]:
    """Query for the pending ``approve_contract`` command of ``dag_id``."""
    return (
        select(Command)
        .where(
            Command.kind == "approve_contract", Command.dag_id == dag_id, Command.status == COMMAND_PENDING
        )
        .order_by(Command.id)
    )
