"""``GET /api/v1/setup``: what the human actions lead to in this deployment.

The views show it before an action is confirmed: who gets an alert (``notify``), how a fix is
delivered and what it may change (``github``, ``repo``, ``limits``) and the confidence the
model needs to decide on its own (``classification.min_confidence``). Read-only; no secret is
shown (the GitHub token only as ``token_configured``).
"""

from __future__ import annotations

from typing import Any

from fastapi import APIRouter, Depends

from dag_healer.api import auth, schemas, views
from dag_healer.api.deps import API_PREFIX, get_config
from dag_healer.config import HealerConfig

router = APIRouter(prefix=API_PREFIX, tags=["setup"])


@router.get("/setup", response_model=schemas.SetupView)
def get_setup(
    user: Any = Depends(auth.require_authenticated),
    cfg: HealerConfig = Depends(get_config),
) -> schemas.SetupView:
    """Alert recipients, fix delivery and limits, and the classification threshold."""
    return views.setup_view(cfg)
