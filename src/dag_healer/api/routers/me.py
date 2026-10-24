"""``GET /api/v1/me``: who is calling, where the Healer views live and which orchestrator is served."""

from __future__ import annotations

from typing import Any

from fastapi import APIRouter, Depends

from dag_healer.api import auth, schemas, views
from dag_healer.api.deps import API_PREFIX, get_adapter, get_config
from dag_healer.config import HealerConfig
from dag_healer.core.ports import OrchestratorAdapter

router = APIRouter(prefix=API_PREFIX, tags=["me"])


@router.get("/me", response_model=schemas.MeView)
def get_me(
    user: Any = Depends(auth.require_authenticated),
    cfg: HealerConfig = Depends(get_config),
    adapter: OrchestratorAdapter | None = Depends(get_adapter),
) -> schemas.MeView:
    """The current user name and the public base URL of the Healer views.

    ``orchestrator`` (words and capabilities) is added for any orchestrator but the default
    Airflow instance, so the answer for that one stays ``{user, base_url}``.
    """
    return schemas.MeView(
        user=auth.user_name(user),
        base_url=views.airflow_base_url(cfg),
        orchestrator=views.orchestrator_view(adapter),
    )
