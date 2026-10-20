"""``GET /api/v1/me``: who is calling and where Airflow lives."""

from __future__ import annotations

from typing import Any

from fastapi import APIRouter, Depends

from dag_healer.api import auth, schemas, views
from dag_healer.api.deps import API_PREFIX, get_config
from dag_healer.config import HealerConfig

router = APIRouter(prefix=API_PREFIX, tags=["me"])


@router.get("/me", response_model=schemas.MeView)
def get_me(
    user: Any = Depends(auth.require_authenticated),
    cfg: HealerConfig = Depends(get_config),
) -> schemas.MeView:
    """The current Airflow user name and the public Airflow base URL."""
    return schemas.MeView(user=auth.user_name(user), base_url=views.airflow_base_url(cfg))
