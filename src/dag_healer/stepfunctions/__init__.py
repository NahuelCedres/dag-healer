"""The AWS Step Functions reference adapter (section 9 of ``docs/architecture/orchestrators.md``).

:data:`FACTORY` is the ``stepfunctions`` entry point of the ``dag_healer.orchestrators`` group.
This module is light on purpose (loading a ``healer.yaml`` imports it for the secret options):
it imports neither boto3 nor the adapter; the options model and the adapter are imported on
first use. Install the ``aws`` extra (``pip install 'dag-healer[aws]'``) to use it.
"""

from __future__ import annotations

from collections.abc import Callable
from datetime import datetime
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    import pydantic

    from dag_healer.config import HealerConfig
    from dag_healer.stepfunctions.adapter import StepFunctionsAdapter

SFN_TYPE = "stepfunctions"
DISPLAY_NAME = "AWS Step Functions"
FACTORY_TARGET = "dag_healer.stepfunctions:FACTORY"


class StepFunctionsFactory:
    """:class:`~dag_healer.orchestrators.registry.AdapterFactory` of the ``stepfunctions`` type."""

    type = SFN_TYPE
    display_name = DISPLAY_NAME
    secret_options: tuple[str, ...] = ("role_external_id",)

    @property
    def options_model(self) -> type[pydantic.BaseModel]:
        """:class:`~dag_healer.stepfunctions.options.StepFunctionsOptions` (imported on first use)."""
        from dag_healer.stepfunctions.options import StepFunctionsOptions

        return StepFunctionsOptions

    def create(
        self,
        cfg: HealerConfig,
        *,
        client: Any | None = None,
        clock: Callable[[], datetime] | None = None,
    ) -> StepFunctionsAdapter:
        """No I/O. ``client``: ``AwsClients``, a ``client_factory(service)`` callable or an object
        with a ``client_factory`` method (``OfflineAws``); ``clock`` returns aware UTC datetimes.

        The ``orchestrator:`` settings apply when the configuration selects this type; otherwise
        (tests building the adapter from a plain configuration) the instance is
        ``stepfunctions`` with default options.
        """
        from dag_healer.orchestrators.registry import orchestrator_settings, parse_options
        from dag_healer.stepfunctions.adapter import StepFunctionsAdapter
        from dag_healer.stepfunctions.options import StepFunctionsOptions

        settings = orchestrator_settings(cfg)
        if settings.type in (SFN_TYPE, FACTORY_TARGET):
            options = parse_options(self, settings.options)
            instance = settings.instance
        else:
            options, instance = StepFunctionsOptions(), SFN_TYPE
        return StepFunctionsAdapter(cfg, options=options, instance=instance, clients=client, clock=clock)


FACTORY = StepFunctionsFactory()

__all__ = ["DISPLAY_NAME", "FACTORY", "FACTORY_TARGET", "SFN_TYPE", "StepFunctionsFactory"]
