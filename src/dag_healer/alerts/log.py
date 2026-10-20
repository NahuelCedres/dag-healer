"""The log channel of the alerts: one masked line per alert on the ``dag_healer.alerts`` logger.

The line carries the subject, incident id, run, classification, violated rule, a short
diagnosis and the link to the task-instance Healer tab. It never contains data rows.
Structured fields are attached to the record as ``record.healer_alert`` for log
shippers. ``waiting_human``, ``reported`` and ``repair_failed`` log at WARNING,
``repaired`` at INFO.
"""

from __future__ import annotations

import logging

from dag_healer.alerts import EVENT_META, AlertOutcome, RenderedAlert
from dag_healer.masking import mask_text

ALERT_LOGGER_NAME = "dag_healer.alerts"

_LEVELS = {"info": logging.INFO, "warning": logging.WARNING}


def alert_level(event: str) -> int:
    """Logging level used for ``event``."""
    return _LEVELS.get(EVENT_META.get(event, {}).get("level", "info"), logging.INFO)


def log_alert(rendered: RenderedAlert, *, logger: logging.Logger | None = None) -> AlertOutcome:
    """Write the alert line of ``rendered`` and return the ``log`` outcome (never raises)."""
    target = logger or logging.getLogger(ALERT_LOGGER_NAME)
    try:
        target.log(
            alert_level(rendered.event),
            "dag-healer alert: %s",
            rendered.log_message,
            extra={"healer_alert": dict(rendered.log_fields)},
        )
    except Exception as exc:  # noqa: BLE001 - alerting never raises
        error = mask_text(f"{type(exc).__name__}: {exc}")[:500]
        return AlertOutcome(
            channel="log", recipients=[], subject=rendered.subject, status="failed", error=error
        )
    return AlertOutcome(channel="log", recipients=[], subject=rendered.subject, status="sent")


__all__ = ["ALERT_LOGGER_NAME", "alert_level", "log_alert"]
