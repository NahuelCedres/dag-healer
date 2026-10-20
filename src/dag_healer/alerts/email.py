"""Alert emails over SMTP (stdlib ``smtplib`` + ``email.message``).

The message is ``multipart/alternative`` (plain text first, HTML second) with
``Message-ID``, ``Date``, ``Auto-Submitted: auto-generated`` and a per-incident
``References`` header so that mail clients thread all alerts of one incident.

Connection: ``smtplib.SMTP`` (``smtplib.SMTP_SSL`` on port 465), optional STARTTLS
(``notify.email.starttls``) and optional login (``username``/``password``), 10 s timeout.
:func:`send_email` never raises: failures become ``failed`` outcomes whose error text
is masked (no addresses or numbers from server replies leak into the store).
"""

from __future__ import annotations

import logging
import re
import smtplib
import ssl
from collections.abc import Callable
from datetime import datetime, timezone
from email.message import EmailMessage
from email.utils import format_datetime, getaddresses, make_msgid, parseaddr
from typing import Any

from dag_healer.alerts import AlertOutcome, AlertStatus, RenderedAlert
from dag_healer.masking import mask_text

logger = logging.getLogger(__name__)

DEFAULT_TIMEOUT = 10.0
SMTPS_PORT = 465
FALLBACK_DOMAIN = "dag-healer.local"
MAX_ERROR_CHARS = 500

_DOMAIN_CHARS = re.compile(r"[^A-Za-z0-9.-]")
_ID_CHARS = re.compile(r"[^A-Za-z0-9._-]")

SmtpFactory = Callable[..., Any]


def _sender_domain(sender: str) -> str:
    address = parseaddr(sender or "")[1]
    domain = _DOMAIN_CHARS.sub("", address.rpartition("@")[2]).strip(".")
    return domain or FALLBACK_DOMAIN


def _addresses(values: list[str]) -> list[str]:
    """Bare addresses (``Name <a@b>`` -> ``a@b``) of the configured recipients."""
    return [address for _name, address in getaddresses(values) if address]


def build_message(email_cfg: Any, rendered: RenderedAlert, *, now: datetime | None = None) -> EmailMessage:
    """Build the ``multipart/alternative`` alert email for ``rendered``."""
    sender = str(getattr(email_cfg, "sender", "") or "").strip()
    recipients = [str(item).strip() for item in (getattr(email_cfg, "to", None) or []) if str(item).strip()]
    domain = _sender_domain(sender)
    incident_id = _ID_CHARS.sub("", rendered.incident_id or "")

    message = EmailMessage()
    message["Subject"] = rendered.subject
    message["From"] = sender
    message["To"] = ", ".join(recipients)
    message["Date"] = format_datetime(now or datetime.now(timezone.utc))
    message["Message-ID"] = make_msgid(
        idstring=f"{_ID_CHARS.sub('', rendered.event)}.{incident_id[:12] or 'alert'}", domain=domain
    )
    if incident_id:
        message["References"] = f"<incident.{incident_id}@{domain}>"
        message["X-Dag-Healer-Incident"] = incident_id
    message["X-Dag-Healer-Event"] = rendered.event
    message["Auto-Submitted"] = "auto-generated"
    message["X-Auto-Response-Suppress"] = "All"
    message.set_content(rendered.text, subtype="plain", charset="utf-8")
    message.add_alternative(rendered.html, subtype="html", charset="utf-8")
    return message


def _default_factory(host: str, port: int, *, timeout: float = DEFAULT_TIMEOUT) -> smtplib.SMTP:
    """``smtplib.SMTP`` (or ``SMTP_SSL`` on port 465) connected to ``host:port``."""
    if port == SMTPS_PORT:
        return smtplib.SMTP_SSL(host, port, timeout=timeout, context=ssl.create_default_context())
    return smtplib.SMTP(host, port, timeout=timeout)


def _error_text(exc: BaseException | str) -> str:
    text = exc if isinstance(exc, str) else f"{type(exc).__name__}: {exc}"
    text = mask_text(text).strip()
    return text if len(text) <= MAX_ERROR_CHARS else text[: MAX_ERROR_CHARS - 1] + "…"


def _close(smtp: Any) -> None:
    try:
        smtp.quit()
    except Exception:  # noqa: BLE001 - the connection may already be gone
        try:
            smtp.close()
        except Exception:  # noqa: BLE001
            pass


def send_email(
    email_cfg: Any,
    rendered: RenderedAlert,
    *,
    smtp_factory: SmtpFactory | None = None,
    timeout: float = DEFAULT_TIMEOUT,
) -> AlertOutcome:
    """Send ``rendered`` to ``notify.email.to``; return the outcome (never raises).

    ``smtp_factory(host, port, timeout=...)`` must return an ``smtplib.SMTP``-like object
    (``ehlo``, ``starttls``, ``login``, ``send_message``, ``quit``).
    """
    recipients = [str(item).strip() for item in (getattr(email_cfg, "to", None) or []) if str(item).strip()]

    def outcome(status: AlertStatus, error: str | None = None) -> AlertOutcome:
        return AlertOutcome(
            channel="email", recipients=recipients, subject=rendered.subject, status=status, error=error
        )

    sender = str(getattr(email_cfg, "sender", "") or "").strip()
    envelope_to = _addresses(recipients)
    if not envelope_to:
        return outcome("skipped", "no recipients configured in notify.email.to")
    if not sender:
        return outcome("failed", "notify.email.from is empty")

    host = str(getattr(email_cfg, "host", "") or "")
    port = int(getattr(email_cfg, "port", 25) or 25)
    smtp = None
    try:
        message = build_message(email_cfg, rendered)
        factory = smtp_factory or _default_factory
        smtp = factory(host, port, timeout=timeout)
        smtp.ehlo()
        if getattr(email_cfg, "starttls", False):
            smtp.starttls(context=ssl.create_default_context())
            smtp.ehlo()
        username = getattr(email_cfg, "username", None)
        if username:
            smtp.login(username, getattr(email_cfg, "password", None) or "")
        refused = smtp.send_message(message, from_addr=parseaddr(sender)[1] or sender, to_addrs=envelope_to)
    except Exception as exc:  # noqa: BLE001 - alerting never raises
        error = _error_text(exc)
        logger.warning(
            "Alert email for event %s (incident %s) via %s:%s failed: %s",
            rendered.event,
            rendered.incident_id or "-",
            host,
            port,
            error,
        )
        return outcome("failed", error)
    finally:
        if smtp is not None:
            _close(smtp)

    error = None
    if refused:
        error = f"{len(refused)} of {len(envelope_to)} recipients were refused by the SMTP server"
        logger.warning("Alert email for incident %s: %s", rendered.incident_id or "-", error)
    logger.info(
        "Alert email sent: event=%s incident=%s recipients=%d",
        rendered.event,
        rendered.incident_id or "-",
        len(envelope_to) - len(refused or {}),
    )
    return outcome("sent", error)


__all__ = ["DEFAULT_TIMEOUT", "SmtpFactory", "build_message", "send_email"]
