"""Incident alerts: a log line always, plus an email when ``notify.email`` is configured.

:func:`notify` is the entry point used by the worker::

    outcomes = notify(cfg, incident.to_dict(), "reported", extra={"violations": [...]})
    for outcome in outcomes:
        repo.record_alert(session, incident_id=incident.id, **outcome.as_record())

It never raises. Events missing from ``notify.events`` produce ``skipped`` outcomes.
Subjects and bodies come from the Jinja2 templates in ``alerts/templates``
(``<event>.txt.j2`` defines ``subject`` and the plain-text body, ``<event>.html.j2`` the
HTML body); a failing template falls back to a minimal built-in text so the alert still
goes out.

Only masked content is rendered: free text (error summary, diagnosis, evidence,
violation messages, notes) goes through :func:`dag_healer.masking.mask_text`, and a data
sample is only shown when it is flagged ``masked: true`` (the shape produced by
``agent.context``); its string cells are scrubbed once more, leaving masking hashes
(``h:<hex>``) and redactions (``***``) intact.

``extra`` overrides incident fields and adds alert-only details. Recognised keys:
``reason`` (why a human is needed), ``error`` (why a repair failed), ``notes``
(list of str), ``violations`` (list of ``Violation`` models or dicts), ``sample``,
``violated_rule``, ``diagnosis``, ``category``, ``confidence``, ``fix_summary``,
``branch``, ``test_path``, ``changed_files``, ``pr_url``, ``pr_md_path``, ``attempts``.
"""

from __future__ import annotations

import html
import json
import logging
import re
from collections.abc import Callable, Iterable, Mapping
from dataclasses import asdict, dataclass, field
from datetime import datetime, timezone
from functools import lru_cache
from pathlib import Path
from typing import TYPE_CHECKING, Any, Literal

from dag_healer import links
from dag_healer.config import ALERT_EVENTS
from dag_healer.masking import HASH_LENGTH, HASH_PREFIX, REDACTED, mask_text

if TYPE_CHECKING:
    from jinja2 import Environment

logger = logging.getLogger(__name__)

AlertEvent = Literal["waiting_human", "reported", "repaired", "repair_failed"]
AlertChannel = Literal["email", "log"]
AlertStatus = Literal["sent", "failed", "skipped"]

TEMPLATES_DIR = Path(__file__).resolve().parent / "templates"
SUBJECT_PREFIX = "[dag-healer]"

MAX_SUBJECT_CHARS = 200
MAX_ERROR_CHARS = 2000
MAX_TEXT_CHARS = 4000
MAX_VALUE_CHARS = 120
MAX_CELL_CHARS = 60
MAX_TEXT_CELL_CHARS = 24
MAX_SAMPLE_ROWS = 20
MAX_SAMPLE_COLUMNS = 20
MAX_VIOLATIONS = 10
MAX_LIST_ITEMS = 10
MAX_CHANGED_FILES = 20
MAX_OUTCOME_ERROR_CHARS = 500
DEFAULT_SAMPLE_ROWS = 5

#: Per-event presentation details shared by the templates.
EVENT_META: dict[str, dict[str, str]] = {
    "waiting_human": {"title": "Human decision needed", "accent": "#7c3aed", "level": "warning"},
    "reported": {"title": "Data issue reported", "accent": "#d97706", "level": "warning"},
    "repaired": {"title": "Fix proposed", "accent": "#059669", "level": "info"},
    "repair_failed": {"title": "Repair failed", "accent": "#dc2626", "level": "warning"},
}

CATEGORY_STYLE: dict[str, dict[str, str]] = {
    "data": {"label": "DATA", "fg": "#92400e", "bg": "#fef3c7"},
    "code": {"label": "CODE", "fg": "#1e40af", "bg": "#dbeafe"},
    "ambiguous": {"label": "AMBIGUOUS", "fg": "#5b21b6", "bg": "#ede9fe"},
}
_UNCLASSIFIED_STYLE = {"label": "UNCLASSIFIED", "fg": "#374151", "bg": "#e5e7eb"}

_HASH_TOKEN = re.compile(rf"{re.escape(HASH_PREFIX)}[0-9a-f]{{{HASH_LENGTH}}}")
_DIFF_HEADER = re.compile(r"^diff --git a/(?P<a>.+?) b/(?P<b>.+)$", re.MULTILINE)
_WHITESPACE = re.compile(r"\s+")
_BLANK_RUNS = re.compile(r"\n{3,}")
_IDENTIFIER_KEYS = ("id", "dag_id", "run_id", "task_id", "map_index", "try_number", "status", "category")


# --------------------------------------------------------------------------- results


@dataclass
class AlertOutcome:
    """The result of one notification attempt on one channel."""

    channel: AlertChannel
    recipients: list[str]
    subject: str
    status: AlertStatus
    error: str | None = None

    def to_dict(self) -> dict[str, Any]:
        """Return a JSON-safe dict."""
        return asdict(self)

    def as_record(self) -> dict[str, Any]:
        """Keyword arguments for :func:`dag_healer.store.repo.record_alert` (minus ``incident_id``)."""
        return {
            "channel": self.channel,
            "recipients": list(self.recipients),
            "subject": self.subject,
            "status": self.status,
            "error": self.error,
        }


@dataclass
class RenderedAlert:
    """A rendered alert: email subject and bodies plus the one-line log message."""

    event: str
    subject: str
    text: str
    html: str
    log_message: str
    log_fields: dict[str, Any] = field(default_factory=dict)
    incident_id: str | None = None
    fallback: bool = False


# --------------------------------------------------------------------------- helpers


def _truncate(text: str, limit: int) -> str:
    if len(text) <= limit:
        return text
    return text[: max(0, limit - 1)].rstrip() + "…"


def _masked(value: Any, limit: int = MAX_TEXT_CHARS) -> str | None:
    """Masked, stripped and truncated free text (``None`` for empty values)."""
    if value is None:
        return None
    text = mask_text(value).strip()
    if not text:
        return None
    return _truncate(text, limit)


def _masked_list(values: Any, limit: int = MAX_LIST_ITEMS) -> list[str]:
    if values is None:
        return []
    if isinstance(values, str):
        values = [values]
    if not isinstance(values, Iterable):
        values = [values]
    items = [item for item in (_masked(value, 500) for value in values) if item]
    return items[:limit]


def _short_value(value: Any) -> str | None:
    """Render an ``expected``/``actual`` value of a violation as short masked text."""
    if value is None:
        return None
    if isinstance(value, str):
        text = value
    else:
        try:
            text = json.dumps(value, default=str, ensure_ascii=False)
        except (TypeError, ValueError):
            text = str(value)
    return _masked(text, MAX_VALUE_CHARS)


def _int(value: Any, default: int) -> int:
    try:
        return int(value)
    except (TypeError, ValueError):
        return default


def _float(value: Any) -> float | None:
    try:
        number = float(value)
    except (TypeError, ValueError):
        return None
    return number if number == number else None  # NaN -> None


def _identifier(value: Any) -> str:
    """Single-line identifier text (no CR/LF, collapsed whitespace)."""
    return _WHITESPACE.sub(" ", str(value or "")).strip()


def task_label(incident: Mapping[str, Any]) -> str:
    """``dag_id.task_id`` plus ``[map_index]`` for mapped task instances."""
    label = f"{_identifier(incident.get('dag_id')) or '?'}.{_identifier(incident.get('task_id')) or '?'}"
    map_index = _int(incident.get("map_index"), -1)
    if map_index >= 0:
        label += f"[{map_index}]"
    return label


def _normalize_violation(item: Any) -> dict[str, Any] | None:
    """Turn a ``Violation`` model, dict or string into a masked display dict."""
    if item is None:
        return None
    if hasattr(item, "model_dump"):
        item = item.model_dump()
    if isinstance(item, str):
        item = {"rule": item}
    if not isinstance(item, Mapping):
        return None
    rule = _identifier(item.get("rule")) or "rule"
    dataset = _identifier(item.get("dataset")) or None
    column = _identifier(item.get("column")) or None
    where = ".".join(part for part in (dataset, column) if part) or None
    row_indexes = item.get("row_indexes")
    return {
        "rule": rule,
        "dataset": dataset,
        "column": column,
        "where": where,
        "message": _masked(item.get("message"), 500),
        "expected": _short_value(item.get("expected")),
        "actual": _short_value(item.get("actual")),
        "rows": len(row_indexes) if isinstance(row_indexes, list) else 0,
    }


def _normalize_violations(values: Any) -> tuple[list[dict[str, Any]], int]:
    if not values:
        return [], 0
    if not isinstance(values, (list, tuple)):
        values = [values]
    normalized = [v for v in (_normalize_violation(item) for item in values) if v]
    return normalized[:MAX_VIOLATIONS], max(0, len(normalized) - MAX_VIOLATIONS)


def _mask_cell(value: Any) -> str:
    """Render one sample cell; string cells are scrubbed again (hash tokens are kept)."""
    if value is None:
        return "null"
    if isinstance(value, bool):
        return "true" if value else "false"
    if isinstance(value, str):
        text = value if (value == REDACTED or _HASH_TOKEN.fullmatch(value)) else mask_text(value)
    elif isinstance(value, (int, float)):
        text = str(value)
    else:
        text = mask_text(json.dumps(value, default=str, ensure_ascii=False))
    return _truncate(_WHITESPACE.sub(" ", text), MAX_CELL_CHARS)


def _text_table(columns: list[str], rows: list[list[str]]) -> list[str]:
    """Fixed-width plain-text table lines."""
    widths = [
        min(MAX_TEXT_CELL_CHARS, max([len(col)] + [len(row[i]) for row in rows]))
        for i, col in enumerate(columns)
    ]

    def line(cells: list[str]) -> str:
        return " | ".join(
            _truncate(cell, width).ljust(width) for cell, width in zip(cells, widths, strict=False)
        ).rstrip()

    lines = [line(columns), "-+-".join("-" * width for width in widths)]
    lines.extend(line(row) for row in rows)
    return lines


def _normalize_sample(sample: Any, max_rows: int) -> dict[str, Any] | None:
    """Masked sample for display, or ``None``. Unflagged samples are never rendered."""
    if not isinstance(sample, Mapping):
        return None
    dataset = _identifier(sample.get("dataset")) or None
    if sample.get("masked") is not True:
        return {"dataset": dataset, "omitted": "The sample is not flagged as masked, so it is not included."}
    raw_rows = sample.get("rows") or []
    if not isinstance(raw_rows, list):
        raw_rows = []
    raw_columns = sample.get("columns") or []
    keys: list[Any] = [
        column.get("name") if isinstance(column, Mapping) else column
        for column in (raw_columns if isinstance(raw_columns, (list, tuple)) else [])
    ]
    if not keys and raw_rows and isinstance(raw_rows[0], Mapping):
        keys = list(raw_rows[0])
    if not keys:
        return None
    total_columns = len(keys)
    keys = keys[:MAX_SAMPLE_COLUMNS]
    columns = [_truncate(_identifier(key), MAX_CELL_CHARS) for key in keys]
    limit = max(0, min(max_rows, MAX_SAMPLE_ROWS))
    rows: list[list[str]] = []
    for raw in raw_rows[:limit]:
        if isinstance(raw, Mapping):
            values = [raw.get(key) for key in keys]
        elif isinstance(raw, (list, tuple)):
            values = list(raw[: len(columns)]) + [None] * max(0, len(columns) - len(raw))
        else:
            continue
        rows.append([_mask_cell(value) for value in values])
    return {
        "dataset": dataset,
        "columns": columns,
        "rows": rows,
        "hidden_columns": max(0, total_columns - len(columns)),
        "hidden_rows": max(0, len(raw_rows) - len(rows)),
        "text_lines": _text_table(columns, rows),
        "omitted": None,
    }


def _changed_files(merged: Mapping[str, Any]) -> list[str]:
    explicit = merged.get("changed_files")
    if isinstance(explicit, (list, tuple)):
        return [_identifier(path) for path in explicit if path][:MAX_CHANGED_FILES]
    diff = merged.get("diff")
    if not isinstance(diff, str) or not diff:
        return []
    files: list[str] = []
    for match in _DIFF_HEADER.finditer(diff):
        path = match.group("b").strip()
        if path and path not in files:
            files.append(path)
    return files[:MAX_CHANGED_FILES]


def _sample_rows_setting(cfg: Any) -> int:
    return _int(
        getattr(getattr(cfg, "masking", None), "sample_rows", DEFAULT_SAMPLE_ROWS), DEFAULT_SAMPLE_ROWS
    )


def build_context(
    cfg: Any, incident: Mapping[str, Any], event: str, extra: Mapping[str, Any] | None = None
) -> dict[str, Any]:
    """Build the complete (masked) template context of an alert."""
    merged: dict[str, Any] = {**dict(incident or {}), **dict(extra or {})}
    incident_id = _identifier(merged.get("id")) or None
    category = (_identifier(merged.get("category")).lower() or None) if merged.get("category") else None
    if category is None and event == "reported":
        category = "data"
    style = CATEGORY_STYLE.get(category or "", _UNCLASSIFIED_STYLE)
    confidence = _float(merged.get("confidence"))
    map_index = _int(merged.get("map_index"), -1)

    violations, violations_more = _normalize_violations(merged.get("violations"))
    violated_rule = _normalize_violation(merged.get("violated_rule"))
    if violated_rule is None and violations and event == "reported":
        violated_rule = violations[0]

    ti_links = links.incident_links(cfg, {**merged, "map_index": map_index})
    pr_url = links.pr_link(cfg, merged)
    pr_kind = None
    if pr_url:
        pr_kind = "pull_request" if str(merged.get("pr_url") or "").strip() == pr_url else "pr_md"

    meta = EVENT_META.get(event, {"title": event, "accent": "#374151", "level": "info"})
    return {
        "event": event,
        "event_title": meta["title"],
        "accent": meta["accent"],
        "incident": {
            "id": incident_id or "",
            "short_id": (incident_id or "")[:8],
            "dag_id": _identifier(merged.get("dag_id")),
            "run_id": _identifier(merged.get("run_id")),
            "task_id": _identifier(merged.get("task_id")),
            "map_index": map_index,
            "try_number": _int(merged.get("try_number"), 1),
            "task_label": task_label({**merged, "map_index": map_index}),
            "status": _identifier(merged.get("status")) or event,
            "category": category,
            "category_label": style["label"],
            "category_fg": style["fg"],
            "category_bg": style["bg"],
            "confidence": confidence,
            "confidence_pct": f"{round(confidence * 100)}%" if confidence is not None else None,
            "source": _identifier(merged.get("source")) or None,
            "occurrences": _int(merged.get("occurrences"), 1),
            "attempts": _int(merged.get("attempts"), 0),
            "error_summary": _masked(merged.get("error_summary"), MAX_ERROR_CHARS),
            "diagnosis": _masked(merged.get("diagnosis")),
            "evidence": _masked_list(merged.get("evidence")),
            "fix_summary": _masked(merged.get("fix_summary"), 1000),
            "test_path": _identifier(merged.get("test_path")) or None,
            "branch": _identifier(merged.get("branch")) or None,
            "created_at": _identifier(merged.get("created_at")) or None,
        },
        "violated_rule": violated_rule,
        "violations": violations,
        "violations_more": violations_more,
        "sample": _normalize_sample(merged.get("sample"), _sample_rows_setting(cfg)),
        "reason": _masked(merged.get("reason"), 1000),
        "error": _masked(merged.get("error"), MAX_ERROR_CHARS),
        "notes": _masked_list(merged.get("notes")),
        "changed_files": _changed_files(merged),
        "links": {**ti_links, "pr": pr_url, "pr_kind": pr_kind},
        "generated_at": datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC"),
    }


# --------------------------------------------------------------------------- rendering


@lru_cache(maxsize=1)
def template_environment() -> Environment:
    """The Jinja2 environment of the alert templates (HTML templates are autoescaped)."""
    from jinja2 import Environment, FileSystemLoader, StrictUndefined

    return Environment(
        loader=FileSystemLoader(str(TEMPLATES_DIR)),
        autoescape=lambda name: bool(name) and name.endswith(".html.j2"),
        undefined=StrictUndefined,
        trim_blocks=True,
        lstrip_blocks=True,
        keep_trailing_newline=True,
    )


def _clean_subject(subject: str) -> str:
    return _truncate(_WHITESPACE.sub(" ", subject).strip(), MAX_SUBJECT_CHARS)


def _tidy_text(text: str) -> str:
    """Strip trailing spaces and collapse runs of blank lines left by empty sections."""
    lines = [line.rstrip() for line in text.replace("\r\n", "\n").split("\n")]
    return _BLANK_RUNS.sub("\n\n", "\n".join(lines)).strip() + "\n"


def _log_parts(context: Mapping[str, Any], subject: str) -> tuple[str, dict[str, Any]]:
    inc = context["incident"]
    rule = context.get("violated_rule")
    fields: dict[str, Any] = {
        "event": context["event"],
        "incident_id": inc["id"],
        "dag_id": inc["dag_id"],
        "task_id": inc["task_id"],
        "run_id": inc["run_id"],
        "map_index": inc["map_index"],
        "try_number": inc["try_number"],
        "category": inc["category"],
        "confidence": inc["confidence"],
        "violated_rule": rule["rule"] if rule else None,
        "link": context["links"]["task_instance"],
        "pr": context["links"]["pr"],
    }
    parts = [subject, f"incident={inc['id'] or '-'}", f"run={inc['run_id'] or '-'}"]
    if inc["category"]:
        parts.append(f"category={inc['category']}")
    if inc["confidence_pct"]:
        parts.append(f"confidence={inc['confidence_pct']}")
    if rule:
        parts.append(f"rule={rule['rule']}" + (f"({rule['where']})" if rule["where"] else ""))
    if inc["diagnosis"]:
        parts.append(f"diagnosis={_truncate(_WHITESPACE.sub(' ', inc['diagnosis']), 300)!r}")
    if context.get("error"):
        parts.append(f"error={_truncate(_WHITESPACE.sub(' ', context['error']), 300)!r}")
    if context["links"]["pr"]:
        parts.append(f"pr={context['links']['pr']}")
    parts.append(f"link={context['links']['task_instance']}")
    return " | ".join(parts), fields


def _fallback(context: Mapping[str, Any]) -> tuple[str, str, str]:
    """Minimal subject/text/html used when a template cannot be rendered."""
    inc = context["incident"]
    subject = f"{SUBJECT_PREFIX} {context['event_title']}: {inc['task_label']}"
    lines = [
        context["event_title"],
        "",
        f"DAG: {inc['dag_id']}",
        f"Task: {inc['task_id']}",
        f"Run: {inc['run_id']}",
        f"Classification: {inc['category_label']}",
    ]
    if inc["diagnosis"]:
        lines += ["", inc["diagnosis"]]
    if inc["error_summary"]:
        lines += ["", "Error:", inc["error_summary"]]
    lines += ["", f"Open the incident: {context['links']['task_instance']}"]
    if context["links"]["pr"]:
        lines.append(f"Proposed fix: {context['links']['pr']}")
    lines += ["", "-- Generated by dag-healer"]
    text = "\n".join(lines) + "\n"
    body = f'<pre style="font-family:monospace;white-space:pre-wrap">{html.escape(text)}</pre>'
    return subject, text, f"<!DOCTYPE html><html><body>{body}</body></html>"


def render_alert(
    cfg: Any, incident: Mapping[str, Any], event: str, *, extra: Mapping[str, Any] | None = None
) -> RenderedAlert:
    """Render the subject, text and HTML bodies and the log line of an alert.

    Never raises for template problems: a minimal built-in rendering is used instead
    (``RenderedAlert.fallback`` is then ``True``).
    """
    try:
        context = build_context(cfg, incident, event, extra)
    except Exception as exc:  # noqa: BLE001 - unexpected incident shape: keep the identifiers only
        logger.warning(
            "Alert context for event %r could not be built (%s: %s); using the identifiers only",
            event,
            type(exc).__name__,
            mask_text(exc),
        )
        identifiers = {key: (incident or {}).get(key) for key in _IDENTIFIER_KEYS}
        context = build_context(cfg, identifiers, event, None)
    fallback = False
    try:
        env = template_environment()
        module = env.get_template(f"{event}.txt.j2").make_module(context)
        subject = _clean_subject(str(getattr(module, "subject", "")))
        if not subject:
            raise ValueError(f"template {event}.txt.j2 does not define a subject")
        text = _tidy_text(str(module))
        html_body = env.get_template(f"{event}.html.j2").render({**context, "subject": subject})
    except Exception as exc:  # noqa: BLE001 - alerts must still go out
        logger.warning(
            "Alert template for event %r failed (%s: %s); using the built-in fallback",
            event,
            type(exc).__name__,
            mask_text(exc),
        )
        subject, text, html_body = _fallback(context)
        subject = _clean_subject(subject)
        fallback = True
    log_message, log_fields = _log_parts(context, subject)
    return RenderedAlert(
        event=event,
        subject=subject,
        text=text,
        html=html_body,
        log_message=log_message,
        log_fields=log_fields,
        incident_id=context["incident"]["id"] or None,
        fallback=fallback,
    )


# --------------------------------------------------------------------------- notify


def _short_error(exc: BaseException | str) -> str:
    text = exc if isinstance(exc, str) else f"{type(exc).__name__}: {exc}"
    return _truncate(mask_text(text).strip(), MAX_OUTCOME_ERROR_CHARS)


def _email_settings(cfg: Any) -> Any:
    notify_cfg = getattr(cfg, "notify", None)
    return getattr(notify_cfg, "email", None)


def notify(
    cfg: Any,
    incident: Mapping[str, Any],
    event: AlertEvent | str,
    *,
    extra: Mapping[str, Any] | None = None,
    smtp_factory: Callable[..., Any] | None = None,
) -> list[AlertOutcome]:
    """Send the alert of ``event`` for ``incident``; return one outcome per channel.

    A log alert is always written for enabled events and an email is sent when
    ``notify.email`` is configured. Events missing from ``notify.events`` are not sent
    (``skipped`` outcomes). ``smtp_factory(host, port, timeout=...)`` replaces
    :class:`smtplib.SMTP` (tests). Never raises; persisting the outcomes is up to the caller.
    """
    try:
        return _notify(cfg, incident, str(event), extra=extra, smtp_factory=smtp_factory)
    except Exception as exc:  # noqa: BLE001 - never raise from alerting
        logger.error("dag-healer alert for event %r failed unexpectedly: %s", event, _short_error(exc))
        return [
            AlertOutcome(channel="log", recipients=[], subject="", status="failed", error=_short_error(exc))
        ]


def _notify(
    cfg: Any,
    incident: Mapping[str, Any],
    event: str,
    *,
    extra: Mapping[str, Any] | None,
    smtp_factory: Callable[..., Any] | None,
) -> list[AlertOutcome]:
    from dag_healer.alerts.email import send_email
    from dag_healer.alerts.log import log_alert

    email_cfg = _email_settings(cfg)
    if event not in ALERT_EVENTS:
        reason = f"unknown alert event {event!r}"
        logger.warning("dag-healer alert skipped: %s", reason)
        return [AlertOutcome(channel="log", recipients=[], subject="", status="skipped", error=reason)]

    rendered = render_alert(cfg, incident, event, extra=extra)
    enabled = list(getattr(getattr(cfg, "notify", None), "events", ALERT_EVENTS) or [])
    if event not in enabled:
        reason = f"event {event!r} is not enabled in notify.events"
        logger.debug("dag-healer alert skipped for incident %s: %s", rendered.incident_id, reason)
        outcomes = [
            AlertOutcome(
                channel="log", recipients=[], subject=rendered.subject, status="skipped", error=reason
            )
        ]
        if email_cfg is not None:
            outcomes.append(
                AlertOutcome(
                    channel="email",
                    recipients=list(getattr(email_cfg, "to", []) or []),
                    subject=rendered.subject,
                    status="skipped",
                    error=reason,
                )
            )
        return outcomes

    outcomes = [log_alert(rendered)]
    if email_cfg is not None:
        outcomes.append(send_email(email_cfg, rendered, smtp_factory=smtp_factory))
    return outcomes


__all__ = [
    "ALERT_EVENTS",
    "CATEGORY_STYLE",
    "EVENT_META",
    "SUBJECT_PREFIX",
    "TEMPLATES_DIR",
    "AlertChannel",
    "AlertEvent",
    "AlertOutcome",
    "AlertStatus",
    "RenderedAlert",
    "build_context",
    "notify",
    "render_alert",
    "task_label",
    "template_environment",
]
