"""Tests for dag_healer.alerts (log + email alerts) against a local aiosmtpd sink.

The opt-in test at the bottom delivers to the Mailpit of the Docker stack:
``HEALER_TEST_MAILPIT=1 python -m pytest tests/unit/test_alerts.py -k mailpit``
(``HEALER_TEST_MAILPIT_SMTP`` / ``HEALER_TEST_MAILPIT_API`` override
``localhost:1025`` / ``http://localhost:8025``).
"""

from __future__ import annotations

import inspect
import logging
import os
import re
import smtplib
import socket
import uuid
from collections.abc import Iterator
from dataclasses import dataclass, field
from email import message_from_bytes, policy
from email.message import EmailMessage
from typing import Any

import pytest
from aiosmtpd.controller import Controller
from aiosmtpd.smtp import AuthResult, LoginPassword

import dag_healer.alerts as alerts
from dag_healer import links
from dag_healer.alerts import AlertOutcome, notify, render_alert
from dag_healer.alerts import email as alert_email
from dag_healer.alerts.log import ALERT_LOGGER_NAME
from dag_healer.config import ALERT_EVENTS, config_from_mapping
from dag_healer.core.vocab import Vocabulary
from dag_healer.orchestrators.fake import FAKE_VOCABULARY
from dag_healer.store.repo import record_alert

RAW_EMAIL = "john.doe@example.com"
RAW_PHONE = "+1 555 123 4567"
RAW_ID_NUMBER = "5551234567"
RUN_ID = "manual__2026-10-17T10:00:00+00:00"
INCIDENT_ID = "0123456789abcdef0123456789abcdef"

SUBJECTS = {
    "reported": "[dag-healer] DATA issue in sales_daily_etl.transform_sales",
    "repaired": "[dag-healer] Fix proposed for sales_daily_etl.transform_sales",
    "waiting_human": "[dag-healer] Human decision needed: sales_daily_etl.transform_sales",
    "repair_failed": "[dag-healer] Repair failed for sales_daily_etl.transform_sales",
}


# --------------------------------------------------------------------------- fixtures


@dataclass
class SinkMessage:
    mail_from: str
    rcpt_tos: list[str]
    message: EmailMessage


@dataclass
class SinkHandler:
    """aiosmtpd handler that keeps the received messages (rejects ``reject*@`` recipients)."""

    messages: list[SinkMessage] = field(default_factory=list)

    async def handle_RCPT(self, server, session, envelope, address, rcpt_options):  # noqa: N802
        if address.startswith("reject"):
            return "550 5.1.1 mailbox unavailable"
        envelope.rcpt_tos.append(address)
        return "250 OK"

    async def handle_DATA(self, server, session, envelope):  # noqa: N802
        parsed = message_from_bytes(envelope.original_content or envelope.content, policy=policy.default)
        self.messages.append(SinkMessage(envelope.mail_from, list(envelope.rcpt_tos), parsed))
        return "250 Message accepted for delivery"


@dataclass
class Sink:
    host: str
    port: int
    handler: SinkHandler

    @property
    def messages(self) -> list[SinkMessage]:
        return self.handler.messages


def _free_port() -> int:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
        sock.bind(("127.0.0.1", 0))
        return sock.getsockname()[1]


def _start_sink(**smtp_kwargs: Any) -> tuple[Controller, Sink]:
    handler = SinkHandler()
    port = _free_port()
    # A fixed server_hostname: aiosmtpd would call socket.getfqdn() for its greeting, which can
    # outlast the start-up timeout on CI hosts whose name does not resolve quickly (macOS).
    controller = Controller(
        handler, hostname="127.0.0.1", port=port, server_hostname="localhost", **smtp_kwargs
    )
    controller.start()
    return controller, Sink("127.0.0.1", port, handler)


@pytest.fixture
def smtp_sink() -> Iterator[Sink]:
    controller, sink = _start_sink()
    try:
        yield sink
    finally:
        controller.stop()


def _authenticator(server, session, envelope, mechanism, auth_data) -> AuthResult:
    ok = isinstance(auth_data, LoginPassword) and auth_data == (b"healer", b"s3cret")
    return AuthResult(success=ok, handled=False)


@pytest.fixture
def auth_smtp_sink() -> Iterator[Sink]:
    controller, sink = _start_sink(auth_require_tls=False, authenticator=_authenticator)
    try:
        yield sink
    finally:
        controller.stop()


@pytest.fixture(autouse=True)
def _isolated_env(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv(links.ENV_AIRFLOW_BASE_URL, raising=False)
    monkeypatch.delenv(links.ENV_PUBLIC_URL, raising=False)


def make_cfg(sink: Sink | None = None, *, events: list[str] | None = None, **email: Any):
    data: dict[str, Any] = {"airflow": {"api_url": "http://localhost:8080"}}
    if sink is not None:
        data["notify"] = {
            "email": {
                "host": sink.host,
                "port": sink.port,
                "from": "dag-healer@healer.example.com",
                "to": ["data-team@example.com", "oncall@example.com"],
                **email,
            }
        }
    if events is not None:
        data.setdefault("notify", {})["events"] = events
    return config_from_mapping(data)


def make_incident(**overrides: Any) -> dict[str, Any]:
    incident: dict[str, Any] = {
        "id": INCIDENT_ID,
        "dag_id": "sales_daily_etl",
        "run_id": RUN_ID,
        "task_id": "transform_sales",
        "map_index": -1,
        "try_number": 2,
        "status": "reported",
        "category": "data",
        "confidence": 0.9,
        "source": "rest",
        "occurrences": 1,
        "error_summary": f"KeyError: 'amount' (row owner {RAW_EMAIL}, phone {RAW_PHONE})",
        "diagnosis": f"Column amount was renamed to amt; contact {RAW_EMAIL} <script>alert(1)</script>",
        "violated_rule": {
            "rule": "missing_column",
            "dataset": "sales",
            "column": "amount",
            "message": "Column 'amount' is missing",
            "expected": "present",
            "actual": None,
        },
        "sample": {
            "dataset": "sales",
            "columns": ["order_id", "customer_email", "amt", "note"],
            "rows": [
                {
                    "order_id": 1,
                    "customer_email": "h:0a1b2c3d4e",
                    "amt": 10.5,
                    "note": f"call {RAW_ID_NUMBER}",
                },
                {"order_id": 2, "customer_email": "h:1234567890", "amt": None, "note": "***"},
            ],
            "masked": True,
        },
        "evidence": [f"owner {RAW_EMAIL} confirmed the rename"],
        "test_path": None,
        "diff": None,
        "pr_url": None,
        "pr_md_path": None,
        "attempts": 0,
    }
    incident.update(overrides)
    return incident


# --------------------------------------------------------------------------- rendering


@pytest.mark.parametrize("event", ALERT_EVENTS)
def test_every_event_renders_from_its_templates(event: str) -> None:
    rendered = render_alert(make_cfg(), make_incident(), event)
    assert rendered.fallback is False
    assert rendered.subject == SUBJECTS[event]
    ti_link = links.task_instance_url(make_cfg(), "sales_daily_etl", RUN_ID, "transform_sales")
    assert ti_link in rendered.text
    assert ti_link in rendered.html
    assert rendered.html.lstrip().startswith("<!DOCTYPE html>")
    assert "Generated by dag-healer" in rendered.text and "Generated by dag-healer" in rendered.html
    assert rendered.text.endswith("\n") and "\n\n\n" not in rendered.text


@pytest.mark.parametrize("event", ALERT_EVENTS)
def test_minimal_incident_renders_without_fallback(event: str) -> None:
    rendered = render_alert(make_cfg(), {"id": "x1", "dag_id": "d", "run_id": "r", "task_id": "t"}, event)
    assert rendered.fallback is False
    assert "d.t" in rendered.subject
    assert "Masked data sample" not in rendered.text


def test_subjects_match_the_examples() -> None:
    cfg = make_cfg()
    assert render_alert(cfg, make_incident(), "reported").subject == (
        "[dag-healer] DATA issue in sales_daily_etl.transform_sales"
    )
    code = make_incident(dag_id="customers_sync", task_id="clean_customers", category="code")
    assert (
        render_alert(cfg, code, "repaired").subject
        == "[dag-healer] Fix proposed for customers_sync.clean_customers"
    )
    ambiguous = make_incident(dag_id="inventory_report", task_id="build_report", category="ambiguous")
    assert render_alert(cfg, ambiguous, "waiting_human").subject == (
        "[dag-healer] Human decision needed: inventory_report.build_report"
    )


def test_reported_without_category_is_a_data_issue() -> None:
    rendered = render_alert(make_cfg(), make_incident(category=None, violated_rule=None), "reported")
    assert rendered.subject.startswith("[dag-healer] DATA issue in")
    assert "classified as a data issue" in rendered.text


def test_mapped_task_label_and_link() -> None:
    rendered = render_alert(make_cfg(), make_incident(map_index=3), "reported")
    assert rendered.subject.endswith("sales_daily_etl.transform_sales[3]")
    assert "/tasks/transform_sales/mapped/3/plugin/healer-ti" in rendered.text


def test_subject_is_a_single_line() -> None:
    rendered = render_alert(
        make_cfg(), make_incident(dag_id="evil\r\nBcc: x@y.z", task_id="t\n2"), "reported"
    )
    assert "\n" not in rendered.subject and "\r" not in rendered.subject


def test_only_masked_values_are_rendered() -> None:
    extra = {
        "reason": f"ask {RAW_EMAIL}",
        "notes": [f"note for {RAW_PHONE}"],
        "error": f"failed for {RAW_EMAIL}",
        "violations": [{"rule": "out_of_range", "dataset": "sales", "column": "qty", "message": RAW_EMAIL}],
    }
    for event in ALERT_EVENTS:
        rendered = render_alert(make_cfg(), make_incident(), event, extra=extra)
        for body in (rendered.text, rendered.html, rendered.log_message, rendered.subject):
            assert RAW_EMAIL not in body
            assert RAW_PHONE not in body
            assert RAW_ID_NUMBER not in body
        assert "[email]" in rendered.text


def test_sample_keeps_masking_tokens_and_scrubs_strings() -> None:
    rendered = render_alert(make_cfg(), make_incident(), "reported")
    for body in (rendered.text, rendered.html):
        assert "h:0a1b2c3d4e" in body
        assert "h:1234567890" in body  # an all-digit hash is not mistaken for a number
        assert "call [number]" in body
        assert "***" in body
    assert "null" in rendered.text


def test_sample_rows_follow_masking_sample_rows() -> None:
    rows = [{"order_id": index, "note": f"row-{index}"} for index in range(10)]
    sample = {"dataset": "sales", "columns": ["order_id", "note"], "rows": rows, "masked": True}
    cfg = config_from_mapping({"masking": {"sample_rows": 2}})
    rendered = render_alert(cfg, make_incident(sample=sample), "reported")
    assert "row-1" in rendered.text and "row-2" not in rendered.text
    assert "8 more rows" in rendered.text


def test_unmasked_sample_is_never_rendered() -> None:
    sample = {"dataset": "sales", "columns": ["email"], "rows": [{"email": "plain@example.org"}]}
    rendered = render_alert(make_cfg(), make_incident(sample=sample), "reported")
    assert "plain@example.org" not in rendered.text + rendered.html
    assert "not flagged as masked" in rendered.text


def test_html_is_escaped_and_has_no_external_assets() -> None:
    rendered = render_alert(make_cfg(), make_incident(), "reported")
    assert "<script>" not in rendered.html
    assert "&lt;script&gt;" in rendered.html
    assert "<link" not in rendered.html and "<img" not in rendered.html and "src=" not in rendered.html


def test_violations_accept_models_and_strings() -> None:
    from dag_healer.contracts.schema import Violation

    violation = Violation(
        rule="nulls_not_allowed",
        dataset="sales",
        column="amount",
        message="3 null values",
        row_indexes=[1, 2, 3],
    )
    rendered = render_alert(
        make_cfg(),
        make_incident(violated_rule="missing_column"),
        "reported",
        extra={"violations": [violation]},
    )
    assert "nulls_not_allowed [sales.amount]: 3 null values" in rendered.text
    assert "missing_column" in rendered.text


def test_repaired_links_the_pull_request_or_pr_md() -> None:
    diff = (
        "diff --git a/src/etl/customers.py b/src/etl/customers.py\n+fix\n"
        "diff --git a/tests/test_c.py b/tests/test_c.py\n"
    )
    with_pr = make_incident(category="code", pr_url="https://github.com/acme/etl/pull/12", diff=diff)
    rendered = render_alert(make_cfg(), with_pr, "repaired")
    assert "https://github.com/acme/etl/pull/12" in rendered.text
    assert "Review the pull request" in rendered.html
    assert "src/etl/customers.py, tests/test_c.py" in rendered.text

    with_md = make_incident(category="code", pr_md_path="/opt/airflow/healer/prs/x/PR.md")
    rendered = render_alert(make_cfg(), with_md, "repaired", extra={"branch": "healer/sales-t-01234567"})
    pr_md = links.pr_md_url(make_cfg(), "sales_daily_etl", INCIDENT_ID)
    assert pr_md in rendered.text and pr_md in rendered.html
    assert "Review PR.md" in rendered.html
    assert "healer/sales-t-01234567" in rendered.text


def test_template_failure_uses_the_fallback(monkeypatch: pytest.MonkeyPatch) -> None:
    class Broken:
        def get_template(self, name: str):
            raise RuntimeError(f"boom for {RAW_EMAIL}")

    monkeypatch.setattr(alerts, "template_environment", lambda: Broken())
    rendered = render_alert(make_cfg(), make_incident(), "reported")
    assert rendered.fallback is True
    assert rendered.subject == "[dag-healer] Data issue reported: sales_daily_etl.transform_sales"
    assert links.task_instance_url(make_cfg(), "sales_daily_etl", RUN_ID, "transform_sales") in rendered.text
    assert RAW_EMAIL not in rendered.text + rendered.html


def test_broken_incident_fields_keep_the_identifiers(monkeypatch: pytest.MonkeyPatch) -> None:
    original = alerts._normalize_sample

    def picky(sample: Any, max_rows: int):
        if sample is not None:
            raise TypeError("unexpected sample shape")
        return original(sample, max_rows)

    monkeypatch.setattr(alerts, "_normalize_sample", picky)
    rendered = render_alert(make_cfg(), make_incident(), "reported")
    assert rendered.subject == SUBJECTS["reported"]
    assert "Masked data sample" not in rendered.text
    assert RAW_EMAIL not in rendered.text


# --------------------------------------------------------------------------- orchestrator words and links

FAKEFLOW_URL = "http://fakeflow.localhost/flows/sales_daily_etl/runs/r?attempt=2&map_index=3"
_GENERATED_AT = re.compile(r"\d{4}-\d{2}-\d{2} \d{2}:\d{2} UTC")


def _stable(rendered: alerts.RenderedAlert) -> tuple[str, str, str]:
    """Subject, text and HTML without the generation minute (two renders may straddle one)."""
    return (
        rendered.subject,
        _GENERATED_AT.sub("<t>", rendered.text),
        _GENERATED_AT.sub("<t>", rendered.html),
    )


def _fakeflow(**extra: Any) -> dict[str, Any]:
    return {"vocabulary": FAKE_VOCABULARY.as_dict(), "native_links": {"Fakeflow": FAKEFLOW_URL}, **extra}


@pytest.mark.parametrize("event", ALERT_EVENTS)
@pytest.mark.parametrize(
    "extra",
    [{"vocabulary": {}, "native_links": {}}, {"vocabulary": Vocabulary().as_dict()}, {"native_links": None}],
    ids=["empty", "airflow-words", "no-links"],
)
def test_airflow_words_and_no_native_links_change_nothing(event: str, extra: dict[str, Any]) -> None:
    incident = make_incident(map_index=3, occurrences=2, attempts=1)
    plain = render_alert(make_cfg(), incident, event)
    assert _stable(render_alert(make_cfg(), incident, event, extra=extra)) == _stable(plain)
    assert "\nDAG:            sales_daily_etl\n" in plain.text
    assert "\nTask:           transform_sales (map index 3)\n" in plain.text
    assert "\nRun:            manual__2026-10-17T10:00:00+00:00\n" in plain.text
    assert "\nTry:            2\n" in plain.text
    assert "\nDAG incidents:    http://localhost:8080/dags/sales_daily_etl/plugin/healer-dag\n" in plain.text
    assert "Open in" not in plain.text and "Open in" not in plain.html


@pytest.mark.parametrize("event", ALERT_EVENTS)
def test_alerts_speak_the_orchestrator_words(event: str) -> None:
    rendered = render_alert(make_cfg(), make_incident(map_index=3), event, extra=_fakeflow())
    assert rendered.fallback is False
    assert rendered.subject == SUBJECTS[event].replace("transform_sales", "transform_sales[3]")
    text, html = rendered.text, rendered.html
    assert "\nFlow:           sales_daily_etl\n" in text
    assert "\nStep:           transform_sales (map index 3)\n" in text
    assert "\nRun:            manual__2026-10-17T10:00:00+00:00\n" in text
    assert "\nAttempt:        2\n" in text
    assert "\nFlow incidents:   http://localhost:8080/dags/sales_daily_etl/plugin/healer-dag\n" in text
    assert f"\nOpen in Fakeflow: {FAKEFLOW_URL}\n" in text
    assert ">Flow</td>" in html and ">Step</td>" in html and ">Attempt</td>" in html and ">Run</td>" in html
    assert ">Flow incidents</a>" in html
    assert f'href="{FAKEFLOW_URL.replace("&", "&amp;")}"' in html and ">Open in Fakeflow</a>" in html
    for body in (text, html):
        assert "DAG" not in body and "Task" not in body and "Try" not in body
        assert "the task" not in body and "task error" not in body


def test_event_texts_use_the_step_word() -> None:
    reported = render_alert(make_cfg(), make_incident(), "reported", extra=_fakeflow())
    for body in (reported.text, reported.html):
        assert "The input data of this step does not match its data contract." in body
        assert "then retry the step from the Healer incident tab." in body
    repaired = render_alert(make_cfg(), make_incident(category="code"), "repaired", extra=_fakeflow())
    assert "Once the fix is deployed, retry the step from the Healer incident tab." in repaired.text
    assert "retry the step from there." in repaired.html
    with_pr = make_incident(category="code", pr_url="https://github.com/acme/etl/pull/12")
    reviewed = render_alert(make_cfg(), with_pr, "repaired", extra=_fakeflow())
    assert "retry the step from the Healer incident tab." in reviewed.html
    failed = render_alert(make_cfg(), make_incident(category="code"), "repair_failed", extra=_fakeflow())
    assert "Original step error" in failed.text and "Original step error" in failed.html


def test_only_absolute_native_links_are_rendered() -> None:
    native = {
        "Fakeflow": FAKEFLOW_URL,
        "Script": "javascript:alert(1)",
        "Relative": "/flows/x",
        "": "http://fakeflow.localhost/empty-label",
        "Spaced": "http://fakeflow.localhost/a b",
        "Multi\nline label": "https://fakeflow.localhost/runs",
    }
    context = alerts.build_context(make_cfg(), make_incident(), "reported", {"native_links": native})
    assert context["links"]["native"] == {
        "Fakeflow": FAKEFLOW_URL,
        "Multi line label": "https://fakeflow.localhost/runs",
    }
    for extra in (None, {"native_links": ["x"]}, {"native_links": {"Fakeflow": 42}}):
        assert alerts.build_context(make_cfg(), make_incident(), "reported", extra)["links"]["native"] == {}


def test_words_fall_back_to_airflow_and_keep_upper_case() -> None:
    words = alerts.build_context(
        make_cfg(), make_incident(), "reported", {"vocabulary": {"step": "state", "pipeline": "", "x": "y"}}
    )["words"]
    assert words["step"] == "state" and words["step_label"] == "State"
    assert words["pipeline"] == "DAG" and words["pipeline_label"] == "DAG"
    assert words["attempt_label"] == "Try" and "x" not in words


def test_fallback_uses_the_orchestrator_words(monkeypatch: pytest.MonkeyPatch) -> None:
    class Broken:
        def get_template(self, name: str):
            raise RuntimeError("boom")

    monkeypatch.setattr(alerts, "template_environment", lambda: Broken())
    rendered = render_alert(make_cfg(), make_incident(), "reported", extra=_fakeflow())
    assert rendered.fallback is True
    assert "\nFlow: sales_daily_etl\nStep: transform_sales\nRun: manual__" in rendered.text
    plain = render_alert(make_cfg(), make_incident(), "reported")
    assert "\nDAG: sales_daily_etl\nTask: transform_sales\nRun: manual__" in plain.text


def test_identifier_only_alerts_keep_the_orchestrator_words(monkeypatch: pytest.MonkeyPatch) -> None:
    original = alerts._normalize_sample

    def picky(sample: Any, max_rows: int):
        if sample is not None:
            raise TypeError("unexpected sample shape")
        return original(sample, max_rows)

    monkeypatch.setattr(alerts, "_normalize_sample", picky)
    rendered = render_alert(make_cfg(), make_incident(), "reported", extra=_fakeflow(sample={"rows": []}))
    assert rendered.fallback is False
    assert "\nFlow:           sales_daily_etl\n" in rendered.text
    assert f"\nOpen in Fakeflow: {FAKEFLOW_URL}\n" in rendered.text
    assert "Masked data sample" not in rendered.text


# --------------------------------------------------------------------------- notify: log channel


def test_no_email_config_writes_only_the_log_alert(caplog: pytest.LogCaptureFixture) -> None:
    caplog.set_level(logging.INFO, logger=ALERT_LOGGER_NAME)
    outcomes = notify(make_cfg(), make_incident(), "reported")
    assert outcomes == [AlertOutcome("log", [], SUBJECTS["reported"], "sent", None)]
    records = [r for r in caplog.records if r.name == ALERT_LOGGER_NAME and hasattr(r, "healer_alert")]
    assert len(records) == 1
    record = records[0]
    assert record.levelno == logging.WARNING
    assert record.healer_alert["incident_id"] == INCIDENT_ID
    assert record.healer_alert["violated_rule"] == "missing_column"
    message = record.getMessage()
    assert SUBJECTS["reported"] in message and "plugin/healer-ti" in message
    assert RAW_EMAIL not in message and "h:0a1b2c3d4e" not in message  # no sample rows in logs


def test_repaired_logs_at_info(caplog: pytest.LogCaptureFixture) -> None:
    caplog.set_level(logging.INFO, logger=ALERT_LOGGER_NAME)
    notify(make_cfg(), make_incident(category="code"), "repaired")
    levels = [r.levelno for r in caplog.records if hasattr(r, "healer_alert")]
    assert levels == [logging.INFO]


def test_disabled_event_is_skipped(smtp_sink: Sink, caplog: pytest.LogCaptureFixture) -> None:
    caplog.set_level(logging.INFO, logger=ALERT_LOGGER_NAME)
    outcomes = notify(make_cfg(smtp_sink, events=["reported"]), make_incident(), "repaired")
    assert [(o.channel, o.status) for o in outcomes] == [("log", "skipped"), ("email", "skipped")]
    assert all("notify.events" in (o.error or "") for o in outcomes)
    assert smtp_sink.messages == []
    assert not [r for r in caplog.records if hasattr(r, "healer_alert")]


def test_disabled_event_without_email_config() -> None:
    outcomes = notify(make_cfg(events=["waiting_human"]), make_incident(), "reported")
    assert [(o.channel, o.status) for o in outcomes] == [("log", "skipped")]


def test_unknown_event_never_raises() -> None:
    outcomes = notify(make_cfg(), make_incident(), "exploded")
    assert [(o.channel, o.status) for o in outcomes] == [("log", "skipped")]


def test_unexpected_errors_never_raise(monkeypatch: pytest.MonkeyPatch) -> None:
    def explode(*args: Any, **kwargs: Any):
        raise ValueError(f"unexpected for {RAW_EMAIL}")

    monkeypatch.setattr(alerts, "render_alert", explode)
    outcomes = notify(make_cfg(), make_incident(), "reported")
    assert [(o.channel, o.status) for o in outcomes] == [("log", "failed")]
    assert RAW_EMAIL not in (outcomes[0].error or "")


def test_outcomes_match_record_alert() -> None:
    params = set(inspect.signature(record_alert).parameters)
    outcome = AlertOutcome("email", ["a@b.c"], "s", "sent")
    assert set(outcome.as_record()) <= params
    assert outcome.to_dict() == {
        "channel": "email",
        "recipients": ["a@b.c"],
        "subject": "s",
        "status": "sent",
        "error": None,
    }


def test_outcomes_can_be_persisted(tmp_path) -> None:
    from dag_healer.store import db, repo

    url = f"sqlite:///{(tmp_path / 'h.db').as_posix()}"
    with db.session_scope(url) as session:
        incident, _created = repo.create_incident(
            session, dag_id="d", run_id="r", task_id="t", map_index=-1, try_number=1, source="rest"
        )
        for outcome in notify(make_cfg(), incident.to_dict(), "reported"):
            repo.record_alert(session, incident_id=incident.id, **outcome.as_record())
        alerts_rows = repo.list_alerts(session, incident.id)
        assert [(a.channel, a.status) for a in alerts_rows] == [("log", "sent")]


# --------------------------------------------------------------------------- notify: email channel


def _parts(message: EmailMessage) -> dict[str, str]:
    return {part.get_content_type(): part.get_content() for part in message.walk() if not part.is_multipart()}


def test_email_is_delivered_with_headers_and_both_parts(smtp_sink: Sink) -> None:
    cfg = make_cfg(smtp_sink)
    outcomes = notify(cfg, make_incident(), "reported")
    assert [(o.channel, o.status, o.error) for o in outcomes] == [
        ("log", "sent", None),
        ("email", "sent", None),
    ]
    assert outcomes[1].recipients == ["data-team@example.com", "oncall@example.com"]
    assert outcomes[1].subject == SUBJECTS["reported"]

    assert len(smtp_sink.messages) == 1
    received = smtp_sink.messages[0]
    assert received.mail_from == "dag-healer@healer.example.com"
    assert received.rcpt_tos == ["data-team@example.com", "oncall@example.com"]
    message = received.message
    assert message["Subject"] == SUBJECTS["reported"]
    assert message["From"] == "dag-healer@healer.example.com"
    assert message["To"] == "data-team@example.com, oncall@example.com"
    assert message["Message-ID"].startswith("<") and message["Message-ID"].endswith("@healer.example.com>")
    assert message["Date"]
    assert message["Auto-Submitted"] == "auto-generated"
    assert message["X-Dag-Healer-Incident"] == INCIDENT_ID
    assert message["X-Dag-Healer-Event"] == "reported"
    assert message["References"] == f"<incident.{INCIDENT_ID}@healer.example.com>"
    assert message.get_content_type() == "multipart/alternative"

    parts = _parts(message)
    assert set(parts) == {"text/plain", "text/html"}
    ti_link = links.task_instance_url(cfg, "sales_daily_etl", RUN_ID, "transform_sales")
    html_link = ti_link.replace("&", "&amp;")
    assert ti_link in parts["text/plain"]
    assert f'href="{html_link}"' in parts["text/html"]
    assert links.dag_url(cfg, "sales_daily_etl") in parts["text/html"]
    assert "missing_column" in parts["text/plain"] and "missing_column" in parts["text/html"]
    assert "h:0a1b2c3d4e" in parts["text/html"]
    for body in parts.values():
        assert RAW_EMAIL not in body and RAW_PHONE not in body and RAW_ID_NUMBER not in body


def test_message_ids_are_unique(smtp_sink: Sink) -> None:
    cfg = make_cfg(smtp_sink)
    notify(cfg, make_incident(), "waiting_human")
    notify(cfg, make_incident(), "waiting_human")
    ids = {m.message["Message-ID"] for m in smtp_sink.messages}
    assert len(ids) == 2


def test_email_login(auth_smtp_sink: Sink) -> None:
    ok = notify(make_cfg(auth_smtp_sink, username="healer", password="s3cret"), make_incident(), "reported")
    assert ok[1].status == "sent"
    bad = notify(make_cfg(auth_smtp_sink, username="healer", password="wrong"), make_incident(), "reported")
    assert bad[1].status == "failed"
    assert "SMTPAuthenticationError" in (bad[1].error or "")
    assert "wrong" not in (bad[1].error or "")
    assert len(auth_smtp_sink.messages) == 1


def test_partially_refused_recipients_are_reported(smtp_sink: Sink) -> None:
    cfg = make_cfg(smtp_sink, to=["data-team@example.com", "reject-me@example.com"])
    outcome = notify(cfg, make_incident(), "reported")[1]
    assert outcome.status == "sent"
    assert "1 of 2 recipients were refused" in (outcome.error or "")
    assert smtp_sink.messages[0].rcpt_tos == ["data-team@example.com"]


def test_all_recipients_refused_fails(smtp_sink: Sink) -> None:
    cfg = make_cfg(smtp_sink, to=["reject-me@example.com"])
    outcome = notify(cfg, make_incident(), "reported")[1]
    assert outcome.status == "failed"
    assert "SMTPRecipientsRefused" in (outcome.error or "")
    assert "reject-me@example.com" not in (outcome.error or "")  # error text is masked


def test_connection_refused_fails_without_raising(caplog: pytest.LogCaptureFixture) -> None:
    sink = Sink("127.0.0.1", _free_port(), SinkHandler())  # nothing listens there
    outcomes = notify(make_cfg(sink), make_incident(), "repair_failed")
    assert [(o.channel, o.status) for o in outcomes] == [("log", "sent"), ("email", "failed")]
    assert outcomes[1].error
    assert any("Alert email" in r.getMessage() and r.levelno == logging.WARNING for r in caplog.records)


def test_empty_recipient_list_is_skipped(smtp_sink: Sink) -> None:
    outcome = notify(make_cfg(smtp_sink, to=[]), make_incident(), "reported")[1]
    assert (outcome.channel, outcome.status) == ("email", "skipped")
    assert smtp_sink.messages == []


class FakeSMTP:
    """Records the calls made by send_email."""

    instances: list[FakeSMTP] = []

    def __init__(self, host: str, port: int, *, timeout: float = 0, fail_on: str | None = None) -> None:
        self.calls: list[tuple[str, Any]] = [("connect", (host, port, timeout))]
        self.fail_on = fail_on
        FakeSMTP.instances.append(self)

    def _call(self, name: str, value: Any = None) -> None:
        self.calls.append((name, value))
        if self.fail_on == name:
            raise smtplib.SMTPException(f"{name} failed")

    def ehlo(self) -> None:
        self._call("ehlo")

    def starttls(self, context: Any = None) -> None:
        self._call("starttls", context is not None)

    def login(self, user: str, password: str) -> None:
        self._call("login", user)

    def send_message(self, message: EmailMessage, from_addr: str, to_addrs: list[str]) -> dict:
        self._call("send_message", (from_addr, list(to_addrs)))
        return {}

    def quit(self) -> None:
        self._call("quit")


def test_starttls_and_login_order_with_a_factory() -> None:
    FakeSMTP.instances.clear()
    cfg = config_from_mapping(
        {
            "notify": {
                "email": {
                    "host": "smtp.example.com",
                    "port": 587,
                    "from": "Healer <healer@example.com>",
                    "to": "a@example.com, B Team <b@example.com>",
                    "starttls": True,
                    "username": "healer",
                    "password": "pw",
                }
            }
        }
    )
    outcomes = notify(cfg, make_incident(), "waiting_human", smtp_factory=FakeSMTP)
    assert outcomes[1].status == "sent"
    names = [name for name, _ in FakeSMTP.instances[0].calls]
    assert names == ["connect", "ehlo", "starttls", "ehlo", "login", "send_message", "quit"]
    calls = dict(FakeSMTP.instances[0].calls)
    assert calls["connect"] == ("smtp.example.com", 587, alert_email.DEFAULT_TIMEOUT)
    assert calls["send_message"] == ("healer@example.com", ["a@example.com", "b@example.com"])


def test_failure_mid_session_still_closes_the_connection() -> None:
    FakeSMTP.instances.clear()

    def factory(host: str, port: int, *, timeout: float) -> FakeSMTP:
        return FakeSMTP(host, port, timeout=timeout, fail_on="send_message")

    sink = Sink("smtp.example.com", 25, SinkHandler())
    outcome = notify(make_cfg(sink), make_incident(), "reported", smtp_factory=factory)[1]
    assert outcome.status == "failed"
    assert outcome.error == "SMTPException: send_message failed"
    assert FakeSMTP.instances[0].calls[-1][0] == "quit"


def test_port_465_uses_implicit_tls(monkeypatch: pytest.MonkeyPatch) -> None:
    created: list[tuple[str, int]] = []

    def fake_ssl(host: str, port: int, *, timeout: float, context: Any) -> FakeSMTP:
        created.append((host, port))
        return FakeSMTP(host, port, timeout=timeout)

    monkeypatch.setattr(alert_email.smtplib, "SMTP_SSL", fake_ssl)
    sink = Sink("smtp.example.com", 465, SinkHandler())
    outcome = notify(make_cfg(sink), make_incident(), "reported")[1]
    assert outcome.status == "sent"
    assert created == [("smtp.example.com", 465)]


# --------------------------------------------------------------------------- Mailpit (opt-in)


@pytest.mark.skipif(
    os.environ.get("HEALER_TEST_MAILPIT") != "1",
    reason="set HEALER_TEST_MAILPIT=1 to deliver to the Mailpit of the Docker stack",
)
def test_delivery_to_running_mailpit() -> None:
    import httpx

    host, _, port = os.environ.get("HEALER_TEST_MAILPIT_SMTP", "localhost:1025").partition(":")
    api = os.environ.get("HEALER_TEST_MAILPIT_API", "http://localhost:8025").rstrip("/")
    marker = f"mailpit_check_{uuid.uuid4().hex[:10]}"
    cfg = make_cfg(Sink(host, int(port or 1025), SinkHandler()))
    incident = make_incident(dag_id=marker, pr_md_path="/tmp/PR.md")

    ids: list[str] = []
    with httpx.Client(base_url=api, timeout=10.0) as client:
        try:
            for event in ALERT_EVENTS:
                outcome = notify(cfg, incident, event)[1]
                assert outcome.status == "sent", outcome.error
            response = client.get("/api/v1/search", params={"query": marker})
            response.raise_for_status()
            found = response.json()["messages"]
            ids = [item["ID"] for item in found]
            assert len(ids) == len(ALERT_EVENTS)
            subjects = {item["Subject"] for item in found}
            assert f"[dag-healer] Human decision needed: {marker}.transform_sales" in subjects
            for message_id in ids:
                detail = client.get(f"/api/v1/message/{message_id}").json()
                assert "plugin/healer-ti" in detail["Text"]
                assert "plugin/healer-ti" in detail["HTML"]
                assert RAW_EMAIL not in detail["Text"] + detail["HTML"]
        finally:
            if ids:  # an empty ID list would delete *every* message in Mailpit
                client.request("DELETE", "/api/v1/messages", json={"IDs": ids}).raise_for_status()
