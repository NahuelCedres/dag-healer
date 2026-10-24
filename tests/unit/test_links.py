"""Tests for dag_healer.links (URLs of the Healer views and links into the orchestrator UI)."""

from __future__ import annotations

from types import SimpleNamespace
from typing import Any
from urllib.parse import unquote, urlsplit

import pytest

from dag_healer import links
from dag_healer.config import config_from_mapping
from dag_healer.orchestrators.fake import FakeAdapter, FakeWorld

RUN_ID = "manual__2026-10-17T10:00:00+00:00"


@pytest.fixture(autouse=True)
def _no_base_url_env(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv(links.ENV_AIRFLOW_BASE_URL, raising=False)
    monkeypatch.delenv(links.ENV_PUBLIC_URL, raising=False)


@pytest.fixture
def cfg():
    return config_from_mapping({"airflow": {"api_url": "http://airflow-apiserver:8080/"}})


# --------------------------------------------------------------------------- base url


def test_base_url_falls_back_to_airflow_api_url(cfg) -> None:
    assert links.airflow_base_url(cfg) == "http://airflow-apiserver:8080"


def test_base_url_prefers_airflow_base_url_then_public_url(cfg, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv(links.ENV_PUBLIC_URL, "https://healer.example.com/")
    assert links.airflow_base_url(cfg) == "https://healer.example.com"
    monkeypatch.setenv(links.ENV_AIRFLOW_BASE_URL, "http://localhost:8080/")
    assert links.airflow_base_url(cfg) == "http://localhost:8080"


def test_blank_environment_values_are_ignored(cfg, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv(links.ENV_AIRFLOW_BASE_URL, "   ")
    monkeypatch.setenv(links.ENV_PUBLIC_URL, "")
    assert links.airflow_base_url(cfg) == "http://airflow-apiserver:8080"


def test_base_url_without_config_uses_default() -> None:
    assert links.airflow_base_url(None) == links.DEFAULT_BASE_URL
    assert links.healer_page_url(None) == "http://localhost:8080/plugin/healer"


def test_base_url_keeps_a_subpath(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv(links.ENV_AIRFLOW_BASE_URL, "https://corp.example.com/airflow/")
    assert links.healer_page_url(None) == "https://corp.example.com/airflow/plugin/healer"
    assert links.dag_url(None, "d") == "https://corp.example.com/airflow/dags/d/plugin/healer-dag"


# --------------------------------------------------------------------------- views


def test_task_instance_url_quotes_every_segment(cfg) -> None:
    url = links.task_instance_url(cfg, "sales_daily_etl", RUN_ID, "transform_sales")
    assert url == (
        "http://airflow-apiserver:8080/dags/sales_daily_etl/runs/"
        "manual__2026-10-17T10%3A00%3A00%2B00%3A00/tasks/transform_sales/plugin/healer-ti"
    )
    path = urlsplit(url).path.split("/")
    assert unquote(path[4]) == RUN_ID


def test_task_instance_url_quotes_slashes_and_spaces(cfg) -> None:
    url = links.task_instance_url(cfg, "my dag", "run/1", "group.task")
    assert "/dags/my%20dag/runs/run%2F1/tasks/group.task/plugin/healer-ti" in url


@pytest.mark.parametrize(
    ("map_index", "suffix"),
    [
        (-1, "/tasks/t/plugin/healer-ti"),
        (None, "/tasks/t/plugin/healer-ti"),
        ("not-a-number", "/tasks/t/plugin/healer-ti"),
        (0, "/tasks/t/mapped/0/plugin/healer-ti"),
        (3, "/tasks/t/mapped/3/plugin/healer-ti"),
        ("7", "/tasks/t/mapped/7/plugin/healer-ti"),
    ],
)
def test_task_instance_url_mapped(cfg, map_index, suffix: str) -> None:
    assert links.task_instance_url(cfg, "d", "r", "t", map_index).endswith(suffix)


def test_dag_healer_and_pr_md_urls(cfg) -> None:
    assert links.dag_url(cfg, "customers_sync") == (
        "http://airflow-apiserver:8080/dags/customers_sync/plugin/healer-dag"
    )
    assert links.healer_page_url(cfg) == "http://airflow-apiserver:8080/plugin/healer"
    assert links.pr_md_url(cfg, "customers_sync", "abc123") == (
        "http://airflow-apiserver:8080/healer/api/v1/dags/customers_sync/incidents/abc123/pr.md"
    )


def test_incident_links(cfg) -> None:
    incident = {"id": "i1", "dag_id": "d", "run_id": RUN_ID, "task_id": "t", "map_index": 2}
    result = links.incident_links(cfg, incident)
    assert set(result) == {"task_instance", "dag", "healer"}
    assert result["task_instance"] == links.task_instance_url(cfg, "d", RUN_ID, "t", 2)
    assert result["dag"] == links.dag_url(cfg, "d")
    assert result["healer"] == links.healer_page_url(cfg)


def test_incident_links_tolerates_missing_fields(cfg) -> None:
    result = links.incident_links(cfg, {})
    assert result["task_instance"].endswith("/dags//runs//tasks//plugin/healer-ti")


def test_pr_link_precedence(cfg) -> None:
    base = {"id": "i1", "dag_id": "d"}
    assert links.pr_link(cfg, base) is None
    assert links.pr_link(cfg, {**base, "has_pr_md": True}) == links.pr_md_url(cfg, "d", "i1")
    assert links.pr_link(cfg, {**base, "pr_md_path": "/x/PR.md"}) == links.pr_md_url(cfg, "d", "i1")
    pr = "https://github.com/o/r/pull/7"
    assert links.pr_link(cfg, {**base, "pr_md_path": "/x/PR.md", "pr_url": f" {pr} "}) == pr
    assert links.pr_link(cfg, {**base, "pr_url": "   "}) is None


# --------------------------------------------------------------------------- base url chain


def test_serve_public_url_comes_after_the_environment(monkeypatch: pytest.MonkeyPatch) -> None:
    cfg = config_from_mapping(
        {
            "airflow": {"api_url": "http://airflow-apiserver:8080"},
            "serve": {"public_url": "https://healer.example/"},
        }
    )
    assert links.airflow_base_url(cfg) == "https://healer.example"
    monkeypatch.setenv(links.ENV_PUBLIC_URL, "https://public.example")
    assert links.airflow_base_url(cfg) == "https://public.example"
    monkeypatch.setenv(links.ENV_AIRFLOW_BASE_URL, "https://airflow.example")
    assert links.airflow_base_url(cfg) == "https://airflow.example"


def test_airflow_api_url_is_the_fallback_of_airflow_only() -> None:
    airflow = config_from_mapping(
        {"airflow": {"api_url": "http://airflow-apiserver:8080"}, "serve": {"host": "0.0.0.0", "port": 9000}}
    )
    assert links.airflow_base_url(airflow) == "http://airflow-apiserver:8080"
    fake = config_from_mapping(
        {
            "orchestrator": {"type": "fake"},
            "airflow": {"api_url": "http://airflow-apiserver:8080"},
            "serve": {"host": "healer.internal", "port": 9000},
        }
    )
    assert links.airflow_base_url(fake) == "http://healer.internal:9000"
    assert links.healer_page_url(fake) == "http://healer.internal:9000/plugin/healer"
    defaults = config_from_mapping({"orchestrator": {"type": "fake"}})
    assert links.airflow_base_url(defaults) == "http://127.0.0.1:8090"


def test_the_explicit_airflow_factory_is_airflow_too() -> None:
    from dag_healer.airflow.adapter import FACTORY_TARGET

    cfg = config_from_mapping(
        {
            "orchestrator": {"type": FACTORY_TARGET, "instance": "airflow-prod"},
            "airflow": {"api_url": "http://airflow.internal:8080"},
        }
    )
    assert links.airflow_base_url(cfg) == "http://airflow.internal:8080"
    assert links.healer_page_url(cfg) == "http://airflow.internal:8080/plugin/healer"
    incident = {"orchestrator": "airflow-prod", "dag_id": "d", "run_id": "r", "task_id": "t"}
    assert links.native_links(cfg, incident) == {
        "Airflow task": "http://airflow.internal:8080/dags/d/runs/r/tasks/t"
    }


def test_base_url_reads_any_object() -> None:
    sfn = SimpleNamespace(type="stepfunctions")
    ipv6 = SimpleNamespace(orchestrator=sfn, serve=SimpleNamespace(host="::1", port=8090))
    assert links.airflow_base_url(ipv6) == "http://[::1]:8090"
    no_section = SimpleNamespace(airflow=SimpleNamespace(api_url="http://af:8080/"))
    assert links.airflow_base_url(no_section) == "http://af:8080"
    assert links.airflow_base_url(SimpleNamespace(orchestrator=sfn)) == links.DEFAULT_BASE_URL
    no_public_url = SimpleNamespace(serve=SimpleNamespace(public_url=None))
    assert links.airflow_base_url(no_public_url) == links.DEFAULT_BASE_URL


def test_public_base_url_is_the_same_function() -> None:
    assert links.public_base_url is links.airflow_base_url


# --------------------------------------------------------------------------- native links


INCIDENT = {
    "id": "i1",
    "orchestrator": "fake",
    "dag_id": "sales daily",
    "run_id": RUN_ID,
    "task_id": "load",
    "map_index": 3,
    "try_number": 2,
    "native_ref": {"seq": 1},
}


def test_native_links_ask_the_adapter() -> None:
    adapter = FakeAdapter(world=FakeWorld())
    expected = {
        "Fakeflow": "http://fakeflow.localhost/flows/sales%20daily/runs/"
        "manual__2026-10-17T10%3A00%3A00%2B00%3A00/steps/load?attempt=2&map_index=3"
    }
    assert links.native_links(None, INCIDENT, adapter=adapter) == expected
    assert links.shown_native_links(None, INCIDENT, adapter=adapter) == expected


def test_native_links_build_the_configured_adapter() -> None:
    cfg = config_from_mapping(
        {"orchestrator": {"type": "fake", "options": {"base_url": "https://flows.example"}}}
    )
    found = links.native_links(cfg, INCIDENT)
    assert list(found) == ["Fakeflow"] and found["Fakeflow"].startswith("https://flows.example/flows/")
    assert links.shown_native_links(cfg, INCIDENT) == found


def test_airflow_native_links_are_not_shown(cfg) -> None:
    airflow = {**INCIDENT, "orchestrator": "airflow"}
    assert links.native_links(cfg, airflow) == {
        "Airflow task": "http://airflow-apiserver:8080/dags/sales%20daily/runs/"
        "manual__2026-10-17T10%3A00%3A00%2B00%3A00/tasks/load/mapped/3"
    }
    assert links.shown_native_links(cfg, airflow) == {}  # the task_instance link opens the Airflow UI


def test_native_links_never_raise() -> None:
    broken = config_from_mapping({"orchestrator": {"type": "no_such_pkg.adapters:FACTORY", "instance": "x"}})
    assert links.native_links(broken, INCIDENT) == {}
    assert links.shown_native_links(broken, INCIDENT) == {}

    class Links:
        def links(self, ref: Any, native: Any) -> dict[str, Any]:
            return {"Console": "javascript:alert(1)", "Logs": "https://logs.example/x", "Bad": 3}

    odd = SimpleNamespace(type="odd", instance="odd", links=Links())
    assert links.native_links(None, INCIDENT, adapter=odd) == {"Logs": "https://logs.example/x"}
    failing = SimpleNamespace(type="odd", instance="odd", links=SimpleNamespace(links=None))
    assert links.native_links(None, INCIDENT, adapter=failing) == {}
