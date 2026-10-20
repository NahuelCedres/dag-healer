"""Tests for dag_healer.links (browser-facing URLs of the Healer views)."""

from __future__ import annotations

from urllib.parse import unquote, urlsplit

import pytest

from dag_healer import links
from dag_healer.config import config_from_mapping

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
