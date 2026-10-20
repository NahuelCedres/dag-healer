"""The dag-healer plugin as Airflow sees it (entry point ``airflow.plugins``)."""

from __future__ import annotations

from typing import Any

from airflow import plugins_manager

EXPECTED_REACT_APPS = {
    "healer": ("Healer", "nav"),
    "healer-dag": ("Healer incidents", "dag"),
    "healer-ti": ("Healer incident", "task_instance"),
}


def _healer_info() -> dict[str, Any]:
    plugins_manager.ensure_plugins_loaded()
    infos = plugins_manager.get_plugin_info(["name", "react_apps", "fastapi_apps", "listeners", "source"])
    matches = [info for info in infos if info["name"] == "dag_healer"]
    assert len(matches) == 1, [info["name"] for info in infos]
    return matches[0]


def test_plugin_is_registered_without_import_errors() -> None:
    _healer_info()
    errors = plugins_manager.get_import_errors()
    assert not [source for source in errors if "dag_healer" in str(source)], errors


def test_plugin_react_apps() -> None:
    react_apps = _healer_info()["react_apps"]
    assert len(react_apps) == 3
    by_route = {app["url_route"]: app for app in react_apps}
    assert set(by_route) == set(EXPECTED_REACT_APPS)
    for route, (name, destination) in EXPECTED_REACT_APPS.items():
        app = by_route[route]
        assert app["name"] == name
        assert app["destination"] == destination
        assert app["bundle_url"].startswith("healer/static/main.umd.cjs")
    assert by_route["healer"].get("nav_top_level") is True


def test_plugin_fastapi_app_and_listener() -> None:
    from fastapi import FastAPI

    info = _healer_info()
    assert info["listeners"] == ["dag_healer.airflow.listener"]
    (app_info,) = info["fastapi_apps"]
    assert app_info["url_prefix"] == "/healer"
    assert app_info["app"].endswith("FastAPI")
    apps, _middlewares = plugins_manager.get_fastapi_plugins()
    healer_apps = [entry for entry in apps if entry.get("url_prefix") == "/healer"]
    assert len(healer_apps) == 1
    assert isinstance(healer_apps[0]["app"], FastAPI)


def test_plugins_endpoint_lists_the_react_apps(client: Any, tokens: dict[str, str]) -> None:
    response = client.get("/api/v2/plugins", headers={"Authorization": f"Bearer {tokens['admin']}"})
    assert response.status_code == 200
    (plugin,) = [p for p in response.json()["plugins"] if p["name"] == "dag_healer"]
    assert {app["url_route"] for app in plugin["react_apps"]} == set(EXPECTED_REACT_APPS)
    assert [app["url_prefix"] for app in plugin["fastapi_apps"]] == ["/healer"]
