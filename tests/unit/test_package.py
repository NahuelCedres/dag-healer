"""Packaging, import-weight, plugin, listener, FastAPI app and CLI skeleton tests."""

from __future__ import annotations

import importlib
import importlib.util
import json
import mimetypes
import os
import subprocess
import sys
import textwrap
import types
from collections.abc import Iterator
from pathlib import Path
from types import SimpleNamespace
from typing import Any

import pytest

import dag_healer
from dag_healer._version import __version__

REPO_ROOT = Path(__file__).resolve().parents[2]
SRC = REPO_ROOT / "src"
HEAVY_MODULES = ("pandas", "sqlalchemy", "fastapi", "starlette", "pydantic", "airflow", "anthropic", "httpx")

# Installs a minimal fake "airflow" package so plugin.py / listener.py can be imported
# on hosts without Airflow. Used both in-process (fixture) and in subprocesses.
FAKE_AIRFLOW_SNIPPET = textwrap.dedent(
    """
    import sys, types

    def _install_fake_airflow():
        root = types.ModuleType("airflow")
        root.__path__ = []
        listeners = types.ModuleType("airflow.listeners")
        listeners.hookimpl = lambda func: func
        plugins_manager = types.ModuleType("airflow.plugins_manager")

        class AirflowPlugin:
            name = None
            listeners = []
            react_apps = []
            fastapi_apps = []
            fastapi_root_middlewares = []
            external_views = []

        plugins_manager.AirflowPlugin = AirflowPlugin
        root.listeners = listeners
        root.plugins_manager = plugins_manager
        modules = {
            "airflow": root,
            "airflow.listeners": listeners,
            "airflow.plugins_manager": plugins_manager,
        }
        sys.modules.update(modules)
        return modules
    """
)


def _run_python(code: str, **env_overrides: str) -> subprocess.CompletedProcess[str]:
    env = dict(os.environ)
    env["PYTHONPATH"] = os.pathsep.join([str(SRC), env.get("PYTHONPATH", "")]).rstrip(os.pathsep)
    env.update(env_overrides)
    return subprocess.run(
        [sys.executable, "-c", code], capture_output=True, text=True, env=env, cwd=str(REPO_ROOT), timeout=120
    )


def _loaded_heavy_modules_script(imports: str, *, fake_airflow: bool = False) -> str:
    prelude = FAKE_AIRFLOW_SNIPPET + "\n_install_fake_airflow()\n" if fake_airflow else ""
    report = textwrap.dedent(
        f"""
        import json, sys
        heavy = {list(HEAVY_MODULES)!r}
        fake = {fake_airflow!r}
        loaded = sorted(
            name for name in sys.modules
            if name.split(".")[0] in heavy and not (fake and name.split(".")[0] == "airflow")
        )
        print(json.dumps(loaded))
        """
    )
    return "\n".join([prelude, imports, report])


# --------------------------------------------------------------------------- metadata


def test_version() -> None:
    assert __version__ == "0.1.0"
    assert dag_healer.__version__ == __version__


def test_pyproject_declares_entry_points() -> None:
    tomllib = pytest.importorskip("tomllib")
    data = tomllib.loads((REPO_ROOT / "pyproject.toml").read_text(encoding="utf-8"))
    project = data["project"]
    assert project["name"] == "dag-healer"
    assert project["dynamic"] == ["version"]
    assert project["requires-python"] == ">=3.10"
    assert project["scripts"]["dag-healer"] == "dag_healer.cli.main:main"
    assert (
        project["entry-points"]["airflow.plugins"]["dag_healer"] == "dag_healer.airflow.plugin:HealerPlugin"
    )
    assert project["optional-dependencies"]["airflow"] == ["apache-airflow>=3.1"]
    assert data["tool"]["hatch"]["version"]["path"] == "src/dag_healer/_version.py"
    assert data["build-system"]["build-backend"] == "hatchling.build"


# --------------------------------------------------------------------------- import weight


def test_package_import_is_light() -> None:
    script = _loaded_heavy_modules_script(
        "import dag_healer, dag_healer.ingest, dag_healer.airflow, dag_healer.airflow.callback\n"
        "assert dag_healer.callback is dag_healer.airflow.callback.on_failure_callback"
    )
    result = _run_python(script)
    assert result.returncode == 0, result.stderr
    assert json.loads(result.stdout.strip().splitlines()[-1]) == []


def test_plugin_and_listener_import_is_light() -> None:
    script = _loaded_heavy_modules_script(
        "import dag_healer.airflow.listener, dag_healer.airflow.plugin\n"
        "plugin_cls = dag_healer.airflow.plugin.HealerPlugin\n"
        "assert plugin_cls.name == 'dag_healer'",
        fake_airflow=True,
    )
    result = _run_python(script)
    assert result.returncode == 0, result.stderr
    assert json.loads(result.stdout.strip().splitlines()[-1]) == []


# --------------------------------------------------------------------------- plugin + listener


@pytest.fixture
def fake_airflow() -> Iterator[dict[str, types.ModuleType]]:
    """Install fake airflow modules and import fresh copies of plugin/listener."""
    import dag_healer.airflow as healer_airflow

    namespace: dict[str, Any] = {}
    exec(FAKE_AIRFLOW_SNIPPET, namespace)  # noqa: S102 - trusted test snippet
    submodules = ("listener", "plugin")
    saved_modules = dict(sys.modules)
    saved_attrs = {name: vars(healer_airflow).get(name) for name in submodules}
    for name in submodules:
        sys.modules.pop(f"dag_healer.airflow.{name}", None)
    modules = namespace["_install_fake_airflow"]()
    try:
        yield modules
    finally:
        for name in [*modules, *(f"dag_healer.airflow.{sub}" for sub in submodules)]:
            sys.modules.pop(name, None)
            if name in saved_modules:
                sys.modules[name] = saved_modules[name]
        for name, value in saved_attrs.items():
            if value is None:
                vars(healer_airflow).pop(name, None)
            else:
                setattr(healer_airflow, name, value)


def test_plugin_definition(
    fake_airflow: dict[str, types.ModuleType], monkeypatch: pytest.MonkeyPatch
) -> None:
    plugin_module = importlib.import_module("dag_healer.airflow.plugin")
    listener_module = importlib.import_module("dag_healer.airflow.listener")
    plugin_cls = plugin_module.HealerPlugin

    assert issubclass(plugin_cls, fake_airflow["airflow.plugins_manager"].AirflowPlugin)
    assert plugin_cls.name == "dag_healer"
    assert plugin_cls.listeners == [listener_module]

    apps = {app["name"]: app for app in plugin_cls.react_apps}
    assert set(apps) == {"Healer", "Healer incidents", "Healer incident"}
    assert apps["Healer"]["destination"] == "nav"
    assert apps["Healer"]["url_route"] == "healer"
    assert apps["Healer"]["nav_top_level"] is True
    assert apps["Healer incidents"]["destination"] == "dag"
    assert apps["Healer incidents"]["url_route"] == "healer-dag"
    assert apps["Healer incident"]["destination"] == "task_instance"
    assert apps["Healer incident"]["url_route"] == "healer-ti"
    for app in apps.values():
        assert app["bundle_url"] == f"healer/static/main.umd.cjs?v={__version__}"
        assert not app["bundle_url"].startswith("/")
        assert app["icon"] == "healer/static/icon.svg"

    # fastapi_apps is a lazy property: a descriptor on the class, a list on instances.
    # get_app is replaced so the process-wide memoized app is not built under fake Airflow.
    from dag_healer.api import app as app_module

    sentinel = app_module.create_app()
    monkeypatch.setattr(app_module, "get_app", lambda: sentinel)
    assert isinstance(vars(plugin_cls)["fastapi_apps"], property)
    (fastapi_app,) = plugin_cls().fastapi_apps
    assert fastapi_app == {"app": sentinel, "url_prefix": "/healer", "name": "dag-healer"}


def test_listener_reports_and_never_raises(
    fake_airflow: dict[str, types.ModuleType], monkeypatch: pytest.MonkeyPatch
) -> None:
    listener = importlib.import_module("dag_healer.airflow.listener")
    calls: list[tuple[dict[str, Any], dict[str, Any]]] = []

    def fake_report(payload: dict[str, Any], **kwargs: Any) -> bool:
        calls.append((payload, kwargs))
        return True

    monkeypatch.setattr(listener, "report_failure", fake_report)
    monkeypatch.setenv("HEALER_API_URL", "http://api-server:8080")
    ti = SimpleNamespace(
        dag_id="d", task_id="t", run_id="r", map_index=3, try_number=1, max_tries=0, state="failed"
    )

    assert listener.on_task_instance_failed(previous_state="running", task_instance=ti, error="boom") is None
    (payload, kwargs) = calls[0]
    assert payload["source"] == "listener"
    assert payload["map_index"] == 3
    assert payload["error"] == "boom"
    assert kwargs["api_url"] == "http://api-server:8080"

    def explode(*args: Any, **kwargs: Any) -> bool:
        raise RuntimeError("network stack on fire")

    monkeypatch.setattr(listener, "report_failure", explode)
    listener.on_task_instance_failed(previous_state=None, task_instance=ti, error=None)
    listener.on_task_instance_failed(previous_state=None, task_instance=object(), error=object())
    listener.on_dag_run_failed(dag_run=SimpleNamespace(dag_id="d", run_id="r"), msg="task_failure")
    listener.on_dag_run_failed(dag_run=None, msg=None)


def test_listener_hook_signatures_match_airflow_specs(fake_airflow: dict[str, types.ModuleType]) -> None:
    import inspect

    listener = importlib.import_module("dag_healer.airflow.listener")
    params = inspect.signature(listener.on_task_instance_failed).parameters
    assert list(params) == ["previous_state", "task_instance", "error"]
    assert list(inspect.signature(listener.on_dag_run_failed).parameters) == ["dag_run", "msg"]


# --------------------------------------------------------------------------- FastAPI app


@pytest.fixture
def client(healer_env: Path) -> Iterator[Any]:
    from fastapi.testclient import TestClient

    from dag_healer.api.app import create_app

    with TestClient(create_app()) as test_client:
        yield test_client


def test_health(client: Any) -> None:
    response = client.get("/health")
    assert response.status_code == 200
    assert response.json() == {"status": "ok", "version": __version__}


def test_static_icon(client: Any) -> None:
    response = client.get("/static/icon.svg")
    assert response.status_code == 200
    assert response.headers["content-type"].startswith("image/svg+xml")
    assert response.headers["cache-control"] == "no-cache"
    assert "currentColor" in response.text
    assert client.get("/static/missing.js").status_code == 404
    assert client.get("/static/../app.py").status_code == 404


def test_bundle_mimetype_registered(client: Any) -> None:
    assert mimetypes.guess_type("main.umd.cjs")[0] == "application/javascript"
    assert mimetypes.guess_type("main.js")[0] == "application/javascript"


def test_docs_disabled_by_default(client: Any) -> None:
    assert client.get("/docs").status_code == 404
    assert client.get("/openapi.json").status_code == 404


def test_docs_enabled_in_debug(healer_env: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    from fastapi.testclient import TestClient

    from dag_healer.api.app import create_app

    monkeypatch.setenv("HEALER_DEBUG", "1")
    with TestClient(create_app()) as debug_client:
        assert debug_client.get("/openapi.json").status_code == 200


def test_get_app_is_memoized() -> None:
    from dag_healer.api.app import get_app

    assert get_app() is get_app()


def test_include_routers_contract(monkeypatch: pytest.MonkeyPatch) -> None:
    from fastapi import APIRouter, FastAPI

    from dag_healer.api import app as app_module

    router = APIRouter(prefix="/api/v1")

    @router.get("/ping")
    def ping() -> dict[str, str]:
        return {"pong": "yes"}

    fake_routers = types.ModuleType(app_module.ROUTERS_MODULE)
    fake_routers.routers = [router]  # type: ignore[attr-defined]
    monkeypatch.setattr(app_module, "_import_routers_module", lambda: fake_routers)
    target = FastAPI()
    app_module.include_routers(target)
    from fastapi.testclient import TestClient

    with TestClient(target) as test_client:
        assert test_client.get("/api/v1/ping").json() == {"pong": "yes"}

    called: list[Any] = []
    fake_routers.include_routers = called.append  # type: ignore[attr-defined]
    app_module.include_routers(target)
    assert called == [target]

    monkeypatch.setattr(app_module, "_import_routers_module", lambda: None)
    app_module.include_routers(FastAPI())  # no routers package yet: nothing to do


def test_include_routers_skips_when_airflow_missing(monkeypatch: pytest.MonkeyPatch) -> None:
    from fastapi import FastAPI

    from dag_healer.api import app as app_module

    def fail() -> None:
        raise ModuleNotFoundError("No module named 'airflow.api_fastapi'", name="airflow.api_fastapi")

    monkeypatch.setattr(app_module, "_import_routers_module", fail)
    monkeypatch.setattr(app_module, "_airflow_installed", lambda: False)
    app_module.include_routers(FastAPI())  # does not raise: API routes disabled

    monkeypatch.setattr(app_module, "_airflow_installed", lambda: True)
    with pytest.raises(ModuleNotFoundError):  # Airflow present but broken: loud
        app_module.include_routers(FastAPI())

    def fail_other() -> None:
        raise ModuleNotFoundError("No module named 'nope'", name="nope")

    monkeypatch.setattr(app_module, "_import_routers_module", fail_other)
    with pytest.raises(ModuleNotFoundError):
        app_module.include_routers(FastAPI())


# --------------------------------------------------------------------------- CLI


def test_cli_version(capsys: pytest.CaptureFixture[str]) -> None:
    from dag_healer.cli.main import main

    assert main(["version"]) == 0
    assert capsys.readouterr().out.strip() == f"dag-healer {__version__}"


def test_cli_without_command_prints_help(capsys: pytest.CaptureFixture[str]) -> None:
    from dag_healer.cli.main import main

    assert main([]) == 2
    assert "usage: dag-healer" in capsys.readouterr().err


def test_cli_registry(monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]) -> None:
    from dag_healer.cli import main as cli

    monkeypatch.setattr(cli, "COMMANDS", dict(cli.COMMANDS))
    seen: dict[str, Any] = {}

    def configure(parser: Any) -> None:
        parser.add_argument("--name", default="world")

    def handler(args: Any) -> int:
        seen["name"] = args.name
        return 3

    cli.register_command("hello", help="Say hello.", handler=handler, configure=configure)
    assert cli.main(["hello", "--name", "airflow"]) == 3
    assert seen == {"name": "airflow"}


@pytest.mark.parametrize("module", ["dag_healer.cli.main", "dag_healer"])
def test_cli_runs_as_module(module: str) -> None:
    env = dict(os.environ)
    env["PYTHONPATH"] = os.pathsep.join([str(SRC), env.get("PYTHONPATH", "")]).rstrip(os.pathsep)
    result = subprocess.run(
        [sys.executable, "-m", module, "version"], capture_output=True, text=True, env=env, timeout=120
    )
    assert result.returncode == 0, result.stderr
    assert result.stdout.strip() == f"dag-healer {__version__}"


# --------------------------------------------------------------------------- build hook


@pytest.fixture
def build_hook() -> Any:
    pytest.importorskip("hatchling")
    spec = importlib.util.spec_from_file_location("dag_healer_hatch_build", REPO_ROOT / "hatch_build.py")
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def test_build_hook_requires_bundle(build_hook: Any, tmp_path: Path) -> None:
    with pytest.raises(build_hook.MissingUIBundleError, match="python scripts/tasks.py ui"):
        build_hook.check_ui_bundle(tmp_path, editable=False, environ={})

    allowed = build_hook.check_ui_bundle(tmp_path, editable=False, environ={"DAG_HEALER_ALLOW_NO_UI": "1"})
    assert allowed is not None and "DAG_HEALER_ALLOW_NO_UI" in allowed
    assert build_hook.check_ui_bundle(tmp_path, editable=True, environ={}) is not None

    bundle = tmp_path / build_hook.BUNDLE_RELPATH
    bundle.parent.mkdir(parents=True)
    bundle.write_text("/* bundle */", encoding="utf-8")
    assert build_hook.check_ui_bundle(tmp_path, editable=False, environ={}) is None
