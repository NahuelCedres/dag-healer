"""The two UI bundles: how they are installed, required by the wheel build and served.

``python scripts/tasks.py ui`` copies ``ui/dist/main.umd.cjs`` (the Airflow plugin views) and
``ui/dist/standalone/`` (the page of ``dag-healer serve``) into ``src/dag_healer/api/static/``;
``hatch_build.py`` requires the first one only and warns without the second; the shell of the
standalone host loads ``healer/static/standalone/index.js``. These tests pin that the names agree
along the chain, without Node or Docker.
"""

from __future__ import annotations

import importlib.util
import json
import re
import sys
from collections.abc import Iterator
from pathlib import Path
from types import ModuleType, SimpleNamespace

import pytest

from dag_healer.api import app as app_module
from dag_healer.hosting import shell

REPO_ROOT = Path(__file__).resolve().parents[2]
HATCH_INTERFACE = "hatchling.builders.hooks.plugin.interface"


def _load(path: Path, name: str) -> ModuleType:
    spec = importlib.util.spec_from_file_location(name, path)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    sys.modules[name] = module
    spec.loader.exec_module(module)
    return module


@pytest.fixture
def tasks() -> Iterator[ModuleType]:
    module = _load(REPO_ROOT / "scripts" / "tasks.py", "dag_healer_tasks_bundles_under_test")
    yield module
    sys.modules.pop("dag_healer_tasks_bundles_under_test", None)


@pytest.fixture
def build_hook(monkeypatch: pytest.MonkeyPatch) -> Iterator[ModuleType]:
    """hatch_build.py, with a stand-in for hatchling's hook base class when hatchling is missing.

    The checks under test are plain functions; only the base class of ``CustomBuildHook`` comes
    from hatchling, which the dev environment does not install (pip builds in isolation).
    """
    if importlib.util.find_spec("hatchling") is None:
        parts = HATCH_INTERFACE.split(".")
        for depth in range(1, len(parts)):
            package = ".".join(parts[:depth])
            monkeypatch.setitem(sys.modules, package, ModuleType(package))
        interface = ModuleType(HATCH_INTERFACE)
        interface.BuildHookInterface = type("BuildHookInterface", (), {})
        monkeypatch.setitem(sys.modules, HATCH_INTERFACE, interface)
    module = _load(REPO_ROOT / "hatch_build.py", "dag_healer_hatch_build_bundles_under_test")
    yield module
    sys.modules.pop("dag_healer_hatch_build_bundles_under_test", None)


def _write(path: Path, text: str) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(text, encoding="utf-8")
    return path


def _tree(directory: Path) -> dict[str, str]:
    return {
        path.relative_to(directory).as_posix(): path.read_text(encoding="utf-8")
        for path in sorted(directory.rglob("*"))
        if path.is_file()
    }


def _hook(root: Path, warnings: list[str], *, target_name: str = "wheel") -> SimpleNamespace:
    """What ``CustomBuildHook.initialize`` reads from hatchling: the target, the root and the app."""
    return SimpleNamespace(
        target_name=target_name, root=str(root), app=SimpleNamespace(display_warning=warnings.append)
    )


# --------------------------------------------------------------------------- install


def test_install_bundle_replaces_the_standalone_directory(
    tasks: ModuleType, monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    dist = tmp_path / "dist"
    _write(dist / "main.umd.cjs", "main bundle")
    _write(dist / "standalone" / "index.js", "standalone entry")
    _write(dist / "standalone" / "assets" / "chunk.js", "standalone chunk")
    static = tmp_path / "pkg" / "static"
    _write(static / "standalone" / "index.js", "older entry")
    _write(static / "standalone" / "old.js", "stale file")
    _write(static / "standalone" / "assets" / "stale.js", "stale chunk")
    monkeypatch.setattr(tasks, "BUNDLE_PATH", static / "main.umd.cjs")

    assert tasks.install_bundle(dist) == static / "main.umd.cjs"

    assert (static / "main.umd.cjs").read_text(encoding="utf-8") == "main bundle"
    assert _tree(static / "standalone") == {
        "assets/chunk.js": "standalone chunk",
        "index.js": "standalone entry",
    }
    # The staging copy is renamed into place, never left next to the target.
    assert sorted(path.name for path in static.iterdir()) == ["main.umd.cjs", "standalone"]


def test_install_standalone_bundle_returns_the_installed_directory(
    tasks: ModuleType, monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    dist = tmp_path / "dist"
    _write(dist / "standalone" / "index.js", "standalone entry")
    target = tmp_path / "pkg" / "static" / "main.umd.cjs"
    monkeypatch.setattr(tasks, "BUNDLE_PATH", target)

    assert tasks.install_standalone_bundle(dist) == target.parent / "standalone"
    assert _tree(target.parent / "standalone") == {"index.js": "standalone entry"}


def test_dist_without_standalone_keeps_the_installed_one(
    tasks: ModuleType, monkeypatch: pytest.MonkeyPatch, tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    # An older dist, built before the standalone bundle existed, has the plugin bundle only.
    dist = tmp_path / "dist"
    _write(dist / "main.umd.cjs", "main bundle")
    _write(dist / "standalone" / "assets" / "chunk.js", "no entry next to it")
    static = tmp_path / "pkg" / "static"
    _write(static / "standalone" / "index.js", "committed entry")
    monkeypatch.setattr(tasks, "BUNDLE_PATH", static / "main.umd.cjs")

    assert tasks.install_bundle(dist) == static / "main.umd.cjs"
    assert tasks.install_standalone_bundle(dist) is None

    assert (static / "main.umd.cjs").read_text(encoding="utf-8") == "main bundle"
    assert _tree(static / "standalone") == {"index.js": "committed entry"}
    assert "no standalone UI bundle" in capsys.readouterr().out


# --------------------------------------------------------------------------- build hook


def test_build_hook_requires_only_the_plugin_bundle(build_hook: ModuleType, tmp_path: Path) -> None:
    _write(tmp_path / build_hook.BUNDLE_RELPATH, "/* bundle */")

    assert build_hook.check_ui_bundle(tmp_path, editable=False, environ={}) is None
    warning = build_hook.check_standalone_bundle(tmp_path)
    assert warning is not None
    assert build_hook.STANDALONE_RELPATH in warning
    assert "python scripts/tasks.py ui" in warning and "503" in warning

    _write(tmp_path / build_hook.STANDALONE_RELPATH, "")
    assert build_hook.check_standalone_bundle(tmp_path) is not None  # an empty entry counts as missing

    _write(tmp_path / build_hook.STANDALONE_RELPATH, "export {};")
    assert build_hook.check_standalone_bundle(tmp_path) is None


@pytest.mark.parametrize("version", ["standard", "editable"])
def test_wheel_builds_warn_without_the_standalone_bundle(
    build_hook: ModuleType, tmp_path: Path, version: str
) -> None:
    warnings: list[str] = []
    _write(tmp_path / build_hook.BUNDLE_RELPATH, "/* bundle */")

    build_hook.CustomBuildHook.initialize(_hook(tmp_path, warnings), version, {})
    assert warnings == [build_hook.missing_standalone_message()]

    warnings.clear()
    _write(tmp_path / build_hook.STANDALONE_RELPATH, "export {};")
    build_hook.CustomBuildHook.initialize(_hook(tmp_path, warnings), version, {})
    assert warnings == []


def test_sdist_builds_check_no_bundle(build_hook: ModuleType, tmp_path: Path) -> None:
    warnings: list[str] = []

    build_hook.CustomBuildHook.initialize(_hook(tmp_path, warnings, target_name="sdist"), "standard", {})
    assert warnings == []


# --------------------------------------------------------------------------- names along the chain


def test_bundle_names_agree_from_the_build_to_the_shell(tasks: ModuleType, build_hook: ModuleType) -> None:
    standalone = (shell.STANDALONE_DIR, shell.STANDALONE_ENTRY)
    assert (tasks.STANDALONE_DIRNAME, tasks.STANDALONE_ENTRY) == standalone
    # scripts/tasks.py writes where hatch_build.py looks: the package's static directory.
    static = ("src", "dag_healer", "api", "static")
    assert tasks.BUNDLE_PATH == REPO_ROOT / build_hook.BUNDLE_RELPATH
    assert Path(build_hook.BUNDLE_RELPATH).parts == (*static, "main.umd.cjs")
    assert Path(build_hook.STANDALONE_RELPATH).parts == (*static, *standalone)
    # The healer app serves that directory at /healer/static, where the shell's module script points.
    assert app_module.STATIC_DIR.parts[-3:] == static[1:]
    assert shell.bundle_file() == app_module.STATIC_DIR.joinpath(*standalone)
    assert shell.SCRIPT_PATH == "healer/static/" + "/".join(standalone)


def test_ui_scripts_build_both_bundles_where_tasks_installs_them(tasks: ModuleType) -> None:
    package = json.loads((REPO_ROOT / "ui" / "package.json").read_text(encoding="utf-8"))
    assert package["scripts"]["build"] == "vite build"
    assert package["scripts"]["build:standalone"] == "vite build --config vite.standalone.config.ts"
    # The Docker fallback builds both too (pnpm build empties dist/, so it runs first), so a
    # contributor without Node never commits a fresh plugin bundle next to a stale standalone one.
    dockerfile = (REPO_ROOT / "docker" / "Dockerfile").read_text(encoding="utf-8")
    assert "RUN pnpm build && pnpm build:standalone" in dockerfile

    config = (REPO_ROOT / "ui" / "vite.standalone.config.ts").read_text(encoding="utf-8")
    assert re.search(rf"""outDir:\s*["']dist/{re.escape(tasks.STANDALONE_DIRNAME)}["']""", config)
    assert re.search(rf"""entryFileNames:\s*["']{re.escape(tasks.STANDALONE_ENTRY)}["']""", config)


def test_the_committed_bundles_are_present(build_hook: ModuleType) -> None:
    # The wheel ships them, so the plugin and `dag-healer serve` work without Node.
    assert build_hook.check_ui_bundle(REPO_ROOT, editable=False, environ={}) is None
    assert build_hook.check_standalone_bundle(REPO_ROOT) is None
