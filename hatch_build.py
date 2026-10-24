"""Hatchling build hook for dag-healer.

The Airflow UI views of dag-healer are a pre-built JavaScript bundle
(``src/dag_healer/api/static/main.umd.cjs``) that end users install without Node.
A wheel without that bundle would install a plugin whose UI tabs fail to load, so
standard wheel builds refuse to proceed when it is missing. Editable installs
(``pip install -e .``) only warn, because developers usually iterate on Python
code first.

Set ``DAG_HEALER_ALLOW_NO_UI=1`` to build a wheel without the bundle anyway
(for example in CI jobs that only run the Python test-suite).

The page of the standalone host (``dag-healer serve``) is a second bundle,
``src/dag_healer/api/static/standalone/``. It is optional: a wheel without it still
ships the Airflow plugin, so every wheel build (standard or editable) only warns
when it is missing; ``dag-healer serve`` then answers 503 on its pages.
"""

from __future__ import annotations

import os
from collections.abc import Mapping
from pathlib import Path
from typing import Any

from hatchling.builders.hooks.plugin.interface import BuildHookInterface

BUNDLE_RELPATH = "src/dag_healer/api/static/main.umd.cjs"
STANDALONE_RELPATH = "src/dag_healer/api/static/standalone/index.js"
ALLOW_NO_UI_ENV = "DAG_HEALER_ALLOW_NO_UI"
_TRUTHY = {"1", "true", "yes", "on"}


class MissingUIBundleError(RuntimeError):
    """Raised when a wheel is built without the UI bundle."""


def bundle_present(root: str | Path) -> bool:
    """Return True when the UI bundle exists and is not empty."""
    bundle = Path(root) / BUNDLE_RELPATH
    return bundle.is_file() and bundle.stat().st_size > 0


def missing_bundle_message() -> str:
    """Explain how to produce the UI bundle."""
    return (
        f"dag-healer: the UI bundle '{BUNDLE_RELPATH}' is missing.\n"
        "  Build it first with:  python scripts/tasks.py ui\n"
        "  (it compiles ui/ with a local Node >= 20.19, or with the node:22-alpine Docker image).\n"
        f"  To build a wheel without the Airflow UI views anyway, set {ALLOW_NO_UI_ENV}=1."
    )


def check_ui_bundle(
    root: str | Path,
    *,
    editable: bool,
    environ: Mapping[str, str] | None = None,
) -> str | None:
    """Validate the UI bundle for a wheel build.

    Returns None when the bundle is present, a warning message when the build may
    continue without it (editable install or explicit opt-out), and raises
    :class:`MissingUIBundleError` otherwise.
    """
    if bundle_present(root):
        return None
    env = os.environ if environ is None else environ
    message = missing_bundle_message()
    if editable:
        return message + "\n  (editable install: continuing without the UI bundle)"
    if env.get(ALLOW_NO_UI_ENV, "").strip().lower() in _TRUTHY:
        return message + f"\n  ({ALLOW_NO_UI_ENV} is set: building the wheel without the UI bundle)"
    raise MissingUIBundleError(message)


def standalone_bundle_present(root: str | Path) -> bool:
    """Return True when the entry of the standalone UI bundle exists and is not empty."""
    entry = Path(root) / STANDALONE_RELPATH
    return entry.is_file() and entry.stat().st_size > 0


def missing_standalone_message() -> str:
    """Explain what a missing standalone bundle means and how to produce it."""
    return (
        f"dag-healer: the standalone UI bundle '{STANDALONE_RELPATH}' is missing.\n"
        "  The Airflow plugin views are not affected, but 'dag-healer serve' answers 503 on its pages.\n"
        "  Build both bundles with:  python scripts/tasks.py ui\n"
        "  (with a local Node >= 20.19, else in the node:22-alpine stage of docker/Dockerfile)."
    )


def check_standalone_bundle(root: str | Path) -> str | None:
    """Return a warning when the standalone bundle is missing, None otherwise (it never raises)."""
    return None if standalone_bundle_present(root) else missing_standalone_message()


class CustomBuildHook(BuildHookInterface):
    """Fail wheel builds that would ship without the UI bundle; warn without the standalone one."""

    PLUGIN_NAME = "custom"

    def initialize(self, version: str, build_data: dict[str, Any]) -> None:
        """Run before the wheel is assembled (``version`` is "standard" or "editable")."""
        if self.target_name != "wheel":
            return
        warning = check_ui_bundle(self.root, editable=version == "editable")
        if warning:
            self.app.display_warning(warning)
        standalone_warning = check_standalone_bundle(self.root)
        if standalone_warning:
            self.app.display_warning(standalone_warning)
