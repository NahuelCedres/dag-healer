#!/usr/bin/env python3
"""Build the dag-healer UI bundles into src/dag_healer/api/static/.

Two bundles come from ui/: ``main.umd.cjs`` (the Airflow plugin views) and ``standalone/``
(the page of ``dag-healer serve``). Thin wrapper around ``python scripts/tasks.py ui`` for
build tooling (hatch_build.py points here when a bundle is missing). By default it uses a
local Node >= 20.19 / 22.12 with pnpm (or corepack) when available; otherwise it compiles ui/
in the node:22-alpine stage of docker/Dockerfile. Both build both bundles. Standard library
only; the exit code is the build's exit code.
"""

from __future__ import annotations

import argparse
import importlib.util
import sys
from collections.abc import Sequence
from pathlib import Path
from types import ModuleType

TASKS_PATH = Path(__file__).resolve().parent / "tasks.py"


def load_tasks() -> ModuleType:
    """Import scripts/tasks.py by path (it is not an installed module)."""
    spec = importlib.util.spec_from_file_location("dag_healer_tasks", TASKS_PATH)
    if spec is None or spec.loader is None:
        raise ImportError(f"cannot load {TASKS_PATH}")
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


def main(argv: Sequence[str] | None = None) -> int:
    """Build the bundles and return the exit code."""
    parser = argparse.ArgumentParser(
        prog="python scripts/build_ui.py",
        description="Build src/dag_healer/api/static/main.umd.cjs and standalone/ from ui/.",
    )
    parser.add_argument(
        "--mode",
        choices=("auto", "local", "docker"),
        default="auto",
        help="auto: local Node if suitable, else Docker (default)",
    )
    args = parser.parse_args(argv)
    tasks = load_tasks()
    try:
        return int(tasks.build_ui(args.mode))
    except tasks.TaskError as exc:
        print(f"error: {exc}", file=sys.stderr)
        return exc.code
    except KeyboardInterrupt:
        print("interrupted", file=sys.stderr)
        return 130


if __name__ == "__main__":
    sys.exit(main())
