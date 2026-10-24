"""Import hygiene of dag_healer.core: stdlib only, every module light and quick to import.

Sections 4 and 11.6 of the design; the import rules of the rest of the package are checked
by ``tests/unit/test_architecture.py``.
"""

from __future__ import annotations

import ast
import json
import os
import subprocess
import sys
from pathlib import Path

import pytest

import dag_healer.core

CORE_DIR = Path(dag_healer.core.__file__).resolve().parent
CORE_MODULES = sorted(CORE_DIR.glob("*.py"))
EXPECTED = {"__init__", "ids", "failures", "evidence", "specs", "vocab", "errors", "tracebacks", "ports"}
FORBIDDEN = ("airflow", "boto3", "botocore", "pydantic", "httpx", "pandas", "sqlalchemy", "fastapi", "yaml")
#: What ``dag_healer.core`` and ``dag_healer.orchestrators`` may add to a process's start-up on
#: top of the standard library modules they use (section 11.6). The standard library part is
#: left out of the measure: it alone takes about 30 ms on a development machine.
IMPORT_BUDGET_SECONDS = 0.05
#: Imports the standard library modules given as JSON, then times the two packages.
TIMED_IMPORT = """
import importlib, json, sys, time
for name in json.loads(sys.argv[1]):
    try:
        importlib.import_module(name)
    except ImportError:
        pass
start = time.perf_counter()
import dag_healer.core, dag_healer.orchestrators
print(time.perf_counter() - start)
"""


def test_core_has_exactly_the_designed_modules() -> None:
    assert {path.stem for path in CORE_MODULES} == EXPECTED


def _runtime_imports(tree: ast.Module) -> list[str]:
    """Absolute module names imported at runtime (``if TYPE_CHECKING:`` blocks are skipped)."""
    names: list[str] = []

    def visit(nodes: list[ast.stmt]) -> None:
        for node in nodes:
            if (
                isinstance(node, ast.If)
                and isinstance(node.test, ast.Name)
                and node.test.id == "TYPE_CHECKING"
            ):
                visit(node.orelse)
                continue
            if isinstance(node, ast.Import):
                names.extend(alias.name for alias in node.names)
            elif isinstance(node, ast.ImportFrom):
                assert node.level == 0, "core uses absolute imports"
                names.append(node.module or "")
            for child in ast.iter_child_nodes(node):
                if isinstance(child, ast.stmt):
                    visit([child])

    visit(tree.body)
    return names


@pytest.mark.parametrize("path", CORE_MODULES, ids=lambda p: p.stem)
def test_core_modules_import_only_stdlib_and_core(path: Path) -> None:
    tree = ast.parse(path.read_text(encoding="utf-8"))
    first = tree.body[1] if ast.get_docstring(tree) else tree.body[0]
    assert isinstance(first, ast.ImportFrom) and first.module == "__future__", "starts with __future__"
    for name in _runtime_imports(tree):
        top = name.split(".")[0]
        if name == "__future__" or name == "dag_healer.core" or name.startswith("dag_healer.core."):
            continue
        assert top in sys.stdlib_module_names, f"{path.name} imports {name}"


def test_importing_core_loads_no_third_party_module() -> None:
    code = (
        "import json, sys; before = set(sys.modules); import dag_healer.core; "
        "print(json.dumps(sorted(set(sys.modules) - before)))"
    )
    result = subprocess.run([sys.executable, "-c", code], capture_output=True, text=True, check=True)
    loaded = json.loads(result.stdout)

    third_party = [name for name in loaded if name.split(".")[0] not in sys.stdlib_module_names]
    assert sorted(name for name in third_party if not name.startswith("dag_healer")) == []
    assert {name for name in loaded if name.startswith("dag_healer")} == {
        "dag_healer",
        "dag_healer._version",
        *(f"dag_healer.core{'' if stem == '__init__' else '.' + stem}" for stem in EXPECTED),
    }
    assert not [name for name in loaded if name.split(".")[0] in FORBIDDEN]


def test_core_and_the_orchestrators_package_import_quickly() -> None:
    # bytecode may be written: compiling the sources is not what the bound is about
    env = {name: value for name, value in os.environ.items() if name != "PYTHONDONTWRITEBYTECODE"}
    code = (
        "import json, sys; before = set(sys.modules); import dag_healer.core, dag_healer.orchestrators; "
        "print(json.dumps(sorted(set(sys.modules) - before)))"
    )
    result = subprocess.run([sys.executable, "-c", code], capture_output=True, text=True, check=True, env=env)
    stdlib = [name for name in json.loads(result.stdout) if name.split(".")[0] in sys.stdlib_module_names]
    assert "dataclasses" in stdlib  # the baseline really holds what the packages pull in

    timings = [
        float(
            subprocess.run(
                [sys.executable, "-c", TIMED_IMPORT, json.dumps(stdlib)],
                capture_output=True,
                text=True,
                check=True,
                env=env,
            ).stdout
        )
        for _ in range(3)
    ]
    assert min(timings) < IMPORT_BUDGET_SECONDS, timings
