"""Import hygiene of dag_healer.core: stdlib only, every module light (section 4 of the design)."""

from __future__ import annotations

import ast
import json
import subprocess
import sys
from pathlib import Path

import pytest

import dag_healer.core

CORE_DIR = Path(dag_healer.core.__file__).resolve().parent
CORE_MODULES = sorted(CORE_DIR.glob("*.py"))
EXPECTED = {"__init__", "ids", "failures", "evidence", "specs", "vocab", "errors", "tracebacks", "ports"}
FORBIDDEN = ("airflow", "boto3", "botocore", "pydantic", "httpx", "pandas", "sqlalchemy", "fastapi", "yaml")


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
