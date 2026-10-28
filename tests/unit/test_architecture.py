"""Architecture guards of the orchestrator refactor (section 11.6 of ``docs/architecture/orchestrators.md``).

* The import rules of section 4, checked on the source with :mod:`ast` (module level,
  function bodies and ``if TYPE_CHECKING:`` blocks alike): the neutral core never imports an
  orchestrator's code or SDK; only the Airflow adapter's runtime modules and ``api/auth.py``
  import Airflow; boto3 / botocore are imported only lazily, by the modules that own them (the
  Step Functions adapter and its offline AWS, the S3 dataset reader, doctor's AWS credential
  probe). The anthropic SDK is imported only lazily, by the two LLM providers; the Amazon
  Bedrock provider (``agent/bedrock.py``) is core, so it reaches boto3 / botocore only through
  that SDK, never with an import of its own.
* The standalone host (step 7): ``hosting/`` and ``cli/serve_cmd.py`` follow the core's rules
  (FastAPI and Starlette are fine); only ``cli/serve_cmd.py`` imports ``hosting``, and neither
  the core nor ``hosting`` imports the CLI; the authentication backends never import the host
  modules that hand them the authorizer; the packages of the ``serve`` extra (PyJWT,
  cryptography, uvicorn) are imported lazily, each by its one user.
* The test sandbox (``repo/sandbox.py``) is core, and stricter: the standard library and
  ``repo/git.py`` only. It runs the engine CLI through ``subprocess`` (no container SDK) and
  reads ``repo.sandbox`` through a protocol; ``repo/pytest_runner.py`` imports it, never the
  other way round, and ``config.py`` never imports it at module level.
* The protected-path policy (``repo/protected_paths.py``) is core and the standard library
  only, so ``config.py`` can validate ``repo.allowed_paths`` with it without a cycle; it does
  so lazily. The read-only GitHub setup checks (``pr/github_checks.py``) are core and get
  every setting from their callers: they never import the configuration, the agent, the
  worker or the CLI, directly or through another module.
* A text guard: no Airflow REST client call outside ``airflow/`` and ``cli/doctor_cmd.py``
  (``scripts/`` is not part of the package).

The other guards of 11.6 sit next to what they protect: ``core/test_core_imports.py``
(``dag_healer.core`` is stdlib only and imports quickly, together with the orchestrators
package), ``orchestrators/test_registry.py`` (the orchestrators package, the registry and
compat import nothing heavy) and ``test_package.py`` (the task-runner import graph).
"""

from __future__ import annotations

import ast
import json
import re
import subprocess
import sys
from collections.abc import Iterator
from pathlib import Path
from typing import NamedTuple

import pytest

import dag_healer

PACKAGE_DIR = Path(dag_healer.__file__).resolve().parent
SRC_DIR = PACKAGE_DIR.parent
PACKAGE = "dag_healer"

#: The neutral core (goal 1 of the design, ``ui/`` aside): package directories and modules.
CORE_PACKAGES = ("agent", "worker", "store", "contracts", "alerts", "pr", "api")
CORE_MODULES = (
    "repo/git.py",
    "repo/profile.py",
    "repo/protected_paths.py",
    "repo/pytest_runner.py",
    "repo/sandbox.py",
    "masking.py",
    "datasets.py",
    "links.py",
)
#: Part of ``api/`` but the Airflow host's authentication, so not neutral.
NOT_CORE = ("api/auth.py",)
#: The orchestrators modules the core may import; they obey the core's rules.
BRIDGE_MODULES = ("orchestrators/__init__.py", "orchestrators/registry.py", "orchestrators/compat.py")

#: Modules that import ``airflow`` at module level (Airflow itself loads them).
AIRFLOW_AT_MODULE_LEVEL = frozenset({"airflow/plugin.py", "airflow/listener.py"})
#: Modules that import ``airflow`` inside functions, which only run in Airflow processes.
AIRFLOW_LAZILY = frozenset({"airflow/__init__.py", "api/auth.py"})
#: The only modules that may import boto3 / botocore, and only inside functions (section 4).
#: ``stepfunctions/testing.py`` is the offline AWS of the tests: it builds real botocore clients;
#: ``cli/doctor_cmd.py`` resolves the bedrock provider's AWS credentials with a botocore session.
AWS_SDK_LAZILY = frozenset(
    {"stepfunctions/aws.py", "stepfunctions/testing.py", "storage/s3.py", "cli/doctor_cmd.py"}
)
#: The only modules that may import the anthropic SDK (the LLM providers), and only inside
#: functions: importing the agent loads no SDK, and the ``aws`` extra stays optional.
ANTHROPIC_SDK_LAZILY = frozenset({"agent/llm.py", "agent/bedrock.py"})
#: Claude on Amazon Bedrock: core, so boto3 / botocore reach it only through the anthropic SDK.
BEDROCK_PROVIDER = "agent/bedrock.py"

#: The standalone host (``dag-healer serve``): the package and the command that runs it.
HOSTING_DIR = "hosting"
SERVE_COMMAND = "cli/serve_cmd.py"
#: The modules of ``hosting/auth`` must not import: the host modules hand the backends their authorizer.
HOST_MODULES = ("dag_healer.hosting.app", "dag_healer.hosting.rbac", "dag_healer.hosting.shell")
#: The packages of the ``serve`` extra, each imported only inside the functions of its one user, so
#: Airflow and the worker run without the extra.
SERVE_EXTRA_LAZILY: dict[str, str] = {
    "jwt": "hosting/auth/alb_oidc.py",
    "cryptography": "hosting/auth/alb_oidc.py",
    "uvicorn": "cli/serve_cmd.py",
}

#: The test sandbox: besides the standard library it imports ``repo/git.py`` only, and the
#: configuration only for type checking (it reads ``repo.sandbox`` through a protocol).
SANDBOX_MODULE = "repo/sandbox.py"
SANDBOX_MAY_IMPORT = frozenset({f"{PACKAGE}.repo.git"})
SANDBOX_MAY_IMPORT_FOR_TYPE_CHECKING = frozenset({f"{PACKAGE}.config"})

#: The protected-path policy: one definition for the write tools, the final check before the
#: commit and ``repo.allowed_paths``. The standard library only, since ``config.py`` uses it.
PROTECTED_PATHS_MODULE = "repo/protected_paths.py"
#: The read-only GitHub setup checks, and what they never import: their callers (a repair and
#: ``doctor``) hand them every setting.
GITHUB_CHECKS_MODULE = "pr/github_checks.py"
GITHUB_CHECKS_NEVER_IMPORT = (f"{PACKAGE}.config", f"{PACKAGE}.agent", f"{PACKAGE}.worker", f"{PACKAGE}.cli")

#: Calls of the Airflow REST client (``dag_healer.airflow.client.AirflowClient``).
AIRFLOW_CLIENT_CALL = re.compile(
    r"\.(list_failed_task_instances|get_task_instance|get_task_log|get_dag|get_dag_source"
    r"|clear_task_instances|trigger_dag_run)\("
)
#: Where those calls may appear: the Airflow adapter and the doctor's own HTTP checks.
CLIENT_CALLS_ALLOWED_IN = ("airflow/", "cli/doctor_cmd.py")


class Import(NamedTuple):
    """One imported module name of a source file."""

    module: str  # absolute dotted name
    line: int
    lazy: bool  # inside a function body
    type_checking: bool  # inside ``if TYPE_CHECKING:``


def _relative(path: Path) -> str:
    return path.relative_to(PACKAGE_DIR).as_posix()


def _module_name(path: Path) -> str:
    parts = path.relative_to(SRC_DIR).with_suffix("").parts
    return ".".join(parts[:-1] if parts[-1] == "__init__" else parts)


def _is_package_module(name: str) -> bool:
    """True when ``name`` is a module or package of this distribution (``dag_healer.x.y``)."""
    if name != PACKAGE and not name.startswith(PACKAGE + "."):
        return False
    base = SRC_DIR.joinpath(*name.split("."))
    return base.with_suffix(".py").is_file() or (base / "__init__.py").is_file()


def _is_type_checking(test: ast.expr) -> bool:
    return (isinstance(test, ast.Name) and test.id == "TYPE_CHECKING") or (
        isinstance(test, ast.Attribute) and test.attr == "TYPE_CHECKING"
    )


def _absolute(path: Path, node: ast.ImportFrom) -> str:
    """The absolute module of ``from <module> import ...`` (relative imports resolved)."""
    if not node.level:
        return node.module or ""
    package = _module_name(path).split(".")
    if path.name != "__init__.py":
        package = package[:-1]
    base = package[: len(package) - (node.level - 1)]
    return ".".join([*base, node.module] if node.module else base)


def imports_of(path: Path) -> list[Import]:
    """Every module ``path`` imports, wherever the import statement is.

    ``from package import name`` also counts as importing ``package.name`` when that is a
    module of this distribution (``from dag_healer.orchestrators import fake``).
    """
    tree = ast.parse(path.read_text(encoding="utf-8"), filename=str(path))
    found: list[Import] = []

    def visit(node: ast.AST, *, lazy: bool, type_checking: bool) -> None:
        if isinstance(node, ast.Import):
            found.extend(Import(alias.name, node.lineno, lazy, type_checking) for alias in node.names)
        elif isinstance(node, ast.ImportFrom):
            module = _absolute(path, node)
            found.append(Import(module, node.lineno, lazy, type_checking))
            for alias in node.names:
                name = f"{module}.{alias.name}"
                if _is_package_module(name):
                    found.append(Import(name, node.lineno, lazy, type_checking))
        if isinstance(node, ast.If) and _is_type_checking(node.test):
            for child in node.body:
                visit(child, lazy=lazy, type_checking=True)
            for child in node.orelse:
                visit(child, lazy=lazy, type_checking=type_checking)
            return
        inner = lazy or isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef, ast.Lambda))
        for child in ast.iter_child_nodes(node):
            visit(child, lazy=inner, type_checking=type_checking)

    visit(tree, lazy=False, type_checking=False)
    return found


def _package_files() -> list[Path]:
    return sorted(PACKAGE_DIR.rglob("*.py"))


def _core_files() -> list[Path]:
    files = [path for name in CORE_PACKAGES for path in sorted((PACKAGE_DIR / name).rglob("*.py"))]
    files += [PACKAGE_DIR / name for name in CORE_MODULES]
    return [path for path in files if _relative(path) not in NOT_CORE]


def _hosting_files() -> list[Path]:
    """The standalone host: ``hosting/**`` and the ``serve`` command."""
    return [*sorted((PACKAGE_DIR / HOSTING_DIR).rglob("*.py")), PACKAGE_DIR / SERVE_COMMAND]


def _within(name: str, package: str) -> bool:
    return name == package or name.startswith(package + ".")


def forbidden_for_the_core(name: str) -> bool:
    """True for an import the neutral core must not make (section 4 of the design).

    No orchestrator SDK (``airflow``, ``boto3``, ``botocore``), no adapter package
    (``dag_healer.airflow``, ``dag_healer.stepfunctions``) and, from ``dag_healer.orchestrators``,
    nothing but the package itself, ``registry`` and ``compat`` (never the fake or the
    conformance suite).
    """
    if name.split(".")[0] in ("airflow", "boto3", "botocore"):
        return True
    if _within(name, f"{PACKAGE}.airflow") or _within(name, f"{PACKAGE}.stepfunctions"):
        return True
    orchestrators = f"{PACKAGE}.orchestrators"
    if _within(name, orchestrators) and name != orchestrators:
        return not (_within(name, f"{orchestrators}.registry") or _within(name, f"{orchestrators}.compat"))
    return False


def _airflow_importers() -> Iterator[tuple[str, Import]]:
    for path in _package_files():
        for item in imports_of(path):
            if item.module.split(".")[0] == "airflow":
                yield _relative(path), item


# =========================================================================== the scanner itself


def test_the_core_scope_exists_and_leaves_out_the_airflow_host() -> None:
    files = {_relative(path) for path in _core_files()}
    assert {"agent/context.py", "worker/pipeline.py", "api/views.py", "links.py", "repo/git.py"} <= files
    assert BEDROCK_PROVIDER in files
    assert "api/auth.py" not in files
    assert not {name for name in files if name.startswith(("airflow/", "orchestrators/", "core/", "cli/"))}
    for name in (*CORE_MODULES, *BRIDGE_MODULES, *AIRFLOW_AT_MODULE_LEVEL, *AIRFLOW_LAZILY):
        assert (PACKAGE_DIR / name).is_file(), name


def test_the_scanner_sees_every_kind_of_import(tmp_path: Path) -> None:
    module = tmp_path / "probe.py"
    module.write_text(
        "from __future__ import annotations\n"
        "import os.path\n"
        "from typing import TYPE_CHECKING\n"
        "from dag_healer import links\n"
        "if TYPE_CHECKING:\n"
        "    from dag_healer.airflow.client import AirflowClient\n"
        "else:\n"
        "    import json\n"
        "def later():\n"
        "    import boto3\n"
        "    from dag_healer.orchestrators import fake\n",
        encoding="utf-8",
    )
    found = {item.module: (item.lazy, item.type_checking) for item in imports_of(module)}
    assert found["os.path"] == (False, False)
    assert found["dag_healer.links"] == (False, False)
    assert found["dag_healer.airflow.client"] == (False, True)
    assert found["json"] == (False, False)
    assert found["boto3"] == (True, False)
    assert found["dag_healer.orchestrators.fake"] == (True, False)
    assert "dag_healer.orchestrators" in found


@pytest.mark.parametrize(
    ("name", "forbidden"),
    [
        ("airflow.models", True),
        ("boto3", True),
        ("botocore.exceptions", True),
        ("dag_healer.airflow", True),
        ("dag_healer.airflow.client", True),
        ("dag_healer.airflow.errors", True),
        ("dag_healer.stepfunctions.aws", True),
        ("dag_healer.orchestrators.fake", True),
        ("dag_healer.orchestrators.conformance", True),
        ("dag_healer.orchestrators", False),
        ("dag_healer.orchestrators.registry", False),
        ("dag_healer.orchestrators.compat", False),
        ("dag_healer.core.ports", False),
        ("dag_healer.ingest", False),
        ("dag_healer.repo.dagscan", False),
        ("airflowish", False),
    ],
)
def test_the_core_rule(name: str, forbidden: bool) -> None:
    assert forbidden_for_the_core(name) is forbidden


# =========================================================================== import rules (section 4)


@pytest.mark.parametrize(
    "path", [*_core_files(), *(PACKAGE_DIR / name for name in BRIDGE_MODULES)], ids=_relative
)
def test_the_core_never_imports_an_orchestrator(path: Path) -> None:
    """The core reaches an orchestrator only through ``dag_healer.core.ports`` and an adapter
    from the registry (or injected); ``TYPE_CHECKING`` imports count too."""
    bad = [
        f"line {item.line}: {item.module}" for item in imports_of(path) if forbidden_for_the_core(item.module)
    ]
    assert bad == [], f"{_relative(path)} imports orchestrator code"


def test_only_the_airflow_runtime_modules_and_the_airflow_auth_import_airflow() -> None:
    importers: dict[str, set[str]] = {}
    for name, item in _airflow_importers():
        importers.setdefault(name, set()).add("lazily" if item.lazy else "at module level")
    assert set(importers) <= AIRFLOW_AT_MODULE_LEVEL | AIRFLOW_LAZILY, importers
    assert AIRFLOW_AT_MODULE_LEVEL <= set(importers)  # the guard does see Airflow imports
    at_module_level = {name for name, how in importers.items() if "at module level" in how}
    assert at_module_level <= AIRFLOW_AT_MODULE_LEVEL, importers


def test_the_aws_sdk_is_imported_only_lazily_by_its_owners() -> None:
    importers = {
        f"{_relative(path)}:{item.line}": item.lazy
        for path in _package_files()
        for item in imports_of(path)
        if item.module.split(".")[0] in ("boto3", "botocore")
    }
    owners = {name.split(":")[0] for name in importers}
    assert owners == AWS_SDK_LAZILY, importers  # the guard sees every owner
    assert all(importers.values()), importers


def test_the_anthropic_sdk_is_imported_only_lazily_by_the_llm_providers() -> None:
    importers = {
        f"{_relative(path)}:{item.line}": item.lazy
        for path in _package_files()
        for item in imports_of(path)
        if item.module.split(".")[0] == "anthropic"
    }
    owners = {name.split(":")[0] for name in importers}
    assert owners == ANTHROPIC_SDK_LAZILY, importers
    assert all(importers.values()), importers


def test_the_bedrock_provider_reaches_the_aws_sdk_only_through_anthropic() -> None:
    """No boto3 / botocore import of its own, not even a lazy one: it checks that the ``aws``
    extra is installed with ``importlib.util.find_spec`` and lets the anthropic SDK import it."""
    found = imports_of(PACKAGE_DIR / BEDROCK_PROVIDER)
    assert [item for item in found if item.module.split(".")[0] in ("boto3", "botocore")] == []
    assert {"anthropic", "importlib.util"} <= {item.module for item in found}  # the guard sees them
    assert all(item.lazy for item in found if item.module == "anthropic")


# =========================================================================== the standalone host (step 7)


def test_the_standalone_host_scope_exists() -> None:
    files = {_relative(path) for path in _hosting_files()}
    assert {
        "hosting/app.py",
        "hosting/rbac.py",
        "hosting/shell.py",
        "hosting/auth/token.py",
        SERVE_COMMAND,
    } <= files
    for name in SERVE_EXTRA_LAZILY.values():
        assert (PACKAGE_DIR / name).is_file(), name


@pytest.mark.parametrize("path", _hosting_files(), ids=_relative)
def test_the_standalone_host_never_imports_an_orchestrator(path: Path) -> None:
    """Neutral like the core: no orchestrator SDK, adapter package, fake adapter or conformance suite."""
    bad = [
        f"line {item.line}: {item.module}" for item in imports_of(path) if forbidden_for_the_core(item.module)
    ]
    assert bad == [], f"{_relative(path)} imports orchestrator code"


def test_only_the_serve_command_imports_the_standalone_host() -> None:
    """``hosting`` builds on the API; the API, the core, the adapters and the task runners never import it."""
    hosting = f"{PACKAGE}.{HOSTING_DIR}"
    importers = [
        f"{_relative(path)}:{item.line}: {item.module}"
        for path in _package_files()
        if not _relative(path).startswith(f"{HOSTING_DIR}/") and _relative(path) != SERVE_COMMAND
        for item in imports_of(path)
        if _within(item.module, hosting)
    ]
    assert importers == []
    assert any(_within(item.module, hosting) for item in imports_of(PACKAGE_DIR / SERVE_COMMAND))


@pytest.mark.parametrize(
    "path",
    [
        *_core_files(),
        *(PACKAGE_DIR / name for name in (*BRIDGE_MODULES, *NOT_CORE)),
        *(path for path in _hosting_files() if _relative(path) != SERVE_COMMAND),
    ],
    ids=_relative,
)
def test_the_core_and_the_standalone_host_never_import_the_cli(path: Path) -> None:
    bad = [
        f"line {item.line}: {item.module}"
        for item in imports_of(path)
        if _within(item.module, f"{PACKAGE}.cli")
    ]
    assert bad == [], f"{_relative(path)} imports the CLI"


def test_the_auth_backends_never_import_the_host_modules() -> None:
    """The host builds the authorizer and hands it to the backend (dependency injection only)."""
    bad = [
        f"{_relative(path)}:{item.line}: {item.module}"
        for path in sorted((PACKAGE_DIR / HOSTING_DIR / "auth").rglob("*.py"))
        for item in imports_of(path)
        if any(_within(item.module, module) for module in HOST_MODULES)
    ]
    assert bad == []


def test_the_serve_extra_is_imported_only_lazily_by_its_users() -> None:
    importers = {
        f"{_relative(path)}:{item.line}": (item.module.split(".")[0], item.lazy)
        for path in _package_files()
        for item in imports_of(path)
        if item.module.split(".")[0] in SERVE_EXTRA_LAZILY
    }
    misplaced = {
        where: package
        for where, (package, _) in importers.items()
        if where.split(":")[0] != SERVE_EXTRA_LAZILY[package]
    }
    assert misplaced == {}
    assert all(lazy for _, lazy in importers.values()), importers
    assert {package for package, _ in importers.values()} == set(SERVE_EXTRA_LAZILY)  # the guard sees them


# =========================================================================== the test sandbox


def test_the_sandbox_runs_the_engine_cli_with_the_standard_library_only() -> None:
    """No container SDK, no orchestrator, no CLI and no runner: ``subprocess`` starts the engine CLI."""
    found = imports_of(PACKAGE_DIR / SANDBOX_MODULE)
    bad = [
        f"line {item.line}: {item.module}"
        for item in found
        if item.module.split(".")[0] not in sys.stdlib_module_names
        and item.module not in SANDBOX_MAY_IMPORT
        and not (item.type_checking and item.module in SANDBOX_MAY_IMPORT_FOR_TYPE_CHECKING)
    ]
    assert bad == []
    assert "subprocess" in {item.module for item in found}  # the guard sees the CLI's runner


def test_the_runner_builds_on_the_sandbox_and_the_configuration_imports_it_lazily() -> None:
    runner = imports_of(PACKAGE_DIR / "repo" / "pytest_runner.py")
    assert any(item.module == f"{PACKAGE}.repo.sandbox" and not item.lazy for item in runner)
    eager = [
        f"line {item.line}: {item.module}"
        for item in imports_of(PACKAGE_DIR / "config.py")
        if _within(item.module, f"{PACKAGE}.repo.sandbox") and not item.lazy and not item.type_checking
    ]
    assert eager == []


# =========================================================================== protected paths, GitHub checks


def _loaded_by(module: str) -> set[str]:
    """The modules that ``import <module>`` adds in a fresh interpreter, indirect imports included."""
    code = (
        f"import json, sys; before = set(sys.modules); import {module}; "
        "print(json.dumps(sorted(set(sys.modules) - before)))"
    )
    result = subprocess.run([sys.executable, "-c", code], capture_output=True, text=True, check=True)
    return set(json.loads(result.stdout))


def test_the_protected_path_policy_imports_the_standard_library_only() -> None:
    found = imports_of(PACKAGE_DIR / PROTECTED_PATHS_MODULE)
    bad = [
        f"line {item.line}: {item.module}"
        for item in found
        if item.module.split(".")[0] not in sys.stdlib_module_names
    ]
    assert bad == []
    assert "fnmatch" in {item.module for item in found}  # the guard sees its imports


def test_the_configuration_imports_the_protected_path_policy_lazily() -> None:
    """Inside the ``repo.allowed_paths`` validator: at module level, every configuration load
    would also load ``dag_healer.repo`` (the DAG scanner)."""
    config = imports_of(PACKAGE_DIR / "config.py")
    found = [item for item in config if _within(item.module, f"{PACKAGE}.repo")]
    assert f"{PACKAGE}.repo.protected_paths" in {item.module for item in found}
    eager = [f"line {item.line}: {item.module}" for item in found if not item.lazy and not item.type_checking]
    assert eager == []


def test_the_protected_path_policy_never_leads_back_to_the_configuration() -> None:
    """No cycle: what the policy loads, through ``dag_healer.repo`` included, is the core and the
    standard library."""
    loaded = _loaded_by(f"{PACKAGE}.repo.protected_paths")
    assert f"{PACKAGE}.repo.protected_paths" in loaded
    assert f"{PACKAGE}.config" not in loaded
    third_party = [
        name
        for name in loaded
        if name.split(".")[0] not in sys.stdlib_module_names and not _within(name, PACKAGE)
    ]
    assert third_party == []


def test_the_github_checks_never_import_the_configuration_the_agent_or_the_cli() -> None:
    found = imports_of(PACKAGE_DIR / GITHUB_CHECKS_MODULE)
    bad = [
        f"line {item.line}: {item.module}"
        for item in found
        if forbidden_for_the_core(item.module)
        or any(_within(item.module, name) for name in GITHUB_CHECKS_NEVER_IMPORT)
    ]
    assert bad == []
    assert f"{PACKAGE}.pr.github" in {item.module for item in found}  # the guard sees its imports


def test_the_github_checks_load_none_of_it_through_another_module() -> None:
    loaded = _loaded_by(f"{PACKAGE}.pr.github_checks")
    assert f"{PACKAGE}.pr.github" in loaded  # the guard sees the package's own modules
    bad = sorted(
        name
        for name in loaded
        if forbidden_for_the_core(name) or any(_within(name, other) for other in GITHUB_CHECKS_NEVER_IMPORT)
    )
    assert bad == []


# =========================================================================== Airflow client calls


def test_no_airflow_client_call_outside_the_airflow_adapter() -> None:
    """The core asks the ports (``inspector.logs``, ``remediator.remediate``...), never the client."""
    calls = [
        f"{_relative(path)}:{number}: {line.strip()}"
        for path in _package_files()
        if not _relative(path).startswith(CLIENT_CALLS_ALLOWED_IN)
        for number, line in enumerate(path.read_text(encoding="utf-8").splitlines(), start=1)
        if AIRFLOW_CLIENT_CALL.search(line)
    ]
    assert calls == []


def test_the_client_call_guard_matches_the_airflow_adapter() -> None:
    text = (PACKAGE_DIR / "airflow" / "adapter.py").read_text(encoding="utf-8")
    called = {match.group(1) for match in AIRFLOW_CLIENT_CALL.finditer(text)}
    assert {"list_failed_task_instances", "get_task_log", "get_dag", "clear_task_instances"} <= called
    assert not AIRFLOW_CLIENT_CALL.search("client.get_dag_run(dag_id, run_id)")
