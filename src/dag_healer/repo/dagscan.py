"""Static discovery of Airflow DAGs, their tasks and the dataset files they touch.

The scanner is pure AST: it never imports or executes user code, so it is safe to run
on any repository (including on hosts where Airflow is not installed).  It understands
Airflow 2 and Airflow 3 idioms:

* ``with DAG(...) as dag:``, ``dag = DAG(...)`` (+ ``dag=dag`` / ``with dag:``),
  ``@dag``/``@dag(dag_id=...)`` decorated functions;
* classic operators (``PythonOperator(task_id=..., python_callable=fn)``,
  ``BashOperator``, ``EmptyOperator``, custom operators, ``.partial().expand()``);
* TaskFlow ``@task``/``@task.<kind>``/``@task(task_id=...)``, ``.override(task_id=...)``,
  ``.expand()``, repeated calls (``fn__1``), ``TaskGroup``/``@task_group`` prefixes;
* callables imported from repo modules (``from src.etl.sales import transform``,
  ``import src.etl.sales as s`` + ``s.transform``, ``from etl import sales`` +
  ``sales.transform``), resolved against the import roots ``.``, ``<dags_dir>``,
  ``src`` and ``plugins`` (plus ``sys.path.insert``/``append`` targets found in the file).

Dataset discovery looks for ``.csv``/``.parquet``/``.json`` string literals in the DAG
file and in the repo modules it imports (one level deep).  Path expressions built with
``os.path.join``/``os.path.dirname(__file__)``/``pathlib`` chains and module constants
are evaluated symbolically and normalised to repo-relative posix paths.
"""

from __future__ import annotations

import ast
import logging
import posixpath
import re
from collections.abc import Callable, Iterable, Iterator
from dataclasses import dataclass, field, replace
from pathlib import Path
from typing import Any, NamedTuple

# TaskInfo / DatasetRef / DagInfo moved to dag_healer.core.specs unchanged; importing them here
# re-exports the same objects (dag_healer.repo.dagscan.TaskInfo is dag_healer.core.specs.TaskInfo).
from dag_healer.core.specs import DagInfo, DatasetRef, TaskInfo
from dag_healer.repo.profile import iter_repo_files

logger = logging.getLogger(__name__)

#: File extensions recognised as datasets and their locator type.
DATASET_EXTENSIONS: dict[str, str] = {".csv": "csv", ".parquet": "parquet", ".json": "json"}

_VROOT = "/__dag_healer_repo__"
_MAX_EVAL_DEPTH = 60
_MAX_REACH_DEPTH = 3
_MAX_LOOP_VALUES = 64
_MAX_SCOPE_DEPTH = 8
_COMMAND_KWARGS = frozenset({"bash_command", "command", "cmds", "arguments", "env"})
_FUNC_NODES = (ast.FunctionDef, ast.AsyncFunctionDef)
_SCOPE_BREAKERS = (ast.FunctionDef, ast.AsyncFunctionDef, ast.Lambda, ast.ClassDef)

_JOIN_FUNCS = frozenset({"os.path.join", "posixpath.join", "ntpath.join"})
_DIRNAME_FUNCS = frozenset({"os.path.dirname", "posixpath.dirname", "ntpath.dirname"})
_BASENAME_FUNCS = frozenset({"os.path.basename", "posixpath.basename", "ntpath.basename"})
_NORMALISE_FUNCS = frozenset(
    {"os.path.abspath", "os.path.realpath", "os.path.normpath", "posixpath.normpath", "ntpath.normpath"}
)
_IDENTITY_FUNCS = frozenset({"str", "os.fspath", "os.path.expanduser", "os.path.expandvars"})
_GETENV_FUNCS = frozenset({"os.getenv", "os.environ.get"})
_PATH_CTORS = frozenset(
    {
        "pathlib.Path",
        "pathlib.PurePath",
        "pathlib.PosixPath",
        "pathlib.PurePosixPath",
        "pathlib.WindowsPath",
        "pathlib.PureWindowsPath",
        "Path",
        "PurePath",
        "PosixPath",
        "PurePosixPath",
    }
)
_PATH_BUILDERS = _JOIN_FUNCS | _PATH_CTORS | _NORMALISE_FUNCS | _IDENTITY_FUNCS | _GETENV_FUNCS
_PATH_IDENTITY_METHODS = frozenset({"resolve", "absolute", "expanduser", "as_posix", "__fspath__"})
_CLIMB_METHODS = _PATH_IDENTITY_METHODS | {"joinpath", "with_suffix", "with_name", "with_stem", "format"}
_STR_METHODS = frozenset({"strip", "rstrip", "lstrip", "lower", "upper", "title", "casefold"})
_GLOB_CHARS = frozenset("*?[]")


# =========================================================================== public API


def scan_dags(repo_root: str | Path, dags_dir: str = "dags") -> list[DagInfo]:
    """Statically discover every DAG defined under ``<repo_root>/<dags_dir>``.

    Never imports or executes user code.  Files that fail to parse are logged and
    skipped.  The result is sorted by (file, line).
    """
    root = Path(repo_root).resolve()
    repo = _Repo(root, dags_dir)
    dags_path = root / repo.dags_dir if repo.dags_dir != "." else root
    if not dags_path.is_dir():
        logger.warning("DAGs folder %s does not exist", dags_path)
        return []
    found: list[tuple[str, int, str, DagInfo]] = []
    for path in iter_repo_files(root, dags_path, suffixes=(".py",)):
        rel = _rel_to(root, path)
        module = repo.load(rel)
        if module is None:
            continue
        try:
            infos = _DagFileScanner(repo, module).scan()
        except RecursionError:
            logger.warning("Skipping %s: source too deeply nested to analyse", rel)
            continue
        found.extend((info.file, lineno, info.dag_id, info) for lineno, info in infos)
    found.sort(key=lambda item: item[:3])
    return [item[3] for item in found]


# =========================================================================== helpers


class _Unresolvable(Exception):
    """Raised when an expression cannot be evaluated statically."""


class _Resolved(NamedTuple):
    file: str  # repo-relative posix path of the module file
    module: str  # dotted module name as imported
    rest: tuple[str, ...]  # attribute path inside the module


def _rel_to(root: Path, path: Path) -> str:
    try:
        return path.resolve().relative_to(root).as_posix()
    except ValueError:
        return path.as_posix()


def _clean_rel(value: str) -> str:
    cleaned = posixpath.normpath(value.replace("\\", "/").strip() or ".")
    return cleaned.lstrip("/") if cleaned != "/" else "."


def _dotted_for_rel(rel: str) -> str:
    stem = rel[:-3] if rel.endswith(".py") else rel
    parts = [p for p in stem.split("/") if p]
    if parts and parts[-1] == "__init__":
        parts = parts[:-1]
    return ".".join(parts)


def _attr_chain(expr: ast.AST) -> list[str] | None:
    parts: list[str] = []
    while isinstance(expr, ast.Attribute):
        parts.append(expr.attr)
        expr = expr.value
    if not isinstance(expr, ast.Name):
        return None
    parts.append(expr.id)
    return list(reversed(parts))


def _sub_blocks(stmt: ast.stmt) -> list[list[ast.stmt]]:
    blocks: list[list[ast.stmt]] = []
    for attr in ("body", "orelse", "finalbody"):
        value = getattr(stmt, attr, None)
        if isinstance(value, list) and value and isinstance(value[0], ast.stmt):
            blocks.append(value)
    for handler in getattr(stmt, "handlers", None) or []:
        blocks.append(handler.body)
    for case in getattr(stmt, "cases", None) or []:
        blocks.append(case.body)
    return blocks


def _iter_scope_stmts(stmts: Iterable[ast.stmt]) -> Iterator[ast.stmt]:
    """Yield statements of a scope, descending into compound statements but not defs."""
    for stmt in stmts:
        yield stmt
        if not isinstance(stmt, (*_FUNC_NODES, ast.ClassDef)):
            for block in _sub_blocks(stmt):
                yield from _iter_scope_stmts(block)


def _iter_nodes(node: ast.AST, prune: Callable[[ast.AST], bool] | None = None) -> Iterator[ast.AST]:
    """Pre-order walk that does not enter nested functions, lambdas or classes.

    Nodes for which ``prune`` returns True are skipped together with their subtree.
    """
    stack = [node]
    while stack:
        current = stack.pop()
        if prune is not None and prune(current):
            continue
        yield current
        children = [c for c in ast.iter_child_nodes(current) if not isinstance(c, _SCOPE_BREAKERS)]
        stack.extend(reversed(children))


def _keyword(call: ast.Call, name: str) -> ast.expr | None:
    return next((kw.value for kw in call.keywords if kw.arg == name), None)


def _extension(text: str) -> str | None:
    lowered = text.lower()
    return next((ext for ext in DATASET_EXTENSIONS if lowered.endswith(ext)), None)


def _looks_like_dataset(text: str) -> bool:
    value = text.strip()
    if not value or any(ch.isspace() for ch in value) or "://" in value:
        return False
    if _GLOB_CHARS & set(value):
        return False
    ext = _extension(value)
    if ext is None:
        return False
    base = value.replace("\\", "/").rsplit("/", 1)[-1]
    return len(base) > len(ext)


def _label(node: ast.AST) -> str:
    chain = _attr_chain(node)
    if chain:
        return ".".join(chain)
    text = ast.unparse(node)
    return text if len(text) <= 40 else text[:37] + "..."


def _is_class_call(call: ast.AST) -> bool:
    if not isinstance(call, ast.Call):
        return False
    chain = _attr_chain(call.func)
    return bool(chain) and chain[-1][:1].isupper() and chain[-1] not in {"Path", "PurePath", "PosixPath"}


def _stmt_assigned_names(stmt: ast.stmt) -> list[str]:
    if isinstance(stmt, ast.Assign):
        return [t.id for t in stmt.targets if isinstance(t, ast.Name)]
    if isinstance(stmt, ast.AnnAssign) and isinstance(stmt.target, ast.Name) and stmt.value is not None:
        return [stmt.target.id]
    return []


# =========================================================================== module index


class _Module:
    """Parsed source file with the symbol tables the scanner needs."""

    def __init__(self, rel: str, tree: ast.Module) -> None:
        self.rel = rel
        self.dotted = _dotted_for_rel(rel)
        self.tree = tree
        self.vpath = f"{_VROOT}/{rel}"
        self.bindings: dict[str, str] = {}
        self.relative_names: set[str] = set()
        self.import_targets: list[str] = []
        self.constants: dict[str, ast.expr] = {}
        self.functions: dict[str, ast.FunctionDef | ast.AsyncFunctionDef] = {}
        self.parents: dict[ast.AST, ast.AST] = {}
        self.sys_path_exprs: list[ast.expr] = []
        self.extra_roots: tuple[str, ...] = ()
        self._locals: dict[ast.AST, dict[str, ast.expr]] = {}
        self._index()

    # -- indexing
    def _index(self) -> None:
        for node in ast.walk(self.tree):
            for child in ast.iter_child_nodes(node):
                self.parents[child] = node
            if isinstance(node, ast.Import):
                self._add_import(node)
            elif isinstance(node, ast.ImportFrom):
                self._add_import_from(node)
            elif isinstance(node, ast.Call):
                self._maybe_sys_path(node)
        for stmt in _iter_scope_stmts(self.tree.body):
            if isinstance(stmt, _FUNC_NODES):
                self.functions.setdefault(stmt.name, stmt)
            for name in _stmt_assigned_names(stmt):
                self.constants.setdefault(name, stmt.value)  # type: ignore[union-attr]

    def _add_import(self, node: ast.Import) -> None:
        for alias in node.names:
            self.import_targets.append(alias.name)
            if alias.asname:
                self.bindings[alias.asname] = alias.name
            else:
                top = alias.name.split(".")[0]
                self.bindings[top] = top

    def _package_parts(self, level: int) -> list[str]:
        parts = self.rel.split("/")[:-1]
        drop = level - 1
        return parts[: len(parts) - drop] if drop else parts

    def _add_import_from(self, node: ast.ImportFrom) -> None:
        if node.level:
            base_parts = self._package_parts(node.level) + (node.module.split(".") if node.module else [])
        else:
            base_parts = node.module.split(".") if node.module else []
        base = ".".join(base_parts)
        if not base:
            return
        for alias in node.names:
            if alias.name == "*":
                self.import_targets.append(base)
                continue
            target = f"{base}.{alias.name}"
            self.import_targets.append(target)
            self.bindings[alias.asname or alias.name] = target
            if node.level:
                self.relative_names.add(alias.asname or alias.name)

    @property
    def is_package(self) -> bool:
        return self.rel.endswith("/__init__.py") or self.rel == "__init__.py"

    def rebase(self, dotted: str, imported_as: str) -> str:
        """Rewrite a repo-root dotted name into the import root this module was reached by.

        Relative imports are recorded relative to the repository root (``src.etl.x``);
        when the module itself was imported as ``etl`` the same symbol is ``etl.x``.
        """
        root_pkg = self.dotted if self.is_package else self.dotted.rpartition(".")[0]
        imported_pkg = imported_as if self.is_package else imported_as.rpartition(".")[0]
        if root_pkg and dotted.startswith(root_pkg + "."):
            return (imported_pkg + "." if imported_pkg else "") + dotted[len(root_pkg) + 1 :]
        return dotted

    def _maybe_sys_path(self, call: ast.Call) -> None:
        chain = _attr_chain(call.func)
        if chain in (["sys", "path", "insert"], ["sys", "path", "append"]) and call.args:
            self.sys_path_exprs.append(call.args[-1])

    # -- queries
    def qualname(self, expr: ast.AST) -> str | None:
        """Dotted name of an expression with its root resolved through imports."""
        chain = _attr_chain(expr)
        if chain is None:
            return None
        root = self.bindings.get(chain[0], chain[0])
        return ".".join([root, *chain[1:]])

    def is_import_rooted(self, expr: ast.AST) -> bool:
        """True when the expression's root name is an imported module/symbol."""
        chain = _attr_chain(expr)
        return bool(chain) and (
            chain[0] in self.bindings or chain[0] in {"os", "posixpath", "ntpath", "pathlib"}
        )

    def local_assigns(self, fn: ast.AST) -> dict[str, ast.expr]:
        """First binding of each simple local name in a function (defaults included)."""
        cached = self._locals.get(fn)
        if cached is not None:
            return cached
        scope: dict[str, ast.expr] = {}
        if isinstance(fn, (*_FUNC_NODES, ast.Lambda)):
            args = fn.args
            positional = [*args.posonlyargs, *args.args]
            for arg, default in zip(
                positional[len(positional) - len(args.defaults) :], args.defaults, strict=False
            ):
                scope.setdefault(arg.arg, default)
            for arg, default in zip(args.kwonlyargs, args.kw_defaults, strict=False):
                if default is not None:
                    scope.setdefault(arg.arg, default)
        if isinstance(fn, _FUNC_NODES):
            for stmt in _iter_scope_stmts(fn.body):
                for name in _stmt_assigned_names(stmt):
                    scope.setdefault(name, stmt.value)  # type: ignore[union-attr]
        self._locals[fn] = scope
        return scope

    def param_names(self, fn: ast.AST) -> frozenset[str]:
        """Names of every parameter of a function or lambda."""
        if not isinstance(fn, (*_FUNC_NODES, ast.Lambda)):
            return frozenset()
        args = fn.args
        names = [a.arg for a in (*args.posonlyargs, *args.args, *args.kwonlyargs)]
        names.extend(a.arg for a in (args.vararg, args.kwarg) if a is not None)
        return frozenset(names)

    def enclosing_functions(self, node: ast.AST) -> list[ast.AST]:
        """Enclosing function definitions, innermost first."""
        chain: list[ast.AST] = []
        current = self.parents.get(node)
        while current is not None:
            if isinstance(current, _FUNC_NODES):
                chain.append(current)
            current = self.parents.get(current)
        return chain

    def ancestors(self, node: ast.AST) -> list[ast.AST]:
        """All ancestors of a node, innermost first."""
        chain: list[ast.AST] = []
        current = self.parents.get(node)
        while current is not None:
            chain.append(current)
            current = self.parents.get(current)
        return chain


# =========================================================================== repository


class _Repo:
    """Lazy, cached view of the repository: modules, import resolution, data files."""

    def __init__(self, root: Path, dags_dir: str) -> None:
        self.root = root
        dags = Path(dags_dir)
        self.dags_dir = _rel_to(root, dags) if dags.is_absolute() else _clean_rel(dags_dir)
        roots = ["", "" if self.dags_dir == "." else self.dags_dir, "src", "plugins"]
        self.roots: tuple[str, ...] = tuple(dict.fromkeys(roots))
        self._modules: dict[str, _Module | None] = {}
        self._is_file: dict[str, bool] = {}
        self._resolved: dict[tuple[str, tuple[str, ...]], _Resolved | None] = {}
        self._data_files: list[str] | None = None
        self.candidates: dict[str, list[_Candidate]] = {}

    def is_file(self, rel: str) -> bool:
        cached = self._is_file.get(rel)
        if cached is None:
            cached = (self.root / rel).is_file() if rel and not rel.startswith("..") else False
            self._is_file[rel] = cached
        return cached

    def load(self, rel: str) -> _Module | None:
        """Parse a repo file once; syntax errors are logged and cached as None."""
        if rel in self._modules:
            return self._modules[rel]
        module: _Module | None = None
        path = self.root / rel
        try:
            tree = ast.parse(path.read_bytes(), filename=str(path))
            module = _Module(rel, tree)
        except (SyntaxError, ValueError) as exc:
            logger.warning("Skipping %s: cannot parse it (%s)", rel, exc)
        except (OSError, RecursionError) as exc:
            logger.warning("Skipping %s: %s", rel, exc)
        self._modules[rel] = module
        if module is not None:
            module.extra_roots = self._sys_path_roots(module)
        return module

    def _sys_path_roots(self, module: _Module) -> tuple[str, ...]:
        roots: list[str] = []
        for expr in module.sys_path_exprs:
            try:
                value = _Evaluator(self, module).eval(expr)
            except _Unresolvable:
                continue
            if isinstance(value, str) and value.startswith(_VROOT):
                rel = posixpath.relpath(posixpath.normpath(value), _VROOT)
                if not rel.startswith(".."):
                    roots.append("" if rel == "." else rel)
        return tuple(dict.fromkeys(roots))

    def _module_file(self, root: str, parts: list[str]) -> str | None:
        base = "/".join([root, *parts]) if root else "/".join(parts)
        for candidate in (f"{base}.py", f"{base}/__init__.py"):
            if self.is_file(candidate):
                return candidate
        return None

    def resolve_dotted(self, dotted: str, extra_roots: tuple[str, ...] = ()) -> _Resolved | None:
        """Map ``a.b.c`` to the repo file of its longest importable module prefix."""
        key = (dotted, extra_roots)
        if key in self._resolved:
            return self._resolved[key]
        parts = [p for p in dotted.split(".") if p]
        result: _Resolved | None = None
        roots = tuple(dict.fromkeys((*self.roots, *extra_roots)))
        for size in range(len(parts), 0, -1):
            for root in roots:
                found = self._module_file(root, parts[:size])
                if found is not None:
                    result = _Resolved(found, ".".join(parts[:size]), tuple(parts[size:]))
                    break
            if result is not None:
                break
        self._resolved[key] = result
        return result

    def data_files(self) -> list[str]:
        if self._data_files is None:
            suffixes = tuple(DATASET_EXTENSIONS)
            self._data_files = [_rel_to(self.root, p) for p in iter_repo_files(self.root, suffixes=suffixes)]
        return self._data_files

    def _prefer(self, matches: list[str]) -> str | None:
        if not matches:
            return None
        return sorted(matches, key=lambda f: ("data" not in f.split("/")[:-1], f.count("/"), f))[0]

    def find_by_name(self, name: str) -> str | None:
        """First repo data file called ``name``, preferring ``data/`` directories."""
        exact = [f for f in self.data_files() if f.rsplit("/", 1)[-1] == name]
        if not exact:
            folded = name.casefold()
            exact = [f for f in self.data_files() if f.rsplit("/", 1)[-1].casefold() == folded]
        return self._prefer(exact)

    def find_by_suffix(self, path: str) -> str | None:
        """Longest existing repo-relative suffix (2+ segments) of ``path``."""
        parts = [
            p for p in path.split("/") if p and not re.fullmatch(r"[A-Za-z]:", p) and p not in (".", "..")
        ]
        for start in range(len(parts) - 1):
            suffix = "/".join(parts[start:])
            if self.is_file(suffix):
                return suffix
        tail = "/".join(parts[-2:]) if len(parts) >= 2 else None
        if tail is None:
            return None
        return self._prefer([f for f in self.data_files() if f == tail or f.endswith("/" + tail)])

    def resolve_path(self, value: str, incomplete: bool, from_rel: str) -> tuple[str, bool]:
        """Normalise an evaluated path to (repo-relative posix path, resolved)."""
        text = value.replace("\\", "/")
        if text.startswith(_VROOT):
            rel = posixpath.relpath(posixpath.normpath(text), _VROOT)
            inside = rel != "." and not rel.startswith("..")
            return rel, inside and not incomplete
        if text.startswith("/") or re.match(r"^[A-Za-z]:/", text):
            match = None if incomplete else self.find_by_suffix(text)
            return (match, True) if match else (posixpath.normpath(text), False)
        rel = posixpath.normpath(text)
        if incomplete:
            base = rel.rsplit("/", 1)[-1]
            match = self.find_by_name(base) if "{" not in base else None
            return (match, True) if match else (rel, False)
        from_dir = posixpath.dirname(from_rel)
        for candidate in (rel, posixpath.join(from_dir, rel), posixpath.join(self.dags_dir, rel)):
            normal = posixpath.normpath(candidate)
            if self.is_file(normal):
                return normal, True
        stripped = re.sub(r"^(?:\.\./)+", "", rel)
        match = self.find_by_suffix(stripped) if "/" in stripped else self.find_by_name(stripped)
        return (match, True) if match else (rel, False)


# =========================================================================== evaluator


class _Evaluator:
    """Symbolic evaluator for string/path expressions (no user code is executed).

    In template mode, unresolvable parts of path-building expressions are rendered as
    ``{expr}`` placeholders and :attr:`incomplete` is set instead of failing.
    """

    def __init__(
        self,
        repo: _Repo,
        module: _Module,
        *,
        funcs: Iterable[ast.AST] = (),
        env: dict[str, Any] | None = None,
        template: bool = False,
        active: set[tuple[Any, ...]] | None = None,
    ) -> None:
        self.repo = repo
        self.module = module
        self.funcs = tuple(funcs)
        self.env = env or {}
        self.template = template
        self.incomplete = False
        self._active = active if active is not None else set()
        self._depth = 0

    def eval(self, node: ast.AST) -> Any:
        """Evaluate ``node`` strictly; raise :class:`_Unresolvable` when impossible."""
        self._depth += 1
        try:
            if self._depth > _MAX_EVAL_DEPTH:
                raise _Unresolvable("too deep")
            return self._eval(node)
        except _Unresolvable:
            raise
        except (TypeError, ValueError, IndexError, KeyError, AttributeError, OverflowError) as exc:
            raise _Unresolvable(str(exc)) from exc
        finally:
            self._depth -= 1

    def part(self, node: ast.AST) -> Any:
        """Evaluate a sub-expression, falling back to a placeholder in template mode."""
        try:
            return self.eval(node)
        except _Unresolvable:
            if not self.template:
                raise
            self.incomplete = True
            return "{" + _label(node) + "}"

    def _text(self, node: ast.AST) -> str:
        value = self.part(node)
        if isinstance(value, bool) or not isinstance(value, (str, int, float)):
            raise _Unresolvable("not a string")
        return str(value)

    def _strict_text(self, node: ast.AST) -> str:
        value = self.eval(node)
        if not isinstance(value, str):
            raise _Unresolvable("not a string")
        return value

    # -- dispatch
    def _eval(self, node: ast.AST) -> Any:
        if isinstance(node, ast.Constant):
            if isinstance(node.value, (str, int, float)) and not isinstance(node.value, bool):
                return node.value
            raise _Unresolvable("constant")
        if isinstance(node, ast.JoinedStr):
            return "".join(self._format_part(v) for v in node.values)
        if isinstance(node, ast.Name):
            return self._name(node.id)
        if isinstance(node, ast.Attribute):
            return self._attribute(node)
        if isinstance(node, ast.Subscript):
            return self._subscript(node)
        if isinstance(node, ast.BinOp):
            return self._binop(node)
        if isinstance(node, (ast.List, ast.Tuple)):
            if any(isinstance(e, ast.Starred) for e in node.elts):
                raise _Unresolvable("starred")
            return [self.eval(e) for e in node.elts]
        if isinstance(node, ast.Dict):
            if any(k is None for k in node.keys):
                raise _Unresolvable("dict unpacking")
            return {self.eval(k): self.eval(v) for k, v in zip(node.keys, node.values, strict=False)}  # type: ignore[arg-type]
        if isinstance(node, ast.Call):
            return self._call(node)
        raise _Unresolvable(type(node).__name__)

    def _format_part(self, node: ast.AST) -> str:
        if isinstance(node, ast.Constant):
            return str(node.value)
        if isinstance(node, ast.FormattedValue):
            value = self.part(node.value)
            if node.format_spec is not None:
                try:
                    return format(value, self._strict_text(node.format_spec))
                except (_Unresolvable, TypeError, ValueError):
                    return str(value)
            return str(value)
        raise _Unresolvable("f-string part")

    def _guarded(self, key: tuple[Any, ...], module: _Module, expr: ast.AST, funcs: Iterable[ast.AST]) -> Any:
        if key in self._active:
            raise _Unresolvable("cycle")
        self._active.add(key)
        try:
            sub = _Evaluator(self.repo, module, funcs=funcs, active=self._active)
            sub._depth = self._depth
            return sub.eval(expr)
        finally:
            self._active.discard(key)

    def _name(self, name: str) -> Any:
        if name in self.env:
            return self.env[name]
        for index, fn in enumerate(self.funcs):
            local = self.module.local_assigns(fn)
            if name in local:
                return self._guarded(("L", id(fn), name), self.module, local[name], self.funcs[index:])
            if name in self.module.param_names(fn):
                raise _Unresolvable(f"parameter {name}")
        if name in self.module.constants:
            return self._guarded(("M", self.module.rel, name), self.module, self.module.constants[name], ())
        if name == "__file__":
            return self.module.vpath
        if name in self.module.bindings:
            return self.imported_constant(self.module.bindings[name])
        raise _Unresolvable(name)

    def imported_constant(self, dotted: str, hops: int = 0) -> Any:
        """Value of a constant defined in another repo module."""
        resolved = self.repo.resolve_dotted(dotted, self.module.extra_roots)
        if resolved is None or len(resolved.rest) != 1 or hops > 3:
            raise _Unresolvable(dotted)
        target = self.repo.load(resolved.file)
        name = resolved.rest[0]
        if target is None:
            raise _Unresolvable(dotted)
        if name in target.constants:
            return self._guarded(("M", target.rel, name), target, target.constants[name], ())
        if name in target.bindings:
            sub = _Evaluator(self.repo, target, active=self._active)
            return sub.imported_constant(target.bindings[name], hops + 1)
        raise _Unresolvable(dotted)

    def _attribute(self, node: ast.Attribute) -> Any:
        if node.attr == "parent":
            return posixpath.dirname(self._strict_text(node.value))
        if node.attr in ("name", "stem", "suffix") and not self.module.is_import_rooted(node.value):
            base = posixpath.basename(self._strict_text(node.value))
            stem, suffix = posixpath.splitext(base)
            return {"name": base, "stem": stem, "suffix": suffix}[node.attr]
        if self.module.is_import_rooted(node):
            qualified = self.module.qualname(node)
            if qualified:
                return self.imported_constant(qualified)
        raise _Unresolvable("attribute")

    def _subscript(self, node: ast.Subscript) -> Any:
        target = node.value
        index_node = node.slice
        if isinstance(target, ast.Attribute) and target.attr == "parents":
            index = self.eval(index_node)
            if not isinstance(index, int) or index < 0:
                raise _Unresolvable("parents index")
            path = self._strict_text(target.value)
            for _ in range(index + 1):
                path = posixpath.dirname(path)
            return path
        container = self.eval(target)
        index = self.eval(index_node)
        if isinstance(container, (list, str, dict)):
            return container[index]
        raise _Unresolvable("subscript")

    def _binop(self, node: ast.BinOp) -> Any:
        left = self.part(node.left)
        right = self.part(node.right)
        if isinstance(node.op, ast.Div) and isinstance(left, str) and isinstance(right, str):
            return posixpath.join(left, right)
        if isinstance(node.op, ast.Add) and type(left) is type(right) and isinstance(left, (str, list)):
            return left + right
        if isinstance(node.op, ast.Mod) and isinstance(left, str):
            return left % (tuple(right) if isinstance(right, list) else right)
        raise _Unresolvable("binop")

    def _call(self, node: ast.Call) -> Any:
        func = node.func
        if any(isinstance(a, ast.Starred) for a in node.args) or any(k.arg is None for k in node.keywords):
            raise _Unresolvable("star args")
        if self.module.is_import_rooted(func) or isinstance(func, ast.Name):
            qualified = self.module.qualname(func)
            if qualified is not None:
                handled, value = self._known_call(qualified, node)
                if handled:
                    return value
        if isinstance(func, ast.Attribute):
            return self._method_call(func, node)
        raise _Unresolvable("call")

    def _known_call(self, qualified: str, node: ast.Call) -> tuple[bool, Any]:
        args = node.args
        if qualified in _JOIN_FUNCS:
            if not args:
                raise _Unresolvable("join()")
            return True, posixpath.join(*[self._text(a) for a in args])
        if qualified in _PATH_CTORS:
            return True, posixpath.join(*[self._text(a) for a in args]) if args else "."
        if qualified in _DIRNAME_FUNCS and args:
            return True, posixpath.dirname(self._strict_text(args[0]))
        if qualified in _BASENAME_FUNCS and args:
            return True, posixpath.basename(self._strict_text(args[0]))
        if qualified in _NORMALISE_FUNCS and args:
            return True, posixpath.normpath(self._text(args[0]))
        if qualified in _IDENTITY_FUNCS and args:
            return True, self._text(args[0])
        if qualified in _GETENV_FUNCS:
            default = args[1] if len(args) > 1 else _keyword(node, "default")
            if default is None:
                raise _Unresolvable("environment variable without default")
            return True, self.eval(default)
        return False, None

    def _method_call(self, func: ast.Attribute, node: ast.Call) -> Any:
        method = func.attr
        if method in _PATH_IDENTITY_METHODS and not node.args:
            return self._strict_text(func.value)
        if method == "joinpath":
            return posixpath.join(self._strict_text(func.value), *[self._text(a) for a in node.args])
        if method == "with_suffix" and node.args:
            root, _ = posixpath.splitext(self._strict_text(func.value))
            return root + self._strict_text(node.args[0])
        if method == "with_name" and node.args:
            return posixpath.join(
                posixpath.dirname(self._strict_text(func.value)), self._strict_text(node.args[0])
            )
        if method == "with_stem" and node.args:
            base = self._strict_text(func.value)
            _, suffix = posixpath.splitext(base)
            return posixpath.join(posixpath.dirname(base), self._strict_text(node.args[0]) + suffix)
        if method == "format":
            template = self._strict_text(func.value)
            positional = [self.part(a) for a in node.args]
            named = {k.arg: self.part(k.value) for k in node.keywords if k.arg}
            return template.format(*positional, **named)
        if method == "replace" and len(node.args) == 2:
            base = self._strict_text(func.value)
            return base.replace(self._strict_text(node.args[0]), self._strict_text(node.args[1]))
        if method in _STR_METHODS and len(node.args) <= 1:
            base = self._strict_text(func.value)
            extra = [self._strict_text(a) for a in node.args]
            return getattr(base, method)(*extra)
        if method == "get" and node.args:
            container = self.eval(func.value)
            if isinstance(container, dict):
                key = self.eval(node.args[0])
                if key in container:
                    return container[key]
                if len(node.args) > 1:
                    return self.eval(node.args[1])
        raise _Unresolvable(f"method {method}")


# =========================================================================== candidates


@dataclass
class _Candidate:
    module: _Module
    lineno: int
    path: str
    type: str
    resolved: bool
    func_ids: frozenset[int]
    ancestors: tuple[ast.AST, ...]
    const_keys: tuple[tuple[str, int, str], ...]

    @property
    def source(self) -> str:
        return f"{self.module.rel}:{self.lineno}"


def _is_path_builder(module: _Module, func: ast.AST) -> bool:
    qualified = module.qualname(func)
    return qualified in _PATH_BUILDERS


def _climb(module: _Module, node: ast.AST) -> ast.AST:
    """Outermost path-building expression containing ``node``."""
    expr = node
    while True:
        parent = module.parents.get(expr)
        if isinstance(parent, ast.BinOp) and isinstance(parent.op, (ast.Div, ast.Add, ast.Mod)):
            expr = parent
            continue
        if isinstance(parent, ast.Call) and any(a is expr for a in parent.args):
            func = parent.func
            if _is_path_builder(module, func) or (
                isinstance(func, ast.Attribute) and func.attr == "joinpath"
            ):
                expr = parent
                continue
        if isinstance(parent, ast.Attribute) and parent.value is expr and parent.attr in _CLIMB_METHODS:
            grand = module.parents.get(parent)
            if isinstance(grand, ast.Call) and grand.func is parent:
                expr = grand
                continue
        return expr


def _const_keys(module: _Module, node: ast.AST) -> tuple[tuple[str, int, str], ...]:
    current = module.parents.get(node)
    while current is not None and not isinstance(current, ast.stmt):
        current = module.parents.get(current)
    if current is None:
        return ()
    names = _stmt_assigned_names(current)
    value = getattr(current, "value", None)
    if not names or _is_class_call(value):
        return ()
    scope = module.enclosing_functions(current)
    scope_id = id(scope[0]) if scope else 0
    return tuple((module.rel, scope_id, name) for name in names)


def _literal_template(node: ast.AST) -> str:
    if isinstance(node, ast.Constant):
        return str(node.value)
    if isinstance(node, ast.JoinedStr):
        pieces = []
        for value in node.values:
            if isinstance(value, ast.Constant):
                pieces.append(str(value.value))
            elif isinstance(value, ast.FormattedValue):
                pieces.append("{" + _label(value.value) + "}")
        return "".join(pieces)
    return ast.unparse(node)


def _try_eval(repo: _Repo, module: _Module, expr: ast.AST, funcs: list[ast.AST]) -> tuple[str, bool] | None:
    evaluator = _Evaluator(repo, module, funcs=funcs, template=True)
    try:
        value = evaluator.eval(expr)
    except _Unresolvable:
        return None
    if not isinstance(value, str) or _extension(value) is None:
        return None
    return value, evaluator.incomplete


def _evaluate_candidate(repo: _Repo, module: _Module, node: ast.AST) -> tuple[str, bool]:
    """Evaluate the path expression around a literal: (value, has placeholders)."""
    funcs = module.enclosing_functions(node)
    outer = _climb(module, node)
    result = _try_eval(repo, module, outer, funcs)
    if result is None and outer is not node:
        result = _try_eval(repo, module, node, funcs)
    if result is None:
        return _literal_template(node), not isinstance(node, ast.Constant)
    return result


def _make_candidate(
    repo: _Repo, module: _Module, node: ast.AST, literal: str | None = None
) -> _Candidate | None:
    if literal is not None:
        value, incomplete = literal, False
    else:
        value, incomplete = _evaluate_candidate(repo, module, node)
    path, resolved = repo.resolve_path(value, incomplete, module.rel)
    ext = _extension(path) or _extension(value)
    if ext is None:
        return None
    ancestors = tuple(module.ancestors(node))
    return _Candidate(
        module=module,
        lineno=getattr(node, "lineno", 0),
        path=path,
        type=DATASET_EXTENSIONS[ext],
        resolved=resolved,
        func_ids=frozenset(id(f) for f in module.enclosing_functions(node)),
        ancestors=ancestors,
        const_keys=_const_keys(module, node),
    )


def _command_tokens(text: str) -> list[str]:
    tokens = []
    for raw in text.split():
        token = raw.strip("'\"`;,()[]{}")
        if "=" in token:
            token = token.split("=", 1)[1]
        if _looks_like_dataset(token):
            tokens.append(token)
    return tokens


def _module_candidates(repo: _Repo, module: _Module) -> list[_Candidate]:
    """All dataset-looking literals of a module (cached per module)."""
    cached = repo.candidates.get(module.rel)
    if cached is not None:
        return cached
    found: list[_Candidate] = []
    for node in ast.walk(module.tree):
        parent = module.parents.get(node)
        candidate: _Candidate | None = None
        if isinstance(node, ast.Constant) and isinstance(node.value, str):
            if isinstance(parent, (ast.JoinedStr, ast.Expr)):
                continue
            if _looks_like_dataset(node.value):
                candidate = _make_candidate(repo, module, node)
            elif isinstance(parent, ast.keyword) and parent.arg in _COMMAND_KWARGS:
                for token in _command_tokens(node.value):
                    extra = _make_candidate(repo, module, node, literal=token)
                    if extra is not None:
                        found.append(extra)
        elif isinstance(node, ast.JoinedStr) and not isinstance(parent, (ast.JoinedStr, ast.FormattedValue)):
            last = node.values[-1] if node.values else None
            if isinstance(last, ast.Constant) and isinstance(last.value, str):
                tail = last.value
                if (
                    _extension(tail)
                    and not any(ch.isspace() for ch in tail)
                    and not (_GLOB_CHARS & set(tail))
                ):
                    candidate = _make_candidate(repo, module, node)
        if candidate is not None:
            found.append(candidate)
    found.sort(key=lambda c: (c.lineno, c.path))
    repo.candidates[module.rel] = found
    return found


# =========================================================================== DAG scanning


@dataclass(eq=False)
class _TaskRecord:
    info: TaskInfo
    body: tuple[_Module, ast.AST] | None
    calls: list[ast.Call]


@dataclass(eq=False)
class _DagBuilder:
    dag_id: str
    lineno: int
    regions: list[ast.AST]
    tasks: list[_TaskRecord] = field(default_factory=list)
    task_ids: set[str] = field(default_factory=set)
    group_counts: dict[str, int] = field(default_factory=dict)

    def unique_id(self, base: str) -> str:
        if base not in self.task_ids:
            return base
        core = re.split(r"__\d+$", base)[0]
        index = 1
        while f"{core}__{index}" in self.task_ids:
            index += 1
        return f"{core}__{index}"

    def add(self, record: _TaskRecord) -> None:
        if record.info.task_id in self.task_ids:
            return
        self.task_ids.add(record.info.task_id)
        self.tasks.append(record)


@dataclass(eq=False)
class _TaskDef:
    task_id: str
    operator: str
    node: ast.FunctionDef | ast.AsyncFunctionDef
    module: _Module
    owner: _DagBuilder | None = None
    owner_prefix: str = ""
    called_by: set[int] = field(default_factory=set)


@dataclass(eq=False)
class _GroupDef:
    group_id: str
    prefix: bool
    node: ast.FunctionDef | ast.AsyncFunctionDef


@dataclass
class _Ctx:
    dag: _DagBuilder | None
    prefix: str
    env: dict[str, Any]
    tasks: dict[str, _TaskDef]
    groups: dict[str, _GroupDef]
    funcs: dict[str, ast.AST]
    chain: tuple[ast.AST, ...]
    depth: int = 0
    active: tuple[ast.AST, ...] = ()


class _Deco(NamedTuple):
    kind: str  # "dag" | "task" | "group"
    operator: str
    call: ast.Call | None


@dataclass
class _Reach:
    func_ids: set[int] = field(default_factory=set)
    call_ids: set[int] = field(default_factory=set)
    const_keys: set[tuple[str, int, str]] = field(default_factory=set)
    modules: dict[str, _Module] = field(default_factory=dict)


class _DagFileScanner:
    """Finds the DAGs of one file and assembles their :class:`DagInfo`."""

    def __init__(self, repo: _Repo, module: _Module) -> None:
        self.repo = repo
        self.module = module
        self.dags: list[_DagBuilder] = []
        self.dag_vars: dict[str, _DagBuilder] = {}
        self.task_defs: list[_TaskDef] = []
        self.call_owners: dict[int, list[tuple[_DagBuilder, _TaskRecord]]] = {}
        self._imported_defs: dict[str, _TaskDef | None] = {}

    # ------------------------------------------------------------------ entry
    def scan(self) -> list[tuple[int, DagInfo]]:
        root = _Ctx(dag=None, prefix="", env={}, tasks={}, groups={}, funcs={}, chain=())
        self._collect_defs(self.module.tree.body, root)
        self._walk_block(self.module.tree.body, root)
        self._add_uncalled_task_defs()
        if not self.dags:
            return []
        return _Assembler(self).build()

    # ------------------------------------------------------------------ symbols
    def _airflow_parts(self, expr: ast.AST) -> list[str] | None:
        chain = _attr_chain(expr)
        if not chain:
            return None
        if chain[0] in self.module.bindings:
            parts = self.module.bindings[chain[0]].split(".") + chain[1:]
            return parts if parts[0] == "airflow" else None
        return chain

    def _is_dag_call(self, expr: ast.AST) -> bool:
        if not isinstance(expr, ast.Call):
            return False
        parts = self._airflow_parts(expr.func)
        return bool(parts) and parts[-1] == "DAG"

    def _is_task_group_call(self, expr: ast.AST) -> bool:
        if not isinstance(expr, ast.Call):
            return False
        parts = self._airflow_parts(expr.func)
        return bool(parts) and parts[-1] == "TaskGroup"

    def _decorator(self, fn: ast.FunctionDef | ast.AsyncFunctionDef) -> _Deco | None:
        for deco in fn.decorator_list:
            call = deco if isinstance(deco, ast.Call) else None
            parts = self._airflow_parts(call.func if call else deco)
            if not parts:
                continue
            if parts[-1] == "dag":
                return _Deco("dag", "", call)
            if parts[-1] == "task_group":
                return _Deco("group", "", call)
            if "task" in parts:
                index = parts.index("task")
                return _Deco("task", "@" + ".".join(parts[index:]), call)
        return None

    def _eval_str(self, expr: ast.AST | None, ctx: _Ctx) -> str | None:
        if expr is None:
            return None
        try:
            value = _Evaluator(self.repo, self.module, funcs=ctx.chain, env=ctx.env).eval(expr)
        except _Unresolvable:
            return None
        return value if isinstance(value, str) and value else None

    def _id_arg(self, call: ast.Call | None, keyword: str, ctx: _Ctx) -> str | None:
        if call is None:
            return None
        expr = _keyword(call, keyword)
        if expr is None and call.args:
            expr = call.args[0]
        return self._eval_str(expr, ctx)

    # ------------------------------------------------------------------ scopes
    def _make_task_def(self, fn: ast.FunctionDef | ast.AsyncFunctionDef, deco: _Deco, ctx: _Ctx) -> _TaskDef:
        task_id = self._eval_str(_keyword(deco.call, "task_id"), ctx) if deco.call else None
        task_def = _TaskDef(task_id=task_id or fn.name, operator=deco.operator, node=fn, module=self.module)
        self.task_defs.append(task_def)
        return task_def

    def _collect_defs(self, stmts: list[ast.stmt], ctx: _Ctx) -> None:
        for stmt in _iter_scope_stmts(stmts):
            if not isinstance(stmt, _FUNC_NODES):
                continue
            ctx.funcs[stmt.name] = stmt
            deco = self._decorator(stmt)
            if deco is None:
                continue
            if deco.kind == "task":
                ctx.tasks[stmt.name] = self._make_task_def(stmt, deco, ctx)
            elif deco.kind == "group":
                group_id = self._eval_str(_keyword(deco.call, "group_id"), ctx) if deco.call else None
                prefix_expr = _keyword(deco.call, "prefix_group_id") if deco.call else None
                prefix = not (isinstance(prefix_expr, ast.Constant) and prefix_expr.value is False)
                ctx.groups[stmt.name] = _GroupDef(group_id or stmt.name, prefix, stmt)

    def _child_ctx(self, fn: ast.AST, ctx: _Ctx, dag: _DagBuilder | None, prefix: str) -> _Ctx:
        child = _Ctx(
            dag=dag,
            prefix=prefix,
            env=dict(ctx.env),
            tasks=dict(ctx.tasks),
            groups=dict(ctx.groups),
            funcs=dict(ctx.funcs),
            chain=(fn, *ctx.chain),
            depth=ctx.depth + 1,
            active=(*ctx.active, fn),
        )
        self._collect_defs(fn.body, child)  # type: ignore[attr-defined]
        return child

    def _new_dag(self, dag_id: str | None, node: ast.AST, region: ast.AST) -> _DagBuilder | None:
        if not dag_id:
            logger.debug(
                "%s:%s: DAG id is not statically resolvable", self.module.rel, getattr(node, "lineno", 0)
            )
            return None
        dag = _DagBuilder(dag_id=dag_id, lineno=getattr(node, "lineno", 0), regions=[region])
        self.dags.append(dag)
        return dag

    # ------------------------------------------------------------------ walking
    def _walk_block(self, stmts: list[ast.stmt], ctx: _Ctx) -> None:
        for stmt in stmts:
            self._walk_stmt(stmt, ctx)

    def _walk_stmt(self, stmt: ast.stmt, ctx: _Ctx) -> None:
        if isinstance(stmt, _FUNC_NODES):
            self._on_function(stmt, ctx)
        elif isinstance(stmt, ast.ClassDef):
            return
        elif isinstance(stmt, (ast.With, ast.AsyncWith)):
            self._on_with(stmt, ctx)
        elif isinstance(stmt, (ast.For, ast.AsyncFor)):
            self._on_for(stmt, ctx)
        elif isinstance(stmt, (ast.Assign, ast.AnnAssign)) and self._is_dag_call(stmt.value):
            dag = self._new_dag(self._id_arg(stmt.value, "dag_id", ctx), stmt, stmt)  # type: ignore[arg-type]
            if dag is not None:
                for name in _stmt_assigned_names(stmt):
                    self.dag_vars[name] = dag
        elif _sub_blocks(stmt):
            for attr in ("test", "subject"):
                if isinstance(getattr(stmt, attr, None), ast.AST):
                    self._scan_calls(getattr(stmt, attr), ctx)
            for block in _sub_blocks(stmt):
                self._walk_block(block, ctx)
        else:
            self._scan_calls(stmt, ctx)

    def _on_function(self, fn: ast.FunctionDef | ast.AsyncFunctionDef, ctx: _Ctx) -> None:
        deco = self._decorator(fn)
        if deco is not None and deco.kind == "dag":
            dag = self._new_dag(self._id_arg(deco.call, "dag_id", ctx) or fn.name, fn, fn)
            if dag is not None:
                self._walk_block(fn.body, self._child_ctx(fn, ctx, dag, ""))
            return
        if deco is not None and deco.kind == "task":
            task_def = ctx.tasks.get(fn.name)
            if (
                task_def is not None
                and task_def.node is fn
                and ctx.dag is not None
                and task_def.owner is None
            ):
                task_def.owner = ctx.dag
                task_def.owner_prefix = ctx.prefix
            return
        if deco is None and ctx.depth < 3:
            self._walk_block(fn.body, self._child_ctx(fn, ctx, None, ""))

    def _on_with(self, stmt: ast.With | ast.AsyncWith, ctx: _Ctx) -> None:
        current = ctx
        for item in stmt.items:
            expr = item.context_expr
            target = item.optional_vars.id if isinstance(item.optional_vars, ast.Name) else None
            if self._is_dag_call(expr):
                dag = self._new_dag(self._id_arg(expr, "dag_id", current), expr, stmt)  # type: ignore[arg-type]
                if dag is not None:
                    if target:
                        self.dag_vars[target] = dag
                    current = replace(current, dag=dag, prefix="")
            elif isinstance(expr, ast.Name) and expr.id in self.dag_vars:
                dag = self.dag_vars[expr.id]
                dag.regions.append(stmt)
                current = replace(current, dag=dag, prefix="")
            elif self._is_task_group_call(expr):
                current = self._enter_task_group(expr, current)  # type: ignore[arg-type]
            else:
                self._scan_calls(expr, current)
        self._walk_block(stmt.body, current)

    def _enter_task_group(self, call: ast.Call, ctx: _Ctx) -> _Ctx:
        dag = ctx.dag
        dag_expr = _keyword(call, "dag")
        if isinstance(dag_expr, ast.Name) and dag_expr.id in self.dag_vars:
            dag = self.dag_vars[dag_expr.id]
        group_id = self._id_arg(call, "group_id", ctx)
        if dag is None or group_id is None:
            return ctx
        prefix_expr = _keyword(call, "prefix_group_id")
        disabled = isinstance(prefix_expr, ast.Constant) and prefix_expr.value is False
        base_prefix = ctx.prefix if dag is ctx.dag else ""
        prefix = base_prefix if disabled else f"{base_prefix}{group_id}."
        return replace(ctx, dag=dag, prefix=prefix)

    def _loop_values(self, stmt: ast.For | ast.AsyncFor, ctx: _Ctx) -> list[dict[str, Any]] | None:
        try:
            values = _Evaluator(self.repo, self.module, funcs=ctx.chain, env=ctx.env).eval(stmt.iter)
        except _Unresolvable:
            return None
        if isinstance(values, dict):
            values = list(values)
        if not isinstance(values, list) or not values or len(values) > _MAX_LOOP_VALUES:
            return None
        target = stmt.target
        bindings: list[dict[str, Any]] = []
        for value in values:
            if isinstance(target, ast.Name):
                bindings.append({target.id: value})
            elif isinstance(target, (ast.Tuple, ast.List)) and all(
                isinstance(e, ast.Name) for e in target.elts
            ):
                if not isinstance(value, list) or len(value) != len(target.elts):
                    return None
                bindings.append({e.id: v for e, v in zip(target.elts, value, strict=False)})  # type: ignore[attr-defined]
            else:
                return None
        return bindings

    def _on_for(self, stmt: ast.For | ast.AsyncFor, ctx: _Ctx) -> None:
        bindings = self._loop_values(stmt, ctx)
        if bindings is None:
            self._scan_calls(stmt.iter, ctx)
            self._walk_block(stmt.body, ctx)
        else:
            for binding in bindings:
                self._walk_block(stmt.body, replace(ctx, env={**ctx.env, **binding}))
        self._walk_block(stmt.orelse, ctx)

    # ------------------------------------------------------------------ calls
    def _scan_calls(self, node: ast.AST, ctx: _Ctx) -> None:
        for sub in _iter_nodes(node):
            if not isinstance(sub, ast.Call):
                continue
            if self._is_dag_call(sub) or self._is_task_group_call(sub):
                continue
            if _keyword(sub, "task_id") is not None and self._operator_name(sub) is not None:
                self._add_operator_task(sub, ctx)
                continue
            target = self._flow_target(sub)
            if target is None:
                continue
            name, override = target
            if name in ctx.groups:
                self._enter_group_function(ctx.groups[name], ctx)
                continue
            task_def = ctx.tasks.get(name) or self._imported_task_def(name)
            if task_def is not None:
                self._add_flow_task(task_def, override, sub, ctx)

    def _operator_name(self, call: ast.Call) -> str | None:
        func = call.func
        if isinstance(func, ast.Attribute) and func.attr == "partial":
            func = func.value
        chain = _attr_chain(func)
        if not chain or not chain[-1][:1].isupper():
            return None
        return chain[-1]

    def _flow_target(self, call: ast.Call) -> tuple[str, ast.expr | None] | None:
        func = call.func
        base: ast.AST = (
            func.value
            if isinstance(func, ast.Attribute) and func.attr in ("expand", "expand_kwargs")
            else func
        )
        override: ast.expr | None = None
        while True:
            if isinstance(base, ast.Name):
                return base.id, override
            if isinstance(base, ast.Call) and isinstance(base.func, ast.Attribute):
                if base.func.attr == "override" and override is None:
                    override = _keyword(base, "task_id")
                if base.func.attr in ("override", "partial"):
                    base = base.func.value
                    continue
            return None

    def _chained_calls(self, call: ast.Call) -> list[ast.Call]:
        calls = [call]
        current: ast.AST = call
        while True:
            parent = self.module.parents.get(current)
            grand = self.module.parents.get(parent) if parent is not None else None
            if isinstance(parent, ast.Attribute) and isinstance(grand, ast.Call) and grand.func is parent:
                calls.append(grand)
                current = grand
                continue
            return calls

    def _register(self, dag: _DagBuilder, record: _TaskRecord) -> None:
        if record.info.task_id in dag.task_ids:
            return
        dag.add(record)
        for call in record.calls:
            self.call_owners.setdefault(id(call), []).append((dag, record))

    def _add_operator_task(self, call: ast.Call, ctx: _Ctx) -> None:
        task_id = self._eval_str(_keyword(call, "task_id"), ctx)
        dag = ctx.dag
        prefix = ctx.prefix
        dag_expr = _keyword(call, "dag")
        if isinstance(dag_expr, ast.Name) and dag_expr.id in self.dag_vars:
            if self.dag_vars[dag_expr.id] is not dag:
                prefix = ""
            dag = self.dag_vars[dag_expr.id]
        if task_id is None or dag is None:
            logger.debug("%s:%s: operator task not attributable to a DAG", self.module.rel, call.lineno)
            return
        operator = self._operator_name(call) or "Operator"
        target = _CallableResolver(self.repo, self.module, ctx.funcs).resolve(
            _keyword(call, "python_callable")
        )
        info = TaskInfo(
            task_id=prefix + task_id,
            operator=operator,
            callable_name=target.name,
            callable_module=target.module,
            callable_file=target.file,
            lineno=call.lineno,
        )
        body = (target.source, target.node) if target.node is not None and target.source is not None else None
        self._register(dag, _TaskRecord(info, body, self._chained_calls(call)))

    def _add_flow_task(
        self, task_def: _TaskDef, override: ast.expr | None, call: ast.Call, ctx: _Ctx
    ) -> None:
        dag = ctx.dag
        if dag is None:
            return
        base = (self._eval_str(override, ctx) if override is not None else None) or task_def.task_id
        task_id = dag.unique_id(ctx.prefix + base)
        task_def.called_by.add(id(dag))
        info = TaskInfo(
            task_id=task_id,
            operator=task_def.operator,
            callable_name=task_def.node.name,
            callable_module=task_def.module.dotted,
            callable_file=task_def.module.rel,
            lineno=task_def.node.lineno,
        )
        self._register(dag, _TaskRecord(info, (task_def.module, task_def.node), self._chained_calls(call)))

    def _enter_group_function(self, group: _GroupDef, ctx: _Ctx) -> None:
        dag = ctx.dag
        if dag is None or group.node in ctx.active or ctx.depth >= _MAX_SCOPE_DEPTH:
            return
        count = dag.group_counts.get(ctx.prefix + group.group_id, 0)
        dag.group_counts[ctx.prefix + group.group_id] = count + 1
        group_id = group.group_id if count == 0 else f"{group.group_id}__{count}"
        prefix = ctx.prefix + group_id + "." if group.prefix else ctx.prefix
        self._walk_block(group.node.body, self._child_ctx(group.node, ctx, dag, prefix))

    def _imported_task_def(self, name: str) -> _TaskDef | None:
        if name in self._imported_defs:
            return self._imported_defs[name]
        result: _TaskDef | None = None
        dotted = self.module.bindings.get(name)
        resolved = self.repo.resolve_dotted(dotted, self.module.extra_roots) if dotted else None
        target = self.repo.load(resolved.file) if resolved and len(resolved.rest) == 1 else None
        if target is not None and resolved is not None:
            fn = target.functions.get(resolved.rest[0])
            if fn is not None:
                deco = _DagFileScanner(self.repo, target)._decorator(fn)
                if deco is not None and deco.kind == "task":
                    task_id = None
                    if deco.call is not None:
                        expr = _keyword(deco.call, "task_id")
                        if expr is not None:
                            try:
                                task_id = _Evaluator(self.repo, target).eval(expr)
                            except _Unresolvable:
                                task_id = None
                    result = _TaskDef(
                        task_id=task_id if isinstance(task_id, str) and task_id else fn.name,
                        operator=deco.operator,
                        node=fn,
                        module=target,
                    )
        self._imported_defs[name] = result
        return result

    def _add_uncalled_task_defs(self) -> None:
        for task_def in self.task_defs:
            dag = task_def.owner
            if dag is None or id(dag) in task_def.called_by:
                continue
            info = TaskInfo(
                task_id=dag.unique_id(task_def.owner_prefix + task_def.task_id),
                operator=task_def.operator,
                callable_name=task_def.node.name,
                callable_module=task_def.module.dotted,
                callable_file=task_def.module.rel,
                lineno=task_def.node.lineno,
            )
            self._register(dag, _TaskRecord(info, (task_def.module, task_def.node), []))


# =========================================================================== callables


@dataclass
class _CallableTarget:
    name: str | None = None
    module: str | None = None
    file: str | None = None
    node: ast.AST | None = None
    source: _Module | None = None


class _CallableResolver:
    """Resolves a ``python_callable=`` expression to its function definition."""

    def __init__(self, repo: _Repo, module: _Module, local_funcs: dict[str, ast.AST]) -> None:
        self.repo = repo
        self.module = module
        self.local_funcs = local_funcs

    def resolve(self, expr: ast.AST | None) -> _CallableTarget:
        if expr is None:
            return _CallableTarget()
        if isinstance(expr, ast.Lambda):
            return self.resolve(expr.body.func) if isinstance(expr.body, ast.Call) else _CallableTarget()
        if isinstance(expr, ast.Call):
            qualified = self.module.qualname(expr.func)
            if qualified in ("functools.partial", "partial") and expr.args:
                return self.resolve(expr.args[0])
            return _CallableTarget()
        if isinstance(expr, ast.Name):
            return self._resolve_name(expr.id)
        chain = _attr_chain(expr)
        if chain and self.module.is_import_rooted(expr):
            return self.resolve_dotted(self.module.qualname(expr) or "")
        return _CallableTarget(name=chain[-1] if chain else None)

    def _resolve_name(self, name: str) -> _CallableTarget:
        node = self.local_funcs.get(name) or self.module.functions.get(name)
        if node is not None:
            return _CallableTarget(name, self.module.dotted, self.module.rel, node, self.module)
        if name in self.module.bindings:
            return self.resolve_dotted(self.module.bindings[name])
        return _CallableTarget(name=name)

    def resolve_dotted(self, dotted: str, hops: int = 0) -> _CallableTarget:
        parts = dotted.split(".")
        fallback = _CallableTarget(name=parts[-1], module=".".join(parts[:-1]) or None)
        resolved = self.repo.resolve_dotted(dotted, self.module.extra_roots)
        if resolved is None or not resolved.rest:
            return fallback
        target = self.repo.load(resolved.file)
        name = ".".join(resolved.rest)
        result = _CallableTarget(name, resolved.module, resolved.file)
        if target is None:
            return result
        if len(resolved.rest) == 1 and resolved.rest[0] in target.functions:
            result.node = target.functions[resolved.rest[0]]
            result.source = target
        elif len(resolved.rest) == 1 and resolved.rest[0] in target.bindings and hops < 3:
            symbol = resolved.rest[0]
            reexport = target.bindings[symbol]
            if symbol in target.relative_names:
                reexport = target.rebase(reexport, resolved.module)
            sub = _CallableResolver(self.repo, target, {}).resolve_dotted(reexport, hops + 1)
            if sub.file is not None:
                return sub
        return result


# =========================================================================== assembly


class _Assembler:
    """Turns scanner results into :class:`DagInfo` objects with attributed datasets."""

    def __init__(self, scanner: _DagFileScanner) -> None:
        self.scanner = scanner
        self.repo = scanner.repo
        self.module = scanner.module
        self.dag_var_names = set(scanner.dag_vars)

    def build(self) -> list[tuple[int, DagInfo]]:
        imports = self._imports()
        reaches = {id(rec): self._reach(rec) for dag in self.scanner.dags for rec in dag.tasks}
        level_one = [self.module] + [m for m in (self.repo.load(rel) for _, rel in imports) if m is not None]
        level_one_rels = {m.rel for m in level_one}
        deeper: dict[str, _Module] = {}
        for reach in reaches.values():
            for rel, module in reach.modules.items():
                if rel not in level_one_rels:
                    deeper[rel] = module
        refs: dict[int, dict[tuple[str | None, str], DatasetRef]] = {id(d): {} for d in self.scanner.dags}
        for module in level_one:
            self._attribute(module, reaches, refs, allow_unattributed=True)
        for rel in sorted(deeper):
            self._attribute(deeper[rel], reaches, refs, allow_unattributed=False)
        import_names = sorted({dotted for dotted, _ in imports})
        results = []
        for dag in self.scanner.dags:
            info = DagInfo(
                dag_id=dag.dag_id,
                file=self.module.rel,
                tasks=[r.info for r in sorted(dag.tasks, key=lambda r: (r.info.lineno, r.info.task_id))],
                datasets=self._finalise(refs[id(dag)]),
                imports=list(import_names),
            )
            results.append((dag.lineno, info))
        return results

    # -- imports
    def _imports(self) -> list[tuple[str, str]]:
        seen: dict[str, str] = {}
        for dotted in self.module.import_targets:
            resolved = self.repo.resolve_dotted(dotted, self.module.extra_roots)
            if resolved is None or resolved.file == self.module.rel:
                continue
            seen.setdefault(resolved.module, resolved.file)
        return sorted(seen.items())

    # -- reach
    def _reach(self, record: _TaskRecord) -> _Reach:
        reach = _Reach()
        work: list[tuple[_Module, ast.AST, int]] = []
        if record.body is not None:
            work.append((record.body[0], record.body[1], 0))
        for call in record.calls:
            reach.call_ids.add(id(call))
            self._reach_call_names(call, reach)
        seen: set[int] = set()
        while work:
            module, fn, depth = work.pop()
            if id(fn) in seen:
                continue
            seen.add(id(fn))
            reach.func_ids.add(id(fn))
            reach.modules[module.rel] = module
            for target in self._referenced(module, fn, reach):
                if depth < _MAX_REACH_DEPTH:
                    work.append((target[0], target[1], depth + 1))
        self._close_constants(reach)
        return reach

    def _reach_call_names(self, call: ast.Call, reach: _Reach) -> None:
        skip = {"dag", "python_callable", "task_id"}
        scope = self.module.enclosing_functions(call)
        nodes: list[ast.AST] = [*call.args, *[k.value for k in call.keywords if k.arg not in skip]]

        def other_task_call(sub: ast.AST) -> bool:
            return isinstance(sub, ast.Call) and sub is not call and id(sub) in self.scanner.call_owners

        for node in nodes:
            for sub in _iter_nodes(node, prune=other_task_call):
                if (
                    isinstance(sub, ast.Name)
                    and isinstance(sub.ctx, ast.Load)
                    and sub.id not in self.dag_var_names
                ):
                    self._add_name_key(self.module, sub.id, scope, reach)
                elif isinstance(sub, ast.Attribute) and self.module.is_import_rooted(sub):
                    self._add_dotted(self.module, self.module.qualname(sub) or "", reach, None)

    def _add_name_key(self, module: _Module, name: str, scope: list[ast.AST], reach: _Reach) -> None:
        for fn in scope:
            if name in module.local_assigns(fn):
                reach.const_keys.add((module.rel, id(fn), name))
                return
        if name in module.constants:
            reach.const_keys.add((module.rel, 0, name))
            reach.modules.setdefault(module.rel, module)
        elif name in module.bindings:
            self._add_dotted(module, module.bindings[name], reach, None)

    def _add_dotted(
        self,
        module: _Module,
        dotted: str,
        reach: _Reach,
        functions: list[tuple[_Module, ast.AST]] | None,
        hops: int = 0,
    ) -> None:
        resolved = self.repo.resolve_dotted(dotted, module.extra_roots) if dotted else None
        if resolved is None or len(resolved.rest) != 1 or hops > 3:
            return
        target = self.repo.load(resolved.file)
        if target is None:
            return
        name = resolved.rest[0]
        if name in target.functions:
            if functions is not None:
                functions.append((target, target.functions[name]))
        elif name in target.constants:
            reach.const_keys.add((target.rel, 0, name))
            reach.modules.setdefault(target.rel, target)
        elif name in target.bindings:
            self._add_dotted(target, target.bindings[name], reach, functions, hops + 1)

    def _referenced(self, module: _Module, fn: ast.AST, reach: _Reach) -> list[tuple[_Module, ast.AST]]:
        functions: list[tuple[_Module, ast.AST]] = []
        for node in ast.walk(fn):
            if isinstance(node, ast.Name) and isinstance(node.ctx, ast.Load):
                name = node.id
                if name in module.functions and module.functions[name] is not fn:
                    functions.append((module, module.functions[name]))
                elif name in module.constants:
                    reach.const_keys.add((module.rel, 0, name))
                elif name in module.bindings:
                    self._add_dotted(module, module.bindings[name], reach, functions)
            elif isinstance(node, ast.Attribute) and module.is_import_rooted(node):
                self._add_dotted(module, module.qualname(node) or "", reach, functions)
        return functions

    def _close_constants(self, reach: _Reach) -> None:
        pending = list(reach.const_keys)
        while pending:
            rel, scope_id, name = pending.pop()
            module = self.repo.load(rel)
            if module is None:
                continue
            expr, scope = self._constant_expr(module, scope_id, name)
            if expr is None:
                continue
            before = set(reach.const_keys)
            for node in ast.walk(expr):
                if isinstance(node, ast.Name) and isinstance(node.ctx, ast.Load):
                    self._add_name_key(module, node.id, scope, reach)
                elif isinstance(node, ast.Attribute) and module.is_import_rooted(node):
                    self._add_dotted(module, module.qualname(node) or "", reach, None)
            pending.extend(reach.const_keys - before)

    def _constant_expr(
        self, module: _Module, scope_id: int, name: str
    ) -> tuple[ast.AST | None, list[ast.AST]]:
        if scope_id == 0:
            return module.constants.get(name), []
        for node in ast.walk(module.tree):
            if isinstance(node, _FUNC_NODES) and id(node) == scope_id:
                scope = [node, *module.enclosing_functions(node)]
                return module.local_assigns(node).get(name), scope
        return None, []

    # -- attribution
    def _owners_by_call(self, candidate: _Candidate) -> list[tuple[_DagBuilder, _TaskRecord]]:
        for ancestor in candidate.ancestors:
            owners = self.scanner.call_owners.get(id(ancestor))
            if owners:
                return owners
        return []

    def _matches(self, candidate: _Candidate, reach: _Reach) -> bool:
        if candidate.func_ids & reach.func_ids:
            return True
        return any(
            key in reach.const_keys for key in candidate.const_keys if key[2] not in self.dag_var_names
        )

    def _attribute(
        self,
        module: _Module,
        reaches: dict[int, _Reach],
        refs: dict[int, dict[tuple[str | None, str], DatasetRef]],
        *,
        allow_unattributed: bool,
    ) -> None:
        for candidate in _module_candidates(self.repo, module):
            hits: list[tuple[_DagBuilder, _TaskRecord]] = []
            if module is self.module:
                hits.extend(self._owners_by_call(candidate))
            for dag in self.scanner.dags:
                for record in dag.tasks:
                    if (dag, record) not in hits and self._matches(candidate, reaches[id(record)]):
                        hits.append((dag, record))
            for dag, record in hits:
                self._add_ref(refs[id(dag)], record.info.task_id, candidate)
            if hits or not allow_unattributed:
                continue
            owners = [
                dag
                for dag in self.scanner.dags
                if module is self.module and any(region in candidate.ancestors for region in dag.regions)
            ]
            for dag in owners or self.scanner.dags:
                self._add_ref(refs[id(dag)], None, candidate)

    @staticmethod
    def _add_ref(
        bucket: dict[tuple[str | None, str], DatasetRef], task_id: str | None, candidate: _Candidate
    ) -> None:
        key = (task_id, candidate.path)
        if key not in bucket:
            bucket[key] = DatasetRef(
                task_id=task_id,
                path=candidate.path,
                type=candidate.type,
                resolved=candidate.resolved,
                source=candidate.source,
            )

    @staticmethod
    def _finalise(bucket: dict[tuple[str | None, str], DatasetRef]) -> list[DatasetRef]:
        attributed_paths = {path for task_id, path in bucket if task_id is not None}
        refs = [
            ref
            for (task_id, path), ref in bucket.items()
            if task_id is not None or path not in attributed_paths
        ]

        def sort_key(ref: DatasetRef) -> tuple[str, int, str, str]:
            file, _, line = ref.source.rpartition(":")
            return (file, int(line) if line.isdigit() else 0, ref.task_id or "", ref.path)

        return sorted(refs, key=sort_key)
