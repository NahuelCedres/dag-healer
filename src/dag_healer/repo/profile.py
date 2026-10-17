"""Profile a user repository: where its tests live and which conventions they follow.

Everything here is static (file system + AST); no user code is imported or executed.
The resulting :class:`TestLayout` tells the repair agent where to write a reproducing
test and which style to imitate.  :func:`snapshot_hashes` supports the
"``dag-healer init`` never modifies DAGs" guarantee.
"""

from __future__ import annotations

import ast
import configparser
import hashlib
import logging
import os
import re
from collections import Counter
from collections.abc import Iterable, Iterator
from dataclasses import asdict, dataclass, field
from fnmatch import fnmatch
from pathlib import Path
from typing import Any

logger = logging.getLogger(__name__)

#: Directory names never descended into when walking a user repository.
SKIP_DIR_NAMES = frozenset(
    {
        "__pycache__",
        "node_modules",
        "venv",
        "site-packages",
        "build",
        "dist",
        "htmlcov",
    }
)

DEFAULT_TEST_PATTERNS = ("test_*.py", "*_test.py")
CONVENTIONAL_TEST_DIRS = ("tests", "test", "src/tests")
_PYTEST_FILES = ("pytest.ini", ".pytest.ini")
_HASH_SKIP_DIRS = frozenset({".git", ".hg", ".svn", "__pycache__", ".venv", "venv", "node_modules"})


@dataclass
class TestLayout:
    """Where the repository keeps its tests and how they are written."""

    __test__ = False  # keep pytest from collecting this dataclass

    tests_dir: str | None
    exists: bool
    framework: str | None  # "pytest" | "unittest" | None
    has_conftest: bool
    file_pattern: str = "test_*.py"
    uses_classes: bool = False
    import_style: str | None = None
    config_file: str | None = None

    def to_dict(self) -> dict[str, Any]:
        """Return a JSON-safe dict view."""
        return asdict(self)


@dataclass
class _PytestConfig:
    file: str
    testpaths: list[str] = field(default_factory=list)
    python_files: list[str] = field(default_factory=list)


@dataclass
class _TestFileFacts:
    rel: str
    imports_pytest: bool = False
    testcase_classes: int = 0
    plain_test_classes: int = 0
    module_test_functions: int = 0
    local_imports: list[tuple[tuple[str, str], str]] = field(default_factory=list)


# --------------------------------------------------------------------------- walking


def _is_skipped_dir(path: Path, name: str) -> bool:
    if name.startswith(".") or name in SKIP_DIR_NAMES or name.endswith(".egg-info"):
        return True
    return (path / "pyvenv.cfg").is_file()


def iter_repo_files(
    root: Path,
    start: Path | None = None,
    *,
    suffixes: tuple[str, ...] | None = None,
) -> Iterator[Path]:
    """Yield files under ``start`` (default ``root``) in deterministic order.

    Hidden directories, virtualenvs, caches and build output are pruned.  ``suffixes``
    filters by (case-insensitive) file-name ending, e.g. ``(".py",)``.
    """
    base = Path(start) if start is not None else Path(root)
    if not base.is_dir():
        return
    lowered = tuple(s.lower() for s in suffixes) if suffixes else None
    for dirpath, dirnames, filenames in os.walk(base):
        current = Path(dirpath)
        dirnames[:] = sorted(d for d in dirnames if not _is_skipped_dir(current / d, d))
        for name in sorted(filenames):
            if lowered is None or name.lower().endswith(lowered):
                yield current / name


def _rel(root: Path, path: Path) -> str:
    try:
        return path.relative_to(root).as_posix()
    except ValueError:
        return path.as_posix()


# --------------------------------------------------------------------------- hashing


def file_sha256(path: str | Path) -> str:
    """Return the hex sha256 digest of a file's bytes."""
    digest = hashlib.sha256()
    with open(path, "rb") as handle:
        for chunk in iter(lambda: handle.read(65536), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _glob_to_regex(pattern: str) -> re.Pattern[str]:
    """Translate a ``**``-aware glob into a regex matched against posix paths."""
    out: list[str] = []
    i = 0
    while i < len(pattern):
        if pattern.startswith("**/", i):
            out.append("(?:.*/)?")
            i += 3
        elif pattern.startswith("**", i):
            out.append(".*")
            i += 2
        elif pattern[i] == "*":
            out.append("[^/]*")
            i += 1
        elif pattern[i] == "?":
            out.append("[^/]")
            i += 1
        else:
            out.append(re.escape(pattern[i]))
            i += 1
    return re.compile("^" + "".join(out) + "$")


def snapshot_hashes(root: str | Path, patterns: Iterable[str] = ("**/*.py",)) -> dict[str, str]:
    """Map every file under ``root`` matching ``patterns`` to its sha256.

    Keys are posix paths relative to ``root``.  VCS metadata, virtualenvs and
    ``__pycache__`` are skipped.  Comparing two snapshots proves that a command
    did not modify the files.
    """
    base = Path(root)
    regexes = [_glob_to_regex(p.replace("\\", "/")) for p in patterns]
    result: dict[str, str] = {}
    if not base.is_dir():
        return result
    for dirpath, dirnames, filenames in os.walk(base):
        current = Path(dirpath)
        dirnames[:] = sorted(
            d for d in dirnames if d not in _HASH_SKIP_DIRS and not (current / d / "pyvenv.cfg").is_file()
        )
        for name in sorted(filenames):
            path = current / name
            rel = _rel(base, path)
            if any(rx.match(rel) for rx in regexes):
                result[rel] = file_sha256(path)
    return dict(sorted(result.items()))


# --------------------------------------------------------------------------- pytest config


def _split_values(raw: Any) -> list[str]:
    if raw is None:
        return []
    if isinstance(raw, str):
        return [v for v in raw.split() if v]
    if isinstance(raw, (list, tuple)):
        return [str(v) for v in raw if str(v).strip()]
    return []


def _ini_section(path: Path, section: str) -> dict[str, str] | None:
    parser = configparser.RawConfigParser(strict=False)
    try:
        parser.read(path, encoding="utf-8")
    except (configparser.Error, UnicodeDecodeError, OSError) as exc:
        logger.debug("Cannot parse %s: %s", path, exc)
        return None
    if not parser.has_section(section):
        return None
    return dict(parser.items(section))


def _pyproject_pytest(path: Path) -> dict[str, Any] | None:
    try:
        text = path.read_text(encoding="utf-8")
    except (OSError, UnicodeDecodeError):
        return None
    try:
        import tomllib  # Python 3.11+
    except ModuleNotFoundError:  # pragma: no cover - Python 3.10 fallback
        found = re.search(r"^\s*\[tool\.pytest(\.ini_options)?\]\s*$", text, re.MULTILINE)
        return {} if found else None
    try:
        data = tomllib.loads(text)
    except tomllib.TOMLDecodeError as exc:
        logger.debug("Cannot parse %s: %s", path, exc)
        return None
    pytest_table = data.get("tool", {}).get("pytest")
    if not isinstance(pytest_table, dict):
        return None
    ini_options = pytest_table.get("ini_options")
    if isinstance(ini_options, dict):
        return ini_options
    native = {k: v for k, v in pytest_table.items() if k != "ini_options"}
    return native if native else None


def _read_pytest_config(root: Path) -> _PytestConfig | None:
    """Find the pytest config file the way pytest does (first match wins)."""
    for name in _PYTEST_FILES:
        path = root / name
        if path.is_file():
            section = _ini_section(path, "pytest") or {}
            return _PytestConfig(
                name, _split_values(section.get("testpaths")), _split_values(section.get("python_files"))
            )
    pyproject = root / "pyproject.toml"
    if pyproject.is_file():
        options = _pyproject_pytest(pyproject)
        if options is not None:
            return _PytestConfig(
                "pyproject.toml",
                _split_values(options.get("testpaths")),
                _split_values(options.get("python_files")),
            )
    for name, section_name in (("tox.ini", "pytest"), ("setup.cfg", "tool:pytest")):
        path = root / name
        if path.is_file():
            section = _ini_section(path, section_name)
            if section is not None:
                return _PytestConfig(
                    name, _split_values(section.get("testpaths")), _split_values(section.get("python_files"))
                )
    return None


# --------------------------------------------------------------------------- test discovery


def _clean_dir(value: str) -> str:
    cleaned = value.replace("\\", "/").strip().rstrip("/")
    while cleaned.startswith("./"):
        cleaned = cleaned[2:]
    return cleaned or "."


def _find_test_files(root: Path, patterns: tuple[str, ...]) -> list[str]:
    files: list[str] = []
    for path in iter_repo_files(root, suffixes=(".py",)):
        if any(fnmatch(path.name, pattern) for pattern in patterns):
            files.append(_rel(root, path))
    return files


def _under(rel: str, directory: str) -> bool:
    return directory == "." or rel == directory or rel.startswith(directory + "/")


def _has_python_files(root: Path, directory: str) -> bool:
    return any(True for _ in iter_repo_files(root, root / directory, suffixes=(".py",)))


def _test_root_of(rel: str) -> str:
    """Directory that "owns" a test file: its top-most tests/test ancestor or its parent."""
    parts = rel.split("/")[:-1]
    for index, part in enumerate(parts):
        if part in ("tests", "test"):
            return "/".join(parts[: index + 1])
    return "/".join(parts) or "."


def _choose_tests_dir(root: Path, test_files: list[str], testpaths: list[str]) -> str | None:
    for candidate in testpaths:
        directory = _clean_dir(candidate)
        if (root / directory).is_dir() and _has_python_files(root, directory):
            return directory
    for directory in CONVENTIONAL_TEST_DIRS:
        if (root / directory).is_dir() and (
            any(_under(f, directory) for f in test_files) or (root / directory / "conftest.py").is_file()
        ):
            return directory
    if not test_files:
        return None
    counts = Counter(_test_root_of(f) for f in test_files)
    ranked = sorted(counts.items(), key=lambda item: (-item[1], item[0].count("/"), item[0]))
    return ranked[0][0]


# --------------------------------------------------------------------------- test file analysis


def _local_top_names(root: Path, exclude: set[str]) -> set[str]:
    names: set[str] = set()
    for base in (root, root / "src", root / "dags"):
        if not base.is_dir():
            continue
        for child in base.iterdir():
            if child.is_dir() and not _is_skipped_dir(child, child.name):
                names.add(child.name)
            elif child.suffix == ".py":
                names.add(child.stem)
    return {n for n in names if n not in exclude and n.isidentifier()}


def _is_testcase_base(base: ast.expr) -> bool:
    name = base.attr if isinstance(base, ast.Attribute) else base.id if isinstance(base, ast.Name) else ""
    return name.endswith("TestCase")


def _has_test_methods(node: ast.ClassDef) -> bool:
    return any(
        isinstance(item, (ast.FunctionDef, ast.AsyncFunctionDef)) and item.name.startswith("test")
        for item in node.body
    )


def _import_facts(node: ast.stmt, local_names: set[str]) -> tuple[bool, tuple[str, str] | None]:
    """Return (imports pytest, (form, top-level package) when it imports repo code)."""
    if isinstance(node, ast.Import):
        tops = [alias.name.split(".")[0] for alias in node.names]
        local = next((t for t in tops if t in local_names), None)
        return "pytest" in tops, (("import", local) if local else None)
    if isinstance(node, ast.ImportFrom) and node.level == 0 and node.module:
        top = node.module.split(".")[0]
        return top == "pytest", (("from", top) if top in local_names else None)
    return False, None


def _analyze_test_file(root: Path, rel: str, local_names: set[str]) -> _TestFileFacts | None:
    path = root / rel
    try:
        tree = ast.parse(path.read_bytes(), filename=str(path))
    except (SyntaxError, ValueError, OSError) as exc:
        logger.debug("Skipping unparsable test file %s: %s", rel, exc)
        return None
    facts = _TestFileFacts(rel=rel)
    for node in ast.walk(tree):
        if isinstance(node, (ast.Import, ast.ImportFrom)):
            imports_pytest, key = _import_facts(node, local_names)
            facts.imports_pytest = facts.imports_pytest or imports_pytest
            if key is not None:
                facts.local_imports.append((key, ast.unparse(node)))
        elif (
            isinstance(node, ast.Attribute) and isinstance(node.value, ast.Name) and node.value.id == "pytest"
        ):
            facts.imports_pytest = True
    for node in tree.body:
        if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)) and node.name.startswith("test"):
            facts.module_test_functions += 1
        elif isinstance(node, ast.ClassDef) and _has_test_methods(node):
            if any(_is_testcase_base(base) for base in node.bases):
                facts.testcase_classes += 1
            elif node.name.startswith("Test"):
                facts.plain_test_classes += 1
    return facts


def _pick_import_style(facts: list[_TestFileFacts]) -> str | None:
    counts: Counter[tuple[str, str]] = Counter()
    first_text: dict[tuple[str, str], str] = {}
    order: list[tuple[str, str]] = []
    for item in facts:
        for key, text in item.local_imports:
            counts[key] += 1
            if key not in first_text:
                first_text[key] = text
                order.append(key)
    if not counts:
        return None
    best = max(order, key=lambda key: (counts[key], -order.index(key)))
    return first_text[best]


def _pick_file_pattern(cfg: _PytestConfig | None, files: list[str]) -> str:
    if cfg and cfg.python_files:
        return cfg.python_files[0]
    names = [f.rsplit("/", 1)[-1] for f in files]
    prefixed = sum(1 for n in names if fnmatch(n, "test_*.py"))
    suffixed = sum(1 for n in names if fnmatch(n, "*_test.py") and not fnmatch(n, "test_*.py"))
    return "*_test.py" if suffixed > prefixed else "test_*.py"


def _pick_framework(cfg: _PytestConfig | None, has_conftest: bool, facts: list[_TestFileFacts]) -> str | None:
    pytest_signals = (
        cfg is not None
        or has_conftest
        or any(f.imports_pytest or f.module_test_functions or f.plain_test_classes for f in facts)
    )
    if pytest_signals:
        return "pytest"
    if any(f.testcase_classes for f in facts):
        return "unittest"
    return "pytest" if facts else None


def _conftest_present(root: Path, tests_dir: str | None) -> bool:
    if (root / "conftest.py").is_file():
        return True
    return bool(tests_dir) and (root / tests_dir / "conftest.py").is_file()


def detect_test_layout(repo_root: str | Path) -> TestLayout:
    """Detect where the repository keeps its tests and which conventions they follow.

    Looks at pytest config files (``pytest.ini``, ``pyproject.toml``
    ``[tool.pytest.ini_options]``/``[tool.pytest]``, ``tox.ini``, ``setup.cfg``
    ``[tool:pytest]``), conventional directories (``tests/``, ``test/``,
    ``src/tests``) and any directory holding ``test_*.py``/``*_test.py`` files.
    When the repository has no tests, ``exists`` is False and ``tests_dir`` None.
    """
    root = Path(repo_root).resolve()
    cfg = _read_pytest_config(root)
    patterns = tuple(cfg.python_files) if cfg and cfg.python_files else DEFAULT_TEST_PATTERNS
    test_files = _find_test_files(root, patterns)
    tests_dir = _choose_tests_dir(root, test_files, cfg.testpaths if cfg else [])
    config_file = cfg.file if cfg else None
    if tests_dir is None:
        has_conftest = _conftest_present(root, None)
        return TestLayout(
            tests_dir=None,
            exists=False,
            framework="pytest" if (cfg or has_conftest) else None,
            has_conftest=has_conftest,
            file_pattern=_pick_file_pattern(cfg, []),
            config_file=config_file,
        )
    in_dir = [f for f in test_files if _under(f, tests_dir)]
    exclude = {"tests", "test", "conftest", "setup", tests_dir.split("/")[0]}
    local_names = _local_top_names(root, exclude)
    facts = [fact for f in in_dir if (fact := _analyze_test_file(root, f, local_names)) is not None]
    has_conftest = _conftest_present(root, tests_dir)
    class_files = sum(
        1 for f in facts if (f.testcase_classes or f.plain_test_classes) and not f.module_test_functions
    )
    function_files = sum(1 for f in facts if f.module_test_functions)
    return TestLayout(
        tests_dir=tests_dir,
        exists=True,
        framework=_pick_framework(cfg, has_conftest, facts),
        has_conftest=has_conftest,
        file_pattern=_pick_file_pattern(cfg, in_dir),
        uses_classes=class_files > function_files,
        import_style=_pick_import_style(facts),
        config_file=config_file,
    )
