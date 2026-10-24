"""The Step Functions :class:`~dag_healer.core.ports.CodeLocator` (section 9.8 of the design).

Order: an explicit ``options.code_map`` entry, then the IaC resolver (SAM in v1), then nothing
(``doctor`` flags the unmapped Task states). For each watched pipeline:

* the state machine resource is matched by ``options.pipelines.<id>.logical_id``, a literal
  ``Name`` equal to the configured machine's name, or ``options.pipelines.<id>.definition``;
* every Task state of the repository ASL (:func:`~dag_healer.stepfunctions.asl.index_states`)
  that runs a Lambda function becomes ``TaskInfo(task_id=<state>, operator="lambda",
  callable_name=<handler function>, callable_module=<handler module>,
  callable_file=<CodeUri>/<module path>.py, lineno=<line of the def>)``; a handler
  ``pkg.mod.fn`` maps to ``<CodeUri>/pkg/mod.py``, ``dir/mod.fn`` to ``<CodeUri>/dir/mod.py``;
* ``scan()`` returns ``DagInfo(dag_id=<pipeline id>, file=<definition file or template>,
  tasks=[...], datasets=<S3 references of the ASL + s3:// literals of the handler modules>)``.

``path_mappings(step)`` maps the Lambda runtime roots to the code of the step's function:
``/var/task`` to its ``CodeUri`` and ``/opt/python`` to its local layers. ``sam build`` installs
third-party packages into those roots (``/opt/python/pandas/...``), and a frame of one must not
map to a repository file that does not exist, so a root is mapped as a whole only when the
repository holds Python modules at its top level (the handler ``app.py``); its directories are
mapped one by one (``/opt/python/etl`` -> ``src/etl``), and a root that is not in the repository is
not mapped. Without a step, every known code root counts, and a prefix that two of them map
differently is left out (which function ran is unknown; the core's suffix match, which checks
that the file exists, stays the fallback). Nothing here does network I/O or imports user code
(handler modules are read with ``ast``).

Details:

* without ``logical_id``, a machine whose deployed name CloudFormation generated
  (``<LogicalId>-<suffix>``) matches the resource that has no literal ``Name`` [I]; with
  ``discover: true``, a template machine with a literal ``Name`` that no pipeline claims is
  reported under that name (the pipeline id discovery gives it);
* a ``code_map`` key is a function's logical id, its literal ``FunctionName`` or the deployed
  function name a definition names; the entry's ``handler`` and ``layers`` fall back to the
  template's when unset;
* the handler module is looked up in the function's code root, then in its layers (the last
  layer first: Lambda extracts later layers over earlier ones); ``lineno`` is the line of the
  handler's ``def`` (else of the assignment or import that binds it), ``0`` when not found;
* Task states that run something else (ECS, Glue, another state machine ...) are listed with
  ``operator=<kind>`` and no callable; non-Python functions get no ``callable_file``;
* ``path_mappings`` finds the step by state name and ``callable_file`` (a state name repeats
  across machines); a step it cannot find derives ``/var/task`` from ``callable_file`` and
  ``callable_module``;
* :meth:`SfnCodeLocator.log_group` and :meth:`SfnCodeLocator.log_groups` report the log groups
  the template sets for the functions (``LoggingConfig.LogGroup``): the inspector records the
  failed state's in ``native.log_group`` (the console link, a fallback of the log lookup) and the
  printed IAM policy adds them;
* an invalid template or definition never raises: the pipelines it describes are not found
  (``doctor`` reports them, see :meth:`SfnCodeLocator.missing_pipelines`), and
  :meth:`SfnCodeLocator.template_problem` says what is wrong with the template (the file and the
  line, never its text).
"""

from __future__ import annotations

import ast
import json
import logging
import posixpath
import threading
from collections.abc import Iterable, Iterator, Mapping
from dataclasses import dataclass, field
from pathlib import Path
from typing import TYPE_CHECKING, Any

from dag_healer.core.evidence import PipelineSource
from dag_healer.core.specs import DagInfo, DatasetRef, PathMapping, TaskInfo
from dag_healer.stepfunctions import arns, asl
from dag_healer.stepfunctions.iac.base import ResolvedFunction, ResolvedStateMachine
from dag_healer.stepfunctions.iac.sam import SamResolver, generated_name_matches

if TYPE_CHECKING:
    from dag_healer.stepfunctions.options import CodeMapEntry, StepFunctionsOptions
    from dag_healer.stepfunctions.runtime import PipelineEntry, PipelineIndex

logger = logging.getLogger(__name__)

LAMBDA_TASK_ROOT = "/var/task"
LAMBDA_LAYER_ROOT = "/opt/python"
LAMBDA_OPERATOR = "lambda"
#: Handler modules (and the modules they import) larger than this are not parsed.
MAX_MODULE_BYTES = 2 * 1024 * 1024
#: At most this many drift notes are returned (a last note counts the rest).
MAX_DRIFT_NOTES = 20
#: Object key suffixes of datasets (``DatasetRef.type``).
DATASET_SUFFIXES: Mapping[str, str] = {
    ".csv": "csv",
    ".parquet": "parquet",
    ".json": "json",
    ".jsonl": "json",
    ".ndjson": "json",
}
#: Directories of a code root that never hold the function's own packages.
NOT_PACKAGE_DIRS = frozenset({"__pycache__", "node_modules"})
#: ItemReader ``ReaderConfig.InputType`` -> ``DatasetRef.type``.
READER_TYPES: Mapping[str, str] = {"CSV": "csv", "JSON": "json", "JSONL": "json", "PARQUET": "parquet"}
#: Module-level blocks whose statements still bind module names.
_BLOCKS: tuple[type[ast.stmt], ...] = tuple(
    kind
    for kind in (ast.If, ast.Try, getattr(ast, "TryStar", None), ast.With, ast.AsyncWith)
    if isinstance(kind, type)
)


@dataclass(frozen=True)
class _Code:
    """Where one Lambda function's code lives (repo-relative posix paths)."""

    root: str  # /var/task ("" = the repository root)
    handler: str | None
    layers: tuple[str, ...]  # /opt/python roots, in the function's layer order
    runtime: str | None = None


@dataclass(frozen=True)
class _Pipeline:
    """A watched pipeline found in the repository."""

    pipeline_id: str
    file: str | None  # repo-relative definition file, or the template for an inline definition
    definition: Mapping[str, Any] | None
    machine: ResolvedStateMachine | None = None


@dataclass(frozen=True)
class _Step:
    """One Task state of a pipeline and the code it runs."""

    state: asl.StateInfo
    target: asl.TaskTarget
    code: _Code | None
    task: TaskInfo

    @property
    def is_lambda(self) -> bool:
        return self.target.kind == LAMBDA_OPERATOR

    @property
    def mapped(self) -> bool:
        """The step's code is found (Python: the handler module and its definition)."""
        if self.code is None:
            return False
        runtime = self.code.runtime
        if runtime is not None and not runtime.startswith("python"):
            return True
        return self.task.callable_file is not None and self.task.lineno > 0


@dataclass
class _Scan:
    """One pipeline with its steps."""

    pipeline: _Pipeline
    steps: list[_Step] = field(default_factory=list)


# =========================================================================== small helpers


def _clean(path: str | None) -> str | None:
    """A repo-relative posix path (``None`` when empty or outside the repository)."""
    if not isinstance(path, str) or not path.strip():
        return None
    text = path.strip().replace("\\", "/")
    while text.startswith("./"):
        text = text[2:]
    if text.startswith("/") or (len(text) > 1 and text[1] == ":"):
        return None
    joined = posixpath.normpath(text)
    if joined in (".", "..") or joined.startswith("../"):
        return None
    return joined


def _join(root: str, relative: str) -> str:
    return posixpath.normpath(posixpath.join(root, relative)) if root else relative


def _is_file(repo_root: Path, relative: str | None) -> bool:
    """True when ``relative`` is a file inside the repository (symlinks may not escape it)."""
    if not relative:
        return False
    try:
        root = repo_root.resolve()
        candidate = (root / relative).resolve()
        candidate.relative_to(root)
    except (OSError, ValueError):
        return False
    return candidate.is_file()


def _read_source(repo_root: Path, relative: str) -> str | None:
    """The text of a repository file (``None`` when missing, too large or outside the repository)."""
    if not _is_file(repo_root, relative):
        return None
    path = repo_root / relative
    try:
        if path.stat().st_size > MAX_MODULE_BYTES:
            return None
        return path.read_text(encoding="utf-8-sig", errors="replace")
    except OSError:
        return None


def _parse_module(repo_root: Path, relative: str) -> ast.Module | None:
    source = _read_source(repo_root, relative)
    if source is None:
        return None
    try:
        return ast.parse(source, filename=relative)
    except (SyntaxError, ValueError, RecursionError, MemoryError):
        logger.debug("dag-healer stepfunctions: cannot parse a handler module")
        return None


def split_handler(handler: str | None) -> tuple[str | None, str | None]:
    """``(module, function)`` of a Lambda Python handler (``pkg.mod.fn`` -> ``("pkg.mod", "fn")``,
    ``dir/mod.fn`` -> ``("dir.mod", "fn")``); ``(None, None)`` when it is not one."""
    if not isinstance(handler, str):
        return None, None
    text = handler.strip().replace("\\", "/")
    module_part, dot, function = text.rpartition(".")
    if not dot or not function or not module_part:
        return None, None
    module = module_part.strip("/").replace("/", ".")
    parts = module.split(".")
    if any(not part or part in (".", "..") or part.isspace() for part in parts):
        return None, None
    return module, function


def _module_files(module: str) -> tuple[str, str]:
    path = module.replace(".", "/")
    return f"{path}.py", f"{path}/__init__.py"


def _find_module(repo_root: Path, module: str, roots: Iterable[str]) -> str | None:
    """The repo-relative file of ``module`` under the first root that has it."""
    for root in roots:
        for candidate in _module_files(module):
            relative = _clean(_join(root, candidate))
            if relative is not None and _is_file(repo_root, relative):
                return relative
    return None


def _binding_line(tree: ast.Module, name: str) -> int:
    """Line of the module-level ``def name`` (the last one), else of the last assignment or
    import that binds ``name``; ``0`` when nothing does."""
    definition, other = 0, 0
    pending: list[ast.stmt] = list(tree.body)
    while pending:
        node = pending.pop(0)
        if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)) and node.name == name:
            definition = max(definition, node.lineno)
        elif isinstance(node, ast.ClassDef) and node.name == name:
            other = max(other, node.lineno)
        elif isinstance(node, ast.Assign):
            if any(isinstance(target, ast.Name) and target.id == name for target in node.targets):
                other = max(other, node.lineno)
        elif isinstance(node, ast.AnnAssign) and isinstance(node.target, ast.Name) and node.target.id == name:
            other = max(other, node.lineno)
        elif isinstance(node, (ast.Import, ast.ImportFrom)):
            for alias in node.names:
                bound = alias.asname or alias.name.split(".")[0]
                if bound == name:
                    other = max(other, node.lineno)
        elif isinstance(node, _BLOCKS):
            for block in ("body", "orelse", "finalbody"):
                pending.extend(getattr(node, block, []) or [])
            for handler in getattr(node, "handlers", []) or []:
                pending.extend(handler.body)
    return definition or other


def _imported_modules(tree: ast.Module) -> list[str]:
    """Absolute module names a module imports (``import a.b``, ``from a import b`` gives both
    ``a`` and ``a.b``)."""
    names: list[str] = []
    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            names.extend(alias.name for alias in node.names)
        elif isinstance(node, ast.ImportFrom) and node.module and not node.level:
            names.append(node.module)
            names.extend(f"{node.module}.{alias.name}" for alias in node.names if alias.name != "*")
    return list(dict.fromkeys(names))


def _dataset_type(key: str | None) -> str | None:
    lowered = (key or "").lower()
    return next((kind for suffix, kind in DATASET_SUFFIXES.items() if lowered.endswith(suffix)), None)


def _joined_text(node: ast.JoinedStr) -> str:
    parts: list[str] = []
    for value in node.values:
        if isinstance(value, ast.Constant) and isinstance(value.value, str):
            parts.append(value.value)
        elif isinstance(value, ast.FormattedValue):
            parts.append("{" + ast.unparse(value.value) + "}")
    return "".join(parts)


def _s3_literals(tree: ast.Module) -> list[tuple[int, str, bool]]:
    """``(line, uri, literal)`` of the ``s3://`` dataset strings of a module (f-strings whose
    literal tail names a dataset count, as not literal)."""
    found: list[tuple[int, str, bool]] = []
    inside_fstring: set[int] = set()
    for node in ast.walk(tree):
        if isinstance(node, ast.JoinedStr):
            inside_fstring.update(id(value) for value in node.values)
            text = _joined_text(node)
            if text.startswith("s3://") and _dataset_type(text) and " " not in text:
                found.append((node.lineno, text, False))
    for node in ast.walk(tree):
        if isinstance(node, ast.Constant) and isinstance(node.value, str) and id(node) not in inside_fstring:
            text = node.value.strip()
            if text.startswith("s3://") and _dataset_type(text) and not any(c.isspace() for c in text):
                found.append((node.lineno, text, True))
    return sorted(found)


def _root_pairs(repo_root: Path, prefix: str, base: str) -> list[tuple[str, str]]:
    """``(runtime prefix, repo base)`` pairs of one code root (see the module docstring): the root
    itself when it holds Python modules at its top level, and each of its package directories."""
    directory = repo_root / base if base else repo_root
    try:
        entries = sorted(directory.iterdir(), key=lambda entry: entry.name)
    except OSError:  # not in the repository (or unreadable): nothing maps into it
        return []
    pairs: list[tuple[str, str]] = []
    try:
        if any(entry.suffix == ".py" and entry.is_file() for entry in entries):
            pairs.append((prefix, base))
        for entry in entries:
            if entry.is_dir() and not entry.name.startswith(".") and entry.name not in NOT_PACKAGE_DIRS:
                pairs.append((f"{prefix}/{entry.name}", _join(base, entry.name)))
    except OSError:
        return []
    return pairs


def _line_of(text: str | None, needle: str | None) -> int:
    if not text or not needle:
        return 0
    position = text.find(json.dumps(needle))
    if position < 0:
        position = text.find(needle)
    return text.count("\n", 0, position) + 1 if position >= 0 else 0


# =========================================================================== the locator


class SfnCodeLocator:
    """Static mapping from watched state machines and their states to repository code."""

    def __init__(
        self,
        cfg: Any | None,
        options: StepFunctionsOptions,
        *,
        pipelines: PipelineIndex | None = None,
    ) -> None:
        self._cfg = cfg
        self.options = options
        self._pipelines = pipelines
        self._lock = threading.Lock()
        self._resolvers: dict[tuple[str, str], SamResolver] = {}
        self._warned: set[str] = set()

    # ------------------------------------------------------------------ the CodeLocator port
    def scan(self, repo_root: Path) -> list[DagInfo]:
        """Every watched pipeline found in the repository, with its Lambda Task states."""
        root = Path(repo_root)
        specs: list[DagInfo] = []
        for scan in self._scan(root):
            pipeline = scan.pipeline
            specs.append(
                DagInfo(
                    dag_id=pipeline.pipeline_id,
                    file=pipeline.file or "",
                    tasks=[step.task for step in scan.steps],
                    datasets=self._datasets(root, scan),
                    imports=self._imports(root, scan),
                )
            )
        logger.debug("dag-healer stepfunctions: scan found %d pipeline(s)", len(specs))
        return specs

    def path_mappings(self, repo_root: Path, step: TaskInfo | None) -> list[PathMapping]:
        """Lambda runtime prefixes -> repo code roots (the core adds ``(repo_root, "")``); see the
        module docstring."""
        root = Path(repo_root)
        steps = [item for scan in self._scan(root) for item in scan.steps if item.code is not None]
        if step is None:
            codes = [item.code for item in steps if item.code is not None]
            codes.extend(self._code_map_codes())
            return self._mappings(root, codes)
        matching = [
            item.code
            for item in steps
            if item.code is not None
            and item.task.task_id == step.task_id
            and (not step.callable_file or item.task.callable_file == step.callable_file)
        ]
        if matching:
            return self._mappings(root, matching)
        derived = self._root_of(step)
        if derived is None:
            codes = [item.code for item in steps if item.code is not None]
            return self._mappings(root, [*codes, *self._code_map_codes()])
        layers = [
            layer for item in steps if item.code and item.code.root == derived for layer in item.code.layers
        ]
        return self._mappings(root, [_Code(derived, None, tuple(dict.fromkeys(layers)))])

    def pipeline_file(self, repo_root: Path, pipeline_id: str, source: PipelineSource | None) -> str | None:
        """The repo-relative definition file of a pipeline (the ASL, else the template)."""
        root = Path(repo_root)
        reported = self._reported_file(root, source)
        if reported is not None:
            return reported
        for pipeline in self._find_pipelines(root):
            if pipeline.pipeline_id == pipeline_id:
                return pipeline.file if _is_file(root, pipeline.file) else None
        return None

    # ------------------------------------------------------------------ doctor and drift
    def unmapped_states(self, repo_root: Path) -> dict[str, list[str]]:
        """``{pipeline_id: [Task states whose code is not found]}`` (doctor).

        Only Lambda Task states count; pipelines whose Lambda states are all mapped are left
        out (:meth:`missing_pipelines` lists the pipelines not found at all).
        """
        unmapped: dict[str, list[str]] = {}
        for scan in self._scan(Path(repo_root)):
            names = [step.state.name for step in scan.steps if step.is_lambda and not step.mapped]
            if names:
                unmapped[scan.pipeline.pipeline_id] = names
        return unmapped

    def log_group(self, repo_root: Path, pipeline_id: str, step_id: str) -> str | None:
        """The log group the template sets (``LoggingConfig.LogGroup``) for the Lambda function of
        state ``step_id`` of ``pipeline_id``; ``None`` when that function logs to its default group
        (``/aws/lambda/<name>``) or is not found."""
        for _pipeline, state, function in self._lambda_functions(Path(repo_root), pipeline_id):
            if state.name == step_id:
                return function.log_group if function is not None else None
        return None

    def log_groups(self, repo_root: Path) -> list[str]:
        """Every log group the template sets for the Lambda functions of the watched pipelines
        (``LoggingConfig.LogGroup``; pipeline then state order, without repeats). The default
        ``/aws/lambda/<name>`` groups are not listed."""
        groups = [
            function.log_group
            for _pipeline, _state, function in self._lambda_functions(Path(repo_root))
            if function is not None and function.log_group
        ]
        return list(dict.fromkeys(groups))

    def template_problem(self, repo_root: Path) -> str | None:
        """Why the configured SAM template cannot be read (the file and the problem, never its
        text); ``None`` when it can, or when no template is configured or present."""
        resolver = self._resolver(Path(repo_root))
        if resolver is None:
            return None
        try:
            resolver.state_machines()
        except ValueError as exc:
            return _template_message(resolver, exc)
        return None

    def missing_pipelines(self, repo_root: Path) -> list[str]:
        """The configured pipelines whose state machine is not found in the repository."""
        found = {pipeline.pipeline_id for pipeline in self._find_pipelines(Path(repo_root))}
        return [entry.pipeline_id for entry in self._entries() if entry.pipeline_id not in found]

    def drift(self, repo_root: Path, pipeline_id: str, deployed: str | Mapping[str, Any]) -> list[str]:
        """Notes about states present in the deployed definition but not in the repository's,
        or the other way round (joined by state name); ``[]`` when they match."""
        root = Path(repo_root)
        pipeline = next((p for p in self._find_pipelines(root) if p.pipeline_id == pipeline_id), None)
        if pipeline is None or pipeline.definition is None:
            return [f"no definition of pipeline {pipeline_id} was found in the repository"]
        where = pipeline.file or "the repository"
        try:
            ran = asl.index_states(deployed)
        except ValueError:
            return ["the deployed definition could not be read"]
        try:
            repo = asl.index_states(pipeline.definition)
        except ValueError:
            return [f"the definition in {where} could not be read"]
        notes: list[str] = []
        for name in ran:
            if name not in repo:
                notes.append(f"state {name!r} is in the deployed definition but not in {where}")
        for name, state in repo.items():
            deployed_state = ran.get(name)
            if deployed_state is None:
                notes.append(f"state {name!r} is in {where} but not in the deployed definition")
            elif deployed_state.type != state.type:
                notes.append(
                    f"state {name!r} is a {deployed_state.type or 'typeless'} state in the deployed "
                    f"definition but a {state.type or 'typeless'} state in {where}"
                )
            elif deployed_state.parent_path != state.parent_path:
                notes.append(
                    f"state {name!r} is {_placement(deployed_state)} in the deployed definition but "
                    f"{_placement(state)} in {where}"
                )
        if len(notes) > MAX_DRIFT_NOTES:
            hidden = len(notes) - (MAX_DRIFT_NOTES - 1)
            notes = [*notes[: MAX_DRIFT_NOTES - 1], f"... and {hidden} more differences"]
        return notes

    # ------------------------------------------------------------------ pipelines
    def _entries(self) -> list[PipelineEntry]:
        if self._pipelines is not None:
            return self._pipelines.entries()
        from dag_healer.stepfunctions.runtime import PipelineIndex

        return PipelineIndex.from_options(self.options).entries()

    def _sam(self, repo_root: Path) -> SamResolver:
        """The (cached) resolver of the repository: it also reads and caches the ASL files."""
        key = (str(repo_root), self.options.template or "")
        with self._lock:
            resolver = self._resolvers.get(key)
            if resolver is None:
                resolver = self._resolvers[key] = SamResolver(repo_root, self.options.template or "")
        return resolver

    def _resolver(self, repo_root: Path) -> SamResolver | None:
        """The IaC resolver, when a template is configured and exists."""
        if not self.options.template:
            return None
        resolver = self._sam(repo_root)
        return resolver if resolver.exists else None

    def _machines(self, resolver: SamResolver | None) -> list[ResolvedStateMachine]:
        if resolver is None:
            return []
        try:
            return resolver.state_machines()
        except ValueError as exc:
            problem = _template_message(resolver, exc)
            with self._lock:
                first = problem not in self._warned
                self._warned.add(problem)
            logger.log(
                logging.WARNING if first else logging.DEBUG,
                "dag-healer stepfunctions: the SAM template %s cannot be read (doctor names the problem)",
                resolver.template_file,
            )
            return []

    def _find_pipelines(self, repo_root: Path) -> list[_Pipeline]:
        resolver = self._resolver(repo_root)
        machines = self._machines(resolver)
        claimed: set[str] = set()
        found: list[_Pipeline] = []
        for entry in self._entries():
            pipeline = self._pipeline_for(repo_root, entry, machines)
            if pipeline is None:
                continue
            if pipeline.machine is not None:
                claimed.add(pipeline.machine.logical_id)
            found.append(pipeline)
        if self.options.discover:
            known = {pipeline.pipeline_id for pipeline in found}
            for machine in machines:
                if machine.name and machine.logical_id not in claimed and machine.name not in known:
                    found.append(self._from_machine(machine.name, machine, None, repo_root))
                    known.add(machine.name)
        return found

    def _pipeline_for(
        self, repo_root: Path, entry: PipelineEntry, machines: list[ResolvedStateMachine]
    ) -> _Pipeline | None:
        try:
            name = entry.name
        except ValueError:
            name = None
        definition_option = _clean(entry.definition)
        machine = None
        if entry.logical_id:
            machine = next((m for m in machines if m.logical_id == entry.logical_id), None)
        if machine is None and name:
            machine = next((m for m in machines if m.name and m.name == name), None)
        if machine is None and definition_option:
            machine = next((m for m in machines if m.definition_file == definition_option), None)
        if machine is None and not entry.logical_id and name:
            generated = [m for m in machines if not m.name and generated_name_matches(name, m.logical_id)]
            machine = generated[0] if len(generated) == 1 else None
        if machine is not None:
            return self._from_machine(entry.pipeline_id, machine, definition_option, repo_root)
        if definition_option:
            definition = self._sam(repo_root).read_definition(definition_option)
            if definition is not None:
                return _Pipeline(entry.pipeline_id, definition_option, definition)
        return None

    def _from_machine(
        self,
        pipeline_id: str,
        machine: ResolvedStateMachine,
        definition_option: str | None,
        repo_root: Path,
    ) -> _Pipeline:
        definition = machine.definition
        file = machine.definition_file if machine.definition_file else machine.template_file
        if definition is None and definition_option:
            fallback = self._sam(repo_root).read_definition(definition_option)
            if fallback is not None:
                definition = asl.substitute(fallback, machine.substitutions)
                file = definition_option
        return _Pipeline(pipeline_id, file, definition, machine)

    # ------------------------------------------------------------------ steps
    def _scan(self, repo_root: Path) -> list[_Scan]:
        resolver = self._resolver(repo_root)
        scans: list[_Scan] = []
        for pipeline in self._find_pipelines(repo_root):
            scans.append(_Scan(pipeline, self._steps(repo_root, pipeline, resolver)))
        return scans

    def _steps(self, repo_root: Path, pipeline: _Pipeline, resolver: SamResolver | None) -> list[_Step]:
        if pipeline.definition is None:
            return []
        try:
            states = asl.index_states(pipeline.definition)
        except ValueError:
            return []
        steps: list[_Step] = []
        for state in states.values():
            target = asl.compute_target(state)
            if target is None:
                continue
            if target.kind != LAMBDA_OPERATOR:
                steps.append(_Step(state, target, None, TaskInfo(task_id=state.name, operator=target.kind)))
                continue
            function = self._function(resolver, pipeline, target)
            code = self._code(target, function)
            steps.append(_Step(state, target, code, self._task_info(repo_root, state, code)))
        return steps

    def _lambda_functions(
        self, repo_root: Path, pipeline_id: str | None = None
    ) -> Iterator[tuple[str, asl.StateInfo, ResolvedFunction | None]]:
        """``(pipeline id, state, template function)`` of every Lambda Task state of the watched
        pipelines (of ``pipeline_id`` only when given); handler modules are not read."""
        resolver = self._resolver(repo_root)
        for pipeline in self._find_pipelines(repo_root):
            if pipeline_id is not None and pipeline.pipeline_id != pipeline_id:
                continue
            if pipeline.definition is None:
                continue
            try:
                states = asl.index_states(pipeline.definition)
            except ValueError:
                continue
            for state in states.values():
                target = asl.compute_target(state)
                if target is not None and target.kind == LAMBDA_OPERATOR:
                    yield pipeline.pipeline_id, state, self._function(resolver, pipeline, target)

    @staticmethod
    def _function(
        resolver: SamResolver | None, pipeline: _Pipeline, target: asl.TaskTarget
    ) -> ResolvedFunction | None:
        if resolver is None:
            return None
        try:
            if pipeline.machine is not None:
                return resolver.function_for(pipeline.machine, target)
            reference = target.function or (target.resource if target.integration == "legacy" else None)
            return resolver.resolve_function(reference)
        except ValueError:
            return None

    def _code_map_entry(self, keys: Iterable[str | None]) -> CodeMapEntry | None:
        code_map = self.options.code_map
        for key in keys:
            if key and key in code_map:
                return code_map[key]
        return None

    def _code(self, target: asl.TaskTarget, function: ResolvedFunction | None) -> _Code | None:
        keys: list[str | None] = []
        if function is not None:
            keys.extend((function.logical_id, function.function_name))
        reference = target.function or target.resource
        if reference and "${" not in reference:  # a literal name or ARN, as deployed
            try:
                keys.append(arns.lambda_function_name(reference))
            except ValueError:
                pass
            keys.append(reference)
        entry = self._code_map_entry(keys)
        if entry is not None:
            return _Code(
                root=entry.path,
                handler=entry.handler or (function.handler if function else None),
                layers=tuple(entry.layers) or (function.layers if function else ()),
                runtime=function.runtime if function else None,
            )
        if function is not None and function.code_uri is not None:
            return _Code(function.code_uri, function.handler, function.layers, function.runtime)
        return None

    def _task_info(self, repo_root: Path, state: asl.StateInfo, code: _Code | None) -> TaskInfo:
        if code is None:
            return TaskInfo(task_id=state.name, operator=LAMBDA_OPERATOR)
        module, function = split_handler(code.handler)
        python = code.runtime is None or code.runtime.startswith("python")
        if module is None or function is None or not python:
            return TaskInfo(
                task_id=state.name, operator=LAMBDA_OPERATOR, callable_name=function, callable_module=module
            )
        file = _find_module(repo_root, module, (code.root, *reversed(code.layers)))
        lineno = 0
        if file is not None:
            tree = _parse_module(repo_root, file)
            lineno = _binding_line(tree, function) if tree is not None else 0
        return TaskInfo(
            task_id=state.name,
            operator=LAMBDA_OPERATOR,
            callable_name=function,
            callable_module=module,
            callable_file=file,
            lineno=lineno,
        )

    # ------------------------------------------------------------------ datasets and imports
    def _datasets(self, repo_root: Path, scan: _Scan) -> list[DatasetRef]:
        refs: dict[tuple[str | None, str], DatasetRef] = {}
        definition_text = _read_source(repo_root, scan.pipeline.file) if scan.pipeline.file else None
        try:
            states = asl.index_states(scan.pipeline.definition) if scan.pipeline.definition else {}
        except ValueError:
            states = {}
        for reference in asl.s3_references(states):
            if reference.role == "result_writer":
                continue
            kind = READER_TYPES.get(reference.input_type or "") or _dataset_type(reference.key)
            if kind is None:
                continue
            path = reference.uri or f"s3://{reference.bucket or '<dynamic>'}/{reference.key or '<dynamic>'}"
            line = _line_of(definition_text, reference.key or reference.bucket)
            refs.setdefault(
                (reference.state, path),
                DatasetRef(
                    task_id=reference.state,
                    path=path,
                    type=kind,
                    resolved=reference.uri is not None,
                    source=f"{scan.pipeline.file or ''}:{line}",
                ),
            )
        for step in scan.steps:
            for relative in self._step_modules(repo_root, step):
                tree = _parse_module(repo_root, relative)
                if tree is None:
                    continue
                for line, uri, literal in _s3_literals(tree):
                    kind = _dataset_type(uri)
                    if kind is not None:
                        refs.setdefault(
                            (step.state.name, uri),
                            DatasetRef(step.state.name, uri, kind, literal, f"{relative}:{line}"),
                        )
        return list(refs.values())

    def _step_modules(self, repo_root: Path, step: _Step) -> list[str]:
        """The handler module of a Python Lambda step and the repository modules it imports."""
        if step.code is None or step.task.callable_file is None:
            return []
        modules = [step.task.callable_file]
        tree = _parse_module(repo_root, step.task.callable_file)
        if tree is not None:
            roots = (step.code.root, *reversed(step.code.layers))
            for name in _imported_modules(tree):
                found = _find_module(repo_root, name, roots)
                if found is not None and found not in modules:
                    modules.append(found)
        return modules

    def _imports(self, repo_root: Path, scan: _Scan) -> list[str]:
        names: list[str] = []
        for step in scan.steps:
            if step.code is None or step.task.callable_file is None:
                continue
            tree = _parse_module(repo_root, step.task.callable_file)
            if tree is None:
                continue
            roots = (step.code.root, *reversed(step.code.layers))
            for name in _imported_modules(tree):
                if _find_module(repo_root, name, roots) is not None:
                    names.append(name)
        return sorted(dict.fromkeys(names))

    # ------------------------------------------------------------------ mappings
    def _code_map_codes(self) -> list[_Code]:
        return [
            _Code(entry.path, entry.handler, tuple(entry.layers)) for entry in self.options.code_map.values()
        ]

    @staticmethod
    def _mappings(repo_root: Path, codes: Iterable[_Code]) -> list[PathMapping]:
        """The mappings of the code roots that may have run (``/var/task`` ones first).

        Within one function a later layer wins over an earlier one (Lambda extracts it last);
        a prefix two functions map differently is left out.
        """
        merged: dict[str, str | None] = {}
        for code in codes:
            own: dict[str, str] = {}
            roots = [
                (LAMBDA_TASK_ROOT, code.root),
                *((LAMBDA_LAYER_ROOT, layer) for layer in reversed(code.layers)),
            ]
            for prefix, base in roots:
                for runtime_prefix, repo_base in _root_pairs(repo_root, prefix, base):
                    own.setdefault(runtime_prefix, repo_base)
            for runtime_prefix, repo_base in own.items():
                if runtime_prefix not in merged:
                    merged[runtime_prefix] = repo_base
                elif merged[runtime_prefix] != repo_base:
                    merged[runtime_prefix] = None  # which function ran is unknown
        found = [PathMapping(prefix, base) for prefix, base in merged.items() if base is not None]
        return [m for m in found if _under(m.runtime_prefix, LAMBDA_TASK_ROOT)] + [
            m for m in found if not _under(m.runtime_prefix, LAMBDA_TASK_ROOT)
        ]

    @staticmethod
    def _root_of(step: TaskInfo) -> str | None:
        """The code root a step's ``callable_file`` implies (``<root>/<module path>.py``)."""
        file = _clean(step.callable_file)
        module = step.callable_module
        if file is None or not module:
            return None
        for suffix in _module_files(module):
            if file == suffix:
                return ""
            if file.endswith("/" + suffix):
                return file[: -len(suffix) - 1]
        return None

    @staticmethod
    def _reported_file(repo_root: Path, source: PipelineSource | None) -> str | None:
        """A repository file the orchestrator reported (``repo:<path>`` origins, relative paths)."""
        if source is None:
            return None
        candidates = [source.relative_path, source.path]
        if source.origin.startswith("repo:"):
            candidates.insert(0, source.origin[len("repo:") :])
        for candidate in candidates:
            if not isinstance(candidate, str) or arns.is_arn(candidate):
                continue
            relative = _clean(candidate)
            if relative is not None and _is_file(repo_root, relative):
                return relative
        return None


def _placement(state: asl.StateInfo) -> str:
    return f"inside {'/'.join(state.parent_path)}" if state.parent_path else "at the top level"


def _under(path: str, root: str) -> bool:
    return path == root or path.startswith(root + "/")


def _template_message(resolver: SamResolver, exc: ValueError) -> str:
    """The template error with the template's repo-relative path (the resolver names its file)."""
    message = str(exc)
    absolute = str(resolver.repo_root / resolver.template)
    if message.startswith(absolute):
        return resolver.template_file + message[len(absolute) :]
    return message


__all__ = [
    "DATASET_SUFFIXES",
    "LAMBDA_LAYER_ROOT",
    "LAMBDA_OPERATOR",
    "LAMBDA_TASK_ROOT",
    "MAX_DRIFT_NOTES",
    "NOT_PACKAGE_DIRS",
    "READER_TYPES",
    "SfnCodeLocator",
    "split_handler",
]
