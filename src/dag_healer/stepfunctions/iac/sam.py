"""AWS SAM templates: state machines, functions, layers (section 9.8 of the design, v1).

* :func:`load_template` reads a template with a ``yaml.SafeLoader`` subclass that understands
  the CloudFormation short tags (``!Ref``, ``!GetAtt``, ``!Sub``, ``!Join``, ``!If``,
  ``!Select``, ``!Split``, ``!FindInMap``, ``!ImportValue``, ``!Base64``, ``!Equals``, ``!Not``,
  ``!And``, ``!Or``, ``!Condition``, ``!GetAZs``, ``!Cidr``): ``!Ref X`` becomes
  ``{"Ref": "X"}``, ``!GetAtt Fn.Arn`` becomes ``{"Fn::GetAtt": ["Fn", "Arn"]}`` and the others
  ``{"Fn::<Name>": ...}`` (any other ``!Tag`` too). Timestamps stay text, as CloudFormation reads
  them. JSON templates load too.
* :class:`SamResolver` implements :class:`~dag_healer.stepfunctions.iac.base.IacResolver`:
  ``AWS::Serverless::StateMachine`` (``DefinitionUri`` or ``Definition``,
  ``DefinitionSubstitutions``, ``Name``), ``AWS::StepFunctions::StateMachine`` (``Definition``,
  ``DefinitionString``), ``AWS::Serverless::Function`` (``CodeUri``, ``Handler``, ``Runtime``,
  ``Layers``, ``LoggingConfig.LogGroup``, with ``Globals.Function`` fallbacks) and
  ``AWS::Serverless::LayerVersion`` (``ContentUri``). Task targets resolve through
  ``${X}`` -> ``DefinitionSubstitutions`` -> ``!GetAtt Fn.Arn`` / ``!Ref Fn`` / ``!Sub`` -> the
  function's logical id; a literal function name or ARN matches a literal ``FunctionName``.

Details:

* paths (``CodeUri``, ``ContentUri``, ``DefinitionUri``) are relative to the template's folder
  and returned repo-relative (posix, ``""`` for the repository root); S3 locations, archives and
  paths outside the repository give ``None``;
* ``Globals.Function`` follows the SAM rules: a function's own value wins, maps are merged and
  lists are additive (the global entries first), so ``Layers`` of both apply;
* a layer is the code that Lambda extracts under ``/opt``: with a ``python*`` ``BuildMethod`` SAM
  installs ``ContentUri`` under ``python/``, so ``/opt/python`` is ``ContentUri`` itself; a
  prebuilt layer whose ``ContentUri`` holds a ``python/`` folder maps that folder;
* ``LoggingConfig.LogGroup`` is a literal name or a ``!Ref`` to an ``AWS::Logs::LogGroup`` with a
  literal ``LogGroupName``;
* inline definitions may use intrinsic functions: ``!Ref X`` and ``!GetAtt X.Arn`` become the
  ``${X}`` / ``${X.Arn}`` placeholders of ``Fn::Sub``, which resolve like substitutions;
* a deployed name CloudFormation generated (``<stack>-<LogicalId>-<suffix>`` for functions,
  ``<LogicalId>-<suffix>`` for state machines) matches the resource whose logical id it embeds
  (:func:`generated_name_matches`, [I]) when the template sets no literal name;
* plain CloudFormation ``AWS::Lambda::Function`` (``Code`` as a local folder, as
  ``aws cloudformation package`` reads it) and ``AWS::Lambda::LayerVersion`` (``Content``) are
  understood as well.

Nothing here does network I/O or runs code from the repository; reading the template and the
definition files (which must stay inside the repository, symlinks included) is the only file
access.
"""

from __future__ import annotations

import json
import logging
import posixpath
import threading
from collections.abc import Mapping
from functools import lru_cache
from pathlib import Path
from typing import TYPE_CHECKING, Any

from dag_healer.stepfunctions import arns
from dag_healer.stepfunctions.asl import load_definition, placeholder_name, placeholder_names, substitute
from dag_healer.stepfunctions.iac.base import ResolvedFunction, ResolvedStateMachine

if TYPE_CHECKING:
    from dag_healer.stepfunctions.asl import TaskTarget

logger = logging.getLogger(__name__)

SERVERLESS_FUNCTION = "AWS::Serverless::Function"
LAMBDA_FUNCTION = "AWS::Lambda::Function"
SERVERLESS_STATE_MACHINE = "AWS::Serverless::StateMachine"
STEPFUNCTIONS_STATE_MACHINE = "AWS::StepFunctions::StateMachine"
SERVERLESS_LAYER = "AWS::Serverless::LayerVersion"
LAMBDA_LAYER = "AWS::Lambda::LayerVersion"
LOG_GROUP = "AWS::Logs::LogGroup"
FUNCTION_TYPES: frozenset[str] = frozenset({SERVERLESS_FUNCTION, LAMBDA_FUNCTION})
STATE_MACHINE_TYPES: frozenset[str] = frozenset({SERVERLESS_STATE_MACHINE, STEPFUNCTIONS_STATE_MACHINE})
LAYER_TYPES: frozenset[str] = frozenset({SERVERLESS_LAYER, LAMBDA_LAYER})
#: The CloudFormation short tags (any other ``!Tag`` becomes ``{"Fn::Tag": ...}`` as well).
SHORT_TAGS: tuple[str, ...] = (
    "Ref",
    "GetAtt",
    "Sub",
    "Join",
    "If",
    "Select",
    "Split",
    "FindInMap",
    "ImportValue",
    "Base64",
    "Equals",
    "Not",
    "And",
    "Or",
    "Condition",
    "GetAZs",
    "Cidr",
)
#: Templates (and definition files) larger than this are refused.
MAX_TEMPLATE_BYTES = 10 * 1024 * 1024
#: How many substitution hops a function reference may take.
MAX_RESOLUTION_DEPTH = 8
_ARCHIVE_SUFFIXES = (".zip", ".jar")


# =========================================================================== loading


@lru_cache(maxsize=1)
def _loader() -> type:
    """The CloudFormation-aware ``yaml.SafeLoader`` subclass (built on first use)."""
    import yaml

    class CloudFormationLoader(yaml.SafeLoader):
        """``SafeLoader`` with the CloudFormation short tags; timestamps stay text."""

    CloudFormationLoader.yaml_implicit_resolvers = {
        first: [(tag, regexp) for tag, regexp in resolvers if tag != "tag:yaml.org,2002:timestamp"]
        for first, resolvers in yaml.SafeLoader.yaml_implicit_resolvers.items()
    }

    def construct(loader: Any, suffix: str, node: Any) -> Any:
        if isinstance(node, yaml.ScalarNode):
            value: Any = loader.construct_scalar(node)
        elif isinstance(node, yaml.SequenceNode):
            value = loader.construct_sequence(node, deep=True)
        else:
            value = loader.construct_mapping(node, deep=True)
        if suffix in ("Ref", "Condition"):
            return {suffix: value}
        if suffix == "GetAtt" and isinstance(value, str):
            resource, _, attribute = value.partition(".")
            return {"Fn::GetAtt": [resource, attribute]}
        return {f"Fn::{suffix}": value}

    CloudFormationLoader.add_multi_constructor("!", construct)
    return CloudFormationLoader


def _yaml_problem(exc: Exception) -> str:
    """Where and what (never the offending text) of a YAML error."""
    problem = getattr(exc, "problem", None) or type(exc).__name__
    mark = getattr(exc, "problem_mark", None)
    return f"{problem} (line {mark.line + 1})" if mark is not None else str(problem)


def parse_template(text: str, *, source: str = "template") -> dict[str, Any]:
    """A template from its text (YAML with short tags, or JSON); :class:`ValueError` otherwise.

    ``source`` names the template in error messages.
    """
    text = text.lstrip("\ufeff")
    data: Any = None
    if text.lstrip().startswith("{"):
        try:
            data = json.loads(text)
        except (ValueError, RecursionError):
            data = None
    if data is None:
        import yaml

        try:
            data = yaml.load(text, Loader=_loader())  # a SafeLoader subclass
        except yaml.YAMLError as exc:
            raise ValueError(f"{source}: invalid template: {_yaml_problem(exc)}") from None
        except RecursionError:
            raise ValueError(f"{source}: invalid template: nested too deeply") from None
    if not isinstance(data, dict):
        raise ValueError(f"{source}: not a SAM/CloudFormation template (expected a mapping)")
    return data


def _read_text(path: Path, what: str) -> str:
    try:
        size = path.stat().st_size
        if size > MAX_TEMPLATE_BYTES:
            raise ValueError(f"{path}: {what} larger than {MAX_TEMPLATE_BYTES} bytes")
        return path.read_text(encoding="utf-8-sig")
    except UnicodeDecodeError:
        raise ValueError(f"{path}: {what} is not UTF-8 text") from None
    except OSError as exc:
        raise ValueError(f"{path}: cannot read the {what} ({type(exc).__name__})") from None


def load_template(path: str | Path) -> dict[str, Any]:
    """A SAM/CloudFormation template (YAML with short tags, or JSON) as a dict.

    Raises :class:`ValueError` for an unreadable or invalid template (the message names the file).
    """
    file = Path(path)
    return parse_template(_read_text(file, "template"), source=str(file))


# =========================================================================== small helpers


def _mapping(value: Any) -> Mapping[str, Any]:
    return value if isinstance(value, Mapping) else {}


def _literal(value: Any) -> str | None:
    """A literal (non-intrinsic) non-empty string."""
    return value.strip() if isinstance(value, str) and value.strip() else None


def _is_intrinsic(value: Any) -> bool:
    if not isinstance(value, Mapping) or len(value) != 1:
        return False
    key = next(iter(value))
    return isinstance(key, str) and (key in ("Ref", "Condition") or key.startswith("Fn::"))


def merge_globals(global_properties: Mapping[str, Any], properties: Mapping[str, Any]) -> dict[str, Any]:
    """SAM ``Globals`` merging: own values win, maps merge, lists add up (global entries first);
    intrinsic functions count as plain values."""
    merged = dict(global_properties)
    for key, value in properties.items():
        base = merged.get(key)
        if (
            isinstance(base, Mapping)
            and isinstance(value, Mapping)
            and not (_is_intrinsic(base) or _is_intrinsic(value))
        ):
            merged[key] = merge_globals(base, value)
        elif isinstance(base, list) and isinstance(value, list):
            merged[key] = [*base, *value]
        else:
            merged[key] = value
    return merged


def generated_name_matches(name: str | None, logical_id: str | None) -> bool:
    """True when ``name`` looks generated by CloudFormation for ``logical_id`` [I].

    Generated physical names end with ``<LogicalId>-<random suffix>``: ``<stack>-<LogicalId>-<suffix>``
    for Lambda functions, ``<LogicalId>-<suffix>`` for state machines. Truncated logical ids
    (very long stack names) do not match.
    """
    if not name or not logical_id:
        return False
    parts = name.split("-")
    return len(parts) >= 2 and parts[-2] == logical_id and parts[-1].isalnum()


def _intrinsics_to_placeholders(value: Any, depth: int = 0) -> Any:
    """``{"Ref": X}`` -> ``"${X}"``, ``{"Fn::GetAtt": [X, A]}`` -> ``"${X.A}"`` and
    ``{"Fn::Sub": text}`` -> ``text`` inside an inline definition."""
    if depth > 64:
        return value
    if isinstance(value, Mapping):
        if len(value) == 1:
            key, argument = next(iter(value.items()))
            if key == "Ref" and isinstance(argument, str):
                return f"${{{argument}}}"
            if key == "Fn::GetAtt":
                parts = argument.split(".", 1) if isinstance(argument, str) else argument
                if isinstance(parts, list) and len(parts) == 2 and all(isinstance(p, str) for p in parts):
                    return f"${{{parts[0]}.{parts[1]}}}"
            if key == "Fn::Sub":
                text = argument[0] if isinstance(argument, list) and argument else argument
                if isinstance(text, str):
                    return text
        return {key: _intrinsics_to_placeholders(item, depth + 1) for key, item in value.items()}
    if isinstance(value, list):
        return [_intrinsics_to_placeholders(item, depth + 1) for item in value]
    return value


def _sub_text(value: Any) -> tuple[str | None, dict[str, Any]]:
    """The text of a ``DefinitionString`` (a string or ``Fn::Sub``) and its ``Fn::Sub`` variables."""
    if isinstance(value, str):
        return value, {}
    if isinstance(value, Mapping) and len(value) == 1 and "Fn::Sub" in value:
        argument = value["Fn::Sub"]
        if isinstance(argument, str):
            return argument, {}
        if isinstance(argument, list) and argument and isinstance(argument[0], str):
            variables = argument[1] if len(argument) > 1 and isinstance(argument[1], Mapping) else {}
            return argument[0], dict(variables)
    return None, {}


# =========================================================================== the resolver


class _Template:
    """One parsed template: its resources, functions and layers (paths repo-relative)."""

    def __init__(self, resolver: SamResolver, data: Mapping[str, Any]) -> None:
        self.resolver = resolver
        self.data = data
        resources = data.get("Resources")
        self.resources: dict[str, Mapping[str, Any]] = {
            str(name): body for name, body in _mapping(resources).items() if isinstance(body, Mapping)
        }
        self.global_function = _mapping(_mapping(data.get("Globals")).get("Function"))
        self.functions: dict[str, ResolvedFunction] = {}
        for logical_id, resource in self.resources.items():
            if resource.get("Type") in FUNCTION_TYPES:
                self.functions[logical_id] = self._function(logical_id, resource)

    def _type(self, logical_id: str) -> str | None:
        kind = self.resources.get(logical_id, {}).get("Type")
        return kind if isinstance(kind, str) else None

    # ------------------------------------------------------------------ functions
    def _function(self, logical_id: str, resource: Mapping[str, Any]) -> ResolvedFunction:
        properties = _mapping(resource.get("Properties"))
        if resource.get("Type") == SERVERLESS_FUNCTION:
            merged = merge_globals(self.global_function, properties)
            image = merged.get("PackageType") == "Image" or "ImageUri" in merged
            inline = "InlineCode" in properties
            code = None if image or inline else merged.get("CodeUri")
        else:
            merged = dict(properties)
            code = merged.get("Code") if isinstance(merged.get("Code"), str) else None
        return ResolvedFunction(
            logical_id=logical_id,
            function_name=_literal(properties.get("FunctionName")),
            code_uri=self.resolver.local_path(code, directory=True),
            handler=_literal(merged.get("Handler")),
            runtime=_literal(merged.get("Runtime")),
            layers=self._layer_roots(merged.get("Layers")),
            log_group=self._log_group(_mapping(merged.get("LoggingConfig")).get("LogGroup")),
        )

    def _layer_roots(self, value: Any) -> tuple[str, ...]:
        roots: list[str] = []
        entries = value if isinstance(value, list) else []
        for entry in entries:
            reference = _mapping(entry).get("Ref")
            if isinstance(reference, str):
                root = self._layer_root(reference)
                if root is not None and root not in roots:
                    roots.append(root)
        return tuple(roots)

    def _layer_root(self, logical_id: str) -> str | None:
        resource = self.resources.get(logical_id)
        if resource is None or resource.get("Type") not in LAYER_TYPES:
            return None
        properties = _mapping(resource.get("Properties"))
        content = properties.get("ContentUri" if resource.get("Type") == SERVERLESS_LAYER else "Content")
        root = self.resolver.local_path(content, directory=True)
        if root is None:
            return None
        build_method = _literal(_mapping(resource.get("Metadata")).get("BuildMethod"))
        if build_method and build_method.lower().startswith("python"):
            return root
        python_dir = posixpath.join(root, "python") if root else "python"
        return python_dir if (self.resolver.repo_root / python_dir).is_dir() else root

    def _log_group(self, value: Any) -> str | None:
        literal = _literal(value)
        if literal is not None:
            return literal
        reference = _mapping(value).get("Ref")
        if isinstance(reference, str) and self._type(reference) == LOG_GROUP:
            properties = _mapping(self.resources[reference].get("Properties"))
            return _literal(properties.get("LogGroupName"))
        return None

    # ------------------------------------------------------------------ state machines
    def state_machines(self) -> list[ResolvedStateMachine]:
        machines: list[ResolvedStateMachine] = []
        for logical_id, resource in self.resources.items():
            if resource.get("Type") in STATE_MACHINE_TYPES:
                machines.append(self._state_machine(logical_id, resource))
        return machines

    def _state_machine(self, logical_id: str, resource: Mapping[str, Any]) -> ResolvedStateMachine:
        properties = _mapping(resource.get("Properties"))
        serverless = resource.get("Type") == SERVERLESS_STATE_MACHINE
        name = _literal(properties.get("Name" if serverless else "StateMachineName"))
        substitutions = dict(_mapping(properties.get("DefinitionSubstitutions")))
        definition_file: str | None = None
        definition: dict[str, Any] | None = None
        uri = properties.get("DefinitionUri") if serverless else None
        if isinstance(uri, str):
            definition_file = self.resolver.local_path(uri, directory=False)
            if definition_file is not None:
                definition = self.resolver.read_definition(definition_file)
        elif isinstance(properties.get("Definition"), Mapping):
            definition = self._inline_definition(properties["Definition"], logical_id)
        elif not serverless and "DefinitionString" in properties:
            text, variables = _sub_text(properties["DefinitionString"])
            for key, item in variables.items():
                substitutions.setdefault(str(key), item)
            if text is not None:
                definition = self._definition_text(text, logical_id)
        if definition is not None:
            definition = substitute(definition, substitutions)
        return ResolvedStateMachine(
            logical_id=logical_id,
            template_file=self.resolver.template_file,
            name=name,
            definition_file=definition_file,
            definition=definition,
            substitutions=substitutions,
        )

    @staticmethod
    def _inline_definition(value: Mapping[str, Any], logical_id: str) -> dict[str, Any] | None:
        try:
            return load_definition(_intrinsics_to_placeholders(value))
        except ValueError as exc:
            logger.debug(
                "dag-healer SAM: inline definition of %s skipped (%s)", logical_id, type(exc).__name__
            )
            return None

    @staticmethod
    def _definition_text(text: str, logical_id: str) -> dict[str, Any] | None:
        try:
            return load_definition(text)
        except ValueError as exc:
            logger.debug(
                "dag-healer SAM: DefinitionString of %s skipped (%s)", logical_id, type(exc).__name__
            )
            return None

    # ------------------------------------------------------------------ function references
    def resolve(
        self, reference: Any, substitutions: Mapping[str, Any], depth: int = 0
    ) -> ResolvedFunction | None:
        """The function a reference (as written in a definition, or a substitution value) names."""
        if depth > MAX_RESOLUTION_DEPTH:
            return None
        if isinstance(reference, Mapping):
            return self._resolve_intrinsic(reference, depth)
        if not isinstance(reference, str) or not reference.strip():
            return None
        text = reference.strip()
        whole = placeholder_name(text)
        if whole is not None:
            if whole in substitutions:
                return self.resolve(substitutions[whole], substitutions, depth + 1)
            return self._symbol(whole)
        if "${" in text:
            text = substitute(text, substitutions)
        if "${" not in text:
            return self._literal_function(text)
        # placeholders inside a longer text (arn:...:function:${Fn}): one of them must name a function
        found: dict[str, ResolvedFunction] = {}
        for name in placeholder_names(text):
            if name in substitutions:
                function = self.resolve(substitutions[name], substitutions, depth + 1)
            else:
                function = self._symbol(name)
            if function is not None:
                found.setdefault(function.logical_id or "", function)
        return next(iter(found.values())) if len(found) == 1 else None

    def _resolve_intrinsic(self, value: Mapping[str, Any], depth: int) -> ResolvedFunction | None:
        if len(value) != 1:
            return None
        key, argument = next(iter(value.items()))
        if key == "Ref" and isinstance(argument, str):
            return self.functions.get(argument)
        if key == "Fn::GetAtt":
            parts = argument.split(".", 1) if isinstance(argument, str) else argument
            if (
                isinstance(parts, list)
                and len(parts) == 2
                and parts[1] == "Arn"
                and isinstance(parts[0], str)
            ):
                return self.functions.get(parts[0])
            return None
        if key == "Fn::Sub":
            text, variables = _sub_text(value)
            return self.resolve(text, variables, depth + 1) if text is not None else None
        return None

    def _symbol(self, name: str) -> ResolvedFunction | None:
        """A ``${...}`` symbol of ``Fn::Sub``: ``Fn`` (Ref) or ``Fn.Arn`` (GetAtt)."""
        logical_id, _, attribute = name.partition(".")
        if attribute and attribute != "Arn":
            return None
        return self.functions.get(logical_id)

    def _literal_function(self, text: str) -> ResolvedFunction | None:
        """A deployed function name or ARN: a literal ``FunctionName``, else a generated name."""
        try:
            name = arns.lambda_function_name(text)
        except ValueError:
            return None
        named = [function for function in self.functions.values() if function.function_name == name]
        if len(named) == 1:
            return named[0]
        generated = [
            function
            for function in self.functions.values()
            if function.function_name is None and generated_name_matches(name, function.logical_id)
        ]
        return generated[0] if len(generated) == 1 else None


class SamResolver:
    """The SAM template ``template`` (repo-relative) of the repository ``repo_root``.

    The template is read on first use and cached per (path, mtime). Paths it returns are
    repo-relative posix paths (``CodeUri: functions/customers/`` -> ``functions/customers``).
    :meth:`state_machines` and :meth:`functions` return ``[]`` when the template does not exist
    and raise :class:`ValueError` when it is invalid (the message names the file).
    """

    def __init__(self, repo_root: str | Path, template: str = "template.yaml") -> None:
        self.repo_root = Path(repo_root)
        self.template = template
        self._lock = threading.Lock()
        self._cached: tuple[tuple[int, int], _Template] | None = None
        self._definitions: dict[str, tuple[tuple[int, int], dict[str, Any] | None]] = {}

    @property
    def exists(self) -> bool:
        """True when the template file exists."""
        return (self.repo_root / self.template).is_file()

    @property
    def template_file(self) -> str:
        """The template's repo-relative posix path."""
        path = Path(self.template)
        if path.is_absolute():
            try:
                return path.resolve().relative_to(self.repo_root.resolve()).as_posix()
            except (OSError, ValueError):
                return path.as_posix()
        return posixpath.normpath(self.template.replace("\\", "/"))

    @property
    def template_dir(self) -> str:
        """The repo-relative folder of the template (``""`` for the repository root)."""
        folder = posixpath.dirname(self.template_file)
        return "" if folder in ("", ".") else folder

    def _parsed(self) -> _Template | None:
        path = self.repo_root / self.template
        try:
            stat = path.stat()
        except OSError:
            return None
        key = (stat.st_mtime_ns, stat.st_size)
        with self._lock:
            if self._cached is not None and self._cached[0] == key:
                return self._cached[1]
        parsed = _Template(self, load_template(path))
        with self._lock:
            self._cached = (key, parsed)
        return parsed

    # ------------------------------------------------------------------ paths and files
    def local_path(self, value: Any, *, directory: bool) -> str | None:
        """A template path property as a repo-relative posix path (``None`` when it is not a
        local path inside the repository). ``directory`` paths lose their trailing slash and
        refuse archives (``.zip``, ``.jar``)."""
        text = _literal(value)
        if text is None or "://" in text:
            return None
        text = text.replace("\\", "/")
        if directory and text.lower().endswith(_ARCHIVE_SUFFIXES):
            return None
        if text.startswith("/") or (len(text) > 1 and text[1] == ":"):
            try:
                relative = Path(text).resolve().relative_to(self.repo_root.resolve()).as_posix()
            except (OSError, ValueError):
                return None
            return "" if relative == "." else relative
        joined = posixpath.normpath(posixpath.join(self.template_dir, text) if self.template_dir else text)
        if joined == ".":
            return "" if directory else None
        if joined == ".." or joined.startswith("../"):
            return None
        return joined

    def read_definition(self, relative: str) -> dict[str, Any] | None:
        """The ASL file ``relative`` (repo-relative) as a definition, ``None`` when it is missing,
        not a definition or outside the repository (a symlink may not leave it); cached per
        (path, mtime)."""
        path = self.repo_root / relative
        try:
            root = self.repo_root.resolve()
            path.resolve().relative_to(root)
            stat = path.stat()
        except (OSError, ValueError):
            return None
        key = (stat.st_mtime_ns, stat.st_size)
        with self._lock:
            cached = self._definitions.get(relative)
            if cached is not None and cached[0] == key:
                return cached[1]
        try:
            definition: dict[str, Any] | None = load_definition(_read_text(path, "definition"))
        except ValueError as exc:
            logger.debug("dag-healer SAM: definition file skipped (%s)", type(exc).__name__)
            definition = None
        with self._lock:
            self._definitions[relative] = (key, definition)
        return definition

    # ------------------------------------------------------------------ the IacResolver protocol
    def state_machines(self) -> list[ResolvedStateMachine]:
        parsed = self._parsed()
        return parsed.state_machines() if parsed is not None else []

    def functions(self) -> list[ResolvedFunction]:
        parsed = self._parsed()
        return list(parsed.functions.values()) if parsed is not None else []

    def function_for(self, machine: ResolvedStateMachine, target: TaskTarget) -> ResolvedFunction | None:
        if target.kind != "lambda":
            return None
        reference = target.function or (target.resource if target.integration == "legacy" else None)
        return self.resolve_function(reference, machine.substitutions)

    def resolve_function(
        self, reference: str | None, substitutions: Mapping[str, Any] | None = None
    ) -> ResolvedFunction | None:
        """The function a reference names (``${X}`` through ``substitutions``, ``${Fn.Arn}``,
        ``${Fn}``, a literal name or ARN), or ``None``."""
        parsed = self._parsed()
        if parsed is None or not reference:
            return None
        return parsed.resolve(reference, dict(substitutions or {}))

    def resource_types(self) -> dict[str, str]:
        """Logical id -> resource type of every resource of the template."""
        parsed = self._parsed()
        if parsed is None:
            return {}
        return {
            logical_id: str(resource.get("Type"))
            for logical_id, resource in parsed.resources.items()
            if isinstance(resource.get("Type"), str)
        }


__all__ = [
    "FUNCTION_TYPES",
    "LAYER_TYPES",
    "MAX_TEMPLATE_BYTES",
    "SHORT_TAGS",
    "STATE_MACHINE_TYPES",
    "SamResolver",
    "generated_name_matches",
    "load_template",
    "merge_globals",
    "parse_template",
]
