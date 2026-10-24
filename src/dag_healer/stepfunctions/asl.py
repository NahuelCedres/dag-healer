"""Amazon States Language definitions: states, Task targets and S3 references (pure).

* :func:`load_definition` reads a definition (JSON text, a mapping, or YAML text for
  ``*.asl.yaml``); SAM placeholders written outside JSON strings (``"MaxConcurrency":
  ${Concurrency}``, valid once CloudFormation substituted them) are read as strings;
* :func:`index_states` flattens every state of a definition, recursing through Parallel
  ``Branches`` and Map ``Iterator``/``ItemProcessor``; state names are unique across the whole
  machine (ASL spec), so the index is keyed by name;
* :func:`compute_target` says what a Task state runs: an optimised Lambda integration
  (``arn:aws:states:::lambda:invoke[.waitForTaskToken]`` with ``Parameters.FunctionName`` or the
  JSONata ``Arguments.FunctionName``), a legacy Lambda ARN, or another service integration;
* :func:`s3_references` lists the S3 locations a definition names (Distributed Map
  ``ItemReader``/``ResultWriter``, literal ``Bucket``/``Key`` parameters, ``s3://`` URIs in
  parameters);
* :func:`substitute` applies SAM/CloudFormation ``DefinitionSubstitutions`` (``${Name}``).

Everything here works on static definition data (the repository's ASL files or the definition
``DescribeStateMachineForExecution`` returns), never on execution input, and does no I/O. Odd
definitions are tolerated: states that are not objects are skipped, a duplicated state name
keeps its first occurrence and sub-machines nested deeper than :data:`MAX_NESTING` are not
indexed. A Task whose ``Resource`` is a single ``${Placeholder}`` is taken for a legacy Lambda
ARN, the SAM idiom (``"Resource": "${MyFunctionArn}"``) [I]; the IaC resolver confirms it.
"""

from __future__ import annotations

import json
import re
from collections.abc import Mapping
from dataclasses import dataclass, field
from functools import lru_cache
from typing import Any

LAMBDA_INVOKE_PREFIX = "arn:aws:states:::lambda:invoke"
#: Deepest Map/Parallel nesting that is indexed.
MAX_NESTING = 64
#: The state types of the ASL specification.
STATE_TYPES: frozenset[str] = frozenset(
    {"Task", "Map", "Parallel", "Pass", "Choice", "Wait", "Succeed", "Fail"}
)
DEFAULT_QUERY_LANGUAGE = "JSONPath"

#: Optimised service integrations (``arn:<partition>:states:::<service>:<action>``) -> TaskTarget kind.
_SERVICE_KINDS: Mapping[str, str] = {
    "lambda": "lambda",
    "ecs": "ecs",
    "batch": "batch",
    "glue": "glue",
    "states": "states",
    "http": "http",
}
_INTEGRATION = re.compile(
    r"^arn:(?P<partition>aws[a-z-]*):states:::(?P<service>[A-Za-z0-9-]+):(?P<action>\S+)$"
)
#: One ARN segment; ``${AWS::Region}``-style placeholders may hold colons.
_SEGMENT = r"(?:\$\{[^{}]*\}|[^:])*"
_LAMBDA_ARN = re.compile(rf"^arn:aws[a-z-]*:lambda:{_SEGMENT}:{_SEGMENT}:function:\S+$")
_ACTIVITY_ARN = re.compile(rf"^arn:aws[a-z-]*:states:{_SEGMENT}:{_SEGMENT}:activity:\S+$")
_PLACEHOLDER = re.compile(r"\$\{([^{}]+)\}")
_WHOLE_PLACEHOLDER = re.compile(r"^\$\{([^{}]+)\}$")
_JSONATA = re.compile(r"^\s*\{%.*%\}\s*$", re.DOTALL)
_S3_URI = re.compile(r"^s3://(?P<bucket>[^/\s]+)(?:/(?P<key>\S*))?$")
#: How deep :func:`s3_references` looks into a parameters template for ``s3://`` URIs.
_MAX_PARAMETER_DEPTH = 16


@dataclass(frozen=True)
class StateInfo:
    """One state of a definition (static definition data: never runtime input)."""

    name: str
    type: str  # Task | Map | Parallel | Pass | Choice | Wait | Succeed | Fail
    resource: str | None = None  # Task "Resource"
    parameters: Mapping[str, Any] | None = None  # JSONPath "Parameters" template
    arguments: Mapping[str, Any] | None = None  # JSONata "Arguments" template
    parent_path: tuple[str, ...] = ()  # enclosing Map/Parallel state names, outermost first
    map_mode: str | None = None  # Map states: "INLINE" | "DISTRIBUTED"
    item_reader: Mapping[str, Any] | None = None
    result_writer: Mapping[str, Any] | None = None
    retry: tuple[Mapping[str, Any], ...] = field(default_factory=tuple)
    catch: tuple[Mapping[str, Any], ...] = field(default_factory=tuple)
    query_language: str = "JSONPath"


@dataclass(frozen=True)
class TaskTarget:
    """What a Task state runs."""

    kind: str  # "lambda" | "ecs" | "batch" | "glue" | "states" | "activity" | "http" | "other"
    integration: str | None = None  # "invoke" | "invoke.waitForTaskToken" | "legacy" | "sync" ...
    function: str | None = None  # Lambda: FunctionName/ARN as written (may be "${Placeholder}")
    resource: str | None = None  # the Task Resource


@dataclass(frozen=True)
class S3Reference:
    """An S3 location named by a definition."""

    state: str
    role: str  # "item_reader" | "result_writer" | "parameters"
    bucket: str | None = None
    key: str | None = None  # an object key, or a prefix for ResultWriter/ItemReader prefixes
    dynamic: bool = False  # True when bucket or key is a JSONPath/JSONata expression
    input_type: str | None = None  # ItemReader ReaderConfig.InputType ("CSV", "JSON", "JSONL", ...)

    @property
    def uri(self) -> str | None:
        """``s3://bucket/key`` when both are literal."""
        if self.dynamic or not self.bucket:
            return None
        return f"s3://{self.bucket}/{self.key or ''}"


# =========================================================================== placeholders


def placeholder_name(text: Any) -> str | None:
    """The name of a value that is exactly one ``${Name}`` placeholder (``None`` otherwise)."""
    if not isinstance(text, str):
        return None
    match = _WHOLE_PLACEHOLDER.match(text.strip())
    return match.group(1) if match else None


def placeholder_names(text: Any) -> list[str]:
    """The names of the ``${Name}`` placeholders of a string, in order (duplicates dropped)."""
    if not isinstance(text, str) or "${" not in text:
        return []
    return list(dict.fromkeys(_PLACEHOLDER.findall(text)))


def is_dynamic(value: Any) -> bool:
    """True for a value that is not literal text: a JSONata ``{% ... %}`` expression, a string with
    an unresolved ``${...}`` placeholder, or not a string at all."""
    if not isinstance(value, str):
        return True
    return bool(_JSONATA.match(value)) or "${" in value


# =========================================================================== loading


@lru_cache(maxsize=1)
def _yaml_loader() -> type:
    """A ``yaml.SafeLoader`` whose timestamps stay text (an ASL value is never a YAML date)."""
    import yaml

    class _PlainLoader(yaml.SafeLoader):
        pass

    _PlainLoader.yaml_implicit_resolvers = {
        first: [(tag, regexp) for tag, regexp in resolvers if tag != "tag:yaml.org,2002:timestamp"]
        for first, resolvers in yaml.SafeLoader.yaml_implicit_resolvers.items()
    }
    return _PlainLoader


def _quote_bare_placeholders(text: str) -> str:
    """Wrap ``${Name}`` tokens written outside JSON strings in double quotes."""
    out: list[str] = []
    index, length, in_string = 0, len(text), False
    while index < length:
        char = text[index]
        if in_string:
            out.append(char)
            if char == "\\" and index + 1 < length:
                out.append(text[index + 1])
                index += 2
                continue
            in_string = char != '"'
            index += 1
            continue
        if char == '"':
            in_string = True
        elif char == "$" and text.startswith("${", index):
            end = text.find("}", index + 2)
            if end != -1:
                out.append(f'"{text[index : end + 1]}"')
                index = end + 1
                continue
        out.append(char)
        index += 1
    return "".join(out)


def _parse_text(text: str) -> Any:
    text = text.lstrip("\ufeff")
    try:
        return json.loads(text)
    except ValueError:
        pass
    if "${" in text:
        try:
            return json.loads(_quote_bare_placeholders(text))
        except ValueError:
            pass
    import yaml

    try:
        return yaml.load(text, Loader=_yaml_loader())  # a SafeLoader subclass
    except yaml.YAMLError:
        raise ValueError("not a state machine definition: neither JSON nor YAML") from None


def _plain(value: Any) -> Any:
    """Mappings as dicts and sequences as lists, recursively (a private copy of the input)."""
    if isinstance(value, Mapping):
        return {str(key): _plain(item) for key, item in value.items()}
    if isinstance(value, (list, tuple)):
        return [_plain(item) for item in value]
    return value


def load_definition(definition: str | bytes | Mapping[str, Any]) -> dict[str, Any]:
    """A definition as a dict; :class:`ValueError` when it is not a state machine definition."""
    try:
        if isinstance(definition, Mapping):
            data = _plain(definition)
        elif isinstance(definition, (bytes, bytearray)):
            try:
                text = bytes(definition).decode("utf-8-sig")
            except UnicodeDecodeError:
                raise ValueError("not a state machine definition: not UTF-8 text") from None
            data = _parse_text(text)
        elif isinstance(definition, str):
            data = _parse_text(definition)
        else:
            raise ValueError(f"not a state machine definition: {type(definition).__name__}")
    except RecursionError:
        raise ValueError("not a state machine definition: nested too deeply") from None
    if not isinstance(data, dict):
        raise ValueError("not a state machine definition: expected an object")
    if not isinstance(data.get("States"), Mapping):
        raise ValueError("not a state machine definition: no States object")
    return data


# =========================================================================== states


def _text(value: Any) -> str | None:
    return value.strip() if isinstance(value, str) and value.strip() else None


def _mapping(value: Any) -> Mapping[str, Any] | None:
    return value if isinstance(value, Mapping) else None


def _mappings(value: Any) -> tuple[Mapping[str, Any], ...]:
    if not isinstance(value, (list, tuple)):
        return ()
    return tuple(item for item in value if isinstance(item, Mapping))


def _query_language(value: Any, default: str) -> str:
    text = _text(value)
    if text is None:
        return default
    return "JSONata" if text.lower() == "jsonata" else "JSONPath" if text.lower() == "jsonpath" else text


def _processor(body: Mapping[str, Any]) -> Mapping[str, Any] | None:
    """The sub-machine of a Map state (``ItemProcessor``, or the legacy ``Iterator``)."""
    return _mapping(body.get("ItemProcessor")) or _mapping(body.get("Iterator"))


def _map_mode(body: Mapping[str, Any]) -> str:
    processor = _processor(body) or {}
    config = _mapping(processor.get("ProcessorConfig")) or {}
    mode = _text(config.get("Mode"))
    return mode.upper() if mode else "INLINE"


def _sub_machines(body: Mapping[str, Any]) -> list[Mapping[str, Any]]:
    kind = body.get("Type")
    if kind == "Parallel":
        return list(_mappings(body.get("Branches")))
    if kind == "Map":
        return [
            machine
            for machine in (body.get("ItemProcessor"), body.get("Iterator"))
            if isinstance(machine, Mapping)
        ]
    return []


def _state_info(name: str, body: Mapping[str, Any], parents: tuple[str, ...], language: str) -> StateInfo:
    kind = _text(body.get("Type")) or ""
    is_map = kind == "Map"
    return StateInfo(
        name=name,
        type=kind,
        resource=_text(body.get("Resource")) if kind == "Task" else None,
        parameters=_mapping(body.get("Parameters")),
        arguments=_mapping(body.get("Arguments")),
        parent_path=parents,
        map_mode=_map_mode(body) if is_map else None,
        item_reader=_mapping(body.get("ItemReader")) if is_map else None,
        result_writer=_mapping(body.get("ResultWriter")) if is_map else None,
        retry=_mappings(body.get("Retry")),
        catch=_mappings(body.get("Catch")),
        query_language=_query_language(body.get("QueryLanguage"), language),
    )


def _index_machine(
    machine: Mapping[str, Any],
    parents: tuple[str, ...],
    language: str,
    index: dict[str, StateInfo],
    depth: int,
) -> None:
    states = machine.get("States")
    if not isinstance(states, Mapping) or depth > MAX_NESTING:
        return
    language = _query_language(machine.get("QueryLanguage"), language)
    for name, body in states.items():
        if not isinstance(name, str) or not isinstance(body, Mapping) or name in index:
            continue
        info = _state_info(name, body, parents, language)
        index[name] = info
        for child in _sub_machines(body):
            _index_machine(child, (*parents, name), info.query_language, index, depth + 1)


def index_states(definition: str | bytes | Mapping[str, Any]) -> dict[str, StateInfo]:
    """Every state by name (nested ones included, with their ``parent_path``).

    The order is the definition's (depth first: a Map or Parallel state is followed by its inner
    states). Raises :class:`ValueError` when ``definition`` is not a state machine definition.
    """
    machine = load_definition(definition)
    index: dict[str, StateInfo] = {}
    _index_machine(machine, (), DEFAULT_QUERY_LANGUAGE, index, 0)
    return index


# =========================================================================== Task targets


def _function_name(state: StateInfo) -> str | None:
    """``FunctionName`` of a Lambda Task as written; ``None`` when missing or dynamic."""
    templates = (
        (state.arguments, state.parameters)
        if state.query_language == "JSONata"
        else (state.parameters, state.arguments)
    )
    for template in templates:
        if not isinstance(template, Mapping):
            continue
        if "FunctionName" in template:
            value = template["FunctionName"]
            if isinstance(value, str) and value.strip() and not _JSONATA.match(value):
                return value.strip()
            return None
        if "FunctionName.$" in template:
            return None
    return None


def compute_target(state: StateInfo) -> TaskTarget | None:
    """The target of a Task state (``None`` for other state types)."""
    if state.type != "Task":
        return None
    resource = _text(state.resource)
    if resource is None:
        return TaskTarget(kind="other")
    if placeholder_name(resource) is not None:
        return TaskTarget(kind="lambda", integration="legacy", function=resource, resource=resource)
    integration = _INTEGRATION.match(resource)
    if integration is not None:
        service, action = integration.group("service"), integration.group("action")
        if service == "aws-sdk":
            sdk_service, _, sdk_action = action.partition(":")
            if sdk_service == "lambda" and sdk_action.startswith("invoke"):
                return TaskTarget("lambda", f"aws-sdk:{sdk_action}", _function_name(state), resource)
            return TaskTarget("other", f"aws-sdk:{action}", None, resource)
        kind = _SERVICE_KINDS.get(service, "other")
        function = _function_name(state) if kind == "lambda" else None
        return TaskTarget(kind, action, function, resource)
    if _LAMBDA_ARN.match(resource):
        return TaskTarget("lambda", "legacy", resource, resource)
    if _ACTIVITY_ARN.match(resource):
        return TaskTarget("activity", None, None, resource)
    return TaskTarget("other", None, None, resource)


# =========================================================================== S3 references


def _template_value(template: Mapping[str, Any], name: str) -> tuple[bool, Any]:
    """``(present, value)`` of ``name`` in a parameters template (``name.$`` is a JSONPath)."""
    if name in template:
        return True, template[name]
    if f"{name}.$" in template:
        return True, None
    return False, None


def _bucket_key(
    state: str,
    role: str,
    template: Mapping[str, Any] | None,
    key_names: tuple[str, ...],
    input_type: str | None,
) -> S3Reference | None:
    if not isinstance(template, Mapping):
        return None
    has_bucket, bucket = _template_value(template, "Bucket")
    if not has_bucket:
        return None
    key: Any = None
    has_key = False
    for name in key_names:
        has_key, key = _template_value(template, name)
        if has_key:
            break
    dynamic = is_dynamic(bucket) or (has_key and is_dynamic(key))
    return S3Reference(
        state=state,
        role=role,
        bucket=bucket.strip() if isinstance(bucket, str) and not is_dynamic(bucket) else None,
        key=key.strip() if isinstance(key, str) and not is_dynamic(key) else None,
        dynamic=dynamic,
        input_type=input_type,
    )


def _templates(holder: Mapping[str, Any]) -> list[Mapping[str, Any]]:
    return [
        template
        for template in (holder.get("Parameters"), holder.get("Arguments"))
        if isinstance(template, Mapping)
    ]


def _s3_uris(value: Any, depth: int = 0) -> list[str]:
    """Literal ``s3://`` URI strings anywhere in a parameters template (keys ending in ``.$`` are
    JSONPath expressions and skipped)."""
    if depth > _MAX_PARAMETER_DEPTH:
        return []
    found: list[str] = []
    if isinstance(value, Mapping):
        for key, item in value.items():
            if isinstance(key, str) and key.endswith(".$"):
                continue
            found.extend(_s3_uris(item, depth + 1))
    elif isinstance(value, (list, tuple)):
        for item in value:
            found.extend(_s3_uris(item, depth + 1))
    elif isinstance(value, str) and value.startswith("s3://") and _S3_URI.match(value.strip()):
        found.append(value.strip())
    return found


def _uri_reference(state: str, uri: str) -> S3Reference:
    match = _S3_URI.match(uri)
    bucket = match.group("bucket") if match else None
    key = match.group("key") if match else None
    return S3Reference(state=state, role="parameters", bucket=bucket, key=key or None, dynamic="${" in uri)


def _reader_input_type(reader: Mapping[str, Any]) -> str | None:
    config = _mapping(reader.get("ReaderConfig")) or {}
    value = _text(config.get("InputType"))
    return value.upper() if value else None


def s3_references(states: Mapping[str, StateInfo]) -> list[S3Reference]:
    """The S3 locations of ``ItemReader``/``ResultWriter`` and literal ``Bucket``/``Key`` parameters.

    ``ItemReader`` gives ``Key`` (``s3:getObject``) or ``Prefix`` (``s3:listObjectsV2``) and the
    ``ReaderConfig.InputType``; ``ResultWriter`` gives ``Prefix``. ``Parameters``/``Arguments`` of
    any state give a ``Bucket`` with its ``Key``/``Prefix`` and every literal ``s3://`` URI they
    hold (role ``parameters``). JSONPath (``Bucket.$``) and JSONata (``{% ... %}``) values, and
    unresolved ``${...}`` placeholders, make a reference ``dynamic``. Duplicates are dropped.
    """
    found: dict[tuple[Any, ...], S3Reference] = {}

    def add(reference: S3Reference | None) -> None:
        if reference is not None:
            found.setdefault(
                (reference.state, reference.role, reference.bucket, reference.key, reference.dynamic),
                reference,
            )

    for name, state in states.items():
        if state.item_reader:
            input_type = _reader_input_type(state.item_reader)
            for template in _templates(state.item_reader):
                add(_bucket_key(name, "item_reader", template, ("Key", "Prefix"), input_type))
        if state.result_writer:
            for template in _templates(state.result_writer):
                add(_bucket_key(name, "result_writer", template, ("Prefix", "Key"), None))
        for template in (state.parameters, state.arguments):
            if not isinstance(template, Mapping):
                continue
            add(_bucket_key(name, "parameters", template, ("Key", "Prefix"), None))
            for uri in _s3_uris(template):
                add(_uri_reference(name, uri))
    return list(found.values())


# =========================================================================== substitutions


def _substitute(value: Any, literal: Mapping[str, str]) -> Any:
    if isinstance(value, str):
        if "${" not in value:
            return value
        return _PLACEHOLDER.sub(lambda match: literal.get(match.group(1), match.group(0)), value)
    if isinstance(value, Mapping):
        return {
            (_substitute(key, literal) if isinstance(key, str) else key): _substitute(item, literal)
            for key, item in value.items()
        }
    if isinstance(value, list):
        return [_substitute(item, literal) for item in value]
    if isinstance(value, tuple):
        return tuple(_substitute(item, literal) for item in value)
    return value


def substitute(value: Any, substitutions: Mapping[str, Any]) -> Any:
    """Replace ``${Name}`` placeholders in every string of ``value`` (recursively); a placeholder
    without a (string) substitution is left as is."""
    literal = {str(name): text for name, text in (substitutions or {}).items() if isinstance(text, str)}
    return _substitute(value, literal)


__all__ = [
    "DEFAULT_QUERY_LANGUAGE",
    "LAMBDA_INVOKE_PREFIX",
    "MAX_NESTING",
    "STATE_TYPES",
    "S3Reference",
    "StateInfo",
    "TaskTarget",
    "compute_target",
    "index_states",
    "is_dynamic",
    "load_definition",
    "placeholder_name",
    "placeholder_names",
    "s3_references",
    "substitute",
]
