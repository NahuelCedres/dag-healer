"""Adapter factories: the ``dag_healer.orchestrators`` entry-point group and the adapter cache.

``healer.yaml`` names its orchestrator with ``orchestrator.type`` (section 5.1 of
``docs/architecture/orchestrators.md``): either an entry-point name of the
:data:`ENTRY_POINT_GROUP` group (``airflow``, ``stepfunctions``, ``fake`` or the name a
third-party distribution registers) or an explicit ``"package.module:factory"`` string.

* :func:`registrations` / :func:`available_types` list the registered types. The built-in
  types are also known from :data:`BUILTIN_TYPES`, so a source checkout whose package
  metadata is missing or stale (an editable install made before an entry point was added)
  still resolves them. The same name registered by two distributions is an error naming both:
  a third-party distribution can never silently shadow a built-in adapter.
* :func:`load_factory` imports the :class:`AdapterFactory` of a type. A missing adapter module,
  a missing optional dependency (``pip install 'dag-healer[aws]'``) or an object that is not a
  factory gives a clear :class:`AdapterLoadError`.
* :func:`get_adapter` builds the adapter of one configuration and caches it per (configuration
  file, its mtime, the orchestrator type and instance, the configuration content), so the API
  process can ask for it on every request. :func:`reset_adapters` empties the caches (tests).
* :func:`orchestrator_settings` reads the ``orchestrator:`` section. A configuration without
  it means ``{type: airflow, instance: airflow, options: {}}`` (every phase-5 ``healer.yaml``).
* :func:`parse_options` validates ``orchestrator.options`` against the factory's
  ``options_model`` (pydantic, imported only when a model is used).

Importing this module loads only the standard library and :mod:`dag_healer.core`; adapter
modules are imported when their type is first loaded.
"""

from __future__ import annotations

import hashlib
import importlib
import importlib.util
import logging
import re
import threading
from collections import OrderedDict
from collections.abc import Hashable, Iterable, Mapping
from dataclasses import dataclass, field
from pathlib import Path
from typing import TYPE_CHECKING, Any, Protocol, runtime_checkable

from dag_healer.core.ids import DEFAULT_ORCHESTRATOR
from dag_healer.core.ports import OrchestratorAdapter

if TYPE_CHECKING:
    import pydantic

    from dag_healer.config import HealerConfig

logger = logging.getLogger(__name__)

ENTRY_POINT_GROUP = "dag_healer.orchestrators"
"""Entry-point group in which orchestrator adapter factories are registered."""
DEFAULT_TYPE = DEFAULT_ORCHESTRATOR
"""Orchestrator type (and instance) of a configuration without an ``orchestrator:`` section."""
DISTRIBUTION = "dag-healer"
"""Name of the distribution that ships the built-in adapters."""

#: The built-in adapters (the same values as the entry points of ``pyproject.toml``).
BUILTIN_TYPES: dict[str, str] = {
    "airflow": "dag_healer.airflow.adapter:FACTORY",
    "fake": "dag_healer.orchestrators.fake:FACTORY",
    "stepfunctions": "dag_healer.stepfunctions:FACTORY",
}
#: The extra a built-in adapter needs (``pip install 'dag-healer[<extra>]'``).
BUILTIN_EXTRAS: dict[str, str] = {"stepfunctions": "aws"}
#: Top-level modules each extra provides (checked before a built-in adapter is imported).
EXTRA_MODULES: dict[str, tuple[str, ...]] = {"aws": ("boto3", "botocore")}

_TARGET = re.compile(r"^[A-Za-z_]\w*(?:\.[A-Za-z_]\w*)*:[A-Za-z_]\w*(?:\.[A-Za-z_]\w*)*$")
_FACTORY_MEMBERS: tuple[str, ...] = ("type", "display_name", "options_model", "secret_options", "create")
_MAX_CACHED_ADAPTERS = 16

_lock = threading.RLock()
_registrations: tuple[Registration, ...] | None = None
_factories: dict[str, AdapterFactory] = {}
_adapters: OrderedDict[tuple[Hashable, ...], OrchestratorAdapter] = OrderedDict()


class AdapterLoadError(RuntimeError):
    """An orchestrator type cannot be resolved, imported or used (an installation/config error)."""


class AdapterOptionsError(AdapterLoadError, ValueError):
    """``orchestrator.options`` is not valid for the configured orchestrator type."""


@runtime_checkable
class AdapterFactory(Protocol):
    """Builds the adapter of one orchestrator type (the object an entry point points at)."""

    type: str
    display_name: str
    options_model: type[pydantic.BaseModel] | None  # validates healer.yaml orchestrator.options
    secret_options: tuple[str, ...]  # dotted names under orchestrator.options (doctor)

    def create(self, cfg: HealerConfig, *, client: Any | None = None) -> OrchestratorAdapter:
        """MUST NOT perform I/O. ``client`` lets tests and legacy callers inject a transport."""
        ...


@dataclass(frozen=True)
class Registration:
    """One registered orchestrator type."""

    type: str
    target: str  # "package.module:attr"
    distribution: str  # the distribution that registered it ("dag-healer" for the built-ins)
    builtin: bool = False  # True: from BUILTIN_TYPES (dag-healer's own metadata is not installed)


@dataclass(frozen=True)
class OrchestratorSettings:
    """The ``orchestrator:`` section of ``healer.yaml`` as the registry reads it."""

    type: str = DEFAULT_TYPE
    instance: str = DEFAULT_TYPE
    options: Mapping[str, Any] = field(default_factory=dict)


# =========================================================================== settings


def orchestrator_settings(cfg: Any) -> OrchestratorSettings:
    """Read ``cfg.orchestrator`` (``type``, ``instance``, ``options``); defaults when absent.

    ``instance`` defaults to ``type`` (the rule of ``OrchestratorConfig.instance_id``, which is
    used when the section provides it). A section may also be a plain mapping.
    """
    section = getattr(cfg, "orchestrator", None)
    if section is None:
        return OrchestratorSettings()

    def read(name: str) -> Any:
        if isinstance(section, Mapping):
            return section.get(name)
        return getattr(section, name, None)

    type_ = str(read("type") or "").strip() or DEFAULT_TYPE
    instance = str(read("instance_id") or read("instance") or "").strip() or type_
    options = read("options")
    if options is None:
        options = {}
    if not isinstance(options, Mapping):
        raise AdapterOptionsError(f"orchestrator.options must be a mapping, not {type(options).__name__}")
    return OrchestratorSettings(type=type_, instance=instance, options=dict(options))


def parse_options(factory: AdapterFactory, options: Mapping[str, Any] | None) -> Any:
    """Validate ``orchestrator.options`` with ``factory.options_model``.

    Returns the model instance, or ``None`` for a factory without options (then any option is
    an error: the Airflow adapter keeps its settings in the top-level ``airflow:`` section).
    Raises :class:`AdapterOptionsError` with one line per problem.
    """
    data = dict(options or {})
    model = factory.options_model
    if model is None:
        if data:
            names = ", ".join(sorted(str(name) for name in data))
            raise AdapterOptionsError(
                f"orchestrator type {factory.type!r} takes no orchestrator.options (got: {names})"
            )
        return None
    from pydantic import ValidationError

    try:
        return model.model_validate(data)
    except ValidationError as exc:
        lines = []
        for problem in exc.errors():
            location = ".".join(str(part) for part in problem.get("loc", ())) or "<root>"
            lines.append(f"  - orchestrator.options.{location}: {problem.get('msg', 'invalid value')}")
        raise AdapterOptionsError(
            f"Invalid orchestrator.options for orchestrator type {factory.type!r}:\n" + "\n".join(lines)
        ) from None


def secret_option_paths(factory: AdapterFactory) -> tuple[str, ...]:
    """Dotted config paths of the factory's secret options (``orchestrator.options.<name>``)."""
    return tuple(f"orchestrator.options.{name}" for name in factory.secret_options)


# =========================================================================== registrations


def _normalize(distribution: str) -> str:
    return re.sub(r"[-_.]+", "-", distribution).lower()


def _distribution_name(entry_point: Any) -> str:
    dist = getattr(entry_point, "dist", None)
    if dist is None:
        return "unknown distribution"
    name = getattr(dist, "name", None)
    if not name:
        try:
            name = dist.metadata["Name"]
        except Exception:  # broken metadata of a third-party distribution
            name = None
    return str(name or "unknown distribution")


def _entry_point_registrations() -> list[Registration]:
    """Registrations found in the installed package metadata."""
    from importlib import metadata

    try:
        entry_points = metadata.entry_points(group=ENTRY_POINT_GROUP)
    except Exception as exc:  # a distribution with unreadable metadata must not break dag-healer
        logger.warning("dag-healer: cannot read the %s entry points: %s", ENTRY_POINT_GROUP, exc)
        return []
    return [
        Registration(type=ep.name, target=str(ep.value).strip(), distribution=_distribution_name(ep))
        for ep in entry_points
    ]


def registrations() -> list[Registration]:
    """Every registered orchestrator type, sorted by type (conflicting entries included).

    The installed entry points come first; each built-in type that dag-healer's own metadata
    does not register (a stale editable install, a source checkout) is added from
    :data:`BUILTIN_TYPES`. The result is cached until :func:`reset_adapters`.
    """
    global _registrations
    with _lock:
        if _registrations is not None:
            return list(_registrations)
    found = _entry_point_registrations()
    own = {item.type for item in found if _normalize(item.distribution) == _normalize(DISTRIBUTION)}
    for type_, target in BUILTIN_TYPES.items():
        if type_ not in own:
            found.append(Registration(type=type_, target=target, distribution=DISTRIBUTION, builtin=True))
    unique = sorted(
        {(item.type, _normalize(item.distribution), item.target): item for item in found}.values(),
        key=lambda item: (item.type, _normalize(item.distribution), item.target),
    )
    with _lock:
        _registrations = tuple(unique)
    return list(unique)


def conflicts(items: Iterable[Registration] | None = None) -> dict[str, list[Registration]]:
    """Types registered more than once (by several distributions, or with several targets)."""
    grouped: dict[str, list[Registration]] = {}
    for item in registrations() if items is None else items:
        grouped.setdefault(item.type, []).append(item)
    return {type_: group for type_, group in grouped.items() if len(group) > 1}


def conflict_message(type_: str, group: Iterable[Registration]) -> str:
    """Human explanation of one conflict (names every distribution involved)."""
    owners = " and ".join(f"{item.distribution} ({item.target})" for item in group)
    return (
        f"orchestrator type {type_!r} is registered more than once in the {ENTRY_POINT_GROUP!r} "
        f"entry-point group: {owners}. Uninstall one of them, or set orchestrator.type to an "
        "explicit 'package.module:factory' string."
    )


def available_types() -> dict[str, str]:
    """``{type: "module:attr"}`` of every registered orchestrator type.

    The same name registered by two distributions is an error naming both (no silent shadowing
    of built-ins): :class:`AdapterLoadError`.
    """
    items = registrations()
    duplicated = conflicts(items)
    if duplicated:
        raise AdapterLoadError(
            "\n".join(conflict_message(type_, group) for type_, group in sorted(duplicated.items()))
        )
    return {item.type: item.target for item in items}


# =========================================================================== factories


def is_target(type_: str) -> bool:
    """True for an explicit ``"package.module:factory"`` string (rather than an entry-point name)."""
    return ":" in type_


def _hint_for_missing(top_module: str, extra: str | None) -> str:
    if extra is None:
        extra = next((name for name, modules in EXTRA_MODULES.items() if top_module in modules), None)
    if extra is not None:
        return f"pip install 'dag-healer[{extra}]'"
    return f"install the distribution that provides the {top_module!r} module"


def _check_extra(label: str, extra: str) -> None:
    """Fail early, with the pip command, when a built-in adapter's optional dependency is missing."""
    for module in EXTRA_MODULES.get(extra, ()):
        try:
            found = importlib.util.find_spec(module) is not None
        except (ImportError, ValueError):
            found = False
        if not found:
            raise AdapterLoadError(
                f"orchestrator type {label!r} needs the {extra!r} extra (module {module!r} is not "
                f"installed): pip install 'dag-healer[{extra}]'"
            )


def _module_exists(module_name: str) -> bool:
    try:
        return importlib.util.find_spec(module_name) is not None
    except ModuleNotFoundError:  # a parent package is missing
        return False
    except (ImportError, ValueError):  # the parent package exists but cannot be imported: let it fail later
        return True


def _import_factory(target: str, *, label: str, extra: str | None, builtin: bool) -> AdapterFactory:
    module_name, _, attr_path = target.partition(":")
    if not _module_exists(module_name):
        where = " (this dag-healer version does not include it)" if builtin else ""
        raise AdapterLoadError(f"orchestrator type {label!r}: module {module_name!r} is not installed{where}")
    if extra is not None:
        _check_extra(label, extra)
    try:
        module = importlib.import_module(module_name)
    except ModuleNotFoundError as exc:
        missing = exc.name or ""
        if missing and (module_name == missing or module_name.startswith(missing + ".")):
            raise AdapterLoadError(
                f"orchestrator type {label!r}: module {module_name!r} is not installed"
            ) from exc
        top = missing.split(".")[0] or "?"
        raise AdapterLoadError(
            f"orchestrator type {label!r} needs the Python module {missing!r}, which is not installed: "
            f"{_hint_for_missing(top, extra)}"
        ) from exc
    except Exception as exc:  # SyntaxError, ImportError, errors raised while the module runs
        raise AdapterLoadError(
            f"orchestrator type {label!r}: importing {module_name!r} failed: {type(exc).__name__}: {exc}"
        ) from exc

    obj: Any = module
    for part in attr_path.split("."):
        try:
            obj = getattr(obj, part)
        except AttributeError:
            raise AdapterLoadError(
                f"orchestrator type {label!r}: module {module_name!r} has no attribute {attr_path!r}"
            ) from None
    if isinstance(obj, type):
        try:
            obj = obj()
        except Exception as exc:
            raise AdapterLoadError(
                f"orchestrator type {label!r}: {target!r} is a class that cannot be instantiated "
                f"without arguments: {type(exc).__name__}: {exc}"
            ) from exc
    if not isinstance(obj, AdapterFactory):
        missing_members = [name for name in _FACTORY_MEMBERS if not hasattr(obj, name)]
        raise AdapterLoadError(
            f"orchestrator type {label!r}: {target!r} is not an adapter factory "
            f"(missing: {', '.join(missing_members) or 'callable create'})"
        )
    return obj


def load_factory(type_: str) -> AdapterFactory:
    """The factory of an entry-point name or of an explicit ``"pkg.mod:factory"`` string.

    Cached per name until :func:`reset_adapters`. Raises :class:`AdapterLoadError` for an
    unknown or conflicting name, a missing module, a missing extra (with the pip command) or an
    object that is not an :class:`AdapterFactory`.
    """
    name = str(type_ or "").strip()
    if not name:
        raise AdapterLoadError("orchestrator.type is empty")
    with _lock:
        cached = _factories.get(name)
    if cached is not None:
        return cached

    if is_target(name):
        if not _TARGET.match(name):
            raise AdapterLoadError(
                f"invalid orchestrator type {name!r}: expected an entry-point name of the "
                f"{ENTRY_POINT_GROUP!r} group or 'package.module:factory'"
            )
        factory = _import_factory(name, label=name, extra=None, builtin=False)
    else:
        matches = [item for item in registrations() if item.type == name]
        if not matches:
            known = ", ".join(sorted({item.type for item in registrations()})) or "none"
            raise AdapterLoadError(
                f"unknown orchestrator type {name!r} (registered types: {known}); use an entry-point "
                f"name of the {ENTRY_POINT_GROUP!r} group or 'package.module:factory'"
            )
        if len(matches) > 1:
            raise AdapterLoadError(conflict_message(name, matches))
        registration = matches[0]
        own = _normalize(registration.distribution) == _normalize(DISTRIBUTION)
        factory = _import_factory(
            registration.target,
            label=name,
            extra=BUILTIN_EXTRAS.get(name) if own else None,
            builtin=own,
        )
        if factory.type != name:
            logger.warning(
                "dag-healer: the factory registered as orchestrator type %r reports type %r",
                name,
                factory.type,
            )
    with _lock:
        return _factories.setdefault(name, factory)


# =========================================================================== adapters


def ensure_adapter(adapter: Any, label: str) -> OrchestratorAdapter:
    """``adapter`` when it is an :class:`~dag_healer.core.ports.OrchestratorAdapter`, else
    :class:`AdapterLoadError` naming the orchestrator type (``label``) whose factory built it."""
    if not isinstance(adapter, OrchestratorAdapter):
        raise AdapterLoadError(
            f"orchestrator type {label!r}: the factory returned {type(adapter).__name__}, "
            "which is not an OrchestratorAdapter"
        )
    return adapter


def _fingerprint(cfg: Any) -> str:
    """Content digest of a configuration (two configurations with equal content share an adapter)."""
    dump = getattr(cfg, "model_dump_json", None)
    if callable(dump):
        try:
            return hashlib.sha256(str(dump()).encode("utf-8")).hexdigest()
        except Exception:  # an exotic value that cannot be serialised: fall back to the identity
            pass
    return f"id:{id(cfg)}"


def _cache_key(cfg: Any, settings: OrchestratorSettings) -> tuple[Hashable, ...]:
    source = getattr(cfg, "source_path", None)
    mtime: int | None = None
    if source is not None:
        try:
            mtime = Path(source).stat().st_mtime_ns
        except OSError:
            mtime = None
    return (
        str(source) if source is not None else None,
        mtime,
        settings.type,
        settings.instance,
        _fingerprint(cfg),
    )


def get_adapter(cfg: HealerConfig, *, client: Any | None = None) -> OrchestratorAdapter:
    """The adapter of ``cfg``'s orchestrator. Cached per (cfg source path, mtime, instance).

    The key also holds the orchestrator type and a digest of the configuration content, so two
    in-memory configurations without a file never share an adapter by accident. With ``client``
    (a transport injected by a test or a legacy caller) a new, uncached adapter is returned.
    Construction does no I/O.
    """
    settings = orchestrator_settings(cfg)
    factory = load_factory(settings.type)
    if client is not None:
        return ensure_adapter(factory.create(cfg, client=client), settings.type)
    key = _cache_key(cfg, settings)
    with _lock:
        adapter = _adapters.get(key)
        if adapter is not None:
            _adapters.move_to_end(key)
            return adapter
        adapter = ensure_adapter(factory.create(cfg), settings.type)
        _adapters[key] = adapter
        while len(_adapters) > _MAX_CACHED_ADAPTERS:
            _adapters.popitem(last=False)
    logger.debug("dag-healer: created the %s adapter for instance %s", settings.type, settings.instance)
    return adapter


def reset_adapters() -> None:
    """Forget the cached registrations, factories and adapters (closing the adapters). Tests."""
    global _registrations
    with _lock:
        adapters = list(_adapters.values())
        _adapters.clear()
        _factories.clear()
        _registrations = None
    for adapter in adapters:
        try:
            adapter.close()
        except Exception as exc:  # closing is best effort
            logger.debug("dag-healer: closing a cached adapter failed: %s", exc)


__all__ = [
    "BUILTIN_EXTRAS",
    "BUILTIN_TYPES",
    "DEFAULT_TYPE",
    "DISTRIBUTION",
    "ENTRY_POINT_GROUP",
    "EXTRA_MODULES",
    "AdapterFactory",
    "AdapterLoadError",
    "AdapterOptionsError",
    "OrchestratorSettings",
    "Registration",
    "available_types",
    "conflict_message",
    "conflicts",
    "ensure_adapter",
    "get_adapter",
    "is_target",
    "load_factory",
    "orchestrator_settings",
    "parse_options",
    "registrations",
    "reset_adapters",
    "secret_option_paths",
]
