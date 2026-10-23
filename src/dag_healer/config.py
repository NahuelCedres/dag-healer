"""dag-healer configuration: the ``healer.yaml`` model, ``${ENV}`` interpolation and loading.

Resolution order of :func:`load_config` without an explicit path:

1. ``$HEALER_CONFIG`` (a file, or a directory containing ``healer.yaml``);
2. ``$AIRFLOW_HOME/healer.yaml``;
3. ``./healer.yaml``.

When nothing is found the defaults are returned, with ``repo.path`` set to the
directory ``$HEALER_CONFIG`` points into (if set) or to the current directory.

String values support ``${VAR}`` and ``${VAR:-default}`` (``$${`` writes a
literal ``${``). A value that is exactly ``${VAR}`` with ``VAR`` unset or empty
becomes ``None`` for optional fields, the field default for fields that have
one, and an empty string otherwise; inside lists such items are dropped. In the
free-form ``orchestrator.options`` such an option is left out, so the adapter's
own default applies.

``version: 1`` stays: the orchestrator abstraction (section 5 of
``docs/architecture/orchestrators.md``) only adds sections. ``orchestrator:`` selects
the adapter (absent: Airflow, configured by the ``airflow:`` section as before) and
``serve:`` configures the standalone host ``dag-healer serve``. Both are left out of
dumps while they hold their defaults, so ``dag-healer check`` prints an existing
``healer.yaml`` exactly as before.
"""

from __future__ import annotations

import fnmatch
import ipaddress
import json
import logging
import os
import re
import threading
import types
from collections.abc import Iterable, Mapping
from pathlib import Path
from typing import Any, Literal, Union, get_args, get_origin
from urllib.parse import urlsplit

import yaml
from pydantic import (
    AliasChoices,
    BaseModel,
    ConfigDict,
    Field,
    PrivateAttr,
    SerializerFunctionWrapHandler,
    ValidationError,
    ValidationInfo,
    field_validator,
    model_serializer,
    model_validator,
)
from pydantic.fields import FieldInfo

from dag_healer.contracts.schema import DatasetLocator
from dag_healer.core.ids import DEFAULT_ORCHESTRATOR

logger = logging.getLogger(__name__)

CONFIG_ENV = "HEALER_CONFIG"
CONFIG_FILENAME = "healer.yaml"
HEALER_HOME_ENV = "HEALER_HOME"
DATABASE_URL_ENV = "HEALER_DATABASE_URL"
SUPPORTED_VERSION = 1

#: Dotted paths of fields that hold secrets (they should be ``${ENV}`` references). Loading a
#: configuration adds ``orchestrator.options.<name>`` for each secret option its orchestrator
#: adapter declares (``factory.secret_options``); see :attr:`HealerConfig.secret_fields`.
SECRET_FIELDS: tuple[str, ...] = (
    "airflow.password",
    "notify.email.password",
    "github.token",
    "ingest.token",
)
ALERT_EVENTS: tuple[str, ...] = ("waiting_human", "reported", "repaired", "repair_failed")

#: ``orchestrator.instance``, and the type when it serves as the instance id: lowercase letters,
#: digits, ``.``, ``_`` and ``-``, at most 64 characters (the store's ``orchestrator`` column).
INSTANCE_PATTERN = r"^[a-z0-9][a-z0-9._-]{0,63}$"
#: ``monitor.modes`` values (kept verbatim): Airflow's ``rest``/``listener``/``callback`` and the
#: neutral ``poll``/``push``/``queue`` of the other orchestrators.
MONITOR_MODES: tuple[str, ...] = ("rest", "listener", "callback", "poll", "push", "queue")
#: What each mode turns on (:meth:`MonitorConfig.wants`): ``poll`` (list failures through the
#: adapter's feed), ``push`` (confirm pushed failure hints) or ``queue`` (drain the hint queue).
MODE_KINDS: Mapping[str, str] = types.MappingProxyType(
    {"rest": "poll", "poll": "poll", "listener": "push", "callback": "push", "push": "push", "queue": "queue"}
)
#: The orchestrators :func:`default_config_yaml` has a ``healer.yaml`` template for.
TEMPLATE_ORCHESTRATORS: tuple[str, ...] = ("airflow", "stepfunctions")

MonitorMode = Literal["rest", "listener", "callback", "poll", "push", "queue"]
RbacAction = Literal["read", "act", "approve_contract"]
ServeAuthBackend = Literal["dev", "token", "trusted_header", "alb_oidc"]

_BASE_SECRET_FIELDS: tuple[str, ...] = SECRET_FIELDS
_secret_fields_lock = threading.Lock()

_ENV_PATTERN = re.compile(r"\$\$\{|\$\{(?P<name>[A-Za-z_][A-Za-z0-9_]*)(?::-(?P<default>[^}]*))?\}")
_WHOLE_REFERENCE = re.compile(r"^\$\{(?P<name>[A-Za-z_][A-Za-z0-9_]*)\}$")
_GITHUB_REPO = re.compile(r"^[A-Za-z0-9_.-]+/[A-Za-z0-9_.-]+$")
_HOOK = re.compile(r"^[A-Za-z_][\w.]*:[A-Za-z_]\w*$")
_INSTANCE = re.compile(INSTANCE_PATTERN)
_FACTORY_TARGET = re.compile(r"^[A-Za-z_]\w*(?:\.[A-Za-z_]\w*)*:[A-Za-z_]\w*(?:\.[A-Za-z_]\w*)*$")
_ENTRY_POINT_NAME = re.compile(r"^[^\s:=\[][^\s:=]*$")
_PRINCIPAL = re.compile(r"^(?:\*|authenticated|(?:user|group):\S(?:.*\S)?)$")
_SHA256_HEX = re.compile(r"^[0-9a-fA-F]{64}$")
_HEADER_NAME = re.compile(r"^[!#$%&'*+.^_`|~0-9A-Za-z-]+$")
_REGION = r"[a-z]{2}(?:-[a-z]+)+-\d{1,2}"
_AWS_REGION = re.compile(rf"^{_REGION}$")
_ALB_ARN = re.compile(
    rf"^arn:aws[a-z-]*:elasticloadbalancing:(?P<region>{_REGION}):\d{{12}}:loadbalancer/app/[^/\s]+/[^/\s]+$"
)
_TOKEN_KEYS = frozenset({"name", "sha256", "groups"})
_warned_missing: set[str] = set()


class ConfigError(ValueError):
    """The configuration file is missing, unreadable or invalid."""


# --------------------------------------------------------------------------- #
# Interpolation
# --------------------------------------------------------------------------- #
class _UnsetEnv(str):
    """Value of a string that was exactly ``${VAR}`` while ``VAR`` is unset or empty."""

    var: str

    def __new__(cls, var: str) -> _UnsetEnv:
        obj = super().__new__(cls, "")
        obj.var = var
        return obj


def has_env_reference(value: str) -> bool:
    """Return True if ``value`` contains at least one ``${VAR}`` reference."""
    return any(match.group("name") for match in _ENV_PATTERN.finditer(value))


def interpolate_env(value: str, env: Mapping[str, str] | None = None) -> str:
    """Expand ``${VAR}`` / ``${VAR:-default}`` references in ``value``.

    ``${VAR:-default}`` uses ``default`` when ``VAR`` is unset or empty; plain
    ``${VAR}`` expands to an empty string then. A value that is exactly
    ``${VAR}`` with ``VAR`` unset or empty returns an "unset" marker (an empty
    string subclass) so the model can apply ``None`` / the field default.
    """
    environ: Mapping[str, str] = os.environ if env is None else env
    whole = _WHOLE_REFERENCE.match(value)
    if whole and not environ.get(whole.group("name")):
        return _UnsetEnv(whole.group("name"))

    def _replace(match: re.Match[str]) -> str:
        name = match.group("name")
        if name is None:  # "$${" escape
            return "${"
        current = environ.get(name)
        if current:
            return current
        default = match.group("default")
        return default if default is not None else ""

    return _ENV_PATTERN.sub(_replace, value)


def _interpolate_tree(node: Any, path: str, refs: set[str], env: Mapping[str, str] | None) -> Any:
    """Interpolate every string of a raw YAML tree, recording paths that used ``${...}``."""
    if isinstance(node, str):
        if has_env_reference(node):
            refs.add(path)
        return interpolate_env(node, env) if "${" in node else node
    if isinstance(node, dict):
        return {
            key: _interpolate_tree(value, f"{path}.{key}" if path else str(key), refs, env)
            for key, value in node.items()
        }
    if isinstance(node, list):
        return [_interpolate_tree(item, path, refs, env) for item in node]
    return node


def _drop_unset_env(node: Any) -> Any:
    """Leave unset ``${VAR}`` markers out of a free-form tree (mapping values and list items)."""
    if isinstance(node, dict):
        return {
            key: _drop_unset_env(value) for key, value in node.items() if not isinstance(value, _UnsetEnv)
        }
    if isinstance(node, list):
        return [_drop_unset_env(item) for item in node if not isinstance(item, _UnsetEnv)]
    return node


def _allows_none(annotation: Any) -> bool:
    if annotation is None or annotation is type(None):
        return True
    origin = get_origin(annotation)
    if origin is Union or origin is types.UnionType:
        return type(None) in get_args(annotation)
    return False


def _validation_aliases(info: FieldInfo) -> tuple[str, ...]:
    """The plain-string input aliases of a field (``validation_alias``, ``AliasChoices``)."""
    alias = info.validation_alias
    if isinstance(alias, str):
        return (alias,)
    if isinstance(alias, AliasChoices):
        return tuple(choice for choice in alias.choices if isinstance(choice, str))
    return ()


def _lookup_field(model: type[BaseModel], key: Any) -> FieldInfo | None:
    fields = model.model_fields
    if key in fields:
        return fields[key]
    for info in fields.values():
        if info.alias == key or key in _validation_aliases(info):
            return info
    return None


# --------------------------------------------------------------------------- #
# Models
# --------------------------------------------------------------------------- #
class _Model(BaseModel):
    """Base model: unknown keys are errors; unset ``${VAR}`` values fall back gracefully."""

    model_config = ConfigDict(extra="forbid", populate_by_name=True)

    @model_validator(mode="before")
    @classmethod
    def _before(cls, data: Any) -> Any:
        if not isinstance(data, dict):
            return data
        return cls._preprocess(dict(data))

    @classmethod
    def _preprocess(cls, data: dict[str, Any]) -> dict[str, Any]:
        """Resolve unset-environment markers and empty values against the field definitions.

        An unset ``${VAR}`` becomes ``None`` when the field is optional, else the
        field default (empty string when there is none). An empty YAML value
        (``key:``) for a non-optional field with a default also uses the default.
        Unset items inside lists are dropped.
        """
        result: dict[str, Any] = {}
        for key, value in data.items():
            if isinstance(value, list):
                value = [item for item in value if not isinstance(item, _UnsetEnv)]
            info = _lookup_field(cls, key)
            if info is not None and (value is None or isinstance(value, _UnsetEnv)):
                if _allows_none(info.annotation):
                    value = None
                elif not info.is_required():
                    continue  # the field default applies
            result[key] = value
        return result


def _as_list(value: Any) -> Any:
    return [value] if isinstance(value, str) else value


def _blank_to_none(value: Any) -> Any:
    """Strip a string; a blank one becomes ``None``."""
    if isinstance(value, str):
        return value.strip() or None
    return value


class RepoConfig(_Model):
    """The user's repository (the one holding the DAGs)."""

    path: Path = Path(".")
    base_branch: str = "main"
    dags_dir: str = "dags"
    tests_dir: str | None = None
    test_command: str = "python -m pytest -q"
    allowed_paths: list[str] = Field(default_factory=lambda: ["src/", "dags/"])
    pytest_timeout: int = Field(default=600, ge=1)

    @field_validator("dags_dir", "tests_dir")
    @classmethod
    def _posix_dir(cls, value: str | None) -> str | None:
        if value is None:
            return None
        cleaned = value.strip().replace("\\", "/").rstrip("/")
        while cleaned.startswith("./"):
            cleaned = cleaned[2:]
        return cleaned or "."

    @field_validator("allowed_paths", mode="before")
    @classmethod
    def _allowed_list(cls, value: Any) -> Any:
        return _as_list(value)


class AirflowConfig(_Model):
    """How the worker reaches the Airflow REST API (v2)."""

    api_url: str = "http://localhost:8080"
    username: str | None = None
    password: str | None = None

    @field_validator("api_url")
    @classmethod
    def _strip_slash(cls, value: str) -> str:
        return value.strip().rstrip("/") or "http://localhost:8080"


def _modes_of(kind: str) -> frozenset[str]:
    return frozenset(mode for mode, mode_kind in MODE_KINDS.items() if mode_kind == kind)


#: The modes that turn on each detection kind and each pushed-event source (see ``wants``).
_ENABLED_BY: Mapping[str, frozenset[str]] = types.MappingProxyType(
    {
        "poll": _modes_of("poll"),
        "push": _modes_of("push"),
        "queue": _modes_of("queue"),
        "rest": _modes_of("poll"),
        "listener": frozenset({"listener", "push"}),
        "callback": frozenset({"callback", "push"}),
        "eventbridge": frozenset({"push"}),
        "sqs": frozenset({"queue"}),
    }
)


class MonitorConfig(_Model):
    """Which pipelines (DAGs) are watched and how failures are detected.

    ``dags`` is the canonical key; ``pipelines`` is accepted as an input alias (one of them).
    ``modes`` are kept verbatim; :meth:`wants` normalises them.
    """

    dags: list[str] = Field(default_factory=lambda: ["*"], validation_alias=AliasChoices("dags", "pipelines"))
    modes: list[MonitorMode] = Field(default_factory=lambda: ["rest", "listener"])
    poll_interval_seconds: int = Field(default=30, ge=1)
    trigger: Literal["final_failure", "every_failure"] = "final_failure"

    @classmethod
    def _preprocess(cls, data: dict[str, Any]) -> dict[str, Any]:
        """Apply the base rules, then refuse ``dags`` and its alias ``pipelines`` together."""
        data = super()._preprocess(data)
        if "dags" in data and "pipelines" in data:
            raise ValueError(
                "'dags' and 'pipelines' are the same setting ('pipelines' is an alias): set only one"
            )
        return data

    @field_validator("dags", "modes", mode="before")
    @classmethod
    def _listify(cls, value: Any) -> Any:
        return _as_list(value)

    def wants(self, kind: str) -> bool:
        """True when ``modes`` turn ``kind`` on.

        ``kind`` is a detection kind: ``"poll"`` (modes ``rest``/``poll``: list failures through
        the adapter's feed), ``"push"`` (``listener``/``callback``/``push``: confirm pushed
        hints) or ``"queue"`` (``queue``: drain the adapter's hint queue). It may also be the
        source of a pushed event, for the per-source check: ``listener`` and ``callback`` each
        need their own mode (as before) or the neutral ``push``, ``eventbridge`` needs ``push``,
        ``sqs`` needs ``queue`` and ``rest`` is ``poll``. Manual (``cli``) incidents need no
        mode. Anything else raises ``ValueError``.
        """
        if kind == "cli":
            return True
        enabling = _ENABLED_BY.get(kind)
        if enabling is None:
            raise ValueError(f"unknown detection kind or incident source {kind!r}")
        return any(mode in enabling for mode in self.modes)


class ContractsConfig(_Model):
    """Where data contracts live (relative to ``repo.path``)."""

    dir: str = "healer/contracts"


class ClassificationConfig(_Model):
    """Classification thresholds."""

    min_confidence: float = Field(default=0.75, ge=0.0, le=1.0)


class EmailConfig(_Model):
    """SMTP settings for alert emails (YAML key ``from`` maps to ``sender``)."""

    host: str
    port: int = Field(default=25, ge=1, le=65535)
    sender: str = Field(alias="from")
    to: list[str]
    starttls: bool = False
    username: str | None = None
    password: str | None = None

    @field_validator("to", mode="before")
    @classmethod
    def _split_recipients(cls, value: Any) -> Any:
        """Accept a comma-separated string or a list (items may contain commas too)."""
        items = _as_list(value)
        if not isinstance(items, list):
            return items
        recipients: list[Any] = []
        for item in items:
            if isinstance(item, str):
                recipients.extend(part.strip() for part in item.split(",") if part.strip())
            else:
                recipients.append(item)
        return recipients


class NotifyConfig(_Model):
    """Notification channels and the incident events that trigger them."""

    email: EmailConfig | None = None
    events: list[Literal["waiting_human", "reported", "repaired", "repair_failed"]] = Field(
        default_factory=lambda: list(ALERT_EVENTS)
    )

    @field_validator("events", mode="before")
    @classmethod
    def _listify(cls, value: Any) -> Any:
        return _as_list(value)

    @model_validator(mode="after")
    def _disable_email_without_host(self) -> NotifyConfig:
        """An email block whose host resolved to an empty value disables email."""
        if self.email is not None and not self.email.host.strip():
            self.email = None
        return self


class GithubConfig(_Model):
    """GitHub pull requests (without repo + token fixes are written as PR.md)."""

    repo: str | None = None
    pr_base: str = "main"
    token: str | None = None

    @field_validator("repo")
    @classmethod
    def _owner_name(cls, value: str | None) -> str | None:
        """Accept ``owner/name`` or a github.com URL and normalise to ``owner/name``."""
        if value is None or not value.strip():
            return None
        repo = value.strip()
        for prefix in ("https://github.com/", "http://github.com/", "git@github.com:"):
            if repo.startswith(prefix):
                repo = repo[len(prefix) :]
        repo = repo.rstrip("/")
        if repo.endswith(".git"):
            repo = repo[:-4]
        if not _GITHUB_REPO.match(repo):
            raise ValueError("expected 'owner/name'")
        return repo


class LlmConfig(_Model):
    """Language model used for classification and repairs."""

    provider: Literal["anthropic", "fake"] = "anthropic"
    model: str = "claude-opus-5-5"
    effort: str = "high"
    max_tokens: int = Field(default=16000, ge=1)
    #: Opt into server-side refusal fallbacks (``fallbacks: "default"`` beta).
    fallbacks: bool = True


class LimitsConfig(_Model):
    """Safety limits for the agent."""

    max_fix_attempts: int = Field(default=3, ge=1)
    max_incidents_per_hour: int = Field(default=10, ge=1)
    max_tokens_per_incident: int = Field(default=400000, ge=1)
    max_diff_files: int = Field(default=10, ge=1)
    max_diff_lines: int = Field(default=400, ge=1)


class MaskingConfig(_Model):
    """Masking of sample data before it is stored, shown, logged or sent to the LLM."""

    default: Literal["hash", "redact"] = "hash"
    columns: list[str] = Field(default_factory=list)
    allow: list[str] = Field(default_factory=list)
    hook: str | None = None
    sample_rows: int = Field(default=5, ge=0, le=1000)

    @field_validator("columns", "allow", mode="before")
    @classmethod
    def _listify(cls, value: Any) -> Any:
        return _as_list(value)

    @field_validator("hook")
    @classmethod
    def _hook_format(cls, value: str | None) -> str | None:
        if value is None or not value.strip():
            return None
        if not _HOOK.match(value.strip()):
            raise ValueError("expected 'package.module:function'")
        return value.strip()


class StoreConfig(_Model):
    """Incident store (SQLAlchemy URL)."""

    url: str | None = None


class IngestConfig(_Model):
    """Push ingestion from the listener / failure callback."""

    token: str | None = None
    api_url: str | None = None


class OrchestratorConfig(_Model):
    """The orchestrator dag-healer watches (``orchestrator:``; absent: Airflow).

    ``type`` is an entry-point name of the ``dag_healer.orchestrators`` group (``airflow``,
    ``stepfunctions``, ``fake`` or a third-party name) or an explicit
    ``"package.module:factory"`` string. ``instance`` is stored on every incident; it
    defaults to the type, so a type that is not a valid instance id needs an explicit one.
    ``options`` are validated by the adapter's ``options_model`` when the adapter is built;
    Airflow keeps using the top-level ``airflow:`` section and takes none.
    """

    type: str = DEFAULT_ORCHESTRATOR
    instance: str | None = Field(default=None, pattern=INSTANCE_PATTERN, validate_default=True)
    options: dict[str, Any] = Field(default_factory=dict)

    @field_validator("type", mode="before")
    @classmethod
    def _type_text(cls, value: Any) -> Any:
        """Strip the type; a blank one means the default (Airflow)."""
        if isinstance(value, str):
            return value.strip() or DEFAULT_ORCHESTRATOR
        return value

    @field_validator("type")
    @classmethod
    def _type_format(cls, value: str) -> str:
        valid = _FACTORY_TARGET.match(value) if ":" in value else _ENTRY_POINT_NAME.match(value)
        if not valid:
            raise ValueError(
                "expected an entry-point name of the dag_healer.orchestrators group (such as "
                "'airflow' or 'stepfunctions') or 'package.module:factory'"
            )
        return value

    @field_validator("instance", mode="before")
    @classmethod
    def _instance_text(cls, value: Any) -> Any:
        return _blank_to_none(value)

    @field_validator("instance")
    @classmethod
    def _instance_required(cls, value: str | None, info: ValidationInfo) -> str | None:
        """Without ``instance`` the type is the instance id, so it must be a valid one."""
        type_ = info.data.get("type")
        if value is None and isinstance(type_, str) and not _INSTANCE.match(type_):
            raise ValueError(
                f"required because the type {type_!r} is not a valid instance id (lowercase "
                "letters, digits, '.', '_' and '-', at most 64 characters)"
            )
        return value

    @field_validator("options", mode="before")
    @classmethod
    def _options_tree(cls, value: Any) -> Any:
        return _drop_unset_env(value) if isinstance(value, dict) else value

    @field_validator("options")
    @classmethod
    def _airflow_takes_no_options(cls, value: dict[str, Any], info: ValidationInfo) -> dict[str, Any]:
        if value and info.data.get("type") == DEFAULT_ORCHESTRATOR:
            raise ValueError(
                "the airflow orchestrator takes no options: its settings are the top-level 'airflow:' section"
            )
        return value

    @property
    def instance_id(self) -> str:
        """The instance id stored on incidents: ``instance``, else the type."""
        return self.instance or self.type


class RbacRule(_Model):
    """One allow rule of the standalone host (``serve.auth.rules``); any matching rule allows.

    ``principals``: ``user:<name>``, ``group:<name>``, ``authenticated`` or ``*``.
    ``pipelines``: fnmatch patterns of pipeline (DAG) ids; a ``!`` prefix excludes.
    """

    principals: list[str]
    pipelines: list[str] = Field(default_factory=lambda: ["*"])
    actions: list[RbacAction] = Field(default_factory=lambda: ["read"])

    @field_validator("principals", "pipelines", "actions", mode="before")
    @classmethod
    def _listify(cls, value: Any) -> Any:
        return _as_list(value)

    @field_validator("principals")
    @classmethod
    def _principal_format(cls, value: list[str]) -> list[str]:
        principals = [item.strip() for item in value]
        for principal in principals:
            if not _PRINCIPAL.match(principal):
                raise ValueError(
                    f"invalid principal {principal!r}: expected 'user:<name>', 'group:<name>', "
                    "'authenticated' or '*'"
                )
        return principals

    @field_validator("pipelines")
    @classmethod
    def _pattern_text(cls, value: list[str]) -> list[str]:
        patterns = [item.strip() for item in value]
        if any(pattern in ("", "!") for pattern in patterns):
            raise ValueError("pipeline patterns must not be empty")
        return patterns


def _token_groups(where: str, value: Any) -> list[str]:
    if value is None:
        return []
    items = _as_list(value)
    if not isinstance(items, list) or not all(isinstance(item, str) for item in items):
        raise ValueError(f"{where}: groups must be a list of strings")
    return [item.strip() for item in items if not isinstance(item, _UnsetEnv) and item.strip()]


def _token_entry(index: int, item: Any) -> dict[str, Any] | None:
    """One ``serve.auth.tokens`` entry as ``{name, sha256, groups}``; ``None`` when unset here."""
    where = f"item {index}"
    if not isinstance(item, Mapping):
        raise ValueError(f"{where}: expected a mapping with name, sha256 and groups")
    unknown = sorted(str(key) for key in item if key not in _TOKEN_KEYS)
    if unknown:
        raise ValueError(
            f"{where}: unknown key(s) {', '.join(unknown)}; a token is configured by the sha256 hex "
            "digest of its value only (name, sha256, groups), never by the token itself"
        )
    digest = item.get("sha256")
    if isinstance(digest, _UnsetEnv):
        return None  # sha256: ${VAR} with VAR unset: this token is not configured here
    if not isinstance(digest, str) or not _SHA256_HEX.match(digest.strip()):
        raise ValueError(f"{where}: sha256 must be the 64-character hex sha256 digest of the token")
    name = item.get("name")
    if not isinstance(name, str) or not name.strip():
        raise ValueError(f"{where}: name must be a non-empty string")
    return {
        "name": name.strip(),
        "sha256": digest.strip().lower(),
        "groups": _token_groups(where, item.get("groups")),
    }


class ServeAuthConfig(_Model):
    """Authentication and permissions of ``dag-healer serve`` (``serve.auth``).

    Backends: ``dev`` (a fixed local user, loopback only), ``token`` (bearer tokens listed by
    their sha256 hex digest only), ``trusted_header`` (a user header set by a proxy listed in
    ``trusted_proxies``) and ``alb_oidc`` (the ALB-signed ``x-amzn-oidc-data`` JWT).
    """

    backend: ServeAuthBackend = "dev"
    tokens: list[dict[str, Any]] = Field(default_factory=list)  # [{name, sha256, groups}] (hashes only)
    user_header: str = "X-Forwarded-User"
    groups_header: str | None = "X-Forwarded-Groups"
    trusted_proxies: list[str] = Field(default_factory=list)  # CIDRs (required for trusted_header)
    alb_arn: str | None = None
    region: str | None = None
    user_claim: str = "email"
    groups_claim: str | None = None
    rules: list[RbacRule] = Field(default_factory=list)

    @field_validator("tokens", mode="before")
    @classmethod
    def _token_hashes(cls, value: Any) -> Any:
        """Accept ``{name, sha256, groups}`` entries: hashes only, a hash listed once."""
        if not isinstance(value, list):
            return value
        tokens: list[dict[str, Any]] = []
        owners: dict[str, str] = {}
        for index, item in enumerate(value):
            token = _token_entry(index, item)
            if token is None:
                continue
            if token["sha256"] in owners:
                raise ValueError(
                    f"tokens {owners[token['sha256']]!r} and {token['name']!r} have the same sha256"
                )
            owners[token["sha256"]] = token["name"]
            tokens.append(token)
        return tokens

    @field_validator("user_header", mode="before")
    @classmethod
    def _user_header_text(cls, value: Any) -> Any:
        if isinstance(value, str):
            return value.strip() or "X-Forwarded-User"
        return value

    @field_validator("groups_header", "alb_arn", "region", "groups_claim", mode="before")
    @classmethod
    def _optional_text(cls, value: Any) -> Any:
        return _blank_to_none(value)

    @field_validator("user_header", "groups_header")
    @classmethod
    def _header_name(cls, value: str | None) -> str | None:
        if value is not None and not _HEADER_NAME.match(value):
            raise ValueError(f"{value!r} is not an HTTP header name")
        return value

    @field_validator("trusted_proxies", mode="before")
    @classmethod
    def _listify(cls, value: Any) -> Any:
        return _as_list(value)

    @field_validator("trusted_proxies")
    @classmethod
    def _networks(cls, value: list[str]) -> list[str]:
        """Each item is an IP address or a CIDR, normalised (``10.0.0.7`` -> ``10.0.0.7/32``)."""
        networks: list[str] = []
        for item in value:
            try:
                networks.append(str(ipaddress.ip_network(item.strip(), strict=False)))
            except ValueError:
                raise ValueError(f"{item!r} is not an IP address or a CIDR such as 10.0.0.0/8") from None
        return networks

    @field_validator("alb_arn")
    @classmethod
    def _alb_arn_format(cls, value: str | None) -> str | None:
        if value is not None and not _ALB_ARN.match(value):
            raise ValueError(
                "expected an Application Load Balancer ARN "
                "(arn:aws:elasticloadbalancing:<region>:<account>:loadbalancer/app/<name>/<id>)"
            )
        return value

    @field_validator("region")
    @classmethod
    def _region_format(cls, value: str | None) -> str | None:
        if value is not None and not _AWS_REGION.match(value):
            raise ValueError("expected an AWS region such as eu-west-1")
        return value

    @field_validator("user_claim", mode="before")
    @classmethod
    def _user_claim_text(cls, value: Any) -> Any:
        if isinstance(value, str):
            return value.strip() or "email"
        return value

    @model_validator(mode="after")
    def _backend_requirements(self) -> ServeAuthConfig:
        """``trusted_header`` needs ``trusted_proxies``; ``alb_oidc`` needs ``alb_arn`` (region from it)."""
        if self.backend == "trusted_header" and not self.trusted_proxies:
            raise ValueError(
                "the trusted_header backend requires trusted_proxies (the CIDRs of the proxies that "
                "set the user header)"
            )
        if self.backend == "alb_oidc":
            match = _ALB_ARN.match(self.alb_arn or "")
            if match is None:
                raise ValueError(
                    "the alb_oidc backend requires alb_arn (the load balancer that signs the users' JWT)"
                )
            if self.region is None:
                self.region = match.group("region")
            elif self.region != match.group("region"):
                raise ValueError(
                    f"region {self.region!r} is not the region of alb_arn ({match.group('region')!r})"
                )
        return self


class ServeConfig(_Model):
    """The standalone host ``dag-healer serve`` (``serve:``; ignored inside Airflow)."""

    host: str = "127.0.0.1"
    port: int = Field(default=8090, ge=1, le=65535)
    public_url: str | None = None
    auth: ServeAuthConfig = Field(default_factory=ServeAuthConfig)

    @field_validator("host", mode="before")
    @classmethod
    def _host_text(cls, value: Any) -> Any:
        if isinstance(value, str):
            return value.strip() or "127.0.0.1"
        return value

    @field_validator("public_url", mode="before")
    @classmethod
    def _public_url_text(cls, value: Any) -> Any:
        return _blank_to_none(value)

    @field_validator("public_url")
    @classmethod
    def _absolute_url(cls, value: str | None) -> str | None:
        """An absolute http(s) URL, without the trailing slash."""
        if value is None:
            return None
        url = value.rstrip("/")
        parts = urlsplit(url)
        if parts.scheme not in ("http", "https") or not parts.netloc:
            raise ValueError("expected an absolute http(s) URL such as https://healer.example.com")
        return url


#: Sections that dumps leave out while they hold their defaults (phase-5 dumps stay identical).
_OPTIONAL_SECTIONS: tuple[str, ...] = ("orchestrator", "serve")


class HealerConfig(_Model):
    """The whole ``healer.yaml``."""

    version: int = SUPPORTED_VERSION
    repo: RepoConfig = Field(default_factory=RepoConfig)
    orchestrator: OrchestratorConfig = Field(default_factory=OrchestratorConfig)
    airflow: AirflowConfig = Field(default_factory=AirflowConfig)
    monitor: MonitorConfig = Field(default_factory=MonitorConfig)
    contracts: ContractsConfig = Field(default_factory=ContractsConfig)
    datasets: dict[str, dict[str, DatasetLocator]] = Field(default_factory=dict)
    classification: ClassificationConfig = Field(default_factory=ClassificationConfig)
    notify: NotifyConfig = Field(default_factory=NotifyConfig)
    github: GithubConfig = Field(default_factory=GithubConfig)
    llm: LlmConfig = Field(default_factory=LlmConfig)
    limits: LimitsConfig = Field(default_factory=LimitsConfig)
    masking: MaskingConfig = Field(default_factory=MaskingConfig)
    serve: ServeConfig = Field(default_factory=ServeConfig)
    store: StoreConfig = Field(default_factory=StoreConfig)
    ingest: IngestConfig = Field(default_factory=IngestConfig)
    debug: bool = False

    _source_path: Path | None = PrivateAttr(default=None)
    _env_refs: frozenset[str] = PrivateAttr(default_factory=frozenset)
    _secret_options: tuple[str, ...] = PrivateAttr(default=())

    @classmethod
    def _preprocess(cls, data: dict[str, Any]) -> dict[str, Any]:
        """Drop top-level ``x-*`` keys (free-form user notes), then apply the base rules."""
        data = {k: v for k, v in data.items() if not (isinstance(k, str) and k.startswith("x-"))}
        return super()._preprocess(data)

    @field_validator("version")
    @classmethod
    def _supported_version(cls, value: int) -> int:
        if value != SUPPORTED_VERSION:
            raise ValueError(f"unsupported config version {value}; expected {SUPPORTED_VERSION}")
        return value

    @model_serializer(mode="wrap")
    def _leave_out_default_sections(self, handler: SerializerFunctionWrapHandler) -> Any:
        """Dump as usual, without ``orchestrator``/``serve`` while they hold their defaults.

        An existing ``healer.yaml`` therefore dumps exactly as before (``dag-healer check``
        output, content digests); a section that differs from its default is always shown.
        """
        data = handler(self)
        if isinstance(data, dict):
            for name in _OPTIONAL_SECTIONS:
                if name in data and _holds_default(self, name):
                    del data[name]
        return data

    # ------------------------------------------------------------------ paths
    @property
    def source_path(self) -> Path | None:
        """The YAML file this configuration was loaded from (``None`` for defaults)."""
        return self._source_path

    @source_path.setter
    def source_path(self, value: str | Path | None) -> None:
        self._source_path = None if value is None else Path(value)

    @property
    def repo_root(self) -> Path:
        """Absolute path of the user's repository."""
        return Path(self.repo.path).expanduser().resolve()

    @property
    def dags_path(self) -> Path:
        """Absolute path of the DAG folder."""
        return (self.repo_root / self.repo.dags_dir).resolve()

    @property
    def contracts_dir(self) -> Path:
        """Absolute path of the contracts directory (relative values are under the repo)."""
        directory = Path(self.contracts.dir).expanduser()
        if directory.is_absolute():
            return directory.resolve()
        return (self.repo_root / directory).resolve()

    @property
    def healer_home(self) -> Path:
        """State directory: ``$HEALER_HOME``, ``$AIRFLOW_HOME/healer`` or ``~/.dag-healer``."""
        explicit = os.environ.get(HEALER_HOME_ENV)
        if explicit:
            return Path(explicit).expanduser()
        airflow_home = os.environ.get("AIRFLOW_HOME")
        if airflow_home:
            return Path(airflow_home).expanduser() / "healer"
        return Path.home() / ".dag-healer"

    @property
    def database_url(self) -> str:
        """Store URL: ``store.url``, ``$HEALER_DATABASE_URL`` or SQLite under ``healer_home``."""
        if self.store.url and self.store.url.strip():
            return self.store.url.strip()
        env_url = os.environ.get(DATABASE_URL_ENV)
        if env_url:
            return env_url
        db_file = (self.healer_home / "healer.db").resolve()
        return f"sqlite:///{db_file.as_posix()}"

    # ---------------------------------------------------------------- helpers
    def is_monitored(self, dag_id: str) -> bool:
        """True if ``dag_id`` matches ``monitor.dags`` (fnmatch; ``!pattern`` excludes)."""
        includes = [p for p in self.monitor.dags if not p.startswith("!")]
        excludes = [p[1:] for p in self.monitor.dags if p.startswith("!")]
        if any(fnmatch.fnmatchcase(dag_id, pattern) for pattern in excludes):
            return False
        if not includes:
            return bool(excludes)
        return any(fnmatch.fnmatchcase(dag_id, pattern) for pattern in includes)

    def dataset_locators(self, dag_id: str) -> dict[str, DatasetLocator]:
        """Return the ``datasets`` locators configured for ``dag_id`` (task_id -> locator)."""
        return dict(self.datasets.get(dag_id, {}))

    def uses_env_reference(self, field_path: str) -> bool:
        """True if the loaded value at ``field_path`` (dotted) came from ``${...}``."""
        return field_path in self._env_refs

    @property
    def secret_fields(self) -> tuple[str, ...]:
        """Dotted paths of this configuration's secret fields.

        The fixed secret fields plus ``orchestrator.options.<name>`` for each secret option of
        the configured adapter, found when the configuration was loaded.
        """
        return tuple(dict.fromkeys((*_BASE_SECRET_FIELDS, *self._secret_options)))

    def secret_literals(self) -> list[str]:
        """Dotted paths of secret fields set to literal values instead of ``${ENV}`` references."""
        literals = [
            path for path in self.secret_fields if _get_path(self, path) and path not in self._env_refs
        ]
        url = self.store.url
        if url and "store.url" not in self._env_refs and _url_has_password(url):
            literals.append("store.url")
        return literals


def _holds_default(cfg: BaseModel, name: str) -> bool:
    """True when the section ``name`` of ``cfg`` equals its field default."""
    info = type(cfg).model_fields.get(name)
    if info is None:
        return False
    return bool(getattr(cfg, name) == info.get_default(call_default_factory=True))


def _get_path(model: Any, dotted: str) -> Any:
    value: Any = model
    for part in dotted.split("."):
        if value is None:
            return None
        value = value.get(part) if isinstance(value, Mapping) else getattr(value, part, None)
    return value


def _url_has_password(url: str) -> bool:
    try:
        return bool(urlsplit(url).password)
    except ValueError:
        return False


def _register_secret_fields(paths: Iterable[str]) -> None:
    """Add ``paths`` to :data:`SECRET_FIELDS` (idempotent; the first-seen order is kept)."""
    global SECRET_FIELDS
    with _secret_fields_lock:
        extra = tuple(path for path in dict.fromkeys(paths) if path not in SECRET_FIELDS)
        if extra:
            SECRET_FIELDS = (*SECRET_FIELDS, *extra)


def _orchestrator_secret_options(section: OrchestratorConfig) -> tuple[str, ...]:
    """``orchestrator.options.<name>`` of each secret option of the configured adapter.

    The adapter factory is imported only when ``options`` are set, so loading a configuration
    without them (every Airflow one) imports nothing new. A factory that cannot be loaded adds
    nothing here; building the adapter reports why.
    """
    if not section.options:
        return ()
    try:
        from dag_healer.orchestrators import registry

        return tuple(registry.secret_option_paths(registry.load_factory(section.type)))
    except Exception as exc:  # an adapter that is not installed must not make healer.yaml unreadable
        logger.debug("dag-healer: no secret options known for orchestrator type %r: %s", section.type, exc)
        return ()


# --------------------------------------------------------------------------- #
# Loading
# --------------------------------------------------------------------------- #
def _format_validation_error(exc: ValidationError, source: Path | None) -> str:
    lines = []
    for error in exc.errors():
        location = ".".join(str(part) for part in error["loc"]) or "<root>"
        if error["type"] == "extra_forbidden":
            message = "unknown key (top-level keys starting with 'x-' are allowed for notes)"
        else:
            message = error["msg"]
        lines.append(f"  - {location}: {message}")
    where = str(source) if source is not None else "the configuration"
    return f"Invalid dag-healer configuration in {where}:\n" + "\n".join(lines)


def config_from_mapping(
    data: Mapping[str, Any],
    *,
    base_dir: str | Path | None = None,
    source_path: str | Path | None = None,
    env: Mapping[str, str] | None = None,
) -> HealerConfig:
    """Build a :class:`HealerConfig` from raw (not yet interpolated) YAML data.

    ``repo.path`` is resolved against ``base_dir`` when relative. The secret options of the
    configured orchestrator adapter are added to :data:`SECRET_FIELDS` and to the
    configuration's :attr:`~HealerConfig.secret_fields`.
    """
    refs: set[str] = set()
    interpolated = _interpolate_tree(dict(data), "", refs, env)
    source = Path(source_path) if source_path is not None else None
    try:
        config = HealerConfig.model_validate(interpolated)
    except ValidationError as exc:
        raise ConfigError(_format_validation_error(exc, source)) from None
    config._env_refs = frozenset(refs)
    config._secret_options = _orchestrator_secret_options(config.orchestrator)
    if config._secret_options:
        _register_secret_fields(config._secret_options)
    config.source_path = source
    if base_dir is not None:
        repo_path = Path(config.repo.path).expanduser()
        if not repo_path.is_absolute():
            repo_path = Path(base_dir) / repo_path
        config.repo.path = repo_path.resolve()
    return config


def _read_yaml(path: Path) -> dict[str, Any]:
    try:
        text = path.read_text(encoding="utf-8")
    except OSError as exc:
        raise ConfigError(f"Cannot read configuration file {path}: {exc}") from None
    try:
        raw = yaml.safe_load(text)
    except yaml.YAMLError as exc:
        raise ConfigError(f"Invalid YAML in {path}: {exc}") from None
    if raw is None:
        return {}
    if not isinstance(raw, dict):
        raise ConfigError(f"{path}: the top level of healer.yaml must be a mapping")
    return raw


def load_config_file(path: str | Path) -> HealerConfig:
    """Load and validate one ``healer.yaml`` file."""
    resolved = Path(path).expanduser().resolve()
    return config_from_mapping(_read_yaml(resolved), base_dir=resolved.parent, source_path=resolved)


def _as_config_file(path: Path) -> Path:
    return path / CONFIG_FILENAME if path.is_dir() else path


def config_candidates() -> list[Path]:
    """The locations :func:`load_config` checks, in order (they may not exist)."""
    candidates: list[Path] = []
    explicit = os.environ.get(CONFIG_ENV)
    if explicit:
        candidates.append(_as_config_file(Path(explicit).expanduser()))
    airflow_home = os.environ.get("AIRFLOW_HOME")
    if airflow_home:
        candidates.append(Path(airflow_home).expanduser() / CONFIG_FILENAME)
    candidates.append(Path.cwd() / CONFIG_FILENAME)
    return candidates


def find_config_file() -> Path | None:
    """Return the first existing configuration file in the resolution order, if any."""
    for candidate in config_candidates():
        if candidate.is_file():
            return candidate
    return None


def load_config(path: str | Path | None = None) -> HealerConfig:
    """Load the dag-healer configuration.

    With ``path`` (a file or a directory holding ``healer.yaml``) that file must
    exist. Without it the resolution order is ``$HEALER_CONFIG``,
    ``$AIRFLOW_HOME/healer.yaml``, ``./healer.yaml``; if none exists the defaults
    are returned (``repo.path`` = the directory of ``$HEALER_CONFIG`` when set,
    else the current directory). Raises :class:`ConfigError` on invalid files.
    """
    if path is not None:
        target = _as_config_file(Path(path).expanduser())
        if not target.is_file():
            raise ConfigError(f"Configuration file not found: {target}")
        return load_config_file(target)

    found = find_config_file()
    if found is not None:
        return load_config_file(found)

    config = HealerConfig()
    explicit = os.environ.get(CONFIG_ENV)
    if explicit:
        missing = _as_config_file(Path(explicit).expanduser())
        if str(missing) not in _warned_missing:
            _warned_missing.add(str(missing))
            logger.warning("%s=%s does not exist; using default settings", CONFIG_ENV, missing)
        config.repo.path = missing.parent.resolve()
    else:
        config.repo.path = Path.cwd().resolve()
    return config


# --------------------------------------------------------------------------- #
# Template
# --------------------------------------------------------------------------- #
def _yaml_str(value: str | Path | None) -> str:
    """Render a scalar as a YAML double-quoted string (JSON strings are valid YAML)."""
    if value is None:
        return "null"
    text = value.as_posix() if isinstance(value, Path) else str(value).replace("\\", "/")
    return json.dumps(text, ensure_ascii=False)


# The templates are str.format strings: literal braces are doubled. The Airflow template is the
# phase-5 one plus the commented ``orchestrator:`` block; both share the sections between
# ``classification:`` and ``masking:``.
_AIRFLOW_HEAD = """\
# dag-healer configuration (healer.yaml), generated by `dag-healer init`.
# Review it and commit it next to your DAGs.
#
# - Relative paths are resolved against the directory of this file.
# - Secrets must be ${{ENV_VAR}} references, never literal values (`dag-healer doctor`
#   flags literals). ${{VAR:-default}} provides a fallback. A value that is exactly
#   ${{VAR}} becomes null (or the field default) when VAR is unset or empty.
# - Top-level keys starting with "x-" are ignored: use them for your own notes.
version: 1

# Repository that contains your DAGs. dags_dir and tests_dir are relative to repo.path;
# tests_dir null means a minimal tests/ folder is created when a fix needs one.
repo:
  path: {repo_path}
  base_branch: main              # branch that fixes are based on
  dags_dir: {dags_dir}
  tests_dir: {tests_dir}
  test_command: "python -m pytest -q"
  allowed_paths: ["src/", "dags/"]   # the repair agent may only edit files under these prefixes
  pytest_timeout: 600            # seconds per pytest run

# Orchestrator (optional). Without this section dag-healer watches Airflow, configured by
# the airflow: section below; `dag-healer orchestrators list` shows the installed types.
# orchestrator:
#   type: airflow                # airflow | stepfunctions | fake | "package.module:factory"
#   instance: null               # stored on every incident (default: the type); unique per store
#   options: {{}}                  # adapter settings; Airflow takes none (they are in airflow:)

airflow:
  api_url: "${{HEALER_API_URL:-http://localhost:8080}}"   # Airflow api-server (REST API v2)
  username: "${{HEALER_AIRFLOW_USERNAME}}"
  password: "${{HEALER_AIRFLOW_PASSWORD}}"

monitor:
  dags: ["*"]                    # fnmatch patterns; prefix a pattern with "!" to exclude DAGs
  modes: [rest, listener]        # rest: poll failed task instances; listener: push from task runners
  poll_interval_seconds: 30
  trigger: final_failure         # final_failure | every_failure (also tries that will be retried)

contracts:
  dir: healer/contracts          # data contracts, relative to repo.path

# Dataset locators per DAG and task (`init` fills in the ones it discovered). Add the
# inputs it could not resolve by hand:
# datasets:
#   my_dag:
#     my_task: {{type: csv, path: data/incoming/input.csv}}
{datasets_block}

"""

_SHARED_SECTIONS = """\
classification:
  min_confidence: 0.75           # below this the incident waits for a human decision

notify:
  # Email is enabled when the SMTP host resolves to a non-empty value.
  email:
    host: "${{HEALER_SMTP_HOST}}"
    port: "${{HEALER_SMTP_PORT:-25}}"
    from: "${{HEALER_SMTP_FROM:-dag-healer@localhost}}"
    to: ["${{HEALER_ALERT_TO:-data-team@example.com}}"]   # comma-separated values are split
    starttls: "${{HEALER_SMTP_STARTTLS:-false}}"
    username: "${{HEALER_SMTP_USERNAME}}"
    password: "${{HEALER_SMTP_PASSWORD}}"
  events: [waiting_human, reported, repaired, repair_failed]

github:
  repo: "${{GITHUB_REPO}}"         # owner/name; without repo and token fixes are written as PR.md
  pr_base: main
  token: "${{GITHUB_TOKEN}}"

llm:
  provider: "${{HEALER_LLM_PROVIDER:-anthropic}}"   # anthropic | fake (offline tests)
  model: claude-opus-5-5
  effort: high
  max_tokens: 16000
  fallbacks: true                # server-side refusal fallbacks (Claude API only)

limits:
  max_fix_attempts: 3
  max_incidents_per_hour: 10
  max_tokens_per_incident: 400000
  max_diff_files: 10
  max_diff_lines: 400

masking:
  default: hash                  # hash | redact
  columns: []                    # always mask these columns
  allow: []                      # never mask these columns
  hook: null                     # "package.module:function", called as function(df, context) -> df
  sample_rows: 5

"""

_AIRFLOW_TAIL = """\
store:
  url: "${{HEALER_DATABASE_URL}}"  # null: SQLite under $HEALER_HOME (single process only)

ingest:
  token: "${{HEALER_INGEST_TOKEN}}"   # shared secret required by the listener / callback endpoint
  api_url: null                  # where task runners reach the api-server (null: autodetect)

debug: "${{HEALER_DEBUG:-false}}"
"""

_TEMPLATE = _AIRFLOW_HEAD + _SHARED_SECTIONS + _AIRFLOW_TAIL

# AWS Step Functions (section 9.14 of the design). The ARNs, the region, the instance and the
# pipeline are examples to replace.
_SFN_HEAD = """\
# dag-healer configuration (healer.yaml) for AWS Step Functions, generated by `dag-healer init`.
# Review it, replace the example ARNs, region and pipeline ids, and commit it next to your
# state machines and Lambda functions.
#
# - Relative paths are resolved against the directory of this file.
# - Secrets must be ${{ENV_VAR}} references, never literal values (`dag-healer doctor`
#   flags literals). ${{VAR:-default}} provides a fallback. A value that is exactly
#   ${{VAR}} becomes null (or the field default) when VAR is unset or empty.
# - Top-level keys starting with "x-" are ignored: use them for your own notes.
version: 1

# Repository that contains your state machine definitions and Lambda code. dags_dir and
# tests_dir are relative to repo.path; tests_dir null means a minimal tests/ folder is
# created when a fix needs one.
repo:
  path: {repo_path}
  base_branch: main              # branch that fixes are based on
  dags_dir: {dags_dir}
  tests_dir: {tests_dir}
  test_command: "python -m pytest -q"
  allowed_paths: ["functions/", "src/"]   # the repair agent edits files under these prefixes only
  pytest_timeout: 600            # seconds per pytest run

# One orchestrator instance is one AWS account and region (`dag-healer orchestrators list`).
orchestrator:
  type: stepfunctions
  instance: sfn-prod-euw1        # stored on every incident; unique per store
  options:
    region: eu-west-1
    profile: null                # local development; null: the default boto3 credential chain
    role_arn: null               # optional AssumeRole (+ role_external_id, a secret option)
    endpoint_url: null           # VPC endpoint / LocalStack (not Step Functions Local)
    template: template.yaml      # SAM template that maps Task states to Lambda code
    pipelines:                   # pipeline id -> state machine; contracts and datasets use the id
      sales_daily_etl:
        state_machine: arn:aws:states:eu-west-1:123456789012:stateMachine:sales-daily-etl-AbC123
        logical_id: SalesDailyEtlStateMachine
    discover: false              # true: also watch every STANDARD state machine (id = its name)
    statuses: [FAILED, TIMED_OUT]   # add ABORTED to report executions a person stopped
    lookback_hours: 24
    history_max_events: 5000
    max_histories_per_poll: 50
    queue_url: "${{HEALER_SFN_QUEUE_URL}}"   # SQS queue of the EventBridge rule (modes: queue)
    log_lookup: true             # add the Lambda's CloudWatch log excerpt to the evidence
    retry_strategy: redrive      # redrive | disabled
    rerun: false                 # true: also offer a new execution with the same input
    code_map: {{}}                 # {{FunctionLogicalId: {{path: functions/x, handler: app.handler}}}}
    links: {{}}                    # overrides of the AWS console link templates

monitor:
  dags: ["*"]                    # pipeline ids (fnmatch; "!" excludes); "pipelines" is an alias
  modes: [poll, queue]           # poll: ListExecutions; queue: SQS; push: /events/eventbridge
  poll_interval_seconds: 60
  trigger: final_failure         # every_failure is not supported (retries are not visible)

contracts:
  dir: healer/contracts          # data contracts, relative to repo.path

# Dataset locators per pipeline and state (`init` fills in the ones it discovered); a path
# may be an s3:// URI. Add the inputs it could not resolve by hand:
# datasets:
#   sales_daily_etl:
#     TransformSales: {{type: csv, path: "s3://acme-landing/incoming/sales.csv"}}
{datasets_block}

"""

_SFN_TAIL = """\
# Standalone host (`dag-healer serve`): the Healer API and UI behind your load balancer.
serve:
  host: 127.0.0.1                # 0.0.0.0 in a container behind the load balancer
  port: 8090
  public_url: null               # e.g. https://healer.example.com; HEALER_PUBLIC_URL overrides it
  auth:
    backend: alb_oidc            # dev | token | trusted_header | alb_oidc
    alb_arn: arn:aws:elasticloadbalancing:eu-west-1:123456789012:loadbalancer/app/healer/abc
    region: eu-west-1
    user_claim: email
    groups_claim: "cognito:groups"
    rules:                       # any matching rule allows; no match answers 403
      - {{principals: ["group:data-eng"], pipelines: ["*"], actions: [read, act, approve_contract]}}
      - {{principals: ["authenticated"], pipelines: ["sales_*"], actions: [read]}}

store:
  url: "${{HEALER_DATABASE_URL}}"  # null: SQLite under $HEALER_HOME (single process only)

ingest:
  token: "${{HEALER_INGEST_TOKEN}}"   # X-Healer-Token of POST /events/eventbridge (API destination)

debug: "${{HEALER_DEBUG:-false}}"
"""

_SFN_TEMPLATE = _SFN_HEAD + _SHARED_SECTIONS + _SFN_TAIL

_TEMPLATES: Mapping[str, str] = types.MappingProxyType({"airflow": _TEMPLATE, "stepfunctions": _SFN_TEMPLATE})


_PLAIN_KEY = re.compile(r"^[A-Za-z_][A-Za-z0-9_.-]*$")


def _yaml_key(value: str) -> str:
    """Render a mapping key plainly when safe, else as a double-quoted string."""
    text = str(value)
    if _PLAIN_KEY.match(text) and text.lower() not in ("true", "false", "null", "yes", "no", "on", "off"):
        return text
    return json.dumps(text, ensure_ascii=False)


def _locator_flow(locator: DatasetLocator | Mapping[str, Any]) -> str:
    """Render a dataset locator as a one-line flow mapping (JSON is valid YAML)."""
    if isinstance(locator, DatasetLocator):
        data: dict[str, Any] = {"type": locator.type, "path": locator.path}
        if locator.options:
            data["options"] = dict(locator.options)
    else:
        data = dict(locator)
    return json.dumps(data, ensure_ascii=False, sort_keys=False, default=str)


def _datasets_block(datasets: Mapping[str, Mapping[str, DatasetLocator | Mapping[str, Any]]] | None) -> str:
    """Render the ``datasets:`` section (``datasets: {}`` when there is nothing to list)."""
    entries = {dag_id: tasks for dag_id, tasks in (datasets or {}).items() if tasks}
    if not entries:
        return "datasets: {}"
    lines = ["datasets:"]
    for dag_id, tasks in entries.items():
        lines.append(f"  {_yaml_key(dag_id)}:")
        for task_id, locator in tasks.items():
            lines.append(f"    {_yaml_key(task_id)}: {_locator_flow(locator)}")
    return "\n".join(lines)


def default_config_yaml(
    *,
    repo_path: str | Path = ".",
    dags_dir: str = "dags",
    tests_dir: str | None = None,
    datasets: Mapping[str, Mapping[str, DatasetLocator | Mapping[str, Any]]] | None = None,
    orchestrator: str = "airflow",
) -> str:
    """Return a commented ``healer.yaml`` template (secrets only as ``${ENV}`` references).

    ``orchestrator`` picks the template (:data:`TEMPLATE_ORCHESTRATORS`): ``"airflow"`` (the
    phase-5 template plus a commented ``orchestrator:`` block) or ``"stepfunctions"`` (the
    AWS Step Functions template of section 9.14 of the design, whose ARNs, region and
    pipeline ids are examples to replace). ``datasets`` (pipeline/DAG id -> step/task id ->
    locator) pre-fills the ``datasets`` section; the default leaves it empty.
    """
    template = _TEMPLATES.get(orchestrator)
    if template is None:
        raise ValueError(
            f"no healer.yaml template for orchestrator {orchestrator!r}; "
            f"expected one of: {', '.join(TEMPLATE_ORCHESTRATORS)}"
        )
    return template.format(
        repo_path=_yaml_str(repo_path),
        dags_dir=_yaml_str(dags_dir),
        tests_dir=_yaml_str(tests_dir),
        datasets_block=_datasets_block(datasets),
    )


__all__ = [
    "INSTANCE_PATTERN",
    "MODE_KINDS",
    "MONITOR_MODES",
    "TEMPLATE_ORCHESTRATORS",
    "AirflowConfig",
    "ClassificationConfig",
    "ConfigError",
    "ContractsConfig",
    "EmailConfig",
    "GithubConfig",
    "HealerConfig",
    "IngestConfig",
    "LimitsConfig",
    "LlmConfig",
    "MaskingConfig",
    "MonitorConfig",
    "NotifyConfig",
    "OrchestratorConfig",
    "RbacRule",
    "RepoConfig",
    "ServeAuthConfig",
    "ServeConfig",
    "StoreConfig",
    "config_candidates",
    "config_from_mapping",
    "default_config_yaml",
    "find_config_file",
    "interpolate_env",
    "load_config",
    "load_config_file",
]
