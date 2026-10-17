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
one, and an empty string otherwise; inside lists such items are dropped.
"""

from __future__ import annotations

import fnmatch
import json
import logging
import os
import re
import types
from collections.abc import Mapping
from pathlib import Path
from typing import Any, Literal, Union, get_args, get_origin
from urllib.parse import urlsplit

import yaml
from pydantic import (
    BaseModel,
    ConfigDict,
    Field,
    PrivateAttr,
    ValidationError,
    field_validator,
    model_validator,
)
from pydantic.fields import FieldInfo

from dag_healer.contracts.schema import DatasetLocator

logger = logging.getLogger(__name__)

CONFIG_ENV = "HEALER_CONFIG"
CONFIG_FILENAME = "healer.yaml"
HEALER_HOME_ENV = "HEALER_HOME"
DATABASE_URL_ENV = "HEALER_DATABASE_URL"
SUPPORTED_VERSION = 1

#: Dotted paths of fields that hold secrets (they should be ``${ENV}`` references).
SECRET_FIELDS: tuple[str, ...] = (
    "airflow.password",
    "notify.email.password",
    "github.token",
    "ingest.token",
)
ALERT_EVENTS: tuple[str, ...] = ("waiting_human", "reported", "repaired", "repair_failed")

_ENV_PATTERN = re.compile(r"\$\$\{|\$\{(?P<name>[A-Za-z_][A-Za-z0-9_]*)(?::-(?P<default>[^}]*))?\}")
_WHOLE_REFERENCE = re.compile(r"^\$\{(?P<name>[A-Za-z_][A-Za-z0-9_]*)\}$")
_GITHUB_REPO = re.compile(r"^[A-Za-z0-9_.-]+/[A-Za-z0-9_.-]+$")
_HOOK = re.compile(r"^[A-Za-z_][\w.]*:[A-Za-z_]\w*$")
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


def _allows_none(annotation: Any) -> bool:
    if annotation is None or annotation is type(None):
        return True
    origin = get_origin(annotation)
    if origin is Union or origin is types.UnionType:
        return type(None) in get_args(annotation)
    return False


def _lookup_field(model: type[BaseModel], key: Any) -> FieldInfo | None:
    fields = model.model_fields
    if key in fields:
        return fields[key]
    for info in fields.values():
        if info.alias == key:
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


class MonitorConfig(_Model):
    """Which DAGs are watched and how failures are detected."""

    dags: list[str] = Field(default_factory=lambda: ["*"])
    modes: list[Literal["rest", "listener", "callback"]] = Field(default_factory=lambda: ["rest", "listener"])
    poll_interval_seconds: int = Field(default=30, ge=1)
    trigger: Literal["final_failure", "every_failure"] = "final_failure"

    @field_validator("dags", "modes", mode="before")
    @classmethod
    def _listify(cls, value: Any) -> Any:
        return _as_list(value)


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


class HealerConfig(_Model):
    """The whole ``healer.yaml``."""

    version: int = SUPPORTED_VERSION
    repo: RepoConfig = Field(default_factory=RepoConfig)
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
    store: StoreConfig = Field(default_factory=StoreConfig)
    ingest: IngestConfig = Field(default_factory=IngestConfig)
    debug: bool = False

    _source_path: Path | None = PrivateAttr(default=None)
    _env_refs: frozenset[str] = PrivateAttr(default_factory=frozenset)

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

    def secret_literals(self) -> list[str]:
        """Dotted paths of secret fields set to literal values instead of ``${ENV}`` references."""
        literals = [path for path in SECRET_FIELDS if _get_path(self, path) and path not in self._env_refs]
        url = self.store.url
        if url and "store.url" not in self._env_refs and _url_has_password(url):
            literals.append("store.url")
        return literals


def _get_path(model: BaseModel, dotted: str) -> Any:
    value: Any = model
    for part in dotted.split("."):
        if value is None:
            return None
        value = getattr(value, part, None)
    return value


def _url_has_password(url: str) -> bool:
    try:
        return bool(urlsplit(url).password)
    except ValueError:
        return False


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

    ``repo.path`` is resolved against ``base_dir`` when relative.
    """
    refs: set[str] = set()
    interpolated = _interpolate_tree(dict(data), "", refs, env)
    source = Path(source_path) if source_path is not None else None
    try:
        config = HealerConfig.model_validate(interpolated)
    except ValidationError as exc:
        raise ConfigError(_format_validation_error(exc, source)) from None
    config._env_refs = frozenset(refs)
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


_TEMPLATE = """\
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

store:
  url: "${{HEALER_DATABASE_URL}}"  # null: SQLite under $HEALER_HOME (single process only)

ingest:
  token: "${{HEALER_INGEST_TOKEN}}"   # shared secret required by the listener / callback endpoint
  api_url: null                  # where task runners reach the api-server (null: autodetect)

debug: "${{HEALER_DEBUG:-false}}"
"""


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
) -> str:
    """Return a commented ``healer.yaml`` template (secrets only as ``${ENV}`` references).

    ``datasets`` (dag_id -> task_id -> locator) pre-fills the ``datasets`` section; the
    default leaves it empty.
    """
    return _TEMPLATE.format(
        repo_path=_yaml_str(repo_path),
        dags_dir=_yaml_str(dags_dir),
        tests_dir=_yaml_str(tests_dir),
        datasets_block=_datasets_block(datasets),
    )


__all__ = [
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
    "RepoConfig",
    "StoreConfig",
    "config_candidates",
    "config_from_mapping",
    "default_config_yaml",
    "find_config_file",
    "interpolate_env",
    "load_config",
    "load_config_file",
]
