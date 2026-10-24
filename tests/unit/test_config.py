"""Tests for dag_healer.config: models, interpolation, resolution order and the template."""

from __future__ import annotations

import textwrap
from pathlib import Path
from typing import Any, get_args

import pytest
import yaml

from dag_healer.config import (
    ConfigError,
    HealerConfig,
    SandboxConfig,
    config_candidates,
    config_from_mapping,
    default_config_yaml,
    interpolate_env,
    load_config,
)
from dag_healer.contracts.schema import DatasetLocator


def write_yaml(path: Path, text: str) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(textwrap.dedent(text), encoding="utf-8")
    return path


def branch_config(path: Path, branch: str) -> Path:
    """A config file whose repo.base_branch identifies which file was loaded."""
    return write_yaml(path, f"repo:\n  base_branch: {branch}\n")


@pytest.fixture
def isolated(healer_env: Path, monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> Path:
    """Clean environment and a fresh working directory; returns the working directory."""
    for name in (
        "HEALER_AIRFLOW_USERNAME",
        "HEALER_AIRFLOW_PASSWORD",
        "HEALER_SMTP_HOST",
        "HEALER_SMTP_PORT",
        "HEALER_SMTP_FROM",
        "HEALER_ALERT_TO",
        "HEALER_SMTP_STARTTLS",
        "HEALER_SMTP_USERNAME",
        "HEALER_SMTP_PASSWORD",
        "HEALER_LLM_PROVIDER",
        "HEALER_SANDBOX_IMAGE",
        "GITHUB_REPO",
        "GITHUB_TOKEN",
    ):
        monkeypatch.delenv(name, raising=False)
    cwd = tmp_path / "cwd"
    cwd.mkdir()
    monkeypatch.chdir(cwd)
    return cwd


# --------------------------------------------------------------------------- defaults
def test_defaults_match_contract() -> None:
    cfg = HealerConfig()
    assert cfg.version == 1
    assert cfg.repo.path == Path(".")
    assert cfg.repo.base_branch == "main"
    assert cfg.repo.dags_dir == "dags"
    assert cfg.repo.tests_dir is None
    assert cfg.repo.test_command == "python -m pytest -q"
    assert cfg.repo.allowed_paths == ["src/", "dags/"]
    assert cfg.repo.pytest_timeout == 600
    assert cfg.repo.sandbox == SandboxConfig()
    assert cfg.airflow.api_url == "http://localhost:8080"
    assert cfg.airflow.username is None and cfg.airflow.password is None
    assert cfg.monitor.dags == ["*"]
    assert cfg.monitor.modes == ["rest", "listener"]
    assert cfg.monitor.poll_interval_seconds == 30
    assert cfg.monitor.trigger == "final_failure"
    assert cfg.contracts.dir == "healer/contracts"
    assert cfg.datasets == {}
    assert cfg.classification.min_confidence == 0.75
    assert cfg.notify.email is None
    assert cfg.notify.events == ["waiting_human", "reported", "repaired", "repair_failed"]
    assert (cfg.github.repo, cfg.github.pr_base, cfg.github.token) == (None, "main", None)
    assert (cfg.llm.provider, cfg.llm.model, cfg.llm.effort, cfg.llm.max_tokens) == (
        "anthropic",
        "claude-opus-5-5",
        "high",
        16000,
    )
    assert cfg.limits.model_dump() == {
        "max_fix_attempts": 3,
        "max_incidents_per_hour": 10,
        "max_tokens_per_incident": 400000,
        "max_diff_files": 10,
        "max_diff_lines": 400,
    }
    assert cfg.masking.model_dump() == {
        "default": "hash",
        "columns": [],
        "allow": [],
        "hook": None,
        "sample_rows": 5,
    }
    assert cfg.store.url is None
    assert cfg.ingest.token is None and cfg.ingest.api_url is None
    assert cfg.debug is False
    assert cfg.source_path is None


def test_load_config_without_any_file_returns_defaults_rooted_at_cwd(isolated: Path) -> None:
    cfg = load_config()
    assert cfg.source_path is None
    assert cfg.repo_root == isolated.resolve()
    assert cfg.contracts_dir == (isolated / "healer" / "contracts").resolve()


def test_empty_file_and_empty_sections_use_defaults(isolated: Path) -> None:
    write_yaml(isolated / "healer.yaml", "")
    assert load_config().repo.base_branch == "main"
    write_yaml(isolated / "healer.yaml", "repo:\nmonitor:\nnotify:\n")
    cfg = load_config()
    assert cfg.repo.dags_dir == "dags"
    assert cfg.monitor.dags == ["*"]


# --------------------------------------------------------------------------- resolution
def test_resolution_order_env_then_airflow_home_then_cwd(
    isolated: Path, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    env_file = branch_config(tmp_path / "explicit" / "custom.yaml", "from-env")
    airflow_home = tmp_path / "airflow_home"
    branch_config(airflow_home / "healer.yaml", "from-airflow-home")
    branch_config(isolated / "healer.yaml", "from-cwd")

    monkeypatch.setenv("HEALER_CONFIG", str(env_file))
    monkeypatch.setenv("AIRFLOW_HOME", str(airflow_home))
    cfg = load_config()
    assert cfg.repo.base_branch == "from-env"
    assert cfg.source_path == env_file.resolve()

    monkeypatch.delenv("HEALER_CONFIG")
    assert load_config().repo.base_branch == "from-airflow-home"

    monkeypatch.delenv("AIRFLOW_HOME")
    cfg = load_config()
    assert cfg.repo.base_branch == "from-cwd"
    assert cfg.source_path == (isolated / "healer.yaml").resolve()


def test_healer_config_may_point_to_a_directory(
    isolated: Path, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    branch_config(tmp_path / "conf" / "healer.yaml", "from-dir")
    monkeypatch.setenv("HEALER_CONFIG", str(tmp_path / "conf"))
    assert load_config().repo.base_branch == "from-dir"


def test_missing_healer_config_falls_through_then_defaults_to_its_directory(
    isolated: Path, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    missing = tmp_path / "repo" / "healer.yaml"
    missing.parent.mkdir()
    monkeypatch.setenv("HEALER_CONFIG", str(missing))
    airflow_home = tmp_path / "airflow_home"
    branch_config(airflow_home / "healer.yaml", "from-airflow-home")
    monkeypatch.setenv("AIRFLOW_HOME", str(airflow_home))
    assert load_config().repo.base_branch == "from-airflow-home"

    monkeypatch.delenv("AIRFLOW_HOME")
    cfg = load_config()
    assert cfg.source_path is None
    assert cfg.repo_root == missing.parent.resolve()
    assert config_candidates()[0] == missing


def test_explicit_missing_path_raises(isolated: Path) -> None:
    with pytest.raises(ConfigError, match="not found"):
        load_config(isolated / "nope.yaml")


def test_explicit_path_wins_over_environment(
    isolated: Path, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setenv("HEALER_CONFIG", str(branch_config(tmp_path / "a.yaml", "from-env")))
    explicit = branch_config(tmp_path / "b.yaml", "from-arg")
    assert load_config(explicit).repo.base_branch == "from-arg"
    assert load_config(str(explicit)).source_path == explicit.resolve()


# --------------------------------------------------------------------------- repo path
def test_relative_repo_path_is_resolved_against_the_yaml_directory(isolated: Path, tmp_path: Path) -> None:
    config_file = write_yaml(tmp_path / "configs" / "healer.yaml", "repo:\n  path: ../my-repo\n")
    cfg = load_config(config_file)
    assert cfg.repo.path == (tmp_path / "my-repo").resolve()
    assert cfg.repo_root == (tmp_path / "my-repo").resolve()
    assert cfg.contracts_dir == (tmp_path / "my-repo" / "healer" / "contracts").resolve()
    assert cfg.dags_path == (tmp_path / "my-repo" / "dags").resolve()


def test_default_repo_path_is_the_yaml_directory(isolated: Path, tmp_path: Path) -> None:
    config_file = write_yaml(tmp_path / "repo" / "healer.yaml", "debug: true\n")
    cfg = load_config(config_file)
    assert cfg.repo_root == (tmp_path / "repo").resolve()
    assert cfg.debug is True


def test_absolute_repo_path_is_kept(isolated: Path, tmp_path: Path) -> None:
    target = (tmp_path / "elsewhere").resolve()
    config_file = write_yaml(tmp_path / "c" / "healer.yaml", f"repo:\n  path: '{target.as_posix()}'\n")
    assert load_config(config_file).repo_root == target


def test_absolute_contracts_dir_is_kept(tmp_path: Path) -> None:
    target = (tmp_path / "contracts").resolve()
    cfg = config_from_mapping({"contracts": {"dir": target.as_posix()}}, base_dir=tmp_path)
    assert cfg.contracts_dir == target


# --------------------------------------------------------------------------- interpolation
@pytest.mark.parametrize(
    ("value", "expected"),
    [
        ("${A}", "alpha"),
        ("x-${A}-y", "x-alpha-y"),
        ("${MISSING:-fallback}", "fallback"),
        ("${EMPTY:-fallback}", "fallback"),
        ("${A:-fallback}", "alpha"),
        ("${MISSING:-}", ""),
        ("pre-${MISSING}-post", "pre--post"),
        ("$${A}", "${A}"),
        ("no refs $ here", "no refs $ here"),
        ("${A}${B}", "alphabeta"),
        ("${MISSING:-http://host:8080/x}", "http://host:8080/x"),
    ],
)
def test_interpolate_env(value: str, expected: str) -> None:
    env = {"A": "alpha", "B": "beta", "EMPTY": ""}
    assert interpolate_env(value, env) == expected


def test_whole_unset_reference_becomes_none_default_or_empty(
    isolated: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setenv("SET_TOKEN", "s3cr3t")
    monkeypatch.setenv("EMPTY_VAR", "")
    data = {
        "airflow": {
            "api_url": "${UNSET_URL}",  # has a default -> default
            "username": "${UNSET_USER}",  # optional -> None
            "password": "${EMPTY_VAR}",  # empty counts as unset -> None
        },
        "repo": {"base_branch": "${UNSET_BRANCH}", "allowed_paths": ["src/", "${UNSET_PATH}"]},
        "github": {"token": "${SET_TOKEN}", "repo": "${UNSET_REPO}"},
        "monitor": {"dags": ["${UNSET_DAG}", "sales_*"]},
        "llm": {"provider": "${UNSET_PROVIDER}"},
        "notify": {
            "email": {
                "host": "smtp.local",
                "from": "${UNSET_FROM}",  # required, no default -> ""
                "to": ["${UNSET_TO}", "a@example.com"],
            }
        },
    }
    cfg = config_from_mapping(data)
    assert cfg.airflow.api_url == "http://localhost:8080"
    assert cfg.airflow.username is None
    assert cfg.airflow.password is None
    assert cfg.repo.base_branch == "main"
    assert cfg.repo.allowed_paths == ["src/"]
    assert cfg.github.token == "s3cr3t"
    assert cfg.github.repo is None
    assert cfg.monitor.dags == ["sales_*"]
    assert cfg.llm.provider == "anthropic"
    assert cfg.notify.email is not None
    assert cfg.notify.email.sender == ""
    assert cfg.notify.email.to == ["a@example.com"]


def test_interpolated_values_are_coerced(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("PORT", "2525")
    monkeypatch.setenv("TLS", "true")
    data = {
        "notify": {
            "email": {
                "host": "${HOST:-mail}",
                "port": "${PORT}",
                "from": "x@example.com",
                "to": "${TO:-a@example.com, b@example.com}",
                "starttls": "${TLS}",
            }
        },
        "debug": "${DEBUG:-false}",
    }
    cfg = config_from_mapping(data)
    email = cfg.notify.email
    assert email is not None
    assert (email.host, email.port, email.starttls) == ("mail", 2525, True)
    assert email.to == ["a@example.com", "b@example.com"]
    assert cfg.debug is False


def test_email_block_without_host_disables_email() -> None:
    cfg = config_from_mapping({"notify": {"email": {"host": "${NO_SMTP_HOST_HERE}", "from": "a", "to": []}}})
    assert cfg.notify.email is None


def test_from_alias_and_field_name_both_work() -> None:
    by_alias = config_from_mapping({"notify": {"email": {"host": "h", "from": "me@x.io", "to": ["y@x.io"]}}})
    assert by_alias.notify.email is not None and by_alias.notify.email.sender == "me@x.io"
    by_name = config_from_mapping({"notify": {"email": {"host": "h", "sender": "me@x.io", "to": ["y@x.io"]}}})
    assert by_name.notify.email is not None and by_name.notify.email.sender == "me@x.io"


# --------------------------------------------------------------------------- validation
def test_unknown_keys_are_rejected_with_their_path() -> None:
    with pytest.raises(ConfigError) as excinfo:
        config_from_mapping({"repo": {"pathh": "."}, "colour": "blue"})
    message = str(excinfo.value)
    assert "repo.pathh: unknown key" in message
    assert "colour: unknown key" in message


def test_x_prefixed_top_level_keys_are_allowed() -> None:
    cfg = config_from_mapping({"x-notes": {"owner": "data team"}, "x-ticket": "DATA-1"})
    assert cfg.repo.base_branch == "main"
    with pytest.raises(ConfigError, match="repo.x-notes"):
        config_from_mapping({"repo": {"x-notes": "only top level"}})


def test_invalid_values_raise_config_error_with_source(isolated: Path) -> None:
    bad = write_yaml(isolated / "healer.yaml", "monitor:\n  trigger: sometimes\n")
    with pytest.raises(ConfigError) as excinfo:
        load_config()
    assert str(bad.resolve()) in str(excinfo.value)
    assert "monitor.trigger" in str(excinfo.value)


@pytest.mark.parametrize(
    "text",
    ["repo: [unclosed\n", "- just\n- a list\n", "version: 2\n"],
)
def test_bad_files_raise_config_error(isolated: Path, text: str) -> None:
    path = write_yaml(isolated / "healer.yaml", text)
    with pytest.raises(ConfigError):
        load_config(path)


def test_github_repo_normalisation_and_hook_format() -> None:
    cfg = config_from_mapping({"github": {"repo": "https://github.com/acme/etl.git"}})
    assert cfg.github.repo == "acme/etl"
    assert config_from_mapping({"github": {"repo": "git@github.com:acme/etl.git"}}).github.repo == "acme/etl"
    with pytest.raises(ConfigError, match="github.repo"):
        config_from_mapping({"github": {"repo": "not a repo"}})
    assert config_from_mapping({"masking": {"hook": "acme.masks:mask"}}).masking.hook == "acme.masks:mask"
    with pytest.raises(ConfigError, match="masking.hook"):
        config_from_mapping({"masking": {"hook": "acme.masks.mask"}})


# --------------------------------------------------------------------------- repo.sandbox
def test_sandbox_defaults_and_dumps() -> None:
    sandbox = HealerConfig().repo.sandbox
    assert sandbox.model_dump() == {
        "backend": "container",
        "engine": "docker",
        "image": None,
        "python": "python",
        "user": None,
        "memory": "2g",
        "cpus": 2.0,
        "pids": 512,
        "work_size": "1g",
        "tmp_size": "512m",
    }
    # a default sandbox is left out of dumps (existing dumps stay the same), another one is shown whole
    assert "sandbox" not in HealerConfig().model_dump(mode="json")["repo"]
    custom = config_from_mapping({"repo": {"sandbox": {"backend": "process"}}})
    assert custom.model_dump(mode="json")["repo"]["sandbox"] == {
        **SandboxConfig().model_dump(mode="json"),
        "backend": "process",
    }
    assert custom.repo.model_dump(exclude={"sandbox"}) == HealerConfig().repo.model_dump()


def test_a_container_sandbox_without_an_image_loads() -> None:
    sandbox = config_from_mapping({"repo": {"sandbox": {"backend": "container", "image": "${NO_IMAGE}"}}})
    assert sandbox.repo.sandbox == SandboxConfig()  # it is reported when a repair needs it, not here


def test_sandbox_settings_are_normalised() -> None:
    raw: dict[str, Any] = {
        "image": "  ",
        "user": " 1000:0 ",
        "python": " ",
        "memory": " 512M ",
        "work_size": 1048576,
        "cpus": None,
    }
    sandbox = config_from_mapping({"repo": {"sandbox": raw}}).repo.sandbox
    assert (sandbox.image, sandbox.user, sandbox.python) == (None, "1000:0", "python")
    assert (sandbox.memory, sandbox.work_size, sandbox.cpus) == ("512m", "1048576", None)
    image = "registry.example.com:5000/etl-tests@sha256:" + "a" * 64
    assert config_from_mapping({"repo": {"sandbox": {"image": image}}}).repo.sandbox.image == image


@pytest.mark.parametrize(
    ("settings", "message"),
    [
        ({"user": "root"}, "user: Value error, the sandbox never runs as root"),
        ({"user": "0"}, "user: Value error, the sandbox never runs as root"),
        ({"user": "0:0"}, "user: Value error, the sandbox never runs as root"),
        ({"user": "root:staff"}, "user: Value error, the sandbox never runs as root"),
        ({"user": "-u"}, "user: Value error, expected a user name or uid"),
        ({"image": "--privileged"}, "image: Value error, '--privileged' is not an image reference"),
        ({"image": "my image"}, "image: Value error, 'my image' is not an image reference"),
        ({"python": "-x"}, "python: Value error, expected the name or the path of the Python interpreter"),
        ({"python": "python;id"}, "python: Value error, expected the name or the path of the Python"),
        ({"memory": "2gb"}, "memory: Value error, expected a size such as 512m or 2g"),
        ({"tmp_size": "-1m"}, "tmp_size: Value error, expected a size such as 512m or 2g"),
        ({"cpus": 0}, "cpus: Input should be greater than 0"),
        ({"pids": 1}, "pids: Input should be greater than or equal to 16"),
        ({"backend": "vm"}, "backend: Input should be 'container' or 'process'"),
        ({"engine": "lxc"}, "engine: Input should be 'docker' or 'podman'"),
        ({"network": "host"}, "network: unknown key"),
    ],
)
def test_invalid_sandbox_settings_are_rejected(settings: dict[str, Any], message: str) -> None:
    with pytest.raises(ConfigError) as excinfo:
        config_from_mapping({"repo": {"sandbox": settings}})
    assert f"  - repo.sandbox.{message}" in str(excinfo.value)


def test_the_sandbox_values_are_the_ones_the_sandbox_module_implements() -> None:
    from dag_healer.repo import sandbox

    fields = SandboxConfig.model_fields
    assert get_args(fields["backend"].annotation) == sandbox.SANDBOX_BACKENDS
    assert get_args(fields["engine"].annotation) == sandbox.SANDBOX_ENGINES


def test_datasets_are_parsed_into_locators() -> None:
    cfg = config_from_mapping(
        {"datasets": {"sales": {"load": {"type": "csv", "path": "./data/in.csv", "options": {"sep": ";"}}}}}
    )
    locator = cfg.datasets["sales"]["load"]
    assert isinstance(locator, DatasetLocator)
    assert locator.type == "csv"
    assert locator.options == {"sep": ";"}
    assert cfg.dataset_locators("sales") == {"load": locator}
    assert cfg.dataset_locators("other") == {}


# --------------------------------------------------------------------------- helpers
@pytest.mark.parametrize(
    ("patterns", "dag_id", "expected"),
    [
        (["*"], "anything", True),
        (["sales_*"], "sales_daily_etl", True),
        (["sales_*"], "customers_sync", False),
        (["sales_*", "customers_sync"], "customers_sync", True),
        (["Sales_*"], "sales_daily_etl", False),  # case-sensitive on every OS
        (["*", "!tmp_*"], "tmp_scratch", False),
        (["*", "!tmp_*"], "sales_daily_etl", True),
        (["!tmp_*"], "sales_daily_etl", True),
        ([], "sales_daily_etl", False),
    ],
)
def test_is_monitored(patterns: list[str], dag_id: str, expected: bool) -> None:
    cfg = config_from_mapping({"monitor": {"dags": patterns}})
    assert cfg.is_monitored(dag_id) is expected


def test_monitor_dags_accepts_a_single_string() -> None:
    assert config_from_mapping({"monitor": {"dags": "sales_*"}}).monitor.dags == ["sales_*"]


def test_healer_home_resolution(healer_env: Path, monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    cfg = HealerConfig()
    assert cfg.healer_home == healer_env
    monkeypatch.delenv("HEALER_HOME")
    monkeypatch.setenv("AIRFLOW_HOME", str(tmp_path / "af"))
    assert cfg.healer_home == tmp_path / "af" / "healer"
    monkeypatch.delenv("AIRFLOW_HOME")
    assert cfg.healer_home == Path.home() / ".dag-healer"


def test_database_url_resolution(healer_env: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    cfg = HealerConfig()
    assert cfg.database_url == f"sqlite:///{(healer_env / 'healer.db').resolve().as_posix()}"
    monkeypatch.setenv("HEALER_DATABASE_URL", "postgresql+psycopg2://u:p@db/healer")
    assert cfg.database_url == "postgresql+psycopg2://u:p@db/healer"
    explicit = config_from_mapping({"store": {"url": "sqlite:///explicit.db"}})
    assert explicit.database_url == "sqlite:///explicit.db"
    from_env_ref = config_from_mapping({"store": {"url": "${HEALER_DATABASE_URL}"}})
    assert from_env_ref.database_url == "postgresql+psycopg2://u:p@db/healer"


def test_secret_literals(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("TOKEN_FROM_ENV", "abc")
    cfg = config_from_mapping(
        {
            "airflow": {"password": "hunter2"},
            "github": {"token": "${TOKEN_FROM_ENV}"},
            "ingest": {"token": "plain-ingest-token"},
            "store": {"url": "postgresql://user:pw@db/healer"},
            "notify": {"email": {"host": "h", "from": "a@b.c", "to": ["d@e.f"], "password": "${NOPE}"}},
        }
    )
    assert cfg.secret_literals() == ["airflow.password", "ingest.token", "store.url"]
    assert cfg.uses_env_reference("github.token")
    clean = config_from_mapping(
        {"store": {"url": "sqlite:///x.db"}, "github": {"token": "${TOKEN_FROM_ENV}"}}
    )
    assert clean.secret_literals() == []


# --------------------------------------------------------------------------- template
def test_default_template_round_trips(isolated: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    text = default_config_yaml(tests_dir="tests")
    assert "\n#" in text  # commented
    path = isolated / "healer.yaml"
    path.write_text(text, encoding="utf-8")
    cfg = load_config()
    assert cfg.repo_root == isolated.resolve()
    assert cfg.repo.tests_dir == "tests"
    assert cfg.notify.email is None  # no SMTP host in the environment
    assert cfg.secret_literals() == []
    assert cfg.store.url is None and cfg.github.token is None and cfg.ingest.token is None
    assert cfg.repo.sandbox == SandboxConfig()  # no HEALER_SANDBOX_IMAGE in the environment
    assert cfg.model_dump(exclude={"repo"}) == HealerConfig().model_dump(exclude={"repo"})

    monkeypatch.setenv("HEALER_SMTP_HOST", "mailpit")
    monkeypatch.setenv("HEALER_SMTP_PORT", "1025")
    monkeypatch.setenv("HEALER_API_URL", "http://airflow-apiserver:8080")
    monkeypatch.setenv("HEALER_INGEST_TOKEN", "tok")
    monkeypatch.setenv("HEALER_LLM_PROVIDER", "fake")
    cfg = load_config()
    assert cfg.notify.email is not None
    assert (cfg.notify.email.host, cfg.notify.email.port) == ("mailpit", 1025)
    assert cfg.notify.email.sender == "dag-healer@localhost"
    assert cfg.notify.email.to == ["data-team@example.com"]
    assert cfg.airflow.api_url == "http://airflow-apiserver:8080"
    assert cfg.ingest.token == "tok"
    assert cfg.llm.provider == "fake"
    assert cfg.secret_literals() == []


@pytest.mark.parametrize("orchestrator", ["airflow", "stepfunctions"])
def test_the_templates_document_the_sandbox(orchestrator: str) -> None:
    text = default_config_yaml(orchestrator=orchestrator)
    lines = text.splitlines()
    start = lines.index("  pytest_timeout: 600            # seconds per pytest run") + 1
    block = lines[start : lines.index("", start)]  # up to the next blank line
    assert block[0].startswith("  # Where pytest runs the repository's tests")
    assert block[3] == "  sandbox:" and all(line.startswith("    ") for line in block[4:])
    assert {line.split(":")[0].strip() for line in block[4:]} == set(SandboxConfig.model_fields)
    raw = yaml.safe_load(text)
    assert raw["repo"]["sandbox"]["image"] == "${HEALER_SANDBOX_IMAGE}"
    assert config_from_mapping(raw, env={}).repo.sandbox == SandboxConfig()  # exactly the defaults
    image = {"HEALER_SANDBOX_IMAGE": "registry.example.com/etl-tests:1.4"}
    sandbox = config_from_mapping(raw, env=image).repo.sandbox
    assert sandbox == SandboxConfig(image="registry.example.com/etl-tests:1.4")


def test_template_parameters_are_quoted(tmp_path: Path) -> None:
    text = default_config_yaml(repo_path="..\\my repo", dags_dir="airflow/dags", tests_dir=None)
    raw = yaml.safe_load(text)
    assert raw["repo"]["path"] == "../my repo"
    assert raw["repo"]["dags_dir"] == "airflow/dags"
    assert raw["repo"]["tests_dir"] is None
    cfg = config_from_mapping(raw, base_dir=tmp_path / "cfg")
    assert cfg.repo_root == (tmp_path / "my repo").resolve()
