"""Tests for dag_healer.config: models, interpolation, resolution order and the template."""

from __future__ import annotations

import json
import textwrap
from pathlib import Path
from typing import Any, get_args

import pytest
import yaml
from pydantic import ValidationError

from dag_healer.config import (
    ConfigError,
    GithubConfig,
    HealerConfig,
    LlmConfig,
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
        "HEALER_LLM_MODEL",
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


FICTIONAL_TOKEN = "ghp_FICTIONAL0123456789abcdefFICTIONAL"


@pytest.mark.parametrize(
    ("line", "where"),
    [
        pytest.param(f"\ttoken: {FICTIONAL_TOKEN}", "line 4, column 1: found character '\\t'", id="tab"),
        pytest.param(
            f'  token: "{FICTIONAL_TOKEN}""', "line 5, column 1: found unexpected end of stream", id="quote"
        ),
        pytest.param(
            f"  token: *{FICTIONAL_TOKEN}", "line 4, column 10: found undefined alias '...'", id="alias"
        ),
        pytest.param(f"  token: !{FICTIONAL_TOKEN} x", "line 4, column 10: could not determine", id="tag"),
    ],
)
def test_invalid_yaml_never_quotes_the_file(isolated: Path, line: str, where: str) -> None:
    """PyYAML's message quotes the line it stopped at, a literal token included: only where and why stay."""
    path = write_yaml(isolated / "healer.yaml", f"version: 1\ngithub:\n  repo: acme-health/etl\n{line}\n")
    with pytest.raises(ConfigError) as excinfo:
        load_config(path)
    message = str(excinfo.value)
    assert message.startswith(f"Invalid YAML in {path.resolve()}, {where}")
    assert "FICTIONAL" not in message and "ghp_" not in message
    assert "\n" not in message


@pytest.mark.parametrize(
    ("entry", "reason"),
    [
        pytest.param(
            ".github/", "protected path: healer config, contracts, CI, data or fixtures", id="github"
        ),
        pytest.param(
            ".GitHub/workflows", "protected path: healer config, contracts, CI, data or fixtures", id="case"
        ),
        pytest.param("data", "protected path: healer config, contracts, CI, data or fixtures", id="data"),
        pytest.param(
            "healer/contracts", "protected path: healer config, contracts, CI, data or fixtures", id="healer"
        ),
        pytest.param(".circleci", "CI configuration", id="circleci"),
        pytest.param("Jenkinsfile", "CI configuration", id="jenkins"),
        pytest.param("./.husky/", "git hook configuration", id="husky"),
        pytest.param(".gitmodules", "git metadata", id="gitmodules"),
        pytest.param("CODEOWNERS", "code owners", id="codeowners"),
        pytest.param(".git/hooks", ".git internals", id="git"),
        pytest.param("src\\.env", ".env files", id="env"),
    ],
)
def test_allowed_paths_cannot_name_a_protected_path(entry: str, reason: str) -> None:
    with pytest.raises(ConfigError) as excinfo:
        config_from_mapping({"repo": {"allowed_paths": ["src/", entry]}})
    assert (
        f"  - repo.allowed_paths: Value error, {entry!r} cannot be an allowed path ({reason}); a fix never "
        "changes it. '.' allows every path that is not protected"
    ) in str(excinfo.value)


@pytest.mark.parametrize(
    "entry",
    [
        pytest.param(".", id="dot"),
        pytest.param("./", id="dot-slash"),
        pytest.param("/", id="slash"),
        pytest.param("src/", id="src"),
        pytest.param("docs/", id="docs"),
        pytest.param("src/.github", id="nested-github"),
        pytest.param("  ", id="blank"),
    ],
)
def test_allowed_paths_that_are_not_protected_are_kept_as_written(entry: str) -> None:
    assert config_from_mapping({"repo": {"allowed_paths": [entry, "dags/"]}}).repo.allowed_paths == [
        entry,
        "dags/",
    ]


# --------------------------------------------------------------------------- github
def test_github_skip_ci_defaults_to_true_and_is_dumped_only_when_false() -> None:
    cfg = HealerConfig()
    assert cfg.github.skip_ci is True
    assert "skip_ci" not in cfg.model_dump()["github"]
    assert "skip_ci" not in json.loads(cfg.model_dump_json())["github"]
    off = config_from_mapping({"github": {"skip_ci": False}})
    assert off.model_dump(mode="json")["github"] == {
        "repo": None,
        "pr_base": "main",
        "token": None,
        "skip_ci": False,
    }
    assert config_from_mapping(off.model_dump(mode="json")).github.skip_ci is False  # the dump loads back


@pytest.mark.parametrize(
    ("value", "expected"),
    [
        pytest.param(False, False, id="false"),
        pytest.param("false", False, id="text-false"),
        pytest.param("${HEALER_SKIP_CI:-false}", False, id="env-fallback"),
        pytest.param(True, True, id="true"),
        pytest.param("${HEALER_SKIP_CI}", True, id="env-unset"),
        pytest.param(None, True, id="empty"),
    ],
)
def test_github_skip_ci_values(value: Any, expected: bool) -> None:
    assert config_from_mapping({"github": {"skip_ci": value}}, env={}).github.skip_ci is expected


def test_github_skip_ci_must_be_a_boolean() -> None:
    with pytest.raises(ConfigError, match="github.skip_ci: Input should be a valid boolean"):
        config_from_mapping({"github": {"skip_ci": "sometimes"}})


@pytest.mark.parametrize("orchestrator", ["airflow", "stepfunctions"])
def test_the_templates_document_github_skip_ci(orchestrator: str) -> None:
    text = default_config_yaml(orchestrator=orchestrator)
    block = text.split("\ngithub:\n", 1)[1].split("\n\n", 1)[0]
    assert block.splitlines() == [
        '  repo: "${GITHUB_REPO}"         # owner/name; without repo and token fixes are written as PR.md',
        "  pr_base: main",
        '  token: "${GITHUB_TOKEN}"',
        "  # true: the fix's commit carries [skip ci], so push and pull_request workflows run on the",
        "  # healer branch only after a person has reviewed it; false only when CI on healer branches",
        "  # holds no secrets. See docs/github/README.md for the token, the base branch's rules and CI.",
        "  skip_ci: true",
    ]
    raw = yaml.safe_load(text)
    assert raw["github"]["skip_ci"] is True
    cfg = config_from_mapping(raw, env={})
    assert cfg.github.skip_ci is True and "skip_ci" not in cfg.model_dump()["github"]


@pytest.mark.parametrize(
    ("contracts", "entry"),
    [
        pytest.param("contracts", "contracts/", id="folder"),
        pytest.param("./contracts/", "contracts/orders", id="inside"),
        pytest.param("src/contracts", "SRC/Contracts", id="case"),
    ],
)
def test_allowed_paths_cannot_name_the_contracts_folder(contracts: str, entry: str) -> None:
    with pytest.raises(ConfigError) as excinfo:
        config_from_mapping({"repo": {"allowed_paths": ["src/", entry]}, "contracts": {"dir": contracts}})
    assert (
        f"repo.allowed_paths: {entry!r} cannot be an allowed path (it is the contracts folder of "
        f"contracts.dir, {contracts!r}, or inside it); a fix never changes it."
    ) in str(excinfo.value)


def test_an_absolute_contracts_folder_is_compared_with_an_absolute_repository(tmp_path: Path) -> None:
    repo = tmp_path / "repo"
    (repo / "contracts").mkdir(parents=True)
    data = {
        "repo": {"path": str(repo), "allowed_paths": ["contracts/"]},
        "contracts": {"dir": str(repo / "contracts")},
    }
    with pytest.raises(ConfigError, match="cannot be an allowed path"):
        config_from_mapping(data)
    outside = {**data, "contracts": {"dir": str(tmp_path / "shared" / "contracts")}}
    assert config_from_mapping(outside).repo.allowed_paths == ["contracts/"]
    whole = {**data, "repo": {"path": str(repo), "allowed_paths": ["."]}}
    assert config_from_mapping(whole).repo.allowed_paths == ["."]  # the folder stays protected inside


@pytest.mark.parametrize(
    ("value", "expected"),
    [
        pytest.param("github_pat_TEST\n", "github_pat_TEST", id="lf"),
        pytest.param("ghp_TEST\r\n", "ghp_TEST", id="crlf"),
        pytest.param("  ghs_TEST\t", "ghs_TEST", id="spaces"),
        pytest.param("   ", None, id="blank"),
        pytest.param("", None, id="empty"),
    ],
)
def test_github_token_loses_the_whitespace_around_it(value: str, expected: str | None) -> None:
    assert config_from_mapping({"github": {"token": value}}).github.token == expected


@pytest.mark.parametrize(
    "value",
    [
        pytest.param("ghp_TE\nST", id="lf-inside"),
        pytest.param("ghp_TE\rST", id="cr-inside"),
        pytest.param("ghp_TE ST", id="space-inside"),
        pytest.param("ghp_T\u00c9ST", id="non-ascii"),
        pytest.param("ghp_TEST\x00", id="nul"),
    ],
)
def test_github_token_must_be_printable_ascii(value: str) -> None:
    with pytest.raises(ConfigError) as excinfo:
        config_from_mapping({"github": {"token": value}})
    message = str(excinfo.value)
    assert "github.token: Value error, the token holds a space, a control character or a non-ASCII" in message
    assert "ghp_" not in message and "ST" not in message.split("github.token:", 1)[1]  # never echoed


@pytest.mark.parametrize(
    ("data", "where"),
    [
        pytest.param({"github": {"token": "ghp_SECRET value"}}, "github.token", id="token"),
        pytest.param({"airflow": {"password": ["SECRET-password"]}}, "airflow.password", id="password"),
        pytest.param({"ingest": {"token": {"value": "SECRET-token"}}}, "ingest.token", id="ingest"),
        pytest.param({"github": {"tokn": "ghp_SECRET"}}, "github.tokn", id="unknown-key"),
    ],
)
def test_a_rejected_value_never_appears_in_the_validation_error(data: dict[str, Any], where: str) -> None:
    """pydantic writes the rejected value into the text of its error unless the model hides it."""
    with pytest.raises(ValidationError) as excinfo:
        HealerConfig.model_validate(data)
    text = f"{excinfo.value}\n{excinfo.value!r}"
    assert where in text and "SECRET" not in text
    with pytest.raises(ConfigError) as loaded:
        config_from_mapping(data)
    assert f"  - {where}: " in str(loaded.value) and "SECRET" not in str(loaded.value)


@pytest.mark.parametrize(
    "key",
    [
        pytest.param("ghp_SECRETfictional0123456789abcdefghij", id="classic"),
        pytest.param("github_pat_SECRET" + "x" * 70, id="fine-grained"),
        pytest.param("SECRET" * 8, id="long"),
    ],
)
def test_a_credential_written_as_a_key_is_not_shown(key: str) -> None:
    with pytest.raises(ConfigError) as loaded:
        config_from_mapping({"github": {key: "value"}})
    message = str(loaded.value)
    assert f"  - github.{key[:4]}...: unknown key" in message and "SECRET" not in message


def test_a_section_validated_alone_hides_the_rejected_token_too() -> None:
    with pytest.raises(ValidationError) as excinfo:
        GithubConfig.model_validate({"token": "ghp_SECRET\nvalue"})
    assert "SECRET" not in f"{excinfo.value}\n{excinfo.value!r}"
    assert "the token holds a space, a control character or a non-ASCII character" in str(excinfo.value)


def test_github_token_from_the_environment_is_stripped() -> None:
    cfg = config_from_mapping({"github": {"token": "${GITHUB_TOKEN}"}}, env={"GITHUB_TOKEN": "ghp_TEST\n"})
    assert cfg.github.token == "ghp_TEST" and cfg.uses_env_reference("github.token")


def test_github_repo_normalisation_and_hook_format() -> None:
    cfg = config_from_mapping({"github": {"repo": "https://github.com/acme/etl.git"}})
    assert cfg.github.repo == "acme/etl"
    assert config_from_mapping({"github": {"repo": "git@github.com:acme/etl.git"}}).github.repo == "acme/etl"
    with pytest.raises(ConfigError, match="github.repo"):
        config_from_mapping({"github": {"repo": "not a repo"}})
    assert config_from_mapping({"masking": {"hook": "acme.masks:mask"}}).masking.hook == "acme.masks:mask"
    with pytest.raises(ConfigError, match="masking.hook"):
        config_from_mapping({"masking": {"hook": "acme.masks.mask"}})


# --------------------------------------------------------------------------- llm
BEDROCK_SETTINGS = ("aws_region", "aws_profile", "endpoint_url")
VPC_ENDPOINT = "https://vpce-0123456789abcdef0-abcdefgh.bedrock-runtime.eu-west-1.vpce.amazonaws.com"
PROVIDER_FROM_ENV = "${HEALER_LLM_PROVIDER:-anthropic}"
NOT_A_REGION = "aws_region: Value error, expected an AWS region such as eu-west-1"
NOT_A_PROFILE = "aws_profile: Value error, expected the name of a profile of the shared AWS config files"
NOT_AN_ENDPOINT = (
    "endpoint_url: Value error, expected an https URL (http only for a loopback host) without credentials, "
    "query or fragment"
)
NOT_A_MODEL = "model: Value error, expected a model ID without whitespace or control characters"


@pytest.mark.parametrize(
    ("value", "env", "expected"),
    [
        pytest.param("bedrock", {}, "bedrock", id="literal"),
        pytest.param(PROVIDER_FROM_ENV, {"HEALER_LLM_PROVIDER": "bedrock"}, "bedrock", id="env"),
        pytest.param(PROVIDER_FROM_ENV, {}, "anthropic", id="env-unset"),
        pytest.param("fake", {}, "fake", id="fake"),
    ],
)
def test_llm_provider_values(value: str, env: dict[str, str], expected: str) -> None:
    assert config_from_mapping({"llm": {"provider": value}}, env=env).llm.provider == expected


def test_an_unknown_llm_provider_is_rejected() -> None:
    with pytest.raises(ConfigError) as excinfo:
        config_from_mapping({"llm": {"provider": "openai"}})
    assert "llm.provider: Input should be 'anthropic', 'bedrock' or 'fake'" in str(excinfo.value)


@pytest.mark.parametrize(
    ("settings", "expected"),
    [
        pytest.param({"aws_region": " eu-west-1 "}, {"aws_region": "eu-west-1"}, id="region"),
        pytest.param({"aws_region": "us-gov-west-1"}, {"aws_region": "us-gov-west-1"}, id="gov-region"),
        pytest.param(
            {"aws_region": "eusc-de-east-1"}, {"aws_region": "eusc-de-east-1"}, id="sovereign-cloud"
        ),
        pytest.param(
            {"aws_region": " ", "aws_profile": "", "endpoint_url": None},
            {"aws_region": None, "aws_profile": None, "endpoint_url": None},
            id="blank",
        ),
        pytest.param(
            {"aws_region": "${NO_SUCH_REGION}", "endpoint_url": "${NO_SUCH_URL}"},
            {"aws_region": None, "endpoint_url": None},
            id="env-unset",
        ),
        pytest.param({"aws_profile": " healer-dev "}, {"aws_profile": "healer-dev"}, id="profile"),
        pytest.param({"endpoint_url": f" {VPC_ENDPOINT}/ "}, {"endpoint_url": VPC_ENDPOINT}, id="https"),
        pytest.param(
            {"endpoint_url": "https://bedrock-proxy.internal:8443/bedrock/"},
            {"endpoint_url": "https://bedrock-proxy.internal:8443/bedrock"},
            id="gateway",
        ),
        pytest.param(
            {"endpoint_url": "http://127.0.0.1:8080/bedrock/"},
            {"endpoint_url": "http://127.0.0.1:8080/bedrock"},
            id="loopback-http",
        ),
        pytest.param(
            {"endpoint_url": "http://[::1]:8080"}, {"endpoint_url": "http://[::1]:8080"}, id="ipv6-loopback"
        ),
        pytest.param(
            {"endpoint_url": "HTTP://localhost:8080"},
            {"endpoint_url": "HTTP://localhost:8080"},
            id="localhost",
        ),
        pytest.param(
            {"model": "  eu.anthropic.claude-opus-5-5 "},
            {"model": "eu.anthropic.claude-opus-5-5"},
            id="model",
        ),
    ],
)
def test_bedrock_settings_are_normalised(settings: dict[str, Any], expected: dict[str, Any]) -> None:
    llm = config_from_mapping({"llm": settings}, env={}).llm
    assert {name: getattr(llm, name) for name in expected} == expected


@pytest.mark.parametrize(
    ("settings", "message"),
    [
        pytest.param({"aws_region": "Europe-SECRET"}, NOT_A_REGION, id="bad-region"),
        pytest.param({"aws_region": "eu_west_1"}, NOT_A_REGION, id="underscore"),
        pytest.param({"aws_profile": "healer SECRET"}, NOT_A_PROFILE, id="profile-space"),
        pytest.param({"aws_profile": "healer\tSECRET"}, NOT_A_PROFILE, id="profile-tab"),
        pytest.param({"aws_secret_access_key": "SECRET"}, "aws_secret_access_key: unknown key", id="no-key"),
        *(
            pytest.param({"endpoint_url": url}, NOT_AN_ENDPOINT, id=name)
            for name, url in (
                ("userinfo", "https://user:SECRET@bedrock.example.com"),
                ("user", "https://SECRET@bedrock.example.com"),
                ("query", "https://bedrock.example.com/?X-Amz-Security-Token=SECRET"),
                ("fragment", "https://bedrock.example.com/#SECRET"),
                ("ftp", "ftp://SECRET.example.com"),
                ("no-scheme", "bedrock-runtime.eu-west-1.amazonaws.com/SECRET"),
                ("port", "https://bedrock.example.com:SECRET"),
                ("space", "https://bedrock example.com/SECRET"),
                ("http", "http://bedrock.collector.SECRET.example.net"),
                ("http-private-address", "http://10.0.0.7:8080/SECRET"),
                ("backslash", "https://SECRET.example.net\\.bedrock-runtime.eu-west-1.amazonaws.com"),
                ("percent-host", "https://SECRET.example.net%2F.bedrock-runtime.eu-west-1.amazonaws.com"),
            )
        ),
        *(
            pytest.param({"model": model}, NOT_A_MODEL, id=name)
            for name, model in (
                ("model-crlf", "eu.anthropic.claude-opus-5-5\r\nSECRET forged log line"),
                ("model-space", "eu.anthropic.claude-opus-5-5 SECRET"),
                ("model-escape", "eu.anthropic.SECRET\x1b[31m"),
            )
        ),
    ],
)
def test_invalid_bedrock_settings_are_rejected_without_their_value(
    settings: dict[str, Any], message: str
) -> None:
    with pytest.raises(ConfigError) as excinfo:
        config_from_mapping({"llm": {"provider": "bedrock", **settings}})
    text = str(excinfo.value)
    assert f"  - llm.{message}" in text
    assert "SECRET" not in text


def test_the_endpoint_error_names_an_example() -> None:
    with pytest.raises(ConfigError) as excinfo:
        config_from_mapping({"llm": {"endpoint_url": "https://user:pw@vpce.example.com"}})
    assert (
        "llm.endpoint_url: Value error, expected an https URL (http only for a loopback host) without "
        f"credentials, query or fragment, such as {VPC_ENDPOINT}"
    ) in str(excinfo.value)


def test_a_model_from_the_environment_cannot_forge_a_log_line() -> None:
    raw = yaml.safe_load(default_config_yaml())
    with pytest.raises(ConfigError) as excinfo:
        config_from_mapping(raw, env={"HEALER_LLM_MODEL": "eu.anthropic.claude-opus-5-5\r\nSECRET"})
    assert f"llm.{NOT_A_MODEL}" in str(excinfo.value)
    assert "SECRET" not in str(excinfo.value)


def test_bedrock_settings_are_dumped_only_when_set() -> None:
    from dag_healer.cli.doctor_cmd import effective_settings

    plain = config_from_mapping({"llm": {"provider": "bedrock", "model": "eu.anthropic.claude-opus-5-5"}})
    for dump in (
        plain.model_dump()["llm"],
        plain.model_dump(mode="json", by_alias=True)["llm"],
        json.loads(plain.model_dump_json())["llm"],
        effective_settings(plain)["llm"],
    ):
        assert list(dump) == ["provider", "model", "effort", "max_tokens", "fallbacks"]
    assert HealerConfig().model_dump()["llm"] == LlmConfig().model_dump()  # the defaults dump as before
    settings = {"aws_region": "eu-west-1", "aws_profile": "healer-dev", "endpoint_url": VPC_ENDPOINT}
    full = config_from_mapping({"llm": {"provider": "bedrock", **settings}})
    assert {name: full.model_dump(mode="json")["llm"][name] for name in BEDROCK_SETTINGS} == settings
    assert effective_settings(full)["llm"]["endpoint_url"] == VPC_ENDPOINT
    assert config_from_mapping(full.model_dump(mode="json")).llm == full.llm  # the dump loads back
    only_region = config_from_mapping({"llm": {"aws_region": "eu-west-1"}}).model_dump()["llm"]
    assert [name for name in BEDROCK_SETTINGS if name in only_region] == ["aws_region"]


def test_the_bedrock_settings_are_not_secrets() -> None:
    from dag_healer.config import SECRET_FIELDS

    assert not [field for field in SECRET_FIELDS if field.startswith("llm.")]
    cfg = config_from_mapping({"llm": {"aws_profile": "healer-dev", "endpoint_url": VPC_ENDPOINT}})
    assert cfg.secret_literals() == []


@pytest.mark.parametrize(
    ("llm", "explicit"),
    [
        pytest.param({}, False, id="absent"),
        pytest.param({"fallbacks": None}, False, id="empty"),
        pytest.param({"fallbacks": "${HEALER_NO_FALLBACKS}"}, False, id="env-unset"),
        pytest.param({"fallbacks": True}, True, id="true"),
        pytest.param({"fallbacks": False}, True, id="false"),
        pytest.param({"fallbacks": "${HEALER_NO_FALLBACKS:-true}"}, True, id="env-fallback"),
    ],
)
def test_fallbacks_are_explicit_only_when_the_file_sets_them(llm: dict[str, Any], explicit: bool) -> None:
    """The bedrock provider warns about ``fallbacks: true`` only when it is written in the file."""
    cfg = config_from_mapping({"llm": {"provider": "bedrock", **llm}}, env={})
    assert ("fallbacks" in cfg.llm.model_fields_set) is explicit
    assert cfg.llm.fallbacks is (llm.get("fallbacks") is not False)


@pytest.mark.parametrize("orchestrator", ["airflow", "stepfunctions"])
def test_the_templates_document_the_bedrock_settings(orchestrator: str) -> None:
    text = default_config_yaml(orchestrator=orchestrator)
    block = text.split("\nllm:\n", 1)[1].split("\n\n", 1)[0].splitlines()
    assert block[:5] == [
        '  provider: "${HEALER_LLM_PROVIDER:-anthropic}"   # anthropic | bedrock | fake (offline tests)',
        '  model: "${HEALER_LLM_MODEL:-claude-opus-5-5}"   '
        "# bedrock: a Bedrock model or inference profile ID",
        "  effort: high",
        "  max_tokens: 16000",
        "  fallbacks: true                # server-side refusal fallbacks (Claude API only)",
    ]
    comments = block[5:]
    assert comments and all(line.startswith("  # ") for line in comments)
    assert "docs/bedrock.md" in comments[0] and "never from this file" in " ".join(comments)
    assert "Cross-Region inference" in " ".join(comments) and "Model catalog" in " ".join(comments)
    keys = [line[4:].split(":", 1)[0] for line in comments]
    assert [key for key in keys if key in LlmConfig.model_fields] == list(BEDROCK_SETTINGS)  # in field order
    raw = yaml.safe_load(text)
    assert set(raw["llm"]) == {"provider", "model", "effort", "max_tokens", "fallbacks"}


@pytest.mark.parametrize(
    ("env", "model"),
    [
        pytest.param({}, "claude-opus-5-5", id="unset"),
        pytest.param({"HEALER_LLM_MODEL": ""}, "claude-opus-5-5", id="empty"),
        pytest.param(
            {"HEALER_LLM_PROVIDER": "bedrock", "HEALER_LLM_MODEL": "eu.anthropic.claude-opus-5-5"},
            "eu.anthropic.claude-opus-5-5",
            id="bedrock",
        ),
    ],
)
def test_the_template_reads_the_model_from_the_environment(env: dict[str, str], model: str) -> None:
    raw = yaml.safe_load(default_config_yaml())
    llm = config_from_mapping(raw, env=env).llm
    assert llm.model == model
    assert llm.provider == env.get("HEALER_LLM_PROVIDER", "anthropic")
    assert (llm.aws_region, llm.aws_profile, llm.endpoint_url) == (None, None, None)


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
