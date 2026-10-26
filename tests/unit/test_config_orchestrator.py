"""Configuration of the orchestrator abstraction (section 5 of docs/architecture/orchestrators.md).

* Phase-5 files keep their effective settings: the frozen copies in
  ``tests/unit/fixtures/config/phase5`` (the template, the ``init`` file of the example
  repository, the demo ``healer.yaml``, the files other tests write, a file with every section
  set) give the ``dag-healer check`` YAML and the secret literals recorded with the phase-5
  ``config.py`` (``effective_settings.json``; re-record with ``HEALER_UPDATE_GOLDEN=1``).
* The Airflow template is the phase-5 one plus a commented ``orchestrator:`` block, the
  ``repo.sandbox`` block and ``github.skip_ci`` (true, the default, so the effective settings
  do not change); the Step Functions template (section 9.14) is valid and shares the common
  sections.
* ``orchestrator:``, ``serve:`` (auth backends, tokens as hashes only, RBAC rules), the
  ``monitor.dags``/``pipelines`` alias, the widened modes with ``MonitorConfig.wants`` and the
  secret options of the adapter (``SECRET_FIELDS`` extended at load time).
* Contracts accept ``pipeline_id``/``step_id`` as input aliases and keep their hashes.
"""

from __future__ import annotations

import itertools
import json
import os
import subprocess
import sys
import textwrap
from collections.abc import Iterator
from pathlib import Path
from typing import Any

import pytest
import yaml

from dag_healer import config as config_module
from dag_healer.cli.doctor_cmd import effective_settings
from dag_healer.cli.main import main
from dag_healer.config import (
    MODE_KINDS,
    MONITOR_MODES,
    TEMPLATE_ORCHESTRATORS,
    ConfigError,
    HealerConfig,
    MonitorConfig,
    OrchestratorConfig,
    RbacRule,
    ServeAuthConfig,
    ServeConfig,
    config_from_mapping,
    default_config_yaml,
    load_config,
)
from dag_healer.contracts.io import (
    compute_source_hash,
    contract_to_dict,
    dump_contract_yaml,
    is_stale,
    load_contract,
    parse_contract,
)
from dag_healer.contracts.schema import Contract, ContractError, DatasetContract, DatasetLocator
from dag_healer.orchestrators import registry

PHASE5 = Path(__file__).parent / "fixtures" / "config" / "phase5"
EXPECTED = PHASE5 / "effective_settings.json"
UPDATE = os.environ.get("HEALER_UPDATE_GOLDEN") == "1"
PHASE5_FILES = sorted(path.name for path in PHASE5.glob("*.yaml"))
BASE_SECRET_FIELDS = ("airflow.password", "notify.email.password", "github.token", "ingest.token")
COMMON_SECTIONS = {
    "contracts",
    "classification",
    "notify",
    "github",
    "llm",
    "limits",
    "masking",
    "store",
    "debug",
}
DIGEST = "9f86d081884c7d659a2feaa0c55ad015a3bf4f1b2b0b822cd15d6c15b0f00a08"  # sha256("test")
ALB_ARN = "arn:aws:elasticloadbalancing:eu-west-1:123456789012:loadbalancer/app/healer/abc"

#: Variables the configuration fixtures read; every recording starts with all of them unset.
CLEARED = (
    "HEALER_CONFIG",
    "HEALER_DATABASE_URL",
    "HEALER_API_URL",
    "HEALER_PUBLIC_URL",
    "HEALER_INGEST_TOKEN",
    "HEALER_DEBUG",
    "HEALER_LLM_PROVIDER",
    "HEALER_FAKE_LLM_DIR",
    "HEALER_AIRFLOW_USERNAME",
    "HEALER_AIRFLOW_PASSWORD",
    "HEALER_SMTP_HOST",
    "HEALER_SMTP_PORT",
    "HEALER_SMTP_FROM",
    "HEALER_ALERT_TO",
    "HEALER_SMTP_STARTTLS",
    "HEALER_SMTP_USERNAME",
    "HEALER_SMTP_PASSWORD",
    "HEALER_SFN_QUEUE_URL",
    "AIRFLOW_HOME",
    "AIRFLOW__CORE__EXECUTION_API_SERVER_URL",
    "AIRFLOW__CORE__DAGS_FOLDER",
    "AIRFLOW__API__BASE_URL",
    "GITHUB_TOKEN",
    "GITHUB_REPO",
    "ANTHROPIC_API_KEY",
    "HEALER_SANDBOX_IMAGE",
)
#: The filled environment of the recording (``"unset"`` is the empty one).
SET_ENV = {
    "HEALER_API_URL": "http://airflow-apiserver:8080",
    "HEALER_AIRFLOW_USERNAME": "admin",
    "HEALER_AIRFLOW_PASSWORD": "s3cr3t-airflow",
    "HEALER_SMTP_HOST": "mailpit",
    "HEALER_SMTP_PORT": "1025",
    "HEALER_SMTP_FROM": "healer@example.com",
    "HEALER_ALERT_TO": "ops@example.com, data@example.com",
    "HEALER_SMTP_STARTTLS": "false",
    "HEALER_SMTP_USERNAME": "mailer",
    "HEALER_SMTP_PASSWORD": "s3cr3t-smtp",
    "GITHUB_REPO": "acme/etl",
    "GITHUB_TOKEN": "ghp_s3cr3t",
    "HEALER_LLM_PROVIDER": "fake",
    "HEALER_DATABASE_URL": "postgresql+psycopg2://healer:s3cr3t-db@postgres/healer",
    "HEALER_INGEST_TOKEN": "s3cr3t-ingest",
    "HEALER_DEBUG": "true",
}
ENVIRONMENTS: dict[str, dict[str, str]] = {"unset": {}, "set": SET_ENV}
#: The datasets ``dag-healer init`` found in ``example_existing_repo`` (``init_example_repo.yaml``).
EXAMPLE_DATASETS = {
    "customers_sync": {"extract": DatasetLocator(type="csv", path="data/incoming/customers.csv")},
    "inventory_report": {"get_inventory": DatasetLocator(type="csv", path="data/incoming/inventory.csv")},
    "sales_daily_etl": {"extract_sales": DatasetLocator(type="csv", path="data/incoming/sales.csv")},
}
#: The ``orchestrator.options`` keys of the Step Functions example (section 9.14).
SFN_OPTION_KEYS = {
    "region",
    "profile",
    "role_arn",
    "endpoint_url",
    "template",
    "pipelines",
    "discover",
    "statuses",
    "lookback_hours",
    "history_max_events",
    "max_histories_per_poll",
    "queue_url",
    "log_lookup",
    "retry_strategy",
    "rerun",
    "code_map",
    "links",
}
#: The example ``healer.yaml`` of section 9.14 of the design, verbatim.
SECTION_9_14 = """\
version: 1
repo: {path: ., base_branch: main, dags_dir: statemachines, allowed_paths: ["functions/", "src/"]}
orchestrator:
  type: stepfunctions
  instance: sfn-prod-euw1
  options:
    region: eu-west-1
    profile: null                   # local dev; else the default boto3 chain (ECS task role, IRSA, env)
    role_arn: null                  # optional AssumeRole (+ role_external_id, a secret option)
    endpoint_url: null              # VPC endpoint / LocalStack (NOT Step Functions Local: unsupported)
    template: template.yaml         # SAM template for the CodeLocator
    pipelines:
      sales_daily_etl:
        state_machine: arn:aws:states:eu-west-1:123456789012:stateMachine:sales-daily-etl-AbC123
        logical_id: SalesDailyEtlStateMachine
    discover: false
    statuses: [FAILED, TIMED_OUT]
    lookback_hours: 24
    history_max_events: 5000
    max_histories_per_poll: 50
    queue_url: "${HEALER_SFN_QUEUE_URL}"
    log_lookup: true
    retry_strategy: redrive         # redrive | disabled
    rerun: false
    code_map: {}                    # {LogicalIdOrFunctionName: {path: functions/x, handler: app.handler}}
    links: {}                       # overrides of the templates in 9.11
monitor: {dags: ["*"], modes: [poll, queue], poll_interval_seconds: 60, trigger: final_failure}
datasets:
  sales_daily_etl:
    TransformSales: {type: csv, path: "s3://acme-landing/incoming/sales.csv"}
serve:
  public_url: https://healer.example.com
  auth:
    backend: alb_oidc
    alb_arn: arn:aws:elasticloadbalancing:eu-west-1:123456789012:loadbalancer/app/healer/abc
    region: eu-west-1
    groups_claim: "cognito:groups"
    rules:
      - {principals: ["group:data-eng"], pipelines: ["*"], actions: [read, act, approve_contract]}
      - {principals: ["authenticated"], pipelines: ["sales_*"], actions: [read]}
store: {url: "${HEALER_DATABASE_URL}"}
ingest: {token: "${HEALER_INGEST_TOKEN}"}
"""


@pytest.fixture(autouse=True)
def _restore_shared_state(monkeypatch: pytest.MonkeyPatch) -> Iterator[None]:
    """Loading a configuration extends ``SECRET_FIELDS`` and may cache an adapter factory."""
    monkeypatch.setattr(config_module, "SECRET_FIELDS", config_module.SECRET_FIELDS)
    yield
    registry.reset_adapters()


@pytest.fixture
def clean_env(healer_env: Path, monkeypatch: pytest.MonkeyPatch) -> Path:
    """No variable a fixture reads is set; returns the temporary HEALER_HOME."""
    for name in CLEARED:
        monkeypatch.delenv(name, raising=False)
    return healer_env


def load(data: dict[str, Any] | str, **kwargs: Any) -> HealerConfig:
    raw = yaml.safe_load(textwrap.dedent(data)) if isinstance(data, str) else data
    return config_from_mapping(raw or {}, **kwargs)


def config_error(data: dict[str, Any] | str) -> str:
    with pytest.raises(ConfigError) as excinfo:
        load(data)
    return str(excinfo.value)


# =========================================================================== phase-5 files


def _normalise(value: Any, replacements: dict[str, str]) -> Any:
    if isinstance(value, str):
        text = value.replace("\\", "/")
        for old, new in replacements.items():
            text = text.replace(old, new)
        return text
    if isinstance(value, dict):
        return {key: _normalise(item, replacements) for key, item in value.items()}
    if isinstance(value, list):
        return [_normalise(item, replacements) for item in value]
    return value


def _record(home: Path) -> dict[str, Any]:
    """``{"<file>@<env>": {check, secret_literals}}`` for every phase-5 file and environment."""
    replacements: dict[str, str] = {}
    for path, token in ((PHASE5, "<CONFIG_DIR>"), (home, "<HEALER_HOME>")):
        for variant in {str(path), str(path.resolve())}:
            replacements[variant.replace("\\", "/")] = token
    results: dict[str, Any] = {}
    for env_name, env in ENVIRONMENTS.items():
        with pytest.MonkeyPatch.context() as mp:
            for name in CLEARED:
                mp.delenv(name, raising=False)
            mp.setenv("HEALER_HOME", str(home))
            for name, value in env.items():
                mp.setenv(name, value)
            for name in PHASE5_FILES:
                cfg = load_config(PHASE5 / name)
                data = _normalise(effective_settings(cfg), replacements)
                results[f"{name}@{env_name}"] = {
                    "check": yaml.safe_dump(data, sort_keys=False, default_flow_style=False),
                    "secret_literals": cfg.secret_literals(),
                }
    return results


@pytest.fixture(scope="module")
def recorded(tmp_path_factory: pytest.TempPathFactory) -> dict[str, Any]:
    home = tmp_path_factory.mktemp("config_home")
    original = config_module.SECRET_FIELDS
    try:
        results = _record(home)
    finally:
        config_module.SECRET_FIELDS = original
    if UPDATE:
        EXPECTED.write_bytes((json.dumps(results, indent=2, sort_keys=True) + "\n").encode("utf-8"))
    return results


def _expected() -> dict[str, Any]:
    return json.loads(EXPECTED.read_text(encoding="utf-8"))


def test_every_phase5_file_is_recorded_under_both_environments(recorded: dict[str, Any]) -> None:
    assert len(PHASE5_FILES) >= 9
    assert sorted(_expected()) == sorted(recorded)
    assert sorted(recorded) == sorted(f"{name}@{env}" for name in PHASE5_FILES for env in ENVIRONMENTS)


@pytest.mark.parametrize("key", sorted(f"{name}@{env}" for name in PHASE5_FILES for env in ENVIRONMENTS))
def test_phase5_files_keep_their_effective_settings(recorded: dict[str, Any], key: str) -> None:
    assert recorded[key] == _expected()[key], f"{key} changed; see {EXPECTED.parent / 'README.md'}"


@pytest.mark.parametrize("name", PHASE5_FILES)
def test_phase5_files_get_the_default_orchestrator_and_serve_sections(clean_env: Path, name: str) -> None:
    cfg = load_config(PHASE5 / name)
    assert cfg.orchestrator == OrchestratorConfig()
    assert (cfg.orchestrator.type, cfg.orchestrator.instance_id, cfg.orchestrator.options) == (
        "airflow",
        "airflow",
        {},
    )
    assert cfg.serve == ServeConfig()
    assert registry.orchestrator_settings(cfg) == registry.OrchestratorSettings("airflow", "airflow", {})
    for dump in (
        cfg.model_dump(),
        cfg.model_dump(mode="json", by_alias=True),
        json.loads(cfg.model_dump_json()),
    ):
        assert "orchestrator" not in dump and "serve" not in dump
    assert cfg.secret_fields == BASE_SECRET_FIELDS
    modes = set(cfg.monitor.modes)
    assert modes <= {"rest", "listener", "callback"}
    assert cfg.monitor.wants("poll") is ("rest" in modes)
    assert cfg.monitor.wants("listener") is ("listener" in modes)
    assert cfg.monitor.wants("callback") is ("callback" in modes)
    assert cfg.monitor.wants("queue") is False


def test_the_demo_file_is_the_init_file_with_the_demo_settings() -> None:
    init = yaml.safe_load((PHASE5 / "init_example_repo.yaml").read_text(encoding="utf-8"))
    demo = yaml.safe_load((PHASE5 / "demo.yaml").read_text(encoding="utf-8"))
    assert demo["monitor"]["poll_interval_seconds"] == 5 and demo["notify"]["email"]["host"] == "mailpit"
    for section in ("repo", "airflow", "datasets", "github", "llm", "store", "ingest"):
        assert demo[section] == init[section]


# =========================================================================== templates


def _without_orchestrator_block(text: str) -> str:
    start = text.index("\n# Orchestrator (optional).")
    end = text.index("\nairflow:\n", start)
    return text[:start] + text[end:]


def _orchestrator_block(text: str) -> list[str]:
    start = text.index("\n# Orchestrator (optional).") + 1
    end = text.index("\n\nairflow:\n", start)
    return text[start:end].splitlines()


def _without_sandbox_block(text: str) -> str:
    """The template without ``repo.sandbox``: from its comment to the blank line after it."""
    start = text.index("\n  # Where pytest runs") + 1
    end = text.index("\n\n", start) + 1
    return text[:start] + text[end:]


def _without_skip_ci_block(text: str) -> str:
    """The template without ``github.skip_ci``: its three comment lines and the setting."""
    start = text.index("\n  # true: the fix's commit carries")
    end = text.index("  skip_ci: true\n", start) + len("  skip_ci: true\n")
    return text[: start + 1] + text[end:]


def _phase5_view(text: str) -> str:
    """The template without the blocks phase 5 did not have (the comment-only ones included)."""
    return _without_orchestrator_block(_without_sandbox_block(_without_skip_ci_block(text)))


def test_airflow_template_is_the_phase5_template_plus_a_commented_block() -> None:
    text = default_config_yaml()
    phase5 = (PHASE5 / "default_template.yaml").read_text(encoding="utf-8")
    assert _phase5_view(text) == phase5
    block = _orchestrator_block(text)
    assert all(line.startswith("#") for line in block)
    assert "# orchestrator:" in block
    raw = yaml.safe_load(text)
    assert raw["repo"].pop("sandbox")["backend"] == "container"  # the blocks phase 5 did not have
    assert raw["github"].pop("skip_ci") is True
    assert raw == yaml.safe_load(phase5)
    assert default_config_yaml(orchestrator="airflow") == text


def test_airflow_template_with_datasets_is_the_phase5_init_file_plus_the_block() -> None:
    text = default_config_yaml(repo_path=".", dags_dir="dags", tests_dir=None, datasets=EXAMPLE_DATASETS)
    assert _phase5_view(text) == (PHASE5 / "init_example_repo.yaml").read_text(encoding="utf-8")


def test_commented_orchestrator_block_documents_the_defaults() -> None:
    block = _orchestrator_block(default_config_yaml())
    start = block.index("# orchestrator:")
    uncommented = "\n".join(line[2:] for line in block[start:])
    section = yaml.safe_load(uncommented)["orchestrator"]
    assert section == {"type": "airflow", "instance": None, "options": {}}
    assert OrchestratorConfig.model_validate(section) == OrchestratorConfig()


def test_stepfunctions_template_follows_section_9_14(clean_env: Path, tmp_path: Path) -> None:
    text = default_config_yaml(orchestrator="stepfunctions", dags_dir="statemachines")
    cfg = load(text, base_dir=tmp_path)
    assert (cfg.orchestrator.type, cfg.orchestrator.instance_id) == ("stepfunctions", "sfn-prod-euw1")
    options = cfg.orchestrator.options
    assert set(options) == SFN_OPTION_KEYS - {"queue_url"}  # ${HEALER_SFN_QUEUE_URL} is unset
    assert cfg.uses_env_reference("orchestrator.options.queue_url")
    assert options["region"] == "eu-west-1" and options["statuses"] == ["FAILED", "TIMED_OUT"]
    assert options["pipelines"]["sales_daily_etl"]["logical_id"] == "SalesDailyEtlStateMachine"
    assert (options["retry_strategy"], options["rerun"], options["discover"]) == ("redrive", False, False)
    assert (options["code_map"], options["links"], options["profile"]) == ({}, {}, None)
    assert cfg.repo.dags_dir == "statemachines" and cfg.repo.allowed_paths == ["functions/", "src/"]
    assert cfg.monitor.dags == ["*"] and cfg.monitor.modes == ["poll", "queue"]
    assert cfg.monitor.poll_interval_seconds == 60 and cfg.monitor.trigger == "final_failure"
    assert cfg.monitor.wants("poll") and cfg.monitor.wants("queue") and not cfg.monitor.wants("push")
    auth = cfg.serve.auth
    assert (auth.backend, auth.alb_arn, auth.region, auth.groups_claim) == (
        "alb_oidc",
        ALB_ARN,
        "eu-west-1",
        "cognito:groups",
    )
    assert auth.rules == [
        RbacRule(principals=["group:data-eng"], pipelines=["*"], actions=["read", "act", "approve_contract"]),
        RbacRule(principals=["authenticated"], pipelines=["sales_*"], actions=["read"]),
    ]
    assert cfg.store.url is None and cfg.ingest.token is None
    assert cfg.secret_literals() == []
    assert cfg.repo_root == tmp_path.resolve()


def test_stepfunctions_template_reads_the_queue_from_the_environment(
    clean_env: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setenv("HEALER_SFN_QUEUE_URL", "https://sqs.eu-west-1.amazonaws.com/123456789012/healer")
    cfg = load(default_config_yaml(orchestrator="stepfunctions"))
    assert set(cfg.orchestrator.options) == SFN_OPTION_KEYS
    assert cfg.orchestrator.options["queue_url"].endswith("/healer")
    assert cfg.secret_literals() == []


def test_both_templates_share_the_common_sections(clean_env: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    for env in ENVIRONMENTS.values():
        for name, value in env.items():
            monkeypatch.setenv(name, value)
        airflow = load(default_config_yaml())
        sfn = load(default_config_yaml(orchestrator="stepfunctions"))
        assert airflow.model_dump(include=COMMON_SECTIONS) == sfn.model_dump(include=COMMON_SECTIONS)
    airflow_text, sfn_text = default_config_yaml(), default_config_yaml(orchestrator="stepfunctions")
    shared = airflow_text[airflow_text.index("classification:") : airflow_text.index("store:")]
    assert shared in sfn_text


def test_stepfunctions_template_renders_its_parameters(tmp_path: Path) -> None:
    datasets = {
        "sales_daily_etl": {"TransformSales": DatasetLocator(type="csv", path="s3://acme/in/sales.csv")}
    }
    text = default_config_yaml(
        orchestrator="stepfunctions",
        repo_path="..\\my repo",
        dags_dir="infra/sfn",
        tests_dir="tests",
        datasets=datasets,
    )
    raw = yaml.safe_load(text)
    assert raw["repo"]["path"] == "../my repo"
    assert (raw["repo"]["dags_dir"], raw["repo"]["tests_dir"]) == ("infra/sfn", "tests")
    cfg = load(raw, base_dir=tmp_path / "cfg")
    assert cfg.repo_root == (tmp_path / "my repo").resolve()
    assert cfg.dataset_locators("sales_daily_etl")["TransformSales"].path == "s3://acme/in/sales.csv"


@pytest.mark.parametrize("orchestrator", ["fake", "dagster", "", "Airflow"])
def test_only_known_templates_render(orchestrator: str) -> None:
    assert TEMPLATE_ORCHESTRATORS == ("airflow", "stepfunctions")
    with pytest.raises(ValueError, match="no healer.yaml template"):
        default_config_yaml(orchestrator=orchestrator)


def test_the_design_example_of_section_9_14_loads(clean_env: Path) -> None:
    cfg = load(SECTION_9_14)
    assert cfg.orchestrator.instance_id == "sfn-prod-euw1"
    assert set(cfg.orchestrator.options) == SFN_OPTION_KEYS - {"queue_url"}
    assert cfg.serve.public_url == "https://healer.example.com"
    assert cfg.serve.auth.user_claim == "email" and len(cfg.serve.auth.rules) == 2
    assert (
        cfg.dataset_locators("sales_daily_etl")["TransformSales"].path
        == "s3://acme-landing/incoming/sales.csv"
    )
    assert cfg.secret_literals() == []


# =========================================================================== orchestrator:


def test_orchestrator_defaults() -> None:
    section = HealerConfig().orchestrator
    assert (section.type, section.instance, section.options, section.instance_id) == (
        "airflow",
        None,
        {},
        "airflow",
    )
    assert load({"orchestrator": None}).orchestrator == section
    assert load({"orchestrator": {}}).orchestrator == section
    assert load({"orchestrator": {"type": "  ", "instance": ""}}).orchestrator == section
    assert (
        load({"orchestrator": {"type": "${UNSET_TYPE}", "instance": "${UNSET_INSTANCE}"}}).orchestrator
        == section
    )


@pytest.mark.parametrize(
    ("section", "instance_id"),
    [
        ({"type": "fake"}, "fake"),
        ({"type": "fake", "instance": "fake-eu"}, "fake-eu"),
        ({"type": "stepfunctions", "instance": " sfn-prod-euw1 "}, "sfn-prod-euw1"),
        ({"type": "airflow", "instance": "airflow-staging"}, "airflow-staging"),
        ({"type": "acme_orch.adapter:FACTORY", "instance": "acme.prod_1"}, "acme.prod_1"),
        ({"type": "fake", "instance": "a" * 64}, "a" * 64),
    ],
)
def test_orchestrator_instance_id(section: dict[str, Any], instance_id: str) -> None:
    cfg = load({"orchestrator": section})
    assert cfg.orchestrator.instance_id == instance_id
    assert registry.orchestrator_settings(cfg).instance == instance_id


@pytest.mark.parametrize(
    ("section", "problem"),
    [
        ({"instance": "Prod"}, "orchestrator.instance: String should match pattern"),
        ({"instance": "-x"}, "orchestrator.instance: String should match pattern"),
        ({"instance": "a" * 65}, "orchestrator.instance: String should match pattern"),
        ({"instance": "has space"}, "orchestrator.instance: String should match pattern"),
        ({"type": "acme.adapter:FACTORY"}, "orchestrator.instance: Value error, required because the type"),
        ({"type": "Acme"}, "orchestrator.instance: Value error, required because the type 'Acme'"),
        ({"type": "pkg.mod:"}, "orchestrator.type: Value error, expected an entry-point name"),
        ({"type": "not a target:"}, "orchestrator.type: Value error, expected an entry-point name"),
        ({"type": "two words"}, "orchestrator.type: Value error, expected an entry-point name"),
        (
            {"options": {"api_url": "http://x"}},
            "orchestrator.options: Value error, the airflow orchestrator takes",
        ),
        ({"type": "fake", "options": ["world"]}, "orchestrator.options: Input should be a valid dictionary"),
        ({"type": "fake", "instnce": "x"}, "orchestrator.instnce: unknown key"),
    ],
)
def test_orchestrator_validation(section: dict[str, Any], problem: str) -> None:
    assert problem in config_error({"orchestrator": section})


def test_orchestrator_type_target_with_an_explicit_instance() -> None:
    cfg = load({"orchestrator": {"type": " acme_orch.adapters:FACTORY ", "instance": "acme"}})
    assert cfg.orchestrator.type == "acme_orch.adapters:FACTORY"
    assert cfg.orchestrator.instance_id == "acme"


def test_unset_options_are_left_out_so_the_adapter_default_applies(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("FAKEFLOW_URL", "https://ff.example.com")
    monkeypatch.delenv("FAKEFLOW_TOKEN", raising=False)
    monkeypatch.delenv("FAKEFLOW_WORLD", raising=False)
    cfg = load(
        {
            "orchestrator": {
                "type": "fake",
                "options": {
                    "base_url": "${FAKEFLOW_URL}",
                    "token": "${FAKEFLOW_TOKEN}",
                    "world": "${FAKEFLOW_WORLD:-worlds/day2.yaml}",
                    "nested": {"keep": 1, "drop": "${FAKEFLOW_TOKEN}", "items": ["a", "${FAKEFLOW_TOKEN}"]},
                    "empty": None,
                },
            }
        }
    )
    assert cfg.orchestrator.options == {
        "base_url": "https://ff.example.com",
        "world": "worlds/day2.yaml",
        "nested": {"keep": 1, "items": ["a"]},
        "empty": None,
    }
    assert cfg.uses_env_reference("orchestrator.options.token")
    assert cfg.uses_env_reference("orchestrator.options.nested.drop")
    assert cfg.secret_literals() == []


# =========================================================================== secret options


def test_secret_options_of_the_adapter_are_secret_fields(
    clean_env: Path, tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    path = tmp_path / "healer.yaml"
    path.write_text(
        "orchestrator:\n  type: fake\n  options: {token: literal-token-value}\n", encoding="utf-8"
    )
    cfg = load_config(path)
    assert cfg.secret_fields == (*BASE_SECRET_FIELDS, "orchestrator.options.token")
    assert cfg.secret_literals() == ["orchestrator.options.token"]
    assert config_module.SECRET_FIELDS[-1] == "orchestrator.options.token"
    settings = effective_settings(cfg)
    assert settings["orchestrator"] == {"type": "fake", "instance": None, "options": {"token": "***"}}
    assert main(["check", "--config", str(path)]) == 0
    captured = capsys.readouterr()
    assert "literal-token-value" not in captured.out + captured.err
    assert "orchestrator.options.token" in captured.err
    load_config(path)
    assert config_module.SECRET_FIELDS.count("orchestrator.options.token") == 1


def test_secret_options_from_the_environment_are_not_literals(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("FAKEFLOW_TOKEN", "from-env")
    cfg = load({"orchestrator": {"type": "fake", "options": {"token": "${FAKEFLOW_TOKEN}"}}})
    assert cfg.orchestrator.options["token"] == "from-env"
    assert "orchestrator.options.token" in cfg.secret_fields
    assert cfg.secret_literals() == []


def test_secret_options_do_not_leak_into_other_configurations() -> None:
    load({"orchestrator": {"type": "fake", "options": {"token": "x"}}})
    assert "orchestrator.options.token" in config_module.SECRET_FIELDS
    airflow = load({"airflow": {"password": "pw"}})
    assert airflow.secret_fields == BASE_SECRET_FIELDS
    assert airflow.secret_literals() == ["airflow.password"]
    other = load(
        {"orchestrator": {"type": "fake", "instance": "f2", "options": {"base_url": "https://x.test"}}}
    )
    assert other.secret_literals() == []


def test_an_adapter_that_cannot_be_loaded_adds_no_secret_options(caplog: pytest.LogCaptureFixture) -> None:
    with caplog.at_level("DEBUG", logger="dag_healer.config"):
        cfg = load(
            {
                "orchestrator": {
                    "type": "dag_healer_no_such_pkg.adapter:FACTORY",
                    "instance": "x",
                    "options": {"a": 1},
                }
            }
        )
    assert cfg.secret_fields == BASE_SECRET_FIELDS
    assert "no secret options known for orchestrator type" in caplog.text


def test_loading_an_airflow_configuration_imports_no_orchestrator_module(tmp_path: Path) -> None:
    script = (
        "import sys\n"
        "from dag_healer.config import config_from_mapping\n"
        "config_from_mapping({'monitor': {'modes': ['rest', 'listener']}, 'airflow': {'password': 'x'}})\n"
        "print(sorted(m for m in sys.modules if m.startswith('dag_healer.orchestrators')))\n"
        "config_from_mapping({'orchestrator': {'type': 'fake', 'options': {'token': 't'}}})\n"
        "print('dag_healer.orchestrators.fake' in sys.modules)\n"
    )
    result = subprocess.run(
        [sys.executable, "-c", script], capture_output=True, text=True, cwd=tmp_path, check=False
    )
    assert result.returncode == 0, result.stderr
    assert result.stdout.split() == ["[]", "True"]


# =========================================================================== monitor:


@pytest.mark.parametrize(
    ("monitor", "dags"),
    [
        ({"pipelines": ["sales_*", "!sales_tmp"]}, ["sales_*", "!sales_tmp"]),
        ({"pipelines": "sales_*"}, ["sales_*"]),
        ({"pipelines": None}, ["*"]),
        ({"pipelines": "${UNSET_PIPELINES}"}, ["*"]),
        ({"pipelines": ["${UNSET_PIPELINE}", "x"]}, ["x"]),
        ({"dags": ["a"]}, ["a"]),
    ],
)
def test_monitor_pipelines_is_an_alias_of_dags(monitor: dict[str, Any], dags: list[str]) -> None:
    cfg = load({"monitor": monitor})
    assert cfg.monitor.dags == dags
    assert cfg.model_dump()["monitor"]["dags"] == dags
    assert "pipelines" not in cfg.model_dump(by_alias=True)["monitor"]


def test_monitor_pipelines_drives_is_monitored() -> None:
    cfg = load({"monitor": {"pipelines": ["sales_*", "!sales_tmp"]}})
    assert cfg.is_monitored("sales_daily") and not cfg.is_monitored("sales_tmp")
    assert not cfg.is_monitored("customers")


def test_monitor_dags_and_pipelines_cannot_both_be_set() -> None:
    message = config_error({"monitor": {"dags": ["a"], "pipelines": ["b"]}})
    assert "monitor: Value error, 'dags' and 'pipelines' are the same setting" in message
    assert "monitor.pipelines: Input should be a valid list" in config_error({"monitor": {"pipelines": 3}})


@pytest.mark.parametrize("modes", [["poll"], ["poll", "push", "queue"], ["queue", "rest"], ["push"], []])
def test_widened_modes_are_kept_verbatim(modes: list[str]) -> None:
    cfg = load({"monitor": {"modes": modes}})
    assert cfg.monitor.modes == modes
    assert cfg.model_dump(mode="json")["monitor"]["modes"] == modes


def test_unknown_modes_are_rejected() -> None:
    message = config_error({"monitor": {"modes": ["rest", "kafka"]}})
    assert (
        "monitor.modes.1: Input should be 'rest', 'listener', 'callback', 'poll', 'push' or 'queue'"
        in message
    )


def test_mode_constants() -> None:
    assert MONITOR_MODES == ("rest", "listener", "callback", "poll", "push", "queue")
    assert set(MODE_KINDS) == set(MONITOR_MODES)
    assert set(MODE_KINDS.values()) == {"poll", "push", "queue"}
    assert MonitorConfig().modes == ["rest", "listener"]


@pytest.mark.parametrize(
    ("modes", "kind", "expected"),
    [
        (["rest"], "poll", True),
        (["poll"], "poll", True),
        (["poll"], "rest", True),
        (["listener"], "poll", False),
        (["listener"], "push", True),
        (["callback"], "push", True),
        (["push"], "push", True),
        (["queue"], "push", False),
        (["queue"], "queue", True),
        (["poll", "push"], "queue", False),
        (["listener"], "listener", True),
        (["listener"], "callback", False),
        (["callback"], "listener", False),
        (["push"], "listener", True),
        (["push"], "callback", True),
        (["push"], "eventbridge", True),
        (["listener", "callback"], "eventbridge", False),
        (["queue"], "sqs", True),
        (["push"], "sqs", False),
        ([], "cli", True),
        ([], "poll", False),
    ],
)
def test_wants(modes: list[str], kind: str, expected: bool) -> None:
    assert MonitorConfig(modes=modes).wants(kind) is expected


@pytest.mark.parametrize(
    "modes",
    [
        list(combo)
        for size in range(4)
        for combo in itertools.combinations(("rest", "listener", "callback"), size)
    ],
)
def test_wants_is_the_phase5_rule_for_airflow_modes(modes: list[str]) -> None:
    monitor = MonitorConfig(modes=modes)
    assert monitor.wants("poll") is ("rest" in modes)
    assert monitor.wants("rest") is ("rest" in modes)
    for source in ("listener", "callback"):
        assert monitor.wants(source) is (source in modes)
    assert monitor.wants("push") is bool({"listener", "callback"} & set(modes))
    assert monitor.wants("queue") is False


@pytest.mark.parametrize("kind", ["kafka", "", "POLL", "task-failed"])
def test_wants_rejects_unknown_kinds(kind: str) -> None:
    with pytest.raises(ValueError, match="unknown detection kind or incident source"):
        MonitorConfig().wants(kind)


# =========================================================================== serve:


def test_serve_defaults() -> None:
    serve = HealerConfig().serve
    assert (serve.host, serve.port, serve.public_url) == ("127.0.0.1", 8090, None)
    auth = serve.auth
    assert (auth.backend, auth.tokens, auth.trusted_proxies, auth.rules) == ("dev", [], [], [])
    assert (auth.user_header, auth.groups_header) == ("X-Forwarded-User", "X-Forwarded-Groups")
    assert (auth.alb_arn, auth.region, auth.user_claim, auth.groups_claim) == (None, None, "email", None)
    assert load({"serve": None}).serve == serve
    assert load({"serve": {"host": " ", "public_url": "", "auth": {}}}).serve == serve


def test_serve_host_port_and_public_url(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("HEALER_SERVE_PORT", "9000")
    serve = load(
        {
            "serve": {
                "host": "0.0.0.0",
                "port": "${HEALER_SERVE_PORT:-8090}",
                "public_url": " https://h.example.com/x/ ",
            }
        }
    ).serve
    assert (serve.host, serve.port, serve.public_url) == ("0.0.0.0", 9000, "https://h.example.com/x")
    assert load({"serve": {"public_url": "${UNSET_PUBLIC_URL}"}}).serve.public_url is None


@pytest.mark.parametrize(
    ("serve", "problem"),
    [
        ({"port": 0}, "serve.port: Input should be greater than or equal to 1"),
        ({"port": 70000}, "serve.port: Input should be less than or equal to 65535"),
        (
            {"public_url": "healer.example.com"},
            "serve.public_url: Value error, expected an absolute http(s) URL",
        ),
        (
            {"public_url": "ftp://h.example.com"},
            "serve.public_url: Value error, expected an absolute http(s) URL",
        ),
        (
            {"auth": {"backend": "kerberos"}},
            "serve.auth.backend: Input should be 'dev', 'token', 'trusted_header'",
        ),
        (
            {"auth": {"user_header": "X User"}},
            "serve.auth.user_header: Value error, 'X User' is not an HTTP header",
        ),
        ({"bind": "0.0.0.0"}, "serve.bind: unknown key"),
    ],
)
def test_serve_validation(serve: dict[str, Any], problem: str) -> None:
    assert problem in config_error({"serve": serve})


def test_serve_tokens_are_sha256_hashes(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("UNSET_SHA", raising=False)
    auth = load(
        {
            "serve": {
                "auth": {
                    "backend": "token",
                    "tokens": [
                        {"name": " ci ", "sha256": DIGEST.upper(), "groups": "bots"},
                        {"name": "ci", "sha256": "a" * 64},  # rotation: two hashes, one name
                        {"name": "cron", "sha256": "${UNSET_SHA}", "groups": ["x"]},  # not configured here
                        {"name": "ops", "sha256": "b" * 64, "groups": ["oncall", "${UNSET_SHA}", " admins "]},
                    ],
                }
            }
        }
    ).serve.auth
    assert auth.tokens == [
        {"name": "ci", "sha256": DIGEST, "groups": ["bots"]},
        {"name": "ci", "sha256": "a" * 64, "groups": []},
        {"name": "ops", "sha256": "b" * 64, "groups": ["oncall", "admins"]},
    ]


@pytest.mark.parametrize(
    ("token", "problem"),
    [
        (
            {"name": "ci", "token": "plain"},
            "item 0: unknown key(s) token; a token is configured by the sha256",
        ),
        ({"name": "ci", "sha256": DIGEST, "value": "plain"}, "item 0: unknown key(s) value;"),
        (
            {"name": "ci", "sha256": "plain-token"},
            "item 0: sha256 must be the 64-character hex sha256 digest",
        ),
        ({"name": "ci", "sha256": "g" * 64}, "item 0: sha256 must be the 64-character hex sha256 digest"),
        ({"name": "ci"}, "item 0: sha256 must be the 64-character hex sha256 digest"),
        ({"sha256": DIGEST}, "item 0: name must be a non-empty string"),
        ({"name": "ci", "sha256": DIGEST, "groups": [1]}, "item 0: groups must be a list of strings"),
        ("plain-token", "item 0: expected a mapping with name, sha256 and groups"),
    ],
)
def test_serve_tokens_never_hold_the_token(token: Any, problem: str) -> None:
    assert problem in config_error({"serve": {"auth": {"tokens": [token]}}})


def test_serve_tokens_list_a_hash_once() -> None:
    tokens = [{"name": "a", "sha256": DIGEST}, {"name": "b", "sha256": DIGEST.upper()}]
    assert "tokens 'a' and 'b' have the same sha256" in config_error({"serve": {"auth": {"tokens": tokens}}})


def test_trusted_header_needs_trusted_proxies() -> None:
    message = config_error({"serve": {"auth": {"backend": "trusted_header"}}})
    assert "serve.auth: Value error, the trusted_header backend requires trusted_proxies" in message
    auth = load(
        {
            "serve": {
                "auth": {
                    "backend": "trusted_header",
                    "trusted_proxies": ["10.1.2.3/8", "192.168.0.7", "fd00::/8"],
                    "user_header": "X-Auth-Request-Email",
                    "groups_header": "",
                }
            }
        }
    ).serve.auth
    assert auth.trusted_proxies == ["10.0.0.0/8", "192.168.0.7/32", "fd00::/8"]
    assert (auth.user_header, auth.groups_header) == ("X-Auth-Request-Email", None)
    assert load({"serve": {"auth": {"trusted_proxies": "127.0.0.1"}}}).serve.auth.trusted_proxies == [
        "127.0.0.1/32"
    ]
    assert "serve.auth.trusted_proxies: Value error, 'proxy.local' is not an IP address" in config_error(
        {"serve": {"auth": {"trusted_proxies": ["proxy.local"]}}}
    )


def test_alb_oidc_needs_the_load_balancer_arn() -> None:
    assert "serve.auth: Value error, the alb_oidc backend requires alb_arn" in config_error(
        {"serve": {"auth": {"backend": "alb_oidc"}}}
    )
    auth = load(
        {"serve": {"auth": {"backend": "alb_oidc", "alb_arn": ALB_ARN, "groups_claim": "groups"}}}
    ).serve.auth
    assert (auth.region, auth.user_claim, auth.groups_claim) == ("eu-west-1", "email", "groups")
    gov = "arn:aws-us-gov:elasticloadbalancing:us-gov-west-1:123456789012:loadbalancer/app/healer/0123abcd"
    assert (
        load({"serve": {"auth": {"backend": "alb_oidc", "alb_arn": gov}}}).serve.auth.region
        == "us-gov-west-1"
    )


@pytest.mark.parametrize(
    ("auth", "problem"),
    [
        (
            {"backend": "alb_oidc", "alb_arn": ALB_ARN, "region": "us-east-1"},
            "region 'us-east-1' is not the region",
        ),
        (
            {"alb_arn": "arn:aws:elasticloadbalancing:eu-west-1:1:loadbalancer/net/x/y"},
            "serve.auth.alb_arn: Value",
        ),
        ({"alb_arn": "healer-alb"}, "expected an Application Load Balancer ARN"),
        ({"region": "Europe"}, "serve.auth.region: Value error, expected an AWS region such as eu-west-1"),
    ],
)
def test_alb_oidc_validation(auth: dict[str, Any], problem: str) -> None:
    assert problem in config_error({"serve": {"auth": auth}})


def test_rbac_rules() -> None:
    rules = load(
        {
            "serve": {
                "auth": {
                    "rules": [
                        {"principals": "group:data eng"},
                        {
                            "principals": ["user:alice", "*", "authenticated"],
                            "pipelines": " sales_* ",
                            "actions": "act",
                        },
                    ]
                }
            }
        }
    ).serve.auth.rules
    assert rules == [
        RbacRule(principals=["group:data eng"], pipelines=["*"], actions=["read"]),
        RbacRule(principals=["user:alice", "*", "authenticated"], pipelines=["sales_*"], actions=["act"]),
    ]


@pytest.mark.parametrize(
    ("rule", "problem"),
    [
        ({"principals": ["alice"]}, "rules.0.principals: Value error, invalid principal 'alice'"),
        ({"principals": ["user:"]}, "invalid principal 'user:'"),
        ({"principals": ["role:admin"]}, "invalid principal 'role:admin'"),
        ({"pipelines": ["*"]}, "rules.0.principals: Field required"),
        ({"principals": ["*"], "actions": ["write"]}, "rules.0.actions.0: Input should be 'read', 'act' or"),
        (
            {"principals": ["*"], "pipelines": ["!"]},
            "rules.0.pipelines: Value error, pipeline patterns must not",
        ),
        ({"principals": ["*"], "dags": ["*"]}, "rules.0.dags: unknown key"),
    ],
)
def test_rbac_rule_validation(rule: dict[str, Any], problem: str) -> None:
    assert problem in config_error({"serve": {"auth": {"rules": [rule]}}})


def test_section_models_validate_on_their_own() -> None:
    assert ServeAuthConfig(backend="token").tokens == []
    assert ServeConfig(port=8091).auth == ServeAuthConfig()
    assert OrchestratorConfig(type="fake", instance="f1").instance_id == "f1"


# =========================================================================== dumps


def test_configured_sections_are_dumped_in_field_order() -> None:
    cfg = load(
        {
            "orchestrator": {"type": "fake", "instance": "fake-dev"},
            "serve": {"port": 9001, "auth": {"backend": "token"}},
        }
    )
    data = cfg.model_dump(mode="json", by_alias=True)
    keys = list(data)
    assert keys.index("repo") + 1 == keys.index("orchestrator") == keys.index("airflow") - 1
    assert keys.index("masking") + 1 == keys.index("serve") == keys.index("store") - 1
    assert data["orchestrator"] == {"type": "fake", "instance": "fake-dev", "options": {}}
    assert data["serve"]["port"] == 9001 and data["serve"]["auth"]["backend"] == "token"
    assert json.loads(cfg.model_dump_json())["serve"]["port"] == 9001
    assert "serve" not in cfg.model_dump(exclude={"serve"}) and list(cfg.model_dump(include={"serve"})) == [
        "serve"
    ]


def test_each_section_is_left_out_only_while_it_holds_its_default() -> None:
    only_serve = load({"serve": {"auth": {"backend": "token"}}}).model_dump()
    assert "serve" in only_serve and "orchestrator" not in only_serve
    explicit_default = load(
        {"orchestrator": {"type": "airflow", "instance": None, "options": {}}}
    ).model_dump()
    assert "orchestrator" not in explicit_default


def test_dumps_round_trip(clean_env: Path) -> None:
    cfg = load(SECTION_9_14)
    again = config_from_mapping(cfg.model_dump(mode="json", by_alias=True))
    assert again.model_dump(mode="json") == cfg.model_dump(mode="json")
    plain = load({"monitor": {"pipelines": ["a"]}})
    assert config_from_mapping(plain.model_dump(mode="json", by_alias=True)).monitor.dags == ["a"]


def test_check_prints_a_configured_orchestrator(
    clean_env: Path, tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    path = tmp_path / "healer.yaml"
    path.write_text(default_config_yaml(orchestrator="stepfunctions"), encoding="utf-8")
    assert main(["check", "--config", str(path)]) == 0
    printed = yaml.safe_load(
        capsys.readouterr().out.split("\n", 1)[1].rsplit("\nConfiguration is valid.", 1)[0]
    )
    assert printed["orchestrator"]["type"] == "stepfunctions"
    assert printed["orchestrator"]["options"]["pipelines"]["sales_daily_etl"]["logical_id"]
    assert printed["serve"]["auth"]["backend"] == "alb_oidc"
    assert printed["monitor"]["modes"] == ["poll", "queue"]


# =========================================================================== contracts


def _contract_with_aliases(path: Path) -> dict[str, Any]:
    raw = yaml.safe_load(path.read_text(encoding="utf-8"))
    raw["pipeline_id"] = raw.pop("dag_id")
    datasets = []
    for dataset in raw["datasets"]:
        dataset = dict(dataset)
        dataset["step_id"] = dataset.pop("task_id")
        datasets.append(dataset)
    raw["datasets"] = datasets
    return raw


def test_contract_aliases_keep_the_approved_hash() -> None:
    path = PHASE5 / "contracts" / "customers_sync.yaml"
    canonical = load_contract(path)
    aliased = parse_contract(_contract_with_aliases(path))
    assert canonical.source_hash == "f01a683db4d36f15d8ab4c145e10fc44c8bb5ad73886eaa91104a6d8be2a33b3"
    assert compute_source_hash(canonical) == canonical.source_hash == compute_source_hash(aliased)
    assert not is_stale(canonical) and not is_stale(aliased)
    assert (aliased.dag_id, aliased.datasets[0].task_id) == ("customers_sync", "extract")
    assert aliased == canonical
    assert contract_to_dict(aliased) == contract_to_dict(canonical)
    text = dump_contract_yaml(aliased)
    assert "dag_id: customers_sync" in text and "task_id: extract" in text
    assert "pipeline_id" not in text and "step_id" not in text


def test_contract_keys_and_aliases_are_exclusive() -> None:
    with pytest.raises(ContractError, match="'dag_id' and 'pipeline_id' are the same field"):
        parse_contract({"dag_id": "a", "pipeline_id": "a"})
    dataset = {"name": "d", "task_id": "t", "step_id": "t", "locator": {"type": "csv", "path": "d.csv"}}
    with pytest.raises(ContractError, match="'task_id' and 'step_id' are the same field"):
        parse_contract({"dag_id": "a", "datasets": [dataset]})
    with pytest.raises(ContractError, match="dag_id must not be empty"):
        parse_contract({"pipeline_id": " "})


def test_contract_models_keep_their_python_field_names() -> None:
    locator = DatasetLocator(type="csv", path="data/in.csv")
    dataset = DatasetContract(name="in", task_id="load", locator=locator)
    contract = Contract(dag_id="etl", datasets=[dataset])
    assert contract.datasets_for_task("load") == [dataset]
    assert (
        Contract.model_validate(
            {"pipeline_id": "etl", "datasets": [{"name": "in", "step_id": "load", "locator": locator}]}
        )
        == contract
    )
    assert contract.model_dump()["dag_id"] == "etl" and contract.model_dump(by_alias=True)["dag_id"] == "etl"
