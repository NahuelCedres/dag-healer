"""Static checks of the Docker stack files (docker/compose.yaml, Dockerfile, .env.example).

They guard the shared contracts (image tag, paths, env vars, profiles, the engine socket of the
test sandbox) without Docker.
"""

from __future__ import annotations

import re
from pathlib import Path
from typing import Any

import pytest
import yaml

REPO_ROOT = Path(__file__).resolve().parents[2]
DOCKER_DIR = REPO_ROOT / "docker"
AIRFLOW_SERVICES = (
    "airflow-init",
    "airflow-apiserver",
    "airflow-scheduler",
    "airflow-dag-processor",
    "airflow-triggerer",
    "healer-worker",
)
SECRETS = ("ANTHROPIC_API_KEY", "GITHUB_TOKEN", "GITHUB_REPO", "HEALER_AIRFLOW_PASSWORD")
#: The AWS settings of Claude on Amazon Bedrock: only healer-worker gets them, passed through.
BEDROCK_AWS_ENV = (
    "AWS_REGION",
    "AWS_DEFAULT_REGION",
    "AWS_ACCESS_KEY_ID",
    "AWS_SECRET_ACCESS_KEY",
    "AWS_SESSION_TOKEN",
    "AWS_BEARER_TOKEN_BEDROCK",
)


@pytest.fixture(scope="module")
def compose() -> dict[str, Any]:
    return yaml.safe_load((DOCKER_DIR / "compose.yaml").read_text(encoding="utf-8"))


@pytest.fixture(scope="module")
def services(compose: dict[str, Any]) -> dict[str, Any]:
    return compose["services"]


def test_project_and_services(compose: dict[str, Any], services: dict[str, Any]) -> None:
    assert compose["name"] == "dag-healer"
    assert set(services) == {
        "postgres",
        "mailpit",
        "tests",
        *AIRFLOW_SERVICES,
    }
    assert not {"redis", "airflow-worker", "flower"} & set(services)
    assert set(compose["volumes"]) == {"postgres-db-volume", "healer-home"}


def test_single_build_for_shared_image(services: dict[str, Any]) -> None:
    built = {name for name, svc in services.items() if "build" in svc}
    assert built == {"airflow-init"}
    assert services["airflow-init"]["build"] == {"context": "..", "dockerfile": "docker/Dockerfile"}
    for name in (*AIRFLOW_SERVICES, "tests"):
        assert services[name]["image"] == "${HEALER_IMAGE:-dag-healer/airflow:dev}", name
        assert services[name]["pull_policy"] == "never", name


@pytest.mark.parametrize("name", AIRFLOW_SERVICES)
def test_common_environment(services: dict[str, Any], name: str) -> None:
    env = services[name]["environment"]
    assert env["AIRFLOW__CORE__EXECUTOR"] == "LocalExecutor"
    assert env["AIRFLOW__CORE__AUTH_MANAGER"].endswith("fab_auth_manager.FabAuthManager")
    assert (
        env["AIRFLOW__DATABASE__SQL_ALCHEMY_CONN"] == "postgresql+psycopg2://airflow:airflow@postgres/airflow"
    )
    assert env["AIRFLOW__CORE__EXECUTION_API_SERVER_URL"] == "http://airflow-apiserver:8080/execution/"
    assert env["AIRFLOW__CORE__DAGS_FOLDER"] == "/opt/airflow/repo/dags"
    assert env["AIRFLOW__CORE__LOAD_EXAMPLES"] == "false"
    assert env["AIRFLOW__CORE__DAGS_ARE_PAUSED_AT_CREATION"] == "false"
    assert env["AIRFLOW__API__BASE_URL"] == "http://localhost:8080"
    assert env["AIRFLOW__CORE__FERNET_KEY"].startswith("${FERNET_KEY:-")
    assert env["AIRFLOW__API_AUTH__JWT_SECRET"].startswith("${AIRFLOW__API_AUTH__JWT_SECRET:-")
    assert env["HEALER_CONFIG"] == "/opt/airflow/repo/healer.yaml"
    assert env["HEALER_HOME"] == "/opt/airflow/healer"
    assert env["HEALER_DATABASE_URL"] == "postgresql+psycopg2://airflow:airflow@postgres/healer"
    assert env["HEALER_API_URL"] == "http://airflow-apiserver:8080"
    assert env["HEALER_INGEST_TOKEN"].startswith("${HEALER_INGEST_TOKEN:-")
    assert env["HEALER_SMTP_HOST"] == "mailpit"
    assert env["HEALER_SMTP_PORT"] == "1025"
    count = int(env["GIT_CONFIG_COUNT"])
    pairs = {env[f"GIT_CONFIG_KEY_{i}"]: env[f"GIT_CONFIG_VALUE_{i}"] for i in range(count)}
    assert pairs["safe.directory"] == "*"


def test_mounts_and_user(services: dict[str, Any]) -> None:
    for name in AIRFLOW_SERVICES:
        svc = services[name]
        assert "${HEALER_REPO_DIR:-../.demo/repo}:/opt/airflow/repo" in svc["volumes"]
        assert "${HEALER_LOGS_DIR:-../.demo/logs}:/opt/airflow/logs" in svc["volumes"]
        assert "healer-home:/opt/airflow/healer" in svc["volumes"]
        expected_user = "0:0" if name == "airflow-init" else "${AIRFLOW_UID:-50000}:0"
        assert svc["user"] == expected_user
    assert "./initdb:/docker-entrypoint-initdb.d:ro" in services["postgres"]["volumes"]


def test_secrets_only_where_needed(services: dict[str, Any]) -> None:
    for name in ("airflow-apiserver", "healer-worker"):
        env = services[name]["environment"]
        assert set(SECRETS) <= set(env), name
        assert env["HEALER_AIRFLOW_USERNAME"] == "${HEALER_AIRFLOW_USERNAME:-admin}"
    for name in ("airflow-scheduler", "airflow-dag-processor", "airflow-triggerer", "airflow-init"):
        assert not set(SECRETS) & set(services[name]["environment"]), name


def test_ports_and_healthchecks(services: dict[str, Any]) -> None:
    assert services["airflow-apiserver"]["ports"] == ["8080:8080"]
    assert set(services["mailpit"]["ports"]) == {"8025:8025", "1025:1025"}
    assert (
        "http://localhost:8080/api/v2/monitor/health" in services["airflow-apiserver"]["healthcheck"]["test"]
    )
    for name in ("airflow-scheduler", "airflow-dag-processor", "airflow-triggerer"):
        assert "airflow jobs check" in services[name]["healthcheck"]["test"][1]
        assert services[name]["depends_on"]["airflow-init"]["condition"] == "service_completed_successfully"


def test_airflow_init_creates_users_and_database(services: dict[str, Any]) -> None:
    init = services["airflow-init"]
    env = init["environment"]
    assert env["_AIRFLOW_DB_MIGRATE"] == "true"
    assert env["_AIRFLOW_WWW_USER_CREATE"] == "true"
    assert env["_AIRFLOW_WWW_USER_USERNAME"] == "${_AIRFLOW_WWW_USER_USERNAME:-admin}"
    assert env["_AIRFLOW_WWW_USER_PASSWORD"] == "${_AIRFLOW_WWW_USER_PASSWORD:-admin}"
    script = init["command"][1]
    assert "--username viewer --password viewer --role Viewer" in script
    assert "HEALER_DATABASE_URL" in script
    assert "chown" in script
    # Every "$" meant for bash must be escaped for compose interpolation.
    unescaped = re.findall(r"(?<!\$)\$(?![${])", script.replace("$$", ""))
    assert unescaped == []


def test_profiles(services: dict[str, Any]) -> None:
    worker = services["healer-worker"]
    assert "profiles" not in worker  # the worker starts with the stack
    assert worker["command"] == ["dag-healer", "worker"]
    # the image entrypoint would otherwise run `airflow dag-healer worker`
    assert worker["entrypoint"][:5] == ["/usr/bin/dumb-init", "--", "/entrypoint", "bash", "-c"]
    # it waits for healer.yaml, then execs the command
    script = worker["entrypoint"][5]
    assert '[ -f "$${HEALER_CONFIG}" ]' in script and 'exec "$$@"' in script
    assert worker["restart"] == "unless-stopped"
    assert worker["depends_on"]["airflow-apiserver"]["condition"] == "service_healthy"
    tests = services["tests"]
    assert tests["profiles"] == ["tests"]
    assert tests["working_dir"] == "/opt/dag-healer"
    assert "../tests:/opt/dag-healer/tests:ro" in tests["volumes"]
    assert tests["command"][:3] == ["python", "-m", "pytest"]
    assert tests["command"][-1] == "tests/airflow"


def test_dockerfile_stages() -> None:
    text = (DOCKER_DIR / "Dockerfile").read_text(encoding="utf-8")
    stages = re.findall(r"^FROM\s+(\S+)(?:\s+AS\s+(\S+))?", text, flags=re.MULTILINE)
    assert [name for _, name in stages] == ["ui", "ui-export", "wheel", "airflow"]
    assert "ARG AIRFLOW_VERSION=3.3.2" in text
    assert "ARG NODE_IMAGE=node:22-alpine" in text
    assert "ARG PYTHON_IMAGE=python:3.12-slim" in text
    assert stages[-1][0] == "apache/airflow:${AIRFLOW_VERSION}"
    assert "pnpm install --frozen-lockfile" in text
    assert 'pip install --no-cache-dir "apache-airflow==${AIRFLOW_VERSION}"' in text
    assert "src/dag_healer/api/static/main.umd.cjs" in text
    assert "/opt/dag-healer/tests/airflow/" in text


def test_env_example_and_initdb() -> None:
    env_text = (DOCKER_DIR / ".env.example").read_text(encoding="utf-8")
    keys = set(re.findall(r"^([A-Z_][A-Z0-9_]*)=", env_text, flags=re.MULTILINE))
    assert {
        "AIRFLOW_UID",
        "FERNET_KEY",
        "AIRFLOW__API_AUTH__JWT_SECRET",
        "HEALER_INGEST_TOKEN",
        "ANTHROPIC_API_KEY",
        "GITHUB_TOKEN",
        "GITHUB_REPO",
        "HEALER_DOCKER_GID",
    } <= keys
    assert re.search(r"^AIRFLOW_UID=50000$", env_text, flags=re.MULTILINE)
    assert re.search(r"^HEALER_DOCKER_GID=0$", env_text, flags=re.MULTILINE)
    sql = (DOCKER_DIR / "initdb" / "01-healer.sql").read_text(encoding="utf-8")
    assert "CREATE DATABASE healer" in sql


def test_dockerignore_keeps_context_small() -> None:
    lines = {
        line.strip()
        for line in (REPO_ROOT / ".dockerignore").read_text(encoding="utf-8").splitlines()
        if line.strip() and not line.startswith("#")
    }
    expected = {".git", ".venv", ".demo", "ui/node_modules", "ui/dist", "ui/.pnpm-store", "docker/.env"}
    assert expected <= lines


def test_worker_environment(services: dict[str, Any]) -> None:
    env = services["healer-worker"]["environment"]
    # offline fake LLM unless docker/.env selects Claude
    assert env["HEALER_LLM_PROVIDER"] == "${HEALER_LLM_PROVIDER:-fake}"
    assert services["airflow-apiserver"]["environment"]["HEALER_LLM_PROVIDER"] == env["HEALER_LLM_PROVIDER"]
    # the model of the generated healer.yaml, passed through (unset: the template's default)
    assert env["HEALER_LLM_MODEL"] is None
    assert services["airflow-apiserver"]["environment"]["HEALER_LLM_MODEL"] is None
    assert env["HEALER_FAKE_LLM_DIR"] == "/opt/airflow/repo/fixtures/fake_llm"
    for key in ("GIT_AUTHOR_NAME", "GIT_COMMITTER_NAME"):
        assert env[key] == "dag-healer"
    for key in ("GIT_AUTHOR_EMAIL", "GIT_COMMITTER_EMAIL"):
        assert env[key] == "dag-healer@localhost"
    assert env["PYTHONPATH"] == ""


def test_scheduler_can_post_listener_events(services: dict[str, Any]) -> None:
    """The task runner (scheduler, LocalExecutor) reaches the ingest endpoint, nothing more."""
    env = services["airflow-scheduler"]["environment"]
    assert env["HEALER_API_URL"] == "http://airflow-apiserver:8080"
    assert env["HEALER_INGEST_TOKEN"].startswith("${HEALER_INGEST_TOKEN:-")
    assert "HEALER_LLM_PROVIDER" not in env and "ANTHROPIC_API_KEY" not in env
    assert "HEALER_LLM_MODEL" not in env


def test_only_the_worker_gets_the_aws_settings_of_bedrock(services: dict[str, Any]) -> None:
    """Passed through from docker/.env or the shell (unset when undefined), to healer-worker only:
    the scheduler runs task code, and doctor for bedrock runs in the worker."""
    worker = services["healer-worker"]["environment"]
    assert {name: worker[name] for name in BEDROCK_AWS_ENV} == dict.fromkeys(BEDROCK_AWS_ENV)
    # no profile (the container has no ~/.aws) and no endpoint override (an empty one breaks the SDK)
    assert not {"AWS_PROFILE", "AWS_DEFAULT_PROFILE", "ANTHROPIC_BEDROCK_BASE_URL"} & set(worker)
    for name, svc in services.items():
        if name == "healer-worker":
            continue
        env = svc.get("environment") or {}
        assert not [key for key in env if key.startswith(("AWS_", "ANTHROPIC_BEDROCK"))], name


def test_only_the_worker_gets_the_engine_socket(services: dict[str, Any]) -> None:
    """The test sandbox: healer-worker alone gets the engine socket, its group and the image name."""
    worker = services["healer-worker"]
    assert "${HEALER_DOCKER_SOCKET:-/var/run/docker.sock}:/var/run/docker.sock" in worker["volumes"]
    assert worker["group_add"] == ["${HEALER_DOCKER_GID:-0}"]
    assert worker["environment"]["HEALER_SANDBOX_IMAGE"] == "${HEALER_IMAGE:-dag-healer/airflow:dev}"
    for name, svc in services.items():
        if name == "healer-worker":
            continue
        assert not any(".sock" in str(volume) for volume in svc.get("volumes") or []), name
        assert "group_add" not in svc, name
        assert "HEALER_SANDBOX_IMAGE" not in (svc.get("environment") or {}), name
    # the start-up hint about an unusable socket keeps every "$" escaped for compose
    script = worker["entrypoint"][5]
    assert "/var/run/docker.sock" in script
    assert re.findall(r"(?<!\$)\$(?![${])", script.replace("$$", "")) == []


def test_dockerfile_copies_a_pinned_docker_cli() -> None:
    text = (DOCKER_DIR / "Dockerfile").read_text(encoding="utf-8")
    copy = re.search(
        r"^COPY --from=docker:\d+\.\d+\.\d+-cli /usr/local/bin/docker /usr/local/bin/docker$",
        text,
        flags=re.MULTILINE,
    )
    assert copy is not None, "the docker CLI comes from a pinned docker:<x.y.z>-cli image"
    assert copy.start() > text.index("FROM apache/airflow:")  # in the final stage
    assert "RUN docker --version" in text
