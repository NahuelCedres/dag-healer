"""``dag-healer doctor`` (environment checks) and ``dag-healer check`` (configuration dump).

``doctor`` runs every check and exits 1 when at least one of them reports an error.
Secrets are never printed: only whether they are set.
"""

from __future__ import annotations

import argparse
import os
import socket
from collections.abc import Callable
from dataclasses import dataclass
from pathlib import Path
from typing import TYPE_CHECKING, Any

from dag_healer._version import __version__
from dag_healer.cli._output import MASK, add_config_option, config_arg, error, load_cli_config, mask_url, warn
from dag_healer.cli.main import register_command

if TYPE_CHECKING:
    from dag_healer.config import HealerConfig

OK = "ok"
WARN = "warn"
ERROR = "error"
SKIP = "skip"

NETWORK_TIMEOUT_SECONDS = 3.0
API_KEY_ENV = "ANTHROPIC_API_KEY"
_LABELS = {OK: "[ OK ]", WARN: "[WARN]", ERROR: "[FAIL]", SKIP: "[SKIP]"}


@dataclass
class CheckResult:
    """Outcome of one doctor check."""

    name: str
    status: str
    detail: str


# --------------------------------------------------------------------------- individual checks


def check_config(config_path: str | None) -> tuple[CheckResult, HealerConfig | None]:
    """Load the configuration; a missing file is a warning, an invalid one an error."""
    from dag_healer.config import ConfigError, load_config

    try:
        cfg = load_config(config_path)
    except ConfigError as exc:
        return CheckResult("config", ERROR, str(exc)), None
    if cfg.source_path is None:
        return CheckResult("config", WARN, "no healer.yaml found; using defaults (run: dag-healer init)"), cfg
    return CheckResult("config", OK, f"loaded {cfg.source_path}"), cfg


def check_repo(cfg: HealerConfig) -> CheckResult:
    """The repository and its DAG folder exist."""
    if not cfg.repo_root.is_dir():
        return CheckResult("repository", ERROR, f"repo.path does not exist: {cfg.repo_root}")
    dags = cfg.dags_path
    if not dags.is_dir():
        return CheckResult("repository", ERROR, f"DAG folder does not exist: {dags}")
    return CheckResult("repository", OK, f"{cfg.repo_root} (DAG folder: {cfg.repo.dags_dir})")


def check_contracts(cfg: HealerConfig) -> CheckResult:
    """Count approved / draft / stale / invalid contracts."""
    from dag_healer.contracts.io import is_stale, load_contract
    from dag_healer.contracts.schema import ContractError

    directory = cfg.contracts_dir
    if not directory.is_dir():
        return CheckResult("contracts", WARN, f"no contracts directory at {directory} (run: dag-healer init)")
    files = sorted([*directory.glob("*.yaml"), *directory.glob("*.yml")])
    approved = drafts = 0
    stale: list[str] = []
    invalid: list[str] = []
    for file in files:
        try:
            contract = load_contract(file)
        except ContractError:
            invalid.append(file.name)
            continue
        if contract.is_approved:
            approved += 1
            if is_stale(contract):
                stale.append(contract.dag_id)
        else:
            drafts += 1
    detail = f"{directory}: {approved} approved, {drafts} draft"
    if invalid:
        return CheckResult("contracts", WARN, f"{detail}; invalid files (ignored): {', '.join(invalid)}")
    if stale:
        return CheckResult("contracts", WARN, f"{detail}; stale (edited after approval): {', '.join(stale)}")
    if not files:
        return CheckResult("contracts", WARN, f"{directory} is empty (run: dag-healer init)")
    if approved == 0:
        return CheckResult("contracts", WARN, f"{detail}; nothing is enforced until a contract is approved")
    return CheckResult("contracts", OK, detail)


def check_store(cfg: HealerConfig) -> CheckResult:
    """The incident store answers ``SELECT 1``."""
    url = cfg.database_url
    try:
        from sqlalchemy import text

        from dag_healer.store import get_engine

        with get_engine(url).connect() as connection:
            connection.execute(text("SELECT 1"))
    except Exception as exc:  # driver-specific exception types
        message = str(exc).strip().splitlines()[0] if str(exc).strip() else ""
        return CheckResult("store", ERROR, f"{mask_url(url)}: {type(exc).__name__}: {message}"[:400])
    note = (
        " (SQLite: single process only; set HEALER_DATABASE_URL for containers)"
        if url.startswith("sqlite")
        else ""
    )
    return CheckResult("store", OK, f"{mask_url(url)}{note}")


def bundle_path() -> Path:
    """Location of the UI bundle inside the installed package."""
    import dag_healer

    return Path(dag_healer.__file__).resolve().parent / "api" / "static" / "main.umd.cjs"


def check_bundle() -> CheckResult:
    """The React bundle served at /healer/static/main.umd.cjs is present."""
    path = bundle_path()
    if not path.is_file() or path.stat().st_size == 0:
        return CheckResult("ui bundle", ERROR, f"missing {path} (build it: python scripts/tasks.py ui)")
    return CheckResult("ui bundle", OK, f"{path} ({path.stat().st_size:,} bytes)")


def check_plugin_entry_point() -> CheckResult:
    """The ``airflow.plugins`` entry point is registered (Airflow discovers the plugin)."""
    from importlib.metadata import entry_points

    for entry in entry_points(group="airflow.plugins"):
        if entry.name == "dag_healer":
            return CheckResult("airflow plugin", OK, f"entry point {entry.value}")
    return CheckResult(
        "airflow plugin", WARN, "entry point 'dag_healer' not found (is dag-healer pip-installed here?)"
    )


def check_llm(cfg: HealerConfig | None) -> CheckResult:
    """An API key is available for the configured LLM provider (the value is never shown)."""
    provider = cfg.llm.provider if cfg is not None else "anthropic"
    if provider == "fake":
        return CheckResult("llm", OK, "provider 'fake' (offline); no API key needed")
    if os.environ.get(API_KEY_ENV, "").strip():
        model = cfg.llm.model if cfg is not None else "?"
        return CheckResult("llm", OK, f"{API_KEY_ENV} is set (value hidden); model {model}")
    return CheckResult("llm", ERROR, f"{API_KEY_ENV} is not set; the worker cannot classify or repair")


def _http_get(url: str) -> Any:
    import httpx

    return httpx.get(url, timeout=NETWORK_TIMEOUT_SECONDS, headers={"Accept": "application/json"})


def _http_post(url: str, payload: dict[str, Any]) -> Any:
    import httpx

    return httpx.post(url, json=payload, timeout=NETWORK_TIMEOUT_SECONDS)


def check_airflow_api(cfg: HealerConfig) -> CheckResult:
    """``GET <airflow.api_url>/api/v2/monitor/health`` answers 200 within 3 s."""
    url = f"{cfg.airflow.api_url}/api/v2/monitor/health"
    try:
        response = _http_get(url)
    except Exception as exc:  # httpx raises several transport errors
        return CheckResult("airflow api", ERROR, f"{url} unreachable: {type(exc).__name__}: {exc}"[:400])
    if response.status_code != 200:
        return CheckResult("airflow api", ERROR, f"{url} answered HTTP {response.status_code}")
    try:
        body = response.json()
    except ValueError:
        body = {}
    components = {
        name: (info or {}).get("status")
        for name, info in (body.items() if isinstance(body, dict) else [])
        if isinstance(info, dict) or info is None
    }
    summary = ", ".join(f"{name}={status}" for name, status in components.items() if status)
    unhealthy = [name for name, status in components.items() if status and status != "healthy"]
    if unhealthy:
        return CheckResult("airflow api", WARN, f"{url}: {summary}")
    return CheckResult("airflow api", OK, f"{url}" + (f": {summary}" if summary else ""))


def check_airflow_auth(cfg: HealerConfig, api_reachable: bool) -> CheckResult:
    """The REST poller's credentials obtain a token (``POST /auth/token``)."""
    if "rest" not in cfg.monitor.modes:
        return CheckResult("airflow auth", SKIP, "REST polling disabled (monitor.modes)")
    username, password = cfg.airflow.username, cfg.airflow.password
    if not username or not password:
        return CheckResult(
            "airflow auth", WARN, "airflow.username/password not set; the worker cannot poll the REST API"
        )
    if not api_reachable:
        return CheckResult("airflow auth", SKIP, "Airflow API unreachable")
    url = f"{cfg.airflow.api_url}/auth/token"
    try:
        response = _http_post(url, {"username": username, "password": password})
    except Exception as exc:
        return CheckResult("airflow auth", ERROR, f"{url}: {type(exc).__name__}: {exc}"[:400])
    if response.status_code in (200, 201):
        return CheckResult("airflow auth", OK, f"token issued for user '{username}'")
    return CheckResult("airflow auth", ERROR, f"{url} answered HTTP {response.status_code} for '{username}'")


def check_smtp(cfg: HealerConfig) -> CheckResult:
    """The SMTP host accepts a TCP connection within 3 s."""
    email = cfg.notify.email
    if email is None:
        return CheckResult("smtp", SKIP, "email alerts disabled (notify.email.host is empty)")
    try:
        with socket.create_connection((email.host, email.port), timeout=NETWORK_TIMEOUT_SECONDS):
            pass
    except OSError as exc:
        return CheckResult("smtp", ERROR, f"{email.host}:{email.port} unreachable: {exc}")
    return CheckResult("smtp", OK, f"{email.host}:{email.port} reachable; alerts to {', '.join(email.to)}")


def check_secret_literals(cfg: HealerConfig) -> CheckResult:
    """Secrets in healer.yaml must be ``${ENV}`` references, never literal values."""
    literals = cfg.secret_literals()
    if literals:
        return CheckResult(
            "secrets",
            WARN,
            f"literal secret values in healer.yaml: {', '.join(literals)} (use ${{ENV}} references)",
        )
    return CheckResult("secrets", OK, "no literal secrets in the configuration")


def check_ingest(cfg: HealerConfig) -> CheckResult:
    """The listener / callback fast path needs a shared ingest token."""
    push_modes = {"listener", "callback"} & set(cfg.monitor.modes)
    if not push_modes:
        return CheckResult("ingest", SKIP, "listener/callback modes disabled")
    if cfg.ingest.token or os.environ.get("HEALER_INGEST_TOKEN", "").strip():
        return CheckResult("ingest", OK, "ingest token is set (value hidden)")
    return CheckResult(
        "ingest", WARN, "HEALER_INGEST_TOKEN not set; listener events are rejected (REST polling still works)"
    )


def check_github(cfg: HealerConfig) -> CheckResult:
    """Pull requests need ``github.repo`` and a token; otherwise fixes become PR.md files."""
    if cfg.github.repo and cfg.github.token:
        return CheckResult("github", OK, f"pull requests on {cfg.github.repo} (token hidden)")
    if cfg.github.repo:
        return CheckResult(
            "github", WARN, f"github.repo is {cfg.github.repo} but no token is set; using PR.md"
        )
    return CheckResult("github", OK, "no GitHub repo configured; fixes are written as PR.md + fix.patch")


# --------------------------------------------------------------------------- doctor


def run_checks(config_path: str | None = None) -> list[CheckResult]:
    """Run every doctor check and return the results in display order."""
    config_result, cfg = check_config(config_path)
    results = [config_result]
    if cfg is None:
        results += [check_bundle(), check_plugin_entry_point(), check_llm(None)]
        return results
    api_result = check_airflow_api(cfg)
    steps: list[Callable[[], CheckResult]] = [
        lambda: check_repo(cfg),
        lambda: check_contracts(cfg),
        lambda: check_store(cfg),
        check_bundle,
        check_plugin_entry_point,
        lambda: check_llm(cfg),
        lambda: api_result,
        lambda: check_airflow_auth(cfg, api_result.status != ERROR),
        lambda: check_smtp(cfg),
        lambda: check_secret_literals(cfg),
        lambda: check_ingest(cfg),
        lambda: check_github(cfg),
    ]
    for step in steps:
        try:
            results.append(step())
        except Exception as exc:  # a broken check must not hide the others
            results.append(
                CheckResult(getattr(step, "__name__", "check"), ERROR, f"{type(exc).__name__}: {exc}")
            )
    return results


def format_results(results: list[CheckResult]) -> str:
    """Render doctor results as aligned lines plus a summary."""
    width = max((len(result.name) for result in results), default=0)
    lines = [f"dag-healer doctor ({__version__})"]
    lines += [f"{_LABELS[r.status]} {r.name.ljust(width)}  {r.detail}" for r in results]
    counts = {status: sum(1 for r in results if r.status == status) for status in _LABELS}
    lines.append(
        f"\n{counts[OK]} ok, {counts[WARN]} warning(s), {counts[ERROR]} error(s), {counts[SKIP]} skipped"
    )
    return "\n".join(lines)


def _cmd_doctor(args: argparse.Namespace) -> int:
    results = run_checks(config_arg(args))
    print(format_results(results))
    return 1 if any(result.status == ERROR for result in results) else 0


# --------------------------------------------------------------------------- check


def effective_settings(cfg: HealerConfig) -> dict[str, Any]:
    """Effective configuration as plain data, with secrets masked and derived paths added."""
    from dag_healer.config import SECRET_FIELDS

    data = cfg.model_dump(mode="json", by_alias=True)
    for dotted in SECRET_FIELDS:
        node: Any = data
        *parents, leaf = dotted.split(".")
        for part in parents:
            node = node.get(part) if isinstance(node, dict) else None
        if isinstance(node, dict) and node.get(leaf):
            node[leaf] = MASK
    if data.get("store", {}).get("url"):
        data["store"]["url"] = mask_url(data["store"]["url"])
    data["derived"] = {
        "source_path": str(cfg.source_path) if cfg.source_path else None,
        "repo_root": str(cfg.repo_root),
        "dags_path": str(cfg.dags_path),
        "contracts_dir": str(cfg.contracts_dir),
        "healer_home": str(cfg.healer_home),
        "database_url": mask_url(cfg.database_url),
    }
    return data


def _cmd_check(args: argparse.Namespace) -> int:
    import yaml

    from dag_healer.config import ConfigError

    try:
        cfg = load_cli_config(args)
    except ConfigError as exc:
        error(str(exc))
        return 1
    source = cfg.source_path or "no healer.yaml found, using defaults"
    print(f"# Effective dag-healer configuration ({source}); secrets are shown as {MASK}")
    print(yaml.safe_dump(effective_settings(cfg), sort_keys=False, default_flow_style=False).rstrip())
    literals = cfg.secret_literals()
    if literals:
        warn(f"literal secret values in healer.yaml: {', '.join(literals)} (use ${{ENV}} references)")
    print("\nConfiguration is valid.")
    return 0


def _configure(parser: argparse.ArgumentParser) -> None:
    add_config_option(parser)


register_command(
    "doctor",
    help="Check configuration, store, UI bundle, Airflow API, SMTP and secrets.",
    handler=_cmd_doctor,
    configure=_configure,
)
register_command(
    "check",
    help="Validate healer.yaml and print the effective settings (secrets masked).",
    handler=_cmd_check,
    configure=_configure,
)

__all__ = [
    "CheckResult",
    "bundle_path",
    "check_airflow_api",
    "check_airflow_auth",
    "check_bundle",
    "check_config",
    "check_contracts",
    "check_github",
    "check_ingest",
    "check_llm",
    "check_plugin_entry_point",
    "check_repo",
    "check_secret_literals",
    "check_smtp",
    "check_store",
    "effective_settings",
    "format_results",
    "run_checks",
]
