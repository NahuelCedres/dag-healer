"""``dag-healer doctor`` (environment checks) and ``dag-healer check`` (configuration dump).

``doctor`` runs every check and exits 1 when at least one of them reports an error.
Secrets are never printed: only whether they are set. The generic checks (configuration,
repository, contracts, store, UI bundle, LLM, test sandbox) come first, then the
``orchestrator`` check (the configured adapter loads and can serve ``monitor.modes`` and
``monitor.trigger``) and the adapter's own checks (``adapter.doctor_checks()``: for Airflow the
API, the poller's login and the plugin entry point), then SMTP, secrets, ingest and GitHub.

With a GitHub token and a repository (``github.repo`` or a GitHub ``origin`` remote), the
read-only GitHub setup checks of :mod:`dag_healer.pr.github_checks` follow, one line per
finding: ``github token``, ``github repository``, ``github base branch`` and ``github
workflows`` (the ``.github/workflows`` files of ``repo.path``). An error there (a base branch
that does not require a reviewed pull request or allows force pushes or deletion, a workflow
that can run the healer branch's code with the repository's secrets) makes doctor exit 1; what
could not be checked is a warning, and checks that fail unexpectedly are one ``github setup``
warning. Without a token or a repository nothing is called.

For the container backend the ``sandbox`` check starts one container of the configured image
(no repository code in it) to prove that it runs as a non-root user, without network, with
pytest importable. ``doctor`` and ``check`` warn about the ``process`` backend, which runs the
repairs' tests without any isolation.
"""

from __future__ import annotations

import argparse
import contextlib
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
    import httpx

    from dag_healer.config import HealerConfig
    from dag_healer.core.ports import OrchestratorAdapter

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


def check_sandbox(cfg: HealerConfig) -> CheckResult:
    """The test sandbox of the CODE repairs (``repo.sandbox``) can run code, isolated.

    ``process`` is a warning (no isolation at all). For a container backend, a reason why it
    cannot run (no image, no engine, the image missing...) is an error that names the fix;
    otherwise a smoke run in one container must show a non-root user, no network and pytest.
    """
    from dag_healer.repo.sandbox import PROCESS_BACKEND_WARNING, build_sandbox

    sandbox = build_sandbox(cfg.repo.sandbox)
    if not sandbox.isolated:
        return CheckResult("sandbox", WARN, PROCESS_BACKEND_WARNING)
    reason = sandbox.unavailable_reason()
    if reason is not None:
        return CheckResult("sandbox", ERROR, reason)
    probe = sandbox.smoke_test()
    return CheckResult("sandbox", OK if probe.ok else ERROR, probe.detail)


def check_orchestrator(cfg: HealerConfig) -> tuple[CheckResult, OrchestratorAdapter | None]:
    """The configured orchestrator adapter loads and can serve ``monitor.modes`` / ``monitor.trigger``.

    Returns the result and a new adapter (``None`` when it cannot be built; building it does no
    I/O). The caller runs the adapter's own checks and closes it.
    """
    from dag_healer.orchestrators import registry

    try:
        settings = registry.orchestrator_settings(cfg)
        adapter = registry.ensure_adapter(registry.load_factory(settings.type).create(cfg), settings.type)
    except registry.AdapterLoadError as exc:
        return CheckResult("orchestrator", ERROR, str(exc)), None
    problems = registry.config_problems(cfg, adapter)
    if problems:
        return CheckResult("orchestrator", ERROR, "; ".join(problems)), adapter
    detail = f"{adapter.display_name} (type {adapter.type}, instance {adapter.instance})"
    return CheckResult("orchestrator", OK, detail), adapter


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
    if not cfg.monitor.wants("poll"):
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
    if not cfg.monitor.wants("push"):
        return CheckResult("ingest", SKIP, "listener/callback modes disabled")
    if cfg.ingest.token or os.environ.get("HEALER_INGEST_TOKEN", "").strip():
        return CheckResult("ingest", OK, "ingest token is set (value hidden)")
    return CheckResult(
        "ingest", WARN, "HEALER_INGEST_TOKEN not set; listener events are rejected (REST polling still works)"
    )


def _github_repository(cfg: HealerConfig) -> str | None:
    """``github.repo``, else the repository of a GitHub ``origin`` remote (as a repair finds it)."""
    if cfg.github.repo:
        return cfg.github.repo
    from dag_healer.repo.git import origin_url, parse_github_repo

    origin = origin_url(cfg.repo_root)
    return parse_github_repo(origin) if origin else None


def _github_transport() -> httpx.BaseTransport | None:
    """The httpx transport of the GitHub setup checks (None: the network; tests replace it)."""
    return None


def check_github(cfg: HealerConfig) -> CheckResult:
    """Pull requests need a repository (``github.repo`` or a GitHub origin remote) and a token.

    Otherwise fixes become PR.md files.
    """
    if cfg.github.repo and cfg.github.token:
        return CheckResult("github", OK, f"pull requests on {cfg.github.repo} (token hidden)")
    if cfg.github.repo:
        return CheckResult(
            "github", WARN, f"github.repo is {cfg.github.repo} but no token is set; using PR.md"
        )
    if cfg.github.token:
        repo = _github_repository(cfg)
        if repo:
            return CheckResult(
                "github", OK, f"pull requests on {repo} (from the origin remote; token hidden)"
            )
    return CheckResult("github", OK, "no GitHub repo configured; fixes are written as PR.md + fix.patch")


def github_setup_checks(cfg: HealerConfig) -> list[CheckResult]:
    """The read-only GitHub setup checks, as ``github <check>`` lines (see the module docstring).

    Nothing is called without a token and a repository: the ``github`` line already says that
    fixes become PR.md files. A check without findings is one OK line with its summary; otherwise
    each finding is a line of its own, an error or a warning, that says what to change.
    """
    token = cfg.github.token
    repo = _github_repository(cfg) if token else None
    if not token or not repo:
        return []
    from dag_healer.pr.github_checks import check_github_setup

    checks = check_github_setup(
        repo=repo,
        base=cfg.github.pr_base,
        token=token,
        skip_ci=cfg.github.skip_ci,
        workflows_root=cfg.repo_root,
        transport=_github_transport(),
        timeout=NETWORK_TIMEOUT_SECONDS,
    )
    results: list[CheckResult] = []
    for check in checks:
        name = f"github {check.name}"
        if not check.findings:
            results.append(CheckResult(name, OK, check.summary))
        results += [CheckResult(name, finding.level, finding.text) for finding in check.findings]
    return results


def _github_setup(cfg: HealerConfig) -> list[CheckResult]:
    """:func:`github_setup_checks`, guarded: a failure becomes one ``github setup`` warning.

    The warning names the exception's type only: its message could quote the token (httpx
    quotes an invalid header value in its own form, which a replace() of the token misses).
    """
    try:
        return github_setup_checks(cfg)
    except Exception as exc:  # a broken check must not hide the others
        from dag_healer.pr.github_checks import setup_failure

        finding = setup_failure(exc)
        return [CheckResult(f"github {finding.check}", finding.level, finding.text)]


# --------------------------------------------------------------------------- doctor


def _guarded(name: str, run: Callable[[], Any]) -> CheckResult:
    """Run one check; an exception (or a result that is not a CheckResult) becomes an error."""
    try:
        result = run()
    except Exception as exc:  # a broken check must not hide the others
        return CheckResult(name, ERROR, f"{type(exc).__name__}: {exc}")
    if not isinstance(result, CheckResult) or result.status not in _LABELS:
        return CheckResult(name, ERROR, f"the check returned {type(result).__name__}, not a CheckResult")
    return result


def _adapter_checks(adapter: OrchestratorAdapter) -> list[CheckResult]:
    """Run ``adapter.doctor_checks()`` in the adapter's order, then close the adapter."""
    try:
        return [_guarded(str(check.name), check.run) for check in adapter.doctor_checks()]
    except Exception as exc:  # doctor_checks() itself failed: report it, keep the other checks
        return [CheckResult(f"{adapter.type} checks", ERROR, f"{type(exc).__name__}: {exc}")]
    finally:
        with contextlib.suppress(Exception):  # closing is best effort
            adapter.close()


def run_checks(config_path: str | None = None) -> list[CheckResult]:
    """Run every doctor check and return the results in display order (see the module docstring)."""
    config_result, cfg = check_config(config_path)
    results = [config_result]
    if cfg is None:
        results += [check_bundle(), check_plugin_entry_point(), check_llm(None)]
        return results
    results += [
        _guarded("repository", lambda: check_repo(cfg)),
        _guarded("contracts", lambda: check_contracts(cfg)),
        _guarded("store", lambda: check_store(cfg)),
        _guarded("ui bundle", check_bundle),
        _guarded("llm", lambda: check_llm(cfg)),
        _guarded("sandbox", lambda: check_sandbox(cfg)),
    ]
    adapter: OrchestratorAdapter | None = None
    try:
        orchestrator_result, adapter = check_orchestrator(cfg)
    except Exception as exc:  # a broken adapter factory must not hide the other checks
        orchestrator_result = CheckResult("orchestrator", ERROR, f"{type(exc).__name__}: {exc}")
    results.append(orchestrator_result)
    if adapter is not None:
        results += _adapter_checks(adapter)
    results += [
        _guarded("smtp", lambda: check_smtp(cfg)),
        _guarded("secrets", lambda: check_secret_literals(cfg)),
        _guarded("ingest", lambda: check_ingest(cfg)),
        _guarded("github", lambda: check_github(cfg)),
    ]
    results += _github_setup(cfg)
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
    if cfg.repo.sandbox.backend == "process":
        from dag_healer.repo.sandbox import PROCESS_BACKEND_WARNING

        warn(PROCESS_BACKEND_WARNING)
    print("\nConfiguration is valid.")
    return 0


def _configure(parser: argparse.ArgumentParser) -> None:
    add_config_option(parser)


register_command(
    "doctor",
    help=(
        "Check configuration, store, UI bundle, test sandbox, Airflow API, SMTP, secrets and the "
        "GitHub setup."
    ),
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
    "check_orchestrator",
    "check_plugin_entry_point",
    "check_repo",
    "check_sandbox",
    "check_secret_literals",
    "check_smtp",
    "check_store",
    "effective_settings",
    "format_results",
    "github_setup_checks",
    "run_checks",
]
