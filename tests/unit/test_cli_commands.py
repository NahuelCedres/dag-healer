"""CLI commands other than init: check, doctor, contracts, incidents and worker."""

from __future__ import annotations

import json
import logging
import shutil
import sys
from collections.abc import Iterator
from pathlib import Path
from typing import Any

import pytest

from dag_healer.cli import doctor_cmd
from dag_healer.cli._output import mask_url, render_table
from dag_healer.cli.main import main
from dag_healer.contracts.io import contract_path, load_contract, save_contract

EXAMPLE_REPO = Path(__file__).resolve().parents[2] / "example_existing_repo"
SECRET = "sk-ant-do-not-print-me-123"


@pytest.fixture
def isolated(healer_env: Path, monkeypatch: pytest.MonkeyPatch) -> Path:
    """Isolated env; also undoes the HEALER_CONFIG export done by main(-c ...)."""
    monkeypatch.setenv("HEALER_CONFIG", "")
    monkeypatch.delenv("HEALER_CONFIG")
    for name in (
        "ANTHROPIC_API_KEY",
        "HEALER_LLM_PROVIDER",
        "HEALER_SMTP_HOST",
        "HEALER_AIRFLOW_USERNAME",
        "HEALER_AIRFLOW_PASSWORD",
        "GITHUB_TOKEN",
        "GITHUB_REPO",
    ):
        monkeypatch.delenv(name, raising=False)
    yield healer_env
    from dag_healer.store import dispose_engines

    dispose_engines()


@pytest.fixture
def repo(tmp_path: Path, isolated: Path, capsys: pytest.CaptureFixture[str]) -> Path:
    """An initialised copy of example_existing_repo."""
    target = tmp_path / "repo"
    shutil.copytree(EXAMPLE_REPO, target, ignore=shutil.ignore_patterns("__pycache__", "processed"))
    assert main(["init", "--repo", str(target)]) == 0
    capsys.readouterr()
    return target


def _cfg_args(repo: Path) -> list[str]:
    return ["-c", str(repo / "healer.yaml")]


def _broken_sqlite_url(tmp_path: Path) -> str:
    blocker = tmp_path / "not-a-dir"
    blocker.write_text("x", encoding="utf-8")
    return f"sqlite:///{(blocker / 'healer.db').as_posix()}"


# --------------------------------------------------------------------------- helpers


def test_render_table_aligns_columns() -> None:
    table = render_table(("A", "LONG_HEADER"), [("x", None), ("yyyy", True)])
    lines = table.splitlines()
    assert lines[0] == "A     LONG_HEADER"
    assert lines[2] == "x     -"
    assert lines[3] == "yyyy  yes"


@pytest.mark.parametrize(
    ("url", "expected"),
    [
        (
            "postgresql+psycopg2://airflow:s3cret@postgres/healer",
            "postgresql+psycopg2://airflow:***@postgres/healer",
        ),
        ("sqlite:///C:/x/healer.db", "sqlite:///C:/x/healer.db"),
        ("postgresql://user@host/db", "postgresql://user@host/db"),
        (None, None),
    ],
)
def test_mask_url(url: str | None, expected: str | None) -> None:
    assert mask_url(url) == expected


# --------------------------------------------------------------------------- check


def test_check_prints_settings_with_secrets_masked(
    tmp_path: Path, isolated: Path, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    config_file = tmp_path / "healer.yaml"
    config_file.write_text(
        "version: 1\n"
        "github: {repo: acme/etl, token: ghp_literal_token_value}\n"
        "airflow: {username: admin, password: '${HEALER_AIRFLOW_PASSWORD}'}\n"
        "store: {url: 'postgresql://healer:pw-in-url@db/healer'}\n",
        encoding="utf-8",
    )
    monkeypatch.setenv("HEALER_AIRFLOW_PASSWORD", SECRET)
    assert main(["check", "--config", str(config_file)]) == 0
    captured = capsys.readouterr()
    assert "ghp_literal_token_value" not in captured.out
    assert SECRET not in captured.out
    assert "pw-in-url" not in captured.out
    assert "token: '***'" in captured.out
    assert "postgresql://healer:***@db/healer" in captured.out
    assert "Configuration is valid." in captured.out
    assert "github.token" in captured.err and "store.url" in captured.err
    assert "airflow.password" not in captured.err  # it is an ${ENV} reference


def test_check_reports_invalid_configuration(
    tmp_path: Path, isolated: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    config_file = tmp_path / "healer.yaml"
    config_file.write_text("limits: {max_fix_attempts: 0}\n", encoding="utf-8")
    assert main(["-c", str(config_file), "check"]) == 1
    assert "limits.max_fix_attempts" in capsys.readouterr().err
    assert main(["check", "-c", str(tmp_path / "missing.yaml")]) == 1
    assert "not found" in capsys.readouterr().err


# --------------------------------------------------------------------------- doctor


class _Response:
    def __init__(self, status_code: int, body: Any = None) -> None:
        self.status_code = status_code
        self._body = body

    def json(self) -> Any:
        if self._body is None:
            raise ValueError("no body")
        return self._body


def _healthy(url: str) -> _Response:
    assert url.endswith("/api/v2/monitor/health")
    return _Response(200, {"metadatabase": {"status": "healthy"}, "scheduler": {"status": "healthy"}})


@pytest.fixture
def healthy_network(monkeypatch: pytest.MonkeyPatch) -> list[tuple[str, int]]:
    """Fake Airflow API and SMTP server; returns the SMTP connections attempted."""
    connections: list[tuple[str, int]] = []

    class _Connection:
        def __enter__(self) -> _Connection:
            return self

        def __exit__(self, *exc: object) -> None:
            return None

    def fake_connect(address: tuple[str, int], timeout: float) -> _Connection:
        assert timeout == doctor_cmd.NETWORK_TIMEOUT_SECONDS
        connections.append(address)
        return _Connection()

    monkeypatch.setattr(doctor_cmd, "_http_get", _healthy)
    monkeypatch.setattr(doctor_cmd, "_http_post", lambda url, payload: _Response(201, {"access_token": "t"}))
    monkeypatch.setattr(doctor_cmd.socket, "create_connection", fake_connect)
    return connections


def _doctor_lines(out: str) -> dict[str, str]:
    """Map check name -> label from lines like ``[ OK ] ui bundle   detail``."""
    lines: dict[str, str] = {}
    for line in out.splitlines():
        if line[:1] == "[" and line[5:6] == "]":
            name = line[7:].split("  ", 1)[0].strip()
            lines[name] = line[:6]
    return lines


def test_doctor_all_good(
    repo: Path,
    healthy_network: list[tuple[str, int]],
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    monkeypatch.setenv("ANTHROPIC_API_KEY", SECRET)
    monkeypatch.setenv("HEALER_AIRFLOW_USERNAME", "admin")
    monkeypatch.setenv("HEALER_AIRFLOW_PASSWORD", "admin-password")
    monkeypatch.setenv("HEALER_SMTP_HOST", "mailpit")
    monkeypatch.setenv("HEALER_SMTP_PORT", "1025")
    monkeypatch.setenv("HEALER_INGEST_TOKEN", "ingest-token-value")
    assert main([*_cfg_args(repo), "doctor"]) == 0
    out = capsys.readouterr().out
    assert SECRET not in out and "admin-password" not in out and "ingest-token-value" not in out
    status = _doctor_lines(out)
    for name in (
        "config",
        "repository",
        "store",
        "ui bundle",
        "llm",
        "orchestrator",
        "airflow api",
        "airflow auth",
        "smtp",
    ):
        assert status[name] == "[ OK ]", (name, out)
    assert status["contracts"] == "[WARN]"  # drafts only: nothing enforced yet
    assert healthy_network == [("mailpit", 1025)]
    assert "0 error(s)" in out


def test_doctor_reports_errors_with_exit_code_1(
    repo: Path, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    def unreachable(url: str) -> Any:
        raise ConnectionError("connection refused")

    def no_smtp(address: tuple[str, int], timeout: float) -> Any:
        raise OSError("connection refused")

    monkeypatch.setattr(doctor_cmd, "_http_get", unreachable)
    monkeypatch.setattr(doctor_cmd.socket, "create_connection", no_smtp)
    monkeypatch.setenv("HEALER_SMTP_HOST", "smtp.invalid")
    assert main([*_cfg_args(repo), "doctor"]) == 1
    status = _doctor_lines(capsys.readouterr().out)
    assert status["airflow api"] == "[FAIL]"
    assert status["llm"] == "[FAIL]"  # ANTHROPIC_API_KEY missing
    assert status["smtp"] == "[FAIL]"
    assert status["airflow auth"] == "[WARN]"  # no credentials configured


def test_doctor_warns_about_literal_secrets_and_missing_bundle(
    tmp_path: Path,
    isolated: Path,
    healthy_network: list[tuple[str, int]],
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    (tmp_path / "dags").mkdir()
    config_file = tmp_path / "healer.yaml"
    config_file.write_text(
        "llm: {provider: fake}\nairflow: {username: admin, password: literal-pw}\n", encoding="utf-8"
    )
    monkeypatch.setattr(doctor_cmd, "bundle_path", lambda: tmp_path / "missing.umd.cjs")
    assert main(["doctor", "--config", str(config_file)]) == 1
    out = capsys.readouterr().out
    assert "literal-pw" not in out
    status = _doctor_lines(out)
    assert status["secrets"] == "[WARN]"
    assert "airflow.password" in out
    assert status["ui bundle"] == "[FAIL]"
    assert status["llm"] == "[ OK ]"  # fake provider needs no key


def test_doctor_with_invalid_config(
    tmp_path: Path, isolated: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    config_file = tmp_path / "healer.yaml"
    config_file.write_text("nope: true\n", encoding="utf-8")
    assert main(["doctor", "-c", str(config_file)]) == 1
    status = _doctor_lines(capsys.readouterr().out)
    assert status["config"] == "[FAIL]"
    assert "ui bundle" in status


def test_doctor_store_error(
    repo: Path,
    tmp_path: Path,
    healthy_network: list[tuple[str, int]],
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    monkeypatch.setenv("HEALER_DATABASE_URL", _broken_sqlite_url(tmp_path))
    assert main([*_cfg_args(repo), "doctor"]) == 1
    assert _doctor_lines(capsys.readouterr().out)["store"] == "[FAIL]"


AIRFLOW_CHECKS = {"airflow api", "airflow auth", "airflow plugin"}
#: Adapters of the doctor tests, importable as ``doctor_adapters:<FACTORY>`` (see ``custom_adapters``).
DOCTOR_ADAPTERS = """
from dataclasses import replace

from dag_healer.core.ports import DoctorCheck
from dag_healer.orchestrators.fake import FAKE_CAPABILITIES, FakeAdapter

CLOSED = []


class BlindAdapter(FakeAdapter):
    display_name = "Blindflow"
    capabilities = replace(FAKE_CAPABILITIES, attempts_visible=False)


class BrokenChecksAdapter(FakeAdapter):
    display_name = "Brokenflow"

    def doctor_checks(self):
        def boom():
            raise RuntimeError("the check exploded")

        return [DoctorCheck("boom", boom), DoctorCheck("odd", lambda: 42)]

    def close(self):
        CLOSED.append(self.instance)


class Factory:
    options_model = None
    secret_options = ()

    def __init__(self, adapter_class):
        self.adapter_class = adapter_class
        self.type = adapter_class.__name__
        self.display_name = adapter_class.display_name

    def create(self, cfg, *, client=None):
        return self.adapter_class(cfg, instance=cfg.orchestrator.instance_id)


BLIND = Factory(BlindAdapter)
BROKEN = Factory(BrokenChecksAdapter)
"""


@pytest.fixture
def custom_adapters(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Iterator[Any]:
    """The ``doctor_adapters`` module above, importable for this test."""
    import importlib

    from dag_healer.orchestrators import registry

    folder = tmp_path / "adapters"
    folder.mkdir()
    (folder / "doctor_adapters.py").write_text(DOCTOR_ADAPTERS, encoding="utf-8")
    monkeypatch.syspath_prepend(str(folder))
    monkeypatch.delitem(sys.modules, "doctor_adapters", raising=False)
    yield importlib.import_module("doctor_adapters")
    registry.reset_adapters()


def _doctor(tmp_path: Path, config: str, capsys: pytest.CaptureFixture[str]) -> tuple[int, str]:
    (tmp_path / "dags").mkdir(exist_ok=True)
    config_file = tmp_path / "healer.yaml"
    config_file.write_text("llm: {provider: fake}\n" + config, encoding="utf-8")
    code = main(["doctor", "-c", str(config_file)])
    return code, capsys.readouterr().out


def _line(out: str, name: str) -> str:
    return next(line for line in out.splitlines() if line[7:].startswith(f"{name} "))


def test_doctor_composes_the_generic_and_the_adapter_checks(
    repo: Path, healthy_network: list[tuple[str, int]], capsys: pytest.CaptureFixture[str]
) -> None:
    from dag_healer.airflow.adapter import AirflowAdapter
    from dag_healer.config import load_config

    main([*_cfg_args(repo), "doctor"])
    out = capsys.readouterr().out
    adapter = AirflowAdapter(load_config(repo / "healer.yaml"))
    adapter_checks = [check.name for check in adapter.doctor_checks()]
    assert set(adapter_checks) == AIRFLOW_CHECKS
    assert list(_doctor_lines(out)) == [
        *("config", "repository", "contracts", "store", "ui bundle", "llm", "orchestrator"),
        *adapter_checks,
        *("smtp", "secrets", "ingest", "github"),
    ]
    assert _line(out, "orchestrator").endswith("  Apache Airflow (type airflow, instance airflow)")


def test_doctor_runs_the_checks_of_another_orchestrator(
    tmp_path: Path, isolated: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    code, out = _doctor(tmp_path, "orchestrator: {type: fake}\n", capsys)
    status = _doctor_lines(out)
    assert code == 0, out
    assert _line(out, "orchestrator").endswith("  Fakeflow (type fake, instance fake)")
    assert status["fakeflow"] == "[ OK ]"
    assert not AIRFLOW_CHECKS & set(status)


def test_doctor_reports_an_orchestrator_that_cannot_load(
    tmp_path: Path, isolated: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    config = "orchestrator: {type: 'no_such_pkg.adapter:FACTORY', instance: elsewhere}\n"
    code, out = _doctor(tmp_path, config, capsys)
    status = _doctor_lines(out)
    assert code == 1
    assert status["orchestrator"] == "[FAIL]"
    assert "module 'no_such_pkg.adapter' is not installed" in _line(out, "orchestrator")
    assert not AIRFLOW_CHECKS & set(status)
    assert status["github"] == "[ OK ]"  # the checks after it still ran


def test_doctor_rejects_a_mode_the_orchestrator_cannot_serve(
    tmp_path: Path, isolated: Path, healthy_network: list[tuple[str, int]], capsys: pytest.CaptureFixture[str]
) -> None:
    code, out = _doctor(tmp_path, "monitor: {modes: [rest, queue]}\n", capsys)
    status = _doctor_lines(out)
    assert code == 1
    assert status["orchestrator"] == "[FAIL]"
    assert "monitor.modes includes queue, but Apache Airflow" in _line(out, "orchestrator")
    assert status["airflow api"] == "[ OK ]"  # the adapter's own checks still run


def test_doctor_rejects_every_failure_without_visible_attempts(
    tmp_path: Path, isolated: Path, custom_adapters: Any, capsys: pytest.CaptureFixture[str]
) -> None:
    config = "orchestrator: {type: 'doctor_adapters:BLIND', instance: blind}\n"
    code, out = _doctor(tmp_path, config + "monitor: {trigger: every_failure}\n", capsys)
    assert code == 1
    line = _line(out, "orchestrator")
    assert line.startswith("[FAIL]") and "every_failure" in line and "Blindflow" in line
    assert _doctor_lines(out)["fakeflow"] == "[ OK ]"


def test_doctor_survives_broken_adapter_checks(
    tmp_path: Path, isolated: Path, custom_adapters: Any, capsys: pytest.CaptureFixture[str]
) -> None:
    config = "orchestrator: {type: 'doctor_adapters:BROKEN', instance: broken}\n"
    code, out = _doctor(tmp_path, config, capsys)
    status = _doctor_lines(out)
    assert code == 1
    assert status["orchestrator"] == "[ OK ]"
    assert _line(out, "boom").endswith("  RuntimeError: the check exploded")
    assert _line(out, "odd").endswith("  the check returned int, not a CheckResult")
    assert status["boom"] == status["odd"] == "[FAIL]"
    assert status["github"] == "[ OK ]"
    assert custom_adapters.CLOSED == ["broken"]


@pytest.mark.parametrize(
    ("modes", "auth", "ingest"),
    [
        (["rest", "listener"], doctor_cmd.WARN, doctor_cmd.WARN),
        (["poll"], doctor_cmd.WARN, doctor_cmd.SKIP),
        (["listener", "callback"], doctor_cmd.SKIP, doctor_cmd.WARN),
        (["push"], doctor_cmd.SKIP, doctor_cmd.WARN),
    ],
)
def test_airflow_auth_and_ingest_follow_the_detection_modes(
    isolated: Path, modes: list[str], auth: str, ingest: str
) -> None:
    from dag_healer.config import config_from_mapping

    cfg = config_from_mapping({"monitor": {"modes": modes}})
    auth_result, ingest_result = doctor_cmd.check_airflow_auth(cfg, True), doctor_cmd.check_ingest(cfg)
    assert (auth_result.status, ingest_result.status) == (auth, ingest)
    if auth == doctor_cmd.SKIP:
        assert auth_result.detail == "REST polling disabled (monitor.modes)"
    if ingest == doctor_cmd.SKIP:
        assert ingest_result.detail == "listener/callback modes disabled"


# --------------------------------------------------------------------------- contracts


def test_contracts_list_without_contracts(
    tmp_path: Path, isolated: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    config_file = tmp_path / "healer.yaml"
    config_file.write_text("version: 1\n", encoding="utf-8")
    assert main(["contracts", "list", "-c", str(config_file)]) == 0
    assert "No contracts" in capsys.readouterr().out


def test_contracts_without_action_prints_help(isolated: Path, capsys: pytest.CaptureFixture[str]) -> None:
    assert main(["contracts"]) == 2
    assert "approve" in capsys.readouterr().out


def test_approve_unknown_or_invalid_dag(repo: Path, capsys: pytest.CaptureFixture[str]) -> None:
    assert main([*_cfg_args(repo), "contracts", "approve", "nope", "--by", "alice"]) == 1
    assert "no contract for DAG 'nope'" in capsys.readouterr().err
    assert main([*_cfg_args(repo), "contracts", "approve", "../etc", "--by", "alice"]) == 1
    assert "invalid dag_id" in capsys.readouterr().err
    assert main([*_cfg_args(repo), "contracts", "approve", "sales_daily_etl", "--by", "  "]) == 1


def test_approve_survives_an_unreachable_store(
    repo: Path, tmp_path: Path, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    monkeypatch.setenv("HEALER_DATABASE_URL", _broken_sqlite_url(tmp_path))
    assert main([*_cfg_args(repo), "contracts", "approve", "customers_sync", "--by", "carol"]) == 0
    captured = capsys.readouterr()
    assert "could not record the approval in the store" in captured.err
    contract = load_contract(contract_path(repo / "healer" / "contracts", "customers_sync"))
    assert contract.is_approved and contract.approved_by == "carol"


def test_stale_contract_is_listed_and_can_be_reapproved(
    repo: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    path = contract_path(repo / "healer" / "contracts", "sales_daily_etl")
    assert main([*_cfg_args(repo), "contracts", "approve", "sales_daily_etl", "--by", "alice"]) == 0
    approved = load_contract(path)
    save_contract(approved.model_copy(update={"notes": "edited after approval"}), path)
    capsys.readouterr()

    assert main([*_cfg_args(repo), "contracts", "list"]) == 0
    out = capsys.readouterr().out
    sales_line = next(line for line in out.splitlines() if line.startswith("sales_daily_etl"))
    assert sales_line.split()[-1] == "yes"
    assert "Stale contracts" in out

    assert main([*_cfg_args(repo), "contracts", "approve", "sales_daily_etl", "--by", "bob"]) == 0
    reapproved = load_contract(path)
    assert reapproved.approved_by == "bob"
    assert reapproved.source_hash != approved.source_hash


# --------------------------------------------------------------------------- incidents


def _seed_incidents(repo: Path) -> None:
    from dag_healer.config import load_config
    from dag_healer.store import create_incident, session_scope, set_status

    cfg = load_config(repo / "healer.yaml")
    with session_scope(cfg.database_url) as session:
        first, _ = create_incident(
            session, dag_id="sales_daily_etl", run_id="manual__1", task_id="transform_sales", try_number=1,
            source="rest", error_summary="KeyError: 'amount'",
        )  # fmt: skip
        create_incident(
            session, dag_id="customers_sync", run_id="manual__2", task_id="transform", try_number=1,
            source="listener", error_summary="ValueError",
        )  # fmt: skip
        set_status(session, first, "reported", "reported as DATA")


def test_incidents_list_filters_and_json(repo: Path, capsys: pytest.CaptureFixture[str]) -> None:
    assert main([*_cfg_args(repo), "incidents", "list"]) == 0
    assert "No incidents found." in capsys.readouterr().out

    _seed_incidents(repo)
    assert main([*_cfg_args(repo), "incidents", "list"]) == 0
    out = capsys.readouterr().out
    assert "sales_daily_etl" in out and "customers_sync" in out and "STATUS" in out

    assert main([*_cfg_args(repo), "incidents", "list", "--status", "reported"]) == 0
    out = capsys.readouterr().out
    assert "sales_daily_etl" in out and "customers_sync" not in out

    assert main([*_cfg_args(repo), "incidents", "list", "--dag", "customers_sync", "--json"]) == 0
    data = json.loads(capsys.readouterr().out)
    assert [item["dag_id"] for item in data] == ["customers_sync"]
    assert data[0]["status"] == "new"


def test_incidents_list_warns_on_unknown_status(repo: Path, capsys: pytest.CaptureFixture[str]) -> None:
    assert main([*_cfg_args(repo), "incidents", "list", "--status", "bogus,new"]) == 0
    assert "unknown status(es): bogus" in capsys.readouterr().err


def test_incidents_list_store_unreachable(
    repo: Path, tmp_path: Path, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    monkeypatch.setenv("HEALER_DATABASE_URL", _broken_sqlite_url(tmp_path))
    assert main([*_cfg_args(repo), "incidents", "list"]) == 1
    assert "cannot read incidents" in capsys.readouterr().err


# --------------------------------------------------------------------------- worker / registry


def test_worker_rejects_an_invalid_config(
    isolated: Path, tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    bad = tmp_path / "healer.yaml"
    bad.write_text("version: 2\n", encoding="utf-8")
    assert main(["-c", str(bad), "worker", "--once"]) == 1
    assert "unsupported config version" in capsys.readouterr().err


def test_worker_that_cannot_start_exits_1(
    isolated: Path, tmp_path: Path, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    from dag_healer.worker import loop

    reason = "monitor.modes includes queue, but Apache Airflow has no hint queue to drain"

    def refuse(cfg: Any, **kwargs: Any) -> Any:
        raise loop.WorkerStartError(reason)

    monkeypatch.setattr(loop, "run_worker", refuse)
    config_file = tmp_path / "healer.yaml"
    config_file.write_text("version: 1\n", encoding="utf-8")
    level = logging.getLogger("dag_healer").level
    assert main(["-c", str(config_file), "worker", "--once", "--provider", "fake"]) == 1
    captured = capsys.readouterr()
    assert f"error: {reason}" in captured.err
    assert "dag-healer worker:" not in captured.out  # no --once summary
    assert logging.getLogger("dag_healer").level == level


def test_all_commands_registered(capsys: pytest.CaptureFixture[str]) -> None:
    from dag_healer.cli.main import COMMANDS, build_parser

    build_parser()
    assert {"init", "check", "contracts", "incidents", "doctor", "worker", "version"} <= set(COMMANDS)


def test_cli_help_does_not_import_heavy_modules() -> None:
    import subprocess
    import sys

    code = (
        "import sys; from dag_healer.cli.main import build_parser; build_parser(); "
        "heavy = [m for m in ('pandas', 'sqlalchemy', 'fastapi', 'anthropic', 'httpx') if m in sys.modules]; "
        "print(','.join(heavy))"
    )
    result = subprocess.run([sys.executable, "-c", code], capture_output=True, text=True, check=True)
    assert result.stdout.strip() == ""
