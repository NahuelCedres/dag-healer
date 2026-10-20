"""Unit tests for :mod:`dag_healer.agent.context` (traceback extraction, path mapping, gathering).

``gather_context`` runs against a temporary copy of ``example_existing_repo`` prepared the
way the demo does it: ``dag-healer init`` drafts the contracts, the sales and customers
contracts are approved, and the day-2 files are copied over ``data/incoming``. The Airflow
client is a duck-typed fake serving logs rendered from the recorded fixtures.
"""

from __future__ import annotations

import json
import shutil
from collections.abc import Iterator
from pathlib import Path
from typing import Any

import pytest

from dag_healer.agent import context as context_module
from dag_healer.agent.context import (
    IncidentContext,
    extract_traceback,
    gather_context,
    map_to_repo_path,
)
from dag_healer.airflow.client import AirflowAPIError, render_log_content
from dag_healer.config import HealerConfig, config_from_mapping, load_config_file
from dag_healer.contracts.io import approve_contract, contract_path, load_contract, save_contract
from dag_healer.contracts.schema import Violation

ROOT = Path(__file__).resolve().parents[2]
EXAMPLE_REPO = ROOT / "example_existing_repo"
LOGS = Path(__file__).parent / "fixtures" / "logs"
INCIDENT_ID = "0123456789abcdef0123456789abcdef"
RUN_ID = "manual__2026-10-17T18:48:46.093857+00:00"

EMAIL = "jane.doe@example.com"
PHONE = "+1 415 555 0134"


def fixture_json(name: str) -> Any:
    return json.loads((LOGS / name).read_text(encoding="utf-8"))


def rendered(name: str) -> str:
    """A recorded/synthetic structured log rendered exactly like AirflowClient.get_task_log."""
    return render_log_content(fixture_json(name))


class FakeClient:
    """Duck-typed stand-in for AirflowClient (only what gather_context uses)."""

    def __init__(
        self,
        log: str = "",
        *,
        dag: dict[str, Any] | None = None,
        source: str | None = None,
        fail: bool = False,
    ) -> None:
        self.log = log
        self.dag = dag
        self.source = source
        self.fail = fail
        self.calls: list[tuple[Any, ...]] = []

    def get_task_log(
        self, dag_id: str, run_id: str, task_id: str, try_number: int, map_index: int = -1
    ) -> str:
        self.calls.append(("log", dag_id, run_id, task_id, try_number, map_index))
        if self.fail:
            raise AirflowAPIError("Service Unavailable", status_code=503)
        return self.log

    def get_dag(self, dag_id: str) -> dict[str, Any]:
        self.calls.append(("dag", dag_id))
        if self.fail or self.dag is None:
            raise AirflowAPIError("The Dag with ID: `x` was not found", status_code=404)
        return self.dag

    def get_dag_source(self, dag_id: str) -> str | None:
        self.calls.append(("source", dag_id))
        if self.fail:
            raise AirflowAPIError("boom", status_code=500)
        return self.source


def incident(dag_id: str, task_id: str, **extra: Any) -> dict[str, Any]:
    """An ``Incident.to_dict()``-shaped mapping."""
    data = {
        "id": INCIDENT_ID,
        "dag_id": dag_id,
        "run_id": RUN_ID,
        "task_id": task_id,
        "map_index": -1,
        "try_number": 1,
        "status": "triaging",
        "source": "rest",
        "error_summary": "",
    }
    data.update(extra)
    return data


@pytest.fixture(scope="module")
def demo_repo(tmp_path_factory: pytest.TempPathFactory) -> Iterator[Path]:
    """example_existing_repo after init, two approvals and the day-2 drop (read-only for tests)."""
    from dag_healer.cli.init_cmd import run_init

    root = tmp_path_factory.mktemp("context") / "legacy repo"
    shutil.copytree(EXAMPLE_REPO, root, ignore=shutil.ignore_patterns("__pycache__", "processed"))
    with pytest.MonkeyPatch.context() as mp:
        for name in ("HEALER_CONFIG", "AIRFLOW_HOME", "HEALER_DATABASE_URL"):
            mp.delenv(name, raising=False)
        mp.setenv("HEALER_HOME", str(root.parent / "healer_home"))
        result = run_init(root)
        assert result.ok
        for dag_id in ("sales_daily_etl", "customers_sync"):
            path = contract_path(root / "healer" / "contracts", dag_id)
            save_contract(approve_contract(load_contract(path), "alice"), path)
        for source in (root / "fixtures" / "day2").glob("*.csv"):
            shutil.copyfile(source, root / "data" / "incoming" / source.name)
        yield root


@pytest.fixture
def cfg(demo_repo: Path, monkeypatch: pytest.MonkeyPatch) -> HealerConfig:
    monkeypatch.delenv("AIRFLOW__CORE__DAGS_FOLDER", raising=False)
    return load_config_file(demo_repo / "healer.yaml")


def _rules(violations: list[Violation]) -> set[tuple[str, str | None]]:
    return {(violation.rule, violation.column) for violation in violations}


# =========================================================================== extract_traceback


def test_extract_traceback_from_the_real_failed_log() -> None:
    exc_type, message, frames, text = extract_traceback(rendered("customers_sync_transform_failed.json"))
    assert exc_type == "ValueError"
    assert message == "unconverted data remains: T08:30:00Z"
    assert len(frames) == 21
    assert frames[0]["function"] == "_run_task_and_map_outcome"
    assert {
        "file": "/opt/airflow/repo/src/etl/customers.py",
        "line": 19,
        "function": "parse_signup_date",
    } in frames
    assert frames[-1] == {
        "file": "/usr/python/lib/python3.13/_strptime.py",
        "line": 558,
        "function": "_strptime",
    }
    assert text.startswith("Traceback (most recent call last):")
    assert text.endswith("ValueError: unconverted data remains: T08:30:00Z")
    assert "Post Execute" not in text and "Task failed with exception" not in text


def test_extract_traceback_keeps_a_chain_together() -> None:
    exc_type, message, frames, text = extract_traceback(
        rendered("sales_daily_etl_transform_chained_keyerror.json")
    )
    assert (exc_type, message) == ("KeyError", "'amount'")
    assert text.count("Traceback (most recent call last):") == 2
    assert "The above exception was the direct cause of the following exception:" in text
    assert frames[0]["function"] == "get_loc" and frames[0]["file"].endswith("pandas/core/indexes/base.py")
    assert {"file": "/opt/airflow/repo/src/etl/sales.py", "line": 25, "function": "transform_sales"} in frames


def test_extract_traceback_from_a_plain_airflow2_log_with_source_lines() -> None:
    text = fixture_json("inventory_report_build_report_plain.json")["content"][0]
    exc_type, message, frames, tb = extract_traceback(text)
    assert (exc_type, message) == ("KeyError", "'qty'")
    assert [frame["function"] for frame in frames] == ["_execute_task", "buildReport", "__getitem__"]
    assert frames[1]["file"] == "/opt/airflow/repo/dags/../src/etl/inventory.py"
    assert "Marking task as FAILED" not in tb
    assert tb.splitlines()[-1] == "KeyError: 'qty'"


def test_extract_traceback_picks_the_last_independent_traceback() -> None:
    text = "\n".join(
        [
            "Traceback (most recent call last):",
            '  File "/opt/airflow/repo/src/a.py", line 1, in first',
            "RuntimeError: first try",
            "[2026-10-17T10:00:00Z] INFO - retrying",
            "Traceback (most recent call last):",
            '  File "/opt/airflow/repo/src/b.py", line 2, in second',
            "pandas.errors.ParserError: Error tokenizing data. C error: Expected 5 fields in line 7, saw 6",
            "",
        ]
    )
    exc_type, message, frames, tb = extract_traceback(text)
    assert exc_type == "pandas.errors.ParserError"
    assert message == "Error tokenizing data. C error: Expected 5 fields in line 7, saw 6"
    assert frames == [{"file": "/opt/airflow/repo/src/b.py", "line": 2, "function": "second"}]
    assert "first try" not in tb


def test_extract_traceback_multiline_message_and_context_chain() -> None:
    text = "\n".join(
        [
            "Traceback (most recent call last):",
            '  File "/opt/airflow/repo/src/a.py", line 3, in load',
            "OSError: disk full",
            "",
            "During handling of the above exception, another exception occurred:",
            "",
            "Traceback (most recent call last):",
            '  File "/opt/airflow/repo/src/a.py", line 9, in run',
            "ValueError: bad rows:",
            "  row 1",
            "  row 2",
            "",
            "trailing text",
        ]
    )
    exc_type, message, frames, tb = extract_traceback(text)
    assert exc_type == "ValueError"
    assert message == "bad rows:\n  row 1\n  row 2"
    assert [frame["line"] for frame in frames] == [3, 9]
    assert tb.startswith("Traceback") and tb.endswith("  row 2")


def test_extract_traceback_without_header_or_without_traceback() -> None:
    truncated = "...[truncated]...\n  File \"/opt/airflow/repo/src/x.py\", line 4, in f\nKeyError: 'amount'"
    exc_type, message, frames, _ = extract_traceback(truncated)
    assert (exc_type, message) == ("KeyError", "'amount'")
    assert frames == [{"file": "/opt/airflow/repo/src/x.py", "line": 4, "function": "f"}]

    assert extract_traceback("airflow.exceptions.AirflowTaskTimeout: Timeout, PID: 4242")[:3] == (
        "airflow.exceptions.AirflowTaskTimeout",
        "Timeout, PID: 4242",
        [],
    )
    assert extract_traceback("Task exited with return code Negsignal.SIGKILL") == (None, None, [], "")
    assert extract_traceback("") == (None, None, [], "")
    assert extract_traceback(None) == (None, None, [], "")


def test_extract_traceback_inside_an_exception_group() -> None:
    text = "\n".join(
        [
            "  + Exception Group Traceback (most recent call last):",
            '  |   File "/opt/airflow/repo/src/x.py", line 1, in main',
            "  | ExceptionGroup: boom (1 sub-exception)",
            "  +-+---------------- 1 ----------------",
            "    | Traceback (most recent call last):",
            '    |   File "/opt/airflow/repo/src/x.py", line 5, in worker',
            "    | KeyError: 'sku'",
            "    +------------------------------------",
        ]
    )
    exc_type, message, frames, _ = extract_traceback(text)
    assert (exc_type, message) == ("KeyError", "'sku'")
    assert frames == [{"file": "/opt/airflow/repo/src/x.py", "line": 5, "function": "worker"}]


# =========================================================================== map_to_repo_path


@pytest.fixture
def small_repo(make_repo: Any, monkeypatch: pytest.MonkeyPatch) -> HealerConfig:
    monkeypatch.delenv("AIRFLOW__CORE__DAGS_FOLDER", raising=False)
    root = make_repo(
        {
            "dags/my_dag.py": "x = 1\n",
            "src/etl/customers.py": "y = 2\n",
            "pipelines/flows/job.py": "z = 3\n",
        },
        name="my repo",
    )
    return config_from_mapping({"repo": {"path": str(root), "dags_dir": "dags"}})


@pytest.mark.parametrize(
    ("path", "expected"),
    [
        ("/opt/airflow/repo/src/etl/customers.py", "src/etl/customers.py"),
        ("/opt/airflow/repo/dags/customers_sync.py", "dags/customers_sync.py"),
        ("/opt/airflow/repo/dags/../src/etl/inventory.py", "src/etl/inventory.py"),
        ("/opt/airflow/dags/my_dag.py", "dags/my_dag.py"),
        ("/opt/airflow/repo", None),
        ("/opt/airflow/repository/src/x.py", None),
        ("/opt/airflow/pandas/_libs/lib.pyx", None),
        ("/home/airflow/.local/lib/python3.13/site-packages/pandas/core/frame.py", None),
        ("/opt/airflow/repo/.venv/lib/python3.13/site-packages/x/y.py", None),
        ("/usr/python/lib/python3.13/_strptime.py", None),
        ("/tmp/airflow/dag_bundles/git/tracking_repo/src/etl/customers.py", "src/etl/customers.py"),
        ("/tmp/airflow/dag_bundles/git/versions/abc/my_dag.py", "dags/my_dag.py"),
        ("/somewhere/else/unknown.py", None),
        ("src/etl/customers.py", "src/etl/customers.py"),
        ("my_dag.py", "dags/my_dag.py"),
        ("src\\etl\\customers.py", "src/etl/customers.py"),
        ("../outside.py", None),
        ("src/etl/missing.py", None),
        ("<frozen importlib._bootstrap>", None),
        ("<string>", None),
        ("", None),
        (None, None),
    ],
)
def test_map_to_repo_path(small_repo: HealerConfig, path: str | None, expected: str | None) -> None:
    assert map_to_repo_path(path, small_repo) == expected


def test_map_to_repo_path_under_the_local_repo_root(small_repo: HealerConfig) -> None:
    root = small_repo.repo_root
    assert map_to_repo_path(root / "pipelines" / "flows" / "job.py", small_repo) == "pipelines/flows/job.py"
    assert map_to_repo_path(str(root / "src" / "new_file.py"), small_repo) == "src/new_file.py"
    assert map_to_repo_path(str(root.parent / "other" / "x.py"), small_repo) is None


def test_map_to_repo_path_uses_the_airflow_dags_folder(
    small_repo: HealerConfig, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setenv("AIRFLOW__CORE__DAGS_FOLDER", "/srv/airflow/my-dags/")
    assert map_to_repo_path("/srv/airflow/my-dags/my_dag.py", small_repo) == "dags/my_dag.py"
    assert map_to_repo_path("/srv/airflow/my-dags/sub/other.py", small_repo) == "dags/sub/other.py"


def test_map_to_repo_path_with_a_custom_dags_dir(make_repo: Any, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("AIRFLOW__CORE__DAGS_FOLDER", raising=False)
    root = make_repo({"airflow/dags/etl.py": "x = 1\n"}, name="custom")
    cfg = config_from_mapping({"repo": {"path": str(root), "dags_dir": "airflow/dags"}})
    assert map_to_repo_path("/opt/airflow/dags/etl.py", cfg) == "airflow/dags/etl.py"
    assert map_to_repo_path("etl.py", cfg) == "airflow/dags/etl.py"


# =========================================================================== gather_context


def test_gather_context_sales_is_a_contract_violation(cfg: HealerConfig) -> None:
    client = FakeClient(rendered("sales_daily_etl_transform_chained_keyerror.json"))
    ctx = gather_context(cfg, incident("sales_daily_etl", "transform_sales"), client)

    assert client.calls[0] == ("log", "sales_daily_etl", RUN_ID, "transform_sales", 1, -1)
    assert ctx.contract_status == "approved"
    assert ctx.contract is not None and ctx.contract.is_approved
    assert ("missing_column", "amount") in _rules(ctx.violations)
    assert ctx.draft_violations == []
    assert ctx.datasets_checked == ["sales"]
    assert (ctx.exception_type, ctx.exception_message) == ("KeyError", "'amount'")
    assert ctx.repo_frames == [{"file": "src/etl/sales.py", "line": 25, "function": "transform_sales"}]
    assert ctx.top_repo_frame == ctx.repo_frames[-1]
    assert ctx.dag_file == "dags/sales_daily_etl.py"  # from the static scan (the fake has no DAG)
    assert ctx.task is not None and ctx.task.callable_name == "transform_sales"
    assert set(ctx.code_snippets) == {"dags/sales_daily_etl.py", "src/etl/sales.py"}
    assert ctx.dag_source == ctx.code_snippets["dags/sales_daily_etl.py"]

    sample = ctx.masked_sample
    assert sample is not None and sample["masked"] is True
    assert ctx.sample_dataset == sample["dataset"] == "sales"
    assert sample["columns"] == ["order_id", "customer_email", "amt", "currency", "order_date"]
    assert 0 < len(sample["rows"]) <= cfg.masking.sample_rows
    for row in sample["rows"]:
        assert str(row["customer_email"]).startswith("h:")
    assert "@" not in json.dumps(sample)


def test_gather_context_masks_error_text_and_log(cfg: HealerConfig) -> None:
    log = rendered("sales_daily_etl_transform_chained_keyerror.json")
    assert EMAIL in log and PHONE in log
    error = f"KeyError: 'amount' while processing {EMAIL} ({PHONE})"
    ctx = gather_context(
        cfg, incident("sales_daily_etl", "transform_sales", error_summary=error), FakeClient(log)
    )
    for text in (ctx.error_text, ctx.log_excerpt, ctx.traceback_text, ctx.exception_message or ""):
        assert EMAIL not in text and PHONE not in text
    assert "[email]" in ctx.log_excerpt and "[phone]" in ctx.log_excerpt
    assert ctx.error_text == "KeyError: 'amount' while processing [email] ([phone])"
    assert "Traceback (most recent call last):" in ctx.log_excerpt
    assert EMAIL not in json.dumps(ctx.to_prompt_dict())


def test_gather_context_customers_is_code_with_valid_data(cfg: HealerConfig) -> None:
    client = FakeClient(
        rendered("customers_sync_transform_failed.json"),
        dag=fixture_json("airflow_api/dag_customers_sync.json"),
    )
    ctx = gather_context(cfg, incident("customers_sync", "transform"), client)

    assert ctx.contract_status == "approved"
    assert ctx.violations == [] and ctx.draft_violations == []
    assert ctx.datasets_checked == ["customers"]
    assert ctx.exception_type == "ValueError"
    assert ctx.exception_message == "unconverted data remains: T08:30:00Z"
    assert {"file": "src/etl/customers.py", "line": 19, "function": "parse_signup_date"} in ctx.repo_frames
    assert [frame["function"] for frame in ctx.repo_frames] == ["transform_customers", "parse_signup_date"]
    assert ctx.dag_file == "dags/customers_sync.py"  # from Airflow's fileloc
    assert ctx.task is not None and ctx.task.callable_file == "src/etl/customers.py"
    assert "def parse_signup_date" in ctx.code_snippets["src/etl/customers.py"]
    assert ctx.error_text.endswith("ValueError: unconverted data remains: T08:30:00Z")

    sample = ctx.masked_sample
    assert sample is not None and sample["dataset"] == "customers"
    pii = {"full_name", "email", "phone"}
    for row in sample["rows"]:
        for column in pii:
            assert str(row[column]).startswith("h:")
    assert "@" not in json.dumps(sample)
    assert not [note for note in ctx.notes if "failed" in note]


def test_gather_context_inventory_has_only_a_draft_contract(cfg: HealerConfig) -> None:
    plain = fixture_json("inventory_report_build_report_plain.json")["content"][0]
    ctx = gather_context(cfg, incident("inventory_report", "build_report"), None, error_text=plain)

    assert ctx.contract_status == "draft"
    assert ctx.violations == []
    assert ("missing_column", "qty") in _rules(ctx.draft_violations)
    assert (ctx.exception_type, ctx.exception_message) == ("KeyError", "'qty'")
    assert ctx.repo_frames == [{"file": "src/etl/inventory.py", "line": 28, "function": "buildReport"}]
    assert ctx.dag_file == "dags/inventory_report.py"
    assert any("No Airflow API client" in note for note in ctx.notes)
    assert any("draft" in note for note in ctx.notes)
    assert "ops-oncall@example.com" not in ctx.error_text and "555-201-7788" not in ctx.error_text
    assert "[email]" in ctx.error_text and "[phone]" in ctx.error_text
    assert "ops-oncall@example.com" not in ctx.log_excerpt
    assert ctx.masked_sample is not None and ctx.masked_sample["dataset"] == "inventory"
    assert "quantity" in ctx.masked_sample["columns"]


def test_gather_context_falls_back_to_the_error_summary_when_airflow_fails(cfg: HealerConfig) -> None:
    summary = (
        "Traceback (most recent call last):\n"
        '  File "/opt/airflow/repo/src/etl/customers.py", line 19, in parse_signup_date\n'
        "ValueError: time data 'olivia.martin@example.com' does not match format '%Y-%m-%d'"
    )
    client = FakeClient(fail=True)
    ctx = gather_context(cfg, incident("customers_sync", "transform", error_summary=summary), client)
    assert any(
        note.startswith("Fetching the task log from Airflow failed: AirflowAPIError") for note in ctx.notes
    )
    assert any("Reading the DAG from the Airflow API failed" in note for note in ctx.notes)
    assert ctx.exception_type == "ValueError"
    assert ctx.exception_message == "time data '[email]' does not match format '%Y-%m-%d'"
    assert ctx.repo_frames == [{"file": "src/etl/customers.py", "line": 19, "function": "parse_signup_date"}]
    assert "olivia.martin" not in ctx.error_text + ctx.log_excerpt + ctx.traceback_text
    assert ctx.dag_file == "dags/customers_sync.py"
    assert ctx.contract_status == "approved"


def test_gather_context_for_an_unknown_dag_never_raises(cfg: HealerConfig) -> None:
    ctx = gather_context(
        cfg, incident("not_in_repo", "t1"), FakeClient("[2026-10-17T10:00:00Z] INFO - nothing")
    )
    assert ctx.contract_status == "missing" and ctx.contract is None
    assert ctx.violations == [] and ctx.draft_violations == [] and ctx.masked_sample is None
    assert ctx.dag_file is None and ctx.task is None and ctx.code_snippets == {}
    assert ctx.exception_type is None
    notes = " | ".join(ctx.notes)
    assert "No Python traceback" in notes and "No data contract" in notes and "was not found" in notes
    assert ctx.error_text == "[2026-10-17T10:00:00Z] INFO - nothing"
    json.dumps(ctx.to_prompt_dict())


def test_gather_context_with_an_empty_incident() -> None:
    cfg = config_from_mapping({"repo": {"path": str(ROOT / "does-not-exist")}})
    ctx = gather_context(cfg, {}, None)
    assert isinstance(ctx, IncidentContext)
    assert ctx.contract_status == "missing"
    assert ctx.notes


def test_stale_contract_violations_are_evidence_only(
    cfg: HealerConfig, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    contracts = tmp_path / "contracts"
    contracts.mkdir()
    approved = load_contract(contract_path(cfg.contracts_dir, "sales_daily_etl"))
    edited = approved.model_copy(update={"notes": "edited after approval"})
    save_contract(edited, contract_path(contracts, "sales_daily_etl"))
    monkeypatch.setattr(cfg.contracts, "dir", str(contracts))

    ctx = gather_context(
        cfg, incident("sales_daily_etl", "transform_sales"), None, error_text="KeyError: 'amount'"
    )
    assert ctx.contract_status == "stale"
    assert ctx.violations == []
    assert ("missing_column", "amount") in _rules(ctx.draft_violations)
    assert any("edited after its approval" in note for note in ctx.notes)


def test_invalid_contract_file_is_reported(
    cfg: HealerConfig, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    contracts = tmp_path / "contracts"
    contracts.mkdir()
    (contracts / "sales_daily_etl.yaml").write_text("dag_id: [unclosed\n", encoding="utf-8")
    monkeypatch.setattr(cfg.contracts, "dir", str(contracts))
    ctx = gather_context(cfg, incident("sales_daily_etl", "transform_sales"), None)
    assert ctx.contract_status == "missing"
    assert any("invalid" in note for note in ctx.notes)


def test_unreadable_dataset_becomes_a_violation(
    cfg: HealerConfig, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    contracts = tmp_path / "contracts"
    contracts.mkdir()
    approved = load_contract(contract_path(cfg.contracts_dir, "customers_sync"))
    dataset = approved.datasets[0]
    moved = dataset.model_copy(
        update={"locator": dataset.locator.model_copy(update={"path": "data/gone.csv"})}
    )
    contract = approve_contract(approved.model_copy(update={"datasets": [moved]}), "alice")
    save_contract(contract, contract_path(contracts, "customers_sync"))
    monkeypatch.setattr(cfg.contracts, "dir", str(contracts))

    ctx = gather_context(cfg, incident("customers_sync", "transform"), None)
    assert ctx.contract_status == "approved"
    assert [violation.rule for violation in ctx.violations] == ["dataset_unreadable"]
    # the sample falls back to the locator configured in healer.yaml (written by init)
    assert ctx.masked_sample is not None and ctx.masked_sample["dataset"] == "customers"
    assert all(str(row["email"]).startswith("h:") for row in ctx.masked_sample["rows"])


def test_sample_prefers_offending_rows(
    cfg: HealerConfig, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    contracts = tmp_path / "contracts"
    contracts.mkdir()
    approved = load_contract(contract_path(cfg.contracts_dir, "customers_sync"))
    dataset = approved.datasets[0]
    columns = [
        column.model_copy(update={"allowed_values": ["CA"]}) if column.name == "country" else column
        for column in dataset.columns
    ]
    strict = approve_contract(
        approved.model_copy(update={"datasets": [dataset.model_copy(update={"columns": columns})]}), "alice"
    )
    save_contract(strict, contract_path(contracts, "customers_sync"))
    monkeypatch.setattr(cfg.contracts, "dir", str(contracts))

    ctx = gather_context(cfg, incident("customers_sync", "transform"), None)
    violation = next(v for v in ctx.violations if v.rule == "not_allowed_value")
    assert ctx.masked_sample is not None
    countries = [row["country"] for row in ctx.masked_sample["rows"]]
    assert len(countries) == cfg.masking.sample_rows
    assert all(country != "CA" for country in countries[: min(len(violation.row_indexes), len(countries))])


def test_dataset_from_healer_yaml_is_sampled_without_a_contract(make_repo: Any) -> None:
    root = make_repo(
        {"data/in.csv": "id,email,score\n1,a@example.com,3\n2,b@example.com,4\n"},
        name="no contract",
    )
    cfg = config_from_mapping(
        {
            "repo": {"path": str(root)},
            "datasets": {"my_dag": {"load": {"type": "csv", "path": "data/in.csv"}}},
        }
    )
    ctx = gather_context(cfg, incident("my_dag", "other_task"), None, error_text="KeyError: 'score2'")
    assert ctx.contract_status == "missing"
    assert ctx.masked_sample is not None and ctx.masked_sample["dataset"] == "in"
    assert ctx.masked_sample["columns"] == ["id", "email", "score"]
    assert all(str(row["email"]).startswith("h:") for row in ctx.masked_sample["rows"])


def test_log_excerpt_is_limited_and_centered_on_the_traceback(cfg: HealerConfig) -> None:
    noise = [f"[2026-10-17T10:00:{i % 60:02d}Z] INFO - processing batch {i}" for i in range(1000)]
    failure = rendered("customers_sync_transform_failed.json").splitlines()
    tail = [f"[2026-10-17T11:00:00Z] INFO - cleanup {i}" for i in range(300)]
    client = FakeClient("\n".join(noise + failure + tail))
    ctx = gather_context(cfg, incident("customers_sync", "transform"), client)
    lines = ctx.log_excerpt.splitlines()
    assert len(lines) <= context_module.MAX_LOG_EXCERPT_LINES
    assert "Traceback (most recent call last):" in lines
    assert "ValueError: unconverted data remains: T08:30:00Z" in lines
    assert lines[0].startswith("[... ") and "earlier log lines omitted" in lines[0]
    assert "later log lines omitted" in lines[-1]
    assert ctx.exception_type == "ValueError"


def test_code_snippets_respect_the_size_budget(cfg: HealerConfig, monkeypatch: pytest.MonkeyPatch) -> None:
    client = FakeClient(rendered("customers_sync_transform_failed.json"))
    dag_size = len((cfg.repo_root / "dags" / "customers_sync.py").read_bytes())

    # room for the DAG file plus a window around the failing lines of src/etl/customers.py
    monkeypatch.setattr(context_module, "MAX_SNIPPET_BYTES", dag_size + 850)
    ctx = gather_context(cfg, incident("customers_sync", "transform"), client)
    assert sum(len(text.encode("utf-8")) for text in ctx.code_snippets.values()) <= dag_size + 850
    snippet = ctx.code_snippets["src/etl/customers.py"]
    assert "omitted by dag-healer" in snippet
    assert "def parse_signup_date" in snippet and 'df["signup_date"].apply(parse_signup_date)' in snippet

    # no room at all for the module: it is left out with a note
    monkeypatch.setattr(context_module, "MAX_SNIPPET_BYTES", dag_size + 100)
    ctx = gather_context(cfg, incident("customers_sync", "transform"), client)
    assert list(ctx.code_snippets) == ["dags/customers_sync.py"]
    assert any("src/etl/customers.py was left out" in note for note in ctx.notes)


def test_to_prompt_dict_is_json_safe(cfg: HealerConfig) -> None:
    client = FakeClient(rendered("sales_daily_etl_transform_chained_keyerror.json"))
    ctx = gather_context(cfg, incident("sales_daily_etl", "transform_sales"), client)
    data = ctx.to_prompt_dict()
    assert json.loads(json.dumps(data)) == data
    assert set(data) == {
        "incident_id", "dag_id", "run_id", "task_id", "map_index", "try_number", "error_text",
        "exception_type", "exception_message", "traceback_text", "repo_frames", "log_excerpt",
        "dag_file", "dag_source", "task", "code_snippets", "contract", "contract_status",
        "violations", "draft_violations", "sample_dataset", "masked_sample", "datasets_checked", "notes",
    }  # fmt: skip
    assert data["contract"]["status"] == "approved"
    assert data["violations"][0]["rule"] == "missing_column"
    assert data["task"]["task_id"] == "transform_sales"


def test_incident_context_defaults() -> None:
    ctx = IncidentContext(incident_id="i", dag_id="d", run_id="r", task_id="t")
    assert ctx.contract_status == "missing" and ctx.top_repo_frame is None
    data = ctx.to_prompt_dict()
    assert data["contract"] is None and data["task"] is None and data["violations"] == []
