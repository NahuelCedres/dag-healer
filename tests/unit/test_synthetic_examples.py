"""The synthetic example cases (``examples/synthetic/``) land in the category they are built for.

The data goes through dag-healer's own contract inference and validation, the pipelines run on
their day-2 files in a throwaway repository and their errors go through the policy classifier.
No Airflow and no network.
"""

from __future__ import annotations

import importlib
import importlib.util
import shutil
import sys
import traceback
from pathlib import Path
from types import ModuleType

import pytest

from dag_healer.agent.fake import load_scripts, match_script
from dag_healer.agent.policy import classify_error
from dag_healer.contracts.infer import infer_dataset_contract
from dag_healer.contracts.validate import validate_dataset
from dag_healer.datasets import load_dataset

ROOT = Path(__file__).resolve().parents[2]
PACK = ROOT / "examples" / "synthetic"
EXAMPLE_REPO = ROOT / "example_existing_repo"
FILES = ("payments.csv", "orders.csv", "fx_rates.csv")

#: module -> (data file, first task, task that fails on day 2)
CASES = {
    "payments": ("payments.csv", "extract_payments", "settle_payments"),
    "orders": ("orders.csv", "extract_orders", "enrich_orders"),
    "fx_rates": ("fx_rates.csv", "load_rates", "publish_rates"),
}
CODE_SCENARIO = {
    "dag_id": "orders_enrichment",
    "task_id": "enrich_orders",
    "exception_type": "ZeroDivisionError",
}


def _generator() -> ModuleType:
    spec = importlib.util.spec_from_file_location("synthetic_generate", PACK / "generate.py")
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def _forget_src_modules() -> None:
    for name in [name for name in sys.modules if name == "src" or name.startswith("src.")]:
        del sys.modules[name]


@pytest.fixture(scope="module")
def data(tmp_path_factory: pytest.TempPathFactory) -> Path:
    root = tmp_path_factory.mktemp("synthetic")
    _generator().generate(root / "day1", root / "day2")
    return root


@pytest.fixture
def repo(tmp_path: Path, monkeypatch: pytest.MonkeyPatch):
    """A throwaway repository: the ETL helpers of the example repo plus the pack's modules."""
    etl = tmp_path / "src" / "etl"
    etl.mkdir(parents=True)
    shutil.copyfile(EXAMPLE_REPO / "src" / "__init__.py", tmp_path / "src" / "__init__.py")
    for name in ("__init__.py", "common.py"):
        shutil.copyfile(EXAMPLE_REPO / "src" / "etl" / name, etl / name)
    for module in CASES:
        shutil.copyfile(PACK / "src" / "etl" / f"{module}.py", etl / f"{module}.py")
    (tmp_path / "data" / "incoming").mkdir(parents=True)
    _forget_src_modules()
    monkeypatch.syspath_prepend(str(tmp_path))
    yield tmp_path
    _forget_src_modules()


def _day2_violations(data: Path, repo: Path, module: str):
    """Infer the contract from day 1 (as ``dag-healer init`` does) and validate day 2."""
    csv_name, first_task, _ = CASES[module]
    locator = {"type": "csv", "path": f"data/incoming/{csv_name}"}
    shutil.copyfile(data / "day1" / csv_name, repo / "data" / "incoming" / csv_name)
    dataset = infer_dataset_contract(
        load_dataset(locator, repo), name=module, task_id=first_task, locator=locator
    )
    shutil.copyfile(data / "day2" / csv_name, repo / "data" / "incoming" / csv_name)
    return dataset, validate_dataset(load_dataset(locator, repo), dataset)


def _day2_error(module: str) -> BaseException:
    """Run the first task and then the failing one on the day-2 file; return its error."""
    _, first_task, failing_task = CASES[module]
    etl = importlib.import_module(f"src.etl.{module}")
    getattr(etl, first_task)()
    with pytest.raises(Exception) as caught:
        getattr(etl, failing_task)()
    return caught.value


def _kind(error: BaseException) -> str:
    text = "".join(traceback.format_exception(type(error), error, error.__traceback__))
    kind, _, _ = classify_error(
        type(error).__name__, str(error), traceback_text=text, repo_frames_present=True
    )
    return kind


def test_the_generator_is_deterministic(data: Path, tmp_path: Path) -> None:
    _generator().generate(tmp_path / "day1", tmp_path / "day2")
    for day in ("day1", "day2"):
        for name in FILES:
            assert (tmp_path / day / name).read_bytes() == (data / day / name).read_bytes(), f"{day}/{name}"


def test_payments_day2_breaks_the_contract_with_a_data_shaped_error(data: Path, repo: Path) -> None:
    dataset, violations = _day2_violations(data, repo, "payments")

    assert {(v.rule, v.column) for v in violations} == {
        ("dtype_mismatch", "amount"),
        ("nulls_not_allowed", "currency"),
    }
    assert {column.name for column in dataset.columns if column.pii} == {
        "payer_name",
        "payer_email",
        "payer_phone",
    }
    error = _day2_error("payments")
    assert isinstance(error, ValueError)
    assert _kind(error) == "data_shape"  # policy gate (a): approved contract violated -> DATA


def test_orders_day2_meets_the_contract_and_fails_in_the_code(data: Path, repo: Path) -> None:
    _, violations = _day2_violations(data, repo, "orders")

    assert violations == []
    error = _day2_error("orders")
    assert isinstance(error, ZeroDivisionError)
    assert _kind(error) == "code"  # policy gate (b): valid data, repository frame -> CODE


def test_fx_rates_day2_meets_the_contract(data: Path, repo: Path) -> None:
    _, violations = _day2_violations(data, repo, "fx_rates")

    assert violations == []


def test_a_refused_pricing_service_is_an_infrastructure_failure() -> None:
    # The error a Linux task runner raises when nothing listens on the pricing port.
    kind, _, marker = classify_error(
        "urllib.error.URLError", "<urlopen error [Errno 111] Connection refused>"
    )

    assert (kind, marker) == ("infra", "Connection refused")


def test_the_fake_llm_scripts_turn_the_regression_test_red_then_green(repo: Path) -> None:
    scripts = load_scripts(PACK / "fixtures" / "fake_llm")
    test_script = match_script(scripts, {**CODE_SCENARIO, "phase": "test"})
    fix_script = match_script(scripts, {**CODE_SCENARIO, "phase": "fix"})
    assert test_script is not None and fix_script is not None
    test_code = next(
        step["input"]["content"] for step in test_script["steps"] if step["tool"] == "write_file"
    )

    def failing_tests() -> list[tuple[str, str]]:
        _forget_src_modules()
        namespace: dict = {}
        exec(compile(test_code, "test_orders_average_item_price.py", "exec"), namespace)
        failures = []
        for name, test in sorted(namespace.items()):
            if name.startswith("test_"):
                try:
                    test()
                except Exception as exc:  # any failure of a test function counts
                    failures.append((name, type(exc).__name__))
        return failures

    assert failing_tests() == [
        ("test_average_item_price_is_none_for_orders_without_items", "ZeroDivisionError")
    ]

    module_path = repo / "src" / "etl" / "orders.py"
    source = module_path.read_text(encoding="utf-8")
    for step in fix_script["steps"]:
        if step["tool"] == "replace_in_file":
            assert source.count(step["input"]["old"]) == 1, step["input"]["old"]
            source = source.replace(step["input"]["old"], step["input"]["new"])
    module_path.write_text(source, encoding="utf-8")

    assert failing_tests() == []
