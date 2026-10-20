"""Tests for dag_healer.agent.tests_gen: test placement and the repair prompts."""

from __future__ import annotations

import re
from pathlib import Path

import pytest

from dag_healer.agent.context import IncidentContext
from dag_healer.agent.tests_gen import (
    FIX_PROMPT_FILE,
    SYSTEM_PROMPT,
    TEST_PROMPT_FILE,
    TOOL_NAMES,
    build_fix_prompt,
    build_test_prompt,
    choose_test_location,
    import_hint,
    load_prompt,
    render_evidence,
    render_template,
    repro_target,
)
from dag_healer.repo.dagscan import TaskInfo
from dag_healer.repo.profile import TestLayout

CUSTOMERS_SOURCE = '''\
"""CRM customers sync."""
from datetime import datetime


def parse_signup_date(value):
    """Parse the signup date sent by the CRM export (always YYYY-MM-DD)."""
    return datetime.strptime(value.strip(), "%Y-%m-%d").date()
'''

NO_TESTS = TestLayout(tests_dir=None, exists=False, framework=None, has_conftest=False)


def _ctx(**overrides: object) -> IncidentContext:
    values: dict[str, object] = {
        "incident_id": "inc0123456789",
        "dag_id": "customers_sync",
        "run_id": "manual__2026-10-17T10:00:00+00:00",
        "task_id": "transform",
        "exception_type": "ValueError",
        "exception_message": "time data '2026-10-02T14:03:00Z' does not match format '%Y-%m-%d'",
        "traceback_text": (
            "Traceback (most recent call last):\n"
            '  File "/opt/airflow/repo/src/etl/customers.py", line 37, in transform_customers\n'
            '  File "/opt/airflow/repo/src/etl/customers.py", line 19, in parse_signup_date\n'
            "ValueError: time data '2026-10-02T14:03:00Z' does not match format '%Y-%m-%d'"
        ),
        "repo_frames": [
            {"file": "src/etl/customers.py", "line": 37, "function": "transform_customers"},
            {"file": "src/etl/customers.py", "line": 30, "function": "<lambda>"},
            {"file": "src/etl/customers.py", "line": 19, "function": "parse_signup_date"},
        ],
        "dag_file": "dags/customers_sync.py",
        "task": TaskInfo(
            task_id="transform",
            operator="PythonOperator",
            callable_name="transform_customers",
            callable_module="src.etl.customers",
            callable_file="src/etl/customers.py",
            lineno=21,
        ),
        "code_snippets": {"src/etl/customers.py": CUSTOMERS_SOURCE},
        "contract_status": "approved",
        "masked_sample": {
            "dataset": "customers",
            "columns": ["customer_id", "email", "signup_date"],
            "rows": [
                {"customer_id": "CUS-1001", "email": "h:1a2b3c4d5e", "signup_date": "2026-10-02T14:03:00Z"}
            ],
            "masked": True,
        },
        "log_excerpt": "[2026-10-17T10:00:01Z] ERROR - Task failed\nValueError: time data",
        "notes": ["Logs read from the Airflow REST API."],
    }
    values.update(overrides)
    return IncidentContext(**values)  # type: ignore[arg-type]


# --------------------------------------------------------------------------- target


def test_repro_target_uses_innermost_named_repo_frame() -> None:
    target = repro_target(_ctx())
    assert target == {
        "file": "src/etl/customers.py",
        "module": "customers",
        "symbol": "parse_signup_date",
        "line": 19,
    }


def test_repro_target_skips_test_frames_and_lambdas() -> None:
    ctx = _ctx(
        repo_frames=[
            {"file": "src/etl/inventory.py", "line": 10, "function": "buildReport"},
            {"file": "src/etl/inventory.py", "line": 12, "function": "<listcomp>"},
            {"file": "tests/test_inventory.py", "line": 3, "function": "test_x"},
        ]
    )
    target = repro_target(ctx)
    assert target["symbol"] == "buildReport"
    assert target["module"] == "inventory"


def test_repro_target_falls_back_to_task_callable_then_dag() -> None:
    ctx = _ctx(repo_frames=[])
    assert repro_target(ctx)["symbol"] == "transform_customers"
    assert repro_target(ctx)["file"] == "src/etl/customers.py"
    bare = _ctx(repo_frames=[], task=None)
    target = repro_target(bare)
    assert target["module"] == "customers_sync"
    assert target["symbol"] == "transform"


# --------------------------------------------------------------------------- location


def test_choose_location_creates_tests_dir_when_repo_has_none(tmp_path: Path) -> None:
    tests_dir, test_file, created = choose_test_location(NO_TESTS, _ctx(), tmp_path)
    assert tests_dir == "tests"
    assert test_file == "tests/test_customers_parse_signup_date.py"
    assert created is True


def test_choose_location_reuses_existing_tests_dir_and_suffix_style(tmp_path: Path) -> None:
    (tmp_path / "test" / "unit").mkdir(parents=True)
    layout = TestLayout(
        tests_dir="test", exists=True, framework="pytest", has_conftest=False, file_pattern="*_test.py"
    )
    tests_dir, test_file, created = choose_test_location(layout, _ctx(), tmp_path)
    assert (tests_dir, test_file, created) == ("test", "test/customers_parse_signup_date_test.py", False)


def test_choose_location_never_reuses_an_existing_file(tmp_path: Path) -> None:
    (tmp_path / "tests").mkdir()
    (tmp_path / "tests" / "test_customers_parse_signup_date.py").write_text("x = 1\n")
    (tmp_path / "tests" / "test_customers_parse_signup_date_regression.py").write_text("x = 1\n")
    layout = TestLayout(tests_dir="tests", exists=True, framework="pytest", has_conftest=False)
    _dir, test_file, created = choose_test_location(layout, _ctx(), tmp_path)
    assert test_file == "tests/test_customers_parse_signup_date_regression_2.py"
    assert created is False


def test_choose_location_snake_cases_camel_case_symbols(tmp_path: Path) -> None:
    ctx = _ctx(repo_frames=[{"file": "src/etl/inventory.py", "line": 5, "function": "buildReport"}])
    _dir, test_file, _created = choose_test_location(NO_TESTS, ctx, tmp_path)
    assert test_file == "tests/test_inventory_build_report.py"


def test_choose_location_with_tests_at_repo_root(tmp_path: Path) -> None:
    layout = TestLayout(tests_dir=".", exists=True, framework="pytest", has_conftest=False)
    tests_dir, test_file, created = choose_test_location(layout, _ctx(), tmp_path)
    assert (tests_dir, test_file, created) == (".", "test_customers_parse_signup_date.py", False)


def test_choose_location_ignores_exotic_file_patterns(tmp_path: Path) -> None:
    layout = TestLayout(
        tests_dir="tests", exists=True, framework="pytest", has_conftest=False, file_pattern="check_*_[ab].py"
    )
    _dir, test_file, _created = choose_test_location(layout, _ctx(), tmp_path)
    assert test_file == "tests/test_customers_parse_signup_date.py"


# --------------------------------------------------------------------------- templates


def test_render_template_does_not_rescan_values() -> None:
    rendered = render_template("a {{ x }} b {{y}} c {{missing}}", {"x": "{{y}}", "y": "Y"})
    assert rendered == "a {{y}} b Y c {{missing}}"


def test_prompt_files_list_tools_and_guardrails() -> None:
    for name in (TEST_PROMPT_FILE, FIX_PROMPT_FILE):
        text = load_prompt(name)
        for tool in TOOL_NAMES:
            assert f"`{tool}(" in text, (name, tool)
        for guard in (
            "healer.yaml",
            ".github/**",
            ".git/**",
            ".env*",
            "fixtures/**",
            "data/**",
            "lockfiles",
            "..",
        ):
            assert guard in text, (name, guard)
    assert "ONE" in load_prompt(TEST_PROMPT_FILE)
    assert "no new dependencies" in load_prompt(FIX_PROMPT_FILE)
    assert "dag-healer" in SYSTEM_PROMPT


def test_import_hint_mirrors_the_dag_import() -> None:
    hint = import_hint(_ctx(), NO_TESTS)
    assert "`from src.etl.customers import parse_signup_date`" in hint


def test_import_hint_prefers_the_scanned_short_module_name() -> None:
    task = TaskInfo(
        task_id="transform",
        operator="PythonOperator",
        callable_name="transform_customers",
        callable_module="etl.customers",
        callable_file="src/etl/customers.py",
    )
    hint = import_hint(_ctx(task=task), None)
    assert "`from etl.customers import parse_signup_date`" in hint


def test_build_test_prompt_contains_the_evidence_and_no_placeholders(tmp_path: Path) -> None:
    prompt = build_test_prompt(_ctx(), NO_TESTS, "tests/test_customers_parse_signup_date.py")
    assert "{{" not in prompt
    assert "tests/test_customers_parse_signup_date.py" in prompt
    assert 'run_tests(["tests/test_customers_parse_signup_date.py"])' in prompt
    assert "ONLY under `tests/`" in prompt
    assert "2026-10-02T14:03:00Z" in prompt  # ISO timestamps survive masking
    assert "`src/etl/customers.py`, `parse_signup_date` (line 19)" in prompt
    assert "def parse_signup_date(value):" in prompt
    assert "h:1a2b3c4d5e" in prompt  # the (already masked) sample is shown as-is
    assert "has no test suite" in prompt
    assert "Contract status: approved" in prompt
    assert "Previous attempts" not in prompt


def test_build_test_prompt_masks_free_text_again() -> None:
    ctx = _ctx(
        exception_message="bad row for olivia.martin@example.com, phone +1 202-555-0101",
        log_excerpt="contact olivia.martin@example.com",
    )
    prompt = build_test_prompt(ctx, NO_TESTS, "tests/test_x.py")
    assert "olivia.martin@example.com" not in prompt
    assert "202-555-0101" not in prompt
    assert "[email]" in prompt


def test_build_test_prompt_existing_suite_style_and_previous_attempts(tmp_path: Path) -> None:
    layout = TestLayout(
        tests_dir="tests",
        exists=True,
        framework="unittest",
        has_conftest=True,
        import_style="from src.etl import common",
    )
    prompt = build_test_prompt(
        _ctx(),
        layout,
        "tests/test_customers_parse_signup_date.py",
        created_tests_dir=False,
        previous_attempts=["Rejected: the test PASSES before any fix", "Rejected: import error"],
    )
    assert "unittest.TestCase" in prompt
    assert "from src.etl import common" in prompt
    assert "already has tests" in prompt
    assert "### Attempt 1" in prompt and "### Attempt 2" in prompt
    assert "PASSES before any fix" in prompt


def test_build_fix_prompt_shows_failure_test_and_limits() -> None:
    test_code = "def test_x():\n    assert parse('```') == {'a': 1}\n"
    prompt = build_fix_prompt(
        _ctx(),
        "tests/test_customers_parse_signup_date.py",
        "exit code 1: 1 failed\nE   ValueError: time data for olivia.martin@example.com",
        test_code=test_code,
        allowed_paths=["src/", "dags/"],
        max_diff_files=10,
        max_diff_lines=400,
        previous_attempts=["Rejected: the change is too large"],
    )
    assert "{{" not in prompt
    assert "ONLY under: `src/`, `dags/`" in prompt
    assert "at most 10 changed files and at most 400 changed lines" in prompt
    assert "1 failed" in prompt
    assert "olivia.martin@example.com" not in prompt
    assert test_code.strip().splitlines()[-1] in prompt
    # the test contains a triple backtick: the fence around it must be longer
    assert re.search(r"^````python$", prompt, re.MULTILINE)
    assert "the change is too large" in prompt


def test_render_evidence_lists_violations_and_contract() -> None:
    from dag_healer.contracts.schema import Violation

    ctx = _ctx(
        draft_violations=[
            Violation(
                rule="missing_column", dataset="inventory", column="qty", message="column qty is missing"
            )
        ]
    )
    text = render_evidence(
        ctx, incident={"diagnosis": "strptime cannot parse ISO timestamps", "evidence": ["e1"]}
    )
    assert "Draft-contract violations" in text
    assert "missing_column" in text
    assert "Classifier diagnosis: strptime cannot parse ISO timestamps" in text
    assert "  - e1" in text


@pytest.mark.parametrize("name", [TEST_PROMPT_FILE, FIX_PROMPT_FILE])
def test_prompts_are_english_markdown(name: str) -> None:
    text = load_prompt(name)
    assert text.startswith("# Phase")
    assert "{{evidence}}" in text and "{{previous_attempts}}" in text
