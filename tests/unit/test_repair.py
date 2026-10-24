"""Tests for dag_healer.agent.repair: RepairTools guardrails and the run_repair flow.

The flow tests use real git repositories, the real pytest runner and a scripted provider
stand-in (``ScriptedProvider``) that calls the repair tools like a model would.
"""

from __future__ import annotations

import os
import re
import shutil
from collections.abc import Callable, Mapping
from pathlib import Path
from typing import Any

import pytest

from dag_healer.agent import repair
from dag_healer.agent.context import IncidentContext
from dag_healer.agent.llm import LLMRefusal, LLMUnavailable, RepairSessionResult
from dag_healer.agent.repair import RepairResult, RepairTools, branch_name, run_repair
from dag_healer.agent.tests_gen import SYSTEM_PROMPT, TOOL_NAMES, system_prompt
from dag_healer.config import HealerConfig, config_from_mapping
from dag_healer.orchestrators.fake import FAKE_VOCABULARY
from dag_healer.pr.github import GitHubError
from dag_healer.repo.git import Workspace, run_git
from dag_healer.repo.pytest_runner import PytestResult

EXAMPLE_REPO = Path(__file__).resolve().parents[2] / "example_existing_repo"
GIT_IDENTITY = {
    "GIT_AUTHOR_NAME": "Test",
    "GIT_AUTHOR_EMAIL": "test@example.invalid",
    "GIT_COMMITTER_NAME": "Test",
    "GIT_COMMITTER_EMAIL": "test@example.invalid",
}
INCIDENT_ID = "4f7c2a9e1b3d4c5e8f9a0b1c2d3e4f5a"

Step = tuple[str, dict[str, Any]] | Callable[[Mapping[str, Callable[..., str]], Mapping[str, Any]], None]


# =========================================================================== helpers


class ScriptedProvider:
    """LLMProvider stand-in: replays scripted tool calls per phase and attempt.

    ``scripts[phase]`` is a list of attempts, each a list of steps; the last attempt repeats.
    A step is ``(tool_name, kwargs)`` or a callable ``(tools_by_name, scenario) -> None``.
    """

    name = "scripted"

    def __init__(
        self,
        scripts: Mapping[str, list[list[Step]]],
        *,
        tokens: tuple[int, int] = (100, 50),
        budget_exhausted: bool = False,
        error: Exception | None = None,
    ) -> None:
        self.scripts = scripts
        self.tokens = tokens
        self.budget_exhausted = budget_exhausted
        self.error = error
        self.calls: list[dict[str, Any]] = []

    def classify(self, *, system: str, prompt: str) -> Any:  # pragma: no cover - unused here
        raise AssertionError("classify must not be called by the repair flow")

    def repair_session(
        self,
        *,
        system: str,
        prompt: str,
        tools: list[Callable[..., str]],
        max_tokens_budget: int,
        scenario: dict[str, Any],
    ) -> RepairSessionResult:
        phase = scenario["phase"]
        by_name = {fn.__name__: fn for fn in tools}
        call: dict[str, Any] = {
            "phase": phase,
            "scenario": dict(scenario),
            "system": system,
            "prompt": prompt,
            "tool_names": sorted(by_name),
            "max_tokens_budget": max_tokens_budget,
            "outputs": [],
        }
        attempts = self.scripts.get(phase) or [[]]
        index = sum(1 for previous in self.calls if previous["phase"] == phase)
        self.calls.append(call)
        if self.error is not None:
            raise self.error
        for step in attempts[min(index, len(attempts) - 1)]:
            if callable(step):
                step(by_name, scenario)
                continue
            name, kwargs = step
            call["outputs"].append((name, by_name[name](**kwargs)))
        return RepairSessionResult(
            final_text=f"scripted {phase} session done",
            input_tokens=self.tokens[0],
            output_tokens=self.tokens[1],
            stop_reason="end_turn",
            budget_exhausted=self.budget_exhausted,
        )

    def phase_calls(self, phase: str) -> list[dict[str, Any]]:
        return [call for call in self.calls if call["phase"] == phase]


def git_commit_all(root: Path, message: str = "initial") -> None:
    if not (root / ".git").exists():
        run_git(["init", "--quiet", "-b", "main"], root)
    run_git(["add", "-A"], root)
    run_git(["-c", "commit.gpgsign=false", "commit", "--quiet", "-m", message], root, env=GIT_IDENTITY)


def write(root: Path, files: Mapping[str, str]) -> None:
    for rel, content in files.items():
        target = root / rel
        target.parent.mkdir(parents=True, exist_ok=True)
        target.write_text(content, encoding="utf-8", newline="\n")


def make_cfg(repo_path: Path, **sections: Any) -> HealerConfig:
    data: dict[str, Any] = {"repo": {"path": str(repo_path), "pytest_timeout": 120}}
    for key, value in sections.items():
        if isinstance(value, dict) and isinstance(data.get(key), dict):
            data[key] = {**data[key], **value}
        else:
            data[key] = value
    return config_from_mapping(data)


def diff_files(diff: str | None) -> set[str]:
    return set(re.findall(r"^diff --git a/(\S+) b/", diff or "", re.MULTILINE))


def source_branches(repo: Path) -> list[str]:
    out = run_git(["for-each-ref", "--format=%(refname:short)", "refs/heads"], repo).stdout
    return sorted(line for line in out.splitlines() if line)


@pytest.fixture(autouse=True)
def _isolated(healer_env: Path) -> Path:
    """Every test gets its own HEALER_HOME (PR.md / work folders)."""
    return healer_env


# --------------------------------------------------------------------------- calc repo

CALC_SOURCE = '''\
"""Tiny module with a latent bug."""


def parse_ratio(text):
    """Parse "a/b" into a float."""
    left, right = text.split("/")
    return int(left) / int(right)
'''
CALC_TEST_PATH = "tests/test_calc_parse_ratio.py"
CALC_TEST = '''\
from src.calc import parse_ratio


def test_parse_ratio_accepts_decimals():
    """compute failed with ValueError on decimal ratios."""
    assert parse_ratio("1.5/3") == 0.5
'''
CALC_PASSING_TEST = """\
from src.calc import parse_ratio


def test_parse_ratio_integers():
    assert parse_ratio("1/2") == 0.5
"""
CALC_IMPORT_ERROR_TEST = """\
from src.calc import does_not_exist


def test_missing():
    assert does_not_exist() == 1
"""
WRITE_CALC_TEST: Step = ("write_file", {"path": CALC_TEST_PATH, "content": CALC_TEST})
RUN_CALC_TEST: Step = ("run_tests", {"paths": [CALC_TEST_PATH]})
FIX_CALC: Step = (
    "replace_in_file",
    {
        "path": "src/calc.py",
        "old": "return int(left) / int(right)",
        "new": "return float(left) / float(right)",
    },
)


@pytest.fixture
def calc_repo(tmp_path: Path) -> Path:
    root = tmp_path / "calc_repo"
    write(
        root,
        {
            "src/__init__.py": "",
            "src/calc.py": CALC_SOURCE,
            "dags/calc_dag.py": "# wires src.calc.parse_ratio into the calc_dag DAG\n",
            ".gitignore": "__pycache__/\n",
        },
    )
    git_commit_all(root)
    return root


def calc_ctx(**overrides: Any) -> IncidentContext:
    values: dict[str, Any] = {
        "incident_id": INCIDENT_ID,
        "dag_id": "calc_dag",
        "run_id": "manual__2026-10-17T10:00:00+00:00",
        "task_id": "compute",
        "exception_type": "ValueError",
        "exception_message": "invalid literal for int() with base 10: '1.5'",
        "repo_frames": [{"file": "src/calc.py", "line": 7, "function": "parse_ratio"}],
        "code_snippets": {"src/calc.py": CALC_SOURCE},
        "contract_status": "approved",
    }
    values.update(overrides)
    return IncidentContext(**values)


def calc_incident(**overrides: Any) -> dict[str, Any]:
    incident = {
        "id": INCIDENT_ID,
        "dag_id": "calc_dag",
        "run_id": "manual__2026-10-17T10:00:00+00:00",
        "task_id": "compute",
        "map_index": -1,
        "try_number": 1,
        "status": "repairing",
        "category": "code",
        "confidence": 0.9,
        "error_summary": "ValueError: invalid literal for int() with base 10: '1.5'",
        "diagnosis": "parse_ratio converts both sides with int() and rejects decimal ratios.",
    }
    incident.update(overrides)
    return incident


def run_calc(cfg: HealerConfig, provider: ScriptedProvider, work: Path, **kwargs: Any) -> RepairResult:
    return run_repair(cfg, calc_incident(), calc_ctx(), provider, workspace_root=work, **kwargs)


# =========================================================================== RepairTools guardrails


@pytest.fixture
def workspace(tmp_path: Path) -> Path:
    root = tmp_path / "ws"
    write(
        root,
        {
            "src/etl/customers.py": "def f():\n    return 1\n",
            "dags/customers_sync.py": "# dag\n",
            "tests/test_existing.py": "def test_ok():\n    assert True\n",
            "data/incoming/customers.csv": "customer_id,email\nC1,olivia.martin@example.com\n",
            "fixtures/day2/customers.csv": "customer_id,email\nC1,olivia.martin@example.com\n",
            "README.md": "readme\n",
            ".env": "SECRET=1\n",
            "healer.yaml": "version: 1\n",
        },
    )
    (root / ".git").mkdir()
    (root / ".git" / "config").write_text("[core]\n", encoding="utf-8")
    (root / "__pycache__").mkdir()
    (root / "__pycache__" / "x.pyc").write_bytes(b"\0\0")
    return root


def tools_for(workspace: Path, phase: str, **cfg_sections: Any) -> RepairTools:
    cfg = make_cfg(workspace, **cfg_sections)
    return RepairTools(workspace, cfg, phase=phase, tests_dir="tests")  # type: ignore[arg-type]


def test_test_phase_writes_only_under_tests_dir(workspace: Path) -> None:
    tools = tools_for(workspace, "test")
    assert tools.write_file("tests/test_new.py", "def test_x():\n    assert 0\n").startswith("OK:")
    assert tools.write_file("tests/conftest.py", "").startswith("OK:")
    refused = tools.write_file("src/etl/customers.py", "broken\n")
    assert refused.startswith("Error:") and "test phase" in refused
    assert tools.replace_in_file("src/etl/customers.py", "return 1", "return 2").startswith("Error:")
    assert (workspace / "src/etl/customers.py").read_text() == "def f():\n    return 1\n"
    assert tools.written_paths == ["tests/test_new.py", "tests/conftest.py"]


def test_fix_phase_writes_only_allowed_paths_and_never_tests(workspace: Path) -> None:
    tools = tools_for(workspace, "fix")
    assert tools.replace_in_file("src/etl/customers.py", "return 1", "return 2").startswith("OK:")
    assert tools.write_file("dags/new_helper.py", "X = 1\n").startswith("OK:")
    for path in ("tests/test_existing.py", "tests/helpers.py", "src/etl/test_inline.py", "src/conftest.py"):
        message = tools.write_file(path, "x = 1\n")
        assert message.startswith("Error:") and "read-only" in message, path
    outside = tools.write_file("README.md", "changed\n")
    assert outside.startswith("Error:") and "src/, dags/" in outside
    assert (workspace / "README.md").read_text() == "readme\n"
    assert (workspace / "tests/test_existing.py").read_text().startswith("def test_ok")


def test_fix_phase_honours_configured_allowed_paths(workspace: Path) -> None:
    tools = tools_for(workspace, "fix", repo={"allowed_paths": ["src/etl/"]})
    assert tools.write_file("src/etl/new.py", "X = 1\n").startswith("OK:")
    assert tools.write_file("dags/x.py", "X = 1\n").startswith("Error:")
    assert tools.write_file("src/other.py", "X = 1\n").startswith("Error:")


@pytest.mark.parametrize(
    "path",
    [
        "/etc/passwd",
        "C:\\Windows\\win.ini",
        "C:/Users/x/file.py",
        "\\\\server\\share\\x.py",
        "~/x.py",
        "../outside.py",
        "tests/../../outside.py",
        "src/..\\..\\outside.py",
        "tests/file.py:stream",
        "",
    ],
)
@pytest.mark.parametrize("phase", ["test", "fix"])
def test_absolute_and_parent_paths_are_rejected(workspace: Path, phase: str, path: str) -> None:
    tools = tools_for(workspace, phase)
    for result in (
        tools.write_file(path, "x = 1\n"),
        tools.read_file(path),
        tools.list_files(path),
        tools.search("x", path),
        tools.replace_in_file(path, "a", "b"),
    ):
        assert result.startswith("Error:"), (path, result)
    assert not (workspace.parent / "outside.py").exists()
    assert tools.written_paths == []


def test_absolute_path_inside_the_workspace_is_rejected_too(workspace: Path) -> None:
    tools = tools_for(workspace, "fix")
    target = str(workspace / "src" / "etl" / "customers.py")
    assert tools.write_file(target, "x\n").startswith("Error: absolute paths are not allowed")
    assert tools.read_file(target).startswith("Error: absolute paths are not allowed")


@pytest.mark.parametrize(
    "path",
    [
        "healer.yaml",
        "src/healer.yaml",
        "healer/contracts/customers_sync.yaml",
        ".github/workflows/ci.yml",
        ".git/config",
        "src/.git/hooks/pre-commit",
        "poetry.lock",
        "src/uv.lock",
        "package-lock.json",
        ".env",
        "src/.env.local",
        "fixtures/day2/customers.csv",
        "fixtures/fake_llm/x.py",
        "data/incoming/customers.csv",
        "data/helper.py",
        "requirements.txt",
        "requirements-dev.txt",
        "pyproject.toml",
        "setup.py",
        "src/etl/sample.csv",
        "src/etl/sample.parquet",
        "tests/data.jsonl",
    ],
)
def test_protected_paths_are_never_written(workspace: Path, path: str) -> None:
    for phase in ("test", "fix"):
        tools = tools_for(workspace, phase, repo={"allowed_paths": ["."]})
        result = tools.write_file(path, "x = 1\n")
        assert result.startswith("Error:"), (phase, path, result)
        assert "denied" in result or "only write under" in result, result


@pytest.mark.parametrize(
    "path", [".GIT/config", "src/.Git/hooks/x", "Healer.yaml", "DATA/x.py", "Fixtures/x.py", "src/.ENV"]
)
def test_deny_checks_ignore_case(workspace: Path, path: str) -> None:
    tools = tools_for(workspace, "fix", repo={"allowed_paths": ["."]})
    assert tools.write_file(path, "x = 1\n").startswith("Error: writing"), path


def test_fix_phase_treats_tests_dir_case_insensitively(workspace: Path) -> None:
    tools = tools_for(workspace, "fix", repo={"allowed_paths": ["."]})
    assert "read-only" in tools.write_file("TESTS/test_existing.py", "x = 1\n")
    assert "read-only" in tools.write_file("src/Test_inline.py", "x = 1\n")


def test_custom_contracts_dir_is_protected(workspace: Path) -> None:
    tools = tools_for(workspace, "fix", repo={"allowed_paths": ["."]}, contracts={"dir": "src/contracts"})
    assert tools.write_file("src/contracts/x.yaml", "a: 1\n").startswith("Error:")
    assert tools.write_file("src/ok.py", "a = 1\n").startswith("OK:")


def test_data_and_secret_files_cannot_be_read_or_searched(workspace: Path) -> None:
    tools = tools_for(workspace, "test")
    for path in (
        "data/incoming/customers.csv",
        "fixtures/day2/customers.csv",
        ".env",
        "healer.yaml",
        ".git/config",
    ):
        assert tools.read_file(path).startswith("Error: reading"), path
    assert tools.search("olivia|SECRET") == "No matches for 'olivia|SECRET' in the repository."
    listing = tools.list_files()
    assert ".git/config" not in listing and "__pycache__" not in listing
    assert "src/etl/customers.py" in listing and "data/incoming/customers.csv" in listing


def test_read_file_numbers_lines_and_pages(workspace: Path) -> None:
    write(workspace, {"src/long.py": "".join(f"line_{i} = {i}\n" for i in range(1, 451))})
    tools = tools_for(workspace, "fix")
    first = tools.read_file("src/long.py")
    assert (
        first.splitlines()[0]
        == "src/long.py (lines 1-400 of 450); call read_file again with start_line=401 for more"
    )
    assert first.splitlines()[1] == "  1| line_1 = 1"
    window = tools.read_file("src/long.py", start_line=448, end_line=999)
    assert window.splitlines()[0] == "src/long.py (lines 448-450 of 450)"
    assert window.splitlines()[-1] == "450| line_450 = 450"
    assert tools.read_file("src/long.py", start_line=500).startswith("Error:")
    assert tools.read_file("src/long.py", start_line=10, end_line=5).startswith("Error:")
    assert tools.read_file("src/missing.py").startswith("Error:")
    assert tools.read_file("src").startswith("Error:")


def test_list_files_and_search(workspace: Path) -> None:
    tools = tools_for(workspace, "fix")
    assert tools.list_files("src", "*.py") == "src/etl/customers.py"
    assert tools.list_files(".", "tests/*.py") == "tests/test_existing.py"
    assert tools.list_files("src", "*.md").startswith("No files")
    assert tools.search(r"def f\(") == "src/etl/customers.py:1: def f():"
    assert tools.search("return", "src/etl/customers.py") == "src/etl/customers.py:2: return 1"
    assert tools.search("(unclosed").startswith("Error: invalid regular expression")


def test_replace_in_file_requires_exactly_one_occurrence_and_keeps_crlf(workspace: Path) -> None:
    target = workspace / "src" / "etl" / "crlf.py"
    target.write_bytes(b"def g(x):\r\n    y = x\r\n    y = x\r\n    return y\r\n")
    tools = tools_for(workspace, "fix")
    assert "not found" in tools.replace_in_file("src/etl/crlf.py", "z = 1", "z = 2")
    assert "occurs 2 times" in tools.replace_in_file("src/etl/crlf.py", "y = x", "y = x + 1")
    assert tools.replace_in_file(
        "src/etl/crlf.py", "y = x\n    return y", "y = x * 2\n    return y"
    ).startswith("OK:")
    assert target.read_bytes() == b"def g(x):\r\n    y = x\r\n    y = x * 2\r\n    return y\r\n"
    assert tools.replace_in_file("src/etl/new.py", "a", "b").startswith("Error:")
    assert tools.replace_in_file("src/etl/crlf.py", "", "b").startswith("Error:")


def test_revert_restores_originals_and_removes_new_files(workspace: Path) -> None:
    tools = tools_for(workspace, "fix")
    tools.replace_in_file("src/etl/customers.py", "return 1", "return 2")
    tools.write_file("src/etl/customers.py", "rewritten\n")
    tools.write_file("src/etl/brand_new.py", "X = 1\n")
    assert "+rewritten" in tools.session_diff()
    tools.revert()
    assert (workspace / "src/etl/customers.py").read_text() == "def f():\n    return 1\n"
    assert not (workspace / "src/etl/brand_new.py").exists()
    assert tools.written_paths == [] and tools.writes == []


def test_write_limit_and_size_limit(workspace: Path) -> None:
    cfg = make_cfg(workspace)
    tools = RepairTools(workspace, cfg, phase="test", tests_dir="tests", max_writes=2)
    assert tools.write_file("tests/a.py", "a\n").startswith("OK:")
    assert tools.write_file("tests/b.py", "b\n").startswith("OK:")
    assert "write limit" in tools.write_file("tests/c.py", "c\n")
    big = tools_for(workspace, "test").write_file("tests/big.py", "x" * (repair.MAX_WRITE_BYTES + 1))
    assert "larger than" in big


def test_symlink_escape_is_rejected(workspace: Path, tmp_path: Path) -> None:
    outside = tmp_path / "outside_dir"
    outside.mkdir()
    try:
        os.symlink(outside, workspace / "tests" / "escape", target_is_directory=True)
    except (OSError, NotImplementedError):
        pytest.skip("symlinks are not available")
    tools = tools_for(workspace, "test")
    assert "outside the repository" in tools.write_file("tests/escape/test_x.py", "x\n")
    assert not (outside / "test_x.py").exists()


def test_run_tests_masks_output_and_validates_paths(workspace: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    seen: list[Any] = []

    def fake_run_pytest(workdir: Path, targets: list[str] | None = None, **kwargs: Any) -> PytestResult:
        seen.append((Path(workdir), targets, kwargs))
        return PytestResult(
            exit_code=1,
            failed=1,
            collected=1,
            duration=0.2,
            output_tail="E   AssertionError: unexpected olivia.martin@example.com\n1 failed in 0.20s",
        )

    monkeypatch.setattr(repair, "run_pytest", fake_run_pytest)
    tools = tools_for(workspace, "fix")
    output = tools.run_tests(["tests/test_existing.py::test_ok"])
    assert output.startswith("$ pytest tests/test_existing.py::test_ok\nexit code 1: 0 passed, 1 failed")
    assert "olivia.martin@example.com" not in output and "[email]" in output
    assert tools.run_tests().startswith("$ pytest tests\n")
    assert seen[0][1] == ["tests/test_existing.py::test_ok"]
    assert seen[1][1] == ["tests"]
    assert seen[0][2]["timeout"] == 120
    assert tools.run_tests(["../x.py"]).startswith("Error:")
    assert tools.run_tests(["tests/missing.py"]).startswith("Error:")
    assert len(seen) == 2


def test_tool_functions_are_plain_named_callables_that_never_raise(workspace: Path) -> None:
    tools = tools_for(workspace, "test")
    functions = tools.as_functions()
    assert [fn.__name__ for fn in functions] == list(TOOL_NAMES)
    by_name = {fn.__name__: fn for fn in functions}
    for fn in functions:
        doc = fn.__doc__ or ""
        assert doc.splitlines()[0].endswith(".") and "Returns:" in doc
    assert "under tests/ only" in (by_name["write_file"].__doc__ or "")
    assert by_name["read_file"](123).startswith("Error:")  # type: ignore[arg-type]
    assert by_name["write_file"]("tests/x.py", None).startswith("Error:")  # type: ignore[arg-type]
    assert by_name["read_file"]("tests/test_existing.py", "a").startswith("Error:")  # type: ignore[arg-type]
    assert by_name["write_file"]("tests/test_new.py", "x = 1\n").startswith("OK:")
    assert tools.written_paths == ["tests/test_new.py"]


def test_tool_functions_convert_to_anthropic_tools(workspace: Path) -> None:
    anthropic = pytest.importorskip("anthropic")
    tools = tools_for(workspace, "fix")
    for fn in tools.as_functions():
        definition = anthropic.beta_tool(fn).to_dict()
        assert definition["name"] == fn.__name__
        assert definition["description"]
        properties = definition["input_schema"]["properties"]
        assert all(prop.get("description") for prop in properties.values()), definition
    write_def = anthropic.beta_tool(tools.as_functions()[3]).to_dict()
    assert write_def["input_schema"]["required"] == ["path", "content"]
    assert "fix phase" in write_def["description"]


def test_branch_name_is_sanitized() -> None:
    assert (
        branch_name("customers_sync", "transform", INCIDENT_ID) == "healer/customers_sync-transform-4f7c2a9e"
    )
    assert branch_name("my dag", "group.task", "ab:cd/ef0123456") == "healer/my-dag-group-task-ab-cd-ef"


# =========================================================================== run_repair end to end


CUSTOMERS_TEST_PATH = "tests/test_customers_parse_signup_date.py"
CUSTOMERS_TEST = '''\
from datetime import date

from src.etl.customers import parse_signup_date


def test_parse_signup_date_accepts_iso_timestamps():
    """customers_sync.transform failed on ISO-8601 signup timestamps."""
    assert parse_signup_date("2026-10-02T14:03:00Z") == date(2026, 10, 2)
'''
CUSTOMERS_OLD = '    return datetime.strptime(value.strip(), "%Y-%m-%d").date()'
CUSTOMERS_NEW = (
    "    value = value.strip()\n"
    '    if value.endswith("Z"):\n'
    '        value = value[:-1] + "+00:00"\n'
    "    return datetime.fromisoformat(value).date()"
)


@pytest.fixture
def example_repo(tmp_path: Path) -> Path:
    """A git copy of example_existing_repo with the day-2 customers drop in the working tree."""
    root = tmp_path / "example_repo"
    shutil.copytree(EXAMPLE_REPO, root, ignore=shutil.ignore_patterns("__pycache__", "processed", ".git"))
    git_commit_all(root)
    shutil.copyfile(
        root / "fixtures" / "day2" / "customers.csv", root / "data" / "incoming" / "customers.csv"
    )
    return root


def customers_ctx(repo: Path) -> IncidentContext:
    return IncidentContext(
        incident_id=INCIDENT_ID,
        dag_id="customers_sync",
        run_id="manual__2026-10-17T10:00:00+00:00",
        task_id="transform",
        exception_type="ValueError",
        exception_message="time data '2026-10-02T14:03:00Z' does not match format '%Y-%m-%d'",
        traceback_text=(
            "Traceback (most recent call last):\n"
            '  File "/opt/airflow/repo/src/etl/customers.py", line 37, in transform_customers\n'
            '  File "/opt/airflow/repo/src/etl/customers.py", line 20, in parse_signup_date\n'
            "ValueError: time data '2026-10-02T14:03:00Z' does not match format '%Y-%m-%d'"
        ),
        repo_frames=[
            {"file": "src/etl/customers.py", "line": 37, "function": "transform_customers"},
            {"file": "src/etl/customers.py", "line": 20, "function": "parse_signup_date"},
        ],
        dag_file="dags/customers_sync.py",
        code_snippets={"src/etl/customers.py": (repo / "src/etl/customers.py").read_text(encoding="utf-8")},
        contract_status="approved",
    )


def test_run_repair_end_to_end_on_the_example_repo(
    example_repo: Path, tmp_path: Path, healer_env: Path
) -> None:
    work = tmp_path / "work"
    provider = ScriptedProvider(
        {
            "test": [
                [
                    ("list_files", {"path": "src/etl", "pattern": "*.py"}),
                    ("read_file", {"path": "src/etl/customers.py"}),
                    ("write_file", {"path": CUSTOMERS_TEST_PATH, "content": CUSTOMERS_TEST}),
                    ("run_tests", {"paths": [CUSTOMERS_TEST_PATH]}),
                ]
            ],
            "fix": [
                [
                    (
                        "replace_in_file",
                        {"path": "src/etl/customers.py", "old": CUSTOMERS_OLD, "new": CUSTOMERS_NEW},
                    ),
                    ("run_tests", {"paths": [CUSTOMERS_TEST_PATH]}),
                    ("run_tests", {}),
                ]
            ],
        }
    )
    cfg = make_cfg(example_repo)
    incident = {
        "id": INCIDENT_ID,
        "dag_id": "customers_sync",
        "run_id": "manual__2026-10-17T10:00:00+00:00",
        "task_id": "transform",
        "map_index": -1,
        "try_number": 1,
        "category": "code",
        "confidence": 0.9,
        "error_summary": "ValueError: time data '2026-10-02T14:03:00Z' does not match format '%Y-%m-%d'",
        "diagnosis": "parse_signup_date hard-codes %Y-%m-%d and rejects valid ISO-8601 timestamps.",
    }

    result = run_repair(cfg, incident, customers_ctx(example_repo), provider, workspace_root=work)

    assert result.status == "repaired", result.error
    assert result.error is None
    assert result.test_path == CUSTOMERS_TEST_PATH
    assert result.test_code == CUSTOMERS_TEST
    assert result.created_tests_dir is True
    # the reproducing test failed before the fix and passes after it
    assert "1 failed" in (result.pytest_before or "")
    assert "ValueError" in (result.pytest_before or "")
    assert "exit code 0: 1 passed, 0 failed" in (result.pytest_after or "")
    # the model saw the failing run, then the green one
    test_run_output = provider.phase_calls("test")[0]["outputs"][-1][1]
    assert "exit code 1" in test_run_output and "1 failed" in test_run_output
    assert "1 passed" in provider.phase_calls("fix")[0]["outputs"][1][1]
    # the diff touches only the fixed module and the new test
    assert diff_files(result.diff) == {"src/etl/customers.py", CUSTOMERS_TEST_PATH}
    assert "+    return datetime.fromisoformat(value).date()" in (result.diff or "")
    # the branch was pushed into the source repository, whose working tree is untouched
    branch = result.branch
    assert branch == "healer/customers_sync-transform-4f7c2a9e"
    assert branch in source_branches(example_repo)
    fixed = run_git(["show", f"{branch}:src/etl/customers.py"], example_repo).stdout
    assert "fromisoformat" in fixed
    assert run_git(["show", f"{branch}:{CUSTOMERS_TEST_PATH}"], example_repo).stdout == CUSTOMERS_TEST
    assert "strptime" in (example_repo / "src/etl/customers.py").read_text(encoding="utf-8")
    status = run_git(["status", "--porcelain", "--untracked-files=all"], example_repo).stdout
    assert status.splitlines() == [" M data/incoming/customers.csv"]
    log = run_git(["log", "-1", "--format=%an <%ae>%n%B", branch], example_repo).stdout
    assert log.startswith("dag-healer <dag-healer@localhost>")
    assert "Fix ValueError in parse_signup_date (customers_sync.transform)" in log
    # PR.md + fix.patch (no GitHub configured)
    assert result.pr_url is None
    pr_dir = healer_env / "prs" / INCIDENT_ID
    assert result.pr_md_path == str(pr_dir / "PR.md")
    assert result.patch_path == str(pr_dir / "fix.patch")
    pr_md = (pr_dir / "PR.md").read_text(encoding="utf-8")
    assert "Generated by dag-healer" in pr_md
    assert "had no test suite" in pr_md
    assert CUSTOMERS_TEST_PATH in pr_md
    assert branch in pr_md
    patch = (pr_dir / "fix.patch").read_bytes().decode("utf-8")
    assert patch == result.diff
    run_git(["apply", "--check", str(pr_dir / "fix.patch")], example_repo)
    # the workspace is gone
    assert not (work / INCIDENT_ID).exists()
    # sessions, scenario, tools, budget
    assert result.attempts == 2
    assert result.tokens_used == 300
    assert [call["phase"] for call in provider.calls] == ["test", "fix"]
    assert provider.calls[0]["scenario"] == {
        "phase": "test",
        "dag_id": "customers_sync",
        "task_id": "transform",
        "exception_type": "ValueError",
        "incident_id": INCIDENT_ID,
        "orchestrator": "airflow",
        "pipeline_id": "customers_sync",
        "step_id": "transform",
    }
    assert provider.calls[1]["scenario"]["phase"] == "fix"
    assert all(call["system"] == SYSTEM_PROMPT for call in provider.calls)
    assert provider.calls[0]["tool_names"] == sorted(TOOL_NAMES)
    assert provider.calls[0]["max_tokens_budget"] == cfg.limits.max_tokens_per_incident
    assert provider.calls[1]["max_tokens_budget"] == cfg.limits.max_tokens_per_incident - 150
    assert CUSTOMERS_TEST_PATH in provider.calls[0]["prompt"]
    assert "1 failed" in provider.calls[1]["prompt"]


def test_fake_provider_scripts_repair_the_demo_customers_bug(
    example_repo: Path, tmp_path: Path, monkeypatch: pytest.MonkeyPatch, healer_env: Path
) -> None:
    """The keyless demo path: the real FakeProvider replays fixtures/fake_llm/customers_sync_*.yaml."""
    from dag_healer.agent.fake import FAKE_DIR_ENV, FakeProvider

    scripts = example_repo / "fixtures" / "fake_llm"
    if not any(scripts.glob("customers_sync_*.yaml")):
        pytest.skip("the demo fake-LLM scripts are not available")
    monkeypatch.delenv(FAKE_DIR_ENV, raising=False)
    cfg = make_cfg(example_repo, llm={"provider": "fake"})
    incident = {"id": INCIDENT_ID, "dag_id": "customers_sync", "run_id": "manual__1", "task_id": "transform"}

    result = run_repair(
        cfg, incident, customers_ctx(example_repo), FakeProvider(cfg), workspace_root=tmp_path / "w"
    )

    assert result.status == "repaired", result.error
    assert result.created_tests_dir is True
    assert result.test_path is not None and result.test_path.startswith("tests/test_")
    assert diff_files(result.diff) == {"src/etl/customers.py", result.test_path}
    assert "failed" in (result.pytest_before or "") and "exit code 0" in (result.pytest_after or "")
    assert result.branch in source_branches(example_repo)
    assert result.pr_md_path is not None and Path(result.pr_md_path).is_file()


# =========================================================================== run_repair failures


def test_provider_that_never_writes_a_test_fails_clearly(calc_repo: Path, tmp_path: Path) -> None:
    provider = ScriptedProvider({"test": [[("read_file", {"path": "src/calc.py"})]]})
    result = run_calc(make_cfg(calc_repo), provider, tmp_path / "work")
    assert result.status == "repair_failed"
    assert result.error == (
        "The repair agent could not produce a failing reproducing test after 3 attempts: "
        f"no test file was written (expected {CALC_TEST_PATH})."
    )
    assert result.attempts == 3
    assert "No test file was written" in provider.calls[1]["prompt"]
    assert [call["phase"] for call in provider.calls] == ["test"] * 3
    assert source_branches(calc_repo) == ["main"]
    assert not (tmp_path / "work" / INCIDENT_ID).exists()
    assert result.pr_md_path is None and result.diff is None


def test_test_that_passes_before_the_fix_is_regenerated_then_fails(calc_repo: Path, tmp_path: Path) -> None:
    passing = ("write_file", {"path": CALC_TEST_PATH, "content": CALC_PASSING_TEST})
    provider = ScriptedProvider({"test": [[passing]]})
    result = run_calc(make_cfg(calc_repo), provider, tmp_path / "work")
    assert result.status == "repair_failed"
    assert "after 3 attempts" in (result.error or "")
    assert "PASSES before any fix" in (result.error or "")
    assert len(provider.phase_calls("test")) == 3 and not provider.phase_calls("fix")
    assert "PASSES before any fix" in provider.calls[1]["prompt"]
    assert "Rejected test (rolled back)" in provider.calls[2]["prompt"]
    assert result.test_path is None
    assert source_branches(calc_repo) == ["main"]


def test_regenerated_test_is_accepted_once_it_fails_for_real(calc_repo: Path, tmp_path: Path) -> None:
    provider = ScriptedProvider(
        {
            "test": [
                [("write_file", {"path": CALC_TEST_PATH, "content": CALC_IMPORT_ERROR_TEST})],
                [("write_file", {"path": CALC_TEST_PATH, "content": CALC_PASSING_TEST})],
                [WRITE_CALC_TEST, RUN_CALC_TEST],
            ],
            "fix": [[FIX_CALC]],
        }
    )
    result = run_calc(make_cfg(calc_repo), provider, tmp_path / "work")
    assert result.status == "repaired", result.error
    assert len(provider.phase_calls("test")) == 3
    assert "could not be collected" in provider.calls[1]["prompt"]
    assert "PASSES before any fix" in provider.calls[2]["prompt"]
    assert result.test_code == CALC_TEST
    assert result.attempts == 4
    assert diff_files(result.diff) == {"src/calc.py", CALC_TEST_PATH}


def test_wrong_fix_is_rolled_back_and_retried(calc_repo: Path, tmp_path: Path) -> None:
    wrong = (
        "replace_in_file",
        {
            "path": "src/calc.py",
            "old": "return int(left) / int(right)",
            "new": "return int(float(left)) / int(right)",
        },
    )
    provider = ScriptedProvider({"test": [[WRITE_CALC_TEST]], "fix": [[wrong], [FIX_CALC]]})
    result = run_calc(make_cfg(calc_repo), provider, tmp_path / "work")
    assert result.status == "repaired", result.error
    second_fix_prompt = provider.phase_calls("fix")[1]["prompt"]
    assert "still fails after the fix" in second_fix_prompt
    assert "Rejected change (rolled back)" in second_fix_prompt
    assert "int(float(left))" in second_fix_prompt
    assert "float(left) / float(right)" in (result.diff or "")
    assert "int(float(left))" not in (result.diff or "")


def test_fix_phase_cannot_edit_the_test(calc_repo: Path, tmp_path: Path) -> None:
    cheat = ("write_file", {"path": CALC_TEST_PATH, "content": CALC_PASSING_TEST})
    provider = ScriptedProvider({"test": [[WRITE_CALC_TEST]], "fix": [[cheat]]})
    cfg = make_cfg(calc_repo, limits={"max_fix_attempts": 1})
    result = run_calc(cfg, provider, tmp_path / "work")
    assert result.status == "repair_failed"
    assert (
        result.error == "No acceptable fix after 1 attempt(s): no file was changed; apply the fix with "
        "replace_in_file or write_file."
    )
    output = provider.phase_calls("fix")[0]["outputs"][0][1]
    assert output.startswith("Error: in the fix phase tests are read-only")
    # the failing state is still visible to the reviewer
    assert diff_files(result.diff) == {CALC_TEST_PATH}


@pytest.mark.parametrize(
    ("limits", "fix_step"),
    [
        ({"max_diff_files": 1}, FIX_CALC),
        (
            {"max_diff_lines": 20},
            (
                "replace_in_file",
                {
                    "path": "src/calc.py",
                    "old": "return int(left) / int(right)",
                    "new": "".join(f"# padding {i}\n    " for i in range(30))
                    + "return float(left) / float(right)",
                },
            ),
        ),
    ],
)
def test_diff_limits_exceeded_fails(
    calc_repo: Path, tmp_path: Path, limits: dict[str, int], fix_step: Step
) -> None:
    provider = ScriptedProvider({"test": [[WRITE_CALC_TEST]], "fix": [[fix_step]]})
    cfg = make_cfg(calc_repo, limits={**limits, "max_fix_attempts": 2})
    result = run_calc(cfg, provider, tmp_path / "work")
    assert result.status == "repair_failed"
    assert "No acceptable fix after 2 attempt(s): the change is too large" in (result.error or "")
    assert len(provider.phase_calls("fix")) == 2
    assert "the change is too large" in provider.phase_calls("fix")[1]["prompt"]
    assert diff_files(result.diff) == {CALC_TEST_PATH}  # the oversized fix was rolled back
    assert source_branches(calc_repo) == ["main"]


def test_token_budget_exhaustion_fails(calc_repo: Path, tmp_path: Path) -> None:
    provider = ScriptedProvider({"test": [[WRITE_CALC_TEST]], "fix": [[FIX_CALC]]}, tokens=(500, 100))
    cfg = make_cfg(calc_repo, limits={"max_tokens_per_incident": 1000})
    result = run_calc(cfg, provider, tmp_path / "work")
    assert result.status == "repair_failed"
    assert result.error == (
        "The token budget of this incident (limits.max_tokens_per_incident = 1000) was exhausted during "
        "the fix phase after 1200 tokens."
    )
    assert result.tokens_used == 1200
    assert [call["max_tokens_budget"] for call in provider.calls] == [1000, 400]
    assert source_branches(calc_repo) == ["main"]


def test_session_reporting_budget_exhausted_stops_the_repair(calc_repo: Path, tmp_path: Path) -> None:
    provider = ScriptedProvider({"test": [[WRITE_CALC_TEST]]}, tokens=(10, 10), budget_exhausted=True)
    result = run_calc(make_cfg(calc_repo), provider, tmp_path / "work")
    assert result.status == "repair_failed"
    assert "exhausted during the test phase" in (result.error or "")
    assert len(provider.calls) == 1


@pytest.mark.parametrize(
    ("error", "expected"),
    [
        (
            LLMUnavailable(
                "ANTHROPIC_API_KEY is not set", partial=RepairSessionResult(input_tokens=10, output_tokens=5)
            ),
            "The LLM is unavailable: ANTHROPIC_API_KEY is not set",
        ),
        (LLMRefusal("refused"), "The LLM declined to repair this incident: refused"),
    ],
)
def test_llm_errors_become_repair_failed(
    calc_repo: Path, tmp_path: Path, error: Exception, expected: str
) -> None:
    provider = ScriptedProvider({}, error=error)
    result = run_calc(make_cfg(calc_repo), provider, tmp_path / "work")
    assert result.status == "repair_failed"
    assert result.error == expected
    assert result.tokens_used == (15 if isinstance(error, LLMUnavailable) else 0)
    assert not (tmp_path / "work" / INCIDENT_ID).exists()


def test_not_a_git_repository(tmp_path: Path) -> None:
    plain = tmp_path / "plain"
    write(plain, {"src/calc.py": CALC_SOURCE})
    provider = ScriptedProvider({})
    result = run_repair(
        make_cfg(plain), calc_incident(), calc_ctx(), provider, workspace_root=tmp_path / "work"
    )
    assert result.status == "repair_failed"
    assert "is not a git repository" in (result.error or "")
    assert provider.calls == []


# =========================================================================== run_repair variants


def test_existing_suite_must_stay_green(tmp_path: Path) -> None:
    root = tmp_path / "suite_repo"
    write(
        root,
        {
            "src/__init__.py": "",
            "src/calc.py": CALC_SOURCE,
            "tests/test_existing.py": CALC_PASSING_TEST,
        },
    )
    git_commit_all(root)
    breaks_suite = (
        "replace_in_file",
        {
            "path": "src/calc.py",
            "old": "return int(left) / int(right)",
            "new": 'return float(left) / float(right) if "." in text else 0.0',
        },
    )
    provider = ScriptedProvider({"test": [[WRITE_CALC_TEST]], "fix": [[breaks_suite], [FIX_CALC]]})
    result = run_calc(make_cfg(root), provider, tmp_path / "work")
    assert result.status == "repaired", result.error
    assert result.created_tests_dir is False
    assert result.test_path == CALC_TEST_PATH
    assert "existing test suite fails" in provider.phase_calls("fix")[1]["prompt"]
    assert "$ pytest tests\n" in (result.pytest_after or "")
    assert "2 passed" in (result.pytest_after or "")
    assert not any("already failed" in note for note in result.notes)


def test_changes_made_outside_the_tools_are_not_committed(calc_repo: Path, tmp_path: Path) -> None:
    work = tmp_path / "work"

    def side_effects(_tools: Mapping[str, Callable[..., str]], scenario: Mapping[str, Any]) -> None:
        ws = work / scenario["incident_id"]
        (ws / "stray.txt").write_text("generated by a test run\n")
        (ws / "src" / "generated.py").write_text("X = 1\n")
        (ws / "dags" / "calc_dag.py").write_text("# modified behind the agent's back\n")

    provider = ScriptedProvider({"test": [[WRITE_CALC_TEST]], "fix": [[FIX_CALC, side_effects]]})
    result = run_calc(make_cfg(calc_repo), provider, work)
    assert result.status == "repaired", result.error
    assert diff_files(result.diff) == {"src/calc.py", CALC_TEST_PATH}
    tree = run_git(["ls-tree", "-r", "--name-only", result.branch or ""], calc_repo).stdout.split()
    assert "stray.txt" not in tree and "src/generated.py" not in tree
    dag = run_git(["show", f"{result.branch}:dags/calc_dag.py"], calc_repo).stdout
    assert "behind the agent's back" not in dag


def test_github_pull_request_from_origin_remote(
    calc_repo: Path, tmp_path: Path, monkeypatch: pytest.MonkeyPatch, healer_env: Path
) -> None:
    run_git(["remote", "add", "origin", "git@github.com:acme/pipelines.git"], calc_repo)
    pushes: list[tuple[str, str, str | None]] = []
    prs: list[dict[str, Any]] = []

    def fake_push(self: Workspace, remote: str, branch: str, *, token: str | None = None, **_: Any) -> None:
        pushes.append((remote, branch, token))

    def fake_create_pull_request(**kwargs: Any) -> str:
        prs.append(kwargs)
        return "https://github.com/acme/pipelines/pull/7"

    monkeypatch.setattr(Workspace, "push", fake_push)
    monkeypatch.setattr(repair, "create_pull_request", fake_create_pull_request)
    provider = ScriptedProvider({"test": [[WRITE_CALC_TEST]], "fix": [[FIX_CALC]]})
    result = run_calc(make_cfg(calc_repo), provider, tmp_path / "work", github_token="ghp_test_token")
    assert result.status == "repaired", result.error
    assert result.pr_url == "https://github.com/acme/pipelines/pull/7"
    assert result.pr_md_path is None and result.patch_path is None
    assert pushes == [("https://github.com/acme/pipelines.git", result.branch, "ghp_test_token")]
    assert prs[0]["repo"] == "acme/pipelines"
    assert prs[0]["head"] == result.branch and prs[0]["base"] == "main"
    assert prs[0]["token"] == "ghp_test_token"
    assert prs[0]["title"] == "Fix ValueError in parse_ratio (calc_dag.compute)"
    assert "Generated by dag-healer" in prs[0]["body"]
    assert source_branches(calc_repo) == ["main"]  # nothing pushed into the local repository
    assert not (healer_env / "prs").exists()


def test_github_failure_falls_back_to_pr_md(
    calc_repo: Path, tmp_path: Path, monkeypatch: pytest.MonkeyPatch, healer_env: Path
) -> None:
    def failing_create_pull_request(**_: Any) -> str:
        raise GitHubError("GitHub rejected the pull request (422)", status_code=422)

    monkeypatch.setattr(Workspace, "push", lambda self, remote, branch, *, token=None, **_: None)
    monkeypatch.setattr(repair, "create_pull_request", failing_create_pull_request)
    cfg = make_cfg(calc_repo, github={"repo": "acme/pipelines", "pr_base": "develop"})
    provider = ScriptedProvider({"test": [[WRITE_CALC_TEST]], "fix": [[FIX_CALC]]})
    result = run_calc(cfg, provider, tmp_path / "work", github_token="ghp_test_token")
    assert result.status == "repaired", result.error
    assert result.pr_url is None
    assert result.pr_md_path is not None and Path(result.pr_md_path).is_file()
    assert any("Opening a GitHub pull request on acme/pipelines failed" in note for note in result.notes)
    assert "Opening a GitHub pull request" in Path(result.pr_md_path).read_text(encoding="utf-8")
    assert result.branch in source_branches(calc_repo)


def test_prepare_notes_checked_out_branch_and_uncommitted_code(calc_repo: Path, tmp_path: Path) -> None:
    run_git(["checkout", "--quiet", "-b", "feature"], calc_repo)
    (calc_repo / "src" / "calc.py").write_text(CALC_SOURCE + "# local edit\n", encoding="utf-8")
    provider = ScriptedProvider({"test": [[]]})
    result = run_calc(make_cfg(calc_repo), provider, tmp_path / "work")
    assert result.status == "repair_failed"
    assert any("based on `feature`" in note for note in result.notes)
    assert any("uncommitted code changes (`src/calc.py`)" in note for note in result.notes)
    # the user's working tree and checked-out branch are left alone
    assert run_git(["symbolic-ref", "--short", "HEAD"], calc_repo).stdout.strip() == "feature"
    assert (calc_repo / "src" / "calc.py").read_text(encoding="utf-8").endswith("# local edit\n")


def test_already_red_suite_only_must_not_get_worse(tmp_path: Path) -> None:
    root = tmp_path / "red_repo"
    write(
        root,
        {
            "src/__init__.py": "",
            "src/calc.py": CALC_SOURCE,
            "tests/test_existing.py": "def test_known_broken():\n    assert 1 == 2\n",
        },
    )
    git_commit_all(root)
    provider = ScriptedProvider({"test": [[WRITE_CALC_TEST]], "fix": [[FIX_CALC]]})
    result = run_calc(make_cfg(root), provider, tmp_path / "work")
    assert result.status == "repaired", result.error
    assert any("already failed before any change (1 failed, 0 errors)" in note for note in result.notes)
    assert "1 failed, 1 passed" in (result.pytest_after or "") or "1 passed, 1 failed" in (
        result.pytest_after or ""
    )


def test_debug_keeps_the_workspace(calc_repo: Path, tmp_path: Path) -> None:
    provider = ScriptedProvider({"test": [[]]})
    result = run_calc(make_cfg(calc_repo, debug=True), provider, tmp_path / "work")
    assert result.status == "repair_failed"
    assert (tmp_path / "work" / INCIDENT_ID / ".git").is_dir()
    assert any("Workspace kept at" in note for note in result.notes)


def test_default_workspace_root_and_unique_branch(calc_repo: Path, healer_env: Path) -> None:
    existing = branch_name("calc_dag", "compute", INCIDENT_ID)
    run_git(["branch", existing], calc_repo)
    seen: list[bool] = []

    def check_workspace(_tools: Mapping[str, Callable[..., str]], scenario: Mapping[str, Any]) -> None:
        seen.append((healer_env / "work" / scenario["incident_id"] / ".git").is_dir())

    provider = ScriptedProvider({"test": [[check_workspace]]})
    result = run_repair(make_cfg(calc_repo), calc_incident(), calc_ctx(), provider)
    assert seen == [True, True, True]
    assert result.branch == existing + "-2"
    assert not (healer_env / "work" / INCIDENT_ID).exists()


def test_retry_reuses_a_leftover_workspace_folder(calc_repo: Path, tmp_path: Path) -> None:
    leftover = tmp_path / "work" / INCIDENT_ID
    write(leftover, {"junk.txt": "from a crashed attempt\n"})
    provider = ScriptedProvider({"test": [[WRITE_CALC_TEST]], "fix": [[FIX_CALC]]})
    result = run_calc(make_cfg(calc_repo), provider, tmp_path / "work")
    assert result.status == "repaired", result.error
    assert "junk.txt" not in (result.diff or "")


def test_result_to_dict_is_json_safe() -> None:
    import json

    result = RepairResult(status="repair_failed", notes=["n"], error="e")
    assert json.loads(json.dumps(result.to_dict()))["status"] == "repair_failed"
    assert result.ok is False


# =========================================================================== orchestrators


def fakeflow_ctx(**overrides: Any) -> IncidentContext:
    """``calc_ctx`` as gathered through the Fakeflow adapter (``orchestrators.fake``)."""
    values: dict[str, Any] = {
        "orchestrator": "fake",
        "orchestrator_name": "Fakeflow",
        "vocabulary": FAKE_VOCABULARY.as_dict(),
    }
    values.update(overrides)
    return calc_ctx(**values)


def test_extra_env_reaches_every_pytest_run(
    calc_repo: Path, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    real_run_pytest = repair.run_pytest
    calls: list[dict[str, Any]] = []

    def recording_run_pytest(workdir: Path, targets: list[str] | None = None, **kwargs: Any) -> PytestResult:
        calls.append(kwargs)
        return real_run_pytest(workdir, targets, **kwargs)

    monkeypatch.setattr(repair, "run_pytest", recording_run_pytest)
    sandbox: dict[str, str | None] = {"FAKEFLOW_TOKEN": None, "AIRFLOW__CORE__LOAD_EXAMPLES": "False"}
    provider = ScriptedProvider(
        {"test": [[WRITE_CALC_TEST, RUN_CALC_TEST]], "fix": [[FIX_CALC, ("run_tests", {})]]}
    )
    result = run_calc(make_cfg(calc_repo), provider, tmp_path / "work", extra_env=sandbox)
    assert result.status == "repaired", result.error
    tool_runs = [call for call in calls if "python" in call]  # the agent's run_tests tool
    own_runs = [call for call in calls if "python" not in call]  # the repair's own checks
    assert (len(tool_runs), len(own_runs)) == (2, 2)
    assert all(call["extra_env"] == sandbox for call in calls)


def test_a_fakeflow_repair_speaks_the_fakeflow_words(
    calc_repo: Path, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    real_render = repair.render_pr_markdown
    rendered: list[dict[str, Any]] = []

    def recording_render(**kwargs: Any) -> str:
        rendered.append(kwargs)
        return real_render(**kwargs)

    monkeypatch.setattr(repair, "render_pr_markdown", recording_render)
    ctx = fakeflow_ctx()
    provider = ScriptedProvider({"test": [[WRITE_CALC_TEST]], "fix": [[FIX_CALC]]})
    result = run_repair(
        make_cfg(calc_repo), calc_incident(orchestrator="fake"), ctx, provider, workspace_root=tmp_path / "w"
    )
    assert result.status == "repaired", result.error
    # both sessions get the Fakeflow system prompt and the neutral scenario keys
    assert system_prompt(ctx) != SYSTEM_PROMPT and "Fakeflow deployment" in system_prompt(ctx)
    assert [call["system"] for call in provider.calls] == [system_prompt(ctx)] * 2
    assert provider.calls[0]["scenario"] == {
        "phase": "test",
        "dag_id": "calc_dag",
        "task_id": "compute",
        "exception_type": "ValueError",
        "incident_id": INCIDENT_ID,
        "orchestrator": "fake",
        "pipeline_id": "calc_dag",
        "step_id": "compute",
    }
    assert all("A Fakeflow step of this repository failed" in call["prompt"] for call in provider.calls)
    # PR.md and the commit message
    assert rendered and all(call["vocabulary"] == FAKE_VOCABULARY.as_dict() for call in rendered)
    assert result.pr_md_path is not None
    assert "Failure reported by Fakeflow (masked):" in Path(result.pr_md_path).read_text(encoding="utf-8")
    message = run_git(["log", "-1", "--format=%B", str(result.branch)], calc_repo).stdout
    run = "manual__2026-10-17T10:00:00+00:00"
    assert f"Incident: {INCIDENT_ID} (flow calc_dag, step compute, run {run})" in message


@pytest.mark.parametrize(("ctx_factory", "step"), [(calc_ctx, "task"), (fakeflow_ctx, "step")])
def test_the_repair_feedback_names_the_step(
    calc_repo: Path, tmp_path: Path, ctx_factory: Callable[..., IncidentContext], step: str
) -> None:
    passing = ("write_file", {"path": CALC_TEST_PATH, "content": CALC_PASSING_TEST})
    provider = ScriptedProvider({"test": [[passing], [WRITE_CALC_TEST]], "fix": [[FIX_CALC]]})
    # the reproducing test fails with ValueError, the incident reported an OverflowError
    ctx = ctx_factory(exception_type="OverflowError")
    result = run_repair(make_cfg(calc_repo), calc_incident(), ctx, provider, workspace_root=tmp_path / "w")
    assert result.status == "repaired", result.error
    assert f"Assert the correct behaviour for the input that broke the {step}." in provider.calls[1]["prompt"]
    assert any(f"check that it fails for the same reason as the {step}." in note for note in result.notes)
