"""The ``context`` line of ``dag-healer doctor`` and ``dag-healer init --context``.

The doctor tests run :func:`dag_healer.cli.doctor_cmd.check_context` on real temporary git
repositories (and the whole ``doctor`` command twice); ``init --context`` runs on copies of
``example_existing_repo``, with and without its own context files (``healer/``). Commits use a
fixed identity passed with ``-c``, the host's git configuration is ignored, and the day that
decides which files are overdue for a review is :data:`TODAY` (``doctor`` passes no date).
"""

from __future__ import annotations

import shutil
import subprocess
from collections.abc import Iterator, Mapping
from datetime import date, datetime, timezone
from pathlib import Path
from typing import Any

import pytest
import yaml

from dag_healer.cli.doctor_cmd import ERROR, OK, SKIP, WARN, check_context, format_results
from dag_healer.cli.init_cmd import write_context_starters
from dag_healer.cli.main import main
from dag_healer.config import HealerConfig, config_from_mapping, load_config_file
from dag_healer.context import read_all, read_context
from dag_healer.context.parse import parse_context_file, split_front_matter
from dag_healer.context.read import CREDENTIAL_ENVS
from dag_healer.context.starter import SKIPPED, WRITTEN
from dag_healer.repo.profile import snapshot_hashes

EXAMPLE_REPO = Path(__file__).resolve().parents[2] / "example_existing_repo"
TODAY = date(2026, 10, 28)
ORG_PATH = "healer/organization.md"
IDENTITY = ("-c", "user.name=Test", "-c", "user.email=test@example.invalid", "-c", "commit.gpgsign=false")
ORG_RULES = (
    "- ORG-1: Personal data is masked everywhere.\n"
    "- ORG-2: A wrong number in a report is worse than a late one.\n"
    "- ORG-3: A fix is the smallest change that makes the failing test pass.\n"
)
#: The DAGs of example_existing_repo and the rule prefixes of their starter ETL Information.
STARTERS = {"customers_sync": "CUS", "inventory_report": "INV", "sales_daily_etl": "SAL"}
HINT = "dag-healer init --context writes starter files"
COMMIT_HINT = (
    "               Answer the questions in the starter files, then commit them to {branch}: dag-healer "
    "reads the committed files only."
)


@pytest.fixture(autouse=True)
def isolated_git(monkeypatch: pytest.MonkeyPatch, tmp_path_factory: pytest.TempPathFactory) -> None:
    """No git configuration of the host and no credential variable of the environment."""
    config = tmp_path_factory.mktemp("gitconfig") / "gitconfig"
    config.write_text("", encoding="utf-8")
    monkeypatch.setenv("GIT_CONFIG_GLOBAL", str(config))
    monkeypatch.setenv("GIT_CONFIG_NOSYSTEM", "1")
    for name in ("GIT_DIR", "GIT_WORK_TREE", "GIT_INDEX_FILE", *CREDENTIAL_ENVS):
        monkeypatch.delenv(name, raising=False)


@pytest.fixture(autouse=True)
def fixed_today(monkeypatch: pytest.MonkeyPatch) -> None:
    """``doctor`` reads with the current day: here it is TODAY, unless a reading passes a day."""
    monkeypatch.setattr("dag_healer.context.read._today", lambda today: today or TODAY)


@pytest.fixture
def isolated(healer_env: Path, monkeypatch: pytest.MonkeyPatch) -> Iterator[Path]:
    """healer_env (HEALER_HOME in tmp_path), and HEALER_CONFIG undone after ``main(["-c", ...])``.

    ``init`` and ``doctor`` open the store, so its engines are disposed afterwards.
    """
    from dag_healer.store import dispose_engines

    monkeypatch.setenv("HEALER_CONFIG", "")
    monkeypatch.delenv("HEALER_CONFIG")
    yield healer_env
    dispose_engines()


# --------------------------------------------------------------------------- helpers


def _git(repo: Path, *args: str) -> str:
    done = subprocess.run(["git", *IDENTITY, *args], cwd=repo, capture_output=True, text=True, check=True)
    return done.stdout.strip()


def _repo(path: Path, branch: str = "main") -> Path:
    path.mkdir(parents=True, exist_ok=True)
    _git(path, "init", "-q", "-b", branch)
    return path


def _write(root: Path, files: Mapping[str, str | bytes]) -> None:
    for relative, content in files.items():
        target = root / relative
        target.parent.mkdir(parents=True, exist_ok=True)
        if isinstance(content, bytes):
            target.write_bytes(content)
        else:
            target.write_text(content, encoding="utf-8", newline="\n")


def _commit(repo: Path, files: Mapping[str, str | bytes] | None = None) -> str:
    """Write ``files``, commit everything and return the sha of the new commit."""
    _write(repo, files or {})
    _git(repo, "add", "-A")
    _git(repo, "commit", "-q", "-m", "commit")
    return _git(repo, "rev-parse", "HEAD")


def _cfg(repo: Path, **sections: Any) -> HealerConfig:
    return config_from_mapping({"repo": {"path": str(repo)}, **sections})


def _org(rules: str = ORG_RULES, *, reviewed: str = "2026-10-01", every: str = "1y") -> str:
    return (
        f'---\nowner: "@data-eng"\nreviewed: {reviewed}\nreview_every: {every}\n---\n\n'
        f"# Organization Context\n\n{rules}"
    )


def _etl(pipeline: str, prefix: str, rules: str) -> str:
    return (
        f'---\npipeline: {pipeline}\nowner: "@data-eng"\nreviewed: 2026-10-01\nreview_every: 1y\n'
        f"rule_prefix: {prefix}\n---\n\n# {pipeline}\n\n{rules}"
    )


def _sales(rules: str = "- SAL-1: Amounts are whole cents.\n- SAL-2: Refunds are left out.\n") -> str:
    return _etl("sales", "SAL", rules)


def _line_of(text: str, needle: str) -> int:
    """The 1-based number of the first line of ``text`` that holds ``needle``."""
    return next(number for number, line in enumerate(text.split("\n"), start=1) if needle in line)


def _padded(text: str, size: int) -> str:
    """``text`` with a last line that makes it ``size`` bytes long."""
    return text + "x" * (size - len(text.encode("utf-8")) - 1) + "\n"


# --------------------------------------------------------------------------- doctor: skipped


def test_doctor_skips_the_context_of_a_repository_without_context_files(tmp_path: Path) -> None:
    repo = _repo(tmp_path / "repo")
    sha = _commit(repo, {"README.md": "The daily jobs.\n"})

    result = check_context(_cfg(repo))

    assert (result.name, result.status) == ("context", SKIP)
    assert result.detail == (
        f"main@{sha[:7]}: no Organization Context (healer/organization.md) and no ETL Information in "
        f"healer/etl/ yet; {HINT}"
    )


@pytest.mark.parametrize("with_git", [False, True], ids=["no-git", "no-commit"])
def test_doctor_skips_the_context_of_a_folder_without_a_commit_and_without_context_files(
    tmp_path: Path, with_git: bool
) -> None:
    root = tmp_path / "repo"
    root.mkdir()
    if with_git:
        _git(root, "init", "-q", "-b", "main")
    cfg = _cfg(root)

    result = check_context(cfg)

    problem = (
        "the repository has no commit of main" if with_git else f"{cfg.repo_root} is not a git repository"
    )
    assert (result.status, result.detail) == (SKIP, f"no context files ({problem}); {HINT}")


# --------------------------------------------------------------------------- doctor: ok


def test_doctor_names_the_commit_the_files_and_their_rules(tmp_path: Path) -> None:
    repo = _repo(tmp_path / "repo")
    sha = _commit(
        repo,
        {
            ORG_PATH: _org(),
            "healer/etl/sales.md": _sales(),
            "healer/etl/stock.md": _etl("stock", "STO", "- STO-1: Stock is counted per SKU.\n"),
        },
    )

    result = check_context(_cfg(repo))

    assert result.status == OK
    assert result.detail == (
        f"main@{sha[:7]}: healer/organization.md (3 rules); 2 of 2 ETL Information file(s) in healer/etl/ "
        "(3 rules)"
    )


def test_doctor_counts_one_rule_and_a_missing_organization_context(tmp_path: Path) -> None:
    repo = _repo(tmp_path / "repo")
    sha = _commit(repo, {"healer/etl/sales.md": _sales("- SAL-1: Amounts are whole cents.\n")})

    result = check_context(_cfg(repo))

    assert result.status == OK
    assert result.detail == (
        f"main@{sha[:7]}: no Organization Context (healer/organization.md); 1 of 1 ETL Information file(s) "
        "in healer/etl/ (1 rule)"
    )


# --------------------------------------------------------------------------- doctor: warnings


def test_doctor_warns_when_the_base_branch_and_its_origin_copy_diverged(tmp_path: Path) -> None:
    repo = _repo(tmp_path / "repo")
    _commit(repo, {"healer/organization.md": _org()})
    _git(repo, "checkout", "-q", "-b", "other")
    remote = _commit(repo, {"healer/organization.md": _org("- ORG-1: Only on origin/main.\n")})
    _git(repo, "checkout", "-q", "main")
    local = _commit(repo, {"healer/organization.md": _org("- ORG-1: Only on main.\n")})
    _git(repo, "update-ref", "refs/remotes/origin/main", remote)

    result = check_context(_cfg(repo))

    assert result.status == WARN
    assert result.detail == (
        f"main@{local[:7]}: healer/organization.md (1 rule); no ETL Information in healer/etl/; "
        f"main ({local[:7]}) and origin/main ({remote[:7]}) have diverged, read from main"
    )


def test_doctor_escapes_the_control_characters_of_what_the_repository_holds(tmp_path: Path) -> None:
    repo = _repo(tmp_path / "repo")
    name = "sales\x1b]8;;evil\x07x\x1b[2K.md"  # a terminal hyperlink and a line erase
    try:
        _commit(
            repo,
            {
                "healer/organization.md": _org(
                    "- ORG-1 (\x1b]8;;https://evil.example/\x07click\x1b]8;;\x07 \x1b[2K): text\n"
                ),
                f"healer/etl/{name}": _sales(),
            },
        )
    except (OSError, subprocess.CalledProcessError):
        pytest.skip("this file system does not take control characters in file names")

    shown = format_results([check_context(_cfg(repo))])

    assert "\x1b" not in shown and "\x07" not in shown
    assert "healer/etl/sales\\x1b]8;;evil\\x07x\\x1b[2K.md (its name is not a pipeline id)" in shown


def test_doctor_warns_about_an_invalid_file_and_shows_its_first_problem(tmp_path: Path) -> None:
    repo = _repo(tmp_path / "repo")
    etl_text = _sales("- SAL1: Amounts are whole cents.\n- sal-2: Refunds are left out.\n- SAL-3: Unique.\n")
    sha = _commit(repo, {ORG_PATH: _org(), "healer/etl/sales.md": etl_text})

    result = check_context(_cfg(repo))

    first = f"line {_line_of(etl_text, 'SAL1')}: SAL1 is not a rule ID; write it as SAL-1"
    assert result.status == WARN
    assert result.detail == (
        f"main@{sha[:7]}: healer/organization.md (3 rules); 1 of 1 ETL Information file(s) in healer/etl/ "
        f"(1 rule); invalid: healer/etl/sales.md (2: {first})"
    )


def test_doctor_warns_about_a_file_overdue_for_a_review(tmp_path: Path) -> None:
    repo = _repo(tmp_path / "repo")
    sha = _commit(repo, {ORG_PATH: _org(reviewed="2026-01-01", every="90d")})

    result = check_context(_cfg(repo))

    assert result.status == WARN
    assert result.detail == (
        f"main@{sha[:7]}: healer/organization.md (3 rules); no ETL Information in healer/etl/; overdue for "
        "a review: healer/organization.md (since 2026-04-01)"
    )


def test_doctor_warns_about_files_refused_for_their_size_or_encoding(tmp_path: Path) -> None:
    repo = _repo(tmp_path / "repo")
    sha = _commit(
        repo,
        {
            ORG_PATH: _org(),
            "healer/etl/sales.md": _padded(_sales(), 2000),
            "healer/etl/stock.md": _etl("stock", "STO", "- STO-1: Caf").encode()
            + "\xe9.\n".encode("latin-1"),
        },
    )

    result = check_context(_cfg(repo, context={"max_file_bytes": 1024}))

    assert result.status == WARN
    assert result.detail == (
        f"main@{sha[:7]}: healer/organization.md (3 rules); 0 of 2 ETL Information file(s) in healer/etl/ "
        "(0 rules); refused: healer/etl/sales.md (it is 2,000 bytes, over the 1,024-byte limit "
        "(context.max_file_bytes); split it or make it shorter), healer/etl/stock.md (it is not UTF-8 text)"
    )


def test_doctor_warns_about_an_etl_information_that_is_not_committed(tmp_path: Path) -> None:
    repo = _repo(tmp_path / "repo")
    sha = _commit(repo, {ORG_PATH: _org()})
    _write(repo, {"healer/etl/sales.md": _sales()})

    result = check_context(_cfg(repo))

    assert result.status == WARN
    assert result.detail == (
        f"main@{sha[:7]}: healer/organization.md (3 rules); no ETL Information in healer/etl/; not "
        "committed, so not read: healer/etl/sales.md"
    )


def test_doctor_warns_about_context_files_that_are_all_not_committed(tmp_path: Path) -> None:
    repo = _repo(tmp_path / "repo")
    sha = _commit(repo, {"README.md": "The daily jobs.\n"})
    _write(repo, {ORG_PATH: _org(), "healer/etl/sales.md": _sales()})

    result = check_context(_cfg(repo))

    assert result.status == WARN
    assert result.detail == (
        f"main@{sha[:7]}: no Organization Context (healer/organization.md); no ETL Information in "
        "healer/etl/; not committed, so not read: healer/organization.md, healer/etl/sales.md"
    )


def test_doctor_warns_about_an_edit_that_is_not_committed(tmp_path: Path) -> None:
    repo = _repo(tmp_path / "repo")
    sha = _commit(repo, {ORG_PATH: _org(), "healer/etl/sales.md": _sales()})
    _write(repo, {ORG_PATH: _org(ORG_RULES + "- ORG-4: Edited, not committed.\n")})

    result = check_context(_cfg(repo))

    assert result.status == WARN
    assert result.detail == (
        f"main@{sha[:7]}: healer/organization.md (3 rules); 1 of 1 ETL Information file(s) in healer/etl/ "
        "(2 rules); edited but not committed (the committed version is read): healer/organization.md"
    )


@pytest.mark.parametrize(
    ("with_git", "files"),
    [(False, {ORG_PATH: _org()}), (True, {"healer/etl/sales.md": _sales()})],
    ids=["no-git", "no-commit"],
)
def test_doctor_warns_about_context_files_without_a_commit_to_read_them_from(
    tmp_path: Path, with_git: bool, files: dict[str, str]
) -> None:
    root = tmp_path / "repo"
    root.mkdir()
    if with_git:
        _git(root, "init", "-q", "-b", "main")
    _write(root, files)
    cfg = _cfg(root)

    result = check_context(cfg)

    problem = (
        "the repository has no commit of main" if with_git else f"{cfg.repo_root} is not a git repository"
    )
    assert (result.status, result.detail) == (
        WARN,
        f"not read: {problem}, and the context files are read from the base branch's commit",
    )


def test_doctor_says_when_the_base_branch_is_not_found(tmp_path: Path) -> None:
    repo = _repo(tmp_path / "repo", branch="trunk")
    sha = _commit(repo, {ORG_PATH: _org()})

    result = check_context(_cfg(repo))

    assert result.detail == (
        f"HEAD@{sha[:7]}: healer/organization.md (3 rules); no ETL Information in healer/etl/; "
        "repo.base_branch main not found: read from HEAD"
    )


def test_doctor_warns_when_the_base_branch_is_not_found(tmp_path: Path) -> None:
    repo = _repo(tmp_path / "repo", branch="trunk")
    _commit(repo, {ORG_PATH: _org()})

    assert check_context(_cfg(repo)).status == WARN


# --------------------------------------------------------------------------- doctor: errors


def test_doctor_fails_on_a_context_file_holding_a_secret(tmp_path: Path) -> None:
    repo = _repo(tmp_path / "repo")
    token = "ghp_" + "a1B2" * 9
    org_text = _org(ORG_RULES + f"- ORG-4: The deploy job pushes with {token}.\n")
    sha = _commit(repo, {ORG_PATH: org_text, "healer/etl/sales.md": _sales()})

    result = check_context(_cfg(repo))

    assert result.status == ERROR
    assert result.detail == (
        f"main@{sha[:7]}: 1 of 1 ETL Information file(s) in healer/etl/ (2 rules); refused, they hold what "
        "looks like a secret (remove it from the history and rotate it): healer/organization.md (it holds "
        f"what looks like a GitHub token on line {_line_of(org_text, token)}; remove it from the file (and "
        "rotate it if it is real))"
    )
    assert token not in result.detail


# --------------------------------------------------------------------------- the doctor command


def _doctor(tmp_path: Path, repo: Path, capsys: pytest.CaptureFixture[str]) -> tuple[int, str, str]:
    """``dag-healer -c <healer.yaml> doctor`` with the fake LLM and orchestrator and the process sandbox."""
    config = tmp_path / "healer.yaml"
    settings = {
        "llm": {"provider": "fake"},
        "orchestrator": {"type": "fake"},
        "repo": {"path": str(repo), "sandbox": {"backend": "process"}},
    }
    config.write_text(yaml.safe_dump(settings), encoding="utf-8")
    code = main(["-c", str(config), "doctor"])
    captured = capsys.readouterr()
    return code, captured.out, captured.err


def _line(out: str, name: str) -> str:
    return next(line for line in out.splitlines() if line[7:].startswith(f"{name} "))


def test_the_doctor_command_prints_the_context_line(
    tmp_path: Path, isolated: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    repo = _repo(tmp_path / "repo")
    sha = _commit(repo, {"dags/.gitkeep": "", ORG_PATH: _org(), "healer/etl/sales.md": _sales()})

    code, out, _err = _doctor(tmp_path, repo, capsys)

    line = _line(out, "context")
    assert code == 0, out
    assert line.startswith("[ OK ] context ")
    assert line.endswith(
        f"  main@{sha[:7]}: healer/organization.md (3 rules); 1 of 1 ETL Information file(s) in healer/etl/ "
        "(2 rules)"
    )


def test_the_doctor_command_fails_on_a_secret_and_never_prints_it(
    tmp_path: Path, isolated: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    repo = _repo(tmp_path / "repo")
    key = "AKIA" + "X" * 16
    org_text = _org(ORG_RULES + f"- ORG-4: The export reads the bucket with {key}.\n")
    _commit(repo, {"dags/.gitkeep": "", ORG_PATH: org_text, "healer/etl/sales.md": _sales()})

    code, out, err = _doctor(tmp_path, repo, capsys)

    line = _line(out, "context")
    assert code == 1
    assert line.startswith("[FAIL] context ")
    assert f"an AWS access key ID on line {_line_of(org_text, key)}" in line
    assert [item for item in out.splitlines() if item.startswith("[FAIL]")] == [line]  # the only error
    assert key not in out and key not in err


# --------------------------------------------------------------------------- init --context


def _copy_example(tmp_path: Path, *, context_files: bool) -> Path:
    """A copy of example_existing_repo, with or without its folder ``healer/`` (its context files)."""
    target = tmp_path / "shop repo"
    shutil.copytree(EXAMPLE_REPO, target, ignore=shutil.ignore_patterns("__pycache__", "processed"))
    if context_files:
        assert (target / ORG_PATH).is_file()
    else:
        shutil.rmtree(target / "healer", ignore_errors=True)
    return target


def _init(repo: Path, capsys: pytest.CaptureFixture[str], *extra: str) -> str:
    assert main(["init", "--repo", str(repo), *extra]) == 0
    return capsys.readouterr().out


def _context_lines(out: str) -> list[str]:
    """The ``Context:`` lines of an init report (up to its ``Code files:`` line)."""
    lines = out.splitlines()
    start = next(index for index, line in enumerate(lines) if line.startswith("Context:"))
    end = next(index for index, line in enumerate(lines) if line.startswith("Code files:"))
    return lines[start:end]


def _front_matter(path: Path) -> Mapping[str, Any]:
    data, _body, _line, problems = split_front_matter(path.read_text(encoding="utf-8"))
    assert data is not None and problems == []
    return data


def _starter_paths(etl_dir: str = "healer/etl") -> list[str]:
    return [f"{etl_dir}/{pipeline}.md" for pipeline in STARTERS]


def _utc_today() -> date:
    return datetime.now(timezone.utc).date()


def test_init_context_writes_the_starter_files(
    tmp_path: Path, isolated: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    repo = _copy_example(tmp_path, context_files=False)
    python_files = snapshot_hashes(repo, ("**/*.py",))
    before = _utc_today()

    out = _init(repo, capsys, "--context")

    after = _utc_today()
    assert _context_lines(out) == [
        "Context:       healer/organization.md: written",
        "               healer/etl/customers_sync.md: written (rules CUS-1, CUS-2, ...)",
        "               healer/etl/inventory_report.md: written (rules INV-1, INV-2, ...)",
        "               healer/etl/sales_daily_etl.md: written (rules SAL-1, SAL-2, ...)",
        COMMIT_HINT.format(branch="main"),
    ]
    organization = _front_matter(repo / ORG_PATH)
    assert (organization["owner"], organization["review_every"]) == ("@your-team", "90d")
    assert before <= organization["reviewed"] <= after
    for pipeline, prefix in STARTERS.items():
        front = _front_matter(repo / "healer" / "etl" / f"{pipeline}.md")
        assert (front["pipeline"], front["rule_prefix"], front["review_every"]) == (pipeline, prefix, "90d")
    assert sorted(path.name for path in (repo / "healer" / "etl").iterdir()) == [
        f"{pipeline}.md" for pipeline in sorted(STARTERS)
    ]
    assert snapshot_hashes(repo, ("**/*.py",)) == python_files
    assert f"Code files:    unchanged ({len(python_files)} Python files verified)" in out


def test_init_context_starter_files_read_as_valid_context_files_once_committed(
    tmp_path: Path, isolated: Path, capsys: pytest.CaptureFixture[str], monkeypatch: pytest.MonkeyPatch
) -> None:
    repo = _copy_example(tmp_path, context_files=False)
    _init(repo, capsys, "--context")
    _git(repo, "init", "-q", "-b", "main")
    sha = _commit(repo)
    reviewed = _front_matter(repo / ORG_PATH)["reviewed"]
    cfg = load_config_file(repo / "healer.yaml")

    report = read_all(cfg, today=reviewed)

    assert report.organization is not None and report.organization.valid
    assert report.organization.rules == []  # the example rules are comments
    assert [(item.pipeline, item.rule_prefix, item.valid, item.rules) for item in report.etl] == [
        (pipeline, prefix, True, []) for pipeline, prefix in STARTERS.items()
    ]
    snapshot = read_context(cfg, "sales_daily_etl", today=reviewed)
    for item in snapshot.files():
        assert item.is_read and "<!--" not in item.text and "-1:" not in item.text  # questions stay out
    assert snapshot.etl is not None and "Builds on the Organization Context" in snapshot.etl.text
    monkeypatch.setattr("dag_healer.context.read._today", lambda today: today or reviewed)
    result = check_context(cfg)
    assert (result.status, result.detail) == (
        OK,
        f"main@{sha[:7]}: healer/organization.md (0 rules); 3 of 3 ETL Information file(s) in healer/etl/ "
        "(0 rules)",
    )


def test_init_context_twice_keeps_every_file_as_it_is(
    tmp_path: Path, isolated: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    repo = _copy_example(tmp_path, context_files=False)
    _init(repo, capsys, "--context")
    organization = repo / ORG_PATH
    organization.write_text(
        organization.read_text(encoding="utf-8") + "\n- ORG-1: Our first rule.\n", encoding="utf-8"
    )
    customers = repo / "healer" / "etl" / "customers_sync.md"
    text = customers.read_text(encoding="utf-8")
    assert text.count('rule_prefix: "CUS" ') == 1
    customers.write_text(text.replace('rule_prefix: "CUS" ', 'rule_prefix: "CRM" '), encoding="utf-8")
    paths = [ORG_PATH, *_starter_paths()]
    before = {path: (repo / path).read_bytes() for path in paths}

    out = _init(repo, capsys, "--context")

    assert {path: (repo / path).read_bytes() for path in paths} == before
    assert _context_lines(out) == [
        "Context:       healer/organization.md: kept (the file exists)",
        "               healer/etl/customers_sync.md: kept (the file exists) (rules CRM-1, CRM-2, ...)",
        "               healer/etl/inventory_report.md: kept (the file exists) (rules INV-1, INV-2, ...)",
        "               healer/etl/sales_daily_etl.md: kept (the file exists) (rules SAL-1, SAL-2, ...)",
    ]


def test_init_context_never_overwrites_the_context_files_of_the_repository(
    tmp_path: Path, isolated: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    repo = _copy_example(tmp_path, context_files=True)
    before = snapshot_hashes(repo, ("**/*",))
    assert {path for path in before if path.startswith("healer/")} == {ORG_PATH, *_starter_paths()}

    out = _init(repo, capsys, "--context", "--force")

    after = snapshot_hashes(repo, ("**/*",))
    assert {path: after[path] for path in before} == before
    assert sorted(set(after) - set(before)) == [
        "healer.yaml",
        *(f"healer/contracts/{pipeline}.yaml" for pipeline in sorted(STARTERS)),
    ]
    assert _context_lines(out) == [
        "Context:       healer/organization.md: kept (the file exists)",
        "               healer/etl/customers_sync.md: kept (the file exists) (rules CUS-1, CUS-2, ...)",
        "               healer/etl/inventory_report.md: kept (the file exists) (rules INV-1, INV-2, ...)",
        "               healer/etl/sales_daily_etl.md: kept (the file exists) (rules SAL-1, SAL-2, ...)",
    ]
    assert "Code files:    unchanged (" in out


def test_init_context_never_gives_a_new_file_the_prefix_of_an_existing_one(
    tmp_path: Path, isolated: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    repo = _copy_example(tmp_path, context_files=False)
    _write(repo, {"healer/etl/customers_sync.md": _etl("customers_sync", "SAL", "- SAL-1: Taken.\n")})

    lines = _context_lines(_init(repo, capsys, "--context"))

    assert (
        "               healer/etl/customers_sync.md: kept (the file exists) (rules SAL-1, SAL-2, ...)"
        in lines
    )
    assert "               healer/etl/sales_daily_etl.md: written (rules SDE-1, SDE-2, ...)" in lines
    assert _front_matter(repo / "healer" / "etl" / "sales_daily_etl.md")["rule_prefix"] == "SDE"


def test_init_without_context_writes_no_context_file(
    tmp_path: Path, isolated: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    repo = _copy_example(tmp_path, context_files=False)

    out = _init(repo, capsys)

    assert not (repo / ORG_PATH).exists() and not (repo / "healer" / "etl").exists()
    assert "Context:" not in out and "Answer the questions" not in out
    assert "Code files:    unchanged (" in out


def test_init_context_writes_where_the_context_section_of_healer_yaml_says(
    tmp_path: Path, isolated: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    repo = _copy_example(tmp_path, context_files=False)
    config = repo / "healer.yaml"
    config.write_text(
        "version: 1\n"
        "repo: {path: ., base_branch: develop}\n"
        "context:\n"
        "  organization: docs/knowledge/organization.md\n"
        "  etl_dir: docs/knowledge/pipelines\n",
        encoding="utf-8",
    )
    config_bytes = config.read_bytes()

    out = _init(repo, capsys, "--context")

    assert config.read_bytes() == config_bytes
    assert not (repo / ORG_PATH).exists() and not (repo / "healer" / "etl").exists()
    assert _context_lines(out) == [
        "Context:       docs/knowledge/organization.md: written",
        "               docs/knowledge/pipelines/customers_sync.md: written (rules CUS-1, CUS-2, ...)",
        "               docs/knowledge/pipelines/inventory_report.md: written (rules INV-1, INV-2, ...)",
        "               docs/knowledge/pipelines/sales_daily_etl.md: written (rules SAL-1, SAL-2, ...)",
        COMMIT_HINT.format(branch="develop"),
    ]
    organization = (repo / "docs" / "knowledge" / "organization.md").read_text(encoding="utf-8")
    assert "(docs/knowledge/pipelines/<pipeline>.md)" in organization
    for path in _starter_paths("docs/knowledge/pipelines"):
        text = (repo / path).read_text(encoding="utf-8")
        assert "Builds on the Organization Context (docs/knowledge/organization.md)." in text


def test_write_context_starters_skips_a_pipeline_id_that_cannot_name_a_file(tmp_path: Path) -> None:
    outcomes = write_context_starters(_cfg(tmp_path), tmp_path, ["sales", "../escape", ".hidden", "sales"])

    assert [(item.path, item.outcome, item.rule_prefix) for item in outcomes] == [
        (ORG_PATH, WRITTEN, None),
        ("healer/etl/sales.md", WRITTEN, "SAL"),
        ("healer/etl/../escape.md", SKIPPED, None),
        ("healer/etl/.hidden.md", SKIPPED, None),
    ]
    assert sorted(path.relative_to(tmp_path).as_posix() for path in tmp_path.rglob("*.md")) == [
        "healer/etl/sales.md",
        ORG_PATH,
    ]
    text = (tmp_path / "healer" / "etl" / "sales.md").read_text(encoding="utf-8")
    assert parse_context_file(text, kind="etl", pipeline="sales").problems == []


@pytest.mark.parametrize("pipeline", ["2024_01", "off_hours_load"])
def test_a_starter_etl_information_is_valid_for_any_pipeline_id(tmp_path: Path, pipeline: str) -> None:
    _organization, starter = write_context_starters(_cfg(tmp_path), tmp_path, [pipeline])
    assert starter.outcome == WRITTEN and starter.rule_prefix

    text = (tmp_path / starter.path).read_text(encoding="utf-8")
    parsed = parse_context_file(
        text + f"\n- {starter.rule_prefix}-1: A rule of the team.\n", kind="etl", pipeline=pipeline
    )

    assert parsed.problems == []
    assert [rule.id for rule in parsed.rules] == [f"{starter.rule_prefix}-1"]


def test_doctor_names_a_context_file_it_cannot_read_apart_from_the_refused_ones(tmp_path: Path) -> None:
    repo = _repo(tmp_path / "repo")
    sha = _commit(repo, {f"{ORG_PATH}/readme.txt": "a folder, not a file\n", "healer/etl/sales.md": _sales()})

    result = check_context(_cfg(repo))

    assert result.status == WARN
    assert result.detail == (
        f"main@{sha[:7]}: 1 of 1 ETL Information file(s) in healer/etl/ (2 rules); "
        f"unreadable: {ORG_PATH} (it is a folder, not a file)"
    )
