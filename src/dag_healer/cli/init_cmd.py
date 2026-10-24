"""``dag-healer init``: discover DAGs, profile their datasets and write draft contracts.

What it does, in order:

1. snapshots the sha256 of every ``*.py`` file of the repository;
2. scans the repository statically with the code locator of the configured orchestrator
   (``adapter.code.scan``; for Airflow :func:`dag_healer.repo.dagscan.scan_dags` of the DAG
   folder, ``--dags-dir`` included; user code is never imported) and detects the test layout
   (:func:`dag_healer.repo.profile.detect_test_layout`);
3. for every DAG with resolvable datasets, loads them (from ``--samples DIR`` when given,
   matched by relative path or file name) and infers a **draft** contract written to
   ``<contracts dir>/<dag_id>.yaml``. Approved contracts are never touched; existing drafts
   are only replaced with ``--force``;
4. writes ``healer.yaml`` (with the detected ``tests_dir`` and the discovered task datasets)
   only when it does not exist yet;
5. snapshots the ``*.py`` files again and fails loudly if anything changed: ``init`` must
   never modify DAGs or code;
6. (the command only: :func:`run_init` never opens the store) creates the incident store, or
   migrates one written by an older dag-healer (:func:`prepare_store`). A store that cannot be
   prepared is only reported: the worker creates or migrates it when it starts.
"""

from __future__ import annotations

import argparse
import contextlib
import logging
import os
import re
import sys
import tempfile
from dataclasses import dataclass, field
from pathlib import Path
from typing import TYPE_CHECKING

from dag_healer.cli._output import add_config_option, config_arg, error, mask_url, render_table
from dag_healer.cli.main import register_command

if TYPE_CHECKING:
    from dag_healer.config import HealerConfig
    from dag_healer.contracts.schema import DatasetContract, DatasetLocator
    from dag_healer.repo.dagscan import DagInfo, DatasetRef
    from dag_healer.repo.profile import TestLayout

log = logging.getLogger(__name__)

FINAL_NOTE = (
    "Contracts are drafts: nothing is enforced until a human approves them "
    "(dag-healer contracts approve <dag_id>)"
)
NO_TESTS_NOTE = "no test suite found; dag-healer will create tests/ when it proposes the first fix"

WRITTEN = "written"
OVERWRITTEN = "overwritten (--force)"
KEPT_APPROVED = "kept (approved)"
KEPT_DRAFT = "kept (draft exists; --force overwrites it)"
KEPT_INVALID = "kept (existing file is invalid)"
NO_DATASETS = "skipped (no datasets found)"
NO_READABLE = "skipped (no readable datasets)"

_NAME_CLEAN = re.compile(r"[^A-Za-z0-9_.-]+")


class InitError(Exception):
    """``init`` cannot run: bad repository path, invalid configuration, ..."""


@dataclass
class DatasetCandidate:
    """A dataset to profile for a DAG (deduplicated by path)."""

    task_id: str | None
    locator: DatasetLocator
    source: str


@dataclass
class DagReport:
    """What ``init`` found and did for one DAG."""

    dag_id: str
    file: str
    tasks: int
    datasets_found: int
    datasets_unresolved: int
    contract: str = ""
    contract_path: Path | None = None
    profiled: list[str] = field(default_factory=list)
    problems: list[str] = field(default_factory=list)


@dataclass
class InitResult:
    """Outcome of :func:`run_init`."""

    repo_root: Path
    dags_dir: str
    config_path: Path
    config_written: bool
    contracts_dir: Path
    layout: TestLayout
    dags: list[DagReport]
    changed_files: list[str]
    python_files: int
    warnings: list[str] = field(default_factory=list)
    store: str | None = None  # set by the command (prepare_store), never by run_init

    @property
    def ok(self) -> bool:
        """True when no repository code file changed."""
        return not self.changed_files


# --------------------------------------------------------------------------- paths / config


def _as_config_file(path: Path) -> Path:
    return path / "healer.yaml" if path.is_dir() else path


def _target_config_path(repo: str | Path | None, config_path: str | Path | None) -> tuple[Path, str]:
    """Return the healer.yaml to read/write and where that choice came from."""
    if config_path:
        return _as_config_file(Path(config_path).expanduser()).resolve(), "option"
    env_value = os.environ.get("HEALER_CONFIG")
    if env_value:
        return _as_config_file(Path(env_value).expanduser()).resolve(), "env"
    base = Path(repo).expanduser() if repo is not None else Path.cwd()
    return (base / "healer.yaml").resolve(), "repo"


def _load_existing_config(path: Path) -> HealerConfig | None:
    """Load ``path`` when it exists (``None`` otherwise); invalid files raise InitError."""
    from dag_healer.config import ConfigError, load_config_file

    if not path.is_file():
        return None
    try:
        return load_config_file(path)
    except ConfigError as exc:
        raise InitError(str(exc)) from None


def _resolve_repo_root(
    repo: str | Path | None, cfg: HealerConfig | None, config_path: Path, origin: str, warnings: list[str]
) -> Path:
    if repo is not None:
        root = Path(repo).expanduser().resolve()
        if cfg is not None and cfg.source_path is not None and cfg.repo_root != root:
            warnings.append(
                f"{config_path} points repo.path to {cfg.repo_root}; using --repo {root} for this run"
            )
    elif cfg is not None:
        root = cfg.repo_root
    elif origin == "env":
        root = config_path.parent
    else:
        root = Path.cwd().resolve()
    if not root.is_dir():
        raise InitError(f"repository folder not found: {root}")
    return root


def _clean_dir(value: str) -> str:
    cleaned = value.strip().replace("\\", "/").rstrip("/")
    while cleaned.startswith("./"):
        cleaned = cleaned[2:]
    return cleaned or "."


def _contracts_dir(cfg: HealerConfig | None, repo_root: Path) -> Path:
    directory = Path(cfg.contracts.dir if cfg is not None else "healer/contracts").expanduser()
    return directory.resolve() if directory.is_absolute() else (repo_root / directory).resolve()


def _display_path(path: Path, repo_root: Path) -> str:
    """Repo-relative posix path when ``path`` is inside the repository, else absolute posix."""
    try:
        return path.resolve().relative_to(repo_root).as_posix()
    except ValueError:
        return path.resolve().as_posix()


def _relative_repo_path(repo_root: Path, base: Path) -> str:
    try:
        relative = os.path.relpath(repo_root, base)
    except ValueError:  # different drives on Windows
        return repo_root.as_posix()
    return Path(relative).as_posix()


# --------------------------------------------------------------------------- scan


def scan_repository(cfg: HealerConfig | None, repo_root: Path, dags_dir: str) -> list[DagInfo]:
    """Every pipeline of ``repo_root``, found by the configured orchestrator's code locator.

    The adapter is built for this scan only (no I/O) from ``cfg`` (the defaults without one)
    with ``repo.path = repo_root`` and ``repo.dags_dir = dags_dir``, and closed afterwards. For
    Airflow this is exactly ``scan_dags(repo_root, dags_dir)``. Raises :class:`InitError` when
    the adapter cannot be loaded or built, or the scan fails.
    """
    from dag_healer.config import HealerConfig
    from dag_healer.core.errors import OrchestratorError
    from dag_healer.orchestrators import registry

    base = cfg if cfg is not None else HealerConfig()
    scan_cfg = base.model_copy(
        update={"repo": base.repo.model_copy(update={"path": repo_root, "dags_dir": dags_dir})}
    )
    try:
        settings = registry.orchestrator_settings(scan_cfg)
        factory = registry.load_factory(settings.type)
        adapter = registry.ensure_adapter(factory.create(scan_cfg), settings.type)
    except registry.AdapterLoadError as exc:
        raise InitError(str(exc)) from None
    except Exception as exc:  # a third-party factory that fails in its own way
        from dag_healer.masking import mask_text

        reason = mask_text(f"{type(exc).__name__}: {exc}")[:500]
        raise InitError(f"the orchestrator adapter cannot be built: {reason}") from None
    try:
        return list(adapter.code.scan(repo_root))
    except OrchestratorError as exc:
        raise InitError(f"{adapter.display_name} could not scan {repo_root}: {exc}") from None
    finally:
        with contextlib.suppress(Exception):  # closing is best effort
            adapter.close()


# --------------------------------------------------------------------------- datasets


def _task_order(dag: DagInfo, task_id: str | None) -> int:
    order = {task.task_id: index for index, task in enumerate(dag.tasks)}
    if task_id is None:
        return len(order) + 1
    return order.get(task_id, len(order))


def dataset_candidates(
    dag: DagInfo, configured: dict[str, DatasetLocator] | None = None
) -> tuple[list[DatasetCandidate], list[DatasetRef]]:
    """Return the resolved datasets of ``dag`` (one per path) and its unresolved references.

    Locators configured in ``healer.yaml`` come first (they may carry reader options). A
    path referenced by several tasks is attributed to the earliest task of the DAG.
    """
    from dag_healer.contracts.schema import DatasetLocator

    by_path: dict[str, DatasetCandidate] = {}
    for task_id, locator in (configured or {}).items():
        by_path.setdefault(locator.path, DatasetCandidate(task_id, locator, "healer.yaml"))
    unresolved: dict[str, DatasetRef] = {}
    for ref in sorted(dag.datasets, key=lambda item: (_task_order(dag, item.task_id), item.path)):
        if not ref.resolved:
            unresolved.setdefault(ref.path, ref)
            continue
        if ref.path in by_path:
            continue
        try:
            locator = DatasetLocator(type=ref.type, path=ref.path)
        except ValueError as exc:
            log.debug("Ignoring dataset reference %s: %s", ref.path, exc)
            continue
        by_path[ref.path] = DatasetCandidate(ref.task_id, locator, ref.source)
    remaining = [ref for path, ref in unresolved.items() if path not in by_path]
    return list(by_path.values()), remaining


def _sample_locator(
    locator: DatasetLocator, repo_root: Path, samples_dir: Path | None
) -> tuple[DatasetLocator, str, str | None]:
    """Return the locator to read, how to display its source, and an optional note."""
    from dag_healer.contracts.schema import DatasetLocator

    if samples_dir is None:
        return locator, locator.path, None
    relative = Path(locator.path)
    options = [] if relative.is_absolute() else [samples_dir / relative]
    options.append(samples_dir / relative.name)
    for candidate in options:
        if candidate.exists():
            sample = DatasetLocator(
                type=locator.type, path=candidate.resolve().as_posix(), options=locator.options
            )
            return sample, _display_path(candidate, repo_root), None
    note = f"{relative.name} not found in --samples {samples_dir}; profiled {locator.path} instead"
    return locator, locator.path, note


def _unique_name(path: str, used: set[str]) -> str:
    stem = _NAME_CLEAN.sub("_", Path(path).stem).strip("_") or "dataset"
    name, counter = stem, 2
    while name in used:
        name, counter = f"{stem}_{counter}", counter + 1
    used.add(name)
    return name


def profile_datasets(
    candidates: list[DatasetCandidate],
    *,
    repo_root: Path,
    samples_dir: Path | None,
    pii_columns: list[str],
    report: DagReport,
) -> tuple[list[DatasetContract], list[str]]:
    """Load each candidate and infer its dataset contract; returns contracts and sources."""
    from dag_healer.contracts.infer import infer_dataset_contract
    from dag_healer.datasets import DatasetError, load_dataset

    contracts: list[DatasetContract] = []
    sources: list[str] = []
    used: set[str] = set()
    for candidate in candidates:
        read_locator, display, note = _sample_locator(candidate.locator, repo_root, samples_dir)
        if note:
            report.problems.append(note)
        try:
            frame = load_dataset(read_locator, repo_root)
        except DatasetError as exc:
            report.problems.append(f"dataset not profiled: {exc}")
            continue
        name = _unique_name(candidate.locator.path, used)
        contracts.append(
            infer_dataset_contract(
                frame,
                name=name,
                task_id=candidate.task_id,
                locator=candidate.locator,
                pii_columns=pii_columns,
            )
        )
        sources.append(f"{display} ({len(frame)} rows)")
        report.profiled.append(name)
    return contracts, sources


# --------------------------------------------------------------------------- contracts


def _existing_contract_action(path: Path, force: bool) -> str | None:
    """Return why an existing contract file is kept, or ``None`` when it may be written."""
    from dag_healer.contracts.io import load_contract
    from dag_healer.contracts.schema import ContractError

    if not path.exists():
        return None
    try:
        existing = load_contract(path)
    except ContractError:
        return KEPT_INVALID
    if existing.is_approved:
        return KEPT_APPROVED
    return None if force else KEPT_DRAFT


def _process_dag(
    dag: DagInfo,
    *,
    repo_root: Path,
    contracts_dir: Path,
    configured: dict[str, DatasetLocator],
    samples_dir: Path | None,
    pii_columns: list[str],
    force: bool,
) -> tuple[DagReport, dict[str, DatasetLocator]]:
    """Profile one DAG and write its draft contract; returns the report and task locators."""
    from dag_healer.contracts.infer import infer_contract
    from dag_healer.contracts.io import contract_path, save_contract

    candidates, unresolved = dataset_candidates(dag, configured)
    report = DagReport(
        dag_id=dag.dag_id,
        file=dag.file,
        tasks=len(dag.tasks),
        datasets_found=len(candidates),
        datasets_unresolved=len(unresolved),
    )
    for ref in unresolved:
        report.problems.append(
            f"unresolved dataset {ref.path} ({ref.source}); add a locator under "
            f"datasets.{dag.dag_id}.<task_id> in healer.yaml"
        )
    task_locators: dict[str, DatasetLocator] = {}
    for candidate in candidates:
        if candidate.task_id and candidate.task_id not in task_locators:
            task_locators[candidate.task_id] = candidate.locator

    try:
        path = contract_path(contracts_dir, dag.dag_id)
    except ValueError as exc:
        report.contract = "skipped (invalid dag_id)"
        report.problems.append(str(exc))
        return report, task_locators
    report.contract_path = path
    if not candidates:
        report.contract = NO_DATASETS
        return report, task_locators
    kept = _existing_contract_action(path, force)
    if kept is not None:
        report.contract = kept
        return report, task_locators

    datasets, sources = profile_datasets(
        candidates, repo_root=repo_root, samples_dir=samples_dir, pii_columns=pii_columns, report=report
    )
    if not datasets:
        report.contract = NO_READABLE
        return report, task_locators
    existed = path.exists()
    save_contract(infer_contract(dag.dag_id, datasets, inferred_from=sources), path)
    report.contract = OVERWRITTEN if existed else WRITTEN
    return report, task_locators


# --------------------------------------------------------------------------- healer.yaml


def _write_text_atomic(target: Path, text: str) -> None:
    target.parent.mkdir(parents=True, exist_ok=True)
    fd, tmp_name = tempfile.mkstemp(prefix=f".{target.name}.", suffix=".tmp", dir=target.parent)
    try:
        with os.fdopen(fd, "w", encoding="utf-8", newline="\n") as handle:
            handle.write(text)
        os.replace(tmp_name, target)
    except BaseException:
        Path(tmp_name).unlink(missing_ok=True)
        raise


def write_config(
    target: Path,
    *,
    repo_root: Path,
    dags_dir: str,
    tests_dir: str | None,
    datasets: dict[str, dict[str, DatasetLocator]],
) -> None:
    """Render ``healer.yaml`` from the template, validate it and write it to ``target``."""
    import yaml

    from dag_healer.config import ConfigError, config_from_mapping, default_config_yaml

    text = default_config_yaml(
        repo_path=_relative_repo_path(repo_root, target.parent),
        dags_dir=dags_dir,
        tests_dir=tests_dir,
        datasets=datasets,
    )
    try:
        config_from_mapping(yaml.safe_load(text) or {}, base_dir=target.parent, source_path=target)
    except ConfigError as exc:  # a bug in the template, never the user's fault
        raise InitError(f"generated healer.yaml is invalid: {exc}") from None
    _write_text_atomic(target, text)


# --------------------------------------------------------------------------- driver


def _diff_snapshots(before: dict[str, str], after: dict[str, str]) -> list[str]:
    changes = [f"modified: {path}" for path in before if path in after and before[path] != after[path]]
    changes += [f"deleted: {path}" for path in before if path not in after]
    changes += [f"added: {path}" for path in after if path not in before]
    return sorted(changes, key=lambda item: item.split(": ", 1)[1])


def run_init(
    repo: str | Path | None = None,
    *,
    config_path: str | Path | None = None,
    samples: str | Path | None = None,
    force: bool = False,
    dags_dir: str | None = None,
) -> InitResult:
    """Run ``dag-healer init`` and return what it did (it prints nothing).

    Raises :class:`InitError` when the repository, DAG folder, samples folder, an existing
    ``healer.yaml`` or the configured orchestrator adapter is unusable.
    """
    from dag_healer.repo.profile import detect_test_layout, snapshot_hashes

    warnings: list[str] = []
    target_config, origin = _target_config_path(repo, config_path)
    cfg = _load_existing_config(target_config)
    repo_root = _resolve_repo_root(repo, cfg, target_config, origin, warnings)
    dags_folder = _clean_dir(dags_dir or (cfg.repo.dags_dir if cfg is not None else "dags"))
    if not (repo_root / dags_folder).is_dir():
        raise InitError(f"DAG folder not found: {repo_root / dags_folder} (use --dags-dir)")
    samples_dir = Path(samples).expanduser().resolve() if samples else None
    if samples_dir is not None and not samples_dir.is_dir():
        raise InitError(f"--samples folder not found: {samples_dir}")

    before = snapshot_hashes(repo_root, ("**/*.py",))
    dags = scan_repository(cfg, repo_root, dags_folder)
    layout = detect_test_layout(repo_root)
    contracts_dir = _contracts_dir(cfg, repo_root)
    configured = cfg.datasets if cfg is not None else {}
    pii_columns = list(cfg.masking.columns) if cfg is not None else []

    reports: list[DagReport] = []
    yaml_datasets: dict[str, dict[str, DatasetLocator]] = {}
    for dag in dags:
        report, task_locators = _process_dag(
            dag,
            repo_root=repo_root,
            contracts_dir=contracts_dir,
            configured=dict(configured.get(dag.dag_id, {})),
            samples_dir=samples_dir,
            pii_columns=pii_columns,
            force=force,
        )
        reports.append(report)
        if task_locators:
            yaml_datasets[dag.dag_id] = task_locators
    if not dags:
        warnings.append(f"no DAGs found under {repo_root / dags_folder}")

    config_written = False
    if not target_config.exists():
        write_config(
            target_config,
            repo_root=repo_root,
            dags_dir=dags_folder,
            tests_dir=layout.tests_dir,
            datasets=yaml_datasets,
        )
        config_written = True

    after = snapshot_hashes(repo_root, ("**/*.py",))
    return InitResult(
        repo_root=repo_root,
        dags_dir=dags_folder,
        config_path=target_config,
        config_written=config_written,
        contracts_dir=contracts_dir,
        layout=layout,
        dags=reports,
        changed_files=_diff_snapshots(before, after),
        python_files=len(after),
        warnings=warnings,
    )


# --------------------------------------------------------------------------- store


def prepare_store(config_path: Path) -> str:
    """Create the incident store of ``config_path``, or migrate an older one; returns the report.

    ``init_db(..., allow_migrate=True)`` on the configured store (``store.url``,
    ``$HEALER_DATABASE_URL`` or the SQLite file under ``$HEALER_HOME``). Never raises: a store
    that cannot be prepared is described as ``not prepared: ...`` (the worker creates or
    migrates it when it starts).
    """
    from dag_healer.config import load_config_file
    from dag_healer.store import get_engine
    from dag_healer.store.db import init_db
    from dag_healer.store.migrations import current_version
    from dag_healer.store.models import SCHEMA_VERSION

    url = ""
    try:
        url = load_config_file(config_path).database_url
        engine = get_engine(url)
        try:
            before = current_version(engine)
            init_db(engine, allow_migrate=True)
            after = current_version(engine)
        finally:
            engine.dispose()
    except Exception as exc:  # ConfigError, MigrationError, driver-specific connection errors
        lines = str(exc).strip().splitlines()
        detail = lines[0] if lines else ""
        if url:
            detail = detail.replace(url, mask_url(url) or "")
        return (
            f"not prepared: {type(exc).__name__}: {detail[:300]}; "
            "the worker creates or migrates it when it starts"
        )
    shown = mask_url(url)
    if before == 0:
        return f"{shown} (created at schema v{after})"
    if before > SCHEMA_VERSION:
        return f"{shown} (schema v{before}, newer than this dag-healer: upgrade dag-healer)"
    if before < after:
        return f"{shown} (migrated from schema v{before} to v{after})"
    return f"{shown} (schema v{after}, up to date)"


# --------------------------------------------------------------------------- output


def _datasets_cell(report: DagReport) -> str:
    return f"{report.datasets_found} found, {report.datasets_unresolved} unresolved"


def _tests_line(layout: TestLayout, repo_root: Path) -> str:
    if not layout.exists or not layout.tests_dir:
        return NO_TESTS_NOTE
    details = [layout.framework or "unknown framework", f"files {layout.file_pattern}"]
    if layout.has_conftest:
        details.append("conftest.py")
    return f"{layout.tests_dir}/ ({', '.join(details)}) in {repo_root}"


def format_report(result: InitResult) -> str:
    """Render the human-readable summary printed by ``dag-healer init``."""
    lines = [
        f"Scanned {result.repo_root} (DAG folder: {result.dags_dir}): {len(result.dags)} DAG(s) found",
        "",
    ]
    if result.dags:
        rows = [
            (r.dag_id, r.file, r.tasks, _datasets_cell(r), r.contract)
            for r in sorted(result.dags, key=lambda item: item.dag_id)
        ]
        lines += [render_table(("DAG_ID", "FILE", "TASKS", "DATASETS", "CONTRACT"), rows), ""]
    problems = [(r.dag_id, p) for r in result.dags for p in r.problems]
    if problems or result.warnings:
        lines.append("Notes:")
        lines += [f"  - {message}" for message in result.warnings]
        lines += [f"  - {dag_id}: {message}" for dag_id, message in problems]
        lines.append("")
    config_state = "written" if result.config_written else "kept existing file (not modified)"
    lines += [
        f"Contracts:     {result.contracts_dir}",
        f"healer.yaml:   {result.config_path} ({config_state})",
        f"Tests:         {_tests_line(result.layout, result.repo_root)}",
    ]
    if result.store is not None:
        lines.append(f"Store:         {result.store}")
    if result.ok:
        lines.append(f"Code files:    unchanged ({result.python_files} Python files verified)")
    lines += ["", FINAL_NOTE]
    return "\n".join(lines)


def _configure(parser: argparse.ArgumentParser) -> None:
    parser.add_argument(
        "--repo",
        metavar="PATH",
        default=None,
        help="Repository holding the DAGs (default: repo.path of an existing healer.yaml, else .).",
    )
    add_config_option(
        parser,
        "healer.yaml to read and create if missing (default: $HEALER_CONFIG, else <repo>/healer.yaml).",
    )
    parser.add_argument(
        "--samples",
        metavar="DIR",
        default=None,
        help="Read sample datasets from DIR instead of the repository "
        "(matched by relative path, then file name).",
    )
    parser.add_argument(
        "--dags-dir", metavar="DIR", default=None, help="DAG folder relative to the repo (default: dags)."
    )
    parser.add_argument("--force", action="store_true", help="Overwrite existing DRAFT contracts.")


def _run(args: argparse.Namespace) -> int:
    try:
        result = run_init(
            args.repo,
            config_path=config_arg(args),
            samples=args.samples,
            force=args.force,
            dags_dir=args.dags_dir,
        )
    except InitError as exc:
        error(str(exc))
        return 1
    if result.ok:
        result.store = prepare_store(result.config_path)
    print(format_report(result))
    if not result.ok:
        error("dag-healer init changed repository code files. This must never happen; please report it:")
        for change in result.changed_files:
            print(f"  {change}", file=sys.stderr)
        return 1
    return 0


register_command(
    "init",
    help="Discover DAGs and datasets, write draft contracts and healer.yaml (never touches code).",
    handler=_run,
    configure=_configure,
)

__all__ = [
    "FINAL_NOTE",
    "NO_TESTS_NOTE",
    "DagReport",
    "DatasetCandidate",
    "InitError",
    "InitResult",
    "dataset_candidates",
    "format_report",
    "prepare_store",
    "profile_datasets",
    "run_init",
    "scan_repository",
    "write_config",
]
