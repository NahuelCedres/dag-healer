"""``dag-healer init``: discover DAGs, profile their datasets and write draft contracts.

What it does, in order:

1. snapshots the sha256 of every ``*.py`` file of the repository;
2. scans the repository statically with the code locator of the configured orchestrator
   (``adapter.code.scan``; for Airflow :func:`dag_healer.repo.dagscan.scan_dags` of the DAG
   folder, ``--dags-dir`` included; user code is never imported) and detects the test layout
   (:func:`dag_healer.repo.profile.detect_test_layout`). Without a ``healer.yaml`` the
   configuration is the template that step 4 writes: ``--orchestrator`` picks it, Airflow's by
   default, or the AWS Step Functions one (whose region, instance and pipelines are examples);
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

The report names pipelines and steps with the words of the orchestrator's adapter (DAGs and
tasks for Airflow, state machines and states for Step Functions). Its closing note gives the
``contracts approve`` command of a draft, ready to run from the current directory
(:func:`approve_command`): only the approver's name is left to fill in.
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

from dag_healer.cli._output import (
    add_config_option,
    config_arg,
    error,
    mask_url,
    render_table,
    shell_command,
)
from dag_healer.cli.main import register_command
from dag_healer.core.vocab import Vocabulary

if TYPE_CHECKING:
    from collections.abc import Iterator

    from dag_healer.config import HealerConfig
    from dag_healer.contracts.schema import DatasetContract, DatasetLocator
    from dag_healer.core.ports import OrchestratorAdapter
    from dag_healer.repo.dagscan import DagInfo, DatasetRef
    from dag_healer.repo.profile import TestLayout

log = logging.getLogger(__name__)

FINAL_NOTE = "Draft contracts are not enforced until a human approves them."
#: Introduces the approve command of :func:`approve_command`.
APPROVE_NOTE = "Review each draft, then approve it under your own name:"
#: Stands for the approver in that command (``--by`` is required: the approver is a person).
APPROVER = "NAME"
NO_TESTS_NOTE = "no test suite found; dag-healer will create tests/ when it proposes the first fix"
#: Notes the Step Functions template that init just wrote (its AWS values are examples).
SFN_TEMPLATE_NOTE = (
    "{config} was written from the AWS Step Functions template: replace the example region, "
    "instance and pipelines of its orchestrator section with yours, then run dag-healer init "
    "again to draft the contracts of your state machines"
)

#: The ``healer.yaml`` templates ``--orchestrator`` picks (``dag_healer.config.TEMPLATE_ORCHESTRATORS``,
#: which a test keeps equal), spelled out here so that ``--help`` imports no pydantic.
ORCHESTRATORS = ("airflow", "stepfunctions")
#: The default ``--dags-dir`` of each template: the DAG folder, or the folder of the state
#: machine definitions (``sam init``'s layout and the example repository's).
DEFAULT_DAGS_DIRS = {"airflow": "dags", "stepfunctions": "statemachine"}

WRITTEN = "written"
OVERWRITTEN = "overwritten (--force)"
KEPT_APPROVED = "kept (approved)"
KEPT_DRAFT = "kept (draft exists; --force overwrites it)"
KEPT_INVALID = "kept (existing file is invalid)"
NO_DATASETS = "skipped (no datasets found)"
NO_READABLE = "skipped (no readable datasets)"
#: The outcomes that leave a draft contract (the ones :func:`approve_command` may name).
DRAFT_OUTCOMES = (WRITTEN, OVERWRITTEN, KEPT_DRAFT)

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
    vocabulary: Vocabulary = field(default_factory=Vocabulary)  # the adapter's words (Airflow's)
    # Set by the command: the next commands need ``-c config_path`` (the current directory and
    # the environment lead them to another healer.yaml, or to none).
    config_option: bool = False

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


@contextlib.contextmanager
def _scan_adapter(cfg: HealerConfig | None, repo_root: Path, dags_dir: str) -> Iterator[OrchestratorAdapter]:
    """The configured orchestrator's adapter, built for one scan (no I/O) and closed afterwards.

    It is built from ``cfg`` (the defaults without one) with ``repo.path = repo_root`` and
    ``repo.dags_dir = dags_dir``. Raises :class:`InitError` when it cannot be loaded or built.
    """
    from dag_healer.config import HealerConfig
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
        yield adapter
    finally:
        with contextlib.suppress(Exception):  # closing is best effort
            adapter.close()


def _scan(adapter: OrchestratorAdapter, repo_root: Path) -> list[DagInfo]:
    """Every pipeline the adapter's code locator finds in ``repo_root`` (InitError on failure)."""
    from dag_healer.core.errors import OrchestratorError

    try:
        return list(adapter.code.scan(repo_root))
    except OrchestratorError as exc:
        raise InitError(f"{adapter.display_name} could not scan {repo_root}: {exc}") from None


def scan_repository(cfg: HealerConfig | None, repo_root: Path, dags_dir: str) -> list[DagInfo]:
    """Every pipeline of ``repo_root``, found by the configured orchestrator's code locator.

    The adapter is built for this scan only (no I/O) from ``cfg`` (the defaults without one)
    with ``repo.path = repo_root`` and ``repo.dags_dir = dags_dir``, and closed afterwards. For
    Airflow this is exactly ``scan_dags(repo_root, dags_dir)``. Raises :class:`InitError` when
    the adapter cannot be loaded or built, or the scan fails.
    """
    with _scan_adapter(cfg, repo_root, dags_dir) as adapter:
        return _scan(adapter, repo_root)


def _vocabulary(adapter: OrchestratorAdapter) -> Vocabulary:
    """The adapter's words (Airflow's when it has none)."""
    words = getattr(adapter, "vocabulary", None)
    return words if isinstance(words, Vocabulary) else Vocabulary()


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
    step: str = "task",
) -> tuple[DagReport, dict[str, DatasetLocator]]:
    """Profile one DAG and write its draft contract; returns the report and task locators.

    ``step`` is the adapter's word for a task (``state`` for Step Functions).
    """
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
            f"datasets.{dag.dag_id}.<{step.replace(' ', '_')}_id> in healer.yaml"
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


def _render_config(
    target: Path,
    *,
    repo_root: Path,
    dags_dir: str,
    tests_dir: str | None,
    datasets: dict[str, dict[str, DatasetLocator]] | None,
    orchestrator: str,
) -> tuple[str, HealerConfig]:
    """The ``healer.yaml`` template of ``orchestrator`` for ``target``, and its validated settings."""
    import yaml

    from dag_healer.config import ConfigError, config_from_mapping, default_config_yaml

    text = default_config_yaml(
        repo_path=_relative_repo_path(repo_root, target.parent),
        dags_dir=dags_dir,
        tests_dir=tests_dir,
        datasets=datasets,
        orchestrator=orchestrator,
    )
    try:
        cfg = config_from_mapping(yaml.safe_load(text) or {}, base_dir=target.parent)
    except ConfigError as exc:  # a bug in the template, or a value of its ${ENV} references
        raise InitError(f"generated healer.yaml is invalid: {exc}") from None
    return text, cfg


def write_config(
    target: Path,
    *,
    repo_root: Path,
    dags_dir: str,
    tests_dir: str | None,
    datasets: dict[str, dict[str, DatasetLocator]],
    orchestrator: str = "airflow",
) -> None:
    """Render ``healer.yaml`` from the template of ``orchestrator``, validate it and write it to
    ``target``."""
    text, _cfg = _render_config(
        target,
        repo_root=repo_root,
        dags_dir=dags_dir,
        tests_dir=tests_dir,
        datasets=datasets,
        orchestrator=orchestrator,
    )
    _write_text_atomic(target, text)


def _check_orchestrator(cfg: HealerConfig, path: Path, orchestrator: str | None) -> None:
    """Refuse ``--orchestrator`` when the existing ``healer.yaml`` names another orchestrator."""
    if orchestrator is None:
        return
    from dag_healer.orchestrators import registry

    configured = registry.orchestrator_settings(cfg).type
    if configured in (orchestrator, registry.BUILTIN_TYPES.get(orchestrator)):
        return
    raise InitError(
        f"{path} names the orchestrator {configured!r}: --orchestrator {orchestrator} only picks "
        "the template of a new healer.yaml, and init never changes an existing one"
    )


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
    orchestrator: str | None = None,
) -> InitResult:
    """Run ``dag-healer init`` and return what it did (it prints nothing).

    ``orchestrator`` (:data:`ORCHESTRATORS`) picks the template of a new ``healer.yaml``
    (Airflow's when ``None``); with an existing file it must name that file's orchestrator.
    Without a ``healer.yaml``, the repository is scanned with the settings of the template
    that is then written. Raises :class:`InitError` when the repository, DAG folder, samples
    folder, an existing ``healer.yaml``, the template or the orchestrator adapter is unusable.
    """
    from dag_healer.repo.profile import detect_test_layout, snapshot_hashes

    warnings: list[str] = []
    target_config, origin = _target_config_path(repo, config_path)
    cfg = _load_existing_config(target_config)
    if cfg is not None:
        _check_orchestrator(cfg, target_config, orchestrator)
    template = orchestrator or "airflow"
    repo_root = _resolve_repo_root(repo, cfg, target_config, origin, warnings)
    default_dags = cfg.repo.dags_dir if cfg is not None else DEFAULT_DAGS_DIRS.get(template, "dags")
    dags_folder = _clean_dir(dags_dir or default_dags)
    if cfg is None:  # the settings of the template that is written below
        _text, settings = _render_config(
            target_config,
            repo_root=repo_root,
            dags_dir=dags_folder,
            tests_dir=None,
            datasets=None,
            orchestrator=template,
        )
    else:
        settings = cfg
    samples_dir = Path(samples).expanduser().resolve() if samples else None

    with _scan_adapter(settings, repo_root, dags_folder) as adapter:
        words = _vocabulary(adapter)
        if not (repo_root / dags_folder).is_dir():
            raise InitError(f"{words.pipeline} folder not found: {repo_root / dags_folder} (use --dags-dir)")
        if samples_dir is not None and not samples_dir.is_dir():
            raise InitError(f"--samples folder not found: {samples_dir}")
        before = snapshot_hashes(repo_root, ("**/*.py",))
        dags = _scan(adapter, repo_root)
    layout = detect_test_layout(repo_root)
    contracts_dir = _contracts_dir(settings, repo_root)
    configured = settings.datasets
    pii_columns = list(settings.masking.columns)

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
            step=words.step,
        )
        reports.append(report)
        if task_locators:
            yaml_datasets[dag.dag_id] = task_locators
    if not dags:
        warnings.append(f"no {words.pipelines} found under {repo_root / dags_folder}")

    config_written = False
    if not target_config.exists():
        write_config(
            target_config,
            repo_root=repo_root,
            dags_dir=dags_folder,
            tests_dir=layout.tests_dir,
            datasets=yaml_datasets,
            orchestrator=template,
        )
        config_written = True
        if template == "stepfunctions":
            warnings.append(SFN_TEMPLATE_NOTE.format(config=target_config.name))

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
        vocabulary=words,
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


def _header(word: str) -> str:
    """A table header made of the adapter's word (``state machine`` -> ``STATE_MACHINE``)."""
    return word.upper().replace(" ", "_")


def approve_command(result: InitResult) -> str | None:
    """The ``contracts approve`` command of the first draft, quoted for this platform's shell.

    ``-c <healer.yaml>`` comes first when :attr:`InitResult.config_option` is set; the approver
    is :data:`APPROVER`, the one value left to fill in. ``None`` when no contract is a draft.
    """
    drafts = sorted(report.dag_id for report in result.dags if report.contract in DRAFT_OUTCOMES)
    if not drafts:
        return None
    command = ["dag-healer"]
    if result.config_option:
        command += ["-c", str(result.config_path)]
    command += ["contracts", "approve", drafts[0], "--by", APPROVER]
    return shell_command(command)


def format_report(result: InitResult) -> str:
    """Render the human-readable summary printed by ``dag-healer init``."""
    words = result.vocabulary
    lines = [
        f"Scanned {result.repo_root} ({words.pipeline} folder: {result.dags_dir}): "
        f"{len(result.dags)} {words.pipeline}(s) found",
        "",
    ]
    if result.dags:
        rows = [
            (r.dag_id, r.file, r.tasks, _datasets_cell(r), r.contract)
            for r in sorted(result.dags, key=lambda item: item.dag_id)
        ]
        headers = (f"{_header(words.pipeline)}_ID", "FILE", _header(words.steps), "DATASETS", "CONTRACT")
        lines += [render_table(headers, rows), ""]
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
    command = approve_command(result)
    if command is not None:
        lines += ["", f"{FINAL_NOTE} {APPROVE_NOTE}", f"  {command}"]
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
        "--dags-dir",
        metavar="DIR",
        default=None,
        help="DAG folder (Step Functions: the folder of the state machine definitions), relative to "
        "the repo (default: repo.dags_dir of an existing healer.yaml, else dags; statemachine with "
        "--orchestrator stepfunctions).",
    )
    parser.add_argument(
        "--orchestrator",
        choices=ORCHESTRATORS,
        default=None,
        help="Template of a new healer.yaml: airflow (the default) or stepfunctions (AWS Step "
        "Functions, with example values to replace). An existing healer.yaml is never changed.",
    )
    parser.add_argument("--force", action="store_true", help="Overwrite existing DRAFT contracts.")


def _needs_config_option(result: InitResult, explicit: bool) -> bool:
    """True when ``dag-healer`` run from here, without ``-c``, would not read ``result.config_path``.

    ``explicit``: the command line named the file, which ``main()`` exported as ``HEALER_CONFIG``
    for this process only, so the next commands need ``-c`` too.
    """
    if explicit:
        return True
    from dag_healer.config import find_config_file

    found = find_config_file()
    return found is None or found.resolve() != result.config_path


def _run(args: argparse.Namespace) -> int:
    try:
        result = run_init(
            args.repo,
            config_path=config_arg(args),
            samples=args.samples,
            force=args.force,
            dags_dir=args.dags_dir,
            orchestrator=args.orchestrator,
        )
    except InitError as exc:
        error(str(exc))
        return 1
    if result.ok:
        result.store = prepare_store(result.config_path)
    result.config_option = _needs_config_option(result, config_arg(args) is not None)
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
    "APPROVER",
    "APPROVE_NOTE",
    "DEFAULT_DAGS_DIRS",
    "FINAL_NOTE",
    "NO_TESTS_NOTE",
    "ORCHESTRATORS",
    "SFN_TEMPLATE_NOTE",
    "DagReport",
    "DatasetCandidate",
    "InitError",
    "InitResult",
    "approve_command",
    "dataset_candidates",
    "format_report",
    "prepare_store",
    "profile_datasets",
    "run_init",
    "scan_repository",
    "write_config",
]
