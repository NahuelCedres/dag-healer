"""The Airflow adapter: Airflow 3 as one implementation of the orchestrator ports (section 8).

:class:`AirflowAdapter` puts the phase-5 pieces (the REST client
:class:`~dag_healer.airflow.client.AirflowClient` and the static DAG scanner
:func:`~dag_healer.repo.dagscan.scan_dags`) behind :class:`~dag_healer.core.ports.OrchestratorAdapter`
(see ``docs/architecture/orchestrators.md``):

=====================  ===========================================================================
Port                   Airflow
=====================  ===========================================================================
``feed.poll``          ``list_failed_task_instances(updated_since=since)`` (``state=failed`` only)
                       with the phase-5 window: ``watermark - max(60 s, 2 x poll_interval)``, 24 h
                       back without a watermark, never before the ``not_before`` floor
``push.normalize``     the ``task-failed`` payload of the listener and ``on_failure_callback``
``push.confirm``       ``get_task_instance`` and the phase-5 rules (``failed`` vs ``up_for_retry``,
                       a later try exists, a try Airflow never ran, 404 -> discarded); a try
                       that has not ended yet is checked again later
``inspector``          ``get_task_instance`` (describe), ``get_task_log`` (logs), ``get_dag`` and
                       ``get_dag_source`` (pipeline_source)
``code``               :class:`AirflowCodeLocator` (``scan_dags`` and the container path mappings)
``remediator``         ``clear_task_instances`` (``RETRY_STEP``); ``trigger_dag_run``
                       (``RERUN_PIPELINE``, off by default)
``links``              :class:`AirflowNativeLinks` (the task instance page of the Airflow UI)
=====================  ===========================================================================

Construction does no I/O: the REST client is built from ``cfg.airflow`` on first use, or
injected (``client=``: an :class:`AirflowClient` on an ``httpx.MockTransport``, or a phase-5 test
double that implements only the methods it needs; a missing method raises
:class:`~dag_healer.core.errors.NotSupported`). Every client error leaves the adapter as a
:mod:`dag_healer.core.errors` exception (:func:`dag_healer.airflow.errors.translate_error`).
Texts handed to the core (log lines, the pushed error, error messages) are RAW: the core masks
them, and the adapter never logs them. This module never imports Airflow, and imports httpx only
when the REST client is first needed.

:data:`FACTORY` is the ``airflow`` entry point of the ``dag_healer.orchestrators`` group. The
Airflow adapter takes no ``orchestrator.options``: its settings stay in the ``airflow:`` section
of ``healer.yaml``.
"""

from __future__ import annotations

import logging
import os
import posixpath
import re
import threading
import time
from collections import OrderedDict
from collections.abc import Callable, Mapping
from dataclasses import replace
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import TYPE_CHECKING, Any, NoReturn
from urllib.parse import quote

from dag_healer.airflow.errors import AIRFLOW_TAXONOMY, error_text, translate_error
from dag_healer.core.errors import NotFound, NotSupported, OrchestratorError
from dag_healer.core.evidence import AttemptDetail, LogBundle, PipelineSource
from dag_healer.core.failures import AttemptState, Confirmation, Failure, FailureHint, FailurePage, Trigger
from dag_healer.core.ids import NO_MAP_INDEX, RUN_LEVEL_STEP, AttemptRef
from dag_healer.core.ports import DoctorCheck, HealthReport, RemediationResult
from dag_healer.core.specs import DagInfo, PathMapping, TaskInfo
from dag_healer.core.vocab import Capabilities, RemediationKind, Vocabulary
from dag_healer.links import airflow_base_url
from dag_healer.orchestrators.registry import orchestrator_settings, parse_options

if TYPE_CHECKING:
    from dag_healer.airflow.client import AirflowClient
    from dag_healer.config import HealerConfig

logger = logging.getLogger(__name__)

AIRFLOW_TYPE = "airflow"
DISPLAY_NAME = "Apache Airflow"
#: ``orchestrator.type`` may also name this factory explicitly.
FACTORY_TARGET = "dag_healer.airflow.adapter:FACTORY"
#: The push channel of the listener and the failure callback (``POST /events/task-failed``).
PUSH_CHANNEL = "task-failed"
#: Incident source of the REST poll (kept, so the UI badges stay identical).
POLL_SOURCE = "rest"
#: Incident sources of pushed events (any other ``source`` of a payload becomes ``listener``).
EVENT_SOURCES: tuple[str, ...] = ("listener", "callback")
DEFAULT_EVENT_SOURCE = "listener"
LOG_SOURCE = "airflow:task-log"
TASK_LINK_LABEL = "Airflow task"
RUN_LINK_LABEL = "Airflow DAG run"

#: Without a watermark the first poll looks this far back (phase 5: ``worker.pipeline``).
INITIAL_LOOKBACK = timedelta(hours=24)
#: Minimum overlap between two polls (the worker's and Airflow's clocks may differ).
MIN_POLL_OVERLAP = timedelta(seconds=60)
DEFAULT_POLL_INTERVAL_SECONDS = 30
DEFAULT_POLL_LIMIT = 200
#: Keys of the poll cursor.
WATERMARK = "watermark"
NOT_BEFORE = "not_before"
#: Keys of the phase-5 ``<healer_home>/state.json`` (imported once into the cursor).
STATE_WATERMARK_KEY = "rest_watermark"
STATE_NOT_BEFORE_KEY = "rest_not_before"

#: Where the repository is mounted in the Airflow containers and Airflow's default DAG folder
#: (the values of ``agent.context.CONTAINER_REPO_ROOT`` / ``DEFAULT_AIRFLOW_DAGS_FOLDER``).
CONTAINER_REPO_ROOT = "/opt/airflow/repo"
DEFAULT_AIRFLOW_DAGS_FOLDER = "/opt/airflow/dags"
DAGS_FOLDER_ENV = "AIRFLOW__CORE__DAGS_FOLDER"

#: Limits of a ``task-failed`` payload (those of the API's ``TaskFailedEvent`` schema).
MAX_ID_CHARS = 250
MAX_EVENT_ERROR_CHARS = 64 * 1024
MAX_SOURCE_CHARS = 32
DEFAULT_LOG_MAX_CHARS = 2_000_000
#: Remediation results remembered for their idempotency keys (per adapter).
MAX_REMEMBERED_REMEDIATIONS = 256
#: How long the outcome of a ``pipeline_source`` call without the text is kept for the call
#: with the text that may follow it in the same gathering, and how many DAGs are kept.
PIPELINE_MEMO_SECONDS = 60.0
MAX_PIPELINE_MEMOS = 64

AIRFLOW_VOCABULARY = Vocabulary()
"""Today's words (DAG, task, try, "Retry task"): every Airflow text stays identical."""

AIRFLOW_CAPABILITIES = Capabilities(
    sources=("rest", "listener", "callback", "cli"),
    push_channels=(PUSH_CHANNEL,),
    attempts_visible=True,
    remediations=(RemediationKind.RETRY_STEP,),
    fix_delivery="repo_sync",
    ui_hosts=("airflow_plugin", "standalone"),
    native_authz=True,
)

#: The Airflow part of the pytest sandbox that does not depend on the run: unit-test mode and no
#: example DAGs. The per-run ``AIRFLOW_HOME`` and its SQLite metadata database stay in
#: ``repo.pytest_runner.scrubbed_env`` (they live in the run's temporary directory).
SANDBOX_ENV: dict[str, str | None] = {
    "AIRFLOW__CORE__UNIT_TEST_MODE": "True",
    "AIRFLOW__CORE__LOAD_EXAMPLES": "False",
}

_RUNNING_STATES = frozenset({"running", "queued", "scheduled", "deferred", "up_for_reschedule", "restarting"})
_INTEGER = re.compile(r"-?[0-9]+")
#: Memo values of ``AirflowAdapter.pipeline_source``: a DAG that could not be read, and nothing.
_UNREADABLE = object()
_NOTHING = object()


# =========================================================================== small helpers


def _utcnow() -> datetime:
    return datetime.now(timezone.utc)


def _as_utc(value: datetime) -> datetime:
    return value.replace(tzinfo=timezone.utc) if value.tzinfo is None else value.astimezone(timezone.utc)


def _text(value: Any) -> str:
    return "" if value is None else str(value)


def _opt_text(value: Any) -> str | None:
    text = _text(value).strip()
    return text or None


def _as_int(value: Any, default: int) -> int:
    if value is None or isinstance(value, bool):
        return default
    try:
        return int(value)
    except (TypeError, ValueError):
        return default


def _segment(value: Any) -> str:
    """Percent-encode one URL path segment (``/``, ``:``, ``+`` and spaces included)."""
    return quote(str(value), safe="")


def parse_timestamp(value: Any) -> datetime | None:
    """An aware UTC datetime from an ISO-8601 timestamp (``...Z`` included); ``None`` if invalid."""
    if isinstance(value, datetime):
        return _as_utc(value)
    if not isinstance(value, str) or not value.strip():
        return None
    text = value.strip()
    if text[-1:] in ("Z", "z"):
        text = text[:-1] + "+00:00"
    try:
        return _as_utc(datetime.fromisoformat(text))
    except ValueError:
        return None


def format_cursor_time(value: datetime) -> str:
    """ISO-8601 UTC text of a cursor time (``2026-10-17T18:40:00+00:00``, as in ``state.json``)."""
    return _as_utc(value).isoformat()


# =========================================================================== the poll cursor


def poll_cursor(watermark: datetime | None, not_before: datetime | None = None) -> dict[str, str | None]:
    """The poll cursor ``{"watermark": <poll start>, "not_before": <floor or None>}`` (JSON-safe)."""
    return {
        WATERMARK: format_cursor_time(watermark) if watermark is not None else None,
        NOT_BEFORE: format_cursor_time(not_before) if not_before is not None else None,
    }


def reset_cursor(moment: datetime) -> dict[str, str | None]:
    """The cursor that restarts the poll at ``moment`` (``dag-healer store reset``).

    Failures updated before ``moment`` are never listed again, even within the overlap of the
    next polls (both the watermark and the floor are ``moment``).
    """
    return poll_cursor(moment, moment)


def cursor_from_state(state: Mapping[str, Any]) -> dict[str, str | None] | None:
    """The cursor equivalent to a phase-5 ``state.json`` (``rest_watermark``, ``rest_not_before``).

    ``None`` when the state holds neither (the first poll then looks 24 hours back); an invalid
    value is dropped, like the phase-5 worker ignored it.
    """
    watermark = parse_timestamp(state.get(STATE_WATERMARK_KEY))
    floor = parse_timestamp(state.get(STATE_NOT_BEFORE_KEY))
    if watermark is None and floor is None:
        return None
    return poll_cursor(watermark, floor)


def _cursor_time(cursor: Mapping[str, Any] | None, key: str) -> datetime | None:
    if not isinstance(cursor, Mapping):
        return None
    value = cursor.get(key)
    if value is None or value == "":
        return None
    moment = parse_timestamp(value)
    if moment is None:
        logger.warning("dag-healer: ignoring the invalid %s %r of the Airflow poll cursor", key, value)
    return moment


# =========================================================================== phase-5 rules


def confirm_try(ti: Any, try_number: int, trigger: str) -> tuple[bool, str]:
    """Does the task instance ``ti`` confirm the reported failure of try ``try_number``?

    The phase-5 rules of ``worker.pipeline._confirm``, verbatim (reasons included);
    :meth:`AirflowAdapter.confirm` applies them once the try has ended. ``ti`` is a
    :class:`~dag_healer.airflow.client.TaskInstanceInfo` (or anything with ``state`` and
    ``try_number``); it shows the task instance's latest try.
    """
    state = _text(getattr(ti, "state", None)).lower() or "unknown"
    latest = _as_int(getattr(ti, "try_number", None), 0)
    if latest < try_number:
        return False, f"Airflow knows only {latest} tries (state {state})"
    if trigger == "every_failure":
        if state in ("failed", "up_for_retry") or latest > try_number:
            return True, f"state {state}, try {latest}"
        return False, f"the task instance is {state}, not failed"
    if latest > try_number:
        return False, f"try {try_number} was retried (try {latest} is {state})"
    if state == "failed":
        return True, f"state failed, try {latest}"
    if state == "up_for_retry":
        return False, "the try will be retried (monitor.trigger is final_failure)"
    return False, f"the task instance is {state}, not failed"


def _unfinished_state(ti: Any, attempt: int) -> str | None:
    """The state of try ``attempt`` while it is the latest try and has not ended, else ``None``.

    The listener posts a failure while the task process finishes; the task SDK supervisor
    records the end of the try (``failed``, ``up_for_retry``) a few seconds later, so a hint
    can be checked while Airflow still shows the try ``running``.
    """
    if _as_int(getattr(ti, "try_number", None), 0) != attempt:
        return None
    state = _text(getattr(ti, "state", None)).lower()
    return state if state in _RUNNING_STATES else None


def _error_line(exc: BaseException) -> str:
    """``Type: <first line of the message>`` (``Type`` alone without one); RAW.

    The text the phase-5 worker reported for an Airflow API call that failed.
    """
    message = str(exc).strip()
    first = message.splitlines()[0] if message else ""
    return f"{type(exc).__name__}: {first}" if first else type(exc).__name__


def attempt_state(ti: Any, attempt: int) -> tuple[AttemptState, bool]:
    """``(state, is_final)`` of try ``attempt`` of a task instance that shows its latest try.

    An earlier try than the latest one failed and was run again (``RETRYING``, not final). For
    the latest try: ``failed`` is the terminal failure, ``up_for_retry`` a failure Airflow
    retries, ``success`` succeeded, the scheduling states are ``RUNNING`` and anything else
    (``upstream_failed``, ``skipped``, ``removed``, no state) is ``UNKNOWN``.
    """
    if _as_int(getattr(ti, "try_number", None), 0) > attempt:
        return AttemptState.RETRYING, False
    state = _text(getattr(ti, "state", None)).lower()
    if state == "failed":
        return AttemptState.FAILED, True
    if state == "up_for_retry":
        return AttemptState.RETRYING, False
    if state == "success":
        return AttemptState.SUCCEEDED, False
    if state in _RUNNING_STATES:
        return AttemptState.RUNNING, False
    return AttemptState.UNKNOWN, False


def cleared_count(response: Mapping[str, Any]) -> int | None:
    """Task instances a ``clearTaskInstances`` answer reports (phase 5: ``worker.commands``)."""
    items = response.get("task_instances")
    if isinstance(items, list):
        return len(items)
    total = response.get("total_entries")
    return int(total) if isinstance(total, int) and not isinstance(total, bool) else None


def airflow_instance(cfg: Any) -> str:
    """The orchestrator instance id of the Airflow adapter for ``cfg``.

    ``orchestrator.instance`` when the configuration selects Airflow (``type: airflow`` or this
    factory's ``"pkg.mod:factory"`` target with an explicit instance), else ``"airflow"``: the
    phase-5 default, also used when a legacy Airflow client is wrapped for a configuration of
    another type.
    """
    settings = orchestrator_settings(cfg)
    if settings.type == AIRFLOW_TYPE:
        return settings.instance
    if settings.type == FACTORY_TARGET and settings.instance != settings.type:
        return settings.instance
    return AIRFLOW_TYPE


# =========================================================================== payload checks


def _id_field(payload: Mapping[str, Any], name: str) -> str:
    value = payload.get(name)
    if not isinstance(value, str):
        raise ValueError(f"{name} must be a string")
    if not value.strip():
        raise ValueError(f"{name} must not be empty")
    if len(value) > MAX_ID_CHARS:
        raise ValueError(f"{name} is longer than {MAX_ID_CHARS} characters")
    return value


def _int_field(payload: Mapping[str, Any], name: str, *, default: int, minimum: int) -> int:
    value = payload.get(name)
    if value is None:
        return default
    if isinstance(value, str) and _INTEGER.fullmatch(value.strip()):
        value = int(value.strip())
    if isinstance(value, bool) or not isinstance(value, int):
        raise ValueError(f"{name} must be an integer")
    if value < minimum:
        raise ValueError(f"{name} must be >= {minimum}")
    return value


# =========================================================================== paths


def _posix(path: Any) -> str:
    return str(path).replace("\\", "/")


def clean_dags_dir(value: str | None) -> str:
    """A DAG folder as ``RepoConfig.dags_dir`` keeps it (posix, no ``./`` prefix, ``"."`` for the root)."""
    cleaned = (value or "dags").strip().replace("\\", "/").rstrip("/")
    while cleaned.startswith("./"):
        cleaned = cleaned[2:]
    return cleaned or "."


def _join_rel(base: str | None, rel: str) -> str | None:
    """Join a repo-relative base directory and a relative path; ``None`` if it escapes."""
    joined = (
        posixpath.normpath(posixpath.join(base, rel)) if base and base != "." else posixpath.normpath(rel)
    )
    if joined in (".", "", "..") or joined.startswith(("../", "/")):
        return None
    return joined


def _inside(root: Path, rel: str) -> bool:
    """True when ``rel`` is a file inside the repository ``root``."""
    try:
        candidate = (root / rel).resolve()
        candidate.relative_to(root)
    except (OSError, ValueError):
        return False
    return candidate.is_file()


def _dag_location(dag: Mapping[str, Any], text: str | None) -> PipelineSource:
    """The :class:`PipelineSource` of a ``get_dag`` answer (and the DAG source, when read)."""
    return PipelineSource(
        path=_opt_text(dag.get("fileloc")),
        relative_path=_opt_text(dag.get("relative_fileloc")),
        text=text,
        language="python",
        origin="api:dagSources" if text is not None else "api:dags",
    )


# =========================================================================== code locator and links


class AirflowCodeLocator:
    """:class:`~dag_healer.core.ports.CodeLocator` of Airflow: the static DAG scan (no network I/O).

    ``dags_dir`` overrides ``cfg.repo.dags_dir`` (``dag-healer init --dags-dir``); otherwise the
    configured folder is read on every call.
    """

    def __init__(self, cfg: HealerConfig, *, dags_dir: str | None = None) -> None:
        self._cfg = cfg
        self._dags_dir = clean_dags_dir(dags_dir) if dags_dir and dags_dir.strip() else None

    @property
    def dags_dir(self) -> str:
        """The repo-relative DAG folder that is scanned."""
        if self._dags_dir is not None:
            return self._dags_dir
        repo = getattr(self._cfg, "repo", None)
        return clean_dags_dir(getattr(repo, "dags_dir", None))

    def scan(self, repo_root: Path) -> list[DagInfo]:
        """Every DAG of ``<repo_root>/<dags_dir>`` (:func:`~dag_healer.repo.dagscan.scan_dags`)."""
        from dag_healer.repo.dagscan import scan_dags

        return scan_dags(repo_root, self.dags_dir)

    def path_mappings(self, repo_root: Path, step: TaskInfo | None) -> list[PathMapping]:
        """Where Airflow runs the repository's code (the core adds ``(repo_root, "")`` itself).

        ``/opt/airflow/repo`` -> the repository root, then ``$AIRFLOW__CORE__DAGS_FOLDER`` (read
        on every call) and ``/opt/airflow/dags`` -> the DAG folder: the phase-5 roots of
        ``agent.context``, in that order, the first mapping of a prefix winning.
        """
        dags_dir = self.dags_dir
        pairs: list[tuple[str, str]] = [(CONTAINER_REPO_ROOT, "")]
        env_dags = os.environ.get(DAGS_FOLDER_ENV)
        if env_dags and env_dags.strip():
            pairs.append((posixpath.normpath(_posix(env_dags.strip())), dags_dir))
        pairs.append((DEFAULT_AIRFLOW_DAGS_FOLDER, dags_dir))
        unique: dict[str, str] = {}
        for prefix, base in pairs:
            unique.setdefault(prefix, base)
        return [PathMapping(runtime_prefix=prefix, repo_base=base) for prefix, base in unique.items()]

    def _config_for(self, repo_root: Path) -> Any:
        """``cfg`` with ``repo.path = repo_root`` and the scanned DAG folder (a copy when they differ)."""
        cfg = self._cfg
        root = Path(repo_root).resolve()
        dags_dir = self.dags_dir
        try:
            if Path(cfg.repo_root) == root and clean_dags_dir(cfg.repo.dags_dir) == dags_dir:
                return cfg
            repo = cfg.repo.model_copy(update={"path": root, "dags_dir": dags_dir})
            return cfg.model_copy(update={"repo": repo})
        except AttributeError:  # not a HealerConfig: map against the configured repository
            return cfg

    def reported_file(self, repo_root: Path, source: PipelineSource) -> str | None:
        """The DAG file Airflow reports, as a file of the repository working tree (phase 5).

        ``fileloc`` mapped into the repository (``agent.context.map_to_repo_path``: the container
        mount, the DAG folders, any path under the repository and a known tail of other absolute
        paths, such as a DAG bundle cloned elsewhere), else ``relative_fileloc`` under the DAG
        folder; ``None`` when neither is a file of the repository.
        """
        from dag_healer.agent.context import map_to_repo_path

        root = Path(repo_root).resolve()
        if source.path:
            mapped = map_to_repo_path(source.path, self._config_for(root))
            if mapped and _inside(root, mapped):
                return mapped
        relative = source.relative_path
        if isinstance(relative, str) and relative.strip():
            candidate = _join_rel(self.dags_dir, _posix(relative.strip()))
            if candidate and _inside(root, candidate):
                return candidate
        return None

    def pipeline_file(self, repo_root: Path, pipeline_id: str, source: PipelineSource | None) -> str | None:
        """Repo-relative file of DAG ``pipeline_id``: Airflow's report first, then the static scan."""
        if source is not None:
            reported = self.reported_file(repo_root, source)
            if reported is not None:
                return reported
        return next((dag.file for dag in self.scan(repo_root) if dag.dag_id == pipeline_id), None)


class AirflowNativeLinks:
    """:class:`~dag_healer.core.ports.NativeLinks` of Airflow: the task instance page of the UI.

    The base URL is the one of the Healer views (:func:`dag_healer.links.airflow_base_url`), the
    address people reach Airflow at; every id is percent-encoded.
    """

    def __init__(self, cfg: HealerConfig) -> None:
        self._cfg = cfg

    def base_url(self) -> str:
        """The public base URL of the Airflow UI (no trailing slash)."""
        return airflow_base_url(self._cfg)

    def links(self, ref: AttemptRef, native: Mapping[str, Any]) -> dict[str, str]:
        """``{"Airflow task": <base>/dags/<d>/runs/<r>/tasks/<t>[/mapped/<i>]}`` (the run page
        ``{"Airflow DAG run": ...}`` when the ref names no task)."""
        run = f"{self.base_url()}/dags/{_segment(ref.pipeline_id)}/runs/{_segment(ref.run_id)}"
        if not ref.resolved or ref.step_id == RUN_LEVEL_STEP:
            return {RUN_LINK_LABEL: run}
        url = f"{run}/tasks/{_segment(ref.step_id)}"
        if int(ref.map_index) >= 0:
            url += f"/mapped/{int(ref.map_index)}"
        return {TASK_LINK_LABEL: url}


# =========================================================================== the adapter


class AirflowAdapter:
    """Every port of Airflow on one object (``links`` and ``code`` are small companions).

    ``client`` is an injected REST client (it is not closed by :meth:`close`); without it an
    :class:`~dag_healer.airflow.client.AirflowClient` is built from ``cfg.airflow`` on first use.
    ``instance`` defaults to :func:`airflow_instance`, ``clock`` (aware UTC) drives the poll
    window, and ``allow_rerun`` offers ``RERUN_PIPELINE`` (a new DAG run with the conf of the
    failed run; off by default).
    """

    type = AIRFLOW_TYPE
    display_name = DISPLAY_NAME
    vocabulary = AIRFLOW_VOCABULARY
    capabilities = AIRFLOW_CAPABILITIES
    taxonomy = AIRFLOW_TAXONOMY
    queue = None
    channels = frozenset({PUSH_CHANNEL})

    def __init__(
        self,
        cfg: HealerConfig,
        *,
        client: AirflowClient | Any | None = None,
        instance: str | None = None,
        clock: Callable[[], datetime] | None = None,
        allow_rerun: bool = False,
    ) -> None:
        self._cfg = cfg
        self._client = client
        self._owns_client = False
        self._lock = threading.RLock()
        self._remediation_lock = threading.RLock()
        self._results: OrderedDict[str, RemediationResult] = OrderedDict()
        self._memo_lock = threading.Lock()
        self._dags: OrderedDict[str, tuple[float, Any]] = OrderedDict()
        self._clock: Callable[[], datetime] = clock or _utcnow
        self._allow_rerun = bool(allow_rerun)
        self.instance = instance or airflow_instance(cfg)
        if self._allow_rerun:
            self.capabilities = replace(
                AIRFLOW_CAPABILITIES,
                remediations=(RemediationKind.RETRY_STEP, RemediationKind.RERUN_PIPELINE),
            )
        self.feed = self
        self.push = self
        self.inspector = self
        self.remediator = self
        self.code = AirflowCodeLocator(cfg)
        self.links = AirflowNativeLinks(cfg)

    def __repr__(self) -> str:
        return f"AirflowAdapter(instance={self.instance!r}, base_url={self.base_url!r})"

    # ------------------------------------------------------------------ the REST client
    @property
    def base_url(self) -> str:
        """The Airflow API the adapter talks to (no I/O, no client is built)."""
        injected = getattr(self._client, "base_url", None) if self._client is not None else None
        if isinstance(injected, str) and injected:
            return injected
        airflow = getattr(self._cfg, "airflow", None)
        return str(getattr(airflow, "api_url", "") or "")

    @property
    def client(self) -> AirflowClient | Any:
        """The REST client (built from ``cfg.airflow`` on first use; no I/O happens here)."""
        with self._lock:
            if self._client is None:
                from dag_healer.airflow.client import AirflowClient

                try:
                    self._client = AirflowClient.from_config(self._cfg)
                except Exception as exc:  # an empty or invalid airflow.api_url
                    raise OrchestratorError(
                        f"cannot create the Airflow API client: {error_text(exc)}", transient=False
                    ) from exc
                self._owns_client = True
                logger.debug("Airflow adapter %s: created the REST client", self.instance)
            return self._client

    def _client_for(self, method: str) -> Any:
        """The client, when it implements ``method`` (:class:`NotSupported` otherwise)."""
        client = self.client
        if not callable(getattr(client, method, None)):
            raise NotSupported(f"the Airflow client {type(client).__name__} has no {method}() method")
        return client

    def _raise(self, exc: BaseException, what: str) -> NoReturn:
        """Raise the core error of ``exc`` (logged without its message: it may carry data)."""
        error = translate_error(exc)
        logger.debug(
            "Airflow adapter %s: %s failed: %s (status %s, transient %s)",
            self.instance,
            what,
            type(exc).__name__,
            error.status_code,
            error.transient,
        )
        if error is exc:
            raise error
        raise error from exc

    def _now(self, now: datetime | None = None) -> datetime:
        return _as_utc(now) if now is not None else _as_utc(self._clock())

    # ------------------------------------------------------------------ FailureFeed
    def poll_since(self, cursor: Mapping[str, Any] | None, *, now: datetime | None = None) -> datetime:
        """Start of the window the next :meth:`poll` lists (the phase-5 rule, see the module docstring)."""
        since, _floor = self._window(cursor, self._now(now))
        return since

    def _window(self, cursor: Mapping[str, Any] | None, now: datetime) -> tuple[datetime, datetime | None]:
        watermark = _cursor_time(cursor, WATERMARK)
        floor = _cursor_time(cursor, NOT_BEFORE)
        if watermark is None:
            since = now - INITIAL_LOOKBACK
        else:
            monitor = getattr(self._cfg, "monitor", None)
            interval = _as_int(getattr(monitor, "poll_interval_seconds", None), DEFAULT_POLL_INTERVAL_SECONDS)
            since = watermark - max(MIN_POLL_OVERLAP, timedelta(seconds=2 * interval))
        if floor is not None and since < floor:
            since = floor
        return since, floor

    def poll(
        self,
        *,
        cursor: Mapping[str, Any] | None,
        is_monitored: Callable[[str], bool],
        limit: int = DEFAULT_POLL_LIMIT,
        now: datetime | None = None,
    ) -> FailurePage:
        """The failed task instances updated in the poll window, as terminal failures (source ``rest``).

        The query lists ``state=failed`` only, so every item is final (a retrying try is never
        listed). Unmonitored DAGs and incomplete rows are left out. The returned cursor is
        ``{"watermark": <now>, "not_before": <the cursor's floor>}``; a failed listing raises a
        core error and the caller keeps its cursor. ``limit`` is advisory: the REST client pages
        through the whole window itself (at most 20 pages of 100 rows, as in phase 5). ``now``
        overrides the clock (the worker passes its iteration time).
        """
        moment = self._now(now)
        since, floor = self._window(cursor, moment)
        try:
            client = self._client_for("list_failed_task_instances")
            listed = client.list_failed_task_instances(updated_since=since)
        except Exception as exc:
            self._raise(exc, "listing failed task instances")
        failures: list[Failure] = []
        rows = 0
        for ti in listed or ():
            rows += 1
            failure = self._polled_failure(ti)
            if failure is not None and is_monitored(failure.ref.pipeline_id):
                failures.append(failure)
        logger.debug(
            "Airflow adapter %s: %d failed task instance(s) updated since %s, %d monitored",
            self.instance,
            rows,
            format_cursor_time(since),
            len(failures),
        )
        return FailurePage(failures=tuple(failures), cursor=poll_cursor(moment, floor), complete=True)

    def _polled_failure(self, ti: Any) -> Failure | None:
        dag_id = _text(getattr(ti, "dag_id", None))
        run_id = _text(getattr(ti, "run_id", None))
        task_id = _text(getattr(ti, "task_id", None))
        if not dag_id or not run_id or not task_id:
            return None
        ref = AttemptRef(
            orchestrator=self.instance,
            pipeline_id=dag_id,
            run_id=run_id,
            step_id=task_id,
            map_index=_as_int(getattr(ti, "map_index", None), NO_MAP_INDEX),
            attempt=_as_int(getattr(ti, "try_number", None), 0),
        )
        return Failure(
            ref=ref,
            state=AttemptState.FAILED,
            is_final=True,
            source=POLL_SOURCE,
            failed_at=parse_timestamp(getattr(ti, "end_date", None)),
        )

    # ------------------------------------------------------------------ PushIntake
    def normalize(
        self, channel: str, payload: Mapping[str, Any], headers: Mapping[str, str]
    ) -> list[FailureHint]:
        """PURE. The ``task-failed`` payload (``dag_healer.ingest.build_payload``) as one hint.

        Keeps ``dag_id``, ``run_id``, ``task_id``, ``map_index`` (default -1), ``try_number``
        (default 1), ``error`` (RAW) and ``source`` (``listener``/``callback``, anything else is
        ``listener``); every other key is dropped. Raises :class:`ValueError` for a malformed
        payload, with the limits of the API's ``TaskFailedEvent`` schema (ids of 1-250
        characters, an error of at most 64 KiB) plus ``map_index >= -1`` and ``try_number >= 1``.
        """
        if channel not in self.channels:
            raise ValueError(
                f"unknown push channel {channel!r} (the Airflow adapter accepts: {PUSH_CHANNEL})"
            )
        if not isinstance(payload, Mapping):
            raise ValueError("the task-failed payload must be a JSON object")
        dag_id = _id_field(payload, "dag_id")
        run_id = _id_field(payload, "run_id")
        task_id = _id_field(payload, "task_id")
        map_index = _int_field(payload, "map_index", default=NO_MAP_INDEX, minimum=NO_MAP_INDEX)
        try_number = _int_field(payload, "try_number", default=1, minimum=1)
        error = payload.get("error")
        if error is None:
            error = ""
        if not isinstance(error, str):
            raise ValueError("error must be a string")
        if len(error) > MAX_EVENT_ERROR_CHARS:
            raise ValueError(f"error is longer than {MAX_EVENT_ERROR_CHARS} characters")
        source = payload.get("source")
        if source is not None and (not isinstance(source, str) or len(source) > MAX_SOURCE_CHARS):
            raise ValueError(f"source must be a string of at most {MAX_SOURCE_CHARS} characters")
        ref = AttemptRef(self.instance, dag_id, run_id, task_id, map_index, try_number)
        return [
            FailureHint(
                ref=ref,
                channel=channel,
                source=source if source in EVENT_SOURCES else DEFAULT_EVENT_SOURCE,
                error_text=error,
            )
        ]

    def confirm(self, hint: FailureHint, *, trigger: Trigger) -> Confirmation:
        """Check a pushed hint against the task instance (the phase-5 rules, :func:`confirm_try`).

        404 -> ``discarded`` ("Airflow does not know this task instance"); any other error ->
        ``retry_later`` with the phase-5 text ``"<Type>: <message>"`` (the core discards the
        event once it is older than its maximum age). A try that is still the latest one and has
        not ended (``running``, ``queued``...) is deferred too: ``retry_later`` with state
        ``RUNNING``, because the listener posts a failure a few seconds before Airflow records
        the end of the try. A confirmed failure keeps the hint's key, source
        (``listener``/``callback``, anything else is ``listener``) and RAW error text (its
        summary and signature come from the pushed text, as in phase 5: no log is fetched).
        """
        ref = hint.ref
        if ref.orchestrator != self.instance:
            return Confirmation(
                verdict="discarded",
                reason=f"the event is for orchestrator instance {ref.orchestrator!r}, not {self.instance!r}",
            )
        if not ref.resolved or ref.step_id == RUN_LEVEL_STEP:
            return Confirmation(verdict="discarded", reason="the event names no task")
        try:
            client = self._client_for("get_task_instance")
            ti = client.get_task_instance(ref.pipeline_id, ref.run_id, ref.step_id, int(ref.map_index))
        except Exception as exc:
            if isinstance(translate_error(exc), NotFound):
                result = Confirmation(verdict="discarded", reason="Airflow does not know this task instance")
            else:
                result = Confirmation(verdict="retry_later", reason=_error_line(exc))
            self._log_confirmation(hint, result.verdict, type(exc).__name__)
            return result
        unfinished = _unfinished_state(ti, ref.attempt)
        if unfinished is not None:
            reason = f"the task instance is {unfinished}, not failed yet"
            self._log_confirmation(hint, "retry_later", reason)
            return Confirmation(verdict="retry_later", reason=reason, state=AttemptState.RUNNING)
        confirmed, reason = confirm_try(ti, ref.attempt, trigger)
        self._log_confirmation(hint, "confirmed" if confirmed else "discarded", reason)
        if not confirmed:
            return Confirmation(verdict="discarded", reason=reason)
        # Confirmed: the try failed terminally (FAILED) or before a retry (RETRYING, every_failure).
        state, final = attempt_state(ti, ref.attempt)
        current = _as_int(getattr(ti, "try_number", None), 0) == ref.attempt
        failure = Failure(
            ref=ref,
            state=state,
            is_final=final,
            source=hint.source if hint.source in EVENT_SOURCES else DEFAULT_EVENT_SOURCE,
            error_summary=hint.error_text,
            failed_at=parse_timestamp(getattr(ti, "end_date", None)) if current else None,
        )
        return Confirmation(verdict="confirmed", reason=reason, failures=(failure,))

    def _log_confirmation(self, hint: FailureHint, verdict: str, why: str) -> None:
        """One debug line per checked hint (ids, verdict and a reason or error type, never the error)."""
        ref = hint.ref
        logger.debug(
            "Airflow adapter %s: %s event of %s.%s (run %s, try %s): %s (%s)",
            self.instance,
            hint.source,
            ref.pipeline_id,
            ref.step_id,
            ref.run_id,
            ref.attempt,
            verdict,
            why,
        )

    # ------------------------------------------------------------------ RunInspector
    def describe(self, ref: AttemptRef, native: Mapping[str, Any]) -> AttemptDetail:
        """The task instance of ``ref`` (``get_task_instance``): state, operator and tries.

        The error is not part of it (``traceback_text`` stays empty): the traceback comes from
        the log, as in phase 5, so the prompts do not change. Raises
        :class:`~dag_healer.core.errors.NotFound` for an unknown task instance or a try Airflow
        never ran.
        """
        try:
            client = self._client_for("get_task_instance")
            ti = client.get_task_instance(ref.pipeline_id, ref.run_id, ref.step_id, int(ref.map_index))
        except Exception as exc:
            self._raise(exc, "describing a task instance")
        latest = _as_int(getattr(ti, "try_number", None), 0)
        if latest < ref.attempt:
            raise NotFound(
                f"Airflow knows only {latest} tries of task {ref.step_id} in run {ref.run_id} "
                f"of DAG {ref.pipeline_id}, not try {ref.attempt}"
            )
        state, final = attempt_state(ti, ref.attempt)
        current = latest == ref.attempt
        max_tries = getattr(ti, "max_tries", None)  # the retries: try_number > max_tries is the last try
        valid = isinstance(max_tries, int) and not isinstance(max_tries, bool)
        return AttemptDetail(
            ref=ref,
            state=state,
            is_final=final,
            error_name=None,
            error_message=None,
            executor=_opt_text(getattr(ti, "operator", None)),
            started_at=parse_timestamp(getattr(ti, "start_date", None)) if current else None,
            ended_at=parse_timestamp(getattr(ti, "end_date", None)) if current else None,
            max_attempts=max_tries + 1 if valid else None,
        )

    def logs(
        self, ref: AttemptRef, detail: AttemptDetail | None, *, max_chars: int = DEFAULT_LOG_MAX_CHARS
    ) -> LogBundle:
        """The rendered log of the try (``get_task_log``); never raises.

        A failed fetch (no such try, Airflow unreachable) gives an empty bundle with source and
        confidence ``none`` and the phase-5 note ``"Fetching the task log from Airflow failed:
        <Type>: <message>"`` (RAW and whole: the core masks the note, then cuts its reason to 500
        characters, as phase 5 did). An empty log is kept with the note ``"The Airflow log of
        try <n> is empty."``. A log longer than ``max_chars`` keeps its end (where the traceback
        is).
        """
        try:
            client = self._client_for("get_task_log")
            raw = client.get_task_log(
                ref.pipeline_id, ref.run_id, ref.step_id, ref.attempt, map_index=int(ref.map_index)
            )
        except Exception as exc:
            logger.debug(
                "Airflow adapter %s: no log for %s.%s (run %s, try %s): %s",
                self.instance,
                ref.pipeline_id,
                ref.step_id,
                ref.run_id,
                ref.attempt,
                type(exc).__name__,
            )
            return LogBundle(
                text="",
                source="none",
                confidence="none",
                notes=(f"Fetching the task log from Airflow failed: {type(exc).__name__}: {exc}",),
            )
        text = raw if isinstance(raw, str) else _text(raw)
        notes: list[str] = []
        if not text.strip():
            notes.append(f"The Airflow log of try {ref.attempt} is empty.")
        truncated = 0 < max_chars < len(text)
        if truncated:
            text = text[-max_chars:]
            notes.append(f"The Airflow log was cut to its last {max_chars} characters.")
        return LogBundle(
            text=text, source=LOG_SOURCE, confidence="exact", truncated=truncated, notes=tuple(notes)
        )

    def pipeline_source(
        self,
        pipeline_id: str,
        *,
        ref: AttemptRef | None = None,
        native: Mapping[str, Any] | None = None,
        include_text: bool = False,
    ) -> PipelineSource | None:
        """Where Airflow says DAG ``pipeline_id`` is (``get_dag``: ``fileloc``, ``relative_fileloc``).

        With ``include_text`` the source Airflow parsed is added (``get_dag_source``; ``None``
        when Airflow has none). Raises :class:`~dag_healer.core.errors.NotFound` for an unknown
        DAG; ``None`` when Airflow answers something that is not a DAG.

        A call without the text always reads the DAG and remembers the outcome (the DAG, or that
        it could not be read) for :data:`PIPELINE_MEMO_SECONDS`. The next call with the text
        consumes it instead of reading the DAG again, so one gathering issues exactly the phase-5
        calls: ``get_dag``, then ``get_dag_source`` when the core asks for the text. After a
        remembered failure that call returns the source alone (no ``path``) instead of raising.
        """
        if not include_text:
            dag = self._read_dag(pipeline_id, remember=True)
            return _dag_location(dag, None) if isinstance(dag, Mapping) else None
        remembered = self._recall_dag(pipeline_id)
        if remembered is _UNREADABLE:
            return PipelineSource(
                text=self._dag_source(pipeline_id), language="python", origin="api:dagSources"
            )
        dag = self._read_dag(pipeline_id, remember=False) if remembered is _NOTHING else remembered
        if not isinstance(dag, Mapping):
            return None
        return _dag_location(dag, self._dag_source(pipeline_id))

    def _read_dag(self, pipeline_id: str, *, remember: bool) -> Any:
        """``get_dag`` (core errors), remembering the outcome for the next text request."""
        try:
            client = self._client_for("get_dag")
            dag = client.get_dag(pipeline_id)
        except Exception as exc:
            if remember:
                self._remember_dag(pipeline_id, _UNREADABLE)
            self._raise(exc, "reading a DAG")
        if remember:
            self._remember_dag(pipeline_id, dag if isinstance(dag, Mapping) else _UNREADABLE)
        return dag

    def _dag_source(self, pipeline_id: str) -> str | None:
        """``get_dag_source`` (core errors); ``None`` when Airflow has no source."""
        try:
            client = self._client_for("get_dag_source")
            source = client.get_dag_source(pipeline_id)
        except Exception as exc:
            self._raise(exc, "reading a DAG source")
        return source if isinstance(source, str) else None

    def _remember_dag(self, pipeline_id: str, outcome: Any) -> None:
        with self._memo_lock:
            self._dags[pipeline_id] = (time.monotonic(), outcome)
            self._dags.move_to_end(pipeline_id)
            while len(self._dags) > MAX_PIPELINE_MEMOS:
                self._dags.popitem(last=False)

    def _recall_dag(self, pipeline_id: str) -> Any:
        """The remembered outcome of ``pipeline_id`` (consumed), ``_NOTHING`` when none is fresh."""
        with self._memo_lock:
            entry = self._dags.pop(pipeline_id, None)
        if entry is None or time.monotonic() - entry[0] > PIPELINE_MEMO_SECONDS:
            return _NOTHING
        return entry[1]

    # ------------------------------------------------------------------ Remediator
    def available(self, ref: AttemptRef, native: Mapping[str, Any]) -> tuple[RemediationKind, ...]:
        """No I/O: clear the task instance (``RETRY_STEP``), plus ``RERUN_PIPELINE`` when allowed."""
        if not ref.resolved or ref.step_id == RUN_LEVEL_STEP:
            return ()
        if self._allow_rerun:
            return (RemediationKind.RETRY_STEP, RemediationKind.RERUN_PIPELINE)
        return (RemediationKind.RETRY_STEP,)

    def remediate(
        self,
        ref: AttemptRef,
        native: Mapping[str, Any],
        kind: RemediationKind,
        *,
        idempotency_key: str,
        actor: str,
    ) -> RemediationResult:
        """Clear the task instance so Airflow runs it again (or trigger a new run, when allowed).

        ``RETRY_STEP`` -> ``clear_task_instances(dag, run, [task] or [(task, map_index)],
        only_failed=True)``; the detail is the phase-5 text "``<actor> cleared <task> in run
        <run> (N task instance(s)); Airflow will run it again.``" (the core adds the "not merged
        yet" sentence of a CODE incident). The same ``idempotency_key`` returns the first result
        without calling Airflow again. Raises :class:`~dag_healer.core.errors.NotSupported` for a
        kind :meth:`available` does not offer.
        """
        if kind not in self.available(ref, native):
            raise NotSupported(f"{kind.value} is not available for this Airflow incident")
        with self._remediation_lock:
            previous = self._results.get(idempotency_key)
            if previous is not None:
                self._results.move_to_end(idempotency_key)
                return previous
            if kind is RemediationKind.RETRY_STEP:
                result = self._retry_step(ref, actor)
            else:
                result = self._rerun_pipeline(ref, actor)
            self._results[idempotency_key] = result
            while len(self._results) > MAX_REMEMBERED_REMEDIATIONS:
                self._results.popitem(last=False)
        return result

    def _retry_step(self, ref: AttemptRef, actor: str) -> RemediationResult:
        map_index = int(ref.map_index)
        task_ids: list[Any] = [ref.step_id] if map_index < 0 else [(ref.step_id, map_index)]
        try:
            client = self._client_for("clear_task_instances")
            response = client.clear_task_instances(ref.pipeline_id, ref.run_id, task_ids, only_failed=True)
        except Exception as exc:
            self._raise(exc, "clearing a task instance")
        count = cleared_count(response if isinstance(response, Mapping) else {})
        detail = (
            f"{actor} cleared {ref.step_id} in run {ref.run_id}"
            + (f" ({count} task instance(s))" if count is not None else "")
            + "; Airflow will run it again."
        )
        logger.info(
            "Airflow adapter %s: cleared %s.%s in run %s (%s task instance(s))",
            self.instance,
            ref.pipeline_id,
            ref.step_id,
            ref.run_id,
            "?" if count is None else count,
        )
        return RemediationResult(
            kind=RemediationKind.RETRY_STEP, accepted=True, detail=detail, native={"cleared": count}
        )

    def _rerun_pipeline(self, ref: AttemptRef, actor: str) -> RemediationResult:
        try:
            client = self._client_for("trigger_dag_run")
            conf: Any = None
            if callable(getattr(client, "get_dag_run", None)):
                run = client.get_dag_run(ref.pipeline_id, ref.run_id)
                conf = run.get("conf") if isinstance(run, Mapping) else None
            response = client.trigger_dag_run(
                ref.pipeline_id, conf=conf if isinstance(conf, Mapping) else None
            )
        except Exception as exc:
            self._raise(exc, "triggering a DAG run")
        new_run = _opt_text(response.get("dag_run_id")) if isinstance(response, Mapping) else None
        new_ref = (
            AttemptRef(self.instance, ref.pipeline_id, new_run, ref.step_id, int(ref.map_index), 1)
            if new_run is not None
            else None
        )
        detail = (
            f"{actor} triggered run {new_run or '(unknown id)'} of DAG {ref.pipeline_id} "
            f"with the configuration of run {ref.run_id}."
        )
        logger.info("Airflow adapter %s: triggered a new run of %s", self.instance, ref.pipeline_id)
        return RemediationResult(
            kind=RemediationKind.RERUN_PIPELINE, accepted=True, detail=detail, new_ref=new_ref
        )

    # ------------------------------------------------------------------ the adapter
    def health(self) -> HealthReport:
        """``GET /api/v2/monitor/health`` and, with a username/password, a login. Never raises.

        ``ok`` is False when the API does not answer or the login fails; components that are
        not ``healthy`` are named in ``detail`` (the API still answers, so ``ok`` stays True).
        """
        base = self.base_url
        try:
            client = self._client_for("health")
            data = client.health()
        except Exception as exc:  # health never raises
            logger.debug("Airflow adapter %s: health check failed: %s", self.instance, type(exc).__name__)
            return HealthReport(
                ok=False,
                detail=f"The Airflow API at {base} is not reachable: {error_text(exc)}",
                checks={"api": "error"},
            )
        checks: dict[str, str] = {"api": "ok"}
        components: dict[str, str] = {}
        if isinstance(data, Mapping):
            for name, info in data.items():
                if isinstance(info, Mapping) and info.get("status"):
                    components[str(name)] = str(info["status"])
        checks.update(components)
        summary = ", ".join(f"{name}={status}" for name, status in components.items())
        detail = f"The Airflow API at {base} answers" + (f": {summary}" if summary else "")
        unhealthy = sorted(name for name, status in components.items() if status != "healthy")
        if unhealthy:
            detail += f" (not healthy: {', '.join(unhealthy)})"
        ok = True
        if getattr(client, "can_login", False) is True and callable(getattr(client, "login", None)):
            try:
                client.login()
            except Exception as exc:  # health never raises
                ok = False
                checks["auth"] = "error"
                detail += f"; the login failed: {error_text(exc)}"
            else:
                checks["auth"] = "ok"
        return HealthReport(ok=ok, detail=detail, checks=checks)

    def doctor_checks(self) -> list[DoctorCheck]:
        """The Airflow checks of ``dag-healer doctor`` (``check_airflow_api``, ``check_airflow_auth``,
        ``check_plugin_entry_point``: the same functions and output as today). No I/O here."""
        cfg = self._cfg
        results: dict[str, Any] = {}

        def api() -> Any:
            from dag_healer.cli.doctor_cmd import check_airflow_api

            results["api"] = check_airflow_api(cfg)
            return results["api"]

        def auth() -> Any:
            from dag_healer.cli.doctor_cmd import ERROR, check_airflow_api, check_airflow_auth

            api_result = results.get("api") or check_airflow_api(cfg)
            return check_airflow_auth(cfg, api_result.status != ERROR)

        def plugin() -> Any:
            from dag_healer.cli.doctor_cmd import check_plugin_entry_point

            return check_plugin_entry_point()

        return [
            DoctorCheck("airflow api", api),
            DoctorCheck("airflow auth", auth),
            DoctorCheck("airflow plugin", plugin),
        ]

    def sandbox_env(self) -> Mapping[str, str | None]:
        """The run-independent Airflow overrides of the pytest sandbox (:data:`SANDBOX_ENV`)."""
        return dict(SANDBOX_ENV)

    def close(self) -> None:
        """Close the REST client the adapter built (an injected one is the caller's). Idempotent."""
        with self._memo_lock:
            self._dags.clear()
        with self._lock:
            client, owned = self._client, self._owns_client
            if owned:
                self._client, self._owns_client = None, False
        if owned and client is not None:
            try:
                client.close()
            except Exception as exc:  # closing is best effort
                logger.debug("Airflow adapter %s: closing the REST client failed: %s", self.instance, exc)


# =========================================================================== the factory


class AirflowFactory:
    """:class:`~dag_healer.orchestrators.registry.AdapterFactory` of the ``airflow`` type."""

    type = AIRFLOW_TYPE
    display_name = DISPLAY_NAME
    options_model = None
    secret_options: tuple[str, ...] = ()

    def create(self, cfg: HealerConfig, *, client: Any | None = None) -> AirflowAdapter:
        """No I/O. ``client`` is an :class:`~dag_healer.airflow.client.AirflowClient` (or a phase-5
        double of one). When the configuration selects Airflow, ``orchestrator.options`` must be
        empty (:class:`~dag_healer.orchestrators.registry.AdapterOptionsError` otherwise)."""
        settings = orchestrator_settings(cfg)
        if settings.type in (AIRFLOW_TYPE, FACTORY_TARGET):
            parse_options(self, settings.options)
        return AirflowAdapter(cfg, client=client)


FACTORY = AirflowFactory()

__all__ = [
    "AIRFLOW_CAPABILITIES",
    "AIRFLOW_TYPE",
    "AIRFLOW_VOCABULARY",
    "DISPLAY_NAME",
    "EVENT_SOURCES",
    "FACTORY",
    "FACTORY_TARGET",
    "INITIAL_LOOKBACK",
    "LOG_SOURCE",
    "MIN_POLL_OVERLAP",
    "PIPELINE_MEMO_SECONDS",
    "POLL_SOURCE",
    "PUSH_CHANNEL",
    "SANDBOX_ENV",
    "AirflowAdapter",
    "AirflowCodeLocator",
    "AirflowFactory",
    "AirflowNativeLinks",
    "airflow_instance",
    "attempt_state",
    "clean_dags_dir",
    "cleared_count",
    "confirm_try",
    "cursor_from_state",
    "format_cursor_time",
    "parse_timestamp",
    "poll_cursor",
    "reset_cursor",
]
