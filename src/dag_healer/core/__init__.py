"""The orchestrator-neutral core types and ports of dag-healer (stdlib only).

Everything an orchestrator adapter exchanges with the core is defined here:

* :mod:`~dag_healer.core.ids` - neutral identifiers (:class:`AttemptRef`, the incident key);
* :mod:`~dag_healer.core.failures` - confirmed failures, push hints, confirmations;
* :mod:`~dag_healer.core.evidence` - error details, logs and pipeline definitions;
* :mod:`~dag_healer.core.specs` - the static code model (``TaskInfo``/``DagInfo``);
* :mod:`~dag_healer.core.vocab` - vocabulary, capabilities and the error taxonomy;
* :mod:`~dag_healer.core.errors` - the only exceptions adapters raise;
* :mod:`~dag_healer.core.tracebacks` - the CPython traceback interchange format;
* :mod:`~dag_healer.core.ports` - the ports (protocols) and the adapter bundle.

The package imports only the standard library (no pydantic, httpx, boto3, airflow or
pandas), so importing it is cheap and safe in every process. See
``docs/architecture/orchestrators.md``.
"""

from __future__ import annotations

from dag_healer.core.errors import NotFound, NotSupported, OrchestratorError, PermissionDenied
from dag_healer.core.evidence import AttemptDetail, LogBundle, PipelineSource
from dag_healer.core.failures import AttemptState, Confirmation, Failure, FailureHint, FailurePage, Trigger
from dag_healer.core.ids import (
    DEFAULT_ORCHESTRATOR,
    NO_MAP_INDEX,
    RUN_LEVEL_STEP,
    UNRESOLVED_STEP,
    AttemptRef,
    PipelineRef,
    RunRef,
)
from dag_healer.core.ports import (
    CodeLocator,
    DoctorCheck,
    FailureFeed,
    HealthReport,
    HintQueue,
    NativeLinks,
    OrchestratorAdapter,
    PushIntake,
    QueuedMessage,
    RemediationResult,
    Remediator,
    RunInspector,
)
from dag_healer.core.specs import DagInfo, DatasetRef, PathMapping, PipelineSpec, StepSpec, TaskInfo
from dag_healer.core.tracebacks import (
    CAUSE_SEPARATOR,
    CONTEXT_SEPARATOR,
    TRACEBACK_HEADER,
    render_python_traceback,
)
from dag_healer.core.vocab import (
    Capabilities,
    ErrorKind,
    ErrorTaxonomy,
    RemediationKind,
    Vocabulary,
    render_vocabulary,
)

__all__ = [
    "CAUSE_SEPARATOR",
    "CONTEXT_SEPARATOR",
    "DEFAULT_ORCHESTRATOR",
    "NO_MAP_INDEX",
    "RUN_LEVEL_STEP",
    "TRACEBACK_HEADER",
    "UNRESOLVED_STEP",
    "AttemptDetail",
    "AttemptRef",
    "AttemptState",
    "Capabilities",
    "CodeLocator",
    "Confirmation",
    "DagInfo",
    "DatasetRef",
    "DoctorCheck",
    "ErrorKind",
    "ErrorTaxonomy",
    "Failure",
    "FailureFeed",
    "FailureHint",
    "FailurePage",
    "HealthReport",
    "HintQueue",
    "LogBundle",
    "NativeLinks",
    "NotFound",
    "NotSupported",
    "OrchestratorAdapter",
    "OrchestratorError",
    "PathMapping",
    "PermissionDenied",
    "PipelineRef",
    "PipelineSource",
    "PipelineSpec",
    "PushIntake",
    "QueuedMessage",
    "RemediationKind",
    "RemediationResult",
    "Remediator",
    "RunInspector",
    "RunRef",
    "StepSpec",
    "TaskInfo",
    "Trigger",
    "Vocabulary",
    "render_python_traceback",
    "render_vocabulary",
]
