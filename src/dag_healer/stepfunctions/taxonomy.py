"""The error taxonomy of AWS Step Functions (section 9.6 of the design).

The deterministic policy asks :meth:`~dag_healer.core.vocab.ErrorTaxonomy.kind_of` first:

* ``infra`` - the platform failed (timeouts, Lambda service errors, a step that never
  started): AMBIGUOUS;
* ``definition`` - a bug in the state machine definition (``States.Runtime`` ..., the errors of
  an ``EvaluationFailed`` event): AMBIGUOUS, a local pytest cannot reproduce it;
* ``data_shape`` - a missing or malformed Distributed Map input;
* ``aggregate`` - a summary of child failures (``States.ExceedToleratedFailureThreshold``);
* ``code`` - the function's package cannot even load (``Runtime.*`` [U names]).

Everything else (``KeyError``, ``ValueError`` ... raised by the function's own code) goes
through the generic data-shape and code gates. Non-Python Lambdas produce no repository
frames, so they never reach CODE.
"""

from __future__ import annotations

from dag_healer.core.vocab import ErrorTaxonomy

INFRA_ERRORS = frozenset(
    {
        "States.Timeout",
        "States.HeartbeatTimeout",
        "States.Permissions",
        "States.Http.Socket",
        "Lambda.ServiceException",
        "Lambda.SdkClientException",
        "Lambda.AWSLambdaException",
        "Lambda.TooManyRequestsException",
        "Lambda.Unknown",
        "Sandbox.Timedout",
        "ExecutionLimitExceeded",
    }
)
INFRA_EVENT_TYPES = frozenset(
    {
        "TaskStartFailed",
        "TaskSubmitFailed",
        "LambdaFunctionStartFailed",
        "LambdaFunctionScheduleFailed",
        "ActivityScheduleFailed",
        "ActivityTimedOut",
    }
)
DEFINITION_ERRORS = frozenset(
    {
        "States.Runtime",
        "States.DataLimitExceeded",
        "States.ResultPathMatchFailure",
        "States.ParameterPathFailure",
        "States.QueryEvaluationError",
        "States.NoChoiceMatched",
    }
)
DATA_SHAPE_ERRORS = frozenset({"States.ItemReaderFailed"})
AGGREGATE_ERRORS = frozenset({"States.ExceedToleratedFailureThreshold"})
#: [U] the Lambda runtime's own error names for a package that cannot load.
CODE_ERRORS = frozenset(
    {"Runtime.ImportModuleError", "Runtime.HandlerNotFound", "Runtime.UserCodeSyntaxError"}
)

SFN_TAXONOMY = ErrorTaxonomy(
    infra=INFRA_ERRORS,
    data_shape=DATA_SHAPE_ERRORS,
    code=CODE_ERRORS,
    definition=DEFINITION_ERRORS,
    aggregate=AGGREGATE_ERRORS,
    infra_event_types=INFRA_EVENT_TYPES,
)
"""The taxonomy of the ``stepfunctions`` adapter (``event_type`` = the step failure event type)."""

__all__ = [
    "AGGREGATE_ERRORS",
    "CODE_ERRORS",
    "DATA_SHAPE_ERRORS",
    "DEFINITION_ERRORS",
    "INFRA_ERRORS",
    "INFRA_EVENT_TYPES",
    "SFN_TAXONOMY",
]
