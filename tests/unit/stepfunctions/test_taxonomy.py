"""The error taxonomy of AWS Step Functions (``stepfunctions/taxonomy.py``, section 9.6 of the design)."""

from __future__ import annotations

import pytest

from dag_healer.core.vocab import ErrorTaxonomy
from dag_healer.stepfunctions.adapter import StepFunctionsAdapter
from dag_healer.stepfunctions.taxonomy import (
    AGGREGATE_ERRORS,
    CODE_ERRORS,
    DATA_SHAPE_ERRORS,
    DEFINITION_ERRORS,
    INFRA_ERRORS,
    INFRA_EVENT_TYPES,
    SFN_TAXONOMY,
)

#: The table of section 9.6, verbatim.
DESIGN_TABLE = {
    "infra": {
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
    },
    "definition": {
        "States.Runtime",
        "States.DataLimitExceeded",
        "States.ResultPathMatchFailure",
        "States.ParameterPathFailure",
        "States.QueryEvaluationError",
        "States.NoChoiceMatched",
    },
    "data_shape": {"States.ItemReaderFailed"},
    "aggregate": {"States.ExceedToleratedFailureThreshold"},
    "code": {"Runtime.ImportModuleError", "Runtime.HandlerNotFound", "Runtime.UserCodeSyntaxError"},
}
DESIGN_EVENT_TYPES = {
    "TaskStartFailed",
    "TaskSubmitFailed",
    "LambdaFunctionStartFailed",
    "LambdaFunctionScheduleFailed",
    "ActivityScheduleFailed",
    "ActivityTimedOut",
}


def test_the_sets_follow_the_design() -> None:
    assert {
        "infra": INFRA_ERRORS,
        "definition": DEFINITION_ERRORS,
        "data_shape": DATA_SHAPE_ERRORS,
        "aggregate": AGGREGATE_ERRORS,
        "code": CODE_ERRORS,
    } == DESIGN_TABLE
    assert INFRA_EVENT_TYPES == DESIGN_EVENT_TYPES
    assert isinstance(SFN_TAXONOMY, ErrorTaxonomy)
    assert StepFunctionsAdapter.taxonomy is SFN_TAXONOMY


def test_the_sets_are_disjoint() -> None:
    sets = list(DESIGN_TABLE.values())
    for index, first in enumerate(sets):
        for second in sets[index + 1 :]:
            assert not first & second


@pytest.mark.parametrize(
    ("error", "kind"),
    [(name, kind) for kind, names in sorted(DESIGN_TABLE.items()) for name in sorted(names)],
)
def test_every_listed_error(error: str, kind: str) -> None:
    assert SFN_TAXONOMY.kind_of(error) == kind
    assert SFN_TAXONOMY.kind_of(f"  {error} ") == kind


@pytest.mark.parametrize("event_type", sorted(DESIGN_EVENT_TYPES))
@pytest.mark.parametrize(
    "error", ["KeyError", "States.Runtime", None, "States.ExceedToleratedFailureThreshold"]
)
def test_an_infra_event_type_wins_over_the_error_name(event_type: str, error: str | None) -> None:
    assert SFN_TAXONOMY.kind_of(error, event_type=event_type) == "infra"


@pytest.mark.parametrize(
    ("error", "event_type", "kind"),
    [
        ("States.Runtime", "EvaluationFailed", "definition"),
        ("States.QueryEvaluationError", "EvaluationFailed", "definition"),
        ("States.ExceedToleratedFailureThreshold", "MapRunFailed", "aggregate"),
        ("States.Timeout", "TaskTimedOut", "infra"),
        ("States.Timeout", "ExecutionTimedOut", "infra"),
        ("Sandbox.Timedout", "TaskFailed", "infra"),
        ("States.Permissions", "TaskFailed", "infra"),
        ("Runtime.ImportModuleError", "LambdaFunctionFailed", "code"),
        ("KeyError", "TaskFailed", None),
        ("ValueError", "LambdaFunctionFailed", None),
        ("pandas.errors.ParserError", "TaskFailed", None),
        ("States.TaskFailed", "TaskFailed", None),
        ("States.ALL", None, None),
        ("Lambda.Unknownish", None, None),
        ("custom.Timeout", None, None),
        ("Error", "FailStateEntered", None),
        (None, "TaskFailed", None),
        ("", None, None),
    ],
)
def test_kind_of(error: str | None, event_type: str | None, kind: str | None) -> None:
    assert SFN_TAXONOMY.kind_of(error, event_type=event_type) == kind
