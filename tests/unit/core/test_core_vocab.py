"""Tests for dag_healer.core.vocab (vocabulary, capabilities, error taxonomy)."""

from __future__ import annotations

import dataclasses
import json

import pytest

from dag_healer.core.vocab import (
    Capabilities,
    ErrorTaxonomy,
    RemediationKind,
    Vocabulary,
    render_vocabulary,
)

FAKEFLOW = Vocabulary(
    product="Fakeflow",
    pipeline="flow",
    pipelines="flows",
    run="run",
    step="step",
    steps="steps",
    attempt="attempt",
    retry_action="Retry step",
)


def test_default_vocabulary_is_todays_airflow_wording() -> None:
    assert Vocabulary().as_dict() == {
        "product": "Airflow",
        "pipeline": "DAG",
        "pipelines": "DAGs",
        "run": "DAG run",
        "step": "task",
        "steps": "tasks",
        "attempt": "try",
        "map_index": "map index",
        "retry_action": "Retry task",
    }
    with pytest.raises(dataclasses.FrozenInstanceError):
        Vocabulary().product = "x"  # type: ignore[misc]


def test_render_vocabulary_replaces_only_known_tokens() -> None:
    template = (
        "The {{product}} {{step}} of the {{pipeline}} failed ({{attempt}} 2); "
        'answer as JSON {"category": "data", "nested": {"a": 1}}; keep {{unknown}}, '
        "{{ product }} and {single}. {{retry_action}}! {{pipelines}} / {{steps}} / {{run}} / {{map_index}}"
    )

    airflow = render_vocabulary(template, Vocabulary())
    fake = render_vocabulary(template, FAKEFLOW)

    assert airflow.startswith("The Airflow task of the DAG failed (try 2); ")
    assert airflow.endswith("Retry task! DAGs / tasks / DAG run / map index")
    assert fake.startswith("The Fakeflow step of the flow failed (attempt 2); ")
    assert fake.endswith("Retry step! flows / steps / run / map index")
    for text in (airflow, fake):
        assert '{"category": "data", "nested": {"a": 1}}' in text
        assert "{{unknown}}" in text and "{{ product }}" in text and "{single}" in text


def test_render_vocabulary_without_tokens_is_the_identity() -> None:
    text = json.dumps({"a": {"b": [1, 2]}, "c": "{x}"})
    assert render_vocabulary(text, FAKEFLOW) is text
    assert render_vocabulary("", FAKEFLOW) == ""


def test_remediation_kinds_and_capability_defaults() -> None:
    assert [kind.value for kind in RemediationKind] == ["retry_step", "resume_run", "rerun_pipeline"]
    caps = Capabilities(sources=("rest",))
    assert caps.push_channels == () and caps.remediations == ()
    assert caps.attempts_visible is True and caps.native_authz is False
    assert (caps.fix_delivery, caps.ui_hosts) == ("repo_sync", ("standalone",))


TAXONOMY = ErrorTaxonomy(
    infra=frozenset({"States.Timeout", "AirflowTaskTimeout"}),
    data_shape=frozenset({"States.ItemReaderFailed"}),
    code=frozenset({"Runtime.ImportModuleError"}),
    definition=frozenset({"States.Runtime"}),
    aggregate=frozenset({"States.ExceedToleratedFailureThreshold"}),
    infra_event_types=frozenset({"TaskStartFailed"}),
)


@pytest.mark.parametrize(
    ("error_name", "event_type", "expected"),
    [
        ("States.Timeout", None, "infra"),
        ("States.Runtime", None, "definition"),
        ("States.Runtime", "EvaluationFailed", "definition"),
        ("States.ExceedToleratedFailureThreshold", None, "aggregate"),
        ("States.ItemReaderFailed", None, "data_shape"),
        ("Runtime.ImportModuleError", None, "code"),
        ("KeyError", None, None),
        ("KeyError", "TaskStartFailed", "infra"),  # the step never started
        (None, "TaskStartFailed", "infra"),
        (None, None, None),
        ("", None, None),
        ("airflow.exceptions.AirflowTaskTimeout", None, "infra"),  # qualified Python name
        ("Lambda.Timeout", None, None),  # "Timeout" alone is in no set
        ("  States.Timeout ", None, "infra"),
    ],
)
def test_error_taxonomy_kind_of(error_name: str | None, event_type: str | None, expected: str | None) -> None:
    assert TAXONOMY.kind_of(error_name, event_type=event_type) == expected


def test_empty_taxonomy_never_decides() -> None:
    assert ErrorTaxonomy().kind_of("States.Timeout", event_type="TaskStartFailed") is None
