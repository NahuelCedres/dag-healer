"""Live test of the bedrock provider against the real Amazon Bedrock (opt-in, run by hand).

It is skipped unless ``HEALER_BEDROCK_TEST_MODEL`` names a Bedrock model or inference profile ID,
and ``tests/live`` is outside the default ``testpaths`` (``tests/unit``), so neither a plain
``pytest`` run nor CI ever collects it. Two tests, in your own AWS account:

* ``dag-healer doctor``'s llm check for the bedrock provider: the region, the model ID and that the
  AWS credentials resolve (no model call);
* one classification of a small synthetic incident (a ``ValueError`` in
  ``src/etl/customers.py::parse_signup_date``, an approved contract, no violation) through
  ``classify_incident`` at ``effort: low``: one InvokeModel request (two or three only when the
  model does not apply structured outputs), about 2,000 to 4,000 tokens billed to the AWS account.

From the repository root, with the region and the credentials of the usual AWS settings (the
instance role, ``AWS_PROFILE=<profile>`` or ``AWS_BEARER_TOKEN_BEDROCK``), which the isolation
of ``tests/conftest.py`` keeps from every test outside ``tests/live`` and ``tests/e2e``::

    HEALER_BEDROCK_TEST_MODEL=eu.anthropic.claude-opus-5-5 AWS_REGION=eu-west-1 \\
        .venv/bin/python -m pytest -q -s -p no:cacheprovider tests/live/test_bedrock_live.py

``HEALER_BEDROCK_TEST_ENDPOINT_URL`` (optional) becomes ``llm.endpoint_url`` (a VPC endpoint).
The output shows the doctor line, then the model, the region, whether structured outputs were
applied, the category and the tokens; never a credential.
"""

from __future__ import annotations

import os
from typing import Any

import pytest

from dag_healer.agent.bedrock import BedrockProvider
from dag_healer.agent.classify import classify_incident
from dag_healer.agent.context import IncidentContext
from dag_healer.config import HealerConfig, config_from_mapping

MODEL_ENV = "HEALER_BEDROCK_TEST_MODEL"
ENDPOINT_ENV = "HEALER_BEDROCK_TEST_ENDPOINT_URL"

pytestmark = pytest.mark.skipif(
    not os.environ.get(MODEL_ENV, "").strip(),
    reason="set HEALER_BEDROCK_TEST_MODEL (a Bedrock model or inference profile ID) to run the live "
    "Amazon Bedrock test",
)

REPO = "/opt/airflow/repo"
SITE = "/home/airflow/.local/lib/python3.13/site-packages"


@pytest.fixture
def cfg(monkeypatch: pytest.MonkeyPatch) -> HealerConfig:
    """``llm.provider: bedrock`` with the model under test; the shell's provider choice is ignored."""
    monkeypatch.delenv("HEALER_LLM_PROVIDER", raising=False)
    llm: dict[str, Any] = {
        "provider": "bedrock",
        "model": os.environ[MODEL_ENV].strip(),
        "effort": "low",
        "max_tokens": 4096,
        "fallbacks": False,
    }
    endpoint = os.environ.get(ENDPOINT_ENV, "").strip()
    if endpoint:
        llm["endpoint_url"] = endpoint
    return config_from_mapping({"llm": llm})


def customers_incident() -> IncidentContext:
    """A CODE failure: ISO-8601 timestamps reach a parser that expects plain dates."""
    traceback = (
        "Traceback (most recent call last):\n"
        f'  File "{REPO}/src/etl/customers.py", line 36, in transform_customers\n'
        f'  File "{SITE}/pandas/core/series.py", line 5084, in apply\n'
        f'  File "{REPO}/src/etl/customers.py", line 19, in parse_signup_date\n'
        '  File "/usr/local/lib/python3.13/_strptime.py", line 558, in _strptime\n'
        "ValueError: unconverted data remains: T08:30:00Z"
    )
    return IncidentContext(
        incident_id="0123456789abcdef0123456789abcdef",
        dag_id="customers_sync",
        run_id="manual__2026-10-26T08:30:00+00:00",
        task_id="transform",
        error_text=traceback,
        exception_type="ValueError",
        exception_message="unconverted data remains: T08:30:00Z",
        traceback_text=traceback,
        repo_frames=[
            {"file": "src/etl/customers.py", "line": 36, "function": "transform_customers"},
            {"file": "src/etl/customers.py", "line": 19, "function": "parse_signup_date"},
        ],
        code_snippets={
            "src/etl/customers.py": (
                "def parse_signup_date(value):\n"
                '    return datetime.strptime(value.strip(), "%Y-%m-%d").date()\n'
            )
        },
        contract_status="approved",
    )


def test_doctor_bedrock_check_is_ok(cfg: HealerConfig) -> None:
    from dag_healer.cli.doctor_cmd import OK, check_llm

    result = check_llm(cfg)
    print(f"\ndoctor llm: {result.detail}")
    assert result.status == OK, result.detail


def test_one_classification_through_bedrock(cfg: HealerConfig) -> None:
    provider = BedrockProvider(cfg)
    outcome = classify_incident(cfg, customers_incident(), provider)
    region = getattr(provider._client, "aws_region", None) or provider.region
    category = outcome.classification.category if outcome.classification is not None else None
    print(
        f"\nmodel {provider.model}, region {region}, structured outputs {provider.structured_outputs}, "
        f"category {category}, tokens {outcome.tokens_used}"
    )
    assert outcome.llm_error is None, outcome.llm_error
    assert outcome.classification is not None
    assert outcome.tokens_used > 0
