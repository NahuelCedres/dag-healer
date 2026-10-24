"""The IAM policy printer (``stepfunctions/iam.py``, section 9.10 of the design).

The read/write split, the resources of the configured machines, discovery, pipelines configured
by name, queue mode, the Lambda logs, S3 datasets of ``healer.yaml``, KMS keys, placeholders and
the JSON rendering.
"""

from __future__ import annotations

import json
import re
from collections.abc import Iterator
from pathlib import Path
from typing import Any

import pytest

from dag_healer import config as config_module
from dag_healer.config import HealerConfig, config_from_mapping, load_config
from dag_healer.orchestrators import registry
from dag_healer.stepfunctions import FACTORY
from dag_healer.stepfunctions.adapter import StepFunctionsAdapter
from dag_healer.stepfunctions.iam import (
    ACCOUNT_PLACEHOLDER,
    POLICY_VERSION,
    QUEUE_PLACEHOLDER,
    REGION_PLACEHOLDER,
    STATE_MACHINE_PLACEHOLDER,
    dataset_uris,
    iam_policies,
    queue_arn,
    render_iam_policy,
    s3_object_arn,
)

from .conftest import ACCOUNT, CUSTOMERS_SM, INVENTORY_SM, REGION, SALES_SM, SFN_REPO, sfn_options

QUEUE_URL = f"https://sqs.{REGION}.amazonaws.com/{ACCOUNT}/healer-sfn-events"
MACHINES = [SALES_SM, CUSTOMERS_SM, INVENTORY_SM]
EXECUTIONS = [
    f"arn:aws:states:{REGION}:{ACCOUNT}:execution:{name}:*"
    for name in ("SalesDailyEtlStateMachine-AbC123", "customers-sync", "InventoryReportStateMachine-XyZ789")
]
DATASETS = {
    "sales_daily_etl": {"TransformSales": {"type": "csv", "path": "s3://acme-landing/incoming/sales.csv"}},
    "customers_sync": {
        "TransformCustomers": {"type": "csv", "path": "s3://acme-landing/incoming/customers.csv"},
        "LoadCustomers": {"type": "csv", "path": "data/local/customers.csv"},
    },
    "inventory_report": {"BuildReport": {"type": "parquet", "path": "s3://acme-landing/inventory/day=2/"}},
    "not_watched": {"Step": {"type": "csv", "path": "s3://other-bucket/private.csv"}},
}


@pytest.fixture(autouse=True)
def _restore_shared_state(monkeypatch: pytest.MonkeyPatch) -> Iterator[None]:
    """Loading a configuration extends ``config.SECRET_FIELDS`` and may cache an adapter factory."""
    monkeypatch.setattr(config_module, "SECRET_FIELDS", config_module.SECRET_FIELDS)
    yield
    registry.reset_adapters()


def _statements(policy: dict[str, Any]) -> dict[str, dict[str, Any]]:
    assert policy["Version"] == POLICY_VERSION
    return {statement["Sid"]: statement for statement in policy["Statement"]}


def _config(tmp_path: Path, **sections: Any) -> HealerConfig:
    return config_from_mapping({"repo": {"path": str(tmp_path)}, **sections}, base_dir=tmp_path)


# =========================================================================== read and write


def test_the_default_policies(tmp_path: Path) -> None:
    policies = iam_policies(sfn_options())
    assert set(policies) == {"read", "write"}
    read = _statements(policies["read"])
    assert list(read) == ["ReadStateMachines", "ReadExecutions", "ReadLambdaLogs", "ReadLambdaConfiguration"]
    assert read["ReadStateMachines"]["Action"] == ["states:DescribeStateMachine", "states:ListExecutions"]
    assert read["ReadStateMachines"]["Resource"] == MACHINES
    assert read["ReadExecutions"]["Action"] == [
        "states:DescribeExecution",
        "states:DescribeStateMachineForExecution",
        "states:GetExecutionHistory",
    ]
    assert read["ReadExecutions"]["Resource"] == EXECUTIONS
    assert read["ReadLambdaLogs"] == {
        "Sid": "ReadLambdaLogs",
        "Effect": "Allow",
        "Action": ["logs:FilterLogEvents"],
        "Resource": [f"arn:aws:logs:{REGION}:{ACCOUNT}:log-group:/aws/lambda/*"],
    }
    assert read["ReadLambdaConfiguration"]["Action"] == ["lambda:GetFunction"]
    assert read["ReadLambdaConfiguration"]["Resource"] == [f"arn:aws:lambda:{REGION}:{ACCOUNT}:function:*"]
    write = _statements(policies["write"])
    assert write == {
        "RedriveExecutions": {
            "Sid": "RedriveExecutions",
            "Effect": "Allow",
            "Action": ["states:RedriveExecution"],
            "Resource": EXECUTIONS,
        }
    }


def test_the_write_role_follows_the_remediations() -> None:
    both = _statements(iam_policies(sfn_options(rerun=True))["write"])
    assert list(both) == ["RedriveExecutions", "StartExecutions"]
    assert both["StartExecutions"]["Action"] == ["states:StartExecution"]
    assert both["StartExecutions"]["Resource"] == MACHINES
    rerun_only = _statements(iam_policies(sfn_options(retry_strategy="disabled", rerun=True))["write"])
    assert list(rerun_only) == ["StartExecutions"]
    assert "write" not in iam_policies(sfn_options(retry_strategy="disabled"))


def test_no_write_action_is_in_the_read_role() -> None:
    read = iam_policies(sfn_options(rerun=True))["read"]
    actions = {action for statement in read["Statement"] for action in statement["Action"]}
    assert not actions & {"states:RedriveExecution", "states:StartExecution", "states:StopExecution"}


# =========================================================================== machines


def test_discover_widens_the_machines_and_lists_them() -> None:
    read = _statements(iam_policies(sfn_options(discover=True))["read"])
    assert read["ListStateMachines"] == {
        "Sid": "ListStateMachines",
        "Effect": "Allow",
        "Action": ["states:ListStateMachines"],
        "Resource": "*",
    }
    assert read["ReadStateMachines"]["Resource"] == [f"arn:aws:states:{REGION}:{ACCOUNT}:stateMachine:*"]
    assert read["ReadExecutions"]["Resource"] == [f"arn:aws:states:{REGION}:{ACCOUNT}:execution:*"]


def test_discover_keeps_machines_of_another_account() -> None:
    other = "arn:aws:states:eu-west-1:210987654321:stateMachine:shared-etl"
    options = sfn_options(
        discover=True, pipelines={"own": {"state_machine": SALES_SM}, "shared": {"state_machine": other}}
    )
    read = _statements(iam_policies(options)["read"])
    assert read["ReadStateMachines"]["Resource"] == [
        f"arn:aws:states:{REGION}:{ACCOUNT}:stateMachine:*",
        other,
    ]
    assert (
        read["ReadExecutions"]["Resource"][-1]
        == "arn:aws:states:eu-west-1:210987654321:execution:shared-etl:*"
    )


def test_a_pipeline_configured_by_name_needs_list_state_machines() -> None:
    options = sfn_options(
        region="eu-central-1", pipelines={"customers_sync": {"state_machine": "customers-sync"}}
    )
    read = _statements(iam_policies(options, account="111122223333")["read"])
    assert read["ListStateMachines"]["Resource"] == "*"
    assert read["ReadStateMachines"]["Resource"] == [
        "arn:aws:states:eu-central-1:111122223333:stateMachine:customers-sync"
    ]
    assert read["ReadExecutions"]["Resource"] == [
        "arn:aws:states:eu-central-1:111122223333:execution:customers-sync:*"
    ]


def test_placeholders_when_nothing_names_the_account_or_region() -> None:
    options = sfn_options(region=None, pipelines={})
    policies = iam_policies(options)
    read = _statements(policies["read"])
    machine = (
        f"arn:aws:states:{REGION_PLACEHOLDER}:{ACCOUNT_PLACEHOLDER}:stateMachine:{STATE_MACHINE_PLACEHOLDER}"
    )
    assert read["ReadStateMachines"]["Resource"] == [machine]
    assert "ListStateMachines" not in read
    assert _statements(policies["write"])["RedriveExecutions"]["Resource"] == [
        f"arn:aws:states:{REGION_PLACEHOLDER}:{ACCOUNT_PLACEHOLDER}:execution:{STATE_MACHINE_PLACEHOLDER}:*"
    ]


def test_explicit_account_and_region_fill_in_what_the_arns_do_not_name() -> None:
    options = sfn_options(region=None, queue_url=QUEUE_URL.replace(f"/{ACCOUNT}/", "/x/"))
    read = _statements(iam_policies(options, account="111122223333", region="us-east-2")["read"])
    assert read["ReadStateMachines"]["Resource"] == MACHINES, "configured ARNs keep their own scope"
    assert read["ReadLambdaLogs"]["Resource"] == [
        "arn:aws:logs:us-east-2:111122223333:log-group:/aws/lambda/*"
    ]
    assert read["ReadEventQueue"]["Resource"] == [f"arn:aws:sqs:{REGION}:111122223333:healer-sfn-events"]


def test_the_region_and_account_default_to_the_configured_arns() -> None:
    read = _statements(iam_policies(sfn_options(region=None))["read"])
    assert read["ReadLambdaLogs"]["Resource"] == [f"arn:aws:logs:{REGION}:{ACCOUNT}:log-group:/aws/lambda/*"]


def test_the_china_partition() -> None:
    options = sfn_options(region="cn-north-1", pipelines={"p": {"state_machine": "etl"}})
    read = _statements(iam_policies(options, account="111122223333")["read"])
    assert read["ReadStateMachines"]["Resource"] == [
        "arn:aws-cn:states:cn-north-1:111122223333:stateMachine:etl"
    ]


# =========================================================================== optional statements


def test_queue_mode_adds_the_sqs_actions_on_the_queue() -> None:
    read = _statements(iam_policies(sfn_options(queue_url=QUEUE_URL))["read"])
    assert read["ReadEventQueue"] == {
        "Sid": "ReadEventQueue",
        "Effect": "Allow",
        "Action": [
            "sqs:ChangeMessageVisibility",
            "sqs:DeleteMessage",
            "sqs:GetQueueAttributes",
            "sqs:ReceiveMessage",
        ],
        "Resource": [f"arn:aws:sqs:{REGION}:{ACCOUNT}:healer-sfn-events"],
    }


@pytest.mark.parametrize(
    ("url", "arn"),
    [
        (QUEUE_URL, f"arn:aws:sqs:{REGION}:{ACCOUNT}:healer-sfn-events"),
        (f"https://{REGION}.queue.amazonaws.com/{ACCOUNT}/legacy", f"arn:aws:sqs:{REGION}:{ACCOUNT}:legacy"),
        (
            f"https://vpce-0a1b2c3d-4e5f.sqs.us-east-2.vpce.amazonaws.com/{ACCOUNT}/private",
            f"arn:aws:sqs:us-east-2:{ACCOUNT}:private",
        ),
        ("http://localhost:4566/000000000000/local-queue", "arn:aws:sqs:eu-south-1:000000000000:local-queue"),
        ("https://queue.example.test/just-a-name", "arn:aws:sqs:eu-south-1:999988887777:just-a-name"),
        ("https://queue.example.test/", f"arn:aws:sqs:eu-south-1:999988887777:{QUEUE_PLACEHOLDER}"),
    ],
)
def test_queue_arn(url: str, arn: str) -> None:
    assert queue_arn(url, region="eu-south-1", account="999988887777") == arn


def test_log_lookup_off_drops_the_logs_statements() -> None:
    read = _statements(iam_policies(sfn_options(log_lookup=False))["read"])
    assert "ReadLambdaLogs" not in read and "ReadLambdaConfiguration" not in read
    read = _statements(iam_policies(sfn_options(log_lookup=False), log_groups=["/sfn-etl/inventory"])["read"])
    assert "ReadLambdaLogs" not in read


def test_custom_log_groups_are_added_to_the_lambda_logs() -> None:
    groups = ["/sfn-etl/inventory", "/aws/lambda/shared", " ", "has space", "/sfn-etl/inventory", "audit"]
    read = _statements(iam_policies(sfn_options(), log_groups=groups)["read"])
    assert read["ReadLambdaLogs"]["Resource"] == [
        f"arn:aws:logs:{REGION}:{ACCOUNT}:log-group:/aws/lambda/*",
        f"arn:aws:logs:{REGION}:{ACCOUNT}:log-group:/sfn-etl/inventory",
        f"arn:aws:logs:{REGION}:{ACCOUNT}:log-group:/sfn-etl/inventory:*",
        f"arn:aws:logs:{REGION}:{ACCOUNT}:log-group:audit",
        f"arn:aws:logs:{REGION}:{ACCOUNT}:log-group:audit:*",
    ]
    assert json.loads(render_iam_policy(sfn_options(), log_groups=groups)) == iam_policies(
        sfn_options(), log_groups=groups
    )


def test_s3_datasets_of_the_watched_pipelines(tmp_path: Path) -> None:
    cfg = _config(tmp_path, datasets=DATASETS)
    read = _statements(iam_policies(sfn_options(), cfg=cfg)["read"])
    assert read["ReadDatasets"] == {
        "Sid": "ReadDatasets",
        "Effect": "Allow",
        "Action": ["s3:GetObject", "s3:GetObjectVersion"],
        "Resource": [
            "arn:aws:s3:::acme-landing/incoming/sales.csv",
            "arn:aws:s3:::acme-landing/incoming/customers.csv",
            "arn:aws:s3:::acme-landing/inventory/day=2/*",
        ],
    }
    discovered = _statements(iam_policies(sfn_options(discover=True), cfg=cfg)["read"])
    assert discovered["ReadDatasets"]["Resource"][-1] == "arn:aws:s3:::other-bucket/private.csv"


def test_no_dataset_statement_without_s3_datasets(tmp_path: Path) -> None:
    local = {"sales_daily_etl": {"TransformSales": {"type": "csv", "path": "data/sales.csv"}}}
    assert "ReadDatasets" not in _statements(iam_policies(sfn_options(), cfg=_config(tmp_path))["read"])
    cfg = _config(tmp_path, datasets=local)
    assert "ReadDatasets" not in _statements(iam_policies(sfn_options(), cfg=cfg)["read"])


def test_dataset_uris_accepts_plain_mappings() -> None:
    class Cfg:
        datasets = {
            "sales_daily_etl": {
                "A": {"type": "csv", "path": " s3://b/k.csv "},
                "B": {"type": "csv", "path": "s3://b/k.csv"},
                "C": "not a locator",
            },
            "customers_sync": "not a mapping",
        }

    assert dataset_uris(Cfg(), sfn_options()) == ["s3://b/k.csv"]
    assert dataset_uris(None, sfn_options()) == []


@pytest.mark.parametrize(
    ("uri", "arn"),
    [
        ("s3://bucket/key.csv", "arn:aws:s3:::bucket/key.csv"),
        ("S3://bucket/a/b/c.parquet", "arn:aws:s3:::bucket/a/b/c.parquet"),
        ("s3://bucket/prefix/", "arn:aws:s3:::bucket/prefix/*"),
        ("s3://bucket", None),
        ("s3://bucket/", None),
        ("s3:///key", None),
        ("data/local.csv", None),
    ],
)
def test_s3_object_arn(uri: str, arn: str | None) -> None:
    assert s3_object_arn(uri) == arn


def test_kms_keys_add_decrypt() -> None:
    key = f"arn:aws:kms:{REGION}:{ACCOUNT}:key/1234abcd-12ab-34cd-56ef-1234567890ab"
    read = _statements(iam_policies(sfn_options(), kms_key_arns=[key, "  ", key, "5678efgh"])["read"])
    assert read["DecryptExecutionData"] == {
        "Sid": "DecryptExecutionData",
        "Effect": "Allow",
        "Action": ["kms:Decrypt"],
        "Resource": [key, f"arn:aws:kms:{REGION}:{ACCOUNT}:key/5678efgh"],
    }


# =========================================================================== shape and rendering


def test_every_statement_is_well_formed(tmp_path: Path) -> None:
    options = sfn_options(rerun=True, discover=True, queue_url=QUEUE_URL)
    policies = iam_policies(options, cfg=_config(tmp_path, datasets=DATASETS), kms_key_arns=["k"])
    for policy in policies.values():
        sids = [statement["Sid"] for statement in policy["Statement"]]
        assert len(sids) == len(set(sids))
        for statement in policy["Statement"]:
            assert re.fullmatch(r"[A-Za-z0-9]+", statement["Sid"])
            assert statement["Effect"] == "Allow"
            assert statement["Action"] == sorted(statement["Action"]) and statement["Action"]
            resource = statement["Resource"]
            assert resource == "*" or (
                isinstance(resource, list) and resource and len(resource) == len(set(resource))
            )


def test_render_iam_policy_is_stable_json(tmp_path: Path) -> None:
    cfg = _config(tmp_path, datasets=DATASETS)
    text = render_iam_policy(sfn_options(queue_url=QUEUE_URL), cfg=cfg, kms_key_arns=["k"])
    assert json.loads(text) == iam_policies(sfn_options(queue_url=QUEUE_URL), cfg=cfg, kms_key_arns=["k"])
    assert text == render_iam_policy(sfn_options(queue_url=QUEUE_URL), cfg=cfg, kms_key_arns=["k"])
    assert text.startswith('{\n  "read": {\n    "Version": "2012-10-17",')


def test_the_adapter_prints_the_policies_of_its_options_and_configuration(tmp_path: Path) -> None:
    cfg = _config(tmp_path, datasets=DATASETS)
    adapter = StepFunctionsAdapter(cfg, options=sfn_options(region=None), clients=lambda service: None)
    printed = json.loads(adapter.iam_policy(account="111122223333"))
    assert printed == iam_policies(adapter.options, cfg=cfg, account="111122223333")
    read = _statements(printed["read"])
    assert read["ReadLambdaLogs"]["Resource"] == [
        f"arn:aws:logs:{REGION}:111122223333:log-group:/aws/lambda/*"
    ]
    assert "ReadDatasets" in read


def test_the_policies_of_the_fixture_repository() -> None:
    """``doctor --print-iam-policy`` for ``tests/unit/fixtures/repos/sfn_etl/healer.yaml``."""
    cfg = load_config(SFN_REPO / "healer.yaml")
    adapter = FACTORY.create(cfg, client=lambda service: None)
    policies = json.loads(adapter.iam_policy())
    read = _statements(policies["read"])
    assert read["ReadStateMachines"]["Resource"] == MACHINES
    assert read["ReadExecutions"]["Resource"] == EXECUTIONS
    assert read["ReadEventQueue"]["Resource"] == [f"arn:aws:sqs:{REGION}:{ACCOUNT}:healer-sfn-events"]
    assert read["ReadLambdaLogs"]["Resource"] == [  # InventoryFunction logs to its own group
        f"arn:aws:logs:{REGION}:{ACCOUNT}:log-group:/aws/lambda/*",
        f"arn:aws:logs:{REGION}:{ACCOUNT}:log-group:/sfn-etl/inventory",
        f"arn:aws:logs:{REGION}:{ACCOUNT}:log-group:/sfn-etl/inventory:*",
    ]
    assert read["ReadDatasets"]["Resource"] == [
        f"arn:aws:s3:::acme-landing/incoming/{name}.csv" for name in ("sales", "customers", "inventory")
    ]
    assert list(_statements(policies["write"])) == ["RedriveExecutions"]
