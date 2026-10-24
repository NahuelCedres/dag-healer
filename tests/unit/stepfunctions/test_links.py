"""Console deep links (``stepfunctions/links.py``, section 9.11 of the design).

One test per default template, the ``options.links`` overrides, ids that need quoting
(``:``, ``+``, ``/``, spaces) round-tripping through ``unquote``, the CloudWatch console
encoding of log groups, the fallbacks (no ``native``, no region, nothing known) and the
absence of I/O.
"""

from __future__ import annotations

from dataclasses import replace
from typing import Any
from urllib.parse import unquote, urlsplit

import pytest

from dag_healer.core.ids import AttemptRef
from dag_healer.core.ports import NativeLinks
from dag_healer.orchestrators.conformance import ODD_RUN_ID, assert_absolute_url
from dag_healer.stepfunctions.links import (
    CONSOLE_LABEL,
    LOGS_LABEL,
    SfnLinks,
    lambda_log_group,
    log_group_console_encoding,
    render_template,
)
from dag_healer.stepfunctions.runtime import native_ref

from .conftest import ACCOUNT, CUSTOMERS_SM, INSTANCE, INVENTORY_SM, REGION, sfn_options

EXECUTION = f"arn:aws:states:{REGION}:{ACCOUNT}:execution:customers-sync:customers-2026-10-02"
FUNCTION_ARN = f"arn:aws:lambda:{REGION}:{ACCOUNT}:function:sfn-etl-CustomersFunction-7G8H9I0J1K2L"
REF = AttemptRef(INSTANCE, "customers_sync", "customers-2026-10-02", "TransformCustomers", -1, 1)
NATIVE = native_ref(
    execution_arn=EXECUTION,
    state_machine_arn=CUSTOMERS_SM,
    status="FAILED",
    function_name=FUNCTION_ARN,
)
CONSOLE = f"https://{REGION}.console.aws.amazon.com"


def _links(make_runtime: Any, **overrides: Any) -> tuple[SfnLinks, list[str]]:
    """Links on a runtime whose clients record any attempt to build one (none may be built)."""
    built: list[str] = []

    def factory(service: str) -> Any:
        built.append(service)
        raise AssertionError(f"links built a {service} client")

    return SfnLinks(make_runtime(sfn_options(**overrides), factory=factory)), built


# =========================================================================== the default templates


def test_the_execution_page(make_runtime: Any) -> None:
    links, built = _links(make_runtime)
    found = links.links(REF, NATIVE)
    assert found[CONSOLE_LABEL] == (
        f"{CONSOLE}/states/home?region={REGION}#/v2/executions/details/{EXECUTION}"
    )
    assert built == []


def test_the_state_machine_page_when_no_execution_can_be_named(make_runtime: Any) -> None:
    links, _built = _links(make_runtime)
    found = links.links(replace(REF, run_id=""), {"state_machine_arn": CUSTOMERS_SM})
    assert found == {
        CONSOLE_LABEL: f"{CONSOLE}/states/home?region={REGION}#/statemachines/view/{CUSTOMERS_SM}"
    }


def test_the_log_group_page(make_runtime: Any) -> None:
    links, _built = _links(make_runtime)
    found = links.links(REF, NATIVE)
    assert found[LOGS_LABEL] == (
        f"https://{REGION}.console.aws.amazon.com/cloudwatch/home?region={REGION}"
        "#logsV2:log-groups/log-group/$252Faws$252Flambda$252Fsfn-etl-CustomersFunction-7G8H9I0J1K2L"
    )
    assert list(found) == [CONSOLE_LABEL, LOGS_LABEL]


def test_a_configured_log_group_wins_over_the_default_one(make_runtime: Any) -> None:
    links, _built = _links(make_runtime)
    found = links.links(REF, {**NATIVE, "log_group": "/sfn-etl/inventory"})
    assert found[LOGS_LABEL].endswith("#logsV2:log-groups/log-group/$252Fsfn-etl$252Finventory")


# =========================================================================== overrides


def test_templates_are_overridable(make_runtime: Any) -> None:
    links, _built = _links(
        make_runtime,
        links={
            "console_execution_url": "https://console.example.test/{partition}/{account}/{state_machine_name}/{execution_name}",
            "cloudwatch_log_url": "https://logs.example.test/{region}?group={log_group}",
        },
    )
    found = links.links(REF, NATIVE)
    assert found == {
        CONSOLE_LABEL: f"https://console.example.test/aws/{ACCOUNT}/customers-sync/customers-2026-10-02",
        LOGS_LABEL: (
            f"https://logs.example.test/{REGION}?group=%2Faws%2Flambda%2Fsfn-etl-CustomersFunction-7G8H9I0J1K2L"
        ),
    }


def test_an_overridden_state_machine_template(make_runtime: Any) -> None:
    links, _built = _links(
        make_runtime, links={"console_state_machine_url": "https://sfn.example.test/{state_machine_arn}"}
    )
    found = links.links(replace(REF, run_id=""), {"state_machine_arn": CUSTOMERS_SM})
    assert found == {CONSOLE_LABEL: f"https://sfn.example.test/{CUSTOMERS_SM}"}


def test_a_template_whose_value_is_unknown_is_left_out(make_runtime: Any) -> None:
    links, _built = _links(
        make_runtime,
        pipelines={},
        links={"cloudwatch_log_url": "https://logs.example.test/{account}/{log_group}"},
    )
    found = links.links(REF, {"function_name": "plain-function-name"})
    assert found == {}  # the account comes from an ARN, and no ARN is known here
    found = links.links(REF, {"function_name": "plain-function-name", "state_machine_arn": CUSTOMERS_SM})
    assert found[LOGS_LABEL] == f"https://logs.example.test/{ACCOUNT}/%2Faws%2Flambda%2Fplain-function-name"


# =========================================================================== ids that need quoting


def test_odd_ids_round_trip(make_runtime: Any) -> None:
    links, _built = _links(make_runtime)
    odd = replace(REF, run_id=ODD_RUN_ID)
    for native in ({}, {"state_machine_arn": CUSTOMERS_SM}):
        found = links.links(odd, native)
        assert found, native
        for url in found.values():
            assert_absolute_url(url)
            assert not any(character.isspace() for character in url)
            assert ODD_RUN_ID in unquote(url)
    url = links.links(odd, {})[CONSOLE_LABEL]
    assert "%2B00:00%20a%2Fb" in url, "':' stays, '+', ' ' and '/' are quoted"


def test_a_reused_execution_name_drops_its_suffix(make_runtime: Any) -> None:
    links, _built = _links(make_runtime)
    found = links.links(replace(REF, run_id="customers-2026-10-02@1790920800000"), {})
    assert found[CONSOLE_LABEL].endswith(f"#/v2/executions/details/{EXECUTION}")
    started = {"start_date": "2026-10-02T06:00:00.000+00:00"}  # the suffix is this start: a marker
    found = links.links(replace(REF, run_id="customers-2026-10-02@1790920800000"), started)
    assert found[CONSOLE_LABEL].endswith(f"#/v2/executions/details/{EXECUTION}")


def test_an_execution_name_ending_in_digits_after_an_at_sign_keeps_them(make_runtime: Any) -> None:
    """``@`` and digits are valid in a name: a suffix that is not the start date is the name."""
    links, _built = _links(make_runtime)
    native = {"start_date": "2026-10-02T06:30:00.000+00:00"}
    found = links.links(replace(REF, run_id="nightly@1790920800000"), native)
    assert unquote(found[CONSOLE_LABEL]).endswith(":execution:customers-sync:nightly@1790920800000")


def test_native_execution_arn_is_the_pointer(make_runtime: Any) -> None:
    links, _built = _links(make_runtime)
    found = links.links(replace(REF, run_id="something-else"), NATIVE)
    assert found[CONSOLE_LABEL].endswith(f"#/v2/executions/details/{EXECUTION}")


# =========================================================================== regions and fallbacks


def test_the_region_of_the_arn_without_a_configured_region(make_runtime: Any) -> None:
    links, _built = _links(make_runtime, region=None)
    other = EXECUTION.replace(REGION, "us-east-2")
    found = links.links(REF, {"execution_arn": other, "function_name": FUNCTION_ARN})
    assert found[CONSOLE_LABEL].startswith(
        "https://us-east-2.console.aws.amazon.com/states/home?region=us-east-2#"
    )
    assert found[LOGS_LABEL].startswith("https://us-east-2.console.aws.amazon.com/cloudwatch/")


def test_the_configured_region_wins(make_runtime: Any) -> None:
    links, _built = _links(make_runtime)
    found = links.links(REF, {"execution_arn": EXECUTION.replace(REGION, "us-east-2")})
    assert found[CONSOLE_LABEL].startswith(f"{CONSOLE}/states/home?region={REGION}#")


def test_the_region_of_the_function_when_nothing_else_names_one(make_runtime: Any) -> None:
    links, _built = _links(make_runtime, region=None, pipelines={})
    found = links.links(REF, {"function_name": FUNCTION_ARN})
    assert list(found) == [LOGS_LABEL]
    assert found[LOGS_LABEL].startswith(f"https://{REGION}.console.aws.amazon.com/cloudwatch/")


def test_the_execution_is_built_from_the_pipelines_known_arn(make_runtime: Any) -> None:
    links, _built = _links(make_runtime)
    ref = AttemptRef(INSTANCE, "inventory_report", "inventory-2026-10-02", "BuildReport", 0, 1)
    found = links.links(ref, {})
    expected = INVENTORY_SM.replace(":stateMachine:", ":execution:") + ":inventory-2026-10-02"
    assert found == {
        CONSOLE_LABEL: f"{CONSOLE}/states/home?region={REGION}#/v2/executions/details/{expected}"
    }


def test_nothing_known_gives_no_links_and_no_io(make_runtime: Any) -> None:
    links, built = _links(make_runtime, pipelines={"by_name": {"state_machine": "customers-sync"}})
    assert links.links(replace(REF, pipeline_id="by_name"), {}) == {}
    assert links.links(replace(REF, pipeline_id="unknown"), {}) == {}
    assert built == [], "a pipeline configured by name is not resolved by links()"


@pytest.mark.parametrize(
    "native",
    [
        None,
        {"execution_arn": "not-an-arn", "state_machine_arn": "arn:aws:lambda:x"},
        {"execution_arn": 42, "function_name": ["x"], "log_group": ""},
        {"function_name": "${CustomersFunctionArn}"},
        {"function_name": f"arn:aws:states:{REGION}:{ACCOUNT}:activity:nope"},
    ],
)
def test_odd_native_values_never_raise(make_runtime: Any, native: Any) -> None:
    links, _built = _links(make_runtime)
    found = links.links(REF, native)
    for url in found.values():
        assert_absolute_url(url)


def test_the_port_satisfies_the_protocol(make_runtime: Any) -> None:
    links, _built = _links(make_runtime)
    assert isinstance(links, NativeLinks)


def test_a_distributed_map_child_execution_links_to_itself(make_runtime: Any) -> None:
    links, _built = _links(make_runtime)
    child = f"arn:aws:states:{REGION}:{ACCOUNT}:execution:customers-sync/Items:0b6c2f"
    found = links.links(REF, {"execution_arn": child})
    assert found[CONSOLE_LABEL].endswith("#/v2/executions/details/" + child.replace("/", "%2F"))
    assert unquote(found[CONSOLE_LABEL]).endswith(child)
    assert urlsplit(found[CONSOLE_LABEL]).netloc == f"{REGION}.console.aws.amazon.com"


# =========================================================================== helpers


def test_render_template_quotes_every_value_but_the_encoded_ones() -> None:
    template = "https://x.test/{region}/{execution_arn}?g={log_group}#{log_group_enc}"
    values = {
        "region": "eu-west-1",
        "execution_arn": "arn:aws:states:eu-west-1:1:execution:m:a b+c/d",
        "log_group": "/aws/lambda/fn",
        "log_group_enc": "$252Faws$252Flambda$252Ffn",
    }
    assert render_template(template, values) == (
        "https://x.test/eu-west-1/arn:aws:states:eu-west-1:1:execution:m:a%20b%2Bc%2Fd"
        "?g=%2Faws%2Flambda%2Ffn#$252Faws$252Flambda$252Ffn"
    )


@pytest.mark.parametrize("values", [{}, {"region": None}, {"region": ""}])
def test_render_template_raises_key_error_for_a_missing_value(values: dict[str, Any]) -> None:
    with pytest.raises(KeyError, match="region"):
        render_template("https://{region}.example.test/", values)


def test_render_template_keeps_text_outside_placeholders() -> None:
    assert render_template("https://x.test/{a}/{ b }/{{c}}", {"a": "1", "c": "2"}) == (
        "https://x.test/1/{ b }/{2}"
    )


@pytest.mark.parametrize(
    ("log_group", "encoded"),
    [
        ("/aws/lambda/fn", "$252Faws$252Flambda$252Ffn"),
        ("/sfn-etl/inventory", "$252Fsfn-etl$252Finventory"),
        ("/aws/vendedlogs/states/x#1", "$252Faws$252Fvendedlogs$252Fstates$252Fx$25231"),
        ("plain_name.v2", "plain_name.v2"),
    ],
)
def test_log_group_console_encoding(log_group: str, encoded: str) -> None:
    assert log_group_console_encoding(log_group) == encoded
    assert unquote(unquote(encoded.replace("$", "%"))) == log_group


@pytest.mark.parametrize(
    ("function", "group"),
    [
        (FUNCTION_ARN, "/aws/lambda/sfn-etl-CustomersFunction-7G8H9I0J1K2L"),
        (f"{FUNCTION_ARN}:live", "/aws/lambda/sfn-etl-CustomersFunction-7G8H9I0J1K2L"),
        (f"{ACCOUNT}:function:my-fn", "/aws/lambda/my-fn"),
        ("my-fn:$LATEST", "/aws/lambda/my-fn"),
        ("my_fn", "/aws/lambda/my_fn"),
        ("${InventoryFunctionArn}", None),
        (f"arn:aws:states:{REGION}:{ACCOUNT}:stateMachine:x", None),
        ("", None),
    ],
)
def test_lambda_log_group(function: str, group: str | None) -> None:
    assert lambda_log_group(function) == group
