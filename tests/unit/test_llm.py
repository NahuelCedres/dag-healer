"""Unit tests for dag_healer.agent.llm, dag_healer.agent.fake and dag_healer.agent.budget.

The Anthropic provider is exercised end to end through the real SDK with an
``httpx2.MockTransport`` (no network): the tests assert the request bodies and the handling
of refusals, truncation, API errors, tool calls, ``pause_turn`` and the token budget.
"""

from __future__ import annotations

import json
import os
import shutil
import subprocess
import sys
from collections.abc import Callable
from pathlib import Path
from typing import Any

import anthropic
import httpx2
import pytest

from dag_healer.agent import llm as llm_module
from dag_healer.agent.budget import TokenBudget, usage_tokens
from dag_healer.agent.fake import (
    FAKE_DIR_ENV,
    NO_SCRIPT_TEXT,
    FakeProvider,
    extract_signals,
    fake_classification,
    load_scripts,
    match_script,
)
from dag_healer.agent.llm import (
    FALLBACK_BETA,
    AnthropicProvider,
    Classification,
    LLMRefusal,
    LLMUnavailable,
    RepairSessionResult,
    get_provider,
    tool_call_record,
)
from dag_healer.config import HealerConfig

PROJECT_ROOT = Path(__file__).resolve().parents[2]
EXAMPLE_REPO = PROJECT_ROOT / "example_existing_repo"
FAKE_SCRIPTS = EXAMPLE_REPO / "fixtures" / "fake_llm"
CODE_SCENARIO = {"dag_id": "customers_sync", "task_id": "transform", "exception_type": "ValueError"}

CLASSIFICATION_JSON = json.dumps(
    {
        "category": "code",
        "confidence": 0.88,
        "diagnosis": "parse_signup_date rejects ISO-8601 timestamps.",
        "violated_rule": None,
        "suspected_file": "src/etl/customers.py",
        "suspected_symbol": "parse_signup_date",
        "evidence": ["ValueError: unconverted data remains"],
        "fix_summary": "Parse with datetime.fromisoformat.",
    }
)


# --------------------------------------------------------------------------- helpers


def message(
    content: list[dict[str, Any]],
    *,
    stop_reason: str = "end_turn",
    usage: dict[str, Any] | None = None,
    **extra: Any,
) -> dict[str, Any]:
    return {
        "id": "msg_test",
        "type": "message",
        "role": "assistant",
        "model": "claude-opus-5-5",
        "content": content,
        "stop_reason": stop_reason,
        "stop_sequence": None,
        "usage": usage or {"input_tokens": 100, "output_tokens": 20},
        **extra,
    }


class FakeAPI:
    """Serves queued responses through an httpx2.MockTransport and records the requests."""

    def __init__(self, *responses: dict[str, Any] | tuple[int, dict[str, Any]]) -> None:
        self.responses = list(responses)
        self.requests: list[httpx2.Request] = []

    def handler(self, request: httpx2.Request) -> httpx2.Response:
        self.requests.append(request)
        if not self.responses:
            raise AssertionError("unexpected extra request")
        item = self.responses.pop(0)
        if isinstance(item, tuple):
            status, body = item
            return httpx2.Response(status, json=body)
        return httpx2.Response(200, json=item)

    def client(self) -> anthropic.Anthropic:
        return anthropic.Anthropic(
            api_key="test-key",
            http_client=httpx2.Client(transport=httpx2.MockTransport(self.handler)),
            max_retries=0,
        )

    def body(self, index: int = 0) -> dict[str, Any]:
        return json.loads(self.requests[index].content)


def provider(api: FakeAPI, **kwargs: Any) -> AnthropicProvider:
    return AnthropicProvider(HealerConfig(), client=api.client(), **kwargs)


def assert_common_request(api: FakeAPI, index: int = 0, *, fallbacks: bool = True) -> dict[str, Any]:
    body = api.body(index)
    request = api.requests[index]
    assert body["model"] == "claude-opus-5-5"
    assert body["max_tokens"] == 16000
    assert body["output_config"]["effort"] == "high"
    for forbidden in ("thinking", "tool_choice", "temperature", "top_p", "top_k"):
        assert forbidden not in body
    assert body["system"][0]["type"] == "text"
    assert body["system"][0]["cache_control"] == {"type": "ephemeral"}
    assert body["messages"][0]["role"] == "user"
    # The SDK's ``?beta=true`` query is not checked: anthropic 2.3 stopped sending it.
    if fallbacks:
        assert request.headers["anthropic-beta"] == FALLBACK_BETA
        assert body["fallbacks"] == "default"
    else:
        assert "anthropic-beta" not in request.headers
        assert "fallbacks" not in body
    assert body["messages"][-1]["role"] in ("user", "assistant")
    return body


# --------------------------------------------------------------------------- models and budget


def test_classification_clamps_confidence_and_has_an_enum_category() -> None:
    assert Classification(category="data", confidence=1.7, diagnosis="d").confidence == 1.0
    assert Classification(category="data", confidence=-2, diagnosis="d").confidence == 0.0
    assert Classification(category="data", confidence=float("nan"), diagnosis="d").confidence == 0.0
    schema = Classification.model_json_schema()
    assert schema["properties"]["category"]["enum"] == ["data", "code", "ambiguous"]
    with pytest.raises(ValueError):
        Classification(category="other", confidence=0.5, diagnosis="d")


def test_token_budget() -> None:
    budget = TokenBudget(1000)
    assert (budget.used, budget.remaining, budget.exhausted) == (0, 1000, False)
    budget.add(400, 100)
    assert (budget.used, budget.remaining, budget.exhausted) == (500, 500, False)
    budget.add_usage({"input_tokens": 300, "cache_read_input_tokens": 150, "output_tokens": 60})
    assert budget.used == 1010 and budget.remaining == 0 and budget.exhausted
    assert TokenBudget(0).exhausted
    with pytest.raises(ValueError):
        TokenBudget(-1)
    assert usage_tokens(None) == (0, 0)
    usage = anthropic.types.Usage(
        input_tokens=10, output_tokens=5, cache_creation_input_tokens=7, cache_read_input_tokens=3
    )
    assert usage_tokens(usage) == (20, 5)


def test_tool_call_record_masks_and_truncates() -> None:
    record = tool_call_record(
        "run_tests",
        {"content": "x" * 5000, "paths": ["a"]},
        "user olivia@example.com " + "y" * 3000,
        is_error=False,
    )
    assert record.input["content"].endswith("...[truncated]")
    assert record.input["paths"] == ["a"]
    assert "olivia@example.com" not in record.output_excerpt
    assert len(record.output_excerpt) <= 2000


# --------------------------------------------------------------------------- get_provider


def test_get_provider_selection(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("HEALER_LLM_PROVIDER", raising=False)
    cfg = HealerConfig()
    assert isinstance(get_provider(cfg), AnthropicProvider)
    cfg.llm.provider = "fake"
    assert isinstance(get_provider(cfg), FakeProvider)
    cfg.llm.provider = "anthropic"
    monkeypatch.setenv("HEALER_LLM_PROVIDER", "fake")
    assert isinstance(get_provider(cfg), FakeProvider)
    monkeypatch.setenv("HEALER_LLM_PROVIDER", "Anthropic")
    assert get_provider(cfg).name == "anthropic"
    monkeypatch.setenv("HEALER_LLM_PROVIDER", "openai")
    with pytest.raises(ValueError, match="Unknown LLM provider"):
        get_provider(cfg)


def test_provider_settings_come_from_the_config() -> None:
    cfg = HealerConfig()
    cfg.llm.model = "claude-sonnet-5-5"
    cfg.llm.effort = "medium"
    cfg.llm.max_tokens = 4000
    cfg.llm.fallbacks = False
    prov = AnthropicProvider(cfg)
    assert (prov.model, prov.effort, prov.max_tokens, prov.fallbacks) == (
        "claude-sonnet-5-5",
        "medium",
        4000,
        False,
    )
    assert HealerConfig().llm.fallbacks is True


def test_missing_api_key_raises_llm_unavailable_at_call_time(monkeypatch: pytest.MonkeyPatch) -> None:
    for name in ("ANTHROPIC_API_KEY", "ANTHROPIC_AUTH_TOKEN"):
        monkeypatch.delenv(name, raising=False)
    prov = get_provider(HealerConfig())
    with pytest.raises(LLMUnavailable, match="ANTHROPIC_API_KEY"):
        prov.classify(system="s", prompt="p")
    with pytest.raises(LLMUnavailable, match="ANTHROPIC_API_KEY"):
        prov.repair_session(system="s", prompt="p", tools=[], max_tokens_budget=10, scenario={})


# --------------------------------------------------------------------------- Anthropic classify


def test_classify_request_shape_with_fallbacks() -> None:
    api = FakeAPI(
        message(
            [{"type": "text", "text": CLASSIFICATION_JSON}],
            usage={
                "input_tokens": 1200,
                "output_tokens": 150,
                "cache_creation_input_tokens": 900,
                "cache_read_input_tokens": 0,
            },
        )
    )
    result, tokens = provider(api).classify(system="SYSTEM PROMPT", prompt="PROMPT <signals>{}</signals>")
    assert isinstance(result, Classification)
    assert (result.category, result.confidence, result.suspected_symbol) == (
        "code",
        0.88,
        "parse_signup_date",
    )
    assert tokens == 1200 + 150 + 900
    body = assert_common_request(api)
    assert body["system"][0]["text"] == "SYSTEM PROMPT"
    assert body["messages"] == [{"role": "user", "content": "PROMPT <signals>{}</signals>"}]
    output_format = body["output_config"]["format"]
    assert output_format["type"] == "json_schema"
    assert set(output_format["schema"]["properties"]) >= {"category", "confidence", "diagnosis", "evidence"}
    assert output_format["schema"]["additionalProperties"] is False


def test_classify_request_shape_without_fallbacks() -> None:
    api = FakeAPI(message([{"type": "text", "text": CLASSIFICATION_JSON}]))
    result, tokens = provider(api, fallbacks=False).classify(system="S", prompt="P")
    assert result.category == "code"
    assert tokens == 120
    assert api.requests[0].url.path == "/v1/messages"
    assert_common_request(api, fallbacks=False)


def test_classify_handles_fallback_blocks() -> None:
    api = FakeAPI(
        message(
            [
                {
                    "type": "fallback",
                    "from": {"model": "claude-opus-5-5"},
                    "to": {"model": "claude-opus-4-8"},
                    "trigger": {"type": "refusal", "category": "cyber"},
                },
                {"type": "text", "text": CLASSIFICATION_JSON},
            ],
            model="claude-opus-4-8",
            usage={
                "input_tokens": 50,
                "output_tokens": 10,
                "iterations": [{"type": "fallback_message", "input_tokens": 50, "output_tokens": 10}],
            },
        )
    )
    result, tokens = provider(api).classify(system="S", prompt="P")
    assert result.category == "code"
    assert tokens == 60


@pytest.mark.parametrize("fallbacks", [True, False])
def test_classify_refusal_before_any_output(fallbacks: bool) -> None:
    api = FakeAPI(
        message(
            [],
            stop_reason="refusal",
            stop_details={"type": "refusal", "category": "cyber", "explanation": "declined"},
        )
    )
    with pytest.raises(LLMRefusal) as info:
        provider(api, fallbacks=fallbacks).classify(system="S", prompt="P")
    assert info.value.category == "cyber"
    assert "cyber" in str(info.value)


def test_classify_refusal_with_partial_output_keeps_the_category() -> None:
    api = FakeAPI(
        message(
            [{"type": "text", "text": '{"category": "co'}],
            stop_reason="refusal",
            stop_details={"type": "refusal", "category": "bio", "explanation": None},
        )
    )
    with pytest.raises(LLMRefusal) as info:
        provider(api).classify(system="S", prompt="P")
    assert info.value.category == "bio"


@pytest.mark.parametrize("fallbacks", [True, False])
def test_classify_truncated_output_is_unavailable(fallbacks: bool) -> None:
    api = FakeAPI(
        message([{"type": "text", "text": '{"category": "code", "confid'}], stop_reason="max_tokens")
    )
    with pytest.raises(LLMUnavailable):
        provider(api, fallbacks=fallbacks).classify(system="S", prompt="P")


def test_classify_max_tokens_with_valid_json_is_still_unavailable() -> None:
    api = FakeAPI(message([{"type": "text", "text": CLASSIFICATION_JSON}], stop_reason="max_tokens"))
    with pytest.raises(LLMUnavailable, match="max_tokens"):
        provider(api).classify(system="S", prompt="P")


@pytest.mark.parametrize(
    ("status", "pattern"),
    [
        (401, "credentials"),
        (403, "not allowed"),
        (404, "llm.model"),
        (429, "rate limit"),
        (500, "500"),
        (529, "529"),
    ],
)
def test_classify_api_errors_become_llm_unavailable(status: int, pattern: str) -> None:
    api = FakeAPI((status, {"type": "error", "error": {"type": "api_error", "message": "boom"}}))
    with pytest.raises(LLMUnavailable, match=pattern):
        provider(api).classify(system="S", prompt="P")


def test_classify_connection_errors_become_llm_unavailable() -> None:
    def handler(request: httpx2.Request) -> httpx2.Response:
        raise httpx2.ConnectError("connection refused", request=request)

    client = anthropic.Anthropic(
        api_key="k", http_client=httpx2.Client(transport=httpx2.MockTransport(handler)), max_retries=0
    )
    with pytest.raises(LLMUnavailable, match="Cannot reach"):
        AnthropicProvider(HealerConfig(), client=client).classify(system="S", prompt="P")


# --------------------------------------------------------------------------- Anthropic repair session


def make_tools(calls: list[tuple[str, dict[str, Any]]]) -> list[Callable[..., str]]:
    def read_file(path: str, start_line: int = 1, end_line: int = 400) -> str:
        """Read a file of the repository.

        Args:
            path: Repository-relative path.
            start_line: First line to return.
            end_line: Last line to return.
        """
        calls.append(("read_file", {"path": path, "start_line": start_line, "end_line": end_line}))
        return f"contents of {path}"

    def run_tests(paths: list[str] | None = None) -> str:
        """Run pytest.

        Args:
            paths: Test files to run (all tests when omitted).
        """
        calls.append(("run_tests", {"paths": paths}))
        raise RuntimeError("pytest crashed")

    return [read_file, run_tests]


def tool_use(tool_id: str, name: str, tool_input: dict[str, Any]) -> dict[str, Any]:
    return {"type": "tool_use", "id": tool_id, "name": name, "input": tool_input}


def test_repair_session_runs_tools_and_records_them() -> None:
    api = FakeAPI(
        message(
            [
                {"type": "text", "text": "Let me read the module."},
                tool_use("toolu_1", "read_file", {"path": "src/etl/customers.py"}),
                tool_use("toolu_2", "run_tests", {"paths": ["tests/test_x.py"]}),
            ],
            stop_reason="tool_use",
            usage={"input_tokens": 1000, "output_tokens": 100, "cache_read_input_tokens": 500},
        ),
        message(
            [{"type": "text", "text": "Done: the fix is in place."}],
            usage={"input_tokens": 1500, "output_tokens": 50},
        ),
    )
    calls: list[tuple[str, dict[str, Any]]] = []
    result = provider(api).repair_session(
        system="REPAIR SYSTEM",
        prompt="Fix it",
        tools=make_tools(calls),
        max_tokens_budget=100_000,
        scenario={"phase": "fix", "dag_id": "customers_sync", "task_id": "transform"},
    )
    assert isinstance(result, RepairSessionResult)
    assert [name for name, _ in calls] == ["read_file", "run_tests"]
    assert result.final_text == "Done: the fix is in place."
    assert result.stop_reason == "end_turn"
    assert (result.input_tokens, result.output_tokens, result.budget_exhausted) == (3000, 150, False)
    assert [record.name for record in result.tool_calls] == ["read_file", "run_tests"]
    first, second = result.tool_calls
    assert first.input == {"path": "src/etl/customers.py"}
    assert first.output_excerpt == "contents of src/etl/customers.py"
    assert first.is_error is False
    assert second.is_error is True and "pytest crashed" in second.output_excerpt

    assert len(api.requests) == 2
    body = assert_common_request(api, 0)
    assert body["system"][0]["text"] == "REPAIR SYSTEM"
    tools = {tool["name"]: tool for tool in body["tools"]}
    assert set(tools) == {"read_file", "run_tests"}
    assert tools["read_file"]["description"].startswith("Read a file of the repository.")
    assert tools["read_file"]["input_schema"]["required"] == ["path"]
    assert (
        tools["read_file"]["input_schema"]["properties"]["path"]["description"] == "Repository-relative path."
    )
    followup = assert_common_request(api, 1)
    results = {block["tool_use_id"]: block for block in followup["messages"][-1]["content"]}
    assert results["toolu_1"]["content"] == "contents of src/etl/customers.py"
    assert results["toolu_2"]["is_error"] is True


def test_repair_session_resumes_pause_turn() -> None:
    api = FakeAPI(
        message([{"type": "text", "text": "Working..."}], stop_reason="pause_turn"),
        message([{"type": "text", "text": "Finished."}]),
    )
    result = provider(api).repair_session(
        system="S", prompt="P", tools=make_tools([]), max_tokens_budget=10_000, scenario={"phase": "test"}
    )
    assert result.final_text == "Finished."
    assert result.tool_calls == []
    assert len(api.requests) == 2
    resumed = api.body(1)["messages"]
    assert resumed[-1]["role"] == "assistant"
    assert resumed[-1]["content"][0]["text"] == "Working..."


def test_repair_session_turns_server_held_threads_off_only_where_the_sdk_has_them() -> None:
    def runner_without_threads(*, tools: Any, max_iterations: int, **params: Any) -> None: ...

    def runner_with_threads(*, tools: Any, max_iterations: int, thread: Any = None, **kw: Any) -> None: ...

    assert llm_module._unthreaded(runner_without_threads) == {}
    assert llm_module._unthreaded(runner_with_threads) == {"thread": False}


def test_repair_session_stops_when_the_budget_is_exhausted() -> None:
    api = FakeAPI(
        message(
            [tool_use("toolu_1", "read_file", {"path": "a.py"})],
            stop_reason="tool_use",
            usage={"input_tokens": 900, "output_tokens": 200},
        )
    )
    calls: list[tuple[str, dict[str, Any]]] = []
    result = provider(api).repair_session(
        system="S", prompt="P", tools=make_tools(calls), max_tokens_budget=1000, scenario={}
    )
    assert result.budget_exhausted is True
    assert result.stop_reason == "tool_use"
    assert calls == []  # the pending tool call is not executed
    assert result.tool_calls == []
    assert len(api.requests) == 1
    assert result.tokens_used == 1100


def test_repair_session_message_cap(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(llm_module, "MAX_RUNNER_MESSAGES", 2)
    api = FakeAPI(
        message([tool_use("toolu_1", "read_file", {"path": "a.py"})], stop_reason="tool_use"),
        message([tool_use("toolu_2", "read_file", {"path": "b.py"})], stop_reason="tool_use"),
    )
    calls: list[tuple[str, dict[str, Any]]] = []
    result = provider(api).repair_session(
        system="S", prompt="P", tools=make_tools(calls), max_tokens_budget=1_000_000, scenario={}
    )
    assert [args["path"] for _, args in calls] == ["a.py"]
    assert len(result.tool_calls) == 1
    assert len(api.requests) == 2
    assert result.budget_exhausted is False


def test_repair_session_refusal_stops_the_loop() -> None:
    api = FakeAPI(
        message(
            [],
            stop_reason="refusal",
            stop_details={"type": "refusal", "category": "cyber", "explanation": None},
        )
    )
    result = provider(api).repair_session(
        system="S", prompt="P", tools=make_tools([]), max_tokens_budget=10_000, scenario={}
    )
    assert result.stop_reason == "refusal"
    assert result.tool_calls == []


def test_repair_session_api_error_keeps_the_partial_result() -> None:
    api = FakeAPI(
        message([tool_use("toolu_1", "read_file", {"path": "a.py"})], stop_reason="tool_use"),
        (500, {"type": "error", "error": {"type": "api_error", "message": "overloaded"}}),
    )
    with pytest.raises(LLMUnavailable) as info:
        provider(api).repair_session(
            system="S", prompt="P", tools=make_tools([]), max_tokens_budget=10_000, scenario={}
        )
    partial = info.value.partial
    assert partial is not None
    assert [record.name for record in partial.tool_calls] == ["read_file"]
    assert partial.tokens_used == 120


def test_repair_session_without_fallbacks_uses_the_beta_runner_without_betas() -> None:
    api = FakeAPI(message([{"type": "text", "text": "ok"}]))
    provider(api, fallbacks=False).repair_session(
        system="S", prompt="P", tools=make_tools([]), max_tokens_budget=10_000, scenario={}
    )
    body = api.body(0)
    assert "fallbacks" not in body
    assert FALLBACK_BETA not in api.requests[0].headers.get("anthropic-beta", "")


# --------------------------------------------------------------------------- fake provider: classify


def prompt_with(signals: dict[str, Any]) -> str:
    return f"Classify.\n<incident>{{}}</incident>\n<signals>\n{json.dumps(signals)}\n</signals>\n"


@pytest.mark.parametrize(
    ("category", "rule", "confidence"),
    [
        ("data", "a_contract_violated", 0.9),
        ("code", "b_code_with_valid_data", 0.9),
        ("ambiguous", "c_no_approved_contract", 0.5),
        ("ambiguous", "infra", 0.5),
        ("ambiguous", "llm", 0.5),
    ],
)
def test_fake_classify_follows_the_policy_hint(category: str, rule: str, confidence: float) -> None:
    signals = {
        "dag_id": "d",
        "task_id": "t",
        "contract_status": "approved",
        "violation_rules": ["missing_column"],
        "violation_details": ["missing_column: sales.amount"],
        "exception_type": "KeyError",
        "exception_message": "'amount'",
        "top_repo_frame": {"file": "src/etl/sales.py", "line": 26, "function": "transform_sales"},
        "policy_hint": {"category": category, "rule": rule, "reason": "because", "llm_decides": False},
    }
    result, tokens = FakeProvider().classify(system="s", prompt=prompt_with(signals))
    assert (result.category, result.confidence, tokens) == (category, confidence, 0)
    assert "d.t" in result.diagnosis
    assert result.suspected_file == "src/etl/sales.py"
    if category == "data":
        assert result.violated_rule == "missing_column: sales.amount"
    if category == "code":
        assert result.fix_summary and "transform_sales" in result.fix_summary


def test_fake_classify_needs_a_signals_block() -> None:
    with pytest.raises(LLMUnavailable, match="no <signals>"):
        FakeProvider().classify(system="s", prompt="no block here")
    with pytest.raises(LLMUnavailable, match="not valid JSON"):
        FakeProvider().classify(system="s", prompt="<signals>{oops</signals>")
    with pytest.raises(LLMUnavailable, match="JSON object"):
        extract_signals("<signals>[1, 2]</signals>")


def test_fake_classify_uses_the_last_signals_block_and_masks() -> None:
    signals = {
        "exception_type": "ValueError",
        "exception_message": "bad row for olivia@example.com",
        "policy_hint": {"category": "code"},
    }
    prompt = '<signals>{"policy_hint": {"category": "data"}}</signals>\n' + prompt_with(signals)
    result, _ = FakeProvider().classify(system="s", prompt=prompt)
    assert result.category == "code"
    assert "olivia@example.com" not in result.diagnosis
    assert fake_classification({}).category == "ambiguous"


# --------------------------------------------------------------------------- fake provider: scripts


def write_script(directory: Path, name: str, body: str) -> None:
    directory.mkdir(parents=True, exist_ok=True)
    (directory / name).write_text(body, encoding="utf-8")


def test_script_matching(tmp_path: Path) -> None:
    write_script(tmp_path, "a_generic.yaml", "match: {dag_id: d}\nsteps: []\n")
    write_script(tmp_path, "b_specific.yaml", "match: {dag_id: d, task_id: t, phase: test}\nsteps: []\n")
    write_script(
        tmp_path, "c_list.yml", "match: {dag_id: [x, y], exception_type: builtins.KeyError}\nsteps: []\n"
    )
    write_script(tmp_path, "d_broken.yaml", "match: [nope]\nsteps: []\n")
    write_script(tmp_path, "e_invalid.yaml", "match: {dag_id: d\n")
    scripts = load_scripts(tmp_path)
    assert [script["name"] for script in scripts] == ["a_generic", "b_specific", "c_list"]
    assert match_script(scripts, {"dag_id": "d", "task_id": "t", "phase": "test"})["name"] == "b_specific"
    assert match_script(scripts, {"dag_id": "d", "task_id": "t", "phase": "fix"})["name"] == "a_generic"
    assert match_script(scripts, {"dag_id": "y", "exception_type": "KeyError"})["name"] == "c_list"
    assert match_script(scripts, {"dag_id": "y", "exception_type": "ValueError"}) is None
    assert match_script(scripts, {"dag_id": "z"}) is None
    assert load_scripts(tmp_path / "missing") == []


def test_repair_session_replays_steps(tmp_path: Path) -> None:
    write_script(
        tmp_path,
        "s.yaml",
        """
match: {dag_id: d, phase: fix}
steps:
  - tool: write_file
    input: {path: src/a.py, content: "x = 1\\n"}
  - tool: nope
    input: {}
  - tool: boom
    input: {value: 3}
  - tool: write_file
    input: {path: .env, content: "SECRET"}
final_text: All done.
""",
    )
    calls: list[dict[str, Any]] = []

    def write_file(path: str, content: str) -> str:
        """Write a file."""
        calls.append({"path": path, "content": content})
        return "Error: path not allowed" if path.startswith(".env") else f"Wrote {path}"

    def boom(value: int) -> str:
        """Always fails."""
        raise ValueError(f"bad value {value}")

    result = FakeProvider(script_dir=tmp_path).repair_session(
        system="s",
        prompt="p",
        tools=[write_file, boom],
        max_tokens_budget=10,
        scenario={"dag_id": "d", "phase": "fix"},
    )
    assert result.final_text == "All done."
    assert result.stop_reason == "end_turn"
    assert (result.input_tokens, result.output_tokens, result.budget_exhausted) == (0, 0, False)
    assert [(r.name, r.is_error) for r in result.tool_calls] == [
        ("write_file", False),
        ("nope", True),
        ("boom", True),
        ("write_file", True),
    ]
    assert "unknown tool" in result.tool_calls[1].output_excerpt
    assert "bad value 3" in result.tool_calls[2].output_excerpt
    assert calls[0] == {"path": "src/a.py", "content": "x = 1\n"}


def test_repair_session_without_a_matching_script(tmp_path: Path) -> None:
    called: list[str] = []

    def write_file(path: str, content: str) -> str:
        """Write a file."""
        called.append(path)
        return "ok"

    result = FakeProvider(script_dir=tmp_path).repair_session(
        system="s",
        prompt="p",
        tools=[write_file],
        max_tokens_budget=10,
        scenario={"dag_id": "other", "phase": "test"},
    )
    assert result.final_text == NO_SCRIPT_TEXT
    assert result.tool_calls == [] and called == []


def test_script_directory_resolution(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv(FAKE_DIR_ENV, raising=False)
    cfg = HealerConfig()
    cfg.repo.path = tmp_path
    assert FakeProvider(cfg).script_dir() == tmp_path.resolve() / "fixtures" / "fake_llm"
    assert FakeProvider(cfg, script_dir=tmp_path / "x").script_dir() == tmp_path / "x"
    monkeypatch.setenv(FAKE_DIR_ENV, str(tmp_path / "env"))
    assert FakeProvider(cfg, script_dir=tmp_path / "x").script_dir() == tmp_path / "env"
    assert FakeProvider().script_dir() == tmp_path / "env"


def test_demo_scripts_match_the_code_scenario() -> None:
    scripts = load_scripts(FAKE_SCRIPTS)
    assert {script["name"] for script in scripts} >= {
        "customers_sync_test",
        "customers_sync_fix",
        "inventory_report_test",
        "inventory_report_fix",
    }
    test_script = match_script(scripts, {**CODE_SCENARIO, "phase": "test", "incident_id": "abc"})
    fix_script = match_script(scripts, {**CODE_SCENARIO, "phase": "fix", "incident_id": "abc"})
    assert test_script is not None and test_script["name"] == "customers_sync_test"
    assert fix_script is not None and fix_script["name"] == "customers_sync_fix"
    assert match_script(scripts, {**CODE_SCENARIO, "exception_type": "KeyError", "phase": "test"}) is None
    written = [step["input"]["path"] for step in test_script["steps"] if step["tool"] == "write_file"]
    assert written == ["tests/test_customers_signup_date.py"]
    replaced = {step["input"]["path"] for step in fix_script["steps"] if step["tool"] == "replace_in_file"}
    assert replaced == {"src/etl/customers.py"}


# --------------------------------------------------------------------------- demo scripts, red then green


class SimpleRepoTools:
    """Minimal stand-in for agent.repair.RepairTools, enough to replay the demo scripts."""

    def __init__(self, root: Path) -> None:
        self.root = root
        self.pytest_exit_codes: list[int] = []

    def functions(self) -> list[Callable[..., str]]:
        root = self.root

        def list_files(path: str = ".", pattern: str = "*") -> str:
            """List files."""
            return "\n".join(sorted(p.relative_to(root).as_posix() for p in (root / path).rglob(pattern)))

        def read_file(path: str, start_line: int = 1, end_line: int = 400) -> str:
            """Read a file."""
            lines = (root / path).read_text(encoding="utf-8").splitlines()
            return "\n".join(lines[start_line - 1 : end_line])

        def write_file(path: str, content: str) -> str:
            """Write a file."""
            target = root / path
            target.parent.mkdir(parents=True, exist_ok=True)
            target.write_text(content, encoding="utf-8", newline="\n")
            return f"Wrote {path}"

        def replace_in_file(path: str, old: str, new: str) -> str:
            """Replace exactly one occurrence."""
            target = root / path
            text = target.read_text(encoding="utf-8")
            if text.count(old) != 1:
                return f"Error: expected exactly one occurrence, found {text.count(old)}"
            target.write_text(text.replace(old, new), encoding="utf-8", newline="\n")
            return f"Replaced 1 occurrence in {path}"

        def run_tests(paths: list[str] | None = None) -> str:
            """Run pytest."""
            env = {
                key: value for key, value in os.environ.items() if key not in ("PYTHONPATH", "PYTEST_ADDOPTS")
            }
            env["PYTHONDONTWRITEBYTECODE"] = "1"
            proc = subprocess.run(
                [
                    sys.executable,
                    "-m",
                    "pytest",
                    "-q",
                    "-p",
                    "no:cacheprovider",
                    "-o",
                    "addopts=",
                    *(paths or []),
                ],
                cwd=root,
                capture_output=True,
                text=True,
                env=env,
                timeout=300,
            )
            self.pytest_exit_codes.append(proc.returncode)
            return f"exit code {proc.returncode}\n{proc.stdout[-4000:]}"

        return [list_files, read_file, write_file, replace_in_file, run_tests]


DEMO_CASES = [
    pytest.param(
        CODE_SCENARIO,
        "tests/test_customers_signup_date.py",
        "src/etl/customers.py",
        "datetime.fromisoformat(value).date()",
        5,  # docstring + four body lines
        id="customers_sync",
    ),
    pytest.param(
        {"dag_id": "inventory_report", "task_id": "build_report", "exception_type": "KeyError"},
        "tests/test_inventory_build_report.py",
        "src/etl/inventory.py",
        'df = df.rename(columns={"quantity": "qty"})',
        2,
        id="inventory_report",
    ),
]


@pytest.mark.parametrize(("match", "test_file", "module", "fixed_snippet", "changed_lines"), DEMO_CASES)
def test_demo_scripts_turn_the_new_test_red_then_green(
    tmp_path: Path, match: dict[str, str], test_file: str, module: str, fixed_snippet: str, changed_lines: int
) -> None:
    repo = tmp_path / "repo"
    shutil.copytree(EXAMPLE_REPO, repo, ignore=shutil.ignore_patterns("__pycache__", "processed"))
    original = (repo / module).read_text(encoding="utf-8")
    tools = SimpleRepoTools(repo)
    prov = FakeProvider(script_dir=FAKE_SCRIPTS)
    scenario = {**match, "incident_id": "0123456789abcdef"}

    red = prov.repair_session(
        system="s",
        prompt="p",
        tools=tools.functions(),
        max_tokens_budget=1,
        scenario={**scenario, "phase": "test"},
    )
    assert red.tool_calls and red.final_text != NO_SCRIPT_TEXT
    assert not any(record.is_error for record in red.tool_calls)
    assert (repo / test_file).is_file()
    assert (repo / module).read_text(encoding="utf-8") == original
    assert tools.pytest_exit_codes == [1]  # collected and failing, not a collection error (2)
    assert "1 failed, 1 passed" in red.tool_calls[-1].output_excerpt

    green = prov.repair_session(
        system="s",
        prompt="p",
        tools=tools.functions(),
        max_tokens_budget=1,
        scenario={**scenario, "phase": "fix"},
    )
    assert green.tool_calls and green.final_text != NO_SCRIPT_TEXT
    assert not any(record.is_error for record in green.tool_calls)
    assert tools.pytest_exit_codes[0] == 1 and tools.pytest_exit_codes[1:] == [0, 0]
    runs = [record for record in green.tool_calls if record.name == "run_tests"]
    assert all("2 passed" in record.output_excerpt for record in runs)
    fixed = (repo / module).read_text(encoding="utf-8")
    assert fixed_snippet in fixed
    changed = [line for line in fixed.splitlines() if line not in original.splitlines()]
    assert len(changed) == changed_lines  # a minimal fix


def test_too_large_max_tokens_is_reported_as_unavailable() -> None:
    api = FakeAPI()
    with pytest.raises(LLMUnavailable, match="max_tokens is too large"):
        provider(api, max_tokens=64_000).classify(system="S", prompt="P")
    assert api.requests == []
