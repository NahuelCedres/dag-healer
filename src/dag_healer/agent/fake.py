"""Offline, deterministic LLM provider for CI, unit tests and the keyless demo.

* :meth:`FakeProvider.classify` reads the JSON block between ``<signals>`` and
  ``</signals>`` that :mod:`dag_healer.agent.classify` appends to every prompt and returns the
  category of the deterministic policy hint it contains (confidence 0.9 for ``data`` / ``code``,
  0.5 for ``ambiguous``) with a diagnosis templated from the signals.
* :meth:`FakeProvider.repair_session` replays YAML scripts. Each script has a ``match``
  mapping (``dag_id``, ``task_id``, ``exception_type``, ``phase``; a missing key matches anything,
  a list matches any of its values) and ``steps`` (``[{tool: name, input: {...}}]``) that are
  executed by calling the tool functions by name. Scripts are read from ``$HEALER_FAKE_LLM_DIR``
  or ``<repo.path>/fixtures/fake_llm``. Without a matching script the session returns
  :data:`NO_SCRIPT_TEXT` and makes no tool calls.
"""

from __future__ import annotations

import json
import logging
import os
from collections.abc import Callable, Mapping
from pathlib import Path
from typing import Any

import yaml

from dag_healer.agent.llm import (
    Classification,
    LLMUnavailable,
    RepairSessionResult,
    ToolCallRecord,
    tool_call_record,
)
from dag_healer.masking import mask_text

logger = logging.getLogger(__name__)

SIGNALS_START = "<signals>"
SIGNALS_END = "</signals>"
FAKE_DIR_ENV = "HEALER_FAKE_LLM_DIR"
DEFAULT_SCRIPT_DIR = "fixtures/fake_llm"
NO_SCRIPT_TEXT = "The fake LLM provider has no script for this scenario."
MATCH_KEYS: tuple[str, ...] = ("dag_id", "task_id", "exception_type", "phase")
CONFIDENT = 0.9
UNSURE = 0.5


# --------------------------------------------------------------------------- signals


def extract_signals(prompt: str) -> dict[str, Any]:
    """Return the JSON object of the last ``<signals>...</signals>`` block of ``prompt``.

    Raises :class:`LLMUnavailable` when the block is missing or is not a JSON object.
    """
    start = prompt.rfind(SIGNALS_START)
    end = prompt.find(SIGNALS_END, start + len(SIGNALS_START)) if start >= 0 else -1
    if start < 0 or end < 0:
        raise LLMUnavailable("The fake LLM provider found no <signals> block in the prompt")
    try:
        data = json.loads(prompt[start + len(SIGNALS_START) : end])
    except json.JSONDecodeError as exc:
        raise LLMUnavailable(f"The <signals> block is not valid JSON: {exc.msg}") from exc
    if not isinstance(data, dict):
        raise LLMUnavailable("The <signals> block must hold a JSON object")
    return data


def _short(name: Any) -> str | None:
    if not name:
        return None
    return str(name).strip().rsplit(".", 1)[-1] or None


def _location(frame: Any) -> str | None:
    if not isinstance(frame, Mapping) or not frame.get("file"):
        return None
    text = str(frame["file"])
    if frame.get("function"):
        text += f"::{frame['function']}"
    if frame.get("line"):
        text += f" (line {frame['line']})"
    return text


def _rules_text(signals: Mapping[str, Any]) -> str:
    details = [str(item) for item in signals.get("violation_details") or []]
    if details:
        return "; ".join(details)
    rules = [str(rule) for rule in signals.get("violation_rules") or []]
    return ", ".join(rules) if rules else "no specific rule"


def fake_classification(signals: Mapping[str, Any]) -> Classification:
    """Deterministic :class:`Classification` consistent with ``signals["policy_hint"]``."""
    hint = signals.get("policy_hint") if isinstance(signals.get("policy_hint"), Mapping) else {}
    category = hint.get("category") if hint.get("category") in ("data", "code", "ambiguous") else "ambiguous"
    rule = str(hint.get("rule") or "")
    exc_type = _short(signals.get("exception_type")) or "an unknown error"
    message = mask_text(signals.get("exception_message") or "").strip()
    error = f"{exc_type}: {message}" if message else exc_type
    frame = signals.get("top_repo_frame") if isinstance(signals.get("top_repo_frame"), Mapping) else None
    where = _location(frame)
    rules = [str(rule) for rule in signals.get("violation_rules") or []]
    details = [str(item) for item in signals.get("violation_details") or []]
    draft_rules = [str(rule) for rule in signals.get("draft_violation_rules") or []]
    status = signals.get("contract_status") or "missing"
    task = ".".join(str(part) for part in (signals.get("dag_id"), signals.get("task_id")) if part)
    task_text = f"Task {task}" if task else "The task"
    missing = signals.get("missing_column_in_error")

    evidence: list[str] = []
    if draft_rules:
        evidence.append(f"{status} contract findings (not enforced): " + ", ".join(draft_rules))
    if where:
        evidence.append(f"innermost repository frame: {where}")
    if missing:
        evidence.append(f"column named in the error: {missing}")

    violated_rule = None
    fix_summary = None
    if category == "data":
        violated_rule = details[0] if details else (rules[0] if rules else None)
        diagnosis = (
            f"{task_text} failed with {error} because its input broke the approved data contract "
            f"({_rules_text(signals)}). The code is behaving as designed: the upstream delivery must be "
            "fixed (or the contract updated and re-approved if the change is intended), then the task can "
            "be retried."
        )
    elif category == "code":
        location = where or "the repository code"
        diagnosis = (
            f"{task_text} failed with {error} raised in {location}. The input data satisfies the approved "
            "contract, so this is a defect in the code: it does not handle input that the contract allows. "
            "A minimal fix plus a regression test should resolve it."
        )
        symbol = frame.get("function") if frame else None
        fix_summary = (
            f"Make {symbol or 'the failing function'} handle the input that triggers {exc_type} "
            "with a minimal change, and cover it with a regression test."
        )
    elif rule == "infra":
        diagnosis = (
            f"{task_text} failed with {error}, which looks like an infrastructure problem rather than a "
            "data or code defect. Check the worker, the network and the services the task calls, then "
            "retry the task."
        )
    elif rule == "c_no_approved_contract":
        findings = f" The {status} contract reports {', '.join(draft_rules)}." if draft_rules else ""
        diagnosis = (
            f"{task_text} failed with {error}, a data-shaped error, but there is no approved data contract "
            f"(status: {status}) to tell whether the input is wrong.{findings} If the upstream change is a "
            "mistake, resolve the incident as data; if the new shape is intended, resolve it as code so the "
            "code is adapted."
        )
    else:
        diagnosis = (
            f"{task_text} failed with {error}. The evidence does not settle whether the data or the code is "
            f"wrong (contract status: {status}); a human should resolve the incident as data or code."
        )

    return Classification(
        category=category,
        confidence=CONFIDENT if category in ("data", "code") else UNSURE,
        diagnosis=diagnosis,
        violated_rule=violated_rule,
        suspected_file=str(frame["file"]) if frame and frame.get("file") else None,
        suspected_symbol=str(frame["function"]) if frame and frame.get("function") else None,
        evidence=evidence,
        fix_summary=fix_summary,
    )


# --------------------------------------------------------------------------- scripts


def load_scripts(directory: str | Path) -> list[dict[str, Any]]:
    """Load the ``*.yaml`` / ``*.yml`` scripts of ``directory`` (sorted by file name).

    Invalid files are skipped with a warning. Each returned script gets a ``name``
    (defaulting to the file stem) and a ``_path``.
    """
    root = Path(directory)
    if not root.is_dir():
        return []
    scripts: list[dict[str, Any]] = []
    for path in sorted([*root.glob("*.yaml"), *root.glob("*.yml")], key=lambda p: p.name):
        try:
            data = yaml.safe_load(path.read_text(encoding="utf-8"))
        except (OSError, yaml.YAMLError) as exc:
            logger.warning("Skipping fake LLM script %s: %s", path.name, exc)
            continue
        if not isinstance(data, dict) or not isinstance(data.get("steps", []), list):
            logger.warning("Skipping fake LLM script %s: expected a mapping with a 'steps' list", path.name)
            continue
        match = data.get("match") or {}
        if not isinstance(match, dict):
            logger.warning("Skipping fake LLM script %s: 'match' must be a mapping", path.name)
            continue
        unknown = sorted(set(match) - set(MATCH_KEYS))
        if unknown:
            logger.warning("Fake LLM script %s: ignoring unknown match keys %s", path.name, unknown)
        data["match"] = match
        data.setdefault("name", path.stem)
        data["_path"] = str(path)
        scripts.append(data)
    return scripts


def _value_matches(key: str, expected: Any, actual: Any) -> bool:
    options = expected if isinstance(expected, list) else [expected]
    if key == "exception_type":
        actual_short = _short(actual)
        return any(_short(option) == actual_short for option in options)
    return any(str(option) == str(actual) for option in options if actual is not None)


def match_script(scripts: list[dict[str, Any]], scenario: Mapping[str, Any]) -> dict[str, Any] | None:
    """Return the script whose ``match`` fits ``scenario`` best (most keys matched; ties by name)."""
    best: dict[str, Any] | None = None
    best_score = -1
    for script in scripts:
        match = {key: value for key, value in script["match"].items() if key in MATCH_KEYS}
        if not all(_value_matches(key, value, scenario.get(key)) for key, value in match.items()):
            continue
        if len(match) > best_score:
            best, best_score = script, len(match)
    return best


class FakeProvider:
    """Deterministic provider: policy-consistent classifications and scripted repair sessions."""

    name = "fake"

    def __init__(self, cfg: Any = None, *, script_dir: str | Path | None = None) -> None:
        self._cfg = cfg
        self._script_dir = Path(script_dir) if script_dir is not None else None

    def script_dir(self) -> Path | None:
        """Where scripts are read from: ``$HEALER_FAKE_LLM_DIR``, ``script_dir`` or the repo fixtures."""
        env = os.environ.get(FAKE_DIR_ENV)
        if env:
            return Path(env).expanduser()
        if self._script_dir is not None:
            return self._script_dir
        repo_root = getattr(self._cfg, "repo_root", None)
        if repo_root is not None:
            return Path(repo_root) / DEFAULT_SCRIPT_DIR
        return None

    def classify(self, *, system: str, prompt: str) -> tuple[Classification, int]:
        """Classify from the ``<signals>`` block of the prompt (no tokens are spent)."""
        return fake_classification(extract_signals(prompt)), 0

    def repair_session(
        self,
        *,
        system: str,
        prompt: str,
        tools: list[Callable[..., str]],
        max_tokens_budget: int,
        scenario: dict[str, Any],
    ) -> RepairSessionResult:
        """Replay the script matching ``scenario`` by calling the tool functions by name."""
        directory = self.script_dir()
        scripts = load_scripts(directory) if directory is not None else []
        script = match_script(scripts, scenario or {})
        if script is None:
            logger.info(
                "Fake LLM: no script in %s matches %s",
                directory,
                {key: (scenario or {}).get(key) for key in MATCH_KEYS},
            )
            return RepairSessionResult(final_text=NO_SCRIPT_TEXT, stop_reason="end_turn")

        by_name: dict[str, Callable[..., Any]] = {}
        for tool in tools:
            name = getattr(tool, "name", None) if hasattr(tool, "to_dict") else None
            func = getattr(tool, "func", tool) if hasattr(tool, "to_dict") else tool
            by_name[str(name or getattr(func, "__name__", ""))] = func

        records: list[ToolCallRecord] = []
        for index, step in enumerate(script.get("steps") or [], start=1):
            if not isinstance(step, Mapping) or not step.get("tool"):
                logger.warning("Fake LLM script %s: step %d has no 'tool'", script["name"], index)
                continue
            records.append(self._run_step(by_name, str(step["tool"]), step.get("input") or {}))
            if records[-1].is_error and step.get("stop_on_error"):
                break

        logger.info("Fake LLM: replayed script %s (%d tool calls)", script["name"], len(records))
        final_text = str(script.get("final_text") or f"Replayed fake LLM script {script['name']}.").strip()
        return RepairSessionResult(final_text=final_text, tool_calls=records, stop_reason="end_turn")

    @staticmethod
    def _run_step(by_name: Mapping[str, Callable[..., Any]], name: str, arguments: Any) -> ToolCallRecord:
        func = by_name.get(name)
        if func is None:
            return tool_call_record(name, arguments, f"Error: unknown tool {name!r}", is_error=True)
        if not isinstance(arguments, Mapping):
            return tool_call_record(name, {}, "Error: step input must be a mapping", is_error=True)
        try:
            output = func(**dict(arguments))
        except Exception as exc:  # the scripted "model" sees the error like a real tool error
            return tool_call_record(name, arguments, f"Error: {type(exc).__name__}: {exc}", is_error=True)
        text = "" if output is None else str(output)
        return tool_call_record(name, arguments, text, is_error=text.lstrip().lower().startswith("error"))


__all__ = [
    "FAKE_DIR_ENV",
    "NO_SCRIPT_TEXT",
    "SIGNALS_END",
    "SIGNALS_START",
    "FakeProvider",
    "extract_signals",
    "fake_classification",
    "load_scripts",
    "match_script",
]
