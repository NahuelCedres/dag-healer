import { renderHook } from "@testing-library/react";
import { describe, expect, it } from "vitest";

import {
  capitalize,
  DEFAULT_DISPLAY_NAME,
  DEFAULT_UI_TEXT,
  DEFAULT_VOCABULARY,
  useUiText,
  vocabularyFrom,
} from "./vocabulary";

describe("DEFAULT_VOCABULARY", () => {
  it("holds the Airflow words of dag_healer.core.vocab.Vocabulary", () => {
    expect(DEFAULT_VOCABULARY).toEqual({
      attempt: "try",
      map_index: "map index",
      pipeline: "DAG",
      pipelines: "DAGs",
      product: "Airflow",
      retry_action: "Retry task",
      run: "DAG run",
      step: "task",
      steps: "tasks",
    });
    expect(DEFAULT_DISPLAY_NAME).toBe("Apache Airflow");
  });

  it("is what the views get outside a provider (the Airflow plugin)", () => {
    expect(DEFAULT_UI_TEXT).toEqual({ host: "airflow_plugin", words: DEFAULT_VOCABULARY });
    expect(renderHook(() => useUiText()).result.current).toBe(DEFAULT_UI_TEXT);
  });
});

describe("vocabularyFrom", () => {
  it("merges the known words of /me over the defaults", () => {
    expect(vocabularyFrom({ pipeline: "flow", pipelines: "flows", retry_action: "Retry step" })).toEqual({
      ...DEFAULT_VOCABULARY,
      pipeline: "flow",
      pipelines: "flows",
      retry_action: "Retry step",
    });
  });

  it("ignores unknown keys, empty words and values that are not strings", () => {
    expect(
      vocabularyFrom({ extra: "x", pipeline: "", product: 3, step: null, steps: "   ", toString: "no" }),
    ).toEqual(DEFAULT_VOCABULARY);
  });

  it.each([undefined, null, "flow", 42, ["flow"]])("returns the defaults for %j", (raw) => {
    expect(vocabularyFrom(raw)).toEqual(DEFAULT_VOCABULARY);
  });

  it("never changes the defaults", () => {
    vocabularyFrom({ pipeline: "flow" });
    expect(DEFAULT_VOCABULARY.pipeline).toBe("DAG");
  });
});

describe("capitalize", () => {
  it.each([
    ["task", "Task"],
    ["flow", "Flow"],
    ["DAG", "DAG"],
    ["state machine", "State machine"],
    ["", ""],
  ])("%j -> %j", (word, expected) => {
    expect(capitalize(word)).toBe(expected);
  });
});
