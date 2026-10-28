import { describe, expect, it } from "vitest";

import type { IncidentDetail } from "src/api";
import { ambiguousIncident, codeIncident, dataIncident, draftContract, event } from "src/test/fixtures";
import { DEFAULT_VOCABULARY } from "src/vocabulary";

import { columnChange, datasetFile, decidedBy, questionFor } from "./decision";

const classified = (data: Record<string, unknown>, overrides: Partial<IncidentDetail> = {}) =>
  ambiguousIncident({ events: [event("classification", "Classified", data)], violations: [], ...overrides });

describe("decidedBy", () => {
  it("names a fixed rule for a broken approved contract", () => {
    expect(decidedBy(dataIncident(), DEFAULT_VOCABULARY)).toEqual({
      detail:
        "the approved contract is violated (column `amount` is missing), so this is DATA. The model's opinion " +
        "does not change that.",
      lead: "Decided by a fixed rule",
      who: "rule",
    });
  });

  it("says a person decides while the contract is a draft", () => {
    const line = decidedBy(ambiguousIncident(), DEFAULT_VOCABULARY);

    expect(line.lead).toBe("Waiting for you");
    expect(line.detail).toContain("the contract of `inventory_report` is a draft (nobody approved it yet)");
  });

  it("gives the model's confidence and the threshold when the model took part", () => {
    expect(decidedBy(codeIncident(), DEFAULT_VOCABULARY, 0.75)).toMatchObject({
      detail: expect.stringContaining("the model agreed with confidence 90%; it needs 75%."),
      lead: "Decided by a fixed rule and the model",
    });
    const unsure = classified({
      category: "ambiguous",
      llm_category: "code",
      llm_confidence: 0.62,
      rule: "d_low_confidence",
    });

    expect(decidedBy(unsure, DEFAULT_VOCABULARY, 0.75)).toMatchObject({
      detail: "the model was not sure enough (confidence 62%; it needs 75%).",
      lead: "Waiting for you",
      who: "model",
    });
    const model = classified({
      category: "code",
      confidence: 0.86,
      contract_status: "draft",
      llm_confidence: 0.86,
      rule: "llm",
    });

    expect(decidedBy(model, DEFAULT_VOCABULARY)).toMatchObject({
      detail: expect.stringContaining(
        "no fixed rule applied (the contract of `inventory_report` is a draft)",
      ),
      lead: "Decided by the model",
    });
  });

  it("names the person who decided or ignored", () => {
    const human = classified({ category: "code", decided_by: "ann", rule: "human_resolution" });

    expect(decidedBy(human, DEFAULT_VOCABULARY).lead).toBe("Decided by ann");
    expect(
      decidedBy(ambiguousIncident({ resolution: "ignore", resolved_by: "bob" }), DEFAULT_VOCABULARY),
    ).toEqual({
      detail: "they closed the incident without any action.",
      lead: "Decided by bob",
      who: "person",
    });
  });

  it("says nothing was decided before the classification", () => {
    expect(decidedBy(ambiguousIncident({ events: [] }), DEFAULT_VOCABULARY).lead).toBe("Not decided yet");
  });
});

describe("questionFor", () => {
  it("asks whether the file changed on purpose when the draft's column was replaced", () => {
    const question = questionFor(ambiguousIncident(), DEFAULT_VOCABULARY, draftContract());

    expect(question.title).toBe("Did `inventory.csv` change on purpose?");
    expect(question.context[0]).toBe(
      "When dag-healer checked it, `inventory.csv` had `quantity` and `warehouse_code` where the draft contract " +
        "expects `qty`.",
    );
    expect(question.main.map((answer) => [answer.action, answer.title])).toEqual([
      ["resolve_code", "Yes, it changed on purpose"],
      ["resolve_data", "No, the file is wrong"],
    ]);
    expect(question.other.map((answer) => answer.action)).toEqual(["retry_task", "ignore"]);
  });

  it("asks the general question without the contract", () => {
    const question = questionFor(ambiguousIncident(), DEFAULT_VOCABULARY);

    expect(question.title).toBe("Is the input wrong, or should the code accept it?");
    expect(question.context[0]).toContain("an error about the shape of the data");
    expect(question.main.map((answer) => answer.action)).toEqual(["resolve_data", "resolve_code"]);
  });

  it("offers to run an infrastructure failure again first", () => {
    const question = questionFor(
      classified({ category: "ambiguous", rule: "infra" }, { error_name: "States.Timeout" }),
      { ...DEFAULT_VOCABULARY, retry_action: "Redrive execution", step: "state" },
    );

    expect(question.title).toBe("Run `build_report` again?");
    expect(question.main.map((answer) => [answer.action, answer.title])).toEqual([
      ["retry_task", "Yes, run it again"],
      ["ignore", "No, close it"],
    ]);
  });

  it("asks what next after a failed fix or an error", () => {
    const failed = questionFor(
      ambiguousIncident({
        allowed_actions: ["retry_repair", "resolve_data", "ignore"],
        events: [event("repair", "Repair failed", { error: "No acceptable fix after 3 attempt(s)." })],
        status: "repair_failed",
      }),
      DEFAULT_VOCABULARY,
    );

    expect(failed.title).toBe("dag-healer could not write a working fix. What now?");
    expect(failed.context[0]).toContain("What it reported: No acceptable fix after 3 attempt(s).");
    expect(failed.main.map((answer) => answer.action)).toEqual(["retry_repair", "resolve_data"]);
    const stopped = questionFor(
      ambiguousIncident({
        events: [
          event("error", "Processing failed while gathering the context: 503", {
            stage: "gathering the context",
          }),
        ],
        status: "error",
      }),
      DEFAULT_VOCABULARY,
    );

    expect(stopped.title).toBe("dag-healer stopped with an error. What now?");
    expect(stopped.context).toEqual([
      "dag-healer stopped before it finished: Processing failed while gathering the context: 503",
    ]);
    // Nothing was repaired yet: no "again", and the report carries what there is.
    expect(stopped.main[0]?.title).toBe("Treat it as a code problem");
    const bare = questionFor(
      ambiguousIncident({ attempts: 2, sample: null, status: "error", violations: [] }),
      DEFAULT_VOCABULARY,
    );

    expect(bare.main[0]?.title).toBe("Try the fix again");
    expect(bare.main[1]?.body).toBe(
      "Report a data problem, with the error (no contract check failed). No code changes.",
    );
  });
});

describe("decidedBy, before and after a person answers", () => {
  it("says an incident that stopped early was not classified", () => {
    expect(decidedBy(ambiguousIncident({ events: [], status: "error" }), DEFAULT_VOCABULARY)).toEqual({
      detail: "dag-healer stopped before it could classify this failure.",
      lead: "Not classified",
      who: "nobody",
    });
  });

  it("names who answered while the worker has not applied it", () => {
    const queued = ambiguousIncident({
      pending_commands: [
        {
          action: "resolve_data",
          actor: "ann",
          created_at: "2026-10-17T12:10:00+00:00",
          id: 3,
          kind: "resolve",
        },
      ],
    });

    expect(decidedBy(queued, DEFAULT_VOCABULARY)).toEqual({
      detail: "report a data problem, queued for dag-healer's worker.",
      lead: "Decided by ann",
      who: "person",
    });
  });
});

describe("columnChange and datasetFile", () => {
  it("compare the missing columns with the columns the file had", () => {
    expect(columnChange(ambiguousIncident().violations, draftContract())).toEqual({
      added: ["quantity", "warehouse_code"],
      dataset: "inventory",
      missing: ["qty"],
    });
    expect(columnChange(ambiguousIncident().violations, undefined)).toBeUndefined();
    expect(columnChange([], draftContract())).toBeUndefined();
  });

  it("name a dataset by its file", () => {
    expect(datasetFile(draftContract(), "inventory")).toBe("inventory.csv");
    expect(datasetFile(draftContract(), "other")).toBe("other");
    expect(datasetFile(undefined, undefined)).toBeUndefined();
  });
});
