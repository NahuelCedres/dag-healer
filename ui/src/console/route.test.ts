import { describe, expect, it } from "vitest";

import { summary } from "src/test/fixtures";
import { DEFAULT_VOCABULARY } from "src/vocabulary";

import { actionWords, foundBy, groupOf, incidentGroup, nextStep, statusMeta, violationShort } from "./labels";
import { parseRoute, routeHref } from "./route";

const BASE = "http://healer.example/airflow/";

describe("parseRoute", () => {
  it("reads the console's place from the query string", () => {
    expect(parseRoute("/airflow/plugin/healer", "?dag=d&incident=i", BASE)).toEqual({
      dagId: "d",
      incidentId: "i",
      view: "incident",
    });
    expect(parseRoute("/airflow/plugin/healer", "?view=pipelines&pipeline=p", BASE)).toEqual({
      pipeline: "p",
      view: "pipelines",
    });
    expect(parseRoute("/airflow/plugin/healer", "?group=needs", BASE)).toEqual({
      group: "needs",
      view: "inbox",
    });
    expect(parseRoute("/airflow/plugin/healer", "?group=bogus", BASE)).toEqual({
      group: undefined,
      view: "inbox",
    });
  });

  it("reads the inbox's filters (a pipeline alone is a filter, not an incident)", () => {
    expect(parseRoute("/airflow/plugin/healer", "?group=done&q=qty&dag=sales&category=data", BASE)).toEqual({
      filters: { category: "data", pipeline: "sales", query: "qty" },
      group: "done",
      view: "inbox",
    });
  });

  it("opens the plugin's other URL shapes on a pipeline or a task instance", () => {
    expect(parseRoute("/airflow/dags/sales/plugin/healer-dag", "", BASE)).toEqual({
      pipeline: "sales",
      view: "pipelines",
    });
    expect(
      parseRoute("/airflow/dags/sales/runs/manual__1%3A2/tasks/load/mapped/3/plugin/healer-ti", "", BASE),
    ).toEqual({ dagId: "sales", mapIndex: 3, runId: "manual__1:2", taskId: "load", view: "task_instance" });
  });
});

describe("routeHref", () => {
  it("puts the place in the Healer page's query string", () => {
    expect(routeHref({ dagId: "d", incidentId: "i", view: "incident" }, BASE)).toBe(
      "http://healer.example/airflow/plugin/healer?dag=d&incident=i",
    );
    expect(routeHref({ pipeline: "p q", view: "pipelines" }, BASE)).toBe(
      "http://healer.example/airflow/plugin/healer?view=pipelines&pipeline=p+q",
    );
    expect(routeHref({ view: "inbox" }, BASE)).toBe("http://healer.example/airflow/plugin/healer");
    expect(
      routeHref(
        { filters: { category: "code", pipeline: "sales", query: "a b" }, group: "needs", view: "inbox" },
        BASE,
      ),
    ).toBe("http://healer.example/airflow/plugin/healer?group=needs&q=a+b&dag=sales&category=code");
  });
});

describe("labels", () => {
  it("group the statuses by what they need", () => {
    expect(["waiting_human", "repair_failed", "error"].map(groupOf)).toEqual(["needs", "needs", "needs"]);
    expect(["new", "triaging", "classified", "reporting", "repairing", "resumed"].map(groupOf)).toEqual(
      Array(6).fill("progress"),
    );
    expect(["reported", "repaired", "ignored"].map(groupOf)).toEqual(["done", "done", "done"]);
    expect(statusMeta("repaired")).toEqual({ group: "done", label: "Fix proposed", tone: "blue" });
    expect(statusMeta("some_new_status").label).toBe("some new status");
  });

  it("say the next step and how a failure was found", () => {
    expect(nextStep(summary({ has_pr_md: true }), DEFAULT_VOCABULARY)).toBe(
      "Review PR.md and open the pull request",
    );
    expect(nextStep(summary({ pr_url: "https://github.com/a/b/pull/1" }), DEFAULT_VOCABULARY)).toBe(
      "Review the pull request",
    );
    expect(nextStep(summary({ status: "reported" }), DEFAULT_VOCABULARY)).toBe(
      "Fix the input, then retry task",
    );
    expect(foundBy("listener", DEFAULT_VOCABULARY)).toBe("Found by the Airflow listener");
    expect(foundBy("rest", DEFAULT_VOCABULARY)).toBe("Found by checking Airflow regularly");
    expect(foundBy("eventbridge", DEFAULT_VOCABULARY)).toBe("Reported by AWS EventBridge");
  });

  it("say what an answered incident waits for, in the orchestrator's words", () => {
    const queued = summary({
      pending_command: {
        action: "retry_task",
        actor: "ann",
        created_at: "2026-10-17T12:10:00+00:00",
        id: 1,
        kind: "retry_task",
      },
      status: "reported",
    });
    const redrive = { ...DEFAULT_VOCABULARY, retry_action: "Redrive execution" };

    expect(incidentGroup(queued)).toBe("progress");
    expect(incidentGroup(summary({ status: "waiting_human" }))).toBe("needs");
    expect(nextStep(queued, DEFAULT_VOCABULARY)).toBe("Queued: retry task");
    expect(nextStep(queued, redrive)).toBe("Queued: redrive execution");
    expect(actionWords(null, DEFAULT_VOCABULARY)).toBe("apply a decision");
    expect(actionWords("resolve_code", DEFAULT_VOCABULARY)).toBe("write a test and a fix");
  });

  it("put a broken check in a few words", () => {
    expect(violationShort({ column: "qty", rule: "missing_column" })).toBe("column `qty` is missing");
    expect(violationShort({ column: null, rule: "row_count_out_of_band" })).toBe("an unexpected row count");
    expect(violationShort({ column: "x", rule: "some_new_rule" })).toBe("some new rule in `x`");
  });
});
