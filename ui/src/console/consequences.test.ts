import { describe, expect, it } from "vitest";

import { ambiguousIncident, codeIncident, dataIncident, draftContract, setup } from "src/test/fixtures";
import { DEFAULT_UI_TEXT, type UiText } from "src/vocabulary";

import {
  alertLine,
  approvalConsequences,
  consequencesOf,
  deliveryLine,
  filesLine,
  retryLine,
} from "./consequences";

const SFN_TEXT: UiText = {
  host: "standalone",
  type: "stepfunctions",
  words: {
    ...DEFAULT_UI_TEXT.words,
    product: "AWS Step Functions",
    retry_action: "Redrive execution",
    step: "state",
  },
};

describe("who is told", () => {
  it("names the email recipients of the event", () => {
    expect(alertLine(setup(), ["reported"])).toBe(
      "Emails `data-team@example.com`, and writes the alert to the worker's log.",
    );
  });

  it("says when only the log gets it, or nobody", () => {
    const noEmail = setup({ alerts: { email: [], events: ["reported"] } });

    expect(alertLine(noEmail, ["reported"])).toContain("worker's log only");
    expect(alertLine(setup({ alerts: { email: ["a@x.org"], events: [] } }), ["reported"])).toContain(
      "Nobody is alerted",
    );
    expect(alertLine(undefined, ["reported"])).toBeUndefined();
  });
});

describe("how a fix is delivered", () => {
  it("opens a pull request with a token and a repository", () => {
    const github = setup();

    github.repairs.token_configured = true;
    github.repairs.repository = "acme/analytics-etl";
    expect(deliveryLine(github, codeIncident())).toBe(
      "It opens a pull request on `acme/analytics-etl` against `main`. Nothing is merged: you review it. Its " +
        "commit carries `[skip ci]`: your CI runs only after a person has reviewed it.",
    );
  });

  it("writes PR.md without a token", () => {
    expect(deliveryLine(setup(), ambiguousIncident())).toContain(
      "No GitHub token is set, so it commits the fix on a new branch `healer/inventory_report-build_report-…`",
    );
  });

  it("says what a fix may change", () => {
    expect(filesLine(setup())).toBe(
      "The fix may change files under `src/` and `dags/` (at most 10 files and 400 lines) and adds a test. It " +
        "never changes contracts, healer.yaml or CI settings.",
    );
  });
});

describe("retries in the orchestrator's words", () => {
  it("clears the task and its downstream tasks in Airflow", () => {
    expect(retryLine(dataIncident(), DEFAULT_UI_TEXT)).toBe(
      "Airflow clears `transform_sales` in DAG run `manual__2026-10-17T12:00:00+00:00`, and the tasks after it " +
        "that failed or were blocked by it, so they run again with the code deployed now.",
    );
  });

  it("redrives a Step Functions execution", () => {
    expect(retryLine(dataIncident(), SFN_TEXT)).toContain("AWS Step Functions redrives execution");
  });
});

describe("consequencesOf", () => {
  it("reports a data problem, tells who gets the alert and that the draft stays a draft", () => {
    const result = consequencesOf(
      "resolve_data",
      ambiguousIncident(),
      DEFAULT_UI_TEXT,
      setup(),
      draftContract(),
    );

    expect(result.confirm).toBe("Report a data problem");
    expect(result.does).toHaveLength(2);
    expect(result.does[1]).toContain("The contract stays a draft");
    expect(result.alert).toContain("`data-team@example.com`");
    expect(result.then).toContain("use Retry task");
  });

  it("writes a test and a fix, with the files it may change", () => {
    const result = consequencesOf(
      "resolve_code",
      ambiguousIncident(),
      DEFAULT_UI_TEXT,
      setup(),
      draftContract(),
    );

    expect(result.confirm).toBe("Write a test and a fix");
    expect(result.files).toContain("`src/` and `dags/`");
    expect(result.does.join(" ")).toContain("update `healer/contracts/inventory_report.yaml` yourself");
  });

  it("says the repair does not read the note of a retry yet", () => {
    const result = consequencesOf(
      "retry_repair",
      ambiguousIncident({ status: "repair_failed" }),
      DEFAULT_UI_TEXT,
      setup(),
    );

    expect(result.does).toContain("Your note goes to the timeline; the repair does not read it yet.");
  });

  it("warns that a proposed fix runs only once deployed", () => {
    const result = consequencesOf(
      "retry_task",
      codeIncident({ action_warnings: { retry_task: ["Deploy it before Redrive execution."] } }),
      DEFAULT_UI_TEXT,
      setup(),
    );

    expect(result.confirm).toBe("Retry task");
    expect(result.warnings).toEqual([
      "The proposed fix runs only once it is merged and deployed: before that, `transform` runs the old code.",
      "Deploy it before Redrive execution.",
    ]);
    expect(result.then).toContain("opens a new incident");
  });

  it("closes an ignored incident for good", () => {
    expect(consequencesOf("ignore", ambiguousIncident(), DEFAULT_UI_TEXT, setup()).then).toContain(
      "It cannot be reopened from the console.",
    );
  });

  it("explains what approving a contract writes", () => {
    const result = approvalConsequences(draftContract(), DEFAULT_UI_TEXT, {
      status: "waiting_human",
      violations: [],
    });

    expect(result.confirm).toBe("Approve the contract");
    expect(result.does[0]).toContain("It does not commit the file or open a pull request");
    expect(result.files).toBe("Only `healer/contracts/inventory_report.yaml` changes.");
    expect(result.then).toBe("This incident is not decided again: answer the question as well.");
    expect(result.warnings).toEqual([]);
    expect(approvalConsequences(draftContract(), DEFAULT_UI_TEXT).then).toBeUndefined();
  });

  it("warns before approving a contract this incident's input breaks", () => {
    const result = approvalConsequences(draftContract(), DEFAULT_UI_TEXT, {
      status: "waiting_human",
      violations: [{ column: "qty", rule: "missing_column" }],
    });

    expect(result.warnings).toEqual([
      "This incident's input breaks this contract (column `qty` is missing). Approve it only if the file is " +
        "wrong; if the file changed on purpose, update `healer/contracts/inventory_report.yaml` first.",
    ]);
  });
});
