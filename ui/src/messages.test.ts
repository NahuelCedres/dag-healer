import { describe, expect, it } from "vitest";

import { ACTION_LABELS, actionLabel } from "./format";
import {
  MASKED_LABEL,
  NO_DAG_ACCESS_MESSAGE,
  NO_TI_INCIDENT_MESSAGE,
  noPipelineAccessMessage,
  noStepInstanceIncidentMessage,
  NOT_MONITORED_MESSAGE,
  notMonitoredMessage,
  QUEUED_MESSAGE,
  SESSION_EXPIRED_MESSAGE,
  sessionExpiredMessage,
  sessionExpiredRetryMessage,
} from "./messages";
import { DEFAULT_UI_TEXT, DEFAULT_VOCABULARY, type UiText, vocabularyFrom } from "./vocabulary";

const FAKEFLOW = vocabularyFrom({
  attempt: "attempt",
  pipeline: "flow",
  pipelines: "flows",
  product: "Fakeflow",
  retry_action: "Retry step",
  run: "run",
  step: "step",
  steps: "steps",
});

describe("message constants", () => {
  it("keep today's Airflow texts", () => {
    expect(SESSION_EXPIRED_MESSAGE).toBe("Your Airflow session expired — reload the page");
    expect(NO_DAG_ACCESS_MESSAGE).toBe("You do not have access to this DAG.");
    expect(QUEUED_MESSAGE).toBe("Queued — the worker will apply it shortly");
    expect(NO_TI_INCIDENT_MESSAGE).toBe("No incident recorded for this task instance.");
    expect(NOT_MONITORED_MESSAGE).toBe(
      "This DAG is not monitored by dag-healer. Add it to monitor.dags in healer.yaml.",
    );
    expect(MASKED_LABEL).toBe("Values are masked by dag-healer");
  });
});

describe("vocabulary-driven messages", () => {
  it("give exactly the constants with the defaults", () => {
    expect(sessionExpiredMessage()).toBe(SESSION_EXPIRED_MESSAGE);
    expect(sessionExpiredMessage(DEFAULT_UI_TEXT)).toBe(SESSION_EXPIRED_MESSAGE);
    expect(noPipelineAccessMessage()).toBe(NO_DAG_ACCESS_MESSAGE);
    expect(noPipelineAccessMessage(DEFAULT_VOCABULARY)).toBe(NO_DAG_ACCESS_MESSAGE);
    expect(notMonitoredMessage(DEFAULT_VOCABULARY)).toBe(NOT_MONITORED_MESSAGE);
    expect(noStepInstanceIncidentMessage(DEFAULT_VOCABULARY)).toBe(NO_TI_INCIDENT_MESSAGE);
  });

  it("keep the Airflow text of a failed action", () => {
    expect(sessionExpiredRetryMessage(DEFAULT_UI_TEXT)).toBe(
      "Your Airflow session expired — reload the page and try again.",
    );
  });

  it("speak of the standalone host's own session", () => {
    const standalone: UiText = { host: "standalone", words: FAKEFLOW };

    expect(sessionExpiredMessage(standalone)).toBe("Your session expired — reload the page");
    expect(sessionExpiredRetryMessage(standalone)).toBe(
      "Your session expired — reload the page and try again.",
    );
    expect(sessionExpiredMessage({ host: "standalone", words: DEFAULT_VOCABULARY })).toBe(
      "Your session expired — reload the page",
    );
  });

  it("use the orchestrator's words", () => {
    expect(noPipelineAccessMessage(FAKEFLOW)).toBe("You do not have access to this flow.");
    expect(notMonitoredMessage(FAKEFLOW)).toBe(
      "This flow is not monitored by dag-healer. Add it to monitor.dags in healer.yaml.",
    );
    expect(noStepInstanceIncidentMessage(FAKEFLOW)).toBe("No incident recorded for this step instance.");
  });
});

describe("actionLabel", () => {
  it("gives today's labels with the defaults", () => {
    for (const [action, label] of Object.entries(ACTION_LABELS)) {
      expect(actionLabel(action as keyof typeof ACTION_LABELS)).toBe(label);
    }
    expect(actionLabel("retry_task", DEFAULT_VOCABULARY)).toBe("Retry task");
  });

  it("takes the retry label from the vocabulary", () => {
    expect(actionLabel("retry_task", FAKEFLOW)).toBe("Retry step");
    expect(actionLabel("retry_repair", FAKEFLOW)).toBe("Retry repair");
    expect(actionLabel("resolve_data", FAKEFLOW)).toBe("Resolve as data");
  });
});
