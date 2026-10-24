import { describe, expect, it } from "vitest";

import { actionWarnings, nativeLinks } from "./model";

describe("nativeLinks", () => {
  it("lists the server's native links as 'label, url' sorted by label", () => {
    expect(
      nativeLinks({
        links: {
          native: {
            "AWS console": "https://console.aws.amazon.com/states/home#/v2/executions/details/x",
            "CloudWatch logs": "https://console.aws.amazon.com/cloudwatch/home#logsV2",
            Fakeflow: "http://fakeflow.localhost/flows/f/runs/r/steps/s?attempt=1",
          },
        },
      }),
    ).toEqual([
      { label: "AWS console", url: "https://console.aws.amazon.com/states/home#/v2/executions/details/x" },
      { label: "CloudWatch logs", url: "https://console.aws.amazon.com/cloudwatch/home#logsV2" },
      { label: "Fakeflow", url: "http://fakeflow.localhost/flows/f/runs/r/steps/s?attempt=1" },
    ]);
  });

  it("keeps only absolute http(s) URLs with a label", () => {
    const native = {
      "": "https://example.com/empty-label",
      data: "data:text/html,<script>alert(1)</script>",
      ftp: "ftp://example.com/file",
      javascript: "javascript:alert(1)",
      mixed: " JavaScript:alert(1)",
      number: 42,
      ok: "https://example.com/ok",
      relative: "/dags/x",
    } as unknown as Record<string, string>;

    expect(nativeLinks({ links: { native } })).toEqual([{ label: "ok", url: "https://example.com/ok" }]);
  });

  it("is empty without native links (Airflow)", () => {
    expect(nativeLinks({})).toEqual([]);
    expect(nativeLinks({ links: null })).toEqual([]);
    expect(nativeLinks({ links: { dag: "x", healer: "y", task_instance: "z" } })).toEqual([]);
    expect(nativeLinks({ links: { native: {} } })).toEqual([]);
  });
});

describe("actionWarnings", () => {
  it("gives the non-empty strings the server sent for the action", () => {
    const incident = {
      action_warnings: {
        retry_task: ["The fix reaches the orchestrator once it is deployed.", "", 3],
      } as unknown as Record<string, Array<string>>,
    };

    expect(actionWarnings(incident, "retry_task")).toEqual([
      "The fix reaches the orchestrator once it is deployed.",
    ]);
    expect(actionWarnings(incident, "ignore")).toEqual([]);
    expect(actionWarnings({}, "retry_task")).toEqual([]);
  });
});
