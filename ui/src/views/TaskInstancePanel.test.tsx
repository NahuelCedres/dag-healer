import { cleanup, fireEvent, screen, waitFor, within } from "@testing-library/react";
import { afterEach, describe, expect, it, vi } from "vitest";

import type { IncidentDetail } from "src/api";
import { HealerTaskIncident } from "src/main";
import {
  MASKED_LABEL,
  NO_DAG_ACCESS_MESSAGE,
  NO_TI_INCIDENT_MESSAGE,
  QUEUED_MESSAGE,
  SESSION_EXPIRED_MESSAGE,
} from "src/messages";
import { ambiguousIncident, codeIncident, dataIncident, RUN_ID, TEST_CODE } from "src/test/fixtures";
import { jsonResponse, mockApi, renderWithPolling } from "src/test/utils";

afterEach(() => {
  // Unmount first so no polling request can reach the real fetch once it is unstubbed.
  cleanup();
  vi.unstubAllGlobals();
});

const TI_ROUTE = /^dags\/[^/]+\/task-instance$/u;

const serve = (incident: IncidentDetail | null) =>
  mockApi([{ handler: () => ({ incident }), path: TI_ROUTE }]);

const renderPanel = (dagId: string, taskId: string, mapIndex = "-1") =>
  renderWithPolling(<HealerTaskIncident dagId={dagId} mapIndex={mapIndex} runId={RUN_ID} taskId={taskId} />);

describe("TaskInstancePanel", () => {
  it("shows a repaired code incident: test, diff, pytest output, PR.md and timeline", async () => {
    const { calls } = serve(codeIncident());

    renderPanel("customers_sync", "transform");
    expect(await screen.findByRole("heading", { name: "Healer incident" })).toBeInTheDocument();

    const request = calls[0];

    expect(request?.path).toBe("dags/customers_sync/task-instance");
    expect(request?.search.get("run_id")).toBe(RUN_ID);
    expect(request?.search.get("task_id")).toBe("transform");
    expect(request?.search.get("map_index")).toBe("-1");
    expect(request?.headers.get("X-Requested-With")).toBe("dag-healer");

    const classification = screen.getByTestId("classification");

    expect(within(classification).getByText("Code")).toBeInTheDocument();
    expect(within(classification).getByText("90%")).toBeInTheDocument();
    expect(within(classification).getByText("b_code_with_valid_data")).toBeInTheDocument();
    expect(within(classification).getByText("Data passes the approved contract")).toBeInTheDocument();

    expect(screen.getByTestId("diagnosis")).toHaveTextContent("parse_signup_date only accepts dates");
    expect(screen.getByTestId("test-path")).toHaveTextContent("tests/test_customers_parse_signup_date.py");
    expect(screen.getByLabelText("Generated test").textContent).toBe(TEST_CODE);

    const diff = screen.getByTestId("diff-view");

    expect(diff.querySelectorAll('[data-kind="add"]')).toHaveLength(1);
    expect(diff.querySelectorAll('[data-kind="del"]')).toHaveLength(1);

    // pytest output is collapsed until asked for.
    expect(screen.queryByText(/1 passed in 0.12s/u)).toBeNull();
    fireEvent.click(screen.getByRole("button", { name: "Show pytest output" }));
    expect(await screen.findByText(/1 passed in 0.12s/u)).toBeInTheDocument();

    // PR.md is linked through the API path resolved against the page base, not the server URL.
    expect(screen.getByRole("link", { name: "View PR.md" })).toHaveAttribute(
      "href",
      new URL(
        "healer/api/v1/dags/customers_sync/incidents/a1b2c3d4e5f60718293a4b5c6d7e8f90/pr.md",
        document.baseURI,
      ).href,
    );
    expect(screen.getByRole("table", { name: "Alerts sent" })).toHaveTextContent("data-team@example.com");
    expect(screen.getAllByTestId("event-item")).toHaveLength(2);
    // Only "Retry task" is allowed for a repaired incident.
    const bar = screen.getByTestId("action-bar");

    expect(
      within(bar)
        .getAllByRole("button")
        .map((button) => button.textContent),
    ).toEqual(["Retry task"]);
  });

  it("links the GitHub pull request when there is one", async () => {
    serve(codeIncident({ has_pr_md: false, pr_md_url: null, pr_url: "https://github.com/acme/etl/pull/12" }));
    renderPanel("customers_sync", "transform");
    expect(await screen.findByRole("link", { name: "Open pull request" })).toHaveAttribute(
      "href",
      "https://github.com/acme/etl/pull/12",
    );
    expect(screen.queryByRole("link", { name: "View PR.md" })).toBeNull();
  });

  it("shows a data incident with the violated rule and the masked sample", async () => {
    const { calls } = serve(dataIncident());

    renderPanel("sales_daily_etl", "load_sales", "3");
    expect(await screen.findByTestId("violated-rule")).toHaveTextContent("missing_column");
    expect(calls[0]?.search.get("map_index")).toBe("3");

    const violations = screen.getByRole("table", { name: "Contract violations" });

    expect(within(violations).getAllByRole("row")).toHaveLength(3);
    expect(within(violations).getByText("nulls_not_allowed")).toBeInTheDocument();

    expect(screen.getByTestId("masked-label")).toHaveTextContent(MASKED_LABEL);
    const sample = screen.getByRole("table", { name: "Masked data sample" });

    expect(within(sample).getByText("h:3f2a9c1b")).toBeInTheDocument();
    expect(within(sample).getByText("null")).toBeInTheDocument();
    expect(
      within(sample)
        .getAllByRole("columnheader")
        .map((header) => header.textContent),
    ).toEqual(["order_id", "customer_email", "amt"]);
    expect(screen.queryByTestId("diff-view")).toBeNull();
  });

  it("queues a human decision on an ambiguous incident and polls until the worker applies it", async () => {
    let state: IncidentDetail = ambiguousIncident();
    let gets = 0;
    const { calls } = mockApi([
      {
        handler: () => {
          gets += 1;
          // The worker applies the command after a couple of polls.
          if (state.pending_commands.length > 0 && gets >= 4) {
            state = ambiguousIncident({
              allowed_actions: [],
              category: "code",
              pending_commands: [],
              status: "repairing",
            });
          }

          return { incident: state };
        },
        path: TI_ROUTE,
      },
      {
        handler: () => {
          state = ambiguousIncident({
            pending_commands: [
              { actor: "admin", created_at: "2026-10-17T13:01:00+00:00", id: 9, kind: "resolve" },
            ],
          });

          return { command_id: 9, status: "pending" };
        },
        method: "POST",
        path: "dags/inventory_report/incidents/ambiambiambiambiambiambiambiambi/resolve",
      },
    ]);

    renderPanel("inventory_report", "build_report");
    const bar = await screen.findByTestId("action-bar");

    expect(
      within(bar)
        .getAllByRole("button")
        .map((button) => button.textContent),
    ).toEqual(["Resolve as data", "Resolve as code", "Ignore"]);

    fireEvent.click(within(bar).getByRole("button", { name: "Resolve as code" }));
    const dialog = await screen.findByRole("alertdialog");

    expect(within(dialog).getByText("Resolve as code?")).toBeInTheDocument();
    fireEvent.change(within(dialog).getByRole("textbox"), { target: { value: "parser bug" } });
    fireEvent.click(within(dialog).getByRole("button", { name: "Resolve as code" }));

    expect(await screen.findByText(QUEUED_MESSAGE)).toBeInTheDocument();
    const post = calls.find((call) => call.method === "POST");

    expect(post?.body).toEqual({ note: "parser bug", resolution: "code" });
    expect(post?.headers.get("X-Requested-With")).toBe("dag-healer");
    expect(post?.headers.get("Content-Type")).toBe("application/json");

    // While the command is pending the buttons are disabled.
    await waitFor(() =>
      expect(within(screen.getByTestId("action-bar")).getByRole("button", { name: "Ignore" })).toBeDisabled(),
    );
    // Fast polling (40 ms in tests) continues until pending_commands is empty.
    await waitFor(() => expect(screen.queryByText(QUEUED_MESSAGE)).toBeNull(), { timeout: 3000 });
    expect(screen.getAllByText("Repairing").length).toBeGreaterThan(0);
    expect(screen.queryByTestId("action-bar")).toBeNull();
    const pollsAfterApply = gets;

    await new Promise((resolve) => setTimeout(resolve, 200));
    // Back to the regular (slow) interval: no more fast polls.
    expect(gets).toBe(pollsAfterApply);
  });

  it("shows the API error inside the dialog when the action is no longer allowed (409)", async () => {
    mockApi([
      { handler: () => ({ incident: ambiguousIncident() }), path: TI_ROUTE },
      {
        handler: () => jsonResponse(409, { detail: "incident is ignored" }),
        method: "POST",
        path: /\/resolve$/u,
      },
    ]);
    renderPanel("inventory_report", "build_report");
    fireEvent.click(await screen.findByRole("button", { name: "Ignore" }));
    const dialog = await screen.findByRole("alertdialog");

    fireEvent.click(within(dialog).getByRole("button", { name: "Ignore" }));
    expect(await within(dialog).findByRole("alert")).toHaveTextContent(
      "This action is no longer allowed for the current status: incident is ignored",
    );
    expect(screen.queryByText(QUEUED_MESSAGE)).toBeNull();
  });

  it("hides the action bar for viewers without edit permission", async () => {
    serve(ambiguousIncident({ allowed_actions: [], can_edit: false }));
    renderPanel("inventory_report", "build_report");
    expect(await screen.findByRole("heading", { name: "Healer incident" })).toBeInTheDocument();
    expect(screen.queryByTestId("action-bar")).toBeNull();
    expect(screen.queryByRole("button", { name: "Resolve as data" })).toBeNull();
  });

  it("hides the action bar when can_edit is false even if actions are listed", async () => {
    serve(ambiguousIncident({ can_edit: false }));
    renderPanel("inventory_report", "build_report");
    await screen.findByRole("heading", { name: "Healer incident" });
    expect(screen.queryByTestId("action-bar")).toBeNull();
  });

  it("says so when no incident exists for the task instance", async () => {
    serve(null);
    renderPanel("sales_daily_etl", "extract");
    expect(await screen.findByText(NO_TI_INCIDENT_MESSAGE)).toBeInTheDocument();
  });

  it("treats a missing route (404) as no incident", async () => {
    mockApi([]);
    renderPanel("sales_daily_etl", "extract");
    expect(await screen.findByText(NO_TI_INCIDENT_MESSAGE)).toBeInTheDocument();
  });

  it("shows the session banner on 401", async () => {
    mockApi([{ handler: () => jsonResponse(401, { detail: "Not authenticated" }), path: TI_ROUTE }]);
    renderPanel("sales_daily_etl", "extract");
    expect(await screen.findByText(SESSION_EXPIRED_MESSAGE)).toBeInTheDocument();
    expect(screen.queryByText(NO_TI_INCIDENT_MESSAGE)).toBeNull();
  });

  it("shows the no-access message on 403", async () => {
    mockApi([{ handler: () => jsonResponse(403, { detail: "Forbidden" }), path: TI_ROUTE }]);
    renderPanel("secret_dag", "extract");
    expect(await screen.findByText(NO_DAG_ACCESS_MESSAGE)).toBeInTheDocument();
  });
});
