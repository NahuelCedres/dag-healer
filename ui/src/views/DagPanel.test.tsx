import { cleanup, screen, within } from "@testing-library/react";
import { afterEach, describe, expect, it, vi } from "vitest";

import { HealerDagIncidents } from "src/main";
import { NO_DAG_ACCESS_MESSAGE, NOT_MONITORED_MESSAGE } from "src/messages";
import { draftContract, summary } from "src/test/fixtures";
import { jsonResponse, mockApi, renderWithPolling } from "src/test/utils";

afterEach(() => {
  // Unmount first so no polling request can reach the real fetch once it is unstubbed.
  cleanup();
  vi.unstubAllGlobals();
});

describe("DagPanel", () => {
  it("shows the contract with its column rules and the incidents of the DAG", async () => {
    const { calls } = mockApi([
      {
        handler: () => ({
          can_edit: true,
          contract: draftContract(),
          incidents: [
            summary({ category: "ambiguous", dag_id: "inventory_report", id: "i1", status: "waiting_human" }),
          ],
          monitored: true,
        }),
        path: "dags/inventory_report/incidents",
      },
    ]);

    renderWithPolling(<HealerDagIncidents dagId="inventory_report" />);
    const card = await screen.findByTestId("contract-card");

    expect(within(card).getByText("Draft")).toBeInTheDocument();
    expect(within(card).getByRole("button", { name: "Approve" })).toBeInTheDocument();
    const rules = within(card).getByRole("table", { name: "Column rules of inventory" });

    expect(within(rules).getByText("qty")).toBeInTheDocument();
    expect(within(rules).getByText("1000")).toBeInTheDocument();
    expect(within(card).getByText("data/incoming/inventory.csv")).toBeInTheDocument();

    const items = screen.getAllByTestId("incident-item");

    expect(items).toHaveLength(1);
    // The DAG name is not repeated inside its own tab.
    expect(within(items[0] as HTMLElement).queryByRole("link", { name: "inventory_report" })).toBeNull();
    expect(calls[0]?.path).toBe("dags/inventory_report/incidents");
    expect(screen.queryByTestId("not-monitored")).toBeNull();
  });

  it("explains how to monitor a DAG that is not monitored", async () => {
    mockApi([
      {
        handler: () => ({ can_edit: false, contract: null, incidents: [], monitored: false }),
        path: "dags/other_dag/incidents",
      },
    ]);
    renderWithPolling(<HealerDagIncidents dagId="other_dag" />);
    expect(await screen.findByText(NOT_MONITORED_MESSAGE)).toBeInTheDocument();
    expect(screen.queryByTestId("contract-card")).toBeNull();
  });

  it("tells the user when they cannot read the DAG (403)", async () => {
    mockApi([{ handler: () => jsonResponse(403, { detail: "Forbidden" }), path: /.*/u }]);
    renderWithPolling(<HealerDagIncidents dagId="secret_dag" />);
    expect(await screen.findByText(NO_DAG_ACCESS_MESSAGE)).toBeInTheDocument();
  });

  it("falls back to an empty state when the route does not exist (404)", async () => {
    mockApi([]);
    renderWithPolling(<HealerDagIncidents dagId="sales_daily_etl" />);
    expect(await screen.findByText("No dag-healer data for this DAG yet.")).toBeInTheDocument();
  });

  it("reads the DAG from the URL when the host passes no props", async () => {
    const { calls } = mockApi([]);

    window.history.pushState({}, "", "/dags/from_url/plugin/healer-dag");
    try {
      renderWithPolling(<HealerDagIncidents />);
      expect(await screen.findByText("from_url")).toBeInTheDocument();
      expect(calls[0]?.path).toBe("dags/from_url/incidents");
    } finally {
      window.history.pushState({}, "", "/");
    }
  });
});
