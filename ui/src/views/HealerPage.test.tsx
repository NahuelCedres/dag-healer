import { cleanup, fireEvent, screen, waitFor, within } from "@testing-library/react";
import { afterEach, describe, expect, it, vi } from "vitest";

import { HealerNav } from "src/main";
import { SESSION_EXPIRED_MESSAGE } from "src/messages";
import { dagSummary, draftContract, approvedContract, RUN_ID, summary } from "src/test/fixtures";
import { jsonResponse, mockApi, renderWithPolling } from "src/test/utils";

afterEach(() => {
  // Unmount first so no polling request can reach the real fetch once it is unstubbed.
  cleanup();
  vi.unstubAllGlobals();
});

const INCIDENTS = [
  summary({
    category: "data",
    created_at: "2026-10-17T10:00:00+00:00",
    dag_id: "sales_daily_etl",
    id: "i-data",
    status: "reported",
    task_id: "load_sales",
  }),
  summary({ id: "i-code", status: "repaired" }),
  summary({
    category: "ambiguous",
    created_at: "2026-10-17T13:00:00+00:00",
    dag_id: "inventory_report",
    error_summary: "KeyError: \"['qty'] not in index\"\nTraceback...",
    id: "i-amb",
    map_index: 2,
    status: "waiting_human",
    task_id: "build_report",
  }),
];

const standardRoutes = () =>
  mockApi([
    { handler: () => ({ incidents: INCIDENTS, total: 3 }), path: "incidents" },
    {
      handler: () => ({
        dags: [
          dagSummary({ last_incident: INCIDENTS[0] ?? null }),
          dagSummary({ contract_status: "draft", dag_id: "inventory_report", open_incidents: 1 }),
        ],
      }),
      path: "dags",
    },
    { handler: () => ({ contracts: [approvedContract(), draftContract()] }), path: "contracts" },
    {
      handler: () => ({ command_id: 7, status: "pending" }),
      method: "POST",
      path: "dags/inventory_report/contract/approve",
    },
  ]);

describe("HealerPage", () => {
  it("shows counts, the incident timeline newest first and links to the task-instance tab", async () => {
    const { calls } = standardRoutes();

    renderWithPolling(<HealerNav />);
    const items = await screen.findAllByTestId("incident-item");

    expect(items.map((item) => item.dataset.incidentId)).toEqual(["i-amb", "i-code", "i-data"]);
    expect(calls.find((call) => call.path === "incidents")?.search.get("limit")).toBe("500");

    const first = items[0] as HTMLElement;

    expect(within(first).getByText("Ambiguous")).toBeInTheDocument();
    expect(within(first).getByText("Waiting for a human")).toBeInTheDocument();
    expect(within(first).getByText("build_report [2]")).toBeInTheDocument();
    // The one-line headline is the first line of the error.
    expect(within(first).getByText("KeyError: \"['qty'] not in index\"")).toBeInTheDocument();
    expect(within(first).getByRole("link", { name: "Open incident" })).toHaveAttribute(
      "href",
      new URL(
        `dags/inventory_report/runs/${encodeURIComponent(RUN_ID)}/tasks/build_report/mapped/2/plugin/healer-ti`,
        document.baseURI,
      ).href,
    );

    const counts = screen.getByTestId("status-counts");

    expect(within(counts).getByText("Open").nextSibling?.textContent).toBe("1");
    expect(within(counts).getByText("Repaired").nextSibling?.textContent).toBe("1");
    expect(within(screen.getByTestId("category-counts")).getByText("Data: 1")).toBeInTheDocument();
  });

  it("filters the timeline by category, status and DAG", async () => {
    standardRoutes();
    renderWithPolling(<HealerNav />);
    await screen.findAllByTestId("incident-item");

    fireEvent.change(screen.getByLabelText("Filter by category"), { target: { value: "data" } });
    expect(screen.getAllByTestId("incident-item").map((item) => item.dataset.incidentId)).toEqual(["i-data"]);

    fireEvent.change(screen.getByLabelText("Filter by category"), { target: { value: "" } });
    fireEvent.change(screen.getByLabelText("Filter by status"), { target: { value: "repaired" } });
    expect(screen.getAllByTestId("incident-item").map((item) => item.dataset.incidentId)).toEqual(["i-code"]);

    fireEvent.change(screen.getByLabelText("Filter by DAG"), { target: { value: "sales_daily_etl" } });
    expect(screen.queryAllByTestId("incident-item")).toHaveLength(0);
    expect(screen.getByText("No incident matches the filters.")).toBeInTheDocument();
  });

  it("lists monitored DAGs and contracts, and approves a draft contract after confirmation", async () => {
    const { calls } = standardRoutes();

    renderWithPolling(<HealerNav />);
    const dagsTable = await screen.findByRole("table", { name: "Monitored DAGs" });

    expect(within(dagsTable).getByRole("link", { name: "inventory_report" })).toHaveAttribute(
      "href",
      new URL("dags/inventory_report/plugin/healer-dag", document.baseURI).href,
    );

    const contractsTable = await screen.findByRole("table", { name: "Contracts" });
    const rows = within(contractsTable).getAllByRole("row");
    const approvedRow = rows.find((row) => row.dataset.dagId === "sales_daily_etl") as HTMLElement;
    const draftRow = rows.find((row) => row.dataset.dagId === "inventory_report") as HTMLElement;

    expect(within(approvedRow).queryByRole("button", { name: "Approve" })).toBeNull();
    expect(within(approvedRow).getByText("admin")).toBeInTheDocument();

    fireEvent.click(within(draftRow).getByRole("button", { name: "Approve" }));
    const dialog = await screen.findByRole("alertdialog");

    fireEvent.change(within(dialog).getByRole("textbox"), { target: { value: "looks right" } });
    fireEvent.click(within(dialog).getByRole("button", { name: "Approve contract" }));

    await waitFor(() => expect(within(draftRow).getByTestId("approval-pending")).toBeInTheDocument());
    const post = calls.find((call) => call.method === "POST");

    expect(post?.path).toBe("dags/inventory_report/contract/approve");
    expect(post?.body).toEqual({ note: "looks right" });
    expect(post?.headers.get("X-Requested-With")).toBe("dag-healer");
  });

  it("shows the session banner on 401", async () => {
    mockApi([{ handler: () => jsonResponse(401, { detail: "Not authenticated" }), path: /.*/u }]);
    renderWithPolling(<HealerNav />);
    expect(await screen.findByText(SESSION_EXPIRED_MESSAGE)).toBeInTheDocument();
    expect(screen.getAllByTestId("session-expired")).toHaveLength(1);
  });

  it("renders empty states when the API routes are missing (404)", async () => {
    mockApi([]);
    renderWithPolling(<HealerNav />);
    expect(await screen.findByText("No incidents recorded yet.")).toBeInTheDocument();
    expect(await screen.findByText("No monitored DAGs.")).toBeInTheDocument();
    expect(await screen.findByText("No data contracts yet.")).toBeInTheDocument();
    expect(screen.queryByRole("alert")).toBeNull();
  });
});
