import { cleanup, fireEvent, screen, waitFor, within } from "@testing-library/react";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";

import { App } from "src/App";
import {
  ambiguousIncident,
  approvedContract,
  codeIncident,
  dagSummary,
  dataIncident,
  DRAFT_YAML,
  draftContract,
  PR_MD,
  setup,
  summary,
} from "src/test/fixtures";
import { jsonResponse, mockApi, renderWithPolling, type Route } from "src/test/utils";

import { PREFS_KEY } from "./prefs";

const AMBIGUOUS = ambiguousIncident();
const DATA = dataIncident();
const CODE = codeIncident();

const list = [
  summary({ ...AMBIGUOUS, links: undefined }),
  summary({ ...DATA, created_at: "2026-10-17T11:00:00+00:00", links: undefined }),
  summary({ ...CODE, created_at: "2026-10-17T10:00:00+00:00", links: undefined }),
  summary({
    category: null,
    dag_id: "customers_sync",
    id: "newnewnewnewnewnewnewnewnewnewne",
    status: "triaging",
    task_id: "extract",
  }),
];

const consoleApi = (extra: Array<Route> = []) =>
  mockApi([
    ...extra,
    { handler: () => ({ incidents: list, total: list.length }), path: "incidents" },
    {
      handler: () => ({
        dags: [
          dagSummary({ contract_status: "draft", dag_id: "inventory_report", open_incidents: 1 }),
          dagSummary({ dag_id: "sales_daily_etl" }),
        ],
      }),
      path: "dags",
    },
    { handler: () => setup() as unknown as Record<string, unknown>, path: "setup" },
    {
      handler: () => AMBIGUOUS as unknown as Record<string, unknown>,
      path: `dags/inventory_report/incidents/${AMBIGUOUS.id}`,
    },
    {
      handler: () => DATA as unknown as Record<string, unknown>,
      path: `dags/sales_daily_etl/incidents/${DATA.id}`,
    },
    {
      handler: () => CODE as unknown as Record<string, unknown>,
      path: `dags/customers_sync/incidents/${CODE.id}`,
    },
    { handler: () => PR_MD, path: `dags/customers_sync/incidents/${CODE.id}/pr.md` },
    {
      handler: () => draftContract() as unknown as Record<string, unknown>,
      path: "dags/inventory_report/contract",
    },
    { handler: () => DRAFT_YAML, path: "dags/inventory_report/contract.yaml" },
    {
      handler: () => approvedContract({ datasets: [] }) as unknown as Record<string, unknown>,
      path: "dags/sales_daily_etl/contract",
    },
    {
      handler: () => ({ can_edit: true, contract: draftContract(), incidents: [list[0]], monitored: true }),
      path: "dags/inventory_report/incidents",
    },
  ]);

const at = (path: string) => window.history.pushState({}, "", path);

beforeEach(() => {
  globalThis.localStorage.removeItem(PREFS_KEY);
  at("/plugin/healer");
});

afterEach(() => {
  cleanup();
  vi.unstubAllGlobals();
  at("/");
});

describe("the inbox", () => {
  it("groups every incident by what it needs, in the sidebar and in the table", async () => {
    consoleApi();
    renderWithPolling(<App mode="nav" />);

    const table = await screen.findByTestId("inbox-table");

    expect(within(table).getAllByRole("row")).toHaveLength(list.length + 1);
    expect(screen.getByRole("button", { name: "Needs you 1" })).toBeInTheDocument();
    expect(screen.getByRole("button", { name: "In progress 1" })).toBeInTheDocument();
    expect(screen.getByRole("button", { name: "Done 2" })).toBeInTheDocument();
    const sidebar = screen.getByTestId("sidebar");

    expect(within(sidebar).getByRole("heading", { name: /Needs you/u })).toBeInTheDocument();
    expect(within(sidebar).getByRole("link", { name: /build_report.*inventory_report/u })).toHaveAttribute(
      "href",
      new URL(`plugin/healer?dag=inventory_report&incident=${AMBIGUOUS.id}`, document.baseURI).href,
    );
    expect(within(table).getByText("Review PR.md and open the pull request")).toBeInTheDocument();
  });

  it("filters by group and searches pipelines, steps and errors", async () => {
    consoleApi();
    renderWithPolling(<App mode="nav" />);
    const table = await screen.findByTestId("inbox-table");

    fireEvent.click(screen.getByRole("button", { name: "Needs you 1" }));
    expect(within(table).getAllByRole("row")).toHaveLength(2);
    fireEvent.click(screen.getByRole("button", { name: "All 4" }));
    fireEvent.change(screen.getByLabelText("Search"), { target: { value: "amount" } });
    expect(within(screen.getByTestId("inbox-table")).getAllByRole("row")).toHaveLength(2);
    expect(
      within(screen.getByTestId("inbox-table")).getByText("sales_daily_etl / transform_sales"),
    ).toBeInTheDocument();
    fireEvent.change(screen.getByLabelText("Search"), { target: { value: "nothing like this" } });
    expect(screen.getByTestId("inbox-none")).toHaveTextContent("No incident matches.");
    fireEvent.click(screen.getByRole("button", { name: "Clear the filters" }));
    expect(within(screen.getByTestId("inbox-table")).getAllByRole("row")).toHaveLength(list.length + 1);
  });

  it("keeps its group and filters in the URL, so Back returns to the same list", async () => {
    consoleApi();
    at("/plugin/healer?group=needs&q=inventory");
    renderWithPolling(<App mode="nav" />);
    const table = await screen.findByTestId("inbox-table");

    expect(screen.getByLabelText("Search")).toHaveValue("inventory");
    expect(screen.getByRole("button", { name: "Needs you 1" })).toHaveAttribute("aria-pressed", "true");
    expect(within(table).getAllByRole("row")).toHaveLength(2);
    fireEvent.change(screen.getByLabelText("Search"), { target: { value: "amount" } });
    fireEvent.click(screen.getByRole("button", { name: "All 4" }));
    await waitFor(() => expect(globalThis.location.search).toBe("?q=amount"));
    await waitFor(() => expect(screen.getByTestId("live-region")).toHaveTextContent("1 incident matches."));
  });

  it("moves an answered incident to In progress until the worker applies the answer", async () => {
    const queued = {
      ...list[0],
      pending_command: {
        action: "resolve_data",
        actor: "ann",
        created_at: "2026-10-17T12:10:00+00:00",
        id: 3,
        kind: "resolve",
      },
    };

    consoleApi([
      { handler: () => ({ incidents: [queued, ...list.slice(1)], total: list.length }), path: "incidents" },
    ]);
    renderWithPolling(<App mode="nav" />);
    const table = await screen.findByTestId("inbox-table");

    expect(screen.getByRole("button", { name: "Needs you 0" })).toBeInTheDocument();
    expect(screen.getByRole("button", { name: "In progress 2" })).toBeInTheDocument();
    expect(within(table).getByText("Queued: report a data problem")).toBeInTheDocument();
    expect(
      within(screen.getByTestId("sidebar")).getByText("Nothing needs you right now."),
    ).toBeInTheDocument();
  });

  it("opens an incident in place and puts focus on its heading", async () => {
    consoleApi();
    renderWithPolling(<App mode="nav" />);
    const table = await screen.findByTestId("inbox-table");

    fireEvent.click(within(table).getByRole("link", { name: "inventory_report / build_report" }));
    const heading = await screen.findByRole("heading", { level: 1, name: "build_report failed" });

    expect(globalThis.location.search).toBe(`?dag=inventory_report&incident=${AMBIGUOUS.id}`);
    await waitFor(() => expect(document.activeElement).toBe(heading));
  });
});

describe("an incident that waits for a person", () => {
  it("leads with the question, then shows each answer's consequences before it is confirmed", async () => {
    const { calls } = consoleApi([
      { handler: () => ({ command_id: 7, status: "pending" }), method: "POST", path: /resolve$/u },
    ]);

    at(`/plugin/healer?dag=inventory_report&incident=${AMBIGUOUS.id}`);
    renderWithPolling(<App mode="nav" />);

    const decision = await screen.findByTestId("decision");

    expect(
      await within(decision).findByRole("heading", { name: "Did inventory.csv change on purpose?" }),
    ).toBeInTheDocument();
    expect(screen.getByTestId("decided-by")).toHaveTextContent("Waiting for you:");
    fireEvent.click(within(decision).getByRole("button", { name: /No, the file is wrong/u }));

    const panel = await screen.findByTestId("confirm-panel");

    await waitFor(() => expect(panel).toHaveTextContent("Emails data-team@example.com"));
    expect(panel).toHaveTextContent("The contract stays a draft");
    expect(document.activeElement).toBe(within(panel).getByRole("heading", { level: 3 }));
    fireEvent.change(within(panel).getByLabelText("Note for the timeline (optional)"), {
      target: { value: "The warehouse export is broken" },
    });
    fireEvent.click(within(panel).getByRole("button", { name: "Report a data problem" }));

    expect(await screen.findByTestId("queued")).toHaveTextContent("Queued: report a data problem.");
    const post = calls.find((call) => call.method === "POST");

    expect(post?.path).toBe(`dags/inventory_report/incidents/${AMBIGUOUS.id}/resolve`);
    expect(post?.body).toEqual({ note: "The warehouse export is broken", resolution: "data" });
  });

  it("puts focus back on the answer when its confirmation is cancelled or refused", async () => {
    consoleApi([
      {
        handler: () => jsonResponse(409, { detail: "Another action is already pending for this incident" }),
        method: "POST",
        path: /resolve$/u,
      },
    ]);
    at(`/plugin/healer?dag=inventory_report&incident=${AMBIGUOUS.id}`);
    renderWithPolling(<App mode="nav" />);

    const decision = await screen.findByTestId("decision");
    const answer = await within(decision).findByRole("button", { name: /No, the file is wrong/u });

    fireEvent.click(answer);
    fireEvent.click(
      within(await screen.findByTestId("confirm-panel")).getByRole("button", { name: "Cancel" }),
    );
    expect(screen.queryByTestId("confirm-panel")).toBeNull();
    expect(document.activeElement).toBe(answer);

    fireEvent.click(answer);
    const panel = await screen.findByTestId("confirm-panel");

    fireEvent.click(within(panel).getByRole("button", { name: "Report a data problem" }));
    const refusal = await within(panel).findByTestId("action-error");

    expect(refusal).toHaveTextContent("This is no longer possible: Another action is already pending");
    await waitFor(() => expect(document.activeElement).toBe(refusal.parentElement));
    expect(within(panel).getByRole("button", { name: "Report a data problem" })).not.toHaveAttribute(
      "aria-disabled",
      "true",
    );
  });

  it("warns before approving a contract that this incident's input breaks", async () => {
    consoleApi();
    at(`/plugin/healer?dag=inventory_report&incident=${AMBIGUOUS.id}`);
    renderWithPolling(<App mode="nav" />);

    const decision = await screen.findByTestId("decision");

    fireEvent.click(await within(decision).findByRole("button", { name: "Approve the contract" }));
    expect(await screen.findByTestId("confirm-panel")).toHaveTextContent(
      "This incident's input breaks this contract (column qty is missing).",
    );
  });

  it("shows the broken rule in plain words next to the lines of its YAML file", async () => {
    consoleApi();
    at(`/plugin/healer?dag=inventory_report&incident=${AMBIGUOUS.id}`);
    renderWithPolling(<App mode="nav" />);

    const check = await screen.findByTestId("contract-check");

    expect(check).toHaveTextContent("The contract expects a column qty; the file has none.");
    await waitFor(() => expect(within(check).getByRole("figure")).toHaveTextContent("- name: qty"));
    expect(within(check).getByRole("figure")).toHaveTextContent(
      "healer/contracts/inventory_report.yaml, line 19–23",
    );
    expect(screen.getByTestId("sample")).toHaveTextContent("empty");
  });

  it("offers no answer to someone who may not act", async () => {
    consoleApi([
      {
        handler: () =>
          ambiguousIncident({ allowed_actions: [], can_edit: false }) as unknown as Record<string, unknown>,
        path: `dags/inventory_report/incidents/${AMBIGUOUS.id}`,
      },
    ]);
    at(`/plugin/healer?dag=inventory_report&incident=${AMBIGUOUS.id}`);
    renderWithPolling(<App mode="nav" />);

    const decision = await screen.findByTestId("decision");

    expect(decision).toHaveTextContent("You can read this incident but not act on it");
    expect(within(decision).queryAllByRole("button")).toHaveLength(0);
  });
});

describe("a reported data problem and a proposed fix", () => {
  it("says who was told and what is left to do", async () => {
    consoleApi();
    at(`/plugin/healer?dag=sales_daily_etl&incident=${DATA.id}`);
    renderWithPolling(<App mode="nav" />);

    const reported = await screen.findByTestId("reported");

    expect(reported).toHaveTextContent("dag-healer emailed data-team@example.com");
    expect(
      within(reported).getByRole("button", { name: "Retry task after the input is fixed" }),
    ).toBeInTheDocument();
    expect(screen.getByTestId("decided-by")).toHaveTextContent("Decided by a fixed rule:");
  });

  it("shows the test results, the diff, PR.md and the CI note", async () => {
    consoleApi();
    at(`/plugin/healer?dag=customers_sync&incident=${CODE.id}`);
    renderWithPolling(<App mode="nav" />);

    const fix = await screen.findByTestId("fix");

    expect(within(fix).getByTestId("test-results")).toHaveTextContent("before the fix: 1 failed, 1 passed");
    expect(within(fix).getByTestId("test-results")).toHaveTextContent("after: 2 passed");
    expect(within(fix).getAllByTestId("diff-file")).toHaveLength(2);
    expect(await within(fix).findByTestId("ci-note")).toHaveTextContent("Its commit carries [skip ci]");
    fireEvent.click(within(fix).getByRole("button", { name: "View PR.md" }));
    const dialog = await screen.findByRole("dialog");

    expect(dialog).toHaveTextContent("dag-healer fix");
    fireEvent.keyDown(document, { key: "Escape" });
    await waitFor(() => expect(screen.queryByRole("dialog")).toBeNull());
    expect(screen.getByTestId("fix-proposed")).toHaveTextContent("dag-healer never merges");
  });
});

describe("the pipelines", () => {
  it("list every pipeline and show a contract in plain words, its file and its approval", async () => {
    const { calls } = consoleApi([
      {
        handler: () => ({ command_id: 9, status: "pending" }),
        method: "POST",
        path: "dags/inventory_report/contract/approve",
      },
    ]);

    at("/plugin/healer?view=pipelines&pipeline=inventory_report");
    renderWithPolling(<App mode="nav" />);

    expect(await screen.findByTestId("pipelines-table")).toHaveTextContent("inventory_report");
    const contract = await screen.findByTestId("contract");

    expect(contract).toHaveTextContent(
      "Checks data/incoming/inventory.csv, the input of the task get_inventory.",
    );
    expect(contract).toHaveTextContent("A whole number, never empty, between 0 and 3,100");
    fireEvent.click(within(contract).getByRole("button", { name: "YAML file" }));
    expect(await within(contract).findByTestId("contract-yaml")).toHaveTextContent(
      "allow_extra_columns: true",
    );
    fireEvent.click(within(contract).getByRole("button", { name: "Approve the contract" }));
    const panel = await within(contract).findByTestId("confirm-panel");

    expect(panel).toHaveTextContent("It does not commit the file or open a pull request");
    fireEvent.click(within(panel).getByRole("button", { name: "Approve the contract" }));
    await waitFor(() =>
      expect(
        calls.some(
          (call) => call.method === "POST" && call.path === "dags/inventory_report/contract/approve",
        ),
      ).toBe(true),
    );
  });
});

describe("the side panel", () => {
  it("hides and shows with its button and with ], remembers it, and moves focus", async () => {
    consoleApi();
    at(`/plugin/healer?dag=inventory_report&incident=${AMBIGUOUS.id}`);
    renderWithPolling(<App mode="nav" />);

    await screen.findByTestId("decision");
    expect(screen.getByTestId("side-panel")).toBeVisible();
    fireEvent.click(screen.getByTestId("panel-hide"));
    await waitFor(() => expect(document.activeElement).toBe(screen.getByTestId("panel-show")));
    expect(screen.getByTestId("latest")).toHaveTextContent("Latest:");
    expect(JSON.parse(globalThis.localStorage.getItem(PREFS_KEY) ?? "{}")).toMatchObject({ panel: false });

    // ⌥] on macOS Spanish layouts types "]" with altKey set.
    fireEvent.keyDown(document.body, { altKey: true, key: "]" });
    await waitFor(() => expect(screen.queryByTestId("panel-show")).toBeNull());
    expect(screen.getByTestId("side-panel")).toBeVisible();
  });

  it("ignores ] while typing, and when single-key shortcuts are off", async () => {
    consoleApi();
    renderWithPolling(<App mode="nav" />);

    await screen.findByTestId("inbox-table");
    fireEvent.keyDown(screen.getByLabelText("Search"), { key: "]" });
    expect(screen.getByTestId("side-panel")).toBeVisible();
    fireEvent.click(screen.getByLabelText(/Single-key shortcuts/u));
    fireEvent.keyDown(document.body, { key: "]" });
    expect(screen.getByTestId("side-panel")).toBeVisible();
    expect(JSON.parse(globalThis.localStorage.getItem(PREFS_KEY) ?? "{}")).toMatchObject({
      shortcuts: false,
    });
  });
});

describe("the Airflow tabs", () => {
  it("embed the incident of a task instance without the console's sidebar", async () => {
    const { calls } = mockApi([
      { handler: () => ({ incident: AMBIGUOUS }), path: "dags/inventory_report/task-instance" },
      { handler: () => setup() as unknown as Record<string, unknown>, path: "setup" },
      {
        handler: () => draftContract() as unknown as Record<string, unknown>,
        path: "dags/inventory_report/contract",
      },
      { handler: () => DRAFT_YAML, path: "dags/inventory_report/contract.yaml" },
    ]);

    renderWithPolling(
      <App dagId="inventory_report" mode="task_instance" runId={AMBIGUOUS.run_id} taskId="build_report" />,
    );

    expect(await screen.findByTestId("decision")).toBeInTheDocument();
    expect(screen.queryByTestId("sidebar")).toBeNull();
    for (const link of screen.getAllByRole("link", { name: "inventory_report" })) {
      expect(link).toHaveAttribute(
        "href",
        new URL("dags/inventory_report/plugin/healer-dag", document.baseURI).href,
      );
    }
    expect(
      calls.find((call) => call.path === "dags/inventory_report/task-instance")?.search.get("task_id"),
    ).toBe("build_report");
    expect(calls.some((call) => call.path === "me")).toBe(false);
  });

  it("ask for a task instance when the context has none", () => {
    mockApi([]);
    at("/plugin/healer");
    renderWithPolling(<App mode="task_instance" />);
    expect(screen.getByText("Open a task instance to see its dag-healer incident.")).toBeInTheDocument();
  });
});
