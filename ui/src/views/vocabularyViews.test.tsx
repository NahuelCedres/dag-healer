import { cleanup, fireEvent, screen, within } from "@testing-library/react";
import { afterEach, describe, expect, it, vi } from "vitest";

import { App, type HealerMode } from "src/App";
import { setAuthorizationProvider } from "src/api";
import { codeIncident, dagSummary, RUN_ID, summary } from "src/test/fixtures";
import { jsonResponse, mockApi, renderWithPolling } from "src/test/utils";
import { type UiText, vocabularyFrom } from "src/vocabulary";

afterEach(() => {
  // Unmount first so no polling request can reach the real fetch once it is unstubbed.
  cleanup();
  setAuthorizationProvider();
  vi.unstubAllGlobals();
});

/** What the standalone host provides for a Fakeflow instance (the words of its `/me`). */
const FAKEFLOW: UiText = {
  host: "standalone",
  words: vocabularyFrom({
    attempt: "attempt",
    map_index: "map index",
    pipeline: "flow",
    pipelines: "flows",
    product: "Fakeflow",
    retry_action: "Retry step",
    run: "run",
    step: "step",
    steps: "steps",
  }),
};

const STEP_URL = `http://fakeflow.localhost/flows/customers_sync/runs/${encodeURIComponent(RUN_ID)}/steps/transform?attempt=1`;
const NATIVE = { links: { native: { Fakeflow: STEP_URL } }, orchestrator: "fake" };
const WARNING = "The fix reaches Fakeflow only after it is deployed.";

const TI_ROUTE = /^dags\/[^/]+\/task-instance$/u;

/** A retry and a repair the worker has not applied yet. */
const QUEUED = codeIncident({
  pending_commands: [
    { actor: "alice", created_at: "2026-10-17T13:01:00+00:00", id: 3, kind: "retry_task" },
    { actor: "bob", created_at: "2026-10-17T13:02:00+00:00", id: 4, kind: "retry_repair" },
  ],
});

/** The App of the standalone host (Fakeflow words), or of the plugin when `text` is null. */
const renderApp = (mode: HealerMode, props: Record<string, string> = {}, text: UiText | null = FAKEFLOW) =>
  renderWithPolling(<App mode={mode} text={text ?? undefined} {...props} />);

const renderTaskInstance = (text: UiText | null = FAKEFLOW) =>
  renderApp("task_instance", { dagId: "customers_sync", runId: RUN_ID, taskId: "transform" }, text);

describe("views with the orchestrator's words (standalone host)", () => {
  it("speak of flows and steps on the Healer page, with the native links", async () => {
    mockApi([
      { handler: () => ({ incidents: [summary({ ...NATIVE, id: "i-1" })], total: 1 }), path: "incidents" },
      { handler: () => ({ dags: [dagSummary({ dag_id: "customers_sync" })] }), path: "dags" },
      { handler: () => ({ contracts: [] }), path: "contracts" },
    ]);
    renderApp("nav");

    const item = await screen.findByTestId("incident-item");

    expect(within(item).getByRole("link", { name: "Open in Fakeflow" })).toHaveAttribute("href", STEP_URL);
    expect(within(item).getByRole("link", { name: "Open in Fakeflow" })).toHaveAttribute("target", "_blank");
    expect(within(item).getByText(`run ${RUN_ID} · attempt 1`)).toBeInTheDocument();
    expect(
      screen.getByText(
        "Step failures of monitored flows, classified by dag-healer as data, code or ambiguous.",
      ),
    ).toBeInTheDocument();
    expect(screen.getByRole("heading", { name: "Monitored flows" })).toBeInTheDocument();

    const table = await screen.findByRole("table", { name: "Monitored flows" });

    expect(within(table).getByRole("columnheader", { name: "Flow" })).toBeInTheDocument();
    const filter = screen.getByLabelText("Filter by flow");

    expect(within(filter).getByRole("option", { name: "All flows" })).toBeInTheDocument();
    expect(screen.queryByText(/DAG/u)).toBeNull();
  });

  it("explain the empty Healer page with the same words", async () => {
    mockApi([
      { handler: () => ({ incidents: [], total: 0 }), path: "incidents" },
      { handler: () => ({ dags: [] }), path: "dags" },
      { handler: () => ({ contracts: [] }), path: "contracts" },
    ]);
    renderApp("nav");

    expect(await screen.findByText("Failed steps of monitored flows will show up here.")).toBeInTheDocument();
    expect(await screen.findByText("No monitored flows.")).toBeInTheDocument();
    expect(
      screen.getByText("List flow ids (or glob patterns) under monitor.dags in healer.yaml."),
    ).toBeInTheDocument();
  });

  it("explain an unmonitored flow in the DAG view", async () => {
    mockApi([
      {
        handler: () => ({ can_edit: false, contract: null, incidents: [], monitored: false }),
        path: "dags/other_flow/incidents",
      },
    ]);
    renderApp("dag", { dagId: "other_flow" });

    expect(
      await screen.findByText(
        "This flow is not monitored by dag-healer. Add it to monitor.dags in healer.yaml.",
      ),
    ).toBeInTheDocument();
  });

  it("offer 'Retry step' with the server's warnings and the native link in the task-instance view", async () => {
    const { calls } = mockApi([
      {
        handler: () => ({
          incident: codeIncident({
            ...NATIVE,
            action_warnings: { retry_task: [WARNING] },
            allowed_actions: ["retry_task"],
          }),
        }),
        path: TI_ROUTE,
      },
      {
        handler: () => jsonResponse(401, { detail: "Not authenticated" }),
        method: "POST",
        path: /\/retry-task$/u,
      },
    ]);
    renderTaskInstance();

    const bar = await screen.findByTestId("action-bar");

    expect(
      within(bar)
        .getAllByRole("button")
        .map((button) => button.textContent),
    ).toEqual(["Retry step"]);
    expect(screen.getByTestId("native-links")).toContainElement(
      screen.getByRole("link", { name: "Open in Fakeflow" }),
    );
    expect(screen.getByRole("link", { name: "Open in Fakeflow" })).toHaveAttribute("href", STEP_URL);
    expect(screen.getByText(/· attempt 1 ·/u)).toBeInTheDocument();

    fireEvent.click(within(bar).getByRole("button", { name: "Retry step" }));
    const dialog = await screen.findByRole("alertdialog");

    expect(within(dialog).getByText("Retry step?")).toBeInTheDocument();
    expect(
      within(dialog).getByText(
        "Retry step: Fakeflow runs the failed step again. A code fix only takes effect once its branch is merged.",
      ),
    ).toBeInTheDocument();
    expect(within(dialog).getByTestId("action-warning")).toHaveTextContent(WARNING);

    fireEvent.click(within(dialog).getByRole("button", { name: "Retry step" }));
    expect(await within(dialog).findByRole("alert")).toHaveTextContent(
      "Your session expired — reload the page and try again.",
    );
    expect(calls.find((call) => call.method === "POST")?.path).toBe(
      "dags/customers_sync/incidents/a1b2c3d4e5f60718293a4b5c6d7e8f90/retry-task",
    );
  });

  it("name a queued retry with the orchestrator's retry action", async () => {
    mockApi([{ handler: () => ({ incident: QUEUED }), path: TI_ROUTE }]);
    renderTaskInstance();

    expect(await screen.findByTestId("queued")).toHaveTextContent("retry step by alice; retry repair by bob");
  });

  it("show the standalone session banner and the step words in the empty states", async () => {
    mockApi([{ handler: () => jsonResponse(401, { detail: "Not authenticated" }), path: TI_ROUTE }]);
    renderTaskInstance();
    expect(await screen.findByText("Your session expired — reload the page")).toBeInTheDocument();
    cleanup();

    mockApi([{ handler: () => ({ incident: null }), path: TI_ROUTE }]);
    renderTaskInstance();
    expect(await screen.findByText("No incident recorded for this step instance.")).toBeInTheDocument();
    expect(
      screen.getByText("dag-healer records an incident when this step instance fails."),
    ).toBeInTheDocument();
    cleanup();

    mockApi([]);
    renderApp("task_instance");
    expect(screen.getByText("Open a step instance to see its dag-healer incident.")).toBeInTheDocument();
  });
});

describe("views without words (Airflow plugin)", () => {
  it("keep today's texts, the Airflow retry description and no header", async () => {
    mockApi([
      {
        handler: () => ({ incident: codeIncident({ allowed_actions: ["retry_task"] }) }),
        path: TI_ROUTE,
      },
    ]);
    renderTaskInstance(null);

    fireEvent.click(await screen.findByRole("button", { name: "Retry task" }));
    const dialog = await screen.findByRole("alertdialog");

    expect(
      within(dialog).getByText(
        "Clear the failed task instance in Airflow so the scheduler runs it again. A code fix only takes " +
          "effect once its branch is merged.",
      ),
    ).toBeInTheDocument();
    expect(within(dialog).queryByTestId("action-warning")).toBeNull();
    expect(screen.getByText(/· try 1 ·/u)).toBeInTheDocument();
    expect(screen.queryByTestId("native-links")).toBeNull();
    expect(screen.queryByTestId("standalone-header")).toBeNull();
  });

  it("name a queued retry as before", async () => {
    mockApi([{ handler: () => ({ incident: QUEUED }), path: TI_ROUTE }]);
    renderTaskInstance(null);

    expect(await screen.findByTestId("queued")).toHaveTextContent("retry task by alice; retry repair by bob");
  });
});

describe("views on the token backend (API calls carry an Authorization header)", () => {
  it("read PR.md through the preview only, since a new tab cannot send the token", async () => {
    setAuthorizationProvider(() => "Bearer alice-token");
    const { calls } = mockApi([
      { handler: () => ({ incident: codeIncident() }), path: TI_ROUTE },
      { handler: () => "# Fix parse_signup_date", path: /\/pr\.md$/u },
    ]);
    renderTaskInstance();

    fireEvent.click(await screen.findByRole("button", { name: "Preview PR.md here" }));
    expect(await screen.findByText("# Fix parse_signup_date")).toBeInTheDocument();
    expect(screen.queryByRole("link", { name: "View PR.md" })).toBeNull();
    expect(calls.find((call) => call.path.endsWith("/pr.md"))?.headers.get("Authorization")).toBe(
      "Bearer alice-token",
    );
  });
});
