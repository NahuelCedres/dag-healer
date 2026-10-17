import { cleanup, render, screen } from "@testing-library/react";
import { afterEach, describe, expect, it } from "vitest";

import DefaultExport, { DagHealerDag, DagHealerNav, DagHealerTaskInstance } from "./main";

afterEach(() => {
  cleanup();
});

describe("bundle globals", () => {
  it("registers the three components on globalThis at module evaluation", () => {
    expect(globalThis.DagHealerNav).toBe(DagHealerNav);
    expect(globalThis.DagHealerDag).toBe(DagHealerDag);
    expect(globalThis.DagHealerTaskInstance).toBe(DagHealerTaskInstance);
    expect(globalThis.AirflowPlugin).toBe(DagHealerNav);
    expect(DefaultExport).toBe(DagHealerNav);
  });
});

describe("views", () => {
  it("renders the nav page", () => {
    render(<DagHealerNav />);
    expect(screen.getByRole("heading", { name: "dag-healer" })).toBeTruthy();
  });

  it("renders the DAG panel from props", () => {
    render(<DagHealerDag dagId="sales_daily_etl" />);
    expect(screen.getByRole("heading", { name: "Healer: sales_daily_etl" })).toBeTruthy();
  });

  it("renders the task instance panel with the resolved context", () => {
    render(<DagHealerTaskInstance dagId="customers_sync" mapIndex="2" runId="run_1" taskId="load" />);
    expect(screen.getByRole("heading", { name: "Healer incident for load" })).toBeTruthy();
    const context = screen.getByTestId("healer-context");

    expect(context.textContent).toContain("customers_sync");
    expect(context.textContent).toContain("run_1");
    expect(context.textContent).toContain("2");
  });

  it("falls back to the URL when the host passes no props", () => {
    window.history.pushState({}, "", "/dags/inventory_report/runs/r1/tasks/build/plugin/healer-ti");
    render(<DagHealerTaskInstance />);
    expect(screen.getByRole("heading", { name: "Healer incident for build" })).toBeTruthy();
    window.history.pushState({}, "", "/");
  });
});
