import { cleanup, render, screen } from "@testing-library/react";
import { afterEach, describe, expect, it, vi } from "vitest";

import { APP_NAMES, LEGACY_APP_NAMES } from "./appNames";
import DefaultExport, { HealerDagIncidents, HealerNav, HealerTaskIncident } from "./main";
import { mockApi } from "./test/utils";

afterEach(() => {
  cleanup();
  vi.unstubAllGlobals();
});

const registry = globalThis as unknown as Record<string, unknown>;

describe("bundle globals", () => {
  it("registers the three components under their react_apps names at module evaluation", () => {
    expect(APP_NAMES).toEqual({ dag: "Healer incidents", nav: "Healer", taskInstance: "Healer incident" });
    expect(registry.Healer).toBe(HealerNav);
    expect(registry["Healer incidents"]).toBe(HealerDagIncidents);
    expect(registry["Healer incident"]).toBe(HealerTaskIncident);
    expect(globalThis.AirflowPlugin).toBe(HealerNav);
    expect(DefaultExport).toBe(HealerNav);
  });

  it("keeps the 0.1 names as aliases for older plugin definitions", () => {
    expect(registry[LEGACY_APP_NAMES.nav]).toBe(HealerNav);
    expect(registry[LEGACY_APP_NAMES.dag]).toBe(HealerDagIncidents);
    expect(registry[LEGACY_APP_NAMES.taskInstance]).toBe(HealerTaskIncident);
  });
});

describe("views", () => {
  it("renders the console on the nav page", async () => {
    mockApi([
      { handler: () => ({ incidents: [], total: 0 }), path: "incidents" },
      { handler: () => ({ dags: [] }), path: "dags" },
    ]);
    render(<HealerNav />);
    expect(
      screen.getByRole("heading", { name: "What failed, and what dag-healer did about it" }),
    ).toBeInTheDocument();
    expect(await screen.findByText("No incidents yet")).toBeInTheDocument();
  });

  it("renders the DAG panel from props", async () => {
    mockApi([
      {
        handler: () => ({ can_edit: true, contract: null, incidents: [], monitored: true }),
        path: "dags/sales_daily_etl/incidents",
      },
    ]);
    render(<HealerDagIncidents dagId="sales_daily_etl" />);
    expect(screen.getByRole("heading", { name: "sales_daily_etl" })).toBeInTheDocument();
    expect(await screen.findByText(/No incidents: no task of this DAG has failed/u)).toBeInTheDocument();
  });

  it("asks for a task instance when the context has none", () => {
    window.history.pushState({}, "", "/plugin/healer");
    render(<HealerTaskIncident />);
    expect(screen.getByText("Open a task instance to see its dag-healer incident.")).toBeInTheDocument();
    window.history.pushState({}, "", "/");
  });
});
