import { describe, expect, it } from "vitest";

import type { HealerMode } from "src/App";

import { standaloneMode } from "./mode";

const RUN = encodeURIComponent("manual__2026-10-17T12:00:00+00:00");

describe("standaloneMode", () => {
  it.each<[string, HealerMode]>([
    ["/plugin/healer", "nav"],
    ["/plugin/healer/", "nav"],
    ["/", "nav"],
    ["", "nav"],
    ["/plugin/other", "nav"],
    ["/dags/d", "nav"],
    ["/healer-dag", "nav"],
    ["/dags/customers_sync/plugin/healer-dag", "dag"],
    ["/dags/customers_sync/plugin/healer-dag/", "dag"],
    [`/dags/customers_sync/runs/${RUN}/tasks/transform/plugin/healer-ti`, "task_instance"],
    ["/dags/d/runs/r/tasks/t/mapped/3/plugin/healer-ti", "task_instance"],
    ["/dags/d/plugin/healer-ti/extra", "nav"],
    ["/dags/d/plugin%2Fhealer-ti", "nav"],
  ])("%j -> %s", (pathname, mode) => {
    expect(standaloneMode(pathname)).toBe(mode);
    expect(standaloneMode(pathname, "http://127.0.0.1:8090/")).toBe(mode);
  });

  it("reads the routes after the deployment base path of <base href>", () => {
    const base = "https://ops.example/healer-ui/";

    expect(standaloneMode("/healer-ui/plugin/healer", base)).toBe("nav");
    expect(standaloneMode("/healer-ui/dags/d/plugin/healer-dag", base)).toBe("dag");
    expect(standaloneMode("/healer-ui/dags/d/runs/r/tasks/t/plugin/healer-ti", base)).toBe("task_instance");
    // The base path itself is never read as a route.
    expect(standaloneMode("/x/plugin/healer-dag/", "https://ops.example/x/plugin/healer-dag/")).toBe("nav");
    expect(
      standaloneMode("/x/plugin/healer-dag/plugin/healer-ti", "https://ops.example/x/plugin/healer-dag/"),
    ).toBe("task_instance");
  });

  it("ignores a base URI that is not a directory or does not prefix the path", () => {
    expect(standaloneMode("/dags/d/plugin/healer-dag", "https://ops.example/dags/d/plugin/healer-dag")).toBe(
      "dag",
    );
    expect(standaloneMode("/dags/d/plugin/healer-dag", "https://ops.example/elsewhere/")).toBe("dag");
    expect(standaloneMode("/dags/d/plugin/healer-dag", "not a url")).toBe("dag");
  });
});
