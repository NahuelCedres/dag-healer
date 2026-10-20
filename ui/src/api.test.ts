import { afterEach, describe, expect, it, vi } from "vitest";

import {
  ApiError,
  apiFetch,
  apiPost,
  apiUrl,
  approveContract,
  ConflictError,
  ForbiddenError,
  getContract,
  getDagIncidents,
  getIncident,
  getMe,
  getPrMarkdown,
  getTaskInstanceIncident,
  hasSessionExpired,
  isApiError,
  listContracts,
  listDags,
  listIncidents,
  NetworkError,
  NotFoundError,
  reportTaskFailed,
  resolveIncident,
  retryRepair,
  retryTask,
  runIncidentAction,
  toApiError,
  UnauthorizedError,
} from "./api";
import { airflowHref, dagHref, healerPageHref, prMarkdownHref, taskInstanceHref } from "./links";

const jsonResponse = (status: number, body: unknown): Response =>
  new Response(JSON.stringify(body), { headers: { "Content-Type": "application/json" }, status });

const mockFetch = (impl: () => Promise<Response>) => {
  const spy = vi.fn(impl);

  vi.stubGlobal("fetch", spy);

  return spy;
};

const lastCall = (spy: ReturnType<typeof mockFetch>) => {
  const [url, init] = spy.mock.calls.at(-1) as unknown as [URL, RequestInit];

  return { body: init.body, headers: new Headers(init.headers), init, url: new URL(String(url)) };
};

afterEach(() => {
  vi.unstubAllGlobals();
});

describe("apiUrl", () => {
  it("resolves against the base URI, keeping a deployment sub-path", () => {
    expect(apiUrl("incidents?limit=5", "http://airflow.test/team/").href).toBe(
      "http://airflow.test/team/healer/api/v1/incidents?limit=5",
    );
  });

  it("strips leading slashes so the base path is never escaped", () => {
    expect(apiUrl("/dags/d/contract", "http://airflow.test/team/").href).toBe(
      "http://airflow.test/team/healer/api/v1/dags/d/contract",
    );
  });

  it("defaults to document.baseURI", () => {
    expect(apiUrl("dags").href).toBe(new URL("healer/api/v1/dags", document.baseURI).href);
  });
});

describe("apiFetch", () => {
  it("sends same-origin credentials and the X-Requested-With header", async () => {
    const spy = mockFetch(() => Promise.resolve(jsonResponse(200, { items: [] })));

    await expect(apiFetch("incidents")).resolves.toEqual({ items: [] });
    const { headers, init, url } = lastCall(spy);

    expect(url.pathname).toContain("healer/api/v1/incidents");
    expect(init.credentials).toBe("same-origin");
    expect(headers.get("X-Requested-With")).toBe("dag-healer");
    expect(headers.get("Accept")).toBe("application/json");
  });

  it("posts JSON bodies", async () => {
    const spy = mockFetch(() => Promise.resolve(jsonResponse(202, { status: "pending" })));

    await apiPost("dags/d/incidents/i1/resolve", { note: "ok", resolution: "data" });
    const { body, headers, init } = lastCall(spy);

    expect(init.method).toBe("POST");
    expect(body).toBe(JSON.stringify({ note: "ok", resolution: "data" }));
    expect(headers.get("Content-Type")).toBe("application/json");
    expect(headers.get("X-Requested-With")).toBe("dag-healer");
  });

  it("returns undefined for empty bodies", async () => {
    mockFetch(() => Promise.resolve(new Response(null, { status: 204 })));
    await expect(apiFetch("noop")).resolves.toBeUndefined();
  });

  it.each([
    [401, UnauthorizedError],
    [403, ForbiddenError],
    [404, NotFoundError],
    [409, ConflictError],
    [500, ApiError],
  ])("maps HTTP %i to a typed error with status and detail", async (status, errorType) => {
    mockFetch(() => Promise.resolve(jsonResponse(status, { detail: "Forbidden" })));
    const error: unknown = await apiFetch("dags/d/contract").catch((caught: unknown) => caught);

    expect(error).toBeInstanceOf(errorType);
    expect(isApiError(error)).toBe(true);
    expect(error).toMatchObject({ detail: "Forbidden", path: "dags/d/contract", status });
    expect((error as ApiError).body).toEqual({ detail: "Forbidden" });
  });

  it("uses the FastAPI detail as the message, including validation errors", async () => {
    mockFetch(() =>
      Promise.resolve(
        jsonResponse(422, { detail: [{ loc: ["body", "resolution"], msg: "Field required" }] }),
      ),
    );
    await expect(apiFetch("x")).rejects.toMatchObject({
      detail: "Field required",
      message: "Field required",
      status: 422,
    });
  });

  it("falls back to a generic detail for non-JSON errors", async () => {
    mockFetch(() => Promise.resolve(new Response("", { status: 502 })));
    await expect(apiFetch("x")).rejects.toMatchObject({
      detail: "Request failed with status 502",
      status: 502,
    });
  });

  it("wraps network failures with status 0", async () => {
    mockFetch(() => Promise.reject(new TypeError("Failed to fetch")));
    const error: unknown = await apiFetch("incidents").catch((caught: unknown) => caught);

    expect(error).toBeInstanceOf(NetworkError);
    expect((error as NetworkError).status).toBe(0);
    expect((error as NetworkError).detail).toContain("Failed to fetch");
  });

  it("normalizes unknown errors and detects expired sessions", () => {
    expect(toApiError(new Error("boom"))).toMatchObject({ detail: "boom", status: 0 });
    expect(hasSessionExpired(undefined, new NotFoundError("x", "nope"))).toBe(false);
    expect(hasSessionExpired(undefined, new UnauthorizedError("x", "Not authenticated"))).toBe(true);
  });
});

describe("endpoints", () => {
  const ok = () => mockFetch(() => Promise.resolve(jsonResponse(200, {})));

  it.each([
    ["getMe", () => getMe(), "/healer/api/v1/me", ""],
    [
      "listIncidents",
      () => listIncidents({ dagId: "d1", limit: 20, status: "waiting_human" }),
      "/healer/api/v1/incidents",
      "?dag_id=d1&limit=20&status=waiting_human",
    ],
    ["listIncidents without filters", () => listIncidents(), "/healer/api/v1/incidents", ""],
    ["listDags", () => listDags(), "/healer/api/v1/dags", ""],
    ["listContracts", () => listContracts(), "/healer/api/v1/contracts", ""],
    ["getDagIncidents", () => getDagIncidents("my dag"), "/healer/api/v1/dags/my%20dag/incidents", ""],
    ["getIncident", () => getIncident("d", "abc"), "/healer/api/v1/dags/d/incidents/abc", ""],
    ["getContract", () => getContract("d"), "/healer/api/v1/dags/d/contract", ""],
  ])("%s calls GET %s", async (_name, call, pathname, search) => {
    const spy = ok();

    await call();
    const { init, url } = lastCall(spy);

    expect(init.method ?? "GET").toBe("GET");
    expect(url.pathname).toBe(pathname);
    expect(url.search).toBe(search);
  });

  it("queries the task-instance incident with run, task and map index", async () => {
    const spy = ok();
    const runId = "scheduled__2026-10-17T00:00:00+00:00";

    await getTaskInstanceIncident("d", { runId, taskId: "t" });
    let { url } = lastCall(spy);

    expect(url.pathname).toBe("/healer/api/v1/dags/d/task-instance");
    expect(url.searchParams.get("run_id")).toBe(runId);
    expect(url.searchParams.get("task_id")).toBe("t");
    expect(url.searchParams.get("map_index")).toBe("-1");

    await getTaskInstanceIncident("d", { mapIndex: "4", runId, taskId: "t" });
    ({ url } = lastCall(spy));
    expect(url.searchParams.get("map_index")).toBe("4");
  });

  it("fetches PR.md as text", async () => {
    const spy = mockFetch(() =>
      Promise.resolve(new Response("# Fix", { headers: { "Content-Type": "text/markdown" }, status: 200 })),
    );

    await expect(getPrMarkdown("d", "i1")).resolves.toBe("# Fix");
    const { headers, url } = lastCall(spy);

    expect(url.pathname).toBe("/healer/api/v1/dags/d/incidents/i1/pr.md");
    expect(headers.get("Accept")).toContain("text/markdown");
  });

  it.each([
    [
      "resolveIncident",
      () => resolveIncident("d", "i1", "data", "  why  "),
      "resolve",
      { note: "why", resolution: "data" },
    ],
    [
      "resolveIncident without note",
      () => resolveIncident("d", "i1", "ignore", "   "),
      "resolve",
      { resolution: "ignore" },
    ],
    ["retryRepair", () => retryRepair("d", "i1"), "retry-repair", {}],
    ["retryTask", () => retryTask("d", "i1", "again"), "retry-task", { note: "again" }],
  ])("%s posts to the incident command endpoint", async (_name, call, action, body) => {
    const spy = mockFetch(() => Promise.resolve(jsonResponse(202, { command_id: 1, status: "pending" })));

    await expect(call()).resolves.toEqual({ command_id: 1, status: "pending" });
    const { init, url } = lastCall(spy);

    expect(init.method).toBe("POST");
    expect(url.pathname).toBe(`/healer/api/v1/dags/d/incidents/i1/${action}`);
    expect(JSON.parse(String(init.body))).toEqual(body);
  });

  it("approves a contract", async () => {
    const spy = mockFetch(() => Promise.resolve(jsonResponse(202, { command_id: 2, status: "pending" })));

    await approveContract("d", "lgtm");
    const { init, url } = lastCall(spy);

    expect(url.pathname).toBe("/healer/api/v1/dags/d/contract/approve");
    expect(JSON.parse(String(init.body))).toEqual({ note: "lgtm" });
  });

  it("maps every action-bar action to its endpoint", async () => {
    const spy = mockFetch(() => Promise.resolve(jsonResponse(202, { command_id: 3, status: "pending" })));
    const expected = {
      ignore: ["resolve", { resolution: "ignore" }],
      resolve_code: ["resolve", { resolution: "code" }],
      resolve_data: ["resolve", { resolution: "data" }],
      retry_repair: ["retry-repair", {}],
      retry_task: ["retry-task", {}],
    } as const;

    for (const [action, [suffix, body]] of Object.entries(expected)) {
      await runIncidentAction("d", "i1", action as keyof typeof expected);
      const { init, url } = lastCall(spy);

      expect(url.pathname).toBe(`/healer/api/v1/dags/d/incidents/i1/${suffix}`);
      expect(JSON.parse(String(init.body))).toEqual(body);
    }
  });

  it("sends the ingest token for task-failed events", async () => {
    const spy = mockFetch(() => Promise.resolve(jsonResponse(202, { accepted: true, pending_event_id: 5 })));

    await expect(reportTaskFailed({ dag_id: "d" }, "secret")).resolves.toEqual({
      accepted: true,
      pending_event_id: 5,
    });
    const { headers, url } = lastCall(spy);

    expect(url.pathname).toBe("/healer/api/v1/events/task-failed");
    expect(headers.get("X-Healer-Token")).toBe("secret");
  });
});

describe("links", () => {
  const base = "http://airflow.test/team/";
  const runId = "manual__2026-10-17T12:00:00+00:00";

  it("builds the task-instance tab URL with encoded segments", () => {
    expect(taskInstanceHref("d", runId, "t", -1, base)).toBe(
      "http://airflow.test/team/dags/d/runs/manual__2026-10-17T12%3A00%3A00%2B00%3A00/tasks/t/plugin/healer-ti",
    );
    expect(taskInstanceHref("d", "r", "t", "3", base)).toBe(
      "http://airflow.test/team/dags/d/runs/r/tasks/t/mapped/3/plugin/healer-ti",
    );
  });

  it("builds the DAG tab, Healer page and PR.md URLs", () => {
    expect(dagHref("my_dag", base)).toBe("http://airflow.test/team/dags/my_dag/plugin/healer-dag");
    expect(healerPageHref(base)).toBe("http://airflow.test/team/plugin/healer");
    expect(prMarkdownHref("d", "i1", base)).toBe(
      "http://airflow.test/team/healer/api/v1/dags/d/incidents/i1/pr.md",
    );
    expect(airflowHref("/dags", base)).toBe("http://airflow.test/team/dags");
  });
});
