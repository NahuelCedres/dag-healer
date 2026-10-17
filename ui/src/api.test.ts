import { afterEach, describe, expect, it, vi } from "vitest";

import {
  ApiError,
  apiFetch,
  apiPost,
  apiUrl,
  ForbiddenError,
  isApiError,
  NetworkError,
  NotFoundError,
  UnauthorizedError,
} from "./api";

const jsonResponse = (status: number, body: unknown): Response =>
  new Response(JSON.stringify(body), { headers: { "Content-Type": "application/json" }, status });

const mockFetch = (impl: () => Promise<Response>) => {
  const spy = vi.fn(impl);

  vi.stubGlobal("fetch", spy);

  return spy;
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
    const [url, init] = spy.mock.calls[0] as unknown as [URL, RequestInit];
    const headers = new Headers(init.headers);

    expect(String(url)).toContain("healer/api/v1/incidents");
    expect(init.credentials).toBe("same-origin");
    expect(headers.get("X-Requested-With")).toBe("dag-healer");
    expect(headers.get("Accept")).toBe("application/json");
  });

  it("posts JSON bodies", async () => {
    const spy = mockFetch(() => Promise.resolve(jsonResponse(200, { status: "pending" })));

    await apiPost("dags/d/incidents/i1/resolve", { note: "ok", resolution: "data" });
    const [, init] = spy.mock.calls[0] as unknown as [URL, RequestInit];

    expect(init.method).toBe("POST");
    expect(init.body).toBe(JSON.stringify({ note: "ok", resolution: "data" }));
    expect(new Headers(init.headers).get("Content-Type")).toBe("application/json");
  });

  it("returns undefined for empty bodies", async () => {
    mockFetch(() => Promise.resolve(new Response(null, { status: 204 })));
    await expect(apiFetch("noop")).resolves.toBeUndefined();
  });

  it.each([
    [401, UnauthorizedError],
    [403, ForbiddenError],
    [404, NotFoundError],
    [500, ApiError],
  ])("maps HTTP %i to a typed error", async (status, errorType) => {
    mockFetch(() => Promise.resolve(jsonResponse(status, { detail: "Forbidden" })));
    const error: unknown = await apiFetch("dags/d/contract").catch((caught: unknown) => caught);

    expect(error).toBeInstanceOf(errorType);
    expect(isApiError(error)).toBe(true);
    expect((error as ApiError).status).toBe(status);
    expect((error as ApiError).path).toBe("dags/d/contract");
  });

  it("uses the FastAPI detail as the message, including validation errors", async () => {
    mockFetch(() =>
      Promise.resolve(
        jsonResponse(422, { detail: [{ loc: ["body", "resolution"], msg: "Field required" }] }),
      ),
    );
    await expect(apiFetch("x")).rejects.toMatchObject({ message: "Field required", status: 422 });
  });

  it("wraps network failures with status 0", async () => {
    mockFetch(() => Promise.reject(new TypeError("Failed to fetch")));
    const error: unknown = await apiFetch("incidents").catch((caught: unknown) => caught);

    expect(error).toBeInstanceOf(NetworkError);
    expect((error as NetworkError).status).toBe(0);
    expect((error as NetworkError).message).toContain("Failed to fetch");
  });
});
