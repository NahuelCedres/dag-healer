/** Test helpers: a routed `fetch` mock for the healer API and fast polling intervals. */
import { render } from "@testing-library/react";
import type { ReactElement } from "react";
import { vi } from "vitest";

import { PollingContext, type PollingIntervals } from "src/hooks/usePolledResource";

export interface RecordedCall {
  body: unknown;
  headers: Headers;
  method: string;
  /** Path after `healer/api/v1/` (decoded), without the query string. */
  path: string;
  search: URLSearchParams;
}

export type RouteResult = Record<string, unknown> | Response | string;

export interface Route {
  handler: (call: RecordedCall) => Promise<RouteResult> | RouteResult;
  method?: string;
  path: RegExp | string;
}

export const jsonResponse = (status: number, body: unknown): Response =>
  new Response(JSON.stringify(body), { headers: { "Content-Type": "application/json" }, status });

const MARKER = "/healer/api/v1/";

/** Stub `fetch` with routes matched in order; unmatched requests answer 404 like FastAPI. */
export const mockApi = (routes: Array<Route>) => {
  const calls: Array<RecordedCall> = [];
  const spy = vi.fn(async (input: RequestInfo | URL, init: RequestInit = {}) => {
    const url = new URL(input instanceof Request ? input.url : String(input));
    const index = url.pathname.indexOf(MARKER);
    const rawPath = index === -1 ? url.pathname : url.pathname.slice(index + MARKER.length);
    const call: RecordedCall = {
      body: typeof init.body === "string" ? (JSON.parse(init.body) as unknown) : undefined,
      headers: new Headers(init.headers),
      method: init.method ?? "GET",
      path: decodeURIComponent(rawPath),
      search: url.searchParams,
    };

    calls.push(call);
    for (const route of routes) {
      const methodMatches = (route.method ?? "GET") === call.method;
      const pathMatches =
        typeof route.path === "string" ? route.path === call.path : route.path.test(call.path);

      if (methodMatches && pathMatches) {
        const result = await route.handler(call);

        if (result instanceof Response) {
          return result;
        }
        if (typeof result === "string") {
          return new Response(result, { headers: { "Content-Type": "text/markdown" }, status: 200 });
        }

        return jsonResponse(call.method === "POST" ? 202 : 200, result);
      }
    }

    return jsonResponse(404, { detail: "Not Found" });
  });

  vi.stubGlobal("fetch", spy);

  return { calls, spy };
};

/** Short command interval so the queued-command polling can be observed with real timers. */
export const FAST_POLLING: PollingIntervals = { commandMs: 40, refreshMs: 60_000 };

export const renderWithPolling = (ui: ReactElement, intervals: PollingIntervals = FAST_POLLING) =>
  render(<PollingContext.Provider value={intervals}>{ui}</PollingContext.Provider>);
