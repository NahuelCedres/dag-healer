import { cleanup, fireEvent, render, screen, waitFor, within } from "@testing-library/react";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";

import { type MeResponse, setAuthorizationProvider } from "src/api";
import { setup } from "src/test/fixtures";
import { jsonResponse, mockApi, type RecordedCall } from "src/test/utils";

import { StandaloneApp } from "./StandaloneApp";
import { clearToken, installTokenAuthorization, readToken, storeToken, TOKEN_STORAGE_KEY } from "./auth";

const TOKEN = "alice-token";

const FAKEFLOW_ME: MeResponse = {
  base_url: "http://127.0.0.1:8090",
  host: "standalone",
  orchestrator: {
    capabilities: { ui_hosts: ["standalone"] },
    display_name: "Fakeflow",
    instance: "fake",
    type: "fake",
    vocabulary: {
      attempt: "attempt",
      map_index: "map index",
      pipeline: "flow",
      pipelines: "flows",
      product: "Fakeflow",
      retry_action: "Retry step",
      run: "run",
      step: "step",
      steps: "steps",
    },
  },
  user: "alice",
};

const unauthorized = () => jsonResponse(401, { detail: "Not authenticated" });

const SETUP = setup() as unknown as Record<string, unknown>;

/** The token backend: only `Bearer alice-token` is accepted. */
const tokenApi = (me: MeResponse = FAKEFLOW_ME) => {
  const accepts = (call: RecordedCall) => call.headers.get("Authorization") === `Bearer ${TOKEN}`;

  return mockApi([
    {
      handler: (call) => (accepts(call) ? (me as unknown as Record<string, unknown>) : unauthorized()),
      path: "me",
    },
    { handler: (call) => (accepts(call) ? { incidents: [], total: 0 } : unauthorized()), path: "incidents" },
    { handler: (call) => (accepts(call) ? { dags: [] } : unauthorized()), path: "dags" },
    { handler: (call) => (accepts(call) ? SETUP : unauthorized()), path: "setup" },
  ]);
};

const signIn = (token: string) => {
  fireEvent.change(screen.getByLabelText("API token"), { target: { value: token } });
  fireEvent.click(screen.getByRole("button", { name: "Sign in" }));
};

afterEach(() => {
  // Unmount first so no polling request can reach the real fetch once it is unstubbed.
  cleanup();
  clearToken();
  setAuthorizationProvider();
  vi.unstubAllGlobals();
  vi.restoreAllMocks();
});

describe("StandaloneApp with the token backend", () => {
  beforeEach(() => {
    // As the standalone entry does for the token backend.
    installTokenAuthorization();
  });

  it("asks for a token before calling the API", () => {
    const { calls } = tokenApi();

    render(<StandaloneApp authBackend="token" mode="nav" />);
    expect(screen.getByRole("heading", { name: "Sign in" })).toBeInTheDocument();
    expect(screen.getByLabelText("API token")).toHaveAttribute("type", "password");
    expect(screen.getByLabelText("API token")).not.toHaveAttribute("name");
    expect(screen.getByTestId("standalone-title")).toHaveTextContent("Healer");
    expect(screen.queryByRole("button", { name: "Sign out" })).toBeNull();
    expect(calls).toHaveLength(0);
  });

  it("checks the token with /me, keeps it for the tab and sends it on every call", async () => {
    const { calls } = tokenApi();

    render(<StandaloneApp authBackend="token" mode="nav" />);
    signIn(`  ${TOKEN}  `);

    // The console replaces the sign-in screen: its sidebar names the orchestrator and the user.
    const sidebar = await screen.findByTestId("sidebar");

    expect(sidebar).toHaveTextContent("Fakeflow");
    expect(within(sidebar).getByRole("link", { name: "Flows" })).toBeInTheDocument();
    expect(screen.getByTestId("standalone-user")).toHaveTextContent("alice");
    expect(
      await screen.findByText("When a step of a watched flow fails, its incident shows up here."),
    ).toBeInTheDocument();
    expect(globalThis.sessionStorage.getItem(TOKEN_STORAGE_KEY)).toBe(TOKEN);

    expect(calls[0]?.path).toBe("me");
    expect(calls.length).toBeGreaterThan(1);
    for (const call of calls) {
      expect(call.headers.get("Authorization")).toBe(`Bearer ${TOKEN}`);
      expect(call.search.toString()).not.toContain(TOKEN);
    }
  });

  it("says so when the token is not accepted, and keeps nothing", async () => {
    const { calls } = tokenApi();

    render(<StandaloneApp authBackend="token" mode="nav" />);
    signIn("wrong-token");

    expect(await screen.findByText("This token was not accepted.")).toBeInTheDocument();
    expect(calls.map((call) => [call.path, call.headers.get("Authorization")])).toEqual([
      ["me", "Bearer wrong-token"],
    ]);
    expect(readToken()).toBeUndefined();
    expect(globalThis.sessionStorage.getItem(TOKEN_STORAGE_KEY)).toBeNull();
  });

  it("rejects a token the server could not accept without calling it", async () => {
    const { calls } = tokenApi();

    render(<StandaloneApp authBackend="token" mode="nav" />);
    signIn("tökén");

    expect(await screen.findByText("This token was not accepted.")).toBeInTheDocument();
    expect(calls).toHaveLength(0);
  });

  it("resumes with the token of the tab and signs out", async () => {
    const reload = vi.spyOn(globalThis.location, "reload").mockImplementation(() => undefined);

    storeToken(TOKEN);
    tokenApi();
    render(<StandaloneApp authBackend="token" mode="nav" />);

    const sidebar = await screen.findByTestId("sidebar");

    expect(within(sidebar).getByText("alice")).toBeInTheDocument();
    fireEvent.click(within(sidebar).getByRole("button", { name: "Sign out" }));
    expect(readToken()).toBeUndefined();
    expect(globalThis.sessionStorage.getItem(TOKEN_STORAGE_KEY)).toBeNull();
    expect(reload).toHaveBeenCalledTimes(1);
  });

  it("forgets a stored token that is no longer accepted", async () => {
    storeToken("revoked-token");
    tokenApi();
    render(<StandaloneApp authBackend="token" mode="nav" />);

    expect(await screen.findByRole("heading", { name: "Sign in" })).toBeInTheDocument();
    expect(readToken()).toBeUndefined();
  });
});

describe("StandaloneApp with the other backends", () => {
  it("shows the session banner on a 401, without a sign-in form or an Authorization header", async () => {
    const { calls } = mockApi([{ handler: unauthorized, path: /.*/u }]);

    render(<StandaloneApp authBackend="trusted_header" mode="nav" />);

    expect(await screen.findByText("Your session expired — reload the page")).toBeInTheDocument();
    expect(screen.queryByLabelText("API token")).toBeNull();
    expect(screen.getByTestId("sidebar")).toHaveTextContent("dag-healer");
    expect(screen.queryByRole("button", { name: "Sign out" })).toBeNull();
    await waitFor(() => expect(calls.length).toBeGreaterThan(1));
    expect(calls.every((call) => !call.headers.has("Authorization"))).toBe(true);
  });

  it("renders the view of the path with the words of /me", async () => {
    const { calls } = mockApi([
      { handler: () => ({ ...FAKEFLOW_ME, user: "dev" }) as unknown as Record<string, unknown>, path: "me" },
      {
        handler: () => ({ can_edit: true, contract: null, incidents: [], monitored: true }),
        path: "dags/customers_sync/incidents",
      },
      { handler: () => ({ incidents: [], total: 0 }), path: "incidents" },
      { handler: () => ({ dags: [] }), path: "dags" },
      { handler: () => SETUP, path: "setup" },
    ]);

    window.history.pushState({}, "", "/dags/customers_sync/plugin/healer-dag");
    try {
      render(<StandaloneApp authBackend="dev" mode="dag" />);
      expect(await screen.findByText(/No incidents: no step of this flow has failed/u)).toBeInTheDocument();
      expect(screen.getByRole("heading", { name: "Every flow dag-healer watches" })).toBeInTheDocument();
      expect(screen.getByTestId("sidebar")).toHaveTextContent("Fakeflow");
      expect(screen.getByTestId("standalone-user")).toHaveTextContent("dev");
      expect(calls[0]?.path).toBe("me");
      expect(calls.map((call) => call.path)).toContain("dags/customers_sync/incidents");
    } finally {
      window.history.pushState({}, "", "/");
    }
  });

  it("uses the Airflow words and name when /me names no orchestrator", async () => {
    mockApi([
      { handler: () => ({ base_url: "http://127.0.0.1:8090", host: "standalone", user: "ann" }), path: "me" },
      { handler: () => ({ incidents: [], total: 0 }), path: "incidents" },
      { handler: () => ({ dags: [] }), path: "dags" },
      { handler: () => SETUP, path: "setup" },
    ]);

    render(<StandaloneApp authBackend="alb_oidc" mode="nav" />);
    const sidebar = await screen.findByTestId("sidebar");

    expect(sidebar).toHaveTextContent("Apache Airflow");
    expect(within(sidebar).getByRole("link", { name: "DAGs" })).toBeInTheDocument();
    expect(
      await screen.findByText("When a task of a watched DAG fails, its incident shows up here."),
    ).toBeInTheDocument();
  });
});
