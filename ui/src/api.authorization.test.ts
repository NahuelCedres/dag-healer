import { afterEach, describe, expect, it, vi } from "vitest";

import { apiFetch, apiPost, getMe, retryTask, setAuthorizationProvider } from "./api";

const mockFetch = () => {
  const spy = vi.fn<typeof fetch>(() =>
    Promise.resolve(
      new Response(JSON.stringify({ ok: true }), {
        headers: { "Content-Type": "application/json" },
        status: 200,
      }),
    ),
  );

  vi.stubGlobal("fetch", spy);

  return spy;
};

const sent = (spy: ReturnType<typeof mockFetch>, index = -1) => {
  const [url, init] = spy.mock.calls.at(index) as [URL, RequestInit];

  return { headers: new Headers(init.headers), init, url: new URL(String(url)) };
};

afterEach(() => {
  setAuthorizationProvider();
  vi.unstubAllGlobals();
});

describe("Authorization header", () => {
  it("is never sent without a provider (the Airflow plugin, cookie-authenticated)", async () => {
    const spy = mockFetch();

    await getMe();
    await apiPost("dags/d/incidents/i/retry-task", {});
    for (const index of [0, 1]) {
      const { headers, init } = sent(spy, index);

      expect(headers.has("Authorization")).toBe(false);
      expect(init.credentials).toBe("same-origin");
      expect(headers.get("X-Requested-With")).toBe("dag-healer");
    }
  });

  it("carries the provider's value on every call, read when the call is made", async () => {
    const spy = mockFetch();
    let token = "first";

    setAuthorizationProvider(() => `Bearer ${token}`);
    await getMe();
    token = "second";
    await retryTask("d", "i", "go");

    expect(sent(spy, 0).headers.get("Authorization")).toBe("Bearer first");
    const post = sent(spy, 1);

    expect(post.headers.get("Authorization")).toBe("Bearer second");
    expect(post.init.method).toBe("POST");
    expect(post.headers.get("X-Requested-With")).toBe("dag-healer");
    // Never in the URL.
    expect(post.url.href).not.toContain("second");
  });

  it.each([undefined, ""])("is left out when the provider returns %j", async (value) => {
    const spy = mockFetch();

    setAuthorizationProvider(() => value);
    await apiFetch("incidents");
    expect(sent(spy).headers.has("Authorization")).toBe(false);
  });

  it("keeps the caller's own header", async () => {
    const spy = mockFetch();
    const provider = vi.fn(() => "Bearer stored");

    setAuthorizationProvider(provider);
    await apiFetch("me", { headers: { Authorization: "Bearer candidate" } });
    expect(sent(spy).headers.get("Authorization")).toBe("Bearer candidate");
    expect(provider).not.toHaveBeenCalled();
  });

  it("stops when the provider is removed", async () => {
    const spy = mockFetch();

    setAuthorizationProvider(() => "Bearer t");
    setAuthorizationProvider();
    await apiFetch("incidents");
    expect(sent(spy).headers.has("Authorization")).toBe(false);
  });
});
