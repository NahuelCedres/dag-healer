import { afterEach, describe, expect, it, vi } from "vitest";

import { apiFetch, setAuthorizationProvider } from "src/api";

import {
  authBackendFromDocument,
  clearToken,
  installTokenAuthorization,
  isUsableToken,
  readToken,
  storeToken,
  TOKEN_STORAGE_KEY,
} from "./auth";

const documentWith = (head: string): Document => {
  const doc = document.implementation.createHTMLDocument("shell");

  doc.head.innerHTML = head;

  return doc;
};

afterEach(() => {
  clearToken();
  setAuthorizationProvider();
  vi.unstubAllGlobals();
  vi.restoreAllMocks();
});

describe("token storage", () => {
  it("keeps the token in sessionStorage for this tab, never in localStorage", () => {
    expect(readToken()).toBeUndefined();
    storeToken("alice-token");
    expect(globalThis.sessionStorage.getItem(TOKEN_STORAGE_KEY)).toBe("alice-token");
    expect(globalThis.localStorage.length).toBe(0);
    expect(readToken()).toBe("alice-token");

    clearToken();
    expect(globalThis.sessionStorage.getItem(TOKEN_STORAGE_KEY)).toBeNull();
    expect(readToken()).toBeUndefined();
  });

  it("reads a token stored by an earlier page of the tab", () => {
    globalThis.sessionStorage.setItem(TOKEN_STORAGE_KEY, "from-before");
    expect(readToken()).toBe("from-before");
  });

  it("ignores a stored value the server could not accept", () => {
    globalThis.sessionStorage.setItem(TOKEN_STORAGE_KEY, " padded ");
    expect(readToken()).toBeUndefined();
  });

  it("works for this page when sessionStorage is blocked", () => {
    vi.spyOn(Storage.prototype, "getItem").mockImplementation(() => {
      throw new DOMException("blocked", "SecurityError");
    });
    vi.spyOn(Storage.prototype, "setItem").mockImplementation(() => {
      throw new DOMException("blocked", "SecurityError");
    });
    vi.spyOn(Storage.prototype, "removeItem").mockImplementation(() => {
      throw new DOMException("blocked", "SecurityError");
    });

    expect(readToken()).toBeUndefined();
    storeToken("page-only");
    expect(readToken()).toBe("page-only");
    clearToken();
    expect(readToken()).toBeUndefined();
  });

  it.each([
    ["alice-token", true],
    ["a b", true],
    ["x".repeat(4096), true],
    ["", false],
    [" alice", false],
    ["alice\n", false],
    ["tökén", false],
    ["x".repeat(4097), false],
  ])("isUsableToken(%j) is %s", (token, usable) => {
    expect(isUsableToken(token)).toBe(usable);
  });
});

describe("authBackendFromDocument", () => {
  it("reads the backend the shell names", () => {
    expect(authBackendFromDocument(documentWith('<meta name="dag-healer-auth" content="token" />'))).toBe(
      "token",
    );
    expect(authBackendFromDocument(documentWith('<meta name="dag-healer-auth" content=" dev " />'))).toBe(
      "dev",
    );
  });

  it("is undefined without the meta element or its content", () => {
    expect(authBackendFromDocument(documentWith(""))).toBeUndefined();
    expect(
      authBackendFromDocument(documentWith('<meta name="dag-healer-auth" content="" />')),
    ).toBeUndefined();
    expect(authBackendFromDocument(documentWith('<meta name="other" content="token" />'))).toBeUndefined();
    expect(authBackendFromDocument()).toBeUndefined();
  });
});

describe("installTokenAuthorization", () => {
  it("sends the signed-in token as a bearer token, and nothing before the sign-in", async () => {
    const spy = vi.fn<typeof fetch>(() =>
      Promise.resolve(new Response("{}", { headers: { "Content-Type": "application/json" }, status: 200 })),
    );
    const authorization = () => new Headers((spy.mock.calls.at(-1) as [URL, RequestInit])[1].headers);

    vi.stubGlobal("fetch", spy);
    installTokenAuthorization();

    await apiFetch("me");
    expect(authorization().has("Authorization")).toBe(false);

    storeToken("alice-token");
    await apiFetch("incidents");
    expect(authorization().get("Authorization")).toBe("Bearer alice-token");

    clearToken();
    await apiFetch("incidents");
    expect(authorization().has("Authorization")).toBe(false);
  });
});
