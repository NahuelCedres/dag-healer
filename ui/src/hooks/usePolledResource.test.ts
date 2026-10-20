import { act, renderHook } from "@testing-library/react";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";

import { NotFoundError } from "src/api";

import { usePolledResource } from "./usePolledResource";

let hidden = false;

beforeEach(() => {
  hidden = false;
  Object.defineProperty(document, "hidden", { configurable: true, get: () => hidden });
  vi.useFakeTimers();
});

afterEach(() => {
  vi.useRealTimers();
  Reflect.deleteProperty(document, "hidden");
});

const flush = () => act(async () => vi.advanceTimersByTimeAsync(0));

describe("usePolledResource", () => {
  it("loads immediately, then every interval", async () => {
    let count = 0;
    const load = vi.fn(() => Promise.resolve(++count));
    const { result } = renderHook(() => usePolledResource("k", load, 1000));

    expect(result.current.loading).toBe(true);
    await flush();
    expect(result.current.data).toBe(1);
    expect(result.current.loading).toBe(false);

    await act(async () => vi.advanceTimersByTimeAsync(1000));
    expect(result.current.data).toBe(2);
    await act(async () => vi.advanceTimersByTimeAsync(2000));
    expect(load).toHaveBeenCalledTimes(4);
  });

  it("pauses while the document is hidden and refreshes when it becomes visible", async () => {
    const load = vi.fn(() => Promise.resolve("ok"));

    renderHook(() => usePolledResource("k", load, 1000));
    await flush();
    expect(load).toHaveBeenCalledTimes(1);

    hidden = true;
    act(() => {
      document.dispatchEvent(new Event("visibilitychange"));
    });
    await act(async () => vi.advanceTimersByTimeAsync(5000));
    expect(load).toHaveBeenCalledTimes(1);

    hidden = false;
    act(() => {
      document.dispatchEvent(new Event("visibilitychange"));
    });
    await flush();
    expect(load).toHaveBeenCalledTimes(2);
  });

  it("uses a data-dependent interval", async () => {
    let pending = true;
    const load = vi.fn(() => Promise.resolve({ pending }));

    renderHook(() => usePolledResource("k", load, (data) => (data?.pending ? 100 : 10_000)));
    await flush();
    await act(async () => vi.advanceTimersByTimeAsync(100));
    expect(load).toHaveBeenCalledTimes(2);
    pending = false;
    await act(async () => vi.advanceTimersByTimeAsync(100));
    expect(load).toHaveBeenCalledTimes(3);
    await act(async () => vi.advanceTimersByTimeAsync(5000));
    expect(load).toHaveBeenCalledTimes(3);
  });

  it("keeps the last data when a refresh fails and exposes the error", async () => {
    let fail = false;
    const load = vi.fn(() =>
      fail ? Promise.reject(new NotFoundError("x", "gone")) : Promise.resolve("data"),
    );
    const { result } = renderHook(() => usePolledResource("k", load, 1000));

    await flush();
    fail = true;
    await act(async () => vi.advanceTimersByTimeAsync(1000));
    expect(result.current.data).toBe("data");
    expect(result.current.error?.status).toBe(404);
  });

  it("does nothing without a key and refetches on refresh()", async () => {
    const load = vi.fn(() => Promise.resolve(1));
    const { rerender, result } = renderHook(({ key }) => usePolledResource(key, load, 0), {
      initialProps: { key: null as string | null },
    });

    await flush();
    expect(load).not.toHaveBeenCalled();
    expect(result.current.loading).toBe(false);

    rerender({ key: "k" });
    await flush();
    expect(load).toHaveBeenCalledTimes(1);
    act(() => result.current.refresh());
    await flush();
    expect(load).toHaveBeenCalledTimes(2);
  });
});
