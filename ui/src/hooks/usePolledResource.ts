import { createContext, useCallback, useContext, useEffect, useRef, useState } from "react";

import { type ApiError, toApiError } from "src/api";

/** Refresh intervals of the views (overridable through {@link PollingContext}, e.g. in tests). */
export interface PollingIntervals {
  /** While a queued command waits for the worker. */
  readonly commandMs: number;
  /** Regular background refresh. */
  readonly refreshMs: number;
}

export const DEFAULT_POLLING: PollingIntervals = { commandMs: 5000, refreshMs: 10_000 };

export const PollingContext = createContext<PollingIntervals>(DEFAULT_POLLING);

export const usePollingIntervals = (): PollingIntervals => useContext(PollingContext);

export interface Resource<T> {
  readonly data: T | undefined;
  readonly error: ApiError | undefined;
  /** True until the first answer (data or error) for the current key arrives. */
  readonly loading: boolean;
  /** Fetch again now (and restart the interval). */
  readonly refresh: () => void;
}

interface ResourceState<T> {
  data: T | undefined;
  error: ApiError | undefined;
  key: string | null;
  loaded: boolean;
}

const isHidden = (): boolean => typeof document !== "undefined" && document.hidden;

/** A fixed interval, or one computed from the latest data after every fetch (0 disables polling). */
export type PollInterval<T> = number | ((data: T | undefined) => number);

/**
 * Fetch `load()` now and every `interval` milliseconds while `key` is not null.
 *
 * Polling pauses while the document is hidden and resumes with an immediate fetch when it becomes
 * visible again. Requests never overlap: the next one is scheduled after the previous one settles,
 * and a stale request is aborted when the key or the interval changes. Data of the current key is
 * kept when a later refresh fails, so a transient error does not blank the view. A function
 * `interval` is evaluated after each fetch with the latest data, so a view can poll faster while a
 * queued command is pending; call `refresh()` to apply a change right away.
 */
export const usePolledResource = <T>(
  key: string | null,
  load: (signal: AbortSignal) => Promise<T>,
  interval: PollInterval<T>,
): Resource<T> => {
  const [state, setState] = useState<ResourceState<T>>({
    data: undefined,
    error: undefined,
    key,
    loaded: false,
  });
  const [tick, setTick] = useState(0);
  const loadRef = useRef(load);
  const intervalRef = useRef(interval);
  const lastDataRef = useRef<{ data: T | undefined; key: string | null }>({ data: undefined, key });

  useEffect(() => {
    loadRef.current = load;
    intervalRef.current = interval;
  });
  // A fixed interval restarts the loop when it changes; a function is re-read after each fetch.
  const fixedInterval = typeof interval === "number" ? interval : -1;

  useEffect(() => {
    if (key === null) {
      return undefined;
    }
    let disposed = false;
    let timer: ReturnType<typeof setTimeout> | undefined;
    let controller: AbortController | undefined;

    const nextInterval = (): number => {
      const current = intervalRef.current;
      const data = lastDataRef.current.key === key ? lastDataRef.current.data : undefined;

      return typeof current === "number" ? current : current(data);
    };

    const schedule = () => {
      clearTimeout(timer);
      const delay = nextInterval();

      if (!disposed && delay > 0 && !isHidden()) {
        timer = setTimeout(() => void run(), delay);
      }
    };

    const run = async () => {
      clearTimeout(timer);
      controller?.abort();
      const current = new AbortController();

      controller = current;
      try {
        const data = await loadRef.current(current.signal);

        if (!disposed && !current.signal.aborted) {
          lastDataRef.current = { data, key };
          setState({ data, error: undefined, key, loaded: true });
        }
      } catch (error) {
        if (!disposed && !current.signal.aborted) {
          setState((previous) => ({
            data: previous.key === key ? previous.data : undefined,
            error: toApiError(error),
            key,
            loaded: true,
          }));
        }
      } finally {
        if (!disposed && controller === current) {
          schedule();
        }
      }
    };

    const onVisibilityChange = () => {
      if (isHidden()) {
        clearTimeout(timer);
      } else {
        void run();
      }
    };

    void run();
    document.addEventListener("visibilitychange", onVisibilityChange);

    return () => {
      disposed = true;
      clearTimeout(timer);
      controller?.abort();
      document.removeEventListener("visibilitychange", onVisibilityChange);
    };
  }, [key, fixedInterval, tick]);

  const refresh = useCallback(() => setTick((value) => value + 1), []);
  const current = state.key === key;

  return {
    data: current ? state.data : undefined,
    error: current ? state.error : undefined,
    loading: key !== null && !(current && state.loaded),
    refresh,
  };
};
