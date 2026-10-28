/**
 * Per-viewer preferences of the console (the side panel, single-key shortcuts, the theme), kept in
 * this browser's localStorage. Storage may be blocked or empty: every read falls back to the
 * defaults and every write is best effort. Nothing here is a credential.
 *
 * The theme: inside Airflow the console follows Airflow's own light or dark mode (the `dark` or
 * `light` class Airflow puts on `<html>`); on `dag-healer serve` the viewer picks system, light or
 * dark.
 */
import { useCallback, useEffect, useState } from "react";

import type { UiHost } from "src/vocabulary";

export type ThemeChoice = "dark" | "light" | "system";
export type Theme = "dark" | "light";

export interface ConsolePrefs {
  /** The side panel is open. */
  readonly panel: boolean;
  /** Single-key shortcuts (`]` toggles the side panel) are on. */
  readonly shortcuts: boolean;
  readonly theme: ThemeChoice;
}

export const PREFS_KEY = "dag-healer.console";

export const DEFAULT_PREFS: ConsolePrefs = { panel: true, shortcuts: true, theme: "system" };

const THEMES: ReadonlyArray<ThemeChoice> = ["dark", "light", "system"];

export const readPrefs = (): ConsolePrefs => {
  try {
    const raw = globalThis.localStorage.getItem(PREFS_KEY);
    const stored = raw === null ? {} : (JSON.parse(raw) as Partial<Record<keyof ConsolePrefs, unknown>>);

    return {
      panel: typeof stored.panel === "boolean" ? stored.panel : DEFAULT_PREFS.panel,
      shortcuts: typeof stored.shortcuts === "boolean" ? stored.shortcuts : DEFAULT_PREFS.shortcuts,
      theme: THEMES.find((theme) => theme === stored.theme) ?? DEFAULT_PREFS.theme,
    };
  } catch {
    return DEFAULT_PREFS;
  }
};

const writePrefs = (prefs: ConsolePrefs): void => {
  try {
    globalThis.localStorage.setItem(PREFS_KEY, JSON.stringify(prefs));
  } catch {
    // Blocked storage: the preference holds for this page only.
  }
};

/** The preferences and a setter that merges a change and keeps it. */
export const usePrefs = (): [ConsolePrefs, (change: Partial<ConsolePrefs>) => void] => {
  const [prefs, setPrefs] = useState<ConsolePrefs>(readPrefs);
  const update = useCallback((change: Partial<ConsolePrefs>) => {
    setPrefs((previous) => {
      const next = { ...previous, ...change };

      writePrefs(next);

      return next;
    });
  }, []);

  return [prefs, update];
};

const DARK_QUERY = "(prefers-color-scheme: dark)";

const systemTheme = (): Theme => {
  try {
    return globalThis.matchMedia(DARK_QUERY).matches ? "dark" : "light";
  } catch {
    return "dark";
  }
};

/** Airflow's mode: the class on `<html>`, else its color-scheme, else the system's. */
export const hostTheme = (root: HTMLElement = document.documentElement): Theme => {
  if (root.classList.contains("dark")) {
    return "dark";
  }
  if (root.classList.contains("light")) {
    return "light";
  }
  const scheme = root.style.colorScheme;

  if (scheme === "dark" || scheme === "light") {
    return scheme;
  }

  return systemTheme();
};

const resolve = (host: UiHost, choice: ThemeChoice): Theme =>
  host === "airflow_plugin" ? hostTheme() : choice === "system" ? systemTheme() : choice;

/** The theme to paint, kept in step with Airflow's mode or the system's while the console is open. */
export const useTheme = (host: UiHost, choice: ThemeChoice): Theme => {
  const [theme, setTheme] = useState<Theme>(() => resolve(host, choice));

  useEffect(() => {
    const update = () => setTheme(resolve(host, choice));

    update();
    let media: MediaQueryList | undefined;

    try {
      media = globalThis.matchMedia(DARK_QUERY);
      media.addEventListener("change", update);
    } catch {
      media = undefined;
    }
    const observer =
      host === "airflow_plugin" && typeof MutationObserver !== "undefined"
        ? new MutationObserver(update)
        : undefined;

    observer?.observe(document.documentElement, { attributeFilter: ["class", "style"], attributes: true });

    return () => {
      media?.removeEventListener("change", update);
      observer?.disconnect();
    };
  }, [host, choice]);

  return theme;
};
