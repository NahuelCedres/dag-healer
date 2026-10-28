/** Pure helpers over the API shapes: sorting, native links, action warnings. */
import type { IncidentAction, IncidentLinks, IncidentSummary } from "src/api";

/** Newest first, by creation time (ties: most recently updated first). */
export const sortNewestFirst = (incidents: ReadonlyArray<IncidentSummary>): Array<IncidentSummary> =>
  [...incidents].sort(
    (left, right) =>
      Date.parse(right.created_at) - Date.parse(left.created_at) ||
      Date.parse(right.updated_at) - Date.parse(left.updated_at),
  );

export interface NativeLink {
  label: string;
  url: string;
}

const isHttpUrl = (value: string): boolean => {
  try {
    const { protocol } = new URL(value);

    return protocol === "http:" || protocol === "https:";
  } catch {
    return false;
  }
};

/**
 * The server-provided links into the orchestrator's own pages (`links.native`), sorted by label.
 *
 * Only absolute http(s) URLs are kept (defence in depth against `javascript:` URLs). Airflow has
 * none: its own UI hosts the Healer views.
 */
export const nativeLinks = (item: { readonly links?: IncidentLinks | null }): Array<NativeLink> => {
  const native: unknown = item.links?.native;

  if (native === null || typeof native !== "object") {
    return [];
  }

  return Object.entries(native as Record<string, unknown>)
    .filter((entry): entry is [string, string] => {
      const [label, url] = entry;

      return label.trim().length > 0 && typeof url === "string" && isHttpUrl(url);
    })
    .map(([label, url]) => ({ label, url }))
    .sort((left, right) => (left.label < right.label ? -1 : left.label > right.label ? 1 : 0));
};

/** The server's warnings for an offered action (`action_warnings[action]`, non-empty strings only). */
export const actionWarnings = (
  incident: { readonly action_warnings?: Record<string, Array<string>> },
  action: IncidentAction,
): Array<string> => {
  const warnings: unknown = incident.action_warnings?.[action];

  return Array.isArray(warnings)
    ? warnings.filter((warning): warning is string => typeof warning === "string" && warning.length > 0)
    : [];
};
