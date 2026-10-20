/** Pure helpers over the API shapes (counts, decisions, sorting). */
import {
  ALL_STATUSES,
  type ContractView,
  type IncidentEvent,
  type IncidentSummary,
  OPEN_STATUSES,
} from "src/api";

/** Draft and stale contracts can be approved (stale = the YAML changed after the last approval). */
export const contractNeedsApproval = (contract: ContractView): boolean =>
  contract.status === "draft" || contract.status === "stale" || contract.stale;

/** Newest first, by creation time (ties: most recently updated first). */
export const sortNewestFirst = (incidents: ReadonlyArray<IncidentSummary>): Array<IncidentSummary> =>
  [...incidents].sort(
    (left, right) =>
      Date.parse(right.created_at) - Date.parse(left.created_at) ||
      Date.parse(right.updated_at) - Date.parse(left.updated_at),
  );

export interface IncidentCounts {
  byCategory: Record<string, number>;
  byStatus: Record<string, number>;
  open: number;
  total: number;
}

/** Counts by category ("none" for unclassified incidents) and by status. */
export const countIncidents = (incidents: ReadonlyArray<IncidentSummary>): IncidentCounts => {
  const counts: IncidentCounts = { byCategory: {}, byStatus: {}, open: 0, total: incidents.length };

  for (const incident of incidents) {
    const category = incident.category ?? "none";

    counts.byCategory[category] = (counts.byCategory[category] ?? 0) + 1;
    counts.byStatus[incident.status] = (counts.byStatus[incident.status] ?? 0) + 1;
    if (OPEN_STATUSES.includes(incident.status)) {
      counts.open += 1;
    }
  }

  return counts;
};

/** Statuses in their lifecycle order, keeping unknown ones at the end. */
export const orderStatuses = (statuses: Iterable<string>): Array<string> => {
  const known = ALL_STATUSES as ReadonlyArray<string>;
  const unique = [...new Set(statuses)];

  return unique.sort((left, right) => {
    const leftIndex = known.indexOf(left);
    const rightIndex = known.indexOf(right);

    return (leftIndex === -1 ? known.length : leftIndex) - (rightIndex === -1 ? known.length : rightIndex);
  });
};

export interface DecisionInfo {
  /** Who decided: "policy", "llm", "human"... when the event says so. */
  decidedBy?: string;
  reason?: string;
  rule?: string;
}

const stringField = (data: Record<string, unknown>, ...keys: Array<string>): string | undefined => {
  for (const key of keys) {
    const value = data[key];

    if (typeof value === "string" && value.length > 0) {
      return value;
    }
  }

  return undefined;
};

/**
 * The policy decision recorded in the event timeline: the latest event whose `data` carries a
 * `rule` (or a nested `decision` object with one). The event message is used as the reason when the
 * data has none.
 */
export const decisionFromEvents = (events: ReadonlyArray<IncidentEvent>): DecisionInfo | undefined => {
  for (let index = events.length - 1; index >= 0; index -= 1) {
    const event = events[index];

    if (event === undefined || event.data === null || typeof event.data !== "object") {
      continue;
    }
    const data = event.data as Record<string, unknown>;
    const nested = data.decision;
    const source =
      nested !== null && typeof nested === "object" && "rule" in nested
        ? (nested as Record<string, unknown>)
        : data;
    const rule = stringField(source, "rule");

    if (rule !== undefined) {
      return {
        decidedBy: stringField(source, "decided_by", "source"),
        reason: stringField(source, "reason") ?? (event.message.length > 0 ? event.message : undefined),
        rule,
      };
    }
  }

  return undefined;
};
