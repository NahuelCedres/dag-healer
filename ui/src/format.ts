/** Small, locale-independent formatting helpers shared by the views. */
import type { Category, ContractStatus, IncidentAction, IncidentStatus } from "src/api";

const pad = (value: number): string => String(value).padStart(2, "0");

/** `2026-10-17 14:03:00` in the browser's time zone; "n/a" for missing or invalid values. */
export const formatDateTime = (value: string | null | undefined): string => {
  if (value === null || value === undefined || value === "") {
    return "n/a";
  }
  const date = new Date(value);

  if (Number.isNaN(date.getTime())) {
    return value;
  }

  return (
    `${date.getFullYear()}-${pad(date.getMonth() + 1)}-${pad(date.getDate())} ` +
    `${pad(date.getHours())}:${pad(date.getMinutes())}:${pad(date.getSeconds())}`
  );
};

/** `0.9` -> `90%`; "n/a" when unknown. */
export const formatConfidence = (value: number | null | undefined): string =>
  value === null || value === undefined || Number.isNaN(value) ? "n/a" : `${Math.round(value * 100)}%`;

/** First non-empty line of a text, shortened to `max` characters. */
export const oneLine = (text: string | null | undefined, max = 160): string => {
  const line = (text ?? "")
    .split(/\r?\n/u)
    .map((part) => part.trim())
    .find((part) => part.length > 0);

  if (line === undefined) {
    return "";
  }

  return line.length > max ? `${line.slice(0, max - 1)}…` : line;
};

/** Render any JSON-ish value as compact text. */
export const formatValue = (value: unknown): string => {
  if (value === null || value === undefined) {
    return "null";
  }
  if (typeof value === "string") {
    return value;
  }
  if (typeof value === "number" || typeof value === "boolean") {
    return String(value);
  }
  try {
    return JSON.stringify(value);
  } catch {
    return String(value);
  }
};

export const STATUS_LABELS: Record<IncidentStatus, string> = {
  classified: "Classified",
  error: "Error",
  ignored: "Ignored",
  new: "New",
  repair_failed: "Repair failed",
  repaired: "Repaired",
  repairing: "Repairing",
  reported: "Reported",
  reporting: "Reporting",
  resumed: "Resumed",
  triaging: "Triaging",
  waiting_human: "Waiting for a human",
};

export const statusLabel = (status: string): string =>
  STATUS_LABELS[status as IncidentStatus] ?? status.replaceAll("_", " ");

export const CATEGORY_LABELS: Record<Category, string> = {
  ambiguous: "Ambiguous",
  code: "Code",
  data: "Data",
};

export const categoryLabel = (category: string | null | undefined): string =>
  category === null || category === undefined
    ? "Unclassified"
    : (CATEGORY_LABELS[category as Category] ?? category);

export const CONTRACT_STATUS_LABELS: Record<ContractStatus, string> = {
  approved: "Approved",
  draft: "Draft",
  missing: "No contract",
  stale: "Stale",
};

export const contractStatusLabel = (status: string): string =>
  CONTRACT_STATUS_LABELS[status as ContractStatus] ?? status;

export const ACTION_LABELS: Record<IncidentAction, string> = {
  ignore: "Ignore",
  resolve_code: "Resolve as code",
  resolve_data: "Resolve as data",
  retry_repair: "Retry repair",
  retry_task: "Retry task",
};

/** Human labels of the decision rules of `dag_healer.agent.policy`. */
export const RULE_LABELS: Record<string, string> = {
  a_contract_violated: "Approved contract violated by the data",
  b_code_with_valid_data: "Data passes the approved contract; the traceback points to repository code",
  c_no_approved_contract: "No approved contract and a data-shaped error",
  d_low_confidence: "Classifier confidence below the threshold",
  infra: "Infrastructure error (timeouts, OOM, killed process...)",
  llm: "Classifier decision",
};

export const ruleLabel = (rule: string): string => RULE_LABELS[rule] ?? rule;

/** `task` or `task [2]` for mapped task instances. */
export const taskLabel = (taskId: string, mapIndex: number | null | undefined): string =>
  mapIndex !== null && mapIndex !== undefined && mapIndex >= 0 ? `${taskId} [${mapIndex}]` : taskId;
