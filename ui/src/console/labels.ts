/**
 * Plain words for what the API reports: statuses and the inbox group they belong to, categories,
 * the next step of an incident, how a failure was found, and contract checks.
 *
 * Stage 1 of the console uses the statuses the backend has today. "Needs you" holds the incidents a
 * person must act on; "In progress" the ones dag-healer is still working on; "Done" the ones whose
 * pipeline of work has ended (a data problem reported, a fix proposed, an incident ignored).
 */
import type { IncidentStatus, IncidentSummary } from "src/api";
import type { Vocabulary } from "src/vocabulary";

export type InboxGroup = "done" | "needs" | "progress";

export const GROUPS: ReadonlyArray<{ id: InboxGroup; label: string }> = [
  { id: "needs", label: "Needs you" },
  { id: "progress", label: "In progress" },
  { id: "done", label: "Done" },
];

/** A state colour, used on small markers only (a dot, a pill). */
export type Tone = "amber" | "blue" | "cyan" | "idle" | "lime" | "pink" | "violet";

interface StatusMeta {
  readonly group: InboxGroup;
  readonly label: string;
  readonly tone: Tone;
}

const STATUS_META: Record<IncidentStatus, StatusMeta> = {
  classified: { group: "progress", label: "Classified", tone: "cyan" },
  error: { group: "needs", label: "Error", tone: "pink" },
  ignored: { group: "done", label: "Ignored", tone: "idle" },
  new: { group: "progress", label: "Detected", tone: "cyan" },
  repair_failed: { group: "needs", label: "Fix failed", tone: "pink" },
  repaired: { group: "done", label: "Fix proposed", tone: "blue" },
  repairing: { group: "progress", label: "Fixing", tone: "cyan" },
  reported: { group: "done", label: "Reported", tone: "amber" },
  reporting: { group: "progress", label: "Reporting", tone: "cyan" },
  resumed: { group: "progress", label: "Applying a decision", tone: "cyan" },
  triaging: { group: "progress", label: "Classifying", tone: "cyan" },
  waiting_human: { group: "needs", label: "Needs you", tone: "violet" },
};

const UNKNOWN_STATUS: StatusMeta = { group: "progress", label: "Unknown", tone: "idle" };

export const statusMeta = (status: string): StatusMeta =>
  STATUS_META[status as IncidentStatus] ?? { ...UNKNOWN_STATUS, label: status.replaceAll("_", " ") };

export const groupOf = (status: string): InboxGroup => statusMeta(status).group;

export const CATEGORY_WORDS: Record<string, string> = {
  ambiguous: "Ambiguous",
  code: "Code",
  data: "Data",
};

/** "Data", "Code", "Ambiguous"; "Not classified" before the classification. */
export const categoryWord = (category: string | null | undefined): string =>
  category === null || category === undefined ? "Not classified" : (CATEGORY_WORDS[category] ?? category);

/** What the incident waits for, in a few words (the inbox's "Next step" column). */
export const nextStep = (incident: IncidentSummary, words: Vocabulary): string => {
  switch (incident.status) {
    case "waiting_human":
      return "Decide what happens next";
    case "repair_failed":
      return "The fix failed: decide what next";
    case "error":
      return "dag-healer stopped: decide what next";
    case "new":
      return "Waiting to be classified";
    case "triaging":
      return "Classifying the failure";
    case "classified":
      return "Acting on the classification";
    case "reporting":
      return "Sending the data alert";
    case "repairing":
      return "Writing a test and a fix";
    case "resumed":
      return "Applying a decision";
    case "reported":
      return `Fix the input, then ${words.retry_action.toLowerCase()}`;
    case "repaired":
      return incident.pr_url === null
        ? incident.has_pr_md
          ? "Review PR.md and open the pull request"
          : "Review the fix"
        : "Review the pull request";
    case "ignored":
      return "Closed without action";
    default:
      return statusMeta(incident.status).label;
  }
};

/** How the failure reached dag-healer, by incident `source`. */
export const foundBy = (source: string, words: Vocabulary): string => {
  switch (source) {
    case "listener":
      return "Found by the Airflow listener";
    case "callback":
      return "Found by the failure callback";
    case "rest":
    case "poll":
      return `Found by checking ${words.product} regularly`;
    case "push":
      return "Reported by a push event";
    case "eventbridge":
      return "Reported by AWS EventBridge";
    case "sqs":
      return "Read from the SQS queue";
    case "cli":
      return "Reported from the command line";
    default:
      return `Found by ${source}`;
  }
};

/** Contract check names, as the product reports them, with plain words for each. */
export const RULE_WORDS: Record<string, string> = {
  dataset_unreadable: "File unreadable",
  dtype_mismatch: "Wrong type",
  missing_column: "Missing column",
  not_allowed_value: "Value not allowed",
  nulls_not_allowed: "Empty values",
  out_of_range: "Out of range",
  row_count_out_of_band: "Unexpected row count",
  unexpected_column: "Unknown column",
};

export const ruleWord = (rule: string): string => RULE_WORDS[rule] ?? rule.replaceAll("_", " ");

/** Contract dtypes in plain words. */
export const DTYPE_WORDS: Record<string, string> = {
  boolean: "true or false",
  date: "a date",
  datetime: "a date and time",
  float: "a number",
  integer: "a whole number",
  string: "text",
  unknown: "any type",
};

export const dtypeWord = (dtype: string): string => DTYPE_WORDS[dtype] ?? dtype;

/** Contract statuses in plain words. */
export const CONTRACT_WORDS: Record<string, string> = {
  approved: "Approved",
  draft: "Draft",
  missing: "No contract",
  stale: "Changed since approval",
};

export const contractWord = (status: string): string => CONTRACT_WORDS[status] ?? status;

export const contractTone = (status: string): Tone =>
  status === "approved" ? "lime" : status === "missing" ? "idle" : "violet";

/** The incident's step, with the map index of a mapped task (`load [3]`). */
export const stepLabel = (incident: Pick<IncidentSummary, "map_index" | "task_id">): string =>
  incident.map_index >= 0 ? `${incident.task_id} [${incident.map_index}]` : incident.task_id;
