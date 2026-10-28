/**
 * Typed reads of the incident timeline. The worker records what it found in the `data` of its events
 * (`classification`, `context`, `repair`, `error`...); these helpers pick the newest event of a kind
 * and read its fields defensively, since the data is free-form JSON.
 */
import type { IncidentEvent, Violation } from "src/api";

type Data = Record<string, unknown>;

const asData = (value: unknown): Data | undefined =>
  value !== null && typeof value === "object" && !Array.isArray(value) ? (value as Data) : undefined;

const str = (data: Data, key: string): string | undefined => {
  const value = data[key];

  return typeof value === "string" && value.length > 0 ? value : undefined;
};

const num = (data: Data, key: string): number | undefined => {
  const value = data[key];

  return typeof value === "number" && Number.isFinite(value) ? value : undefined;
};

const strings = (data: Data, key: string): Array<string> => {
  const value = data[key];

  return Array.isArray(value) ? value.filter((item): item is string => typeof item === "string") : [];
};

/** The newest event of `kind` that carries data, with that data. */
const newest = (
  events: ReadonlyArray<IncidentEvent>,
  kind: string,
): { data: Data; event: IncidentEvent } | undefined => {
  for (let index = events.length - 1; index >= 0; index -= 1) {
    const event = events[index];
    const data = event === undefined ? undefined : asData(event.data);

    if (event !== undefined && event.kind === kind && data !== undefined) {
      return { data, event };
    }
  }

  return undefined;
};

/** The classification recorded by the worker (the latest one: a person's decision comes later). */
export interface Classification {
  readonly category?: string;
  readonly confidence?: number;
  readonly contractStatus?: string;
  /** "policy", the model provider's name, or the person who decided. */
  readonly decidedBy?: string;
  readonly errorKind?: string;
  readonly llmCategory?: string;
  readonly llmConfidence?: number;
  readonly needsLlmConfirmation: boolean;
  readonly reason?: string;
  readonly rule: string;
  readonly violations: Array<Violation>;
}

const violationsOf = (data: Data): Array<Violation> => {
  const value = data.violations;

  return Array.isArray(value)
    ? value.filter(
        (item): item is Violation => asData(item) !== undefined && typeof (item as Data).rule === "string",
      )
    : [];
};

export const classificationOf = (events: ReadonlyArray<IncidentEvent>): Classification | undefined => {
  const found = newest(events, "classification");
  const rule = found === undefined ? undefined : str(found.data, "rule");

  if (found === undefined || rule === undefined) {
    return undefined;
  }
  const { data } = found;

  return {
    category: str(data, "category"),
    confidence: num(data, "confidence"),
    contractStatus: str(data, "contract_status"),
    decidedBy: str(data, "decided_by"),
    errorKind: str(data, "error_kind"),
    llmCategory: str(data, "llm_category"),
    llmConfidence: num(data, "llm_confidence"),
    needsLlmConfirmation: data.needs_llm_confirmation === true,
    reason: str(data, "reason"),
    rule,
    violations: violationsOf(data),
  };
};

export interface RepoFrame {
  readonly file: string;
  readonly function?: string;
  readonly line?: number;
}

/** What the worker gathered before classifying (`context` event). */
export interface GatheredContext {
  readonly contractStatus?: string;
  readonly datasetsChecked: Array<string>;
  readonly draftViolationCount: number;
  readonly exceptionMessage?: string;
  readonly exceptionType?: string;
  readonly notes: Array<string>;
  /** Outermost first, as in a traceback. */
  readonly repoFrames: Array<RepoFrame>;
  readonly sampleDataset?: string;
  readonly violationCount: number;
}

export const contextOf = (events: ReadonlyArray<IncidentEvent>): GatheredContext | undefined => {
  const found = newest(events, "context");

  if (found === undefined) {
    return undefined;
  }
  const { data } = found;
  const frames = Array.isArray(data.repo_frames) ? data.repo_frames : [];

  return {
    contractStatus: str(data, "contract_status"),
    datasetsChecked: strings(data, "datasets_checked"),
    draftViolationCount: num(data, "draft_violation_count") ?? 0,
    exceptionMessage: str(data, "exception_message"),
    exceptionType: str(data, "exception_type"),
    notes: strings(data, "notes"),
    repoFrames: frames
      .map(asData)
      .filter((frame): frame is Data => frame !== undefined && typeof frame.file === "string")
      .map((frame) => ({
        file: frame.file as string,
        function: str(frame, "function"),
        line: num(frame, "line"),
      })),
    sampleDataset: str(data, "sample_dataset"),
    violationCount: num(data, "violation_count") ?? 0,
  };
};

/** The outcome of the latest repair (`repair` event). */
export interface RepairOutcome {
  readonly branch?: string;
  readonly createdTestsDir: boolean;
  readonly error?: string;
  readonly message: string;
  readonly notes: Array<string>;
  readonly patchPath?: string;
  readonly prMdPath?: string;
  readonly prUrl?: string;
  readonly status?: string;
  readonly testPath?: string;
}

export const repairOf = (events: ReadonlyArray<IncidentEvent>): RepairOutcome | undefined => {
  const found = newest(events, "repair");

  if (found === undefined) {
    return undefined;
  }
  const { data, event } = found;

  return {
    branch: str(data, "branch"),
    createdTestsDir: data.created_tests_dir === true,
    error: str(data, "error"),
    message: event.message,
    notes: strings(data, "notes"),
    patchPath: str(data, "patch_path"),
    prMdPath: str(data, "pr_md_path"),
    prUrl: str(data, "pr_url"),
    status: str(data, "status"),
    testPath: str(data, "test_path"),
  };
};

/** The latest processing error (`error` event): where the worker stopped, and why. */
export const processingErrorOf = (
  events: ReadonlyArray<IncidentEvent>,
): { message: string; stage?: string } | undefined => {
  const found = newest(events, "error");

  return found === undefined ? undefined : { message: found.event.message, stage: str(found.data, "stage") };
};

/** The newest event of any kind (what the side panel's "Latest" line shows). */
export const latestEvent = (events: ReadonlyArray<IncidentEvent>): IncidentEvent | undefined =>
  events.length === 0 ? undefined : events[events.length - 1];

/** The note a person wrote with a command, from its timeline message ("... Note: <text>"). */
export const noteOf = (message: string): string | undefined => {
  const at = message.lastIndexOf(" Note: ");

  return at === -1 ? undefined : message.slice(at + " Note: ".length).trim() || undefined;
};
