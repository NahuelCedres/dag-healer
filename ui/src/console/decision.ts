/**
 * The decision an incident needs, in plain words: who decided so far ("Decided by"), and the
 * question a person answers, with its answers.
 *
 * Texts may hold `code` spans in backticks; {@link RichText} renders them. Every sentence is built
 * from what the worker recorded (the classification and context events, the violations, the
 * contract): nothing is assumed that the backend did not report.
 */
import type { ContractView, IncidentAction, IncidentDetail, Violation } from "src/api";
import type { Vocabulary } from "src/vocabulary";

import { classificationOf, contextOf, processingErrorOf, repairOf } from "./events";
import { categoryWord, stepLabel } from "./labels";

const percent = (value: number): string => value.toFixed(2);

const contractPhrase = (status: string | undefined, pipeline: string, short = false): string => {
  switch (status) {
    case "draft":
      return `the contract of \`${pipeline}\` is a draft${short ? "" : " (nobody approved it yet)"}`;
    case "stale":
      return `the contract of \`${pipeline}\` changed after it was approved`;
    case "approved":
      return `the contract of \`${pipeline}\` is approved`;
    default:
      return `\`${pipeline}\` has no contract`;
  }
};

const threshold = (minConfidence: number | undefined): string =>
  minConfidence === undefined ? "" : `; it needs ${percent(minConfidence)}`;

/** "missing_column on `amount`, nulls_not_allowed on `amt`". */
const violationList = (violations: ReadonlyArray<Violation>): string =>
  violations
    .slice(0, 4)
    .map((violation) => (violation.column ? `${violation.rule} on \`${violation.column}\`` : violation.rule))
    .join(", ");

export interface DecidedBy {
  /** "Decided by a fixed rule", "Waiting for you"... */
  readonly lead: string;
  readonly detail: string;
  /** Who decided: a fixed rule, the model, a person, or nobody yet. */
  readonly who: "model" | "nobody" | "person" | "rule";
}

/**
 * The "Decided by" line: whether a fixed rule, the model (with its confidence) or a person decided,
 * from the latest classification event and the resolution.
 */
export const decidedBy = (incident: IncidentDetail, words: Vocabulary, minConfidence?: number): DecidedBy => {
  const pipeline = incident.dag_id;

  if (incident.resolution === "ignore") {
    return {
      detail: "they closed the incident without any action.",
      lead: `Decided by ${incident.resolved_by ?? "a person"}`,
      who: "person",
    };
  }
  const found = classificationOf(incident.events);

  if (found === undefined) {
    return {
      detail: "dag-healer has not classified this failure yet.",
      lead: "Not decided yet",
      who: "nobody",
    };
  }
  const category = categoryWord(found.category).toUpperCase();
  const model = found.llmConfidence ?? found.confidence;

  switch (found.rule) {
    case "human_resolution":
      return {
        detail: `they resolved it as ${category}; the classification was skipped.`,
        lead: `Decided by ${found.decidedBy ?? incident.resolved_by ?? "a person"}`,
        who: "person",
      };
    case "a_contract_violated":
      return {
        detail:
          `the approved contract is violated (${violationList(found.violations) || "see the checks below"}), ` +
          "so this is DATA. The model's opinion does not change that.",
        lead: "Decided by a fixed rule",
        who: "rule",
      };
    case "infra":
      return {
        detail:
          "this looks like an infrastructure problem, not bad data or a bug, so a fixed rule hands it to a person.",
        lead: "Waiting for you",
        who: "rule",
      };
    case "c_no_approved_contract":
      return {
        detail:
          `${contractPhrase(found.contractStatus, pipeline)} and the error is about the shape of the data, ` +
          "so a fixed rule hands it to a person.",
        lead: "Waiting for you",
        who: "rule",
      };
    case "definition":
      return {
        detail: `a fixed rule found an error in the ${words.pipeline} definition, which a test cannot reproduce.`,
        lead: "Waiting for you",
        who: "rule",
      };
    case "aggregate":
      return {
        detail: "the failure only sums up failed child runs, and none of them is available to look at.",
        lead: "Waiting for you",
        who: "rule",
      };
    case "d_low_confidence":
      return {
        detail:
          `the model was not sure enough (confidence ${model === undefined ? "unknown" : percent(model)}` +
          `${threshold(minConfidence)}).`,
        lead: "Waiting for you",
        who: "model",
      };
    case "b_code_with_valid_data":
      if (found.category === "code") {
        return {
          detail:
            "the input passes the approved contract and the traceback points into your code; the model " +
            `agreed with confidence ${model === undefined ? "unknown" : percent(model)}${threshold(minConfidence)}.`,
          lead: "Decided by a fixed rule and the model",
          who: "rule",
        };
      }

      return {
        detail:
          "the input passes the approved contract and the traceback points into your code, but " +
          (found.llmCategory === undefined
            ? "no model was available to confirm it."
            : `the model said ${categoryWord(found.llmCategory).toUpperCase()}.`),
        lead: "Waiting for you",
        who: "model",
      };
    default:
      if (found.category === "ambiguous") {
        return {
          detail:
            found.llmCategory === undefined
              ? "no fixed rule applied and no model was available to decide."
              : `no fixed rule applied and the model could not tell (confidence ${
                  model === undefined ? "unknown" : percent(model)
                }${threshold(minConfidence)}).`,
          lead: "Waiting for you",
          who: "model",
        };
      }

      return {
        detail:
          `no fixed rule applied (${contractPhrase(found.contractStatus, pipeline, true)}), and it classified this as ` +
          `${category} with confidence ${model === undefined ? "unknown" : percent(model)}${threshold(minConfidence)}.`,
        lead: "Decided by the model",
        who: "model",
      };
  }
};

// ------------------------------------------------------------------------------------------------ //
// The question
// ------------------------------------------------------------------------------------------------ //

export interface Answer {
  readonly action: IncidentAction;
  readonly body: string;
  readonly title: string;
}

export interface Question {
  readonly context: Array<string>;
  /** The main answers (two cards); `other` are the smaller ones ("Not sure yet?"). */
  readonly main: Array<Answer>;
  readonly other: Array<Answer>;
  readonly title: string;
}

/** The file name of a dataset (`data/incoming/inventory.csv` → `inventory.csv`), else its name. */
export const datasetFile = (
  contract: ContractView | null | undefined,
  dataset: string | undefined,
): string | undefined => {
  if (dataset === undefined) {
    return undefined;
  }
  const found = contract?.datasets.find((candidate) => candidate.name === dataset);
  const path = found?.locator.path;

  return path === undefined || path.length === 0 ? dataset : (path.split("/").pop() ?? path);
};

export interface ColumnChange {
  readonly added: Array<string>;
  readonly dataset: string;
  readonly missing: Array<string>;
}

/**
 * Columns the contract expects that the input lacks, and columns the input has that the contract
 * does not list, from the `missing_column` findings (their `actual` holds the columns present).
 */
export const columnChange = (
  violations: ReadonlyArray<Violation>,
  contract: ContractView | null | undefined,
): ColumnChange | undefined => {
  const missing = violations.filter((violation) => violation.rule === "missing_column" && violation.column);
  const first = missing[0];

  if (first === undefined || contract === null || contract === undefined) {
    return undefined;
  }
  const dataset = first.dataset ?? contract.datasets[0]?.name;
  const declared = contract.datasets
    .find((candidate) => candidate.name === dataset)
    ?.columns.map((c) => c.name);
  const present = Array.isArray(first.actual)
    ? first.actual.filter((c): c is string => typeof c === "string")
    : [];

  if (dataset === undefined || declared === undefined || present.length === 0) {
    return undefined;
  }

  return {
    added: present.filter((column) => !declared.includes(column)),
    dataset,
    missing: missing
      .filter((violation) => (violation.dataset ?? dataset) === dataset)
      .map((v) => v.column as string),
  };
};

const codeList = (items: ReadonlyArray<string>): string => {
  const quoted = items.map((item) => `\`${item}\``);

  if (quoted.length <= 1) {
    return quoted.join("");
  }

  return `${quoted.slice(0, -1).join(", ")} and ${quoted.at(-1) ?? ""}`;
};

const errorText = (incident: IncidentDetail): string => {
  const context = contextOf(incident.events);
  const type = context?.exceptionType ?? incident.error_name ?? undefined;

  if (type !== undefined && context?.exceptionMessage !== undefined) {
    return `${type}: ${context.exceptionMessage}`;
  }

  return type ?? (incident.error_summary.split("\n")[0] ?? "").trim();
};

const DATA_ANSWER = (title: string): Answer => ({
  action: "resolve_data",
  body: "Report a data problem, with the failed checks and a masked sample. No code changes.",
  title,
});

const CODE_ANSWER = (title: string, body?: string): Answer => ({
  action: "resolve_code",
  body: body ?? "dag-healer writes a test that reproduces the failure and a minimal fix, for you to review.",
  title,
});

const IGNORE_ANSWER: Answer = {
  action: "ignore",
  body: "Close the incident without any action.",
  title: "Ignore",
};

const retryAnswer = (words: Vocabulary, title?: string): Answer => ({
  action: "retry_task",
  body: `${words.product} runs the failed ${words.step} again, with the code deployed now.`,
  title: title ?? words.retry_action,
});

/** The question of an incident that needs a person: waiting_human, repair_failed or error. */
export const questionFor = (
  incident: IncidentDetail,
  words: Vocabulary,
  contract?: ContractView | null,
): Question => {
  const step = `\`${stepLabel(incident)}\``;

  if (incident.status === "repair_failed") {
    const repair = repairOf(incident.events);
    const why = repair?.error ?? repair?.message;

    return {
      context: [
        `dag-healer tried to reproduce the failure of ${step} with a test and fix it, and could not.` +
          (why === undefined ? "" : ` What it reported: ${why}`),
      ],
      main: [
        {
          action: "retry_repair",
          body: "Run the repair again on a fresh copy of the repository: a new test, a new fix, a new branch.",
          title: "Try the fix again",
        },
        DATA_ANSWER("It is a data problem after all"),
      ],
      other: [IGNORE_ANSWER],
      title: "The fix did not work. What now?",
    };
  }
  if (incident.status === "error") {
    const failure = processingErrorOf(incident.events);

    return {
      context: [
        failure === undefined
          ? "dag-healer stopped before it finished with this incident."
          : `dag-healer stopped before it finished: ${failure.message}`,
      ],
      main: [
        {
          action: "retry_repair",
          body: "Treat it as a code problem and run the repair: a test, a minimal fix and a pull request.",
          title: "Try again as a code problem",
        },
        DATA_ANSWER("It is a data problem"),
      ],
      other: [IGNORE_ANSWER],
      title: "dag-healer stopped with an error. What now?",
    };
  }

  const found = classificationOf(incident.events);
  const rule = found?.rule;
  const error = errorText(incident);

  if (rule === "infra") {
    return {
      context: [
        `${step} failed with \`${error}\`. It looks like an infrastructure problem (a timeout, a lost ` +
          "connection, memory...), not bad data or a bug in the code.",
      ],
      main: [retryAnswer(words, "Yes, run it again"), { ...IGNORE_ANSWER, title: "No, close it" }],
      other: [DATA_ANSWER("It is a data problem"), CODE_ANSWER("It is a code problem")],
      title: `Run ${step} again?`,
    };
  }
  if (rule === "definition") {
    return {
      context: [found?.reason ?? `The ${words.pipeline} definition has an error.`],
      main: [
        retryAnswer(words, "I fixed the definition: run it again"),
        { ...IGNORE_ANSWER, title: "Close it" },
      ],
      other: [DATA_ANSWER("It is a data problem"), CODE_ANSWER("It is a code problem")],
      title: `Fix the ${words.pipeline} definition, then run it again?`,
    };
  }

  const change = columnChange(incident.violations, contract);

  if (change !== undefined && change.added.length > 0) {
    const file = datasetFile(contract, change.dataset) ?? change.dataset;
    const status =
      contract?.status === "approved" ? "approved" : contract?.status === "stale" ? "changed" : "draft";

    return {
      context: [
        `When dag-healer checked it, \`${file}\` had ${codeList(change.added)} where the ${status} contract ` +
          `expects ${codeList(change.missing)}.`,
        `${step} failed with \`${error}\`. Because ${contractPhrase(contract?.status, incident.dag_id)}, ` +
          "dag-healer does not guess whether the file or the code is wrong.",
      ],
      main: [
        CODE_ANSWER(
          "Yes, it changed on purpose",
          "The code should follow the new columns: dag-healer writes a test and a minimal fix, for you to review.",
        ),
        DATA_ANSWER("No, the file is wrong"),
      ],
      other: [retryAnswer(words), IGNORE_ANSWER],
      title: `Did \`${file}\` change on purpose?`,
    };
  }

  const context: Array<string> = [];

  if (rule === "c_no_approved_contract") {
    context.push(
      `${step} failed with \`${error}\`, an error about the shape of the data. Because ` +
        `${contractPhrase(found?.contractStatus, incident.dag_id)}, dag-healer does not decide whether the ` +
        "input or the code is wrong.",
    );
  } else if (rule === "b_code_with_valid_data") {
    context.push(
      `${step} failed with \`${error}\`. The input passes the approved contract and the traceback points into ` +
        "your code, but the model did not confirm a code defect.",
    );
  } else if (rule === "d_low_confidence" && found?.llmCategory !== undefined) {
    context.push(
      `${step} failed with \`${error}\`. The model leans to ${categoryWord(found.llmCategory).toUpperCase()} but ` +
        "is not sure enough to act on its own.",
    );
  } else {
    context.push(`${step} failed with \`${error}\`, and dag-healer cannot tell on its own what is wrong.`);
  }

  return {
    context,
    main: [
      DATA_ANSWER("The input is wrong"),
      CODE_ANSWER(
        "The code should accept it",
        "dag-healer writes a test that reproduces the failure and a minimal fix so the code handles this input.",
      ),
    ],
    other: [retryAnswer(words), IGNORE_ANSWER],
    title: "Is the input wrong, or should the code accept it?",
  };
};

/** What a person can still do with a reported (DATA) incident. */
export const reportedAnswers = (words: Vocabulary): Array<Answer> => [
  retryAnswer(words, `${words.retry_action} after the input is fixed`),
  CODE_ANSWER("The code should accept this input"),
  IGNORE_ANSWER,
];
