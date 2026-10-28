/**
 * What an action leads to, shown before it is confirmed: what dag-healer will do, who gets an alert
 * and which files change. Built from the incident, the contract and `GET /setup` (the deployment's
 * alert recipients, how fixes are delivered and their limits), so every line says what this
 * dag-healer really does. Lines may hold `code` spans in backticks.
 */
import type { ContractView, IncidentAction, IncidentDetail, SetupResponse } from "src/api";
import { actionWarnings } from "src/model";
import { orchestratorType, type UiText } from "src/vocabulary";

import { stepLabel } from "./labels";

export interface Consequences {
  /** The label of the confirm button ("Report a data problem"). */
  readonly confirm: string;
  /** Who is told (alerts). */
  readonly alert?: string;
  /** What dag-healer will do. */
  readonly does: Array<string>;
  /** Which files change. */
  readonly files?: string;
  /** What happens to the incident afterwards. */
  readonly then?: string;
  /** Warnings of the server for this action (`action_warnings`), and other cautions. */
  readonly warnings: Array<string>;
}

const OPEN = new Set(["new", "triaging", "classified", "reporting", "repairing", "waiting_human", "resumed"]);

const list = (items: ReadonlyArray<string>): string => {
  if (items.length <= 1) {
    return items.join("");
  }

  return `${items.slice(0, -1).join(", ")} and ${items.at(-1) ?? ""}`;
};

/** Who hears about `event` ("reported", "repaired"...), from `notify` (unknown without setup). */
export const alertLine = (
  setup: SetupResponse | undefined,
  events: ReadonlyArray<string>,
): string | undefined => {
  if (setup === undefined) {
    return undefined;
  }
  const on = events.filter((event) => setup.alerts.events.includes(event));

  if (on.length === 0) {
    return "Nobody is alerted: these alerts are turned off in `notify.events`.";
  }
  if (setup.alerts.email.length === 0) {
    return "The alert is written to the worker's log only: no email is set up (`notify.email`).";
  }

  return `Emails ${list(setup.alerts.email.map((address) => `\`${address}\``))}, and writes the alert to the worker's log.`;
};

/** What a fix may change, from `repo.allowed_paths` and the diff limits. */
export const filesLine = (setup: SetupResponse | undefined): string => {
  if (setup === undefined) {
    return "The fix may change your code and adds a test. It never changes contracts, healer.yaml or CI settings.";
  }
  const {
    allowed_paths: paths,
    max_diff_files: maxFiles,
    max_diff_lines: maxLines,
    tests_dir: tests,
  } = setup.repairs;
  const where = paths.length === 0 ? "your code" : `files under ${list(paths.map((path) => `\`${path}\``))}`;
  const test = tests === null ? "adds a test" : `adds a test under \`${tests}/\``;

  return (
    `The fix may change ${where} (at most ${maxFiles} files and ${maxLines} lines) and ${test}. ` +
    "It never changes contracts, healer.yaml or CI settings."
  );
};

/** How a fix reaches the repository, from `github` (a pull request, or PR.md without a token). */
export const deliveryLine = (setup: SetupResponse | undefined, incident: IncidentDetail): string => {
  const branch = `\`healer/${incident.dag_id}-${incident.task_id}-…\``;

  if (setup === undefined) {
    return `The fix goes on a new branch ${branch}, as a pull request or as PR.md. Nothing is merged: you review it.`;
  }
  const { pr_base: base, repository, skip_ci: skipCi, token_configured: token } = setup.repairs;
  const ci = skipCi
    ? " Its commit carries `[skip ci]`: your CI runs only after a person has reviewed it."
    : "";

  if (token && repository !== null) {
    return `It opens a pull request on \`${repository}\` against \`${base}\`. Nothing is merged: you review it.${ci}`;
  }
  if (token) {
    return (
      `It opens a pull request against \`${base}\` on the repository's GitHub origin (\`github.repo\` is not set). ` +
      `Nothing is merged: you review it.${ci}`
    );
  }

  return (
    `No GitHub token is set, so it commits the fix on a new branch ${branch} in the worker's copy of the ` +
    `repository and writes PR.md and fix.patch for you to open the pull request.${ci}`
  );
};

/** What the orchestrator does on a retry, in its own words. */
export const retryLine = (incident: IncidentDetail, text: UiText): string => {
  const { words } = text;
  const step = `\`${stepLabel(incident)}\``;

  switch (orchestratorType(text)) {
    case "airflow":
      return (
        `Airflow clears ${step} in DAG run \`${incident.run_id}\`, and the tasks after it that failed or were ` +
        "blocked by it, so they run again with the code deployed now."
      );
    case "stepfunctions":
      return (
        `AWS Step Functions redrives execution \`${incident.run_id}\` from the state that failed, with the code ` +
        "deployed now."
      );
    default:
      return `${words.product} runs ${step} again (${words.retry_action}), with the code deployed now.`;
  }
};

const contractStillOpen = (
  contract: ContractView | null | undefined,
  incident: IncidentDetail,
): string | undefined => {
  if (contract === undefined) {
    return undefined;
  }
  if (contract === null) {
    return `\`${incident.dag_id}\` has no contract, so the next failure like this one waits for a person again.`;
  }
  if (contract.status === "draft" || contract.status === "stale") {
    return (
      `The contract stays ${contract.status === "draft" ? "a draft" : "unapproved"}, so the next failure like this ` +
      "one waits for a person again. Approve it to let dag-healer decide on its own."
    );
  }

  return undefined;
};

/**
 * The consequences of `action` on `incident`. `contract` is the incident's contract (null: none,
 * undefined: not loaded); `setup` is `GET /setup` (undefined while it loads or when it failed).
 */
export const consequencesOf = (
  action: IncidentAction,
  incident: IncidentDetail,
  text: UiText,
  setup: SetupResponse | undefined,
  contract?: ContractView | null,
): Consequences => {
  const { words } = text;
  const step = `\`${stepLabel(incident)}\``;
  const warnings = actionWarnings(incident, action);

  switch (action) {
    case "resolve_data":
      return {
        alert: alertLine(setup, ["reported"]),
        confirm: "Report a data problem",
        does: [
          "dag-healer treats this as a data problem: it reports the failed checks with a masked sample. No code changes.",
          ...[contractStillOpen(contract, incident)].filter((line): line is string => line !== undefined),
        ],
        then:
          `The incident moves to Reported. dag-healer does not run ${step} again: once the input is fixed, use ` +
          `${words.retry_action}.`,
        warnings,
      };
    case "resolve_code":
    case "retry_repair": {
      const retry = action === "retry_repair";

      return {
        alert: alertLine(setup, ["repaired", "repair_failed"]),
        confirm: retry ? "Run the repair again" : "Write a test and a fix",
        does: [
          (retry
            ? "dag-healer runs the repair again on a fresh copy of the repository, as a code problem: "
            : "dag-healer treats this as a code problem: ") +
            "it writes a test that reproduces the failure, then the smallest fix that makes it pass, and runs " +
            "the tests in a sandbox.",
          deliveryLine(setup, incident),
          ...(contract !== undefined && contract !== null && contract.status !== "approved"
            ? [
                "The fix never changes the contract: if the input changed on purpose, update " +
                  `\`${contract.path}\` yourself, then approve it.`,
              ]
            : []),
          ...(retry ? ["Your note goes to the timeline; the repair does not read it yet."] : []),
        ],
        files: filesLine(setup),
        then: "The incident moves to Fixing, then to Fix proposed, or back to Needs you if the fix fails.",
        warnings,
      };
    }
    case "retry_task": {
      const fixPending = incident.status === "repaired" && (incident.pr_url !== null || incident.has_pr_md);

      return {
        confirm: words.retry_action,
        does: [retryLine(incident, text)],
        then: OPEN.has(incident.status)
          ? `This incident stays as it is. If ${step} fails again the same way, the failure is added to it.`
          : `This incident stays as it is. If ${step} fails again, dag-healer opens a new incident.`,
        warnings: [
          ...(fixPending
            ? [
                `The proposed fix runs only once it is merged and deployed: before that, ${step} runs the old code.`,
              ]
            : []),
          ...warnings,
        ],
      };
    }
    case "ignore":
      return {
        confirm: "Ignore the incident",
        does: ["dag-healer closes the incident without any action: nothing is sent and no code changes."],
        then: `It cannot be reopened from the console. If ${step} fails again, dag-healer opens a new incident.`,
        warnings,
      };
    default:
      return { confirm: String(action), does: [], warnings };
  }
};

/**
 * The consequences of approving `contract`. `incidentStatus` is the status of the incident the
 * approval is offered on, if any: an open incident is not decided again by the approval.
 */
export const approvalConsequences = (
  contract: ContractView,
  text: UiText,
  incidentStatus?: string,
): Consequences => ({
  confirm: "Approve the contract",
  does: [
    `The worker marks \`${contract.path}\` as approved by you: it writes \`status: approved\`, your name and the ` +
      "time into the file in its copy of the repository. It does not commit the file or open a pull request: " +
      "commit it yourself so the approval is kept.",
    `From then on, an input that breaks this contract makes a ${text.words.step} failure DATA without asking ` +
      "you, and dag-healer decides the others knowing the input is right.",
    "The worker refuses the approval if the file changes before it applies it.",
  ],
  files: `Only \`${contract.path}\` changes.`,
  then:
    incidentStatus !== undefined && OPEN.has(incidentStatus)
      ? "This incident is not decided again: answer the question as well."
      : undefined,
  warnings: [],
});
