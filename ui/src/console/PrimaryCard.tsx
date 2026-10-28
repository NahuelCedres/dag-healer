/**
 * The first card of an incident, which says what it needs: the decision (a question in plain words
 * with its answers), what dag-healer is doing now, or what is left after a report, a proposed fix or
 * an ignore. Every answer shows its consequences before it is confirmed.
 */
import { useEffect, useId, useRef, useState } from "react";

import {
  approveContract,
  type ContractView,
  type IncidentAction,
  type IncidentDetail,
  runIncidentAction,
  type SetupResponse,
} from "src/api";
import { useUiText } from "src/vocabulary";

import { ConfirmPanel } from "./Confirm";
import { approvalConsequences, consequencesOf } from "./consequences";
import { type Answer, questionFor, reportedAnswers } from "./decision";
import { noteOf, repairOf } from "./events";
import { groupOf, stepLabel } from "./labels";
import { exactTime, relativeTime } from "./time";
import { Dot, Notice, RichText } from "./ui";

type Choice = IncidentAction | "approve_contract";

const ACTION_WORDS: Record<string, string> = {
  approve_contract: "approve the contract",
  ignore: "ignore the incident",
  resolve: "apply your decision",
  resolve_code: "write a test and a fix",
  resolve_data: "report a data problem",
  retry_repair: "run the repair again",
  retry_task: "run it again",
};

/** True when the contract can be approved from here. */
export const approvable = (contract: ContractView | null | undefined): contract is ContractView =>
  contract !== null &&
  contract !== undefined &&
  (contract.status === "draft" || contract.status === "stale") &&
  contract.can_approve &&
  !contract.pending_approval;

interface CardProps {
  readonly contract: ContractView | null | undefined;
  readonly incident: IncidentDetail;
  /** Called after an action was queued (the page then polls faster). */
  readonly onQueued: (action: Choice) => void;
  readonly setup?: SetupResponse;
}

/** The answers, the chosen one's consequences and the confirmation. */
const Answers = ({
  compact = false,
  contract,
  incident,
  main,
  onQueued,
  other,
  setup,
}: CardProps & {
  /** Small buttons only (the actions left after a report or a proposed fix). */
  readonly compact?: boolean;
  readonly main: Array<Answer>;
  readonly other: Array<Answer>;
}) => {
  const text = useUiText();
  const [choice, setChoice] = useState<Choice>();
  const allowed = (answer: Answer) => incident.allowed_actions.includes(answer.action);
  const shownMain = main.filter(allowed);
  const shownOther = other.filter(allowed);
  const first = compact ? [] : shownMain.length > 0 ? shownMain : shownOther.slice(0, 2);
  const rest = compact
    ? [...shownMain, ...shownOther]
    : shownMain.length > 0
      ? shownOther
      : shownOther.slice(2);
  const approval = approvable(contract) ? contract : undefined;
  const titleOf = (action: Choice): string =>
    action === "approve_contract"
      ? "Approve the contract"
      : ([...main, ...other].find((answer) => answer.action === action)?.title ?? action);

  const toggle = (action: Choice) => setChoice((current) => (current === action ? undefined : action));

  const confirm = async (note: string) => {
    if (choice === undefined) {
      return;
    }
    if (choice === "approve_contract") {
      await approveContract(incident.dag_id, note);
    } else {
      await runIncidentAction(incident.dag_id, incident.id, choice, note);
    }
    const done = choice;

    setChoice(undefined);
    onQueued(done);
  };

  return (
    <>
      {first.length === 0 ? undefined : (
        <div className="choices">
          {first.map((answer) => (
            <button
              aria-pressed={choice === answer.action}
              className="choice"
              data-action={answer.action}
              key={answer.action}
              onClick={() => toggle(answer.action)}
              type="button"
            >
              <strong>
                <RichText text={answer.title} />
              </strong>
              <span>
                <RichText text={answer.body} />
              </span>
            </button>
          ))}
        </div>
      )}
      {rest.length === 0 && approval === undefined ? undefined : (
        <div className="row">
          {first.length === 0 ? undefined : <span className="faint small">Not sure yet?</span>}
          {rest.map((answer) => (
            <button
              aria-pressed={choice === answer.action}
              className="btn sm"
              data-action={answer.action}
              key={answer.action}
              onClick={() => toggle(answer.action)}
              type="button"
            >
              {answer.title}
            </button>
          ))}
          {approval === undefined ? undefined : (
            <button
              aria-pressed={choice === "approve_contract"}
              className="btn sm ghost"
              data-action="approve_contract"
              onClick={() => toggle("approve_contract")}
              type="button"
            >
              Approve the contract
            </button>
          )}
        </div>
      )}
      {choice === undefined ? undefined : (
        <ConfirmPanel
          consequences={
            choice === "approve_contract" && approval !== undefined
              ? approvalConsequences(approval, text, incident.status)
              : consequencesOf(choice as IncidentAction, incident, text, setup, contract)
          }
          heading={`If you choose “${titleOf(choice).replaceAll("`", "")}”`}
          key={choice}
          onCancel={() => setChoice(undefined)}
          onConfirm={confirm}
        />
      )}
    </>
  );
};

/** The decision of an incident that needs a person (waiting_human, repair_failed, error). */
const DecisionCard = (props: CardProps) => {
  const { contract, incident } = props;
  const { words } = useUiText();
  const id = useId();
  const question = questionFor(incident, words, contract);

  return (
    <section aria-labelledby={id} className="card decide" data-testid="decision">
      <div className="card-h">
        <p className="lbl">Needs your decision</p>
        <span className="faint small">You decide; dag-healer does the rest.</span>
      </div>
      <h2 className="q" id={id}>
        <RichText text={question.title} />
      </h2>
      {question.context.map((line) => (
        <p className="muted" key={line}>
          <RichText text={line} />
        </p>
      ))}
      {incident.can_edit ? (
        <Answers {...props} main={question.main} other={question.other} />
      ) : (
        <Notice>
          <p>
            You can read this incident but not act on it: someone who may change{" "}
            <span className="mono">{incident.dag_id}</span> decides.
          </p>
        </Notice>
      )}
    </section>
  );
};

/** What dag-healer is doing now, for an incident in progress. */
const ProgressCard = ({ incident }: { readonly incident: IncidentDetail }) => {
  const id = useId();
  const { words } = useUiText();
  const doing: Record<string, string> = {
    classified: "It classified the failure and is acting on it.",
    new: `It found the failure and will classify it shortly (it classifies a limited number of failures per hour).`,
    repairing:
      "It is writing a test that reproduces the failure, then a minimal fix, in a sandbox. This usually takes a few " +
      "minutes.",
    reporting: "It is reporting the data problem and sending the alert.",
    resumed: "It is applying a person's decision.",
    triaging: `It is reading the error, the ${words.step}'s code and the contract to classify the failure.`,
  };

  return (
    <section aria-labelledby={id} className="card queued" data-testid="progress">
      <Dot tone="cyan" />
      <div className="stack" style={{ gap: 4 }}>
        <h2 id={id}>dag-healer is working on it</h2>
        <p className="muted">
          {doing[incident.status] ?? "It is working on this incident."} Nothing is needed from you yet.
        </p>
      </div>
    </section>
  );
};

/** After this long, a queued action is worth a word about the worker. */
const SLOW_COMMAND_MS = 2 * 60_000;

/** A queued action, until the worker applies it (no cancel yet: the API has no route for it). */
const PendingCard = ({
  incident,
  queued,
}: {
  readonly incident: IncidentDetail;
  readonly queued?: Choice;
}) => {
  const { words } = useUiText();
  const ref = useRef<HTMLParagraphElement>(null);
  const command = incident.pending_commands[0];
  const kind =
    command === undefined
      ? queued
      : command.kind === "resolve"
        ? (incident.events
            .filter((event) => event.kind === "command_queued")
            .map((event) => (event.data as Record<string, unknown> | null)?.action)
            .filter((action): action is string => typeof action === "string")
            .at(-1) ?? "resolve")
        : command.kind;
  const what =
    kind === "retry_task"
      ? words.retry_action.toLowerCase()
      : (ACTION_WORDS[kind ?? "resolve"] ?? "your action");
  const waited = command === undefined ? 0 : Date.now() - new Date(command.created_at ?? "").getTime();

  // Focus follows the viewer's own action only: a command someone else queued must not move it.
  const own = queued !== undefined;

  useEffect(() => {
    if (own) {
      ref.current?.focus();
    }
  }, [own]);

  return (
    <section className="card queued" data-testid="queued">
      <Dot tone="cyan" />
      <div className="stack" style={{ gap: 4 }}>
        <p ref={ref} role="status" tabIndex={-1}>
          <strong>Queued: {what}.</strong>{" "}
          <span className="muted">
            {command === undefined
              ? "dag-healer's worker applies it within a few seconds."
              : `Asked by ${command.actor} ${relativeTime(command.created_at)}; dag-healer's worker applies it within a few seconds.`}
          </span>
        </p>
        {waited > SLOW_COMMAND_MS ? (
          <p className="faint small">
            It has waited {relativeTime(command?.created_at).replace(" ago", "")}: check that dag-healer's
            worker is running (dag-healer worker).
          </p>
        ) : undefined}
      </div>
    </section>
  );
};

/** A reported data problem: who was told, and what a person can still do. */
const ReportedCard = (props: CardProps) => {
  const { incident } = props;
  const id = useId();
  const { words } = useUiText();
  const sent = incident.alerts.filter((alert) => alert.status === "sent");
  const emails = [...new Set(sent.flatMap((alert) => alert.recipients ?? []))];

  return (
    <section aria-labelledby={id} className="card" data-testid="reported">
      <div className="card-h">
        <h2 id={id}>Reported as a data problem</h2>
      </div>
      <p className="muted">
        {emails.length > 0
          ? `dag-healer emailed ${emails.join(", ")} with the failed checks and a masked sample. `
          : sent.length > 0
            ? "dag-healer wrote the alert to the worker's log (no email is set up). "
            : "No alert was sent. "}
        It does not run <span className="mono">{stepLabel(incident)}</span> again by itself: once the input is
        fixed, use {words.retry_action}.
      </p>
      {incident.can_edit ? (
        <Answers {...props} compact main={[]} other={reportedAnswers(words)} />
      ) : undefined}
    </section>
  );
};

/** A proposed fix: the steps until the pipeline runs again with it. */
const FixProposedCard = (props: CardProps) => {
  const { incident } = props;
  const id = useId();
  const { words } = useUiText();
  const repair = repairOf(incident.events);

  return (
    <section aria-labelledby={id} className="card" data-testid="fix-proposed">
      <div className="card-h">
        <h2 id={id}>A fix is proposed</h2>
        <span className="faint small">dag-healer never merges: the steps below are yours.</span>
      </div>
      <ol className="steps">
        <li>
          <strong>Review the change</strong>
          <span className="muted">The test, the diff and the test results are below.</span>
        </li>
        <li>
          <strong>{incident.pr_url === null ? "Open the pull request" : "Merge the pull request"}</strong>
          <span className="muted">
            {incident.pr_url === null ? (
              <>
                No pull request was opened: the fix is on branch{" "}
                <span className="mono">{repair?.branch ?? "healer/…"}</span> in the worker's copy of the
                repository, described in PR.md. Push the branch and open the pull request with it.
              </>
            ) : (
              <a href={incident.pr_url} rel="noopener noreferrer" target="_blank">
                The pull request
              </a>
            )}
          </span>
        </li>
        <li>
          <strong>Deploy it</strong>
          <span className="muted">
            A {words.step} runs the code that is deployed: deploy the merged fix before you run it again.
          </span>
        </li>
        <li>
          <strong>{words.retry_action}</strong>
          <span className="muted">
            dag-healer does not follow the result yet: check that the run succeeds.
          </span>
          {incident.can_edit ? (
            <Answers
              {...props}
              compact
              main={[]}
              other={[
                {
                  action: "retry_task",
                  body: `${words.product} runs the failed ${words.step} again.`,
                  title: words.retry_action,
                },
              ]}
            />
          ) : undefined}
        </li>
      </ol>
    </section>
  );
};

/** An ignored incident: who closed it, and their note. */
const IgnoredCard = ({ incident }: { readonly incident: IncidentDetail }) => {
  const id = useId();
  const event = [...incident.events]
    .reverse()
    .find((candidate) => candidate.message.startsWith("Ignored by"));
  const note = event === undefined ? undefined : noteOf(event.message);

  return (
    <section aria-labelledby={id} className="card" data-testid="ignored">
      <h2 id={id}>Ignored</h2>
      <p className="muted">
        {incident.resolved_by === null ? "Closed" : `Closed by ${incident.resolved_by}`} without any action
        {event === undefined ? "" : ` on ${exactTime(event.at)}`}. The timeline keeps what happened.
      </p>
      {note === undefined ? undefined : <p className="note-box">“{note}”</p>}
    </section>
  );
};

/** The card that leads the incident page, by what the incident needs now. */
export const PrimaryCard = ({ queued, ...props }: CardProps & { readonly queued?: Choice }) => {
  const { incident } = props;

  if (incident.pending_commands.length > 0 || queued !== undefined) {
    return <PendingCard incident={incident} queued={queued} />;
  }
  switch (incident.status) {
    case "reported":
      return <ReportedCard {...props} />;
    case "repaired":
      return <FixProposedCard {...props} />;
    case "ignored":
      return <IgnoredCard incident={incident} />;
    default:
      return groupOf(incident.status) === "needs" ? (
        <DecisionCard {...props} />
      ) : (
        <ProgressCard incident={incident} />
      );
  }
};
