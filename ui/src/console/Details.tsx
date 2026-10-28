/** The details of an incident (where, when, how it was found), its alerts and its timeline. */
import type { IncidentDetail, IncidentEvent } from "src/api";
import { airflowHref } from "src/links";
import { nativeLinks } from "src/model";
import { capitalize, useUiText, type Vocabulary } from "src/vocabulary";

import { actionWords, foundBy, statusMeta, stepLabel } from "./labels";
import { NavLink } from "./nav";
import { exactTime, shortAge } from "./time";
import { CopyButton, Icon } from "./ui";

const seg = encodeURIComponent;

/** Links into the orchestrator's own pages: the server's native links, or Airflow's task page. */
const OpenIn = ({ incident }: { readonly incident: IncidentDetail }) => {
  const { host, words } = useUiText();
  const links = nativeLinks(incident);

  if (host === "airflow_plugin" && links.length === 0) {
    const mapped = incident.map_index >= 0 ? `/mapped/${incident.map_index}` : "";

    links.push({
      label: words.product,
      url: airflowHref(
        `dags/${seg(incident.dag_id)}/runs/${seg(incident.run_id)}/tasks/${seg(incident.task_id)}${mapped}`,
      ),
    });
  }
  if (links.length === 0) {
    return null;
  }

  return (
    <div className="psec">
      <h3 className="lbl">Open in {words.product}</h3>
      <div className="links">
        {links.map((link) => (
          <a
            className="btn sm ghost"
            href={link.url}
            key={link.label}
            rel="noopener noreferrer"
            target="_blank"
          >
            <Icon name="out" size={14} />
            {link.label === words.product ? `The ${words.step} in ${words.product}` : link.label}
          </a>
        ))}
      </div>
    </div>
  );
};

export const IncidentDetails = ({ incident }: { readonly incident: IncidentDetail }) => {
  const { words } = useUiText();
  const rows: Array<[string, string]> = [
    [capitalize(words.step), stepLabel(incident)],
    [capitalize(words.run), incident.run_id],
    [capitalize(words.attempt), String(incident.try_number)],
    ["Detected", exactTime(incident.created_at)],
    ["Updated", exactTime(incident.updated_at)],
  ];

  return (
    <>
      <div className="psec">
        <dl className="kv" data-testid="incident-details">
          <dt>{capitalize(words.pipeline)}</dt>
          <dd>
            <NavLink className="mono" place={{ pipeline: incident.dag_id, to: "pipelines" }}>
              {incident.dag_id}
            </NavLink>
          </dd>
          {rows.map(([label, value]) => (
            <div key={label} style={{ display: "contents" }}>
              <dt>{label}</dt>
              <dd className="mono">{value}</dd>
            </div>
          ))}
          <dt>Found by</dt>
          <dd>{foundBy(incident.source, words).replace(/^Found by |^Reported by |^Read from /u, "")}</dd>
          {incident.orchestrator === undefined || incident.orchestrator === "airflow" ? undefined : (
            <>
              <dt>Instance</dt>
              <dd className="mono">{incident.orchestrator}</dd>
            </>
          )}
          {incident.occurrences > 1 ? (
            <>
              <dt>Seen</dt>
              <dd>{incident.occurrences} times</dd>
            </>
          ) : undefined}
          {incident.attempts > 0 ? (
            <>
              <dt>Repair runs</dt>
              <dd>{incident.attempts}</dd>
            </>
          ) : undefined}
          {incident.tokens_used > 0 ? (
            <>
              <dt>Model tokens</dt>
              <dd>{incident.tokens_used.toLocaleString("en-US")}</dd>
            </>
          ) : undefined}
          <dt>Incident</dt>
          <dd className="mono">{incident.id}</dd>
        </dl>
        <div className="row">
          <CopyButton label="Incident id" text={incident.id} />
        </div>
      </div>
      <OpenIn incident={incident} />
    </>
  );
};

export const Alerts = ({ incident }: { readonly incident: IncidentDetail }) => (
  <div className="psec" data-testid="alerts">
    <h3 className="lbl">Alerts</h3>
    {incident.alerts.length === 0 ? (
      <p className="muted">No alert was sent for this incident.</p>
    ) : (
      <ul className="tl">
        {[...incident.alerts].reverse().map((alert, index) => (
          // Alerts are listed as recorded and never reordered.
          <li key={`${alert.sent_at}-${alert.channel}-${index}`}>
            <span className="tt" title={exactTime(alert.sent_at)}>
              {shortAge(alert.sent_at)}
            </span>
            <span className="msg">
              <strong>{alert.channel === "log" ? "Worker log" : alert.channel}</strong>{" "}
              {alert.status === "sent" ? "sent" : alert.status}
              {(alert.recipients ?? []).length > 0 ? ` to ${(alert.recipients ?? []).join(", ")}` : ""}:{" "}
              {alert.subject}
              {alert.error === null ? "" : ` (${alert.error})`}
            </span>
          </li>
        ))}
      </ul>
    )}
  </div>
);

const actorOf = (event: IncidentEvent): string => event.actor || "dag-healer";

const dataOf = (event: IncidentEvent): Record<string, unknown> =>
  event.data !== null && typeof event.data === "object" && !Array.isArray(event.data)
    ? (event.data as Record<string, unknown>)
    : {};

/** An alert outcome in plain words: "wrote the “Needs you” alert to the worker's log". */
const alertOutcome = (name: string, outcome: Record<string, unknown>): string => {
  const sent = outcome.status === "sent";
  const alert = `the “${name}” alert`;

  switch (outcome.channel) {
    case "log":
      return sent ? `wrote ${alert} to the worker's log` : `could not write ${alert} to the worker's log`;
    case "email":
      return sent ? `emailed ${alert}` : `could not email ${alert}`;
    default:
      return `${sent ? "sent" : "could not send"} ${alert} (${String(outcome.channel)})`;
  }
};

/**
 * Plain words for the events whose message is a log line (an alert, the incident's creation, a
 * queued action); undefined keeps the message the worker wrote.
 */
export const eventText = (event: IncidentEvent, words: Vocabulary): string | undefined => {
  const data = dataOf(event);

  switch (event.kind) {
    case "alert": {
      if (typeof data.event !== "string") {
        return undefined;
      }
      const name = statusMeta(data.event).label;
      const outcomes = Array.isArray(data.outcomes)
        ? data.outcomes.filter(
            (outcome): outcome is Record<string, unknown> => outcome !== null && typeof outcome === "object",
          )
        : [];

      return outcomes.length === 0
        ? `did not send the “${name}” alert: it is turned off in notify.events`
        : outcomes.map((outcome) => alertOutcome(name, outcome)).join("; ");
    }
    case "command_queued":
      return typeof data.action === "string" ? `asked to ${actionWords(data.action, words)}` : undefined;
    case "created": {
      const source = /^Incident created from (\S+)/u.exec(event.message)?.[1];
      const found = source === undefined ? undefined : foundBy(source, words);

      return found === undefined
        ? undefined
        : `opened the incident (${found.charAt(0).toLowerCase()}${found.slice(1)})`;
    }
    default:
      return undefined;
  }
};

/** The words of a message before its details ("Classified as code" of "Classified as code (…)"). */
const clause = (message: string): string => message.split(/[(:]/u)[0]?.trim() ?? "";

/**
 * True for a status event that says again what the event before it said (the worker records a
 * status next to the repair, error and classification events).
 */
const repeats = (before: IncidentEvent | undefined, event: IncidentEvent): boolean =>
  before !== undefined &&
  event.kind === "status" &&
  (event.message === before.message ||
    (clause(event.message).length >= 12 && clause(event.message) === clause(before.message)));

/** "<actor> <message>", without saying the actor twice when the message starts with it. */
export const EventText = ({ event }: { readonly event: IncidentEvent }) => {
  const { words } = useUiText();
  const actor = actorOf(event);
  const plain = eventText(event, words);

  if (plain !== undefined) {
    return (
      <>
        <strong>{actor}</strong> {plain}
      </>
    );
  }

  return event.message.startsWith(`${actor} `) ? (
    <>
      <strong>{actor}</strong>
      {event.message.slice(actor.length)}
    </>
  ) : (
    <>
      <strong>{actor}</strong> {event.message}
    </>
  );
};

export const Timeline = ({ events }: { readonly events: ReadonlyArray<IncidentEvent> }) => (
  <div className="psec" data-testid="timeline">
    <h3 className="lbl">Timeline</h3>
    {events.length === 0 ? (
      <p className="muted">Nothing recorded yet.</p>
    ) : (
      <ol className="tl">
        {events
          .filter((event, index) => !repeats(events[index - 1], event))
          .reverse()
          .map((event, index) => (
            // Events are listed newest first as recorded and never reordered.
            <li key={`${event.at}-${event.kind}-${index}`}>
              <span className="tt" title={exactTime(event.at)}>
                {shortAge(event.at)}
              </span>
              <span className="msg">
                <EventText event={event} />
              </span>
            </li>
          ))}
      </ol>
    )}
  </div>
);
