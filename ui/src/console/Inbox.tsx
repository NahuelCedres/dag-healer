/**
 * The inbox: every incident of every pipeline this dag-healer watches, grouped by what it needs
 * (Needs you, In progress, Done), with filters and a search over pipelines, steps, errors and runs.
 */
import { useId, useMemo, useRef, useState } from "react";

import type { IncidentSummary } from "src/api";
import { sortNewestFirst } from "src/model";
import { capitalize, useUiText } from "src/vocabulary";

import { ViewLayout } from "./Layout";
import { ApiProblem } from "./Problems";
import { useConsoleData } from "./data";
import { useHeadingFocus } from "./focus";
import { categoryWord, GROUPS, groupOf, type InboxGroup, nextStep, stepLabel } from "./labels";
import { NavLink } from "./nav";
import { exactTime, relativeTime } from "./time";
import { CategoryChip, Icon, Loading, StatusPill } from "./ui";

const ALL = "";
const UNCLASSIFIED = "none";

const SHOW: ReadonlyArray<{ id: InboxGroup | typeof ALL; label: string }> = [
  { id: ALL, label: "All" },
  ...GROUPS,
];

/** The incidents that match the search (pipeline, step, error, run or incident id). */
export const matches = (incident: IncidentSummary, query: string): boolean => {
  const needle = query.trim().toLowerCase();

  if (needle.length === 0) {
    return true;
  }

  return [
    incident.dag_id,
    incident.task_id,
    incident.error_summary,
    incident.error_name ?? "",
    incident.run_id,
    incident.id,
  ]
    .join("\n")
    .toLowerCase()
    .includes(needle);
};

/** How dag-healer works, for someone new to it (in the inbox's side panel). */
const HowItWorks = () => {
  const { words } = useUiText();

  return (
    <>
      <div className="psec">
        <h3 className="lbl">How dag-healer works</h3>
        <ol className="steps">
          <li>
            <strong>It notices a failure</strong>
            <span className="muted">
              A {words.step} fails in {words.product}; dag-healer records an incident within seconds.
            </span>
          </li>
          <li>
            <strong>It classifies it</strong>
            <span className="muted">
              Fixed rules decide first, from the {words.pipeline}'s contract; the model helps within those
              rules.
            </span>
          </li>
          <li>
            <strong>It acts, or asks you</strong>
            <span className="muted">
              DATA: it reports the broken input. CODE: it writes a test, a minimal fix and a pull request.
              AMBIGUOUS: it asks you.
            </span>
          </li>
        </ol>
      </div>
      <div className="psec">
        <h3 className="lbl">The groups</h3>
        <dl className="kv">
          <dt>Needs you</dt>
          <dd className="muted">A decision only a person can make, or a fix that failed.</dd>
          <dt>In progress</dt>
          <dd className="muted">dag-healer is classifying, reporting or fixing.</dd>
          <dt>Done</dt>
          <dd className="muted">
            Reported, fix proposed or ignored. A proposed fix still needs your review.
          </dd>
        </dl>
      </div>
      <div className="psec">
        <h3 className="lbl">Contracts</h3>
        <p className="muted">
          A contract is a YAML file per {words.pipeline} that says what its input must look like: the columns,
          their types, ranges and row counts. dag-healer checks a failed run's input against it. A broken
          approved contract means DATA; a draft one means it asks you.
        </p>
      </div>
    </>
  );
};

export const Inbox = ({ group }: { readonly group?: InboxGroup }) => {
  const { words } = useUiText();
  const resource = useConsoleData().incidents;
  const heading = useRef<HTMLHeadingElement>(null);
  const searchId = useId();
  const [shown, setShown] = useState<InboxGroup | typeof ALL>(group ?? ALL);
  const [query, setQuery] = useState("");
  const [pipeline, setPipeline] = useState(ALL);
  const [category, setCategory] = useState(ALL);
  const incidents = useMemo(() => sortNewestFirst(resource?.data?.incidents ?? []), [resource?.data]);

  useHeadingFocus(heading, true);

  const counts = useMemo(() => {
    const result: Record<string, number> = { done: 0, needs: 0, progress: 0 };

    for (const incident of incidents) {
      result[groupOf(incident.status)] = (result[groupOf(incident.status)] ?? 0) + 1;
    }

    return result;
  }, [incidents]);
  const pipelines = useMemo(
    () => [...new Set(incidents.map((incident) => incident.dag_id))].sort(),
    [incidents],
  );
  const rows = incidents.filter(
    (incident) =>
      (shown === ALL || groupOf(incident.status) === shown) &&
      (pipeline === ALL || incident.dag_id === pipeline) &&
      (category === ALL || (incident.category ?? UNCLASSIFIED) === category) &&
      matches(incident, query),
  );
  const filtered = query.trim().length > 0 || pipeline !== ALL || category !== ALL;
  const total = resource?.data?.total ?? incidents.length;
  const clear = () => {
    setQuery("");
    setPipeline(ALL);
    setCategory(ALL);
  };

  return (
    <ViewLayout panel={<HowItWorks />} panelTitle="About dag-healer" testId="inbox">
      {resource?.error === undefined ? undefined : (
        <ApiProblem error={resource.error} forbidden="You do not have access to dag-healer data." />
      )}
      <header className="page-h">
        <div>
          <p className="lbl">Inbox</p>
          <h1 ref={heading} tabIndex={-1}>
            What failed, and what dag-healer did about it
          </h1>
        </div>
      </header>
      <div className="fchips" role="group" aria-label="Show">
        {SHOW.map((option) => (
          <button
            aria-pressed={shown === option.id}
            className="fchip"
            data-group={option.id || "all"}
            key={option.id || "all"}
            onClick={() => setShown(option.id)}
            type="button"
          >
            {option.label}
            <span className="n">{option.id === ALL ? incidents.length : (counts[option.id] ?? 0)}</span>
          </button>
        ))}
      </div>
      <div className="filters">
        <label className="field" htmlFor={searchId} style={{ flex: "1 1 260px" }}>
          <span>Search</span>
          <input
            className="input"
            id={searchId}
            onChange={(event) => setQuery(event.target.value)}
            placeholder={`${capitalize(words.pipeline)}, ${words.step}, error or run`}
            type="search"
            value={query}
          />
        </label>
        <label className="field">
          <span>{capitalize(words.pipeline)}</span>
          <select className="select" onChange={(event) => setPipeline(event.target.value)} value={pipeline}>
            <option value={ALL}>All {words.pipelines}</option>
            {pipelines.map((id) => (
              <option key={id} value={id}>
                {id}
              </option>
            ))}
          </select>
        </label>
        <label className="field">
          <span>Category</span>
          <select className="select" onChange={(event) => setCategory(event.target.value)} value={category}>
            <option value={ALL}>All categories</option>
            {["data", "code", "ambiguous", UNCLASSIFIED].map((id) => (
              <option key={id} value={id}>
                {categoryWord(id === UNCLASSIFIED ? null : id)}
              </option>
            ))}
          </select>
        </label>
        {filtered ? (
          <button className="btn sm ghost" onClick={clear} type="button">
            Clear the filters
          </button>
        ) : undefined}
      </div>
      {resource === undefined || resource.loading ? (
        <Loading what="Loading the incidents…" />
      ) : incidents.length === 0 && resource.data !== undefined ? (
        <div className="card" data-testid="inbox-empty">
          <h2>No incidents yet</h2>
          <p className="muted">
            When a {words.step} of a watched {words.pipeline} fails, its incident shows up here.
          </p>
        </div>
      ) : rows.length === 0 && resource.data !== undefined ? (
        <div className="card" data-testid="inbox-none">
          <p className="muted">
            {shown === "needs" && !filtered
              ? "Nothing needs you right now."
              : "No incident matches. Try another group, or clear the filters."}
          </p>
        </div>
      ) : (
        <div className="card flush">
          <div className="scroll">
            <table className="tbl stackable" data-testid="inbox-table">
              <caption className="sr-only">
                {rows.length} {rows.length === 1 ? "incident" : "incidents"}
              </caption>
              <thead>
                <tr>
                  <th scope="col">Status</th>
                  <th scope="col">
                    {capitalize(words.pipeline)} / {words.step}
                  </th>
                  <th scope="col">Category</th>
                  <th scope="col">Next step</th>
                  <th scope="col">Detected</th>
                </tr>
              </thead>
              <tbody>
                {rows.map((incident) => (
                  <tr data-incident-id={incident.id} key={incident.id}>
                    <td className="nowrap" data-label="Status">
                      <StatusPill status={incident.status} />
                    </td>
                    <td className="wide" data-label={`${capitalize(words.pipeline)} / ${words.step}`}>
                      <NavLink className="rowlink" place={{ incident, to: "incident" }}>
                        {incident.dag_id} / {stepLabel(incident)}
                      </NavLink>
                      <div className="faint small mono wrap-anywhere">
                        {(incident.error_summary.split("\n")[0] ?? incident.error_name ?? "").slice(0, 140)}
                      </div>
                    </td>
                    <td data-label="Category">
                      <CategoryChip category={incident.category} />
                    </td>
                    <td className="muted" data-label="Next step">
                      {nextStep(incident, words)}
                      {incident.occurrences > 1 ? (
                        <div className="faint small">Seen {incident.occurrences} times</div>
                      ) : undefined}
                    </td>
                    <td className="num muted" data-label="Detected" title={exactTime(incident.created_at)}>
                      {relativeTime(incident.created_at)}
                    </td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        </div>
      )}
      {total > incidents.length ? (
        <p className="faint small">
          Showing the newest {incidents.length} of {total} incidents.
        </p>
      ) : undefined}
      <p className="faint small">
        <NavLink place={{ to: "pipelines" }}>
          <Icon name="pipelines" size={14} /> See every {words.pipeline} and its contract
        </NavLink>
      </p>
    </ViewLayout>
  );
};
