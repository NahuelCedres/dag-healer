/**
 * The pipelines: every pipeline dag-healer watches, has a contract for or has seen fail, with its
 * recent incidents and its contract in plain words. In the Airflow DAG tab it shows one pipeline.
 */
import { useId, useMemo, useRef } from "react";

import { type DagIncidentsResponse, getDagIncidents } from "src/api";
import { usePolledResource, usePollingIntervals } from "src/hooks/usePolledResource";
import { notMonitoredMessage } from "src/messages";
import { sortNewestFirst } from "src/model";
import { capitalize, useUiText } from "src/vocabulary";

import { ContractCard } from "./ContractCard";
import { useLayout, ViewLayout } from "./Layout";
import { ApiProblem } from "./Problems";
import { useConsoleData, usePipelineContract } from "./data";
import { useHeadingFocus } from "./focus";
import { contractTone, contractWord, GROUPS, incidentGroup, nextStep, stepLabel } from "./labels";
import { NavLink } from "./nav";
import { exactTime, relativeTime } from "./time";
import { Loading, Notice, RichText, StatusPill, toneStyle, usePageTitle } from "./ui";

/** Incidents shown per group on a pipeline's page. */
const PER_GROUP = 10;

/** One pipeline: its recent incidents by group, and its contract. */
export const PipelineDetail = ({ dagId }: { readonly dagId: string }) => {
  const { words } = useUiText();
  const id = useId();
  const { commandMs, refreshMs } = usePollingIntervals();
  const { embedded } = useLayout();
  const heading = useRef<HTMLHeadingElement>(null);
  const resource = usePolledResource<DagIncidentsResponse>(
    `dag:${dagId}`,
    (signal) => getDagIncidents(dagId, signal),
    (data) => (data?.contract?.pending_approval === true ? commandMs : refreshMs),
  );
  const { contract, file, refresh } = usePipelineContract(dagId);
  const incidents = useMemo(() => sortNewestFirst(resource.data?.incidents ?? []), [resource.data]);

  useHeadingFocus(heading, embedded ? false : true);

  return (
    <section
      aria-labelledby={id}
      className="stack"
      data-pipeline={dagId}
      data-testid="pipeline"
      style={{ gap: 20 }}
    >
      <div className="page-h">
        <div>
          <p className="lbl">{capitalize(words.pipeline)}</p>
          <h2
            className="mono"
            id={id}
            ref={heading}
            style={{ fontSize: 18, lineHeight: "26px" }}
            tabIndex={-1}
          >
            {dagId}
          </h2>
        </div>
      </div>
      {resource.error === undefined ? undefined : <ApiProblem error={resource.error} />}
      {resource.data !== undefined && !resource.data.monitored ? (
        <Notice>
          <p>{notMonitoredMessage(words)}</p>
        </Notice>
      ) : undefined}
      <section className="card" aria-label="Recent incidents">
        <h3>Recent incidents</h3>
        {resource.loading ? (
          <Loading />
        ) : incidents.length === 0 ? (
          <p className="muted">
            No incidents: no {words.step} of this {words.pipeline} has failed since dag-healer watches it.
          </p>
        ) : (
          GROUPS.map((group) => {
            const items = incidents.filter((incident) => incidentGroup(incident) === group.id);

            return items.length === 0 ? undefined : (
              <div className="stack" key={group.id} style={{ gap: 6 }}>
                <p className="lbl">
                  {group.label} · {items.length}
                </p>
                <ul className="tl">
                  {items.slice(0, PER_GROUP).map((incident) => (
                    <li data-incident-id={incident.id} key={incident.id}>
                      <span className="tt" title={exactTime(incident.created_at)}>
                        {relativeTime(incident.created_at).replace(" ago", "")}
                      </span>
                      <span className="stack" style={{ gap: 2 }}>
                        <span className="row" style={{ gap: 8 }}>
                          <StatusPill status={incident.status} />
                          <NavLink className="rowlink" place={{ incident, to: "incident" }}>
                            {stepLabel(incident)}
                          </NavLink>
                        </span>
                        <span className="faint small">{nextStep(incident, words)}</span>
                      </span>
                    </li>
                  ))}
                </ul>
                {items.length > PER_GROUP ? (
                  <p className="faint small">and {items.length - PER_GROUP} more in the inbox.</p>
                ) : undefined}
              </div>
            );
          })
        )}
      </section>
      <ContractCard
        contract={contract}
        file={file}
        level={3}
        onQueued={() => {
          refresh();
          resource.refresh();
        }}
        pipeline={dagId}
      />
    </section>
  );
};

/** What a contract is, and the selected pipeline's facts (the side panel). */
const AboutContracts = ({ dagId }: { readonly dagId?: string }) => {
  const { words } = useUiText();
  const dag = useConsoleData().dags?.data?.dags.find((candidate) => candidate.dag_id === dagId);

  return (
    <>
      {dag === undefined ? undefined : (
        <div className="psec">
          <dl className="kv">
            <dt>{capitalize(words.pipeline)}</dt>
            <dd className="mono">{dag.dag_id}</dd>
            <dt>Watched</dt>
            <dd>{dag.monitored ? "Yes" : "No (not in monitor.dags)"}</dd>
            <dt>Contract</dt>
            <dd>{contractWord(dag.contract_status)}</dd>
            <dt>Open incidents</dt>
            <dd>{dag.open_incidents}</dd>
          </dl>
        </div>
      )}
      <div className="psec">
        <h3 className="lbl">What a contract is</h3>
        <p className="muted">
          A YAML file per {words.pipeline}, under <code>healer/contracts/</code> in your repository: the
          columns its input must have, their types, ranges and the number of rows. dag-healer checks a failed
          run's input against it.
        </p>
        <p className="muted">
          A broken approved contract makes a failure DATA. While a contract is a draft, or changed after its
          approval, dag-healer asks you instead.
        </p>
        <p className="muted">
          YAML, because a program checks it: what people need to explain belongs in comments and notes next to
          it.
        </p>
      </div>
    </>
  );
};

export const Pipelines = ({ pipeline }: { readonly pipeline?: string }) => {
  const { words } = useUiText();
  const { embedded } = useLayout();
  const resource = useConsoleData().dags;
  const heading = useRef<HTMLHeadingElement>(null);
  const dags = resource?.data?.dags ?? [];
  const selected = pipeline ?? undefined;

  useHeadingFocus(heading, !embedded && selected === undefined);
  usePageTitle(
    embedded
      ? undefined
      : selected === undefined
        ? capitalize(words.pipelines)
        : `${selected} · ${capitalize(words.pipelines)}`,
  );

  if (embedded) {
    return (
      <ViewLayout panelTitle={`About this ${words.pipeline}`} testId="pipelines">
        {selected === undefined ? undefined : <PipelineDetail dagId={selected} />}
      </ViewLayout>
    );
  }

  return (
    <ViewLayout panel={<AboutContracts dagId={selected} />} panelTitle="About contracts" testId="pipelines">
      <header className="page-h">
        <div>
          <p className="lbl">{capitalize(words.pipelines)}</p>
          <h1 ref={heading} tabIndex={-1}>
            Every {words.pipeline} dag-healer watches
          </h1>
        </div>
      </header>
      {resource?.error === undefined ? undefined : (
        <ApiProblem error={resource.error} forbidden="You do not have access to dag-healer data." />
      )}
      {resource === undefined || resource.loading ? (
        <Loading />
      ) : dags.length === 0 ? (
        <div className="card">
          <p className="muted">
            <RichText
              text={`No ${words.pipelines} yet: list them under \`monitor.dags\` in healer.yaml, or draft a contract with \`dag-healer init\`.`}
            />
          </p>
        </div>
      ) : (
        <div className="card flush">
          <div className="scroll">
            <table className="tbl stackable" data-testid="pipelines-table">
              <thead>
                <tr>
                  <th scope="col">{capitalize(words.pipeline)}</th>
                  <th scope="col">Contract</th>
                  <th scope="col">Open incidents</th>
                  <th scope="col">Last incident</th>
                </tr>
              </thead>
              <tbody>
                {dags.map((dag) => (
                  <tr
                    aria-current={dag.dag_id === selected ? "true" : undefined}
                    data-dag-id={dag.dag_id}
                    key={dag.dag_id}
                  >
                    <td className="wide" data-label={capitalize(words.pipeline)}>
                      <NavLink className="rowlink" place={{ pipeline: dag.dag_id, to: "pipelines" }}>
                        {dag.dag_id}
                      </NavLink>
                      {dag.monitored ? undefined : (
                        <span className="tag" style={{ marginLeft: 8 }}>
                          not watched
                        </span>
                      )}
                    </td>
                    <td data-label="Contract">
                      <span className="pill plain" style={toneStyle(contractTone(dag.contract_status))}>
                        {contractWord(dag.contract_status)}
                      </span>
                    </td>
                    <td className="num" data-label="Open incidents">
                      {dag.open_incidents}
                    </td>
                    <td data-label="Last incident">
                      {dag.last_incident === null ? (
                        <span className="faint">None</span>
                      ) : (
                        <span className="row" style={{ gap: 8 }}>
                          <StatusPill status={dag.last_incident.status} />
                          <span className="faint small" title={exactTime(dag.last_incident.created_at)}>
                            {relativeTime(dag.last_incident.created_at)}
                          </span>
                        </span>
                      )}
                    </td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        </div>
      )}
      {selected === undefined ? (
        dags.length === 0 ? undefined : (
          <p className="faint small">Pick a {words.pipeline} to see its recent incidents and its contract.</p>
        )
      ) : (
        <PipelineDetail dagId={selected} key={selected} />
      )}
    </ViewLayout>
  );
};
