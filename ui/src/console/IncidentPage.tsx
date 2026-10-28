/**
 * The incident page. It leads with what the incident needs (the decision, for an incident waiting
 * for a person), then the evidence (what happened, the contract check, the masked sample), the fix,
 * and, in the side panel, the details, the alerts and the timeline.
 */
import { useEffect, useRef, useState } from "react";

import {
  type ApiError,
  type ContractView,
  getIncident,
  getTaskInstanceIncident,
  hasSessionExpired,
  type IncidentAction,
  type IncidentDetail,
} from "src/api";
import { usePolledResource, usePollingIntervals } from "src/hooks/usePolledResource";
import { noStepInstanceIncidentMessage } from "src/messages";
import { useUiText } from "src/vocabulary";

import { Alerts, EventText, IncidentDetails, Timeline } from "./Details";
import { ContractCheck, ErrorDetails, HowDecided, SampleCard, WhatHappened } from "./Evidence";
import { FixCard } from "./Fix";
import { useLayout, ViewLayout } from "./Layout";
import { PrimaryCard } from "./PrimaryCard";
import { ApiProblem } from "./Problems";
import { usePipelineContract, useSetup } from "./data";
import { decidedBy } from "./decision";
import { classificationOf, contextOf, latestEvent } from "./events";
import { useHeadingFocus } from "./focus";
import { foundBy, groupOf, statusMeta, stepLabel } from "./labels";
import { NavLink } from "./nav";
import { exactTime, relativeTime, shortAge } from "./time";
import { CategoryChip, Loading, Notice, RichText, StatusPill, useAnnounce, usePageTitle } from "./ui";

export type IncidentSource =
  | { readonly dagId: string; readonly incidentId: string }
  | { readonly dagId: string; readonly mapIndex: number; readonly runId: string; readonly taskId: string };

const sourceKey = (source: IncidentSource): string =>
  "incidentId" in source
    ? JSON.stringify(["incident", source.dagId, source.incidentId])
    : JSON.stringify(["ti", source.dagId, source.runId, source.taskId, source.mapIndex]);

const load = async (source: IncidentSource, signal: AbortSignal): Promise<IncidentDetail | null> => {
  if ("incidentId" in source) {
    try {
      return await getIncident(source.dagId, source.incidentId, signal);
    } catch (caught) {
      if ((caught as ApiError).status === 404) {
        return null;
      }
      throw caught;
    }
  }

  return (
    await getTaskInstanceIncident(
      source.dagId,
      { mapIndex: source.mapIndex, runId: source.runId, taskId: source.taskId },
      signal,
    )
  ).incident;
};

type Queued = IncidentAction | "approve_contract";

/**
 * The contract check is shown once dag-healer looked at the input (or found violations): before
 * that there is nothing to say, and a CODE incident of a pipeline without contract has no check.
 */
const showsContractCheck = (incident: IncidentDetail, contract: ContractView | null | undefined): boolean => {
  if (incident.violations.length > 0) {
    return true;
  }
  const looked = contextOf(incident.events) !== undefined || classificationOf(incident.events) !== undefined;

  return looked && !(incident.category === "code" && contract === null);
};

const IncidentView = ({
  incident,
  onQueued,
  problem,
  queued,
}: {
  readonly incident: IncidentDetail;
  readonly onQueued: (action: Queued) => void;
  /** A failed refresh (the last good answer stays on screen). */
  readonly problem?: ApiError;
  readonly queued?: Queued;
}) => {
  const text = useUiText();
  const { words } = text;
  const setup = useSetup();
  const { embedded } = useLayout();
  const { contract, file, refresh } = usePipelineContract(incident.dag_id);
  const heading = useRef<HTMLHeadingElement>(null);
  const decided = decidedBy(incident, words, setup?.min_confidence);
  const latest = latestEvent(incident.events);
  const { setPanelOpen } = useLayout();
  const error = incident.error_name ?? (incident.error_summary.split("\n")[0] ?? "").trim();

  useHeadingFocus(heading, true);
  usePageTitle(`${stepLabel(incident)} failed · ${incident.dag_id}`);

  const queuedAction = (action: Queued) => {
    if (action === "approve_contract") {
      refresh();
    }
    onQueued(action);
  };

  return (
    <ViewLayout
      latest={
        latest === undefined ? undefined : (
          <p className="latest" data-testid="latest">
            <span>
              Latest: <EventText event={latest} /> · {shortAge(latest.at)}
            </span>
            <button className="linkish" onClick={() => setPanelOpen(true)} type="button">
              Show the timeline
            </button>
          </p>
        )
      }
      panel={
        <>
          <IncidentDetails incident={incident} />
          <Alerts incident={incident} />
          <Timeline events={incident.events} />
        </>
      }
      panelTitle="Incident details"
      testId="incident-page"
    >
      {problem === undefined ? undefined : <ApiProblem error={problem} />}
      <header data-incident-id={incident.id}>
        {embedded ? undefined : (
          <NavLink className="btn sm ghost back-link" place={{ to: "inbox" }}>
            ← Inbox
          </NavLink>
        )}
        <div className="crumb">
          <span>{text.displayName ?? words.product}</span>
          <span aria-hidden="true">/</span>
          <NavLink place={{ pipeline: incident.dag_id, to: "pipelines" }}>{incident.dag_id}</NavLink>
        </div>
        <div className="page-h">
          <h1 ref={heading} tabIndex={-1}>
            <span className="mono" style={{ fontWeight: 500 }}>
              {stepLabel(incident)}
            </span>{" "}
            failed
          </h1>
          <div className="pills">
            <StatusPill status={incident.status} />
            <CategoryChip category={incident.category} />
          </div>
        </div>
        <div className="meta">
          <span title={exactTime(incident.created_at)}>Detected {relativeTime(incident.created_at)}</span>
          <span>{foundBy(incident.source, words)}</span>
          <span>
            {words.attempt.charAt(0).toUpperCase() + words.attempt.slice(1)} {incident.try_number}
          </span>
          {incident.occurrences > 1 ? <span>Seen {incident.occurrences} times</span> : undefined}
          {error.length > 0 ? <span className="mono">{error}</span> : undefined}
        </div>
        <p className="decided" data-testid="decided-by">
          <strong>{decided.lead}</strong>: <RichText text={decided.detail} />
        </p>
      </header>
      {contract?.pending_approval === true ? (
        <Notice testId="approval-queued">
          <p>
            The approval of <span className="mono">{contract.path}</span> is queued; dag-healer's worker
            applies it within a few seconds.
          </p>
        </Notice>
      ) : undefined}
      <PrimaryCard
        contract={contract}
        incident={incident}
        onQueued={queuedAction}
        queued={queued}
        setup={setup}
      />
      <WhatHappened incident={incident} />
      {showsContractCheck(incident, contract) ? (
        <ContractCheck contract={contract} file={file} incident={incident} />
      ) : undefined}
      <SampleCard incident={incident} />
      <FixCard incident={incident} setup={setup} />
      <HowDecided incident={incident} />
      <ErrorDetails incident={incident} />
    </ViewLayout>
  );
};

/** The incident page of an incident id or of a task instance's latest incident. */
export const IncidentPage = ({ source }: { readonly source: IncidentSource }) => {
  const { words } = useUiText();
  const announce = useAnnounce();
  const { commandMs, refreshMs } = usePollingIntervals();
  const [queued, setQueued] = useState<{ action: Queued; data: IncidentDetail | null | undefined }>();
  const resource = usePolledResource<IncidentDetail | null>(
    sourceKey(source),
    (signal) => load(source, signal),
    (data) =>
      data !== null &&
      data !== undefined &&
      (data.pending_commands.length > 0 || groupOf(data.status) === "progress")
        ? commandMs
        : refreshMs,
  );
  const incident = resource.data;
  const pending = useRef<{ count: number; status?: string }>({ count: 0 });

  // A fresh answer after an action replaces the local "queued" state with the server's.
  useEffect(() => {
    if (queued !== undefined && resource.data !== queued.data) {
      setQueued(undefined);
    }
  }, [queued, resource.data]);

  // Say when the worker has applied a queued action.
  useEffect(() => {
    if (incident === null || incident === undefined) {
      return;
    }
    const before = pending.current;

    if (before.count > 0 && incident.pending_commands.length === 0) {
      announce(
        `dag-healer applied the decision: ${incident.dag_id} / ${incident.task_id} is now ${statusMeta(incident.status).label}.`,
      );
    }
    pending.current = { count: incident.pending_commands.length, status: incident.status };
  }, [announce, incident]);

  if (resource.loading) {
    return (
      <ViewLayout panel={<Loading />} panelTitle="Incident details" testId="incident-page">
        <Loading what="Loading the incident…" />
      </ViewLayout>
    );
  }
  if (incident === null || (incident === undefined && resource.error?.status === 404)) {
    return (
      <ViewLayout panelTitle="Incident details" testId="incident-page">
        <h1 className="sr-only">Incident not found</h1>
        <Notice>
          <p>
            {"incidentId" in source
              ? "This incident does not exist, or it belongs to another orchestrator instance."
              : `${noStepInstanceIncidentMessage(words)} dag-healer records one when the ${words.step} fails.`}
          </p>
          <p>
            <NavLink place={{ to: "inbox" }}>Go to the inbox</NavLink>
          </p>
        </Notice>
      </ViewLayout>
    );
  }
  if (incident === undefined) {
    return (
      <ViewLayout panelTitle="Incident details" testId="incident-page">
        {resource.error === undefined ? undefined : <ApiProblem error={resource.error} />}
      </ViewLayout>
    );
  }

  return (
    <IncidentView
      incident={incident}
      onQueued={(action) => {
        announce(
          action === "approve_contract"
            ? "Queued: approve the contract."
            : "Queued. dag-healer's worker applies it shortly.",
        );
        if (action !== "approve_contract") {
          setQueued({ action, data: resource.data });
        }
        resource.refresh();
      }}
      problem={resource.error !== undefined && hasSessionExpired(resource.error) ? resource.error : undefined}
      queued={queued?.action}
    />
  );
};
