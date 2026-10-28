/**
 * The data the console shares between its views: the incident list, the pipelines and `GET /setup`,
 * polled once by the shell. The embedded views of the Airflow tabs load what they need themselves.
 */
import { createContext, useContext } from "react";

import {
  type ApiError,
  type ContractView,
  type DagListResponse,
  getContract,
  getContractFile,
  getSetup,
  type IncidentListResponse,
  type SetupResponse,
} from "src/api";
import { type Resource, usePolledResource, usePollingIntervals } from "src/hooks/usePolledResource";

/** Incidents loaded for the inbox and the sidebar (the API's maximum page). */
export const INCIDENT_LIMIT = 500;

const SETUP_REFRESH_MS = 60_000;

export interface ConsoleData {
  readonly dags?: Resource<DagListResponse>;
  readonly incidents?: Resource<IncidentListResponse>;
  readonly setup?: Resource<SetupResponse>;
}

export const ConsoleDataContext = createContext<ConsoleData>({});

export const useConsoleData = (): ConsoleData => useContext(ConsoleDataContext);

/** `GET /setup`, from the shell when it loaded it, else loaded here (the embedded views). */
export const useSetup = (): SetupResponse | undefined => {
  const shared = useConsoleData().setup;
  const own = usePolledResource<SetupResponse>(
    shared === undefined ? "setup" : null,
    getSetup,
    SETUP_REFRESH_MS,
  );

  return (shared ?? own).data;
};

/** The contract of a pipeline: the view (null when it has none) and its file, as written. */
export interface PipelineContract {
  /** null: the pipeline has no contract; undefined: not known yet (or not readable). */
  readonly contract: ContractView | null | undefined;
  readonly error?: ApiError;
  readonly file?: string;
  readonly refresh: () => void;
}

/** The contract of `dagId`, polled faster while its approval is queued. */
export const usePipelineContract = (dagId: string | undefined): PipelineContract => {
  const { commandMs, refreshMs } = usePollingIntervals();
  const view = usePolledResource<ContractView | null>(
    dagId === undefined ? null : `contract:${dagId}`,
    async (signal) => {
      try {
        return await getContract(dagId ?? "", signal);
      } catch (caught) {
        if ((caught as ApiError).status === 404) {
          return null;
        }
        throw caught;
      }
    },
    (data) => (data?.pending_approval === true ? commandMs : refreshMs * 3),
  );
  const file = usePolledResource<string | null>(
    dagId === undefined || view.data === null || view.data === undefined
      ? null
      : `contract-file:${dagId}:${view.data.status}:${view.data.approved_at ?? ""}`,
    async (signal) => {
      try {
        const text = await getContractFile(dagId ?? "", signal);

        return typeof text === "string" ? text : null;
      } catch {
        return null;
      }
    },
    0,
  );
  const contract = view.error !== undefined && view.data === undefined ? undefined : view.data;

  return { contract, error: view.error, file: file.data ?? undefined, refresh: view.refresh };
};
