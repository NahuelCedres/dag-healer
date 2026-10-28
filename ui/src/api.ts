/**
 * Typed client for the dag-healer API mounted under `/healer`: by the plugin on the Airflow API server,
 * or by `dag-healer serve` (the standalone host).
 *
 * Requests are same-origin. In the Airflow plugin the `_token` cookie (HttpOnly) is sent by the
 * browser on its own, so the UI never handles credentials, and the plugin never installs an
 * authorization provider: Airflow reads a Bearer header before its cookie, so a stray header would
 * break its session. Only the standalone entry of the token backend installs one (see
 * {@link setAuthorizationProvider}). URLs are resolved against `document.baseURI`, which both hosts
 * set with `<base href>`, so deployments under a sub-path keep working.
 *
 * The types below mirror the JSON shapes of the `/healer/api/v1` endpoints. The optional fields are
 * the additive ones: the server leaves them out for the default Airflow instance.
 */

export const API_PREFIX = "healer/api/v1/";

/** Header required by the healer API on every request (CSRF guard for state-changing calls). */
export const REQUESTED_WITH = "dag-healer";

// --------------------------------------------------------------------------------------------- //
// Shapes
// --------------------------------------------------------------------------------------------- //

export type Category = "ambiguous" | "code" | "data";

export type IncidentStatus =
  | "classified"
  | "error"
  | "ignored"
  | "new"
  | "repair_failed"
  | "repaired"
  | "repairing"
  | "reported"
  | "reporting"
  | "resumed"
  | "triaging"
  | "waiting_human";

/** Statuses of incidents that still need work (by the worker or by a human). */
export const OPEN_STATUSES: ReadonlyArray<IncidentStatus> = [
  "new",
  "triaging",
  "classified",
  "reporting",
  "repairing",
  "waiting_human",
  "resumed",
];

export const ALL_STATUSES: ReadonlyArray<IncidentStatus> = [
  ...OPEN_STATUSES,
  "reported",
  "repaired",
  "repair_failed",
  "ignored",
  "error",
];

export const CATEGORIES: ReadonlyArray<Category> = ["data", "code", "ambiguous"];

export type IncidentAction = "ignore" | "resolve_code" | "resolve_data" | "retry_repair" | "retry_task";

export type Resolution = "code" | "data" | "ignore";

export type ContractStatus = "approved" | "draft" | "missing" | "stale";

export interface IncidentSummary {
  category: Category | null;
  confidence: number | null;
  created_at: string;
  dag_id: string;
  /** Not part of the summary contract; used when a server includes it. */
  diagnosis?: string | null;
  /** Native error name reported by the orchestrator (left out when unknown). */
  error_name?: string | null;
  error_summary: string;
  has_pr_md: boolean;
  id: string;
  /** Only when the server has native links to show (never for Airflow). */
  links?: IncidentLinks | null;
  map_index: number;
  occurrences: number;
  /** Orchestrator instance of the incident (left out for the default Airflow instance). */
  orchestrator?: string;
  /** The action a person queued that the worker has not applied yet (left out when none). */
  pending_command?: PendingCommand | null;
  pr_url: string | null;
  run_id: string;
  source: string;
  status: IncidentStatus;
  task_id: string;
  try_number: number;
  updated_at: string;
}

/** One broken contract rule (`dag_healer.contracts.schema.Violation`). */
export interface Violation {
  actual?: unknown;
  column?: string | null;
  dataset?: string;
  expected?: unknown;
  message?: string;
  row_indexes?: Array<number>;
  rule: string;
}

/** The rule picked as the main cause of a data incident (a violation, or a looser object). */
export type ViolatedRule = Partial<Violation> & Record<string, unknown>;

/** Masked rows of a dataset; `rows` are records (column -> value) or positional arrays. */
export interface MaskedSample {
  columns: Array<string>;
  dataset: string;
  masked: true;
  rows: Array<Array<unknown> | Record<string, unknown>>;
}

export interface IncidentEvent {
  actor: string;
  at: string;
  data: unknown;
  kind: string;
  message: string;
}

export interface AlertRecord {
  channel: string;
  error: string | null;
  recipients: Array<string> | null;
  sent_at: string;
  status: string;
  subject: string;
}

export interface PendingCommand {
  /** The action as the person chose it (`resolve_data`, `retry_task`...); left out when unknown. */
  action?: string | null;
  actor: string;
  created_at: string;
  id: number | string;
  kind: string;
}

export interface IncidentLinks {
  dag?: string | null;
  healer?: string | null;
  /** Labelled links into the orchestrator's own pages (`{"AWS console": url}`); empty for Airflow. */
  native?: Record<string, string>;
  task_instance?: string | null;
}

export interface IncidentDetail extends IncidentSummary {
  /** Texts to show next to offered actions (`{"retry_task": [...]}`). */
  action_warnings?: Record<string, Array<string>>;
  alerts: Array<AlertRecord>;
  allowed_actions: Array<IncidentAction>;
  attempts: number;
  can_edit: boolean;
  diagnosis: string | null;
  diff: string | null;
  events: Array<IncidentEvent>;
  evidence: Array<string>;
  links: IncidentLinks;
  pending_commands: Array<PendingCommand>;
  pr_md_url: string | null;
  pytest_output: string | null;
  resolution: string | null;
  resolved_by: string | null;
  sample: MaskedSample | null;
  test_code: string | null;
  test_path: string | null;
  tokens_used: number;
  violated_rule: ViolatedRule | null;
  violations: Array<Violation>;
}

export interface DagSummary {
  contract_status: ContractStatus;
  dag_id: string;
  last_incident: IncidentSummary | null;
  monitored: boolean;
  open_incidents: number;
}

export interface ColumnRuleView {
  allowed_values: Array<string> | null;
  dtype: string;
  max: number | null;
  min: number | null;
  name: string;
  note: string | null;
  nullable: boolean;
  pii: boolean;
}

export interface ContractDatasetView {
  /** Whether columns the contract does not list are accepted (left out when true, the default). */
  allow_extra_columns?: boolean;
  columns: Array<ColumnRuleView>;
  locator: { path: string; type: string };
  name: string;
  row_count: { max: number | null; min: number | null };
  task_id: string | null;
}

export interface ContractView {
  approved_at: string | null;
  approved_by: string | null;
  can_approve: boolean;
  dag_id: string;
  datasets: Array<ContractDatasetView>;
  path: string;
  pending_approval: boolean;
  stale: boolean;
  status: ContractStatus;
}

/** `/me` → `orchestrator`: the orchestrator of this dag-healer (absent for the default Airflow instance). */
export interface MeOrchestrator {
  capabilities: Record<string, unknown>;
  display_name: string;
  instance: string;
  type: string;
  /** The keys of `dag_healer.core.vocab.Vocabulary` (`product`, `pipeline`, `step`, `retry_action`...). */
  vocabulary: Record<string, string>;
}

export interface MeResponse {
  base_url: string | null;
  /** Where the views are served (left out inside Airflow). */
  host?: "airflow_plugin" | "standalone";
  orchestrator?: MeOrchestrator | null;
  user: Record<string, unknown> | string | null;
}

/** `GET /setup`: what the human actions lead to in this deployment (no secret). */
export interface SetupResponse {
  alerts: {
    /** Recipients of the email channel (`notify.email.to`); empty without email. */
    email: Array<string>;
    /** The incident events that send alerts (`notify.events`). */
    events: Array<string>;
  };
  contracts_dir: string;
  /** `classification.min_confidence`: the confidence the model needs to decide on its own. */
  min_confidence: number;
  repairs: {
    allowed_paths: Array<string>;
    base_branch: string;
    max_diff_files: number;
    max_diff_lines: number;
    pr_base: string;
    /** `github.repo` (owner/name), when set. */
    repository: string | null;
    skip_ci: boolean;
    tests_dir: string | null;
    /** Whether this process has a GitHub token (the token itself is never sent). */
    token_configured: boolean;
  };
}

export interface IncidentListResponse {
  incidents: Array<IncidentSummary>;
  total: number;
}

export interface DagListResponse {
  dags: Array<DagSummary>;
}

export interface ContractListResponse {
  contracts: Array<ContractView>;
}

export interface DagIncidentsResponse {
  can_edit: boolean;
  contract: ContractView | null;
  incidents: Array<IncidentSummary>;
  monitored: boolean;
}

export interface TaskInstanceIncidentResponse {
  incident: IncidentDetail | null;
}

/** Answer of every state-changing endpoint: the queued command the worker will apply. */
export interface CommandAccepted {
  command_id: number | string;
  status: string;
}

export interface TaskFailedAccepted {
  accepted: boolean;
  pending_event_id: number | string;
}

// --------------------------------------------------------------------------------------------- //
// Errors
// --------------------------------------------------------------------------------------------- //

/**
 * Error raised for any failed API call.
 *
 * `status` is the HTTP status (0 for network failures) and `detail` the human-readable message taken
 * from the FastAPI `{"detail": ...}` body; `body` keeps the raw parsed body.
 */
export class ApiError extends Error {
  readonly body: unknown;
  readonly detail: string;
  readonly path: string;
  readonly status: number;

  constructor(status: number, detail: string, path: string, body?: unknown) {
    super(detail);
    this.name = "ApiError";
    this.status = status;
    this.detail = detail;
    this.path = path;
    this.body = body;
  }
}

/** 401: the session is missing or expired (the Airflow cookie, or the standalone host's credentials). */
export class UnauthorizedError extends ApiError {
  constructor(path: string, detail: string, body?: unknown) {
    super(401, detail, path, body);
    this.name = "UnauthorizedError";
  }
}

/** 403: the current user lacks the Airflow permission required by the endpoint. */
export class ForbiddenError extends ApiError {
  constructor(path: string, detail: string, body?: unknown) {
    super(403, detail, path, body);
    this.name = "ForbiddenError";
  }
}

/** 404: unknown incident, DAG or contract (or an older plugin without that route). */
export class NotFoundError extends ApiError {
  constructor(path: string, detail: string, body?: unknown) {
    super(404, detail, path, body);
    this.name = "NotFoundError";
  }
}

/** 409: the action is not allowed for the current status of the incident. */
export class ConflictError extends ApiError {
  constructor(path: string, detail: string, body?: unknown) {
    super(409, detail, path, body);
    this.name = "ConflictError";
  }
}

/** The request never reached the server (offline, DNS, aborted...). */
export class NetworkError extends ApiError {
  constructor(path: string, cause: unknown) {
    super(0, `Network error while calling ${path}: ${describe(cause)}`, path, cause);
    this.name = "NetworkError";
  }
}

export const isApiError = (error: unknown): error is ApiError => error instanceof ApiError;

/** True when one of the errors is a 401 (the session expired). */
export const hasSessionExpired = (...errors: Array<unknown>): boolean =>
  errors.some((error) => isApiError(error) && error.status === 401);

/** Normalize anything thrown by a loader into an {@link ApiError}. */
export const toApiError = (error: unknown, path = ""): ApiError =>
  isApiError(error) ? error : new ApiError(0, describe(error), path, error);

function describe(value: unknown): string {
  if (value instanceof Error) {
    return value.message;
  }

  return typeof value === "string" ? value : (JSON.stringify(value) ?? String(value));
}

/** Human-readable message from a FastAPI error body (`{"detail": "..."}` or validation errors). */
const messageFromDetail = (status: number, detail: unknown): string => {
  if (typeof detail === "string" && detail.length > 0) {
    return detail;
  }
  if (Array.isArray(detail) && detail.length > 0) {
    return detail
      .map((item: unknown) =>
        item !== null && typeof item === "object" && "msg" in item ? String(item.msg) : describe(item),
      )
      .join("; ");
  }

  return `Request failed with status ${status}`;
};

const readBody = async (response: Response): Promise<unknown> => {
  const text = await response.text();

  if (text.length === 0) {
    return undefined;
  }
  const contentType = response.headers.get("content-type") ?? "";

  if (contentType.includes("json")) {
    try {
      return JSON.parse(text) as unknown;
    } catch {
      return text;
    }
  }

  return text;
};

const errorFromResponse = async (response: Response, path: string): Promise<ApiError> => {
  const body = await readBody(response);
  const rawDetail = body !== null && typeof body === "object" && "detail" in body ? body.detail : body;
  const detail = messageFromDetail(response.status, rawDetail);

  switch (response.status) {
    case 401:
      return new UnauthorizedError(path, detail, body);
    case 403:
      return new ForbiddenError(path, detail, body);
    case 404:
      return new NotFoundError(path, detail, body);
    case 409:
      return new ConflictError(path, detail, body);
    default:
      return new ApiError(response.status, detail, path, body);
  }
};

// --------------------------------------------------------------------------------------------- //
// Transport
// --------------------------------------------------------------------------------------------- //

/** Build the absolute URL of an API path such as `"incidents?limit=20"` or `"dags/x/contract"`. */
export const apiUrl = (path: string, baseUri: string = document.baseURI): URL =>
  // A leading slash would resolve against the origin root and escape the deployment base path.
  new URL(API_PREFIX + path.replace(/^\/+/u, ""), baseUri);

/** The `Authorization` header value to send (for example `Bearer <token>`), or undefined for none. */
export type AuthorizationProvider = () => string | undefined;

let authorizationProvider: AuthorizationProvider | undefined;

/**
 * Install the provider of the `Authorization` header of every API call (none removes it).
 *
 * Only the standalone entry of the token backend calls this: a browser cannot send a bearer token on
 * its own. The Airflow plugin never does (see the module docstring).
 */
export const setAuthorizationProvider = (provider?: AuthorizationProvider): void => {
  authorizationProvider = provider;
};

/** True when API calls carry an `Authorization` header that a plain link (a new tab) cannot send. */
export const hasAuthorizationProvider = (): boolean => authorizationProvider !== undefined;

/**
 * Call the healer API and return the parsed JSON (or text) body.
 *
 * Always sends `credentials: "same-origin"` and `X-Requested-With: dag-healer`, plus the
 * `Authorization` of the installed provider unless the caller passed one; throws an
 * {@link ApiError} subclass for non-2xx responses and network failures.
 */
export const apiFetch = async <T = unknown>(path: string, init: RequestInit = {}): Promise<T> => {
  const headers = new Headers(init.headers);

  headers.set("X-Requested-With", REQUESTED_WITH);
  if (!headers.has("Accept")) {
    headers.set("Accept", "application/json");
  }
  if (typeof init.body === "string" && !headers.has("Content-Type")) {
    headers.set("Content-Type", "application/json");
  }
  if (authorizationProvider !== undefined && !headers.has("Authorization")) {
    const authorization = authorizationProvider();

    if (authorization !== undefined && authorization.length > 0) {
      headers.set("Authorization", authorization);
    }
  }

  let response: Response;

  try {
    response = await fetch(apiUrl(path), { ...init, credentials: "same-origin", headers });
  } catch (error) {
    throw new NetworkError(path, error);
  }

  if (!response.ok) {
    throw await errorFromResponse(response, path);
  }

  return (await readBody(response)) as T;
};

/** POST a JSON body to the healer API. */
export const apiPost = <T = unknown>(path: string, body: unknown = {}, init: RequestInit = {}): Promise<T> =>
  apiFetch<T>(path, { ...init, body: JSON.stringify(body), method: "POST" });

const seg = (value: string): string => encodeURIComponent(value);

const query = (params: Record<string, number | string | undefined>): string => {
  const search = new URLSearchParams();

  for (const [key, value] of Object.entries(params)) {
    if (value !== undefined && value !== "") {
      search.set(key, String(value));
    }
  }
  const text = search.toString();

  return text.length > 0 ? `?${text}` : "";
};

/** `{note}` when a non-empty note was typed, `{}` otherwise. */
const noteBody = (note?: string): { note?: string } => {
  const trimmed = note?.trim();

  return trimmed === undefined || trimmed.length === 0 ? {} : { note: trimmed };
};

// --------------------------------------------------------------------------------------------- //
// Endpoints
// --------------------------------------------------------------------------------------------- //

/** `GET /me`: the signed-in user, the public base URL and, outside Airflow, the host and orchestrator. */
export const getMe = (signal?: AbortSignal) => apiFetch<MeResponse>("me", { signal });

/** `GET /setup`: alert recipients, how a fix is delivered, its limits and the classification threshold. */
export const getSetup = (signal?: AbortSignal) => apiFetch<SetupResponse>("setup", { signal });

export interface IncidentQuery {
  dagId?: string;
  limit?: number;
  status?: string;
}

/** `GET /incidents`: incidents of the DAGs the user can read, newest first. */
export const listIncidents = ({ dagId, limit, status }: IncidentQuery = {}, signal?: AbortSignal) =>
  apiFetch<IncidentListResponse>(`incidents${query({ dag_id: dagId, limit, status })}`, { signal });

/** `GET /dags`: monitored DAGs with their contract status and last incident. */
export const listDags = (signal?: AbortSignal) => apiFetch<DagListResponse>("dags", { signal });

/** `GET /contracts`: every data contract the user can read. */
export const listContracts = (signal?: AbortSignal) =>
  apiFetch<ContractListResponse>("contracts", { signal });

/** `GET /dags/{dag_id}/incidents`: incidents, contract and monitoring status of one DAG. */
export const getDagIncidents = (dagId: string, signal?: AbortSignal) =>
  apiFetch<DagIncidentsResponse>(`dags/${seg(dagId)}/incidents`, { signal });

/** `GET /dags/{dag_id}/incidents/{incident_id}`. */
export const getIncident = (dagId: string, incidentId: string, signal?: AbortSignal) =>
  apiFetch<IncidentDetail>(`dags/${seg(dagId)}/incidents/${seg(incidentId)}`, { signal });

/** Path of the PR.md of an incident, relative to the API prefix. */
export const prMarkdownPath = (dagId: string, incidentId: string): string =>
  `dags/${seg(dagId)}/incidents/${seg(incidentId)}/pr.md`;

/** `GET /dags/{dag_id}/incidents/{incident_id}/pr.md` as text. */
export const getPrMarkdown = (dagId: string, incidentId: string, signal?: AbortSignal) =>
  apiFetch<string>(prMarkdownPath(dagId, incidentId), {
    headers: { Accept: "text/markdown, text/plain;q=0.9, */*;q=0.1" },
    signal,
  });

export interface TaskInstanceRef {
  mapIndex?: number | string;
  runId: string;
  taskId: string;
}

/** `GET /dags/{dag_id}/task-instance`: the latest incident of a task instance (any try), or null. */
export const getTaskInstanceIncident = (
  dagId: string,
  { mapIndex = -1, runId, taskId }: TaskInstanceRef,
  signal?: AbortSignal,
) =>
  apiFetch<TaskInstanceIncidentResponse>(
    `dags/${seg(dagId)}/task-instance${query({ map_index: mapIndex, run_id: runId, task_id: taskId })}`,
    { signal },
  );

/** `GET /dags/{dag_id}/contract` (404 when the DAG has no contract). */
export const getContract = (dagId: string, signal?: AbortSignal) =>
  apiFetch<ContractView>(`dags/${seg(dagId)}/contract`, { signal });

/** `GET /dags/{dag_id}/contract.yaml`: the contract file as written in the repository (404 without one). */
export const getContractFile = (dagId: string, signal?: AbortSignal) =>
  apiFetch<string>(`dags/${seg(dagId)}/contract.yaml`, {
    headers: { Accept: "application/yaml, text/plain;q=0.9, */*;q=0.1" },
    signal,
  });

/** `POST .../resolve`: a human decides the incident is a data problem, a code bug, or noise. */
export const resolveIncident = (dagId: string, incidentId: string, resolution: Resolution, note?: string) =>
  apiPost<CommandAccepted>(`dags/${seg(dagId)}/incidents/${seg(incidentId)}/resolve`, {
    ...noteBody(note),
    resolution,
  });

/** `POST .../retry-repair`: run the repair agent again. */
export const retryRepair = (dagId: string, incidentId: string, note?: string) =>
  apiPost<CommandAccepted>(`dags/${seg(dagId)}/incidents/${seg(incidentId)}/retry-repair`, noteBody(note));

/** `POST .../retry-task`: clear the failed task instance and its failed downstream tasks in Airflow. */
export const retryTask = (dagId: string, incidentId: string, note?: string) =>
  apiPost<CommandAccepted>(`dags/${seg(dagId)}/incidents/${seg(incidentId)}/retry-task`, noteBody(note));

/** `POST /dags/{dag_id}/contract/approve`. */
export const approveContract = (dagId: string, note?: string) =>
  apiPost<CommandAccepted>(`dags/${seg(dagId)}/contract/approve`, noteBody(note));

/**
 * `POST /events/task-failed` with the ingest token. Used by the listener and by tooling, never by
 * the views (the token must not reach browsers in production).
 */
export const reportTaskFailed = (payload: Record<string, unknown>, token: string) =>
  apiPost<TaskFailedAccepted>("events/task-failed", payload, { headers: { "X-Healer-Token": token } });

/** Queue the command behind one of the action-bar buttons. */
export const runIncidentAction = (
  dagId: string,
  incidentId: string,
  action: IncidentAction,
  note?: string,
): Promise<CommandAccepted> => {
  switch (action) {
    case "ignore":
      return resolveIncident(dagId, incidentId, "ignore", note);
    case "resolve_code":
      return resolveIncident(dagId, incidentId, "code", note);
    case "resolve_data":
      return resolveIncident(dagId, incidentId, "data", note);
    case "retry_repair":
      return retryRepair(dagId, incidentId, note);
    case "retry_task":
      return retryTask(dagId, incidentId, note);
    default:
      return Promise.reject(new ApiError(422, `Unknown action ${String(action)}`, ""));
  }
};
