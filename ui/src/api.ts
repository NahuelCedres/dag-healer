/**
 * Minimal client for the dag-healer API mounted by the plugin under `/healer` on the Airflow API server.
 *
 * Requests are same-origin: the Airflow `_token` cookie (HttpOnly) is sent by the browser on its own,
 * so the UI never handles credentials. URLs are resolved against `document.baseURI`, which the Airflow
 * UI sets with `<base href>`, so deployments under a sub-path keep working.
 */

export const API_PREFIX = "healer/api/v1/";

/** Header required by the healer API on every request (CSRF guard for state-changing calls). */
export const REQUESTED_WITH = "dag-healer";

/** Error raised for any failed API call; `status` is the HTTP status, or 0 for network failures. */
export class ApiError extends Error {
  readonly detail: unknown;
  readonly path: string;
  readonly status: number;

  constructor(status: number, message: string, path: string, detail?: unknown) {
    super(message);
    this.name = "ApiError";
    this.status = status;
    this.path = path;
    this.detail = detail;
  }
}

/** 401: the Airflow session cookie is missing or expired. */
export class UnauthorizedError extends ApiError {
  constructor(path: string, detail?: unknown) {
    super(401, "Your Airflow session has expired. Reload the page to sign in again.", path, detail);
    this.name = "UnauthorizedError";
  }
}

/** 403: the current user lacks the Airflow permission required by the endpoint. */
export class ForbiddenError extends ApiError {
  constructor(path: string, message: string, detail?: unknown) {
    super(403, message, path, detail);
    this.name = "ForbiddenError";
  }
}

/** 404: unknown incident, DAG or contract. */
export class NotFoundError extends ApiError {
  constructor(path: string, message: string, detail?: unknown) {
    super(404, message, path, detail);
    this.name = "NotFoundError";
  }
}

/** The request never reached the server (offline, DNS, CORS, aborted...). */
export class NetworkError extends ApiError {
  constructor(path: string, cause: unknown) {
    super(0, `Network error while calling ${path}: ${describe(cause)}`, path, cause);
    this.name = "NetworkError";
  }
}

export const isApiError = (error: unknown): error is ApiError => error instanceof ApiError;

function describe(value: unknown): string {
  if (value instanceof Error) {
    return value.message;
  }

  return typeof value === "string" ? value : (JSON.stringify(value) ?? String(value));
}

/** Build the absolute URL of an API path such as `"incidents?limit=20"` or `"dags/x/contract"`. */
export const apiUrl = (path: string, baseUri: string = document.baseURI): URL =>
  // A leading slash would resolve against the origin root and escape the deployment base path.
  new URL(API_PREFIX + path.replace(/^\/+/u, ""), baseUri);

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

const toApiError = async (response: Response, path: string): Promise<ApiError> => {
  const body = await readBody(response);
  const detail = body !== null && typeof body === "object" && "detail" in body ? body.detail : body;
  const message = messageFromDetail(response.status, detail);

  switch (response.status) {
    case 401:
      return new UnauthorizedError(path, detail);
    case 403:
      return new ForbiddenError(path, message, detail);
    case 404:
      return new NotFoundError(path, message, detail);
    default:
      return new ApiError(response.status, message, path, detail);
  }
};

/**
 * Call the healer API and return the parsed JSON (or text) body.
 *
 * Always sends `credentials: "same-origin"` and `X-Requested-With: dag-healer`; throws an
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

  let response: Response;

  try {
    response = await fetch(apiUrl(path), { ...init, credentials: "same-origin", headers });
  } catch (error) {
    throw new NetworkError(path, error);
  }

  if (!response.ok) {
    throw await toApiError(response, path);
  }

  return (await readBody(response)) as T;
};

/** POST a JSON body to the healer API. */
export const apiPost = <T = unknown>(path: string, body: unknown = {}): Promise<T> =>
  apiFetch<T>(path, { body: JSON.stringify(body), method: "POST" });
