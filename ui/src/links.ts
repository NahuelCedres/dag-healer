/**
 * Links to Airflow UI routes (and the PR.md endpoint), resolved against `document.baseURI` so they keep
 * working when Airflow is served under a sub-path. Every path segment is URL-encoded: run ids contain
 * `:` and `+`. The standalone host (`dag-healer serve`) serves the same URL shapes, so the same links
 * work there.
 */
import { apiUrl, prMarkdownPath } from "src/api";

/**
 * Last path segment of each Healer view (the `url_route` of its react_apps entry in
 * `dag_healer.airflow.plugin`). The standalone host serves the same URL shapes.
 */
export const PLUGIN_ROUTES = {
  dag: "healer-dag",
  nav: "healer",
  task_instance: "healer-ti",
} as const;

const seg = (value: string): string => encodeURIComponent(value);

const currentBase = (): string => document.baseURI;

/** Absolute URL of an Airflow UI path such as `"dags/x"` (a leading slash is ignored). */
export const airflowHref = (path: string, baseUri: string = currentBase()): string =>
  new URL(path.replace(/^\/+/u, ""), baseUri).href;

/** The "Healer incident" tab of a task instance (`/mapped/<i>` for mapped tasks). */
export const taskInstanceHref = (
  dagId: string,
  runId: string,
  taskId: string,
  mapIndex: number | string = -1,
  baseUri: string = currentBase(),
): string => {
  const index = Number(mapIndex);
  const mapped = Number.isInteger(index) && index >= 0 ? `/mapped/${index}` : "";

  return airflowHref(
    `dags/${seg(dagId)}/runs/${seg(runId)}/tasks/${seg(taskId)}${mapped}/plugin/${PLUGIN_ROUTES.task_instance}`,
    baseUri,
  );
};

/** The "Healer incidents" tab of a DAG. */
export const dagHref = (dagId: string, baseUri: string = currentBase()): string =>
  airflowHref(`dags/${seg(dagId)}/plugin/${PLUGIN_ROUTES.dag}`, baseUri);

/** The top-level "Healer" page. */
export const healerPageHref = (baseUri: string = currentBase()): string =>
  airflowHref(`plugin/${PLUGIN_ROUTES.nav}`, baseUri);

/** The PR.md of an incident, served by the healer API. */
export const prMarkdownHref = (dagId: string, incidentId: string, baseUri: string = currentBase()): string =>
  apiUrl(prMarkdownPath(dagId, incidentId), baseUri).href;
