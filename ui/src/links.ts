/**
 * Links to Airflow UI routes (and the PR.md endpoint), resolved against `document.baseURI` so they keep
 * working when Airflow is served under a sub-path. Every path segment is URL-encoded: run ids contain
 * `:` and `+`.
 */
import { apiUrl, prMarkdownPath } from "src/api";

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
    `dags/${seg(dagId)}/runs/${seg(runId)}/tasks/${seg(taskId)}${mapped}/plugin/healer-ti`,
    baseUri,
  );
};

/** The "Healer incidents" tab of a DAG. */
export const dagHref = (dagId: string, baseUri: string = currentBase()): string =>
  airflowHref(`dags/${seg(dagId)}/plugin/healer-dag`, baseUri);

/** The top-level "Healer" page. */
export const healerPageHref = (baseUri: string = currentBase()): string =>
  airflowHref("plugin/healer", baseUri);

/** The PR.md of an incident, served by the healer API. */
export const prMarkdownHref = (dagId: string, incidentId: string, baseUri: string = currentBase()): string =>
  apiUrl(prMarkdownPath(dagId, incidentId), baseUri).href;
