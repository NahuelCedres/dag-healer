/**
 * Resolution of the Airflow context (DAG, run, task, map index) a plugin view is rendered for.
 *
 * Airflow 3.1 passes `dagId`, `runId`, `taskId` and `mapIndex` as props; 3.3 may also pass `dag`,
 * `dagRun` and `taskInstance` objects. Props are not guaranteed for every destination, so the
 * current location is parsed as a fallback.
 */

export interface HealerContext {
  dagId?: string;
  mapIndex?: string;
  runId?: string;
  taskId?: string;
}

export interface ContextSourceProps {
  dag?: unknown;
  dagId?: string;
  dagRun?: unknown;
  mapIndex?: number | string;
  runId?: string;
  taskId?: string;
  taskInstance?: unknown;
}

const CONTEXT_KEYS = ["dagId", "runId", "taskId", "mapIndex"] as const;

const safeDecode = (segment: string): string => {
  try {
    return decodeURIComponent(segment);
  } catch {
    return segment;
  }
};

const nonEmpty = (value: unknown): string | undefined => {
  if (typeof value === "number" && Number.isFinite(value)) {
    return String(value);
  }

  return typeof value === "string" && value.length > 0 ? value : undefined;
};

const pick = (source: unknown, keys: ReadonlyArray<string>): string | undefined => {
  if (source === null || typeof source !== "object") {
    return undefined;
  }
  const record = source as Record<string, unknown>;

  for (const key of keys) {
    const value = nonEmpty(record[key]);

    if (value !== undefined) {
      return value;
    }
  }

  return undefined;
};

const splitPath = (pathname: string): Array<string> => pathname.split("/").filter((part) => part.length > 0);

/** Number of leading path segments that belong to the deployment base path (from `<base href>`). */
const baseSegmentCount = (segments: Array<string>, baseUri: string | undefined): number => {
  if (baseUri === undefined) {
    return 0;
  }
  let basePath: string;

  try {
    basePath = new URL(baseUri).pathname;
  } catch {
    return 0;
  }
  // Only a directory-like base (trailing slash) is a deployment prefix; a page URL is not.
  if (!basePath.endsWith("/")) {
    return 0;
  }
  const baseSegments = splitPath(basePath);
  const isPrefix = baseSegments.every((part, index) => segments[index] === part);

  return isPrefix ? baseSegments.length : 0;
};

/**
 * Decoded segments of `pathname` after the deployment base path taken from `baseUri` (all of them
 * when `baseUri` is not a directory prefixing the pathname).
 */
export const pathSegmentsAfterBase = (pathname: string, baseUri?: string): Array<string> => {
  const segments = splitPath(pathname).map(safeDecode);

  return segments.slice(baseSegmentCount(segments, baseUri));
};

const parseFrom = (segments: Array<string>, start: number): HealerContext => {
  const dagsIndex = segments.indexOf("dags", start);
  const dagId = dagsIndex === -1 ? undefined : segments[dagsIndex + 1];

  if (dagId === undefined) {
    return {};
  }
  const context: HealerContext = { dagId };
  let cursor = dagsIndex + 2;

  if (segments[cursor] === "runs" && segments[cursor + 1] !== undefined) {
    context.runId = segments[cursor + 1];
    cursor += 2;
  }
  const taskId = segments[cursor] === "tasks" ? segments[cursor + 1] : undefined;

  // "/tasks/group/<group_id>" is a task group view, not a task.
  if (taskId !== undefined && taskId !== "group") {
    context.taskId = taskId;
    cursor += 2;
    const mapIndex = segments[cursor] === "mapped" ? segments[cursor + 1] : undefined;

    if (mapIndex !== undefined && /^-?\d+$/u.test(mapIndex)) {
      context.mapIndex = mapIndex;
    }
  }

  return context;
};

/**
 * Parse `/dags/:dagId[/runs/:runId][/tasks/:taskId[/mapped/:mapIndex]]` out of a pathname.
 *
 * The `dags` segment may appear after any deployment base path (for example `/airflow/dags/...`).
 * When `baseUri` is a directory that prefixes the pathname, the search starts after it so a base
 * path that itself contains a `dags` segment does not confuse the parser.
 */
export const parseAirflowPath = (pathname: string, baseUri?: string): HealerContext => {
  const segments = splitPath(pathname).map(safeDecode);
  const start = baseSegmentCount(segments, baseUri);
  const context = parseFrom(segments, start);

  return context.dagId === undefined && start > 0 ? parseFrom(segments, 0) : context;
};

const compact = (context: HealerContext): HealerContext => {
  const result: HealerContext = {};

  for (const key of CONTEXT_KEYS) {
    const value = nonEmpty(context[key]);

    if (value !== undefined) {
      result[key] = value;
    }
  }

  return result;
};

/** Extract a context from the 3.3+ `dag`, `dagRun` and `taskInstance` objects (REST response shape). */
export const contextFromObjects = (props: ContextSourceProps): HealerContext => {
  const { dag, dagRun, taskInstance } = props;
  const context: HealerContext = {
    dagId:
      pick(taskInstance, ["dag_id", "dagId"]) ??
      pick(dagRun, ["dag_id", "dagId"]) ??
      pick(dag, ["dag_id", "dagId"]),
    mapIndex: pick(taskInstance, ["map_index", "mapIndex"]),
    runId:
      pick(taskInstance, ["dag_run_id", "run_id", "runId"]) ??
      pick(dagRun, ["dag_run_id", "run_id", "runId"]),
    taskId: pick(taskInstance, ["task_id", "taskId"]),
  };

  return compact(context);
};

/** Field-wise merge: earlier layers win, a layer for a different DAG is ignored entirely. */
const mergeLayers = (layers: Array<HealerContext>): HealerContext => {
  const result: HealerContext = {};

  for (const layer of layers) {
    if (result.dagId !== undefined && layer.dagId !== undefined && layer.dagId !== result.dagId) {
      continue;
    }
    for (const key of CONTEXT_KEYS) {
      if (result[key] === undefined && layer[key] !== undefined) {
        result[key] = layer[key];
      }
    }
  }

  return result;
};

const currentLocation = (): { baseUri?: string; pathname: string } => {
  if (typeof window === "undefined") {
    return { pathname: "" };
  }

  return { baseUri: document.baseURI, pathname: window.location.pathname };
};

/**
 * Resolve the context for a plugin view: explicit scalar props first, then the 3.3 objects, then
 * the current URL. Values from a source describing a different DAG are never mixed in.
 */
export const resolveContext = (
  props: ContextSourceProps,
  location: { baseUri?: string; pathname: string } = currentLocation(),
): HealerContext => {
  const fromProps = compact({
    dagId: props.dagId,
    mapIndex: props.mapIndex === undefined ? undefined : String(props.mapIndex),
    runId: props.runId,
    taskId: props.taskId,
  });

  return mergeLayers([
    fromProps,
    contextFromObjects(props),
    parseAirflowPath(location.pathname, location.baseUri),
  ]);
};
