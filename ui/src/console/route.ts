/**
 * Where the console is: the inbox, an incident, or the pipelines, read from the URL so every view
 * can be linked, reloaded and reached with the browser's Back button.
 *
 * The console lives on the Healer page (`plugin/healer`, in both hosts) and keeps its place in the
 * query string: `?dag=<id>&incident=<id>`, `?view=pipelines&pipeline=<id>`, `?group=needs`. On
 * `dag-healer serve` the shell also answers the plugin's other URL shapes, which open the console
 * on a pipeline (`/dags/<d>/plugin/healer-dag`) or on a task instance's incident
 * (`/dags/<d>/runs/<r>/tasks/<t>[/mapped/<i>]/plugin/healer-ti`).
 */
import { useCallback, useEffect, useState } from "react";

import { parseAirflowPath, pathSegmentsAfterBase } from "src/context/pluginContext";
import { healerPageHref, PLUGIN_ROUTES } from "src/links";

import type { InboxGroup } from "./labels";

export type ConsoleRoute =
  | { readonly dagId: string; readonly incidentId: string; readonly view: "incident" }
  | {
      readonly dagId: string;
      readonly mapIndex: number;
      readonly runId: string;
      readonly taskId: string;
      readonly view: "task_instance";
    }
  | { readonly group?: InboxGroup; readonly view: "inbox" }
  | { readonly pipeline?: string; readonly view: "pipelines" };

const GROUPS: ReadonlyArray<InboxGroup> = ["needs", "progress", "done"];

const nonEmpty = (value: string | null): string | undefined =>
  value === null || value.trim().length === 0 ? undefined : value;

/** The route of a location: its query string first, then (outside the Healer page) its path. */
export const parseRoute = (pathname: string, search: string, baseUri?: string): ConsoleRoute => {
  const query = new URLSearchParams(search);
  const dagId = nonEmpty(query.get("dag"));
  const incidentId = nonEmpty(query.get("incident"));

  if (dagId !== undefined && incidentId !== undefined) {
    return { dagId, incidentId, view: "incident" };
  }
  if (query.get("view") === "pipelines") {
    return { pipeline: nonEmpty(query.get("pipeline")), view: "pipelines" };
  }
  const segments = pathSegmentsAfterBase(pathname, baseUri);

  if (segments.at(-2) === "plugin") {
    const context = parseAirflowPath(pathname, baseUri);

    if (segments.at(-1) === PLUGIN_ROUTES.dag && context.dagId !== undefined) {
      return { pipeline: context.dagId, view: "pipelines" };
    }
    if (
      segments.at(-1) === PLUGIN_ROUTES.task_instance &&
      context.dagId !== undefined &&
      context.runId !== undefined &&
      context.taskId !== undefined
    ) {
      return {
        dagId: context.dagId,
        mapIndex: context.mapIndex === undefined ? -1 : Number(context.mapIndex),
        runId: context.runId,
        taskId: context.taskId,
        view: "task_instance",
      };
    }
  }
  const group = query.get("group");

  return { group: GROUPS.find((candidate) => candidate === group), view: "inbox" };
};

/** The URL of a route: the Healer page with the route in its query string. */
export const routeHref = (route: ConsoleRoute, baseUri?: string): string => {
  const url = new URL(healerPageHref(baseUri));

  switch (route.view) {
    case "incident":
      url.searchParams.set("dag", route.dagId);
      url.searchParams.set("incident", route.incidentId);
      break;
    case "pipelines":
      url.searchParams.set("view", "pipelines");
      if (route.pipeline !== undefined) {
        url.searchParams.set("pipeline", route.pipeline);
      }
      break;
    case "task_instance":
      // Only reached from the standalone host's own path; the console links incidents by id.
      return globalThis.location.href;
    default:
      if (route.group !== undefined) {
        url.searchParams.set("group", route.group);
      }
      break;
  }

  return url.href;
};

const currentRoute = (): ConsoleRoute =>
  parseRoute(globalThis.location.pathname, globalThis.location.search, document.baseURI);

/**
 * The route of the current location and `navigate`, which pushes a new history entry (keeping the
 * host's history state, so a router around the plugin sees no change) and follows Back/Forward.
 */
export const useConsoleRoute = (): { navigate: (route: ConsoleRoute) => void; route: ConsoleRoute } => {
  const [route, setRoute] = useState<ConsoleRoute>(currentRoute);

  useEffect(() => {
    const onPop = () => setRoute(currentRoute());

    globalThis.addEventListener("popstate", onPop);

    return () => globalThis.removeEventListener("popstate", onPop);
  }, []);

  const navigate = useCallback((next: ConsoleRoute) => {
    const href = routeHref(next, document.baseURI);

    if (href !== globalThis.location.href) {
      globalThis.history.pushState(globalThis.history.state, "", href);
    }
    setRoute(next.view === "task_instance" ? currentRoute() : next);
  }, []);

  return { navigate, route };
};

/** True for a plain left click, which the console handles itself (others open a new tab). */
export const isPlainClick = (event: {
  altKey: boolean;
  button: number;
  ctrlKey: boolean;
  defaultPrevented: boolean;
  metaKey: boolean;
  shiftKey: boolean;
}): boolean =>
  !event.defaultPrevented &&
  event.button === 0 &&
  !event.metaKey &&
  !event.ctrlKey &&
  !event.shiftKey &&
  !event.altKey;
