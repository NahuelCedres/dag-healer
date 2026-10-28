/**
 * Links between the console's places. In the console shell they move inside the page (the URL
 * changes, Back works, a new tab opens with a modified click); in the Airflow DAG and task tabs they
 * go to the matching Airflow pages, which host the Healer views.
 */
import { createContext, type MouseEvent, type ReactNode, useContext } from "react";

import type { IncidentSummary } from "src/api";
import { dagHref, healerPageHref, taskInstanceHref } from "src/links";

import type { InboxGroup } from "./labels";
import { type ConsoleRoute, isPlainClick, routeHref } from "./route";

type IncidentKey = Pick<IncidentSummary, "dag_id" | "id" | "map_index" | "run_id" | "task_id">;

export type Place =
  | { readonly group?: InboxGroup; readonly to: "inbox" }
  | { readonly incident: IncidentKey; readonly to: "incident" }
  | { readonly pipeline?: string; readonly to: "pipelines" };

export interface ConsoleNav {
  /** Move inside the page; undefined in the embedded views, whose links load Airflow pages. */
  readonly go?: (route: ConsoleRoute) => void;
}

export const NavContext = createContext<ConsoleNav>({});

export const routeOf = (place: Place): ConsoleRoute => {
  switch (place.to) {
    case "incident":
      return { dagId: place.incident.dag_id, incidentId: place.incident.id, view: "incident" };
    case "pipelines":
      return { pipeline: place.pipeline, view: "pipelines" };
    default:
      return { group: place.group, view: "inbox" };
  }
};

/** The href of a place: a console URL in the shell, the Airflow page of the tabs otherwise. */
export const placeHref = (place: Place, inShell: boolean): string => {
  if (inShell) {
    return routeHref(routeOf(place));
  }
  switch (place.to) {
    case "incident":
      return taskInstanceHref(
        place.incident.dag_id,
        place.incident.run_id,
        place.incident.task_id,
        place.incident.map_index,
      );
    case "pipelines":
      return place.pipeline === undefined ? routeHref(routeOf(place)) : dagHref(place.pipeline);
    default:
      return healerPageHref();
  }
};

export const NavLink = ({
  ariaCurrent,
  children,
  className,
  place,
  testId,
  title,
}: {
  readonly ariaCurrent?: "page" | undefined;
  readonly children: ReactNode;
  readonly className?: string;
  readonly place: Place;
  readonly testId?: string;
  readonly title?: string;
}) => {
  const { go } = useContext(NavContext);
  const onClick = (event: MouseEvent<HTMLAnchorElement>) => {
    if (go !== undefined && isPlainClick(event)) {
      event.preventDefault();
      go(routeOf(place));
    }
  };

  return (
    <a
      aria-current={ariaCurrent}
      className={className}
      data-testid={testId}
      href={placeHref(place, go !== undefined)}
      onClick={onClick}
      title={title}
    >
      {children}
    </a>
  );
};
