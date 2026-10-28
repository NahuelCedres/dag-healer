/**
 * The console: the shell (sidebar, work area, side panel) of the Healer page in Airflow and of every
 * page of `dag-healer serve`, and the embedded views of the Airflow DAG and task-instance tabs.
 */
import { type MouseEvent, useEffect, useMemo, useRef, useState } from "react";

import { getSetup, listDags, listIncidents } from "src/api";
import type { HealerContext } from "src/context/pluginContext";
import { usePolledResource, usePollingIntervals } from "src/hooks/usePolledResource";
import { useUiText } from "src/vocabulary";

import { Frame } from "./Frame";
import { Inbox } from "./Inbox";
import { IncidentPage } from "./IncidentPage";
import { LayoutContext, MAIN_ID } from "./Layout";
import { Pipelines } from "./Pipelines";
import { Sidebar } from "./Sidebar";
import { ConsoleDataContext, INCIDENT_LIMIT } from "./data";
import { MoveContext } from "./focus";
import { NavContext } from "./nav";
import { usePrefs, useTheme } from "./prefs";
import { type ConsoleRoute, useConsoleRoute } from "./route";
import { Notice } from "./ui";

const SETUP_REFRESH_MS = 60_000;

const View = ({ route }: { readonly route: ConsoleRoute }) => {
  switch (route.view) {
    case "incident":
      return (
        <IncidentPage
          key={`${route.dagId}/${route.incidentId}`}
          source={{ dagId: route.dagId, incidentId: route.incidentId }}
        />
      );
    case "task_instance":
      return (
        <IncidentPage
          key={`${route.dagId}/${route.runId}/${route.taskId}/${route.mapIndex}`}
          source={{ dagId: route.dagId, mapIndex: route.mapIndex, runId: route.runId, taskId: route.taskId }}
        />
      );
    case "pipelines":
      return <Pipelines pipeline={route.pipeline} />;
    default:
      return <Inbox filters={route.filters} group={route.group} key={route.group ?? "all"} />;
  }
};

const skipToContent = (event: MouseEvent<HTMLAnchorElement>) => {
  event.preventDefault();
  document.getElementById(MAIN_ID)?.focus();
};

export interface ConsoleShellProps {
  readonly onSignOut?: () => void;
  readonly user?: string;
}

export const ConsoleShell = ({ onSignOut, user }: ConsoleShellProps) => {
  const text = useUiText();
  const standalone = text.host === "standalone";
  const [prefs, setPrefs] = usePrefs();
  const theme = useTheme(text.host, prefs.theme);
  const { navigate, route } = useConsoleRoute();
  const { refreshMs } = usePollingIntervals();
  const incidents = usePolledResource(
    "incidents",
    (signal) => listIncidents({ limit: INCIDENT_LIMIT }, signal),
    refreshMs,
  );
  const dags = usePolledResource("dags", listDags, refreshMs * 3);
  const setup = usePolledResource("setup", getSetup, SETUP_REFRESH_MS);
  const [moves, setMoves] = useState(0);
  const firstRoute = useRef(true);

  // Every move after the first render puts focus on the new view's heading (see focus.ts).
  useEffect(() => {
    if (firstRoute.current) {
      firstRoute.current = false;

      return;
    }
    setMoves((count) => count + 1);
  }, [route]);

  // On `dag-healer serve` the console is the whole page: paint the page in the theme.
  useEffect(() => {
    if (!standalone) {
      return undefined;
    }
    document.documentElement.dataset.dhcTheme = theme;

    return () => {
      delete document.documentElement.dataset.dhcTheme;
    };
  }, [standalone, theme]);

  const data = useMemo(() => ({ dags, incidents, setup }), [dags, incidents, setup]);
  const nav = useMemo(() => ({ go: navigate }), [navigate]);
  const layout = useMemo(
    () => ({
      embedded: false,
      panelOpen: prefs.panel,
      setPanelOpen: (open: boolean) => setPrefs({ panel: open }),
      shortcuts: prefs.shortcuts,
    }),
    [prefs.panel, prefs.shortcuts, setPrefs],
  );

  return (
    <Frame className="shell-root" testId="console" theme={theme}>
      <a className="skip" href={`#${MAIN_ID}`} onClick={skipToContent}>
        Skip to content
      </a>
      <ConsoleDataContext.Provider value={data}>
        <NavContext.Provider value={nav}>
          <LayoutContext.Provider value={layout}>
            <MoveContext.Provider value={moves}>
              <div className="shell">
                <Sidebar
                  onPrefs={setPrefs}
                  onSignOut={onSignOut}
                  prefs={prefs}
                  route={route}
                  themeChoice={standalone}
                  user={user}
                />
                <View route={route} />
              </div>
            </MoveContext.Provider>
          </LayoutContext.Provider>
        </NavContext.Provider>
      </ConsoleDataContext.Provider>
    </Frame>
  );
};

const EMBEDDED_LAYOUT = { embedded: true, panelOpen: true, setPanelOpen: () => undefined, shortcuts: false };

/** The Airflow DAG tab (a pipeline) or task-instance tab (its incident), inside Airflow's page. */
export const EmbeddedView = ({
  context,
  mode,
}: {
  readonly context: HealerContext;
  readonly mode: "dag" | "task_instance";
}) => {
  const { host, words } = useUiText();
  const theme = useTheme(host, "system");
  const { dagId, mapIndex, runId, taskId } = context;

  let body;

  if (mode === "dag") {
    body =
      dagId === undefined ? (
        <Notice>
          <p>Open a {words.pipeline} to see its dag-healer incidents.</p>
        </Notice>
      ) : (
        <Pipelines pipeline={dagId} />
      );
  } else {
    body =
      dagId === undefined || runId === undefined || taskId === undefined ? (
        <Notice>
          <p>Open a {words.step} instance to see its dag-healer incident.</p>
        </Notice>
      ) : (
        <IncidentPage
          source={{ dagId, mapIndex: mapIndex === undefined ? -1 : Number(mapIndex), runId, taskId }}
        />
      );
  }

  return (
    <Frame className="embedded" testId={mode === "dag" ? "dag-panel" : "task-instance-panel"} theme={theme}>
      <LayoutContext.Provider value={EMBEDDED_LAYOUT}>{body}</LayoutContext.Provider>
    </Frame>
  );
};
