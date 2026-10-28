import { ErrorBoundary } from "src/components/ErrorBoundary";
import { ConsoleShell, EmbeddedView } from "src/console/Console";
import { type ContextSourceProps, resolveContext } from "src/context/pluginContext";
import { DEFAULT_UI_TEXT, type UiText, UiTextContext } from "src/vocabulary";

/** Which Airflow destination the component is mounted in (one per react_apps entry). */
export type HealerMode = "dag" | "nav" | "task_instance";

export type AppProps = ContextSourceProps & {
  readonly mode: HealerMode;
  /** Sign out of the standalone host (its token backend); the Airflow plugin passes none. */
  readonly onSignOut?: () => void;
  /** Words and host of the views; without it they print today's Airflow texts (the plugin). */
  readonly text?: UiText;
  /** The signed-in user, shown by the standalone host. */
  readonly user?: string;
};

/**
 * Root of every dag-healer view; `mode` picks the view, the rest are the host props.
 *
 * The Healer page, and every page of `dag-healer serve`, is the console: a sidebar with the inbox,
 * the work area and a side panel. The Airflow DAG and task-instance tabs embed one of its views (a
 * pipeline, an incident) in Airflow's own page.
 */
export const App = ({ mode, onSignOut, text, user, ...props }: AppProps) => {
  const uiText = text ?? DEFAULT_UI_TEXT;
  const shell = mode === "nav" || uiText.host === "standalone";

  return (
    <UiTextContext.Provider value={uiText}>
      <ErrorBoundary>
        {shell ? (
          <ConsoleShell onSignOut={onSignOut} user={user} />
        ) : (
          <EmbeddedView context={resolveContext(props)} mode={mode} />
        )}
      </ErrorBoundary>
    </UiTextContext.Provider>
  );
};
