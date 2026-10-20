/**
 * Entry point of the dag-healer UI bundle (built as UMD into `main.umd.cjs`).
 *
 * The Airflow host imports the bundle once per URL and then reads `globalThis[react_app.name]`; the
 * react_apps "name" is also the label Airflow shows. So the three components below are registered on
 * globalThis under their visible names while the module evaluates. All three react_apps entries of
 * the plugin share this single bundle.
 */
import type { FC } from "react";

import { App } from "./App";
import { APP_NAMES, LEGACY_APP_NAMES } from "./appNames";

/** Props passed by the Airflow host (3.1+: ids from the route params, as strings). */
export interface PluginComponentProps {
  dag?: unknown;
  dagId?: string;
  dagRun?: unknown;
  mapIndex?: string;
  runId?: string;
  taskId?: string;
  taskInstance?: unknown;
}

/** Top-level "Healer" page (react_apps destination "nav"). */
export const HealerNav: FC<PluginComponentProps> = (props) => <App mode="nav" {...props} />;

/** "Healer incidents" tab of a DAG (react_apps destination "dag"). */
export const HealerDagIncidents: FC<PluginComponentProps> = (props) => <App mode="dag" {...props} />;

/** "Healer incident" tab of a task instance (react_apps destination "task_instance"). */
export const HealerTaskIncident: FC<PluginComponentProps> = (props) => (
  <App mode="task_instance" {...props} />
);

HealerNav.displayName = "HealerNav";
HealerDagIncidents.displayName = "HealerDagIncidents";
HealerTaskIncident.displayName = "HealerTaskIncident";

const registry = globalThis as unknown as Record<string, FC<PluginComponentProps> | undefined>;

for (const [names, component] of [
  [[APP_NAMES.nav, LEGACY_APP_NAMES.nav], HealerNav],
  [[APP_NAMES.dag, LEGACY_APP_NAMES.dag], HealerDagIncidents],
  [[APP_NAMES.taskInstance, LEGACY_APP_NAMES.taskInstance], HealerTaskIncident],
] as const) {
  for (const name of names) {
    registry[name] = component;
  }
}
// Last-resort fallback read by the host when globalThis[name] is missing. The host resets this
// shared global before each plugin import, so the named globals above are what really matters.
globalThis.AirflowPlugin = HealerNav;

export default HealerNav;
