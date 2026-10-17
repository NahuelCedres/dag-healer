/**
 * Entry point of the dag-healer UI bundle (built as UMD into `main.umd.cjs`).
 *
 * The Airflow host imports the bundle once per URL and then reads `globalThis[react_app.name]`, so the
 * three components below are registered on globalThis while the module evaluates. All three
 * react_apps entries of the plugin share this single bundle.
 */
import type { FC } from "react";

import { App } from "./App";

/** Props passed by the Airflow host (3.1: ids from the route; 3.3 may add the objects). */
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
export const DagHealerNav: FC<PluginComponentProps> = (props) => <App mode="nav" {...props} />;

/** "Healer" tab of a DAG (react_apps destination "dag"). */
export const DagHealerDag: FC<PluginComponentProps> = (props) => <App mode="dag" {...props} />;

/** "Healer" tab of a task instance (react_apps destination "task_instance"). */
export const DagHealerTaskInstance: FC<PluginComponentProps> = (props) => (
  <App mode="task_instance" {...props} />
);

DagHealerNav.displayName = "DagHealerNav";
DagHealerDag.displayName = "DagHealerDag";
DagHealerTaskInstance.displayName = "DagHealerTaskInstance";

globalThis.DagHealerNav = DagHealerNav;
globalThis.DagHealerDag = DagHealerDag;
globalThis.DagHealerTaskInstance = DagHealerTaskInstance;
// Last-resort fallback read by the host when globalThis[name] is missing. The host resets this
// shared global before each plugin import, so the named globals above are what really matters.
globalThis.AirflowPlugin = DagHealerNav;

export default DagHealerNav;
