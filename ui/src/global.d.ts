import type { FC } from "react";

import type { PluginComponentProps } from "./main";

export type global = object;

declare global {
  /**
   * Shared last-resort global read by the Airflow host when globalThis[name] is missing.
   *
   * The components themselves are registered under their react_apps names ("Healer", "Healer
   * incidents", "Healer incident"), which contain spaces and are therefore set through an index
   * signature in main.tsx.
   */
  var AirflowPlugin: FC<PluginComponentProps> | undefined;
}
