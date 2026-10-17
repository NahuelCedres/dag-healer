import type { SystemContext } from "@chakra-ui/react";
import type { FC } from "react";

import type { PluginComponentProps } from "./main";

export type global = object;

declare global {
  /** Chakra UI system exposed by the Airflow host UI (undefined during local development). */
  var ChakraUISystem: SystemContext | undefined;

  /** Plugin components registered by this bundle; names match the react_apps entries of the plugin. */
  var DagHealerNav: FC<PluginComponentProps> | undefined;
  var DagHealerDag: FC<PluginComponentProps> | undefined;
  var DagHealerTaskInstance: FC<PluginComponentProps> | undefined;

  /** Shared last-resort global read by the Airflow host when globalThis[name] is missing. */
  var AirflowPlugin: FC<PluginComponentProps> | undefined;
}
