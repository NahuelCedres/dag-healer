import type { SystemContext } from "@chakra-ui/react";
import type { FC } from "react";

import type { PluginComponentProps } from "./main";

export type global = object;

declare global {
  /** Chakra UI system exposed by the Airflow host UI (undefined during local development). */
  var ChakraUISystem: SystemContext | undefined;

  /**
   * Shared last-resort global read by the Airflow host when globalThis[name] is missing.
   *
   * The components themselves are registered under their react_apps names ("Healer", "Healer
   * incidents", "Healer incident"), which contain spaces and are therefore set through an index
   * signature in main.tsx.
   */
  var AirflowPlugin: FC<PluginComponentProps> | undefined;
}
