/**
 * The view of a standalone page. The shell of `dag-healer serve` serves the Airflow plugin's URL
 * shapes: `/plugin/healer`, `/dags/<d>/plugin/healer-dag` and
 * `/dags/<d>/runs/<r>/tasks/<t>[/mapped/<i>]/plugin/healer-ti`, below the `<base href>` path.
 */
import type { HealerMode } from "src/App";
import { pathSegmentsAfterBase } from "src/context/pluginContext";
import { PLUGIN_ROUTES } from "src/links";

/** `.../plugin/healer-ti` → task_instance, `.../plugin/healer-dag` → dag, anything else → nav. */
export const standaloneMode = (pathname: string, baseUri?: string): HealerMode => {
  const segments = pathSegmentsAfterBase(pathname, baseUri);

  if (segments.at(-2) === "plugin") {
    switch (segments.at(-1)) {
      case PLUGIN_ROUTES.task_instance:
        return "task_instance";
      case PLUGIN_ROUTES.dag:
        return "dag";
      default:
        break;
    }
  }

  return "nav";
};
