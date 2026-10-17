import { StrictMode } from "react";
import { createRoot } from "react-dom/client";

import { App, type HealerMode } from "./App";

// Development entry point (`pnpm dev`). Pick the view with ?mode=nav|dag|task_instance; the Airflow
// context comes from the path, e.g. /dags/my_dag/runs/<run_id>/tasks/<task_id>?mode=task_instance.
const MODES: ReadonlyArray<HealerMode> = ["nav", "dag", "task_instance"];
const requested = new URLSearchParams(globalThis.location.search).get("mode");
const mode = MODES.find((candidate) => candidate === requested) ?? "nav";

createRoot(document.querySelector("#root") as HTMLDivElement).render(
  <StrictMode>
    <App mode={mode} />
  </StrictMode>,
);
