/** User-facing messages shared by several views (and asserted by the tests). */

export const SESSION_EXPIRED_MESSAGE = "Your Airflow session expired — reload the page";

export const NO_DAG_ACCESS_MESSAGE = "You do not have access to this DAG.";

export const QUEUED_MESSAGE = "Queued — the worker will apply it shortly";

export const NO_TI_INCIDENT_MESSAGE = "No incident recorded for this task instance.";

export const NOT_MONITORED_MESSAGE =
  "This DAG is not monitored by dag-healer. Add it to monitor.dags in healer.yaml.";

export const MASKED_LABEL = "Values are masked by dag-healer";
