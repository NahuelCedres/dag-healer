/**
 * User-facing messages shared by several views (and asserted by the tests).
 *
 * The constants are today's Airflow texts. The functions build the same texts from the orchestrator's
 * words and host (`src/vocabulary.ts`); with the defaults they return exactly the constants.
 */
import { DEFAULT_UI_TEXT, DEFAULT_VOCABULARY, type UiText, type Vocabulary } from "src/vocabulary";

export const SESSION_EXPIRED_MESSAGE = "Your Airflow session expired — reload the page";

export const NO_DAG_ACCESS_MESSAGE = "You do not have access to this DAG.";

export const QUEUED_MESSAGE = "Queued — the worker will apply it shortly";

export const NO_TI_INCIDENT_MESSAGE = "No incident recorded for this task instance.";

export const NOT_MONITORED_MESSAGE =
  "This DAG is not monitored by dag-healer. Add it to monitor.dags in healer.yaml.";

export const MASKED_LABEL = "Values are masked by dag-healer";

/** "Your <product> session" inside the Airflow UI; the standalone host has its own session. */
const session = ({ host, words }: UiText): string =>
  host === "standalone" ? "Your session" : `Your ${words.product} session`;

/** Banner of a 401 ({@link SESSION_EXPIRED_MESSAGE} with the defaults). */
export const sessionExpiredMessage = (text: UiText = DEFAULT_UI_TEXT): string =>
  `${session(text)} expired — reload the page`;

/** Error of an action that answered 401 (the "... and try again." variant). */
export const sessionExpiredRetryMessage = (text: UiText = DEFAULT_UI_TEXT): string =>
  `${session(text)} expired — reload the page and try again.`;

/** 403 on a pipeline ({@link NO_DAG_ACCESS_MESSAGE} with the defaults). */
export const noPipelineAccessMessage = (words: Vocabulary = DEFAULT_VOCABULARY): string =>
  `You do not have access to this ${words.pipeline}.`;

/** A pipeline outside `monitor.dags` ({@link NOT_MONITORED_MESSAGE} with the defaults). */
export const notMonitoredMessage = (words: Vocabulary = DEFAULT_VOCABULARY): string =>
  `This ${words.pipeline} is not monitored by dag-healer. Add it to monitor.dags in healer.yaml.`;

/** A step instance without incident ({@link NO_TI_INCIDENT_MESSAGE} with the defaults). */
export const noStepInstanceIncidentMessage = (words: Vocabulary = DEFAULT_VOCABULARY): string =>
  `No incident recorded for this ${words.step} instance.`;
