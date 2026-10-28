/**
 * Words of the orchestrator the views talk about ("DAG", "task", "Retry task"...), and the host the
 * views are served by.
 *
 * The defaults are the Airflow words (`dag_healer.core.vocab.Vocabulary`), so every view renders
 * today's text when nobody provides a {@link UiText}. The Airflow plugin never provides one (and never
 * calls `/me`); the standalone host reads the orchestrator's words from `GET /me` and provides them
 * through {@link UiTextContext}.
 */
import { createContext, useContext } from "react";

/** The keys of `dag_healer.core.vocab.Vocabulary` (`/me` → `orchestrator.vocabulary`). */
export interface Vocabulary {
  attempt: string;
  map_index: string;
  pipeline: string;
  pipelines: string;
  product: string;
  retry_action: string;
  run: string;
  step: string;
  steps: string;
}

/** The Airflow words: every vocabulary-driven text renders today's literal text with them. */
export const DEFAULT_VOCABULARY: Vocabulary = {
  attempt: "try",
  map_index: "map index",
  pipeline: "DAG",
  pipelines: "DAGs",
  product: "Airflow",
  retry_action: "Retry task",
  run: "DAG run",
  step: "task",
  steps: "tasks",
};

/** Orchestrator name of the default Airflow instance, whose `/me` answer has no orchestrator block. */
export const DEFAULT_DISPLAY_NAME = "Apache Airflow";

/** Where the views are served: inside the Airflow UI, or by `dag-healer serve`. */
export type UiHost = "airflow_plugin" | "standalone";

export interface UiText {
  /** The orchestrator's name ("AWS Step Functions"); {@link DEFAULT_DISPLAY_NAME} when unset. */
  readonly displayName?: string;
  readonly host: UiHost;
  /** The orchestrator type (`/me` → `orchestrator.type`); "airflow" when unset. */
  readonly type?: string;
  readonly words: Vocabulary;
}

export const DEFAULT_UI_TEXT: UiText = { host: "airflow_plugin", words: DEFAULT_VOCABULARY };

/** The orchestrator type of a {@link UiText}: "airflow" unless `/me` named another one. */
export const orchestratorType = (text: UiText): string => text.type ?? "airflow";

export const UiTextContext = createContext<UiText>(DEFAULT_UI_TEXT);

/** Words and host of the current view (today's Airflow strings outside a provider). */
export const useUiText = (): UiText => useContext(UiTextContext);

const VOCABULARY_KEYS = Object.keys(DEFAULT_VOCABULARY) as Array<keyof Vocabulary>;

/**
 * The vocabulary of a `/me` answer: its non-empty string values of known keys, merged over the
 * defaults. Unknown keys, other value types and anything that is not an object are ignored.
 */
export const vocabularyFrom = (raw: unknown): Vocabulary => {
  if (raw === null || typeof raw !== "object" || Array.isArray(raw)) {
    return DEFAULT_VOCABULARY;
  }
  const record = raw as Record<string, unknown>;
  const words: Vocabulary = { ...DEFAULT_VOCABULARY };

  for (const key of VOCABULARY_KEYS) {
    const value = record[key];

    if (typeof value === "string" && value.trim().length > 0) {
      words[key] = value;
    }
  }

  return words;
};

/** First letter upper-cased, the rest kept: "task" → "Task", "DAG" stays "DAG". */
export const capitalize = (word: string): string => word.charAt(0).toUpperCase() + word.slice(1);
