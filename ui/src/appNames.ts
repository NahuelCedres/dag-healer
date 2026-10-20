/** react_apps names of the plugin: the labels Airflow shows and the globalThis keys (see airflow/plugin.py). */
export const APP_NAMES = {
  dag: "Healer incidents",
  nav: "Healer",
  taskInstance: "Healer incident",
} as const;

/** Names registered by dag-healer 0.1 plugin definitions, kept as aliases for older plugin.py files. */
export const LEGACY_APP_NAMES = {
  dag: "DagHealerDag",
  nav: "DagHealerNav",
  taskInstance: "DagHealerTaskInstance",
} as const;
