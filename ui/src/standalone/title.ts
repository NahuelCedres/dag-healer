import type { MeResponse } from "src/api";
import { DEFAULT_DISPLAY_NAME } from "src/vocabulary";

/** Product name at the start of the standalone page title. */
export const PRODUCT = "Healer";

/**
 * Title of the standalone header, the same string the shell puts in `<title>`: the product with the
 * orchestrator ("Healer · Fakeflow", "Healer · AWS Step Functions"). "Healer · Apache Airflow" when
 * `/me` names no orchestrator (the default Airflow instance); "Healer" before `/me` answers or when it
 * fails.
 */
export const titleFor = (me?: MeResponse | null): string => {
  if (me === undefined || me === null) {
    return PRODUCT;
  }
  if (me.orchestrator === undefined || me.orchestrator === null) {
    return `${PRODUCT} · ${DEFAULT_DISPLAY_NAME}`;
  }
  const name = me.orchestrator.display_name;

  return typeof name === "string" && name.trim().length > 0 ? `${PRODUCT} · ${name}` : PRODUCT;
};
