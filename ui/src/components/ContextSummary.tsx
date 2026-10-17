import { Code, Grid, Text } from "@chakra-ui/react";

import type { HealerContext } from "src/context/pluginContext";

const FIELDS: ReadonlyArray<{ key: keyof HealerContext; label: string }> = [
  { key: "dagId", label: "DAG" },
  { key: "runId", label: "Run" },
  { key: "taskId", label: "Task" },
  { key: "mapIndex", label: "Map index" },
];

/** Key/value list of the resolved Airflow context; missing values render as "n/a". */
export const ContextSummary = ({ context }: { readonly context: HealerContext }) => (
  <Grid
    aria-label="Resolved Airflow context"
    columnGap={4}
    data-testid="healer-context"
    rowGap={1}
    templateColumns="max-content 1fr"
  >
    {FIELDS.map(({ key, label }) => (
      <ContextRow key={key} label={label} value={context[key]} />
    ))}
  </Grid>
);

const ContextRow = ({ label, value }: { readonly label: string; readonly value: string | undefined }) => (
  <>
    <Text color="fg.muted" fontSize="sm">
      {label}
    </Text>
    {value === undefined ? (
      <Text color="fg.subtle" fontSize="sm">
        n/a
      </Text>
    ) : (
      <Code fontSize="sm" width="fit-content">
        {value}
      </Code>
    )}
  </>
);
