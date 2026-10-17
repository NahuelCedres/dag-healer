import { Heading, Stack, Text } from "@chakra-ui/react";

import { ContextSummary } from "src/components/ContextSummary";
import type { HealerContext } from "src/context/pluginContext";

/** Top-level "Healer" page (nav destination): incident timeline, contracts and monitored DAGs. */
export const HealerPage = ({ context }: { readonly context: HealerContext }) => (
  <Stack gap={4}>
    <Heading as="h1" size="xl">
      dag-healer
    </Heading>
    <Text color="fg.muted">
      Incidents detected across your monitored DAGs, their data contracts and repair status will appear here.
    </Text>
    <ContextSummary context={context} />
  </Stack>
);
