import { Heading, Stack, Text } from "@chakra-ui/react";

import { ContextSummary } from "src/components/ContextSummary";
import type { HealerContext } from "src/context/pluginContext";

/** "Healer" tab of a DAG: contract status and the incidents of this DAG. */
export const DagPanel = ({ context }: { readonly context: HealerContext }) => (
  <Stack gap={4}>
    <Heading as="h2" size="lg">
      Healer: {context.dagId ?? "unknown DAG"}
    </Heading>
    <Text color="fg.muted">The data contract and the incidents of this DAG will appear here.</Text>
    <ContextSummary context={context} />
  </Stack>
);
