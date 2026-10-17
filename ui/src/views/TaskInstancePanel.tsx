import { Heading, Stack, Text } from "@chakra-ui/react";

import { ContextSummary } from "src/components/ContextSummary";
import type { HealerContext } from "src/context/pluginContext";

/** "Healer" tab of a task instance: classification, diagnosis, evidence and actions. */
export const TaskInstancePanel = ({ context }: { readonly context: HealerContext }) => (
  <Stack gap={4}>
    <Heading as="h2" size="lg">
      Healer incident for {context.taskId ?? "unknown task"}
    </Heading>
    <Text color="fg.muted">
      The classification, diagnosis, masked sample, generated test, diff and pull request of this task
      instance will appear here.
    </Text>
    <ContextSummary context={context} />
  </Stack>
);
