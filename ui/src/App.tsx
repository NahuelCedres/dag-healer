import { Box, ChakraProvider } from "@chakra-ui/react";

import { ErrorBoundary } from "src/components/ErrorBoundary";
import { ColorModeProvider } from "src/context/colorMode";
import { type HealerContext, resolveContext } from "src/context/pluginContext";
import { DagPanel } from "src/views/DagPanel";
import { HealerPage } from "src/views/HealerPage";
import { TaskInstancePanel } from "src/views/TaskInstancePanel";

import type { PluginComponentProps } from "./main";
import { localSystem } from "./theme";

/** Which Airflow destination the component is mounted in (one per react_apps entry). */
export type HealerMode = "dag" | "nav" | "task_instance";

export type AppProps = PluginComponentProps & {
  readonly mode: HealerMode;
};

const View = ({ context, mode }: { readonly context: HealerContext; readonly mode: HealerMode }) => {
  switch (mode) {
    case "dag":
      return <DagPanel context={context} />;
    case "task_instance":
      return <TaskInstancePanel context={context} />;
    default:
      return <HealerPage context={context} />;
  }
};

/** Root of every dag-healer view; `mode` picks the view, the rest are the host props. */
export const App = ({ mode, ...props }: AppProps) => {
  // Use the Chakra UI system provided by the Airflow Core UI, so the plugin has a consistent
  // theming with the host Airflow UI; fall back to localSystem for local development.
  const system = globalThis.ChakraUISystem ?? localSystem;
  const context = resolveContext(props);

  return (
    <ChakraProvider value={system}>
      <ColorModeProvider>
        <Box data-healer-mode={mode} p={{ base: 4, md: 6 }} width="100%">
          <ErrorBoundary>
            <View context={context} mode={mode} />
          </ErrorBoundary>
        </Box>
      </ColorModeProvider>
    </ChakraProvider>
  );
};
