import { Box, ChakraProvider } from "@chakra-ui/react";
import type { ReactNode } from "react";

import { ErrorBoundary } from "src/components/ErrorBoundary";
import { ColorModeProvider } from "src/context/colorMode";
import { type ContextSourceProps, type HealerContext, resolveContext } from "src/context/pluginContext";
import { DagPanel } from "src/views/DagPanel";
import { HealerPage } from "src/views/HealerPage";
import { TaskInstancePanel } from "src/views/TaskInstancePanel";
import { DEFAULT_UI_TEXT, type UiText, UiTextContext } from "src/vocabulary";

import { localSystem } from "./theme";

/** Which Airflow destination the component is mounted in (one per react_apps entry). */
export type HealerMode = "dag" | "nav" | "task_instance";

export type AppProps = ContextSourceProps & {
  /** Rendered above the view (the standalone host's header); the Airflow plugin passes none. */
  readonly header?: ReactNode;
  readonly mode: HealerMode;
  /** Words and host of the views; without it they print today's Airflow texts (the plugin). */
  readonly text?: UiText;
};

const View = ({ context, mode }: { readonly context: HealerContext; readonly mode: HealerMode }) => {
  switch (mode) {
    case "dag":
      return <DagPanel context={context} />;
    case "task_instance":
      return <TaskInstancePanel context={context} />;
    default:
      return <HealerPage />;
  }
};

/** Root of every dag-healer view; `mode` picks the view, the rest are the host props. */
export const App = ({ header, mode, text, ...props }: AppProps) => {
  // Use the Chakra UI system provided by the Airflow Core UI, so the plugin has a consistent
  // theming with the host Airflow UI; fall back to localSystem for local development and the
  // standalone host.
  const system = globalThis.ChakraUISystem ?? localSystem;
  const context = resolveContext(props);

  return (
    <ChakraProvider value={system}>
      <ColorModeProvider>
        <UiTextContext.Provider value={text ?? DEFAULT_UI_TEXT}>
          {header}
          <Box data-healer-mode={mode} p={{ base: 4, md: 6 }} width="100%">
            <ErrorBoundary>
              <View context={context} mode={mode} />
            </ErrorBoundary>
          </Box>
        </UiTextContext.Provider>
      </ColorModeProvider>
    </ChakraProvider>
  );
};
