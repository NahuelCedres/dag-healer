import { Box, Heading, Text } from "@chakra-ui/react";
import { Component, type ErrorInfo, type ReactNode } from "react";

type ErrorBoundaryProps = {
  readonly children: ReactNode;
};

type ErrorBoundaryState = {
  error: Error | undefined;
};

/** Keeps a crash inside the plugin from breaking the surrounding Airflow page. */
export class ErrorBoundary extends Component<ErrorBoundaryProps, ErrorBoundaryState> {
  public state: ErrorBoundaryState = { error: undefined };

  public static getDerivedStateFromError(error: Error): ErrorBoundaryState {
    return { error };
  }

  public componentDidCatch(error: Error, info: ErrorInfo): void {
    console.error("dag-healer UI crashed:", error, info.componentStack);
  }

  public render(): ReactNode {
    const { error } = this.state;

    if (error === undefined) {
      return this.props.children;
    }

    return (
      <Box borderColor="border.error" borderRadius="md" borderWidth="1px" p={4} role="alert">
        <Heading size="md">dag-healer could not render this view</Heading>
        <Text color="fg.muted" fontSize="sm" mt={2}>
          {error.message}
        </Text>
      </Box>
    );
  }
}
