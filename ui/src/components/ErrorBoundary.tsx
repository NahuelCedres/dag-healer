import { Component, type ErrorInfo, type ReactNode } from "react";

import "src/console/console.css";

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
      <div className="dhc" style={{ padding: 16 }}>
        <div className="notice error" role="alert">
          <div className="stack" style={{ gap: 6 }}>
            <h2 style={{ fontSize: 14 }}>dag-healer could not render this view</h2>
            <p className="muted">{error.message}</p>
          </div>
        </div>
      </div>
    );
  }
}
