import { Alert, Button } from "@chakra-ui/react";

import type { ApiError } from "src/api";
import { noPipelineAccessMessage, sessionExpiredMessage } from "src/messages";
import { useUiText } from "src/vocabulary";

const reload = () => {
  globalThis.location.reload();
};

/** Banner shown when any call answered 401: the session expired (inside Airflow, its `_token` cookie). */
export const SessionExpiredBanner = () => {
  const text = useUiText();

  return (
    <Alert.Root data-testid="session-expired" status="warning">
      <Alert.Indicator />
      <Alert.Content>
        <Alert.Title>{sessionExpiredMessage(text)}</Alert.Title>
      </Alert.Content>
      <Button onClick={reload} size="xs" variant="outline">
        Reload
      </Button>
    </Alert.Root>
  );
};

/**
 * Inline notice for a failed GET. 401 renders the session banner, 403 a "no access" message (by
 * default about the pipeline, "You do not have access to this DAG."); callers handle 404 themselves
 * (it usually means "nothing recorded yet").
 */
export const ApiErrorNotice = ({
  error,
  forbiddenMessage,
}: {
  readonly error: ApiError;
  readonly forbiddenMessage?: string;
}) => {
  const { words } = useUiText();

  if (error.status === 401) {
    return <SessionExpiredBanner />;
  }
  if (error.status === 403) {
    return (
      <Alert.Root data-testid="forbidden" status="error">
        <Alert.Indicator />
        <Alert.Title>{forbiddenMessage ?? noPipelineAccessMessage(words)}</Alert.Title>
      </Alert.Root>
    );
  }

  return (
    <Alert.Root data-testid="api-error" status="error">
      <Alert.Indicator />
      <Alert.Content>
        <Alert.Title>
          {error.status === 0
            ? "Cannot reach the dag-healer API."
            : `The dag-healer API answered ${error.status}.`}
        </Alert.Title>
        <Alert.Description>{error.detail}</Alert.Description>
      </Alert.Content>
    </Alert.Root>
  );
};
