import { Alert, Button } from "@chakra-ui/react";

import type { ApiError } from "src/api";
import { NO_DAG_ACCESS_MESSAGE, SESSION_EXPIRED_MESSAGE } from "src/messages";

const reload = () => {
  globalThis.location.reload();
};

/** Banner shown when any call answered 401: the Airflow `_token` cookie expired. */
export const SessionExpiredBanner = () => (
  <Alert.Root data-testid="session-expired" status="warning">
    <Alert.Indicator />
    <Alert.Content>
      <Alert.Title>{SESSION_EXPIRED_MESSAGE}</Alert.Title>
    </Alert.Content>
    <Button onClick={reload} size="xs" variant="outline">
      Reload
    </Button>
  </Alert.Root>
);

/**
 * Inline notice for a failed GET. 401 renders the session banner, 403 a "no access" message;
 * callers handle 404 themselves (it usually means "nothing recorded yet").
 */
export const ApiErrorNotice = ({
  error,
  forbiddenMessage = NO_DAG_ACCESS_MESSAGE,
}: {
  readonly error: ApiError;
  readonly forbiddenMessage?: string;
}) => {
  if (error.status === 401) {
    return <SessionExpiredBanner />;
  }
  if (error.status === 403) {
    return (
      <Alert.Root data-testid="forbidden" status="error">
        <Alert.Indicator />
        <Alert.Title>{forbiddenMessage}</Alert.Title>
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
