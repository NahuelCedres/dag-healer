/** What the console says when the API fails: an expired session, no access, or an error. */
import type { ApiError } from "src/api";
import { noPipelineAccessMessage, sessionExpiredMessage } from "src/messages";
import { useUiText } from "src/vocabulary";

import { Notice } from "./ui";

const reload = () => {
  globalThis.location.reload();
};

export const SessionExpired = () => {
  const text = useUiText();

  return (
    <Notice testId="session-expired" tone="warn">
      <div className="row">
        <p>{sessionExpiredMessage(text)}</p>
        <button className="btn sm" onClick={reload} type="button">
          Reload
        </button>
      </div>
    </Notice>
  );
};

/**
 * A failed request: 401 is the session banner, 403 a "no access" line (about the pipeline unless
 * `forbidden` says otherwise), anything else the API's message.
 */
export const ApiProblem = ({
  error,
  forbidden,
}: {
  readonly error: ApiError;
  readonly forbidden?: string;
}) => {
  const { words } = useUiText();

  if (error.status === 401) {
    return <SessionExpired />;
  }
  if (error.status === 403) {
    return (
      <Notice testId="forbidden" tone="error">
        <p>{forbidden ?? noPipelineAccessMessage(words)}</p>
      </Notice>
    );
  }

  return (
    <Notice testId="api-error" tone="error">
      <p>
        {error.status === 0
          ? "Cannot reach the dag-healer API."
          : `The dag-healer API answered ${error.status}.`}
      </p>
      <p className="muted">{error.detail}</p>
    </Notice>
  );
};
