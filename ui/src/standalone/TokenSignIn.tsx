import { type FormEvent, useId, useState } from "react";

import { apiFetch, type MeResponse, toApiError } from "src/api";

import { bearer, isUsableToken, MAX_TOKEN_LENGTH, storeToken } from "./auth";

export const TOKEN_REJECTED_MESSAGE = "This token was not accepted.";

/** `GET /me` with a candidate token, sent explicitly because it is not kept yet. */
const checkToken = (token: string): Promise<MeResponse> =>
  apiFetch<MeResponse>("me", { headers: { Authorization: bearer(token) } });

/**
 * Sign-in card of the token backend. The API token is checked with `GET /me` before it is kept for
 * this tab; `onSignedIn` gets that answer. The input has no `name`, so the token can never be
 * submitted as a form field (and end up in a URL).
 */
export const TokenSignIn = ({ onSignedIn }: { readonly onSignedIn: (me: MeResponse) => void }) => {
  const [token, setToken] = useState("");
  const [busy, setBusy] = useState(false);
  const [error, setError] = useState<string>();
  const inputId = useId();
  const errorId = useId();

  const submit = (event: FormEvent<HTMLFormElement>) => {
    event.preventDefault();
    const candidate = token.trim();

    if (!isUsableToken(candidate)) {
      setError(TOKEN_REJECTED_MESSAGE);

      return;
    }
    setBusy(true);
    setError(undefined);
    checkToken(candidate)
      .then((me) => {
        storeToken(candidate);
        onSignedIn(me);
      })
      .catch((caught: unknown) => {
        const apiError = toApiError(caught);

        setError(apiError.status === 401 ? TOKEN_REJECTED_MESSAGE : apiError.detail);
        setBusy(false);
      });
  };

  return (
    <form className="card" data-testid="token-sign-in" noValidate onSubmit={submit}>
      <h1 style={{ fontSize: 18, lineHeight: "26px" }}>Sign in</h1>
      <p className="muted">
        Sign in with an API token of this dag-healer, one whose sha256 is listed under serve.auth.tokens in
        healer.yaml. It is kept in this tab only.
      </p>
      <label className="field" htmlFor={inputId}>
        <span>API token</span>
        <input
          aria-describedby={error === undefined ? undefined : errorId}
          aria-invalid={error === undefined ? undefined : true}
          autoComplete="off"
          className="input"
          id={inputId}
          maxLength={MAX_TOKEN_LENGTH}
          onChange={(event) => setToken(event.target.value)}
          spellCheck={false}
          type="password"
          value={token}
        />
      </label>
      {error === undefined ? undefined : (
        <p className="notice error" id={errorId} role="alert">
          {error}
        </p>
      )}
      <div className="row">
        <button className="btn primary" disabled={busy} type="submit">
          {busy ? "Signing in…" : "Sign in"}
        </button>
      </div>
    </form>
  );
};
