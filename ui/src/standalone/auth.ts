/**
 * Browser side of the standalone `token` backend (`serve.auth.backend: token`).
 *
 * A browser cannot send a bearer token on its own, so the user signs in with an API token, which the
 * standalone entry keeps for this tab (sessionStorage, plus memory when storage is blocked) and sends
 * as `Authorization: Bearer <token>` on every API call. The token is never put in localStorage, a URL
 * or a log. The shell names the backend in `<meta name="dag-healer-auth">`; the Airflow plugin never
 * loads this module.
 */
import { setAuthorizationProvider } from "src/api";

export const TOKEN_STORAGE_KEY = "dag-healer.token";

/** Name of the `<meta>` element in which the shell names the auth backend. */
export const AUTH_META_NAME = "dag-healer-auth";

/** `serve.auth.backend` of the token sign-in. */
export const TOKEN_BACKEND = "token";

/** Longest token the server accepts. */
export const MAX_TOKEN_LENGTH = 4096;

// Space (U+0020) to tilde (U+007E): the characters a header value can carry as they are.
const PRINTABLE_ASCII = /^[ -~]+$/u;

// The token of this page, so a sign-in also works where sessionStorage throws (blocked storage).
let pageToken: string | undefined;

/** True for a value the server can accept: 1 to 4096 printable ASCII characters, no outer spaces. */
export const isUsableToken = (token: string): boolean =>
  token.length > 0 &&
  token.length <= MAX_TOKEN_LENGTH &&
  token.trim() === token &&
  PRINTABLE_ASCII.test(token);

/** The signed-in token of this tab, if any. */
export const readToken = (): string | undefined => {
  if (pageToken !== undefined) {
    return pageToken;
  }
  try {
    const stored = globalThis.sessionStorage.getItem(TOKEN_STORAGE_KEY);

    return stored !== null && isUsableToken(stored) ? stored : undefined;
  } catch {
    return undefined;
  }
};

/** Keep a token for this tab (it survives reloads while sessionStorage works). */
export const storeToken = (token: string): void => {
  pageToken = token;
  try {
    globalThis.sessionStorage.setItem(TOKEN_STORAGE_KEY, token);
  } catch {
    // Blocked storage: the token is kept for this page only.
  }
};

/** Forget the token of this tab. */
export const clearToken = (): void => {
  pageToken = undefined;
  try {
    globalThis.sessionStorage.removeItem(TOKEN_STORAGE_KEY);
  } catch {
    // Blocked storage: nothing was stored.
  }
};

/** The `Authorization` header value of a token. */
export const bearer = (token: string): string => `Bearer ${token}`;

/** The auth backend named by the shell (`<meta name="dag-healer-auth" content="...">`), if any. */
export const authBackendFromDocument = (doc: Document = document): string | undefined => {
  const content = doc.querySelector(`meta[name="${AUTH_META_NAME}"]`)?.getAttribute("content")?.trim();

  return content === undefined || content.length === 0 ? undefined : content;
};

/** Send the signed-in token on every API call (read per call, so a sign-in applies right away). */
export const installTokenAuthorization = (): void => {
  setAuthorizationProvider(() => {
    const token = readToken();

    return token === undefined ? undefined : bearer(token);
  });
};
