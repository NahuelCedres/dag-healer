import { type ReactNode, useEffect, useMemo, useState } from "react";

import { App, type HealerMode } from "src/App";
import { getMe, type MeResponse, toApiError } from "src/api";
import { Frame } from "src/console/Frame";
import { usePrefs, useTheme } from "src/console/prefs";
import { DEFAULT_DISPLAY_NAME, type UiText, vocabularyFrom } from "src/vocabulary";

import { TokenSignIn } from "./TokenSignIn";
import { clearToken, readToken, TOKEN_BACKEND } from "./auth";
import { titleFor } from "./title";

type Session =
  | { readonly me: MeResponse; readonly status: "ready" }
  | { readonly status: "failed" }
  | { readonly status: "loading" }
  | { readonly status: "signed_out" };

export interface StandaloneAppProps {
  /** `serve.auth.backend`, from the shell's `<meta name="dag-healer-auth">`. */
  readonly authBackend?: string;
  readonly mode: HealerMode;
}

const signOut = () => {
  clearToken();
  globalThis.location.reload();
};

/** The page around the screens drawn before the console (loading, sign-in), in the viewer's theme. */
const Screen = ({ children, title }: { readonly children: ReactNode; readonly title: string }) => {
  const [prefs] = usePrefs();
  const theme = useTheme("standalone", prefs.theme);

  useEffect(() => {
    document.documentElement.dataset.dhcTheme = theme;
  }, [theme]);

  return (
    <Frame theme={theme}>
      <main className="center-card stack" data-testid="standalone-screen">
        <p className="lbl" data-testid="standalone-title">
          {title}
        </p>
        {children}
      </main>
    </Frame>
  );
};

/**
 * Root of the standalone page: asks `GET /me` once, then renders the console with the orchestrator's
 * words, the user and, for the token backend, Sign out. With the token backend, a missing or
 * rejected token shows the sign-in card instead. When `/me` fails otherwise, the console still
 * renders (with the default words), so its views show their own errors, such as the session banner
 * on a 401.
 */
export const StandaloneApp = ({ authBackend, mode }: StandaloneAppProps) => {
  const tokenMode = authBackend === TOKEN_BACKEND;
  const [session, setSession] = useState<Session>(() =>
    tokenMode && readToken() === undefined ? { status: "signed_out" } : { status: "loading" },
  );
  const loading = session.status === "loading";

  useEffect(() => {
    if (!loading) {
      return undefined;
    }
    const controller = new AbortController();

    getMe(controller.signal)
      .then((me) => {
        if (!controller.signal.aborted) {
          setSession({ me, status: "ready" });
        }
      })
      .catch((caught: unknown) => {
        if (controller.signal.aborted) {
          return;
        }
        if (tokenMode && toApiError(caught).status === 401) {
          clearToken();
          setSession({ status: "signed_out" });
        } else {
          setSession({ status: "failed" });
        }
      });

    return () => controller.abort();
  }, [loading, tokenMode]);

  const me = session.status === "ready" ? session.me : undefined;
  const text = useMemo<UiText>(
    () => ({
      displayName:
        typeof me?.orchestrator?.display_name === "string" && me.orchestrator.display_name.trim().length > 0
          ? me.orchestrator.display_name
          : DEFAULT_DISPLAY_NAME,
      host: "standalone",
      type: me?.orchestrator?.type,
      words: vocabularyFrom(me?.orchestrator?.vocabulary),
    }),
    [me],
  );

  if (session.status === "signed_out") {
    return (
      <Screen title={titleFor(undefined)}>
        <TokenSignIn onSignedIn={(signedIn) => setSession({ me: signedIn, status: "ready" })} />
      </Screen>
    );
  }
  if (loading) {
    return (
      <Screen title={titleFor(undefined)}>
        <p className="muted" role="status">
          Loading…
        </p>
      </Screen>
    );
  }

  return (
    <App
      mode={mode}
      onSignOut={tokenMode ? signOut : undefined}
      text={text}
      user={typeof me?.user === "string" ? me.user : undefined}
    />
  );
};
