import { Box, ChakraProvider, HStack, Spinner, Text } from "@chakra-ui/react";
import { type ReactNode, useEffect, useMemo, useState } from "react";

import { App, type HealerMode } from "src/App";
import { getMe, type MeResponse, toApiError } from "src/api";
import { ColorModeProvider } from "src/context/colorMode";
import { localSystem } from "src/theme";
import { type UiText, vocabularyFrom } from "src/vocabulary";

import { StandaloneHeader } from "./StandaloneHeader";
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

/** Chakra providers of the screens drawn without App (loading, sign-in); App has its own. */
const Frame = ({ children }: { readonly children: ReactNode }) => (
  <ChakraProvider value={localSystem}>
    <ColorModeProvider>{children}</ColorModeProvider>
  </ChakraProvider>
);

/**
 * Root of the standalone page: asks `GET /me` once, then renders the same App as the Airflow plugin
 * with the orchestrator's words and a small header. With the token backend, a missing or rejected
 * token shows the sign-in card instead. When `/me` fails otherwise, App still renders (with the
 * default words), so its views show their own errors, such as the session banner on a 401.
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
    () => ({ host: "standalone", words: vocabularyFrom(me?.orchestrator?.vocabulary) }),
    [me],
  );
  const header = (
    <StandaloneHeader
      onSignOut={tokenMode && session.status !== "signed_out" ? signOut : undefined}
      title={titleFor(me)}
      user={typeof me?.user === "string" ? me.user : undefined}
    />
  );

  if (session.status === "signed_out") {
    return (
      <Frame>
        {header}
        <Box p={{ base: 4, md: 6 }}>
          <TokenSignIn onSignedIn={(signedIn) => setSession({ me: signedIn, status: "ready" })} />
        </Box>
      </Frame>
    );
  }
  if (loading) {
    return (
      <Frame>
        {header}
        <HStack color="fg.muted" gap={2} p={{ base: 4, md: 6 }}>
          <Spinner size="sm" />
          <Text fontSize="sm">Loading…</Text>
        </HStack>
      </Frame>
    );
  }

  return <App header={header} mode={mode} text={text} />;
};
