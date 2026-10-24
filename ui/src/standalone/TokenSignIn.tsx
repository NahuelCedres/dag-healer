import { Button, Card, Field, Heading, Input, Stack, Text } from "@chakra-ui/react";
import { type FormEvent, useState } from "react";

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
    <Card.Root asChild data-testid="token-sign-in" maxWidth="md" mx="auto" size="sm" variant="outline">
      <form noValidate onSubmit={submit}>
        <Card.Header>
          <Heading as="h1" size="lg">
            Sign in
          </Heading>
        </Card.Header>
        <Card.Body>
          <Stack gap={3}>
            <Text color="fg.muted" fontSize="sm">
              Sign in with an API token of this dag-healer, one whose sha256 is listed under serve.auth.tokens
              in healer.yaml. It is kept in this tab only.
            </Text>
            <Field.Root invalid={error !== undefined}>
              <Field.Label>API token</Field.Label>
              <Input
                autoComplete="off"
                maxLength={MAX_TOKEN_LENGTH}
                onChange={(event) => setToken(event.target.value)}
                spellCheck={false}
                type="password"
                value={token}
              />
              {error === undefined ? undefined : <Field.ErrorText>{error}</Field.ErrorText>}
            </Field.Root>
          </Stack>
        </Card.Body>
        <Card.Footer>
          <Button loading={busy} type="submit">
            Sign in
          </Button>
        </Card.Footer>
      </form>
    </Card.Root>
  );
};
