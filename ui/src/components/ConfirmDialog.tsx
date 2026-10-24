import { Button, CloseButton, Dialog, Field, Portal, Stack, Text, Textarea } from "@chakra-ui/react";
import { type ReactNode, useState } from "react";

export interface ConfirmDialogProps {
  readonly busy?: boolean;
  readonly confirmLabel: string;
  readonly description: ReactNode;
  readonly error?: string;
  readonly onCancel: () => void;
  readonly onConfirm: (note: string) => void;
  readonly title: string;
  /** Shown under the description (for example the server's `action_warnings` of the action). */
  readonly warnings?: ReadonlyArray<string>;
  /** Show the optional free-text note sent with the command. */
  readonly withNote?: boolean;
}

/**
 * Confirmation step of every state-changing action. Render it only while the action is pending
 * (it opens on mount), so its note field always starts empty.
 */
export const ConfirmDialog = ({
  busy = false,
  confirmLabel,
  description,
  error,
  onCancel,
  onConfirm,
  title,
  warnings = [],
  withNote = true,
}: ConfirmDialogProps) => {
  const [note, setNote] = useState("");

  return (
    <Dialog.Root
      onOpenChange={(details) => {
        if (!details.open && !busy) {
          onCancel();
        }
      }}
      open
      placement="center"
      role="alertdialog"
    >
      <Portal>
        <Dialog.Backdrop />
        <Dialog.Positioner>
          <Dialog.Content>
            <Dialog.Header>
              <Dialog.Title>{title}</Dialog.Title>
            </Dialog.Header>
            <Dialog.Body>
              <Stack gap={4}>
                <Text color="fg.muted" fontSize="sm">
                  {description}
                </Text>
                {warnings.map((warning) => (
                  <Text color="fg.warning" data-testid="action-warning" fontSize="sm" key={warning}>
                    {warning}
                  </Text>
                ))}
                {withNote ? (
                  <Field.Root>
                    <Field.Label>Note (optional)</Field.Label>
                    <Textarea
                      maxLength={2000}
                      onChange={(event) => setNote(event.target.value)}
                      placeholder="Why? Visible in the incident timeline."
                      value={note}
                    />
                  </Field.Root>
                ) : undefined}
                {error === undefined ? undefined : (
                  <Text color="fg.error" fontSize="sm" role="alert">
                    {error}
                  </Text>
                )}
              </Stack>
            </Dialog.Body>
            <Dialog.Footer>
              <Button disabled={busy} onClick={onCancel} variant="outline">
                Cancel
              </Button>
              <Button loading={busy} onClick={() => onConfirm(note)}>
                {confirmLabel}
              </Button>
            </Dialog.Footer>
            <Dialog.CloseTrigger asChild disabled={busy}>
              <CloseButton size="sm" />
            </Dialog.CloseTrigger>
          </Dialog.Content>
        </Dialog.Positioner>
      </Portal>
    </Dialog.Root>
  );
};
