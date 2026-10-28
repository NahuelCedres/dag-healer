/**
 * The confirmation of an action: its consequences first (what dag-healer will do, who is told,
 * which files change), then an optional note for the timeline and the confirm button.
 */
import { type ReactNode, useEffect, useId, useRef, useState } from "react";

import { toApiError } from "src/api";
import { sessionExpiredRetryMessage } from "src/messages";
import { type UiText, useUiText } from "src/vocabulary";

import type { Consequences } from "./consequences";
import { Notice, RichText } from "./ui";

/** The message of a failed action. */
export const actionError = (error: unknown, text: UiText): string => {
  const apiError = toApiError(error);

  switch (apiError.status) {
    case 401:
      return sessionExpiredRetryMessage(text);
    case 403:
      return "You do not have permission to do this.";
    case 409:
      return `This is no longer possible: ${apiError.detail}`;
    default:
      return apiError.detail;
  }
};

export const ConfirmPanel = ({
  consequences,
  heading,
  onCancel,
  onConfirm,
}: {
  readonly consequences: Consequences;
  /** "If the input is wrong", "Approve the contract". */
  readonly heading: string;
  readonly onCancel: () => void;
  /** Queue the action; the panel shows the error when it fails. */
  readonly onConfirm: (note: string) => Promise<unknown>;
}) => {
  const text = useUiText();
  const headingRef = useRef<HTMLHeadingElement>(null);
  const errorRef = useRef<HTMLDivElement>(null);
  const noteId = useId();
  const [note, setNote] = useState("");
  const [busy, setBusy] = useState(false);
  const [error, setError] = useState<string>();

  // The consequences are read first: focus goes to the heading of the panel when it opens.
  useEffect(() => {
    headingRef.current?.focus();
  }, [heading]);

  // A refusal takes focus, so it is read out and Tab continues from it.
  useEffect(() => {
    if (error !== undefined) {
      errorRef.current?.focus();
    }
  }, [error]);

  const confirm = () => {
    if (busy) {
      return;
    }
    setBusy(true);
    setError(undefined);
    onConfirm(note)
      .catch((caught: unknown) => setError(actionError(caught, text)))
      .finally(() => setBusy(false));
  };

  const block = (label: string, body: ReactNode) => (
    <div className="stack" style={{ gap: 4 }}>
      <p className="lbl">{label}</p>
      {body}
    </div>
  );

  return (
    <section aria-labelledby={`${noteId}-h`} className="confirm" data-testid="confirm-panel">
      <h3 id={`${noteId}-h`} ref={headingRef} tabIndex={-1}>
        {heading}
      </h3>
      {block(
        "What dag-healer will do",
        <ul className="cons">
          {consequences.does.map((line) => (
            <li key={line}>
              <RichText text={line} />
            </li>
          ))}
        </ul>,
      )}
      {consequences.alert === undefined
        ? undefined
        : block(
            "Who is told",
            <p className="muted">
              <RichText text={consequences.alert} />
            </p>,
          )}
      {consequences.files === undefined
        ? undefined
        : block(
            "Which files change",
            <p className="muted">
              <RichText text={consequences.files} />
            </p>,
          )}
      {consequences.then === undefined
        ? undefined
        : block(
            "Afterwards",
            <p className="muted">
              <RichText text={consequences.then} />
            </p>,
          )}
      {consequences.warnings.length === 0 ? undefined : (
        <Notice tone="warn">
          {consequences.warnings.map((warning) => (
            <p key={warning}>
              <RichText text={warning} />
            </p>
          ))}
        </Notice>
      )}
      <label className="field" htmlFor={noteId}>
        <span>Note for the timeline (optional)</span>
        <textarea
          id={noteId}
          maxLength={2000}
          onChange={(event) => setNote(event.target.value)}
          placeholder="Why you chose this"
          rows={2}
          value={note}
        />
      </label>
      {error === undefined ? undefined : (
        <div ref={errorRef} tabIndex={-1}>
          <Notice testId="action-error" tone="error">
            <p>{error}</p>
          </Notice>
        </div>
      )}
      {/* aria-disabled, not disabled: a disabled button would drop the focus while the action is sent. */}
      <div className="row">
        <button aria-disabled={busy} className="btn primary" onClick={confirm} type="button">
          {busy ? "Sending…" : consequences.confirm}
        </button>
        <button
          aria-disabled={busy}
          className="btn ghost"
          onClick={() => {
            if (!busy) {
              onCancel();
            }
          }}
          type="button"
        >
          Cancel
        </button>
        <span className="faint small">The worker applies it within a few seconds.</span>
      </div>
    </section>
  );
};
