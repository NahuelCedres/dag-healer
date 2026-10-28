/**
 * A modal dialog: the page behind it is inert, Tab stays inside, Escape closes it, and focus goes
 * back to the control that opened it.
 */
import { type ReactNode, useEffect, useId, useRef } from "react";
import { createPortal } from "react-dom";

import { useFrame } from "./Frame";
import { Icon } from "./ui";

const FOCUSABLE =
  'a[href], button:not([disabled]), input:not([disabled]), select:not([disabled]), textarea:not([disabled]), [tabindex="0"]';

export const Modal = ({
  actions,
  children,
  onClose,
  subtitle,
  title,
}: {
  readonly actions?: ReactNode;
  readonly children: ReactNode;
  readonly onClose: () => void;
  readonly subtitle?: ReactNode;
  readonly title: string;
}) => {
  const titleId = useId();
  const dialog = useRef<HTMLDivElement>(null);
  const close = useRef<HTMLButtonElement>(null);
  const onCloseRef = useRef(onClose);
  const { content, dialogs } = useFrame();

  useEffect(() => {
    onCloseRef.current = onClose;
  });

  useEffect(() => {
    const opener = document.activeElement instanceof HTMLElement ? document.activeElement : null;
    const background = content.current;

    background?.setAttribute("inert", "");
    close.current?.focus();

    const onKey = (event: KeyboardEvent) => {
      if (event.key === "Escape") {
        event.preventDefault();
        onCloseRef.current();

        return;
      }
      if (event.key !== "Tab" || dialog.current === null) {
        return;
      }
      const items = [...dialog.current.querySelectorAll<HTMLElement>(FOCUSABLE)];
      const first = items[0];
      const last = items.at(-1);

      if (first === undefined || last === undefined) {
        return;
      }
      if (!dialog.current.contains(document.activeElement)) {
        event.preventDefault();
        first.focus();
      } else if (event.shiftKey && document.activeElement === first) {
        event.preventDefault();
        last.focus();
      } else if (!event.shiftKey && document.activeElement === last) {
        event.preventDefault();
        first.focus();
      }
    };

    document.addEventListener("keydown", onKey);

    return () => {
      document.removeEventListener("keydown", onKey);
      background?.removeAttribute("inert");
      if (opener !== null && document.body.contains(opener)) {
        opener.focus();
      }
    };
  }, [content]);

  const overlay = (
    <div
      className="overlay"
      onMouseDown={(event) => {
        if (event.target === event.currentTarget) {
          onClose();
        }
      }}
    >
      <div aria-labelledby={titleId} aria-modal="true" className="modal" ref={dialog} role="dialog">
        <div className="modal-h">
          <div className="stack" style={{ gap: 2, minWidth: 0 }}>
            <h2 id={titleId}>{title}</h2>
            {subtitle === undefined ? undefined : (
              <p className="faint small mono wrap-anywhere">{subtitle}</p>
            )}
          </div>
          <div className="row" style={{ flexWrap: "nowrap" }}>
            {actions}
            <button aria-label="Close" className="icon-btn" onClick={onClose} ref={close} type="button">
              <Icon name="x" />
            </button>
          </div>
        </div>
        <div className="modal-b">{children}</div>
      </div>
    </div>
  );

  return dialogs === null ? overlay : createPortal(overlay, dialogs);
};
