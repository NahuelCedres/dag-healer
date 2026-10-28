/**
 * The root element of every console view: the `.dhc` scope of the styles, the theme, the live
 * region, and a place for dialogs next to the content (the content turns inert behind a dialog).
 */
import { createContext, type ReactNode, type RefObject, useContext, useMemo, useRef, useState } from "react";

import "./console.css";
import type { Theme } from "./prefs";
import { AnnounceProvider } from "./ui";

interface FrameParts {
  /** The content, made inert while a dialog is open. */
  readonly content: RefObject<HTMLDivElement | null>;
  /** Where dialogs render: inside the theme scope, outside the content. */
  readonly dialogs: HTMLDivElement | null;
}

const FrameContext = createContext<FrameParts>({ content: { current: null }, dialogs: null });

export const useFrame = (): FrameParts => useContext(FrameContext);

export const Frame = ({
  children,
  className,
  testId,
  theme,
}: {
  readonly children: ReactNode;
  readonly className?: string;
  readonly testId?: string;
  readonly theme: Theme;
}) => {
  const content = useRef<HTMLDivElement>(null);
  const [dialogs, setDialogs] = useState<HTMLDivElement | null>(null);
  const parts = useMemo(() => ({ content, dialogs }), [dialogs]);

  return (
    <div
      className={className === undefined ? "dhc" : `dhc ${className}`}
      data-testid={testId}
      data-theme={theme}
    >
      <FrameContext.Provider value={parts}>
        <AnnounceProvider>
          <div ref={content}>{children}</div>
          <div ref={setDialogs} />
        </AnnounceProvider>
      </FrameContext.Provider>
    </div>
  );
};
