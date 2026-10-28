/** Small building blocks of the console: text with code spans, state markers, icons, notices. */
import {
  createContext,
  type CSSProperties,
  Fragment,
  type MouseEvent,
  type ReactNode,
  useCallback,
  useContext,
  useEffect,
  useMemo,
  useRef,
  useState,
} from "react";

import { useUiText } from "src/vocabulary";

import { categoryWord, statusMeta, type Tone } from "./labels";

/** A text whose `backtick` spans render as code (the texts of decision.ts and consequences.ts). */
export const RichText = ({ text }: { readonly text: string }) => {
  const parts = text.split("`");

  return (
    <>
      {parts.map((part, index) =>
        // Odd parts were between backticks. Parts never reorder, so the index is a stable key.
        index % 2 === 1 ? <code key={index}>{part}</code> : <Fragment key={index}>{part}</Fragment>,
      )}
    </>
  );
};

export const toneStyle = (tone: Tone): CSSProperties => ({ "--c": `var(--${tone})` }) as CSSProperties;

export const Dot = ({ hollow = false, tone }: { readonly hollow?: boolean; readonly tone: Tone }) => (
  <span aria-hidden="true" className={hollow ? "dot hollow" : "dot"} style={toneStyle(tone)} />
);

/** The status of an incident: a small coloured dot and its name (colour never stands alone). */
export const StatusPill = ({ status }: { readonly status: string }) => {
  const meta = statusMeta(status);

  return (
    <span className="pill" data-status={status} style={toneStyle(meta.tone)}>
      <Dot tone={meta.tone} />
      {meta.label}
    </span>
  );
};

/** The category, as a neutral chip: colour is kept for the state. */
export const CategoryChip = ({ category }: { readonly category: string | null | undefined }) => (
  <span className="chip">{categoryWord(category)}</span>
);

const ICONS: Record<string, ReactNode> = {
  check: <path d="m5 12.5 4.5 4.5L19 7.5" />,
  copy: (
    <>
      <rect height="12" rx="2" width="12" x="8" y="8" />
      <path d="M16 8V6a2 2 0 0 0-2-2H6a2 2 0 0 0-2 2v8a2 2 0 0 0 2 2h2" />
    </>
  ),
  doc: (
    <>
      <path d="M7 3h7l5 5v13H7z" />
      <path d="M14 3v5h5" />
    </>
  ),
  inbox: (
    <>
      <path d="M3.5 13.5h5l1.5 3h4l1.5-3h5" />
      <path d="M5.5 5h13l2.5 8.5V19a1 1 0 0 1-1 1H4a1 1 0 0 1-1-1v-5.5z" />
    </>
  ),
  out: (
    <>
      <path d="M14 4h6v6" />
      <path d="M20 4 11 13" />
      <path d="M18 14v5a1 1 0 0 1-1 1H5a1 1 0 0 1-1-1V7a1 1 0 0 1 1-1h5" />
    </>
  ),
  panelClose: (
    <>
      <rect height="16" rx="2" width="18" x="3" y="4" />
      <path d="M15 4v16" />
      <path d="m8 10 2 2-2 2" />
    </>
  ),
  panelOpen: (
    <>
      <rect height="16" rx="2" width="18" x="3" y="4" />
      <path d="M15 4v16" />
      <path d="m10 10-2 2 2 2" />
    </>
  ),
  pipelines: (
    <>
      <circle cx="5" cy="6" r="2.4" />
      <circle cx="19" cy="6" r="2.4" />
      <circle cx="12" cy="18" r="2.4" />
      <path d="M7.4 6h9.2" />
      <path d="m6.3 8.2 4.4 7.6" />
      <path d="m17.7 8.2-4.4 7.6" />
    </>
  ),
  search: (
    <>
      <circle cx="11" cy="11" r="6.5" />
      <path d="m20 20-4.2-4.2" />
    </>
  ),
  x: (
    <>
      <path d="M6 6l12 12" />
      <path d="M18 6 6 18" />
    </>
  ),
};

export const Icon = ({ name, size = 16 }: { readonly name: keyof typeof ICONS; readonly size?: number }) => (
  <svg
    aria-hidden="true"
    fill="none"
    height={size}
    stroke="currentColor"
    strokeLinecap="round"
    strokeLinejoin="round"
    strokeWidth="1.75"
    viewBox="0 0 24 24"
    width={size}
  >
    {ICONS[name]}
  </svg>
);

/** The product mark: three quiet nodes, one marked. */
export const Mark = () => (
  <svg aria-hidden="true" height="22" viewBox="0 0 24 24" width="22">
    <path d="M6 17 12 7l6 10" fill="none" stroke="var(--border-strong)" strokeWidth="1.6" />
    <circle cx="6" cy="17" fill="var(--panel)" r="2.6" stroke="var(--idle)" strokeWidth="1.6" />
    <circle cx="18" cy="17" fill="var(--panel)" r="2.6" stroke="var(--idle)" strokeWidth="1.6" />
    <circle cx="12" cy="7" fill="var(--cyan)" r="3" />
  </svg>
);

/** A one-line notice: neutral, a warning or an error (errors and warnings are announced). */
export const Notice = ({
  children,
  testId,
  tone = "info",
}: {
  readonly children: ReactNode;
  readonly testId?: string;
  readonly tone?: "error" | "info" | "warn";
}) => (
  <div
    className={tone === "info" ? "notice" : `notice ${tone}`}
    data-testid={testId}
    role={tone === "info" ? undefined : "alert"}
  >
    <Dot tone={tone === "error" ? "pink" : tone === "warn" ? "amber" : "idle"} />
    <div className="stack" style={{ gap: 6 }}>
      {children}
    </div>
  </div>
);

export const Loading = ({ what = "Loading…" }: { readonly what?: string }) => (
  <p className="muted" role="status">
    {what}
  </p>
);

// ------------------------------------------------------------------------------------------------ //
// Announcements: one polite live region for the whole console, plus short toasts
// ------------------------------------------------------------------------------------------------ //

/** `quiet`: read the message out without a toast (what it says is already on the screen). */
type Announce = (message: string, options?: { readonly quiet?: boolean }) => void;

interface Announcer {
  readonly announce: Announce;
}

const AnnounceContext = createContext<Announcer>({ announce: () => undefined });

export const useAnnounce = (): Announce => useContext(AnnounceContext).announce;

const TOAST_MS = 5000;

/** The live region and the toasts; `announce` both reads a message out and shows it briefly. */
export const AnnounceProvider = ({ children }: { readonly children: ReactNode }) => {
  const [message, setMessage] = useState("");
  const [toasts, setToasts] = useState<Array<{ id: number; text: string }>>([]);
  const counter = useRef(0);

  const announce = useCallback<Announce>((text, options) => {
    counter.current += 1;
    const id = counter.current;

    setMessage("");
    // A new text node after a tick, so a repeated message is read out again.
    globalThis.setTimeout(() => setMessage(text), 50);
    if (options?.quiet === true) {
      return;
    }
    setToasts((current) => [...current.slice(-2), { id, text }]);
    globalThis.setTimeout(() => setToasts((current) => current.filter((toast) => toast.id !== id)), TOAST_MS);
  }, []);

  const value = useMemo(() => ({ announce }), [announce]);

  return (
    <AnnounceContext.Provider value={value}>
      {children}
      <div aria-live="polite" className="sr-only" data-testid="live-region" role="status">
        {message}
      </div>
      <div aria-hidden="true" className="toasts">
        {toasts.map((toast) => (
          <div className="toast" key={toast.id}>
            {toast.text}
          </div>
        ))}
      </div>
    </AnnounceContext.Provider>
  );
};

/**
 * Copy `text` the old way, for pages without the Clipboard API (served over plain HTTP from a host
 * other than localhost): a hidden text area next to `anchor`, selected and copied. Focus returns to
 * `anchor`. False when the browser refuses.
 */
const copyBySelection = (text: string, anchor: HTMLElement): boolean => {
  const area = document.createElement("textarea");

  area.value = text;
  area.readOnly = true;
  area.setAttribute("aria-hidden", "true");
  area.style.cssText = "position:fixed;top:0;left:0;width:1px;height:1px;opacity:0;pointer-events:none";
  // Next to the button, so the copy works inside a dialog that keeps focus to itself.
  anchor.after(area);
  area.select();
  let copied = false;

  try {
    // The deprecated command is the only copy left to a page without the Clipboard API.
    copied = document.execCommand("copy");
  } catch {
    copied = false;
  }
  area.remove();
  anchor.focus();

  return copied;
};

/** Copy a text, saying whether it worked (clipboard access can be blocked). */
export const CopyButton = ({ label, text }: { readonly label: string; readonly text: string }) => {
  const announce = useAnnounce();
  // "Incident id" → "incident id" inside a sentence; "PR.md" stays as it is.
  const inline = /^[A-Z][a-z]/u.test(label) ? label.charAt(0).toLowerCase() + label.slice(1) : label;
  const copy = (event: MouseEvent<HTMLButtonElement>) => {
    const button = event.currentTarget;
    const done = () => announce(`${label} copied`);
    // Never the text itself: it can be long (PR.md), and it is on the screen already.
    const fallback = () =>
      copyBySelection(text, button)
        ? done()
        : announce(
            `Copying is blocked in this browser: select the ${inline} and press Ctrl+C (⌘C on a Mac).`,
          );
    const clipboard = globalThis.navigator.clipboard as Clipboard | undefined;

    if (clipboard === undefined) {
      fallback();

      return;
    }
    void clipboard.writeText(text).then(done, fallback);
  };

  return (
    <button className="btn sm ghost" onClick={copy} type="button">
      <Icon name="copy" size={14} />
      Copy {inline}
    </button>
  );
};

/**
 * The page title of a view on `dag-healer serve` ("Inbox · Healer · Apache Airflow"), so tabs and
 * history entries say where they lead. Inside Airflow the page and its title are Airflow's.
 */
export const usePageTitle = (title: string | undefined): void => {
  const { host } = useUiText();

  useEffect(() => {
    if (host !== "standalone" || title === undefined) {
      return undefined;
    }
    const before = document.title;

    document.title = `${title} · ${before}`;

    return () => {
      document.title = before;
    };
  }, [host, title]);
};

/** Run `onKey` for keydown events on the document while mounted. */
export const useDocumentKeys = (onKey: (event: KeyboardEvent) => void): void => {
  const handler = useRef(onKey);

  useEffect(() => {
    handler.current = onKey;
  });
  useEffect(() => {
    const listener = (event: KeyboardEvent) => handler.current(event);

    document.addEventListener("keydown", listener);

    return () => document.removeEventListener("keydown", listener);
  }, []);
};
