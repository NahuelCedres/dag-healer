/**
 * The layout of a console view: the work area and the side panel. In the console shell the panel
 * is a collapsible column (its button, a slim rail when hidden, and the `]` key); in the embedded
 * views of the Airflow tabs it is a plain section after the work area.
 */
import { createContext, type ReactNode, useContext, useEffect, useId, useRef } from "react";

import { useUiText } from "src/vocabulary";

import { Icon, useDocumentKeys } from "./ui";

export interface LayoutState {
  /** True in the Airflow DAG and task tabs: no sidebar, no collapsible panel, no shortcuts. */
  readonly embedded: boolean;
  readonly panelOpen: boolean;
  readonly setPanelOpen: (open: boolean) => void;
  readonly shortcuts: boolean;
}

export const LayoutContext = createContext<LayoutState>({
  embedded: true,
  panelOpen: true,
  setPanelOpen: () => undefined,
  shortcuts: false,
});

export const useLayout = (): LayoutState => useContext(LayoutContext);

const inField = (target: EventTarget | null): boolean =>
  target instanceof Element && target.closest("input, textarea, select, [contenteditable]") !== null;

/**
 * True for the `]` that toggles the panel. `event.key` is the character typed, so ⌥ (macOS) and
 * AltGr (Windows, where it also sets ctrlKey) produce it on Spanish and other layouts.
 */
export const isPanelKey = (event: KeyboardEvent): boolean => {
  const altGr = typeof event.getModifierState === "function" && event.getModifierState("AltGraph");

  return (
    event.key === "]" && !event.metaKey && (!event.ctrlKey || event.altKey || altGr) && !inField(event.target)
  );
};

const MAIN_ID = "dhc-main";

export const ViewLayout = ({
  children,
  latest,
  panel,
  panelTitle,
  testId,
}: {
  readonly children: ReactNode;
  /** One line shown in the work area while the panel is hidden (the latest event). */
  readonly latest?: ReactNode;
  readonly panel?: ReactNode;
  readonly panelTitle: string;
  readonly testId?: string;
}) => {
  const { embedded, panelOpen, setPanelOpen, shortcuts } = useLayout();
  const { host } = useUiText();
  const panelId = useId();
  const toggled = useRef(false);
  const hide = useRef<HTMLButtonElement>(null);
  const show = useRef<HTMLButtonElement>(null);
  const hasPanel = panel !== undefined;

  // After the panel opens or closes, focus moves to the button that does the opposite.
  useEffect(() => {
    if (toggled.current) {
      toggled.current = false;
      (panelOpen ? hide : show).current?.focus();
    }
  }, [panelOpen]);

  useDocumentKeys((event) => {
    if (
      embedded ||
      !hasPanel ||
      !shortcuts ||
      !isPanelKey(event) ||
      document.querySelector(".dhc .overlay")
    ) {
      return;
    }
    event.preventDefault();
    const focusInPanel =
      document.activeElement instanceof Element &&
      document.activeElement.closest(`[data-panel="${panelId}"]`) !== null;

    toggled.current = focusInPanel;
    setPanelOpen(!panelOpen);
  });

  const toggle = (open: boolean) => {
    toggled.current = true;
    setPanelOpen(open);
  };
  const keyHint = shortcuts ? " (])" : "";
  // Airflow's page has its own main landmark: inside it the work area is a labelled region.
  const Main = host === "standalone" ? "main" : "section";
  const mainLabel = host === "standalone" ? {} : { "aria-label": "Healer console" };

  if (embedded) {
    return (
      <div className="embedded" data-testid={testId}>
        <div className="work">
          {children}
          {hasPanel ? (
            <section aria-label={panelTitle} className="card">
              <h2 className="lbl">{panelTitle}</h2>
              {panel}
            </section>
          ) : undefined}
        </div>
      </div>
    );
  }

  return (
    <>
      <Main className="main" data-testid={testId} id={MAIN_ID} tabIndex={-1} {...mainLabel}>
        <div className="work">
          {children}
          {!panelOpen && latest !== undefined ? latest : undefined}
        </div>
      </Main>
      {!hasPanel ? undefined : (
        <aside
          aria-labelledby={`${panelId}-t`}
          className="panel"
          data-panel={panelId}
          data-testid="side-panel"
          hidden={!panelOpen}
          id={panelId}
        >
          <div className="panel-h">
            <h2 className="lbl" id={`${panelId}-t`}>
              {panelTitle}
            </h2>
            <button
              aria-controls={panelId}
              aria-expanded="true"
              aria-label={`Hide ${panelTitle.toLowerCase()}`}
              className="icon-btn"
              data-testid="panel-hide"
              onClick={() => toggle(false)}
              ref={hide}
              title={`Hide this panel${keyHint}`}
              type="button"
            >
              <Icon name="panelClose" />
            </button>
          </div>
          {panel}
        </aside>
      )}
      {!hasPanel || panelOpen ? undefined : (
        <div className="rail" data-panel={panelId}>
          <button
            aria-controls={panelId}
            aria-expanded="false"
            aria-label={`Show ${panelTitle.toLowerCase()}`}
            className="rail-btn"
            data-testid="panel-show"
            onClick={() => toggle(true)}
            ref={show}
            title={`Show ${panelTitle.toLowerCase()}${keyHint}`}
            type="button"
          >
            <Icon name="panelOpen" />
            <span className="rail-label">{panelTitle}</span>
          </button>
        </div>
      )}
    </>
  );
};

export { MAIN_ID };
