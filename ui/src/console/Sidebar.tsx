/**
 * The sidebar of the console shell: the product, the places (Inbox, Pipelines), the incidents by
 * what they need, and the viewer's settings (theme on `dag-healer serve`, single-key shortcuts, the
 * user and Sign out).
 */
import { useId, useMemo } from "react";

import { sortNewestFirst } from "src/model";
import { capitalize, DEFAULT_DISPLAY_NAME, useUiText } from "src/vocabulary";

import { useConsoleData } from "./data";
import { GROUPS, incidentGroup, nextStep, statusMeta, stepLabel } from "./labels";
import { NavLink } from "./nav";
import type { ConsolePrefs, ThemeChoice } from "./prefs";
import type { ConsoleRoute } from "./route";
import { shortAge } from "./time";
import { Dot, Icon, Mark } from "./ui";

/** Done incidents listed in the sidebar (the inbox has them all). */
const DONE_SHOWN = 6;

export interface SidebarProps {
  readonly onPrefs: (change: Partial<ConsolePrefs>) => void;
  readonly onSignOut?: () => void;
  readonly prefs: ConsolePrefs;
  readonly route: ConsoleRoute;
  /** Offer the theme choice (the standalone host; inside Airflow the theme follows Airflow's). */
  readonly themeChoice: boolean;
  readonly user?: string;
}

export const Sidebar = ({ onPrefs, onSignOut, prefs, route, themeChoice, user }: SidebarProps) => {
  const text = useUiText();
  const { words } = text;
  const themeId = useId();
  const incidents = useConsoleData().incidents;
  const sorted = useMemo(() => sortNewestFirst(incidents?.data?.incidents ?? []), [incidents?.data]);
  const needs = sorted.filter((incident) => incidentGroup(incident) === "needs").length;
  const currentIncident = route.view === "incident" ? route.incidentId : undefined;

  return (
    <aside aria-label="Sidebar" className="side" data-testid="sidebar">
      <div className="brand">
        <Mark />
        <span>
          <span className="brand-name">dag-healer</span>
          <span className="brand-sub">{text.displayName ?? DEFAULT_DISPLAY_NAME}</span>
        </span>
      </div>
      <nav aria-label="Console" className="nav">
        <NavLink
          ariaCurrent={route.view === "inbox" ? "page" : undefined}
          place={{ to: "inbox" }}
          testId="nav-inbox"
        >
          <Icon name="inbox" />
          <span>Inbox</span>
          {needs > 0 ? (
            <span aria-label={`${needs} need you`} className="count">
              {needs}
            </span>
          ) : undefined}
        </NavLink>
        <NavLink
          ariaCurrent={route.view === "pipelines" ? "page" : undefined}
          place={{ to: "pipelines" }}
          testId="nav-pipelines"
        >
          <Icon name="pipelines" />
          <span>{capitalize(words.pipelines)}</span>
        </NavLink>
      </nav>
      <div className="groups">
        {GROUPS.map((group) => {
          const items = sorted.filter((incident) => incidentGroup(incident) === group.id);
          const shown = group.id === "done" ? items.slice(0, DONE_SHOWN) : items;

          if (items.length === 0 && group.id !== "needs") {
            return null;
          }

          return (
            <section className="group" data-group={group.id} key={group.id}>
              <h2 className="lbl">
                {group.label}
                <span className="n">{items.length}</span>
              </h2>
              {items.length === 0 ? (
                <p className="empty-li">Nothing needs you right now.</p>
              ) : (
                <ul>
                  {shown.map((incident) => (
                    <li key={incident.id}>
                      {/* The step first: in a narrow column the pipeline's name would hide it. */}
                      <NavLink
                        ariaCurrent={incident.id === currentIncident ? "page" : undefined}
                        className="item"
                        place={{ incident, to: "incident" }}
                        title={`${incident.dag_id} / ${stepLabel(incident)}: ${nextStep(incident, words)}`}
                      >
                        <Dot tone={statusMeta(incident.status).tone} />
                        <span className="t">{stepLabel(incident)}</span>
                        <span className="age">{shortAge(incident.created_at)}</span>
                        <span className="sub">
                          <span className="sr-only">{statusMeta(incident.status).label}: </span>
                          <span className="mono">{incident.dag_id}</span> · {nextStep(incident, words)}
                        </span>
                      </NavLink>
                    </li>
                  ))}
                  {items.length > shown.length ? (
                    <li>
                      <NavLink className="empty-li" place={{ group: group.id, to: "inbox" }}>
                        All {items.length} in the inbox
                      </NavLink>
                    </li>
                  ) : undefined}
                </ul>
              )}
            </section>
          );
        })}
      </div>
      <div className="side-foot">
        {user === undefined && onSignOut === undefined ? undefined : (
          <div className="row" style={{ justifyContent: "space-between" }}>
            {user === undefined ? undefined : (
              <span className="muted small" data-testid="standalone-user">
                {user}
              </span>
            )}
            {onSignOut === undefined ? undefined : (
              <button className="btn sm ghost" onClick={onSignOut} type="button">
                Sign out
              </button>
            )}
          </div>
        )}
        <details className="settings" data-testid="settings">
          <summary>Display settings</summary>
          <div className="stack" style={{ gap: 10, paddingTop: 8 }}>
            {themeChoice ? (
              <div className="field">
                <span id={themeId}>Theme</span>
                <div aria-labelledby={themeId} className="seg" role="group">
                  {(["system", "light", "dark"] as Array<ThemeChoice>).map((choice) => (
                    <button
                      aria-pressed={prefs.theme === choice}
                      key={choice}
                      onClick={() => onPrefs({ theme: choice })}
                      type="button"
                    >
                      {capitalize(choice)}
                    </button>
                  ))}
                </div>
              </div>
            ) : (
              <p className="faint small">The theme follows {words.product}'s light or dark mode.</p>
            )}
            <label className="check">
              <input
                checked={prefs.shortcuts}
                onChange={(event) => onPrefs({ shortcuts: event.target.checked })}
                type="checkbox"
              />
              <span>
                Single-key shortcuts <span className="faint">(] shows or hides the side panel)</span>
              </span>
            </label>
          </div>
        </details>
      </div>
    </aside>
  );
};
