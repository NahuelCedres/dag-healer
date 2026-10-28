# dag-healer Console: design brief

## Product
dag-healer attaches to EXISTING data pipelines (Airflow today, AWS Step Functions and others through connectors), detects task failures, classifies each as DATA / CODE / AMBIGUOUS, and heals them: CODE gets a failing pytest test + a minimal fix + a PR (or PR.md); DATA gets the violated data-contract rule with a masked sample and an alert; AMBIGUOUS waits for a human decision (resolve as data / as code / ignore). The console is one UI for every orchestrator: the Healer page of the Airflow plugin, and every page of `dag-healer serve`. Its code lives in `ui/src/console/`.

## Direction
Calm and quiet. The console is read during an incident, often by someone tired or new to data engineering, so nothing on the screen competes for attention:
- **The decision comes first.** An incident that waits for a person opens with the question in plain words ("Did `inventory.csv` change on purpose?") and its answers. Each answer shows what dag-healer will do, who gets an alert and which files change before it is confirmed.
- **Plain words next to the source of truth.** A broken contract rule is explained in a sentence, next to the lines of its YAML file; identifiers (DAG, task, rule names, columns) stay visible in monospace.
- **Honest about who decided.** Every incident says whether a fixed rule, the model (with its confidence) or a person decided.
- **Hierarchy from position, size and weight, not colour.** Colour marks state only, on small elements: a status dot and its pill, a test result, a diff line. No glows, no pulses, no gradient washes, no coloured card borders.
- Original design: do not copy any other product's name, logo or branding. Product name in the UI: "dag-healer" (lowercase).

## Design system
The tokens live in `ui/src/console/console.css`, scoped under `.dhc` so they never leak into the Airflow UI.

Fonts: the platform's UI and monospace stacks (`ui-sans-serif, system-ui, ...` and `ui-monospace, SFMono-Regular, Menlo, Consolas, ...`). The console loads no web font: the standalone page's Content-Security-Policy allows only its own origin. Sizes: body 13px/20px; meta 12px/16px; section labels 11px uppercase, letter-spacing .08em, weight 600; card titles 14px 600; page titles 20px 600; the decision's question 18px 600; code 12px/19px.

Colours, in two themes. Text colours pass WCAG AA (4.5:1) on every surface; the borders of form controls pass 3:1.

| Token | Dark | Light | Use |
|---|---|---|---|
| ground | `#0B0D12` | `#F5F6F8` | page, code blocks |
| sidebar | `#0F1218` | `#EDEFF3` | sidebar, diff headers |
| panel | `#13171F` | `#FFFFFF` | cards, side panel |
| raised | `#191E28` | `#F2F4F7` | buttons, confirmations |
| hover | `#1E2430` | `#E6E9EF` | hover, current item |
| border / border-strong | `#242A36` / `#3A4356` | `#DCE0E7` / `#A9B1BE` | separators |
| control-border | `#646E83` | `#7D8695` | inputs and selects |
| text / muted / faint | `#E4E7EE` / `#A2AAB9` / `#8A93A5` | `#1A1E26` / `#4A5262` / `#5F6879` | text |

State colours, muted, on small markers only, always with a text label next to them (never colour alone):

| State | Dark | Light | Marks |
|---|---|---|---|
| violet | `#A790D8` | `#6A4CB0` | Needs you |
| pink | `#E0708A` | `#B23A57` | Fix failed, error, a failed check, a failing test, removed lines |
| cyan | `#5DB7C0` | `#1F6F7A` | in progress, links, the focus ring |
| amber | `#D6A65C` | `#8A5A12` | reported (a data problem) |
| blue | `#7C9FE0` | `#3459A8` | fix proposed, diff hunks |
| lime | `#8DBE6C` | `#3F7A27` | passing tests, added lines, an approved contract |
| idle | `#6B7487` | `#6E7788` | ignored, not run |

As text, a state colour is mixed with the text colour (70% in a pill, 85% in a diff line), so a pill's label and a diff line pass AA on every surface, a hovered row and their own tinted background included; `idle` alone never colours text.

The category (DATA / CODE / AMBIGUOUS) is a neutral chip: colour is kept for the state. The primary button is neutral (text colour on the ground colour), not a state colour.

Themes: inside Airflow the console follows Airflow's light or dark mode (the `dark` or `light` class Airflow puts on `<html>`); on `dag-healer serve` the viewer picks system, light or dark (kept in localStorage).

Shapes and spacing: radius 6px controls, 10px cards and panels, 999px pills; spacing 4/8/12/16/24; 1px borders. Icons are inline stroke SVG, 16px, `stroke-width: 1.75`, `stroke: currentColor`, `aria-hidden="true"`. Real `<button>`, `<a href>`, `<input>` with `<label>`; icon-only buttons get `aria-label`. Targets are at least 28px, 40px on touch screens.

## The shell
- **Sidebar** (248px): the mark and "dag-healer" with the orchestrator's name; the places (Inbox, with the number of incidents that need a person; the pipelines, in the orchestrator's word: DAGs, state machines); the incidents by what they need: **Needs you** (always shown: "Nothing needs you right now."), **In progress**, **Done** (the newest few). Each item: a status dot, `pipeline / step` in monospace, its age, and its next step in a few words. At the bottom: the user and Sign out (on `dag-healer serve`) and the display settings (theme, single-key shortcuts).
- **Work area**: one view at a time (the inbox, an incident, the pipelines). Its place is in the URL, so every view can be linked and Back works.
- **Side panel** (312px): details, alerts and the timeline of an incident; what dag-healer is and how contracts work elsewhere. It collapses to a slim rail with its button or the `]` key (which also works with ⌥ and AltGr, is ignored while typing, and can be turned off). While it is hidden, the incident shows its latest event and a "Show the timeline" link, so nothing is only in the panel.
- **Narrow screens**: under 900px the panel moves below the work area; under 760px the sidebar becomes a top bar and the incident lists are reached from the inbox. Tables stack into labelled rows when the work area is narrow (a container query), not only on phones; pages never scroll sideways at 320px.
- **The Airflow DAG and task tabs** embed a pipeline and an incident in Airflow's own page, without the sidebar; the panel's content follows the work area.

## Accessibility
A skip link, a `main` landmark (on `dag-healer serve`), one `h1` per view and `h2` per card. Focus moves on purpose: to the heading of each new view, into the heading of a confirmation when it opens (its consequences are read first), to the queued notice once an action is sent, to the opposite control when the panel opens or closes, and back to the opener when a dialog closes. Dialogs trap Tab, close with Escape and make the page behind inert. One polite live region announces what changed (queued, applied). Focus rings are 2px, in the focus colour, never removed. Motion: none beyond a short transition of the skip link, and none with `prefers-reduced-motion`.

## Voice
Plain, short sentences; the product's real names in monospace next to them. Say what will happen and to whom ("Emails `data-team@example.com`, and writes the alert to the worker's log."), never what the product cannot do yet as if it could. Copy is English.

## Earlier artboards
The `.dc.html` artboards in this folder are the first exploration, a "neon ops" look with glowing graphs and status dots. That direction was dropped: no glows, no pulses, colour on small markers only, as described above. They stay for the record; this brief is the reference.
