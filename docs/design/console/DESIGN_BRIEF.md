# dag-healer Console: design brief for artboard authors

## Product
dag-healer attaches to EXISTING data pipelines (Airflow today, AWS Step Functions and others through connectors), detects task failures, classifies each as DATA / CODE / AMBIGUOUS, and heals them: CODE gets a failing pytest test + a minimal fix + a PR (or PR.md); DATA gets the violated data-contract rule with a masked sample and an alert; AMBIGUOUS waits for a human decision (resolve as data / as code / ignore). This canvas designs the STANDALONE console (`dag-healer serve`): one UI for every orchestrator. The Airflow plugin keeps its own native look; this console is the product's own home.

## Direction
"Neon ops": the dense, dark, three-pane layout of a modern agent IDE (left sidebar with workspaces and live lists, a center area of tabs and split panes with terminal-like agent logs, a right inspector panel, a thin status bar at the bottom), combined with a NEON look for showing DAGs: pipeline graphs glow like a circuit board, nodes and edges lit by their state. Original design: do not copy any other product's name, logo or branding. Product name in UI: "dag-healer" (lowercase).

## Design system (use exactly these values)
Fonts (one Google Fonts link in `<helmet>`):
`<link rel="stylesheet" href="https://fonts.googleapis.com/css2?family=Geist:wght@400;500;600;700&family=Geist+Mono:wght@400;500;600&display=swap">`
- UI: `'Geist', ui-sans-serif, system-ui, sans-serif`. Code, IDs, logs, numbers in tables: `'Geist Mono', ui-monospace, 'SFMono-Regular', Consolas, monospace`.
- Sizes: body UI 13px/20px; meta 12px/16px; section labels 11px uppercase, letter-spacing .08em, weight 600; panel titles 14px 600; page titles 20px 600; mono log text 12.5px/20px.

Colors (dark only):
- ground `#07090D` (app background, graph canvas), sidebar `#0B0E14`, panel `#0F131B`, raised `#151A24`, hover/selected `#1B2130`, border `#212836`, border-strong `#2C3446`
- text `#E7EAF2`, text-muted `#A3ACBE`, text-faint `#7D879B` (never darker than this for text)
- NEON state colors (each also has a glow = same color at ~45% alpha):
  - cyan `#2DE2E6` running / active / links / focus ring / selection
  - pink `#FF3EA5` FAILED (the failing node, error lines)
  - lime `#B6F35C` succeeded / HEALED / tests passing
  - amber `#FFB547` category DATA
  - blue `#5B9BFF` category CODE
  - violet `#B28CFF` category AMBIGUOUS / needs a human
  - idle gray `#5B6578` queued, skipped, not run
- Pink vs lime also differ strongly in lightness; always pair state color with a text label or icon shape (check, cross, clock, question), never color alone.

Neon treatment:
- Glowing node: `border: 1px solid <c>; box-shadow: 0 0 0 1px <c>33, 0 0 18px <c>66, inset 0 0 14px <c>22; background: #0F131B;` text in `#E7EAF2`, a small state glyph in `<c>`.
- Glowing edge: arrow svg (diagrams.md) with `stroke: <c>` and `filter: drop-shadow(0 0 4px <c>)` in its style; dimmed edges `#2C3446` without glow.
- Status dot: 8px circle, background `<c>`, `box-shadow: 0 0 8px <c>`.
- Graph canvas background: `#07090D` with a dotted grid `background-image: radial-gradient(#1A2030 1px, transparent 1px); background-size: 20px 20px` (put it in a `<helmet><style>` class, never inline).
- Glow is for state (DAG nodes, edges, dots, the active item, one hero number); chrome stays flat and quiet. No gradient washes, no left-border accent cards, no emoji.

Shapes and spacing: radius 6px controls, 10px cards/panels, 999px pills; spacing scale 4/8/12/16/24/32; 1px borders `#212836`; icons are inline stroke SVG 16px, `stroke-width: 1.75`, `stroke: currentColor`, `fill: none`, `aria-hidden="true"` when decorative. Real `<button>`, `<a href>`, `<input>` + `<label>`; icon-only buttons get `aria-label`. Hit targets in the phone artboard >= 44px.

## Shared shell (desktop artboards)
Every desktop artboard is a PAGE (fluid root: no px root width, `min-height: 100vh` allowed because it is a whole-screen app shell) with:
1. Left sidebar (width 264px, background `#0B0E14`, right border): product mark (inline SVG: three small connected nodes, one glowing cyan) + "dag-healer"; nav items with icons: Incidents (badge 4), Pipelines, Contracts, Connectors, Search (with "Ctrl K" hint); section "CONNECTORS": "Airflow · prod" (lime dot), "Step Functions · us-east-1" (lime dot), "Dagster" (gray dot, "not connected"); section "NEEDS YOU" (violet count 1): inventory_report / build_report; section "HEALING" (cyan count 1): orders-ingest / ValidateOrder; section "RECENT": customers_sync / transform (repaired, lime), sales_daily_etl / transform_sales (data, amber). Each list item: status dot, `pipeline / step` in 13px, a second line with a mono chip for the repo (`analytics-etl`) or branch, a PR icon when there is one, and a relative time right-aligned (2m, 6m, 9m, 14m). Mark the artboard's own nav item active (selected background + cyan text).
2. Main area (flex 999 1 560px; min-width 0) with a top tab strip (36px tall, `#0B0E14`, tabs with small state dot + close icon, a "+" button), the artboard's content, and a 28px status bar at the bottom: "worker online · polling 5s" (lime dot), "Claude Opus 5.5" + a thin usage meter "41% of daily token budget", "Airflow" and "Step Functions" health dots, "4 incidents · 1 needs you" right-aligned.
3. The layout must wrap at phone width (sidebar stacks on top, panes stack, wide tables scroll in an `overflow-x: auto` box).
Write the shell markup out in each artboard (each artboard is self-contained), keeping it consistent.

## Demo data (use these exact names and facts; no invented metrics)
Repo `analytics-etl` (an existing Airflow repo). Connectors: Airflow 3.3.2 "prod" (plugin installed, listener + REST polling, 3 DAGs monitored), Step Functions us-east-1 (EventBridge rule + execution history, 2 state machines monitored), Dagster (not connected).
Incidents (newest first):
1. inventory_report / build_report: AMBIGUOUS, status "needs you" (waiting_human). Error `KeyError: 'qty'`. Contract for inventory_report is a DRAFT (not approved), so dag-healer will not decide alone. Evidence: today's inventory.csv has columns sku, warehouse, warehouse_code, quantity, updated_at; the draft contract expects sku, warehouse, qty, updated_at. So: `qty` missing, `quantity` and `warehouse_code` new. 2m ago. Source: listener.
2. orders-ingest (Step Functions state machine) / ValidateOrder (Lambda task): CODE, status "healing" (repairing). Execution history shows `TaskFailed` with error `KeyError` and cause `'currency'`, raised in the Lambda handler `src/orders/validate.py` line 42, function `validate_order` (mapped to code through the repo's SAM template). Agent currently writing a failing test. 6m ago. Source: EventBridge.
3. customers_sync / transform: CODE, status "repaired". Root cause: `parse_signup_date` in `src/etl/customers.py` uses `datetime.strptime(value, "%Y-%m-%d")` and fails on `2026-10-02T14:03:00Z`, a value the approved contract allows. Fix: accept ISO-8601 timestamps (handle trailing Z) and keep returning a date. Test added: `tests/test_customers_signup_date.py` (the repo had no tests; dag-healer created `tests/`). Diff: 2 files changed, +21 -2. pytest before: 1 failed, 1 passed; after: 2 passed. Branch `healer/customers_sync-transform-3f9a2c1d`, PR.md ready (no GitHub token configured) or "PR #1" if shown with GitHub. 9m ago. Source: REST.
4. sales_daily_etl / transform_sales: DATA, status "reported". Error `KeyError: 'amount'`. Approved contract violated: `missing_column amount` and `unexpected_column amt`; 4 rows with empty `amt`. Masked sample columns: order_id, customer_email (masked like `h:5f2c1a9e0b`), amt, currency, order_date. Alert emailed to data-team@example.com. 14m ago.
DAG structures:
- sales_daily_etl: extract_sales -> transform_sales -> load_sales (transform failed today).
- customers_sync: extract -> transform -> load (transform failed, now repaired; load not run).
- inventory_report: get_inventory -> build_report -> publish_report.
- orders-ingest (Step Functions): ReceiveOrder -> ValidateOrder -> (Map) EnrichItems -> WriteOrders; ValidateOrder failed; Catch -> NotifyOps.
Masked values look like `h:` + 10 hex chars, or `[email]`, `[phone]`. Never show a raw email or phone.

## The .dc.html format (essentials; the full rules were given by the canvas type)
- One self-contained file: `<!doctype html><html lang="en"><head><meta charset="utf-8"><title>…</title><script src="./support.js"></script></head><body><x-dc> <helmet><style>body{margin:0} …</style> + the font link</helmet> …markup… </x-dc><script type="text/x-dc" data-dc-script data-props='{…,"$preview":{"width":W,"height":H}}'>class Component extends DCLogic { renderVals() { return {…}; } }</script></body></html>`. Keep the `<script src="./support.js"></script>` line EXACTLY.
- Close every non-void element; quote every attribute. All UI is markup inside `<x-dc>`, never built by script (no innerHTML, no appendChild, no window.X). No network except the Google Fonts link. No iframe/object/embed. No global keydown handlers.
- Inline `style="…"` for element styles; `<helmet><style>` only for page basics (body, a/a:hover colors, the dotted-grid class, keyframes, scrollbar styling, hover states).
- `{{hole}}` is a dotted lookup into `renderVals()` only, never an expression. Repeats: `<sc-for list="{{items}}" as="item" hint-placeholder-count="3">…{{item.x}}…</sc-for>`. Branches: `<sc-if value="{{flag}}" hint-placeholder-val="{{ true }}">…</sc-if>`. Events: `onClick="{{ handler }}"` with the function returned from renderVals; per-item handlers are attached to each item in renderVals; state via `this.state` / `this.setState`. Conditional styling in loops: precompute per-item values in renderVals.
- Copy the viewer may retype stays literal markup (not props). `data-props` holds only 1-3 real levers (for example a `glow` intensity enum or a `density` toggle), single-quoted JSON, plus `$preview`.
- PAGE artboards: fluid root, `max-width` not needed for an app shell, flex rows with `flex-wrap: wrap`, content `flex: 999 1 560px; min-width: 0`; grids use `repeat(auto-fit, minmax(min(240px, 100%), 1fr))`.
- DIAGRAMS (the DAG graphs): follow diagrams.md exactly: one host `<div>` with `position: relative` and px `width`/`height` (inside a horizontally scrollable box so the page stays fluid); every node is a separately placed absolute `<div>` card with inline width/height/box-sizing and its label as text + inline tags only (one `<span>`); every edge is its own small arrow `<svg>` per straight segment copied from the example's shape (unique marker ids `dc-arrow-head-filled-N`); all numbers computed on a grid before writing; paint order: background bands, arrows, cards, labels. Write every node and edge as literal markup (no sc-for for diagram elements).
