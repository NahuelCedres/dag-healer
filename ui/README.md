# dag-healer-ui

The React UI of dag-healer: the console. The same views ship in two bundles:

- the **Airflow plugin bundle** (`main.umd.cjs`): it runs **inside the Airflow 3 UI** as a React plugin
  (`react_apps`) and talks to the healer API that the same plugin mounts on the Airflow API server under `/healer`;
- the **standalone bundle** (`standalone/index.js`): the page of `dag-healer serve`, the host that serves the API and
  the views without Airflow (for example for AWS Step Functions). See [Standalone host](#standalone-host).

The project was bootstrapped with the official Airflow tool (`dev/react-plugin-tools/bootstrap.py` from
`apache/airflow`, tag `3.3.2`) and keeps its toolchain: Vite 8 (library mode), React 19, TypeScript, ESLint, Prettier
and Vitest. The console draws its own markup and styles (`src/console/console.css`): no component library, so both
bundles stay small and the look is the same in both hosts.

## The console

The design is calm: muted colours, colour only on small state markers (a dot, a pill, a diff line), no glows or
pulses, a dark and a light theme. Contracts and decisions are explained in plain words, next to the YAML and the
identifiers they come from. `docs/design/console/DESIGN_BRIEF.md` describes it.

- **The shell** (the Healer page, and every page of `dag-healer serve`): a sidebar with the incidents by what they
  need (Needs you, In progress, Done), the work area and a collapsible side panel. Its place is in the URL
  (`?dag=<id>&incident=<id>`, `?view=pipelines&pipeline=<id>`, `?group=needs`), so every view can be linked and
  Back works.
- **The inbox** lists every incident of every pipeline this dag-healer serves, with group filters, a pipeline and a
  category filter and a search over pipelines, steps, errors and runs.
- **An incident** leads with what it needs. One that waits for a person opens with the question in plain words and
  its answers (resolve as data or as code, retry, ignore, retry repair, approve the contract); each answer shows
  what dag-healer will do, who gets an alert and which files change before it is confirmed (`GET /setup` says it for
  this deployment). "Decided by" says whether a fixed rule, the model (with its confidence) or a person decided.
  Then the evidence (what happened, each broken contract rule in plain words next to the lines of its YAML file, the
  masked sample), the fix (the test, the test results before and after, the diff, the pull request or PR.md and the
  CI note of PR.md) and, in the side panel, the details, the alerts and the timeline.
- **The pipelines** list every pipeline with its contract status and open incidents; one pipeline shows its recent
  incidents and its contract in plain words or as its file, with the approval when it is due.
- **The Airflow DAG and task tabs** embed a pipeline and an incident in Airflow's own page, without the sidebar.
- **Keyboard and screen readers:** a skip link, landmarks and headings; focus moves to the heading of each new view,
  into a confirmation's heading when it opens, to the queued notice once an action is sent, and back to the opener
  when a dialog closes (dialogs trap Tab and leave the page behind inert). `]` shows or hides the side panel (⌥ and
  AltGr work, it is ignored while typing, and it can be turned off); the panel's state and that setting are kept per
  viewer in localStorage.
- **Themes:** inside Airflow the console follows Airflow's light or dark mode (the class Airflow puts on `<html>`);
  on `dag-healer serve` the viewer picks system, light or dark.

## How Airflow loads it

The plugin (`dag_healer.airflow.plugin.HealerPlugin`) registers three `react_apps` that share one bundle,
`healer/static/main.umd.cjs?v=<version>` (relative, so it resolves against `<base href>` and works under a sub-path):

| `react_apps` name (= label) | Destination     | URL route                                                       | View                                       |
| --------------------------- | --------------- | --------------------------------------------------------------- | ------------------------------------------ |
| `Healer`                    | `nav`           | `/plugin/healer`                                                | the console shell (`src/console/Console`)  |
| `Healer incidents`          | `dag`           | `/dags/<dag_id>/plugin/healer-dag`                              | the pipeline (`src/console/Pipelines`)     |
| `Healer incident`           | `task_instance` | `/dags/<dag_id>/runs/<run_id>/tasks/<task_id>/plugin/healer-ti` | the incident (`src/console/IncidentPage`)  |

The Airflow host `import()`s the bundle once per URL and then reads `globalThis[<react_apps name>]`; the same name
is the label Airflow shows in the navigation and on the tabs. So `src/main.tsx` assigns `globalThis["Healer"]`,
`globalThis["Healer incidents"]` and `globalThis["Healer incident"]` while the module evaluates (the second and third
tabs reuse the already evaluated bundle); the names live in `src/appNames.ts` and must match
`src/dag_healer/airflow/plugin.py`. The 0.1 names (`DagHealerNav`, `DagHealerDag`, `DagHealerTaskInstance`) are
registered as aliases so a new bundle keeps working with an older plugin definition. The bundle also sets
`globalThis.AirflowPlugin` (the nav page) as a last-resort fallback, but the host resets that shared global before
every plugin import, so never rely on it. The UMD global of the module object is `DagHealer`.

Bundle contract (see `vite.config.ts`):

- Format UMD, file `dist/main.umd.cjs`, library name `DagHealer`.
- `react`, `react-dom`, `react-router-dom` and `react/jsx-runtime` are **external**, mapped to the globals the host
  exposes: `React`, `ReactDOM`, `ReactRouterDOM`, `ReactJSXRuntime`. Never bundle React: two copies of React break
  hooks.
- CSS is injected by JS (`vite-plugin-css-injected-by-js`). Every rule is scoped under `.dhc`, the root of the views,
  so nothing leaks into the Airflow UI.

### Context resolution

`src/context/pluginContext.ts` resolves `dagId`, `runId`, `taskId` and `mapIndex` in this order, never mixing values
that belong to a different DAG:

1. Scalar props from the host (Airflow 3.1+: `dagId`, `runId`, `taskId`, `mapIndex`).
2. The `taskInstance`, `dagRun` and `dag` objects some 3.3 destinations pass (REST shape: `dag_id`, `dag_run_id`,
   `task_id`, `map_index`).
3. `window.location.pathname`: `/dags/:dagId[/runs/:runId][/tasks/:taskId[/mapped/:mapIndex]]`, found after the
   deployment base path taken from `document.baseURI`.

### API calls

`src/api.ts` is a typed client for every `/healer/api/v1` endpoint (the types mirror the JSON shapes of the API;
the optional fields are the additive ones, which the server leaves out for the default Airflow instance). Every call
resolves its URL with `new URL("healer/api/v1/" + path, document.baseURI)` and sends `credentials: "same-origin"`
plus `X-Requested-With: dag-healer` (required by the API for its CSRF guard). The Airflow session cookie (`_token`,
HttpOnly) is sent by the browser; the UI never sees credentials. The plugin never sends an `Authorization` header
(Airflow reads a Bearer header before its cookie): only the standalone entry of the token backend installs an
authorization provider (`setAuthorizationProvider`). Failures throw typed errors with `status` and `detail` (the
FastAPI `detail` message): `UnauthorizedError` (401), `ForbiddenError` (403), `NotFoundError` (404), `ConflictError`
(409), `ApiError` (any other status) and `NetworkError` (status 0).

The views use plain React state and effects (`src/hooks/usePolledResource.ts`, not the host's react-query): they
refresh every 10 s, every 5 s while a queued command or contract approval is pending or an incident is in progress,
and pause while the tab is hidden. A 401 shows "Your Airflow session expired — reload the page"; a 403 on a DAG shows
"You do not have access to this DAG."; a 404 renders an empty state. Links to Airflow pages (`src/links.ts`) are built
relative to `document.baseURI` with URL-encoded segments (run ids contain `:` and `+`).

State-changing actions (resolve as data / code, ignore, retry repair, retry task, approve contract) show their
consequences and an optional note before they are confirmed (with the incident's `action_warnings` for that action),
POST the command, show it as queued and poll until the incident's `pending_commands` is empty. Only the incident's
`allowed_actions` are offered, and none when `can_edit` is false. The texts of decisions and consequences are pure
functions of the API's data (`src/console/decision.ts`, `src/console/consequences.ts`), tested on their own.

### Vocabulary

The views speak the orchestrator's words (`src/vocabulary.ts`): "DAG", "task", "try", "Retry task"... come from a
`Vocabulary` whose defaults are the Airflow words of `dag_healer.core.vocab.Vocabulary`, so every text renders exactly
as before when nobody provides one. The plugin never provides one and never calls `/me`. The standalone host reads
`/me` → `orchestrator.vocabulary` (for example "flow", "step", "attempt", "Retry step" for Fakeflow) and provides it
through `UiTextContext`, with `host: "standalone"` (its session banner says "Your session expired — reload the page").
`src/messages.ts` keeps today's texts as constants and builds them from the words (`notMonitoredMessage(words)`...);
`actionLabel(action, words)` takes the retry label from `retry_action`. Links into the orchestrator's own pages
(`links.native`, never sent for Airflow) show as an extra "Open in <label>" link; the links to the Healer views stay
client-built and relative.

## Standalone host

`dag-healer serve` serves the API under `/healer` and a small HTML shell (`dag_healer.hosting.shell`) on the plugin's
URL shapes: `/plugin/healer`, `/dags/<dag_id>/plugin/healer-dag` and
`/dags/<dag_id>/runs/<run_id>/tasks/<task_id>[/mapped/<i>]/plugin/healer-ti`. The shell sets `<base href>`, names the
auth backend in `<meta name="dag-healer-auth">` and loads `healer/static/standalone/index.js?v=<version>` as an ES
module.

- Entry: `src/standalone.tsx` (modelled on `src/dev.tsx`). It picks the view from the path after the `<base href>`
  path (`src/standalone/mode.ts`: `.../plugin/healer-ti` → task instance, `.../plugin/healer-dag` → DAG, anything
  else → the Healer page) and renders `StandaloneApp`.
- `src/standalone/StandaloneApp.tsx` asks `/me` once, then renders the **same `App`**: the console shell, whose sidebar
  shows the orchestrator's name, the user and, for the token backend, "Sign out", and speaks the orchestrator's
  words. On this host every URL shape opens the shell: the DAG tab's shape on the pipeline, the task-instance tab's
  shape on its incident.
- Token backend (`serve.auth.backend: token`): a browser cannot send a bearer token by itself, so the page shows a
  sign-in card (`TokenSignIn`). The token is checked with `GET /me` before it is kept for this tab (sessionStorage,
  or memory when storage is blocked; never localStorage, a URL or a log), and `src/standalone/auth.ts` sends it as
  `Authorization: Bearer <token>` on every API call. A 401 shows the sign-in card again; "Sign out" forgets the
  token. Since a new tab cannot carry the token, the incident page then shows PR.md in its dialog only. The other backends (`dev`, `trusted_header`, `alb_oidc`) need nothing from the page: a 401 shows the session
  banner.
- Bundle contract (see `vite.standalone.config.ts`): format ES, React 19 and ReactDOM **bundled**
  (nothing external: the shell has no host globals), CSS injected by JS, no inline script, file names without hashes
  (`index.js`, and `assets/*.js` only if the build ever splits) so rebuilds give clean diffs.
- `src/main.tsx`, `vite.config.ts` and the plugin bundle are unaffected: nothing under `src/standalone/` is reachable
  from `main.tsx` (`src/entries.test.ts` checks it).

## Requirements

Vite 8 needs Node `^20.19 || >=22.12`; the project declares `node >=22` and `pnpm@10.25.0` (`packageManager`, enabled
through corepack). If your local Node is older (for example 20.17), use the Docker commands below; they need nothing
but Docker.

## Develop

```bash
cd ui
pnpm install
pnpm dev            # http://localhost:5173
```

`pnpm dev` renders `src/dev.tsx`. Pick the view with `?mode=nav|dag|task_instance` and put the Airflow context in the
path, for example
`http://localhost:5173/dags/customers_sync/runs/<run_id>/tasks/<task_id>?mode=task_instance`.

The dev server proxies `/healer` to the Airflow API server (`http://localhost:8080`, override with
`HEALER_DEV_AIRFLOW_URL`) with `changeOrigin: false`. Log in to Airflow at `http://localhost:8080` first: cookies are
not port-scoped, so the `_token` cookie set by Airflow on `localhost` is also sent to `localhost:5173` and the proxied
GET requests are authenticated. State-changing POSTs from the dev server may be rejected by the API's Origin check
(the origin is `localhost:5173`, not Airflow's `[api] base_url`); exercise those from the real Airflow UI.

With Docker instead of a local Node (the proxy target must then be the host machine):

```bash
docker run --rm -it -p 5173:5173 -e HEALER_DEV_AIRFLOW_URL=http://host.docker.internal:8080 \
  -v "$PWD/ui:/app" -w /app node:22-alpine \
  sh -c "corepack enable && pnpm install && pnpm dev --host 0.0.0.0"
```

## Build

The supported way, from the repository root:

```bash
python scripts/tasks.py ui            # --local forces the local Node toolchain, --docker the container
```

With a suitable local Node it runs `pnpm install --frozen-lockfile`, `pnpm build` (the plugin bundle) and
`pnpm build:standalone` (the standalone bundle, after `pnpm build`, which empties `dist/`), then copies
`dist/main.umd.cjs` to `src/dag_healer/api/static/main.umd.cjs` and replaces `src/dag_healer/api/static/standalone/`
with `dist/standalone/`. The healer FastAPI app serves them at `/healer/static/main.umd.cjs` and
`/healer/static/standalone/index.js`. Without a local Node it builds both the same way in the `ui` stage of
`docker/Dockerfile` and installs them likewise.

Manual equivalent (Git Bash on Windows needs `MSYS_NO_PATHCONV=1`; quote the path, it may contain spaces):

```bash
MSYS_NO_PATHCONV=1 docker run --rm -e CI=true -e COREPACK_ENABLE_DOWNLOAD_PROMPT=0 \
  -v "$PWD/ui:/app" -w /app node:22-alpine \
  sh -c "corepack enable && pnpm install --frozen-lockfile && pnpm build && pnpm build:standalone"
cp ui/dist/main.umd.cjs src/dag_healer/api/static/main.umd.cjs
rm -rf src/dag_healer/api/static/standalone && cp -R ui/dist/standalone src/dag_healer/api/static/standalone
```

PowerShell:

```powershell
docker run --rm -e CI=true -e COREPACK_ENABLE_DOWNLOAD_PROMPT=0 -v "${PWD}\ui:/app" -w /app node:22-alpine `
  sh -c "corepack enable && pnpm install --frozen-lockfile && pnpm build && pnpm build:standalone"
Copy-Item ui\dist\main.umd.cjs src\dag_healer\api\static\main.umd.cjs
Remove-Item -Recurse -Force -ErrorAction SilentlyContinue src\dag_healer\api\static\standalone
Copy-Item -Recurse ui\dist\standalone src\dag_healer\api\static\standalone
```

`CI=true` lets pnpm run without a TTY and `COREPACK_ENABLE_DOWNLOAD_PROMPT=0` stops corepack from asking before it
downloads pnpm. On Windows a bind-mounted `node_modules` is slow (the first install takes minutes); mounting a named
volume over it (`-v dag-healer-ui-modules:/app/node_modules`) makes repeated builds take seconds.

Output in `ui/dist/` (gitignored): `main.umd.cjs` (about 130 kB, 36 kB gzipped; React is not bundled), plus
`main.d.ts` and `main.d.ts.map`, and `standalone/index.js` (about 320 kB, 95 kB gzipped; React included). **Commit both rebuilt bundles** in `src/dag_healer/api/static/` (`main.umd.cjs` and `standalone/`): the
wheel ships them, so end users install dag-healer without Node. `hatch_build.py` refuses a wheel without
`main.umd.cjs` and only warns without `standalone/index.js` (then `dag-healer serve` answers 503 on its pages). The
CI job "UI bundle and wheel" builds both, warns when the committed bundles differ from that build and checks that the
wheel ships them byte for byte.

## Quality checks

```bash
pnpm lint           # eslint + tsc (tsconfig.app.json)
pnpm test           # vitest (happy-dom)
pnpm format:check   # prettier; `pnpm format` rewrites
```

Run them in the same container by replacing `pnpm build` in the command above. The CI job "UI bundle and wheel" runs
them too. Run them inside `ui/`: from another directory corepack does not see the pnpm version pinned in
`package.json` (or name it, as in `corepack pnpm@10.25.0 --dir ui test`).

## Layout

```
src/main.tsx                  plugin bundle entry: PluginComponentProps, the three components, globalThis registration
src/appNames.ts               react_apps names (labels + globalThis keys) and the 0.1 aliases
src/App.tsx                   App({mode, text?, user?, onSignOut?, ...props}): UiText provider, error boundary, the
                              console shell or an embedded view
src/api.ts                    typed client of /healer/api/v1 (shapes, endpoints, typed errors, authorization provider)
src/links.ts                  hrefs of the Healer views (task-instance / DAG tabs, Healer page) and PR.md
src/vocabulary.ts             orchestrator words (Airflow defaults), UiTextContext, vocabularyFrom(/me)
src/hooks/usePolledResource.ts polling hook (interval, pause when hidden, abort, keep data on error)
src/model.ts, format.ts       pure helpers: sorting, native links, action warnings, formatting
src/messages.ts               shared user-facing messages (constants + vocabulary builders)
src/diff.ts                   unified diff parser of the fix's diff
src/context/pluginContext.ts  props -> objects -> URL context resolver
src/components/ErrorBoundary  keeps a crash of the views inside them
src/console/                  the console:
  Console.tsx                   the shell (sidebar, work area, side panel) and the embedded views of the tabs
  Sidebar, Inbox, IncidentPage, PrimaryCard (the decision), Evidence, Fix, Details (timeline), Pipelines,
  ContractCard, Confirm (consequences + note), Layout (side panel, ] key), Modal, Frame (theme scope, dialogs),
  nav and route (the console's place in the URL), focus, data (shared resources), prefs (panel, shortcuts, theme)
  labels, decision, consequences, events, texts, time: pure functions behind the texts (+ *.test.ts)
  console.css                   the calm look: tokens of both themes, layout, narrow screens
src/standalone.tsx            standalone bundle entry (`pnpm build:standalone`)
src/standalone/               StandaloneApp, TokenSignIn, auth (token), mode (path -> view), title
src/test/                     fetch mock, fixtures and jest-dom typings for the tests
src/global.d.ts               globalThis declarations (AirflowPlugin)
src/dev.tsx                   `pnpm dev` entry (not part of any bundle)
```

## Upgrading dependencies

Dependencies marked as external are shared with the host Airflow UI: keep `react` and `react-dom` on the same major
version as the Airflow UI you target (React 19 in Airflow 3.3). The standalone bundle includes its own copy of the
same versions, so both bundles render the views with the same React. `pnpm-lock.yaml` must stay in sync with
`package.json` because builds use `pnpm install --frozen-lockfile`.
