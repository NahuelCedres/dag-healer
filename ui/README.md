# dag-healer-ui

The React UI of dag-healer. It runs **inside the Airflow 3 UI** as a React plugin (`react_apps`) and talks to the
healer API that the same plugin mounts on the Airflow API server under `/healer`.

The project was bootstrapped with the official Airflow tool (`dev/react-plugin-tools/bootstrap.py` from
`apache/airflow`, tag `3.3.2`) and keeps its toolchain: Vite 8 (library mode), React 19, Chakra UI v3, TypeScript,
ESLint, Prettier and Vitest.

## How Airflow loads it

The plugin (`dag_healer.airflow.plugin.HealerPlugin`) registers three `react_apps` that share one bundle,
`healer/static/main.umd.cjs?v=<version>` (relative, so it resolves against `<base href>` and works under a sub-path):

| `react_apps` name (= label) | Destination     | URL route                                                       | View                              |
| --------------------------- | --------------- | --------------------------------------------------------------- | --------------------------------- |
| `Healer`                    | `nav`           | `/plugin/healer`                                                | `src/views/HealerPage.tsx`        |
| `Healer incidents`          | `dag`           | `/dags/<dag_id>/plugin/healer-dag`                              | `src/views/DagPanel.tsx`          |
| `Healer incident`           | `task_instance` | `/dags/<dag_id>/runs/<run_id>/tasks/<task_id>/plugin/healer-ti` | `src/views/TaskInstancePanel.tsx` |

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
- CSS is injected by JS (`vite-plugin-css-injected-by-js`).
- Theme: `ChakraProvider` uses `globalThis.ChakraUISystem` (provided by the host) and falls back to the local
  default system during development.

### Context resolution

`src/context/pluginContext.ts` resolves `dagId`, `runId`, `taskId` and `mapIndex` in this order, never mixing values
that belong to a different DAG:

1. Scalar props from the host (Airflow 3.1+: `dagId`, `runId`, `taskId`, `mapIndex`).
2. The `taskInstance`, `dagRun` and `dag` objects some 3.3 destinations pass (REST shape: `dag_id`, `dag_run_id`,
   `task_id`, `map_index`).
3. `window.location.pathname`: `/dags/:dagId[/runs/:runId][/tasks/:taskId[/mapped/:mapIndex]]`, found after the
   deployment base path taken from `document.baseURI`.

### API calls

`src/api.ts` is a typed client for every `/healer/api/v1` endpoint (the types mirror the JSON shapes of the API).
Every call resolves its URL with `new URL("healer/api/v1/" + path, document.baseURI)` and sends
`credentials: "same-origin"` plus `X-Requested-With: dag-healer` (required by the API for its CSRF guard). The Airflow
session cookie (`_token`, HttpOnly) is sent by the browser; the UI never sees credentials. Failures throw typed
errors with `status` and `detail` (the FastAPI `detail` message): `UnauthorizedError` (401), `ForbiddenError` (403),
`NotFoundError` (404), `ConflictError` (409), `ApiError` (any other status) and `NetworkError` (status 0).

The views use plain React state and effects (`src/hooks/usePolledResource.ts`, not the host's react-query): they
refresh every 10 s, every 5 s while a queued command or contract approval is pending, and pause while the tab is
hidden. A 401 shows "Your Airflow session expired — reload the page"; a 403 on a DAG shows "You do not have access to
this DAG."; a 404 (for example an older plugin without a route) renders an empty state. Links to Airflow pages
(`src/links.ts`) are built relative to `document.baseURI` with URL-encoded segments (run ids contain `:` and `+`).

State-changing actions (resolve as data / code, ignore, retry repair, retry task, approve contract) open a
confirmation dialog with an optional note, POST the command, show "Queued — the worker will apply it shortly" and
poll until the incident's `pending_commands` is empty. The action bar only shows the incident's `allowed_actions`
and is hidden when `can_edit` is false. Colors come from the host theme through semantic tokens; only badges pick a
`colorPalette` (category: data orange, code blue, ambiguous purple; status: waiting_human yellow, repaired green,
repair_failed/error red, reported orange, others gray), see `src/palettes.ts`.

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
python scripts/tasks.py ui
```

It runs the same container command shown below and copies the result to
`src/dag_healer/api/static/main.umd.cjs`, the file the healer FastAPI app serves at `/healer/static/main.umd.cjs`.

Manual equivalent (Git Bash on Windows needs `MSYS_NO_PATHCONV=1`; quote the path, it may contain spaces):

```bash
MSYS_NO_PATHCONV=1 docker run --rm -e CI=true -e COREPACK_ENABLE_DOWNLOAD_PROMPT=0 \
  -v "$PWD/ui:/app" -w /app node:22-alpine \
  sh -c "corepack enable && pnpm install --frozen-lockfile && pnpm build"
cp ui/dist/main.umd.cjs src/dag_healer/api/static/main.umd.cjs
```

PowerShell:

```powershell
docker run --rm -e CI=true -e COREPACK_ENABLE_DOWNLOAD_PROMPT=0 -v "${PWD}\ui:/app" -w /app node:22-alpine `
  sh -c "corepack enable && pnpm install --frozen-lockfile && pnpm build"
Copy-Item ui\dist\main.umd.cjs src\dag_healer\api\static\main.umd.cjs
```

`CI=true` lets pnpm run without a TTY and `COREPACK_ENABLE_DOWNLOAD_PROMPT=0` stops corepack from asking before it
downloads pnpm. On Windows a bind-mounted `node_modules` is slow (the first install takes minutes); mounting a named
volume over it (`-v dag-healer-ui-modules:/app/node_modules`) makes repeated builds take seconds.

Output in `ui/dist/` (gitignored): `main.umd.cjs` (about 382 kB, 102 kB gzipped; Chakra UI and Emotion are bundled,
React is not), plus `main.d.ts` and `main.d.ts.map`. **Commit the rebuilt bundle** in
`src/dag_healer/api/static/main.umd.cjs`: the wheel ships it, so end users install dag-healer without Node, and
`hatch_build.py` only rebuilds it when it is missing.

## Quality checks

```bash
pnpm lint           # eslint + tsc (tsconfig.app.json)
pnpm test           # vitest (happy-dom)
pnpm format:check   # prettier; `pnpm format` rewrites
```

Run them in the same container by replacing `pnpm build` in the command above.

## Layout

```
src/main.tsx                  bundle entry: PluginComponentProps, the three exported components, globalThis registration
src/appNames.ts               react_apps names (labels + globalThis keys) and the 0.1 aliases
src/App.tsx                   App({mode, ...props}): Chakra + color mode providers, error boundary, view switch
src/api.ts                    typed client of /healer/api/v1 (shapes, endpoints, typed errors)
src/links.ts                  hrefs of Airflow routes (task-instance / DAG tabs, Healer page) and PR.md
src/hooks/usePolledResource.ts polling hook (interval, pause when hidden, abort, keep data on error)
src/model.ts, format.ts       pure helpers: counts, sorting, decision from events, labels and formatting
src/palettes.ts, messages.ts  badge color palettes and shared user-facing messages
src/diff.ts                   unified diff parser used by DiffView
src/context/pluginContext.ts  props -> objects -> URL context resolver
src/context/colorMode/        color mode provider from the template (next-themes)
src/components/               badges, DiffView, MaskedSampleTable, ViolationsTable, IncidentTimeline, EventTimeline,
                              Contracts (table, rules, approve button), ConfirmDialog, ApiErrorNotice, ...
src/views/                    HealerPage, DagPanel, TaskInstancePanel (+ *.test.tsx)
src/test/                     fetch mock, fixtures and jest-dom typings for the tests
src/global.d.ts               globalThis declarations (ChakraUISystem, AirflowPlugin)
src/dev.tsx                   `pnpm dev` entry (not part of the bundle)
```

## Upgrading dependencies

Dependencies marked as external are shared with the host Airflow UI: keep `react` and `react-dom` on the same major
version as the Airflow UI you target (React 19 in Airflow 3.3). `pnpm-lock.yaml` must stay in sync with
`package.json` because builds use `pnpm install --frozen-lockfile`.
