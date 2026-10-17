# dag-healer development stack (Docker)

A local Apache Airflow 3.3.2 deployment with the dag-healer plugin installed, used for the
demo, the Airflow-dependent tests and manual testing. It is based on the official reference
`docker-compose.yaml` of Airflow 3.3.2, switched to the **LocalExecutor** (no Redis, Celery
worker or Flower).

> Development only. Every secret in `docker/.env.example` is a fixed, publicly known value.
> Do not expose these ports beyond your machine.

## Quick start

All commands run from the repository root and work on Windows, macOS and Linux (paths with
spaces included). `make <target>` is an optional shortcut for the same commands.

```bash
python scripts/tasks.py up            # first run builds the image (UI + wheel + Airflow), then waits
python scripts/tasks.py ps            # container status
python scripts/tasks.py logs -f airflow-scheduler
python scripts/tasks.py healer doctor # any dag-healer command, inside the api-server
python scripts/tasks.py airflow dags list
python scripts/tasks.py down          # stop (keeps the databases)
python scripts/tasks.py reset --yes   # stop, delete volumes and .demo/
```

`up` first runs `prepare-demo-repo`: it copies `example_existing_repo/` to `.demo/repo/`,
turns it into a git repository (branch `main`, commit "Initial legacy repo") and creates
`.demo/logs/`. It also creates `docker/.env` from `docker/.env.example` when it is missing.

Then open:

| What | URL | Credentials |
|---|---|---|
| Airflow UI / REST API | http://localhost:8080 | `admin` / `admin` (Admin), `viewer` / `viewer` (Viewer, read-only) |
| dag-healer page | http://localhost:8080/plugin/healer | same as Airflow |
| Mailpit (alert e-mails) | http://localhost:8025 | none |

## Services

| Service | Image | Ports | Role |
|---|---|---|---|
| `postgres` | `postgres:16` | - | Airflow metadata DB `airflow` + dag-healer DB `healer` (created by `initdb/01-healer.sql`) |
| `mailpit` | `axllent/mailpit:v1.31.6` | 8025 (UI), 1025 (SMTP) | SMTP sink for the alerts |
| `airflow-init` | `dag-healer/airflow:dev` | - | One-shot: builds the image, migrates the DB, creates `admin` and `viewer` (and lets the Viewer role read the plugin list, which the UI needs to show the dag-healer views), ensures the `healer` DB exists, fixes ownership of the mounted directories |
| `airflow-apiserver` | `dag-healer/airflow:dev` | 8080 | UI, REST API, Execution API and the `/healer` FastAPI app of the plugin |
| `airflow-scheduler` | `dag-healer/airflow:dev` | - | Scheduler; with the LocalExecutor it also runs the tasks (and the dag-healer listener) |
| `airflow-dag-processor` | `dag-healer/airflow:dev` | - | Parses `/opt/airflow/repo/dags` |
| `airflow-triggerer` | `dag-healer/airflow:dev` | - | Deferrable operators |
| `healer-worker` | `dag-healer/airflow:dev` | - | Profile `worker`: `dag-healer worker` (reconcile, triage, report, repair) |
| `tests` | `dag-healer/airflow:dev` | - | Profile `tests`: `pytest tests/airflow` inside the image |

Only `airflow-init` has a `build:` section; the other services reuse the image it builds
(`pull_policy: never`), so `docker compose build` builds it exactly once.

### Profiles

```bash
python scripts/tasks.py up --worker          # core services + healer-worker
python scripts/tasks.py test-airflow         # rebuild the image, run tests/airflow, remove the container
python scripts/tasks.py test-airflow --no-build -k auth   # extra arguments go to pytest
```

The `tests` service is standalone: SQLite metadata DB and SimpleAuthManager
(`admin:admin,viewer:viewer`, `ALL_ADMINS=false`) under `/tmp/airflow-tests`, with the
repository's `tests/` bind-mounted read-only at `/opt/dag-healer/tests` (so test edits need
no rebuild; code changes do, because dag-healer is installed in the image as a wheel).

## Image (`docker/Dockerfile`)

Multi-stage, build context = repository root:

1. `ui` - `node:22-alpine`, `corepack enable`, `pnpm install --frozen-lockfile`, `pnpm build`
   (Vite 8 needs Node `^20.19 || >=22.12`, so the host Node is never required).
2. `ui-export` - `scratch` with only `ui/dist`, for
   `docker build -f docker/Dockerfile --target ui-export --output type=local,dest=<dir> .`
   (`python scripts/tasks.py ui` uses it and copies the bundle to
   `src/dag_healer/api/static/main.umd.cjs`).
3. `wheel` - `python:3.12-slim`, copies the fresh bundle into the package and runs
   `pip wheel --no-deps`.
4. final - `apache/airflow:3.3.2` + `pip install "apache-airflow==3.3.2" <wheel> pytest
   -r example_existing_repo/requirements.txt` (Airflow pinned so pip never moves it).

Build arguments: `AIRFLOW_VERSION` (3.3.2), `EXTRA_REQUIREMENTS` (requirements file relative
to the build context, installed when it exists; empty to skip), `INSTALL_TEST_TOOLS`
(`true` installs pytest).

## Mounts and paths

| Host | Container | Notes |
|---|---|---|
| `.demo/repo` (`HEALER_REPO_DIR`) | `/opt/airflow/repo` | The user's repository; DAGs folder `/opt/airflow/repo/dags`, config `/opt/airflow/repo/healer.yaml` |
| `.demo/logs` (`HEALER_LOGS_DIR`) | `/opt/airflow/logs` | Airflow task logs |
| volume `healer-home` | `/opt/airflow/healer` | `HEALER_HOME`: worker clones, `PR.md` / `fix.patch` |
| volume `postgres-db-volume` | `/var/lib/postgresql/data` | Both databases |

`PYTHONPATH=/opt/airflow/repo` makes the repository root importable for DAG code
(`from src.etl import sales`), except in `healer-worker`, which never imports user code.
Git inside the containers runs with `safe.directory=*`, `core.autocrlf=false` and
`core.filemode=false` (via `GIT_CONFIG_*`), because the bind mount belongs to another uid.

## Environment

Shared by every Airflow container (`x-airflow-common-env`): LocalExecutor, FabAuthManager,
`AIRFLOW__CORE__EXECUTION_API_SERVER_URL=http://airflow-apiserver:8080/execution/`,
`AIRFLOW__API__BASE_URL=http://localhost:8080`, DAGs not paused at creation, no examples,
`HEALER_CONFIG`, `HEALER_HOME`, `HEALER_DATABASE_URL=postgresql+psycopg2://airflow:airflow@postgres/healer`,
`HEALER_API_URL=http://airflow-apiserver:8080`, `HEALER_INGEST_TOKEN`,
`HEALER_SMTP_HOST=mailpit`, `HEALER_SMTP_PORT=1025`.

Only `airflow-apiserver` and `healer-worker` also receive `HEALER_AIRFLOW_USERNAME` /
`HEALER_AIRFLOW_PASSWORD`, `ANTHROPIC_API_KEY`, `GITHUB_TOKEN`, `GITHUB_REPO` and
`HEALER_LLM_PROVIDER`: with the LocalExecutor the scheduler runs the DAGs' own task code,
which has no business seeing those credentials. The optional ones are passed through from
`docker/.env` or the shell and stay unset when they are not defined.

Without an Anthropic key, uncomment `HEALER_LLM_PROVIDER=fake` in `docker/.env`.

## Linux notes

Set `AIRFLOW_UID` in `docker/.env` to your uid (`id -u`); `scripts/tasks.py` does it when it
creates the file. Otherwise files written by the containers under `.demo/` belong to uid 50000.

## Troubleshooting

- `python scripts/tasks.py logs airflow-init` - migrations, user creation, ownership fixes.
- `python scripts/tasks.py wait` - polls http://localhost:8080/api/v2/monitor/health.
- Docker Desktop needs at least 4 GB of memory and 2 CPUs for Airflow.
- Ports 8080, 8025 or 1025 already in use: stop the other program, or edit `docker/compose.yaml`.
- Postgres volumes created before `initdb/` existed have no `healer` database; `airflow-init`
  creates it on the next `up`.
