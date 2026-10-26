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
python scripts/tasks.py demo          # the whole day-2 demo (see "Demo" below); or: make demo
python scripts/tasks.py up            # first run builds the image (UI + wheel + Airflow), then waits
python scripts/tasks.py init          # dag-healer init on the mounted repo (the worker waits for it)
python scripts/tasks.py ps            # container status
python scripts/tasks.py logs -f airflow-scheduler
python scripts/tasks.py doctor        # dag-healer doctor in healer-worker, with the test sandbox
python scripts/tasks.py healer contracts list   # any dag-healer command, inside the api-server
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
| `healer-worker` | `dag-healer/airflow:dev` | - | `dag-healer worker` (reconcile, triage, report, repair); starts with the stack and waits until `healer.yaml` exists. The only service with the engine socket: it starts the test sandbox containers (see "Test sandbox") |
| `tests` | `dag-healer/airflow:dev` | - | Profile `tests`: `pytest tests/airflow` inside the image |

Only `airflow-init` has a `build:` section; the other services reuse the image it builds
(`pull_policy: never`), so `docker compose build` builds it exactly once.

### Profiles

`healer-worker` is a default service (no profile). `python scripts/tasks.py up --no-worker`
starts everything else. The only profile is `tests`:

```bash
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
   -r example_existing_repo/requirements.txt` (Airflow pinned so pip never moves it) + the
   docker CLI (`COPY --from=docker:29.8.2-cli`, a static binary without an engine), which
   `healer-worker` uses to start the test sandbox. The same image is the sandbox image, so it
   must hold no secrets.

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
| `/var/run/docker.sock` (`HEALER_DOCKER_SOCKET`) | `/var/run/docker.sock` | `healer-worker` only: the engine socket for the test sandbox |

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
which has no business seeing those credentials. The scheduler only gets what the listener
needs to post failure events: `HEALER_API_URL` and `HEALER_INGEST_TOKEN`. The optional
credentials are passed through from `docker/.env` or the shell and stay unset when they are
not defined. For real pull requests, `GITHUB_TOKEN` is a fine-grained personal access token
limited to the one repository in `GITHUB_REPO`: [docs/github/README.md](../docs/github/README.md)
shows how to create it and protect the base branch, and `python scripts/tasks.py doctor` checks
both.

`healer-worker` additionally sets `HEALER_FAKE_LLM_DIR=/opt/airflow/repo/fixtures/fake_llm`,
the git identity of its fix commits (`GIT_AUTHOR_NAME` / `GIT_COMMITTER_NAME` `dag-healer`,
e-mail `dag-healer@localhost`), an empty `PYTHONPATH` and
`HEALER_SANDBOX_IMAGE=${HEALER_IMAGE:-dag-healer/airflow:dev}` (the image of the test
sandbox), and joins the group `HEALER_DOCKER_GID` (default 0) of the engine socket.

### LLM provider

`HEALER_LLM_PROVIDER` defaults to **`fake`**: an offline provider that follows the
deterministic policy and replays the YAML scripts in `<repo>/fixtures/fake_llm`, so the demo
needs no API key. **To use Claude, set `HEALER_LLM_PROVIDER=anthropic` and
`ANTHROPIC_API_KEY` in `docker/.env`**, then `python scripts/tasks.py up` (the containers are
recreated with the new environment).

## Test sandbox

A CODE repair runs pytest several times: the existing suite, the generated test, then the test
and the suite after each fix. In this stack each run is a fresh container of
`dag-healer/airflow:dev`, started by `healer-worker` with the image's docker CLI on the host's
engine, next to the stack's own containers (not inside `healer-worker`). The `healer.yaml` that
`dag-healer init` writes selects it: `repo.sandbox.backend: container`, with
`repo.sandbox.image` taken from `HEALER_SANDBOX_IMAGE`. Each run gets a copy of the clone without
`.git` (streamed into the container: no host folder is mounted), no network, a read-only root
file system, no capabilities, limits on memory, CPUs and processes, and the image's user
`airflow` (uid 50000), which finds pytest and the example repository's libraries in the image.
[docs/security.md](../docs/security.md) (section 4.3) lists what the sandbox enforces and what
it does not.

What the stack adds for it, to `healer-worker` only:

- the engine socket, `${HEALER_DOCKER_SOCKET:-/var/run/docker.sock}` mounted at
  `/var/run/docker.sock`;
- the socket's group, `group_add: ["${HEALER_DOCKER_GID:-0}"]`;
- `HEALER_SANDBOX_IMAGE=${HEALER_IMAGE:-dag-healer/airflow:dev}`. The sandbox never pulls the
  image: it uses the one that `airflow-init` builds.

> The engine socket is root-equivalent: whoever can use it can start a privileged container and
> read or change every file of the engine's host (on Docker Desktop and OrbStack, their Linux
> VM). The stack gives it to `healer-worker` only, never to the Airflow services (the scheduler
> runs the DAGs' task code), and the sandbox containers never get it.

`python scripts/tasks.py doctor` runs `dag-healer doctor` in `healer-worker`: its `sandbox` line
must be `[ OK ]`, after a probe container has shown a non-root user, no network and an importable
pytest. The demo runs the same check before it triggers the DAGs. In the api-server, which has no
socket and no `HEALER_SANDBOX_IMAGE`, the check always fails.

- **Docker Desktop and OrbStack** (macOS, Windows): usually nothing to set. Inside the
  containers the socket belongs to root (OrbStack: `root:root`, mode 0660), and group 0 is the
  primary group of every Airflow container. If `healer-worker` cannot use it, look at its owner
  and mode in the container (`python scripts/tasks.py shell healer-worker`, then
  `ls -ln /var/run/docker.sock`) and set `HEALER_DOCKER_GID` to its group.
- **Linux (Docker Engine)**: the socket belongs to the `docker` group. When `scripts/tasks.py`
  creates `docker/.env`, it writes that group's id to `HEALER_DOCKER_GID`. For a `docker/.env`
  created earlier, add the line yourself: `HEALER_DOCKER_GID=` followed by the output of
  `stat -c %g /var/run/docker.sock`.
- **Rootless Docker or Podman**: point `HEALER_DOCKER_SOCKET` at the user's socket, for example
  `/run/user/1000/docker.sock`, or `/run/user/1000/podman/podman.sock` after
  `systemctl --user enable --now podman.socket`, and set `HEALER_DOCKER_GID` to its group. The
  docker CLI of the image talks to Podman's Docker-compatible API, so `repo.sandbox.engine` stays
  `docker`. A rootless engine is the safer choice: neither its socket nor its containers give
  root on the host.

`healer-worker` prints "cannot use /var/run/docker.sock" at start-up when the socket is missing
or not writable for it; the worker itself logs why CODE repairs will fail until the sandbox works.
After a change to `docker/.env`, run `python scripts/tasks.py up` again (the containers are
recreated).

Every sandbox container carries the label `dag-healer.sandbox=1` and is removed when its run
ends, or killed and removed at `repo.pytest_timeout` (or once its output passes 256 MiB).
`docker ps -a --filter label=dag-healer.sandbox=1` shows leftovers of a worker that was killed
mid-run; a later run removes them once they have expired.

To debug a repair without containers, set `repo.sandbox.backend: process` in
`.demo/repo/healer.yaml` and restart the worker
(`docker compose -f docker/compose.yaml restart healer-worker`). pytest then runs inside
`healer-worker` without any isolation, and the worker and `dag-healer doctor` warn about it.

## Demo

`python scripts/tasks.py demo` (same as `make demo` or `python scripts/demo.py`) runs the
day-2 story of `example_existing_repo/` end to end (standard library only, Windows-safe):

1. stops `healer-worker` and recreates `.demo/repo` from `example_existing_repo/` (new git
   history; the `dags/` folder is rewritten in place so Airflow never loses the DAGs);
2. `docker compose up --detach --wait` (builds the image when it is missing; `--build` forces
   a rebuild);
3. in the api-server, `dag-healer store migrate` (a store left by an older image is upgraded
   first; this also runs with `--no-reset`) and `dag-healer store reset --yes --artifacts`
   (empties the store and restarts the worker's poll at that moment, so older failures in
   Airflow's history are ignored); then `dag-healer init --repo /opt/airflow/repo`, the demo
   settings in `healer.yaml` (poll every 5 s; e-mail alerts to `mailpit:1025` for
   `data-team@example.com`), the approval of the `sales_daily_etl` and `customers_sync`
   contracts (`inventory_report` stays a draft), a commit "Add dag-healer config and
   contracts", and the worker is started. `dag-healer doctor` then runs in `healer-worker`,
   and the demo stops early when its `sandbox` check fails (see "Test sandbox");
4. copies `fixtures/day2/*.csv` over `data/incoming/` (uncommitted), triggers the three DAGs
   through the REST API (admin token) and waits for their runs to fail;
5. waits (up to `--timeout`, 600 s) for the three incidents: `sales_daily_etl` DATA
   `reported`, `customers_sync` CODE `repaired` (new test, fix, PR.md, branch `healer/...`
   in `.demo/repo`; every pytest run in a sandbox container), `inventory_report` AMBIGUOUS
   `waiting_human`;
6. prints the incidents, the URLs (Healer page, each task-instance "Healer incident" tab,
   Mailpit) and copies each `PR.md` / `fix.patch` to `.demo/prs/<incident_id>/`.

Options: `--no-reset` (keep the repo, store and config), `--build`, `--listener-only`
(`monitor.modes: [listener]`: only the Airflow listener creates incidents, the REST poll is
off), `--timeout SECONDS`. The AMBIGUOUS incident is left for you: open its "Healer incident"
tab and click "Resolve as code" or "Resolve as data".

`python scripts/tasks.py e2e` (`scripts/e2e_smoke.py`) runs the demo and then checks
everything through the APIs (incidents, diff, PR.md, branch, Mailpit e-mails with masked
samples, 401/403 answers, resolve as code and as data) and the test sandbox (the doctor's
`sandbox` check, the worker log of the CODE repair, the socket mounted into `healer-worker`
only, no sandbox container left); it exits 1 when a check fails.
`HEALER_E2E=1 python -m pytest tests/e2e` wraps it for pytest.

Other store commands (run with `python scripts/tasks.py healer ...`): `store stats` (rows per
table), `store status` (schema version and pending migrations), `store migrate [--dry-run]`
and `store reset --yes [--artifacts] [--keep-state]`. The worker, `dag-healer init` and
`store migrate` create or upgrade the store schema; the api-server never migrates a store
(its store routes answer 503 until the schema is current).

## Linux notes

Set `AIRFLOW_UID` in `docker/.env` to your uid (`id -u`); `scripts/tasks.py` does it when it
creates the file. Otherwise files written by the containers under `.demo/` belong to uid 50000.

Set `HEALER_DOCKER_GID` in `docker/.env` to the group of the engine socket
(`stat -c %g /var/run/docker.sock`), so that `healer-worker` can start the test sandbox;
`scripts/tasks.py` does it too when it creates the file. A rootless engine needs
`HEALER_DOCKER_SOCKET` as well (see "Test sandbox").

## Troubleshooting

- `python scripts/tasks.py logs airflow-init` - migrations, user creation, ownership fixes.
- `python scripts/tasks.py wait` - polls http://localhost:8080/api/v2/monitor/health.
- `python scripts/tasks.py logs --follow healer-worker` - one line per incident transition.
  "waiting for /opt/airflow/repo/healer.yaml" means `python scripts/tasks.py init` (or the
  demo) has not run yet.
- "The Docker daemon did not answer within 60 s": Docker Desktop is starting or stuck; check
  its window (an error dialog may be waiting) or restart it.
- `[FAIL] sandbox` in `python scripts/tasks.py doctor`, or CODE incidents that end
  `repair_failed` with "The test sandbox is not usable": the reason says what to fix. The usual
  ones: the socket is not reachable from `healer-worker` (`HEALER_DOCKER_SOCKET`,
  `HEALER_DOCKER_GID`), or the image is missing (`python scripts/tasks.py build`). Once it is
  fixed, "Retry repair" on the incident runs the repair again.
- Docker Desktop needs at least 4 GB of memory and 2 CPUs for Airflow.
- Ports 8080, 8025 or 1025 already in use: stop the other program, or edit `docker/compose.yaml`.
- Postgres volumes created before `initdb/` existed have no `healer` database; `airflow-init`
  creates it on the next `up`.
