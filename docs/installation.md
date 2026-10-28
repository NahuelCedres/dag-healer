# Installing dag-healer

This guide installs dag-healer into an existing Apache Airflow 3.1+ deployment (dag-healer is
developed against 3.3.2) and takes it as far as its first incidents. The examples follow Acme
Health, a health-tech team that runs the ETLs of its client Acme General Hospital. Its DAGs live
in the GitHub repository `acme-health/etl`, with the base branch `main`. To see dag-healer at work
before you install it, run the [end-to-end demo](#7-try-the-end-to-end-demo).

dag-healer runs in three places:

| Part | Where it runs | What it does |
|---|---|---|
| The Airflow plugin | Airflow's own environment: the api-server, and every process that runs tasks | Serves the dag-healer views and the `/healer` API inside the Airflow UI, and reports each task failure to that API (a task listener) |
| The worker (`dag-healer worker`) | A host of its own, with a container engine | Confirms each failure through the Airflow REST API, classifies it, sends the alerts and repairs CODE failures in a test sandbox |
| The store | A PostgreSQL database, shared by the api-server and the worker | Incidents, their events and alerts, and the commands that people queue from the UI |

You need:

- Airflow 3.1 or later, on Python 3.10 or later;
- the DAGs in a git repository, with their DAG folder (`dags/` by default);
- for the worker: a host with Python 3.10 or later, git, and Docker or Podman; a PostgreSQL
  database for the store; an Anthropic API key, or access to Claude on Amazon Bedrock; and an
  Airflow account of its own;
- optionally, a GitHub token for pull requests (without one, fixes are written as `PR.md` and
  `fix.patch` files) and an SMTP server for the alerts.

## 1. Install dag-healer into Airflow's environment

dag-healer is not on PyPI yet: install it from a checkout of this repository. The checkout holds
the pre-built UI bundles, so you do not need Node.js. Install it in the same pip call as the
Airflow version you run (`airflow version` prints it), so that pip never moves Airflow:

```bash
git clone https://github.com/NahuelCedres/dag-healer.git
pip install "apache-airflow==3.3.2" ./dag-healer
```

Install it wherever Airflow runs:

- in the api-server, which serves the views and the API;
- in every component that runs tasks (the scheduler with the LocalExecutor, the Celery workers,
  the image of the Kubernetes pods): the listener reports failures from there. Where it is
  missing, the worker still finds the failures when it polls the REST API, every
  `monitor.poll_interval_seconds` (30 by default).

With the official Docker image, build a wheel and extend the image:

```bash
pip wheel --no-deps -w dist ./dag-healer
```

```dockerfile
FROM apache/airflow:3.3.2
COPY dist/dag_healer-0.1.0-py3-none-any.whl /tmp/
RUN pip install --no-cache-dir "apache-airflow==3.3.2" /tmp/dag_healer-0.1.0-py3-none-any.whl
```

The plugin registers itself through the `airflow.plugins` entry point: nothing goes into the
`plugins/` folder. Restart the Airflow components, then check that Airflow has loaded it:
`airflow plugins` lists `dag_healer`, and the Airflow UI shows a Healer entry in its navigation, a
Healer incidents tab on each DAG and a Healer incident tab on each task instance.

With the FAB auth manager, only the Admin and Op roles can read the list of plugins by default,
and the Airflow UI builds the dag-healer views from that list. Give the read permission to the
other roles that should see them:

```bash
airflow roles add-perms Viewer User --resource Plugins --action can_read
```

Airflow's processes get only what the plugin needs:

| Variable | api-server | Task processes |
|---|---|---|
| `HEALER_CONFIG`: the path of `healer.yaml` (default: `$AIRFLOW_HOME/healer.yaml`) | yes | no |
| `HEALER_DATABASE_URL`: the store (step 4) | yes | no |
| `HEALER_INGEST_TOKEN`: a random secret that the listener sends with each failure | yes | yes |
| `HEALER_API_URL`: the URL of the api-server | no | only when Airflow's `[core] execution_api_server_url` and `[api] base_url` do not lead to it |

Create the ingest token with, for example,
`python -c "import secrets; print(secrets.token_urlsafe(32))"`. Never give Airflow's processes the
worker's credentials (the Anthropic API key, the GitHub token, the worker's Airflow account): the
task processes run your DAGs' code ([docs/security.md](security.md), section 6).

The api-server reads `healer.yaml`, and the contracts folder that it names, for its views; it
rereads the file at most every 10 seconds. Point `HEALER_CONFIG` at the `healer.yaml` of a copy of
the repository that the api-server can read, at the same commit as the worker's checkout: an
approval from the views is refused when the two copies of the contract differ.

## 2. Run `dag-healer init`

Run `dag-healer init` in a checkout of the DAG repository, with dag-healer installed (on the
worker's host, or on your own machine):

```console
$ cd /srv/acme-health/etl
$ dag-healer init
Scanned /srv/acme-health/etl (DAG folder: dags): 2 DAG(s) found

DAG_ID               FILE                         TASKS  DATASETS               CONTRACT
-------------------  ---------------------------  -----  ---------------------  --------
lab_results_daily    dags/lab_results_daily.py    2      1 found, 0 unresolved  written
ward_length_of_stay  dags/ward_length_of_stay.py  2      1 found, 0 unresolved  written

Contracts:     /srv/acme-health/etl/healer/contracts
healer.yaml:   /srv/acme-health/etl/healer.yaml (written)
Tests:         no test suite found; dag-healer will create tests/ when it proposes the first fix
Store:         postgresql+psycopg2://healer:***@db.acme-health.internal/healer (created at schema v2)
Code files:    unchanged (7 Python files verified)

Contracts are drafts: nothing is enforced until a human approves them (dag-healer contracts approve <dag_id>)
```

`init` never imports or changes your code: it parses the DAG files without running them, and
checks at the end that no Python file has changed. It:

- writes a draft contract for every DAG whose input files it finds,
  `healer/contracts/<dag_id>.yaml`, inferred from those files. Approved contracts are never
  touched, and existing drafts are kept unless you pass `--force`;
- writes `healer.yaml` when it does not exist yet, with the inputs it found under `datasets`;
- creates the store, or upgrades the store of an older dag-healer: the database of
  `HEALER_DATABASE_URL`, else an SQLite file under `HEALER_HOME` (default
  `$AIRFLOW_HOME/healer`, else `~/.dag-healer`).

| Option | Use |
|---|---|
| `--dags-dir DIR` | The DAG folder, relative to the repository (default `dags`) |
| `--samples DIR` | Profile the files of `DIR` instead of the repository's, matched by relative path, then by file name, for example a copy of one day's input files |
| `--force` | Replace the existing draft contracts |
| `--repo PATH`, `-c PATH` | Another repository, another `healer.yaml` |

A note such as `unresolved dataset ...` names an input that `init` could not resolve from the
code: add its locator under `datasets.<dag_id>.<task_id>` in `healer.yaml`, then run `init`
again.

Commit `healer.yaml` and `healer/contracts/` next to your DAGs. Every committer can then change
dag-healer's settings, so protect the file like deployment configuration, for example with a
code-owner review ([docs/security.md](security.md), sections 6 and 7).

## 3. Configure `healer.yaml`

The file that `init` writes is commented, and every secret in it is an `${ENV}` reference, never a
value. `dag-healer check` validates it and prints the effective settings, with secrets masked.
These settings matter first.

### The repository and the contracts

```yaml
repo:
  path: "."                      # relative to this file
  base_branch: main
  dags_dir: "dags"
  tests_dir: null                # null: a minimal tests/ folder comes with the first fix
  allowed_paths: ["src/", "dags/"]
contracts:
  dir: healer/contracts          # relative to repo.path
datasets:
  lab_results_daily:
    extract_results: {"type": "csv", "path": "data/incoming/lab_results.csv"}
```

- The worker clones `repo.path` for every repair, so on the worker's host `repo.path` must be a
  git checkout of the repository with the branch `base_branch`. The clone holds the committed
  history only.
- A fix may change files under `allowed_paths` only; the new test goes into the tests folder.
- The worker checks each approved contract against the files that its locators name: relative to
  `repo.path`, absolute, or `s3://` URIs (install `./dag-healer[aws]` for those). It must see the
  same files as your tasks.

### The repair sandbox

```yaml
repo:
  sandbox:
    backend: container           # process: no isolation, for development and tests only
    engine: docker               # or podman
    image: "${HEALER_SANDBOX_IMAGE}"
```

Every pytest run of a CODE repair (your test suite, the generated test and the fix) happens in a
fresh container of `repo.sandbox.image`, without network, credentials or host folders, as a
non-root user. dag-healer never pulls that image: build it on the worker's container engine, with
Python, pytest and your repository's test requirements, a non-root `USER`, no `VOLUME` and no
secrets. For Acme Health, from its checkout:

```dockerfile
# sandbox.Dockerfile
FROM python:3.12-slim
RUN useradd --create-home --uid 10001 --shell /usr/sbin/nologin sandbox
COPY requirements.txt /tmp/requirements.txt
RUN pip install --no-cache-dir pytest -r /tmp/requirements.txt
USER sandbox
```

```bash
docker build -t acme-health/etl-sandbox:1 -f sandbox.Dockerfile .
export HEALER_SANDBOX_IMAGE=acme-health/etl-sandbox:1   # in the worker's environment
```

When your tests import Airflow, start from your `apache/airflow` image instead: its user `airflow`
is not root. Rebuild the image when the requirements change. Without a usable sandbox, a CODE
repair runs no code at all and ends `repair_failed` with the reason; once the sandbox works,
"Retry repair" on the incident runs it again (restart the worker after a change to `healer.yaml`
or to its environment). [docs/security.md](security.md) describes what the sandbox enforces
(section 4.3) and how to deploy it (section 6): prefer a rootless engine, because access to a
rootful engine is root on its host.

### Pull requests

```yaml
github:
  repo: "${GITHUB_REPO}"         # acme-health/etl
  pr_base: main
  token: "${GITHUB_TOKEN}"
  skip_ci: true
```

With a token and a repository (`github.repo`, or a GitHub `origin` remote of `repo.path`),
dag-healer pushes each CODE fix to a `healer/...` branch and opens a pull request into `pr_base`.
It never merges. Without them, it pushes the branch into the local repository and writes `PR.md`
and `fix.patch` under `$HEALER_HOME/prs/<incident_id>/`; the views offer that `PR.md` only when
the api-server sees the worker's `HEALER_HOME`. Keep `skip_ci: true`: CI then runs on a healer
branch only after a person has reviewed it. [docs/github/README.md](github/README.md) shows how to
create the token (a fine-grained token limited to the one repository, with Contents and Pull
requests set to read and write), protect the base branch and run CI after the review. Make the
`github ...` lines of `dag-healer doctor` clean before you give the token to the worker.

### The language model

```yaml
llm:
  provider: "${HEALER_LLM_PROVIDER:-anthropic}"
  model: claude-opus-5-5
  effort: high                   # low | medium | high | xhigh | max
  max_tokens: 16000
```

The worker classifies failures and writes fixes with Claude, through the Anthropic API. Give it
the key as `ANTHROPIC_API_KEY` in the worker's environment, never in `healer.yaml` and never in
Airflow's processes. `limits` caps the fix attempts and the tokens of an incident, the size of a
fix and the number of incidents triaged per hour.

To use Claude on Amazon Bedrock in your own AWS account instead, with the AWS credentials of the
worker's host (for example an EC2 instance role) and no API key, set the provider to `bedrock` and
`model` to a Bedrock model or inference profile ID, such as `eu.anthropic.claude-opus-5-5`.
[docs/bedrock.md](bedrock.md) covers model access, the IAM policy and the region.

## 4. Run the worker

Run the worker on a host or VM of its own, as a dedicated user, and never inside the Airflow
scheduler or workers: it runs model-written code in its sandbox and holds the credentials below
([docs/security.md](security.md), section 6). It needs no Airflow, because it talks to Airflow
through the REST API. Install dag-healer there, with the driver of the store's database:

```bash
python3 -m venv /opt/dag-healer
/opt/dag-healer/bin/pip install ./dag-healer psycopg2-binary
```

Give the worker a git checkout of the repository with `healer.yaml` (the `repo.path` of step 3),
the `docker` or `podman` CLI with access to its engine, and this environment:

| Variable | What it holds |
|---|---|
| `HEALER_CONFIG` | The path of `healer.yaml` in the checkout (or pass `-c PATH`) |
| `HEALER_DATABASE_URL` | The store, the same as the api-server's, for example `postgresql+psycopg2://healer:<password>@db.acme-health.internal/healer` |
| `HEALER_API_URL` | The Airflow api-server, for example `https://airflow.acme-health.internal` |
| `HEALER_AIRFLOW_USERNAME`, `HEALER_AIRFLOW_PASSWORD` | The worker's Airflow account |
| `ANTHROPIC_API_KEY` | The Anthropic API key |
| `HEALER_SANDBOX_IMAGE` | The sandbox image |
| `GITHUB_REPO`, `GITHUB_TOKEN` | Optional: pull requests |
| `HEALER_SMTP_HOST`, `HEALER_SMTP_PORT`, `HEALER_SMTP_FROM`, `HEALER_ALERT_TO`, ... | Optional: the alert e-mails of `notify.email` |
| `HEALER_HOME` | Optional: where the worker keeps its clones and `PR.md` files (default `~/.dag-healer`) |

Give the worker's Airflow account a role of its own that can read DAGs, DAG code, DAG runs, task
instances and task logs, and edit task instances only if people use "Retry task". Never give it
Admin ([docs/security.md](security.md), gap G7). Then check the setup and start the worker:

```bash
dag-healer doctor
dag-healer worker
```

`dag-healer worker` runs until it is stopped (SIGTERM or Ctrl+C) and logs one line per incident
transition: run it under your service manager. `dag-healer worker --once` runs one iteration and
exits, with exit code 1 when that iteration hit errors. `dag-healer incidents list` shows the
incidents from the command line.

On its first poll, the worker looks back 24 hours, so the failures of the last day become
incidents too (it triages at most `limits.max_incidents_per_hour`, 10 by default, in any hour; the
others wait). To ignore them, run `dag-healer store reset --yes` once, before the worker's first
start: it empties the store and makes the poll start now.

Keep the checkout on the base branch and up to date: the worker reads the contracts from it, and
every repair starts from its committed history.

## 5. Read `dag-healer doctor`

`dag-healer doctor` checks the environment that runs it: run it on the worker's host, with the
worker's environment, after every change. Each line is one check: `[ OK ]`, `[WARN]` (it works,
with a limitation), `[FAIL]` (an error: doctor exits 1) or `[SKIP]` (not used by this
configuration). Secrets are never printed. For Acme Health's worker:

```text
dag-healer doctor (0.1.0)
[ OK ] config              loaded /srv/acme-health/etl/healer.yaml
[ OK ] repository          /srv/acme-health/etl (DAG folder: dags)
[ OK ] contracts           /srv/acme-health/etl/healer/contracts: 1 approved, 1 draft
[ OK ] store               postgresql+psycopg2://healer:***@db.acme-health.internal/healer
[ OK ] ui bundle           /opt/dag-healer/lib/python3.12/site-packages/dag_healer/api/static/main.umd.cjs (384,227 bytes)
[ OK ] llm                 ANTHROPIC_API_KEY is set (value hidden); model claude-opus-5-5
[ OK ] sandbox             docker 29.8.2, image acme-health/etl-sandbox:1: uid 10001 (sandbox), no network (interfaces: lo), pytest 9.2.0 on Python 3.12.15
[ OK ] orchestrator        Apache Airflow (type airflow, instance airflow)
[ OK ] airflow api         https://airflow.acme-health.internal/api/v2/monitor/health: metadatabase=healthy, scheduler=healthy, triggerer=healthy, dag_processor=healthy
[ OK ] airflow auth        token issued for user 'dag-healer'
[ OK ] airflow plugin      entry point dag_healer.airflow.plugin:HealerPlugin
[ OK ] smtp                smtp.acme-health.internal:587 reachable; alerts to data-oncall@acme-health.example
[ OK ] secrets             no literal secrets in the configuration
[WARN] ingest              HEALER_INGEST_TOKEN not set; listener events are rejected (REST polling still works)
[ OK ] github              pull requests on acme-health/etl (token hidden)
[ OK ] github token        a fine-grained personal access token
[ OK ] github repository   the token's account is not an admin of acme-health/etl
[ OK ] github base branch  main requires a pull request with an approving review and blocks force pushes and deletion, and requires status checks
[ OK ] github workflows    4 workflow files checked

18 ok, 1 warning(s), 0 error(s), 0 skipped
```

The `ingest` warning is expected here: the ingest token belongs to the api-server and the task
processes, not to the worker.

| Line | What it checks | What to do when it is not OK |
|---|---|---|
| `config` | `healer.yaml` was found (`-c`, `HEALER_CONFIG`, `$AIRFLOW_HOME/healer.yaml`, `./healer.yaml`) and is valid | A warning means that none was found and the defaults apply: set `HEALER_CONFIG`, or run `dag-healer init`. An error names the invalid setting |
| `repository` | `repo.path` and its DAG folder exist | Fix `repo.path` (relative to `healer.yaml`) or `repo.dags_dir` |
| `contracts` | The approved, draft, stale and invalid contracts of `contracts.dir` | A warning for a missing or empty folder (run `dag-healer init`), while no contract is approved (nothing is enforced), for stale contracts (edited after their approval) and for invalid files (ignored): approve the contracts (step 6), approve the stale ones again, or fix the files |
| `store` | The store answers a query | Check `HEALER_DATABASE_URL`, the database driver and the network. The SQLite note means that the store works for one process on one machine only |
| `ui bundle` | The bundle of the views is in the installed package | Install dag-healer from a checkout that holds `src/dag_healer/api/static/main.umd.cjs`, or build it with `python scripts/tasks.py ui` |
| `llm` | With `anthropic`, `ANTHROPIC_API_KEY` is set. With `bedrock`, the model and the region are valid and AWS credentials resolve. No request is sent and no value is shown | Set what the line names in the worker's environment; for Amazon Bedrock, see [docs/bedrock.md](bedrock.md) |
| `sandbox` | The engine answers and holds the image, and a probe container runs as a non-root user, without network, with pytest | The line says what to fix: no image is set, the `docker` or `podman` CLI is missing, the engine cannot be reached (start it, or give the worker access to it: `DOCKER_HOST` or `CONTAINER_HOST`, the socket and its group), the image is missing (build it on that engine) or runs as root, or the probe found a network or no pytest. A warning means `backend: process`, which isolates nothing |
| `orchestrator` | The orchestrator adapter loads and supports `monitor.modes` and `monitor.trigger` | Fix the setting that the line names |
| `airflow api` | `GET <airflow.api_url>/api/v2/monitor/health` answers within 3 seconds | Check `HEALER_API_URL` and the network. A warning means that an Airflow component is not healthy |
| `airflow auth` | The worker's Airflow account gets a token (`POST /auth/token`) | A warning means that the user name or the password is not set, so the worker cannot poll. An error means that Airflow refused the account. Skipped when REST polling is off or the API cannot be reached |
| `airflow plugin` | The plugin's entry point is installed in this environment | On the worker's host this only says that dag-healer is installed; `airflow plugins`, in Airflow's environment, shows that Airflow has loaded it |
| `smtp` | The SMTP server accepts a connection | Check `HEALER_SMTP_HOST` and `HEALER_SMTP_PORT`. Skipped while `notify.email.host` is empty (no e-mail alerts) |
| `secrets` | No secret in `healer.yaml` is a literal value | Replace each value that the line names with an `${ENV}` reference |
| `ingest` | An ingest token is set in this environment | The api-server and the task processes need `HEALER_INGEST_TOKEN`; the worker does not. Without it there, Airflow's listener events are refused, and the worker still finds the failures by polling. Skipped when the listener and callback modes are off |
| `github` | Where fixes go: pull requests on a repository, or `PR.md` and `fix.patch` files | A warning means a repository without a token, so fixes are written as `PR.md` files: set `GITHUB_TOKEN` for pull requests |
| `github token`, `github repository`, `github base branch`, `github workflows`, `github setup` | With a token, read-only checks of the token, the repository, the base branch's rules and the workflows | See ["What doctor reports"](github/README.md#what-doctor-reports) in the GitHub guide |

## 6. Approve the contracts

A contract says what the input of a DAG must look like: its columns, their types and
nullability, numeric ranges and a band for the row count. `init` infers drafts from the files that
it profiled, and a draft is never enforced: its violations are evidence only, and a failure that
looks like a data problem waits for a person (AMBIGUOUS). dag-healer reports DATA only when an
approved contract is violated.

Review each draft before you approve it:

- the types: a column of dates without a time, such as `2026-10-01`, is inferred as `string` with
  a note, so that timestamps are not rejected; set `date` or `datetime` to require ISO 8601
  values;
- `nullable`, which is true when the sample held a null, and the numeric ranges, inferred with a
  10x headroom;
- `row_count`: at least half the sampled rows, and at most three times as many plus 10;
- `pii: true`, which masks a column in samples, alerts and prompts. `init` flags columns by their
  names: flag the others yourself, such as Acme Health's `patient_id`, or list them in
  `masking.columns`.

Then approve the contracts that you want enforced:

```console
$ dag-healer contracts approve lab_results_daily --by maria.lopez
Approved contract for 'lab_results_daily' (1 dataset(s)) by maria.lopez at 2026-10-27T23:19:34+00:00: /srv/acme-health/etl/healer/contracts/lab_results_daily.yaml
$ dag-healer contracts list
DAG_ID               STATUS    DATASETS  APPROVED_BY  APPROVED_AT                STALE
-------------------  --------  --------  -----------  -------------------------  -----
lab_results_daily    approved  1         maria.lopez  2026-10-27T23:19:34+00:00  no
ward_length_of_stay  draft     1         -            -                          -

Drafts are not enforced until approved: ward_length_of_stay
```

The approval records `approved_by`, `approved_at` and a hash of the contract in its file. A
contract edited after its approval is stale, and it is not enforced until it is approved again
(`contracts list` and doctor show it). Commit each approval through a pull request, like code:
the worker enforces the contract once its checkout holds the approved file.

A user who can edit a DAG in Airflow can also approve its contract from the dag-healer views. The
worker then writes the approval into its own checkout, where it applies at once; commit it from
there.

## 7. Try the end-to-end demo

The demo plays the "day 2" of a legacy repository, `example_existing_repo/`, on a local Airflow
3.3.2 in Docker: three DAGs fail, one for each category. It needs a clone of this repository,
Python 3.10 or later, and Docker with Compose v2 (Docker Desktop with at least 4 GB of memory and 2
CPUs), but no API key:

```bash
python scripts/tasks.py demo
```

It builds the image, starts the stack, runs `dag-healer init` on a copy of the repository,
approves two of its three contracts, drops the next day's input files and triggers the DAGs. Then
it waits for three incidents:

| DAG | Day 2 | Outcome |
|---|---|---|
| `sales_daily_etl` | Upstream renamed the `amount` column to `amt` | DATA, `reported`: the approved contract is violated, and an alert with a masked sample reaches Mailpit |
| `customers_sync` | Some signup dates are ISO 8601 timestamps, which a latent bug cannot parse | CODE, `repaired`: a regression test and a fix on a `healer/...` branch, with `PR.md` and `fix.patch` |
| `inventory_report` | Upstream renamed `qty` to `quantity`, and the contract is still a draft | AMBIGUOUS, `waiting_human` |

Then open the dag-healer page at http://localhost:8080/plugin/healer (user `admin`, password
`admin`) and Mailpit at http://localhost:8025. The demo copies each `PR.md` and `fix.patch` to
`.demo/prs/<incident_id>/`. Decide the AMBIGUOUS incident in its Healer incident tab, with
"Resolve as code" or "Resolve as data". `python scripts/tasks.py down` stops the stack.

The stack uses the offline `fake` provider, which replays the scripts of
`example_existing_repo/fixtures/fake_llm`. For real diagnoses, set `HEALER_LLM_PROVIDER=anthropic`
and `ANTHROPIC_API_KEY` in `docker/.env`, then run the demo again.
[docker/README.md](../docker/README.md) describes the stack. Its secrets are fixed and public:
keep it on your machine.

## AWS Step Functions

dag-healer also watches AWS Step Functions state machines whose steps are Python Lambda
functions. Without Airflow's UI, `dag-healer serve` is the console: the same views and API, on a
host of its own. Install both extras:

```bash
pip install "./dag-healer[aws,serve]"
```

- `dag-healer init` writes the Airflow template. Start from the `healer.yaml` of the example
  repository instead: `orchestrator.type: stepfunctions`, the pipelines and their state machines,
  and `monitor.modes: [poll, queue]`. With that file in place, `init` drafts the contracts of the
  pipelines (`repo.dags_dir` is then the folder of the state machine definitions).
- The worker reads executions, their histories and the Lambda logs with the default boto3
  credential chain, or with the `profile` or `role_arn` of `orchestrator.options`.
- `dag-healer serve` listens on `serve.host` and `serve.port` (default `127.0.0.1:8090`), with the
  views at `/plugin/healer`, and shares the store with the worker. `serve.auth.backend` signs
  people in: `dev`, a fixed local user, works on a loopback address only; for a team, use `token`,
  `trusted_header` (behind a proxy) or `alb_oidc` (behind an AWS load balancer), with
  `serve.auth.rules` for who may read, act on or approve the contract of which pipeline. Without
  rules, these backends refuse every request. Section 7.3 of
  [the orchestrator design](architecture/orchestrators.md) describes them.

[examples/stepfunctions_repo/README.md](../examples/stepfunctions_repo/README.md) deploys a sample
stack with AWS SAM and plays its three failures end to end.
