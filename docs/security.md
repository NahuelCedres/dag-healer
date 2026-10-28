# dag-healer: security model and threat model

Status: describes dag-healer 0.1.x (alpha) with the Airflow adapter. Every mitigation listed in
section 4 is enforced by the code. Recommendations and planned work are listed separately, as
gaps (section 5) and deployment advice (section 6). Module paths are relative to
`src/dag_healer/`. To report a vulnerability, see [`SECURITY.md`](../SECURITY.md).

---

## 0. Summary

- dag-healer turns untrusted text into model prompts. Whoever controls a task's code or its input
  data can write task logs, tracebacks, exception messages, dataset rows and pushed failure
  events. That text is masked and sent to the model provider (the Claude API, or Amazon Bedrock
  in the owner's AWS account: [`docs/bedrock.md`](bedrock.md)), and the model's answers drive
  what happens next.
- The model's output is untrusted too. A deterministic policy bounds the classification, the
  repair agent's file tools enforce path rules and size limits, and nothing is merged
  automatically.
- **The CODE path runs model-written code before a person reviews it.** Every pytest run of a
  repair (the repository's own tests, the generated test and the fix) happens in the test
  sandbox (`repo.sandbox`). By default, that is a fresh container for every run: no network, no
  credentials, no host path, a non-root user, resource limits and a throw-away copy of the clone.
  Without a usable sandbox, a CODE repair runs no code at all. The container still shares the
  host's kernel, and the worker's access to the container engine is root-equivalent on a rootful
  engine's host (gap G1). The `process` backend, meant for development and tests, runs the code
  without any isolation.
- By default, the repository's CI does not run the change until a person starts it after the
  review: the fix's commit carries `[skip ci]` (4.5). Workflows that the marker does not stop
  can still run it earlier (G2).
- Masking reduces, but does not prevent, the flow of personal data and secrets to the model, the
  store, alerts and pull requests (G4, G5).
- Deploy the worker as if it ran untrusted code, because it does: a dedicated host for the worker
  and its container engine, least-privilege credentials, restricted network egress and a GitHub
  token that can reach only the one repository (section 6). `dag-healer doctor` and every pull
  request report a token, base branch rules or workflows that fall short of that advice; GitHub
  itself enforces only the token's scope and the repository's rules (G3). The pull request's
  report comes after the workflows that the marker does not stop have run, so `doctor` must be
  clean before the worker gets the token.

---

## 1. System overview

| Component | Runs in | Holds |
|---|---|---|
| Airflow plugin: the API under `/healer`, the React views and the task listener | The Airflow api-server; the listener runs in every task process | The ingest token (API and listener), the store URL (API) |
| Worker (`dag-healer worker`) | Its own process or container | Airflow API credentials, the Anthropic API key or AWS credentials for Amazon Bedrock (an instance role), GitHub token, SMTP password, store URL, access to the container engine |
| Store | SQLite under `HEALER_HOME`, or the database at `store.url` / `HEALER_DATABASE_URL` | Incidents, events, commands, alerts |
| Repair workspace | `<healer_home>/work/<incident_id>`: a clone of the repository | The committed history of the repository |
| Test sandbox (`repo.sandbox`) | A fresh container for every pytest run, started by the worker with the `docker` or `podman` CLI on its container engine. The `process` backend (development and tests only): a child process of the worker | A throw-away copy of the clone without `.git`, and the scrubbed test environment |

The path of one failure:

1. **Detection.** The listener posts a failure hint (including the error text) to
   `POST /healer/api/v1/events/task-failed`, authenticated with the shared `X-Healer-Token`. The
   worker also polls the Airflow REST API. A hint is only stored: the worker confirms the failure
   through the Airflow API before it creates an incident.
2. **Evidence.** The worker fetches the task log and extracts the traceback. It reads the DAG
   file and the repository files named in the traceback, validates the DAG's data contract
   against the dataset files and builds a masked sample. Free text is masked before it is stored
   or sent anywhere.
3. **Classification.** The masked evidence goes to the model in one structured-output call (on
   Amazon Bedrock, when the model refuses structured outputs, a JSON answer checked against the
   same schema). A deterministic policy takes the final decision.
4. **Follow-up.**
   - DATA: an alert with the violated rule and the masked sample.
   - AMBIGUOUS: the incident waits for a person in the dag-healer views.
   - CODE: a repair. In a fresh clone, the model first writes one failing test, then a minimal
     fix, each in its own tool session. pytest runs both, in the test sandbox (4.3). The result
     is checked against the protected paths (4.2) before and after it is committed on a
     `healer/...` branch, by default with `[skip ci]` in the commit message (4.5). With a
     GitHub token and a repository (`github.repo` or a GitHub `origin` remote), the worker
     checks how the token and the repository are set up, pushes the branch and opens a pull
     request that lists the problems it found. Otherwise, or when the push or the pull request
     fails, the branch is pushed into the local repository, and `PR.md` and `fix.patch` are
     written.
5. **People act** from the UI: resolve an incident, retry the repair, retry the task or approve a
   contract. The API checks the user's Airflow permissions and queues a command in the store. The
   worker applies the command with its own credentials.

---

## 2. Assets

| Asset | Where it lives | Impact if it leaks or is misused |
|---|---|---|
| GitHub token (`github.token`, usually `GITHUB_TOKEN`) | The worker's environment. git gets it through `GIT_ASKPASS`, the REST API through a header | Pushing to the DAG repository (any branch that the token's scope and the repository's rules allow), opening pull requests, reading private code. A classic token with the `repo` scope reaches every repository the account can write to. |
| Anthropic API key (`ANTHROPIC_API_KEY`) | The worker's environment, read by the Anthropic SDK | Model usage billed to the owner's account. |
| AWS credentials for Amazon Bedrock (`llm.provider: bedrock`) | Where the AWS SDK's default chain finds them for the worker: the EC2 instance role, through the instance metadata service; the credentials of an ECS task or EKS pod; the worker's environment (`AWS_*`, a Bedrock API key in `AWS_BEARER_TOKEN_BEDROCK`); the shared AWS files. Never `healer.yaml` | Model usage billed to the owner's AWS account, and whatever else the role or key allows. Give the worker's role only `bedrock:InvokeModel` on the chosen model or inference profile ([`docs/bedrock.md`](bedrock.md)). |
| Airflow API credentials (`airflow.username`, `airflow.password`) | The worker's environment; exchanged for a JWT | Whatever the account's Airflow role allows. For an Admin: managing connections and variables, and clearing or triggering any run. |
| Ingest token (`ingest.token`, `HEALER_INGEST_TOKEN`) | The api-server, and every task process that runs the listener | Posting failure hints (each one is confirmed through the Airflow API). |
| Store URL (`store.url`, `HEALER_DATABASE_URL`) | The worker and the api-server (in the development stack, every Airflow container) | Reading every incident; queuing commands that the worker applies without further checks: approving contracts, starting repairs, retrying tasks (G6). |
| SMTP password (`notify.email.password`) | The worker | Sending mail as dag-healer. |
| Access to the container engine (its socket, `DOCKER_HOST` or `CONTAINER_HOST`) | The worker, which starts the test sandbox with it. In the development stack, the host's engine socket mounted into `healer-worker` | With a rootful engine, root on the engine's host: whoever can start containers can start a privileged one with the host's file system mounted. With a rootless engine, the account of the engine's user. |
| Data samples | Raw dataset files read by the worker. Masked samples in the store, the prompts, the UI and the alerts | Disclosure of personal or confidential data. |
| DAG source code and the repository | The repository working tree, the repair clone, the prompts, pull requests | Disclosure of code and of any secret committed in it or left in the working tree; changes to code that Airflow runs. |
| Secrets of the Airflow deployment | The worker's environment when it runs from an Airflow image with the deployment's settings (`AIRFLOW__DATABASE__*`, the Fernet key, the API's JWT secret, `AIRFLOW_CONN_*`) | Access to the Airflow metadata database and to every connection; Airflow API tokens forged for any user. |

---

## 3. Trust boundaries

dag-healer treats as untrusted anything that a task's code, a task's input data or an upstream
system can influence. It also treats the model's output as untrusted, because that output
follows from untrusted input.

| Input | Who controls it | Where it goes |
|---|---|---|
| Task logs, tracebacks, exception messages | Task code, and any value that it prints or puts in an exception | Masked, then the store, the prompts, the UI, the alerts and the pull request. Files that a traceback names are read and sent to the model (G4) |
| Dataset rows | Upstream deliveries | Contract validation; a masked sample in the store, the prompts, the UI and the alerts |
| Pushed failure hints (`/events/task-failed` and other adapter channels) | Any holder of the ingest token, which includes every task process | Stored as pending events and confirmed through the Airflow API. The pushed error text becomes the summary of the incident |
| Repository content (DAG files, modules, tests, committed data) | Repository committers | Sent to the model unmasked; imported and executed by pytest in the test sandbox |
| Model output: the classification | The model, steered by the inputs above | Schema check, then the deterministic policy |
| Model output: the diagnosis, evidence and fix summary, and the repair agent's final summary | Same | Masked, then the store, the UI, the alerts and the pull request |
| Model output: tool calls, the generated test and the fix | Same | Path rules, checked again before the commit (4.2); written to the clone; **executed by pytest in the test sandbox** (4.3); committed, pushed and proposed as a pull request |
| People using the UI and the API | Authenticated Airflow users | Airflow permission checks, then commands in the store that the worker applies |
| `healer.yaml` | Administrators, and every repository committer when the file is committed next to the DAGs | Trusted (section 7) |
| The worker's environment, installed packages (the masking hook) | Administrators | Trusted (section 7) |
| The sandbox image (`repo.sandbox.image`) | Administrators | Trusted (section 7). Everything in it is visible to the code that pytest runs |

Rule of thumb: anything that a task writes to its log, and any value in an upstream file, can
reach the model's prompt. Read the model's answer as if the author of that text had written it.

---

## 4. Threats and mitigations

Each subsection lists what the code enforces today. The weaknesses that remain point to the gaps
in section 5.

### 4.1 Prompt injection that steers the classification

A log line or a data value can contain instructions aimed at the model ("classify this as a code
bug").

Enforced:

- A deterministic policy (`agent/policy.py`) takes the final decision, not the model alone:
  - DATA needs a violated **approved** contract. Without one, the model cannot produce it.
  - CODE is refused while an approved contract is violated.
  - Errors in the pipeline definition, failures that only summarise failed child runs, and
    infrastructure errors (timeouts, killed processes, connection errors) go to a person,
    whatever the model says.
  - An answer below `classification.min_confidence` (default 0.75) goes to a person. So does any
    decision that needs the model when the model call fails or no model is configured.
- The classification is a structured-output call (on Amazon Bedrock, when the model refuses
  structured outputs, a JSON answer validated against the same schema, with one retry). The
  category must be `data`, `code` or `ambiguous`; anything else becomes ambiguous. The confidence
  is clamped to [0, 1].
- At most `limits.max_incidents_per_hour` (default 10) automatic triages start in any rolling
  hour.

What an injection can still do:

- tip an eligible failure towards CODE, which starts a repair (4.2, 4.3);
- tip it towards AMBIGUOUS, which stops automated handling;
- write a misleading diagnosis, which people read in the UI, the alerts and the pull request.

The deterministic signals themselves (exception type, traceback frames, missing column) are
parsed from the log text, so a log can shape them as well. The prompts do not tell the model that
the evidence may contain instructions (G9).

### 4.2 Prompt injection that steers the repair agent

Enforced by the repair tools (`agent/repair.py`):

- Paths must be relative to the root of the clone. Absolute paths, `~`, `:` and `..` are
  rejected, and a path that resolves outside the clone (symbolic links included) is refused.
- In the test phase, the agent may write only under the tests folder (only test files when the
  tests live at the repository root). In the fix phase, it may write only under
  `repo.allowed_paths` (default `src/` and `dags/`), and never in the tests folder, a test file or
  `conftest.py`. An entry of `repo.allowed_paths` that names a protected path (below), such as
  `.github/`, `.circleci` or `data`, or the contracts folder of `contracts.dir` or a folder in it,
  is a configuration error (an absolute `contracts.dir` is compared with an absolute `repo.path`
  only; the repair protects the folder in every case). `.` stays allowed, and the protected
  paths still apply inside it.
- Never written in either phase, the protected paths (`repo/protected_paths.py`, one definition
  for the tools, the final checks below and the configuration):
  - dag-healer's own files: `healer.yaml` at any depth, `healer/**`, the contracts folder,
    `data/**` and `fixtures/**`;
  - CI definitions, at the root of the repository: `.github/**`, `.gitlab-ci.yml`,
    `.gitlab-ci.yaml`, `.gitlab/**`, `.circleci/**`, `Jenkinsfile`, `azure-pipelines.yml`,
    `azure-pipelines.yaml`, `.azure-pipelines/**`, `.buildkite/**`, `buildkite/**`,
    `buildkite.yml`, `buildkite.yaml`, `buildkite.json`, `bitbucket-pipelines.yml`,
    `.travis.yml`, `.drone.yml`, `.drone.yaml`, `.drone.star`, `.drone.jsonnet`,
    `.woodpecker.yml`, `.woodpecker.yaml`, `.woodpecker/**`, `buildspec.yml`, `buildspec.yaml`,
    `cloudbuild.yaml`, `cloudbuild.yml`, `cloudbuild.json`, `appveyor.yml`, `.appveyor.yml`,
    `.cirrus.yml`, `.cirrus.star`, `.semaphore/**`, `.teamcity/**`, `.tekton/**`, `.build.yml`,
    `.builds/**`, `.gitea/**` and `.forgejo/**`;
  - git hook managers and the configuration their hooks run, at the root:
    `.pre-commit-config.yaml`, `.pre-commit-config.yml`, `.husky/**`, `.huskyrc*`,
    `.githooks/**`, `.git-hooks/**`, every file or folder whose name starts with `lefthook*` or
    `.lefthook*` (lefthook reads `lefthook.yml`, `.lefthook-local.toml` and the like),
    `.config/lefthook*`, `.overcommit.yml`, `.simple-git-hooks*` and `simple-git-hooks*`, and
    the lint-staged and commitlint configuration that hooks often run: `.lintstagedrc*`,
    `lint-staged.config.*`, `.commitlintrc*` and `commitlint.config.*`;
  - development environments that run commands when someone opens the repository, at the
    root: `.devcontainer/**`, `.devcontainer.json` (Codespaces runs their lifecycle commands
    with the user's secrets), `.vscode/**` (tasks that run when the folder opens), `.gitpod.yml`
    and `.gitpod.Dockerfile`;
  - git internals and metadata, at any depth: `.git/**`, `.gitattributes`, `.gitmodules` and
    `.lfsconfig`;
  - `CODEOWNERS` at the root, in `docs/` and in `.github/`;
  - lockfiles, dependency manifests (`requirements*.txt`, `pyproject.toml`, `setup.py`,
    `setup.cfg`, `package.json`, ...), `.env*` and data files (`.csv`, `.parquet`, `.jsonl`,
    `.pkl`, `.db`, archives, ...);
  - what a protected path that is a symbolic link committed in the repository leads to inside
    it, and the links on the way: with `.pre-commit-config.yaml -> src/ci/hooks.yaml`, the file
    `src/ci/hooks.yaml`; with `.husky -> src/hooks`, everything under `src/hooks/`; a protected
    link to the root protects everything. Hook managers and CI services that read a protected
    file from a checkout read what its link leads to.

  Paths are compared case-insensitively and without the trailing dots and spaces that Windows
  drops from names, so `.GitHub/x` and `.github./x` are both `.github/x`, and `..` components
  are resolved first (a path that leaves the repository is protected). Names are folded at least
  as macOS and Windows fold them: `upper()` then `casefold()`, so `requirementſ.txt` (a long s)
  is `requirements.txt` and `.gıthub` (a dotless i) is `.github`; folding more only protects
  more. The write tools check both the path as written and the path that it reaches through a
  symbolic link inside the clone, and read the links from the clone's `HEAD`. The tool
  descriptions sent to the model name only the main protected paths; a refused write says why.
- Never read or searched: `.git/**`, `.env*`, `healer.yaml`, `data/**`, `fixtures/**` and data
  files, compared as the protected paths are. `read_file` checks these rules on the path as
  written, not on the target of a symbolic link (G5).
- The work happens in a fresh clone of the committed history. The clone is deleted afterwards
  unless `debug` is on. Test runs work on a throw-away copy of the clone, never on the clone
  itself (4.3).
- Files in the clone that the tools did not write are reverted before the size check and before
  the commit, both sides of a rename included: a tracked file moved to a path that the tools
  also wrote comes back too, and a repository left inside the clone is removed. Files that git
  ignores and the clone's `.git` folder are not checked. This matters under the `process`
  backend only, where the code that pytest runs can still reach the clone (4.3).
- **A final check before the commit.** After that revert and the diff limits, the worker lists
  every path of the change (the index, the working tree and the untracked files, both sides of a
  rename) and compares each one with the protected paths. It also checks that the clone's `HEAD`
  is still the commit that the branch started from. If either check fails, the repair ends
  `repair_failed` with the reason, and nothing is committed or pushed.
- **A check of the commit itself.** Right after the commit, before anything is pushed, the
  worker checks the commit, which is what it then pushes (by its sha, whatever the branch points
  to by then): its only parent must be the commit that the branch started from; it may touch no
  protected path (both sides of a rename), add or change no symbolic link and no gitlink (a
  submodule, or a repository that `git add` found in the clone), and stay within the diff
  limits; and while `github.skip_ci` is on, its stored message must open with the skip line
  after the title (4.5). Otherwise the branch is reset to the commit that it started from, the
  repair ends `repair_failed`, and nothing is pushed. This catches a file written between the
  checks of the clone and the commit too. The check, the diff in the description and in
  `fix.patch`, and the push read the commit's objects as they are stored: the worker's git
  ignores replace refs, graft files and the commit-graph file (4.3), so none planted in the clone
  can show the check another commit, other parents or another tree than the ones pushed. So the
  protected paths hold for what is pushed, whatever wrote the change into the clone's files,
  also with `repo.allowed_paths: ["."]` or a bug in the tools. They do not hold against code that
  rewrites the clone's object files or its git configuration, which only the `process` backend
  lets the code that pytest runs do (4.3).
- The test must be collected and must fail before the fix. If a fix attempt changes the test,
  the test is restored and the attempt is rejected. The fix must make the test pass without
  adding failures to the existing suite.
- Limits:
  - a change of at most `limits.max_diff_files` (default 10) files and `limits.max_diff_lines`
    (default 400) changed lines, the test included;
  - per session: 40 writes, 20 test runs, 40 model turns, and 200 KB per `write_file` call;
  - per repair run: `limits.max_tokens_per_incident` tokens (default 400,000), 3 attempts at the
    test and `limits.max_fix_attempts` attempts at the fix (default 3). "Retry repair" starts a
    new run with fresh limits, and the classification is not counted against them.
- Delivery: the worker pushes only `healer/...` branches, refuses to push to the branch it
  cloned, never force-pushes and never merges. A change reaches the base branch only when a person
  merges the pull request (4.5).

What remains: within these rules, the model chooses what the test and the fix contain. That code
is executed (4.3), pushed to the repository, where CI runs it after a person has started it (or
earlier, in the cases of G2), and shown to a reviewer who may trust it. The path rules bind the
model's file tools and the commit only. They do not bind the code that pytest runs: the test
sandbox does (4.3).

### 4.3 Running model-written code (the test sandbox)

During a repair the worker runs `python -m pytest` several times: the existing suite, the
generated test before the fix, the test and the suite after each fix attempt, and every run that
the agent asks for through its `run_tests` tool. The generated test, the repository's own code
and the fix all run before any person has reviewed them.

Every one of these runs happens in the test sandbox that `repo.sandbox.backend` selects:

- `container` (the default): a fresh container for every run, started with the `docker` or
  `podman` CLI (`repo.sandbox.engine`) from the image `repo.sandbox.image`, on the local engine
  or on the one that `DOCKER_HOST` or `CONTAINER_HOST` names;
- `process`: pytest runs as a child process of the worker, without isolation. It is meant for
  development and tests. It is never chosen implicitly: `dag-healer doctor` reports it as a
  warning, `dag-healer check` prints a warning, and so does the worker when it starts.

Enforced by `agent/repair.py` and `worker/pipeline.py`:

- **No sandbox, no code.** Before a CODE repair clones anything, the worker checks that its
  sandbox can run. The container backend cannot when no image is configured, the engine CLI is
  missing, the engine does not answer, the image is not on the engine, or the image or
  `repo.sandbox.user` would run as root. The incident then ends `repair_failed` with the reason
  and how to fix it, and no code runs: neither the repository's tests nor the generated test or
  fix. The classification stays CODE, and "Retry repair" works once the sandbox does (a change
  to `healer.yaml` or to the worker's environment needs a worker restart).
- **A run that cannot start stops the repair.** When the sandbox fails to start pytest during a
  repair (an engine error, a missing image, pytest missing from the image), the repair ends
  `repair_failed` in the same way. An engine error is never read as a test result.

Enforced for every backend (`repo/sandbox.py`, `repo/pytest_runner.py`, `repo/git.py`):

- **A throw-away copy.** Every run works on a copy of the clone without its `.git` folder,
  deleted after the run. The copy never follows a symbolic link: links that are absolute or point
  outside the repository are left out, and so are FIFOs, sockets and devices. A linked folder is
  never entered. Repository tests that need git fail in the copy.
- **No git hook runs.** Every git command of the worker (clone, status, diff, commit, push, ...)
  runs with `core.hooksPath` pointing at an empty folder of the worker's own and with
  `core.fsmonitor` set to `false` (off since git 2.36; older git runs the `false` program, which
  does nothing). So do the `upload-pack` and `receive-pack` that local clones and pushes
  start in the source repository. Hooks and fsmonitor commands planted in the clone or in the
  source repository never run.
- **Objects as they are stored.** Every git command of the worker also runs with
  `GIT_NO_REPLACE_OBJECTS=1` and `core.useReplaceRefs` set to `false` on its command line (before
  git 2.42, a `core.useReplaceRefs` line in the clone's configuration overrode the variable),
  with `GIT_GRAFT_FILE` naming a file that never exists (`/dev/null/grafts`: `/dev/null` is no
  folder, so no file can be created there; on Windows, a file in that empty folder) and with
  `core.commitGraph` set to `false`. A replace ref (`refs/replace/`), a graft file
  (`.git/info/grafts`) or a commit-graph file can each make git show another commit, other
  parents or another tree than the objects that `git push` sends; planted in the clone, none of
  them changes what the worker checks, shows or pushes (4.2).
- **Environment scrub.** The test process does not inherit:
  - the Anthropic, GitHub and SMTP credentials, and every `HEALER_*` and `DAG_HEALER_*` setting;
  - Airflow's metadata database, API authentication, secrets backend, Celery, SMTP and e-mail
    settings, its Fernet and secret keys, `AIRFLOW_CONFIG`, connections (`AIRFLOW_CONN_*`) and
    variables (`AIRFLOW_VAR_*`);
  - `PYTHONPATH`, `PYTHONSTARTUP`, `PYTEST_*`, `GIT_ASKPASS`, `SSH_ASKPASS` and `SSH_AUTH_SOCK`;
  - any variable whose name contains `SECRET`, `TOKEN`, `PASSWORD`, `PASSWD`, `API_KEY`,
    `APIKEY`, `PRIVATE_KEY` or `CREDENTIAL`.
- **Cloud credentials.** These are always removed, after any override from the orchestrator
  adapter:
  - the AWS credential, profile and role variables: `AWS_ACCESS_KEY_ID`,
    `AWS_SECRET_ACCESS_KEY`, `AWS_SESSION_TOKEN`, `AWS_SECURITY_TOKEN`, `AWS_BEARER_TOKEN_BEDROCK`
    (a Bedrock API key), `AWS_PROFILE`, `AWS_DEFAULT_PROFILE`, `AWS_ROLE_ARN`,
    `AWS_ROLE_SESSION_NAME` and `AWS_WEB_IDENTITY_TOKEN_FILE`;
  - the legacy names that older AWS tools read: `AWS_ACCESS_KEY`, `AWS_SECRET_KEY`,
    `AWS_CREDENTIAL_FILE`, `EC2_ACCESS_KEY` and `EC2_SECRET_KEY`;
  - the ECS and EKS container-credential variables (`AWS_CONTAINER_CREDENTIALS_RELATIVE_URI`,
    `AWS_CONTAINER_CREDENTIALS_FULL_URI`, `AWS_CONTAINER_AUTHORIZATION_TOKEN` and
    `AWS_CONTAINER_AUTHORIZATION_TOKEN_FILE`);
  - every `ANTHROPIC_*` variable: the Anthropic SDK's keys and its endpoint settings for the
    Claude API and Amazon Bedrock (`ANTHROPIC_BEDROCK_BASE_URL`,
    `ANTHROPIC_BEDROCK_MANTLE_BASE_URL`);
  - botocore's Bedrock endpoint overrides (`AWS_ENDPOINT_URL_BEDROCK*`);
  - `GOOGLE_APPLICATION_CREDENTIALS` and `AZURE_CLIENT_SECRET`.

  `AWS_SHARED_CREDENTIALS_FILE`, `AWS_CONFIG_FILE` and `BOTO_CONFIG` are set to empty values and
  `AWS_EC2_METADATA_DISABLED` to `true`. botocore and boto3 (the AWS CLI and the Python SDK) then
  read neither the shared credential and config files (not even the default ones under `~/.aws`)
  nor a boto config file (`~/.boto`, `/etc/boto.cfg`: botocore's `boto-config` credential
  provider), and the AWS SDKs do not ask the instance metadata service for credentials. The
  JavaScript v3 and Go v2 SDKs treat an empty file variable as unset and read `~/.aws`: under
  `process`, which isolates nothing anyway, code that uses them finds the worker user's files.
  `AWS_REGION`, `AWS_DEFAULT_REGION` and the generic `AWS_ENDPOINT_URL` stay: they hold no
  credential. Under the container backend, the run also has no network (below), so neither the
  instance metadata service (`169.254.169.254`, `fd00:ec2::254`) nor the container-credential
  endpoint (`169.254.170.2`) is reachable. Under `process`, code on an EC2 instance can still
  query the instance metadata service itself and get the instance role's credentials:
  `AWS_EC2_METADATA_DISABLED` only advises the SDKs.
- **Airflow** inside the tests points at a throw-away `AIRFLOW_HOME` with an SQLite database in
  unit-test mode.
- **Time.** After `repo.pytest_timeout` seconds (default 600), the run is killed. A container is
  killed and removed by its name (killing the CLI alone would leave it running). Under `process`,
  the whole process group is killed on POSIX, and the process tree with `taskkill /T` on Windows,
  best effort.
- **Output.** stdin is empty, and the repository's `addopts` and the pytest cache are ignored. The
  output goes to a temporary file, and only its last 20,000 characters are kept. That output is
  masked before it reaches the model, the store or the pull request.

Enforced for the container backend (`repo/sandbox.py`):

- **Isolation.** Each run is a `docker run` (or `podman run`) with `--rm`, `--network none`, a
  read-only root file system (`--read-only`), tmpfs mounts for the copy of the clone (`/work`)
  and for `/tmp` (`noexec`; the tests' `HOME`, `TMPDIR` and `AIRFLOW_HOME` live there),
  `--cap-drop ALL`, `--security-opt no-new-privileges`, `--pids-limit`, `--memory` with an equal
  `--memory-swap` (no swap), `--cpus`, `--init`, `--shm-size 64m` and `--log-driver none`.
  Besides the two tmpfs mounts, the engine's own small ones stay writable: `/dev/shm`, and with
  Podman also `/run` and `/var/tmp` (its `--read-only-tmpfs`). Every process of a run ends with its
  container. The limits come from `repo.sandbox` (defaults: 2 GB of memory, 2 CPUs, 512
  processes and threads, 1 GB for the copy, 512 MB for `/tmp`).
- **No host path.** There is no bind mount and no named volume. (The engine still gives an
  image's own `VOLUME` folders anonymous volumes, removed with the container; the sandbox image
  should declare none.) The copy of the clone reaches the container as a tar stream on its
  standard input. A small bootstrap, run by the image's Python (`repo.sandbox.python`), unpacks
  it into `/work` and refuses absolute paths, `..`, links that leave `/work` and special files,
  then starts pytest. This also works with a remote or rootless engine, and when the worker
  itself runs in a container.
- **The image is never pulled** (`--pull never`): it must already be on the engine.
- **Never root.** The container runs as the image's own `USER`, or as `repo.sandbox.user`.
  `root` and uid 0 are refused in `repo.sandbox.user`, and so is an image whose `USER` is empty or
  root while `repo.sandbox.user` is not set: no container starts. Inside the container, the
  bootstrap refuses to run when its effective uid is 0.
- **Environment by name only.** The engine CLI receives the test environment as `-e NAME`
  arguments and the values in its own environment, never on its command line. Variables that
  describe the worker's host are left out (`PATH`, `HOME`, `TMPDIR`, the locale, `XDG_*`,
  `PYTHONHOME`, `PYTHONUSERBASE`, `VIRTUAL_ENV`, `LD_PRELOAD`, `DOCKER_*`, `CONTAINER_*`, ...), so
  that the image's own settings apply. The proxy variables are passed empty, so the docker CLI
  cannot add the proxies of its `config.json`, which may hold credentials. Besides the test
  environment, the CLI process gets only what it needs to reach the engine (`PATH`, `HOME`,
  `DOCKER_HOST`, `CONTAINER_HOST`, ...), and none of that is passed into the container.
- **Engine errors are not test results.** A run counts as a test result only when the bootstrap
  has reported its start and has started pytest, and the engine CLI has reported no error of its
  own afterwards (an exit code from 125 to 127 with an error line, for example when the engine
  goes away mid-run). Everything else (an unreachable engine, a missing image, a root user,
  pytest missing from the image) is a launch failure, which stops the repair. When the CLI fails
  mid-run or is killed by a signal, the container is killed and removed by its name.
- **Output limit.** A run whose output passes 256 MiB is killed and removed like a run past its
  timeout, and its output ends with a note that says so. Under `process`, `RLIMIT_FSIZE` plays
  that part (below).
- **Leftovers.** Each container has a unique name that carries an expiry time, and the label
  `dag-healer.sandbox=1`. A later run removes the containers whose expiry has passed, such as
  those of a worker that crashed.
- **Doctor.** `dag-healer doctor` checks that the engine answers and holds the image, then starts
  a probe container. The probe must run as a non-root user, see no network interface besides
  loopback, fail to open an outbound connection and import pytest.

Enforced for the `process` backend, as a guard against accidents only:

- **Resources (POSIX).** The soft limits of `RLIMIT_AS` (8 GiB), `RLIMIT_NPROC` (4096) and
  `RLIMIT_FSIZE` (1 GiB) are lowered in the child process, never raised. A limit that the kernel
  refuses is skipped: macOS does not let a process lower `RLIMIT_AS`, so there is no
  address-space limit on macOS. Windows gets no resource limits.

What remains:

- **A shared kernel.** A container shares the kernel of the engine's host. Code that exploits a
  vulnerability of the kernel or of the container runtime escapes to that host (G1).
- **Access to the engine is root-equivalent.** The worker can start containers, so whoever
  controls the worker process controls the engine. With a rootful engine (Docker Engine running
  as root, the VM of Docker Desktop), that means root on the engine's host. Prefer a rootless
  engine (rootless Podman or rootless Docker), or a host or VM dedicated to the worker and its
  engine (section 6).
- **The image is visible to the tests.** Its files and its `ENV` settings can be read by the code
  that pytest runs, and can end up in the output. The image must hold no secrets: no credentials,
  tokens or private keys, and no package index configuration with a password in it.
- **The output still leaves the sandbox.** The last 20,000 characters of each run's output reach
  the model, the store and the pull request, masked by pattern only (G5). A test can therefore
  pass on what it can read (the repository and the image) to the model provider and into the pull
  request. The engine keeps no log of a sandbox container (`--log-driver none`), whatever its
  default log driver is.
- **The scrub works by variable name** (G8). A secret in a variable whose name matches none of
  its rules reaches the tests, inside the container as well.
- **`process` is not a sandbox.** Under the `process` backend, the code runs as the worker's OS
  user, with the worker's network access and file system (G1, G11):
  - it can read the worker's environment, from `/proc/<parent pid>/environ` on Linux or with
    `ps -E -p <parent pid>` on macOS, including every credential that the scrub removed from its
    own environment;
  - it can open the files that the worker's AWS variables point at, and the worker user's
    `~/.aws` and `~/.boto`;
  - on an EC2 instance, it can get the instance role's credentials from the instance metadata
    service, and with them the worker's Amazon Bedrock permissions;
  - it can change the clone before and after the worker commits it: its files, its git
    configuration and its object files. The worker's git ignores the clone's hooks, fsmonitor,
    replace refs, graft file and commit-graph, but still uses the rest of its configuration,
    including the programs that it names, such as a filter driver or `gpg.program`, and the
    rewriting of the URL that a push goes to (`url.<base>.insteadOf`). It also reads the object
    files as they are, so code that rewrites them can make the check of the commit (4.2) read
    other content than the one that the push delivers;
  - it can plant git hooks in the source repository. dag-healer's own git commands ignore them,
    but a person or another tool that runs git there later runs them;
  - it can write wherever the worker can, including the repository working tree, `healer.yaml`
    and `HEALER_HOME`.

### 4.4 Secrets and personal data in prompts, pull requests, PR.md files and alerts

Enforced:

- **Masking** (`masking.py`) runs before anything is stored, shown, logged, e-mailed or sent to
  the model:
  - Free text goes through `mask_text`, which replaces e-mail addresses, IPv4 addresses, phone
    numbers, IBANs and runs of 7 or more digits. Dates are kept on purpose: they are often the
    key evidence. Free text means the error text, the exception message, the traceback, the log
    excerpt, violation messages, the orchestrator's error name and notes, including the notes
    that users type in the UI.
  - The log excerpt is limited to 200 lines around the last traceback.
  - Samples have `masking.sample_rows` rows (default 5), offending rows first. Some columns are
    hashed (`h:` and 10 hex characters of a salted SHA-256, `masking.default: hash`) or redacted
    (`***`, `masking.default: redact`), whatever their type:
    - columns flagged `pii: true` in the contract;
    - columns listed in `masking.columns`;
    - columns whose name looks personal or secret (e-mail, phone, person name, address, document
      or tax number, IBAN, card, IP address, birth date, password, token, API key, ...).

    The string cells of the other text columns go through `mask_text`. `masking.allow` exempts
    columns. `masking.hook` can replace or extend the result. If the hook fails, no sample is
    produced (fail closed). The API and the alerts mask a stored sample again before they show
    it.
  - Contract violations on columns flagged `pii: true`, or with a name that looks sensitive, do
    not report the observed minimum, maximum or offending values.
- **Repair tools.** They refuse to read data files and the other files listed in 4.2. The prompts
  tell the model not to try to recover masked values.
- **Pull request body and PR.md** (`pr/prmd.py`):
  - the error summary, the diagnosis, the pytest output and the notes are masked again;
  - the problems of the GitHub setup checks (4.5) are plain text: each one is put on one line
    and its Markdown characters are escaped, what is wrong cut at 400 characters and what to
    change kept whole (dag-healer's own text; only past 600 characters would it be cut), and
    at most 20 are listed;
  - code goes into fences longer than any run of backticks it contains, so it cannot break out of
    its block;
  - long sections are truncated, and the whole description stays below 65,000 characters
    (GitHub refuses a body over 65,536): the diff, the test, the pytest output, the diagnosis,
    the notes and the error shrink further, in that order, while "Before you merge" and "CI"
    always stay complete; the full change stays on the branch and in `fix.patch`;
  - `PR.md` and `fix.patch` are written under `<healer_home>/prs/<incident_id>/` (the incident id
    must be a safe file name);
  - the API serves `PR.md` only from that folder, as `text/markdown` with
    `X-Content-Type-Options: nosniff` and `Cache-Control: no-store`.
- **Alerts** (`alerts/`) are built only from masked fields. A sample is included only when it is
  flagged as masked, and its cells are scrubbed again. HTML templates are autoescaped.
- **The GitHub token** is sent only in the `Authorization` header of REST calls and to git
  through `GIT_ASKPASS`. It never appears on a command line or in a URL. It is replaced by `***`
  in git output and in GitHub error messages, before a long one is cut. A REST call that fails
  before GitHub answers is reported by the exception's type only, because the HTTP library can
  quote the `Authorization` header in its message. `github.token` loses the whitespace around
  it when the configuration is loaded (a secret file often ends with a line break), and must
  then be printable ASCII, or the configuration is refused without echoing it. The setup checks
  (4.5) name only its kind, told from its prefix (classic, fine-grained, OAuth, GitHub App
  installation or GitHub App user), never the token or any part of it.
- **The Anthropic API key.** When the Claude API provider quotes an error answer (an API error
  other than 401, 403, 404 and 429), the values of `ANTHROPIC_API_KEY` and
  `ANTHROPIC_AUTH_TOKEN`, and every fragment of 16 or more characters of them, are replaced by
  `[hidden]`, so a gateway in `ANTHROPIC_BASE_URL` that echoes the key leaves it out of the
  diagnosis. When the key can still be read once the answer is URL-decoded and its whitespace
  is taken out, `[hidden]` stands in for the whole answer.
- **AWS credentials** (`llm.provider: bedrock`) never come from `healer.yaml`: the AWS SDK finds
  them through its default chain, or in the profile that `llm.aws_profile` names.
  `dag-healer doctor` shows only where they come from (`iam-role`, `env`, ...) or that
  `AWS_BEARER_TOKEN_BEDROCK` is set, never a value.
  - **No redirect is followed.** The HTTP clients of both LLM providers (the Claude API's and
    Amazon Bedrock's) never follow one: a 3xx answer ends the call after that one request, so
    neither the request, with its credentials (the API key, the Bedrock API key, or the signature
    and the session token of temporary AWS credentials), nor the prompt reaches the host that the
    redirect names. The bedrock provider's region, wherever it comes from, must be an AWS Region
    code, and `ANTHROPIC_BEDROCK_BASE_URL` follows the rules of `llm.endpoint_url` (https, http
    only for a loopback host, no user info, query, fragment or backslash): both name the host
    that receives the signed requests. An `http` endpoint never goes through a proxy of the
    environment (`HTTP_PROXY`, `ALL_PROXY`), which would receive its signed requests and prompts
    in cleartext.
  - **Credential variables.** The whitespace around the values of `AWS_BEARER_TOKEN_BEDROCK` and
    of the environment's AWS keys (`AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY` and the session
    token, in `AWS_SESSION_TOKEN` or in `AWS_SECURITY_TOKEN`, its legacy name, which botocore
    reads first) falls away before the client signs, since a newline left in a header makes h11
    refuse it and quote it whole. A value with whitespace, a control character or a non-ASCII
    character within it, in one of these five variables, is refused before any request, and the
    message names the variable only.
  - **Error messages.** The bedrock provider's error messages hide the values of the credentials
    that the client authenticates with (the Bedrock API key; the access key, secret key and
    session token of its AWS session, from a profile or a role as well as from the environment)
    and of the environment's credential variables (`AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY`,
    `AWS_SESSION_TOKEN`, `AWS_SECURITY_TOKEN`, `AWS_BEARER_TOKEN_BEDROCK`). The AWS errors that
    `dag-healer doctor` shows hide the latter: doctor builds no Bedrock client. Both hide them as
    they are, URL-encoded, JSON-escaped, or as a fragment of 16 or more characters, and hide a
    whole message in which one of them can still be read once its spaces are taken out. They
    also hide access key IDs, the value after a credential's name (`x-amz-security-token`,
    `Authorization`, `SessionToken`, `SecretAccessKey`, `aws_session_token`, `Credential=`,
    `Signature=`...) and long random-looking strings, and they cut off the canonical request that
    AWS quotes when it refuses a signature. AWS's own message is left out whole when it talks
    about a token, a signature, a secret, an API key, a password, a bearer token, authorization
    or an `x-amz-` header (the AWS error type of the answer, when it has one, stands in for it),
    and an answer without a JSON message is never quoted. The error type itself is shown only
    when it is shaped like AWS's names of errors (capitalised words of letters, 3 to 64 in all,
    such as `AccessDeniedException`) and holds no credential, and a refusal's category only when
    it is lower-case letters and `_` (32 at most) and holds no credential (one check for both
    providers, for the refusal of a classification and the worker's warning about a refusal
    during a repair): `an unknown error type` and `unspecified` stand in for any other value.
  - **Debug logs.** botocore's records that quote a request or an answer are dropped from the
    worker's logs, on every botocore logger and for every client (the Bedrock provider, the S3
    dataset reader, the Step Functions adapter): `Making request for` (the parameters), `Sending
    http request` (the signed request with its `Authorization` and `X-Amz-Security-Token`
    headers), `Response headers` and `Response body` (an STS answer holds new credentials), and
    the canonical request, string to sign and signature of SigV4. As a backstop, any botocore
    record whose text holds an `Authorization` or `X-Amz-Security-Token` header or a credential
    field (`SessionToken`, `SecretAccessKey`, `Token`...) is dropped too. Under `-vv`, the
    anthropic SDK's own `Request options` record, which quotes every request body (the prompts),
    is dropped for both providers, unless `ANTHROPIC_LOG=debug` asks for the SDK's debug logs.
    At every verbosity, the records of the anthropic SDK, of the HTTP clients under it (httpx2,
    httpx) and of their transports (httpcore) show no headers of an answer, which AWS or a
    gateway can fill with an echo of a credential (`[headers not shown]` stands in for them), and
    the reason phrase of the status line only as the standard one of the status code (`403
    Forbidden`), since the endpoint writes that phrase too. Any of their records that names an
    `Authorization`, `x-api-key` or `x-amz-security-token` header, quotes a header value that h11
    refused or other bytes that it could not read (a status line, a chunk header or footer),
    quotes httpx's `HTTPStatusError`, or holds the value of a credential variable (or a fragment
    of 16 characters of one) is dropped, and so is the SDK's record of the stop reason of a
    repair's last turn. The bedrock provider's own line at the end of a repair names a stop
    reason of Claude's only (`other` stands in for any other value) and it logs no server-side
    fallback: the SDK does not check these fields of an answer, and Bedrock has no fallbacks.
- **Configuration.** The secret fields are `airflow.password`, `notify.email.password`,
  `github.token`, `ingest.token`, the adapter's secret options and a password inside `store.url`.
  They are meant to be `${ENV}` references. `dag-healer doctor` and `dag-healer check` warn about
  literal values, and `check` prints the configuration with secrets masked. When `healer.yaml`
  is not valid YAML, the error names the line, the column and the problem only: PyYAML's own
  message quotes the line, which may hold a literal secret.
- **The UI** renders model text as plain text, never as HTML.

What remains: files named in a traceback are read without the deny rules and sent unmasked (G4).
Masking is pattern-based and misses secrets, names and addresses in free text, and source code,
the generated test and the diff are never masked (G5). Model-written Markdown goes into the pull
request body (G10).

Data that leaves the deployment:

- **To the model provider:** the masked evidence, the DAG file, every repository file named in
  the traceback, every repository file that the repair agent reads, and the masked pytest output.
  - With `llm.provider: anthropic`, that is the Claude API (`api.anthropic.com`).
  - With `bedrock`, it is Amazon Bedrock in the owner's AWS account
    ([`docs/bedrock.md`](bedrock.md)). The requests go to the runtime endpoint of the configured
    region (`bedrock-runtime.<region>.amazonaws.com`), or to the VPC endpoint or gateway of
    `llm.endpoint_url`. Where the model processes them follows `llm.model`: an in-Region model ID
    keeps them in that Region (for Claude Opus 5.5, `eu-west-2` only); a geographic inference
    profile (`eu.`, `us.`, `au.`, `jp.`) sends them to one of the Regions of that geography that
    the profile lists; a global profile (`global.`) to any commercial Region. Amazon Bedrock gives
    model providers no access to the prompts and answers. Whether AWS keeps them depends on the
    account's data retention mode in the Region; Claude Fable 5 and Fable 5.1 require a mode in
    which AWS keeps them for up to 30 days and may review them. Model invocation logging, when the
    owner turns it on, stores them in the owner's account.
- **To GitHub:** the branch and the pull request, with their masked text, the generated test and
  the diff, visible to everyone who can see the repository.

### 4.5 GitHub token scope and the repository

[`docs/github/README.md`](github/README.md) describes the matching setup on GitHub: the token,
the rules of the base branch, how CI runs after review and what `dag-healer doctor` reports.

Enforced:

- The token is used for three things only:
  - `git push` of the `healer/...` branch, through a `GIT_ASKPASS` helper that reads the token
    from the environment of that one git process. Credential helpers are disabled for that push,
    so the token is never stored.
  - The GitHub REST endpoints that open or find a pull request. dag-healer never calls a merge
    endpoint.
  - The read-only requests of the setup checks (below).
- With a token, the branch is pushed to `https://github.com/<owner>/<name>.git`, built from
  `github.repo` or from a GitHub `origin` remote. Any user info in the URL is dropped.
- The worker refuses to push to the branch it cloned and never force-pushes.
- **CI after review** (`agent/repair.py`, `pr/prmd.py`, `repo/git.py`). With
  `github.skip_ci: true`, the default, the message of the fix's commit opens its first paragraph
  after the title with
  `[skip ci] CI runs after a person has reviewed this change (see the pull request).` It does so
  whether the branch goes to GitHub or only into the local repository, which a person may push
  later. GitHub then starts no workflow triggered by `push` or `pull_request` for that push, or
  for the pull request while that commit is its head, and many other CI services skip the commit
  too.
  - The worker commits with `--cleanup=verbatim`, so `commit.cleanup` and `core.commentChar` in
    its git configuration cannot drop the marker line as a comment, and the check of the commit
    (4.2) refuses a stored message that does not open with it: nothing is pushed then.
  - The pull request description and `PR.md` get a "CI" section. It says that CI has not run and
    how to start it after the review: an empty commit made with `git commit-tree` on top of the
    fetched branch and pushed with `git push --no-verify`. The branch is never checked out, so
    none of the reviewer's git hooks runs with its code in the working tree (`commit-tree` runs
    no hook, `--no-verify` skips pre-push). It also says that a base branch that requires the CI
    checks blocks the merge until they pass, and that the marker must stay out of the commit
    that reaches the base branch. When the branch is only in the local repository, the section
    says to push it with `--no-verify` and to run `dag-healer doctor` with a token first, because
    the push and the pull request start the workflows that the marker does not stop. When it
    could not be pushed even there, and only `fix.patch` holds the change, the section says to
    commit the patch with `git commit --no-verify` and a message whose body opens with the
    marker, before the push.
  - dag-healer's own text in the description, the problems found by the setup checks included,
    never spells a skip marker literally. The parts that come from the model or the repository
    (the diagnosis, the notes, the test, the diff and the pytest output) are not filtered.
  - The values from the failure in the commit message and in the pull request title (the
    exception type, the symbol, the ids and the test path) are put on one line, with square
    brackets turned into parentheses, so they can neither add a marker nor push it out of the
    paragraph after the title.
  - With `github.skip_ci: false`, the message carries no marker and the description has no CI
    section.
- **Setup checks** (`pr/github_checks.py`). With a token and a repository, after the commit and
  before the push, the worker checks how the token, the repository and its workflows are set up.
  It sends only `GET` requests, only to `api.github.com`, and never follows a redirect or a link
  from an answer: `/repos/{owner}/{repo}`, `/installation/repositories` (GitHub App tokens
  only), `/repos/{owner}/{repo}/rules/branches/{branch}` and the rulesets behind those rules,
  and, when the rules leave a requirement unmet, the branch and its classic protection. Each
  answer is read uncompressed and up to 2 MB, within twice the request timeout (5 seconds per
  operation, 3 under `doctor`); all the requests of one run share six times that timeout. It
  reads the workflow files of the clone (`.github/workflows/*.yml` and `*.yaml`) from disk,
  with PyYAML's pure-Python safe loader. YAML aliases and merge keys can make a small file
  huge, so each file has a budget: its loading may build 100,000 mapping entries in all, the
  ones that merge keys copy included, and its analysis may read 100,000 nodes, list and
  mapping items and string characters in all. A string or a container that several jobs share
  counts once for each of them, and a job condition once for each event that it is read for.
  The loading and the analysis each also stop after 2 seconds, and numbers and dates are loaded
  as the text they are written in: the scan never reads them, and PyYAML builds some of them in
  quadratic time (a YAML 1.1 base-60 integer such as `1:9:9:9`, integer keys whose hashes
  collide). The merge keys are merged in linear time too. In practice, the analysis of a
  workflow file of more than about 100 KB exceeds the budget too. It follows no symbolic link,
  and it reports symbolic links, files over 1 MB, the files beyond 2 MB of workflow files in all
  or beyond the first 200, files that cannot be read, that are not UTF-8, not valid YAML or too
  large or complex to load or analyse as not checked, never as OK. `dag-healer doctor` runs the
  same checks, on the workflow files of `repo.path`.
  - Errors: a base branch that does not require a pull request with at least one approving
    review, that allows force pushes, or that can be deleted; a `pull_request_target` workflow
    that refers to the pull request's head, whatever its activity `types` (`review_requested`
    fires when GitHub requests the code owners' review as the pull request opens, a bot can add a
    label before anyone has reviewed the pull request, and closing it is how a reviewer rejects
    it); a `create` workflow that uses secrets or write permissions.
  - Warnings: a classic personal access token or an OAuth token (their scope covers every
    repository the account can write to), a GitHub App user token (it acts for its user on every
    repository that the App is installed on and the user can access), or a token whose kind its
    prefix does not tell; the `workflow` scope, or admin or delete scopes; scopes that could not
    be read, for classic, OAuth and unknown tokens; a GitHub App installation that reaches more
    than one repository; an admin account behind the token; with `skip_ci`, a base branch that
    requires no status checks; a ruleset that the token's account can bypass, rulesets whose
    answer was lost, unreadable or an error other than a refusal, and the rulesets beyond the
    first 10 that hold a required rule, which are not read; classic branch protection that the
    token cannot read; a `create` workflow; with `skip_ci: false`, a `push` or `pull_request`
    workflow that runs for healer branches and uses secrets or write permissions; workflow files
    that were not checked.
  - What the workflow scan recognises: a head reference is `github.event.pull_request.head`
    (and anything under it), `github.head_ref`, `GITHUB_HEAD_REF`,
    `github.event.pull_request.merge_commit_sha`, a `refs/pull/` ref, `pull/<number>/head` or
    `pull/<number>/merge`, or `gh pr checkout`; a workflow uses secrets or write permissions when
    an expression names a secret other than `secrets.GITHUB_TOKEN`, a job passes
    `secrets: inherit`, or the workflow or a job sets `permissions: write-all` or a permission to
    `write` (`id-token: write` included, which gives OIDC tokens for cloud credentials). A job
    that calls a reusable workflow of the same folder counts what that workflow does. A job
    condition skips a job only in the exact forms that
    [`docs/github/README.md`](github/README.md) shows, as the whole condition or one of its
    top-level `&&` conditions.
  - A check that cannot run (a network error, a timeout, an answer that comes too slowly, a 403
    or 404 answer, an answer it does not understand or that is too large) is one warning that
    says what was not checked, never an OK; so are the scopes that an answer does not list and
    the rulesets whose answer was lost, unreadable or an error other than a refusal (a server
    error, a rate limit, whether GitHub answers 429 or 403, a redirect). A check that fails
    unexpectedly is one warning of its own, and the other checks, the local workflow scan
    included, still run; when the checks cannot start at all, one `setup` warning takes their
    place (`github setup` under `doctor`), named by the exception's type only. Only a ruleset
    that GitHub refuses to show (a 403 or 404 answer, unless it is the rate limit's: an
    organization ruleset, for example), or whose answer does not say whether the token's account
    can bypass it, adds nothing.
  - The findings never block: the branch is pushed and the pull request opened as before. The
    description lists them first, right after its opening note, under "Before you merge",
    errors first, each with what to change. `doctor` prints one line per finding and exits 1 on
    an error.
- Without a token or a repository, nothing is pushed over the network and the setup checks send
  no request. The branch is pushed into the local repository, and `PR.md` and `fix.patch` are
  written, without a "Before you merge" section.

What remains:

- GitHub enforces only the token's scope and the repository's rules (G3). The setup checks
  report a classic or OAuth token, a GitHub App user token, a GitHub App installation that
  reaches more than the one repository, an admin account and a base branch that a change can
  reach without review, but they cannot change any of them, and the pull request is opened
  anyway. For a fine-grained token they check the kind only: GitHub does not tell which
  repositories it reaches or which permissions it holds (Workflows, Administration: write), so
  its OK line says nothing about them. A leaked token (G1) can do everything its scope allows.
- **The report comes after the fact.** The `create` workflows start when the branch is pushed
  and the `pull_request_target` ones when the pull request opens, before anyone reads its
  "Before you merge" section. `dag-healer doctor` is the only warning ahead of time: make its
  `github ...` lines clean before the worker gets the token.
- The marker stops only `push` and `pull_request` workflows. `pull_request_target` and `create`
  workflows, and those of other events, still run. The checks read only the workflow files of
  `.github/workflows/`, not the reusable workflows of other repositories, the actions that the
  workflows use or the scripts that they run, and recognise only the head references, the
  credentials and the job conditions listed above (G2).
- With `github.skip_ci: false`, CI runs the change with the repository's secrets before review.
  So does a reviewer who starts CI without reading the change (G2).
- By default, a squash merge copies the commit messages, the marker included, into the commit
  that reaches the base branch. The base branch's CI then skips that commit, unless the person
  who merges deletes the marker from the message. A merge message that copies the pull request
  description can also carry a marker that the model wrote into the diagnosis, the notes, the
  test or the diff.
- The checks are a snapshot taken before the push. A rule, a workflow or a token that changes
  later shows up only in the next pull request or the next `doctor` run.

### 4.6 Access to the UI and the API

Enforced (`api/`):

- Airflow mounts plugin applications without authentication, so each route declares its own.
  Every `/healer/api/v1` route except the ingest routes (`/events/*`) requires an Airflow user
  (`Authorization: Bearer <jwt>`, or the `_token` cookie of the Airflow UI). Tests check that
  every such route declares it.
- Routes scoped to a DAG (`/dags/{dag_id}/...`) check Airflow's own permissions for the DAG in the
  path:

  | Action | Airflow permission on the DAG |
  |---|---|
  | Read the DAG's incidents | Read task instances |
  | Resolve an incident, retry the repair, retry the task | Edit task instances |
  | Read the DAG's contract | Read DAG code |
  | Approve the contract | Edit the DAG |

- The lists across DAGs (`/incidents`, `/dags`, `/contracts`) show only the DAGs that the user can
  read (G12). A permission check that fails denies access. Without Airflow, every route that
  needs a user answers 503.
- Every state-changing request outside the ingest routes must carry
  `X-Requested-With: dag-healer`. Its `Origin` or `Referer`, when present, must match the Airflow
  base URL (`AIRFLOW__API__BASE_URL`, `HEALER_PUBLIC_URL`) or the request's own host. Request
  bodies are capped at 1 MiB.
- The API never acts by itself: it queues a command, and the worker applies it.
  - An approval records the hash of the contract file at the time of the request, and the worker
    refuses it if the file changed before the worker applied it.
  - Contract paths are confined to the contracts folder.
- The ingest routes authenticate with the shared `X-Healer-Token`:
  - the token is compared in constant time, and without a configured token the routes answer 503;
  - they accept 120 requests per minute per client address (per channel and token on adapter
    channels), and refuse requests that announce a body larger than 64 KiB;
  - an event is only a hint: the worker confirms each failure through the Airflow API before it
    creates an incident, and ignores DAGs that it does not monitor.
- Public without authentication: `/healer/health` and the static UI bundle. The interactive API
  docs are served only when `HEALER_DEBUG` is on.

What remains: G6 (the worker trusts the store), G7 (one Airflow account for the worker), G12
(coarse read permissions), G14 (one ingest token for every task process).

Not covered yet: the standalone host (`dag-healer serve`) and its authentication backends.
Section 7.3 of [the orchestrator design](architecture/orchestrators.md) describes their checks.

---

## 5. Known gaps

The gaps are ranked by severity in a typical deployment, which combines their impact with how
easily untrusted input reaches them:

- **Critical:** untrusted input, such as a log line or a data value, can lead to code execution
  with the worker's credentials.
- **High:** a leaked credential or a common configuration gives access far beyond dag-healer's
  own scope.
- **Medium:** data or secrets reach the model provider, the store or pull requests; dag-healer's
  permission model can be bypassed from a common foothold; or the defences against injection are
  weaker than they could be.
- **Low:** the gap needs an unusual configuration or an existing foothold, or its impact is
  limited.

| ID | Severity | Gap |
|---|---|---|
| G3 | High | Only the token's scope and the repository's rules limit what the GitHub token can do |
| G1 | Medium | Model-written code runs in containers that share the host kernel |
| G2 | Medium | Some CI workflows can still run healer branches before review |
| G4 | Medium | Files named in a traceback are read and sent as they are |
| G5 | Medium | Masking is pattern-based, and code is never masked |
| G6 | Medium | The worker trusts every command in the store |
| G7 | Medium | One Airflow account acts for everyone |
| G8 | Medium | The environment scrub works by variable name |
| G9 | Medium | The prompts do not mark the evidence as untrusted |
| G10 | Low | Model-written Markdown goes into the pull request body |
| G11 | Low | The `process` backend's limits and timeout do not stop hostile code |
| G12 | Low | Read permissions are coarser than they look |
| G13 | Low | Alert e-mail uses plain SMTP by default |
| G14 | Low | Every task process shares one ingest token |
| G15 | Low | The repair agent's actions are not recorded |
| G16 | Low | Debug mode keeps clones and serves the API docs |
| G17 | Low | git skips its repository ownership check |

**G1 (Medium): model-written code runs in containers that share the host kernel.** Any incident
classified CODE runs code in the test sandbox, and through prompt injection (4.1, 4.2), whoever
controls a log line or a data value can try to get code of their choice into the test. Under the
container backend (the default), that code runs in a fresh container without network,
credentials or host paths, as a non-root user without capabilities (4.3). What remains:

- **The kernel.** The container shares the kernel of the engine's host. A vulnerability of the
  kernel or of the container runtime lets the code escape to that host, where the engine runs,
  and often the worker too.
- **The engine.** On a rootful engine, the worker's access to the engine is root on the engine's
  host (section 2). Code that escapes the container lands on the host that this access controls.
- **The output.** The code can read the repository and the image, and write what it finds to its
  output, which reaches the model, the store and the pull request after masking (G5).

Under the `process` backend, none of this isolation applies, and G1 is Critical. The code runs as
the worker's OS user, with the worker's network access (the Airflow API and metadata database,
internal services, the internet and cloud metadata endpoints: on EC2, the instance role's
credentials, and with them the worker's Amazon Bedrock permissions). It can read the worker's
environment, including every credential that the scrub removed, from `/proc/<parent pid>/environ`
on Linux or with `ps -E -p <parent pid>` on macOS, and open the worker user's `~/.aws` and
`~/.boto`. It can change the clone, its git configuration and its object files
before and after the worker commits (4.3), and write wherever the worker can: the repository
working tree (in the development stack, the folder that Airflow loads DAGs from), `healer.yaml`,
`HEALER_HOME` (the SQLite store, the PR artifacts) and the data files that the repair tools refuse
to read.

- Until fixed: keep the container backend everywhere but in development and tests, and follow
  section 6: a host or VM dedicated to the worker and its engine, preferably a rootless engine, a
  sandbox image without secrets, and restricted egress for the worker itself.
- Possible fix: a boundary stronger than a shared kernel: an engine whose runtime is gVisor or
  Kata Containers, or sandbox backends that run each test run in its own VM or task (ECS/Fargate
  `RunTask`, a Kubernetes Job) behind the same sandbox interface.

**G2 (Medium): some CI workflows can still run healer branches before review.** The
`healer/...` branch is pushed to the repository itself, not to a fork, and the pull request is
opened from it. On GitHub, the workflows that run for a branch of the same repository get the
repository's secrets. By default (`github.skip_ci: true`), the fix's commit carries
`[skip ci]`, so GitHub starts no workflow triggered by `push` or `pull_request` for it: those run
only once a person starts CI after the review (4.5). What remains:

- **Workflows that the marker does not stop.** A `pull_request_target` workflow runs with the
  repository's secrets whatever the commit message says. If it checks out or runs the pull
  request's head, it runs the model-written code with them. A `create` workflow runs when the
  branch is pushed, with the branch's code checked out by default. `dag-healer doctor` and the
  pull request report both (4.5): an error for a `pull_request_target` workflow that refers to
  the head, whatever its activity types, and for a `create` workflow that uses secrets or write
  permissions, OIDC included, a warning for any other `create` workflow. The pull request's
  report comes after those workflows have run, so only `doctor` warns ahead of time. The scan
  reads only the files in `.github/workflows/` (following the reusable workflows of that folder
  that a job calls), not the reusable workflows of other repositories, the actions or the
  scripts that the workflows run, and it recognises only the head references, the credentials
  and the job conditions that 4.5 and [`docs/github/README.md`](github/README.md) list: a
  workflow that reaches the head another way is not reported, and one that skips healer
  branches with another condition is reported anyway. Workflows triggered by `workflow_run`,
  `pull_request_review`, `pull_request_review_comment`, `issue_comment`, `workflow_dispatch`,
  `merge_group` or `schedule` ignore the marker too, and are not checked.
- **`github.skip_ci: false`.** CI runs as soon as the branch is pushed. `doctor` and the pull
  request warn about every `push` or `pull_request` workflow that runs for healer branches and
  uses secrets or write permissions (the default permissions of `GITHUB_TOKEN`, set in the
  repository's settings, are not read).
- **The reviewer.** Starting CI takes one empty commit. A reviewer who pushes it without reading
  the change runs the change with the repository's secrets. The commands that the pull request
  shows never check the branch out, so the reviewer's own git hooks do not run its code; a
  reviewer who checks it out anyway runs the post-checkout hook with it.
- Until fixed: keep `github.skip_ci: true`, and start CI only after the review. Leave healer
  branches out of `pull_request_target` and `create` workflows with a job condition:
  `if: ${{ !startsWith(github.head_ref, 'healer/') }}` for pull request events, and
  `if: ${{ !startsWith(github.ref, 'refs/heads/healer/') }}` for `create` and `push`. Under
  `push`, `branches-ignore: ['healer/**']` leaves the branch out too. Under `pull_request` and
  `pull_request_target`, on the other hand, `branches` and `branches-ignore` filter the base
  branch, the one that the pull request targets, so they do not leave out a healer pull request.
  Keep deployment secrets in environments that require a reviewer. Or run without a GitHub token
  (PR.md mode).
- Possible fix: an option to push to a fork. The `pull_request` workflows of a pull request from
  a fork get no secrets (`pull_request_target` workflows still do).

**G3 (High): only the token's scope and the repository's rules limit what the GitHub token can
do.** The worker's code pushes only `healer/...` branches and only opens pull requests. GitHub
itself enforces only the token's scope and the repository's rules, and the token sits in the
worker's environment, which code that escapes the test sandbox can reach (G1). A token with
write access to the repository contents can push to any branch that no rule protects, the base
branch included. A classic token with the `repo` scope covers every repository its account can
write to. dag-healer cannot enforce the scope or the rules, so it reports them (4.5):
`dag-healer doctor` and every pull request warn about a classic or OAuth token, a GitHub App
user token, the `workflow` scope, admin or delete scopes, a GitHub App installation that
reaches more than one repository and an admin account behind the token. They report as errors
a base branch that does not require a pull request with an approving review, or that allows
force pushes or deletion. These reports never stop the pull request. For a fine-grained token,
GitHub tells neither which repositories it reaches nor which permissions it holds, so
`doctor`'s OK line for it checks its kind only.

- Until fixed: use a fine-grained personal access token limited to the one repository, with
  "Contents: read and write", "Pull requests: read and write" and no other permission than
  "Metadata: read", which GitHub adds by itself (in particular no "Workflows" and no
  "Administration"), or a GitHub App installed on that repository only, for an account with at
  most the Write role. Protect the base branch with the ruleset of
  [`docs/github/base-branch-ruleset.json`](github/base-branch-ruleset.json) (a pull request with
  an approving review, no force pushes, no deletion, no bypass) and add the CI's checks as
  required status checks. [`docs/github/README.md`](github/README.md) describes each step, and
  `dag-healer doctor` checks the result.

**G4 (Medium): files named in a traceback are read and sent as they are.** To show the model the
code around a failure, the worker reads every repository file that a traceback frame names and
sends its content to the model unmasked, as code. It reads these files from the repository
working tree, which also holds files that git ignores, and applies neither the repair tools' deny
rules nor a file-type filter. The frames come from the last traceback in the task log or in the
pushed error text. A log that ends with text shaped like a traceback, with a frame such as
`File "/opt/airflow/repo/.env", line 1`, is enough to send a `.env` file, `healer.yaml` or a raw
data file to the model. From there, the content can reach the diagnosis, which people read in the
UI, the alerts and the pull request (see also G10).

- Possible fix: apply the repair tools' deny rules to these files, and read only Python sources.

**G5 (Medium): masking is pattern-based, and code is never masked.**

- `mask_text` replaces e-mail addresses, IPv4 addresses, phone numbers, IBANs and runs of 7 or more
  digits. Everything else in a log line or an exception message passes through unchanged: names,
  postal addresses, IPv6 addresses, numbers shorter than 7 digits or glued to letters
  (`acct12345678`), API keys, tokens, passwords and connection strings.
- In samples, only string cells are scrubbed. Values nested in lists or objects (JSON or struct
  columns) reach the store and the prompts unmasked.
- Numeric, boolean and date columns stay as they are, unless the contract flags them,
  `masking.columns` lists them or their name looks sensitive. A numeric `salary` column, for
  example, is shown as is.
- Contract violations hide the observed values only for columns flagged `pii: true` or with a name
  that looks sensitive. Columns listed only in `masking.columns` still report their minimum,
  maximum and offending values, through `mask_text`.
- The exception type and the function names of traceback frames are stored and sent as they are.
- Hashing is pseudonymisation, not anonymisation. The salt is the incident id, which the UI and
  pull requests show. Anyone can hash every candidate of a small set (birth dates, short ids, phone
  numbers) and match the hash to recover the value. Prefer `masking.default: redact` for such
  data.
- Source code goes to the model as it is: the DAG file, the files named in the traceback (G4) and
  every file that the repair agent reads. The agent's read deny list covers only `.env*`,
  `healer.yaml`, `data/**`, `fixtures/**` and data files, and `read_file` checks it on the path as
  written. The agent can read a committed `credentials.json`, a private key, a settings module with
  a password in it, or a symbolic link that points to a denied file.
- The generated test and the diff go into the commit and the pull request verbatim. The test
  prompt asks the model to reuse the value that broke the task, for example the string quoted in
  the error message. An unmasked value from an exception message can therefore end up in the
  repository's history.
- Until fixed:
  - flag personal columns with `pii: true`, list extra columns in `masking.columns`, and add a
    `masking.hook` for rules specific to your data;
  - keep secrets and raw rows out of task logs and exception messages, and keep secrets out of
    the repository and its working tree;
  - review the test before you merge it.

**G6 (Medium): the worker trusts every command in the store.** The API checks the user's Airflow
permissions before it queues a command, but the worker applies every command that it finds in the
store without checking who queued it. Write access to the store is therefore enough to approve
contracts, start repairs and retry tasks with the worker's Airflow account, and read access shows
every incident. The store URL carries that access. The development stack gives it to every Airflow
container, including the scheduler, which runs task code, and its store shares a database user
with the Airflow metadata database.

- Until fixed: give the store URL only to the api-server and the worker, with a database user of
  its own.

**G7 (Medium): one Airflow account acts for everyone.** The worker uses its own Airflow account
for everything: it reads the logs and the code of every monitored DAG, and it clears task
instances when a user asks for a retry (the user's permission is checked when the request is
queued). The development stack uses the `admin` account. If those credentials leak (G1), the
attacker gets whatever that account can do. For an Admin, that includes managing connections and
variables, and running any DAG.

- Until fixed: give the worker a dedicated account whose role can read DAGs, DAG code, DAG runs,
  task instances and task logs, and edit task instances only if people use "Retry task".

**G8 (Medium): the environment scrub works by variable name.** A secret in a variable whose name
matches none of the rules reaches the test process, in the container sandbox too: the container
gets the scrubbed environment by name (4.3). Examples: `GH_PAT`, `DB_PASS`, a
`DATABASE_URL` with a password in it, `AIRFLOW__CORE__FERNET_KEY_CMD` or the legacy
`AIRFLOW__CORE__SQL_ALCHEMY_CONN`. The scrub does not use the variable names that `healer.yaml`
references for its secret fields. This matters even without hostile code: a repository test that
prints its environment sends the value to the model and, through the pytest output, to the pull
request.

- Until fixed: give secret variables names that contain `TOKEN`, `SECRET`, `PASSWORD` or
  `API_KEY`, and give the worker only the variables it needs.
- Possible fix: also remove the variables that the secret fields reference, or build the test
  environment from an allow-list.

**G9 (Medium): the prompts do not mark the evidence as untrusted.** The classification prompt
wraps the evidence in an `<incident>` JSON block, and the repair prompts put the traceback, the log
excerpt, the sample and the code in fenced blocks. No prompt tells the model that the evidence
may contain instructions, or that it must not follow them. The deterministic policy, the tool
rules and human review are the controls that matter, but such an instruction would be cheap
defence in depth. A successful injection that stays within the rules is most likely to show up as
a misleading diagnosis or agent summary, such as "this change is safe to merge".

**G10 (Low): model-written Markdown goes into the pull request body.** The diagnosis and the
repair agent's final summary are masked, but they go into the body as Markdown, not as code.
Links and images in them render on GitHub. GitHub fetches images through its image proxy, so an
injected image URL can carry what the model saw (masked evidence, source code, the files of G4)
to another host as soon as someone opens the pull request.

- Possible fix: put model text in a fenced block, or escape its Markdown.

**G11 (Low): the `process` backend's limits and timeout do not stop hostile code.** This gap
concerns the `process` backend only. Under the container backend, the engine enforces the limits,
the sandbox kills a run whose output passes 256 MiB, and every process of a run ends with its
container (4.3).

- Only the soft limits are lowered, so code in the test can raise them again up to the hard
  limits.
- The process group is killed only at the timeout. When pytest finishes in time, processes that a
  test started in the background keep running, and a process that starts its own session
  (`setsid`) survives even the kill at the timeout.
- On Windows there are no resource limits, and the kill is best effort. On macOS there is no
  address-space limit.

**G12 (Low): read permissions are coarser than they look.**

- The incident detail includes the diff, the generated test and the masked sample, but it only
  needs read access to the DAG's task instances.
- The lists across DAGs need only read access to the DAG: `/incidents` and `/dags` show masked
  error summaries, and `/contracts` shows every contract. Reading one contract through
  `/dags/{dag_id}/contract`, on the other hand, needs read access to the DAG code.
- A custom role that can read a DAG, but not its task instances or its code, still sees its error
  summaries and its contract. A role that can read task instances but not code still sees
  fragments of the code.

**G13 (Low): alert e-mail uses plain SMTP by default.** `notify.email.starttls` defaults to false
and the port to 25. The alerts (masked) and the SMTP login therefore travel unencrypted, unless
you enable STARTTLS or use port 465, which uses implicit TLS.

**G14 (Low): every task process shares one ingest token.** The listener needs the token, so any
task code can read it and post failure hints. The worker acts on a hint only after the Airflow API
confirms a real failure. The pushed error text then becomes the summary of that incident, but a
task can already write anything to its own log, so this adds little. The rate limiter keeps its
counters inside each api-server process, and the 64 KiB limit is checked against the
`Content-Length` header only: a body sent without one is capped at 1 MiB.

**G15 (Low): the repair agent's actions are not recorded.** Neither the prompts nor the agent's
individual tool calls are stored. The store keeps the classification answer, the final test, the
diff and the masked pytest output. After a suspected injection, those and the worker's logs are
all the evidence there is of what the agent did.

**G16 (Low): debug mode keeps clones and serves the API docs.** `debug: true` keeps every repair
clone under `<healer_home>/work/`, with the committed history and any committed data files. The
`HEALER_DEBUG` environment variable (which the `healer.yaml` template also uses for `debug`)
makes the API serve `/healer/docs` and `/healer/openapi.json` without authentication. The API
routes themselves still require it.

**G17 (Low): git skips its repository ownership check.** Every git command of the worker runs with
`safe.directory=*`, and the development stack sets it globally through `GIT_CONFIG_*`, so that
bind-mounted repositories owned by another user work. This turns off git's protection against a
repository whose configuration another user can write. Hooks and `core.fsmonitor` are disabled
for every git command of the worker (4.3), which removes the most direct way such a configuration
runs a program, but its other settings still apply.

- Until fixed: let the worker's OS user own the working tree.

---

## 6. Deployment recommendations

These recommendations close or reduce the gaps above until the code does.

1. **Isolate the worker.** Run it on a VM or host of its own, as a dedicated OS user, with nothing
   else on it but the container engine of its test sandbox. Do not run it inside the Airflow
   scheduler or the Airflow workers.
2. **The test sandbox** (G1):
   - keep `repo.sandbox.backend: container`, the default. `process` is for development and tests
     only;
   - prefer a rootless engine (rootless Podman or rootless Docker): the worker's access to a
     rootful engine is root on its host. Never give the engine socket to Airflow's processes, or
     to anything else that runs task code;
   - build a sandbox image with Python, pytest and the repository's test requirements, a non-root
     `USER`, no `VOLUME` and no secrets: no credentials, tokens or private keys, and no package
     index configuration with a password in it. Build it on the worker's engine (the sandbox never
     pulls) and rebuild it when the requirements change;
   - run `dag-healer doctor` after every change: its `sandbox` check must be OK;
   - for a boundary stronger than a shared kernel, consider gVisor or Kata Containers as the
     engine's runtime.
3. **Restrict network egress** to what the worker needs: the model provider (the Claude API,
   `api.anthropic.com`, or the Amazon Bedrock runtime endpoint of the region,
   `bedrock-runtime.<region>.amazonaws.com`, or its VPC endpoint), GitHub (`github.com`,
   `api.github.com`), the Airflow API, the store and the SMTP server. Block the Airflow metadata
   database, other internal services and the cloud metadata endpoints (`169.254.169.254`,
   `169.254.170.2`, `fd00:ec2::254`, `metadata.google.internal`), unless the worker itself needs
   them (G1): a worker that uses an EC2 instance role needs the instance metadata service.
4. **GitHub** (G2, G3), as [`docs/github/README.md`](github/README.md) describes:
   - use a fine-grained personal access token limited to the one repository, with "Contents"
     and "Pull requests" set to read and write, "Metadata: read" (which GitHub adds by itself)
     and no other permission: no "Workflows" and no "Administration". Or use a GitHub App
     installed on that repository only. Create the token for an account of its own with at
     most the Write role (a GitHub App acts as its own account): the pull requests are opened by
     that account, and GitHub does not let the author of a pull request approve it;
   - protect the base branch with
     [`docs/github/base-branch-ruleset.json`](github/base-branch-ruleset.json), and add the CI's
     checks as required status checks, so that nothing is merged before CI has run. GitHub
     shows classic branch protection only to an admin of the repository or to a GitHub App with
     "Administration: read", so the checks cannot read it with the token advised here: check it
     by hand, or use the ruleset;
   - keep `github.skip_ci: true`, and start CI on a healer branch only after you have read the
     change;
   - leave healer branches out of `pull_request_target` and `create` workflows with the job
     conditions of G2, and keep deployment secrets in environments that require a reviewer;
   - run `dag-healer doctor` before the worker gets the token, and after every change to the
     token, the rules or the workflows: its `github ...` lines must be OK. The pull request's
     own report comes after the `create` and `pull_request_target` workflows have run;
   - or run without a token, and apply `fix.patch` yourself.
5. **Amazon Bedrock** (`llm.provider: bedrock`, G1), as [`docs/bedrock.md`](bedrock.md)
   describes:
   - give the worker a role of its own (on EC2, an instance role) whose only permission is
     `bedrock:InvokeModel` on the chosen inference profile and the foundation models behind it,
     or on the in-Region model. Let an administrator enable the model once (the First Time Use
     form and the AWS Marketplace subscription), so that the role needs no `aws-marketplace`
     permission;
   - for health data and other regulated data, choose an in-Region model ID or the geographic
     inference profile of the data's jurisdiction, never a `global.` profile, deny global routing
     in the role's policy (`aws:RequestedRegion` equal to `unspecified`), and check the account's
     data retention mode in that Region;
   - reach Bedrock through a VPC endpoint (`com.amazonaws.<region>.bedrock-runtime`) whose policy
     allows only `bedrock:InvokeModel`;
   - keep the container backend of the test sandbox: under `process`, the code that pytest runs
     can get the instance role's credentials from the instance metadata service;
   - require IMDSv2, and raise its hop limit to 2 only on a host where the worker itself runs in
     a container: every container with network access on that host can then get the role's
     credentials;
   - prefer the role to Bedrock API keys (`AWS_BEARER_TOKEN_BEDROCK`) and to access keys in the
     environment, and run `dag-healer doctor` where the worker runs: its `llm` line must be OK.
6. **Airflow** (G7): give the worker a dedicated account with the smallest role that works, never
   Admin.
7. **Secrets** (G1, G8):
   - give the worker only the credentials it needs, in variables whose names contain `TOKEN`,
     `SECRET`, `PASSWORD` or `API_KEY`;
   - keep the Airflow deployment's own secrets (metadata database URL, Fernet key, JWT secret,
     connections) out of the worker's environment: the worker talks to Airflow through its REST
     API only;
   - task processes need only `HEALER_API_URL` and `HEALER_INGEST_TOKEN`. The development
     compose file keeps the Anthropic, AWS, GitHub and Airflow API credentials away from the
     scheduler, which runs task code, but it still gives every container the store URL (G6).
8. **The store** (G6): give the store URL only to the api-server and the worker, with a database
   user of its own.
9. **Configuration** (section 7): protect `healer.yaml` like deployment configuration, for
   example with a code-owner review, when it lives in the repository.
10. **The repository** (G1, G4, G5):
    - under the container backend, the code that pytest runs cannot reach the repository working
      tree. Under the `process` backend it can: give the worker a working tree that Airflow does
      not load DAGs from, or accept that a repair can change live DAG files;
    - keep secrets and real data out of the repository, and out of the working tree that the
      worker reads.
11. **Masking** (G5): flag personal columns with `pii: true`, add `masking.columns`, prefer
    `redact` for identifiers from a small set of values, and use a `masking.hook` for
    data-specific rules. Do not log raw rows, credentials or personal data from tasks.
12. **Transport** (G13): use STARTTLS or SMTPS for alerts, and HTTPS for the Airflow API when it
    is not local.
13. **Debug** (G16): keep `debug` and `HEALER_DEBUG` off in production.
14. **Review** every healer pull request like an outside contribution, the test as well as the
    fix. Watch for file, network, subprocess or environment access in either. Read its "Before
    you merge" section first, and start CI only once you have read the change.
15. **The development stack** under `docker/` uses fixed, publicly known secrets and gives the
    worker the Airflow `admin` account. It also mounts the host's container engine socket into
    `healer-worker`, so that the worker can start the test sandbox, which makes that container
    root-equivalent on the engine's host (on Docker Desktop and OrbStack, their Linux VM). Keep
    the stack on a local machine.

---

## 7. Assumptions and scope

- Administrators who control `healer.yaml`, the worker's environment, the installed packages
  (including a masking hook), the sandbox image and the deployment are trusted.
- `dag-healer init` suggests committing `healer.yaml` next to the DAGs. Then every committer can
  change dag-healer's settings, such as the masking rules, the allowed paths, where pull
  requests and alerts go, and whether CI runs on healer branches before review
  (`github.skip_ci`). So can code that escapes the test sandbox (G1). The API rereads the file
  at most every 10 seconds.
- Repository committers get the same trust as in the Airflow deployment itself: their code
  already runs in Airflow, and it also runs in the test sandbox during repairs.
- dag-healer relies on Airflow's own authentication and authorization, and inherits their
  strengths and weaknesses.
- The owner's agreement with Anthropic governs how the Claude API handles prompts. With Amazon
  Bedrock, the owner's agreements with AWS and Amazon Bedrock's data protection govern them
  instead.
- The offline `fake` LLM provider replays scripts from the repository. It is meant for demos and
  tests only.
- This document covers the Airflow adapter. Other orchestrator adapters, such as the AWS Step
  Functions adapter, get the same core sandbox rules, including the cloud-credential scrub, and
  the same gaps.
