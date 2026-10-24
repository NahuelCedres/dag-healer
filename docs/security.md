# dag-healer: security model and threat model

Status: describes dag-healer 0.1.x (alpha) with the Airflow adapter. Every mitigation listed in
section 4 is enforced by the code. Recommendations and planned work are listed separately, as
gaps (section 5) and deployment advice (section 6). Module paths are relative to
`src/dag_healer/`. To report a vulnerability, see [`SECURITY.md`](../SECURITY.md).

---

## 0. Summary

- dag-healer turns untrusted text into model prompts. Whoever controls a task's code or its input
  data can write task logs, tracebacks, exception messages, dataset rows and pushed failure
  events. That text is masked and sent to the model through the Anthropic API, and the model's
  answers drive what happens next.
- The model's output is untrusted too. A deterministic policy bounds the classification, the
  repair agent's file tools enforce path rules and size limits, and nothing is merged
  automatically.
- **The CODE path runs model-written code before a person reviews it.** The worker runs the
  generated test, and later the fix, with pytest under the worker's own OS user, with the
  worker's file system and network access. The pytest runner removes credentials from the test's
  environment and applies a timeout and resource limits. This guards against accidents, but it
  is not a sandbox for hostile code (gap G1).
- Masking reduces, but does not prevent, the flow of personal data and secrets to the model, the
  store, alerts and pull requests (G4, G5).
- Until these gaps are closed, deploy the worker as if it ran untrusted code: least-privilege
  credentials, restricted network egress and a GitHub token that can reach only the one
  repository (section 6).

---

## 1. System overview

| Component | Runs in | Holds |
|---|---|---|
| Airflow plugin: the API under `/healer`, the React views and the task listener | The Airflow api-server; the listener runs in every task process | The ingest token (API and listener), the store URL (API) |
| Worker (`dag-healer worker`) | Its own process or container | Airflow API credentials, Anthropic API key, GitHub token, SMTP password, store URL |
| Store | SQLite under `HEALER_HOME`, or the database at `store.url` / `HEALER_DATABASE_URL` | Incidents, events, commands, alerts |
| Repair workspace | `<healer_home>/work/<incident_id>`: a clone of the repository | The committed history of the repository |

The path of one failure:

1. **Detection.** The listener posts a failure hint (including the error text) to
   `POST /healer/api/v1/events/task-failed`, authenticated with the shared `X-Healer-Token`. The
   worker also polls the Airflow REST API. A hint is only stored: the worker confirms the failure
   through the Airflow API before it creates an incident.
2. **Evidence.** The worker fetches the task log and extracts the traceback. It reads the DAG
   file and the repository files named in the traceback, validates the DAG's data contract
   against the dataset files and builds a masked sample. Free text is masked before it is stored
   or sent anywhere.
3. **Classification.** The masked evidence goes to the model in one structured-output call. A
   deterministic policy takes the final decision.
4. **Follow-up.**
   - DATA: an alert with the violated rule and the masked sample.
   - AMBIGUOUS: the incident waits for a person in the dag-healer views.
   - CODE: a repair. In a fresh clone, the model first writes one failing test, then a minimal
     fix, each in its own tool session. pytest runs both. The result is committed on a
     `healer/...` branch. With a GitHub token and a repository (`github.repo` or a GitHub
     `origin` remote), the branch is pushed and a pull request is opened. Otherwise, or when the
     push or the pull request fails, the branch is pushed into the local repository, and
     `PR.md` and `fix.patch` are written.
5. **People act** from the UI: resolve an incident, retry the repair, retry the task or approve a
   contract. The API checks the user's Airflow permissions and queues a command in the store. The
   worker applies the command with its own credentials.

---

## 2. Assets

| Asset | Where it lives | Impact if it leaks or is misused |
|---|---|---|
| GitHub token (`github.token`, usually `GITHUB_TOKEN`) | The worker's environment. git gets it through `GIT_ASKPASS`, the REST API through a header | Pushing to the DAG repository (any branch that the token's scope and the branch protection allow), opening pull requests, reading private code. A classic token with the `repo` scope reaches every repository the account can write to. |
| Anthropic API key (`ANTHROPIC_API_KEY`) | The worker's environment, read by the Anthropic SDK | Model usage billed to the owner's account. |
| Airflow API credentials (`airflow.username`, `airflow.password`) | The worker's environment; exchanged for a JWT | Whatever the account's Airflow role allows. For an Admin: managing connections and variables, and clearing or triggering any run. |
| Ingest token (`ingest.token`, `HEALER_INGEST_TOKEN`) | The api-server, and every task process that runs the listener | Posting failure hints (each one is confirmed through the Airflow API). |
| Store URL (`store.url`, `HEALER_DATABASE_URL`) | The worker and the api-server (in the development stack, every Airflow container) | Reading every incident; queuing commands that the worker applies without further checks: approving contracts, starting repairs, retrying tasks (G6). |
| SMTP password (`notify.email.password`) | The worker | Sending mail as dag-healer. |
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
| Repository content (DAG files, modules, tests, committed data) | Repository committers | Sent to the model unmasked; imported and executed by pytest |
| Model output: the classification | The model, steered by the inputs above | Schema check, then the deterministic policy |
| Model output: the diagnosis, evidence and fix summary, and the repair agent's final summary | Same | Masked, then the store, the UI, the alerts and the pull request |
| Model output: tool calls, the generated test and the fix | Same | Path rules; written to the clone; **executed by pytest in the worker**; committed, pushed and proposed as a pull request |
| People using the UI and the API | Authenticated Airflow users | Airflow permission checks, then commands in the store that the worker applies |
| `healer.yaml` | Administrators, and every repository committer when the file is committed next to the DAGs | Trusted (section 7) |
| The worker's environment, installed packages (the masking hook) | Administrators | Trusted (section 7) |

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
- The classification is a structured-output call. The category must be `data`, `code` or
  `ambiguous`; anything else becomes ambiguous. The confidence is clamped to [0, 1].
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
  `conftest.py`.
- Never written in either phase: `healer.yaml`, `healer/**`, the contracts folder, `.github/**`,
  `.git/**`, lockfiles, dependency manifests (`requirements*.txt`, `pyproject.toml`, `setup.py`,
  `setup.cfg`, ...), `.env*`, `data/**`, `fixtures/**` and data files (`.csv`, `.parquet`,
  `.jsonl`, `.pkl`, `.db`, archives, ...).
- Never read or searched: `.git/**`, `.env*`, `healer.yaml`, `data/**`, `fixtures/**` and data
  files. `read_file` checks these rules on the path as written, not on the target of a symbolic
  link (G5).
- The work happens in a fresh clone of the committed history. The clone is deleted afterwards
  unless `debug` is on.
- Files that the tools did not write, for example files created by a test run, are reverted
  before the size check and before the commit. Files that git ignores and the clone's `.git`
  folder are not checked, and the files that the tools wrote are not checked again after the
  last test run.
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
  merges the pull request.

What remains: within these rules, the model chooses what the test and the fix contain. That code
is executed (4.3), pushed to the repository where CI may run it (G2), and shown to a reviewer who
may trust it. The path rules bind the model's file tools only. They do not bind the code that
pytest runs.

### 4.3 Running model-written code (the pytest sandbox)

During a repair the worker runs `python -m pytest` in the clone several times: the existing
suite, the generated test before the fix, the test and the suite after each fix attempt, and
every run that the agent asks for through its `run_tests` tool. The generated test, the
repository's own code and the fix all run before any person has reviewed them.

Enforced by `repo/pytest_runner.py`:

- **Environment scrub.** The test process does not inherit:
  - the Anthropic, GitHub and SMTP credentials, and every `HEALER_*` and `DAG_HEALER_*` setting;
  - Airflow's metadata database, API authentication, secrets backend, Celery, SMTP and e-mail
    settings, its Fernet and secret keys, `AIRFLOW_CONFIG`, connections (`AIRFLOW_CONN_*`) and
    variables (`AIRFLOW_VAR_*`);
  - `PYTHONPATH`, `PYTHONSTARTUP`, `PYTEST_*`, `GIT_ASKPASS`, `SSH_ASKPASS` and `SSH_AUTH_SOCK`;
  - any variable whose name contains `SECRET`, `TOKEN`, `PASSWORD`, `PASSWD`, `API_KEY`,
    `APIKEY`, `PRIVATE_KEY` or `CREDENTIAL`.
- **Cloud credentials.** The AWS credential variables (including the ECS and EKS
  container-credential variables), `GOOGLE_APPLICATION_CREDENTIALS` and `AZURE_CLIENT_SECRET` are
  always removed, after any override from the orchestrator adapter. AWS SDKs are told not to use
  instance metadata or the shared credential and config files.
- **Airflow** inside the tests points at a throw-away `AIRFLOW_HOME` with an SQLite database in
  unit-test mode.
- **Time.** After `repo.pytest_timeout` seconds (default 600), the whole process group is killed
  on POSIX. On Windows the process tree is killed with `taskkill /T`, best effort.
- **Resources (POSIX).** The soft limits of `RLIMIT_AS` (8 GiB), `RLIMIT_NPROC` (4096) and
  `RLIMIT_FSIZE` (1 GiB) are lowered in the child process, never raised. A limit that the kernel
  refuses is skipped: macOS does not let a process lower `RLIMIT_AS`, so there is no
  address-space limit on macOS. Windows gets no resource limits.
- **Output.** stdin is empty (`/dev/null`), and the repository's `addopts` and the pytest cache
  are ignored. The output goes to a temporary file, and only its last 20,000 characters are kept.
  That output is masked before it reaches the model, the store or the pull request.

What the runner does not do: it does not isolate the test from the worker (G1). Code in a test
runs as the worker's OS user, with the worker's network access. The credentials are gone from
the test's own environment, but the code can read them from the worker process
(`/proc/<parent pid>/environ` on Linux). It can plant a git hook in the clone, and the worker
later runs that hook with its full environment when it commits. It can also write wherever the
worker can, including the repository working tree.

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
  - code goes into fences longer than any run of backticks it contains, so it cannot break out of
    its block;
  - long sections are truncated; the full change stays on the branch and in `fix.patch`;
  - `PR.md` and `fix.patch` are written under `<healer_home>/prs/<incident_id>/` (the incident id
    must be a safe file name);
  - the API serves `PR.md` only from that folder, as `text/markdown` with
    `X-Content-Type-Options: nosniff` and `Cache-Control: no-store`.
- **Alerts** (`alerts/`) are built only from masked fields. A sample is included only when it is
  flagged as masked, and its cells are scrubbed again. HTML templates are autoescaped.
- **The GitHub token** is sent only in the `Authorization` header of REST calls and to git
  through `GIT_ASKPASS`. It never appears on a command line or in a URL. It is replaced by `***`
  in git output and in GitHub error messages.
- **Configuration.** The secret fields are `airflow.password`, `notify.email.password`,
  `github.token`, `ingest.token`, the adapter's secret options and a password inside `store.url`.
  They are meant to be `${ENV}` references. `dag-healer doctor` and `dag-healer check` warn about
  literal values, and `check` prints the configuration with secrets masked.
- **The UI** renders model text as plain text, never as HTML.

What remains: files named in a traceback are read without the deny rules and sent unmasked (G4).
Masking is pattern-based and misses secrets, names and addresses in free text, and source code,
the generated test and the diff are never masked (G5). Model-written Markdown goes into the pull
request body (G10).

Data that leaves the deployment:

- **To the Anthropic API:** the masked evidence, the DAG file, every repository file named in the
  traceback, every repository file that the repair agent reads, and the masked pytest output.
- **To GitHub:** the branch and the pull request, with their masked text, the generated test and
  the diff, visible to everyone who can see the repository.

### 4.5 GitHub token scope and the repository

Enforced:

- The token is used for two things only:
  - `git push` of the `healer/...` branch, through a `GIT_ASKPASS` helper that reads the token
    from the environment of that one git process. Credential helpers are disabled for that push,
    so the token is never stored.
  - The GitHub REST endpoints that open or find a pull request. dag-healer never calls a merge
    endpoint.
- With a token, the branch is pushed to `https://github.com/<owner>/<name>.git`, built from
  `github.repo` or from a GitHub `origin` remote. Any user info in the URL is dropped.
- The worker refuses to push to the branch it cloned and never force-pushes.
- Without a token or a repository, nothing is pushed over the network. The branch is pushed into
  the local repository, and `PR.md` and `fix.patch` are written.

What remains:

- dag-healer enforces these restrictions in its own code. GitHub enforces only the token's scope
  and the repository's rules. A leaked token (G1) can do everything its scope allows (G3).
- The `healer/...` branch is pushed to the repository itself, not to a fork. Workflows triggered
  by `push` or `pull_request` on that branch run the model-written code in CI, with the
  repository's secrets, before review (G2).

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
| G1 | Critical | Model-written code runs with the worker's privileges |
| G2 | High | Healer branches run in the repository's CI before review |
| G3 | High | Only the token's scope limits what the GitHub token can do |
| G4 | Medium | Files named in a traceback are read and sent as they are |
| G5 | Medium | Masking is pattern-based, and code is never masked |
| G6 | Medium | The worker trusts every command in the store |
| G7 | Medium | One Airflow account acts for everyone |
| G8 | Medium | The environment scrub works by variable name |
| G9 | Medium | The prompts do not mark the evidence as untrusted |
| G10 | Low | Model-written Markdown goes into the pull request body |
| G11 | Low | The resource limits and the timeout do not stop hostile code |
| G12 | Low | Read permissions are coarser than they look |
| G13 | Low | Alert e-mail uses plain SMTP by default |
| G14 | Low | Every task process shares one ingest token |
| G15 | Low | The repair agent's actions are not recorded |
| G16 | Low | Debug mode keeps clones and serves the API docs |
| G17 | Low | git skips its repository ownership check |

**G1 (Critical): model-written code runs with the worker's privileges.** The pytest runner is a
guard against accidents, not a sandbox for hostile code. Code in a generated test or fix, or in
the repository's own tests, runs:

- **With the worker's network access.** It can reach the Airflow API and metadata database,
  internal services, the internet and cloud metadata endpoints over plain HTTP. Only AWS SDKs are
  told not to use instance metadata; Google and Azure SDKs are not.
- **Next to the worker's secrets.** On Linux it can read the worker's environment from
  `/proc/<parent pid>/environ`, including every credential that the scrub removed from its own
  environment.
- **With a way out of the sandbox.** It can write git hooks or git configuration into the clone's
  `.git` folder, or into the source repository's. The worker runs them with its full environment
  the next time it runs git there: a `pre-commit` hook, for example, runs when the fix is
  committed.
- **With the worker's file system access.** That includes the repository working tree. In the
  development stack, that working tree is also the folder that Airflow loads DAGs from, so a test
  can change live DAG code. It also includes `healer.yaml`, `HEALER_HOME` (the SQLite store, the
  PR artifacts) and the data files that the repair tools refuse to read.

The `/proc` read and the planted `pre-commit` hook were both checked on Linux. Any incident
classified CODE reaches this code path. Through prompt injection (4.1, 4.2), whoever controls a
log line or a data value can try to get code of their choice into the test.

- Until fixed: section 6 (isolated worker, egress allow-list, least-privilege credentials, a
  separate working tree).
- Possible fix: run pytest in a separate container or sandbox with no network, no credentials
  and its own copy of the clone. Run the worker's git commands with hooks disabled, and restore
  the clone's `.git/config` after each test run.

**G2 (High): healer branches run in the repository's CI before review.** The `healer/...` branch
is pushed to the repository itself, not to a fork, and the pull request is opened from it. On
GitHub, workflows triggered by `push` or `pull_request` for a branch of the same repository run
with the repository's secrets. Any such workflow runs the model-written test and fix before a
person has looked at them. dag-healer does not see or change the target repository's CI.

- Until fixed: exclude `healer/**` from workflows that use secrets (`branches-ignore`), keep
  deployment secrets in environments that require a reviewer, or run without a GitHub token
  (PR.md mode).
- Possible fix: an option to push to a fork.

**G3 (High): only the token's scope limits what the GitHub token can do.** The worker's code
pushes only `healer/...` branches and only opens pull requests. GitHub itself enforces only the
token's scope and the repository's rules, and the token sits in the worker's environment, where
G1 applies. A token with write access to the repository contents can push to any unprotected
branch, the base branch included. The development `docker/.env.example` asks for a classic token
with the `repo` scope, which covers every repository the account can write to.

- Until fixed: use a fine-grained personal access token or a GitHub App installation token,
  limited to the one repository, with "Contents: read and write" and "Pull requests: read and
  write". Protect the base branch with a ruleset or branch protection: require a pull request with
  an approving review, and block force pushes and deletion.

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
matches none of the rules reaches the test process. Examples: `GH_PAT`, `DB_PASS`, a
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

**G11 (Low): the resource limits and the timeout do not stop hostile code.**

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
repository whose configuration another user can write.

- Until fixed: let the worker's OS user own the working tree.

---

## 6. Deployment recommendations

These recommendations close or reduce the gaps above until the code does.

1. **Isolate the worker.** Run it in its own container or VM, as a dedicated OS user, with
   nothing else in it. Do not run it inside the Airflow scheduler or the Airflow workers.
2. **Restrict network egress** to what the worker needs: the Anthropic API (`api.anthropic.com`),
   GitHub (`github.com`, `api.github.com`), the Airflow API, the store and the SMTP server. Block
   the Airflow metadata database, other internal services and the cloud metadata endpoints
   (`169.254.169.254`, `169.254.170.2`, `fd00:ec2::254`, `metadata.google.internal`), unless the
   worker itself needs them (G1).
3. **GitHub** (G2, G3):
   - use a fine-grained token or a GitHub App, limited to the one repository, with "Contents"
     and "Pull requests" set to read and write;
   - protect the base branch;
   - keep CI secrets away from workflows that run on `healer/**`;
   - or run without a token, and apply `fix.patch` yourself.
4. **Airflow** (G7): give the worker a dedicated account with the smallest role that works, never
   Admin.
5. **Secrets** (G1, G8):
   - give the worker only the credentials it needs, in variables whose names contain `TOKEN`,
     `SECRET`, `PASSWORD` or `API_KEY`;
   - keep the Airflow deployment's own secrets (metadata database URL, Fernet key, JWT secret,
     connections) out of the worker's environment: the worker talks to Airflow through its REST
     API only;
   - task processes need only `HEALER_API_URL` and `HEALER_INGEST_TOKEN`. The development
     compose file keeps the Anthropic, GitHub and Airflow API credentials away from the
     scheduler, which runs task code, but it still gives every container the store URL (G6).
6. **The store** (G6): give the store URL only to the api-server and the worker, with a database
   user of its own.
7. **Configuration** (section 7): protect `healer.yaml` like deployment configuration, for
   example with a code-owner review, when it lives in the repository.
8. **The repository** (G1, G4, G5):
   - where your deployment allows it, give the worker a working tree that Airflow does not load
     DAGs from. Otherwise, accept that a repair can change live DAG files;
   - keep secrets and real data out of the repository, and out of the working tree that the
     worker reads.
9. **Masking** (G5): flag personal columns with `pii: true`, add `masking.columns`, prefer
   `redact` for identifiers from a small set of values, and use a `masking.hook` for data-specific
   rules. Do not log raw rows, credentials or personal data from tasks.
10. **Transport** (G13): use STARTTLS or SMTPS for alerts, and HTTPS for the Airflow API when it
    is not local.
11. **Debug** (G16): keep `debug` and `HEALER_DEBUG` off in production.
12. **Review** every healer pull request like an outside contribution, the test as well as the
    fix. Watch for file, network, subprocess or environment access in either.
13. **The development stack** under `docker/` uses fixed, publicly known secrets and gives the
    worker the Airflow `admin` account. Keep it on a local machine.

---

## 7. Assumptions and scope

- Administrators who control `healer.yaml`, the worker's environment, the installed packages
  (including a masking hook) and the deployment are trusted.
- `dag-healer init` suggests committing `healer.yaml` next to the DAGs. Then every committer can
  change dag-healer's settings, such as the masking rules, the allowed paths, and where pull
  requests and alerts go. So can code that escapes the test sandbox (G1). The API rereads the file
  at most every 10 seconds.
- Repository committers get the same trust as in the Airflow deployment itself: their code
  already runs in Airflow, and it also runs in the worker during repairs.
- dag-healer relies on Airflow's own authentication and authorization, and inherits their
  strengths and weaknesses.
- The owner's agreement with Anthropic governs how the Anthropic API handles prompts.
- The offline `fake` LLM provider replays scripts from the repository. It is meant for demos and
  tests only.
- This document covers the Airflow adapter. Other orchestrator adapters, such as the AWS Step
  Functions adapter, get the same core sandbox rules, including the cloud-credential scrub, and
  the same gaps.
