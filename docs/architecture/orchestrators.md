# dag-healer: orchestrator abstraction (ports and adapters)

Status: final design. It merges the three proposals with the verified Step Functions fact sheet, and it is meant to be committed as `docs/architecture/orchestrators.md`. Implementation starts after the phase 2-5 commit lands.

Tags used in this document:
- **[V]**: verified in an AWS or vendor document, either by the fact sheet or by an extra check made while writing this design.
- **[I]**: an inference from verified facts.
- **[U]**: not verified. Every [U] item sits behind exactly one function or one config template and is covered by a fixture that a recorded response will replace.

Extra checks made while writing this design (all [V]):
- `ListExecutions` returns the most recent execution first. Running executions are ordered by `startDate`/`redriveDate`, the others by `stopDate`. `statusFilter` takes a single value.
- In `GetExecutionHistory`, `includeExecutionData` governs only the "input or output of a history event".
- In the documented history sample, `PassStateEntered` has `previousEventId: 0`.
- EventBridge connection `AuthorizationType` is `BASIC | OAUTH_CLIENT_CREDENTIALS | API_KEY`, and `ApiKeyAuthParameters{ApiKeyName, ApiKeyValue}` defines a header.
- React 19 no longer publishes UMD builds.
- ALB authentication:
  - `x-amzn-oidc-data` is an ES256 JWT, and its *header* carries `signer`, `kid` and `exp`.
  - The public key comes from `https://public-keys.auth.elb.{region}.amazonaws.com/{key-id}`. GovCloud uses different S3 endpoints.
  - `x-amzn-oidc-identity` and `x-amzn-oidc-accesstoken` are unsigned.
  - AWS recommends restricting targets to the load balancer's security group.
  - `OnUnauthenticatedRequest: deny` returns 401 to AJAX calls.

---

## 0. Decision summary

| Topic | Decision | Taken from | Why |
|---|---|---|---|
| Package layout | `dag_healer/airflow/` **stays where it is** and becomes the Airflow adapter. New packages: `core/`, `orchestrators/`, `hosting/`, `stepfunctions/`. Nothing moves except three dataclass definitions and three string constants, which are re-exported from their old modules. | P3 + P1 | The `airflow.plugins` entry point, the listener module object and `dag_healer.callback` keep their identity, so no shim modules are needed. |
| Port shape | One `OrchestratorAdapter` bundle made of small optional ports: `FailureFeed`, `PushIntake`, `HintQueue`, `RunInspector`, `CodeLocator`, `Remediator`, `NativeLinks`, plus `Vocabulary`, `Capabilities` and `ErrorTaxonomy` | P2/P3, slimmed | Adapters implement only what their orchestrator offers, and capabilities follow from which ports are present. |
| Error interchange | Structured `AttemptDetail` (error name, message, raw cause) **plus** a Python-format `traceback_text` that each adapter renders | P2 + P1 | The existing `extract_traceback` / `map_to_repo_path` / policy / repair keep working unchanged, and structured fields are not lost. |
| Push events | Hints only. The worker confirms each one against the orchestrator API and creates the incident from the **confirmed** ref. Run-level hints (EventBridge) get their step localised during confirmation. | P1 + P3 | Keeps the rule "the API is the truth, events are hints", and also defeats forged events. |
| Step Functions push transport | EventBridge → SQS → `HintQueue` (recommended); EventBridge API destination → `POST /events/eventbridge` (optional); polling always on | Fact sheet + P1/P3 | Delivery is best effort and out of order [V]. SQS needs no inbound endpoint. |
| Store key | Keep the physical columns `dag_id/run_id/task_id/map_index/try_number`, add ORM synonyms, add an `orchestrator` (instance) column **to the unique key** in schema v2 | P2/P3 | Running Airflow and Step Functions side by side during a migration (the user's scenario) needs two workers sharing one store. Changing the key is cheapest now, while only dev and demo DBs exist. |
| `map_index` / `attempt` types | Stay `int`. `attempt = redriveCount + 1`. `Retry` attempts are counted in `native_ref`. A Distributed Map failure gives one incident on the parent. | P1/P3 (deviates from the fact sheet's [I] proposal of string keys) | See section 2.3. Retries are never incidents, because incidents are only for terminal attempts (fact sheet). |
| HTTP API | Wire contract v1 frozen: same `/healer/api/v1/dags/{dag_id}/...` routes and JSON field names, additive fields only | P1/P2 | The Airflow UI and Airflow's `requires_access_dag` (which reads the `dag_id` path parameter) stay unchanged. A neutral `/api/v2` is future work. |
| Auth | `AuthBackend` bundles the user dependency and the existing `Authorizer` protocol. Wiring uses the two dependency overrides that already exist (`get_current_user`, `get_authorizer`). | P1 | No route changes. The tests already use this exact mechanism. |
| Standalone UI | `dag-healer serve` mounts the same `create_app()` at `/healer` and serves a second Vite build (React bundled) on **the same URL shapes** as the Airflow host | P1 | `links.py`, `ui/src/links.ts` and the `pluginContext` pathname resolver are reused unchanged. |
| Contracts | `dag_id`/`task_id` stay the canonical YAML keys. `pipeline_id`/`step_id` are accepted only as input aliases. | P3 | `compute_source_hash` hashes `contract_to_dict()`. Renaming the keys would mark every approved contract stale. |
| Poll cursor | New `healer_kv` table, one key per orchestrator instance. A phase-5 `state.json` is imported once. | P2/P3 | Safe with several containers and several instances. |
| Sandbox | The core always scrubs cloud credentials and disables IMDS in the pytest sandbox | all three | LLM-written tests run user code inside a worker that has AWS permissions. |
| "Repaired" ≠ "retry now" | Remediation gets a delivery gate (`fix_delivery = "ci_deploy"` for Step Functions). The UI warns that a redrive reruns the *deployed* code with the *original* definition. | Fact sheet §8 | Merging a PR does not deploy anything on AWS. |

---

## 1. Goals and non-goals

### Goals

1. **Neutral core.** The core is `agent/`, `worker/`, `store/`, `contracts/`, `alerts/`, `pr/`, `repo/{git,profile,pytest_runner}.py`, `masking.py`, `datasets.py`, `links.py`, `api/` (except `api/auth.py`) and `ui/`. It never imports `airflow`, `boto3`, `botocore`, `dag_healer.airflow.*` (other than the two re-exported constants, which move to `core`) or `dag_healer.stepfunctions.*`. It reaches an orchestrator only through `dag_healer.core.ports`, from an adapter that `dag_healer.orchestrators.registry` returns or that is injected.
2. **Airflow is one adapter.** It passes the same conformance suite as the fake orchestrator and the Step Functions reference adapter.
3. **The Airflow plugin UX stays exactly as it is:**
   - same entry point `dag_healer = "dag_healer.airflow.plugin:HealerPlugin"`, same `HealerPlugin` class, same listener module object;
   - same react_apps: names `Healer` / `Healer incidents` / `Healer incident`, destinations nav / dag / task_instance, url_routes `healer` / `healer-dag` / `healer-ti`, same `main.umd.cjs` bundle with React from the host;
   - same `/healer` prefix, the same `/healer/api/v1` routes and JSON field names (new fields are additive only), the same CSRF rules;
   - same `dag_healer.callback`, the same `POST /events/task-failed` ingest payload;
   - same email and PR.md text, and the same prompt text (snapshot-tested byte for byte);
   - every existing `healer.yaml`, contract file (including `source_hash`) and store keeps working with no manual step except the store migration, which runs on its own.
4. **A Step Functions reference adapter** that proves the seam and can be built and unit-tested on the Windows host with no AWS credentials (pure parsers plus botocore `Stubber`).
5. **A standalone host** (`dag-healer serve`) that serves the same API and React views for orchestrators that have no UI plugin system, with pluggable authentication and per-pipeline permissions.
6. **Third-party adapters** can ship as separate distributions through the `dag_healer.orchestrators` entry-point group and validate themselves with the packaged conformance suite.

### Non-goals (this refactor)

- A complete Step Functions product. Express workflows, Distributed Map child-level incidents, ECS/Batch/Glue log correlation, CDK/Terraform/Serverless code mapping, an IAM-derived authorizer and the deployed-fix check are designed here but marked as future work (section 12.3).
- Several orchestrator instances in one worker process. One `healer.yaml` means one instance and one worker. Instances can share a store.
- Renaming the physical DB columns, the v1 wire fields (`dag_id`, `task_id`, ...), the product name or the `HEALER_*` environment variables.
- Alembic. A small in-package, version-gated migration runner is added instead.
- Repairing non-Python code. Repair stays pytest-based. Node and Java Lambdas, Glue/Spark scripts and ASL definitions can be classified but never get a CODE repair.

---

## 2. Core concepts and neutral identifiers

### 2.1 Concept mapping

| Neutral concept | Airflow 3 | AWS Step Functions | Dagster | Prefect |
|---|---|---|---|---|
| **orchestrator instance** (`orchestrator`) | one Airflow deployment (`airflow`) | one account + region (`sfn-prod-euw1`); state machine names are unique per account and region [V] | one instance / deployment | one server / workspace |
| **pipeline** (`pipeline_id` ↔ column `dag_id`) | `dag_id` | stable id from `healer.yaml` (`options.pipelines`), defaulting to the state machine **name**; the ARN goes in `native_ref` | job name | flow name (or deployment) |
| **run** (`run_id`) | DAG run `run_id` | Standard: execution **name** (≤ 80 chars; reusable 90 days after close [V], see 2.2). Express: future. | `run_id` | flow-run UUID |
| **step** (`step_id` ↔ `task_id`) | `task_id` (task-group prefix included) | name of the innermost failing state; state names are unique across the whole machine and ≤ 80 chars [V] | `step_key` (dynamic mapping key included; format [U]) | task-run key / name |
| **fan-out index** (`map_index`, int, -1 = none) | `map_index` | Inline Map iteration `index` from `MapIteration*EventDetails` [V], else -1; Distributed Map: -1 on the parent incident (2.3) | -1 (mapping key is in the step key) | `.map()` index / -1 |
| **attempt** (`attempt` ↔ `try_number`, ≥ 1) | `try_number` | `redriveCount + 1` [V field]; `Retry` attempts are internal (`native_ref.retry_attempts`) | step attempt / re-execution | `run_count` [V] |
| **terminal failure** | TI state `failed` (vs `up_for_retry`) | execution status `FAILED` / `TIMED_OUT` (`ABORTED` opt-in: a person stopped it) | run `FAILURE` | flow run `Failed` |
| **push detection** | listener `on_task_instance_failed`, `on_failure_callback` → `POST /events/task-failed` | EventBridge "Step Functions Execution Status Change" (Standard only, best effort, can arrive out of order, **no state name**) [V] → SQS or API destination | `@run_failure_sensor` [V] → webhook | Automation `EventTrigger` on `prefect.flow-run.Failed` + `call-webhook` [V] |
| **pull detection (truth)** | REST `taskInstances?state=failed` | `ListExecutions(statusFilter)` + `DescribeExecution` + `GetExecutionHistory` [V] | GraphQL `runsOrError(filter:{statuses:[FAILURE]})` [V] | `POST /api/flow_runs/filter` + `/api/task_runs/filter` [V] |
| **step error** | structured log `error_detail` | history `*FailedEventDetails.{error,cause}` [V]; a Lambda cause is JSON `{errorMessage,errorType,stackTrace}` [V shape; requestId U] | step failure event `error.to_string()` [V] | state message |
| **logs** | `/api/v2/.../logs/{try}` | CloudWatch Logs of the compute (Lambda `/aws/lambda/<fn>` or `LoggingConfig.LogGroup` [V]) | `logsForRun` + `capturedLogsMetadata` [V] | logs filter API (request shape [U]) |
| **pipeline definition** | DAG file (`fileloc`) / `dagSources` | ASL from `DescribeStateMachineForExecution` (the definition that ran) [V] / repo `*.asl.json` / IaC | code location module | deployment entrypoint |
| **step code** | `python_callable` (AST dagscan) | Task `Resource` → Lambda → IaC (SAM `CodeUri` + `Handler`, CDK `aws:asset:path`, Terraform `archive_file.source_dir`, Serverless `handler`) [V] | op / asset function | task function |
| **runtime path → repo** | `/opt/airflow/repo`, dags folder | `/var/task` → the function's `CodeUri`, `/opt/python` → layer [I] | image paths | image paths |
| **datasets** | files in the repo | S3 objects (Distributed Map `ItemReader`, handler literals) | assets | blocks / artifacts |
| **remediation** | `clearTaskInstances` (one task) | `RedriveExecution` (resumes every failed branch or iteration, same definition) [V] or `StartExecution` with the same input | `ReexecutionOptions.from_failure` [V] | `prefect flow-run retry` [V] |
| **fix delivery** | repo sync (git-sync or a mounted repo) | CI deploy (a merged PR does not deploy) | CI / code location reload | deployment build |
| **user permissions** | Airflow auth manager | standalone RBAC (an IAM-simulated authorizer later) | standalone | standalone |
| **UI host** | embedded `react_apps` | standalone `dag-healer serve` | standalone | standalone |
| **native deep link** | `dags/<d>/runs/<r>/tasks/<t>[/mapped/<i>]` | console execution URL (pattern [U], configurable template) | run page | flow-run page (`flow_run\|ui_url` [V]) |

### 2.2 Identity rules

- **Incident key** = `(orchestrator, pipeline_id, run_id, step_id, map_index, attempt)`, stored as `(orchestrator, dag_id, run_id, task_id, map_index, try_number)`. It is unique in the store.
- `orchestrator` is the **instance id** from `healer.yaml` (`orchestrator.instance`, default = the adapter type). Prod and staging Airflow, or two AWS regions, are two instances.
- **Sentinels**, both in `core/ids.py`:
  - `UNRESOLVED_STEP = ""`: a push hint whose step is not known yet. It appears only on `healer_pending_events`.
  - `RUN_LEVEL_STEP = "__run__"`: a failure that has no step, such as an execution timeout or abort, or a history still unreadable after the confirmation retries. The UI shows it as "run-level failure".
- **`native_ref`** (JSON column) holds adapter-owned, JSON-safe pointers such as the execution ARN, event ids, `retry_attempts`, `map_run_arn`, the Lambda request id and the start date. The core never interprets it and passes it back to the adapter.
  - **Privacy rule:** execution input or output, `cause` text and data rows never go into `native_ref`. The conformance suite enforces a 4 KB limit and a payload-key denylist.
- **Error text** travels raw from the adapter and is masked by the core (`masking.mask_text`) before any persistence or prompt. The core is the single masking point (P2).
- **Execution-name reuse** (Step Functions names can be reused 90 days after close [V]): `native_ref.start_date` is stored. If a key collides with a different `start_date`, the adapter sets `run_id = f"{name}@{startDate_ms}"`.

### 2.3 Why `map_index` and `attempt` stay integers (deviation from the fact sheet's inference)

The fact sheet *infers* [I] that the key should become `map_key: str` and `attempt_key: str` (`"redrive<n>.retry<m>"`). None of the verified facts forces this.

- **Attempt.** Incidents are raised only for **terminal** attempts (fact sheet §4, matching Airflow's `failed` vs `up_for_retry`). Within one redrive segment a Standard execution has at most one terminal step failure [I]: an uncaught failure ends the execution; an inline Map or Parallel fails on its first failing iteration or branch and then propagates or is caught; a caught failure is not terminal. Retry counters reset on redrive [V]. So `redriveCount + 1` identifies the terminal attempt uniquely, and the in-segment retry count is evidence (`native_ref.retry_attempts`), not identity.
- **Distributed Map.** A child execution name is a string, but one incident per *child* would turn one bad input file into up to 10,000 incidents (default concurrency [V]). Retry and redrive also act on the parent or Map Run, not on one child [V]. The design files **one incident for the parent execution**, with `step_id = <Map state>` and `native_ref.{map_run_arn, failed_children (sample of up to 5), item_counts}`. That fits an integer `map_index = -1`.
- **Dagster.** Its step key already contains dynamic mapping keys (format [U]), so `map_index = -1`.

If per-child incidents are ever needed, schema v3 adds a nullable `map_key VARCHAR(256)` to the key. That is the only change this decision defers.

### 2.4 Neutral names versus physical names

| Neutral (Python, new API fields, prompts) | Physical (DB column, existing JSON keys, contract YAML) |
|---|---|
| `orchestrator` | `orchestrator` (new) |
| `pipeline_id` | `dag_id` |
| `run_id` | `run_id` |
| `step_id` | `task_id` |
| `map_index` | `map_index` |
| `attempt` | `try_number` |

The ORM exposes both names: the column attribute plus `sqlalchemy.orm.synonym`.

---

## 3. Ports

Every type below lives in `src/dag_healer/core/`. These modules are stdlib only, need Python ≥ 3.10, start with `from __future__ import annotations`, and import no pydantic, httpx, boto3, airflow or pandas. Third-party types appear only under `TYPE_CHECKING`.

### 3.1 `core/ids.py`

```python
"""Neutral identifiers (stdlib only)."""
from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass, replace
from typing import Any

NO_MAP_INDEX = -1
UNRESOLVED_STEP = ""
"""``step_id`` of a push hint whose failing step is not known yet (pending events only)."""
RUN_LEVEL_STEP = "__run__"
"""``step_id`` of a failure without a step (execution timeout/abort, unreadable history)."""


@dataclass(frozen=True, slots=True)
class PipelineRef:
    """A pipeline of one orchestrator instance (Airflow DAG, state machine, Dagster job)."""

    orchestrator: str
    pipeline_id: str


@dataclass(frozen=True, slots=True)
class RunRef:
    """One run of a pipeline (DAG run, execution, flow run)."""

    orchestrator: str
    pipeline_id: str
    run_id: str

    @property
    def pipeline(self) -> PipelineRef:
        return PipelineRef(self.orchestrator, self.pipeline_id)


@dataclass(frozen=True, slots=True)
class AttemptRef:
    """One attempt of one step (fan-out element) of one run: the incident key.

    Store columns: dag_id = pipeline_id, task_id = step_id, map_index = fan-out index
    (-1: none), try_number = attempt (1-based).
    """

    orchestrator: str
    pipeline_id: str
    run_id: str
    step_id: str
    map_index: int = NO_MAP_INDEX
    attempt: int = 1

    @property
    def run(self) -> RunRef:
        return RunRef(self.orchestrator, self.pipeline_id, self.run_id)

    @property
    def pipeline(self) -> PipelineRef:
        return PipelineRef(self.orchestrator, self.pipeline_id)

    @property
    def resolved(self) -> bool:
        """False while the failing step is unknown (``step_id == UNRESOLVED_STEP``)."""
        return self.step_id != UNRESOLVED_STEP

    def key(self) -> tuple[str, str, str, str, int, int]:
        """``(orchestrator, pipeline_id, run_id, step_id, map_index, attempt)``."""
        return (self.orchestrator, self.pipeline_id, self.run_id, self.step_id, self.map_index, self.attempt)

    def store_key(self) -> dict[str, Any]:
        """Physical store columns for ``store.repo`` calls."""
        return {"orchestrator": self.orchestrator, "dag_id": self.pipeline_id, "run_id": self.run_id,
                "task_id": self.step_id, "map_index": int(self.map_index), "try_number": int(self.attempt)}

    def with_step(self, step_id: str, *, map_index: int = NO_MAP_INDEX) -> AttemptRef:
        """Copy with the step localised (resolving an ``UNRESOLVED_STEP`` hint)."""
        return replace(self, step_id=step_id, map_index=map_index)

    def next_attempt(self) -> AttemptRef:
        return replace(self, attempt=self.attempt + 1)

    @classmethod
    def from_row(cls, row: Mapping[str, Any], *, default_orchestrator: str = "airflow") -> AttemptRef:
        """Build from ``Incident.to_dict()`` / ``PendingEvent.to_dict()`` (physical keys)."""
        ...
```

### 3.2 `core/failures.py`

```python
class AttemptState(str, Enum):
    FAILED = "failed"        # terminal: no automatic retry left
    RETRYING = "retrying"    # failed; an automatic retry follows (Airflow up_for_retry, SFN Retry)
    CAUGHT = "caught"        # failed; handled by the pipeline itself (SFN Catch)
    RUNNING = "running"
    SUCCEEDED = "succeeded"
    UNKNOWN = "unknown"


Trigger = Literal["final_failure", "every_failure"]


@dataclass(frozen=True)
class Failure:
    """A confirmed failed attempt as the orchestrator reports it (the source of truth)."""

    ref: AttemptRef                    # always resolved (or RUN_LEVEL_STEP)
    state: AttemptState
    is_final: bool                     # no automatic retry left; drives monitor.trigger
    source: str                        # "rest" | "poll" | "listener" | "callback" | "eventbridge" | "sqs" | "push" | "cli"
    error_name: str | None = None      # "KeyError" | "States.Timeout" | "AirflowTaskTimeout"
    error_summary: str = ""            # RAW (unmasked); the core masks before persisting
    failed_at: datetime | None = None  # aware UTC
    native: Mapping[str, Any] = field(default_factory=dict)  # JSON-safe, no payloads (-> native_ref)


@dataclass(frozen=True)
class FailureHint:
    """An unconfirmed push notification. Forgeable: always confirmed before an incident exists."""

    ref: AttemptRef                    # step_id may be UNRESOLVED_STEP
    channel: str                       # "task-failed" | "eventbridge"
    source: str                        # incident source if confirmed ("listener", "callback", "eventbridge", "sqs")
    error_text: str = ""               # RAW; masked by the core before it is stored
    received_at: datetime | None = None
    native: Mapping[str, Any] = field(default_factory=dict)


@dataclass(frozen=True)
class Confirmation:
    """Result of checking a hint against the orchestrator API."""

    verdict: Literal["confirmed", "discarded", "retry_later"]
    reason: str                        # shown in the pending-event row and logs (masked)
    failures: tuple[Failure, ...] = ()  # confirmed + localised; usually one


@dataclass(frozen=True)
class FailurePage:
    failures: tuple[Failure, ...]
    cursor: Mapping[str, Any]          # opaque, JSON-safe; persisted in healer_kv per instance
    complete: bool = True              # False: page cap hit, poll again on the next cycle
```

### 3.3 `core/evidence.py`

```python
@dataclass(frozen=True)
class AttemptDetail:
    """Everything the orchestrator knows about one failed attempt (RAW text)."""

    ref: AttemptRef
    state: AttemptState
    is_final: bool
    error_name: str | None
    error_message: str | None
    traceback_text: str = ""           # Python format ("Traceback (most recent call last):") when derivable
    raw_error: str = ""                # orchestrator-native error text (Airflow error_detail, SFN cause)
    executor: str | None = None        # operator class | "lambda" | "ecs" | "glue" ...
    resource: str | None = None        # Lambda function ARN/name, operator path ...
    started_at: datetime | None = None
    ended_at: datetime | None = None
    max_attempts: int | None = None
    native: Mapping[str, Any] = field(default_factory=dict)


@dataclass(frozen=True)
class LogBundle:
    text: str                          # RAW, chronological, already rendered to lines
    source: str                        # "airflow:task-log" | "cloudwatch:/aws/lambda/fn" | "none"
    confidence: Literal["exact", "heuristic", "none"] = "exact"
    truncated: bool = False
    notes: tuple[str, ...] = ()


@dataclass(frozen=True)
class PipelineSource:
    """Where (and optionally what) the orchestrator says a pipeline is."""

    path: str | None = None            # as reported (runtime absolute path, repo-relative path or ARN)
    relative_path: str | None = None   # relative to the orchestrator's code root (Airflow relative_fileloc)
    text: str | None = None            # DAG source / ASL JSON, when asked for and available
    language: Literal["python", "asl-json", "asl-yaml", "unknown"] = "python"
    origin: str = ""                   # "api:dagSources" | "api:DescribeStateMachineForExecution" | "repo:<path>"
```

### 3.4 `core/specs.py` (static code model)

`TaskInfo`, `DatasetRef` and `DagInfo` **move here unchanged**, the same dataclasses with the same fields. `repo/dagscan.py` imports and re-exports them, so `dag_healer.repo.dagscan.TaskInfo is dag_healer.core.specs.TaskInfo` and `isinstance` checks and pickles keep working.

```python
@dataclass
class TaskInfo: ...      # task_id, operator, callable_name, callable_module, callable_file, lineno (unchanged)
@dataclass
class DatasetRef: ...    # task_id, path (repo-relative posix OR URI such as s3://b/k), type, resolved, source
@dataclass
class DagInfo: ...       # dag_id, file, tasks, datasets, imports; task(), to_dict() (unchanged)

StepSpec = TaskInfo      # neutral aliases: task_id = step id, operator = step kind ("lambda", "PythonOperator")
PipelineSpec = DagInfo   # dag_id = pipeline id, file = definition file (DAG .py, *.asl.json, template.yaml)


@dataclass(frozen=True)
class PathMapping:
    """Maps a runtime path prefix seen in tracebacks to a repo-relative base."""

    runtime_prefix: str                # "/opt/airflow/repo" | "/var/task"
    repo_base: str                     # "" | "functions/transform"
```

### 3.5 `core/vocab.py`

```python
@dataclass(frozen=True)
class Vocabulary:
    """Words used in prompts, PR.md, emails and the UI. Airflow defaults reproduce today's text."""

    product: str = "Airflow"
    pipeline: str = "DAG"
    pipelines: str = "DAGs"
    run: str = "DAG run"
    step: str = "task"
    steps: str = "tasks"
    attempt: str = "try"
    map_index: str = "map index"
    retry_action: str = "Retry task"

    def as_dict(self) -> dict[str, str]: ...


def render_vocabulary(text: str, vocabulary: Vocabulary) -> str:
    """Replace ``{{product}}``-style tokens (plain str.replace; JSON braces in prompts are safe)."""


class RemediationKind(str, Enum):
    RETRY_STEP = "retry_step"          # Airflow: clear exactly this task instance
    RESUME_RUN = "resume_run"          # SFN: RedriveExecution (all failed branches/iterations)
    RERUN_PIPELINE = "rerun_pipeline"  # new run with the same input/conf (opt-in)


@dataclass(frozen=True)
class Capabilities:
    sources: tuple[str, ...]                                   # incident sources the adapter can produce
    push_channels: tuple[str, ...] = ()                        # ingest channels its PushIntake accepts
    attempts_visible: bool = True                              # every automatic retry is an observable failure
    remediations: tuple[RemediationKind, ...] = ()
    fix_delivery: Literal["repo_sync", "ci_deploy"] = "repo_sync"
    ui_hosts: tuple[Literal["airflow_plugin", "standalone"], ...] = ("standalone",)
    native_authz: bool = False                                 # the host can answer per-pipeline permissions


ErrorKind = Literal["data_shape", "code", "infra", "definition", "aggregate"]


@dataclass(frozen=True)
class ErrorTaxonomy:
    """Native error names and event types, classified for the deterministic policy."""

    infra: frozenset[str] = frozenset()
    data_shape: frozenset[str] = frozenset()
    code: frozenset[str] = frozenset()
    definition: frozenset[str] = frozenset()          # bug in the pipeline definition (SFN States.Runtime)
    aggregate: frozenset[str] = frozenset()           # summary of child failures (ExceedToleratedFailureThreshold)
    infra_event_types: frozenset[str] = frozenset()   # SFN TaskStartFailed, LambdaFunctionStartFailed ...

    def kind_of(self, error_name: str | None, *, event_type: str | None = None) -> ErrorKind | None:
        """Classify a native error; ``None`` lets the generic policy decide."""
```

### 3.6 `core/errors.py` and `core/tracebacks.py`

```python
class OrchestratorError(RuntimeError):
    """An orchestrator call failed. ``transient`` -> the worker retries later (no state change)."""

    def __init__(self, detail: str = "", *, transient: bool = False, status_code: int | None = None) -> None: ...

class NotSupported(OrchestratorError): ...      # capability missing (e.g. redrive on an Express execution)
class NotFound(OrchestratorError): ...
class PermissionDenied(OrchestratorError): ...
```

`core/tracebacks.py` holds `TRACEBACK_HEADER`, `CAUSE_SEPARATOR` and `CONTEXT_SEPARATOR` (moved from `airflow/client.py`, which re-exports them), plus:

```python
def render_python_traceback(frames: Sequence[tuple[str, int, str, str | None]],
                            exc_type: str, message: str | None) -> str:
    """Render ``(file, line, function, code)`` frames as CPython traceback text that
    ``agent.context.extract_traceback`` parses (used by the SFN Lambda cause renderer)."""
```

Adapters translate `httpx` errors, `AirflowAPIError` and `botocore.exceptions.ClientError` into these error classes. The conformance suite checks this.

### 3.7 `core/ports.py`

```python
@runtime_checkable
class FailureFeed(Protocol):
    def poll(self, *, cursor: Mapping[str, Any] | None, is_monitored: Callable[[str], bool],
             limit: int = 200) -> FailurePage:
        """Terminal or retrying failures observed after ``cursor`` (overlap allowed: the store key
        dedupes). ``is_final`` must be set on every item; refs must be resolved."""


@runtime_checkable
class PushIntake(Protocol):
    channels: frozenset[str]

    def normalize(self, channel: str, payload: Mapping[str, Any],
                  headers: Mapping[str, str]) -> list[FailureHint]:
        """PURE (no I/O; runs in the API process). Keep only whitelisted fields (never execution
        input/output). Raise ValueError for a malformed payload (-> HTTP 422)."""

    def confirm(self, hint: FailureHint, *, trigger: Trigger) -> Confirmation:
        """I/O: verify the hint against the API, decide final vs retrying, localise the step.
        Transient errors -> ``retry_later``; unknown/forged/stale -> ``discarded``."""


@dataclass(frozen=True)
class QueuedMessage:
    receipt: str                       # opaque ack handle (SQS receipt handle)
    channel: str                       # PushIntake channel used to normalise ``payload``
    payload: Mapping[str, Any]


@runtime_checkable
class HintQueue(Protocol):
    """A durable queue the worker drains (EventBridge -> SQS). At-least-once delivery."""

    def receive(self, *, max_items: int = 10) -> list[QueuedMessage]: ...
    def ack(self, receipts: Sequence[str]) -> None:
        """Delete messages once their pending events are committed."""


@runtime_checkable
class RunInspector(Protocol):
    def describe(self, ref: AttemptRef, native: Mapping[str, Any]) -> AttemptDetail: ...
    def logs(self, ref: AttemptRef, detail: AttemptDetail | None, *, max_chars: int = 2_000_000) -> LogBundle:
        """Never raises for missing logs: returns ``LogBundle(text="", source="none", confidence="none", notes=...)``."""
    def pipeline_source(self, pipeline_id: str, *, ref: AttemptRef | None = None,
                        native: Mapping[str, Any] | None = None,
                        include_text: bool = False) -> PipelineSource | None: ...


@runtime_checkable
class CodeLocator(Protocol):
    """Static mapping from pipelines and steps to repo code. No network I/O, never executes user code."""

    def scan(self, repo_root: Path) -> list[PipelineSpec]:
        """Every pipeline in the repository with its steps and dataset references (used by init)."""
    def path_mappings(self, repo_root: Path, step: StepSpec | None) -> list[PathMapping]:
        """Runtime-prefix -> repo-base pairs for this step (the core adds ``(repo_root, "")``)."""
    def pipeline_file(self, repo_root: Path, pipeline_id: str, source: PipelineSource | None) -> str | None:
        """Repo-relative definition file of a pipeline, using the orchestrator's report when given."""


@dataclass(frozen=True)
class RemediationResult:
    kind: RemediationKind
    accepted: bool
    detail: str                        # human text, masked by the core before it is stored as an event
    new_ref: AttemptRef | None = None  # e.g. same run, attempt + 1 after a redrive
    warnings: tuple[str, ...] = ()     # e.g. "redrive reruns the deployed code with the original definition"
    native: Mapping[str, Any] = field(default_factory=dict)


@runtime_checkable
class Remediator(Protocol):
    def available(self, ref: AttemptRef, native: Mapping[str, Any]) -> tuple[RemediationKind, ...]:
        """No I/O: what can be offered for this incident now (e.g. SFN redrive window of 14 days)."""
    def remediate(self, ref: AttemptRef, native: Mapping[str, Any], kind: RemediationKind, *,
                  idempotency_key: str, actor: str) -> RemediationResult:
        """Raise NotSupported when ``kind`` is not in ``available``; idempotent for the same key."""


@runtime_checkable
class NativeLinks(Protocol):
    def links(self, ref: AttemptRef, native: Mapping[str, Any]) -> dict[str, str]:
        """Labelled absolute links into the orchestrator's own UI, e.g. {"AWS console": url}."""


@dataclass(frozen=True)
class HealthReport:
    ok: bool
    detail: str
    checks: Mapping[str, str] = field(default_factory=dict)


@runtime_checkable
class OrchestratorAdapter(Protocol):
    """Everything the core needs from one orchestrator instance.

    One class may implement several ports and point several attributes at ``self``.
    Construction does no network I/O (the API process builds one per config).
    """

    type: str                          # "airflow" | "stepfunctions" | "fake" | <entry point name>
    instance: str                      # stored in incidents.orchestrator
    display_name: str                  # "Apache Airflow" | "AWS Step Functions"
    vocabulary: Vocabulary
    capabilities: Capabilities
    taxonomy: ErrorTaxonomy
    feed: FailureFeed | None
    push: PushIntake | None
    queue: HintQueue | None
    inspector: RunInspector
    code: CodeLocator
    remediator: Remediator | None
    links: NativeLinks

    def health(self) -> HealthReport: ...                       # never raises
    def doctor_checks(self) -> list[DoctorCheck]: ...           # DoctorCheck(name, run: Callable[[], CheckResult])
    def sandbox_env(self) -> Mapping[str, str | None]: ...      # pytest sandbox overrides (None removes)
    def close(self) -> None: ...
```

### 3.8 Adapter factory and registry (`orchestrators/registry.py`)

```python
ENTRY_POINT_GROUP = "dag_healer.orchestrators"


@runtime_checkable
class AdapterFactory(Protocol):
    type: str
    display_name: str
    options_model: type[pydantic.BaseModel] | None   # validates healer.yaml orchestrator.options
    secret_options: tuple[str, ...]                   # dotted names under orchestrator.options (doctor)

    def create(self, cfg: HealerConfig, *, client: Any | None = None) -> OrchestratorAdapter:
        """MUST NOT perform I/O. ``client`` lets tests and legacy callers inject a transport."""


def available_types() -> dict[str, str]:
    """``{type: "module:attr"}`` from the entry-point group. The same name registered by two
    distributions is an error naming both (no silent shadowing of built-ins)."""

def load_factory(type_: str) -> AdapterFactory:
    """Entry-point name or an explicit "pkg.mod:factory" string; a missing extra gives a clear
    error ("pip install 'dag-healer[aws]'")."""

def get_adapter(cfg: HealerConfig, *, client: Any | None = None) -> OrchestratorAdapter:
    """Cached per (cfg source path, mtime, instance)."""

def reset_adapters() -> None: ...                    # tests
```

`orchestrators/compat.py`:

```python
def as_adapter(cfg: HealerConfig, obj: Any | None) -> OrchestratorAdapter | None:
    """OrchestratorAdapter -> itself; a legacy Airflow client (duck type with get_task_log /
    get_dag / get_dag_source, e.g. the phase-5 FakeAirflowClient) -> AirflowAdapter(cfg, client=obj);
    None -> None (offline gathering, today's behaviour)."""
```

### 3.9 How the core calls the ports

**Worker** (`worker/loop.py`, `worker/pipeline.py`, `worker/commands.py`):

```python
def run_worker(cfg, *, once=False, provider=None, client=None, adapter=None, sleep=time.sleep) -> int:
    adapter = adapter or as_adapter(cfg, client) or get_adapter(cfg)
    init_db(engine, allow_migrate=True)              # section 6.4
    _refuse_foreign_kind(cfg, adapter)               # instance id already used by a different type -> error
    ...  # loop: apply_commands -> reconcile -> claim/process (phase-5 behaviour unchanged)

def reconcile(cfg, session_factory, adapter, *, now) -> ReconcileReport:
    inst = adapter.instance
    # 1) pull (Airflow "rest", neutral "poll")
    if adapter.feed and cfg.monitor.wants("poll"):
        cursor = kv_get(s, f"cursor:{inst}")
        page = adapter.feed.poll(cursor=cursor, is_monitored=cfg.is_monitored)
        for f in page.failures:
            if f.is_final or cfg.monitor.trigger == "every_failure":
                _create_incident(cfg, s, f)          # masks error_summary, signature, native_ref, error_name
        kv_set(s, f"cursor:{inst}", dict(page.cursor))
    # 2) durable queue (SFN EventBridge -> SQS)
    if adapter.queue and cfg.monitor.wants("queue"):
        msgs = adapter.queue.receive()
        for m in msgs:
            for h in adapter.push.normalize(m.channel, m.payload, {}):
                add_pending_event(s, **h.ref.store_key(), channel=m.channel,
                                  payload={"error": mask_text(h.error_text), "source": h.source},
                                  native_ref=dict(h.native))
        s.commit(); adapter.queue.ack([m.receipt for m in msgs])
    # 3) confirm every pending hint of THIS instance (listener, callback, eventbridge, sqs)
    if adapter.push:
        for row in list_pending_events(s, PENDING_CHECK, orchestrator=inst):
            c = adapter.push.confirm(_hint_from_row(row), trigger=cfg.monitor.trigger)
            if c.verdict == "confirmed":
                for f in c.failures: _create_incident(cfg, s, f)   # key from the CONFIRMED ref
                mark_pending_event(s, row, PENDING_CONFIRMED)
            elif c.verdict == "discarded":
                mark_pending_event(s, row, PENDING_DISCARDED)
            # retry_later: kept; discarded with a reason after 1 h (received_at)
```

- **`retry_task` command:**
  1. `kind = adapter.remediator.available(ref, native)[0]` (Airflow: `RETRY_STEP`; SFN: `RESUME_RUN`, or `RERUN_PIPELINE` when configured).
  2. `res = adapter.remediator.remediate(ref, native, kind, idempotency_key=f"healer-{command.id}-{incident.id[:16]}", actor=command.actor)`.
  3. The worker records an event with `mask_text(res.detail)` and the warnings.
  4. `NotSupported` marks the command `failed` with a clear message.
- **The phase-5 confirmation rules for Airflow** (`failed` vs `up_for_retry`, a later try exists, `trigger`) move verbatim into `AirflowAdapter.confirm`.

**Context gathering** (`agent/context.py`):

```python
def gather_context(cfg, incident, client, *, error_text=None) -> IncidentContext:
    adapter = as_adapter(cfg, client)                 # parameter name and position unchanged
    ref = AttemptRef.from_row(incident, default_orchestrator=adapter.instance if adapter else "airflow")
    native = incident.get("native_ref") or {}
    detail = _safely(ctx, "Describing the failed attempt", lambda: adapter.inspector.describe(ref, native))
    logs = _safely(ctx, "Fetching the step log", lambda: adapter.inspector.logs(ref, detail))
    # error info: extract_traceback(detail.traceback_text) first, then extract_traceback(logs.text),
    #             then error_text / incident.error_summary (unchanged fallback chain)
    # pipeline file: adapter.code.pipeline_file(repo_root, dag_id, adapter.inspector.pipeline_source(dag_id))
    # step spec: cached adapter.code.scan(repo_root) -> .task(step_id)   (TaskInfo, unchanged type)
    # path roots: [(repo_root, "")] + adapter.code.path_mappings(repo_root, ctx.task)
    #             -> map_to_repo_path(path, cfg, mappings=...)  (None -> Airflow defaults, old tests pass)
    # contracts, datasets, masked sample: unchanged (keyed by pipeline_id == dag_id; s3:// via datasets)
```

New `IncidentContext` fields, all defaulted, so `policy`, `classify`, `repair` and `tests_gen` need no change:
- `orchestrator: str = "airflow"`
- `error_name: str | None = None`
- `native: dict = {}` (masked keys only)
- `log_source: str | None = None`
- `log_confidence: str = "exact"`
- `vocabulary: dict[str, str] = {}`
- `native_links: dict[str, str] = {}`

`to_prompt_dict()` adds `orchestrator`, `vocabulary`, `error_name` and `log_confidence`. The legacy keys stay.

**Policy** (`agent/policy.py`):
- `compute_signals(ctx, *, taxonomy: ErrorTaxonomy | None = None)`. `Signals` gains `native_error_name` and `native_error_kind`.
- `taxonomy.kind_of(ctx.error_name, event_type=ctx.native.get("event_type"))` runs first:
  - `infra` → today's infra rule (AMBIGUOUS);
  - `definition` → new rule `"definition"` (AMBIGUOUS: an ASL bug cannot be reproduced by a local pytest);
  - `aggregate` → classify the sampled child failure when present, else AMBIGUOUS;
  - `code` / `data_shape` → feed `error_kind`.
- `INFRA_TYPES` stays exactly as it is, Airflow names included. Airflow decisions therefore cannot change, and Airflow exception names never occur in Lambdas.
- `classify_incident(cfg, ctx, provider)` keeps its signature and reads the taxonomy from `get_adapter(cfg).taxonomy` (cached, no I/O).

---

## 4. Package layout (before → after)

```
src/dag_healer/
  core/                        NEW  ids.py, failures.py, evidence.py, specs.py, vocab.py, errors.py,
                                    tracebacks.py, ports.py   (stdlib only; __init__ re-exports, light)
  orchestrators/               NEW  __init__.py (light), registry.py, compat.py,
                                    fake.py        FakeAdapter + FakeWorld + StaticCodeLocator
                                    conformance.py packaged pytest suite (imports pytest lazily)
  airflow/                     KEPT: the Airflow adapter package (task runners still import only
                                     __init__/listener/callback, which stay light)
    __init__.py, plugin.py, listener.py, callback.py   UNCHANGED
    client.py                  unchanged API; the 3 traceback constants now come from core.tracebacks (re-exported)
    adapter.py                 NEW  FACTORY, AirflowAdapter, AirflowCodeLocator (section 8)
    errors.py                  NEW  AIRFLOW_TAXONOMY
  stepfunctions/               NEW  reference adapter, optional extra "aws" (section 9)
  hosting/                     NEW  standalone host
    app.py                     create_standalone_app(cfg)
    shell.py                   index.html shell (SPA fallback)
    rbac.py                    RuleAuthorizer
    auth/{token,trusted_header,alb_oidc,dev}.py   AuthBackend implementations
  storage/                     NEW  s3.py (S3 dataset reader, lazy boto3)
  store/migrations.py          NEW  version-gated migration runner (section 6)
  cli/serve_cmd.py, cli/store_cmd.py, cli/orchestrators_cmd.py   NEW (store_cmd extends the demo's
                                    "store reset" if phase 2-5 already created the module)
  repo/dagscan.py              TaskInfo/DatasetRef/DagInfo imported from core.specs and re-exported
  api/auth.py                  + AuthBackend, AirflowAuthBackend, apply_auth_backend, Principal-aware user_name
  api/app.py                   create_app(*, auth=None); get_app() unchanged
  api/static/standalone/       NEW  second UI bundle (built)
  agent/, worker/, alerts/, pr/, contracts/, links.py, config.py, datasets.py, masking.py, ingest.py
                               edited in place (sections 3.9, 5, 6, 7)
ui/src/standalone.tsx          NEW  standalone entry (based on today's ui/src/dev.tsx)
ui/vite.standalone.config.ts   NEW  ES-module app build with React bundled
```

**Compatibility.** The only things that move are the bodies of `TaskInfo`/`DatasetRef`/`DagInfo` (into `core.specs`) and the traceback constants (into `core.tracebacks`). Both old modules re-export the *same objects*. So:

| Kept working | How |
|---|---|
| `airflow.plugins` entry point `dag_healer.airflow.plugin:HealerPlugin` | file untouched |
| listener registration (`listeners = [healer_listener]`) | module untouched; same module object |
| `from dag_healer import callback` / `dag_healer.callback.on_failure_callback` | untouched |
| `from dag_healer.repo.dagscan import TaskInfo, DagInfo, DatasetRef, scan_dags` | re-export |
| `from dag_healer.airflow.client import CAUSE_SEPARATOR, CONTEXT_SEPARATOR, TRACEBACK_HEADER` | re-export |
| `gather_context(cfg, incident, client)`, `run_worker(..., client=)` | `compat.as_adapter` |
| `api.auth.get_current_user` / `get_authorizer` / `require_dag_access` | unchanged names and semantics |

**Import rules,** enforced by an AST architecture test (11.6):
- `core` imports only stdlib and `core`.
- The core packages listed in goal 1 never import `airflow`, `boto3`, `botocore`, `dag_healer.airflow.adapter`, `dag_healer.airflow.client` or `dag_healer.stepfunctions*`. They may import `core`, `orchestrators.registry` and `orchestrators.compat`.
- Only `airflow/{plugin,listener}.py` and `api/auth.py` import `airflow` (today's rule).
- Only `stepfunctions/aws.py` and `storage/s3.py` import `boto3`, and only lazily.
- Task runners never import `orchestrators` or `core`: an extension of the existing light-import test.

---

## 5. Configuration

### 5.1 `healer.yaml` additions (`version: 1` stays; every change is additive)

```yaml
orchestrator:                 # absent -> {type: airflow, instance: airflow}
  type: airflow               # entry-point name in group dag_healer.orchestrators, or "pkg.mod:factory"
  instance: null              # default = type; stored on every incident; unique per store and type
  options: {}                 # validated by the adapter's options_model. Airflow keeps using the
                              # top-level "airflow:" section, so options must stay empty for it.
serve:                        # used only by "dag-healer serve" (ignored inside Airflow)
  host: 127.0.0.1
  port: 8090
  public_url: null            # also HEALER_PUBLIC_URL
  auth:
    backend: dev              # dev | token | trusted_header | alb_oidc
    rules: []                 # RBAC rules, see 7.3
```

`config.py`:

```python
class OrchestratorConfig(_Model):
    type: str = "airflow"
    instance: str | None = Field(default=None, pattern=r"^[a-z0-9][a-z0-9._-]{0,63}$")
    options: dict[str, Any] = Field(default_factory=dict)

    @property
    def instance_id(self) -> str:
        return self.instance or self.type

class RbacRule(_Model):
    principals: list[str]                                   # "user:alice" | "group:data-eng" | "authenticated" | "*"
    pipelines: list[str] = Field(default_factory=lambda: ["*"])   # fnmatch; "!" prefix excludes
    actions: list[Literal["read", "act", "approve_contract"]] = Field(default_factory=lambda: ["read"])

class ServeAuthConfig(_Model):
    backend: Literal["dev", "token", "trusted_header", "alb_oidc"] = "dev"
    tokens: list[dict[str, Any]] = Field(default_factory=list)   # [{name, sha256, groups}] (hashes only)
    user_header: str = "X-Forwarded-User"
    groups_header: str | None = "X-Forwarded-Groups"
    trusted_proxies: list[str] = Field(default_factory=list)     # CIDRs (required for trusted_header)
    alb_arn: str | None = None
    region: str | None = None
    user_claim: str = "email"
    groups_claim: str | None = None
    rules: list[RbacRule] = Field(default_factory=list)

class ServeConfig(_Model):
    host: str = "127.0.0.1"
    port: int = Field(default=8090, ge=1, le=65535)
    public_url: str | None = None
    auth: ServeAuthConfig = Field(default_factory=ServeAuthConfig)

class HealerConfig(_Model):
    ...
    orchestrator: OrchestratorConfig = Field(default_factory=OrchestratorConfig)
    serve: ServeConfig = Field(default_factory=ServeConfig)
```

`MonitorConfig`:
- `dags` stays canonical. It also accepts `pipelines` (`validation_alias=AliasChoices("dags", "pipelines")`).
- `modes` widens to `list[Literal["rest", "listener", "callback", "poll", "push", "queue"]]`. Values are kept verbatim, so `dag-healer check` output is unchanged.
- `wants(kind)` normalises them: `rest`/`poll` → poll; `listener`/`callback`/`push` → confirm pushed hints; `queue` → drain `HintQueue`.
- The worker start and `doctor` reject a mode the adapter cannot serve. They also reject `trigger: every_failure` when `capabilities.attempts_visible` is false (Step Functions).

Other config changes:
- **Secrets.** `SECRET_FIELDS` is extended at load time with `orchestrator.options.<f>` for each `factory.secret_options`, using the existing `_env_refs` dotted-path tracking. `serve.auth.tokens` accepts only sha256 hashes.
- **`default_config_yaml(..., orchestrator: str = "airflow")`.** The Airflow template renders exactly as today plus a commented `orchestrator:` block (snapshot-tested). `orchestrator="stepfunctions"` renders the template in 9.14.
- **Datasets.** `DatasetLocator.path` accepts `s3://bucket/key`. `datasets.load_dataset` dispatches on the scheme (`s3` → `storage/s3.py`) before today's local path logic.

### 5.2 Entry points and extras (`pyproject.toml`)

```toml
[project.entry-points."dag_healer.orchestrators"]
airflow = "dag_healer.airflow.adapter:FACTORY"
stepfunctions = "dag_healer.stepfunctions:FACTORY"
fake = "dag_healer.orchestrators.fake:FACTORY"
# [project.entry-points."airflow.plugins"] stays exactly as today

[project.optional-dependencies]
airflow = ["apache-airflow>=3.1"]
aws = ["boto3>=1.34"]
serve = ["uvicorn>=0.29", "PyJWT[crypto]>=2.8"]
dev = ["pytest>=8", "aiosmtpd", "ruff", "boto3>=1.34"]   # botocore.stub for the SFN tests
```

Entry points become visible only after `pip install`, so an editable install must be reinstalled after adding one. `load_factory` also accepts `"pkg.mod:factory"` strings, which serves development and tests.

---

## 6. Data model changes and migration

### 6.1 Decision

- **Keep** the physical columns (`dag_id`, `run_id`, `task_id`, `map_index`, `try_number`). Add neutral `synonym`s (`pipeline_id`, `step_id`, `attempt`). There is no data rewrite.
- **Add** an `orchestrator` column (the instance id) **to the incident unique key** now, in schema v2. Mixed estates, such as an Airflow-to-Step-Functions migration running side by side, then share one store and dedupe correctly. Today only dev and demo DBs exist, which makes now the cheapest moment for the one non-additive DDL (a table rebuild on SQLite).
- Each process serves one instance. The API lists, filters and enqueues only for `cfg.orchestrator.instance_id`, and each worker applies only its own instance's commands and pending events. A combined multi-instance view is future work.

### 6.2 Schema v2 (`store/models.py`, `SCHEMA_VERSION = 2`)

| Table | Change |
|---|---|
| `healer_incidents` | Adds `orchestrator VARCHAR(64) NOT NULL DEFAULT 'airflow'`, `native_ref JSON NULL` and `error_name VARCHAR(256) NULL`.<br>Unique key `uq_healer_incidents_ti` (5 columns) is replaced by `uq_healer_incidents_key (orchestrator, dag_id, run_id, task_id, map_index, try_number)`.<br>Indexes become `ix_healer_incidents_orch_dag_created (orchestrator, dag_id, created_at)` and `ix_healer_incidents_signature (orchestrator, dag_id, task_id, signature)`. |
| `healer_pending_events` | Adds `orchestrator VARCHAR(64) NOT NULL DEFAULT 'airflow'`, `channel VARCHAR(32) NOT NULL DEFAULT 'task-failed'` and `native_ref JSON NULL`. `task_id` stays NOT NULL; unresolved hints use `UNRESOLVED_STEP = ""`. |
| `healer_commands` | Adds `orchestrator VARCHAR(64) NOT NULL DEFAULT 'airflow'`. |
| `healer_contracts` (cache) | Adds `orchestrator`. PK `dag_id` becomes `(orchestrator, dag_id)`. Rows are copied with `'airflow'`. |
| `healer_kv` (new) | `key VARCHAR(250) PK, value JSON, updated_at`. Holds `cursor:<instance>`. The first worker run imports the phase-5 `<healer_home>/state.json` watermark if present. |
| `healer_incident_events`, `healer_alerts`, `healer_schema_version` | unchanged |

Model code:
- `SOURCES = ("listener", "callback", "rest", "cli", "poll", "push", "eventbridge", "sqs")`. All fit in `String(16)`. Adapters declare `capabilities.sources ⊆ SOURCES`, and the conformance suite checks it.
- `Incident.pipeline_id = synonym("dag_id")`, `step_id = synonym("task_id")`, `attempt = synonym("try_number")`. `to_dict()` gains the three new columns automatically; legacy keys are unchanged.
- `native_ref` is not a `HEAVY_FIELDS` member: it is small, ≤ 4 KB by contract.

`store/repo.py`, backward compatible:
- `_TI_KEY_COLUMNS = ("orchestrator", "dag_id", "run_id", "task_id", "map_index", "try_number")`. `_insert_incident` keeps `on_conflict_do_nothing(index_elements=list(_TI_KEY_COLUMNS))`, which must match the new unique key exactly.
- `create_incident(..., orchestrator="airflow", native_ref=None, error_name=None, **extra)`. The defaults keep every phase-5 call valid.
- `get_incident_by_key(..., orchestrator="airflow")`. `_find_open_duplicate` adds the orchestrator filter.
- `list_incidents(..., orchestrator: str | None = None)`. `pending_commands(..., orchestrator=None)`. `list_pending_events(status, *, orchestrator=None)`. `add_pending_event(..., orchestrator="airflow", channel="task-failed", native_ref=None)`. `enqueue_command(..., orchestrator="airflow")`.
- `create_incident_from_failure(session, f: Failure, *, signature, now=None) -> tuple[Incident, bool]` masks `f.error_summary` and maps `f.native` to `native_ref`.
- `kv_get(session, key) -> Any | None` and `kv_set(session, key, value) -> None`.

### 6.3 Migration runner (`store/migrations.py`)

```python
@dataclass(frozen=True)
class Migration:
    version: int
    description: str
    upgrade: Callable[[Connection, str], None]       # (conn, dialect name); every step re-inspects

MIGRATIONS: tuple[Migration, ...] = (Migration(2, "orchestrator-aware keys", _upgrade_v2),)

def current_version(engine: Engine) -> int:
    """0 = empty DB; 1 = healer_incidents exists and no version row >= 2; else max(version)."""
def pending(engine: Engine) -> list[Migration]: ...
def migrate(engine: Engine, *, target: int = SCHEMA_VERSION, dry_run: bool = False) -> list[int]:
    """Apply pending migrations in order under a lock; idempotent; returns applied versions."""
```

Locking:
- **PostgreSQL:** one transaction with `SELECT pg_advisory_xact_lock(hashtext('dag_healer_schema'))`.
- **SQLite:** an `AUTOCOMMIT` connection, `PRAGMA foreign_keys=OFF` *outside* the transaction, then `BEGIN IMMEDIATE`.
- Each migration re-reads `current_version` after taking the lock. Each DDL step is guarded by `sqlalchemy.inspect()` (does the column, index or constraint exist?), so a half-applied run can be repeated.

**`_upgrade_v2`, PostgreSQL:**

```sql
ALTER TABLE healer_incidents ADD COLUMN IF NOT EXISTS orchestrator VARCHAR(64) NOT NULL DEFAULT 'airflow';
ALTER TABLE healer_incidents ADD COLUMN IF NOT EXISTS native_ref JSON;
ALTER TABLE healer_incidents ADD COLUMN IF NOT EXISTS error_name VARCHAR(256);
ALTER TABLE healer_incidents DROP CONSTRAINT IF EXISTS uq_healer_incidents_ti;
ALTER TABLE healer_incidents ADD CONSTRAINT uq_healer_incidents_key
  UNIQUE (orchestrator, dag_id, run_id, task_id, map_index, try_number);
DROP INDEX IF EXISTS ix_healer_incidents_dag_created;
DROP INDEX IF EXISTS ix_healer_incidents_signature;
CREATE INDEX IF NOT EXISTS ix_healer_incidents_orch_dag_created ON healer_incidents (orchestrator, dag_id, created_at);
CREATE INDEX IF NOT EXISTS ix_healer_incidents_signature ON healer_incidents (orchestrator, dag_id, task_id, signature);
ALTER TABLE healer_pending_events ADD COLUMN IF NOT EXISTS orchestrator VARCHAR(64) NOT NULL DEFAULT 'airflow';
ALTER TABLE healer_pending_events ADD COLUMN IF NOT EXISTS channel VARCHAR(32) NOT NULL DEFAULT 'task-failed';
ALTER TABLE healer_pending_events ADD COLUMN IF NOT EXISTS native_ref JSON;
ALTER TABLE healer_commands ADD COLUMN IF NOT EXISTS orchestrator VARCHAR(64) NOT NULL DEFAULT 'airflow';
ALTER TABLE healer_contracts ADD COLUMN IF NOT EXISTS orchestrator VARCHAR(64) NOT NULL DEFAULT 'airflow';
ALTER TABLE healer_contracts DROP CONSTRAINT IF EXISTS healer_contracts_pkey;
ALTER TABLE healer_contracts ADD PRIMARY KEY (orchestrator, dag_id);
-- healer_kv is created by create_all (new table)
```

ADD COLUMN with a constant default is metadata-only on PostgreSQL ≥ 11. The constraint swap takes a short `ACCESS EXCLUSIVE` lock on small tables.

**`_upgrade_v2`, SQLite.** Before the rebuild, the file is copied to `<db>.bak-v1`.

1. Additive `ALTER TABLE ... ADD COLUMN` for `healer_pending_events` and `healer_commands`. SQLite accepts NOT NULL there only with a non-null default, which `'airflow'` / `'task-failed'` provide.
2. Rebuild `healer_incidents` and `healer_contracts`, because SQLite cannot drop an inline `UNIQUE` or a PK. Follow SQLite's documented procedure exactly:
   1. `CREATE TABLE <t>__v2` using DDL compiled from the v2 `Table` (`Table.to_metadata(MetaData(), name="<t>__v2")`).
   2. `INSERT INTO <t>__v2 (<all v1 columns>, orchestrator) SELECT <all v1 columns>, 'airflow' FROM <t>`.
   3. `DROP TABLE <t>`, then `ALTER TABLE <t>__v2 RENAME TO <t>`. Never rename the *old* table first: SQLite ≥ 3.26 would rewrite the child foreign keys to follow it.
   4. Recreate the indexes.
   5. `PRAGMA foreign_key_check`: abort and roll back if any row is reported.
   6. `COMMIT`, then `PRAGMA foreign_keys=ON`.

   With foreign keys off, `DROP TABLE` does not cascade, and the children (`healer_incident_events`, `healer_alerts`, `healer_commands`) reference the table by name, so they resolve to the rebuilt table.
3. Insert version row 2.

### 6.4 `init_db` and who migrates

`init_db(engine, *, allow_migrate: bool = False)`:
- **Empty DB:** `create_all` builds v2 directly and stamps version rows 1 and 2. Any process may do this.
- **Existing DB below `SCHEMA_VERSION`:**
  - with `allow_migrate=True`, it runs `migrate()`. This is the case for `dag-healer worker`, `dag-healer serve`, `dag-healer store migrate` and `dag-healer init`;
  - otherwise (the Airflow api-server plugin) it raises `StoreSchemaOutdated`. Store-backed routes then answer 503 `"dag-healer store schema v1: start the worker or run 'dag-healer store migrate'"`, and web processes never run the rebuild.
- **Bug to fix in the same change:** today's `init_db` would insert a v2 version row without migrating once `SCHEMA_VERSION` is bumped.
- CLI: `dag-healer store status` (current and pending versions) and `dag-healer store migrate [--dry-run]` (prints the SQL).

**Rollout:**
1. Stop the worker.
2. Deploy the new wheel to the worker and the api-server.
3. The worker (or `store migrate`) migrates.

An old API process keeps working, because new columns have defaults. An old worker breaks: its ON CONFLICT target no longer matches a unique index, so the worker and the migration ship together. Document this in `docs/upgrade.md`, including taking a backup first.

### 6.5 Contracts

Contracts are keyed by pipeline id, so `healer/contracts/<pipeline_id>.yaml` keeps working.
- `Contract.dag_id` accepts the input alias `pipeline_id`, and `DatasetContract.task_id` accepts `step_id`.
- `contract_to_dict` keeps writing `dag_id`/`task_id`, so `compute_source_hash` is byte-stable and approved contracts do not go stale.
- Sharing one contract between an Airflow DAG and its migrated state machine (`aliases:`) is future work.

---

## 7. API and UI hosting

### 7.1 One FastAPI app, two hosts

```python
# api/app.py
def create_app(*, auth: AuthBackend | None = None) -> FastAPI:
    """Today's app. With ``auth`` (standalone host) installs the backend through the two existing
    dependency overrides; with None the app is byte-identical to today's (Airflow host)."""

def get_app() -> FastAPI: ...      # unchanged; used by HealerPlugin.fastapi_apps
```

| | Airflow plugin host (unchanged) | Standalone host (`dag-healer serve`) |
|---|---|---|
| Process | Airflow api-server mounts `get_app()` at `/healer` | uvicorn runs `create_standalone_app(cfg)`, which mounts `create_app(auth=backend)` at `/healer` |
| Users | `airflow.api_fastapi.core_api.security.get_user` (Bearer, then `_token` cookie) | `AuthBackend.user_dependency()` |
| Permissions | `AirflowAuthorizer` (`requires_access_dag`, `is_authorized_dag`, `get_authorized_dag_ids`) | `RuleAuthorizer` (healer.yaml RBAC) |
| UI | `react_apps` in Airflow (UMD bundle, host React) | SPA shell plus the standalone bundle (React bundled) |
| URL shapes | `/plugin/healer`, `/dags/<d>/plugin/healer-dag`, `/dags/<d>/runs/<r>/tasks/<t>[/mapped/<i>]/plugin/healer-ti` | **the same shapes**, served by the shell |
| CSRF origins | `AIRFLOW__API__BASE_URL`, `HEALER_PUBLIC_URL` | `serve.public_url`, `HEALER_PUBLIC_URL` |

### 7.2 `AuthBackend` (`api/auth.py`, next to the existing `Authorizer`)

```python
@runtime_checkable
class AuthBackend(Protocol):
    """A user dependency plus the Authorizer that answers pipeline (DAG) permission questions."""

    name: str

    def user_dependency(self) -> Callable[..., Any]:
        """FastAPI dependency (may declare sub-dependencies) returning the user; raises 401/403."""
    def authorizer(self) -> Authorizer:
        """The existing Protocol: check_dag_access, authorized_dag_ids, can_read_dag, can_edit_dag."""
    def allowed_origins(self) -> list[str]:
        """Extra origins for RequestGuardMiddleware's Origin/Referer check."""


class AirflowAuthBackend:
    """Today's behaviour packaged (default when create_app(auth=None))."""
    name = "airflow"
    def user_dependency(self): return _airflow_get_user
    def authorizer(self): return _AUTHORIZER
    def allowed_origins(self): return configured_base_urls()


def apply_auth_backend(app: FastAPI, backend: AuthBackend) -> None:
    """app.dependency_overrides[get_current_user] = backend.user_dependency()
       app.dependency_overrides[get_authorizer]   = backend.authorizer
    (the same two keys the host tests override; require_dag_access depends on both)."""


@dataclass(frozen=True)
class Principal:
    """Standalone user. Duck-compatible with Airflow's BaseUser for user_name()."""
    id: str
    name: str
    groups: frozenset[str] = frozenset()
    claims: Mapping[str, Any] = field(default_factory=dict)
    def get_id(self) -> str: return self.id
    def get_name(self) -> str: return self.name
```

How the wiring behaves:
- Overrides live on the healer sub-app object. The router's `dependency_overrides_provider` is the app that created the routes, so they apply inside a mounted sub-app [I, already relied on by the host tests].
- Using overrides as production wiring is a deliberate choice. It is the only mechanism that substitutes a dependency *together with its own sub-dependencies*, which Airflow's `GetUserDep` needs.
- Routes stay `/dags/{dag_id}/...`, with no change: `RuleAuthorizer.check_dag_access` reads `request.path_params["dag_id"]` just as Airflow does.

### 7.3 Standalone authentication and authorization (`hosting/`)

**`RuleAuthorizer(rules, *, candidates: Callable[[], set[str]])`** implements the existing `Authorizer`:
- `(method, entity)` maps to a neutral action: `GET` → `read`; `PUT TASK_INSTANCE` → `act` (resolve, retry repair, retry task); `PUT CODE` → `approve_contract`.
- Rules match principals (`user:x`, `group:y`, `authenticated`, `*`) and pipelines (fnmatch, `!` excludes). Any allow wins; with no match it fails closed (403 `Forbidden`).
- `authorized_dag_ids` filters candidates: incidents of this instance, contracts and configured pipelines.

Backends (`hosting/auth/`), each returning `Principal`:

| Backend | Use | Rules |
|---|---|---|
| `token` | CLI and bots | `Authorization: Bearer <t>`; sha256 compared with `hmac.compare_digest` against `serve.auth.tokens` (hashes only) |
| `trusted_header` | oauth2-proxy / IAP | `user_header` + `groups_header`, accepted **only** when the peer address is in `trusted_proxies`, else 401 |
| `alb_oidc` | AWS ALB `authenticate-oidc` / Cognito | Verify `x-amzn-oidc-data` (ES256) with the key at `https://public-keys.auth.elb.{region}.amazonaws.com/{kid}` (cached; GovCloud endpoints differ). The JWT **header** must have `signer == alb_arn`; enforce the header's `exp`. User = configured claim (default `email`, else `sub`); groups = `groups_claim`. Never trust `x-amzn-oidc-identity`. The ALB adds base64 padding to the JWT [V]: strip it before verification [I]. Targets must accept traffic only from the ALB security group [V]. |
| `dev` | local development | fixed user `dev` in group `admin`. `serve` refuses to start with it unless bound to a loopback address. |

`OnUnauthenticatedRequest: deny` makes the ALB return 401 to the SPA's XHR calls [V]. The UI's existing "session expired, reload" banner then applies.

### 7.4 Wire contract: v1 frozen, additive only

- `IncidentSummary` adds `orchestrator: str = "airflow"`, `error_name: str | None` and `links: IncidentLinks | None`.
- `IncidentLinks` adds `native: dict[str, str]` (labelled links from `adapter.links`). `task_instance`, `dag` and `healer` are unchanged.
- `allowed_actions` keeps the same ids. `retry_task` is offered only when `remediator.available(ref, native)` is non-empty. Its label comes from `vocabulary.retry_action` ("Redrive execution" for Step Functions), and it carries `warnings` (for example the `ci_deploy` warning when status is `repaired`).
- `GET /me` adds `host: "airflow_plugin" | "standalone"` and `orchestrator: {type, instance, display_name, vocabulary, capabilities}`. `base_url` keeps today's meaning; in standalone it is `serve.public_url`.
- `POST /events/task-failed` is unchanged. It is the Airflow listener and callback channel.
- New `POST /events/{channel}` for the other channels of the configured adapter's `PushIntake`, for example `eventbridge`:
  - same `X-Healer-Token` check, CSRF exemption and masking, stored as `PendingEvent(orchestrator, channel)`;
  - the rate limiter keys on `(channel, token hash)`, because EventBridge source IPs are shared;
  - the static `task-failed` route is declared first, so it keeps matching before the generic one.
  - EventBridge API destinations use a connection with `AuthorizationType: API_KEY`, `ApiKeyName: X-Healer-Token` [V], and must get a response within 5 s [V]. The handler is a pure normalize-and-store returning 202, which fits.

### 7.5 Deep links (`links.py`)

- The signatures of `task_instance_url`, `dag_url`, `healer_page_url`, `pr_md_url` and `incident_links` are unchanged. They point at the Healer views, which both hosts serve on the same shapes.
- The base URL chain is `AIRFLOW__API__BASE_URL` → `HEALER_PUBLIC_URL` → `cfg.serve.public_url` → `cfg.airflow.api_url`. The last fallback is used only when `orchestrator.type == "airflow"`; otherwise it is `http://{serve.host}:{serve.port}`. `public_base_url` is added as an alias of `airflow_base_url`.
- New: `native_links(cfg, incident) -> dict[str, str]` delegates to `get_adapter(cfg).links.links(ref, native_ref)`.
  - Airflow: `{"Airflow task": <base>/dags/<d>/runs/<r>/tasks/<t>[/mapped/<i>]}`.
  - Step Functions: `{"AWS console": ..., "CloudWatch logs": ...}` from configurable templates (9.11).
- Emails add an "Open in {product}" line only when native links exist **and** the type is not Airflow, so Airflow emails stay byte-identical.

### 7.6 React views reused in both hosts

- `ui/src/main.tsx` and `vite.config.ts` (UMD with React, ReactDOM, router and jsx-runtime external) are **unchanged**.
- New `ui/src/standalone.tsx`, modelled on today's `ui/src/dev.tsx`:
  - bundles React 19, ReactDOM and Chakra (React 19 ships no UMD [V], so the shell cannot provide host globals);
  - draws a small header (product name, user from `/me`);
  - picks `mode` from `location.pathname` (`/plugin/healer` → nav, `/dags/:d/plugin/healer-dag` → dag, `/dags/:d/runs/:r/tasks/:t[/mapped/:i]/plugin/healer-ti` → task_instance);
  - renders the **same** `App`. The existing pathname fallback in `context/pluginContext.ts` already parses these shapes.
- New `ui/vite.standalone.config.ts` builds an ES-module app into `src/dag_healer/api/static/standalone/` (`index.js` + assets), served by the existing `/healer/static` mount. `api.ts` (`new URL("healer/api/v1/...", document.baseURI)`) and `links.ts` work unchanged.
- Labels ("DAG", "task", "Retry task", the `messages.ts` strings such as "This DAG is not monitored ... monitor.dags") come from `me.orchestrator.vocabulary`. The defaults are today's Airflow strings, so the plugin renders identically before `/me` answers or if it fails.
- Server-provided `incident.links` are preferred when present. The native links show as an extra "Open in …" link.
- `hosting/shell.py` serves `index.html` (`<base href="{root_path}/">`, `<div id="root">`, `<script type="module" src="healer/static/standalone/index.js?v=...">`) for `/`, `/plugin/{route}` and `/dags/{rest:path}`. `/` redirects to `plugin/healer`.
- `scripts/build_ui.py` builds both bundles (`pnpm build && pnpm build:standalone`). `hatch_build.py` still *requires* only `main.umd.cjs` and warns if `standalone/` is missing, in which case `serve` answers 503 on `/` with a clear message. CI builds both.

### 7.7 Standalone deployment topology (Step Functions)

- An ECS or EC2 service runs `dag-healer serve` behind an ALB with the `authenticate-oidc`/Cognito action.
- A second service runs `dag-healer worker` with an IAM role.
- Both share Postgres. The repo is a git checkout on a volume, cloned for repairs exactly as today.
- An ALB listener rule forwards `/healer/api/v1/events/*` and `/healer/health` **without** the authenticate action; those paths are token-protected.

---

## 8. Airflow adapter (`dag_healer/airflow/adapter.py`)

| Today (phase 0-5) | After | Notes |
|---|---|---|
| `airflow/client.py` | unchanged; constants re-exported from `core.tracebacks` | `AirflowAPIError` → `OrchestratorError(transient = status in (None, 429, 5xx))` |
| worker `reconcile`, REST branch | `AirflowAdapter.poll` → `list_failed_task_instances(updated_since=cursor.watermark - 2 min)` | `Failure(source="rest", is_final=(state == "failed"))`; cursor `{"watermark": max end_date}`; source label stays `rest` so UI badges are identical |
| worker confirmation of listener/callback events | `AirflowAdapter.confirm` (phase-5 rules moved verbatim: `get_task_instance`; `failed` vs `up_for_retry`; later try exists; 404 → discarded) | |
| `api/routers/events.py` `task-failed` | unchanged route; `AirflowAdapter.normalize("task-failed", ...)` also exists for the conformance suite and future generic use | |
| `agent/context._fetch_log` | `AirflowAdapter.logs` → `get_task_log` rendered by `render_log_content` (`confidence="exact"`) | |
| error from the structured log | `AirflowAdapter.describe` → `get_task_instance` + `traceback_text` from the rendered `error_detail` | |
| `agent/context._api_dag_file`, `get_dag_source` | `pipeline_source` → `PipelineSource(path=fileloc, relative_path=relative_fileloc, text=get_dag_source(...) if include_text)` | |
| `agent/context._path_roots` (Airflow part) | `AirflowCodeLocator.path_mappings` → `("/opt/airflow/repo", "")`, `($AIRFLOW__CORE__DAGS_FOLDER, dags_dir)`, `("/opt/airflow/dags", dags_dir)` | `map_to_repo_path(path, cfg, mappings=None)` defaults to exactly these |
| `repo/dagscan.scan_dags` | `AirflowCodeLocator.scan(repo_root)` → `scan_dags(repo_root, cfg.repo.dags_dir)` (honours `init --dags-dir`) | dagscan code unchanged |
| worker `retry_task` → `clear_task_instances` | `remediate(..., RETRY_STEP)` → `clear_task_instances(dag_id, run_id, [task_id], only_failed=True)`; `RERUN_PIPELINE` → `trigger_dag_run` (off by default) | |
| `policy.INFRA_TYPES` (Airflow names) | stays in the policy; `AIRFLOW_TAXONOMY` repeats them (`AirflowTaskTimeout`, `AirflowTaskTerminated`, `AirflowZombieTaskException`, `TaskDeferralTimeout`) | decisions identical |
| `pytest_runner.scrubbed_env` Airflow part | `sandbox_env()` returns the `AIRFLOW_HOME` / `UNIT_TEST_MODE` / sqlite overrides; the core keeps the generic scrub plus the new cloud scrub | |
| `cli/doctor_cmd` `check_airflow_api` / `check_airflow_auth` / `check_plugin_entry_point` | `doctor_checks()` (same functions, same output) | the doctor CLI composes the generic checks with the adapter's |
| `links.py` Airflow routes | unchanged (Healer views) + `links.links()` (native TI page) | |
| `airflow/{plugin,listener,callback}.py`, `ingest.py` | **unchanged**; documented as the adapter's runtime side (push channel and UI host) | |

```python
class AirflowAdapter:  # implements OrchestratorAdapter, FailureFeed, PushIntake, RunInspector, Remediator, NativeLinks
    type = "airflow"
    display_name = "Apache Airflow"
    vocabulary = Vocabulary()
    capabilities = Capabilities(sources=("rest", "listener", "callback", "cli"), push_channels=("task-failed",),
                                attempts_visible=True, remediations=(RemediationKind.RETRY_STEP,),
                                fix_delivery="repo_sync", ui_hosts=("airflow_plugin", "standalone"), native_authz=True)
    taxonomy = AIRFLOW_TAXONOMY
    queue = None

    def __init__(self, cfg: HealerConfig, *, client: AirflowClient | Any | None = None) -> None:
        self._cfg, self._client = cfg, client          # AirflowClient.from_config(cfg) on first use (no I/O here)
        self.instance = cfg.orchestrator.instance_id
        self.feed = self.push = self.inspector = self.remediator = self.links = self
        self.code = AirflowCodeLocator(cfg)

FACTORY = AirflowFactory()   # type "airflow", options_model None (settings stay in cfg.airflow), secret_options ()
```

---

## 9. Step Functions adapter (reference design, `dag_healer/stepfunctions/`)

### 9.1 Facts the design rests on

The full list with sources is in the fact sheet. The points that shape the adapter:

**EventBridge [V]**
- Only Standard workflows emit "Execution Status Change". Delivery is best effort and can be out of order.
- `detail` carries `error`/`cause` and `redriveCount` but **no state name**.
- Input or output is dropped above 248 KiB.

**GetExecutionHistory [V]**
- Standard only. `maxResults` ≤ 1000. `reverseOrder` is available. Tokens expire after 24 h.
- `includeExecutionData` governs the "input or output of a history event".
- Events carry `id`/`previousEventId`.
- In the doc sample, `*StateEntered` can have `previousEventId: 0`.
- Rate bucket 400, refill 20/s.

**Executions [V]**
- `ListExecutions` returns the newest first and is ordered by `stopDate` for finished executions. It takes one `statusFilter` per call and is eventually consistent.
- `DescribeStateMachineForExecution` returns the definition that actually ran.

**RedriveExecution [V]**
- Standard only, status not SUCCEEDED, within 14 days, fewer than 24,999 events.
- Same input, definition and ARN; retry counters reset.
- `clientToken` is idempotent for the last 10 tokens for 15 min.
- Errors: `ExecutionNotRedrivable` / `ExecutionLimitExceeded` / `ValidationException`.

**Lambda [V]**
- The error payload is `{errorMessage, errorType, stackTrace}`.
- The default log group is `/aws/lambda/<fn>`, unless `LoggingConfig.LogGroup` overrides it.
- Logs can take 5 to 10 minutes to appear.

**Uncertain [U]**
- Whether `cause` and `taskScheduledEventDetails.parameters` survive `includeExecutionData=false` [I: `cause` is not "input or output", so it should survive].
- Whether `requestId` is in the cause.
- The `stackTrace` element shape per runtime.
- The console URL format.
- The resource types for `ListMapRuns` and `DescribeMapRun`.
- Whether Distributed Map children emit EventBridge events.
- The Express ARN format.

### 9.2 Identity and options

- **One adapter instance = one account + region.** `pipeline_id` comes from `options.pipelines` (`{pipeline_id: {state_machine: ARN|name, logical_id?, definition?}}`). Stable ids matter because SAM and CloudFormation names often carry random suffixes, and contracts are keyed by pipeline id. With `discover: true`, `ListStateMachines` adds every STANDARD machine, with `pipeline_id` = its name.
- **`run_id` = execution name** (`@startDate` suffix on reuse, see 2.2). **`step_id` = innermost failing state name.** **`map_index` = inline Map iteration index**, else -1. **`attempt = redriveCount + 1`.**
- **`native_ref`** keys: `execution_arn`, `state_machine_arn`, `start_date`, `stop_date`, `status`, `redrive_count`, `failed_event_id`, `entered_event_id`, `event_type`, `resource`, `resource_type`, `function_name`, `request_id`, `retry_attempts`, `step_path`, `map_run_arn`, `failed_children`, `definition_version_arn`.

### 9.3 Modules

| Module | Contents |
|---|---|
| `__init__.py` | `FACTORY` (light; no boto3 import) |
| `options.py` | `StepFunctionsOptions` (pydantic; 9.14) |
| `aws.py` | `AwsClients(options, *, client_factory=None)`: lazy boto3 `Session` (profile or AssumeRole with external id); clients `stepfunctions`, `logs`, `lambda`, `sqs`, `s3`, `sts`, all with `Config(retries={"mode": "adaptive", "max_attempts": 10}, connect_timeout=5, read_timeout=20)`; `client_factory` is the Stubber injection point; `ClientError` → core errors (throttling → transient, AccessDenied → PermissionDenied) |
| `arns.py` | parse and build state machine, execution, Map Run and version/alias ARNs |
| `events.py` | **pure**: EventBridge envelope (or SQS body wrapping one) → `FailureHint` |
| `history.py` | **pure**: `localize(events, *, redrive_count) -> Localized` (9.5) |
| `causes.py` | **pure**: Lambda cause JSON → `CauseInfo` → `render_python_traceback` |
| `asl.py` | **pure**: `index_states(definition) -> {name: StateInfo(type, resource, parameters, parent_path)}` (recursive through `Branches`, `Iterator`, `ItemProcessor`); `compute_target(state)`; S3 references (`ItemReader`, `ResultWriter`, literal Bucket/Key) |
| `iac/base.py`, `iac/sam.py` | `IacResolver` protocol; SAM resolver (v1) |
| `locator.py` | `SfnCodeLocator` (9.8) |
| `feed.py`, `intake.py`, `queue.py`, `inspector.py`, `logs.py` | ports (9.4 – 9.7) |
| `remediation.py`, `links.py`, `taxonomy.py` | 9.9, 9.11, 9.6 |
| `doctor.py`, `iam.py` | checks; `dag-healer doctor --print-iam-policy` (9.10) |
| `adapter.py` | `StepFunctionsAdapter` wiring |

### 9.4 Detection

**Poll** (`feed.py`; always on, the safety net):
- For each monitored STANDARD state machine and each status in `options.statuses` (default `FAILED`, `TIMED_OUT`; `ABORTED` is opt-in), call `ListExecutions(stateMachineArn, statusFilter=status, maxResults=100)`.
- Page while `stopDate >= cursor[sm] - overlap`, with `overlap = 5 min` because of eventual consistency.
- A redriven execution that fails again comes back with a new `stopDate` and a higher `redriveCount`.
- Each new `(executionArn, redriveCount)` is localised (9.5).
- Cursor: `{state_machine_arn: last_stop_ms}`. AWS `nextToken`s are never persisted (they expire after 24 h).
- At most `options.max_histories_per_poll` (default 50) per cycle, for the 20/s history refill; `complete=False` when that cap is hit.

**Queue** (`queue.py`; recommended push):
- EventBridge rule `{"source":["aws.states"],"detail-type":["Step Functions Execution Status Change"],"detail":{"status":["FAILED","TIMED_OUT"],"stateMachineArn":[...]}}` → SQS with a DLQ.
- `receive()` uses `ReceiveMessage(MaxNumberOfMessages=10, WaitTimeSeconds=1)`; `ack()` uses `DeleteMessageBatch` after the pending events are committed.
- Delivery is at-least-once, and the incident key dedupes.

**API destination** (optional): the same rule → API destination `https://<serve>/healer/api/v1/events/eventbridge` with an `API_KEY` connection. It needs the unauthenticated ALB path (7.7).

**`normalize`** (pure):
- Validate `source == "aws.states"`, the detail-type, and status ∈ statuses.
- Hint fields: `ref = AttemptRef(instance, pipeline_id_for(stateMachineArn), detail.name, UNRESOLVED_STEP, -1, detail.redriveCount + 1)`, `native = {execution_arn, state_machine_arn, status, redrive_count, error}`, `error_text = f"{error}: {cause}"`.
- **Drop `input`/`output`.**

**Caught failures and Express:** an execution that ends SUCCEEDED after a `Catch` is invisible to EventBridge and to polling. Only CloudWatch Logs at level ERROR show it [V]. This is future work, opt-in.

### 9.5 Confirmation and localisation

**`confirm(hint)`**
1. `DescribeExecution(execution_arn)`. `ExecutionDoesNotExist` → `discarded` ("forged or deleted").
2. Status not in statuses → `discarded`. Throttling or KMS throttling → `retry_later`.
3. `redriveCount + 1 != hint.attempt` → `discarded` ("stale; a newer attempt exists"); the newer one arrives on its own.
4. Localise, then return `confirmed(Failure(source=hint.source, is_final=True, ...))`.

**`history.localize`** (pure, table-tested):
1. Read the history with `reverseOrder=True`, `maxResults=1000`, `includeExecutionData=False`, until the latest `ExecutionRedriven` (only the last redrive segment matters) or `options.history_max_events`.
2. The terminal event is `ExecutionFailed`, `ExecutionTimedOut` or `ExecutionAborted`, with `error`/`cause` in its details.
3. The **terminal step failure** is the last event of type `TaskFailed`, `TaskTimedOut`, `TaskStartFailed`, `TaskSubmitFailed`, `LambdaFunctionFailed`, `LambdaFunctionTimedOut`, `LambdaFunctionStartFailed`, `LambdaFunctionScheduleFailed`, `ActivityFailed`, `ActivityTimedOut`, `ActivityScheduleFailed`, `MapRunFailed`, `EvaluationFailed` or `FailStateEntered` that is:
   - not followed by another `*Scheduled` of the same state entry (that would be a `Retry`);
   - not followed by a `*StateExited` of that state (that would be a `Catch`).
4. **Enclosing state.** Follow `previousEventId` to the nearest `*StateEntered` (`stateEnteredEventDetails.name`). Because entry events can point to 0 [V sample], fall back to the nearest preceding `*StateEntered` that has no matching `*StateExited`.
5. **Inline Map.** A `MapIterationFailed{index, name}` on the path gives `map_index = index`. `step_id` is the inner failing state, and `step_path = "<Map>[<index>]/<state>"`.
6. **Distributed Map.** `MapRunFailed` gives `step_id = <Map state>`, `map_index = -1`, and `map_run_arn` from `MapRunStarted`.
   - v1 stops here.
   - Future: `DescribeMapRun` + `ListExecutions(mapRunArn, statusFilter=FAILED, maxResults=5)`, localise the first Standard child into `failed_children`.
7. `EvaluationFailed` gives `step_id = evaluationFailedEventDetails.state` and `event_type = "EvaluationFailed"` (a definition error).
8. Terminal without any step failure (an execution timeout) gives `RUN_LEVEL_STEP`.
9. `retry_attempts` = the number of `*Scheduled` events of that state entry minus 1. `resource`, `resource_type` and `function_name` come from the `*Scheduled` or `*Failed` details, or from the definition (9.8).
10. Unparseable or unreadable history (Express, KMS denied): after 3 cycles of `retry_later`, confirm with `RUN_LEVEL_STEP` and a note. The incident is still created.

Optimised `lambda:invoke` Tasks emit `Task*` events with `resourceType`/`resource`. Legacy Lambda-ARN Tasks emit `LambdaFunction*` events [I]. Both are handled.

### 9.6 Error rendering and taxonomy

**`describe`**
- Reads the error and cause from the localised event (re-reading history by event id when needed).
- If the cause is missing because of `includeExecutionData=false` [U], falls back to `DescribeExecution.cause`.
- When the cause is Lambda JSON, `causes.parse` handles both `stackTrace` shapes, preformatted strings and `[file, line, func, code]` lists [U per runtime], and renders `traceback_text`. That text then goes through `agent.context.extract_traceback` unchanged.
- Otherwise `traceback_text=""`, `raw_error=f"{error}: {cause}"`, and `error_name=error`.

**`SFN_TAXONOMY`**

| Kind | Names and event types |
|---|---|
| infra | `States.Timeout`, `States.HeartbeatTimeout`, `States.Permissions`, `States.Http.Socket`, `Lambda.ServiceException`, `Lambda.SdkClientException`, `Lambda.AWSLambdaException`, `Lambda.TooManyRequestsException`, `Lambda.Unknown`, `Sandbox.Timedout`, `ExecutionLimitExceeded`; event types `TaskStartFailed`, `TaskSubmitFailed`, `LambdaFunctionStartFailed`, `LambdaFunctionScheduleFailed`, `ActivityScheduleFailed`, `ActivityTimedOut` |
| definition | `States.Runtime`, `States.DataLimitExceeded`, `States.ResultPathMatchFailure`, `States.ParameterPathFailure`, `States.QueryEvaluationError`, `States.NoChoiceMatched`; event `EvaluationFailed` |
| data_shape | `States.ItemReaderFailed` (missing or malformed S3 input) |
| aggregate | `States.ExceedToleratedFailureThreshold` |
| code | `Runtime.ImportModuleError`, `Runtime.HandlerNotFound`, `Runtime.UserCodeSyntaxError` [U names] |

Everything else (`KeyError`, `ValueError`, ...) goes through the existing data-shape and code gates.

All infra, definition and aggregate outcomes end AMBIGUOUS in v1, which is the safe default. Non-Python Lambdas produce no repo frames, so they never reach CODE.

### 9.7 Log correlation (`logs.py`, v1: Lambda only)

1. Log group: `GetFunction(...).Configuration.LoggingConfig.LogGroup`, else `/aws/lambda/<function>`.
2. Window: `[TaskStarted - 1 s, TaskFailed + 5 s]`.
3. If `request_id` is known (cause JSON, or a `START RequestId:` match), call `FilterLogEvents(filterPattern='"<request_id>"')` with confidence `exact`. Otherwise take the stream with an `[ERROR]` / `Traceback` / `Task timed out` line closest to the failure, with confidence `heuristic`.
4. JSON-format Lambda logs (`timestamp`, `level`, `message`, `requestId`, `errorType`, `stackTrace`) are rendered to lines. Output is capped at 200 lines.
5. Lambda logs can lag 5 to 10 minutes [V], so the **cause is the primary evidence** and the log is a bonus. A late or empty log adds a note ("logs may not be delivered yet") and is never fatal. Bounded deferral of gathering is future work.
6. The docs recommend that functions log `$$.Execution.Id`. That is the strongest correlation key.

### 9.8 Code mapping (`locator.py`)

The order is: explicit `options.code_map`, then the IaC resolvers, then nothing (a note, and `doctor` flags unmapped Task states).

**SAM (v1).**
1. A YAML `SafeLoader` subclass tolerates CloudFormation short tags (`!Ref`, `!GetAtt`, `!Sub`, `!Join`, `!If`, ...), turning them into `{"Fn::...": ...}` dicts.
2. `AWS::Serverless::StateMachine` (`DefinitionUri` / `Definition`, `DefinitionSubstitutions`) is matched to the pipeline by `logical_id` or a literal `Name`.
3. `asl.index_states` walks the repo ASL. For each Task state:
   - optimised `arn:aws:states:::lambda:invoke` with `Parameters.FunctionName` / JSONata `Arguments.FunctionName`, or a direct Lambda ARN;
   - `${X}` is resolved through `DefinitionSubstitutions` → `!GetAtt Fn.Arn` / `!Ref Fn` → logical id → `AWS::Serverless::Function` (`CodeUri`, `Handler`, with `Globals.Function` fallbacks);
   - result: `TaskInfo(task_id=<state>, operator="lambda", callable_name=<handler fn>, callable_module=<module>, callable_file=<CodeUri>/<module path>.py, lineno=<AST line of def>)`.
   - A handler `pkg.mod.fn` maps to `<CodeUri>/pkg/mod.py` [I].
4. `scan()` returns `DagInfo(dag_id=<pipeline_id>, file=<DefinitionUri or template>, tasks=[...], datasets=<S3 refs + repo literals in handler modules>)`.
5. `path_mappings(step)` returns `[PathMapping("/var/task", <CodeUri of step>)]`, plus layers `("/opt/python", <layer ContentUri>)`. With no step, it returns all known code roots; the context's suffix match remains the fallback.
6. The deployed definition (`DescribeStateMachineForExecution`) is joined to the repo ASL **by state name**. A state present in one and missing in the other is a drift note in the prompt and the UI.

**Future resolvers (same `IacResolver` protocol):**
- **CDK:** `cdk.out/*.template.json` + `Metadata["aws:asset:path"]` [V]. These are staged copies unless `aws:cdk:disable-asset-staging` / `--no-staging` is used, so hash-match them back to the source or ask for a `code_map` entry.
- **Terraform** (optional `python-hcl2`): `aws_lambda_function{handler, filename}` + `data.archive_file{source_dir|source_file}`; `aws_sfn_state_machine.definition` (heredoc or `templatefile`).
- **Serverless Framework:** `functions.<n>.handler` (`path/module.fn`); state machine `Fn::GetAtt: [n, Arn]` [V]; deployed name `service-stage-function` [U].
- **Fallback:** CloudFormation tags `aws:cloudformation:logical-id` / `stack-name` via `lambda:ListTags`. Propagation varies by resource type [V].

### 9.9 Remediation (`remediation.py`)

**`available(ref, native)`** (no I/O):
- `RESUME_RUN` when `options.retry_strategy` allows it, the machine is STANDARD, status ≠ SUCCEEDED and `stop_date` is within 14 days.
- `RERUN_PIPELINE` only when `options.rerun: true`.

**`RESUME_RUN`:**
- `RedriveExecution(executionArn, clientToken=idempotency_key[:64])` (token length limit [U]).
- `ExecutionNotRedrivable` / `ValidationException` → `RemediationResult(accepted=False, detail=<AWS message>)`.
- Success → `new_ref = ref.next_attempt()`. The next failure arrives as a new incident with `attempt + 1`.

**`RERUN_PIPELINE`:**
- `DescribeExecution.input` is read in memory only, never stored or logged. Inputs ≤ 256 KiB [V].
- `StartExecution(stateMachineArn, name=f"{orig[:52]}-healer-{n}", input=...)` gives `new_ref` with a new `run_id`.
- This is the only option after an ASL change: a redrive uses the original definition [V].

**Delivery gate** (fact sheet §8):
- `capabilities.fix_delivery = "ci_deploy"`. For incidents in status `repaired`, `retry_task` comes with the warning: "A redrive reruns the code deployed now with the original definition. Deploy the merged fix first."
- Future `deployed_fix_check`: compare `GetFunction` `CodeSha256`/`Version` with the fix commit and enable the action only when the fix is live.

Vocabulary: `retry_action = "Redrive execution"`. A redrive resumes **every** failed branch or iteration, not one step [V].

### 9.10 IAM (printed by `doctor --print-iam-policy`; verify against the Service Authorization Reference)

| Role | Actions | Resource |
|---|---|---|
| worker read | `states:ListExecutions`, `states:DescribeStateMachine` | `arn:aws:states:<r>:<a>:stateMachine:<prefix>*` [V level] |
| worker read | `states:DescribeExecution`, `states:GetExecutionHistory`, `states:DescribeStateMachineForExecution` | `arn:aws:states:<r>:<a>:execution:<prefix>*:*` [V level] |
| discovery (opt-in) | `states:ListStateMachines` | `*` [V] |
| Distributed Map (future) | `states:DescribeMapRun`, `states:ListMapRuns` | mapRun / execution [U exact type] |
| logs | `logs:FilterLogEvents` | the Lambda log group ARNs |
| code mapping (opt-in) | `lambda:GetFunction`, `lambda:ListTags` | the functions |
| queue mode | `sqs:ReceiveMessage`, `sqs:DeleteMessage`, `sqs:ChangeMessageVisibility`, `sqs:GetQueueAttributes` | the queue |
| datasets | `s3:GetObject` (+ `s3:GetObjectVersion`) | the dataset prefixes |
| KMS | `kms:Decrypt` | the CMK, if the state machine uses one [V] |
| **write role (opt-in, per machine)** | `states:RedriveExecution` | `...:execution:<sm>:*` [V]; Map Run: `...:execution:<sm>/<label>:*` [V] |
| write role (opt-in) | `states:StartExecution` | `...:stateMachine:<sm>` |

- Read and write are separate roles. Executions inherit the state machine's tags, so `aws:ResourceTag` conditions can scope the write role per machine [V].
- UI users' permissions live in `serve.auth.rules`, not IAM, in v1. Future `IamSimulatedAuthorizer`: map a principal claim to a role and call `iam:SimulatePrincipalPolicy` for `states:DescribeExecution` (read) and `states:RedriveExecution` (act), cached for 5 minutes.

### 9.11 Console links (`links.py`)

```yaml
console_execution_url: "https://{region}.console.aws.amazon.com/states/home?region={region}#/v2/executions/details/{execution_arn}"   # [U]
console_state_machine_url: "https://{region}.console.aws.amazon.com/states/home?region={region}#/statemachines/view/{state_machine_arn}"  # [U]
cloudwatch_log_url: "https://{region}.console.aws.amazon.com/cloudwatch/home?region={region}#logsV2:log-groups/log-group/{log_group_enc}"   # [U]
```

The defaults are commonly used patterns and are not documented contracts. They are configurable in `options.links`, and each has one unit test. Placeholders are quoted with `quote(value, safe=":")`.

### 9.12 Express workflows and Distributed Map (limits)

- **Express** has no EventBridge events, no `GetExecutionHistory`, no `ListExecutions` or `DescribeExecution` (except Map Run children), no redrive and no definition history [V]. Its history exists only in CloudWatch Logs, delivered best effort.
  - v1: `doctor` reports EXPRESS machines as unsupported, and `discover` skips them.
  - Future: a logs-based `FailureFeed` (`FilterLogEvents` with `{ $.type = "ExecutionFailed" }` on the logging group). Level ALL is needed for exact step names; level-ERROR logging of `*StateEntered` [U]. Attempt is always 1, remediation is `RERUN_PIPELINE` only, and stricter `max_incidents_per_hour`.
- **Distributed Map:** a parent-level incident (9.5 step 6). Child histories are separate, are kept 90 days, and Express children are visible only through logs [V]. Child descent is future work.

### 9.13 Datasets on S3 (`storage/s3.py`, v1, small)

- `load_s3_dataset(locator, *, max_bytes=50 MB)` uses `GetObject` with `Range: bytes=0-<max_bytes>` for CSV and JSON Lines, and a capped full download for Parquet.
- Masking is unchanged.
- Optional `version_at=<failed_at>` for versioned buckets is future work.
- With it, a violated contract on an S3 input reaches DATA (gate a) instead of AMBIGUOUS.

### 9.14 Options and example `healer.yaml`

```yaml
version: 1
repo: {path: ., base_branch: main, dags_dir: statemachines, allowed_paths: ["functions/", "src/"]}
orchestrator:
  type: stepfunctions
  instance: sfn-prod-euw1
  options:
    region: eu-west-1
    profile: null                   # local dev; else the default boto3 chain (ECS task role, IRSA, env)
    role_arn: null                  # optional AssumeRole (+ role_external_id, a secret option)
    endpoint_url: null              # VPC endpoint / LocalStack (NOT Step Functions Local: unsupported)
    template: template.yaml         # SAM template for the CodeLocator
    pipelines:
      sales_daily_etl:
        state_machine: arn:aws:states:eu-west-1:123456789012:stateMachine:sales-daily-etl-AbC123
        logical_id: SalesDailyEtlStateMachine
    discover: false
    statuses: [FAILED, TIMED_OUT]
    lookback_hours: 24
    history_max_events: 5000
    max_histories_per_poll: 50
    queue_url: "${HEALER_SFN_QUEUE_URL}"
    log_lookup: true
    retry_strategy: redrive         # redrive | disabled
    rerun: false
    code_map: {}                    # {LogicalIdOrFunctionName: {path: functions/x, handler: app.handler}}
    links: {}                       # overrides of the templates in 9.11
monitor: {dags: ["*"], modes: [poll, queue], poll_interval_seconds: 60, trigger: final_failure}
datasets:
  sales_daily_etl:
    TransformSales: {type: csv, path: "s3://acme-landing/incoming/sales.csv"}
serve:
  public_url: https://healer.example.com
  auth:
    backend: alb_oidc
    alb_arn: arn:aws:elasticloadbalancing:eu-west-1:123456789012:loadbalancer/app/healer/abc
    region: eu-west-1
    groups_claim: "cognito:groups"
    rules:
      - {principals: ["group:data-eng"], pipelines: ["*"], actions: [read, act, approve_contract]}
      - {principals: ["authenticated"], pipelines: ["sales_*"], actions: [read]}
store: {url: "${HEALER_DATABASE_URL}"}
ingest: {token: "${HEALER_INGEST_TOKEN}"}
```

`capabilities = Capabilities(sources=("poll", "eventbridge", "sqs", "cli"), push_channels=("eventbridge",), attempts_visible=False, remediations=(RESUME_RUN, RERUN_PIPELINE), fix_delivery="ci_deploy", ui_hosts=("standalone",))`.

`vocabulary = Vocabulary(product="AWS Step Functions", pipeline="state machine", pipelines="state machines", run="execution", step="state", steps="states", attempt="redrive attempt", map_index="item index", retry_action="Redrive execution")`.

---

## 10. Dagster and Prefect notes (fit check, no code)

**Dagster**

| Port | Mapping |
|---|---|
| `FailureFeed` | GraphQL `runsOrError(filter:{statuses:[FAILURE]}, cursor, limit)` [V] |
| `PushIntake` | a user-installed `@run_failure_sensor` [V] that posts `{run_id, job_name, step_keys}` to `/events/dagster`; `confirm` re-reads the run through GraphQL |
| `RunInspector` | `get_step_failure_events()` / `logsForRun` give `step_key` and `error.to_string()` with the Python stack [V], so `traceback_text` maps directly; `capturedLogsMetadata(logKey)` gives the stdout/stderr URLs [V] |
| `CodeLocator` | AST over `@op` / `@asset` (reuses `repo/pyscan`-style helpers) |
| `Remediator` | `ReexecutionOptions.from_failure` [V] (`RESUME_RUN`) |

Identity: `pipeline_id` = job, `run_id` = run id, `step_id` = step key (mapping-key format [U]), `map_index = -1`. Host: standalone. Python source, so the pytest repair path is reused.

**Prefect**

| Port | Mapping |
|---|---|
| `PushIntake` | Automation `EventTrigger(expect={"prefect.flow-run.Failed"})` + `call-webhook` [V] with `{{ flow_run.id }}`, normalised from `prefect.resource.id = prefect.flow-run.<uuid>` [V] |
| `FailureFeed` | `POST /api/flow_runs/filter` (`state.type.any_ ["FAILED"]`) + `/api/task_runs/filter` [V] |
| `RunInspector` | task-run state message; logs filter API (request shape [U]) |
| `Remediator` | `flow-run retry`, which keeps the run id and increments `run_count` [V] (`RESUME_RUN`) |
| `NativeLinks` | `flow_run\|ui_url` [V] |

Identity: `attempt` = `run_count`. Host: standalone.

Both fit the ports without changes. Both confirm that the push → confirm → localise pattern, the opaque `native_ref` and the neutral `Vocabulary` are the right seams.

---

## 11. Testing

### 11.1 Adapter conformance suite (`dag_healer/orchestrators/conformance.py`, packaged)

```python
@dataclass(frozen=True)
class SeededFailure:
    ref: AttemptRef
    is_final: bool
    exception_type: str                # expected from extract_traceback
    repo_frame: str | None             # repo-relative file of the innermost repo frame
    step_file: str | None              # TaskInfo.callable_file the CodeLocator must find

class ConformanceHarness(Protocol):
    def build(self, tmp_path: Path) -> tuple[HealerConfig, OrchestratorAdapter]: ...
    def seeded_failure(self) -> SeededFailure: ...
    def seeded_retrying(self) -> SeededFailure | None: ...       # None when attempts are not visible
    def forged_hint(self) -> FailureHint | None: ...
    def push_samples(self) -> list[tuple[str, dict, dict]]: ...   # (channel, payload, headers)
    def io_calls(self) -> int: ...                                # transport/Stubber call counter

class OrchestratorConformance:
    """Subclass and provide a ``harness`` fixture."""
```

The suite covers:
1. **Protocols.** `isinstance` for every present port. Capabilities are consistent: `push_channels ⇔ push`, `feed`, `remediations ⇔ remediator`, `sources ⊆ store.SOURCES`.
2. **Construction does no I/O** (`io_calls() == 0`).
3. **`poll`** returns the seeded failure with the exact key. Two overlapping polls create no new incident when run through the real store.
4. **`normalize` is pure.** It runs under a socket guard and drops input/output keys. Every push sample confirms to the **same key** as `poll`; the forged hint is `discarded`; a retrying attempt has `is_final=False`.
5. **Evidence.** `describe` + `logs` + `extract_traceback` give `exception_type`. Frames map through `path_mappings` to `repo_frame`, which exists in the fixture repo.
6. **`logs`** never raises for a missing attempt and returns notes.
7. **`CodeLocator.scan`** is offline, finds the step, and `callable_file` exists.
8. **`remediate`** respects `available`, raises `NotSupported` otherwise, and is idempotent for the same key.
9. **Links** are absolute http(s). Ids containing `:`, `+`, `/` and spaces round-trip.
10. **Refs** fit the store (lengths ≤ 250, `map_index ≥ -1`, `attempt ≥ 1`). `native` is JSON-safe, ≤ 4 KB, and has no payload keys (`input`, `output`, `parameters`).
11. **Errors** are only `core.errors` types: a 500 or a network failure gives `OrchestratorError(transient=True)`. `health()` never raises.
12. **No payload leaks.** Captured log records of the adapter contain no seeded PII string.
13. **Import hygiene** (subprocess): importing the adapter package does not import boto3, airflow, fastapi or pandas.
14. **End to end.** `worker.process_incident` with `FakeProvider` and the repo's `fixtures/fake_llm` scripts gives DATA → reported, CODE → repaired (PR.md + test), AMBIGUOUS → waiting_human.

It runs against:
- `fake` (in-memory);
- `airflow` (httpx `MockTransport` + the phase-2 fixtures in `tests/unit/fixtures/logs/airflow_api/`, including the real failed `customers_sync` log);
- `stepfunctions` (botocore `Stubber` + `tests/unit/fixtures/stepfunctions/`).

### 11.2 Fake orchestrator (`orchestrators/fake.py`)

- `FakeWorld` provides:
  - `add_pipeline(pipeline_id, *, file, steps: dict[str, TaskInfo])`
  - `fail(pipeline_id, run_id, step_id, *, exc: BaseException | None = None, text: str | None = None, map_index=-1, attempt=1, final=True, error_name=None) -> AttemptRef`
  - `push(channel, payload)`
  - `remediations: list`
- `fail(exc=...)` formats a real traceback from an exception raised by the fixture repo's code in-process, so the frames point at real files.
- Vocabulary `Vocabulary(product="Fakeflow", pipeline="flow", run="run", step="step", attempt="attempt", retry_action="Retry step")`. All capabilities are on.
- `type: fake` with `options.world: fixtures/fake_orchestrator/day2.yaml` runs the complete DATA/CODE/AMBIGUOUS demo **natively on Windows, without Docker**: `python scripts/tasks.py demo-fake`.

### 11.3 Step Functions fixtures and tools

Fixtures in `tests/unit/fixtures/stepfunctions/` are built from the documented shapes now and replaced by sanitised recordings once an account is available:
- EventBridge FAILED / TIMED_OUT / ABORTED / SUCCEEDED events, plain and SQS-wrapped.
- History pages:
  - Python Lambda `KeyError` after 2 retries;
  - the `parse_signup_date` `ValueError`;
  - inline Map iteration 3 failing;
  - Parallel branch;
  - caught failure;
  - `MapRunFailed` with `ExceedToleratedFailureThreshold`;
  - `ExecutionTimedOut`;
  - `States.Runtime` `EvaluationFailed`;
  - a redriven execution failing again;
  - legacy `LambdaFunctionFailed`;
  - a `*StateEntered` with `previousEventId: 0`.
- `ListExecutions`, `DescribeExecution` (failed and running), `DescribeStateMachineForExecution`, `FilterLogEvents` (text and JSON Lambda logs), `GetFunction`, `RedriveExecution` success and `ExecutionNotRedrivable`, S3 `GetObject`.

Fixture repo `tests/unit/fixtures/repos/sfn_etl/`:
- `template.yaml` (SAM, `DefinitionSubstitutions`, `Globals`), `statemachine/etl.asl.json` (with a Map), `functions/{sales,customers,inventory}/app.py` importing `src/etl/*` copied from `example_existing_repo` (same latent bug and fix);
- `healer.yaml`, contracts (one with an `s3://` locator served by a stubbed S3), and day-2 CSVs.

Tools:
- **botocore `Stubber`** is primary: offline, no credentials, exact request assertions. Clients use dummy credentials and `AWS_EC2_METADATA_DISABLED=true`.
- **moto** is used only in one opt-in wiring test (`HEALER_TEST_MOTO=1`) for `ListStateMachines`/`ListExecutions` paging. It lacks `redrive_execution` and `test_state`, and its history is static unless `execute_state_machine` is on [V].
- **Step Functions Local is not used:** it is unsupported [V].
- **TestState** (real AWS) is a future CI check for ASL fixes.
- **Live e2e** (`HEALER_SFN_LIVE=1`, manual) deploys `examples/stepfunctions_repo/` with SAM and asserts the three categories.

### 11.4 Store migration tests

- `tests/unit/fixtures/store/schema_v1.sql` is the frozen phase-5 DDL. A populated v1 SQLite DB (incidents, events, alerts, commands, pending events, contracts) is migrated twice. Assertions:
  - the columns exist and the defaults read `airflow`;
  - row counts are equal and `foreign_key_check` is clean;
  - the same `(dag, run, task, idx, try)` can now be inserted under `orchestrator='sfn-x'`, while a duplicate under `airflow` is still rejected;
  - the second run is a no-op and `<db>.bak-v1` exists.
- A race: two engines call `migrate()`, and each version is applied exactly once.
- The same test runs against Postgres in the compose `tests` profile (`tests/airflow/test_store_migration_pg.py`).
- `init_db(allow_migrate=False)` on a v1 DB raises `StoreSchemaOutdated`, and the API answers 503.

### 11.5 Golden regression snapshots

Step 0 freezes, for the three demo incidents:
- the classify prompt;
- the repair prompts;
- PR.md;
- alert email text and HTML;
- `IncidentDetail` JSON;
- the `default_config_yaml()` output;
- `dag-healer check` output.

Under the Airflow adapter every later step must reproduce them byte for byte, apart from the documented additive JSON fields.

### 11.6 Guard and architecture tests

- `tests/unit/test_architecture.py` (AST import scan) enforces the rules in section 4.
- A grep guard: no `.list_failed_task_instances(`, `.get_task_instance(`, `.get_task_log(`, `.get_dag(`, `.get_dag_source(`, `.clear_task_instances(` or `.trigger_dag_run(` call outside `airflow/`, `cli/doctor_cmd.py` and `scripts/`.
- `core` and `orchestrators/__init__` import in < 50 ms with no third-party modules (`sys.modules` diff).
- The task-runner import graph is unchanged (extends the existing light-import test).

### 11.7 Host and auth tests

- The existing `test_api_routes.py` / `test_api_security.py` pass unmodified.
- The same route suite is parametrised over:
  - (a) the faked Airflow overrides used today;
  - (b) standalone + `token` + RBAC;
  - (c) standalone + `alb_oidc` with a locally generated ES256 key, a stubbed key fetch, padded tokens, and a wrong `signer` or expired header `exp` → 401;
  - (d) `trusted_header` from an untrusted peer → 401.
- `dev` on a non-loopback host fails at startup.
- `/me` vocabulary and `allowed_actions` follow capabilities (no `retry_task` without a remediator).
- `/events/eventbridge`: the token check, 422 on a malformed envelope, `input` stripped.
- `tests/unit/test_standalone_host.py`:
  - the shell is served at `/plugin/healer` and `/dags/x/runs/y/tasks/z/plugin/healer-ti`;
  - `/healer/static/standalone/index.js` has a JavaScript MIME type;
  - the route guard test is reused.
- Container (`tests/airflow`): the auth matrix is unchanged, plus "plugin registered with 3 react_apps + `/healer`" and "`/me.orchestrator.type == "airflow"`".
- UI (vitest): standalone path → mode resolution; vocabulary defaults equal today's strings; existing view tests untouched.
- `pytest_runner`: a sandboxed test printing `os.environ` shows no `AWS_*` credential variables and `AWS_EC2_METADATA_DISABLED=true`.

---

## 12. Implementation plan for this repository

Prerequisite: the phase 2-5 workflow is committed. Every step ends with `tests/unit` and ruff green on the Windows host. From step 6 on, `python scripts/tasks.py test-airflow` and `python scripts/tasks.py demo` must also stay green with unchanged outcomes.

### 12.1 Steps

**Step 0. Baseline** (0.5 d)
- Tag `phase-5`.
- Inventory:
  - grep `worker/`, `agent/context.py`, `cli/` and `api/` for every `client.` call and amend the table in section 8;
  - did phase 2-5 add a column (forced category)? If yes, fold it into migration v2;
  - did it add `cli/store_cmd.py` (`store reset`)? If yes, extend it;
  - where does the watermark live (`state.json`)?
  - does `DatasetLocator` accept `s3://`?
- Capture `tests/unit/fixtures/store/schema_v1.sql` and a populated v1 DB.
- Record the golden snapshots (11.5).
- Commit this document as `docs/architecture/orchestrators.md`.
- *Gate:* inventory reviewed.

**Step 1. Core types and ports** (1.5 d)
- New: `src/dag_healer/core/{__init__,ids,failures,evidence,specs,vocab,errors,tracebacks,ports}.py`.
- Move the dataclass bodies `TaskInfo`/`DatasetRef`/`DagInfo` into `core/specs.py`, with re-exports in `repo/dagscan.py`.
- Move the three traceback constants into `core/tracebacks.py`. `airflow/client.py` re-exports them, and `agent/context.py` imports them from `core`.
- Tests: `tests/unit/core/*`.
- *Gate:* there is no behaviour change, and every existing test passes unmodified.

**Step 2. Registry, compat, fake adapter, conformance** (1.5 d)
- `src/dag_healer/orchestrators/{__init__,registry,compat,fake,conformance}.py`.
- `pyproject.toml`: entry points and extras.
- `cli/orchestrators_cmd.py` (`dag-healer orchestrators list`); `cli/main.py` registers it.
- `tests/unit/orchestrators/test_fake.py`.
- *Gate:* the fake passes conformance; the end-to-end test is skipped until step 6.

**Step 3. Configuration** (1 d; parallel with step 4)
- `config.py`: `OrchestratorConfig`, `ServeConfig`, `ServeAuthConfig`, `RbacRule`, `monitor.dags/pipelines` alias, widened modes + `wants()`, dynamic secret options, template comment block, SFN template variant.
- `contracts/schema.py`: input aliases `pipeline_id` / `step_id`.
- Tests: every existing config fixture and the demo YAML load to identical effective settings; the template snapshot is unchanged except the comment block.

**Step 4. Store v2** (2 d; parallel with step 3)
- `store/models.py`, `store/migrations.py`, `store/db.py` (`init_db(allow_migrate=...)`, `StoreSchemaOutdated`), `store/repo.py` (6-column key, `create_incident_from_failure`, `kv_*`, orchestrator filters), `cli/store_cmd.py` (`status`, `migrate`).
- Tests: `tests/unit/test_store_migrations.py`, `tests/airflow/test_store_migration_pg.py`.
- *Gate:* the v1 fixture DB and the demo DB migrate losslessly and idempotently.

**Step 5. Airflow adapter** (1.5-2 d; after steps 2 and 3)
- `airflow/adapter.py` (`AirflowAdapter`, `AirflowCodeLocator`, `FACTORY`) and `airflow/errors.py`. The phase-5 confirmation rules are *copied* into `confirm`; the worker switches in step 6.
- `tests/unit/orchestrators/test_airflow_adapter.py` (conformance with `MockTransport`).
- *Gate:* conformance green except the end-to-end case.

**Step 6. Core call sites onto the ports** (2.5-3 d; runs alone, because it touches shared files)
- `agent/context.py` (`as_adapter`, ports, `map_to_repo_path(..., mappings=None)`, new `IncidentContext` fields)
- `agent/policy.py` (taxonomy, `definition` rule), `agent/classify.py`, `agent/tests_gen.py`, `agent/prompts/{classify,repair_test,repair_fix}.md` (vocabulary tokens), `agent/repair.py` (scenario fields, `extra_env=adapter.sandbox_env()`)
- `repo/pytest_runner.py`: core cloud-credential scrub — `AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY`, `AWS_SESSION_TOKEN`, `AWS_SECURITY_TOKEN`, `AWS_PROFILE`, `AWS_ROLE_ARN`, `AWS_WEB_IDENTITY_TOKEN_FILE`, `AWS_CONTAINER_CREDENTIALS_*`, `AWS_CONTAINER_AUTHORIZATION_TOKEN*`, `GOOGLE_APPLICATION_CREDENTIALS`, `AZURE_CLIENT_SECRET` — plus `AWS_EC2_METADATA_DISABLED=true` and empty `AWS_SHARED_CREDENTIALS_FILE` / `AWS_CONFIG_FILE`
- `pr/prmd.py`, `alerts/templates/*` (vocabulary; Airflow identical), `links.py` (base chain, `native_links`)
- `worker/{loop,pipeline,commands}.py` (`adapter=` keyword, keeping `client=`; cursor in `healer_kv` with a one-time `state.json` import; queue drain; remediation)
- `cli/{init_cmd,doctor_cmd,worker_cmd}.py` (`code.scan`, `doctor_checks`, `check_orchestrator`)
- `api/deps.py` (`get_adapter`, instance filter), `api/views.py`, `api/schemas.py`, `api/routers/{incidents,dags,contracts,events,me}.py` (additive fields, `/events/{channel}`, capability-filtered actions, 503 on an outdated schema)
- Tests: guard tests (11.6); `test_worker_pipeline.py` parametrised over `[airflow(FakeAirflowClient), fake(FakeAdapter)]`; with `fake`, the prompts and PR.md contain "Fakeflow" and never "Airflow".
- *Gate:* golden snapshots byte-identical; the demo gives the same 3 outcomes; listener-only mode works; container tests green.

**Step 7. Standalone host and UI build** (2.5-3 d; parallel with step 8)
- `api/auth.py` (`AuthBackend`, `AirflowAuthBackend`, `apply_auth_backend`, `Principal` in `user_name`), `api/app.py` (`create_app(auth=)`, origins).
- `hosting/{__init__,app,shell,rbac}.py`, `hosting/auth/{token,trusted_header,alb_oidc,dev}.py`, `cli/serve_cmd.py`, `cli/main.py`.
- `ui/src/standalone.tsx`, `ui/vite.standalone.config.ts`, `ui/package.json` (`build:standalone`), `ui/src/{messages.ts,api.ts,model.ts}` (vocabulary, optional fields), `scripts/build_ui.py`, `hatch_build.py`, `pyproject.toml` `serve` extra.
- Tests: 11.7.
- *Gate:* `dag-healer serve` with `orchestrator.type: fake` and the `token` backend shows the three views with working actions on the Windows host; the Airflow bundle and views are unchanged (screenshots compared in the running stack).

**Step 8. Step Functions reference adapter v1** (4-5 d; parallel with step 7)
- `src/dag_healer/stepfunctions/*` (9.3), `src/dag_healer/storage/s3.py` + scheme dispatch in `datasets.py`.
- Fixtures (11.3), `tests/unit/stepfunctions/test_{events,history,causes,asl,sam,logs,remediation,taxonomy,links,iam}.py`, `tests/unit/orchestrators/test_stepfunctions_conformance.py`, `tests/unit/fixtures/repos/sfn_etl/`, `examples/stepfunctions_repo/`.
- *Gate:* conformance green including end to end. `dag-healer worker --once` with `type: stepfunctions` against Stubber recordings gives DATA (S3 contract), CODE (PR.md touches only `functions/customers/` or `src/etl/customers.py` and the new test) and AMBIGUOUS, all without AWS credentials.

**Step 9. Docs** (0.5 d)
- `docs/orchestrators/writing-an-adapter.md` (entry point, factory, conformance usage), `docs/orchestrators/stepfunctions.md` (IAM JSON, EventBridge → SQS rule, API destination + `API_KEY` connection, ALB OIDC + unauthenticated events path, limitations, unverified items), `docs/upgrade.md` (store v2 rollout), and a "Supported orchestrators" section in the README.

Order: 0 → 1 → 2, then 3 ∥ 4, then 5, then 6 alone, then 7 ∥ 8 (their files are disjoint; `pyproject.toml` is edited only by step 7, and step 8 adds its extra in a final one-line edit), then 9.

Total effort: about **16-20 engineer-days**, or 4-5 sequential workflow runs of 3-6 parallel agents.

### 12.2 Step Functions v1 scope (buildable and testable on Windows without credentials)

In v1:
- Standard workflows.
- Lambda Tasks, both optimised `lambda:invoke` and legacy ARNs.
- Inline Map, and Parallel.
- Distributed Map at parent level.
- Detection: poll + SQS queue + EventBridge API-destination channel.
- Confirmation and localisation from history.
- Lambda cause → Python traceback.
- Lambda CloudWatch log excerpt.
- SAM + `code_map` code mapping.
- Redrive, plus opt-in restart with the `ci_deploy` warning.
- Configurable console links, taxonomy, doctor checks and the IAM printer.
- S3 dataset reader.
- Fixtures, Stubber tests, conformance, fixture repo and example SAM repo.

### 12.3 Future work (explicitly not in v1)

- Express workflows (logs-based feed).
- Distributed Map child descent and nested `states:startExecution.sync` children.
- Caught-failure detection (logs at ERROR, opt-in).
- ECS / Batch / Glue log correlation and cause parsers.
- CDK / Terraform / Serverless resolvers.
- `deployed_fix_check` (`CodeSha256` against the fix commit).
- TestState-based ASL fix validation.
- `IamSimulatedAuthorizer`.
- Bounded log-arrival deferral.
- Per-child incidents (`map_key` in schema v3).
- A multi-instance combined view.
- A neutral `/api/v2` and physical column renames (with Alembic).
- Contract `aliases:` across orchestrators.
- Dagster and Prefect adapters.

---

## 13. Risks and open questions

### Risks

1. **Refactoring code that is being written right now** (worker, context, API).
   - *Mitigation:* start only after the phase-5 commit; keep every public signature (`gather_context(cfg, incident, client)`, `run_worker(..., client=)`, `api.auth` names); golden snapshots from step 0; the grep and architecture guards; step 6 runs alone.
2. **The SQLite table rebuild** in v2 is the only non-additive DDL.
   - *Mitigation:* SQLite's documented procedure (create, copy, drop, rename-new); `foreign_key_check`; an automatic `.bak-v1` copy; tests on a frozen v1 DB; Postgres uses a constraint swap.
   - *Residual risk:* an old worker breaks on the new ON CONFLICT target, so the worker and the migration must ship together.
3. **Production use of `dependency_overrides`** for host composition could surprise future contributors.
   - *Mitigation:* `apply_auth_backend` is the only writer; a test asserts the default app has no overrides; documented in `api/auth.py`.
4. **Push events cannot name the failing state** (EventBridge), so localisation costs one or more `GetExecutionHistory` calls per failure, under a 20/s refill.
   - *Mitigation:* reverse paging with an early stop, `history_max_events`, `max_histories_per_poll`, adaptive retries, `retry_later`, the existing `max_incidents_per_hour`.
5. **Semantic gaps between Step Functions and Airflow.**
   - A redrive resumes *all* failed branches and reruns the *deployed* code with the *original* definition: vocabulary label, `ci_deploy` warning, opt-in restart.
   - Retries are invisible as incidents: `every_failure` is rejected for this adapter.
   - Caught failures are invisible: documented, future opt-in.
6. **Repo-versus-deployment drift** (git clone against deployed Lambda or ASL).
   - *Mitigation:* the deployed definition joined by state name with drift notes; `doctor` flags unmapped states; the future `deployed_fix_check`.
7. **Weak evidence on Step Functions.** `cause` is capped at 32 KB, logs lag, `stackTrace` shapes vary, correlation without a request id is heuristic.
   - *Mitigation:* the cause is primary; a tolerant, fixture-driven parser; confidence flags in the prompt and UI; recommend logging `$$.Execution.Id` and JSON Lambda logs.
8. **The credential sandbox.** LLM-written tests run with the worker's network access, and ECS task-role endpoints are still reachable by raw HTTP even after the scrub.
   - *Mitigation:* the scrub plus IMDS disable, tested; network isolation stays out of scope and must be documented loudly for AWS deployments.
9. **The privacy surface grows** (EventBridge input, history data, CloudWatch lines).
   - *Mitigation:* `includeExecutionData=false`; normalisers keep a whitelist; the `native_ref` size limit and denylist; the core as the single masking point; a conformance check for PII in adapter logs.
10. **Standalone authorization duplicates policy** held in IAM or the IdP.
    - `trusted_header` is spoofable without the proxy, so `trusted_proxies` is required.
    - `alb_oidc` must verify the signature, `signer` and header `exp`, and targets must accept only ALB traffic.
11. **Two UI bundles** mean drift between host React and bundled React 19, longer builds and a larger wheel.
    - *Mitigation:* one `App`, CI builds both, the bundle size is reported.
12. **A single instance per process** means a mixed view needs two hosts until a multi-instance view exists.

### Open questions

1. Should `pipeline_id` for Step Functions default to the state machine name (current proposal) or require explicit `options.pipelines` entries when names carry stack suffixes?
2. Should `States.Permissions` be infra (AMBIGUOUS, current proposal) or a definition/IaC bug?
3. Should RBAC rules live in `healer.yaml` (current proposal) or in the store, editable from the UI?
4. Does the standalone UI show "Healer" only, or the product name with the orchestrator? Recommended: "Healer · AWS Step Functions".
5. What share of target users run Express workflows? This decides the priority of the logs-based feed.
6. Is the cross-orchestrator contract `aliases:` needed for the Airflow → Step Functions migration itself, so that a DAG and its replacement state machine share one approved contract?

### Unverified items to confirm with one real execution before 1.0

Each sits behind one function or template with a fixture:
- the console URL formats;
- whether a Lambda `cause` carries `requestId`, and the `stackTrace` element shape per Python runtime;
- whether `includeExecutionData=false` drops `cause` or `taskScheduledEventDetails.parameters`;
- CloudWatch execution-log record fields;
- the resource types of `ListMapRuns`/`DescribeMapRun`;
- whether Distributed Map Standard children emit EventBridge events;
- the Express ARN and `run_id` format;
- the `.sync` cause content for ECS, Batch and Glue;
- the `Runtime.*` Lambda error names used in the taxonomy;
- the `RedriveExecution` `clientToken` length limit;
- the Dagster step-key format for mapped ops;
- the Prefect logs filter request shape.

---

## Scorecard

Scale is 1 to 10, where 10 is best. For migration risk, 10 means the lowest risk.

| Criterion | P1 Minimal seam | P2 Hexagonal | P3 Step-Functions-first |
|---|---|---|---|
| Correctness against the facts | **8**. Facts are accurate and its uncertain items are flagged. It misses the "repaired ≠ deployed" gate except as a PR note. Its one-orchestrator-per-store rule conflicts with the side-by-side migration this design must support (a design flaw, not a factual error). | **7**. Attempt = ordinal of failed attempts including `Retry`, which contradicts the "terminal attempts only" principle and Airflow's try semantics. It suggests `endpoint_url` for "Step Functions Local", which is unsupported. Otherwise sound. | **8**. The most careful [U] flagging; correct Distributed Map and redrive handling. A few claims (ERROR-level logs omitting `*StateEntered`, the IAM resource mapping) were stated from memory, though they were flagged. |
| Fit with existing code | **9**. Accurate inventory; reuses the existing `Authorizer` and the two overrides; no moves. | **5**. Moves `airflow/*`, `api/auth.py`, `dagscan` and `ingest` behind shims (listener and plugin identity risk) and rewrites route auth to neutral `Identity`. | **6**. Keeps the `airflow/` package, but adds a new `/pipelines` route family, replaces `requires_access_dag` with direct `is_authorized_dag` calls, and adds a UI `HostAdapter` and hash routing. |
| Migration risk | **9**. Additive ADD COLUMN only, no route or UI change. | **5**. A rebuild of three tables, a changed ON CONFLICT target, web 503, moved modules. | **5**. Two migrations (v2 additive + v3 rebuild), new routes, UI routing rewrite. |
| Fit for Step Functions / Dagster / Prefect | **6**. Text-only error interchange; Airflow-shaped `FailureRef`; one store per orchestrator; no S3 datasets. | **8**. Clean small ports, log deferral, S3 store, packaged conformance suite. | **10**. Run-level sentinel, SQS with ack, Distributed Map dedupe, `includeExecutionData=false`, contract-hash stability, credential scrub, S3 datasets. |
| Testability | **8**. Conformance suite, fake adapter, golden snapshots, migration and guard tests. | **9**. Packaged conformance harness, fake builder, host-parametrised API tests, a Windows demo without Docker. | **9**. Scenario catalogue, packaged conformance suite, Stubber-first, golden parity, auth parity tests. |
| **Average** | **8.0** | **6.8** | **7.6** |

What the final design takes from each proposal:
- **P1:** the layout discipline (no moves), the dependency-override `AuthBackend`, standalone URL mirroring, confirm-then-create from the confirmed ref, `map_to_repo_path(..., mappings=None)`, and the stable pipeline-id map.
- **P2:** the small optional ports, the single masking point, ORM synonyms, the `healer_kv` cursor, the web-processes-never-migrate rule with 503, the packaged conformance harness, and the fake world.
- **P3:** keeping `dag_healer/airflow/` as the adapter package, the run-level sentinel, Distributed Map dedupe, the SQS ack flow, contract keys kept for hash stability, the credential scrub, the taxonomy with event types, the S3 datasets, and the localisation details.
- **The fact sheet** decided: terminal-only incidents, the redrive/deploy gate, `includeExecutionData=false`, the SQS recommendation, the IAM split, and the testing toolchain.
