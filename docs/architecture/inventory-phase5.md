# Phase-5 inventory (step 0 of the orchestrator refactor)

Baseline: tag `phase-5` (commit `0f2f10f`) plus the design commit `24a41c2`. This file records
what the code actually does today wherever `docs/architecture/orchestrators.md` relies on
it, as asked by step 0 of section 12.1. Section 8 of the design was amended from these
findings. Paths are relative to `src/dag_healer/` unless they start with `tests/`, `docs/`
or `docker/`.

## 1. Every Airflow client call outside `airflow/`

Searched `worker/`, `agent/context.py`, `cli/` and `api/` (plus the rest of the package)
for `client.` and for the `AirflowClient` methods. `AirflowClient` offers `health`,
`list_failed_task_instances`, `get_task_instance`, `get_task_log`, `get_dag`,
`get_dag_source`, `clear_task_instances`, `trigger_dag_run` and `get_dag_run`.

| Caller | Call | What the caller does with it |
|---|---|---|
| `worker/pipeline.py` `_reconcile_rest` | `list_failed_task_instances(updated_since=since)` | Lists **only** `state=failed` task instances (`updated_at_gte` filter, paged by `id`). Unmonitored DAGs are skipped (`stats.unmonitored`), keys already in the store are skipped **before** any other call. |
| `worker/pipeline.py` `_reconcile_rest` | `get_task_log(dag_id, run_id, task_id, try_number, map_index=)` | Only for keys not in the store yet: `summarize_failure(dag_id, task_id, log)` gives the masked `error_summary` and the dedupe `signature` of the new incident (source `rest`). A failed log fetch still creates the incident (empty summary). |
| `worker/pipeline.py` `_reconcile_pending` | `get_task_instance(dag_id, run_id, task_id, map_index)` | Confirms a `pending_check` event with `_confirm(ti, try_number, trigger)`; see section 2 below. The incident summary and signature come from the pushed payload's `error` text, not from a log. |
| `worker/commands.py` `_retry_task` | `clear_task_instances(dag_id, run_id, task_ids, only_failed=True)` | `task_ids` is `[task_id]`, or `[(task_id, map_index)]` for a mapped task instance; `run_id`, `task_id`, `map_index` may be overridden by the command payload. Message: "`<actor>` cleared `<task>` in run `<run>` (N task instance(s)); Airflow will run it again.", plus "The proposed fix is not merged yet, so the task runs the current code." for a CODE incident with a branch or PR.md. `client is None` fails the command. |
| `worker/loop.py` `_default_client` / `run_worker` | `AirflowClient.from_config(cfg)`, `health()`, `base_url`, `close()` | The client is built once per worker; an unreachable API is only a warning (the poll retries). It is closed on exit when the worker created it. `run_worker(..., client=)` injects a client (tests use `FakeAirflowClient`). |
| `agent/context.py` `_fetch_log` | `get_task_log(dag_id, run_id, task_id, try_number, map_index=)` | The rendered log is the primary evidence: `extract_traceback(log)` first, then the explicit `error_text` / `incident.error_summary`. **No** `get_task_instance` call: the context never reads the TI state, operator or `error_detail` directly. `client is None` adds a note. |
| `agent/context.py` `_api_dag_file` | `get_dag(dag_id)` | `fileloc` mapped with `map_to_repo_path` when it is inside the repo, else `relative_fileloc` joined with `repo.dags_dir`; a mapped file outside the working tree adds a note. Falls back to the static scan's `DagInfo.file`. |
| `agent/context.py` `_apply_repo_info` | `get_dag_source(dag_id)` | Only when the DAG file cannot be read from the working tree. |
| `api/` | none | The API never calls Airflow's REST API. Airflow is used only through `api/auth.py` (`get_user`, `requires_access_dag`, `is_authorized_dag`, `get_authorized_dag_ids`). `api/deps.py` reads `request.client.host` (the rate limiter), which is not the Airflow client. |
| `cli/` | none | `doctor_cmd.check_airflow_api` (`GET <api_url>/api/v2/monitor/health`) and `check_airflow_auth` (`POST <api_url>/auth/token`) call httpx directly; `check_plugin_entry_point` reads the `airflow.plugins` entry points. `init_cmd` uses `scan_dags(repo_root, dags_folder)` with `--dags-dir`. |
| `scripts/` | direct REST calls | `scripts/demo.py` / `scripts/e2e_smoke.py` trigger DAGs through the REST API (excluded from the guard of 11.6). |

`trigger_dag_run` and `get_dag_run` have no caller in the package today.

## 2. Phase-5 confirmation and polling rules (to move verbatim into the Airflow adapter)

`worker/pipeline.py`:

- **REST poll window.** The watermark is the start time (`now`) of the last successful poll,
  not the latest `end_date`. `since = watermark - max(60 s, 2 x monitor.poll_interval_seconds)`;
  without a watermark `since = now - 24 h` (`INITIAL_LOOKBACK`); `since` is never earlier
  than the `rest_not_before` floor written by `dag-healer store reset`. A failed listing
  does not advance the watermark.
- **REST poll and `monitor.trigger`.** The poll lists `state=failed` only, so it never
  reports a retrying try: with `trigger: every_failure` the retrying tries come only from
  pushed events. Every REST failure is therefore `is_final=True` today.
- **Pending events** (`list_pending_events()`: `pending_check` rows, oldest first, at most
  500 per pass):
  - payload `source` not in `("listener", "callback")` becomes `listener`;
  - a source missing from `monitor.modes` -> discarded ("... events are not enabled");
  - an unmonitored DAG -> discarded;
  - no client -> kept (`pending_left`);
  - `get_task_instance` answering 404 -> discarded ("Airflow does not know this task
    instance"); any other error -> kept, and discarded once `received_at` is older than
    `PENDING_MAX_AGE = 24 h` (the design's 3.9 sketch says 1 h);
  - `_confirm(ti, try_number, trigger)`: `ti.try_number < try` -> discarded; with
    `every_failure`, confirmed when the state is `failed`/`up_for_retry` or a later try
    exists; with `final_failure`, a later try -> discarded ("was retried"), `failed` ->
    confirmed, `up_for_retry` -> discarded ("will be retried"), anything else -> discarded;
  - a confirmed event creates the incident with the event's key and source, a note "The
    `<source>` event was confirmed by the Airflow API (`<reason>`)." and the
    `pending_event_id`.

## 3. Store: no phase 2-5 column, `store_cmd.py` exists

- `store/models.py` is unchanged since phase 0-1 (`git diff 3034ba4 0f2f10f` touches only
  `store/db.py`: a PostgreSQL connect timeout). `SCHEMA_VERSION = 1`.
- **Forced category.** No new column: a human resolution is stored in the existing
  `resolution` (`data` | `code` | `ignore`) and `resolved_by` columns, and
  `worker/pipeline.FORCED_CATEGORIES = ("data", "code")` skips the classification. Nothing
  to fold into migration v2.
- `SOURCES = ("listener", "callback", "rest", "cli")` (unchanged).
- **v1 constraint names** (used by `_upgrade_v2`), confirmed in both dialects:
  `uq_healer_incidents_ti UNIQUE (dag_id, run_id, task_id, map_index, try_number)`,
  `healer_contracts_pkey` (PostgreSQL; inline `PRIMARY KEY (dag_id)` on SQLite),
  indexes `ix_healer_incidents_status`, `ix_healer_incidents_dag_created`,
  `ix_healer_incidents_signature`, `ix_healer_incident_events_incident`,
  `ix_healer_alerts_incident`, `ix_healer_commands_status`,
  `ix_healer_pending_events_status`; foreign keys `healer_incident_events_incident_id_fkey`,
  `healer_alerts_incident_id_fkey` and `healer_commands_incident_id_fkey`, all
  `ON DELETE CASCADE`.
- **`init_db` bug confirmed** (6.4): it runs `create_all` and inserts
  `SchemaVersion(version=SCHEMA_VERSION)` when that row is missing, so bumping
  `SCHEMA_VERSION` would stamp v2 on a v1 database without migrating it.
- **`cli/store_cmd.py` exists** (phase 2-5): `dag-healer store stats` and
  `dag-healer store reset --yes [--keep-state] [--artifacts]`. `reset` deletes the rows of
  every table except `healer_schema_version` (it would also empty the future `healer_kv`)
  and then calls `worker.pipeline.reset_state(cfg)`, which writes the watermark and the
  floor into `state.json`. Step 4 extends this module with `status` / `migrate`; step 6 must
  move the reset of the poll cursor to `healer_kv` (including the not-before floor).
- **Demo store.** The Docker stack keeps the store in PostgreSQL 16
  (`HEALER_DATABASE_URL=postgresql+psycopg2://airflow:airflow@postgres/healer`), so the demo
  DB takes the PostgreSQL migration path; unit tests and single-process installs use SQLite.

Frozen fixtures captured for step 4: `tests/unit/fixtures/store/schema_v1.sql` (SQLite DDL of
`init_db`), `tests/unit/fixtures/store/schema_v1.postgresql.sql` (`pg_dump --schema-only`
of the demo stack; it contains `SELECT pg_catalog.set_config('search_path', '', false);`,
so run it on a dedicated connection) and `tests/unit/fixtures/store/healer_v1.db` (4
incidents, 12 events, 3 alerts, 3 commands, 3 pending events, 3 contract states, version 1),
checked by `tests/unit/test_store_v1_fixture.py`.

## 4. Where the watermark lives

`<healer_home>/state.json` (`worker/pipeline.py`: `STATE_FILE`, `state_path`, `load_state`,
`save_state`, `get_watermark`, `set_watermark`, `get_not_before`, `reset_state`), written
atomically, with two keys:

- `rest_watermark`: ISO-8601 UTC start time of the last successful REST poll;
- `rest_not_before`: ISO-8601 UTC floor set by `store reset` (failures updated earlier are
  never listed again).

`healer_home` is `$HEALER_HOME`, else `$AIRFLOW_HOME/healer`, else `~/.dag-healer`. In the
Docker stack it is the `healer-home` volume at `/opt/airflow/healer`. The one-time import
into `healer_kv` (6.2) must carry **both** keys: the floor matters right after a reset.

## 5. `DatasetLocator` and `s3://`

- `contracts/schema.DatasetLocator.path` accepts any non-empty string: the validator only
  turns `\` into `/` and drops leading `./`, so `s3://bucket/key.csv` validates and keeps its
  double slash.
- `datasets.resolve_dataset_path` treats it as a relative path (`<repo>/s3:/bucket/key.csv`),
  so `load_dataset` fails with "dataset file not found". Step 8 adds the scheme dispatch.
- `repo/dagscan.scan_dags` ignores `s3://` literals (no `DatasetRef` is produced).

## 6. Other findings that affect later steps

1. **`links` name clash in the adapter bundle.** `OrchestratorAdapter.links` is an
   attribute of type `NativeLinks`, whose method is also called `links(ref, native)`. The
   section 8 sketch `self.feed = self.push = ... = self.links = self` therefore replaces the
   `links()` method of `AirflowAdapter` with the adapter object itself (calling
   `adapter.links.links(...)` would fail). The Airflow adapter (step 5) and the fake adapter
   (step 2) need a separate small object for `links`; `tests/unit/core/test_core_ports.py`
   shows the pattern.
2. **`to_prompt_dict()` is the classify prompt.** `agent/classify.context_payload` puts
   `IncidentContext.to_prompt_dict()` verbatim into the prompt, so the keys that 3.9 adds
   (`orchestrator`, `vocabulary`, `error_name`, `log_confidence`) change the classify prompt.
   Step 6 must either leave them out of the prompt under the Airflow adapter or re-record
   the golden snapshots as a documented change.
3. **Existing API tests pin the field sets.** `tests/unit/test_api_routes.py` asserts the
   exact key set of an `IncidentSummary` (`test_list_incidents_admin`) and the exact `/me`
   body (`test_me`), so the additive fields of 7.4 (`orchestrator`, `error_name`, `links`,
   `host`, `orchestrator` in `/me`) break "existing tests pass unmodified" unless those
   assertions are relaxed in the same step.
4. **`dag-healer check` prints `cfg.model_dump()`.** New `orchestrator:` / `serve:` sections
   (step 3) appear in its output even with defaults, so "check output unchanged" needs the
   new sections excluded when they are defaults, or a re-recorded snapshot.
5. **`describe` has no Airflow equivalent today.** The phase-5 context extracts the error
   from the log. If `AirflowAdapter.describe` calls `get_task_instance`, that is one more
   API call per incident; returning `traceback_text=""` keeps the error extraction on the
   log and the prompts identical.
6. **Push confirmation uses the payload text.** A confirmed listener/callback incident keeps
   the pushed `error` text for its summary and signature (no log fetch); a REST incident
   uses the log. `create_incident_from_failure` must keep both behaviours.
7. **Core import time.** `import dag_healer.core` takes about 45 ms on the Windows host,
   of which about 30 ms is the standard library (`dataclasses`, `typing`, `enum`); the
   "< 50 ms" guard of 11.6 is tight there and should measure against a stdlib baseline.
8. **Golden snapshots.** `tests/unit/test_golden_snapshots.py` records the 11.5 artifacts
   under `tests/unit/fixtures/golden/` (see its `README.md`); the pytest output embedded in
   PR.md and in the fix prompt is reduced to its `E`/`FAILED`/`ERROR` lines and summary,
   because the full output depends on the Python installation and the terminal width.
