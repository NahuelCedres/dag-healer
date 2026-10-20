# Airflow REST fixtures

Used by `tests/unit/test_airflow_client.py` and `tests/unit/test_context.py`.

Recorded from the demo stack (Airflow 3.3.2, `docker/compose.yaml`) on 2026-10-17 and
sanitized (`file_token` replaced with `REDACTED`; the data is the fake demo data):

| File | Request |
|------|---------|
| `customers_sync_transform_failed.json` | `GET .../taskInstances/transform/logs/1?full_content=true` (`Accept: application/json`) of `customers_sync` failing on the day-2 CRM export (`ValueError` in `parse_signup_date`) |
| `sales_daily_etl_extract_success.json` | the same endpoint for a successful `extract_sales` try |
| `airflow_api/task_instances.json` | `GET /api/v2/dags/~/dagRuns/~/taskInstances` (all states) |
| `airflow_api/task_instance_failed.json` | `GET .../taskInstances/transform` |
| `airflow_api/dag_customers_sync.json` | `GET /api/v2/dags/customers_sync` |
| `airflow_api/dag_source_customers_sync.json` | `GET /api/v2/dagSources/customers_sync` (`Accept: application/json`) |
| `airflow_api/dag_run_customers_sync.json` | `GET /api/v2/dags/customers_sync/dagRuns/{run_id}` |
| `airflow_api/clear_task_instances.json` | `POST /api/v2/dags/customers_sync/clearTaskInstances` (recorded with `dry_run: true`) |
| `airflow_api/trigger_dag_run.json` | `POST /api/v2/dags/customers_sync/dagRuns` |
| `airflow_api/health.json` | `GET /api/v2/monitor/health` |

Synthetic, modelled on the recorded shapes:

* `sales_daily_etl_transform_chained_keyerror.json`: a structured log whose `error_detail`
  holds a two-exception chain (pandas `KeyError` raised `from` the index lookup), plus a log
  line with an e-mail address and a phone number to check masking.
* `inventory_report_build_report_plain.json`: `content` as a list of plain strings (Airflow 2
  style file log with source lines and caret markers in the traceback).
