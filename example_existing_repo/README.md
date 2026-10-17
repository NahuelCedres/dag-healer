# data-pipelines

Airflow DAGs for the daily batch jobs: sales, CRM customers and inventory.

> Last updated: March 2024 (Airflow 2.7 migration). Some of this may be out of date,
> ask in #data-eng if something does not match.

## Layout

- `dags/` - one file per pipeline
- `src/etl/` - the actual job code (the DAGs just wire it together)
- `data/incoming/` - files dropped by the upstream teams (sftp sync job, see the ops wiki)
- `data/processed/` - job outputs, not committed

## Running locally

```bash
python3.8 -m venv venv && . venv/bin/activate
pip install -r requirements.txt
airflow dags test sales_daily_etl 2024-03-01
```

You can also call the jobs directly without Airflow:

```bash
python -c "from src.etl import sales; sales.extract_sales(); sales.transform_sales(); sales.load_sales()"
```

## Pipelines

| DAG | Owner | Schedule | Notes |
|-----|-------|----------|-------|
| `sales_daily_etl` | data-eng | daily 06:00 UTC | POS export -> daily summary per currency. Loads into `analytics.sales_daily` and emails the report to finance. |
| `customers_sync` | crm-team | nightly | CRM export -> customers dim. Signup dates are always `YYYY-MM-DD`. |
| `inventory_report` | ops | hourly | Stock per SKU across warehouses, flags SKUs below 20 units. |

## Deploy

Merge to `main`. The Airflow box pulls the repo every 5 minutes (cron job on the
scheduler host, ask Dave).

## Tests

TODO
