"""
Daily sales ETL.

Picks up the POS export dropped by the upstream team in data/incoming/sales.csv,
builds the per day / per currency revenue summary and publishes it for finance.

Owner: data-eng (#data-eng on slack)
"""
import os
import sys
from datetime import datetime, timedelta

# make src/ importable when airflow parses this file
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from airflow.providers.standard.operators.python import PythonOperator  # noqa: E402
from airflow.sdk import DAG  # noqa: E402

from src.etl import sales  # noqa: E402

default_args = {
    "owner": "data-eng",
    "depends_on_past": False,
    "retries": 0,
    "retry_delay": timedelta(minutes=5),  # leftover from when we had retries=3
}

with DAG(
    "sales_daily_etl",
    default_args=default_args,
    description="daily sales summary for finance",
    schedule=None,  # was "0 6 * * *", now triggered after the upstream drop
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=["legacy"],
) as dag:

    extract = PythonOperator(
        task_id="extract_sales",
        python_callable=sales.extract_sales,
    )

    transform = PythonOperator(
        task_id="transform_sales",
        python_callable=sales.transform_sales,
        op_kwargs={"report_currency": "USD"},
    )

    load = PythonOperator(
        task_id="load_sales",
        python_callable=sales.load_sales,
    )

    # send_report = PythonOperator(
    #     task_id="send_report",
    #     python_callable=sales.send_report,
    #     op_kwargs={"to": "finance-reports@example.com"},
    # )

    # TODO: re-enable send_report once finance gives us the new SMTP relay
    extract >> transform >> load  # >> send_report
