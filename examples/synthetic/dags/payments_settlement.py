"""payments_settlement: payment provider exports -> settlement ledger (synthetic example)."""

from datetime import datetime

from airflow.providers.standard.operators.python import PythonOperator
from airflow.sdk import DAG
from src.etl.payments import extract_payments, load_ledger, settle_payments

with DAG(
    dag_id="payments_settlement",
    default_args={"owner": "finance-data", "retries": 0},
    schedule=None,
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=["synthetic", "finance"],
    doc_md="Settles the daily payment provider exports into the settlement ledger.",
):
    extract = PythonOperator(task_id="extract_payments", python_callable=extract_payments)
    settle = PythonOperator(task_id="settle_payments", python_callable=settle_payments)
    load = PythonOperator(
        task_id="load_ledger", python_callable=load_ledger, op_kwargs={"target": "settlement_ledger"}
    )

    extract >> settle >> load
