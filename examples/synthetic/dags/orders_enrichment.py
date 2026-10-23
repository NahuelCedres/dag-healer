"""orders_enrichment: order system exports -> orders mart for BI (synthetic example)."""

from datetime import datetime

from airflow.providers.standard.operators.python import PythonOperator
from airflow.sdk import DAG
from src.etl.orders import enrich_orders, extract_orders, load_orders

with DAG(
    dag_id="orders_enrichment",
    default_args={"owner": "bi-team", "retries": 0},
    schedule=None,
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=["synthetic", "bi"],
    doc_md="Adds per-order metrics to the order system export and loads the orders mart.",
):
    extract = PythonOperator(task_id="extract_orders", python_callable=extract_orders)
    enrich = PythonOperator(task_id="enrich_orders", python_callable=enrich_orders)
    load = PythonOperator(
        task_id="load_orders", python_callable=load_orders, op_kwargs={"target": "orders_mart"}
    )

    extract >> enrich >> load
