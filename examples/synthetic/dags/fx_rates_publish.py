"""fx_rates_publish: daily FX rates -> pricing service (synthetic example)."""

from datetime import datetime

from airflow.providers.standard.operators.python import PythonOperator
from airflow.sdk import DAG
from src.etl.fx_rates import load_rates, publish_rates

with DAG(
    dag_id="fx_rates_publish",
    default_args={"owner": "pricing-team", "retries": 0},
    schedule=None,
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=["synthetic", "pricing"],
    doc_md="Publishes the latest daily FX rates to the pricing service.",
):
    load = PythonOperator(task_id="load_rates", python_callable=load_rates)
    publish = PythonOperator(task_id="publish_rates", python_callable=publish_rates)

    load >> publish
