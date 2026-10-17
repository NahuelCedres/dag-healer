# customers_sync - CRM export -> customers dim
# maintained by crm-team
import os, sys
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from datetime import datetime

from airflow.sdk import DAG
from airflow.providers.standard.operators.python import PythonOperator

from src.etl.customers import extract_customers, transform_customers, load_customers


with DAG(
    dag_id="customers_sync",
    default_args={"owner": "crm-team", "retries": 0},
    schedule=None,
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=["legacy"],
    doc_md="Syncs the CRM customers export into the customers dim. See README.",
):
    t1 = PythonOperator(task_id="extract", python_callable=extract_customers)

    t2 = PythonOperator(task_id="transform", python_callable=transform_customers)

    t3 = PythonOperator(
        task_id="load",
        python_callable=load_customers,
        op_kwargs={"target": "customers_dim"},
    )

    t1 >> t2 >> t3
