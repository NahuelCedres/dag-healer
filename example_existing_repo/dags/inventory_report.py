import sys
import os

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from datetime import datetime

from airflow.sdk import DAG
from airflow.providers.standard.operators.python import PythonOperator

import src.etl.inventory as inv

args = {
    "owner": "ops",
    "retries": 0,
}

dag = DAG(
    dag_id="inventory_report",
    default_args=args,
    description="Inventory per SKU + low stock flags",
    schedule=None,  # TODO hourly again once the warehouse feed is stable
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=["legacy"],
)

getInventory = PythonOperator(
    task_id="get_inventory",
    python_callable=inv.getInventory,
    dag=dag,
)

buildReport = PythonOperator(
    task_id="build_report",
    python_callable=inv.buildReport,
    op_kwargs={"low_stock_threshold": 25},  # purchasing asked for 25 instead of 20
    dag=dag,
)

publish = PythonOperator(task_id="publish_report", python_callable=inv.publishReport, dag=dag)

getInventory >> buildReport >> publish
