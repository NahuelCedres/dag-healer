# ruff: noqa
# Context-manager DAG with Airflow 3 imports, a TaskGroup and loop-generated tasks.
from airflow.providers.standard.operators.empty import EmptyOperator
from airflow.providers.standard.operators.python import PythonOperator
from airflow.sdk import DAG, TaskGroup

from etl import inventory

TABLES = ["stock", "orders"]

with DAG(dag_id="inventory_report", schedule="@daily") as dag:
    start = EmptyOperator(task_id="start")
    with TaskGroup("staging"):
        for table in TABLES:
            EmptyOperator(task_id=f"load_{table}")
    report = PythonOperator(
        task_id="build_report",
        python_callable=inventory.build_report,
        op_kwargs={"warehouses": "warehouses.csv"},
    )
    start >> report
