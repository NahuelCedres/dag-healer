# ruff: noqa
# Airflow 3 TaskFlow DAG with an explicit dag_id and a callable module imported from src/.
from pathlib import Path

from airflow.sdk import dag, task

from etl import customers

DATA = Path(__file__).parent.parent / "data"


@dag(dag_id="customers_sync", schedule=None, catchup=False)
def customers_pipeline():
    @task
    def extract():
        return str(DATA / "incoming" / "customers.csv")

    @task(task_id="clean_customers")
    def clean(path):
        return customers.clean(path)

    @task.python
    def publish(rows):
        return rows

    publish(clean(extract()))


customers_pipeline()
