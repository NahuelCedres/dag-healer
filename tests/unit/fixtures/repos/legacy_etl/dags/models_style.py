# ruff: noqa
# airflow.models import, DAG object reused as a context manager, aliased module import.
from airflow.models import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

import src.etl.sales as s

dag = DAG(dag_id="models_style", schedule=None)

with dag:
    archive = BashOperator(task_id="archive", bash_command="cp data/incoming/sales.csv /tmp/archive/")
    clean = PythonOperator(task_id="clean", python_callable=s.transform)
    archive >> clean
