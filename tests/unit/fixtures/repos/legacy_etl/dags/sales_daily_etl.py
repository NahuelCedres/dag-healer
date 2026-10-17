# ruff: noqa
# Legacy Airflow 2 style DAG: module-level DAG object, dag=dag on every operator.
import os
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

import src.etl.common as common
from src.etl.sales import load, transform

BASE = os.path.join(os.path.dirname(__file__), "..")
INPUT = os.path.join(BASE, "data", "incoming", "sales.csv")

default_args = {"owner": "data", "retries": 1, "retry_delay": timedelta(minutes=5)}

dag = DAG(
    "sales_daily_etl",
    default_args=default_args,
    schedule_interval="@daily",
    start_date=datetime(2024, 1, 1),
    catchup=False,
)


def extract(**context):
    import pandas as pd

    df = pd.read_csv(INPUT)
    return len(df)


t1 = PythonOperator(task_id="extract", python_callable=extract, dag=dag)
t2 = PythonOperator(task_id="transform", python_callable=transform, dag=dag)
t3 = PythonOperator(task_id="load", python_callable=load, dag=dag)
t4 = PythonOperator(task_id="audit", python_callable=common.audit, dag=dag)
t5 = BashOperator(task_id="notify", bash_command="echo sales loaded", dag=dag)

t1 >> t2 >> t3 >> t4 >> t5
