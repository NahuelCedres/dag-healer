# ruff: noqa
# Bare @dag (dag_id defaults to the function name) with module-level @task functions.
from airflow.decorators import dag, task


@task
def fetch(source):
    return source


@task(task_id="combine_all")
def combine(values):
    return values


@task.bash
def announce():
    return "echo done"


@dag(schedule=None)
def taskflow_default():
    first = fetch("a")
    second = fetch("b")
    special = fetch.override(task_id="fetch_special")("c")
    mapped = fetch.expand(source=["x", "y"])
    combine([first, second, special, mapped]) >> announce()


taskflow_default()
