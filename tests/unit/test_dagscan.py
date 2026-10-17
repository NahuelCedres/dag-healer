"""Tests for the static DAG scanner (dag_healer.repo.dagscan)."""

from __future__ import annotations

import logging
from pathlib import Path

import pytest

from dag_healer.repo.dagscan import DagInfo, DatasetRef, scan_dags
from dag_healer.repo.profile import snapshot_hashes

LEGACY_REPO = Path(__file__).parent / "fixtures" / "repos" / "legacy_etl"


@pytest.fixture(scope="module")
def legacy_dags() -> dict[str, DagInfo]:
    return {dag.dag_id: dag for dag in scan_dags(LEGACY_REPO)}


def _datasets(dag: DagInfo) -> set[tuple[str | None, str, bool]]:
    return {(ref.task_id, ref.path, ref.resolved) for ref in dag.datasets}


def _only(dags: list[DagInfo]) -> DagInfo:
    assert len(dags) == 1, [d.dag_id for d in dags]
    return dags[0]


# --------------------------------------------------------------------------- fixture repo


def test_fixture_repo_finds_every_dag_sorted_by_file(legacy_dags: dict[str, DagInfo]) -> None:
    dags = scan_dags(LEGACY_REPO)
    assert [d.dag_id for d in dags] == [
        "customers_sync",
        "inventory_report",
        "models_style",
        "sales_daily_etl",
        "taskflow_default",
    ]
    assert [d.file for d in dags] == sorted(d.file for d in dags)
    assert "helpers" not in legacy_dags


def test_legacy_module_level_dag_with_dag_kwarg(legacy_dags: dict[str, DagInfo]) -> None:
    dag = legacy_dags["sales_daily_etl"]
    assert dag.file == "dags/sales_daily_etl.py"
    assert [(t.task_id, t.operator) for t in dag.tasks] == [
        ("extract", "PythonOperator"),
        ("transform", "PythonOperator"),
        ("load", "PythonOperator"),
        ("audit", "PythonOperator"),
        ("notify", "BashOperator"),
    ]
    extract = dag.task("extract")
    assert extract is not None
    assert (extract.callable_name, extract.callable_module, extract.callable_file) == (
        "extract",
        "dags.sales_daily_etl",
        "dags/sales_daily_etl.py",
    )
    notify = dag.task("notify")
    assert notify is not None and notify.callable_name is None
    assert dag.imports == ["src.etl.common", "src.etl.sales"]


def test_callables_imported_with_from_and_aliased_module(legacy_dags: dict[str, DagInfo]) -> None:
    sales = legacy_dags["sales_daily_etl"]
    transform = sales.task("transform")
    audit = sales.task("audit")
    assert transform is not None and audit is not None
    assert (transform.callable_module, transform.callable_file) == ("src.etl.sales", "src/etl/sales.py")
    assert (audit.callable_name, audit.callable_module, audit.callable_file) == (
        "audit",
        "src.etl.common",
        "src/etl/common.py",
    )
    clean = legacy_dags["models_style"].task("clean")
    assert clean is not None
    assert (clean.callable_name, clean.callable_file) == ("transform", "src/etl/sales.py")


def test_callable_from_package_import_resolved_under_src(legacy_dags: dict[str, DagInfo]) -> None:
    report = legacy_dags["inventory_report"].task("build_report")
    assert report is not None
    assert (report.callable_name, report.callable_module, report.callable_file) == (
        "build_report",
        "etl.inventory",
        "src/etl/inventory.py",
    )
    assert legacy_dags["customers_sync"].imports == ["etl.customers"]


def test_legacy_datasets_attributed_to_tasks(legacy_dags: dict[str, DagInfo]) -> None:
    datasets = _datasets(legacy_dags["sales_daily_etl"])
    assert ("extract", "data/incoming/sales.csv", True) in datasets  # module constant in the DAG file
    assert ("transform", "data/incoming/sales.csv", True) in datasets  # literal inside the callable
    assert ("transform", "data/clean/sales_clean.parquet", True) in datasets  # constant used by callable
    assert ("load", "data/clean/sales_clean.parquet", True) in datasets
    assert ("audit", "data/audit.json", True) in datasets  # reached through a helper function
    assert all(ref.task_id is not None for ref in legacy_dags["sales_daily_etl"].datasets)
    ref = next(r for r in legacy_dags["sales_daily_etl"].datasets if r.task_id == "extract")
    assert ref == DatasetRef("extract", "data/incoming/sales.csv", "csv", True, "dags/sales_daily_etl.py:14")


def test_taskflow_dag_with_explicit_id_and_nested_tasks(legacy_dags: dict[str, DagInfo]) -> None:
    dag = legacy_dags["customers_sync"]
    assert [(t.task_id, t.operator, t.callable_name) for t in dag.tasks] == [
        ("extract", "@task", "extract"),
        ("clean_customers", "@task", "clean"),
        ("publish", "@task.python", "publish"),
    ]
    assert all(t.callable_file == "dags/customers_sync.py" for t in dag.tasks)
    datasets = _datasets(dag)
    assert ("extract", "data/incoming/customers.csv", True) in datasets  # pathlib chain from __file__
    # Constant of an imported module that no task uses: kept, but not attributed or found.
    assert (None, "customers_reference.json", False) in datasets


def test_bare_dag_decorator_override_expand_and_repeated_calls(legacy_dags: dict[str, DagInfo]) -> None:
    dag = legacy_dags["taskflow_default"]
    ids = {t.task_id for t in dag.tasks}
    assert ids == {"fetch", "fetch__1", "fetch__2", "fetch_special", "combine_all", "announce"}
    announce = dag.task("announce")
    assert announce is not None and announce.operator == "@task.bash"
    assert dag.imports == []


def test_context_manager_dag_task_group_and_loops(legacy_dags: dict[str, DagInfo]) -> None:
    dag = legacy_dags["inventory_report"]
    assert [(t.task_id, t.operator) for t in dag.tasks] == [
        ("start", "EmptyOperator"),
        ("staging.load_orders", "EmptyOperator"),
        ("staging.load_stock", "EmptyOperator"),
        ("build_report", "PythonOperator"),
    ]
    datasets = _datasets(dag)
    # Bare filename in op_kwargs: located under data/ in preference to fixtures/.
    assert ("build_report", "data/lookup/warehouses.csv", True) in datasets
    assert ("build_report", "data/incoming/inventory.csv", True) in datasets


def test_models_dag_reused_as_context_manager_and_bash_paths(legacy_dags: dict[str, DagInfo]) -> None:
    dag = legacy_dags["models_style"]
    assert [t.task_id for t in dag.tasks] == ["archive", "clean"]
    datasets = _datasets(dag)
    assert ("archive", "data/incoming/sales.csv", True) in datasets  # token of bash_command
    assert ("clean", "data/incoming/sales.csv", True) in datasets


def test_scan_never_modifies_the_repository() -> None:
    before = snapshot_hashes(LEGACY_REPO, ("**/*",))
    scan_dags(LEGACY_REPO)
    assert snapshot_hashes(LEGACY_REPO, ("**/*",)) == before


def test_scan_is_deterministic() -> None:
    assert [d.to_dict() for d in scan_dags(LEGACY_REPO)] == [d.to_dict() for d in scan_dags(LEGACY_REPO)]


# --------------------------------------------------------------------------- DAG styles


def test_airflow2_decorators_and_dag_kwarg_outside_context(make_repo) -> None:
    root = make_repo(
        {
            "dags/a.py": """
                from airflow.decorators import dag, task
                from airflow.models.dag import DAG as AirflowDAG
                from airflow.operators.empty import EmptyOperator

                with AirflowDAG(dag_id="ctx") as ctx_dag:
                    EmptyOperator(task_id="inside")

                EmptyOperator(task_id="outside", dag=ctx_dag)
                EmptyOperator(task_id="orphan")

                @dag(dag_id="flow")
                def flow_dag():
                    @task()
                    def step():
                        return 1
                    step()

                flow_dag()
            """
        }
    )
    dags = {d.dag_id: d for d in scan_dags(root)}
    assert set(dags) == {"ctx", "flow"}
    assert [t.task_id for t in dags["ctx"].tasks] == ["inside", "outside"]
    assert [t.task_id for t in dags["flow"].tasks] == ["step"]


def test_dag_id_positional_constant_and_file_name(make_repo) -> None:
    root = make_repo(
        {
            "dags/my_pipeline.py": """
                import os
                from pathlib import Path
                from airflow import DAG
                from airflow.sdk import DAG as SdkDAG

                PREFIX = "team"
                first = DAG(f"{PREFIX}_first")
                second = DAG(dag_id=os.path.basename(__file__).replace(".py", ""))
                third = SdkDAG(Path(__file__).stem + "_v2")
            """
        }
    )
    assert [d.dag_id for d in scan_dags(root)] == ["team_first", "my_pipeline", "my_pipeline_v2"]


def test_dags_generated_in_a_loop(make_repo) -> None:
    root = make_repo(
        {
            "dags/generated.py": """
                from airflow import DAG
                from airflow.operators.empty import EmptyOperator

                for name, table in [("orders", "t_orders"), ("users", "t_users")]:
                    with DAG(dag_id=f"sync_{name}") as dag:
                        EmptyOperator(task_id=f"copy_{table}")
            """
        }
    )
    dags = scan_dags(root)
    assert [(d.dag_id, [t.task_id for t in d.tasks]) for d in dags] == [
        ("sync_orders", ["copy_t_orders"]),
        ("sync_users", ["copy_t_users"]),
    ]


def test_unresolvable_dag_ids_are_skipped(make_repo) -> None:
    root = make_repo(
        {
            "dags/factory.py": """
                from airflow import DAG

                def build(dag_id):
                    with DAG(dag_id) as dag:
                        pass
                    return dag

                for dag_id in load_ids():
                    globals()[dag_id] = build(dag_id)

                with DAG("static") as dag:
                    pass
            """
        }
    )
    assert [d.dag_id for d in scan_dags(root)] == ["static"]


def test_task_groups_nesting_and_prefix_disabled(make_repo) -> None:
    root = make_repo(
        {
            "dags/groups.py": """
                from airflow.sdk import DAG, TaskGroup, task, task_group
                from airflow.providers.standard.operators.empty import EmptyOperator

                @task
                def leaf():
                    return 1

                @task_group
                def section():
                    leaf()

                with DAG("groups") as dag:
                    with TaskGroup("outer"):
                        with TaskGroup(group_id="inner"):
                            EmptyOperator(task_id="deep")
                        section()
                    with TaskGroup("flat", prefix_group_id=False):
                        EmptyOperator(task_id="no_prefix")
            """
        }
    )
    ids = [t.task_id for t in _only(scan_dags(root)).tasks]
    assert sorted(ids) == ["no_prefix", "outer.inner.deep", "outer.section.leaf"]


def test_mapped_operator_with_partial_expand(make_repo) -> None:
    root = make_repo(
        {
            "dags/mapped.py": """
                from airflow.sdk import dag
                from airflow.providers.standard.operators.python import PythonOperator

                def process(path):
                    return path

                @dag
                def mapped():
                    PythonOperator.partial(task_id="process", python_callable=process).expand(
                        op_args=[["data/a.csv"], ["data/b.csv"]]
                    )

                mapped()
            """,
            "data/a.csv": "x\n1\n",
        }
    )
    dag = _only(scan_dags(root))
    assert [(t.task_id, t.callable_name) for t in dag.tasks] == [("process", "process")]
    assert _datasets(dag) == {("process", "data/a.csv", True), ("process", "data/b.csv", False)}


def test_imported_taskflow_function_and_reexported_callable(make_repo) -> None:
    root = make_repo(
        {
            "dags/shared.py": """
                from airflow.sdk import DAG
                from airflow.providers.standard.operators.python import PythonOperator
                from etl.tasks import extract
                from etl import run_report

                with DAG("shared") as dag:
                    extract()
                    PythonOperator(task_id="report", python_callable=run_report)
            """,
            "src/etl/__init__.py": "from .reports import run_report\n",
            "src/etl/reports.py": "def run_report():\n    return 1\n",
            "src/etl/tasks.py": """
                from airflow.sdk import task

                @task(task_id="extract_orders")
                def extract():
                    return "data/orders.csv"
            """,
        }
    )
    dag = _only(scan_dags(root))
    extract, report = dag.tasks if dag.tasks[0].task_id == "extract_orders" else dag.tasks[::-1]
    assert (extract.task_id, extract.operator, extract.callable_file) == (
        "extract_orders",
        "@task",
        "src/etl/tasks.py",
    )
    assert (report.callable_name, report.callable_module, report.callable_file) == (
        "run_report",
        "etl.reports",
        "src/etl/reports.py",
    )
    assert ("extract_orders", "data/orders.csv", False) in _datasets(dag)


def test_lambda_and_partial_callables(make_repo) -> None:
    root = make_repo(
        {
            "dags/wrapped.py": """
                import functools
                from airflow import DAG
                from airflow.operators.python import PythonOperator
                from src.jobs import run

                with DAG("wrapped") as dag:
                    PythonOperator(task_id="lam", python_callable=lambda: run("x"))
                    PythonOperator(task_id="part", python_callable=functools.partial(run, 1))
                    PythonOperator(task_id="ext", python_callable=external_fn)
            """,
            "src/jobs.py": "def run(arg):\n    return arg\n",
        }
    )
    tasks = {t.task_id: t for t in _only(scan_dags(root)).tasks}
    assert (tasks["lam"].callable_name, tasks["lam"].callable_file) == ("run", "src/jobs.py")
    assert (tasks["part"].callable_name, tasks["part"].callable_module) == ("run", "src.jobs")
    assert (tasks["ext"].callable_name, tasks["ext"].callable_file) == ("external_fn", None)


def test_function_local_imports_and_sys_path_insert(make_repo) -> None:
    root = make_repo(
        {
            "dags/local_imports.py": """
                import os
                import sys
                from airflow import DAG
                from airflow.operators.python import PythonOperator

                sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "lib"))
                from helpers.io import load

                def run():
                    from pipelines import cleaning
                    return cleaning.clean()

                with DAG("local_imports") as dag:
                    PythonOperator(task_id="load", python_callable=load)
                    PythonOperator(task_id="run", python_callable=run)
            """,
            "lib/helpers/io.py": "def load():\n    return open('data/raw.json')\n",
            "src/pipelines/cleaning.py": "def clean():\n    return 'data/clean.csv'\n",
            "data/raw.json": "{}",
        }
    )
    dag = _only(scan_dags(root))
    load = dag.task("load")
    assert load is not None and load.callable_file == "lib/helpers/io.py"
    assert "helpers.io" in dag.imports and "pipelines.cleaning" in dag.imports
    datasets = _datasets(dag)
    assert ("load", "data/raw.json", True) in datasets
    assert ("run", "data/clean.csv", False) in datasets


def test_custom_dags_dir_and_dags_root_imports(make_repo) -> None:
    root = make_repo(
        {
            "airflow_home/dags/job.py": """
                from airflow import DAG
                from airflow.operators.python import PythonOperator
                from common import task_fn

                with DAG("job") as dag:
                    PythonOperator(task_id="t", python_callable=task_fn)
            """,
            "airflow_home/dags/common.py": "def task_fn():\n    return 'x.parquet'\n",
        }
    )
    assert scan_dags(root) == []
    dag = _only(scan_dags(root, dags_dir="airflow_home/dags"))
    assert dag.file == "airflow_home/dags/job.py"
    task = dag.tasks[0]
    assert (task.callable_module, task.callable_file) == ("common", "airflow_home/dags/common.py")


# --------------------------------------------------------------------------- dataset resolution


def test_os_path_join_with_dirname_parent_and_constants(make_repo) -> None:
    root = make_repo(
        {
            "dags/paths.py": """
                import os
                from os.path import dirname, join
                from airflow import DAG
                from airflow.operators.python import PythonOperator

                HERE = os.path.dirname(os.path.abspath(__file__))
                BASE = os.path.join(HERE, "..")
                DATA = join(BASE, "data")
                RAW = os.path.join(DATA, "incoming", "sales.csv")
                OUT = os.path.join(dirname(dirname(__file__)), "data", "out", "sales.parquet")

                def extract():
                    return RAW

                def write():
                    target = OUT
                    return target

                with DAG("paths") as dag:
                    PythonOperator(task_id="extract", python_callable=extract)
                    PythonOperator(task_id="write", python_callable=write)
            """,
            "data/incoming/sales.csv": "a\n1\n",
        }
    )
    assert _datasets(_only(scan_dags(root))) == {
        ("extract", "data/incoming/sales.csv", True),
        ("write", "data/out/sales.parquet", True),
    }


def test_pathlib_chains(make_repo) -> None:
    root = make_repo(
        {
            "dags/pl.py": """
                from pathlib import Path
                import pathlib
                from airflow.sdk import dag, task

                ROOT = Path(__file__).resolve().parents[1]
                DATA = ROOT.joinpath("data")

                @dag
                def pl():
                    @task
                    def a():
                        return DATA / "incoming" / "a.csv"

                    @task
                    def b():
                        here = pathlib.Path(__file__)
                        return (here.parent.parent / "data" / "b.json").with_suffix(".parquet")

                    a() >> b()

                pl()
            """
        }
    )
    assert _datasets(_only(scan_dags(root))) == {
        ("a", "data/incoming/a.csv", True),
        ("b", "data/b.parquet", True),
    }


def test_constants_from_imported_modules(make_repo) -> None:
    root = make_repo(
        {
            "dags/cfg_dag.py": """
                import os
                from airflow import DAG
                from airflow.operators.python import PythonOperator
                from etl import settings
                from etl.settings import DATA_DIR

                def a():
                    return os.path.join(DATA_DIR, "a.csv")

                def b():
                    return settings.REPORT

                with DAG("cfg_dag") as dag:
                    PythonOperator(task_id="a", python_callable=a)
                    PythonOperator(task_id="b", python_callable=b)
            """,
            "src/etl/settings.py": """
                import os
                ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
                DATA_DIR = os.path.join(ROOT, "data")
                REPORT = os.path.join(DATA_DIR, "reports", "daily.json")
            """,
        }
    )
    assert _datasets(_only(scan_dags(root))) == {
        ("a", "data/a.csv", True),
        ("b", "data/reports/daily.json", True),
    }


def test_bare_and_relative_literals_are_located_in_the_repo(make_repo) -> None:
    root = make_repo(
        {
            "dags/bare.py": """
                from airflow import DAG
                from airflow.operators.python import PythonOperator

                def read():
                    import pandas as pd
                    pd.read_csv("customers.csv")
                    pd.read_csv("incoming/orders.csv")
                    pd.read_csv("data/missing.csv")
                    pd.read_json("nowhere.json")
                    pd.read_parquet("/opt/airflow/repo/data/facts.parquet")
                    pd.read_parquet("/mnt/elsewhere/other.parquet")

                with DAG("bare") as dag:
                    PythonOperator(task_id="read", python_callable=read)
            """,
            "fixtures/customers.csv": "a\n",
            "data/raw/customers.csv": "a\n",
            "data/incoming/orders.csv": "a\n",
            "data/facts.parquet": b"PAR1",
        }
    )
    assert _datasets(_only(scan_dags(root))) == {
        ("read", "data/raw/customers.csv", True),
        ("read", "data/incoming/orders.csv", True),
        ("read", "data/missing.csv", False),
        ("read", "nowhere.json", False),
        ("read", "data/facts.parquet", True),
        ("read", "/mnt/elsewhere/other.parquet", False),
    }


def test_templated_paths_are_unresolved_placeholders(make_repo) -> None:
    root = make_repo(
        {
            "dags/tpl.py": """
                import os
                from airflow import DAG
                from airflow.operators.python import PythonOperator

                BASE = os.path.join(os.path.dirname(__file__), "..", "data")

                def read(ds, data_dir):
                    a = os.path.join(BASE, f"sales_{ds}.csv")
                    b = os.path.join(data_dir, "lookup.csv")
                    c = "{}/events.json".format(data_dir)
                    return a, b, c

                with DAG("tpl") as dag:
                    PythonOperator(task_id="read", python_callable=read)
            """,
            "data/ref/lookup.csv": "a\n",
        }
    )
    assert _datasets(_only(scan_dags(root))) == {
        ("read", "data/sales_{ds}.csv", False),
        ("read", "data/ref/lookup.csv", True),  # concrete file name found under data/
        ("read", "{data_dir}/events.json", False),
    }


def test_getenv_defaults_and_ignored_strings(make_repo) -> None:
    root = make_repo(
        {
            "dags/env.py": '''
                """Reads data/doc_only.csv but this docstring is not a dataset."""
                import os
                from airflow import DAG
                from airflow.operators.python import PythonOperator

                INPUT = os.getenv("INPUT_PATH", "data/input.csv")

                def run():
                    """Loads things.csv."""
                    files = [f for f in os.listdir(".") if f.endswith(".csv")]
                    pattern = "data/*.csv"
                    url = "https://example.com/feed.json"
                    return INPUT, files, pattern, url

                with DAG("env") as dag:
                    PythonOperator(task_id="run", python_callable=run)
            ''',
            "data/input.csv": "a\n",
        }
    )
    assert _datasets(_only(scan_dags(root))) == {("run", "data/input.csv", True)}


def test_unattributed_literals_stay_on_the_dag(make_repo) -> None:
    root = make_repo(
        {
            "dags/two.py": """
                from airflow import DAG
                from airflow.operators.empty import EmptyOperator

                SHARED = "data/shared.csv"

                with DAG("first", params={"input": "data/first.csv"}) as d1:
                    EmptyOperator(task_id="a")

                with DAG("second") as d2:
                    EmptyOperator(task_id="b")
            """
        }
    )
    dags = {d.dag_id: d for d in scan_dags(root)}
    assert _datasets(dags["first"]) == {(None, "data/first.csv", False), (None, "data/shared.csv", False)}
    assert _datasets(dags["second"]) == {(None, "data/shared.csv", False)}
    assert dags["first"].datasets[0].source == "dags/two.py:5"


def test_dataset_passed_to_taskflow_call_is_attributed(make_repo) -> None:
    root = make_repo(
        {
            "dags/flow.py": """
                from airflow.sdk import dag, task

                RAW = "data/raw.csv"

                @task
                def load(path):
                    return path

                @task
                def save(rows, target):
                    return target

                @dag
                def flow():
                    target = "data/out.parquet"
                    save(load(RAW), target)

                flow()
            """
        }
    )
    assert _datasets(_only(scan_dags(root))) == {
        ("load", "data/raw.csv", False),
        ("save", "data/out.parquet", False),
    }


# --------------------------------------------------------------------------- resilience


def test_syntax_errors_are_logged_and_skipped(make_repo, caplog: pytest.LogCaptureFixture) -> None:
    root = make_repo(
        {
            "dags/broken.py": "from airflow import DAG\nwith DAG('broken') as dag\n    pass\n",
            "dags/good.py": """
                from airflow import DAG
                from airflow.operators.python import PythonOperator
                from src.bad import fn

                with DAG("good") as dag:
                    PythonOperator(task_id="t", python_callable=fn)
            """,
            "src/bad.py": "def fn(:\n",
            "dags/binary.py": b"\x00\x01\x02",
        }
    )
    with caplog.at_level(logging.WARNING, logger="dag_healer.repo.dagscan"):
        dags = scan_dags(root)
    dag = _only(dags)
    assert dag.dag_id == "good"
    task = dag.tasks[0]
    assert (task.callable_name, task.callable_module, task.callable_file) == ("fn", "src.bad", "src/bad.py")
    messages = " ".join(r.getMessage() for r in caplog.records)
    assert "dags/broken.py" in messages and "src/bad.py" in messages


def test_missing_dags_dir_and_non_dag_files(make_repo, tmp_path: Path) -> None:
    assert scan_dags(tmp_path / "does-not-exist") == []
    root = make_repo({"dags/util.py": "def helper():\n    return 'data/x.csv'\n"})
    assert scan_dags(root) == []


def test_ignores_hidden_and_cache_directories(make_repo) -> None:
    dag_src = "from airflow import DAG\nd = DAG('{}')\n"
    root = make_repo(
        {
            "dags/real.py": dag_src.format("real"),
            "dags/.hidden/x.py": dag_src.format("hidden"),
            "dags/__pycache__/y.py": dag_src.format("cached"),
            "dags/sub/nested.py": dag_src.format("nested"),
        }
    )
    assert [d.dag_id for d in scan_dags(root)] == ["real", "nested"]


def test_dag_info_to_dict_is_json_ready(legacy_dags: dict[str, DagInfo]) -> None:
    import json

    payload = legacy_dags["sales_daily_etl"].to_dict()
    assert json.loads(json.dumps(payload))["tasks"][0]["task_id"] == "extract"
    assert set(payload) == {"dag_id", "file", "tasks", "datasets", "imports"}
