"""Airflow DAG for Gold aggregation and serving loads."""

from __future__ import annotations

try:
    from airflow import DAG
    from airflow.operators.bash import BashOperator
except (ImportError, ModuleNotFoundError):
    DAG = None
    BashOperator = None

try:
    from airflow.dags.common import DEFAULT_ARGS, airflow_schedule, airflow_start_date, project_command, task_commands
except ModuleNotFoundError:
    from common import DEFAULT_ARGS, airflow_schedule, airflow_start_date, project_command, task_commands

DAG_ID = "gold_pipeline"
TASK_IDS = [
    "build_gold_tables",
    "load_gold_to_postgres",
    "run_data_quality_checks",
]


def build_dag():
    """Build the Gold serving DAG without heavy work at parse time."""

    if DAG is None or BashOperator is None:
        return None

    commands = task_commands()
    with DAG(
        dag_id=DAG_ID,
        default_args=DEFAULT_ARGS,
        start_date=airflow_start_date(),
        schedule_interval=airflow_schedule("@daily"),
        catchup=False,
        tags=["lakehouse", "gold"],
    ) as dag:
        build_gold_tables = BashOperator(
            task_id="build_gold_tables",
            bash_command=project_command(commands["build_gold_tables"]),
        )
        load_gold_to_postgres = BashOperator(
            task_id="load_gold_to_postgres",
            bash_command=project_command(commands["load_gold_to_postgres"]),
        )
        run_data_quality_checks = BashOperator(
            task_id="run_data_quality_checks",
            bash_command=project_command(commands["run_data_quality_checks"]),
        )

        build_gold_tables >> load_gold_to_postgres >> run_data_quality_checks
        return dag


dag = build_dag()
