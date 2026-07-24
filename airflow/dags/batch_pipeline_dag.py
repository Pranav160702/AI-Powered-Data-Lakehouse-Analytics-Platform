"""Airflow DAG for Bronze and Silver batch pipelines."""

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

DAG_ID = "batch_pipeline"
TASK_IDS = [
    "validate_source_files",
    "ingest_bronze",
    "transform_silver",
    "run_data_quality_checks",
]


def build_dag():
    """Build the batch pipeline DAG without creating Spark sessions at parse time."""

    if DAG is None or BashOperator is None:
        return None

    commands = task_commands()
    with DAG(
        dag_id=DAG_ID,
        default_args=DEFAULT_ARGS,
        start_date=airflow_start_date(),
        schedule_interval=airflow_schedule("@daily"),
        catchup=False,
        tags=["lakehouse", "batch"],
    ) as dag:
        validate_source_files = BashOperator(
            task_id="validate_source_files",
            bash_command=project_command(commands["validate_source_files"]),
        )
        ingest_bronze = BashOperator(
            task_id="ingest_bronze",
            bash_command=project_command(commands["ingest_bronze"]),
        )
        transform_silver = BashOperator(
            task_id="transform_silver",
            bash_command=project_command(commands["transform_silver"]),
        )
        run_data_quality_checks = BashOperator(
            task_id="run_data_quality_checks",
            bash_command=project_command(commands["run_data_quality_checks"]),
        )

        validate_source_files >> ingest_bronze >> transform_silver >> run_data_quality_checks
        return dag


dag = build_dag()
