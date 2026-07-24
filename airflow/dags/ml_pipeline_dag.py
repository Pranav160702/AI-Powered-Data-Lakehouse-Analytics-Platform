"""Airflow DAG for demand forecasting ML pipeline."""

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

DAG_ID = "ml_pipeline"
TASK_IDS = [
    "prepare_features",
    "train_model",
    "evaluate_model",
    "save_model",
    "generate_predictions",
]


def build_dag():
    """Build the ML DAG without importing Spark or training code at parse time."""

    if DAG is None or BashOperator is None:
        return None

    commands = task_commands()
    with DAG(
        dag_id=DAG_ID,
        default_args=DEFAULT_ARGS,
        start_date=airflow_start_date(),
        schedule_interval=airflow_schedule("@daily"),
        catchup=False,
        tags=["lakehouse", "ml"],
    ) as dag:
        prepare_features = BashOperator(
            task_id="prepare_features",
            bash_command=project_command(commands["prepare_features"]),
        )
        train_model = BashOperator(
            task_id="train_model",
            bash_command=project_command(commands["train_model"]),
        )
        evaluate_model = BashOperator(
            task_id="evaluate_model",
            bash_command=project_command(commands["evaluate_model"]),
        )
        save_model = BashOperator(
            task_id="save_model",
            bash_command=project_command(commands["save_model"]),
        )
        generate_predictions = BashOperator(
            task_id="generate_predictions",
            bash_command=project_command(commands["generate_predictions"]),
        )

        prepare_features >> train_model >> evaluate_model >> save_model >> generate_predictions
        return dag


dag = build_dag()
