"""Unit tests for lightweight Airflow DAG definitions."""

import sys
from pathlib import Path

DAGS_DIR = Path(__file__).resolve().parents[2] / "airflow" / "dags"
if str(DAGS_DIR) not in sys.path:
    sys.path.insert(0, str(DAGS_DIR))

from batch_pipeline_dag import TASK_IDS as BATCH_TASK_IDS
from common import project_command, task_commands
from gold_pipeline_dag import TASK_IDS as GOLD_TASK_IDS
from ml_pipeline_dag import TASK_IDS as ML_TASK_IDS


def test_batch_dag_task_ids_match_phase_requirements() -> None:
    """Batch DAG metadata should include required batch tasks."""

    assert BATCH_TASK_IDS == [
        "validate_source_files",
        "ingest_bronze",
        "transform_silver",
        "run_data_quality_checks",
    ]


def test_gold_dag_task_ids_match_phase_requirements() -> None:
    """Gold DAG metadata should include required serving tasks."""

    assert GOLD_TASK_IDS == [
        "build_gold_tables",
        "load_gold_to_postgres",
        "run_data_quality_checks",
    ]


def test_ml_dag_task_ids_match_phase_requirements() -> None:
    """ML DAG metadata should include required ML tasks."""

    assert ML_TASK_IDS == [
        "prepare_features",
        "train_model",
        "evaluate_model",
        "save_model",
        "generate_predictions",
    ]


def test_airflow_commands_run_from_project_root() -> None:
    """Airflow command helper should execute from the repository root."""

    command = project_command("python ml/train_demand_forecast.py")

    assert "cd " in command
    assert command.endswith("python ml/train_demand_forecast.py")
    assert "train_model" in task_commands()
