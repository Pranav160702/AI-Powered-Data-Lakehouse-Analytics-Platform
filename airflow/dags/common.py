"""Shared helpers for lightweight Airflow DAG definitions."""

from __future__ import annotations

import os
from datetime import datetime, timedelta
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[2]
AIRFLOW_DAGS_DIR = PROJECT_ROOT / "airflow" / "dags"

DEFAULT_ARGS = {
    "owner": "lakehouse",
    "depends_on_past": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
}


def project_command(command: str) -> str:
    """Return a shell command that runs from the project root."""

    return f"cd {PROJECT_ROOT} && {command}"


def task_commands() -> dict[str, str]:
    """Return command strings used by Airflow DAG tasks."""

    return {
        "validate_source_files": "python - <<'PY'\nfrom pathlib import Path\nrequired = ['customers.csv','products.csv','categories.csv','orders.csv','order_items.csv','payments.csv','inventory.csv']\nmissing = [name for name in required if not (Path('data/raw') / name).exists()]\nif missing:\n    raise FileNotFoundError(f'Missing source files: {missing}')\nprint('source files validated')\nPY",
        "ingest_bronze": "python ingestion/batch_ingestion.py",
        "transform_silver": "python lakehouse/silver/silver_pipeline.py",
        "build_gold_tables": "python lakehouse/gold/gold_pipeline.py",
        "load_gold_to_postgres": "python database/load_gold_to_postgres.py",
        "run_data_quality_checks": "python -m pytest tests/unit/test_batch_schemas.py tests/unit/test_database_serving.py",
        "prepare_features": "python ml/feature_engineering_cli.py",
        "train_model": "python ml/train_demand_forecast.py",
        "evaluate_model": "python - <<'PY'\nfrom pathlib import Path\nmetrics = sorted(Path('models').glob('demand_forecast_metrics_*.json'))\nif not metrics:\n    raise FileNotFoundError('No demand forecast metrics found')\nprint(metrics[-1])\nPY",
        "save_model": "python - <<'PY'\nfrom pathlib import Path\nif not Path('models/demand_forecast_latest.joblib').exists():\n    raise FileNotFoundError('Latest model artifact missing')\nprint('latest model artifact exists')\nPY",
        "generate_predictions": "python ml/predict_demand.py",
    }


def airflow_start_date() -> datetime:
    """Return a stable DAG start date."""

    return datetime(2026, 7, 23)


def airflow_schedule(default: str | None = "@daily") -> str | None:
    """Allow schedules to be disabled for local DAG testing."""

    if os.getenv("AIRFLOW_DISABLE_SCHEDULES", "false").lower() == "true":
        return None
    return default
