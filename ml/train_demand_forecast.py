"""Train a demand forecasting model from Silver lakehouse data."""

from __future__ import annotations

import argparse
import logging
import sys
from datetime import UTC, datetime
from pathlib import Path

import pandas as pd
from sklearn.ensemble import RandomForestRegressor
from sklearn.pipeline import Pipeline
from sklearn.preprocessing import StandardScaler

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from config.logging_config import configure_logging
from config.spark_config import spark_session
from ml.evaluate_model import evaluate_predictions
from ml.feature_engineering import FEATURE_COLUMNS, TARGET_COLUMN, load_features_from_silver
from ml.model_registry import save_json_artifact, save_model_bundle, model_version

logger = logging.getLogger(__name__)


def time_based_split(
    features: pd.DataFrame,
    test_days: int,
) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Split feature rows by date, keeping the latest days for test data."""

    if features.empty:
        raise ValueError("Cannot train on an empty feature dataset.")
    max_date = pd.to_datetime(features["sales_date"]).max()
    cutoff = max_date - pd.Timedelta(days=test_days)
    train_df = features[features["sales_date"] <= cutoff].copy()
    test_df = features[features["sales_date"] > cutoff].copy()
    if train_df.empty or test_df.empty:
        sorted_features = features.sort_values("sales_date")
        split_index = max(1, int(len(sorted_features) * 0.8))
        train_df = sorted_features.iloc[:split_index].copy()
        test_df = sorted_features.iloc[split_index:].copy()
    if train_df.empty or test_df.empty:
        raise ValueError("Not enough rows for a time-based train/test split.")
    return train_df, test_df


def train_model(features: pd.DataFrame, test_days: int = 14) -> dict:
    """Train and evaluate a RandomForest demand model."""

    train_df, test_df = time_based_split(features, test_days=test_days)
    model = Pipeline(
        steps=[
            ("scaler", StandardScaler()),
            (
                "regressor",
                RandomForestRegressor(
                    n_estimators=120,
                    min_samples_leaf=1,
                    random_state=42,
                    n_jobs=-1,
                ),
            ),
        ]
    )
    model.fit(train_df[FEATURE_COLUMNS], train_df[TARGET_COLUMN])
    predictions = model.predict(test_df[FEATURE_COLUMNS])
    metrics = evaluate_predictions(test_df[TARGET_COLUMN], predictions)
    return {
        "model": model,
        "metrics": metrics,
        "train_rows": len(train_df),
        "test_rows": len(test_df),
        "max_sales_date": str(pd.to_datetime(features["sales_date"]).max().date()),
    }


def parse_args() -> argparse.Namespace:
    """Parse training CLI arguments."""

    parser = argparse.ArgumentParser(description="Train demand forecasting model.")
    parser.add_argument("--test-days", type=int, default=14)
    return parser.parse_args()


def main() -> None:
    """Train and persist a demand forecasting model."""

    args = parse_args()
    configure_logging()
    with spark_session(app_name="train-demand-forecast") as spark:
        features = load_features_from_silver(spark)

    trained = train_model(features, test_days=args.test_days)
    version = model_version()
    bundle = {
        "model": trained["model"],
        "feature_columns": FEATURE_COLUMNS,
        "target_column": TARGET_COLUMN,
        "metrics": trained["metrics"],
        "trained_at": datetime.now(UTC).isoformat(),
        "max_sales_date": trained["max_sales_date"],
    }
    model_path = save_model_bundle(bundle, version=version)
    metrics_path = save_json_artifact(
        {
            "version": version,
            "model_path": str(model_path),
            "train_rows": trained["train_rows"],
            "test_rows": trained["test_rows"],
            **trained["metrics"],
        },
        f"demand_forecast_metrics_{version}.json",
    )
    logger.info("Saved model to %s", model_path)
    logger.info("Saved metrics to %s", metrics_path)
    logger.info("Metrics: %s", trained["metrics"])


if __name__ == "__main__":
    main()
