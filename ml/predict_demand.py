"""Generate seven-day demand forecasts from the latest trained model."""

from __future__ import annotations

import argparse
import logging
import sys
from pathlib import Path

import pandas as pd

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from config.logging_config import configure_logging
from config.spark_config import spark_session
from ml.feature_engineering import FEATURE_COLUMNS, load_features_from_silver
from ml.model_registry import load_latest_model_bundle, model_dir, model_version

logger = logging.getLogger(__name__)


def build_future_features(history: pd.DataFrame, horizon_days: int) -> pd.DataFrame:
    """Create recursive future feature rows for each product."""

    history = history.copy()
    history["sales_date"] = pd.to_datetime(history["sales_date"])
    latest_date = history["sales_date"].max()
    products = (
        history.sort_values("sales_date")
        .groupby("product_id", as_index=False)
        .tail(1)[["product_id", "category_id"]]
    )
    future_rows = []
    working = history[["sales_date", "product_id", "category_id", "units_sold", "revenue"]].copy()

    for day_offset in range(1, horizon_days + 1):
        forecast_date = latest_date + pd.Timedelta(days=day_offset)
        for row in products.itertuples(index=False):
            product_history = working[working["product_id"] == row.product_id].sort_values("sales_date")
            units = product_history["units_sold"]
            future_rows.append(
                {
                    "sales_date": forecast_date,
                    "product_id": int(row.product_id),
                    "category_id": int(row.category_id),
                    "units_sold": 0.0,
                    "revenue": float(product_history["revenue"].tail(7).mean() or 0.0),
                    "lag_1": float(units.iloc[-1]) if len(units) >= 1 else 0.0,
                    "lag_7": float(units.iloc[-7]) if len(units) >= 7 else float(units.mean() or 0.0),
                    "rolling_mean_7": float(units.tail(7).mean() or 0.0),
                    "rolling_mean_30": float(units.tail(30).mean() or 0.0),
                }
            )
        new_rows = pd.DataFrame(future_rows[-len(products) :])
        working = pd.concat(
            [
                working,
                new_rows[["sales_date", "product_id", "category_id", "units_sold", "revenue"]],
            ],
            ignore_index=True,
        )

    future = pd.DataFrame(future_rows)
    future["day_of_week"] = pd.to_datetime(future["sales_date"]).dt.dayofweek
    future["month"] = pd.to_datetime(future["sales_date"]).dt.month
    future["week_of_year"] = pd.to_datetime(future["sales_date"]).dt.isocalendar().week.astype(int)
    return future


def generate_predictions(history: pd.DataFrame, horizon_days: int = 7) -> pd.DataFrame:
    """Generate demand predictions for the configured horizon."""

    bundle = load_latest_model_bundle()
    model = bundle["model"]
    future = build_future_features(history, horizon_days=horizon_days)
    future["predicted_units_sold"] = model.predict(future[FEATURE_COLUMNS]).clip(min=0)
    return future[
        [
            "sales_date",
            "product_id",
            "category_id",
            "predicted_units_sold",
            "lag_1",
            "rolling_mean_7",
            "rolling_mean_30",
        ]
    ].sort_values(["sales_date", "product_id"])


def save_predictions(predictions: pd.DataFrame, version: str | None = None) -> Path:
    """Save predictions to the model artifact directory."""

    resolved_version = version or model_version()
    path = model_dir() / f"demand_forecast_predictions_{resolved_version}.csv"
    predictions.to_csv(path, index=False)
    latest = model_dir() / "demand_forecast_predictions_latest.csv"
    predictions.to_csv(latest, index=False)
    return path


def parse_args() -> argparse.Namespace:
    """Parse prediction CLI arguments."""

    parser = argparse.ArgumentParser(description="Generate demand forecasts.")
    parser.add_argument("--horizon-days", type=int, default=7)
    return parser.parse_args()


def main() -> None:
    """Generate and persist demand forecasts."""

    args = parse_args()
    configure_logging()
    with spark_session(app_name="predict-demand") as spark:
        history = load_features_from_silver(spark)
    predictions = generate_predictions(history, horizon_days=args.horizon_days)
    path = save_predictions(predictions)
    logger.info("Saved %s forecast rows to %s", len(predictions), path)


if __name__ == "__main__":
    main()
