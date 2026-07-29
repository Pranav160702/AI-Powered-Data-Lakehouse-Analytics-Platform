"""Demand forecasting dashboard page."""

from __future__ import annotations

import sys
from pathlib import Path

import pandas as pd
import streamlit as st

PROJECT_ROOT = Path(__file__).resolve().parents[2]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from config.settings import get_settings
from dashboard.components.charts import line_chart


def _predictions_path() -> Path:
    settings = get_settings()
    return settings.resolve_path(settings.model_dir) / "demand_forecast_predictions_latest.csv"


def render(_data) -> None:
    """Render demand forecasting predictions."""

    path = _predictions_path()
    if not path.exists():
        st.info("No forecasts available yet. Run `python ml/train_demand_forecast.py` and `python ml/predict_demand.py`.")
        return

    predictions = pd.read_csv(path, parse_dates=["sales_date"])
    if predictions.empty:
        st.info("Forecast file exists but contains no rows.")
        return

    product_ids = sorted(predictions["product_id"].unique().tolist())
    selected_product = st.selectbox("Product", product_ids)
    filtered = predictions[predictions["product_id"] == selected_product].sort_values("sales_date")
    line_chart(
        filtered,
        "sales_date",
        "predicted_units_sold",
        f"Seven-day demand forecast for product {selected_product}",
    )
    st.dataframe(filtered, use_container_width=True, hide_index=True)
