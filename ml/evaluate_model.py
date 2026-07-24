"""Demand forecasting model evaluation helpers."""

from __future__ import annotations

import math

import pandas as pd
from sklearn.metrics import mean_absolute_error, mean_squared_error, r2_score


def evaluate_predictions(y_true: pd.Series, y_pred) -> dict[str, float]:
    """Return MAE, RMSE, and R-squared for predictions."""

    return {
        "mae": float(mean_absolute_error(y_true, y_pred)),
        "rmse": float(math.sqrt(mean_squared_error(y_true, y_pred))),
        "r2": float(r2_score(y_true, y_pred)) if len(y_true) > 1 else 0.0,
    }
