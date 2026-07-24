"""Unit tests for demand forecasting feature engineering."""

import pandas as pd

from ml.feature_engineering import FEATURE_COLUMNS, add_time_series_features
from ml.train_demand_forecast import time_based_split


def test_add_time_series_features_creates_expected_columns() -> None:
    """Feature engineering should create calendar, lag, and rolling columns."""

    raw = pd.DataFrame(
        {
            "sales_date": pd.date_range("2026-07-01", periods=8),
            "product_id": [1] * 8,
            "category_id": [2] * 8,
            "units_sold": [1, 2, 3, 4, 5, 6, 7, 8],
            "revenue": [10.0] * 8,
        }
    )

    features = add_time_series_features(raw)

    assert set(FEATURE_COLUMNS).issubset(features.columns)
    assert features.loc[1, "lag_1"] == 1
    assert features.loc[7, "lag_7"] == 1
    assert features["rolling_mean_7"].notna().all()


def test_time_based_split_keeps_latest_rows_for_test() -> None:
    """Training split should respect date order."""

    features = add_time_series_features(
        pd.DataFrame(
            {
                "sales_date": pd.date_range("2026-07-01", periods=10),
                "product_id": [1] * 10,
                "category_id": [2] * 10,
                "units_sold": list(range(10)),
                "revenue": [10.0] * 10,
            }
        )
    )

    train_df, test_df = time_based_split(features, test_days=2)

    assert train_df["sales_date"].max() < test_df["sales_date"].max()
    assert len(train_df) > len(test_df)
