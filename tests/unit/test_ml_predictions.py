"""Unit tests for demand forecast prediction feature generation."""

import pandas as pd

from ml.predict_demand import build_future_features


def test_build_future_features_creates_rows_for_each_product_and_day() -> None:
    """Future features should cover every product for every horizon day."""

    history = pd.DataFrame(
        {
            "sales_date": pd.date_range("2026-07-01", periods=3).tolist() * 2,
            "product_id": [1, 1, 1, 2, 2, 2],
            "category_id": [10, 10, 10, 20, 20, 20],
            "units_sold": [1, 2, 3, 2, 3, 4],
            "revenue": [10, 20, 30, 20, 30, 40],
        }
    )

    future = build_future_features(history, horizon_days=7)

    assert len(future) == 14
    assert sorted(future["product_id"].unique().tolist()) == [1, 2]
    assert future["lag_1"].notna().all()
