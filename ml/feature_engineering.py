"""Feature engineering for product-level demand forecasting."""

from __future__ import annotations

import sys
from pathlib import Path

import pandas as pd
from pyspark.sql import functions as F

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from config.settings import get_settings

FEATURE_COLUMNS = [
    "product_id",
    "category_id",
    "day_of_week",
    "month",
    "week_of_year",
    "lag_1",
    "lag_7",
    "rolling_mean_7",
    "rolling_mean_30",
    "revenue",
]
TARGET_COLUMN = "units_sold"


def build_daily_product_sales_spark(spark):
    """Build daily product-level sales from Silver Delta tables."""

    settings = get_settings()
    silver_root = settings.resolve_path(settings.warehouse_dir) / "silver"
    orders = spark.read.format("delta").load(str(silver_root / "orders"))
    order_items = spark.read.format("delta").load(str(silver_root / "order_items"))
    products = spark.read.format("delta").load(str(silver_root / "products"))

    revenue_statuses = ["delivered", "shipped", "processing"]
    return (
        order_items.join(
            orders.filter(F.col("order_status").isin(revenue_statuses)).select(
                "order_id", F.col("order_date").alias("sales_date")
            ),
            on="order_id",
            how="inner",
        )
        .join(products.select("product_id", "category_id"), on="product_id", how="left")
        .groupBy("sales_date", "product_id", "category_id")
        .agg(
            F.sum("quantity").cast("double").alias("units_sold"),
            F.round(F.sum("item_total"), 2).alias("revenue"),
        )
        .orderBy("sales_date", "product_id")
    )


def add_time_series_features(df: pd.DataFrame) -> pd.DataFrame:
    """Add calendar, lag, and rolling demand features."""

    if df.empty:
        raise ValueError("Cannot engineer ML features from an empty sales dataset.")

    features = df.copy()
    features["sales_date"] = pd.to_datetime(features["sales_date"])
    features = features.sort_values(["product_id", "sales_date"]).reset_index(drop=True)
    features["day_of_week"] = features["sales_date"].dt.dayofweek
    features["month"] = features["sales_date"].dt.month
    features["week_of_year"] = features["sales_date"].dt.isocalendar().week.astype(int)

    grouped = features.groupby("product_id", group_keys=False)
    features["lag_1"] = grouped["units_sold"].shift(1)
    features["lag_7"] = grouped["units_sold"].shift(7)
    features["rolling_mean_7"] = grouped["units_sold"].transform(
        lambda series: series.shift(1).rolling(window=7, min_periods=1).mean()
    )
    features["rolling_mean_30"] = grouped["units_sold"].transform(
        lambda series: series.shift(1).rolling(window=30, min_periods=1).mean()
    )

    fallback = features.groupby("product_id")["units_sold"].transform("mean")
    for column in ["lag_1", "lag_7", "rolling_mean_7", "rolling_mean_30"]:
        features[column] = features[column].fillna(fallback).fillna(0.0)

    features["category_id"] = features["category_id"].fillna(0).astype(int)
    features["product_id"] = features["product_id"].astype(int)
    features["units_sold"] = features["units_sold"].astype(float)
    features["revenue"] = features["revenue"].fillna(0.0).astype(float)
    return features


def load_features_from_silver(spark) -> pd.DataFrame:
    """Load Silver data through Spark and return ML-ready pandas features."""

    sales_df = build_daily_product_sales_spark(spark)
    rows = [row.asDict() for row in sales_df.collect()]
    return add_time_series_features(pd.DataFrame(rows))
