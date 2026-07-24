"""Integration tests for real-time metric aggregation logic."""

from datetime import datetime

from config.spark_config import spark_session
from streaming.realtime_aggregations import realtime_metrics


def test_realtime_metrics_aggregates_core_event_counts() -> None:
    """Real-time metrics should aggregate purchases, views, users, and failures."""

    with spark_session(app_name="realtime-metrics-test") as spark:
        events = spark.createDataFrame(
            [
                ("e1", 1, "purchase_completed", 10.0, 2, datetime(2026, 7, 23, 10, 0)),
                ("e2", 2, "product_view", 9.0, 1, datetime(2026, 7, 23, 10, 1)),
                ("e3", 2, "payment_failed", 9.0, 1, datetime(2026, 7, 23, 10, 2)),
                ("e4", 1, "add_to_cart", 10.0, 1, datetime(2026, 7, 23, 10, 3)),
            ],
            ["event_id", "customer_id", "event_type", "price", "quantity", "event_timestamp"],
        )

        rows = [row.asDict() for row in realtime_metrics(events).collect()]

    assert rows
    assert max(row["revenue_per_window"] for row in rows) == 20.0
    assert max(row["orders_per_window"] for row in rows) == 1
    assert max(row["active_users"] for row in rows) == 2
    assert max(row["product_views"] for row in rows) == 1
    assert max(row["payment_failures"] for row in rows) == 1
