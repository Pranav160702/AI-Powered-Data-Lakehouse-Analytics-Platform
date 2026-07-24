"""Integration tests for Gold business aggregations."""

from datetime import date

from config.spark_config import spark_session
from lakehouse.gold.transformations import build_daily_sales_summary


def test_daily_sales_summary_excludes_cancelled_and_returned_orders() -> None:
    """Daily sales should include active revenue statuses only."""

    with spark_session(app_name="gold-daily-sales-test") as spark:
        orders_df = spark.createDataFrame(
            [
                (1, 101, date(2026, 7, 20), "delivered", 100.0),
                (2, 102, date(2026, 7, 20), "cancelled", 50.0),
                (3, 101, date(2026, 7, 20), "processing", 25.0),
                (4, 103, date(2026, 7, 21), "returned", 90.0),
            ],
            ["order_id", "customer_id", "order_date", "order_status", "order_total"],
        )
        order_items_df = spark.createDataFrame(
            [
                (1, 1, 2),
                (2, 2, 1),
                (3, 3, 4),
                (4, 4, 1),
            ],
            ["order_item_id", "order_id", "quantity"],
        )

        rows = {
            row["sales_date"]: row.asDict()
            for row in build_daily_sales_summary(orders_df, order_items_df).collect()
        }

    assert set(rows) == {date(2026, 7, 20)}
    assert rows[date(2026, 7, 20)]["total_orders"] == 2
    assert rows[date(2026, 7, 20)]["total_revenue"] == 125.0
    assert rows[date(2026, 7, 20)]["total_items_sold"] == 6
    assert rows[date(2026, 7, 20)]["average_order_value"] == 62.5
    assert rows[date(2026, 7, 20)]["unique_customers"] == 1
