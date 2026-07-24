"""Real-time Gold metrics from cleaned Kafka event streams."""

from __future__ import annotations

import argparse
import logging
import sys
from pathlib import Path

from pyspark.sql import functions as F

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from config.logging_config import configure_logging
from config.settings import get_settings
from config.spark_config import spark_session
from streaming.checkpoint_manager import checkpoint_path
from streaming.stream_processor import KAFKA_SPARK_PACKAGE, read_kafka_events, parse_event_json

logger = logging.getLogger(__name__)


def cleaned_events_for_metrics(spark):
    """Return parsed, valid, watermarked events for real-time aggregations."""

    settings = get_settings()
    return (
        parse_event_json(read_kafka_events(spark))
        .withColumn("event_timestamp", F.to_timestamp("event_timestamp"))
        .filter(F.col("event_id").isNotNull())
        .filter(F.col("event_timestamp").isNotNull())
        .withWatermark("event_timestamp", settings.streaming_watermark_delay)
        .dropDuplicates(["event_id"])
    )


def realtime_metrics(df):
    """Aggregate event-time windowed business metrics."""

    settings = get_settings()
    window_col = F.window(
        "event_timestamp",
        settings.streaming_window_duration,
        settings.streaming_slide_duration,
    )
    return (
        df.groupBy(window_col)
        .agg(
            F.sum(
                F.when(F.col("event_type") == "purchase_completed", F.col("price") * F.col("quantity"))
                .otherwise(0.0)
            ).alias("revenue_per_window"),
            F.sum(F.when(F.col("event_type") == "purchase_completed", 1).otherwise(0)).alias(
                "orders_per_window"
            ),
            F.countDistinct("customer_id").alias("active_users"),
            F.sum(F.when(F.col("event_type") == "product_view", 1).otherwise(0)).alias(
                "product_views"
            ),
            F.sum(F.when(F.col("event_type") == "payment_failed", 1).otherwise(0)).alias(
                "payment_failures"
            ),
            F.sum(F.when(F.col("event_type") == "add_to_cart", 1).otherwise(0)).alias(
                "add_to_cart_events"
            ),
            F.sum(F.when(F.col("event_type") == "checkout_started", 1).otherwise(0)).alias(
                "checkout_events"
            ),
        )
        .withColumn("window_start", F.col("window.start"))
        .withColumn("window_end", F.col("window.end"))
        .withColumn(
            "payment_failure_rate",
            F.when(
                F.col("orders_per_window") + F.col("payment_failures") > 0,
                F.round(
                    F.col("payment_failures")
                    / (F.col("orders_per_window") + F.col("payment_failures")),
                    4,
                ),
            ).otherwise(0.0),
        )
        .withColumn(
            "cart_abandonment_rate",
            F.when(
                F.col("add_to_cart_events") > 0,
                F.round(
                    1 - (F.col("orders_per_window") / F.col("add_to_cart_events")),
                    4,
                ),
            ).otherwise(0.0),
        )
        .drop("window")
    )


def start_realtime_metrics_stream(spark) -> None:
    """Start real-time metrics aggregation into Gold Delta."""

    settings = get_settings()
    gold_path = settings.resolve_path(settings.warehouse_dir) / "gold" / "realtime_metrics"
    query = (
        realtime_metrics(cleaned_events_for_metrics(spark))
        .writeStream.format("delta")
        .outputMode("update")
        .option("checkpointLocation", str(checkpoint_path("realtime_metrics")))
        .start(str(gold_path))
    )
    logger.info("Started real-time metrics stream: %s", gold_path)
    query.awaitTermination()


def parse_args() -> argparse.Namespace:
    """Parse CLI arguments for real-time aggregations."""

    return argparse.ArgumentParser(description="Run real-time Gold aggregations.").parse_args()


def main() -> None:
    """Run real-time aggregation stream."""

    parse_args()
    configure_logging()
    with spark_session(
        app_name="streaming-realtime-metrics",
        extra_packages=[KAFKA_SPARK_PACKAGE],
    ) as spark:
        start_realtime_metrics_stream(spark)


if __name__ == "__main__":
    main()
