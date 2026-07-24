"""Kafka-to-Delta streaming ingestion for e-commerce events."""

from __future__ import annotations

import argparse
import logging
import sys
from pathlib import Path

from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import (
    DoubleType,
    IntegerType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from config.kafka_config import event_topics
from config.logging_config import configure_logging
from config.settings import get_settings
from config.spark_config import spark_session
from streaming.checkpoint_manager import checkpoint_path

logger = logging.getLogger(__name__)

KAFKA_SPARK_PACKAGE = "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1"

EVENT_SCHEMA = StructType(
    [
        StructField("event_id", StringType(), True),
        StructField("customer_id", IntegerType(), True),
        StructField("session_id", StringType(), True),
        StructField("event_type", StringType(), True),
        StructField("product_id", IntegerType(), True),
        StructField("category_id", IntegerType(), True),
        StructField("quantity", IntegerType(), True),
        StructField("price", DoubleType(), True),
        StructField("city", StringType(), True),
        StructField("device_type", StringType(), True),
        StructField("event_timestamp", StringType(), True),
    ]
)


def read_kafka_events(spark) -> DataFrame:
    """Read raw Kafka records from configured event topics."""

    settings = get_settings()
    return (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", settings.kafka_bootstrap_servers)
        .option("subscribe", ",".join(event_topics()))
        .option("startingOffsets", "latest")
        .option("failOnDataLoss", "false")
        .load()
    )


def parse_event_json(kafka_df: DataFrame) -> DataFrame:
    """Parse Kafka JSON payloads using the explicit event schema."""

    raw = kafka_df.select(
        F.col("topic"),
        F.col("partition"),
        F.col("offset"),
        F.col("timestamp").alias("kafka_timestamp"),
        F.col("key").cast("string").alias("message_key"),
        F.col("value").cast("string").alias("raw_json"),
    )
    parsed = raw.withColumn("event", F.from_json("raw_json", EVENT_SCHEMA))
    return parsed.select("*", "event.*").drop("event")


def add_streaming_bronze_metadata(df: DataFrame) -> DataFrame:
    """Add ingestion metadata to raw streaming events."""

    return (
        df.withColumn("ingestion_timestamp", F.current_timestamp())
        .withColumn("source_system", F.lit("kafka"))
        .withColumn("source_file", F.col("topic"))
        .withColumn("batch_id", F.col("offset").cast("string"))
        .withColumn("record_hash", F.sha2(F.coalesce(F.col("raw_json"), F.lit("")), 256))
        .withColumn("is_malformed", F.col("event_id").isNull())
    )


def clean_streaming_events(df: DataFrame) -> DataFrame:
    """Apply Silver-level streaming event validation and standardization."""

    settings = get_settings()
    valid_event_types = [
        "product_view",
        "product_search",
        "add_to_cart",
        "remove_from_cart",
        "checkout_started",
        "purchase_completed",
        "payment_failed",
    ]
    return (
        df.filter(~F.col("is_malformed"))
        .withColumn("event_timestamp", F.to_timestamp("event_timestamp"))
        .withColumn("event_type", F.lower(F.trim(F.col("event_type"))))
        .withColumn("city", F.initcap(F.trim(F.col("city"))))
        .withColumn("device_type", F.lower(F.trim(F.col("device_type"))))
        .filter(F.col("event_id").isNotNull())
        .filter(F.col("customer_id").isNotNull())
        .filter(F.col("event_timestamp").isNotNull())
        .filter(F.col("event_type").isin(valid_event_types))
        .filter(F.col("price").isNull() | (F.col("price") >= 0))
        .filter(F.col("quantity").isNull() | (F.col("quantity") > 0))
        .withWatermark("event_timestamp", settings.streaming_watermark_delay)
        .dropDuplicates(["event_id"])
    )


def start_bronze_stream(spark) -> None:
    """Start the raw Kafka-to-Bronze Delta stream."""

    settings = get_settings()
    bronze_path = settings.resolve_path(settings.warehouse_dir) / "bronze" / "events"
    query = (
        add_streaming_bronze_metadata(parse_event_json(read_kafka_events(spark)))
        .writeStream.format("delta")
        .outputMode("append")
        .option("checkpointLocation", str(checkpoint_path("bronze_events")))
        .start(str(bronze_path))
    )
    logger.info("Started Bronze event stream: %s", bronze_path)
    query.awaitTermination()


def start_silver_stream(spark) -> None:
    """Start the Kafka-to-Silver cleaned event stream."""

    settings = get_settings()
    silver_path = settings.resolve_path(settings.warehouse_dir) / "silver" / "events"
    query = (
        clean_streaming_events(
            add_streaming_bronze_metadata(parse_event_json(read_kafka_events(spark)))
        )
        .writeStream.format("delta")
        .outputMode("append")
        .option("checkpointLocation", str(checkpoint_path("silver_events")))
        .start(str(silver_path))
    )
    logger.info("Started Silver event stream: %s", silver_path)
    query.awaitTermination()


def parse_args() -> argparse.Namespace:
    """Parse stream processor CLI arguments."""

    parser = argparse.ArgumentParser(description="Run Kafka event streaming processors.")
    parser.add_argument(
        "--target",
        choices=["bronze", "silver"],
        default="silver",
        help="Stream target to run.",
    )
    return parser.parse_args()


def main() -> None:
    """Run one streaming processor."""

    args = parse_args()
    configure_logging()
    with spark_session(
        app_name=f"streaming-{args.target}-events",
        extra_packages=[KAFKA_SPARK_PACKAGE],
    ) as spark:
        if args.target == "bronze":
            start_bronze_stream(spark)
        else:
            start_silver_stream(spark)


if __name__ == "__main__":
    main()
