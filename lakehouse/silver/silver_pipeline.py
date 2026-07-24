"""Run Bronze-to-Silver cleaning for batch lakehouse tables."""

from __future__ import annotations

import argparse
import logging
import sys
from dataclasses import dataclass
from pathlib import Path

from pyspark.sql import DataFrame, SparkSession

PROJECT_ROOT = Path(__file__).resolve().parents[2]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from config.logging_config import configure_logging
from config.settings import get_settings
from config.spark_config import spark_session
from ingestion.schemas import SOURCE_DEFINITIONS
from lakehouse.silver.transformations import (
    clean_categories,
    clean_customers,
    clean_inventory,
    clean_order_items,
    clean_orders,
    clean_payments,
    clean_products,
)

logger = logging.getLogger(__name__)

PROCESSING_ORDER = [
    "categories",
    "customers",
    "products",
    "orders",
    "order_items",
    "payments",
    "inventory",
]

TABLE_DEPENDENCIES = {
    "categories": [],
    "customers": [],
    "products": ["categories"],
    "orders": ["customers"],
    "order_items": ["categories", "products", "customers", "orders"],
    "payments": ["customers", "orders"],
    "inventory": ["categories", "products"],
}


@dataclass(frozen=True)
class SilverTransformResult:
    """Summary of one Silver transformation run."""

    table_name: str
    bronze_path: Path
    silver_path: Path
    quarantine_path: Path
    records_read: int
    records_written: int
    invalid_records: int


def read_bronze_table(spark: SparkSession, bronze_root: Path, table_name: str) -> DataFrame:
    """Read a Bronze Delta table."""

    path = bronze_root / table_name
    if not path.exists():
        raise FileNotFoundError(f"Bronze table is missing: {path}")
    logger.info("Reading Bronze table %s from %s", table_name, path)
    return spark.read.format("delta").load(str(path))


def write_silver_outputs(
    valid_df: DataFrame,
    invalid_df: DataFrame,
    silver_path: Path,
    quarantine_path: Path,
) -> tuple[int, int]:
    """Write valid Silver rows and invalid quarantined rows."""

    records_written = valid_df.count()
    invalid_records = invalid_df.count()

    silver_path.mkdir(parents=True, exist_ok=True)
    valid_df.write.format("delta").mode("overwrite").option(
        "overwriteSchema", "true"
    ).save(str(silver_path))

    if invalid_records > 0:
        quarantine_path.mkdir(parents=True, exist_ok=True)
        invalid_df.write.format("delta").mode("overwrite").option(
            "overwriteSchema", "true"
        ).save(str(quarantine_path))
        logger.warning("Quarantined %s invalid records at %s", invalid_records, quarantine_path)

    return records_written, invalid_records


def transform_table(
    spark: SparkSession,
    table_name: str,
    bronze_root: Path,
    silver_root: Path,
    quarantine_root: Path,
    silver_cache: dict[str, DataFrame],
) -> SilverTransformResult:
    """Clean and validate a single Bronze table into Silver."""

    bronze_df = read_bronze_table(spark, bronze_root, table_name)
    records_read = bronze_df.count()

    if table_name == "categories":
        valid_df, invalid_df = clean_categories(bronze_df)
    elif table_name == "customers":
        valid_df, invalid_df = clean_customers(bronze_df)
    elif table_name == "products":
        valid_df, invalid_df = clean_products(bronze_df, silver_cache["categories"])
    elif table_name == "orders":
        valid_df, invalid_df = clean_orders(bronze_df, silver_cache["customers"])
    elif table_name == "order_items":
        valid_df, invalid_df = clean_order_items(
            bronze_df, silver_cache["orders"], silver_cache["products"]
        )
    elif table_name == "payments":
        valid_df, invalid_df = clean_payments(bronze_df, silver_cache["orders"])
    elif table_name == "inventory":
        valid_df, invalid_df = clean_inventory(bronze_df, silver_cache["products"])
    else:
        raise ValueError(f"Unsupported Silver table: {table_name}")

    silver_path = silver_root / table_name
    quarantine_path = quarantine_root / "silver" / table_name
    records_written, invalid_records = write_silver_outputs(
        valid_df, invalid_df, silver_path, quarantine_path
    )
    silver_cache[table_name] = spark.read.format("delta").load(str(silver_path))

    logger.info(
        "Silver transformation completed for %s: read=%s written=%s invalid=%s",
        table_name,
        records_read,
        records_written,
        invalid_records,
    )
    return SilverTransformResult(
        table_name=table_name,
        bronze_path=bronze_root / table_name,
        silver_path=silver_path,
        quarantine_path=quarantine_path,
        records_read=records_read,
        records_written=records_written,
        invalid_records=invalid_records,
    )


def transform_selected_tables(
    spark: SparkSession,
    table_names: list[str] | None = None,
) -> list[SilverTransformResult]:
    """Transform all selected Bronze tables into cleaned Silver Delta tables."""

    settings = get_settings()
    bronze_root = settings.resolve_path(settings.warehouse_dir) / "bronze"
    silver_root = settings.resolve_path(settings.warehouse_dir) / "silver"
    quarantine_root = settings.resolve_path(settings.warehouse_dir) / "quarantine"
    selected = table_names or PROCESSING_ORDER
    invalid_names = set(selected).difference(SOURCE_DEFINITIONS)
    if invalid_names:
        raise ValueError(f"Unknown table names: {', '.join(sorted(invalid_names))}")

    expanded = set(selected)
    for table_name in selected:
        expanded.update(TABLE_DEPENDENCIES[table_name])
    ordered_tables = [table for table in PROCESSING_ORDER if table in expanded]
    silver_cache: dict[str, DataFrame] = {}
    return [
        transform_table(
            spark=spark,
            table_name=table_name,
            bronze_root=bronze_root,
            silver_root=silver_root,
            quarantine_root=quarantine_root,
            silver_cache=silver_cache,
        )
        for table_name in ordered_tables
    ]


def parse_args(default_tables: list[str] | None = None) -> argparse.Namespace:
    """Parse command-line arguments for Silver transformation."""

    parser = argparse.ArgumentParser(description="Transform Bronze Delta tables to Silver.")
    parser.add_argument(
        "--tables",
        nargs="*",
        choices=PROCESSING_ORDER,
        default=default_tables,
        help="Optional list of tables to transform. Dependencies must be included.",
    )
    return parser.parse_args()


def main(default_tables: list[str] | None = None) -> None:
    """Run the Silver transformation pipeline from the command line."""

    args = parse_args(default_tables=default_tables)
    configure_logging()
    with spark_session(app_name="silver-layer-transformations") as spark:
        results = transform_selected_tables(spark=spark, table_names=args.tables)
    for result in results:
        logger.info("Result: %s", result)


if __name__ == "__main__":
    main()
