"""Run Silver-to-Gold business aggregations."""

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
from lakehouse.gold.transformations import (
    build_category_performance,
    build_customer_360,
    build_daily_sales_summary,
    build_inventory_health,
    build_product_performance,
)

logger = logging.getLogger(__name__)

GOLD_TABLES = [
    "daily_sales_summary",
    "product_performance",
    "category_performance",
    "customer_360",
    "inventory_health",
]

REQUIRED_SILVER_TABLES = [
    "categories",
    "customers",
    "products",
    "orders",
    "order_items",
    "inventory",
]


@dataclass(frozen=True)
class GoldBuildResult:
    """Summary of one Gold table build."""

    table_name: str
    gold_path: Path
    records_written: int


def read_silver_tables(spark: SparkSession, silver_root: Path) -> dict[str, DataFrame]:
    """Read all Silver tables needed for Gold aggregation."""

    tables = {}
    for table_name in REQUIRED_SILVER_TABLES:
        path = silver_root / table_name
        if not path.exists():
            raise FileNotFoundError(f"Silver table is missing: {path}")
        logger.info("Reading Silver table %s from %s", table_name, path)
        tables[table_name] = spark.read.format("delta").load(str(path))
    return tables


def build_gold_dataframe(table_name: str, silver: dict[str, DataFrame]) -> DataFrame:
    """Build a selected Gold DataFrame from Silver inputs."""

    if table_name == "daily_sales_summary":
        return build_daily_sales_summary(silver["orders"], silver["order_items"])
    if table_name == "product_performance":
        return build_product_performance(
            silver["orders"],
            silver["order_items"],
            silver["products"],
            silver["categories"],
        )
    if table_name == "category_performance":
        return build_category_performance(
            silver["orders"],
            silver["order_items"],
            silver["products"],
            silver["categories"],
        )
    if table_name == "customer_360":
        return build_customer_360(silver["customers"], silver["orders"])
    if table_name == "inventory_health":
        return build_inventory_health(
            silver["inventory"],
            silver["products"],
            silver["orders"],
            silver["order_items"],
        )
    raise ValueError(f"Unsupported Gold table: {table_name}")


def write_gold_table(df: DataFrame, gold_root: Path, table_name: str) -> GoldBuildResult:
    """Write one Gold table as a Delta snapshot."""

    gold_path = gold_root / table_name
    records_written = df.count()
    gold_path.mkdir(parents=True, exist_ok=True)
    df.write.format("delta").mode("overwrite").option("overwriteSchema", "true").save(
        str(gold_path)
    )
    logger.info(
        "Gold table built for %s: written=%s path=%s",
        table_name,
        records_written,
        gold_path,
    )
    return GoldBuildResult(
        table_name=table_name,
        gold_path=gold_path,
        records_written=records_written,
    )


def build_selected_gold_tables(
    spark: SparkSession,
    table_names: list[str] | None = None,
) -> list[GoldBuildResult]:
    """Build all selected Gold tables."""

    settings = get_settings()
    silver_root = settings.resolve_path(settings.warehouse_dir) / "silver"
    gold_root = settings.resolve_path(settings.warehouse_dir) / "gold"
    selected = table_names or GOLD_TABLES
    invalid_names = set(selected).difference(GOLD_TABLES)
    if invalid_names:
        raise ValueError(f"Unknown Gold table names: {', '.join(sorted(invalid_names))}")

    silver = read_silver_tables(spark, silver_root)
    results = []
    for table_name in selected:
        gold_df = build_gold_dataframe(table_name, silver)
        results.append(write_gold_table(gold_df, gold_root, table_name))
    return results


def parse_args(default_tables: list[str] | None = None) -> argparse.Namespace:
    """Parse command-line arguments for Gold aggregation."""

    parser = argparse.ArgumentParser(description="Build Gold business Delta tables.")
    parser.add_argument(
        "--tables",
        nargs="*",
        choices=GOLD_TABLES,
        default=default_tables,
        help="Optional list of Gold tables to build. Defaults to all.",
    )
    return parser.parse_args()


def main(default_tables: list[str] | None = None) -> None:
    """Run the Gold aggregation pipeline from the command line."""

    args = parse_args(default_tables=default_tables)
    configure_logging()
    with spark_session(app_name="gold-layer-aggregations") as spark:
        results = build_selected_gold_tables(spark=spark, table_names=args.tables)
    for result in results:
        logger.info("Result: %s", result)


if __name__ == "__main__":
    main()
