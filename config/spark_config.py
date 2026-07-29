"""Centralized Spark session builder with Delta Lake support."""

from collections.abc import Mapping
from contextlib import contextmanager
from typing import Iterator

from pyspark.sql import SparkSession

from config.logging_config import get_logger
from config.settings import Settings, get_settings

logger = get_logger(__name__)

try:
    from delta import configure_spark_with_delta_pip
except ModuleNotFoundError:
    configure_spark_with_delta_pip = None


def build_spark_session(
    app_name: str | None = None,
    extra_configs: Mapping[str, str] | None = None,
    extra_packages: list[str] | None = None,
    settings: Settings | None = None,
) -> SparkSession:
    """Create a SparkSession configured for Delta Lake batch or streaming jobs."""

    resolved_settings = settings or get_settings()
    warehouse_dir = resolved_settings.resolve_path(resolved_settings.warehouse_dir)
    warehouse_dir.mkdir(parents=True, exist_ok=True)

    builder = SparkSession.builder.appName(app_name or resolved_settings.app_name).config(
        "spark.sql.shuffle.partitions",
        str(resolved_settings.spark_sql_shuffle_partitions),
    )

    if resolved_settings.environment.lower() == "databricks":
        builder = builder.config("spark.databricks.delta.schema.autoMerge.enabled", "true")
    else:
        builder = (
            builder.master(resolved_settings.spark_master_url)
            .config("spark.driver.memory", resolved_settings.spark_driver_memory)
            .config("spark.executor.memory", resolved_settings.spark_executor_memory)
            .config("spark.sql.warehouse.dir", str(warehouse_dir))
            .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
            .config(
                "spark.sql.catalog.spark_catalog",
                "org.apache.spark.sql.delta.catalog.DeltaCatalog",
            )
        )

    for key, value in (extra_configs or {}).items():
        builder = builder.config(key, value)

    if resolved_settings.environment.lower() == "databricks":
        spark = builder.getOrCreate()
    else:
        if configure_spark_with_delta_pip is None:
            raise RuntimeError(
                "delta-spark is required for local Spark runs. Install project requirements."
            )
        spark = configure_spark_with_delta_pip(
            builder, extra_packages=extra_packages
        ).getOrCreate()
    if resolved_settings.environment.lower() == "databricks":
        logger.info("Spark session started on Databricks serverless")
    else:
        spark.sparkContext.setLogLevel("WARN")
        logger.info(
            "Spark session started",
            extra={
                "app_name": spark.sparkContext.appName,
                "master": spark.sparkContext.master,
            },
        )
    return spark


def stop_spark_session(spark: SparkSession | None) -> None:
    """Stop a SparkSession if one is active."""

    if spark is not None:
        spark.stop()
        logger.info("Spark session stopped")


@contextmanager
def spark_session(
    app_name: str | None = None,
    extra_configs: Mapping[str, str] | None = None,
    extra_packages: list[str] | None = None,
) -> Iterator[SparkSession]:
    """Context manager that starts and reliably stops Spark."""

    spark = build_spark_session(
        app_name=app_name,
        extra_configs=extra_configs,
        extra_packages=extra_packages,
    )
    try:
        yield spark
    finally:
        stop_spark_session(spark)
