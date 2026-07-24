"""Batch CSV ingestion into the Bronze Delta layer."""

from __future__ import annotations

import argparse
import logging
import sys
from dataclasses import dataclass
from pathlib import Path
from uuid import uuid4

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from config.logging_config import configure_logging
from config.settings import get_settings
from config.spark_config import spark_session
from ingestion.schemas import (
    CORRUPT_RECORD_COLUMN,
    SOURCE_DEFINITIONS,
    SourceDefinition,
    get_source_definition,
)

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class BronzeIngestionResult:
    """Summary of one Bronze ingestion run."""

    table_name: str
    source_path: Path
    bronze_path: Path
    quarantine_path: Path
    batch_id: str
    records_read: int
    records_written: int
    malformed_records: int


def read_source_csv(spark: SparkSession, source: SourceDefinition, path: Path) -> DataFrame:
    """Read a CSV source using its explicit raw schema."""

    if not path.exists():
        raise FileNotFoundError(f"Required source file is missing: {path}")

    logger.info("Reading source file: %s", path)
    return (
        spark.read.option("header", "true")
        .option("mode", "PERMISSIVE")
        .option("columnNameOfCorruptRecord", CORRUPT_RECORD_COLUMN)
        .schema(source.schema)
        .csv(str(path))
    )


def add_bronze_metadata(
    df: DataFrame,
    source: SourceDefinition,
    source_path: Path,
    batch_id: str,
) -> DataFrame:
    """Add standard Bronze metadata without altering source business columns."""

    hash_columns = [
        F.coalesce(F.col(column).cast("string"), F.lit("")) for column in source.columns
    ]
    return (
        df.withColumn("ingestion_timestamp", F.current_timestamp())
        .withColumn("source_system", F.lit(source.source_system))
        .withColumn("source_file", F.lit(str(source_path)))
        .withColumn("batch_id", F.lit(batch_id))
        .withColumn("record_hash", F.sha2(F.concat_ws("||", *hash_columns), 256))
    )


def split_valid_and_malformed(df: DataFrame) -> tuple[DataFrame, DataFrame]:
    """Split parsed source records from malformed records captured by Spark CSV."""

    malformed = df.filter(F.col(CORRUPT_RECORD_COLUMN).isNotNull())
    valid = df.filter(F.col(CORRUPT_RECORD_COLUMN).isNull()).drop(CORRUPT_RECORD_COLUMN)
    return valid, malformed


def ingest_source_to_bronze(
    spark: SparkSession,
    source: SourceDefinition,
    raw_data_dir: Path,
    bronze_root: Path,
    quarantine_root: Path,
    batch_id: str | None = None,
) -> BronzeIngestionResult:
    """Ingest one configured CSV source into Bronze Delta and quarantine malformed rows."""

    resolved_batch_id = batch_id or str(uuid4())
    source_path = source.source_path(raw_data_dir)
    bronze_path = bronze_root / source.table_name
    quarantine_path = quarantine_root / "bronze" / source.table_name / resolved_batch_id

    raw_df = read_source_csv(spark, source, source_path)
    records_read = raw_df.count()
    with_metadata = add_bronze_metadata(raw_df, source, source_path, resolved_batch_id)
    valid_df, malformed_df = split_valid_and_malformed(with_metadata)

    records_written = valid_df.count()
    malformed_records = malformed_df.count()

    bronze_path.mkdir(parents=True, exist_ok=True)
    valid_df.write.format("delta").mode("append").save(str(bronze_path))

    if malformed_records > 0:
        quarantine_path.mkdir(parents=True, exist_ok=True)
        malformed_df.write.format("delta").mode("append").save(str(quarantine_path))
        logger.warning(
            "Quarantined %s malformed records for %s at %s",
            malformed_records,
            source.table_name,
            quarantine_path,
        )

    logger.info(
        "Bronze ingestion completed for %s: read=%s written=%s malformed=%s batch_id=%s",
        source.table_name,
        records_read,
        records_written,
        malformed_records,
        resolved_batch_id,
    )
    return BronzeIngestionResult(
        table_name=source.table_name,
        source_path=source_path,
        bronze_path=bronze_path,
        quarantine_path=quarantine_path,
        batch_id=resolved_batch_id,
        records_read=records_read,
        records_written=records_written,
        malformed_records=malformed_records,
    )


def ingest_all_sources(
    spark: SparkSession,
    table_names: list[str] | None = None,
    batch_id: str | None = None,
) -> list[BronzeIngestionResult]:
    """Ingest all configured source files, or a selected subset, into Bronze."""

    settings = get_settings()
    raw_data_dir = settings.resolve_path(settings.raw_data_dir)
    bronze_root = settings.resolve_path(settings.warehouse_dir) / "bronze"
    quarantine_root = settings.resolve_path(settings.warehouse_dir) / "quarantine"
    selected_names = table_names or list(SOURCE_DEFINITIONS)
    resolved_batch_id = batch_id or str(uuid4())

    return [
        ingest_source_to_bronze(
            spark=spark,
            source=get_source_definition(table_name),
            raw_data_dir=raw_data_dir,
            bronze_root=bronze_root,
            quarantine_root=quarantine_root,
            batch_id=resolved_batch_id,
        )
        for table_name in selected_names
    ]


def parse_args(default_tables: list[str] | None = None) -> argparse.Namespace:
    """Parse command-line arguments for Bronze ingestion."""

    parser = argparse.ArgumentParser(description="Ingest batch CSV sources to Bronze Delta.")
    parser.add_argument(
        "--tables",
        nargs="*",
        choices=sorted(SOURCE_DEFINITIONS),
        default=default_tables,
        help="Optional list of table names to ingest. Defaults to every source.",
    )
    parser.add_argument("--batch-id", default=None, help="Optional reusable batch id.")
    return parser.parse_args()


def main(default_tables: list[str] | None = None) -> None:
    """Run Bronze ingestion from the command line."""

    args = parse_args(default_tables=default_tables)
    configure_logging()
    with spark_session(app_name="batch-bronze-ingestion") as spark:
        results = ingest_all_sources(
            spark=spark,
            table_names=args.tables,
            batch_id=args.batch_id,
        )
    for result in results:
        logger.info("Result: %s", result)


if __name__ == "__main__":
    main()
