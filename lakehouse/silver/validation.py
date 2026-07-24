"""Reusable validation helpers for Silver transformations."""

from collections.abc import Sequence

from pyspark.sql import Column, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.window import Window


def normalize_name(column_name: str) -> Column:
    """Trim repeated whitespace and convert text to title case."""

    return F.initcap(F.regexp_replace(F.trim(F.col(column_name)), r"\s+", " "))


def is_blank(column_name: str) -> Column:
    """Return true when a string column is null or empty after trimming."""

    return F.col(column_name).isNull() | (F.trim(F.col(column_name)) == "")


def with_quality_flags(df: DataFrame) -> DataFrame:
    """Initialize a data-quality flag array."""

    return df.withColumn("data_quality_flags", F.array().cast("array<string>"))


def add_quality_flag(df: DataFrame, condition: Column, flag: str) -> DataFrame:
    """Append a named data-quality flag when a validation condition fails."""

    return df.withColumn(
        "data_quality_flags",
        F.when(
            condition,
            F.concat(F.col("data_quality_flags"), F.array(F.lit(flag))),
        ).otherwise(F.col("data_quality_flags")),
    )


def add_foreign_key_flag(
    df: DataFrame,
    ref_df: DataFrame,
    column_name: str,
    ref_column_name: str,
    flag: str,
) -> DataFrame:
    """Append a quality flag when a non-null foreign key does not exist."""

    marker_column = f"__{column_name}_exists"
    reference = ref_df.select(
        F.col(ref_column_name).alias(column_name), F.lit(True).alias(marker_column)
    ).dropDuplicates([column_name])
    checked = df.join(reference, on=column_name, how="left")
    return add_quality_flag(
        checked,
        F.col(column_name).isNotNull() & F.col(marker_column).isNull(),
        flag,
    ).drop(marker_column)


def deduplicate_by_key(df: DataFrame, key_columns: Sequence[str]) -> DataFrame:
    """Keep the newest Bronze record for each primary-key combination."""

    window = Window.partitionBy(*key_columns).orderBy(
        F.col("ingestion_timestamp").desc_nulls_last(),
        F.col("batch_id").desc_nulls_last(),
    )
    return (
        df.withColumn("__row_number", F.row_number().over(window))
        .filter(F.col("__row_number") == 1)
        .drop("__row_number")
    )


def split_valid_and_invalid(df: DataFrame) -> tuple[DataFrame, DataFrame]:
    """Split valid records from records with one or more quality flags."""

    invalid = df.filter(F.size(F.col("data_quality_flags")) > 0)
    valid = df.filter(F.size(F.col("data_quality_flags")) == 0)
    return valid, invalid
