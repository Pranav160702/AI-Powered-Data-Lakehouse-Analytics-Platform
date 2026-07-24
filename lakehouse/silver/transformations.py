"""Silver-layer cleaning transformations for batch e-commerce tables."""

from __future__ import annotations

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from lakehouse.silver.validation import (
    add_foreign_key_flag,
    add_quality_flag,
    deduplicate_by_key,
    is_blank,
    normalize_name,
    split_valid_and_invalid,
    with_quality_flags,
)


def _with_silver_metadata(df: DataFrame) -> DataFrame:
    """Add standard Silver processing metadata."""

    return df.withColumn("silver_processed_timestamp", F.current_timestamp())


def _finalize(df: DataFrame, key_columns: list[str]) -> tuple[DataFrame, DataFrame]:
    """Split records by quality, deduplicate valid rows, and add Silver metadata."""

    valid_df, invalid_df = split_valid_and_invalid(df)
    valid_df = deduplicate_by_key(valid_df, key_columns)
    return _with_silver_metadata(valid_df), _with_silver_metadata(invalid_df)


def clean_customers(bronze_df: DataFrame) -> tuple[DataFrame, DataFrame]:
    """Clean, type, deduplicate, and validate customer records."""

    df = (
        bronze_df.withColumn("customer_id", F.col("customer_id").cast("long"))
        .withColumn("customer_name", normalize_name("customer_name"))
        .withColumn("email", F.lower(F.trim(F.col("email"))))
        .withColumn("phone", F.trim(F.col("phone")))
        .withColumn("city", normalize_name("city"))
        .withColumn("state", normalize_name("state"))
        .withColumn("country", normalize_name("country"))
        .withColumn("registration_date", F.to_date("registration_date"))
        .withColumn(
            "customer_segment",
            F.coalesce(F.lower(F.trim(F.col("customer_segment"))), F.lit("unknown")),
        )
    )
    df = with_quality_flags(df)
    df = add_quality_flag(df, F.col("customer_id").isNull(), "missing_customer_id")
    df = add_quality_flag(df, is_blank("customer_name"), "missing_customer_name")
    df = add_quality_flag(df, is_blank("email"), "missing_email")
    df = add_quality_flag(df, F.col("registration_date").isNull(), "invalid_registration_date")
    return _finalize(df, ["customer_id"])


def clean_categories(bronze_df: DataFrame) -> tuple[DataFrame, DataFrame]:
    """Clean, type, deduplicate, and validate category records."""

    df = (
        bronze_df.withColumn("category_id", F.col("category_id").cast("long"))
        .withColumn("category_name", normalize_name("category_name"))
        .withColumn("department", normalize_name("department"))
    )
    df = with_quality_flags(df)
    df = add_quality_flag(df, F.col("category_id").isNull(), "missing_category_id")
    df = add_quality_flag(df, is_blank("category_name"), "missing_category_name")
    df = add_quality_flag(df, is_blank("department"), "missing_department")
    return _finalize(df, ["category_id"])


def clean_products(
    bronze_df: DataFrame,
    categories_df: DataFrame,
) -> tuple[DataFrame, DataFrame]:
    """Clean, type, deduplicate, and validate product records."""

    df = (
        bronze_df.withColumn("product_id", F.col("product_id").cast("long"))
        .withColumn("product_name", normalize_name("product_name"))
        .withColumn("category_id", F.col("category_id").cast("long"))
        .withColumn("brand", normalize_name("brand"))
        .withColumn("price", F.col("price").cast("double"))
        .withColumn("cost_price", F.col("cost_price").cast("double"))
        .withColumn("rating", F.col("rating").cast("double"))
        .withColumn("created_at", F.to_date("created_at"))
    )
    df = with_quality_flags(df)
    df = add_quality_flag(df, F.col("product_id").isNull(), "missing_product_id")
    df = add_quality_flag(df, is_blank("product_name"), "missing_product_name")
    df = add_quality_flag(df, F.col("category_id").isNull(), "missing_category_id")
    df = add_quality_flag(df, F.col("price").isNull() | (F.col("price") <= 0), "invalid_price")
    df = add_quality_flag(
        df,
        F.col("cost_price").isNull() | (F.col("cost_price") < 0),
        "invalid_cost_price",
    )
    df = add_quality_flag(
        df,
        F.col("rating").isNull() | (F.col("rating") < 0) | (F.col("rating") > 5),
        "invalid_rating",
    )
    df = add_quality_flag(df, F.col("created_at").isNull(), "invalid_created_at")
    df = add_foreign_key_flag(
        df, categories_df, "category_id", "category_id", "missing_category_fk"
    )
    return _finalize(df, ["product_id"])


def clean_orders(
    bronze_df: DataFrame,
    customers_df: DataFrame,
) -> tuple[DataFrame, DataFrame]:
    """Clean, type, deduplicate, and validate order records."""

    normalized_status = F.lower(F.regexp_replace(F.trim(F.col("order_status")), r"\s+", "_"))
    df = (
        bronze_df.withColumn("order_id", F.col("order_id").cast("long"))
        .withColumn("customer_id", F.col("customer_id").cast("long"))
        .withColumn("order_date", F.to_date("order_date"))
        .withColumn("order_status", normalized_status)
        .withColumn("shipping_city", normalize_name("shipping_city"))
        .withColumn("shipping_state", normalize_name("shipping_state"))
        .withColumn("payment_method", F.lower(F.trim(F.col("payment_method"))))
        .withColumn("order_total", F.col("order_total").cast("double"))
    )
    valid_statuses = ["delivered", "shipped", "processing", "cancelled", "returned"]
    df = with_quality_flags(df)
    df = add_quality_flag(df, F.col("order_id").isNull(), "missing_order_id")
    df = add_quality_flag(df, F.col("customer_id").isNull(), "missing_customer_id")
    df = add_quality_flag(df, F.col("order_date").isNull(), "invalid_order_date")
    df = add_quality_flag(df, ~F.col("order_status").isin(valid_statuses), "invalid_order_status")
    df = add_quality_flag(
        df,
        F.col("order_total").isNull() | (F.col("order_total") < 0),
        "invalid_order_total",
    )
    df = add_foreign_key_flag(
        df, customers_df, "customer_id", "customer_id", "missing_customer_fk"
    )
    return _finalize(df, ["order_id"])


def clean_order_items(
    bronze_df: DataFrame,
    orders_df: DataFrame,
    products_df: DataFrame,
) -> tuple[DataFrame, DataFrame]:
    """Clean, type, deduplicate, and validate order-item records."""

    df = (
        bronze_df.withColumn("order_item_id", F.col("order_item_id").cast("long"))
        .withColumn("order_id", F.col("order_id").cast("long"))
        .withColumn("product_id", F.col("product_id").cast("long"))
        .withColumn("quantity", F.col("quantity").cast("long"))
        .withColumn("unit_price", F.col("unit_price").cast("double"))
        .withColumn("discount", F.col("discount").cast("double"))
        .withColumn("item_total", F.col("item_total").cast("double"))
    )
    df = with_quality_flags(df)
    df = add_quality_flag(df, F.col("order_item_id").isNull(), "missing_order_item_id")
    df = add_quality_flag(df, F.col("order_id").isNull(), "missing_order_id")
    df = add_quality_flag(df, F.col("product_id").isNull(), "missing_product_id")
    df = add_quality_flag(df, F.col("quantity").isNull() | (F.col("quantity") <= 0), "invalid_quantity")
    df = add_quality_flag(
        df,
        F.col("unit_price").isNull() | (F.col("unit_price") <= 0),
        "invalid_unit_price",
    )
    df = add_quality_flag(
        df,
        F.col("discount").isNull() | (F.col("discount") < 0),
        "invalid_discount",
    )
    df = add_quality_flag(
        df,
        F.col("item_total").isNull() | (F.col("item_total") < 0),
        "invalid_item_total",
    )
    df = add_foreign_key_flag(df, orders_df, "order_id", "order_id", "missing_order_fk")
    df = add_foreign_key_flag(
        df, products_df, "product_id", "product_id", "missing_product_fk"
    )
    return _finalize(df, ["order_item_id"])


def clean_payments(
    bronze_df: DataFrame,
    orders_df: DataFrame,
) -> tuple[DataFrame, DataFrame]:
    """Clean, type, deduplicate, and validate payment records."""

    df = (
        bronze_df.withColumn("payment_id", F.col("payment_id").cast("long"))
        .withColumn("order_id", F.col("order_id").cast("long"))
        .withColumn("payment_method", F.lower(F.trim(F.col("payment_method"))))
        .withColumn("payment_status", F.lower(F.trim(F.col("payment_status"))))
        .withColumn("payment_amount", F.col("payment_amount").cast("double"))
        .withColumn("payment_timestamp", F.to_timestamp("payment_timestamp"))
    )
    valid_statuses = ["paid", "failed", "refunded"]
    df = with_quality_flags(df)
    df = add_quality_flag(df, F.col("payment_id").isNull(), "missing_payment_id")
    df = add_quality_flag(df, F.col("order_id").isNull(), "missing_order_id")
    df = add_quality_flag(df, ~F.col("payment_status").isin(valid_statuses), "invalid_payment_status")
    df = add_quality_flag(
        df,
        F.col("payment_amount").isNull() | (F.col("payment_amount") < 0),
        "invalid_payment_amount",
    )
    df = add_quality_flag(
        df, F.col("payment_timestamp").isNull(), "invalid_payment_timestamp"
    )
    df = add_foreign_key_flag(df, orders_df, "order_id", "order_id", "missing_order_fk")
    order_totals = orders_df.select(
        "order_id", F.col("order_total").alias("__expected_order_total")
    ).dropDuplicates(["order_id"])
    df = df.join(order_totals, on="order_id", how="left")
    df = add_quality_flag(
        df,
        F.col("payment_status").isin("paid", "refunded")
        & F.col("__expected_order_total").isNotNull()
        & (F.abs(F.col("payment_amount") - F.col("__expected_order_total")) > 0.05),
        "payment_amount_mismatch",
    ).drop("__expected_order_total")
    return _finalize(df, ["payment_id"])


def clean_inventory(
    bronze_df: DataFrame,
    products_df: DataFrame,
) -> tuple[DataFrame, DataFrame]:
    """Clean, type, deduplicate, and validate inventory records."""

    df = (
        bronze_df.withColumn("product_id", F.col("product_id").cast("long"))
        .withColumn("warehouse_id", F.upper(F.trim(F.col("warehouse_id"))))
        .withColumn("stock_quantity", F.col("stock_quantity").cast("long"))
        .withColumn("reorder_level", F.col("reorder_level").cast("long"))
        .withColumn("last_updated", F.to_timestamp("last_updated"))
    )
    df = with_quality_flags(df)
    df = add_quality_flag(df, F.col("product_id").isNull(), "missing_product_id")
    df = add_quality_flag(df, is_blank("warehouse_id"), "missing_warehouse_id")
    df = add_quality_flag(
        df,
        F.col("stock_quantity").isNull() | (F.col("stock_quantity") < 0),
        "invalid_stock_quantity",
    )
    df = add_quality_flag(
        df,
        F.col("reorder_level").isNull() | (F.col("reorder_level") < 0),
        "invalid_reorder_level",
    )
    df = add_quality_flag(df, F.col("last_updated").isNull(), "invalid_last_updated")
    df = add_foreign_key_flag(
        df, products_df, "product_id", "product_id", "missing_product_fk"
    )
    return _finalize(df, ["product_id", "warehouse_id"])
