"""Gold-layer business aggregations built from Silver tables."""

from __future__ import annotations

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

REVENUE_ORDER_STATUSES = ["delivered", "shipped", "processing"]


def revenue_orders(orders_df: DataFrame) -> DataFrame:
    """Return orders that should contribute to business revenue metrics."""

    return orders_df.filter(F.col("order_status").isin(REVENUE_ORDER_STATUSES))


def build_daily_sales_summary(
    orders_df: DataFrame,
    order_items_df: DataFrame,
) -> DataFrame:
    """Build daily order, revenue, item, AOV, and customer metrics."""

    orders = revenue_orders(orders_df)
    item_totals = order_items_df.groupBy("order_id").agg(
        F.sum("quantity").alias("items_sold")
    )
    daily = (
        orders.join(item_totals, on="order_id", how="left")
        .groupBy(F.col("order_date").alias("sales_date"))
        .agg(
            F.countDistinct("order_id").alias("total_orders"),
            F.round(F.sum("order_total"), 2).alias("total_revenue"),
            F.coalesce(F.sum("items_sold"), F.lit(0)).cast("long").alias("total_items_sold"),
            F.round(F.avg("order_total"), 2).alias("average_order_value"),
            F.countDistinct("customer_id").alias("unique_customers"),
        )
        .orderBy("sales_date")
    )
    return daily


def build_product_performance(
    orders_df: DataFrame,
    order_items_df: DataFrame,
    products_df: DataFrame,
    categories_df: DataFrame,
) -> DataFrame:
    """Build product-level sales and revenue performance."""

    orders = revenue_orders(orders_df).select("order_id")
    product_dim = products_df.join(categories_df, on="category_id", how="left").select(
        "product_id", "product_name", "category_name", "rating"
    )
    return (
        order_items_df.join(orders, on="order_id", how="inner")
        .join(product_dim, on="product_id", how="left")
        .groupBy("product_id", "product_name", "category_name")
        .agg(
            F.sum("quantity").cast("long").alias("units_sold"),
            F.round(F.sum(F.col("quantity") * F.col("unit_price")), 2).alias("gross_revenue"),
            F.round(F.sum("discount"), 2).alias("discount_amount"),
            F.round(F.sum("item_total"), 2).alias("net_revenue"),
            F.round(F.avg("rating"), 2).alias("average_rating"),
        )
        .orderBy(F.col("net_revenue").desc(), F.col("units_sold").desc())
    )


def build_category_performance(
    orders_df: DataFrame,
    order_items_df: DataFrame,
    products_df: DataFrame,
    categories_df: DataFrame,
) -> DataFrame:
    """Build category-level sales, units, revenue, and AOV metrics."""

    orders = revenue_orders(orders_df).select("order_id")
    product_category = products_df.select("product_id", "category_id").join(
        categories_df.select("category_id", "category_name"), on="category_id", how="left"
    )
    return (
        order_items_df.join(orders, on="order_id", how="inner")
        .join(product_category, on="product_id", how="left")
        .groupBy("category_id", "category_name")
        .agg(
            F.countDistinct("order_id").alias("total_orders"),
            F.sum("quantity").cast("long").alias("units_sold"),
            F.round(F.sum("item_total"), 2).alias("revenue"),
        )
        .withColumn(
            "average_order_value",
            F.round(F.col("revenue") / F.col("total_orders"), 2),
        )
        .orderBy(F.col("revenue").desc())
    )


def build_customer_360(
    customers_df: DataFrame,
    orders_df: DataFrame,
) -> DataFrame:
    """Build customer lifetime, recency, and segment metrics."""

    orders = revenue_orders(orders_df)
    metrics = orders.groupBy("customer_id").agg(
        F.countDistinct("order_id").alias("total_orders"),
        F.round(F.sum("order_total"), 2).alias("lifetime_value"),
        F.round(F.avg("order_total"), 2).alias("average_order_value"),
        F.max("order_date").alias("last_order_date"),
    )
    return (
        customers_df.select("customer_id", "customer_name", "customer_segment")
        .join(metrics, on="customer_id", how="left")
        .fillna(
            {
                "total_orders": 0,
                "lifetime_value": 0.0,
                "average_order_value": 0.0,
            }
        )
        .withColumn(
            "days_since_last_order",
            F.when(
                F.col("last_order_date").isNotNull(),
                F.datediff(F.current_date(), F.col("last_order_date")),
            ),
        )
        .select(
            "customer_id",
            "customer_name",
            "total_orders",
            "lifetime_value",
            "average_order_value",
            "last_order_date",
            "days_since_last_order",
            "customer_segment",
        )
    )


def build_inventory_health(
    inventory_df: DataFrame,
    products_df: DataFrame,
    orders_df: DataFrame,
    order_items_df: DataFrame,
) -> DataFrame:
    """Build inventory status and estimated stock coverage metrics."""

    recent_orders = revenue_orders(orders_df).filter(
        F.col("order_date") >= F.date_sub(F.current_date(), 30)
    )
    recent_daily_units = (
        order_items_df.join(recent_orders.select("order_id", "order_date"), on="order_id")
        .groupBy("product_id", "order_date")
        .agg(F.sum("quantity").alias("daily_units_sold"))
    )
    demand_velocity = recent_daily_units.groupBy("product_id").agg(
        F.avg("daily_units_sold").alias("avg_daily_units_sold_30d")
    )
    product_dim = products_df.select("product_id", "product_name")
    return (
        inventory_df.join(product_dim, on="product_id", how="left")
        .join(demand_velocity, on="product_id", how="left")
        .withColumn(
            "inventory_status",
            F.when(F.col("stock_quantity") <= 0, F.lit("out_of_stock"))
            .when(F.col("stock_quantity") <= F.col("reorder_level"), F.lit("low_stock"))
            .otherwise(F.lit("healthy")),
        )
        .withColumn(
            "estimated_days_remaining",
            F.when(
                F.col("avg_daily_units_sold_30d") > 0,
                F.round(F.col("stock_quantity") / F.col("avg_daily_units_sold_30d"), 1),
            ),
        )
        .select(
            "product_id",
            "product_name",
            "stock_quantity",
            "reorder_level",
            "inventory_status",
            "estimated_days_remaining",
        )
        .orderBy(
            F.when(F.col("inventory_status") == "out_of_stock", 0)
            .when(F.col("inventory_status") == "low_stock", 1)
            .otherwise(2),
            F.col("estimated_days_remaining").asc_nulls_last(),
        )
    )
