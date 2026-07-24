"""Read-only SQL queries used by the Streamlit dashboard."""

OVERVIEW_KPIS = """
SELECT
    COALESCE(SUM(total_revenue), 0) AS total_revenue,
    COALESCE(SUM(total_orders), 0) AS total_orders,
    COALESCE(SUM(total_items_sold), 0) AS units_sold,
    COALESCE(AVG(average_order_value), 0) AS average_order_value,
    COALESCE(SUM(unique_customers), 0) AS unique_customers
FROM daily_sales_summary
"""

DAILY_SALES = """
SELECT
    sales_date,
    total_orders,
    total_revenue,
    total_items_sold,
    average_order_value,
    unique_customers
FROM daily_sales_summary
ORDER BY sales_date
"""

MONTHLY_SALES = """
SELECT
    DATE_TRUNC('month', sales_date)::date AS sales_month,
    SUM(total_orders) AS total_orders,
    SUM(total_revenue) AS total_revenue,
    SUM(total_items_sold) AS total_items_sold,
    AVG(average_order_value) AS average_order_value
FROM daily_sales_summary
GROUP BY DATE_TRUNC('month', sales_date)
ORDER BY sales_month
"""

PRODUCT_PERFORMANCE = """
SELECT
    product_id,
    product_name,
    category_name,
    units_sold,
    gross_revenue,
    discount_amount,
    net_revenue,
    average_rating
FROM product_performance
ORDER BY net_revenue DESC
LIMIT %(limit)s
"""

CATEGORY_PERFORMANCE = """
SELECT
    category_id,
    category_name,
    total_orders,
    units_sold,
    revenue,
    average_order_value
FROM category_performance
ORDER BY revenue DESC
"""

CUSTOMER_360 = """
SELECT
    customer_id,
    customer_name,
    total_orders,
    lifetime_value,
    average_order_value,
    last_order_date,
    days_since_last_order,
    customer_segment
FROM customer_360
ORDER BY lifetime_value DESC
LIMIT %(limit)s
"""

CUSTOMER_SEGMENTS = """
SELECT
    customer_segment,
    COUNT(*) AS customer_count,
    AVG(average_order_value) AS average_order_value,
    SUM(lifetime_value) AS lifetime_value
FROM customer_360
GROUP BY customer_segment
ORDER BY lifetime_value DESC
"""

INVENTORY_HEALTH = """
SELECT
    product_id,
    product_name,
    stock_quantity,
    reorder_level,
    inventory_status,
    estimated_days_remaining
FROM inventory_health
ORDER BY
    CASE inventory_status
        WHEN 'out_of_stock' THEN 0
        WHEN 'low_stock' THEN 1
        ELSE 2
    END,
    estimated_days_remaining NULLS LAST
LIMIT %(limit)s
"""

INVENTORY_STATUS = """
SELECT
    inventory_status,
    COUNT(*) AS product_count
FROM inventory_health
GROUP BY inventory_status
ORDER BY product_count DESC
"""

REALTIME_METRICS = """
SELECT
    window_start,
    window_end,
    revenue_per_window,
    orders_per_window,
    active_users,
    product_views,
    payment_failures,
    add_to_cart_events,
    checkout_events,
    payment_failure_rate,
    cart_abandonment_rate
FROM realtime_metrics
ORDER BY window_start DESC
LIMIT %(limit)s
"""
