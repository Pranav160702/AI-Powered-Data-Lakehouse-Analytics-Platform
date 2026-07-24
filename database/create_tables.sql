CREATE TABLE IF NOT EXISTS daily_sales_summary (
    sales_date DATE PRIMARY KEY,
    total_orders BIGINT NOT NULL,
    total_revenue DOUBLE PRECISION NOT NULL,
    total_items_sold BIGINT NOT NULL,
    average_order_value DOUBLE PRECISION NOT NULL,
    unique_customers BIGINT NOT NULL
);

CREATE TABLE IF NOT EXISTS product_performance (
    product_id BIGINT PRIMARY KEY,
    product_name VARCHAR(255) NOT NULL,
    category_name VARCHAR(255),
    units_sold BIGINT NOT NULL,
    gross_revenue DOUBLE PRECISION NOT NULL,
    discount_amount DOUBLE PRECISION NOT NULL,
    net_revenue DOUBLE PRECISION NOT NULL,
    average_rating DOUBLE PRECISION
);

CREATE TABLE IF NOT EXISTS category_performance (
    category_id BIGINT PRIMARY KEY,
    category_name VARCHAR(255) NOT NULL,
    total_orders BIGINT NOT NULL,
    units_sold BIGINT NOT NULL,
    revenue DOUBLE PRECISION NOT NULL,
    average_order_value DOUBLE PRECISION NOT NULL
);

CREATE TABLE IF NOT EXISTS customer_360 (
    customer_id BIGINT PRIMARY KEY,
    customer_name VARCHAR(255) NOT NULL,
    total_orders BIGINT NOT NULL,
    lifetime_value DOUBLE PRECISION NOT NULL,
    average_order_value DOUBLE PRECISION NOT NULL,
    last_order_date DATE,
    days_since_last_order BIGINT,
    customer_segment VARCHAR(100) NOT NULL
);

CREATE TABLE IF NOT EXISTS inventory_health (
    product_id BIGINT PRIMARY KEY,
    product_name VARCHAR(255) NOT NULL,
    stock_quantity BIGINT NOT NULL,
    reorder_level BIGINT NOT NULL,
    inventory_status VARCHAR(50) NOT NULL,
    estimated_days_remaining DOUBLE PRECISION
);

CREATE TABLE IF NOT EXISTS realtime_metrics (
    window_start TIMESTAMP PRIMARY KEY,
    window_end TIMESTAMP NOT NULL,
    revenue_per_window DOUBLE PRECISION NOT NULL,
    orders_per_window BIGINT NOT NULL,
    active_users BIGINT NOT NULL,
    product_views BIGINT NOT NULL,
    payment_failures BIGINT NOT NULL,
    add_to_cart_events BIGINT NOT NULL,
    checkout_events BIGINT NOT NULL,
    payment_failure_rate DOUBLE PRECISION NOT NULL,
    cart_abandonment_rate DOUBLE PRECISION NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_daily_sales_summary_sales_date
    ON daily_sales_summary (sales_date);

CREATE INDEX IF NOT EXISTS idx_product_performance_net_revenue
    ON product_performance (net_revenue DESC);

CREATE INDEX IF NOT EXISTS idx_category_performance_revenue
    ON category_performance (revenue DESC);

CREATE INDEX IF NOT EXISTS idx_customer_360_lifetime_value
    ON customer_360 (lifetime_value DESC);

CREATE INDEX IF NOT EXISTS idx_customer_360_segment
    ON customer_360 (customer_segment);

CREATE INDEX IF NOT EXISTS idx_inventory_health_status
    ON inventory_health (inventory_status);

CREATE INDEX IF NOT EXISTS idx_realtime_metrics_window_start
    ON realtime_metrics (window_start DESC);
