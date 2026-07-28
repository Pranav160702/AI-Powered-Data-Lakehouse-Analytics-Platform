# AI-Powered Data Lakehouse Analytics Platform - Detailed Project Explanation

## 1. Project Architecture

This project is an end-to-end e-commerce analytics platform that demonstrates a modern data lakehouse architecture. It combines batch processing, streaming processing, analytical serving, dashboards, machine learning, GenAI-assisted analytics, orchestration, testing, and containerized local deployment.

At a high level, the platform follows the medallion architecture:

- Bronze layer stores raw ingested data with ingestion metadata.
- Silver layer stores cleaned, typed, validated, deduplicated business entities.
- Gold layer stores business-ready aggregates and analytical marts.
- PostgreSQL serving layer exposes Gold tables to dashboards and the AI assistant.
- Streamlit, ML, and GenAI consume the curated serving layer and model artifacts.

```text
                         +----------------------------+
                         | Synthetic E-Commerce Data  |
                         | scripts/generate_sample... |
                         +-------------+--------------+
                                       |
                                       v
                            CSV files in data/raw/
                                       |
                                       v
+----------------+        +-----------+------------+        +---------------------+
| Source Schemas | -----> | Batch Bronze Ingestion | -----> | Bronze Delta Tables |
| ingestion/     |        | ingestion/batch_...    |        | warehouse/bronze/   |
| schemas.py     |        +------------------------+        +----------+----------+
+----------------+                                             raw + metadata
                                                                    |
                                                                    v
                                                        +-----------+------------+
                                                        | Silver Transformations |
                                                        | lakehouse/silver/      |
                                                        +-----------+------------+
                                                                    |
                                    +-------------------------------+------------------------------+
                                    | typed, cleaned, deduplicated, validated, quarantined failures |
                                    v                                                              v
                         +----------+-----------+                                      +-----------+-----------+
                         | Silver Delta Tables  |                                      | Quarantine Delta     |
                         | warehouse/silver/    |                                      | warehouse/quarantine |
                         +----------+-----------+                                      +-----------------------+
                                    |
                                    v
                         +----------+-----------+
                         | Gold Aggregations    |
                         | lakehouse/gold/      |
                         +----------+-----------+
                                    |
                                    v
                         +----------+-----------+
                         | Gold Delta Tables     |
                         | warehouse/gold/       |
                         +----------+-----------+
                                    |
                                    v
                         +----------+-----------+
                         | PostgreSQL Serving    |
                         | database/create_...   |
                         | database/load_...     |
                         +----------+-----------+
                                    |
                 +------------------+-------------------+------------------+
                 |                                      |                  |
                 v                                      v                  v
       +---------+---------+                  +---------+---------+  +-----+------+
       | Streamlit Dashboard|                  | GenAI SQL Assistant|  | Analytics  |
       | dashboard/         |                  | genai/             |  | Services   |
       +-------------------+                  +-------------------+  +------------+


Streaming path:

+-------------------+        +----------------+        +----------------------------+
| Event Generator   | -----> | Kafka Topics   | -----> | Spark Structured Streaming |
| ingestion/event...|        | customer/order |        | streaming/stream_...       |
+-------------------+        | payment/events |        +-------------+--------------+
                             +----------------+                      |
                                                                    v
                                                       +------------+-------------+
                                                       | Bronze/Silver Event Delta|
                                                       | warehouse/bronze/events  |
                                                       | warehouse/silver/events  |
                                                       +------------+-------------+
                                                                    |
                                                                    v
                                                       +------------+-------------+
                                                       | Real-Time Gold Metrics   |
                                                       | streaming/realtime_...   |
                                                       +------------+-------------+
                                                                    |
                                                                    v
                                                       +------------+-------------+
                                                       | PostgreSQL realtime_...  |
                                                       | Dashboard Real-Time Page |
                                                       +--------------------------+


ML path:

+---------------------+        +------------------------+        +-----------------------+
| Silver Orders/Items | -----> | Feature Engineering    | -----> | RandomForest Forecast |
| Products            |        | ml/feature_engine...   |        | ml/train_demand...    |
+---------------------+        +------------------------+        +-----------+-----------+
                                                                        |
                                                                        v
                                                           +------------+-------------+
                                                           | Versioned Model Artifacts|
                                                           | models/*.joblib/*.json   |
                                                           +------------+-------------+
                                                                        |
                                                                        v
                                                           +------------+-------------+
                                                           | Forecast CSV for UI      |
                                                           | dashboard Forecasting    |
                                                           +--------------------------+


Orchestration and runtime:

+----------------------+       +----------------------+       +----------------------+
| Docker Compose       |       | Airflow DAGs         |       | Makefile/Scripts     |
| postgres, kafka,     |       | batch, gold, ml      |       | local commands       |
| zookeeper, spark,    |       | pipelines            |       | and shortcuts        |
| dashboard, airflow   |       +----------------------+       +----------------------+
+----------------------+
```

## 2. Main Technologies Used

| Area | Implementation |
| --- | --- |
| Batch processing | PySpark 3.5.1 |
| Lakehouse storage | Delta Lake through `delta-spark` |
| Streaming | Kafka and Spark Structured Streaming |
| Serving database | PostgreSQL |
| Dashboard | Streamlit, Pandas, Plotly |
| ML | scikit-learn RandomForestRegressor, pandas feature engineering, joblib artifacts |
| GenAI | Groq LLM API, prompt-based SQL generation, deterministic SQL validation |
| Orchestration | Apache Airflow DAGs using BashOperator |
| Runtime | Dockerfile and Docker Compose |
| Configuration | Pydantic settings loaded from environment variables and `.env` |
| Testing | pytest and ruff |

## 3. Repository Structure

```text
.
|-- airflow/                 Airflow DAGs for batch, Gold, and ML pipelines
|-- analytics/               SQL query strings and dashboard data-access service
|-- config/                  Settings, logging, Spark, and Kafka configuration
|-- dashboard/               Streamlit app, pages, and reusable UI components
|-- database/                PostgreSQL schema, SQLAlchemy connection, loaders, models
|-- genai/                   Natural-language analytics assistant
|-- ingestion/               Batch ingestion schemas and Kafka event generator
|-- lakehouse/               Bronze, Silver, and Gold lakehouse pipelines
|-- ml/                      Feature engineering, training, evaluation, prediction, registry
|-- scripts/                 Operational scripts for local pipeline execution
|-- streaming/               Kafka-to-Delta streams and real-time aggregations
|-- tests/                   Unit and integration tests
|-- Dockerfile               Application image with Python, Java, Spark dependencies
|-- docker-compose.yml       Local service stack
|-- Makefile                 Developer shortcuts
|-- requirements.txt         Python dependency lock list
|-- pyproject.toml           pytest and ruff configuration
`-- README.md                Short project overview
```

## 4. Configuration System

Configuration is centralized in `config/settings.py` through a Pydantic `Settings` class.

Important settings include:

- `data_dir`, `raw_data_dir`, `warehouse_dir`, `checkpoint_dir`, and `model_dir` for local storage locations.
- `spark_master_url`, driver memory, executor memory, and shuffle partitions for Spark runtime behavior.
- Kafka bootstrap servers and topic names for streaming.
- Streaming watermark and window settings.
- PostgreSQL host, port, database, username, password, pool size, overflow, and timeout.
- Groq API key and model name for the GenAI assistant.

The settings class reads from `.env`, supports environment variable overrides, and resolves relative paths from the project root. This makes the same code usable locally and inside Docker containers.

The PostgreSQL SQLAlchemy URL is generated through `postgres_sqlalchemy_url`. It is used internally and should not be logged because it contains credentials.

## 5. Spark and Delta Lake Configuration

Spark setup is centralized in `config/spark_config.py`.

The `build_spark_session` function:

- Creates a Spark session with a project-specific app name.
- Uses `settings.spark_master_url`, which defaults to `local[*]`.
- Sets driver and executor memory.
- Sets `spark.sql.shuffle.partitions`.
- Creates the warehouse directory if it does not exist.
- Enables Delta Lake SQL extensions.
- Sets Spark catalog to Delta catalog.
- Enables Delta schema auto-merge.
- Supports additional Spark configs and packages.

The `spark_session` context manager starts Spark and reliably stops it after the pipeline completes. This pattern prevents long-running Spark sessions from being left behind during CLI runs.

## 6. Synthetic Source Data Generation

The project includes reproducible synthetic e-commerce data generation in `scripts/generate_sample_data.py`.

It creates CSV files under `data/raw/` for:

- Customers
- Categories
- Products
- Orders
- Order items
- Payments
- Inventory

The generator models realistic relationships:

- Products belong to categories.
- Orders belong to customers.
- Order items belong to orders and products.
- Payments align with order totals.
- Inventory belongs to products and warehouses.

The default generated volume is:

- 5,000 customers
- 1,000 products
- 20 categories
- 50,000 orders
- 1 to 4 items per order

The script uses a fixed default seed so data can be regenerated consistently for testing and demos.

## 7. Batch Ingestion and the Bronze Layer

Batch ingestion is implemented in `ingestion/batch_ingestion.py`.

### 7.1 Source Definitions

`ingestion/schemas.py` defines every raw source using `SourceDefinition`.

Each source definition contains:

- Target table name.
- Raw CSV file name.
- Expected source columns.
- Source system name.
- Explicit Spark schema.

All raw fields are initially read as strings. This is intentional: the Bronze layer preserves raw input shape and delays type conversion until Silver.

The schema also includes `_corrupt_record`, allowing Spark CSV permissive mode to capture malformed rows.

### 7.2 Bronze Ingestion Flow

For each configured source:

1. Read the CSV with header support, permissive parsing, explicit schema, and corrupt-record capture.
2. Add Bronze metadata.
3. Split valid parsed records from malformed records.
4. Append valid records into `warehouse/bronze/<table_name>`.
5. Write malformed rows into `warehouse/quarantine/bronze/<table_name>/<batch_id>`.
6. Return a structured ingestion result with read, written, and malformed counts.

### 7.3 Bronze Metadata

Bronze records receive:

- `ingestion_timestamp`: timestamp when Spark ingested the row.
- `source_system`: source identifier, such as `ecommerce_batch`.
- `source_file`: exact source path.
- `batch_id`: UUID or provided batch ID.
- `record_hash`: SHA-256 hash of source business columns.

This metadata provides observability, lineage, deduplication support, and debugging context.

## 8. Silver Layer: Cleaning, Typing, Validation, and Quarantine

The Silver pipeline is implemented in:

- `lakehouse/silver/silver_pipeline.py`
- `lakehouse/silver/transformations.py`
- `lakehouse/silver/validation.py`

Silver converts raw Bronze records into reliable analytical entities.

### 8.1 Processing Order and Dependencies

Silver transformations run in dependency order:

1. `categories`
2. `customers`
3. `products`
4. `orders`
5. `order_items`
6. `payments`
7. `inventory`

This matters because several tables require foreign-key validation:

- Products depend on categories.
- Orders depend on customers.
- Order items depend on orders and products.
- Payments depend on orders.
- Inventory depends on products.

If a user selects only certain tables, the Silver pipeline expands the selection to include dependencies automatically.

### 8.2 Cleaning Concepts

Silver transformations perform:

- Type casting from string to numeric, date, timestamp, or normalized text.
- Name normalization using trimming, repeated whitespace cleanup, and title casing.
- Lowercasing normalized categorical fields such as email, payment method, order status, event type, and device type.
- Warehouse ID standardization with uppercase formatting.
- Null and blank checks.
- Positive-number checks for prices, quantities, totals, stock, and reorder levels.
- Domain checks for valid order and payment statuses.
- Rating range validation.
- Payment-to-order amount consistency checks.
- Foreign-key checks against already-cleaned Silver tables.

### 8.3 Data Quality Flags

The Silver layer uses an array column named `data_quality_flags`.

Examples of flags include:

- `missing_customer_id`
- `missing_email`
- `invalid_order_date`
- `invalid_order_status`
- `invalid_price`
- `invalid_quantity`
- `payment_amount_mismatch`
- `missing_product_fk`
- `missing_category_fk`

Rows with an empty `data_quality_flags` array are considered valid.

Rows with one or more flags are written to the Silver quarantine area.

### 8.4 Deduplication

Valid Silver rows are deduplicated by business keys.

Examples:

- Customers by `customer_id`
- Products by `product_id`
- Orders by `order_id`
- Order items by `order_item_id`
- Inventory by `product_id` and `warehouse_id`

The deduplication function keeps the newest record based on:

- `ingestion_timestamp`
- `batch_id`

### 8.5 Silver Metadata

Silver outputs receive:

- `silver_processed_timestamp`

This records when the Silver version of the record was produced.

## 9. Gold Layer: Business-Ready Analytics Tables

The Gold pipeline is implemented in:

- `lakehouse/gold/gold_pipeline.py`
- `lakehouse/gold/transformations.py`

Gold transforms cleaned Silver data into dashboard-ready business metrics.

The project defines five batch Gold tables:

- `daily_sales_summary`
- `product_performance`
- `category_performance`
- `customer_360`
- `inventory_health`

The Gold layer only counts revenue for these order statuses:

- `delivered`
- `shipped`
- `processing`

Cancelled and returned orders are excluded from revenue metrics.

### 9.1 Daily Sales Summary

Source tables:

- Silver `orders`
- Silver `order_items`

Metrics:

- `sales_date`
- `total_orders`
- `total_revenue`
- `total_items_sold`
- `average_order_value`
- `unique_customers`

This table powers executive KPIs and sales trend charts.

### 9.2 Product Performance

Source tables:

- Silver `orders`
- Silver `order_items`
- Silver `products`
- Silver `categories`

Metrics:

- Units sold
- Gross revenue
- Discount amount
- Net revenue
- Average product rating
- Category name

This table supports top-product rankings and product revenue analysis.

### 9.3 Category Performance

Source tables:

- Silver `orders`
- Silver `order_items`
- Silver `products`
- Silver `categories`

Metrics:

- Total orders
- Units sold
- Revenue
- Average order value

This table supports category-level performance comparisons.

### 9.4 Customer 360

Source tables:

- Silver `customers`
- Silver `orders`

Metrics:

- Total orders
- Lifetime value
- Average order value
- Last order date
- Days since last order
- Customer segment

This table creates a customer-level analytical profile for retention and segmentation use cases.

### 9.5 Inventory Health

Source tables:

- Silver `inventory`
- Silver `products`
- Silver `orders`
- Silver `order_items`

Metrics:

- Stock quantity
- Reorder level
- Inventory status
- Estimated days remaining

Inventory status is derived as:

- `out_of_stock` when stock is zero or below.
- `low_stock` when stock is at or below reorder level.
- `healthy` otherwise.

Estimated days remaining is based on recent 30-day demand velocity.

## 10. PostgreSQL Serving Layer

The serving layer is implemented in:

- `database/create_tables.sql`
- `database/load_gold_to_postgres.py`
- `database/models.py`
- `database/connection.py`

The purpose of this layer is to expose curated Gold data through fast relational tables that are easy for Streamlit and the GenAI assistant to query.

### 10.1 Serving Tables

PostgreSQL tables include:

- `daily_sales_summary`
- `product_performance`
- `category_performance`
- `customer_360`
- `inventory_health`
- `realtime_metrics`

### 10.2 Indexing

Indexes are created for common dashboard access patterns:

- Sales date lookup.
- Product revenue ranking.
- Category revenue ranking.
- Customer lifetime value ranking.
- Customer segment filtering.
- Inventory status filtering.
- Real-time metric window ordering.

### 10.3 Gold-to-PostgreSQL Load

`database/load_gold_to_postgres.py`:

1. Creates serving tables and indexes if needed.
2. Reads Gold Delta tables with Spark.
3. Selects columns according to table models.
4. Collects rows into Python dictionaries.
5. Converts null-like values to DBAPI-safe `None`.
6. Deletes current table contents.
7. Inserts the latest Gold snapshot in batches.

This implements snapshot replacement semantics: PostgreSQL reflects the latest Gold table state after each load.

## 11. Analytics Query Service

The dashboard data service lives in:

- `analytics/queries.py`
- `analytics/kpi_service.py`

`analytics/queries.py` stores read-only PostgreSQL SQL queries for:

- Overview KPIs
- Daily sales
- Monthly sales
- Product performance
- Category performance
- Customer 360
- Customer segments
- Inventory health
- Inventory status
- Real-time metrics

`analytics/kpi_service.py` executes these queries and returns a `DashboardData` dataclass containing pandas DataFrames. This separates dashboard rendering from database access.

## 12. Streamlit Dashboard

The dashboard starts from `dashboard/app.py`.

It configures:

- Wide Streamlit layout.
- Sidebar navigation.
- Cached PostgreSQL engine.
- Cached dashboard data with a 300-second TTL.
- Friendly database readiness errors.

Dashboard pages:

- Overview
- Sales
- Products
- Customers
- Inventory
- Real-Time
- Forecasting
- AI Assistant

### 12.1 Overview Page

The Overview page summarizes key e-commerce KPIs and trend data. It uses the `overview_kpis`, `daily_sales`, and related dashboard datasets.

### 12.2 Sales Page

The Sales page focuses on:

- Daily sales table.
- Sales trend visualization.
- Revenue and order movement over time.

### 12.3 Product Analytics Page

The Product page focuses on product-level performance:

- Net revenue ranking.
- Units sold.
- Discounts.
- Gross and net revenue.
- Category context.
- Average rating.

### 12.4 Customer Analytics Page

The Customer page presents customer lifetime and segmentation metrics:

- Top customers by lifetime value.
- Order counts.
- Average order value.
- Segment distribution and value.

### 12.5 Inventory Analytics Page

The Inventory page shows:

- Out-of-stock, low-stock, and healthy products.
- Stock quantity.
- Reorder level.
- Estimated days remaining.

### 12.6 Real-Time Monitoring Page

The Real-Time page consumes `realtime_metrics` from PostgreSQL.

It shows:

- Live revenue
- Live orders
- Active users
- Product views
- Payment failures
- Windowed event metrics

If no real-time metrics exist yet, it tells the user to run Kafka streams and load `realtime_metrics`.

### 12.7 Forecasting Page

The Forecasting page reads:

```text
models/demand_forecast_predictions_latest.csv
```

It displays demand forecasts for selected products after the ML pipeline has trained a model and generated predictions.

### 12.8 AI Assistant Page

The AI Assistant page lets a user ask natural-language analytics questions. It delegates the flow to the GenAI assistant and displays:

- Generated answer
- Generated SQL
- Query result table

## 13. Streaming Architecture

Streaming is implemented in:

- `ingestion/event_generator.py`
- `streaming/stream_processor.py`
- `streaming/realtime_aggregations.py`
- `streaming/checkpoint_manager.py`
- `streaming/load_realtime_to_postgres.py`

### 13.1 Event Generator

The event generator simulates e-commerce activity and publishes JSON events to Kafka.

Supported event types:

- `product_view`
- `product_search`
- `add_to_cart`
- `remove_from_cart`
- `checkout_started`
- `purchase_completed`
- `payment_failed`

Events include:

- `event_id`
- `customer_id`
- `session_id`
- `event_type`
- `product_id`
- `category_id`
- `quantity`
- `price`
- `city`
- `device_type`
- `event_timestamp`

Events are routed to Kafka topics based on event type. Purchases go to the order events topic, payment failures go to the payment events topic, and most browsing/cart events go to the customer events topic.

### 13.2 Kafka-to-Delta Stream Processor

`streaming/stream_processor.py` reads Kafka records using Spark Structured Streaming.

It:

- Reads from configured Kafka event topics.
- Parses JSON payloads with an explicit schema.
- Preserves Kafka metadata such as topic, partition, offset, timestamp, key, and raw JSON.
- Adds Bronze-style metadata for streaming events.
- Marks malformed records.
- Cleans valid event records for Silver.

The stream can run in two target modes:

- `bronze`: writes raw parsed events to `warehouse/bronze/events`.
- `silver`: writes cleaned valid events to `warehouse/silver/events`.

### 13.3 Streaming Data Quality

The Silver streaming path:

- Filters malformed events.
- Converts event timestamps.
- Normalizes event type, city, and device type.
- Requires event ID, customer ID, timestamp, and valid event type.
- Rejects negative prices.
- Rejects non-positive quantities.
- Applies event-time watermarks.
- Deduplicates by `event_id`.

### 13.4 Real-Time Gold Metrics

`streaming/realtime_aggregations.py` builds windowed real-time metrics from Kafka events.

It uses:

- Event-time windows.
- Watermarking.
- Deduplication by event ID.
- Sliding window configuration from settings.

Metrics include:

- `revenue_per_window`
- `orders_per_window`
- `active_users`
- `product_views`
- `payment_failures`
- `add_to_cart_events`
- `checkout_events`
- `payment_failure_rate`
- `cart_abandonment_rate`
- `window_start`
- `window_end`

These metrics are written to `warehouse/gold/realtime_metrics`, then can be loaded into PostgreSQL for the dashboard.

### 13.5 Checkpoints

Streaming checkpoints are stored under the configured checkpoint directory. Checkpoints allow Spark Structured Streaming to maintain offsets and state between runs.

## 14. Machine Learning Demand Forecasting

The ML subsystem is implemented in:

- `ml/feature_engineering.py`
- `ml/feature_engineering_cli.py`
- `ml/train_demand_forecast.py`
- `ml/evaluate_model.py`
- `ml/predict_demand.py`
- `ml/model_registry.py`

### 14.1 Feature Engineering

The ML pipeline builds daily product-level sales from Silver tables.

Source tables:

- Silver `orders`
- Silver `order_items`
- Silver `products`

It filters to revenue-producing orders and groups data by:

- `sales_date`
- `product_id`
- `category_id`

Target:

- `units_sold`

Feature columns:

- `product_id`
- `category_id`
- `day_of_week`
- `month`
- `week_of_year`
- `lag_1`
- `lag_7`
- `rolling_mean_7`
- `rolling_mean_30`
- `revenue`

Time-series features include lags and rolling means per product. Missing lag values are filled with product-level average demand, then zero if needed.

### 14.2 Training

`ml/train_demand_forecast.py` trains a scikit-learn pipeline:

- `StandardScaler`
- `RandomForestRegressor`

The train/test split is time-based. By default, the latest 14 days are used for testing. If that split is not possible because of data shape, the code falls back to an 80/20 chronological split.

The trained bundle includes:

- Model pipeline.
- Feature columns.
- Target column.
- Evaluation metrics.
- Training timestamp.
- Maximum sales date used in training.

### 14.3 Prediction

`ml/predict_demand.py` loads the latest model bundle and produces recursive future features for each product.

By default, it forecasts 7 days ahead.

Prediction outputs include:

- Forecast date
- Product ID
- Category ID
- Predicted units sold
- Lag and rolling features used for context

### 14.4 Model Registry

`ml/model_registry.py` implements a simple local model registry.

It writes:

- Versioned model files: `models/demand_forecast_<version>.joblib`
- Latest model pointer: `models/demand_forecast_latest.joblib`
- Versioned metrics JSON files
- Versioned prediction CSV files
- Latest prediction CSV pointer: `models/demand_forecast_predictions_latest.csv`

This allows the dashboard to always read the latest forecast while preserving historical artifacts.

## 15. GenAI Analytics Assistant

The GenAI subsystem is implemented in:

- `genai/analytics_assistant.py`
- `genai/llm_client.py`
- `genai/sql_generator.py`
- `genai/sql_validator.py`
- `genai/schema_context.py`
- `genai/query_executor.py`
- `genai/response_generator.py`
- `genai/cli.py`

The assistant converts natural-language questions into safe PostgreSQL analytics queries.

### 15.1 Assistant Flow

`answer_question` performs the complete flow:

1. Build a SQL-generation prompt.
2. Send the prompt to the configured LLM client.
3. Parse the LLM response as JSON.
4. Validate the generated SQL deterministically.
5. Execute the SQL against PostgreSQL.
6. Summarize the result into a user-facing answer.
7. Return the question, SQL, explanation, answer, and result DataFrame.

### 15.2 Approved Schema Context

`genai/schema_context.py` defines the only tables the assistant may query:

- `daily_sales_summary`
- `product_performance`
- `category_performance`
- `customer_360`
- `inventory_health`
- `realtime_metrics`

Only approved columns are included in the prompt.

### 15.3 Prompt Rules

The SQL prompt tells the model to:

- Return valid JSON with `sql` and `explanation`.
- Generate exactly one SELECT query.
- Avoid write, DDL, permission, procedure, copy, and execution commands.
- Query only approved tables.
- Prefer aggregates for KPI questions.
- Add a default limit for row-level lists.
- Never expose credentials or system details.

### 15.4 Deterministic SQL Safety Validation

The validator enforces rules after the LLM response:

- SQL cannot be empty.
- Only one statement is allowed.
- Query must start with `SELECT`.
- Blocked keywords are rejected.
- Query must reference at least one approved table.
- Unknown tables are rejected.
- A default `LIMIT` is appended if the query does not include one.

This is important because the LLM is not trusted blindly. The deterministic validator is the safety gate before database execution.

### 15.5 Groq LLM Client

`genai/llm_client.py` implements a Groq-backed client. It uses:

- `GROQ_API_KEY`
- `GROQ_MODEL`
- Temperature `0.0`
- JSON response format

The assistant fails clearly if `GROQ_API_KEY` is missing.

## 16. Airflow Orchestration

Airflow DAGs are located in `airflow/dags/`.

They are intentionally lightweight: DAG files do not create Spark sessions or load heavy modules at parse time. Instead, they use `BashOperator` tasks that run project commands from the repository root.

### 16.1 Batch Pipeline DAG

File:

```text
airflow/dags/batch_pipeline_dag.py
```

Tasks:

1. `validate_source_files`
2. `ingest_bronze`
3. `transform_silver`
4. `run_data_quality_checks`

This DAG validates raw CSV availability, ingests Bronze, transforms Silver, and runs selected data-quality tests.

### 16.2 Gold Pipeline DAG

File:

```text
airflow/dags/gold_pipeline_dag.py
```

Tasks:

1. `build_gold_tables`
2. `load_gold_to_postgres`
3. `run_data_quality_checks`

This DAG turns Silver data into Gold marts and refreshes the PostgreSQL serving layer.

### 16.3 ML Pipeline DAG

File:

```text
airflow/dags/ml_pipeline_dag.py
```

Tasks:

1. `prepare_features`
2. `train_model`
3. `evaluate_model`
4. `save_model`
5. `generate_predictions`

This DAG runs the full forecasting lifecycle.

### 16.4 Shared DAG Configuration

`airflow/dags/common.py` provides:

- Default owner.
- Retry count.
- Retry delay.
- Stable start date.
- Optional schedule disabling through `AIRFLOW_DISABLE_SCHEDULES`.
- Shell command construction from the project root.

## 17. Docker and Local Runtime

The project includes a production-style local runtime using Docker Compose.

### 17.1 Dockerfile

The Docker image:

- Uses `python:3.11-slim-bookworm`.
- Sets Python no-bytecode and unbuffered output.
- Installs Java 17 for Spark.
- Installs Linux packages such as Bash, curl, GCC, G++, Java runtime, and procps.
- Installs Python dependencies from `requirements.txt`.
- Copies the project code into `/opt/app`.
- Makes scripts executable.
- Exposes Streamlit port `8501`.
- Starts Streamlit by default.

### 17.2 Docker Compose Services

`docker-compose.yml` defines:

- PostgreSQL
- Zookeeper
- Kafka
- Spark master
- Spark worker
- Dashboard
- Airflow webserver
- Airflow scheduler

Important local ports:

- PostgreSQL: host `5433` to container `5432`
- Kafka: `9092`
- Zookeeper: `2181`
- Spark master UI: `8080`
- Spark master RPC: `7077`
- Streamlit dashboard: `8501`
- Airflow webserver: `8088`

### 17.3 Health Checks

Compose services include health checks:

- PostgreSQL uses `pg_isready`.
- Zookeeper uses `srvr` and checks for `Mode`.
- Kafka checks broker API versions.
- Spark master checks the web port.
- Dashboard checks Streamlit health endpoint.
- Airflow webserver checks `/health`.

### 17.4 Volumes

The dashboard container mounts local directories:

- `./data`
- `./warehouse`
- `./checkpoints`
- `./models`
- `./logs`

This keeps generated data, Delta tables, streaming checkpoints, ML models, and logs visible on the host machine.

## 18. Developer Commands

The Makefile provides shortcuts:

```bash
make setup
make generate-data
make bronze
make silver
make gold
make postgres-load
make dashboard
make ml
make kafka-topics
make docker-build
make docker-up
make docker-down
make docker-logs
make test
```

The equivalent manual batch flow is:

```bash
python scripts/generate_sample_data.py
python ingestion/batch_ingestion.py
python lakehouse/silver/silver_pipeline.py
python lakehouse/gold/gold_pipeline.py
python database/load_gold_to_postgres.py
streamlit run dashboard/app.py
```

## 19. Testing Strategy

Tests are stored under `tests/`.

The project includes unit and integration coverage for:

- Batch source schemas.
- Database serving models and loaders.
- Dashboard components.
- Docker assets.
- Spark configuration.
- Silver transformations.
- Gold transformations.
- Streaming event logic.
- Real-time aggregation logic.
- ML feature engineering.
- ML predictions.
- GenAI SQL generation.
- GenAI SQL validation.
- Airflow DAG structure.

`pyproject.toml` configures pytest:

- Test path: `tests`
- Python path: project root
- Additional summary output with `-ra`

Ruff is configured for Python 3.11 with a 100-character line length and selected lint rules.

## 20. Data Quality and Reliability Concepts Implemented

This project implements several reliability patterns:

- Explicit schemas for raw ingestion.
- Permissive CSV parsing with corrupt-record capture.
- Bronze operational metadata.
- Quarantine storage for malformed and invalid records.
- Silver type enforcement.
- Silver data-quality flags.
- Foreign-key validation.
- Deduplication with timestamp and batch ordering.
- Delta Lake transactional storage.
- Spark Structured Streaming checkpoints.
- Event-time watermarks.
- Streaming deduplication by event ID.
- PostgreSQL serving-table indexes.
- SQL allowlisting for AI-generated queries.
- Read-only AI query execution.
- Docker health checks.
- Airflow retries.
- Cached dashboard connections and query results.

## 21. Conceptual Data Lifecycle

The full lifecycle is:

1. Generate source data in CSV format.
2. Ingest raw files into Bronze Delta.
3. Preserve lineage and ingestion metadata.
4. Quarantine malformed raw rows.
5. Clean and type records into Silver.
6. Validate quality and relationships.
7. Quarantine invalid Silver rows.
8. Build Gold business marts.
9. Load Gold snapshots into PostgreSQL.
10. Query PostgreSQL from dashboards.
11. Ask natural-language questions through the GenAI assistant.
12. Produce Kafka events for real-time behavior.
13. Stream events through Spark.
14. Build real-time windowed Gold metrics.
15. Load real-time metrics into PostgreSQL.
16. Train ML forecasts from Silver data.
17. Save model artifacts and prediction files.
18. Display forecasts in Streamlit.
19. Orchestrate repeatable flows with Airflow.
20. Validate behavior with tests.

## 22. Important Security Notes

Do not commit real secrets.

The project expects secrets such as `GROQ_API_KEY` to be placed in `.env`, not in `.env.example`.

The GenAI assistant is deliberately restricted:

- It receives only approved analytics schema context.
- It validates generated SQL before execution.
- It only permits SELECT queries.
- It blocks destructive or administrative keywords.
- It rejects unknown tables.

These safeguards reduce risk when using an LLM to generate SQL.

## 23. Current Runtime Expectations

For the complete platform to work locally:

1. Python dependencies must be installed.
2. Java must be available for Spark.
3. Docker services must be running for PostgreSQL, Kafka, and optional Airflow/Spark services.
4. Raw sample data must exist.
5. Bronze, Silver, and Gold pipelines must be run in order.
6. Gold tables must be loaded into PostgreSQL.
7. Streamlit can then read serving tables.
8. Forecasts appear only after the ML train and prediction flow has run.
9. Real-time metrics appear only after Kafka event streaming and real-time loading have run.
10. AI assistant requires `GROQ_API_KEY`.

## 24. Why This Project Matters

This repository is not just a dashboard. It is a compact version of a modern analytics platform:

- Data engineering through Spark and Delta Lake.
- Data modeling through Gold analytical marts.
- Serving through PostgreSQL.
- Business intelligence through Streamlit.
- Real-time analytics through Kafka and Spark Structured Streaming.
- Predictive analytics through scikit-learn forecasting.
- Natural-language analytics through a guarded GenAI SQL assistant.
- Workflow orchestration through Airflow.
- Local reproducibility through Docker Compose.
- Quality and maintainability through tests, configuration, and linting.

The design shows how raw operational data can be converted into trusted, queryable, and interactive business intelligence while preserving lineage, validation, and operational repeatability.
