# AI-Powered Data Lakehouse Analytics Platform

An end-to-end e-commerce analytics platform built with PySpark, Delta Lake, Kafka, PostgreSQL, Streamlit, machine learning, GenAI, Airflow, and Docker.

The project demonstrates a production-style data platform using the medallion architecture:

- Bronze: raw batch and streaming data
- Silver: cleaned, typed, validated data
- Gold: business-ready analytics tables
- Serving: PostgreSQL tables for dashboards and AI analytics
- Consumption: Streamlit dashboards, ML forecasts, and a GenAI analytics assistant

## Architecture

```text
Batch CSV/JSON
     |
     v
PySpark Batch Ingestion
     |
     v
Bronze Delta Lake
     |
     v
Silver Delta Lake
     |
     v
Gold Delta Lake
     |
     +------------------> PostgreSQL Serving Layer
                              |
                              v
                         Streamlit Dashboard
                              |
                              v
                    GenAI Analytics Assistant

Kafka Events
     |
     v
Spark Structured Streaming
     |
     v
Real-Time Gold Metrics
     |
     v
PostgreSQL + Real-Time Dashboard

Gold/Silver Data
     |
     v
Demand Forecasting ML Pipeline
     |
     v
Forecasting Dashboard

Airflow orchestrates batch, Gold, and ML pipelines.
Docker Compose can run the platform dependencies locally.
```

## Cloud Target Architecture

The project is being migrated with the following local-to-cloud mapping while
keeping Streamlit as the visualization layer:

| Local project component | Cloud-integrated component |
| --- | --- |
| `data/raw/` | Amazon S3 `raw/` through a Databricks volume path |
| `warehouse/bronze/` | Bronze Delta tables stored on S3 |
| `warehouse/silver/` | Silver Delta tables stored on S3 |
| `warehouse/gold/` | Gold Delta tables stored on S3 |
| Local Spark | Databricks Spark serverless jobs |
| Local PostgreSQL container | Amazon RDS PostgreSQL |
| Local FastAPI | Local first, later App Runner, ECS, or EC2 |
| Local Streamlit | Local Streamlit dashboard |
| Local Airflow | Local initially; Databricks Jobs for cloud batch runs |
| Local model files | Databricks volume or S3-backed model artifact path |

Cloud batch flow:

```text
Synthetic / source data
     |
     v
Amazon S3 raw/
     |
     v
Databricks Bronze Delta
     |
     v
Databricks Silver Delta
     |
     v
Databricks Gold Delta
     |
     v
Amazon RDS PostgreSQL
     |
     v
FastAPI Analytics API
     |
     v
Streamlit Dashboard
     |
     v
GenAI SQL Assistant
```

Cloud streaming flow:

```text
Event Generator
     |
     v
Kafka
     |
     v
Spark Structured Streaming
     |
     v
Bronze Event Delta on S3
     |
     v
Silver Event Delta on S3
     |
     v
Gold realtime_metrics Delta
     |
     v
Amazon RDS PostgreSQL
     |
     v
Streamlit Real-Time Dashboard
```

## Features

- Synthetic e-commerce data generation
- Batch ingestion from CSV sources
- Bronze, Silver, and Gold Delta Lake layers
- Data quality validation and quarantine handling
- PostgreSQL serving tables with indexes
- Streamlit dashboard with multiple analytics pages
- Kafka event producer for real-time e-commerce activity
- Spark Structured Streaming processors
- Real-time metrics page
- Demand forecasting with feature engineering and model artifacts
- GenAI assistant that converts natural language into safe SQL
- Airflow DAGs for orchestration
- Dockerfile, Docker Compose, Makefile, and tests

## Tech Stack

| Area | Technology |
| --- | --- |
| Data processing | PySpark, Spark SQL |
| Storage | Delta Lake |
| Streaming | Kafka, Spark Structured Streaming |
| Serving database | PostgreSQL |
| Dashboard | Streamlit, Plotly, Pandas |
| ML | scikit-learn, joblib |
| GenAI | Groq LLM API, SQLAlchemy |
| Orchestration | Apache Airflow |
| DevOps | Docker, Docker Compose, Makefile |
| Testing | pytest |

## Repository Structure

```text
.
|-- airflow/                 # Airflow DAGs
|-- analytics/               # SQL queries and dashboard data services
|-- config/                  # Settings, logging, Spark, Kafka config
|-- dashboard/               # Streamlit app, pages, and components
|-- database/                # PostgreSQL schema, models, loaders
|-- genai/                   # Natural-language analytics assistant
|-- ingestion/               # Batch ingestion and Kafka event generator
|-- lakehouse/               # Bronze, Silver, and Gold pipelines
|-- ml/                      # Forecasting feature, training, evaluation, prediction code
|-- scripts/                 # Local execution scripts
|-- streaming/               # Structured Streaming and real-time loaders
|-- tests/                   # Unit and integration tests
|-- Dockerfile
|-- docker-compose.yml
|-- Makefile
|-- pyproject.toml
|-- requirements.txt
`-- .env.example
```

## Prerequisites

Install these before running the project locally:

- Python 3.11 or 3.12
- Java 11 or Java 17
- Docker and Docker Compose
- PostgreSQL, either local or through Docker Compose
- Kafka, either local or through Docker Compose

For the GenAI assistant, you also need a Groq API key.

## Environment Setup

Create a virtual environment and install dependencies:

```bash
python3 -m venv .venv
source .venv/bin/activate
python -m pip install --upgrade pip
python -m pip install -r requirements.txt
```

Create a local environment file:

```bash
cp .env.example .env
```

Update `.env` with your local values:

```env
POSTGRES_HOST=localhost
POSTGRES_PORT=5432
POSTGRES_DB=lakehouse_analytics
POSTGRES_USER=lakehouse_user
POSTGRES_PASSWORD=change_me

KAFKA_BOOTSTRAP_SERVERS=localhost:9092

GROQ_API_KEY=
GROQ_MODEL=openai/gpt-oss-120b
```


## Quick Start

Use one command to start the local demo platform, run the batch lakehouse pipeline,
train forecasts, seed real-time dashboard metrics, and restart the dashboard:

```bash
make start-full-demo
```

Open:

```text
http://localhost:8501
```

To start the same setup with live Kafka/Spark streaming instead of seeded demo
real-time rows:

```bash
LIVE_STREAMING=true make start-full-demo
```

Use this manual flow only when you want to run each batch step yourself.

```bash
source .venv/bin/activate

python scripts/generate_sample_data.py
python ingestion/batch_ingestion.py
python lakehouse/silver/silver_pipeline.py
python lakehouse/gold/gold_pipeline.py
python database/load_gold_to_postgres.py
streamlit run dashboard/app.py
```

Open:

```text
http://localhost:8501
```

## Run With Make

The Makefile provides shortcuts:

```bash
make setup
make generate-data
make bronze
make silver
make gold
make postgres-load
make streamlit
make start-full-demo
make streaming-up
make streaming-down
make airflow-up
make airflow-down
```

Run tests:

```bash
make test
```

Run Docker workflow:

```bash
make docker-build
make docker-up
make docker-logs
make docker-down
```

## Batch Pipeline

### 1. Generate Sample Data

```bash
python scripts/generate_sample_data.py
```

This creates e-commerce source files under:

```text
data/raw/
```

Main generated entities:

- customers
- categories
- products
- orders
- order_items
- payments
- inventory

### 2. Bronze Ingestion

```bash
python ingestion/batch_ingestion.py
```

Run selected tables:

```bash
python ingestion/batch_ingestion.py --tables customers products orders
```

Bronze output:

```text
warehouse/bronze/<table_name>
```

Malformed rows, when found, are written to:

```text
warehouse/quarantine/bronze/
```

### 3. Silver Transformation

```bash
python lakehouse/silver/silver_pipeline.py
```

Run selected tables:

```bash
python lakehouse/silver/silver_pipeline.py --tables customers products orders
```

Silver output:

```text
warehouse/silver/<table_name>
```

Invalid business records, when found, are written to:

```text
warehouse/quarantine/silver/
```

### 4. Gold Aggregation

```bash
python lakehouse/gold/gold_pipeline.py
```

Gold tables:

- `daily_sales_summary`
- `product_performance`
- `category_performance`
- `customer_360`
- `inventory_health`

Gold output:

```text
warehouse/gold/<table_name>
```

## PostgreSQL Serving Layer

Create PostgreSQL tables and load Gold Delta snapshots:

```bash
python database/load_gold_to_postgres.py
```

Load selected tables:

```bash
python database/load_gold_to_postgres.py --tables daily_sales_summary product_performance
```

Skip table creation if the schema already exists:

```bash
python database/load_gold_to_postgres.py --skip-create-tables
```

Serving tables:

- `daily_sales_summary`
- `product_performance`
- `category_performance`
- `customer_360`
- `inventory_health`
- `realtime_metrics`

## Streamlit Dashboard

Start the dashboard:

```bash
streamlit run dashboard/app.py
```

Dashboard pages:

- Overview
- Sales Analytics
- Product Analytics
- Customer Analytics
- Inventory Analytics
- Real-Time Monitoring
- Demand Forecasting
- AI Assistant

The dashboard reads from PostgreSQL, so load the serving tables first.

## Real-Time Streaming Pipeline

For an instant demo, `make start-full-demo` seeds recent real-time metric windows
into PostgreSQL. For live streaming, run:

```bash
LIVE_STREAMING=true make start-full-demo
```

Or start/stop the streaming services separately after the core platform is up:

```bash
make streaming-up
make streaming-down
```

The live streaming profile runs:

- `event-producer`: generates Kafka e-commerce events
- `event-bronze`: stores raw Kafka events in `warehouse/bronze/events`
- `event-silver`: stores cleaned Kafka events in `warehouse/silver/events`
- `realtime-metrics`: builds Spark Structured Streaming Gold metrics
- `realtime-loader`: periodically loads Gold real-time metrics into PostgreSQL

Real-time outputs:

```text
warehouse/gold/realtime_metrics
checkpoints/
```

Open the Real-Time Monitoring page in Streamlit to view the metrics.

## Demand Forecasting

Run the full ML pipeline:

```bash
bash scripts/run_ml_pipeline.sh
```

Or run each step manually:

```bash
python ml/feature_engineering_cli.py
python ml/train_demand_forecast.py --test-days 14
python ml/evaluate_model.py
python ml/predict_demand.py --horizon-days 7
```

Generated model artifacts:

```text
models/demand_forecast_features.csv
models/demand_forecast_latest.joblib
models/demand_forecast_metrics_<version>.json
models/demand_forecast_predictions_latest.csv
```

The Forecasting dashboard page reads prediction output from `models/`.

## GenAI Analytics Assistant

Set your Groq API key in `.env`:

```env
GROQ_API_KEY=your_groq_api_key
GROQ_MODEL=openai/gpt-oss-120b
```

Ask a question from the CLI:

```bash
python genai/cli.py "Which products generated the highest revenue?"
```

You can also use the AI Assistant page in Streamlit.

The assistant includes SQL safety controls:

- SELECT-only SQL
- approved Gold/serving table enforcement
- blocked destructive statements
- read-only query execution
- configurable result limits

## Airflow Orchestration

Airflow DAGs are located in:

```text
airflow/dags/
```

DAGs:

- `batch_pipeline_dag.py`
- `gold_pipeline_dag.py`
- `ml_pipeline_dag.py`

Local script equivalents:

```bash
bash scripts/run_batch_pipeline.sh
bash scripts/run_gold_pipeline.sh
bash scripts/run_ml_pipeline.sh
```

The DAG files are lightweight and do not start Spark sessions at parse time.

You only need to run Airflow locally if you want the DAG schedules to execute
automatically. Without Airflow, use `make start-full-demo` or the scripts
manually.

Start Airflow after setting `AIRFLOW_ADMIN_PASSWORD` in `.env`:

```bash
make airflow-up
```

Then open `http://localhost:8088`, enable/unpause the DAGs, and trigger them
or wait for their schedules. Stop Airflow with:

```bash
make airflow-down
```

## Docker Compose

Create `.env` first:

```bash
cp .env.example .env
```

Build and start services:

```bash
docker compose build
docker compose up -d postgres zookeeper kafka spark-master spark-worker dashboard
```

Useful service URLs:

```text
Streamlit:     http://localhost:8501
Spark Master:  http://localhost:8080
Airflow:       http://localhost:8088
PostgreSQL:    localhost:5433
Kafka:         localhost:9092
```

Airflow is opt-in and requires credentials in `.env`:

```env
AIRFLOW_ADMIN_USERNAME=admin
AIRFLOW_ADMIN_PASSWORD=change_me_before_running_airflow
```

Start Airflow:

```bash
make airflow-up
```

Stop Airflow:

```bash
make airflow-down
```

Stop services:

```bash
docker compose down
```

## Databricks on AWS

This repo now includes a Databricks-on-AWS deployment scaffold:

```text
databricks.yml
databricks/resources/jobs.yml
.env.databricks.example
docs/databricks_aws_deployment.md
```

Recommended cloud path:

```text
S3-backed Unity Catalog volume
-> Databricks Delta Lake tables
-> Databricks Jobs for batch, streaming, and ML
-> RDS/Aurora PostgreSQL serving layer
-> FastAPI REST API
-> Streamlit dashboard
```

Validate the local cloud configuration first:

```bash
make cloud-validate
```

After installing the AWS CLI and Databricks CLI, the Make targets load `.env`
and pass the required bundle variables:

```bash
make databricks-validate
make databricks-put-secrets
make databricks-deploy
make databricks-run-batch
```

Run a concise cloud readiness check from this machine:

```bash
make cloud-check
```

For a config-only check without network calls:

```bash
make cloud-check CLOUD_CHECK_ARGS=--skip-network
```

For Databricks Free Edition/serverless, the default lakehouse root is:

```text
/Volumes/workspace/default/ai_powered_lakehouse
```

If Databricks cannot reach RDS, allow PostgreSQL port `5432` from Databricks
serverless networking or load the Databricks-produced Gold Delta tables from a
network location that can reach RDS.

To use the supported local fallback after a Databricks run has produced Gold
Delta tables:

```bash
make databricks-load-gold-local
```

See `docs/databricks_aws_deployment.md` for setup details.

View logs:

```bash
docker compose logs -f
```

## Testing

Run all tests:

```bash
pytest
```

Run focused tests:

```bash
pytest tests/test_spark_config.py
pytest tests/unit
pytest tests/integration
```

Useful test groups:

```bash
pytest tests/unit/test_database_serving.py
pytest tests/unit/test_dashboard_components.py
pytest tests/unit/test_genai_sql_validator.py tests/unit/test_genai_sql_generator.py
pytest tests/unit/test_ml_features.py tests/unit/test_ml_predictions.py
pytest tests/unit/test_airflow_dags.py
pytest tests/unit/test_docker_assets.py
```

## Configuration Reference

Main configuration lives in `.env`.

| Variable | Purpose |
| --- | --- |
| `DATA_DIR` | Local generated data folder |
| `WAREHOUSE_DIR` | Delta Lake warehouse folder |
| `CHECKPOINT_DIR` | Streaming checkpoint folder |
| `MODEL_DIR` | ML artifact folder |
| `SPARK_MASTER_URL` | Spark master, usually `local[*]` for local |
| `KAFKA_BOOTSTRAP_SERVERS` | Kafka bootstrap address |
| `POSTGRES_HOST` | PostgreSQL host |
| `POSTGRES_PORT` | PostgreSQL port |
| `POSTGRES_DB` | PostgreSQL database |
| `POSTGRES_USER` | PostgreSQL username |
| `POSTGRES_PASSWORD` | PostgreSQL password |
| `GROQ_API_KEY` | Groq API key for GenAI assistant |
| `GROQ_MODEL` | Groq model name |

## End-to-End Execution Order

Use this order when demonstrating the complete platform:

1. Configure `.env`
2. Start PostgreSQL
3. Generate sample data
4. Run Bronze ingestion
5. Run Silver transformations
6. Run Gold aggregations
7. Load Gold tables into PostgreSQL
8. Start Streamlit dashboard
9. Run ML pipeline for forecasting
10. Configure Groq key and use AI Assistant
11. Start Kafka
12. Create Kafka topics
13. Run event generator
14. Run real-time aggregation
15. Load real-time metrics into PostgreSQL
16. View Real-Time Monitoring dashboard

## Project Summary

This repository is a complete data engineering and analytics portfolio project. It shows how raw e-commerce data can be ingested, validated, transformed, served, visualized, forecasted, queried with AI, orchestrated, tested, and containerized using a modern open-source data stack.
