# Databricks on AWS Deployment Plan

This project can be moved from local Docker to a Databricks-on-AWS lakehouse with:

```text
AWS S3 + Databricks + Delta Lake + Databricks Jobs
```

The local project already uses Spark, Delta Lake, batch jobs, Structured Streaming,
and scheduled workflows. Databricks is the most direct cloud upgrade path because
Databricks Delta tables are Delta Lake by default and Databricks Jobs can
orchestrate repeatable ETL and ML workflows.

## Target Architecture

```text
Batch CSV/API data
-> S3 landing zone
-> Databricks Job: generate/load raw files
-> Bronze Delta on S3
-> Silver Delta on S3
-> Gold Delta on S3
-> RDS/Aurora PostgreSQL serving layer
-> FastAPI REST API
-> Streamlit/BI/GenAI assistant

Live events
-> Amazon MSK or Confluent Cloud Kafka
-> Databricks Structured Streaming
-> Gold realtime_metrics Delta on S3
-> Databricks scheduled loader job
-> RDS/Aurora PostgreSQL
-> FastAPI REST API
```

## Recommended AWS Services

| Need | Recommended Service |
| --- | --- |
| Lakehouse storage | Amazon S3 |
| Spark compute | Databricks Jobs clusters |
| Delta tables | Databricks Delta Lake |
| Workflow orchestration | Databricks Jobs |
| Streaming broker | Amazon MSK or Confluent Cloud |
| Serving database | Amazon RDS PostgreSQL or Aurora PostgreSQL |
| REST API | FastAPI on ECS, App Runner, EC2, or Kubernetes |
| Secrets | Databricks secrets backed by AWS Secrets Manager or Databricks secret scopes |
| Monitoring | Databricks job run logs, CloudWatch, RDS metrics |

## Storage Pattern

The current code uses Python `Path` objects and local-style paths such as:

```text
data/
warehouse/
checkpoints/
models/
```

Direct `s3://...` URIs do not behave like local filesystem paths with Python
`Path`. The recommended Databricks pattern is:

1. Create an S3 bucket or prefix for this lakehouse.
2. Register it as a Unity Catalog external location.
3. Create a Unity Catalog volume backed by that S3 location.
4. Use the volume path in environment variables:

```text
/Volumes/<catalog>/<schema>/<volume>/ai_powered_lakehouse
```

That keeps the existing project code working while storing the actual data in S3.

Example:

```env
LAKEHOUSE_ROOT=/Volumes/main/lakehouse/ai_powered_lakehouse
DATA_DIR=/Volumes/main/lakehouse/ai_powered_lakehouse/data
RAW_DATA_DIR=/Volumes/main/lakehouse/ai_powered_lakehouse/data/raw
WAREHOUSE_DIR=/Volumes/main/lakehouse/ai_powered_lakehouse/warehouse
CHECKPOINT_DIR=/Volumes/main/lakehouse/ai_powered_lakehouse/checkpoints
MODEL_DIR=/Volumes/main/lakehouse/ai_powered_lakehouse/models
```

## Databricks Bundle Files Added

This repository now includes:

```text
databricks.yml
databricks/resources/jobs.yml
.env.databricks.example
```

`databricks.yml` defines the bundle, workspace target, synced project files,
and deployment targets.

`databricks/resources/jobs.yml` defines:

- `lakehouse_batch_pipeline`
- `lakehouse_realtime_metrics_stream`
- `lakehouse_realtime_loader`

## Job Mapping

### Batch Pipeline Job

The Databricks batch job maps to the local medallion flow:

```text
generate_sample_data
-> ingest_bronze
-> transform_silver
-> build_gold
-> load_postgres
```

ML runs from Silver in the same job:

```text
transform_silver
-> train_demand_forecast
-> predict_demand
```

The default schedule is daily at 02:00 UTC and paused by default.

### Streaming Metrics Job

The streaming metrics job runs:

```text
streaming/realtime_aggregations.py
```

It reads Kafka events and writes:

```text
warehouse/gold/realtime_metrics
```

This job is long-running by design.

### Realtime Loader Job

The loader job runs:

```text
streaming/load_realtime_to_postgres.py
```

It is scheduled every minute and paused by default. It reads the Gold realtime
Delta table and loads `realtime_metrics` into PostgreSQL.

## Prerequisites

1. AWS account with S3, VPC, IAM, and optionally MSK/RDS access.
2. Databricks workspace on AWS.
3. Databricks CLI installed and authenticated.
4. S3 bucket/prefix for lakehouse storage.
5. Unity Catalog external location and volume, or a DBFS mount, backed by S3.
6. RDS/Aurora PostgreSQL database reachable from Databricks.
7. MSK/Kafka endpoint reachable from Databricks for live streaming.

## Databricks CLI Commands

Validate bundle:

```bash
databricks bundle validate -t dev
```

Deploy bundle:

```bash
databricks bundle deploy -t dev
```

Run the batch pipeline:

```bash
databricks bundle run -t dev lakehouse_batch_pipeline
```

Run the streaming metrics job:

```bash
databricks bundle run -t dev lakehouse_realtime_metrics_stream
```

Run the realtime loader once:

```bash
databricks bundle run -t dev lakehouse_realtime_loader
```

## Variables To Set

At deployment time, set these bundle variables in your Databricks configuration
or pass them through your target configuration:

```text
workspace_host
lakehouse_root
kafka_bootstrap_servers
postgres_host
postgres_db
postgres_user
```

Do not hard-code passwords in the bundle. Use Databricks secrets for:

```text
POSTGRES_PASSWORD
GROQ_API_KEY
Kafka credentials, if using SASL/SSL
```

## Important Follow-Up Work

This scaffold is the first cloud migration step. Before production use:

- Replace plain PostgreSQL password environment variables with Databricks secrets.
- Add IAM roles and Unity Catalog grants for the S3-backed volume.
- Add MSK security settings if the Kafka cluster requires SASL/SSL.
- Add a PostgreSQL read-only user for dashboard and GenAI queries.
- Deploy the FastAPI service near RDS and point Streamlit at `ANALYTICS_API_URL`.
- Add freshness and row-count audit tables.
- Replace delete/insert serving loads with staging-table swaps or bulk COPY.
- Add Databricks job alerts for failure and SLA misses.
- Decide whether the Streamlit dashboard remains Docker-hosted, moves to
  Databricks Apps, or is deployed separately.

## Why This Path

Databricks gives the smallest conceptual migration from this local project:

- The existing transformations are already Spark DataFrame jobs.
- The warehouse is already Delta Lake.
- The project already separates Bronze, Silver, and Gold.
- Databricks Jobs can orchestrate the same script boundaries as the local CLI.
- S3 becomes the durable storage layer under Delta tables.

This keeps the project explainable: local Docker proves the architecture, while
Databricks on AWS shows how it becomes a production lakehouse.
