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

Batch flow:

```text
Synthetic / source data
-> Amazon S3 raw/
-> Databricks Bronze Delta
-> Databricks Silver Delta
-> Databricks Gold Delta
-> Amazon RDS PostgreSQL
-> FastAPI Analytics API
-> Streamlit Dashboard
-> GenAI SQL Assistant
```

Streaming flow:

```text
Event Generator
-> Kafka
-> Spark Structured Streaming
-> Bronze Event Delta on S3
-> Silver Event Delta on S3
-> Gold realtime_metrics Delta
-> Amazon RDS PostgreSQL
-> Streamlit Real-Time Dashboard
```

## Recommended AWS Services

| Need | Recommended Service |
| --- | --- |
| Lakehouse storage | Amazon S3 |
| Spark compute | Databricks serverless jobs |
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
LAKEHOUSE_ROOT=/Volumes/workspace/default/ai_powered_lakehouse
DATA_DIR=/Volumes/workspace/default/ai_powered_lakehouse/data
RAW_DATA_DIR=/Volumes/workspace/default/ai_powered_lakehouse/data/raw
WAREHOUSE_DIR=/Volumes/workspace/default/ai_powered_lakehouse/warehouse
CHECKPOINT_DIR=/Volumes/workspace/default/ai_powered_lakehouse/checkpoints
MODEL_DIR=/Volumes/workspace/default/ai_powered_lakehouse/models
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
5. Unity Catalog external location and volume, or a managed volume, backed by S3.
6. RDS/Aurora PostgreSQL database reachable from Databricks.
7. MSK/Kafka endpoint reachable from Databricks for live streaming.

## Local Validation

Validate `.env` before deploying. The validator confirms required AWS,
Databricks, and PostgreSQL settings are present without printing secret values:

```bash
make cloud-validate
```

This does not replace cloud connectivity checks. Install the AWS CLI and
Databricks CLI before running bundle commands.

## Databricks CLI Commands

The Make targets load `.env` and pass the required Databricks bundle variables.
Databricks Free Edition/serverless uses the managed volume path below by default:

```text
/Volumes/workspace/default/ai_powered_lakehouse
```

Validate bundle:

```bash
make databricks-validate
```

Create or update Databricks secrets:

```bash
make databricks-put-secrets
```

Deploy bundle:

```bash
make databricks-deploy
```

Run the batch pipeline:

```bash
make databricks-run-batch
```

Run the streaming metrics job:

```bash
databricks bundle run -t dev lakehouse_realtime_metrics_stream
```

Run the realtime loader once:

```bash
databricks bundle run -t dev lakehouse_realtime_loader
```

If Databricks serverless times out while connecting to RDS, the Databricks
job has reached the serving load step but RDS networking is blocking the
connection. Update the RDS security group/VPC rules to allow PostgreSQL port
`5432` from Databricks serverless networking, or copy Gold output from the
Databricks volume and load it from a machine that can reach RDS.

## Current Cloud Readiness

Validated:

- AWS CLI authentication.
- Databricks CLI authentication.
- S3 list/write/delete from the local machine.
- Databricks bundle validation and deployment.
- Databricks batch run through Bronze, Silver, Gold, training, and prediction.
- Local machine to Amazon RDS PostgreSQL.
- Streamlit dashboard reads the RDS serving tables.

Open blocker:

- Databricks serverless to Amazon RDS PostgreSQL still times out on port `5432`.
  This blocks the Databricks `load_postgres` and `lakehouse_realtime_loader`
  tasks until RDS networking allows Databricks serverless egress.

## Variables To Set

At deployment time, set these bundle variables in your Databricks configuration
or pass them through your target configuration:

```text
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
