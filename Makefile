.PHONY: setup test generate-data bronze silver gold postgres-load dashboard streamlit ml start-full-demo streaming-up streaming-down airflow-up airflow-down cloud-validate cloud-check databricks-put-secrets databricks-validate databricks-deploy databricks-run-batch databricks-load-gold-local docker-build docker-up docker-down docker-logs kafka-topics

setup:
	python3 -m venv .venv
	.venv/bin/python -m pip install --upgrade pip
	.venv/bin/python -m pip install -r requirements.txt

test:
	.venv/bin/python -m pytest

generate-data:
	.venv/bin/python scripts/generate_sample_data.py

bronze:
	.venv/bin/python ingestion/batch_ingestion.py

silver:
	.venv/bin/python lakehouse/silver/silver_pipeline.py

gold:
	.venv/bin/python lakehouse/gold/gold_pipeline.py

postgres-load:
	.venv/bin/python database/load_gold_to_postgres.py

dashboard streamlit:
	.venv/bin/streamlit run dashboard/app.py

ml:
	scripts/run_ml_pipeline.sh

start-full-demo:
	scripts/start_full_demo.sh

streaming-up:
	docker compose --profile streaming up -d --build event-producer event-bronze event-silver realtime-metrics realtime-loader

streaming-down:
	docker compose --profile streaming stop event-producer event-bronze event-silver realtime-metrics realtime-loader

airflow-up:
	docker compose --profile orchestration up -d --build airflow-webserver airflow-scheduler

airflow-down:
	docker compose --profile orchestration stop airflow-webserver airflow-scheduler

cloud-validate:
	.venv/bin/python scripts/validate_cloud_config.py

cloud-check:
	.venv/bin/python scripts/check_cloud_connectivity.py $(CLOUD_CHECK_ARGS)

databricks-put-secrets:
	set -a; . ./.env; set +a; databricks secrets create-scope lakehouse || true; databricks secrets put-secret lakehouse postgres_password --string-value "$$POSTGRES_PASSWORD"

databricks-validate:
	set -a; . ./.env; set +a; databricks bundle validate -t dev --var lakehouse_root="$${DATABRICKS_LAKEHOUSE_ROOT:-/Volumes/workspace/default/ai_powered_lakehouse}" --var postgres_host="$$POSTGRES_HOST" --var postgres_db="$$POSTGRES_DB" --var postgres_user="$$POSTGRES_USER" --var postgres_sslmode="$${POSTGRES_SSLMODE:-require}"

databricks-deploy:
	set -a; . ./.env; set +a; databricks bundle deploy -t dev --var lakehouse_root="$${DATABRICKS_LAKEHOUSE_ROOT:-/Volumes/workspace/default/ai_powered_lakehouse}" --var postgres_host="$$POSTGRES_HOST" --var postgres_db="$$POSTGRES_DB" --var postgres_user="$$POSTGRES_USER" --var postgres_sslmode="$${POSTGRES_SSLMODE:-require}"

databricks-run-batch:
	set -a; . ./.env; set +a; databricks bundle run -t dev lakehouse_batch_pipeline --var lakehouse_root="$${DATABRICKS_LAKEHOUSE_ROOT:-/Volumes/workspace/default/ai_powered_lakehouse}" --var postgres_host="$$POSTGRES_HOST" --var postgres_db="$$POSTGRES_DB" --var postgres_user="$$POSTGRES_USER" --var postgres_sslmode="$${POSTGRES_SSLMODE:-require}"

databricks-load-gold-local:
	scripts/load_databricks_gold_to_rds_local.sh

kafka-topics:
	scripts/create_kafka_topics.sh

docker-build:
	docker compose build

docker-up:
	docker compose up -d

docker-down:
	docker compose down

docker-logs:
	docker compose logs -f
