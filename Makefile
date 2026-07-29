.PHONY: setup test generate-data bronze silver gold postgres-load dashboard streamlit ml start-full-demo streaming-up streaming-down airflow-up airflow-down databricks-validate databricks-deploy databricks-run-batch docker-build docker-up docker-down docker-logs kafka-topics

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

databricks-validate:
	databricks bundle validate -t dev

databricks-deploy:
	databricks bundle deploy -t dev

databricks-run-batch:
	databricks bundle run -t dev lakehouse_batch_pipeline

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
