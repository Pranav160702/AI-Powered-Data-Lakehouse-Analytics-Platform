.PHONY: setup test generate-data bronze silver gold postgres-load dashboard streamlit ml start-full-demo docker-build docker-up docker-down docker-logs kafka-topics

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
