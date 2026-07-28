#!/usr/bin/env bash
set -euo pipefail

LIVE_STREAMING="${LIVE_STREAMING:-false}"

COMPOSE_SERVICES=(
  postgres
  zookeeper
  kafka
  spark-master
  spark-worker
  dashboard
)

echo "Starting core Docker services..."
docker compose up -d --build "${COMPOSE_SERVICES[@]}"

echo "Waiting for services to become healthy..."
for attempt in {1..45}; do
  postgres_status="$(docker inspect -f '{{.State.Health.Status}}' lakehouse-postgres 2>/dev/null || true)"
  dashboard_status="$(docker inspect -f '{{.State.Health.Status}}' lakehouse-dashboard 2>/dev/null || true)"
  kafka_status="$(docker inspect -f '{{.State.Health.Status}}' lakehouse-kafka 2>/dev/null || true)"
  zookeeper_status="$(docker inspect -f '{{.State.Health.Status}}' lakehouse-zookeeper 2>/dev/null || true)"

  if [[ "${postgres_status}" == "healthy" \
    && "${dashboard_status}" == "healthy" \
    && "${kafka_status}" == "healthy" \
    && "${zookeeper_status}" == "healthy" ]]; then
    break
  fi

  if [[ "${attempt}" == "45" ]]; then
    echo "Timed out waiting for services."
    docker compose ps
    exit 1
  fi

  sleep 5
done

echo "Creating Kafka topics..."
docker compose exec -T kafka kafka-topics \
  --bootstrap-server localhost:9092 \
  --create \
  --if-not-exists \
  --topic customer-events \
  --partitions 3 \
  --replication-factor 1
docker compose exec -T kafka kafka-topics \
  --bootstrap-server localhost:9092 \
  --create \
  --if-not-exists \
  --topic order-events \
  --partitions 3 \
  --replication-factor 1
docker compose exec -T kafka kafka-topics \
  --bootstrap-server localhost:9092 \
  --create \
  --if-not-exists \
  --topic payment-events \
  --partitions 3 \
  --replication-factor 1
docker compose exec -T kafka kafka-topics \
  --bootstrap-server localhost:9092 \
  --create \
  --if-not-exists \
  --topic inventory-events \
  --partitions 3 \
  --replication-factor 1

echo "Generating source data..."
docker compose exec -T dashboard python scripts/generate_sample_data.py

echo "Running Bronze ingestion..."
docker compose exec -T dashboard python ingestion/batch_ingestion.py

echo "Running Silver transformations..."
docker compose exec -T dashboard python lakehouse/silver/silver_pipeline.py

echo "Running Gold aggregations..."
docker compose exec -T dashboard python lakehouse/gold/gold_pipeline.py

echo "Loading Gold tables into PostgreSQL..."
docker compose exec -T dashboard python database/load_gold_to_postgres.py

echo "Training demand forecast model..."
docker compose exec -T dashboard python ml/train_demand_forecast.py

echo "Generating demand forecasts..."
docker compose exec -T dashboard python ml/predict_demand.py

if [[ "${LIVE_STREAMING}" == "true" ]]; then
  echo "Starting live streaming services..."
  docker compose --profile streaming up -d --build event-producer event-bronze event-silver realtime-metrics realtime-loader
  echo "Live streaming services are running. Real-time metrics will refresh periodically."
else
  echo "Seeding dashboard real-time metrics..."
  docker compose exec -T dashboard python scripts/seed_realtime_metrics.py
fi

echo "Clearing Streamlit cache by restarting dashboard..."
docker compose restart dashboard

echo "Ready."
echo "Dashboard: http://localhost:8501"
if [[ "${LIVE_STREAMING}" == "true" ]]; then
  echo "Streaming logs: docker compose --profile streaming logs -f event-producer event-bronze event-silver realtime-metrics realtime-loader"
fi
