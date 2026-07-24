#!/usr/bin/env bash
set -euo pipefail

BOOTSTRAP_SERVERS="${KAFKA_BOOTSTRAP_SERVERS:-localhost:9092}"
PARTITIONS="${KAFKA_PARTITIONS:-3}"
REPLICATION_FACTOR="${KAFKA_REPLICATION_FACTOR:-1}"
TOPICS=(
  "${KAFKA_CUSTOMER_EVENTS_TOPIC:-customer-events}"
  "${KAFKA_ORDER_EVENTS_TOPIC:-order-events}"
  "${KAFKA_PAYMENT_EVENTS_TOPIC:-payment-events}"
  "${KAFKA_INVENTORY_EVENTS_TOPIC:-inventory-events}"
)

for topic in "${TOPICS[@]}"; do
  kafka-topics.sh \
    --bootstrap-server "${BOOTSTRAP_SERVERS}" \
    --create \
    --if-not-exists \
    --topic "${topic}" \
    --partitions "${PARTITIONS}" \
    --replication-factor "${REPLICATION_FACTOR}"
done

echo "Kafka topics are ready on ${BOOTSTRAP_SERVERS}"
