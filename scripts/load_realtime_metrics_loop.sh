#!/usr/bin/env bash
set -euo pipefail

INTERVAL_SECONDS="${REALTIME_LOAD_INTERVAL_SECONDS:-60}"

echo "Starting real-time metrics loader loop with interval ${INTERVAL_SECONDS}s"

while true; do
  if python streaming/load_realtime_to_postgres.py; then
    echo "Loaded real-time metrics into PostgreSQL"
  else
    echo "Real-time metrics load skipped or failed; will retry"
  fi
  sleep "${INTERVAL_SECONDS}"
done
