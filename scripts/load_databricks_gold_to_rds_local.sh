#!/usr/bin/env bash
set -euo pipefail

PROJECT_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$PROJECT_ROOT"

if [[ ! -f .env ]]; then
  echo "Missing .env. Copy .env.example to .env and fill cloud values first." >&2
  exit 1
fi

set -a
# shellcheck disable=SC1091
. ./.env
set +a

LAKEHOUSE_ROOT="${DATABRICKS_LAKEHOUSE_ROOT:-/Volumes/workspace/default/ai_powered_lakehouse}"
TMP_ROOT="${TMP_ROOT:-/tmp/lakehouse_databricks_gold}"
TMP_WAREHOUSE="$TMP_ROOT/warehouse"

rm -rf "$TMP_ROOT"
mkdir -p "$TMP_WAREHOUSE"

echo "Copying Databricks Gold Delta tables to a local temporary path..."
databricks fs cp -r "dbfs:${LAKEHOUSE_ROOT}/warehouse/gold" "$TMP_WAREHOUSE/gold"

echo "Loading copied Gold tables into PostgreSQL from this machine..."
ENVIRONMENT=local WAREHOUSE_DIR="$TMP_WAREHOUSE" .venv/bin/python database/load_gold_to_postgres.py

echo "Gold tables loaded into PostgreSQL."
