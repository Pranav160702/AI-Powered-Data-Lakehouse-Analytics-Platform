#!/usr/bin/env bash
set -euo pipefail

TARGET="${1:-silver}"

if [[ "${TARGET}" == "metrics" ]]; then
  python streaming/realtime_aggregations.py
elif [[ "${TARGET}" == "load-metrics" ]]; then
  python streaming/load_realtime_to_postgres.py
else
  python streaming/stream_processor.py --target "${TARGET}"
fi
