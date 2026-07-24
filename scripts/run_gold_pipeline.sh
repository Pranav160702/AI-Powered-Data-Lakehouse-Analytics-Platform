#!/usr/bin/env bash
set -euo pipefail

python lakehouse/gold/gold_pipeline.py
python database/load_gold_to_postgres.py
