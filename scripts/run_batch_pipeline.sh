#!/usr/bin/env bash
set -euo pipefail

python ingestion/batch_ingestion.py
python lakehouse/silver/silver_pipeline.py
