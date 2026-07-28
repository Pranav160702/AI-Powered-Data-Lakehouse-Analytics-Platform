#!/usr/bin/env bash
set -euo pipefail

python scripts/generate_sample_data.py
python ingestion/batch_ingestion.py
python lakehouse/silver/silver_pipeline.py
