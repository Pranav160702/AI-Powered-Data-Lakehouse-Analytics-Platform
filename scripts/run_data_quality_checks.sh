#!/usr/bin/env bash
set -euo pipefail

python -m pytest tests/unit/test_batch_schemas.py tests/unit/test_database_serving.py
