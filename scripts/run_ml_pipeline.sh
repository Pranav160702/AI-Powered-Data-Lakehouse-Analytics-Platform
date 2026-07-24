#!/usr/bin/env bash
set -euo pipefail

python ml/feature_engineering_cli.py
python ml/train_demand_forecast.py
python ml/predict_demand.py
