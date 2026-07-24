"""Local model registry helpers for versioned ML artifacts."""

from __future__ import annotations

import json
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import joblib

from config.settings import get_settings


def model_version() -> str:
    """Return a filesystem-safe model version timestamp."""

    return datetime.now(UTC).strftime("%Y%m%d_%H%M%S")


def model_dir() -> Path:
    """Return and create the configured model artifact directory."""

    settings = get_settings()
    path = settings.resolve_path(settings.model_dir)
    path.mkdir(parents=True, exist_ok=True)
    return path


def save_model_bundle(bundle: dict[str, Any], version: str | None = None) -> Path:
    """Persist a versioned model bundle and update the latest pointer."""

    resolved_version = version or model_version()
    path = model_dir() / f"demand_forecast_{resolved_version}.joblib"
    joblib.dump(bundle, path)
    latest = model_dir() / "demand_forecast_latest.joblib"
    joblib.dump(bundle, latest)
    return path


def load_latest_model_bundle() -> dict[str, Any]:
    """Load the latest demand forecasting model bundle."""

    path = model_dir() / "demand_forecast_latest.joblib"
    if not path.exists():
        raise FileNotFoundError("No trained demand forecasting model found.")
    return joblib.load(path)


def save_json_artifact(payload: dict[str, Any], file_name: str) -> Path:
    """Save a JSON artifact under the model directory."""

    path = model_dir() / file_name
    path.write_text(json.dumps(payload, indent=2, default=str), encoding="utf-8")
    return path
