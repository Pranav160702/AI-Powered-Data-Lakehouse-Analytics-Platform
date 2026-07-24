"""Checkpoint path helpers for streaming jobs."""

from pathlib import Path

from config.settings import get_settings


def checkpoint_path(job_name: str) -> Path:
    """Return and create a checkpoint path for a streaming job."""

    settings = get_settings()
    path = settings.resolve_path(settings.checkpoint_dir) / "streaming" / job_name
    path.mkdir(parents=True, exist_ok=True)
    return path
