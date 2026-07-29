"""Validate cloud environment settings without printing secrets."""

from __future__ import annotations

import shutil
import sys
from dataclasses import dataclass
from pathlib import Path
from urllib.parse import urlparse

from pydantic import ValidationError

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from config.settings import Settings


SECRET_KEYS = {
    "aws_secret_access_key",
    "databricks_token",
    "postgres_password",
    "groq_api_key",
}


@dataclass(frozen=True)
class ValidationMessage:
    """One validation result."""

    level: str
    message: str


def _present(value: str | None) -> bool:
    return bool(value and value.strip())


def _summarize(key: str, value: str | None) -> str:
    if not _present(value):
        return "missing"
    if key in SECRET_KEYS:
        return "set <redacted>"
    return f"set length={len(value or '')}"


def validate_settings(settings: Settings) -> list[ValidationMessage]:
    """Validate required cloud settings."""

    messages: list[ValidationMessage] = []
    required = {
        "aws_access_key_id": settings.aws_access_key_id,
        "aws_secret_access_key": settings.aws_secret_access_key,
        "aws_default_region": settings.aws_default_region,
        "s3_bucket_name": settings.s3_bucket_name,
        "databricks_host": settings.databricks_host,
        "databricks_token": settings.databricks_token,
        "databricks_workspace_path": settings.databricks_workspace_path,
        "postgres_host": settings.postgres_host,
        "postgres_db": settings.postgres_db,
        "postgres_user": settings.postgres_user,
        "postgres_password": settings.postgres_password,
    }

    for key, value in required.items():
        messages.append(ValidationMessage("info", f"{key}: {_summarize(key, value)}"))
        if not _present(value):
            messages.append(ValidationMessage("error", f"{key} is required."))

    if settings.databricks_host:
        parsed = urlparse(settings.databricks_host)
        if parsed.scheme != "https" or not parsed.netloc:
            messages.append(
                ValidationMessage(
                    "error",
                    "databricks_host must be a full HTTPS workspace URL.",
                )
            )

    if settings.s3_bucket_name and settings.s3_bucket_name.startswith("s3://"):
        messages.append(
            ValidationMessage("error", "s3_bucket_name should be only the bucket name.")
        )

    for key, prefix in {
        "s3_raw_prefix": settings.s3_raw_prefix,
        "s3_bronze_prefix": settings.s3_bronze_prefix,
        "s3_silver_prefix": settings.s3_silver_prefix,
        "s3_gold_prefix": settings.s3_gold_prefix,
    }.items():
        if prefix.startswith("s3://"):
            messages.append(
                ValidationMessage("error", f"{key} should be a prefix, not a full S3 URI.")
            )

    if settings.postgres_port <= 0:
        messages.append(ValidationMessage("error", "postgres_port must be positive."))

    for executable in ["aws", "databricks"]:
        if shutil.which(executable):
            messages.append(ValidationMessage("info", f"{executable} CLI: installed"))
        else:
            messages.append(
                ValidationMessage(
                    "warning",
                    f"{executable} CLI is not installed locally; cloud connectivity cannot be checked yet.",
                )
            )

    try:
        messages.append(
            ValidationMessage(
                "info",
                f"cloud_lakehouse_root: set length={len(settings.cloud_lakehouse_root)}",
            )
        )
    except ValueError as exc:
        messages.append(ValidationMessage("error", str(exc)))

    return messages


def main() -> int:
    """Run cloud configuration validation."""

    try:
        settings = Settings()
    except ValidationError as exc:
        print("ERROR: .env could not be parsed into project settings.")
        for error in exc.errors():
            print(f"ERROR: {'.'.join(str(part) for part in error['loc'])}: {error['msg']}")
        return 1

    messages = validate_settings(settings)
    for message in messages:
        print(f"{message.level.upper()}: {message.message}")

    return 1 if any(message.level == "error" for message in messages) else 0


if __name__ == "__main__":
    sys.exit(main())
