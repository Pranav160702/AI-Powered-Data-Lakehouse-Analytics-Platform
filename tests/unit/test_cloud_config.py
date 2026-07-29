"""Unit tests for cloud configuration helpers."""

from __future__ import annotations

from config.settings import Settings
from scripts.validate_cloud_config import validate_settings


def test_settings_builds_s3_uris() -> None:
    """Settings should build normalized S3 URIs from bucket and prefixes."""

    settings = Settings(s3_bucket_name="lakehouse-bucket", _env_file=None)

    assert settings.s3_uri() == "s3://lakehouse-bucket"
    assert settings.s3_uri("/raw/orders/") == "s3://lakehouse-bucket/raw/orders"
    assert settings.cloud_lakehouse_root == "s3://lakehouse-bucket"


def test_settings_prefers_databricks_lakehouse_root() -> None:
    """A Databricks lakehouse path should override the derived S3 root."""

    settings = Settings(
        s3_bucket_name="lakehouse-bucket",
        databricks_lakehouse_root="/Volumes/main/lakehouse/project/",
        _env_file=None,
    )

    assert settings.cloud_lakehouse_root == "/Volumes/main/lakehouse/project"


def test_cloud_validator_flags_missing_required_values() -> None:
    """Cloud validation should report missing required cloud values."""

    settings = Settings(
        aws_access_key_id=None,
        aws_secret_access_key=None,
        aws_default_region=None,
        s3_bucket_name=None,
        databricks_host=None,
        databricks_token=None,
        databricks_workspace_path=None,
        postgres_password="",
        _env_file=None,
    )

    messages = validate_settings(settings)
    errors = [message.message for message in messages if message.level == "error"]

    assert "aws_access_key_id is required." in errors
    assert "s3_bucket_name is required." in errors
    assert "databricks_host is required." in errors
    assert "postgres_password is required." in errors
