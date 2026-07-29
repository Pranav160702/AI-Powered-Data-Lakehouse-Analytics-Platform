"""Run non-secret cloud connectivity checks for the lakehouse project."""

from __future__ import annotations

import argparse
import os
import shutil
import subprocess
import sys
from dataclasses import dataclass
from pathlib import Path

from sqlalchemy import create_engine, text
from sqlalchemy.exc import SQLAlchemyError

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from config.settings import Settings
from scripts.validate_cloud_config import validate_settings


@dataclass(frozen=True)
class CheckResult:
    """One connectivity check result."""

    name: str
    status: str
    detail: str


def run_command(args: list[str], timeout: int = 30) -> tuple[bool, str]:
    """Run a command and return a redacted success/failure summary."""

    if shutil.which(args[0]) is None:
        return False, f"{args[0]} CLI is not installed"
    try:
        completed = subprocess.run(
            args,
            cwd=PROJECT_ROOT,
            check=False,
            capture_output=True,
            text=True,
            timeout=timeout,
        )
    except subprocess.TimeoutExpired:
        return False, f"{args[0]} timed out after {timeout}s"

    if completed.returncode == 0:
        return True, "ok"
    output = (completed.stderr or completed.stdout).strip().splitlines()
    detail = output[-1] if output else f"{args[0]} exited with {completed.returncode}"
    return False, detail[:240]


def check_configuration(settings: Settings) -> CheckResult:
    """Validate required configuration values."""

    messages = validate_settings(settings)
    errors = [message.message for message in messages if message.level == "error"]
    if errors:
        return CheckResult("configuration", "FAIL", "; ".join(errors))
    return CheckResult("configuration", "PASS", "required cloud settings are present")


def export_cli_environment(settings: Settings) -> None:
    """Expose loaded settings to AWS and Databricks CLIs without printing secrets."""

    assignments = {
        "AWS_ACCESS_KEY_ID": settings.aws_access_key_id,
        "AWS_SECRET_ACCESS_KEY": settings.aws_secret_access_key,
        "AWS_DEFAULT_REGION": settings.aws_default_region,
        "DATABRICKS_HOST": settings.databricks_host,
        "DATABRICKS_TOKEN": settings.databricks_token,
    }
    for key, value in assignments.items():
        if value:
            os.environ[key] = value


def check_aws_identity() -> CheckResult:
    """Verify AWS credentials can call STS."""

    ok, detail = run_command(["aws", "sts", "get-caller-identity"], timeout=30)
    return CheckResult("aws-sts", "PASS" if ok else "FAIL", detail)


def check_s3(settings: Settings) -> CheckResult:
    """Verify the configured S3 bucket is reachable."""

    if not settings.s3_bucket_name:
        return CheckResult("s3-bucket", "FAIL", "S3_BUCKET_NAME is missing")
    ok, detail = run_command(["aws", "s3", "ls", settings.s3_uri()], timeout=30)
    return CheckResult("s3-bucket", "PASS" if ok else "FAIL", detail)


def check_databricks_user() -> CheckResult:
    """Verify Databricks CLI authentication."""

    ok, detail = run_command(["databricks", "current-user", "me"], timeout=30)
    return CheckResult("databricks-auth", "PASS" if ok else "FAIL", detail)


def check_databricks_bundle(settings: Settings) -> CheckResult:
    """Validate the Databricks bundle with the current cloud variables."""

    args = [
        "databricks",
        "bundle",
        "validate",
        "-t",
        "dev",
        "--var",
        f"lakehouse_root={settings.cloud_lakehouse_root}",
        "--var",
        f"postgres_host={settings.postgres_host}",
        "--var",
        f"postgres_db={settings.postgres_db}",
        "--var",
        f"postgres_user={settings.postgres_user}",
    ]
    ok, detail = run_command(args, timeout=60)
    return CheckResult("databricks-bundle", "PASS" if ok else "FAIL", detail)


def check_postgres(settings: Settings) -> CheckResult:
    """Verify this machine can connect to PostgreSQL."""

    engine = create_engine(settings.postgres_sqlalchemy_url)
    try:
        with engine.connect() as connection:
            connection.execute(text("SELECT 1"))
    except SQLAlchemyError as exc:
        return CheckResult("postgres-local", "FAIL", str(exc).splitlines()[0][:240])
    finally:
        engine.dispose()
    return CheckResult("postgres-local", "PASS", "SELECT 1 succeeded")


def parse_args() -> argparse.Namespace:
    """Parse CLI arguments."""

    parser = argparse.ArgumentParser(description="Check cloud connectivity.")
    parser.add_argument(
        "--skip-network",
        action="store_true",
        help="Only validate configuration and skip network calls.",
    )
    return parser.parse_args()


def main() -> int:
    """Run checks and print a concise status table."""

    args = parse_args()
    settings = Settings()
    export_cli_environment(settings)
    checks = [check_configuration(settings)]

    if not args.skip_network:
        checks.extend(
            [
                check_aws_identity(),
                check_s3(settings),
                check_databricks_user(),
                check_databricks_bundle(settings),
                check_postgres(settings),
            ]
        )

    for check in checks:
        print(f"{check.status:4} {check.name}: {check.detail}")

    return 1 if any(check.status == "FAIL" for check in checks) else 0


if __name__ == "__main__":
    sys.exit(main())
