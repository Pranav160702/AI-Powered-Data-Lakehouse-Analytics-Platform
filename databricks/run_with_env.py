"""Run a project script in Databricks with task-level environment settings."""

from __future__ import annotations

import argparse
import os
import runpy
import sys
from pathlib import Path


def parse_args() -> argparse.Namespace:
    """Parse wrapper arguments."""

    parser = argparse.ArgumentParser(description="Run a bundle script with env vars.")
    parser.add_argument("--script", required=True, help="Script path relative to bundle root.")
    parser.add_argument(
        "--env",
        action="append",
        default=[],
        help="Environment assignment in KEY=VALUE form. Can be supplied multiple times.",
    )
    parser.add_argument(
        "--secret-env",
        action="append",
        default=[],
        help="Environment assignment in KEY=scope/key form resolved with Databricks secrets.",
    )
    parser.add_argument(
        "script_args",
        nargs=argparse.REMAINDER,
        help="Arguments forwarded to the target script after an optional -- separator.",
    )
    return parser.parse_args()


def apply_environment(assignments: list[str]) -> None:
    """Apply KEY=VALUE assignments to the current process environment."""

    for assignment in assignments:
        if "=" not in assignment:
            raise ValueError(f"Invalid environment assignment: {assignment}")
        key, value = assignment.split("=", 1)
        os.environ[key] = value


def get_databricks_secret(scope: str, key: str) -> str:
    """Read one Databricks secret from a Python file task."""

    try:
        from pyspark.dbutils import DBUtils
        from pyspark.sql import SparkSession
    except ModuleNotFoundError as exc:
        raise RuntimeError("Databricks secret resolution requires PySpark.") from exc

    spark = SparkSession.builder.getOrCreate()
    return DBUtils(spark).secrets.get(scope=scope, key=key)


def apply_secret_environment(assignments: list[str]) -> None:
    """Apply KEY=scope/key assignments from Databricks secrets."""

    for assignment in assignments:
        if "=" not in assignment:
            raise ValueError(f"Invalid secret environment assignment: {assignment}")
        env_key, secret_ref = assignment.split("=", 1)
        if "/" not in secret_ref:
            raise ValueError(f"Invalid secret reference: {secret_ref}")
        scope, secret_key = secret_ref.split("/", 1)
        os.environ[env_key] = get_databricks_secret(scope, secret_key)


def main() -> None:
    """Set environment and run the requested script."""

    args = parse_args()
    wrapper_path = Path(globals().get("__file__", sys.argv[0])).resolve()
    bundle_root = wrapper_path.parents[1]
    script_path = bundle_root / args.script
    if not script_path.exists():
        raise FileNotFoundError(f"Bundle script not found: {script_path}")

    apply_environment(args.env)
    apply_secret_environment(args.secret_env)

    if str(bundle_root) not in sys.path:
        sys.path.insert(0, str(bundle_root))

    forwarded_args = args.script_args
    if forwarded_args and forwarded_args[0] == "--":
        forwarded_args = forwarded_args[1:]
    sys.argv = [str(script_path), *forwarded_args]
    runpy.run_path(str(script_path), run_name="__main__")


if __name__ == "__main__":
    main()
