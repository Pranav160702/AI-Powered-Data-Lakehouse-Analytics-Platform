"""Smoke tests for the centralized Spark configuration."""

from config.spark_config import spark_session


def test_spark_session_counts_rows() -> None:
    """Verify Spark can start and execute a simple DataFrame action."""

    with spark_session(app_name="phase1-spark-test") as spark:
        df = spark.createDataFrame([(1, "ok"), (2, "ready")], ["id", "status"])
        assert df.count() == 2
