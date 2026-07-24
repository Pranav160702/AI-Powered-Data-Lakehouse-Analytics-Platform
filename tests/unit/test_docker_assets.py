"""Unit tests for Docker and compose assets."""

from pathlib import Path

import yaml

PROJECT_ROOT = Path(__file__).resolve().parents[2]


def test_dockerfile_exists_and_runs_streamlit() -> None:
    """Dockerfile should build the Python app image and expose Streamlit."""

    dockerfile = PROJECT_ROOT / "Dockerfile"
    content = dockerfile.read_text(encoding="utf-8")

    assert "python:3.11-slim" in content
    assert "openjdk-17-jre-headless" in content
    assert "streamlit" in content


def test_compose_defines_core_services() -> None:
    """Docker Compose should include the required platform services."""

    compose = yaml.safe_load((PROJECT_ROOT / "docker-compose.yml").read_text(encoding="utf-8"))
    services = set(compose["services"])

    assert {
        "postgres",
        "zookeeper",
        "kafka",
        "spark-master",
        "spark-worker",
        "dashboard",
        "airflow-webserver",
        "airflow-scheduler",
    }.issubset(services)


def test_compose_services_have_health_checks_where_expected() -> None:
    """Externally-facing services should declare health checks."""

    compose = yaml.safe_load((PROJECT_ROOT / "docker-compose.yml").read_text(encoding="utf-8"))

    for service_name in ["postgres", "zookeeper", "kafka", "spark-master", "dashboard", "airflow-webserver"]:
        assert "healthcheck" in compose["services"][service_name]
