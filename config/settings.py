"""Application settings loaded from environment variables."""

from functools import lru_cache
from pathlib import Path

from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    """Typed runtime configuration for local and container execution."""

    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        extra="ignore",
        case_sensitive=False,
        protected_namespaces=("settings_",),
    )

    app_name: str = Field(default="AI-Powered Data Lakehouse Analytics Platform")
    environment: str = Field(default="local")
    log_level: str = Field(default="INFO")

    data_dir: Path = Field(default=Path("data"))
    raw_data_dir: Path = Field(default=Path("data/raw"))
    warehouse_dir: Path = Field(default=Path("warehouse"))
    checkpoint_dir: Path = Field(default=Path("checkpoints"))
    model_dir: Path = Field(default=Path("models"))

    spark_master_url: str = Field(default="local[*]")
    spark_driver_memory: str = Field(default="2g")
    spark_executor_memory: str = Field(default="2g")
    spark_sql_shuffle_partitions: int = Field(default=8, ge=1)

    kafka_bootstrap_servers: str = Field(default="localhost:9092")
    kafka_customer_events_topic: str = Field(default="customer-events")
    kafka_order_events_topic: str = Field(default="order-events")
    kafka_payment_events_topic: str = Field(default="payment-events")
    kafka_inventory_events_topic: str = Field(default="inventory-events")
    streaming_watermark_delay: str = Field(default="10 minutes")
    streaming_window_duration: str = Field(default="5 minutes")
    streaming_slide_duration: str = Field(default="1 minute")

    postgres_host: str = Field(default="localhost")
    postgres_port: int = Field(default=5432)
    postgres_db: str = Field(default="lakehouse_analytics")
    postgres_user: str = Field(default="lakehouse_user")
    postgres_password: str = Field(default="change_me")
    postgres_pool_size: int = Field(default=5, ge=1)
    postgres_max_overflow: int = Field(default=10, ge=0)
    postgres_connect_timeout: int = Field(default=10, ge=1)

    groq_api_key: str | None = Field(default=None)
    groq_model: str = Field(default="llama-3.1-70b-versatile")

    @property
    def project_root(self) -> Path:
        """Return the repository root inferred from this file location."""

        return Path(__file__).resolve().parents[1]

    def resolve_path(self, path: Path) -> Path:
        """Resolve a configured relative path from the project root."""

        return path if path.is_absolute() else self.project_root / path

    @property
    def postgres_sqlalchemy_url(self) -> str:
        """Return the SQLAlchemy URL for PostgreSQL without logging it."""

        return (
            f"postgresql+psycopg2://{self.postgres_user}:{self.postgres_password}"
            f"@{self.postgres_host}:{self.postgres_port}/{self.postgres_db}"
        )


@lru_cache
def get_settings() -> Settings:
    """Return cached application settings."""

    return Settings()
