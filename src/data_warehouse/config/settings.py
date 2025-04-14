"""Configuration settings for the data warehouse."""

import os
from pathlib import Path
from typing import Literal

from pydantic import Field, computed_field
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    """Data warehouse configuration settings."""

    # Project paths
    PROJECT_ROOT: Path = Field(
        default_factory=lambda: Path(
            os.environ.get(
                "DATA_WAREHOUSE_ROOT",  # First check if explicitly set in env
                # If not set, try to determine from file location or working directory
                os.path.abspath(
                    os.path.join(
                        os.path.dirname(os.path.abspath(__file__)),  # Config dir
                        "..",
                        "..",
                        "..",  # Up to project root
                    )
                ),
            )
        ),
        description=(
            "Absolute path to the project root. "
            "Resolution order: (1) DATA_WAREHOUSE_ROOT env var, "
            "(2) three levels up from this file. "
            "Set DATA_WAREHOUSE_ROOT to override in containers or production."
        ),
    )

    # Environment setting
    ENVIRONMENT: Literal["development", "testing", "production"] = Field(default="development")

    # Database settings
    POSTGRES_HOST: str = Field(default="localhost")
    POSTGRES_PORT: int = Field(default=5432)
    POSTGRES_USER: str = Field(default="postgres")
    POSTGRES_PASSWORD: str = Field(default="postgres")
    POSTGRES_DB: str = Field(default="data_warehouse")

    # MinIO settings
    MINIO_HOST: str = Field(default="localhost")
    MINIO_PORT: int = Field(default=9000)
    MINIO_ACCESS_KEY: str = Field(default="minioadmin")
    MINIO_SECRET_KEY: str = Field(default="minioadmin")
    MINIO_SECURE: bool = Field(default=False)

    # DBT settings
    DBT_TARGET: Literal["dev", "prod"] = Field(default="dev")

    # API settings
    API_HOST: str = Field(default="0.0.0.0")
    API_PORT: int = Field(default=8000)
    API_WORKERS: int = Field(default=1)

    # Logging
    LOG_LEVEL: Literal["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"] = Field(
        default="INFO",
    )
    LOG_FORMAT: str = Field(
        default=(
            "<green>{time:YYYY-MM-DD HH:mm:ss}</green> | "
            "<level>{level: <8}</level> | "
            "<cyan>{name}</cyan>:<cyan>{function}</cyan>:<cyan>{line}</cyan> - "
            "<level>{message}</level>"
        ),
    )

    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        extra="ignore",
        validate_default=True,
    )

    @computed_field
    @property
    def DATA_DIR(self) -> Path:
        """Get the data directory path."""
        return self.PROJECT_ROOT / "data"

    @computed_field
    @property
    def DUCKDB_PATH(self) -> Path:
        """Get the DuckDB database path."""
        return self.DATA_DIR / "warehouse.db"

    @computed_field
    @property
    def DBT_PROJECT_DIR(self) -> Path:
        """Get the DBT project directory path."""
        return self.PROJECT_ROOT / "dbt"

    @computed_field
    @property
    def DBT_PROFILES_DIR(self) -> Path:
        """Get the DBT profiles directory path."""
        return self.PROJECT_ROOT / "dbt/config"

    @computed_field
    @property
    def DAGSTER_HOME(self) -> Path:
        """Get the Dagster home directory path."""
        return self.PROJECT_ROOT / "dagster_home"


settings = Settings()
