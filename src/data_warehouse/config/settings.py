"""Configuration settings for the data warehouse."""

from pathlib import Path
from typing import Literal

from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    """Data warehouse configuration settings."""

    # Project paths
    PROJECT_ROOT: Path = Field(default=Path(__file__).parent.parent.parent.parent)
    DATA_DIR: Path = Field(default=PROJECT_ROOT / "data")

    # Database settings
    POSTGRES_HOST: str = Field(default="localhost")
    POSTGRES_PORT: int = Field(default=5432)
    POSTGRES_USER: str = Field(default="postgres")
    POSTGRES_PASSWORD: str = Field(default="postgres")
    POSTGRES_DB: str = Field(default="data_warehouse")

    # DuckDB settings
    DUCKDB_PATH: Path = Field(default=DATA_DIR / "warehouse.db")

    # DBT settings
    DBT_PROJECT_DIR: Path = Field(default=PROJECT_ROOT / "dbt")
    DBT_PROFILES_DIR: Path = Field(default=PROJECT_ROOT / "dbt/config")
    DBT_TARGET: Literal["dev", "prod"] = Field(default="dev")

    # Dagster settings
    DAGSTER_HOME: Path = Field(default=PROJECT_ROOT / "dagster_home")

    # API settings
    API_HOST: str = Field(default="0.0.0.0")
    API_PORT: int = Field(default=8000)
    API_WORKERS: int = Field(default=1)

    # Logging
    LOG_LEVEL: Literal["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"] = Field(
        default="INFO",
    )
    LOG_FORMAT: str = Field(
        default="<green>{time:YYYY-MM-DD HH:mm:ss}</green> | "
        "<level>{level: <8}</level> | "
        "<cyan>{name}</cyan>:<cyan>{function}</cyan>:<cyan>{line}</cyan> - <level>{message}</level>",
    )

    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        extra="ignore",
        validate_default=True,
    )


settings = Settings()
