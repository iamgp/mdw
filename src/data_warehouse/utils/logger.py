"""Logging configuration for the data warehouse."""

import sys
from typing import Any

from loguru import logger

from data_warehouse.config.settings import settings


def setup_logger() -> None:
    """Configure the logger with project settings."""
    config: dict[str, Any] = {
        "handlers": [
            {
                "sink": sys.stdout,
                "format": settings.LOG_FORMAT,
                "level": settings.LOG_LEVEL,
                "colorize": True,
            },
        ],
    }

    # Remove default handler and apply our configuration
    logger.remove()
    logger.configure(**config)

    # Add file logging in non-development environments
    if settings.ENVIRONMENT != "development":
        log_file = settings.PROJECT_ROOT / "logs" / "data_warehouse.log"
        log_file.parent.mkdir(parents=True, exist_ok=True)

        logger.add(
            str(log_file),
            rotation="10 MB",
            retention="1 week",
            compression="zip",
            level=settings.LOG_LEVEL,
            format=settings.LOG_FORMAT,
        )

    logger.debug("Logger configured successfully")


# Configure logger on module import
setup_logger()
