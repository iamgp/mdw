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


# Configure logger on module import
setup_logger()
