"""Logging configuration for the data warehouse."""

import sys
from typing import Any, Literal

from loguru import logger

from data_warehouse.config.settings import settings

LogLevel = Literal["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"]


def setup_logger(verbose: bool = False) -> None:
    """Configure the logger with project settings.

    Args:
        verbose: If True, set log level to DEBUG regardless of settings
    """
    # Determine the log level based on verbose flag or settings
    log_level: LogLevel = "DEBUG" if verbose else settings.LOG_LEVEL

    config: dict[str, Any] = {
        "handlers": [
            {
                "sink": sys.stdout,
                "format": settings.LOG_FORMAT,
                "level": log_level,
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
            level=log_level,
            format=settings.LOG_FORMAT,
        )

    logger.debug("Logger configured successfully")


# Configure logger once on module import - will be reconfigured when CLI runs
setup_logger()


def get_command_logger(module_name: str):
    """Get a logger instance with context for CLI commands.

    Args:
        module_name: The name of the module using the logger

    Returns:
        A logger instance with context
    """
    return logger.bind(module=module_name)
