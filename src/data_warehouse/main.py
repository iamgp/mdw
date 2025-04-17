"""Main entry point for data warehouse application."""

import asyncio
import sys

from loguru import logger

from data_warehouse.config.settings import settings
from data_warehouse.core.storage import initialize_storage
from data_warehouse.utils.logger import setup_logger


async def startup() -> None:
    """Initialize and start the data warehouse application.

    Returns:
        None
    """
    logger.info("Startup sequence initiated")
    # Setup logging configuration
    setup_logger()
    logger.info(f"Starting data warehouse in {settings.ENVIRONMENT} environment")

    # Initialize all storage components
    logger.info("Initializing storage layer...")
    await initialize_storage()

    # Add additional initialization here as needed
    # e.g., API server, scheduled jobs, etc.

    logger.info("Data warehouse initialization complete")
    logger.info("Startup sequence complete")


def main() -> None:
    """Main entry point for the application."""
    logger.info("Main entry point called")
    try:
        asyncio.run(startup())

        # Keep the application running in production mode
        if settings.ENVIRONMENT == "production":
            # In a real app, you'd run the API server here
            logger.info("Data warehouse running...")
            # Run forever until interrupted
            asyncio.run(asyncio.Event().wait())
    except KeyboardInterrupt:
        logger.info("Data warehouse shutting down...")
    except Exception as e:
        logger.error(f"Failed to start data warehouse: {e}")
        sys.exit(1)
    logger.info("Main entry point complete")


if __name__ == "__main__":
    main()
