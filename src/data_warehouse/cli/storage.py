"""Storage-related CLI commands."""

import asyncio
import sys
from pathlib import Path

import click
from loguru import logger

from data_warehouse.config.settings import settings
from data_warehouse.core.exceptions import DatabaseError, StorageError
from data_warehouse.core.storage import initialize_storage
from data_warehouse.utils.logger import setup_logger


@click.group()
def storage():
    """Storage-related commands."""
    pass


@storage.command("init")
@click.option(
    "--force",
    is_flag=True,
    help="Force reinitialization even if storage is already initialized",
)
def init_storage(force: bool):
    """Initialize all storage components (PostgreSQL, DuckDB, MinIO).

    This creates the necessary schemas, tables, and buckets for the data warehouse.
    """
    setup_logger()
    logger.info("Initializing storage components...")

    try:
        # Create data directory if it doesn't exist
        Path(settings.DATA_DIR).mkdir(parents=True, exist_ok=True)

        # Run the async initialization function
        asyncio.run(_init_storage(force))
        logger.info("Storage initialization complete")
    except (DatabaseError, StorageError) as e:
        logger.error(f"Storage initialization failed: {e}")
        sys.exit(1)
    except Exception as e:
        logger.error(f"Unexpected error during storage initialization: {e}")
        sys.exit(1)


async def _init_storage(force: bool):
    """Asynchronous implementation of storage initialization.

    Args:
        force: Whether to force reinitialization

    Raises:
        DatabaseError: If database initialization fails
        StorageError: If storage initialization fails
    """
    # TODO: Implement force reinitialization logic if needed
    await initialize_storage()


@storage.command("status")
def storage_status():
    """Check the status of all storage components."""
    setup_logger()
    logger.info("Checking storage status...")

    try:
        # Call async function to check all storage components
        asyncio.run(_check_storage_status())
        logger.info("Storage status check complete")
    except Exception as e:
        logger.error(f"Storage status check failed: {e}")
        sys.exit(1)


async def _check_storage_status():
    """Asynchronous implementation of storage status check."""
    # This is a placeholder for actual implementation
    # Import these inside the function to prevent circular imports
    from data_warehouse.core.storage.duckdb import get_duckdb_client
    from data_warehouse.core.storage.minio import get_minio_client
    from data_warehouse.core.storage.postgres import get_postgres_connection

    # Check PostgreSQL
    try:
        async with get_postgres_connection() as conn:
            # Execute a simple query to verify connection
            cursor = await conn.execute("SELECT version()")
            version = await cursor.fetchone()
            logger.info(f"PostgreSQL: Connected - {version[0]}")
    except Exception as e:
        logger.error(f"PostgreSQL: Connection failed - {e}")

    # Check DuckDB
    try:
        client = get_duckdb_client()
        result = client.execute_query("SELECT version()")
        version = result.fetchone()[0]
        logger.info(f"DuckDB: Connected - {version}")
    except Exception as e:
        logger.error(f"DuckDB: Connection failed - {e}")

    # Check MinIO
    try:
        client = get_minio_client()
        # List buckets to verify connection
        buckets = [bucket.name for bucket in client.client.list_buckets()]
        logger.info(f"MinIO: Connected - {len(buckets)} buckets found {buckets}")
    except Exception as e:
        logger.error(f"MinIO: Connection failed - {e}")


if __name__ == "__main__":
    storage()
