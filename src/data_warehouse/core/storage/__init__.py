"""Storage functionality for the data warehouse."""

from data_warehouse.core.exceptions import DatabaseError, StorageError
from data_warehouse.utils.logger import logger

# Import storage components
from .duckdb import (
    DuckDBClient,
    get_duckdb_client,
    get_duckdb_connection,
    initialize_duckdb,
)
from .minio import MinioClient, get_minio_client, initialize_minio
from .postgres import (
    create_table,
    execute_query,
    get_postgres_connection,
    get_postgres_cursor,
    initialize_postgres,
)


async def initialize_storage() -> None:
    """Initialize all storage components.

    Raises:
        DatabaseError: If database initialization fails
        StorageError: If storage initialization fails
    """
    try:
        # Initialize PostgreSQL
        logger.info("Initializing PostgreSQL storage...")
        await initialize_postgres()

        # Initialize DuckDB
        logger.info("Initializing DuckDB storage...")
        initialize_duckdb()

        # Initialize MinIO
        logger.info("Initializing MinIO storage...")
        initialize_minio()

        logger.info("Storage layer initialized successfully")
    except (DatabaseError, StorageError) as e:
        logger.error(f"Failed to initialize storage layer: {e}")
        raise
