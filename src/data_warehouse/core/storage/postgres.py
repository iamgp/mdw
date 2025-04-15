"""PostgreSQL storage functionality for the data warehouse."""

import contextlib
from collections.abc import AsyncGenerator
from typing import Any, cast

import psycopg
from psycopg import AsyncConnection, AsyncCursor
from psycopg.rows import dict_row

from data_warehouse.config.settings import settings
from data_warehouse.core.exceptions import DatabaseError
from data_warehouse.utils.logger import logger


@contextlib.asynccontextmanager
async def get_postgres_connection() -> AsyncGenerator[AsyncConnection[Any], None]:
    """Get a PostgreSQL connection.

    Yields:
        An async PostgreSQL connection

    Raises:
        DatabaseError: If connection fails
    """
    conn = None
    try:
        conn = await psycopg.AsyncConnection.connect(
            host=settings.POSTGRES_HOST,
            port=settings.POSTGRES_PORT,
            user=settings.POSTGRES_USER,
            password=settings.POSTGRES_PASSWORD,
            dbname=settings.POSTGRES_DB,
            row_factory=dict_row,  # type: ignore[arg-type]
            # Pyright: dict_row is valid for async connections, but type stubs are too strict.
        )
        logger.debug("PostgreSQL connection established")
        yield conn
    except Exception as e:
        logger.error(f"Failed to connect to PostgreSQL: {e}")
        raise DatabaseError("Failed to connect to PostgreSQL") from e
    finally:
        if conn is not None:
            logger.debug("Closing PostgreSQL connection")
            await conn.close()


@contextlib.asynccontextmanager
async def get_postgres_cursor() -> AsyncGenerator[AsyncCursor[Any], None]:
    """Get a PostgreSQL cursor.

    Yields:
        An async PostgreSQL cursor

    Raises:
        DatabaseError: If connection fails
    """
    async with get_postgres_connection() as conn:
        async with conn.cursor() as cursor:
            logger.debug("PostgreSQL cursor created")
            yield cursor


async def execute_query(
    query: str, params: dict[str, Any] | None = None
) -> list[dict[str, Any]]:
    """Execute a query on PostgreSQL.

    Args:
        query: The SQL query to execute
        params: Query parameters

    Returns:
        List of results as dictionaries

    Raises:
        DatabaseError: If query execution fails
    """
    try:
        async with get_postgres_cursor() as cursor:
            await cursor.execute(query, params)  # type: ignore[arg-type]
            # Pyright: query is a string, but stubs expect LiteralString/Query. This is safe in our usage.
            if cursor.description:
                return cast(list[dict[str, Any]], await cursor.fetchall())
            return []
    except Exception as e:
        logger.error(f"Failed to execute PostgreSQL query: {e}")
        raise DatabaseError(f"Failed to execute PostgreSQL query: {e}") from e


async def create_table(
    table_name: str,
    columns: dict[str, str],
    if_not_exists: bool = True,
    primary_key: str | None = None,
) -> None:
    """Create a table in PostgreSQL.

    Args:
        table_name: Name of the table to create
        columns: Dictionary of column names and their SQL types
        if_not_exists: Whether to add IF NOT EXISTS clause
        primary_key: Column name to use as primary key

    Raises:
        DatabaseError: If table creation fails
    """
    existence_clause = "IF NOT EXISTS " if if_not_exists else ""
    column_definitions = [f"{name} {data_type}" for name, data_type in columns.items()]

    if primary_key:
        column_definitions.append(f"PRIMARY KEY ({primary_key})")

    definition_str = ", ".join(column_definitions)
    query = f"CREATE TABLE {existence_clause}{table_name} ({definition_str})"

    try:
        await execute_query(query)
        logger.info(f"Table {table_name} created or already exists")
    except Exception as e:
        logger.error(f"Failed to create table {table_name}: {e}")
        raise DatabaseError(f"Failed to create table {table_name}") from e


async def initialize_postgres() -> None:
    """Initialize PostgreSQL with required schemas and tables.

    Raises:
        DatabaseError: If initialization fails
    """
    try:
        # Create schema if it doesn't exist
        await execute_query("CREATE SCHEMA IF NOT EXISTS warehouse")
        logger.info("PostgreSQL schema 'warehouse' created or already exists")

        # Create metadata table
        await create_table(
            "warehouse.datasets",
            {
                "id": "SERIAL",
                "name": "VARCHAR(255) NOT NULL",
                "description": "TEXT",
                "created_at": "TIMESTAMP DEFAULT CURRENT_TIMESTAMP",
                "updated_at": "TIMESTAMP DEFAULT CURRENT_TIMESTAMP",
                "schema": "JSONB",
                "tags": "JSONB",
                "metadata": "JSONB",
            },
            primary_key="id",
        )

        # Create data partitioning table
        await create_table(
            "warehouse.partitions",
            {
                "id": "SERIAL",
                "dataset_id": "INTEGER NOT NULL REFERENCES warehouse.datasets(id)",
                "partition_key": "VARCHAR(255) NOT NULL",
                "min_value": "VARCHAR(255)",
                "max_value": "VARCHAR(255)",
                "row_count": "INTEGER",
                "created_at": "TIMESTAMP DEFAULT CURRENT_TIMESTAMP",
                "storage_location": "VARCHAR(255)",
                "storage_type": "VARCHAR(50)",
                "metadata": "JSONB",
            },
            primary_key="id",
        )

        logger.info("PostgreSQL tables initialized successfully")
    except Exception as e:
        logger.error(f"Failed to initialize PostgreSQL: {e}")
        raise DatabaseError("Failed to initialize PostgreSQL") from e
