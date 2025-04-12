"""Database connection utilities."""

import contextlib
from collections.abc import AsyncGenerator, Generator

import duckdb
import psycopg
from psycopg import AsyncConnection, AsyncCursor

from data_warehouse.config.settings import settings
from data_warehouse.core.exceptions import DatabaseError
from data_warehouse.utils.logger import logger


@contextlib.asynccontextmanager
async def get_postgres_connection() -> AsyncGenerator[AsyncConnection[dict], None]:
    """Get a PostgreSQL connection.

    Yields:
        An async PostgreSQL connection

    Raises:
        DatabaseError: If connection fails

    """
    try:
        conn = await psycopg.AsyncConnection.connect(
            host=settings.POSTGRES_HOST,
            port=settings.POSTGRES_PORT,
            user=settings.POSTGRES_USER,
            password=settings.POSTGRES_PASSWORD,
            dbname=settings.POSTGRES_DB,
            row_factory=dict_row_factory,
        )
        yield conn
    except Exception as e:
        logger.error(f"Failed to connect to PostgreSQL: {e}")
        raise DatabaseError("Failed to connect to PostgreSQL", e)
    finally:
        if "conn" in locals():
            await conn.close()


@contextlib.asynccontextmanager
async def get_postgres_cursor() -> AsyncGenerator[AsyncCursor[dict], None]:
    """Get a PostgreSQL cursor.

    Yields:
        An async PostgreSQL cursor

    Raises:
        DatabaseError: If cursor creation fails

    """
    async with get_postgres_connection() as conn:
        try:
            async with conn.cursor() as cur:
                yield cur
        except Exception as e:
            logger.error(f"Failed to create PostgreSQL cursor: {e}")
            raise DatabaseError("Failed to create PostgreSQL cursor", e)


@contextlib.contextmanager
def get_duckdb_connection() -> Generator[duckdb.DuckDBPyConnection, None, None]:
    """Get a DuckDB connection.

    Yields:
        A DuckDB connection

    Raises:
        DatabaseError: If connection fails

    """
    try:
        # Ensure data directory exists
        settings.DATA_DIR.mkdir(parents=True, exist_ok=True)

        # Connect to DuckDB
        conn = duckdb.connect(str(settings.DUCKDB_PATH))
        yield conn
    except Exception as e:
        logger.error(f"Failed to connect to DuckDB: {e}")
        raise DatabaseError("Failed to connect to DuckDB", e)
    finally:
        if "conn" in locals():
            conn.close()


def dict_row_factory(cursor: AsyncCursor[dict]) -> dict:
    """Create a dictionary row factory for PostgreSQL cursors.

    Args:
        cursor: The cursor to create a row factory for

    Returns:
        A function that converts rows to dictionaries

    """
    return lambda values: {
        desc.name: value
        for desc, value in zip(cursor.description, values, strict=False)
        if desc is not None
    }
