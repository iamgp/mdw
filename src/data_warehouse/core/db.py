"""Database connection utilities."""

import contextlib
import logging
from collections.abc import AsyncGenerator, Callable, Generator
from pathlib import Path
from typing import Any

import duckdb
import psycopg
from psycopg import AsyncCursor


# Provide minimal local config and logger if missing
# Local fallback settings/logger/DatabaseError only (no unresolved imports)
class Settings:
    POSTGRES_HOST: str = "localhost"
    POSTGRES_PORT: int = 5432
    POSTGRES_USER: str = "user"
    POSTGRES_PASSWORD: str = "password"
    POSTGRES_DB: str = "db"
    DATA_DIR: Path = Path(".")
    DUCKDB_PATH: str = ":memory:"


settings = Settings()


class DatabaseError(Exception):
    pass


logger = logging.getLogger(__name__)


@contextlib.asynccontextmanager
async def get_postgres_connection() -> AsyncGenerator[Any, None]:
    conn: Any | None = None  # type: ignore
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
        )
        yield conn
    except Exception as e:
        logger.error(f"Failed to connect to PostgreSQL: {e}")  # type: ignore
        raise DatabaseError("Failed to connect to PostgreSQL") from e  # type: ignore
    finally:
        if conn is not None:
            await conn.close()


@contextlib.asynccontextmanager
async def get_postgres_cursor() -> AsyncGenerator[AsyncCursor[dict], None]:  # type: ignore[reportUnknownParameterType]
    # Pyright: Return type is partially unknown due to generic dict.
    """Get a PostgreSQL cursor.

    Yields:
        An async PostgreSQL cursor

    Raises:
        DatabaseError: If cursor creation fails

    """
    async with get_postgres_connection() as conn:  # type: ignore
        # Pyright: conn type is partially unknown.
        try:
            async with conn.cursor() as cur:  # type: ignore
                # Pyright: cur type is partially unknown.
                yield cur
        except Exception as e:
            logger.error(f"Failed to create PostgreSQL cursor: {e}")  # type: ignore
            raise DatabaseError("Failed to create PostgreSQL cursor") from e  # type: ignore


@contextlib.contextmanager
def get_duckdb_connection() -> Generator[Any, None, None]:  # type: ignore
    """Get a DuckDB connection.

    Yields:
        A DuckDB connection

    Raises:
        DatabaseError: If connection fails

    """
    import pathlib

    data_dir = getattr(settings, "DATA_DIR", ".")
    duckdb_path = getattr(settings, "DUCKDB_PATH", ":memory:")
    if isinstance(data_dir, str):
        pathlib.Path(data_dir).mkdir(parents=True, exist_ok=True)
    else:
        data_dir.mkdir(parents=True, exist_ok=True)
    conn = None
    try:
        conn = duckdb.connect(str(duckdb_path))  # type: ignore
        yield conn
    except Exception as e:
        logger.error(f"Failed to connect to DuckDB: {e}")
        raise DatabaseError("Failed to connect to DuckDB") from e
    finally:
        if conn is not None:
            conn.close()


def dict_row_factory(cursor: Any) -> Callable[[Any], dict[str, Any]]:
    """Create a dictionary row factory for PostgreSQL cursors.

    Args:
        cursor: The cursor to create a row factory for

    Returns:
        A function that converts rows to dictionaries

    """
    return lambda values: {
        (desc.name if hasattr(desc, "name") else str(desc)): value
        for desc, value in zip(cursor.description or [], values, strict=False)
        if desc is not None
    }


class DuckDBConnection:
    """DuckDB connection manager."""

    def __init__(self, database: str = ":memory:"):
        """Initialize DuckDB connection.

        Args:
            database: Path to the database file or ":memory:" for in-memory database.
        """
        self.database: str = database
        self.conn: Any | None = self._connect()

    def _connect(self) -> Any | None:  # type: ignore
        """Connect to DuckDB.

        Returns:
            Connection object

        Raises:
            DatabaseError: If connection fails.
        """
        try:
            conn = duckdb.connect(str(self.database))  # type: ignore
            return conn
        except Exception as e:
            logger.error(f"Failed to connect to DuckDB: {e}")
            raise DatabaseError("Failed to connect to DuckDB") from e

    async def get_cursor(self) -> Any | None:
        """Get a DuckDB cursor for executing queries.

        Returns:
            Connection object

        Raises:
            DatabaseError: If cursor creation fails.
        """
        try:
            return duckdb.connect(str(self.database))  # type: ignore
        except Exception as e:
            logger.error(f"Failed to create DuckDB cursor: {e}")
            raise DatabaseError("Failed to create DuckDB cursor") from e
