"""DuckDB storage functionality for the data warehouse."""

import contextlib
import os
from collections.abc import Generator
from pathlib import Path
from typing import Any

import duckdb
from loguru import logger

from data_warehouse.config.settings import settings
from data_warehouse.core.exceptions import DatabaseError


class DuckDBClient:
    """DuckDB client for analytical queries."""

    _instance = None

    def __new__(cls):
        """Implement singleton pattern for DuckDB client."""
        if cls._instance is None:
            cls._instance = super(DuckDBClient, cls).__new__(cls)
            cls._instance._initialized = False
        return cls._instance

    def __init__(self):
        """Initialize DuckDB client with configuration settings."""
        if self._initialized:
            return

        # Ensure data directory exists
        os.makedirs(settings.DATA_DIR, exist_ok=True)

        # Setup the connection
        self.db_path = settings.DUCKDB_PATH
        self.connection = duckdb.connect(str(self.db_path))

        # Configure settings for optimal analytical performance
        self.connection.execute("PRAGMA threads=8")
        self.connection.execute("PRAGMA memory_limit='4GB'")

        self._initialized = True
        logger.debug(f"DuckDB client initialized with database at {self.db_path}")

    def close(self):
        """Close the DuckDB connection."""
        if hasattr(self, "connection") and self.connection:
            self.connection.close()
            logger.debug("DuckDB connection closed")

    def execute_query(
        self, query: str, parameters: dict[str, Any] | None = None
    ) -> duckdb.DuckDBPyResult:
        """Execute a SQL query on DuckDB.

        Args:
            query: SQL query to execute
            parameters: Optional query parameters

        Returns:
            Query result

        Raises:
            DatabaseError: If query execution fails
        """
        try:
            if parameters:
                result = self.connection.execute(query, parameters)
            else:
                result = self.connection.execute(query)
            return result
        except Exception as e:
            logger.error(f"Failed to execute DuckDB query: {e}")
            raise DatabaseError(f"Failed to execute DuckDB query: {e}") from e

    def query_to_df(self, query: str, parameters: dict[str, Any] | None = None) -> Any:
        """Execute a query and return results as a pandas DataFrame.

        Args:
            query: SQL query to execute
            parameters: Optional query parameters

        Returns:
            pandas DataFrame with query results

        Raises:
            DatabaseError: If query execution fails
        """
        try:
            result = self.execute_query(query, parameters)
            return result.df()
        except Exception as e:
            logger.error(f"Failed to convert DuckDB result to DataFrame: {e}")
            raise DatabaseError(
                f"Failed to convert DuckDB result to DataFrame: {e}"
            ) from e

    def create_table_from_query(
        self, table_name: str, query: str, replace: bool = False
    ) -> None:
        """Create a table from a query.

        Args:
            table_name: Name of the table to create
            query: SQL query to use as the table source
            replace: Whether to replace the table if it exists

        Raises:
            DatabaseError: If table creation fails
        """
        try:
            mode = (
                "CREATE OR REPLACE TABLE" if replace else "CREATE TABLE IF NOT EXISTS"
            )
            full_query = f"{mode} {table_name} AS {query}"
            self.execute_query(full_query)
            logger.info(f"Table {table_name} created from query")
        except Exception as e:
            logger.error(f"Failed to create table {table_name} from query: {e}")
            raise DatabaseError(
                f"Failed to create table {table_name} from query"
            ) from e

    def create_view(self, view_name: str, query: str, replace: bool = False) -> None:
        """Create a view from a query.

        Args:
            view_name: Name of the view to create
            query: SQL query to use as the view source
            replace: Whether to replace the view if it exists

        Raises:
            DatabaseError: If view creation fails
        """
        try:
            mode = "CREATE OR REPLACE VIEW" if replace else "CREATE VIEW IF NOT EXISTS"
            full_query = f"{mode} {view_name} AS {query}"
            self.execute_query(full_query)
            logger.info(f"View {view_name} created")
        except Exception as e:
            logger.error(f"Failed to create view {view_name}: {e}")
            raise DatabaseError(f"Failed to create view {view_name}") from e

    def load_csv(
        self,
        table_name: str,
        csv_path: str | Path,
        auto_detect: bool = True,
        delimiter: str = ",",
        header: bool = True,
        sample_size: int = 1000,
        replace: bool = False,
    ) -> None:
        """Load CSV data into a DuckDB table.

        Args:
            table_name: Name of the table to create
            csv_path: Path to the CSV file
            auto_detect: Whether to auto-detect types
            delimiter: CSV delimiter character
            header: Whether the CSV has a header row
            sample_size: Number of rows to sample for type detection
            replace: Whether to replace the table if it exists

        Raises:
            DatabaseError: If CSV loading fails
        """
        try:
            mode = (
                "CREATE OR REPLACE TABLE" if replace else "CREATE TABLE IF NOT EXISTS"
            )

            options = []
            if auto_detect:
                options.append("AUTO_DETECT=TRUE")
            if header:
                options.append("HEADER=TRUE")
            options.append(f"DELIMITER='{delimiter}'")
            options.append(f"SAMPLE_SIZE={sample_size}")

            options_str = ", ".join(options)
            query = f"{mode} {table_name} AS SELECT * FROM read_csv('{csv_path}', {options_str})"

            self.execute_query(query)
            logger.info(f"Loaded CSV data from {csv_path} into table {table_name}")
        except Exception as e:
            logger.error(f"Failed to load CSV data into table {table_name}: {e}")
            raise DatabaseError(
                f"Failed to load CSV data into table {table_name}"
            ) from e

    def export_query_to_csv(
        self, query: str, csv_path: str | Path, parameters: dict[str, Any] | None = None
    ) -> None:
        """Export query results to a CSV file.

        Args:
            query: SQL query to execute
            csv_path: Path to save the CSV file
            parameters: Optional query parameters

        Raises:
            DatabaseError: If CSV export fails
        """
        try:
            result = self.execute_query(query, parameters)
            result.write_csv(str(csv_path))
            logger.info(f"Exported query results to {csv_path}")
        except Exception as e:
            logger.error(f"Failed to export query results to CSV: {e}")
            raise DatabaseError("Failed to export query results to CSV") from e

    def list_tables(self, schema: str = "main") -> list[str]:
        """List all tables in a schema.

        Args:
            schema: Schema name to list tables from

        Returns:
            List of table names

        Raises:
            DatabaseError: If listing tables fails
        """
        try:
            query = f"SELECT table_name FROM information_schema.tables WHERE table_schema = '{schema}' AND table_type = 'BASE TABLE'"
            result = self.execute_query(query)
            return [row[0] for row in result.fetchall()]
        except Exception as e:
            logger.error(f"Failed to list tables: {e}")
            raise DatabaseError("Failed to list tables") from e

    def get_table_schema(self, table_name: str) -> list[dict[str, str]]:
        """Get the schema of a table.

        Args:
            table_name: Name of the table to get schema for

        Returns:
            List of column information dictionaries

        Raises:
            DatabaseError: If getting schema fails
        """
        try:
            # Extract schema and table name
            parts = table_name.split(".")
            if len(parts) == 2:
                schema_name, table = parts
            else:
                schema_name, table = "main", table_name

            query = f"""
                SELECT column_name, data_type, is_nullable
                FROM information_schema.columns
                WHERE table_schema = '{schema_name}' AND table_name = '{table}'
                ORDER BY ordinal_position
            """

            result = self.execute_query(query)

            return [
                {
                    "name": row[0],
                    "type": row[1],
                    "nullable": row[2] == "YES",
                }
                for row in result.fetchall()
            ]
        except Exception as e:
            logger.error(f"Failed to get schema for table {table_name}: {e}")
            raise DatabaseError(f"Failed to get schema for table {table_name}") from e


@contextlib.contextmanager
def get_duckdb_connection() -> Generator[duckdb.DuckDBPyConnection, None, None]:
    """Get a DuckDB connection context manager.

    Yields:
        DuckDB connection

    Raises:
        DatabaseError: If connection fails
    """
    client = None
    try:
        client = DuckDBClient()
        yield client.connection
    except Exception as e:
        logger.error(f"Error with DuckDB connection: {e}")
        raise DatabaseError("Failed to get DuckDB connection") from e
    finally:
        # We don't close the connection here as it's managed by the singleton
        # This allows connection reuse throughout the application
        pass


def get_duckdb_client() -> DuckDBClient:
    """Get the DuckDB client singleton.

    Returns:
        DuckDBClient instance
    """
    return DuckDBClient()


def initialize_duckdb() -> None:
    """Initialize DuckDB with necessary setup.

    Raises:
        DatabaseError: If initialization fails
    """
    try:
        client = get_duckdb_client()

        # Create schema for warehouse data
        client.execute_query("CREATE SCHEMA IF NOT EXISTS warehouse")

        # Create basic metadata tables
        client.execute_query("""
            CREATE TABLE IF NOT EXISTS warehouse.dataset_registry (
                id INTEGER PRIMARY KEY,
                name VARCHAR NOT NULL UNIQUE,
                description VARCHAR,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                source_type VARCHAR,
                location VARCHAR,
                schema_json VARCHAR,
                row_count BIGINT,
                metadata_json VARCHAR
            )
        """)

        # Create a view for easier querying of registered datasets
        client.execute_query("""
            CREATE OR REPLACE VIEW warehouse.datasets AS
            SELECT
                id, name, description, created_at, updated_at,
                source_type, location,
                json_extract_json(schema_json, '$') AS schema,
                row_count,
                json_extract_json(metadata_json, '$') AS metadata
            FROM warehouse.dataset_registry
        """)

        logger.info("DuckDB initialized successfully")
    except Exception as e:
        logger.error(f"Failed to initialize DuckDB: {e}")
        raise DatabaseError("Failed to initialize DuckDB") from e
