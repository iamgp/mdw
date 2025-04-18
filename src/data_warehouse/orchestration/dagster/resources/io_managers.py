"""
Dagster I/O managers for storing and retrieving operation results.

This module provides I/O managers for loading and storing data between Dagster
operations and assets. These managers handle the serialization, deserialization,
and storage of data across pipeline runs.
"""

from typing import Any, Literal, cast

import pandas as pd
from dagster import Field, InputContext, IOManager, OutputContext, StringSource, io_manager


class PostgresIOManager(IOManager):
    """I/O manager for storing and retrieving data from PostgreSQL."""

    def __init__(self, conn_string: str):
        """Initialize PostgreSQL I/O manager with connection string.

        Args:
            conn_string: PostgreSQL connection string
        """
        self.conn_string = conn_string

    def handle_output(self, context: OutputContext, obj: Any) -> None:
        """Store output data to PostgreSQL.

        Args:
            context: Dagster output context
            obj: Data to store (typically a DataFrame)
        """
        if not isinstance(obj, pd.DataFrame):
            context.log.warning(f"PostgresIOManager only supports DataFrame outputs, got {type(obj)}")
            return

        table_name = context.asset_key.path[-1] if context.has_asset_key else f"{context.op_def.name}_{context.name}"
        schema = context.asset_key.path[-2] if context.has_asset_key and len(context.asset_key.path) > 1 else "public"

        context.log.info(f"Writing data to PostgreSQL: {schema}.{table_name}")

        # Get metadata from the output context
        metadata = context.metadata or {}
        if_exists: Literal["replace", "append", "fail"] = cast(
            Literal["replace", "append", "fail"], metadata.get("if_exists", "replace")
        )

        obj.to_sql(name=table_name, con=self.conn_string, schema=schema, if_exists=if_exists, index=False)

        context.log.info(f"Successfully wrote {len(obj)} rows to {schema}.{table_name}")

    def load_input(self, context: InputContext) -> pd.DataFrame:
        """Load input data from PostgreSQL.

        Args:
            context: Dagster input context

        Returns:
            DataFrame containing the requested data
        """
        if context.upstream_output is None:
            raise ValueError("PostgresIOManager requires upstream output context")

        upstream_context = context.upstream_output

        table_name = (
            upstream_context.asset_key.path[-1]
            if upstream_context.has_asset_key
            else f"{upstream_context.op_def.name}_{upstream_context.name}"
        )
        schema = (
            upstream_context.asset_key.path[-2]
            if upstream_context.has_asset_key and len(upstream_context.asset_key.path) > 1
            else "public"
        )

        context.log.info(f"Loading data from PostgreSQL: {schema}.{table_name}")

        query = f"SELECT * FROM {schema}.{table_name}"
        df = pd.read_sql(query, self.conn_string)

        context.log.info(f"Successfully loaded {len(df)} rows from {schema}.{table_name}")
        return df


class DuckDBIOManager(IOManager):
    """I/O manager for storing and retrieving data from DuckDB."""

    def __init__(self, db_path: str):
        """Initialize DuckDB I/O manager with database path.

        Args:
            db_path: Path to DuckDB database file
        """
        self.db_path = db_path

    def handle_output(self, context: OutputContext, obj: Any) -> None:
        """Store output data to DuckDB.

        Args:
            context: Dagster output context
            obj: Data to store (typically a DataFrame)
        """
        if not isinstance(obj, pd.DataFrame):
            context.log.warning(f"DuckDBIOManager only supports DataFrame outputs, got {type(obj)}")
            return

        import duckdb

        table_name = context.asset_key.path[-1] if context.has_asset_key else f"{context.op_def.name}_{context.name}"
        schema = context.asset_key.path[-2] if context.has_asset_key and len(context.asset_key.path) > 1 else "main"

        context.log.info(f"Writing data to DuckDB: {schema}.{table_name}")

        # Get metadata from the output context
        metadata = context.metadata or {}
        if_exists = metadata.get("if_exists", "replace")

        # Use typed variable to avoid linter issue with partially unknown connect type
        conn: duckdb.DuckDBPyConnection = duckdb.connect(database=self.db_path)

        # Create schema if it doesn't exist
        conn.execute(f"CREATE SCHEMA IF NOT EXISTS {schema}")

        # Drop table if replacing
        if if_exists == "replace":
            conn.execute(f"DROP TABLE IF EXISTS {schema}.{table_name}")

        # Write data
        conn.execute(f"CREATE TABLE IF NOT EXISTS {schema}.{table_name} AS SELECT * FROM obj")

        conn.close()
        context.log.info(f"Successfully wrote {len(obj)} rows to {schema}.{table_name}")

    def load_input(self, context: InputContext) -> pd.DataFrame:
        """Load input data from DuckDB.

        Args:
            context: Dagster input context

        Returns:
            DataFrame containing the requested data
        """
        if context.upstream_output is None:
            raise ValueError("DuckDBIOManager requires upstream output context")

        import duckdb

        upstream_context = context.upstream_output

        table_name = (
            upstream_context.asset_key.path[-1]
            if upstream_context.has_asset_key
            else f"{upstream_context.op_def.name}_{upstream_context.name}"
        )
        schema = (
            upstream_context.asset_key.path[-2]
            if upstream_context.has_asset_key and len(upstream_context.asset_key.path) > 1
            else "main"
        )

        context.log.info(f"Loading data from DuckDB: {schema}.{table_name}")

        # Use typed variable to avoid linter issue with partially unknown connect type
        conn: duckdb.DuckDBPyConnection = duckdb.connect(database=self.db_path)
        df = conn.execute(f"SELECT * FROM {schema}.{table_name}").fetchdf()
        conn.close()

        context.log.info(f"Successfully loaded {len(df)} rows from {schema}.{table_name}")
        return df


@io_manager(
    config_schema={
        "conn_string": Field(
            StringSource,
            description="PostgreSQL connection string",
        ),
    },
    required_resource_keys={"postgres"},
)
def postgres_io_manager(context) -> PostgresIOManager:
    """Create a PostgreSQL I/O manager.

    Args:
        context: Dagster resource context

    Returns:
        Configured PostgreSQL I/O manager
    """
    conn_string = str(context.resource_config["conn_string"])
    return PostgresIOManager(conn_string)


@io_manager(
    config_schema={
        "db_path": Field(
            StringSource,
            description="Path to DuckDB database file",
        ),
    },
    required_resource_keys={"duckdb"},
)
def duckdb_io_manager(context) -> DuckDBIOManager:
    """Create a DuckDB I/O manager.

    Args:
        context: Dagster resource context

    Returns:
        Configured DuckDB I/O manager
    """
    db_path = str(context.resource_config["db_path"])
    return DuckDBIOManager(db_path)
