"""
Database connection resources for Dagster.

This module defines resources for connecting to databases like PostgreSQL and DuckDB.
"""

from collections.abc import Iterator
from contextlib import contextmanager

import duckdb
import psycopg2
from dagster import ConfigurableResource, resource


class PostgresResource(ConfigurableResource):
    """Resource for connecting to PostgreSQL."""

    host: str
    port: int
    user: str
    password: str
    database: str

    @contextmanager
    def get_connection(self) -> Iterator[psycopg2.extensions.connection]:
        """Get a connection to the PostgreSQL database."""
        conn = psycopg2.connect(
            host=self.host,
            port=self.port,
            user=self.user,
            password=self.password,
            database=self.database,
        )
        try:
            yield conn
        finally:
            conn.close()


class DuckDBResource(ConfigurableResource):
    """Resource for connecting to DuckDB."""

    database_path: str

    @contextmanager
    def get_connection(self) -> Iterator[duckdb.DuckDBPyConnection]:
        """Get a connection to the DuckDB database."""
        conn = duckdb.connect(self.database_path)
        try:
            yield conn
        finally:
            conn.close()


@resource(config_schema={"database_path": str})
def duckdb_resource(context):
    """Resource for connecting to DuckDB."""
    database_path = context.resource_config["database_path"]
    return DuckDBResource(database_path=database_path)


@resource(
    config_schema={
        "host": str,
        "port": int,
        "user": str,
        "password": str,
        "database": str,
    }
)
def postgres_resource(context):
    """Resource for connecting to PostgreSQL."""
    return PostgresResource(
        host=context.resource_config["host"],
        port=context.resource_config["port"],
        user=context.resource_config["user"],
        password=context.resource_config["password"],
        database=context.resource_config["database"],
    )
