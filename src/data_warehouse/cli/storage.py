"""Storage-related CLI commands."""

import asyncio
import sys
from pathlib import Path

import click

from data_warehouse.config.settings import settings
from data_warehouse.core.exceptions import DatabaseError, StorageError
from data_warehouse.core.storage import initialize_storage
from data_warehouse.utils.error_handler import confirm_action, handle_exceptions
from data_warehouse.utils.logger import get_command_logger

# Get a logger for this module
log = get_command_logger(__name__)


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
@handle_exceptions()
def init_storage(force: bool):
    """Initialize all storage components (PostgreSQL, DuckDB, MinIO).

    This creates the necessary schemas, tables, and buckets for the data warehouse.
    """
    log.info("Initializing storage components...")

    try:
        # Create data directory if it doesn't exist
        Path(settings.DATA_DIR).mkdir(parents=True, exist_ok=True)

        # If force is enabled, confirm the destructive action
        if force and not confirm_action(
            "This will recreate all storage components and may result in data loss. Continue?",
            default=False,
        ):
            return

        # Run the async initialization function
        asyncio.run(_init_storage(force))
        log.info("Storage initialization complete")
        click.secho("Storage initialized successfully!", fg="green")
    except (DatabaseError, StorageError) as e:
        log.error(f"Storage initialization failed: {e}")
        click.secho(f"Storage initialization failed: {e}", fg="red", err=True)
        sys.exit(1)


async def _init_storage(force: bool):
    """Asynchronous implementation of storage initialization.

    Args:
        force: Whether to force reinitialization

    Raises:
        DatabaseError: If database initialization fails
        StorageError: If storage initialization fails
    """
    log.info("Initializing storage (async)...")
    # TODO: Implement force reinitialization logic if needed
    await initialize_storage()
    log.info("Storage initialization (async) complete")


@storage.command("status")
@handle_exceptions()
def storage_status():
    """Check the status of all storage components."""
    log.info("Checking storage status...")

    try:
        # Call async function to check all storage components
        asyncio.run(_check_storage_status())
        log.info("Storage status check complete")
    except Exception as e:
        log.error(f"Storage status check failed: {e}")
        raise


async def _check_storage_status():  # type: ignore[reportRedeclaration]
    log.info("Starting async storage status check...")
    # This is a placeholder for actual implementation
    # Import these inside the function to prevent circular imports
    from typing import Any

    from data_warehouse.core.storage.duckdb import get_duckdb_client
    from data_warehouse.core.storage.minio import get_minio_client
    from data_warehouse.core.storage.postgres import get_postgres_connection

    click.echo("Storage Status:")
    # Check PostgreSQL
    try:
        async with get_postgres_connection() as conn:
            # Execute a simple query to verify connection
            cursor = await conn.execute("SELECT version()")
            postgres_version = await cursor.fetchone()
            if postgres_version is not None:
                log.info(f"PostgreSQL: Connected - {postgres_version[0]}")
                click.secho("✅ PostgreSQL: Connected", fg="green")
                click.echo(f"   Version: {postgres_version[0]}")
            else:
                log.error("PostgreSQL: No version returned")
                click.secho("❌ PostgreSQL: No version returned", fg="red")
    except Exception as e:
        log.error(f"PostgreSQL: Connection failed - {e}")
        click.secho("❌ PostgreSQL: Connection failed", fg="red")
        click.echo(f"   Error: {e}")

    # Check DuckDB
    try:
        client = get_duckdb_client()
        result = client.execute_query("SELECT version()")
        version_row = result.fetchone() if hasattr(result, "fetchone") else None  # type: ignore[attr-defined]
        # Pyright: fetchone() return type is partially unknown, suppressing error.
        duckdb_version: Any = version_row[0] if version_row is not None else None  # type: ignore[index]
        if duckdb_version is not None:
            log.info(f"DuckDB: Connected - {duckdb_version}")
            click.secho("✅ DuckDB: Connected", fg="green")
            click.echo(f"   Version: {duckdb_version}")
        else:
            log.error("DuckDB: No version returned")
            click.secho("❌ DuckDB: No version returned", fg="red")
    except Exception as e:
        log.error(f"DuckDB: Connection failed - {e}")
        click.secho("❌ DuckDB: Connection failed", fg="red")
        click.echo(f"   Error: {e}")

    # Check MinIO
    try:
        client = get_minio_client()
        # List buckets to verify connection
        buckets = [bucket.name for bucket in client.client.list_buckets()]  # type: ignore[attr-defined]
        # Pyright: list_buckets() return type is partially unknown, suppressing error.
        log.info(f"MinIO: Connected - {len(buckets)} buckets found {buckets}")
        click.secho("✅ MinIO: Connected", fg="green")
        click.echo(f"   Buckets: {', '.join(buckets) if buckets else 'No buckets found'}")
    except Exception as e:
        log.error(f"MinIO: Connection failed - {e}")
        click.secho("❌ MinIO: Connection failed", fg="red")
        click.echo(f"   Error: {e}")
    log.info("Async storage status check complete")


if __name__ == "__main__":
    import asyncio

    asyncio.run(storage_status())

    storage()
