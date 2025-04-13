"""Main CLI entry point for the data warehouse."""

import click
from loguru import logger

from data_warehouse import __version__
from data_warehouse.cli.ingestion import ingestion
from data_warehouse.cli.storage import storage
from data_warehouse.cli.transforms import transforms
from data_warehouse.config.settings import settings
from data_warehouse.utils.logger import setup_logger


@click.group(context_settings={"help_option_names": ["-h", "--help"]})
@click.version_option(version=__version__, prog_name="data-warehouse")
@click.option("--verbose", "-v", is_flag=True, help="Enable verbose logging output.")
def cli(verbose: bool):
    """Data Warehouse CLI - Management and operations tool.

    This CLI provides commands for managing various aspects of the data warehouse,
    including storage setup, data ingestion, transformations, and monitoring.
    """
    # Initialize logger with appropriate verbosity
    setup_logger()  # Existing function doesn't take parameters, will enhance in subtask 4.3
    logger.debug(f"Data Warehouse CLI started in {settings.ENVIRONMENT} environment")


# Add command groups
cli.add_command(storage)
cli.add_command(ingestion)
cli.add_command(transforms)


# Add top-level commands
@cli.command("version")
def version():
    """Show the current version of the data warehouse."""
    click.echo(f"Data Warehouse version: {__version__}")
    click.echo(f"Environment: {settings.ENVIRONMENT}")


@cli.command("info")
def info():
    """Display information about the data warehouse configuration."""
    click.echo("Data Warehouse Configuration:")
    click.echo(f"Environment: {settings.ENVIRONMENT}")
    click.echo(f"Data Directory: {settings.DATA_DIR}")
    click.echo(f"Log Level: {settings.LOG_LEVEL}")
    click.echo(f"PostgreSQL Database: {settings.POSTGRES_DB}")
    click.echo(f"DuckDB Path: {settings.DUCKDB_PATH}")
    click.echo(f"MinIO: {settings.MINIO_HOST}:{settings.MINIO_PORT}")


@cli.command("doctor")
def doctor():
    """Run diagnostics to check if the data warehouse is configured correctly."""
    click.echo("Running diagnostic checks...")

    # Check environment variables
    click.echo("Checking environment variables...")
    missing_vars: list[str] = []
    for var in [
        "POSTGRES_HOST",
        "POSTGRES_USER",
        "POSTGRES_PASSWORD",
        "MINIO_ACCESS_KEY",
    ]:
        if not getattr(settings, var, None):
            missing_vars.append(var)

    if missing_vars:
        click.secho(
            f"Warning: Missing environment variables: {', '.join(missing_vars)}",
            fg="yellow",
        )
    else:
        click.secho("Environment variables: OK", fg="green")

    # Check data directory
    click.echo("Checking data directory...")
    data_dir = settings.DATA_DIR
    if not data_dir.exists():
        click.secho(f"Warning: Data directory does not exist: {data_dir}", fg="yellow")
    else:
        click.secho(f"Data directory: OK ({data_dir})", fg="green")

    click.echo(
        "\nRun 'data-warehouse storage status' for detailed storage connection status."
    )


if __name__ == "__main__":
    cli()
