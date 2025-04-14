"""Ingestion-related CLI commands."""

import click
from loguru import logger


@click.group()
def ingestion():
    """Data ingestion commands.

    These commands allow you to ingest data from various sources into the data warehouse.
    """
    pass


@ingestion.command("list-sources")
def list_sources():
    """List all available data sources for ingestion."""
    click.echo("Available data sources:")

    # This will be implemented fully in future tasks
    # For now, just show a placeholder message
    sources = ["csv", "json", "postgres", "api"]
    for source in sources:
        click.echo(f"- {source}")


@ingestion.command("run")
@click.argument("source", type=click.Choice(["csv", "json", "postgres", "api"]))
@click.option("--file", "-f", help="Source file path for file-based ingestion.")
@click.option("--table", "-t", help="Source table for database ingestion.")
@click.option("--endpoint", "-e", help="API endpoint for API-based ingestion.")
@handle_exceptions()
def run_ingestion(
    source: str,
    file: str | None = None,
    table: str | None = None,
    endpoint: str | None = None,
):
    """Run a data ingestion job from a specified source.

    Examples:
        data-warehouse ingestion run csv --file /path/to/data.csv
        data-warehouse ingestion run postgres --table source_table
    """
    click.echo(f"Starting ingestion from {source} source...")

    # Handle different source types with appropriate parameters
    if source in ["csv", "json"] and not file:
        raise click.UsageError(f"--file option is required for {source} ingestion")
    elif source == "postgres" and not table:
        raise click.UsageError("--table option is required for postgres ingestion")
    elif source == "api" and not endpoint:
        raise click.UsageError("--endpoint option is required for api ingestion")

    # Log the details
    logger.info(f"Ingestion started - Source: {source}")
    if file:
        logger.info(f"File: {file}")
    if table:
        logger.info(f"Table: {table}")
    if endpoint:
        logger.info(f"Endpoint: {endpoint}")

    # This will be implemented in future tasks
    # For now, just show a placeholder message
    click.echo("Ingestion job started (placeholder for actual implementation)")
    click.echo("Check logs for progress updates")

if __name__ == "__main__":
    ingestion()
