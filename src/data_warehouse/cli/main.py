"""Main CLI entry point for the data warehouse."""

import click
from loguru import logger

from data_warehouse.cli.storage import storage
from data_warehouse.config.settings import settings
from data_warehouse.utils.logger import setup_logger


@click.group()
def cli():
    """Data Warehouse CLI commands."""
    # Initialize logger
    setup_logger()
    logger.debug(f"Data Warehouse CLI started in {settings.ENVIRONMENT} environment")


# Add command groups
cli.add_command(storage)


# Add top-level commands
@cli.command("version")
def version():
    """Show the current version of the data warehouse."""
    click.echo("Data Warehouse version: 0.1.0")
    click.echo(f"Environment: {settings.ENVIRONMENT}")


if __name__ == "__main__":
    cli()
