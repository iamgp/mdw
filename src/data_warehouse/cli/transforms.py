"""Transformation-related CLI commands."""

import click
from loguru import logger

from data_warehouse.utils.error_handler import handle_exceptions


@click.group()
def transforms():
    """Data transformation commands.

    These commands allow you to manage and run data transformation processes using dbt.
    """
    pass


@transforms.command("list")
def list_transforms():
    """List all available data transformation models."""
    click.echo("Available transformation models:")

    # This will be implemented fully in future tasks
    # For now, just show a placeholder message
    models = ["staging_sales", "dim_customers", "dim_products", "fact_orders"]
    for model in models:
        click.echo(f"- {model}")


@transforms.command("run")
@click.argument("model", required=False)
@click.option("--full-refresh", is_flag=True, help="Perform a full refresh of the models.")
@handle_exceptions()
def run_transform(model: str | None = None, full_refresh: bool = False):
    """Run dbt transformations on the data warehouse.

    If a model is specified, only that model and its dependencies will be run.
    Otherwise, all models will be run.

    Examples:
        data-warehouse transforms run
        data-warehouse transforms run fact_orders
        data-warehouse transforms run fact_orders --full-refresh
    """
    if model:
        click.echo(f"Running transformation for model: {model}")
        cmd = f"dbt run --select {model}"
    else:
        click.echo("Running all transformations")
        cmd = "dbt run"

    if full_refresh:
        cmd += " --full-refresh"
        click.echo("Using full refresh mode")

    # Log the command
    logger.info(f"Running transformation command: {cmd}")

    # This will be implemented in future tasks
    # For now, just show a placeholder message
    click.echo("Transformation job started (placeholder for actual implementation)")
    click.echo("Command that would run: " + cmd)
    click.echo("Check logs for progress updates")


@transforms.command("test")
@click.argument("model", required=False)
@handle_exceptions()
def test_transform(model: str | None = None):
    """Run tests on the transformed data.

    If a model is specified, only tests for that model will be run.
    Otherwise, all tests will be run.

    Examples:
        data-warehouse transforms test
        data-warehouse transforms test fact_orders
    """
    if model:
        click.echo(f"Running tests for model: {model}")
        cmd = f"dbt test --select {model}"
    else:
        click.echo("Running all tests")
        cmd = "dbt test"

    # Log the command
    logger.info(f"Running test command: {cmd}")

    # This will be implemented in future tasks
    # For now, just show a placeholder message
    click.echo("Test job started (placeholder for actual implementation)")
    click.echo("Command that would run: " + cmd)
    click.echo("Check logs for progress updates")


if __name__ == "__main__":
    transforms()
