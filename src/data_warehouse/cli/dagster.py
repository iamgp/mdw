"""Dagster orchestration CLI commands."""

import os
import subprocess
from pathlib import Path

import click
from loguru import logger

from data_warehouse.config.settings import settings
from data_warehouse.utils.error_handler import handle_exceptions


@click.group()
def dagster():
    """Dagster workflow orchestration commands.

    These commands allow you to manage and run Dagster workflows for the data warehouse.
    """
    pass


@dagster.command("ui")
@click.option("--host", default="127.0.0.1", help="Host to bind the Dagster UI server to.")
@click.option("--port", default=3000, type=int, help="Port to run the Dagster UI server on.")
@click.option("--reload/--no-reload", default=True, help="Enable/disable auto-reload on code changes.")
@handle_exceptions()
def start_ui(host: str, port: int, reload: bool):
    """Start the Dagster UI server for local development.

    This command launches the Dagster UI, allowing you to visualize and interact with
    your data pipeline through the web interface.

    Examples:
        data-warehouse dagster ui
        data-warehouse dagster ui --port 8000
        data-warehouse dagster ui --no-reload
    """
    click.echo(f"Starting Dagster UI server on http://{host}:{port}")

    # Set environment variables
    os.environ["DAGSTER_HOME"] = str(settings.DAGSTER_HOME)

    # Get the path to the repository
    repo_path = Path(settings.PROJECT_ROOT) / "src" / "data_warehouse" / "orchestration" / "dagster" / "__init__.py"

    if not repo_path.exists():
        raise click.UsageError(f"Dagster repository file not found at {repo_path}")

    # Construct the dagster command
    cmd = ["dagster", "dev", "-m", "src.data_warehouse.orchestration.dagster", "--host", host, "--port", str(port)]

    if not reload:
        cmd.append("--no-reload")

    # Log the command
    logger.info(f"Running command: {' '.join(cmd)}")

    try:
        # Use subprocess.run to execute the command
        # We don't capture output so it streams directly to the console
        click.echo("Starting Dagster UI server... (Press CTRL+C to stop)")
        subprocess.run(cmd)
    except KeyboardInterrupt:
        click.echo("\nDagster UI server stopped")


@dagster.command("daemon")
@handle_exceptions()
def start_daemon():
    """Start the Dagster daemon process.

    The daemon is responsible for running schedules and sensors in the background.
    This is required for production deployments or when you want to use schedules
    and sensors locally.

    Example:
        data-warehouse dagster daemon
    """
    click.echo("Starting Dagster daemon process")

    # Set environment variables
    os.environ["DAGSTER_HOME"] = str(settings.DAGSTER_HOME)

    # Ensure the Dagster home directory exists
    dagster_home = Path(os.environ["DAGSTER_HOME"])
    dagster_home.mkdir(parents=True, exist_ok=True)

    # Check if dagster.yaml exists in DAGSTER_HOME
    dagster_yaml = dagster_home / "dagster.yaml"
    if not dagster_yaml.exists():
        project_dagster_yaml = Path(settings.PROJECT_ROOT) / "dagster.yaml"
        if project_dagster_yaml.exists():
            # Copy the config from the project root to DAGSTER_HOME
            with open(project_dagster_yaml) as src, open(dagster_yaml, "w") as dst:
                dst.write(src.read())
            click.echo(f"Copied dagster.yaml configuration to {dagster_home}")
        else:
            click.echo("Warning: No dagster.yaml found. Using default configuration.")

    # Run the dagster-daemon command
    cmd = ["dagster-daemon", "run"]

    logger.info(f"Running command: {' '.join(cmd)}")

    try:
        click.echo("Starting Dagster daemon... (Press CTRL+C to stop)")
        subprocess.run(cmd)
    except KeyboardInterrupt:
        click.echo("\nDagster daemon stopped")


@dagster.command("materialize")
@click.argument("assets", nargs=-1)
@click.option("--all", "all_assets", is_flag=True, help="Materialize all assets.")
@click.option("--tags", help="Tags in JSON format to be added to the run.")
@handle_exceptions()
def materialize_assets(assets: tuple[str, ...], all_assets: bool = False, tags: str | None = None):
    """Materialize Dagster assets from the command line.

    This command allows you to trigger materialization of specific assets or all assets.
    You can specify assets by name or use --all to materialize all assets.

    Examples:
        data-warehouse dagster materialize raw_sales transformed_sales
        data-warehouse dagster materialize --all
    """
    if not assets and not all_assets:
        raise click.UsageError("You must specify asset names or use --all to materialize all assets")

    # Set environment variables
    os.environ["DAGSTER_HOME"] = str(settings.DAGSTER_HOME)

    # Construct the dagster command for materialization
    cmd = ["dagster", "asset", "materialize"]

    if all_assets:
        cmd.append("--all")
    else:
        cmd.extend(["--selection", " ".join(list(assets))])

    if tags:
        cmd.extend(["--tags", tags])

    # Get the path to the repository
    repo_option = ["-m", "src.data_warehouse.orchestration.dagster"]
    cmd.extend(repo_option)

    logger.info(f"Running command: {' '.join(cmd)}")

    click.echo(f"Materializing {'all' if all_assets else ', '.join(assets)} assets...")

    # Execute the command
    result = subprocess.run(cmd)

    if result.returncode == 0:
        click.echo("Successfully materialized assets")
    else:
        click.echo("Failed to materialize assets. Check logs for details.")


@dagster.command("run")
@click.argument("job_name", required=True)
@click.option("--tags", help="Tags in JSON format to be added to the run.")
@click.option("--config", type=click.Path(exists=True), help="Path to a YAML config file for the run.")
@handle_exceptions()
def run_job(job_name: str, tags: str | None = None, config: str | None = None):
    """Run a specific Dagster job by name.

    This command allows you to trigger a job execution by providing its name.
    You can also pass configuration and tags for the job run.

    Examples:
        data-warehouse dagster run daily_refresh
        data-warehouse dagster run etl_job --config job_config.yaml
    """
    # Set environment variables
    os.environ["DAGSTER_HOME"] = str(settings.DAGSTER_HOME)

    # Construct the dagster command for running a job
    cmd = ["dagster", "job", "execute", job_name]

    # Add the repository module path
    cmd.extend(["-m", "src.data_warehouse.orchestration.dagster"])

    if tags:
        cmd.extend(["--tags", tags])

    if config:
        cmd.extend(["--config", config])

    logger.info(f"Running command: {' '.join(cmd)}")

    click.echo(f"Executing job: {job_name}...")

    # Execute the command
    result = subprocess.run(cmd)

    if result.returncode == 0:
        click.echo(f"Successfully executed job: {job_name}")
    else:
        click.echo(f"Failed to execute job: {job_name}. Check logs for details.")


@dagster.command("list-assets")
@handle_exceptions()
def list_assets():
    """List all available Dagster assets in the data warehouse.

    This command displays all defined assets in the Dagster repository.

    Example:
        data-warehouse dagster list-assets
    """
    # Set environment variables
    os.environ["DAGSTER_HOME"] = str(settings.DAGSTER_HOME)

    # Construct the command to list assets
    cmd = ["dagster", "asset", "list", "-m", "src.data_warehouse.orchestration.dagster"]

    logger.info(f"Running command: {' '.join(cmd)}")

    click.echo("Listing Dagster assets:")

    # Execute the command
    subprocess.run(cmd)


@dagster.command("list-jobs")
@handle_exceptions()
def list_jobs():
    """List all available Dagster jobs in the data warehouse.

    This command displays all defined jobs in the Dagster repository.

    Example:
        data-warehouse dagster list-jobs
    """
    # Set environment variables
    os.environ["DAGSTER_HOME"] = str(settings.DAGSTER_HOME)

    # Construct the command to list jobs
    cmd = ["dagster", "job", "list", "-m", "src.data_warehouse.orchestration.dagster"]

    logger.info(f"Running command: {' '.join(cmd)}")

    click.echo("Listing Dagster jobs:")

    # Execute the command
    subprocess.run(cmd)


@dagster.command("list-schedules")
@handle_exceptions()
def list_schedules():
    """List all defined Dagster schedules in the data warehouse.

    This command displays all schedules defined in the Dagster repository.

    Example:
        data-warehouse dagster list-schedules
    """
    # Set environment variables
    os.environ["DAGSTER_HOME"] = str(settings.DAGSTER_HOME)

    # Construct the command to list schedules
    cmd = ["dagster", "schedule", "list", "-m", "src.data_warehouse.orchestration.dagster"]

    logger.info(f"Running command: {' '.join(cmd)}")

    click.echo("Listing Dagster schedules:")

    # Execute the command
    subprocess.run(cmd)


@dagster.command("wipe")
@click.option("--yes", is_flag=True, help="Skip confirmation prompt.")
@handle_exceptions()
def wipe_storage():
    """Wipe all Dagster storage, including run history and event logs.

    This is a destructive operation that will delete all Dagster run history,
    event logs, and other persisted data. Use with caution!

    Example:
        data-warehouse dagster wipe --yes
    """
    if not click.confirm(
        "This will delete all Dagster run history and cannot be undone. Are you sure?",
        default=False,
        abort=True,
        show_default=True,
    ):
        return

    # Set environment variables
    os.environ["DAGSTER_HOME"] = str(settings.DAGSTER_HOME)

    # Construct the command to wipe storage
    cmd = ["dagster", "instance", "wipe", "--yes"]

    logger.info(f"Running command: {' '.join(cmd)}")

    click.echo("Wiping Dagster storage...")

    # Execute the command
    result = subprocess.run(cmd)

    if result.returncode == 0:
        click.echo("Successfully wiped Dagster storage.")
    else:
        click.echo("Failed to wipe Dagster storage. Check logs for details.")


if __name__ == "__main__":
    dagster()
