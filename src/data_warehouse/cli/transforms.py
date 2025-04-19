"""Transformation-related CLI commands."""

import os
import subprocess
from pathlib import Path

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
@click.option("--store-results", is_flag=True, help="Store test results in the monitoring database.")
@click.option(
    "--project-dir",
    type=click.Path(exists=True, file_okay=False, dir_okay=True, path_type=Path),
    help="DBT project directory.",
)
@click.option("--db-url", help="Database URL for storing results, defaults to configured database.")
@handle_exceptions()
def test_dbt(
    model: str | None = None, store_results: bool = False, project_dir: Path | None = None, db_url: str | None = None
):
    """Run DBT tests on the data models.

    If a model is specified, only tests for that model will be run.
    Otherwise, all tests will be run.

    Examples:
        data-warehouse transforms test-dbt
        data-warehouse transforms test-dbt customer_model
        data-warehouse transforms test-dbt --store-results
    """
    # Save current directory to return to it later
    original_dir = os.getcwd()

    if project_dir is None:
        project_dir = Path(original_dir) / "dbt_postgres"

    # Change to the DBT project directory
    os.chdir(project_dir)

    # Build the DBT test command
    cmd_parts = ["dbt", "test"]
    if model:
        cmd_parts.extend(["--select", model])

    cmd = cmd_parts
    click.echo(f"Running DBT tests: {' '.join(cmd)}")

    # Execute the DBT test command
    success = False
    try:
        result = subprocess.run(cmd, check=True)
        click.echo(f"DBT tests completed with exit code: {result.returncode}")
        success = True
    except subprocess.CalledProcessError as e:
        click.echo(f"DBT tests failed with exit code: {e.returncode}")
        if hasattr(e, "stderr") and e.stderr:
            click.echo(f"Error: {e.stderr.decode('utf-8')}")
    finally:
        # Store test results if requested (regardless of success/failure)
        if store_results:
            target_path = os.path.join(project_dir, "target")
            click.echo(f"{'Successfully completed' if success else 'Failed'} tests, logging results...")
            log_results(target_path, db_url)
        os.chdir(original_dir)


def log_results(target_path: str | Path, db_url: str | None = None) -> None:
    """Log DBT test results to the database

    Args:
        target_path: Path to the DBT target directory containing run_results.json
        db_url: Optional database URL for storing results, defaults to configured database if None
    """
    try:
        # Convert target_path to Path object if it's a string
        if isinstance(target_path, str):
            target_path = Path(target_path)

        # Verify target path exists
        if not target_path.exists():
            click.echo(f"Error: Target directory {target_path} not found")
            return

        # Find the script path
        script_path = _find_script_path()
        if script_path is None:
            click.secho("Error: Could not find log_dbt_test_results.py script in any expected location", fg="red")
            return

        # Build and execute the command
        _execute_logging_command(script_path, target_path, db_url)

    except subprocess.CalledProcessError as e:
        click.echo(f"Failed to log test results: {e}")
        if hasattr(e, "stderr") and e.stderr:
            click.echo(f"Error details: {e.stderr}")
    except Exception as ex:
        click.echo(f"Unexpected error logging test results: {ex}")


def _find_script_path() -> Path | None:
    """Find the path to the log_dbt_test_results.py script.

    Returns:
        Path to the script if found, None otherwise
    """
    # Import here to avoid circular imports
    # Try multiple potential locations for the script
    potential_script_paths = [
        Path(__file__).parent.parent.parent.parent / "scripts" / "log_dbt_test_results.py",
        Path.cwd() / "scripts" / "log_dbt_test_results.py",
    ]

    for path in potential_script_paths:
        if path.exists():
            return path

    return None


def _execute_logging_command(script_path: Path, target_path: Path, db_url: str | None) -> None:
    """Execute the log_dbt_test_results.py script.

    Args:
        script_path: Path to the script
        target_path: Path to the DBT target directory
        db_url: Optional database URL
    """
    # Handle None db_url by using empty string
    cmd = ["python", str(script_path), "--target-dir", str(target_path)]
    if db_url:
        cmd.extend(["--db-url", db_url])
    click.echo(f"Logging test results with command: {' '.join(cmd)}")

    result = subprocess.run(cmd, check=True, capture_output=True, text=True)
    click.echo(f"Test results logging completed with exit code: {result.returncode}")

    # Display output from the script for better visibility
    if result.stdout:
        click.echo(f"Output: {result.stdout}")


@transforms.command("init-dbt-projects")
@click.option("--postgres-name", default="dbt_postgres", help="Name for the Postgres DBT project directory.")
@click.option("--duckdb-name", default="dbt_duckdb", help="Name for the DuckDB DBT project directory.")
@click.option("--common-name", default="dbt_common", help="Name for the shared DBT macros/tests directory.")
@handle_exceptions()
def init_dbt_projects(postgres_name: str, duckdb_name: str, common_name: str):
    """Non-interactively create DBT projects for Postgres and DuckDB, plus a shared directory for macros/tests."""
    base_dir = Path.cwd()
    projects = [(postgres_name, "postgres"), (duckdb_name, "duckdb")]
    for project_name, adapter in projects:
        project_dir = base_dir / project_name
        if not project_dir.exists():
            project_dir.mkdir(parents=True)
            click.echo(f"Created directory: {project_dir}")
        # Create minimal dbt_project.yml
        dbt_project_yml = project_dir / "dbt_project.yml"
        if not dbt_project_yml.exists():
            dbt_project_yml.write_text(f"""
name: '{project_name}'
version: '1.0.0'
config-version: 2
profile: '{project_name}'

model-paths: ['models']
seed-paths: ['seeds']
macro-paths: ['macros']
test-paths: ['tests']

# Add more config as needed
""")
            click.echo(f"Created {dbt_project_yml}")
        # Create standard directories
        for subdir in ["models", "macros", "tests", "seeds", "snapshots", "analyses"]:
            d = project_dir / subdir
            if not d.exists():
                d.mkdir()
        # Add README
        readme = project_dir / "README.md"
        if not readme.exists():
            readme.write_text(f"# {project_name}\n\nDBT project for {adapter}.")
    # Shared directory for macros/tests
    common_dir = base_dir / common_name
    if not common_dir.exists():
        common_dir.mkdir(parents=True)
        click.echo(f"Created directory: {common_dir}")
    readme_path = common_dir / "README.md"
    if not readme_path.exists():
        readme_path.write_text(
            """# Shared DBT Macros and Tests\n\nPlace common macros and tests here. Symlink or copy into each DBT project as needed."""
        )
        click.echo(f"Created {readme_path}")
    click.secho("DBT projects and shared directory initialized.", fg="green")
    # Output profiles.yml snippet
    click.secho("\nAdd the following to your ~/.dbt/profiles.yml:\n", fg="yellow")
    click.echo(f"""
{postgres_name}:
  target: dev
  outputs:
    dev:
      type: postgres
      host: localhost
      user: postgres
      password: yourpassword
      port: 5432
      dbname: data_warehouse
      schema: public
      threads: 4
      keepalives_idle: 0
      connect_timeout: 10
      search_path: public
      sslmode: prefer

{duckdb_name}:
  target: dev
  outputs:
    dev:
      type: duckdb
      path: ./data_warehouse.duckdb
      threads: 4
      schema: main
""")


@transforms.command("log-results")
@click.option(
    "--results-path",
    default=None,
    help="Path to the DBT run_results.json file (default: attempts to find latest).",
    type=click.Path(dir_okay=False, path_type=Path),
)
@click.option(
    "--db-target",
    type=click.Choice(["postgres", "duckdb"]),
    default="postgres",
    help="Target database to log results to.",
)
@click.option(
    "--dbt-project",
    type=click.Choice(["postgres", "duckdb"]),
    default="postgres",
    help="DBT project directory containing target/run_results.json (e.g., dbt_postgres or dbt_duckdb).",
)
def log_test_results(results_path: Path | None, db_target: str, dbt_project: str):
    """Parse DBT test results from run_results.json and log them to the monitoring database."""
    project_dir = Path.cwd() / f"dbt_{dbt_project}"
    if results_path is None:
        results_path = project_dir / "target" / "run_results.json"
        if not results_path.exists():
            click.secho(f"Error: Could not find default run_results.json at {results_path}", fg="red")
            click.echo(
                "Please run 'dbt test' in the appropriate project first or provide the path using --results-path."
            )
            return

    if not results_path.exists():
        click.secho(f"Error: Specified results file not found: {results_path}", fg="red")
        return

    # Construct the command to run the standalone script
    cmd = [
        "python",
        str(Path.cwd() / "scripts" / "log_dbt_test_results.py"),
        "--results-path",
        str(results_path.resolve()),
        "--db-target",
        db_target,
    ]
    logger.info(f"Running command: {' '.join(cmd)}")
    try:
        subprocess.run(cmd, check=True, capture_output=True, text=True)
        logger.info("Successfully logged DBT test results.")
    except subprocess.CalledProcessError as e:
        logger.error(f"Error logging DBT test results: {e}")
        logger.error(f"Stderr: {e.stderr}")
        logger.error(f"Stdout: {e.stdout}")
        click.secho("Error running results logging script. Check logs.", fg="red")


if __name__ == "__main__":
    transforms()
