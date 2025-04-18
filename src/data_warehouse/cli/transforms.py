"""Transformation-related CLI commands."""

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
