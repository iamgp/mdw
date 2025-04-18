import json
from pathlib import Path
from typing import Any

import click
from loguru import logger
from sqlalchemy import create_engine, text

from data_warehouse.config.settings import settings
from data_warehouse.utils.error_handler import handle_exceptions


def parse_dbt_results(results_path: Path) -> list[dict[str, Any]]:
    """Parses run_results.json to extract test results."""
    if not results_path.exists():
        logger.error(f"Results file not found: {results_path}")
        return []

    try:
        with open(results_path) as f:
            data = json.load(f)
    except json.JSONDecodeError:
        logger.error(f"Error decoding JSON from {results_path}")
        return []

    test_results: list[dict[str, Any]] = []
    invocation_id = data.get("metadata", {}).get("invocation_id")
    run_timestamp = data.get("metadata", {}).get("generated_at")

    for result in data.get("results", []):
        if result.get("status") == "error" or result.get("unique_id", "").startswith("test."):
            test_name = result.get("unique_id", "").split(".")[-2] if "." in result.get("unique_id", "") else None
            model_name = (
                result.get("unique_id", "").split(".")[-3] if len(result.get("unique_id", "").split(".")) > 3 else None
            )
            column_name = result.get("column_name")  # May not always be present

            test_results.append(
                {
                    "result_id": result.get("unique_id"),
                    "invocation_id": invocation_id,
                    "test_unique_id": result.get("unique_id"),
                    "test_name": test_name,
                    "test_type": result.get("adapter_response", {}).get("test_type"),  # Might vary
                    "model_name": model_name,
                    "column_name": column_name,
                    "status": result.get("status"),
                    "execution_time": result.get("timing", [{}])[0].get("elapsed"),
                    "failure_details": result.get("message"),
                    "rows_affected": result.get("failures"),  # Number of failing rows
                    "run_timestamp": run_timestamp,
                }
            )
    return test_results


def log_results_to_db(results: list[dict[str, Any]], db_url: str):
    """Logs parsed test results to the specified database."""
    if not results:
        logger.info("No test results to log.")
        return

    engine = create_engine(db_url)
    insert_query = text("""
        INSERT INTO monitoring.dbt_test_results (
            result_id, invocation_id, test_unique_id, test_name, test_type,
            model_name, column_name, status, execution_time, failure_details,
            rows_affected, run_timestamp
        ) VALUES (
            :result_id, :invocation_id, :test_unique_id, :test_name, :test_type,
            :model_name, :column_name, :status, :execution_time, :failure_details,
            :rows_affected, :run_timestamp
        )
        ON CONFLICT (result_id) DO UPDATE SET
            status = EXCLUDED.status,
            execution_time = EXCLUDED.execution_time,
            failure_details = EXCLUDED.failure_details,
            rows_affected = EXCLUDED.rows_affected,
            run_timestamp = EXCLUDED.run_timestamp;
    """)  # Using UPSERT for PostgreSQL compatibility

    try:
        with engine.connect() as connection:
            with connection.begin():  # Start transaction
                connection.execute(insert_query, results)
            logger.info(f"Successfully logged {len(results)} test results to the database.")
    except Exception as e:
        logger.error(f"Error logging results to database: {e}")


@click.command()
@click.option(
    "--results-path",
    default="dbt_postgres/target/run_results.json",  # Default to postgres project
    help="Path to the DBT run_results.json file.",
    type=click.Path(exists=True, dir_okay=False, path_type=Path),
)
@click.option(
    "--db-target",
    type=click.Choice(["postgres", "duckdb"]),
    default="postgres",
    help="Target database to log results to.",
)
@handle_exceptions()
def main(results_path: Path, db_target: str):
    """Parses DBT test results and logs them to the monitoring database."""
    logger.info(f"Parsing results from: {results_path}")
    parsed_results = parse_dbt_results(results_path)

    if db_target == "postgres":
        # Construct Postgres URL from settings
        db_url = f"postgresql://{settings.POSTGRES_USER}:{settings.POSTGRES_PASSWORD}@\
{settings.POSTGRES_HOST}:{settings.POSTGRES_PORT}/{settings.POSTGRES_DB}"
        # Ensure monitoring schema exists in Postgres
        engine = create_engine(db_url)
        try:
            with engine.connect() as connection:
                connection.execute(text("CREATE SCHEMA IF NOT EXISTS monitoring;"))
                connection.commit()  # Explicit commit needed for DDL outside transaction
        except Exception as e:
            logger.warning(f"Could not ensure monitoring schema exists in Postgres: {e}")
    elif db_target == "duckdb":
        # Construct DuckDB URL from settings
        db_url = f"duckdb:///{settings.DUCKDB_PATH.resolve()}"
        # Ensure monitoring schema exists in DuckDB
        engine = create_engine(db_url)
        try:
            with engine.connect() as connection:
                # Create table directly if not exists (no separate schema creation needed usually)
                with open("dbt_common/sql/create_monitoring_schema.sql") as f:
                    connection.execute(text(f.read()))
                connection.commit()
        except Exception as e:
            logger.warning(f"Could not ensure monitoring schema exists in DuckDB: {e}")
    else:
        logger.error(f"Unsupported database target: {db_target}")
        return

    logger.info(f"Logging results to {db_target} database...")
    log_results_to_db(parsed_results, db_url)

    # Check for failures and log a critical message if any exist
    failures = [r for r in parsed_results if r.get("status") in ["fail", "error"]]
    if failures:
        logger.critical(
            f"{len(failures)} DBT tests failed or errored. Check the 'monitoring.dbt_test_results' table or DBT artifacts for details."
        )
    else:
        logger.info("All DBT tests passed or were skipped.")


if __name__ == "__main__":
    main()
