"""
Dagster repository definition.

This module defines the Dagster repository for the data warehouse project.
"""

from dagster import Definitions, load_assets_from_modules, repository

from src.data_warehouse.orchestration.dagster import assets
from src.data_warehouse.orchestration.dagster.resources.database import (
    duckdb_resource,
    postgres_resource,
)
from src.data_warehouse.orchestration.dagster.resources.io_managers import (
    duckdb_io_manager,
    postgres_io_manager,
)
from src.data_warehouse.orchestration.dagster.schedules import daily_schedule
from src.data_warehouse.orchestration.dagster.sensors import new_data_sensor

# Create the definitions object that combines all components
defs = Definitions(
    assets=load_assets_from_modules([assets]),
    schedules=[daily_schedule],
    sensors=[new_data_sensor],
    resources={
        "postgres": postgres_resource.configured(
            {
                "host": "localhost",
                "port": 5432,
                "user": "postgres",
                "password": "postgres",
                "database": "data_warehouse",
            }
        ),
        "duckdb": duckdb_resource.configured(
            {
                "database_path": "data/analytics.duckdb",
            }
        ),
        "postgres_io_manager": postgres_io_manager,
        "duckdb_io_manager": duckdb_io_manager,
    },
)


# For backward compatibility with older Dagster versions
@repository
def data_warehouse_repository():
    """Define the data_warehouse repository."""
    return [
        *defs.get_all_pipelines(),
        *defs.get_all_jobs(),
        *defs.get_all_assets(),
        *defs.get_all_schedules(),
        *defs.get_all_sensors(),
    ]
