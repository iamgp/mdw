"""
Dagster repository definition.

This module defines the Dagster repository that organizes all assets and jobs
in the data warehouse.
"""

from dagster import AssetSelection, Definitions, ScheduleDefinition, load_assets_from_modules

from src.data_warehouse.orchestration.dagster.assets import (
    marts,
    raw_data,
    transformed_data,
)
from src.data_warehouse.orchestration.dagster.resources.database import (
    duckdb_resource,
    postgres_resource,
)
from src.data_warehouse.orchestration.dagster.resources.io_managers import (
    duckdb_io_manager,
    postgres_io_manager,
)

# Load assets from modules
raw_data_assets = load_assets_from_modules([raw_data])
transformed_data_assets = load_assets_from_modules([transformed_data])
mart_assets = load_assets_from_modules([marts])

# Combine all assets
all_assets = [
    *raw_data_assets,
    *transformed_data_assets,
    *mart_assets,
]

# Define schedule for daily data pipeline
daily_warehouse_update = ScheduleDefinition(
    name="daily_warehouse_update",
    cron_schedule="0 5 * * *",  # Run daily at 5:00 AM
    asset_selection=AssetSelection.all(),
    execution_timezone="UTC",
)

# Create repository definition
defs = Definitions(
    assets=all_assets,
    schedules=[daily_warehouse_update],
    resources={
        "postgres": postgres_resource.configured(
            {
                "conn_string": {"env": "POSTGRES_CONNECTION_STRING"},
            }
        ),
        "duckdb": duckdb_resource.configured(
            {
                "database_path": {"env": "DUCKDB_DATABASE_PATH"},
            }
        ),
        "postgres_io_manager": postgres_io_manager.configured(
            {
                "conn_string": {"env": "POSTGRES_CONNECTION_STRING"},
            }
        ),
        "duckdb_io_manager": duckdb_io_manager.configured(
            {
                "db_path": {"env": "DUCKDB_DATABASE_PATH"},
            }
        ),
        "api": {
            "base_url": {"env": "API_BASE_URL"},
        },
        "file_system": {
            "data_dir": {"env": "DATA_FILES_DIRECTORY"},
        },
    },
)
