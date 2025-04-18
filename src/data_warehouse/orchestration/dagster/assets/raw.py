"""
Raw data assets for Dagster.

This module defines assets for ingesting raw data into the data warehouse.
"""

import pandas as pd
from dagster import AssetExecutionContext, asset

from src.data_warehouse.orchestration.dagster.resources.database import PostgresResource


@asset(
    group_name="raw",
    io_manager_key="postgres_io_manager",
    required_resource_keys={"postgres"},
    description="Raw data from source system",
)
def raw_data(context: AssetExecutionContext, postgres: PostgresResource) -> pd.DataFrame:
    """
    Extract raw data from source system.

    This is a sample asset that demonstrates how to extract data using Dagster.
    In a real implementation, this would connect to a source system and extract data.
    """
    context.log.info("Extracting raw data from source system")

    # Sample data for demonstration
    data = {
        "id": [1, 2, 3, 4, 5],
        "name": ["Item 1", "Item 2", "Item 3", "Item 4", "Item 5"],
        "value": [10.5, 20.3, 30.1, 40.9, 50.7],
        "category": ["A", "B", "A", "C", "B"],
        "date": ["2023-01-01", "2023-01-02", "2023-01-03", "2023-01-04", "2023-01-05"],
    }

    df = pd.DataFrame(data)
    context.log.info(f"Extracted {len(df)} rows of raw data")

    # In a real implementation, we would use the postgres resource to store the data
    # For example:
    # with postgres.get_connection() as conn:
    #     with conn.cursor() as cur:
    #         # Store data in PostgreSQL
    #         # ...

    return df
