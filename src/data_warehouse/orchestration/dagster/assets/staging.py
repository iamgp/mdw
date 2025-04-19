"""
Staging data assets for Dagster.

This module defines assets for transforming raw data into a staging format.
"""

import pandas as pd
from dagster import AssetExecutionContext, asset  # type: ignore

from src.data_warehouse.orchestration.dagster.resources.database import PostgresResource


@asset(
    group_name="staging",
    io_manager_key="postgres_io_manager",
    deps=["raw_data"],
    required_resource_keys={"postgres"},
    description="Cleaned and transformed data ready for the warehouse",
)
def staging_data(context: AssetExecutionContext, postgres: PostgresResource, raw_data: pd.DataFrame) -> pd.DataFrame:
    """
    Transform raw data into staging format.

    This asset takes raw data and applies transformations like cleaning,
    normalization, and validation to prepare it for the warehouse layer.
    """
    context.log.info(f"Transforming {len(raw_data)} rows of raw data into staging format")

    # Sample transformation for demonstration
    df = raw_data.copy()

    # Convert date string to datetime
    df["date"] = pd.to_datetime(df["date"])

    # Standardize categories to uppercase
    df["category"] = df["category"].str.upper()

    # Add derived column
    df["value_category"] = df["value"].astype(str) + "-" + df["category"]

    context.log.info(f"Transformed data into {len(df)} rows of staging data")

    # In a real implementation, we would use the postgres resource to store the data
    # with postgres.get_connection() as conn:
    #     # Store transformed data
    #     # ...

    return df
