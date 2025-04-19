"""
Warehouse data assets for Dagster.

This module defines assets for final data warehouse models and aggregations.
"""

import pandas as pd
from dagster import AssetExecutionContext, asset  # type: ignore

from src.data_warehouse.orchestration.dagster.resources.database import DuckDBResource


@asset(
    group_name="warehouse",
    io_manager_key="duckdb_io_manager",
    deps=["staging_data"],
    required_resource_keys={"duckdb"},
    description="Final data warehouse model with aggregations",
)
def warehouse_data(context: AssetExecutionContext, duckdb: DuckDBResource, staging_data: pd.DataFrame) -> pd.DataFrame:
    """
    Create final warehouse data model with aggregations.

    This asset takes staging data and creates the final warehouse model
    with appropriate aggregations and transformations for analytics.
    """
    context.log.info(f"Creating warehouse data model from {len(staging_data)} rows of staging data")

    # Sample aggregation for demonstration
    # Group by category and calculate aggregations
    agg_df = (
        staging_data.groupby("category")
        .agg(
            count=("id", "count"),
            avg_value=("value", "mean"),
            min_value=("value", "min"),
            max_value=("value", "max"),
            earliest_date=("date", "min"),
            latest_date=("date", "max"),
        )
        .reset_index()
    )

    context.log.info(f"Created warehouse model with {len(agg_df)} aggregated rows")

    # In a real implementation, we would use the duckdb resource to store the data
    # with duckdb.get_connection() as conn:
    #     # Store aggregated data
    #     # ...

    return agg_df
