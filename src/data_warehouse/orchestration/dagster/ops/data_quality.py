"""
Data quality operations for Dagster.

This module provides operations for validating data quality in Dagster pipelines.
"""

import pandas as pd
from dagster import Field, OpExecutionContext, Out, op


@op(
    description="Validates data against quality checks",
    out={"validated_data": Out(description="Data that passed validation")},
    config_schema={
        "min_rows": Field(int, default_value=1, description="Minimum number of rows required"),
        "required_columns": Field([str], default_value=[], description="List of columns that must be present"),
        "max_nulls_percent": Field(float, default_value=10.0, description="Maximum percentage of null values allowed"),
    },
)
def validate_data(context: OpExecutionContext, data: pd.DataFrame) -> pd.DataFrame:
    """
    Validate data against quality checks and raise an exception if it fails.

    Args:
        context: The Dagster execution context
        data: The DataFrame to validate

    Returns:
        The validated DataFrame (unchanged if all checks pass)

    Raises:
        Exception: If data fails any quality checks
    """
    # Check minimum rows
    min_rows = context.op_config["min_rows"]
    if len(data) < min_rows:
        raise Exception(f"Data has {len(data)} rows, but minimum {min_rows} required")

    # Check required columns
    required_columns = context.op_config["required_columns"]
    missing_columns = [col for col in required_columns if col not in data.columns]
    if missing_columns:
        raise Exception(f"Data is missing required columns: {missing_columns}")

    # Check for null values
    max_nulls_percent = context.op_config["max_nulls_percent"]
    null_percentages = data.isnull().mean() * 100
    columns_with_too_many_nulls = null_percentages[null_percentages > max_nulls_percent].index.tolist()
    if columns_with_too_many_nulls:
        null_details = {col: f"{null_percentages[col]:.2f}%" for col in columns_with_too_many_nulls}
        raise Exception(
            f"The following columns exceed the maximum null percentage ({max_nulls_percent}%): {null_details}"
        )

    context.log.info(f"Data passed all quality checks: {len(data)} rows, {len(data.columns)} columns")
    return data
