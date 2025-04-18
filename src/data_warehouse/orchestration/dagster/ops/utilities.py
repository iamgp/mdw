"""
Utility operations for Dagster.

This module provides utility operations for Dagster pipelines, such as logging, metrics, etc.
"""

import pandas as pd
from dagster import OpExecutionContext, op


@op(
    description="Logs metrics about a DataFrame",
)
def log_metrics(context: OpExecutionContext, data: pd.DataFrame, step_name: str | None = None) -> pd.DataFrame:
    """
    Log metrics about a DataFrame, such as row count, column counts, etc.

    Args:
        context: The Dagster execution context
        data: The DataFrame to log metrics about
        step_name: Optional name of the processing step to include in logs

    Returns:
        The input DataFrame (unchanged)
    """
    prefix = f"[{step_name}] " if step_name else ""

    # Basic metrics
    row_count = len(data)
    column_count = len(data.columns)
    context.log.info(f"{prefix}DataFrame has {row_count} rows and {column_count} columns")

    # Column-level metrics
    for column in data.columns:
        null_count = data[column].isnull().sum()
        null_percent = (null_count / row_count * 100) if row_count > 0 else 0

        # Log different details based on column type
        if pd.api.types.is_numeric_dtype(data[column]):
            # Numeric column
            context.log.info(
                f"{prefix}Column '{column}' (numeric): "
                f"min={data[column].min()}, "
                f"max={data[column].max()}, "
                f"mean={data[column].mean():.2f}, "
                f"null={null_count} ({null_percent:.1f}%)"
            )
        elif pd.api.types.is_datetime64_dtype(data[column]):
            # Datetime column
            context.log.info(
                f"{prefix}Column '{column}' (datetime): "
                f"min={data[column].min()}, "
                f"max={data[column].max()}, "
                f"null={null_count} ({null_percent:.1f}%)"
            )
        else:
            # String or other column
            unique_count = data[column].nunique()
            context.log.info(
                f"{prefix}Column '{column}' (other): "
                f"unique values={unique_count}, "
                f"null={null_count} ({null_percent:.1f}%)"
            )

    # In a real implementation, we might also send these metrics to a monitoring system
    # like Prometheus or record them in Dagster's event log for visualization

    return data
