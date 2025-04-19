"""
Transformed data assets for the data warehouse.

This module defines Dagster assets that transform raw data into cleaned,
validated, and enriched forms for the transformed layer of the data warehouse.
"""

import pandas as pd
from dagster import AssetExecutionContext, AssetIn, asset  # type: ignore

from src.data_warehouse.orchestration.dagster.resources.database import PostgresResource
from src.data_warehouse.utils.transformations import (
    clean_customer_data,
    clean_order_data,
    clean_product_data,
    clean_store_location_data,
    enrich_inventory_data,
)


@asset(
    io_manager_key="postgres_io_manager",
    key_prefix=["transformed"],
    compute_kind="pandas",
    group_name="transformed_data",
    ins={
        "raw_data": AssetIn(key=["raw", "raw_customers"]),
    },
)
def customers(context: AssetExecutionContext, raw_data: pd.DataFrame) -> pd.DataFrame:
    """Transform raw customer data into cleaned and standardized format.

    This asset cleans and validates customer data from the raw layer,
    applying business rules and data quality standards.

    Args:
        context: Asset execution context
        raw_data: Raw customer data

    Returns:
        Cleaned and transformed customer data
    """
    context.log.info(f"Transforming {len(raw_data)} raw customer records")

    # Apply transformations
    transformed_df = clean_customer_data(raw_data)

    # Log transformation statistics
    initial_count = len(raw_data)
    final_count = len(transformed_df)
    dropped_count = initial_count - final_count

    context.log.info(f"Transformation complete: {final_count} records kept, {dropped_count} records filtered out")

    return transformed_df


@asset(
    io_manager_key="postgres_io_manager",
    key_prefix=["transformed"],
    compute_kind="pandas",
    group_name="transformed_data",
    ins={
        "raw_data": AssetIn(key=["raw", "raw_orders"]),
    },
)
def orders(context: AssetExecutionContext, raw_data: pd.DataFrame) -> pd.DataFrame:
    """Transform raw order data into cleaned and standardized format.

    This asset cleans and validates order data from the raw layer,
    applying business rules and data quality standards.

    Args:
        context: Asset execution context
        raw_data: Raw order data

    Returns:
        Cleaned and transformed order data
    """
    context.log.info(f"Transforming {len(raw_data)} raw order records")

    # Apply transformations
    transformed_df = clean_order_data(raw_data)

    # Log transformation statistics
    initial_count = len(raw_data)
    final_count = len(transformed_df)
    dropped_count = initial_count - final_count

    context.log.info(f"Transformation complete: {final_count} records kept, {dropped_count} records filtered out")

    return transformed_df


@asset(
    io_manager_key="postgres_io_manager",
    key_prefix=["transformed"],
    compute_kind="pandas",
    group_name="transformed_data",
    ins={
        "raw_data": AssetIn(key=["raw", "raw_products"]),
    },
)
def products(context: AssetExecutionContext, raw_data: pd.DataFrame) -> pd.DataFrame:
    """Transform raw product data into cleaned and standardized format.

    This asset cleans and validates product data from the raw layer,
    applying business rules and data quality standards.

    Args:
        context: Asset execution context
        raw_data: Raw product data

    Returns:
        Cleaned and transformed product data
    """
    context.log.info(f"Transforming {len(raw_data)} raw product records")

    # Apply transformations
    transformed_df = clean_product_data(raw_data)

    # Log transformation statistics
    initial_count = len(raw_data)
    final_count = len(transformed_df)
    dropped_count = initial_count - final_count

    context.log.info(f"Transformation complete: {final_count} records kept, {dropped_count} records filtered out")

    return transformed_df


@asset(
    io_manager_key="postgres_io_manager",
    key_prefix=["transformed"],
    compute_kind="pandas",
    group_name="transformed_data",
    ins={
        "raw_data": AssetIn(key=["raw", "raw_store_locations"]),
    },
)
def store_locations(context: AssetExecutionContext, raw_data: pd.DataFrame) -> pd.DataFrame:
    """Transform raw store location data into cleaned and standardized format.

    This asset cleans and validates store location data from the raw layer,
    applying business rules and data quality standards.

    Args:
        context: Asset execution context
        raw_data: Raw store location data

    Returns:
        Cleaned and transformed store location data
    """
    context.log.info(f"Transforming {len(raw_data)} raw store location records")

    # Apply transformations
    transformed_df = clean_store_location_data(raw_data)

    # Log transformation statistics
    initial_count = len(raw_data)
    final_count = len(transformed_df)
    dropped_count = initial_count - final_count

    context.log.info(f"Transformation complete: {final_count} records kept, {dropped_count} records filtered out")

    return transformed_df


@asset(
    io_manager_key="postgres_io_manager",
    key_prefix=["transformed"],
    compute_kind="pandas",
    group_name="transformed_data",
    ins={
        "raw_data": AssetIn(key=["raw", "raw_inventory"]),
        "products": AssetIn(key=["transformed", "products"]),
        "store_locations": AssetIn(key=["transformed", "store_locations"]),
    },
)
def inventory(
    context: AssetExecutionContext,
    raw_data: pd.DataFrame,
    products: pd.DataFrame,
    store_locations: pd.DataFrame,
) -> pd.DataFrame:
    """Transform and enrich raw inventory data.

    This asset cleans inventory data and enriches it with product and
    store location information.

    Args:
        context: Asset execution context
        raw_data: Raw inventory data
        products: Transformed product data
        store_locations: Transformed store location data

    Returns:
        Enriched inventory data
    """
    context.log.info(f"Transforming {len(raw_data)} raw inventory records")

    # Apply transformations and enrichment
    transformed_df = enrich_inventory_data(raw_data, products, store_locations)

    # Log transformation statistics
    len(raw_data)
    final_count = len(transformed_df)

    context.log.info(f"Transformation complete: {final_count} records processed")

    return transformed_df


@asset(  # type: ignore
    group_name="transformed",
    io_manager_key="postgres_io_manager",
    required_resource_keys={"postgres"},
    description="Transformed data layer",
)
def transformed_data(context: AssetExecutionContext, postgres: PostgresResource) -> pd.DataFrame:
    """
    Transform staging data into the transformed layer.

    This is a sample asset that demonstrates the transformed data layer using Dagster.
    In a real implementation, this would apply business logic and transformations.
    """
    # ... rest of the existing code ...
