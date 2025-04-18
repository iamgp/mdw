"""
Raw data assets for the data warehouse.

This module defines Dagster assets that extract data from external sources
and load it into the raw layer of the data warehouse.
"""

import pandas as pd
from dagster import AssetExecutionContext, asset

from src.data_warehouse.sources.api_client import APIClient
from src.data_warehouse.sources.file_client import FileClient


@asset(
    io_manager_key="postgres_io_manager",
    key_prefix=["raw"],
    compute_kind="API",
    group_name="raw_data",
)
def raw_customers(context: AssetExecutionContext) -> pd.DataFrame:
    """Extract customer data from the API and load it into the raw layer.

    This asset fetches customer data from an external API and loads it
    into the raw layer of the data warehouse without transformation.

    Returns:
        DataFrame containing raw customer data
    """
    context.log.info("Extracting raw customer data from API")

    # Initialize API client
    api_client = APIClient(base_url=context.resources.api.base_url)

    # Fetch customer data
    customer_data = api_client.get_customers()

    # Convert to DataFrame
    df = pd.DataFrame(customer_data)

    context.log.info(f"Extracted {len(df)} customer records")

    # Return without transformation
    return df


@asset(
    io_manager_key="postgres_io_manager",
    key_prefix=["raw"],
    compute_kind="API",
    group_name="raw_data",
)
def raw_orders(context: AssetExecutionContext) -> pd.DataFrame:
    """Extract order data from the API and load it into the raw layer.

    This asset fetches order data from an external API and loads it
    into the raw layer of the data warehouse without transformation.

    Returns:
        DataFrame containing raw order data
    """
    context.log.info("Extracting raw order data from API")

    # Initialize API client
    api_client = APIClient(base_url=context.resources.api.base_url)

    # Fetch order data
    order_data = api_client.get_orders()

    # Convert to DataFrame
    df = pd.DataFrame(order_data)

    context.log.info(f"Extracted {len(df)} order records")

    # Return without transformation
    return df


@asset(
    io_manager_key="postgres_io_manager",
    key_prefix=["raw"],
    compute_kind="API",
    group_name="raw_data",
)
def raw_products(context: AssetExecutionContext) -> pd.DataFrame:
    """Extract product data from the API and load it into the raw layer.

    This asset fetches product data from an external API and loads it
    into the raw layer of the data warehouse without transformation.

    Returns:
        DataFrame containing raw product data
    """
    context.log.info("Extracting raw product data from API")

    # Initialize API client
    api_client = APIClient(base_url=context.resources.api.base_url)

    # Fetch product data
    product_data = api_client.get_products()

    # Convert to DataFrame
    df = pd.DataFrame(product_data)

    context.log.info(f"Extracted {len(df)} product records")

    # Return without transformation
    return df


@asset(
    io_manager_key="postgres_io_manager",
    key_prefix=["raw"],
    compute_kind="File",
    group_name="raw_data",
)
def raw_store_locations(context: AssetExecutionContext) -> pd.DataFrame:
    """Extract store location data from CSV files.

    This asset reads store location data from CSV files and loads it
    into the raw layer of the data warehouse without transformation.

    Returns:
        DataFrame containing raw store location data
    """
    context.log.info("Extracting raw store location data from files")

    # Initialize file client
    file_client = FileClient(data_dir=context.resources.file_system.data_dir)

    # Read store location data
    df = file_client.read_csv("store_locations.csv")

    context.log.info(f"Extracted {len(df)} store location records")

    # Return without transformation
    return df


@asset(
    io_manager_key="postgres_io_manager",
    key_prefix=["raw"],
    compute_kind="File",
    group_name="raw_data",
)
def raw_inventory(context: AssetExecutionContext) -> pd.DataFrame:
    """Extract inventory data from CSV files.

    This asset reads inventory data from CSV files and loads it
    into the raw layer of the data warehouse without transformation.

    Returns:
        DataFrame containing raw inventory data
    """
    context.log.info("Extracting raw inventory data from files")

    # Initialize file client
    file_client = FileClient(data_dir=context.resources.file_system.data_dir)

    # Read inventory data
    df = file_client.read_csv("inventory.csv")

    context.log.info(f"Extracted {len(df)} inventory records")

    # Return without transformation
    return df
