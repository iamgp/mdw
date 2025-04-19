"""
Data transformation utility functions.

This module provides functions for cleaning, validating, and enriching
data from various sources for the data warehouse.
"""

import re
from datetime import datetime

import numpy as np
import pandas as pd


def clean_customer_data(df: pd.DataFrame) -> pd.DataFrame:
    """Clean and standardize customer data.

    Args:
        df: Raw customer data DataFrame

    Returns:
        Cleaned customer DataFrame
    """
    # Create a copy to avoid modifying the original
    result_df = df.copy()

    # Drop duplicates based on customer_id
    result_df.drop_duplicates(subset=["customer_id"], keep="last", inplace=True)

    # Fill missing values
    result_df["email"] = result_df["email"].fillna("")
    result_df["phone"] = result_df["phone"].fillna("")

    # Standardize phone numbers (remove non-digit characters)
    result_df["phone"] = result_df["phone"].apply(lambda x: re.sub(r"\D", "", str(x)) if pd.notna(x) else "")

    # Convert names to title case
    for col in ["first_name", "last_name"]:
        if col in result_df.columns:
            result_df[col] = result_df[col].str.title()

    # Remove rows with missing critical data
    result_df = result_df.dropna(subset=["customer_id", "first_name", "last_name"])

    # Add standardized full_name column
    result_df["full_name"] = result_df["first_name"] + " " + result_df["last_name"]

    # Add data quality columns
    result_df["has_email"] = result_df["email"].str.len() > 0
    result_df["has_phone"] = result_df["phone"].str.len() > 0
    result_df["last_updated"] = datetime.now()

    return result_df


def clean_order_data(df: pd.DataFrame) -> pd.DataFrame:
    """Clean and standardize order data.

    Args:
        df: Raw order data DataFrame

    Returns:
        Cleaned order DataFrame
    """
    # Create a copy to avoid modifying the original
    result_df = df.copy()

    # Drop duplicates based on order_id
    result_df.drop_duplicates(subset=["order_id"], keep="last", inplace=True)

    # Convert date columns to datetime
    date_columns = ["order_date", "ship_date", "delivery_date"]
    for col in date_columns:
        if col in result_df.columns:
            result_df[col] = pd.to_datetime(result_df[col], errors="coerce")

    # Calculate order_total if not present
    if "order_total" not in result_df.columns and "item_price" in result_df.columns and "quantity" in result_df.columns:
        result_df["order_total"] = result_df["item_price"] * result_df["quantity"]

    # Remove orders with missing critical data
    result_df = result_df.dropna(subset=["order_id", "customer_id", "order_date"])

    # Add derived columns
    if "ship_date" in result_df.columns and "order_date" in result_df.columns:
        result_df["processing_days"] = (result_df["ship_date"] - result_df["order_date"]).dt.days

        # Handle negative values (incorrect dates)
        result_df.loc[result_df["processing_days"] < 0, "processing_days"] = np.nan

    # Add status columns
    if "order_status" not in result_df.columns:
        result_df["order_status"] = "Unknown"
        if "ship_date" in result_df.columns:
            result_df.loc[pd.notna(result_df["ship_date"]), "order_status"] = "Shipped"
        if "delivery_date" in result_df.columns:
            result_df.loc[pd.notna(result_df["delivery_date"]), "order_status"] = "Delivered"

    # Add last_updated timestamp
    result_df["last_updated"] = datetime.now()

    return result_df


def clean_product_data(df: pd.DataFrame) -> pd.DataFrame:
    """Clean and standardize product data.

    Args:
        df: Raw product data DataFrame

    Returns:
        Cleaned product DataFrame
    """
    # Create a copy to avoid modifying the original
    result_df = df.copy()

    # Drop duplicates based on product_id
    result_df.drop_duplicates(subset=["product_id"], keep="last", inplace=True)

    # Cleanup product names
    if "product_name" in result_df.columns:
        # Trim whitespace
        result_df["product_name"] = result_df["product_name"].str.strip()

        # Replace missing names with placeholder
        result_df["product_name"] = result_df["product_name"].fillna("Unknown Product")

    # Standardize price columns
    for col in ["price", "cost", "msrp"]:
        if col in result_df.columns:
            # Convert to numeric
            result_df[col] = pd.to_numeric(result_df[col], errors="coerce")

            # Replace negative values with NaN
            result_df.loc[result_df[col] < 0, col] = np.nan

    # Calculate margins if price and cost are available
    if "price" in result_df.columns and "cost" in result_df.columns:
        result_df["margin"] = result_df["price"] - result_df["cost"]
        result_df["margin_percent"] = (result_df["margin"] / result_df["price"]) * 100

        # Handle division by zero
        result_df.loc[result_df["price"] == 0, "margin_percent"] = 0

    # Remove rows with missing critical data
    result_df = result_df.dropna(subset=["product_id"])

    # Add last_updated timestamp
    result_df["last_updated"] = datetime.now()

    return result_df


def clean_store_location_data(df: pd.DataFrame) -> pd.DataFrame:
    """Clean and standardize store location data.

    Args:
        df: Raw store location data DataFrame

    Returns:
        Cleaned store location DataFrame
    """
    # Create a copy to avoid modifying the original
    result_df = df.copy()

    # Drop duplicates based on store_id
    result_df.drop_duplicates(subset=["store_id"], keep="last", inplace=True)

    # Standardize address fields
    address_cols = ["address", "city", "state", "zip_code", "country"]
    for col in address_cols:
        if col in result_df.columns:
            # Trim whitespace
            result_df[col] = result_df[col].str.strip()

            # Convert state and country to uppercase
            if col in ["state", "country"]:
                result_df[col] = result_df[col].str.upper()

    # Standardize zip codes
    if "zip_code" in result_df.columns:
        # Keep only numeric parts for US zip codes
        result_df["zip_code"] = result_df["zip_code"].apply(
            lambda x: re.sub(r"[^0-9]", "", str(x)) if pd.notna(x) else ""
        )

    # Format phone numbers
    if "phone" in result_df.columns:
        result_df["phone"] = result_df["phone"].apply(lambda x: re.sub(r"\D", "", str(x)) if pd.notna(x) else "")

    # Convert date columns to datetime
    date_columns = ["opening_date", "closing_date"]
    for col in date_columns:
        if col in result_df.columns:
            result_df[col] = pd.to_datetime(result_df[col], errors="coerce")

    # Add is_active flag
    result_df["is_active"] = True
    if "closing_date" in result_df.columns:
        result_df.loc[pd.notna(result_df["closing_date"]), "is_active"] = False

    # Remove rows with missing critical data
    result_df = result_df.dropna(subset=["store_id"])

    # Add last_updated timestamp
    result_df["last_updated"] = datetime.now()

    return result_df


def enrich_inventory_data(
    inventory_df: pd.DataFrame, products_df: pd.DataFrame, store_locations_df: pd.DataFrame
) -> pd.DataFrame:
    """Enrich inventory data with product and store information.

    Args:
        inventory_df: Raw inventory data
        products_df: Product data for enrichment
        store_locations_df: Store location data for enrichment

    Returns:
        Enriched inventory data
    """
    # Create a copy to avoid modifying the original
    result_df = inventory_df.copy()

    # Merge with product data
    if "product_id" in result_df.columns and "product_id" in products_df.columns:
        # Select product columns to include
        product_cols = ["product_id", "product_name", "category", "price", "cost"]
        product_cols = [col for col in product_cols if col in products_df.columns]

        # Merge product data
        result_df = pd.merge(result_df, products_df[product_cols], on="product_id", how="left")

    # Merge with store location data
    if "store_id" in result_df.columns and "store_id" in store_locations_df.columns:
        # Select store location columns to include
        store_cols = ["store_id", "store_name", "city", "state", "is_active"]
        store_cols = [col for col in store_cols if col in store_locations_df.columns]

        # Merge store location data
        result_df = pd.merge(result_df, store_locations_df[store_cols], on="store_id", how="left")

    # Calculate inventory value
    if "quantity" in result_df.columns and "cost" in result_df.columns:
        result_df["inventory_value"] = result_df["quantity"] * result_df["cost"]

    if "quantity" in result_df.columns and "price" in result_df.columns:
        result_df["retail_value"] = result_df["quantity"] * result_df["price"]

    # Add inventory status based on quantity
    if "quantity" in result_df.columns:
        # Create inventory status column
        result_df["inventory_status"] = "In Stock"
        result_df.loc[result_df["quantity"] <= 0, "inventory_status"] = "Out of Stock"
        result_df.loc[result_df["quantity"] < 10, "inventory_status"] = "Low Stock"

    # Add last_updated timestamp
    result_df["last_updated"] = datetime.now()

    return result_df
