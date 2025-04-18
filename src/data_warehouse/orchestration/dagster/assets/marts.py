"""
Data mart assets for the data warehouse.

This module defines Dagster assets that create data marts by aggregating
transformed data for business analytics and reporting.
"""

import pandas as pd
from dagster import AssetExecutionContext, AssetIn, asset


@asset(
    io_manager_key="duckdb_io_manager",
    key_prefix=["marts"],
    compute_kind="pandas",
    group_name="data_marts",
    ins={
        "orders": AssetIn(key=["transformed", "orders"]),
        "customers": AssetIn(key=["transformed", "customers"]),
    },
)
def customer_orders_mart(context: AssetExecutionContext, orders: pd.DataFrame, customers: pd.DataFrame) -> pd.DataFrame:
    """Create customer orders mart for sales analysis.

    This asset aggregates order data by customer to provide a view of
    customer purchase history and value.

    Args:
        context: Asset execution context
        orders: Transformed order data
        customers: Transformed customer data

    Returns:
        Customer orders mart for analytics
    """
    context.log.info("Creating customer orders mart")

    # Ensure order_date is datetime
    if "order_date" in orders.columns:
        orders["order_date"] = pd.to_datetime(orders["order_date"], errors="coerce")

    # Aggregate orders by customer
    customer_order_counts = (
        orders.groupby("customer_id")
        .agg(
            order_count=pd.NamedAgg(column="order_id", aggfunc="nunique"),
            first_order_date=pd.NamedAgg(column="order_date", aggfunc="min"),
            last_order_date=pd.NamedAgg(column="order_date", aggfunc="max"),
            total_spend=pd.NamedAgg(column="order_total", aggfunc="sum"),
        )
        .reset_index()
    )

    # Calculate days since last order
    if "last_order_date" in customer_order_counts.columns:
        today = pd.Timestamp.now().normalize()
        customer_order_counts["days_since_last_order"] = (today - customer_order_counts["last_order_date"]).dt.days

    # Merge with customer data
    result_df = pd.merge(customers, customer_order_counts, on="customer_id", how="left")

    # Fill NaN values for customers with no orders
    result_df["order_count"] = result_df["order_count"].fillna(0).astype(int)
    result_df["total_spend"] = result_df["total_spend"].fillna(0)

    # Add customer segments based on purchase behavior
    result_df["customer_segment"] = "Unknown"

    # RFM Segmentation (simplified)
    # New customers: 1 order
    result_df.loc[result_df["order_count"] == 1, "customer_segment"] = "New"

    # Loyal customers: 2+ orders and ordered in last 90 days
    loyal_mask = (result_df["order_count"] >= 2) & (result_df["days_since_last_order"] <= 90)
    result_df.loc[loyal_mask, "customer_segment"] = "Loyal"

    # At risk: 2+ orders but no order in 90-180 days
    at_risk_mask = (
        (result_df["order_count"] >= 2)
        & (result_df["days_since_last_order"] > 90)
        & (result_df["days_since_last_order"] <= 180)
    )
    result_df.loc[at_risk_mask, "customer_segment"] = "At Risk"

    # Churned: 2+ orders but no order in 180+ days
    churned_mask = (result_df["order_count"] >= 2) & (result_df["days_since_last_order"] > 180)
    result_df.loc[churned_mask, "customer_segment"] = "Churned"

    # Add additional metrics
    result_df["average_order_value"] = (
        result_df["total_spend"] / result_df["order_count"].replace(0, float("nan"))
    ).fillna(0)

    context.log.info(f"Created customer orders mart with {len(result_df)} customer records")

    return result_df


@asset(
    io_manager_key="duckdb_io_manager",
    key_prefix=["marts"],
    compute_kind="pandas",
    group_name="data_marts",
    ins={
        "products": AssetIn(key=["transformed", "products"]),
        "inventory": AssetIn(key=["transformed", "inventory"]),
    },
)
def product_inventory_mart(
    context: AssetExecutionContext, products: pd.DataFrame, inventory: pd.DataFrame
) -> pd.DataFrame:
    """Create product inventory mart for inventory analysis.

    This asset aggregates inventory data by product to provide a view of
    inventory levels and valuation.

    Args:
        context: Asset execution context
        products: Transformed product data
        inventory: Transformed inventory data

    Returns:
        Product inventory mart for analytics
    """
    context.log.info("Creating product inventory mart")

    # Aggregate inventory by product
    inventory_by_product = (
        inventory.groupby("product_id")
        .agg(
            total_quantity=pd.NamedAgg(column="quantity", aggfunc="sum"),
            store_count=pd.NamedAgg(column="store_id", aggfunc="nunique"),
            inventory_value=pd.NamedAgg(column="inventory_value", aggfunc="sum"),
            retail_value=pd.NamedAgg(column="retail_value", aggfunc="sum"),
        )
        .reset_index()
    )

    # Merge with product data
    result_df = pd.merge(products, inventory_by_product, on="product_id", how="left")

    # Fill NaN values for products with no inventory
    result_df["total_quantity"] = result_df["total_quantity"].fillna(0).astype(int)
    result_df["store_count"] = result_df["store_count"].fillna(0).astype(int)
    result_df["inventory_value"] = result_df["inventory_value"].fillna(0)
    result_df["retail_value"] = result_df["retail_value"].fillna(0)

    # Add inventory metrics
    result_df["potential_profit"] = result_df["retail_value"] - result_df["inventory_value"]

    # Add inventory status
    result_df["inventory_status"] = "In Stock"
    result_df.loc[result_df["total_quantity"] <= 0, "inventory_status"] = "Out of Stock"
    result_df.loc[result_df["total_quantity"] < 10, "inventory_status"] = "Low Stock"

    context.log.info(f"Created product inventory mart with {len(result_df)} product records")

    return result_df


@asset(
    io_manager_key="duckdb_io_manager",
    key_prefix=["marts"],
    compute_kind="pandas",
    group_name="data_marts",
    ins={
        "orders": AssetIn(key=["transformed", "orders"]),
        "products": AssetIn(key=["transformed", "products"]),
        "store_locations": AssetIn(key=["transformed", "store_locations"]),
    },
)
def sales_by_store_mart(
    context: AssetExecutionContext, orders: pd.DataFrame, products: pd.DataFrame, store_locations: pd.DataFrame
) -> pd.DataFrame:
    """Create sales by store mart for location-based sales analysis.

    This asset aggregates sales data by store to provide a view of
    store performance.

    Args:
        context: Asset execution context
        orders: Transformed order data
        products: Transformed product data
        store_locations: Transformed store location data

    Returns:
        Sales by store mart for analytics
    """
    context.log.info("Creating sales by store mart")

    # Ensure we have store_id in orders, if not, this is a simple example
    if "store_id" not in orders.columns:
        context.log.warning("No store_id found in orders data. Creating sample data.")
        # Create sample data for demonstration
        orders["store_id"] = 1  # Assume all orders from store 1 if missing

    # Ensure order_date is datetime
    if "order_date" in orders.columns:
        orders["order_date"] = pd.to_datetime(orders["order_date"], errors="coerce")
        # Extract year and month
        orders["year"] = orders["order_date"].dt.year
        orders["month"] = orders["order_date"].dt.month

    # Aggregate orders by store, year, and month
    sales_by_store = (
        orders.groupby(["store_id", "year", "month"])
        .agg(
            order_count=pd.NamedAgg(column="order_id", aggfunc="nunique"),
            customer_count=pd.NamedAgg(column="customer_id", aggfunc="nunique"),
            total_sales=pd.NamedAgg(column="order_total", aggfunc="sum"),
            avg_order_value=pd.NamedAgg(column="order_total", aggfunc="mean"),
        )
        .reset_index()
    )

    # Merge with store data
    result_df = pd.merge(sales_by_store, store_locations, on="store_id", how="left")

    # Calculate year-over-year and month-over-month growth
    # This would require more data and temporal logic

    context.log.info(f"Created sales by store mart with {len(result_df)} store-month records")

    return result_df
