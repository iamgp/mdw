"""
Dagster resources for the data warehouse project.

This module defines resources that can be used across Dagster ops and assets.
"""

from .database import duckdb_resource, postgres_resource
from .io_managers import duckdb_io_manager, postgres_io_manager

__all__ = ["postgres_resource", "duckdb_resource", "postgres_io_manager", "duckdb_io_manager"]
