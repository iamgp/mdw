"""
Dagster assets for the data warehouse project.

This module exports assets from various submodules to be used in the repository.
"""

from dagster import load_assets_from_modules

from . import raw, staging, warehouse

# Load all assets from submodules
all_assets = load_assets_from_modules([raw, staging, warehouse])
