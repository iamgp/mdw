"""
Domain-Centric Workflow System for Data Warehouse.

This module provides a comprehensive workflow engine for building ETL pipelines
in a domain-centric way. It includes base classes, registry mechanisms, and
workflow discovery.
"""

__version__ = "0.1.0"

# Define explicit public interface to control imports
__all__ = [
    "WorkflowBase",
    "WorkflowRegistry",
    "WorkflowExecutor",
    "WorkflowValidator",
    "WorkflowMonitor",
    "ExtractorBase",
    "TransformerBase",
    "LoaderBase",
]

from data_warehouse.workflow.base import (  # noqa
    WorkflowBase,
    WorkflowRegistry,
    WorkflowExecutor,
    WorkflowValidator,
    WorkflowMonitor,
)
from data_warehouse.workflow.etl import (  # noqa
    ExtractorBase,
    TransformerBase,
    LoaderBase,
)
