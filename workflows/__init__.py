"""
Workflow System for the data warehouse.

This package provides a modular workflow system for building and managing data pipelines.
"""

from workflows.base import BaseExtractor, BaseLoader, BaseTransformer, Pipeline
from workflows.cli import cli
from workflows.dagster_integration import (
    create_extractor_resource,
    create_loader_resource,
    create_transformer_resource,
    extractor_to_dagster_op,
    loader_to_dagster_op,
    pipeline_to_dagster_job,
    transformer_to_dagster_op,
)
from workflows.discovery import (
    discover_extractors,
    discover_loaders,
    discover_transformers,
)
from workflows.docs_generator import DocsGenerator
from workflows.exceptions import (
    ConfigurationError,
    ExtractorError,
    LoaderError,
    PipelineError,
    TransformerError,
    ValidationError,
    WorkflowError,
    WorkflowManagerError,
)
from workflows.registry import Registry
from workflows.templates import TemplateGenerator, TemplateParser
from workflows.validator import WorkflowValidator
from workflows.watcher import WorkflowFileHandler, WorkflowWatcher
from workflows.workflow_manager import WorkflowManager

__all__ = [
    # Base classes
    "BaseExtractor",
    "BaseLoader",
    "BaseTransformer",
    "Pipeline",
    # Discovery functions
    "discover_extractors",
    "discover_loaders",
    "discover_transformers",
    # Management classes
    "Registry",
    "WorkflowManager",
    "WorkflowValidator",
    # Template handling
    "TemplateGenerator",
    "TemplateParser",
    # Watch functionality
    "WorkflowWatcher",
    "WorkflowFileHandler",
    # Dagster integration
    "extractor_to_dagster_op",
    "transformer_to_dagster_op",
    "loader_to_dagster_op",
    "pipeline_to_dagster_job",
    "create_extractor_resource",
    "create_transformer_resource",
    "create_loader_resource",
    # Documentation generation
    "DocsGenerator",
    # CLI tool
    "cli",
    # Exceptions
    "WorkflowError",
    "ConfigurationError",
    "ValidationError",
    "ExtractorError",
    "TransformerError",
    "LoaderError",
    "PipelineError",
    "WorkflowManagerError",
]
