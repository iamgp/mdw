"""
Initialize the workflows package.

Exposes key components for easier access.
"""

from data_warehouse.workflows.base import BaseExtractor, BaseLoader, BaseTransformer, Pipeline
from data_warehouse.workflows.cli import cli
from data_warehouse.workflows.dagster_integration import (
    ExtractorToDagsterOp,
    LoaderToDagsterOp,
    PipelineToDagsterJob,
    TransformerToDagsterOp,
    create_dagster_repository,
)
from data_warehouse.workflows.discovery import (
    discover_extractors,
    discover_loaders,
    discover_pipelines,
    discover_transformers,
)
from data_warehouse.workflows.docs_generator import DocsGenerator
from data_warehouse.workflows.exceptions import (
    ConfigurationError,
    DiscoveryError,
    DocsGeneratorError,
    ExtractorError,
    LoaderError,
    PipelineError,
    RegistryError,
    TransformerError,
    ValidationError,
    WatcherError,
)
from data_warehouse.workflows.registry import Registry
from data_warehouse.workflows.templates import TemplateGenerator, TemplateParser
from data_warehouse.workflows.validator import WorkflowValidator
from data_warehouse.workflows.watcher import WorkflowFileHandler, WorkflowWatcher
from data_warehouse.workflows.workflow_manager import WorkflowManager

__all__ = [
    # Base Classes
    "BaseExtractor",
    "BaseLoader",
    "BaseTransformer",
    "Pipeline",
    # Workflow Manager
    "WorkflowManager",
    # Registry
    "Registry",
    # Discovery
    "discover_extractors",
    "discover_loaders",
    "discover_pipelines",
    "discover_transformers",
    # Templates
    "TemplateGenerator",
    "TemplateParser",
    # Validator
    "WorkflowValidator",
    # Dagster Integration
    "ExtractorToDagsterOp",
    "TransformerToDagsterOp",
    "LoaderToDagsterOp",
    "PipelineToDagsterJob",
    "create_dagster_repository",
    # Watcher
    "WorkflowWatcher",
    "WorkflowFileHandler",
    # Docs Generator
    "DocsGenerator",
    # CLI
    "cli",
    # Exceptions
    "ConfigurationError",
    "ValidationError",
    "ExtractorError",
    "TransformerError",
    "LoaderError",
    "PipelineError",
    "RegistryError",
    "DiscoveryError",
    "WatcherError",
    "DocsGeneratorError",
]
