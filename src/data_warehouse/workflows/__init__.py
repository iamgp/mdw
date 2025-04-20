"""
Initialize the workflows package.

Exposes key components for easier access.
"""

from data_warehouse.workflows.core.base import BaseExtractor, BaseLoader, BaseTransformer, Pipeline
from data_warehouse.workflows.core.discovery import (
    discover_extractors,
    discover_loaders,
    discover_transformers,
)
from data_warehouse.workflows.core.exceptions import (
    ConfigurationError,
    ExtractorError,
    LoaderError,
    PipelineError,
    TransformerError,
    ValidationError,
)
from data_warehouse.workflows.core.registry import Registry
from data_warehouse.workflows.core.workflow_manager import WorkflowManager
from data_warehouse.workflows.integrations.dagster_integration import (
    extractor_to_dagster_op,
    loader_to_dagster_op,
    pipeline_to_dagster_job,
    transformer_to_dagster_op,
)
from data_warehouse.workflows.tools.cli import cli
from data_warehouse.workflows.tools.docs_generator import DocsGenerator
from data_warehouse.workflows.tools.templates import TemplateGenerator, TemplateParser
from data_warehouse.workflows.tools.validator import WorkflowValidator
from data_warehouse.workflows.tools.watcher import WorkflowFileHandler, WorkflowWatcher

__all__ = [
    # Base Classes (from core.base)
    "BaseExtractor",
    "BaseLoader",
    "BaseTransformer",
    "Pipeline",
    # Workflow Manager (from core.workflow_manager)
    "WorkflowManager",
    # Registry (from core.registry)
    "Registry",
    # Discovery (from core.discovery)
    "discover_extractors",
    "discover_loaders",
    "discover_transformers",
    # Templates (from tools.templates)
    "TemplateGenerator",
    "TemplateParser",
    # Validator (from tools.validator)
    "WorkflowValidator",
    # Dagster Integration (from integrations.dagster_integration)
    "extractor_to_dagster_op",
    "transformer_to_dagster_op",
    "loader_to_dagster_op",
    "pipeline_to_dagster_job",
    # Watcher (from tools.watcher)
    "WorkflowWatcher",
    "WorkflowFileHandler",
    # Docs Generator (from tools.docs_generator)
    "DocsGenerator",
    # CLI (from tools.cli)
    "cli",
    # Exceptions (from core.exceptions)
    "ConfigurationError",
    "ValidationError",
    "ExtractorError",
    "TransformerError",
    "LoaderError",
    "PipelineError",
]
