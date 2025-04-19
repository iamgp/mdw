"""
Workflow-specific exceptions for handling errors in the data warehouse workflow system.

These exceptions provide a consistent way to handle errors across different workflow components.
"""

from typing import Any


class WorkflowError(Exception):
    """Base exception for all workflow-related errors."""

    def __init__(self, message: str, details: dict[str, Any] | None = None):
        self.message = message
        self.details = details or {}
        super().__init__(message)


class ValidationError(WorkflowError):
    """Exception raised when validation fails for workflow inputs or outputs."""

    pass


class ExtractorError(WorkflowError):
    """Exception raised when an extractor fails to retrieve data from the source."""

    pass


class TransformerError(WorkflowError):
    """Exception raised when a transformer fails to process data."""

    pass


class LoaderError(WorkflowError):
    """Exception raised when a loader fails to load data to the destination."""

    pass


class PipelineError(WorkflowError):
    """Exception raised when a pipeline fails to execute."""

    pass


class WorkflowManagerError(WorkflowError):
    """Exception raised when the workflow manager encounters an error."""

    pass


class ConfigurationError(WorkflowError):
    """Exception raised when there is an error in the workflow configuration."""

    pass
