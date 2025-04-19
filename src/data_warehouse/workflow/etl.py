"""
ETL Base Classes for the Workflow System.

This module defines the base classes for ETL operations:
- ExtractorBase: For data extraction operations
- TransformerBase: For data transformation operations
- LoaderBase: For data loading operations
"""

from __future__ import annotations

import abc
from typing import Any, Generic, TypeVar

from pydantic import BaseModel, Field

from data_warehouse.workflow.base import WorkflowContext

# Type definitions
T = TypeVar("T")
SourceType = TypeVar("SourceType")
TargetType = TypeVar("TargetType")


class ExtractorConfig(BaseModel):
    """Configuration for data extractors."""

    source_name: str = Field(..., description="Name of the data source")
    batch_size: int = Field(100, description="Number of records to extract per batch")
    timeout_seconds: int = Field(60, description="Timeout in seconds for extraction operations")
    retry_count: int = Field(3, description="Number of retries for failed extractions")
    incremental: bool = Field(True, description="Whether to use incremental extraction")
    credentials: dict[str, Any] = Field(default_factory=dict, description="Credentials for accessing the source")


class ExtractorBase(Generic[SourceType], abc.ABC):
    """Base class for all data extractors."""

    def __init__(self, config: dict[str, Any] | None = None) -> None:
        """Initialize the extractor with configuration."""
        self.config = ExtractorConfig(**(config or {}))

    @abc.abstractmethod
    def extract(self, context: WorkflowContext) -> SourceType:
        """Extract data from the source.

        Args:
            context: The workflow context

        Returns:
            The extracted data

        Raises:
            ExtractorError: If extraction fails
        """
        pass

    def validate_source(self) -> bool:
        """Validate the data source configuration.

        Returns:
            True if the source is valid, False otherwise
        """
        # Override in subclasses for specific validation
        return True

    def get_source_schema(self) -> dict[str, Any]:
        """Get the schema of the data source.

        Returns:
            Dictionary representing the schema
        """
        return {}

    def list_available_sources(self) -> list[str]:
        """List available data sources.

        Returns:
            List of source names
        """
        return []


class TransformerConfig(BaseModel):
    """Configuration for data transformers."""

    batch_size: int = Field(100, description="Number of records to process per batch")
    parallel_processing: bool = Field(False, description="Whether to use parallel processing")
    num_workers: int = Field(4, description="Number of worker processes/threads")
    validation_level: str = Field("warn", description="Validation level (none, warn, strict)")
    filter_nulls: bool = Field(True, description="Whether to filter out null values")


class TransformerBase(Generic[SourceType, TargetType], abc.ABC):
    """Base class for all data transformers."""

    def __init__(self, config: dict[str, Any] | None = None) -> None:
        """Initialize the transformer with configuration."""
        self.config = TransformerConfig(**(config or {}))

    @abc.abstractmethod
    def transform(self, data: SourceType, context: WorkflowContext) -> TargetType:
        """Transform the extracted data.

        Args:
            data: The data to transform
            context: The workflow context

        Returns:
            The transformed data

        Raises:
            TransformerError: If transformation fails
        """
        pass

    def validate_data(self, data: Any) -> bool:
        """Validate input data before transformation.

        Args:
            data: The data to validate

        Returns:
            True if data is valid, False otherwise
        """
        # Override in subclasses for specific validation
        return True

    def get_target_schema(self) -> dict[str, Any]:
        """Get the schema of the transformed data.

        Returns:
            Dictionary representing the schema
        """
        return {}


class LoaderConfig(BaseModel):
    """Configuration for data loaders."""

    target_name: str = Field(..., description="Name of the data target")
    batch_size: int = Field(100, description="Number of records to load per batch")
    timeout_seconds: int = Field(120, description="Timeout in seconds for load operations")
    retry_count: int = Field(3, description="Number of retries for failed loads")
    upsert: bool = Field(True, description="Whether to use upsert instead of insert")
    credentials: dict[str, Any] = Field(default_factory=dict, description="Credentials for accessing the target")
    parallel_loading: bool = Field(False, description="Whether to use parallel loading")
    num_workers: int = Field(4, description="Number of worker processes/threads")


class LoaderBase(Generic[TargetType], abc.ABC):
    """Base class for all data loaders."""

    def __init__(self, config: dict[str, Any] | None = None) -> None:
        """Initialize the loader with configuration."""
        self.config = LoaderConfig(**(config or {}))

    @abc.abstractmethod
    def load(self, data: TargetType, context: WorkflowContext) -> int:
        """Load data into the target system.

        Args:
            data: The data to load
            context: The workflow context

        Returns:
            Number of records loaded

        Raises:
            LoaderError: If loading fails
        """
        pass

    def validate_target(self) -> bool:
        """Validate the data target configuration.

        Returns:
            True if the target is valid, False otherwise
        """
        # Override in subclasses for specific validation
        return True

    def get_target_schema(self) -> dict[str, Any]:
        """Get the schema of the target system.

        Returns:
            Dictionary representing the schema
        """
        return {}

    def list_available_targets(self) -> list[str]:
        """List available data targets.

        Returns:
            List of target names
        """
        return []
