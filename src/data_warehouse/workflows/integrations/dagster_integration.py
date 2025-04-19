"""
Dagster integration for the data warehouse workflow system.

This module provides functionality to convert workflow components to Dagster ops
and pipelines to Dagster jobs, enabling workflow orchestration with Dagster.
"""

import logging
from typing import Any, TypeVar

from dagster import (
    AssetMaterialization,
    Field,
    In,
    MetadataValue,
    Nothing,
    Out,
    Output,
    Permissive,
    ResourceDefinition,
    job,
    op,
)

from data_warehouse.workflows.core.base import BaseExtractor, BaseLoader, BaseTransformer, Pipeline
from data_warehouse.workflows.core.exceptions import TransformerError

logger = logging.getLogger(__name__)

# Generic type variables for input and output types
T = TypeVar("T")
U = TypeVar("U")


def _create_dagster_metadata(component: Any) -> dict[str, Any]:
    """
    Create Dagster metadata from a workflow component.

    Args:
        component: The workflow component

    Returns:
        A dictionary of metadata for Dagster
    """
    metadata = {}

    # Add component name and type
    if hasattr(component, "name"):
        metadata["name"] = MetadataValue.text(component.name)
    if hasattr(component, "__class__"):
        metadata["type"] = MetadataValue.text(component.__class__.__name__)

    # Add any metadata from the component
    if hasattr(component, "get_metadata") and callable(component.get_metadata):
        component_metadata = component.get_metadata()
        for key, value in component_metadata.items():
            if isinstance(value, str | int | float | bool):
                metadata[key] = MetadataValue.text(str(value))
            elif isinstance(value, dict):
                metadata[key] = MetadataValue.json(value)
            elif isinstance(value, list):
                metadata[key] = MetadataValue.json(value)

    return metadata


def extractor_to_dagster_op(extractor: BaseExtractor[T]) -> Any:
    """
    Convert a workflow extractor to a Dagster op.

    Args:
        extractor: The workflow extractor to convert

    Returns:
        A Dagster op that wraps the extractor
    """

    @op(
        name=f"{extractor.name}_op",
        description=f"Dagster op for extractor {extractor.name}",
        out=Out(description="Extracted data"),
        config_schema={"extractor_config": Field(Permissive(), description="Extractor configuration")},
        tags={"component_type": "extractor"},
    )
    def extractor_op(context) -> T:
        """
        Dagster op that wraps an extractor.

        Args:
            context: The Dagster context

        Returns:
            The extracted data
        """
        logger.info(f"Running extractor: {extractor.name}")

        # Apply any configuration from Dagster
        config = context.op_config.get("extractor_config", {})
        if config:
            for key, value in config.items():
                if hasattr(extractor, key):
                    setattr(extractor, key, value)

        # Extract data
        try:
            data = extractor.extract()

            # Record asset materialization event
            context.log_event(
                AssetMaterialization(
                    asset_key=f"extract_{extractor.name}",
                    description=f"Data extracted by {extractor.name}",
                    metadata=_create_dagster_metadata(extractor),
                )
            )

            return data
        except Exception as e:
            context.log.error(f"Error in extractor {extractor.name}: {str(e)}")
            raise

    return extractor_op


def transformer_to_dagster_op(transformer: BaseTransformer[T, U]) -> Any:
    """
    Convert a workflow transformer to a Dagster op.

    Args:
        transformer: The workflow transformer to convert

    Returns:
        A Dagster op that wraps the transformer
    """

    @op(
        name=f"{transformer.name}_op",
        description=f"Dagster op for transformer {transformer.name}",
        ins={"data": In(description="Input data to transform")},
        out=Out(description="Transformed data"),
        config_schema={"transformer_config": Field(Permissive(), description="Transformer configuration")},
        tags={"component_type": "transformer"},
    )
    def transformer_op(context, data: T) -> U:
        """
        Dagster op that wraps a transformer.

        Args:
            context: The Dagster context
            data: The input data to transform

        Returns:
            The transformed data
        """
        logger.info(f"Running transformer: {transformer.name}")

        # Apply any configuration from Dagster
        config = context.op_config.get("transformer_config", {})
        if config:
            for key, value in config.items():
                if hasattr(transformer, key):
                    setattr(transformer, key, value)

        # Transform data
        try:
            if hasattr(transformer, "validate_input") and callable(transformer.validate_input):
                if not transformer.validate_input(data):
                    raise TransformerError(f"Input validation failed for transformer {transformer.name}")

            transformed_data = transformer.transform(data)

            if hasattr(transformer, "validate_output") and callable(transformer.validate_output):
                if not transformer.validate_output(transformed_data):
                    raise TransformerError(f"Output validation failed for transformer {transformer.name}")

            # Record asset materialization event
            context.log_event(
                AssetMaterialization(
                    asset_key=f"transform_{transformer.name}",
                    description=f"Data transformed by {transformer.name}",
                    metadata=_create_dagster_metadata(transformer),
                )
            )

            return transformed_data
        except Exception as e:
            context.log.error(f"Error in transformer {transformer.name}: {str(e)}")
            raise

    return transformer_op


def loader_to_dagster_op(loader: BaseLoader[T]) -> Any:
    """
    Convert a workflow loader to a Dagster op.

    Args:
        loader: The workflow loader to convert

    Returns:
        A Dagster op that wraps the loader
    """

    @op(
        name=f"{loader.name}_op",
        description=f"Dagster op for loader {loader.name}",
        ins={"data": In(description="Data to load")},
        out=Out(Nothing, description="Loading confirmation"),
        config_schema={"loader_config": Field(Permissive(), description="Loader configuration")},
        tags={"component_type": "loader"},
    )
    def loader_op(context, data: T) -> None:
        """
        Dagster op that wraps a loader.

        Args:
            context: The Dagster context
            data: The data to load
        """
        logger.info(f"Running loader: {loader.name}")

        # Apply any configuration from Dagster
        config = context.op_config.get("loader_config", {})
        if config:
            for key, value in config.items():
                if hasattr(loader, key):
                    setattr(loader, key, value)

        # Load data
        try:
            loader.load(data)

            # Record asset materialization event
            context.log_event(
                AssetMaterialization(
                    asset_key=f"load_{loader.name}",
                    description=f"Data loaded by {loader.name}",
                    metadata=_create_dagster_metadata(loader),
                )
            )

            return Output(None)
        except Exception as e:
            context.log.error(f"Error in loader {loader.name}: {str(e)}")
            raise

    return loader_op


def pipeline_to_dagster_job(pipeline: Pipeline) -> Any:
    """
    Convert a workflow pipeline to a Dagster job.

    Args:
        pipeline: The workflow pipeline to convert

    Returns:
        A Dagster job that wraps the pipeline
    """
    # Convert components to Dagster ops
    extractor_op = extractor_to_dagster_op(pipeline.extractor)

    transformer_ops = []
    for transformer in pipeline.transformers:
        transformer_ops.append(transformer_to_dagster_op(transformer))

    loader_op = None
    if pipeline.loader:
        loader_op = loader_to_dagster_op(pipeline.loader)

    # Create the Dagster job
    @job(
        name=f"{pipeline.name}_job",
        description=f"Dagster job for pipeline {pipeline.name}",
        tags={
            "pipeline_name": pipeline.name,
            "pipeline_config": str(pipeline.config),
        },
    )
    def pipeline_job():
        """Dagster job that wraps a workflow pipeline."""
        # Connect the ops together
        data = extractor_op()

        # Apply transformers in sequence
        for transformer_op in transformer_ops:
            data = transformer_op(data)

        # Apply loader if it exists
        if loader_op:
            loader_op(data)

    return pipeline_job


class WorkflowResource:
    """
    Dagster resource for workflow components.

    This class provides access to workflow components (extractors, transformers, loaders)
    as Dagster resources.
    """

    def __init__(self, component: Any) -> None:
        """
        Initialize a WorkflowResource.

        Args:
            component: The workflow component
        """
        self.component = component

    def get_component(self) -> Any:
        """
        Get the wrapped workflow component.

        Returns:
            The workflow component
        """
        return self.component


def create_extractor_resource(extractor: BaseExtractor[Any]) -> ResourceDefinition:
    """
    Create a Dagster resource for an extractor.

    Args:
        extractor: The extractor to wrap

    Returns:
        A Dagster resource definition
    """
    return ResourceDefinition(
        resource_fn=lambda _: WorkflowResource(extractor),
        description=f"Resource for extractor {extractor.name}",
    )


def create_transformer_resource(transformer: BaseTransformer[Any, Any]) -> ResourceDefinition:
    """
    Create a Dagster resource for a transformer.

    Args:
        transformer: The transformer to wrap

    Returns:
        A Dagster resource definition
    """
    return ResourceDefinition(
        resource_fn=lambda _: WorkflowResource(transformer),
        description=f"Resource for transformer {transformer.name}",
    )


def create_loader_resource(loader: BaseLoader[Any]) -> ResourceDefinition:
    """
    Create a Dagster resource for a loader.

    Args:
        loader: The loader to wrap

    Returns:
        A Dagster resource definition
    """
    return ResourceDefinition(
        resource_fn=lambda _: WorkflowResource(loader),
        description=f"Resource for loader {loader.name}",
    )
