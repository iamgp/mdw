"""
Validation module for the data warehouse workflow system.

This module provides a WorkflowValidator class for validating workflow components
and pipelines, ensuring they are properly configured and compatible with each other.
"""

import logging
from collections.abc import Mapping

from data_warehouse.workflows.core.base import BaseExtractor, BaseLoader, BaseTransformer, Pipeline
from data_warehouse.workflows.core.exceptions import ValidationError

logger = logging.getLogger(__name__)


class WorkflowValidator:
    """
    A class for validating workflow components and pipelines.

    The workflow validator is responsible for:
    - Validating individual components (extractors, transformers, loaders)
    - Validating pipelines
    - Validating the entire workflow
    - Ensuring compatibility between components
    """

    def validate_component(self, component: BaseExtractor | BaseTransformer | BaseLoader) -> bool:
        """
        Validate a workflow component.

        Args:
            component: The component to validate

        Returns:
            True if the component is valid

        Raises:
            ValidationError: If the component is invalid
        """
        # All components must have a name
        if not hasattr(component, "name") or not component.name:
            raise ValidationError(f"Component {component.__class__.__name__} must have a name")

        # Component-specific validation
        if isinstance(component, BaseExtractor):
            return self._validate_extractor(component)
        elif isinstance(component, BaseTransformer):
            return self._validate_transformer(component)
        elif isinstance(component, BaseLoader):
            return self._validate_loader(component)
        else:
            raise ValidationError(f"Unknown component type: {type(component)}")

    def _validate_extractor(self, extractor: BaseExtractor) -> bool:
        """
        Validate an extractor.

        Args:
            extractor: The extractor to validate

        Returns:
            True if the extractor is valid

        Raises:
            ValidationError: If the extractor is invalid
        """
        # Extractors must have a source
        if not hasattr(extractor, "source") or not extractor.source:
            raise ValidationError(f"Extractor {extractor.name} must have a source")

        # Validate extract method existence
        if not hasattr(extractor, "extract") or not callable(extractor.extract):
            raise ValidationError(f"Extractor {extractor.name} must have an extract method")

        return True

    def _validate_transformer(self, transformer: BaseTransformer) -> bool:
        """
        Validate a transformer.

        Args:
            transformer: The transformer to validate

        Returns:
            True if the transformer is valid

        Raises:
            ValidationError: If the transformer is invalid
        """
        # Validate transform method existence
        if not hasattr(transformer, "transform") or not callable(transformer.transform):
            raise ValidationError(f"Transformer {transformer.name} must have a transform method")

        return True

    def _validate_loader(self, loader: BaseLoader) -> bool:
        """
        Validate a loader.

        Args:
            loader: The loader to validate

        Returns:
            True if the loader is valid

        Raises:
            ValidationError: If the loader is invalid
        """
        # Validate load method existence
        if not hasattr(loader, "load") or not callable(loader.load):
            raise ValidationError(f"Loader {loader.name} must have a load method")

        return True

    def validate_pipeline(self, pipeline: Pipeline) -> bool:
        """
        Validate a pipeline.

        Args:
            pipeline: The pipeline to validate

        Returns:
            True if the pipeline is valid

        Raises:
            ValidationError: If the pipeline is invalid
        """
        # A pipeline must have an extractor
        if not pipeline.extractor:
            raise ValidationError(f"Pipeline {pipeline.name} must have an extractor")

        # A pipeline must have at least one transformer
        if not pipeline.transformers or len(pipeline.transformers) == 0:
            raise ValidationError(f"Pipeline {pipeline.name} must have at least one transformer")

        # Validate all components in the pipeline
        self.validate_component(pipeline.extractor)
        for transformer in pipeline.transformers:
            self.validate_component(transformer)

        if pipeline.loader:
            self.validate_component(pipeline.loader)

        # Validate compatibility between components
        self._validate_extractor_to_transformer_compatibility(pipeline.extractor, pipeline.transformers[0])

        # Validate compatibility between transformers
        for i in range(len(pipeline.transformers) - 1):
            self._validate_transformer_to_transformer_compatibility(
                pipeline.transformers[i], pipeline.transformers[i + 1]
            )

        # Validate compatibility between the last transformer and the loader
        if pipeline.loader:
            self._validate_transformer_to_loader_compatibility(pipeline.transformers[-1], pipeline.loader)

        return True

    def _validate_extractor_to_transformer_compatibility(
        self, extractor: BaseExtractor, transformer: BaseTransformer
    ) -> bool:
        """
        Validate compatibility between an extractor and a transformer.

        Args:
            extractor: The extractor
            transformer: The transformer

        Returns:
            True if the components are compatible

        Raises:
            ValidationError: If the components are incompatible
        """
        # Check if the transformer accepts the output format of the extractor
        if hasattr(transformer, "accepts_formats") and hasattr(extractor, "output_format"):
            if extractor.output_format not in transformer.accepts_formats:
                raise ValidationError(
                    f"Transformer {transformer.name} does not accept format "
                    f"{extractor.output_format} from extractor {extractor.name}"
                )

        return True

    def _validate_transformer_to_transformer_compatibility(
        self, transformer1: BaseTransformer, transformer2: BaseTransformer
    ) -> bool:
        """
        Validate compatibility between two transformers.

        Args:
            transformer1: The first transformer
            transformer2: The second transformer

        Returns:
            True if the transformers are compatible

        Raises:
            ValidationError: If the transformers are incompatible
        """
        # Check if the second transformer accepts the output format of the first transformer
        if hasattr(transformer2, "accepts_formats") and hasattr(transformer1, "output_format"):
            if transformer1.output_format not in transformer2.accepts_formats:
                raise ValidationError(
                    f"Transformer {transformer2.name} does not accept format "
                    f"{transformer1.output_format} from transformer {transformer1.name}"
                )

        return True

    def _validate_transformer_to_loader_compatibility(self, transformer: BaseTransformer, loader: BaseLoader) -> bool:
        """
        Validate compatibility between a transformer and a loader.

        Args:
            transformer: The transformer
            loader: The loader

        Returns:
            True if the components are compatible

        Raises:
            ValidationError: If the components are incompatible
        """
        # Check if the loader accepts the output format of the transformer
        if hasattr(loader, "accepts_formats") and hasattr(transformer, "output_format"):
            if transformer.output_format not in loader.accepts_formats:
                raise ValidationError(
                    f"Loader {loader.name} does not accept format "
                    f"{transformer.output_format} from transformer {transformer.name}"
                )

        return True

    def validate_workflow(
        self,
        extractors: Mapping[str, BaseExtractor],
        transformers: Mapping[str, BaseTransformer],
        loaders: Mapping[str, BaseLoader],
        pipelines: Mapping[str, Pipeline],
    ) -> bool:
        """
        Validate the entire workflow.

        Args:
            extractors: A mapping of extractor names to extractors
            transformers: A mapping of transformer names to transformers
            loaders: A mapping of loader names to loaders
            pipelines: A mapping of pipeline names to pipelines

        Returns:
            True if the workflow is valid

        Raises:
            ValidationError: If the workflow is invalid
        """
        logger.info("Validating entire workflow...")
        for pipeline in pipelines.values():
            self.validate_pipeline(pipeline)

        logger.info("Workflow validation successful.")
        return True
