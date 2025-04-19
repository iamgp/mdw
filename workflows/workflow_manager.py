"""
Workflow Manager for the data warehouse workflow system.

This module provides a WorkflowManager class that integrates with the registry
and discovery systems to manage workflow components and pipelines.
"""

from typing import Any

from workflows.base import BaseExtractor, BaseLoader, BaseTransformer, Pipeline
from workflows.exceptions import ConfigurationError, ValidationError
from workflows.registry import Registry
from workflows.templates import TemplateGenerator, TemplateParser
from workflows.validator import WorkflowValidator


class WorkflowManager:
    """
    A class for managing workflow components and pipelines.

    The workflow manager is responsible for:
    - Using the registry to manage workflow components
    - Handling discovery of components
    - Template parsing and validation
    - Component validation
    - Pipeline execution
    """

    def __init__(self) -> None:
        """Initialize a WorkflowManager instance with the necessary components."""
        self.registry = Registry()
        self.validator = WorkflowValidator()
        self.template_parser = TemplateParser()
        self.template_generator = TemplateGenerator()

    def discover_components(
        self,
        extractors_path: str = "workflows/extractors",
        transformers_path: str = "workflows/transformers",
        loaders_path: str = "workflows/loaders",
        config_dict: dict[str, dict[str, Any]] | None = None,
    ) -> None:
        """
        Discover and register all workflow components.

        Args:
            extractors_path: The path to the extractors package
            transformers_path: The path to the transformers package
            loaders_path: The path to the loaders package
            config_dict: Configuration for components
        """
        # Use the registry's reload methods to discover and register components
        self.registry.reload_extractors(extractors_path, config_dict)
        self.registry.reload_transformers(transformers_path, config_dict)
        self.registry.reload_loaders(loaders_path, config_dict)

    def register_extractor(self, extractor: BaseExtractor) -> None:
        """
        Register an extractor with the registry.

        Args:
            extractor: The extractor to register

        Raises:
            ConfigurationError: If an extractor with the same name is already registered
        """
        self.registry.register_extractor(extractor)

    def register_transformer(self, transformer: BaseTransformer) -> None:
        """
        Register a transformer with the registry.

        Args:
            transformer: The transformer to register

        Raises:
            ConfigurationError: If a transformer with the same name is already registered
        """
        self.registry.register_transformer(transformer)

    def register_loader(self, loader: BaseLoader) -> None:
        """
        Register a loader with the registry.

        Args:
            loader: The loader to register

        Raises:
            ConfigurationError: If a loader with the same name is already registered
        """
        self.registry.register_loader(loader)

    def register_pipeline(self, pipeline: Pipeline) -> None:
        """
        Register a pipeline with the registry.

        Args:
            pipeline: The pipeline to register

        Raises:
            ConfigurationError: If a pipeline with the same name is already registered
        """
        self.registry.register_pipeline(pipeline)

    def create_pipeline_from_template(self, template_path: str) -> Pipeline:
        """
        Create a pipeline from a template file.

        Args:
            template_path: The path to the template file

        Returns:
            The created pipeline

        Raises:
            ConfigurationError: If the template cannot be loaded or parsed
            ValidationError: If the template is invalid
        """
        # Parse and validate the template
        template = self.template_parser.parse_and_validate(template_path)

        # Resolve references in the template
        resolved_template = self.template_parser.resolve_references(template)

        # Extract pipeline information
        if not resolved_template.get("pipelines"):
            raise ValidationError("No pipelines defined in the template")

        pipeline_data = resolved_template["pipelines"][0]  # Using the first pipeline for simplicity

        # Extract component configurations
        extractor_data = pipeline_data.get("extractor")
        transformer_data_list = pipeline_data.get("transformers", [])
        loader_data = pipeline_data.get("loader")

        if not extractor_data:
            raise ValidationError("No extractor defined in the pipeline")

        # Get or create components
        extractor_name = extractor_data.get("name")
        if not extractor_name:
            raise ValidationError("Extractor name is missing")

        # Try to find the extractor in the registry or create a new one
        try:
            extractor = self.registry.get_extractor(extractor_name)
        except KeyError:
            raise ConfigurationError(f"Extractor '{extractor_name}' not found in registry") from None

        # Process transformers
        transformers = []
        for transformer_data in transformer_data_list:
            transformer_name = transformer_data.get("name")
            if not transformer_name:
                raise ValidationError("Transformer name is missing")

            try:
                transformer = self.registry.get_transformer(transformer_name)
                transformers.append(transformer)
            except KeyError:
                raise ConfigurationError(f"Transformer '{transformer_name}' not found in registry") from None

        # Process loader (if defined)
        loader = None
        if loader_data:
            loader_name = loader_data.get("name")
            if not loader_name:
                raise ValidationError("Loader name is missing")

            try:
                loader = self.registry.get_loader(loader_name)
            except KeyError:
                raise ConfigurationError(f"Loader '{loader_name}' not found in registry") from None

        # Create and validate the pipeline
        pipeline = Pipeline(
            name=pipeline_data.get("name", "pipeline"),
            extractor=extractor,
            transformers=transformers,
            loader=loader,
            config=pipeline_data.get("config", {}),
        )

        # Validate the pipeline
        self.validator.validate_pipeline(pipeline)

        return pipeline

    def validate_workflow(self) -> bool:
        """
        Validate the entire workflow.

        Returns:
            True if the workflow is valid

        Raises:
            ValidationError: If the workflow is invalid
        """
        return self.validator.validate_workflow(
            self.registry.get_all_extractors(),
            self.registry.get_all_transformers(),
            self.registry.get_all_loaders(),
            self.registry.get_all_pipelines(),
        )

    def execute_pipeline(self, pipeline_name: str) -> Any:
        """
        Execute a pipeline by name.

        Args:
            pipeline_name: The name of the pipeline to execute

        Returns:
            The result of the pipeline execution

        Raises:
            KeyError: If no pipeline with the given name exists
        """
        pipeline = self.registry.get_pipeline(pipeline_name)
        return pipeline.execute()

    def reload_components(self) -> None:
        """
        Reload all components from their respective directories.
        This is useful for hot-reloading when files change.
        """
        self.discover_components()

    def create_template_from_pipeline(self, pipeline_name: str, output_path: str, format: str = "yaml") -> None:
        """
        Create a template file from an existing pipeline.

        Args:
            pipeline_name: The name of the pipeline to convert to a template
            output_path: The path where to save the template
            format: The format to use ("yaml" or "json")

        Raises:
            KeyError: If no pipeline with the given name exists
            ConfigurationError: If the template cannot be saved
        """
        # Get the pipeline
        pipeline = self.registry.get_pipeline(pipeline_name)

        # Create extractor template
        extractor = pipeline.extractor
        extractor_template = self.template_generator.generate_extractor_template(
            extractor.name, extractor.__class__.__name__, extractor.config
        )

        # Create transformer templates
        transformer_templates = []
        for transformer in pipeline.transformers:
            transformer_template = self.template_generator.generate_transformer_template(
                transformer.name, transformer.__class__.__name__, transformer.config
            )
            transformer_templates.append(transformer_template)

        # Create loader template (if a loader is defined)
        loader_template = None
        if pipeline.loader:
            loader = pipeline.loader
            loader_template = self.template_generator.generate_loader_template(
                loader.name, loader.__class__.__name__, loader.config
            )

        # Create pipeline template
        pipeline_template = self.template_generator.generate_pipeline_template(
            pipeline.name,
            extractor.name,
            [transformer.name for transformer in pipeline.transformers],
            pipeline.loader.name if pipeline.loader else None,
            f"Pipeline generated from {pipeline_name}",
            pipeline.config,
            {"generated": True, "original_pipeline": pipeline_name},
        )

        # Create workflow template
        workflow_template = self.template_generator.generate_workflow_template(
            f"{pipeline_name}_workflow",
            f"Workflow template generated from pipeline {pipeline_name}",
            "1.0.0",
            [extractor_template],
            transformer_templates,
            [loader_template] if loader_template else [],
            [pipeline_template],
        )

        # Save the template
        self.template_generator.save_template(workflow_template, output_path, format)

    def get_extractor(self, name: str) -> BaseExtractor:
        """
        Get an extractor by name from the registry.

        Args:
            name: The name of the extractor

        Returns:
            The extractor with the given name

        Raises:
            KeyError: If no extractor with the given name exists
        """
        return self.registry.get_extractor(name)

    def get_transformer(self, name: str) -> BaseTransformer:
        """
        Get a transformer by name from the registry.

        Args:
            name: The name of the transformer

        Returns:
            The transformer with the given name

        Raises:
            KeyError: If no transformer with the given name exists
        """
        return self.registry.get_transformer(name)

    def get_loader(self, name: str) -> BaseLoader:
        """
        Get a loader by name from the registry.

        Args:
            name: The name of the loader

        Returns:
            The loader with the given name

        Raises:
            KeyError: If no loader with the given name exists
        """
        return self.registry.get_loader(name)

    def get_pipeline(self, name: str) -> Pipeline:
        """
        Get a pipeline by name from the registry.

        Args:
            name: The name of the pipeline

        Returns:
            The pipeline with the given name

        Raises:
            KeyError: If no pipeline with the given name exists
        """
        return self.registry.get_pipeline(name)

    def get_all_extractors(self) -> dict[str, BaseExtractor]:
        """
        Get all registered extractors from the registry.

        Returns:
            A dictionary mapping extractor names to extractors
        """
        return self.registry.get_all_extractors()

    def get_all_transformers(self) -> dict[str, BaseTransformer]:
        """
        Get all registered transformers from the registry.

        Returns:
            A dictionary mapping transformer names to transformers
        """
        return self.registry.get_all_transformers()

    def get_all_loaders(self) -> dict[str, BaseLoader]:
        """
        Get all registered loaders from the registry.

        Returns:
            A dictionary mapping loader names to loaders
        """
        return self.registry.get_all_loaders()

    def get_all_pipelines(self) -> dict[str, Pipeline]:
        """
        Get all registered pipelines from the registry.

        Returns:
            A dictionary mapping pipeline names to pipelines
        """
        return self.registry.get_all_pipelines()

    def clear_registry(self) -> None:
        """Clear all registered components from the registry."""
        self.registry.clear()
