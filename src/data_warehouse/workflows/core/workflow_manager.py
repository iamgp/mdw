"""
Workflow Manager for the data warehouse workflow system.

This module provides a WorkflowManager class that integrates with the registry
and discovery systems to manage workflow components and pipelines.
"""

import logging
from collections.abc import Callable
from typing import Any

from data_warehouse.workflows.core.base import BaseExtractor, BaseLoader, BaseTransformer, Pipeline
from data_warehouse.workflows.core.exceptions import ConfigurationError, ValidationError
from data_warehouse.workflows.core.registry import Registry
from data_warehouse.workflows.tools.templates import TemplateGenerator, TemplateParser
from data_warehouse.workflows.tools.validator import WorkflowValidator

logger = logging.getLogger(__name__)


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
        extractors_path: str = "src/data_warehouse/extractors",
        transformers_path: str = "src/data_warehouse/transformers",
        loaders_path: str = "src/data_warehouse/loaders",
        # config_dict is no longer used here, discovery only finds types
    ) -> None:
        """
        Discover component *types* (classes) and register them with the registry.
        Does not instantiate components.

        Args:
            extractors_path: The path to the extractors package
            transformers_path: The path to the transformers package
            loaders_path: The path to the loaders package
        """
        # Use the registry's reload methods to discover and register component types
        self.registry.reload_extractors(extractors_path)
        self.registry.reload_transformers(transformers_path)
        self.registry.reload_loaders(loaders_path)

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
        Create and register components and a pipeline from a template file.

        This method parses the template, instantiates the defined components
        (extractors, transformers, loaders) using their configurations from the
        template, registers these instances, and finally creates and registers
        the pipeline itself.

        Args:
            template_path: The path to the template file

        Returns:
            The created and registered pipeline instance

        Raises:
            ConfigurationError: If the template cannot be loaded/parsed or references unknown types
            ValidationError: If the template structure is invalid or components fail validation
        """
        # Parse and validate the template structure
        template = self.template_parser.parse_and_validate(template_path)
        # Note: Reference resolution might need adjustment depending on its implementation
        resolved_template = self.template_parser.resolve_references(template)

        # 1. Instantiate and register components defined in the template
        self._instantiate_components_from_template(
            resolved_template, "extractors", self.registry.get_extractor_type, self.registry.register_extractor
        )
        self._instantiate_components_from_template(
            resolved_template, "transformers", self.registry.get_transformer_type, self.registry.register_transformer
        )
        self._instantiate_components_from_template(
            resolved_template, "loaders", self.registry.get_loader_type, self.registry.register_loader
        )

        # 2. Create and register the pipeline(s)
        if not resolved_template.get("pipelines"):
            raise ValidationError("No pipelines defined in the template")

        # Assuming only one pipeline per template for now
        pipeline_data = resolved_template["pipelines"][0]

        pipeline_name = pipeline_data.get("name", "default_pipeline")
        pipeline_config = pipeline_data.get("config", {})
        description = resolved_template.get("description", f"Pipeline '{pipeline_name}' loaded from template")

        # Get referenced component *instances*
        extractor_ref_obj = pipeline_data.get("extractor")
        if not extractor_ref_obj:
            raise ValidationError(f"Pipeline '{pipeline_name}' must define an 'extractor'")

        # --- Get component NAME for registry lookup (Handles resolved dicts) ---
        if isinstance(extractor_ref_obj, dict):
            extractor_name_to_lookup = extractor_ref_obj.get("name")
            if not extractor_name_to_lookup:
                raise ValidationError("Resolved extractor reference in pipeline is missing a 'name'.")
        elif isinstance(extractor_ref_obj, str):
            extractor_name_to_lookup = extractor_ref_obj  # If resolve_references didn't resolve it
        else:
            raise ValidationError(f"Invalid format for extractor reference in pipeline: {extractor_ref_obj}")
        # --- End Get Name ---

        try:
            # Use the extracted name for lookup
            extractor_instance = self.registry.get_extractor(extractor_name_to_lookup)
        except KeyError:
            raise ConfigurationError(
                f"Extractor instance '{extractor_name_to_lookup}' needed by pipeline '{pipeline_name}' not found. Was it defined and instantiated correctly from the template?"
            ) from None

        transformer_instances = []
        for transformer_ref_obj in pipeline_data.get("transformers", []):
            # --- Get component NAME for registry lookup ---
            if isinstance(transformer_ref_obj, dict):
                transformer_name_to_lookup = transformer_ref_obj.get("name")
                if not transformer_name_to_lookup:
                    raise ValidationError("Resolved transformer reference in pipeline is missing a 'name'.")
            elif isinstance(transformer_ref_obj, str):
                transformer_name_to_lookup = transformer_ref_obj
            else:
                raise ValidationError(f"Invalid format for transformer reference in pipeline: {transformer_ref_obj}")
            # --- End Get Name ---
            try:
                transformer_instance = self.registry.get_transformer(transformer_name_to_lookup)
                transformer_instances.append(transformer_instance)
            except KeyError:
                raise ConfigurationError(
                    f"Transformer instance '{transformer_name_to_lookup}' needed by pipeline '{pipeline_name}' not found. Was it defined and instantiated correctly from the template?"
                ) from None

        loader_instance = None
        loader_ref_obj = pipeline_data.get("loader")
        if loader_ref_obj:
            # --- Get component NAME for registry lookup ---
            if isinstance(loader_ref_obj, dict):
                loader_name_to_lookup = loader_ref_obj.get("name")
                if not loader_name_to_lookup:
                    raise ValidationError("Resolved loader reference in pipeline is missing a 'name'.")
            elif isinstance(loader_ref_obj, str):
                loader_name_to_lookup = loader_ref_obj
            else:
                raise ValidationError(f"Invalid format for loader reference in pipeline: {loader_ref_obj}")
            # --- End Get Name ---
            try:
                loader_instance = self.registry.get_loader(loader_name_to_lookup)
            except KeyError:
                raise ConfigurationError(
                    f"Loader instance '{loader_name_to_lookup}' needed by pipeline '{pipeline_name}' not found. Was it defined and instantiated correctly from the template?"
                ) from None

        # Create the pipeline instance
        pipeline = Pipeline(
            name=pipeline_name,
            extractor=extractor_instance,
            transformers=transformer_instances,
            loader=loader_instance,
            config=pipeline_config,
            # Consider adding description/metadata from template to Pipeline class
        )

        # Validate and register the pipeline instance
        self.validator.validate_pipeline(pipeline)
        self.register_pipeline(pipeline)
        logger.info(f"Successfully created and registered pipeline: '{pipeline.name}'")

        return pipeline

    def _instantiate_components_from_template(
        self,
        template: dict,
        component_key: str,
        type_getter: Callable[[str], type[Any]],
        instance_register: Callable[[Any], None],
    ) -> None:
        """Helper to instantiate and register components of a specific type from the template."""
        # Using Any for simplicity as the helper handles different Base types.
        for component_data in template.get(component_key, []):
            comp_name = component_data.get("name")
            comp_type_name = component_data.get("type")
            comp_config = component_data.get("config", {})

            if not comp_name or not comp_type_name:
                raise ValidationError(
                    f"Component definition in '{component_key}' section is missing 'name' or 'type'. Data: {component_data}"
                )

            try:
                # Get the component class (Type) from the registry
                component_class = type_getter(comp_type_name)
            except KeyError:
                raise ConfigurationError(
                    f"Component type '{comp_type_name}' (for name '{comp_name}') not found in registry. Ensure it's discoverable."
                ) from None

            try:
                # Instantiate the component
                instance = component_class(name=comp_name, config=comp_config)
                # Register the instance
                logger.debug(f"Inside _instantiate_components_from_template: component_key = '{component_key}'")
                logger.debug(
                    f"Before registering '{comp_name}', registry dict keys: {list(getattr(self.registry, component_key).keys())}"
                )
                instance_register(instance)
                logger.debug(
                    f"After registering '{comp_name}', registry dict keys: {list(getattr(self.registry, component_key).keys())}"
                )
                logger.debug(f"Instantiated and registered {component_key[:-1]}: {comp_name} (Type: {comp_type_name})")
            except Exception as e:
                logger.error(
                    f"Failed to instantiate/register {component_key[:-1]} '{comp_name}' (Type: {comp_type_name}): {e}",
                    exc_info=True,
                )
                # Decide if this should halt processing or just log
                raise ConfigurationError(f"Failed to instantiate {component_key[:-1]} '{comp_name}': {e}") from e

    def validate_workflow(self) -> bool:
        """
        Validate the entire workflow (registered instances).
        Note: This now only validates registered *instances*.
        Validation of types or templates happens separately.
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
        Reload all component *types* from their respective directories.
        This is useful for hot-reloading when component class definitions change.
        Does NOT reload instances or pipelines defined in templates.
        """
        self.discover_components(
            extractors_path="src/data_warehouse/extractors",
            transformers_path="src/data_warehouse/transformers",
            loaders_path="src/data_warehouse/loaders",
        )

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

    def get_extractor_type(self, class_name: str) -> type[BaseExtractor]:
        return self.registry.get_extractor_type(class_name)

    def get_transformer_type(self, class_name: str) -> type[BaseTransformer]:
        return self.registry.get_transformer_type(class_name)

    def get_loader_type(self, class_name: str) -> type[BaseLoader]:
        return self.registry.get_loader_type(class_name)

    def get_all_extractor_types(self) -> dict[str, type[BaseExtractor]]:
        return self.registry.get_all_extractor_types()

    def get_all_transformer_types(self) -> dict[str, type[BaseTransformer]]:
        return self.registry.get_all_transformer_types()

    def get_all_loader_types(self) -> dict[str, type[BaseLoader]]:
        return self.registry.get_all_loader_types()

    def clear_registry(self) -> None:
        """Clear all registered components from the registry."""
        self.registry.clear()
