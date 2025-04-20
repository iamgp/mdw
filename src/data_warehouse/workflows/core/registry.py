"""
Registry system for the data warehouse workflow system.

This module provides a registry for tracking all available workflow components.
"""

import logging
from typing import TypeVar

from data_warehouse.workflows.core.base import BaseExtractor, BaseLoader, BaseTransformer, Pipeline
from data_warehouse.workflows.core.discovery import (
    discover_extractors,
    discover_loaders,
    discover_transformers,
)
from data_warehouse.workflows.core.exceptions import ConfigurationError

logger = logging.getLogger(__name__)

# Type variable for component instances
T = TypeVar("T")


class Registry:
    """
    A registry for tracking available workflow components and their types.

    The registry serves as a central repository for:
    - Discovered component *types* (classes).
    - Instantiated and configured component *instances*.
    - Defined pipelines.
    """

    def __init__(self) -> None:
        """Initialize a Registry instance."""
        # Storage for instantiated components
        self.extractors: dict[str, BaseExtractor] = {}
        self.transformers: dict[str, BaseTransformer] = {}
        self.loaders: dict[str, BaseLoader] = {}
        self.pipelines: dict[str, Pipeline] = {}

        # Storage for discovered component types (classes)
        self.extractor_types: dict[str, type[BaseExtractor]] = {}
        self.transformer_types: dict[str, type[BaseTransformer]] = {}
        self.loader_types: dict[str, type[BaseLoader]] = {}

    # --- Type Registration/Retrieval ---

    def register_extractor_type(self, extractor_class: type[BaseExtractor]) -> None:
        """Register a discovered extractor class."""
        class_name = extractor_class.__name__
        if class_name in self.extractor_types:
            logger.warning(f"Extractor type '{class_name}' is already registered. Overwriting.")
        self.extractor_types[class_name] = extractor_class

    def register_transformer_type(self, transformer_class: type[BaseTransformer]) -> None:
        """Register a discovered transformer class."""
        class_name = transformer_class.__name__
        if class_name in self.transformer_types:
            logger.warning(f"Transformer type '{class_name}' is already registered. Overwriting.")
        self.transformer_types[class_name] = transformer_class

    def register_loader_type(self, loader_class: type[BaseLoader]) -> None:
        """Register a discovered loader class."""
        class_name = loader_class.__name__
        if class_name in self.loader_types:
            logger.warning(f"Loader type '{class_name}' is already registered. Overwriting.")
        self.loader_types[class_name] = loader_class

    def get_extractor_type(self, class_name: str) -> type[BaseExtractor]:
        """Get an extractor class by its name."""
        if class_name not in self.extractor_types:
            raise KeyError(f"No extractor type '{class_name}' is registered")
        return self.extractor_types[class_name]

    def get_transformer_type(self, class_name: str) -> type[BaseTransformer]:
        """Get a transformer class by its name."""
        if class_name not in self.transformer_types:
            raise KeyError(f"No transformer type '{class_name}' is registered")
        return self.transformer_types[class_name]

    def get_loader_type(self, class_name: str) -> type[BaseLoader]:
        """Get a loader class by its name."""
        if class_name not in self.loader_types:
            raise KeyError(f"No loader type '{class_name}' is registered")
        return self.loader_types[class_name]

    def get_all_extractor_types(self) -> dict[str, type[BaseExtractor]]:
        """Get all registered extractor types."""
        return self.extractor_types.copy()

    def get_all_transformer_types(self) -> dict[str, type[BaseTransformer]]:
        """Get all registered transformer types."""
        return self.transformer_types.copy()

    def get_all_loader_types(self) -> dict[str, type[BaseLoader]]:
        """Get all registered loader types."""
        return self.loader_types.copy()

    # --- Instance Registration/Retrieval (Unchanged) ---

    def register_extractor(self, extractor: BaseExtractor) -> None:
        """Register an extractor instance."""
        logger.debug(
            f"Attempting to register extractor instance. Name: '{extractor.name}', Type: {type(extractor.name)}"
        )
        if not isinstance(extractor.name, str):
            logger.error(f"Extractor name is not a string! Got type: {type(extractor.name)}. Value: {extractor.name}")
            raise ConfigurationError(f"Invalid extractor name type: {type(extractor.name)}")

        if extractor.name in self.extractors:
            logger.warning(f"Extractor instance with name '{extractor.name}' is already registered. Overwriting.")
        self.extractors[extractor.name] = extractor
        logger.debug(f"Successfully registered extractor instance: '{extractor.name}'")

    def register_transformer(self, transformer: BaseTransformer) -> None:
        """Register a transformer instance."""
        if transformer.name in self.transformers:
            raise ConfigurationError(f"Transformer instance with name '{transformer.name}' is already registered")
        self.transformers[transformer.name] = transformer

    def register_loader(self, loader: BaseLoader) -> None:
        """Register a loader instance."""
        if loader.name in self.loaders:
            raise ConfigurationError(f"Loader instance with name '{loader.name}' is already registered")
        self.loaders[loader.name] = loader

    def register_pipeline(self, pipeline: Pipeline) -> None:
        """Register a pipeline instance."""
        if pipeline.name in self.pipelines:
            raise ConfigurationError(f"Pipeline with name '{pipeline.name}' is already registered")
        self.pipelines[pipeline.name] = pipeline

    # --- Unregister Methods (Should handle both types and instances if needed, simplified for now) ---

    def unregister_extractor(self, name: str) -> None:
        """Unregister an extractor instance by name."""
        if name not in self.extractors:
            raise KeyError(f"No extractor instance with name '{name}' is registered")
        del self.extractors[name]
        # Potential: Also remove type? Depends on desired lifecycle.

    def unregister_transformer(self, name: str) -> None:
        """Unregister a transformer instance by name."""
        if name not in self.transformers:
            raise KeyError(f"No transformer instance with name '{name}' is registered")
        del self.transformers[name]

    def unregister_loader(self, name: str) -> None:
        """Unregister a loader instance by name."""
        if name not in self.loaders:
            raise KeyError(f"No loader instance with name '{name}' is registered")
        del self.loaders[name]

    def unregister_pipeline(self, name: str) -> None:
        """Unregister a pipeline instance by name."""
        if name not in self.pipelines:
            raise KeyError(f"No pipeline with name '{name}' is registered")
        del self.pipelines[name]

    # --- Instance Getters (Unchanged) ---

    def get_extractor(self, name: str) -> BaseExtractor:
        """Get an extractor instance by name."""
        if name not in self.extractors:
            raise KeyError(f"No extractor instance with name '{name}' is registered")
        return self.extractors[name]

    def get_transformer(self, name: str) -> BaseTransformer:
        """Get a transformer instance by name."""
        if name not in self.transformers:
            raise KeyError(f"No transformer instance with name '{name}' is registered")
        return self.transformers[name]

    def get_loader(self, name: str) -> BaseLoader:
        """Get a loader instance by name."""
        if name not in self.loaders:
            raise KeyError(f"No loader instance with name '{name}' is registered")
        return self.loaders[name]

    def get_pipeline(self, name: str) -> Pipeline:
        """Get a pipeline instance by name."""
        if name not in self.pipelines:
            raise KeyError(f"No pipeline instance with name '{name}' is registered")
        return self.pipelines[name]

    def get_all_extractors(self) -> dict[str, BaseExtractor]:
        """Get all registered extractor instances."""
        return self.extractors.copy()

    def get_all_transformers(self) -> dict[str, BaseTransformer]:
        """Get all registered transformer instances."""
        return self.transformers.copy()

    def get_all_loaders(self) -> dict[str, BaseLoader]:
        """Get all registered loader instances."""
        return self.loaders.copy()

    def get_all_pipelines(self) -> dict[str, Pipeline]:
        """Get all registered pipeline instances."""
        return self.pipelines.copy()

    # --- Clearing Methods ---

    def clear(self) -> None:
        """Clear all registered components (types and instances) and pipelines."""
        self.extractors.clear()
        self.transformers.clear()
        self.loaders.clear()
        self.pipelines.clear()
        self.extractor_types.clear()
        self.transformer_types.clear()
        self.loader_types.clear()
        logger.info("Registry cleared.")

    # --- Reload Methods (Modified to register types only) ---

    def reload_extractors(self, package_path: str = "src/data_warehouse/extractors") -> None:
        """
        Discover and register extractor *types* (classes) from a package.
        Does not instantiate components.
        """
        logger.info(f"Reloading extractor types from {package_path}...")
        self.extractor_types.clear()  # Clear previous types
        try:
            discovered_classes = discover_extractors(package_path)
            logger.info(f"Discovered {len(discovered_classes)} extractor classes.")
            for component_class in discovered_classes:
                try:
                    self.register_extractor_type(component_class)
                    logger.debug(f"Registered extractor type: {component_class.__name__}")
                except Exception as e:
                    logger.error(f"Failed to register extractor type {component_class.__name__}: {e}")

        except ConfigurationError as e:
            logger.error(f"Error discovering extractor types: {e}")
        except Exception:
            logger.exception(f"Unexpected error during extractor type reload from {package_path}")

    def reload_transformers(self, package_path: str = "src/data_warehouse/transformers") -> None:
        """
        Discover and register transformer *types* (classes) from a package.
        Does not instantiate components.
        """
        logger.info(f"Reloading transformer types from {package_path}...")
        self.transformer_types.clear()  # Clear previous types
        try:
            discovered_classes = discover_transformers(package_path)
            logger.info(f"Discovered {len(discovered_classes)} transformer classes.")
            for component_class in discovered_classes:
                try:
                    self.register_transformer_type(component_class)
                    logger.debug(f"Registered transformer type: {component_class.__name__}")
                except Exception as e:
                    logger.error(f"Failed to register transformer type {component_class.__name__}: {e}")

        except ConfigurationError as e:
            logger.error(f"Error discovering transformer types: {e}")
        except Exception:
            logger.exception(f"Unexpected error during transformer type reload from {package_path}")

    def reload_loaders(self, package_path: str = "src/data_warehouse/loaders") -> None:
        """
        Discover and register loader *types* (classes) from a package.
        Does not instantiate components.
        """
        logger.info(f"Reloading loader types from {package_path}...")
        self.loader_types.clear()  # Clear previous types
        try:
            discovered_classes = discover_loaders(package_path)
            logger.info(f"Discovered {len(discovered_classes)} loader classes.")
            for component_class in discovered_classes:
                try:
                    self.register_loader_type(component_class)
                    logger.debug(f"Registered loader type: {component_class.__name__}")
                except Exception as e:
                    logger.error(f"Failed to register loader type {component_class.__name__}: {e}")

        except ConfigurationError as e:
            logger.error(f"Error discovering loader types: {e}")
        except Exception:
            logger.exception(f"Unexpected error during loader type reload from {package_path}")

    def reload_all_component_types(
        self,
        extractors_path: str = "src/data_warehouse/extractors",
        transformers_path: str = "src/data_warehouse/transformers",
        loaders_path: str = "src/data_warehouse/loaders",
    ) -> None:
        """Reload all component types."""
        self.reload_extractors(extractors_path)
        self.reload_transformers(transformers_path)
        self.reload_loaders(loaders_path)
