"""
Registry system for the data warehouse workflow system.

This module provides a registry for tracking all available workflow components.
"""

import logging
from typing import Any, TypeVar

from data_warehouse.workflows.base import BaseExtractor, BaseLoader, BaseTransformer, Pipeline
from data_warehouse.workflows.discovery import (
    discover_extractors,
    discover_loaders,
    discover_transformers,
)
from data_warehouse.workflows.exceptions import ConfigurationError

logger = logging.getLogger(__name__)

# Type variable for component instances
T = TypeVar("T")


class Registry:
    """
    A registry for tracking all available workflow components.

    The registry serves as a central repository for all extractors, transformers,
    loaders, and pipelines in the workflow system. It provides methods for
    registering, retrieving, and managing components.
    """

    def __init__(self) -> None:
        """Initialize a Registry instance."""
        self.extractors: dict[str, BaseExtractor] = {}
        self.transformers: dict[str, BaseTransformer] = {}
        self.loaders: dict[str, BaseLoader] = {}
        self.pipelines: dict[str, Pipeline] = {}

    def register_extractor(self, extractor: BaseExtractor) -> None:
        """
        Register an extractor.

        Args:
            extractor: The extractor to register

        Raises:
            ConfigurationError: If an extractor with the same name is already registered
        """
        if extractor.name in self.extractors:
            raise ConfigurationError(f"Extractor with name '{extractor.name}' is already registered")

        self.extractors[extractor.name] = extractor

    def register_transformer(self, transformer: BaseTransformer) -> None:
        """
        Register a transformer.

        Args:
            transformer: The transformer to register

        Raises:
            ConfigurationError: If a transformer with the same name is already registered
        """
        if transformer.name in self.transformers:
            raise ConfigurationError(f"Transformer with name '{transformer.name}' is already registered")

        self.transformers[transformer.name] = transformer

    def register_loader(self, loader: BaseLoader) -> None:
        """
        Register a loader.

        Args:
            loader: The loader to register

        Raises:
            ConfigurationError: If a loader with the same name is already registered
        """
        if loader.name in self.loaders:
            raise ConfigurationError(f"Loader with name '{loader.name}' is already registered")

        self.loaders[loader.name] = loader

    def register_pipeline(self, pipeline: Pipeline) -> None:
        """
        Register a pipeline.

        Args:
            pipeline: The pipeline to register

        Raises:
            ConfigurationError: If a pipeline with the same name is already registered
        """
        if pipeline.name in self.pipelines:
            raise ConfigurationError(f"Pipeline with name '{pipeline.name}' is already registered")

        self.pipelines[pipeline.name] = pipeline

    def unregister_extractor(self, name: str) -> None:
        """
        Unregister an extractor by name.

        Args:
            name: The name of the extractor to unregister

        Raises:
            KeyError: If no extractor with the given name is registered
        """
        if name not in self.extractors:
            raise KeyError(f"No extractor with name '{name}' is registered")

        del self.extractors[name]

    def unregister_transformer(self, name: str) -> None:
        """
        Unregister a transformer by name.

        Args:
            name: The name of the transformer to unregister

        Raises:
            KeyError: If no transformer with the given name is registered
        """
        if name not in self.transformers:
            raise KeyError(f"No transformer with name '{name}' is registered")

        del self.transformers[name]

    def unregister_loader(self, name: str) -> None:
        """
        Unregister a loader by name.

        Args:
            name: The name of the loader to unregister

        Raises:
            KeyError: If no loader with the given name is registered
        """
        if name not in self.loaders:
            raise KeyError(f"No loader with name '{name}' is registered")

        del self.loaders[name]

    def unregister_pipeline(self, name: str) -> None:
        """
        Unregister a pipeline by name.

        Args:
            name: The name of the pipeline to unregister

        Raises:
            KeyError: If no pipeline with the given name is registered
        """
        if name not in self.pipelines:
            raise KeyError(f"No pipeline with name '{name}' is registered")

        del self.pipelines[name]

    def get_extractor(self, name: str) -> BaseExtractor:
        """
        Get an extractor by name.

        Args:
            name: The name of the extractor

        Returns:
            The extractor with the given name

        Raises:
            KeyError: If no extractor with the given name is registered
        """
        if name not in self.extractors:
            raise KeyError(f"No extractor with name '{name}' is registered")

        return self.extractors[name]

    def get_transformer(self, name: str) -> BaseTransformer:
        """
        Get a transformer by name.

        Args:
            name: The name of the transformer

        Returns:
            The transformer with the given name

        Raises:
            KeyError: If no transformer with the given name is registered
        """
        if name not in self.transformers:
            raise KeyError(f"No transformer with name '{name}' is registered")

        return self.transformers[name]

    def get_loader(self, name: str) -> BaseLoader:
        """
        Get a loader by name.

        Args:
            name: The name of the loader

        Returns:
            The loader with the given name

        Raises:
            KeyError: If no loader with the given name is registered
        """
        if name not in self.loaders:
            raise KeyError(f"No loader with name '{name}' is registered")

        return self.loaders[name]

    def get_pipeline(self, name: str) -> Pipeline:
        """
        Get a pipeline by name.

        Args:
            name: The name of the pipeline

        Returns:
            The pipeline with the given name

        Raises:
            KeyError: If no pipeline with the given name is registered
        """
        if name not in self.pipelines:
            raise KeyError(f"No pipeline with name '{name}' is registered")

        return self.pipelines[name]

    def get_all_extractors(self) -> dict[str, BaseExtractor]:
        """
        Get all registered extractors.

        Returns:
            A dictionary mapping extractor names to extractors
        """
        return self.extractors.copy()

    def get_all_transformers(self) -> dict[str, BaseTransformer]:
        """
        Get all registered transformers.

        Returns:
            A dictionary mapping transformer names to transformers
        """
        return self.transformers.copy()

    def get_all_loaders(self) -> dict[str, BaseLoader]:
        """
        Get all registered loaders.

        Returns:
            A dictionary mapping loader names to loaders
        """
        return self.loaders.copy()

    def get_all_pipelines(self) -> dict[str, Pipeline]:
        """
        Get all registered pipelines.

        Returns:
            A dictionary mapping pipeline names to pipelines
        """
        return self.pipelines.copy()

    def clear(self) -> None:
        """Clear all registered components."""
        self.extractors.clear()
        self.transformers.clear()
        self.loaders.clear()
        self.pipelines.clear()

    def reload_extractors(
        self, package_path: str = "workflows/extractors", config_dict: dict[str, dict[str, Any]] | None = None
    ) -> None:
        """
        Reload extractors from the extractors package.

        Args:
            package_path: The path to the extractors package
            config_dict: A dictionary mapping extractor names to configurations
        """
        # Clear existing extractors
        self.extractors.clear()

        # Discover new extractors
        extractor_classes = discover_extractors(package_path)

        # Instantiate and register new extractors
        for extractor_class in extractor_classes:
            default_name = extractor_class.__name__

            if config_dict and default_name in config_dict:
                config = config_dict[default_name]
                name = config.get("name", default_name)
                extractor = extractor_class(name, config)
            else:
                extractor = extractor_class(default_name)

            self.register_extractor(extractor)

    def reload_transformers(
        self, package_path: str = "workflows/transformers", config_dict: dict[str, dict[str, Any]] | None = None
    ) -> None:
        """
        Reload transformers from the transformers package.

        Args:
            package_path: The path to the transformers package
            config_dict: A dictionary mapping transformer names to configurations
        """
        # Clear existing transformers
        self.transformers.clear()

        # Discover new transformers
        transformer_classes = discover_transformers(package_path)

        # Instantiate and register new transformers
        for transformer_class in transformer_classes:
            default_name = transformer_class.__name__

            if config_dict and default_name in config_dict:
                config = config_dict[default_name]
                name = config.get("name", default_name)
                transformer = transformer_class(name, config)
            else:
                transformer = transformer_class(default_name)

            self.register_transformer(transformer)

    def reload_loaders(
        self, package_path: str = "workflows/loaders", config_dict: dict[str, dict[str, Any]] | None = None
    ) -> None:
        """
        Reload loaders from the loaders package.

        Args:
            package_path: The path to the loaders package
            config_dict: A dictionary mapping loader names to configurations
        """
        # Clear existing loaders
        self.loaders.clear()

        # Discover new loaders
        loader_classes = discover_loaders(package_path)

        # Instantiate and register new loaders
        for loader_class in loader_classes:
            default_name = loader_class.__name__

            if config_dict and default_name in config_dict:
                config = config_dict[default_name]
                name = config.get("name", default_name)
                loader = loader_class(name, config)
            else:
                loader = loader_class(default_name)

            self.register_loader(loader)

    def reload_all(
        self,
        extractors_path: str = "workflows/extractors",
        transformers_path: str = "workflows/transformers",
        loaders_path: str = "workflows/loaders",
        config: dict[str, dict[str, dict[str, Any]]] | None = None,
    ) -> None:
        """
        Reload all components from their respective packages.

        Args:
            extractors_path: The path to the extractors package
            transformers_path: The path to the transformers package
            loaders_path: The path to the loaders package
            config: A dictionary containing configurations for all component types
        """
        # Clear all components
        self.clear()

        # Reload all components
        extractor_config = config.get("extractors", {}) if config else {}
        transformer_config = config.get("transformers", {}) if config else {}
        loader_config = config.get("loaders", {}) if config else {}

        self.reload_extractors(extractors_path, extractor_config)
        self.reload_transformers(transformers_path, transformer_config)
        self.reload_loaders(loaders_path, loader_config)
