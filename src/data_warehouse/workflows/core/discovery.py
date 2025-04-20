"""
Component discovery system for the data warehouse workflow system.

This module provides functionality to scan directories and automatically discover
and register workflow components (extractors, transformers, loaders, etc.).
"""

import importlib
import importlib.util
import inspect
import logging
import os
import pkgutil
import sys
from collections.abc import Callable
from typing import Any, TypeVar

from data_warehouse.workflows.core.base import BaseExtractor, BaseLoader, BaseTransformer
from data_warehouse.workflows.core.exceptions import ConfigurationError

logger = logging.getLogger(__name__)

# Type variable for workflow component classes
T = TypeVar("T")


def discover_modules(package_path: str) -> list[str]:
    """
    Discover all Python modules in a package.

    Args:
        package_path: The path to the package to scan

    Returns:
        A list of module names discovered in the package

    Raises:
        ConfigurationError: If the package path doesn't exist
    """
    if not os.path.exists(package_path):
        raise ConfigurationError(f"Package path does not exist: {package_path}")

    # Get the package name from the path
    package_name = os.path.basename(package_path)

    # Make sure the package path is in sys.path so we can import from it
    # NOTE: Modifying sys.path can be risky, consider alternatives if possible
    if package_path not in sys.path:
        # Find the parent directory containing the package
        parent_dir = os.path.dirname(package_path)
        if parent_dir not in sys.path:
            sys.path.insert(0, parent_dir)

    # Discover all modules in the package
    module_names = []

    try:
        # Ensure the top-level package itself is importable if needed
        # Example: if package_path is 'src/data_warehouse/extractors', ensure 'data_warehouse' is importable
        # This part might need adjustment based on project structure and how packages are installed/managed.
        # Trying to import the immediate parent might not always work correctly.
        # It's generally better if the 'src' directory or project root is in PYTHONPATH.

        # Use pkgutil.iter_modules to find modules directly within the path
        for _, name, is_pkg in pkgutil.iter_modules([package_path]):
            if not is_pkg:  # Only include modules, not sub-packages
                # Construct the full module name based on the package structure
                # Assuming the parent of package_path contains the base package
                # e.g., if package_path = src/data_warehouse/extractors,
                # we might want data_warehouse.extractors.module_name
                # This requires knowing the base package name relative to sys.path

                # Simplistic approach: use basename as package name
                # This might break if package_path is deeply nested relative to PYTHONPATH entries
                full_module_name = f"{package_name}.{name}"
                module_names.append(full_module_name)
                logger.debug(f"Discovered module: {full_module_name}")
            # Add recursive call here if subpackages needed to be scanned
            # elif is_pkg:
            #     module_names.extend(discover_modules(os.path.join(package_path, name)))

    except ImportError as e:
        logger.error(f"ImportError during module discovery in {package_path}: {e}. Check PYTHONPATH.")
        # Decide if this should raise ConfigurationError or just return empty/partial list
        # raise ConfigurationError(f"Error importing package {package_name}: {str(e)}") from e
    except Exception as e:
        logger.error(f"Unexpected error during module discovery in {package_path}: {e}")
        # Decide if this should raise or return empty

    return module_names


def import_module(module_name: str) -> Any:
    """
    Import a module by name.

    Args:
        module_name: The name of the module to import

    Returns:
        The imported module

    Raises:
        ConfigurationError: If the module cannot be imported
    """
    try:
        return importlib.import_module(module_name)
    except ImportError as e:
        raise ConfigurationError(f"Error importing module {module_name}: {str(e)}") from e


def discover_component_classes(module: Any, base_class: type[T], exclude_base: bool = True) -> list[type[T]]:
    """
    Discover all subclasses of a base class in a module.

    Args:
        module: The module to scan
        base_class: The base class to look for
        exclude_base: Whether to exclude the base class itself

    Returns:
        A list of component classes discovered in the module
    """
    component_classes = []

    for _name, obj in inspect.getmembers(module, inspect.isclass):
        # Check if the object is a subclass of the base class
        # and is defined in this module (not imported)
        is_subclass = issubclass(obj, base_class)
        is_from_module = obj.__module__ == module.__name__
        is_base = obj is base_class

        if is_subclass and is_from_module and (not exclude_base or not is_base):
            component_classes.append(obj)

    return component_classes


def discover_extractors(package_path: str = "src/data_warehouse/extractors") -> list[type[BaseExtractor]]:
    """
    Discover all extractor classes in the extractors package.

    Args:
        package_path: The path to the extractors package

    Returns:
        A list of extractor classes discovered in the package
    """
    extractors = []

    for module_name in discover_modules(package_path):
        module = import_module(module_name)
        extractors.extend(discover_component_classes(module, BaseExtractor))

    return extractors


def discover_transformers(package_path: str = "src/data_warehouse/transformers") -> list[type[BaseTransformer]]:
    """
    Discover all transformer classes in the transformers package.

    Args:
        package_path: The path to the transformers package

    Returns:
        A list of transformer classes discovered in the package
    """
    transformers = []

    for module_name in discover_modules(package_path):
        module = import_module(module_name)
        transformers.extend(discover_component_classes(module, BaseTransformer))

    return transformers


def discover_loaders(package_path: str = "src/data_warehouse/loaders") -> list[type[BaseLoader]]:
    """
    Discover all loader classes in the loaders package.

    Args:
        package_path: The path to the loaders package

    Returns:
        A list of loader classes discovered in the package
    """
    loaders = []

    for module_name in discover_modules(package_path):
        module = import_module(module_name)
        loaders.extend(discover_component_classes(module, BaseLoader))

    return loaders


def instantiate_component(component_class: type[T], name: str, config: dict[str, Any] | None = None) -> T:
    """
    Instantiate a component class with a name and config.

    Args:
        component_class: The component class to instantiate
        name: The name to give the component instance
        config: Configuration parameters for the component

    Returns:
        An instance of the component class
    """
    return component_class(name, config)


def discover_and_instantiate_components(
    discover_func: Callable[[], list[type[T]]], config_dict: dict[str, dict[str, Any]]
) -> dict[str, T]:
    """
    Discover component classes and instantiate them with configurations.

    Args:
        discover_func: A function that discovers component classes
        config_dict: A dictionary mapping component names to configurations

    Returns:
        A dictionary mapping component names to component instances
    """
    components = {}

    # Discover component classes
    component_classes = discover_func()

    # Instantiate each component class with its configuration
    for component_class in component_classes:
        # Use the class name as the default component name
        default_name = component_class.__name__

        # Check if there's a configuration for this component
        if default_name in config_dict:
            config = config_dict[default_name]
            # Use the name from the config if provided, otherwise use the default
            name = config.get("name", default_name)
            components[name] = instantiate_component(component_class, name, config)
        else:
            # No configuration, use default name and empty config
            components[default_name] = instantiate_component(component_class, default_name, {})

    return components
