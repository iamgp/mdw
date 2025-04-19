"""
Template system for the data warehouse workflow system.

This module provides functionality for parsing, validating, and generating
workflow templates in YAML or JSON format.
"""

import json
import os
from typing import Any

import yaml
from jsonschema import Draft7Validator
from jsonschema import ValidationError as JsonSchemaError

from workflows.exceptions import ConfigurationError, ValidationError

# Define JSON Schema for workflow templates
EXTRACTOR_SCHEMA = {
    "type": "object",
    "required": ["name", "type"],
    "properties": {"name": {"type": "string"}, "type": {"type": "string"}, "config": {"type": "object"}},
}

TRANSFORMER_SCHEMA = {
    "type": "object",
    "required": ["name", "type"],
    "properties": {"name": {"type": "string"}, "type": {"type": "string"}, "config": {"type": "object"}},
}

LOADER_SCHEMA = {
    "type": "object",
    "required": ["name", "type"],
    "properties": {"name": {"type": "string"}, "type": {"type": "string"}, "config": {"type": "object"}},
}

PIPELINE_SCHEMA = {
    "type": "object",
    "required": ["name", "extractor", "transformers"],
    "properties": {
        "name": {"type": "string"},
        "description": {"type": "string"},
        "version": {"type": "string"},
        "author": {"type": "string"},
        "extractor": {"$ref": "#/definitions/extractor_ref"},
        "transformers": {"type": "array", "items": {"$ref": "#/definitions/transformer_ref"}},
        "loader": {"$ref": "#/definitions/loader_ref"},
        "config": {"type": "object"},
        "metadata": {"type": "object"},
    },
}

WORKFLOW_SCHEMA = {
    "type": "object",
    "required": ["version", "pipelines"],
    "properties": {
        "version": {"type": "string"},
        "description": {"type": "string"},
        "extractors": {"type": "array", "items": {"$ref": "#/definitions/extractor"}},
        "transformers": {"type": "array", "items": {"$ref": "#/definitions/transformer"}},
        "loaders": {"type": "array", "items": {"$ref": "#/definitions/loader"}},
        "pipelines": {"type": "array", "items": {"$ref": "#/definitions/pipeline"}},
    },
    "definitions": {
        "extractor": EXTRACTOR_SCHEMA,
        "transformer": TRANSFORMER_SCHEMA,
        "loader": LOADER_SCHEMA,
        "pipeline": PIPELINE_SCHEMA,
        "extractor_ref": {"oneOf": [{"type": "string"}, {"$ref": "#/definitions/extractor"}]},
        "transformer_ref": {"oneOf": [{"type": "string"}, {"$ref": "#/definitions/transformer"}]},
        "loader_ref": {"oneOf": [{"type": "string"}, {"$ref": "#/definitions/loader"}]},
    },
}


class TemplateParser:
    """
    A class for parsing and validating workflow templates.

    The template parser is responsible for loading templates from YAML or JSON files,
    validating them against a schema, and converting them to a format that can be used
    by the workflow system.
    """

    def __init__(self, schema: dict[str, Any] | None = None) -> None:
        """
        Initialize a TemplateParser instance.

        Args:
            schema: The JSON schema to use for validation. If None, the default schema is used.
        """
        self.schema = schema or WORKFLOW_SCHEMA
        self.validator = Draft7Validator(self.schema)

    def load_template(self, file_path: str) -> dict[str, Any]:
        """
        Load a template from a file.

        Args:
            file_path: The path to the template file (YAML or JSON)

        Returns:
            The parsed template as a dictionary

        Raises:
            ConfigurationError: If the file cannot be loaded or parsed
        """
        if not os.path.exists(file_path):
            raise ConfigurationError(f"Template file does not exist: {file_path}")

        try:
            # Determine file format based on extension
            ext = os.path.splitext(file_path)[1].lower()

            with open(file_path) as f:
                if ext in [".yaml", ".yml"]:
                    return yaml.safe_load(f)
                elif ext == ".json":
                    return json.load(f)
                else:
                    raise ConfigurationError(f"Unsupported template format: {ext}")
        except (yaml.YAMLError, json.JSONDecodeError) as e:
            raise ConfigurationError(f"Error parsing template file: {str(e)}") from e

    def validate_template(self, template: dict[str, Any]) -> None:
        """
        Validate a template against the schema.

        Args:
            template: The template to validate

        Raises:
            ValidationError: If the template does not conform to the schema
        """
        try:
            self.validator.validate(template)
        except JsonSchemaError as e:
            path = "/".join(str(p) for p in e.path)
            raise ValidationError(f"Template validation error at {path}: {e.message}") from e

    def parse_and_validate(self, file_path: str) -> dict[str, Any]:
        """
        Load and validate a template from a file.

        Args:
            file_path: The path to the template file

        Returns:
            The parsed and validated template

        Raises:
            ConfigurationError: If the file cannot be loaded or parsed
            ValidationError: If the template does not conform to the schema
        """
        template = self.load_template(file_path)
        self.validate_template(template)
        return template

    def resolve_references(self, template: dict[str, Any]) -> dict[str, Any]:
        """
        Resolve references in a template.

        This method resolves string references to components (e.g., extractor names)
        to their full definitions from the template.

        Args:
            template: The template to resolve

        Returns:
            A copy of the template with all references resolved

        Raises:
            ValidationError: If a reference cannot be resolved
        """
        result = template.copy()

        # Create dictionaries to look up components by name
        extractors = {e["name"]: e for e in template.get("extractors", [])}
        transformers = {t["name"]: t for t in template.get("transformers", [])}
        loaders = {loader["name"]: loader for loader in template.get("loaders", [])}

        # Resolve references in pipelines
        for pipeline in result.get("pipelines", []):
            # Resolve extractor reference
            if isinstance(pipeline["extractor"], str):
                extractor_name = pipeline["extractor"]
                if extractor_name not in extractors:
                    raise ValidationError(f"Undefined extractor reference: {extractor_name}")
                pipeline["extractor"] = extractors[extractor_name]

            # Resolve transformer references
            resolved_transformers = []
            for transformer in pipeline.get("transformers", []):
                if isinstance(transformer, str):
                    transformer_name = transformer
                    if transformer_name not in transformers:
                        raise ValidationError(f"Undefined transformer reference: {transformer_name}")
                    resolved_transformers.append(transformers[transformer_name])
                else:
                    resolved_transformers.append(transformer)
            pipeline["transformers"] = resolved_transformers

            # Resolve loader reference
            if "loader" in pipeline and isinstance(pipeline["loader"], str):
                loader_name = pipeline["loader"]
                if loader_name not in loaders:
                    raise ValidationError(f"Undefined loader reference: {loader_name}")
                pipeline["loader"] = loaders[loader_name]

        return result


class TemplateGenerator:
    """
    A class for generating workflow templates.

    The template generator is responsible for creating template files based on
    existing workflow components or from scratch.
    """

    def __init__(self, template_dir: str = "workflows/templates") -> None:
        """
        Initialize a TemplateGenerator instance.

        Args:
            template_dir: The directory where templates are stored
        """
        self.template_dir = template_dir

        # Create template directory if it doesn't exist
        if not os.path.exists(template_dir):
            os.makedirs(template_dir)

    def generate_workflow_template(
        self,
        name: str,
        description: str = "",
        version: str = "1.0.0",
        extractors: list[dict[str, Any]] | None = None,
        transformers: list[dict[str, Any]] | None = None,
        loaders: list[dict[str, Any]] | None = None,
        pipelines: list[dict[str, Any]] | None = None,
    ) -> dict[str, Any]:
        """
        Generate a workflow template.

        Args:
            name: The name of the workflow
            description: A description of the workflow
            version: The version of the workflow
            extractors: A list of extractor definitions
            transformers: A list of transformer definitions
            loaders: A list of loader definitions
            pipelines: A list of pipeline definitions

        Returns:
            The generated workflow template
        """
        return {
            "name": name,
            "description": description,
            "version": version,
            "extractors": extractors or [],
            "transformers": transformers or [],
            "loaders": loaders or [],
            "pipelines": pipelines or [],
        }

    def generate_extractor_template(
        self, name: str, type_name: str, config: dict[str, Any] | None = None
    ) -> dict[str, Any]:
        """
        Generate an extractor template.

        Args:
            name: The name of the extractor
            type_name: The type (class) of the extractor
            config: Configuration parameters for the extractor

        Returns:
            The generated extractor template
        """
        return {"name": name, "type": type_name, "config": config or {}}

    def generate_transformer_template(
        self, name: str, type_name: str, config: dict[str, Any] | None = None
    ) -> dict[str, Any]:
        """
        Generate a transformer template.

        Args:
            name: The name of the transformer
            type_name: The type (class) of the transformer
            config: Configuration parameters for the transformer

        Returns:
            The generated transformer template
        """
        return {"name": name, "type": type_name, "config": config or {}}

    def generate_loader_template(
        self, name: str, type_name: str, config: dict[str, Any] | None = None
    ) -> dict[str, Any]:
        """
        Generate a loader template.

        Args:
            name: The name of the loader
            type_name: The type (class) of the loader
            config: Configuration parameters for the loader

        Returns:
            The generated loader template
        """
        return {"name": name, "type": type_name, "config": config or {}}

    def generate_pipeline_template(
        self,
        name: str,
        extractor: str | dict[str, Any],
        transformers: list[str | dict[str, Any]] | None = None,
        loader: str | dict[str, Any] | None = None,
        description: str = "",
        config: dict[str, Any] | None = None,
        metadata: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        """
        Generate a pipeline template.

        Args:
            name: The name of the pipeline
            extractor: The extractor to use (either a name or a definition)
            transformers: A list of transformers to apply (either names or definitions)
            loader: The loader to use (either a name or a definition)
            description: A description of the pipeline
            config: Configuration parameters for the pipeline
            metadata: Additional metadata for the pipeline

        Returns:
            The generated pipeline template
        """
        return {
            "name": name,
            "description": description,
            "extractor": extractor,
            "transformers": transformers or [],
            "loader": loader,
            "config": config or {},
            "metadata": metadata or {},
        }

    def save_template(self, template: dict[str, Any], file_path: str, format: str = "yaml") -> None:
        """
        Save a template to a file.

        Args:
            template: The template to save
            file_path: The path where to save the template
            format: The format to use ("yaml" or "json")

        Raises:
            ConfigurationError: If the template cannot be saved
        """
        try:
            # Make sure the directory exists
            os.makedirs(os.path.dirname(file_path), exist_ok=True)

            with open(file_path, "w") as f:
                if format.lower() == "yaml":
                    yaml.dump(template, f, default_flow_style=False)
                elif format.lower() == "json":
                    json.dump(template, f, indent=2)
                else:
                    raise ConfigurationError(f"Unsupported template format: {format}")
        except (OSError, yaml.YAMLError, json.JSONDecodeError) as e:
            raise ConfigurationError(f"Error saving template: {str(e)}") from e

    def create_example_template(self, file_path: str, format: str = "yaml") -> None:
        """
        Create an example template file.

        Args:
            file_path: The path where to save the example template
            format: The format to use ("yaml" or "json")

        Raises:
            ConfigurationError: If the template cannot be saved
        """
        # Create example extractors
        example_extractors = [
            self.generate_extractor_template(
                "csv_extractor", "CsvExtractor", {"file_path": "/path/to/data.csv", "delimiter": ","}
            ),
            self.generate_extractor_template(
                "api_extractor",
                "ApiExtractor",
                {"url": "https://api.example.com/data", "headers": {"Accept": "application/json"}},
            ),
        ]

        # Create example transformers
        example_transformers = [
            self.generate_transformer_template(
                "filter_transformer", "FilterTransformer", {"field": "status", "value": "active"}
            ),
            self.generate_transformer_template(
                "rename_transformer",
                "RenameTransformer",
                {"mappings": {"old_name": "new_name", "old_value": "new_value"}},
            ),
        ]

        # Create example loaders
        example_loaders = [
            self.generate_loader_template(
                "database_loader",
                "DatabaseLoader",
                {"connection_string": "postgresql://username:password@localhost:5432/database", "table": "data"},
            ),
            self.generate_loader_template(
                "file_loader", "FileLoader", {"file_path": "/path/to/output.csv", "mode": "append"}
            ),
        ]

        # Create example pipelines
        example_pipelines = [
            self.generate_pipeline_template(
                "example_pipeline_1",
                "csv_extractor",
                ["filter_transformer", "rename_transformer"],
                "database_loader",
                "An example pipeline that extracts from CSV, applies filtering and renaming, and loads to a database",
                {"batch_size": 1000, "retry_attempts": 3},
                {"owner": "data-team", "schedule": "daily"},
            ),
            self.generate_pipeline_template(
                "example_pipeline_2",
                "api_extractor",
                ["filter_transformer"],
                "file_loader",
                "An example pipeline that extracts from an API, applies filtering, and loads to a file",
                {"batch_size": 500},
                {"owner": "analytics-team", "schedule": "hourly"},
            ),
        ]

        # Create example workflow
        example_workflow = self.generate_workflow_template(
            "example_workflow",
            "An example workflow with multiple pipelines",
            "1.0.0",
            example_extractors,
            example_transformers,
            example_loaders,
            example_pipelines,
        )

        # Save the example workflow template
        self.save_template(example_workflow, file_path, format)
