"""
Unit tests for the template system.
"""

import json
import os
import tempfile
import unittest
from typing import Any

import pytest
import yaml

from workflows.exceptions import ConfigurationError, ValidationError
from workflows.templates import (
    TemplateGenerator,
    TemplateParser,
)


class TestTemplateParser(unittest.TestCase):
    """Test the template parser."""

    def setUp(self) -> None:
        """Set up the test case."""
        self.parser = TemplateParser()
        self.temp_dir = tempfile.TemporaryDirectory()

    def tearDown(self) -> None:
        """Clean up after the test."""
        self.temp_dir.cleanup()

    def _create_temp_file(self, content: dict[str, Any], extension: str) -> str:
        """Create a temporary file with the given content and extension."""
        file_path = os.path.join(self.temp_dir.name, f"test_template.{extension}")
        with open(file_path, "w") as f:
            if extension in ["yaml", "yml"]:
                yaml.dump(content, f)
            elif extension == "json":
                json.dump(content, f)
        return file_path

    def test_load_yaml_template(self) -> None:
        """Test loading a YAML template."""
        # Create a simple valid template
        template = {
            "version": "1.0.0",
            "pipelines": [
                {
                    "name": "test_pipeline",
                    "extractor": {"name": "test_extractor", "type": "TestExtractor"},
                    "transformers": [{"name": "test_transformer", "type": "TestTransformer"}],
                }
            ],
        }
        file_path = self._create_temp_file(template, "yaml")

        # Load the template
        loaded_template = self.parser.load_template(file_path)

        # Check that the template was loaded correctly
        assert loaded_template["version"] == "1.0.0"
        assert loaded_template["pipelines"][0]["name"] == "test_pipeline"

    def test_load_json_template(self) -> None:
        """Test loading a JSON template."""
        # Create a simple valid template
        template = {
            "version": "1.0.0",
            "pipelines": [
                {
                    "name": "test_pipeline",
                    "extractor": {"name": "test_extractor", "type": "TestExtractor"},
                    "transformers": [{"name": "test_transformer", "type": "TestTransformer"}],
                }
            ],
        }
        file_path = self._create_temp_file(template, "json")

        # Load the template
        loaded_template = self.parser.load_template(file_path)

        # Check that the template was loaded correctly
        assert loaded_template["version"] == "1.0.0"
        assert loaded_template["pipelines"][0]["name"] == "test_pipeline"

    def test_load_invalid_format(self) -> None:
        """Test loading a template with an invalid format."""
        # Create a file with an unsupported extension
        file_path = os.path.join(self.temp_dir.name, "test_template.txt")
        with open(file_path, "w") as f:
            f.write("This is not a valid template format")

        # Attempt to load the template
        with pytest.raises(ConfigurationError):
            self.parser.load_template(file_path)

    def test_validate_valid_template(self) -> None:
        """Test validating a valid template."""
        # Create a valid template
        template = {
            "version": "1.0.0",
            "pipelines": [
                {
                    "name": "test_pipeline",
                    "extractor": {"name": "test_extractor", "type": "TestExtractor"},
                    "transformers": [{"name": "test_transformer", "type": "TestTransformer"}],
                }
            ],
        }

        # Validate the template (should not raise an exception)
        self.parser.validate_template(template)

    def test_validate_invalid_template(self) -> None:
        """Test validating an invalid template."""
        # Create an invalid template (missing required field 'version')
        template = {
            "pipelines": [
                {
                    "name": "test_pipeline",
                    "extractor": {"name": "test_extractor", "type": "TestExtractor"},
                    "transformers": [{"name": "test_transformer", "type": "TestTransformer"}],
                }
            ],
        }

        # Validate the template (should raise an exception)
        with pytest.raises(ValidationError):
            self.parser.validate_template(template)

    def test_parse_and_validate(self) -> None:
        """Test parsing and validating a template."""
        # Create a valid template
        template = {
            "version": "1.0.0",
            "pipelines": [
                {
                    "name": "test_pipeline",
                    "extractor": {"name": "test_extractor", "type": "TestExtractor"},
                    "transformers": [{"name": "test_transformer", "type": "TestTransformer"}],
                }
            ],
        }
        file_path = self._create_temp_file(template, "yaml")

        # Parse and validate the template
        parsed_template = self.parser.parse_and_validate(file_path)

        # Check that the template was parsed correctly
        assert parsed_template["version"] == "1.0.0"
        assert parsed_template["pipelines"][0]["name"] == "test_pipeline"

    def test_resolve_references(self) -> None:
        """Test resolving references in a template."""
        # Create a template with references
        template = {
            "version": "1.0.0",
            "extractors": [
                {"name": "test_extractor", "type": "TestExtractor"},
            ],
            "transformers": [
                {"name": "test_transformer", "type": "TestTransformer"},
            ],
            "loaders": [
                {"name": "test_loader", "type": "TestLoader"},
            ],
            "pipelines": [
                {
                    "name": "test_pipeline",
                    "extractor": "test_extractor",
                    "transformers": ["test_transformer"],
                    "loader": "test_loader",
                }
            ],
        }

        # Resolve references
        resolved_template = self.parser.resolve_references(template)

        # Check that references were resolved correctly
        pipeline = resolved_template["pipelines"][0]
        assert isinstance(pipeline["extractor"], dict)
        assert pipeline["extractor"]["name"] == "test_extractor"
        assert isinstance(pipeline["transformers"][0], dict)
        assert pipeline["transformers"][0]["name"] == "test_transformer"
        assert isinstance(pipeline["loader"], dict)
        assert pipeline["loader"]["name"] == "test_loader"

    def test_resolve_references_undefined(self) -> None:
        """Test resolving undefined references in a template."""
        # Create a template with an undefined reference
        template = {
            "version": "1.0.0",
            "extractors": [
                {"name": "test_extractor", "type": "TestExtractor"},
            ],
            "pipelines": [
                {
                    "name": "test_pipeline",
                    "extractor": "undefined_extractor",  # This reference doesn't exist
                    "transformers": [],
                }
            ],
        }

        # Attempt to resolve references
        with pytest.raises(ValidationError):
            self.parser.resolve_references(template)


class TestTemplateGenerator(unittest.TestCase):
    """Test the template generator."""

    def setUp(self) -> None:
        """Set up the test case."""
        self.temp_dir = tempfile.TemporaryDirectory()
        self.generator = TemplateGenerator(template_dir=self.temp_dir.name)

    def tearDown(self) -> None:
        """Clean up after the test."""
        self.temp_dir.cleanup()

    def test_generate_workflow_template(self) -> None:
        """Test generating a workflow template."""
        # Generate a workflow template
        template = self.generator.generate_workflow_template(
            name="test_workflow",
            description="Test workflow",
            version="1.0.0",
            extractors=[
                self.generator.generate_extractor_template("test_extractor", "TestExtractor"),
            ],
            transformers=[
                self.generator.generate_transformer_template("test_transformer", "TestTransformer"),
            ],
            loaders=[
                self.generator.generate_loader_template("test_loader", "TestLoader"),
            ],
            pipelines=[
                self.generator.generate_pipeline_template(
                    "test_pipeline",
                    "test_extractor",
                    ["test_transformer"],
                    "test_loader",
                ),
            ],
        )

        # Check that the template was generated correctly
        assert template["name"] == "test_workflow"
        assert template["description"] == "Test workflow"
        assert template["version"] == "1.0.0"
        assert len(template["extractors"]) == 1
        assert template["extractors"][0]["name"] == "test_extractor"
        assert len(template["transformers"]) == 1
        assert template["transformers"][0]["name"] == "test_transformer"
        assert len(template["loaders"]) == 1
        assert template["loaders"][0]["name"] == "test_loader"
        assert len(template["pipelines"]) == 1
        assert template["pipelines"][0]["name"] == "test_pipeline"

    def test_generate_extractor_template(self) -> None:
        """Test generating an extractor template."""
        # Generate an extractor template
        template = self.generator.generate_extractor_template(
            name="test_extractor",
            type_name="TestExtractor",
            config={"source": "test_source"},
        )

        # Check that the template was generated correctly
        assert template["name"] == "test_extractor"
        assert template["type"] == "TestExtractor"
        assert template["config"]["source"] == "test_source"

    def test_generate_transformer_template(self) -> None:
        """Test generating a transformer template."""
        # Generate a transformer template
        template = self.generator.generate_transformer_template(
            name="test_transformer",
            type_name="TestTransformer",
            config={"param": "value"},
        )

        # Check that the template was generated correctly
        assert template["name"] == "test_transformer"
        assert template["type"] == "TestTransformer"
        assert template["config"]["param"] == "value"

    def test_generate_loader_template(self) -> None:
        """Test generating a loader template."""
        # Generate a loader template
        template = self.generator.generate_loader_template(
            name="test_loader",
            type_name="TestLoader",
            config={"destination": "test_destination"},
        )

        # Check that the template was generated correctly
        assert template["name"] == "test_loader"
        assert template["type"] == "TestLoader"
        assert template["config"]["destination"] == "test_destination"

    def test_generate_pipeline_template(self) -> None:
        """Test generating a pipeline template."""
        # Generate a pipeline template
        template = self.generator.generate_pipeline_template(
            name="test_pipeline",
            extractor="test_extractor",
            transformers=["test_transformer1", "test_transformer2"],
            loader="test_loader",
            description="Test pipeline",
            config={"batch_size": 100},
            metadata={"author": "test_author"},
        )

        # Check that the template was generated correctly
        assert template["name"] == "test_pipeline"
        assert template["description"] == "Test pipeline"
        assert template["extractor"] == "test_extractor"
        assert template["transformers"] == ["test_transformer1", "test_transformer2"]
        assert template["loader"] == "test_loader"
        assert template["config"]["batch_size"] == 100
        assert template["metadata"]["author"] == "test_author"

    def test_save_template_yaml(self) -> None:
        """Test saving a template in YAML format."""
        # Create a template
        template = {
            "version": "1.0.0",
            "pipelines": [
                {
                    "name": "test_pipeline",
                    "extractor": {"name": "test_extractor", "type": "TestExtractor"},
                    "transformers": [{"name": "test_transformer", "type": "TestTransformer"}],
                }
            ],
        }

        # Save the template
        file_path = os.path.join(self.temp_dir.name, "test_template.yaml")
        self.generator.save_template(template, file_path, "yaml")

        # Check that the file was created
        assert os.path.exists(file_path)

        # Check that the file contains the expected content
        with open(file_path) as f:
            loaded_template = yaml.safe_load(f)
        assert loaded_template["version"] == "1.0.0"
        assert loaded_template["pipelines"][0]["name"] == "test_pipeline"

    def test_save_template_json(self) -> None:
        """Test saving a template in JSON format."""
        # Create a template
        template = {
            "version": "1.0.0",
            "pipelines": [
                {
                    "name": "test_pipeline",
                    "extractor": {"name": "test_extractor", "type": "TestExtractor"},
                    "transformers": [{"name": "test_transformer", "type": "TestTransformer"}],
                }
            ],
        }

        # Save the template
        file_path = os.path.join(self.temp_dir.name, "test_template.json")
        self.generator.save_template(template, file_path, "json")

        # Check that the file was created
        assert os.path.exists(file_path)

        # Check that the file contains the expected content
        with open(file_path) as f:
            loaded_template = json.load(f)
        assert loaded_template["version"] == "1.0.0"
        assert loaded_template["pipelines"][0]["name"] == "test_pipeline"

    def test_create_example_template(self) -> None:
        """Test creating an example template."""
        # Create an example template
        file_path = os.path.join(self.temp_dir.name, "example_template.yaml")
        self.generator.create_example_template(file_path)

        # Check that the file was created
        assert os.path.exists(file_path)

        # Check that the file contains the expected content
        with open(file_path) as f:
            template = yaml.safe_load(f)
        assert "version" in template
        assert "extractors" in template
        assert "transformers" in template
        assert "loaders" in template
        assert "pipelines" in template


if __name__ == "__main__":
    unittest.main()
