"""
Unit tests for the workflow validator.
"""

import unittest
from typing import Any

import pytest

from workflows.base import BaseExtractor, BaseLoader, BaseTransformer, Pipeline
from workflows.exceptions import ValidationError
from workflows.validator import WorkflowValidator


# Create concrete implementations with format tracking for testing
class TestExtractor(BaseExtractor[list[dict[str, Any]]]):
    """Test extractor that outputs a list of dictionaries."""

    def __init__(self, name: str, config: dict[str, Any] | None = None) -> None:
        """Initialize the TestExtractor."""
        super().__init__(name, config)
        self.source = config.get("source", "test_source") if config else "test_source"
        self.output_format = "list_of_dicts"

    def extract(self) -> list[dict[str, Any]]:
        """Extract test data."""
        return [
            {"id": 1, "name": "Test 1"},
            {"id": 2, "name": "Test 2"},
        ]

    def validate_source(self) -> bool:
        """Validate the test source."""
        return True


class TestTransformer(BaseTransformer[list[dict[str, Any]], list[dict[str, Any]]]):
    """Test transformer that adds a field to each dictionary."""

    def __init__(self, name: str, config: dict[str, Any] | None = None) -> None:
        """Initialize the TestTransformer."""
        super().__init__(name, config)
        self.accepts_formats = ["list_of_dicts"]
        self.output_format = "list_of_dicts"

    def transform(self, data: list[dict[str, Any]]) -> list[dict[str, Any]]:
        """Transform test data."""
        for item in data:
            item["transformed"] = True
        return data

    def validate_input(self, data: list[dict[str, Any]]) -> bool:
        """Validate test input."""
        return isinstance(data, list)

    def validate_output(self, data: list[dict[str, Any]]) -> bool:
        """Validate test output."""
        return isinstance(data, list)


class AnotherTestTransformer(BaseTransformer[list[dict[str, Any]], dict[str, Any]]):
    """Test transformer that outputs a single dictionary."""

    def __init__(self, name: str, config: dict[str, Any] | None = None) -> None:
        """Initialize the AnotherTestTransformer."""
        super().__init__(name, config)
        self.accepts_formats = ["list_of_dicts"]
        self.output_format = "dict"

    def transform(self, data: list[dict[str, Any]]) -> dict[str, Any]:
        """Transform test data."""
        result = {}
        for item in data:
            result[str(item["id"])] = item
        return result

    def validate_input(self, data: list[dict[str, Any]]) -> bool:
        """Validate test input."""
        return isinstance(data, list)

    def validate_output(self, data: dict[str, Any]) -> bool:
        """Validate test output."""
        return isinstance(data, dict)


class TestLoader(BaseLoader[list[dict[str, Any]]]):
    """Test loader that loads a list of dictionaries."""

    def __init__(self, name: str, config: dict[str, Any] | None = None) -> None:
        """Initialize the TestLoader."""
        super().__init__(name, config)
        self.accepts_formats = ["list_of_dicts"]

    def load(self, data: list[dict[str, Any]]) -> None:
        """Load test data."""
        pass

    def validate_destination(self) -> bool:
        """Validate the test destination."""
        return True


class AnotherTestLoader(BaseLoader[dict[str, Any]]):
    """Test loader that loads a dictionary."""

    def __init__(self, name: str, config: dict[str, Any] | None = None) -> None:
        """Initialize the AnotherTestLoader."""
        super().__init__(name, config)
        self.accepts_formats = ["dict"]

    def load(self, data: dict[str, Any]) -> None:
        """Load test data."""
        pass

    def validate_destination(self) -> bool:
        """Validate the test destination."""
        return True


class TestWorkflowValidator(unittest.TestCase):
    """Test the workflow validator."""

    def setUp(self) -> None:
        """Set up the test case."""
        self.validator = WorkflowValidator()

        # Create some test components
        self.extractor = TestExtractor("test_extractor")
        self.transformer1 = TestTransformer("test_transformer1")
        self.transformer2 = AnotherTestTransformer("test_transformer2")
        self.loader1 = TestLoader("test_loader1")
        self.loader2 = AnotherTestLoader("test_loader2")

        # Create pipelines
        self.valid_pipeline1 = Pipeline(
            "valid_pipeline1",
            self.extractor,
            [self.transformer1],
            self.loader1,
        )

        self.valid_pipeline2 = Pipeline(
            "valid_pipeline2",
            self.extractor,
            [self.transformer1, self.transformer2],
            self.loader2,
        )

        # Create collections
        self.extractors = {"test_extractor": self.extractor}
        self.transformers = {
            "test_transformer1": self.transformer1,
            "test_transformer2": self.transformer2,
        }
        self.loaders = {
            "test_loader1": self.loader1,
            "test_loader2": self.loader2,
        }
        self.pipelines = {
            "valid_pipeline1": self.valid_pipeline1,
            "valid_pipeline2": self.valid_pipeline2,
        }

    def test_validate_component_extractor(self) -> None:
        """Test validating an extractor."""
        result = self.validator.validate_component(self.extractor)
        assert result

    def test_validate_component_transformer(self) -> None:
        """Test validating a transformer."""
        result = self.validator.validate_component(self.transformer1)
        assert result

    def test_validate_component_loader(self) -> None:
        """Test validating a loader."""
        result = self.validator.validate_component(self.loader1)
        assert result

    def test_validate_pipeline(self) -> None:
        """Test validating a pipeline."""
        result = self.validator.validate_pipeline(self.valid_pipeline1)
        assert result

    def test_validate_workflow(self) -> None:
        """Test validating the entire workflow."""
        result = self.validator.validate_workflow(self.extractors, self.transformers, self.loaders, self.pipelines)
        assert result

    def test_invalid_pipeline_no_extractor(self) -> None:
        """Test validating a pipeline with no extractor."""
        invalid_pipeline = Pipeline(
            "invalid_pipeline",
            None,  # type: ignore
            [self.transformer1],
            self.loader1,
        )

        with pytest.raises(ValidationError):
            self.validator.validate_pipeline(invalid_pipeline)

    def test_invalid_pipeline_no_transformers(self) -> None:
        """Test validating a pipeline with no transformers."""
        invalid_pipeline = Pipeline(
            "invalid_pipeline",
            self.extractor,
            [],
            self.loader1,
        )

        with pytest.raises(ValidationError):
            self.validator.validate_pipeline(invalid_pipeline)

    def test_format_compatibility_checking(self) -> None:
        """Test format compatibility checking between components."""
        # This pipeline should be valid
        valid_pipeline = Pipeline(
            "valid_pipeline",
            self.extractor,  # output_format = "list_of_dicts"
            [self.transformer1],  # accepts_formats = ["list_of_dicts"]
            self.loader1,  # accepts_formats = ["list_of_dicts"]
        )

        result = self.validator.validate_pipeline(valid_pipeline)
        assert result

        # This pipeline should also be valid with chained transformers
        valid_pipeline2 = Pipeline(
            "valid_pipeline2",
            self.extractor,  # output_format = "list_of_dicts"
            [
                self.transformer1,  # accepts_formats = ["list_of_dicts"], output_format = "list_of_dicts"
                self.transformer2,  # accepts_formats = ["list_of_dicts"], output_format = "dict"
            ],
            self.loader2,  # accepts_formats = ["dict"]
        )

        result = self.validator.validate_pipeline(valid_pipeline2)
        assert result


if __name__ == "__main__":
    unittest.main()
