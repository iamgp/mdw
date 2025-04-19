"""
Unit tests for the base workflow classes.
"""

import unittest
from typing import Any

from data_warehouse.workflows.core.base import (
    BaseExtractor,
    BaseLoader,
    BaseTransformer,
    Pipeline,
    WorkflowManager,
)
from data_warehouse.workflows.core.exceptions import ValidationError


# Create concrete implementations for testing
class SimpleExtractor(BaseExtractor[list[dict[str, Any]]]):
    """Simple extractor that returns a static list of dictionaries."""

    def extract(self) -> list[dict[str, Any]]:
        """Extract a static list of dictionaries."""
        return [
            {"id": 1, "name": "Item 1"},
            {"id": 2, "name": "Item 2"},
        ]

    def validate_source(self) -> bool:
        """Always valid for this simple implementation."""
        return True


class SimpleTransformer(BaseTransformer[list[dict[str, Any]], list[dict[str, Any]]]):
    """Simple transformer that adds a 'transformed' field to each item."""

    def transform(self, data: list[dict[str, Any]]) -> list[dict[str, Any]]:
        """Add a 'transformed' field to each item."""
        for item in data:
            item["transformed"] = True
        return data

    def validate_input(self, data: list[dict[str, Any]]) -> bool:
        """Validate that data is a list of dictionaries with required fields."""
        if not isinstance(data, list):
            raise ValidationError("Data must be a list")

        for item in data:
            if not isinstance(item, dict):
                raise ValidationError("Each item must be a dictionary")
            if "id" not in item:
                raise ValidationError("Each item must have an 'id' field")
            if "name" not in item:
                raise ValidationError("Each item must have a 'name' field")

        return True

    def validate_output(self, data: list[dict[str, Any]]) -> bool:
        """Validate that each item has a 'transformed' field."""
        for item in data:
            if "transformed" not in item:
                raise ValidationError("Each item must have a 'transformed' field")

        return True


class SimpleLoader(BaseLoader[list[dict[str, Any]]]):
    """Simple loader that stores the data in memory."""

    def __init__(self, name: str, config: dict[str, Any] | None = None) -> None:
        """Initialize a SimpleLoader instance."""
        super().__init__(name, config)
        self.loaded_data: list[dict[str, Any]] = []

    def load(self, data: list[dict[str, Any]]) -> None:
        """Store the data in memory."""
        self.loaded_data = data

    def validate_destination(self) -> bool:
        """Always valid for this simple implementation."""
        return True


class TestBaseClasses(unittest.TestCase):
    """Test the base workflow classes."""

    def setUp(self) -> None:
        """Set up the test case."""
        self.extractor = SimpleExtractor("test_extractor")
        self.transformer = SimpleTransformer("test_transformer")
        self.loader = SimpleLoader("test_loader")
        self.pipeline = Pipeline("test_pipeline", self.extractor, [self.transformer], self.loader)
        self.workflow_manager = WorkflowManager()

    def test_extractor(self) -> None:
        """Test the extractor."""
        # Test extraction
        data = self.extractor.extract()
        assert len(data) == 2
        assert data[0]["id"] == 1
        assert data[1]["name"] == "Item 2"

        # Test source validation
        assert self.extractor.validate_source()

        # Test metadata
        metadata = self.extractor.get_metadata()
        assert metadata["name"] == "test_extractor"
        assert metadata["type"] == "SimpleExtractor"

    def test_transformer(self) -> None:
        """Test the transformer."""
        # Test input validation
        data = self.extractor.extract()
        assert self.transformer.validate_input(data)

        # Test transformation
        transformed_data = self.transformer.transform(data)
        assert transformed_data[0]["transformed"]
        assert transformed_data[1]["transformed"]

        # Test output validation
        assert self.transformer.validate_output(transformed_data)

        # Test metadata
        metadata = self.transformer.get_metadata()
        assert metadata["name"] == "test_transformer"
        assert metadata["type"] == "SimpleTransformer"

    def test_loader(self) -> None:
        """Test the loader."""
        # Test destination validation
        assert self.loader.validate_destination()

        # Test loading
        data = self.transformer.transform(self.extractor.extract())
        self.loader.load(data)

        # Verify data was loaded
        assert len(self.loader.loaded_data) == 2
        assert self.loader.loaded_data[0]["transformed"]

        # Test metadata
        metadata = self.loader.get_metadata()
        assert metadata["name"] == "test_loader"
        assert metadata["type"] == "SimpleLoader"

    def test_pipeline(self) -> None:
        """Test the pipeline."""
        # Execute the pipeline
        result = self.pipeline.execute()

        # Verify the pipeline worked end-to-end
        assert len(result) == 2
        assert result[0]["transformed"]

        # Verify loader received the data
        assert len(self.loader.loaded_data) == 2
        assert self.loader.loaded_data[0]["transformed"]

        # Test pipeline metadata
        metadata = self.pipeline.get_metadata()
        assert metadata["name"] == "test_pipeline"
        assert metadata["extractor"]["name"] == "test_extractor"

    def test_workflow_manager(self) -> None:
        """Test the workflow manager."""
        # Register components
        self.workflow_manager.register_extractor(self.extractor)
        self.workflow_manager.register_transformer(self.transformer)
        self.workflow_manager.register_loader(self.loader)
        self.workflow_manager.register_pipeline(self.pipeline)

        # Verify registration
        assert len(self.workflow_manager.get_all_extractors()) == 1
        assert len(self.workflow_manager.get_all_transformers()) == 1
        assert len(self.workflow_manager.get_all_loaders()) == 1
        assert len(self.workflow_manager.get_all_pipelines()) == 1

        # Retrieve components
        retrieved_extractor = self.workflow_manager.get_extractor("test_extractor")
        retrieved_transformer = self.workflow_manager.get_transformer("test_transformer")
        retrieved_loader = self.workflow_manager.get_loader("test_loader")
        retrieved_pipeline = self.workflow_manager.get_pipeline("test_pipeline")

        # Verify retrieval
        assert retrieved_extractor == self.extractor
        assert retrieved_transformer == self.transformer
        assert retrieved_loader == self.loader
        assert retrieved_pipeline == self.pipeline


if __name__ == "__main__":
    unittest.main()
