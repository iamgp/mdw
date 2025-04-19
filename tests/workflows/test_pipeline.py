"""
Unit tests for the Pipeline class.
"""

import unittest
from typing import Any
from unittest.mock import MagicMock, patch

import pytest

from data_warehouse.workflows.core.base import BaseExtractor, BaseLoader, BaseTransformer, Pipeline


class TestPipeline(unittest.TestCase):
    """Test the Pipeline class."""

    def setUp(self) -> None:
        """Set up the test case."""
        # Create mock components
        self.mock_extractor = MagicMock(spec=BaseExtractor)
        self.mock_extractor.name = "mock_extractor"
        self.mock_extractor.extract.return_value = {"data": "raw_data"}

        self.mock_transformer1 = MagicMock(spec=BaseTransformer)
        self.mock_transformer1.name = "mock_transformer1"
        self.mock_transformer1.transform.return_value = {"data": "transformed_data_1"}

        self.mock_transformer2 = MagicMock(spec=BaseTransformer)
        self.mock_transformer2.name = "mock_transformer2"
        self.mock_transformer2.transform.return_value = {"data": "transformed_data_2"}

        self.mock_loader = MagicMock(spec=BaseLoader)
        self.mock_loader.name = "mock_loader"

        # Create pipeline
        self.pipeline = Pipeline(
            name="test_pipeline",
            extractor=self.mock_extractor,
            transformers=[self.mock_transformer1, self.mock_transformer2],
            loader=self.mock_loader,
        )

    def test_init(self) -> None:
        """Test the Pipeline initialization."""
        assert self.pipeline.name == "test_pipeline"
        assert self.pipeline.extractor == self.mock_extractor
        assert self.pipeline.transformers == [self.mock_transformer1, self.mock_transformer2]
        assert self.pipeline.loader == self.mock_loader

    def test_run_pipeline(self) -> None:
        """Test the run method of the Pipeline."""
        # Run the pipeline
        result = self.pipeline.run()

        # Check that the extractor was called
        self.mock_extractor.extract.assert_called_once()

        # Check that the transformers were called with the correct data
        self.mock_transformer1.transform.assert_called_once_with({"data": "raw_data"})
        self.mock_transformer2.transform.assert_called_once_with({"data": "transformed_data_1"})

        # Check that the loader was called with the correct data
        self.mock_loader.load.assert_called_once_with({"data": "transformed_data_2"})

        # Check the result
        assert result == {"data": "transformed_data_2"}

    def test_run_pipeline_with_no_transformers(self) -> None:
        """Test the run method of the Pipeline with no transformers."""
        # Create a pipeline with no transformers
        pipeline = Pipeline(
            name="test_pipeline_no_transformers",
            extractor=self.mock_extractor,
            transformers=[],
            loader=self.mock_loader,
        )

        # Run the pipeline
        result = pipeline.run()

        # Check that the extractor was called
        self.mock_extractor.extract.assert_called_once()

        # Check that the loader was called with the correct data (directly from extractor)
        self.mock_loader.load.assert_called_once_with({"data": "raw_data"})

        # Check the result
        assert result == {"data": "raw_data"}

    @patch("workflows.base.logger")
    def test_pipeline_with_error_handling(self, mock_logger: MagicMock) -> None:
        """Test the Pipeline with error handling."""
        # Set up the extractor to raise an exception
        self.mock_extractor.extract.side_effect = ValueError("Test error")

        # Run the pipeline (should raise exception)
        with pytest.raises(ValueError, match="Test error"):
            self.pipeline.run()

        # Check that the logger was called with an error
        mock_logger.error.assert_called()

    def test_str_representation(self) -> None:
        """Test the string representation of the Pipeline."""
        pipeline_str = str(self.pipeline)
        assert "test_pipeline" in pipeline_str
        assert "mock_extractor" in pipeline_str
        assert "mock_transformer1" in pipeline_str
        assert "mock_transformer2" in pipeline_str
        assert "mock_loader" in pipeline_str

    def test_repr_representation(self) -> None:
        """Test the repr representation of the Pipeline."""
        pipeline_repr = repr(self.pipeline)
        assert "test_pipeline" in pipeline_repr
        assert "mock_extractor" in pipeline_repr
        assert "mock_transformer1" in pipeline_repr
        assert "mock_transformer2" in pipeline_repr
        assert "mock_loader" in pipeline_repr


class CustomExtractor(BaseExtractor[Any]):
    """Custom extractor for integration testing."""

    def extract(self) -> Any:
        return self.config.get("data", "default_extracted")

    def validate_source(self) -> bool:
        return True


class CustomTransformer(BaseTransformer[Any, Any]):
    """Custom transformer for integration testing."""

    def transform(self, data: Any) -> Any:
        return f"{data}_{self.config.get('suffix', 'transformed')}"

    def validate_input(self, data: Any) -> bool:
        return True

    def validate_output(self, data: Any) -> bool:
        return True


class CustomLoader(BaseLoader[Any]):
    """Custom loader for integration testing."""

    def __init__(self, name: str = "custom_loader", **kwargs: Any):
        """Initialize with a results list to store loaded data."""
        super().__init__(name=name, **kwargs)
        self.results: list[dict[str, Any]] = []

    def load(self, data: Any) -> None:
        """Load method that stores the data in the results list."""
        self.results.append(data)

    def validate_destination(self) -> bool:
        return True


class TestPipelineIntegration(unittest.TestCase):
    """Integration tests for the Pipeline class with real components."""

    def test_pipeline_integration(self) -> None:
        """Test the Pipeline with real extractor, transformer, and loader."""
        # Create actual components
        extractor = CustomExtractor(name="custom_extractor")
        transformer = CustomTransformer(name="custom_transformer")
        loader = CustomLoader(name="custom_loader")

        # Create pipeline
        pipeline = Pipeline(name="integration_pipeline", extractor=extractor, transformers=[transformer], loader=loader)

        # Run the pipeline
        result = pipeline.run()

        # Check the result
        assert result == {"data": "source_data_transformed"}

        # Check that the loader stored the correct data
        assert len(loader.results) == 1
        assert loader.results[0] == {"data": "source_data_transformed"}


if __name__ == "__main__":
    unittest.main()
