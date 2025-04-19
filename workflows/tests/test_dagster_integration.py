"""
Unit tests for the Dagster integration module.
"""

import unittest
from typing import Any
from unittest.mock import MagicMock, patch

from dagster import AssetIn, AssetsDefinition

from workflows.base import BaseExtractor, BaseLoader, BaseTransformer, Pipeline
from workflows.dagster_integration import (
    create_dagster_asset_from_component,
    create_dagster_job_from_pipeline,
    get_dagster_asset_key,
    get_dagster_op_name,
)


class MockExtractor(BaseExtractor):
    """Mock extractor for testing."""

    def __init__(self, name: str = "mock_extractor", **kwargs: Any):
        """Initialize the mock extractor."""
        super().__init__(name=name, **kwargs)

    def extract(self) -> dict[str, Any]:
        """Mock extract method."""
        return {"data": "mock_data"}


class MockTransformer(BaseTransformer):
    """Mock transformer for testing."""

    def __init__(self, name: str = "mock_transformer", **kwargs: Any):
        """Initialize the mock transformer."""
        super().__init__(name=name, **kwargs)

    def transform(self, data: dict[str, Any]) -> dict[str, Any]:
        """Mock transform method."""
        return {"transformed_data": data["data"] + "_transformed"}


class MockLoader(BaseLoader):
    """Mock loader for testing."""

    def __init__(self, name: str = "mock_loader", **kwargs: Any):
        """Initialize the mock loader."""
        super().__init__(name=name, **kwargs)

    def load(self, data: dict[str, Any]) -> None:
        """Mock load method."""
        pass


class TestDagsterIntegration(unittest.TestCase):
    """Test the Dagster integration module."""

    def setUp(self) -> None:
        """Set up the test case."""
        self.extractor = MockExtractor(name="test_extractor")
        self.transformer = MockTransformer(name="test_transformer")
        self.loader = MockLoader(name="test_loader")

        self.pipeline = Pipeline(
            name="test_pipeline",
            extractor=self.extractor,
            transformers=[self.transformer],
            loader=self.loader,
        )

    def test_get_dagster_op_name(self) -> None:
        """Test the get_dagster_op_name function."""
        # Test extractor
        extractor_op_name = get_dagster_op_name(self.extractor)
        assert extractor_op_name == "test_extractor_extract"

        # Test transformer
        transformer_op_name = get_dagster_op_name(self.transformer)
        assert transformer_op_name == "test_transformer_transform"

        # Test loader
        loader_op_name = get_dagster_op_name(self.loader)
        assert loader_op_name == "test_loader_load"

    def test_get_dagster_asset_key(self) -> None:
        """Test the get_dagster_asset_key function."""
        # Test extractor
        extractor_asset_key = get_dagster_asset_key(self.extractor)
        assert extractor_asset_key == ["test_extractor"]

        # Test transformer
        transformer_asset_key = get_dagster_asset_key(self.transformer)
        assert transformer_asset_key == ["test_transformer"]

        # Test loader
        loader_asset_key = get_dagster_asset_key(self.loader)
        assert loader_asset_key == ["test_loader"]

    def test_create_dagster_asset_from_component(self) -> None:
        """Test the create_dagster_asset_from_component function."""
        # Test extractor
        extractor_asset = create_dagster_asset_from_component(self.extractor)
        assert isinstance(extractor_asset, AssetsDefinition)

        # Test transformer
        transformer_asset = create_dagster_asset_from_component(
            self.transformer, ins={"data": AssetIn(["test_extractor"])}
        )
        assert isinstance(transformer_asset, AssetsDefinition)

        # Test loader
        loader_asset = create_dagster_asset_from_component(self.loader, ins={"data": AssetIn(["test_transformer"])})
        assert isinstance(loader_asset, AssetsDefinition)

    @patch("workflows.dagster_integration.create_dagster_asset_from_component")
    def test_create_dagster_job_from_pipeline(self, mock_create_asset: MagicMock) -> None:
        """Test the create_dagster_job_from_pipeline function."""
        # Mock the create_dagster_asset_from_component function
        mock_extractor_asset = MagicMock()
        mock_transformer_asset = MagicMock()
        mock_loader_asset = MagicMock()

        mock_create_asset.side_effect = [
            mock_extractor_asset,
            mock_transformer_asset,
            mock_loader_asset,
        ]

        # Create job
        job = create_dagster_job_from_pipeline(self.pipeline)

        # Check that create_dagster_asset_from_component was called correctly
        assert mock_create_asset.call_count == 3

        # Check job
        assert job is not None


if __name__ == "__main__":
    unittest.main()
