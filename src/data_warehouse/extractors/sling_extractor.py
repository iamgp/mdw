"""
Sling Extractor Implementation.

This module provides an extractor that leverages the sling-python library
to extract data from various sources supported by Sling.
"""

import logging
from datetime import datetime
from typing import Any

from sling import Sling

from data_warehouse.workflows.core.base import BaseExtractor, MetadataType
from data_warehouse.workflows.core.exceptions import ExtractorError, ValidationError

logger = logging.getLogger(__name__)


class SlingExtractor(BaseExtractor[list[dict[str, Any]]]):
    """
    An extractor that uses the sling-python library to pull data.

    Configuration requires a 'sling_config' key which directly maps to the
    configuration dictionary expected by sling-python.
    See https://slingdata-io.github.io/sling-python/sling.html#sling
    for configuration options.

    Example config:
        {
            "sling_config": {
                "source": "file",
                "source_options": {"path": "/path/to/your/data.csv"},
                "format": "csv"
            }
        }
    """

    def __init__(self, name: str, config: dict[str, Any] | None = None) -> None:
        """
        Initialize SlingExtractor.

        Args:
            name: Name of the extractor instance.
            config: Configuration dictionary. Must contain 'sling_config'.
        """
        super().__init__(name, config)
        if not self.config or "sling_config" not in self.config:
            raise ValidationError("SlingExtractor configuration must include a 'sling_config' key.")
        self._sling_config = self.config["sling_config"]

    def extract(self) -> list[dict[str, Any]]:
        """
        Extracts data using the configured Sling instance.

        Returns:
            A list of dictionaries representing the extracted records.

        Raises:
            ExtractorError: If extraction fails due to Sling errors.
        """
        logger.info(f"Starting Sling extraction for '{self.name}'...")
        try:
            # Potential for connection pooling/retry logic could be added here
            # if Sling doesn't handle it adequately internally.
            # For now, relying on Sling's internal mechanisms.
            sling_instance = Sling(**self._sling_config)
            records = list(sling_instance.stream())
            self.last_run_time = datetime.now()
            logger.info(f"Sling extraction for '{self.name}' completed successfully. {len(records)} records extracted.")
            return records
        except Exception as e:
            logger.error(
                f"An unexpected error occurred during Sling extraction for '{self.name}': {e}",
                exc_info=True,
            )
            raise ExtractorError(f"Unexpected error during Sling extraction: {e}") from e

    def validate_source(self) -> bool:
        """
        Validates the Sling source configuration.

        Attempts a minimal connection or check if possible, depending on the source type.
        Currently performs a basic configuration check. A more robust check might
        involve trying a 'limit 1' query or similar.

        Returns:
            True if the configuration seems valid, False otherwise.

        Raises:
            ValidationError: If the configuration is invalid.
        """
        logger.info(f"Validating Sling source configuration for '{self.name}'...")
        if not isinstance(self._sling_config, dict):
            raise ValidationError("SlingExtractor 'sling_config' must be a dictionary.")
        if "source" not in self._sling_config:
            raise ValidationError("SlingExtractor 'sling_config' must contain a 'source' key.")

        # Basic validation passed, could add source-specific checks here later
        # For now, assume config is valid if keys are present
        logger.info(f"Sling source configuration for '{self.name}' appears valid.")
        return True

    def get_metadata(self) -> MetadataType:
        """
        Get metadata specific to the SlingExtractor.
        Includes Sling source type.
        """
        metadata = super().get_metadata()
        metadata["sling_source_type"] = self._sling_config.get("source", "unknown")
        return metadata
