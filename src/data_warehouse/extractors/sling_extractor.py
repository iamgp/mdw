"""
Sling Extractor Implementation.

This module provides an extractor that leverages the sling-python library
to extract data from various sources supported by Sling.
"""

import logging
import time  # Import time for duration calculation
from datetime import datetime
from typing import Any

# Import Prometheus metrics types
from prometheus_client import REGISTRY, Counter, Histogram
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

    # Class variables to hold metrics, ensuring they are defined only once
    _metrics_initialized = False
    SLING_EXTRACTOR_DURATION: Histogram | None = None
    SLING_EXTRACTOR_RECORDS: Counter | None = None
    SLING_EXTRACTOR_ERRORS: Counter | None = None

    @classmethod
    def _initialize_metrics(cls) -> None:
        """Initialize Prometheus metrics if they haven't been already."""
        if not cls._metrics_initialized:
            logger.debug("Initializing SlingExtractor Prometheus metrics...")
            # Check if metrics already exist in the registry to prevent errors
            # during hot-reloading or multiple initializations in tests
            if "sling_extractor_duration_seconds" not in REGISTRY._names_to_collectors:
                cls.SLING_EXTRACTOR_DURATION = Histogram(
                    "sling_extractor_duration_seconds",
                    "Duration of Sling extractor execution",
                    ["extractor_name"],
                )
            if "sling_extractor_records_total" not in REGISTRY._names_to_collectors:
                cls.SLING_EXTRACTOR_RECORDS = Counter(
                    "sling_extractor_records_total",
                    "Total number of records extracted by Sling extractor",
                    ["extractor_name"],
                )
            if "sling_extractor_errors_total" not in REGISTRY._names_to_collectors:
                cls.SLING_EXTRACTOR_ERRORS = Counter(
                    "sling_extractor_errors_total",
                    "Total number of errors during Sling extractor execution",
                    ["extractor_name"],
                )
            cls._metrics_initialized = True

    def __init__(self, name: str, config: dict[str, Any] | None = None) -> None:
        """
        Initialize SlingExtractor.

        Args:
            name: Name of the extractor instance.
            config: Configuration dictionary. Must contain 'sling_config'.
        """
        # Ensure metrics are initialized before super().__init__ might use them indirectly
        self._initialize_metrics()
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
        start_time = time.monotonic()
        try:
            # Sling config should be nested, e.g., {"source": {"stream": ..., "options": {}}}
            # Pass the nested source dict to the 'source' parameter of Sling()
            source_config = self._sling_config.get("source")
            if not isinstance(source_config, dict):
                raise ExtractorError(f"Invalid or missing 'source' configuration in sling_config for '{self.name}'")

            # Assuming Sling() takes source and target dicts, and we only have source
            sling_instance = Sling(source=source_config)
            # If Sling expects Source/Target *objects*, this will need further adjustment:
            # source_obj = sling.Source(**source_config)
            # sling_instance = Sling(source=source_obj)

            records = list(sling_instance.stream())
            self.last_run_time = datetime.now()

            # Record metrics on success
            duration = time.monotonic() - start_time
            if self.SLING_EXTRACTOR_DURATION:
                self.SLING_EXTRACTOR_DURATION.labels(extractor_name=self.name).observe(duration)
            if self.SLING_EXTRACTOR_RECORDS:
                self.SLING_EXTRACTOR_RECORDS.labels(extractor_name=self.name).inc(len(records))

            logger.info(
                f"Sling extraction for '{self.name}' completed successfully in {duration:.2f}s. {len(records)} records extracted."
            )
            return records
        except Exception as e:
            # Record error metric
            duration = time.monotonic() - start_time
            if self.SLING_EXTRACTOR_DURATION:  # Observe duration even on error
                self.SLING_EXTRACTOR_DURATION.labels(extractor_name=self.name).observe(duration)
            if self.SLING_EXTRACTOR_ERRORS:
                self.SLING_EXTRACTOR_ERRORS.labels(extractor_name=self.name).inc()

            logger.error(
                f"An unexpected error occurred during Sling extraction for '{self.name}' after {duration:.2f}s: {e}",
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
