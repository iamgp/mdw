"""Nightscout data extraction module."""

from datetime import datetime, timedelta
from typing import Any

import sling
from dateutil import parser
from loguru import logger
from pydantic import Field
from pydantic import ValidationError as PydanticValidationError

from data_warehouse.extractors.sling_extractor import SlingExtractor
from data_warehouse.workflows.core.base import BaseWorkflowConfig
from data_warehouse.workflows.core.exceptions import ExtractorError, ValidationError

from .models import GlucoseData, GlucoseReading


class NightscoutConfig(BaseWorkflowConfig):
    """Configuration for Nightscout data extraction."""

    base_url: str = Field(..., description="Nightscout base URL")
    api_secret: str = Field(..., description="Nightscout API secret")
    lookback_hours: int = Field(24, description="Hours of data to fetch")


class NightscoutExtractor(SlingExtractor):
    """Handles data extraction from Nightscout API using Sling."""

    def __init__(self, name: str, config: dict[str, Any] | None = None) -> None:
        """
        Initialize NightscoutExtractor.

        The main config dictionary should contain the 'sling_config' structure
        required by the base SlingExtractor, plus Nightscout-specific details like
        'base_url', 'api_secret', and 'lookback_hours' outside the 'sling_config'.

        Args:
            name: Name of the extractor instance.
            config: Configuration dictionary. Expected to contain keys like
                    'base_url', 'api_secret', 'lookback_hours', and optionally
                    a base 'sling_config' to merge with.
        """
        if not config or not all(k in config for k in ["base_url", "api_secret"]):
            raise ValidationError("NightscoutExtractor config requires 'base_url' and 'api_secret'.")

        # Extract Nightscout-specific fields for NightscoutConfig validation/use
        nightscout_specific_config = {k: v for k, v in config.items() if k in NightscoutConfig.model_fields}

        # Add required BaseWorkflowConfig fields (name, description) if missing
        if "name" not in nightscout_specific_config:
            nightscout_specific_config["name"] = name  # Use the extractor instance name
        if "description" not in nightscout_specific_config:
            nightscout_specific_config["description"] = (
                f"Nightscout extractor instance: {name}"  # Add a default description
            )

        # Validate and store the specific Nightscout config part
        try:
            self.nightscout_config = NightscoutConfig(**nightscout_specific_config)
        except PydanticValidationError as e:
            # Re-raise as our internal ValidationError for consistency if needed, or handle directly
            raise ValidationError(f"Invalid Nightscout configuration for '{name}': {e}") from e

        # Calculate times needed for sling config construction
        end_time = datetime.utcnow()
        start_time = end_time - timedelta(hours=self.nightscout_config.lookback_hours)

        # Prepare the *specific* Sling config needed for Nightscout API calls
        nightscout_sling_params = {
            "source": "api",
            "source_options": {
                "url": f"{self.nightscout_config.base_url}/api/v1/entries/sgv",
                "auth": {"type": "header", "token": self.nightscout_config.api_secret, "header_name": "API-SECRET"},
                "params": [
                    {"key": "find[dateString][$gte]", "value": start_time.isoformat()},
                    {"key": "find[dateString][$lte]", "value": end_time.isoformat()},
                ],
                "json_path": "$",
            },
        }

        # Extract any *base* sling config provided in the main config
        base_sling_config = config.get("sling_config", {})

        # Create the final merged sling_config dictionary
        final_sling_config = {**base_sling_config, **nightscout_sling_params}

        # Create the config to pass to the BASE SlingExtractor __init__.
        # Critically, this MUST contain the final merged config under the 'sling_config' key
        # so the base class validation passes and it can potentially use it initially.
        config_for_base = {**config, "sling_config": final_sling_config}

        # Call the base class __init__ first
        super().__init__(name, config_for_base)

        # **IMPORTANT**: Explicitly set self._sling_config *after* super().__init__
        # to ensure it holds the *merged* config needed for Nightscout operations,
        # overriding what the base class might have set based on the original config.
        self._sling_config = final_sling_config

        self.logger = logger.bind(component=f"nightscout_extractor.{name}")

    def extract(self) -> GlucoseData:
        """
        Extracts glucose readings from Nightscout API using Sling
        and transforms the result into GlucoseData.

        Returns:
            GlucoseData object containing the extracted readings.

        Raises:
            ExtractorError: If extraction or transformation fails.
        """
        self.logger.info(f"Starting Nightscout extraction for '{self.name}' using Sling...")
        try:
            # Base class constructor already validates sling_config presence
            # Use the merged sling_config stored in the base class
            sling_instance = sling.Sling(**self._sling_config)
            raw_data: list[dict[str, Any]] = list(sling_instance.stream())

            self.last_run_time = datetime.now()
            self.logger.info(f"Sling extraction successful. {len(raw_data)} raw records received.")

            # Transform raw data into GlucoseReading objects
            readings = []
            for entry in raw_data:
                try:
                    # Validate and parse timestamp
                    timestamp_str = entry.get("dateString")
                    if not timestamp_str or not isinstance(timestamp_str, str):
                        raise ValueError("Missing or invalid 'dateString'")
                    parsed_timestamp = parser.parse(timestamp_str)

                    # Validate and parse glucose value
                    glucose_val = entry.get("sgv")
                    if glucose_val is None:
                        raise ValueError("Missing 'sgv' value")
                    try:
                        parsed_glucose = float(glucose_val)
                    except (ValueError, TypeError) as ve:
                        raise ValueError(f"Invalid 'sgv' value: {glucose_val}") from ve

                    # Create GlucoseReading instance only if parsing succeeds
                    readings.append(
                        GlucoseReading(
                            timestamp=parsed_timestamp,
                            glucose=parsed_glucose,
                            device=entry.get("device", "unknown"),
                            type=entry.get("type", "sgv"),
                            direction=entry.get("direction"),
                            noise=entry.get("noise"),
                            filtered=entry.get("filtered"),
                            unfiltered=entry.get("unfiltered"),
                            rssi=entry.get("rssi"),
                        )
                    )
                except (ValueError, PydanticValidationError, TypeError) as e:
                    self.logger.warning(f"Skipping record due to transformation/validation error: {e}. Record: {entry}")
                    continue

            start_time = self.last_run_time - timedelta(
                hours=self.nightscout_config.lookback_hours
            )  # Approximate start time

            glucose_data = GlucoseData(
                readings=readings,
                start_time=start_time,
                end_time=self.last_run_time,  # Use actual run time as end time
                device_info={"source": "nightscout", "version": "1.0", "extractor": self.name},
            )
            self.logger.info(f"Transformation complete. {len(readings)} valid GlucoseReadings created.")
            return glucose_data

        except Exception as e:
            # Check if it's a Sling error based on type or message if possible,
            # otherwise, treat as unexpected.
            # For now, keeping the SlingClientError/SlingConnectionError check
            # even if they might not exist, to show intent. If they cause errors,
            # we'll remove them.
            is_sling_error = False
            try:
                # Attempt to check specific errors if they might exist
                if isinstance(e, (sling.SlingClientError, sling.SlingConnectionError)):
                    is_sling_error = True
            except AttributeError:
                # sling.SlingClientError or SlingConnectionError don't exist
                # We might rely on error message inspection or treat all non-parsing errors from Sling as ExtractorErrors
                pass  # Decide on a strategy here - for now, assume base Exception catch handles it

            if is_sling_error:
                self.logger.error(f"Sling extraction failed for '{self.name}': {e}", exc_info=True)
                raise ExtractorError(f"Sling extraction failed: {e}") from e
            else:
                # Handle other unexpected errors during extraction/transformation phase
                self.logger.error(
                    f"An unexpected error occurred during Nightscout extraction or transformation for '{self.name}': {e}",
                    exc_info=True,
                )
                raise ExtractorError(f"Unexpected error during Nightscout extraction/transformation: {e}") from e

    # No need to override validate_source unless Nightscout needs specific checks
    # The base SlingExtractor validation should suffice for the API source.

    # We can override get_metadata if we want to add more Nightscout-specific details
    # def get_metadata(self) -> MetadataType:
    #     metadata = super().get_metadata()
    #     metadata["nightscout_base_url"] = self.nightscout_config.base_url
    #     metadata["lookback_hours"] = self.nightscout_config.lookback_hours
    #     return metadata
