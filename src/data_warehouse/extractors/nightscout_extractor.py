"""Nightscout data extraction module using SlingExtractor."""

from datetime import datetime, timedelta
from typing import Any

from loguru import logger
from pydantic import Field
from pydantic import ValidationError as PydanticValidationError

from data_warehouse.extractors.sling_extractor import SlingExtractor
from data_warehouse.workflows.core.base import BaseWorkflowConfig
from data_warehouse.workflows.core.exceptions import ValidationError

# Assuming models are defined here relative to the original location
# If this path is incorrect, we'll need to adjust it based on actual location
# TODO: Check if this model import is correct after moving the file


class NightscoutConfig(BaseWorkflowConfig):
    """Configuration for Nightscout data extraction."""

    base_url: str = Field(..., description="Nightscout base URL")
    api_secret: str | None = Field(None, description="Nightscout API secret (optional)")
    lookback_hours: int = Field(24, description="Hours of data to fetch")


class NightscoutExtractor(SlingExtractor):
    """Handles data extraction from Nightscout API using Sling."""

    def __init__(self, name: str, config: dict[str, Any] | None = None) -> None:
        """
        Initialize NightscoutExtractor.

        The main config dictionary should contain the 'sling_config' structure
        required by the base SlingExtractor, plus Nightscout-specific details like
        'base_url', 'lookback_hours' outside the 'sling_config'.
        'api_secret' is optional.

        Args:
            name: Name of the extractor instance.
            config: Configuration dictionary. Expected to contain 'base_url' and optionally
                    'api_secret', 'lookback_hours', and 'sling_config'.
        """
        # Only base_url is strictly required now
        if not config or "base_url" not in config:
            raise ValidationError("NightscoutExtractor config requires 'base_url'.")

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

        # Prepare the *specific* Sling SOURCE config needed for Nightscout API calls
        source_config = {
            "stream": f"{self.nightscout_config.base_url}/api/v1/entries/sgv",
            # Params removed from here
            "options": {
                "jmespath": "$",
                # Nest params and auth under a hypothetical request_options key
                "request_options": {
                    "params": [
                        {"key": "find[dateString][$gte]", "value": start_time.isoformat()},
                        {"key": "find[dateString][$lte]", "value": end_time.isoformat()},
                    ]
                    # Auth will be added here conditionally below
                },
            },
        }

        # Add auth to options.request_options if api_secret is provided
        if self.nightscout_config.api_secret:
            if "request_options" not in source_config["options"]:
                source_config["options"]["request_options"] = {}  # Should exist from params, but safety check
            source_config["options"]["request_options"]["auth"] = {
                "type": "header",
                "token": self.nightscout_config.api_secret,
                "header_name": "API-SECRET",
            }

        # Extract any *base* sling config
        base_sling_config = config.get("sling_config", {})

        # Create the final config dictionary, nesting the source config
        final_sling_config = {
            **base_sling_config,  # Merge base options
            "source": source_config,  # Add the nested source configuration
        }

        # Create the config dict to pass to the BASE class __init__.
        config_for_base = {
            **(config or {}),
            "sling_config": final_sling_config,  # Add/overwrite with the structured one
        }

        # Call the base class __init__ first, passing the prepared config
        super().__init__(name, config_for_base)

        # Base class SlingExtractor sets self._sling_config = self.config["sling_config"],
        # so we no longer need to set it explicitly here.

        self.logger = logger.bind(component=f"nightscout_extractor.{name}")

    # No need to override validate_source unless Nightscout needs specific checks
    # The base SlingExtractor validation should suffice for the API source.

    # We can override get_metadata if we want to add more Nightscout-specific details
    # def get_metadata(self) -> MetadataType:
    #     metadata = super().get_metadata()
    #     metadata["nightscout_base_url"] = self.nightscout_config.base_url
    #     metadata["lookback_hours"] = self.nightscout_config.lookback_hours
    #     return metadata
