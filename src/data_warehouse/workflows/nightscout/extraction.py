"""Nightscout data extraction module."""

from datetime import datetime, timedelta

import httpx
from dagster import OpExecutionContext
from loguru import logger
from pydantic import Field

from ..._templates.base_workflow import BaseWorkflowConfig
from .models import GlucoseData, GlucoseReading


class NightscoutConfig(BaseWorkflowConfig):
    """Configuration for Nightscout data extraction."""

    base_url: str = Field(..., description="Nightscout base URL")
    api_secret: str = Field(..., description="Nightscout API secret")
    lookback_hours: int = Field(24, description="Hours of data to fetch")


class NightscoutExtractor:
    """Handles data extraction from Nightscout API."""

    def __init__(self, config: NightscoutConfig) -> None:
        """Initialize with configuration."""
        self.config = config
        self.logger = logger.bind(component="nightscout_extractor")

    async def extract_glucose_data(self, context: OpExecutionContext) -> GlucoseData:
        """Extract glucose readings from Nightscout API."""
        async with httpx.AsyncClient() as client:
            end_time = datetime.utcnow()
            start_time = end_time - timedelta(hours=self.config.lookback_hours)

            url = f"{self.config.base_url}/api/v1/entries/sgv"
            params = {
                "find[dateString][$gte]": start_time.isoformat(),
                "find[dateString][$lte]": end_time.isoformat(),
            }
            headers = {"API-SECRET": self.config.api_secret}

            try:
                response = await client.get(url, params=params, headers=headers)
                response.raise_for_status()
                data = response.json()

                readings = [
                    GlucoseReading(
                        timestamp=entry["dateString"],
                        glucose=entry["sgv"],
                        device=entry.get("device", "unknown"),
                        type=entry.get("type", "sgv"),
                        direction=entry.get("direction"),
                        noise=entry.get("noise"),
                        filtered=entry.get("filtered"),
                        unfiltered=entry.get("unfiltered"),
                        rssi=entry.get("rssi"),
                    )
                    for entry in data
                ]

                return GlucoseData(
                    readings=readings,
                    start_time=start_time,
                    end_time=end_time,
                    device_info={"source": "nightscout", "version": "1.0"},
                )

            except Exception as e:
                self.logger.error(f"Failed to extract data: {e!s}")
                raise
