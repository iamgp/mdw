"""
Nightscout Data Extraction Module.

This module provides classes for extracting data from Nightscout API.
"""

from datetime import datetime, timedelta
from typing import Any

import requests
from loguru import logger

from data_warehouse.core.exceptions import ExtractorError
from data_warehouse.workflow.base import WorkflowContext
from data_warehouse.workflow.etl import ExtractorBase


class NightscoutExtractor(ExtractorBase[dict[str, Any]]):
    """Extractor for Nightscout API data."""

    # Default API limit for Nightscout record count
    DEFAULT_RECORD_LIMIT = 10000

    def __init__(self, config: dict[str, Any] | None = None) -> None:
        """Initialize the Nightscout extractor.

        Args:
            config: Configuration dictionary
        """
        super().__init__(config)
        self.headers = {
            "Accept": "application/json",
            "Content-Type": "application/json",
        }

        # Add API key if provided
        if self.config.credentials.get("api_secret"):
            api_secret = self.config.credentials["api_secret"]
            self.headers["api-secret"] = api_secret

    def extract(self, context: WorkflowContext) -> dict[str, Any]:
        """Extract data from Nightscout API.

        Args:
            context: The workflow context

        Returns:
            Dictionary of extracted data by type

        Raises:
            ExtractorError: If extraction fails
        """
        nightscout_url = context.config.get("nightscout_url")

        if not nightscout_url:
            raise ExtractorError("Missing Nightscout URL in configuration")

        # Determine time range for extraction
        days_to_extract = context.config.get("days_to_extract", 1)
        end_date = datetime.now()
        start_date = end_date - timedelta(days=days_to_extract)

        # Get the configurable record limit (default: 10000)
        record_limit = context.config.get("record_limit", self.DEFAULT_RECORD_LIMIT)

        # Format dates for API
        start_date_str = start_date.strftime("%Y-%m-%d")
        end_date_str = end_date.strftime("%Y-%m-%d")

        logger.info(
            f"Extracting Nightscout data from {start_date_str} to {end_date_str} (limit: {record_limit} records)"
        )

        # Prepare the result container
        result = {
            "entries": [],
            "treatments": [],
            "profiles": [],
            "devicestatus": [],
        }

        try:
            # Extract entries (CGM data)
            entries = self._extract_entries(nightscout_url, start_date, end_date, record_limit)
            result["entries"] = entries
            logger.info(f"Extracted {len(entries)} CGM entries")

            # Extract treatments
            treatments = self._extract_treatments(nightscout_url, start_date, end_date, record_limit)
            result["treatments"] = treatments
            logger.info(f"Extracted {len(treatments)} treatments")

            # Extract profiles
            profiles = self._extract_profiles(nightscout_url)
            result["profiles"] = profiles
            logger.info(f"Extracted {len(profiles)} profiles")

            # Extract device status
            devicestatus = self._extract_devicestatus(nightscout_url, start_date, end_date, record_limit)
            result["devicestatus"] = devicestatus
            logger.info(f"Extracted {len(devicestatus)} device statuses")

            return result

        except requests.RequestException as e:
            error_msg = f"Failed to extract data from Nightscout API: {str(e)}"
            if hasattr(e, "response") and e.response is not None:
                error_msg += f", Status code: {e.response.status_code}"
                if e.response.text:
                    error_msg += f", Response: {e.response.text[:200]}..."

            logger.error(error_msg)
            raise ExtractorError(error_msg) from e

    def _extract_entries(
        self, nightscout_url: str, start_date: datetime, end_date: datetime, record_limit: int
    ) -> list[dict[str, Any]]:
        """Extract CGM entries from Nightscout.

        Args:
            nightscout_url: The Nightscout instance URL
            start_date: Start date for extraction
            end_date: End date for extraction
            record_limit: Maximum number of records to retrieve

        Returns:
            List of CGM entries

        Raises:
            requests.RequestException: If the API request fails
        """
        # Convert dates to timestamps for Nightscout API
        start_timestamp = int(start_date.timestamp() * 1000)
        end_timestamp = int(end_date.timestamp() * 1000)

        # Format the URL for entries
        entries_url = f"{nightscout_url}/api/v1/entries.json"
        params = {
            "find[date][$gte]": start_timestamp,
            "find[date][$lte]": end_timestamp,
            "count": record_limit,
        }

        logger.debug(f"Requesting entries from {entries_url} (limit: {record_limit})")
        response = requests.get(entries_url, params=params, headers=self.headers)
        response.raise_for_status()

        return response.json()

    def _extract_treatments(
        self, nightscout_url: str, start_date: datetime, end_date: datetime, record_limit: int
    ) -> list[dict[str, Any]]:
        """Extract treatments from Nightscout.

        Args:
            nightscout_url: The Nightscout instance URL
            start_date: Start date for extraction
            end_date: End date for extraction
            record_limit: Maximum number of records to retrieve

        Returns:
            List of treatments

        Raises:
            requests.RequestException: If the API request fails
        """
        # Convert dates to timestamps for Nightscout API
        start_timestamp = int(start_date.timestamp() * 1000)
        end_timestamp = int(end_date.timestamp() * 1000)

        # Format the URL for treatments
        treatments_url = f"{nightscout_url}/api/v1/treatments.json"
        params = {
            "find[created_at][$gte]": start_timestamp,
            "find[created_at][$lte]": end_timestamp,
            "count": record_limit,
        }

        logger.debug(f"Requesting treatments from {treatments_url} (limit: {record_limit})")
        response = requests.get(treatments_url, params=params, headers=self.headers)
        response.raise_for_status()

        return response.json()

    def _extract_profiles(self, nightscout_url: str) -> list[dict[str, Any]]:
        """Extract profiles from Nightscout.

        Args:
            nightscout_url: The Nightscout instance URL

        Returns:
            List of profiles

        Raises:
            requests.RequestException: If the API request fails
        """
        # Format the URL for profiles
        profiles_url = f"{nightscout_url}/api/v1/profile.json"

        logger.debug(f"Requesting profiles from {profiles_url}")
        response = requests.get(profiles_url, headers=self.headers)
        response.raise_for_status()

        return response.json()

    def _extract_devicestatus(
        self, nightscout_url: str, start_date: datetime, end_date: datetime, record_limit: int
    ) -> list[dict[str, Any]]:
        """Extract device status from Nightscout.

        Args:
            nightscout_url: The Nightscout instance URL
            start_date: Start date for extraction
            end_date: End date for extraction
            record_limit: Maximum number of records to retrieve

        Returns:
            List of device statuses

        Raises:
            requests.RequestException: If the API request fails
        """
        # Convert dates to timestamps for Nightscout API
        start_timestamp = int(start_date.timestamp() * 1000)
        end_timestamp = int(end_date.timestamp() * 1000)

        # Format the URL for device status
        devicestatus_url = f"{nightscout_url}/api/v1/devicestatus.json"
        params = {
            "find[created_at][$gte]": start_timestamp,
            "find[created_at][$lte]": end_timestamp,
            "count": record_limit,
        }

        logger.debug(f"Requesting device status from {devicestatus_url} (limit: {record_limit})")
        response = requests.get(devicestatus_url, params=params, headers=self.headers)
        response.raise_for_status()

        return response.json()

    def validate_source(self) -> bool:
        """Validate the Nightscout API configuration.

        Returns:
            True if the source is valid, False otherwise
        """
        nightscout_url = self.config.source_name

        if not nightscout_url:
            logger.warning("Missing Nightscout URL")
            return False

        try:
            # Check if Nightscout is accessible by calling the status endpoint
            status_url = f"{nightscout_url}/api/v1/status.json"
            response = requests.get(status_url, headers=self.headers)
            response.raise_for_status()

            # Validate that it's actually a Nightscout instance
            status = response.json()
            if "version" not in status:
                logger.warning("Not a valid Nightscout instance")
                return False

            logger.info(f"Successfully validated Nightscout instance: v{status['version']}")
            return True

        except Exception as e:
            logger.warning(f"Failed to validate Nightscout source: {str(e)}")
            return False
