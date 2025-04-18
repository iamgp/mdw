"""
API client for fetching data from external APIs.

This module provides a client for making API requests to fetch data from
external systems. It handles authentication, pagination, and error handling.
"""

import logging
import time
from datetime import datetime
from typing import Any

import requests
from requests.exceptions import RequestException


class APIClient:
    """Client for fetching data from external APIs."""

    def __init__(self, base_url: str, api_key: str | None = None, timeout: int = 30):
        """Initialize API client.

        Args:
            base_url: Base URL for API endpoints
            api_key: Optional API key for authentication
            timeout: Request timeout in seconds
        """
        self.base_url = base_url.rstrip("/")
        self.api_key = api_key
        self.timeout = timeout
        self.logger = logging.getLogger(__name__)
        self.session = requests.Session()

        # Setup basic headers
        headers = {"Content-Type": "application/json"}
        if api_key:
            headers["Authorization"] = f"Bearer {api_key}"

        self.session.headers.update(headers)

    def _make_request(
        self,
        method: str,
        endpoint: str,
        params: dict[str, Any] | None = None,
        data: dict[str, Any] | None = None,
        retry_count: int = 3,
        retry_delay: int = 2,
    ) -> dict[str, Any]:
        """Make an HTTP request to the API.

        Args:
            method: HTTP method (GET, POST, etc.)
            endpoint: API endpoint path
            params: Optional query parameters
            data: Optional request body data
            retry_count: Number of retry attempts for failed requests
            retry_delay: Delay between retries in seconds

        Returns:
            Parsed JSON response as a dictionary

        Raises:
            ValueError: If the API request fails after retries
        """
        url = f"{self.base_url}/{endpoint.lstrip('/')}"
        attempts = 0

        while attempts < retry_count:
            try:
                self.logger.debug(f"Making {method} request to {url}")

                # Make the request
                if method.upper() == "GET":
                    response = self.session.get(url, params=params, timeout=self.timeout)
                elif method.upper() == "POST":
                    response = self.session.post(url, params=params, json=data, timeout=self.timeout)
                else:
                    raise ValueError(f"Unsupported HTTP method: {method}")

                # Check for success
                response.raise_for_status()

                # Parse and return response
                return response.json()

            except RequestException as e:
                attempts += 1
                self.logger.warning(f"Request failed (attempt {attempts}/{retry_count}): {str(e)}")

                if attempts >= retry_count:
                    self.logger.error(f"Request failed after {retry_count} attempts")
                    raise ValueError(f"API request failed: {str(e)}") from e

                # Wait before retrying
                time.sleep(retry_delay)

    def get_customers(self) -> list[dict[str, Any]]:
        """Fetch customer data from the API.

        Returns:
            List of customer records
        """
        self.logger.info("Fetching customer data")
        response = self._make_request("GET", "customers")

        # Ensure the response has the expected format
        if not isinstance(response, dict) or "customers" not in response:
            raise ValueError("Invalid customer data format")

        return response["customers"]

    def get_orders(self, start_date: datetime | None = None) -> list[dict[str, Any]]:
        """Fetch order data from the API.

        Args:
            start_date: Optional start date to filter orders

        Returns:
            List of order records
        """
        self.logger.info("Fetching order data")

        params = {}
        if start_date:
            params["start_date"] = start_date.strftime("%Y-%m-%d")

        response = self._make_request("GET", "orders", params=params)

        # Ensure the response has the expected format
        if not isinstance(response, dict) or "orders" not in response:
            raise ValueError("Invalid order data format")

        return response["orders"]

    def get_products(self) -> list[dict[str, Any]]:
        """Fetch product data from the API.

        Returns:
            List of product records
        """
        self.logger.info("Fetching product data")
        response = self._make_request("GET", "products")

        # Ensure the response has the expected format
        if not isinstance(response, dict) or "products" not in response:
            raise ValueError("Invalid product data format")

        return response["products"]
