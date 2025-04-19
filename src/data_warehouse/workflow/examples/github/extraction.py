"""
GitHub Data Extraction Module.

This module provides classes for extracting data from GitHub API.
"""

from typing import Any

import requests
from loguru import logger

from data_warehouse.core.exceptions import ExtractorError
from data_warehouse.workflow.base import WorkflowContext
from data_warehouse.workflow.etl import ExtractorBase


class GitHubExtractor(ExtractorBase[list[dict[str, Any]]]):
    """Extractor for GitHub API data."""

    API_BASE_URL = "https://api.github.com"

    def __init__(self, config: dict[str, Any] | None = None) -> None:
        """Initialize the GitHub extractor.

        Args:
            config: Configuration dictionary
        """
        super().__init__(config)
        self.headers = {
            "Accept": "application/vnd.github.v3+json",
        }
        if self.config.credentials.get("token"):
            self.headers["Authorization"] = f"Bearer {self.config.credentials['token']}"

    def extract(self, context: WorkflowContext) -> list[dict[str, Any]]:
        """Extract data from GitHub API.

        Args:
            context: The workflow context

        Returns:
            List of extracted data records

        Raises:
            ExtractorError: If extraction fails
        """
        # Extract repository or organization name from context
        entity_type = context.config.get("entity_type", "repo")
        entity_name = context.config.get("entity_name")

        if not entity_name:
            raise ExtractorError("Missing entity_name in context config")

        endpoint = context.config.get("endpoint", "issues")
        params = context.config.get("params", {})

        logger.info(f"Extracting GitHub {endpoint} for {entity_type} {entity_name}")

        try:
            # Construct the API path based on entity type
            if entity_type == "repo":
                if "/" in entity_name:
                    owner, repo = entity_name.split("/", 1)
                else:
                    raise ExtractorError("Repository name must be in format 'owner/repo'")

                api_path = f"/repos/{owner}/{repo}/{endpoint}"
            elif entity_type == "org":
                api_path = f"/orgs/{entity_name}/{endpoint}"
            elif entity_type == "user":
                api_path = f"/users/{entity_name}/{endpoint}"
            else:
                raise ExtractorError(f"Unsupported entity type: {entity_type}")

            # Make the API request
            url = f"{self.API_BASE_URL}{api_path}"
            logger.debug(f"Making request to {url}")

            response = requests.get(url, headers=self.headers, params=params)
            response.raise_for_status()

            # Parse response
            data = response.json()
            if isinstance(data, dict):
                # Some APIs return a single object instead of a list
                return [data]
            return data

        except requests.RequestException as e:
            error_msg = f"Failed to extract data from GitHub API: {str(e)}"
            if hasattr(e, "response") and e.response is not None:
                error_msg += f", Status code: {e.response.status_code}"
                if e.response.text:
                    error_msg += f", Response: {e.response.text[:200]}..."

            logger.error(error_msg)
            raise ExtractorError(error_msg) from e

    def validate_source(self) -> bool:
        """Validate the GitHub API configuration.

        Returns:
            True if the source is valid, False otherwise
        """
        # Check if a token is provided

        # Validate the API access with a simple request
        try:
            response = requests.get(f"{self.API_BASE_URL}/rate_limit", headers=self.headers)
            response.raise_for_status()
            return True
        except requests.RequestException as e:
            logger.warning(f"GitHub API validation failed: {str(e)}")
            return False

    def list_available_sources(self) -> list[str]:
        """List available GitHub API endpoints.

        Returns:
            List of available endpoint names
        """
        return [
            "issues",
            "pulls",
            "releases",
            "commits",
            "repositories",
            "users",
            "teams",
            "projects",
        ]
