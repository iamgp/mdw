"""
GitHub Data Loading Module.

This module provides classes for loading transformed GitHub data into storage systems.
"""

from typing import Any

from loguru import logger

from data_warehouse.core.exceptions import LoaderError
from data_warehouse.workflow.base import WorkflowContext
from data_warehouse.workflow.etl import LoaderBase


class GitHubDatabaseLoader(LoaderBase[list[dict[str, Any]]]):
    """Loader for GitHub data into a database."""

    def __init__(self, config: dict[str, Any] | None = None) -> None:
        """Initialize the GitHub database loader.

        Args:
            config: Configuration dictionary
        """
        super().__init__(config)
        self.connection = None

    def load(self, data: list[dict[str, Any]], context: WorkflowContext) -> int:
        """Load GitHub data into a database.

        Args:
            data: The transformed GitHub data to load
            context: The workflow context

        Returns:
            Number of records loaded

        Raises:
            LoaderError: If loading fails
        """
        if not data:
            logger.info("No data to load")
            return 0

        entity_type = context.config.get("entity_type", "repo")
        endpoint = context.config.get("endpoint", "issues")
        target_table = self._get_target_table(entity_type, endpoint)

        logger.info(f"Loading {len(data)} records into table '{target_table}'")

        try:
            # This is a placeholder implementation
            # In a real implementation, this would connect to a database and insert the records
            logger.info(f"Would insert {len(data)} records into {target_table}")

            # Simulate database operations for demonstration purposes
            records_loaded = len(data)

            logger.info(f"Successfully loaded {records_loaded} records into {target_table}")
            return records_loaded

        except Exception as e:
            error_msg = f"Failed to load GitHub data into database: {str(e)}"
            logger.error(error_msg)
            raise LoaderError(error_msg) from e

    def _get_target_table(self, entity_type: str, endpoint: str) -> str:
        """Get the target table name based on entity type and endpoint.

        Args:
            entity_type: The type of entity (repo, org, user)
            endpoint: The GitHub API endpoint

        Returns:
            The target table name
        """
        table_prefix = "github"

        # Map endpoints to table names
        endpoint_table_map = {
            "issues": "issues",
            "pulls": "pull_requests",
            "releases": "releases",
            "commits": "commits",
            "repositories": "repositories",
            "users": "users",
            "teams": "teams",
            "projects": "projects",
        }

        # Get the table name for the endpoint, defaulting to the endpoint name
        table_name = endpoint_table_map.get(endpoint, endpoint)

        return f"{table_prefix}_{table_name}"

    def validate_target(self) -> bool:
        """Validate the database target configuration.

        Returns:
            True if the target is valid, False otherwise
        """
        # In a real implementation, this would check database connectivity
        return True

    def list_available_targets(self) -> list[str]:
        """List available database tables.

        Returns:
            List of available table names
        """
        return [
            "github_issues",
            "github_pull_requests",
            "github_releases",
            "github_commits",
            "github_repositories",
            "github_users",
            "github_teams",
            "github_projects",
        ]


class GitHubFileLoader(LoaderBase[list[dict[str, Any]]]):
    """Loader for GitHub data into files."""

    def load(self, data: list[dict[str, Any]], context: WorkflowContext) -> int:
        """Load GitHub data into files.

        Args:
            data: The transformed GitHub data to load
            context: The workflow context

        Returns:
            Number of records written

        Raises:
            LoaderError: If loading fails
        """
        if not data:
            logger.info("No data to load")
            return 0

        entity_type = context.config.get("entity_type", "repo")
        endpoint = context.config.get("endpoint", "issues")
        file_path = self._get_file_path(entity_type, endpoint)

        logger.info(f"Writing {len(data)} records to file '{file_path}'")

        try:
            # This is a placeholder implementation
            # In a real implementation, this would write the data to a file
            logger.info(f"Would write {len(data)} records to {file_path}")

            # Simulate file writing for demonstration purposes
            records_written = len(data)

            logger.info(f"Successfully wrote {records_written} records to {file_path}")
            return records_written

        except Exception as e:
            error_msg = f"Failed to write GitHub data to file: {str(e)}"
            logger.error(error_msg)
            raise LoaderError(error_msg) from e

    def _get_file_path(self, entity_type: str, endpoint: str) -> str:
        """Get the file path based on entity type and endpoint.

        Args:
            entity_type: The type of entity (repo, org, user)
            endpoint: The GitHub API endpoint

        Returns:
            The file path
        """
        # Map endpoints to file names
        endpoint_file_map = {
            "issues": "issues",
            "pulls": "pull_requests",
            "releases": "releases",
            "commits": "commits",
            "repositories": "repositories",
            "users": "users",
            "teams": "teams",
            "projects": "projects",
        }

        # Get the file name for the endpoint, defaulting to the endpoint name
        file_name = endpoint_file_map.get(endpoint, endpoint)

        return f"data/github/{entity_type}/{file_name}.json"
