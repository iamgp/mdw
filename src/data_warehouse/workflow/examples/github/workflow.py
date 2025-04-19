"""
GitHub Workflow Implementation.

This module provides a comprehensive workflow for GitHub data extraction,
transformation, and loading.
"""

from typing import Any

from loguru import logger

from data_warehouse.workflow.base import WorkflowBase, WorkflowContext
from data_warehouse.workflow.examples.github.extraction import GitHubExtractor
from data_warehouse.workflow.examples.github.load import GitHubDatabaseLoader, GitHubFileLoader
from data_warehouse.workflow.examples.github.transform import GitHubTransformer


class GitHubWorkflow(WorkflowBase):
    """Workflow for processing GitHub data.

    This workflow extracts data from the GitHub API, transforms it into a
    standardized format, and loads it into a database or file.
    """

    def __init__(self, config: dict[str, Any] | None = None) -> None:
        """Initialize the GitHub workflow.

        Args:
            config: Configuration dictionary
        """
        super().__init__(config)

        # Set up default configuration
        if not self.config.get("entity_type"):
            self.config["entity_type"] = "repo"
        if not self.config.get("entity_name"):
            self.config["entity_name"] = "octocat/hello-world"
        if not self.config.get("endpoint"):
            self.config["endpoint"] = "issues"

        # Initialize ETL components
        self.extractor = GitHubExtractor(config)
        self.transformer = GitHubTransformer(config)

        # Determine the loader type based on config
        loader_type = self.config.get("loader_type", "database")
        if loader_type == "file":
            self.loader = GitHubFileLoader(config)
        else:
            self.loader = GitHubDatabaseLoader(config)

        logger.info(
            f"Initialized GitHub workflow for {self.config['entity_type']} "
            f"'{self.config['entity_name']}', endpoint '{self.config['endpoint']}'"
        )

    def extract(self, context: WorkflowContext) -> WorkflowContext:
        """Extract data from the GitHub API.

        Args:
            context: The workflow context

        Returns:
            Updated workflow context with extracted data
        """
        logger.info("Starting GitHub data extraction")

        # Copy configuration to context for use by the extractor
        if not context.config:
            context.config = {}
        context.config.update(self.config)

        # Extract data from GitHub API
        extracted_data = self.extractor.extract(context)

        # Update the context with the extracted data
        context.update_data({"extracted_data": extracted_data})

        logger.info(f"Extracted {len(extracted_data)} records from GitHub API")
        return context

    def transform(self, context: WorkflowContext) -> WorkflowContext:
        """Transform the extracted GitHub data.

        Args:
            context: The workflow context with extracted data

        Returns:
            Updated workflow context with transformed data
        """
        logger.info("Starting GitHub data transformation")

        # Get the extracted data from the context
        extracted_data = context.get_data("extracted_data")

        if not extracted_data:
            logger.warning("No data to transform")
            context.update_data({"transformed_data": []})
            return context

        # Transform the data
        transformed_data = self.transformer.transform(extracted_data, context)

        # Update the context with the transformed data
        context.update_data({"transformed_data": transformed_data})

        logger.info(f"Transformed {len(transformed_data)} GitHub records")
        return context

    def load(self, context: WorkflowContext) -> WorkflowContext:
        """Load the transformed GitHub data.

        Args:
            context: The workflow context with transformed data

        Returns:
            Updated workflow context with loading results
        """
        logger.info("Starting GitHub data loading")

        # Get the transformed data from the context
        transformed_data = context.get_data("transformed_data")

        if not transformed_data:
            logger.warning("No data to load")
            context.update_data({"records_loaded": 0})
            return context

        # Load the data
        records_loaded = self.loader.load(transformed_data, context)

        # Update the context with the loading results
        context.update_data(
            {"records_loaded": records_loaded, "loader_type": self.config.get("loader_type", "database")}
        )

        logger.info(f"Loaded {records_loaded} GitHub records")
        return context

    def _validate_config(self) -> None:
        """Validate the workflow configuration.

        Raises:
            WorkflowError: If the configuration is invalid
        """
        required_fields = ["entity_type", "entity_name", "endpoint"]
        missing_fields = []

        for field in required_fields:
            if field not in self.config:
                missing_fields.append(field)
                # Set default values that will be used in __init__
                self.config[field] = None

        # Report all missing fields at once with clear error message
        if missing_fields:
            logger.warning(f"Missing required configuration fields: {', '.join(missing_fields)}. Using defaults.")

        # Validate credential presence if token is required
        if not self.config.get("credentials", {}).get("token"):
            logger.warning("No GitHub API token provided in credentials. Rate limits may apply.")

        # Validate loader configuration
        loader_type = self.config.get("loader_type", "database")
        if loader_type not in ["database", "file"]:
            logger.warning(f"Invalid loader_type '{loader_type}'. Using 'database' as default.")
            self.config["loader_type"] = "database"
