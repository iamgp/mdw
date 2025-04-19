"""
Nightscout Workflow Integration.

This module provides the main workflow class for Nightscout data processing.
"""

from typing import Any

from loguru import logger

from data_warehouse.core.exceptions import WorkflowError
from data_warehouse.workflow.base import WorkflowBase, WorkflowContext
from data_warehouse.workflow.examples.nightscout.extraction import NightscoutExtractor
from data_warehouse.workflow.examples.nightscout.load import NightscoutLoader
from data_warehouse.workflow.examples.nightscout.transform import NightscoutTransformer


class NightscoutWorkflow(WorkflowBase):
    """Workflow for processing Nightscout data.

    This workflow extracts data from a Nightscout API instance, transforms it into a
    standardized format, and loads it into the data warehouse.
    """

    def __init__(self, config: dict[str, Any] | None = None) -> None:
        """Initialize the workflow with configuration.

        Args:
            config: Configuration dictionary containing settings for all workflow components
        """
        super().__init__(config)

        # Create component configurations from the main config
        # Use empty dictionaries as defaults to prevent None values
        extractor_config = self.config.get("extractor", {})
        transformer_config = self.config.get("transformer", {})
        loader_config = self.config.get("loader", {})

        # Initialize components
        self.extractor = NightscoutExtractor(extractor_config)
        self.transformer = NightscoutTransformer(transformer_config)
        self.loader = NightscoutLoader(loader_config)

    def extract(self, context: WorkflowContext) -> WorkflowContext:
        """Extract data from Nightscout API.

        Args:
            context: Workflow context

        Returns:
            Updated context with extracted data

        Raises:
            WorkflowError: If extraction fails
        """
        try:
            logger.info("Starting Nightscout data extraction")
            extracted_data = self.extractor.extract(context)
            context.update_data({"extracted_data": extracted_data})
            logger.info("Nightscout data extraction completed successfully")
            return context
        except Exception as e:
            logger.error(f"Nightscout data extraction failed: {str(e)}")
            raise WorkflowError(f"Nightscout data extraction failed: {str(e)}") from e

    def transform(self, context: WorkflowContext) -> WorkflowContext:
        """Transform Nightscout data.

        Args:
            context: Workflow context with extracted data

        Returns:
            Updated context with transformed data

        Raises:
            WorkflowError: If transformation fails
        """
        try:
            logger.info("Starting Nightscout data transformation")
            extracted_data = context.get_data("extracted_data")
            if not extracted_data:
                logger.warning("No extracted data found in context, skipping transformation")
                # Update context even when no data is available
                context.update_data({"transformed_data": {}})
                return context

            transformed_data = self.transformer.transform(extracted_data, context)
            context.update_data({"transformed_data": transformed_data})
            logger.info("Nightscout data transformation completed successfully")
            return context
        except Exception as e:
            logger.error(f"Nightscout data transformation failed: {str(e)}")
            raise WorkflowError(f"Nightscout data transformation failed: {str(e)}") from e

    def load(self, context: WorkflowContext) -> WorkflowContext:
        """Load transformed Nightscout data into the data warehouse.

        Args:
            context: Workflow context with transformed data

        Returns:
            Updated context with loading results

        Raises:
            WorkflowError: If loading fails
        """
        try:
            logger.info("Starting Nightscout data loading")
            transformed_data = context.get_data("transformed_data")
            if not transformed_data:
                logger.warning("No transformed data found in context, skipping loading")
                # Always update context even when no data is loaded
                context.update_data({"records_loaded": 0})
                return context

            records_loaded = self.loader.load(transformed_data, context)
            context.update_data({"records_loaded": records_loaded})
            logger.info(f"Nightscout data loading completed successfully: {records_loaded} records loaded")
            return context
        except Exception as e:
            logger.error(f"Nightscout data loading failed: {str(e)}")
            raise WorkflowError(f"Nightscout data loading failed: {str(e)}") from e

    def _validate_config(self) -> None:
        """Validate the workflow configuration.

        Raises:
            WorkflowError: If configuration is invalid
        """
        required_sections = ["extractor", "loader"]
        for section in required_sections:
            if section not in self.config:
                raise WorkflowError(f"Missing required configuration section: {section}")

        # Validate extractor config
        extractor_config = self.config.get("extractor", {})
        if not extractor_config.get("source_name"):
            raise WorkflowError("Missing required extractor parameter: source_name")

        # Validate source URL
        if not extractor_config.get("credentials", {}).get("api_url"):
            raise WorkflowError("Missing required extractor credential: api_url")

        # Validate loader config
        loader_config = self.config.get("loader", {})
        if not loader_config.get("target_name"):
            raise WorkflowError("Missing required loader parameter: target_name")

        # Validate connection string
        if not loader_config.get("credentials", {}).get("connection_string"):
            raise WorkflowError("Missing required loader credential: connection_string")
