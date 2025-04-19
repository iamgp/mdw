"""
Workflow Template.

Use this template as a starting point for creating new workflows.
Replace the placeholders with your implementation.
"""

from typing import Any

from loguru import logger

from data_warehouse.workflow.base import WorkflowBase, WorkflowContext
from data_warehouse.workflow.etl import ExtractorBase, LoaderBase, TransformerBase


class MyWorkflowExtractor(ExtractorBase[list[dict[str, Any]]]):
    """Data extractor for MyWorkflow."""

    def extract(self, context: WorkflowContext) -> list[dict[str, Any]]:
        """Extract data from the source.

        Args:
            context: The workflow context

        Returns:
            List of extracted data records
        """
        logger.info("Extracting data")

        # Implementation goes here
        # Example:
        # source_name = self.config.source_name
        # records = api_client.fetch_data(source_name)
        # return records

        # Placeholder implementation
        return [{"id": 1, "name": "Example"}]


class MyWorkflowTransformer(TransformerBase[list[dict[str, Any]], list[dict[str, Any]]]):
    """Data transformer for MyWorkflow."""

    def transform(self, data: list[dict[str, Any]], context: WorkflowContext) -> list[dict[str, Any]]:
        """Transform the extracted data.

        Args:
            data: The data to transform
            context: The workflow context

        Returns:
            Transformed data
        """
        logger.info(f"Transforming {len(data)} records")

        # Implementation goes here
        # Example:
        # transformed_data = []
        # for record in data:
        #     transformed_record = self._transform_record(record)
        #     transformed_data.append(transformed_record)
        # return transformed_data

        # Placeholder implementation
        return [{"id": record["id"], "name": record["name"].upper()} for record in data]


class MyWorkflowLoader(LoaderBase[list[dict[str, Any]]]):
    """Data loader for MyWorkflow."""

    def load(self, data: list[dict[str, Any]], context: WorkflowContext) -> int:
        """Load data into the target system.

        Args:
            data: The data to load
            context: The workflow context

        Returns:
            Number of records loaded
        """
        logger.info(f"Loading {len(data)} records")

        # Implementation goes here
        # Example:
        # target_name = self.config.target_name
        # return database.insert_records(target_name, data)

        # Placeholder implementation
        logger.info(f"Would load {len(data)} records to {self.config.target_name}")
        return len(data)


class MyWorkflow(WorkflowBase):
    """Example workflow implementation.

    Replace this with your workflow implementation.
    """

    def __init__(self, config: dict[str, Any] | None = None) -> None:
        """Initialize the workflow with configuration."""
        super().__init__(config)

        # Initialize ETL components
        self.extractor = MyWorkflowExtractor(config)
        self.transformer = MyWorkflowTransformer(config)
        self.loader = MyWorkflowLoader(config)

    def extract(self, context: WorkflowContext) -> WorkflowContext:
        """Extract data from source systems.

        Args:
            context: The workflow context

        Returns:
            Updated workflow context with extracted data
        """
        extracted_data = self.extractor.extract(context)
        context.update_data({"extracted_data": extracted_data})
        return context

    def transform(self, context: WorkflowContext) -> WorkflowContext:
        """Transform extracted data.

        Args:
            context: The workflow context with extracted data

        Returns:
            Updated workflow context with transformed data
        """
        extracted_data = context.get_data("extracted_data")
        transformed_data = self.transformer.transform(extracted_data, context)
        context.update_data({"transformed_data": transformed_data})
        return context

    def load(self, context: WorkflowContext) -> WorkflowContext:
        """Load transformed data into target systems.

        Args:
            context: The workflow context with transformed data

        Returns:
            Updated workflow context with loading results
        """
        transformed_data = context.get_data("transformed_data")
        records_loaded = self.loader.load(transformed_data, context)
        context.update_data({"records_loaded": records_loaded})
        return context
