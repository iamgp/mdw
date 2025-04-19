"""Base workflow template for data warehouse workflows."""

from abc import ABC, abstractmethod
from typing import Any

from dagster import OpExecutionContext, asset
from loguru import logger
from pydantic import BaseModel


class BaseWorkflowConfig(BaseModel):
    """Base configuration for all workflows."""

    name: str
    description: str
    version: str = "0.1.0"
    enabled: bool = True
    schedule_interval: str | None = None
    tags: list[str] = []
    metadata: dict[str, Any] = {}


class BaseWorkflow(ABC):
    """Base class for all data warehouse workflows."""

    def __init__(self, config: BaseWorkflowConfig) -> None:
        """Initialize the workflow with configuration."""
        self.config = config
        self.logger = logger.bind(workflow=config.name)

    @abstractmethod
    async def extract(self, context: OpExecutionContext) -> dict[str, Any]:
        """Extract data from source."""
        pass

    @abstractmethod
    async def transform(
        self, context: OpExecutionContext, data: dict[str, Any]
    ) -> dict[str, Any]:
        """Transform extracted data."""
        pass

    @abstractmethod
    async def load(self, context: OpExecutionContext, data: dict[str, Any]) -> None:
        """Load transformed data into warehouse."""
        pass

    @asset
    async def run(self, context: OpExecutionContext) -> None:
        """Execute the complete workflow."""
        try:
            self.logger.info(f"Starting workflow: {self.config.name}")

            # Extract
            raw_data = await self.extract(context)
            context.log.info(f"Extracted data: {len(raw_data)} records")

            # Transform
            transformed_data = await self.transform(context, raw_data)
            context.log.info(f"Transformed data: {len(transformed_data)} records")

            # Load
            await self.load(context, transformed_data)
            context.log.info("Data loaded successfully")

            self.logger.info(f"Workflow completed: {self.config.name}")
        except Exception as e:
            self.logger.error(f"Workflow failed: {e!s}")
            raise
