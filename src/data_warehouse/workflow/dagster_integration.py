"""
Dagster Integration for Workflow System.

This module provides integration between the workflow system and Dagster,
enabling workflows to be orchestrated as Dagster assets and ops.
"""

from collections.abc import Callable
from typing import Any

from dagster import (
    AssetExecutionContext,
    AssetIn,
    AssetKey,
    DagsterLogManager,
    InputContext,
    IOManager,
    MetadataValue,
    OpExecutionContext,
    Out,
    OutputContext,
    asset,
    io_manager,
    op,
)
from loguru import logger

from data_warehouse.core.exceptions import WorkflowError
from data_warehouse.workflow.base import WorkflowBase, WorkflowContext, WorkflowRegistry


class WorkflowIOManager(IOManager):
    """IO Manager for workflow data exchange.

    This IO manager handles the exchange of data between workflow steps in Dagster.
    """

    def __init__(self, storage_dir: str = "/tmp/workflow_io") -> None:
        """Initialize the IO manager.

        Args:
            storage_dir: Directory for temporary data storage
        """
        self.storage_dir = storage_dir
        self._cache: dict[str, Any] = {}  # Simple in-memory store for data exchange

    def handle_output(self, context: OutputContext, obj: Any) -> None:
        """Store output data from a workflow step.

        Args:
            context: Dagster output context
            obj: Output data from the workflow step
        """
        # Store the output data in our cache using a unique key
        key = str(context.get_run_scoped_output_identifier())
        self._cache[key] = obj

        # Add metadata to help with debugging and monitoring
        context.add_output_metadata(
            {
                "record_count": MetadataValue.int(len(obj) if isinstance(obj, list | dict) else 1),
                "data_type": MetadataValue.text(str(type(obj))),
            }
        )

    def load_input(self, context: InputContext) -> Any:
        """Load input data for a workflow step.

        Args:
            context: Dagster input context

        Returns:
            Input data for the workflow step
        """
        # Retrieve data from cache using the unique identifier
        key = str(context.get_run_scoped_output_identifier())
        return self._cache.get(key)


@io_manager
def workflow_io_manager() -> WorkflowIOManager:
    """Factory function for WorkflowIOManager.

    Returns:
        A new WorkflowIOManager instance
    """
    return WorkflowIOManager()


class DagsterWorkflowAdapter:
    """Adapter for converting workflows to Dagster assets and ops."""

    def __init__(self, registry: WorkflowRegistry | None = None) -> None:
        """Initialize the adapter.

        Args:
            registry: Optional workflow registry to use. If not provided,
                     the singleton registry will be used.
        """
        self.registry = registry or WorkflowRegistry()

    def workflow_to_asset(
        self,
        workflow_class: type[WorkflowBase],
        config: dict[str, Any] | None = None,
        deps: list[AssetKey] | None = None,
        name_prefix: str = "",
        group_name: str | None = None,
    ) -> Callable:
        """Convert a workflow to a Dagster asset.

        Args:
            workflow_class: The workflow class to convert
            config: Configuration for the workflow
            deps: Dependencies on other assets
            name_prefix: Prefix for the asset name
            group_name: Asset group name

        Returns:
            A Dagster asset function
        """
        # Get workflow ID and extract the class name part
        workflow_id = workflow_class.get_workflow_id()
        class_name = workflow_id.split(".")[-1]

        # Initialize asset dependencies
        ins = {}
        if deps:
            for i, dep in enumerate(deps):
                ins[f"dep_{i}"] = AssetIn(key=dep)

        # Create the asset decorator
        asset_decorator = asset(
            name=f"{name_prefix}{class_name}".lower(),
            group_name=group_name,
            ins=ins,
            io_manager_key="workflow_io_manager",
            compute_kind="workflow",
        )

        # Define the asset function
        @asset_decorator
        def workflow_asset(context: AssetExecutionContext, **inputs: Any) -> dict[str, Any]:
            """Execute the workflow as a Dagster asset.

            Args:
                context: Dagster asset execution context
                inputs: Dependency inputs

            Returns:
                Workflow execution results
            """
            # Configure logging
            _setup_loguru_dagster_bridge(context.log)

            # Create and configure workflow instance
            workflow = workflow_class(config or {})

            # Create workflow context
            workflow_context = WorkflowContext(workflow_id=workflow_id)

            # Add dependency data to workflow context
            for key, value in inputs.items():
                if value is not None:
                    workflow_context.update_data({key: value})

            # Add Dagster context
            workflow_context.update_data({"dagster_context": context})

            try:
                # Execute the workflow
                logger.info(f"Executing workflow as Dagster asset: {workflow_id}")
                result_context = workflow.execute()

                # Return results
                return result_context.data

            except Exception as e:
                logger.error(f"Workflow asset execution failed: {str(e)}")
                context.log.error(str(e))
                raise WorkflowError(f"Error executing workflow as Dagster asset: {str(e)}") from e

        return workflow_asset

    def workflow_to_op(
        self,
        workflow_class: type[WorkflowBase],
        config: dict[str, Any] | None = None,
        name_prefix: str = "",
        required_resource_keys: list[str] | None = None,
    ) -> Callable:
        """Convert a workflow to a Dagster op.

        Args:
            workflow_class: The workflow class to convert
            config: Configuration for the workflow
            name_prefix: Prefix for the op name
            required_resource_keys: Keys for required Dagster resources

        Returns:
            A Dagster op function
        """
        # Get workflow ID and extract the class name part
        workflow_id = workflow_class.get_workflow_id()
        class_name = workflow_id.split(".")[-1]

        # Create the op decorator
        op_decorator = op(
            name=f"{name_prefix}{class_name}".lower(),
            required_resource_keys=set(required_resource_keys or []),
            out={"result": Out(dict[str, Any])},
        )

        # Define the op function
        @op_decorator
        def workflow_op(context: OpExecutionContext) -> dict[str, Any]:
            """Execute the workflow as a Dagster op.

            Args:
                context: Dagster op execution context

            Returns:
                Workflow execution results
            """
            # Configure logging
            _setup_loguru_dagster_bridge(context.log)

            # Create and configure workflow instance
            workflow = workflow_class(config or {})

            # Create workflow context
            workflow_context = WorkflowContext(workflow_id=workflow_id)

            # Add Dagster context and resources
            workflow_context.update_data(
                {
                    "dagster_context": context,
                    "dagster_resources": context.resources,
                }
            )

            try:
                # Execute the workflow
                logger.info(f"Executing workflow as Dagster op: {workflow_id}")
                result_context = workflow.execute()

                # Return results
                return result_context.data

            except Exception as e:
                logger.error(f"Workflow op execution failed: {str(e)}")
                context.log.error(str(e))
                raise WorkflowError(f"Error executing workflow as Dagster op: {str(e)}") from e

        return workflow_op

    def convert_all_workflows_to_assets(
        self,
        domain: str | None = None,
        config_factory: Callable[[type[WorkflowBase]], dict[str, Any]] | None = None,
        name_prefix: str = "",
        group_by_domain: bool = True,
    ) -> dict[str, Callable]:
        """Convert all registered workflows to Dagster assets.

        Args:
            domain: Optional domain filter for workflows
            config_factory: Optional function to generate configs for workflows
            name_prefix: Prefix for asset names
            group_by_domain: Whether to group assets by domain

        Returns:
            Dictionary of asset names to asset functions
        """
        assets = {}

        # Get workflows to convert
        if domain:
            workflows = self.registry.get_workflows_by_domain(domain)
        else:
            workflows = list(self.registry.get_all_workflows().values())

        # Convert each workflow to an asset
        for workflow_class in workflows:
            # Get workflow ID
            workflow_id = workflow_class.get_workflow_id()

            # Get config from factory if provided
            config = None
            if config_factory:
                config = config_factory(workflow_class)

            # Get domain for grouping
            workflow_domain = domain
            if group_by_domain and not workflow_domain:
                # Extract domain from module path
                parts = workflow_id.split(".")
                workflow_domain = parts[-2] if len(parts) > 2 else "default"

            # Convert to asset
            asset_fn = self.workflow_to_asset(
                workflow_class,
                config=config,
                name_prefix=name_prefix,
                group_name=workflow_domain if group_by_domain else None,
            )

            # Store in assets dict
            class_name = workflow_id.split(".")[-1]
            asset_name = f"{name_prefix}{class_name}".lower()
            assets[asset_name] = asset_fn

        return assets


def _setup_loguru_dagster_bridge(dagster_logger: DagsterLogManager) -> None:
    """Set up bridge from loguru to Dagster logging.

    Args:
        dagster_logger: Dagster logger to bridge to
    """

    class DagsterLoggerHandler:
        """Handler that redirects loguru logs to Dagster."""

        def write(self, message: str) -> None:
            """Write a log message to Dagster.

            Args:
                message: Log message
            """
            dagster_logger.info(message.rstrip())

    # Only add the Dagster handler if it's not already present
    # This preserves existing sinks and prevents duplicate handlers
    if not any(isinstance(handler.sink, DagsterLoggerHandler) for handler in logger._core.handlers.values()):
        logger.add(DagsterLoggerHandler(), format="{message}")
