"""
Core Workflow System Base Classes.

This module defines the foundational classes for the workflow system, including:
- WorkflowBase: Abstract base class for all workflows
- WorkflowRegistry: Registry for workflow discovery and management
- WorkflowExecutor: Engine for executing workflows
- WorkflowValidator: Validates workflow configurations
- WorkflowMonitor: Tracks and reports on workflow execution
"""

from __future__ import annotations

import abc
import inspect
import pkgutil
from dataclasses import dataclass, field
from enum import Enum
from importlib import import_module
from typing import Any, TypeVar

from loguru import logger
from pydantic import BaseModel, Field, validator

from data_warehouse.core.exceptions import WorkflowError, WorkflowNotFoundError

# Type definitions
T = TypeVar("T", bound="WorkflowBase")
ConfigType = dict[str, Any]


class WorkflowStatus(str, Enum):
    """Workflow execution status."""

    PENDING = "pending"
    RUNNING = "running"
    COMPLETED = "completed"
    FAILED = "failed"
    CANCELLED = "cancelled"


class WorkflowConfig(BaseModel):
    """Configuration for a workflow."""

    name: str = Field(..., description="Unique name of the workflow")
    version: str = Field("0.1.0", description="Version of the workflow")
    description: str = Field("", description="Description of the workflow")
    domain: str = Field(..., description="Business domain this workflow belongs to")
    enabled: bool = Field(True, description="Whether this workflow is enabled")

    @validator("name")
    def name_must_be_valid(cls, v: str) -> str:
        """Validate that the workflow name is valid."""
        if not v or not v.strip():
            raise ValueError("Workflow name cannot be empty")
        if " " in v:
            raise ValueError("Workflow name cannot contain spaces")
        return v.lower()


@dataclass
class WorkflowContext:
    """Context object passed between workflow steps."""

    workflow_id: str
    config: dict[str, Any] = field(default_factory=dict)
    data: dict[str, Any] = field(default_factory=dict)
    metadata: dict[str, Any] = field(default_factory=dict)

    def update_data(self, data: dict[str, Any]) -> None:
        """Update the data dictionary with new data."""
        self.data.update(data)

    def get_data(self, key: str, default: Any = None) -> Any:
        """Get data by key with optional default value."""
        return self.data.get(key, default)


class WorkflowBase(abc.ABC):
    """Abstract base class for all workflows."""

    def __init__(self, config: dict[str, Any] | None = None) -> None:
        """Initialize the workflow with optional configuration."""
        self.config = config or {}
        self._validate_config()
        self.status = WorkflowStatus.PENDING
        self.context = WorkflowContext(workflow_id=self.get_workflow_id())

    @classmethod
    def get_workflow_id(cls) -> str:
        """Get the unique identifier for this workflow."""
        return f"{cls.__module__}.{cls.__name__}"

    @abc.abstractmethod
    def extract(self, context: WorkflowContext) -> WorkflowContext:
        """Extract data from source systems."""
        pass

    @abc.abstractmethod
    def transform(self, context: WorkflowContext) -> WorkflowContext:
        """Transform extracted data."""
        pass

    @abc.abstractmethod
    def load(self, context: WorkflowContext) -> WorkflowContext:
        """Load transformed data into target systems."""
        pass

    def execute(self) -> WorkflowContext:
        """Execute the full workflow."""
        try:
            self.status = WorkflowStatus.RUNNING
            logger.info(f"Starting workflow: {self.get_workflow_id()}")

            # Execute the ETL steps
            self.context = self.extract(self.context)
            self.context = self.transform(self.context)
            self.context = self.load(self.context)

            self.status = WorkflowStatus.COMPLETED
            logger.info(f"Workflow completed: {self.get_workflow_id()}")
            return self.context

        except Exception as e:
            self.status = WorkflowStatus.FAILED
            logger.error(f"Workflow failed: {self.get_workflow_id()}, Error: {str(e)}")
            raise WorkflowError(f"Error executing workflow: {str(e)}") from e

    @abc.abstractmethod
    def _validate_config(self) -> None:
        """Validate the workflow configuration."""
        # Override in subclasses to implement specific validation
        pass


class WorkflowRegistry:
    """Central registry for workflow discovery and management."""

    _instance = None
    _workflows: dict[str, type[WorkflowBase]] = {}
    _domains: dict[str, set[str]] = {}

    def __new__(cls) -> WorkflowRegistry:
        """Ensure registry is a singleton."""
        if cls._instance is None:
            cls._instance = super().__new__(cls)
        return cls._instance

    def register(self, workflow_class: type[WorkflowBase], domain: str = "default") -> None:
        """Register a workflow class."""
        workflow_id = workflow_class.get_workflow_id()
        self._workflows[workflow_id] = workflow_class

        # Register with domain
        if domain not in self._domains:
            self._domains[domain] = set()
        self._domains[domain].add(workflow_id)

        logger.debug(f"Registered workflow: {workflow_id} in domain: {domain}")

    def unregister(self, workflow_id: str) -> None:
        """Unregister a workflow by its ID."""
        if workflow_id in self._workflows:
            self._workflows[workflow_id]
            del self._workflows[workflow_id]

            # Clean up domain registrations
            for _domain, workflows in self._domains.items():
                if workflow_id in workflows:
                    workflows.remove(workflow_id)

            logger.debug(f"Unregistered workflow: {workflow_id}")

    def get_workflow(self, workflow_id: str) -> type[WorkflowBase]:
        """Get a workflow class by its ID."""
        if workflow_id not in self._workflows:
            raise WorkflowNotFoundError(f"Workflow not found: {workflow_id}")
        return self._workflows[workflow_id]

    def get_workflows_by_domain(self, domain: str) -> list[type[WorkflowBase]]:
        """Get all workflow classes for a specific domain."""
        if domain not in self._domains:
            return []

        return [self._workflows[workflow_id] for workflow_id in self._domains[domain]]

    def get_all_workflows(self) -> dict[str, type[WorkflowBase]]:
        """Get all registered workflows."""
        return self._workflows.copy()

    def get_all_domains(self) -> list[str]:
        """Get all registered domains."""
        return list(self._domains.keys())

    def discover_workflows(self, package_name: str = "data_warehouse.workflow") -> None:
        """Discover and register all workflow implementations in a package."""
        logger.info(f"Discovering workflows in package: {package_name}")

        try:
            package = import_module(package_name)
            package_path = getattr(package, "__path__", [])

            for _, module_name, is_pkg in pkgutil.iter_modules(package_path):
                full_module_name = f"{package_name}.{module_name}"

                # Skip internal modules
                if module_name.startswith("_"):
                    continue

                # Recursively process subpackages
                if is_pkg:
                    self.discover_workflows(full_module_name)
                    continue

                # Import the module and search for workflow classes
                try:
                    module = import_module(full_module_name)
                    for _name, obj in inspect.getmembers(module):
                        # Check if it's a class that inherits from WorkflowBase
                        if inspect.isclass(obj) and issubclass(obj, WorkflowBase) and obj != WorkflowBase:
                            # Extract domain from module path
                            domain_parts = full_module_name.split(".")
                            domain = domain_parts[-2] if len(domain_parts) > 2 else "default"

                            # Register the workflow
                            self.register(obj, domain=domain)

                except Exception as e:
                    logger.warning(f"Error importing module {full_module_name}: {str(e)}")

        except Exception as e:
            logger.error(f"Error discovering workflows: {str(e)}")


class WorkflowExecutor:
    """Handles execution of workflows."""

    def __init__(self, registry: WorkflowRegistry | None = None) -> None:
        """Initialize with an optional registry."""
        self.registry = registry or WorkflowRegistry()

    def execute_workflow(self, workflow_id: str, config: dict[str, Any] | None = None) -> WorkflowContext:
        """Execute a workflow by its ID."""
        workflow_class = self.registry.get_workflow(workflow_id)
        workflow = workflow_class(config)
        return workflow.execute()

    def execute_domain_workflows(self, domain: str, config: dict[str, Any] | None = None) -> dict[str, WorkflowContext]:
        """Execute all workflows in a domain."""
        results = {}
        workflows = self.registry.get_workflows_by_domain(domain)

        for workflow_class in workflows:
            workflow_id = workflow_class.get_workflow_id()
            try:
                workflow = workflow_class(config)
                results[workflow_id] = workflow.execute()
            except Exception as e:
                logger.error(f"Error executing workflow {workflow_id}: {str(e)}")
                results[workflow_id] = None

        return results


class WorkflowValidator:
    """Validates workflow definitions and configurations."""

    @staticmethod
    def validate_workflow_class(workflow_class: type[WorkflowBase]) -> bool:
        """Validate that a workflow class meets all requirements."""
        # Check that it's a proper subclass
        if not inspect.isclass(workflow_class) or not issubclass(workflow_class, WorkflowBase):
            logger.error(f"{workflow_class} is not a valid WorkflowBase subclass")
            return False

        # Check that required methods are implemented
        required_methods = ["extract", "transform", "load"]
        for method_name in required_methods:
            method = getattr(workflow_class, method_name, None)
            if not method or method.__func__ is getattr(WorkflowBase, method_name):
                logger.error(f"{workflow_class.__name__} must implement {method_name}")
                return False

        return True

    @staticmethod
    def validate_workflow_config(config: dict[str, Any]) -> bool:
        """Validate a workflow configuration."""
        try:
            WorkflowConfig(**config)
            return True
        except Exception as e:
            logger.error(f"Invalid workflow configuration: {str(e)}")
            return False


class WorkflowMonitor:
    """Tracks and reports on workflow execution."""

    def __init__(self) -> None:
        """Initialize the workflow monitor."""
        self.workflows: dict[str, dict[str, Any]] = {}

    def register_workflow_execution(
        self, workflow_id: str, status: WorkflowStatus, metadata: dict[str, Any] | None = None
    ) -> None:
        """Register a workflow execution event."""
        if workflow_id not in self.workflows:
            self.workflows[workflow_id] = {
                "executions": [],
                "last_status": None,
            }

        execution = {
            "timestamp": import_module("datetime").datetime.now().isoformat(),
            "status": status,
            "metadata": metadata or {},
        }

        self.workflows[workflow_id]["executions"].append(execution)
        self.workflows[workflow_id]["last_status"] = status

        logger.info(f"Workflow {workflow_id} status: {status}")

    def get_workflow_status(self, workflow_id: str) -> WorkflowStatus | None:
        """Get the last status of a workflow."""
        if workflow_id not in self.workflows:
            return None
        return self.workflows[workflow_id]["last_status"]

    def get_workflow_history(self, workflow_id: str) -> list[dict[str, Any]]:
        """Get the execution history of a workflow."""
        if workflow_id not in self.workflows:
            return []
        return self.workflows[workflow_id]["executions"]
