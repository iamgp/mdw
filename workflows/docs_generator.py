"""
Documentation generator for the data warehouse workflow system.

This module provides functionality to generate Markdown documentation
for workflow components and pipelines.
"""

import logging
import os
from typing import Any

from workflows.base import BaseExtractor, BaseLoader, BaseTransformer, Pipeline
from workflows.workflow_manager import WorkflowManager

logger = logging.getLogger(__name__)


class DocsGenerator:
    """
    A class for generating workflow documentation in Markdown format.

    This generator creates documentation for extractors, transformers, loaders,
    and pipelines based on their structure and metadata.
    """

    def __init__(self, output_dir: str = "docs/workflows") -> None:
        """
        Initialize a DocsGenerator instance.

        Args:
            output_dir: Directory where documentation files will be saved
        """
        self.output_dir = output_dir
        self.workflow_manager = WorkflowManager()

    def _ensure_output_dir(self) -> None:
        """
        Ensure the output directory exists.

        Creates the output directory and any necessary parent directories.
        """
        if not os.path.exists(self.output_dir):
            os.makedirs(self.output_dir, exist_ok=True)
            logger.info(f"Created documentation directory: {self.output_dir}")

    def _generate_component_doc(self, name: str, component: Any, component_type: str) -> str:
        """
        Generate Markdown documentation for a workflow component.

        Args:
            name: The component name
            component: The component instance
            component_type: The type of component (extractor, transformer, loader)

        Returns:
            Markdown documentation as a string
        """
        doc = f"# {name} ({component_type.capitalize()})\n\n"

        # Add description if available
        class_doc = component.__class__.__doc__
        if class_doc:
            doc += f"{class_doc.strip()}\n\n"

        # Add class info
        doc += f"**Class:** `{component.__class__.__name__}`\n\n"

        # Add configuration info if available
        if hasattr(component, "config") and component.config:
            doc += "## Configuration\n\n"
            doc += "```python\n"
            doc += f"{component.config}\n"
            doc += "```\n\n"

        # Add metadata if available
        if hasattr(component, "get_metadata") and callable(component.get_metadata):
            metadata = component.get_metadata()
            if metadata:
                doc += "## Metadata\n\n"
                doc += "| Key | Value |\n"
                doc += "| --- | ----- |\n"
                for key, value in metadata.items():
                    doc += f"| {key} | {value} |\n"
                doc += "\n"

        # Add component-specific information
        if component_type == "extractor":
            doc += self._generate_extractor_doc(component)
        elif component_type == "transformer":
            doc += self._generate_transformer_doc(component)
        elif component_type == "loader":
            doc += self._generate_loader_doc(component)

        return doc

    def _generate_extractor_doc(self, extractor: BaseExtractor[Any]) -> str:
        """
        Generate Markdown documentation specific to extractors.

        Args:
            extractor: The extractor instance

        Returns:
            Markdown documentation as a string
        """
        doc = "## Extractor Details\n\n"

        # Add source information
        if hasattr(extractor, "source"):
            doc += f"**Source:** `{extractor.source}`\n\n"

        # Add output format if available
        if hasattr(extractor, "output_format"):
            doc += f"**Output Format:** `{extractor.output_format}`\n\n"

        return doc

    def _generate_transformer_doc(self, transformer: BaseTransformer[Any, Any]) -> str:
        """
        Generate Markdown documentation specific to transformers.

        Args:
            transformer: The transformer instance

        Returns:
            Markdown documentation as a string
        """
        doc = "## Transformer Details\n\n"

        # Add input/output format information
        if hasattr(transformer, "accepts_formats"):
            formats = ", ".join(f"`{fmt}`" for fmt in transformer.accepts_formats)
            doc += f"**Accepts Formats:** {formats}\n\n"

        if hasattr(transformer, "output_format"):
            doc += f"**Output Format:** `{transformer.output_format}`\n\n"

        return doc

    def _generate_loader_doc(self, loader: BaseLoader[Any]) -> str:
        """
        Generate Markdown documentation specific to loaders.

        Args:
            loader: The loader instance

        Returns:
            Markdown documentation as a string
        """
        doc = "## Loader Details\n\n"

        # Add destination information if available
        if hasattr(loader, "destination"):
            doc += f"**Destination:** `{loader.destination}`\n\n"

        # Add accepted formats if available
        if hasattr(loader, "accepts_formats"):
            formats = ", ".join(f"`{fmt}`" for fmt in loader.accepts_formats)
            doc += f"**Accepts Formats:** {formats}\n\n"

        return doc

    def _generate_pipeline_doc(self, name: str, pipeline: Pipeline) -> str:
        """
        Generate Markdown documentation for a pipeline.

        Args:
            name: The pipeline name
            pipeline: The pipeline instance

        Returns:
            Markdown documentation as a string
        """
        doc = f"# {name} (Pipeline)\n\n"

        # Add description
        if hasattr(pipeline, "__doc__") and pipeline.__doc__:
            doc += f"{pipeline.__doc__.strip()}\n\n"

        # Add pipeline components
        doc += "## Pipeline Components\n\n"
        doc += f"- **Extractor:** [{pipeline.extractor.name}](./{pipeline.extractor.name.lower()}.md) "
        doc += f"(`{pipeline.extractor.__class__.__name__}`)\n"

        doc += "- **Transformers:**\n"
        for transformer in pipeline.transformers:
            doc += f"  - [{transformer.name}](./{transformer.name.lower()}.md) "
            doc += f"(`{transformer.__class__.__name__}`)\n"

        if pipeline.loader:
            doc += f"- **Loader:** [{pipeline.loader.name}](./{pipeline.loader.name.lower()}.md) "
            doc += f"(`{pipeline.loader.__class__.__name__}`)\n"

        # Add configuration
        if pipeline.config:
            doc += "\n## Configuration\n\n"
            doc += "```python\n"
            doc += f"{pipeline.config}\n"
            doc += "```\n\n"

        # Add flow diagram
        doc += "## Flow Diagram\n\n"
        doc += "```mermaid\n"
        doc += "graph LR\n"

        # Add extractor node
        doc += f"    E[{pipeline.extractor.name}] --> T1\n"

        # Add transformer nodes
        for i, transformer in enumerate(pipeline.transformers):
            if i < len(pipeline.transformers) - 1:
                doc += f"    T{i + 1}[{transformer.name}] --> T{i + 2}\n"
            elif pipeline.loader:
                doc += f"    T{i + 1}[{transformer.name}] --> L\n"

        # Add loader node if present
        if pipeline.loader:
            doc += f"    L[{pipeline.loader.name}]\n"

        doc += "```\n\n"

        return doc

    def _generate_index_doc(
        self,
        extractors: dict[str, BaseExtractor[Any]],
        transformers: dict[str, BaseTransformer[Any, Any]],
        loaders: dict[str, BaseLoader[Any]],
        pipelines: dict[str, Pipeline],
    ) -> str:
        """
        Generate an index Markdown document with links to all components.

        Args:
            extractors: Dictionary of extractors
            transformers: Dictionary of transformers
            loaders: Dictionary of loaders
            pipelines: Dictionary of pipelines

        Returns:
            Markdown documentation as a string
        """
        doc = "# Data Warehouse Workflow Documentation\n\n"
        doc += "This documentation provides details about the workflows configured in the data warehouse system.\n\n"

        # Add pipelines section
        doc += "## Pipelines\n\n"
        if pipelines:
            for name, _pipeline in pipelines.items():
                doc += f"- [{name}](./{name.lower()}.md)\n"
        else:
            doc += "No pipelines configured.\n"

        # Add extractors section
        doc += "\n## Extractors\n\n"
        if extractors:
            for name in extractors.keys():
                doc += f"- [{name}](./{name.lower()}.md)\n"
        else:
            doc += "No extractors configured.\n"

        # Add transformers section
        doc += "\n## Transformers\n\n"
        if transformers:
            for name in transformers.keys():
                doc += f"- [{name}](./{name.lower()}.md)\n"
        else:
            doc += "No transformers configured.\n"

        # Add loaders section
        doc += "\n## Loaders\n\n"
        if loaders:
            for name in loaders.keys():
                doc += f"- [{name}](./{name.lower()}.md)\n"
        else:
            doc += "No loaders configured.\n"

        return doc

    def generate_docs(self) -> None:
        """
        Generate documentation for all workflow components.

        This method discovers all workflow components, generates Markdown documentation
        for each one, and saves the files to the output directory.
        """
        self._ensure_output_dir()

        # Discover components
        self.workflow_manager.discover_components()

        # Get all components
        extractors = self.workflow_manager.get_all_extractors()
        transformers = self.workflow_manager.get_all_transformers()
        loaders = self.workflow_manager.get_all_loaders()
        pipelines = self.workflow_manager.get_all_pipelines()

        # Generate documentation for extractors
        for name, extractor in extractors.items():
            doc = self._generate_component_doc(name, extractor, "extractor")
            file_path = os.path.join(self.output_dir, f"{name.lower()}.md")
            with open(file_path, "w", encoding="utf-8") as f:
                f.write(doc)
            logger.info(f"Generated documentation for extractor: {name}")

        # Generate documentation for transformers
        for name, transformer in transformers.items():
            doc = self._generate_component_doc(name, transformer, "transformer")
            file_path = os.path.join(self.output_dir, f"{name.lower()}.md")
            with open(file_path, "w", encoding="utf-8") as f:
                f.write(doc)
            logger.info(f"Generated documentation for transformer: {name}")

        # Generate documentation for loaders
        for name, loader in loaders.items():
            doc = self._generate_component_doc(name, loader, "loader")
            file_path = os.path.join(self.output_dir, f"{name.lower()}.md")
            with open(file_path, "w", encoding="utf-8") as f:
                f.write(doc)
            logger.info(f"Generated documentation for loader: {name}")

        # Generate documentation for pipelines
        for name, pipeline in pipelines.items():
            doc = self._generate_pipeline_doc(name, pipeline)
            file_path = os.path.join(self.output_dir, f"{name.lower()}.md")
            with open(file_path, "w", encoding="utf-8") as f:
                f.write(doc)
            logger.info(f"Generated documentation for pipeline: {name}")

        # Generate index file
        index_doc = self._generate_index_doc(extractors, transformers, loaders, pipelines)
        index_path = os.path.join(self.output_dir, "index.md")
        with open(index_path, "w", encoding="utf-8") as f:
            f.write(index_doc)
        logger.info(f"Generated index documentation at: {index_path}")

    def generate_pipeline_doc(self, pipeline_name: str) -> str | None:
        """
        Generate documentation for a specific pipeline.

        Args:
            pipeline_name: The name of the pipeline to document

        Returns:
            The file path where the documentation was saved, or None if the pipeline was not found
        """
        self._ensure_output_dir()

        # Discover components
        self.workflow_manager.discover_components()

        try:
            # Get the pipeline
            pipeline = self.workflow_manager.get_pipeline(pipeline_name)

            # Generate documentation
            doc = self._generate_pipeline_doc(pipeline_name, pipeline)
            file_path = os.path.join(self.output_dir, f"{pipeline_name.lower()}.md")
            with open(file_path, "w", encoding="utf-8") as f:
                f.write(doc)
            logger.info(f"Generated documentation for pipeline: {pipeline_name}")
            return file_path
        except KeyError:
            logger.error(f"Pipeline not found: {pipeline_name}")
            return None
