"""
Base classes for the data warehouse workflow system.

This module defines the abstract base classes that provide the standard interfaces
for extraction, transformation, and loading operations.
"""

from abc import ABC, abstractmethod
from datetime import datetime
from typing import Any, Generic, TypeVar

# Define type variables for input and output types
InputType = TypeVar("InputType")
OutputType = TypeVar("OutputType")
MetadataType = dict[str, Any]


class BaseExtractor(Generic[OutputType], ABC):
    """
    Base class for all data extractors.

    An extractor is responsible for retrieving data from a source system
    and providing it in a standardized format for further processing.
    """

    def __init__(self, name: str, config: dict[str, Any] | None = None) -> None:
        """
        Initialize a BaseExtractor instance.

        Args:
            name: A unique name for this extractor instance
            config: Configuration parameters for the extractor
        """
        self.name = name
        self.config = config or {}
        self.last_run_time: datetime | None = None

    @abstractmethod
    def extract(self) -> OutputType:
        """
        Extract data from the source.

        Returns:
            The extracted data in the specified output format

        Raises:
            ExtractorError: If the extraction fails
        """
        pass

    @abstractmethod
    def validate_source(self) -> bool:
        """
        Validate that the source is available and properly configured.

        Returns:
            True if the source is valid, False otherwise
        """
        pass

    def get_metadata(self) -> MetadataType:
        """
        Get metadata about this extractor.

        Returns:
            A dictionary containing metadata about the extractor
        """
        return {
            "name": self.name,
            "type": self.__class__.__name__,
            "last_run": self.last_run_time.isoformat() if self.last_run_time else None,
            "config": {k: v for k, v in self.config.items() if not k.startswith("_")},
        }


class BaseTransformer(Generic[InputType, OutputType], ABC):
    """
    Base class for all data transformers.

    A transformer is responsible for processing input data and
    transforming it into a different format or structure.
    """

    def __init__(self, name: str, config: dict[str, Any] | None = None) -> None:
        """
        Initialize a BaseTransformer instance.

        Args:
            name: A unique name for this transformer instance
            config: Configuration parameters for the transformer
        """
        self.name = name
        self.config = config or {}
        self.last_run_time: datetime | None = None

    @abstractmethod
    def transform(self, data: InputType) -> OutputType:
        """
        Transform the input data.

        Args:
            data: The input data to transform

        Returns:
            The transformed data

        Raises:
            TransformerError: If the transformation fails
        """
        pass

    @abstractmethod
    def validate_input(self, data: InputType) -> bool:
        """
        Validate that the input data meets the requirements for this transformer.

        Args:
            data: The input data to validate

        Returns:
            True if the input is valid, False otherwise

        Raises:
            ValidationError: If validation fails with details
        """
        pass

    @abstractmethod
    def validate_output(self, data: OutputType) -> bool:
        """
        Validate that the output data meets the requirements.

        Args:
            data: The output data to validate

        Returns:
            True if the output is valid, False otherwise

        Raises:
            ValidationError: If validation fails with details
        """
        pass

    def get_metadata(self) -> MetadataType:
        """
        Get metadata about this transformer.

        Returns:
            A dictionary containing metadata about the transformer
        """
        return {
            "name": self.name,
            "type": self.__class__.__name__,
            "last_run": self.last_run_time.isoformat() if self.last_run_time else None,
            "config": {k: v for k, v in self.config.items() if not k.startswith("_")},
        }


class BaseLoader(Generic[InputType], ABC):
    """
    Base class for all data loaders.

    A loader is responsible for loading transformed data into a destination
    system, such as a database, file, or API.
    """

    def __init__(self, name: str, config: dict[str, Any] | None = None) -> None:
        """
        Initialize a BaseLoader instance.

        Args:
            name: A unique name for this loader instance
            config: Configuration parameters for the loader
        """
        self.name = name
        self.config = config or {}
        self.last_run_time: datetime | None = None

    @abstractmethod
    def load(self, data: InputType) -> None:
        """
        Load data into the destination.

        Args:
            data: The data to load

        Raises:
            LoaderError: If the loading fails
        """
        pass

    @abstractmethod
    def validate_destination(self) -> bool:
        """
        Validate that the destination is available and properly configured.

        Returns:
            True if the destination is valid, False otherwise
        """
        pass

    def get_metadata(self) -> MetadataType:
        """
        Get metadata about this loader.

        Returns:
            A dictionary containing metadata about the loader
        """
        return {
            "name": self.name,
            "type": self.__class__.__name__,
            "last_run": self.last_run_time.isoformat() if self.last_run_time else None,
            "config": {k: v for k, v in self.config.items() if not k.startswith("_")},
        }


class Pipeline:
    """
    A class for chaining extractors, transformers, and loaders together.

    A pipeline defines a sequence of operations to extract, transform,
    and load data, forming a complete ETL workflow.
    """

    def __init__(
        self,
        name: str,
        extractor: BaseExtractor,
        transformers: list[BaseTransformer] | None = None,
        loader: BaseLoader | None = None,
        config: dict[str, Any] | None = None,
    ) -> None:
        """
        Initialize a Pipeline instance.

        Args:
            name: A unique name for this pipeline
            extractor: The extractor to use
            transformers: A list of transformers to apply in sequence
            loader: The loader to use
            config: Configuration parameters for the pipeline
        """
        self.name = name
        self.extractor = extractor
        self.transformers = transformers or []
        self.loader = loader
        self.config = config or {}
        self.last_run_time: datetime | None = None

    def execute(self) -> Any:
        """
        Execute the pipeline.

        This method will:
        1. Extract data using the extractor
        2. Apply each transformer in sequence
        3. Load the transformed data using the loader (if provided)

        Returns:
            The final transformed data (even if it was loaded)

        Raises:
            PipelineError: If any step in the pipeline fails
        """
        try:
            # Validate extractor source
            if not self.extractor.validate_source():
                raise PipelineError(f"Extractor source for '{self.name}' is invalid")

            # Extract
            data = self.extractor.extract()
            self.extractor.last_run_time = datetime.now()

            # Transform
            for transformer in self.transformers:
                # Validate input before transformation
                transformer.validate_input(data)

                # Transform the data
                data = transformer.transform(data)

                # Validate output after transformation
                transformer.validate_output(data)

                # Update last run time
                transformer.last_run_time = datetime.now()

            # Load (if a loader is provided)
            if self.loader:
                # Validate loader destination
                if not self.loader.validate_destination():
                    raise PipelineError(f"Loader destination for '{self.name}' is invalid")

                self.loader.load(data)
                self.loader.last_run_time = datetime.now()

            # Update pipeline's last run time
            self.last_run_time = datetime.now()

            # Return the final data
            return data
        except Exception as e:
            # Wrap any exception in a PipelineError
            if not isinstance(e, PipelineError):
                raise PipelineError(f"Pipeline '{self.name}' execution failed: {str(e)}") from e
            raise

    def get_metadata(self) -> MetadataType:
        """
        Get metadata about this pipeline.

        Returns:
            A dictionary containing metadata about the pipeline
        """
        return {
            "name": self.name,
            "type": self.__class__.__name__,
            "extractor": self.extractor.get_metadata(),
            "transformers": [t.get_metadata() for t in self.transformers],
            "loader": self.loader.get_metadata() if self.loader else None,
            "last_run": self.last_run_time.isoformat() if self.last_run_time else None,
            "config": {k: v for k, v in self.config.items() if not k.startswith("_")},
        }


class WorkflowManager:
    """
    A class for managing workflow components and pipelines.

    The workflow manager is responsible for:
    - Registering workflow components (extractors, transformers, loaders)
    - Managing pipelines
    - Providing discovery capabilities
    """

    def __init__(self) -> None:
        """Initialize a WorkflowManager instance."""
        self.extractors: dict[str, BaseExtractor] = {}
        self.transformers: dict[str, BaseTransformer] = {}
        self.loaders: dict[str, BaseLoader] = {}
        self.pipelines: dict[str, Pipeline] = {}

    def register_extractor(self, extractor: BaseExtractor) -> None:
        """
        Register an extractor.

        Args:
            extractor: The extractor to register
        """
        self.extractors[extractor.name] = extractor

    def register_transformer(self, transformer: BaseTransformer) -> None:
        """
        Register a transformer.

        Args:
            transformer: The transformer to register
        """
        self.transformers[transformer.name] = transformer

    def register_loader(self, loader: BaseLoader) -> None:
        """
        Register a loader.

        Args:
            loader: The loader to register
        """
        self.loaders[loader.name] = loader

    def register_pipeline(self, pipeline: Pipeline) -> None:
        """
        Register a pipeline.

        Args:
            pipeline: The pipeline to register
        """
        self.pipelines[pipeline.name] = pipeline

    def get_extractor(self, name: str) -> BaseExtractor:
        """
        Get an extractor by name.

        Args:
            name: The name of the extractor

        Returns:
            The extractor with the given name

        Raises:
            KeyError: If no extractor with the given name exists
        """
        return self.extractors[name]

    def get_transformer(self, name: str) -> BaseTransformer:
        """
        Get a transformer by name.

        Args:
            name: The name of the transformer

        Returns:
            The transformer with the given name

        Raises:
            KeyError: If no transformer with the given name exists
        """
        return self.transformers[name]

    def get_loader(self, name: str) -> BaseLoader:
        """
        Get a loader by name.

        Args:
            name: The name of the loader

        Returns:
            The loader with the given name

        Raises:
            KeyError: If no loader with the given name exists
        """
        return self.loaders[name]

    def get_pipeline(self, name: str) -> Pipeline:
        """
        Get a pipeline by name.

        Args:
            name: The name of the pipeline

        Returns:
            The pipeline with the given name

        Raises:
            KeyError: If no pipeline with the given name exists
        """
        return self.pipelines[name]

    def get_all_extractors(self) -> dict[str, BaseExtractor]:
        """
        Get all registered extractors.

        Returns:
            A dictionary mapping extractor names to extractors
        """
        return self.extractors

    def get_all_transformers(self) -> dict[str, BaseTransformer]:
        """
        Get all registered transformers.

        Returns:
            A dictionary mapping transformer names to transformers
        """
        return self.transformers

    def get_all_loaders(self) -> dict[str, BaseLoader]:
        """
        Get all registered loaders.

        Returns:
            A dictionary mapping loader names to loaders
        """
        return self.loaders

    def get_all_pipelines(self) -> dict[str, Pipeline]:
        """
        Get all registered pipelines.

        Returns:
            A dictionary mapping pipeline names to pipelines
        """
        return self.pipelines
