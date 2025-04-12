"""Base classes for data warehouse components."""

from abc import ABC, abstractmethod
from typing import Any, Generic, TypeVar

from data_warehouse.utils.logger import logger
from pydantic import BaseModel

# Type variables for generic type hints
ConfigT = TypeVar("ConfigT", bound=BaseModel)
DataT = TypeVar("DataT")


class Component(Generic[ConfigT], ABC):
    """Base class for all data warehouse components."""

    def __init__(self, config: ConfigT) -> None:
        """Initialize the component.

        Args:
            config: Component configuration

        """
        self.config = config
        self.logger = logger.bind(component=self.__class__.__name__)

    @abstractmethod
    async def initialize(self) -> None:
        """Initialize the component."""
        raise NotImplementedError

    @abstractmethod
    async def cleanup(self) -> None:
        """Clean up component resources."""
        raise NotImplementedError


class Extractor(Component[ConfigT], Generic[ConfigT, DataT]):
    """Base class for data extractors."""

    @abstractmethod
    async def extract(self) -> DataT:
        """Extract data from the source.

        Returns:
            The extracted data

        """
        raise NotImplementedError


class Transformer(Component[ConfigT], Generic[ConfigT, DataT]):
    """Base class for data transformers."""

    @abstractmethod
    async def transform(self, data: DataT) -> DataT:
        """Transform the input data.

        Args:
            data: Data to transform

        Returns:
            The transformed data

        """
        raise NotImplementedError


class Loader(Component[ConfigT], Generic[ConfigT, DataT]):
    """Base class for data loaders."""

    @abstractmethod
    async def load(self, data: DataT) -> None:
        """Load data into the target.

        Args:
            data: Data to load

        """
        raise NotImplementedError


class Pipeline(Component[ConfigT]):
    """Base class for data pipelines."""

    def __init__(
        self,
        config: ConfigT,
        extractors: list[Extractor[Any, Any]],
        transformers: list[Transformer[Any, Any]],
        loaders: list[Loader[Any, Any]],
    ) -> None:
        """Initialize the pipeline.

        Args:
            config: Pipeline configuration
            extractors: List of data extractors
            transformers: List of data transformers
            loaders: List of data loaders

        """
        super().__init__(config)
        self.extractors = extractors
        self.transformers = transformers
        self.loaders = loaders

    async def run(self) -> dict[str, Any]:
        """Run the pipeline.

        Returns:
            Dictionary containing pipeline execution metrics

        """
        metrics: dict[str, Any] = {
            "extractors": [],
            "transformers": [],
            "loaders": [],
        }

        # Initialize all components
        for component in [*self.extractors, *self.transformers, *self.loaders]:
            await component.initialize()

        try:
            # Extract
            extracted_data = []
            for extractor in self.extractors:
                self.logger.info(f"Running extractor: {extractor.__class__.__name__}")
                data = await extractor.extract()
                extracted_data.append(data)
                metrics["extractors"].append(
                    {
                        "name": extractor.__class__.__name__,
                        "status": "success",
                    }
                )

            # Transform
            transformed_data = extracted_data
            for transformer in self.transformers:
                transformer_name = transformer.__class__.__name__
                self.logger.info(f"Running transformer: {transformer_name}")
                transformed_data = [
                    await transformer.transform(data) for data in transformed_data
                ]
                metrics["transformers"].append(
                    {
                        "name": transformer_name,
                        "records_processed": len(transformed_data),
                    }
                )

            # Load
            for loader in self.loaders:
                self.logger.info(f"Running loader: {loader.__class__.__name__}")
                for data in transformed_data:
                    await loader.load(data)
                metrics["loaders"].append(
                    {
                        "name": loader.__class__.__name__,
                        "status": "success",
                    }
                )

        finally:
            # Cleanup all components
            for component in [*self.extractors, *self.transformers, *self.loaders]:
                await component.cleanup()

        return metrics
