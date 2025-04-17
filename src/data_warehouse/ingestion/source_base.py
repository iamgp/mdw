from typing import Any


class SourceBase:
    """
    Abstract base class for data source connectors.
    """

    def extract(self) -> Any:
        """Extract data from the source. Must be implemented by subclasses."""
        raise NotImplementedError("extract() must be implemented by the source connector.")

    def validate(self) -> bool:
        """Validate the source connection. Returns True if valid, else False."""
        return True
