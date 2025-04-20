from typing import Any

from data_warehouse.workflows.core.base import BaseTransformer


class IdentityTransformer(BaseTransformer[Any, Any]):
    """A simple transformer that passes data through unchanged."""

    def transform(self, data: Any) -> Any:
        """Returns the input data without modification."""
        # No operation needed, just return the data
        return data

    def validate_input(self, data: Any) -> bool:
        """Always returns True as any input is acceptable."""
        return True

    def validate_output(self, data: Any) -> bool:
        """Always returns True as the output is the same as the input."""
        return True
