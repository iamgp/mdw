"""Basic Transformer Implementations."""

from typing import Any

from data_warehouse.workflows.core.base import BaseTransformer


class PassthroughTransformer(BaseTransformer[Any, Any]):
    """A simple transformer that passes data through without modification."""

    def validate_input(self, data: Any) -> bool:
        """Validate input data (always true for passthrough)."""
        # Basic validation: check if data is not None, adjust as needed
        # For a true passthrough, we might not even need None check
        # return data is not None
        return True  # Simplest validation for passthrough

    def validate_output(self, data: Any) -> bool:
        """Validate output data (always true for passthrough)."""
        # Output is same as input, so validation logic could be similar
        # return data is not None
        return True  # Simplest validation for passthrough

    def transform(self, data: Any) -> Any:
        """
        Returns the input data unchanged.

        Args:
            data: The input data.

        Returns:
            The same input data.
        """
        if data is None:
            # Or raise TransformerError based on expected behavior
            return None
        # No transformation needed, just return the data
        return data
