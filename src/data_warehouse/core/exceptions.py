"""Custom exceptions for the data warehouse."""

from typing import Any


class DataWarehouseError(Exception):
    """Base exception for all data warehouse errors."""


class ConfigurationError(DataWarehouseError):
    """Raised when there is a configuration error."""


class DatabaseError(DataWarehouseError):
    """Raised when there is a database-related error."""

    def __init__(self, message: str, original_error: Any | None = None) -> None:
        """Initialize the database error.

        Args:
            message: The error message
            original_error: The original exception that caused this error

        """
        super().__init__(message)
        self.original_error = original_error


class ExtractorError(DataWarehouseError):
    """Raised when there is an error in data extraction."""


class TransformerError(DataWarehouseError):
    """Raised when there is an error in data transformation."""


class LoaderError(DataWarehouseError):
    """Raised when there is an error in data loading."""


class ValidationError(DataWarehouseError):
    """Raised when there is a data validation error."""
