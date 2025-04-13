"""Custom exceptions for the data warehouse."""


class DataWarehouseError(Exception):
    """Base exception for all data warehouse errors."""

    def __init__(self, message: str, cause: Exception | None = None):
        """Initialize with an error message and optional cause.

        Args:
            message: Error message
            cause: Optional exception that caused this error
        """
        self.message = message
        self.cause = cause
        super().__init__(message)


class DatabaseError(DataWarehouseError):
    """Exception raised for database errors."""

    def __init__(self, message: str, cause: Exception | None = None):
        """Initialize with an error message and optional cause.

        Args:
            message: Error message
            cause: Optional exception that caused this error
        """
        super().__init__(f"Database error: {message}", cause)


class StorageError(DataWarehouseError):
    """Exception raised for storage errors."""

    def __init__(self, message: str, cause: Exception | None = None):
        """Initialize with an error message and optional cause.

        Args:
            message: Error message
            cause: Optional exception that caused this error
        """
        super().__init__(f"Storage error: {message}", cause)


class ConfigurationError(DataWarehouseError):
    """Exception raised for configuration errors."""

    def __init__(self, message: str, cause: Exception | None = None):
        """Initialize with an error message and optional cause.

        Args:
            message: Error message
            cause: Optional exception that caused this error
        """
        super().__init__(f"Configuration error: {message}", cause)


class ValidationError(DataWarehouseError):
    """Exception raised for data validation errors."""

    def __init__(self, message: str, cause: Exception | None = None):
        """Initialize with an error message and optional cause.

        Args:
            message: Error message
            cause: Optional exception that caused this error
        """
        super().__init__(f"Validation error: {message}", cause)


class ProcessingError(DataWarehouseError):
    """Exception raised for data processing errors."""

    def __init__(self, message: str, cause: Exception | None = None):
        """Initialize with an error message and optional cause.

        Args:
            message: Error message
            cause: Optional exception that caused this error
        """
        super().__init__(f"Processing error: {message}", cause)


class ApiError(DataWarehouseError):
    """Exception raised for API errors."""

    def __init__(
        self,
        message: str,
        status_code: int = 500,
        cause: Exception | None = None,
    ):
        """Initialize with an error message, status code, and optional cause.

        Args:
            message: Error message
            status_code: HTTP status code
            cause: Optional exception that caused this error
        """
        self.status_code = status_code
        super().__init__(f"API error ({status_code}): {message}", cause)


class ExtractorError(DataWarehouseError):
    """Raised when there is an error in data extraction."""


class TransformerError(DataWarehouseError):
    """Raised when there is an error in data transformation."""


class LoaderError(DataWarehouseError):
    """Raised when there is an error in data loading."""
