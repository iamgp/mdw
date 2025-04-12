"""Test fixtures and configuration."""

from collections.abc import Generator
from pathlib import Path
from typing import Any

import pytest


@pytest.fixture(scope="session")
def test_dir() -> Path:
    """Return the test directory path."""
    return Path(__file__).parent


@pytest.fixture(scope="session")
def fixtures_dir(test_dir: Path) -> Path:
    """Return the fixtures directory path."""
    return test_dir / "fixtures"


@pytest.fixture(scope="session")
def project_root() -> Path:
    """Return the project root directory path."""
    return Path(__file__).parent.parent


@pytest.fixture
def test_data() -> dict[str, Any]:
    """Provide test data for the test cases.

    Returns:
        Dict[str, Any]: Test data dictionary containing key-value pairs for testing
    """
    return {"key": "value"}


@pytest.fixture(scope="function")
def mock_env(monkeypatch: pytest.MonkeyPatch) -> None:
    """Mock environment variables for testing.

    Args:
        monkeypatch: Pytest fixture for modifying the test environment
    """
    monkeypatch.setenv("POSTGRES_USER", "test_user")
    monkeypatch.setenv("POSTGRES_PASSWORD", "test_password")
    monkeypatch.setenv("POSTGRES_HOST", "localhost")
    monkeypatch.setenv("POSTGRES_PORT", "5432")
    monkeypatch.setenv("POSTGRES_DB", "test_db")


@pytest.fixture(scope="function")
def mock_config(request: pytest.FixtureRequest) -> dict[str, Any]:
    """Mock configuration for testing.

    Args:
        request: Pytest fixture request object

    Returns:
        Dict[str, Any]: Mock configuration dictionary with database connection
        parameters
    """
    return {
        "user": "test_user",
        "password": "test_password",
        "host": "localhost",
        "port": 5432,
        "database": "test_db",
    }


@pytest.fixture(scope="function")
def mock_db_connection(
    request: pytest.FixtureRequest,
) -> Generator[dict[str, bool], None, None]:
    """Mock database connection for testing.

    Args:
        request: Pytest fixture request object

    Yields:
        Dict[str, bool]: Mock connection object representing a database connection
    """
    # Here you would typically set up a test database connection
    connection = {"connected": True}  # Mock connection object
    yield connection
    # Clean up would happen here
    connection["connected"] = False
