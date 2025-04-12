"""Test fixtures and configuration."""

import os
from collections.abc import Generator
from pathlib import Path
from typing import Any

import pytest
from _pytest.fixtures import FixtureRequest
from _pytest.monkeypatch import MonkeyPatch


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
        dict: Test data dictionary
    """
    return {"key": "value"}


@pytest.fixture
def mock_config() -> dict[str, Any]:
    """Provide mock configuration for testing.

    Returns:
        dict: Mock configuration dictionary
    """
    return {
        "database": "test_db",
        "host": "localhost",
        "port": 5432,
    }


@pytest.fixture
def mock_env() -> dict[str, str]:
    """Provide mock environment variables.

    Returns:
        dict: Mock environment variables
    """
    return {
        "DB_HOST": "localhost",
        "DB_PORT": "5432",
        "DB_NAME": "test_db",
    }


@pytest.fixture
def _temp_env(
    request: FixtureRequest, monkeypatch: MonkeyPatch
) -> Generator[None, None, None]:
    """Set up temporary environment variables for tests.

    Args:
        request: Pytest fixture request object
        monkeypatch: Pytest monkeypatch fixture
    """
    marker = request.node.get_closest_marker("env")
    if marker:
        for key, value in marker.kwargs.items():
            monkeypatch.setenv(key, str(value))

    yield None

    os.environ.clear()
    os.environ.update(old_environ)
