"""Tests for project root settings resolution."""

import os
import sys
from pathlib import Path
from unittest import mock


def test_project_root_resolution_default():
    """Test the default project root resolution works correctly."""
    # Import inside test to ensure clean environment
    from data_warehouse.config.settings import settings

    # Get the actual project root using similar logic to what settings uses
    expected_root = Path(
        os.path.abspath(
            os.path.join(
                os.path.dirname(os.path.abspath(__file__)),  # test file dir
                "..",
                "..",
                "..",  # Up to project root
            )
        )
    )

    # Check that the detected project root is correct
    assert settings.PROJECT_ROOT.resolve() == expected_root.resolve()

    # Verify the data dir is a subdirectory of project root
    assert settings.DATA_DIR.resolve().parent == settings.PROJECT_ROOT.resolve()


def test_project_root_from_env_variable():
    """Test that PROJECT_ROOT can be set via environment variable."""
    # Set the environment variable
    test_path = "/tmp/custom_project_root"
    with mock.patch.dict(os.environ, {"DATA_WAREHOUSE_ROOT": test_path}):
        # Reload settings to pick up the env variable
        # We need to reload the module to apply the new env var
        if "data_warehouse.config.settings" in sys.modules:
            del sys.modules["data_warehouse.config.settings"]

        # Re-import with new environment
        from data_warehouse.config.settings import settings

        # Verify it picks up the environment variable
        assert str(settings.PROJECT_ROOT) == test_path

        # Verify child paths are derived correctly
        assert str(settings.DATA_DIR) == os.path.join(test_path, "data")


def test_project_root_with_different_working_directory():
    """Test PROJECT_ROOT resolves correctly from different working directories."""
    original_dir = os.getcwd()

    try:
        # Change to a different directory
        os.chdir("/tmp")

        # Clear the module cache to ensure a fresh import
        if "data_warehouse.config.settings" in sys.modules:
            del sys.modules["data_warehouse.config.settings"]

        # Import settings from the new working directory
        from data_warehouse.config.settings import settings

        # The project root should still be determined by file location, not cwd
        assert "/tmp" not in str(settings.PROJECT_ROOT)

        # Check that computed paths are working
        assert settings.DATA_DIR.name == "data"
        assert settings.DUCKDB_PATH.name == "warehouse.db"

    finally:
        # Restore the original directory
        os.chdir(original_dir)
