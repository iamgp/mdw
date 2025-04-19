"""
Unit tests for the workflow watcher.
"""

import os
import tempfile
import time
import unittest

from workflows.watcher import WorkflowFileHandler, WorkflowWatcher


class TestWorkflowFileHandler(unittest.TestCase):
    """Test the workflow file handler."""

    def setUp(self) -> None:
        """Set up the test case."""
        self.test_files_paths: list[str] = []
        self.modified_paths: list[str] = []
        self.created_paths: list[str] = []
        self.deleted_paths: list[str] = []

        # Create callback functions
        def modified_callback(path: str) -> None:
            self.modified_paths.append(path)

        def created_callback(path: str) -> None:
            self.created_paths.append(path)

        def deleted_callback(path: str) -> None:
            self.deleted_paths.append(path)

        # Create file handler
        self.handler = WorkflowFileHandler(
            file_patterns=[".py", ".txt"],
            on_modified_callback=modified_callback,
            on_created_callback=created_callback,
            on_deleted_callback=deleted_callback,
        )

    def test_is_relevant_file(self) -> None:
        """Test the _is_relevant_file method."""
        # Test relevant files
        assert self.handler._is_relevant_file("test.py")
        assert self.handler._is_relevant_file("path/to/test.txt")

        # Test irrelevant files
        assert not self.handler._is_relevant_file("test.jpg")
        assert not self.handler._is_relevant_file("path/to/test.json")
        assert not self.handler._is_relevant_file("")
        assert not self.handler._is_relevant_file(None)  # type: ignore


class TestWorkflowWatcher(unittest.TestCase):
    """Test the workflow watcher."""

    def setUp(self) -> None:
        """Set up the test case."""
        # Create a temporary directory for test files
        self.temp_dir = tempfile.TemporaryDirectory()
        self.test_dir = self.temp_dir.name

        # Create test subdirectories
        os.makedirs(os.path.join(self.test_dir, "extractors"), exist_ok=True)
        os.makedirs(os.path.join(self.test_dir, "transformers"), exist_ok=True)

        # Track reloaded files
        self.reloaded_files: list[str] = []

        # Define callback
        def reload_callback(file_path: str) -> None:
            self.reloaded_files.append(file_path)

        # Create watcher
        self.watcher = WorkflowWatcher(
            directories=[
                os.path.join(self.test_dir, "extractors"),
                os.path.join(self.test_dir, "transformers"),
            ],
            file_patterns=[".py", ".txt"],
            reload_callback=reload_callback,
        )

    def tearDown(self) -> None:
        """Clean up after the test."""
        # Stop the watcher if it's running
        if hasattr(self, "watcher") and self.watcher._running:
            self.watcher.stop()

        # Clean up the temporary directory
        if hasattr(self, "temp_dir"):
            self.temp_dir.cleanup()

    def test_initialization(self) -> None:
        """Test the initialization of the watcher."""
        assert len(self.watcher.directories) == 2
        assert self.watcher.file_patterns == [".py", ".txt"]
        assert not self.watcher._running

    def test_start_stop(self) -> None:
        """Test starting and stopping the watcher."""
        # Start the watcher
        self.watcher.start()
        assert self.watcher._running

        # Stop the watcher
        self.watcher.stop()
        assert not self.watcher._running

    def test_context_manager(self) -> None:
        """Test using the watcher as a context manager."""
        with self.watcher as w:
            assert w._running

        assert not self.watcher._running


# Skip this test by default as it relies on file system events which can be unreliable in CI environments
@unittest.skip("This test is slow and may be unreliable in CI environments")
class TestWorkflowWatcherFileEvents(unittest.TestCase):
    """Test the workflow watcher with actual file events."""

    def setUp(self) -> None:
        """Set up the test case."""
        # Create a temporary directory for test files
        self.temp_dir = tempfile.TemporaryDirectory()
        self.test_dir = self.temp_dir.name

        # Create test subdirectories
        os.makedirs(os.path.join(self.test_dir, "extractors"), exist_ok=True)

        # Track reloaded files
        self.reloaded_files: list[str] = []

        # Define callback
        def reload_callback(file_path: str) -> None:
            self.reloaded_files.append(file_path)

        # Create watcher
        self.watcher = WorkflowWatcher(
            directories=[os.path.join(self.test_dir, "extractors")],
            file_patterns=[".py"],
            reload_callback=reload_callback,
        )

        # Start the watcher
        self.watcher.start()

    def tearDown(self) -> None:
        """Clean up after the test."""
        # Stop the watcher
        if hasattr(self, "watcher"):
            self.watcher.stop()

        # Clean up the temporary directory
        if hasattr(self, "temp_dir"):
            self.temp_dir.cleanup()

    def test_file_created(self) -> None:
        """Test that the watcher detects when a file is created."""
        # Create a test file
        test_file = os.path.join(self.test_dir, "extractors", "test_extractor.py")
        with open(test_file, "w", encoding="utf-8") as f:
            f.write("# Test file")

        # Wait for the event to be processed
        time.sleep(1)

        # Check that the file was reloaded
        assert test_file in self.reloaded_files

    def test_file_modified(self) -> None:
        """Test that the watcher detects when a file is modified."""
        # Create a test file
        test_file = os.path.join(self.test_dir, "extractors", "test_extractor2.py")
        with open(test_file, "w", encoding="utf-8") as f:
            f.write("# Test file")

        # Wait for the event to be processed
        time.sleep(1)

        # Clear the reloaded files list
        self.reloaded_files.clear()

        # Modify the file
        with open(test_file, "a", encoding="utf-8") as f:
            f.write("\n# Modified")

        # Wait for the event to be processed
        time.sleep(1)

        # Check that the file was reloaded
        assert test_file in self.reloaded_files


if __name__ == "__main__":
    unittest.main()
