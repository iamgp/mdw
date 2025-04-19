"""
Workflow file watcher for the data warehouse workflow system.

This module provides a WorkflowWatcher class that monitors workflow directories
for changes and triggers reloading of components when files are modified.
"""

import logging
import os
from collections.abc import Callable
from typing import Any

from watchdog.events import FileSystemEvent, FileSystemEventHandler
from watchdog.observers import Observer

logger = logging.getLogger(__name__)


class WorkflowFileHandler(FileSystemEventHandler):
    """
    File system event handler for workflow files.

    This class handles file system events (create, modify, delete) and
    triggers callbacks when relevant files are changed.
    """

    def __init__(
        self,
        file_patterns: list[str] | None = None,
        on_modified_callback: Callable[[str], None] | None = None,
        on_created_callback: Callable[[str], None] | None = None,
        on_deleted_callback: Callable[[str], None] | None = None,
    ) -> None:
        """
        Initialize a WorkflowFileHandler.

        Args:
            file_patterns: List of file patterns to watch (e.g., ["*.py", "*.yaml"])
            on_modified_callback: Callback function when a file is modified
            on_created_callback: Callback function when a file is created
            on_deleted_callback: Callback function when a file is deleted
        """
        self.file_patterns = file_patterns or [".py"]
        self.on_modified_callback = on_modified_callback
        self.on_created_callback = on_created_callback
        self.on_deleted_callback = on_deleted_callback

    def _is_relevant_file(self, path: str) -> bool:
        """
        Check if a file matches the patterns we're watching.

        Args:
            path: The file path to check

        Returns:
            True if the file matches any of the patterns, False otherwise
        """
        if not path:
            return False

        return any(path.endswith(pattern) for pattern in self.file_patterns)

    def on_modified(self, event: FileSystemEvent) -> None:
        """
        Handle file modification events.

        Args:
            event: The file system event
        """
        src_path = str(event.src_path) if event.src_path else ""
        if not event.is_directory and self._is_relevant_file(src_path):
            logger.info(f"File modified: {src_path}")
            if self.on_modified_callback:
                self.on_modified_callback(src_path)

    def on_created(self, event: FileSystemEvent) -> None:
        """
        Handle file creation events.

        Args:
            event: The file system event
        """
        src_path = str(event.src_path) if event.src_path else ""
        if not event.is_directory and self._is_relevant_file(src_path):
            logger.info(f"File created: {src_path}")
            if self.on_created_callback:
                self.on_created_callback(src_path)

    def on_deleted(self, event: FileSystemEvent) -> None:
        """
        Handle file deletion events.

        Args:
            event: The file system event
        """
        src_path = str(event.src_path) if event.src_path else ""
        if not event.is_directory and self._is_relevant_file(src_path):
            logger.info(f"File deleted: {src_path}")
            if self.on_deleted_callback:
                self.on_deleted_callback(src_path)


class WorkflowWatcher:
    """
    Watches workflow directories for file changes and triggers reloads.

    This class monitors the workflow directories (extractors, transformers, loaders, etc.)
    and triggers reloading of components when files are modified, created, or deleted.
    """

    def __init__(
        self,
        directories: list[str] | None = None,
        file_patterns: list[str] | None = None,
        reload_callback: Callable[[str], None] | None = None,
    ) -> None:
        """
        Initialize a WorkflowWatcher.

        Args:
            directories: List of directories to watch
            file_patterns: List of file patterns to watch
            reload_callback: Callback function when a file changes
        """
        self.directories = directories or [
            "workflows/extractors",
            "workflows/transformers",
            "workflows/loaders",
            "workflows/pipelines",
            "workflows/templates",
        ]
        self.file_patterns = file_patterns or [".py", ".yaml", ".yml", ".json"]
        self.reload_callback = reload_callback
        self.observer = Observer()
        self.handler = WorkflowFileHandler(
            file_patterns=self.file_patterns,
            on_modified_callback=self._on_file_changed,
            on_created_callback=self._on_file_changed,
            on_deleted_callback=self._on_file_changed,
        )
        self._running = False

    def _on_file_changed(self, file_path: str) -> None:
        """
        Handle file change events.

        Args:
            file_path: The path of the changed file
        """
        # Determine component type from file path
        for dir_name in ["extractors", "transformers", "loaders", "pipelines", "templates"]:
            if dir_name in file_path:
                break

        # Call the reload callback with the component type and file path
        if self.reload_callback:
            try:
                self.reload_callback(file_path)
            except Exception as e:
                logger.error(f"Error in reload callback: {str(e)}")

    def start(self) -> None:
        """
        Start watching the workflow directories.

        This method sets up watchdog observers for each directory and starts monitoring.
        """
        if self._running:
            logger.warning("Workflow watcher is already running")
            return

        # Create observers for each directory
        for directory in self.directories:
            if os.path.exists(directory) and os.path.isdir(directory):
                self.observer.schedule(self.handler, directory, recursive=True)
                logger.info(f"Watching directory: {directory}")
            else:
                logger.warning(f"Directory not found: {directory}")

        # Start the observer
        self.observer.start()
        self._running = True
        logger.info("Workflow watcher started")

    def stop(self) -> None:
        """
        Stop watching the workflow directories.

        This method stops the watchdog observers and cleans up resources.
        """
        if not self._running:
            logger.warning("Workflow watcher is not running")
            return

        self.observer.stop()
        self.observer.join()
        self._running = False
        logger.info("Workflow watcher stopped")

    def __enter__(self) -> "WorkflowWatcher":
        """
        Context manager entry method.

        Returns:
            The WorkflowWatcher instance
        """
        self.start()
        return self

    def __exit__(self, exc_type: Any, exc_val: Any, exc_tb: Any) -> None:
        """
        Context manager exit method.

        Args:
            exc_type: Exception type
            exc_val: Exception value
            exc_tb: Exception traceback
        """
        self.stop()
