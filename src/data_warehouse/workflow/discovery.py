"""
Workflow Discovery System.

This module implements the workflow discovery mechanism with hot-reloading capabilities.
It provides functionality to automatically discover and register workflows in the system.
"""

import importlib
import importlib.util
import os
import sys
import threading
import time
from collections.abc import Callable

from loguru import logger
from watchdog.events import FileSystemEvent, FileSystemEventHandler
from watchdog.observers import Observer

from data_warehouse.core.exceptions import WorkflowDiscoveryError
from data_warehouse.workflow.base import WorkflowBase, WorkflowRegistry


class WorkflowDiscovery:
    """Workflow discovery system that finds and registers workflows."""

    def __init__(self, registry: WorkflowRegistry | None = None) -> None:
        """Initialize the discovery system.

        Args:
            registry: Optional workflow registry to use. If not provided,
                     the singleton registry will be used.
        """
        self.registry = registry or WorkflowRegistry()
        self.discovered_modules: dict[str, float] = {}  # module path -> timestamp
        self.observer: Observer | None = None
        self.watching = False
        self._lock = threading.RLock()

    def discover_workflows(self, package_path: str) -> list[type[WorkflowBase]]:
        """Discover and register all workflows in a package.

        Args:
            package_path: Import path of the package to scan

        Returns:
            List of discovered workflow classes

        Raises:
            WorkflowDiscoveryError: If discovery fails
        """
        try:
            # Import the package
            logger.info(f"Discovering workflows in package: {package_path}")
            package = importlib.import_module(package_path)

            # Get the filesystem path
            if not hasattr(package, "__path__"):
                raise WorkflowDiscoveryError(f"Invalid package: {package_path}")

            fs_path = package.__path__[0]
            discovered = []

            # Walk the directory tree
            for root, dirs, files in os.walk(fs_path):
                # Skip hidden directories and _templates, _common
                dirs[:] = [d for d in dirs if not d.startswith("_") and not d.startswith(".")]

                # Process Python files
                for file in files:
                    if file.endswith(".py") and not file.startswith("_"):
                        file_path = os.path.join(root, file)
                        rel_path = os.path.relpath(file_path, fs_path)
                        module_path = f"{package_path}.{os.path.splitext(rel_path)[0].replace(os.path.sep, '.')}"

                        # Skip if already processed and file hasn't changed
                        file_timestamp = os.path.getmtime(file_path)
                        if (
                            module_path in self.discovered_modules
                            and self.discovered_modules[module_path] >= file_timestamp
                        ):
                            continue

                        # Import module and find workflow classes
                        found_workflows = self._import_module_workflows(module_path, file_path)
                        discovered.extend(found_workflows)

                        # Update timestamp
                        self.discovered_modules[module_path] = file_timestamp

            logger.info(f"Discovered {len(discovered)} workflows in {package_path}")
            return discovered

        except Exception as e:
            logger.error(f"Error discovering workflows: {str(e)}")
            raise WorkflowDiscoveryError(f"Error discovering workflows: {str(e)}") from e

    def _import_module_workflows(self, module_path: str, file_path: str) -> list[type[WorkflowBase]]:
        """Import a module and register any workflow classes.

        Args:
            module_path: Import path of the module
            file_path: Filesystem path of the module

        Returns:
            List of workflow classes found in the module
        """
        try:
            # Import the module
            try:
                # Reload if already loaded
                if module_path in sys.modules:
                    module = importlib.reload(sys.modules[module_path])
                else:
                    module = importlib.import_module(module_path)

            except (ModuleNotFoundError, ImportError) as e:
                # Try importing by file path if normal import fails
                logger.warning(f"Error importing {module_path}, trying to load from file path: {str(e)}")
                spec = importlib.util.spec_from_file_location(module_path, file_path)
                if spec is None or spec.loader is None:
                    raise ImportError(f"Could not create module spec for {file_path}") from e

                module = importlib.util.module_from_spec(spec)
                sys.modules[module_path] = module
                spec.loader.exec_module(module)

            # Find workflow classes
            discovered = []
            for attr_name in dir(module):
                attr = getattr(module, attr_name)

                # Check if it's a WorkflowBase subclass
                if (
                    isinstance(attr, type)
                    and issubclass(attr, WorkflowBase)
                    and attr != WorkflowBase
                    and attr.__module__ == module.__name__
                ):
                    # Extract domain from module path
                    parts = module_path.split(".")
                    domain = parts[-2] if len(parts) > 2 else "default"

                    # Register the workflow
                    with self._lock:
                        self.registry.register(attr, domain=domain)

                    discovered.append(attr)
                    logger.debug(f"Registered workflow: {attr.__name__} from {module_path}")

            return discovered

        except Exception as e:
            logger.error(f"Error importing module {module_path}: {str(e)}")
            return []

    def start_watching(self, package_paths: list[str], reload_callback: Callable[[], None] | None = None) -> None:
        """Start watching directories for changes and hot-reload workflows.

        Args:
            package_paths: List of package import paths to watch
            reload_callback: Callback function to execute after reloading workflows
        """
        if self.watching:
            logger.warning("Already watching for workflow changes")
            return

        with self._lock:
            # Initialize observer
            self.observer = Observer()

            # Initial discovery of workflows
            watched_dirs: set[str] = set()
            for package_path in package_paths:
                try:
                    package = importlib.import_module(package_path)
                    if hasattr(package, "__path__"):
                        fs_path = package.__path__[0]
                        watched_dirs.add(fs_path)
                        self.discover_workflows(package_path)
                except Exception as e:
                    logger.error(f"Error in initial discovery of {package_path}: {str(e)}")

            # Set up file system watchers
            if self.observer:  # Add null check
                for dir_path in watched_dirs:
                    handler = WorkflowFileHandler(self, package_paths, reload_callback)
                    self.observer.schedule(handler, dir_path, recursive=True)
                    logger.info(f"Watching directory for workflow changes: {dir_path}")

                # Start observer
                self.observer.start()
                self.watching = True
                logger.info("Workflow hot-reloading enabled")

    def stop_watching(self) -> None:
        """Stop watching for filesystem changes."""
        if not self.watching or self.observer is None:
            return

        with self._lock:
            self.observer.stop()
            self.observer.join()
            self.observer = None
            self.watching = False
            logger.info("Workflow hot-reloading disabled")

    def reload_workflows(
        self, package_paths: list[str], reload_callback: Callable[[], None] | None = None
    ) -> list[type[WorkflowBase]]:
        """Reload workflows from the specified packages.

        Args:
            package_paths: List of package import paths to reload
            reload_callback: Callback function to execute after reloading

        Returns:
            List of all workflows after reloading
        """
        with self._lock:
            # Rediscover workflows
            discovered: list[type[WorkflowBase]] = []
            for package_path in package_paths:
                discovered.extend(self.discover_workflows(package_path))

            # Execute callback if provided
            if reload_callback is not None:
                reload_callback()

            return discovered


class WorkflowFileHandler(FileSystemEventHandler):
    """File system event handler for workflow hot-reloading."""

    def __init__(
        self, discovery: WorkflowDiscovery, package_paths: list[str], reload_callback: Callable[[], None] | None = None
    ) -> None:
        """Initialize the file handler.

        Args:
            discovery: Workflow discovery instance
            package_paths: List of package import paths being watched
            reload_callback: Callback function to execute after reloading
        """
        self.discovery = discovery
        self.package_paths = package_paths
        self.reload_callback = reload_callback
        self.last_reload_time = 0
        self.cooldown_period = 2.0  # seconds

    def on_modified(self, event: FileSystemEvent) -> None:
        """Handle file modification events.

        Args:
            event: File system event
        """
        if self._should_process_event(event):
            self._debounced_reload()

    def on_created(self, event: FileSystemEvent) -> None:
        """Handle file creation events.

        Args:
            event: File system event
        """
        if self._should_process_event(event):
            self._debounced_reload()

    def _should_process_event(self, event: FileSystemEvent) -> bool:
        """Check if an event should trigger workflow reloading.

        Args:
            event: File system event

        Returns:
            True if the event should be processed, False otherwise
        """
        # Ignore directory events
        if event.is_directory:
            return False

        # Check if it's a Python file
        path = event.src_path
        if not isinstance(path, str) or not path.endswith(".py"):
            return False

        # Ignore hidden files and internal modules
        filename = os.path.basename(path)
        if not isinstance(filename, str):
            return False

        if filename.startswith("_") or filename.startswith("."):
            return False

        return True

    def _debounced_reload(self) -> None:
        """Reload workflows with debouncing to prevent rapid multiple reloads."""
        current_time = time.time()

        # Check if we're in the cooldown period
        if current_time - self.last_reload_time < self.cooldown_period:
            return

        # Update timestamp and reload
        self.last_reload_time = current_time

        # Run in a separate thread to not block the file watcher
        threading.Timer(0.5, self._reload_workflows).start()

    def _reload_workflows(self) -> None:
        """Reload workflows from all watched packages."""
        try:
            logger.info("Hot-reloading workflows due to file changes...")
            self.discovery.reload_workflows(self.package_paths, self.reload_callback)
        except Exception as e:
            logger.error(f"Error during hot-reload: {str(e)}")
