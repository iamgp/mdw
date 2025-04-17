"""Documentation-related CLI commands."""

import subprocess
import sys

import click

from data_warehouse.utils.error_handler import handle_exceptions
from data_warehouse.utils.logger import get_command_logger

log = get_command_logger(__name__)


@click.group()
def docs():
    """Documentation management commands.

    Serve or build the project documentation using MkDocs.
    """
    pass


@docs.command("serve")
@click.option("--host", default="127.0.0.1", help="Host to serve docs on (default: 127.0.0.1)")
@click.option("--port", default=8000, type=int, help="Port to serve docs on (default: 8000)")
@handle_exceptions()
def serve_docs(host: str, port: int) -> None:
    """Serve the documentation locally with live reload."""
    cmd = [sys.executable, "-m", "mkdocs", "serve", "-f", "docs/mkdocs.yml", "--dev-addr", f"{host}:{port}"]
    log.info(f"Serving documentation at http://{host}:{port} ...")
    try:
        subprocess.run(cmd, check=True)
    except subprocess.CalledProcessError as exc:
        log.error(f"Failed to serve documentation: {exc}")
        sys.exit(exc.returncode)


@docs.command("build")
@handle_exceptions()
def build_docs() -> None:
    """Build the static documentation site."""
    cmd = [sys.executable, "-m", "mkdocs", "build", "-f", "docs/mkdocs.yml"]
    log.info("Building static documentation site ...")
    try:
        subprocess.run(cmd, check=True)
        log.success("Documentation built successfully.")
    except subprocess.CalledProcessError as exc:
        log.error(f"Failed to build documentation: {exc}")
        sys.exit(exc.returncode)
