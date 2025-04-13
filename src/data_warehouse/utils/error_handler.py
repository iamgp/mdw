"""Error handling utilities for the CLI.

This module provides utilities for handling errors in a consistent manner across
the CLI commands. It includes decorators and helpers for catching and formatting errors.
"""

import functools
import sys
import traceback
from collections.abc import Callable
from typing import Any, TypeVar

import click
from loguru import logger

from data_warehouse.config.settings import settings
from data_warehouse.core.exceptions import (
    DatabaseError,
    DataWarehouseError,
    StorageError,
    ValidationError,
)

F = TypeVar("F", bound=Callable[..., Any])


def handle_exceptions(exit_on_error: bool = True) -> Callable[[F], F]:
    """Decorator to handle exceptions in a consistent manner.

    Args:
        exit_on_error: Whether to exit the program on error

    Returns:
        A decorator that handles exceptions
    """

    def decorator(func: F) -> F:
        @functools.wraps(func)
        def wrapper(*args: Any, **kwargs: Any) -> Any:
            try:
                return func(*args, **kwargs)
            except click.Abort:
                # Handle ctrl+c
                click.echo("\nOperation aborted by user.", err=True)
                if exit_on_error:
                    sys.exit(1)
            except click.UsageError as e:
                # Handle CLI usage errors
                click.secho(f"Error: {str(e)}", fg="red", err=True)
                if exit_on_error:
                    sys.exit(2)
            except ValidationError as e:
                # Handle validation errors
                click.secho(f"Validation Error: {str(e)}", fg="red", err=True)
                if exit_on_error:
                    sys.exit(3)
            except DatabaseError as e:
                # Handle database errors
                click.secho(f"Database Error: {str(e)}", fg="red", err=True)
                if exit_on_error:
                    sys.exit(4)
            except StorageError as e:
                # Handle storage errors
                click.secho(f"Storage Error: {str(e)}", fg="red", err=True)
                if exit_on_error:
                    sys.exit(5)
            except DataWarehouseError as e:
                # Handle known application errors
                click.secho(f"Error: {str(e)}", fg="red", err=True)
                if exit_on_error:
                    sys.exit(6)
            except Exception as e:
                # Handle unexpected errors
                click.secho("An unexpected error occurred:", fg="red", err=True)
                click.secho(str(e), fg="red", err=True)

                # Log the full traceback for debugging
                logger.error(f"Unexpected error: {str(e)}")
                logger.debug(f"Traceback: {traceback.format_exc()}")

                # Show traceback in debug mode
                if settings.LOG_LEVEL == "DEBUG":
                    click.echo(traceback.format_exc(), err=True)
                else:
                    click.echo(
                        "Run with --verbose for detailed error information.", err=True
                    )

                if exit_on_error:
                    sys.exit(10)

            return None  # Should only reach here if exit_on_error is False

        return wrapper  # type: ignore

    return decorator


def confirm_action(
    message: str, abort_message: str = "Operation cancelled.", default: bool = False
) -> bool:
    """Ask for confirmation before proceeding with a potentially dangerous action.

    Args:
        message: The confirmation message to display
        abort_message: The message to display if the user aborts
        default: The default answer (True for yes, False for no)

    Returns:
        True if the user confirmed, False otherwise
    """
    try:
        if click.confirm(message, default=default):
            return True
        else:
            click.echo(abort_message)
            return False
    except click.Abort:
        click.echo("\nOperation aborted by user.")
        return False
