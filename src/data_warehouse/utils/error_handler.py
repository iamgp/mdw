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


def _handle_click_abort(exit_on_error: bool) -> None:
    click.echo("\nOperation aborted by user.", err=True)
    if exit_on_error:
        sys.exit(1)


def _handle_usage_error(e: Exception, exit_on_error: bool) -> None:
    click.secho(f"Error: {str(e)}", fg="red", err=True)
    if exit_on_error:
        sys.exit(2)


def _handle_validation_error(e: Exception, exit_on_error: bool) -> None:
    click.secho(f"Validation Error: {str(e)}", fg="red", err=True)
    if exit_on_error:
        sys.exit(3)


def _handle_database_error(e: Exception, exit_on_error: bool) -> None:
    click.secho(f"Database Error: {str(e)}", fg="red", err=True)
    if exit_on_error:
        sys.exit(4)


def _handle_storage_error(e: Exception, exit_on_error: bool) -> None:
    click.secho(f"Storage Error: {str(e)}", fg="red", err=True)
    if exit_on_error:
        sys.exit(5)


def _handle_datawarehouse_error(e: Exception, exit_on_error: bool) -> None:
    click.secho(f"Error: {str(e)}", fg="red", err=True)
    if exit_on_error:
        sys.exit(6)


def _handle_unexpected_error(e: Exception, exit_on_error: bool) -> None:
    click.secho("An unexpected error occurred:", fg="red", err=True)
    click.secho(str(e), fg="red", err=True)
    logger.error(f"Unexpected error: {str(e)}")
    logger.debug(f"Traceback: {traceback.format_exc()}")
    if settings.LOG_LEVEL == "DEBUG":
        click.echo(traceback.format_exc(), err=True)
    else:
        click.echo("Run with --verbose for detailed error information.", err=True)
    if exit_on_error:
        sys.exit(10)


def handle_exceptions(exit_on_error: bool = True) -> Callable[[F], F]:
    """Decorator to handle exceptions in a consistent manner.

    Args:
        exit_on_error: Whether to exit the program on error

    Returns:
        A decorator that handles exceptions
    """

    logger.debug(f"Creating exception handler decorator (exit_on_error={exit_on_error})")

    def decorator(func: F) -> F:
        @functools.wraps(func)
        def wrapper(*args: Any, **kwargs: Any) -> Any:
            logger.debug(f"Calling wrapped function: {func.__name__}")
            try:
                return func(*args, **kwargs)
            except click.Abort:
                logger.debug("Handling click.Abort exception")
                _handle_click_abort(exit_on_error)
            except click.UsageError as e:
                logger.debug(f"Handling click.UsageError: {e}")
                _handle_usage_error(e, exit_on_error)
            except ValidationError as e:
                logger.debug(f"Handling ValidationError: {e}")
                _handle_validation_error(e, exit_on_error)
            except DatabaseError as e:
                logger.debug(f"Handling DatabaseError: {e}")
                _handle_database_error(e, exit_on_error)
            except StorageError as e:
                logger.debug(f"Handling StorageError: {e}")
                _handle_storage_error(e, exit_on_error)
            except DataWarehouseError as e:
                logger.debug(f"Handling DataWarehouseError: {e}")
                _handle_datawarehouse_error(e, exit_on_error)
            except Exception as e:
                logger.debug(f"Handling unexpected Exception: {e}")
                _handle_unexpected_error(e, exit_on_error)
            return None  # Should only reach here if exit_on_error is False

        return wrapper  # type: ignore

    return decorator


def confirm_action(message: str, abort_message: str = "Operation cancelled.", default: bool = False) -> bool:
    """Ask for confirmation before proceeding with a potentially dangerous action.

    Args:
        message: The confirmation message to display
        abort_message: The message to display if the user aborts
        default: The default answer (True for yes, False for no)

    Returns:
        True if the user confirmed, False otherwise
    """
    logger.debug(f"Prompting user for confirmation: {message}")
    try:
        if click.confirm(message, default=default):
            logger.debug("User confirmed action.")
            return True
        else:
            click.echo(abort_message)
            logger.debug("User declined action.")
            return False
    except click.Abort:
        click.echo("\nOperation aborted by user.")
        logger.debug("User aborted action via click.Abort.")
        return False
