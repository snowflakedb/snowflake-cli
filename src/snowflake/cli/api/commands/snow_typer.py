from __future__ import annotations

import logging
from functools import wraps
from typing import Callable, Optional

import typer
from snowflake.cli.api.commands.decorators import (
    global_options,
    global_options_with_connection,
)
from snowflake.cli.api.commands.flags import DEFAULT_CONTEXT_SETTINGS
from snowflake.cli.api.exceptions import CommandReturnTypeError
from snowflake.cli.api.output.types import CommandResult

log = logging.getLogger(__name__)


class SnowTyper(typer.Typer):
    def __init__(self, /, **kwargs):
        super().__init__(
            **kwargs,
            context_settings=DEFAULT_CONTEXT_SETTINGS,
            pretty_exceptions_show_locals=False,
        )

    @wraps(typer.Typer.command)
    def command(
        self,
        name: Optional[str] = None,
        requires_global_options: bool = True,
        requires_connection: bool = False,
        is_enabled: Callable[[], bool] | None = None,
        **kwargs,
    ):
        """
        Custom implementation of Typer.command that adds ability to execute additional
        logic before and after execution as well as process the result and act on possible
        errors.
        """
        if is_enabled is not None and not is_enabled():
            return lambda func: func

        def custom_command(command_callable):
            """Custom command wrapper similar to Typer.command."""
            if requires_connection:
                command_callable = global_options_with_connection(command_callable)
            elif requires_global_options:
                command_callable = global_options(command_callable)

            @wraps(command_callable)
            def command_callable_decorator(*args, **kw):
                """Wrapper around command callable. This is what happens at "runtime"."""
                self.pre_execute()
                try:
                    result = command_callable(*args, **kw)
                    return self.process_result(result)
                except Exception as err:
                    self.exception_handler(err)
                    raise
                finally:
                    self.post_execute()

            return super(SnowTyper, self).command(name=name, **kwargs)(
                command_callable_decorator
            )

        return custom_command

    @staticmethod
    def pre_execute():
        """
        Callback executed before running any command callable (after context execution).
        Pay attention to make this method safe to use if performed operations are not necessary
        for executing the command in proper way.
        """
        from snowflake.cli.app.telemetry import log_command_usage

        log.debug("Executing command pre execution callback")
        log_command_usage()

    @staticmethod
    def process_result(result):
        """Command result processor"""
        from snowflake.cli.app.printing import print_result

        # Because we still have commands like "logs" that do not return anything.
        # We should improve it in future.
        if not result:
            return
        if not isinstance(result, CommandResult):
            raise CommandReturnTypeError(type(result))
        print_result(result)

    @staticmethod
    def exception_handler(exception: Exception):
        """
        Callback executed on command execution error.
        """
        log.debug("Executing command exception callback")

    @staticmethod
    def post_execute():
        """
        Callback executed after running any command callable. Pay attention to make this method safe to
        use if performed operations are not necessary for executing the command in proper way.
        """
        from snowflake.cli.app.telemetry import flush_telemetry

        log.debug("Executing command post execution callback")
        flush_telemetry()
