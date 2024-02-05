from __future__ import annotations

import logging
import sys
from functools import wraps
from typing import Optional

import typer
from rich import print as rich_print
from snowflake.cli.api.cli_global_context import cli_context
from snowflake.cli.api.commands.decorators import (
    global_options,
    global_options_with_connection,
)
from snowflake.cli.api.commands.flags import DEFAULT_CONTEXT_SETTINGS, DebugOption
from snowflake.cli.api.exceptions import CommandReturnTypeError
from snowflake.cli.api.output.types import CommandResult
from snowflake.cli.app.printing import print_result
from snowflake.cli.app.telemetry import flush_telemetry, log_command_usage

log = logging.getLogger(__name__)


def _handle_exception(exception: Exception):
    if cli_context.enable_tracebacks:
        raise exception
    else:
        rich_print(
            "\nAn unexpected exception occurred. Use --debug option to see the traceback. Exception message:\n\n"
            + exception.__str__()
        )
        raise SystemExit(1)


class SnowTyper(typer.Typer):
    def __init__(self, *args, **kwargs):
        super().__init__(
            *args,
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
        **kwargs,
    ):
        cls_super = super()

        def custom_command(command_callable):
            if requires_connection:
                command_callable = global_options_with_connection(command_callable)
            elif requires_global_options:
                command_callable = global_options(command_callable)

            @wraps(command_callable)
            def command_callable_decorator(*args, **kw):
                self.pre_execute_callback()
                try:
                    result = command_callable(*args, **kw)
                    return self.process_result(result)
                except Exception as err:
                    self.exception_execute_callback(err)
                    raise
                finally:
                    self.post_execute_callback()

            return cls_super.command(name=name, **kwargs)(command_callable_decorator)

        return custom_command

    @staticmethod
    def pre_execute_callback():
        """
        Callback executed before running any command callable (after context execution).
        Pay attention to make this method safe to use if performed operations are not necessary
        for executing the command in proper way.
        """
        log.debug("Executing command pre execution callback")
        log_command_usage()

    @staticmethod
    def process_result(result):
        """Command result processor"""
        if not isinstance(result, CommandResult):
            raise CommandReturnTypeError(type(result))
        print_result(result)

    @staticmethod
    def exception_execute_callback(exception: Exception):
        """
        Callback executed on command execution error.
        """
        log.debug("Executing command exception callback")

    @staticmethod
    def post_execute_callback():
        """
        Callback executed after running any command callable. Pay attention to make this method safe to
        use if performed operations are not necessary for executing the command in proper way.
        """
        log.debug("Executing command post execution callback")
        flush_telemetry()


class SnowCliMainTyper(typer.Typer):
    """
    Top-level SnowCLI Typer.
    It contains global exception handling.
    """

    def __init__(self):
        super().__init__(
            context_settings=DEFAULT_CONTEXT_SETTINGS,
            pretty_exceptions_show_locals=False,
        )

    def __call__(self, *args, **kwargs):
        # early detection of "--debug" flag
        # necessary in case of errors which happen during argument parsing
        # (for example badly formatted config file)
        # Hack: We have to go around Typer by checking sys.argv as it does not allow
        #       to easily peek into subcommand arguments.
        DebugOption.callback(
            any(param in sys.argv for param in DebugOption.param_decls)
        )

        try:
            super().__call__(*args, **kwargs)
        except Exception as exception:
            _handle_exception(exception)
