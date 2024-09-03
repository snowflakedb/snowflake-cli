# Copyright (c) 2024 Snowflake Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

from __future__ import annotations

import dataclasses
import logging
from functools import wraps
from typing import Any, Callable, Dict, List, Optional, Tuple

import typer
from click import ClickException
from snowflake.cli.api.commands.decorators import (
    global_options,
    global_options_with_connection,
)
from snowflake.cli.api.commands.execution_metadata import (
    ExecutionMetadata,
    ExecutionStatus,
)
from snowflake.cli.api.commands.flags import DEFAULT_CONTEXT_SETTINGS
from snowflake.cli.api.commands.typer_pre_execute import run_pre_execute_commands
from snowflake.cli.api.exceptions import CommandReturnTypeError
from snowflake.cli.api.output.types import CommandResult
from snowflake.cli.api.sanitizers import sanitize_for_terminal
from snowflake.cli.api.sql_execution import SqlExecutionMixin

log = logging.getLogger(__name__)


class SnowTyper(typer.Typer):
    def __init__(self, /, **kwargs):
        self._sanitize_kwargs(kwargs)
        super().__init__(
            **kwargs,
            context_settings=DEFAULT_CONTEXT_SETTINGS,
            pretty_exceptions_show_locals=False,
            no_args_is_help=True,
            add_completion=True,
            rich_markup_mode="markdown",
        )

    @staticmethod
    def _sanitize_kwargs(kwargs: Dict):
        # Sanitize all string options that are visible in terminal output
        known_keywords = [
            "help",
            "short_help",
            "options_metavar",
            "rich_help_panel",
            "epilog",
        ]
        for kw in known_keywords:
            if kw in kwargs:
                kwargs[kw] = sanitize_for_terminal(kwargs[kw])
        return kwargs

    @wraps(typer.Typer.command)
    def command(
        self,
        name: Optional[str] = None,
        requires_global_options: bool = True,
        requires_connection: bool = False,
        is_enabled: Callable[[], bool] | None = None,
        require_warehouse: bool = False,
        **kwargs,
    ):
        """
        Custom implementation of Typer.command that adds ability to execute additional
        logic before and after execution as well as process the result and act on possible
        errors.
        """
        name = sanitize_for_terminal(name)
        self._sanitize_kwargs(kwargs)
        if is_enabled is not None and not is_enabled():
            return lambda func: func

        def custom_command(command_callable):
            """Custom command wrapper similar to Typer.command."""
            # Sanitize doc string which is used to create help in terminal
            command_callable.__doc__ = sanitize_for_terminal(command_callable.__doc__)

            if requires_connection:
                command_callable = global_options_with_connection(command_callable)
            elif requires_global_options:
                command_callable = global_options(command_callable)

            @wraps(command_callable)
            def command_callable_decorator(*args, **kw):
                """Wrapper around command callable. This is what happens at "runtime"."""
                execution = ExecutionMetadata()
                self.pre_execute(execution, require_warehouse=require_warehouse)
                try:
                    result = command_callable(*args, **kw)
                    self.process_result(result)
                    execution.complete(ExecutionStatus.SUCCESS)
                except Exception as err:
                    execution.complete(ExecutionStatus.FAILURE)
                    self.exception_handler(err, execution)
                    raise
                finally:
                    self.post_execute(execution)

            return super(SnowTyper, self).command(name=name, **kwargs)(
                command_callable_decorator
            )

        return custom_command

    @staticmethod
    def pre_execute(execution: ExecutionMetadata, require_warehouse: bool = False):
        """
        Callback executed before running any command callable (after context execution).
        Pay attention to make this method safe to use if performed operations are not necessary
        for executing the command in proper way.
        """
        from snowflake.cli._app.telemetry import log_command_usage

        log.debug("Executing command pre execution callback")
        run_pre_execute_commands()
        log_command_usage(execution)
        if require_warehouse and not SqlExecutionMixin().session_has_warehouse():
            raise ClickException(
                "The command requires warehouse. No warehouse found in current connection."
            )

    @staticmethod
    def process_result(result):
        """Command result processor"""
        from snowflake.cli._app.printing import print_result

        # Because we still have commands like "logs" that do not return anything.
        # We should improve it in future.
        if not result:
            return
        if not isinstance(result, CommandResult):
            raise CommandReturnTypeError(type(result))
        print_result(result)

    @staticmethod
    def exception_handler(exception: Exception, execution: ExecutionMetadata):
        """
        Callback executed on command execution error.
        """
        from snowflake.cli._app.telemetry import log_command_execution_error

        log.debug("Executing command exception callback")
        log_command_execution_error(exception, execution)

    @staticmethod
    def post_execute(execution: ExecutionMetadata):
        """
        Callback executed after running any command callable. Pay attention to make this method safe to
        use if performed operations are not necessary for executing the command in proper way.
        """
        from snowflake.cli._app.telemetry import flush_telemetry, log_command_result

        log.debug("Executing command post execution callback")
        log_command_result(execution)
        flush_telemetry()


@dataclasses.dataclass
class SnowTyperCommandData:
    """
    Class for storing data of commands to be registered in SnowTyper instances created by SnowTyperFactory.
    """

    func: Callable
    args: Tuple[Any, ...]
    kwargs: Dict[str, Any]


class SnowTyperFactory:
    """
    SnowTyper factory. Usage is similar to SnowTyper, except that create_instance()
    creates actual SnowTyper instance.
    """

    def __init__(
        self,
        /,
        name: Optional[str] = None,
        help: Optional[str] = None,  # noqa: A002
        short_help: Optional[str] = None,
        is_hidden: Optional[Callable[[], bool]] = None,
        deprecated: bool = False,
    ):
        self.name = name
        self.help = help
        self.short_help = short_help
        self.is_hidden = is_hidden
        self.deprecated = deprecated
        self.commands_to_register: List[SnowTyperCommandData] = []
        self.subapps_to_register: List[SnowTyperFactory] = []
        self.callbacks_to_register: List[Callable] = []

    def create_instance(self) -> SnowTyper:
        app = SnowTyper(
            name=self.name,
            help=self.help,
            short_help=self.short_help,
            hidden=self.is_hidden() if self.is_hidden else False,
            deprecated=self.deprecated,
        )
        # register commands
        for command in self.commands_to_register:
            app.command(*command.args, **command.kwargs)(command.func)
        # register callbacks
        for callback in self.callbacks_to_register:
            app.callback()(callback)
        # add subgroups
        for subapp in self.subapps_to_register:
            app.add_typer(subapp.create_instance())
        return app

    def command(self, *args, **kwargs):
        def decorator(command):
            self.commands_to_register.append(
                SnowTyperCommandData(command, args=args, kwargs=kwargs)
            )
            return command

        return decorator

    def add_typer(self, snow_typer: SnowTyperFactory) -> None:
        self.subapps_to_register.append(snow_typer)

    def callback(self):
        def decorator(callback):
            self.callbacks_to_register.append(callback)
            return callback

        return decorator
