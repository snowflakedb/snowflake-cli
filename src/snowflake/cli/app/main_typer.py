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

import sys

import typer
from snowflake.cli.api.cli_global_context import get_cli_context
from snowflake.cli.api.commands.flags import DEFAULT_CONTEXT_SETTINGS, DebugOption
from snowflake.cli.api.console import cli_console


def _handle_exception(exception: Exception):
    if get_cli_context().enable_tracebacks:
        raise exception
    else:
        cli_console.warning(
            "\nAn unexpected exception occurred. Use --debug option to see the traceback. Exception message:\n\n"
            + exception.__str__()
        )
        raise SystemExit(1)


class SnowCliMainTyper(typer.Typer):
    """
    Top-level SnowCLI Typer.
    It contains global exception handling.
    """

    def __init__(self):
        super().__init__(
            context_settings=DEFAULT_CONTEXT_SETTINGS,
            pretty_exceptions_show_locals=False,
            add_completion=True,
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
