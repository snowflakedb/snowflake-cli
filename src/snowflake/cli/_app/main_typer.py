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
from pathlib import Path

import typer
from snowflake.cli._app.version_check import (
    maybe_show_new_version_banner,
    reset_banner_display_state,
    start_background_refresh,
)
from snowflake.cli.api.cli_global_context import get_cli_context
from snowflake.cli.api.commands.flags import DEFAULT_CONTEXT_SETTINGS, DebugOption
from snowflake.cli.api.config import config_init
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


def _config_file_argv(*call_args) -> list[str]:
    """Argv for an early ``--config-file`` peek before Click/Typer parsing."""
    if call_args:
        argv: list[str] = []
        for arg in call_args:
            if isinstance(arg, (list, tuple)):
                argv.extend(str(item) for item in arg)
            else:
                argv.append(str(arg))
        return argv
    return sys.argv[1:]


def _maybe_init_config_from_args(*call_args) -> None:
    """Apply ``--config-file`` before version-cache work when argv is available."""
    argv = _config_file_argv(*call_args)
    for index, arg in enumerate(argv):
        if arg == "--config-file" and index + 1 < len(argv):
            config_path = Path(argv[index + 1])
            break
        if arg.startswith("--config-file="):
            config_path = Path(arg.split("=", 1)[1])
            break
    else:
        return

    # Do not create a missing config file here; Click's ``exists=True`` option
    # validates the path and reports a clear error.
    if config_path.exists():
        config_init(config_path)


def _run_cli_invocation(self, run):
    """Shared startup/teardown for both ``__call__`` and ``main`` entry points."""
    DebugOption.callback(any(param in sys.argv for param in DebugOption.param_decls))
    reset_banner_display_state()
    try:
        return run()
    except Exception as exception:
        _handle_exception(exception)
    finally:
        maybe_show_new_version_banner()


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
        def run():
            _maybe_init_config_from_args(*args)
            start_background_refresh()
            return super(SnowCliMainTyper, self).__call__(*args, **kwargs)

        return _run_cli_invocation(self, run)

    def main(self, args=None, prog_name=None, **extra):
        cli_args = list(args or ())

        def run():
            _maybe_init_config_from_args(*cli_args)
            start_background_refresh()
            return super(SnowCliMainTyper, self).main(
                args=cli_args, prog_name=prog_name, **extra
            )

        return _run_cli_invocation(self, run)
