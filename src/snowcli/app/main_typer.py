import sys

import click
from snowcli.cli.common.cli_global_context import cli_context, cli_context_manager
from snowcli.cli.common.flags import DEFAULT_CONTEXT_SETTINGS, DebugOption
from typer import Typer


def _handle_exception(exception: Exception):
    if cli_context.enable_tracebacks:
        raise exception
    else:
        click.echo(
            "An unexpected exception occurred. Use --debug option to see the traceback.\n"
            + "Exception message:\n"
            + exception.__str__()
        )
        raise SystemExit(1)


class SnowCliMainTyper(Typer):
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
        #       to easily peek into subcommand arguments.  We can simply look for "--debug"
        #       as DebugOption is parsed only in this format (it's not possible to do "--debug false" etc.)
        DebugOption.callback("--debug" in sys.argv)

        try:
            super().__call__(*args, **kwargs)
        except Exception as exception:
            _handle_exception(exception)
