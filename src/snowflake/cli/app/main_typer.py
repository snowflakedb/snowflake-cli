import sys

from rich import print as rich_print
from snowflake.cli.api.cli_global_context import cli_context
from snowflake.cli.api.commands.flags import DEFAULT_CONTEXT_SETTINGS, DebugOption
from typer import Typer


def _handle_exception(exception: Exception):
    if cli_context.enable_tracebacks:
        raise exception
    else:
        rich_print(
            "\nAn unexpected exception occurred. Use --debug option to see the traceback. Exception message:\n\n"
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
        #       to easily peek into subcommand arguments.
        DebugOption.callback(
            any(param in sys.argv for param in DebugOption.param_decls)
        )

        try:
            super().__call__(*args, **kwargs)
        except Exception as exception:
            _handle_exception(exception)
