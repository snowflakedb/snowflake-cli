import logging
import sys

import click
from snowcli.cli.common.cli_global_context import cli_context
from snowcli.cli.common.flags import DEFAULT_CONTEXT_SETTINGS, DebugOption
from snowcli.cli.loggers import remove_console_output_handler_from_logs
from typer import Typer


def _handle_exception(exception: Exception):
    # NOTES FOR REVIEWER:
    # pczajka: I'd expect logs to contain traceback in case of exception, but I also wanted to preserve
    #   current behavior. I decided to:
    #   - Keep console output handler, so the users won't loose the output they've seen until now
    #   - delete console handler in the line below, as:
    #     - the program is about to end :)
    #     - I'm not changing/reimplementing the logic of visibility of traceback on console output
    #
    # at some point we might want to reconsider to unify printing console output via "print" and "logging"
    # (below is the only use of "click.echo" my grep search found)
    remove_console_output_handler_from_logs()
    logger = logging.getLogger("snowcli")
    logger.exception(exception)

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
        #       to easily peek into subcommand arguments.
        DebugOption.callback(
            any(param in sys.argv for param in DebugOption.param_decls)
        )

        try:
            super().__call__(*args, **kwargs)
        except Exception as exception:
            _handle_exception(exception)
