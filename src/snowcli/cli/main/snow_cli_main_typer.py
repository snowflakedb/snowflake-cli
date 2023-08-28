import click
from typer import Typer

from snowcli.cli.common.flags import DEFAULT_CONTEXT_SETTINGS
from snowcli.cli.common.snow_cli_global_context import snow_cli_global_context_manager


def _handle_exception(exception: Exception):
    enable_tracebacks = (
        snow_cli_global_context_manager.get_global_context_copy().enable_tracebacks
    )
    if enable_tracebacks:
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
        try:
            super().__call__(*args, **kwargs)
        except Exception as exception:
            _handle_exception(exception)
