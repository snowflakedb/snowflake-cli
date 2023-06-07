import click
from typer import Typer

from snowcli.cli import DEFAULT_CONTEXT_SETTINGS, OutsideTyperGlobalContext


class SnowCliMainTyper(Typer):
    def __init__(self, outside_typer_global_context: OutsideTyperGlobalContext):
        super().__init__(
            context_settings=DEFAULT_CONTEXT_SETTINGS,
            pretty_exceptions_show_locals=False,
        )
        self.outside_typer_global_context = outside_typer_global_context

    def __handle_exception(self, exception: Exception):
        debug = self.outside_typer_global_context.debug_logs_and_tracebacks
        if debug:
            raise exception from None
        else:
            click.echo(
                "An unexpected exception occurred. Use --debug option to see a traceback.\n"
                + "Exception message:\n"
                + exception.__str__()
            )
            raise SystemExit(1)

    def __call__(self, *args, **kwargs):
        try:
            super().__call__(*args, **kwargs)
        except Exception as exception:
            self.__handle_exception(exception)
