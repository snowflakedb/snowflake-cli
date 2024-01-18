from __future__ import annotations

from rich import print
from snowflake.cli.api.cli_global_context import cli_context
from snowflake.cli.api.console.abc import AbstractConsole
from snowflake.cli.api.console.enum import Output


class CliConsole(AbstractConsole):
    def format_message(self, message: str) -> str:
        """Toolbox for displaying intermediate information in commands.

        It provides autmatic indentation based on previously used output."""
        if self.should_indent_output:
            return f"  {message}"
        return f"{message}"

    def phase(self, message: str):
        """Utility for displaying high level steps in commands.

        Messages are never indented. Subsequent step messages will be indented."""
        self._ctx.push(Output.PHASE)
        self._print(message)

    def step(self, message: str):
        """Prints indented message.

        Indentation is based on previous usage of `phase` method."""
        self._ctx.push(Output.STEP)
        text = self.format_message(message)
        self._print(text)


def get_cli_console() -> CliConsole:
    console = CliConsole(print_fn=print, cli_context=cli_context)
    return console


cli_console = get_cli_console()
