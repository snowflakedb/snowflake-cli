from __future__ import annotations

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

    def _print(self, message: str):
        if self.is_silent:
            return
        print(message)

    def phase(self, message: str):
        """Utility for displaying high level steps in commands.

        Messages are never indented. Subsequent step messages will be indented."""
        self.register_output(Output.PHASE)
        self._print(message)

    def step(self, message: str):
        """Prints indented message.

        Indentation is based on previous usage of `phase` method."""
        self.register_output(Output.STEP)
        text = self.format_message(message)
        self._print(text)


def get_cli_console() -> AbstractConsole:
    console = CliConsole(cli_context=cli_context)
    return console


cli_console = get_cli_console()
