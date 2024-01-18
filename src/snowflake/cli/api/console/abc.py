from abc import ABC, abstractmethod
from typing import Callable

from snowflake.cli.api.cli_global_context import _CliGlobalContextAccess
from snowflake.cli.api.console.context import CliConsoleContext


class AbstractConsole(ABC):
    """Interface for cli console implementation.

    Each console should have two methods implemented:
    - `phase` for major informations on command actions
    - `step` for more detailed informations on step
    """

    _ctx: CliConsoleContext
    _print_fn: Callable
    _cli_context: _CliGlobalContextAccess

    def __init__(self, print_fn: Callable, cli_context: _CliGlobalContextAccess):
        super().__init__()
        self._ctx = CliConsoleContext()
        self._cli_context = cli_context
        self._print_fn = print_fn

    @property
    def is_silent(self) -> bool:
        """Returns information whether intermediate output is muted."""
        return self._cli_context.silent

    @property
    def should_indent_output(self) -> bool:
        """Informs if intermediate output is intended or not."""
        return self._ctx.is_in_phase

    def _print(self, message: str):
        if self.is_silent:
            return
        self._print_fn(message)

    @abstractmethod
    def phase(self, message: str):
        """Prints not indented output."""
        ...

    @abstractmethod
    def step(self, message: str):
        """Prints message according to _should_indent_output flag."""
        ...
