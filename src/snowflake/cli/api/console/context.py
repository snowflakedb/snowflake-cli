from __future__ import annotations

from snowflake.cli.api.console.enum import Output


class CliConsoleContext:
    """Context for tracking phases and steps.

    Provides the information whether the phase was called.
    It is used for calcualting indentation of next output.
    """

    _stack: list[Output]
    __slots__ = ("_stack",)

    def __init__(self) -> None:
        self._stack = []

    def push(self, output: Output):
        self._stack.append(output)

    @property
    def is_in_phase(self) -> bool:
        return Output.PHASE in self._stack

    def reset(self):
        self._stack = []
