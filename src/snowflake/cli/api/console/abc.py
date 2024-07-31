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

from abc import ABC, abstractmethod
from contextlib import contextmanager
from typing import Callable, Iterator, Optional

from rich import print as rich_print
from rich.text import Text
from snowflake.cli.api.cli_global_context import (
    _CliGlobalContextAccess,
    get_cli_context,
)


class AbstractConsole(ABC):
    """Interface for cli console implementation.

    Each console should have three methods implemented:
    - `step` - for more detailed information on steps
    - `warning` - for displaying messages in a style that makes it
      visually stand out from other output
    - `phase` a context manager for organising steps into logical group
    """

    _print_fn: Callable[[str], None]
    _cli_context: _CliGlobalContextAccess
    _in_phase: bool

    def __init__(self):
        super().__init__()
        self._cli_context = get_cli_context()
        self._in_phase = False

    @property
    def is_silent(self) -> bool:
        """Returns information whether intermediate output is muted."""
        return self._cli_context.silent

    @property
    def in_phase(self) -> bool:
        """Indicated whether output should be grouped."""
        return self._in_phase

    def _print(self, text: Text):
        if self.is_silent:
            return
        rich_print(text)

    @contextmanager
    @abstractmethod
    def phase(
        self,
        enter_message: str,
        exit_message: Optional[str] = None,
    ) -> Iterator[Callable[[str], None]]:
        """A context manager for organising steps into logical group."""

    @contextmanager
    @abstractmethod
    def indented(self):
        """
        A context manager for temporarily indenting messages and warnings. Phases and steps cannot be used in indented blocks,
        but multiple indented blocks can be nested (use sparingly).
        """

    @abstractmethod
    def step(self, message: str):
        """Displays a message to output."""

    @abstractmethod
    def message(self, _message: str):
        """Displays an informational message to output."""

    @abstractmethod
    def warning(self, message: str):
        """Displays message in a style that makes it visually stand out from other output.

        Intended for displaying messages related to important messages."""
