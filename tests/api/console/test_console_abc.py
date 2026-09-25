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

from contextlib import contextmanager

from rich.text import Text
from snowflake.cli.api.console.abc import AbstractConsole


def test_console_base_class(capsys):
    class TConsole(AbstractConsole):
        @contextmanager
        def phase(self, enter_message: str, exit_message: str):
            print(enter_message)
            yield self.step
            print(exit_message)

        @contextmanager
        def indented(self):
            yield

        def step(self, message: str):
            print(message)

        def warning(self, message: str):
            print(message)

        def message(self, message: str):
            print(message)

        def panel(self, message: str):
            print(message)

        @contextmanager
        def spinner(self):
            yield

        def styled_message(self, message: str, style: str = ""):
            return self._print(Text(message), end="")

    console = TConsole()
    assert not console.is_silent

    with console.phase("Enter", "Exit"):
        console.step("b")
        console.warning("c")
        console.message("d")
        with console.indented():
            console.message("e")
            console.warning("f")
            console.styled_message("g\n")

    out, _ = capsys.readouterr()
    assert out == "Enter\nb\nc\nd\ne\nf\ng\nExit\n"


def test_plain_message_default_prints_markup_literally_and_strips_ansi(capsys):
    class TConsole(AbstractConsole):
        @contextmanager
        def phase(self, enter_message: str, exit_message: str):
            yield self.step

        @contextmanager
        def indented(self):
            yield

        def step(self, message: str):
            pass

        def warning(self, message: str):
            pass

        def message(self, message: str):
            pass

        def panel(self, message: str):
            pass

        @contextmanager
        def spinner(self):
            yield

        def styled_message(self, message: str, style: str = ""):
            pass

    TConsole().plain_message("tag \033[31m[/x]\033[0m here")

    out, _ = capsys.readouterr()
    assert out == "tag [/x] here\n"
    assert "\x1b" not in out
