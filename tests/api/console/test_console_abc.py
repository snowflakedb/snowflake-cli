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

    console = TConsole()
    assert not console.is_silent

    with console.phase("Enter", "Exit"):
        console.step("b")
        console.warning("c")
        console.message("d")
        with console.indented():
            console.message("e")
            console.warning("f")

    out, _ = capsys.readouterr()
    assert out == "Enter\nb\nc\nd\ne\nf\nExit\n"
