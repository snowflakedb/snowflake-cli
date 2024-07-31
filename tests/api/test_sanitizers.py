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

import pytest
from snowflake.cli.api.commands.snow_typer import SnowTyper
from snowflake.cli.api.sanitizers import sanitize_for_terminal
from typer.testing import CliRunner


@pytest.mark.parametrize(
    "text, expected",
    [
        ("'\033[0i\007'", "'\x07'"),
        ("'\033[5i\007'", "'\x07'"),
        ("'🤯\033[1000;b\077'", "'🤯?'"),
        (
            "'\033[H\007''\033]1337;ClearScrollback\077''\033[2J\007''\033[1;31m'Error, enable MaliciousPlugin in your config'\033[#F\007'",
            "'\x07''1337;ClearScrollback?''\x07'''Error, enable MaliciousPlugin in your config'\x07'",
        ),
    ],
)
def test_sanitize_for_terminal(text, expected):
    result = sanitize_for_terminal(text)
    assert result == expected


def test_snow_typer_help_sanitization(os_agnostic_snapshot):
    app = SnowTyper()

    escape_text = "'\033[0i\007'"

    @app.command(epilog=escape_text, options_metavar=escape_text)
    def func1():
        """doc 1 '\033[0i\007'"""
        return 42

    @app.command(help=escape_text, rich_help_panel=escape_text, short_help=escape_text)
    def func2():
        return escape_text

    # Escape as arg not kwarg
    @app.command("'🤯\033[1000;b\077'")
    def func3():
        """doc 3"""
        return 42

    @app.command(name="'\033[5i\007'")
    def func4():
        """doc 4"""
        return 42

    runner = CliRunner()
    result = runner.invoke(app, ["--help"])
    assert result.output == os_agnostic_snapshot

    result = runner.invoke(app, ["func2"])
    assert result.output == ""
