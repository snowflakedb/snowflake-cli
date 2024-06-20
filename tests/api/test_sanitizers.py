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


def test_snow_typer_help_sanitization(snapshot):
    app = SnowTyper()

    @app.command()
    def func1():
        """'\033[0i\007'"""
        return 42

    @app.command(help="'\033[0i\007'")
    def func2():
        return "'\033[0i\007'"

    runner = CliRunner()
    result = runner.invoke(app, ["--help"])
    assert result.output == snapshot

    result = runner.invoke(app, ["func2"])
    assert result.output == ""
