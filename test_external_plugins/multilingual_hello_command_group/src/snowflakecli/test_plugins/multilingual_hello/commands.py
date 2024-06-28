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

import typer
from snowflake.cli.api.commands.snow_typer import SnowTyperFactory
from snowflake.cli.api.output.types import CommandResult, SingleQueryResult
from snowflakecli.test_plugins.multilingual_hello.hello_language import HelloLanguage
from snowflakecli.test_plugins.multilingual_hello.manager import (
    MultilingualHelloManager,
)

app = SnowTyperFactory(
    name="multilingual-hello",
    help="Says hello in various languages",
)


def _hello(
    name: str,
    language: HelloLanguage,
) -> CommandResult:
    hello_manager = MultilingualHelloManager()
    cursor = hello_manager.say_hello(name, language)
    return SingleQueryResult(cursor)


@app.command("hello-en", requires_connection=True)
def hello_en(
    name: str = typer.Argument(help="Your name"),
    **options,
) -> CommandResult:
    """
    Says hello in English
    """
    return _hello(name, HelloLanguage.en)


@app.command("hello-fr", requires_connection=True)
def hello_fr(
    name: str = typer.Argument(help="Your name"),
    **options,
) -> CommandResult:
    """
    Says hello in French
    """
    return _hello(name, HelloLanguage.fr)


@app.command("hello", requires_connection=True)
def hello(
    name: str = typer.Argument(help="Your name"),
    language: HelloLanguage = typer.Option(..., "--language", help="Your language"),
    **options,
) -> CommandResult:
    """
    Says hello in your language
    """
    return _hello(name, language)
