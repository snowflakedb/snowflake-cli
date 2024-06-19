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
from snowflake.cli.api.commands.decorators import (
    with_output,
)
from snowflake.cli.api.commands.snow_typer import SnowTyperFactory
from snowflake.cli.api.output.types import CommandResult, SingleQueryResult
from snowflakecli.test_plugins.snowpark_hello.manager import SnowparkHelloManager

app = SnowTyperFactory(name="hello")


@app.command("hello", requires_connection=True, requires_global_options=True)
@with_output
def hello(
    name: str = typer.Argument(help="Your name"),
    **options,
) -> CommandResult:
    """
    Says hello
    """
    hello_manager = SnowparkHelloManager()
    cursor = hello_manager.say_hello(name)
    return SingleQueryResult(cursor)
