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

from typing import Optional

import typer
from snowflake.cli.api.commands.snow_typer import SnowTyperFactory
from snowflake.cli.api.output.types import (
    CommandResult,
    MessageResult,
)

# simple Typer with defaults because it won't become a command group as it contains only one command
app = SnowTyperFactory()

DEFAULT_SOURCE = "undefined"


def _source_argument_callback(value):
    return value if value is not None else DEFAULT_SOURCE


SourceArgument = typer.Argument(
    None,
    help=f"local path to template directory or URL to git repository with templates. Default: {DEFAULT_SOURCE}",
    callback=_source_argument_callback,
    show_default=False,
)
NameOption = typer.Option(
    ...,
    help="which subdirectory of SOURCE should be used to create a template",
    show_default=False,
)


@app.command(no_args_is_help=True)
def init(
    source: Optional[str] = SourceArgument, name: str = NameOption
) -> CommandResult:
    """
    Creates project from template.
    """

    return MessageResult("OK")
