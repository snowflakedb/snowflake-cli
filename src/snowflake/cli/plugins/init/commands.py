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

from typing import List, Optional

import typer
from click import ClickException
from snowflake.cli.api.commands.flags import (
    NoInteractiveOption,
    VariablesOption,
    parse_key_value_variables,
)
from snowflake.cli.api.commands.snow_typer import SnowTyperFactory
from snowflake.cli.api.output.types import (
    CommandResult,
    MessageResult,
)
from snowflake.cli.api.secure_path import SecurePath

# simple Typer with defaults because it won't become a command group as it contains only one command
app = SnowTyperFactory()

# TODO: create repo and override this parameter
DEFAULT_SOURCE = "undefined"


NameArgument = typer.Argument(
    help="which subdirectory of SOURCE should be used to create a template",
    show_default=False,
)
SourceOption = typer.Option(
    default=DEFAULT_SOURCE,
    help=f"local path to template directory or URL to git repository with templates.",
)


@app.command(no_args_is_help=True)
def init(
    name: str = NameArgument,
    template_source: Optional[str] = SourceOption,
    variables: Optional[List[str]] = VariablesOption,
    no_interactive: bool = NoInteractiveOption,
    **options,
) -> CommandResult:
    """
    Creates project from template.
    """
    variables_from_flags = {
        v.key: v.value
        for v in parse_key_value_variables(variables if variables else [])
    }
    with SecurePath.temporary_directory() as tmpdir:
        if not (local_template_dir := SecurePath(template_source)).exists():
            # assume template is URL
            raise NotImplementedError("urls not supported (yet)")

        else:
            if not (template_origin := (local_template_dir / name)).exists():
                raise ClickException(
                    f"Template {name} cannot be found under {local_template_dir}"
                )
            template_origin.copy(tmpdir.path)
            path_for_rendering = tmpdir / name

        path_for_rendering.copy(".")
    return MessageResult("OK")
