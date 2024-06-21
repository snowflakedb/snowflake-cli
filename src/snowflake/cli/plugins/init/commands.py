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
import yaml
from click import ClickException
from snowflake.cli.api.commands.flags import (
    NoInteractiveOption,
    VariablesOption,
    parse_key_value_variables,
)
from snowflake.cli.api.commands.snow_typer import SnowTyperFactory

# from snowflake.cli.api.utils.rendering import get_project_template_cli_jinja_env
from snowflake.cli.api.constants import DEFAULT_SIZE_LIMIT_MB
from snowflake.cli.api.output.types import (
    CommandResult,
    MessageResult,
)
from snowflake.cli.api.project.schemas.template import Template
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

TEMPLATE_METADATA_FILE_NAME = "template.yml"


def _read_template_metadata(template_root: SecurePath) -> Template:
    template_metadata_path = template_root / TEMPLATE_METADATA_FILE_NAME
    if not template_metadata_path.exists():
        raise FileNotFoundError("Template does not have template.yml file")
    with template_metadata_path.open(read_file_limit_mb=DEFAULT_SIZE_LIMIT_MB) as fd:
        return Template(**yaml.safe_load(fd))


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

    is_remote_source = not SecurePath(template_source).exists()

    # copy/download template into tmpdir, so it is going to be removed in case command ens with an error
    with SecurePath.temporary_directory() as tmpdir:
        if is_remote_source:
            # assume template is URL
            raise NotImplementedError("urls not supported (yet)")

        else:
            template_origin = SecurePath(template_source) / name
            if not template_origin.exists():
                raise ClickException(
                    f"Template '{name}' cannot be found under {template_source}"
                )
            template_origin.copy(tmpdir.path)
            template_root = tmpdir / template_origin.name

        template_metadata = _read_template_metadata(template_root)

        template_root.copy(".")
    return MessageResult("OK")
