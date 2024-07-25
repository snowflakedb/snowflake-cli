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

from snowflake.cli.api.commands.snow_typer import SnowTyperFactory
from snowflake.cli.api.constants import TEMPLATES_PATH
from snowflake.cli.api.output.types import CommandResult, MessageResult
from snowflake.cli.api.secure_path import SecurePath
from typer import Argument


def _create_project_template(template_name: str, project_directory: str):
    SecurePath(TEMPLATES_PATH / template_name).copy(
        project_directory, dirs_exist_ok=True
    )


def add_init_command(
    app: SnowTyperFactory,
    project_type: str,
    template: str,
    help_message: Optional[str] = None,
):
    @app.command(deprecated=True)
    def init(
        project_name: str = Argument(
            f"example_{project_type.lower()}",
            help=(
                help_message
                if help_message is not None
                else f"Name of the {project_type} project you want to create."
            ),
        ),
        **options,
    ) -> CommandResult:

        _create_project_template(template, project_directory=project_name)
        return MessageResult(f"Initialized the new project in {project_name}/")

    project_type_doc = (
        project_type if project_type.lower() != "streamlit" else "Streamlit app"
    )

    init.__doc__ = (
        f"Initializes this directory with a sample set "
        f"of files for creating a {project_type_doc} project. "
        f"This command is deprecated and will be removed soon. "
        f"Please use 'snow init' instead"
    )

    return init
