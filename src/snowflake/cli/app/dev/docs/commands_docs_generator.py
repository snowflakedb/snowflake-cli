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

import logging
from typing import List, Optional

from click import Command
from snowflake.cli.api.secure_path import SecurePath
from snowflake.cli.app.dev.docs.template_utils import get_template_environment
from typer.core import TyperArgument

log = logging.getLogger(__name__)

CMD_USAGE_TMPL = "usage.rst.jinja2"
OVERVIEW_TMPL = "overview.rst.jinja2"


def generate_command_docs(
    root: SecurePath, command: Command, cmd_parts: Optional[List] = None
):
    """
    Iterates recursively through commands info. Creates a file structure resembling
    commands structure. For each terminal command creates a "usage" rst file.
    """
    if getattr(command, "hidden", False):
        return

    root.mkdir(exist_ok=True)
    if cmd_parts is None:
        _render_command_usage(command, root, cmd_parts, template_name=OVERVIEW_TMPL)

    cmd_parts = cmd_parts or []
    if hasattr(command, "commands"):
        for command_name, command_info in command.commands.items():
            path = root / command.name if command.name != "default" else root
            generate_command_docs(path, command_info, [*cmd_parts, command_name])
    else:
        _render_command_usage(command, root, cmd_parts)


def get_main_option(options: List[str]) -> str:
    long_options = [option for option in options if option.startswith("--")]
    if long_options:
        return long_options[0]

    short_options = [option for option in options if option.startswith("-")]
    if short_options:
        return short_options[0]

    return ""


def _render_command_usage(
    command: Command,
    root: SecurePath,
    path: Optional[List] = None,
    template_name: str = CMD_USAGE_TMPL,
):
    # This is end command
    command_name = command.name
    env = get_template_environment()
    env.filters[get_main_option.__name__] = get_main_option
    template = env.get_template(template_name)
    arguments = []
    options = []
    for param in command.params:
        if isinstance(param, TyperArgument):
            arguments.append(param)
        else:
            options.append(param)

    # RST files are presumed to be standalone pages in the docs with a matching item in the left nav.
    # Included files, which these are, need to use the .txt extension.
    file_path = root / f"usage-{command_name}.txt"
    log.info("Creating %s", file_path)
    with file_path.open("w+") as fh:
        fh.write(
            template.render(
                {
                    "help": command.help,
                    "name": command_name,
                    "options": options,
                    "arguments": arguments,
                    "path": path,
                }
            )
        )
